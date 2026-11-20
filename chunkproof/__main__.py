import argparse
import json
import tempfile
from pathlib import Path
from . import engine, custody, rollout, entities

def demo():
    root=Path(tempfile.mkdtemp(prefix='chunkproof-'));src=root/'source';src.mkdir();(src/'sample.dat').write_bytes(b'abcdefghij')
    db=root/'ledger.db';version=engine.manifest(db,'delivery-1',src,4)
    media=root/'media';media.mkdir(); rows=[]
    data=(src/'sample.dat').read_bytes()
    for c in engine.plan(db,version):
        rel=f"{c['idx']}.part";part=data[c['start']:c['end']];(media/rel).write_bytes(part)
        rows.append(json.dumps({'file':'sample.dat','path':rel,'index':c['idx'],'sha256':c['sha256']}))
    receipts=root/'receipts.jsonl';receipts.write_text('\n'.join(rows),encoding='utf-8')
    engine.receive(db,version,'medium-1',media,receipts)
    engine.assemble(db,version,media,root/'out')
    result=engine.seal(db,version,root/'out')
    custody.transfer(db,version,'medium-1',1,'origin','carrier','2026-01-01T08:00:00Z')
    custody.recipient(db,'recipient-1','2026-01-02T00:00:00Z')
    from .store import connect
    ledger=connect(db);body_hash=ledger.execute('SELECT body_hash FROM manifest WHERE version=?',(version,)).fetchone()['body_hash'];ledger.close()
    custody.acknowledge(db,'recipient-1',version,body_hash,'2026-01-01T09:00:00Z','accepted')
    print(engine.report(db,version,root/'report.json'));print(result,custody.decide(db,'recipient-1',version))
    rollout.create(db,'demo-rollout',version,[{'name':'pilot','quorum':1,'members':[{'recipient':'recipient-1','weight':1,'mandatory':True}]}])
    print(rollout.evaluate(db,'demo-rollout'))
    feed=root/'feed';feed.mkdir()
    (feed/'north.jsonl').write_text('\n'.join(json.dumps(x,ensure_ascii=False) for x in (
        {'local_id':'N1','cohort':'north'},{'local_id':'N2','cohort':'north'}))+'\n',encoding='utf-8')
    (feed/'south.jsonl').write_text('\n'.join(json.dumps(x,ensure_ascii=False) for x in (
        {'local_id':'S1','cohort':'south'},{'local_id':'S2','cohort':'south'}))+'\n',encoding='utf-8')
    (feed/'links.jsonl').write_text('\n'.join(json.dumps(x,ensure_ascii=False) for x in (
        {'left_id':'N1','right_id':'S1','weight':2},
        {'left_id':'N1','right_id':'S2','weight':1},
        {'left_id':'N2','right_id':'S1','weight':1}))+'\n',encoding='utf-8')
    (root/'entity-feed-spec.json').write_text(json.dumps(
        {'sources':[{'name':'north','path':'north.jsonl'},{'name':'south','path':'south.jsonl'}],
         'links':'links.jsonl'}),encoding='utf-8')
    fversion=engine.manifest(db,'entity-delivery',feed,16)
    fmedia=root/'fmedia';fmedia.mkdir();frows=[]
    for c in engine.plan(db,fversion):
        rel=f"fp-{c['path'].replace('/','_')}-{c['idx']}";(fmedia/rel).write_bytes((feed/c['path']).read_bytes()[c['start']:c['end']])
        frows.append(json.dumps({'file':c['path'],'path':rel,'index':c['idx'],'sha256':c['sha256']}))
    freceipts=root/'freceipts.jsonl';freceipts.write_text('\n'.join(frows),encoding='utf-8')
    engine.receive(db,fversion,'feed-medium',fmedia,freceipts)
    engine.assemble(db,fversion,fmedia,root/'fout')
    print(engine.seal(db,fversion,root/'fout'))
    entities.register(db,'demo-feed',fversion,root/'entity-feed-spec.json')
    print(json.dumps(entities.inspect(db,'demo-feed'),ensure_ascii=False))
    op=entities.assess(db,'demo-feed','operational',2,'demo-op')
    cert=entities.assess(db,'demo-feed','certified',2,'demo-cert')
    print(json.dumps(op,ensure_ascii=False,sort_keys=True))
    print(json.dumps(cert,ensure_ascii=False,sort_keys=True))
    print(json.dumps(entities.verify(db,'demo-op'),ensure_ascii=False))
    print(json.dumps(entities.verify(db,'demo-cert'),ensure_ascii=False))

def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('demo')
    m=sub.add_parser('manifest');m.add_argument('--db',required=True);m.add_argument('--delivery',required=True);m.add_argument('--root',required=True);m.add_argument('--block',type=int,default=4)
    a=sub.add_parser('plan');a.add_argument('--db',required=True);a.add_argument('--version',required=True)
    r=sub.add_parser('receive');r.add_argument('--db',required=True);r.add_argument('--version',required=True);r.add_argument('--medium',required=True);r.add_argument('--root',required=True);r.add_argument('--receipts',required=True)
    t=sub.add_parser('custody-transfer');t.add_argument('--db',required=True);t.add_argument('--version',required=True);t.add_argument('--medium',required=True);t.add_argument('--seq',type=int,required=True);t.add_argument('--from-holder',required=True);t.add_argument('--to-holder',required=True);t.add_argument('--at',required=True)
    a=sub.add_parser('recipient');a.add_argument('--db',required=True);a.add_argument('--name',required=True);a.add_argument('--deadline',required=True)
    a=sub.add_parser('acknowledge');a.add_argument('--db',required=True);a.add_argument('--name',required=True);a.add_argument('--version',required=True);a.add_argument('--manifest-hash',required=True);a.add_argument('--at',required=True);a.add_argument('--claim',required=True)
    a=sub.add_parser('acceptance');a.add_argument('--db',required=True);a.add_argument('--name',required=True);a.add_argument('--version',required=True)
    a=sub.add_parser('rollout-create');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--version',required=True);a.add_argument('--policy',required=True)
    a=sub.add_parser('rollout-evaluate');a.add_argument('--db',required=True);a.add_argument('--id',required=True)
    a=sub.add_parser('entity-register');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--version',required=True);a.add_argument('--spec',required=True)
    a=sub.add_parser('entity-inspect');a.add_argument('--db',required=True);a.add_argument('--id',required=True)
    a=sub.add_parser('entity-assess');a.add_argument('--db',required=True);a.add_argument('--feed',required=True);a.add_argument('--policy',choices=('operational','certified'),required=True);a.add_argument('--threshold',type=int,required=True);a.add_argument('--id',required=True);a.add_argument('--budget',type=int,default=entities.DEFAULT_BUDGET)
    a=sub.add_parser('entity-verify');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--budget',type=int,default=entities.DEFAULT_BUDGET)
    for command in ('verify-chunks','reconcile','assemble','seal','report'):
        q=sub.add_parser(command);q.add_argument('--db',required=True);q.add_argument('--version',required=True)
        if command=='verify-chunks':q.add_argument('--medium',required=True);q.add_argument('--root',required=True)
        if command=='assemble':q.add_argument('--root',required=True);q.add_argument('--out',required=True)
        if command=='seal':q.add_argument('--out',required=True)
        if command=='report':q.add_argument('--path',required=True)
    x=p.parse_args()
    if x.command=='demo':demo()
    elif x.command=='manifest':print(engine.manifest(x.db,x.delivery,x.root,x.block))
    elif x.command=='plan':print(json.dumps(engine.plan(x.db,x.version)))
    elif x.command=='receive':print(engine.receive(x.db,x.version,x.medium,x.root,x.receipts))
    elif x.command=='custody-transfer':print(custody.transfer(x.db,x.version,x.medium,x.seq,x.from_holder,x.to_holder,x.at))
    elif x.command=='recipient':custody.recipient(x.db,x.name,x.deadline)
    elif x.command=='acknowledge':print(custody.acknowledge(x.db,x.name,x.version,x.manifest_hash,x.at,x.claim))
    elif x.command=='acceptance':print(custody.decide(x.db,x.name,x.version))
    elif x.command=='rollout-create':print(rollout.create(x.db,x.id,x.version,json.loads(Path(x.policy).read_text(encoding='utf-8'))))
    elif x.command=='rollout-evaluate':print(rollout.evaluate(x.db,x.id))
    elif x.command=='entity-register':print(entities.register(x.db,x.id,x.version,x.spec))
    elif x.command=='entity-inspect':print(json.dumps(entities.inspect(x.db,x.id),ensure_ascii=False))
    elif x.command=='entity-assess':print(json.dumps(entities.assess(x.db,x.feed,x.policy,x.threshold,x.id,x.budget),ensure_ascii=False,sort_keys=True))
    elif x.command=='entity-verify':print(json.dumps(entities.verify(x.db,x.id,x.budget),ensure_ascii=False))
    elif x.command=='verify-chunks':print(engine.verify_chunks(x.db,x.version,x.medium,x.root))
    elif x.command=='reconcile':print(json.dumps(engine.reconcile(x.db,x.version)))
    elif x.command=='assemble':print(engine.assemble(x.db,x.version,x.root,x.out))
    elif x.command=='seal':print(engine.seal(x.db,x.version,x.out))
    elif x.command=='report':print(engine.report(x.db,x.version,x.path))
if __name__=='__main__':main()
