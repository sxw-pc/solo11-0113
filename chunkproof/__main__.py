import argparse
import json
import tempfile
from pathlib import Path
from . import engine, custody, rollout, latency

def demo():
    root=Path(tempfile.mkdtemp(prefix='chunkproof-'));src=root/'source';src.mkdir();(src/'sample.dat').write_bytes(b'abcdefghij')
    samples=[('d1',12),('d2',18),('d3',25),('d4',31),('d5',47),('d6',90)]
    (src/'latency.jsonl').write_text(''.join(json.dumps({'sample_id':i,'latency_ms':v})+'\n' for i,v in samples),encoding='utf-8')
    db=root/'ledger.db';version=engine.manifest(db,'delivery-1',src,4)
    media=root/'media';media.mkdir(); rows=[]
    for c in engine.plan(db,version):
        rel=f"{c['path'].replace('/','_')}-{c['idx']}.part";part=(src/c['path']).read_bytes()[c['start']:c['end']];(media/rel).write_bytes(part)
        rows.append(json.dumps({'file':c['path'],'path':rel,'index':c['idx'],'sha256':c['sha256']}))
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
    spec=root/'latency-spec.json';spec.write_text(json.dumps({'paths':['latency.jsonl']}),encoding='utf-8')
    latency.register(db,'demo-latency',version,spec)
    assessment=latency.assess(db,'demo-latency',100,'demo-assessment',memory_rows=2,scratch_bytes=65536)
    print(json.dumps({'assessment':assessment['assessment'],'verdict':assessment['verdict'],
                      'samples':assessment.get('samples'),'p95_ms':assessment.get('p95_ms'),
                      'sla':assessment.get('sla')},ensure_ascii=False))
    print(latency.recheck(db,'demo-assessment')['verified'])

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
    a=sub.add_parser('latency-register');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--version',required=True);a.add_argument('--spec',required=True)
    a=sub.add_parser('latency-evaluate');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--limit-ms',type=int,required=True);a.add_argument('--max-rows',type=int,default=1000)
    a=sub.add_parser('latency-assess');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--assessment',required=True);a.add_argument('--limit-ms',type=int,required=True);a.add_argument('--memory-rows',type=int,required=True);a.add_argument('--scratch-bytes',type=int,required=True);a.add_argument('--scratch-dir')
    a=sub.add_parser('latency-recheck');a.add_argument('--db',required=True);a.add_argument('--assessment',required=True)
    a=sub.add_parser('latency-assessment');a.add_argument('--db',required=True);a.add_argument('--assessment',required=True)
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
    elif x.command=='latency-register':print(latency.register(x.db,x.id,x.version,x.spec))
    elif x.command=='latency-evaluate':print(json.dumps(latency.evaluate(x.db,x.id,x.limit_ms,x.max_rows)))
    elif x.command=='latency-assess':print(json.dumps(latency.assess(x.db,x.id,x.limit_ms,x.assessment,x.memory_rows,x.scratch_bytes,scratch_dir=x.scratch_dir),ensure_ascii=False))
    elif x.command=='latency-recheck':print(json.dumps(latency.recheck(x.db,x.assessment),ensure_ascii=False))
    elif x.command=='latency-assessment':print(json.dumps(latency.status(x.db,x.assessment),ensure_ascii=False))
    elif x.command=='verify-chunks':print(engine.verify_chunks(x.db,x.version,x.medium,x.root))
    elif x.command=='reconcile':print(json.dumps(engine.reconcile(x.db,x.version)))
    elif x.command=='assemble':print(engine.assemble(x.db,x.version,x.root,x.out))
    elif x.command=='seal':print(engine.seal(x.db,x.version,x.out))
    elif x.command=='report':print(engine.report(x.db,x.version,x.path))
if __name__=='__main__':main()
