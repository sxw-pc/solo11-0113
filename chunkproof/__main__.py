import argparse
import json
import tempfile
from pathlib import Path
from . import engine, custody, rollout, terrain

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
    demo_terrain(root)

def demo_terrain(root):
    src=root/'terrain-source';src.mkdir()
    cells=[{'x':0,'y':0,'height':1,'runoff':1,'outlet':'L'},
           {'x':1,'y':0,'height':3,'runoff':2},
           {'x':2,'y':0,'height':1,'runoff':1,'outlet':'R'}]
    (src/'cells.jsonl').write_text('\n'.join(json.dumps(c) for c in cells)+'\n',encoding='utf-8')
    (src/'spec.json').write_text(json.dumps({'path':'cells.jsonl','neighbors':4}),encoding='utf-8')
    db=root/'terrain.db';version=engine.manifest(db,'terrain-delivery',src,64)
    media=root/'terrain-media';media.mkdir();rows=[]
    for chunk in engine.plan(db,version):
        raw=(src/chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        rel=f"{Path(chunk['path']).name}-{chunk['idx']}.part";(media/rel).write_bytes(raw)
        rows.append(json.dumps({'file':chunk['path'],'index':chunk['idx'],'path':rel}))
    receipts=root/'terrain-receipts.jsonl';receipts.write_text('\n'.join(rows),encoding='utf-8')
    engine.receive(db,version,'terrain-media-1',media,receipts)
    engine.assemble(db,version,media,root/'terrain-out');engine.seal(db,version,root/'terrain-out')
    terrain.register(db,'demo-grid',version,src/'spec.json')
    for model in terrain.MODELS:
        result=terrain.route(db,'demo-grid',model,{'L':3},f'demo-{model}')
        print(model,{name:item['passed'] for name,item in result['outlets'].items()})
    print(terrain.verify(db,'demo-single_flow'))

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
    a=sub.add_parser('terrain-register');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--version',required=True);a.add_argument('--spec',required=True)
    a=sub.add_parser('terrain-inspect');a.add_argument('--db',required=True);a.add_argument('--id',required=True)
    a=sub.add_parser('terrain-route');a.add_argument('--db',required=True);a.add_argument('--id',required=True);a.add_argument('--model',required=True,choices=terrain.MODELS);a.add_argument('--thresholds',default='{}');a.add_argument('--route',required=True);a.add_argument('--budget',type=int,default=terrain.DEFAULT_BUDGET)
    a=sub.add_parser('terrain-verify');a.add_argument('--db',required=True);a.add_argument('--route',required=True)
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
    elif x.command=='terrain-register':print(terrain.register(x.db,x.id,x.version,x.spec))
    elif x.command=='terrain-inspect':print(json.dumps(terrain.inspect(x.db,x.id),ensure_ascii=False))
    elif x.command=='terrain-route':print(json.dumps(terrain.route(x.db,x.id,x.model,json.loads(x.thresholds),x.route,x.budget),ensure_ascii=False))
    elif x.command=='terrain-verify':print(json.dumps(terrain.verify(x.db,x.route),ensure_ascii=False))
    elif x.command=='verify-chunks':print(engine.verify_chunks(x.db,x.version,x.medium,x.root))
    elif x.command=='reconcile':print(json.dumps(engine.reconcile(x.db,x.version)))
    elif x.command=='assemble':print(engine.assemble(x.db,x.version,x.root,x.out))
    elif x.command=='seal':print(engine.seal(x.db,x.version,x.out))
    elif x.command=='report':print(engine.report(x.db,x.version,x.path))
if __name__=='__main__':main()
