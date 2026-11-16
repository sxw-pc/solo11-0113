import argparse
import json
import tempfile
from pathlib import Path
from . import engine

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
    result=engine.seal(db,version,root/'out');print(engine.report(db,version,root/'report.json'));print(result)

def main():
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('demo')
    m=sub.add_parser('manifest');m.add_argument('--db',required=True);m.add_argument('--delivery',required=True);m.add_argument('--root',required=True);m.add_argument('--block',type=int,default=4)
    a=sub.add_parser('plan');a.add_argument('--db',required=True);a.add_argument('--version',required=True)
    r=sub.add_parser('receive');r.add_argument('--db',required=True);r.add_argument('--version',required=True);r.add_argument('--medium',required=True);r.add_argument('--root',required=True);r.add_argument('--receipts',required=True)
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
    elif x.command=='verify-chunks':print(engine.verify_chunks(x.db,x.version,x.medium,x.root))
    elif x.command=='reconcile':print(json.dumps(engine.reconcile(x.db,x.version)))
    elif x.command=='assemble':print(engine.assemble(x.db,x.version,x.root,x.out))
    elif x.command=='seal':print(engine.seal(x.db,x.version,x.out))
    elif x.command=='report':print(engine.report(x.db,x.version,x.path))
if __name__=='__main__':main()
