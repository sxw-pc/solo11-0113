import hashlib
import json
import os
from pathlib import Path, PurePosixPath
from .store import connect

def sha(data): return hashlib.sha256(data).hexdigest()
def safe_path(s):
    p=PurePosixPath(s)
    if not s or p.is_absolute() or '..' in p.parts or '\\' in s or ':' in s or str(p)!=s: raise ValueError('unsafe_path')
    return s
def canonical(obj): return json.dumps(obj,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()

def manifest(db_path, delivery, root, block=4):
    root=Path(root); files=[]; seen=set()
    if block<=0: raise ValueError('bad_block')
    for path in sorted(p for p in root.rglob('*') if p.is_file()):
        rel=safe_path(path.relative_to(root).as_posix()); key=rel.casefold()
        if key in seen: raise ValueError('case_collision')
        seen.add(key); chunks=[]; full=hashlib.sha256();start=0;index=0
        with path.open('rb') as stream:
            while part:=stream.read(block):
                full.update(part);chunks.append({'index':index,'start':start,'end':start+len(part),'sha256':sha(part)})
                start+=len(part);index+=1
        files.append({'path':rel,'size':start,'sha256':full.hexdigest(),'chunks':chunks})
    body=canonical({'delivery':delivery,'block':block,'files':files}); version=sha(body)
    db=connect(db_path)
    with db:
        db.execute("INSERT OR IGNORE INTO delivery VALUES (?,'草拟')",(delivery,))
        old=db.execute('SELECT body FROM manifest WHERE version=?',(version,)).fetchone()
        if old and old['body']!=body: raise ValueError('manifest_conflict')
        if not old:
            db.execute('INSERT INTO manifest(version,delivery_id,body_hash,body) VALUES (?,?,?,?)',(version,delivery,sha(body),body))
            for f in files:
                fid=db.execute('INSERT INTO file_entry(version,path,size,sha256) VALUES (?,?,?,?)',(version,f['path'],f['size'],f['sha256'])).lastrowid
                for c in f['chunks']:
                    db.execute('INSERT INTO chunk(file_id,idx,start,end,sha256) VALUES (?,?,?,?,?)',(fid,c['index'],c['start'],c['end'],c['sha256']))
        db.execute("UPDATE delivery SET status='清单冻结' WHERE id=?",(delivery,))
    db.close();return version

def plan(db_path,version):
    db=connect(db_path); rows=[dict(x) for x in db.execute('SELECT f.path,c.idx,c.start,c.end,c.sha256 FROM chunk c JOIN file_entry f ON f.id=c.file_id WHERE f.version=? ORDER BY f.path,c.idx',(version,))];db.close();return rows

def receive(db_path,version,medium,root,receipt_path):
    root=Path(root).resolve(); raw=Path(receipt_path).read_bytes(); records=[json.loads(x) for x in raw.decode().splitlines() if x.strip()]
    db=connect(db_path);count=0
    with db:
        if not db.execute('SELECT 1 FROM manifest WHERE version=?',(version,)).fetchone(): raise ValueError('unknown_version')
        old=db.execute('SELECT id FROM attempt WHERE version=? AND medium=? AND source_hash=?',(version,medium,sha(raw))).fetchone()
        if old:return 0
        aid=db.execute('INSERT INTO attempt(version,medium,source_hash,source,root) VALUES (?,?,?,?,?)',(version,medium,sha(raw),raw,str(root))).lastrowid
        for item in records:
            rel=safe_path(item['path']); logical=safe_path(item['file']); idx=int(item['index']); candidate=(root/rel).resolve()
            if not candidate.is_relative_to(root): raise ValueError('unsafe_path')
            row=db.execute('SELECT c.id,c.sha256,c.end-c.start AS size FROM chunk c JOIN file_entry f ON f.id=c.file_id WHERE f.version=? AND f.path=? AND c.idx=?',(version,logical,idx)).fetchone()
            data=candidate.read_bytes() if candidate.is_file() else None
            actual=sha(data) if data is not None else None
            status='missing' if data is None else ('wrong_version' if row is None else ('verified' if actual==row['sha256'] and len(data)==row['size'] else 'corrupt'))
            if status=='verified' and item.get('sha256') and item['sha256']!=actual:status='claim_conflict'
            rid=db.execute('INSERT INTO receipt(attempt_id,chunk_id,medium,path,claimed_hash,actual_hash,actual_size,status) VALUES (?,?,?,?,?,?,?,?)',(aid,row['id'] if row else None,medium,rel,item.get('sha256'),actual,len(data) if data is not None else None,status)).lastrowid
            if row and status=='verified': db.execute("UPDATE chunk SET status='verified' WHERE id=?",(row['id'],))
            if status!='verified': db.execute('INSERT INTO issue(version,code,detail,receipt_id) VALUES (?,?,?,?)',(version,status,f'{logical}:{idx}',rid))
            count+=1
        db.execute("UPDATE delivery SET status='接收中' WHERE id=(SELECT delivery_id FROM manifest WHERE version=?)",(version,))
    db.close();return count

def reconcile(db_path,version):
    db=connect(db_path); result={}
    for c in db.execute('SELECT c.id,f.path,c.idx,c.sha256 FROM chunk c JOIN file_entry f ON f.id=c.file_id WHERE f.version=?',(version,)):
        candidates=[dict(r) for r in db.execute("SELECT * FROM receipt WHERE chunk_id=? AND status='verified' ORDER BY id",(c['id'],))]
        result[f"{c['path']}:{c['idx']}"]=candidates[0]['id'] if candidates else None
    db.close();return result

def verify_chunks(db_path,version,medium,root):
    root=Path(root).resolve();db=connect(db_path);out=[]
    with db:
        receipts=[dict(r) for r in db.execute('SELECT r.*,c.sha256 AS expected,c.end-c.start AS expected_size FROM receipt r JOIN attempt a ON a.id=r.attempt_id LEFT JOIN chunk c ON c.id=r.chunk_id WHERE a.version=? AND r.medium=?',(version,medium))]
        for r in receipts:
            candidate=(root/r['path']).resolve()
            if not candidate.is_relative_to(root):raise ValueError('unsafe_path')
            data=candidate.read_bytes() if candidate.is_file() else None
            actual=sha(data) if data is not None else None
            status='missing' if data is None else ('wrong_version' if r['chunk_id'] is None else ('verified' if actual==r['expected'] and len(data)==r['expected_size'] else 'corrupt'))
            if status=='verified' and r['claimed_hash'] and r['claimed_hash']!=actual:status='claim_conflict'
            db.execute('UPDATE receipt SET actual_hash=?,actual_size=?,status=? WHERE id=?',(actual,len(data) if data is not None else None,status,r['id']))
            if status!='verified':db.execute('INSERT INTO issue(version,code,detail,receipt_id) VALUES (?,?,?,?)',(version,status,r['path'],r['id']))
            out.append((r['id'],status))
    db.close();return out

def assemble(db_path,version,root,out,plan=None):
    from . import reuse
    db=connect(db_path); root=Path(root).resolve();out=Path(out);out.mkdir(parents=True,exist_ok=True)
    plan_items=None;plan_id=None
    if plan is not None:
        if plan.get('version')!=version: raise ValueError('plan_version_mismatch')
        if plan.get('contract') not in reuse.CONTRACTS: raise ValueError('unknown_contract')
        plan_id=plan['plan_id']
        if not db.execute('SELECT 1 FROM reuse_plan WHERE id=?',(plan_id,)).fetchone():
            raise ValueError('plan_not_frozen')
        fresh=reuse.reuse_plan(db_path,version,plan['contract'],plan['recipients'],plan['old_versions'])
        if fresh['plan_id']!=plan_id: raise ValueError('plan_stale')
        plan_items={x['key']:x for x in plan['items']}
    selected=reconcile(db_path,version);made=[]
    for f in db.execute('SELECT * FROM file_entry WHERE version=? ORDER BY path',(version,)):
        target=(out/f['path']).resolve()
        if not target.is_relative_to(out.resolve()): raise ValueError('unsafe_output')
        chunks=list(db.execute('SELECT * FROM chunk WHERE file_id=? ORDER BY idx',(f['id'],)))
        needs=[];complete=True
        for c in chunks:
            key=f"{f['path']}:{c['idx']}"; rid=selected.get(key)
            if rid is not None:
                needs.append(('receipt',rid,c))
            elif plan_items is not None and plan_items[key]['conclusion']=='reusable' \
                    and plan_items[key]['basis']=='content_reuse_old_delivery':
                needs.append(('reuse',plan_items[key]['eligible'],c))
            else:
                complete=False;break
        if not complete: continue
        target.parent.mkdir(parents=True,exist_ok=True); temp=target.with_suffix(target.suffix+'.part')
        proof_rows=[]
        with temp.open('wb') as stream:
            for kind,ref,c in needs:
                if kind=='receipt':
                    rec=db.execute('SELECT r.path,a.root FROM receipt r JOIN attempt a ON a.id=r.attempt_id WHERE r.id=?',(ref,)).fetchone()
                    candidate=(Path(rec['root'])/rec['path']).resolve()
                    if not candidate.is_relative_to(Path(rec['root']).resolve()):raise ValueError('unsafe_path')
                    data=candidate.read_bytes()
                    if sha(data)!=c['sha256'] or len(data)!=c['end']-c['start']: raise ValueError('source_changed')
                    stream.write(data);proof_rows.append(('receipt',ref,None))
                else:
                    data=reuse.read_source(ref['source_root'],ref['source_path'])
                    if data is None or sha(data)!=c['sha256'] or len(data)!=c['end']-c['start']:
                        raise ValueError('reuse_source_changed')
                    stream.write(data);proof_rows.append(('reuse',ref,c))
        if target.exists() and sha(target.read_bytes())!=f['sha256']:
            temp.unlink();raise ValueError('output_conflict')
        os.replace(temp,target)
        receipt_only=[ref for kind,ref,_ in proof_rows if kind=='receipt']
        with db:
            db.execute('INSERT OR REPLACE INTO assembled_file(version,file_id,path,hash,receipt_ids) VALUES (?,?,?,?,?)',
                       (version,f['id'],str(target),sha(target.read_bytes()),json.dumps(receipt_only)))
            db.execute('DELETE FROM assembled_proof WHERE version=? AND file_id=?',(version,f['id']))
            for position,(kind,ref,c) in enumerate(proof_rows):
                if kind=='receipt':
                    db.execute('INSERT INTO assembled_proof(version,file_id,position,kind,proof_id) VALUES (?,?,?,?,?)',
                               (version,f['id'],position,'receipt',ref))
                else:
                    pid=db.execute('INSERT OR REPLACE INTO reuse_proof(plan_id,version,chunk_id,old_version,old_receipt_id,source_medium,source_root,source_path,sha256,size,status) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                        (plan_id,version,c['id'],ref['old_version'],ref['old_receipt_id'],ref['medium'],
                         ref['source_root'],ref['source_path'],c['sha256'],c['end']-c['start'],'verified')).lastrowid
                    db.execute('INSERT INTO assembled_proof(version,file_id,position,kind,proof_id) VALUES (?,?,?,?,?)',
                               (version,f['id'],position,'reuse',pid))
        made.append(str(target))
    db.close();return made

def seal(db_path,version,out):
    from . import reuse
    db=connect(db_path); outcomes={}
    with db:
        db.execute('DELETE FROM decision WHERE version=?',(version,))
        for f in db.execute('SELECT * FROM file_entry WHERE version=?',(version,)):
            path=Path(out)/f['path']
            if not path.is_file():outcomes[f['path']]='missing';continue
            digest=hashlib.sha256();size=0
            with path.open('rb') as stream:
                while part:=stream.read(1024*1024):digest.update(part);size+=len(part)
            if size!=f['size'] or digest.hexdigest()!=f['sha256']:
                outcomes[f['path']]='corrupt';continue
            proof=db.execute('SELECT * FROM assembled_file WHERE version=? AND file_id=?',(version,f['id'])).fetchone()
            required={c['id'] for c in db.execute('SELECT id FROM chunk WHERE file_id=?',(f['id'],))}
            chain=list(db.execute('SELECT * FROM assembled_proof WHERE version=? AND file_id=? ORDER BY position',(version,f['id'])))
            verified=set();proof_invalid=False
            for link in chain:
                if link['kind']=='receipt':
                    rows=db.execute("SELECT chunk_id FROM receipt WHERE id=? AND status='verified'",(link['proof_id'],)).fetchall()
                    verified.update(r['chunk_id'] for r in rows)
                else:
                    rp=db.execute('SELECT * FROM reuse_proof WHERE id=?',(link['proof_id'],)).fetchone()
                    ok,_=reuse.proof_current(db,rp) if rp else (False,None)
                    if not ok:proof_invalid=True
                    else:verified.add(rp['chunk_id'])
            outcomes[f['path']]='verified' if (proof and Path(proof['path']).resolve()==path.resolve()
                and proof['hash']==digest.hexdigest() and not proof_invalid
                and verified==required and len(chain)==len(required)) else 'unproven'
        status='可发布' if outcomes and all(x=='verified' for x in outcomes.values()) else ('拒收' if 'corrupt' in outcomes.values() else '证据不足')
        db.execute('INSERT INTO decision(version,result,detail) VALUES (?,?,?)',(version,status,json.dumps(outcomes,sort_keys=True)))
        db.execute('UPDATE delivery SET status=? WHERE id=(SELECT delivery_id FROM manifest WHERE version=?)',(status,version))
    db.close();return status

def report(db_path,version,path):
    db=connect(db_path); obj={'version':version,'manifest':[dict(r) for r in db.execute('SELECT path,size,sha256 FROM file_entry WHERE version=?',(version,))],
    'chunks':[dict(r) for r in db.execute('SELECT c.id,f.path,c.idx,c.start,c.end,c.sha256 FROM chunk c JOIN file_entry f ON f.id=c.file_id WHERE f.version=?',(version,))],
    'receipts':[dict(r) for r in db.execute('SELECT r.id,r.medium,r.path,r.actual_hash,r.status,a.source_hash FROM receipt r JOIN attempt a ON a.id=r.attempt_id WHERE a.version=?',(version,))],
    'issues':[dict(r) for r in db.execute('SELECT code,detail,receipt_id FROM issue WHERE version=?',(version,))],
    'decision':[dict(r) for r in db.execute('SELECT result,detail FROM decision WHERE version=?',(version,))],
    'assemblies':[dict(r) for r in db.execute('SELECT file_id,path,hash,receipt_ids FROM assembled_file WHERE version=?',(version,))],
    'custody':[dict(r) for r in db.execute('SELECT medium,seq,from_holder,to_holder,at,status FROM custody_event WHERE version=? ORDER BY medium,seq',(version,))],
    'acceptance':[dict(r) for r in db.execute('SELECT recipient_id,result,reason FROM acceptance WHERE version=? ORDER BY recipient_id',(version,))],
    'reuse_plans':[dict(r) for r in db.execute('SELECT id,version,contract,recipients,old_versions,frozen FROM reuse_plan WHERE version=?',(version,))],
    'reuse_proofs':[dict(r) for r in db.execute('SELECT id,plan_id,version,chunk_id,old_version,old_receipt_id,source_medium,source_path,sha256,size,status FROM reuse_proof WHERE version=?',(version,))],
    'proof_chain':[dict(r) for r in db.execute('SELECT version,file_id,position,kind,proof_id FROM assembled_proof WHERE version=? ORDER BY file_id,position',(version,))]}
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')
    path.with_suffix('.md').write_text('# 分段传验报告\n\n'+f"结论：{obj['decision'][0]['result'] if obj['decision'] else '未封验'}\n\n"+json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')
    db.close();return path
