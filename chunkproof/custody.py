"""Custody qualification and recipient acceptance after byte-level sealing."""
from datetime import datetime
from .store import connect

def instant(value):
    t=datetime.fromisoformat(value.replace('Z','+00:00'))
    if t.tzinfo is None:raise ValueError('timezone_required')
    return t

def transfer(path,version,medium,seq,origin,destination,at):
    instant(at)
    if not origin or not destination or origin==destination:raise ValueError('holder_transition')
    db=connect(path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM manifest WHERE version=?',(version,)).fetchone():raise ValueError('unknown_version')
            old=db.execute('SELECT * FROM custody_event WHERE medium=? AND seq=?',(medium,seq)).fetchone()
            if old:
                if (old['version'],old['from_holder'],old['to_holder'],old['at'])!=(version,origin,destination,at):raise ValueError('custody_identity_conflict')
                return old['status']
            prev=db.execute('SELECT * FROM custody_event WHERE medium=? ORDER BY seq DESC LIMIT 1',(medium,)).fetchone()
            if prev and prev['status']=='quarantined':raise ValueError('medium_quarantined')
            if prev and (seq!=prev['seq']+1 or origin!=prev['to_holder'] or instant(at)<instant(prev['at'])):raise ValueError('custody_chain_break')
            if not prev and (seq!=1 or origin!='origin'):raise ValueError('custody_origin_missing')
            status='quarantined' if destination=='quarantine' else 'trusted'
            db.execute('INSERT INTO custody_event(medium,seq,from_holder,to_holder,at,version,status) VALUES (?,?,?,?,?,?,?)',(medium,seq,origin,destination,at,version,status))
            return status
    finally:db.close()

def recipient(path,name,deadline):
    instant(deadline);db=connect(path)
    try:
        with db:
            old=db.execute('SELECT deadline FROM recipient WHERE id=?',(name,)).fetchone()
            if old and old['deadline']!=deadline:raise ValueError('recipient_contract_conflict')
            db.execute('INSERT OR IGNORE INTO recipient VALUES (?,?)',(name,deadline))
    finally:db.close()

def acknowledge(path,name,version,manifest_hash,at,claim):
    instant(at)
    if claim not in {'accepted','rejected'}:raise ValueError('bad_claim')
    db=connect(path)
    try:
        with db:
            old=db.execute('SELECT * FROM acknowledgement WHERE recipient_id=? AND version=? AND at=?',(name,version,at)).fetchone()
            if old:
                if old['manifest_hash']!=manifest_hash or old['claim']!=claim:raise ValueError('ack_identity_conflict')
                return old['id']
            return db.execute('INSERT INTO acknowledgement(recipient_id,version,manifest_hash,at,claim) VALUES (?,?,?,?,?)',(name,version,manifest_hash,at,claim)).lastrowid
    finally:db.close()

def decide(path,name,version):
    db=connect(path)
    try:
        with db:
            recipient=db.execute('SELECT * FROM recipient WHERE id=?',(name,)).fetchone()
            manifest=db.execute('SELECT body_hash FROM manifest WHERE version=?',(version,)).fetchone()
            if not recipient or not manifest:raise ValueError('unknown_contract')
            seal=db.execute('SELECT result FROM decision WHERE version=?',(version,)).fetchone()
            acks=list(db.execute('SELECT * FROM acknowledgement WHERE recipient_id=? AND version=? ORDER BY at,id',(name,version)))
            receipt_media=[r['medium'] for r in db.execute(
                'SELECT DISTINCT r.medium FROM assembled_file f JOIN json_each(f.receipt_ids) j JOIN receipt r ON r.id=j.value WHERE f.version=?',(version,))]
            reuse_rows=list(db.execute(
                'SELECT DISTINCT rp.id AS proof_id FROM reuse_proof rp JOIN assembled_proof ap '
                "ON ap.kind='reuse' AND ap.proof_id=rp.id WHERE rp.version=?",(version,)))
            statuses=[db.execute('SELECT status,version FROM custody_event WHERE medium=? ORDER BY seq DESC LIMIT 1',(m,)).fetchone() for m in receipt_media]
            if not seal or seal['result']!='可发布':result,reason='待核','unsealed'
            elif not receipt_media and not reuse_rows:result,reason='待核','custody_untrusted'
            elif any(not x or x['version']!=version or x['status']!='trusted' for x in statuses):result,reason='待核','custody_untrusted'
            else:
                from .reuse import proof_current
                reuse_bad=False
                for row in reuse_rows:
                    rp=db.execute('SELECT * FROM reuse_proof WHERE id=?',(row['proof_id'],)).fetchone()
                    ok,_=proof_current(db,rp) if rp else (False,None)
                    if not ok:reuse_bad=True;break
                if reuse_bad:result,reason='待核','reuse_custody_untrusted'
                elif any(x['manifest_hash']!=manifest['body_hash'] for x in acks):result,reason='争议','manifest_mismatch'
                elif any(x['claim']=='rejected' for x in acks):result,reason='拒收','recipient_rejected'
                elif not acks:result,reason='待回执','ack_missing'
                elif instant(acks[-1]['at'])>instant(recipient['deadline']):result,reason='逾期','after_deadline'
                else:result,reason='已交付','verified_acceptance'
            db.execute('INSERT INTO acceptance VALUES (?,?,?,?) ON CONFLICT(recipient_id,version) DO UPDATE SET result=excluded.result,reason=excluded.reason',(name,version,result,reason))
            return {'result':result,'reason':reason}
    finally:db.close()
