import json
import pytest
from chunkproof import custody,engine,rollout
from chunkproof.store import connect

def sealed(tmp_path):
    src=tmp_path/'src';src.mkdir();(src/'f').write_bytes(b'abcdefgh')
    db=tmp_path/'db.sqlite';version=engine.manifest(db,'delivery',src,4)
    medium=tmp_path/'medium';medium.mkdir();data=(src/'f').read_bytes();rows=[]
    for chunk in engine.plan(db,version):
        rel=f"p{chunk['idx']}";(medium/rel).write_bytes(data[chunk['start']:chunk['end']])
        rows.append(json.dumps({'file':'f','path':rel,'index':chunk['idx']}))
    receipts=tmp_path/'r.jsonl';receipts.write_text('\n'.join(rows),encoding='utf-8')
    engine.receive(db,version,'M',medium,receipts)
    engine.assemble(db,version,medium,tmp_path/'out')
    assert engine.seal(db,version,tmp_path/'out')=='可发布'
    conn=connect(db);digest=conn.execute('SELECT body_hash FROM manifest WHERE version=?',(version,)).fetchone()[0];conn.close()
    return db,version,digest

def test_sealed_bytes_do_not_prove_custody_or_delivery(tmp_path):
    db,version,digest=sealed(tmp_path)
    custody.recipient(db,'R','2026-01-02T00:00:00Z')
    assert custody.decide(db,'R',version)['reason']=='custody_untrusted'
    custody.transfer(db,version,'M',1,'origin','carrier','2026-01-01T08:00:00Z')
    assert custody.decide(db,'R',version)['result']=='待回执'
    custody.acknowledge(db,'R',version,digest,'2026-01-01T09:00:00Z','accepted')
    assert custody.decide(db,'R',version)['result']=='已交付'
    report=json.loads(engine.report(db,version,tmp_path/'report.json').read_text(encoding='utf-8'))
    assert report['acceptance'][0]['result']=='已交付'

def test_custody_quarantine_and_chain_failure_change_acceptance(tmp_path):
    db,version,digest=sealed(tmp_path);custody.recipient(db,'R','2026-01-02T00:00:00Z')
    custody.transfer(db,version,'M',1,'origin','carrier','2026-01-01T08:00:00Z')
    with pytest.raises(ValueError,match='custody_chain_break'):
        custody.transfer(db,version,'M',3,'carrier','receiver','2026-01-01T08:20:00Z')
    custody.transfer(db,version,'M',2,'carrier','quarantine','2026-01-01T08:30:00Z')
    custody.acknowledge(db,'R',version,digest,'2026-01-01T09:00:00Z','accepted')
    assert custody.decide(db,'R',version)['reason']=='custody_untrusted'
    with pytest.raises(ValueError,match='medium_quarantined'):
        custody.transfer(db,version,'M',3,'quarantine','receiver','2026-01-01T08:40:00Z')
    with pytest.raises(ValueError,match='custody_identity_conflict'):
        custody.transfer(db,version,'M',2,'carrier','receiver','2026-01-01T08:30:00Z')

def test_recipient_dispute_and_deadline(tmp_path):
    db,version,digest=sealed(tmp_path)
    custody.transfer(db,version,'M',1,'origin','carrier','2026-01-01T08:00:00Z')
    custody.recipient(db,'R','2026-01-01T10:00:00Z')
    custody.acknowledge(db,'R',version,'wrong-hash','2026-01-01T09:00:00Z','accepted')
    assert custody.decide(db,'R',version)['result']=='争议'
    with pytest.raises(ValueError,match='ack_identity_conflict'):
        custody.acknowledge(db,'R',version,digest,'2026-01-01T09:00:00Z','accepted')
    custody.recipient(db,'R2','2026-01-01T10:00:00Z')
    custody.acknowledge(db,'R2',version,digest,'2026-01-01T11:00:00Z','accepted')
    assert custody.decide(db,'R2',version)['result']=='逾期'

def test_staged_weighted_rollout_requires_pilot_and_mandatory_consent(tmp_path):
    db,version,digest=sealed(tmp_path)
    custody.transfer(db,version,'M',1,'origin','carrier','2026-01-01T08:00:00Z')
    for name in ('pilot','major','minor'):
        custody.recipient(db,name,'2026-01-02T00:00:00Z')
    waves=[{'name':'pilot','quorum':1,'members':[{'recipient':'pilot','weight':1,'mandatory':True}]},
           {'name':'general','quorum':2,'members':[{'recipient':'major','weight':2,'mandatory':True},
                                                  {'recipient':'minor','weight':1}]}]
    assert rollout.create(db,'release',version,waves)==rollout.create(db,'release',version,waves)
    first=rollout.evaluate(db,'release')
    assert [x['status'] for x in first['waves']]==['等待','未开放']
    custody.acknowledge(db,'pilot',version,digest,'2026-01-01T09:00:00Z','accepted')
    assert [x['status'] for x in rollout.evaluate(db,'release')['waves']]==['完成','等待']
    custody.acknowledge(db,'major',version,digest,'2026-01-01T10:00:00Z','accepted')
    assert rollout.evaluate(db,'release')['status']=='已发布'
    assert connect(db).execute('SELECT count(*) FROM rollout_decision WHERE rollout_id=?',('release',)).fetchone()[0]==3
    with pytest.raises(ValueError,match='rollout_identity_conflict'):
        rollout.create(db,'release',version,waves[:1])

def test_rollout_aborts_on_mandatory_dispute_after_seal(tmp_path):
    db,version,digest=sealed(tmp_path)
    custody.transfer(db,version,'M',1,'origin','carrier','2026-01-01T08:00:00Z')
    custody.recipient(db,'pilot','2026-01-02T00:00:00Z')
    custody.acknowledge(db,'pilot',version,'wrong','2026-01-01T09:00:00Z','accepted')
    rollout.create(db,'release',version,[{'name':'pilot','quorum':1,
                                         'members':[{'recipient':'pilot','weight':1,'mandatory':True}]}])
    result=rollout.evaluate(db,'release')
    assert result['status']=='中止' and result['waves'][0]['evidence'][0]['result']=='争议'
