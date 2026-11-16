import json
import pytest
from chunkproof import engine

def setup(tmp_path,data=b'abcdefghij'):
    src=tmp_path/'src';src.mkdir();(src/'f').write_bytes(data)
    db=tmp_path/'db.sqlite';v=engine.manifest(db,'d',src,4);return src,db,v

def test_manifest_plan(tmp_path):
    src,db,v=setup(tmp_path)
    assert v==engine.manifest(db,'d',src,4)
    assert [(r['start'],r['end']) for r in engine.plan(db,v)]==[(0,4),(4,8),(8,10)]

def test_empty_file(tmp_path):
    src,db,v=setup(tmp_path,b'')
    assert engine.plan(db,v)==[]

def test_path_rejection():
    for x in ('../a','/a','a\\b','C:bad'):
        with pytest.raises(ValueError):engine.safe_path(x)

def test_receive_corrupt_and_duplicate(tmp_path):
    src,db,v=setup(tmp_path);media=tmp_path/'media';media.mkdir();(media/'f').write_bytes(b'wrong')
    receipt=tmp_path/'r.jsonl';receipt.write_text(json.dumps({'file':'f','path':'f','index':0}),encoding='utf-8')
    assert engine.receive(db,v,'m',media,receipt)==1
    assert engine.receive(db,v,'m',media,receipt)==0
    assert engine.reconcile(db,v)['f:0'] is None

def test_report_unknown(tmp_path):
    src,db,v=setup(tmp_path)
    assert engine.seal(db,v,tmp_path/'out')=='证据不足'
    obj=json.loads(engine.report(db,v,tmp_path/'r.json').read_text(encoding='utf-8'))
    assert obj['decision'][0]['result']=='证据不足'

def test_full_delivery_and_atomic_assembly(tmp_path):
    src,db,v=setup(tmp_path);media=tmp_path/'media';media.mkdir();rows=[]
    data=(src/'f').read_bytes()
    for c in engine.plan(db,v):
        rel=f"part{c['idx']}";(media/rel).write_bytes(data[c['start']:c['end']])
        rows.append({'file':'f','path':rel,'index':c['idx'],'sha256':c['sha256']})
    receipt=tmp_path/'receipt.jsonl';receipt.write_text('\n'.join(json.dumps(x) for x in rows),encoding='utf-8')
    assert engine.receive(db,v,'m1',media,receipt)==3
    assert all(engine.reconcile(db,v).values())
    out=tmp_path/'out';assert engine.assemble(db,v,media,out)==[str(out/'f')]
    assert (out/'f').read_bytes()==data
    assert engine.seal(db,v,out)=='可发布'
    assert (tmp_path/'out'/'f.part').exists() is False

def test_wrong_version_and_missing_block(tmp_path):
    src,db,v=setup(tmp_path);media=tmp_path/'media';media.mkdir()
    receipt=tmp_path/'receipt.jsonl';receipt.write_text(json.dumps({'file':'f','path':'missing','index':99}),encoding='utf-8')
    engine.receive(db,v,'m',media,receipt)
    assert engine.reconcile(db,v)['f:0'] is None
    obj=json.loads(engine.report(db,v,tmp_path/'report.json').read_text(encoding='utf-8'))
    assert obj['issues'][0]['code']=='missing'

def test_corrupt_candidate_cannot_publish(tmp_path):
    src,db,v=setup(tmp_path);media=tmp_path/'media';media.mkdir();(media/'part').write_bytes(b'xxx')
    receipt=tmp_path/'receipt.jsonl';receipt.write_text(json.dumps({'file':'f','path':'part','index':0}),encoding='utf-8')
    engine.receive(db,v,'m',media,receipt)
    assert engine.assemble(db,v,media,tmp_path/'out')==[]
    assert engine.seal(db,v,tmp_path/'out')=='证据不足'

def test_output_conflict_preserves_existing(tmp_path):
    src,db,v=setup(tmp_path,b'');out=tmp_path/'out';out.mkdir();(out/'f').write_bytes(b'other')
    with pytest.raises(ValueError,match='output_conflict'):engine.assemble(db,v,tmp_path,out)
    assert (out/'f').read_bytes()==b'other'

def test_reception_transaction_rollback_on_unsafe_path(tmp_path):
    src,db,v=setup(tmp_path);media=tmp_path/'media';media.mkdir()
    receipt=tmp_path/'bad.jsonl';receipt.write_text('\n'.join([json.dumps({'file':'f','path':'missing','index':0}),json.dumps({'file':'f','path':'../escape','index':1})]),encoding='utf-8')
    with pytest.raises(ValueError):engine.receive(db,v,'m',media,receipt)
    from chunkproof.store import connect
    dbh=connect(db)
    assert dbh.execute('SELECT count(*) FROM receipt').fetchone()[0]==0
    assert dbh.execute('SELECT count(*) FROM attempt').fetchone()[0]==0

def test_reverify_detects_media_changed_after_receipt(tmp_path):
    src,db,v=setup(tmp_path);media=tmp_path/'media';media.mkdir();(media/'part').write_bytes(b'abcd')
    receipt=tmp_path/'receipt.jsonl';receipt.write_text(json.dumps({'file':'f','path':'part','index':0}),encoding='utf-8')
    engine.receive(db,v,'m',media,receipt)
    assert engine.verify_chunks(db,v,'m',media)[0][1]=='verified'
    (media/'part').write_bytes(b'xxxx')
    assert engine.verify_chunks(db,v,'m',media)[0][1]=='corrupt'
    assert engine.reconcile(db,v)['f:0'] is None

def test_multiple_media_selects_verified_candidate(tmp_path):
    src,db,v=setup(tmp_path);bad=tmp_path/'bad';good=tmp_path/'good';bad.mkdir();good.mkdir()
    (bad/'p').write_bytes(b'xxxx');(good/'p').write_bytes(b'abcd')
    one=tmp_path/'one.jsonl';two=tmp_path/'two.jsonl'
    one.write_text(json.dumps({'file':'f','path':'p','index':0,'medium':'bad'}))
    two.write_text(json.dumps({'file':'f','path':'p','index':0,'medium':'good'}))
    engine.receive(db,v,'bad',bad,one);engine.receive(db,v,'good',good,two)
    assert engine.reconcile(db,v)['f:0'] is not None
