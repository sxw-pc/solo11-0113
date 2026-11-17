import json

import pytest

from chunkproof import engine, custody, rollout, reuse
from chunkproof.store import connect


def deliver(tmp, name, data, block=4):
    """合成一次完整交付：清单、介质、回执、组装、封验。返回台账与关键句柄。"""
    src = tmp / f'{name}-src'
    src.mkdir(parents=True, exist_ok=True)
    (src / 'f').write_bytes(data)
    db = tmp / 'ledger.sqlite'
    version = engine.manifest(db, f'delivery-{name}', src, block)
    medium = tmp / f'{name}-medium'
    medium.mkdir(parents=True, exist_ok=True)
    rows = []
    for c in engine.plan(db, version):
        rel = f"p{c['idx']}"
        (medium / rel).write_bytes(data[c['start']:c['end']])
        rows.append({'file': 'f', 'path': rel, 'index': c['idx'], 'sha256': c['sha256']})
    receipt = tmp / f'{name}.jsonl'
    receipt.write_text('\n'.join(json.dumps(x) for x in rows), encoding='utf-8')
    engine.receive(db, version, f'M-{name}', medium, receipt)
    out = tmp / f'{name}-out'
    engine.assemble(db, version, medium, out)
    assert engine.seal(db, version, out) == '可发布'
    return {'db': db, 'version': version, 'medium': medium, 'out': out, 'data': data}


def trust(db, version, medium, seq, origin='origin', destination='carrier', at='2026-01-01T08:00:00Z'):
    return custody.transfer(db, version, medium, seq, origin, destination, at)


def new_delivery(tmp, data, block=4):
    src = tmp / 'new-src'
    src.mkdir(parents=True, exist_ok=True)
    (src / 'f').write_bytes(data)
    db = tmp / 'ledger.sqlite'
    version = engine.manifest(db, 'delivery-new', src, block)
    return db, version, src


def receive_some(db, version, medium, rows, tmp, name):
    medium.mkdir(parents=True, exist_ok=True)
    receipt = tmp / name
    receipt.write_text('\n'.join(json.dumps(x) for x in rows), encoding='utf-8')
    engine.receive(db, version, f'M-{name}', medium, receipt)


def test_same_hash_missing_block_two_contracts_diverge(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    strict = reuse.reuse_plan(db, v2, reuse.STRICT, ['R1'])
    content = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    # 同一缺块输入，两份独立合同给出不同结论
    assert strict['still_missing'] == ['f:0', 'f:1', 'f:2']
    assert strict['reusable'] == 0
    assert content['still_missing'] == []
    assert content['reusable'] == 3
    assert strict['evidence_scope'] != content['evidence_scope']
    cand = next(i for i in content['items'] if i['key'] == 'f:0')['eligible']
    assert cand['old_version'] == old['version'] and cand['medium'] == 'M-old'
    # dry-run 只读：台账中没有任何计划或复用证明
    conn = connect(db)
    assert conn.execute('SELECT count(*) FROM reuse_plan').fetchone()[0] == 0
    assert conn.execute('SELECT count(*) FROM reuse_proof').fetchone()[0] == 0
    conn.close()
    # 新版本普通 receive/reconcile 流程仍然可用
    assert all(v is None for v in engine.reconcile(db, v2).values())


def test_strict_contract_accepts_new_receipt_and_ignores_old_bytes(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    # 新版本介质只送来块 0
    m2 = tmp_path / 'new-medium'
    m2.mkdir()
    (m2 / 'p0').write_bytes(b'abcd')
    receive_some(db, v2, m2, [{'file': 'f', 'path': 'p0', 'index': 0}], tmp_path, 'new.jsonl')
    strict = reuse.reuse_plan(db, v2, reuse.STRICT, ['R1'])
    assert strict['reusable'] == 1
    assert set(strict['still_missing']) == {'f:1', 'f:2'}
    reuse.freeze_plan(db, strict)
    assert engine.assemble(db, v2, m2, tmp_path / 'new-out', strict) == []
    assert engine.seal(db, v2, tmp_path / 'new-out') == '证据不足'


def test_collision_same_digest_but_different_half_open_interval_rejected(tmp_path, monkeypatch):
    # 摘要碰巧相同但半开区间不同：伪造哈希让 (0,2) 与 (0,4) 摘要一致
    real_sha = engine.sha
    fake = {b'ab': 'deadbeef' * 8, b'abcd': 'deadbeef' * 8}
    monkeypatch.setattr(engine, 'sha', lambda data: fake.get(data, real_sha(data)))
    old = deliver(tmp_path, 'old', b'abXYZ', block=2)
    db = tmp_path / 'ledger.sqlite'
    src = tmp_path / 'new-src'
    src.mkdir()
    (src / 'f').write_bytes(b'abcdZZ')
    v2 = engine.manifest(db, 'delivery-new', src, 4)
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    content = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    item = next(i for i in content['items'] if i['key'] == 'f:0')
    assert item['conclusion'] == 'still_missing'
    assert any(x['code'] == 'no_matching_old_chunk' for x in item['excluded'])


def test_source_single_byte_tamper_blocks_plan_and_assembly(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    # 计划生成前旧介质块 0 被单字节篡改
    (old['medium'] / 'p0').write_bytes(b'xbcd')
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    item = next(i for i in content_items(plan) if i['key'] == 'f:0')
    assert item['conclusion'] == 'still_missing'
    assert any(x['code'] == 'source_bytes_changed' for x in item['excluded'])
    assert plan['still_missing'] == ['f:0']
    # 冻结后、组装前再篡改另一块：计划冻结时的候选已变化，新鲜度闸门先拦截
    (old['medium'] / 'p0').write_bytes(b'abcd')
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    reuse.freeze_plan(db, plan)
    (old['medium'] / 'p1').write_bytes(b'xefg')
    with pytest.raises(ValueError, match='plan_stale'):
        engine.assemble(db, v2, tmp_path / 'new-medium', tmp_path / 'new-out', plan)


def test_assemble_reverifies_every_selected_source_byte(tmp_path, monkeypatch):
    # 冻结后来源字节变化，但让新鲜度比对看到的计划仍一致（模拟绕过计划重算的陈旧冻结）：
    # assemble 自身必须对每个被选来源重新验字节，而不是信任冻结快照。
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    reuse.freeze_plan(db, plan)
    (old['medium'] / 'p1').write_bytes(b'xefg')
    import chunkproof.reuse as reuse_mod
    monkeypatch.setattr(reuse_mod, 'reuse_plan', lambda *a, **k: plan)
    with pytest.raises(ValueError, match='reuse_source_changed'):
        engine.assemble(db, v2, tmp_path / 'new-medium', tmp_path / 'new-out', plan)
    conn = connect(db)
    assert conn.execute('SELECT count(*) FROM reuse_proof').fetchone()[0] == 0
    conn.close()


def content_items(plan):
    return plan['items']


def test_medium_quarantine_removes_candidate_and_blocks_seal(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    assert plan['reusable'] == 3
    # 交接后隔离介质：custody 变化必须驱动候选集合，计划结论整体改变
    custody.transfer(db, old['version'], 'M-old', 2, 'carrier', 'quarantine',
                     '2026-01-01T09:00:00Z')
    plan2 = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    assert plan2['plan_id'] != plan['plan_id']
    assert plan2['still_missing'] == ['f:0', 'f:1', 'f:2']
    assert any(x['code'] == 'medium_quarantined' for i in plan2['items'] for x in i['excluded'])
    # 隔离介质不可再交接；新介质恢复可信后才能用原冻结计划组装，组装后再隔离仍须阻断
    with pytest.raises(ValueError, match='medium_quarantined'):
        custody.transfer(db, old['version'], 'M-old', 3, 'quarantine', 'carrier2',
                         '2026-01-01T09:30:00Z')


def test_quarantine_after_assembly_drives_seal_and_acceptance(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    reuse.freeze_plan(db, plan)
    out = tmp_path / 'new-out'
    engine.assemble(db, v2, tmp_path / 'new-medium', out, plan)
    assert engine.seal(db, v2, out) == '可发布'
    # 组装封验之后介质才被隔离：结论必须随 custody 变化，不得沿用旧的可发布结论
    custody.transfer(db, old['version'], 'M-old', 2, 'carrier', 'quarantine',
                     '2026-01-01T09:00:00Z')
    assert engine.seal(db, v2, out) == '证据不足'
    # 封验闸门先生效；恢复可信后，复用保管链证据仍须被独立复核
    verdict = custody.decide(db, 'R1', v2)
    assert verdict['result'] == '待核' and verdict['reason'] == 'unsealed'


def test_recipient_scope_difference_excludes_old_chunk(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    custody.recipient(db, 'R2', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    # 旧版只面向 R1，新版范围是 R2 —— 属于另一收件范围
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R2'])
    assert plan['still_missing'] == ['f:0', 'f:1', 'f:2']
    assert any(x['code'] == 'recipient_scope_mismatch' for i in plan['items'] for x in i['excluded'])
    # 范围一致后候选恢复
    plan2 = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    assert plan2['still_missing'] == []


def test_mixed_reuse_and_new_receipt_assemble_and_publish(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    # 新版本只接收块 1；块 0、2 借用旧版
    m2 = tmp_path / 'new-medium'
    m2.mkdir()
    (m2 / 'p1').write_bytes(b'efgh')
    receive_some(db, v2, m2, [{'file': 'f', 'path': 'p1', 'index': 1}], tmp_path, 'new.jsonl')
    trust(db, v2, 'M-new.jsonl', 1, destination='receiver', at='2026-02-01T08:00:00Z')
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    assert plan['reusable'] == 3  # 块 1 有新回执，块 0/2 有复用候选
    assert plan['still_missing'] == []
    reuse.freeze_plan(db, plan)
    out = tmp_path / 'new-out'
    made = engine.assemble(db, v2, m2, out, plan)
    assert made == [str(out / 'f')]
    assert (out / 'f').read_bytes() == b'abcdefghij'
    assert engine.seal(db, v2, out) == '可发布'
    conn = connect(db)
    kinds = [r['kind'] for r in conn.execute(
        'SELECT kind FROM assembled_proof WHERE version=? ORDER BY position', (v2,))]
    conn.close()
    assert kinds == ['reuse', 'receipt', 'reuse']
    # 复用证明不伪造新版本回执：新版本 receipt 仍只有块 1
    conn = connect(db)
    assert conn.execute(
        "SELECT count(*) FROM receipt r JOIN attempt a ON a.id=r.attempt_id "
        "WHERE a.version=? AND r.status='verified'", (v2,)).fetchone()[0] == 1
    conn.close()
    body_hash = connect(db).execute('SELECT body_hash FROM manifest WHERE version=?', (v2,)).fetchone()[0]
    custody.acknowledge(db, 'R1', v2, body_hash, '2026-02-01T09:00:00Z', 'accepted')
    rollout.create(db, 'new-roll', v2,
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    result = rollout.evaluate(db, 'new-roll')
    assert result['status'] == '已发布'
    report = json.loads(engine.report(db, v2, tmp_path / 'report.json').read_text(encoding='utf-8'))
    assert len(report['reuse_proofs']) == 2
    assert report['reuse_plans'][0]['contract'] == reuse.CONTENT


def test_old_seal_not_publishable_excludes_candidate(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    conn = connect(db)
    conn.execute("UPDATE decision SET result='证据不足' WHERE version=?", (old['version'],))
    conn.commit()
    conn.close()
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    assert plan['still_missing'] == ['f:0', 'f:1', 'f:2']
    assert any(x['code'] == 'old_seal_not_publishable' for i in plan['items'] for x in i['excluded'])


def test_proof_invalidated_after_assembly_blocks_seal_and_wave(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    reuse.freeze_plan(db, plan)
    out = tmp_path / 'new-out'
    engine.assemble(db, v2, tmp_path / 'new-medium', out, plan)
    assert engine.seal(db, v2, out) == '可发布'
    rollout.create(db, 'new-roll', v2,
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    body_hash = connect(db).execute('SELECT body_hash FROM manifest WHERE version=?', (v2,)).fetchone()[0]
    custody.acknowledge(db, 'R1', v2, body_hash, '2026-02-01T09:00:00Z', 'accepted')
    assert rollout.evaluate(db, 'new-roll')['status'] == '已发布'
    # 旧来源单字节篡改：复用证明失效，重新封验与波次都被阻断
    (old['medium'] / 'p2').write_bytes(b'ix')
    assert engine.seal(db, v2, out) == '证据不足'
    result = rollout.evaluate(db, 'new-roll')
    assert result['status'] != '已发布'
    assert result['waves'][0]['evidence'][0]['result'] == '待核'


def test_assemble_requires_frozen_and_current_plan(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    with pytest.raises(ValueError, match='plan_not_frozen'):
        engine.assemble(db, v2, tmp_path / 'new-medium', tmp_path / 'new-out', plan)
    reuse.freeze_plan(db, plan)
    custody.transfer(db, old['version'], 'M-old', 2, 'carrier', 'quarantine',
                     '2026-01-01T09:00:00Z')
    with pytest.raises(ValueError, match='plan_stale'):
        engine.assemble(db, v2, tmp_path / 'new-medium', tmp_path / 'new-out', plan)


def test_plan_input_validation(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    with pytest.raises(ValueError, match='unknown_contract'):
        reuse.reuse_plan(db, v2, 'bogus', ['R1'])
    with pytest.raises(ValueError, match='empty_recipient_scope'):
        reuse.reuse_plan(db, v2, reuse.CONTENT, [])
    with pytest.raises(ValueError, match='unknown_recipient'):
        reuse.reuse_plan(db, v2, reuse.CONTENT, ['nobody'])
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    with pytest.raises(ValueError, match='unknown_old_version'):
        reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'], old_versions=[v2])
    with pytest.raises(ValueError, match='unknown_old_version'):
        reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'], old_versions=['nope'])


def test_immutable_old_artifacts_and_normal_flow_unchanged(tmp_path):
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    conn = connect(db)
    before = {'manifest': conn.execute('SELECT count(*) FROM manifest').fetchone()[0],
              'receipt': conn.execute('SELECT count(*) FROM receipt').fetchone()[0],
              'decision': conn.execute('SELECT result FROM decision WHERE version=?',
                                       (old['version'],)).fetchone()[0]}
    conn.close()
    old_bytes = {p.name: p.read_bytes() for p in old['medium'].iterdir()}
    plan = reuse.reuse_plan(db, v2, reuse.CONTENT, ['R1'])
    reuse.freeze_plan(db, plan)
    engine.assemble(db, v2, tmp_path / 'new-medium', tmp_path / 'new-out', plan)
    engine.seal(db, v2, tmp_path / 'new-out')
    # 旧 manifest、回执数量与旧版封验结论不改写，原始块文件逐字节不变
    conn = connect(db)
    assert conn.execute('SELECT count(*) FROM manifest').fetchone()[0] == before['manifest']
    assert conn.execute('SELECT count(*) FROM receipt').fetchone()[0] == before['receipt']
    assert conn.execute('SELECT result FROM decision WHERE version=?',
                        (old['version'],)).fetchone()[0] == before['decision']
    conn.close()
    assert {p.name: p.read_bytes() for p in old['medium'].iterdir()} == old_bytes


def test_cli_reuse_plan_and_assemble(tmp_path, monkeypatch, capsys):
    from chunkproof import __main__ as cli
    old = deliver(tmp_path, 'old', b'abcdefghij')
    db, v2, _ = new_delivery(tmp_path, b'abcdefghij')
    custody.recipient(db, 'R1', '2026-02-02T00:00:00Z')
    trust(db, old['version'], 'M-old', 1)
    rollout.create(db, 'old-roll', old['version'],
                   [{'name': 'w', 'quorum': 1,
                     'members': [{'recipient': 'R1', 'weight': 1, 'mandatory': True}]}])
    plan_file = tmp_path / 'plan.json'
    monkeypatch.setattr('sys.argv', ['chunkproof', 'reuse-plan', '--db', str(db),
                                     '--version', v2, '--contract', reuse.CONTENT,
                                     '--recipient', 'R1', '--freeze'])
    cli.main()
    plan = json.loads(capsys.readouterr().out)
    plan_file.write_text(json.dumps(plan), encoding='utf-8')
    assert plan['reusable'] == 3
    monkeypatch.setattr('sys.argv', ['chunkproof', 'assemble', '--db', str(db),
                                     '--version', v2, '--root', str(tmp_path / 'new-medium'),
                                     '--out', str(tmp_path / 'new-out'), '--plan', str(plan_file)])
    cli.main()
    assert (tmp_path / 'new-out' / 'f').read_bytes() == b'abcdefghij'
    monkeypatch.setattr('sys.argv', ['chunkproof', 'seal', '--db', str(db),
                                     '--version', v2, '--out', str(tmp_path / 'new-out')])
    cli.main()
    assert '可发布' in capsys.readouterr().out
