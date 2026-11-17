import json
from pathlib import Path

import pytest

from chunkproof import engine, latency


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'latency'


def sealed_batch(tmp_path):
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'latency-delivery', FIXTURE, 16)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (FIXTURE / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'part-{index}'
        (media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / 'receipts.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M1', media, receipt_path)
    engine.assemble(db, version, media, tmp_path / 'out')
    assert engine.seal(db, version, tmp_path / 'out') == '可发布'
    return db, version


def test_exact_small_batch_and_current_memory_limit(tmp_path):
    db, version = sealed_batch(tmp_path)
    digest = latency.register(db, 'latency-1', version, FIXTURE / 'spec.json')
    assert digest == latency.register(db, 'latency-1', version, FIXTURE / 'spec.json')
    assert latency.evaluate(db, 'latency-1', 80) == {
        'samples': 5, 'p95_ms': 100, 'sla': 'fail'}
    with pytest.raises(ValueError, match='latency_memory_limit'):
        latency.evaluate(db, 'latency-1', 80, max_rows=4)
    with pytest.raises(ValueError, match='latency_identity_conflict'):
        latency.register(db, 'latency-1', 'other-version', FIXTURE / 'spec.json')


def test_rejects_changed_assembled_bytes(tmp_path):
    db, version = sealed_batch(tmp_path)
    latency.register(db, 'latency-1', version, FIXTURE / 'spec.json')
    (tmp_path / 'out' / 'latency-a.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='latency_bytes_changed'):
        latency.evaluate(db, 'latency-1', 80)
