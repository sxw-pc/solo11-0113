import json
from pathlib import Path

import pytest

from chunkproof import changefeed, engine


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'changefeed'


def sealed_run(tmp_path):
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'changefeed-delivery', FIXTURE, 16)
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


def test_changefeed_freezes_transactions_and_reports_incomplete(tmp_path):
    db, version = sealed_run(tmp_path)
    digest = changefeed.register(db, 'feed-1', version, FIXTURE / 'spec.json')
    assert digest == changefeed.register(db, 'feed-1', version, FIXTURE / 'spec.json')
    assert changefeed.inspect(db, 'feed-1') == {
        'transactions': 7, 'rows': 8, 'incomplete': ['t5']}
    with pytest.raises(ValueError, match='changefeed_identity_conflict'):
        changefeed.register(db, 'feed-1', 'other-version', FIXTURE / 'spec.json')


def test_changefeed_rejects_changed_assembled_bytes(tmp_path):
    db, version = sealed_run(tmp_path)
    (tmp_path / 'out' / 'rows.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='changefeed_bytes_changed'):
        changefeed.register(db, 'feed-1', version, FIXTURE / 'spec.json')
