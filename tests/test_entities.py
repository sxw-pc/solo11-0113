import json
from pathlib import Path

import pytest

from chunkproof import engine, entities


FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures'


def sealed_feed(tmp_path):
    root = FIXTURES / 'entity-feed'
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'entity-delivery', root, 16)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (root / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
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


def test_entity_feed_indexes_sealed_rows_and_links(tmp_path):
    db, version = sealed_feed(tmp_path)
    spec = FIXTURES / 'entity-feed-spec.json'
    digest = entities.register(db, 'population-1', version, spec)
    assert digest == entities.register(db, 'population-1', version, spec)
    assert entities.inspect(db, 'population-1') == {
        'source_rows': {'north': 2, 'south': 2}, 'raw_rows': 4, 'candidate_links': 3}
    with pytest.raises(ValueError, match='feed_identity_conflict'):
        entities.register(db, 'population-1', 'other-version', spec)


def test_entity_feed_rejects_changed_assembled_bytes(tmp_path):
    db, version = sealed_feed(tmp_path)
    (tmp_path / 'out' / 'north.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='feed_bytes_changed'):
        entities.register(db, 'population-1', version, FIXTURES / 'entity-feed-spec.json')
