import json
from pathlib import Path

import pytest

from chunkproof import engine, fieldgrid


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'field-grid'


def sealed_grid(tmp_path, fixture=FIXTURE):
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'grid-delivery', fixture, 16)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
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


def test_grid_freezes_sealed_measurements(tmp_path):
    db, version = sealed_grid(tmp_path)
    digest = fieldgrid.register(db, 'grid-1', version, FIXTURE / 'spec.json')
    assert digest == fieldgrid.register(db, 'grid-1', version, FIXTURE / 'spec.json')
    assert fieldgrid.inspect(db, 'grid-1') == {
        'raw_measurements': 8, 'positions': 4, 'overlaps': 4}
    with pytest.raises(ValueError, match='grid_identity_conflict'):
        fieldgrid.register(db, 'grid-1', 'other-version', FIXTURE / 'spec.json')


def test_grid_rejects_changed_assembled_bytes(tmp_path):
    db, version = sealed_grid(tmp_path)
    (tmp_path / 'out' / 'a.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='grid_bytes_changed'):
        fieldgrid.register(db, 'grid-1', version, FIXTURE / 'spec.json')


def test_forced_source_coverage_is_registered_without_mosaicking(tmp_path):
    fixture = FIXTURE.parent / 'field-grid-forced'
    db, version = sealed_grid(tmp_path, fixture)
    fieldgrid.register(db, 'forced', version, fixture / 'spec.json')
    assert fieldgrid.inspect(db, 'forced') == {
        'raw_measurements': 8, 'positions': 5, 'overlaps': 3}
