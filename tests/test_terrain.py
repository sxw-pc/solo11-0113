import json
from pathlib import Path

import pytest

from chunkproof import engine, terrain


FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures'


def sealed_grid(tmp_path, fixture):
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'terrain-delivery', fixture, 16)
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


@pytest.mark.parametrize('name,cells,runoff', [('terrain-branch', 3, 4),
                                              ('terrain-flat', 5, 5)])
def test_terrain_freezes_cells_without_routing(tmp_path, name, cells, runoff):
    fixture = FIXTURES / name
    db, version = sealed_grid(tmp_path, fixture)
    digest = terrain.register(db, 'terrain-1', version, fixture / 'spec.json')
    assert digest == terrain.register(db, 'terrain-1', version, fixture / 'spec.json')
    assert terrain.inspect(db, 'terrain-1') == {
        'cells': cells, 'raw_runoff': runoff, 'outlets': 2}
    with pytest.raises(ValueError, match='terrain_identity_conflict'):
        terrain.register(db, 'terrain-1', 'other-version', fixture / 'spec.json')


def test_terrain_rejects_changed_assembled_bytes(tmp_path):
    fixture = FIXTURES / 'terrain-branch'
    db, version = sealed_grid(tmp_path, fixture)
    (tmp_path / 'out' / 'cells.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='terrain_bytes_changed'):
        terrain.register(db, 'terrain-1', version, fixture / 'spec.json')
