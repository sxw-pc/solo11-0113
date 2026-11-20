import json
import time
from pathlib import Path

import pytest

from chunkproof import engine, terrain
from chunkproof.store import connect


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


def prepared(tmp_path, fixture, grid_id='terrain-1'):
    path = Path(fixture)
    if not path.is_absolute():
        path = FIXTURES / fixture
    db, version = sealed_grid(tmp_path, path)
    terrain.register(db, grid_id, version, path / 'spec.json')
    return db, version


def amount(fraction):
    return fraction['num'] / fraction['den']


def outlet_loads(result):
    return {name: amount(item['received']) for name, item in result['outlets'].items()}


def region_named(result, region_id):
    return next(item for item in result['regions'] if item['id'] == region_id)


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


# ---------------------------------------------------------------------------
# 1. Two legal models disagree on outlet loads for the same frozen evidence
# ---------------------------------------------------------------------------

def test_single_and_multiple_flow_disagree_on_branch(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    single = terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-single')
    multiple = terrain.route(db, 'terrain-1', 'multiple_flow', {'L': 3}, 'route-multiple')
    assert single['cells_hash'] == multiple['cells_hash']
    assert single['version'] == multiple['version']
    assert outlet_loads(single) == {'L': 3, 'R': 1}
    assert outlet_loads(multiple) == {'L': 2, 'R': 2}
    assert single['outlets']['L']['passed'] is True
    assert multiple['outlets']['L']['passed'] is False
    assert single['model'] == 'single_flow' and multiple['model'] == 'multiple_flow'
    assert single['conservation']['balanced'] and multiple['conservation']['balanced']


def test_flat_region_coalesces_and_splits_exactly(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-flat')
    single = terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-single')
    multiple = terrain.route(db, 'terrain-1', 'multiple_flow', {'L': 3}, 'route-multiple')
    assert outlet_loads(single) == {'L': 4, 'R': 1}
    assert outlet_loads(multiple) == {'L': 2.5, 'R': 2.5}
    assert multiple['outlets']['L']['received'] == {'num': 5, 'den': 2}
    assert multiple['outlets']['R']['received'] == {'num': 5, 'den': 2}
    # the three equal-height cells form exactly one plateau, not three flows
    plateau = [item for item in multiple['regions'] if item['height'] == 2]
    assert len(plateau) == 1
    assert sorted(map(tuple, plateau[0]['cells'])) == [(1, 0), (2, 0), (3, 0)]
    assert plateau[0]['runoff'] == {'num': 3, 'den': 1}
    split = plateau[0]['split']
    assert split['kind'] == 'multiple' and split['denominator'] == 2
    assert {target['target'] for target in split['targets']} == {'0:0', '0:4'}
    assert len(single['regions']) == len(multiple['regions'])


def test_single_flow_tie_break_is_stable_not_input_order(tmp_path):
    fixture = tmp_path / 'shuffled'
    fixture.mkdir()
    lines = (FIXTURES / 'terrain-branch' / 'cells.jsonl').read_text(encoding='utf-8').splitlines()
    (fixture / 'cells.jsonl').write_text('\n'.join(reversed(lines)) + '\n', encoding='utf-8')
    (fixture / 'spec.json').write_text(
        json.dumps({'path': 'cells.jsonl', 'neighbors': 4}), encoding='utf-8')
    db, _ = prepared(tmp_path, fixture)
    forward = terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-forward')
    assert outlet_loads(forward) == {'L': 3, 'R': 1}
    split = region_named(forward, forward['regions'][0]['id'])
    assert split['split']['edge'] == [[1, 0], [0, 0]]
    assert split['split']['target'] == '0:0'


# ---------------------------------------------------------------------------
# 2. Plateau coalescing and multi-level split
# ---------------------------------------------------------------------------

def test_plateau_with_hole_receives_and_drains_through_many_edges(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-cascade')
    result = terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'route-cascade')
    assert result['status'] == 'routed'
    assert result['conservation']['balanced'] is True
    # height-4 cells split into two plateaus because the missing interior
    # cell (2,2) severs the 4-neighbour connection; no cell is invented
    upper = [item for item in result['regions'] if item['height'] == 4]
    assert sorted(tuple(sorted(map(tuple, item['cells']))) for item in upper) == [
        ((0, 2), (1, 2)), ((3, 2), (4, 2))]
    # the h=2 ring is one flat region even though (2,2) is absent from the
    # frozen JSONL: the ring stays connected around the hole
    ring = [item for item in result['regions'] if item['height'] == 2]
    assert len(ring) == 1
    assert [2, 2] not in ring[0]['cells']
    assert len(ring[0]['cells']) == 5
    assert ring[0]['split']['kind'] == 'multiple'
    assert len({target['target'] for target in ring[0]['split']['targets']}) >= 2
    # local total equals everything that reached an outlet or stayed behind
    total = sum(amount(item['runoff']) for item in result['regions'])
    assert amount(result['conservation']['local']) == total
    assert total == pytest.approx(amount(result['conservation']['accounted']))


def test_multi_level_split_keeps_exact_rationals(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-cascade')
    result = terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'route-cascade')
    for item in result['regions']:
        if item['split'] and item['split']['kind'] == 'multiple':
            split = item['split']
            assert split['denominator'] == sum(target['weight'] for target in split['targets'])
            assert sum(target['numerator'] for target in split['targets']) == split['denominator']
            assert sum(amount(target['amount']) for target in split['targets']) == pytest.approx(
                amount(item['outflow']))
    # at least one region carries a fractional load, proving exact rational math
    assert any(item['received']['den'] > 1 for item in result['regions'])


# ---------------------------------------------------------------------------
# 3. Closed depressions, holes and conservation
# ---------------------------------------------------------------------------

def test_closed_depression_retains_water_and_does_not_guess_outlet(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-depression')
    result = terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'route-depression')
    pit = region_named(result, '2:1')  # region ids are "y:x"; this is cell (1,2)
    assert pit['height'] == -1
    assert pit['no_outflow_reason'] == 'closed_depression'
    assert amount(pit['retained']) > 0
    assert amount(pit['outflow']) == 0
    assert pit['split'] is None
    # three higher regions share edges into the pit; every one of those edges
    # merges into a single inflow, which is exactly what the pit retains
    feeding = [item for item in result['regions'] if item['split'] and any(
        target['target'] == pit['id'] for target in item['split']['targets'])]
    assert len(feeding) == 3
    assert amount(pit['retained']) == pytest.approx(
        sum(amount(target['amount']) for item in feeding for target in item['split']['targets']
            if target['target'] == pit['id']))
    total_retained = amount(result['conservation']['retained'])
    assert total_retained == pytest.approx(
        sum(amount(item['retained']) for item in result['regions']))
    assert amount(result['conservation']['local']) == pytest.approx(
        amount(result['conservation']['outlet_received']) + total_retained)


def test_zero_runoff_depression_retains_nothing(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-large')
    result = terrain.route(db, 'terrain-1', 'single_flow', {}, 'route-large')
    pocket = [item for item in result['regions'] if item['id'] in ('10:10', '10:11',
                                                                   '11:10', '11:11')]
    assert pocket, 'the 2x2 zero-runoff pocket must remain a closed region'
    retained = amount(result['conservation']['retained'])
    for item in pocket:
        assert item['no_outflow_reason'] == 'closed_depression'
        assert amount(item['runoff']) == 0
        assert amount(item['outflow']) == 0
        # whatever it holds is only what higher neighbours sent it
        assert amount(item['retained']) == amount(item['received'])
    assert retained > 0 and retained == pytest.approx(
        sum(amount(item['retained']) for item in result['regions']))


def test_outlets_are_terminal_local_minima_and_unique_regions(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-depression')
    result = terrain.route(db, 'terrain-1', 'single_flow', {}, 'route-depression')
    terminals = {item['outlet']: item for item in result['regions'] if item['outlet']}
    assert set(terminals) == {'W', 'E'}
    for item in terminals.values():
        assert item['split'] is None
        assert amount(item['to_outlet']) == amount(item['runoff']) + amount(item['received'])


def _custom_grid(tmp_path, cells, block=16):
    fixture = tmp_path / 'custom'
    fixture.mkdir()
    (fixture / 'cells.jsonl').write_text(
        '\n'.join(json.dumps(c) for c in cells) + '\n', encoding='utf-8')
    (fixture / 'spec.json').write_text(
        json.dumps({'path': 'cells.jsonl', 'neighbors': 4}), encoding='utf-8')
    return prepared(tmp_path, fixture)


def test_outlet_with_second_outlet_in_region_is_rejected(tmp_path):
    cells = [{'x': 0, 'y': 0, 'height': 2, 'runoff': 1, 'outlet': 'A'},
             {'x': 1, 'y': 0, 'height': 2, 'runoff': 1, 'outlet': 'B'},
             {'x': 2, 'y': 0, 'height': 1, 'runoff': 0}]
    db, _ = _custom_grid(tmp_path, cells)
    with pytest.raises(ValueError, match='outlet_region_shared'):
        terrain.route(db, 'terrain-1', 'single_flow', {}, 'bad-route')


def test_outlet_that_is_not_a_local_minimum_is_rejected(tmp_path):
    cells = [{'x': 0, 'y': 0, 'height': 3, 'runoff': 1, 'outlet': 'A'},
             {'x': 1, 'y': 0, 'height': 1, 'runoff': 1},
             {'x': 2, 'y': 0, 'height': 0, 'runoff': 1, 'outlet': 'B'}]
    db, _ = _custom_grid(tmp_path, cells)
    with pytest.raises(ValueError, match='outlet_not_local_minimum'):
        terrain.route(db, 'terrain-1', 'single_flow', {}, 'bad-route')


def test_outlet_on_a_plateau_with_a_lower_neighbour_is_rejected(tmp_path):
    # the marked plateau has a lower neighbour, so water could leave it: the
    # outlet marker would be a guess, not a terminal
    cells = [{'x': 0, 'y': 0, 'height': 4, 'runoff': 2, 'outlet': 'A'},
             {'x': 1, 'y': 0, 'height': 4, 'runoff': 2},
             {'x': 2, 'y': 0, 'height': 4, 'runoff': 2},
             {'x': 3, 'y': 0, 'height': 1, 'runoff': 1}]
    db, _ = _custom_grid(tmp_path, cells)
    with pytest.raises(ValueError, match='outlet_not_local_minimum'):
        terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'bad-route')


def test_missing_coordinates_are_not_invented(tmp_path):
    # a 3x3 block with the centre absent.  The ring must coalesce around the
    # hole and never report a cell at (1,1).
    cells = [{'x': x, 'y': y, 'height': 2, 'runoff': 1}
             for y in range(3) for x in range(3) if (x, y) != (1, 1)]
    cells[0].update({'height': 0, 'outlet': 'A'})   # (0,0) drain
    db, _ = _custom_grid(tmp_path, cells)
    result = terrain.route(db, 'terrain-1', 'single_flow', {'A': 8}, 'hole-route')
    assert result['outlets']['A']['received'] == {'num': 8, 'den': 1}
    assert result['outlets']['A']['passed'] is True
    cells_seen = [tuple(cell) for item in result['regions'] for cell in item['cells']]
    assert (1, 1) not in cells_seen
    assert len(cells_seen) == 8
    assert result['conservation']['balanced'] is True


def test_bad_thresholds_and_model_are_rejected(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    with pytest.raises(ValueError, match='bad_terrain_model'):
        terrain.route(db, 'terrain-1', 'nearest_outlet', {}, 'r')
    with pytest.raises(ValueError, match='bad_terrain_thresholds'):
        terrain.route(db, 'terrain-1', 'single_flow', {'Z': 1}, 'r')
    with pytest.raises(ValueError, match='bad_terrain_thresholds'):
        terrain.route(db, 'terrain-1', 'single_flow', {'L': -1}, 'r')
    with pytest.raises(ValueError, match='unknown_terrain_grid'):
        terrain.route(db, 'missing-grid', 'single_flow', {}, 'r')


# ---------------------------------------------------------------------------
# 4. Versions, idempotency and independent verification
# ---------------------------------------------------------------------------

def test_route_is_idempotent_by_identity_and_conflicts_on_drift(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    first = terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-1')
    again = terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-1')
    assert first == again
    assert connect(db).execute(
        'SELECT count(*) FROM terrain_route WHERE route_id=?', ('route-1',)).fetchone()[0] == 1
    with pytest.raises(ValueError, match='terrain_route_identity_conflict'):
        terrain.route(db, 'terrain-1', 'multiple_flow', {'L': 3}, 'route-1')
    with pytest.raises(ValueError, match='terrain_route_identity_conflict'):
        terrain.route(db, 'terrain-1', 'single_flow', {'L': 4}, 'route-1')


def test_new_delivery_version_gets_its_own_route_and_old_is_reviewable(tmp_path):
    fixture = FIXTURES / 'terrain-branch'
    db, old_version = prepared(tmp_path, 'terrain-branch')
    old = terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-old')
    # a new delivery with a different cell file (R runoff raised to 2)
    newer = tmp_path / 'newer'
    newer.mkdir()
    lines = (fixture / 'cells.jsonl').read_text(encoding='utf-8').replace(
        '"runoff":1,"outlet":"R"', '"runoff":2,"outlet":"R"')
    (newer / 'cells.jsonl').write_text(lines, encoding='utf-8')
    (newer / 'spec.json').write_text((fixture / 'spec.json').read_text(encoding='utf-8'),
                                     encoding='utf-8')
    extra_media = tmp_path / 'newer-media'
    extra_media.mkdir()
    new_version = engine.manifest(db, 'terrain-delivery-2', newer, 16)
    receipts = []
    for index, chunk in enumerate(engine.plan(db, new_version)):
        raw = (newer / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'part-{index}'
        (extra_media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / 'receipts-2.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, new_version, 'M2', extra_media, receipt_path)
    engine.assemble(db, new_version, extra_media, tmp_path / 'out2')
    assert engine.seal(db, new_version, tmp_path / 'out2') == '可发布'
    terrain.register(db, 'terrain-2', new_version, newer / 'spec.json')
    new = terrain.route(db, 'terrain-2', 'single_flow', {'L': 3}, 'route-new')
    assert new['version'] != old['version']
    assert new['cells_hash'] != old['cells_hash']
    assert outlet_loads(new) == {'L': 3, 'R': 2}
    # the old route still verifies against the old frozen cells
    assert terrain.verify(db, 'route-old')['status'] == 'routed'
    assert terrain.verify(db, 'route-new')['status'] == 'routed'
    assert terrain.verify(db, 'route-old')['cells_hash'] == old['cells_hash']


def test_verify_recomputes_from_frozen_cells(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-cascade')
    terrain.route(db, 'terrain-1', 'multiple_flow', {'A': 5}, 'route-1')
    checked = terrain.verify(db, 'route-1')
    assert checked['verified'] is True
    assert checked['conservation']['balanced'] is True


def test_verify_fails_when_an_edge_is_tampered(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-cascade')
    terrain.route(db, 'terrain-1', 'single_flow', {}, 'route-1')
    with db_open(db) as ledger:
        row = ledger.execute('SELECT height FROM terrain_cell WHERE grid_id=? AND x=1 AND y=2',
                             ('terrain-1',)).fetchone()
        ledger.execute('UPDATE terrain_cell SET height=? WHERE grid_id=? AND x=1 AND y=2',
                       (row['height'] + 1, 'terrain-1'))
    with pytest.raises(ValueError, match='terrain_route_verify_failed'):
        terrain.verify(db, 'route-1')


def test_verify_fails_when_local_runoff_is_lost(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-1')
    with db_open(db) as ledger:
        ledger.execute('UPDATE terrain_cell SET runoff=0 WHERE grid_id=? AND x=1 AND y=0',
                       ('terrain-1',))
    with pytest.raises(ValueError, match='terrain_route_verify_failed'):
        terrain.verify(db, 'route-1')


def test_verify_fails_when_a_region_row_losses_its_runoff(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-flat')
    terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'route-1')
    with db_open(db) as ledger:
        ledger.execute("UPDATE terrain_route_region SET runoff='1/1' "
                       "WHERE route_id='route-1' AND region_id='0:1'")
    with pytest.raises(ValueError, match='terrain_route_verify_failed'):
        terrain.verify(db, 'route-1')


def test_verify_fails_on_evidence_tampering(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-1')
    with db_open(db) as ledger:
        ledger.execute("UPDATE terrain_route SET evidence=replace(evidence,'\"num\":3','\"num\":4')"
                       " WHERE route_id='route-1'")
    with pytest.raises(ValueError, match='terrain_route_verify_failed'):
        terrain.verify(db, 'route-1')


def test_verify_fails_when_the_source_bytes_change(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-1')
    (tmp_path / 'out' / 'cells.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='terrain_bytes_changed'):
        terrain.verify(db, 'route-1')


def test_unknown_route_is_rejected(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-branch')
    with pytest.raises(ValueError, match='unknown_terrain_route'):
        terrain.verify(db, 'no-such-route')


def db_open(path):
    import contextlib

    @contextlib.contextmanager
    def opened():
        handle = connect(path)
        try:
            with handle:
                yield handle
        finally:
            handle.close()
    return opened()


# ---------------------------------------------------------------------------
# 5. Scale budget and non-regression of the sealed pipeline
# ---------------------------------------------------------------------------

def test_large_fixture_routes_within_budget(tmp_path):
    fixture = FIXTURES / 'terrain-large'
    db, version = sealed_grid(tmp_path, fixture, )
    terrain.register(db, 'terrain-large', version, fixture / 'spec.json')
    assert terrain.inspect(db, 'terrain-large')['cells'] == 1599
    started = time.monotonic()
    single = terrain.route(db, 'terrain-large', 'single_flow', {}, 'large-single')
    multiple = terrain.route(db, 'terrain-large', 'multiple_flow', {}, 'large-multiple')
    elapsed = time.monotonic() - started
    assert elapsed < 10
    for result in (single, multiple):
        assert result['status'] == 'routed'
        assert result['conservation']['balanced'] is True
        assert amount(result['conservation']['local']) == 1595
    # (20,20) is absent from the fixture: no report may contain it
    for result in (single, multiple):
        for item in result['regions']:
            assert [20, 20] not in item['cells']
    assert amount(single['conservation']['outlet_received']) == 1079
    assert amount(single['conservation']['retained']) == 516
    assert amount(single['conservation']['outlet_received']) + amount(
        single['conservation']['retained']) == 1595
    assert terrain.verify(db, 'large-multiple')['verified'] is True


def test_exhausted_budget_issues_no_outlet_loads(tmp_path):
    db, _ = prepared(tmp_path, 'terrain-cascade')
    result = terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'starved', budget=1)
    assert result['status'] == 'indeterminate'
    assert result['reason'] == 'budget_exhausted'
    assert 'outlets' not in result and 'regions' not in result
    assert result == terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'starved', budget=1)
    with pytest.raises(ValueError, match='terrain_route_identity_conflict'):
        terrain.route(db, 'terrain-1', 'multiple_flow', {}, 'starved', budget=2)
    checked = terrain.verify(db, 'starved')
    assert checked['status'] == 'indeterminate'
    assert connect(db).execute(
        'SELECT count(*) FROM terrain_route_outlet WHERE route_id=?', ('starved',)).fetchone()[0] == 0


def test_sealed_pipeline_is_not_changed_by_routing(tmp_path):
    fixture = FIXTURES / 'terrain-branch'
    db, version = sealed_grid(tmp_path, fixture)
    before = connect(db).execute('SELECT result,detail FROM decision WHERE version=?',
                                 (version,)).fetchone()
    terrain.register(db, 'terrain-1', version, fixture / 'spec.json')
    terrain.route(db, 'terrain-1', 'single_flow', {'L': 3}, 'route-1')
    after = connect(db).execute('SELECT result,detail FROM decision WHERE version=?',
                                (version,)).fetchone()
    assert tuple(before) == tuple(after) == ('可发布', tuple(before)[1])
    assert sorted(p.name for p in (tmp_path / 'out').iterdir()) == ['cells.jsonl', 'spec.json']
