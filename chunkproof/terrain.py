"""Freeze sealed terrain cells, then route runoff between flat regions.

The terrain subsystem deliberately separates two proofs:

``register`` freezes integer cells from bytes already sealed by ``engine``.
``route`` builds connected equal-height regions (flat areas), contracts them
into a strictly descending DAG and only then distributes water.  Water is
tracked with :class:`fractions.Fraction` so conservation is exact rather than
the result of per-cell rounding.

Two routing models are legal and produce different business answers for the
same frozen observation, so the chosen model is part of the immutable route
identity:

``single_flow``
    Every non-terminal region sends all of its water along the single edge
    with the largest drop.  Ties are broken deterministically by the
    downstream cell ``(y, x)`` and then the downstream region representative
    coordinate; input line order is never consulted.

``multiple_flow``
    Water is split in proportion to the summed drop weights of every strictly
    descending edge.  Edges into the same receiving region are merged before
    splitting.
"""
import hashlib
import json
from fractions import Fraction
from pathlib import Path

from .store import connect

MODELS = ('single_flow', 'multiple_flow')
DEFAULT_BUDGET = 1_000_000
NEIGHBORS = ((0, 1), (0, -1), (1, 0), (-1, 0))


def _canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _frac(value):
    return {'num': value.numerator, 'den': value.denominator}


def _frac_value(value):
    return Fraction(value['num'], value['den'])


def _frac_text(value):
    return f'{value.numerator}/{value.denominator}'


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256,a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_terrain_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('terrain_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def _normalized(lines):
    cells = []
    for item in lines:
        cells.append({'x': item['x'], 'y': item['y'], 'height': item['height'],
                      'runoff': item['runoff'], 'outlet': item.get('outlet')})
    cells.sort(key=lambda c: (c['y'], c['x']))
    return cells


def _cells_digest(cells):
    body = [[c['x'], c['y'], c['height'], c['runoff'], c['outlet']] for c in cells]
    return hashlib.sha256(_canonical(body).encode('utf-8')).hexdigest()


def _frozen_cells(db, grid_id):
    return [dict(row) for row in db.execute(
        'SELECT x,y,height,runoff,outlet FROM terrain_cell WHERE grid_id=? ORDER BY y,x',
        (grid_id,))]


def register(db_path, grid_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not grid_id or not isinstance(spec.get('path'), str) or spec.get('neighbors') != 4:
        raise ValueError('bad_terrain_spec')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM terrain_grid WHERE id=?', (grid_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('terrain_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('terrain_not_sealed')
            cells = []
            for item in _sealed_lines(db, version, spec['path']):
                x, y, height, runoff = (item[k] for k in ('x', 'y', 'height', 'runoff'))
                outlet = item.get('outlet')
                if any(type(n) is not int for n in (x, y, height, runoff)) or runoff < 0 \
                        or (outlet is not None and (not isinstance(outlet, str) or not outlet)):
                    raise ValueError('bad_terrain_cell')
                cells.append((grid_id, x, y, height, runoff, outlet))
            if not cells or len({(c[1], c[2]) for c in cells}) != len(cells):
                raise ValueError('duplicate_or_empty_terrain')
            outlets = [c[5] for c in cells if c[5] is not None]
            if not outlets or len(set(outlets)) != len(outlets):
                raise ValueError('duplicate_or_missing_outlet')
            db.execute('INSERT INTO terrain_grid VALUES (?,?,?,?)', (grid_id, version, digest, raw))
            db.executemany('INSERT INTO terrain_cell VALUES (?,?,?,?,?,?)', cells)
            return digest
    finally:
        db.close()


def inspect(db_path, grid_id):
    """Summarize raw terrain coverage and runoff before any flow modeling."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM terrain_grid WHERE id=?', (grid_id,)).fetchone():
            raise ValueError('unknown_terrain_grid')
        row = db.execute('''SELECT count(*) cells,sum(runoff) runoff,
            sum(CASE WHEN outlet IS NOT NULL THEN 1 ELSE 0 END) outlets
            FROM terrain_cell WHERE grid_id=?''', (grid_id,)).fetchone()
        return {'cells': row['cells'], 'raw_runoff': row['runoff'], 'outlets': row['outlets']}
    finally:
        db.close()


class _Exhausted(Exception):
    pass


class _Budget:
    """Deterministic work counter: running dry must not issue outlet loads."""

    def __init__(self, limit):
        if type(limit) is not int or limit < 0:
            raise ValueError('bad_terrain_budget')
        self.limit = limit
        self.used = 0

    def spend(self, amount=1):
        self.used += amount
        if self.used > self.limit:
            raise _Exhausted


def _build_regions(cells, budget):
    """Coalesce 4-connected equal-height cells into flat regions."""
    by_pos = {(c['x'], c['y']): c for c in cells}
    owner = {}
    regions = []
    for pos in sorted(by_pos, key=lambda p: (p[1], p[0])):
        if pos in owner:
            continue
        height = by_pos[pos]['height']
        group = []
        stack = [pos]
        owner[pos] = None
        budget.spend()
        while stack:
            x, y = stack.pop()
            group.append((x, y))
            for dx, dy in NEIGHBORS:
                nxt = (x + dx, y + dy)
                cell = by_pos.get(nxt)
                if cell is not None and nxt not in owner and cell['height'] == height:
                    owner[nxt] = None
                    stack.append(nxt)
                    budget.spend()
        group.sort(key=lambda p: (p[1], p[0]))
        rep = group[0]
        rid = f'{rep[1]}:{rep[0]}'
        outlets = sorted({by_pos[p]['outlet'] for p in group if by_pos[p]['outlet'] is not None})
        regions.append({'id': rid, 'height': height, 'rep': (rep[1], rep[0]),
                        'cells': group, 'outlets': outlets,
                        'runoff': sum(Fraction(by_pos[p]['runoff']) for p in group),
                        'out_edges': {}})
        for p in group:
            owner[p] = rid

    regional = {r['id']: r for r in regions}
    for region in regions:
        groups = {}
        for x, y in region['cells']:
            for dx, dy in NEIGHBORS:
                target = (x + dx, y + dy)
                cell = by_pos.get(target)
                if cell is None:
                    continue
                trid = owner[target]
                if trid == region['id'] or cell['height'] >= region['height']:
                    continue
                budget.spend()
                group = groups.setdefault(trid, {'target': trid, 'drop': region['height'] - cell['height'],
                                                 'weight': 0, 'edges': []})
                group['weight'] += group['drop']
                group['edges'].append(((x, y), target))
        region['out_edges'] = groups

    for region in regions:
        if len(region['outlets']) > 1:
            raise ValueError('outlet_region_shared')
        if region['outlets'] and region['out_edges']:
            raise ValueError('outlet_not_local_minimum')
    return regions, regional


def _compute(cells, model, thresholds, budget):
    """Return the model-specific core evidence for frozen cells."""
    regions, regional = _build_regions(cells, budget)
    order = sorted(regions, key=lambda r: (-r['height'], r['rep']))
    incoming = {}
    region_evidence = []
    outlet_results = {}
    for region in order:
        budget.spend()
        received = sum((amount for _, amount in incoming.get(region['id'], [])), Fraction(0))
        total = region['runoff'] + received
        outflow = Fraction(0)
        retained = Fraction(0)
        to_outlet = Fraction(0)
        split = None
        reason = None
        if region['outlets']:
            label = region['outlets'][0]
            to_outlet = total
            threshold = thresholds.get(label)
            passed = None if threshold is None else total >= threshold
            outlet_results[label] = {
                'region': region['id'], 'runoff': _frac(region['runoff']),
                'received': _frac(total), 'threshold': threshold, 'passed': passed,
                'contributions': [{'from': source, 'amount': _frac(amount)}
                                  for source, amount in sorted(incoming.get(region['id'], []))],
            }
        elif not region['out_edges']:
            retained = total
            reason = 'closed_depression'
        elif model == 'single_flow':
            max_drop = max(edge['drop'] for edge in region['out_edges'].values())
            candidates = []
            for edge in region['out_edges'].values():
                if edge['drop'] != max_drop:
                    continue
                rep = regional[edge['target']]['rep']
                for source, dest in edge['edges']:
                    candidates.append((dest[1], dest[0], rep[0], rep[1],
                                       source[1], source[0], edge))
            chosen = min(candidates, key=lambda item: item[:6])
            edge = chosen[6]
            outflow = total
            split = {'kind': 'single', 'target': edge['target'], 'drop': edge['drop'],
                     'numerator': 1, 'denominator': 1,
                     'edge': [[chosen[5], chosen[4]], [chosen[1], chosen[0]]]}
            incoming.setdefault(edge['target'], []).append((region['id'], total))
        else:
            denominator = sum(edge['weight'] for edge in region['out_edges'].values())
            split = {'kind': 'multiple', 'denominator': denominator, 'targets': []}
            for edge in sorted(region['out_edges'].values(),
                               key=lambda e: regional[e['target']]['rep']):
                amount = total * Fraction(edge['weight'], denominator)
                outflow += amount
                split['targets'].append({'target': edge['target'], 'drop': edge['drop'],
                                         'weight': edge['weight'], 'numerator': edge['weight'],
                                         'denominator': denominator, 'amount': _frac(amount)})
                if amount:
                    incoming.setdefault(edge['target'], []).append((region['id'], amount))
        region_evidence.append({
            'id': region['id'], 'height': region['height'], 'rep': [region['rep'][0], region['rep'][1]],
            'cells': [[x, y] for x, y in region['cells']],
            'outlet': region['outlets'][0] if region['outlets'] else None,
            'runoff': _frac(region['runoff']), 'received': _frac(received),
            'outflow': _frac(outflow), 'retained': _frac(retained),
            'to_outlet': _frac(to_outlet), 'split': split,
            'no_outflow_reason': reason,
        })

    local = sum((region['runoff'] for region in regions), Fraction(0))
    outlet_total = sum((_frac_value(item['received']) for item in outlet_results.values()), Fraction(0))
    retained_total = sum((_frac_value(item['retained']) for item in region_evidence), Fraction(0))
    balances = all(
        _frac_value(item['runoff']) + _frac_value(item['received'])
        == _frac_value(item['outflow']) + _frac_value(item['retained']) + _frac_value(item['to_outlet'])
        for item in region_evidence)
    return {
        'status': 'routed',
        'regions': region_evidence,
        'outlets': outlet_results,
        'conservation': {
            'local': _frac(local), 'outlet_received': _frac(outlet_total),
            'retained': _frac(retained_total), 'accounted': _frac(outlet_total + retained_total),
            'balanced': outlet_total + retained_total == local,
            'regions_balanced': balances,
        },
    }


def _identity(route_id, grid_id, version, cells_hash, model, thresholds, budget):
    return {'route_id': route_id, 'grid_id': grid_id, 'version': version,
            'cells_hash': cells_hash, 'model': model,
            'thresholds': {key: thresholds[key] for key in sorted(thresholds)},
            'budget': budget}


def _region_row(route_id, item):
    return (route_id, item['id'], item['height'], item['outlet'],
            _frac_text(_frac_value(item['runoff'])), _frac_text(_frac_value(item['received'])),
            _frac_text(_frac_value(item['outflow'])), _frac_text(_frac_value(item['retained'])),
            _canonical(item['split']), item['no_outflow_reason'], _canonical(item['cells']))


def _outlet_row(route_id, label, item):
    return (route_id, label, item['region'], _frac_text(_frac_value(item['received'])),
            item['threshold'], None if item['passed'] is None else int(item['passed']),
            _canonical(item['contributions']))


def _write_rows(db, route_id, core):
    for item in core['regions']:
        db.execute('INSERT INTO terrain_route_region VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                   _region_row(route_id, item))
    for label, item in core['outlets'].items():
        db.execute('INSERT INTO terrain_route_outlet VALUES (?,?,?,?,?,?,?)',
                   _outlet_row(route_id, label, item))


def _validate_thresholds(thresholds, outlet_labels):
    if not isinstance(thresholds, dict):
        raise ValueError('bad_terrain_thresholds')
    for label, threshold in thresholds.items():
        if label not in outlet_labels or type(threshold) is not int or threshold < 0:
            raise ValueError('bad_terrain_thresholds')


def route(db_path, grid_id, model, threshold_by_outlet, route_id, budget=DEFAULT_BUDGET):
    """Route frozen runoff to outlets under one declared model and seal the result."""
    if model not in MODELS:
        raise ValueError('bad_terrain_model')
    if not route_id:
        raise ValueError('bad_terrain_route_id')
    thresholds = dict(threshold_by_outlet or {})
    _Budget(budget)  # validates the budget type early
    db = connect(db_path)
    try:
        grid = db.execute('SELECT version FROM terrain_grid WHERE id=?', (grid_id,)).fetchone()
        if not grid:
            raise ValueError('unknown_terrain_grid')
        version = grid['version']
        cells = _frozen_cells(db, grid_id)
        if not cells:
            raise ValueError('unknown_terrain_grid')
        cells_hash = _cells_digest(cells)
        outlet_labels = {c['outlet'] for c in cells if c['outlet'] is not None}
        _validate_thresholds(thresholds, outlet_labels)
        identity = _identity(route_id, grid_id, version, cells_hash, model, thresholds, budget)

        old = db.execute('SELECT * FROM terrain_route WHERE route_id=?', (route_id,)).fetchone()
        if old:
            same = (old['grid_id'] == grid_id and old['version'] == version
                    and old['cells_hash'] == cells_hash and old['model'] == model
                    and old['budget'] == budget
                    and old['thresholds'] == _canonical(identity['thresholds']))
            if not same:
                raise ValueError('terrain_route_identity_conflict')
            return json.loads(old['evidence'])

        work = _Budget(budget)
        try:
            core = _compute(cells, model, thresholds, work)
        except _Exhausted:
            core = {'status': 'indeterminate', 'reason': 'budget_exhausted',
                    'budget_used': work.used}
        evidence = _identity(route_id, grid_id, version, cells_hash, model, thresholds, budget)
        evidence.update(core)
        text = _canonical(evidence)
        digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
        with db:
            db.execute('''INSERT INTO terrain_route(route_id,grid_id,version,cells_hash,model,
                thresholds,budget,status,evidence,evidence_hash) VALUES (?,?,?,?,?,?,?,?,?,?)''',
                       (route_id, grid_id, version, cells_hash, model,
                        _canonical(identity['thresholds']), budget, core['status'],
                        text, digest))
            if core['status'] == 'routed':
                _write_rows(db, route_id, core)
        return json.loads(text)
    finally:
        db.close()


def verify(db_path, route_id):
    """Recompute a sealed route from frozen cells and reject any drift."""
    db = connect(db_path)
    try:
        row = db.execute('SELECT * FROM terrain_route WHERE route_id=?', (route_id,)).fetchone()
        if not row:
            raise ValueError('unknown_terrain_route')
        stored = row['evidence']
        if hashlib.sha256(stored.encode('utf-8')).hexdigest() != row['evidence_hash']:
            raise ValueError('terrain_route_verify_failed')
        evidence = json.loads(stored)
        grid = db.execute('SELECT version,spec FROM terrain_grid WHERE id=?', (row['grid_id'],)).fetchone()
        if not grid or grid['version'] != row['version']:
            raise ValueError('terrain_route_verify_failed')
        if (evidence.get('route_id'), evidence.get('grid_id'), evidence.get('version'),
                evidence.get('model'), evidence.get('cells_hash'), evidence.get('budget'),
                evidence.get('thresholds')) != (row['route_id'], row['grid_id'], row['version'],
                                                row['model'], row['cells_hash'], row['budget'],
                                                json.loads(row['thresholds'])):
            raise ValueError('terrain_route_verify_failed')

        cells = _frozen_cells(db, row['grid_id'])
        if not cells or _cells_digest(cells) != row['cells_hash']:
            raise ValueError('terrain_route_verify_failed')
        spec = json.loads(grid['spec'])
        if _normalized(_sealed_lines(db, row['version'], spec['path'])) != _normalized(cells):
            raise ValueError('terrain_route_verify_failed')

        thresholds = json.loads(row['thresholds'])
        work = _Budget(row['budget'])
        try:
            core = _compute(cells, row['model'], thresholds, work)
        except _Exhausted:
            core = {'status': 'indeterminate', 'reason': 'budget_exhausted',
                    'budget_used': work.used}
        expected = _identity(row['route_id'], row['grid_id'], row['version'], row['cells_hash'],
                             row['model'], thresholds, row['budget'])
        expected.update(core)
        if _canonical(expected) != stored:
            raise ValueError('terrain_route_verify_failed')
        if core['status'] == 'routed':
            stored_regions = {row['region_id']: tuple(row) for row in db.execute(
                'SELECT * FROM terrain_route_region WHERE route_id=?', (route_id,))}
            expected_regions = {item['id']: _region_row(route_id, item) for item in core['regions']}
            stored_outlets = {row['outlet']: tuple(row) for row in db.execute(
                'SELECT * FROM terrain_route_outlet WHERE route_id=?', (route_id,))}
            expected_outlets = {label: _outlet_row(route_id, label, item)
                                for label, item in core['outlets'].items()}
            if stored_regions != expected_regions or stored_outlets != expected_outlets:
                raise ValueError('terrain_route_verify_failed')
        return {'route_id': route_id, 'verified': True, 'status': core['status'],
                'cells_hash': row['cells_hash'], 'evidence_hash': row['evidence_hash'],
                'conservation': core.get('conservation')}
    finally:
        db.close()
