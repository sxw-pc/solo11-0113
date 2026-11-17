"""Freeze sealed terrain cells and local runoff without routing water."""
import hashlib
import json
from pathlib import Path

from .store import connect


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
