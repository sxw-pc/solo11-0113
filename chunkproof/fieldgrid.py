"""Freeze sealed gridded environmental measurements from two instruments."""
import hashlib
import json
from pathlib import Path

from .store import connect


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_grid_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('grid_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, grid_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    sources = spec.get('sources')
    if not grid_id or not isinstance(sources, list) or len(sources) != 2 \
            or any(not isinstance(s.get('name'), str) or not s['name'] for s in sources) \
            or sources[0]['name'] == sources[1]['name']:
        raise ValueError('bad_grid_spec')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM field_grid WHERE id=?', (grid_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('grid_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('grid_not_sealed')
            cells = []
            for source in sources:
                for item in _sealed_lines(db, version, source['path']):
                    x, y, value, error = (item[k] for k in ('x', 'y', 'value', 'error_cost'))
                    if any(type(n) is not int for n in (x, y, value, error)) or value < 0 or error < 0:
                        raise ValueError('bad_grid_cell')
                    cells.append((grid_id, source['name'], x, y, value, error))
            if len({(c[1], c[2], c[3]) for c in cells}) != len(cells):
                raise ValueError('duplicate_grid_cell')
            db.execute('INSERT INTO field_grid VALUES (?,?,?,?)', (grid_id, version, digest, raw))
            db.executemany('INSERT INTO field_cell VALUES (?,?,?,?,?,?)', cells)
            return digest
    finally:
        db.close()


def inspect(db_path, grid_id):
    """Return raw coverage without choosing one measurement per grid cell."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM field_grid WHERE id=?', (grid_id,)).fetchone():
            raise ValueError('unknown_grid')
        raw = db.execute('SELECT count(*) n FROM field_cell WHERE grid_id=?', (grid_id,)).fetchone()['n']
        positions = db.execute('''SELECT count(*) n FROM
            (SELECT x,y FROM field_cell WHERE grid_id=? GROUP BY x,y)''', (grid_id,)).fetchone()['n']
        overlaps = db.execute('''SELECT count(*) n FROM
            (SELECT x,y FROM field_cell WHERE grid_id=? GROUP BY x,y HAVING count(*)>1)''', (grid_id,)).fetchone()['n']
        return {'raw_measurements': raw, 'positions': positions, 'overlaps': overlaps}
    finally:
        db.close()
