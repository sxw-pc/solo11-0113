"""Index sealed, pseudonymous entity feeds without inferring cross-source identity."""
import hashlib
import json
from pathlib import Path

from .store import connect


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_feed_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('feed_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, feed_id, version, spec_path):
    """Freeze feed membership, row identities and candidate evidence from sealed files."""
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not feed_id or not isinstance(spec.get('sources'), list) or len(spec['sources']) != 2:
        raise ValueError('bad_feed_spec')
    left, right = (part['name'] for part in spec['sources'])
    if left == right:
        raise ValueError('duplicate_source')
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM entity_feed WHERE id=?', (feed_id,)).fetchone()
            digest = hashlib.sha256(raw).hexdigest()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('feed_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('feed_not_sealed')
            rows = []
            for part in spec['sources']:
                for item in _sealed_lines(db, version, part['path']):
                    rows.append((feed_id, part['name'], item['local_id'], item['cohort']))
            identities = {(source, local_id) for _, source, local_id, _ in rows}
            if len(identities) != len(rows):
                raise ValueError('duplicate_local_id')
            edges = []
            for item in _sealed_lines(db, version, spec['links']):
                if (left, item['left_id']) not in identities or (right, item['right_id']) not in identities:
                    raise ValueError('unknown_link_endpoint')
                weight = item['weight']
                if type(weight) is not int or weight <= 0:
                    raise ValueError('bad_link_weight')
                edges.append((feed_id, item['left_id'], item['right_id'], weight))
            if len({(e[1], e[2]) for e in edges}) != len(edges):
                raise ValueError('duplicate_link')
            db.execute('INSERT INTO entity_feed VALUES (?,?,?,?)', (feed_id, version, digest, raw))
            db.executemany('INSERT INTO entity_row VALUES (?,?,?,?)', rows)
            db.executemany('INSERT INTO entity_link VALUES (?,?,?,?)', edges)
            return digest
    finally:
        db.close()


def inspect(db_path, feed_id):
    """Report raw source volume and evidence inventory; no match is chosen here."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM entity_feed WHERE id=?', (feed_id,)).fetchone():
            raise ValueError('unknown_feed')
        by_source = {row['source']: row['n'] for row in db.execute(
            'SELECT source,count(*) n FROM entity_row WHERE feed_id=? GROUP BY source ORDER BY source', (feed_id,))}
        links = db.execute('SELECT count(*) n FROM entity_link WHERE feed_id=?', (feed_id,)).fetchone()['n']
        return {'source_rows': by_source, 'raw_rows': sum(by_source.values()), 'candidate_links': links}
    finally:
        db.close()
