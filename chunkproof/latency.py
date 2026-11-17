"""Small-batch exact latency quantile over sealed JSONL source files."""
import hashlib
import json
import math
from pathlib import Path

from .store import connect


def _sealed_path(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_latency_file')
    source = Path(row['path'])
    if hashlib.sha256(source.read_bytes()).hexdigest() != row['sha256']:
        raise ValueError('latency_bytes_changed')
    return source


def register(db_path, batch_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    paths = spec.get('paths')
    if not batch_id or not isinstance(paths, list) or not paths or len(set(paths)) != len(paths):
        raise ValueError('bad_latency_spec')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM latency_batch WHERE id=?', (batch_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('latency_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('latency_not_sealed')
            for relative_path in paths:
                _sealed_path(db, version, relative_path)
            db.execute('INSERT INTO latency_batch VALUES (?,?,?,?)', (batch_id, version, digest, raw))
            return digest
    finally:
        db.close()


def evaluate(db_path, batch_id, limit_ms, max_rows=1000):
    """Exact nearest-rank p95; intentionally holds a small batch in memory."""
    if type(limit_ms) is not int or limit_ms < 0 or type(max_rows) is not int or max_rows <= 0:
        raise ValueError('bad_latency_query')
    db = connect(db_path)
    try:
        batch = db.execute('SELECT * FROM latency_batch WHERE id=?', (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown_latency_batch')
        values, identities = [], set()
        for relative_path in json.loads(batch['spec'])['paths']:
            source = _sealed_path(db, batch['version'], relative_path)
            with source.open(encoding='utf-8') as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    ident, value = row['sample_id'], row['latency_ms']
                    if not ident or ident in identities or type(value) is not int or value < 0:
                        raise ValueError('bad_latency_record')
                    identities.add(ident)
                    values.append(value)
                    if len(values) > max_rows:
                        raise ValueError('latency_memory_limit')
        if not values:
            raise ValueError('empty_latency_batch')
        values.sort()
        p95 = values[math.ceil(95 * len(values) / 100) - 1]
        return {'samples': len(values), 'p95_ms': p95,
                'sla': 'pass' if p95 <= limit_ms else 'fail'}
    finally:
        db.close()
