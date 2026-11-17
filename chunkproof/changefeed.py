"""Freeze sealed transaction metadata and row changes without replaying them."""
import hashlib
import json
from pathlib import Path

from .store import connect


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_changefeed_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('changefeed_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, run_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not run_id or not {'transactions', 'rows'} <= spec.keys():
        raise ValueError('bad_changefeed_spec')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM changefeed_run WHERE id=?', (run_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('changefeed_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('changefeed_not_sealed')
            transactions = []
            for item in _sealed_lines(db, version, spec['transactions']):
                tx_id, source, seq, parents, expected, authority = (item[key] for key in
                    ('tx_id', 'source', 'source_seq', 'parents', 'expected_rows', 'authority'))
                if not tx_id or not source or type(seq) is not int or seq <= 0 or not isinstance(parents, list) \
                        or any(not isinstance(p, str) or not p for p in parents) \
                        or len(set(parents)) != len(parents) or tx_id in parents \
                        or type(expected) is not int or expected <= 0 \
                        or type(authority) is not int or authority <= 0:
                    raise ValueError('bad_transaction')
                transactions.append((run_id, tx_id, source, seq, json.dumps(parents, sort_keys=True), expected, authority))
            if len({r[1] for r in transactions}) != len(transactions) \
                    or len({(r[2], r[3]) for r in transactions}) != len(transactions):
                raise ValueError('duplicate_transaction_identity')
            tx_ids = {r[1] for r in transactions}
            rows = []
            for item in _sealed_lines(db, version, spec['rows']):
                tx_id, ordinal, key, op = (item[k] for k in ('tx_id', 'ordinal', 'entity_key', 'op'))
                value = item.get('value')
                if tx_id not in tx_ids or type(ordinal) is not int or ordinal < 0 or not key \
                        or op not in ('put', 'delete') or (op == 'put' and value is None):
                    raise ValueError('bad_change_row')
                rows.append((run_id, tx_id, ordinal, key, op, json.dumps(value, sort_keys=True)))
            if len({(r[1], r[2]) for r in rows}) != len(rows):
                raise ValueError('duplicate_change_ordinal')
            db.execute('INSERT INTO changefeed_run VALUES (?,?,?,?)', (run_id, version, digest, raw))
            db.executemany('INSERT INTO change_tx VALUES (?,?,?,?,?,?,?)', transactions)
            db.executemany('INSERT INTO change_row VALUES (?,?,?,?,?,?)', rows)
            return digest
    finally:
        db.close()


def inspect(db_path, run_id):
    """Report raw transaction coverage, not a materialized entity state."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM changefeed_run WHERE id=?', (run_id,)).fetchone():
            raise ValueError('unknown_changefeed_run')
        transactions = db.execute('SELECT count(*) n FROM change_tx WHERE run_id=?', (run_id,)).fetchone()['n']
        rows = db.execute('SELECT count(*) n FROM change_row WHERE run_id=?', (run_id,)).fetchone()['n']
        incomplete = [r['tx_id'] for r in db.execute('''SELECT t.tx_id FROM change_tx t
            LEFT JOIN change_row c ON c.run_id=t.run_id AND c.tx_id=t.tx_id
            WHERE t.run_id=? GROUP BY t.tx_id HAVING count(c.ordinal)<>t.expected_rows ORDER BY t.tx_id''', (run_id,))]
        return {'transactions': transactions, 'rows': rows, 'incomplete': incomplete}
    finally:
        db.close()
