"""Freeze sealed, revisioned text records without creating a search index."""
import hashlib
import json
from pathlib import Path

from .store import connect


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256,a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_document_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('document_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, batch_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not batch_id or not isinstance(spec.get('path'), str) or not spec['path']:
        raise ValueError('bad_document_spec')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM document_batch WHERE id=?', (batch_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('document_batch_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('document_batch_not_sealed')
            records = []
            for item in _sealed_lines(db, version, spec['path']):
                doc_id, revision, title, body = (item[k] for k in ('doc_id', 'revision', 'title', 'body'))
                if not isinstance(doc_id, str) or not doc_id or type(revision) is not int \
                        or revision <= 0 or not isinstance(title, str) or not isinstance(body, str):
                    raise ValueError('bad_document_row')
                records.append((batch_id, doc_id, revision, title, body))
            if not records or len({(r[1], r[2]) for r in records}) != len(records):
                raise ValueError('duplicate_or_empty_document_rows')
            db.execute('INSERT INTO document_batch VALUES (?,?,?,?)', (batch_id, version, digest, raw))
            db.executemany('INSERT INTO document_row VALUES (?,?,?,?,?)', records)
            return digest
    finally:
        db.close()


def inspect(db_path, batch_id):
    """Return revision inventory only; no tokenization or query results."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM document_batch WHERE id=?', (batch_id,)).fetchone():
            raise ValueError('unknown_document_batch')
        row = db.execute('''SELECT count(*) revisions,count(DISTINCT doc_id) documents
            FROM document_row WHERE batch_id=?''', (batch_id,)).fetchone()
        return {'documents': row['documents'], 'revisions': row['revisions']}
    finally:
        db.close()
