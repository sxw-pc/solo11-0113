"""Sealed, revisioned document records with phrase search over persisted token positions.

Registration freezes the JSONL bytes into ``document_row``.  ``index`` builds a
durable inverted position index from those frozen rows: every token carries the
minimal *original* Unicode code-point span it came from, tokens never cross a
field boundary, and older revisions keep their own postings so historical views
stay queryable.  ``search`` only reads the persisted index; ``verify`` rebuilds
the tokens from the frozen source and cross-checks every claimed and omitted hit.
"""
import hashlib
import json
import unicodedata
from pathlib import Path

from .store import connect

ANALYZER_VERSION = '1'  # bump when tokenization/normalization rules change
FIELDS = ('title', 'body')


# --------------------------------------------------------------------------
# sealed source access
# --------------------------------------------------------------------------
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


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def _span_digest(spans):
    return _sha(_canonical([[field, start, end] for field, start, end in spans]).encode('utf-8'))


# --------------------------------------------------------------------------
# registration (unchanged contract: proof of JSONL bytes, no tokenization)
# --------------------------------------------------------------------------
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


# --------------------------------------------------------------------------
# analyzers: token string + minimal contiguous original code-point span
# --------------------------------------------------------------------------
def _is_word(ch):
    # Marks attach to the preceding letter so decomposed forms do not split.
    return unicodedata.category(ch)[0] in ('L', 'N', 'M')


def _exact_tokens(text):
    # Unicode-whitespace split; case and in-word punctuation are preserved.
    tokens = []
    start = None
    for i, ch in enumerate(text):
        if ch.isspace():
            if start is not None:
                tokens.append((text[start:i], start, i))
                start = None
        elif start is None:
            start = i
    if start is not None:
        tokens.append((text[start:], start, len(text)))
    return tokens


def _cluster_end(text, i):
    # Keep a base character with the combining marks / Hangul jamo that NFKC
    # can compose it with, so normalization never crosses a mapped span.
    j = i + 1
    while j < len(text):
        ch = text[j]
        if (unicodedata.category(ch)[0] == 'M'
                or 'ᄀ' <= ch <= 'ᅟ' or 'ᅠ' <= ch <= 'ᇿ'):
            j += 1
        else:
            break
    return j


def _folded_tokens(text):
    # NFKC + casefold first, then split on non-alphanumeric boundaries.  Each
    # normalized code point carries the original cluster span it came from, so
    # casefold expansions and compatibility contractions map back to original
    # code-point offsets rather than normalized-string offsets.
    chars, origin_start, origin_end = [], [], []
    i = 0
    while i < len(text):
        j = _cluster_end(text, i)
        for normalized in unicodedata.normalize('NFKC', text[i:j]).casefold():
            chars.append(normalized)
            origin_start.append(i)
            origin_end.append(j)
        i = j
    tokens = []
    start = None
    for j in range(len(chars) + 1):
        boundary = j == len(chars) or not _is_word(chars[j])
        if boundary:
            if start is not None:
                tokens.append((''.join(chars[start:j]), origin_start[start], origin_end[j - 1]))
                start = None
        elif start is None:
            start = j
    return tokens


_ANALYZERS = {'exact': _exact_tokens, 'folded': _folded_tokens}


def _analyzer(name):
    if name not in _ANALYZERS:
        raise ValueError('unknown_analyzer')
    return _ANALYZERS[name]


def _load_batch(db, batch_id):
    batch = db.execute('SELECT * FROM document_batch WHERE id=?', (batch_id,)).fetchone()
    if not batch:
        raise ValueError('unknown_document_batch')
    return batch


def _frozen_rows(db, batch):
    """Re-derive rows from the sealed file and prove they equal ``document_row``.

    The spec bytes stored on the batch are checked against ``spec_hash``; the
    sealed JSONL bytes are separately checked against ``file_entry.sha256`` by
    ``_sealed_lines``.  Both bindings must hold before an index may be built.
    """
    if _sha(batch['spec']) != batch['spec_hash']:
        raise ValueError('document_spec_changed')
    spec = json.loads(batch['spec'])
    lines = _sealed_lines(db, batch['version'], spec['path'])
    rows = db.execute('''SELECT doc_id,revision,title,body FROM document_row
        WHERE batch_id=? ORDER BY doc_id,revision''', (batch['id'],)).fetchall()
    expected = {(item['doc_id'], item['revision']): item for item in lines}
    frozen = {(r['doc_id'], r['revision']): r for r in rows}
    if set(expected) != set(frozen):
        raise ValueError('document_source_changed')
    for key, rec in frozen.items():
        item = expected[key]
        if rec['title'] != item['title'] or rec['body'] != item['body']:
            raise ValueError('document_source_changed')
    return frozen, lines


# --------------------------------------------------------------------------
# index build
# --------------------------------------------------------------------------
def index(db_path, batch_id, analyzer, index_id):
    tokenizer = _analyzer(analyzer)
    if not isinstance(index_id, str) or not index_id:
        raise ValueError('bad_index_id')
    request_digest = _sha(b'build\0' + _canonical({'batch': batch_id, 'analyzer': analyzer,
                                                   'index': index_id}).encode('utf-8'))
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT * FROM document_index WHERE id=?', (index_id,)).fetchone()
            if old:
                if (old['batch_id'], old['analyzer'], old['analyzer_version'],
                        old['request_digest']) != (batch_id, analyzer, ANALYZER_VERSION, request_digest):
                    raise ValueError('index_identity_conflict')
                return old['revisions_digest']
            batch = _load_batch(db, batch_id)
            # Re-derive raw bytes from the sealed/assembled file and cross-check
            # both the frozen batch spec hash and the individual row digests.
            frozen, lines = _frozen_rows(db, batch)

            revision_digest_input = []
            postings = []
            for key in sorted(frozen, key=lambda k: (k[0], k[1])):
                item = frozen[key]
                doc_id, revision = key
                title_hash = _sha(item['title'].encode('utf-8'))
                body_hash = _sha(item['body'].encode('utf-8'))
                revision_digest_input.append([doc_id, revision, title_hash, body_hash])
                for field in FIELDS:
                    for pos, (token, start, end) in enumerate(tokenizer(item[field])):
                        postings.append((index_id, doc_id, revision, field,
                                         pos, token, start, end))
            revisions_digest = _sha(_canonical(revision_digest_input).encode('utf-8'))
            # Single transaction: an interrupted build leaves no index row, no
            # revision rows and no postings, so no half index is ever visible.
            db.execute('''INSERT INTO document_index(id,batch_id,version,spec_hash,line_digest,
                revisions_digest,analyzer,analyzer_version,request_digest)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                       (index_id, batch_id, batch['version'], batch['spec_hash'],
                        _sha(_canonical(lines).encode('utf-8')),
                        revisions_digest, analyzer, ANALYZER_VERSION, request_digest))
            db.executemany('INSERT INTO document_index_revision VALUES (?,?,?,?,?)',
                           [(index_id, *item) for item in revision_digest_input])
            db.executemany('INSERT INTO document_token VALUES (?,?,?,?,?,?,?,?)', postings)
            return revisions_digest
    finally:
        db.close()


def _visible_revisions(db, index_id, max_revision):
    rows = db.execute('''SELECT doc_id,revision FROM document_index_revision
        WHERE index_id=? ORDER BY doc_id,revision''', (index_id,)).fetchall()
    latest = {}
    for row in rows:
        if max_revision is not None and row['revision'] > max_revision:
            continue
        if row['doc_id'] not in latest or row['revision'] > latest[row['doc_id']]:
            latest[row['doc_id']] = row['revision']
    return latest


def _load_index(db, index_id):
    row = db.execute('SELECT * FROM document_index WHERE id=?', (index_id,)).fetchone()
    if not row:
        raise ValueError('unknown_index')
    return row


def _analyze_phrase(index_row, phrase):
    if not isinstance(phrase, str):
        raise ValueError('bad_phrase')
    tokenizer = _analyzer(index_row['analyzer'])
    tokens = [token for token, _, _ in tokenizer(phrase)]
    if not tokens:
        raise ValueError('empty_phrase')
    return tokens


def search(db_path, index_id, phrase, max_revision=None):
    if max_revision is not None and (type(max_revision) is not int or max_revision <= 0):
        raise ValueError('bad_max_revision')
    db = connect(db_path)
    try:
        index_row = _load_index(db, index_id)
        tokens = _analyze_phrase(index_row, phrase)
        visible = _visible_revisions(db, index_id, max_revision)
        if not visible:
            return []
        # Look up the first phrase token through the inverted table, then keep
        # only postings whose (doc_id, revision) is visible for this query.  A
        # predicate over a per-document revision map is used instead of a huge
        # OR list so scale is bounded by index size, not query string depth.
        candidates = db.execute('''SELECT doc_id,revision,field,pos,start,end FROM document_token
            WHERE index_id=? AND token=?
            ORDER BY doc_id,revision,field,pos''', (index_id, tokens[0])).fetchall()
        results = []
        for cand in candidates:
            doc_id, revision, field = cand['doc_id'], cand['revision'], cand['field']
            if visible.get(doc_id) != revision:
                continue
            following = db.execute(f'''SELECT pos,token,start,end FROM document_token
                WHERE index_id=? AND doc_id=? AND revision=? AND field=? AND pos>=?
                ORDER BY pos LIMIT ?''',
                                   (index_id, doc_id, revision, field, cand['pos'], len(tokens))).fetchall()
            if len(following) != len(tokens):
                continue
            if [row['token'] for row in following] != tokens:
                continue
            if following[0]['pos'] != cand['pos']:
                continue
            start, end = following[0]['start'], following[-1]['end']
            snippet = _frozen_text(db, index_row['batch_id'], doc_id, revision, field, start, end)
            if not snippet:
                raise ValueError('token_span_invalid')
            results.append({'doc_id': doc_id, 'revision': revision, 'field': field,
                            'start': start, 'end': end, 'text': snippet})
        results.sort(key=lambda x: (x['doc_id'], x['revision'], FIELDS.index(x['field']),
                                    x['start'], x['end']))
        return results
    finally:
        db.close()


def _frozen_text(db, batch_id, doc_id, revision, field, start, end):
    row = db.execute('SELECT title,body FROM document_row WHERE batch_id=? AND doc_id=? AND revision=?',
                     (batch_id, doc_id, revision)).fetchone()
    if not row:
        raise ValueError('unknown_document_revision')
    text = row[field]
    if end > len(text):
        raise ValueError('token_span_invalid')
    return text[start:end]


# --------------------------------------------------------------------------
# independent replay verification
# --------------------------------------------------------------------------
def _expected_hits(frozen, tokenizer, tokens, max_revision):
    grouped = {}
    for (doc_id, revision), item in frozen.items():
        grouped.setdefault(doc_id, []).append((revision, item))
    hits = []
    for doc_id, revisions in grouped.items():
        candidates = [r for r in revisions if max_revision is None or r[0] <= max_revision]
        if not candidates:
            continue
        revision, item = max(candidates, key=lambda r: r[0])
        for field in FIELDS:
            field_tokens = tokenizer(item[field])
            for pos in range(len(field_tokens)):
                window = [t[0] for t in field_tokens[pos:pos + len(tokens)]]
                if window != tokens:
                    continue
                start, end = field_tokens[pos][1], field_tokens[pos + len(tokens) - 1][2]
                hits.append((doc_id, revision, field, start, end))
    return hits


def verify(db_path, index_id, phrase, max_revision=None, expected=None):
    if max_revision is not None and (type(max_revision) is not int or max_revision <= 0):
        raise ValueError('bad_max_revision')
    db = connect(db_path)
    try:
        index_row = _load_index(db, index_id)
        tokens = _analyze_phrase(index_row, phrase)
        analyzer = _analyzer(index_row['analyzer'])
        batch = _load_batch(db, index_row['batch_id'])
        frozen, lines = _frozen_rows(db, batch)
        if _sha(_canonical(lines).encode('utf-8')) != index_row['line_digest']:
            raise ValueError('document_source_changed')
        index_revs = {(r['doc_id'], r['revision']) for r in db.execute(
            'SELECT doc_id,revision FROM document_index_revision WHERE index_id=?', (index_id,))}
        if index_revs != set(frozen):
            raise ValueError('index_revision_set_mismatch')
        revision_digest_input = []
        for key in sorted(frozen):
            item = frozen[key]
            revision_digest_input.append([key[0], key[1],
                                          _sha(item['title'].encode('utf-8')),
                                          _sha(item['body'].encode('utf-8'))])
        if _sha(_canonical(revision_digest_input).encode('utf-8')) != index_row['revisions_digest']:
            raise ValueError('index_revision_digest_mismatch')
        stored_analysis = []
        for (doc_id, revision), item in sorted(frozen.items()):
            for field in FIELDS:
                expected_tokens = analyzer(item[field])
                persisted = db.execute('''SELECT token,start,end FROM document_token
                    WHERE index_id=? AND doc_id=? AND revision=? AND field=? ORDER BY pos''',
                                       (index_id, doc_id, revision, field)).fetchall()
                if len(persisted) != len(expected_tokens):
                    raise ValueError('token_replay_mismatch')
                for row, (token, start, end) in zip(persisted, expected_tokens):
                    if (row['token'], row['start'], row['end']) != (token, start, end):
                        raise ValueError('token_replay_mismatch')
                    if end > len(item[field]) or start >= end:
                        raise ValueError('token_span_invalid')
                stored_analysis.append([doc_id, revision, field, _span_digest(expected_tokens)])
        replay_digest = _sha(_canonical(stored_analysis).encode('utf-8'))
        actual = search(db_path, index_id, phrase, max_revision)
        actual_keys = set()
        for hit in actual:
            key = (hit['doc_id'], hit['revision'], hit['field'], hit['start'], hit['end'])
            # A hit's snippet must be exactly the frozen source slice: this
            # catches tampered spans and cross-field concat just as surely as
            # an explicit expected list would.
            if frozen[(hit['doc_id'], hit['revision'])][hit['field']][hit['start']:hit['end']] != hit['text']:
                raise ValueError('token_span_invalid')
            actual_keys.add(key)
        expected_keys = set(_expected_hits(frozen, analyzer, tokens, max_revision))
        if expected is not None:
            expected_keys = {(str(d), int(r), str(f), int(s), int(e)) for d, r, f, s, e in expected}
        false_positives = sorted(actual_keys - expected_keys)
        false_negatives = sorted(expected_keys - actual_keys)
        ok = not false_positives and not false_negatives
        return {'ok': ok, 'analyzer': index_row['analyzer'],
                'analyzer_version': index_row['analyzer_version'],
                'replay_digest': replay_digest, 'revisions_digest': index_row['revisions_digest'],
                'hits': len(expected_keys), 'false_positives': false_positives,
                'false_negatives': false_negatives}
    finally:
        db.close()
