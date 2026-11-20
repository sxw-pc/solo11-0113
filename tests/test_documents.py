import json
from pathlib import Path

import pytest

from chunkproof import documents, engine


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'documents'


def sealed_batch(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'document-delivery', FIXTURE, 32)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (FIXTURE / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
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


def test_document_revisions_are_frozen(tmp_path):
    db, version = sealed_batch(tmp_path)
    digest = documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    assert digest == documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    assert documents.inspect(db, 'corpus-1') == {'documents': 5, 'revisions': 6}
    with pytest.raises(ValueError, match='document_batch_identity_conflict'):
        documents.register(db, 'corpus-1', 'other-version', FIXTURE / 'spec.json')


def test_document_bytes_are_checked_before_indexing(tmp_path):
    db, version = sealed_batch(tmp_path)
    (tmp_path / 'out' / 'documents.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='document_bytes_changed'):
        documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')


# --------------------------------------------------------------------------
# helpers for index/search/verify
# --------------------------------------------------------------------------
def corpus(tmp_path, name, rows):
    """Seal a synthetic JSONL corpus and register it; return (db, batch, fixture)."""
    fixture = tmp_path / f'corpus-{name}'
    fixture.mkdir()
    (fixture / 'spec.json').write_text(json.dumps({'path': 'documents.jsonl',
                                                   'fields': ['title', 'body']}), encoding='utf-8')
    (fixture / 'documents.jsonl').write_text(
        '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows) + '\n', encoding='utf-8')
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, f'delivery-{name}', fixture, 16)
    media = tmp_path / f'medium-{name}'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'part-{index}'
        (media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / f'receipts-{name}.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M1', media, receipt_path)
    engine.assemble(db, version, media, tmp_path / f'out-{name}')
    assert engine.seal(db, version, tmp_path / f'out-{name}') == '可发布'
    documents.register(db, f'corpus-{name}', version, fixture / 'spec.json')
    return db, f'corpus-{name}', fixture


def hits(db, index_id, phrase, max_revision=None):
    return [(h['doc_id'], h['revision'], h['field'], h['start'], h['end'], h['text'])
            for h in documents.search(db, index_id, phrase, max_revision)]


def test_analyzers_diverge_on_same_corpus(tmp_path):
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    exact_index = documents.index(db, 'corpus-1', 'exact', 'exact')
    folded_index = documents.index(db, 'corpus-1', 'folded', 'folded')
    assert exact_index == documents.index(db, 'corpus-1', 'exact', 'exact')
    assert folded_index == documents.index(db, 'corpus-1', 'folded', 'folded')
    documents.index(db, 'corpus-1', 'folded', 'folded')
    # Latest revision of each document only.  d1 title/body split is never a
    # cross-field hit; d4 moved on to revision 2 ("new city").
    assert hits(db, 'exact', 'new york') == [('d5', 1, 'body', 0, 8, 'new york')]
    assert [(d, r, f, t) for d, r, f, s, e, t in hits(db, 'folded', 'new york')] == [
        ('d2', 1, 'body', 'new-york'), ('d3', 1, 'body', 'NEW YORK'), ('d5', 1, 'body', 'new york')]
    # Historical view: d4 revision 1 is still queryable; d4.2 remains excluded.
    assert [(d, r) for d, r, *_ in hits(db, 'exact', 'new york', 1)] == [('d4', 1), ('d5', 1)]
    assert [(d, r) for d, r, *_ in hits(db, 'folded', 'new york', 1)] == [
        ('d2', 1), ('d3', 1), ('d4', 1), ('d5', 1)]
    # exact keeps case and in-word punctuation: "new-york" is one token.
    assert hits(db, 'exact', 'NEW YORK') == [('d3', 1, 'body', 0, 8, 'NEW YORK')]
    assert [(d, r) for d, r, *_ in hits(db, 'exact', 'new-york')] == [('d2', 1)]
    # A folded query tokenizes to ["new","york"] and so cannot match the single
    # exact token "new-york"; conversely folded finds all three spellings.
    assert [(d, r) for d, r, *_ in hits(db, 'folded', 'NEW YORK')] == [
        ('d2', 1), ('d3', 1), ('d5', 1)]
    for index_id in ('exact', 'folded'):
        assert documents.verify(db, index_id, 'new york')['ok'] is True
        assert documents.verify(db, index_id, 'new york', 1)['ok'] is True


def test_field_boundaries_offsets_and_unicode_expansion(tmp_path):
    text = 'Straẞe ﬁle mañana ＡＢＣ'
    db, batch, _ = corpus(tmp_path, 'unicode', [
        {'doc_id': 'u1', 'revision': 1, 'title': 'Straße', 'body': text},
        {'doc_id': 'u2', 'revision': 1, 'title': 'New', 'body': 'York'},
    ])
    documents.index(db, batch, 'folded', 'u-folded')
    # casefold expansion (ß -> ss) maps to the whole original code point, and
    # the compatibility ligatures/contractions keep original offsets too.  The
    # title and body both fold to "strasse", so both are reported separately.
    assert hits(db, 'u-folded', 'strasse') == [
        ('u1', 1, 'title', 0, 6, 'Straße'), ('u1', 1, 'body', 0, 6, 'Straẞe')]
    assert hits(db, 'u-folded', 'file') == [('u1', 1, 'body', 7, 10, 'ﬁle')]
    assert hits(db, 'u-folded', 'mañana') == [('u1', 1, 'body', 11, 18, 'mañana')]
    assert hits(db, 'u-folded', 'abc') == [('u1', 1, 'body', 19, 22, 'ＡＢＣ')]
    # Phrase must be contiguous inside one field; "New"+"York" title/body is not.
    assert hits(db, 'u-folded', 'new york') == []
    assert hits(db, 'u-folded', 'new') == [('u2', 1, 'title', 0, 3, 'New')]
    assert documents.verify(db, 'u-folded', 'strasse')['ok'] is True
    documents.index(db, batch, 'exact', 'u-exact')
    # exact keeps the ligature/umlaut sequences intact and is case-sensitive.
    assert hits(db, 'u-exact', 'strasse') == []
    assert hits(db, 'u-exact', 'Straße') == [('u1', 1, 'title', 0, 6, 'Straße')]
    assert hits(db, 'u-exact', 'ﬁle') == [('u1', 1, 'body', 7, 10, 'ﬁle')]


def test_revision_visibility_and_newer_revision_without_phrase(tmp_path):
    db, batch, _ = corpus(tmp_path, 'revisions', [
        {'doc_id': 'a', 'revision': 1, 'title': 'T', 'body': 'alpha beta'},
        {'doc_id': 'a', 'revision': 2, 'title': 'T', 'body': 'gamma'},
        {'doc_id': 'a', 'revision': 3, 'title': 'T', 'body': 'delta'},
        {'doc_id': 'b', 'revision': 1, 'title': 'T', 'body': 'alpha'},
    ])
    documents.index(db, batch, 'exact', 'r')
    assert hits(db, 'r', 'alpha') == [('b', 1, 'body', 0, 5, 'alpha')]
    assert [(d, r) for d, r, *_ in hits(db, 'r', 'alpha', 1)] == [('a', 1), ('b', 1)]
    # At max_revision=2 document a's visible revision is 2 ("gamma"), so its
    # older "alpha" must not leak into the result.
    assert hits(db, 'r', 'alpha', 2) == [('b', 1, 'body', 0, 5, 'alpha')]
    # Latest visible revision is a.3 ("delta"); older revision 1 is not mixed in.
    assert [(d, r) for d, r, *_ in hits(db, 'r', 'delta')] == [('a', 3)]
    assert hits(db, 'r', 'delta', 2) == []
    assert [(d, r) for d, r, *_ in hits(db, 'r', 'delta', 3)] == [('a', 3)]
    # At max_revision=2 the visible a.2 is "gamma"; "alpha" only survives at a.1.
    assert hits(db, 'r', 'gamma') == []          # latest a.3 is "delta"
    assert [(d, r) for d, r, *_ in hits(db, 'r', 'gamma', 2)] == [('a', 2)]
    assert hits(db, 'r', 'gamma', 1) == []
    assert documents.verify(db, 'r', 'alpha', 2)['ok'] is True
    assert documents.verify(db, 'r', 'delta', 1)['ok'] is True


def test_empty_and_punctuation_queries_are_deterministic(tmp_path):
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    documents.index(db, 'corpus-1', 'exact', 'exact')
    documents.index(db, 'corpus-1', 'folded', 'folded')
    for index_id in ('exact', 'folded'):
        for phrase in ('', '   '):
            with pytest.raises(ValueError, match='empty_phrase'):
                documents.search(db, index_id, phrase)
        with pytest.raises(ValueError, match='bad_max_revision'):
            documents.search(db, index_id, 'new', 0)
        with pytest.raises(ValueError, match='unknown_index'):
            documents.search(db, 'missing', 'new')
    # folded has no word tokens at all; exact treats "---" as one unmatched token.
    for phrase in ('---', '.,!'):
        with pytest.raises(ValueError, match='empty_phrase'):
            documents.search(db, 'folded', phrase)
        assert documents.search(db, 'exact', phrase) == []
    with pytest.raises(ValueError, match='unknown_analyzer'):
        documents.index(db, 'corpus-1', 'stemmed', 'x')
    # exact matches a punctuation-bearing token literally; it never matches all.
    assert [(d, r) for d, r, *_ in hits(db, 'exact', 'new-york')] == [('d2', 1)]


def test_index_conflicts_idempotence_and_analyzer_isolation(tmp_path):
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    first = documents.index(db, 'corpus-1', 'exact', 'one')
    assert documents.index(db, 'corpus-1', 'exact', 'one') == first
    with pytest.raises(ValueError, match='index_identity_conflict'):
        documents.index(db, 'corpus-1', 'folded', 'one')
    with pytest.raises(ValueError, match='index_identity_conflict'):
        documents.index(db, 'other-batch', 'exact', 'one')
    # A folded index does not consult exact postings and vice versa.
    documents.index(db, 'corpus-1', 'folded', 'two')
    assert [(d, r) for d, r, *_ in hits(db, 'one', 'NEW YORK', 1)] == [('d3', 1)]
    assert [(d, r) for d, r, *_ in hits(db, 'two', 'NEW YORK', 1)] == [
        ('d2', 1), ('d3', 1), ('d4', 1), ('d5', 1)]
    with pytest.raises(ValueError, match='unknown_document_batch'):
        documents.index(db, 'missing', 'exact', 'three')


def test_verify_detects_tampered_spans_cross_field_and_wrong_analyzer(tmp_path):
    from chunkproof.store import connect
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    documents.index(db, 'corpus-1', 'folded', 'f')
    assert documents.verify(db, 'f', 'new york')['ok'] is True
    # Tampered span (claims a wider original slice than the token).
    handle = connect(db)
    with handle:
        handle.execute("UPDATE document_token SET end=end+1 "
                       "WHERE index_id='f' AND doc_id='d5' AND field='body' AND pos=0")
    handle.close()
    with pytest.raises(ValueError, match='(token_replay_mismatch|token_span_invalid)'):
        documents.verify(db, 'f', 'new york')
    # Prompt 4: a rebuilt copy (same corpus, same rows) demonstrates that a
    # postings row from a different index cannot be smuggled in.
    db, version = sealed_batch(tmp_path / 'second')
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    documents.index(db, 'corpus-1', 'folded', 'f')
    handle = connect(db)
    with handle:
        handle.execute("INSERT INTO document_token VALUES "
                       "('f','d1',1,'title',1,'york',3,7)")
    handle.close()
    with pytest.raises(ValueError, match='token_replay_mismatch'):
        documents.verify(db, 'f', 'new york')
    # Postings produced by another analyzer cannot pass verification.
    db, version = sealed_batch(tmp_path / 'third')
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    documents.index(db, 'corpus-1', 'exact', 'e')
    handle = connect(db)
    with handle:
        handle.execute('UPDATE document_index SET analyzer=? WHERE id=?', ('folded', 'e'))
    handle.close()
    with pytest.raises(ValueError, match='(index_revision_digest_mismatch|token_replay_mismatch)'):
        documents.verify(db, 'e', 'new york')
    # Explicit expected list is honoured: omitting a real hit is a false negative.
    db, version = sealed_batch(tmp_path / 'fourth')
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    documents.index(db, 'corpus-1', 'folded', 'f')
    result = documents.verify(db, 'f', 'new york', expected=[
        ('d3', 1, 'body', 0, 8), ('d5', 1, 'body', 0, 8)])
    assert result['ok'] is False and ('d2', 1, 'body', 0, 8) in result['false_positives']


def test_interrupted_build_exposes_no_half_index(tmp_path):
    from chunkproof import store
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')

    class Boom(Exception):
        pass

    original = store.connect
    calls = {'n': 0}

    class Flaky:
        def __init__(self, real):
            self._real = real
            self.row_factory = real.row_factory

        def execute(self, sql, params=()):
            return self._real.execute(sql, params)

        def executemany(self, sql, params):
            if 'INSERT INTO document_token' in sql:
                calls['n'] += 1
                # Fail *after* the index and revision rows were already inserted
                # inside this transaction.
                raise Boom()
            return self._real.executemany(sql, params)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return self._real.__exit__(exc_type, exc, tb)

        def close(self):
            self._real.close()

    def flaky(path):
        return Flaky(original(path))

    documents_mod = __import__('chunkproof.documents', fromlist=['connect'])
    documents_mod.connect = flaky
    try:
        with pytest.raises(Boom):
            documents.index(db, 'corpus-1', 'folded', 'half')
    finally:
        documents_mod.connect = original
    handle = store.connect(db)
    assert handle.execute("SELECT count(*) FROM document_index WHERE id='half'").fetchone()[0] == 0
    assert handle.execute("SELECT count(*) FROM document_index_revision WHERE index_id='half'").fetchone()[0] == 0
    assert handle.execute("SELECT count(*) FROM document_token WHERE index_id='half'").fetchone()[0] == 0
    handle.close()
    with pytest.raises(ValueError, match='unknown_index'):
        documents.search(db, 'half', 'new york')
    # A retry after the interruption produces the same digest as a clean build.
    digest = documents.index(db, 'corpus-1', 'folded', 'half')
    assert documents.verify(db, 'half', 'new york')['ok'] is True
    assert hits(db, 'half', 'new york') == hits(db, 'half', 'new york')
    assert digest == documents.index(db, 'corpus-1', 'folded', 'half')


def test_rebuilt_index_over_same_frozen_rows_is_reproducible(tmp_path):
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    assert documents.index(db, 'corpus-1', 'folded', 'a') == documents.index(db, 'corpus-1', 'folded', 'b')
    # New delivery version -> new batch -> independent index, old one still works.
    copy = tmp_path / 'copy'
    copy.mkdir()
    for path in FIXTURE.iterdir():
        (copy / path.name).write_bytes(path.read_bytes())
    other_db = tmp_path / 'other.db'
    other_version = engine.manifest(other_db, 'document-delivery', copy, 32)
    media = tmp_path / 'other-medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(other_db, other_version)):
        raw = (copy / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'part-{index}'
        (media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / 'other-receipts.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(other_db, other_version, 'M', media, receipt_path)
    engine.assemble(other_db, other_version, media, tmp_path / 'other-out')
    assert engine.seal(other_db, other_version, tmp_path / 'other-out') == '可发布'
    documents.register(other_db, 'corpus-1', other_version, copy / 'spec.json')
    documents.index(other_db, 'corpus-1', 'folded', 'b')
    assert hits(other_db, 'b', 'new york') == hits(db, 'a', 'new york')
    assert documents.verify(other_db, 'b', 'new york')['ok'] is True


def test_duplicate_doc_revision_with_divergent_text_is_rejected(tmp_path):
    fixture = tmp_path / 'dup'
    fixture.mkdir()
    (fixture / 'spec.json').write_text(json.dumps({'path': 'documents.jsonl'}), encoding='utf-8')
    (fixture / 'documents.jsonl').write_text('\n'.join([
        json.dumps({'doc_id': 'x', 'revision': 1, 'title': 'A', 'body': 'one'}),
        json.dumps({'doc_id': 'x', 'revision': 1, 'title': 'A', 'body': 'two'}),
    ]), encoding='utf-8')
    db = tmp_path / 'dup.db'
    version = engine.manifest(db, 'dup-delivery', fixture, 8)
    media = tmp_path / 'dup-medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        (media / f'p{index}').write_bytes((fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']])
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': f'p{index}', 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / 'dup-receipts.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M', media, receipt_path)
    engine.assemble(db, version, media, tmp_path / 'dup-out')
    assert engine.seal(db, version, tmp_path / 'dup-out') == '可发布'
    with pytest.raises(ValueError, match='duplicate_or_empty_document_rows'):
        documents.register(db, 'dup-batch', version, fixture / 'spec.json')


def test_new_delivery_version_keeps_old_index_answerable(tmp_path):
    from chunkproof.store import connect
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    documents.index(db, 'corpus-1', 'folded', 'old')
    old_hits = hits(db, 'old', 'new york')
    # Alter the delivered JSONL, seal it as a second version of the same
    # delivery, and register/index it separately.
    altered = tmp_path / 'altered'
    altered.mkdir()
    for name in ('spec.json', 'documents.jsonl'):
        (altered / name).write_bytes((FIXTURE / name).read_bytes())
    rows = [json.loads(line) for line in (altered / 'documents.jsonl').read_text(encoding='utf-8').splitlines() if line]
    rows[4]['body'] = 'new city data'          # d4 rev 2: still no "new york"
    rows[5]['body'] = 'fresh start'            # d5 loses the phrase
    (altered / 'documents.jsonl').write_text(
        '\n'.join(json.dumps(r) for r in rows) + '\n', encoding='utf-8')
    new_version = engine.manifest(db, 'document-delivery', altered, 32)
    assert new_version != version
    media = tmp_path / 'altered-medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, new_version)):
        (media / f'p{index}').write_bytes((altered / chunk['path']).read_bytes()[chunk['start']:chunk['end']])
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': f'p{index}', 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / 'altered-receipts.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, new_version, 'M', media, receipt_path)
    engine.assemble(db, new_version, media, tmp_path / 'altered-out')
    assert engine.seal(db, new_version, tmp_path / 'altered-out') == '可发布'
    documents.register(db, 'corpus-1-v2', new_version, altered / 'spec.json')
    documents.index(db, 'corpus-1-v2', 'folded', 'new')
    assert [(h['doc_id'], h['revision']) for h in documents.search(db, 'new', 'new york')] == [
        ('d2', 1), ('d3', 1)]
    # The old index still answers and verifies against the old frozen rows.
    assert hits(db, 'old', 'new york') == old_hits
    assert documents.verify(db, 'old', 'new york')['ok'] is True
    # Indices are bound to distinct spec hashes / versions.
    handle = connect(db)
    rows = {r['id']: (r['batch_id'], r['version']) for r in handle.execute(
        'SELECT id,batch_id,version FROM document_index')}
    handle.close()
    assert rows['old'][1] == version and rows['new'][1] == new_version


def test_cli_documents_index_search_verify(tmp_path, capsys):
    from chunkproof.__main__ import main
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'corpus-1', version, FIXTURE / 'spec.json')
    import sys
    argv = ['chunkproof']
    for args in (
            ['documents-index', '--db', str(db), '--batch', 'corpus-1',
             '--analyzer', 'folded', '--index', 'cli'],
            ['documents-search', '--db', str(db), '--index', 'cli',
             '--phrase', 'new york', '--max-revision', '1'],
            ['documents-verify', '--db', str(db), '--index', 'cli',
             '--phrase', 'new york']):
        sys.argv = argv + args
        main()
    out = capsys.readouterr().out.splitlines()
    found = json.loads(out[1])
    assert [(h['doc_id'], h['revision']) for h in found] == [('d2', 1), ('d3', 1), ('d4', 1), ('d5', 1)]
    assert json.loads(out[2])['ok'] is True


def test_unicode_fixture_round_trips_offsets_and_revisions(tmp_path):
    db, version = sealed_batch(tmp_path)
    documents.register(db, 'unicode', version, FIXTURE / 'unicode.spec.json')
    assert documents.inspect(db, 'unicode') == {'documents': 2, 'revisions': 4}
    documents.index(db, 'unicode', 'folded', 'u-folded')
    documents.index(db, 'unicode', 'exact', 'u-exact')
    # u1.2 has moved on: its old ß/ligature body is not part of the current view.
    assert hits(db, 'u-folded', 'strasse') == [('u1', 2, 'title', 0, 7, 'Strasse')]
    assert hits(db, 'u-folded', 'file') == [('u2', 2, 'body', 9, 12, 'ﬁle')]
    assert hits(db, 'u-folded', 'strasse', 1) == [('u1', 1, 'title', 0, 6, 'Straße')]
    # full-width ABC is compatibility-folded but keeps original code-point offsets.
    assert hits(db, 'u-folded', 'abc', 1) == [('u1', 1, 'body', 12, 15, 'ＡＢＣ')]
    assert hits(db, 'u-folded', 'mañana', 1) == [('u1', 1, 'body', 4, 11, 'mañana')]
    # exact never NFKC-folds or casefolds.
    assert hits(db, 'u-exact', 'file', 1) == []
    assert hits(db, 'u-exact', 'ﬁle', 1) == [('u1', 1, 'body', 0, 3, 'ﬁle')]
    for index_id in ('u-folded', 'u-exact'):
        for limit in (None, 1, 2):
            assert documents.verify(db, index_id, 'strasse' if index_id == 'u-folded' else 'Straße',
                                    limit)['ok'] is True


def _sealed_from_rows(tmp_path, rows, block=16):
    """Seal ``rows`` as documents.jsonl without the fixture directory."""
    fixture = tmp_path / 'synthetic'
    fixture.mkdir()
    (fixture / 'spec.json').write_text(json.dumps({'path': 'documents.jsonl'}), encoding='utf-8')
    (fixture / 'documents.jsonl').write_text(
        '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows) + '\n', encoding='utf-8')
    db = tmp_path / 'synthetic.db'
    version = engine.manifest(db, 'synthetic-delivery', fixture, block)
    media = tmp_path / 'synthetic-medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'p{index}'
        (media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt = tmp_path / 'synthetic-receipts.jsonl'
    receipt.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M', media, receipt)
    engine.assemble(db, version, media, tmp_path / 'synthetic-out')
    assert engine.seal(db, version, tmp_path / 'synthetic-out') == '可发布'
    documents.register(db, 'synthetic', version, fixture / 'spec.json')
    return db, fixture


def test_synthetic_scale_index_is_bounded_and_persisted(tmp_path):
    import time
    from chunkproof.store import connect
    docs, revision = 2000, 5   # 10 000 revisions, some documents revised 5x
    rows = []
    for d in range(docs):
        for r in range(1, revision + 1):
            body = f'document {d} revision {r} shared launch phrase token{r}'
            rows.append({'doc_id': f'doc-{d:05d}', 'revision': r,
                         'title': f'Title {d}', 'body': body})
    db, fixture = _sealed_from_rows(tmp_path, rows, block=64 * 1024)
    started = time.monotonic()
    documents.index(db, 'synthetic', 'folded', 'big')
    elapsed = time.monotonic() - started
    assert elapsed < 30, elapsed
    handle = connect(db)
    assert handle.execute("SELECT count(*) FROM document_index_revision "
                          "WHERE index_id='big'").fetchone()[0] == docs * revision
    assert handle.execute("SELECT count(*) FROM document_token WHERE index_id='big'"
                          ).fetchone()[0] > docs * revision
    # Search reads only the index: clobbering the assembled source after the
    # build still yields results (search never re-reads the JSONL).
    (tmp_path / 'synthetic-out' / 'documents.jsonl').write_text('gone', encoding='utf-8')
    handle.close()
    found = documents.search(db, 'big', 'shared launch phrase')
    assert len(found) == docs  # only each document's newest revision is visible
    assert all(hit['revision'] == revision for hit in found)
    assert len(documents.search(db, 'big', 'shared launch phrase', 2)) == docs
    assert all(hit['revision'] == 2 for hit in documents.search(db, 'big', 'shared launch phrase', 2))
    # Verification re-reads the frozen source, so a mutated source is rejected.
    with pytest.raises(ValueError, match='document_bytes_changed'):
        documents.verify(db, 'big', 'shared')

