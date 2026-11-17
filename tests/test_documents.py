import json
from pathlib import Path

import pytest

from chunkproof import documents, engine


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'documents'


def sealed_batch(tmp_path):
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
