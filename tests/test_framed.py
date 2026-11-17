import json
from pathlib import Path

import pytest

from chunkproof import engine, framed


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'framed-events.json'


def sealed_feed(tmp_path, corrupt=False):
    spec = json.loads(FIXTURE.read_text(encoding='utf-8'))
    parts = [framed.pack(i, event) for i, event in enumerate(spec['events'], 1)]
    if corrupt:
        middle = bytearray(parts[spec['corrupt_sequence'] - 1])
        at = middle.index(b'middle')
        middle[at] = ord('x')
        parts[spec['corrupt_sequence'] - 1] = bytes(middle)
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'events.cpr').write_bytes(b''.join(parts))
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'framed-delivery', source, 64)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (source / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
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


def test_strict_decode_after_byte_seal(tmp_path):
    db, version = sealed_feed(tmp_path)
    framed.register(db, 'events-1', version, 'events.cpr')
    assert [row['sequence'] for row in framed.decode(db, 'events-1')] == [1, 2, 3]
    with pytest.raises(ValueError, match='frame_feed_identity_conflict'):
        framed.register(db, 'events-1', 'other-version', 'events.cpr')


def test_byte_seal_does_not_hide_corrupt_middle_frame(tmp_path):
    db, version = sealed_feed(tmp_path, corrupt=True)
    framed.register(db, 'events-1', version, 'events.cpr')
    with pytest.raises(ValueError, match='frame_crc'):
        framed.decode(db, 'events-1')
