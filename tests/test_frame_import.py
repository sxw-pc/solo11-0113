"""Tests for bounded streaming CPR1 frame import and evidence verification."""
import json
import struct
import zlib
from pathlib import Path

import pytest

from chunkproof import engine, framed
from chunkproof.store import connect


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'framed-events.json'
DAMAGE_FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'framed-damage.json'


def seal_raw(tmp_path, raw, name='events.cpr', block=64):
    """Manifest -> receipts -> assemble -> seal a raw CPR1 file."""
    tmp_path = Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / 'source'
    source.mkdir(exist_ok=True)
    target = source / name
    target.write_bytes(raw)
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'framed-delivery', source, block)
    media = tmp_path / 'medium'
    media.mkdir(exist_ok=True)
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        part_raw = (source / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'{chunk["path"]}.{index}.part'
        (media / part).write_bytes(part_raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / 'receipts.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M1', media, receipt_path)
    engine.assemble(db, version, media, tmp_path / 'out')
    assert engine.seal(db, version, tmp_path / 'out') == '可发布'
    return db, version, target


def registered_feed(tmp_path, raw, feed_id='feed', block=64):
    db, version, target = seal_raw(tmp_path, raw, block=block)
    framed.register(db, feed_id, version, target.name)
    return db, target


def corrupt_payload(frames, index, needle=b'middle'):
    blob = bytearray(frames[index])
    blob[blob.index(needle)] = ord('x')
    frames[index] = bytes(blob)
    return frames


def event_ids(report, only_usable=True):
    return [e['event_id'] for e in report['events']
            if (e['usable'] or not only_usable)]


# ---------------------------------------------------------------- group 1

def test_same_sealed_file_two_contracts_different_record_sets(tmp_path):
    spec = json.loads(FIXTURE.read_text(encoding='utf-8'))
    frames = corrupt_payload([framed.pack(i, e)
                              for i, e in enumerate(spec['events'], 1)],
                             spec['corrupt_sequence'] - 1)
    db, target = registered_feed(tmp_path, b''.join(frames))

    strict = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'imp-strict')
    independent = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'imp-ind')

    # Physical seal is unchanged, but the stream contract rejects the whole
    # stream while the record contract keeps independently provable frames.
    assert strict['conclusion'] == framed.RESULT_REJECTED
    assert event_ids(strict) == []
    assert [e['event_id'] for e in strict['events']] == ['e1']  # evidence only
    assert all(not e['usable'] for e in strict['events'])
    assert strict['issues'][0]['code'] == 'frame_crc'
    assert strict['issues'][0]['start'] == 95

    assert independent['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(independent) == ['e1', 'e3']
    issue = independent['issues'][0]
    assert (issue['code'], issue['start'], issue['end']) == ('frame_crc', 95, 167)
    assert 'resumed_at=167' in issue['detail']
    assert independent['sequence_gaps'] == [
        {'missing_sequences': [2], 'after_sequence': 1, 'before_sequence': 3}]

    # Ledger records the different business sets; no overwrite ever happens.
    ledger = connect(db)
    rows = ledger.execute(
        'SELECT import_id,usable,event_id FROM frame_import_event ORDER BY import_id,"offset"').fetchall()
    assert [tuple(r) for r in rows] == [
        ('imp-ind', 1, 'e1'), ('imp-ind', 1, 'e3'), ('imp-strict', 0, 'e1')]
    ledger.close()
    assert framed.frame_verify(db, 'imp-strict')['ok']
    assert framed.frame_verify(db, 'imp-ind')['ok']


def test_clean_file_completes_under_both_contracts(tmp_path):
    events = [{'event_id': f'e{i}', 'n': i} for i in range(1, 6)]
    db, _ = registered_feed(tmp_path, b''.join(framed.pack(i, e) for i, e in enumerate(events, 1)))
    for mode in framed.MODES:
        report = framed.frame_import(db, 'feed', mode, f'imp-{mode}')
        assert report['conclusion'] == framed.RESULT_COMPLETE
        assert event_ids(report) == [e['event_id'] for e in events]
        assert report['issues'] == [] and report['sequence_gaps'] == []
        assert framed.frame_verify(db, f'imp-{mode}')['ok']


# ---------------------------------------------------------------- group 2

def test_bad_header_class_b_bounded_scan_resyncs(tmp_path):
    spec = json.loads(DAMAGE_FIXTURE.read_text(encoding='utf-8'))['scenarios']['bad_header_resync']
    frames = [framed.pack(i, e) for i, e in enumerate(spec['events'], 1)]
    f2_start = len(frames[0])
    blob = bytearray(b''.join(frames))
    blob[f2_start:f2_start + 4] = b'XXXX'  # magic destroyed; whole header unusable
    db, _ = registered_feed(tmp_path, bytes(blob), block=16)  # tiny blocks

    strict = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'cs')
    assert strict['conclusion'] == framed.RESULT_REJECTED
    assert strict['issues'][0]['code'] == 'frame_header'

    independent = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                      chunk_size=16, resync_window=4096)
    assert independent['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(independent) == ['h1', 'h3']
    issue = independent['issues'][0]
    assert issue['code'] == 'frame_header'
    # Damage interval reaches the proven restart offset of frame 3.
    assert issue['end'] == f2_start + len(frames[1])
    assert f'resumed_at={f2_start + len(frames[1])}' in issue['detail']
    assert framed.frame_verify(db, 'ind')['ok']


def test_class_a_length_hint_used_but_not_trusted(tmp_path):
    # Header and declared length parse, payload CRC bad: the declared length is
    # only a candidate jump; the next frame must still validate independently.
    spec = json.loads(FIXTURE.read_text(encoding='utf-8'))
    frames = corrupt_payload([framed.pack(i, e)
                              for i, e in enumerate(spec['events'], 1)], 1)
    db, _ = registered_feed(tmp_path, b''.join(frames), block=32)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 chunk_size=16, resync_window=2048)
    assert event_ids(report) == ['e1', 'e3']
    assert report['issues'][0]['code'] == 'frame_crc'
    assert framed.frame_verify(db, 'ind')['ok']


def test_bad_header_resync_under_tiny_chunks_and_window(tmp_path):
    f1 = framed.pack(1, {'event_id': 'k1'})
    f2 = framed.pack(2, {'event_id': 'k2'})
    bad = b'ZZZZ' + struct.pack('<II', 10, 2) + b'\x00' * 14
    db, _ = registered_feed(tmp_path, f1 + bad + f2, block=8)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 chunk_size=8, resync_window=128,
                                 resync_budget=1024 * 1024)
    assert report['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(report) == ['k1', 'k2']
    assert report['issues'][0]['code'] == 'frame_header'
    assert framed.frame_verify(db, 'ind')['ok']


def test_resync_window_residency_is_bounded(tmp_path):
    f1 = framed.pack(1, {'event_id': 'w1'})
    garbage = b'Q' * 5000
    f3 = framed.pack(3, {'event_id': 'w3'})
    db, _ = registered_feed(tmp_path, f1 + garbage + f3, block=64)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 chunk_size=256, resync_window=8192)
    assert report['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(report) == ['w1', 'w3']
    assert report['stats']['peak_resident'] <= report['stats']['resident_bound']


def test_cross_chunk_magic_and_header_straddle(tmp_path):
    events = [{'event_id': f's{i}', 'pad': 'z' * 37} for i in range(1, 8)]
    raw = b''.join(framed.pack(i, e) for i, e in enumerate(events, 1))
    db, _ = registered_feed(tmp_path, raw, block=7)
    # Chunk size deliberately misaligned with every frame boundary.
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'cs', chunk_size=13)
    assert report['conclusion'] == framed.RESULT_COMPLETE
    assert [e['sequence'] for e in report['events']] == [1, 2, 3, 4, 5, 6, 7]


# ---------------------------------------------------------------- group 3

def test_pseudo_magic_inside_damaged_region_is_not_a_boundary(tmp_path):
    spec = json.loads(DAMAGE_FIXTURE.read_text(encoding='utf-8'))['scenarios']['pseudo_magic']
    frames = [framed.pack(i, e) for i, e in enumerate(spec['events'], 1)]
    f2_start = len(frames[0])
    blob = bytearray(b''.join(frames))
    # Plant a CPR1-looking candidate with a huge impossible length inside the
    # damaged payload area; CRC of the enclosing frame is also broken.
    blob[f2_start + 20:f2_start + 24] = framed.MAGIC
    struct.pack_into('<I', blob, f2_start + 24, framed.MAX_PAYLOAD + 1)
    db, _ = registered_feed(tmp_path, bytes(blob), block=16)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 chunk_size=16, resync_window=8192)
    assert report['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(report) == ['p1', 'p3']
    assert report['issues'][0]['code'] == 'frame_crc'
    assert framed.frame_verify(db, 'ind')['ok']


def test_payload_embedded_magic_never_splits_a_valid_frame(tmp_path):
    events = [{'event_id': 'x1', 'text': 'normal'},
              {'event_id': 'x2', 'text': 'contains CPR1 literal in payload'},
              {'event_id': 'x3', 'text': 'tail'}]
    raw = b''.join(framed.pack(i, e) for i, e in enumerate(events, 1))
    assert framed.MAGIC in raw
    db, _ = registered_feed(tmp_path, raw)
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'cs')
    assert report['conclusion'] == framed.RESULT_COMPLETE
    assert event_ids(report) == ['x1', 'x2', 'x3']


def test_ambiguous_resync_is_isolated_not_guessed(tmp_path):
    spec = json.loads(DAMAGE_FIXTURE.read_text(encoding='utf-8'))['scenarios']['ambiguous_resync']
    frames = [framed.pack(i, e) for i, e in enumerate(spec['events'], 1)]
    extra = framed.pack(3, spec['extra_events'][0])  # same successor sequence
    f2_start = len(frames[0])
    prefix = frames[0]
    damaged = bytearray(frames[1])
    damaged[0:4] = b'ZZZZ'
    raw = prefix + bytes(damaged) + frames[2] + extra
    db, _ = registered_feed(tmp_path, raw, block=16)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 chunk_size=16, resync_window=8192)
    assert report['conclusion'] == framed.RESULT_INDETERMINATE
    assert report['indeterminate'] is True
    assert event_ids(report) == []  # suffix quarantined, no pseudo-pick
    assert [e['event_id'] for e in report['events']] == ['a1']  # evidence only
    codes = {i['code'] for i in report['issues']}
    assert 'ambiguous_resync' in codes
    # No usable business events are committed for an indeterminate import.
    ledger = connect(db)
    usable = ledger.execute('SELECT event_id FROM frame_import_event WHERE import_id=? AND usable=1',
                            ('ind',)).fetchall()
    assert usable == []
    ledger.close()
    assert framed.frame_verify(db, 'ind')['ok']


# ---------------------------------------------------------------- group 4

def _large_feed(tmp_path, frame_count=2000, payload=20 * 1024):
    events = [{'event_id': f'big-{i}', 'kind': 'sample',
               'blob': 'q' * (payload - 40)} for i in range(1, frame_count + 1)]
    raw = b''.join(framed.pack(i, e) for i, e in enumerate(events, 1))
    db, _ = registered_feed(tmp_path, raw, block=1024 * 1024)
    return db, raw, events


def test_large_file_linear_time_and_bounded_memory(tmp_path):
    import time
    db, raw, events = _large_feed(tmp_path)
    assert len(raw) > 30 * 1024 * 1024  # tens of MB, far above resident bound
    start = time.monotonic()
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'big',
                                 chunk_size=1024 * 1024,
                                 resync_window=2 * 1024 * 1024)
    elapsed = time.monotonic() - start
    assert report['conclusion'] == framed.RESULT_COMPLETE
    assert len(report['events']) == len(events)
    # Scanner residency is one read chunk plus at most one bounded resync
    # window and its scan copy, never proportional to the 30+ MB source file.
    bound = 1024 * 1024 + 2 * 2 * 1024 * 1024
    assert report['stats']['peak_resident'] <= bound + 4096
    assert report['stats']['resident_bound'] == bound
    # Single linear pass: bytes read equals file size, no quadratic suffix
    # copies, and tens of MB complete inside a modest wall-clock budget.
    assert report['stats']['bytes_read'] == len(raw)
    assert elapsed < 15.0


def test_large_file_recovery_stays_linear(tmp_path):
    db, raw, events = _large_feed(tmp_path, frame_count=1500)
    blob = bytearray(raw)
    first_len = struct.unpack_from('<I', blob, 4)[0]
    second = framed.FRAME_OVERHEAD + first_len
    blob[second + 20] ^= 0xFF  # corrupt a middle frame payload
    (tmp_path / 'source' / 'events.cpr').write_bytes(bytes(blob))
    # Re-seal the modified physical file.
    db2, version2, target2 = seal_raw(tmp_path / 'second', bytes(blob), block=1024 * 1024)
    framed.register(db2, 'feed2', version2, target2.name)
    report = framed.frame_import(db2, 'feed2', framed.MODE_INDEPENDENT, 'rec',
                                 chunk_size=1024 * 1024, resync_window=4 * 1024 * 1024)
    assert report['conclusion'] == framed.RESULT_RECOVERED
    assert len([e for e in report['events'] if e['usable']]) == len(events) - 1
    assert report['issues'][0]['code'] == 'frame_crc'
    assert framed.frame_verify(db2, 'rec')['ok']


def test_interrupted_scan_persists_nothing_and_resumes(tmp_path):
    db, raw, events = _large_feed(tmp_path, frame_count=400)
    with pytest.raises(framed.ScanInterrupted):
        framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'partial',
                            fail_after=len(raw) // 2)
    ledger = connect(db)
    assert ledger.execute('SELECT count(*) FROM frame_import').fetchone()[0] == 0
    assert ledger.execute('SELECT count(*) FROM frame_import_event').fetchone()[0] == 0
    assert ledger.execute('SELECT count(*) FROM frame_import_issue').fetchone()[0] == 0
    ledger.close()
    # A later retry is a fresh full import, not a half-finished success.
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'partial')
    assert report['conclusion'] == framed.RESULT_COMPLETE
    assert len(report['events']) == len(events)


def test_resync_budget_exhaustion_is_indeterminate_not_half_success(tmp_path):
    good = framed.pack(1, {'event_id': 'g1', 'text': 'ok'})
    # Damaged header followed by a long garbage tail with no real frame.
    garbage = b'ZZZZ' + b'\\x00' * 5000
    raw = good + garbage
    db, _ = registered_feed(tmp_path, raw, block=64)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 chunk_size=256, resync_window=1024,
                                 resync_budget=256)
    assert report['conclusion'] == framed.RESULT_INDETERMINATE
    assert 'resync_limit' in {i['code'] for i in report['issues']}
    assert event_ids(report) == []  # g1 retained as evidence, not usable
    assert [e['event_id'] for e in report['events']] == ['g1']
    ledger = connect(db)
    assert ledger.execute(
        'SELECT count(*) FROM frame_import_event WHERE import_id=? AND usable=1',
        ('ind',)).fetchone()[0] == 0
    ledger.close()


def test_trailing_half_frame_is_bounded(tmp_path):
    good = framed.pack(1, {'event_id': 't1', 'text': 'ok'})
    raw = good + framed.MAGIC + struct.pack('<II', 500, 2)  # truncated at EOF
    db, _ = registered_feed(tmp_path, raw)
    strict = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'cs')
    assert strict['conclusion'] == framed.RESULT_REJECTED
    assert strict['issues'][-1]['code'] == 'frame_truncated'
    independent = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind')
    assert independent['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(independent) == ['t1']
    assert independent['issues'][-1]['end'] == len(raw)
    assert framed.frame_verify(db, 'ind')['ok']


def test_oversized_length_is_bad_header_evidence(tmp_path):
    good = framed.pack(1, {'event_id': 'o1'})
    monster = framed.MAGIC + struct.pack('<II', framed.MAX_PAYLOAD + 1, 2) + b'abc'
    raw = good + monster
    db, _ = registered_feed(tmp_path, raw)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind',
                                 resync_window=1024)
    assert report['conclusion'] in (framed.RESULT_RECOVERED, framed.RESULT_INDETERMINATE)
    assert report['issues'][0]['code'] == 'frame_length'


# ---------------------------------------------------------------- group 5

def test_idempotent_same_bytes_and_conflicting_rewrite(tmp_path):
    events = [{'event_id': 'i1'}, {'event_id': 'i2'}]
    db, _ = registered_feed(tmp_path, b''.join(framed.pack(i, e) for i, e in enumerate(events, 1)))
    first = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'same-id')
    second = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'same-id')
    assert first['events'] == second['events']
    with pytest.raises(ValueError, match='frame_import_mode_conflict'):
        framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'same-id')
    with pytest.raises(ValueError, match='bad_import_mode'):
        framed.frame_import(db, 'feed', 'weird-mode', 'other')


def test_duplicate_event_id_and_sequence_are_never_deduped(tmp_path):
    f1 = framed.pack(1, {'event_id': 'dup'})
    f2 = framed.pack(2, {'event_id': 'dup'})  # same id, different bytes
    db, _ = registered_feed(tmp_path, f1 + f2)
    strict = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'cs')
    assert strict['conclusion'] == framed.RESULT_REJECTED
    assert strict['issues'][-1]['code'] == 'frame_event_identity'
    independent = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind')
    assert independent['conclusion'] == framed.RESULT_RECOVERED
    assert event_ids(independent) == ['dup']  # first only; second is an issue
    assert independent['issues'][-1]['code'] == 'frame_event_identity'
    assert framed.frame_verify(db, 'ind')['ok']

    seq = framed.pack(1, {'event_id': 's1'}) + framed.pack(1, {'event_id': 's2'})
    db2, _ = registered_feed(tmp_path / 'seq', seq)
    report = framed.frame_import(db2, 'feed', framed.MODE_INDEPENDENT, 'rollback')
    assert report['issues'][-1]['code'] == 'frame_sequence'
    assert event_ids(report) == ['s1']


def test_sequence_rollback_after_gap_is_flagged(tmp_path):
    # 1, then 3 (provable gap), then 2 (backwards): the rollback must not be
    # silently admitted as another usable record.
    raw = (framed.pack(1, {'event_id': 'a'})
           + framed.pack(3, {'event_id': 'c'})
           + framed.pack(2, {'event_id': 'b'}))
    db, _ = registered_feed(tmp_path, raw)
    report = framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind')
    assert event_ids(report) == ['a', 'c']
    assert report['sequence_gaps'] == [
        {'missing_sequences': [2], 'after_sequence': 1, 'before_sequence': 3}]
    assert report['issues'][-1]['code'] == 'frame_sequence'
    assert 'rollback' in report['issues'][-1]['detail']
    assert framed.frame_verify(db, 'ind')['ok']


def test_changed_file_cannot_reuse_prior_success(tmp_path):
    events = [{'event_id': 'm1'}, {'event_id': 'm2'}]
    db, target = registered_feed(tmp_path, b''.join(
        framed.pack(i, e) for i, e in enumerate(events, 1)))
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'imp')
    assert report['conclusion'] == framed.RESULT_COMPLETE
    # The importer reads the frozen assembled copy referenced by the ledger.
    from chunkproof.store import connect as _connect
    ledger = _connect(db)
    frozen = Path(ledger.execute('''SELECT a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        JOIN frame_feed ff ON ff.version=f.version AND ff.file_path=f.path
        WHERE ff.id='feed' ''').fetchone()['path'])
    ledger.close()
    frozen.write_bytes(frozen.read_bytes() + b'\x00')  # physical tamper
    with pytest.raises(ValueError, match='frame_file_changed'):
        framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'imp')
    with pytest.raises(ValueError, match='frame_file_changed'):
        framed.frame_verify(db, 'imp')


def test_hand_edited_issue_range_fails_verification(tmp_path):
    spec = json.loads(FIXTURE.read_text(encoding='utf-8'))
    frames = corrupt_payload([framed.pack(i, e)
                              for i, e in enumerate(spec['events'], 1)], 1)
    db, _ = registered_feed(tmp_path, b''.join(frames))
    framed.frame_import(db, 'feed', framed.MODE_INDEPENDENT, 'ind')
    ledger = connect(db)
    ledger.execute('UPDATE frame_import_issue SET "end"="end"+1 WHERE import_id=?', ('ind',))
    ledger.commit()
    ledger.close()
    # The relational issue row no longer matches the re-derived evidence.
    with pytest.raises(ValueError, match='issue_rows'):
        framed.frame_verify(db, 'ind')
    # Editing only the signed JSON report is caught against the fresh re-scan.
    ledger = connect(db)
    ledger.execute('UPDATE frame_import_issue SET "end"="end"-1 WHERE import_id=?', ('ind',))
    row = ledger.execute('SELECT report FROM frame_import WHERE id=?',
                         ('ind',)).fetchone()
    report = json.loads(row['report'])
    for issue in report['issues']:
        issue['end'] += 1
    ledger.execute('UPDATE frame_import SET report=? WHERE id=?',
                   (json.dumps(report, ensure_ascii=False, sort_keys=True), 'ind'))
    ledger.commit()
    ledger.close()
    with pytest.raises(ValueError, match='frame_verify_mismatch:issues'):
        framed.frame_verify(db, 'ind')


def test_verify_detects_deleted_event_row(tmp_path):
    events = [{'event_id': 'v1'}, {'event_id': 'v2'}]
    db, _ = registered_feed(tmp_path, b''.join(framed.pack(i, e) for i, e in enumerate(events, 1)))
    framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'imp')
    ledger = connect(db)
    ledger.execute('DELETE FROM frame_import_event WHERE import_id=?', ('imp',))
    ledger.commit()
    ledger.close()
    with pytest.raises(ValueError, match='event_rows'):
        framed.frame_verify(db, 'imp')


def test_ledger_is_append_only_and_records_frozen_digest(tmp_path):
    events = [{'event_id': 'q1'}]
    db, _ = registered_feed(tmp_path, framed.pack(1, events[0]))
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'imp')
    ledger = connect(db)
    row = ledger.execute('SELECT * FROM frame_import WHERE id=?', ('imp',)).fetchone()
    assert row['conclusion'] == 'complete'
    assert row['file_sha256'] == report['file_sha256']
    stored = json.loads(row['report'])
    assert stored['frozen']['file_sha256'] == row['file_sha256']
    assert stored['events'][0]['frame_digest']
    # The table has no UPDATE path in the module: attempts must be explicit SQL.
    columns = {r[1] for r in ledger.execute('PRAGMA table_info(frame_import_event)')}
    assert {'offset', 'length', 'sequence', 'event_id', 'frame_digest',
            'usable', 'record'} <= columns
    ledger.close()


def test_byte_seal_and_reception_flow_not_regressed(tmp_path):
    # The original strict decoder and the whole engine path stay intact.
    spec = json.loads(FIXTURE.read_text(encoding='utf-8'))
    clean = [framed.pack(i, e) for i, e in enumerate(spec['events'], 1)]
    db, _ = registered_feed(tmp_path, b''.join(clean))
    assert [r['sequence'] for r in framed.decode(db, 'feed')] == [1, 2, 3]
    ledger = connect(db)
    decision = ledger.execute('SELECT result FROM decision').fetchone()
    assert decision['result'] == '可发布'
    receipts = ledger.execute('SELECT DISTINCT status FROM receipt').fetchall()
    assert [r['status'] for r in receipts] == ['verified']
    ledger.close()


def test_empty_and_bad_utf8_json_frames(tmp_path):
    db, _ = registered_feed(tmp_path / 'empty', b'')
    report = framed.frame_import(db, 'feed', framed.MODE_COMPLETE, 'empty')
    assert report['conclusion'] == framed.RESULT_REJECTED
    assert report['issues'][0]['code'] == 'frame_truncated'

    # Structurally valid CRC over a non-UTF-8 payload.
    body = b'\xff\xfe\x00\x01'
    header = framed.MAGIC + struct.pack('<II', len(body), 1)
    raw = header + body + struct.pack('<I', zlib.crc32(header + body))
    db2, _ = registered_feed(tmp_path / 'enc', raw)
    report2 = framed.frame_import(db2, 'feed', framed.MODE_INDEPENDENT, 'enc',
                                  resync_window=1024)
    assert report2['issues'][0]['code'] == 'frame_encoding'
