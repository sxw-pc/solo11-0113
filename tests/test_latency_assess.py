"""Resumable bounded-memory exact p95 assessments over sealed JSONL sources."""
import json
import math
import subprocess
import sys
from pathlib import Path

import pytest

from chunkproof import engine, latency
from chunkproof.store import connect

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'fixtures' / 'latency'


def seal_files(tmp_path, files, block=1 << 20):
    src = tmp_path / 'src'
    src.mkdir(parents=True)
    for relative, data in files.items():
        target = src / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data if isinstance(data, bytes) else data.encode('utf-8'))
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'latency-delivery', src, block)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (src / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
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


def register(db, tmp_path, version, paths, batch='batch-1'):
    spec = tmp_path / 'spec.json'
    spec.write_text(json.dumps({'paths': sorted(paths)}), encoding='utf-8')
    latency.register(db, batch, version, spec)
    return batch


def fixture_files():
    return {name: (FIXTURE / name).read_bytes()
            for name in ('latency-a.jsonl', 'latency-b.jsonl')}


def sealed_fixture(tmp_path):
    db, version = seal_files(tmp_path, fixture_files())
    register(db, tmp_path, version, ['latency-a.jsonl', 'latency-b.jsonl'])
    return db, version


def jsonl(rows):
    return ''.join(json.dumps({'sample_id': ident, 'latency_ms': value}) + '\n'
                   for ident, value in rows)


class Crash(Exception):
    pass


def crash_on(event_name, events):
    def hook(event):
        events.append(event)
        if event == event_name:
            raise Crash(event)
    return hook


def assessment_row(db, assessment_id):
    conn = connect(db)
    row = conn.execute('SELECT * FROM latency_assessment WHERE id=?', (assessment_id,)).fetchone()
    conn.close()
    return row


# --- acceptance group 1: five-line fixture under tight memory ---------------

def test_five_line_fixture_bounded_memory_exact(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    with pytest.raises(ValueError, match='latency_memory_limit'):
        latency.evaluate(db, 'batch-1', 80, max_rows=4)
    result = latency.assess(db, 'batch-1', 80, 'fixture-strict',
                            memory_rows=4, scratch_bytes=4096, scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'issued'
    assert result['samples'] == 5 and result['rank'] == 5
    assert result['p95_ms'] == 100 and result['sla'] == 'fail'
    evidence = result['evidence']
    assert evidence['count_less'] == 4 and evidence['count_le'] == 5
    assert evidence['count_less'] < evidence['rank'] <= evidence['count_le']
    assert latency.recheck(db, 'fixture-strict')['verified'] is True


def test_assess_hashes_sources_in_chunks(tmp_path, monkeypatch):
    db, _ = sealed_fixture(tmp_path)

    def forbidden(self, *args, **kwargs):
        raise AssertionError('read_bytes must not swallow whole inputs')

    monkeypatch.setattr(Path, 'read_bytes', forbidden)
    result = latency.assess(db, 'batch-1', 80, 'chunked', 4, 4096,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'issued' and result['p95_ms'] == 100


# --- acceptance group 2: multi-file exactness and id uniqueness -------------

def test_multi_file_exact_p95_not_per_file_average(tmp_path):
    files = {'a.jsonl': jsonl([(f'a{i}', 1) for i in range(100)]),
             'b.jsonl': jsonl([(f'b{i}', 50) for i in range(100)]),
             'c.jsonl': jsonl([(f'c{i}', 100) for i in range(100)])}
    db, version = seal_files(tmp_path, files)
    register(db, tmp_path, version, sorted(files))
    # Global nearest-rank p95 is 100; averaging per-file p95 would give ~50.
    passed = latency.assess(db, 'batch-1', 100, 'trap-pass', 8, 1 << 20,
                            scratch_dir=tmp_path / 's1')
    assert passed['verdict'] == 'issued' and passed['p95_ms'] == 100
    assert passed['samples'] == 300 and passed['rank'] == 285
    assert passed['sla'] == 'pass'
    failed = latency.assess(db, 'batch-1', 99, 'trap-fail', 8, 1 << 20,
                            scratch_dir=tmp_path / 's2')
    assert failed['verdict'] == 'issued' and failed['sla'] == 'fail'


def test_duplicate_id_caught_across_merge_groups(tmp_path):
    # memory_rows=2 -> four id runs; the duplicate pair lands in different
    # merge groups, so only the final ordered merge can catch it.
    rows = [('a', 5), ('b', 6), ('c', 7), ('d', 8), ('e', 9), ('f', 10), ('g', 11), ('a', 12)]
    db, version = seal_files(tmp_path, {'lat.jsonl': jsonl(rows)})
    register(db, tmp_path, version, ['lat.jsonl'])
    result = latency.assess(db, 'batch-1', 80, 'dup-merge', 2, 65536,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'duplicate_sample_id'
    assert result['detail'] == 'a'


def test_duplicate_sample_id_across_files_invalid(tmp_path):
    files = {'a.jsonl': jsonl([('x', 1), ('y', 2)]), 'b.jsonl': jsonl([('x', 3)])}
    db, version = seal_files(tmp_path, files)
    register(db, tmp_path, version, sorted(files))
    events = []
    result = latency.assess(db, 'batch-1', 80, 'dup', 4, 4096,
                            scratch_dir=tmp_path / 's',
                            _hook=lambda event: events.append(event))
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'duplicate_sample_id'
    assert result['detail'] == 'x'
    assert 'sla' not in result and 'p95_ms' not in result
    assert assessment_row(db, 'dup')['status'] == 'invalid'
    events.clear()
    again = latency.assess(db, 'batch-1', 80, 'dup', 4, 4096,
                           scratch_dir=tmp_path / 's',
                           _hook=lambda event: events.append(event))
    assert again == result and events == []


BAD_LINES = [
    json.dumps({'sample_id': 'x', 'latency_ms': -1}),
    json.dumps({'sample_id': 'x', 'latency_ms': 1.5}),
    json.dumps({'sample_id': 'x', 'latency_ms': True}),
    json.dumps({'sample_id': 'x'}),
    json.dumps({'sample_id': 'x', 'latency_ms': 1, 'extra': 1}),
    json.dumps({'sample_id': '', 'latency_ms': 1}),
    json.dumps({'sample_id': 5, 'latency_ms': 1}),
    json.dumps({'sample_id': 'x', 'latency_ms': 2 ** 63}),
    '{"sample_id": "x", "latency_ms": 1',
    '{"sample_id": "x", "sample_id": "y", "latency_ms": 1}',
]


@pytest.mark.parametrize('line', BAD_LINES)
def test_bad_records_invalid(tmp_path, line):
    db, version = seal_files(tmp_path, {'bad.jsonl': line + '\n'})
    register(db, tmp_path, version, ['bad.jsonl'])
    result = latency.assess(db, 'batch-1', 80, 'bad-1', 4, 4096,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'bad_latency_record'
    assert 'sla' not in result


def test_invalid_utf8_invalid(tmp_path):
    db, version = seal_files(tmp_path, {'bad.jsonl': b'\xff\xfe\xfd\n'})
    register(db, tmp_path, version, ['bad.jsonl'])
    result = latency.assess(db, 'batch-1', 80, 'utf8', 4, 4096,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'bad_latency_record'


def test_empty_batch_invalid(tmp_path):
    db, version = seal_files(tmp_path, {'e.jsonl': '\n\n  \n'})
    register(db, tmp_path, version, ['e.jsonl'])
    result = latency.assess(db, 'batch-1', 80, 'empty', 4, 4096,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'empty_latency_batch'


def test_boundary_rank_on_plateau(tmp_path):
    small = [(f's{i}', i) for i in range(1, 16)]
    plateau = [(f'p{i}', 50) for i in range(5)]
    files = {'a.jsonl': jsonl(small[:8] + plateau[:2]), 'b.jsonl': jsonl(small[8:] + plateau[2:])}
    db, version = seal_files(tmp_path, files)
    register(db, tmp_path, version, sorted(files))
    result = latency.assess(db, 'batch-1', 50, 'plateau', 4, 1 << 20,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'issued'
    assert result['samples'] == 20 and result['rank'] == 19
    assert result['p95_ms'] == 50 and result['sla'] == 'pass'
    assert result['evidence']['count_less'] == 15
    assert result['evidence']['count_le'] == 20


def test_single_sample_single_memory_row(tmp_path):
    db, version = seal_files(tmp_path, {'one.jsonl': jsonl([('only', 42)])})
    register(db, tmp_path, version, ['one.jsonl'])
    result = latency.assess(db, 'batch-1', 42, 'one', 1, 1024,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'issued'
    assert result['rank'] == 1 and result['p95_ms'] == 42 and result['sla'] == 'pass'


# --- acceptance group 3: streaming hash and source drift --------------------

def test_source_drift_invalidates(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    target = tmp_path / 'out' / 'latency-a.jsonl'
    target.write_text(target.read_text(encoding='utf-8') + ' ', encoding='utf-8')
    result = latency.assess(db, 'batch-1', 80, 'drift', 4, 4096,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'latency_bytes_changed'
    assert 'sla' not in result
    with pytest.raises(ValueError, match='assessment_not_issued'):
        latency.recheck(db, 'drift')


def test_source_drift_after_checkpoint_discards_run(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    events = []
    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 80, 'drift-late', 4, 4096,
                       scratch_dir=tmp_path / 's',
                       _hook=crash_on('checkpoint:validated', events))
    target = tmp_path / 'out' / 'latency-b.jsonl'
    target.write_text('tampered\n', encoding='utf-8')
    result = latency.assess(db, 'batch-1', 80, 'drift-late', 4, 4096,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'invalid'
    assert result['reason'] == 'latency_bytes_changed'
    assert assessment_row(db, 'drift-late')['status'] == 'invalid'


# --- acceptance group 4: interrupt, resume, scratch corruption --------------

def test_resume_from_stable_checkpoints(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    scratch = tmp_path / 's'
    events = []
    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 80, 'resume', 4, 4096, scratch_dir=scratch,
                       _hook=crash_on('checkpoint:validated', events))
    row = assessment_row(db, 'resume')
    assert row['status'] == 'running' and row['result'] is None
    assert json.loads(row['checkpoint'])['phase'] == 'validated'
    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 80, 'resume', 4, 4096, scratch_dir=scratch,
                       _hook=crash_on('checkpoint:ids_checked', events))
    events.clear()
    result = latency.assess(db, 'batch-1', 80, 'resume', 4, 4096, scratch_dir=scratch,
                            _hook=lambda event: events.append(event))
    assert result['verdict'] == 'issued' and result['p95_ms'] == 100
    assert result['sla'] == 'fail'
    assert 'run:value' not in events  # phase 1 was not redone
    events.clear()
    again = latency.assess(db, 'batch-1', 80, 'resume', 4, 4096, scratch_dir=scratch,
                           _hook=lambda event: events.append(event))
    assert again == result and events == []  # issued result returned directly


def test_resume_mid_merge_stays_exact(tmp_path):
    rows = [(f's{i}', (i * 37) % 101) for i in range(40)]
    db, version = seal_files(tmp_path, {'lat.jsonl': jsonl(rows)})
    register(db, tmp_path, version, ['lat.jsonl'])
    scratch = tmp_path / 's'
    events = []
    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 50, 'mid-merge', 4, 65536, scratch_dir=scratch,
                       _hook=crash_on('checkpoint:merged', events))
    assert assessment_row(db, 'mid-merge')['status'] == 'running'
    result = latency.assess(db, 'batch-1', 50, 'mid-merge', 4, 65536,
                            scratch_dir=scratch)
    expected = sorted(value for _, value in rows)
    rank = math.ceil(95 * len(expected) / 100)
    assert result['verdict'] == 'issued'
    assert result['rank'] == rank and result['p95_ms'] == expected[rank - 1]


def test_corrupted_scratch_segment_discarded(tmp_path):
    rows = [(f's{i}', (i * 13) % 57) for i in range(40)]
    db, version = seal_files(tmp_path, {'lat.jsonl': jsonl(rows)})
    register(db, tmp_path, version, ['lat.jsonl'])
    scratch = tmp_path / 's'
    events = []
    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 50, 'corrupt', 4, 65536, scratch_dir=scratch,
                       _hook=crash_on('checkpoint:validated', events))
    victim = next(scratch.glob('v*.bin'))
    data = bytearray(victim.read_bytes())
    data[0] ^= 0xFF
    victim.write_bytes(bytes(data))
    result = latency.assess(db, 'batch-1', 50, 'corrupt', 4, 65536,
                            scratch_dir=scratch)
    expected = sorted(value for _, value in rows)
    rank = math.ceil(95 * len(expected) / 100)
    assert result['verdict'] == 'issued'
    assert result['p95_ms'] == expected[rank - 1]


def test_missing_scratch_segment_discarded(tmp_path):
    rows = [(f's{i}', (i * 7) % 33) for i in range(30)]
    db, version = seal_files(tmp_path, {'lat.jsonl': jsonl(rows)})
    register(db, tmp_path, version, ['lat.jsonl'])
    scratch = tmp_path / 's'
    events = []
    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 50, 'missing', 4, 65536, scratch_dir=scratch,
                       _hook=crash_on('checkpoint:validated', events))
    next(scratch.glob('i*.bin')).unlink()
    result = latency.assess(db, 'batch-1', 50, 'missing', 4, 65536,
                            scratch_dir=scratch)
    expected = sorted(value for _, value in rows)
    rank = math.ceil(95 * len(expected) / 100)
    assert result['verdict'] == 'issued'
    assert result['p95_ms'] == expected[rank - 1]


def test_crash_mid_phase_leaves_no_orphans(tmp_path):
    rows = [(f's{i}', (i * 11) % 43) for i in range(30)]
    db, version = seal_files(tmp_path, {'lat.jsonl': jsonl(rows)})
    register(db, tmp_path, version, ['lat.jsonl'])
    scratch = tmp_path / 's'
    events = []

    def hook(event):
        events.append(event)
        if events.count('run:value') == 2:
            raise Crash('mid-phase')

    with pytest.raises(Crash):
        latency.assess(db, 'batch-1', 20, 'mid-phase', 4, 65536,
                       scratch_dir=scratch, _hook=hook)
    assert assessment_row(db, 'mid-phase')['status'] == 'running'
    result = latency.assess(db, 'batch-1', 20, 'mid-phase', 4, 65536,
                            scratch_dir=scratch)
    expected = sorted(value for _, value in rows)
    rank = math.ceil(95 * len(expected) / 100)
    assert result['verdict'] == 'issued'
    assert result['p95_ms'] == expected[rank - 1]
    assert not scratch.exists()


@pytest.mark.parametrize('seed', range(20))
def test_assess_matches_evaluate_on_random_small_batches(tmp_path, seed):
    import random
    rng = random.Random(seed)
    rows = [(f's{i}', rng.randrange(0, 30)) for i in range(rng.randrange(1, 60))]
    db, version = seal_files(tmp_path, {'r.jsonl': jsonl(rows)})
    register(db, tmp_path, version, ['r.jsonl'])
    expected = latency.evaluate(db, 'batch-1', 25, max_rows=1000)
    result = latency.assess(db, 'batch-1', 25, 'cross', rng.randrange(2, 9), 1 << 20,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'issued'
    assert result['samples'] == expected['samples']
    assert result['p95_ms'] == expected['p95_ms']
    assert result['sla'] == expected['sla']


def test_assessment_identity_conflict_and_issued_immutability(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    scratch = tmp_path / 's'
    result = latency.assess(db, 'batch-1', 80, 'identity', 4, 4096, scratch_dir=scratch)
    assert result['verdict'] == 'issued'
    with pytest.raises(ValueError, match='assessment_identity_conflict'):
        latency.assess(db, 'batch-1', 90, 'identity', 4, 4096, scratch_dir=scratch)
    with pytest.raises(ValueError, match='assessment_identity_conflict'):
        latency.assess(db, 'batch-1', 80, 'identity', 8, 4096, scratch_dir=scratch)
    # Tampering after issuance cannot rewrite the issued conclusion.
    target = tmp_path / 'out' / 'latency-a.jsonl'
    target.write_text('tampered\n', encoding='utf-8')
    again = latency.assess(db, 'batch-1', 80, 'identity', 4, 4096, scratch_dir=scratch)
    assert again == result
    check = latency.recheck(db, 'identity')
    assert check['verified'] is False and check['reason'] == 'latency_bytes_changed'
    fresh = latency.assess(db, 'batch-1', 80, 'identity-2', 4, 4096,
                           scratch_dir=tmp_path / 's2')
    assert fresh['verdict'] == 'invalid' and fresh['reason'] == 'latency_bytes_changed'


# --- acceptance group 5: large input, budgets, idempotency, regression ------

def test_large_input_exact_within_dual_budget(tmp_path):
    import random
    rng = random.Random(20261121)
    files, baseline = {}, []
    for f in range(3):
        rows = [(f'f{f}-s{i}', rng.randrange(0, 2000)) for i in range(33400)]
        baseline.extend(value for _, value in rows)
        files[f'lat-{f}.jsonl'] = jsonl(rows)
    assert sum(len(text.splitlines()) for text in files.values()) == 100200
    db, version = seal_files(tmp_path, files)
    register(db, tmp_path, version, sorted(files))
    scratch = tmp_path / 'scratch'
    baseline.sort()
    rank = math.ceil(95 * len(baseline) / 100)
    expected = baseline[rank - 1]
    result = latency.assess(db, 'batch-1', expected, 'big', memory_rows=256,
                            scratch_bytes=6 << 20, scratch_dir=scratch)
    assert result['verdict'] == 'issued'
    assert result['samples'] == len(baseline) and result['rank'] == rank
    assert result['p95_ms'] == expected and result['sla'] == 'pass'
    evidence = result['evidence']
    assert evidence['count_less'] == sum(1 for value in baseline if value < expected)
    assert evidence['count_le'] == sum(1 for value in baseline if value <= expected)
    assert evidence['count_less'] < rank <= evidence['count_le']
    assert not scratch.exists()  # scratch released after issuance
    again = latency.assess(db, 'batch-1', expected, 'big', 256, 6 << 20,
                           scratch_dir=scratch)
    assert again == result  # idempotent return of the issued result
    assert len(connect(db).execute('SELECT id FROM latency_assessment').fetchall()) == 1
    check = latency.recheck(db, 'big')
    assert check['verified'] is True
    assert check['observed']['samples'] == len(baseline)


def test_scratch_budget_indeterminate_with_reason(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    result = latency.assess(db, 'batch-1', 80, 'tight', 4, 8,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'indeterminate'
    assert result['reason'] == 'scratch_budget'
    detail = result['detail']
    assert detail['needed_bytes'] > detail['budget_bytes']
    assert 'sla' not in result and 'p95_ms' not in result
    assert assessment_row(db, 'tight')['status'] == 'indeterminate'
    # A later attempt with an adequate budget is a new assessment identity.
    ok = latency.assess(db, 'batch-1', 80, 'roomy', 4, 4096,
                        scratch_dir=tmp_path / 's2')
    assert ok['verdict'] == 'issued' and ok['p95_ms'] == 100


def test_memory_budget_indeterminate_when_merge_impossible(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    result = latency.assess(db, 'batch-1', 80, 'mem-tight', 1, 1 << 20,
                            scratch_dir=tmp_path / 's')
    assert result['verdict'] == 'indeterminate'
    assert result['reason'] == 'memory_budget'


def test_new_delivery_version_gets_new_assessment_old_stays_queryable(tmp_path):
    db, version = sealed_fixture(tmp_path)
    first = latency.assess(db, 'batch-1', 80, 'v1-assessment', 4, 4096,
                           scratch_dir=tmp_path / 's1')
    assert first['verdict'] == 'issued' and first['p95_ms'] == 100
    # A new delivery version with different bytes is sealed and registered anew.
    changed = {'latency-a.jsonl': jsonl([('a1', 10), ('a2', 20), ('a3', 99)]),
               'latency-b.jsonl': jsonl([('b1', 40), ('b2', 95)])}
    src2 = tmp_path / 'v2'
    db2, version2 = seal_files(src2, changed)
    assert version2 != version
    register(db2, src2, version2, ['latency-a.jsonl', 'latency-b.jsonl'], batch='batch-v2')
    second = latency.assess(db2, 'batch-v2', 80, 'v2-assessment', 4, 4096,
                            scratch_dir=src2 / 's2')
    assert second['verdict'] == 'issued' and second['p95_ms'] == 99
    stored = latency.status(db, 'v1-assessment')
    assert stored['status'] == 'issued'
    assert stored['result'] == first  # old conclusion queryable with old evidence
    assert latency.recheck(db, 'v1-assessment')['verified'] is True
    assert latency.status(db2, 'v2-assessment')['result'] == second
    with pytest.raises(ValueError, match='unknown_latency_assessment'):
        latency.status(db, 'no-such-assessment')


def test_cli_assess_and_recheck(tmp_path):
    db, _ = sealed_fixture(tmp_path)
    out = subprocess.run([sys.executable, '-B', '-m', 'chunkproof', 'latency-assess',
                          '--db', str(db), '--id', 'batch-1', '--assessment', 'cli-1',
                          '--limit-ms', '80', '--memory-rows', '4',
                          '--scratch-bytes', '4096', '--scratch-dir', str(tmp_path / 's')],
                         capture_output=True, text=True, check=True, cwd=ROOT)
    result = json.loads(out.stdout)
    assert result['verdict'] == 'issued' and result['p95_ms'] == 100
    assert result['sla'] == 'fail'
    out = subprocess.run([sys.executable, '-B', '-m', 'chunkproof', 'latency-recheck',
                          '--db', str(db), '--assessment', 'cli-1'],
                         capture_output=True, text=True, check=True, cwd=ROOT)
    assert json.loads(out.stdout)['verified'] is True
    out = subprocess.run([sys.executable, '-B', '-m', 'chunkproof', 'latency-assessment',
                          '--db', str(db), '--assessment', 'cli-1'],
                         capture_output=True, text=True, check=True, cwd=ROOT)
    stored = json.loads(out.stdout)
    assert stored['status'] == 'issued' and stored['result']['p95_ms'] == 100


def test_demo_end_to_end_still_runs():
    out = subprocess.run([sys.executable, '-B', '-m', 'chunkproof', 'demo'],
                         capture_output=True, text=True, check=True, cwd=ROOT)
    assert '可发布' in out.stdout
    assert '"verdict": "issued"' in out.stdout or '"verdict":"issued"' in out.stdout
