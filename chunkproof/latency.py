"""Small-batch exact latency quantile over sealed JSONL source files."""
import hashlib
import json
import math
import os
import shutil
import struct
from heapq import heappop, heappush
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


# ---------------------------------------------------------------------------
# Resumable exact assessment under bounded memory and bounded scratch.
#
# Strategy: one streaming pass verifies each frozen file's SHA-256 in chunks
# and strictly validates every JSONL record, spilling sorted runs of values
# and of sample ids to scratch (never more than memory_rows sample values or
# ids in memory).  Ids are merged with adjacency checks to prove batch-wide
# uniqueness; values are k-way merged (fan-in <= memory_rows) until the exact
# nearest-rank ceil(0.95*N) value is known.  Every stable phase is checkpointed
# into latency_assessment so an interrupted run resumes; on resume the frozen
# sources and every scratch segment are re-verified, and any drift or corrupt
# segment discards the computation instead of continuing it.  Budgets that
# cannot complete yield an indeterminate verdict with the exact resource
# shortfall; invalid data yields an invalid verdict and no SLA is ever issued
# from partial order statistics.
# ---------------------------------------------------------------------------

_HASH_CHUNK = 1024 * 1024
_MAX_LINE = 1024 * 1024
_MAX_FAN_IN = 128
_MAX_VALUE = (1 << 63) - 1
_MAX_ID = 65535
_VALUE = struct.Struct('<q')
_ID_LEN = struct.Struct('<H')
_END = object()


class _Invalid(Exception):
    """Data or integrity problem; maps to a terminal invalid verdict."""

    def __init__(self, reason, detail=None):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


class _Budget(Exception):
    """Resource budget cannot finish the exact computation."""

    def __init__(self, reason, detail):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


def _fire(hook, event):
    if hook is not None:
        hook(event)


def _hash_stream(path):
    """Chunked whole-file digest; never reads the file into memory."""
    digest = hashlib.sha256()
    size = 0
    with Path(path).open('rb') as stream:
        while part := stream.read(_HASH_CHUNK):
            digest.update(part)
            size += len(part)
    return digest.hexdigest(), size


def _frozen_files(db, version, paths):
    frozen = []
    for relative_path in paths:
        row = db.execute('''SELECT f.sha256, f.size, a.path FROM file_entry f
            JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
            WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
        if not row:
            raise ValueError('unassembled_latency_file')
        frozen.append({'path': relative_path, 'source': row['path'],
                       'sha256': row['sha256'], 'size': row['size']})
    return frozen


def _unique_pairs(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise _Invalid('bad_latency_record', 'duplicate key')
        obj[key] = value
    return obj


def _record_stream(path, relative_path):
    """Strictly validated (sample_id, latency_ms) stream over one JSONL file."""
    with Path(path).open('rb') as stream:
        number = 0
        while True:
            raw = stream.readline(_MAX_LINE + 1)
            if not raw:
                return
            number += 1
            if len(raw) > _MAX_LINE:
                raise _Invalid('bad_latency_record', f'{relative_path}:{number} oversized line')
            if not raw.strip():
                continue
            try:
                text = raw.decode('utf-8')
            except UnicodeDecodeError:
                raise _Invalid('bad_latency_record', f'{relative_path}:{number} utf-8') from None
            try:
                record = json.loads(text, object_pairs_hook=_unique_pairs)
            except json.JSONDecodeError:
                raise _Invalid('bad_latency_record', f'{relative_path}:{number} json') from None
            if not isinstance(record, dict) or set(record) != {'sample_id', 'latency_ms'}:
                raise _Invalid('bad_latency_record', f'{relative_path}:{number} keys')
            ident, value = record['sample_id'], record['latency_ms']
            if not isinstance(ident, str) or not ident or len(ident.encode('utf-8')) > _MAX_ID:
                raise _Invalid('bad_latency_record', f'{relative_path}:{number} sample_id')
            if type(value) is not int or not 0 <= value <= _MAX_VALUE:
                raise _Invalid('bad_latency_record', f'{relative_path}:{number} latency_ms')
            yield ident, value


class _Scratch:
    """Bounded scratch space; every byte written is reserved against the budget."""

    def __init__(self, root, budget):
        self.root = Path(root)
        self.budget = budget
        self.used = 0

    def reset(self):
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)
        self.used = 0

    def reconcile(self, listed):
        """Keep exactly the checkpointed segments; account their bytes."""
        self.root.mkdir(parents=True, exist_ok=True)
        keep = {entry['file'] for entry in listed}
        for item in self.root.iterdir():
            if item.name not in keep:
                item.unlink()
        self.used = sum(entry['bytes'] for entry in listed)

    def reserve(self, nbytes):
        if self.used + nbytes > self.budget:
            raise _Budget('scratch_budget', {'used_bytes': self.used,
                                             'needed_bytes': self.used + nbytes,
                                             'budget_bytes': self.budget})
        self.used += nbytes

    def release(self, nbytes):
        self.used = max(0, self.used - nbytes)


class _RunWriter:
    """One sorted run segment on scratch; bytes are reserved before writing."""

    def __init__(self, scratch, kind, seq):
        self.scratch = scratch
        self.path = scratch.root / f'{kind}{seq:06d}.bin'
        self.stream = self.path.open('wb')
        self.digest = hashlib.sha256()
        self.count = 0
        self.nbytes = 0

    def write(self, blob):
        self.scratch.reserve(len(blob))
        self.stream.write(blob)
        self.digest.update(blob)
        self.nbytes += len(blob)
        self.count += 1

    def close(self):
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.stream.close()
        return {'file': self.path.name, 'count': self.count,
                'bytes': self.nbytes, 'sha256': self.digest.hexdigest()}

    def abort(self):
        self.stream.close()
        self.scratch.release(self.nbytes)
        self.path.unlink(missing_ok=True)


def _read_run(entry, kind, scratch):
    path = scratch.root / entry['file']
    with path.open('rb') as stream:
        if kind == 'v':
            while data := stream.read(_VALUE.size):
                yield _VALUE.unpack(data)[0]
        else:
            while head := stream.read(_ID_LEN.size):
                (size,) = _ID_LEN.unpack(head)
                yield stream.read(size).decode('utf-8')


def _verify_run(scratch, entry):
    path = scratch.root / entry['file']
    if not path.is_file():
        return False
    digest, size = _hash_stream(path)
    return size == entry['bytes'] and digest == entry['sha256']


def _delete_entries(scratch, entries):
    for entry in entries:
        (scratch.root / entry['file']).unlink(missing_ok=True)
        scratch.release(entry['bytes'])


def _emit_merged(entries, kind, scratch):
    """K-way merge over sorted run segments; heap holds one item per run."""
    heap, streams = [], []
    for index, entry in enumerate(entries):
        stream = _read_run(entry, kind, scratch)
        streams.append(stream)
        item = next(stream, _END)
        if item is not _END:
            heappush(heap, (item, index, stream))
    try:
        while heap:
            item, index, stream = heappop(heap)
            yield item
            following = next(stream, _END)
            if following is not _END:
                heappush(heap, (following, index, stream))
    finally:
        for stream in streams:
            stream.close()


def _flush_runs(scratch, values, id_buf, value_runs, id_runs, seq, hook):
    if not values:
        return seq
    values.sort()
    writer = _RunWriter(scratch, 'v', seq)
    seq += 1
    for item in values:
        writer.write(_VALUE.pack(item))
    value_runs.append(writer.close())
    _fire(hook, 'run:value')
    writer = _RunWriter(scratch, 'i', seq)
    seq += 1
    for ident in sorted(id_buf):
        raw = ident.encode('utf-8')
        writer.write(_ID_LEN.pack(len(raw)) + raw)
    id_runs.append(writer.close())
    _fire(hook, 'run:id')
    values.clear()
    id_buf.clear()


def _phase_validate(frozen, scratch, memory_rows, seq, hook):
    """Single streaming pass: chunked hash check, strict parse, spill sorted runs."""
    values, id_buf, id_cache = [], [], set()
    value_runs, id_runs, sources = [], [], {}
    total = 0
    for entry in frozen:
        digest, size = _hash_stream(entry['source'])
        if digest != entry['sha256'] or size != entry['size']:
            raise _Invalid('latency_bytes_changed', entry['path'])
        sources[entry['path']] = {'sha256': digest, 'size': size}
        for ident, value in _record_stream(entry['source'], entry['path']):
            if ident in id_cache:
                raise _Invalid('duplicate_sample_id', ident)
            id_cache.add(ident)
            values.append(value)
            id_buf.append(ident)
            total += 1
            if len(values) >= memory_rows:
                _flush_runs(scratch, values, id_buf, value_runs, id_runs, seq, hook)
                seq += 2
                id_cache.clear()
    _flush_runs(scratch, values, id_buf, value_runs, id_runs, seq, hook)
    return total, value_runs, id_runs, sources, seq + 2


def _merge_pass(scratch, entries, kind, fan_in, seq, hook):
    """Merge groups of up to fan_in runs; returns (runs, seq, replaced)."""
    out, replaced = [], []
    for start in range(0, len(entries), fan_in):
        group = entries[start:start + fan_in]
        if len(group) == 1:
            out.append(group[0])
            continue
        writer = _RunWriter(scratch, kind, seq)
        seq += 1
        last = None
        try:
            for item in _emit_merged(group, kind, scratch):
                if kind == 'i':
                    if last is not None and item == last:
                        raise _Invalid('duplicate_sample_id', item)
                    raw = item.encode('utf-8')
                    writer.write(_ID_LEN.pack(len(raw)) + raw)
                else:
                    writer.write(_VALUE.pack(item))
                last = item
        except Exception:
            writer.abort()
            raise
        out.append(writer.close())
        _fire(hook, f'run:{kind}')
        replaced.extend(group)
    return out, seq, replaced


def _final_unique_check(scratch, id_runs):
    previous = _END
    for ident in _emit_merged(id_runs, 'i', scratch):
        if previous is not _END and ident == previous:
            raise _Invalid('duplicate_sample_id', ident)
        previous = ident


def _final_rank(scratch, value_runs, rank):
    value, emitted = None, 0
    for item in _emit_merged(value_runs, 'v', scratch):
        emitted += 1
        if emitted == rank:
            value = item
            break
    if value is None:
        raise RuntimeError('rank_beyond_samples')
    less = le = 0
    for entry in value_runs:
        for item in _read_run(entry, 'v', scratch):
            if item < value:
                less += 1
            if item <= value:
                le += 1
    if not less < rank <= le:
        raise RuntimeError('rank_evidence_inconsistent')
    return value, less, le


def _save_checkpoint(db, assessment_id, checkpoint):
    with db:
        db.execute('UPDATE latency_assessment SET checkpoint=?,updated=CURRENT_TIMESTAMP WHERE id=?',
                   (json.dumps(checkpoint), assessment_id))


def _finish(db, assessment_id, status, result, checkpoint=None):
    payload = json.dumps(result, ensure_ascii=False, sort_keys=True)
    with db:
        if checkpoint is None:
            db.execute('UPDATE latency_assessment SET status=?,result=?,updated=CURRENT_TIMESTAMP WHERE id=?',
                       (status, payload, assessment_id))
        else:
            db.execute('''UPDATE latency_assessment SET status=?,result=?,checkpoint=?,
                updated=CURRENT_TIMESTAMP WHERE id=?''',
                       (status, payload, json.dumps(checkpoint), assessment_id))


def _resume(db, assessment_id, checkpoint, frozen, scratch):
    """Re-verify frozen sources and checkpointed segments before continuing."""
    for entry in frozen:
        digest, size = _hash_stream(entry['source'])
        if digest != entry['sha256'] or size != entry['size']:
            raise _Invalid('latency_bytes_changed', entry['path'])
    listed = checkpoint.get('value_runs', []) + checkpoint.get('id_runs', [])
    scratch.reconcile(listed)
    if all(_verify_run(scratch, entry) for entry in listed):
        return checkpoint
    checkpoint = {'phase': 'start', 'seq': checkpoint.get('seq', 1)}
    _save_checkpoint(db, assessment_id, checkpoint)
    return checkpoint


def _compute(db, assessment_id, frozen, scratch, memory_rows, limit_ms,
             checkpoint, hook, base, spec_hash, fingerprint):
    fan_in = min(memory_rows, _MAX_FAN_IN)
    seq = checkpoint.get('seq', 1)
    while True:
        phase = checkpoint['phase']
        if phase == 'start':
            scratch.reset()
            total, value_runs, id_runs, sources, seq = _phase_validate(
                frozen, scratch, memory_rows, seq, hook)
            if total == 0:
                raise _Invalid('empty_latency_batch')
            checkpoint = {'phase': 'validated', 'total_rows': total,
                          'rank': (95 * total + 99) // 100,
                          'value_runs': value_runs, 'id_runs': id_runs,
                          'sources': sources, 'seq': seq}
            _save_checkpoint(db, assessment_id, checkpoint)
            _fire(hook, 'checkpoint:validated')
        elif phase == 'validated':
            id_runs = checkpoint['id_runs']
            while len(id_runs) > fan_in:
                if fan_in < 2:
                    raise _Budget('memory_budget', {'memory_rows': memory_rows, 'fan_in': fan_in,
                                                    'runs': len(id_runs),
                                                    'reason': 'merge fan-in below 2'})
                id_runs, seq, replaced = _merge_pass(scratch, id_runs, 'i', fan_in, seq, hook)
                checkpoint = {**checkpoint, 'id_runs': id_runs, 'seq': seq}
                _save_checkpoint(db, assessment_id, checkpoint)
                _fire(hook, 'checkpoint:merged')
                _delete_entries(scratch, replaced)
            _final_unique_check(scratch, id_runs)
            checkpoint = {**checkpoint, 'phase': 'ids_checked', 'id_runs': [], 'seq': seq}
            _save_checkpoint(db, assessment_id, checkpoint)
            _fire(hook, 'checkpoint:ids_checked')
            _delete_entries(scratch, id_runs)
        elif phase in ('ids_checked', 'merged'):
            value_runs = checkpoint['value_runs']
            while len(value_runs) > fan_in:
                if fan_in < 2:
                    raise _Budget('memory_budget', {'memory_rows': memory_rows, 'fan_in': fan_in,
                                                    'runs': len(value_runs),
                                                    'reason': 'merge fan-in below 2'})
                value_runs, seq, replaced = _merge_pass(scratch, value_runs, 'v', fan_in, seq, hook)
                checkpoint = {**checkpoint, 'phase': 'merged', 'value_runs': value_runs, 'seq': seq}
                _save_checkpoint(db, assessment_id, checkpoint)
                _fire(hook, 'checkpoint:merged')
                _delete_entries(scratch, replaced)
            value, less, le = _final_rank(scratch, value_runs, checkpoint['rank'])
            evidence = {'definition': 'nearest-rank:ceil(0.95*N)',
                        'samples': checkpoint['total_rows'], 'rank': checkpoint['rank'],
                        'value': value, 'count_less': less, 'count_le': le,
                        'files': [{'path': path, 'sha256': source['sha256'], 'size': source['size']}
                                  for path, source in checkpoint['sources'].items()],
                        'spec_hash': spec_hash, 'fingerprint': fingerprint,
                        'memory_rows': memory_rows, 'scratch_bytes': scratch.budget}
            result = {**base, 'verdict': 'issued', 'samples': checkpoint['total_rows'],
                      'rank': checkpoint['rank'], 'p95_ms': value,
                      'sla': 'pass' if value <= limit_ms else 'fail', 'evidence': evidence}
            return checkpoint, result


def _default_scratch_root(db_path, assessment_id):
    token = hashlib.sha256(assessment_id.encode('utf-8')).hexdigest()[:16]
    return Path(db_path).resolve().parent / 'latency-scratch' / token


def _fingerprint(batch_id, batch, limit_ms, memory_rows, scratch_bytes, frozen):
    body = json.dumps({'batch': batch_id, 'version': batch['version'],
                       'spec': batch['spec_hash'], 'limit_ms': limit_ms,
                       'memory_rows': memory_rows, 'scratch_bytes': scratch_bytes,
                       'files': [[f['path'], f['sha256'], f['size']] for f in frozen]},
                      sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(body).hexdigest()


def assess(db_path, batch_id, limit_ms, assessment_id, memory_rows, scratch_bytes,
           scratch_dir=None, _hook=None):
    """Exact nearest-rank p95 with bounded memory, bounded scratch and resume.

    Returns a verdict dict: 'issued' (with SLA and rank-boundary evidence),
    'invalid' (data/integrity failure, no SLA) or 'indeterminate' (budgets
    cannot finish, with the exact resource shortfall).  Re-entering with the
    same assessment id and identical inputs resumes from the last stable
    checkpoint or returns the issued result; different inputs conflict.
    """
    if type(limit_ms) is not int or limit_ms < 0:
        raise ValueError('bad_latency_query')
    if type(memory_rows) is not int or memory_rows < 1:
        raise ValueError('bad_latency_query')
    if type(scratch_bytes) is not int or scratch_bytes < 0:
        raise ValueError('bad_latency_query')
    if not isinstance(assessment_id, str) or not assessment_id:
        raise ValueError('bad_latency_query')
    db = connect(db_path)
    try:
        batch = db.execute('SELECT * FROM latency_batch WHERE id=?', (batch_id,)).fetchone()
        if not batch:
            raise ValueError('unknown_latency_batch')
        frozen = _frozen_files(db, batch['version'], json.loads(batch['spec'])['paths'])
        fingerprint = _fingerprint(batch_id, batch, limit_ms, memory_rows, scratch_bytes, frozen)
        row = db.execute('SELECT * FROM latency_assessment WHERE id=?', (assessment_id,)).fetchone()
        if row:
            if row['fingerprint'] != fingerprint:
                raise ValueError('assessment_identity_conflict')
            if row['status'] in ('issued', 'invalid'):
                _discard_scratch(scratch_dir, db_path, assessment_id)
                return json.loads(row['result'])
            checkpoint = json.loads(row['checkpoint'])
        else:
            checkpoint = {'phase': 'start', 'seq': 1}
            with db:
                db.execute('''INSERT INTO latency_assessment
                    (id,batch_id,version,limit_ms,memory_rows,scratch_bytes,fingerprint,status,checkpoint)
                    VALUES (?,?,?,?,?,?,?,'running',?)''',
                           (assessment_id, batch_id, batch['version'], limit_ms, memory_rows,
                            scratch_bytes, fingerprint, json.dumps(checkpoint)))
        scratch = _Scratch(Path(scratch_dir) if scratch_dir
                           else _default_scratch_root(db_path, assessment_id), scratch_bytes)
        base = {'assessment': assessment_id, 'batch': batch_id,
                'version': batch['version'], 'limit_ms': limit_ms}
        try:
            if checkpoint['phase'] != 'start':
                checkpoint = _resume(db, assessment_id, checkpoint, frozen, scratch)
            checkpoint, result = _compute(db, assessment_id, frozen, scratch, memory_rows,
                                          limit_ms, checkpoint, _hook, base,
                                          batch['spec_hash'], fingerprint)
        except _Invalid as problem:
            result = {**base, 'verdict': 'invalid',
                      'reason': problem.reason, 'detail': problem.detail}
            _finish(db, assessment_id, 'invalid', result)
            _discard_scratch(scratch_dir, db_path, assessment_id)
            return result
        except _Budget as problem:
            result = {**base, 'verdict': 'indeterminate',
                      'reason': problem.reason, 'detail': problem.detail}
            _finish(db, assessment_id, 'indeterminate', result)
            _discard_scratch(scratch_dir, db_path, assessment_id)
            return result
        _finish(db, assessment_id, 'issued', result,
                checkpoint={'phase': 'done', 'total_rows': checkpoint['total_rows'],
                            'rank': checkpoint['rank']})
        _fire(_hook, 'checkpoint:issued')
        _discard_scratch(scratch_dir, db_path, assessment_id)
        return result
    finally:
        db.close()


def _discard_scratch(scratch_dir, db_path, assessment_id):
    root = Path(scratch_dir) if scratch_dir else _default_scratch_root(db_path, assessment_id)
    shutil.rmtree(root, ignore_errors=True)


def status(db_path, assessment_id):
    """Stored verdict and progress of one assessment; old conclusions stay queryable."""
    db = connect(db_path)
    try:
        row = db.execute('SELECT * FROM latency_assessment WHERE id=?', (assessment_id,)).fetchone()
        if not row:
            raise ValueError('unknown_latency_assessment')
        return {'assessment': assessment_id, 'batch': row['batch_id'], 'version': row['version'],
                'status': row['status'], 'limit_ms': row['limit_ms'],
                'memory_rows': row['memory_rows'], 'scratch_bytes': row['scratch_bytes'],
                'checkpoint': json.loads(row['checkpoint']),
                'result': json.loads(row['result']) if row['result'] else None,
                'created': row['created'], 'updated': row['updated']}
    finally:
        db.close()


def recheck(db_path, assessment_id):
    """Offline re-verification of an issued assessment's rank-boundary evidence."""
    db = connect(db_path)
    try:
        row = db.execute('SELECT * FROM latency_assessment WHERE id=?', (assessment_id,)).fetchone()
        if not row:
            raise ValueError('unknown_latency_assessment')
        if row['status'] != 'issued':
            raise ValueError('assessment_not_issued')
        result = json.loads(row['result'])
        evidence = result['evidence']
        total = less = le = 0
        for entry in evidence['files']:
            live = db.execute('''SELECT a.path FROM assembled_file a
                JOIN file_entry f ON f.id=a.file_id AND f.version=a.version
                WHERE a.version=? AND f.path=?''', (row['version'], entry['path'])).fetchone()
            if not live:
                return {'assessment': assessment_id, 'verified': False,
                        'reason': 'assembly_missing', 'file': entry['path']}
            digest, size = _hash_stream(live['path'])
            if digest != entry['sha256'] or size != entry['size']:
                return {'assessment': assessment_id, 'verified': False,
                        'reason': 'latency_bytes_changed', 'file': entry['path']}
            for _, value in _record_stream(live['path'], entry['path']):
                total += 1
                less += 1 if value < evidence['value'] else 0
                le += 1 if value <= evidence['value'] else 0
        checks = {'samples': total == evidence['samples'],
                  'rank': (95 * total + 99) // 100 == evidence['rank'],
                  'count_less': less == evidence['count_less'],
                  'count_le': le == evidence['count_le'],
                  'boundary': evidence['count_less'] < evidence['rank'] <= evidence['count_le'],
                  'sla': (evidence['value'] <= result['limit_ms']) == (result['sla'] == 'pass')}
        return {'assessment': assessment_id, 'verified': all(checks.values()), 'checks': checks,
                'observed': {'samples': total, 'count_less': less, 'count_le': le}}
    finally:
        db.close()
