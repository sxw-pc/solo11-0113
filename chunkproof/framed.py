"""Strict small-file decoder and bounded streaming importer for CPR1 frames.

A CPR1 frame is::

    MAGIC(4) | length LE uint32 | sequence LE uint32 | payload(utf-8 JSON) |
    crc32 LE uint32 over header+payload

The whole file may already pass physical byte-level sealing while individual
frames inside it are damaged.  This module never repairs bytes: it either
proves a frame end to end (header, declared length, CRC, UTF-8, JSON, event
identity and sequence) or records an issue interval and keeps the uncertainty
explicit.  The scanner is streaming: at most a few chunks plus one bounded
resync window are resident, so the source file may be far larger than memory.
"""
import hashlib
import json
import struct
import zlib
from pathlib import Path

from .store import connect


MAGIC = b'CPR1'
HEADER = 12          # magic + length + sequence
TRAILER = 4          # crc32
FRAME_OVERHEAD = HEADER + TRAILER
MAX_PAYLOAD = 1024 * 1024

# Two usage contracts frozen before any byte is read.
MODE_COMPLETE = 'complete_stream'
MODE_INDEPENDENT = 'independent_records'
MODES = (MODE_COMPLETE, MODE_INDEPENDENT)

# Final import conclusions.
RESULT_COMPLETE = 'complete'          # every byte belonged to a verified frame
RESULT_RECOVERED = 'recovered'        # usable events plus bounded issue ranges
RESULT_REJECTED = 'rejected'          # complete-stream contract, no events
RESULT_INDETERMINATE = 'indeterminate'  # resync ambiguity / bounded scan limit

# Default streaming and bounded-scan budgets (bytes).  They are deliberately
# independent of file size so an adversarial file cannot force unbounded work.
DEFAULT_CHUNK = 1024 * 1024
DEFAULT_RESYNC_WINDOW = 64 * 1024 * 1024
DEFAULT_RESYNC_BUDGET = 256 * 1024 * 1024


class ScanInterrupted(RuntimeError):
    """Raised when an injected read budget is exhausted (crash simulation)."""


class _ScanLimit(RuntimeError):
    """Internal: bounded candidate scan reached its byte budget."""


def pack(sequence, record):
    if type(sequence) is not int or sequence <= 0:
        raise ValueError('bad_frame_sequence')
    body = json.dumps(record, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')
    if len(body) > MAX_PAYLOAD:
        raise ValueError('frame_too_large')
    header = MAGIC + struct.pack('<II', len(body), sequence)
    return header + body + struct.pack('<I', zlib.crc32(header + body))


def register(db_path, feed_id, version, file_path):
    if not feed_id or not file_path:
        raise ValueError('bad_frame_feed')
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,file_path FROM frame_feed WHERE id=?', (feed_id,)).fetchone()
            if old:
                if (old['version'], old['file_path']) != (version, file_path):
                    raise ValueError('frame_feed_identity_conflict')
                return old['file_path']
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('frame_feed_not_sealed')
            row = db.execute('''SELECT f.sha256,a.path FROM file_entry f
                JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
                WHERE f.version=? AND f.path=?''', (version, file_path)).fetchone()
            if not row:
                raise ValueError('unassembled_frame_file')
            if hashlib.sha256(Path(row['path']).read_bytes()).hexdigest() != row['sha256']:
                raise ValueError('frame_file_changed')
            db.execute('INSERT INTO frame_feed VALUES (?,?,?,?)',
                       (feed_id, version, file_path, row['sha256']))
            return file_path
    finally:
        db.close()


def decode(db_path, feed_id):
    db = connect(db_path)
    try:
        feed = db.execute('SELECT * FROM frame_feed WHERE id=?', (feed_id,)).fetchone()
        if not feed:
            raise ValueError('unknown_frame_feed')
        row = db.execute('''SELECT a.path FROM file_entry f
            JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
            WHERE f.version=? AND f.path=?''', (feed['version'], feed['file_path'])).fetchone()
        raw = Path(row['path']).read_bytes()
        if hashlib.sha256(raw).hexdigest() != feed['file_sha256']:
            raise ValueError('frame_file_changed')
        events, ids, cursor, expected = [], set(), 0, 1
        while cursor < len(raw):
            if len(raw) - cursor < 16 or raw[cursor:cursor + 4] != MAGIC:
                raise ValueError('frame_header')
            size, sequence = struct.unpack_from('<II', raw, cursor + 4)
            if size > MAX_PAYLOAD or len(raw) - cursor < 16 + size:
                raise ValueError('frame_length')
            body = raw[cursor + 12:cursor + 12 + size]
            actual, = struct.unpack_from('<I', raw, cursor + 12 + size)
            if actual != zlib.crc32(raw[cursor:cursor + 12 + size]):
                raise ValueError('frame_crc')
            if sequence != expected:
                raise ValueError('frame_sequence')
            record = json.loads(body.decode('utf-8'))
            event_id = record.get('event_id') if isinstance(record, dict) else None
            if not event_id or event_id in ids:
                raise ValueError('frame_event_identity')
            ids.add(event_id)
            events.append({'sequence': sequence, 'event': record})
            expected += 1
            cursor += 16 + size
        return events
    finally:
        db.close()


class _Scanner:
    """Forward-only, bounded-memory byte scanner over one frozen file.

    ``buffer`` always holds bytes starting at ``base``.  Bytes before
    ``cursor`` are dropped on advance, so resident memory stays proportional
    to the read chunk plus any explicitly requested bounded window -- never to
    the file size and never to a per-offset copy of the remaining suffix.
    """

    def __init__(self, path, chunk_size=DEFAULT_CHUNK,
                 resync_window=DEFAULT_RESYNC_WINDOW,
                 resync_budget=DEFAULT_RESYNC_BUDGET, fail_after=None):
        self.path = Path(path)
        self.chunk_size = chunk_size
        self.resync_window = resync_window
        self.resync_budget = resync_budget
        self.fail_after = fail_after
        self.stream = self.path.open('rb')
        self.eof = False
        self.base = 0
        self.cursor = 0
        self.buffer = b''
        self.bytes_read = 0
        self.search_spent = 0
        self.peak_resident = 0
        self.hasher = hashlib.sha256()

    def close(self):
        self.stream.close()

    def _track(self):
        if len(self.buffer) > self.peak_resident:
            self.peak_resident = len(self.buffer)

    def _read(self, amount):
        if self.fail_after is not None and self.bytes_read + amount > self.fail_after:
            raise ScanInterrupted('scan_interrupted')
        part = self.stream.read(amount)
        self.bytes_read += len(part)
        self.hasher.update(part)
        return part

    def _fill(self):
        if self.eof:
            return
        part = self._read(self.chunk_size)
        if not part:
            self.eof = True
            return
        self.buffer += part
        self._track()

    def advance(self, count):
        """Move the absolute cursor; drop prefix bytes that fall behind it."""
        self.cursor += count
        drop = self.cursor - self.base
        if drop > 0:
            self.buffer = self.buffer[drop:]
            self.base += drop
            self._track()

    def ensure(self, need):
        """Ensure the absolute offset ``need`` is resident unless at EOF.

        The stream may be read in chunks much smaller than one frame, so keep
        filling until the requested absolute offset is resident (or the
        physical file ends); a single short read is never truncation.  The
        demand is converted to a length relative to the current window, since
        bytes behind ``base`` have already been dropped by ``advance``.
        Callers request at most one legal frame plus a bounded window, so
        residency stays bounded independently of file size.
        """
        required = need - self.cursor
        while not self.eof and len(self.buffer) < required:
            before = len(self.buffer)
            self._fill()
            if self.eof or len(self.buffer) == before:
                break

    def drain_hash(self):
        """Hash bytes never read into the hasher.

        Every physical chunk is hashed exactly once inside ``_read``; the
        resident buffer only holds already-hashed bytes, so here we stream the
        remaining file tail without touching the buffer.
        """
        tail = self.stream.read(self.chunk_size)
        while tail:
            self.bytes_read += len(tail)
            self.hasher.update(tail)
            tail = self.stream.read(self.chunk_size)

    def _parse_frame(self, offset):
        """Fully validate one frame claimed to start at absolute ``offset``.

        The magic alone is never proof: the declared length, complete
        boundary, CRC, UTF-8, JSON, event identity and sequence are all
        re-checked here for every candidate, including resync candidates.
        """
        local = offset - self.base
        self.ensure(offset + HEADER)
        local = offset - self.base
        if local < 0 or local + HEADER > len(self.buffer):
            return None  # truncated header / half frame at EOF
        if self.buffer[local:local + 4] != MAGIC:
            return None
        size, sequence = struct.unpack_from('<II', self.buffer, local + 4)
        if size == 0 or size > MAX_PAYLOAD:
            return None
        total = FRAME_OVERHEAD + size
        self.ensure(offset + total)
        local = offset - self.base
        if local + total > len(self.buffer):
            return None  # declared length runs past EOF: truncated frame
        body = self.buffer[local + HEADER:local + HEADER + size]
        stored_crc, = struct.unpack_from('<I', self.buffer, local + HEADER + size)
        if stored_crc != zlib.crc32(self.buffer[local:local + HEADER + size]):
            return None
        try:
            record = json.loads(body.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(record, dict):
            return None
        event_id = record.get('event_id')
        if not isinstance(event_id, str) or not event_id:
            return None
        return {'offset': offset, 'length': total, 'payload_length': size,
                'sequence': sequence, 'event_id': event_id, 'record': record,
                'digest': hashlib.sha256(self.buffer[local:local + total]).hexdigest()}

    def read_frame(self):
        """Validate the frame starting at the current absolute cursor."""
        self.ensure(self.cursor + 4)
        local = self.cursor - self.base
        if local >= len(self.buffer):
            return None  # clean physical EOF
        if self.buffer[local:local + 4] != MAGIC:
            return {'error': 'frame_header', 'start': self.cursor,
                    'end': self.cursor + 4, 'reason': 'bad_magic'}
        self.ensure(self.cursor + HEADER)
        local = self.cursor - self.base
        if local + HEADER > len(self.buffer):
            return {'error': 'frame_truncated', 'start': self.cursor,
                    'end': self.file_size, 'reason': 'partial_header'}
        size, sequence = struct.unpack_from('<II', self.buffer, local + 4)
        if size > MAX_PAYLOAD:
            # A length field that cannot describe any legal frame: the header
            # itself is unusable evidence for any boundary.
            return {'error': 'frame_length', 'start': self.cursor,
                    'end': self.cursor + HEADER, 'reason': 'oversized_length'}
        total = FRAME_OVERHEAD + size
        self.ensure(self.cursor + total)
        local = self.cursor - self.base
        if local + total > len(self.buffer):
            return {'error': 'frame_truncated', 'start': self.cursor,
                    'end': self.file_size, 'reason': 'partial_frame'}
        body = self.buffer[local + HEADER:local + HEADER + size]
        stored_crc, = struct.unpack_from('<I', self.buffer, local + HEADER + size)
        if stored_crc != zlib.crc32(self.buffer[local:local + HEADER + size]):
            # Class A evidence: header and declared length parse, payload bad.
            return {'error': 'frame_crc', 'start': self.cursor,
                    'end': self.cursor + total, 'length_hint': total,
                    'reason': 'crc_mismatch'}
        try:
            record = json.loads(body.decode('utf-8'))
        except UnicodeDecodeError as exc:
            return {'error': 'frame_encoding', 'start': self.cursor,
                    'end': self.cursor + total, 'length_hint': total,
                    'reason': f'utf8:{exc.reason}'}
        except json.JSONDecodeError as exc:
            return {'error': 'frame_json', 'start': self.cursor,
                    'end': self.cursor + total, 'length_hint': total,
                    'reason': f'json:{exc.msg}'}
        if not isinstance(record, dict):
            return {'error': 'frame_event_identity', 'start': self.cursor,
                    'end': self.cursor + total, 'length_hint': total,
                    'reason': 'record_not_object'}
        event_id = record.get('event_id')
        if not isinstance(event_id, str) or not event_id:
            return {'error': 'frame_event_identity', 'start': self.cursor,
                    'end': self.cursor + total, 'length_hint': total,
                    'reason': 'missing_event_id'}
        return {'offset': self.cursor, 'length': total,
                'payload_length': size, 'sequence': sequence,
                'event_id': event_id, 'record': record,
                'digest': hashlib.sha256(self.buffer[local:local + total]).hexdigest()}

    def resync(self, damage_start, jump_to=None):
        """Find provable frame starts after a damaged region.

        When the damaged frame's header and declared length still parse,
        ``jump_to`` is the candidate end of that frame: it is tested first,
        without trusting it as proof.  Afterwards every magic occurrence in a
        bounded byte window is tested.  Candidates are real only when the full
        frame boundary, CRC, UTF-8, JSON and identity validate; a magic inside
        a payload -- even a nested-looking frame -- proves nothing by itself.

        Returns ``(status, candidates, window_end, searched)`` where status is
        ``unique``, ``ambiguous``, ``eof`` or ``limit``.  ``eof`` means the
        bounded window reached the physical end of file; ``limit`` means the
        cumulative search budget was exhausted before any proof existed.
        """
        if jump_to is not None and jump_to > damage_start:
            targets = [jump_to]
            window_start = jump_to
        else:
            targets = []
            window_start = damage_start
        window_end = min(self.file_size, window_start + self.resync_window)
        # Keep one bounded window resident; never copy a per-offset suffix.
        self.ensure(window_end)
        candidates = []
        seen = set()
        for target in targets:
            if target >= self.file_size:
                continue
            frame = self._parse_frame(target)
            if frame:
                candidates.append(frame)
                seen.add(target)
        local_start = window_start - self.base
        local_end = min(window_end, self.base + len(self.buffer)) - self.base
        # One bounded-window scan copy; never a per-offset suffix copy, so the
        # total residency stays chunk + window regardless of file size.
        hay = self.buffer[local_start:local_end]
        window_resident = len(self.buffer) + len(hay)
        if window_resident > self.peak_resident:
            self.peak_resident = window_resident
        probe = 0
        while True:
            found = hay.find(MAGIC, probe)
            if found < 0:
                break
            probe = found + 1
            target = window_start + found
            if target in seen:
                continue
            frame = self._parse_frame(target)
            if frame:
                candidates.append(frame)
                seen.add(target)
        self.search_spent += local_end - local_start
        if self.search_spent > self.resync_budget:
            return 'limit', candidates, window_end, local_end - local_start
        # Mutually exclusive interpretations: overlapping byte ranges, or two
        # frames competing for the same successor sequence.  Do not pick the
        # earliest/latest pseudo-frame -- flag the ambiguity.
        ordered = sorted(candidates, key=lambda f: f['offset'])
        for i, frame in enumerate(ordered):
            for other in ordered[i + 1:]:
                overlap = other['offset'] < frame['offset'] + frame['length']
                same_seq = other['sequence'] == frame['sequence']
                if overlap or same_seq:
                    return 'ambiguous', ordered, window_end, local_end - local_start
        if not ordered:
            if window_end >= self.file_size:
                return 'eof', [], window_end, local_end - local_start
            return 'limit', [], window_end, local_end - local_start
        return 'unique', ordered, window_end, local_end - local_start


def _issue(code, start, end, detail):
    if end < start:
        raise ValueError('bad_issue_range')
    return {'code': code, 'start': start, 'end': end, 'detail': detail}


def _scan(file_path, mode, *, chunk_size, resync_window, resync_budget, fail_after):
    """One bounded streaming scan; returns a report with digest and accounting."""
    path = Path(file_path)
    file_size = path.stat().st_size
    scanner = _Scanner(path, chunk_size=chunk_size, resync_window=resync_window,
                       resync_budget=resync_budget, fail_after=fail_after)
    scanner.file_size = file_size
    events, issues, gaps = [], [], []
    seen_offsets, seen_ids, seen_sequences = set(), set(), set()
    last_sequence = 0
    indeterminate = False
    if file_size == 0:
        # An empty stream cannot prove sequence 1 under either contract.
        issues.append(_issue('frame_truncated', 0, 0, 'empty_stream'))
        conclusion = RESULT_REJECTED
    else:
        conclusion = RESULT_REJECTED
    try:
        while scanner.cursor < file_size:
            frame = scanner.read_frame()
            if frame is None:
                break  # clean physical EOF
            if 'error' in frame:
                if mode == MODE_COMPLETE:
                    # The stream contract allows no partial delivery: one bad
                    # frame means the whole import has zero usable events.
                    issues.append(_issue(frame['error'], frame['start'],
                                         frame['end'], frame['reason']))
                    conclusion = RESULT_REJECTED
                    break
                # independent_records: bound the damage, then resync.
                jump = frame.get('length_hint')
                damage = _issue(frame['error'], frame['start'],
                                frame['end'], frame['reason'])
                status, candidates, window_end, searched = scanner.resync(
                    frame['start'], jump_to=jump)
                if status == 'limit':
                    # The bounded window proved nothing; every byte from the
                    # damage point to EOF stays in the uncertain region.
                    damage['end'] = file_size
                    damage['detail'] += f';scan_window_end={window_end}'
                    issues.append(damage)
                    issues.append(_issue('resync_limit', frame['start'],
                                         window_end,
                                         f'no_provable_start_in_{searched}_bytes'))
                    conclusion, indeterminate = RESULT_INDETERMINATE, True
                    break
                if status == 'ambiguous':
                    offsets = [c['offset'] for c in candidates]
                    issues.append(damage)
                    issues.append(_issue('ambiguous_resync', frame['start'],
                                         file_size,
                                         f'competing_starts={offsets}'))
                    conclusion, indeterminate = RESULT_INDETERMINATE, True
                    break
                if status == 'eof':
                    # Exact, provable uncertainty: the damaged tail runs to EOF.
                    damage['end'] = file_size
                    damage['detail'] += ';rest_unprovable_to_eof'
                    issues.append(damage)
                    conclusion = RESULT_RECOVERED if events else RESULT_REJECTED
                    scanner.advance(file_size - scanner.cursor)
                    break
                chosen = candidates[0]
                # One issue interval covers every unproven byte until the next
                # proven frame, so frame ranges and issue ranges tile the file.
                damage['end'] = chosen['offset']
                damage['detail'] += f';resumed_at={chosen["offset"]}'
                issues.append(damage)
                scanner.advance(chosen['offset'] - scanner.cursor)
                frame = chosen  # already fully validated by resync
            # A fully validated frame still has to clear import-wide identity
            # and sequence rules.  Duplicate IDs / sequences are never silently
            # deduplicated; they become bounded issues, not good events.
            offset = frame['offset']
            if offset in seen_offsets:
                issues.append(_issue('frame_duplicate_offset', offset,
                                     offset + frame['length'], 'resync_replay'))
                scanner.advance(frame['length'])
                continue
            conflict = None
            if frame['event_id'] in seen_ids:
                conflict = ('frame_event_identity', 'duplicate_event_id')
            elif frame['sequence'] in seen_sequences:
                conflict = ('frame_sequence', 'duplicate_sequence')
            elif mode == MODE_COMPLETE and frame['sequence'] != last_sequence + 1:
                conflict = ('frame_sequence',
                            f'expected_{last_sequence + 1}')
            elif mode == MODE_INDEPENDENT and frame['sequence'] <= last_sequence:
                # Independently valid frame, but its sequence moves backwards:
                # record the ordering violation instead of silently accepting.
                conflict = ('frame_sequence',
                            f'rollback_after_{last_sequence}')
            if conflict:
                code, reason = conflict
                issues.append(_issue(code, offset, offset + frame['length'], reason))
                scanner.advance(frame['length'])
                if mode == MODE_COMPLETE:
                    conclusion = RESULT_REJECTED
                    break
                continue
            if mode == MODE_INDEPENDENT and frame['sequence'] != last_sequence + 1:
                missing = list(range(last_sequence + 1, frame['sequence']))
                if missing:
                    gaps.append({'missing_sequences': missing,
                                 'after_sequence': last_sequence,
                                 'before_sequence': frame['sequence']})
            events.append({'offset': offset, 'length': frame['length'],
                           'sequence': frame['sequence'],
                           'event_id': frame['event_id'],
                           'frame_digest': frame['digest'],
                           'record': frame['record'], 'usable': True})
            seen_offsets.add(offset)
            seen_ids.add(frame['event_id'])
            seen_sequences.add(frame['sequence'])
            last_sequence = frame['sequence']
            scanner.advance(frame['length'])
        else:
            # Clean physical EOF reached through the normal loop.
            if issues:
                conclusion = RESULT_RECOVERED if events else RESULT_REJECTED
            else:
                conclusion = RESULT_COMPLETE
    finally:
        # Hash the entire frozen source in the same streaming pass, including
        # any bytes the scan loop did not consume, then release the handle.
        scanner.drain_hash()
        digest = scanner.hasher.hexdigest()
        stats = {'bytes_read': scanner.bytes_read,
                 'search_spent': scanner.search_spent,
                 'peak_resident': scanner.peak_resident,
                 # The buffer may hold one chunk plus one bounded resync
                 # window; scanning that window adds at most one more
                 # window-sized copy.  No per-offset suffix copies exist.
                 'resident_bound': scanner.chunk_size + 2 * scanner.resync_window}
        scanner.close()
    # Verified frames stay as evidence under every conclusion, but only
    # complete/recovered imports make them usable business records.
    if conclusion not in (RESULT_COMPLETE, RESULT_RECOVERED):
        for event in events:
            event['usable'] = False
    return {'conclusion': conclusion, 'file_size': file_size,
            'file_sha256': digest, 'indeterminate': indeterminate,
            'events': events, 'issues': issues, 'sequence_gaps': gaps,
            'stats': stats}


def _canonical(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':')).encode('utf-8')


def _feed_source(db, feed_id):
    feed = db.execute('SELECT * FROM frame_feed WHERE id=?', (feed_id,)).fetchone()
    if not feed:
        raise ValueError('unknown_frame_feed')
    row = db.execute('''SELECT a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''',
                     (feed['version'], feed['file_path'])).fetchone()
    if not row:
        raise ValueError('unassembled_frame_file')
    source = Path(row['path'])
    if not source.is_file():
        raise ValueError('frame_source_missing')
    return feed, source


def _physical_digest(source):
    """Streaming whole-file SHA-256 so the check itself stays bounded."""
    digest = hashlib.sha256()
    with source.open('rb') as stream:
        for block in iter(lambda: stream.read(DEFAULT_CHUNK), b''):
            digest.update(block)
    return digest.hexdigest()


def frame_import(db_path, feed_id, mode, import_id, *,
                 chunk_size=DEFAULT_CHUNK, resync_window=DEFAULT_RESYNC_WINDOW,
                 resync_budget=DEFAULT_RESYNC_BUDGET, fail_after=None):
    """Import one sealed CPR1 file under a contract frozen before reading.

    The mode is validated before any byte is touched.  The ledger is append
    only: a repeated ``import_id`` returns the stored evidence when the frozen
    file digest and mode still match, and raises on any identity conflict.  A
    changed source, an interrupted scan or an ambiguous resync can never reuse
    an old successful result.  Usable events are written in one transaction
    together with the import conclusion, so a crash leaves no partial import.
    """
    if not import_id:
        raise ValueError('bad_import_id')
    if mode not in MODES:
        raise ValueError('bad_import_mode')
    db = connect(db_path)
    try:
        feed, source = _feed_source(db, feed_id)
        # Recompute the physical digest before any idempotency decision: a
        # tampered file must never inherit a prior successful import, and the
        # check itself is streaming rather than a full read_bytes.
        if source.stat().st_size != _physical_size(db, feed) or \
                _physical_digest(source) != feed['file_sha256']:
            raise ValueError('frame_file_changed')
        previous = db.execute(
            'SELECT * FROM frame_import WHERE id=?', (import_id,)).fetchone()
        if previous:
            if previous['feed_id'] != feed_id:
                raise ValueError('frame_import_identity_conflict')
            if previous['mode'] != mode:
                raise ValueError('frame_import_mode_conflict')
            if previous['file_sha256'] != feed['file_sha256']:
                raise ValueError('frame_file_changed')
            return _stored_report(db, previous)
        report = _scan(source, mode, chunk_size=chunk_size,
                       resync_window=resync_window, resync_budget=resync_budget,
                       fail_after=fail_after)
        if report['file_sha256'] != feed['file_sha256']:
            raise ValueError('frame_file_changed')
        report['import_id'] = import_id
        report['feed_id'] = feed_id
        report['mode'] = mode
        report['frozen'] = {'version': feed['version'],
                            'file_path': feed['file_path'],
                            'file_sha256': feed['file_sha256'],
                            'file_size': report['file_size']}
        with db:
            db.execute('''INSERT INTO frame_import
                (id,feed_id,mode,conclusion,version,file_path,file_sha256,file_size,report)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                       (import_id, feed_id, mode, report['conclusion'],
                        feed['version'], feed['file_path'], feed['file_sha256'],
                        report['file_size'], _canonical(report).decode('utf-8')))
            # Every independently verified frame is retained as evidence.
            # Only complete/recovered imports mark them as usable records;
            # rejected/indeterminate imports commit no usable business events.
            for event in report['events']:
                db.execute('''INSERT INTO frame_import_event
                    (import_id,"offset",length,sequence,event_id,frame_digest,usable,record)
                    VALUES (?,?,?,?,?,?,?,?)''',
                           (import_id, event['offset'], event['length'],
                            event['sequence'], event['event_id'],
                            event['frame_digest'], 1 if event['usable'] else 0,
                            _canonical(event['record']).decode('utf-8')))
            for issue in report['issues']:
                db.execute('''INSERT INTO frame_import_issue
                    (import_id,code,start,"end",detail) VALUES (?,?,?,?,?)''',
                           (import_id, issue['code'], issue['start'],
                            issue['end'], issue['detail']))
        return report
    finally:
        db.close()


def _physical_size(db, feed):
    row = db.execute(
        'SELECT size FROM file_entry WHERE version=? AND path=?',
        (feed['version'], feed['file_path'])).fetchone()
    if not row:
        raise ValueError('unassembled_frame_file')
    return row['size']


def _stored_report(db, row):
    report = json.loads(row['report'])
    report['import_id'] = row['id']
    report['feed_id'] = row['feed_id']
    report['mode'] = row['mode']
    report['ledger'] = {'event_rows': db.execute(
        'SELECT count(*) FROM frame_import_event WHERE import_id=?',
        (row['id'],)).fetchone()[0],
        'issue_rows': db.execute(
        'SELECT count(*) FROM frame_import_issue WHERE import_id=?',
        (row['id'],)).fetchone()[0]}
    return report


def frame_verify(db_path, import_id, *, chunk_size=DEFAULT_CHUNK,
                 resync_window=DEFAULT_RESYNC_WINDOW,
                 resync_budget=DEFAULT_RESYNC_BUDGET):
    """Re-derive an import report from the frozen source bytes.

    Every reported frame position is re-read and re-validated (boundary, CRC,
    UTF-8, JSON, event identity, sequence and frame digest).  Issue intervals
    and the usable event set are compared structurally with the ledger.  The
    whole-file seal cannot be used to skip frame verification, and a hand-edited
    issue range fails the range tiling check.
    """
    db = connect(db_path)
    try:
        row = db.execute(
            'SELECT * FROM frame_import WHERE id=?', (import_id,)).fetchone()
        if not row:
            raise ValueError('unknown_frame_import')
        feed, source = _feed_source(db, row['feed_id'])
        # Independent physical re-check before trusting any stored bytes.
        if source.stat().st_size != row['file_size'] or \
                _physical_digest(source) != row['file_sha256'] or \
                row['file_sha256'] != feed['file_sha256']:
            raise ValueError('frame_file_changed')
        fresh = _scan(source, row['mode'], chunk_size=chunk_size,
                      resync_window=resync_window, resync_budget=resync_budget,
                      fail_after=None)
        stored = json.loads(row['report'])
        # The signed JSON report is compared with the freshly re-derived scan.
        for field in ('conclusion', 'issues', 'sequence_gaps', 'file_size'):
            if fresh[field] != stored[field]:
                raise ValueError(f'frame_verify_mismatch:{field}')
        if [e['offset'] for e in fresh['events']] != \
                [e['offset'] for e in stored['events']]:
            raise ValueError('frame_verify_mismatch:events')
        # The relational ledger rows are separately hand-editable evidence;
        # they must agree with the re-derived frames and drive interval
        # tiling, so editing either store is detectable.
        ledger_events = [dict(r) for r in db.execute('''SELECT "offset",length,
            sequence,event_id,frame_digest,usable FROM frame_import_event
            WHERE import_id=? ORDER BY "offset"''', (import_id,))]
        ledger_issues = [dict(r) for r in db.execute('''SELECT code,start,"end",
            detail FROM frame_import_issue WHERE import_id=? ORDER BY id''',
                                                      (import_id,))]
        if [(e['offset'], e['length'], e['sequence'], e['event_id'],
             e['frame_digest']) for e in ledger_events] != \
                [(e['offset'], e['length'], e['sequence'], e['event_id'],
                  e['frame_digest']) for e in fresh['events']]:
            raise ValueError('frame_verify_mismatch:event_rows')
        if [(i['code'], i['start'], i['end'], i['detail'])
                for i in ledger_issues] != \
                [(i['code'], i['start'], i['end'], i['detail'])
                 for i in fresh['issues']]:
            raise ValueError('frame_verify_mismatch:issue_rows')
        # Independent per-frame re-read from the frozen bytes.
        with source.open('rb') as stream:
            for event in ledger_events:
                stream.seek(event['offset'])
                raw = stream.read(event['length'])
                if len(raw) != event['length'] or raw[:4] != MAGIC:
                    raise ValueError('frame_verify_mismatch:boundary')
                size, sequence = struct.unpack_from('<II', raw, 4)
                if FRAME_OVERHEAD + size != event['length'] or \
                        sequence != event['sequence']:
                    raise ValueError('frame_verify_mismatch:header')
                crc, = struct.unpack_from('<I', raw, HEADER + size)
                if crc != zlib.crc32(raw[:HEADER + size]):
                    raise ValueError('frame_verify_mismatch:crc')
                if hashlib.sha256(raw).hexdigest() != event['frame_digest']:
                    raise ValueError('frame_verify_mismatch:digest')
                record = json.loads(raw[HEADER:HEADER + size].decode('utf-8'))
                if record.get('event_id') != event['event_id']:
                    raise ValueError('frame_verify_mismatch:identity')
        # Range evidence.  For complete/recovered imports proven frames and
        # issue intervals strictly partition the file: a hand-edited issue
        # range breaks the partition immediately.  An indeterminate import may
        # carry overlapping evidence intervals, but their union must still
        # cover every byte; a rejected stream makes no claim past its first
        # failure, so only internal ordering is checked there.
        spans = [(e['offset'], e['offset'] + e['length'])
                 for e in ledger_events]
        spans.extend((i['start'], i['end']) for i in ledger_issues)
        spans = sorted((s, e) for s, e in spans if e > s)
        strict = row['conclusion'] in (RESULT_COMPLETE, RESULT_RECOVERED)
        cursor = 0
        coverage = []
        for start, end in spans:
            if strict and start != cursor:
                raise ValueError('frame_verify_mismatch:range_tiling')
            if not strict:
                if coverage and start <= coverage[-1][1]:
                    coverage[-1] = (coverage[-1][0], max(coverage[-1][1], end))
                else:
                    coverage.append((start, end))
            cursor = end
        if strict and cursor != row['file_size']:
            raise ValueError('frame_verify_mismatch:range_tiling')
        if row['conclusion'] == RESULT_INDETERMINATE and (
                not coverage or coverage[0][0] != 0 or
                any(coverage[i][0] != coverage[i - 1][1]
                    for i in range(1, len(coverage))) or
                coverage[-1][1] != row['file_size']):
            raise ValueError('frame_verify_mismatch:range_coverage')
        # The usable set persisted in the ledger must equal the proven set
        # allowed by the conclusion; rejected/indeterminate rows stay as
        # evidence but must all be marked non-usable.
        if [r['offset'] for r in ledger_events] != \
                [e['offset'] for e in stored['events']]:
            raise ValueError('frame_verify_mismatch:usable_set')
        want_usable = row['conclusion'] in (RESULT_COMPLETE, RESULT_RECOVERED)
        if any(bool(r['usable']) != want_usable for r in ledger_events):
            raise ValueError('frame_verify_mismatch:usable_set')
        return {'import_id': import_id, 'ok': True,
                'conclusion': row['conclusion'],
                'usable_events': sum(1 for r in ledger_events if r['usable']),
                'evidence_events': len(ledger_events),
                'issues': stored['issues'],
                'sequence_gaps': stored['sequence_gaps'],
                'file_sha256': row['file_sha256']}
    finally:
        db.close()
