"""Strict small-file decoder for locally delivered CPR1 event frames."""
import hashlib
import json
import struct
import zlib
from pathlib import Path

from .store import connect


MAGIC = b'CPR1'
MAX_PAYLOAD = 1024 * 1024


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
