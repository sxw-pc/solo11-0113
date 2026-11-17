"""Freeze sealed telemetry readings and clock calibration evidence."""
import hashlib
import json
from pathlib import Path

from .store import connect


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_telemetry_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('telemetry_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, run_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not run_id or not {'samples', 'anchors', 'rate_lo_ms', 'rate_hi_ms'} <= spec.keys():
        raise ValueError('bad_telemetry_spec')
    lo, hi = spec['rate_lo_ms'], spec['rate_hi_ms']
    if type(lo) is not int or type(hi) is not int or not 0 < lo <= hi:
        raise ValueError('bad_rate_bounds')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM telemetry_run WHERE id=?', (run_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('telemetry_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('telemetry_not_sealed')
            samples = []
            for item in _sealed_lines(db, version, spec['samples']):
                device, seq, tick, value = (item[key] for key in ('device', 'seq', 'tick', 'value'))
                if not device or any(type(x) is not int for x in (seq, tick, value)) or value < 0:
                    raise ValueError('bad_sample')
                samples.append((run_id, device, seq, tick, value))
            if len({(r[1], r[2]) for r in samples}) != len(samples):
                raise ValueError('duplicate_sample_sequence')
            devices = {r[1] for r in samples}
            anchors = []
            for item in _sealed_lines(db, version, spec['anchors']):
                device, tick, utc_lo, utc_hi = (item[key] for key in
                                                 ('device', 'tick', 'utc_lo_ms', 'utc_hi_ms'))
                if device not in devices or any(type(x) is not int for x in (tick, utc_lo, utc_hi)) or utc_lo > utc_hi:
                    raise ValueError('bad_anchor')
                anchors.append((run_id, device, tick, utc_lo, utc_hi))
            if len({(r[1], r[2]) for r in anchors}) != len(anchors):
                raise ValueError('duplicate_anchor_tick')
            if devices - {r[1] for r in anchors}:
                raise ValueError('uncalibrated_device')
            db.execute('INSERT INTO telemetry_run VALUES (?,?,?,?)', (run_id, version, digest, raw))
            db.executemany('INSERT INTO telemetry_sample VALUES (?,?,?,?,?)', samples)
            db.executemany('INSERT INTO telemetry_anchor VALUES (?,?,?,?,?)', anchors)
            return digest
    finally:
        db.close()


def inspect(db_path, run_id):
    """Show raw volume without assigning readings to UTC windows."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM telemetry_run WHERE id=?', (run_id,)).fetchone():
            raise ValueError('unknown_telemetry_run')
        readings = {r['device']: r['n'] for r in db.execute(
            'SELECT device,count(*) n FROM telemetry_sample WHERE run_id=? GROUP BY device ORDER BY device', (run_id,))}
        total = db.execute('SELECT COALESCE(sum(value),0) n FROM telemetry_sample WHERE run_id=?', (run_id,)).fetchone()['n']
        anchors = db.execute('SELECT count(*) n FROM telemetry_anchor WHERE run_id=?', (run_id,)).fetchone()['n']
        return {'readings': readings, 'raw_total': total, 'anchors': anchors}
    finally:
        db.close()
