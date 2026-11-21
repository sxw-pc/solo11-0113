"""Freeze sealed telemetry readings and clock calibration evidence."""
import hashlib
import json
from fractions import Fraction
from pathlib import Path

from . import clockmath
from .engine import canonical
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


def _load_run(db, run_id):
    run = db.execute('SELECT * FROM telemetry_run WHERE id=?', (run_id,)).fetchone()
    if not run:
        raise ValueError('unknown_telemetry_run')
    spec = json.loads(run['spec'].decode('utf-8'))
    devices = [r['device'] for r in db.execute(
        'SELECT DISTINCT device FROM telemetry_sample WHERE run_id=? ORDER BY device', (run_id,))]
    readings, anchors = {}, {}
    for device in devices:
        readings[device] = [dict(r) for r in db.execute(
            'SELECT seq,tick,value FROM telemetry_sample WHERE run_id=? AND device=? ORDER BY seq',
            (run_id, device))]
        anchors[device] = [dict(r) for r in db.execute(
            'SELECT tick,utc_lo_ms,utc_hi_ms FROM telemetry_anchor WHERE run_id=? AND device=? ORDER BY tick',
            (run_id, device))]
    if any(not anchors[d] for d in devices):
        raise ValueError('uncalibrated_device')
    files = [dict(r) for r in db.execute(
        'SELECT path,size,sha256 FROM file_entry WHERE version=? ORDER BY path', (run['version'],))]
    return run, spec, devices, readings, anchors, files


def _evidence(run, spec, files, readings, anchors):
    return {
        'version': run['version'],
        'spec_hash': run['spec_hash'],
        'spec_summary': {'rate_lo_ms': spec['rate_lo_ms'], 'rate_hi_ms': spec['rate_hi_ms'],
                         'samples': spec['samples'], 'anchors': spec['anchors']},
        'manifest_files': files,
        'readings_summary': {d: [{'seq': r['seq'], 'tick': r['tick'], 'value': r['value']}
                                 for r in readings[d]] for d in sorted(readings)},
        'anchors_summary': {d: anchors[d] for d in sorted(anchors)},
    }


def _frac_text(value):
    value = Fraction(value)
    return str(value.numerator) if value.denominator == 1 else f'{value.numerator}/{value.denominator}'


def _parse_frac(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f'bad_{field}')
    try:
        return Fraction(value)
    except (ValueError, ZeroDivisionError):
        raise ValueError(f'bad_{field}')


def _replay_clock(clock, anchors, rate_lo, rate_hi):
    rate = _parse_frac(clock['rate_ms_per_tick'], 'clock_rate')
    offset = _parse_frac(clock['offset_ms'], 'clock_offset')
    if not Fraction(rate_lo) <= rate <= Fraction(rate_hi):
        return None
    for a in anchors:
        utc = offset + rate * a['tick']
        if not Fraction(a['utc_lo_ms']) <= utc <= Fraction(a['utc_hi_ms']):
            return None
    return rate, offset


def assess(db_path, run_id, window_start_ms, window_end_ms, threshold, policy,
           assessment_id, nominal_clocks=None, budget=1_000_000):
    """Adjudicate whether windowed business volume reaches ``threshold``.

    ``policy`` is either ``'nominal'`` (one explicitly submitted shared clock
    per device, anchor-validated before use) or ``'guaranteed'`` (exact global
    lower/upper bounds over every physically feasible shared clock).
    """
    if not assessment_id:
        raise ValueError('bad_assessment_id')
    if type(window_start_ms) is not int or type(window_end_ms) is not int \
            or window_start_ms >= window_end_ms:
        raise ValueError('bad_window')
    if type(threshold) is not int or threshold < 0:
        raise ValueError('bad_threshold')
    if policy not in ('nominal', 'guaranteed'):
        raise ValueError('bad_policy')
    if type(budget) is not int or budget <= 0:
        raise ValueError('bad_budget')
    db = connect(db_path)
    try:
        run, spec, devices, readings, anchors, files = _load_run(db, run_id)
        rate_lo, rate_hi = spec['rate_lo_ms'], spec['rate_hi_ms']
        if policy == 'nominal':
            if not isinstance(nominal_clocks, dict) or set(nominal_clocks) != set(devices):
                raise ValueError('bad_nominal_clocks')
        device_results = {}
        if policy == 'nominal':
            for device in devices:
                submitted = nominal_clocks[device]
                if not isinstance(submitted, dict) or {'offset_ms', 'rate_ms_per_tick'} - submitted.keys():
                    raise ValueError('bad_nominal_clocks')
                rate = _parse_frac(submitted['rate_ms_per_tick'], 'clock_rate')
                offset = _parse_frac(submitted['offset_ms'], 'clock_offset')
                outcome = clockmath.nominal_assessment(
                    rate_lo, rate_hi,
                    [(a['tick'], a['utc_lo_ms'], a['utc_hi_ms']) for a in anchors[device]],
                    [(r['tick'], r['value']) for r in readings[device]],
                    window_start_ms, window_end_ms, offset, rate)
                device_results[device] = {
                    'submitted_clock': outcome['clock'],
                    'clock_feasible': outcome['feasible_clock'],
                    'anchor_violations': outcome['anchor_violations'],
                    'readings': outcome['readings'],
                    'window_total': outcome['window_total'],
                }
            invalid = [d for d in devices if not device_results[d]['clock_feasible']]
            if invalid:
                result, reason = 'invalid', 'nominal_clock_anchor_conflict'
                total = None
            else:
                total = sum(device_results[d]['window_total'] for d in devices)
                if total >= threshold:
                    result, reason = 'pass', 'nominal_total_at_or_above_threshold'
                else:
                    result, reason = 'fail', 'nominal_total_below_threshold'
        else:
            exact = True
            total_lo = total_hi = 0
            witnessed_lo = witnessed_hi = 0
            for device in devices:
                outcome = clockmath.guaranteed_bounds(
                    rate_lo, rate_hi,
                    [(a['tick'], a['utc_lo_ms'], a['utc_hi_ms']) for a in anchors[device]],
                    [(r['tick'], r['value']) for r in readings[device]],
                    window_start_ms, window_end_ms, budget)
                if not outcome['feasible']:
                    device_results[device] = {
                        'feasible': False, 'conflict_anchors': outcome['conflict']}
                    continue
                if not outcome['exact']:
                    exact = False
                entry = {'feasible': True, 'exact': outcome['exact'],
                         'points_evaluated': outcome['points_evaluated'],
                         'remaining_budget': outcome['remaining_budget']}
                if outcome['lower'] is None:
                    entry.update({'witnessed_lower': None, 'witnessed_upper': None,
                                  'lower_witness': None, 'upper_witness': None})
                else:
                    entry.update({
                        'witnessed_lower': outcome['lower'],
                        'witnessed_upper': outcome['upper'],
                        'lower_witness': outcome['lower_witness'],
                        'upper_witness': outcome['upper_witness']})
                    witnessed_lo += outcome['lower']
                    witnessed_hi += outcome['upper']
                    if outcome['exact']:
                        total_lo += outcome['lower']
                        total_hi += outcome['upper']
                device_results[device] = entry
            infeasible = [d for d in devices if not device_results[d]['feasible']]
            if infeasible:
                result, reason = 'invalid', 'infeasible_clock_calibration'
            elif not exact:
                result, reason = 'indeterminate', 'budget_exhausted'
            elif total_lo >= threshold:
                result, reason = 'pass', 'guaranteed_lower_bound_at_or_above_threshold'
            elif total_hi < threshold:
                result, reason = 'fail', 'guaranteed_upper_bound_below_threshold'
            else:
                result, reason = 'indeterminate', 'bounds_straddle_threshold'
        payload = {
            'assessment_id': assessment_id,
            'run_id': run_id,
            'policy': policy,
            'result': result,
            'reason': reason,
            'window': {'start_ms': window_start_ms, 'end_ms': window_end_ms,
                       'interval': '[start,end)'},
            'threshold': threshold,
            'budget': budget if policy == 'guaranteed' else None,
            'evidence': _evidence(run, spec, files, readings, anchors),
            'devices': {d: device_results[d] for d in devices},
            'aggregate_total': total if policy == 'nominal' else None,
            'aggregate_bounds': (None if policy == 'nominal' or result == 'invalid'
                                 or not exact else
                                 {'lower': _frac_text(total_lo),
                                  'upper': _frac_text(total_hi)}),
            'witnessed_bounds': (None if policy == 'nominal' or result == 'invalid'
                                 or exact else
                                 {'lower': _frac_text(witnessed_lo),
                                  'upper': _frac_text(witnessed_hi)}),
        }
        body = canonical(_jsonable(payload))
        digest = hashlib.sha256(body).hexdigest()
        with db:
            old = db.execute('SELECT run_id,payload_hash FROM telemetry_assessment WHERE id=?',
                             (assessment_id,)).fetchone()
            if old:
                if old['run_id'] != run_id:
                    raise ValueError('assessment_run_conflict')
                if old['payload_hash'] != digest:
                    raise ValueError('assessment_identity_conflict')
                stored = db.execute('SELECT payload FROM telemetry_assessment WHERE id=?',
                                    (assessment_id,)).fetchone()
                return json.loads(stored['payload'].decode('utf-8'))
            db.execute('INSERT INTO telemetry_assessment VALUES (?,?,?,?,?,?,?,?)',
                       (assessment_id, run_id, run['version'], result, policy,
                        digest, body, None))
            return json.loads(body.decode('utf-8'))
    finally:
        db.close()


def _jsonable(obj):
    if isinstance(obj, Fraction):
        return _frac_text(obj)
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    return obj


def verify(db_path, assessment_id):
    """Re-derive every claim of a stored assessment from the frozen evidence."""
    db = connect(db_path)
    try:
        stored = db.execute('SELECT * FROM telemetry_assessment WHERE id=?',
                            (assessment_id,)).fetchone()
        if not stored:
            raise ValueError('unknown_assessment')
        payload = json.loads(stored['payload'].decode('utf-8'))
        if hashlib.sha256(stored['payload']).hexdigest() != stored['payload_hash']:
            raise ValueError('assessment_payload_tampered')
        if stored['run_id'] != payload['run_id'] or stored['version'] != payload['evidence']['version']:
            raise ValueError('assessment_run_mismatch')
        run, spec, devices, readings, anchors, files = _load_run(db, stored['run_id'])
        if run['version'] != payload['evidence']['version'] \
                or run['spec_hash'] != payload['evidence']['spec_hash']:
            raise ValueError('assessment_run_mismatch')
        # Re-read every sealed byte through its delivery hash before trusting it.
        for item in payload['evidence']['manifest_files']:
            row = db.execute('''SELECT f.sha256,a.path FROM file_entry f
                JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
                WHERE f.version=? AND f.path=?''',
                             (run['version'], item['path'])).fetchone()
            if not row:
                raise ValueError('sealed_evidence_missing')
            raw = Path(row['path']).read_bytes()
            if hashlib.sha256(raw).hexdigest() != row['sha256'] \
                    or row['sha256'] != item['sha256']:
                raise ValueError('sealed_evidence_tampered')
        expected_evidence = _evidence(run, spec, files, readings, anchors)
        if canonical(_jsonable(expected_evidence)) != canonical(
                _jsonable(payload['evidence'])):
            raise ValueError('assessment_evidence_tampered')
        window = payload['window']
        rate_lo, rate_hi = spec['rate_lo_ms'], spec['rate_hi_ms']
        threshold = payload['threshold']
        # The contract inputs are part of the signed certificate; reject any
        # malformed or rewritten window/threshold before replaying.
        if set(window) != {'start_ms', 'end_ms', 'interval'} \
                or window['interval'] != '[start,end)' \
                or type(window['start_ms']) is not int \
                or type(window['end_ms']) is not int \
                or window['start_ms'] >= window['end_ms']:
            raise ValueError('assessment_window_tampered')
        if type(threshold) is not int or threshold < 0:
            raise ValueError('assessment_threshold_tampered')
        if payload['policy'] not in ('nominal', 'guaranteed') \
                or payload['policy'] != stored['policy']:
            raise ValueError('assessment_policy_tampered')
        if payload['policy'] == 'guaranteed':
            if type(payload.get('budget')) is not int or payload['budget'] <= 0:
                raise ValueError('assessment_budget_tampered')
        elif payload.get('budget') is not None:
            raise ValueError('assessment_budget_tampered')
        if payload['run_id'] != stored['run_id']:
            raise ValueError('assessment_run_mismatch')
        if payload['policy'] == 'nominal':
            for device in devices:
                claim = payload['devices'][device]
                clock = claim['submitted_clock']
                replay = _replay_clock(clock, anchors[device], rate_lo, rate_hi)
                if replay is None:
                    if claim['clock_feasible']:
                        raise ValueError('nominal_clock_anchor_violation')
                    continue
                if not claim['clock_feasible']:
                    raise ValueError('nominal_clock_falsely_rejected')
                rate, offset = replay
                outcome = clockmath.nominal_assessment(
                    rate_lo, rate_hi,
                    [(a['tick'], a['utc_lo_ms'], a['utc_hi_ms']) for a in anchors[device]],
                    [(r['tick'], r['value']) for r in readings[device]],
                    window['start_ms'], window['end_ms'], offset, rate)
                if canonical(_jsonable(outcome['readings'])) != canonical(
                        _jsonable(claim['readings'])) \
                        or outcome['window_total'] != claim['window_total']:
                    raise ValueError('nominal_assignment_mismatch')
            invalid = any(not payload['devices'][d]['clock_feasible'] for d in devices)
            if invalid:
                expected_result, expected_reason = 'invalid', 'nominal_clock_anchor_conflict'
            else:
                total = sum(payload['devices'][d]['window_total'] for d in devices)
                expected_result = 'pass' if total >= threshold else 'fail'
                expected_reason = ('nominal_total_at_or_above_threshold'
                                   if total >= threshold else 'nominal_total_below_threshold')
                if total != payload['aggregate_total']:
                    raise ValueError('nominal_total_mismatch')
        else:
            exact = True
            total_lo = total_hi = 0
            witnessed_lo = witnessed_hi = 0
            for device in devices:
                claim = payload['devices'][device]
                if not claim['feasible']:
                    check = clockmath.feasible_polygon(
                        rate_lo, rate_hi,
                        [(a['tick'], a['utc_lo_ms'], a['utc_hi_ms']) for a in anchors[device]])
                    if check:
                        raise ValueError('infeasibility_falsely_claimed')
                    continue
                outcome = clockmath.guaranteed_bounds(
                    rate_lo, rate_hi,
                    [(a['tick'], a['utc_lo_ms'], a['utc_hi_ms']) for a in anchors[device]],
                    [(r['tick'], r['value']) for r in readings[device]],
                    window['start_ms'], window['end_ms'], payload['budget'])
                if not outcome['feasible']:
                    raise ValueError('infeasible_calibration_missed')
                if outcome['exact'] != claim['exact']:
                    raise ValueError('exactness_mismatch')
                if outcome['lower'] is None and claim['lower_witness'] is not None:
                    raise ValueError('witness_without_budget')
                if outcome['lower'] is not None:
                    for kind, bound_key in (('lower_witness', 'witnessed_lower'),
                                            ('upper_witness', 'witnessed_upper')):
                        witness = claim[kind]
                        replay = _replay_clock(witness['clock'], anchors[device], rate_lo, rate_hi)
                        if replay is None:
                            raise ValueError('witness_clock_infeasible')
                        rate, offset = replay
                        in_ticks = {r['tick'] for r in readings[device]
                                    if window['start_ms'] <= offset + rate * r['tick']
                                    < window['end_ms']}
                        witnessed = sum(r['value'] for r in readings[device]
                                        if r['tick'] in in_ticks)
                        if witnessed != claim[bound_key]:
                            raise ValueError('witness_replay_mismatch')
                        if in_ticks != set(witness['ticks']):
                            raise ValueError('witness_reading_set_mismatch')
                    witnessed_lo += claim['witnessed_lower']
                    witnessed_hi += claim['witnessed_upper']
                if outcome['exact']:
                    if outcome['lower'] != Fraction(claim['witnessed_lower']) \
                            or outcome['upper'] != Fraction(claim['witnessed_upper']):
                        raise ValueError('guaranteed_bound_mismatch')
                    total_lo += outcome['lower']
                    total_hi += outcome['upper']
                else:
                    exact = False
            infeasible = any(not payload['devices'][d]['feasible'] for d in devices)
            if infeasible:
                expected_result, expected_reason = 'invalid', 'infeasible_clock_calibration'
            elif not exact:
                expected_result, expected_reason = 'indeterminate', 'budget_exhausted'
                if payload['witnessed_bounds'] != {
                        'lower': _frac_text(witnessed_lo),
                        'upper': _frac_text(witnessed_hi)}:
                    raise ValueError('witnessed_bounds_mismatch')
            elif total_lo >= threshold:
                expected_result, expected_reason = 'pass', 'guaranteed_lower_bound_at_or_above_threshold'
            elif total_hi < threshold:
                expected_result, expected_reason = 'fail', 'guaranteed_upper_bound_below_threshold'
            else:
                expected_result, expected_reason = 'indeterminate', 'bounds_straddle_threshold'
            expected_aggregate = (None if infeasible or not exact else
                                  {'lower': _frac_text(total_lo),
                                   'upper': _frac_text(total_hi)})
            if payload['aggregate_bounds'] != expected_aggregate:
                raise ValueError('aggregate_bounds_mismatch')
        if payload['result'] != expected_result or payload['reason'] != expected_reason:
            raise ValueError('assessment_verdict_mismatch')
        if stored['result'] != expected_result or stored['policy'] != payload['policy']:
            raise ValueError('assessment_record_mismatch')
        return {'assessment_id': assessment_id, 'valid': True,
                'result': expected_result, 'reason': expected_reason}
    finally:
        db.close()
