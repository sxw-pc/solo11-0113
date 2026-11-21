"""Tests for exact UTC time-window assessment of sealed telemetry runs."""
import json
import random
from fractions import Fraction
from pathlib import Path

import pytest

from chunkproof import engine, telemetry
from chunkproof.store import connect as store_connect


FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'telemetry-run'
MULTI = Path(__file__).resolve().parents[1] / 'fixtures' / 'telemetry-multi'


def seal_fixture(tmp_path, fixture):
    db = tmp_path / 'ledger.db'
    version = engine.manifest(db, 'telemetry-delivery', fixture, 16)
    media = tmp_path / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
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


def registered(tmp_path, fixture, run_id='run-1'):
    db, version = seal_fixture(tmp_path, fixture)
    telemetry.register(db, run_id, version, fixture / 'spec.json')
    return db, version


def make_fixture(tmp_path, name, readings, anchors, rate_lo, rate_hi):
    root = tmp_path / name
    root.mkdir()
    (root / 'readings.jsonl').write_text(
        '\n'.join(json.dumps(r) for r in readings) + '\n', encoding='utf-8')
    (root / 'anchors.jsonl').write_text(
        '\n'.join(json.dumps(a) for a in anchors) + '\n', encoding='utf-8')
    (root / 'spec.json').write_text(json.dumps(
        {'samples': 'readings.jsonl', 'anchors': 'anchors.jsonl',
         'rate_lo_ms': rate_lo, 'rate_hi_ms': rate_hi}), encoding='utf-8')
    return root


def replay(payload, device, kind):
    w = payload['devices'][device][kind]
    rate = Fraction(w['clock']['rate_ms_per_tick'])
    offset = Fraction(w['clock']['offset_ms'])
    return rate, offset, w['ticks']


# ---------------------------------------------------------------- group 1
class TestContractDivergence:
    def test_fixture_nominal_pass_but_guaranteed_indeterminate(self, tmp_path):
        db, version = registered(tmp_path, FIXTURE, 'shift-1')
        nominal = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'nominal',
                                   'a-nom', nominal_clocks={
                                       'D1': {'offset_ms': 0, 'rate_ms_per_tick': 1000}})
        guaranteed = telemetry.assess(db, 'shift-1', 5000, 6000, 10,
                                      'guaranteed', 'a-gua')
        assert nominal['result'] == 'pass'
        assert nominal['aggregate_total'] == 10
        assert nominal['devices']['D1']['readings'][0]['in_window'] is True
        assert nominal['devices']['D1']['readings'][1]['in_window'] is False
        assert guaranteed['result'] == 'indeterminate'
        assert guaranteed['aggregate_bounds'] == {'lower': '0', 'upper': '10'}
        # offset=1000 pushes both readings out: 6000 and 7000, window [5000,6000)
        rate, offset, ticks = replay(guaranteed, 'D1', 'lower_witness')
        assert offset == 1000 and ticks == []
        rate, offset, ticks = replay(guaranteed, 'D1', 'upper_witness')
        assert ticks == [6]
        assert telemetry.verify(db, 'a-nom')['valid'] is True
        assert telemetry.verify(db, 'a-gua')['valid'] is True

    def test_per_reading_endpoint_choice_is_physically_impossible(self, tmp_path):
        # Putting tick 5 at its earliest favorable end AND tick 6 at its latest
        # favorable end would require two different offsets for one device.
        root = make_fixture(tmp_path, 'fx', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10},
            {'device': 'D1', 'seq': 2, 'tick': 6, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': -1000, 'utc_hi_ms': 1000}],
            1000, 1000)
        db, version = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 5000, 6000, 20, 'guaranteed', 'g')
        assert out['aggregate_bounds']['upper'] != '20'
        assert out['aggregate_bounds'] == {'lower': '0', 'upper': '10'}

    def test_nominal_rejects_favorable_unlogged_clock(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        # offset=1000 satisfies the anchor but yields total 0: it is a legal
        # nominal clock, yet its parameters must be recorded, never swapped in.
        honest = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'nominal',
                                  'honest', nominal_clocks={
                                      'D1': {'offset_ms': 1000,
                                             'rate_ms_per_tick': 1000}})
        assert honest['result'] == 'fail' and honest['aggregate_total'] == 0
        assert honest['devices']['D1']['submitted_clock']['offset_ms'] == '1000'
        with pytest.raises(ValueError, match='bad_nominal_clocks'):
            telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'nominal', 'x')


# ---------------------------------------------------------------- group 2
class TestSharedOffsetDriftAndHalfOpen:
    def test_half_open_endpoint_exactly_equal_is_excluded(self, tmp_path):
        # rate pinned to 1000, offset pinned to exactly 0: tick 5 -> 5000 in,
        # tick 6 -> 6000 == end, excluded.
        root = make_fixture(tmp_path, 'half', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 7},
            {'device': 'D1', 'seq': 2, 'tick': 6, 'value': 11}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': 0, 'utc_hi_ms': 0}],
            1000, 1000)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 5000, 6000, 7, 'guaranteed', 'g')
        assert out['result'] == 'pass'
        assert out['aggregate_bounds'] == {'lower': '7', 'upper': '7'}
        nom = telemetry.assess(db, 'r', 5000, 6000, 7, 'nominal', 'n',
                               nominal_clocks={'D1': {'offset_ms': 0,
                                                      'rate_ms_per_tick': 1000}})
        assert [x['in_window'] for x in nom['devices']['D1']['readings']] == [True, False]

    def test_half_open_start_exactly_equal_is_included(self, tmp_path):
        root = make_fixture(tmp_path, 'start', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 3},
            {'device': 'D1', 'seq': 2, 'tick': 4, 'value': 5}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': 0, 'utc_hi_ms': 0}],
            1000, 1000)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 5000, 9000, 8, 'guaranteed', 'g')
        # tick5 -> 5000 == start included (3), tick4 -> 4000 excluded: total 3
        assert out['aggregate_bounds'] == {'lower': '3', 'upper': '3'}
        assert out['result'] == 'fail'

    def test_continuous_rate_drift_changes_membership(self, tmp_path):
        # rate in [1000, 1020], anchor tick0 pins offset to [-50, 50].
        root = make_fixture(tmp_path, 'drift', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10},
            {'device': 'D1', 'seq': 2, 'tick': 6, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': -50, 'utc_hi_ms': 50}],
            1000, 1020)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 5000, 6000, 10, 'guaranteed', 'g')
        assert out['result'] == 'indeterminate'
        lo = Fraction(out['aggregate_bounds']['lower'])
        hi = Fraction(out['aggregate_bounds']['upper'])
        assert lo <= 10 <= hi
        # Witness clocks must really attain their bounds and obey the anchor.
        for kind in ('lower_witness', 'upper_witness'):
            rate, offset, ticks = replay(out, 'D1', kind)
            assert 1000 <= rate <= 1020 and -50 <= offset <= 50
            for anchor in (0,):
                assert -50 <= offset + rate * anchor <= 50

    def test_exact_bounds_dominate_random_feasible_clocks(self, tmp_path):
        root = make_fixture(tmp_path, 'rand', [
            {'device': 'D1', 'seq': i + 1, 'tick': i, 'value': (i * 3) % 7}
            for i in range(20)],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': -100, 'utc_hi_ms': 100},
             {'device': 'D1', 'tick': 19, 'utc_lo_ms': 18900, 'utc_hi_ms': 19200}],
            990, 1010)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 4000, 12000, 30, 'guaranteed', 'g')
        assert out['devices']['D1']['exact'] is True
        lo = Fraction(out['aggregate_bounds']['lower'])
        hi = Fraction(out['aggregate_bounds']['upper'])
        rng = random.Random(20261121)
        readings = [json.loads(line) for line in
                    (root / 'readings.jsonl').read_text().splitlines() if line.strip()]
        for _ in range(400):
            rate = Fraction(rng.randint(990_000, 1010_000), 1000)
            # offset must satisfy both anchors at this rate; intersect ranges.
            off0 = (-100, 100)
            off19 = (18900 - 19 * rate, 19200 - 19 * rate)
            low = max(off0[0], off19[0])
            high = min(off0[1], off19[1])
            assert low <= high
            offset = low + Fraction(rng.randint(0, 1000), 1000) * (high - low)
            total = sum(r['value'] for r in readings
                        if 4000 <= offset + rate * r['tick'] < 12000)
            assert lo <= total <= hi


# ---------------------------------------------------------------- group 3
class TestMultipleDevicesAndDegenerateRegions:
    def test_three_device_fixture_registers_and_assesses(self, tmp_path):
        db, version = registered(tmp_path, MULTI, 'multi')
        assert telemetry.inspect(db, 'multi') == {
            'readings': {'D1': 12, 'D2': 15, 'D3': 18},
            'raw_total': 122, 'anchors': 8}
        out = telemetry.assess(db, 'multi', 0, 50_000, 100, 'guaranteed', 'g')
        assert set(out['devices']) == {'D1', 'D2', 'D3'}
        assert all(d['exact'] for d in out['devices'].values())
        lo = Fraction(out['aggregate_bounds']['lower'])
        hi = Fraction(out['aggregate_bounds']['upper'])
        assert lo <= 122 and hi <= 122
        anchors_on_disk = [json.loads(line) for line in
                           (MULTI / 'anchors.jsonl').read_text().splitlines() if line.strip()]
        for device in ('D1', 'D2', 'D3'):
            for kind in ('lower_witness', 'upper_witness'):
                rate, offset, ticks = replay(out, device, kind)
                assert 990 <= rate <= 1010
                for a in anchors_on_disk:
                    if a['device'] == device:
                        assert a['utc_lo_ms'] <= offset + rate * a['tick'] <= a['utc_hi_ms']
        assert telemetry.verify(db, 'g')['valid'] is True

    def test_feasible_region_collapsed_to_a_line(self, tmp_path):
        # rate fixed 1000; two anchors leave offset in a single segment
        # [-100, 100].  Window [4900, 6100) always contains tick 6 and only
        # contains tick 5 when offset >= 50, so the exact range is 10..20.
        root = make_fixture(tmp_path, 'line', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10},
            {'device': 'D1', 'seq': 2, 'tick': 6, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': -100, 'utc_hi_ms': 100},
             {'device': 'D1', 'tick': 1, 'utc_lo_ms': 900, 'utc_hi_ms': 1100}],
            1000, 1000)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 4900, 6100, 10, 'guaranteed', 'g')
        assert out['devices']['D1']['exact'] is True
        bounds = out['aggregate_bounds']
        assert bounds == {'lower': '10', 'upper': '20'}
        rate, offset, ticks = replay(out, 'D1', 'upper_witness')
        assert rate == 1000 and offset == -100 and ticks == [5, 6]
        rate, offset, ticks = replay(out, 'D1', 'lower_witness')
        assert offset == 100 and ticks == [5]

    def test_feasible_region_collapsed_to_a_point(self, tmp_path):
        root = make_fixture(tmp_path, 'point', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10},
            {'device': 'D1', 'seq': 2, 'tick': 6, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': 0, 'utc_hi_ms': 0},
             {'device': 'D1', 'tick': 1, 'utc_lo_ms': 1000, 'utc_hi_ms': 1000}],
            990, 1010)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 5000, 7000, 20, 'guaranteed', 'g')
        # Only clock is rate=1000, offset=0: both readings inside.
        assert out['result'] == 'pass'
        assert out['aggregate_bounds'] == {'lower': '20', 'upper': '20'}
        rate, offset, _ = replay(out, 'D1', 'lower_witness')
        assert (rate, offset) == (Fraction(1000), Fraction(0))

    def test_independent_device_bounds_combine_without_splitting(self, tmp_path):
        root = make_fixture(tmp_path, 'two', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10},
            {'device': 'D2', 'seq': 1, 'tick': 5, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': -1000, 'utc_hi_ms': 1000},
             {'device': 'D2', 'tick': 0, 'utc_lo_ms': 0, 'utc_hi_ms': 0}],
            1000, 1000)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 5000, 6000, 15, 'guaranteed', 'g')
        # D1 ranges 0..10, D2 fixed at 10: aggregate 10..20 straddles 15.
        assert out['aggregate_bounds'] == {'lower': '10', 'upper': '20'}
        assert out['result'] == 'indeterminate'
        assert out['devices']['D2']['lower_witness']['clock']['offset_ms'] == '0'

    def test_threshold_equality_uses_at_or_above_rule(self, tmp_path):
        # Unique feasible clock gives an exact windowed total of 10.
        root = make_fixture(tmp_path, 'eq', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': 0, 'utc_hi_ms': 0}],
            1000, 1000)
        db, _ = registered(tmp_path, root, 'r')
        assert telemetry.assess(db, 'r', 5000, 6000, 10, 'guaranteed',
                                'eq-pass')['result'] == 'pass'
        assert telemetry.assess(db, 'r', 5000, 6000, 11, 'guaranteed',
                                'eq-fail')['result'] == 'fail'
        nominal = telemetry.assess(db, 'r', 5000, 6000, 10, 'nominal',
                                   'eq-nom', nominal_clocks={
                                       'D1': {'offset_ms': 0,
                                              'rate_ms_per_tick': 1000}})
        assert nominal['result'] == 'pass'

    def test_multi_device_budget_is_accounted_per_device(self, tmp_path):
        db, _ = registered(tmp_path, MULTI, 'multi')
        out = telemetry.assess(db, 'multi', 0, 50_000, 100, 'guaranteed',
                               'g', budget=100_000)
        # Each device receives the full budget independently; none may consume
        # another device's allowance.
        assert all(d['exact'] for d in out['devices'].values())
        assert all(d['remaining_budget'] > 0 for d in out['devices'].values())
        assert telemetry.verify(db, 'g')['valid'] is True


# ---------------------------------------------------------------- group 4
class TestInfeasibleBudgetAndCertificates:
    def test_conflicting_anchors_yield_invalid_with_reference(self, tmp_path):
        root = make_fixture(tmp_path, 'conflict', [
            {'device': 'D1', 'seq': 1, 'tick': 5, 'value': 10}],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': 0, 'utc_hi_ms': 10},
             {'device': 'D1', 'tick': 1, 'utc_lo_ms': 0, 'utc_hi_ms': 10}],
            1000, 1000)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 0, 100, 1, 'guaranteed', 'g')
        assert out['result'] == 'invalid'
        conflict = out['devices']['D1']['conflict_anchors']
        assert {c['tick'] for c in conflict} == {0, 1}
        assert telemetry.verify(db, 'g')['valid'] is True
        nom = telemetry.assess(db, 'r', 0, 100, 1, 'nominal', 'n',
                               nominal_clocks={'D1': {'offset_ms': 5,
                                                      'rate_ms_per_tick': 1000}})
        assert nom['result'] == 'invalid'
        assert nom['devices']['D1']['anchor_violations']
        assert telemetry.verify(db, 'n')['valid'] is True

    def test_nominal_clock_outside_rate_or_anchors_is_invalid(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        bad_rate = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'nominal',
                                    'bad-rate', nominal_clocks={
                                        'D1': {'offset_ms': 0,
                                               'rate_ms_per_tick': 999}})
        assert bad_rate['result'] == 'invalid'
        bad_off = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'nominal',
                                   'bad-off', nominal_clocks={
                                       'D1': {'offset_ms': 5000,
                                              'rate_ms_per_tick': 1000}})
        assert bad_off['result'] == 'invalid'
        assert telemetry.verify(db, 'bad-rate')['valid'] is True

    def test_budget_exhaustion_is_indeterminate_with_witnessed_bounds(self, tmp_path):
        root = make_fixture(tmp_path, 'budget', [
            {'device': 'D1', 'seq': i + 1, 'tick': i, 'value': (i * 5) % 11}
            for i in range(16)],
            [{'device': 'D1', 'tick': 0, 'utc_lo_ms': -200, 'utc_hi_ms': 200},
             {'device': 'D1', 'tick': 15, 'utc_lo_ms': 14800, 'utc_hi_ms': 15200}],
            990, 1010)
        db, _ = registered(tmp_path, root, 'r')
        out = telemetry.assess(db, 'r', 2000, 10000, 50, 'guaranteed',
                               'g-small', budget=300)
        assert out['result'] == 'indeterminate'
        assert out['reason'] == 'budget_exhausted'
        assert out['devices']['D1']['exact'] is False
        # Partial work is retained as a replayable witness, never sold as a
        # global bound; the stored totals match the claimed clocks.
        device = out['devices']['D1']
        if device['lower_witness'] is not None:
            for kind in ('lower_witness', 'upper_witness'):
                rate, offset, ticks = replay(out, 'D1', kind)
                replayed = sum(r['value'] for r in
                               json.loads((root / 'readings.jsonl').read_text())
                               if r['tick'] in ticks)
                bound_key = ('witnessed_lower' if kind == 'lower_witness'
                             else 'witnessed_upper')
                assert replayed == device[bound_key]
        assert telemetry.verify(db, 'g-small')['valid'] is True
        full = telemetry.assess(db, 'r', 2000, 10000, 50, 'guaranteed', 'g-full')
        assert full['devices']['D1']['exact'] is True
        assert telemetry.verify(db, 'g-full')['valid'] is True

    def test_illegal_window_threshold_and_policy_are_rejected(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        with pytest.raises(ValueError, match='bad_window'):
            telemetry.assess(db, 'shift-1', 6000, 6000, 10, 'guaranteed', 'a')
        with pytest.raises(ValueError, match='bad_window'):
            telemetry.assess(db, 'shift-1', 7000, 6000, 10, 'guaranteed', 'a')
        with pytest.raises(ValueError, match='bad_threshold'):
            telemetry.assess(db, 'shift-1', 5000, 6000, -1, 'guaranteed', 'a')
        with pytest.raises(ValueError, match='bad_policy'):
            telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'wishful', 'a')
        with pytest.raises(ValueError, match='unknown_telemetry_run'):
            telemetry.assess(db, 'nope', 5000, 6000, 10, 'guaranteed', 'a')

    def test_tampered_anchor_cannot_verify(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        out = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'g')
        # Direct ledger tampering of an anchor after the certificate is stored.
        with store_connect(db) as ledger:
            ledger.execute("UPDATE telemetry_anchor SET utc_lo_ms=-5000 "
                           "WHERE run_id='shift-1'")
        with pytest.raises(ValueError, match='assessment_evidence_tampered'):
            telemetry.verify(db, 'g')
        with pytest.raises(ValueError):
            telemetry.verify(db, 'missing-id')

    def test_tampered_sealed_bytes_cannot_verify(self, tmp_path):
        db, version = registered(tmp_path, FIXTURE, 'shift-1')
        telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'g')
        out_dir = tmp_path / 'out'
        (out_dir / 'anchors.jsonl').write_text('{"device":"D1"}', encoding='utf-8')
        with pytest.raises(ValueError, match='sealed_evidence_tampered'):
            telemetry.verify(db, 'g')

    def test_forged_bound_witness_fails_replay(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        out = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'g')
        # Forge a certificate claiming an upper bound 20 that no shared clock
        # can produce; store it under a new assessment id.
        forged = json.loads(json.dumps(out))
        forged['assessment_id'] = 'forged'
        forged['devices']['D1']['witnessed_upper'] = 20
        forged['devices']['D1']['upper_witness'] = {
            'clock': {'rate_ms_per_tick': '1000', 'offset_ms': '0'},
            'ticks': [5, 6]}
        forged['aggregate_bounds'] = {'lower': '0', 'upper': '20'}
        body = telemetry.canonical(telemetry._jsonable(forged))
        import hashlib
        with store_connect(db) as ledger:
            ledger.execute('INSERT INTO telemetry_assessment VALUES (?,?,?,?,?,?,?,?)',
                           ('forged', 'shift-1',
                            ledger.execute("SELECT version FROM telemetry_run WHERE id='shift-1'")
                            .fetchone()[0], 'indeterminate', 'guaranteed',
                            hashlib.sha256(body).hexdigest(), body, None))
        with pytest.raises(ValueError, match='witness_replay_mismatch'):
            telemetry.verify(db, 'forged')

    def test_rewritten_contract_inputs_without_rehash_are_rejected(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'g')
        # Flip payload bytes while leaving the stored hash column untouched.
        def rewrite(mutate, assessment_id='g'):
            with store_connect(db) as ledger:
                row = ledger.execute(
                    'SELECT payload FROM telemetry_assessment WHERE id=?',
                    (assessment_id,)).fetchone()
                obj = json.loads(row[0])
                mutate(obj)
                ledger.execute('UPDATE telemetry_assessment SET payload=? WHERE id=?',
                               (json.dumps(obj, ensure_ascii=False).encode(),
                                assessment_id))

        rewrite(lambda o: o.__setitem__('threshold', 1))
        with pytest.raises(ValueError, match='assessment_payload_tampered'):
            telemetry.verify(db, 'g')

    def test_malformed_window_or_budget_in_certificate_are_rejected(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        out = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'g')
        import hashlib

        def forge(mutate, new_id):
            forged = json.loads(json.dumps(out))
            forged['assessment_id'] = new_id
            mutate(forged)
            body = telemetry.canonical(telemetry._jsonable(forged))
            with store_connect(db) as ledger:
                version = ledger.execute(
                    "SELECT version FROM telemetry_run WHERE id='shift-1'").fetchone()[0]
                ledger.execute(
                    'INSERT INTO telemetry_assessment VALUES (?,?,?,?,?,?,?,?)',
                    (new_id, 'shift-1', version, forged['result'], 'guaranteed',
                     hashlib.sha256(body).hexdigest(), body, None))

        forge(lambda o: o['window'].update({'end_ms': 5000}), 'bad-window')
        with pytest.raises(ValueError, match='assessment_window_tampered'):
            telemetry.verify(db, 'bad-window')
        forge(lambda o: o.__setitem__('budget', 0), 'bad-budget')
        with pytest.raises(ValueError, match='assessment_budget_tampered'):
            telemetry.verify(db, 'bad-budget')


# ---------------------------------------------------------------- group 5
class TestVersioningIdempotencyAndNonRegression:
    def test_same_id_same_payload_is_idempotent_and_conflicts_otherwise(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        a1 = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'dup')
        a2 = telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'guaranteed', 'dup')
        assert a1 == a2
        with pytest.raises(ValueError, match='assessment_identity_conflict'):
            telemetry.assess(db, 'shift-1', 5000, 6000, 11, 'guaranteed', 'dup')
        with pytest.raises(ValueError, match='assessment_identity_conflict'):
            telemetry.assess(db, 'shift-1', 5000, 6000, 10, 'nominal', 'dup',
                             nominal_clocks={'D1': {'offset_ms': 0,
                                                    'rate_ms_per_tick': 1000}})

    def test_new_delivery_version_gets_new_run_old_evidence_persists(self, tmp_path):
        db, v1 = registered(tmp_path, FIXTURE, 'run-v1')
        old = telemetry.assess(db, 'run-v1', 5000, 6000, 10, 'guaranteed', 'old-g')
        assert telemetry.verify(db, 'old-g')['valid'] is True
        # Build a new delivery version with changed readings.
        new_fixture = tmp_path / 'v2'
        new_fixture.mkdir()
        (new_fixture / 'readings.jsonl').write_text(
            '{"device":"D1","seq":1,"tick":5,"value":99}\n', encoding='utf-8')
        (new_fixture / 'anchors.jsonl').write_text(
            (FIXTURE / 'anchors.jsonl').read_text(), encoding='utf-8')
        (new_fixture / 'spec.json').write_text((FIXTURE / 'spec.json').read_text(),
                                               encoding='utf-8')
        version2 = engine.manifest(db, 'delivery-2', new_fixture, 16)
        media = tmp_path / 'media2'
        media.mkdir()
        receipts = []
        for index, chunk in enumerate(engine.plan(db, version2)):
            raw = (new_fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
            part = f'p-{index}'
            (media / part).write_bytes(raw)
            receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                        'path': part, 'sha256': chunk['sha256']}))
        rp = tmp_path / 'r2.jsonl'
        rp.write_text('\n'.join(receipts), encoding='utf-8')
        engine.receive(db, version2, 'M2', media, rp)
        engine.assemble(db, version2, media, tmp_path / 'out2')
        assert engine.seal(db, version2, tmp_path / 'out2') == '可发布'
        telemetry.register(db, 'run-v2', version2, new_fixture / 'spec.json')
        fresh = telemetry.assess(db, 'run-v2', 5000, 6000, 10, 'guaranteed', 'new-g')
        assert fresh['evidence']['version'] == version2 != v1
        # Old certificate still replays against the old frozen evidence.
        again = telemetry.verify(db, 'old-g')
        assert again['valid'] is True and again['result'] == old['result']
        with pytest.raises(ValueError, match='assessment_run_conflict'):
            telemetry.assess(db, 'run-v2', 5000, 6000, 10, 'guaranteed', 'old-g')

    def test_inspect_still_does_not_assign_windows(self, tmp_path):
        db, _ = registered(tmp_path, FIXTURE, 'shift-1')
        assert telemetry.inspect(db, 'shift-1') == {
            'readings': {'D1': 2}, 'raw_total': 20, 'anchors': 1}

    def test_register_still_rejects_duplicate_identities(self, tmp_path):
        db, version = seal_fixture(tmp_path, FIXTURE)
        digest = telemetry.register(db, 'shift-1', version, FIXTURE / 'spec.json')
        assert digest == telemetry.register(db, 'shift-1', version,
                                            FIXTURE / 'spec.json')
        with pytest.raises(ValueError, match='telemetry_identity_conflict'):
            telemetry.register(db, 'shift-1', 'other-version',
                               FIXTURE / 'spec.json')
