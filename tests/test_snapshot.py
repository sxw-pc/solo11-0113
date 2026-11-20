"""Offline tests for recipient-queryable changefeed snapshots.

Everything here is built from sealed JSONL byte evidence: the fixtures are
manifested, received, assembled and sealed exactly like a physical delivery
before any changefeed registration happens.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

from chunkproof import changefeed, engine
from chunkproof.store import connect

ROOT = Path(__file__).resolve().parents[1]
SMALL = ROOT / 'fixtures' / 'changefeed'
LARGE = ROOT / 'fixtures' / 'changefeed_large'


def seal_fixture(tmp_path, fixture, run_id='feed-1', delivery='d', slot='first'):
    """Manifest -> receive -> assemble -> seal -> register a frozen feed."""
    work = tmp_path / slot
    work.mkdir(exist_ok=True)
    db = work / 'ledger.db'
    version = engine.manifest(db, delivery, fixture, 16)
    media = work / 'medium'
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (fixture / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'part-{index}'
        (media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = work / 'receipts.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M1', media, receipt_path)
    engine.assemble(db, version, media, work / 'out')
    assert engine.seal(db, version, work / 'out') == '可发布'
    changefeed.register(db, run_id, version, fixture / 'spec.json')
    return db, version


def write_feed(tmp_path, name, transactions, rows):
    """Freeze a synthetic feed directory and return its spec path."""
    folder = tmp_path / name
    folder.mkdir()
    (folder / 'transactions.jsonl').write_text(
        '\n'.join(json.dumps(t, ensure_ascii=False) for t in transactions), encoding='utf-8')
    (folder / 'rows.jsonl').write_text(
        '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows), encoding='utf-8')
    spec = folder / 'spec.json'
    spec.write_text(json.dumps({'transactions': 'transactions.jsonl', 'rows': 'rows.jsonl'}),
                    encoding='utf-8')
    return folder, spec


def tx(tx_id, source, seq, parents, expected, authority):
    return {'tx_id': tx_id, 'source': source, 'source_seq': seq, 'parents': parents,
            'expected_rows': expected, 'authority': authority}


def row(tx_id, ordinal, key, op, value=None):
    item = {'tx_id': tx_id, 'ordinal': ordinal, 'entity_key': key, 'op': op}
    if op == 'put':
        item['value'] = value
    return item


# --------------------------------------------------------------------------- #
# Acceptance group 1: the two contracts diverge on the same conflict evidence
# --------------------------------------------------------------------------- #

def test_two_contracts_produce_different_snapshots_on_same_evidence(tmp_path):
    db, _ = seal_fixture(tmp_path, SMALL)
    coverage = changefeed.materialize(db, 'feed-1', 'max_coverage', 'snap-cover')
    authority = changefeed.materialize(db, 'feed-1', 'max_authority', 'snap-auth')

    assert coverage['selection']['tx_ids'] == ['t1', 't3', 't4', 't7', 't8']
    assert coverage['selection']['metrics'] == {'transactions': 5, 'authority': 5}
    assert coverage['entities']['x'] == {'value': 7, 'last_write_tx': 't7'}
    assert coverage['entities']['y'] == {'value': 8, 'last_write_tx': 't8'}

    assert authority['selection']['tx_ids'] == ['t1', 't3', 't4', 't6']
    assert authority['selection']['metrics'] == {'transactions': 4, 'authority': 8}
    assert authority['entities']['x'] == {'value': 100, 'last_write_tx': 't6'}
    assert authority['entities']['y'] == {'value': 100, 'last_write_tx': 't6'}

    # Same frozen run, same evidence hash, materially different query results.
    assert coverage['frozen']['spec_hash'] == authority['frozen']['spec_hash']
    assert coverage['evidence_hash'] != authority['evidence_hash']
    assert changefeed.query(db, 'snap-cover', 'x')['x']['value'] == 7
    assert changefeed.query(db, 'snap-auth', 'x')['x']['value'] == 100


def test_large_fixture_policies_diverge_across_four_sources(tmp_path):
    db, _ = seal_fixture(tmp_path, LARGE, run_id='large')
    coverage = changefeed.materialize(db, 'large', 'max_coverage', 'lc')
    authority = changefeed.materialize(db, 'large', 'max_authority', 'la')

    assert len(coverage['candidates']) == 27
    assert coverage['selection']['metrics'] == {'transactions': 18, 'authority': 23}
    assert authority['selection']['metrics'] == {'transactions': 14, 'authority': 30}
    # c2+c4 (two tx, authority 5) wins on coverage; c3 alone (authority 4) wins
    # on authority. The b-chain vs heavy b4 bundle diverges the same way.
    assert {'c2', 'c4'} <= set(coverage['selection']['tx_ids'])
    assert 'c3' in authority['selection']['tx_ids']
    # Coverage keeps the low-weight B chain pieces, authority takes the heavy
    # b4 bundle (which excludes b3/b5 through its multi-key closure).
    assert 'b5' in coverage['selection']['tx_ids']
    assert 'b4' not in coverage['selection']['tx_ids']
    assert 'b4' in authority['selection']['tx_ids']
    assert 'b5' not in authority['selection']['tx_ids']
    assert coverage['entities']['k']['value'] == 20
    assert authority['entities']['k']['value'] == 30
    assert authority['entities']['h']['value'] == 99
    # Causal rewrite chain a1->a3->a6 leaves the descendant value.
    assert coverage['entities']['g'] == {'value': 3, 'last_write_tx': 'a6'}
    # Delete/put chain on m terminates with the key absent.
    assert 'm' not in coverage['entities']
    for snapshot_id in ('lc', 'la'):
        verdict = changefeed.verify(db, snapshot_id)
        assert verdict['status'] == 'verified'


# --------------------------------------------------------------------------- #
# Acceptance group 2: atomic row counts, causal closure, transport gaps
# --------------------------------------------------------------------------- #

def test_incomplete_transaction_never_takes_effect(tmp_path):
    # t5 claims two rows but only ordinal 0 arrives: it and its effects vanish.
    db, _ = seal_fixture(tmp_path, SMALL)
    doc = changefeed.materialize(db, 'feed-1', 'max_coverage', 's')
    assert doc['excluded'] == [{'tx_id': 't5', 'reason': 'missing_ordinal'}]
    assert 't5' not in doc['selection']['tx_ids']
    assert 'd' not in doc['entities']
    candidate = next(c for c in doc['candidates'] if c['tx_id'] == 't5')
    assert candidate == {'tx_id': 't5', 'eligible': False, 'reason': 'missing_ordinal',
                         'authority': 1, 'source_seq': 2}


def test_transport_seq_gap_does_not_block_independent_complete_tx(tmp_path):
    # A's seq 2 is absent, but t3 (seq 3) only depends on the complete t1.
    transactions = [
        tx('t1', 'A', 1, [], 1, 1), tx('t3', 'A', 3, ['t1'], 1, 1),
        tx('u1', 'B', 1, [], 1, 1)]
    rows = [row('t1', 0, 'a', 'put', 1), row('t3', 0, 'b', 'put', 2),
            row('u1', 0, 'c', 'put', 3)]
    folder, spec = write_feed(tmp_path, 'gap', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='gap')
    doc = changefeed.materialize(db, 'gap', 'max_coverage', 's')
    assert doc['selection']['tx_ids'] == ['t1', 't3', 'u1']
    assert all(c['eligible'] for c in doc['candidates'])


def test_missing_parent_and_missing_ordinal_exclude_without_half_effects(tmp_path):
    transactions = [
        tx('root', 'A', 1, [], 1, 1),
        tx('orphan', 'A', 2, ['ghost'], 1, 1),       # parent never sent
        tx('gappy', 'B', 1, ['root'], 2, 1)]         # claims ordinals 0,1; 1 absent
    rows = [row('root', 0, 'a', 'put', 1), row('orphan', 0, 'b', 'put', 2),
            row('gappy', 0, 'c', 'put', 3)]
    folder, spec = write_feed(tmp_path, 'pathology', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='p')
    doc = changefeed.materialize(db, 'p', 'max_coverage', 's')
    reasons = {e['tx_id']: e['reason'] for e in doc['excluded']}
    assert reasons == {'orphan': 'missing_parent', 'gappy': 'missing_ordinal'}
    assert doc['selection']['tx_ids'] == ['root']
    assert set(doc['entities']) == {'a'}


def test_duplicate_ordinal_is_rejected_at_registration(tmp_path):
    # Duplicate (tx, ordinal) pairs violate frozen feed integrity outright.
    transactions = [tx('dup', 'A', 1, [], 2, 1)]
    rows = [row('dup', 0, 'c', 'put', 3), row('dup', 0, 'd', 'put', 4)]
    folder, _ = write_feed(tmp_path, 'dupord', transactions, rows)
    with pytest.raises(ValueError, match='duplicate_change_ordinal'):
        seal_fixture(tmp_path, folder, run_id='dup')


def test_causal_cycle_and_its_downstream_are_never_selected(tmp_path):
    transactions = [
        tx('x1', 'A', 1, [], 1, 1), tx('x2', 'A', 2, ['x3'], 1, 1),
        tx('x3', 'A', 3, ['x2'], 1, 1), tx('x4', 'A', 4, ['x3'], 1, 1),
        tx('free', 'B', 1, [], 1, 1)]
    rows = [row('x1', 0, 'a', 'put', 1), row('x2', 0, 'b', 'put', 2),
            row('x3', 0, 'c', 'put', 3), row('x4', 0, 'd', 'put', 4),
            row('free', 0, 'e', 'put', 5)]
    folder, _ = write_feed(tmp_path, 'cycle', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='cyc')
    doc = changefeed.materialize(db, 'cyc', 'max_coverage', 's')
    reasons = {e['tx_id']: e['reason'] for e in doc['excluded']}
    assert reasons['x2'] == 'causal_cycle'
    assert reasons['x3'] == 'causal_cycle'
    assert reasons['x4'] == 'missing_parent'
    assert doc['selection']['tx_ids'] == ['free', 'x1']


def test_selected_set_is_parent_closed_and_replay_is_topological(tmp_path):
    db, _ = seal_fixture(tmp_path, LARGE, run_id='large')
    doc = changefeed.materialize(db, 'large', 'max_coverage', 'lc')
    selected = set(doc['selection']['tx_ids'])
    ledger = connect(db)
    for tx_id in selected:
        parents = json.loads(ledger.execute(
            'SELECT parents FROM change_tx WHERE run_id=? AND tx_id=?',
            ('large', tx_id)).fetchone()['parents'])
        assert set(parents) <= selected
    order = doc['selection']['replay_order']
    position = {tx_id: i for i, tx_id in enumerate(order)}
    for tx_id in selected:
        parents = json.loads(ledger.execute(
            'SELECT parents FROM change_tx WHERE run_id=? AND tx_id=?',
            ('large', tx_id)).fetchone()['parents'])
        assert all(position[p] < position[tx_id] for p in parents if p in selected)
    ledger.close()


# --------------------------------------------------------------------------- #
# Acceptance group 3: concurrent key conflicts and delete/put semantics
# --------------------------------------------------------------------------- #

def test_concurrent_conflict_is_not_settled_by_arrival_or_seq(tmp_path):
    # Same key, no ancestor relation: picking either is a conflict, never both.
    transactions = [
        tx('low', 'A', 9, [], 1, 1), tx('high', 'B', 1, [], 1, 5)]
    rows = [row('low', 0, 'k', 'put', 'a'), row('high', 0, 'k', 'put', 'b')]
    folder, _ = write_feed(tmp_path, 'conflict', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='cf')
    coverage = changefeed.materialize(db, 'cf', 'max_coverage', 'c')
    authority = changefeed.materialize(db, 'cf', 'max_authority', 'a')
    # Coverage tie (one tx each) breaks on the canonical tx_id set.
    assert coverage['selection']['tx_ids'] == ['high']
    assert coverage['entities']['k']['value'] == 'b'
    assert authority['selection']['tx_ids'] == ['high']
    candidate = next(c for c in coverage['candidates'] if c['tx_id'] == 'low')
    assert candidate['reason'] == 'conflicts_with_selected'
    assert candidate['conflicts'] == ['high']


def test_ancestor_order_applies_descendant_value_on_same_key(tmp_path):
    transactions = [tx('p', 'A', 1, [], 1, 1), tx('q', 'A', 2, ['p'], 1, 1)]
    rows = [row('p', 0, 'k', 'put', 'old'), row('q', 0, 'k', 'put', 'new')]
    folder, _ = write_feed(tmp_path, 'causal', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='ca')
    doc = changefeed.materialize(db, 'ca', 'max_coverage', 's')
    assert doc['entities']['k'] == {'value': 'new', 'last_write_tx': 'q'}
    assert doc['selection']['replay_order'] == ['p', 'q']


def test_delete_then_put_and_delete_removal(tmp_path):
    transactions = [
        tx('m1', 'A', 1, [], 1, 1), tx('m2', 'A', 2, ['m1'], 1, 1),
        tx('m3', 'A', 3, ['m2'], 1, 1), tx('m4', 'B', 1, [], 1, 1)]
    rows = [row('m1', 0, 'm', 'put', 1), row('m2', 0, 'm', 'delete'),
            row('m3', 0, 'm', 'put', 3), row('m4', 0, 'gone', 'delete')]
    folder, _ = write_feed(tmp_path, 'delete', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='dl')
    doc = changefeed.materialize(db, 'dl', 'max_coverage', 's')
    assert doc['entities']['m'] == {'value': 3, 'last_write_tx': 'm3'}
    assert 'gone' not in doc['entities']


def test_unrelated_sources_merge_into_one_snapshot(tmp_path):
    transactions = [
        tx('s1', 'A', 1, [], 1, 1), tx('s2', 'B', 1, [], 1, 1),
        tx('s3', 'C', 1, [], 1, 1), tx('s4', 'D', 1, [], 1, 1)]
    rows = [row('s1', 0, 'k1', 'put', 1), row('s2', 0, 'k2', 'put', 2),
            row('s3', 0, 'k3', 'put', 3), row('s4', 0, 'k4', 'put', 4)]
    folder, _ = write_feed(tmp_path, 'merge', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='mg')
    doc = changefeed.materialize(db, 'mg', 'max_coverage', 's')
    assert doc['selection']['tx_ids'] == ['s1', 's2', 's3', 's4']
    assert {k: v['value'] for k, v in doc['entities'].items()} == {
        'k1': 1, 'k2': 2, 'k3': 3, 'k4': 4}


def test_multikey_transaction_drags_its_full_conflict_closure(tmp_path):
    # tx heavy writes both x and y; two lightweight tx split those keys.
    transactions = [
        tx('heavy', 'A', 1, [], 2, 5), tx('lx', 'B', 1, [], 1, 1),
        tx('ly', 'C', 1, [], 1, 1)]
    rows = [row('heavy', 0, 'x', 'put', 100), row('heavy', 1, 'y', 'put', 100),
            row('lx', 0, 'x', 'put', 7), row('ly', 0, 'y', 'put', 8)]
    folder, _ = write_feed(tmp_path, 'multikey', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='mk')
    coverage = changefeed.materialize(db, 'mk', 'max_coverage', 'c')
    authority = changefeed.materialize(db, 'mk', 'max_authority', 'a')
    # Coverage prefers the two single-key tx; authority prefers the heavy one.
    assert coverage['selection']['tx_ids'] == ['lx', 'ly']
    assert authority['selection']['tx_ids'] == ['heavy']
    assert authority['entities']['x']['value'] == 100
    # No feasible snapshot may mix heavy with either lightweight tx.
    for doc in (coverage, authority):
        selected = set(doc['selection']['tx_ids'])
        if 'heavy' in selected:
            assert not ({'lx', 'ly'} & selected)


# --------------------------------------------------------------------------- #
# Acceptance group 4: optimality certificate and search budget
# --------------------------------------------------------------------------- #

def test_certificate_records_global_optimum_for_both_objectives(tmp_path):
    db, _ = seal_fixture(tmp_path, SMALL)
    doc = changefeed.materialize(db, 'feed-1', 'max_coverage', 's')
    cert = doc['optimality']
    assert set(cert) == {'max_coverage', 'max_authority'}
    assert cert['max_coverage']['optimum'] == {
        'primary': 5, 'secondary': 5, 'tx_ids': ['t1', 't3', 't4', 't7', 't8']}
    assert cert['max_authority']['optimum']['tx_ids'] == ['t1', 't3', 't4', 't6']
    # 6 eligible tx (t5 blocked); optimistic bound includes every candidate.
    assert cert['max_coverage']['upper_bound'] == {'primary': 6, 'secondary': 10}
    assert cert['max_authority']['upper_bound'] == {'primary': 10, 'secondary': 6}
    assert cert['max_coverage']['nodes_explored'] <= cert['max_coverage']['node_budget']


def test_exhausted_budget_returns_indeterminate_with_proven_bound(tmp_path):
    # Four free tx plus a ten-way conflict group: a small budget proves an
    # incumbent but cannot finish exploring the conflict group.
    transactions = [tx(f'f{i}', 'B', i + 1, [], 1, 1) for i in range(4)]
    transactions += [tx(f'w{i}', 'A', i + 1, [], 1, 1) for i in range(10)]
    rows = [row(f'f{i}', 0, f'free{i}', 'put', i) for i in range(4)]
    rows += [row(f'w{i}', 0, 'shared', 'put', i) for i in range(10)]
    folder, _ = write_feed(tmp_path, 'budget', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='bd')
    result = changefeed.materialize(db, 'bd', 'max_coverage', 'ind', node_budget=6)
    assert result['status'] == 'indeterminate'
    assert result['search']['nodes_explored'] > 6
    bound = result['search']['proven_bound']
    assert bound is not None
    assert bound['primary'] >= 5  # four free tx plus at least one conflict tx
    assert result['search']['upper_bound']['primary'] == 14
    # No snapshot row may be sealed when the proof did not terminate.
    ledger = connect(db)
    assert ledger.execute('SELECT count(*) n FROM snapshot').fetchone()['n'] == 0
    ledger.close()
    with pytest.raises(ValueError, match='unknown_snapshot'):
        changefeed.verify(db, 'ind')


def test_default_budget_solves_large_feed_within_limits(tmp_path):
    db, _ = seal_fixture(tmp_path, LARGE, run_id='large')
    doc = changefeed.materialize(db, 'large', 'max_coverage', 'lc')
    for stats in doc['optimality'].values():
        assert stats['nodes_explored'] <= changefeed.DEFAULT_NODE_BUDGET
        assert 'optimum' in stats
    assert changefeed.verify(db, 'lc')['status'] == 'verified'


def test_branch_and_bound_matches_brute_force_enumeration(tmp_path):
    # Independent oracle: enumerate every feasible closed/conflict-free subset
    # and confirm the branch-and-bound optimum equals the exhaustive one.
    import itertools
    transactions = [
        tx('r', 'A', 1, [], 1, 2),
        tx('a', 'A', 2, ['r'], 1, 3), tx('b', 'A', 3, ['r'], 1, 1),
        tx('c', 'B', 1, ['r'], 1, 4), tx('d', 'B', 2, ['a'], 1, 1),
        tx('e', 'C', 1, ['r'], 2, 5), tx('f', 'C', 2, ['r'], 1, 2),
        tx('g', 'D', 1, [], 1, 1), tx('h', 'D', 2, ['g'], 1, 6)]
    rows = [
        row('r', 0, 'base', 'put', 0), row('a', 0, 'k1', 'put', 1),
        row('b', 0, 'k1', 'put', 2), row('c', 0, 'k2', 'put', 3),
        row('d', 0, 'k2', 'put', 4), row('e', 0, 'k1', 'put', 5),
        row('e', 1, 'k2', 'put', 5), row('f', 0, 'k3', 'put', 6),
        row('g', 0, 'k3', 'put', 7), row('h', 0, 'k4', 'put', 8)]
    folder, _ = write_feed(tmp_path, 'oracle', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='oracle')

    # Brute force over the eligible set using the same structural rules.
    evidence = changefeed._load_evidence(connect(db), 'oracle')
    eligible, _, ancestors = changefeed._classify(evidence)
    conflicts = changefeed._conflict_graph(eligible, ancestors, evidence)
    eligible = sorted(eligible)

    def feasible(subset):
        chosen = set(subset)
        for tx_id in chosen:
            if not set(evidence['txs'][tx_id]['parents']) <= chosen:
                return False
            if conflicts[tx_id] & chosen:
                return False
        return True

    for policy in changefeed.POLICIES:
        best_key, best_set = None, None
        for size in range(len(eligible) + 1):
            for subset in itertools.combinations(eligible, size):
                if not feasible(subset):
                    continue
                count = len(subset)
                weight = sum(evidence['txs'][t]['authority'] for t in subset)
                candidate = ((count, weight) if policy == 'max_coverage'
                             else (weight, count)) + (tuple(sorted(subset)),)
                if best_key is None or candidate > best_key:
                    best_key, best_set = candidate, set(subset)
        doc = changefeed.materialize(db, 'oracle', policy, f'o-{policy}')
        assert set(doc['selection']['tx_ids']) == best_set
        assert changefeed.verify(db, f'o-{policy}')['status'] == 'verified'


def test_no_greedy_or_split_row_cheat_can_match_optimum(tmp_path):
    # Greedy-by-authority would take heavy first and lose two feasible tx;
    # splitting heavy's rows is forbidden because transactions are atomic.
    transactions = [
        tx('heavy', 'A', 1, [], 2, 5), tx('lx', 'B', 1, [], 1, 1),
        tx('ly', 'C', 1, [], 1, 1)]
    rows = [row('heavy', 0, 'x', 'put', 100), row('heavy', 1, 'y', 'put', 100),
            row('lx', 0, 'x', 'put', 7), row('ly', 0, 'y', 'put', 8)]
    folder, _ = write_feed(tmp_path, 'greed', transactions, rows)
    db, _ = seal_fixture(tmp_path, folder, run_id='gr')
    doc = changefeed.materialize(db, 'gr', 'max_coverage', 's')
    assert doc['selection']['metrics'] == {'transactions': 2, 'authority': 2}
    heavy = next(c for c in doc['candidates'] if c['tx_id'] == 'heavy')
    assert heavy['reason'] == 'conflicts_with_selected'


# --------------------------------------------------------------------------- #
# Acceptance group 5: versions, idempotency, independent review, no regression
# --------------------------------------------------------------------------- #

def test_snapshot_idempotent_same_bytes_conflicting_diverges(tmp_path):
    db, _ = seal_fixture(tmp_path, SMALL)
    first = changefeed.materialize(db, 'feed-1', 'max_coverage', 'snap')
    second = changefeed.materialize(db, 'feed-1', 'max_coverage', 'snap')
    assert first['evidence_hash'] == second['evidence_hash']
    with pytest.raises(ValueError, match='snapshot_identity_conflict'):
        changefeed.materialize(db, 'feed-1', 'max_authority', 'snap')
    with pytest.raises(ValueError, match='snapshot_policy_taken'):
        changefeed.materialize(db, 'feed-1', 'max_coverage', 'other-snap')


def test_revised_delivery_uses_new_run_and_never_rewrites_old_snapshot(tmp_path):
    db, v1 = seal_fixture(tmp_path, SMALL, run_id='feed-1')
    old = changefeed.materialize(db, 'feed-1', 'max_coverage', 'old-snap')
    old_hash = old['evidence_hash']

    # A revised delivery ships changed row content under its own sealed version.
    revised = tmp_path / 'revised'
    revised.mkdir()
    transactions = (SMALL / 'transactions.jsonl').read_text(encoding='utf-8')
    revised_rows = []
    for line in (SMALL / 'rows.jsonl').read_text(encoding='utf-8').splitlines():
        item = json.loads(line)
        if item['tx_id'] == 't7' and item['entity_key'] == 'x':
            item['value'] = 777
        revised_rows.append(json.dumps(item, ensure_ascii=False))
    (revised / 'transactions.jsonl').write_text(transactions, encoding='utf-8')
    (revised / 'rows.jsonl').write_text('\n'.join(revised_rows), encoding='utf-8')
    (revised / 'spec.json').write_text(
        (SMALL / 'spec.json').read_text(encoding='utf-8'), encoding='utf-8')
    db2, v2 = seal_fixture(tmp_path, revised, run_id='feed-2',
                           delivery='d2', slot='second')
    assert v1 != v2
    new = changefeed.materialize(db2, 'feed-2', 'max_coverage', 'new-snap')
    assert new['entities']['x']['value'] == 777
    # The old snapshot remains sealed and still answers the old values.
    assert changefeed.query(db, 'old-snap', 'x')['x']['value'] == 7
    assert changefeed.verify(db, 'old-snap')['evidence_hash'] == old_hash


def test_verify_rejects_tampered_selection_rows_and_values(tmp_path):
    import hashlib
    from chunkproof.engine import canonical

    db, _ = seal_fixture(tmp_path, SMALL)
    changefeed.materialize(db, 'feed-1', 'max_coverage', 'snap')
    ledger = connect(db)
    record = ledger.execute('SELECT evidence FROM snapshot WHERE id=?', ('snap',)).fetchone()
    sealed = json.loads(record['evidence'].decode('utf-8'))
    ledger.close()

    def reseal(document, expect_match):
        payload = canonical(document)
        # A self-consistent hash column defeats the outer integrity gate so the
        # semantic replay checks underneath must catch the forgery.
        ledger = connect(db)
        ledger.execute('UPDATE snapshot SET evidence=?, evidence_hash=? WHERE id=?',
                       (payload, hashlib.sha256(payload).hexdigest(), 'snap'))
        ledger.commit(); ledger.close()
        with pytest.raises(ValueError, match=expect_match):
            changefeed.verify(db, 'snap')

    # Tamper 1: forge the selected set with an extra conflicting transaction.
    forged = json.loads(json.dumps(sealed))
    forged['selection']['tx_ids'] = sorted(set(forged['selection']['tx_ids']) | {'t6'})
    reseal(forged, 'selection_not_optimal|concurrent_conflict_in_snapshot')

    # Tamper 2: change a final value without replaying the frozen rows.
    altered = json.loads(json.dumps(sealed))
    altered['entities']['x']['value'] = 999
    reseal(altered, 'snapshot_values_mismatch')

    # Tamper 3: drop a frozen row from the sealed assembled JSONL bytes that
    # verify actually replays — the digest map must then disagree.
    ledger = connect(db)
    ledger.execute('UPDATE snapshot SET evidence=?, evidence_hash=? WHERE id=?',
                   (canonical(sealed), hashlib.sha256(canonical(sealed)).hexdigest(), 'snap'))
    row_file = ledger.execute('''SELECT a.path FROM assembled_file a
        JOIN file_entry f ON f.version=a.version AND f.id=a.file_id
        WHERE f.path='rows.jsonl' ''').fetchone()['path']
    ledger.commit(); ledger.close()
    original = Path(row_file).read_bytes()
    kept = b'\n'.join(line for line in original.splitlines()
                      if b'"t7"' not in line)
    Path(row_file).write_bytes(kept)
    try:
        with pytest.raises(ValueError, match='frozen_evidence_mismatch|changefeed_bytes_changed'):
            changefeed.verify(db, 'snap')
    finally:
        Path(row_file).write_bytes(original)

    # Tamper 4: corrupt the stored hash column; the outer integrity gate fires.
    ledger = connect(db)
    ledger.execute('UPDATE snapshot SET evidence_hash=? WHERE id=?', ('0' * 64, 'snap'))
    ledger.commit(); ledger.close()
    with pytest.raises(ValueError, match='snapshot_evidence_tampered'):
        changefeed.verify(db, 'snap')


def test_same_tx_id_different_content_across_runs_is_distinct_evidence(tmp_path):
    db, v1 = seal_fixture(tmp_path, SMALL, run_id='feed-1')
    other_dir = tmp_path / 'other'
    other_dir.mkdir()
    transactions = [json.loads(l) for l in
                    (SMALL / 'transactions.jsonl').read_text().splitlines() if l.strip()]
    for item in transactions:
        if item['tx_id'] == 't1':
            item['authority'] = 9  # same tx_id, different body
    rows = [json.loads(l) for l in
            (SMALL / 'rows.jsonl').read_text().splitlines() if l.strip()]
    (other_dir / 'transactions.jsonl').write_text(
        '\n'.join(json.dumps(t) for t in transactions), encoding='utf-8')
    (other_dir / 'rows.jsonl').write_text(
        '\n'.join(json.dumps(r) for r in rows), encoding='utf-8')
    (other_dir / 'spec.json').write_text(
        json.dumps({'transactions': 'transactions.jsonl', 'rows': 'rows.jsonl'}),
        encoding='utf-8')
    db2, v2 = seal_fixture(tmp_path, other_dir, run_id='feed-2', delivery='d2', slot='second')
    assert v1 != v2
    first = changefeed.materialize(db, 'feed-1', 'max_coverage', 's1')
    second = changefeed.materialize(db2, 'feed-2', 'max_coverage', 's2')
    assert first['frozen']['spec_hash'] != second['frozen']['spec_hash']
    assert changefeed.verify(db, 's1')['status'] == 'verified'
    assert changefeed.verify(db2, 's2')['status'] == 'verified'


def test_cli_materialize_verify_and_query(tmp_path):
    db, _ = seal_fixture(tmp_path, SMALL)
    env = dict(**__import__('os').environ, PYTHONPATH=str(ROOT))
    def run(*args):
        proc = subprocess.run([sys.executable, '-B', '-m', 'chunkproof', *args],
                              cwd=tmp_path, capture_output=True, text=True, env=env, check=True)
        return proc.stdout
    out = run('changefeed-materialize', '--db', str(db), '--run', 'feed-1',
              '--policy', 'max_coverage', '--snapshot', 'cli-snap')
    assert json.loads(out)['selection']['tx_ids'] == ['t1', 't3', 't4', 't7', 't8']
    verdict = json.loads(run('changefeed-verify', '--db', str(db), '--snapshot', 'cli-snap'))
    assert verdict['status'] == 'verified'
    queried = json.loads(run('changefeed-query', '--db', str(db),
                             '--snapshot', 'cli-snap', '--key', 'x'))
    assert queried['x']['value'] == 7


def test_byte_sealing_and_receipt_evidence_remain_intact(tmp_path):
    # The changefeed work must not regress manifest/assemble/seal behaviour.
    db, version = seal_fixture(tmp_path, SMALL)
    ledger = connect(db)
    decision = ledger.execute('SELECT result FROM decision WHERE version=?',
                              (version,)).fetchone()['result']
    assert decision == '可发布'
    assembled = ledger.execute('SELECT hash,path FROM assembled_file').fetchall()
    assert len(assembled) == 3
    for entry in assembled:
        assert __import__('hashlib').sha256(
            Path(entry['path']).read_bytes()).hexdigest() == entry['hash']
    ledger.close()
    assert changefeed.inspect(db, 'feed-1') == {
        'transactions': 7, 'rows': 8, 'incomplete': ['t5']}
