import itertools
import json
import random
from pathlib import Path

import pytest

from chunkproof import engine, entities
from chunkproof.store import connect


FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures'


def seal_tree(db, root, tmp_path, delivery='entity-delivery', block=16):
    """Run the full chunk receipt -> assemble -> seal pipeline for a source tree."""
    version = engine.manifest(db, delivery, root, block)
    media = tmp_path / ('media-' + delivery)
    media.mkdir()
    receipts = []
    for index, chunk in enumerate(engine.plan(db, version)):
        raw = (root / chunk['path']).read_bytes()[chunk['start']:chunk['end']]
        part = f'part-{chunk["path"].replace("/", "_")}-{chunk["idx"]}'
        (media / part).write_bytes(raw)
        receipts.append(json.dumps({'file': chunk['path'], 'index': chunk['idx'],
                                    'path': part, 'sha256': chunk['sha256']}))
    receipt_path = tmp_path / f'receipts-{delivery}.jsonl'
    receipt_path.write_text('\n'.join(receipts), encoding='utf-8')
    engine.receive(db, version, 'M1', media, receipt_path)
    engine.assemble(db, version, media, tmp_path / f'out-{delivery}')
    assert engine.seal(db, version, tmp_path / f'out-{delivery}') == '可发布'
    return version


def sealed_feed(tmp_path, root=FIXTURES / 'entity-feed', spec_name='entity-feed-spec.json',
                feed_id='population-1', delivery='entity-delivery'):
    db = tmp_path / 'ledger.db'
    version = seal_tree(db, root, tmp_path, delivery)
    spec = root / spec_name if (root / spec_name).exists() else FIXTURES / spec_name
    entities.register(db, feed_id, version, spec)
    return db, feed_id


def write_feed(root, north, south, links):
    root.mkdir(parents=True, exist_ok=True)
    (root / 'north.jsonl').write_text(
        '\n'.join(json.dumps({'local_id': x, 'cohort': 'north'}) for x in north) + '\n',
        encoding='utf-8')
    (root / 'south.jsonl').write_text(
        '\n'.join(json.dumps({'local_id': x, 'cohort': 'south'}) for x in south) + '\n',
        encoding='utf-8')
    (root / 'links.jsonl').write_text(
        '\n'.join(json.dumps({'left_id': l, 'right_id': r, 'weight': w}) for l, r, w in links)
        + ('\n' if links else ''), encoding='utf-8')
    spec = root / 'spec.json'
    spec.write_text(json.dumps(
        {'sources': [{'name': 'north', 'path': 'north.jsonl'},
                     {'name': 'south', 'path': 'south.jsonl'}],
         'links': 'links.jsonl'}), encoding='utf-8')
    return spec


def custom_feed(tmp_path, north, south, links, name='custom'):
    root = tmp_path / name
    spec = write_feed(root, north, south, links)
    db = tmp_path / f'{name}.db'
    version = seal_tree(db, root, tmp_path, delivery=name)
    entities.register(db, name, version, spec)
    return db, name


def brute_force_extrema(edges, left_nodes, right_nodes):
    """Exhaustive reference: max total weight and min/max cardinality among optima."""
    adj = {l: [] for l in left_nodes}
    for l, r, w in edges:
        adj[l].append((r, w))
    best_weight = -1
    cardinalities = []

    def recurse(i, used_right, weight, cards):
        nonlocal best_weight, cardinalities
        if i == len(left_nodes):
            if weight > best_weight:
                best_weight = weight
                cardinalities = [cards]
            elif weight == best_weight:
                cardinalities.append(cards)
            return
        recurse(i + 1, used_right, weight, cards)
        for r, w in adj[left_nodes[i]]:
            if r not in used_right:
                recurse(i + 1, used_right | {r}, weight + w, cards + 1)

    recurse(0, set(), 0, 0)
    return best_weight, min(cardinalities), max(cardinalities)


# --------------------------------------------------------------------------
# Existing registration / inspection behaviour
# --------------------------------------------------------------------------

def test_entity_feed_indexes_sealed_rows_and_links(tmp_path):
    root = FIXTURES / 'entity-feed'
    db = tmp_path / 'ledger.db'
    version = seal_tree(db, root, tmp_path)
    spec = FIXTURES / 'entity-feed-spec.json'
    digest = entities.register(db, 'population-1', version, spec)
    assert digest == entities.register(db, 'population-1', version, spec)
    assert entities.inspect(db, 'population-1') == {
        'source_rows': {'north': 2, 'south': 2}, 'raw_rows': 4, 'candidate_links': 3}
    with pytest.raises(ValueError, match='feed_identity_conflict'):
        entities.register(db, 'population-1', 'other-version', spec)


def test_entity_feed_rejects_changed_assembled_bytes(tmp_path):
    root = FIXTURES / 'entity-feed'
    db = tmp_path / 'ledger.db'
    version = seal_tree(db, root, tmp_path)
    (tmp_path / 'out-entity-delivery' / 'north.jsonl').write_text('tampered', encoding='utf-8')
    with pytest.raises(ValueError, match='feed_bytes_changed'):
        entities.register(db, 'population-1', version, FIXTURES / 'entity-feed-spec.json')


# --------------------------------------------------------------------------
# Group 1: global competition and the two contracts on the same evidence
# --------------------------------------------------------------------------

def test_operational_contract_picks_deterministic_maximum_matching(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'operational', 2, 'op-1')
    assert result['max_total_weight'] == 2
    assert result['match_count_min'] == 1
    assert result['match_count_max'] == 2
    matching = result['operational_matching']
    # Greedy on canonical edge order rejects N1-S1 (forces only one pair) and
    # settles on the two-pair optimum, never on a per-edge local maximum.
    assert matching == [('N1', 'S2', 1), ('N2', 'S1', 1)]
    assert result['dedup_estimate'] == 2
    assert result['conclusion'] == 'pass'
    assert entities.verify(db, 'op-1')['ok'] is True
    failing = entities.assess(db, feed_id, 'operational', 1, 'op-2')
    assert failing['conclusion'] == 'fail'
    # Same id + same payload is idempotent; a changed threshold conflicts.
    assert entities.assess(db, feed_id, 'operational', 2, 'op-1')['body_hash'] == result['body_hash']
    with pytest.raises(ValueError, match='assessment_conflict'):
        entities.assess(db, feed_id, 'operational', 1, 'op-1')


def test_certified_contract_proves_bounds_without_picking_one_matching(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'certified', 2, 'cert-1')
    assert result['max_total_weight'] == 2
    assert result['match_count_min'] == 1
    assert result['match_count_max'] == 2
    assert (result['dedup_min'], result['dedup_max']) == (2, 3)
    assert result['conclusion'] == 'indeterminate'
    assert result['operational_matching'] is None
    # Threshold semantics on the certified envelope.
    assert entities.assess(db, feed_id, 'certified', 1, 'cert-fail')['conclusion'] == 'fail'
    assert entities.assess(db, feed_id, 'certified', 3, 'cert-pass')['conclusion'] == 'pass'
    verified = entities.verify(db, 'cert-1')
    assert verified['ok'] is True and verified['conclusion'] == 'indeterminate'


def test_both_contracts_bind_to_the_same_frozen_evidence(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    op = entities.assess(db, feed_id, 'operational', 2, 'same-op')
    cert = entities.assess(db, feed_id, 'certified', 2, 'same-cert')
    assert op['feed_id'] == cert['feed_id'] == feed_id
    assert op['version'] == cert['version']
    assert op['spec_hash'] == cert['spec_hash']
    assert op['row_digest'] == cert['row_digest']
    assert op['link_digest'] == cert['link_digest']
    assert op['raw_rows'] == cert['raw_rows'] == 4
    with pytest.raises(ValueError, match='bad_policy'):
        entities.assess(db, feed_id, 'informal', 2, 'x')
    with pytest.raises(ValueError, match='bad_threshold'):
        entities.assess(db, feed_id, 'certified', -1, 'y')
    with pytest.raises(ValueError, match='unknown_feed'):
        entities.assess(db, 'missing-feed', 'certified', 2, 'z')


# --------------------------------------------------------------------------
# Group 2: extremal bounds and replayable witnesses
# --------------------------------------------------------------------------

def test_witnesses_replay_to_legal_optimal_matchings(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'certified', 2, 'w-1')
    edges = {('N1', 'S1'): 2, ('N1', 'S2'): 1, ('N2', 'S1'): 1}
    minimum, maximum = result['witness_min'], result['witness_max']
    assert len(minimum) == 1 and minimum == [('N1', 'S1', 2)]
    assert len(maximum) == 2
    for witness, expected_cards in ((minimum, 1), (maximum, 2)):
        lefts = [e[0] for e in witness]
        rights = [e[1] for e in witness]
        assert len(lefts) == len(set(lefts)) and len(rights) == len(set(rights))
        assert sum(edges[(l, r)] for l, r, _ in witness) == 2
        assert len(witness) == expected_cards
    # Independent brute-force enumeration agrees with the certified envelope.
    weight, kmin, kmax = brute_force_extrema(
        [('N1', 'S1', 2), ('N1', 'S2', 1), ('N2', 'S1', 1)], ['N1', 'N2'], ['S1', 'S2'])
    assert (weight, kmin, kmax) == (2, 1, 2)
    assert (result['match_count_min'], result['match_count_max']) == (kmin, kmax)


def test_extremal_solver_matches_brute_force_on_random_graphs():
    rng = random.Random(20261120)
    meter = entities._BudgetMeter(entities.DEFAULT_BUDGET)
    for case in range(60):
        n_left = rng.randint(1, 5)
        n_right = rng.randint(1, 5)
        left = [f'L{i}' for i in range(n_left)]
        right = [f'R{i}' for i in range(n_right)]
        edges = []
        for l, r in itertools.product(left, right):
            if rng.random() < 0.45:
                edges.append((l, r, rng.randint(1, 4)))
        weight, kmin, kmax = brute_force_extrema(edges, left, right)
        evidence = entities.solve_component(edges, left, right, set(), meter)
        assert evidence['total_weight'] == weight
        assert evidence['min_cards'] == kmin and evidence['max_cards'] == kmax
        # The deterministic operational pick must be a weight-optimal maximum.
        selected = evidence['selected']
        assert len(selected) == kmax
        assert sum(w for _, _, w in selected) == weight
        # A second call returns byte-identical witnesses (stable replay).
        again = entities.solve_component(edges, left, right, set(),
                                         entities._BudgetMeter(entities.DEFAULT_BUDGET))
        assert again['min_witness'] == evidence['min_witness']
        assert again['max_witness'] == evidence['max_witness']
        assert again['selected'] == selected


# --------------------------------------------------------------------------
# Group 3: no edges, registration conflicts, multiple components, unmatched rows
# --------------------------------------------------------------------------

def test_no_candidate_edges_means_nobody_deduplicates(tmp_path):
    db, feed_id = custom_feed(tmp_path, ['N1', 'N2'], ['S1', 'S2'], [], name='edgefree')
    op = entities.assess(db, feed_id, 'operational', 3, 'ne-op')
    cert = entities.assess(db, feed_id, 'certified', 3, 'ne-cert')
    for result in (op, cert):
        assert result['max_total_weight'] == 0
        assert result['match_count_min'] == result['match_count_max'] == 0
        assert result['dedup_min'] == result['dedup_max'] == 4
        assert result['witness_min'] == [] and result['witness_max'] == []
    assert op['operational_matching'] == []
    assert op['conclusion'] == 'fail'
    assert cert['conclusion'] == 'fail'
    assert entities.assess(db, feed_id, 'certified', 4, 'ne-pass')['conclusion'] == 'pass'
    assert entities.verify(db, 'ne-op')['ok'] is True
    assert entities.verify(db, 'ne-cert')['ok'] is True


@pytest.mark.parametrize('links,code', [
    ([('N1', 'S1', 0)], 'bad_link_weight'),
    ([('N1', 'S1', -2)], 'bad_link_weight'),
    ([('N1', 'S1', True)], 'bad_link_weight'),
    ([('N1', 'S1', 1), ('N1', 'S1', 2)], 'duplicate_link'),
    ([('N1', 'SX', 1)], 'unknown_link_endpoint'),
    ([('NX', 'S1', 1)], 'unknown_link_endpoint'),
])
def test_registration_rejects_bad_link_inventory(tmp_path, links, code):
    root = tmp_path / 'badlinks'
    spec = write_feed(root, ['N1'], ['S1'], links)
    db = tmp_path / 'badlinks.db'
    version = seal_tree(db, root, tmp_path, delivery='badlinks')
    with pytest.raises(ValueError, match=code):
        entities.register(db, 'bad', version, spec)


def test_duplicate_local_id_is_rejected_at_registration(tmp_path):
    root = tmp_path / 'dup'
    root.mkdir()
    (root / 'north.jsonl').write_text('\n'.join(json.dumps(x) for x in (
        {'local_id': 'N1', 'cohort': 'north'}, {'local_id': 'N1', 'cohort': 'north'})) + '\n',
        encoding='utf-8')
    (root / 'south.jsonl').write_text(json.dumps({'local_id': 'S1', 'cohort': 'south'}) + '\n',
                                      encoding='utf-8')
    (root / 'links.jsonl').write_text('', encoding='utf-8')
    spec = root / 'spec.json'
    spec.write_text(json.dumps({'sources': [
        {'name': 'north', 'path': 'north.jsonl'},
        {'name': 'south', 'path': 'south.jsonl'}], 'links': 'links.jsonl'}), encoding='utf-8')
    db = tmp_path / 'dup.db'
    version = seal_tree(db, root, tmp_path, delivery='dup')
    with pytest.raises(ValueError, match='duplicate_local_id'):
        entities.register(db, 'dup-feed', version, spec)


def test_multiple_disconnected_components_compose_global_bounds(tmp_path):
    db = tmp_path / 'ledger.db'
    root = FIXTURES / 'entity-feed-multi'
    version = seal_tree(db, root, tmp_path, delivery='multi-delivery')
    entities.register(db, 'multi-feed', version, FIXTURES / 'entity-feed-multi-spec.json')
    result = entities.assess(db, 'multi-feed', 'certified', 6, 'multi-cert')
    # Component A mirrors the canonical ambiguity (W=2, cards 1..2);
    # component B is a single forced edge (W=1, one pair); N4/S4/S5 never match.
    assert result['raw_rows'] == 9
    assert result['max_total_weight'] == 3
    assert (result['match_count_min'], result['match_count_max']) == (2, 3)
    assert (result['dedup_min'], result['dedup_max']) == (6, 7)
    # Threshold 6 lies inside the envelope: indeterminate, not pass.
    assert result['conclusion'] == 'indeterminate'
    assert entities.assess(db, 'multi-feed', 'certified', 7, 'multi-pass')['conclusion'] == 'pass'
    assert entities.assess(db, 'multi-feed', 'certified', 5, 'multi-fail')['conclusion'] == 'fail'
    op = entities.assess(db, 'multi-feed', 'operational', 6, 'multi-op')
    assert op['dedup_estimate'] == 6 and op['conclusion'] == 'pass'
    assert len(op['operational_matching']) == 3
    assert entities.verify(db, 'multi-cert')['ok'] is True
    assert entities.verify(db, 'multi-op')['ok'] is True
    # Component-level evidence is frozen alongside the global result.
    comps = result['components']
    assert [(c['match_count_min'], c['match_count_max']) for c in comps] == [(1, 2), (1, 1)]
    # Brute force over the whole graph confirms the composition did not lose
    # the global cardinality envelope.
    edges = [('N1', 'S1', 2), ('N1', 'S2', 1), ('N2', 'S1', 1), ('N3', 'S3', 1)]
    weight, kmin, kmax = brute_force_extrema(
        edges, ['N1', 'N2', 'N3', 'N4'], ['S1', 'S2', 'S3', 'S4', 'S5'])
    assert (weight, kmin, kmax) == (3, 2, 3)


def test_unmatched_rows_are_counted_as_distinct_entities(tmp_path):
    db, feed_id = custom_feed(
        tmp_path, ['N1', 'N2', 'N3'], ['S1', 'S2', 'S3'], [('N1', 'S1', 5)], name='unmatched')
    result = entities.assess(db, feed_id, 'certified', 5, 'u-cert')
    assert result['match_count_min'] == result['match_count_max'] == 1
    assert result['dedup_min'] == result['dedup_max'] == 5
    assert result['conclusion'] == 'pass'


# --------------------------------------------------------------------------
# Group 4: versions, idempotency and tamper verification
# --------------------------------------------------------------------------

def _rehash_assessment(db_path, assessment_id, new_body):
    ledger = connect(db_path)
    raw = entities.canonical(new_body)
    digest = __import__('hashlib').sha256(raw).hexdigest()
    with ledger:
        ledger.execute('UPDATE entity_assessment SET body_hash=?, body=? WHERE id=?',
                       (digest, raw, assessment_id))
    ledger.close()


def test_assessment_idempotent_same_payload_conflicts_on_change(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    first = entities.assess(db, feed_id, 'certified', 2, 'idem')
    second = entities.assess(db, feed_id, 'certified', 2, 'idem')
    assert first['body_hash'] == second['body_hash']
    with pytest.raises(ValueError, match='assessment_conflict'):
        entities.assess(db, feed_id, 'certified', 3, 'idem')
    with pytest.raises(ValueError, match='assessment_conflict'):
        entities.assess(db, feed_id, 'operational', 2, 'idem')


def test_new_delivery_version_gets_new_feed_and_old_assessment_stays_replayable(tmp_path):
    root = tmp_path / 'vfeed'
    spec = write_feed(root, ['N1', 'N2'], ['S1', 'S2'],
                      [('N1', 'S1', 2), ('N1', 'S2', 1), ('N2', 'S1', 1)])
    db = tmp_path / 'versions.db'
    v1 = seal_tree(db, root, tmp_path, delivery='v1')
    entities.register(db, 'feed-v1', v1, spec)
    old = entities.assess(db, 'feed-v1', 'certified', 2, 'old-cert')
    # A new delivery wave changes the bytes; manifest hashes diverge.
    (root / 'north.jsonl').write_text(
        json.dumps({'local_id': 'N1', 'cohort': 'north'}) + '\n'
        + json.dumps({'local_id': 'N2', 'cohort': 'north'}) + '\n'
        + json.dumps({'local_id': 'N3', 'cohort': 'north'}) + '\n', encoding='utf-8')
    v2 = seal_tree(db, root, tmp_path, delivery='v2')
    assert v1 != v2
    # Old feed id cannot silently follow the new version.
    with pytest.raises(ValueError, match='feed_identity_conflict'):
        entities.register(db, 'feed-v1', v2, spec)
    entities.register(db, 'feed-v2', v2, spec)
    new = entities.assess(db, 'feed-v2', 'certified', 2, 'new-cert')
    assert new['version'] == v2 and old['version'] == v1
    # The historic assessment still verifies against the frozen old records.
    assert entities.verify(db, 'old-cert')['ok'] is True
    assert entities.verify(db, 'new-cert')['ok'] is True


def test_verify_rejects_tampered_edge_weights(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    entities.assess(db, feed_id, 'certified', 2, 'tamper-edge')
    ledger = connect(db)
    with ledger:
        ledger.execute("UPDATE entity_link SET weight=9 WHERE feed_id=? AND left_id='N1'", (feed_id,))
    ledger.close()
    report = entities.verify(db, 'tamper-edge')
    assert report['ok'] is False
    assert any(not c['ok'] and c['check'] == 'link_digest' for c in report['checks'])


def test_verify_rejects_forged_bounds_missing_cooptimal_matching(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'certified', 2, 'forged')
    forged = dict(result)
    forged['match_count_min'] = forged['match_count_max']
    forged['dedup_max'] = forged['dedup_min']
    forged['witness_min'] = forged['witness_max']
    _rehash_assessment(db, 'forged', forged)
    report = entities.verify(db, 'forged')
    assert report['ok'] is False
    assert any(not c['ok'] and c['check'] == 'match_count_bounds' for c in report['checks'])


def test_verify_rejects_suboptimal_witness(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'certified', 2, 'bad-witness')
    forged = dict(result)
    forged['witness_max'] = [('N1', 'S1', 2)]  # legal but only one pair, not the max witness
    _rehash_assessment(db, 'bad-witness', forged)
    report = entities.verify(db, 'bad-witness')
    assert report['ok'] is False
    assert any(not c['ok'] and c['check'] == 'witness_max_optimal' for c in report['checks'])


def test_verify_rejects_body_tampering_and_feed_swap(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'certified', 2, 'body-tamper')
    forged = dict(result)
    forged['threshold'] = 99
    # Deliberately do not rehash: the frozen body_hash must expose the edit.
    ledger = connect(db)
    with ledger:
        ledger.execute('UPDATE entity_assessment SET body=? WHERE id=?',
                       (entities.canonical(forged), 'body-tamper'))
    ledger.close()
    assert entities.verify(db, 'body-tamper')['ok'] is False

    # A second feed exists; rebinding the assessment to it cannot pass.
    other_root = tmp_path / 'other'
    other_spec = write_feed(other_root, ['A'], ['B'], [('A', 'B', 1)])
    other_db = db
    other_version = seal_tree(other_db, other_root, tmp_path, delivery='other-delivery')
    entities.register(other_db, 'other-feed', other_version, other_spec)
    swapped = dict(result)
    swapped['feed_id'] = 'other-feed'
    _rehash_assessment(db, 'body-tamper', swapped)
    report = entities.verify(db, 'body-tamper')
    assert report['ok'] is False
    assert any(not c['ok'] for c in report['checks'])


def test_verify_unknown_assessment(tmp_path):
    db, _ = sealed_feed(tmp_path)
    with pytest.raises(ValueError, match='unknown_assessment'):
        entities.verify(db, 'never-written')


# --------------------------------------------------------------------------
# Group 5: scale, resource budget, non-regression
# --------------------------------------------------------------------------

def test_sparse_20_plus_20_graph_solves_within_offline_budget(tmp_path):
    rng = random.Random(4242)
    north = [f'N{i:02d}' for i in range(20)]
    south = [f'S{i:02d}' for i in range(20)]
    links = []
    # Sparse candidate graph: about two candidates per row, plus an ambiguous
    # high-weight component deliberately seeded at the front.
    links += [('N00', 'S00', 5), ('N00', 'S01', 3), ('N01', 'S00', 3), ('N01', 'S01', 3)]
    for n in north[2:]:
        i = int(n[1:])
        choices = rng.sample(south, 2)
        links.append((n, choices[0], rng.randint(1, 6)))
        links.append((n, choices[1], rng.randint(1, 6)))
    db, feed_id = custom_feed(tmp_path, north, south, links, name='scale20')
    result = entities.assess(db, feed_id, 'certified', 20, 'scale-cert')
    assert result['exhausted'] is False
    assert result['nodes_visited'] <= entities.DEFAULT_BUDGET
    assert result['raw_rows'] == 40
    assert result['match_count_min'] <= result['match_count_max']
    assert result['dedup_min'] == 40 - result['match_count_max']
    assert result['dedup_max'] == 40 - result['match_count_min']
    # Every claimed witness is independently replayable.
    edges = {(l, r): w for l, r, w in links}
    for witness in (result['witness_min'], result['witness_max']):
        lefts = [e[0] for e in witness]
        rights = [e[1] for e in witness]
        assert len(lefts) == len(set(lefts)) == len(set(rights))
        assert sum(edges[(l, r)] for l, r, _ in witness) == result['max_total_weight']
    assert entities.verify(db, 'scale-cert')['ok'] is True
    op = entities.assess(db, feed_id, 'operational', 20, 'scale-op')
    assert entities.verify(db, 'scale-op')['ok'] is True
    assert op['dedup_estimate'] == 40 - result['match_count_max']


def test_budget_exhaustion_returns_indeterminate_and_trivial_envelope(tmp_path):
    db, feed_id = sealed_feed(tmp_path)
    result = entities.assess(db, feed_id, 'certified', 2, 'poor', budget=1)
    assert result['exhausted'] is True
    assert result['conclusion'] == 'indeterminate'
    assert result['max_total_weight'] is None
    assert result['match_count_min'] is None and result['match_count_max'] is None
    assert result['dedup_min'] == 0 and result['dedup_max'] == result['raw_rows'] == 4
    # The exhausted ledger entry itself is internally consistent and replayable.
    report = entities.verify(db, 'poor')
    assert report['ok'] is True and report['exhausted'] is True
    # A verifier running out of budget must refuse to certify, never approve.
    entities.assess(db, feed_id, 'certified', 2, 'rich')
    report = entities.verify(db, 'rich', budget=1)
    assert report['ok'] is False and report['exhausted'] is True
    assert any(c['check'] == 'verification_budget' and not c['ok'] for c in report['checks'])
