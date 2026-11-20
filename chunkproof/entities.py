"""Index sealed, pseudonymous entity feeds and assess cross-source identity alignment.

Registration freezes rows and candidate links from sealed delivery bytes only.
Assessment solves the global one-to-one maximum-weight matching on the frozen
candidate graph.  The operational contract picks one deterministic maximum
matching; the certified contract proves the minimum/maximum number of matched
pairs over *all* maximum-total-weight matchings, with replayable witnesses for
both extremes.
"""
import hashlib
import json
from pathlib import Path

from .engine import canonical
from .store import connect

DEFAULT_BUDGET = 1_000_000


class _Budget(Exception):
    """Raised when the exhaustive search can no longer prove optimality."""


class _BudgetMeter:
    def __init__(self, limit):
        self.limit = limit
        self.used = 0

    def tick(self):
        self.used += 1
        if self.used > self.limit:
            raise _Budget()


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_feed_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('feed_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, feed_id, version, spec_path):
    """Freeze feed membership, row identities and candidate evidence from sealed files."""
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not feed_id or not isinstance(spec.get('sources'), list) or len(spec['sources']) != 2:
        raise ValueError('bad_feed_spec')
    left, right = (part['name'] for part in spec['sources'])
    if left == right:
        raise ValueError('duplicate_source')
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM entity_feed WHERE id=?', (feed_id,)).fetchone()
            digest = hashlib.sha256(raw).hexdigest()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('feed_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('feed_not_sealed')
            rows = []
            for part in spec['sources']:
                for item in _sealed_lines(db, version, part['path']):
                    rows.append((feed_id, part['name'], item['local_id'], item['cohort']))
            identities = {(source, local_id) for _, source, local_id, _ in rows}
            if len(identities) != len(rows):
                raise ValueError('duplicate_local_id')
            edges = []
            for item in _sealed_lines(db, version, spec['links']):
                if (left, item['left_id']) not in identities or (right, item['right_id']) not in identities:
                    raise ValueError('unknown_link_endpoint')
                weight = item['weight']
                if type(weight) is not int or weight <= 0:
                    raise ValueError('bad_link_weight')
                edges.append((feed_id, item['left_id'], item['right_id'], weight))
            if len({(e[1], e[2]) for e in edges}) != len(edges):
                raise ValueError('duplicate_link')
            db.execute('INSERT INTO entity_feed VALUES (?,?,?,?)', (feed_id, version, digest, raw))
            db.executemany('INSERT INTO entity_row VALUES (?,?,?,?)', rows)
            db.executemany('INSERT INTO entity_link VALUES (?,?,?,?)', edges)
            return digest
    finally:
        db.close()


def inspect(db_path, feed_id):
    """Report raw source volume and evidence inventory; no match is chosen here."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM entity_feed WHERE id=?', (feed_id,)).fetchone():
            raise ValueError('unknown_feed')
        by_source = {row['source']: row['n'] for row in db.execute(
            'SELECT source,count(*) n FROM entity_row WHERE feed_id=? GROUP BY source ORDER BY source', (feed_id,))}
        links = db.execute('SELECT count(*) n FROM entity_link WHERE feed_id=?', (feed_id,)).fetchone()['n']
        return {'source_rows': by_source, 'raw_rows': sum(by_source.values()), 'candidate_links': links}
    finally:
        db.close()


# --------------------------------------------------------------------------
# Global one-to-one matching over the frozen candidate graph
# --------------------------------------------------------------------------

class _Incompatible(Exception):
    """Raised when a required edge set cannot belong to any legal matching."""


def _canonical_edges(links):
    """Normative edge order: (left_id, right_id, weight), sorted lexicographically."""
    return sorted((r['left_id'], r['right_id'], r['weight']) for r in links)


def _split_components(edges, left_nodes, right_nodes):
    """Split the bipartite candidate graph into connected components (union-find)."""
    parent = {node: node for node in left_nodes | right_nodes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for left, right, _ in edges:
        union(left, right)
    groups = {}
    for node in parent:
        groups.setdefault(find(node), set()).add(node)
    components = []
    for members in groups.values():
        l = sorted(members & left_nodes)
        r = sorted(members & right_nodes)
        sub = [e for e in edges if e[0] in members]
        components.append((l, r, sub))
    # Deterministic component order for any replay.
    components.sort(key=lambda comp: (comp[0][0] if comp[0] else comp[1][0]))
    return components


def _best_matching(edges, left_idx, right_idx, maximize_cards, required, meter):
    """Optimal one-to-one matching of one bipartite component.

    All edge weights are transformed by score = weight*(|L|+1)+1, so every
    match adds one cardinality unit; maximizing score first maximizes total
    weight, then (among equal-weight optima) the number of matched pairs when
    maximize_cards is True.  For minimum cardinality among weight-optima the
    score uses -1 per match.  ``required`` is a set of canonical edge tuples
    that every admissible matching must contain.
    """
    n_left, n_right = len(left_idx), len(right_idx)
    base = n_left + 1
    adj = {i: [] for i in range(n_left)}
    required_pairs = set()
    for edge in edges:
        left, right, weight = edge
        score = weight * base + (1 if maximize_cards else -1)
        adj[left_idx[left]].append((right_idx[right], score, edge))
        if edge in required:
            required_pairs.add((left_idx[left], right_idx[right]))
    for pairs in adj.values():
        # Heaviest, then canonical order, first.
        pairs.sort(key=lambda item: (-item[1], item[2][0], item[2][1]))

    used_right = [False] * n_right
    chosen = []
    for li, ri in required_pairs:
        if used_right[ri] or any(li == other for other, _ in chosen):
            raise _Incompatible()
        if not any(ri == candidate for candidate, _, _ in adj[li]):
            raise _Incompatible()
        used_right[ri] = True
        chosen.append((li, ri))
    free_left = [i for i in range(n_left) if not any(i == li for li, _ in chosen)]
    best = {'score': sum(score for li, ri in chosen
                         for candidate, score, _ in adj[li] if candidate == ri),
            'pairs': sorted(chosen)}

    def upper_bound(position, used):
        total = 0
        seen_rights = set()
        for i in range(position, len(free_left)):
            options = [s for ri, s, _ in adj[free_left[i]] if not used[ri] and ri not in seen_rights]
            if not options:
                continue
            total += options[0]
        return total

    def dfs(position, current, used):
        meter.tick()
        if current > best['score']:
            best['score'] = current
            best['pairs'] = sorted(chosen)
        if position == len(free_left):
            return
        if current + upper_bound(position, used) <= best['score']:
            return
        li = free_left[position]
        dfs(position + 1, current, used)  # leave this row unmatched
        for ri, score, _ in adj[li]:
            if not used[ri]:
                used[ri] = True
                chosen.append((li, ri))
                dfs(position + 1, current + score, used)
                chosen.pop()
                used[ri] = False

    dfs(0, best['score'], used_right)
    index_to_edge = {(li, ri): edge for edge in edges
                     for li, ri in ((left_idx[edge[0]], right_idx[edge[1]]),)}
    return best['score'], [index_to_edge[pair] for pair in best['pairs']]


def solve_component(edges, left_nodes, right_nodes, required, meter):
    """All extremal witnesses for one candidate-graph component."""
    left_idx = {node: i for i, node in enumerate(left_nodes)}
    right_idx = {node: i for i, node in enumerate(right_nodes)}
    max_score, max_witness = _best_matching(
        edges, left_idx, right_idx, True, required, meter)
    if required:
        # Required edges must themselves belong to the weight-optimum.
        opt_score, _ = _best_matching(edges, left_idx, right_idx, True, set(), meter)
        if opt_score != max_score:
            raise _Incompatible()
    base = len(left_nodes) + 1
    total_weight = max_score // base
    _, min_witness = _best_matching(
        edges, left_idx, right_idx, False, required, meter)
    # Deterministic operational pick: greedily keep canonical edges while an
    # optimal matching containing every kept edge still exists.
    selected = set()
    for edge in edges:
        try:
            score, witness = _best_matching(
                edges, left_idx, right_idx, True, selected | {edge}, meter)
        except _Incompatible:
            continue
        if score == max_score and len(witness) == len(max_witness):
            selected.add(edge)
    return {
        'total_weight': total_weight,
        'max_cards': len(max_witness),
        'min_cards': len(min_witness),
        'max_witness': sorted(max_witness),
        'min_witness': sorted(min_witness),
        'selected': sorted(selected),
    }


def _load_frozen_feed(db_path, feed_id):
    db = connect(db_path)
    try:
        feed = db.execute('SELECT * FROM entity_feed WHERE id=?', (feed_id,)).fetchone()
        if not feed:
            raise ValueError('unknown_feed')
        rows = [dict(r) for r in db.execute(
            'SELECT source,local_id,cohort FROM entity_row WHERE feed_id=? ORDER BY source,local_id', (feed_id,))]
        links = [dict(r) for r in db.execute(
            'SELECT left_id,right_id,weight FROM entity_link WHERE feed_id=? ORDER BY left_id,right_id', (feed_id,))]
        spec = json.loads(feed['spec'])
        return feed, spec, rows, links
    finally:
        db.close()


def assess(db_path, feed_id, policy, threshold, assessment_id, budget=DEFAULT_BUDGET):
    """Freeze a deterministic (operational) or bound-proving (certified) assessment."""
    if policy not in ('operational', 'certified'):
        raise ValueError('bad_policy')
    if type(threshold) is not int or threshold < 0:
        raise ValueError('bad_threshold')
    if not assessment_id:
        raise ValueError('bad_assessment_id')
    feed, spec, rows, links = _load_frozen_feed(db_path, feed_id)
    source_names = [part['name'] for part in spec['sources']]
    left_source, right_source = source_names
    left_nodes = sorted(r['local_id'] for r in rows if r['source'] == left_source)
    right_nodes = sorted(r['local_id'] for r in rows if r['source'] == right_source)
    edges = _canonical_edges(links)
    raw_rows = len(rows)
    row_digest = hashlib.sha256(canonical(rows)).hexdigest()
    link_digest = hashlib.sha256(canonical(edges)).hexdigest()

    meter = _BudgetMeter(budget)
    components = _split_components(edges, set(left_nodes), set(right_nodes))
    # Isolated rows (no candidate edge) cannot match; only edge-bearing
    # components participate in the optimization and the frozen evidence list.
    components = [comp for comp in components if comp[2]]
    exhausted = False
    comp_evidence = []
    try:
        for left, right, sub_edges in components:
            comp_evidence.append(solve_component(sub_edges, left, right, set(), meter))
    except _Budget:
        exhausted = True

    body = {
        'assessment_id': assessment_id,
        'feed_id': feed_id,
        'version': feed['version'],
        'spec_hash': feed['spec_hash'],
        'policy': policy,
        'threshold': threshold,
        'budget_limit': budget,
        'nodes_visited': meter.used,
        'row_digest': row_digest,
        'link_digest': link_digest,
        'raw_rows': raw_rows,
        'candidate_links': len(edges),
    }
    if exhausted:
        # Only provable bounds survive: nothing certified above the trivial
        # envelope; a feasible partial match must never be sold as optimal.
        body.update({
            'exhausted': True,
            'max_total_weight': None,
            'match_count_min': None,
            'match_count_max': None,
            'dedup_min': 0,
            'dedup_max': raw_rows,
            'witness_min': None,
            'witness_max': None,
            'operational_matching': None,
            'conclusion': 'indeterminate',
        })
    else:
        total_weight = sum(c['total_weight'] for c in comp_evidence)
        cards_min = sum(c['min_cards'] for c in comp_evidence)
        cards_max = sum(c['max_cards'] for c in comp_evidence)
        witness_min = sorted(e for c in comp_evidence for e in c['min_witness'])
        witness_max = sorted(e for c in comp_evidence for e in c['max_witness'])
        selected = sorted(e for c in comp_evidence for e in c['selected'])
        body.update({
            'exhausted': False,
            'max_total_weight': total_weight,
            'match_count_min': cards_min,
            'match_count_max': cards_max,
            'dedup_min': raw_rows - cards_max,
            'dedup_max': raw_rows - cards_min,
            'witness_min': witness_min,
            'witness_max': witness_max,
            'components': [
                {'left_nodes': left, 'right_nodes': right,
                 'total_weight': evidence['total_weight'],
                 'match_count_min': evidence['min_cards'],
                 'match_count_max': evidence['max_cards'],
                 'witness_min': evidence['min_witness'],
                 'witness_max': evidence['max_witness']}
                for (left, right, _), evidence in zip(components, comp_evidence)],
        })
        if policy == 'operational':
            body['operational_matching'] = selected
            dedup = raw_rows - len(selected)
            body['dedup_estimate'] = dedup
            body['conclusion'] = 'pass' if dedup <= threshold else 'fail'
        else:
            if body['dedup_max'] <= threshold:
                conclusion = 'pass'
            elif body['dedup_min'] > threshold:
                conclusion = 'fail'
            else:
                conclusion = 'indeterminate'
            body['conclusion'] = conclusion
            body['operational_matching'] = None

    raw_body = canonical(body)
    body_hash = hashlib.sha256(raw_body).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT feed_id,body_hash,body FROM entity_assessment WHERE id=?',
                             (assessment_id,)).fetchone()
            if old:
                if old['feed_id'] != feed_id or old['body_hash'] != body_hash:
                    raise ValueError('assessment_conflict')
            else:
                db.execute('INSERT INTO entity_assessment(id,feed_id,body_hash,body) VALUES (?,?,?,?)',
                           (assessment_id, feed_id, body_hash, raw_body))
    finally:
        db.close()
    body['body_hash'] = body_hash
    return body


def _valid_matching(witness, edges, raw_rows):
    """Replay a witness against the frozen edge set and the one-to-one rule."""
    if witness is None:
        return False
    edge_set = set(edges)
    seen_left, seen_right = set(), set()
    for edge in witness:
        if tuple(edge) not in edge_set:
            return False
        if edge[0] in seen_left or edge[1] in seen_right:
            return False
        seen_left.add(edge[0])
        seen_right.add(edge[1])
    return len(witness) <= raw_rows


def verify(db_path, assessment_id, budget=DEFAULT_BUDGET):
    """Recompute legality, optimum score and extremal bounds from frozen rows/links."""
    db = connect(db_path)
    try:
        record = db.execute('SELECT * FROM entity_assessment WHERE id=?', (assessment_id,)).fetchone()
        if not record:
            raise ValueError('unknown_assessment')
        stored = json.loads(record['body'])
        feed = db.execute('SELECT * FROM entity_feed WHERE id=?', (record['feed_id'],)).fetchone()
        if not feed:
            raise ValueError('unknown_feed')
        rows = [dict(r) for r in db.execute(
            'SELECT source,local_id,cohort FROM entity_row WHERE feed_id=? ORDER BY source,local_id',
            (record['feed_id'],))]
        links = [dict(r) for r in db.execute(
            'SELECT left_id,right_id,weight FROM entity_link WHERE feed_id=? ORDER BY left_id,right_id',
            (record['feed_id'],))]
    finally:
        db.close()

    checks = []

    def check(name, ok, detail=''):
        checks.append({'check': name, 'ok': bool(ok), 'detail': detail})

    rehashed = hashlib.sha256(canonical(stored)).hexdigest()
    check('body_hash', rehashed == record['body_hash'])
    check('feed_binding', stored['feed_id'] == record['feed_id']
          and stored['version'] == feed['version'] and stored['spec_hash'] == feed['spec_hash'])
    edges = _canonical_edges(links)
    check('row_digest', stored['row_digest'] == hashlib.sha256(canonical(rows)).hexdigest())
    check('link_digest', stored['link_digest'] == hashlib.sha256(canonical(edges)).hexdigest())
    check('raw_rows', stored['raw_rows'] == len(rows))
    check('policy', stored['policy'] in ('operational', 'certified'))
    check('threshold', type(stored['threshold']) is int and stored['threshold'] >= 0)

    if stored.get('exhausted'):
        # An exhausted ledger entry may only claim the trivial envelope.
        check('exhausted_envelope', stored['dedup_min'] == 0 and stored['dedup_max'] == len(rows)
              and stored['max_total_weight'] is None and stored['conclusion'] == 'indeterminate')
        ok = all(item['ok'] for item in checks)
        return {'assessment_id': assessment_id, 'ok': ok, 'exhausted': True, 'checks': checks}

    spec = json.loads(feed['spec'])
    left_nodes = sorted(r['local_id'] for r in rows if r['source'] == spec['sources'][0]['name'])
    right_nodes = sorted(r['local_id'] for r in rows if r['source'] == spec['sources'][1]['name'])
    meter = _BudgetMeter(budget)
    components = _split_components(edges, set(left_nodes), set(right_nodes))
    components = [comp for comp in components if comp[2]]
    recomputed = []
    try:
        for left, right, sub_edges in components:
            recomputed.append(solve_component(sub_edges, left, right, set(), meter))
    except _Budget:
        check('verification_budget', False, 'verifier_budget_exhausted')
        return {'assessment_id': assessment_id, 'ok': False, 'exhausted': True, 'checks': checks}

    total_weight = sum(c['total_weight'] for c in recomputed)
    cards_min = sum(c['min_cards'] for c in recomputed)
    cards_max = sum(c['max_cards'] for c in recomputed)
    check('max_total_weight', stored['max_total_weight'] == total_weight)
    check('match_count_bounds', stored['match_count_min'] == cards_min
          and stored['match_count_max'] == cards_max)
    dedup_min, dedup_max = len(rows) - cards_max, len(rows) - cards_min
    check('dedup_bounds', stored['dedup_min'] == dedup_min and stored['dedup_max'] == dedup_max)

    check('witness_min_legal', _valid_matching(stored['witness_min'], edges, len(rows)))
    check('witness_max_legal', _valid_matching(stored['witness_max'], edges, len(rows)))
    if _valid_matching(stored['witness_min'], edges, len(rows)):
        w_min = sum(dict(((l, r), w) for l, r, w in edges)[(l, r)]
                    for l, r, _ in stored['witness_min'])
        check('witness_min_optimal', w_min == total_weight
              and len(stored['witness_min']) == cards_min)
    else:
        check('witness_min_optimal', False)
    if _valid_matching(stored['witness_max'], edges, len(rows)):
        w_max = sum(dict(((l, r), w) for l, r, w in edges)[(l, r)]
                    for l, r, _ in stored['witness_max'])
        check('witness_max_optimal', w_max == total_weight
              and len(stored['witness_max']) == cards_max)
    else:
        check('witness_max_optimal', False)

    if stored['policy'] == 'operational':
        matching = stored.get('operational_matching')
        legal = _valid_matching(matching, edges, len(rows))
        check('operational_matching_legal', legal)
        if legal:
            weight = sum(dict(((l, r), w) for l, r, w in edges)[(l, r)] for l, r, _ in matching)
            check('operational_matching_optimal',
                  weight == total_weight and len(matching) == cards_max)
            expected = 'pass' if (len(rows) - len(matching)) <= stored['threshold'] else 'fail'
            check('conclusion', stored['conclusion'] == expected
                  and stored['dedup_estimate'] == len(rows) - len(matching))
        else:
            check('conclusion', False)
    else:
        if dedup_max <= stored['threshold']:
            expected = 'pass'
        elif dedup_min > stored['threshold']:
            expected = 'fail'
        else:
            expected = 'indeterminate'
        check('conclusion', stored['conclusion'] == expected)

    ok = all(item['ok'] for item in checks)
    return {'assessment_id': assessment_id, 'ok': ok, 'exhausted': False,
            'conclusion': stored['conclusion'], 'checks': checks}
