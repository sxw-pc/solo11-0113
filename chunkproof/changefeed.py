"""Freeze sealed transaction metadata and materialize queryable entity snapshots.

Registration only freezes evidence; it never replays rows. Materialization turns
the frozen transactions/rows into a recipient-queryable key/value snapshot under
an exact, reviewable optimization contract:

* a transaction is a candidate only when every ordinal 0..expected_rows-1 is
  present exactly once, every explicit parent exists and is itself a candidate,
  and the parent graph is acyclic;
* a selected set is closed under explicit parents and free of concurrent key
  conflicts (two complete transactions touching the same key with no ancestor
  relationship);
* each transaction is atomic — all of its rows replay together or none do;
* ``max_coverage`` lexicographically maximizes (transaction count, authority
  sum) and ``max_authority`` lexicographically maximizes (authority sum,
  transaction count); final ties break on the canonical sorted tx_id set.

The search is exact branch-and-bound. If the node budget is exhausted no
snapshot is issued: the run reports ``indeterminate`` with proven bounds.
"""
import hashlib
import json
from pathlib import Path

from .store import connect
from .engine import canonical

POLICIES = ('max_coverage', 'max_authority')
DEFAULT_NODE_BUDGET = 200_000


class _BudgetExhausted(Exception):
    pass


def _sealed_lines(db, version, relative_path):
    row = db.execute('''SELECT f.sha256, a.path FROM file_entry f
        JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
        WHERE f.version=? AND f.path=?''', (version, relative_path)).fetchone()
    if not row:
        raise ValueError('unassembled_changefeed_file')
    raw = Path(row['path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != row['sha256']:
        raise ValueError('changefeed_bytes_changed')
    return [json.loads(line) for line in raw.decode('utf-8').splitlines() if line.strip()]


def register(db_path, run_id, version, spec_path):
    raw = Path(spec_path).read_bytes()
    spec = json.loads(raw)
    if not run_id or not {'transactions', 'rows'} <= spec.keys():
        raise ValueError('bad_changefeed_spec')
    digest = hashlib.sha256(raw).hexdigest()
    db = connect(db_path)
    try:
        with db:
            old = db.execute('SELECT version,spec_hash FROM changefeed_run WHERE id=?', (run_id,)).fetchone()
            if old:
                if (old['version'], old['spec_hash']) != (version, digest):
                    raise ValueError('changefeed_identity_conflict')
                return digest
            decision = db.execute('SELECT result FROM decision WHERE version=?', (version,)).fetchone()
            if not decision or decision['result'] != '可发布':
                raise ValueError('changefeed_not_sealed')
            transactions = []
            for item in _sealed_lines(db, version, spec['transactions']):
                tx_id, source, seq, parents, expected, authority = (item[key] for key in
                    ('tx_id', 'source', 'source_seq', 'parents', 'expected_rows', 'authority'))
                if not tx_id or not source or type(seq) is not int or seq <= 0 or not isinstance(parents, list) \
                        or any(not isinstance(p, str) or not p for p in parents) \
                        or len(set(parents)) != len(parents) or tx_id in parents \
                        or type(expected) is not int or expected <= 0 \
                        or type(authority) is not int or authority <= 0:
                    raise ValueError('bad_transaction')
                transactions.append((run_id, tx_id, source, seq, json.dumps(parents, sort_keys=True), expected, authority))
            if len({r[1] for r in transactions}) != len(transactions) \
                    or len({(r[2], r[3]) for r in transactions}) != len(transactions):
                raise ValueError('duplicate_transaction_identity')
            tx_ids = {r[1] for r in transactions}
            rows = []
            for item in _sealed_lines(db, version, spec['rows']):
                tx_id, ordinal, key, op = (item[k] for k in ('tx_id', 'ordinal', 'entity_key', 'op'))
                value = item.get('value')
                if tx_id not in tx_ids or type(ordinal) is not int or ordinal < 0 or not key \
                        or op not in ('put', 'delete') or (op == 'put' and value is None):
                    raise ValueError('bad_change_row')
                rows.append((run_id, tx_id, ordinal, key, op, json.dumps(value, sort_keys=True)))
            if len({(r[1], r[2]) for r in rows}) != len(rows):
                raise ValueError('duplicate_change_ordinal')
            db.execute('INSERT INTO changefeed_run VALUES (?,?,?,?)', (run_id, version, digest, raw))
            db.executemany('INSERT INTO change_tx VALUES (?,?,?,?,?,?,?)', transactions)
            db.executemany('INSERT INTO change_row VALUES (?,?,?,?,?,?)', rows)
            return digest
    finally:
        db.close()


def inspect(db_path, run_id):
    """Report raw transaction coverage, not a materialized entity state."""
    db = connect(db_path)
    try:
        if not db.execute('SELECT 1 FROM changefeed_run WHERE id=?', (run_id,)).fetchone():
            raise ValueError('unknown_changefeed_run')
        transactions = db.execute('SELECT count(*) n FROM change_tx WHERE run_id=?', (run_id,)).fetchone()['n']
        rows = db.execute('SELECT count(*) n FROM change_row WHERE run_id=?', (run_id,)).fetchone()['n']
        incomplete = [r['tx_id'] for r in db.execute('''SELECT t.tx_id FROM change_tx t
            LEFT JOIN change_row c ON c.run_id=t.run_id AND c.tx_id=t.tx_id
            WHERE t.run_id=? GROUP BY t.tx_id HAVING count(c.ordinal)<>t.expected_rows ORDER BY t.tx_id''', (run_id,))]
        return {'transactions': transactions, 'rows': rows, 'incomplete': incomplete}
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# Frozen evidence loading and structural eligibility
# --------------------------------------------------------------------------- #

def _load_evidence(db, run_id):
    """Read the frozen run straight from the sealed JSONL byte evidence."""
    run = db.execute('SELECT * FROM changefeed_run WHERE id=?', (run_id,)).fetchone()
    if not run:
        raise ValueError('unknown_changefeed_run')
    version = run['version']
    spec = json.loads(run['spec'].decode('utf-8'))
    tx_records = _sealed_lines(db, version, spec['transactions'])
    row_records = _sealed_lines(db, version, spec['rows'])
    txs = {}
    for item in tx_records:
        txs[item['tx_id']] = {
            'tx_id': item['tx_id'], 'source': item['source'],
            'source_seq': item['source_seq'], 'parents': list(item['parents']),
            'expected_rows': item['expected_rows'], 'authority': item['authority'],
            'rows': []}
    for item in row_records:
        txs[item['tx_id']]['rows'].append({
            'ordinal': item['ordinal'], 'entity_key': item['entity_key'],
            'op': item['op'], 'value': item.get('value')})
    for tx in txs.values():
        tx['rows'].sort(key=lambda r: r['ordinal'])
    file_hashes = {}
    for rel in (spec['transactions'], spec['rows']):
        entry = db.execute('''SELECT f.sha256 FROM file_entry f
            JOIN assembled_file a ON a.version=f.version AND a.file_id=f.id
            WHERE f.version=? AND f.path=?''', (version, rel)).fetchone()
        file_hashes[rel] = entry['sha256']
    return {
        'run_id': run_id, 'version': version, 'spec_hash': run['spec_hash'],
        'spec': spec, 'txs': txs, 'file_hashes': file_hashes}


def _classify(evidence):
    """Split frozen transactions into eligible candidates and blocked ones.

    Block reasons: missing_ordinal (missing or duplicated row ordinal),
    missing_parent (an explicit parent is absent, blocked or unknown),
    causal_cycle (the parent graph reaches itself).
    """
    txs = evidence['txs']
    reasons = {}
    complete = set()
    for tx_id, tx in txs.items():
        ordinals = [r['ordinal'] for r in tx['rows']]
        if len(ordinals) != tx['expected_rows'] or sorted(ordinals) != list(range(tx['expected_rows'])):
            reasons[tx_id] = 'missing_ordinal'
        else:
            complete.add(tx_id)

    # DFS coloring over explicit parents to find every tx that sits on a cycle.
    color = {tx_id: 0 for tx_id in txs}
    on_cycle = set()

    def visit(node, stack):
        color[node] = 1
        stack.append(node)
        for parent in txs[node]['parents']:
            if parent not in txs:
                continue
            if color[parent] == 1:
                on_cycle.update(stack[stack.index(parent):])
            elif color[parent] == 0:
                visit(parent, stack)
        stack.pop()
        color[node] = 2

    for tx_id in sorted(txs):
        if color[tx_id] == 0:
            visit(tx_id, [])

    # Kahn-style fixed point: a tx is eligible only when complete, not on a
    # cycle, and every parent is an already eligible candidate.
    eligible = set()
    progress = True
    while progress:
        progress = False
        for tx_id in sorted(txs):
            if tx_id in eligible or tx_id not in complete or tx_id in on_cycle:
                continue
            blocked_parent = next((p for p in txs[tx_id]['parents'] if p not in eligible), None)
            if blocked_parent is None:
                eligible.add(tx_id)
                progress = True
    for tx_id in txs:
        if tx_id in eligible:
            continue
        if tx_id in on_cycle:
            reasons[tx_id] = 'causal_cycle'
        elif tx_id not in complete:
            reasons.setdefault(tx_id, 'missing_ordinal')
        else:
            reasons[tx_id] = 'missing_parent'

    # Ancestor closure inside the eligible graph (Kahn accumulation).
    ancestors = {}
    remaining = set(eligible)
    while remaining:
        ready = sorted(tx_id for tx_id in remaining
                       if all(p not in remaining for p in txs[tx_id]['parents'] if p in eligible))
        if not ready:  # eligible graph is acyclic; defensive only
            raise ValueError('causal_cycle')
        for tx_id in ready:
            parents = [p for p in txs[tx_id]['parents'] if p in eligible]
            closure = set(parents)
            for parent in parents:
                closure |= ancestors[parent]
            ancestors[tx_id] = closure
            remaining.discard(tx_id)
    return eligible, reasons, ancestors


# --------------------------------------------------------------------------- #
# Exact branch-and-bound optimization
# --------------------------------------------------------------------------- #

def _conflict_graph(eligible, ancestors, evidence):
    """Concurrent write conflicts: same key, neither tx is an ancestor."""
    keys = {}
    for tx_id in eligible:
        for row in evidence['txs'][tx_id]['rows']:
            keys.setdefault(row['entity_key'], set()).add(tx_id)
    conflicts = {tx_id: set() for tx_id in eligible}
    for writers in keys.values():
        writers = list(writers)
        for i in range(len(writers)):
            for j in range(i + 1, len(writers)):
                a, b = writers[i], writers[j]
                if a in ancestors[b] or b in ancestors[a]:
                    continue  # causal rewrite, ordered by parents, not a conflict
                conflicts[a].add(b)
                conflicts[b].add(a)
    return conflicts


def _optimize(eligible, ancestors, conflicts, evidence, objective, node_budget):
    """Exact search over closed, conflict-free selected sets.

    Returns (best_set, metrics, search_stats, exhausted). Selection state is
    propagated by fixed-point closure so every explored state is feasible;
    optimistic upper bounds prune branches that cannot beat the incumbent.
    """
    order = sorted(eligible)
    index = {tx_id: i for i, tx_id in enumerate(order)}
    parent_idx = [frozenset(index[p] for p in evidence['txs'][tx_id]['parents'] if p in eligible)
                  for tx_id in order]
    conflict_idx = [frozenset(index[p] for p in conflicts[tx_id]) for tx_id in order]
    authority = [evidence['txs'][tx_id]['authority'] for tx_id in order]
    n = len(order)
    nodes = 0
    exhausted = False

    # -1 undecided, 1 selected, 0 excluded; fixed-point closure maintains
    # feasibility (parents forced in, conflicts forced out, descendants out).
    state = [-1] * n
    best = None  # (primary, secondary, canonical tuple, selected frozenset)

    def score(selected):
        count = len(selected)
        weight = sum(authority[i] for i in selected)
        if objective == 'max_coverage':
            return count, weight
        return weight, count

    def key_for(selected):
        primary, secondary = score(selected)
        return primary, secondary, tuple(sorted(order[i] for i in selected))

    def branch():
        nonlocal nodes, best, exhausted
        nodes += 1
        if nodes > node_budget:
            exhausted = True
            raise _BudgetExhausted()
        undecided = [i for i, flag in enumerate(state) if flag == -1]
        if not undecided:
            selected = frozenset(i for i, flag in enumerate(state) if flag == 1)
            candidate = key_for(selected)
            if best is None or candidate > best[:3]:
                best = (candidate[0], candidate[1], candidate[2], selected)
            return
        # Most constrained undecided tx first; ties on tx_id for determinism.
        undecided.sort(key=lambda i: (-len(conflict_idx[i]), order[i]))
        chosen = undecided[0]

        def include(i):
            forced = [i]
            seen = {i}
            while forced:
                node = forced.pop()
                if state[node] == 0:
                    return None
                if state[node] == 1:
                    continue
                state[node] = 1
                for parent in parent_idx[node]:
                    if state[parent] == 0:
                        return None
                    if state[parent] == -1 and parent not in seen:
                        seen.add(parent)
                        forced.append(parent)
                for rival in conflict_idx[node]:
                    if state[rival] == 1:
                        return None
                    if state[rival] == -1:
                        state[rival] = 0
                        seen.add(rival)
            return seen

        # Include branch first: tight incumbent early, then exclude branch.
        snapshot = state[:]
        touched = include(chosen)
        if touched is not None:
            if not _pruned():
                branch()
        for i in range(n):
            state[i] = snapshot[i]
        state[chosen] = 0
        if not _pruned():
            branch()
        for i in range(n):
            state[i] = snapshot[i]

    def _pruned():
        """Optimistic upper bound; only numeric bounds prune, ties reach leaves."""
        if best is None:
            return False
        selected = frozenset(i for i, flag in enumerate(state) if flag == 1)
        undecided = [i for i, flag in enumerate(state) if flag == -1]
        # Every undecided tx optimistically included: unattainable-high bound.
        optimistic = set(selected) | set(undecided)
        prim_ub, sec_ub = score(optimistic)
        return prim_ub < best[0] or (prim_ub == best[0] and sec_ub < best[1])

    try:
        branch()
    except _BudgetExhausted:
        exhausted = True

    upper_primary, upper_secondary = score(set(range(n)))
    stats = {'nodes_explored': nodes, 'node_budget': node_budget,
             'upper_bound': {'primary': upper_primary, 'secondary': upper_secondary}}
    if best is None or exhausted:
        incumbent = None
        if best is not None:
            incumbent = {'primary': best[0], 'secondary': best[1],
                         'tx_ids': sorted(order[i] for i in best[3])}
        stats['proven_bound'] = incumbent
        return None, None, stats, True
    selected_tx = frozenset(order[i] for i in best[3])
    metrics = {'transactions': best[0] if objective == 'max_coverage' else best[1],
               'authority': best[1] if objective == 'max_coverage' else best[0]}
    stats['optimum'] = {'primary': best[0], 'secondary': best[1],
                        'tx_ids': list(best[2])}
    return selected_tx, metrics, stats, False


# --------------------------------------------------------------------------- #
# Replay and evidence assembly
# --------------------------------------------------------------------------- #

def _topo_order(selected, evidence):
    """Causal replay order: parents first, tx_id tie-break (Kahn)."""
    remaining = set(selected)
    ordered = []
    while remaining:
        ready = sorted(tx_id for tx_id in remaining
                       if all(p not in remaining for p in evidence['txs'][tx_id]['parents']))
        if not ready:  # eligible graph is acyclic; defensive only
            raise ValueError('causal_cycle')
        ordered.extend(ready)
        remaining.difference_update(ready)
    return ordered


def _replay(selected, evidence):
    entities = {}
    last_writer = {}
    applied = []
    for tx_id in _topo_order(selected, evidence):
        applied.append(tx_id)
        for row in evidence['txs'][tx_id]['rows']:
            if row['op'] == 'put':
                entities[row['entity_key']] = row['value']
            else:
                entities.pop(row['entity_key'], None)
            last_writer[row['entity_key']] = tx_id
    return applied, {key: {'value': entities[key], 'last_write_tx': last_writer[key]}
                     for key in sorted(entities)}


def _digest_map(evidence):
    tx_digest, row_digest = {}, {}
    for tx_id, tx in evidence['txs'].items():
        tx_digest[tx_id] = hashlib.sha256(canonical({
            'tx_id': tx_id, 'source': tx['source'], 'source_seq': tx['source_seq'],
            'parents': tx['parents'], 'expected_rows': tx['expected_rows'],
            'authority': tx['authority']})).hexdigest()
        row_digest[tx_id] = [hashlib.sha256(canonical(row)).hexdigest()
                             for row in tx['rows']]
    return tx_digest, row_digest


def _candidate_evidence(tx_id, evidence, eligible, reasons, conflicts, selected):
    if tx_id not in eligible:
        return {'tx_id': tx_id, 'eligible': False, 'reason': reasons[tx_id],
                'authority': evidence['txs'][tx_id]['authority'],
                'source_seq': evidence['txs'][tx_id]['source_seq']}
    if tx_id in selected:
        reason = 'selected'
    elif any(p in eligible and p not in selected for p in evidence['txs'][tx_id]['parents']):
        reason = 'ancestor_excluded'
    elif conflicts[tx_id] & selected:
        reason = 'conflicts_with_selected'
    else:
        reason = 'not_in_optimal_set'
    return {'tx_id': tx_id, 'eligible': True, 'reason': reason,
            'authority': evidence['txs'][tx_id]['authority'],
            'source_seq': evidence['txs'][tx_id]['source_seq'],
            'parents': list(evidence['txs'][tx_id]['parents']),
            'conflicts': sorted(conflicts[tx_id])}


def materialize(db_path, run_id, policy, snapshot_id, node_budget=DEFAULT_NODE_BUDGET):
    """Build and seal the optimal entity snapshot for one business policy."""
    if policy not in POLICIES:
        raise ValueError('unknown_policy')
    if not snapshot_id:
        raise ValueError('bad_snapshot_id')
    if not isinstance(node_budget, int) or node_budget <= 0:
        raise ValueError('bad_node_budget')
    db = connect(db_path)
    try:
        evidence = _load_evidence(db, run_id)
        existing = db.execute('SELECT id,evidence_hash,evidence,status FROM snapshot WHERE id=?',
                              (snapshot_id,)).fetchone()
        eligible, reasons, ancestors = _classify(evidence)
        conflicts = _conflict_graph(eligible, ancestors, evidence)

        searches = {}
        chosen = {}
        for objective in POLICIES:
            selected, metrics_or_none, stats, exhausted = _optimize(
                eligible, ancestors, conflicts, evidence, objective, node_budget)
            searches[objective] = stats
            if objective == policy:
                chosen = {'selected': selected, 'exhausted': exhausted, 'metrics': metrics_or_none}

        tx_digest, row_digest = _digest_map(evidence)
        candidates = [_candidate_evidence(tx_id, evidence, eligible, reasons, conflicts,
                                          chosen['selected'] or frozenset())
                      for tx_id in sorted(evidence['txs'])]
        if chosen['exhausted'] or chosen['selected'] is None:
            result = {
                'snapshot_id': snapshot_id, 'run_id': run_id, 'policy': policy,
                'status': 'indeterminate', 'frozen': {
                    'version': evidence['version'], 'spec_hash': evidence['spec_hash'],
                    'sealed_files': evidence['file_hashes']},
                'search': searches[policy], 'all_policy_searches': searches,
                'candidates': candidates}
            return result

        selected = chosen['selected']
        replay_order, entities = _replay(selected, evidence)
        document = {
            'snapshot_id': snapshot_id, 'run_id': run_id, 'policy': policy,
            'status': 'sealed',
            'frozen': {'run_id': run_id, 'version': evidence['version'],
                       'spec_hash': evidence['spec_hash'],
                       'sealed_files': evidence['file_hashes'],
                       'transaction_digests': tx_digest, 'row_digests': row_digest},
            'candidates': candidates,
            'excluded': [{'tx_id': tx_id, 'reason': reasons[tx_id]}
                         for tx_id in sorted(evidence['txs']) if tx_id not in eligible],
            'selection': {
                'tx_ids': sorted(selected), 'metrics': chosen['metrics'],
                'replay_order': replay_order},
            'optimality': searches,
            'entities': entities}
        payload = canonical(document)
        evidence_hash = hashlib.sha256(payload).hexdigest()
        document['evidence_hash'] = evidence_hash
        with db:
            if existing:
                if existing['status'] != 'sealed' or existing['evidence_hash'] != evidence_hash \
                        or existing['evidence'] != payload:
                    raise ValueError('snapshot_identity_conflict')
                cached = json.loads(existing['evidence'].decode('utf-8'))
                cached['evidence_hash'] = existing['evidence_hash']
                return cached
            clash = db.execute('SELECT id FROM snapshot WHERE run_id=? AND policy=?',
                               (run_id, policy)).fetchone()
            if clash:
                raise ValueError('snapshot_policy_taken')
            db.execute('INSERT INTO snapshot(id,run_id,policy,status,evidence_hash,evidence) VALUES (?,?,?,?,?,?)',
                       (snapshot_id, run_id, policy, 'sealed', evidence_hash, payload))
        return document
    finally:
        db.close()


def verify(db_path, snapshot_id, node_budget=DEFAULT_NODE_BUDGET):
    """Independently replay frozen evidence and re-prove the sealed optimum."""
    db = connect(db_path)
    try:
        record = db.execute('SELECT * FROM snapshot WHERE id=?', (snapshot_id,)).fetchone()
        if not record:
            raise ValueError('unknown_snapshot')
        document = json.loads(record['evidence'].decode('utf-8'))
        if record['status'] != 'sealed' or document['status'] != 'sealed':
            raise ValueError('snapshot_not_sealed')
        # Column hash independently covers every sealed byte; the embedded
        # field in the returned view must name the same digest.
        payload_hash = hashlib.sha256(record['evidence']).hexdigest()
        if payload_hash != record['evidence_hash']:
            raise ValueError('snapshot_evidence_tampered')
        evidence = _load_evidence(db, record['run_id'])

        # The sealed digest map must match the bytes reconstructed from the
        # sealed JSONL files — dropped rows or changed values fail here.
        tx_digest, row_digest = _digest_map(evidence)
        if document['frozen']['transaction_digests'] != tx_digest \
                or document['frozen']['row_digests'] != row_digest \
                or document['frozen']['sealed_files'] != evidence['file_hashes'] \
                or document['frozen']['spec_hash'] != evidence['spec_hash']:
            raise ValueError('frozen_evidence_mismatch')

        eligible, reasons, ancestors = _classify(evidence)
        conflicts = _conflict_graph(eligible, ancestors, evidence)
        selected, metrics, stats, exhausted = _optimize(
            eligible, ancestors, conflicts, evidence, record['policy'], node_budget)
        if exhausted:
            raise ValueError('verify_budget_exhausted')
        sealed_set = frozenset(document['selection']['tx_ids'])
        if sealed_set != selected:
            raise ValueError('selection_not_optimal')
        if document['selection']['metrics'] != metrics:
            raise ValueError('selection_metrics_mismatch')

        # Structural checks on the sealed set itself.
        for tx_id in sealed_set:
            if tx_id not in eligible:
                raise ValueError('ineligible_transaction_selected')
            if not set(evidence['txs'][tx_id]['parents']) <= sealed_set:
                raise ValueError('causal_closure_broken')
        for tx_id in sealed_set:
            if conflicts[tx_id] & sealed_set:
                raise ValueError('concurrent_conflict_in_snapshot')

        replay_order, entities = _replay(sealed_set, evidence)
        if replay_order != document['selection']['replay_order']:
            raise ValueError('replay_order_mismatch')
        if entities != document['entities']:
            raise ValueError('snapshot_values_mismatch')

        # Re-prove the companion policy evidence stored at seal time.
        for objective in POLICIES:
            alt_selected, _, alt_stats, alt_exhausted = _optimize(
                eligible, ancestors, conflicts, evidence, objective, node_budget)
            if alt_exhausted:
                raise ValueError('verify_budget_exhausted')
            sealed_stats = document['optimality'][objective]
            if sealed_stats['optimum']['tx_ids'] != sorted(alt_selected) \
                    or (sealed_stats['optimum']['primary'], sealed_stats['optimum']['secondary']) != \
                    (alt_stats['optimum']['primary'], alt_stats['optimum']['secondary']):
                raise ValueError('optimality_certificate_mismatch')
        return {'snapshot_id': snapshot_id, 'status': 'verified',
                'policy': record['policy'], 'transactions': sorted(sealed_set),
                'entities': entities, 'evidence_hash': record['evidence_hash']}
    finally:
        db.close()


def query(db_path, snapshot_id, entity_key=None):
    """Read-only key/value queries against a sealed snapshot."""
    db = connect(db_path)
    try:
        record = db.execute('SELECT * FROM snapshot WHERE id=?', (snapshot_id,)).fetchone()
        if not record:
            raise ValueError('unknown_snapshot')
        if record['status'] != 'sealed':
            raise ValueError('snapshot_not_sealed')
        document = json.loads(record['evidence'].decode('utf-8'))
        entities = document['entities']
        if entity_key is not None:
            if entity_key not in entities:
                raise ValueError('unknown_entity_key')
            return {entity_key: entities[entity_key]}
        return entities
    finally:
        db.close()
