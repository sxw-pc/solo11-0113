"""Exact affine-clock geometry for sealed telemetry time-window assessment.

Each device's run shares one affine clock ``utc_ms = offset + rate*tick`` with
real-valued ``rate`` inside the spec's integer rate strip and every calibration
anchor requiring ``utc_lo <= offset + rate*anchor_tick <= utc_hi``.  All
arithmetic uses :class:`fractions.Fraction`, so half-open window endpoints and
degenerate feasible regions (a single segment or point) are decided exactly.

The guaranteed bounds are found by an arrangement argument rather than a sample
grid.  The in-window predicate of a reading changes only across the two lines
``offset + rate*tick = window_start`` and ``offset + rate*tick = window_end``.
The feasible polygon is therefore split into convex cells by a finite set of
lines; the windowed sum is constant on the interior of every 2D cell and on
every 1D edge, and only needs explicit evaluation at 0D vertices.  Sampling the
vertices, the midpoint of every line segment inside the feasible region and
strictly interior points on both sides of every such segment visits every cell,
so the extrema are global, not grid guesses.
"""
from fractions import Fraction
from math import gcd


def _lcm(a, b):
    return abs(a * b) // gcd(a, b) if a and b else abs(a or b)


def line_key(a, b, c):
    """Canonical integer identity of line ``a*rate + b*offset = c``."""
    a, b, c = Fraction(a), Fraction(b), Fraction(c)
    den = _lcm(_lcm(a.denominator, b.denominator), c.denominator)
    ia, ib, ic = int(a * den), int(b * den), int(c * den)
    g = gcd(gcd(abs(ia), abs(ib)), abs(ic))
    if g:
        ia, ib, ic = ia // g, ib // g, ic // g
    sign = next((x for x in (ia, ib) if x), 1)
    if sign < 0:
        ia, ib, ic = -ia, -ib, -ic
    return ia, ib, ic


def _anchor_halfplanes(anchors):
    """Both sides of every anchor bound; lo==hi yields two equal planes."""
    planes = []
    for tick, lo, hi in anchors:
        tick, lo, hi = Fraction(tick), Fraction(lo), Fraction(hi)
        planes.append((tick, Fraction(1), lo, 'anchor'))
        planes.append((-tick, Fraction(-1), -hi, 'anchor'))
    return planes


def _outer_box(rate_lo, rate_hi, anchors):
    """A bounded box guaranteed to contain the anchor-feasible region."""
    off_lo = min(Fraction(lo) - Fraction(tick) * Fraction(rate_hi)
                 for tick, lo, hi in anchors)
    off_hi = max(Fraction(hi) - Fraction(tick) * Fraction(rate_lo)
                 for tick, lo, hi in anchors)
    return [(Fraction(rate_lo), Fraction(rate_hi)), (off_lo, off_hi)]


def _clip(polygon, plane):
    """Sutherland-Hodgman clipping for half-plane a*rate+b*offset >= c."""
    a, b, c = plane
    if not polygon:
        return []
    out = []

    def inside(p):
        return a * p[0] + b * p[1] >= c

    prev = polygon[-1]
    prev_in = inside(prev)
    for cur in polygon:
        cur_in = inside(cur)
        if cur_in != prev_in:
            d = (a * (cur[0] - prev[0]) + b * (cur[1] - prev[1]))
            t = (c - a * prev[0] - b * prev[1]) / d
            out.append((prev[0] + t * (cur[0] - prev[0]),
                        prev[1] + t * (cur[1] - prev[1])))
        if cur_in:
            out.append(cur)
        prev, prev_in = cur, cur_in
    return out


def feasible_polygon(rate_lo, rate_hi, anchors):
    """Return vertices of the closed feasible region, or [] when infeasible."""
    (rx0, rx1), (oy0, oy1) = _outer_box(rate_lo, rate_hi, anchors)
    polygon = [(rx0, oy0), (rx1, oy0), (rx1, oy1), (rx0, oy1)]
    planes = [(Fraction(1), Fraction(0), Fraction(rate_lo), 'rate'),
              (Fraction(-1), Fraction(0), Fraction(-rate_hi), 'rate')]
    planes += _anchor_halfplanes(anchors)
    for a, b, c, _kind in planes:
        polygon = _clip(polygon, (a, b, c))
        if not polygon:
            return []
    return polygon


def subset_feasible(rate_lo, rate_hi, planes):
    """Feasibility check for an arbitrary half-plane subset (IIS search)."""
    rate_lo, rate_hi = Fraction(rate_lo), Fraction(rate_hi)
    y0 = y1 = None
    for a, b, c, _kind in planes:
        if b == 0:
            continue
        for r in (rate_lo, rate_hi):
            bound = (c - a * r) / b
            if y0 is None or bound < y0:
                y0 = bound
            if y1 is None or bound > y1:
                y1 = bound
    if y0 is None:
        polygon = [(rate_lo, Fraction(0)), (rate_hi, Fraction(0)),
                   (rate_hi, Fraction(1)), (rate_lo, Fraction(1))]
    else:
        pad = max(Fraction(1), abs(y1 - y0))
        polygon = [(rate_lo, y0 - pad), (rate_hi, y0 - pad),
                   (rate_hi, y1 + pad), (rate_lo, y1 + pad)]
    for a, b, c, _kind in planes:
        polygon = _clip(polygon, (a, b, c))
        if not polygon:
            return False
    return True


def conflict_reference(rate_lo, rate_hi, anchors):
    """A minimal set of anchor bounds proving that no shared clock exists."""
    planes = [(Fraction(1), Fraction(0), Fraction(rate_lo), 'rate'),
              (Fraction(-1), Fraction(0), Fraction(-rate_hi), 'rate')]
    planes += _anchor_halfplanes(anchors)
    kept = list(planes)
    for plane in planes:
        if plane[3] != 'anchor':
            continue
        trial = [p for p in kept if p is not plane]
        if not subset_feasible(rate_lo, rate_hi, trial):
            kept = trial
    refs = []
    for a, b, c, _kind in kept:
        if b == 0:
            continue
        if b == 1:
            refs.append({'tick': int(a), 'bound': 'utc_lo_ms', 'value': int(c)})
        else:
            refs.append({'tick': int(-a), 'bound': 'utc_hi_ms', 'value': int(-c)})
    return refs


class BudgetExhausted(Exception):
    """Raised when the exact enumeration cannot finish inside the budget."""


def _intersection(line1, line2):
    (a1, b1, c1), (a2, b2, c2) = line1, line2
    det = a1 * b2 - a2 * b1
    if det == 0:
        return None
    return ((b2 * c1 - b1 * c2) / det, (a1 * c2 - a2 * c1) / det)


def _reading_lines(tick, window_start, window_end):
    """Lines where reading membership of [start, end) changes."""
    t = Fraction(tick)
    return [line_key(t, 1, window_start), line_key(t, 1, window_end)]


def guaranteed_bounds(rate_lo, rate_hi, anchors, readings, window_start,
                      window_end, budget=1_000_000):
    """Exact global lower/upper windowed sums over every feasible shared clock.

    Returns ``{'feasible': False, 'conflict': ...}`` or a dict with
    ``lower``/``upper`` Fractions, replayable witnesses, the points evaluated
    and the remaining budget.  ``exact`` is False when the budget ran out: the
    reported values are witnessed only and must never be presented as global
    bounds.
    """
    rate_lo, rate_hi = Fraction(rate_lo), Fraction(rate_hi)
    window_start, window_end = Fraction(window_start), Fraction(window_end)
    polygon = feasible_polygon(rate_lo, rate_hi, anchors)
    if not polygon:
        return {'feasible': False, 'conflict': conflict_reference(
            rate_lo, rate_hi, anchors)}
    remaining = budget

    def consume(n=1):
        nonlocal remaining
        remaining -= n
        if remaining < 0:
            raise BudgetExhausted

    planes = [(Fraction(1), Fraction(0), Fraction(rate_lo)),
              (Fraction(-1), Fraction(0), Fraction(-rate_hi))]
    planes += [(a, b, c) for a, b, c, _ in _anchor_halfplanes(anchors)]
    line_map = {}
    for a, b, c in planes:
        line_map[line_key(a, b, c)] = (Fraction(a), Fraction(b), Fraction(c))
    for tick, value in readings:
        for key in _reading_lines(tick, window_start, window_end):
            ia, ib, ic = key
            line_map[key] = (Fraction(ia), Fraction(ib), Fraction(ic))
    lines = sorted(line_map.values(), key=lambda x: line_key(*x))

    def inside(point):
        r, o = point
        return all(a * r + b * o >= c for a, b, c in planes)

    # Every line contributes the feasible points sitting on it: polygon
    # vertices plus intersections with every non-parallel line.  Keep the
    # clipping ring order when dropping duplicate vertices; only collapse to
    # sorted extremes when the region degenerated to a segment or point.
    unique = list(dict.fromkeys(polygon))
    if len(unique) <= 2:
        polygon = sorted(unique, key=lambda p: (p[0], p[1]))
    else:
        polygon = unique
    on_line = {key: set() for key in line_map}
    points = set(polygon)
    lower_cat, upper_cat = {}, {}
    exact = True
    try:
        consume(len(polygon))
        # A line can run parallel to the polygon boundary and overlap it in a
        # degenerate region, so intersect every line with every polygon edge,
        # not just with other lines.  Degenerate polygons (a segment or point)
        # collapse to one undirected segment between their extreme vertices.
        if len(polygon) == 1:
            poly_edges = []
        elif len(polygon) == 2:
            poly_edges = [(polygon[0], polygon[1])]
        else:
            poly_edges = [(e1, e2) for e1, e2 in
                          zip(polygon, polygon[1:] + polygon[:1]) if e1 != e2]
        for i, (a1, b1, c1) in enumerate(lines):
            key1 = line_key(a1, b1, c1)
            for e1, e2 in poly_edges:
                edge = (e2[0] - e1[0], e2[1] - e1[1])
                denom = a1 * edge[0] + b1 * edge[1]
                if denom == 0:
                    if a1 * e1[0] + b1 * e1[1] == c1:
                        on_line[key1].add(e1)
                        on_line[key1].add(e2)
                    continue
                t = ((c1 - a1 * e1[0] - b1 * e1[1]) / denom)
                if Fraction(0) <= t <= 1:
                    p = (e1[0] + t * edge[0], e1[1] + t * edge[1])
                    if inside(p):
                        points.add(p)
                        on_line[key1].add(p)
            for a2, b2, c2 in lines[i + 1:]:
                p = _intersection((a1, b1, c1), (a2, b2, c2))
                if p is None:
                    continue
                consume()
                if inside(p):
                    points.add(p)
                    on_line[key1].add(p)
                    on_line[line_key(a2, b2, c2)].add(p)
    except BudgetExhausted:
        exact = False

    def evaluate(point):
        r, o = point
        total = 0
        members = []
        for tick, value in readings:
            utc = o + r * tick
            if window_start <= utc < window_end:
                total += value
                members.append(tick)
        return total, members

    def on_any_line(point):
        r, o = point
        return any(a * r + b * o == c for a, b, c in lines)

    def consider(catalog, members, total, clock):
        if total not in catalog:
            catalog[total] = {'clock': clock, 'ticks': sorted(members)}

    try:
        # 0D cells: vertices (and every other feasible intersection).
        for point in sorted(points, key=lambda p: (p[0], p[1])):
            consume()
            total, members = evaluate(point)
            clock = {'rate_ms_per_tick': _frac_str(point[0]),
                     'offset_ms': _frac_str(point[1])}
            consider(lower_cat, members, total, clock)
            consider(upper_cat, members, total, clock)
        # 1D cells and the 2D cells on either side of every segment.
        for key, bucket in on_line.items():
            if len(bucket) < 2:
                continue
            ia, ib, _ic = key
            length2 = ia * ia + ib * ib
            ordered = sorted(bucket, key=lambda p: (p[0], p[1]))
            for p, q in zip(ordered, ordered[1:]):
                if p == q:
                    continue
                mid = ((p[0] + q[0]) / 2, (p[1] + q[1]) / 2)
                consume()
                total, members = evaluate(mid)
                clock = {'rate_ms_per_tick': _frac_str(mid[0]),
                         'offset_ms': _frac_str(mid[1])}
                consider(lower_cat, members, total, clock)
                consider(upper_cat, members, total, clock)
                # Exact interior probe: step along the integer normal of the
                # line, shrinking by halves until a strictly interior point is
                # reached, so no other arrangement line can be crossed twice.
                for sign in (1, -1):
                    scale = Fraction(1, 2)
                    side = None
                    for _ in range(64):
                        candidate = (mid[0] + sign * scale * ib / length2,
                                     mid[1] - sign * scale * ia / length2)
                        if not inside(candidate):
                            scale /= 2
                            continue
                        if on_any_line(candidate):
                            scale /= 2
                            continue
                        side = candidate
                        break
                    if side is None:
                        continue
                    consume()
                    st, sm = evaluate(side)
                    sclock = {'rate_ms_per_tick': _frac_str(side[0]),
                              'offset_ms': _frac_str(side[1])}
                    consider(lower_cat, sm, st, sclock)
                    consider(upper_cat, sm, st, sclock)
    except BudgetExhausted:
        exact = False

    if lower_cat:
        lower, lower_witness = min(lower_cat), lower_cat[min(lower_cat)]
        upper, upper_witness = max(upper_cat), upper_cat[max(upper_cat)]
    else:
        lower = upper = None
        lower_witness = upper_witness = None
    return {
        'feasible': True,
        'exact': exact,
        'lower': lower,
        'upper': upper,
        'lower_witness': lower_witness,
        'upper_witness': upper_witness,
        'points_evaluated': budget - max(remaining, 0),
        'budget': budget,
        'remaining_budget': max(remaining, 0),
    }


def nominal_assessment(rate_lo, rate_hi, anchors, readings, window_start,
                       window_end, offset, rate):
    """Validate one submitted shared clock and assign every reading exactly."""
    rate, offset = Fraction(rate), Fraction(offset)
    window_start, window_end = Fraction(window_start), Fraction(window_end)
    violations = []
    if not Fraction(rate_lo) <= rate <= Fraction(rate_hi):
        violations.append({'reason': 'rate_outside_spec',
                           'rate_lo_ms': rate_lo, 'rate_hi_ms': rate_hi})
    for tick, lo, hi in anchors:
        utc = offset + rate * tick
        if not Fraction(lo) <= utc <= Fraction(hi):
            violations.append({'tick': tick, 'utc_lo_ms': lo, 'utc_hi_ms': hi,
                               'derived_utc_ms': _frac_str(utc)})
    assigned = []
    total = 0
    for tick, value in readings:
        utc = offset + rate * tick
        inside = window_start <= utc < window_end
        if inside:
            total += value
        assigned.append({'tick': tick, 'value': value,
                         'utc_ms': _frac_str(utc), 'in_window': inside})
    return {
        'feasible_clock': not violations,
        'anchor_violations': violations,
        'clock': {'rate_ms_per_tick': _frac_str(rate),
                  'offset_ms': _frac_str(offset)},
        'readings': assigned,
        'window_total': total,
    }


def _frac_str(value):
    value = Fraction(value)
    return str(value.numerator) if value.denominator == 1 else (
        f'{value.numerator}/{value.denominator}')
