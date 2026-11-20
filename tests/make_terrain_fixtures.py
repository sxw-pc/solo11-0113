"""Generate offline synthetic terrain fixtures with deterministic cell data."""
import json
import sys
from pathlib import Path


def write_fixture(root, spec, cells):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    lines = '\n'.join(json.dumps(c, sort_keys=True, separators=(',', ':')) for c in cells)
    (root / 'cells.jsonl').write_text(lines + '\n', encoding='utf-8')
    (root / 'spec.json').write_text(json.dumps(spec, sort_keys=True), encoding='utf-8')


def depression():
    # 5x5 bowl.  A single west outlet at (2,0) and a south-east outlet at
    # (4,2) are separate terminal regions.  The h=2 ring is one large flat
    # region, the outer h=3 ring another.  (1,2) is a pit at height -1: no
    # lower neighbour, so its water (own + the h=1 cell at (2,2)) is retained.
    height = {
        (2, 0): 0, (4, 2): 0,
        (1, 0): 2, (3, 0): 2, (1, 1): 2, (2, 1): 2, (3, 1): 2,
        (3, 2): 2, (1, 3): 2, (2, 3): 2, (3, 3): 2,
        (2, 2): 1, (1, 2): -1,
    }
    cells = []
    for y in range(5):
        for x in range(5):
            h = height.get((x, y), 3)
            item = {'x': x, 'y': y, 'height': h, 'runoff': 1}
            if (x, y) == (2, 0):
                item['outlet'] = 'W'
            elif (x, y) == (4, 2):
                item['outlet'] = 'E'
            cells.append(item)
    # zero local runoff on the outer flat and on the pit
    for item in cells:
        if (item['x'], item['y']) in ((0, 4), (1, 2)):
            item['runoff'] = 0
    return {'path': 'cells.jsonl', 'neighbors': 4, 'height_unit': 'synthetic'}, cells


def cascade():
    # Multi-level branching plus a plateau that both receives from several
    # edges and drains through several edges, and a plateau with an internal
    # hole (non-cell coordinate missing from the JSONL, which must not be
    # invented).
    height = {
        (0, 0): 0, (1, 0): 0, (2, 0): 0, (3, 0): 0, (4, 0): 0,
        (0, 1): 2, (1, 1): 2, (2, 1): 2, (3, 1): 2, (4, 1): 2,
        (0, 2): 4, (1, 2): 4, (2, 2): 4, (3, 2): 4, (4, 2): 4,
        (0, 3): 7, (1, 3): 7, (2, 3): 7, (3, 3): 7, (4, 3): 7,
    }
    # remove an interior cell to make a hole; do not fabricate it
    hole = (2, 2)
    cells = []
    outlets = {'A': (0, 0), 'B': (4, 0)}
    for (x, y), h in sorted(height.items(), reverse=True):
        if (x, y) == hole:
            continue
        item = {'x': x, 'y': y, 'height': h, 'runoff': 1}
        for label, pos in outlets.items():
            if (x, y) == pos:
                item['outlet'] = label
        cells.append(item)
    # give the top plateau varied runoff and one zero cell
    cells[0]['runoff'] = 3
    cells[1]['runoff'] = 0
    # negative elevation outlet row adjustments: keep A and B at height -2
    for item in cells:
        if item.get('outlet'):
            item['height'] = -2
    return {'path': 'cells.jsonl', 'neighbors': 4, 'height_unit': 'synthetic'}, cells


def large():
    # 40x40 stair: every interior cell has a strictly lower neighbour.  The
    # right column holds two separate terminal basins: (39,0) at height -1
    # (outlet O0, a local minimum) and (39,39) at height -2 (outlet O1).  The
    # rest of the column at height 0 drains into the lower O1 basin.  A 2x2
    # zero-runoff pocket is a closed depression with nothing to retain, and
    # (20,20) is missing from the JSONL: it must stay missing, never invented.
    cells = []
    for y in range(40):
        for x in range(40):
            if (x, y) == (20, 20):
                continue
            if x == 39:
                if y == 0:
                    item = {'x': x, 'y': y, 'height': -1, 'runoff': 1, 'outlet': 'O0'}
                elif y == 39:
                    item = {'x': x, 'y': y, 'height': -2, 'runoff': 1, 'outlet': 'O1'}
                else:
                    item = {'x': x, 'y': y, 'height': 0, 'runoff': 1}
            else:
                item = {'x': x, 'y': y, 'height': 40 - x, 'runoff': 1}
            cells.append(item)
    by_pos = {(c['x'], c['y']): c for c in cells}
    for pos in ((10, 10), (10, 11), (11, 10), (11, 11)):
        by_pos[pos].update({'height': 20, 'runoff': 0})
    return {'path': 'cells.jsonl', 'neighbors': 4, 'height_unit': 'synthetic'}, cells


def main(bases):
    for name, builder in (('terrain-depression', depression),
                          ('terrain-cascade', cascade),
                          ('terrain-large', large)):
        spec, cells = builder()
        write_fixture(Path(bases) / name, spec, cells)


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'fixtures')
