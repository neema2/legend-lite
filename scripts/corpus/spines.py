"""
Seed SPINES: the fewest links that let a deep query reach a value (the improvement plan's Phase 2).

The deep services navigate four hops, and on the seed almost none of them arrived anywhere:
394 of 400 DSDeep services expected no rows, and of 397 four-level trees 16 had an object at
the bottom. The cause was not depth. It was three joins. Every taxonomy row reaches a legal
entity through book -> desk -> business unit -> legal entity, and BUSINESS_UNIT, an extension
table the expansion derived from DESK, has two rows: one with an absent legal entity (A2) and
one with a dangling one (A1). Both breaks are deliberate and both stay; there was simply no
third row whose chain held. So 362 chains broke at that one hop.

This pass takes every chain the deep, dense and tree generators will read, and gives each
complete paths ending in a non-null value -- three where the root has three rows, and two
DISTINCT values where the data allows, so a filter over the chain can keep some rows and drop
others and leave a limit something to cut (WANT). Two moves, both on a foreign key into a
primary key (every hop these chains cross is one, a self-join's included):

  RELINK  point a row's foreign key at a different parent that completes the chain
  CLONE   add a copy of a row under a new key, linked onward, for a parent to point at --
          where every row of a table is load-bearing

Chains are taken most constrained first (fewest root rows), and once a chain is satisfied the
links of the paths that witness it are FROZEN: a later move may point such a key only at a parent
that gives the witnessed chain the same value, so a later chain can never undo an earlier one. A first version without that re-pointed one business unit's
legal entity 5,818 times, each chain wanting its own value through the same row.

What it never does:

  * change an absent (A2) or dangling (A1, A11) key while it is the last one in its column, so
    every break the expansion and the hand-written seed placed survives
  * point a key at the last parent that had no children through it (A3), or a row at itself
  * move a parent's last child away over a to-many a service counts (_counted): a battery
    service (the fan-out family F0-F6 and its kin), or one of aggregates.py's, which takes only
    ends whose every parent has children. A count that read a number still reads one. The
    first engine run moved trader TRD-001's one daily P&L to TRD-002, and F5's count of it then
    met F6 (a count over an empty to-many is 1); and three aggregate services left the corpus,
    their ends no longer usable. Two are back; a CLONE is a new parent with no children, so
    AA_SalesPersonCounts stays out (aggregates.counted). Elsewhere a parent may be
    left with no children: holding every parent to one left 49 DSDeep chains short of three
    paths rather than 30, since in the expansion's two-row tables each row is usually its
    parent's only child
  * touch a primary key, invent a leaf value, or add a row to the root of the chain it is
    completing
  * edit what a DERIVED table reads (TRADE_FLAT, the partitions, the pre-aggregate): those are
    computed from the seed before the expansion runs, and check() recomputes them afterwards
    to prove they did not move

It runs after the expansion (flat.all_tables), because the chains run through expanded tables.
Deterministic: no RNG; the cheapest move wins, ties by table order.
"""
from __future__ import annotations

import collections
import re
from dataclasses import dataclass

import model

# Complete paths each chain needs, where its root has that many rows. Two is the floor the plan
# sets: a filter over the chain has something to keep and something to drop. Three, so that after
# a filter keeping between a fifth and four fifths of them (selective.py) two rows can be left
# for a limit below the result size to cut.
WANT = 3

# Costs, in edits. A clone is dearer than a relink because it adds a row every service over the
# table sees; changing a key that is absent or dangling (where the column has another) is
# dearer than re-pointing a valid one, because those rows are the adversarial ones.
RELINK, RELINK_UNSET, CLONE = 1.0, 1.5, 2.0
INF = float("inf")

# Passes of collect-and-fix. A clone can add a root row to a class another generator ranks by
# its rows, and so bring a new chain into range; the next pass spines it. Two settle the model.
PASSES = 3

CLONE_TAG = "-SP"


@dataclass(frozen=True)
class Target:
    """A chain to reach: navigation hops then a scalar leaf, from `root` at `as_of`."""
    root: str
    path: tuple
    as_of: str | None
    who: str          # the service that needs it, for a message
    # A chain a query FILTERS on needs two distinct values, and the values its witnesses reach
    # must stay what they were. One a query only projects or fetches needs values, any values.
    filtered: bool = True


# A witnessed value that any non-null value may replace (a chain that is not filtered on).
ANY = object()


# What the pass did, for check() and a reader: (table, key, column, old, new) per relink, and
# (table, new key, template key) per clone; and the chains it could not satisfy, with why.
EDITS: list[tuple] = []
CLONES: list[tuple] = []
UNSATISFIED: list[tuple[Target, str]] = []


# ---------------------------------------------------------------------------- the frozen set

def _frozen() -> tuple[set[tuple[str, str]], set[str]]:
    """(columns no edit may change, tables no clone may grow).

    TRADE_FLAT, the partitions, the partial cache and the pre-aggregate are DERIVED from the
    hand-written seed before the expansion runs, so an edit to anything they read would leave
    them stale -- and the invariance groups over them would then fail for a reason that has
    nothing to do with the engine.
    """
    import flat
    import partition

    cols = {("TRADE", col) for col in partition.COLUMNS}
    cols |= {("TRADE", "BOOK_ID"), ("TRADE", "NOTIONAL"), ("TRADE", "COUNTERPARTY_ID")}
    # Every hop flat.build walks: one hop from TRADE in each of its paths.
    hop_col = {"instrument": "INSTRUMENT_ID", "book": "BOOK_ID",
               "counterparty": "COUNTERPARTY_ID", "trader": "TRADER_ID"}
    cols |= {("TRADE", hop_col[p[0]]) for p in flat.COLUMNS.values() if len(p) > 1}
    return cols, {"TRADE"}


# ---------------------------------------------------------------------------- the walk

def _counted(c: model.Corpus, before: dict[str, list[dict]]) -> set[tuple[str, str]]:
    """(child table, key column) of every to-many end a service COUNTS: a battery service (the
    fan-out family F0-F6 and its kin), or an aggregate service over the seed `before` the pass.
    A parent with children there keeps one, so a count that read a number still reads one."""
    import aggregates
    import battery
    import literals

    paths = [(s.root, p.path) for s in battery.SPECS for p in s.projections if p.agg == "count"]
    paths += [(root, [prop])
              for root, prop in aggregates.counted(c, literals.walked(before), before)]
    out = set()
    for root, path in paths:
        cls, table, hop = root, c.main_table.get(root), None
        for step in path:
            end = c.ends.get((cls, step))
            if end is None or end.join is None or table is None:
                hop = None
                break
            j = c.joins[end.join]
            tgt, _fc, tc = j.other(table)
            hop = (tgt, tc) if j.condition is None else None
            cls, table = end.target, tgt
        if hop is not None:
            out.add(hop)
    return out


def _root_rows(c: model.Corpus, t: Target, tables) -> list[int]:
    """Indexes into the root table of the rows all() sees: its store filter and milestoning
    applied, as the oracle applies them."""
    import oracle
    from query import Spec

    s = Spec("spine", "spine", "", t.root)
    s.as_of = t.as_of
    seen = {id(r) for r in oracle._milestoned(c, s, oracle._mapping_filtered(
        c, t.root, oracle._rows_for(c, t.root, tables), tables))}
    return [i for i, r in enumerate(tables[c.main_table[t.root]]) if id(r) in seen]


def _hops(c: model.Corpus, t: Target):
    """(hops, leaf column) when every hop is a foreign key into a single-column primary key and
    the leaf is a plain column; else the reason this pass leaves it alone."""
    path = list(t.path)
    if t.root in c.unions:
        return "a union root"
    if c.resolve_derived(t.root, path) is not None:
        return "ends on a derived property"
    try:
        _table, col, hops = c.resolve(t.root, path)
    except KeyError as e:
        return f"does not resolve ({e})"
    owner = c.owner_of(t.root, path)
    if (owner, path[-1]) in c.dyna or (owner, path[-1]) in c.chains or owner in c.json_backed:
        return "ends on a computed or bound property"
    for _j, ftab, fcol, ttab, tcol in hops:
        if not fcol:
            return "crosses a join with a general condition"
        if c.tables[ttab].pk != [tcol]:
            return "crosses a join into a column that is not the target's key"
    return hops, col


def _walk(tables, row, hops):
    """The rows a chain passes through from `row`, as (table, index) per level; short where it
    breaks."""
    out, cur = [], row
    for _j, _ftab, fcol, ttab, tcol in hops:
        key = cur.get(fcol)
        j = None if key is None else next(
            (x for x, r in enumerate(tables[ttab]) if r.get(tcol) == key), None)
        if j is None:
            break
        cur = tables[ttab][j]
        out.append((ttab, j))
    return out


def _paths(c, t: Target, tables, hops, col):
    """[(root index, the value it reaches or None, the (table, index) per level)]."""
    root = c.main_table[t.root]
    out = []
    for i in _root_rows(c, t, tables):
        via = _walk(tables, tables[root][i], hops)
        v = tables[via[-1][0]][via[-1][1]].get(col) if len(via) == len(hops) else None
        out.append((i, v, [(root, i)] + via))
    return out


# ---------------------------------------------------------------------------- one chain

class _Fixer:
    """The cheapest edits that complete one more path of one chain."""

    def __init__(self, c, tables, frozen_cols, no_clone, frozen_links, seeded):
        self.c, self.tables = c, tables
        self.frozen_cols, self.no_clone, self.links = frozen_cols, no_clone, frozen_links
        self.seeded = seeded

    def _hop_facts(self, hop):
        """What a relink over this hop may and may not do, computed once per hop.

        (parent keys, absent keys in the column, dangling keys in the column, the parent that
        must keep no children because it is the last one with none, the parents whose one child
        must stay because it is their last)."""
        _j, ftab, fcol, ttab, tcol = hop
        parents = {r.get(tcol) for r in self.tables[ttab]}
        vals = [r.get(fcol) for r in self.tables[ftab]]
        absent = sum(1 for v in vals if v is None)
        dangling = sum(1 for v in vals if v is not None and v not in parents)
        used = collections.Counter(vals)
        childless = [k for k in parents if k not in used]
        only = {k for k in self.seeded(ftab, fcol) if used[k] == 1}
        return parents, absent, dangling, set(childless) if len(childless) == 1 else set(), only

    def _relink_cost(self, hop, facts, i) -> float:
        """What it costs to change row i's key over `hop`, or INF where it is not ours. A key a
        satisfied chain witnesses through may still move, to a parent that keeps that chain's
        value (_keeps)."""
        _j, ftab, fcol, _ttab, _tcol = hop
        if (ftab, fcol) in self.frozen_cols or fcol in self.c.tables[ftab].pk:
            return INF
        parents, absent, dangling, _barred, only = facts
        v = self.tables[ftab][i].get(fcol)
        if v is None:
            return RELINK_UNSET if absent >= 2 else INF          # keep one absent key (A2)
        if v not in parents:
            return RELINK_UNSET if dangling >= 2 else INF        # keep one dangling key (A1)
        if v in only:
            return INF                                           # a parent keeps its last child
        return RELINK

    def _keeps(self, cons, ttab, j) -> bool:
        """Would pointing a frozen key at row j of `ttab` keep every value witnessed through it?"""
        for suffix, col, value in cons:
            cur = self.tables[ttab][j]
            for _h, _f, fcol, nxt, tcol in suffix:
                key = cur.get(fcol)
                cur = None if key is None else next(
                    (r for r in self.tables[nxt] if r.get(tcol) == key), None)
                if cur is None:
                    return False
            if cur.get(col) is None or (value is not ANY and cur.get(col) != value):
                return False
        return True

    def solve(self, hops, col, roots, exclude):
        """best[k][i] = (cost to complete row i of level k, (how, row below)); level len(hops)
        is the leaf. new[k] = the cost of a fresh clone at level k, linked onward (k >= 1).
        Also returns, per hop, the rows below it a key may be pointed at, cheapest first."""
        n = len(hops)
        best, new, options = [None] * (n + 1), [INF] * (n + 1), [None] * n
        best[n] = [(0.0 if r.get(col) is not None and r.get(col) not in exclude else INF, None)
                   for r in self.tables[hops[-1][3]]]
        for k in range(n - 1, -1, -1):
            hop = hops[k]
            _j, ftab, fcol, ttab, tcol = hop
            facts = self._hop_facts(hop)
            down = best[k + 1]
            below = self.tables[ttab]
            idx = {r.get(tcol): j for j, r in enumerate(below)}
            options[k] = sorted((cost, j) for j, (cost, _h) in enumerate(down)
                                if cost < INF and below[j].get(tcol) not in facts[3])
            via_new = new[k + 1]
            rows = self.tables[ftab]
            level = []
            for i in range(len(rows)):
                if k == 0 and i not in roots:
                    level.append((INF, None))
                    continue
                t = idx.get(rows[i].get(fcol))
                choice = (down[t][0] if t is not None else INF, ("keep", t))
                rc = self._relink_cost(hop, facts, i)
                cons = self.links.get((ftab, i, fcol))
                if rc < INF:
                    # Never a row's own key: over a self-join that would be a row managing
                    # itself. A witnessed key moves only to a parent that keeps the witnessed
                    # values and needs no edit below, so nothing under it moves after the check.
                    alt = next(((cost, j) for cost, j in options[k]
                                if j != t and not (ftab == ttab and j == i)
                                and (not cons or (cost == 0 and self._keeps(cons, ttab, j)))),
                               (INF, None))
                    if rc + alt[0] < choice[0]:
                        choice = (rc + alt[0], ("relink", alt[1]))
                    if not cons and rc + via_new < choice[0]:
                        choice = (rc + via_new, ("relink-new", None))
                level.append(choice)
            best[k] = level
            if k >= 1 and ftab not in self.no_clone and rows:
                new[k] = CLONE + min(options[k][0][0] if options[k] else INF, via_new)
        return best, new, options

    def apply(self, hops, plan, k, i):
        """Complete existing row i of level k along its cheapest choices, editing as it goes."""
        best, _new, _options = plan
        while k < len(hops):
            _j, ftab, fcol, ttab, tcol = hops[k]
            how, j = best[k][i][1]
            if how == "keep":
                i, k = j, k + 1
                continue
            if how == "relink-new":
                j = self._clone(hops, plan, k + 1)
            rows = self._writable(ftab)
            old = rows[i].get(fcol)
            rows[i][fcol] = self.tables[ttab][j][tcol]
            EDITS.append((ftab, self._pk(ftab, i), fcol, old, rows[i][fcol]))
            if how == "relink-new":
                return                    # the clone completed its own onward path
            i, k = j, k + 1

    def _clone(self, hops, plan, k) -> int:
        """Add a copy of a row at level k, completed onward along the cheapest path; its index."""
        _best, new, options = plan
        _j, ftab, fcol, ttab, tcol = hops[k]
        rows = self._writable(ftab)
        # The template is the row holding the most values, so the clone reads like the table's
        # other rows rather than like its emptiest one.
        tpl = max(range(len(rows)),
                  key=lambda x: (sum(v is not None for v in rows[x].values()), -x))
        pk = self.c.tables[ftab].pk[0]
        base = rows[tpl][pk]
        row = dict(rows[tpl])
        row[pk] = self._fresh_key(ftab, pk, base)
        rows.append(row)
        me = len(rows) - 1
        CLONES.append((ftab, row[pk], base))
        if options[k] and not new[k + 1] < options[k][0][0]:
            j = options[k][0][1]
            row[fcol] = self.tables[ttab][j][tcol]
            if k + 1 < len(hops):
                self.apply(hops, plan, k + 1, j)
        else:
            j = self._clone(hops, plan, k + 1)
            row[fcol] = self.tables[ttab][j][tcol]
        return me

    def _fresh_key(self, table, pk, base):
        """A key for a clone of the row keyed `base`: `base-SPn`, or as much of it as the
        column's width allows. Never a key already held -- nor one a foreign key into the table
        already holds, since a dangling key that suddenly resolved would undo an A1."""
        taken = {r.get(pk) for r in self.tables[table]}
        for j in self.c.joins.values():
            if j.condition is not None:
                continue
            for mine, theirs in (((j.right_table, j.right_col), (j.left_table, j.left_col)),
                                 ((j.left_table, j.left_col), (j.right_table, j.right_col))):
                if mine == (table, pk):
                    taken |= {r.get(theirs[1]) for r in self.tables.get(theirs[0], [])}
        col = self.c.tables[table].columns[pk]
        if col.kind == "int":
            n = max((k for k in taken if isinstance(k, int)), default=0) + 1
            while n in taken:
                n += 1
            return n
        m = re.search(r"CHAR\s*\(\s*(\d+)", col.type, re.I)
        width = int(m.group(1)) if m else None
        n = 1
        while True:
            key = f"{base}{CLONE_TAG}{n}"
            if width is not None and len(key) > width:
                key = f"{base}"[: max(0, width - len(str(n)))] + str(n)
            if key not in taken:
                return key
            n += 1

    def _pk(self, table, i):
        return tuple(self.tables[table][i].get(k) for k in self.c.tables[table].pk)

    def _writable(self, table) -> list[dict]:
        rows = self.tables[table]
        if not isinstance(rows, _Rows):
            rows = _Rows(dict(r) for r in rows)
            self.tables[table] = rows
        return rows


class _Rows(list):
    """A table's rows once this pass has copied them, so the seed's own lists are never edited."""


def _freeze(links: dict, t: Target, hops, col, path):
    """Freeze the links of one complete path: per hop, (table, row index, key column) -> the
    rest of the chain from that key's parent, its leaf column and the value the path reaches. A
    later move may point the key elsewhere only where the same walk reaches the same value."""
    _root, value, via = path
    value = value if t.filtered else ANY
    for k, ((table, i), (_j, ftab, fcol, _tt, _tc)) in enumerate(zip(via, hops)):
        assert table == ftab
        cons = links.setdefault((ftab, i, fcol), [])
        if (tuple(hops[k + 1:]), col, value) not in cons:
            cons.append((tuple(hops[k + 1:]), col, value))


def _witnesses(paths):
    """Up to WANT complete paths: the first, the first after it reaching a different value, then
    the rest in order."""
    done = [p for p in paths if p[1] is not None]
    if not done:
        return []
    other = next((p for p in done[1:] if p[1] != done[0][1]), None)
    ordered = [done[0]] + ([other] if other else []) + [p for p in done[1:] if p is not other]
    return ordered[:WANT]


def _settle(c, tables, t: Target, shape, ctx) -> str | None:
    """Edits until `t` has WANT complete paths, two values where the data has them; its witness
    paths frozen. None when it is satisfied, else why not."""
    hops, col = shape
    frozen_cols, no_clone, links, distinct_ok, seeded = ctx
    for _attempt in range(2 * WANT + 1):
        paths = _paths(c, t, tables, hops, col)
        want = min(WANT, len(paths))
        if want == 0:
            return "the root has no rows"
        done = [p for p in paths if p[1] is not None]
        wit = _witnesses(paths)
        values = {p[1] for p in done}
        if len(done) >= want and (len(values) >= min(2, want) or t in distinct_ok
                                  or not t.filtered):
            for w in wit:
                _freeze(links, t, hops, col, w)
            return None
        # The first path stays as it is; the second is what the search may re-route.
        for w in wit[:1]:
            _freeze(links, t, hops, col, w)
        f = _Fixer(c, tables, frozen_cols, no_clone, links, seeded)
        # Complete paths first; once there are enough, a second VALUE.
        exclude = values if len(done) >= want else set()
        roots = {p[0] for p in paths} - {w[0] for w in wit[:1]}
        plan = f.solve(hops, col, roots, exclude)
        best = plan[0]
        cand = sorted((best[0][i][0], i) for i in roots if best[0][i][0] < INF
                      and (exclude or next(p[1] for p in paths if p[0] == i) is None))
        if not cand:
            for w in wit:
                _freeze(links, t, hops, col, w)   # keep what there is
            if len(done) >= want:
                distinct_ok.add(t)           # complete; the data offers this chain one value
                return None
            return f"no edit completes a path ({len(done)} of {want})"
        f.apply(hops, plan, 0, cand[0][1])
    return "did not converge"


# ---------------------------------------------------------------------------- the pass

def targets(c: model.Corpus, tables) -> list[Target]:
    """Every chain a deep, dense or tree generator will read, from the generators themselves --
    so the chains spined are the chains queried, not a second guess at them."""
    import deepstack
    import graphs
    import literals
    import stacks

    seeded = literals.walked(tables)
    out = (deepstack.spine_targets(c, seeded, tables) + graphs.spine_targets(c, seeded, tables)
           + stacks.spine_targets(c, seeded, tables))
    return list(dict.fromkeys(out))


# id(corpus) -> (the edited tables' rows, EDITS, CLONES, UNSATISFIED). all_tables runs several
# times in one build, from the same seed and expansion each time, so the search runs once and
# later calls take its result.
_DONE: dict[int, tuple] = {}


def build(c: model.Corpus, tables: dict[str, list[dict]]) -> None:
    """Spine `tables` in place: a table it edits is replaced by an edited copy."""
    if id(c) in _DONE:
        rows, edits, clones, unsatisfied = _DONE[id(c)]
        for name, rs in rows.items():
            tables[name] = _Rows(dict(r) for r in rs)
        EDITS[:], CLONES[:], UNSATISFIED[:] = edits, clones, unsatisfied
        return
    _build(c, tables)
    _DONE[id(c)] = ({name: [dict(r) for r in rs] for name, rs in tables.items()
                     if isinstance(rs, _Rows)}, list(EDITS), list(CLONES), list(UNSATISFIED))


def _build(c: model.Corpus, tables: dict[str, list[dict]]) -> None:
    EDITS.clear()
    CLONES.clear()
    UNSATISFIED.clear()
    frozen_cols, no_clone = _frozen()
    links: dict = {}
    distinct_ok: set = set()
    # The parents with children before the pass, per key column a service counts over.
    before, keys = dict(tables), {}
    counted = _counted(c, before)

    def seeded(ftab, fcol):
        if (ftab, fcol) not in keys:
            keys[ftab, fcol] = ({r.get(fcol) for r in before[ftab]} - {None}
                                if (ftab, fcol) in counted else set())
        return keys[ftab, fcol]
    ctx = (frozen_cols, no_clone, links, distinct_ok, seeded)
    settled: dict[Target, str | None] = {}
    for _pass in range(PASSES):
        fresh = [t for t in targets(c, tables) if t not in settled]
        if not fresh:
            break
        shapes = {t: _hops(c, t) for t in fresh}
        # Already satisfied chains first, only to freeze their witnesses, so no edit below can
        # break them; then the rest. A chain whose root table other chains pass THROUGH goes
        # before them: frozen, its first hops are their middle ones, so it must choose them
        # first -- a chain rooted at a taxonomy row can re-route its own row instead, and a
        # chain rooted at BOOK, every link of whose rows the taxonomies had frozen, could not.
        # Then the most constrained (fewest root rows) first.
        through: dict[str, int] = {}
        for t in fresh:
            if isinstance(shapes[t], tuple):
                for ttab in {h[3] for h in shapes[t][0]}:
                    through[ttab] = through.get(ttab, 0) + 1
        order = sorted(fresh, key=lambda t: (-through.get(c.main_table[t.root], 0),
                                             len(_root_rows(c, t, tables)), -len(t.path),
                                             t.who, t.path))
        for t in order:
            if isinstance(shapes[t], tuple):
                hops, col = shapes[t]
                paths = _paths(c, t, tables, hops, col)
                done = [p for p in paths if p[1] is not None]
                whole = (len(done) >= min(WANT, len(paths))
                         and (len({p[1] for p in done}) >= min(2, len(done)) or not t.filtered))
                for w in _witnesses(paths)[:WANT if whole else 1]:
                    _freeze(links, t, hops, col, w)
        for t in order:
            shape = shapes[t]
            settled[t] = shape if isinstance(shape, str) else _settle(c, tables, t, shape, ctx)
    UNSATISFIED.extend((t, why) for t, why in settled.items() if why is not None)


def check(c: model.Corpus, before: dict[str, list[dict]], after: dict[str, list[dict]]) -> list[str]:
    """The spines removed nothing the seed placed on purpose, and moved no derived table.

    Per column it edited: an absent key and a dangling key survive wherever there was one, a
    parent with no children through it survives wherever there was one, and no parent that had
    children over a to-many a service counts is left with none. And TRADE_FLAT, the
    partitions, the partial cache and the pre-aggregate, recomputed from the spined tables,
    equal the ones computed before the pass.
    """
    import aggregate
    import flat
    import partition

    bad = []
    counted = _counted(c, before)
    for table, col in sorted({(t, col) for t, _k, col, _o, _n in EDITS}):
        join = next((j for j in c.joins.values() if j.condition is None and (
            (j.left_table == table and j.left_col == col)
            or (j.right_table == table and j.right_col == col and not j.self_join))), None)
        if join is None:
            continue
        ptab, _fc, pcol = join.other(table)

        def shapes(data):
            keys = {r.get(pcol) for r in data.get(ptab, [])}
            vals = [r.get(col) for r in data[table]]
            used = set(vals)
            return (any(v is None for v in vals),
                    any(v is not None and v not in keys for v in vals),
                    any(k not in used for k in keys))
        for name, was, now in zip(("an absent key (A2)", "a dangling key (A1)",
                                   "a parent with no children (A3)"),
                                  shapes(before), shapes(after)):
            if was and not now:
                bad.append(f"spines: {table}.{col} lost {name}")
        had = {r.get(col) for r in before[table]}
        has = {r.get(col) for r in after[table]}
        emptied = sorted(str(k) for k in {r.get(pcol) for r in before.get(ptab, [])}
                         if k in had and k not in has)
        if emptied and (table, col) in counted:
            bad.append(f"spines: {table}.{col} left {', '.join(emptied)} in {ptab} with no "
                       f"children")
    derived = {"TRADE_FLAT": flat.build(c, after), "TRADE_FLAT_PARTIAL": flat.partial(c, after),
               "TRADE_BY_BOOK": aggregate.build(c, after), **partition.build(c, after)}
    for name, rows in derived.items():
        if rows != before[name]:
            bad.append(f"spines: {name}, recomputed from the spined tables, differs; a spine "
                       f"edited a column it reads")
    return bad
