"""
Filters that compare against a value the data holds, and limits that cut (the improvement plan's
Phase 2, decision D5).

The generated stacks filtered with `$x.p > ' '`. Every seeded string sorts above a space, so the
guard kept every row whose value was present and dropped the absent ones: a real predicate, and
the one that makes an outer join's NULL visible, but it never asked the engine to compare two
values. 2,166 of the corpus's comparisons were that guard. And 1,597 queries ended in
`->limit(25)` over tables of five to forty rows, where the limit cut nothing.

So a guard becomes a comparison against a value of the filtered column itself, chosen so that it
keeps between a fifth and four fifths of the rows whose value is present -- it has to drop some
and keep some, or the comparison could be wrong in either direction and still pass. The operator
varies (`>`, `>=`, `<`, `<=`, `==`): each still drops the absent rows, as the guard did, because
every ordered comparison and `==` over an absent value is false (oracle._cmp). A tenth of the
guards stay as written (kept()), since the bare guard through an outer join is worth keeping.

A limit becomes about half the rows the query returns without it, so it removes rows. The sort
keys are the root's whole primary key, and the oracle still refuses a cut-off that falls inside a
tie; a limit that would is moved down until one does not.

Each comparand is chosen over the rows where its own column holds a value, and a conjunction of
two is accepted only if the query still returns rows -- at least two where a limit follows, so the
limit has a row to cut -- and a value past its first column.
"""
from __future__ import annotations

import zlib

import oracle
from query import Pred, Proj, Spec

GUARD = " "
OPS = (">", ">=", "<", "<=", "==")
LOW, HIGH = 0.2, 0.8


def is_guard(f) -> bool:
    return isinstance(f, Pred) and f.op == ">" and f.value == GUARD


def kept(key: str) -> bool:
    """The tenth of guards left as `> ' '`, by a hash of the service's (or pair's) key."""
    return zlib.crc32(key.encode("utf-8")) % 10 == 0


def _probe(spec: Spec, filters: list, path: list[str]) -> Spec:
    s = Spec(spec.name + "__probe", spec.pattern, "", spec.root)
    s.as_of = spec.as_of
    s.mapping = spec.mapping
    s.filters = list(filters)
    s.projections = [Proj("v", list(path))]
    return s


def _string_typed(c, root: str, path: list[str]) -> bool:
    owner = c.owner_of(root, path)
    pr = c.classes[owner].props.get(path[-1]) if owner in c.classes else None
    return pr is not None and pr.type == "String" and (owner, path[-1]) not in c.enum_props


# Candidates tried per conjunct before giving up on it. The first nearly always holds; the bound
# keeps a pathological conjunction from evaluating the query hundreds of times.
TRIES = 6


def _yields_rows(c, spec: Spec, tables) -> bool:
    """Does the query's expected result have rows, and a value past its first column (or at its
    tree's deepest level)? What emptiness.check() asks of every result outside a family."""
    import emptiness

    try:
        rows = oracle.as_json_rows(c, spec, oracle.evaluate(c, spec, tables))
    except (oracle.Fanout, oracle.Unsupported):
        return False
    return emptiness.classify(spec, rows) == "ROWS"


def _candidates(c, spec: Spec, i: int, tables, key: str) -> list[Pred]:
    """Comparisons that could replace conjunct i, each keeping between LOW and HIGH of the rows
    where the column holds a value; the preferred one (by the key's hash) first, the rest in
    turn."""
    f = spec.filters[i]
    vals = [r["v"] for r in oracle.evaluate(c, _probe(spec, [], f.path), tables)]
    present = [v for v in vals if v is not None]
    n = len(present)
    cands = []
    for v in sorted(set(present)):
        for op in OPS:
            k = sum(1 for x in present if oracle._cmp(op, x, v))
            if 1 <= k and LOW * n <= k <= HIGH * n:
                cands.append(Pred(list(f.path), op, v))
    if not cands:
        return []
    at = zlib.crc32(f"{key}#{i}".encode("utf-8")) % len(cands)
    return (cands[at:] + cands[:at])[:TRIES]


def _holds(c, spec: Spec, tables, wanted: bool, need: int) -> bool:
    """The query, its limit aside, still returns at least `need` rows and is not hollow -- or it
    never did, and tuning cannot make it worse."""
    if not wanted:
        return True
    saved, spec.limit = spec.limit, None
    try:
        rows = oracle.evaluate(c, spec, tables)
    except (oracle.Fanout, oracle.Unsupported):
        return False
    finally:
        spec.limit = saved
    return len(rows) >= need and _yields_rows(c, spec, tables)


def _search(c, spec: Spec, tables, key: str, guards: list[int], wanted: bool, need: int) -> bool:
    """Replace every conjunct in `guards`, in order; True when the result still holds. Leaves the
    filters as found when it fails."""
    if not guards:
        return _holds(c, spec, tables, wanted, need)
    i, rest = guards[0], guards[1:]
    original = spec.filters[i]
    for cand in _candidates(c, spec, i, tables, key):
        spec.filters[i] = cand
        if _search(c, spec, tables, key, rest, wanted, need):
            return True
    spec.filters[i] = original
    return False


def tune(c, spec: Spec, tables, key: str | None = None) -> None:
    """Each `> ' '` conjunct of `spec.filters` becomes a comparison against a value of its column,
    then the limit is brought below the result size -- never at the cost of a result that had
    rows becoming empty or hollow. Where a limit follows, the comparands are chosen to leave it
    two rows to cut from if any can. A conjunction that cannot be tuned whole is tuned one
    conjunct at a time, and one that cannot be tuned at all keeps its guards."""
    key = key or spec.short
    wanted = _yields_rows(c, spec, tables)
    guards = [i for i, f in enumerate(spec.filters)
              if is_guard(f) and _string_typed(c, spec.root, f.path)]
    if guards and not kept(key):
        needs = [2, 1] if spec.limit is not None and spec.sort else [1]
        if not any(_search(c, spec, tables, key, guards, wanted, need) for need in needs):
            for i in guards:
                if _search(c, spec, tables, key, [i], wanted, 1):
                    break
    _tune_limit(c, spec, tables, wanted)


def _tune_limit(c, spec: Spec, tables, wanted: bool) -> None:
    """A limit below the result size, where a sort precedes it: about half the rows, moved up
    where the first half is hollow and down past a tie the oracle refuses."""
    if spec.limit is None or not spec.sort:
        return
    saved = spec.limit
    spec.limit = None
    try:
        n = len(oracle.evaluate(c, spec, tables))
    finally:
        spec.limit = saved
    if n < 2:
        return
    half = (n + 1) // 2
    for limit in list(range(half, n)) + list(range(half - 1, 0, -1)):
        spec.limit = limit
        try:
            oracle.evaluate(c, spec, tables)
        except oracle.Fanout:
            continue
        if not wanted or _yields_rows(c, spec, tables):
            return
    spec.limit = saved
