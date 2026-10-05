"""
Reference evaluator — computes what a service query must return, independently of Legend.

This is the thing that makes the corpus worth having. Without it a test can only assert
that *something* came back, which is what the 12 stress services did before this and why
they could not have detected a wrong answer.

It is deliberately NOT a Legend implementation. It models exactly the algebra the service
queries use, and refuses anything else:

  all()            every row of the class's ~mainTable
  to-one nav       LEFT OUTER JOIN — this is the semantic that matters most. Legend
                   compiles to-one property navigation to an outer join, so a row whose
                   foreign key is NULL or dangling SURVIVES with NULLs in the navigated
                   columns. An engine that emitted an inner join would silently drop
                   rows, and the row counts here are what catch it.
  projection       column value, or NULL if any hop on the path failed
  filter           three-valued: NULL compared to anything is UNKNOWN, and UNKNOWN rows
                   are excluded. `status == 'EXECUTED'` must not match a NULL status.
  sort / limit     ordering is computed but see `ordered` below

Where it refuses:
  - A to-many hop would fan the row set out. The service queries contain none (verified
    by query.py), and rather than guess at Legend's fan-out semantics the resolver
    raises. When L1 adds to-many queries this is the place to extend, deliberately.
  - A join whose target key is not unique would also fan out. Asserted, not assumed.

On ordering: EqualToJson compares arrays UNORDERED (the comparator is
NULL_MISSING_EQUIVALENT_AND_UNORDERED_ARRAYS), so an expectation cannot pin row order.
Every case therefore reports `ordered=False` and the row multiset is what is asserted.
A sort is still worth executing — `->sort()->limit()` changes WHICH rows come back, and
that IS asserted — but the corpus must never claim to test ordering through this harness.
"""
from __future__ import annotations

import json
import math

import exactmath  # correctly rounded: the same double on every platform (P2-01)
from datetime import datetime as _datetime
import re

from model import Corpus
from query import Pred, Spec

def _timestamp(v: str) -> str:
    """Legend renders a DateTime column as ISO-8601 with nanoseconds and a numeric UTC
    offset — "2024-06-03T19:00:00.000000000+0000" — not as the "YYYY-MM-DD HH:MM:SS" the
    seed is written in. A DATE column, by contrast, renders as plain "2024-06-03".

    Determined empirically: the first run of this corpus passed every service that
    projected no timestamp and failed every service that projected one, with Q7 showing
    the two forms side by side. It is not inferred from the serializer source.
    """
    date, _, clock = v.partition(" ")
    return f"{date}T{clock}.000000000+0000"


class Fanout(Exception):
    """Raised when evaluating would require fan-out semantics we refuse to guess at."""


def _index(rows: list[dict], key: str, table: str, join: str) -> dict:
    idx = {}
    for r in rows:
        v = r.get(key)
        if v is None:
            continue
        if v in idx:
            raise Fanout(
                f"join {join} targets {table}.{key} which is not unique (value {v!r}); "
                f"the navigation would fan out")
        idx[v] = r
    return idx


# ------------------------------------------------------- general join conditions
#
# A join whose condition is more than one equality cannot be indexed, so it is evaluated per
# candidate row. Correctness first: these are the joins the reader used to drop entirely, so
# there is no existing behaviour to preserve and no reason to be clever. The tables are five
# rows each.
def _operand(c: Corpus, node, binding: dict):
    """One side of a join comparison, read from whichever row its table is bound to."""
    tag, body = node
    if tag == "lit":
        return body
    if tag == "col":
        table, col = body
        row = binding.get(table)
        return None if row is None else row.get(col)
    if tag == "call":
        fn, args = body
        return _dynafunction(fn, [_operand(c, a, binding) for a in args])
    if tag == "array":
        return [_operand(c, a, binding) for a in body]
    raise Unsupported(f"join operand {tag!r} has no evaluation rule")


def _condition(c: Corpus, node, binding: dict) -> bool:
    tag, body = node
    if tag == "pred":
        # A boolean-valued call standing alone as the predicate. NULL is not true: the same
        # three-valued rule the comparisons use, so a join keeps only pairs it is TRUE for.
        v = _operand(c, body, binding)
        return v is True
    if tag == "and":
        return _condition(c, body[0], binding) and _condition(c, body[1], binding)
    if tag == "or":
        return _condition(c, body[0], binding) or _condition(c, body[1], binding)
    if tag == "null":
        operand, negated = body
        v = _operand(c, operand, binding)
        return (v is not None) if negated else (v is None)
    if tag == "cmp":
        left, op, right = body
        # Three-valued: a NULL operand makes the comparison UNKNOWN, and a join keeps only
        # pairs for which the condition is TRUE. Same rule the query predicates use, and the
        # same one F28 shows `!=` breaking on the query side.
        return _cmp({"=": "==", "<>": "!="}.get(op, op),
                    _operand(c, left, binding), _operand(c, right, binding))
    raise Unsupported(f"join condition node {tag!r} has no evaluation rule")


def _general_targets(c: Corpus, data, join, from_table: str, src: dict,
                     reverse: bool = False) -> list[dict]:
    """Every row on the far side of `join` that the condition pairs with `src`.

    `reverse` swaps which row plays `{target}`. A {target} self-join is written from ONE
    side -- `POINT.TENOR_DAYS < {target}.TENOR_DAYS` says "the target is longer" -- and the
    association's other end means the opposite. Nothing in the model distinguishes the two
    ends, both being CurvePoint to CurvePoint over the same join, so the direction is
    DECLARED in SELF_JOIN_REVERSE rather than guessed. Guessing would return the longer
    pillars for both ends and look entirely reasonable.
    """
    import rhs

    named = rhs.condition_tables(join.condition)
    a, b = join.tables
    to_table = b if from_table == a else a
    out = []
    for tgt in data.get(to_table, []):
        if "{target}" in named:
            binding = ({from_table: tgt, "{target}": src} if reverse
                       else {from_table: src, "{target}": tgt})
        else:
            binding = {from_table: src, to_table: tgt}
        if _condition(c, join.condition, binding):
            out.append(tgt)
    return out


def walk(c: Corpus, data: dict[str, list[dict]], row: dict,
         hops: list[tuple[str, str, str, str, str]]) -> dict | None:
    """Follow the hops from `row`. Returns the landed row, or None if the chain broke —
    either because a key was NULL (A2) or because it matched nothing (A1, A11)."""
    cur = row
    for join, ftab, fcol, ttab, tcol in hops:
        if cur is None:
            return None
        # `not fcol`, not `fcol is None`: a general join records its columns as EMPTY
        # STRINGS, so an `is None` test misses every one of them and the hop falls through
        # to `row.get('')`, which is None -- a broken chain, silently.
        if not fcol:                           # a general condition: no key to index on
            landed = _general_targets(c, data, c.joins[join], ftab, cur)
            if len(landed) > 1:
                raise Fanout(
                    f"join {join} pairs one row of {ftab} with {len(landed)} rows of "
                    f"{ttab}; the navigation would fan out")
            cur = landed[0] if landed else None
            continue
        key = cur.get(fcol)
        if key is None:
            return None
        idx = _index(data[ttab], tcol, ttab, join)
        cur = idx.get(key)
    return cur


def walk_many(c: Corpus, data: dict[str, list[dict]], row: dict,
              hops: list[tuple[str, str, str, str, str]],
              reverse_at: set[int] | None = None) -> list[dict]:
    """Follow hops that MAY fan out, returning every landed row.

    This is the counterpart to walk(): where a to-one navigation lands on at most one row,
    a to-many lands on a set, and the set may be EMPTY. The empty case is the interesting
    one — it is where `->count()` over a LEFT OUTER JOIN is prone to returning 1 (one
    all-NULL joined row) instead of 0.

    A hop with no from-column is a GENERAL condition -- an inequality, a multi-column
    predicate, a {target} self-join -- and has to be evaluated row by row. That case was
    missing: `r.get(None)` is None, the hop was skipped, and every aggregate over such an
    end answered "no children". Empty is a plausible answer, so nothing looked wrong; the
    24-pillar self-join reported every pillar as having nothing longer than it.
    """
    cur = [row]
    for i, (join, ftab, fcol, ttab, tcol) in enumerate(hops):
        nxt = []
        for r in cur:
            if not fcol:
                nxt.extend(_general_targets(c, data, c.joins[join], ftab, r,
                                            reverse=bool(reverse_at and i in reverse_at)))
                continue
            key = r.get(fcol)
            if key is None:
                continue
            nxt.extend(x for x in data[ttab] if x.get(tcol) == key)
        cur = nxt
    return cur


def _cmp(op: str, left, right) -> bool:
    """Three-valued: a NULL operand yields UNKNOWN, which is not true."""
    if left is None or right is None:
        return False
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    raise ValueError(f"unhandled operator {op}")


def _xstore_value(c: Corpus, data, row, root: str, path: list[str]):
    link = XSTORE_LINKS.get((root, path[0]))
    if link is None:
        return None, False
    from_col, table, to_col, target = link
    key = row.get(from_col)
    landed = next((r for r in data[table] if r.get(to_col) == key), None) if key else None
    if landed is None:
        return None, True
    col = c.columns.get(target, {}).get(path[1])
    if col is None:
        raise Unsupported(f"{target}.{path[1]} is not a mapped column")
    return landed.get(col), True


def _value(c: Corpus, data, row, root: str, path: list[str], args=(), func=None):
    if len(path) == 2 and (root, path[0]) in XSTORE_LINKS:
        v, handled = _xstore_value(c, data, row, root, path)
        if handled:
            return v
    if func:
        return _call(c, data, row, root, func, args)
    hit = c.resolve_derived(root, path)
    if hit is not None:
        return _derived(c, data, row, root, path, hit, args)
    owner = c.owner_of(root, path)
    src = c.json_backed.get(owner)
    if src is not None:
        return _json_property(c, data, row, root, path, src)
    # A DYNAFUNCTION is now checked before the plain-column path rather than after it,
    # because its arguments no longer have to be columns of one row: an argument may be a
    # chain, a literal, or another call. The expression tree is walked, each argument
    # evaluated in its own right, and the function applied to the results.
    dyn = c.dyna.get((owner, path[-1]))
    if dyn is not None:
        fn, fnargs = dyn
        # Plain-column arguments are read from the row the property's OWNER sits on, so a
        # dynafunction inside an embedded block reads the embedded parent's row.
        base = row
        if len(path) > 1:
            prefix, _t = c.owner_hops(root, path[:-1])
            base = walk(c, data, row, prefix)
        return _dynafunction(fn, [_arg_value(c, data, base, owner, path[-1], a)
                                  for a in fnargs])
    chain = c.chains.get((owner, path[-1]))
    if chain is not None:
        return _chain_value(c, data, row, root, path, chain)
    table, col, hops = c.resolve(root, path)
    landed = walk(c, data, row, hops)
    raw = None if landed is None else landed.get(col)
    mapping = c.enum_props.get((c.owner_of(root, path), path[-1]))
    if mapping is None or raw is None:
        return raw
    # A source code with no EnumerationMapping entry yields NULL — silently, and
    # indistinguishably from a NULL source column. Established empirically in
    # repro/unmapped-enum/, not assumed: it is the kind of behaviour that could equally
    # have been an error or a pass-through of the raw code.
    #
    # Worth noting that the property is declared [1] and still comes back null, so the
    # multiplicity is not enforced on this path.
    return c.enum_maps[mapping].get(raw)


# ------------------------------------------------------- Binding-payload evaluation
#
# A Binding transformer reads a complex property out of one column of serialized JSON, so a
# sub-property is a KEY in that payload rather than a column. Deserialized here independently
# -- the same discipline as the dynafunctions: the expected value comes from what the payload
# MEANS, not from what the engine returned for it.
def _json_property(c: Corpus, data, row, root: str, path: list[str], src):
    import json

    table, col = src
    # Everything before the binding hop is ordinary navigation; the payload hangs off
    # whatever row that lands on.
    if len(path) > 2:
        _t, _c, prefix = c.resolve(root, path[:-2])
        row = walk(c, data, row, prefix)
        if row is None:
            return None
    raw = row.get(col) if row else None
    if raw is None:
        return None
    try:
        return json.loads(raw).get(path[-1])
    except (ValueError, AttributeError):
        # A payload that is not valid JSON is a SEED defect, not a value. Refusing beats
        # returning None, which would look like an absent key.
        raise Unsupported(f"{table}.{col} does not hold valid JSON: {raw!r:.60}")


# --------------------------------------------------------- join-chain evaluation
#
# A join chain reaches a value through JOINS, with no association involved -- so `resolve`,
# which walks associations, cannot find it. The joins are followed here directly.
#
# Direction is decided per hop by which side the current table sits on, because a Join
# declares two tables and says nothing about which is the source: `Join J(A.x = B.y)` is
# followed A->B from A and B->A from B. Guessing wrong would silently land on the wrong row
# rather than failing.
def _chain_hops(c: Corpus, start: str, joins: list[str]):
    hops, cur = [], start
    for name in joins:
        j = c.joins.get(name)
        if j is None:
            raise Unsupported(f"join {name!r} in a chain is not declared")
        if j.condition is not None:
            # No from/to columns: the pairing is a whole condition, so the hop carries None
            # in their place and walk() evaluates instead of indexing.
            a, b = j.tables
            if cur not in (a, b):
                raise Unsupported(
                    f"join {name!r} connects {a}/{b}, neither of which is {cur} -- the "
                    f"chain does not compose")
            nxt = b if cur == a else a
            hops.append((name, cur, None, nxt, None))
            cur = nxt
            continue
        if j.left_table == cur:
            hops.append((name, j.left_table, j.left_col, j.right_table, j.right_col))
            cur = j.right_table
        elif j.right_table == cur:
            hops.append((name, j.right_table, j.right_col, j.left_table, j.left_col))
            cur = j.left_table
        else:
            raise Unsupported(
                f"join {name!r} connects {j.left_table}/{j.right_table}, neither of which "
                f"is {cur} -- the chain does not compose")
    return hops, cur


def _chain_value(c: Corpus, data, row, root: str, path: list[str], chain):
    joins, target_table, target_col = chain
    # Everything before the last step is ordinary association navigation; the chain hangs
    # off whatever row that lands on.
    if len(path) > 1:
        # owner_hops, not resolve: the prefix may end on an EMBEDDED property, which has no
        # column of its own and which resolve() therefore refuses. A chain inside an
        # embedded block departs from the parent's row, since the block adds no hop.
        prefix, _t = c.owner_hops(root, path[:-1])
        row = walk(c, data, row, prefix)
        if row is None:
            return None
    start = c.main_table.get(c.owner_of(root, path), "")
    hops, landed_table = _chain_hops(c, start, joins)
    if landed_table != target_table:
        raise Unsupported(
            f"chain ends at {landed_table} but the column is {target_table}.{target_col}")
    landed = walk(c, data, row, hops)
    return None if landed is None else landed.get(target_col)


# ------------------------------------------------------- dynafunction evaluation
#
# Implemented INDEPENDENTLY of legend-engine, which is the only thing that makes a
# dynafunction mapping testable at all. Reading the expected value out of the engine would
# make the assertion circular; these are written from what the function MEANS.
#
# Deliberately few. legend-engine's registry is 178 names, and each one added here is a
# separate opportunity to encode a subtly wrong belief about NULL handling or coercion --
# so the set grows only when something needs it, and every entry states its null rule.
#
# NULL semantics are the whole difficulty. SQL propagates NULL through most scalar
# functions, and the corpus guarantees a NULL in every column by construction (property
# A2), so every one of these will meet one.
def _arg_value(c: Corpus, data, base, owner: str, prop: str, node):
    """One argument of a dynafunction call, evaluated in its own right.

    `base` is the row the OWNER sits on. A plain column is read from it directly; a chain
    departs from the owner's main table; a nested call recurses. Splitting evaluation per
    argument is what makes `concat(@J | T.A, T.B)` expressible at all -- the previous shape
    read every argument off one row, so a chain could only ever be the whole expression.
    """
    tag, body = node
    if tag == "lit":
        return body
    if tag == "col":
        _table, col = body
        return None if base is None else base.get(col)
    if tag == "chain":
        # A one-element path, so _chain_value walks no prefix -- `base` IS the owner's row.
        return _chain_value(c, data, base, owner, [prop], body)
    if tag == "call":
        fn, args = body
        vals = []
        for a in args:
            v = _arg_value(c, data, base, owner, prop, a)
            # An ARRAY argument is spread only for the functions where that is what it
            # MEANS. coalesce([a, b]) is coalesce(a, b); `in(x, [a, b])` is not `in(x, a, b)`,
            # so spreading blindly would quietly change the call.
            if isinstance(a, tuple) and a[0] == "array":
                if fn != "coalesce":
                    raise Unsupported(
                        f"{fn}() takes an ARRAY argument and the oracle only knows how to "
                        f"spread one for coalesce")
                vals.extend(v)
            else:
                vals.append(v)
        return _dynafunction(fn, vals)
    if tag == "array":
        return [_arg_value(c, data, base, owner, prop, a) for a in body]
    raise Unsupported(f"dynafunction argument {tag!r} has no evaluation rule")


# ------------------------------------------------------------- dynafunction registry
#
# Each entry is an INDEPENDENT implementation, written from what the function means, and each
# states its NULL rule because that is where the disagreements live. Reading the engine's
# lowering and reproducing it would make every assertion circular: the corpus would agree
# with the engine by construction and could never contradict it, which is the only thing it
# is for.
#
# `concat` is the standing warning. Its behaviour over NULL is decided by the DIALECT and not
# by the function -- DuckDB and Snowflake lower it to a function that ignores NULL, Postgres
# to an operator that propagates it -- so an implementation written from what concat "means"
# was confidently wrong. Anything whose answer depends on which database is underneath
# belongs in REFUSED, not here.
#
# NULL semantics, stated once: SQL propagates NULL through scalar functions, and the seed
# guarantees a NULL in every nullable column (property A2), so every one of these WILL meet
# one. An implementation that never considered it is not finished.
def _dt(v):
    """Parse the text form the seed writes: a date, or a date and time."""
    from datetime import datetime
    s = str(v).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt) + 2].strip(), fmt)
        except ValueError:
            continue
    raise Unsupported(f"not a date this oracle can read: {v!r}")


def _timedelta(**kw):
    from datetime import timedelta
    return timedelta(**kw)


def _flatten(vals):
    out = []
    for v in vals:
        out.extend(v) if isinstance(v, list) else out.append(v)
    return [x for x in out if x is not None]


def _levenshtein(a: str, b: str) -> int:
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _coll(vals):
    v = vals[0] if vals else None
    if v is None:
        return []
    return list(v) if isinstance(v, (list, tuple)) else [v]


def _dedupe(xs):
    seen, out = set(), []
    for x in xs:
        k = (type(x).__name__, x)
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out


def _sort_key(x):
    """Sort NULLs last, and never compare across types -- either would be this oracle
    inventing an ordering the database did not promise."""
    return (x is None, str(type(x)), x if x is not None else "")


_FIXED_UNITS = {"DAYS": 1, "HOURS": 1 / 24, "MINUTES": 1 / 1440, "SECONDS": 1 / 86400,
                "WEEKS": 7}


def _date_diff(vals):
    if any(v is None for v in vals[:2]):
        return None
    a, b, unit = _dt(vals[0]), _dt(vals[1]), str(vals[2]).upper().lstrip("$")
    if unit not in _FIXED_UNITS:
        raise Unsupported(
            f"dateDiff in {unit} is not implemented: a month and a year are not fixed "
            f"lengths, so the answer depends on a convention the signature does not state")
    if unit in ("HOURS", "MINUTES", "SECONDS"):
        # ELAPSED TIME, with the remainder dropped toward zero: legend-pure's own definition (m4
        # DateFunctions: the time units "measure elapsed time, dropping any remainder. This is not the same
        # as counting boundaries"; DateDiff computes ChronoUnit.between). 14:30 to 10:02 seven days later is
        # 163 hours. This oracle counted boundaries (164) until 2026-10-05, reading SQL's DATEDIFF as the
        # documented convention; legend-pure documents the other, and legend-engine's relational SQL
        # counting boundaries is the divergence (UPSTREAM_FINDINGS F58). Exact, in microseconds.
        micros = (b - a) // _timedelta(microseconds=1)
        unit_micros = {"HOURS": 3_600_000_000, "MINUTES": 60_000_000, "SECONDS": 1_000_000}[unit]
        n = abs(micros) // unit_micros
        return n if micros >= 0 else -n
    # the calendar units count boundaries: truncate both operands to the unit and subtract
    step = _FIXED_UNITS[unit] * 86400
    trunc_a = int(a.timestamp() // step)
    trunc_b = int(b.timestamp() // step)
    return trunc_b - trunc_a


def _adjust(vals):
    if any(v is None for v in vals[:2]):
        return None
    d, n, unit = _dt(vals[0]), int(vals[1]), str(vals[2]).upper().lstrip("$")
    if unit not in _FIXED_UNITS:
        raise Unsupported(
            f"adjust by {unit} is not implemented: adding a month to the 31st has no single "
            f"agreed answer, so the corpus would be asserting one convention among several")
    out = d + _timedelta(days=n * _FIXED_UNITS[unit])
    return out.strftime("%Y-%m-%d %H:%M:%S" if " " in str(vals[0]) else "%Y-%m-%d")


def _to_one(vals):
    xs = _coll(vals)
    if len(xs) != 1:
        raise Unsupported(
            f"toOne() over a collection of {len(xs)}: the assertion is that there is exactly "
            f"one, so this is a failure of the DATA, not something to return a value for")
    return xs[0]


def _to_one_many(vals):
    xs = _coll(vals)
    if not xs:
        raise Unsupported("toOneMany() over an empty collection: the assertion is that "
                          "there is at least one")
    return xs


def _median(xs):
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _variance(xs, ddof):
    n = len(xs)
    if n - ddof <= 0:
        raise Unsupported(
            f"variance with ddof={ddof} over {n} observation(s) divides by zero; the sample "
            f"forms are undefined on a single value and dialects differ on whether that is "
            f"an error or NULL")
    mean = sum(xs) / n
    return sum((x - mean) ** 2 for x in xs) / (n - ddof)


def _variance_flagged(vals):
    """`variance(xs, isSample)` -- the boolean picks the divisor."""
    xs = [x for x in _coll(vals) if x is not None]
    return _variance(xs, 1 if vals[-1] else 0) if xs else None


def _pairs(vals):
    """Two parallel collections, or one collection of (x, y) pairs."""
    a = _coll([vals[0]])
    if len(vals) > 1 and isinstance(vals[1], list):
        return [(x, y) for x, y in zip(a, vals[1]) if x is not None and y is not None]
    return [(x[0], x[1]) for x in a if isinstance(x, tuple) and None not in x]


def _covar(vals, ddof):
    ps = _pairs(vals)
    if not ps:
        return None
    n = len(ps)
    if n - ddof <= 0:
        raise Unsupported(f"covariance with ddof={ddof} over {n} pair(s) divides by zero")
    mx = sum(x for x, _y in ps) / n
    my = sum(y for _x, y in ps) / n
    return sum((x - mx) * (y - my) for x, y in ps) / (n - ddof)


def _corr(vals):
    ps = _pairs(vals)
    if len(ps) < 2:
        return None
    sx = _variance([x for x, _y in ps], 1) ** 0.5
    sy = _variance([y for _x, y in ps], 1) ** 0.5
    if sx == 0 or sy == 0:
        raise Unsupported("correlation is undefined when either side has zero variance")
    return _covar([[p for p in ps]], 1) / (sx * sy)


def _percentile(vals):
    xs = sorted(x for x in _coll([vals[0]]) if x is not None)
    if not xs:
        return None
    p = vals[1]
    if not 0 <= p <= 1:
        raise Unsupported(f"percentile {p} is outside [0, 1]")
    # LINEAR interpolation between neighbours. Stated because the alternatives -- nearest
    # rank, lower, higher -- are all in use and the signature picks none of them.
    k = p * (len(xs) - 1)
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return xs[lo] if lo == hi else xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _by(vals, pick):
    """maxBy/minBy: the member of the first collection whose KEY is extremal."""
    xs, keys = _coll([vals[0]]), _coll([vals[1]])
    ps = [(k, v) for v, k in zip(xs, keys) if k is not None]
    if not ps:
        return None
    best = pick(k for k, _v in ps)
    return next(v for k, v in ps if k == best)


def _wavg(vals):
    ps = _pairs(vals)
    total = sum(w for _v, w in ps)
    if not ps or total == 0:
        return None
    return sum(v * w for v, w in ps) / total


_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _day_of_week(vals, forward):
    if vals[0] is None:
        return None
    d = _dt(vals[0])
    want = str(vals[1]).strip().lstrip("$").capitalize()
    if want not in _DAYS:
        raise Unsupported(f"{want!r} is not a day name this oracle recognises")
    target = _DAYS.index(want) + 1
    delta = (d.isoweekday() - target) % 7 or 7      # STRICTLY previous, never the same day
    return (d - _timedelta(days=delta)).strftime("%Y-%m-%d")


def _format_date(vals):
    if vals[0] is None:
        return None
    fmt = str(vals[1])
    # The two NAMED formats relational execution accepts. Every dialect that implements
    # formatDate offers exactly these two and rejects anything else with "Unsupported
    # DateFormat", so a format PATTERN like 'yyyy-MM-dd' never reaches SQL at all.
    #
    # Implementing them is not deference: ISO 8601 is a published standard and its rendering
    # of a date is '2024-06-03' whoever writes the code. The nanosecond variant is spelled
    # out to nine fractional digits by the same standard.
    if fmt == "ISO8601":
        return _dt(vals[0]).strftime("%Y-%m-%d")
    if fmt == "ISO8601_NanoSecondPrecision":
        return _dt(vals[0]).strftime("%Y-%m-%dT%H:%M:%S.%f") + "000"
    # Otherwise only the strftime-compatible subset. A format string is a little language,
    # and guessing at the rest would mean asserting a translation nobody wrote down.
    if any(c in fmt for c in "[]{}"):
        raise Unsupported(f"date format {fmt!r} is outside the strftime subset implemented")
    return _dt(vals[0]).strftime(fmt)


def _time_bucket(vals):
    if vals[0] is None:
        return None
    d, n, unit = _dt(vals[0]), int(vals[1]), str(vals[2]).upper().lstrip("$")
    if unit not in _FIXED_UNITS:
        raise Unsupported(f"timeBucket by {unit} needs a fixed-length unit")
    from datetime import datetime
    epoch = datetime(1970, 1, 1)
    span = _FIXED_UNITS[unit] * 86400 * n
    secs = (d - epoch).total_seconds()
    return (epoch + _timedelta(seconds=secs - (secs % span))).strftime("%Y-%m-%d %H:%M:%S")


def _pad(vals, left):
    if any(v is None for v in vals[:2]):
        return None
    s, width = str(vals[0]), int(vals[1])
    fill = str(vals[2]) if len(vals) > 2 and vals[2] is not None else " "
    if len(s) >= width or not fill:
        return s
    pad = (fill * width)[:width - len(s)]
    return pad + s if left else s + pad


def _split_part(vals):
    if any(v is None for v in vals[:3]):
        return None
    parts = str(vals[0]).split(str(vals[1]))
    i = int(vals[2])
    return parts[i - 1] if 1 <= i <= len(parts) else None


def _num(v):
    """A numeric argument, or None. Booleans are excluded deliberately: Python's bool is an
    int, so `abs(True)` would silently answer 1 rather than refusing a nonsense call."""
    if isinstance(v, bool) or v is None:
        return None
    return v


def _propagating(f, arity=None):
    """Wrap a total function so ANY null argument yields null -- the SQL scalar rule."""
    def impl(vals):
        if arity is not None and len(vals) != arity:
            raise Unsupported(f"expected {arity} argument(s), got {len(vals)}")
        if any(v is None for v in vals):
            return None
        return f(*vals)
    return impl


def _variadic_num(f, identity):
    """plus/times over any arity, NULL-propagating."""
    def impl(vals):
        if any(v is None for v in vals):
            return None
        out = identity
        for v in vals:
            out = f(out, v)
        return out
    return impl


def _guarded(f, ok, why, arity=1):
    """A function that is well defined on PART of its domain, refusing only outside it.

    Refusing the whole function was an overcorrection. `sqrt` has one agreed answer for every
    non-negative input; only the negatives are contested. Since the seed controls its own
    inputs, refusing the ambiguous REGION keeps the function testable and keeps the corpus
    honest at the same time -- and if a negative ever does reach it, the build fails loudly
    rather than asserting whichever answer this implementation happened to choose.
    """
    def impl(vals):
        if len(vals) != arity:
            raise Unsupported(f"expected {arity} argument(s), got {len(vals)}")
        if any(v is None for v in vals):
            return None
        if not ok(*vals):
            raise Unsupported(f"{f.__name__ if hasattr(f, '__name__') else 'function'}"
                              f"{tuple(vals)}: {why}. The seed must not produce it.")
        return f(*vals)
    return impl


class _Dbl(float):
    """A float that came out of a division, or out of arithmetic on one.

    A tag rather than a wrapper: it IS a float everywhere -- comparisons, JSON, formatting,
    further arithmetic -- and the only thing that reads the tag is `*`, which uses it to
    decide whether the engine would have been in decimal or in double at that point.
    """


def _exact_addsub(op, a, b):
    """Add, subtract or multiply the way a DECIMAL column does.

    Two DECIMAL(18,4) marks 108.7500 and 107.9000 differ by 0.85 exactly in the database and
    by 0.8499999999999943 in binary floating point. Same lesson as _exact_sum, one level
    down: the earlier fix covered the `sum` aggregate and left the arithmetic inside derived
    expressions alone, so it reappeared the moment a derived property subtracted one mark
    from another.

    Multiplication was left out of the first version on the grounds that the corpus's money
    multiplications were already agreeing. They were, by luck: every factor in the corpus at
    the time was a sum of powers of two. A confidence score of 0.5500 scaled to a percentage
    is not, and 0.55 * 100 is 55.00000000000001 in binary floating point against 55.0000 in
    the column -- so `*` is here now, on the same reasoning as `+`.

    Division still is not, and it is contagious. It raises a scale question a column type
    does not answer -- what precision should 1/3 have -- so the engine drops to double the
    moment one appears, and everything computed FROM that result is a double too. Three
    services proved it: `faceValue * couponRate / 100.0 * fxRate` gives 2.9625000000000004
    from the engine, not the 2.9625 exact decimal would produce, because the `/ 100.0` in the
    middle made the left operand of the last multiply a double. `_Dbl` below is the tag that
    carries that, and `*` honours it.

    Addition and subtraction do NOT check the tag. They should, by the same argument -- but
    every one of them agrees with the engine today, and widening a rule past its evidence is
    how the multiplication case got mis-scoped in the first place. When a sum after a
    division disagrees, that is the moment to extend this, with the disagreement as the
    reason.
    """
    from decimal import Decimal, InvalidOperation
    plain = {"+": lambda x, y: x + y, "-": lambda x, y: x - y, "*": lambda x, y: x * y}[op]
    # A 4-byte column stays 4-byte through arithmetic. `25.0 - pct` over a FLOAT column is
    # computed in single precision and printed as -4.799999, not -4.799999237060547 -- so
    # the narrowing has to survive the operation rather than only the read. Same shape as
    # the _Dbl division taint below, and checked first because F32 is the narrower claim.
    import flat
    if isinstance(a, flat.F32) or isinstance(b, flat.F32):
        return flat.F32(plain(float(a), float(b)))
    # An 8-byte DOUBLE column computes in BINARY, not decimal. This is the extension the
    # docstring above anticipated, arrived at the way it asked for: a subtraction disagreed.
    # `1.0710 - 1.0708` over two DOUBLE columns is 0.00019999999999997797 from the engine
    # and 0.0002 from exact decimal. DECIMAL columns are untouched and still exact.
    if isinstance(a, flat.F64) or isinstance(b, flat.F64):
        return flat.F64(plain(float(a), float(b)))
    if op == "*" and (isinstance(a, _Dbl) or isinstance(b, _Dbl)):
        return _Dbl(plain(a, b))
    if isinstance(a, bool) or isinstance(b, bool):
        return plain(a, b)
    try:
        da, db = Decimal(str(a)), Decimal(str(b))
    except (InvalidOperation, TypeError, ValueError):
        return plain(a, b)
    out = plain(da, db)
    return int(out) if isinstance(a, int) and isinstance(b, int) else float(out)


def _exact_avg(vals):
    """Average the way AVG over a DECIMAL column does: divide in decimal, then narrow.

    Not sum/count in double. Three CPI prints of 3.4, 3.3 and 3.0 average to
    3.2333333333333334 from the engine and to 3.233333333333333 from a double division of
    the same exact sum -- one ulp apart, because the engine divides the exact decimal 9.7 by
    3 and narrows the RESULT, where a double division narrows first and divides after.

    Both readings agree on the fourteen-element commodity series, so the one-ulp case is the
    only evidence there is; it is enough, and it points one way.
    """
    from decimal import Decimal, InvalidOperation

    try:
        total = sum((Decimal(str(v)) for v in vals), Decimal(0))
        return _Dbl(float(total / Decimal(len(vals))))
    except (InvalidOperation, TypeError, ValueError):
        return _Dbl(sum(vals) / len(vals))


def _exact_sum(vals):
    """Sum the way a DECIMAL column sums, not the way a float does.

    Adding sixty coupons of 11.88 gives 712.8000000000001 in binary floating point and
    712.80 in the database, because the column is DECIMAL(18,2) and the database adds
    decimals exactly. The difference is invisible on a three-row fan-out and appears the
    moment a real coupon schedule is aggregated -- which is what a thirty-year bond paying
    semi-annually is for.

    Summed as Decimal when every value is expressible as one, and left alone otherwise, so
    a genuinely floating-point column (a price, a rate) is not silently rounded. This is not
    deference to the engine: money adds exactly, and a corpus that sums it in binary is the
    one that is wrong.
    """
    from decimal import Decimal, InvalidOperation
    if not vals:
        return 0
    try:
        total = sum((Decimal(str(v)) for v in vals), Decimal(0))
    except (InvalidOperation, TypeError):
        return sum(vals)
    as_float = float(total)
    return int(total) if all(isinstance(v, int) for v in vals) else as_float


def _agg(f, empty=None):
    """An aggregate over a COLLECTION argument: the single argument is a list."""
    def impl(vals):
        xs = vals[0] if len(vals) == 1 and isinstance(vals[0], list) else vals
        present = [x for x in xs if x is not None]
        return f(present) if present else empty
    return impl


IMPL = {
    # ---- string ----------------------------------------------------------------------
    # concat over NULL yields the OTHER argument, and over ALL-NULL the EMPTY STRING.
    # Dialect-dependent, and this corpus executes on the function dialects (DuckDB); see the
    # note above. "Ignores NULL" taken to its conclusion: concatenating nothing is "".
    "concat": lambda vals: "".join(str(v) for v in vals if v is not None),
    "toUpper": _propagating(lambda s: str(s).upper(), 1),
    "toLower": _propagating(lambda s: str(s).lower(), 1),
    "trim": _propagating(lambda s: str(s).strip(), 1),
    "ltrim": _propagating(lambda s: str(s).lstrip(), 1),
    "rtrim": _propagating(lambda s: str(s).rstrip(), 1),
    "length": _propagating(lambda s: len(str(s)), 1),
    "reverseString": _propagating(lambda s: str(s)[::-1], 1),
    "ascii": _propagating(lambda s: ord(str(s)[0]) if str(s) else None, 1),
    "char": _propagating(lambda n: chr(int(n)), 1),
    "startsWith": _propagating(lambda s, p: str(s).startswith(str(p)), 2),
    "endsWith": _propagating(lambda s, p: str(s).endswith(str(p)), 2),
    "contains": _propagating(lambda s, p: str(p) in str(s), 2),
    "isAlphaNumeric": _propagating(lambda s: str(s).isalnum(), 1),
    "repeatString": _propagating(lambda s, n: str(s) * int(n), 2),
    "parseBoolean": _propagating(lambda s: str(s).strip().lower() == "true", 1),
    "parseInteger": _propagating(lambda s: int(str(s).strip()), 1),
    "parseFloat": _propagating(lambda s: float(str(s).strip()), 1),
    "parseDecimal": _propagating(lambda s: float(str(s).strip().rstrip("dD")), 1),

    # ---- null-inspecting: TOTAL, never return NULL themselves -------------------------
    "isNull": lambda vals: vals[0] is None,
    "isNotNull": lambda vals: vals[0] is not None,
    "isEmpty": lambda vals: vals[0] is None,
    "isNotEmpty": lambda vals: vals[0] is not None,
    "coalesce": lambda vals: next((v for v in vals if v is not None), None),

    # ---- boolean ---------------------------------------------------------------------
    "not": _propagating(lambda b: not b, 1),
    "equal": lambda vals: _cmp("==", vals[0], vals[1]),
    "eq": lambda vals: _cmp("==", vals[0], vals[1]),
    "greaterThan": lambda vals: _cmp(">", vals[0], vals[1]),
    "greaterThanEqual": lambda vals: _cmp(">=", vals[0], vals[1]),
    "lessThan": lambda vals: _cmp("<", vals[0], vals[1]),
    "lessThanEqual": lambda vals: _cmp("<=", vals[0], vals[1]),
    # Membership. NULL is a member of nothing, and NULLs inside the list are ignored --
    # SQL's `x IN (a, b)` is UNKNOWN when x is NULL, and a predicate that is not TRUE fails.
    "in": lambda vals: (False if vals[0] is None else
                        vals[0] in [h for h in (vals[1] or []) if h is not None]),

    # ---- arithmetic: NULL propagates, as through every SQL scalar operator ------------
    # These are the SAFE ones: `a + b` means the same in every dialect this corpus reaches,
    # so an independent implementation is not a bet about lowering.
    "plus": _variadic_num(lambda a, b: a + b, 0),
    "times": _variadic_num(lambda a, b: a * b, 1),
    "minus": lambda vals: (None if any(v is None for v in vals)
                           else (-vals[0] if len(vals) == 1 else vals[0] - sum(vals[1:]))),
    "abs": _propagating(lambda v: abs(v), 1),
    "sign": _propagating(lambda v: 0 if v == 0 else (1 if v > 0 else -1), 1),
    "ceiling": _propagating(lambda v: math.ceil(v), 1),
    "floor": _propagating(lambda v: math.floor(v), 1),
    "exp": _propagating(exactmath.exp, 1),
    "pow": _propagating(exactmath.pow, 2),
    "toDecimal": _propagating(lambda v: float(v), 1),
    "toFloat": _propagating(lambda v: float(v), 1),
    "toString": _propagating(lambda v: str(v), 1),
    # math.cbrt, not `x ** (1/3)`. The exponent form is inexact by construction -- 1/3 is
    # not representable, so it computes a slightly wrong power of a correct base and lands
    # one ulp out (362.5 -> ...043 where the true cube root is ...044). This is not deferring
    # to the engine's answer; it is that a cube root computed as a cube root is right and a
    # cube root computed as a fractional power is approximately right.
    "cbrt": _propagating(exactmath.cbrt, 1),

    # ---- functions belonging to a LINKED PROJECT ----
    #
    # Implemented here from the project's own source, not read back from the engine -- the
    # same rule every other entry follows, and the reason the registry refuses an unknown
    # name rather than guessing. A cross-project call is exactly where reading the engine's
    # answer would be most tempting and most circular: the thing under test IS whether the
    # call lowers correctly across the boundary.
    #
    # `core_fx::convert(amount, rate) { $amount * $rate }`
    #
    # PLAIN float multiply, not the exact-decimal one the corpus uses for column arithmetic.
    # The exactness rule follows the COLUMN type: a DECIMAL column multiplies exactly, and
    # these operands come from DOUBLE columns through a function whose parameters are
    # declared Float. Multiplying exactly gave 204006.45 against the engine's
    # 204006.44999999998 -- the oracle being more precise than the thing it models.
    "core_fx::convert": _propagating(lambda a, r: a * r, 2),
    # `core_fx::invert(rate) { 1.0 / $rate }`
    "core_fx::invert": _propagating(lambda r: None if r == 0 else _Dbl(1.0 / r), 1),
    # `core_fx::midOf(bid, ask) { ($bid + $ask) / 2.0 }`
    "core_fx::midOf": _propagating(
        lambda b, a: _Dbl(float(_exact_addsub("+", b, a)) / 2.0), 2),

    # `core_types::ctBasisPointsToRate(bp) { $bp / 10000.0 }`
    #
    # core-types is reached TRANSITIVELY: the corpus links fee-core, whose FeeSchedule calls
    # this, and core-types is linked only because fee-core needs it. So this entry is the
    # first oracle implementation of a function belonging to a project the corpus does not
    # itself depend on -- which is precisely the position a real consumer is in, having to
    # reimplement a fact it inherited without asking for it.
    #
    # `_Dbl` because `/` yields a Float whatever the operands were, and the corpus's
    # exact-decimal path must not claim more precision than the engine has.
    "core_types::ctBasisPointsToRate": _propagating(lambda bp: _Dbl(float(bp) / 10000.0), 1),
    # `core_types::ctMinorUnits(ccy)` -- ISO 4217 minor-unit digits, defaulting to 2. The
    # membership lists are core-types' own, transcribed rather than inferred.
    "core_types::ctMinorUnits": _propagating(
        lambda ccy: (0 if ccy in ("JPY", "KRW", "CLP", "ISK", "VND")
                     else 3 if ccy in ("KWD", "BHD", "JOD", "OMR", "TND") else 2), 1),
    # Trigonometry: total over the reals, so no domain guard is needed.
    # Well defined on part of the domain, refusing only outside it -- see _guarded.
    "sqrt": _guarded(math.sqrt, lambda v: v >= 0,
                     "sqrt of a negative has no agreed answer"),
    "log": _guarded(exactmath.log, lambda v: v > 0, "log is undefined at zero and below"),
    "log10": _guarded(exactmath.log10, lambda v: v > 0, "log10 is undefined at zero and below"),
    "acos": _guarded(exactmath.acos, lambda v: -1 <= v <= 1, "acos is defined on [-1, 1]"),
    "asin": _guarded(exactmath.asin, lambda v: -1 <= v <= 1, "asin is defined on [-1, 1]"),
    "cot": _guarded(exactmath.cot, lambda v: abs(math.sin(v)) > 1e-9,
                    "cot is undefined where sin is zero"),
    "divide": _guarded(lambda a, b: a / b, lambda a, b: b != 0,
                       "division by zero is an error in some dialects and NULL in others",
                       arity=2),
    # mod/rem disagree between dialects only on NEGATIVE operands, so those are refused and
    # the non-negative case -- which is unambiguous -- is implemented.
    "mod": _guarded(lambda a, b: int(a) % int(b), lambda a, b: a >= 0 and b > 0,
                    "the sign of the result for negative operands differs by dialect",
                    arity=2),
    "rem": _guarded(lambda a, b: math.fmod(a, b), lambda a, b: a >= 0 and b > 0,
                    "the sign of the result for negative operands differs by dialect",
                    arity=2),
    # A half-way value rounds half-up in some dialects and half-even in others; every other
    # value has one answer.
    "round": _guarded(lambda v: int(math.floor(v + 0.5)),
                      lambda v: abs(v - math.floor(v) - 0.5) > 1e-9,
                      "a half-way value rounds half-up in some dialects and half-even in "
                      "others"),
    "sin": _propagating(exactmath.sin, 1), "cos": _propagating(exactmath.cos, 1),
    "tan": _propagating(exactmath.tan, 1), "sinh": _propagating(exactmath.sinh, 1),
    "cosh": _propagating(exactmath.cosh, 1), "tanh": _propagating(exactmath.tanh, 1),
    "atan": _propagating(exactmath.atan, 1), "atan2": _propagating(exactmath.atan2, 2),

    # ---- date and time ---------------------------------------------------------------
    #
    # A date arrives as text -- "2024-06-03" or "2024-06-03 19:00:00" -- because that is what
    # the seed writes and what the CSV carries, so each of these parses, computes, and
    # renders back in the same shape it received. Deliberately no timezone arithmetic: every
    # value in this corpus is naive, and a function that silently assumed UTC would be
    # asserting a timezone the data does not carry.
    "year": _propagating(lambda d: _dt(d).year, 1),
    "monthNumber": _propagating(lambda d: _dt(d).month, 1),
    "dayOfMonth": _propagating(lambda d: _dt(d).day, 1),
    "dayOfYear": _propagating(lambda d: _dt(d).timetuple().tm_yday, 1),
    # ISO weekday: Monday is 1. Stated because the alternative convention (Sunday first) is
    # equally common and the signature does not say which.
    # Sunday=1 .. Saturday=7, NOT ISO. The signature does not fix a convention and I picked
    # ISO from habit; the engine picked the other one and says so where it lowers -- the
    # DuckDB extension writes `dayofweek(d) + 1` with the comment "(Sunday = 0, Saturday = 6)
    # >> we need from 1 to 7". A stated convention beats an assumed one, so this follows the
    # engine here. It is the `concat` lesson again: where behaviour is a choice rather than a
    # consequence, an implementation written from what the NAME means is a guess.
    "dayOfWeekNumber": _propagating(lambda d: _dt(d).isoweekday() % 7 + 1, 1),
    "weekOfYear": _propagating(lambda d: _dt(d).isocalendar()[1], 1),
    "quarterNumber": _propagating(lambda d: (_dt(d).month - 1) // 3 + 1, 1),
    "hour": _propagating(lambda d: _dt(d).hour, 1),
    "minute": _propagating(lambda d: _dt(d).minute, 1),
    "second": _propagating(lambda d: _dt(d).second, 1),
    # `datePart` strips the time; the first-of-period family truncates upward from there.
    "datePart": _propagating(lambda d: _dt(d).strftime("%Y-%m-%d"), 1),
    "firstDayOfMonth": _propagating(
        lambda d: _dt(d).replace(day=1).strftime("%Y-%m-%d"), 1),
    "firstDayOfYear": _propagating(
        lambda d: _dt(d).replace(month=1, day=1).strftime("%Y-%m-%d"), 1),
    "firstDayOfQuarter": _propagating(
        lambda d: _dt(d).replace(month=(_dt(d).month - 1) // 3 * 3 + 1,
                                 day=1).strftime("%Y-%m-%d"), 1),
    # Week starts MONDAY, matching the ISO weekday above; the two must agree or a caller
    # combining them gets an off-by-one that only shows on Sundays.
    "firstDayOfWeek": _propagating(
        lambda d: (_dt(d) - _timedelta(days=_dt(d).isoweekday() - 1)).strftime("%Y-%m-%d"), 1),
    "firstHourOfDay": _propagating(
        lambda d: _dt(d).replace(hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S"), 1),
    "firstMinuteOfHour": _propagating(
        lambda d: _dt(d).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S"), 1),
    "firstSecondOfMinute": _propagating(
        lambda d: _dt(d).replace(second=0).strftime("%Y-%m-%d %H:%M:%S"), 1),
    "date": lambda vals: (None if any(v is None for v in vals) else
                          "%04d-%02d-%02d" % (vals[0], vals[1], vals[2])
                          if len(vals) == 3 else None),

    # ---- bitwise: exact on integers, with no dialect freedom -------------------------
    "bitAnd": _propagating(lambda a, b: int(a) & int(b), 2),
    "bitOr": _propagating(lambda a, b: int(a) | int(b), 2),
    "bitXor": _propagating(lambda a, b: int(a) ^ int(b), 2),
    "bitNot": _propagating(lambda a: ~int(a), 1),
    "bitShiftLeft": _propagating(lambda a, b: int(a) << int(b), 2),
    "bitShiftRight": _propagating(lambda a, b: int(a) >> int(b), 2),

    # ---- string, continued -----------------------------------------------------------
    "left": _guarded(lambda s, n: str(s)[:int(n)], lambda s, n: n >= 0,
                     "a negative width has no agreed meaning", arity=2),
    "right": _guarded(lambda s, n: str(s)[-int(n):] if int(n) else "",
                      lambda s, n: n >= 0,
                      "a negative width has no agreed meaning", arity=2),
    "joinStrings": lambda vals: (None if vals[0] is None else
                                 str(vals[-1]).join(str(x) for x in vals[0]
                                                    if x is not None)),
    "encodeBase64": _propagating(
        lambda s: __import__("base64").b64encode(str(s).encode()).decode(), 1),
    "decodeBase64": _propagating(
        lambda s: __import__("base64").b64decode(str(s)).decode(), 1),
    "levenshteinDistance": _propagating(lambda a, b: _levenshtein(str(a), str(b)), 2),

    # ---- boolean over a collection ---------------------------------------------------
    # `and`/`or` over an EMPTY collection are the identities: all-of-nothing is true,
    # any-of-nothing is false. Stated because it is the case a caller forgets.
    "and": lambda vals: all(_flatten(vals)),
    "or": lambda vals: any(_flatten(vals)),
    "between": lambda vals: (None if any(v is None for v in vals[:3]) else
                             vals[1] <= vals[0] <= vals[2]),

    # ---- collection: shape operations over a list ------------------------------------
    #
    # These take a COLLECTION and return one, so they are only meaningful where the corpus
    # already has a to-many hop. Order-dependent members (first, last, take, drop) are
    # implemented against the order the seed writes, which is the only order the corpus
    # controls -- a query that relies on them without an explicit sort is relying on the
    # database, and this oracle would then be asserting the database's choice.
    "size": lambda vals: len(_coll(vals)),
    "first": lambda vals: next(iter(_coll(vals)), None),
    "last": lambda vals: (_coll(vals) or [None])[-1],
    "init": lambda vals: _coll(vals)[:-1],
    "tail": lambda vals: _coll(vals)[1:],
    "reverse": lambda vals: list(reversed(_coll(vals))),
    "take": lambda vals: _coll(vals)[:int(vals[1])],
    "drop": lambda vals: _coll(vals)[int(vals[1]):],
    "limit": lambda vals: _coll(vals)[:int(vals[1])],
    "slice": lambda vals: _coll(vals)[int(vals[1]):int(vals[2])],
    "paginated": lambda vals: _coll(vals)[int(vals[1]):int(vals[1]) + int(vals[2])],
    "concatenate": lambda vals: _coll([vals[0]]) + _coll([vals[1]]),
    "union": lambda vals: _coll([vals[0]]) + _coll([vals[1]]),
    "add": lambda vals: _coll([vals[0]]) + [vals[1]],
    # `distinct` and `removeDuplicates` keep the FIRST occurrence, which is the only
    # order-preserving reading; a set would lose the order the seed established.
    "distinct": lambda vals: _dedupe(_coll(vals)),
    "removeDuplicates": lambda vals: _dedupe(_coll(vals)),
    "isDistinct": lambda vals: len(_dedupe(_coll(vals))) == len(_coll(vals)),
    "sort": lambda vals: sorted(_coll(vals), key=_sort_key),
    "range": lambda vals: list(range(int(vals[0]), int(vals[1]),
                                     int(vals[2]) if len(vals) > 2 else 1)),
    "list": lambda vals: _coll(vals),
    # A Map is a list of pairs here, because that is all the corpus ever builds.
    "newMap": lambda vals: dict(_coll(vals)),
    "keys": lambda vals: list(vals[0].keys()) if isinstance(vals[0], dict) else [],
    "values": lambda vals: list(vals[0].values()) if isinstance(vals[0], dict) else [],
    "get": lambda vals: (vals[0] or {}).get(vals[1]),
    "put": lambda vals: {**(vals[0] or {}), vals[1]: vals[2]},
    "putAll": lambda vals: {**(vals[0] or {}), **dict(_coll([vals[1]]))},

    # ---- date, continued -------------------------------------------------------------
    "month": _propagating(lambda d: _dt(d).month, 1),
    "dayOfWeek": _propagating(lambda d: _dt(d).strftime("%A"), 1),
    "firstMillisecondOfSecond": _propagating(
        lambda d: _dt(d).strftime("%Y-%m-%d %H:%M:%S"), 1),
    # `dateDiff` in whole units of the named duration. Only the units whose length is FIXED
    # are implemented: a month and a year vary, so "how many months between" depends on a
    # convention the signature does not state.
    "dateDiff": lambda vals: _date_diff(vals),
    "adjust": lambda vals: _adjust(vals),

    # ---- language: conditionals, casts and multiplicity ------------------------------
    #
    # `if` is LAZY in Pure -- its branches are functions, not values -- but by the time a
    # value reaches this oracle both branches are already evaluated, so laziness cannot be
    # observed here. That is a limit of the harness rather than of the implementation, and
    # it is stated so nobody reads a passing `if` test as evidence about short-circuiting.
    "if": lambda vals: vals[1] if vals[0] else vals[2],
    "cast": lambda vals: vals[0],
    "subType": lambda vals: vals[0],
    "whenSubType": lambda vals: vals[0],
    "extractEnumValue": lambda vals: vals[1],
    # Multiplicity assertions: they narrow a TYPE without changing a value, and they FAIL
    # when the collection cannot satisfy the bound. Returning the value regardless would
    # make them look total when their whole purpose is to be partial.
    "toOne": lambda vals: _to_one(vals),
    "toOneMany": lambda vals: _to_one_many(vals),

    # ---- variant: JSON in and out ----------------------------------------------------
    #
    # Deliberately independent of the engine's serializer. F27 is exactly what happens when
    # a value crosses this boundary and the two sides disagree about what came back.
    "toJson": lambda vals: __import__("json").dumps(vals[0], sort_keys=True,
                                                    separators=(",", ":")),
    "fromJson": _propagating(lambda s: __import__("json").loads(str(s)), 1),
    # A Variant is a JSON value, so converting a String produces the JSON TOKEN -- quotes
    # included. `toVariant('alpha')` is `"alpha"`, five characters plus two. Passing the bare
    # string through was treating a Variant as a synonym for its contents, which it is not:
    # the same reasoning as F27, where a Binding returns the raw JSON token rather than the
    # declared type.
    "toVariant": lambda vals: (None if vals[0] is None else json.dumps(vals[0])),
    "to": lambda vals: vals[0],
    "toMany": lambda vals: _coll(vals),

    # ---- collection: the higher-order and retrieval forms ----------------------------
    #
    # These take a FUNCTION, so the oracle receives the already-applied result rather than a
    # lambda -- the harness evaluates arguments before dispatch. That is a real limit and it
    # is stated: a passing `map` test says the projection was right, not that laziness or
    # evaluation order was.
    "map": lambda vals: [vals[1](x) for x in _coll(vals)] if callable(vals[1])
                        else _coll(vals),
    "filter": lambda vals: [x for x in _coll(vals) if vals[1](x)] if callable(vals[1])
                           else _coll(vals),
    "exists": lambda vals: any(vals[1](x) for x in _coll(vals)) if callable(vals[1])
                           else bool(_coll(vals)),
    "sortBy": lambda vals: sorted(_coll(vals),
                                  key=lambda x: _sort_key(vals[1](x) if callable(vals[1])
                                                          else x)),
    "sortByReversed": lambda vals: sorted(
        _coll(vals), key=lambda x: _sort_key(vals[1](x) if callable(vals[1]) else x),
        reverse=True),
    # `getAll` and its milestoned variants are RETRIEVAL, not computation: they name the row
    # set a query starts from. The oracle already builds that set in _rows_for, so these are
    # identity over it rather than a second implementation that could disagree with the
    # first.
    "getAll": lambda vals: _coll(vals),
    "getAllVersions": lambda vals: _coll(vals),
    "getAllVersionsInRange": lambda vals: _coll(vals),
    "getAllForEachDate": lambda vals: _coll(vals),
    "objectReferenceIn": lambda vals: (False if vals[0] is None
                                       else vals[0] in _coll([vals[1]])),

    # ---- statistics over a collection ------------------------------------------------
    #
    # The POPULATION and SAMPLE forms differ only in the divisor -- n against n-1 -- and that
    # is exactly the kind of difference a corpus using one of them can never catch in the
    # other. Both are implemented, and the sample forms refuse a single observation rather
    # than dividing by zero.
    "median": _agg(lambda xs: _median(xs)),
    "mode": _agg(lambda xs: max(set(xs), key=xs.count)),
    "variancePopulation": _agg(lambda xs: _variance(xs, 0)),
    "varianceSample": _agg(lambda xs: _variance(xs, 1)),
    "variance": lambda vals: _variance_flagged(vals),
    "stdDevPopulation": _agg(lambda xs: _variance(xs, 0) ** 0.5),
    "stdDevSample": _agg(lambda xs: _variance(xs, 1) ** 0.5),
    "covarPopulation": lambda vals: _covar(vals, 0),
    "covarSample": lambda vals: _covar(vals, 1),
    "corr": lambda vals: _corr(vals),
    "percentile": lambda vals: _percentile(vals),
    # maxBy/minBy: the extremum of one collection SELECTED BY another. A tie takes the
    # FIRST, which is a choice -- stated because the alternative (last) is equally defensible
    # and the signature settles neither.
    "maxBy": lambda vals: _by(vals, max),
    "minBy": lambda vals: _by(vals, min),
    "wavg": lambda vals: _wavg(vals),
    # Pair constructors for the aggregate forms above: they carry a value and its weight.
    "rowMapper": lambda vals: (vals[0], vals[1]),
    "wavgRowMapper": lambda vals: (vals[0], vals[1]),
    "flatten": lambda vals: [x for v in _coll(vals) for x in (v if isinstance(v, list)
                                                              else [v])],

    # ---- date, the remaining forms ---------------------------------------------------
    # `firstDayOfThis*` read a clock, so they are refused below rather than implemented; the
    # DAY-OF-WEEK navigation is pure arithmetic and is implemented here.
    "previousDayOfWeek": lambda vals: _day_of_week(vals, forward=False),
    "mostRecentDayOfWeek": lambda vals: _day_of_week(vals, forward=False),
    "formatDate": lambda vals: _format_date(vals),
    "timeBucket": lambda vals: _time_bucket(vals),

    # ---- conventions STATED rather than refused ---------------------------------------
    #
    # These were refused because the signature does not fix a convention -- where a string
    # index starts, what happens when a pad width is shorter than the input, how NULL behaves
    # in greatest/least. Refusing was the wrong call: a convention I can WRITE DOWN and then
    # TEST is not a guess, because the corpus executes it and a wrong choice shows up as a
    # failing service rather than as a silent agreement.
    #
    # That is exactly how `concat` was corrected: an implementation stated from meaning, a
    # test that disagreed, and a rule sharpened as a result. Refusing would have left the
    # behaviour unknown instead.
    #
    # Each states its convention, so a failure says WHICH assumption was wrong.
    #
    # substring/indexOf are ONE-BASED and the end index is EXCLUSIVE.
    "substring": lambda vals: (None if vals[0] is None else
                               str(vals[0])[int(vals[1]) - 1:
                                            int(vals[2]) - 1 if len(vals) > 2 else None]),
    "indexOf": lambda vals: (None if any(v is None for v in vals[:2]) else
                             str(vals[0]).find(str(vals[1])) + 1),
    # Pad to the width; a string ALREADY at least that long is returned unchanged rather
    # than truncated.
    "lpad": lambda vals: _pad(vals, left=True),
    "rpad": lambda vals: _pad(vals, left=False),
    # splitPart is ONE-BASED; an out-of-range part is NULL rather than the empty string.
    "splitPart": lambda vals: _split_part(vals),
    # replace is non-overlapping, left to right -- Python's str.replace, which is also what
    # every SQL REPLACE does.
    "replace": _propagating(lambda s, a, b: str(s).replace(str(a), str(b)), 3),
    # greatest/least IGNORE nulls and return null only when everything is null, matching the
    # aggregate convention already used by max/min above. Stated because the propagating
    # reading is equally common.
    "greatest": _agg(max), "least": _agg(min),

    # ---- aggregates over a collection ------------------------------------------------
    # The EMPTY case is stated, not left to fall out: sum of nothing is 0, but max of
    # nothing is absent rather than 0, and conflating the two is how an empty group starts
    # reporting a value it does not have.
    "sum": _agg(_exact_sum, empty=0),
    "max": _agg(max), "min": _agg(min),
    "average": _agg(_exact_avg),
    "count": lambda vals: len([v for v in (vals[0] if isinstance(vals[0], list) else vals)]),
}

# Deliberately NOT implemented, each with the reason. A refusal is an answer: it says the
# corpus looked at the function and decided it could not asserted honestly, which is
# different from nobody having looked.

# ---------------------------------------------------------------------------------------
# Calendar aggregations.
#
# `cw_Date_$0_1$__String_1__Date_1__Number_$0_1$__Number_$0_1$_` -- four parameters and a
# `Number[0..1]` RETURN. That trailing type is what makes these implementable, and I had
# miscounted it as a fifth parameter and refused the whole family on the strength of the
# miscount. A per-row optional Number cannot be an aggregate: the function emits this row's
# value or emits nothing, and whatever aggregation follows is the caller's. So the only
# unknown left is the WINDOW, and a window is a convention that can be stated.
#
# Stated convention, in full, because every one of these is a guess that a failing test is
# welcome to correct:
#
#   * The calendar NAME is ignored. Every day is a business day; there are no holidays.
#   * Weeks start on Monday, matching ISO, not the Sunday=1 that `dayOfWeekNumber` uses --
#     those are different questions and the corpus should not assume one answers the other.
#   * The fiscal year is the calendar year.
#   * Every window is INCLUSIVE at both ends.
#   * "to date" means up to and including the reference date; a "prior" window is the same
#     span shifted back one period, not the whole prior period.
#
# What this buys is the thing a refusal cannot: a disagreement. If the engine's `pwtd` runs
# Sunday-to-Saturday, or excludes its right end, the test says so and names the function.
# Refusing kept the corpus silent about thirty functions on the grounds that it might be
# wrong about them, which is a standard that would have refused `concat` and `substring` too
# -- and being wrong about substring is how F37 was found.
def _shift_years(d, n):
    try:
        return d.replace(year=d.year - n)
    except ValueError:                      # 29 February in a non-leap target year
        return d.replace(year=d.year - n, day=28)


def _week_start(d):
    return d - _timedelta(days=d.weekday())


def _cal_window(name, end):
    """(first, last) of the window `name` relative to the reference date `end`."""
    q_first = _dt(end).replace(month=((end.month - 1) // 3) * 3 + 1, day=1)
    m_first = end.replace(day=1)
    y_first = end.replace(month=1, day=1)
    ws = _week_start(end)
    return {
        "ytd": (y_first, end),
        "mtd": (m_first, end),
        "qtd": (q_first, end),
        "wtd": (ws, end),
        "cw": (ws, ws + _timedelta(days=6)),
        "cw_fm": (max(ws, m_first), end),
        "cme": (m_first, (m_first + _timedelta(days=32)).replace(day=1) - _timedelta(days=1)),
        "pw": (ws - _timedelta(days=7), ws - _timedelta(days=1)),
        "pw_fm": (max(ws - _timedelta(days=7), m_first), end - _timedelta(days=7)),
        "pwtd": (ws - _timedelta(days=7), end - _timedelta(days=7)),
        "p4wtd": (ws - _timedelta(days=28), end),
        "p12wtd": (ws - _timedelta(days=84), end),
        "p52wtd": (ws - _timedelta(days=364), end),
        "pmtd": (_shift_month(m_first, 1), _shift_month(end, 1)),
        "p12mtd": (_shift_month(m_first, 12), end),
        "pqtd": (_shift_month(q_first, 3), _shift_month(end, 3)),
        "pytd": (_shift_years(y_first, 1), _shift_years(end, 1)),
        "pymtd": (_shift_years(m_first, 1), _shift_years(end, 1)),
        "pyqtd": (_shift_years(q_first, 1), _shift_years(end, 1)),
        "pywtd": (_shift_years(ws, 1), _shift_years(end, 1)),
        "priorDay": (end - _timedelta(days=1), end - _timedelta(days=1)),
        "priorYear": (_shift_years(y_first, 1),
                      _shift_years(y_first, 1).replace(month=12, day=31)),
        "CYMinus2": (_shift_years(y_first, 2),
                     _shift_years(y_first, 2).replace(month=12, day=31)),
        "CYMinus3": (_shift_years(y_first, 3),
                     _shift_years(y_first, 3).replace(month=12, day=31)),
        "reportEndDay": (end, end),
        # The averaging names select the same span as their to-date twin. The AVERAGE is not
        # this function's job -- a Number[0..1] per row cannot compute one -- so the suffix
        # describes what the caller is expected to do with the column.
        "annualized": (y_first, end),
        "pma": (_shift_month(m_first, 1), _shift_month(end, 1)),
        "pwa": (ws - _timedelta(days=7), ws - _timedelta(days=1)),
        "p4wa": (ws - _timedelta(days=28), ws - _timedelta(days=1)),
        "p12wa": (ws - _timedelta(days=84), ws - _timedelta(days=1)),
        "p52wa": (ws - _timedelta(days=364), ws - _timedelta(days=1)),
    }[name]


def _shift_month(d, n):
    """`d` moved back `n` whole months, clamped to the shorter month's last day."""
    y, m = d.year, d.month - n
    while m <= 0:
        m += 12
        y -= 1
    day = d.day
    while True:
        try:
            return d.replace(year=y, month=m, day=day)
        except ValueError:
            day -= 1


def _calendar(name):
    def impl(vals):
        date, _cal_name, end, value = (list(vals) + [None] * 4)[:4]
        if date is None or end is None:
            return None
        d, e = _dt(date).date() if hasattr(_dt(date), "date") else _dt(date), _dt(end)
        d = d.date() if hasattr(d, "date") else d
        e = e.date() if hasattr(e, "date") else e
        first, last = _cal_window(name, e)
        first = first.date() if hasattr(first, "date") else first
        last = last.date() if hasattr(last, "date") else last
        return value if first <= d <= last else None
    return impl


CALENDAR = ("annualized cme cw cw_fm CYMinus2 CYMinus3 mtd p12mtd p12wa p12wtd p4wa "
            "p4wtd p52wa p52wtd pma pmtd pqtd priorDay priorYear pw pw_fm pwa pwtd "
            "pymtd pyqtd pytd pywa pywtd qtd reportEndDay wtd ytd").split()



# ---------------------------------------------------------------------------------------
# Functions previously refused, now implemented against a STATED convention.
#
# Each refusal below was of the form "the signature does not fix X". That is true, and it is
# also true of `concat` over NULL, of `substring`'s index base, and of `dayOfWeekNumber`'s
# first day of the week -- all three of which are implemented here, and two of which produced
# findings precisely because the stated convention turned out to differ from the engine's.
# A refusal buys silence; a stated convention buys a disagreement that names the function.
_JARO_PREFIX_SCALE = 0.1        # the values from Winkler's original paper, not a choice of
_JARO_THRESHOLD = 0.7           # mine -- the metric is not defined without them


def _jaro(a, b):
    if a == b:
        return 1.0
    la, lb = len(a), len(b)
    if not la or not lb:
        return 0.0
    reach = max(la, lb) // 2 - 1
    a_hit, b_hit = [False] * la, [False] * lb
    matches = 0
    for i, ch in enumerate(a):
        for j in range(max(0, i - reach), min(lb, i + reach + 1)):
            if not b_hit[j] and b[j] == ch:
                a_hit[i] = b_hit[j] = True
                matches += 1
                break
    if not matches:
        return 0.0
    k = transposes = 0
    for i, ch in enumerate(a):
        if a_hit[i]:
            while not b_hit[k]:
                k += 1
            if ch != b[k]:
                transposes += 1
            k += 1
    transposes //= 2
    return (matches / la + matches / lb
            + (matches - transposes) / matches) / 3


def _jaro_winkler(vals):
    a, b = str(vals[0]), str(vals[1])
    j = _jaro(a, b)
    if j <= _JARO_THRESHOLD:
        return j
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return j + prefix * _JARO_PREFIX_SCALE * (1 - j)


def _java_hash_code(s):
    """Java's String.hashCode: h = 31*h + c, wrapped to a signed 32-bit int.

    "Implementation-defined" was the old reason for refusing this, and it is wrong in the way
    that matters: the algorithm is FIXED by the Java library specification, published, and
    relied upon by serialisation formats. It is defined by an implementation, which is not the
    same as being undefined.
    """
    h = 0
    for ch in str(s):
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - 0x100000000 if h >= 0x80000000 else h


def _hash(vals):
    """hash(text, algorithm). The algorithm is NAMED by the caller, which is exactly what the
    old refusal said was missing -- it is the second argument."""
    import hashlib
    if vals[0] is None:
        return None
    algo = str(vals[1]).lower().replace("-", "") if len(vals) > 1 else "md5"
    if algo not in ("md5", "sha1", "sha256"):
        raise Unsupported(f"hash algorithm {algo!r} is outside the three this implements")
    return getattr(hashlib, algo)(str(vals[0]).encode()).hexdigest()


def _regex(fn):
    """The regex family, on POSIX-ish patterns only.

    The old reason -- "regex DIALECT is the database's" -- is real but not total. Dialects
    differ at the edges: named groups, lookbehind, \\d inside a class, possessive quantifiers.
    They agree on the core, and a pattern that uses only literals, character classes,
    anchors and the three quantifiers means the same thing in every engine anyone ships.
    So this implements the core and REFUSES the edges, the same shape as `sqrt` being
    implemented while its negative-input region is refused.
    """
    import re as _re

    def impl(vals):
        if vals[0] is None:
            return None
        pattern = str(vals[1])
        if any(tok in pattern for tok in ("(?<", "(?P", "(?#", "\\p{", "*+", "++")):
            raise Unsupported(
                f"pattern {pattern!r} uses a construct whose meaning differs between regex "
                f"dialects; only the portable core is implemented")
        s = str(vals[0])
        if fn == "matches":
            return _re.fullmatch(pattern, s) is not None
        if fn == "regexpLike":
            return _re.search(pattern, s) is not None
        if fn == "regexpCount":
            return len(_re.findall(pattern, s))
        if fn == "regexpIndexOf":
            m = _re.search(pattern, s)
            return (m.start() + 1) if m else 0
        if fn == "regexpExtract":
            m = _re.search(pattern, s)
            return m.group(0) if m else None
        if fn == "regexpReplace":
            return _re.sub(pattern, str(vals[2]), s)
        raise Unsupported(fn)
    return impl


def _parse_date(vals):
    """ISO 8601, and only ISO 8601 -- stated, not guessed at.

    The old refusal said the accepted formats are not fixed by the signature. True; so this
    fixes them. A date the engine accepts and this does not produces an Unsupported naming
    the input, which is a report rather than a wrong answer.
    """
    if vals[0] is None:
        return None
    s = str(vals[0]).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m"):
        try:
            return _datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise Unsupported(f"date {s!r} is not ISO 8601, the only format this implements")


def _convert_timezone(vals):
    """The conversion is arithmetic once both zones are named, and the second argument names
    one. Every value in this corpus is naive, so the SOURCE is stated to be UTC."""
    from zoneinfo import ZoneInfo
    if vals[0] is None:
        return None
    d = _dt(vals[0])
    if d.tzinfo is None:
        d = d.replace(tzinfo=ZoneInfo("UTC"))
    return d.astimezone(ZoneInfo(str(vals[1]))).strftime("%Y-%m-%d %H:%M:%S")


def _eval(vals):
    """eval(f, ...args) applies f. The old refusal -- "the assertion belongs to whatever was
    passed" -- describes what eval DOES rather than a reason it cannot be done; applying a
    function is a computation like any other, and getting it wrong is possible."""
    f = vals[0]
    if not callable(f):
        raise Unsupported("eval's first argument is not callable in this fixture")
    return f(*vals[1:])


def _reduce(vals):
    """reduce(collection, seed, accumulator), left-to-right. Fold ORDER is the thing the
    signature leaves open, so it is stated: left, which is what every relational engine does
    to a row stream because it is the only order a stream affords."""
    import functools
    xs = _coll([vals[0]])
    seed, f = vals[1], vals[2]
    if not callable(f):
        raise Unsupported("reduce's accumulator is not callable in this fixture")
    return functools.reduce(lambda a, b: f(a, b), xs, seed)


def _pivot(vals):
    """pivot(rows, keyColumn, valueColumn): one column per distinct key.

    The old refusal was that the result's COLUMN NAMES depend on the data, so the shape is
    not knowable from the query alone. That is a reason the shape cannot be predicted
    STATICALLY -- but the oracle is not static. It holds the data, so it can compute the
    columns the same way the engine must.
    """
    rows, key, val = _rows(vals[0]), vals[1], vals[2]
    keys = _dedupe([r.get(key) for r in rows])
    return [{str(k): next((r.get(val) for r in rows if r.get(key) == k), None) for k in keys}]


def _as_of_join(vals):
    """asOfJoin(left, right, on, at): for each left row the LATEST right row at or before it.

    The tie rule the old refusal complained about is stated here: an exact timestamp match
    counts as at-or-before, so a row exactly on the boundary joins. That is the reading that
    makes "as of" mean "as of", and it is the one a failing test would correct.
    """
    left, right, keyl, keyr = _rows(vals[0]), _rows(vals[1]), vals[2], vals[3]
    out = []
    for lr in left:
        eligible = [rr for rr in right if rr.get(keyr) is not None
                    and rr[keyr] <= lr.get(keyl)]
        best = max(eligible, key=lambda rr: rr[keyr], default=None)
        out.append({**lr, **({} if best is None else best)})
    return out


def _lateral(vals):
    """lateral(rows, f): f is evaluated per row and its rows concatenated.

    The old reason -- "evaluation order of the correlated side is not observable from a row
    set" -- is an argument that one PROPERTY of it is unobservable, not that the result is.
    The result is a flat map, and a flat map is assertable.
    """
    f = vals[1]
    if not callable(f):
        raise Unsupported("lateral's correlated side is not callable in this fixture")
    return [out for r in _rows(vals[0]) for out in _rows(f(r))]



def _column_projections_from_root(rows, relation, columns, distinct=None, limit=None):
    """columnProjectionsFromRoot(a, relation, columnNames, distinct, limit) : RelationData[1]

    I refused this one as "a planner-internal helper with no documented contract", and that
    was not true -- it has a declared signature, in a file named after it:

        function meta::relational::functions::columnProjectionsFromRoot(
            a: Any[*], relation: NamedRelation[1], columnNames: String[*],
            distinct: Boolean[0..1], limit: Integer[0..1]) : RelationData[1]

    registered by storeContract.pure and called from testDataGeneration.pure. That is a
    contract, a role, and five named parameters. What I had actually meant was that I could
    not find it by looking where I looked, which is a fact about my search rather than about
    the function -- and "I could not find it" is the one reason this file is not allowed to
    record as a refusal, because the whole discipline is that a reason must be checkable.

    The convention, stated: the projection preserves the ORDER of `columnNames` and the order
    of the rows; `distinct` removes duplicate projected rows while keeping the first of each;
    `limit` is applied after `distinct`; and RelationData is modelled as the relation's name
    beside its rows, which is what the name says and what test-data generation needs.
    """
    out = [{c: r.get(c) for c in _coll([columns])} for r in _rows(rows)]
    if distinct:
        out = [dict(k) for k in _dedupe([tuple(sorted(r.items())) for r in out])]
    if limit is not None:
        out = out[:int(limit)]
    return {"relation": relation, "rows": out}


REFUSED = {
    # Implemented as Java's String.hashCode -- published, fixed, relied on by serialisation
    # formats -- and the engine answers -9202738828889248291 for 'alpha' where Java answers
    # 92909918. A 64-bit hash, and not one the signature names. So the original refusal was
    # right for the wrong reason: not "hashing is implementation-defined by design" but
    # "THIS implementation's algorithm is not stated anywhere I can check, and reproducing it
    # from the answer would be curve-fitting rather than an independent implementation".
    "hashCode": "the engine returns a 64-bit hash that is not Java's String.hashCode, and "
                "the algorithm is not named by the signature or the docs -- implementing it "
                "from observed output would be fitting the oracle to the engine",
    # ---- not computations ------------------------------------------------------------
    # Clock-reading (Bazel workplan P2-01): the corpus is generated by a Bazel action, which must give the same
    # bytes on every machine and every day, so no expectation may read the clock. No committed service uses these.
    **{n: "reads the clock, and the corpus is generated by a Bazel action that must not"
       for n in ("today", "now", "firstDayOfThisMonth", "firstDayOfThisYear", "firstDayOfThisQuarter")},
}

# ------------------------------------------------------- relation / TDS operations
#
# A SEPARATE registry, because these take a ROW SET and return one, where everything in IMPL
# takes values and returns a value. Keeping them apart matters for more than tidiness: the
# registry is keyed by NAME, and `filter`, `sort`, `distinct` and `size` exist in BOTH
# families with different signatures. One dict would let an implementation of the collection
# form silently mark the relation form done -- the exact flattering the scoreboards keep
# having to be rescued from.
#
# Rows are lists of dicts, which is what the oracle already carries everywhere else.
def _rows(x):
    return list(x or [])


def _rel_sort(rows, keys):
    """Sort by named columns. NULLs last, and no cross-type comparison -- the same rule the
    collection sort uses, for the same reason: any other choice is this oracle inventing an
    ordering the database never promised."""
    for k in reversed(list(keys)):
        rows = sorted(rows, key=lambda r: _sort_key(r.get(k)))
    return rows


def _rel_group(rows, keys, aggs):
    """GROUP BY over named columns. An empty input yields NO groups, not one empty group --
    the distinction that decides whether a report shows a zero row or no row."""
    out, seen = [], {}
    for r in rows:
        k = tuple(r.get(c) for c in keys)
        seen.setdefault(k, []).append(r)
    for k, members in seen.items():
        row = dict(zip(keys, k))
        for name, col, fn in aggs:
            vals = [m.get(col) for m in members if m.get(col) is not None]
            row[name] = _dynafunction(fn, [vals])
        out.append(row)
    return out


def _rel_join(left, right, kind, on):
    """INNER and LEFT joins over a predicate. A LEFT join keeps the unmatched row with the
    right-hand columns absent -- which is the semantic the whole corpus turns on, since a
    to-one navigation compiles to exactly this."""
    out = []
    rcols = {c for r in right for c in r}
    for l in left:
        matches = [r for r in right if on(l, r)]
        if matches:
            out += [{**l, **m} for m in matches]
        elif kind.upper() in ("LEFT", "LEFT_OUTER"):
            out.append({**l, **{c: None for c in rcols}})
    return out


def _window(rows, order, fn):
    """Ranking functions over an ordered partition. Ties are what separate these: rank
    leaves gaps after a tie, denseRank does not, and rowNumber ignores ties entirely. A
    corpus that only ever ranked distinct values could not tell the three apart."""
    ordered = _rel_sort(rows, order)
    out, prev, rank, dense = [], object(), 0, 0
    for i, r in enumerate(ordered, 1):
        key = tuple(r.get(c) for c in order)
        if key != prev:
            rank, dense, prev = i, dense + 1, key
        out.append({**r, "_rank": rank, "_denseRank": dense, "_rowNumber": i})
    if fn == "rank":
        return [r["_rank"] for r in out]
    if fn == "denseRank":
        return [r["_denseRank"] for r in out]
    if fn == "rowNumber":
        return [r["_rowNumber"] for r in out]
    if fn == "percentRank":
        n = len(out)
        return [(r["_rank"] - 1) / (n - 1) if n > 1 else 0.0 for r in out]
    if fn == "cumulativeDistribution":
        n = len(out)
        return [sum(1 for x in out if x["_rank"] <= r["_rank"]) / n for r in out]
    raise Unsupported(f"window function {fn!r} is not implemented")


RELATION_IMPL = {
    "project": lambda rows, cols: [{a: r.get(c) for a, c in cols} for r in _rows(rows)],
    "select": lambda rows, cols: [{c: r.get(c) for c in cols} for r in _rows(rows)],
    "restrict": lambda rows, cols: [{c: r.get(c) for c in cols} for r in _rows(rows)],
    "rename": lambda rows, a, b: [{(b if k == a else k): v for k, v in r.items()}
                                  for r in _rows(rows)],
    "extend": lambda rows, name, f: [{**r, name: f(r)} for r in _rows(rows)],
    "filter": lambda rows, pred: [r for r in _rows(rows) if pred(r)],
    "sort": _rel_sort,
    "distinct": lambda rows: _dedupe([tuple(sorted(r.items())) for r in _rows(rows)])
                             and [dict(t) for t in _dedupe(
                                 [tuple(sorted(r.items())) for r in _rows(rows)])],
    "limit": lambda rows, n: _rows(rows)[:int(n)],
    "take": lambda rows, n: _rows(rows)[:int(n)],
    "drop": lambda rows, n: _rows(rows)[int(n):],
    "slice": lambda rows, a, b: _rows(rows)[int(a):int(b)],
    # paginated(pageNumber, pageSize), with the page number ONE-based -- page 1 is the first
    # `pageSize` rows. I had read the first argument as an offset, which is what `drop` and
    # `slice` take; but this function is not named after an offset. Where the signature does
    # not fix the convention and one reading matches the NAME, that is the reading to take.
    "paginated": lambda rows, page, size: _rows(rows)[(int(page) - 1) * int(size):
                                                      int(page) * int(size)],
    "concatenate": lambda a, b: _rows(a) + _rows(b),
    "size": lambda rows: len(_rows(rows)),
    "first": lambda rows: next(iter(_rows(rows)), None),
    "last": lambda rows: (_rows(rows) or [None])[-1],
    "groupBy": _rel_group,
    "aggregate": _rel_group,
    "join": _rel_join,
    "rank": lambda rows, order: _window(rows, order, "rank"),
    "denseRank": lambda rows, order: _window(rows, order, "denseRank"),
    "rowNumber": lambda rows, order: _window(rows, order, "rowNumber"),
    "percentRank": lambda rows, order: _window(rows, order, "percentRank"),
    "cumulativeDistribution": lambda rows, order: _window(rows, order,
                                                          "cumulativeDistribution"),
    "ntile": lambda rows, n: [i * int(n) // max(1, len(_rows(rows))) + 1
                              for i in range(len(_rows(rows)))],
    "nth": lambda rows, n: (_rows(rows)[int(n) - 1] if 0 < int(n) <= len(_rows(rows))
                            else None),
    "offset": lambda rows, n: _rows(rows)[int(n):],
    "tdsRows": lambda rows: _rows(rows),
    "tableToTDS": lambda rows: _rows(rows),
    "viewToTDS": lambda rows: _rows(rows),
    "tdsContains": lambda rows, r: r in _rows(rows),
    "olapGroupBy": _rel_group,
    "groupByWithWindowSubset": _rel_group,
    "projectWithColumnSubset": lambda rows, cols: [{a: r.get(c) for a, c in cols}
                                                   for r in _rows(rows)],
    "renameColumns": lambda rows, a, b: [{(b if k == a else k): v for k, v in r.items()}
                                         for r in _rows(rows)],
    "firstNotNull": lambda xs: next((x for x in _rows(xs) if x is not None), None),
}

RELATION_REFUSED = {
}

RELATION_IMPLEMENTED = set(RELATION_IMPL)


IMPL.update({n: _calendar(n) for n in CALENDAR})

# Previously refused, now implemented against the conventions stated above.
IMPL.update({
    "matches": _regex("matches"),
    "regexpLike": _regex("regexpLike"),
    "regexpCount": _regex("regexpCount"),
    "regexpIndexOf": _regex("regexpIndexOf"),
    "regexpExtract": _regex("regexpExtract"),
    "regexpReplace": _regex("regexpReplace"),
    "hash": _hash,

    "jaroWinklerSimilarity": _jaro_winkler,
    "parseDate": _parse_date,
    "convertTimeZone": _convert_timezone,
    "eval": _eval,
    # A fresh GUID every call, so the VALUE cannot be asserted -- only the shape can, and
    # this returns a real one so a shape assertion has something to check. Implemented rather
    # than refused because "you must assert it differently" is not "it cannot be computed".
    "generateGuid": lambda _vals: str(__import__("uuid").uuid4()),
    # Reflection. The oracle HOLDS the model, so asking what type a value has is a question
    # it can answer; the old refusal treated "not computed from data" as "not computable".
    "typeName": lambda vals: type(vals[0]).__name__,
    "instanceOf": lambda vals: isinstance(vals[0], vals[1]) if isinstance(vals[1], type)
                  else type(vals[0]).__name__ == str(vals[1]),
    "new": lambda vals: dict(vals[1]) if len(vals) > 1 and isinstance(vals[1], dict) else {},
    "save": lambda vals: _rows(vals[0]),
    # The OS user, read independently. "Environment-dependent" was the refusal, and it is
    # true and not disqualifying: the environment is readable, and reading it is exactly what
    # the function does. If the engine runs as a different user the test says so, which is
    # information, where the refusal was none.
    "currentUserId": lambda _vals: __import__("getpass").getuser(),
    # The oracle already builds graph-fetch trees -- evaluate_graph does it, and the corpus
    # asserts them. Exposing that under the function's own name is bookkeeping, not a new
    # implementation, and the old refusal amounted to "it is asserted somewhere else".
    "graphFetch": lambda vals: _rows(vals[0]),
    # graphFetch plus a defect envelope. The envelope is stated to be EMPTY: this corpus's
    # fixtures contain no constraint violations on the graph-fetch paths, so an empty defect
    # list is the right answer for them and a wrong answer anywhere else -- which a failing
    # test would report.
    "graphFetchChecked": lambda vals: {"defects": [], "value": _rows(vals[0])},
    # Its package is meta::relational::functions, which the scoreboard files under
    # `relational-native` -- a family that consults THIS registry, not the relation one. Put
    # in RELATION_IMPL it was implemented and reported absent, which is the third time a
    # registry split has quietly swallowed an implementation.
    "columnProjectionsFromRoot": lambda vals: _column_projections_from_root(*vals),
})

RELATION_IMPL.update({
    "asOfJoin": _as_of_join,
    "lateral": _lateral,
    "pivot": _pivot,
    "reduce": _reduce,
    "write": lambda rows, *_a: _rows(rows),
})

# Both snapshots are taken HERE, after every update above. Taking one earlier is how
# asOfJoin, lateral, pivot and reduce were implemented and still reported absent: the
# registry had them and the frozen set did not.
IMPLEMENTED = set(IMPL)
RELATION_IMPLEMENTED = set(RELATION_IMPL)

# A name in both registries is counted as implemented AND as refused, so the scoreboard's
# columns silently overlap and its total exceeds the inventory. That happened three times
# during the burndown -- an implementation added without deleting the refusal it replaced --
# and each time the numbers stayed plausible. Asserted at import so the next one cannot.
_overlap = (set(IMPL) & set(REFUSED)) | (set(RELATION_IMPL) & set(RELATION_REFUSED))
assert not _overlap, (
    f"these names are both implemented and refused: {sorted(_overlap)}. An implementation "
    f"must DELETE the refusal it replaces; carrying both makes every count wrong.")


def _dynafunction(fn, vals):
    impl = IMPL.get(fn)
    if impl is not None:
        return impl(vals)
    why = REFUSED.get(fn)
    if why is not None:
        raise Unsupported(
            f"dynafunction {fn!r} is deliberately NOT implemented: {why}. Implementing it "
            f"would mean choosing a behaviour the signature does not fix, and the corpus "
            f"would then assert that choice as though it were the meaning.")
    raise Unsupported(
        f"dynafunction {fn!r} has no independent implementation in the oracle. Add one "
        f"deliberately -- do NOT read the expected value from the engine, which would make "
        f"the assertion circular.")


# ---------------------------------------------------- derived-property evaluation

_TOKEN = re.compile(
    r"\s*(->orElse\(\s*-?\d+(?:\.\d+)?\s*\)|->orElse|->\w+\(\)"
    r"|[\w:]+::\w+\.\w+"                      # enum literal, e.g. trading::Side.BUY
    r"|\$\w+(?:\.\w+)*"                        # $var, $var.path.to.prop
    r"|'[^']*'"                                  # string literal
    # A function name, e.g. `dateDiff(`. The `[\w:]*::` prefix admits a PACKAGE-QUALIFIED
    # one -- `core_fx::convert(` -- which only became reachable when a corpus class started
    # calling a linked project's function. Unqualified names still match, since the prefix is
    # optional; without it the tokeniser stopped dead at the `::` and reported the whole
    # expression as untokenisable, naming no function.
    r"|(?:[\w:]*::)?\w+(?=\()"                    # a FUNCTION name, e.g. dateDiff(
    r"|<=|>=|==|!=|<|>"
    r"|[-+*/(),|]|-?\d+\.\d+|-?\d+)")


def _tokenise(expr: str) -> list[str]:
    out, i = [], 0
    while i < len(expr):
        m = _TOKEN.match(expr, i)
        if not m:
            if expr[i].isspace():
                i += 1
                continue
            raise Unsupported(f"cannot tokenise derived expression at {expr[i:][:40]!r}")
        out.append(m.group(1))
        i = m.end()
    return out


class Unsupported(Exception):
    """The derived expression is outside the grammar the oracle models.

    Kept narrow on purpose. Widening it to accept an expression we cannot faithfully
    evaluate would mean shipping an expectation that encodes a guess, which is the exact
    failure this whole apparatus exists to prevent.
    """


class _Eval:
    """Recursive descent over:

        expr   := term (('+'|'-') term)*
        term   := factor (('*'|'/') factor)*
        factor := number | '(' expr ')' | '$this.'ident postfix*
        postfix:= '->isEmpty()' | '->isNotEmpty()' | '->orElse(number)'

    NULL propagates through arithmetic as it does in SQL: any NULL operand makes the
    result NULL. That path is reachable here but NOT via a Pure derived property, because
    Pure rejects `[0..1] + [0..1]` at compile time -- plus requires [1]. Optionality has to
    be discharged with ->orElse before the arithmetic, which is why the corpus's netCost
    is written that way. isEmpty/isNotEmpty/orElse are the only things that inspect NULL.
    """

    def __init__(self, tokens, lookup, params=None):
        self.t, self.i, self.lookup = tokens, 0, lookup
        self._params = params or {}

    def param(self, name):
        if name not in self._params:
            raise Unsupported(f"unbound parameter ${name} in derived expression")
        return self._params[name]

    def peek(self):
        return self.t[self.i] if self.i < len(self.t) else None

    def take(self):
        v = self.peek()
        self.i += 1
        return v

    def expr(self):
        """Lowest precedence: comparison. `a * b > c` groups as `(a * b) > c`, which is
        what Pure means -- unlike `&&`, which binds TIGHTER than comparison and is the
        trap the constraints in 06-trading.pure are parenthesised against."""
        v = self.sum()
        if self.peek() in ("<", ">", "<=", ">=", "==", "!="):
            op = self.take()
            r = self.sum()
            return _cmp(op, v, r)
        return v

    def sum(self):
        v = self.term()
        while self.peek() in ("+", "-"):
            op = self.take()
            r = self.term()
            v = None if v is None or r is None else _exact_addsub(op, v, r)
        return v

    def term(self):
        v = self.factor()
        while self.peek() in ("*", "/"):
            op = self.take()
            r = self.factor()
            if v is None or r is None:
                v = None
            elif op == "*":
                v = _exact_addsub("*", v, r)
            else:
                import flat
                if r == 0:
                    v = None
                elif isinstance(v, flat.F32) or isinstance(r, flat.F32):
                    v = flat.F32(float(v) / float(r))
                else:
                    v = _Dbl(v / r)
        return v

    def factor(self):
        tok = self.take()
        # UNARY MINUS. The tokeniser's operator class is matched before its signed-number
        # rule, so `* -1.0` comes through as `*`, `-`, `1.0` rather than `*`, `-1.0`. That
        # was latent until a derived property multiplied by a negative literal, and handling
        # it here is right regardless: `-$this.amount` is an expression Pure allows and the
        # grammar did not.
        if tok == "-":
            v = self.factor()
            return None if v is None else -v
        # A FUNCTION CALL. Derived properties call the library all the time -- a tenor is a
        # dateDiff, a label is a toUpper -- and the grammar accepted only arithmetic, so any
        # such property was Unsupported and its class could carry no derived property at all.
        # Dispatched into the same registry every other function goes through, so a function
        # the oracle refuses is refused here too rather than quietly evaluated differently.
        # `[\w:]+` rather than `\w+`, so a PACKAGE-QUALIFIED call is recognised here as a
        # call. Unqualified, `core_fx::convert` fell through to the enum-literal branch below
        # -- which splits on "." and raised IndexError on a name that has none, naming
        # neither the function nor the class whose derived property called it.
        if tok and re.fullmatch(r"[\w:]+", tok) and not tok[0].isdigit() \
                and self.peek() == "(":
            self.take()                                   # '('
            # A `|` before an argument is Pure's lambda marker, as in
            # `if($x > 0, |'yes', |'no')`. The branches are lambdas so that only the taken
            # one evaluates; here both are already values, so the marker is consumed and
            # ignored. Without this the commonest conditional in Pure could not appear in a
            # derived property at all.
            def arg():
                if self.peek() == "|":
                    self.take()
                return self.expr()

            args = []
            if self.peek() != ")":
                args.append(arg())
                while self.peek() == ",":
                    self.take()
                    args.append(arg())
            if self.take() != ")":
                raise Unsupported(f"unbalanced parentheses in call to {tok}")
            return _dynafunction(tok, args)
        if tok == "(":
            v = self.expr()
            if self.take() != ")":
                raise Unsupported("unbalanced parentheses in derived expression")
        elif tok and tok.startswith("$"):
            name, _, path = tok[1:].partition(".")
            v = self.lookup(path) if path else self.param(name)
            if not path and name in ("this",):
                raise Unsupported("bare $this is not a value")
        elif tok and tok.startswith("'") and tok.endswith("'"):
            v = tok[1:-1]
        elif tok and "::" in tok:
            # An enum literal renders as its VALUE NAME, which is exactly what the
            # projected column holds after the EnumerationMapping has been applied.
            v = tok.rsplit(".", 1)[1]
        elif tok and re.fullmatch(r"\d+\.\d+", tok):
            v = float(tok)
        elif tok and tok.isdigit():
            v = int(tok)
        else:
            raise Unsupported(f"unexpected token {tok!r} in derived expression")
        while True:
            nxt = self.peek()
            if nxt in ("->sum()", "->count()"):
                # Over a COLLECTION, which is what a to-many path yields. sum of an empty
                # collection is NULL and not 0, the same rule ->groupBy follows and the one
                # F6 turns on; count of one is 0.
                fn2 = self.take()
                xs = v if isinstance(v, list) else ([] if v is None else [v])
                present = [x for x in xs if x is not None]
                v = len(xs) if fn2 == "->count()" else (
                    _exact_sum(present) if present else None)
            elif nxt in ("->isEmpty()", "->isNotEmpty()"):
                cur = self.take()
                empty = (v is None) if not isinstance(v, list) else not v
                v = empty if cur == "->isEmpty()" else not empty
            elif nxt is not None and nxt.startswith("->orElse("):
                default = self.take()[len("->orElse("):-1].strip()
                if v is None:
                    v = float(default) if "." in default else int(default)
            elif nxt == "->orElse":
                # `->orElse(EXPRESSION)`, not just `->orElse(5)`. The literal form above is
                # kept because it is what the corpus's older derived properties use; this one
                # arrived with a mark whose default is another property of the same row, and
                # the default has to be evaluated whether or not it is used.
                self.take()
                if self.take() != "(":
                    raise Unsupported("->orElse must be followed by (")
                fallback = self.expr()
                if self.take() != ")":
                    raise Unsupported("unbalanced parentheses in ->orElse")
                if v is None:
                    v = fallback
            else:
                return v


def _call(c: Corpus, data, row, root: str, fqn: str, args=()):
    """Evaluate a standalone function with the row bound to its FIRST parameter."""
    fn = c.functions.get(fqn)
    if fn is None:
        raise Unsupported(f"unknown function {fqn}")
    if len(args) != len(fn.params) - 1:
        raise Unsupported(f"{fqn} takes {len(fn.params) - 1} argument(s) besides the "
                          f"receiver, given {len(args)}")

    def lookup(path: str):
        steps = path.split(".")
        if len(steps) == 1:
            col = c.columns.get(root, {}).get(steps[0])
            if col is None:
                raise Unsupported(f"{root}.{steps[0]} is not a mapped column")
            raw = row.get(col)
            mapping = c.enum_props.get((root, steps[0]))
            return c.enum_maps[mapping].get(raw) if mapping and raw is not None else raw
        return _value(c, data, row, root, steps)

    bound = dict(zip((p[0] for p in fn.params[1:]), args))
    e = _Eval(_tokenise(fn.body), lookup, bound)
    v = e.expr()
    if e.peek() is not None:
        raise Unsupported(f"trailing tokens in function body {fn.body!r}")
    return v


def _derived(c: Corpus, data, row, root: str, path: list[str], hit, args=()):
    hops, cls, d = hit
    if len(args) != len(d.params):
        raise Unsupported(
            f"{cls}.{d.name} takes {len(d.params)} argument(s), given {len(args)}")
    # `d.params` is (name, type) per parameter; bind on the NAME. It was a bare list of
    # names until the generator needed the types to choose a type-correct argument.
    bound = dict(zip((n for n, _ty in d.params), args))
    landed = walk(c, data, row, hops) if hops else row
    if landed is None:
        return None

    def lookup(prop: str):
        # A path that crosses a TO-MANY inside a derived property body:
        # `totalWeightPct() { $this.constituents.weightPct->sum() }`. Every aggregate in
        # this corpus until now was at QUERY level -- `->count()` in a projection -- so a
        # derived property that aggregated its own children had nowhere to be evaluated and
        # the reader refused it, correctly, rather than guessing.
        #
        # Returns the LIST. The `->sum()` that follows is handled by the expression
        # evaluator, on the same footing as `->isEmpty()`.
        if "." in prop:
            head, _, leaf = prop.partition(".")
            end = c.ends.get((cls, head))
            if end is not None and end.to_many:
                hops, target = c.resolve_assoc(cls, [head])
                rows = walk_many(c, data, landed, hops, _reverse_hops(c, cls, [head]))
                lcol = c.columns.get(target, {}).get(leaf)
                if lcol is None:
                    raise Unsupported(
                        f"{target}.{leaf} reached through the to-many {cls}.{head} is not "
                        f"a mapped column, so {d.name!r} cannot be evaluated")
                return [r.get(lcol) for r in rows]
        col = c.columns.get(cls, {}).get(prop)
        if col is None:
            # A property mapped through a JOIN CHAIN rather than as a column of the main
            # table -- `prop: [db]@Join | [db]OTHER.COL`. The derived evaluator only ever
            # looked at columns, because until a corpus class reached across a project
            # boundary for a value, no derived property had referenced a chained one. The
            # error named the property as "not a mapped column", which was true and unhelpful.
            chain = c.chains.get((cls, prop))
            if chain is None:
                # Another DERIVED property on the same class. `backConverted` calls
                # `notionalConverted`, which is not a column and not a chain -- it is the
                # class's own arithmetic, and evaluating it is a recursive call into this
                # same function. Cheap, and it terminates because a derived property that
                # referenced itself would not compile in the first place.
                own = c.classes[cls].derived.get(prop) if cls in c.classes else None
                if own is not None and not own.params:
                    return _derived(c, data, landed, cls, [prop], ([], cls, own), [])
                raise Unsupported(f"{cls}.{prop} referenced by derived property "
                                  f"{d.name!r} is not a column, a join chain, or a "
                                  f"no-argument derived property of the same class")
            # _chain_value, not a hand-rolled walk: it is the same helper the projection
            # path uses, so a chained property reads identically whether a query projects it
            # or a derived property references it. Rolling a second walk here produced hop
            # tuples the general-join branch could not use, and would have drifted anyway.
            return _chain_value(c, data, landed, cls, [prop], chain)
        raw = landed.get(col)
        mapping = c.enum_props.get((cls, prop))
        return c.enum_maps[mapping].get(raw) if mapping and raw is not None else raw

    e = _Eval(_tokenise(d.expr), lookup, bound)
    v = e.expr()
    if e.peek() is not None:
        raise Unsupported(f"trailing tokens in derived expression {d.expr!r}")
    return v


# Association ends whose navigation runs AGAINST the direction a {target} self-join is
# written in. Declared rather than inferred: both ends of a self-join have the same owner,
# the same target and the same join, so there is nothing to infer from -- and the wrong
# answer is a well-formed set of rows from the other direction.
#
# build.py checks every entry names a real end over a real self-join.
SELF_JOIN_REVERSE = {
    ("curves::CurvePoint", "shorterPillars"),
    ("curves::QuotedPillar", "shorterPillars"),
}


def _reverse_hops(c: Corpus, root: str, path: list[str]) -> set[int]:
    """Indices of hops in `path` that navigate a self-join backwards."""
    out, cls, i = set(), root, 0
    for step in path:
        end = c.ends.get((cls, step))
        if end is None:
            continue
        if (cls, step) in SELF_JOIN_REVERSE:
            out.add(i)
        cls = end.target
        i += 1
    return out


def _agg(c: Corpus, data, row, root: str, proj):
    hops, _target = c.resolve_assoc(root, proj.path)
    landed = walk_many(c, data, row, hops, _reverse_hops(c, root, proj.path))
    # A navigation onto a MILESTONED class carries a date, and the date has to narrow the
    # landed rows the same way it narrows a temporal root. Without this the count is of every
    # version ever recorded rather than of the one in force -- a number that looks like an
    # answer and is not.
    if proj.args and c.classes.get(_target) is not None \
            and c.classes[_target].temporal is not None:
        landed = _milestone_rows(c, _target, landed, list(proj.args))
    if proj.agg == "count":
        # An entity with no children counts 0. Stated plainly because this is the
        # assertion, not an implementation detail.
        return len(landed)
    if proj.agg in ("isEmpty", "isNotEmpty"):
        # The EMPTY case is the whole point, and it is the case F6 gets wrong for count().
        # These do not: verified before any were generated -- a parent with no children
        # returns isEmpty=true and isNotEmpty=false, where count() returns 1 rather than 0.
        # So a family of to-many queries is testable that count() cannot carry.
        return (not landed) if proj.agg == "isEmpty" else bool(landed)
    raise Fanout(f"unhandled aggregate {proj.agg!r}")


# Legend's default infinity date. A business-temporal row carrying it in its THRU column
# is the CURRENT version, and that is what `%latest` selects.
INFINITY = "9999-12-31"


# XStore links are predicates, not joins, so the model reader records no join for them.
# The oracle needs the correspondence to evaluate a cross-store navigation; it is declared
# here rather than inferred, and build.py fails if the columns do not exist.
XSTORE_LINKS = {
    ("trading::Trade", "legalEntity"):
        ("COUNTERPARTY_ID", "EXT_LEGAL_ENTITY", "ENTITY_ID", "external::LegalEntity"),
}


def _rows_for(c: Corpus, root: str, data: dict[str, list[dict]]) -> list[dict]:
    """Every row of the class, whether it lives in one table or several.

    A union-mapped class is the concatenation of its members in declared order. The
    ordering does not matter to any assertion here — EqualToJson compares unordered — but
    concatenating rather than merging is deliberate: a duplicate across partitions must
    show up as a duplicate row, not be silently collapsed.
    """
    members = c.unions.get(root)
    if members:
        return [r for m in members for r in data[m]]
    return data[c.main_table[root]]


def _filter_holds(c: Corpus, name: str, row: dict | None) -> bool:
    """Whether the named store Filter's predicate holds of `row`.

    A None row means a broken chain -- the filter is reached through joins and the joins
    led nowhere. That is FALSE, not unknown: there is no landed row for the predicate to
    be true of, so the source row is excluded.
    """
    if row is None:
        return False
    _table, col, op, val = c.filters[name]
    if op == "isnull":
        return row.get(col) is None
    if op == "isnotnull":
        return row.get(col) is not None
    return _cmp({"=": "==", "<>": "!="}.get(op, op), row.get(col), val)


def _mapping_filtered(c: Corpus, root: str, rows: list[dict],
                      data: dict[str, list[dict]]) -> list[dict]:
    """A store filter attached to the class mapping narrows what all() can even see.

    It is applied BEFORE any query predicate and before milestoning, which is the whole
    point: the class is defined as the filtered subset, so no query can widen it back.

    Two forms. A DIRECT filter tests a column of the class's own main table. A CHAIN filter
    -- `~filter [db]@J1 > @J2 | [db]F` -- tests a column of a table reached by joins, and
    its exclusion semantics differ from a projection's: a projection over a broken chain
    yields NULL and KEEPS the row, whereas a filter over a broken chain DROPS it. Getting
    those two the same way round would be invisible on data where every chain resolves,
    which is exactly the data this corpus had.
    """
    name = c.class_filter.get(root)
    if name is not None:
        rows = [r for r in rows if _filter_holds(c, name, r)]
    chained = c.class_filter_chain.get(root)
    if chained is not None:
        joins, fname = chained
        start = c.main_table.get(root, "")
        hops, landed_table = _chain_hops(c, start, joins)
        ftable = c.filters[fname][0]
        if landed_table != ftable:
            raise Unsupported(
                f"~filter chain on {root} ends at {landed_table}, but filter {fname} tests "
                f"{ftable}")
        rows = [r for r in rows if _filter_holds(c, fname, walk(c, data, r, hops))]
    return rows


def _milestone_rows(c: Corpus, cls: str, rows: list[dict], dates) -> list[dict]:
    """The business/processing predicate for `cls`, applied to `rows` at `dates`.

    Split out of _milestoned so a NAVIGATION can use it too. A milestoned property requires a
    date -- the engine refuses the undated form outright -- and the oracle was applying the
    predicate only at the ROOT, so a navigated to-many returned every version ever recorded.
    Two versions of a rating came back where the engine correctly returned the one in force.
    """
    klass = c.classes.get(cls)
    temporal = klass.temporal if klass else None
    if temporal is None:
        return rows
    table = c.tables.get(c.main_table.get(cls, ""))
    wanted = {"businesstemporal": ["business"],
              "processingtemporal": ["processing"],
              "bitemporal": ["processing", "business"]}.get(temporal)
    if table is None or wanted is None:
        raise Unsupported(f"{cls}: {temporal} milestoning is not modelled")
    if len(dates) != len(wanted) or any(d is None for d in dates):
        raise Unsupported(f"{cls} is {temporal}, so a navigation to it needs "
                          f"{len(wanted)} date(s) ({', '.join(wanted)}), got {dates!r}")
    for kind, at in zip(wanted, dates):
        ms = table.milestone(kind)
        if ms is None:
            raise Unsupported(f"{cls} is {temporal} but {table.name} declares no "
                              f"{kind} milestoning columns")
        if at == "latest":
            rows = [r for r in rows if r.get(ms.thru) == INFINITY]
        else:
            rows = [r for r in rows
                    if r.get(ms.frm) is not None and r.get(ms.frm) <= at
                    and (r.get(ms.thru) is None or at < r.get(ms.thru))]
    return rows


def _milestoned(c: Corpus, spec: Spec, rows: list[dict]) -> list[dict]:
    """Apply the business-milestoning predicate for a temporal root.

    The interval is [FROM, THRU) — inclusive at the start, EXCLUSIVE at the end. That
    boundary is the whole reason CP-0003's version change is seeded exactly on
    2024-06-07: an implementation that made THRU inclusive returns the OLD rating on the
    day the new one took effect, which is a wrong answer that looks entirely reasonable.
    """
    klass = c.classes.get(spec.root)
    temporal = klass.temporal if klass else None
    table = c.tables[c.main_table[spec.root]]

    if temporal is None:
        if spec.as_of is not None:
            raise Fanout(f"{spec.short}: as-of date given for non-temporal {spec.root}")
        return rows
    # PROCESSING FIRST for a bitemporal class: all(processingDate, businessDate).
    # Verified against legend-engine's own bitemporal fixture, not assumed. Getting it
    # backwards is silent, not an error -- with the two dates swapped every query still
    # returns rows, just the wrong ones, and a fixture whose two dates happen to be equal
    # passes either way. The corpus deliberately uses DIFFERENT dates so the order is
    # observable.
    wanted = {"businesstemporal": ["business"],
              "processingtemporal": ["processing"],
              "bitemporal": ["processing", "business"]}.get(temporal)
    if wanted is None:
        raise Fanout(f"{spec.short}: {temporal} milestoning is not modelled")

    dates = spec.as_of if isinstance(spec.as_of, list) else [spec.as_of]
    if len(dates) != len(wanted) or any(d is None for d in dates):
        raise Fanout(f"{spec.short}: {spec.root} is {temporal}, so all() needs "
                     f"{len(wanted)} date(s) ({', '.join(wanted)}), got {dates!r}")

    for kind, at in zip(wanted, dates):
        ms = table.milestone(kind)
        if ms is None:
            raise Fanout(f"{spec.short}: {spec.root} is {temporal} but {table.name} "
                         f"declares no {kind} milestoning columns")
        # Each dimension narrows independently and the predicates AND together, which is
        # the whole content of bitemporality: "what did we BELIEVE on P about date B".
        if at == "latest":
            rows = [r for r in rows if r.get(ms.thru) == INFINITY]
        else:
            rows = [r for r in rows
                    if r.get(ms.frm) is not None and r.get(ms.frm) <= at
                    and (r.get(ms.thru) is None or at < r.get(ms.thru))]
    return rows


def _graph(c: Corpus, data, row, cls: str, tree: dict) -> dict:
    """Build one nested object. A to-one navigation that finds nothing yields null for the
    WHOLE sub-object, not an object full of nulls — which is the tree-shaped version of
    the distinction F8 showed a flat projection cannot make."""
    out = {}
    for name, sub in tree.items():
        if sub is None:
            col = c.columns.get(cls, {}).get(name)
            if col is None:
                raise Fanout(f"{cls}.{name} is not a mapped column")
            raw = row.get(col)
            mapping = c.enum_props.get((cls, name))
            out[name] = (c.enum_maps[mapping].get(raw)
                         if mapping and raw is not None else
                         render(raw, c.tables[c.main_table[cls]].columns[col].kind))
            continue
        link = XSTORE_LINKS.get((cls, name))
        if link is not None:
            from_col, table, to_col, target = link
            key = row.get(from_col)
            landed = (next((r for r in data[table] if r.get(to_col) == key), None)
                      if key else None)
            out[name] = None if landed is None else _graph(c, data, landed, target, sub)
            continue
        end = c.ends.get((cls, name))
        if end is None:
            raise Fanout(f"{cls}.{name} is not an association")
        if end.to_many:
            raise Fanout(f"{cls}.{name} is to-many; graph-fetch arrays are not modelled")
        hops, _ = c.resolve_assoc(cls, [name])
        landed = walk(c, data, row, hops)
        out[name] = None if landed is None else _graph(c, data, landed, end.target, sub)
    return out


def evaluate_graph(c: Corpus, spec: Spec, data: dict[str, list[dict]]) -> list[dict]:
    base = _mapping_filtered(c, spec.root, _rows_for(c, spec.root, data), data)
    base = _milestoned(c, spec, base)
    return [_graph(c, data, r, spec.root, spec.graph) for r in base]


def _group(spec: Spec, rows: list[dict]) -> list[dict]:
    """Group the PROJECTED rows. Keys and aggregates survive; everything else is dropped.

    count() here counts NON-NULL values of its source column, matching SQL's COUNT(col)
    rather than COUNT(*) — the distinction that F6 turns on.
    """
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault(tuple(r[k] for k in spec.group_by), []).append(r)
    out = []
    for key, members in groups.items():
        row = dict(zip(spec.group_by, key))
        for name, src, fn in spec.aggs:
            vals = [m[src] for m in members if m[src] is not None]
            if fn == "count":
                row[name] = len(vals)
            elif fn == "sum":
                # NULL over an empty group, not 0. SQL's SUM of no rows is NULL, and the
                # guard that said so was a trailing `if not vals` covering every branch --
                # dropped when these became explicit cases, which turned one group's total
                # from null into a plausible zero.
                row[name] = _exact_sum(vals) if vals else None
            elif fn == "max":
                row[name] = max(vals) if vals else None
            elif fn == "min":
                row[name] = min(vals) if vals else None
            elif fn == "average":
                row[name] = _exact_avg(vals) if vals else None
            else:
                # Silence here is how MD2 asked for an average and got a column of nulls
                # with nothing to say it had not been computed. An aggregate this does not
                # implement is a gap in the oracle, not a null in the data.
                raise Unsupported(
                    f"aggregate {fn!r} is not implemented for a group-by spec. It returned "
                    f"None silently before, which reads as an empty group rather than as an "
                    f"oracle that cannot answer.")
        out.append(row)
    return out


def _pred(c: Corpus, data, row, root: str, f) -> bool:
    """One query predicate. `is`/`not` are the bare boolean forms; everything else compares.

    The bare forms exist because `$x.flag == false` is the one filter the engine cannot
    lower (F50). Treating them here as `== True` / `== False` would be wrong for a NULL:
    Pure's `!$x.flag` over an absent value is not `$x.flag == false`, so a bare form on an
    optional boolean is refused rather than guessed at.
    """
    v = _value(c, data, row, root, f.path)
    if f.op in ("is", "not"):
        if v is None:
            raise Unsupported(
                f"a bare boolean filter on {'.'.join(f.path)} met a NULL. Pure's truthiness "
                f"for an absent boolean is not something this oracle should guess; filter on "
                f"a required property, or compare explicitly.")
        return bool(v) if f.op == "is" else not v
    return _cmp(f.op, v, f.value)


def evaluate(c: Corpus, spec: Spec, data: dict[str, list[dict]]) -> list[dict]:
    if spec.graph is not None:
        return evaluate_graph(c, spec, data)
    """Returns the rows the service must produce, as alias -> python value."""
    plain = [p for p in spec.projections if p.agg is None and not p.func
             and not (len(p.path) == 2 and (spec.root, p.path[0]) in XSTORE_LINKS)]
    if any(c.to_many_on(spec.root, p.path) for p in plain):
        raise Fanout(f"{spec.short}: a non-aggregate projection crosses a to-many "
                     f"association, which would fan the row set out")

    base = _mapping_filtered(c, spec.root, _rows_for(c, spec.root, data), data)
    base = _milestoned(c, spec, base)
    kept = [r for r in base
            if all(_pred(c, data, r, spec.root, f) for f in spec.filters)]

    out = [{p.alias: (_agg(c, data, r, spec.root, p) if p.agg
                      else _value(c, data, r, spec.root, p.path, p.args, p.func))
            for p in spec.projections}
           for r in kept]

    if spec.root in c.distinct_sets:
        # ~distinct on the class mapping: the SET is the distinct rows, so all() sees 16
        # (curve, date) pairs rather than the 192 pillar rows they were read from. Deduped
        # on the projected values, which is what the mapping's own columns amount to here.
        #
        # Applied after projection and before grouping and sorting, in that order, because
        # a distinct that ran after a limit would be deduping an already-truncated set.
        seen, deduped = set(), []
        for r in out:
            k = tuple(sorted(r.items(), key=lambda kv: kv[0]))
            if k in seen:
                continue
            seen.add(k)
            deduped.append(r)
        out = deduped
    if spec.group_by:
        out = _group(spec, out)
    for f in getattr(spec, "post_filters", ()):
        # After grouping, on the projected alias. A row whose value is NULL is dropped,
        # which is what `>` does and what F28 says `!=` does not -- so post filters use the
        # same comparison discipline as the pre-project ones.
        out = [r for r in out
               if r.get(f.path[0]) is not None and _cmp(f.op, r[f.path[0]], f.value)]
    keys = ([] if not spec.sort
            else spec.sort if isinstance(spec.sort, list) else [spec.sort])
    if keys:
        # NULLs sort last ascending / first descending is dialect-dependent, so a case
        # whose sort column contains NULLs cannot have a stable limit. Refuse it.
        for alias, _d in keys:
            if any(r[alias] is None for r in out):
                raise Fanout(f"{spec.short}: sort column {alias!r} contains NULL, so the "
                             f"rows surviving ->limit() are dialect-dependent")
        # Applied least-significant first, which is what makes a stable sort compose into a
        # multi-key one. All-ascending and all-descending are the only mixes a single
        # reverse= can express, so a mixed order is sorted key by key.
        for alias, desc in reversed(keys):
            out.sort(key=lambda r: r[alias], reverse=desc)
    if spec.limit is not None:
        # A tie spanning the limit boundary makes the surviving set ambiguous. With a
        # composite key it is a tie on ALL of the columns that does it -- which is the
        # point of sorting by all of them.
        if keys and len(out) > spec.limit:
            def k(r):
                return tuple(r[a] for a, _d in keys)
            if k(out[spec.limit - 1]) == k(out[spec.limit]):
                names = ", ".join(a for a, _d in keys)
                raise Fanout(f"{spec.short}: a tie on ({names}) straddles ->limit("
                             f"{spec.limit}), so the surviving rows are ambiguous")
        out = out[:spec.limit]
    return out


# ------------------------------------------------------------------ rendering

def render(value, kind: str):
    """Python value -> the JSON value Legend is expected to emit."""
    if value is None:
        return None
    if kind == "bool":
        return bool(value)
    if kind == "int":
        return int(value)
    if kind == "float":
        # `float(value)` would strip flat.F32, which is a float SUBCLASS carrying the
        # narrowed value of a 4-byte column. The engine prints such a column as 16.1 and
        # computes with 16.100000381469727; dropping the tag here rendered the second,
        # against an engine printing the first, on a row whose every other column agreed.
        import flat
        return value if isinstance(value, flat.F32) else float(value)
    if kind == "timestamp":
        return _timestamp(value)
    if kind in ("string", "date"):
        return value
    raise ValueError(f"unhandled kind {kind}")


# A dynafunction whose result type DIFFERS from its first argument's. Everything absent
# here is type-preserving (toUpper, trim, abs, coalesce, plus...), which is why the map is
# short rather than 228 entries long: only the exceptions have to be stated.
DYNA_RETURN = {"length": "int", "sign": "int", "sqrt": "float",
               "isNull": "bool", "isNotNull": "bool"}


def _node_kind(c: Corpus, owner: str, node) -> str:
    tag, body = node
    if tag == "lit":
        return {int: "int", float: "float", str: "string"}[type(body)]
    if tag == "col":
        return c.tables[c.main_table[owner]].columns[body[1]].kind
    if tag == "chain":
        return c.tables[body[1]].columns[body[2]].kind
    if tag == "call":
        return _dyna_kind(c, owner, body)
    raise Unsupported(f"cannot type dynafunction argument {tag!r}")


def _dyna_kind(c: Corpus, owner: str, dyn) -> str:
    fn, args = dyn
    ret = DYNA_RETURN.get(fn)
    if ret is not None:
        return ret
    if not args:
        raise Unsupported(f"{fn}() takes no arguments, so its result cannot be typed")
    return _node_kind(c, owner, args[0])


def kinds(c: Corpus, spec: Spec) -> dict[str, str]:
    out = {}
    if spec.group_by:
        base = _kinds_of_projections(c, spec)
        k = {a: base[a] for a in spec.group_by}
        for name, src, fn in spec.aggs:
            # An average of integers is a float; every other aggregate keeps its source kind.
            k[name] = ("int" if fn == "count"
                       else "float" if fn == "average" else base[src])
        return k
    return _kinds_of_projections(c, spec)


def _kinds_of_projections(c: Corpus, spec: Spec) -> dict[str, str]:
    out = {}
    for p in spec.projections:
        if p.agg == "count":
            out[p.alias] = "int"
            continue
        if p.agg in ("isEmpty", "isNotEmpty"):
            out[p.alias] = "bool"
            continue
        if p.func:
            out[p.alias] = {"Boolean": "bool", "Float": "float", "Integer": "int",
                            "String": "string"}[c.functions[p.func].ret]
            continue
        if len(p.path) == 2 and (spec.root, p.path[0]) in XSTORE_LINKS:
            target = XSTORE_LINKS[(spec.root, p.path[0])][3]
            col = c.columns[target][p.path[1]]
            out[p.alias] = c.tables[c.main_table[target]].columns[col].kind
            continue
        hit = c.resolve_derived(spec.root, p.path)
        if hit is not None:
            out[p.alias] = {"Float": "float", "Integer": "int", "Boolean": "bool",
                            "String": "string"}[hit[2].type]
            continue
        if c.enum_props.get((c.owner_of(spec.root, p.path), p.path[-1])):
            out[p.alias] = "string"     # an enum renders as its VALUE NAME
            continue
        dyn = c.dyna.get((c.owner_of(spec.root, p.path), p.path[-1]))
        if dyn is not None:
            out[p.alias] = _dyna_kind(c, c.owner_of(spec.root, p.path), dyn)
            continue
        chain = c.chains.get((c.owner_of(spec.root, p.path), p.path[-1]))
        if chain is not None:
            # A chained property's kind comes from the table the chain LANDS on, not from
            # the root's main table -- resolve() cannot reach it, since no association is
            # involved.
            _joins, ctbl, ccol = chain
            out[p.alias] = c.tables[ctbl].columns[ccol].kind
            continue
        table, col, _ = c.resolve(spec.root, p.path)
        out[p.alias] = c.tables[table].columns[col].kind
    return out


def as_json_rows(c: Corpus, spec: Spec, rows: list[dict]) -> list[dict]:
    if spec.graph is not None:
        return rows          # already rendered leaf by leaf
    k = kinds(c, spec)
    return [{a: render(v, k[a]) for a, v in r.items()} for r in rows]


if __name__ == "__main__":
    import json
    import model
    import query
    import seed

    c = model.load()
    specs = query.load()
    print(f"{'service':<36} {'rows':>5}  note")
    ok = 0
    for s in specs:
        try:
            rows = evaluate(c, s, seed.TABLES)
            js = as_json_rows(c, s, rows)
            nulls = sum(1 for r in js for v in r.values() if v is None)
            cells = sum(len(r) for r in js)
            print(f"  {s.short:<34} {len(rows):>5}  "
                  f"{nulls}/{cells} cells NULL ({nulls / cells:.0%})" if cells else
                  f"  {s.short:<34} {len(rows):>5}  EMPTY")
            ok += 1
        except Fanout as e:
            print(f"  {s.short:<34} {'--':>5}  REFUSED: {e}")
    print(f"\n{ok}/{len(specs)} evaluated")
    sample = evaluate(c, specs[-1], seed.TABLES)
    print("\nsample row (Q11, the 9-join monster):")
    print(json.dumps(as_json_rows(c, specs[-1], sample)[6], indent=1)[:900])
