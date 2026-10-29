"""
Reads ###Service elements into query specs the oracle can evaluate.

Only the fragment of the language the stress services actually use is parsed:

    <root>.all()
      [ ->filter({v| <pred>}) ]
      ->project(~[ <col> | <alias>:v|$v.<path> , ... ])
      [ ->sort(~<alias>-><asc|desc>ending()) ]
      [ ->limit(<n>) ]

  <pred> := $v.<path> <op> <literal> [ && <pred> ]     op in == != < <= > >=

Deliberately narrow. A parser that accepted more would let a query into the corpus that
the oracle cannot predict, and the expectation would silently become whatever the engine
produced — which is the failure mode this whole exercise exists to remove. Anything
unrecognised raises.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from model import QUERIES


class DateArg(str):
    """A Pure DATE literal argument: `%2024-02-01`, not `'2024-02-01'`.

    A milestoned property REQUIRES a business date -- "The property 'x' is milestoned with
    stereotypes: [businesstemporal] and requires date parameters: [businessDate]" -- and a
    date is the one argument type whose Pure literal is not its quoted string.
    """


class DateTimeArg(str):
    """A Pure DATETIME literal, held as the seed writes a TIMESTAMP ("2024-06-03 12:00:00") so it
    compares with seeded values, and written as Pure spells it: `%2024-06-03T12:00:00`."""


class StrictTimeArg(str):
    """A Pure STRICTTIME literal, `%17:30:00`; held as "17:30:00"."""


class EnumArg(str):
    """An enumeration value, `typedvalues::Grade.STANDARD`. Held as the value's NAME, which is
    what the oracle reads an enum-mapped property as, and carrying its enumeration for the text."""

    def __new__(cls, enum: str, value: str):
        s = super().__new__(cls, value)
        s.enum = enum
        return s


@dataclass(frozen=True)
class Param:
    """A service parameter in a predicate: `$p`, or `$p->toOne()` for an optional one. Its value
    comes from the test being evaluated (oracle.evaluate's `params`)."""
    name: str
    to_one: bool = False


@dataclass(frozen=True)
class PathArg:
    """A milestoning date read from the row the query is on: `$x.createdTime`. A path from the
    ROOT, whatever hop it dates (the improvement plan's Phase 3)."""
    path: tuple


class Hop(str):
    """A milestoned property in a path, navigated with its date(s): `bookLimit(%2024-06-03T09:21:00)`.

    Held as the property's NAME, so the model resolves it as any step, and carrying what the text
    and the oracle need (the improvement plan's Phase 3):
      dates     the property's date arguments: a DateTimeArg, a DateArg, a Param or a PathArg, one
                per milestoning dimension (processing first, for a bitemporal target). None for the
                no-argument form, which takes the date the navigation leaves from.
      versions  None; 'all' for the generated `<name>AllVersions`; or 'range' for
                `<name>AllVersionsInRange(start, end)`, with `dates` the two bounds.
    """

    def __new__(cls, name: str, dates=None, versions: str | None = None):
        s = super().__new__(cls, name)
        s.dates = None if dates is None else tuple(dates)
        s.versions = versions
        return s


@dataclass(frozen=True)
class Versions:
    """A root that reads more than one version: `.allVersions()` when `start` is None, else
    `->getAllVersionsInRange(start, end)` -- the function form, since the pinned engine's grammar
    refuses `.allVersionsInRange(...)` on a class (DomainParseTreeWalker.allOrFunction)."""
    start: object = None
    end: object = None


class Nullary(str):
    """A platform function of no arguments, as an argument: `pi()`. Held as its name."""


@dataclass(frozen=True)
class Apply:
    """Platform functions applied in a QUERY to the value at a path (the improvement plan's Phase 4):
    `$x.path->f(a)->g(b)`, each step a (function, arguments) pair, or an operator written infix
    (`+`, `-`, `*`, `/`) with its right operand. `ret` is the last step's Pure return type.

    `through_map` writes the chain inside `map`, `$x.path->map(v|$v->f(a))`: how Pure applies a function
    to an optional value. legend-pure's map over an empty collection is empty, so an absent value stays
    absent whatever the function (the oracle's _apply); the engine's SQL does not always agree (F68).

    `let` binds the value first, `let v = $x.path; $v->f(a);`: the same value, through a variable (stage 6)."""
    steps: tuple
    ret: str
    through_map: bool = False
    let: bool = False


# Relation operations after the projection (the improvement plan's Phase 4, stage 4), in a Spec's `relation`, applied
# in order after any groupBy: each written in the paradigm of its Spec, `->extend(~[...])` for a Relation and
# `->extend([col(...)])` for a TDS, and evaluated by the oracle's RELATION_IMPL.
@dataclass(frozen=True)
class Extend:
    """A column computed from another: `->extend(~[name: r|$r.col->f()])`, the functions an Apply's. `of` is the
    column's Pure type, which a TDS reads it by (`$r.getFloat('col')`)."""
    name: str
    col: str
    apply: Apply
    of: str | None = None


@dataclass(frozen=True)
class Window:
    """A window column: `->extend(over(~[partition], [~order->ascending()]), ~[name: {p,w,r|$p->fn($w,$r)}])` for a
    ranking function (`args` after the row: ntile's bucket count), or `{p,w,r|$r.col}:y|$y->fn()` for an aggregate
    over the partition (`col` set), or `{p,w,r|$p->lag($r).read}` for a function that answers another row of the
    partition, whose column `read` names (lag, lead and nth; `args` its offset). A TDS writes it as `olapGroupBy`.
    `ret` is its Pure type."""
    name: str
    fn: str
    partition: tuple
    order: tuple = ()          # ((column, descending), ...)
    args: tuple = ()
    col: str | None = None
    ret: str = "Integer"
    read: str | None = None


@dataclass(frozen=True)
class Rename:
    old: str
    new: str


@dataclass(frozen=True)
class Select:
    """`->select(~[cols])`, or a TDS's `restrict([...])`."""
    cols: tuple


@dataclass(frozen=True)
class Distinct:
    pass


@dataclass(frozen=True)
class Sort:
    """`->sort([~a->ascending(), ...])` inside the pipeline, where an ordinal operation needs a total order."""
    keys: tuple                 # ((column, descending), ...)


@dataclass(frozen=True)
class Cut:
    """An ordinal operation: `take(n)`, `drop(n)`, `limit(n)`, `slice(a, b)`, or a TDS's `paginated(page, size)`."""
    fn: str
    args: tuple


@dataclass(eq=False)
class Concatenate:
    """`->concatenate(<other>)`: the other relation a Spec of its own, projecting the same columns."""
    other: object


@dataclass(eq=False)
class Join:
    """`->join(<other>, JoinKind.INNER, {a, b| $a.x == $b.y})`: `on` pairs a left column with a right one, and the
    two sides' columns are distinct names."""
    other: object
    kind: str                  # INNER | LEFT
    on: tuple                  # ((left column, right column), ...)


# The value an If's branch takes when it is the tested value itself.
SELF = "<the value>"


@dataclass(frozen=True)
class If:
    """`if(<test>, |<then>, |<otherwise>)` over the value at a path (the improvement plan's Phase 4, stage 5). The
    test is the value under `op` and `value`, as a filter compares it (`is` for a Boolean, `isEmpty` for an absent
    one); each branch is a literal, or SELF for the value itself (`->toOne()` under `isEmpty`)."""
    op: str
    value: object
    then: object
    otherwise: object
    ret: str


class Agg(tuple):
    """An aggregate column of a groupBy: (result alias, source alias, function), as every generator before Phase 4
    writes one, and since its stage 3 (the aggregates) carrying what a platform aggregate needs:

      args    the aggregate's arguments after the collection: `percentile(0.5)`, `joinStrings(',')`
      pair    a second source alias, for an aggregate over pairs: the map is `mapper($x.src, $x.pair)`
      mapper  `rowMapper` or `wavgRowMapper` (meta::pure::functions::math::mathUtility), with `pair`
      then    steps (query.Apply's) applied to the aggregate's result: `->greaterThan(t)` compares an inexact one
      ret     the Pure type of the column, the last step's result

    A tuple, so `for name, src, fn in spec.aggs` reads it as it reads the plain triples."""

    def __new__(cls, name: str, src: str, fn: str, args=(), pair: str | None = None, mapper: str | None = None,
                then=(), ret: str | None = None):
        a = super().__new__(cls, (name, src, fn))
        a.args, a.pair, a.mapper, a.then, a.ret = tuple(args), pair, mapper, tuple(then), ret
        return a


@dataclass
class Pred:
    path: list[str]
    op: str
    # A literal; a list of literals or a Param for 'in'; nothing for 'isEmpty' / 'isNotEmpty'.
    value: object = None
    # Functions applied to the path's value before it is compared (Phase 4): the left operand is the
    # result. With op 'is' or 'not' the result is a Boolean and the filter itself.
    apply: Apply | None = None


# A predicate TREE (the improvement plan's Phase 1). A spec's `filters` is still a list of
# conjuncts, as every generator before writes it; a conjunct may now be one of these. Each atom
# is two-valued, as legend-pure defines a comparison over an absent value (oracle._pred), so
# the combinators are plain boolean ones.
@dataclass
class And:
    items: list


@dataclass
class Or:
    items: list


@dataclass
class Not:
    item: object


@dataclass(frozen=True)
class ParamEmpty:
    """`$p->isEmpty()`: true when the test supplies no value for an optional parameter."""
    name: str


def atoms(filters) -> list[Pred]:
    """The comparisons in a filter list, trees flattened: what a reader of the paths a spec
    filters on wants, whatever combines them."""
    out = []
    for f in filters:
        if isinstance(f, (And, Or)):
            out += atoms(f.items)
        elif isinstance(f, Not):
            out += atoms([f.item])
        elif isinstance(f, Pred):
            out.append(f)
    return out


@dataclass
class Proj:
    alias: str
    path: list[str]
    agg: str | None = None      # None -> a column; 'count' -> over the landed SET
    args: list = field(default_factory=list)   # arguments to a QUALIFIED property
    # A standalone function called extension-style: `$x->stress::rules::isBuy()`.
    func: str | None = None
    # Platform functions applied to the path's value (Phase 4): the column is their result.
    apply: Apply | None = None
    # With `agg`: the property of the landed rows it reduces, `$x.path->map(t|$t.<over>)->sum()` (stage 6).
    over: str | None = None


@dataclass
class Spec:
    name: str            # stress::Q0_...
    pattern: str
    doc: str
    root: str            # trading::Trade
    # Business date for a <<temporal.businesstemporal>> root: an ISO date, or the literal
    # 'latest'. None for a non-temporal class, where all() takes no argument. A list of two for a
    # bitemporal root (processing first). Since Phase 3 a date may also be a DateTimeArg or a Param,
    # and `versions` (below) replaces all() with every version, or the versions in a range.
    as_of: str | None = None
    projections: list[Proj] = field(default_factory=list)
    filters: list[Pred] = field(default_factory=list)
    # (alias, descending), or a LIST of them for a composite key. A single column is not a
    # stable order when the class's primary key has three, and `->limit` on top of an
    # unstable order is ambiguous rather than wrong -- which the oracle refuses outright.
    sort: tuple[str, bool] | list[tuple[str, bool]] | None = None
    limit: int | None = None
    # L2 mapping invariance: the same query emitted against a second mapping/runtime.
    # None means the corpus default (stress::AllMapping / stress::RT).
    mapping: str | None = None
    runtime: str | None = None
    # The ###Data element the testSuite's primary connection references. Defaults to the
    # main corpus one; a service over a store with its OWN connection needs its own, since
    # test data is bound to a connection rather than to a runtime.
    data_element: str | None = None
    # The identifiedConnection id inside that runtime — what the testSuite's
    # `data: [ connections: [ <key>: ... ] ]` is matched against.
    connection: str | None = None
    # Graph fetch: a TREE of property names instead of a flat projection.
    # {'tradeId': None, 'instrument': {'name': None}} means
    #   #{ Trade { tradeId, instrument { name } } }#
    # None as a value marks a leaf. When set, `projections` is unused.
    graph: dict | None = None
    # Another Spec whose expectation this one REUSES verbatim.
    #
    # For an invariance case the oracle cannot evaluate — an M2M target has no table, so
    # there is nothing to read — the claim under test IS that this service returns what
    # the mirrored one returns. Deriving a second expectation would be circular; stating
    # the reuse makes the claim explicit instead of hiding it in a duplicated literal.
    # The projection ALIASES must match, which build.py checks.
    mirrors: object | None = None
    # Applied to the mirrored rows before they become the expectation. For shapes the
    # oracle cannot produce directly — a unit-typed value serialises as an OBJECT, not a
    # number — this keeps the expectation DERIVED from the seed rather than captured from
    # engine output.
    transform: object | None = None
    # 'relation' -> project(~[alias:x|$x.p])   the newer paradigm
    # 'tds'      -> project([x|$x.p], ['alias'])  the legacy one
    #
    # Not interchangeable: a Relation projection is REJECTED over a ModelChainConnection
    # ("Non TDS return type not supported for Model Connections"), so an M2M service must
    # use the legacy form. See docs/UPSTREAM_FINDINGS.md F11.
    paradigm: str = "relation"
    # Query-level aggregation, applied AFTER project and BEFORE sort/limit.
    #   group_by  aliases of the projected columns to group on
    #   aggs      (result alias, source alias, 'count'|'sum'|'max'|'min'), or an Agg
    # Only the keys and the aggregates survive; every other projected column is dropped,
    # which is what groupBy means and a common way to write a query that silently loses
    # a column you thought you were selecting.
    # MultiExecution: [(key, mapping, runtime)]. The same query, several bindings,
    # selected per test by `keys: ['<key>']`.
    # Extra (connection id, ###Data element) pairs for a multi-store service.
    extra_data: list = field(default_factory=list)
    multi: list = field(default_factory=list)
    multi_key: str = "variant"
    group_by: list = field(default_factory=list)
    aggs: list = field(default_factory=list)
    # Predicates applied AFTER project (and after groupBy), on PROJECTED ALIASES rather than
    # on model paths. A different plan shape from `filters`: the join has already happened
    # and the predicate is over the result, so the engine must decide whether it can be
    # pushed back down. Nothing in the corpus had ever asked it to.
    post_filters: list = field(default_factory=list)
    # Service parameters, (name, Pure type, multiplicity: '1' | '0..1' | '*'), and the tests that
    # bind them: (test id, {name: value}), each with its own expected result. A parameter a test
    # leaves out is absent, which only an optional one may be. No cases: one unnamed test, as
    # every service before Phase 1 has.
    params: list = field(default_factory=list)
    cases: list = field(default_factory=list)
    # The declared family an EMPTY or HOLLOW expected result belongs to (emptiness.FAMILIES), or
    # None: the build fails on an empty or hollow result outside one (the improvement plan's
    # Phase 2, rule 4), and on a declared one that has rows.
    emptiness: str | None = None
    # A temporal root read at more than one version (Versions): every version, or those in a range.
    # None reads the version(s) in force at `as_of`.
    versions: object | None = None
    # Relation operations after the projection and any groupBy (Phase 4, stage 4): Extend, Window, Rename, Select,
    # Distinct, Sort, Cut, Concatenate and Join, in order.
    relation: list = field(default_factory=list)
    # A top-level `let` per (name, literal), before the query (Phase 4, stage 5): `{|let t = 42; <query>;}`. A
    # predicate reads one as a Param of that name.
    lets: list = field(default_factory=list)

    @property
    def short(self) -> str:
        return self.name.split("::")[-1]


_SERVICE = re.compile(r"^Service\s+([\w:]+)\s*$")
_PATTERN = re.compile(r"pattern:\s*'([^']*)'")
_DOC = re.compile(r"documentation:\s*'((?:[^']|'')*)'")
_ROOT = re.compile(r"query:\s*\|([\w:]+)\.all\(\s*([^)]*)\s*\)")
_ALIASED = re.compile(r"^(\w+)\s*:\s*(\w+)\s*\|\s*\$\2\.(.+)$")
_AGGED = re.compile(r"^(\w+)\s*:\s*(\w+)\s*\|\s*\$\2\.(.+?)->(count)\(\)$")
# `alias:x|$x->pkg::fn(args)` — a standalone function, not a property.
_FUNCCALL = re.compile(r"^(\w+)\s*:\s*(\w+)\s*\|\s*\$\2->([\w:]+)\(([^)]*)\)$")
_FILTER = re.compile(r"->filter\(\s*\{\s*(\w+)\s*\|(.*?)\}\s*\)", re.S)
_SORT = re.compile(r"->sort\(\s*~(\w+)->(\w+)\(\)\s*\)")
_LIMIT = re.compile(r"->limit\(\s*(\d+)\s*\)")
_GROUPBY = re.compile(r"->groupBy\(~\[([^\]]*)\],\s*~\[(.*?)\]\s*\)", re.S)
_AGGSPEC = re.compile(r"(\w+):\s*(\w+)\|\$\2\.(\w+)\s*:\s*agg\|\$agg->(\w+)\(\)")
_COND = re.compile(r"\$(\w+)\.([\w.]+)\s*(==|!=|<=|>=|<|>)\s*(.+)")


def _literal(tok: str):
    tok = tok.strip()
    if tok.startswith("'") and tok.endswith("'"):
        return tok[1:-1]
    if tok in ("true", "false"):
        return tok == "true"
    if re.fullmatch(r"-?\d+", tok):
        return int(tok)
    if re.fullmatch(r"-?\d*\.\d+", tok):
        return float(tok)
    raise ValueError(f"unhandled literal {tok!r}")


def _projections(body: str) -> list[Proj]:
    """Split the ~[...] body on top-level commas and classify each entry."""
    out, depth, cur = [], 0, []
    for ch in body:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur))

    projs = []
    for raw in out:
        e = " ".join(raw.split())
        if not e:
            continue
        m = _FUNCCALL.match(e)
        if m:
            args = [_literal(a) for a in m.group(4).split(",") if a.strip()]
            projs.append(Proj(m.group(1), [], None, args, m.group(3)))
            continue
        m = _AGGED.match(e)
        if m:
            projs.append(Proj(m.group(1), m.group(3).split("."), m.group(4)))
            continue
        m = _ALIASED.match(e)
        if m:
            path, args = m.group(3), []
            call = re.fullmatch(r"(.+?)\(([^)]*)\)", path)
            if call:
                path = call.group(1)
                args = [_literal(a) for a in call.group(2).split(",") if a.strip()]
            projs.append(Proj(m.group(1), path.split("."), None, args))
        elif re.fullmatch(r"\w+", e):
            projs.append(Proj(e, [e]))
        else:
            raise ValueError(f"unhandled projection entry {e!r}")
    return projs


def _tds_project(q: str):
    """Legacy `project([lambdas], ['names'])` -> the same Proj list the Relation form
    yields, so a spec round-trips whichever paradigm it was written in."""
    i = q.index("->project(")
    j = q.index("[", i) + 1
    depth, k = 1, j
    while depth:
        if q[k] == "[":
            depth += 1
        elif q[k] == "]":
            depth -= 1
            if depth == 0:
                break
        k += 1
    lams = q[j:k]
    m = re.search(r"\[([^\]]*)\]", q[k + 1:])
    names = [n.strip().strip("'") for n in m.group(1).split(",")]
    projs = []
    for raw, name in zip(_split_top(lams), names):
        e = " ".join(raw.split())
        mm = re.fullmatch(r"(\w+)\s*\|\s*\$\1\.(.+)", e)
        if not mm:
            raise ValueError(f"unhandled TDS projection lambda {e!r}")
        path, agg = mm.group(2), None
        am = re.fullmatch(r"(.+?)->(count)\(\)", path)
        if am:
            path, agg = am.group(1), am.group(2)
        args = []
        call = re.fullmatch(r"(.+?)\(([^)]*)\)", path)
        if call:
            path = call.group(1)
            args = [_literal(a) for a in call.group(2).split(",") if a.strip()]
        projs.append(Proj(name, path.split("."), agg, args))
    return projs


def _split_top(body: str) -> list[str]:
    out, depth, cur = [], 0, []
    for ch in body:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur))
    return out


def _project_body(q: str) -> str:
    """Extract the ~[ ... ] body of the project() call by bracket matching."""
    i = q.index("->project(")
    j = q.index("~[", i) + 2
    depth, k = 1, j
    while depth:
        if q[k] == "[":
            depth += 1
        elif q[k] == "]":
            depth -= 1
            if depth == 0:
                break
        k += 1
    return q[j:k]


def _preds(var: str, body: str) -> list[Pred]:
    out = []
    for part in body.split("&&"):
        part = part.strip().strip("()").strip()
        if not part:
            continue
        m = _COND.match(part)
        if not m:
            raise ValueError(f"unhandled filter condition {part!r}")
        if m.group(1) != var:
            raise ValueError(f"filter var {m.group(1)!r} != lambda var {var!r}")
        out.append(Pred(m.group(2).split("."), m.group(3), _literal(m.group(4))))
    return out


def parse(text: str) -> list[Spec]:
    specs, cur, buf = [], None, []
    for line in text.splitlines():
        m = _SERVICE.match(line)
        if m:
            if cur:
                specs.append(_finish(cur, "\n".join(buf)))
            cur, buf = m.group(1), []
            continue
        if cur:
            buf.append(line)
    if cur:
        specs.append(_finish(cur, "\n".join(buf)))
    return specs


def _finish(name: str, body: str) -> Spec:
    pat = _PATTERN.search(body)
    doc = _DOC.search(body)
    root = _ROOT.search(body)
    if not (pat and root):
        raise ValueError(f"service {name}: missing pattern or root")
    s = Spec(name, pat.group(1), doc.group(1) if doc else "", root.group(1))
    arg = root.group(2).strip()
    if arg:
        dates = [a.strip()[1:] for a in arg.split(",") if a.strip()]
        s.as_of = dates[0] if len(dates) == 1 else dates
    if re.search(r"->project\(\s*\[", body):
        s.paradigm = "tds"
        s.projections = _tds_project(body)
    else:
        s.projections = _projections(_project_body(body))
    f = _FILTER.search(body)
    if f:
        s.filters = _preds(f.group(1), f.group(2))
    so = _SORT.search(body)
    if so:
        d = so.group(2)
        if d not in ("ascending", "descending"):
            raise ValueError(f"unhandled sort direction {d}")
        s.sort = (so.group(1), d == "descending")
    gb = _GROUPBY.search(body)
    if gb:
        s.group_by = [k.strip() for k in gb.group(1).split(",") if k.strip()]
        s.aggs = [(m.group(1), m.group(3), m.group(4))
                  for m in _AGGSPEC.finditer(gb.group(2))]
    li = _LIMIT.search(body)
    if li:
        s.limit = int(li.group(1))
    if not s.projections:
        raise ValueError(f"service {name}: no projections")
    aliases = [p.alias for p in s.projections]
    if len(set(aliases)) != len(aliases):
        dupes = sorted({a for a in aliases if aliases.count(a) > 1})
        raise ValueError(f"service {name}: duplicate aliases {dupes}")
    for alias, _desc in ([] if not s.sort
                         else s.sort if isinstance(s.sort, list) else [s.sort]):
        if alias not in aliases:
            raise ValueError(f"service {name}: sorts on unprojected {alias!r}")
    return s


def short_name(c, cls: str) -> str:
    """A service-name fragment for a class, unique across the corpus.

    The generators name services after the SHORT class name, which was unique until two
    packages both defined a Confirmation -- ops:: has one and middleoffice:: has one -- and
    the dense generator emitted `D_ConfirmationDense` twice. The engine's complaint,
    "Duplicated element", names the service rather than the two classes behind it.

    Ambiguous short names get their package's last segment in front. Unambiguous ones do not,
    so no existing service is renamed and no quarantine entry keyed on a name is repointed.
    """
    short = cls.split("::")[-1]
    if not hasattr(c, "_short_counts"):
        counts: dict[str, int] = {}
        for k in c.classes:
            counts[k.split("::")[-1]] = counts.get(k.split("::")[-1], 0) + 1
        c._short_counts = counts
    if c._short_counts.get(short, 0) > 1:
        pkg = cls.split("::")[-2] if "::" in cls else ""
        return pkg[:1].upper() + pkg[1:] + short
    return short


def apply_temporal(c, spec) -> None:
    """Give a temporal root the date(s) `all()` requires.

    Every generator needs this and none of them had it, because until the milestoned classes
    were made reachable no generated service was ever rooted at one -- the two milestoned
    classes were islands and only hand-written services touched them. The moment they were
    reachable, three generators failed the BUILD in turn with

        counterparty::RatingVersion is businesstemporal, so all() needs 1 date(s)
        products::InstrumentRating is bitemporal, so all() needs 2 date(s)

    `latest` rather than a fixed date: every milestoned row in the seed is live at latest, so
    the expectation does not depend on which dates the seeder happened to choose, and the
    milestoning is still applied.
    """
    if spec.as_of is not None:
        return
    temporal = c.classes[spec.root].temporal if spec.root in c.classes else None
    if temporal == "bitemporal":
        spec.as_of = ["latest", "latest"]
    elif temporal:
        spec.as_of = "latest"


def load() -> list[Spec]:
    return parse(QUERIES.read_text())


if __name__ == "__main__":
    import model

    c = model.load()
    specs = load()
    print(f"{len(specs)} services\n")
    total = bad = fanout = 0
    for s in specs:
        errs = []
        for p in s.projections:
            total += 1
            try:
                if c.resolve_derived(s.root, p.path) is None:
                    c.resolve(s.root, p.path)
                if c.to_many_on(s.root, p.path):
                    fanout += 1
            except KeyError as e:
                errs.append(f"{p.alias}: {e}")
        for f in s.filters:
            try:
                c.resolve(s.root, f.path)
            except KeyError as e:
                errs.append(f"filter {'.'.join(f.path)}: {e}")
        bad += len(errs)
        flags = []
        if s.filters:
            flags.append(f"filter x{len(s.filters)}")
        if s.sort:
            flags.append("sort" + ("v" if s.sort[1] else "^"))
        if s.limit:
            flags.append(f"limit {s.limit}")
        print(f"  {s.short:<34} root={s.root:<28} cols={len(s.projections):>2} "
              f"{' '.join(flags):<22} {'OK' if not errs else 'UNRESOLVED ' + str(len(errs))}")
        for e in errs[:4]:
            print("       !", e)
    print(f"\n{total} projection paths, {bad} unresolved, {fanout} cross a to-many hop")
