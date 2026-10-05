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


@dataclass
class Pred:
    path: list[str]
    op: str
    value: object


@dataclass
class Proj:
    alias: str
    path: list[str]
    agg: str | None = None      # None -> a column; 'count' -> over the landed SET
    args: list = field(default_factory=list)   # arguments to a QUALIFIED property
    # A standalone function called extension-style: `$x->stress::rules::isBuy()`.
    func: str | None = None


@dataclass
class Spec:
    name: str            # stress::Q0_...
    pattern: str
    doc: str
    root: str            # trading::Trade
    # Business date for a <<temporal.businesstemporal>> root: an ISO date, or the literal
    # 'latest'. None for a non-temporal class, where all() takes no argument.
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
    #   aggs      (result alias, source alias, 'count'|'sum'|'max'|'min')
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
