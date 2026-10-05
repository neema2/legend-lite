"""
Which features are EXECUTED, as distinct from present.

density.py counts feature occurrences in the sources. That is the right measure of whether
the corpus USES a construct and the wrong measure of whether anything TESTS it: every object
in 60-dense-store.pure -- a Schema, a TabularFunction, a MultiGrainFilter, a View, and five
joins -- was declared exactly once, referenced by no mapping, and reached by no service,
while scoring as present. Nine taxonomy entries were decoration.

So this asks a different question, per feature: is there a service that RUNS, whose expected
value would CHANGE if the feature behaved differently?

That question cannot be answered by pattern-matching, so each feature has a predicate over
the resolved model and the generated specs instead. A feature is executed when some
non-quarantined spec touches a site that uses it:

    class-mapping features   a spec whose root, or a class on one of its projection paths,
                             is the class the mapping maps
    store features           a spec that reads the table, or follows the join, involved

A quarantined spec does not count. A known-failing service pins a defect, which is valuable,
but it does not demonstrate that the feature works -- and counting it would let a feature be
"executed" by a test that never passes.

Where a predicate cannot be written, the feature is reported UNKNOWN and counted as NOT
executed. Conservative on purpose: the failure this file exists to prevent is a coverage
number that flatters, and an optimistic default would rebuild exactly that.
"""
from __future__ import annotations

import sys

import re

import model

# The mapping a service runs against when its spec names none.
DEFAULT_MAPPING = "stress::AllMapping"


# ---------------------------------------------------------------- body attribution
#
# The SAME patterns density.py uses, applied per class mapping and attributed to the class
# it maps. That coupling is deliberate: `present` and `executed` must differ only in the
# QUESTION asked, never in what counts as a use. Two pattern sets would drift, and the first
# symptom would be a feature reported present-but-unexecuted purely because the two files
# disagreed about what it looks like.
#
# density's own block regex does not capture the class name -- it only counts -- so this
# repeats it with the name captured.
_BLOCK = re.compile(
    r"^\s*\*?([\w:]+)(?:\[\w+\])?(?:\s+extends\s*\[\w+\])?\s*:\s*"
    r"(Relational|Pure|Operation|XStore|AggregationAware|Relation)\s*\{(.*?)\n\s*\}",
    re.S | re.M)

# A class mapping's KIND is a feature in its own right, and one that no body pattern can
# see: `cls: XStore { ... }` differs from `cls: Relational { ... }` in the header alone.
_KIND_FEATURE = {"XStore": "E3  XStore",
                 "AggregationAware": "E4  AggregationAware",
                 "Operation": "E6  Operation union"}

# `include other::Mapping[storeA->storeB]` -- store substitution, which is declared at
# MAPPING level and belongs to no class.
_SUBSTITUTION = re.compile(r"include\s+[\w:]+\s*\[[\w:]+\s*->")
# A plain `include other::Mapping`. A service naming one mapping resolves classes mapped in
# everything it includes, so attributing a feature to the mapping that DECLARES it and
# comparing against the mapping a service NAMES misses every included one -- which is most
# of them, since this corpus has one aggregate mapping including the rest.
# The trailing group is a STORE SUBSTITUTION -- `include M [dbA -> dbB]` -- which is still
# an include, and was not matched while the pattern anchored on end-of-line.
_INCLUDE = re.compile(r"^\s*include\s+([\w:]+)\s*(?:\[[^\]]*\])?\s*$", re.M)


def include_closure() -> dict[str, set[str]]:
    """mapping -> itself and every mapping it includes, transitively."""
    import density

    src, _blocks = density.load()
    direct: dict[str, set[str]] = {}
    heads = list(_MAPPING_HEAD.finditer(src))
    for i, h in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(src)
        direct[h.group(1)] = set(_INCLUDE.findall(src[h.start():end]))
    out = {}
    for name in direct:
        seen, stack = {name}, [name]
        while stack:
            cur = stack.pop()
            for nxt in direct.get(cur, ()):
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        out[name] = seen
    return out


def class_mappings() -> dict[str, set[str]]:
    """class -> every mapping that maps it.

    Not c.declared_in, which holds one mapping per class and so cannot answer "can THIS
    mapping resolve this class" for the four services that deliberately name an alternative
    mapping of trading::Trade or hier::Issuer.
    """
    import density

    src, _blocks = density.load()
    out: dict[str, set[str]] = {}
    heads = list(_MAPPING_HEAD.finditer(src))
    for i, h in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(src)
        for m in _SET_IMPL.finditer(src[h.start():end]):
            out.setdefault(m.group(1), set()).add(h.group(1))
    return out


_MAPPING_HEAD = re.compile(r"^Mapping\s+([\w:]+)", re.M)
# A set implementation head: an optional `*` for the root set, the class, an optional set id,
# an optional `extends`, then the mapping kind.
_SET_IMPL = re.compile(
    r"^\s*\*?([\w:]+)\s*(?:\[\w+\])?\s*(?:extends\s*\[\w+\]\s*)?:\s*"
    r"(?:Relational|Relation|Operation|Pure|XStore|AggregationAware)\b", re.M)


def class_features() -> dict[tuple[str, str], set[str]]:
    """(mapping, class) -> the taxonomy ids that class mapping's body uses.

    Keyed by MAPPING as well as class, because a class mapped twice does not inherit one
    mapping's features into the other. trading::Trade is mapped plainly in the main mapping
    and with `Otherwise` in reporting::OtherwiseMapping; keyed by class alone, Otherwise
    counted as executed because a passing service reaches the PLAIN mapping -- while the
    only service using the Otherwise one is quarantined under F13. That is the flattering
    this file exists to prevent, arriving through the same class-collision the reader has.
    """
    import density

    src, _blocks = density.load()
    body_pats = [(n, p) for n, p, scope in density.FEATURES if scope == "body"]
    out: dict[tuple[str, str], set[str]] = {}
    heads = list(_MAPPING_HEAD.finditer(src))
    for i, h in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(src)
        section = src[h.start():end]
        for cls, kind, body in _BLOCK.findall(section):
            for name, pat in body_pats:
                if re.search(pat, body, re.M):
                    out.setdefault((h.group(1), cls), set()).add(name)
            if kind in _KIND_FEATURE:
                out.setdefault((h.group(1), cls), set()).add(_KIND_FEATURE[kind])
    return out


def mapping_features() -> dict[str, set[str]]:
    """mapping -> constructs declared at MAPPING level, belonging to no class."""
    import density

    src, _blocks = density.load()
    out: dict[str, set[str]] = {}
    heads = list(_MAPPING_HEAD.finditer(src))
    for i, h in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(src)
        if _SUBSTITUTION.search(src[h.start():end]):
            out.setdefault(h.group(1), set()).add("C2  store substitution")
    return out


def _spec_touches(c: model.Corpus, spec) -> tuple[set[str], set[tuple[str, str]]]:
    """(classes, properties) the spec's evaluation passes through.

    Both are needed, and conflating them flatters the report. A property-level feature has
    to be PROJECTED to be executed, not merely to live on a class some service reads:
    hier::Issuer carries a Binding transformer and is read by H_Issuer, which does not
    project it -- so counting the class alone reported the Binding as executed while the
    only service that projects one is quarantined under F27.
    """
    classes, props = {spec.root}, set()
    for p in getattr(spec, "projections", []):
        cls = spec.root
        for step in p.path:
            props.add((cls, step))
            end = c.ends.get((cls, step))
            if end is not None:
                # The ASSOCIATION itself, not only the class it leads to. An
                # AssociationMapping's body is attributed to the association, so a feature
                # written there -- explicit source and target set ids, for one -- is
                # credited only if something records that a service navigated it.
                if end.assoc:
                    classes.add(end.assoc)
                cls = end.target
            elif (cls, step) in c.embedded:
                cls = c.embedded[(cls, step)]
            else:
                break
            classes.add(cls)
    tree = getattr(spec, "graph", None)
    if tree:
        stack = [(spec.root, tree)]
        while stack:
            cls, node = stack.pop()
            for step, sub in (node or {}).items():
                props.add((cls, step))
                end = c.ends.get((cls, step))
                nxt = end.target if end is not None else c.embedded.get((cls, step))
                if end is not None and end.assoc:
                    classes.add(end.assoc)
                if nxt:
                    classes.add(nxt)
                    if isinstance(sub, dict):
                        stack.append((nxt, sub))
    for pred in getattr(spec, "filters", []):
        props.add((spec.root, pred.path[-1]))
    for key in getattr(spec, "group_by", []):
        props.add((spec.root, key))
    return classes, props


def _spec_tables(c: model.Corpus, classes: set[str]) -> set[str]:
    return {c.main_table[k] for k in classes if k in c.main_table}


def _spec_joins(c: model.Corpus, spec, classes: set[str]) -> set[str]:
    """Joins the spec follows: association ends it navigates, and chains it projects."""
    out = set()
    for p in getattr(spec, "projections", []):
        cls = spec.root
        for step in p.path:
            end = c.ends.get((cls, step))
            if end is not None:
                if end.join:
                    out.add(end.join)
                cls = end.target
                continue
            chain = c.chains.get((cls, step))
            if chain is not None:
                out.update(chain[0])
            dyn = c.dyna.get((cls, step))
            if dyn is not None:
                import rhs
                for joins, _t, _col in rhs.chains(("call", dyn)):
                    out.update(joins)
            if (cls, step) in c.embedded:
                cls = c.embedded[(cls, step)]
    for cls in classes:
        chained = c.class_filter_chain.get(cls)
        if chained:
            out.update(chained[0])
    return out


def reached(c: model.Corpus, specs, quarantined: set[str]):
    """(classes, properties, tables, joins) reached by a spec that is expected to PASS."""
    classes, props, tables, joins = set(), set(), set(), set()
    for spec in specs:
        if spec.name in quarantined:
            continue
        ks, ps = _spec_touches(c, spec)
        classes |= ks
        props |= ps
        tables |= _spec_tables(c, ks)
        joins |= _spec_joins(c, spec, ks)
    return classes, props, tables, joins


# ---------------------------------------------------------------- the predicates
#
# Keyed to density.py's taxonomy ids so the two reports line up entry for entry: `present`
# there and `executed` here are meant to be read side by side, and the gap between them is
# the thing worth looking at.
def report(c: model.Corpus, specs, quarantined: set[str]) -> list[tuple[str, bool, str]]:
    ks, ps, ts, js = reached(c, specs, quarantined)
    # (mapping, class) pairs a passing service actually resolves through. A spec with no
    # explicit mapping runs against the corpus's default one.
    closure = include_closure()
    reachable = set()
    used_mappings: set[str] = set()
    for spec in specs:
        if spec.name in quarantined:
            continue
        mp = getattr(spec, "mapping", None) or DEFAULT_MAPPING
        mps = closure.get(mp, {mp})
        used_mappings |= mps
        for k in _spec_touches(c, spec)[0]:
            for m in mps:
                reachable.add((m, k))

    def any_prop(d, pred=lambda k: True):
        return sorted(k for k in d if k in ps and pred(k))

    def joins_where(pred):
        return sorted(n for n in js if pred(c.joins[n]))

    out = []

    def add(name, hits, note=""):
        out.append((name, bool(hits), note or (f"{len(hits)} site(s): "
                                               f"{', '.join(map(str, list(hits)[:2]))}"
                                               if hits else "no executing service")))

    add("A2  join chain", [k for k in c.chains if k in ps])
    add("A3  dynafunction", any_prop(c.dyna))
    add("A4  dyna over join",
        [k for k, v in c.dyna.items() if k in ps and _has_chain(v)])
    add("A5  enum transformer", any_prop(c.enum_props))
    add("A6  embedded", [k for k, v in c.embedded.items()
                         if k in ps and v not in c.json_backed])
    add("A9  Binding transformer", [k for k in c.bindings if k in ps])
    add("B2  ~filter", [k for k in c.class_filter if k in ks])
    add("B3  ~filter via join", [k for k in c.class_filter_chain if k in ks])
    add("B9  extends [id]", [k for k in ks if c.classes.get(k) and c.classes[k].supertype])
    add("D6  Filter", ([c.class_filter[k] for k in c.class_filter if k in ks]
                       + [c.class_filter_chain[k][1] for k in c.class_filter_chain
                          if k in ks]))
    add("D7  View", [v for v in c.views if v in ts])
    add("D3  multi-column join", joins_where(lambda j: _cond_kind(j, "and")))
    add("--  join non-equality", joins_where(lambda j: _cond_kind(j, "ineq")))
    add("--  join with or", joins_where(lambda j: _cond_kind(j, "or")))
    add("D4  join w/ dynafunction", joins_where(lambda j: _cond_kind(j, "call")))
    add("D5  self-join {target}", joins_where(lambda j: j.self_join))
    add("--  milestoning", [t for t in ts if c.tables[t].milestoning])
    add("--  composite PK", [t for t in ts if len(c.tables[t].pk) > 1])
    add("D8  Schema", [n for n in ts if c.tables[n].schema != "default"])

    # Everything else density.py counts in a class-mapping BODY, attributed to its class and
    # asked whether any passing service reaches that class. Reported for every body feature
    # rather than a chosen few, because the entries left out of a coverage report are
    # precisely the ones nobody looks at.
    # Mapping-level constructs: executed when a PASSING service runs against that mapping.
    for mp, names in sorted(mapping_features().items()):
        for name in sorted(names):
            add(name, [mp] if mp in used_mappings else [])

    byclass = class_features()
    covered = {n for n, _ok, _w in out}
    for name in sorted({n for fs in byclass.values() for n in fs}):
        if name in covered:
            continue
        add(name, sorted(f"{m}/{k.split('::')[-1]}" for (m, k), fs in byclass.items()
                         if name in fs and (m, k) in reachable))
    # NOT a gap in the corpus. `###Data` materializes Tables only, so a mapping over a
    # TabularFunction fails at test-session setup with the function reported as a missing
    # table -- see repro/tabularfunction-untestable/, where a real Table in the same Schema
    # seeded the same way is the control. The construct is unreachable by any service test,
    # so it is reported separately rather than counted against the total: closing it is not
    # something the corpus can do.
    out.append(("D10 TabularFunction", None,
                "UNTESTABLE by construction -- ###Data materializes Tables only; see "
                "repro/tabularfunction-untestable/"))
    add("D11 MultiGrainFilter",
        [n for k in c.class_filter if k in ks
         for n in [c.class_filter[k]] if n in c.multigrain])
    return out


def _has_chain(dyn) -> bool:
    import rhs
    return bool(rhs.chains(("call", dyn)))


def _cond_kind(j, kind: str) -> bool:
    """Whether a join's condition contains a given shape."""
    if j.condition is None:
        return False

    def walk(node):
        tag, body = node
        if tag in ("and", "or"):
            return (tag == kind) or walk(body[0]) or walk(body[1])
        if tag == "cmp":
            left, op, right = body
            if kind == "ineq" and op != "=":
                return True
            if kind == "call" and (left[0] == "call" or right[0] == "call"):
                return True
            return False
        return False

    return walk(j.condition)


# One computation per corpus per run (Bazel workplan P2-05): build.py and all_specs both need the base list, and
# spread.build both need its result; the lists are read, never mutated, so the same object is handed back.
_BASE_SPECS: dict[int, list] = {}


def base_specs(c: model.Corpus):
    """Every spec except the coverage-directed ones.

    Split out because spread.py needs a BASELINE to measure against, and that baseline has to
    be identical in the two places it is computed -- build.py, which emits the services, and
    all_specs, which the scoreboard reads. Computing it separately in each was the first
    version, and the two disagreed: the generator selected against one set and the scoreboard
    scored against another, so the file and the number described different corpora.
    """
    import flat
    import aggregates
    import battery
    import combos
    import graphs
    import hier
    import query
    import stacks
    import taxonomy
    import tomany

    if id(c) in _BASE_SPECS:
        return _BASE_SPECS[id(c)]
    tables = flat.all_tables(c)
    seeded = {k for k, v in tables.items() if v}
    _BASE_SPECS[id(c)] = (query.load() + list(battery.SPECS) + stacks.build(c, seeded)
            + graphs.build(c, seeded, tables) + aggregates.build(c, seeded, tables)
            + hier.specs(c) + combos.specs(c)
            + tomany.build(c, seeded, tables)
            + taxonomy.build(c, seeded, tables))
    return _BASE_SPECS[id(c)]


def all_specs(c: model.Corpus):
    """Every spec the corpus emits, from every generator."""
    import flat
    import spread

    base = base_specs(c)
    tables = flat.all_tables(c)
    seeded = {k for k, v in tables.items() if v}
    return base + spread.build(c, seeded, base)


# Every taxonomy feature that a service can execute, all of them with a passing one today.
# A ratchet, not a target: build.py fails if any stops executing.
#
# D10 TabularFunction is absent and cannot be added: `###Data` materializes Tables only, so a
# mapping over one fails at test-session setup. That is a property of the platform's test
# framework rather than a gap here -- see the note in report() and
# repro/tabularfunction-untestable/.
BASELINE = {
    "--  composite PK", "--  join non-equality", "--  join with or", "--  milestoning",
    "A10 src/tgt ids", "A2  join chain", "A3  dynafunction", "A4  dyna over join",
    "A5  enum transformer", "A6  embedded", "A7  Otherwise", "A8  Inline",
    "A9  Binding transformer", "B10 scope block", "B2  ~filter", "B3  ~filter via join",
    "B4  ~distinct", "B5  ~groupBy", "B6  ~primaryKey", "B9  extends [id]",
    "C2  store substitution", "C5  local property +", "D11 MultiGrainFilter",
    "D3  multi-column join", "D4  join w/ dynafunction", "D5  self-join {target}",
    "D6  Filter", "D7  View", "D8  Schema", "E2  Pure/M2M ~src", "E3  XStore",
    "E4  AggregationAware", "E5  Relation ~func", "E6  Operation union",
}


def regressions(rows) -> list[str]:
    return sorted(n for n, ok, _w in rows if ok is False and n in BASELINE)


if __name__ == "__main__":
    import quarantine

    c = model.load()
    rows = report(c, all_specs(c), set(quarantine.ENGINE_QUARANTINE) | set(quarantine.HANGS))
    done = sum(1 for _n, ok, _w in rows if ok)
    blocked = sum(1 for _n, ok, _w in rows if ok is None)
    measurable = len(rows) - blocked
    for name, ok, why in rows:
        mark = "n/a " if ok is None else ("EXEC" if ok else "  --")
        print(f"  {mark}  {name:<26} {why}")
    print(f"\n{done} of {measurable} testable features are EXECUTED by a passing service"
          + (f"; {blocked} untestable by construction" if blocked else ""))
    if "--gate" in sys.argv:
        gaps = [n for n, ok, _w in rows if ok is False]
        if gaps:
            raise SystemExit(f"\nnot executed: {', '.join(gaps)}")
