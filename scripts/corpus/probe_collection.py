"""
Which COLLECTION functions execute, one column per function over a to-many end.

The fourth probe, and the last shape the other three cannot reach. Scalars live in a property
mapping, aggregates in a groupBy, relation operations in a query; collection functions live
over a NAVIGATION -- `$x.children->exists(...)` -- and need a parent with children before any
of them can be written at all.

That structural requirement is why this block went untested longest. Adding a scalar to a
probe costs a line; adding a collection function costs a second table, a join, an
association, and a seed where the parent-child cardinalities are interesting.

The seed gives each parent a DIFFERENT shape, because most of these functions are only
distinguishable across shapes:

    P1   children 3, 1, 2      several, unsorted, distinct
    P2   children 5, 5         several, all EQUAL      -> distinct/removeDuplicates bite here
    P3   no children           EMPTY                   -> isEmpty, exists, and the fold-style
                                                          functions have to survive it

The empty parent is the important one. Every aggregate-shaped collection function has an
answer over an empty collection that differs from its answer over a non-empty one, and the
corpus has already found one defect (F6, count() over an empty to-many returning 1) that only
appears there. A probe with three well-populated parents would have missed it.
"""
from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run as runner  # noqa: E402

# parent -> the child values it owns
KIDS = {"P1": [3, 1, 2], "P2": [5, 5], "P3": []}

# (name, Pure return type, expression over $x.kids, oracle result per parent)
#
# The oracle column is a plain Python function of the child list rather than a call into
# oracle.IMPL, because these are the COLLECTION overloads and IMPL is keyed by bare name --
# `contains` there is the string one. Keying two different functions by one name is the
# collision the relation registry was split off to avoid, and the collection family has the
# same problem waiting; until it is split, going through IMPL here would compare against the
# wrong implementation and call the agreement meaningful.
CASES = [
    ("size", "Integer", "$x.kids->size()", len),
    ("isEmpty", "Boolean", "$x.kids->isEmpty()", lambda k: len(k) == 0),
    ("isNotEmpty", "Boolean", "$x.kids->isNotEmpty()", lambda k: len(k) > 0),
    ("exists", "Boolean", "$x.kids->exists(c|$c.v > 2)", lambda k: any(v > 2 for v in k)),
    ("map", "Integer", "$x.kids->map(c|$c.v)->sum()", sum),
    ("filter", "Integer", "$x.kids->filter(c|$c.v > 1)->size()",
     lambda k: len([v for v in k if v > 1])),
    ("distinct", "Integer", "$x.kids->map(c|$c.v)->distinct()->size()",
     lambda k: len(set(k))),
    ("removeDuplicates", "Integer", "$x.kids->map(c|$c.v)->removeDuplicates()->size()",
     lambda k: len(set(k))),
    ("sort", "Integer", "$x.kids->map(c|$c.v)->sort()->first()",
     lambda k: sorted(k)[0] if k else None),
    ("reverse", "Integer", "$x.kids->map(c|$c.v)->reverse()->first()",
     lambda k: k[-1] if k else None),
    ("first", "Integer", "$x.kids->map(c|$c.v)->first()", lambda k: k[0] if k else None),
    ("last", "Integer", "$x.kids->map(c|$c.v)->last()", lambda k: k[-1] if k else None),
    ("contains", "Boolean", "$x.kids->map(c|$c.v)->contains(5)", lambda k: 5 in k),
    ("in", "Boolean", "5->in($x.kids->map(c|$c.v))", lambda k: 5 in k),
    ("greatest", "Integer", "$x.kids->map(c|$c.v)->greatest()",
     lambda k: max(k) if k else None),
    ("least", "Integer", "$x.kids->map(c|$c.v)->least()", lambda k: min(k) if k else None),
    ("and", "Boolean", "$x.kids->map(c|$c.v > 0)->and()", lambda k: all(v > 0 for v in k)),
    ("or", "Boolean", "$x.kids->map(c|$c.v > 2)->or()", lambda k: any(v > 2 for v in k)),
    ("union", "Integer", "$x.kids->map(c|$c.v)->union([9])->size()", lambda k: len(k) + 1),
    ("limit", "Integer", "$x.kids->map(c|$c.v)->limit(2)->size()",
     lambda k: min(len(k), 2)),
    ("paginated", "Integer", "$x.kids->map(c|$c.v)->paginated(1, 2)->size()",
     lambda k: len(k[0:2])),
    ("sortByReversed", "Integer",
     "$x.kids->sortByReversed(c|$c.v)->map(c|$c.v)->first()",
     lambda k: sorted(k, reverse=True)[0] if k else None),
    ("add", "Integer", "$x.kids->map(c|$c.v)->add(9)->size()", lambda k: len(k) + 1),
    ("concatenate", "Integer", "$x.kids->map(c|$c.v)->concatenate([7, 8])->size()",
     lambda k: len(k) + 2),
    ("init", "Integer", "$x.kids->map(c|$c.v)->init()->size()",
     lambda k: max(len(k) - 1, 0)),
    ("tail", "Integer", "$x.kids->map(c|$c.v)->tail()->size()",
     lambda k: max(len(k) - 1, 0)),
    ("take", "Integer", "$x.kids->map(c|$c.v)->take(2)->size()", lambda k: min(len(k), 2)),
    ("drop", "Integer", "$x.kids->map(c|$c.v)->drop(1)->size()",
     lambda k: max(len(k) - 1, 0)),
    ("slice", "Integer", "$x.kids->map(c|$c.v)->slice(0, 2)->size()",
     lambda k: len(k[0:2])),
    ("sortBy", "Integer", "$x.kids->sortBy(c|$c.v)->map(c|$c.v)->first()",
     lambda k: sorted(k)[0] if k else None),
    ("joinStrings", "String", "$x.kids->map(c|$c.v->toString())->joinStrings('-')",
     lambda k: "-".join(str(v) for v in k)),
    ("count", "Integer", "$x.kids->count()", len),
]

MODEL = """###Pure
Class col::P
{{
   k: String[1];
}}

Class col::C
{{
   ck: String[1];
   pk: String[1];
   v: Integer[1];
}}

Association col::PC
{{
   parent: col::P[1];
   kids: col::C[*];
}}

###Relational
Database col::DB
(
   Table P ( K VARCHAR(10) PRIMARY KEY )
   Table C ( CK VARCHAR(10) PRIMARY KEY, PK VARCHAR(10), V INTEGER )
   Join PC ( P.K = C.PK )
)

###Mapping
Mapping col::M
(
   col::P: Relational
   {{
      ~primaryKey ( [col::DB]P.K )
      ~mainTable [col::DB]P
      k: [col::DB]P.K
   }}
   col::C: Relational
   {{
      ~primaryKey ( [col::DB]C.CK )
      ~mainTable [col::DB]C
      ck: [col::DB]C.CK,
      pk: [col::DB]C.PK,
      v: [col::DB]C.V
   }}
   col::PC: Relational
   {{
      AssociationMapping
      (
         kids[col_P, col_C]: [col::DB]@PC,
         parent[col_C, col_P]: [col::DB]@PC
      )
   }}
)

###Connection
RelationalDatabaseConnection col::Conn
{{ store: col::DB; type: DuckDB; specification: DuckDB {{ }}; auth: Test; }}

###Runtime
Runtime col::RT
{{ mappings: [ col::M ]; connections: [ col::DB: [ env: col::Conn ] ]; }}

###Data
Data col::Seed
{{
  Relational
  #{{
    default.P:
      'K\\n' +
      'P1\\n' +
      'P2\\n' +
      'P3\\n';
    default.C:
      'CK,PK,V\\n' +
      'C1,P1,3\\n' +
      'C2,P1,1\\n' +
      'C3,P1,2\\n' +
      'C4,P2,5\\n' +
      'C5,P2,5\\n';
  }}#
}}

###Service
Service col::Svc
{{
   pattern: '/col/svc';
   documentation: 'One column per collection function over a to-many end, three parent shapes.';
   execution: Single
   {{
      // The RELATION form of project, because the sort below is the relation form. Mixing
      // them -- `project([...], [...])` followed by `sort(~k->ascending())` -- fails with
      // "no matching signature" for `sort`, and the message names sort rather than the
      // projection that produced the wrong type, so every case in the probe reported the
      // same error and none of them was at fault.
      query: |col::P.all()->project(~[k:x|$x.k{cols}])
                 ->sort(~k->ascending());
      mapping: col::M;
      runtime: col::RT;
   }}
   testSuites:
   [
      Svc_suite:
      {{
         data: [ connections: [ env: Reference #{{ col::Seed }}# ] ]
         tests:
         [
            expected_rows:
            {{
               serializationFormat: PURE_TDSOBJECT;
               asserts: [ a: EqualToJson #{{ expected: ExternalFormat #{{
                   contentType: 'application/json'; data: '{data}'; }}#; }}# ]
            }}
         ]
      }}
   ]
}}
"""


# The disagreements the improvement plan's Phase 4 adjudicated (2026-10-28), each the ENGINE's, by finding (see
# probe_functions.ADJUDICATED). `first`'s expectation over P1 assumes the seed's order, which no store promises; its
# verdict stands on the row count, one row per child.
ADJUDICATED = {
    **{n: "the engine's: F6, an aggregate over an empty to-many" for n in ("count", "map", "joinStrings")},
    **{n: "the engine's: F43, not applied over a to-many (a row per child)"
       for n in ("first", "sortBy", "greatest", "least", "and", "or")},
}


def expected(cases) -> list:
    rows = []
    for p in sorted(KIDS):
        row = {"k": p}
        for i, (_n, _r, _e, fn) in enumerate(cases):
            row[f"f{i}"] = fn(KIDS[p])
        rows.append(row)
    return rows


def build(cases) -> str:
    cols = "".join(f", f{i}:x|{expr}" for i, (_n, _r, expr, _f) in enumerate(cases))
    names = ""
    return MODEL.format(cols=cols, names=names,
                        data=json.dumps(expected(cases)).replace("'", "\\'"))


def attempt(cases) -> tuple[str, str]:
    work = Path(tempfile.mkdtemp())
    src = work / "col.pure"
    src.write_text(build(cases), encoding="utf-8")
    _status, out = runner.launch([str(src), "--testable=col::Svc", f"--dump={work}"], work)
    if re.search(r"\bPASS\b", out):
        return "ok", "agrees"
    dumps = sorted(work.glob("*.actual.json"))
    if dumps:
        return "differs", dumps[0].read_text()
    for pat, why in (
            (r"The function '(\w+)'.{0,80}?is not supported yet", "not supported yet"),
            (r"dyna function \[(\w+)\] is not registered", "not in the DynaFunctionRegistry"),
            (r"No SQL translation exists for the PURE function '([\w:]+)'", "no SQL translation"),
            (r"Can't find a match for function '([\w:]+)", "no matching signature")):
        m = re.search(pat, out, re.S)
        if m:
            return m.group(1).split("::")[-1], why
    m = re.search(r"(?:EngineException|Caused by)[^\n]*", out)
    return "ERROR", (m.group(0) if m else out[-300:])


def bisect(cases):
    """Halve a set that fails for a reason no message attributes to a function."""
    if len(cases) == 1:
        return [], cases
    mid = len(cases) // 2
    good, bad = [], []
    for half in (cases[:mid], cases[mid:]):
        if attempt(half)[0] == "ok":
            good += half
        else:
            g, b = bisect(half)
            good += g
            bad += b
    return good, bad


def main() -> None:
    cases, rejected = list(CASES), []
    for _round in range(len(CASES) + 1):
        outcome, detail = attempt(cases)
        if outcome == "ok":
            print(f"{len(cases)} collection functions execute and agree with the oracle.")
            break
        if outcome in ("ERROR", "differs"):
            if outcome == "differs":
                _report_diff(cases, detail)
            print(f"  bisecting {len(cases)} cases", flush=True)
            cases, dropped = bisect(cases)
            for c in dropped:
                out2, why2 = attempt([c])
                why2 = why2 if out2 not in ("ok",) else "fails only in company"
                if out2 == "differs":
                    why2 = "DISAGREES with the oracle" + (f" -- {ADJUDICATED[c[0]]}"
                                                          if c[0] in ADJUDICATED else "")
                rejected.append((c[0], why2))
                print(f"  REJECTED  {c[0]:<20} {why2[:90]}", flush=True)
            if not cases:
                break
            continue
        rejected.append((outcome, detail))
        print(f"  REJECTED  {outcome:<20} {detail}", flush=True)
        cases = [c for c in cases if c[0] != outcome]

    # Failures are recorded too. A function the probe RAN and could not pass is a different
    # thing from one nobody has tried, and leaving it out of the file made 20 documented
    # failures reappear in the scoreboard's "never run" list.
    _merge_evidence([c[0] for c in cases], rejected)
    print(f"\n  {len(cases)} executed and agreeing, {len(rejected)} rejected")


def _report_diff(cases, actual) -> None:
    try:
        got = json.loads(actual[actual.index("["):])
    except (ValueError, json.JSONDecodeError):
        return
    want = expected(cases)
    # ROW COUNT first, and stop there if it differs. Zipping a 6-row result against a 3-row
    # expectation produces a list of per-cell "disagreements" that look like values leaking
    # between parents -- I read exactly that into `first()` before checking the shape, and it
    # was wrong. One row per parent is the claim; if the engine returned one row per CHILD,
    # every cell comparison after that is comparing unrelated rows.
    if len(got) != len(want):
        print(f"    {cases[0][0] if len(cases) == 1 else 'result'}: "
              f"{len(want)} rows expected, {len(got)} returned -- the operation did not "
              f"reduce the collection, so the comparison stops here")
        return
    for w, g in zip(want, got):
        for i, (name, _r, _e, _f) in enumerate(cases):
            if w.get(f"f{i}") != g.get(f"f{i}"):
                print(f"    {name:<18} parent {w['k']}: "
                      f"oracle={w.get(f'f{i}')!r} engine={g.get(f'f{i}')!r}")


def _merge_evidence(names, failing=()) -> None:
    import probe_functions

    # a disagreement's verdict whole, since it names the finding; an engine message cut to its first 60 characters
    probe_functions.merge_evidence("collection", sorted(set(names)),
                                   [(n, w if w.startswith("DISAGREES") else w[:60]) for n, w in failing])


if __name__ == "__main__":
    main()
