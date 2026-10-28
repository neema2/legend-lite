"""
Which expected results may be empty or hollow (the improvement plan's Phase 2, its rule 4).

An expected result with no rows can only catch rows that should not be there. A HOLLOW one --
rows, but nothing past the first column, or a tree whose deepest level is never reached --
cannot catch a wrong navigated value, which is what a deep query exists to test. On
2026-10-19 2,200 of 4,736 expected results were empty and 280 hollow, because the deep
generators navigated chains the seed broke and filtered on subtypes the seed never filled.

So an empty or hollow expectation is allowed only in a declared FAMILY, where the emptiness is
the assertion: a spec states its family (Spec.emptiness), and check() fails the build for an
empty or hollow result outside one -- and for a declared one that has rows, so a declaration
cannot outlive the reason for it.

classify() is the report's own reading (docs/stress-corpus-feature-report-2026-09-29/tools/
expectations.py), so what the build allows is what the report counts.
"""
from __future__ import annotations

# The families, each with the reason its emptiness is the point.
EMPTY_SUBTYPE = "empty subtype"
BEFORE_FIRST_VERSION = "before the first version"
EXCLUDES_ALL = "excludes everything"

FAMILIES = {
    EMPTY_SUBTYPE: "a subtype the seed holds no row of, projected bare: a wrong ~filter returns "
                   "the whole table, so an empty answer is the evidence that the filter works "
                   "(taxonomy.py, hier.py)",
    BEFORE_FIRST_VERSION: "a milestoned query at a date before any version begins, which must "
                          "find nothing",
    EXCLUDES_ALL: "a filter built to exclude every row: `==` against a value no row holds, which "
                  "an absent value does not equal either",
}
# The plan named a fourth, isEmpty over a to-many end, and no service needs it: tomany.py's
# emptiness services project the test as a column, so they expect rows of booleans.


def declare_subtype(c, spec, rows) -> None:
    """A bare projection over a class whose set has a store ~filter, expecting nothing, is an
    empty subtype: no filter, grouping or tree of its own could have emptied it."""
    bare = not spec.filters and not spec.post_filters and not spec.group_by and spec.graph is None
    if bare and not rows and (spec.root in c.class_filter or spec.root in c.class_filter_chain):
        spec.emptiness = EMPTY_SUBTYPE


def _depths(v, d=0):
    """(deepest level a key is present at, deepest level holding a non-null value)."""
    asked, reached = d, d if v is not None else d - 1
    if isinstance(v, dict):
        for x in v.values():
            a, r = _depths(x, d + 1)
            asked, reached = max(asked, a), max(reached, r)
    elif isinstance(v, list):
        for x in v:
            a, r = _depths(x, d)
            asked, reached = max(asked, a), max(reached, r)
    return asked, reached


def classify(spec, rows: list) -> str:
    """EMPTY, HOLLOW or ROWS for one test's expected rows (as oracle.as_json_rows gives them).

    HOLLOW for a tree: a level the tree asks for holds no value in any object. For a projection:
    more than one column, and every column but the first absent in every row.
    """
    if not rows:
        return "EMPTY"
    if spec.graph is not None:
        asked = reached = 0
        for r in rows:
            a, b = _depths(r, 1)
            asked, reached = max(asked, a), max(reached, b)
        return "HOLLOW" if reached < asked else "ROWS"
    if not all(isinstance(r, dict) for r in rows):
        return "ROWS"
    cols = list(rows[0])
    absent = {k for k in cols if all(r.get(k) is None for r in rows)}
    return "HOLLOW" if len(cols) > 1 and absent >= set(cols[1:]) else "ROWS"


def check(results) -> list[str]:
    """results: (spec, test id, expected rows) per test. An empty or hollow one outside a
    declared family, a declared one that is neither, or an unknown family, is a problem."""
    bad = []
    by_spec: dict = {}
    for spec, tid, rows in results:
        by_spec.setdefault(id(spec), (spec, []))[1].append((tid, classify(spec, rows)))
    for spec, kinds in by_spec.values():
        family = getattr(spec, "emptiness", None)
        if family is not None and family not in FAMILIES:
            bad.append(f"{spec.short} declares the emptiness family {family!r}, which is not one "
                       f"of {sorted(FAMILIES)}")
            continue
        for tid, kind in kinds:
            if kind != "ROWS" and family is None:
                bad.append(f"{spec.short} ({tid}) expects {kind.lower()} rows outside a declared "
                           f"emptiness family (emptiness.FAMILIES)")
        if family is not None and all(kind == "ROWS" for _t, kind in kinds):
            bad.append(f"{spec.short} declares {family!r} but every test expects rows")
    return bad
