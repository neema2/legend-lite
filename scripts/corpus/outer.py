"""
Explicit join types: the services of 99-outer-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 9).

The plan asked for full and right outer joins in property-mapping join chains, each with seed rows on one side only
so a wrong join type changes the rows. legend-engine 4.145.0 has neither (probed 2026-10-31,
scripts/corpus/repro/mapping-join-types/):
  - a chain's join type is INNER or OUTER: `(RIGHT_OUTER)`, `(FULL_OUTER)` and `(LEFT_OUTER)` are parse errors
    ("Unsupported join type 'RIGHT_OUTER'. The supported join types are: [INNER, OUTER]"), and the compiler reads every
    type but INNER as LEFT_OUTER (HelperRelationalBuilder.buildElementWithJoinsJoinTreeNode);
  - a property mapping's first join takes no type ("Do not support specifying join type for the first join in the
    classMapping", HelperRelationalBuilder.validatePropertyMappings);
  - a typed later hop of a property's chain answers as an untyped one: the root reaches the chain through an outer
    join, so a to-one leaf is NULL where either hop lands on nothing, whichever type the second hop has.
Where a type does change the answer is a ~filter's chain: there the leading hop may be typed, an untyped hop is an
OUTER join, and a filter that a NULL passes (`X is null`) holds over the NULL columns an OUTER hop leaves where it
lands on nothing, while an INNER hop drops the row. The outer:: pack (8119-outer.pure) writes each typing:

  OJ{T,R}_Booked, _BookedIn     a property chain whose second hop is (OUTER), and (INNER): the same rows
  OJ{T,R}_NoAcc{Outer,Inner,Plain}       a ~filter through one hop, typed OUTER, INNER, untyped
  OJ{T,R}_NoLabel{Outer,Inner,Mixed,Plain}  a ~filter through two hops: OUTER > OUTER, INNER > INNER, INNER > OUTER,
                                            untyped > untyped

each as a TDS (OJT_, its tests on H2) and as a Relation (OJR_, on DuckDB); both databases answered every probe alike.
The oracle reads a ~filter chain hop by hop (oracle._filter_over_chain).
"""
from __future__ import annotations

from query import Proj, Spec

MAPPING, RUNTIME, DATA, CONNECTION = "outer::OuterMapping", "outer::OuterRT", "outer::OuterData", "outerEnv"
DB = "outer::OuterDB"

# class (short) -> what it answers
CLASSES = {
    "OjBooked": "A property chain, its second hop (OUTER): the account's label, NULL where either hop lands on nothing.",
    "OjBookedIn": "The same chain, its second hop (INNER): the same rows, the root reaching the chain through an outer "
                  "join either way.",
    "OjNoAccOuter": "A ~filter through one (OUTER) hop, `account is null`: an order whose allocation is missing, or "
                    "who has none, passes over the NULL columns the hop leaves.",
    "OjNoAccInner": "The same ~filter through an (INNER) hop: only the allocation that exists and has no account.",
    "OjNoAccPlain": "The same ~filter, its hop untyped: an OUTER join's answer.",
    "OjNoLabelOuter": "A ~filter through two (OUTER) hops, `label is null`: every order but the one booked to a "
                      "labelled account.",
    "OjNoLabelInner": "Two (INNER) hops: only the order booked to an account with no label.",
    "OjNoLabelMixed": "(INNER) then (OUTER): the orders with an allocation whose account is missing, absent or "
                      "unlabelled.",
    "OjNoLabelPlain": "Two untyped hops: the (OUTER) answer.",
}


def specs(c) -> list[Spec]:
    out = []
    for cls, doc in CLASSES.items():
        leaf = ("accountLabel", ["accountLabel"]) if cls.startswith("OjBooked") else ("allocId", ["allocId"])
        for prefix, paradigm, db in (("OJT", "tds", "H2"), ("OJR", "relation", "DuckDB")):
            name = cls[2:]
            s = Spec(f"stress::{prefix}_{name}", f"/stress/outer/{prefix.lower()}/{name.lower()}",
                     f"{doc} On {db}.", f"outer::{cls}")
            s.projections = [Proj("orderId", ["orderId"]), Proj(leaf[0], leaf[1])]
            s.paradigm = paradigm
            s.mapping, s.runtime, s.data_element, s.connection = MAPPING, RUNTIME, DATA, CONNECTION
            out.append(s)
    return out


def data_element(c, tables) -> str:
    import emit

    return emit.store_data_element(c, tables, DB, DATA)


def check(c, tables, specs_) -> list[str]:
    """Each typing of the ~filter answers rows the others do not, so a join type read wrongly changes an answer; the two
    property chains answer alike."""
    import oracle

    def rows(name):
        s = next(x for x in specs_ if x.short == "OJT_" + name)
        return sorted(r["orderId"] for r in oracle.evaluate(c, s, tables))

    problems = []
    if rows("NoAccOuter") != rows("NoAccPlain") or rows("NoAccOuter") == rows("NoAccInner"):
        problems.append("the one-hop ~filter's OUTER and untyped answers must agree, and differ from INNER's")
    two = [rows(n) for n in ("NoLabelOuter", "NoLabelInner", "NoLabelMixed")]
    if len({tuple(r) for r in two}) != 3 or rows("NoLabelPlain") != two[0]:
        problems.append("the two-hop ~filter's OUTER, INNER and mixed answers must differ, and untyped be OUTER's")
    booked = [sorted(map(str, oracle.evaluate(c, next(x for x in specs_ if x.short == "OJT_" + n), tables)))
              for n in ("Booked", "BookedIn")]
    if booked[0] != booked[1]:
        problems.append("a property chain's typed second hop changed its rows")
    return problems
