"""
Constraints that combine: the services of 99-checks-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 8).

The corpus had eight class constraints, each a comparison of one or two properties of one type, and no service
asserted a violation. The checks:: pack (8117-checks.pure) holds twenty more, on four classes of its own store, each
combining types or functions: dates against dates and a date literal, an enumeration value against a date's presence,
startsWith, length, toUpper and in over strings, exists, forAll and size over a to-many property, an `if`, a dateDiff,
toOne over an optional value, navigated properties (a basket's desk, a line's basket), and a Warn with an external id
and a message. Its seed (CK_* in seed.py, apart from the expansion) violates each one.

legend-engine 4.145.0 checks a class's constraints whenever it graph-fetches the class. These services fetch CHECKED
(`graphFetchChecked`), so each object comes back as {defects, value}, and the oracle evaluates every constraint over
the fetched graph as the engine does (oracle._enrich, oracle._defects): a fetch also reads every property a constraint
reads, and checks the constraints of each object it so reaches whose class is not already on its path, so a basket
carries its line's defects (path lines[0]) and its desk's (path desk), and a line its basket's and its basket's desk's.

  CK_<Class>Checked      every object of a class, checked
  CK_<Constraint>        one constraint's violating rows and a row that holds, its properties in the tree
  CK_...Defects          the nested defects: a line's under its basket, a desk's under its basket, a basket's and
                         its desk's under a line
  CKU_<Class>            an UNCHECKED fetch of objects with no defect: the engine raises on any defect, a Warn's too
                         (the generated withConstraintsApplied), so the oracle refuses an unchecked fetch of one
  CKP_<Class>            a projection of every row, violators among them: a projection evaluates no constraint (F16)

Each shape was probed on the pinned release before these were written (8117-checks.pure's header).
"""
from __future__ import annotations

from query import Pred, Proj, Spec

CHECKS = "checks::CheckMapping", "checks::CheckRT"
DB, DATA, CONNECTION = "checks::CheckDB", "checks::CheckData", "checkEnv"
DESK, DEAL, BASKET, LINE = "checks::CkDesk", "checks::CkDeal", "checks::CkBasket", "checks::CkLine"
CLASSES = (DESK, DEAL, BASKET, LINE)
_ID = {DESK: "deskId", DEAL: "dealId", BASKET: "basketId", LINE: "lineId"}

# constraint -> (its class, the rows that violate it, a row that holds it, what it combines)
CONSTRAINTS = {
    "deskCodeShape": (DESK, ["DSK3", "DSK4"], "DSK1", "startsWith and length over a code"),
    "regionKnown": (DESK, ["DSK5"], "DSK1", "in over a list of strings"),
    "activeDeskOpened": (DESK, ["DSK6"], "DSK2", "a Boolean's negation or a date against a date literal"),
    "settleNotBeforeTrade": (DEAL, ["D02"], "D01", "a date against a date"),
    "cancelledHasDate": (DEAL, ["D03", "D15"], "D01", "an enumeration value or an optional date's presence"),
    "cancelAfterTrade": (DEAL, ["D04"], "D14", "an optional date's absence, or it through toOne against a date"),
    "forwardMatures": (DEAL, ["D05", "D06"], "D14", "an `if` on an enumeration value, each branch a date's presence"),
    "maturityAfterSettle": (DEAL, ["D07"], "D14", "an optional date through toOne against a date"),
    "refShape": (DEAL, ["D08", "D09", "D15"], "D01", "startsWith and length over a reference"),
    "ccyUpper": (DEAL, ["D10", "D15"], "D01", "toUpper of a string against itself"),
    "feeWithinNotional": (DEAL, ["D11"], "D01", "an optional Float through toOne against arithmetic"),
    "settledWithinWeek": (DEAL, ["D12"], "D14", "an enumeration value or a dateDiff in days"),
    "notionalCap": (DEAL, ["D13"], "D01", "a Warn with an external id and a message naming the deal"),
    "hasLiveLine": (BASKET, ["B02", "B08"], "B01", "exists over a to-many property, empty for B08"),
    "linesInCcy": (BASKET, ["B03"], "B01", "forAll over a to-many property against the owner's own"),
    "swapHasTwoLines": (BASKET, ["B04"], "B07", "an `if` on an enumeration value over a to-many's size"),
    "linesBeforeClose": (BASKET, ["B05"], "B01", "forAll comparing each line's date with the basket's"),
    "euDeskInEur": (BASKET, ["B06"], "B07", "a navigated to-one property, absent for B05 and B08, or a string"),
    "sideSign": (LINE, ["B09-1"], "B07-2", "an `if` on an enumeration value over a Float's sign"),
    "lineOfBasket": (LINE, ["Z10-1"], "B01-1", "startsWith against a navigated to-one property"),
}


def _bind(s: Spec) -> Spec:
    s.mapping, s.runtime = CHECKS
    s.data_element, s.connection = DATA, CONNECTION
    return s


def _fetch(name: str, doc: str, root: str, tree: dict, ids=None, checked: bool = True) -> Spec:
    prefix = "CK" if checked else "CKU"
    s = Spec(f"stress::{prefix}_{name}", f"/stress/checks/{prefix.lower()}/{name.lower()}", doc, root)
    s.graph = tree
    s.checked = checked
    if ids is not None:
        s.filters = [Pred([_ID[root]], "in", list(ids))]
    return _bind(s)


def _leaves(*names) -> dict:
    return {n: None for n in names}


def _read(c, cls: str, constraint: str) -> list[str]:
    """The constraint's own properties a tree can hold as leaves: its class's columns that it reads."""
    import oracle

    k = next(x for x in c.classes[cls].constraints if x.name == constraint)
    return sorted(p for p in oracle._ck_required(c, k) if p in c.columns.get(cls, {}))


def specs(c) -> list[Spec]:
    out = [
        _fetch("DeskChecked", "Every desk, checked: a code's shape, a region in a list, an active desk's opening.",
               DESK, _leaves("deskId", "code", "region")),
        _fetch("DealChecked", "Every deal, checked: ten constraints over dates, enumeration values, strings and "
                              "amounts; D15 breaks three, D13 a Warn.", DEAL, _leaves("dealId", "state", "kind", "ccy")),
        _fetch("BasketChecked", "Every basket, checked: over its lines (exists, forAll, size) and its desk; B09's and "
                                "B10's lines and B11's desk carry defects of their own.", BASKET,
               _leaves("basketId", "kind", "ccy")),
        _fetch("LineChecked", "Every line, checked: its own constraints, and its basket's, which the fetch reaches "
                              "through lineOfBasket.", LINE, _leaves("lineId", "side", "qty")),
        _fetch("BasketDeskChecked", "Every basket with its desk in the tree, which the constraints' own reads of the "
                                    "desk join.", BASKET, {"basketId": None, "desk": _leaves("deskId", "region")}),
        _fetch("LineBasketChecked", "Every line with its basket in the tree.", LINE,
               {"lineId": None, "basket": _leaves("basketId", "ccy")}),
    ]
    for name, (cls, bad, good, what) in CONSTRAINTS.items():
        tree = _leaves(_ID[cls], *[p for p in _read(c, cls, name) if p != _ID[cls]])
        out.append(_fetch(name[0].upper() + name[1:],
                          f"{name}: {what}. {', '.join(bad)} violate{'s' if len(bad) == 1 else ''} it; {good} holds.",
                          cls, tree, bad + [good]))
    out += [
        _fetch("BasketLineDefects", "A line's defect under its basket, path lines[0]: B09's sideSign, B10's "
                                    "lineOfBasket; B01's lines hold.", BASKET, _leaves("basketId"),
               ["B09", "B10", "B01"]),
        _fetch("BasketDeskDefect", "A desk's defect under its basket, path desk: B11's desk is in no known region.",
               BASKET, {"basketId": None, "desk": _leaves("deskId")}, ["B11", "B07"]),
        _fetch("LineBasketDefects", "A basket's defect under its line, path basket, and its desk's, path basket, "
                                    "desk.", LINE, _leaves("lineId"), ["B02-1", "B11-1", "B01-1"]),
        _fetch("Desk", "An unchecked fetch of desks that hold every constraint.", DESK,
               _leaves("deskId", "active", "opened"), ["DSK1", "DSK2"], checked=False),
        _fetch("Deal", "An unchecked fetch of deals that hold every constraint.", DEAL,
               _leaves("dealId", "state", "kind", "settleDate"), ["D01", "D14"], checked=False),
        _fetch("Basket", "An unchecked fetch of baskets whose lines and desks hold every constraint too.", BASKET,
               {"basketId": None, "kind": None, "desk": _leaves("deskId")}, ["B01", "B07"], checked=False),
        _fetch("Line", "An unchecked fetch of lines whose baskets hold every constraint.", LINE,
               _leaves("lineId", "qty", "side"), ["B01-1", "B07-2"], checked=False),
    ]
    deals = Spec("stress::CKP_Deals", "/stress/checks/ckp/deals",
                 "Every deal projected, the violators among them: a projection evaluates no constraint (F16).", DEAL)
    deals.projections = [Proj(p, [p]) for p in ("dealId", "ref", "state", "settleDate", "tradeDate")]
    baskets = Spec("stress::CKP_Baskets", "/stress/checks/ckp/baskets",
                   "Every basket projected as a TDS with its desk's region, absent for B05 and B08.", BASKET)
    baskets.projections = [Proj("basketId", ["basketId"]), Proj("kind", ["kind"]),
                           Proj("deskRegion", ["desk", "region"])]
    baskets.paradigm = "tds"
    out += [_bind(deals), _bind(baskets)]
    return out


def data_element(c, tables) -> str:
    import emit

    return emit.store_data_element(c, tables, DB, DATA)


def check(c, tables, specs_) -> list[str]:
    """Every pack constraint violated by a row its own service asserts; every fetch answers two objects or more (one
    serializes as an object, not an array); every projection control returns rows that violate."""
    import oracle

    problems = []
    named = {s.short: s for s in specs_}
    for name, (cls, bad, good, _what) in CONSTRAINTS.items():
        s = named.get("CK_" + name[0].upper() + name[1:])
        rows = oracle.evaluate(c, s, tables) if s else []
        hit = {r["value"][_ID[cls]] for r in rows for d in r["defects"] if d["id"] == name and not d["path"]}
        if hit != set(bad):
            problems.append(f"{name}: violated by {sorted(hit)} in its service, expected {bad}")
        if any(d for r in rows if r["value"][_ID[cls]] == good for d in r["defects"]):
            problems.append(f"{name}: {good} was to hold every constraint")
    declared = {k.name for cls in CLASSES for k in c.classes[cls].constraints}
    problems += [f"{n}: a constraint of the pack with no service" for n in sorted(declared - set(CONSTRAINTS))]
    for s in specs_:
        if s.graph is not None and len(oracle.evaluate(c, s, tables)) < 2:
            problems.append(f"{s.short}: fewer than two objects (one serializes as an object, not an array)")
    return problems
