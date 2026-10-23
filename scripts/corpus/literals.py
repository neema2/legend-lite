"""
Typed constants and parameters: the services of 99-typed-services.pure
(docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 1).

The corpus filtered on `' '` -- a not-null guard -- and took one parameter, which lite never ran.
Real queries compare typed values and take parameters, so these services do, over
typedvalues::TypedValue (8112-typedvalues.pure): one property of every primitive type at [1] and
at [0..1], seeded so that each comparison keeps some rows and drops others (seed.TYPED_VALUE).

  TL_/TP_   the comparison matrix: every operator a type has, at the ROOT (the optional property,
            whose column the seed nulls) and one HOP away from a trade (the required property,
            absent for the seven trades with no typed value). TL_ writes the comparand as a
            literal; TP_ is its twin taking it as a parameter, and must return the same rows
            (an invariance group, checked before anything is written).
  TQ_       predicate trees: `!`, `||` and `in` over absent values, where the answer turns on
            how Pure compares an absent value (oracle._cmp: false, and true for `!=`).
  TE_       every value of the enumeration compared, at both sites, and an `in` over three.
  TO_       an optional parameter in the optional-filter form, `$p->isEmpty() || ...`, tested
            given and absent.
  TM_       a to-many parameter, `->in($p)`, tested with several values and with one.
  TV_       the controls: every property projected, at the root and one hop away, so a wrong
            rendering of a type shows in one place rather than in every filter over it.

What the pinned engine cannot run, or answers wrongly, is written anyway with its correct
expectation and quarantined against a finding (scripts/corpus/quarantine.py): a Decimal literal
past 16 fractional digits (F61), a Number parameter (F62), StrictTime in a relational query (F63),
an equality under a negated `||` or `&&` over an absent value (F64), and an enumeration parameter
under `!=` or `!in` (F65). Byte has no literal and no comparison, so it is projected only.
"""
from __future__ import annotations

from decimal import Decimal as D

import oracle
from query import (And, DateArg, DateTimeArg, EnumArg, Not, Or, Param, ParamEmpty, Pred, Proj,
                   Spec, StrictTimeArg)

ROOT = "typedvalues::TypedValue"
HOP_ROOT = "trading::Trade"
HOP = "typedValue"
# StrictTime and Byte, which have no relational column type, on a class of their own over the same
# rows (8112-typedvalues.pure says why).
TIME_ROOT = "typedvalues::TimeAndOctet"
TIME_HOP = "timeAndOctet"
GRADE = "typedvalues::Grade"
MAPPING = "stress::AllMapping"

# The pack's table, hidden from the generators that walk the model (stacks, graphs, deepstack,
# aggregates, tomany, taxonomy, spread): build.py and executed.base_specs hand them walked(), not
# every seeded table. Walked, the pack displaced roots under their caps (five DS*_CustodiedCrypto
# Holding services went), changed seventeen dense services reaching it from a trade, and carried
# StrictTime and Byte into their trees and stacks. Its services are these, written deliberately.
FIXTURE_TABLES = frozenset({"TYPED_VALUE"})


def walked(tables: dict[str, list[dict]]) -> set[str]:
    """The seeded tables the model-walking generators may root at or navigate into."""
    return {k for k, v in tables.items() if v and k not in FIXTURE_TABLES}


ORDERED = ("==", "!=", "<", "<=", ">", ">=")
EQUALITY = ("==", "!=")
OP_NAME = {"==": "Eq", "!=": "Ne", "<": "Lt", "<=": "Le", ">": "Gt", ">=": "Ge", "in": "In"}


def _grade(v: str) -> EnumArg:
    return EnumArg(GRADE, v)


# (name in service names, required property, optional property, Pure type, operators,
#  comparand for an operator). The comparands are seed.TYPED_VALUE's marked values.
TYPES = [
    ("Name", "name", "nameOpt", "String", ORDERED, lambda op: "alpha"),
    ("Active", "active", "activeOpt", "Boolean", EQUALITY, lambda op: True),
    ("Quantity", "quantity", "quantityOpt", "Integer", ORDERED, lambda op: 42),
    ("Rate", "rate", "rateOpt", "Float", ORDERED, lambda op: 2.25),
    ("Amount", "amount", "amountOpt", "Decimal", ORDERED, lambda op: D("1.0000000000000001")),
    # A Number compared across Pure's two number types: an Integer for the ordered operators,
    # a Float for equality (whether `2.0 == 2` holds is a question of Pure's equality between
    # types, which this matrix does not ask).
    ("Metric", "metric", "metricOpt", "Number", ORDERED, lambda op: 2.5 if op in EQUALITY else 2),
    ("ValueDate", "valueDate", "valueDateOpt", "Date", ORDERED, lambda op: DateArg("2024-06-03")),
    ("TradeDate", "tradeDate", "tradeDateOpt", "StrictDate", ORDERED,
     lambda op: DateArg("2024-02-29")),
    ("StampedAt", "stampedAt", "stampedAtOpt", "DateTime", ORDERED,
     lambda op: DateTimeArg("2024-06-03 12:00:00")),
    # StrictTime is not ordered in legend-pure: no greaterThan or lessThan takes one. On TimeAndOctet.
    ("Cutoff", "cutoff", "cutoffOpt", "StrictTime", EQUALITY, lambda op: StrictTimeArg("17:30:00")),
    ("Grade", "grade", "gradeOpt", GRADE, EQUALITY + ("in",),
     lambda op: [_grade("PRIME"), _grade("WATCH")] if op == "in" else _grade("STANDARD")),
]
BY_NAME = {t[0]: t for t in TYPES}
TIMED = {"Cutoff"}

# Every property of each class, in declaration order, for the controls.
ALL_PROPS = ["valueId"] + [p for t in TYPES if t[0] not in TIMED for p in (t[1], t[2])]
TIME_PROPS = ["valueId", "cutoff", "cutoffOpt", "octet", "octetOpt"]


def _site(site: str, tname: str | None = None):
    """(root class, the path to a property, the identifier projection) for a site. At the root
    a comparison reads the OPTIONAL property; one hop away, the REQUIRED one."""
    timed = tname in TIMED
    if site == "Root":
        return (TIME_ROOT if timed else ROOT), (lambda t: [t[2]]), Proj("valueId", ["valueId"])
    return HOP_ROOT, (lambda t: [TIME_HOP if timed else HOP, t[1]]), Proj("tradeId", ["tradeId"])


def _spec(name: str, doc: str, root: str, ident: Proj, values: list[tuple[str, list[str]]],
          filters: list, params=None, cases=None) -> Spec:
    s = Spec(f"stress::{name}", f"/stress/typed/{name.lower()}", doc, root)
    s.projections = [ident] + [Proj(alias, path) for alias, path in values]
    s.filters = filters
    s.mapping = MAPPING
    s.params = params or []
    s.cases = cases or []
    return s


def matrix() -> list[tuple[Spec, Spec]]:
    """(literal, parameter twin) for every operator of every type, at both sites."""
    out = []
    for t in TYPES:
        tname, _req, _opt, ptype, ops, comparand = t
        for site in ("Root", "Hop"):
            root, path, ident = _site(site, tname)
            for op in ops:
                value = comparand(op)
                stem = f"{tname}_{OP_NAME[op]}_{site}"
                where = "the optional property" if site == "Root" else "a trade's typed value"
                lit = _spec(f"TL_{stem}", f"{ptype} `{op}` against a literal, on {where}.",
                            root, ident, [("value", path(t))], [Pred(path(t), op, value)])
                many = op == "in"
                twin = _spec(f"TP_{stem}",
                             f"{ptype} `{op}` against a parameter; TL_{stem} with the literal "
                             f"must return the same rows.",
                             root, ident, [("value", path(t))], [Pred(path(t), op, Param("p"))],
                             params=[("p", ptype, "*" if many else "1")],
                             cases=[("expected_rows", {"p": value})])
                out.append((lit, twin))
    return out


def _paths(site: str):
    """A property's path at a site, by its type's name: the optional property at the root,
    the required one through a trade's typed value."""
    def p(tname: str) -> list[str]:
        assert tname not in TIMED, f"{tname} lives on {TIME_ROOT}; a tree reads {ROOT}"
        t = BY_NAME[tname]
        return [t[2]] if site == "Root" else [HOP, t[1]]
    return p


def _trees(p) -> list[tuple[str, str, object]]:
    """(name, documentation, predicate) per tree, over the paths `p` gives."""
    alpha, q42, r225 = "alpha", 42, 2.25
    noon = DateTimeArg("2024-06-03 12:00:00")
    june3, leap = DateArg("2024-06-03"), DateArg("2024-02-29")
    amount = D("1.0000000000000001")
    g = _grade
    return [
        ("NotEq", "`!` over an equality: keeps the absent names.",
         Not(Pred(p("Name"), "==", alpha))),
        ("NotGt", "`!` over an ordered comparison: an absent quantity is not greater, so kept.",
         Not(Pred(p("Quantity"), ">", q42))),
        ("NotLe", "`!` over `<=` on a DateTime.",
         Not(Pred(p("StampedAt"), "<=", noon))),
        ("NotInGrade", "`!` over `in` on an enumeration: an absent or unmapped grade is in "
                       "nothing, so kept.",
         Not(Pred(p("Grade"), "in", [g("PRIME"), g("WATCH")]))),
        ("NotInName", "`!` over `in` on a String.",
         Not(Pred(p("Name"), "in", ["alpha", "beta"]))),
        ("Or", "`||` of two comparisons on different columns.",
         Or([Pred(p("Name"), "==", "beta"), Pred(p("Rate"), ">", r225)])),
        ("OrNe", "`||` with a `!=`, which is true of an absent value.",
         Or([Pred(p("Quantity"), "!=", q42), Pred(p("Active"), "==", True)])),
        ("NotOr", "`!` over `||`: true only where neither side holds, absent sides included.",
         Not(Or([Pred(p("ValueDate"), "<", june3), Pred(p("Active"), "==", True)]))),
        ("NotAnd", "`!` over `&&` with a `!=` inside.",
         Not(And([Pred(p("StampedAt"), ">=", noon), Pred(p("Name"), "!=", alpha)]))),
        ("OrAnd", "`||` of an `&&` and an `in`.",
         Or([And([Pred(p("Quantity"), ">=", q42), Pred(p("Amount"), ">", amount)]),
             Pred(p("Grade"), "in", [g("DEFAULTED")])])),
        ("AndNotOr", "`&&` with `!` over `||`.",
         And([Pred(p("Rate"), ">", 0.0),
              Not(Or([Pred(p("Name"), "==", alpha), Pred(p("Grade"), "==", g("WATCH"))]))])),
        ("EmptyOr", "`isEmpty` or a comparison on the same StrictDate.",
         Or([Pred(p("TradeDate"), "isEmpty"), Pred(p("TradeDate"), "<", leap)])),
        ("NotNot", "`!!` cancels: an absent rate is not `<=`, so dropped.",
         Not(Not(Pred(p("Rate"), "<=", r225)))),
        ("PresentNotGt", "`isNotEmpty` and `!` over `>` on a Number.",
         And([Pred(p("Metric"), "isNotEmpty"), Not(Pred(p("Metric"), ">", 2))])),
        ("NotInDate", "`!` over `in` on a Date.",
         Not(Pred(p("ValueDate"), "in", [june3, DateArg("2024-06-04")]))),
        ("OrNegatives", "`||` of three comparisons with zero, on three number types.",
         Or([Pred(p("Amount"), "<", D("0")), Pred(p("Quantity"), "<", 0),
             Pred(p("Rate"), "<", 0.0)])),
        ("NotOrNe", "`!` over `||` of two `!=`: both sides present and equal.",
         Not(Or([Pred(p("Name"), "!=", alpha), Pred(p("Quantity"), "!=", q42)]))),
        ("InOrNotIn", "`in` or `!` over `in`.",
         Or([Pred(p("Quantity"), "in", [41, 43]),
             Not(Pred(p("Grade"), "in", [g("STANDARD"), g("PRIME")]))])),
        ("NotEqBool", "`!` over a Boolean equality: `!($x.active == false)` keeps the absent.",
         Not(Pred(p("Active"), "==", False))),
        ("AndOrNot", "`&&` of an `||` over two dates and a `!`.",
         And([Or([Pred(p("ValueDate"), ">=", june3), Pred(p("TradeDate"), "==", leap)]),
              Not(Pred(p("Name"), "==", "beta"))])),
    ]


def _referenced(f, out: list):
    """Every property path a predicate reads, in first-use order."""
    if isinstance(f, (And, Or)):
        for x in f.items:
            _referenced(x, out)
    elif isinstance(f, Not):
        _referenced(f.item, out)
    elif isinstance(f, Pred) and f.path not in out:
        out.append(f.path)
    return out


def trees() -> list[Spec]:
    out = []
    for site in ("Root", "Hop"):
        root, _path, ident = _site(site)
        for name, doc, pred in _trees(_paths(site)):
            values = [(path[-1].removesuffix("Opt"), path) for path in _referenced(pred, [])]
            where = "the optional properties" if site == "Root" else "a trade's typed value"
            out.append(_spec(f"TQ_{name}_{site}", f"{doc} On {where}.", root, ident, values,
                             [pred]))
    return out


# (type, operator, the value given, the values for a to-many parameter's two tests)
_OPTIONAL = [("Name", "=="), ("Active", "=="), ("Quantity", "=="), ("Rate", "=="),
             ("Amount", "=="), ("ValueDate", "=="), ("TradeDate", "=="), ("StampedAt", "<="),
             ("Grade", "==")]
_MANY = {
    "Name": (["alpha", "beta"], ["O'Neil"]),
    "Active": ([True, False], [False]),
    "Quantity": ([42, 43], [-5]),
    "Rate": ([2.25, 2.375], [-1.5]),
    "Amount": ([D("1.0000000000000001"), D("0.1")], [D("1.0000000000000000")]),
    "ValueDate": ([DateArg("2024-06-03"), DateArg("2024-06-04")], [DateArg("1970-01-01")]),
    "TradeDate": ([DateArg("2024-02-29"), DateArg("2024-03-01")], [DateArg("2020-02-29")]),
    "StampedAt": ([DateTimeArg("2024-06-03 12:00:00"), DateTimeArg("2024-06-03 00:00:00")],
                  [DateTimeArg("2024-06-03 12:00:01")]),
    "Grade": ([_grade("PRIME"), _grade("WATCH")], [_grade("DEFAULTED")]),
}


def parameters() -> list[Spec]:
    """Optional and to-many parameters, each service tested twice. Number and StrictTime are
    left to the matrix's twins (the pinned engine refuses both as parameter types: F62, F63)."""
    out = []
    root, _path, ident = _site("Root")
    for tname, op in _OPTIONAL:
        t = BY_NAME[tname]
        path = [t[2]]
        out.append(_spec(
            f"TO_{tname}_{OP_NAME[op]}",
            f"An optional {t[3]} parameter: `$p->isEmpty() || ($x.{t[2]} {op} $p->toOne())`. "
            f"Given, it filters; absent, every row is kept.",
            root, ident, [("value", path)],
            [Or([ParamEmpty("p"), Pred(path, op, Param("p", to_one=True))])],
            params=[("p", t[3], "0..1")],
            cases=[("given", {"p": t[5](op)}), ("absent", {})]))
    # one more at the hop, so the optional form also meets an absent navigation
    t = BY_NAME["Quantity"]
    hroot, hpath, hident = _site("Hop")
    out.append(_spec(
        "TO_Quantity_Ge_Hop",
        "An optional Integer parameter over a trade's typed value: `$p->isEmpty() || "
        "($x.typedValue.quantity >= $p->toOne())`.",
        hroot, hident, [("value", hpath(t))],
        [Or([ParamEmpty("p"), Pred(hpath(t), ">=", Param("p", to_one=True))])],
        params=[("p", "Integer", "0..1")],
        cases=[("given", {"p": 42}), ("absent", {})]))
    for tname, (several, one) in _MANY.items():
        t = BY_NAME[tname]
        path = [t[2]]
        out.append(_spec(
            f"TM_{tname}_In",
            f"A to-many {t[3]} parameter: `$x.{t[2]}->in($p)`, with several values and with one.",
            root, ident, [("value", path)], [Pred(path, "in", Param("p"))],
            params=[("p", t[3], "*")],
            cases=[("several", {"p": several}), ("one", {"p": one})]))
    g = BY_NAME["Grade"]
    out.append(_spec(
        "TM_Grade_NotIn_Hop",
        "A to-many enumeration parameter under `!`: `!$x.typedValue.grade->in($p)` keeps the "
        "trades with no typed value.",
        hroot, hident, [("value", hpath(g))], [Not(Pred(hpath(g), "in", Param("p")))],
        params=[("p", GRADE, "*")],
        cases=[("several", {"p": [_grade("PRIME"), _grade("STANDARD")]}),
               ("one", {"p": [_grade("DEFAULTED")]})]))
    return out


def enumerations() -> list[Spec]:
    """Every value of the enumeration, compared at both sites, and an `in` over three of them:
    each value's mapping is asked separately, and the unmapped code 'Z' equals none of them."""
    out = []
    for site in ("Root", "Hop"):
        root, path, ident = _site(site)
        g = BY_NAME["Grade"]
        for value in ("PRIME", "STANDARD", "WATCH", "DEFAULTED"):
            out.append(_spec(f"TE_{value.title()}_Eq_{site}",
                             f"An enumeration compared with its value {value}.", root, ident,
                             [("value", path(g))], [Pred(path(g), "==", _grade(value))]))
        three = [_grade("PRIME"), _grade("STANDARD"), _grade("WATCH")]
        out.append(_spec(f"TE_AllButDefaulted_In_{site}",
                         "An enumeration `in` three of its four values: the fourth, the absent and "
                         "the unmapped are dropped.", root, ident, [("value", path(g))],
                         [Pred(path(g), "in", three)]))
    return out


def scale18() -> list[Spec]:
    """F61 side by side: a Decimal comparand with 18 fractional digits, as a literal and as a
    parameter. legend-engine casts a Decimal LITERAL to DECIMAL(32,16), so 1.000000000000000001
    becomes 1.0 and the literal service matches TV-07's 1.0 rather than TV-08; the parameter is
    bound exactly and passes. The two must agree."""
    root, _path, ident = _site("Root")
    value = D("1.000000000000000001")
    path = ["amountOpt"]
    lit = _spec("TL_AmountScale18_Eq_Root",
                "A Decimal literal with 18 fractional digits. Its twin, the parameter, must "
                "return the same row (F61: the engine rounds the literal to scale 16).",
                root, ident, [("value", path)], [Pred(path, "==", value)])
    twin = _spec("TP_AmountScale18_Eq_Root",
                 "The same comparand as a Decimal parameter, which the engine binds exactly.",
                 root, ident, [("value", path)], [Pred(path, "==", Param("p"))],
                 params=[("p", "Decimal", "1")], cases=[("expected_rows", {"p": value})])
    return [lit, twin]


def controls() -> list[Spec]:
    root, _p, ident = _site("Root")
    hroot, _hp, hident = _site("Hop")
    return [
        _spec("TV_Root_All", "Every property of every type projected: the control the filters "
              "over each type are read against.", root, ident,
              [(p, [p]) for p in ALL_PROPS[1:]], []),
        _spec("TV_Hop_All", "Every property projected one hop from a trade, over the outer "
              "join: seven trades have no typed value and read absent throughout.",
              hroot, hident, [(p, [HOP, p]) for p in ALL_PROPS], []),
        _spec("TV_TimeAndOctet_Root", "The StrictTime and Byte properties projected, the control "
              "for the Cutoff services.", TIME_ROOT, ident, [(p, [p]) for p in TIME_PROPS[1:]], []),
        _spec("TV_TimeAndOctet_Hop", "The StrictTime and Byte properties projected one hop from a "
              "trade.", hroot, hident, [(p, [TIME_HOP, p]) for p in TIME_PROPS], []),
    ]


def build() -> list[Spec]:
    pairs = matrix()
    return ([s for pair in pairs for s in pair] + trees() + enumerations() + parameters()
            + scale18() + controls())


def invariance_groups(specs: list[Spec]) -> list[list[Spec]]:
    """Each literal and its parameter twin, by name."""
    by = {s.short: s for s in specs}
    return [[s, by["TP_" + s.short[3:]]] for s in specs
            if s.short.startswith("TL_") and "TP_" + s.short[3:] in by]


def cases(c, spec: Spec, tables) -> list[tuple[str, dict | None, list[dict]]]:
    """(test id, the parameters it binds, the rows the oracle expects) per test."""
    if not spec.cases:
        return [("expected_rows", None,
                 oracle.as_json_rows(c, spec, oracle.evaluate(c, spec, tables)))]
    return [(tid, params, oracle.as_json_rows(c, spec, oracle.evaluate(c, spec, tables, params)))
            for tid, params in spec.cases]


# ------------------------------------------------------------------------------- checks

def _three_valued(c, data, row, root, f):
    """SQL's reading of the same predicate: None for UNKNOWN. Only to show which rows the
    choice decides -- the expected results are Pure's (oracle._pred)."""
    if isinstance(f, And):
        vs = [_three_valued(c, data, row, root, x) for x in f.items]
        return False if False in vs else None if None in vs else True
    if isinstance(f, Or):
        vs = [_three_valued(c, data, row, root, x) for x in f.items]
        return True if True in vs else None if None in vs else False
    if isinstance(f, Not):
        v = _three_valued(c, data, row, root, f.item)
        return None if v is None else not v
    v = oracle._value(c, data, row, root, f.path)
    if f.op == "isEmpty":
        return v is None
    if f.op == "isNotEmpty":
        return v is not None
    if v is None:
        return None
    if f.op == "in":
        return any(oracle._cmp("==", v, m) for m in f.value)
    return oracle._cmp(f.op, v, f.value)


def null_decided(c, spec: Spec, tables) -> int:
    """How many of the root's rows Pure's comparison over an absent value keeps or drops
    differently from SQL's three-valued logic."""
    rows = oracle._rows_for(c, spec.root, tables)
    return sum(1 for r in rows
               if all(oracle._pred(c, tables, r, spec.root, f) for f in spec.filters)
               != all(_three_valued(c, tables, r, spec.root, f) is True for f in spec.filters))


def check(c, specs: list[Spec], tables) -> list[str]:
    """Every service discriminates: each test keeps some of the root's rows and drops others,
    except the absent case of an optional parameter, which keeps them all by design. The
    controls filter nothing. Enough trees turn on the absent-value rule to be worth having."""
    bad = []
    decided = 0
    for spec in specs:
        total = len(oracle._rows_for(c, spec.root, tables))
        for tid, _params, rows in cases(c, spec, tables):
            kept = len(rows)
            if spec.short.startswith("TV_") or tid == "absent":
                if kept != total:
                    bad.append(f"{spec.short}/{tid} keeps {kept} of {total} rows; it filters "
                               f"nothing, so it must keep them all")
            elif not 0 < kept < total:
                bad.append(f"{spec.short}/{tid} keeps {kept} of {total} rows: the comparand "
                           f"does not discriminate (seed.TYPED_VALUE)")
        if spec.short.startswith("TQ_") and null_decided(c, spec, tables):
            decided += 1
    if decided < 20:
        bad.append(f"only {decided} predicate trees keep or drop a row because of how an absent "
                   f"value compares; the phase asks for at least 20")
    return bad
