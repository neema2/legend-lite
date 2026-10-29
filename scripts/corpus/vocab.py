"""
The function vocabulary: the services of 99-vocab-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 4).

The corpus's queries used 25 platform functions. legend-engine's SQL registry has 262 names, and the probes ran most
of the ones the oracle implements on seeds of their own (docs/FUNCTIONS_EXECUTED.tsv), in a property mapping, where no
service of the corpus reached them, so nothing tested lite on them. These services apply each function in a QUERY,
over typedvalues::TypedValue (8112-typedvalues.pure), whose fifteen rows hold a tie on each comparand and both its
neighbours, negatives, zero, INTEGER's two extremes, strings that differ by case or end in a space or carry an
apostrophe, Decimals a double cannot tell apart, a leap day, midnight and the last second of a day, and an absent value
in every optional column:

  VP_   the function projected over a required property: every boundary value, none absent
  VN_   the same function through `map` over the optional property, `$x.opt->map(v|$v->f())`, which is how Pure
        applies a function to an optional value. legend-pure's map over an empty collection is empty, so an absent
        value stays absent whatever the function (oracle._apply); where the engine's SQL turns a NULL into a value,
        that is F68
  VF_   the function in a filter over the required property, against a comparand that keeps between a fifth and four
        fifths of the rows; a Boolean function is the filter itself

Expectations are legend-pure's definitions, through the oracle (oracle._apply over oracle.IMPL), never either engine's
output. A function whose result in floating point depends on the platform's last bit (a transcendental: F60's lesson)
is compared against a threshold the oracle's value clears by a margin, `->greaterThan(t)`, rather than projected raw.
What the pinned engine answers wrongly is written with legend-pure's answer and quarantined against a finding
(scripts/corpus/quarantine.py).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal as D

import oracle
from query import (Agg, Apply, Concatenate, Cut, DateArg, DateTimeArg, Distinct, EnumArg, Extend, Join, Pred,
                   Proj, Rename, Select, Sort, Spec, Window)

ROOT = "typedvalues::TypedValue"
MAPPING = "stress::AllMapping"

# (required property, optional property) of TypedValue, by the type they hold
STRING = ("name", "nameOpt")
INTEGER = ("quantity", "quantityOpt")
FLOAT = ("rate", "rateOpt")
DECIMAL = ("amount", "amountOpt")
DATE = ("valueDate", "valueDateOpt")             # Date
STRICT_DATE = ("tradeDate", "tradeDateOpt")      # StrictDate
DATE_TIME = ("stampedAt", "stampedAtOpt")        # DateTime
BOOLEAN = ("active", "activeOpt")


def _unit(u: str) -> EnumArg:
    return EnumArg("DurationUnit", u)


@dataclass(frozen=True)
class Case:
    """One function, applied as `steps` (query.Apply's) to `subject`; `ret` is the result's Pure type.
    `inexact`: a floating-point result whose last bit is the platform's, so it is compared, never projected."""
    key: str
    subject: tuple
    steps: tuple
    ret: str
    inexact: bool = False


def _c(key, subject, ret, *steps, inexact=False) -> Case:
    return Case(key, subject, tuple((fn, tuple(args)) for fn, args in steps), ret, inexact)


CASES = [
    # ---- strings: 'alpha ' ends in a space, "O'Neil" carries an apostrophe, case differs among the alphas
    _c("ToUpper", STRING, "String", ("toUpper", ())),
    _c("ToLower", STRING, "String", ("toLower", ())),
    _c("Trim", STRING, "String", ("trim", ())),
    _c("Ltrim", STRING, "String", ("ltrim", ())),
    _c("Rtrim", STRING, "String", ("rtrim", ())),
    _c("ReverseString", STRING, "String", ("reverseString", ())),
    _c("Length", STRING, "Integer", ("length", ())),
    _c("Substring", STRING, "String", ("substring", (0, 2))),
    _c("SubstringFrom", STRING, "String", ("substring", (1,))),
    _c("IndexOf", STRING, "Integer", ("indexOf", ("a",))),
    _c("Lpad", STRING, "String", ("lpad", (10, "*"))),
    _c("Rpad", STRING, "String", ("rpad", (10, "*"))),
    _c("Replace", STRING, "String", ("replace", ("a", "4"))),
    _c("StartsWith", STRING, "Boolean", ("startsWith", ("al",))),
    _c("EndsWith", STRING, "Boolean", ("endsWith", ("a",))),
    _c("Contains", STRING, "Boolean", ("contains", ("ph",))),
    _c("IsAlphaNumeric", STRING, "Boolean", ("isAlphaNumeric", ())),
    _c("Ascii", STRING, "Integer", ("ascii", ())),
    _c("EncodeBase64", STRING, "String", ("encodeBase64", ())),
    _c("DecodeBase64", STRING, "String", ("encodeBase64", ()), ("decodeBase64", ())),
    _c("LevenshteinDistance", STRING, "Integer", ("levenshteinDistance", ("alpha",))),
    _c("RepeatString", STRING, "String", ("repeatString", (2,))),
    _c("PlusString", STRING, "String", ("+", ("!",))),
    _c("Matches", STRING, "Boolean", ("matches", ("[a-z]+",))),
    _c("RegexpLike", STRING, "Boolean", ("regexpLike", ("^a",))),
    _c("RegexpCount", STRING, "Integer", ("regexpCount", ("a",))),
    _c("RegexpIndexOf", STRING, "Integer", ("regexpIndexOf", ("ph",))),
    # ---- integers: -2147483648 and 2147483647, INTEGER's extremes, are rows
    _c("AbsInteger", INTEGER, "Integer", ("abs", ())),
    _c("SignInteger", INTEGER, "Integer", ("sign", ())),
    _c("Mod", INTEGER, "Integer", ("mod", (7,))),
    _c("Rem", INTEGER, "Integer", ("rem", (7,))),
    _c("PlusInteger", INTEGER, "Integer", ("+", (1,))),
    _c("DivideInteger", INTEGER, "Float", ("/", (4,))),
    _c("BitAnd", INTEGER, "Integer", ("bitAnd", (255,))),
    _c("BitOr", INTEGER, "Integer", ("bitOr", (256,))),
    _c("BitXor", INTEGER, "Integer", ("bitXor", (255,))),
    _c("BitNot", INTEGER, "Integer", ("bitNot", ())),
    _c("BitShiftRight", INTEGER, "Integer", ("bitShiftRight", (1,))),
    _c("ToStringInteger", INTEGER, "String", ("toString", ())),
    _c("ParseInteger", INTEGER, "Integer", ("toString", ()), ("parseInteger", ())),
    _c("ToFloat", INTEGER, "Float", ("toFloat", ())),
    # ---- floats: every value exact in binary, negatives and zero among them
    _c("AbsFloat", FLOAT, "Float", ("abs", ())),
    _c("SignFloat", FLOAT, "Integer", ("sign", ())),
    _c("PlusFloat", FLOAT, "Float", ("+", (0.5,))),
    _c("MinusFloat", FLOAT, "Float", ("-", (0.25,))),
    _c("TimesFloat", FLOAT, "Float", ("*", (2.0,))),
    _c("DivideFloat", FLOAT, "Float", ("/", (4.0,))),
    _c("Ceiling", FLOAT, "Integer", ("ceiling", ())),
    _c("Floor", FLOAT, "Integer", ("floor", ())),
    _c("Round", FLOAT, "Integer", ("round", ())),
    _c("Sqrt", FLOAT, "Float", ("abs", ()), ("sqrt", ())),
    _c("Pow", FLOAT, "Float", ("pow", (2,))),
    # exp over atan's bounded range: 1e10 is a row, and exp(1e10) is past every double
    _c("Exp", FLOAT, "Float", ("atan", ()), ("exp", ()), inexact=True),
    _c("Log", FLOAT, "Float", ("abs", ()), ("+", (1.0,)), ("log", ()), inexact=True),
    _c("Log10", FLOAT, "Float", ("abs", ()), ("+", (1.0,)), ("log10", ()), inexact=True),
    _c("Sin", FLOAT, "Float", ("sin", ()), inexact=True),
    _c("Cos", FLOAT, "Float", ("cos", ()), inexact=True),
    _c("Tan", FLOAT, "Float", ("tan", ()), inexact=True),
    _c("Atan", FLOAT, "Float", ("atan", ()), inexact=True),
    # ---- decimals: 1.0000000000000001 and 1.0000000000000000 are one double
    _c("AbsDecimal", DECIMAL, "Decimal", ("abs", ())),
    _c("PlusDecimal", DECIMAL, "Decimal", ("+", (D("1.5"),))),
    _c("TimesDecimal", DECIMAL, "Decimal", ("*", (2,))),
    # ---- dates: a leap day, 1970-01-01, 1999-12-31 and 2025-01-01 among them
    _c("Year", DATE, "Integer", ("year", ())),
    _c("MonthNumber", DATE, "Integer", ("monthNumber", ())),
    _c("DayOfMonth", DATE, "Integer", ("dayOfMonth", ())),
    _c("DayOfYear", DATE, "Integer", ("dayOfYear", ())),
    _c("DayOfWeekNumber", DATE, "Integer", ("dayOfWeekNumber", ())),
    _c("WeekOfYear", DATE, "Integer", ("weekOfYear", ())),
    _c("QuarterNumber", DATE, "Integer", ("quarterNumber", ())),
    _c("FirstDayOfMonth", DATE, "Date", ("firstDayOfMonth", ())),
    _c("FirstDayOfQuarter", DATE, "Date", ("firstDayOfQuarter", ())),
    _c("FirstDayOfYear", DATE, "Date", ("firstDayOfYear", ())),
    _c("FirstDayOfWeek", DATE, "Date", ("firstDayOfWeek", ())),
    _c("DateDiffDays", STRICT_DATE, "Integer", ("dateDiff", (DateArg("2024-01-01"), _unit("DAYS")))),
    _c("AdjustDays", STRICT_DATE, "Date", ("adjust", (1, _unit("DAYS")))),
    # ---- date-times: midnight, the last second of a day, 1999-12-31 23:59:59
    _c("Hour", DATE_TIME, "Integer", ("hour", ())),
    _c("Minute", DATE_TIME, "Integer", ("minute", ())),
    _c("Second", DATE_TIME, "Integer", ("second", ())),
    _c("DatePart", DATE_TIME, "Date", ("datePart", ())),
    _c("FirstHourOfDay", DATE_TIME, "DateTime", ("firstHourOfDay", ())),
    _c("FirstMinuteOfHour", DATE_TIME, "DateTime", ("firstMinuteOfHour", ())),
    _c("FirstSecondOfMinute", DATE_TIME, "DateTime", ("firstSecondOfMinute", ())),
    _c("AdjustHours", DATE_TIME, "DateTime", ("adjust", (5, _unit("HOURS")))),
    _c("DateDiffSeconds", DATE_TIME, "Integer",
       ("dateDiff", (DateTimeArg("2024-06-03 12:00:00"), _unit("SECONDS")))),
    # ---- booleans
    _c("Not", BOOLEAN, "Boolean", ("not", ())),
    _c("ToStringBoolean", BOOLEAN, "String", ("toString", ())),
]

# The fraction of the fifteen rows a filter must keep (the improvement plan's Phase 2 rule for a comparand)
LOW, HIGH = 3, 12


def _spec(name: str, doc: str, projections: list[Proj], filters: list) -> Spec:
    s = Spec(f"stress::{name}", f"/stress/vocab/{name.lower()}", doc, ROOT)
    s.projections = [Proj("valueId", ["valueId"])] + projections
    s.filters = filters
    s.mapping = MAPPING
    return s


def _values(c, tables, case: Case, prop: str, through_map: bool) -> list:
    """The function's value on each row, as the oracle computes it."""
    probe = _spec("probe", "", [Proj("r", [prop], apply=Apply(case.steps, case.ret, through_map))], [])
    return [r["r"] for r in oracle.evaluate(c, probe, tables)]


def _literal(value, ret: str):
    """A value of the function's result as a query literal of its type."""
    if ret in ("Date", "StrictDate"):
        return DateArg(value)
    if ret == "DateTime":
        return DateTimeArg(value)
    return value


def _threshold(values: list) -> tuple[str, object] | None:
    """For an inexact result: (`>`, t) with t a short decimal strictly between two adjacent values, near the middle,
    so no platform's last bit can move a row across it."""
    xs = sorted(set(v for v in values if v is not None))
    order = sorted(range(1, len(xs)), key=lambda i: abs(i - len(xs) / 2))
    for i in order:
        lo, hi = xs[i - 1], xs[i]
        if hi - lo < 1e-6:
            continue
        for digits in range(0, 7):
            t = round((lo + hi) / 2, digits)
            if lo + 1e-9 < t < hi - 1e-9:
                kept = sum(1 for v in values if v is not None and v > t)
                if LOW <= kept <= HIGH:
                    return ">", float(t)
                break
    return None


def _comparand(values: list, ret: str) -> tuple[str, object] | None:
    """(operator, comparand) keeping between LOW and HIGH of the rows: a value of the result itself, the middle ones
    first, under `>=`, `<`, `>` and `==`."""
    # A Decimal comparand past 16 fractional digits is rounded in the SQL (F61, Phase 1's, which its own services
    # pin), so none is chosen here.
    xs = sorted(set(v for v in values if v is not None
                    and not (isinstance(v, D) and v.as_tuple().exponent < -16)),
                key=lambda v: (str(type(v)), v))
    order = sorted(range(len(xs)), key=lambda i: abs(i - len(xs) / 2))
    for i in order:
        for op in (">=", "<", ">", "=="):
            kept = sum(1 for v in values if v is not None and oracle._cmp(op, v, xs[i]))
            if LOW <= kept <= HIGH:
                return op, _literal(xs[i], ret)
    return None


def build(c, tables) -> list[Spec]:
    out = []
    for case in CASES:
        req, opt = case.subject
        fns = " then ".join(f"`{fn}`" for fn, _a in case.steps)
        direct = Apply(case.steps, case.ret)
        mapped = Apply(case.steps, case.ret, through_map=True)
        values = _values(c, tables, case, req, False)
        if case.inexact:
            cut = _threshold(values)
            if cut is None:
                raise SystemExit(f"vocab: no threshold separates {case.key}'s values: {values}")
            op, t = cut
            # projected as the comparison, so the platform's last bit cannot reach the expectation
            direct = Apply(case.steps + (("greaterThan", (t,)),), "Boolean")
            mapped = Apply(case.steps + (("greaterThan", (t,)),), "Boolean", through_map=True)
        out.append(_spec(f"VP_{case.key}", f"{fns} over the required `{req}`, projected.",
                         [Proj("r", [req], apply=direct)], []))
        out.append(_spec(f"VN_{case.key}", f"{fns} through map over the optional `{opt}`: an absent value "
                                           f"stays absent.",
                         [Proj("r", [opt], apply=mapped)], []))
        if case.inexact:
            flt = Pred([req], op, t, apply=Apply(case.steps, case.ret))
        elif case.ret == "Boolean":
            kept = sum(1 for v in values if v)
            flt = Pred([req], "is" if kept >= LOW or kept >= len(values) - kept else "not", apply=direct)
        else:
            cut = _comparand(values, case.ret)
            if cut is None:
                raise SystemExit(f"vocab: no comparand keeps a fifth to four fifths of {case.key}: {values}")
            flt = Pred([req], cut[0], cut[1], apply=direct)
        out.append(_spec(f"VF_{case.key}", f"{fns} over the required `{req}`, in a filter.",
                         [Proj("r", [req])], [flt]))
    return out + build_aggregates(c, tables) + build_relations(c, tables)


# ---- stage 3: aggregates in a groupBy
#
# `project(~[k: <key>, v: <value>])->groupBy(~[k], ~[r: x|$x.v : agg|$agg->f(args)])->sort(~k->ascending())`, each
# aggregate as legend-pure defines it (or legend-engine's Pure, core_functions_standard/math/aggregator), through
# the oracle's _REDUCERS:
#
#   VG_   over a required property, keyed on `active` (nine rows and six): every boundary value of the column
#   VGN_  over the optional property. An aggregate Pure defines over an empty collection is keyed on `segment`,
#         whose group A holds TV-02 alone, the row absent in every optional column, so A's values are all absent:
#         sum is 0 there, count 0, joinStrings '', max, min, first, last and percentile absent. An aggregate Pure
#         fails on over nothing (average, median, the variances and standard deviations) is keyed on `active`,
#         whose two groups each hold absent and present values: the absent ones are ignored
#   VGS_  a sample variance or standard deviation over a required property keyed on `segment`: over A's one value
#         Pure's variance is 0 (`if ($numbers->size() == 1, | 0, ...)`)
#
# An aggregate over pairs (maxBy, minBy, wavg, the covariances, corr) maps them with rowMapper or wavgRowMapper, over
# two required properties: an absent side misaligns rowMapper's collections, which legend-engine's own PCT for
# covarSample says the platform does not handle. An inexact result (a mean of non-dyadic values, a variance, a
# correlation: F60's lesson) is compared in the reducer against a threshold its values clear by a margin,
# `->greaterThan(t)`, never projected raw. mode is over `metric`, whose groups each have one most frequent value:
# Pure's mode is the smallest of the most frequent, and a database's choice among a tie is its own.

KEY_ACTIVE = ("k", ["active"])
KEY_SEGMENT = ("k", ["segment"])
METRIC = ("metric", "metricOpt")
PAIR_RATE_METRIC = ("rate", "metric")
PAIR_NAME_QUANTITY = ("name", "quantity")

# The aggregates Pure defines over an empty collection (oracle._REDUCERS): their VGN_ is keyed on `segment`.
TOTAL = {"count", "sum", "max", "min", "first", "last", "percentile", "joinStrings"}


@dataclass(frozen=True)
class GCase:
    """One aggregate over `subject` (required, optional) or over a `pair` of required properties. `ret` is its
    Pure type; `optional`: whether a VGN_ is written; `singleton`: whether a VGS_ is."""
    key: str
    fn: str
    subject: tuple | None
    ret: str
    args: tuple = ()
    pair: tuple | None = None
    mapper: str | None = None
    inexact: bool = False
    optional: bool = True
    singleton: bool = False


AGG_CASES = [
    GCase("CountName", "count", STRING, "Integer"),
    GCase("SumQuantity", "sum", INTEGER, "Integer"),
    GCase("SumRate", "sum", FLOAT, "Float"),
    GCase("SumAmount", "sum", DECIMAL, "Decimal"),
    GCase("AverageQuantity", "average", INTEGER, "Float"),
    GCase("AverageRate", "average", FLOAT, "Float"),
    GCase("AverageAmount", "average", DECIMAL, "Float", inexact=True),
    GCase("MaxQuantity", "max", INTEGER, "Integer"),
    GCase("MaxRate", "max", FLOAT, "Float"),
    GCase("MaxAmount", "max", DECIMAL, "Decimal"),
    GCase("MaxValueDate", "max", DATE, "Date"),
    GCase("MaxTradeDate", "max", STRICT_DATE, "StrictDate"),
    GCase("MaxStampedAt", "max", DATE_TIME, "DateTime"),
    GCase("MinQuantity", "min", INTEGER, "Integer"),
    GCase("MinRate", "min", FLOAT, "Float"),
    GCase("MinAmount", "min", DECIMAL, "Decimal"),
    GCase("MinValueDate", "min", DATE, "Date"),
    GCase("MinTradeDate", "min", STRICT_DATE, "StrictDate"),
    GCase("MinStampedAt", "min", DATE_TIME, "DateTime"),
    GCase("MedianQuantity", "median", INTEGER, "Float"),
    GCase("MedianRate", "median", FLOAT, "Float"),
    GCase("ModeMetric", "mode", METRIC, "Number", optional=False),
    GCase("StdDevPopulationQuantity", "stdDevPopulation", INTEGER, "Float", inexact=True),
    GCase("StdDevSampleQuantity", "stdDevSample", INTEGER, "Float", inexact=True, singleton=True),
    GCase("VariancePopulationQuantity", "variancePopulation", INTEGER, "Float", inexact=True),
    GCase("VarianceSampleQuantity", "varianceSample", INTEGER, "Float", inexact=True, singleton=True),
    GCase("StdDevPopulationRate", "stdDevPopulation", FLOAT, "Float", inexact=True),
    GCase("StdDevSampleRate", "stdDevSample", FLOAT, "Float", inexact=True),
    GCase("VariancePopulationRate", "variancePopulation", FLOAT, "Float", inexact=True),
    GCase("VarianceSampleRate", "varianceSample", FLOAT, "Float", inexact=True),
    GCase("PercentileQuantity", "percentile", INTEGER, "Number", args=(0.5,)),
    GCase("PercentileRate", "percentile", FLOAT, "Number", args=(0.5,)),
    GCase("JoinStringsName", "joinStrings", STRING, "String", args=(",",)),
    GCase("FirstName", "first", STRING, "String"),
    GCase("LastName", "last", STRING, "String"),
    GCase("MaxByName", "maxBy", None, "String", pair=PAIR_NAME_QUANTITY, mapper="rowMapper"),
    GCase("MinByName", "minBy", None, "String", pair=PAIR_NAME_QUANTITY, mapper="rowMapper"),
    GCase("WavgRate", "wavg", None, "Float", pair=PAIR_RATE_METRIC, mapper="wavgRowMapper", inexact=True),
    GCase("CovarPopulationRate", "covarPopulation", None, "Number", pair=PAIR_RATE_METRIC, mapper="rowMapper",
          inexact=True),
    GCase("CovarSampleRate", "covarSample", None, "Number", pair=PAIR_RATE_METRIC, mapper="rowMapper",
          inexact=True),
    GCase("CorrRate", "corr", None, "Number", pair=PAIR_RATE_METRIC, mapper="rowMapper", inexact=True),
]


def _gspec(name: str, doc: str, key: tuple, value: str, case: GCase, then=(), ret=None, pair_value=None) -> Spec:
    s = Spec(f"stress::{name}", f"/stress/vocab/{name.lower()}", doc, ROOT)
    s.projections = [Proj(key[0], key[1]), Proj("v", [value])]
    pair = None
    if pair_value is not None:
        s.projections.append(Proj("w", [pair_value]))
        pair = "w"
    s.group_by = [key[0]]
    s.aggs = [Agg("r", "v", case.fn, case.args, pair=pair, mapper=case.mapper, then=then, ret=ret or case.ret)]
    s.sort = (key[0], False)
    s.mapping = MAPPING
    return s


def _group_threshold(values: list) -> float | None:
    """For inexact results, one per group: a short decimal strictly between two adjacent values, near the middle,
    far enough from each that no platform's last bit moves a group across it."""
    xs = sorted(set(float(v) for v in values if v is not None))
    order = sorted(range(1, len(xs)), key=lambda i: abs(i - len(xs) / 2))
    for i in order:
        lo, hi = xs[i - 1], xs[i]
        if hi - lo < 1e-6 * max(abs(lo), abs(hi), 1.0):
            continue
        margin = 1e-7 * max(abs(lo), abs(hi), 1.0)
        for digits in range(-12, 7):
            t = round((lo + hi) / 2, digits)
            if lo + margin < t < hi - margin:
                return float(t)
    return None


def _grouped(c, tables, name: str, doc: str, key: tuple, value: str, case: GCase, pair_value=None) -> Spec:
    """The service; an inexact aggregate compares against a threshold between its groups' values."""
    s = _gspec(name, doc, key, value, case, pair_value=pair_value)
    if not case.inexact:
        return s
    values = [r["r"] for r in oracle.evaluate(c, s, tables)]
    t = _group_threshold(values)
    if t is None:
        raise SystemExit(f"vocab: no threshold separates {name}'s groups: {values}")
    return _gspec(name, f"{doc} Compared with {t!r}, as its value is inexact.", key, value, case,
                  then=(("greaterThan", (t,)),), ret="Boolean", pair_value=pair_value)


def build_aggregates(c, tables) -> list[Spec]:
    import emit

    out = []
    for case in AGG_CASES:
        call = f"`{case.fn}({', '.join(emit._literal(a) for a in case.args)})`"
        if case.pair is not None:
            a, b = case.pair
            out.append(_grouped(c, tables, f"VG_{case.key}",
                                f"{call} over {case.mapper}(`{a}`, `{b}`), grouped by `active`.",
                                KEY_ACTIVE, a, case, pair_value=b))
            continue
        req, opt = case.subject
        out.append(_grouped(c, tables, f"VG_{case.key}", f"{call} over the required `{req}`, grouped by `active`.",
                            KEY_ACTIVE, req, case))
        if case.optional:
            total = case.fn in TOTAL
            key = KEY_SEGMENT if total else KEY_ACTIVE
            why = ("whose group A holds only absent values" if total
                   else "whose groups each hold absent and present values")
            out.append(_grouped(c, tables, f"VGN_{case.key}",
                                f"{call} over the optional `{opt}`, grouped by `{key[1][0]}`, {why}.",
                                key, opt, case))
        if case.singleton:
            out.append(_grouped(c, tables, f"VGS_{case.key}",
                                f"{call} over the required `{req}`, grouped by `segment`, whose group A holds one "
                                f"value: Pure's variance of one value is 0.", KEY_SEGMENT, req, case))
    return out


# ---- stage 4: relation and TDS shapes
#
# Over trading::Trade, whose twenty trades hold A5's ties: four trades of 2024-06-03 share a notional of 250,000, so
# rank, denseRank and rowNumber differ there (seed.py). Each shape runs in the Relation paradigm (VR_) and the legacy
# TDS one (VT_), as query.Spec's `relation` operations evaluated by the oracle's RELATION_IMPL and _relation_ops:
# extend, rename, select (restrict), distinct, the ordinal operations after a total order (take, drop, slice, limit,
# paginated), concatenate, inner and left joins, and the window functions (over() for a Relation, olapGroupBy for a
# TDS), partitioned by trade date and ordered by notional. rowNumber and ntile take the trade id after the notional,
# since a tie leaves their numbering to the database.

TRADE = "trading::Trade"
TYPED = "typedvalues::TypedValue"
ORDER_TOTAL = (("n", False), ("id", False))
BY_ID = Sort((("id", False),))


def _trades(name: str, doc: str, cols, ops, filters=(), tds=False) -> Spec:
    s = Spec(f"stress::{name}", f"/stress/vocab/{name.lower()}", doc, TRADE)
    s.projections = [Proj(a, [p]) for a, p in cols]
    s.filters = list(filters)
    s.relation = list(ops)
    s.mapping = MAPPING
    s.paradigm = "tds" if tds else "relation"
    return s


ID_N = (("id", "tradeId"), ("n", "notional"))
ID_D_N = (("id", "tradeId"), ("d", "tradeDate"), ("n", "notional"))


def _typed_side(tds: bool) -> Spec:
    """A join's other side: the typed values that name a trade, each with its trade's id."""
    s = Spec("stress::side", "", "", TYPED)
    s.projections = [Proj("vid", ["valueId"]), Proj("vq", ["quantity"]), Proj("tid", ["valuedTrade", "tradeId"])]
    s.filters = [Pred(["valuedTrade", "tradeId"], "isNotEmpty")]
    s.mapping = MAPPING
    s.paradigm = "tds" if tds else "relation"
    return s


def _relation_cases(tds: bool) -> list[tuple[str, str, tuple, list, list]]:
    """(name, doc, columns, operations, filters) of each shape, in one paradigm."""
    concat_other = _trades("other", "", ID_N, [], [Pred(["notional"], "<", 100000.0)], tds)
    windows = [("Rank", "rank", (("n", False),), ()), ("DenseRank", "denseRank", (("n", False),), ())]
    if tds:
        # olapGroupBy sorts by one key (SortInformation[0..1]), and its ranking functions are rank, averageRank,
        # denseRank and rowNumber (meta::pure::functions::math::olap, each `fail('not implemented')` in Pure: a
        # marker for the SQL)
        windows.append(("RowNumber", "rowNumber", (("id", False),), ()))
    else:
        windows += [("PercentRank", "percentRank", (("n", False),), ()),
                    ("CumulativeDistribution", "cumulativeDistribution", (("n", False),), ()),
                    ("RowNumber", "rowNumber", ORDER_TOTAL, ()), ("Ntile", "ntile", ORDER_TOTAL, (3,))]
    ret = {"percentRank": "Float", "cumulativeDistribution": "Float"}
    cases = [
        ("Extend", "a column computed from another, the notional halved",
         ID_N, [Extend("half", "n", Apply((("/", (2.0,)),), "Float"), of="Float"), BY_ID], []),
        ("ExtendString", "a column computed from another, the id's digits",
         ID_N, [Extend("digits", "id", Apply((("substring", (4,)),), "String"), of="String"), BY_ID], []),
        ("Rename", "a column renamed", ID_N, [Rename("n", "amount"), BY_ID], []),
        ("Select", "two of three columns kept", ID_D_N, [Select(("id", "n")), BY_ID], []),
        ("Distinct", "the distinct trade dates", (("d", "tradeDate"),),
         [Distinct(), Sort((("d", False),))], []),
        ("Take", "the five largest notionals", ID_N, [Sort((("n", True), ("id", False))), Cut("take", (5,))], []),
        ("Limit", "the three smallest notionals", ID_N, [Sort(ORDER_TOTAL), Cut("limit", (3,))], []),
        ("Drop", "all but the fifteen smallest notionals", ID_N, [Sort(ORDER_TOTAL), Cut("drop", (15,))], []),
        ("Slice", "the third to seventh smallest notionals, across the tie at 250,000", ID_N,
         [Sort(ORDER_TOTAL), Cut("slice", (2, 7))], []),
        ("Concatenate", "the notionals over 400,000 followed by those under 100,000", ID_N,
         [Concatenate(concat_other), BY_ID], [Pred(["notional"], ">", 400000.0)]),
        ("JoinInner", "each trade with the typed value naming it, inner", ID_N,
         [Join(_typed_side(tds), "INNER", (("id", "tid", "String"),)), BY_ID], []),
        ("JoinLeft", "each trade with the typed value naming it, or none", ID_N,
         [Join(_typed_side(tds), "LEFT_OUTER" if tds else "LEFT", (("id", "tid", "String"),)), BY_ID], []),
    ]
    if tds:
        cases.append(("Paginated", "the second page of four, by notional", ID_N,
                      [Sort(ORDER_TOTAL), Cut("paginated", (2, 4))], []))
    for key, fn, order, args in windows:
        cases.append((key, f"`{fn}` over each trade date's notionals", ID_D_N,
                      [Window("w", fn, ("d",), order, args, ret=ret.get(fn, "Integer")), BY_ID], []))
    for key, fn, r in (("WindowSum", "sum", "Float"), ("WindowMax", "max", "Float"), ("WindowCount", "count", "Integer")):
        cases.append((key, f"`{fn}` of each trade date's notionals, on every trade", ID_D_N,
                      [Window("w", fn, ("d",), col="n", ret=r), BY_ID], []))
    return cases


def build_relations(c, tables) -> list[Spec]:
    out = []
    for tds, prefix, how in ((False, "VR_", "a Relation"), (True, "VT_", "a TDS")):
        for key, doc, cols, ops, filters in _relation_cases(tds):
            out.append(_trades(f"{prefix}{key}", f"{doc.capitalize()}, over {how}.", cols, ops, filters, tds))
    return out


def check(c, specs: list[Spec], tables) -> list[str]:
    """Every filter keeps between a fifth and four fifths of the rows, every projection through map meets an absent
    value (the point of VN_), every groupBy has two groups or more, and each grouped aggregate over an optional value
    meets the absent values its key is chosen for: segment A's all absent, or a group with both."""
    problems = []
    for s in specs:
        rows = oracle.evaluate(c, s, tables)
        if s.short.startswith("VF_") and not 1 <= len(rows) <= 14:
            problems.append(f"{s.short}: its filter keeps {len(rows)} of 15 rows")
        if s.short.startswith("VN_") and not any(r["r"] is None for r in rows):
            problems.append(f"{s.short}: no absent value reaches the map")
        if s.short.startswith("VG") and len(rows) < 2:
            problems.append(f"{s.short}: {len(rows)} group(s)")
        if s.short.startswith("VGN_"):
            flat = oracle.evaluate(c, _ungrouped(s), tables)
            groups: dict = {}
            for r in flat:
                groups.setdefault(r["k"], []).append(r["v"])
            if s.projections[0].path == ["segment"]:
                met = any(all(v is None for v in vs) for vs in groups.values())
            else:
                met = any(None in vs and any(v is not None for v in vs) for vs in groups.values())
            if not met:
                problems.append(f"{s.short}: no group meets the absent values its key is chosen for")
    return problems


def _ungrouped(s: Spec) -> Spec:
    """The projection a groupBy service groups."""
    u = Spec(s.name, s.pattern, s.doc, s.root)
    u.projections, u.mapping = s.projections, s.mapping
    return u
