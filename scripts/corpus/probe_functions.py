"""
Which registered functions actually EXECUTE, one minimal service per function.

`getSupportedFunctions()` is the map the engine consults before reporting "No SQL translation
exists", so it looks like the definitive list of what a query may contain. It over-reports.
`previousDayOfWeek` is in it, and using one fails at execution with

    [unsupported-api] The function 'previousDayOfWeek' (state: [Select, false])
    is not supported yet

-- registered, routed, and then refused by the thing it routes to. A registry entry means a
handler was bound, not that the handler works in the position you used it.

So this probes each function the oracle implements, in a real mapping, against a real
connection, and reports three outcomes:

    RUNS        the engine produced a value
    UNSUPPORTED the engine refused it, with its message
    MISMATCH    both produced a value and they disagree

MISMATCH is the interesting one and is NOT reported as a defect here. The oracle is an
independent implementation, so a disagreement means one of the two is wrong and the probe
cannot say which -- that adjudication is a person's job, and the corpus has been wrong at
least as often as the engine (concat over NULL, `!=` over NULL, the all-NULL empty string).

Run it before widening the combination matrix. A function that cannot execute has no business
in the matrix, and finding that out from a 980-cell service failing is far more expensive
than finding it out from one probe.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run as runner  # noqa: E402

def _calendar_cases():
    """One case per calendar aggregation, all with the same four-argument shape.

    They are added programmatically because they are genuinely uniform -- the same call with
    a different name -- and writing thirty near-identical tuples by hand invites exactly the
    copy-paste slip that F35 is made of.
    """
    import oracle
    return [(n, "Integer", f"{n}(T.D, 'default', T.D2, T.I)",
             ["2024-06-03", "default", "2024-06-13", 7])
            for n in oracle.CALENDAR]


# (function, Pure return type, expression over the probe table, oracle args)
# The probe table carries one row with values chosen so no function meets an argument it
# would refuse: positive numbers, a non-empty string, a real date.
CASES = [
    ("toUpper", "String", "toUpper(T.S)", ["alpha"]),
    ("toLower", "String", "toLower(T.S)", ["alpha"]),
    ("trim", "String", "trim(T.S)", ["alpha"]),
    ("ltrim", "String", "ltrim(T.S)", ["alpha"]),
    ("rtrim", "String", "rtrim(T.S)", ["alpha"]),
    ("reverseString", "String", "reverseString(T.S)", ["alpha"]),
    ("length", "Integer", "length(T.S)", ["alpha"]),
    ("substring", "String", "substring(T.S, 2, 4)", ["alpha", 2, 4]),
    ("indexOf", "Integer", "indexOf(T.S, 'ph')", ["alpha", "ph"]),
    ("lpad", "String", "lpad(T.S, 8, 'x')", ["alpha", 8, "x"]),
    ("rpad", "String", "rpad(T.S, 8, 'x')", ["alpha", 8, "x"]),
    ("replace", "String", "replace(T.S, 'a', 'z')", ["alpha", "a", "z"]),
    ("splitPart", "String", "splitPart(T.S, 'l', 1)", ["alpha", "l", 1]),
    ("left", "String", "left(T.S, 3)", ["alpha", 3]),
    ("right", "String", "right(T.S, 3)", ["alpha", 3]),
    ("concat", "String", "concat(T.S, T.S2)", ["alpha", "beta"]),
    ("coalesce", "String", "coalesce(T.S, T.S2)", ["alpha", "beta"]),
    ("startsWith", "Boolean", "startsWith(T.S, 'al')", ["alpha", "al"]),
    ("endsWith", "Boolean", "endsWith(T.S, 'ha')", ["alpha", "ha"]),
    ("contains", "Boolean", "contains(T.S, 'ph')", ["alpha", "ph"]),
    ("isAlphaNumeric", "Boolean", "isAlphaNumeric(T.S)", ["alpha"]),
    ("isNull", "Boolean", "isNull(T.S)", ["alpha"]),
    ("isNotNull", "Boolean", "isNotNull(T.S)", ["alpha"]),
    ("not", "Boolean", "not(T.B)", [True]),
    ("ascii", "Integer", "ascii(T.S)", ["alpha"]),
    ("encodeBase64", "String", "encodeBase64(T.S)", ["alpha"]),
    ("decodeBase64", "String", "decodeBase64(T.S64)", ["YWxwaGE="]),
    ("levenshteinDistance", "Integer", "levenshteinDistance(T.S, T.S2)", ["alpha", "beta"]),
    ("abs", "Integer", "abs(T.I)", [7]),
    ("sign", "Integer", "sign(T.I)", [7]),
    ("plus", "Integer", "plus(T.I, T.J)", [7, 3]),
    ("minus", "Integer", "minus(T.I, T.J)", [7, 3]),
    ("times", "Integer", "times(T.I, T.J)", [7, 3]),
    ("mod", "Integer", "mod(T.I, T.J)", [7, 3]),
    ("rem", "Float", "rem(T.I, T.J)", [7, 3]),
    ("divide", "Float", "divide(T.F, T.G)", [10.0, 4.0]),
    ("sqrt", "Float", "sqrt(T.F)", [10.0]),
    ("cbrt", "Float", "cbrt(T.F)", [10.0]),
    ("exp", "Float", "exp(T.G)", [4.0]),
    ("log", "Float", "log(T.F)", [10.0]),
    ("log10", "Float", "log10(T.F)", [10.0]),
    ("pow", "Float", "pow(T.G, T.G)", [4.0, 4.0]),
    ("round", "Integer", "round(T.F)", [10.0]),
    ("ceiling", "Integer", "ceiling(T.F)", [10.0]),
    ("floor", "Integer", "floor(T.F)", [10.0]),
    ("sin", "Float", "sin(T.F)", [10.0]),
    ("cos", "Float", "cos(T.F)", [10.0]),
    ("tan", "Float", "tan(T.F)", [10.0]),
    ("atan", "Float", "atan(T.F)", [10.0]),
    ("atan2", "Float", "atan2(T.F, T.G)", [10.0, 4.0]),
    ("toDecimal", "Float", "toDecimal(T.I)", [7]),
    ("toFloat", "Float", "toFloat(T.I)", [7]),
    ("toString", "String", "toString(T.I)", [7]),
    ("bitAnd", "Integer", "bitAnd(T.I, T.J)", [7, 3]),
    ("bitOr", "Integer", "bitOr(T.I, T.J)", [7, 3]),
    ("bitXor", "Integer", "bitXor(T.I, T.J)", [7, 3]),
    ("bitNot", "Integer", "bitNot(T.I)", [7]),
    ("bitShiftLeft", "Integer", "bitShiftLeft(T.I, T.J)", [7, 3]),
    ("bitShiftRight", "Integer", "bitShiftRight(T.I, T.J)", [7, 3]),
    ("year", "Integer", "year(T.D)", ["2024-06-03"]),
    ("monthNumber", "Integer", "monthNumber(T.D)", ["2024-06-03"]),
    ("dayOfMonth", "Integer", "dayOfMonth(T.D)", ["2024-06-03"]),
    ("dayOfYear", "Integer", "dayOfYear(T.D)", ["2024-06-03"]),
    ("dayOfWeekNumber", "Integer", "dayOfWeekNumber(T.D)", ["2024-06-03"]),
    ("weekOfYear", "Integer", "weekOfYear(T.D)", ["2024-06-03"]),
    ("quarterNumber", "Integer", "quarterNumber(T.D)", ["2024-06-03"]),
    ("hour", "Integer", "hour(T.TS)", ["2024-06-03 19:15:00"]),
    ("minute", "Integer", "minute(T.TS)", ["2024-06-03 19:15:00"]),
    ("second", "Integer", "second(T.TS)", ["2024-06-03 19:15:00"]),
    ("datePart", "StrictDate", "datePart(T.TS)", ["2024-06-03 19:15:00"]),
    ("firstDayOfMonth", "StrictDate", "firstDayOfMonth(T.D)", ["2024-06-03"]),
    ("firstDayOfYear", "StrictDate", "firstDayOfYear(T.D)", ["2024-06-03"]),
    ("firstDayOfQuarter", "StrictDate", "firstDayOfQuarter(T.D)", ["2024-06-03"]),
    ("firstDayOfWeek", "StrictDate", "firstDayOfWeek(T.D)", ["2024-06-03"]),
    ("firstHourOfDay", "DateTime", "firstHourOfDay(T.TS)", ["2024-06-03 19:15:00"]),
    ("firstMinuteOfHour", "DateTime", "firstMinuteOfHour(T.TS)", ["2024-06-03 19:15:00"]),
    ("firstSecondOfMinute", "DateTime", "firstSecondOfMinute(T.TS)", ["2024-06-03 19:15:00"]),
    ("previousDayOfWeek", "StrictDate", "previousDayOfWeek(T.D, 'Monday')",
     ["2024-06-03", "Monday"]),
    ("mostRecentDayOfWeek", "StrictDate", "mostRecentDayOfWeek(T.D, 'Monday')",
     ["2024-06-03", "Monday"]),
    ("greatest", "Integer", "greatest(T.I, T.J)", [[7, 3]]),
    # -- second batch: trigonometry over a bounded operand, comparisons, parsing, and the
    # date functions whose extra argument is an enum or a format string. Anything the engine
    # refuses is dropped by the bisect, so a case that turns out not to be reachable from a
    # property mapping costs one round rather than a broken probe.
    ("acos", "Float", "acos(T.H)", [0.5]),
    ("asin", "Float", "asin(T.H)", [0.5]),
    ("cosh", "Float", "cosh(T.H)", [0.5]),
    ("sinh", "Float", "sinh(T.H)", [0.5]),
    ("tanh", "Float", "tanh(T.H)", [0.5]),
    ("cot", "Float", "cot(T.F)", [10.0]),
    ("char", "String", "char(T.I)", [7]),
    ("repeatString", "String", "repeatString(T.S, 2)", ["alpha", 2]),
    ("between", "Boolean", "between(T.I, 1, 9)", [7, 1, 9]),
    ("eq", "Boolean", "eq(T.I, T.J)", [7, 3]),
    ("equal", "Boolean", "equal(T.I, T.J)", [7, 3]),
    ("greaterThan", "Boolean", "greaterThan(T.I, T.J)", [7, 3]),
    ("greaterThanEqual", "Boolean", "greaterThanEqual(T.I, T.J)", [7, 3]),
    ("lessThan", "Boolean", "lessThan(T.I, T.J)", [7, 3]),
    ("lessThanEqual", "Boolean", "lessThanEqual(T.I, T.J)", [7, 3]),
    ("and", "Boolean", "and(T.B, T.B)", [True, True]),
    ("or", "Boolean", "or(T.B, T.B)", [True, True]),
    ("parseInteger", "Integer", "parseInteger(T.N)", ["42"]),
    ("parseFloat", "Float", "parseFloat(T.NF)", ["42.5"]),
    ("parseDecimal", "Float", "parseDecimal(T.NF)", ["42.5"]),
    ("parseBoolean", "Boolean", "parseBoolean(T.NB)", ["true"]),
    ("month", "Integer", "month(T.D)", ["2024-06-03"]),
    ("dayOfWeek", "String", "dayOfWeek(T.D)", ["2024-06-03"]),
    ("firstMillisecondOfSecond", "DateTime", "firstMillisecondOfSecond(T.TS)",
     ["2024-06-03 19:15:00"]),
    # The enum arguments are written as STRING LITERALS, not as `DurationUnit.DAYS`. Inside a
    # relational property mapping `X.Y` is a table-and-column reference, so an enum literal
    # there is read as a table -- "Can't find table 'DurationUnit'" -- and the fully qualified
    # form is a parse error. The lowerings strip quotes off the argument, so a literal is
    # what they expect. Nothing says so; it is visible only in the transform's source.
    ("dateDiff", "Integer", "dateDiff(T.D, T.D2, 'DAYS')",
     ["2024-06-03", "2024-06-13", "DAYS"]),
    ("adjust", "StrictDate", "adjust(T.D, 5, 'DAYS')", ["2024-06-03", 5, "DAYS"]),
    # ISO8601, not a format PATTERN. Relational formatDate accepts exactly two named formats
    # -- ISO8601 and ISO8601_NanoSecondPrecision -- in every dialect that implements it, and
    # `yyyy-MM-dd` fails with "Unsupported DateFormat". Consistent across dialects, so it is
    # a limitation of the relational lowering rather than a defect in one of them.
    ("formatDate", "String", "formatDate(T.D, 'ISO8601')", ["2024-06-03", "ISO8601"]),
    ("date", "StrictDate", "date(2024, 6, 3)", [2024, 6, 3]),
    # No two-argument `max`/`min`: their only lowering is the single-argument AGGREGATE
    # `max(%s)`, so a two-argument call dies inside the string formatter with "Unused format
    # args. [2] arguments provided" (F45). `greatest` and `least` are the two-argument
    # spellings that work, and both are probed above.

    ("timeBucket", "DateTime", "timeBucket(T.TS, 1, 'DAYS')",
     ["2024-06-03 19:15:00", 1, "DAYS"]),
    # -- lang. These are the type- and control-flow operations, and it is not obvious any of
    # them is reachable from a relational property mapping at all; that is the question.
    ("if", "String", "if(T.I > 5, 'big', 'small')", [True, "big", "small"]),
    # No `cast`: inside a relational mapping `@X` is a JOIN reference, so `cast(T.I, @Float)`
    # is read as a join named Float -- "Can't find join 'Float' in database 'DB'". The same
    # shape as DurationUnit.DAYS being read as a table: relational mapping syntax claims the
    # punctuation these type-level operations need.

    ("toOne", "Integer", "toOne(T.I)", [7]),
    ("toOneMany", "Integer", "toOneMany(T.I)", [7]),
    # -- variant. A JSON payload in a VARCHAR column, converted in and out.
    ("toVariant", "String", "toVariant(T.S)", ["alpha"]),
    # The JSON payload is a brace-free array. The model is built with str.format, so a `{`
    # in the seed is read as a replacement field -- and the failure is an IndexError from
    # format() rather than anything to do with JSON.
    ("fromJson", "String", "fromJson(T.JS)", ["[1]"]),
    # toJson over the JSON column, not the plain string one. Applied to 'alpha' the engine
    # tries to PARSE it as JSON and fails with "Malformed JSON at byte 0" -- so toJson
    # expects something already JSON-shaped rather than converting an arbitrary value.
    # The oracle argument is the PARSED value, not the JSON text. The column holds the text
    # `[1]`; the variant it denotes is the list [1]; toJson renders that back to `[1]`.
    # Passing the text itself made the oracle encode a STRING and answer `"[1]"` -- quotes
    # and all -- which is the right answer to the wrong question.
    ("toJson", "String", "toJson(T.JS)", [[1]]),
    # -- the families that were refused by the oracle until now. Each is implemented against
    # a stated convention, so a disagreement here names the convention that was wrong.
    ("matches", "Boolean", "matches(T.S, 'al.*')", ["alpha", "al.*"]),
    # The regexp family is probed at the arity its LOWERING accepts, not the arity the
    # registry advertises. Each transform is typed to a fixed number of parameters --
    # String[3] for regexpCount, String[4] for regexpIndexOf, String[5] for regexpExtract --
    # and the shorter overloads, which are registered, cannot reach them (F46). The trailing
    # argument is the regex flags.
    #
    # regexpIndexOf's third argument is the GROUP and regexpExtract's are extractAll and the
    # group, as legend-engine's Pure signatures name them. Until 2026-10-28 both asked for group 1
    # of a pattern with no group, which Pure answers with -1 and nothing, and the probe compared
    # that with the whole match (the improvement plan's Phase 4 adjudication).
    ("regexpLike", "Boolean", "regexpLike(T.S, 'ph', 'i')", ["alpha", "ph"]),
    ("regexpCount", "Integer", "regexpCount(T.S, 'a', 'i')", ["alpha", "a"]),
    ("regexpIndexOf", "Integer", "regexpIndexOf(T.S, 'ph', 0, 'CASE_SENSITIVE')",
     ["alpha", "ph", 0]),
    ("regexpExtract", "String", "regexpExtract(T.S, 'ph', 'false', 0, 'CASE_SENSITIVE')",
     ["alpha", "ph", False, 0]),
    # regexpReplace has no arity that works: three arguments hits the same index error and
    # four is refused as well. Left at the natural spelling so the failure is recorded
    # against the function rather than hidden by omission.
    ("regexpReplace", "String", "regexpReplace(T.S, 'a', 'z')", ["alpha", "a", "z"]),
    ("hash", "String", "hash(T.S, 'MD5')", ["alpha", "MD5"]),
    ("hashCode", "Integer", "hashCode(T.S)", ["alpha"]),
    ("jaroWinklerSimilarity", "Float", "jaroWinklerSimilarity(T.S, T.S2)",
     ["alpha", "beta"]),
    ("parseDate", "StrictDate", "parseDate(T.DS)", ["2024-06-03"]),
    ("convertTimeZone", "DateTime", "convertTimeZone(T.TS, 'UTC')",
     ["2024-06-03 19:15:00", "UTC"]),
    ("today", "StrictDate", "today()", []),
    ("firstDayOfThisMonth", "StrictDate", "firstDayOfThisMonth()", []),
    ("firstDayOfThisYear", "StrictDate", "firstDayOfThisYear()", []),
    ("firstDayOfThisQuarter", "StrictDate", "firstDayOfThisQuarter()", []),
    # These three cannot AGREE by construction -- two clock readings taken seconds apart, and
    # a fresh GUID each call. They are probed anyway: whether the engine can execute them is
    # a separate question from whether their values can be compared, and the first question
    # is the one the burndown asks.
    ("now", "DateTime", "now()", []),
    ("currentUserId", "String", "currentUserId()", []),
    ("generateGuid", "String", "generateGuid()", []),
    ("least", "Integer", "least(T.I, T.J)", [[7, 3]]),
]

CASES += _calendar_cases()

MODEL = """Class probe::P
{{
   k: String[1];
{props}
}}

###Relational
Database probe::DB
(
   Table T ( K VARCHAR(20) PRIMARY KEY, S VARCHAR(50), S2 VARCHAR(50), S64 VARCHAR(50),
             N VARCHAR(20), NF VARCHAR(20), NB VARCHAR(20), JS VARCHAR(40),
             DS VARCHAR(20),
             I INTEGER, J INTEGER, F DOUBLE, G DOUBLE, H DOUBLE, B BIT,
             D DATE, D2 DATE, TS TIMESTAMP )
)

###Mapping
Mapping probe::M
(
   probe::P: Relational
   {{
      ~primaryKey ( [probe::DB]T.K )
      ~mainTable [probe::DB]T
      k: [probe::DB]T.K,
{maps}
   }}
)

###Connection
RelationalDatabaseConnection probe::Conn
{{ store: probe::DB; type: DuckDB; specification: DuckDB {{ }}; auth: Test; }}
###Runtime
Runtime probe::RT {{ mappings: [ probe::M ]; connections: [ probe::DB: [ env: probe::Conn ] ]; }}

###Data
Data probe::Seed
{{
  Relational
  #{{
    default.T:
      'K,S,S2,S64,N,NF,NB,JS,DS,I,J,F,G,H,B,D,D2,TS\\n' +
      'R1,alpha,beta,YWxwaGE=,42,42.5,true,[1],2024-06-03,7,3,10.0,4.0,0.5,true,'
        + '2024-06-03,2024-06-13,2024-06-03 19:15:00\\n';
  }}#
}}

###Service
Service probe::Svc
{{
   pattern: '/probe/svc';
   documentation: 'One column per registered function, so a refusal names the function.';
   execution: Single
   {{
      query: |probe::P.all()->project(~[{cols}]);
      mapping: probe::M;
      runtime: probe::RT;
   }}
   testSuites:
   [
      Svc_suite:
      {{
         data: [ connections: [ env: Reference #{{ probe::Seed }}# ] ]
         tests:
         [
            expected_rows:
            {{
               serializationFormat: PURE_TDSOBJECT;
               asserts: [ a: EqualToJson #{{ expected: ExternalFormat #{{
                   contentType: 'application/json'; data: '[]'; }}#; }}# ]
            }}
         ]
      }}
   ]
}}
"""


def build(cases):
    props = "\n".join(f"   f{i}: {ret}[0..1];" for i, (_n, ret, _e, _a) in enumerate(cases))
    maps = ",\n".join(f"      f{i}: [probe::DB]{expr.replace('T.', 'T.')}"
                      for i, (_n, _r, expr, _a) in enumerate(cases))
    cols = ", ".join(f"f{i}:x|$x.f{i}" for i in range(len(cases)))
    return MODEL.format(props=props, maps=maps, cols=cols)


def attempt(cases) -> tuple[str, str]:
    """Run one probe. Returns (outcome, detail) where outcome is 'ok' or a function name."""
    work = Path(tempfile.mkdtemp())
    tmp = work / "probe.pure"
    tmp.write_text(build(cases), encoding="utf-8")
    _status, out = runner.launch([str(tmp), "--testable=probe::Svc", f"--dump={work}"], work)
    m = re.search(r"The function '(\w+)'.{0,80}?is not supported yet", out, re.S)
    if m:
        return m.group(1), "not supported yet"
    m = re.search(r"No SQL translation exists for the PURE function '([\w:]+)'", out)
    if m:
        return m.group(1).split("::")[-1], "no SQL translation"
    # The console prints `actual` truncated at 300 characters, which is right for a human
    # reading a diff and useless for parsing 79 columns. --dump writes the payload whole.
    dumps = sorted(work.glob("*.actual.json"))
    if dumps:
        return "ok", dumps[0].read_text()
    m = re.search(r"actual\s*:\s*(.*)", out)
    if m:
        return "ok", m.group(1)
    # A function can be in getSupportedFunctions() and missing from the DynaFunctionRegistry
    # the lowering actually consults -- two registries, one function, two answers (F36). The
    # message is buried inside a quoted assert failure, so it needs its own pattern or the
    # bisect files it as unattributed.
    m = re.search(r"dyna function \[(\w+)\] is not registered", out)
    if m:
        return m.group(1), "not in the DynaFunctionRegistry"
    # Compilation errors name the function they could not match, which is as good as a
    # refusal for the purpose of dropping it.
    m = re.search(r"Can't find a match for function '([\w:]+)", out)
    if m:
        return m.group(1).split("::")[-1], "no matching signature"
    m = re.search(r"(?:EngineException|Caused by)[^\n]*", out)
    return "ERROR", (m.group(0) if m else out[-300:])


def bisect_error(cases) -> tuple[list, list]:
    """Split a set that fails for a reason no message attributes to a function.

    Some failures name nothing usable -- a type mismatch deep in plan generation, a NULL
    pointer. Rather than stopping the whole probe on one bad case, halve the set and keep
    whichever halves run. Log2(n) runs finds every offender, which is the difference between
    a probe that grows by adding a line and one that has to be nursed through every addition.
    """
    if len(cases) == 1:
        return [], cases
    mid = len(cases) // 2
    good, bad = [], []
    for half in (cases[:mid], cases[mid:]):
        outcome, _d = attempt(half)
        if outcome == "ok":
            good += half
        else:
            g, b = bisect_error(half)
            good += g
            bad += b
    return good, bad


def _same(a, b) -> bool:
    """Equal allowing for PRESENTATION differences the probe is not asking about.

    A float rendered 3.1622776601683795 by one side and 3.16227766016838 by the other is the
    same answer at different precision, and a date rendered with or without a time component
    is the same instant. Neither is what this probe is looking for -- it is looking for a
    different ANSWER -- so both are normalised away rather than reported as disagreements
    that a person then has to dismiss one at a time.
    """
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == b:
            return True
        scale = max(abs(a), abs(b), 1.0)
        return abs(a - b) <= 1e-9 * scale
    sa, sb = str(a), str(b)
    # A timestamp rendered '2024-06-03T19:00:00.000000000+0000' and one rendered
    # '2024-06-03 19:00:00' are the same instant. Normalise the separator and the trailing
    # zero fraction/offset rather than reporting three false disagreements a person then has
    # to dismiss -- the probe is looking for a different ANSWER, not a different format.
    def norm(s: str) -> str:
        s = s.replace("T", " ")
        s = re.sub(r"\.0+(?=$|[+-Z])", "", s)
        s = re.sub(r"(?:[+-]\d{4}|Z)$", "", s)
        return s.strip()
    # Compared for EQUALITY after normalisation, not by prefix. An earlier version allowed
    # either value to be a prefix of the other, to absorb a timestamp printed at lower
    # precision -- and it quietly passed `substring` where the engine said 'lpha' and the
    # oracle said 'lp'. A comparison loose enough to forgive formatting was loose enough to
    # forgive a wrong answer, which is the one thing the probe exists to catch.
    return norm(sa) == norm(sb)


def compare(cases, actual: str) -> list:
    """Engine value against oracle value, per function.

    A disagreement is NOT called a defect. The oracle is an independent implementation, so a
    mismatch says one of the two is wrong without saying which, and this corpus has been the
    wrong one at least as often as the engine -- concat over all-NULL, `!=` over NULL. What
    the probe can honestly do is put the two numbers next to each other and stop.
    """
    import json

    import oracle
    try:
        rows = json.loads(actual[actual.index("["):])
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"  could not parse the engine result: {exc}")
        return [], []
    got = rows[0]
    agree, differ, nooracle = 0, [], []
    for i, (name, _ret, _expr, args) in enumerate(cases):
        engine = got.get(f"f{i}")
        if name in SHAPE:
            # a value no two calls share: what is compared is its shape
            if SHAPE[name](engine):
                agree += 1
            else:
                differ.append((name, engine, "a value of its shape"))
            continue
        try:
            mine = oracle._dynafunction(name, args)
        except Exception as exc:                        # Unsupported, or a real bug in mine
            nooracle.append((name, f"{type(exc).__name__}: {str(exc)[:80]}"))
            continue
        if _same(engine, mine):
            agree += 1
        else:
            differ.append((name, engine, mine))
    print(f"\n  oracle agrees with the engine on {agree} of {len(cases)}")
    if nooracle:
        print(f"  no oracle value for {len(nooracle)}: "
              + ", ".join(f"{n} ({w})" for n, w in nooracle))
    if differ:
        print(f"\n  {len(differ)} DISAGREEMENTS -- one side is wrong and the probe cannot say")
        print("  which; each needs a person to adjudicate against the dialect's own rules:")
        for name, engine, mine in differ:
            print(f"    {name:<22} engine={engine!r:<28} oracle={mine!r}")
    return differ, nooracle


# Functions whose every call answers differently (a fresh GUID), compared by the shape of the value
# rather than the value. Before 2026-10-28 generateGuid was compared with the oracle's own fresh GUID
# and recorded as disagreeing (the improvement plan's Phase 4 adjudication).
SHAPE = {
    "generateGuid": lambda v: isinstance(v, str) and re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", v) is not None,
}


# The checkout's copy: `bazel run` names the workspace, and the file is a draft the probes refresh (the Bazel workplan's
# G-13), never an output of the build.
EVIDENCE = (Path(os.environ["BUILD_WORKSPACE_DIRECTORY"]) if os.environ.get("BUILD_WORKSPACE_DIRECTORY")
            else Path(__file__).resolve().parents[2]) / "docs/FUNCTIONS_EXECUTED.tsv"

# The disagreements the improvement plan's Phase 4 adjudicated (2026-10-28), each the ENGINE's, by finding: the
# evidence names the side that is wrong. A disagreement not listed is unadjudicated, and a person must decide it
# before the function goes into a service.
ADJUDICATED = {
    "adjust": "the engine's: F38, a StrictDate rendered as a DateTime",
    "dayOfYear": "the engine's: F35, DuckDB's day() is the day of the month",
    "firstDayOfWeek": "the engine's: F38, a StrictDate rendered as a DateTime",
    # stage 2: the oracle now reads legend-pure's definitions, which the engine's SQL does not follow here either
    "indexOf": "the engine's: F72, indexOf is SQL's strpos, one-based with 0 for a miss",
    "dayOfWeekNumber": "the engine's: F74, dayOfWeekNumber counts from Sunday",
}

# Functions that WERE run, diagnosed, written up, and then removed from the case list because
# leaving them in fails the whole probe. Recorded here so they are not reported as untouched:
# "we tried this and here is what happened" is a different state from "nobody has looked", and
# the scoreboard has no business conflating them just because the case is no longer runnable.
DIAGNOSED = {
    "max": "REFUSED: two-arg form has only the aggregate lowering (F45)",
    "min": "REFUSED: two-arg form has only the aggregate lowering (F45)",
    "cast": "REFUSED: @Float is read as a join reference in a relational mapping (F45)",
    "variance": "REFUSED: lowers to a list function, invalid in a grouped select (F40)",
    "isEmpty": "REFUSED: aggregate position lowers to a per-row null test (F40)",
    "isNotEmpty": "REFUSED: aggregate position lowers to a per-row null test (F40)",
}


def merge_evidence(probe: str, agreeing, failing=()) -> None:
    """Record this probe's verdicts, replacing only ITS OWN rows.

    The file was keyed by bare function name and every probe rewrote the names it knew, so
    the collection probe's `size` -- which fails over a to-many -- silently retracted the
    relation probe's `size`, which passes. The scoreboard fell by fourteen functions and the
    cause was bookkeeping, not the engine.

    Keying by (probe, function) fixes that, and keeps the two facts separate: `size` executes
    as a relation operation AND fails as a collection one, which is exactly the kind of
    per-family distinction the function families exist to express.
    """
    f = EVIDENCE
    rows = []
    if f.exists():
        rows = [tuple(line.split("\t")) for line in f.read_text().splitlines()[1:]
                if line.count("\t") == 2]
    rows = [r for r in rows if r[1] != probe]
    rows += [(n, probe, "ok") for n in agreeing]
    rows += [(n, probe, why) for n, why in failing]
    f.write_text("\n".join(["function\tprobe\tverdict"]
                            + ["\t".join(r) for r in sorted(set(rows))]) + "\n")


def record(ran, rejected, differ=(), nooracle=()) -> None:
    """Write the evidence file the function scoreboard reads.

    Written from what actually ran rather than maintained by hand, for the same reason the
    implemented/refused counts are read out of the oracle's registries: a list of "functions
    we have tested" that is edited separately from the testing drifts, and drifts in the
    flattering direction. A function leaves this file the moment it stops executing.

    The matrix contributes too. Its cells are asserted per-cell against independently
    computed values, which is the same standard the probe holds, so a function exercised
    there is as executed as one exercised here.
    """
    import combos

    unjudged = dict(nooracle)
    names = {name for name, _r, _e, _a in ran} - set(differ) - set(unjudged)
    # Only the transforms a matrix cell USES: until 2026-10-28 every function the matrix could
    # spell was added, `substring` among them, which XFORM then left out (F37), so a function the
    # probe found disagreeing was counted as agreeing too.
    used = {x for xs in combos.XFORM.values() for x in xs}
    for key, fn in list(combos._UNARY.items()) + list(combos._BINARY.items()):
        if key in used:
            names.add(fn)
    for key, (fn, _lit) in combos._LITERAL_ARG.items():
        if key in used:
            names.add(fn)
    fails = [(n, f"REFUSED: {why}") for n, why in rejected]
    fails += [(n, "DISAGREES with the oracle" + (f" -- {ADJUDICATED[n]}" if n in ADJUDICATED else ""))
              for n in differ]
    # Ran, and the oracle has no value to compare: a clock, an identity, an unspecified hash. Not
    # agreement, which is what the probe recorded for them before 2026-10-28.
    fails += [(n, f"RUNS; no oracle value -- {why}") for n, why in sorted(unjudged.items())
              if n not in names]
    fails += sorted(DIAGNOSED.items())
    merge_evidence("scalar", sorted(names), fails)
    print(f"\n  {len(names)} functions executed and agreeing, "
          f"{len(fails)} refused or disagreeing")


def main() -> None:
    """Bisect by refusal: run the whole set, drop whatever the engine names, run again.

    One unsupported column fails the entire query, so the engine reports refusals one at a
    time. Removing the named function and retrying walks the whole list in as many rounds as
    there are refusals, which is far fewer than one round per function.
    """
    cases, rejected, differ, nooracle = list(CASES), [], [], []
    for _round in range(len(CASES)):
        outcome, detail = attempt(cases)
        if outcome == "ok":
            print(f"{len(cases)} functions EXECUTE in a relational property mapping.")
            found, nooracle = compare(cases, detail)
            differ = [d[0] for d in found]
            break
        if outcome == "ERROR":
            print(f"  unattributed failure, bisecting: {detail[:110]}", flush=True)
            cases, dropped = bisect_error(cases)
            # Re-run each dropped case ALONE to get its own message. The bisect only knows
            # that a set failed, so recording "fails with no message naming it" throws away a
            # diagnosis the engine is perfectly willing to give -- `eq` and `parseBoolean`
            # were filed under that phrase for a while when their real messages, once asked
            # for individually, named the exact registry that had never heard of them.
            for c in dropped:
                _out, why = attempt([c])
                why = why if _out != "ok" else "fails only in company"
                if _out == "ERROR":
                    why = f"unattributed: {why[:70]}"
                rejected.append((c[0], why))
                print(f"  REJECTED  {c[0]:<22} {why}", flush=True)
            if not cases:
                break
            continue
        rejected.append((outcome, detail))
        print(f"  REJECTED  {outcome:<22} {detail}", flush=True)
        cases = [c for c in cases if c[0] != outcome]
    else:
        print("every case was rejected")

    record(cases, rejected, differ, nooracle)

    if rejected:
        print(f"\n{len(rejected)} of {len(CASES)} registered functions do NOT execute here.")
        print("They are in getSupportedFunctions(), so the engine routes to a handler and the")
        print("handler then refuses. A registry that over-reports matters: it is what a")
        print("planner consults to decide a query is expressible BEFORE running it.")


if __name__ == "__main__":
    main()
