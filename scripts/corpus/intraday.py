"""
Intraday milestoning: the services of 99-intraday-services.pure
(docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 3).

Every milestoned query in the corpus was day-grained: DATE processing columns, StrictDate dates. These
read intraday::BookLimit, BookStatus (processing-temporal, TIMESTAMP columns, a DateTime infinity date)
and MarkRevision (bitemporal: DATE business, TIMESTAMP processing), 8113-intraday.pure, at a time of day,
over a seed whose versions change within a day, twice in one day and exactly at midnight
(seed.BOOK_LIMIT_PT). A version is [in, out), so the instant of a change is the boundary that matters.

  ITL_/ITP_  the book limits in force at every instant the seed changes one: one second BEFORE it and
             AT it, as a DateTime literal (ITL_); and at it as a DateTime parameter, `all($asOf)`
             (ITP_), which must return the same rows (an invariance group). ITL_Latest and
             ITL_Year2099 read %latest and a far instant, which must agree.
  ITS_/ITD_  a StrictDate (literal and parameter) where the columns are TIMESTAMPs: its midnight, the
             twin of the DateTime literal at 00:00:00; and a Date parameter bound to a DateTime.
  ITV_/ITR_  `allVersions()` and `getAllVersionsInRange(start, end)`, the range as literals and as
             parameters: a version overlaps [start, end] when it starts at or before the end and
             ends after the start.
  ITN_       from a pre-trade limit check: its book's limit at an instant, at a parameter, at
             midnight, at %latest, and when the check ran (`$x.bookLimit($x.checkedAt)`); its
             versions counted. From a valuation run: its instrument's mark with both dates, literal,
             %latest, or read from the run (`$x.mark($x.runAt, $x.valuationDate)`). Not from a
             trade: a milestoned property on trading::Trade breaks the engine's compile of its XStore
             mapping (docs/UPSTREAM_FINDINGS.md, F66).
  ITX_       from a dated limit to its book's status with no date of its own, which takes the
             limit's (date propagation), beside the same navigation dated explicitly.
  ITM_       the bitemporal marks at a processing instant and a business date: before, at and after
             a publication, a correction and a midnight restatement.
  ITG_       graph fetch with a dated property, and with a propagated one.

Probed on the pinned engine before any service was written (the plan's Phase 3 record): every form
here ran and agreed with the oracle, except that the engine's grammar refuses `.allVersionsInRange(...)`
on a class (DomainParseTreeWalker.allOrFunction: "is not supported"), so the root range is written as the
function, `->getAllVersionsInRange(...)`; and that a connection's `timezone` never reaches a service
test, so nothing here depends on one.
"""
from __future__ import annotations

import emptiness
import literals
import oracle
import seed
from query import DateArg, DateTimeArg, Hop, Param, PathArg, Pred, Proj, Spec, Versions

LIMIT = "intraday::BookLimit"
MARK = "intraday::MarkRevision"
CHECK = "intraday::LimitCheck"
RUN = "intraday::ValuationRun"
MAPPING = "stress::AllMapping"
# The seed's open-ended processing date: the table's INFINITY_DATE, not an instant anything changes at.
INFINITY_TS = "9999-12-31 00:00:00"

LIMIT_COLS = ["bookId", "limitAmount", "currency", "revision", "approvedBy"]
MARK_COLS = ["instrumentId", "price", "source"]

cases = literals.cases


def _spec(name: str, doc: str, root: str, values, as_of=None, filters=None, params=None,
          cases_=None, versions=None, graph=None) -> Spec:
    s = Spec(f"stress::{name}", f"/stress/intraday/{name.lower()}", doc, root)
    s.projections = [Proj(alias, list(path)) if not isinstance(path, Proj) else path
                     for alias, path in values]
    s.as_of = as_of
    s.versions = versions
    s.filters = filters or []
    s.params = params or []
    s.cases = cases_ or []
    s.graph = graph
    s.mapping = MAPPING
    return s


def _limit_cols():
    return [(p, [p]) for p in LIMIT_COLS]


def _stamp(t: str) -> str:
    """'2024-06-03 09:21:00' -> '20240603T092100', a service-name fragment."""
    return t.replace("-", "").replace(":", "").replace(" ", "T")


def _second_before(t: str) -> str:
    import datetime as dt
    v = dt.datetime.strptime(t, "%Y-%m-%d %H:%M:%S") - dt.timedelta(seconds=1)
    return v.strftime("%Y-%m-%d %H:%M:%S")


def change_instants() -> list[str]:
    """Every instant the seed records or supersedes a book limit at: where a reading changes."""
    return sorted({t for r in seed.BOOK_LIMIT_PT for t in (r["IN_Z"], r["OUT_Z"])} - {INFINITY_TS})


def instants() -> tuple[list[tuple[Spec, Spec]], list[tuple[Spec, Spec]]]:
    """(before, at) per change instant, and (literal, parameter) per instant. The earliest instant
    has no `before`: nothing is in force a second before the first limit is recorded."""
    pairs, twins = [], []
    first = change_instants()[0]
    for t in change_instants():
        stamp = _stamp(t)
        at = _spec(f"ITL_{stamp}_At", f"The book limits in force AT {t}, the instant one changes: the "
                   f"new version answers and the old one does not ([in, out)).", LIMIT,
                   _limit_cols(), as_of=DateTimeArg(t))
        twin = _spec(f"ITP_{stamp}_At", f"ITL_{stamp}_At with the instant as a DateTime parameter, "
                     f"`all($asOf)`: the same rows.", LIMIT, _limit_cols(), as_of=Param("asOf"),
                     params=[("asOf", "DateTime", "1")],
                     cases_=[("expected_rows", {"asOf": DateTimeArg(t)})])
        twins.append((at, twin))
        if t == first:
            continue
        b = _second_before(t)
        before = _spec(f"ITL_{stamp}_Before", f"The book limits in force at {b}, one second before "
                       f"a change at {t}: still the old version.", LIMIT, _limit_cols(),
                       as_of=DateTimeArg(b))
        pairs.append((before, at))
    return pairs, twins


def dates() -> list[Spec]:
    """StrictDates and Dates against TIMESTAMP columns, %latest, and a far instant."""
    out = [
        _spec("ITS_20240603", "A StrictDate where the processing columns are TIMESTAMPs: its "
              "midnight. BK-EQ's limit is recorded at exactly 2024-06-03 00:00:00, so it is in "
              "force; ITL_20240603T000000_At must agree.", LIMIT, _limit_cols(),
              as_of=DateArg("2024-06-03")),
        _spec("ITS_20240604", "A StrictDate at the midnight BK-CASH-US's limit is reset: the new "
              "version. ITL_20240604T000000_At must agree.", LIMIT, _limit_cols(),
              as_of=DateArg("2024-06-04")),
        _spec("ITSP_20240604", "ITS_20240604 with the date as a StrictDate parameter.", LIMIT,
              _limit_cols(), as_of=Param("asOf"), params=[("asOf", "StrictDate", "1")],
              cases_=[("expected_rows", {"asOf": DateArg("2024-06-04")})]),
        _spec("ITD_20240603T094530", "A Date parameter bound to a DateTime: the instant "
              "BK-CASH-US's limit is raised. ITL_20240603T094530_At must agree.", LIMIT,
              _limit_cols(), as_of=Param("asOf"), params=[("asOf", "Date", "1")],
              cases_=[("expected_rows", {"asOf": DateTimeArg("2024-06-03 09:45:30")})]),
        _spec("ITL_Latest", "%latest: the versions whose out is the DateTime infinity date. "
              "BK-CASH-EU's limit was withdrawn with no successor, so it is absent.", LIMIT,
              _limit_cols(), as_of="latest"),
        _spec("ITL_Year2099", "An instant long after every change: the same versions as %latest.",
              LIMIT, _limit_cols(), as_of=DateTimeArg("2099-01-01 00:00:00")),
    ]
    return out


def versions() -> list[Spec]:
    morning = (DateTimeArg("2024-06-03 09:00:00"), DateTimeArg("2024-06-03 10:00:00"))
    return [
        _spec("ITV_All", "Every version of every book limit, `allVersions()`: no date narrows.",
              LIMIT, _limit_cols(), versions=Versions()),
        _spec("ITV_OneBook", "Every version of one book's limit: four, three of them recorded on "
              "one day.", LIMIT, _limit_cols(), versions=Versions(),
              filters=[Pred(["bookId"], "==", "BK-CASH-US")]),
        _spec("ITR_Morning", "The versions that overlap 09:00 to 10:00 on 3 June: a version counts "
              "when it starts at or before the end and ends after the start.", LIMIT,
              _limit_cols(), versions=Versions(*morning)),
        _spec("ITRP_Morning", "ITR_Morning with the bounds as DateTime parameters.", LIMIT,
              _limit_cols(), versions=Versions(Param("rangeStart"), Param("rangeEnd")),
              params=[("rangeStart", "DateTime", "1"), ("rangeEnd", "DateTime", "1")],
              cases_=[("expected_rows", {"rangeStart": morning[0], "rangeEnd": morning[1]})]),
        _spec("ITR_Boundaries", "A range whose start is the instant BK-CASH-US's first limit ends "
              "and whose end is the instant BK-VOL-01's begins: the first is out, the second in.",
              LIMIT, _limit_cols(), versions=Versions(DateTimeArg("2024-06-03 09:21:00"),
                                                      DateTimeArg("2024-06-03 09:35:00"))),
        _spec("ITR_Gap", "A range inside BK-EQ's hour without a limit: it has no version there.",
              LIMIT, _limit_cols(), versions=Versions(DateTimeArg("2024-06-03 12:00:00"),
                                                      DateTimeArg("2024-06-03 12:59:59"))),
        _spec("ITR_Days", "A range of two StrictDates over TIMESTAMP columns: midnight to midnight, "
              "inclusive at the end, so the version recorded at 2024-06-04 00:00:00 is in.", LIMIT,
              _limit_cols(), versions=Versions(DateArg("2024-06-03"), DateArg("2024-06-04"))),
    ]


def _check(name, doc, values, **kw) -> Spec:
    return _spec(name, doc, CHECK, [("checkId", ["checkId"])] + values, **kw)


def _run(name, doc, values, **kw) -> Spec:
    return _spec(name, doc, RUN, [("runId", ["runId"])] + values, **kw)


# The books with a version overlapping ITN_LimitVersionsInRange's range: a count over none is F6's
# (count() over an empty to-many returns 1 on the engine), so only these are counted.
_COUNTED_BOOKS = ["BK-CASH-US", "BK-VOL-01", "BK-EQ", "BK-FX", "BK-RATES"]


def navigation() -> list[Spec]:
    """From a limit check and a valuation run, which are not temporal, so every milestoned property
    takes its date(s)."""
    at0921, before0921 = DateTimeArg("2024-06-03 09:21:00"), DateTimeArg("2024-06-03 09:20:59")
    checked = PathArg(("checkedAt",))
    morning = [DateTimeArg("2024-06-03 09:30:00"), DateTimeArg("2024-06-04 00:00:00")]

    def limit(dates, *props):
        return [(p, [Hop("bookLimit", dates), p]) for p in props]

    def mark(dates):
        return [(p, [Hop("mark", dates), p]) for p in ("price", "source")]

    return [
        _check("ITN_LimitAt0921", "Each check's book limit at 09:21:00 on 3 June, the instant "
               "BK-CASH-US's is cut.", limit([at0921], "limitAmount", "revision")),
        _check("ITN_LimitBefore0921", "The same a second earlier: the limit before the cut.",
               limit([before0921], "limitAmount", "revision")),
        _check("ITN_LimitParam0921", "ITN_LimitAt0921 with the instant as a DateTime parameter.",
               limit([Param("asOf")], "limitAmount", "revision"),
               params=[("asOf", "DateTime", "1")], cases_=[("expected_rows", {"asOf": at0921})]),
        _check("ITN_LimitMidnight", "Each check's book limit at the StrictDate 2024-06-04: its "
               "midnight, when BK-CASH-US's is reset.", limit([DateArg("2024-06-04")],
                                                               "limitAmount", "revision")),
        _check("ITN_LimitMidnightDateTime", "ITN_LimitMidnight at the DateTime 2024-06-04T00:00:00.",
               limit([DateTimeArg("2024-06-04 00:00:00")], "limitAmount", "revision")),
        _check("ITN_LimitLatest", "Each check's book limit at `%latest`: the version whose out is "
               "the DateTime infinity date. BK-CASH-EU's was withdrawn, so its checks read none.",
               limit(["latest"], "limitAmount", "revision")),
        _check("ITN_LimitWhenChecked", "The limit in force when each check ran, "
               "`$x.bookLimit($x.checkedAt)`: checks run a second before a change, at it, in a gap, "
               "after a withdrawal, against a book with no limit, no book, and a book that does not "
               "exist.", limit([checked], "limitAmount", "revision", "approvedBy")),
        _check("ITN_UnderLimitWhenChecked", "The checks whose book's limit when they ran was below "
               "4,500,000: a check with no limit then is not below anything.",
               limit([checked], "limitAmount"),
               filters=[Pred([Hop("bookLimit", [checked]), "limitAmount"], "<", 4500000.0)]),
        _check("ITN_LimitVersions", "How many versions each check's book limit has, "
               "`bookLimitAllVersions->count()`, for the books that have one (F6).",
               [("versions", Proj("versions", [Hop("bookLimit", versions="all")], "count"))],
               filters=[Pred(["bookId"], "in", _COUNTED_BOOKS + ["BK-CASH-EU"])]),
        _check("ITN_LimitVersionsInRange", "How many versions of each check's book limit overlap "
               "09:30 on 3 June to midnight: inclusive at the end, so BK-CASH-US's version "
               "recorded at midnight counts. Only the books with one are counted (F6).",
               [("versions", Proj("versions", [Hop("bookLimit", morning, versions="range")],
                                  "count"))],
               filters=[Pred(["bookId"], "in", _COUNTED_BOOKS)]),
        _check("ITN_NoLimitInRange", "Whether each check's book limit has no version overlapping "
               "09:30 on 3 June to midnight: BK-CASH-EU's is recorded on the 5th, and three checks "
               "have no limit at all.",
               [("none", Proj("none", [Hop("bookLimit", morning, versions="range")], "isEmpty"))]),
        _run("ITN_MarkOfficial", "Each run's instrument's 3 June mark as believed at 18:05:00, the "
             "instant AAPL's is made official: `mark(%2024-06-03T18:05:00, %2024-06-03)`.",
             mark([DateTimeArg("2024-06-03 18:05:00"), DateArg("2024-06-03")])),
        _run("ITN_MarkWhenRun", "The mark each run read: for the date it values, as believed when it "
             "ran, `mark($x.runAt, $x.valuationDate)` -- both dates from the row.",
             mark([PathArg(("runAt",)), PathArg(("valuationDate",))])),
        _run("ITN_MarkLatest", "Each run's instrument's current mark as now believed, "
             "`mark(%latest, %latest)`. (Pure takes %latest in a property only when every argument "
             "is %latest.)", mark(["latest", "latest"])),
    ]


def propagation() -> list[Spec]:
    """From a dated book limit to its book's status: `$x.status` takes the limit's date."""
    t0935 = DateTimeArg("2024-06-03 09:35:00")
    t0945 = DateTimeArg("2024-06-03 09:45:30")
    cols = [("bookId", ["bookId"]), ("limitAmount", ["limitAmount"])]
    status = [("state", ["status", "state"]), ("reason", ["status", "reason"])]
    return [
        _spec("ITX_StatusAt0935", "Each book limit at 09:35 with its book's status, which takes the "
              "limit's date: BK-CASH-US is halted then, under its cut limit.", LIMIT, cols + status,
              as_of=t0935),
        _spec("ITX_StatusExplicit0935", "ITX_StatusAt0935 with the status dated explicitly at the "
              "same instant: the same rows.", LIMIT,
              cols + [(a, [Hop("status", [t0935]), p[1]]) for a, p in status], as_of=t0935),
        _spec("ITXP_StatusAt0935", "ITX_StatusAt0935 with the instant as a parameter: the status "
              "takes the bound date.", LIMIT, cols + status, as_of=Param("asOf"),
              params=[("asOf", "DateTime", "1")], cases_=[("expected_rows", {"asOf": t0935})]),
        _spec("ITX_StatusAt0945", "At 09:45:30 the limit is raised and the halt ends, at the same "
              "instant: both navigate to their new versions.", LIMIT, cols + status, as_of=t0945),
        _spec("ITX_StatusOverride", "Each limit at 09:35 with its book's status at 23:30, dated "
              "explicitly: BK-FX is halted for the end-of-day roll then.", LIMIT,
              cols + [(a, [Hop("status", [DateTimeArg("2024-06-03 23:30:00")]), p[1]])
                      for a, p in status], as_of=t0935),
        _spec("ITX_StatusLatest", "Each limit at %latest with its book's status, which takes "
              "%latest too.", LIMIT, cols + status, as_of="latest"),
        _spec("ITX_StatusAllVersions", "Every version of every limit with its book's status at "
              "09:35, dated explicitly: an all-versions root has no date to propagate.", LIMIT,
              cols + [("revision", ["revision"])]
              + [(a, [Hop("status", [t0935]), p[1]]) for a, p in status],
              versions=Versions()),
    ]


def marks() -> list[Spec]:
    """The bitemporal marks: all(processing, business), processing at a time of day."""
    cols = [(p, [p]) for p in MARK_COLS]

    def at(name, p, b, doc, **kw):
        return _spec(name, doc, MARK, cols, as_of=[p, b], **kw)

    d3, d4 = DateArg("2024-06-03"), DateArg("2024-06-04")
    return [
        _empty(at("ITM_BeforeFirst", DateTimeArg("2024-06-03 07:59:59"), d3,
                  "The 3 June marks as believed at 07:59:59, one second before the first is "
                  "recorded: none yet.")),
        at("ITM_Prelim", DateTimeArg("2024-06-03 18:04:59"), d3,
           "The 3 June marks at 18:04:59: AAPL's is still preliminary."),
        at("ITM_Official", DateTimeArg("2024-06-03 18:05:00"), d3,
           "The 3 June marks at 18:05:00, the instant AAPL's is made official."),
        at("ITMP_Official", Param("believedAt"), Param("markDate"),
           "ITM_Official with both dates as parameters, a DateTime and a StrictDate.",
           params=[("believedAt", "DateTime", "1"), ("markDate", "StrictDate", "1")],
           cases_=[("expected_rows", {"believedAt": DateTimeArg("2024-06-03 18:05:00"),
                                      "markDate": d3})]),
        at("ITM_LastSecond", DateTimeArg("2024-06-03 23:59:58"), d3,
           "The 3 June marks at 23:59:58: the gilt's is not recorded until the last second."),
        at("ITM_LastSecondAt", DateTimeArg("2024-06-03 23:59:59"), d3,
           "The 3 June marks at 23:59:59: the gilt's is."),
        at("ITM_Restated", DateTimeArg("2024-06-04 00:00:00"), d3,
           "The 3 June marks at midnight, when the overnight batch restates AAPL's."),
        at("ITM_RestatedStrict", d4, d3,
           "ITM_Restated with the processing date as the StrictDate 2024-06-04: its midnight."),
        at("ITM_OneSecond", DateTimeArg("2024-06-04 17:45:00"), d4,
           "The 4 June marks at 17:45:00: MSFT's preliminary mark, which lives for one second."),
        at("ITM_OneSecondAfter", DateTimeArg("2024-06-04 17:45:01"), d4,
           "The 4 June marks at 17:45:01: MSFT's official one."),
        at("ITM_Latest", "latest", d3, "The 3 June marks as now believed: AAPL's restatement."),
        at("ITM_LatestLatest", "latest", "latest",
           "The current marks as now believed: each instrument's last business date."),
        _spec("ITM_AllVersions", "Every mark ever recorded, for every business date: "
              "`allVersions()` on a bitemporal class.", MARK, cols, versions=Versions()),
    ]


def _empty(s: Spec) -> Spec:
    s.emptiness = emptiness.BEFORE_FIRST_VERSION
    return s


def graphs() -> list[Spec]:
    return [
        _spec("ITG_CheckLimit", "Graph fetch of each check with its book limit at 09:21:00, the "
              "property keyed with its date as the engine serializes it.", CHECK, [],
              graph={"checkId": None,
                     Hop("bookLimit", [DateTimeArg("2024-06-03 09:21:00")]):
                         {"limitAmount": None, "revision": None}}),
        _spec("ITG_LimitStatus", "Graph fetch of the book limits at 09:35 with each book's status, "
              "which takes the limit's date.", LIMIT, [], as_of=DateTimeArg("2024-06-03 09:35:00"),
              graph={"bookId": None, "limitAmount": None, "status": {"state": None}}),
    ]


def build() -> list[Spec]:
    pairs, twins = instants()
    at = [a for a, _t in twins]
    return (at + [b for b, _a in pairs] + [t for _a, t in twins] + dates() + versions()
            + navigation() + propagation() + marks() + graphs())


def invariance_groups(specs: list[Spec]) -> list[list[Spec]]:
    """Each instant as a literal and as a parameter; a StrictDate and a Date parameter beside the
    DateTime they mean; %latest and a far instant; a range as literals and as parameters; a
    propagated date beside the explicit one."""
    by = {s.short: s for s in specs}
    groups = [[by[s.short], by["ITP_" + s.short[4:]]] for s in specs
              if s.short.startswith("ITL_") and "ITP_" + s.short[4:] in by]
    for names in (["ITL_20240603T000000_At", "ITS_20240603"],
                  ["ITL_20240604T000000_At", "ITS_20240604", "ITSP_20240604"],
                  ["ITL_20240603T094530_At", "ITD_20240603T094530"],
                  ["ITL_Latest", "ITL_Year2099"],
                  ["ITR_Morning", "ITRP_Morning"],
                  ["ITN_LimitAt0921", "ITN_LimitParam0921"],
                  ["ITN_LimitMidnight", "ITN_LimitMidnightDateTime"],
                  ["ITX_StatusAt0935", "ITX_StatusExplicit0935", "ITXP_StatusAt0935"],
                  ["ITM_Official", "ITMP_Official"],
                  ["ITM_Restated", "ITM_RestatedStrict"]):
        groups.append([by[n] for n in names])
    return groups


# ------------------------------------------------------------------------------- checks

def _time_of_day(d) -> bool:
    return isinstance(d, DateTimeArg) or (isinstance(d, str) and len(d) > 10 and d != "latest")


def at_time_of_day(spec: Spec) -> bool:
    """Whether the spec reads a milestoned class at a time of day: a DateTime, or a parameter a test
    binds to one, at the root, in a range or on a dated property."""
    bound = [v for _t, ps in spec.cases for v in (ps or {}).values()]
    dates = list(spec.as_of if isinstance(spec.as_of, list) else [spec.as_of])
    if spec.versions is not None:
        dates += [spec.versions.start, spec.versions.end]
    paths = [p.path for p in spec.projections] + [f.path for f in spec.filters
                                                  if isinstance(f, Pred)]
    if spec.graph:
        paths.append(list(spec.graph))
    dates += [d for path in paths for s in path if isinstance(s, Hop) for d in (s.dates or ())]
    return (any(_time_of_day(d) for d in dates)
            or any(isinstance(d, Param) for d in dates) and any(_time_of_day(v) for v in bound)
            or any(isinstance(d, PathArg) for d in dates))


def check(c, specs: list[Spec], tables) -> list[str]:
    """Each instant is a boundary: the reading a second before a change differs from the reading at
    it. Enough services read at a time of day to meet the plan's measure."""
    bad = []
    by = {s.short: s for s in specs}
    for s in specs:
        if s.short.endswith("_Before"):
            at = by[s.short[:-len("_Before")] + "_At"]
            if (oracle.as_json_rows(c, s, oracle.evaluate(c, s, tables))
                    == oracle.as_json_rows(c, at, oracle.evaluate(c, at, tables))):
                bad.append(f"{s.short} and {at.short} read the same rows: the instant is not a "
                           f"boundary (seed.BOOK_LIMIT_PT)")
    timed = sum(1 for s in specs if at_time_of_day(s))
    if timed < 20:
        bad.append(f"only {timed} intraday services read at a time of day; the phase asks for 20")
    return bad
