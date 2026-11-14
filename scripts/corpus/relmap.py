"""
Relation (`~func`) mappings: the services of 99-relmap-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 6).

The corpus had two relation class mappings (modeljoin::JoinMapping), which covered one of EMIT's thirteen relation-
mapping features. These services reach the relmap:: pack (8116-relmap.pure): relation class mappings over the seeded
TRADE and BOOK (stress::TestData) and HIER_ISSUER (hier::IssuerData), so every test reads the corpus's seed.

Expected results come by INVARIANCE, as m2m.py's do: each relation-mapped service (RM_) mirrors a twin (RMR_) over the
same classes mapped relationally over the same tables (relmap::Twins, and hier::IssuerMapping for the binding), which
computes the same rows in SQL -- the source relation's filter, group-by or expression written in the query -- and
each group is in battery.INVARIANCE_GROUPS, so build.py fails if the two ever expect different rows. The oracle
evaluates the twin alone; the window's twin is a relation-API query over the relational class, which the oracle
evaluates through RELATION_IMPL's rank.

  ~src                       relmap::Sourced: the source a call of a typed relation function
  filter                     relmap::Executed: `->filter` in the source relation; TRD-0009's EXECUTE is not kept
  group-by                   relmap::Shaped's RmBookTotal: `->groupBy` in the source relation, as a TDS (the
                             relation-API projection is F88's)
  window function            relmap::Shaped's RmRanked: `->extend(over(...), ~[RANK: ...])`, three tied notionals
  expression right-hand side relmap::Shaped's RmPriced: arithmetic, concatenation, a conditional, a function of $src
  primary key                relmap::Keyed (`~primaryKey: TRADE_ID`) and relmap::KeyedPair (`[BOOK_ID, TRADE_ID]`)
  embedded, inline embedded  relmap::Shaped's RmFill, relmap::Inlined
  union                      relmap::Unioned (two relation sets), relmap::MixedUnion (a relation and a relational set)
  local property             `+bookRef`, which relmap::Crossed's XStore and relmap::Joined's ModelJoin read
  XStore, model join         a deal's book, a book's deals
  binding transformer        relmap::Profiled: hier::Issuer's profile out of its JSON column

Each shape was probed on the pinned release first (8116-relmap.pure's header). Every one runs as a projection and
none as a graph fetch, which the plan's D3 leaves for the bump, so no service fetches a graph.
"""
from __future__ import annotations

from query import And, Apply, Concatenate, Distinct, If, Or, Pred, Proj, Sort, Spec, Window

TWINS = "relmap::Twins", "relmap::TwinsRT"
SOURCED = "relmap::Sourced", "relmap::SourcedRT"
EXECUTED = "relmap::Executed", "relmap::ExecutedRT"
KEYED = "relmap::Keyed", "relmap::KeyedRT"
KEYED_PAIR = "relmap::KeyedPair", "relmap::KeyedPairRT"
UNIONED = "relmap::Unioned", "relmap::UnionedRT"
MIXED = "relmap::MixedUnion", "relmap::MixedUnionRT"
CROSSED = "relmap::Crossed", "relmap::CrossedRT"
JOINED = "relmap::Joined", "relmap::JoinedRT"
SHAPED = "relmap::Shaped", "relmap::ShapedRT"
INLINED = "relmap::Inlined", "relmap::InlinedRT"
PROFILED = "relmap::Profiled", "relmap::ProfiledRT"
ISSUER_TWIN = "hier::IssuerMapping", "relmap::IssuerTwinRT"
CONNECTION = "environment"
ISSUER_CONNECTION, ISSUER_DATA = "issuerEnv", "hier::IssuerData"

# Every relation mapping of the pack: scripts/corpus/model.py skips them, as it skips modeljoin::JoinMapping
RELATION_MAPPINGS = {m for m, _r in (SOURCED, EXECUTED, KEYED, KEYED_PAIR, UNIONED, MIXED, CROSSED, JOINED, SHAPED,
                                    INLINED, PROFILED)}

DEAL, DESK, FILL = "relmap::RmDeal", "relmap::RmDesk", "relmap::RmFill"


def _bind(s: Spec, binding, connection=CONNECTION, data=None) -> Spec:
    s.mapping, s.runtime = binding
    s.connection, s.data_element = connection, data
    return s


def _twin(name: str, doc: str, root: str, cols, filters=(), binding=TWINS, connection=CONNECTION,
          data=None) -> Spec:
    """A twin: the class read relationally from the same table, the expectation its relation-mapped services share."""
    s = Spec(f"stress::RMR_{name}", f"/stress/relmap/twin/{name.lower()}", doc, root)
    s.projections = list(cols)
    s.filters = list(filters)
    return _bind(s, binding, connection, data)


def _rm(name: str, doc: str, root: str, twin: Spec, binding, cols, filters=(), tds=False, connection=CONNECTION,
        data=None) -> Spec:
    """A relation-mapped service: a projection (no graph fetch runs over a relation mapping at the pin), mirroring
    its twin's rows."""
    s = Spec(f"stress::RM_{name}", f"/stress/relmap/{name.lower()}", doc, root)
    s.projections = list(cols)
    s.filters = list(filters)
    s.paradigm = "tds" if tds else "relation"
    s.mirrors = twin
    return _bind(s, binding, connection, data)


def _same(s: Spec, twin: Spec) -> Spec:
    """Sort, limit, grouping and relation operations as the twin's: the rows the two compare are the same rows."""
    s.sort, s.limit, s.group_by, s.aggs = twin.sort, twin.limit, list(twin.group_by), list(twin.aggs)
    s.relation = list(twin.relation)
    return s


def _cols(*names) -> list[Proj]:
    return [Proj(n.split(".")[-1], n.split(".")) for n in names]


_DEAL = ["dealId", "bookId", "qty", "px", "notional", "status", "tradeType", "currency", "tradeDate", "fees"]


# ---------------------------------------------------------------- the deal's own set: ~src, a filter, a primary key

def _deals() -> list[list[Spec]]:
    every = _twin("Deals", "Every deal's columns, read relationally.", DEAL, _cols(*_DEAL))
    groups = [[every,
               _rm("SourcedDeals", "`~src`: the source a call of a typed relation function, every column.", DEAL,
                   every, SOURCED, _cols(*_DEAL)),
               _rm("SourcedDealsTds", "The `~src` set projected as a TDS.", DEAL, every, SOURCED, _cols(*_DEAL),
                   tds=True),
               _rm("KeyedDeals", "`~primaryKey: TRADE_ID`, the single spelling.", DEAL, every, KEYED, _cols(*_DEAL)),
               _rm("KeyedPairDeals", "`~primaryKey: [BOOK_ID, TRADE_ID]`, a composite key.", DEAL, every, KEYED_PAIR,
                   _cols(*_DEAL))]]

    large = [Pred(["notional"], ">", 300000.0)]
    t = _twin("DealsLarge", "The deals whose notional exceeds 300,000.", DEAL, _cols("dealId", "notional", "status"),
              filters=large)
    groups.append([t, _rm("SourcedLarge", "A query filter over the `~src` set.", DEAL, t, SOURCED,
                          _cols("dealId", "notional", "status"), filters=large)])

    t = _twin("DealsTop", "The five largest notionals.", DEAL, _cols("dealId", "notional"))
    t.sort, t.limit = [("notional", True), ("dealId", False)], 5
    groups.append([t, _same(_rm("SourcedTop", "The `~src` set ordered and cut.", DEAL, t, SOURCED,
                                _cols("dealId", "notional")), t)])

    t = _twin("DealBooks", "The books the deals are booked in, each once.", DEAL, _cols("bookId"))
    t.relation = [Distinct(), Sort((("bookId", False),))]
    groups.append([t, _same(_rm("KeyedBooks", "The keyed set's books, made distinct.", DEAL, t, KEYED,
                                _cols("bookId")), t)])

    executed = [Pred(["status"], "==", "EXECUTED")]
    t = _twin("Executed", "The executed deals: TRD-0009's status is written `EXECUTE`.", DEAL,
              _cols("dealId", "bookId", "notional", "status"), filters=executed)
    groups.append([t, _rm("Executed", "A filter in the source relation, `->filter(r | $r.STATUS == 'EXECUTED')`.",
                          DEAL, t, EXECUTED, _cols("dealId", "bookId", "notional", "status"))])
    stacked = [Pred(["notional"], ">", 400000.0)]
    t = _twin("ExecutedLarge", "The executed deals whose notional exceeds 400,000.", DEAL,
              _cols("dealId", "notional"), filters=executed + stacked)
    groups.append([t, _rm("ExecutedLarge", "A query filter stacked on the source relation's.", DEAL, t, EXECUTED,
                          _cols("dealId", "notional"), filters=stacked)])
    t = _twin("ExecutedByBook", "The executed deals counted and their notional summed per book.", DEAL,
              _cols("dealId", "bookId", "notional"), filters=executed)
    t.group_by, t.aggs = ["bookId"], [("deals", "dealId", "count"), ("total", "notional", "sum")]
    groups.append([t, _same(_rm("ExecutedByBook", "The filtered source grouped in the query.", DEAL, t, EXECUTED,
                                _cols("dealId", "bookId", "notional")), t)])
    return groups


# ---------------------------------------------------------------- the source relation's group-by and window

def _shapes() -> list[list[Spec]]:
    totals = _twin("BookTotals", "Each book's total notional and its deals, grouped in the query.", DEAL,
                   _cols("dealId", "bookId", "notional"))
    totals.group_by = ["bookId"]
    totals.aggs = [("total", "notional", "sum"), ("dealCount", "dealId", "count")]
    book_total = "relmap::RmBookTotal"
    groups = [[totals,
               _rm("BookTotals", "A group-by in the source relation, a row per book, projected as a TDS.", book_total,
                   totals, SHAPED, _cols("bookId", "total", "dealCount"), tds=True),
               _rm("BookTotalsRelation", "The same projected through the relation API: DuckDB refuses the SQL the "
                                         "engine writes (F88).", book_total, totals, SHAPED,
                   _cols("bookId", "total", "dealCount"))]]
    over_a_million = [Pred(["total"], ">", 1000000.0)]
    t = _twin("BookTotalsLarge", "The books whose deals total more than a million.", DEAL,
              _cols("dealId", "bookId", "notional"))
    t.group_by, t.aggs = list(totals.group_by), list(totals.aggs)
    t.post_filters = list(over_a_million)
    groups.append([t, _rm("BookTotalsLarge", "A query filter on the group-by's aggregate, as a TDS.", book_total, t,
                          SHAPED, _cols("bookId", "total", "dealCount"), filters=over_a_million, tds=True)])

    rank = Window("rank", "rank", ("bookId",), (("notional", True),))
    ranked = "relmap::RmRanked"
    t = _twin("Ranked", "Each deal's rank by notional within its book, largest first: three of BK-CASH-US's deals tie "
                        "at 250,000.", DEAL, _cols("dealId", "bookId", "notional"))
    t.relation = [rank]
    groups.append([t, _rm("Ranked", "A window in the source relation, `rank` over each book's notionals.", ranked,
                          t, SHAPED, _cols("dealId", "bookId", "notional", "rank"))])
    cash = [Pred(["bookId"], "==", "BK-CASH-US")]
    t = _twin("RankedCash", "BK-CASH-US's deals ranked: a filter on the partition key keeps every rank.", DEAL,
              _cols("dealId", "bookId", "notional"), filters=cash)
    t.relation = [rank]
    groups.append([t, _rm("RankedCash", "A query filter on the window's partition key.", ranked, t, SHAPED,
                          _cols("dealId", "bookId", "notional", "rank"), filters=cash)])
    return groups


# ---------------------------------------------------------------- expression right-hand sides

_LOWER = Apply((("toLower", ()),), "String")
_BAND = If(">", 300000.0, "large", "small", "String")
_PRICED = [Proj("dealId", ["dealId"]), Proj("doubled", ["notional"], apply=Apply((("*", (2.0,)),), "Float")),
           Proj("ref", ["ref"]), Proj("band", ["notional"], apply=_BAND),
           Proj("statusLower", ["status"], apply=_LOWER)]


def _expressions() -> list[list[Spec]]:
    priced = "relmap::RmPriced"
    t = _twin("Priced", "The deal's notional doubled, its derived reference, its band and its status lower-cased, "
                        "computed in the query.", DEAL, _PRICED)
    groups = [[t, _rm("Priced", "Expression right-hand sides over `$src`: arithmetic, concatenation, a conditional "
                                "and a function.", priced, t, SHAPED,
                      _cols("dealId", "doubled", "ref", "band", "statusLower"))]]
    t = _twin("PricedLarge", "The deals in the large band.", DEAL,
              [Proj("dealId", ["dealId"]), Proj("band", ["notional"], apply=_BAND)],
              filters=[Pred(["notional"], "==", "large", apply=_BAND)])
    groups.append([t, _rm("PricedLarge", "A query filter on a conditional right-hand side.", priced, t, SHAPED,
                          _cols("dealId", "band"), filters=[Pred(["band"], "==", "large")])])
    t = _twin("PricedExecuted", "The deals whose status, lower-cased, is `executed`.", DEAL,
              [Proj("dealId", ["dealId"]), Proj("doubled", ["notional"], apply=Apply((("*", (2.0,)),), "Float"))],
              filters=[Pred(["status"], "==", "executed", apply=_LOWER)])
    groups.append([t, _rm("PricedExecuted", "A query filter on a function's right-hand side.", priced, t, SHAPED,
                          _cols("dealId", "doubled"), filters=[Pred(["statusLower"], "==", "executed")])])
    t = _twin("PricedTop", "The three largest doubled notionals.", DEAL,
              [Proj("dealId", ["dealId"]), Proj("doubled", ["notional"], apply=Apply((("*", (2.0,)),), "Float"))])
    t.sort, t.limit = [("doubled", True), ("dealId", False)], 3
    groups.append([t, _same(_rm("PricedTop", "Ordered on an arithmetic right-hand side and cut.", priced, t, SHAPED,
                                _cols("dealId", "doubled")), t)])
    return groups


# ---------------------------------------------------------------- embedded and inline embedded

def _embedded() -> list[list[Spec]]:
    cols = _cols("dealId", "terms.qty", "terms.px", "terms.currency")
    t = _twin("Fill", "Each deal's terms, an embedded property read relationally.", FILL, cols)
    groups = [[t,
               _rm("Fill", "An embedded property mapping, `terms ( qty: QUANTITY, ... )`, over the relation.", FILL,
                   t, SHAPED, cols),
               _rm("Inline", "An inline embedded property mapping, `terms () Inline [inTerms]`, its set over the same "
                             "relation.", FILL, t, INLINED, cols)]]
    big = [Pred(["terms", "qty"], ">", 2000.0)]
    t = _twin("FillLarge", "The deals of more than 2,000 units.", FILL, _cols("dealId", "terms.qty"), filters=big)
    groups.append([t,
                   _rm("FillLarge", "A query filter through the embedded property.", FILL, t, SHAPED,
                       _cols("dealId", "terms.qty"), filters=big),
                   _rm("InlineLarge", "A query filter through the inline embedded property.", FILL, t, INLINED,
                       _cols("dealId", "terms.qty"), filters=big)])
    t = _twin("FillTop", "The four largest quantities.", FILL, _cols("dealId", "terms.qty"))
    t.sort, t.limit = [("qty", True), ("dealId", False)], 4
    groups.append([t, _same(_rm("FillTop", "Ordered on an embedded property and cut.", FILL, t, SHAPED,
                                _cols("dealId", "terms.qty")), t)])
    return groups


# ---------------------------------------------------------------- unions

def _unions() -> list[list[Spec]]:
    cols = _cols("dealId", "bookId", "notional", "status", "tradeType")
    either = Or([Pred(["tradeType"], "==", "PRINCIPAL"), Pred(["bookId"], "==", "BK-VOL-01")])
    t = _twin("Union", "The principal deals and BK-VOL-01's, which no principal deal is in.", DEAL, cols,
              filters=[either])
    groups = [[t,
               _rm("Union", "A union of two relation sets, each filtered in its source relation.", DEAL, t, UNIONED,
                   cols),
               _rm("UnionTds", "The union projected as a TDS.", DEAL, t, UNIONED, cols, tds=True)]]
    executed = [Pred(["status"], "==", "EXECUTED")]
    t = _twin("UnionExecuted", "The union's executed deals.", DEAL, _cols("dealId", "status"),
              filters=[And([either] + executed)])
    groups.append([t, _rm("UnionExecuted", "A query filter over the union, applied to each set.", DEAL, t, UNIONED,
                          _cols("dealId", "status"), filters=executed)])

    brief = _cols("dealId", "notional", "status")
    every = _twin("MixedUnionAll", "", DEAL, brief)
    t = _twin("MixedUnion", "The principal deals, then every deal again: a union keeps both.", DEAL, brief,
              filters=[Pred(["tradeType"], "==", "PRINCIPAL")])
    t.relation = [Concatenate(every)]
    groups.append([t, _rm("MixedUnion", "A union of a relation set and a relational one over the same table: the "
                                        "principal deals twice.", DEAL, t, MIXED, brief)])
    return groups


# ---------------------------------------------------------------- a local property, XStore and ModelJoin

def _associations() -> list[list[Spec]]:
    cols = [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"]), Proj("deskCurrency", ["desk", "currency"])]
    t = _twin("DealDesk", "Each deal with its book's name and currency, over the store's Trade_Book join.", DEAL,
              cols)
    groups = [[t,
               _rm("CrossedDealDesk", "An XStore association over a local property, `$this.bookRef == "
                                      "$that.bookId`.", DEAL, t, CROSSED, cols),
               _rm("JoinedDealDesk", "A ModelJoin over the same local property.", DEAL, t, JOINED, cols)]]
    eur = [Pred(["desk", "currency"], "==", "EUR")]
    t = _twin("EurDeals", "The deals booked in a EUR book.", DEAL, _cols("dealId", "notional"), filters=eur)
    groups.append([t,
                   _rm("CrossedEurDeals", "A query filter through the XStore association.", DEAL, t, CROSSED,
                       _cols("dealId", "notional"), filters=eur),
                   _rm("JoinedEurDeals", "A query filter through the ModelJoin.", DEAL, t, JOINED,
                       _cols("dealId", "notional"), filters=eur)])
    active = [Proj("bookId", ["bookId"]), Proj("name", ["name"]), Proj("hasDeals", ["deals"], agg="isNotEmpty")]
    t = _twin("DeskActivity", "Whether each book has deals: six of nine have none.", DESK, active)
    groups.append([t,
                   _rm("CrossedDeskActivity", "The XStore association's to-many end, from the book.", DESK, t,
                       CROSSED, active),
                   _rm("JoinedDeskActivity", "The ModelJoin's to-many end, from the book.", DESK, t, JOINED,
                       active)])
    t = _twin("DealDeskTop", "The first five deals by book name, then id.", DEAL,
              [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])])
    t.sort, t.limit = [("deskName", False), ("dealId", False)], 5
    groups.append([t, _same(_rm("CrossedDealDeskTop", "Ordered on a property across the XStore association and cut.",
                                DEAL, t, CROSSED, [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])]),
                            t)])
    return groups


# ---------------------------------------------------------------- a binding transformer

def _binding() -> list[list[Spec]]:
    issuer = "hier::Issuer"
    cols = [Proj("issuerId", ["issuerId"]), Proj("flagged", ["profile", "flagged"])]
    t = _twin("IssuerFlags", "Each issuer's profile flag through hier::IssuerMapping's Binding transformer; HI-0005 "
                             "has no profile.", issuer, cols, binding=ISSUER_TWIN, connection=ISSUER_CONNECTION,
              data=ISSUER_DATA)
    groups = [[t, _rm("IssuerFlags", "A binding transformer in a relation mapping, `profile: Binding "
                                     "hier::ProfileBinding: PROFILE_JSON`: the Boolean leaf, which the engine reads "
                                     "back as it is (F27 turns the others into their JSON tokens).", issuer, t,
                      PROFILED, cols, connection=ISSUER_CONNECTION, data=ISSUER_DATA)]]
    flagged = [Pred(["profile", "flagged"], "==", True)]
    t = _twin("IssuersFlagged", "The issuers whose profile is flagged.", issuer, _cols("issuerId"), filters=flagged,
              binding=ISSUER_TWIN, connection=ISSUER_CONNECTION, data=ISSUER_DATA)
    groups.append([t, _rm("IssuersFlagged", "A query filter on a property read through the binding transformer.",
                          issuer, t, PROFILED, _cols("issuerId"), filters=flagged, connection=ISSUER_CONNECTION,
                          data=ISSUER_DATA)])
    return groups


INVARIANCE_GROUPS = _deals() + _shapes() + _expressions() + _embedded() + _unions() + _associations() + _binding()
# The concatenated side of the mixed union's twin is written into the twin's query, not a service of its own
SPECS = [s for g in INVARIANCE_GROUPS for s in g]
NAMES = {s.name for s in SPECS}


def check(c, tables) -> list[str]:
    """Every twin has rows, so no pair agrees on nothing, and every relation-mapped service mirrors a twin."""
    import oracle

    problems = [f"{s.short}: no rows" for s in SPECS
                if s.short.startswith("RMR_") and not oracle.evaluate(c, s, tables)]
    problems += [f"{s.short}: a relation-mapped service with no twin" for s in SPECS
                 if s.short.startswith("RM_") and getattr(s, "mirrors", None) is None]
    return problems
