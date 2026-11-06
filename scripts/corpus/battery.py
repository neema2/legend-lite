"""
The fan-out battery — the half of L0 the original 12 services could not reach.

All 530 projection paths in the 12 stress services are to-one navigations. That was
measured, not assumed (`python3 query.py` reports "0 cross a to-many hop"). It means the
corpus exercised outer joins that land on at most one row and never one that fans out —
and fan-out is where the interesting defects live.

Specifically, `count()` over a to-many association compiles to an aggregate over a LEFT
OUTER JOIN. An entity with no children still contributes one all-NULL joined row, so an
implementation that emits `COUNT(*)` rather than `COUNT(child_key)` returns **1** where
the answer is **0**. It is a silent wrong answer: no error, no empty result, just a count
that is too high by one for exactly the rows a naive fixture would not contain.

The seed was built with that in mind. Every root below has at least one childless member:

  INST-NESN    listed, never traded, no positions, no greeks
  CP-0005      onboarded, never traded with, no settlements, no CSA
  BK-LEGACY    closed book, no trades, no PnL
  DSK-CREDIT   no books, no traders, no PnL
  SEC-55       no instruments
  POS-0003/4/6 no greeks

Each service pairs identifying columns with several `->count()` projections, so one row
carries several independent assertions and a defect in any one of them reddens the suite.
"""
from __future__ import annotations

import emptiness
import m2m
import relmap
from query import DateArg, Pred, Proj, Spec

PREFIX = "stress::F"


def _spec(n: int, name: str, root: str, doc: str, ids: list[tuple[str, str]],
          counts: list[tuple[str, str]], calls: list[tuple[str, str, list]] = ()) -> Spec:
    s = Spec(f"{PREFIX}{n}_{name}", f"/stress/f{n}", doc, root)
    s.projections = ([Proj(a, p.split(".")) for a, p in ids]
                     + [Proj(a, p.split("."), "count") for a, p in counts]
                     + [Proj(a, p.split("."), None, list(args)) for a, p, args in calls])
    return s


# ---------------------------------------------------------------- L2 invariance
#
# The SAME question, asked of the canonical model (which joins four ways) and of the
# reporting model (which reads one denormalized table). The projection aliases are
# identical by construction, so the two expectations must be equal cell for cell —
# build.py asserts that before either is emitted, and a mismatch fails the build rather
# than shipping two tests that quietly disagree.
#
# This is the strongest assertion in the corpus because it needs no oracle to be
# meaningful: two mappings of the same facts must agree, so a disagreement is the
# engine's. The oracle is still used, which makes it stronger again — it pins the VALUE
# as well as the agreement.

_INVARIANT_COLUMNS = [
    ("tradeId", "tradeId", "tradeId"),
    ("tradeDate", "tradeDate", "tradeDate"),
    ("quantity", "quantity", "quantity"),
    ("price", "price", "price"),
    ("notional", "notional", "notional"),
    ("side", "side", "side"),
    ("status", "status", "status"),
    ("currency", "currency", "currency"),
    ("grossAmount", "grossAmount", "grossAmount"),
    # The four navigations that the flat shape has already resolved. TRD-0007's
    # counterparty is an orphan and two trades have no trader, so these carry the NULLs
    # that make the comparison worth doing.
    ("instrName", "instrument.name", "instrName"),
    ("instrTicker", "instrument.ticker", "instrTicker"),
    ("instrIsin", "instrument.isin", "instrIsin"),
    ("bookName", "book.name", "bookName"),
    ("cptyName", "counterparty.legalName", "cptyName"),
    ("cptyLei", "counterparty.lei", "cptyLei"),
    ("traderLast", "trader.lastName", "traderLast"),
]


def _invariance_pair():
    canonical = Spec("stress::N0_TradeCanonical", "/stress/n0",
                     "Mapping invariance, canonical side: reached by joining "
                     "trading::Trade to instrument, book, counterparty and trader.",
                     "trading::Trade")
    canonical.projections = [Proj(a, p.split(".")) for a, p, _ in _INVARIANT_COLUMNS]

    flat = Spec("stress::N1_TradeFlat", "/stress/n1",
                "Mapping invariance, reporting side: the identical question answered from "
                "one denormalized table through reporting::FlatMapping. Must return "
                "exactly the rows N0 returns.",
                "reporting::FlatTrade")
    flat.projections = [Proj(a, f.split(".")) for a, _, f in _INVARIANT_COLUMNS]
    flat.mapping, flat.runtime = "reporting::FlatMapping", "stress::FlatRT"
    return canonical, flat


def _embedded_variant(canonical: Spec) -> Spec:
    """The strongest form: the IDENTICAL query — same root class, same property paths,
    same generated text — resolved by a mapping that reads one denormalized table through
    embedded property mappings instead of joining four ways.

    Its expectation needs no separate derivation. oracle.py deliberately does not model
    reporting::EmbeddedFlatMapping, so evaluating this spec walks the canonical mapping
    and produces the canonical answer — which is exactly the claim under test.
    """
    s = Spec("stress::N2_TradeEmbeddedFlat", "/stress/n2",
             "Mapping invariance, embedded side: byte-identical query to N0, resolved "
             "against TRADE_FLAT through embedded property mappings. No join is emitted "
             "at all, and the answer must not change.",
             canonical.root)
    s.projections = list(canonical.projections)
    s.mapping, s.runtime = "reporting::EmbeddedFlatMapping", "stress::EmbeddedFlatRT"
    return s


CANONICAL, FLAT = _invariance_pair()
EMBEDDED = _embedded_variant(CANONICAL)


def _inline_variant(canonical: Spec) -> Spec:
    """The same query again, resolved by a mapping that reuses a ROOT set implementation
    for the counterparty via `counterparty () Inline[cptyInline]` instead of repeating its
    column bindings inline. Same answer or the reuse is not equivalent."""
    s = Spec("stress::N3_TradeInlineFlat", "/stress/n3",
             "Mapping invariance, inline side: identical query to N0 and N2, resolved by "
             "a mapping whose embedded counterparty is `Inline[cptyInline]` -- an empty "
             "property block delegating to a root set implementation over the same table.",
             canonical.root)
    s.projections = list(canonical.projections)
    s.mapping, s.runtime = "reporting::InlineFlatMapping", "stress::InlineFlatRT"
    return s


INLINE = _inline_variant(CANONICAL)


def _multi_variant(canonical: Spec) -> Spec:
    """Mapping invariance expressed INSIDE one service, via MultiExecution.

    Stronger than a pair of services: there is literally one query text, so the two runs
    cannot drift apart by someone editing one and not the other. Both tests assert the
    SAME expectation and differ only by `keys:`.
    """
    s = Spec("stress::N4_TradeMultiExecution", "/stress/n4",
             "MultiExecution: ONE query, run against the canonical mapping and the "
             "embedded flat mapping under two keys. Both must return the same rows -- "
             "the L2 invariance, asserted without a second query to keep in sync.",
             canonical.root)
    s.projections = list(canonical.projections)
    s.multi = [("canonical", "stress::AllMapping", "stress::RT"),
               ("flat", "reporting::EmbeddedFlatMapping", "stress::EmbeddedFlatRT")]
    s.multi_key = "shape"
    return s


MULTI = _multi_variant(CANONICAL)


# ------------------------------------------------------------ L4 union invariance
#
# Orthogonal to the L2 group. L2 varies how a row is ASSEMBLED (join vs denormalized);
# this varies where rows COME FROM (one table vs a union of three, one of them empty).
# The projection is restricted to the columns the partitions carry.

_UNION_COLUMNS = ["tradeId", "tradeDate", "quantity", "price", "notional", "side",
                  "status", "currency"]


def _union_pair():
    whole = Spec("stress::U0_TradeWhole", "/stress/u0",
                 "Union invariance, whole side: every trade from the single TRADE table.",
                 "trading::Trade")
    whole.projections = [Proj(a, [a]) for a in _UNION_COLUMNS]

    parts = Spec("stress::U1_TradePartitioned", "/stress/u1",
                 "Union invariance, partitioned side: the same trades reached through an "
                 "Operation union over TRADE_EQ, TRADE_RATES and the EMPTY TRADE_FX. A "
                 "union leg contributing no rows must add nothing and remove nothing.",
                 "trading::HistTrade")
    parts.projections = [Proj(a, [a]) for a in _UNION_COLUMNS]
    return whole, parts


WHOLE, PARTITIONED = _union_pair()


# ------------------------------------------------------ L4 filter invariance
#
# The predicate applied in the MAPPING versus the same predicate applied in the QUERY.
# Independent of the other two groups, and the cheapest kind of invariance to get wrong:
# a store filter that silently failed to apply would make E1 a superset of E0 and nothing
# else in the corpus would notice.

def _filter_pair():
    in_query = Spec("stress::E0_ExecutedByQuery", "/stress/e0",
                    "Filter invariance, query side: every trade, narrowed by a predicate "
                    "in the query.", "trading::Trade")
    in_query.projections = [Proj(a, [a]) for a in _UNION_COLUMNS]
    in_query.filters = [Pred(["status"], "==", "EXECUTED")]

    in_mapping = Spec("stress::E1_ExecutedByMapping", "/stress/e1",
                      "Filter invariance, mapping side: the SAME rows reached through a "
                      "class whose mapping carries ~filter [store::DB] ExecutedTrades. "
                      "The query has no predicate at all.", "trading::ExecutedTrade")
    in_mapping.projections = [Proj(a, [a]) for a in _UNION_COLUMNS]
    in_mapping.mapping, in_mapping.runtime = "trading::ExecutedMapping", "stress::ExecutedRT"
    return in_query, in_mapping


BY_QUERY, BY_MAPPING = _filter_pair()

# Each group must agree internally; groups are independent of one another.
# ------------------------------------------------------------- L5 M2M invariance
#
# The fourth invariance, and the one that crosses an execution ENGINE rather than a
# mapping shape: M0 reads trading::Trade through SQL, M1 reads canonical::CanonicalTrade
# through the M2M engine fed by that same SQL. The properties are renamed and one is
# computed in the M2M layer, so a mapping that quietly passed the source object through
# would not satisfy the projection.

# `side` is deliberately NOT here. An EnumerationMapping is not applied when the
# relational mapping feeds a ModelChainConnection -- the raw storage code arrives instead
# of the enum value (F12) -- and including it would redden this pair for a reason that has
# nothing to do with the invariance being tested. It is asserted separately by M2.
_M2M_COLUMNS = [
    ("identifier", "tradeId", "identifier"),
    ("executedOn", "tradeDate", "executedOn"),
    ("unitPrice", "price", "unitPrice"),
    ("units", "quantity", "units"),
    ("state", "status", "state"),
    ("grossValue", "grossAmount", "grossValue"),
]


def _m2m_pair():
    src = Spec("stress::M0_TradeRelational", "/stress/m0",
               "M2M invariance, source side: trading::Trade straight from SQL. "
               "grossAmount is the derived property; the M2M side computes the same "
               "product in the M2M engine instead.", "trading::Trade")
    src.projections = [Proj(a, s.split(".")) for a, s, _ in _M2M_COLUMNS]

    tgt = Spec("stress::M1_TradeCanonical", "/stress/m1",
               "M2M invariance, canonical side: the same facts through "
               "relational -> source model -> M2M -> canonical model, with every property "
               "renamed on the way.", "canonical::CanonicalTrade")
    tgt.projections = [Proj(a, t.split(".")) for a, _, t in _M2M_COLUMNS]
    tgt.mapping, tgt.runtime = "canonical::M2MMapping", "stress::CanonicalRT"
    tgt.connection = "environment"
    # The oracle cannot evaluate an M2M target: it has no table to read. The claim IS
    # that it returns what the relational side returns, so it mirrors it.
    tgt.mirrors = src
    # A Relation projection is REJECTED over a ModelChainConnection, so the canonical side
    # must use the legacy TDS form. That is F11, and it is why this pair also happens to
    # be the corpus's only coverage of the legacy paradigm end-to-end.
    tgt.paradigm = "tds"
    return src, tgt


RELATIONAL_SIDE, CANONICAL_SIDE = _m2m_pair()


# ---------------------------------------------------- L5 Otherwise / fallback
#
# One class, one table, two mappings differing ONLY in whether the embedded counterparty
# falls back to a join. Five of twenty trades have the cache populated; the other fifteen
# have only the FK.
#
#   O0  embedded only  -> counterparty present for 5, NULL for 15
#   O1  Otherwise      -> counterparty present for 19, NULL only for the one whose
#                         counterparty genuinely does not exist
#
# O1 mirrors the canonical answer, so it is a fifth invariance. O0 is asserted separately
# and MUST differ -- if the two agreed, Otherwise would be doing nothing and the test
# would be passing for the wrong reason.

_OTHERWISE_COLUMNS = [("tradeId", "tradeId"), ("notional", "notional"),
                      ("status", "status"),
                      ("cptyId", "counterparty.counterpartyId"),
                      ("cptyName", "counterparty.legalName"),
                      ("cptyLei", "counterparty.lei")]


def _otherwise_specs():
    canonical = Spec("stress::_O1MirrorSource", "/stress/_o1mirror", "", "trading::Trade")
    canonical.projections = [Proj(a, p.split(".")) for a, p in _OTHERWISE_COLUMNS]

    fallback = Spec("stress::O1_CounterpartyOtherwise", "/stress/o1",
                    "Embedded counterparty with an Otherwise fallback. Only 5 of 20 rows "
                    "carry the inline cache; the rest resolve through the join, and the "
                    "result must equal what the canonical model returns.",
                    "trading::Trade")
    fallback.projections = [Proj(a, p.split(".")) for a, p in _OTHERWISE_COLUMNS]
    fallback.mapping, fallback.runtime = "reporting::OtherwiseMapping", "stress::OtherwiseRT"
    fallback.mirrors = canonical
    return fallback


OTHERWISE = _otherwise_specs()


def _confluence_spec():
    """Otherwise AND scope on one class mapping -- see 82-confluence.pure.

    The two constructs had never executed together, because each lived alone in its own
    mapping and 148 of the stacking scoreboard's pairs are exactly that situation: two
    mapping-level constructs whose mappings never meet. Closing one of them needs a model
    change, not a query, which is what the confluence file is.

    Mirrors the canonical Trade projection for the same reason O1 does: the claim is that a
    trade read through a scope block with an Otherwise fallback equals the trade read
    plainly, and deriving a second expectation would test the deriving rather than the claim.
    """
    canonical = Spec("stress::_ConfMirrorSource", "/stress/_confmirror", "", "trading::Trade")
    canonical.projections = [Proj(a, p.split(".")) for a, p in _OTHERWISE_COLUMNS]

    s = Spec("stress::CF_Confluence", "/stress/cf",
             "Otherwise and scope on one class mapping: the embedded counterparty resolves "
             "through its fallback join while notional and status come through a scope "
             "block. Neither construct is new; their meeting is.",
             "conf::ConfTrade")
    s.projections = [Proj(a, p.split(".")) for a, p in _OTHERWISE_COLUMNS]
    s.mapping, s.runtime = "conf::Confluence", "conf::ConfluenceRT"
    s.mirrors = canonical
    return s


CONFLUENCE = _confluence_spec()


def _fixed_income_specs():
    """Queries a fixed-income desk actually runs, over the coupon schedule.

    Written by hand rather than generated because the point is that they are the REAL
    questions: what is scheduled, what has been paid, what is still owed, and which
    securities have holes in their payment history. A generated projection of five columns
    off DebtSecurity would exercise the same joins and ask nothing.
    """
    out = []

    # The securities master, with the derived annual coupon a desk quotes rather than the
    # rate, and the same figure converted at a supplied rate. The perpetual's maturityDate
    # is null and has to survive the projection.
    terms = Spec("stress::FI0_SecurityTerms", "/stress/fi0",
                 "Debt securities with their terms, the derived annual coupon, and that "
                 "coupon converted at a caller-supplied FX rate. One security is perpetual, "
                 "so maturityDate is null for it.",
                 "fixedincome::DebtSecurity")
    terms.projections = [Proj("securityId", ["securityId"]),
                         Proj("description", ["description"]),
                         Proj("issuerName", ["issuerName"]),
                         Proj("currency", ["currency"]),
                         Proj("couponRate", ["couponRate"]),
                         Proj("maturityDate", ["maturityDate"]),
                         Proj("annualCoupon", ["annualCoupon"]),
                         Proj("annualCouponGbp", ["annualCouponIn"], args=[0.79])]
    terms.sort = ("securityId", False)
    out.append(terms)

    # The schedule itself, deepest-fanning join in the corpus: 130 periods over 6 securities.
    # Sorted by security then period so the order is the schedule's own.
    sched = Spec("stress::FI1_CouponSchedule", "/stress/fi1",
                 "The accrual schedule: every coupon period with its accrual window, day "
                 "count and amount, and the payment against it where one exists. 130 rows "
                 "over 6 securities -- the deepest fan-out in the corpus -- and 87 of them "
                 "have no payment because the coupon is not due yet.",
                 "fixedincome::CouponPeriod")
    sched.projections = [Proj("periodId", ["periodId"]),
                         Proj("securityId", ["securityId"]),
                         Proj("periodNumber", ["periodNumber"]),
                         Proj("accrualStart", ["accrualStartDate"]),
                         Proj("accrualEnd", ["accrualEndDate"]),
                         Proj("accrualDays", ["accrualDays"]),
                         Proj("couponAmount", ["couponAmount"]),
                         Proj("dailyAccrual", ["dailyAccrual"]),
                         Proj("netPaid", ["payment", "netAmount"])]
    sched.sort = ("periodId", False)
    out.append(sched)

    # What a coupon actually pays after withholding, by issuer domicile. The German and
    # British issuers withhold; the US ones do not, so gross and net differ on some rows and
    # not others -- which is the case a `gross - tax = net` assertion has to see both of.
    paid = Spec("stress::FI2_CouponPayments", "/stress/fi2",
                "Coupons actually paid, with withholding. Withholding is a function of the "
                "issuer's domicile, so gross and net differ for the German and British "
                "issuers and agree for the US ones.",
                "fixedincome::CouponPayment")
    paid.projections = [Proj("paymentId", ["paymentId"]),
                        Proj("paymentDate", ["paymentDate"]),
                        Proj("grossAmount", ["grossAmount"]),
                        Proj("withholdingTax", ["withholdingTax"]),
                        Proj("netAmount", ["netAmount"]),
                        Proj("currency", ["currency"]),
                        Proj("issuer", ["period", "security", "issuerName"])]
    paid.sort = ("paymentId", False)
    out.append(paid)

    # Per-security totals. groupBy over a 60-deep fan-out, which is where an aggregate that
    # double-counts a join shows up and a 3-row fan-out hides it.
    totals = Spec("stress::FI3_ScheduleTotals", "/stress/fi3",
                  "Scheduled coupon totals per security: how many periods, how much is "
                  "scheduled in total. Aggregated over a fan-out sixty deep at its widest, "
                  "which is where an aggregate that double-counts its join stops hiding.",
                  "fixedincome::CouponPeriod")
    totals.projections = [Proj("securityId", ["securityId"]),
                          Proj("couponAmount", ["couponAmount"])]
    totals.group_by = ["securityId"]
    totals.aggs = [("periodCount", "couponAmount", "count"),
                   ("totalScheduled", "couponAmount", "sum"),
                   ("largestCoupon", "couponAmount", "max")]
    totals.sort = ("securityId", False)
    out.append(totals)

    # Which periods have no payment. isEmpty over a zero-or-one end, where the empty half is
    # the majority rather than an edge case.
    unpaid = Spec("stress::FI4_UnpaidPeriods", "/stress/fi4",
                  "Coupon periods with no payment against them. The empty half of this "
                  "association is 87 of 130 rows, so emptiness is the common case here "
                  "rather than the exception it is everywhere else in the corpus.",
                  "fixedincome::CouponPeriod")
    unpaid.projections = [Proj("periodId", ["periodId"]),
                          Proj("paymentDate", ["paymentDate"]),
                          Proj("isPaid", ["isPaid"]),
                          Proj("awaitingPayment", ["payment"], agg="isEmpty")]
    unpaid.sort = ("periodId", False)
    out.append(unpaid)
    return out


FIXED_INCOME = _fixed_income_specs()


def _otc_specs():
    """The questions a derivatives middle office actually asks.

    Not projections chosen to exercise a join -- the trade book, the leg detail, the risk
    roll-up by asset class, and the compliance query that looks for uncollateralised trades
    with no master agreement. Each is a real report, and each happens to stack several
    constructs because real reports do.
    """
    out = []

    book = Spec("stress::OT0_TradeBook", "/stress/ot0",
                "The OTC book: every trade with its tenor and its notional converted at a "
                "supplied rate. clearingHouse and masterAgreement are null for the "
                "bilateral trades, which is most of them.",
                "derivatives::OtcTrade")
    book.projections = [Proj("otcId", ["otcId"]),
                        Proj("productType", ["productType"]),
                        Proj("assetClass", ["assetClass"]),
                        Proj("notional", ["notional"]),
                        Proj("currency", ["currency"]),
                        Proj("clearingHouse", ["clearingHouse"]),
                        Proj("masterAgreement", ["masterAgreement"]),
                        Proj("tenorYears", ["tenorYears"]),
                        Proj("notionalGbp", ["notionalIn"], args=[0.79])]
    book.sort = ("otcId", False)
    out.append(book)

    # Leg detail, navigating back up to the parent trade. A basis swap has a floating leg on
    # both sides, so fixedRate is null on both -- the row that breaks a "one leg is fixed"
    # assumption.
    legs = Spec("stress::OT1_SwapLegs", "/stress/ot1",
                "Swap legs with their parent's product type. Fixed legs carry a rate and "
                "no index; floating legs the reverse; a basis swap has floating on BOTH "
                "sides, so neither of its legs has a fixed rate.",
                "derivatives::SwapLeg")
    legs.projections = [Proj("legId", ["legId"]),
                        Proj("legNumber", ["legNumber"]),
                        Proj("legType", ["legType"]),
                        Proj("payReceive", ["payReceive"]),
                        Proj("fixedRate", ["fixedRate"]),
                        Proj("floatingIndex", ["floatingIndex"]),
                        Proj("spreadBp", ["spreadBp"]),
                        Proj("dayCount", ["dayCount"]),
                        Proj("productType", ["trade", "productType"]),
                        Proj("counterpartyId", ["trade", "counterpartyId"])]
    legs.sort = ("legId", False)
    out.append(legs)

    # Risk roll-up. Notional by asset class is the first slide of every derivatives risk
    # pack, and it aggregates over a discriminated taxonomy rather than a flat table.
    risk = Spec("stress::OT2_NotionalByAssetClass", "/stress/ot2",
                "Notional by asset class and clearing status: the roll-up a derivatives "
                "risk pack opens with.",
                "derivatives::OtcTrade")
    risk.projections = [Proj("assetClass", ["assetClass"]),
                        Proj("clearingStatus", ["clearingStatus"]),
                        Proj("notional", ["notional"])]
    risk.group_by = ["assetClass", "clearingStatus"]
    risk.aggs = [("tradeCount", "notional", "count"),
                 ("totalNotional", "notional", "sum"),
                 ("largestTrade", "notional", "max")]
    risk.sort = ("assetClass", False)
    out.append(risk)

    # The subtype query. This is what the thirteen discriminator filters are FOR: asking for
    # swaps and getting swaps. If a ~filter is wrong the set returns the whole table, and the
    # only way to notice is to ask a subclass for its own rows and count them.
    irs = Spec("stress::OT3_InterestRateSwaps", "/stress/ot3",
               "Interest rate swaps only, with the DV01 a rates desk sizes them by. The "
               "discriminator is the point: if the ~filter were wrong this would return all "
               "thirteen OTC trades rather than the one that is an IRS.",
               "derivatives::InterestRateSwap")
    irs.projections = [Proj("otcId", ["otcId"]),
                       Proj("productType", ["productType"]),
                       Proj("notional", ["notional"]),
                       Proj("tenorYears", ["tenorYears"]),
                       Proj("dv01", ["dv01"])]
    irs.sort = ("otcId", False)
    out.append(irs)

    # Credit, where the subtype's own risk measure is a different formula entirely.
    cds = Spec("stress::OT4_CreditProtection", "/stress/ot4",
               "Single-name CDS with loss-given-default at the standard 40% recovery. A "
               "different subtype, a different risk measure, the same base table.",
               "derivatives::CreditDefaultSwap")
    cds.projections = [Proj("otcId", ["otcId"]),
                       Proj("counterpartyId", ["counterpartyId"]),
                       Proj("notional", ["notional"]),
                       Proj("lossGivenDefault", ["lossGivenDefault"])]
    cds.sort = ("otcId", False)
    out.append(cds)

    # Compliance: uncollateralised trades, and which of them have no master agreement. A real
    # query, and it lands on the row seeded with three genuinely absent optional columns.
    compliance = Spec("stress::OT5_UncollateralisedTrades", "/stress/ot5",
                      "Uncollateralised OTC trades and whether they have option terms. The "
                      "commodity swap has no clearing house and no master agreement -- three "
                      "optional columns genuinely absent on one row.",
                      "derivatives::OtcTrade")
    compliance.projections = [Proj("otcId", ["otcId"]),
                              Proj("productType", ["productType"]),
                              Proj("collateralised", ["collateralised"]),
                              Proj("masterAgreement", ["masterAgreement"]),
                              Proj("noOptionTerms", ["optionTerms"], agg="isEmpty")]
    # `==`, not `=`. The corpus's predicate vocabulary is Pure's, and `=` is not in it.
    compliance.filters = [Pred(["collateralised"], "==", False)]
    compliance.sort = ("otcId", False)
    out.append(compliance)
    return out


OTC = _otc_specs()


def _risk_specs():
    """The reports a market-risk function actually produces.

    The run summary, the firm VaR by model, the sensitivity ladder by tenor, the stress
    results against limits, and the one query that matters operationally: which runs
    produced nothing.
    """
    out = []

    runs = Spec("stress::MR0_RunSummary", "/stress/mr0",
                "Risk runs with their wall-clock duration. The failed run has no completion "
                "time, so its duration is null rather than zero, and the intraday run has "
                "no approver.",
                "risk::RiskRun")
    runs.projections = [Proj("runId", ["runId"]),
                        Proj("cobDate", ["cobDate"]),
                        Proj("runType", ["runType"]),
                        Proj("status", ["status"]),
                        Proj("approvedBy", ["approvedBy"]),
                        Proj("durationMinutes", ["durationMinutes"]),
                        Proj("noMeasures", ["measures"], agg="isEmpty")]
    runs.sort = ("runId", False)
    out.append(runs)

    # Firm VaR across models. Three models of one question, which is exactly why measureType
    # is a discriminator: the numbers are not comparable but they are all VaR.
    var = Spec("stress::MR1_FirmVar", "/stress/mr1",
               "Firm-level risk measures with the model that produced each. The XVA rows "
               "have no confidence level and no horizon -- a CVA is an adjustment, not a "
               "percentile.",
               "risk::RiskMeasure")
    var.projections = [Proj("measureId", ["measureId"]),
                       Proj("measureType", ["measureType"]),
                       Proj("confidence", ["confidence"]),
                       Proj("horizonDays", ["horizonDays"]),
                       Proj("measureValue", ["measureValue"]),
                       Proj("modelName", ["modelName"]),
                       Proj("valueEur", ["valueIn"], args=[0.92]),
                       Proj("cobDate", ["measureRun", "cobDate"])]
    var.sort = ("measureId", False)
    out.append(var)

    # The subtype query: historical VaR only. If the discriminator were wrong this would
    # return all eleven measures instead of the three that are historical VaR.
    hist = Spec("stress::MR2_HistoricalVar", "/stress/mr2",
                "Historical VaR only, across runs. The discriminator is the point: eleven "
                "measures in the table, three of them this type.",
                "risk::HistoricalVar")
    hist.projections = [Proj("measureId", ["measureId"]),
                        Proj("scope", ["scope"]),
                        Proj("measureValue", ["measureValue"]),
                        Proj("lookbackDays", ["lookbackDays"])]
    hist.sort = ("measureId", False)
    out.append(hist)

    # The sensitivity ladder: the report a rates desk reads every morning. Navigates from
    # the sensitivity to its factor AND to its trade, so one row carries three tables.
    ladder = Spec("stress::MR3_SensitivityLadder", "/stress/mr3",
                  "The sensitivity ladder: every exposure with the factor it loads on and "
                  "the trade it came from. A spot factor has no tenor, so those rows carry "
                  "a null where the curve rows carry a bucket.",
                  "risk::Sensitivity")
    ladder.projections = [Proj("sensitivityId", ["sensitivityId"]),
                          Proj("measureType", ["measureType"]),
                          Proj("measureValue", ["measureValue"]),
                          Proj("curveName", ["factor", "curveName"]),
                          Proj("tenor", ["factor", "tenor"]),
                          Proj("assetClass", ["factor", "assetClass"]),
                          Proj("productType", ["sensitivityTrade", "productType"]),
                          Proj("notional", ["sensitivityTrade", "notional"])]
    ladder.sort = ("sensitivityId", False)
    out.append(ladder)

    # DV01 by curve and tenor -- the aggregation every rates report opens with.
    dv01 = Spec("stress::MR4_Dv01ByTenor", "/stress/mr4",
                "DV01 aggregated by curve and tenor bucket. The subtype restricts to rates "
                "sensitivities, so credit and equity exposures do not contaminate the "
                "ladder.",
                "risk::Dv01Sensitivity")
    dv01.projections = [Proj("factorId", ["factorId"]),
                        Proj("measureValue", ["measureValue"])]
    dv01.group_by = ["factorId"]
    dv01.aggs = [("points", "measureValue", "count"),
                 ("totalDv01", "measureValue", "sum"),
                 ("largest", "measureValue", "max")]
    dv01.sort = ("factorId", False)
    out.append(dv01)

    # Stress results against limits, navigating to the scenario definition.
    stress = Spec("stress::MR5_StressResults", "/stress/mr5",
                  "Stress results with the scenario behind each. The hypothetical scenarios "
                  "have no historical event date; the severe ones breach the limit.",
                  "risk::ScenarioResult")
    stress.projections = [Proj("resultId", ["resultId"]),
                          Proj("stressedPnl", ["stressedPnl"]),
                          Proj("breachedLimit", ["breachedLimit"]),
                          Proj("scenarioName", ["scenario", "scenarioName"]),
                          Proj("scenarioType", ["scenario", "scenarioType"]),
                          Proj("severity", ["scenario", "severity"]),
                          Proj("asOfEvent", ["scenario", "asOfEvent"]),
                          Proj("isRegulatory", ["scenario", "isRegulatory"])]
    stress.sort = ("resultId", False)
    out.append(stress)
    return out


RISK = _risk_specs()


def _middle_office_specs():
    """The middle office's own reports: the blotter, the outstanding confirmations, the
    break ageing, and the resets that moved cash."""
    out = []

    blotter = Spec("stress::MO0_LifecycleBlotter", "/stress/mo0",
                   "Every lifecycle event with its settlement lag and the trade it belongs "
                   "to. Which columns are populated depends on the event type: a reset has "
                   "a fixing and no notional change, a compression the reverse.",
                   "middleoffice::LifecycleEvent")
    blotter.projections = [Proj("eventId", ["eventId"]),
                           Proj("eventType", ["eventType"]),
                           Proj("status", ["status"]),
                           Proj("notionalDelta", ["notionalDelta"]),
                           Proj("fixingRate", ["fixingRate"]),
                           Proj("cashFlow", ["cashFlow"]),
                           Proj("settlementLagDays", ["settlementLagDays"]),
                           Proj("productType", ["eventTrade", "productType"])]
    blotter.sort = ("eventId", False)
    out.append(blotter)

    # The subtype query. Resets are the events that move cash without changing size, and
    # asking for them by class is what the discriminator is for.
    resets = Spec("stress::MO1_RateResets", "/stress/mo1",
                  "Rate resets only: the events that produce a cash flow from a fixing "
                  "without changing the notional. Two of twelve events.",
                  "middleoffice::RateResetEvent")
    resets.projections = [Proj("eventId", ["eventId"]),
                          Proj("effectiveDate", ["effectiveDate"]),
                          Proj("fixingRate", ["fixingRate"]),
                          Proj("cashFlow", ["cashFlow"])]
    resets.sort = ("eventId", False)
    out.append(resets)

    confs = Spec("stress::MO2_Confirmations", "/stress/mo2",
                 "Confirmations with hours to match. The outstanding and disputed ones have "
                 "no match time, so the duration is null rather than large -- which is the "
                 "distinction an ageing report has to keep.",
                 "middleoffice::Confirmation")
    confs.projections = [Proj("confirmationId", ["confirmationId"]),
                         Proj("method", ["method"]),
                         Proj("platform", ["platform"]),
                         Proj("status", ["status"]),
                         Proj("chaseCount", ["chaseCount"]),
                         Proj("hoursToMatch", ["hoursToMatch"]),
                         Proj("counterpartyId", ["confirmedTrade", "counterpartyId"])]
    confs.sort = ("confirmationId", False)
    out.append(confs)

    breaks = Spec("stress::MO3_BreakAgeing", "/stress/mo3",
                  "Break ageing with the disputed field and both sides' values. A "
                  "confirmation break has no single field in dispute, so those columns are "
                  "null for it.",
                  "middleoffice::TradeBreak")
    breaks.projections = [Proj("breakId", ["breakId"]),
                          Proj("breakType", ["breakType"]),
                          Proj("severity", ["severity"]),
                          Proj("status", ["status"]),
                          Proj("ageDays", ["ageDays"]),
                          Proj("resolvedDate", ["resolvedDate"]),
                          Proj("fieldName", ["fieldName"]),
                          Proj("ourValue", ["ourValue"]),
                          Proj("theirValue", ["theirValue"])]
    breaks.sort = ("breakId", False)
    out.append(breaks)

    # Break count by severity -- the slide the daily ops call opens with.
    bysev = Spec("stress::MO4_BreaksBySeverity", "/stress/mo4",
                 "Breaks by severity and status: the daily ops call's first slide.",
                 "middleoffice::TradeBreak")
    bysev.projections = [Proj("severity", ["severity"]),
                         Proj("status", ["status"]),
                         Proj("ageDays", ["ageDays"])]
    bysev.group_by = ["severity", "status"]
    bysev.aggs = [("breakCount", "ageDays", "count"),
                  ("oldestDays", "ageDays", "max"),
                  ("totalDays", "ageDays", "sum")]
    bysev.sort = ("severity", False)
    out.append(bysev)

    # Trades with no break at all -- the clean book, which is the majority.
    clean = Spec("stress::MO5_CleanTrades", "/stress/mo5",
                 "Trades with no break against them. Emptiness over a to-many where the "
                 "empty side is the good outcome and the common one.",
                 "derivatives::OtcTrade")
    clean.projections = [Proj("otcId", ["otcId"]),
                         Proj("productType", ["productType"]),
                         Proj("noBreaks", ["breaks"], agg="isEmpty"),
                         Proj("noEvents", ["lifecycleEvents"], agg="isEmpty")]
    clean.sort = ("otcId", False)
    out.append(clean)
    return out


MIDDLE_OFFICE = _middle_office_specs()


def _back_office_specs():
    """The back office's own reports: the payment blotter, the cash ladder, the standing
    instructions in force, and the reconciliation -- which is the only one that matters."""
    out = []

    pmts = Spec("stress::BO0_PaymentBlotter", "/stress/bo0",
                "Payments with the instruction that routed each and the trade behind it. "
                "Failed, cancelled and pending payments have no settlement time and the "
                "failed one carries a reason.",
                "backoffice::Payment")
    pmts.projections = [Proj("paymentId", ["paymentId"]),
                        Proj("paymentType", ["paymentType"]),
                        Proj("direction", ["direction"]),
                        Proj("amount", ["amount"]),
                        Proj("signedAmount", ["signedAmount"]),
                        Proj("status", ["status"]),
                        Proj("settledAt", ["settledAt"]),
                        Proj("failReason", ["failReason"]),
                        Proj("correspondentBic", ["instruction", "correspondentBic"])]
    pmts.sort = ("paymentId", False)
    out.append(pmts)

    # The reconciliation. Three outcomes, three classes, and the null columns are the
    # evidence: an unmatched statement item has no payment, an unmatched ledger item has no
    # movement, and a matched item has nobody assigned to it.
    recon = Spec("stress::BO1_Reconciliation", "/stress/bo1",
                 "The nostro reconciliation. An unmatched STATEMENT item has no payment "
                 "behind it and an unmatched LEDGER item has no movement, so the two null "
                 "columns are the finding rather than missing data.",
                 "backoffice::ReconciliationItem")
    recon.projections = [Proj("reconId", ["reconId"]),
                         Proj("itemType", ["itemType"]),
                         Proj("amount", ["amount"]),
                         Proj("status", ["status"]),
                         Proj("assignedTo", ["assignedTo"]),
                         Proj("ageDays", ["ageDays"]),
                         Proj("paymentId", ["paymentId"]),
                         Proj("movementId", ["movementId"]),
                         Proj("accountName", ["reconNostro", "accountName"])]
    recon.sort = ("reconId", False)
    out.append(recon)

    # The subtype nobody wants to see: right amount, wrong day. An amount-only reconciliation
    # would call this matched.
    vdate = Spec("stress::BO2_ValueDateBreaks", "/stress/bo2",
                 "Value-date mismatches only: matched on money, broken on timing. The break "
                 "an amount-only reconciliation misses entirely.",
                 "backoffice::ValueDateMismatchItem")
    vdate.projections = [Proj("reconId", ["reconId"]),
                         Proj("amount", ["amount"]),
                         Proj("assignedTo", ["assignedTo"]),
                         Proj("ageDays", ["ageDays"])]
    vdate.sort = ("reconId", False)
    out.append(vdate)

    ssi = Spec("stress::BO3_StandingInstructions", "/stress/bo3",
               "Standing settlement instructions. The superseded one carries an end date "
               "and the securities one has no currency -- it settles a position, not an "
               "amount.",
               "backoffice::SettlementInstruction")
    ssi.projections = [Proj("ssiId", ["ssiId"]),
                       Proj("currency", ["currency"]),
                       Proj("instrumentType", ["instrumentType"]),
                       Proj("correspondentBic", ["correspondentBic"]),
                       Proj("isDefault", ["isDefault"]),
                       Proj("effectiveTo", ["effectiveTo"]),
                       Proj("status", ["status"]),
                       Proj("legalName", ["ssiCounterparty", "legalName"])]
    ssi.sort = ("ssiId", False)
    out.append(ssi)

    # Cash by currency and status -- the treasury view.
    ladder = Spec("stress::BO4_CashByCurrency", "/stress/bo4",
                  "Payments by currency and status: the treasury cash ladder.",
                  "backoffice::Payment")
    ladder.projections = [Proj("currency", ["currency"]),
                          Proj("status", ["status"]),
                          Proj("amount", ["amount"])]
    ladder.group_by = ["currency", "status"]
    ladder.aggs = [("paymentCount", "amount", "count"),
                   ("totalAmount", "amount", "sum"),
                   ("largest", "amount", "max")]
    ladder.sort = ("currency", False)
    out.append(ladder)

    # Accounts with nothing outstanding. The inactive GBP account has no movements at all.
    accts = Spec("stress::BO5_NostroAccounts", "/stress/bo5",
                 "Nostro accounts with whether anything is outstanding against them. The "
                 "inactive GBP account has no movements and no reconciliation items, which "
                 "is two empty to-manys on one row.",
                 "backoffice::NostroAccount")
    accts.projections = [Proj("nostroId", ["nostroId"]),
                         Proj("accountName", ["accountName"]),
                         Proj("isActive", ["isActive"]),
                         Proj("noMovements", ["movements"], agg="isEmpty"),
                         Proj("noReconItems", ["reconItems"], agg="isEmpty")]
    accts.sort = ("nostroId", False)
    out.append(accts)
    return out


BACK_OFFICE = _back_office_specs()


def _m2m_enum_probe():
    """The same chain plus the enum-mapped property. Quarantined: F12."""
    s = Spec("stress::M2_CanonicalWithEnum", "/stress/m2",
             "The M2M chain projecting the enum-mapped `side`. The relational mapping "
             "translates 'B' to BUY; through a ModelChainConnection the raw code arrives "
             "instead. Isolated from M1 so one defect does not mask an unrelated "
             "invariance.", "canonical::CanonicalTrade")
    s.projections = [Proj("identifier", ["identifier"]), Proj("side", ["side"])]
    s.mapping, s.runtime = "canonical::M2MMapping", "stress::CanonicalRT"
    s.connection, s.paradigm = "environment", "tds"
    mirror = Spec("stress::_M2MirrorSource", "/stress/_m2mirror", "", "trading::Trade")
    mirror.projections = [Proj("identifier", ["tradeId"]), Proj("side", ["side"])]
    s.mirrors = mirror
    return s


CANONICAL_WITH_ENUM = _m2m_enum_probe()


# The M2M pack's groups (the improvement plan's Phase 5, m2m.py): each M2M service with the relational twin it
# mirrors, following _m2m_pair. The relation-mapping pack's (Phase 6, relmap.py): each relation-mapped service with
# the relational twin it mirrors.
INVARIANCE_GROUPS = ([[CANONICAL, FLAT, EMBEDDED, INLINE, MULTI], [WHOLE, PARTITIONED],
                      [BY_QUERY, BY_MAPPING], [RELATIONAL_SIDE, CANONICAL_SIDE]]
                     + m2m.INVARIANCE_GROUPS + relmap.INVARIANCE_GROUPS)
INVARIANCE = [s for g in INVARIANCE_GROUPS for s in g]


# ---------------------------------------------------------------- L3 temporal
#
# The same question asked at four business dates. T1 and T2 are one day apart and
# straddle CP-0003's version boundary, which is the assertion that matters: the interval
# is [FROM, THRU), so 2024-06-06 must return the OLD rating and 2024-06-07 the NEW one.
# An implementation that treated THRU as inclusive returns the old rating on the day the
# new one took effect — a wrong answer that reads as entirely reasonable.

def _temporal(n: int, name: str, as_of: str, doc: str) -> Spec:
    s = Spec(f"{PREFIX}{n}_{name}", f"/stress/t{n}", doc, "counterparty::RatingVersion")
    s.as_of = as_of
    s.projections = [Proj(a, [a]) for a in
                     ("counterpartyId", "rating", "agency", "outlook",
                      "isInvestmentGrade")]
    return s


TEMPORAL = [
    _temporal(10, "RatingsAsOf2019", "2019-01-01",
              "Before CP-0003 and CP-0004 were onboarded: only the two oldest "
              "counterparties have a rating in force."),
    _temporal(11, "RatingsAsOf2021", "2021-01-01",
              "CP-0004 still rated (its rating is withdrawn in 2022) and CP-0001 is in "
              "its middle version — a date where the answer differs from BOTH the "
              "earliest and the current state."),
    _temporal(12, "RatingsDayBeforeChange", "2024-06-06",
              "The day BEFORE CP-0003 crosses out of investment grade. Must return BBB."),
    _temporal(13, "RatingsOnChangeDate", "2024-06-07",
              "The day the new version takes effect. [FROM, THRU) is start-inclusive and "
              "end-EXCLUSIVE, so this must return BB+, not BBB. One day apart from T12 "
              "and the only difference between them is the boundary rule."),
    _temporal(14, "RatingsLatest", "latest",
              "%latest — the rows whose THRU is the infinity date. CP-0004's rating was "
              "withdrawn without a successor, so it must NOT appear; CP-0005 has no "
              "history at all and never appears."),
]


# ------------------------------------------------------------- L4 self-join
SELF_JOIN = [
    _spec(21, "TraderManagerChain", "org::Trader",
          "A {target} self-join walked two levels. TRD-001 and TRD-003 report to nobody "
          "(absent key), TRD-004 reports to a trader who has left (dangling key), and the "
          "two-level hop reaches a manager's manager. All three must come back as rows "
          "with NULLs where the chain stops -- never a dropped row and never a loop.",
          [("traderId", "traderId"), ("lastName", "lastName"),
           ("mgrId", "manager.traderId"), ("mgrLast", "manager.lastName"),
           ("mgrDesk", "manager.desk.name"),
           ("grandMgrId", "manager.manager.traderId"),
           ("grandMgrLast", "manager.manager.lastName")],
          []),
]


# ---------------------------------------------------------- L3b bitemporal
#
# The same BUSINESS date asked at two PROCESSING dates. B0 and B1 differ only in when the
# question was asked, and they must give different answers -- that is the entire content
# of bitemporality, and a store that collapses the two dimensions cannot produce it.

def _bitemporal(n: int, name: str, processing: str, business: str, doc: str) -> Spec:
    """Argument order is all(PROCESSING, BUSINESS) — engine order, not the order the
    concepts are usually said in."""
    s = Spec(f"stress::B{n}_{name}", f"/stress/b{n}", doc, "products::InstrumentRating")
    s.as_of = [processing, business]
    s.projections = [Proj(a, [a]) for a in ("instrumentId", "creditRating", "source")]
    return s


BITEMPORAL = [
    _bitemporal(0, "RatingAsBelievedThen", "2024-02-01", "2024-02-01",
                "Business 2024-02-01 as believed on 2024-02-01: INST-HSBA is 'A'. This is "
                "what a report run that month would have said."),
    _bitemporal(1, "RatingAsBelievedNow", "2024-04-01", "2024-02-01",
                "The SAME business date, asked after the correction landed: 'A-'. Neither "
                "answer is wrong; a single-temporal store cannot hold both."),
    _bitemporal(2, "RatingBeforeAnyBelief", "2024-01-05", "2024-02-01",
                "The same business date asked BEFORE anything was recorded about it. The "
                "processing dimension excludes every row, so INST-HSBA is absent "
                "entirely -- not null, absent."),
]


# ------------------------------------------------------------- graph fetch
#
# A completely different execution path: the result is a TREE, not a TDS. The earlier
# audit of legend-engine found two defects here, and the corpus now contains exactly the
# constructs they involve -- so these three are aimed rather than exploratory.
#
#   G0  to-one nesting with an orphan: the whole sub-object must be null, not an object
#       full of nulls
#   G1  graph fetch over the UNION-mapped class (audit finding: cast exception)
#   G2  graph fetch over a milestoned class at %latest (audit finding: parser NPE)

def _graph_spec(n: int, name: str, root: str, doc: str, tree: dict,
                as_of: str | None = None) -> Spec:
    s = Spec(f"stress::G{n}_{name}", f"/stress/g{n}", doc, root)
    s.graph, s.as_of = tree, as_of
    return s


GRAPH = [
    _graph_spec(0, "TradeTree", "trading::Trade",
                "Nested to-one navigation. TRD-0007's counterparty does not exist, so the "
                "whole counterparty sub-object must be null -- the tree-shaped version of "
                "a distinction a flat projection cannot make.",
                {"tradeId": None, "status": None, "notional": None,
                 "instrument": {"instrumentId": None, "name": None, "ticker": None},
                 "counterparty": {"counterpartyId": None, "legalName": None,
                                  "lei": None}}),

    _graph_spec(1, "UnionTree", "trading::HistTrade",
                "Graph fetch over a class mapped by an Operation UNION, including an "
                "empty leg. Deliberately projects NO enum -- see G3, which does, and "
                "fails for an unrelated reason that would otherwise mask this one.",
                {"tradeId": None, "notional": None, "status": None}),

    _graph_spec(3, "UnionTreeWithEnum", "trading::HistTrade",
                "The same tree plus the enum-mapped `side`. TRD-0012 carries source code "
                "'ZZ', which has no EnumerationMapping entry. A TDS projection returns "
                "NULL for it (F-A14); graph fetch RAISES. Same data, same mapping, two "
                "execution paths, two semantics -- see F10.",
                {"tradeId": None, "side": None, "notional": None, "status": None}),

    _graph_spec(2, "MilestonedTree", "counterparty::RatingVersion",
                "Graph fetch over a business-temporal class at %latest. An earlier audit "
                "reported a parser NPE on %latest in a graph-fetch tree.",
                {"counterpartyId": None, "rating": None, "outlook": None}, "latest"),
]


# ================================================================ STACK LAYER
#
# The corpus is otherwise a set of narrow probes: median 2 features per service, exactly
# one service above 4, none above 6. That is deliberate for ATTRIBUTION -- F10, F12 and
# F13 are each reportable precisely because they were isolated -- but a defect that only
# appears when eight features interact is invisible to a probe by construction.
#
# So the corpus needs both layers, and this is the second one. These services stack as
# many CURRENTLY-PASSING features as one query can legitimately carry. They deliberately
# exclude anything already quarantined: a stack containing a known defect is red for a
# reason nobody has to look for, and would mask every interaction it was built to find.

STACK = [
    _spec(30, "TradeEverything", "trading::Trade",
          "The deep stack: enum-mapped side, a derived property, a QUALIFIED property "
          "with two different arguments, three independent 3-hop navigation chains, a "
          "two-condition filter, a sort and a limit -- one query. Each of these passes "
          "alone elsewhere in the corpus; this asserts they still hold together.",
          [("tradeId", "tradeId"),
           ("side", "side"),                                  # enum
           ("status", "status"),
           ("quantity", "quantity"), ("price", "price"),
           # three separate 3-hop chains through different domains
           ("sectorName", "instrument.sector.name"),
           ("sectorGics", "instrument.sector.gicsCode"),
           ("deskName", "book.desk.name"),
           ("deskRegion", "book.desk.region"),
           ("cptyCountry", "counterparty.country.name"),
           ("cptyRegion", "counterparty.country.region"),
           ("traderDesk", "trader.desk.name"),
           # derived
           ("grossAmount", "grossAmount")],
          [],
          # qualified, twice with different arguments
          [("grossGbp", "grossAmountIn", [0.79]),
           ("grossEur", "grossAmountIn", [0.92])]),

    _spec(32, "TradeRollupEverything", "trading::Trade",
          "The widest stack in the corpus: a two-condition filter, a 3-hop navigation, an "
          "enum-mapped column and a FUNCTION result used as GROUP KEYS, a derived "
          "property used as an aggregate SOURCE, count and sum, a sort and a limit. Ten "
          "features in one query, and the aggregates must decompose to the ungrouped "
          "totals.",
          [("bookName", "book.name"), ("deskRegion", "book.desk.region"),
           ("side", "side"), ("notional", "notional"), ("gross", "grossAmount")],
          []),

    _spec(31, "TradeRules", "trading::Trade",
          "Standalone FUNCTIONS called extension-style, alongside the columns they are "
          "computed from. isBuy reads an ENUM inside a function body -- a different path "
          "from projecting one, which matters given F10/F12/F13 -- and "
          "counterpartyRegion NAVIGATES inside the body, so TRD-0007's orphan must yield "
          "null rather than an error.",
          [("tradeId", "tradeId"), ("side", "side"), ("quantity", "quantity"),
           ("price", "price"), ("cptyRegionDirect", "counterparty.country.region")],
          []),
]

# F32: a function result as a GROUP KEY -- a boolean partition of the trades computed by
# a shared rule rather than read from a column.
_F32 = next(s for s in STACK if s.short.startswith("F32_"))
_F32.projections.append(Proj("isLarge", [], None, [], "stress::rules::isLargeTrade"))
_F32.filters = [Pred(["status"], "==", "EXECUTED"), Pred(["quantity"], ">", 500.0)]
_F32.group_by = ["bookName", "deskRegion", "side", "isLarge"]
_F32.aggs = [("tradeCount", "notional", "count"), ("totalGross", "gross", "sum"),
             ("maxNotional", "notional", "max")]
_F32.sort = ("bookName", False)
_F32.limit = 20

# Functions are a distinct projection kind, so they are attached after construction.
# Referenced BY NAME, not by index: inserting F32 into the list silently moved STACK[1]
# from F31 to F32 and the function projections landed on the wrong service.
_by_name = {s.short.split("_")[0]: s for s in STACK}
_by_name["F31"].projections += [
    Proj("isLarge", [], None, [], "stress::rules::isLargeTrade"),
    Proj("isBuy", [], None, [], "stress::rules::isBuy"),
    Proj("over1m", [], None, [1000000.0], "stress::rules::exceeds"),
    Proj("over100k", [], None, [100000.0], "stress::rules::exceeds"),
    Proj("cptyRegionViaFn", [], None, [], "stress::rules::counterpartyRegion"),
]
# One function call on the deep stack too, so it is exercised in combination rather than
# only in isolation.
_by_name["F30"].projections.append(
    Proj("isLarge", [], None, [], "stress::rules::isLargeTrade"))
STACK[0].filters = [Pred(["status"], "==", "EXECUTED"),
                    Pred(["quantity"], ">", 500.0)]
STACK[0].sort = ("tradeId", False)
STACK[0].limit = 12


# ------------------------------------------------------------ L5 XStore
#
# A navigation that crosses a STORE boundary. No Join can express it -- a Join lives
# inside one Database -- so the link is a predicate over a local property:
#   legalEntity: $this.entityRef == $that.entityId
#
# The seed makes all three outcomes reachable (A18): trades whose entity exists, a trade
# whose counterparty is absent from the external master, and an entity no trade matches.
# The registered names differ from the local legal names, so a query that accidentally
# read the LOCAL counterparty would return a different string rather than the same one.

def _xstore():
    """XStore navigation works under GRAPH FETCH and fails under a projection (F15), so
    the corpus covers it on the path that supports it and quarantines the other."""
    graph = Spec("stress::X0_TradeExternalEntity", "/stress/x0",
                 "Cross-store navigation by graph fetch: trading::Trade in store::DB "
                 "reaching external::LegalEntity in external::EntityDB through an XStore "
                 "association. Two stores, two connections, two ###Data elements.",
                 "trading::Trade")
    graph.graph = {"tradeId": None, "notional": None, "status": None,
                   "legalEntity": {"registeredName": None, "jurisdiction": None,
                                   "isSanctioned": None}}
    graph.mapping, graph.runtime = "external::EntityMapping", "stress::XStoreRT"
    graph.extra_data = [("entities", "external::EntityData")]

    proj = Spec("stress::X1_ExternalEntityProjection", "/stress/x1",
                "The SAME cross-store navigation as a projection. Fails with an internal "
                "Pure match failure -- the relational SQL generator has no branch for "
                "XStorePropertyMapping. Quarantined: F15.", "trading::Trade")
    proj.projections = [Proj("tradeId", ["tradeId"]),
                        Proj("entityName", ["legalEntity", "registeredName"])]
    proj.mapping, proj.runtime = "external::EntityMapping", "stress::XStoreRT"
    proj.extra_data = [("entities", "external::EntityData")]
    return graph, proj


XSTORE, XSTORE_PROJECTION = _xstore()


# --------------------------------------------------------------- L5 ModelJoin
#
# The same navigation as the canonical model -- trade to its book -- but the relationship
# is a PREDICATE between two Relation-mapped classes rather than a Join in the store.
# Unlike XStore it names both sides as explicit lambda parameters. Same answer, or the
# ModelJoin does not mean what the association means.

def _modeljoin():
    s = Spec("stress::J0_TradeBookModelJoin", "/stress/j0",
             "trading::Trade to positions::Book through a ModelJoin over two Relation "
             "class mappings, instead of the store Join the canonical model uses. The "
             "answer must not change.", "trading::Trade")
    s.projections = [Proj("tradeId", ["tradeId"]), Proj("notional", ["notional"]),
                     Proj("status", ["status"]),
                     Proj("bookName", ["book", "name"]),
                     Proj("bookCurrency", ["book", "currency"])]
    s.mapping, s.runtime = "modeljoin::JoinMapping", "stress::ModelJoinRT"
    src = Spec("stress::_J0Mirror", "/stress/_j0m", "", "trading::Trade")
    src.projections = list(s.projections)
    s.mirrors = src
    return s


MODELJOIN = _modeljoin()


# ---------------------------------------------------------------- L6 Measure
#
# A unit-typed property. It serialises as an OBJECT carrying its unit, never as a bare
# number, which is the entire point of having Measures rather than a Float and a
# convention. The expectation is still DERIVED from the seed — the notional comes from the
# oracle and the unit envelope is wrapped around it — rather than captured from output.

def _unit_envelope(unit_id: str):
    def wrap(row):
        return {"identifier": row["identifier"],
                "amount": {"unit": [{"unitId": unit_id, "exponentValue": 1}],
                           "value": row["amount"]}}
    return wrap


def _measure():
    s = Spec("stress::MU0_MonetaryTrade", "/stress/mu0",
             "A Measure-typed amount through the M2M layer. Units cannot be mapped "
             "relationally at all -- legend-engine has no Unit handling in the relational "
             "compiler -- so a monetary amount that carries its unit has to be composed "
             "in an M2M mapping with newUnit(...)->cast(...).",
             "canonical::MonetaryTrade")
    s.graph = {"identifier": None, "amount": None}
    s.mapping, s.runtime = "canonical::MoneyMapping", "stress::MoneyRT"
    s.connection = "environment"
    src = Spec("stress::_MU0Mirror", "/stress/_mu0m", "", "trading::Trade")
    src.projections = [Proj("identifier", ["tradeId"]), Proj("amount", ["notional"])]
    s.mirrors = src
    s.transform = _unit_envelope("stress::Money~USD")
    return s


MEASURE = _measure()


# ------------------------------------------------- L5 AggregationAware
#
# ONE class, TWO query shapes, and the engine picks the table. A0 is a row-level
# projection, which must read the detail; A1 is a groupBy whose keys and aggregates match
# the declared view, which should read the pre-aggregated table.
#
# Both must return correct answers -- and because the aggregate is DERIVED from the
# detail, both do so whether or not the rewrite fires. The result cannot distinguish them.
# That is stated rather than papered over: what the pair asserts is that an
# AggregationAware mapping is CORRECT under both shapes, not that the rewrite happened.
# Which table was read is a question for the generated SQL.

def _aggregation_aware():
    detail = Spec("stress::A0_FactDetail", "/stress/a0",
                  "Row-level projection over an AggregationAware class: must read the "
                  "DETAIL table, one row per trade.", "reporting::TradeFact")
    detail.projections = [Proj("bookId", ["bookId"]), Proj("notional", ["notional"])]
    detail.mapping, detail.runtime = ("reporting::AggregationAwareMapping",
                                      "stress::AggregationRT")

    rolled = Spec("stress::A1_FactRollup", "/stress/a1",
                  "A groupBy matching the declared view exactly. Legacy TDS form on "
                  "purpose: the AggregationAware rewrite dispatches only on that "
                  "signature, so the same query written with ~[...] would silently read "
                  "the detail table instead.", "reporting::TradeFact")
    rolled.projections = [Proj("bookId", ["bookId"]), Proj("notional", ["notional"])]
    rolled.mapping, rolled.runtime = ("reporting::AggregationAwareMapping",
                                      "stress::AggregationRT")
    rolled.paradigm = "tds"
    rolled.group_by = ["bookId"]
    rolled.aggs = [("totalNotional", "notional", "sum")]
    # The oracle does not model AggregationAwareMapping, so both mirror the equivalent
    # query on trading::Trade -- which IS the equivalence the mapping asserts.
    d_src = Spec("stress::_A0Mirror", "/stress/_a0m", "", "trading::Trade")
    d_src.projections = [Proj("bookId", ["book", "bookId"]), Proj("notional", ["notional"])]
    detail.mirrors = d_src

    r_src = Spec("stress::_A1Mirror", "/stress/_a1m", "", "trading::Trade")
    r_src.projections = [Proj("bookId", ["book", "bookId"]), Proj("notional", ["notional"])]
    r_src.group_by = ["bookId"]
    r_src.aggs = [("totalNotional", "notional", "sum")]
    rolled.mirrors = r_src
    return detail, rolled


FACT_DETAIL, FACT_ROLLUP = _aggregation_aware()
AGGREGATION = [FACT_DETAIL, FACT_ROLLUP]


# ---------------------------------------------------------------- L4 rollup
ROLLUP = [
    _spec(20, "NotionalByBook", "reporting::BookRollup",
          "The rollup View: notional by book, grouped by the engine. Its rows are GROUPS, "
          "so BK-LEGACY -- which has no trades -- produces no group and is ABSENT. "
          "Contrast F2_BookChildCounts, which outer-joins from BOOK and returns a row for "
          "it. Two defensible answers to one question, differing on exactly the empty "
          "case, both asserted here.",
          [("bookId", "bookId"), ("tradeCount", "tradeCount"),
           ("totalNotional", "totalNotional"), ("maxNotional", "maxNotional")],
          []),
]


DERIVED = [
    _spec(7, "TradeDerivedProperties", "trading::Trade",
          "Derived properties alongside the columns they are computed from, so a wrong "
          "value cannot hide: grossAmount is total arithmetic over two [1] measures.",
          [("tradeId", "tradeId"), ("quantity", "quantity"), ("price", "price"),
           ("commission", "commission"), ("fees", "fees"),
           ("settlementDate", "settlementDate"), ("side", "side"),
           ("grossAmount", "grossAmount")],
          [],
          # The same quantity*price, scaled by an argument. Two different rates on one
          # row, so a qualified property that ignored its parameter — returning the
          # unscaled gross twice — is caught rather than merely looking plausible.
          [("grossInGbp", "grossAmountIn", [0.79]),
           ("grossInEur", "grossAmountIn", [0.92])]),

    _spec(8, "SettlementTradeDerived", "settlement::Settlement",
          "A derived property reached ACROSS an association, so it is evaluated on a "
          "joined row rather than the root. STL-9999 points at a trade that does not "
          "exist: grossAmount must be NULL there, which it is in both engines because "
          "NULL arithmetic propagates. The case that does NOT survive this shape -- a "
          "derived property that INSPECTS nullness -- is F8, in repro/derived-on-absent/.",
          [("settlementId", "settlementId"), ("status", "status"),
           ("tradeId", "trade.tradeId"), ("tradeGross", "trade.grossAmount"),
          ],
          []),
]

def _market_data_specs():
    """Time series: the questions that are about the SEQUENCE, not about the row.

    A price on a row is a fact anyone can project. What makes a time series a time series is
    that the interesting quantities -- change, direction, range, staleness, revision -- are
    relationships between observations, and every one of them lands on a construct the flat
    corpus exercises weakly:

      * `change` and `changeInBps` are derived properties over a NULLABLE prior value, where
        the discharge has to be `orElse(obsValue)` and not `orElse(0.0)`. MD0 projects both
        so the first observation of every series is visible as a zero rather than as a level.
      * per-series aggregates run over a fan-out that is NOT uniform -- fourteen for a daily
        series, three for a monthly one. MD2's count column proves the group-by is grouping
        rather than counting the table.
      * the bitemporal revisions are the only place in the corpus where the two axes move
        INDEPENDENTLY. MD4-MD6 ask the same business date on three processing dates and get
        three answers, one of which is that the row did not exist yet.
    """
    out = []

    # The series master. Small, but it carries the qualified property and the boolean
    # derived, and it is the parent every other query here fans out from.
    cat = Spec("stress::MD0_SeriesCatalogue", "/stress/md0",
               "The market-data catalogue: sixteen series across rates, FX, equity, "
               "commodity and macro, with the vendor ticker built from a caller-supplied "
               "vendor code and the derived daily/monthly flag.",
               "timeseries::TimeSeries")
    cat.projections = [Proj("seriesId", ["seriesId"]),
                       Proj("seriesName", ["seriesName"]),
                       Proj("assetClass", ["assetClass"]),
                       Proj("frequency", ["frequency"]),
                       Proj("currency", ["currency"]),
                       Proj("decimalPlaces", ["decimalPlaces"]),
                       Proj("isDaily", ["isDaily"]),
                       Proj("bloombergTicker", ["tickerOn"], args=["BBG"])]
    cat.sort = ("seriesId", False)
    out.append(cat)

    # Every observation with its move. 191 rows, and 16 of them -- one per series -- have a
    # null prior value, so the orElse discharge is exercised on 8% of the rows rather than on
    # a single hand-placed edge case.
    obs = Spec("stress::MD1_ObservationHistory", "/stress/md1",
               "Every observation with the move since the previous print, in level and in "
               "basis points, plus the series it belongs to. Sixteen of the 191 rows are a "
               "series' first observation and have no prior value: change is zero there "
               "because the discharge is orElse(obsValue), and would be the entire level if "
               "it were orElse(0.0).",
               "timeseries::Observation")
    obs.projections = [Proj("obsId", ["obsId"]),
                       Proj("obsDate", ["obsDate"]),
                       Proj("obsValue", ["obsValue"]),
                       Proj("priorValue", ["priorValue"]),
                       Proj("change", ["change"]),
                       Proj("changeInBps", ["changeInBps"]),
                       Proj("status", ["status"]),
                       Proj("seriesName", ["series", "seriesName"]),
                       Proj("assetClass", ["series", "assetClass"])]
    obs.sort = ("obsId", False)
    out.append(obs)

    # Per-series statistics. The count column is the point: it must read 14 for the daily
    # series and 3 for the monthly ones. A group-by that has silently become a scan of the
    # whole table returns 191 in every row and is otherwise indistinguishable.
    stats = Spec("stress::MD2_SeriesStatistics", "/stress/md2",
                 "Per-series statistics over the observation history: how many prints, the "
                 "high, the low and the mean. The count is the assertion that matters -- 14 "
                 "for a daily series and 3 for a monthly one. A group-by that has collapsed "
                 "into a table scan returns 191 everywhere and looks perfectly reasonable.",
                 "timeseries::Observation")
    stats.projections = [Proj("seriesId", ["seriesId"]), Proj("obsValue", ["obsValue"])]
    stats.group_by = ["seriesId"]
    stats.aggs = [("observations", "obsValue", "count"),
                  ("high", "obsValue", "max"),
                  ("low", "obsValue", "min"),
                  ("mean", "obsValue", "average")]
    stats.sort = ("seriesId", False)
    out.append(stats)

    # The non-final prints. A flash estimate is a forecast sitting in a table of
    # measurements, and every average that does not exclude it is wrong by however much the
    # final print moved.
    prelim = Spec("stress::MD3_UnrevisedPrints", "/stress/md3",
                  "Observations that are not final: the three monthly series' latest prints "
                  "are flash estimates and the earliest are restatements. Filtering on "
                  "status is what separates a measurement from a forecast, and the corpus "
                  "should not be able to average them together by accident.",
                  "timeseries::Observation")
    prelim.projections = [Proj("obsId", ["obsId"]),
                          Proj("obsDate", ["obsDate"]),
                          Proj("obsValue", ["obsValue"]),
                          Proj("status", ["status"]),
                          Proj("isFinal", ["isFinal"]),
                          Proj("frequency", ["series", "frequency"])]
    # `not`, not `== False`. The comparison form is the one shape the engine cannot lower
    # (F50): `$x.isFinal == false` over a derived Boolean reaches DuckDB as SQL it rejects.
    # MD7 below pins that; this one asks the question in the form that works.
    prelim.filters = [Pred(["isFinal"], "not", None)]
    prelim.sort = ("obsId", False)
    out.append(prelim)

    # The broken form, pinned. Identical to MD3 in every respect except that the filter is
    # written as a comparison to a boolean literal rather than as a negation -- which is how
    # most people would write it, and which the engine cannot lower.
    broken = Spec("stress::MD7_UnrevisedPrintsEq", "/stress/md7",
                  "MD3 with the filter written `isFinal == false` instead of `!isFinal`. "
                  "The two mean the same thing and one of them reaches DuckDB as SQL it "
                  "rejects with a parser error. Quarantined under F50; the probe at "
                  "scripts/corpus/probe_derived_filter.py separates this from the four "
                  "derived-property filter forms that work.",
                  "timeseries::Observation")
    broken.projections = list(prelim.projections)
    broken.filters = [Pred(["isFinal"], "==", False)]
    broken.sort = ("obsId", False)
    out.append(broken)

    # The three bitemporal reads. Same business date, three processing dates -- and unlike
    # the existing B0-B2 over credit ratings, the two axes here are days apart rather than
    # simultaneous, so a mapping that quietly used one date for both would still pass B0-B2
    # and fail these.
    for n, (proc, doc) in enumerate([
        ("2024-05-20",
         "April as believed on 20 May: CPI reads 3.4 and unemployment 3.9, the first prints. "
         "This is what a report run that week would have said, and it is not what the "
         "current data says."),
        ("2024-06-20",
         "The SAME business date after both restatements landed: CPI 3.5, unemployment 4.0. "
         "The business axis has not moved at all; only what we believe about it has."),
        ("2024-05-01",
         "The same business date asked BEFORE the April numbers were published. "
         "Unemployment exists -- it printed on 3 May, which is still after this -- so the "
         "processing filter removes CPI entirely and leaves nothing. Absent, not null.")]):
        b = Spec(f"stress::MD{4 + n}_RevisionAsOf", f"/stress/md{4 + n}", doc,
                 "timeseries::ObservationRevision")
        b.as_of = [proc, "2024-04-30"]
        b.projections = [Proj("seriesId", ["seriesId"]),
                         Proj("revisedValue", ["revisedValue"]),
                         Proj("revisionReason", ["revisionReason"])]
        b.sort = ("seriesId", False)
        if proc == "2024-05-01":
            b.emptiness = emptiness.BEFORE_FIRST_VERSION
        out.append(b)

    return out


MARKET_DATA = _market_data_specs()


def _curve_specs():
    """Yield curves: what a three-column primary key can be stacked with.

    `composite PK` had been the corpus's most isolated construct -- two milestoned classes,
    three properties each, nothing navigating out -- and 33 uncovered feature pairs had it on
    one side. These put it together with the constructs it had never met, and they are
    hand-written because the generators cannot reach it: stacks.py chains along to-ONE ends
    and a curve pillar's only to-one is the embedded curve, so CurvePoint drops out of every
    generated set the moment the self-join is declared honestly as to-many on both sides.

    That is the division the corpus makes everywhere: the generators supply volume over shapes
    they understand, and the judgement about what is worth putting next to what is written
    down by hand.
    """
    out = []

    KEY = [Proj("curveId", ["curveId"]), Proj("cobDate", ["cobDate"]),
           Proj("tenorLabel", ["tenorLabel"])]
    KEY_SORT = [("curveId", False), ("cobDate", False), ("tenorLabel", False)]

    # The pillar grid. Composite key, the embedded curve (which emits no join at all, because
    # the columns are on the pillar's own row), the enum through that embedded block, two
    # derived properties and a qualified one -- six constructs on one row.
    grid = Spec("stress::CV0_CurvePillars", "/stress/cv0",
                "Every curve pillar: the three-column key, the rate, the stored discount "
                "factor, the rate in basis points, the tenor in years, the long-end flag, "
                "the present value of a million at that factor, and the curve itself read "
                "through an EMBEDDED mapping -- so the curve's name, currency and enum-mapped "
                "type come off the pillar's own row with no join emitted.",
                "curves::CurvePoint")
    grid.projections = KEY + [
        Proj("tenorDays", ["tenorDays"]),
        Proj("zeroRate", ["zeroRate"]),
        Proj("discountFactor", ["discountFactor"]),
        Proj("zeroRateBps", ["zeroRateBps"]),
        Proj("tenorYears", ["tenorYears"]),
        Proj("isLongEnd", ["isLongEnd"]),
        Proj("pvOfMillion", ["presentValueOf"], args=[1000000.0]),
        Proj("curveName", ["curveRef", "curveName"]),
        Proj("curveCurrency", ["curveRef", "currency"]),
        Proj("curveType", ["curveRef", "curveType"]),
        # The SAME three facts reached by join instead of off the row. A denormalization
        # that has drifted shows up here as two columns disagreeing in one service.
        Proj("joinedName", ["curve", "curveName"]),
        Proj("joinedCurrency", ["curve", "currency"]),
        Proj("joinedType", ["curve", "curveType"])]
    grid.sort = KEY_SORT
    out.append(grid)

    # The ~filter subtype. 15Y and 20Y are interpolated on every curve, so this is 160 of the
    # 192 rows -- a filter doing real work. The failure mode of a wrong ~filter is silent:
    # it returns the whole table and every column still looks right.
    quoted = Spec("stress::CV1_QuotedPillars", "/stress/cv1",
                  "Only the pillars the market actually quotes, through the subtype's "
                  "~filter. The 15Y and 20Y are interpolated on all eight curves, so this is "
                  "160 rows of 192 -- and if the filter were wrong it would be 192 with no "
                  "error and no type mismatch.",
                  "curves::QuotedPillar")
    quoted.projections = KEY + [
        Proj("tenorDays", ["tenorDays"]),
        Proj("zeroRate", ["zeroRate"]),
        Proj("isInterpolated", ["isInterpolated"]),
        Proj("curveName", ["curveRef", "curveName"]),
        Proj("curveType", ["curveRef", "curveType"])]
    quoted.sort = KEY_SORT
    out.append(quoted)

    # Out to the published series a pillar was fitted to. Four of 192 pillars have one, so
    # 188 rows carry a wholly absent sub-object rather than a null column -- and the two are
    # the same thing in a projection and different things in a tree.
    fitted = Spec("stress::CV2_FittedPillars", "/stress/cv2",
                  "Each pillar and the market-data series it was fitted to, where there is "
                  "one. Four pillars of 192 have a source series, so the navigation is empty "
                  "for 188 of them -- the majority case, not an edge case. Reached from a "
                  "composite-key root, which is the combination nothing executed before.",
                  "curves::CurvePoint")
    fitted.projections = KEY + [
        Proj("sourceSeriesId", ["sourceSeriesId"]),
        Proj("seriesName", ["sourceSeries", "seriesName"]),
        Proj("seriesFrequency", ["sourceSeries", "frequency"]),
        Proj("seriesAssetClass", ["sourceSeries", "assetClass"])]
    fitted.sort = KEY_SORT
    out.append(fitted)

    # The self-join, asked the one way that both the engine and the oracle can answer.
    #
    # Three constraints shape this. `count()` over an EMPTY to-many returns 1 rather than 0
    # (F6), so the 30Y pillar -- which has nothing longer than it -- is filtered out rather
    # than quarantining the whole service. `isEmpty()` would have avoided that, but it
    # duplicates the source row once per joined row over a self-join (F51). And the REVERSE
    # end returns the forward set (F52), so `shorterPillars` is not projected here.
    #
    # CV6 and CV7 pin the two defects; this one covers the construct.
    shape = Spec("stress::CV3_PillarNeighbours", "/stress/cv3",
                 "For each pillar except the longest, how many pillars on the same curve and "
                 "date sit further out. The join is a three-column self-join with an "
                 "inequality on the third -- the join a forward rate is computed across, and "
                 "the first thing in the corpus to put one under a composite primary key. "
                 "The 30Y is filtered out because it has nothing longer than it and F6 makes "
                 "count() over an empty to-many return 1.",
                 "curves::CurvePoint")
    shape.projections = KEY + [
        Proj("tenorDays", ["tenorDays"]),
        Proj("zeroRate", ["zeroRate"]),
        Proj("pillarsFurtherOut", ["longerPillars"], agg="count")]
    shape.filters = [Pred(["tenorLabel"], "!=", "30Y")]
    shape.sort = KEY_SORT
    out.append(shape)

    # F51, pinned: the same end asked as emptiness. Every boolean is right and the row count
    # is not -- the source row comes back once per joined row.
    empty = Spec("stress::CV6_PillarEmptiness", "/stress/cv6",
                 "CV3 asked as emptiness instead of as a count, over every pillar. The "
                 "booleans are correct and the ROWS are not: a pillar with three longer "
                 "pillars is returned three times. Quarantined under F51, which "
                 "scripts/corpus/probe_ineq_aggregate.py narrows to self-joins -- the same "
                 "isEmpty over a to-many to a DIFFERENT table returns one row per parent.",
                 "curves::CurvePoint")
    empty.projections = KEY + [
        Proj("tenorDays", ["tenorDays"]),
        Proj("isLastPillar", ["longerPillars"], agg="isEmpty")]
    empty.sort = KEY_SORT
    out.append(empty)

    # F52, pinned: the reverse direction of the same self-join.
    reverse = Spec("stress::CV7_PillarsShorter", "/stress/cv7",
                   "How many pillars sit SHORTER than each one -- the other end of the same "
                   "association, over the same {target} self-join. The 1M pillar is filtered "
                   "out for the F6 reason CV3 filters out the 30Y. Quarantined under F52: "
                   "the engine returns the FORWARD set for both ends, so every count here "
                   "is the number of pillars further out rather than nearer in.",
                   "curves::CurvePoint")
    reverse.projections = KEY + [
        Proj("tenorDays", ["tenorDays"]),
        Proj("pillarsNearerIn", ["shorterPillars"], agg="count")]
    reverse.filters = [Pred(["tenorLabel"], "!=", "1M")]
    reverse.sort = KEY_SORT
    out.append(reverse)

    # Per-curve statistics over a composite-key table. 24 pillars per curve across the two
    # cob dates, so a group-by that has collapsed into a table scan returns 192 everywhere.
    stats = Spec("stress::CV4_CurveShape", "/stress/cv4",
                 "Per-curve statistics over the pillar grid: how many pillars, the highest "
                 "and lowest rate on the curve, and the mean. 24 rows per curve over two cob "
                 "dates; a group-by that has quietly become a scan returns 192 in every row.",
                 "curves::CurvePoint")
    stats.projections = [Proj("curveId", ["curveId"]), Proj("zeroRate", ["zeroRate"])]
    stats.group_by = ["curveId"]
    stats.aggs = [("pillars", "zeroRate", "count"),
                  ("highest", "zeroRate", "max"),
                  ("lowest", "zeroRate", "min"),
                  ("mean", "zeroRate", "average")]
    stats.sort = ("curveId", False)
    out.append(stats)

    # The curve master itself, with the enum read directly rather than through the embedded
    # block -- so the two readings of curveType must agree.
    cat = Spec("stress::CV5_CurveCatalogue", "/stress/cv5",
               "The curve master: eight curves with their interpolation, day count and "
               "enum-mapped type. The legacy IBOR curve is the inactive one. CV0 reads the "
               "same enum through an EMBEDDED mapping of a denormalized column; this reads "
               "it from the curve's own table, and the two must agree.",
               "curves::YieldCurve")
    cat.projections = [Proj("curveId", ["curveId"]),
                       Proj("curveName", ["curveName"]),
                       Proj("currency", ["currency"]),
                       Proj("curveType", ["curveType"]),
                       Proj("interpolation", ["interpolation"]),
                       Proj("dayCountBasis", ["dayCountBasis"]),
                       Proj("isActive", ["isActive"])]
    cat.sort = ("curveId", False)
    out.append(cat)

    # ~distinct doing real work: twelve pillars per (curve, date), so 16 rows out of 192.
    # If the ~distinct were dropped the query still runs and returns 192 rows of perfectly
    # correct data, which is why a distinct with nothing to collapse tests nothing.
    dates = Spec("stress::CV8_CurveDates", "/stress/cv8",
                 "The (curve, date) pairs a curve exists for, read ~distinct off the pillar "
                 "table. Twelve pillars collapse to one row, so 192 become 16 -- and the "
                 "label beside them is built by a dynafunction in the mapping rather than "
                 "stored, which is the transform ~distinct has to collapse ACROSS.",
                 "curves::CurveDate")
    dates.projections = [Proj("curveId", ["curveId"]),
                         Proj("cobDate", ["cobDate"]),
                         Proj("curveLabel", ["curveLabel"])]
    dates.sort = [("curveId", False), ("cobDate", False)]
    out.append(dates)

    # The VIEW. Its own aggregation happens in the store rather than in the query, so the
    # numbers here have to agree with CV4's group-by over the same rows by two different
    # routes -- one through a ~groupBy view, one through a query-level groupBy.
    summary = Spec("stress::CV9_CurveSummaries", "/stress/cv9",
                   "Per curve and date, read from a VIEW whose ~groupBy aggregates in the "
                   "store. CV4 computes the same highest and lowest rate with a query-level "
                   "groupBy over the pillars; these must agree, and they are arrived at by "
                   "completely different routes. The curve is navigated back from the "
                   "summary, so a view row reaches a table row.",
                   "curves::CurveSummary")
    summary.projections = [Proj("curveId", ["curveId"]),
                           Proj("cobDate", ["cobDate"]),
                           Proj("pillarCount", ["pillarCount"]),
                           Proj("highestRate", ["highestRate"]),
                           Proj("lowestRate", ["lowestRate"]),
                           Proj("curveName", ["summarised", "curveName"]),
                           Proj("interpolation", ["summarised", "interpolation"])]
    summary.sort = [("curveId", False), ("cobDate", False)]
    out.append(summary)

    # A join CHAIN and a join with OR, both from a composite-key root, plus the local
    # property declared with `+` in the mapping.
    bench = Spec("stress::CV10_PillarBenchmark", "/stress/cv10",
                 "Each pillar, the series it was fitted to, and the series it EFFECTIVELY "
                 "uses -- its own where one is recorded and the curve's benchmark where none "
                 "is, which is a fallback and so an `or` in the join. Beside them, the "
                 "curve's benchmark reached the long way: pillar to curve to series, two "
                 "hops from a three-column key.",
                 "curves::CurvePoint")
    bench.projections = [Proj("curveId", ["curveId"]),
                         Proj("cobDate", ["cobDate"]),
                         Proj("tenorLabel", ["tenorLabel"]),
                         Proj("ownSeries", ["sourceSeriesId"]),
                         Proj("effectiveSeries", ["effectiveSeries", "seriesId"]),
                         Proj("effectiveName", ["effectiveSeries", "seriesName"]),
                         Proj("curveBenchmark", ["curve", "benchmarkSeries", "seriesId"])]
    bench.sort = [("curveId", False), ("cobDate", False), ("tenorLabel", False)]
    out.append(bench)

    return out


CURVES = _curve_specs()


def _brokerage_specs():
    """The two joins that are not key lookups, next to everything a trade carries.

    A range join and a disjunction were the corpus's two most isolated constructs -- 20 and 21
    uncovered pairs apiece -- because both lived only on curve pillars, a class with a
    composite key and little else. From a trade they meet the enum transformer, the join
    chains, the qualified properties and the cross-domain navigations that make trading::Trade
    the densest class in the model.
    """
    out = []

    # The range join, with the tier's own derived and qualified properties, and the trade's
    # enum-mapped side beside them so the two constructs share a row.
    tiered = Spec("stress::BK0_TieredBrokerage", "/stress/bk0",
                  "Each trade with the brokerage tier its notional falls into, reached by a "
                  "RANGE join -- `notional >= min and notional < max`, with no key to join "
                  "on and none possible, because the band is a property of the amount. All "
                  "four bands are occupied and the largest holds one trade, which is the "
                  "band an open upper edge gets wrong first.",
                  "trading::Trade")
    tiered.projections = [Proj("tradeId", ["tradeId"]),
                          Proj("notional", ["notional"]),
                          Proj("side", ["side"]),
                          Proj("tierId", ["brokerageTier", "tierId"]),
                          Proj("tierName", ["brokerageTier", "tierName"]),
                          Proj("bpsRate", ["brokerageTier", "bpsRate"]),
                          Proj("minimumFee", ["brokerageTier", "minimumFee"])]
    tiered.sort = ("tradeId", False)
    out.append(tiered)

    # The same range join reached the other way -- from the tier down to its trades -- with
    # the tier's derived rate and its qualified fee. A to-many count over a range join, which
    # nothing had asked for.
    bands = Spec("stress::BK1_BrokerageBands", "/stress/bk1",
                 "Each brokerage band, how many trades fall in it, the rate as a fraction "
                 "rather than in basis points, and what the band would charge on a million. "
                 "The count is over a RANGE join, so a band with no trades in it would be "
                 "the F6 case -- all four are occupied, deliberately.",
                 "brokerage::BrokerageTier")
    bands.projections = [Proj("tierId", ["tierId"]),
                         Proj("tierName", ["tierName"]),
                         Proj("minNotional", ["minNotional"]),
                         Proj("maxNotional", ["maxNotional"]),
                         Proj("rateFraction", ["rateFraction"]),
                         Proj("feeOnMillion", ["feeOn"], args=[1000000.0]),
                         Proj("tradesInBand", ["tieredTrades"], agg="count")]
    bands.sort = ("tierId", False)
    out.append(bands)

    # The disjunction. Asked as a count because it is genuinely to-many -- an unqualified
    # `or` says "every rule that applies", not "the venue rule if there is one" -- and every
    # trade matches at least one rule, so the F6 empty case does not arise.
    routed = Spec("stress::BK2_ClearingRoutes", "/stress/bk2",
                  "How many clearing rules each trade matches, over a join that matches on "
                  "venue OR on product. Most trades match two -- their venue's rule and "
                  "their product's -- and the one executed on a lower-case venue code "
                  "matches only its product's, the comparison being case sensitive. A "
                  "routing table really does return every applicable rule; choosing between "
                  "them is a precedence the join cannot express.",
                  "trading::Trade")
    routed.projections = [Proj("tradeId", ["tradeId"]),
                          Proj("venue", ["executionVenue"]),
                          Proj("tradeType", ["tradeType"]),
                          Proj("notional", ["notional"]),
                          Proj("matchingRules", ["clearingRoutes"], agg="count")]
    routed.sort = ("tradeId", False)
    out.append(routed)

    # The route table from the other side: which trades each rule catches, and the two kinds
    # of rule side by side with their null halves visible.
    rules = Spec("stress::BK3_RoutingRules", "/stress/bk3",
                 "The routing table itself. A venue rule carries no product and a product "
                 "rule carries no venue, so exactly one of the two columns is null on every "
                 "row -- which is what makes the `or` behind BK2 a real disjunction rather "
                 "than two conditions that happen to agree.",
                 "brokerage::ClearingRoute")
    rules.projections = [Proj("routeId", ["routeId"]),
                         Proj("venueCode", ["venueCode"]),
                         Proj("productCode", ["productCode"]),
                         Proj("clearingHouse", ["clearingHouse"]),
                         Proj("settlementCycle", ["settlementCycle"]),
                         Proj("isNetted", ["isNetted"]),
                         Proj("tradesRouted", ["routedTrades"], agg="count")]
    rules.sort = ("routeId", False)
    out.append(rules)

    return out


BROKERAGE = _brokerage_specs()


def _schedule_specs():
    """The versioned fee schedule: milestoning next to a range join and a composite key.

    Milestoning had 13 uncovered pairs for the reason every isolated construct has them --
    the two milestoned classes in the corpus carry three properties each and navigate to
    almost nothing, so there was nothing for milestoning to co-occur WITH. A schedule is
    versioned for real, its table has a composite key by construction, and reaching a trade
    from a band is a range join. One navigation, four constructs.
    """
    out = []

    for n, (as_of, doc) in enumerate([
        ("2024-06-01",
         "The schedule in force in June: the rates as they stand after the April cut. Read "
         "at a business date, from a milestoned table with a composite key, and joined to "
         "the trades in each band by RANGE -- which band a trade falls in is a property of "
         "its notional, so there is no key to join on and none possible."),
        ("2024-02-01",
         "The SAME query at a business date before the cut. Every band holds the same "
         "trades and charges more for them, which is what a client asking 'what would this "
         "have cost in February' is asking. Nothing about the query changes except the date "
         "-- and if milestoning were not applied, nothing about the ANSWER would change "
         "either, which is exactly the failure this pins."),
        ("2023-06-01",
         "Before the schedule existed at all. The milestoning predicate excludes every row, "
         "so the result is empty -- absent, not null, and not the earliest version.")]):
        s = Spec(f"stress::SC{n}_ScheduleAsOf", f"/stress/sc{n}", doc,
                 "schedule::TierVersion")
        s.as_of = as_of
        if as_of == "2023-06-01":
            s.emptiness = emptiness.BEFORE_FIRST_VERSION
        s.projections = [Proj("tierId", ["tierId"]),
                         Proj("tierName", ["tierName"]),
                         Proj("minNotional", ["minNotional"]),
                         Proj("maxNotional", ["maxNotional"]),
                         Proj("bpsRate", ["bpsRate"]),
                         Proj("minimumFee", ["minimumFee"]),
                         Proj("rateFraction", ["rateFraction"]),
                         Proj("feeOnMillion", ["feeOn"], args=[1000000.0]),
                         Proj("feeOnFiveMillion", ["feeOn"], args=[5000000.0])]
        s.sort = ("tierId", False)
        out.append(s)

    return out


SCHEDULE = _schedule_specs()


def _large_exposure_specs():
    """A composite key, in a schema, reached by a range join and a disjunction.

    62 of the corpus's remaining uncovered pairs had `composite PK`, `join non-equality` or
    `join with or` on one side, always because each lived alone on a class carrying nothing
    else. These put all three on one class, in a schema-qualified table, with an enum
    transformer, a ~filter subtype, a ~distinct set, class constraints and four datatypes
    nothing had previously declared.
    """
    out = []

    KEY = [Proj("reportId", ["reportId"]), Proj("cobDate", ["cobDate"]),
           Proj("lineNumber", ["lineNumber"])]
    KEY_SORT = [("reportId", False), ("cobDate", False), ("lineNumber", False)]

    # The return itself. Composite key over a SCHEMA-qualified table, the enum through a
    # transformer, both derived properties and the qualified one.
    lines = Spec("stress::LE0_ExposureLines", "/stress/le0",
                 "Every large-exposure line: the three-column key, the enum-mapped exposure "
                 "class, what credit risk mitigation bought, the headroom to the 25% limit "
                 "and the net exposure converted at a supplied rate. The table is "
                 "schema-qualified -- the first one in this corpus any mapping reads -- and "
                 "carries SMALLINT, CHAR, NUMERIC and REAL columns, four types nothing had "
                 "declared before.",
                 "largeexp::ExposureLine")
    lines.projections = KEY + [
        Proj("counterpartyId", ["counterpartyId"]),
        Proj("groupId", ["groupId"]),
        Proj("countryCode", ["countryCode"]),
        Proj("exposureClass", ["exposureClass"]),
        Proj("grossExposure", ["grossExposure"]),
        Proj("netExposure", ["netExposure"]),
        Proj("pctOfCapital", ["pctOfCapital"]),
        Proj("mitigationRatio", ["mitigationRatio"]),
        Proj("headroomToLimit", ["headroomToLimit"]),
        Proj("netExposureGbp", ["netExposureIn"], args=[0.79]),
        Proj("reportedAt", ["reportedAt"]),
        Proj("isExempt", ["isExempt"])]
    lines.sort = KEY_SORT
    out.append(lines)

    # The RANGE join, from the composite key out to the band. Two lines are above 25% and
    # one is at exactly 0.0, which is the floor of the first band -- the boundary a
    # half-open range gets wrong first.
    banded = Spec("stress::LE1_BandedExposures", "/stress/le1",
                  "Each line with the threshold band its percentage falls in, reached by a "
                  "RANGE join from a three-column key: `pct >= floor and pct < ceiling`, "
                  "with no key to join on because which band applies is a property of the "
                  "number. One line sits at exactly 0.0, the floor of the first band, which "
                  "is the boundary a half-open range gets wrong first.",
                  "largeexp::ExposureLine")
    banded.projections = KEY + [
        Proj("pctOfCapital", ["pctOfCapital"]),
        Proj("bandId", ["thresholdBand", "bandId"]),
        Proj("bandName", ["thresholdBand", "bandName"]),
        Proj("isReportable", ["thresholdBand", "isReportable"]),
        Proj("isBreach", ["thresholdBand", "isBreach"])]
    banded.sort = KEY_SORT
    out.append(banded)

    # The DISJUNCTION, asked as a count because it is genuinely to-many: a rule attaches to
    # a counterparty or to a country, and a line can match on both axes.
    exempt = Spec("stress::LE2_ExemptionMatches", "/stress/le2",
                  "How many exemption rules reach each line, over a join that matches on "
                  "counterparty OR on country. A rule carries one or the other and never "
                  "both, so the `or` is a real disjunction. Quarantined under F6: five of "
                  "the twelve lines match no rule at all and count() over an empty to-many "
                  "returns 1. LE6 counts the same join from the rule side, where every rule "
                  "reaches a line, and passes.",
                  "largeexp::ExposureLine")
    exempt.projections = KEY + [
        Proj("counterpartyId", ["counterpartyId"]),
        Proj("countryCode", ["countryCode"]),
        Proj("isExempt", ["isExempt"]),
        Proj("matchingRules", ["exemptionRules"], agg="count")]
    exempt.sort = KEY_SORT
    out.append(exempt)

    # The ~filter subtype: ten lines of twelve, so the filter is doing real work.
    reportable = Spec("stress::LE3_ReportableLines", "/stress/le3",
                      "Only the lines at or above the 5% reporting threshold, through the "
                      "subtype's ~filter over a schema-qualified table. Ten of twelve, so a "
                      "filter that had stopped working would return twelve with no error.",
                      "largeexp::ReportableLine")
    reportable.projections = KEY + [
        Proj("pctOfCapital", ["pctOfCapital"]),
        Proj("exposureClass", ["exposureClass"]),
        Proj("netExposure", ["netExposure"])]
    reportable.sort = KEY_SORT
    out.append(reportable)

    # ~distinct with a dynafunction beside it: twelve lines collapse to three returns.
    headers = Spec("stress::LE4_ReturnHeaders", "/stress/le4",
                   "The distinct (report, cob date) pairs, read ~distinct off the line "
                   "table. Twelve rows collapse to three, and the reference beside them is "
                   "built by a dynafunction in the mapping rather than stored.",
                   "largeexp::ReturnHeader")
    headers.projections = [Proj("reportId", ["reportId"]),
                           Proj("cobDate", ["cobDate"]),
                           Proj("returnRef", ["returnRef"])]
    headers.sort = [("reportId", False), ("cobDate", False)]
    out.append(headers)

    # Per-return aggregates over a composite-key table, with a group whose groupId is NULL.
    totals = Spec("stress::LE5_ReturnTotals", "/stress/le5",
                  "Per return: how many lines, the largest single exposure, the total net "
                  "and the mean percentage. Grouped over a schema-qualified composite-key "
                  "table, and three of the twelve lines belong to no connected group at "
                  "all -- the null a grouping rule has to survive.",
                  "largeexp::ExposureLine")
    totals.projections = [Proj("reportId", ["reportId"]), Proj("netExposure", ["netExposure"])]
    totals.group_by = ["reportId"]
    totals.aggs = [("lineCount", "netExposure", "count"),
                   ("largestExposure", "netExposure", "max"),
                   ("smallestExposure", "netExposure", "min"),
                   ("totalNet", "netExposure", "sum"),
                   ("meanNet", "netExposure", "average")]
    totals.sort = ("reportId", False)
    out.append(totals)

    # The rule table from the other side, with both null halves visible.
    rules = Spec("stress::LE6_ExemptionRules", "/stress/le6",
                 "The exemption rules themselves. Each carries a counterparty or a country "
                 "and never both, so exactly one of the two columns is null on every row -- "
                 "which is what makes the join behind LE2 a disjunction rather than two "
                 "conditions that happen to agree.",
                 "largeexp::ExemptionRule")
    rules.projections = [Proj("ruleId", ["ruleId"]),
                         Proj("exemptCounterpartyId", ["exemptCounterpartyId"]),
                         Proj("exemptCountryCode", ["exemptCountryCode"]),
                         Proj("basis", ["basis"]),
                         Proj("linesReached", ["exemptedLines"], agg="count")]
    rules.sort = ("ruleId", False)
    out.append(rules)

    return out


LARGE_EXPOSURES = _large_exposure_specs()


def _project_link_specs():
    """The dependency graph, executing.

    The 56 projects in projects/ are compile-only: no data, no runtimes, no services. That
    proves a cross-project reference COMPILES and stops there -- and three of this session's
    findings (F50, F51, F53) compile perfectly and fail at execution, so the gap is real
    rather than tidy.

    These are the first services that run against a project. Two directions, testing
    different things:

      PL0-PL1  rooted at a PROJECT class, over seeded project data. The first rows anything
               in projects/ has produced.
      PL2-PL4  rooted at a CORPUS class, reaching a project attribute across the boundary --
               which is the question a user actually asks of a dependency, and the one no
               compile check can answer.

    The join is a RANGE on purpose. A key equality that lowers wrongly returns nothing and is
    obvious; a range that lowers wrongly returns the WRONG BAND, which is a plausible number
    in the right shape.
    """
    out = []

    # Rooted at the project's own class. Nothing in projects/ had ever produced a row.
    ladder = Spec("stress::PL0_TenorLadder", "/stress/pl0",
                  "Every tenor bucket in the linked project's ladder, read from the corpus. "
                  "The first service in this corpus rooted at a class that belongs to a "
                  "PROJECT rather than to the corpus, and the first rows anything in "
                  "projects/ has produced -- the graph is compile-only by design.",
                  "core_tenor::CtnTenorBucket")
    ladder.projections = [Proj("bucketId", ["bucketId"]),
                          Proj("band", ["band"]),
                          Proj("label", ["label"]),
                          Proj("minDays", ["minDays"]),
                          Proj("maxDays", ["maxDays"]),
                          Proj("spanDays", ["spanDays"]),
                          Proj("sortOrder", ["sortOrder"])]
    ladder.sort = ("bucketId", False)
    out.append(ladder)

    # A project class navigating a project association, executed from the corpus. The enum
    # comes through the project's own EnumerationMapping, which nothing had exercised.
    laddered = Spec("stress::PL1_BucketLadder", "/stress/pl1",
                    "Each bucket with the ladder it belongs to -- a navigation entirely "
                    "INSIDE the linked project, executed from the corpus. The band is an "
                    "enum resolved by the project's own EnumerationMapping, which until now "
                    "had been compiled and never run.",
                    "core_tenor::CtnTenorBucket")
    laddered.projections = [Proj("bucketId", ["bucketId"]),
                            Proj("band", ["band"]),
                            Proj("ladderName", ["ladder", "name"]),
                            Proj("dayCount", ["ladder", "dayCountConvention"])]
    laddered.sort = ("bucketId", False)
    out.append(laddered)

    # THE BOUNDARY, in the direction a user asks it: a corpus root reaching a project
    # attribute. 192 pillars across nine bands.
    banded = Spec("stress::PL2_PillarTenorBand", "/stress/pl2",
                  "Every curve pillar with the tenor band it falls in -- a CORPUS class "
                  "reaching an attribute of a PROJECT class, across a range join. This is "
                  "the query a dependency exists to allow, and the one a compile check "
                  "cannot answer: a range that lowers wrongly returns the wrong band rather "
                  "than nothing, which is a plausible number in the right shape.",
                  "curves::CurvePoint")
    banded.projections = [Proj("curveId", ["curveId"]),
                          Proj("cobDate", ["cobDate"]),
                          Proj("tenorLabel", ["tenorLabel"]),
                          Proj("tenorDays", ["tenorDays"]),
                          Proj("bandCode", ["tenorBucket", "band"]),
                          Proj("bandLabel", ["tenorBucket", "label"]),
                          Proj("bandMinDays", ["tenorBucket", "minDays"]),
                          Proj("bandMaxDays", ["tenorBucket", "maxDays"])]
    banded.sort = [("curveId", False), ("cobDate", False), ("tenorLabel", False)]
    out.append(banded)

    # WITHDRAWN: the same boundary reached from a ~filter SUBTYPE.
    #
    # `curves::QuotedPillar` is a subtype of `curves::CurvePoint`, and the association to the
    # linked project's bucket names the BASE's set id -- there is one association and its ends
    # already name a source and a target. Rooting a query at the subtype set fails with
    # `Void not supported!`, which is F49, now shown to survive a project boundary: the
    # association's far end is in a different project entirely and the failure is identical.
    #
    # QUARANTINED IS NOT ENOUGH. The failure is an exception during test-suite
    # INITIALISATION, which kills the JVM the batch runs in -- so the service does not report
    # FAIL, it reports nothing, and neither do the other seventy-nine services in its batch.
    # A quarantine entry can only hold a service that fails; this one has to be absent. Same
    # shape as graph-fetch-over-~distinct, which spread.py excludes for the same reason.
    #
    # PL2 asks the identical question from the BASE and passes, so the boundary itself is
    # sound and nothing about it goes untested.

    # And the boundary UNDER an aggregate: group the corpus's pillars by a project's band.
    # A group-by whose key comes from the other side of the boundary.
    byband = Spec("stress::PL4_PillarsPerBand", "/stress/pl4",
                  "How many curve pillars fall in each tenor band, and the rate range within "
                  "it. The grouping key is a PROJECT attribute reached across the boundary, "
                  "so this is a group-by whose key the corpus does not own. Two of the nine "
                  "bands hold no pillar at all and are absent rather than zero.",
                  "curves::CurvePoint")
    byband.projections = [Proj("bandCode", ["tenorBucket", "band"]),
                          Proj("zeroRate", ["zeroRate"])]
    byband.group_by = ["bandCode"]
    byband.aggs = [("pillars", "zeroRate", "count"),
                   ("highestRate", "zeroRate", "max"),
                   ("lowestRate", "zeroRate", "min")]
    byband.sort = ("bandCode", False)
    out.append(byband)

    # ---- core-fx: a cross-project FUNCTION CALL, executed ----
    #
    # projects/ compiles function calls across a boundary and has never run one. Both
    # derived properties here call `core_fx::convert`, which lives in another project, on a
    # rate reached across the boundary -- and the second one converts back, so a wrong
    # lowering shows up as a number that does not round-trip rather than as a plausible one.
    priced = Spec("stress::PL5_ConvertedNotional", "/stress/pl5",
                  "Each trade's notional converted through a PROJECT's FX rate by a PROJECT's "
                  "function, and converted back. The round trip is the assertion: a wrong "
                  "rate or a wrong lowering gives a number that does not return to where it "
                  "started, where a single conversion would just look plausible.",
                  "projectlink::PricedTrade")
    priced.projections = [Proj("tradeId", ["tradeId"]),
                          Proj("notional", ["notional"]),
                          Proj("fxMid", ["fxMid"]),
                          Proj("notionalConverted", ["notionalConverted"]),
                          Proj("backConverted", ["backConverted"])]
    priced.sort = ("tradeId", False)
    out.append(priced)

    # ---- core-ratings: MILESTONING across the boundary ----
    #
    # `all(%date)` on a class in another project. Three dates, three answers, and the middle
    # one is the whole point -- CP-0001 is A before 1 May and A- after, so a boundary that
    # dropped the temporal predicate would return both rows on every date and look busy.
    for n, (as_of, doc) in enumerate([
        ("2024-02-01",
         "Ratings as at 1 February, read from a MILESTONED class in another project. CP-0001 "
         "is A and CP-0003 is still investment grade at BBB-."),
        ("2024-08-01",
         "The SAME query six months later: CP-0001 is A- and CP-0003 has fallen to BB+. "
         "Nothing about the query changes but the date -- and if the boundary dropped the "
         "milestoning predicate, nothing about the ANSWER would change either, which is "
         "exactly what this pins."),
        ("2023-06-01",
         "Before any of these ratings existed. Every row is excluded by the temporal "
         "predicate, so the result is empty -- absent, not null.")]):
        r = Spec(f"stress::PL{6 + n}_RatingsAsOf", f"/stress/pl{6 + n}", doc,
                 "core_ratings::RatingVersion")
        r.as_of = as_of
        if as_of == "2023-06-01":
            r.emptiness = emptiness.BEFORE_FIRST_VERSION
        r.projections = [Proj("entityId", ["entityId"]),
                         Proj("rating", ["rating"]),
                         Proj("agency", ["agency"])]
        r.sort = ("entityId", False)
        out.append(r)

    # The same milestoned class reached FROM a corpus class. Two of the five counterparties
    # are unrated, so the navigation lands on nothing for them -- asked as a count, since
    # every counterparty here has at least one rating or the F6 empty-set case would apply.
    rated = Spec("stress::PL9_CounterpartyRatingCount", "/stress/pl9",
                 "How many rating versions each counterparty has ever had, reaching a "
                 "MILESTONED class in another project from a CORPUS root. Counted rather "
                 "than projected because the navigation is to-many; two counterparties have "
                 "no rating at all, which is F6's empty-set case and why they are filtered "
                 "out rather than shown as zero.",
                 "counterparty::Counterparty")
    # The date is REQUIRED, not optional: navigating a milestoned property without one
    # fails at compile with "requires date parameters: [businessDate]". credit-core reported
    # that from projects/, where it could only be compiled; this is the same rule holding at
    # execution, across a project boundary.
    rated.projections = [Proj("counterpartyId", ["counterpartyId"]),
                         Proj("legalName", ["legalName"]),
                         Proj("versions", ["ratingVersions"], agg="count",
                              args=[DateArg("2024-08-01")])]
    rated.filters = [Pred(["counterpartyId"], "<", "CP-0004")]
    rated.sort = ("counterpartyId", False)
    out.append(rated)

    # ---- fee-core: the first linked project that DEPENDS ON another linked project ----
    #
    # Every seeded DAYS_TO_MATURITY is a band boundary of core-tenor's ladder or is outside
    # every band -- 7 and 8 straddle CTN-01/CTN-02, 365 and 366 straddle CTN-05/CTN-06, 3654
    # is exactly CTN-09's first day, and 40000 is past its last. A midpoint would pass
    # whether the range join is half-open, closed, or off by one; these do not.
    #
    # The band is asserted, not the ladder name: a wrong lowering of a RANGE join returns a
    # plausible neighbouring band, which is the failure that looks like a correct answer.
    band = Spec("stress::PL10_FeeScheduleTenorBand", "/stress/pl10",
                "Which core-tenor band each fee schedule falls in, resolved by a RANGE join "
                "declared in fee-core over a table it only includes -- a project-to-project "
                "edge executed. Every seeded tenor is a band boundary or outside every band, "
                "so an off-by-one in the half-open range changes an answer here. The last "
                "row matches no band at all and must read null rather than the nearest one.",
                "fee_core::FeeSchedule")
    band.projections = [Proj("productCode", ["productCode"]),
                        Proj("tierCode", ["tierCode"]),
                        Proj("effectiveDate", ["effectiveDate"]),
                        Proj("days", ["daysToMaturity"]),
                        Proj("bucketId", ["bucket", "bucketId"]),
                        # An ENUM read through core-tenor's own EnumerationMapping, at the
                        # far end of a range join declared in a third project.
                        Proj("band", ["bucket", "band"]),
                        Proj("bandLabel", ["bucket", "label"])]
    # The composite key, in its declared order -- and it takes all three to order these rows,
    # because the same product and tier appear twice and so do the same product and date.
    band.sort = ("productCode", False)
    out.append(band)

    # A derived and a QUALIFIED property whose bodies both call a function belonging to
    # core-types -- a project the corpus does not link for itself and reaches only because
    # fee-core depends on it. `grossFee` takes an argument, which no other linked-project
    # service exercises.
    fees = Spec("stress::PL11_FeeScheduleGrossFee", "/stress/pl11",
                "A derived property and a qualified property TAKING AN ARGUMENT, both "
                "calling core_types::ctBasisPointsToRate across two project boundaries at "
                "once: the corpus links fee-core, and fee-core is why core-types is here at "
                "all. The oracle reimplements that function from core-types' own source "
                "rather than reading the engine's answer back.",
                "fee_core::FeeSchedule")
    fees.projections = [Proj("productCode", ["productCode"]),
                        Proj("tierCode", ["tierCode"]),
                        Proj("effectiveDate", ["effectiveDate"]),
                        Proj("bps", ["rateBasisPoints"]),
                        Proj("rate", ["rate"]),
                        Proj("grossOnMillion", ["grossFee"], args=[1000000.0])]
    fees.sort = ("productCode", False)
    out.append(fees)

    # ---- core-instrument: a three-level subtype hierarchy over ONE wide table ----
    #
    # The root set carries no ~filter, so this is every row whatever it turns out to be --
    # including the equity with no subtype, which belongs to Equity and to neither of
    # Equity's own subtypes. Both enum transformers are MANY-TO-ONE ('EQ' and 'EQUITY' both
    # mean EQUITY, 'A' and 'ACTIVE' both mean ACTIVE) and both forms are seeded, so a
    # transformer that had lost half its entries would show up here as a null.
    census = Spec("stress::PL12_InstrumentTypeCensus", "/stress/pl12",
                  "Every row of a project's instrument master through its UNFILTERED root "
                  "set, with both enum transformers resolved. The asset class and the "
                  "status are each seeded in both of their accepted source forms, because a "
                  "many-to-one enumeration mapping that had lost half its entries reads as "
                  "a null rather than as an error.",
                  "core_instrument::Instrument")
    census.projections = [Proj("instrumentId", ["instrumentId"]),
                          Proj("instrumentType", ["instrumentType"]),
                          Proj("subType", ["instrumentSubType"]),
                          Proj("assetClass", ["assetClass"]),
                          Proj("status", ["status"]),
                          Proj("currency", ["currency"])]
    census.sort = ("instrumentId", False)
    out.append(census)

    # A THIRD-level set: ciConvertibleBond extends ciBond extends ciBase. It declares four
    # columns of its own and inherits the rest through two levels of `extends`, so this
    # reads a property mapping from each level of the chain in one projection.
    conv = Spec("stress::PL13_ConvertibleBondTerms", "/stress/pl13",
                "A THIRD-level subtype set -- ciConvertibleBond extends ciBond extends "
                "ciBase -- projecting one property from each level: the name from the root "
                "set, the coupon and its enum from the middle one, the conversion terms "
                "from its own. An `extends` that failed to inherit property mappings would "
                "leave the first two null while the third stayed right.",
                "core_instrument::ConvertibleBond")
    conv.projections = [Proj("instrumentId", ["instrumentId"]),
                        Proj("name", ["name"]),
                        Proj("couponRate", ["couponRate"]),
                        Proj("couponType", ["couponType"]),
                        Proj("conversionRatio", ["conversionRatio"]),
                        Proj("conversionPrice", ["conversionPrice"])]
    conv.sort = ("instrumentId", False)
    out.append(conv)

    # The one subtype told apart by a column that is NOT the subtype discriminator: the call
    # filter tests PUT_CALL. Every seeded PUT_CALL sits on a row whose type really is OPTION
    # -- see F56 for what happens when it does not, and why no such row is seeded here.
    calls = Spec("stress::PL14_CallOptionStrikes", "/stress/pl14",
                 "A subtype set whose ~filter tests a column unrelated to the subtype "
                 "discriminator -- PUT_CALL rather than INSTRUMENT_SUBTYPE. Its sibling "
                 "PutOption reads the same table through the opposite predicate, so the two "
                 "partition the options and neither may return the other's row.",
                 "core_instrument::CallOption")
    calls.projections = [Proj("instrumentId", ["instrumentId"]),
                         Proj("strikePrice", ["strikePrice"]),
                         Proj("optionStyle", ["optionStyle"]),
                         Proj("underlyingInstrumentId", ["underlyingInstrumentId"])]
    calls.sort = ("instrumentId", False)
    out.append(calls)

    # ---- core-calendar: a VIEW as a mapped class, and functions over a CLASS ----
    #
    # CC_HOLIDAY_COUNT is a View with a ~groupBy. No DDL is created for it and nothing seeds
    # it -- the engine folds the GROUP BY into the SQL -- so the oracle aggregates CC_HOLIDAY
    # itself and the two have to agree about grouping, about counting, and about what happens
    # to a calendar that forms no group at all. JPTO has no holiday rows, so it must be
    # ABSENT here rather than present with a count of zero, which is the one thing a
    # ~groupBy view does that an outer join would not.
    counts = Spec("stress::PL15_HolidayCountView", "/stress/pl15",
                  "A VIEW with a ~groupBy, mapped as a class's ~mainTable and read across a "
                  "project boundary. Nothing seeds it: the engine folds the GROUP BY into "
                  "the SQL and the oracle aggregates the underlying rows independently. One "
                  "calendar has holidays in two years, one in a single year, and one has "
                  "none at all and so forms no group -- which is the difference between an "
                  "aggregation and an outer join, and is invisible unless a seed contains "
                  "the empty case.",
                  "core_calendar::CcHolidayCount")
    counts.projections = [Proj("calendarId", ["calendarId"]),
                          Proj("holidayYear", ["holidayYear"]),
                          Proj("holidayCount", ["holidayCount"]),
                          # A function whose PARAMETER IS A CLASS, called extension-style on
                          # the root: `$x->core_calendar::ccBusinessDaysInYear()`. Every
                          # cross-project function executed so far takes numbers.
                          Proj("businessDays", [], None, [],
                               "core_calendar::ccBusinessDaysInYear")]
    counts.sort = ("calendarId", False)
    out.append(counts)

    # Two hops of to-one navigation INSIDE a dependency -- cycle to market to calendar --
    # rather than from the corpus into one. Plus both class-parameter predicates, on a seed
    # where T+1 and T+2 both occur: a predicate that is true everywhere asserts nothing.
    cycles = Spec("stress::PL16_SettlementCycleChain", "/stress/pl16",
                  "Two hops of navigation entirely WITHIN a dependency, and two functions "
                  "whose parameter is a class rather than a number. The settlement lag is "
                  "read both as a column and through the project's own accessor, so a "
                  "function that lowered wrongly would disagree with the column beside it.",
                  "core_calendar::CcSettlementCycle")
    cycles.projections = [Proj("cycleId", ["cycleId"]),
                          Proj("assetClass", ["assetClass"]),
                          Proj("settlementDays", ["settlementDays"]),
                          Proj("mic", ["market", "mic"]),
                          Proj("calendarId", ["market", "calendar", "calendarId"]),
                          Proj("centre", ["market", "calendar", "financialCentre"]),
                          Proj("lag", [], None, [],
                               "core_calendar::ccSettlementLagDays"),
                          Proj("isTPlusTwo", [], None, [],
                               "core_calendar::ccIsTPlusTwo")]
    cycles.sort = ("cycleId", False)
    out.append(cycles)

    # ---- core-account: nested embedded, and joins that need BOTH key columns ----
    #
    # `ownership.taxResidence.countryCode` is two levels of embedded on ONE row -- three
    # classes read from CA_ACCOUNT with no join at all, which the corpus has nowhere else.
    # `correspondence` is a SIBLING block after that nested one, which is the shape the
    # reader used to lose entirely.
    #
    # The joins are the other half. ACCOUNT_NO "0001" exists under both institutions and
    # BRANCH_CODE "MAIN" does too, so a join on one column of the two-column key returns a
    # WRONG row rather than no row: INST-DE/0001 must read Frankfurt and not London.
    # Account 0003 points at a branch code that exists only under the OTHER institution, so
    # its branch must be null -- a one-column join would find one.
    acct = Spec("stress::PL19_AccountEmbeddedAndKeys", "/stress/pl19",
                "Two levels of EMBEDDED on one row, a sibling block after the nested one, "
                "and two joins that each need BOTH columns of a composite key. The seed "
                "reuses account numbers and branch codes across institutions, so a join "
                "that dropped a column would return a plausible row from the wrong bank "
                "rather than nothing.",
                "core_account::Account")
    acct.projections = [Proj("institutionId", ["institutionId"]),
                        Proj("accountNo", ["accountNo"]),
                        Proj("ownerName", ["ownership", "ownerName"]),
                        # The nested block, two levels down, still on the same row.
                        Proj("taxCountry", ["ownership", "taxResidence", "countryCode"]),
                        # The sibling block that follows the nested one.
                        Proj("corrCity", ["correspondence", "city"]),
                        Proj("institutionName", ["institution", "name"]),
                        Proj("branchName", ["branch", "name"]),
                        Proj("branchCity", ["branch", "city"])]
    acct.sort = [("institutionId", False), ("accountNo", False)]
    out.append(acct)

    # The same embedded construct on a different class, reached through a THREE-column key,
    # plus a navigation back to the account the mandate hangs off.
    mand = Spec("stress::PL20_MandateHolder", "/stress/pl20",
                "An embedded holder block on a row keyed by THREE columns, and a navigation "
                "back to the account it belongs to over a two-column join. One mandate has "
                "no named holder at all, so the embedded block is present as a shape and "
                "empty of values -- which is a different thing from the mandate having no "
                "holder property.",
                "core_account::AccountMandate")
    mand.projections = [Proj("institutionId", ["institutionId"]),
                        Proj("accountNo", ["accountNo"]),
                        Proj("mandateSeq", ["mandateSeq"]),
                        Proj("role", ["role"]),
                        Proj("holderName", ["holder", "holderName"]),
                        Proj("holderCountry", ["holder", "countryCode"]),
                        Proj("accountName", ["account", "accountName"])]
    mand.sort = [("institutionId", False), ("accountNo", False), ("mandateSeq", False)]
    out.append(mand)

    # ---- core-geo: the first linked project whose edges are ASSOCIATIONS ----
    #
    # Nine linked projects model every edge as a class-typed property over a join. This one
    # declares ten Associations with mapped ends -- Legend's other edge style -- and until
    # now the corpus had never executed one that belonged to a dependency. F49 and F57 are
    # both defects in association ends, which is the reason this was worth linking.
    #
    # Three hops of it in one chain, and a ONE-TO-ONE at the end of another. Reykjavik has
    # no time zone and New Zealand has no profile row, so both to-ones land on nothing in a
    # projection that otherwise resolves.
    geo = Spec("stress::PL21_CityRegionChain", "/stress/pl21",
               "Three hops through ASSOCIATIONS declared inside a dependency -- city to "
               "country to sub-region to macro-region -- alongside a one-to-one to the "
               "country profile. Every edge here is an Association with a mapped end, "
               "which nine previously linked projects between them do not have one of. "
               "One city has no time zone and one country has no profile, so both to-ones "
               "land on nothing without the chain itself breaking.",
               "core_geo::CgCity")
    geo.projections = [Proj("cityId", ["cityId"]),
                       Proj("cityName", ["name"]),
                       Proj("timeZone", ["timeZone", "ianaName"]),
                       Proj("country", ["country", "name"]),
                       Proj("subRegion", ["country", "subRegion", "name"]),
                       Proj("macroRegion", ["country", "subRegion", "macroRegion", "name"]),
                       Proj("incomeGroup", ["country", "profile", "incomeGroup"])]
    geo.sort = ("cityId", False)
    out.append(geo)

    # A MANY-TO-MANY through a link table: two associations end to end, in opposite
    # directions, from the row that joins them. The UK's EU membership carries an exit date
    # and a WITHDRAWN status, so a reader that ignored the link row's own columns would
    # report the UK as an EU member.
    blocs = Spec("stress::PL22_BlocMembership", "/stress/pl22",
                 "A many-to-many resolved through its LINK TABLE: two associations end to "
                 "end from the membership row, one to the country and one to the bloc. The "
                 "link row carries its own state -- one membership has an exit date and a "
                 "WITHDRAWN status -- which is the whole reason the relationship is a table "
                 "rather than a join.",
                 "core_geo::CgBlocMembership")
    blocs.projections = [Proj("membershipId", ["membershipId"]),
                         Proj("status", ["status"]),
                         Proj("exitDate", ["exitDate"]),
                         Proj("country", ["country", "name"]),
                         Proj("countryRegion", ["country", "subRegion", "name"]),
                         Proj("bloc", ["bloc", "name"]),
                         Proj("blocType", ["bloc", "blocType"])]
    blocs.sort = ("membershipId", False)
    out.append(blocs)

    # ---- index-core: TWO already-executable parents from one row ----
    #
    # fee-core has one dependency and forms a diamond with it. This class joins into
    # core-instrument AND core-geo by two different keys, so a single constituent row
    # reaches two separate projects -- and one of each pair is seeded to LAND while the
    # other is seeded to miss, so neither can be right by accident: IDX-C-004 names an
    # instrument core-instrument does not have, IDX-C-006 names a country core-geo does not.
    cons = Spec("stress::PL23_ConstituentTwoParents", "/stress/pl23",
                "One row reaching TWO different dependencies by two different keys, plus a "
                "hop up its own project to the index. One constituent names an instrument "
                "that does not exist and another names a country that does not, so each "
                "cross-project join has a case that lands and a case that does not -- and "
                "the two cases are on different rows, so a join wired to the wrong project "
                "cannot pass by coincidence.",
                "index_core::IdxConstituent")
    cons.projections = [Proj("constituentId", ["constituentId"]),
                        Proj("weightPct", ["weightPct"]),
                        # into core-instrument
                        Proj("instrumentName", ["instrument", "name"]),
                        Proj("assetClass", ["instrument", "assetClass"]),
                        # into core-geo, and one hop further up ITS chain
                        Proj("issuerCountry", ["issuerCountry", "name"]),
                        Proj("issuerSubRegion", ["issuerCountry", "subRegion", "name"]),
                        # and back up index-core's own chain
                        Proj("indexName", ["rebalance", "index", "name"])]
    cons.sort = ("constituentId", False)
    out.append(cons)

    # A ~groupBy VIEW joined ACROSS a project boundary. Nothing seeds it: the engine folds
    # the GROUP BY into the SQL and the oracle aggregates the constituent rows itself. The
    # rebalance with no constituents forms NO group rather than a group of zero, and the
    # group keyed on "FR" reaches core-geo and lands on nothing.
    cw = Spec("stress::PL24_CountryWeightView", "/stress/pl24",
              "An aggregating VIEW whose grouping key is the foreign key into ANOTHER "
              "project, so the rollup is joined across the boundary and can then be walked "
              "up that project's own chain. Nothing seeds a view. One group's key is a "
              "country core-geo does not carry, so the join out of the aggregate lands on "
              "nothing; a rebalance with no constituents forms no group at all.",
              "index_core::IdxCountryWeight")
    cw.projections = [Proj("rebalanceId", ["rebalanceId"]),
                      Proj("issuerCountryCode", ["issuerCountryCode"]),
                      Proj("totalWeightPct", ["totalWeightPct"]),
                      Proj("constituentCount", ["constituentCount"]),
                      Proj("countryName", ["country", "name"]),
                      Proj("indexCode", ["rebalance", "index", "code"])]
    cw.sort = [("rebalanceId", False), ("issuerCountryCode", False)]
    out.append(cw)

    # ---- core-units: two joins to the SAME table, and exact decimals in a schema ----
    #
    # CuConversion reaches CuUnit twice -- once as fromUnit, once as toUnit -- over two
    # different joins into one table. F51 and F52 are both in that neighbourhood, and this
    # is the first time the corpus has executed the shape across a project boundary.
    #
    # The factors are NUMERIC(20,8) and are definitions rather than measurements: 0.45359237
    # IS the international pound. A conversion carried through a double and back would round
    # somewhere in the eighth place, which is precisely what these columns exist to prevent.
    conv = Spec("stress::PL17_UnitConversionPair", "/stress/pl17",
                "A conversion and its inverse are two rows under a composite key on an "
                "ORDERED pair, and each reaches the unit table TWICE over two different "
                "joins -- once as the source unit and once as the target. The factors are "
                "NUMERIC(20,8), so a value that went through a double would differ in the "
                "eighth place; the temperature rows carry a non-zero offset, one of them "
                "negative, which a conversion written as a bare multiply gets wrong and "
                "nothing else does.",
                "core_units::CuConversion")
    conv.projections = [Proj("fromUnitCode", ["fromUnitCode"]),
                        Proj("toUnitCode", ["toUnitCode"]),
                        Proj("factor", ["factor"]),
                        Proj("offsetValue", ["offsetValue"]),
                        # The same table, reached two different ways, in one projection.
                        Proj("fromName", ["fromUnit", "unitName"]),
                        Proj("toName", ["toUnit", "unitName"]),
                        Proj("converted", ["convert"], None, [100.0])]
    conv.sort = ("fromUnitCode", False)
    out.append(conv)

    # A qualified property that CONCATENATES -- F54's shape -- but on a chain that always
    # lands, plus a derived Boolean and a qualified property taking a number. The unit table
    # lives in the `uom` SCHEMA, so every column here is reached as [db]uom.TABLE.COL.
    units = Spec("stress::PL18_UnitLabels", "/stress/pl18",
                 "Every unit with its label built by a qualified property that "
                 "concatenates, its base-unit predicate, and a quantity converted to the "
                 "base unit. The tables are in a SCHEMA rather than the default one, and "
                 "the navigation to the quantity kind crosses out of it and back.",
                 "core_units::CuUnit")
    units.projections = [Proj("unitCode", ["unitCode"]),
                         Proj("label", ["label"]),
                         Proj("isBaseUnit", ["isBaseUnit"]),
                         Proj("factorToBase", ["factorToBase"]),
                         Proj("inBase", ["toBase"], None, [2.5]),
                         Proj("kindName", ["quantityKind", "kindName"]),
                         Proj("isRatio", ["quantityKind", "isRatioScale"])]
    units.sort = ("unitCode", False)
    out.append(units)

    return out


PROJECT_LINK = _project_link_specs()


SPECS = (STACK + INVARIANCE + AGGREGATION
         + [XSTORE, XSTORE_PROJECTION, MODELJOIN, MEASURE,
            CANONICAL_WITH_ENUM, OTHERWISE, CONFLUENCE]) + FIXED_INCOME + OTC + RISK + MIDDLE_OFFICE + BACK_OFFICE + MARKET_DATA + CURVES + BROKERAGE + SCHEDULE + LARGE_EXPOSURES + PROJECT_LINK + TEMPORAL + BITEMPORAL + GRAPH + ROLLUP + SELF_JOIN + DERIVED + [
    _spec(0, "InstrumentChildCounts", "products::Instrument",
          "Fan-out: per-instrument child counts. INST-NESN is childless on every end, "
          "which is the count-over-outer-join case.",
          [("instrumentId", "instrumentId"), ("name", "name"), ("ticker", "ticker"),
           ("assetClass", "assetClass"), ("isActive", "isActive")],
          [("tradeCount", "trades"), ("positionCount", "positions"),
           ("greeksCount", "instrumentGreeks")]),

    _spec(1, "CounterpartyChildCounts", "counterparty::Counterparty",
          "Fan-out: per-counterparty exposure. CP-0005 has no trades, settlements or "
          "collateral agreements; CP-0004 has trades but no CSA.",
          [("counterpartyId", "counterpartyId"), ("legalName", "legalName"),
           ("tier", "tier"), ("isActive", "isActive"),
           ("countryName", "country.name")],
          [("tradeCount", "trades"), ("settlementCount", "settlementsWithCpty"),
           ("csaCount", "collateralAgreements")]),

    _spec(2, "BookChildCounts", "positions::Book",
          "Fan-out: per-book activity. BK-LEGACY is closed and has neither trades nor "
          "PnL rows, while still holding one position.",
          [("bookId", "bookId"), ("name", "name"), ("currency", "currency"),
           ("isActive", "isActive"), ("deskName", "desk.name"),
           ("deskRegion", "desk.region")],
          [("tradeCount", "trades"), ("positionCount", "positions"),
           ("pnlCount", "dailyPnLs")]),

    _spec(3, "DeskChildCounts", "org::Desk",
          "Fan-out: desk rollup. DSK-CREDIT has no books, no traders and no PnL — three "
          "independent zeroes on one row.",
          [("deskId", "deskId"), ("name", "name"), ("region", "region"),
           ("assetClass", "assetClass"), ("isActive", "isActive")],
          [("bookCount", "books"), ("traderCount", "traders"),
           ("pnlCount", "dailyPnLs")]),

    _spec(4, "SectorInstrumentCounts", "refdata::Sector",
          "Fan-out across a link that is itself broken elsewhere: SEC-55 has no "
          "instruments, and INST-GILT30 points at a sector id that does not exist, so it "
          "is counted under no sector at all.",
          [("sectorId", "sectorId"), ("name", "name"), ("gicsCode", "gicsCode"),
           ("isActive", "isActive")],
          [("instrumentCount", "instruments")]),

    _spec(5, "TraderChildCounts", "org::Trader",
          "Fan-out with a NULL foreign key on the child side: two trades carry no trader, "
          "so they must be counted under no trader rather than under all of them.",
          [("traderId", "traderId"), ("firstName", "firstName"),
           ("lastName", "lastName"), ("seniority", "seniority"),
           ("isActive", "isActive"), ("deskName", "desk.name")],
          [("tradeCount", "trades"), ("pnlCount", "dailyPnLs")]),

    _spec(6, "PositionGreeksCounts", "positions::Position",
          "Fan-out on the shallowest possible association, so a failure here cannot be "
          "blamed on join depth. Three of six positions have no greeks row.",
          [("positionId", "positionId"), ("direction", "direction"),
           ("currency", "currency"), ("isOpen", "isOpen"),
           ("instrumentName", "instrument.name")],
          [("greeksCount", "greeks")]),
]
