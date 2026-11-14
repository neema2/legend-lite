"""
Test shapes (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 8): existing services given a suite of two tests, the second
with two assertions, one an EqualTo.

Every service before had one suite of one test with one EqualToJson assertion, which compares rows UNORDERED (the
non-finding in docs/UPSTREAM_FINDINGS.md): no test could tell an ordering wrong. These services sort on a key no two
of their rows share, and each gains a second test, `in_order`, serialized PURE_TDSOBJECT, asserting

  sameRows   EqualToJson, the rows the first test asserts
  inOrder    EqualTo, the serialized answer EXACTLY: the rows in the sort's order, as the engine writes them

so a suite holds two tests and a test two assertions. The expected text is the oracle's rows written as the pinned
engine's PURE_TDSOBJECT serializer writes them, compact JSON with Java's spelling of a double (emit.exact_text). A
value read from a DECIMAL column is written at the column's scale (8.0000), and one computed from it at the
database's (UPSTREAM_FINDINGS' non-findings), so the services take their doubles from DOUBLE columns alone: the
first choice of 36 held fifteen with DECIMAL columns, whose `in_order` failed on the engine for that alone, the rows
and their order the oracle's.

Probed on the pinned release (4.145.0) first. EqualTo meets a service's answer as text: under PURE_TDSOBJECT the
answer is the rows, and a graph fetch's serialize its {builder, values}; under the default format it is the result
envelope, whose executionTraceID is a fresh UUID per run (docs/SURFACE_BLOCKED.tsv's TA-01), so only those two forms
are asserted. EqualToTDS does not parse there ("Unknown test assertion type: EqualToTDS"; TST-15), so no service has
one: one unparseable test stops the whole corpus loading on the engine.

lite judges EqualToJson and skips a test holding an EqualTo ("EqualTo (a spec value) is not judged by this runner"),
so each `in_order` is SKIPPED there and the first test is judged as before.
"""
from __future__ import annotations

import decimal

# The services shaped: projections and aggregates over the taxonomies, the fan-out battery, deep stacks and the
# relation mappings, each sorted on a key no two of its rows share, its doubles read from DOUBLE columns.
SHAPED = [
    "stress::BK2_ClearingRoutes", "stress::BK3_RoutingRules", "stress::BO3_StandingInstructions",
    "stress::CV5_CurveCatalogue", "stress::CV8_CurveDates", "stress::CI_All", "stress::CUSTX_All", "stress::DOCX_All",
    "stress::EXCX_All", "stress::KYCX_All", "stress::TXB_All", "stress::TXE_All", "stress::TXI_All",
    "stress::MO5_CleanTrades", "stress::MO3_BreakAgeing", "stress::MD0_SeriesCatalogue", "stress::MR0_RunSummary",
    "stress::OT5_UncollateralisedTrades", "stress::PL12_InstrumentTypeCensus", "stress::LE6_ExemptionRules",
    "stress::D_BookDense", "stress::DSLocal_Settlement", "stress::DSLocal_OpsConfirmation",
    "stress::DSLocal_CouponPayment", "stress::DSDeep_CommodityContract", "stress::DSPost_FundingSource",
    "stress::DSAgg_CouponPeriod", "stress::AA_CurvesYieldCurveCounts", "stress::AA_ClearingRouteCounts",
    "stress::RMR_DealsTop", "stress::RMR_DealDeskTop", "stress::RM_SourcedTop", "stress::RM_CrossedDealDeskTop",
    "stress::TM_TradeEmpty", "stress::TM_BookEmpty", "stress::VGN_CountName",
]

# The second test of each, (test id, [(assertion id, kind)]): 'json' an EqualToJson of the rows, 'text' an EqualTo
# of the exact answer
TESTS = [("in_order", [("sameRows", "json"), ("inOrder", "text")])]


def _formattable(v) -> bool:
    if v is None or isinstance(v, (bool, int)):
        return True
    if isinstance(v, decimal.Decimal):
        return False    # written at the column's or the arithmetic's scale, not the seed's digits (_uncertain_floats)
    if isinstance(v, str):
        return all(32 <= ord(ch) < 127 for ch in v)
    if isinstance(v, float):
        return v == 0 or 1e-3 <= abs(v) < 1e7
    return False


def _uncertain_floats(c, spec, rows) -> list[str]:
    """The aliases holding a double whose text is not certain: the engine writes a value read from a DECIMAL(p,s) column
    as a BigDecimal at the column's scale (8.0000), and one computed from it at the database's scale for the
    arithmetic (-666400.000), so only a plain column of DOUBLE or FLOAT is spelled as Java spells a double (probed on
    4.145.0, 2026-10-31)."""
    floats = {k for r in rows for k, v in r.items() if isinstance(v, float)}
    out = []
    for p in spec.projections:
        if p.alias not in floats:
            continue
        plain = p.agg is None and not p.func and getattr(p, "apply", None) is None and not p.args
        try:
            table, col, _ = c.resolve(spec.root, p.path) if plain else (None, None, None)
        except Exception:
            table = None
        base = c.tables[table].columns[col].type.upper().split("(")[0] if table else None
        if base not in ("DOUBLE", "FLOAT"):
            out.append(p.alias)
    out += sorted(floats - {p.alias for p in spec.projections})   # an aggregate's or a relation operation's
    return out


def check(c, emitted: dict) -> list[str]:
    """Each shaped service is a projection, sorted on a key no two of its rows share and none lacks, with two rows or
    more, every value one whose text is certain (emit.exact_text, _uncertain_floats): an ordering the engine might
    break ties in another way, or a value it might spell another way, would be asserted as a guess."""
    problems = []
    for name in SHAPED:
        hit = emitted.get(name)
        if hit is None:
            problems.append(f"{name}: shaped and not emitted")
            continue
        spec, rows = hit
        keys = spec.sort if isinstance(spec.sort, list) else [spec.sort] if spec.sort else []
        if spec.graph is not None or spec.cases or spec.multi or not keys:
            problems.append(f"{name}: not a sorted projection with one test")
            continue
        order = [tuple(r.get(k) for k, _d in keys) for r in rows]
        if len(rows) < 2 or len(set(order)) != len(order) or any(None in t for t in order):
            problems.append(f"{name}: its sort is not a total order over its {len(rows)} rows")
        if not all(_formattable(v) for r in rows for v in r.values()):
            problems.append(f"{name}: a value whose serialized text is not certain")
        uncertain = _uncertain_floats(c, spec, rows)
        if uncertain:
            problems.append(f"{name}: doubles not read from a DOUBLE column, whose text is not certain: {uncertain}")
    return problems
