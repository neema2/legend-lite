"""
Mapping test suites: the mappings of 99-mapping-tests.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 8).

The corpus had no mapping test suite, EMIT's main relational test form (161 suites in 75 of its models). These
mirror existing services: each suite's function is a service's query, its data the service's ###Data bound to the
store the service's runtime connects, and its one assertion the service's expected rows, so the oracle computes nothing
new. Each `stress::MT_<mapping>` includes the mapping its services run against and holds their suites; a mapping test
runs its function against the mapping that holds it, includes and all.

Probed on the pinned release first (4.145.0): a mapping test may have ONE assertion ("Mapping Tests can only have one
assertion", at compile); it serializes its answer RAW, which is the same rows as a service's PURE_TDSOBJECT, and a
graph fetch's objects bare -- no {builder, values} envelope, and one object as an object, not an array of one; a
mapping that includes another runs its suites over both; and a suite's function parses no lambda written with braces,
`filter({x|...})` (F89), so each is written without them, `filter(x|...)`, the same Pure.

The engine runs them (run.py takes a `stress::` mapping's test suites as a testable, beside the services and the
function testables, the plan's D4). lite does not: its runner takes services, and a mapping test runner is product work
outside the plan (docs/DEFERRED_TEST_EXECUTION.md). lite's model build reads them, strictly, as it reads the rest.
"""
from __future__ import annotations

import re

# The services mirrored, by name: their queries span every generator's shapes -- the hand-written battery, graph
# fetches (nested, a union, milestoned), deep stacks, the typed literals and enumerations, the function vocabulary and
# relation operations, milestoning at a time of day, DenseMapping, relation mappings, checked fetches with their
# defects, the combination matrix, and the flat, ModelJoin and executed mappings.
MIRRORED = [
    "stress::Q0_TradeInstrSectorCptyTraderBook", "stress::Q1_TradeBookDeskInstrTrader",
    "stress::Q2_PnLBookDeskTrader", "stress::Q3_SettlementTradeInstrCptyCountry", "stress::Q4_ConfirmTradeInstrSector",
    "stress::Q5_SalesCreditTradeBookDesk", "stress::Q6_GreeksPositionInstr",
    "stress::Q7_TradeReportTradeInstrBookCpty", "stress::Q8_CollateralCptyCountry",
    "stress::Q9_PositionInstrSectorCurrency", "stress::Q10_TradeFilterSortLimit", "stress::Q11_TradeMonster9Joins",
    "stress::G0_TradeTree", "stress::G1_UnionTree", "stress::G2_MilestonedTree",
    "stress::DSDeep_SalesCredit", "stress::DSDeep_TradeReport", "stress::DSDeep_DailyPnL",
    "stress::DSGroup_SalesCredit", "stress::DSGroup_Settlement",
    "stress::DSTree_Trade", "stress::DSTree_Settlement",
    "stress::TX_All", "stress::TX_AccretingSwap", "stress::AA_CurvesYieldCurveCounts", "stress::D_TradeDense",
    "stress::TL_Name_Ne_Root", "stress::TL_Name_Ge_Hop", "stress::TL_Name_Lt_Root",
    "stress::TE_Prime_Ne_Root", "stress::TE_AllButDefaulted_In_Root", "stress::TQ_Or_Root",
    "stress::VF_ToUpper", "stress::VF_Substring", "stress::VF_Lpad", "stress::VG_SumQuantity",
    "stress::VG_AverageRate", "stress::VR_Extend", "stress::VR_Concatenate", "stress::VT_Extend",
    "stress::ITL_20240603T093500_At", "stress::ITL_20240605T093500_At",
    "stress::DMD_AccountingRule", "stress::DMD_JournalEntry", "stress::DMD_ClearedTrade",
    "stress::RMR_Deals", "stress::RMR_Executed", "stress::RM_BookTotals", "stress::RM_Ranked", "stress::RM_Priced",
    "stress::CK_DealChecked", "stress::CK_LineBasketDefects", "stress::CK_BasketDeskDefect", "stress::CKU_Deal",
    "stress::CKP_Deals",
    "stress::CB_C8", "stress::CB_C9",
    "stress::N1_TradeFlat", "stress::J0_TradeBookModelJoin", "stress::E1_ExecutedByMapping",
]

DEFAULT_MAPPING, DEFAULT_RUNTIME = "stress::AllMapping", "stress::RT"
DEFAULT_CONNECTION, DEFAULT_DATA = "environment", "stress::TestData"


def test_mapping(mapping: str) -> str:
    """The mapping that holds the suites of the services over `mapping`."""
    return "stress::MT_" + mapping.split("::")[-1]


def runtime_stores(text: str) -> dict[str, dict[str, str]]:
    """runtime -> {connection id: the store it connects}, read from every Runtime element of the corpus."""
    out = {}
    for m in re.finditer(r"^Runtime\s+([\w:]+)\s*\{(.*?)^\}", text, re.S | re.M):
        conns = m.group(2)[m.group(2).find("connections"):]
        # `store::DB: [ environment: store::Conn, other: x::Conn ]`, the store a path and its list of no brackets
        out[m.group(1)] = {cid: store for store, ids in re.findall(r"(\w+(?:::\w+)+)\s*:\s*\[([^\[\]]*)\]", conns)
                           for cid in re.findall(r"(\w+)\s*:\s*\w+(?:::\w+)+", ids)}
    return out


def _unbraced(text: str) -> str:
    """`text` with each braced one-parameter lambda an argument, `(` or `,` then `{x|...}`, written without its braces,
    `x|...`: the same Pure, in the one form a mapping test's function parses. The mapping lexer reads `{` as a brace
    only after `:` or `[` and anywhere else opens a class mapping's island (UPSTREAM_FINDINGS F89)."""
    out, i = [], 0
    while i < len(text):
        m = re.compile(r"([(,]\s*)\{(\s*\w+\s*\|)").match(text, i)
        if not m:
            out.append(text[i])
            i += 1
            continue
        out.append(m.group(1) + m.group(2))
        j, depth, quote = m.end(), 1, False
        while depth:
            ch = text[j]
            if quote:
                quote = ch != "'"
            elif ch == "'":
                quote = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            if depth:
                out.append(ch)
            j += 1
        i = j
    return "".join(out)


def _function(spec) -> str:
    """The service's query, as the suite's function."""
    import emit

    text = emit.graph_text(spec) if spec.graph is not None else emit.query_text(spec)
    head = "    query: "
    if not text.startswith(head):
        raise ValueError(f"{spec.short}: a query text the suite cannot take: {text[:40]!r}")
    body = _unbraced(text[len(head):])
    if re.search(r"[(,]\s*\{", body):
        raise ValueError(f"{spec.short}: a lambda of several parameters keeps its braces, which a mapping test's "
                         f"function cannot hold (F89)")
    return "      function: " + body.replace("\n", "\n  ")


def _expected(spec, rows) -> str:
    """The service's rows, as a mapping test serializes them: RAW, a graph fetch's objects bare (one as an object)."""
    import emit

    payload = rows if spec.graph is None else (rows[0] if len(rows) == 1 else rows)
    return emit.json_text(payload)


def render(emitted: dict, runtimes: dict[str, dict[str, str]]) -> str:
    """The test mappings, each holding a suite per mirrored service over the mapping it includes."""
    by_mapping: dict[str, list] = {}
    for name in MIRRORED:
        spec, rows = emitted[name]
        by_mapping.setdefault(spec.mapping or DEFAULT_MAPPING, []).append((spec, rows))
    out = []
    for mapping, specs in by_mapping.items():
        suites = []
        for spec, rows in specs:
            runtime = spec.runtime or DEFAULT_RUNTIME
            store = runtimes[runtime][spec.connection or DEFAULT_CONNECTION]
            suites.append(f"""    {spec.short}:
    {{
{_function(spec)}
      tests:
      [
        // the service's expected rows: {len(rows)}
        expected_rows:
        {{
          data:
          [
            {store}:
              Reference
              #{{
                {spec.data_element or DEFAULT_DATA}
              }}#
          ];
          asserts:
          [
            matchesOracle:
              EqualToJson
              #{{
                expected:
                  ExternalFormat
                  #{{
                    contentType: 'application/json';
                    data: '{_expected(spec, rows)}';
                  }}#;
              }}#
          ];
        }}
      ];
    }}""")
        body = ",\n".join(suites)
        out.append(f"""Mapping {test_mapping(mapping)}
(
  include {mapping}

  testSuites:
  [
{body}
  ]
)""")
    return "\n\n".join(out)


def check(emitted: dict) -> list[str]:
    """Each mirrored service is emitted, binds one mapping and one connection, and takes no parameter, so its query
    is a mapping test's function as it stands."""
    problems = []
    for name in MIRRORED:
        hit = emitted.get(name)
        if hit is None:
            problems.append(f"{name}: mirrored by a mapping test and not emitted")
            continue
        spec, _rows = hit
        if spec.params or spec.cases or spec.multi or spec.extra_data or spec.lets:
            problems.append(f"{name}: takes parameters, lets or several bindings, which a mapping test's function "
                            f"cannot")
    if len(set(MIRRORED)) != len(MIRRORED):
        problems.append("a service is mirrored twice")
    return problems
