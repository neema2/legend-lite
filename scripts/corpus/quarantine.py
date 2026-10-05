"""
Known-failing cases, and why.

A corpus that deletes or "fixes" the cases an engine gets wrong is worse than no corpus:
it launders a defect into a passing suite. These cases stay, with their CORRECT
expectations, and are reported as KNOWN-FAIL. `run.py` exits non-zero if a quarantined
case starts passing (the defect is fixed — remove it from here) or if an unquarantined
case fails (a regression).

Nothing may be added here without a minimized reproduction under repro/ and an entry in
docs/UPSTREAM_FINDINGS.md. "It fails and I do not know why" is not a quarantine reason.

A quarantine is PER ENGINE, not per corpus. Sharing one list across both harnesses was
wrong and briefly made the differential report the six F6 services as "fixed": they fail in
legend-engine and AGREE in legend-lite, which is precisely the finding. A case can be
broken in one engine and correct in the other, so each harness carries its own list.

  ENGINE_QUARANTINE   legend-engine vs the oracle  -> scripts/corpus/run.py
  LITE_QUARANTINE     legend-lite   vs the oracle  -> CorpusDifferentialTest
"""
from __future__ import annotations

# testable fqn -> (finding id, one-line reason)
ENGINE_QUARANTINE: dict[str, tuple[str, str]] = {
    f"stress::{name}": ("F6", "count() over an empty to-many returns 1, not 0")
    for name in (
        "F0_InstrumentChildCounts",
        "F1_CounterpartyChildCounts",
        "F2_BookChildCounts",
        "F3_DeskChildCounts",
        "F4_SectorInstrumentCounts",
        "F6_PositionGreeksCounts",
    )
}

# F50: a derived BOOLEAN compared to a boolean literal cannot be lowered. `$x.isFinal ==
# false` reaches the database as SQL it rejects with a parser error; `!$x.isFinal`,
# `$x.isFinal`, and the same comparison over a derived Float or String all run. MD3 is this
# query written the way that works, so the construct is covered as well as pinned.
#
# The corpus had never filtered on a derived property at all before this -- a thousand
# services projected them constantly, which is a different code path and a working one.
# F6 again, on a disjunction: five of the twelve exposure lines match no exemption rule at
# all, and count() over that empty set returns 1 rather than 0. Not a new defect -- the same
# one the F-series pins -- but worth an instance here because the join is an `or` between two
# tables rather than a foreign key, and nothing had asked F6's question over one of those.
#
# The construct is still covered green: LE6 counts the same join from the RULE side, where
# every rule reaches at least one line, and LE1 reaches the range join the same way.
# F54: a qualified property whose body CONCATENATES returns a string built from empty parts
# when the to-one chain it is reached through lands on nothing -- `"BBG/:"` rather than null.
# Three of the eight curves here have no benchmark series, so three of sixteen rows carry a
# fabricated ticker.
#
# The same property doing arithmetic over the same absent object is correct, which is what
# makes this narrow enough to pin rather than a general nullability complaint. See
# scripts/corpus/probe_qualified_broken_chain.py for the four-case separation.
#
# Two services reach it, both generated: the dense stack over curve summaries and the
# filtered variant of the same reach. Both project the same `tickerOn` through the same
# to-one chain, so they are one defect seen twice rather than two.
for _n in ("D_CurveSummaryDense", "SP_defCurveSummaryFilter"):
    ENGINE_QUARANTINE[f"stress::{_n}"] = (
        "F54", "a concatenating qualified property fabricates a value on a broken chain")

ENGINE_QUARANTINE["stress::LE2_ExemptionMatches"] = (
    "F6", "count() over an empty to-many returns 1, not 0")

ENGINE_QUARANTINE["stress::MD7_UnrevisedPrintsEq"] = (
    "F50", "derived Boolean == boolean literal in a filter generates invalid SQL")

# F51: `isEmpty()` over a to-many end whose join is not a single key equality returns the
# source row once per joined row. The booleans are all correct, so it looks like a
# duplicate-row problem rather than an aggregation one. `count()` over the identical end does
# NOT duplicate, and `isEmpty()` over a plain foreign-key association does not either.
#
# First scoped to SELF-joins, which was too narrow: an `or` join between two different tables
# duplicates the same way. The probe carries the case that showed it.
ENGINE_QUARANTINE["stress::CV6_PillarEmptiness"] = (
    "F51", "isEmpty() over a to-many reached by a non-key join returns one row per joined row")

# F52: both ends of a {target} self-join return the same set. `shorterPillars` and
# `longerPillars` are opposite directions of one inequality and the engine answers the
# forward one for both. Nothing in the model distinguishes the ends, which is why the oracle
# has to be TOLD the direction (oracle.SELF_JOIN_REVERSE) -- but the oracle is told, and the
# engine is not asking.
ENGINE_QUARANTINE["stress::CV7_PillarsShorter"] = (
    "F52", "the reverse end of a {target} self-join returns the forward set")

# F35 -- `dayOfYear` lowers to DuckDB's `day()`, so it answers the day of the MONTH. The
# service asserts 155 for 2024-06-03, which is right, and fails. Kept rather than corrected:
# an expectation adjusted to 3 would make the suite green by recording the defect as the
# specification, and this one is invisible without an independently computed expectation.
# F37 -- `substring` is 0-based with an exclusive end in memory and 1-based with a LENGTH in
# SQL. The two services run the identical call on the identical string; the in-memory one
# PASSES, which is what makes the relational one a defect rather than a corpus assumption.
ENGINE_QUARANTINE["stress::F37_SubstringSql"] = (
    "F37", "substring in SQL is 1-based and takes a length, not a 0-based exclusive end")

# F41 -- `first()` on a relation returns the whole relation. The service sorts four rows
# into a total order and asserts the first one; the engine returns all four. Nothing errors,
# which is what makes it worth a permanent test rather than a note.
# F13 again, reached a different way. CF_Confluence is the confluence mapping's service --
# scope and Otherwise on one class, which is the point of 82-confluence.pure -- and its
# Otherwise half fails exactly as O1 does: the fallback never fires under a TDS projection,
# so the rows without an inline cache come back NULL. Same table, same join, same Otherwise
# clause, a different owning class; the defect does not care.
#
# Quarantined rather than reworked, because the alternative is to stop projecting the
# counterparty and keep only the scope half, which would make the service green by removing
# the construct it was written to combine.
ENGINE_QUARANTINE["stress::CF_Confluence"] = (
    "F13", "Otherwise never falls back under TDS projection, as O1")

ENGINE_QUARANTINE["stress::F41_RelationFirst"] = (
    "F41", "first() on a relation returns every row instead of the first")

# F39 -- startsWith/endsWith/contains compile the PATTERN operand as literal text, so a
# column pattern yields `S like 'root.P%'` and is false for every row. Row 3 of the service
# has both operands present and genuinely matching, so the failure cannot be read as a NULL
# question -- 'alpha' starts with 'a' and the engine says false.
ENGINE_QUARANTINE["stress::F39_NullBoolean"] = (
    "F39", "startsWith/endsWith/contains with a column pattern are false for every row")

# F38 -- four properties declared StrictDate; `firstDayOfWeek` alone renders as a DateTime.
# The other three are asserted in the same row and pass, so the file fails on exactly the
# one column that is wrong.
ENGINE_QUARANTINE["stress::F38_FirstDayTypes"] = (
    "F38", "firstDayOfWeek renders a StrictDate property as a DateTime")

ENGINE_QUARANTINE["stress::F35_DayOfYear"] = (
    "F35", "dayOfYear lowers to DuckDB day(), returning the day of the month")

# F24 -- a DateTime serializes WITH a UTC offset through TDS projection and WITHOUT one
# through graph fetch. Same column, same mapping, same row, two execution paths.
#
# Listed by name rather than derived, even though the rule ("every generated tree containing
# a DateTime") is mechanical, because a derived quarantine would silently absorb the next
# timestamp defect too. When F24 is fixed these come back together, and if all but one do
# the remaining one is telling us something.
#
# The cost of listing is staleness, and it has already been paid once: a seed change altered
# which properties the graph-fetch ranking selects, two of these stopped projecting a
# DateTime at all, and they began "passing" while the defect they pinned was untouched.
# That is the same laundering this file exists to prevent, arriving from the other
# direction. So the list stays explicit AND check() asserts that every name in it still
# projects a DateTime -- an entry that has stopped exercising its defect is reported rather
# than silently counted as a pin.
ENGINE_QUARANTINE.update({
    f"stress::{name}": ("F24", "graph fetch omits the UTC offset TDS projection includes")
    for name in (
        # Added with the middle-office and risk domains: both carry timestamps -- a
        # confirmation is sent and matched at a time, a risk run starts and completes at one
        # -- so their generated trees project a DateTime and meet F24 like every other.
        # Both Confirmation classes now carry their package in the service name, because
        # ops:: and middleoffice:: each define one and the generators name from the SHORT
        # class name. Renaming a generated service repoints any quarantine entry keyed on it,
        # which the build catches and did.
        #
        # Only ONE of the two generates a tree: graphs.py dedupes by tree SHAPE, and the two
        # Confirmations have the same shape, so the middle-office one displaced the ops one
        # that used to be here as GG_ConfirmationTree. Adding a class can therefore remove a
        # service, which is worth knowing before reading the count.
        "GG_TradeTree",
        "GG_ClearedTradeTree",
        "GG_TradeExceptionTree",
        "GG_SalesCreditTree",
        "GG_CashSettlementTree",
        "GG_AllocationTree",
    )
})

# F28 -- `!=` is the ONLY comparison that keeps a row whose operand is NULL.
#
# Against a NULL column the engine excludes the row for `==` and for `>`, and KEEPS it for
# `!=`. Those cannot both be right:
#
#   SQL three-valued logic     NULL <> 'x' is UNKNOWN, so `!=` must EXCLUDE  -- it does not
#   Pure collection semantics  [] == 'x' is false, so `!=` is true and keeps -- but then the
#                              ordered comparisons should not be excluding either
#
# So this is an internal inconsistency in the engine's filter lowering rather than a
# disagreement about which model to prefer, and it holds whichever model Legend intends.
# The oracle stays on three-valued logic: it is self-consistent, and it is what `==` and `>`
# already do.
#
# Minimized in repro/not-equals-null/, which pins all three operators side by side -- the
# `==` and `>` cases PASS there, which is the evidence that the `!=` case is the odd one.
#
# Found by the seed change that put a NULL in EVERY nullable column rather than only the
# first. Until then no `!=` predicate in the corpus had ever met a NULL, so four generated
# services diverged at once the moment one could.
ENGINE_QUARANTINE.update({
    f"stress::{name}": ("F28", "`!=` keeps a row whose operand is NULL; `==` and `>` do not")
    for name in (
        # The trio lives in combos.predicate_specs; only the `!=` member diverges, and its
        # two passing companions are what identify it as the odd one.
        "CB_NotEqualsNull",
    )
})

# F29 -- a graph fetch returning exactly ONE row serializes `values` as a bare OBJECT
# rather than a one-element ARRAY. Two rows produce an array from the same query shape, the
# same mapping and the same serialization format, so the JSON TYPE of `values` depends on
# the row count. Minimized in repro/graphfetch-single-row/, which puts the one-row and
# two-row cases side by side; the two-row case PASSES there.
ENGINE_QUARANTINE.update({
    "stress::GG_PortfolioTree": ("F29", "one-row graph fetch yields an object, not a "
                                        "one-element array"),
})

# F26 -- a CROSS-DATABASE join compiles and then cannot execute. The chain
# HIER_INSTRUMENT (hier::HierDB) -> HIER_ISSUER (hier::IssuerDB) -> HIER_COUNTRY is accepted
# by the grammar and by the compiler, and the planner emits ONE SQL statement joining tables
# that live in two different physical connections. Whichever connection runs it, the other
# store's table is absent: "Catalog Error: Table with name HIER_ISSUER does not exist".
#
# Not a packaging problem -- the runtime connects both stores and both ###Data elements are
# referenced. A single relational join cannot span connections; that is what XStore exists
# for. The construct is reachable, compiles clean, and fails only at execution.
# NOT quarantined any more. The chain was rerouted through two hops INSIDE one database, so
# it executes and the join-chain semantics are actually asserted. The cross-database join
# itself is still declared in 62-mapping-features.pure -- the CONSTRUCT stays covered at
# compile time, and F26 records why it cannot be executed. Coverage of a construct and
# coverage of its semantics are different claims, and this corpus can now make both.

# F27 -- a Binding transformer returns the JSON-ENCODED value, not the decoded one. A
# String property read out of a JSON payload comes back as "\"sector-001\"" -- quotes and
# all -- where the value of the key is sector-001. The oracle asserts the decoded string
# because that is what a String[0..1] property means; the engine disagrees.
ENGINE_QUARANTINE["stress::H_IssuerBinding"] = (
    "F27", "Binding returns each value as its raw JSON token, not its declared type")

ENGINE_QUARANTINE["stress::G3_UnionTreeWithEnum"] = (
    "F10", "graph fetch RAISES on an unmapped enum code where TDS returns null")

ENGINE_QUARANTINE["stress::M2_CanonicalWithEnum"] = (
    "F12", "EnumerationMapping not applied through a ModelChainConnection")

ENGINE_QUARANTINE["stress::O1_CounterpartyOtherwise"] = (
    "F13", "Otherwise never falls back under TDS projection")

ENGINE_QUARANTINE["stress::X1_ExternalEntityProjection"] = (
    "F15", "XStore navigation is unsupported in a relational projection")

# Cases that do not FAIL but HANG. They stay in the corpus as the statement of what should
# work, and are excluded from the run: a test that never returns does not report a
# failure, it blocks every other test behind it. That is how F15 first presented -- a
# 50-minute "slow run" that was one service never returning.
#
# Each needs a minimized repro proving the hang, exactly like any other quarantine entry.
HANGS = {
    "stress::X1_ExternalEntityProjection":
        ("F15", "hangs rather than failing; see repro/xstore/"),
}

ENGINE_QUARANTINE["stress::F32_TradeRollupEverything"] = (
    "F14", "groupBy on an enum-mapped column groups by the source code, not the value")

# legend-lite agrees with the reference evaluator on every service EXCEPT F32, where it
# shares F14 with legend-engine. That is the differential's stated blind spot arriving:
# the two engines agree with each other and both diverge from the reference, so only the
# independent oracle can see it.
LITE_QUARANTINE: dict[str, tuple[str, str]] = {
    "stress::F32_TradeRollupEverything": (
        "F14", "shared with legend-engine: groupBy uses the enum SOURCE code"),
    # 2026-10-05, the differential's first run under Bazel (P3-18): an open semantics question, not yet a defect
    # either side owns; repro/datediff-hours-off-boundary/
    "stress::MO2_Confirmations": (
        "F58", "dateDiff HOURS from a start off the hour: the oracle counts boundaries (164), lite truncates (163)"),
}

# Kept for callers that predate the split; run.py is the legend-engine harness.
QUARANTINE = ENGINE_QUARANTINE

# stress::F5_TraderChildCounts is deliberately NOT quarantined. It is the only fan-out
# service whose data contains no childless entity, and it passes. Keeping it in the green
# set is what proves the other six fail because of the empty case specifically, rather
# than because aggregate projections are broken in general.


def check_f24(c, tables) -> list[str]:
    """Every F24-quarantined service must still PROJECT a DateTime.

    An entry that no longer does has stopped exercising the defect it pins, and will start
    reporting as FIXED while the defect is untouched -- which is the same laundering this
    file exists to prevent, arriving from the direction of the seed rather than the engine.
    Two entries reached that state when a data change altered which properties the graph
    ranking selects, so this is checked rather than assumed.
    """
    import json
    import graphs
    import oracle

    seeded = {k for k, v in tables.items() if v}
    by_name = {s.name: s for s in graphs.build(c, seeded, tables)}
    bad = []
    for fqn, (fid, _why) in ENGINE_QUARANTINE.items():
        if fid != "F24":
            continue
        spec = by_name.get(fqn)
        if spec is None:
            bad.append(f"{fqn}: quarantined under F24 but no such generated service")
            continue
        if "+0000" not in json.dumps(oracle.evaluate_graph(c, spec, tables)):
            bad.append(f"{fqn}: quarantined under F24 but projects no DateTime, so it no "
                       f"longer exercises the defect -- re-point or remove the entry")
    # And the other direction. A generated tree that DOES project a DateTime and is not
    # listed will fail as a REGRESSION, which is the right alarm but the wrong diagnosis --
    # it reads as a new defect rather than as this list having gone stale.
    listed = {k for k, (fid, _w) in ENGINE_QUARANTINE.items() if fid == "F24"}
    for name, spec in by_name.items():
        if name in listed:
            continue
        if "+0000" in json.dumps(oracle.evaluate_graph(c, spec, tables)):
            bad.append(f"{name}: projects a DateTime but is not quarantined under F24; it "
                       f"will report as a regression rather than as a known defect")
    return bad
