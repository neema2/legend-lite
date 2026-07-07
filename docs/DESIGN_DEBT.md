# Design-debt ledger (lowering/sql/exec — 2026-07-05 smell audit)

> The MODULE-WIDE audit ledger (parser, F, G, architecture — four parallel
> auditors) lives in AUDIT_2026_07.md. This file remains the
> lowering/sql/exec deferred list.

The audit report's HIGH findings are FIXED (see the cleanup commit). This
ledger tracks the deliberately deferred MEDIUM/LOW items so they are owed,
not forgotten. Remove entries as they land.

- M1 (full form): retire UnfoldableRef exception-as-control-flow for a
  sealed `Resolution { Resolved(SqlExpr) | Unfoldable(column) }` protocol.
  CONTAINED for now: stack traces suppressed, every catch site isolates
  once then fails loudly.
- M3: copy-paste pairs — groupBy/groupByOnto, sort/sortOnto tails;
  aliasOf vs fromAlias (put alias() on SqlSource); Projection output-name
  rule duplicated (put outputName() on Projection); isMany triplicated
  (belongs on Multiplicity).
- M4: Cast(value, target, boolean array) — drop the flag; use
  SqlType.Array (already in the vocabulary).
- M5: boolean-mode parameters — extendWith(append), narrowTo(distinct),
  asSource(leftSide) reused by pivot, listExists(forAll). Split methods.
- M6: Lowerer is ~1100 lines and single-use (mutable aliasCounter +
  letBindings, no reuse guard) — extract join/window sections; guard or
  document single-use.
- M10: guard-then-body double dispatch (relationPredicate, Windows.lookup).
- M11: one exception type for three meanings — introduce
  UnsupportedPureFeature / DialectCapabilityException vs plain
  IllegalStateException for invariants.
- L1: inline-FQN noise (Lowerer case labels, Compiler.execute, dialects).
- L2: dead surface — Column.of, SortKey.asc/desc (test-only), Join.Kind.CROSS
  never constructed, SqlExpr.Case never emitted by lowering, Reducer.distinct
  always false. Mark test-only or delete.
- L3: ExecutionResult.Graph null-handling inconsistencies.
- L4: resolver contract is a stringly BiFunction with prop==null meaning
  bare-var — name it (ColumnResolver interface).
- L5: SqlSelect API asymmetries (withProjections also swaps outputs;
  withDistinct can't clear; no factory beside starOf).
- L6: Join.outputs() allocates per call on hot resolution paths.
- L7: Aggregates.reducerFor's redundant second param.
- L8: ResultShape classifies Variant ClassType as GRAPH — needs an explicit
  arm when bare-Variant results become reachable.
