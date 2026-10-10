# Phase 3b audit (2026-10-09, the `auditor` agent, read-only over the nine commits 691c5002d..bacbda6f5)

Verdict: ready after fixes. 1 blocker, 7 should-fix, 5 notes. The agent ran no Bazel command; everything came from
reading git, the code and the pinned upstream trees (`@legend_engine_src`, `@legend_pure_src`). The fixes landed in the
commit after bacbda6f5 (`PHASE_3B_LANDING.md` §3 says which fix answers which item).

## Blocker

**B1. The refusal of `inferRelationalType(rop, Boolean)` rests on a false reason, and it changes behaviour.**
- Where: `core/src/main/java/com/legend/platform/PlatformPure.java`, `refusedVersionsTable()`, the first entry; commit
  84fb743bc's message says "only the databricks extension outside the closure calls it".
- Evidence: core_relational itself calls it: `core_relational/relational/relationalMappingExecution.pure:207`
  (`getRelationalTypeFromRelationalPropertyMapping`: `relationalOperationElement->inferRelationalType(false)`), used at
  lines 96 and 101 to work out a relational property mapping's `sourceDataType`. Upstream's one-argument version is
  just `inferRelationalType($rop, true)`, so the Boolean version is the real body, not separate machinery.
- Before Phase 3b this id was a Body row; the commit made it Refused (ENGINE_MACHINERY).
- Fix: decide the version again from the real callers; correct the reason in the code, the commit text and the
  landing note.

## Should-fix

- **S1. TranslationContext refusals: reason holds, callers unnamed.** In-closure callers: `relationalGraphFetch.pure:616/707`,
  `testDataGeneration.pure:912/999/1121/1156`, `testRunner.pure` (graph-fetch and test-data machinery). Name them.
- **S2. Census ceilings not lowered after the walls fell.** `ManifestWorldCensusTest` still pinned 37 walls / 1,437
  bodies while 84fb743bc measured 27 / 1,430. Re-pin to the final tip's census, dated.
- **S3. The dot-call refusal's scope is wider and narrower than the design.** Wider: `classFqn` includes the raw name of
  a generic receiver, so `my::Person.all(now())` (the parser's property-form call) gets "no qualified property 'all'
  ... on 'meta::pure::metamodel::type::Class'" (before: an unknown-function error). Narrower: primitive and enum-value
  receivers still fall back to a function, the leniency PARK-14 refused. The milestoned branch matches `af.function()`,
  not the simple name; a milestoned dot call whose name the resolver qualified is refused: use `qname`.
- **S4. `qualifiedProperty` is not legend-pure's lookup.** Depth-first, first match by arity; H6 says legend-pure walks
  the C3 order and runs the matcher over every candidate (differs on diamonds and same-arity overloads split across a
  class and its superclass). State it or match C3 and collect all. `derivedOverloadArity`'s removal loses nothing.
- **S5. The boot path resolves the platform's bodies in a different scope than before.** `Compiler.boot()` adopted
  before `NameResolver.resolve`, so a system body resolved under the prelude declaration's section imports; the graph
  path adopts after resolution. Resolve first and adopt after, and update the comment.
- **S6. Ledger rows not closed in their fix commits.** PARK-12's and PARK-14's anchors went in 84fb743bc and
  a6ac6d618, the rows only in bacbda6f5; stale references at `Typer.java:597`, `PlatformPure.java:22`,
  `SystemMetamodel.java:1559`: reword as history.
- **S7. Two commit messages overstate.** 469ebfa38 says "2751 -> 2762" while its file says 2757 (f26c2fefb corrects
  it). bacbda6f5 and S38 say legend-engine's plain SQL "would give NULL" and that `false` is legend-pure's own value;
  neither was measured (`MissingValueInComputedColumnTest` measures lite's DuckDB and H2 output only). Mark them as
  claims read from upstream.

## Notes

1. **Side maps:** no reader is still keyed by qualified name where a function could be looked up; the class/enum-only
   readers (`PreludeGenerator`, `MinimalCorpus`), the owner lookups (`ModelNormalizer`, `AssociationSynthesis`,
   `StoreSubstitutionRewrite`) are correct; the driver lookups use `e.element()` (the id for a function);
   `EagerCorpusCompileProbe`'s nested name fallback is dead code; two overloads whose parameter types share a simple
   name in different packages share one key (predates the change; the duplicate check catches it).
2. **`CheckedLayer.without` is correct**; a tolerant build cannot end with two declarations of one id and no wall
   (`PlatformPureTest.parameterNamesMustAgree` pins it).
3. **Implementation table:** conflicts and dangling are reported for the three new kinds; `refused.put` in the
   refused-versions loop would override a WalledBodies refusal of the same id silently; a catalog-only table and a
   graph without core_relational report about 45 dangling entries (nothing asserts on them except the spec world test).
4. **Tests and guards:** the probe's two display names said "is NULL" while asserting `false`; the new tests pin
   behaviour, none vacuous; every ratchet and BUILD move carries a dated reason; `//spec:phase3b_probes` uses no
   hard-coded path.
5. **Landing note:** placeholders left for the run; no local paths; it states the PARK-13 deviation from the plan's L3
   row openly.
