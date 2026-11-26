# Audit: 34d30715d (step 9, slice 2), diff 4d44bf54..origin/claude/edition-step9-slice2-clean

**Verdict: 0 BLOCKER, 4 SHOULD-FIX, 4 NIT.** The move does what it says. Product routes do not lose any behaviour that is reachable today (the reasoning is under "Checked and sound"). The weak spots are in the guards. The new factory, the marker and the newly open dialect classes in the shipped `:edge` jar are protected only by prose. One owner allowlist entry is wider than the class it is for. One guard's walk was filtered more widely than it needed to be.

---

## SHOULD-FIX 1: `PureText.withDialects` / `OfTheEdge` is an unguarded way to mint an edge style, and it ships in the product's edge jar

`core/src/main/java/com/legend/edge/PureText.java:86-95` (isEdgeStyle, the `OfTheEdge` interface) and `:382-432` (withDialects).

**What is wrong**
- Before this commit, `isEdgeStyle` compared identity against four constants, so nothing could forge an edge style.
- Now it also answers true for `style instanceof OfTheEdge`. `OfTheEdge` is a package-private interface, and `withDialects(base, Function<DatabaseType, SqlDialect>)` is a package-private factory that accepts any dialect function.
- "Package-private" here means any class in `com.legend.edge`, in any jar on the classpath. That includes the shipped edge sources in `core/src/main/java/com/legend/edge` (PureV1Routes, EdgeServer, EnginePlans and so on). The package is split across `:edge` and `:edge_harness`.
- Such a class can:
  - call `PureText.withDialects(PureText.PURE_V1, t -> anyDialect)`; or
  - implement `OfTheEdge` directly, skipping the `isEdgeStyle(base)` check entirely.
- Either way, `SqlTextVerdicts.appeals` (`isEdgeStyle`) accepts the result.
- Nothing pins who calls `withDialects`. `PlanStyleSurfaceTest` adds `PureText$4 = ALL`, which approves every instance of the factory's class in advance, whoever builds it. Its comment says "the factory is package-private, so only the harness makes one", and that is not enforced.

**Scenario.** A later change in `PureV1Routes` hands the planner `PureText.withDialects(PURE_V1, HarnessText-like dialects defined in :edge)`. The result:
- PlanStyleSurfaceTest stays green: no new PlanStyle class appears.
- EdgeSpellingHookTest stays green: it walks only PureText's and HarnessText's member classes.
- The H2-text translation or the scan order comes back onto the shipped server, and no guard turns red.

**Fix**
1. Replace the interface with a private static final nested class, for example `private static final class Redialected implements PlanStyle`, built only inside `withDialects`. Have `isEdgeStyle` test `instanceof Redialected`. Code outside PureText can then neither implement nor construct it.
2. Add an ArchUnit rule: the callers of `PureText.withDialects` are exactly `{com.legend.edge.HarnessText}`, and no class in `legend.product.jars` or `libedge.jar` calls it.
3. Replace the PlanStyleSurfaceTest prose with the rule's name.

## SHOULD-FIX 2: `PureText.DuckDb` and `PureText.Postgres` are now `public` and non-final with public constructors, so "open to the harness's subclass alone" is unenforced

`PureText.java:469-471` and `:510-512`.

**What is wrong.** Any class can now subclass the edge's DuckDB or Postgres dialect. A new top-level class in the product edge sources would pass every existing guard:
- `com.legend.edge.Foo extends PureText.DuckDb { raw(...) / scanOrderPasses() }` passes `HookRulingTest.onlyTheEdgeOverridesASpellingHook`, because `inEdge` is true for the package.
- It passes `ArchitectureTest.DIALECTS_ARE_THE_PRODUCTS_OR_THE_EDGES`, which allows anything in `com.legend.edge..`.
- It passes both pin walks in `EdgeSpellingHookTest` (`theEdgesDialectsOverrideOnlyThePinnedArms` and `theHarnessesDialectsOverrideOnlyThePinnedArms`). Both walk the member classes of PureText and HarnessText only.

Together with finding 1, this is exactly the shape of the two hooks this slice removed, and nothing would see it come back.

**Fix**
1. Give both classes a package-private constructor (`DuckDb() {}`). `PureText.dialect` and HarnessText are in the same package, so both still work, and code outside the package can no longer subclass them.
2. Add a jar-reading guard: every class in the product and edge jars that is assignable to `PureText.DuckDb` or `PureText.Postgres` (other than the class itself) is a member of `com.legend.edge.HarnessText` and lives in `libedge_harness.jar`.

## SHOULD-FIX 3: the `DatabaseOwners.EVERY` entry for HarnessText is wider than the file it is for

`core/src/test/java/com/legend/DatabaseOwners.java:172-177`.

**What is wrong**
- `EVERY` gives `Kind.EVERY` with `DATABASES.keySet()`, so HarnessText.java may now name every database.
- It names two: `DatabaseType.DuckDB` and `DatabaseType.Postgres`, plus the `DuckDb` and `Postgres` subclasses.
- PureText is in `EVERY` because it is the edge's full registry. HarnessText is not a full registry.
- The user objected today to code outside dialects naming databases, so a blanket pass here is the wrong default.

**Scenario.** A later edit adds `if (type == DatabaseType.Snowflake) return new SnowflakeHack();` to `HarnessText.dialect`. `DialectLeakRatchetTest` stays green.

**Fix.** Register HarnessText as an owner of exactly `{DuckDB, Postgres}`. Either use the DIALECT branch with a `NAMED_BY_ROLE`-style row (`HARNESS + "edge/HarnessText.java" -> Set.of("DuckDB", "Postgres")`, the form `RawSqlBoundary`'s H2 row already uses), or add a small HARNESS owner map. Then any third database in the file goes red.

## SHOULD-FIX 4: `EdgeSpellingHookTest.armsOf` exempts every non-dialect member type of PureText, not just `OfTheEdge`

`core/src/test/java/com/legend/integration/EdgeSpellingHookTest.java:284-297`.

**What is wrong**
- Before this commit, `theEdgesDialectsOverrideOnlyThePinnedArms` compared the full set of PureText's member classes against `OVERRIDES`. Adding any nested type to PureText (a dialect, a helper class, a pass, a facet) failed until someone pinned it with a reason.
- The filter `SqlDialect.class.isAssignableFrom(c)` was added so that `OfTheEdge` would be accepted. As written, it silently accepts every future non-dialect member type as well.

**Scenario.** Someone nests `static final class EngineOrder implements SqlRewriter` in PureText. Before, the test failed; now it passes. The "holds no state" check still runs, but it only looks at fields.

**Fix.** Keep the filter, and add an exact pin on what it leaves out: the non-dialect member types of PureText must equal `{"OfTheEdge"}` (or nothing, once finding 1 makes the marker a private class). Alternatively, exclude `OfTheEdge` by name.

---

## NIT 5: the commit says "Nothing in the product gained a row", but the shipped edge library did gain test-only API

- `:edge` ships in the server and now carries a factory (`withDialects`), a marker (`OfTheEdge`) and an anonymous style class (`PureText$4`) that exist only for the test harness.
- `PlanStyleSurfaceTest` gains rows for two classes of the shipped jar (`$4` and `$OfTheEdge`).
- `ModeBranchCensusTest`, which is report-only and meant to fall to 0, gains one call site: the `isEdgeStyle` call at `PureText.java:388` inside `withDialects`. `isEdgeStyle` itself also gains an `instanceof` branch.

The commit message does disclose the four rows, but the sentence quoted above overstates. Reword it, or drop the `isEdgeStyle` call in `withDialects`; after finding 1's guard, the only caller is known.

## NIT 6: stale comments in product sources that still credit the edge's dialects with the moved hooks

- `core/src/main/java/com/legend/sql/dialect/AnsiSqlRenderer.java` (raw's javadoc, about lines 305-306): "so the compatibility edge's DuckDB and Postgres translate legend-engine's H2 text here".
- `core/src/main/java/com/legend/sql/dialect/DuckDb.java`, the comment above `ps.addAll(scanOrderPasses())` in `passes()`: "the scan-order passes the compatibility edge supplies". The commit updated the two comments above it but not this one.
- `core/src/main/java/com/legend/setup/Ddl.java:19` ("the edge.RawSqlBoundary translator serves"), `core/src/main/java/com/legend/sql/ScanOrder.java:18`, `core/src/main/java/com/legend/lowering/Lowerer.java:1991`, and the `isEdgeStyle` javadoc in `PureText`, which lists four styles without mentioning the marker.

## NIT 7: the scan-order install guard no longer sees its install site

`core/src/test/java/com/legend/TestLaneOrderGuardrailTest.java:78-99`.

- `filesMatching(PASS_INSTALL / SWITCH_LITERAL)` now asserts empty sets over `core/src/main/java` only. That is a tightening for the product, and correct.
- The one real install site, HarnessText.java, is read only through `core.duckdb_dialect`, for a count of 1 inside that one file.
- So a second `new StableScanOrder()`, or a second read of the switch, in another harness, spec or pct file would go unseen. Before this commit, the install site was inside the scanned set, so a second one would have been caught.

Optional fix: also scan `core/src/edge_harness/java` and pin `{com/legend/edge/HarnessText.java}`.

## NIT 8: `withDialects` and HarnessText do not check that each harness dialect is the edge's dialect of the same database

`withDialects` accepts any function. `theHarnessesDialectsOverrideOnlyThePinnedArms` checks the nested classes' superclass names, not what `HarnessText.dialect(t)` returns for each type. A slip such as `DuckDB -> new Postgres()` would pass.

Fix: add a test that, for every type in `Databases.implemented()`, `HarnessText.dialect(t).getClass()` is either `PureText.dialect(t).getClass()` or a direct subclass of it.

---

## Checked and found sound

- **The moves.** RawSqlBoundary and StableScanOrder are renames at 94% and 98% similarity. Only their javadoc changed: `{@link PureText.DuckDb}` became `{@link HarnessText.DuckDb}`, plus a history sentence. No code changed.
- **The two product dialect files.** `sql/dialect/DuckDb.java` (two comment lines) and `SqlDialect.java` (the raw javadoc) change comments only. The interface, signatures and visibility are untouched.
- **BUILD.**
  - `:edge_harness` stays `testonly = True`, and its visibility is unchanged (`//spec`, `//pct` and the core package).
  - Bazel's testonly rule stops any product target (planner, exec, server, `:edge`, `:edge_server`) from depending on it.
  - The new deps (`:database`, `:model`, `:sql`, `:sql_dialect`) all point down from the harness into the product. `:edge`'s deps are unchanged.
  - `tools/deps/core-layers.txt` already lists `edge_harness` among the not_layers.
  - `core_edge_tests_lib` and `//spec` already depend on `:edge_harness`. Every test that names HarnessText is in `EDGE_TEST_SRCS` or spec.
- **Product behaviour, the raw translation.** On today's product routes, `raw(...)` saw a session dialect that differed from the declared language only on the test road:
  - `QueryDoor.refuseTestOnly` refuses `RAW_SQL` (executeInDb's read) and `INLINE_SEEDS` (from()'s sqlSetups) on every product door.
  - A product query's dialect is always `style.dialect(Compiler.executesOn(...).type())`, the declared type. A runtime's declared `testDataSetupSqls` carry `writtenFor = cd.databaseType()`, so an H2 connection is rendered by the H2 dialect and `raw` was the identity before and after.
  - A system-routed product query's target (`SystemCatalog.forQuery`) carries no declared seeds.
  - `EffectPlanner` and `LoadPlanner.seeds` are reached only from the test planner (`TestKinds`).

  So pure/v1 (`PURE_V1`, the `EdgeServer` and `PureV1Routes` routes) answers as before.
- **Product behaviour, the scan order.** The only removal is a hidden knob. A shipped server started with `-Dlegend.exec.engineScanOrder=true` used to add StableScanOrder's ORDER BY on DuckDB under pure/v1, and now cannot. Nothing documents or sets that knob outside `spec/corpus.bzl`. Losing it fits the 2026-08-29 ruling, but say so to the user.
- **The style swap in callers.**
  - Every caller that needs translation or scan order switches to the harness style: MinimalCorpus's audience options, LeanSqlCorpusTest, and ExecuteInDbDuckDbEdgeTest via EdgeProgram.
  - `MinimalCorpus.openWorkspace(PureText.UPSTREAM)` only picks H2 session settings, which the harness style delegates (`twoForm`).
  - Nothing compares `== ENGINE_TESTS` or `== UPSTREAM` outside PureText and EdgeProgram, and EdgeProgram was updated.
  - `SqlTextVerdicts.appeals` needs the marker, and has it.
  - `PlanMaker`'s declared-load cache keys on `dialect.getClass().getName()`, which tells `HarnessText$DuckDb` apart from `PureText$DuckDb`.
- **Guard edits that are moves or shrinks.**
  - JavaEvalLedgerTest: two rows removed, the files left `core/src/main`. A shrink.
  - TestLaneOrderGuardrailTest: StableScanOrder left two allowlists, and the install and switch sets became empty. Shrinks.
  - SqlTextRatchetTest: RawSqlBoundary's exemption removed. A shrink.
  - DatabaseOwners NAMED_BY_ROLE and the DIALECT rule for RawSqlBoundary: a like-for-like path move, still exactly `{H2}`.
  - DialectOwnerExclusivityTest: a path update.
  - RawSqlLedgerTest: the scan now includes `core/src/edge_harness/java`, and the pin moves from PureText.java to HarnessText.java at the same count, 2.
  - EdgeSpellingHookTest: the total of pinned arms is unchanged (DuckDb raw and scanOrderPasses, Postgres raw), and the harness's arms are checked as spelling arms with no state.
  - PlanStyleFacetsTest: adding `HarnessText.UPSTREAM` widens coverage.
  - All of them are dated 2026-11-26 and name the slice.
- **Coverage of the moved files.** `ArchitectureTest`'s `CORE_AND_EDGE_CLASSES` and `HookRulingTest` read `legend.edge.jars`, which includes `:edge_harness`. So the `StableScanOrder.FIRINGS` static-state row and the spelling-hook rule still see the moved classes.
- **Delegation.** `withDialects` delegates every method but `dialect` to `base`, so the harness style keeps the base's edition, facets and two-forms.
- **"PCT is not wrapped."** True in the diff: pct still uses `PureText.UPSTREAM` and `ENGINE_PCT`, and only `spec/corpus.bzl` sets the scan-order flag.

## Not checked (needs a build or a run)

- That it compiles. In particular, the package-private access across the split package between two jars on one classpath. It works on the application classpath and would break under JPMS.
- That `PlanStyleSurfaceTest`'s `PureText$4` numbering is right (I inferred it from the source order).
- That `ArchitectureTest.staticStateIsConstantOrRegistered` accepts HarnessText's two static style fields: an anonymous class capturing a method reference and a base style.
- That TestLaneOrderGuardrailTest's normalized shape match passes on HarnessText.java's line breaks.
- The corpus answers on the DuckDB and Postgres lanes, and that the lean-SQL baseline is unchanged.
- The claim that no PCT body sends H2-written text to DuckDB or Postgres: the probe was never committed. One round of `//pct:pct_duckdb`, `pct_postgres` and the channel-B twins would confirm it.
- Whether EdgeCasesGoldenTest's goldens and the core edge tests that run on DuckDB under `PureText.UPSTREAM` (BodyRouterParityTest's backend loop) depended on the translation. From reading, their connections declare the backend's own type, so no.
