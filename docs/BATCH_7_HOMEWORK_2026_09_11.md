# Batch 7 (the `spec` module) — homework before code (2026-09-11)

USER: "Do we need any homework before starting 7?" The program's batch-7 numbers
("9 files, 47 referenced core classes all already public, 3 test-side helpers, zero
gate-chain cost") were measured 2026-09-10, before batches 5 and 6 added checkout-reading
tests. Re-measured at a51b5d7bc; every number below is a command a session can rerun.

## 1. What moves — 15 files, two test-only packages

Files under `core/src/test` that read a checkout (`Corpus.ENGINE_ROOT`, `pureRoot()`,
`engineRoot()`, `PLATFORM_ROOTS`, the root properties): **12**. They sit in two packages
that exist ONLY in test scope, so the whole packages move and nothing in `core/main`
loses package-private reach:

| package | files | reads a checkout |
|---|---|---|
| `com.legend.rcorpus` | Corpus, DuckWorkspaces, EagerCorpusCompileProbe, LibraryPlatformNamespaceGuardTest, MinimalCorpus, MinimalCorpusTest | 3 (the rest are its helpers/tests) |
| `com.legend.tools` | CensusWorlds, CoreImportsParityTest, DynaFnRegistryTest, NativeSignatureGeneratorTest, PlatformNamesSpellingTest, PreludeGeneratorTest, SpecBodyCensusTest, SubsumedRegistryTest, UpstreamPathManifestTest | 9 |

None of the 15 uses `com.legend.testing.*` (measured: zero references). One shared
dependency: **`com.legend.claims.Claims`** is read by `PreludeGeneratorTest` (moves) AND by
`lowering/LoweringClaims` + `ClaimRegistryTest` (stay — they read Pure.java's source and
the registries, never a checkout). `Claims` is a measurement over main code that must
stay out of main (its javadoc; the program's D1). So it must be visible to two modules.

## 2. What the movers write — 7 write/read paths into core's tree

The generators write GENERATED resources into `core/src/main` relative to the module
directory (`Path.of("src/main/java/com/legend/builtin/DynaFn.java")`, `NameResolver.java`,
`PlatformTypes.java`, `native-membership.tsv`, `Pure.java`, and `PreludeGeneratorTest`'s
walk of `src/main/java`). From `spec/` these become `../core/src/main/…` — one `CORE`
root constant in spec, every path through it (the parity direction is unchanged: the
generated resource lives in core, the generator outside; that IS the contract).

## 3. What names the movers by path — 3 registers

- `JdbcSurfaceCensusTest` lists `core/src/test/java/com/legend/rcorpus/DuckWorkspaces.java`
  and `…/MinimalCorpus.java` as JDBC test roots → the two rows move to `spec/…`.
- `SkipCensusTest` already walks sibling modules by basename (the assumption-file set is
  unaffected by the move).
- `DanglingStateGuardTest` walks a module list (`Path.of("..", m, …)`) → `spec` joins it.

## 4. Gate wiring — 2 lines, no lane change

- Gate 4 / gate 5: `mvn -pl core test -Dtest=MinimalCorpusTest …` → `-pl spec`.
- Gate 1 (`mvn -pl core test`) loses the 15 files automatically; the checkout-reading
  parity tests then run in the gate-4/5 lane's module (`spec`), which CI already provides
  with the oracle checkouts (gate-env). Gate 2 installs core first; `spec` resolves the
  installed jar exactly as `parser-equivalence` does today.
- CI lanes are numbered by gate; nothing changes in the workflow matrix.

## 5. The enforcement the program promised — and the groupId problem

The program says `core/pom.xml` bans `org.finos.legend.*` (Maven enforcer) and ArchUnit
bans the imports. Measured today: **zero** `import org.finos.legend` in `core/src`
(main and test) and zero legend artifacts on core's classpath (`classpath-convergence.sh`)
— but NOTHING enforces it. And the ban as written cannot be written: **our own Maven
groupId is `org.finos.legend`** (root `pom.xml` line 7; batch 0 renamed the `nlq`
PACKAGE squat, the artifact groupId squat remains). `bannedDependencies` on
`org.finos.legend:*` would ban `legend-lite-core` itself.

Two ways to write the ban:

- (a) pattern on artifactIds: `org.finos.legend:legend-engine-*` and `org.finos.legend:legend-pure-*` — works today, but is a list that a new upstream artifact prefix (`legend-shared-*`?) silently escapes;
- (b) **rename our groupId to `com.legend`** (root pom + the four module poms + `pct`/`parser-equivalence` dependency coordinates on `legend-lite-core`; `~/.m2` re-install) — then `org.finos.legend:*` is bannable wholesale and the boundary is a one-line rule. It matches the package rename batch 0 did and the thesis's sentence 2.

**Recommendation: (b), inside batch 7** — the boundary is the point of the batch, and (a)
is a hand list.

## 6. What `spec` needs on its classpath

`legend-lite-core` (installed), junit, the two JDBC drivers `MinimalCorpus` executes
against (DuckDB, H2 — test scope, the same versions core's tests use), `Claims` via
core's **test-jar** (a `maven-jar-plugin` `test-jar` goal in core; `spec` depends on
`legend-lite-core:test-jar`). No `org.finos.legend` artifact — the same enforcer rule as
core's. ArchUnit's rules that pin lowering/normalizer dependency surfaces stay in core;
`spec` gets one rule: nothing in it imports upstream Java.

## 7. Decisions to take before code

1. `Claims` sharing: core test-jar (recommended — it keeps `Claims` a test-scope
   measurement and gives `spec` the same view) vs. moving `Claims` + `ClaimRegistryTest`
   + `LoweringClaims` to `spec` (then core's gate 1 no longer asserts the ledger — wrong:
   the ledger is core's fact).
2. The groupId: `com.legend` rename (recommended) vs. an artifactId pattern list.

## 8. Cost estimate

15 files moved (`git mv`, package names unchanged), 1 new module pom, 2 pom changes for
the test-jar, 7 path edits in the generators, 2 register rows, 1 module-list entry, 2
gate lines, the enforcer rule ×2 modules, the ArchUnit rule in spec, and — if (b) — the
groupId in 6 poms. Done-criterion (program §4): none — "that is the point": every
number the moved tests pin (rosters, DIVERGENT 0, membership 752, UNSUPPORTED 37,
PINNED_COUNT 91, …) must land byte-identical from the new module.
