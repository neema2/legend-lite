# Step 9, slice 2: state at the pause (2026-11-26, about 22:30 UTC)

Paused by the orchestrator (the user's decision: all attention on step 6). Nothing of this slice merges while
paused. The slice's record is `slice2-2026-11-26.md`, beside this note.

## Where the work is

| Branch | Head | What it is |
|---|---|---|
| `claude/edition-step9-slice2-clean` | `40c246ea5` | the slice as two commits on the batch's head `4d44bf54`: the move (`34d30715d`), then the audit's eight fixes. **This is the one to merge.** |
| `claude/edition-step9-slice2` | `bed34b673` | the earlier, merged form (the fixes branch and the batch merged in). Kept, not for merging: it lacks the audit's fixes. |

Nothing is half done. No file is mid-edit, and the clean branch builds. This note's own commit sits on top of
`40c246ea5` and changes no code.

## The audit's eight items

All eight are written and verified on the tree of `40c246ea5` (the runs were made on `676d267e1`, the same
tree before its three work-in-progress commits were made one).

| # | Item | State | Verified by |
|---|---|---|---|
| 1 | the factory's style a private final class (`PureText$Redialected`), its one caller held | done | `PlanStyleSurfaceTest.onlyTheHarnessMakesAStyleWithItsDialects`, `theStylesOverridersArePinned` (`//core:guardrails_edge`) |
| 2 | the two open dialects' constructors package-private, their subclasses held | done | `HookRulingTest.theEdgesOpenDialectsAreSubclassedByTheHarnessAlone` (`//core:guardrails_edge`) |
| 3 | `HarnessText.java` an owner of DuckDB and Postgres alone | done | `DialectLeakRatchetTest`, `DialectOwnerExclusivityTest` (`//core:census`); the exclusivity scan now reads the harness's sources too |
| 4 | the hook test's non-dialect member types pinned by name | done | `EdgeSpellingHookTest.theEdgesDialectsOverrideOnlyThePinnedArms` (`//core:core_edge_tests`) |
| 5 | the record says what the shipped edge library gained | done | `slice2-2026-11-26.md` |
| 6 | six stale comments in product sources, line counts kept | done | the build; `git diff --numstat` reads 1/1 on each file but DuckDb's 3/3 and SqlDialect's 2/2 |
| 7 | the scan-order install pinned over the harness's and the tests' sources | done | `HarnessDisciplineTest.theCorpussScanOrderIsInstalledInOnePlace` (`//core:census`) |
| 8 | the harness's dialect of each database the edge's or its direct subclass | done | `EdgeSpellingHookTest.theHarnessesDialectOfEachDatabaseIsTheEdgesOrItsDirectSubclass` (`//core:core_edge_tests`) |

One choice of mine inside item 1, for the reviewer: the factory no longer calls `isEdgeStyle` on its base. It
admits exactly `UPSTREAM` and `ENGINE_TESTS` by identity (the two the harness plans in) and refuses any other,
so the mode-branch census gains no call site.

The two things the audit could not check, since settled: the factory's class is no longer anonymous (so the
`PureText$4` numbering question is gone), and `ArchitectureTest` accepts `HarnessText`'s two static style
fields (`//core:guardrails` and `//core:guardrails_edge` pass).

## Results on the fixed tree

| Run | Result |
|---|---|
| `bazel build //core:all //spec:all //pct:all` | green |
| the guard round (`guardrails`, `guardrails_edge`, `census`, `census_edge`, `//tools/guards:all`, `//tools/deps:all`, `core_edge_tests`) | 33 pass, 1 skipped (`classpath_test`, arm only), 0 fail |
| `//spec:corpus_duckdb`, `//spec:corpus_h2`, `//pct:edge_contract_test`, `//pct:pct_channel_b`, `//core:core_tests_testplan`, `//core:core_tests_testexec`, `//docs:all`, `//gates:pct_h2` | 81 of 81 pass |
| `//gates:local` with `--lockfile_mode=error` | 556 of 572 pass, 1 skipped, 15 fail: the 14 root-only targets (all 14 pass run unprivileged) and `//datacube:timeseries_queries_test`, killed by the container's memory limit mid-run with no failed assertion |

## The two measurements

1. **The server switch (`legend.exec.engineScanOrder`): ran.** `git grep engineScanOrder` on the clean branch:
   outside `docs/`, the property is named by `spec/corpus.bzl` (the lanes' flag), `CorpusOn` and `CorpusOne`
   (set in code), `MinimalCorpusTest` (comments), `HarnessText` (the one read) and three guards. No product
   source, no script, no user-facing page sets or documents it. The record has the line.
2. **PCT on DuckDB and Postgres: the head ran, the base had not finished at the pause.**
   - Head (`40c246ea5`'s tree): every suite passes on both databases, 0 failed. Essential 345, Grammar 137,
     Relation 469, Standard 205, Unclassified 93, the same five counts on DuckDB and on Postgres (Postgres run
     through `tools/postgres/as_unprivileged.sh`).
   - Base (`4d44bf54`): a run was already started when the pause arrived and was left to end by itself. Its
     counts, when it ends, are added under "After the pause" below; if that section is missing, the run did
     not finish and is the first thing to redo.

## Exact next action when resumed

1. Read the base's PCT counts (below, or rerun `//pct:pct_duckdb` and `//pct:pct_postgres` at `4d44bf54`) and
   the rerun of `//datacube:timeseries_queries_test` alone on the head.
2. Send the orchestrator the final report: head `40c246ea5`, 819 lines of the two classes out of
   `core/src/main` (net 768 with `PureText`'s growth), every guard row (the record's table), the harness's
   dependencies, the results above, and what the product still names of the moved classes (comments only:
   `Census.java:132`, `Lowerer.java:1991`, `Ddl.java:19`, `ScanOrder.java:18`, `DuckDb.java:767` and `:778`,
   `SqlDialect.java:366`, `PureText.java`'s two class javadocs).
3. Nothing else is owed by this slice.
