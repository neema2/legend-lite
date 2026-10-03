# Spike S1: a JVM test runner that speaks Bazel's test protocol (2026-10-03)

**Plan item:** Phase 1.1 (a test runner that follows Bazel's test protocol), rule R6 ("tests follow Bazel's test protocol: XML, filter, sharding") and R2 ("policy lives in BUILD files").
**Base:** `main` at `23b441852`. **Branch:** `spike/s1-test-runner`, two local commits (`3866fc772`, `d530e6436`). Not pushed.
**Machine:** macOS arm64, Bazel 9.2.0, rules_java 9.9.0, rules_jvm_external 7.1, JUnit Platform 1.11.3 / Jupiter 5.11.3 (from `junit-platform-console-standalone`), JDK 25 runtime.
Every build used `--local_resources=cpu=3 --jobs=3`.

## Decision

**GO with B: extend our own `tools/junit/JUnitMain` on the JUnit Platform Launcher API.** Do not adopt contrib_rules_jvm's `java_junit5_test` (A).

| # | Question | Answer |
|---|---|---|
| 1 | Can a runner keep every lane's selection exactly and speak the whole protocol? | **Yes (B).** The prototype is about 350 lines (446 with comments) of runner code. It passes all seven checks on `//json:tests`, `//core:guardrails` and `//pct:pct_channel_b` (E1 to E8). Selection is identical testcase by testcase. No BUILD file outside `tools/junit` has to change. |
| 2 | Does contrib_rules_jvm 0.34.0 (A) work on Bazel 9.2 and rules_java 9.9? | **Yes, mechanically.** It adds two modules to the graph (`contrib_rules_jvm`, `apple_rules_lint`); protobuf, rules_go and gazelle are already there. It runs, writes test.xml, filters and shards (E9). |
| 3 | Why not A, then? | (a) **It runs one class per target.** Every package, multi-class, class-name-pattern or tag lane must become a hand-written `@Suite` class. That moves selection policy out of BUILD files into Java (against R2): 11 of the 34 targets. (b) **An empty selection, or a `--test_filter` that matches nothing, passes** with zero testcases (E9). That breaks R4 "no silent skips" and drops our `--fail-if-no-tests`. (c) Its Java agent blocks `System.exit` by throwing (stricter than detecting it), and costs about 5 s per JVM (0.25 s becomes 5.3 s on `//json:tests`). (d) It has no `${TEST_UNDECLARED_OUTPUTS_DIR}` expansion and no prerun, so the corpus lanes could not move until Phase 3.1. (e) It does not solve the JUnit 3 / PCT sharding problem either (E7 applies to it unchanged, since it uses the same post-discovery-filter mechanism). |
| 4 | C: anything better? | Nothing off the shelf. rules_java 9.9 has no JUnit 5 runner. JUnit's ConsoleLauncher has no sharding and no single-file XML. Bazel's own runner is JUnit 4. The useful "C" ideas are folded into B: contrib's `Class#method` filter syntax, Bazel's own round-robin sharding, and a guard that fails a shard split an engine silently ignores. |
| 5 | Plan done-criterion "`shard_count = 4` works on a PCT target" | **Needs rewording.** The PCT classes are JUnit 3 suites (`PureTestBuilder` suites wrapped in `PureTestHelperFramework.wrapSuite`'s `TestSetup`). The vintage engine cannot filter inside them, so a per-test split runs the whole class in every shard (E7). With B, that now fails loudly. The fix is `shard_by = "class"` (prototyped, E7) or one target per PCT class. Proposed criterion: "`pct_duckdb` runs with `shard_count = N, shard_by = "class"`, or as one target per suite class." |

## Evidence

All commands ran from the spike worktree. Output is trimmed and paths are shortened. Testcase identity was checked by extracting `classname#name` from every `<testcase>` (baseline: the console launcher's `test.outputs/junit/TEST-*.xml`; after: Bazel's `test.xml`), sorting with `LC_ALL=C`, then `diff`ing.

### E0. Baseline: what today's runner does with the protocol

```
$ bazel test //json:tests --test_output=all
[       308 tests found           ]  [       308 tests successful      ]
$ grep -c '<testcase' bazel-testlogs/json/tests/test.xml
1                                   # Bazel's synthetic one: <testcase name="json/tests" ...>
$ bazel test //json:tests --test_filter=com.legend.json.JsonTest$EscapePrimitives ...
[       308 tests found           ]  # filter silently ignored
$ bazel test //json:tests            # with shard_count = 3 on the target
ERROR: Testing //json:tests (shard 2 of 3) failed: Sharding requested, but the test runner did not
advertise support for it by touching TEST_SHARD_STATUS_FILE. ...
$ bazel test //tools/junit/spike:premature_exit     # a test calling System.exit(0)
//tools/junit/spike:premature_exit        PASSED in 1.3s                    # (!)
```

### E1. Selection is identical (same target, before and after)

| Target | Selectors exercised | Before (ConsoleLauncher) | After (B) | Identity |
|---|---|---|---|---|
| `//json:tests` | `--select-package` | 308 tests | 308 tests | `diff` empty: IDENTICAL |
| `//core:guardrails` | `--select-package` + `--include-tag=guardrail`; 4 engines on the classpath (jupiter, vintage, suite, archunit) | 80 | 80 | IDENTICAL |
| `//pct:pct_channel_b` | `--select-package`, `upstream = True` | 5 | 5 | IDENTICAL |
| `//core:census` | `--select-package` + `--include-tag=census` | 12 | 12 | IDENTICAL |
| `//parser-equivalence:parser_parity` | `--select-package` + `--exclude-classname` (one per diagnostics class plus `ProtocolRosterCensusTest`) | 43 (PASSED in 732 s) | **not completed**: the JVM was killed with SIGTERM (`exited with error code 143`) after 442 s, during a machine-wide resource emergency. It is not a runner verdict. Not re-run, resource limits. | expected IDENTICAL: `ClassNameFilter.excludeClassNamePatterns` is the same call the console launcher makes for `--exclude-classname` |

```
$ bazel test //core:guardrails --test_output=all
[        80 tests found           ]  [        80 tests successful      ]
[bazel] selected 80 tests (a parameterized/dynamic container counts once)
$ python3 ids.py after.txt bazel-testlogs/core/guardrails/test.xml; diff after.txt before.txt && echo IDENTICAL
IDENTICAL
```

**Not run, resource limits** (the coordinator stopped heavy lanes mid-spike): `core_tests`, `spec_tests`, `stress_suites`, `scale_*`, `corpus_*`, `pct_duckdb`/`pct_h2`/`pct_postgres`, `diagnostics`, and the warehouse lanes. What to expect from the selector mapping:
- They use only the selectors already exercised above (`--select-package`, `--select-class` single or multiple, `--include-tag`, `--exclude-tag`, `--include-classname=.*`, `--exclude-classname`). B maps each one to the same Launcher filter the console launcher builds (its `DiscoveryRequestCreator`), so identical selection is expected.
- The PCT lanes are the exception for *sharding* only (E7). Their unsharded selection is plain `--select-class`, so it is expected to be identical too.
- The `pct_channel_b` row above was run before the stop.

The runner keeps the console launcher's one implicit selector. With no `--include-classname`, `ClassNameFilter.STANDARD_INCLUDE_PATTERN` (`^(Test.*|.+[.$]Test.*|.*Tests?)$`) applies, as before. This is why `scale_*`'s `--include-classname=.*` still means what it says.

### E2. `--test_filter` runs only what it names

```
$ bazel test //json:tests --test_filter='escapesQuote$'
[         1 tests found           ]
$ bazel test //json:tests --test_filter='JsonTest\$Escape'          # a nested class
[        34 tests found           ]
$ bazel test //core:guardrails --test_filter='com.legend.ArchitectureTest#'        # one class
[        38 tests found           ]
$ bazel test //core:guardrails --test_filter='ArchitectureTest#coreModuleHasNoUtilPackage'   # one method
[         1 tests found           ]
$ grep -o '<testcase [^>]*>' bazel-testlogs/core/guardrails/test.xml
<testcase classname="com.legend.ArchitectureTest" name="coreModuleHasNoUtilPackage()" time="0.033">
$ bazel test //json:tests --test_filter=NoSuchTest
[bazel] --test_filter=NoSuchTest matched no test
//json:tests                         FAILED in 1.7s
```

The filter is a regex *found* in `fully.qualified.Class#method` (Bazel's JUnit 4 runner's form, and contrib's), applied as a post-discovery filter after the lane's own selectors. So it can only narrow a lane, never widen it.

### E3. test.xml lists real testcases

```
$ head -c 300 bazel-testlogs/json/tests/test.xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?><testsuites><testsuite errors="0" failures="0"
 name="JUnit Jupiter" skipped="0" tests="308" time="0.188" ...><testcase classname="com.legend.json.JsonTest$WriterCompact" ...
$ grep -c '<testcase' bazel-testlogs/core/guardrails/test.xml
80
```

How: `LegacyXmlReportGeneratingListener` writes one `TEST-<engine>.xml` per engine into a temporary directory. The runner merges the non-empty ones into one `<testsuites>` at `$XML_OUTPUT_FILE`, without the `<properties>` block (the JVM's whole property table, which names the machine).

### E4. Sharding: union = unsharded set, no duplicates, balanced

`--test_sharding_strategy=forced=N` gives the same environment as `shard_count = N`, without editing BUILD files.

```
$ bazel test //json:tests            # shard_count = 3
[bazel] selected 215 tests ..., shard 0/3 ran 91
[bazel] selected 215 tests ..., shard 1/3 ran 112
[bazel] selected 215 tests ..., shard 2/3 ran 105
union of shard_{1,2,3}_of_3/test.xml: 308 == unsharded set, duplicates 0
$ bazel test //core:guardrails --test_sharding_strategy=forced=3
shards 26 / 27 / 27; union == unsharded set (80), duplicates 0
$ bazel test //pct:pct_channel_b --test_sharding_strategy=forced=4
shards 2 / 1 / 1 / 1; union == unsharded set (5), duplicates 0;  wall 26.8 s -> 15.6 s
```

The algorithm is round-robin over each engine's selected tests, sorted by unique id (Bazel's own JUnit 4 default), and each engine starts at its own offset. A first version hashed each unique id (contrib's method). It split `pct_channel_b`'s 5 tests as 1 / 0 / 4 / 0, so it was replaced. The 215 is "test units": a parameterized or dynamic container is one unit, because a post-discovery filter cannot see its invocations, so all 308 invocations still land in exactly one shard.

### E5. An empty selection fails

```
$ bazel test //tools/junit/spike:empty_selection     # --select-package=com.legend.nosuchpackage
[         0 tests found           ]
[bazel] the selection found no tests (--fail-if-no-tests)
//tools/junit/spike:empty_selection     FAILED in 0.8s
```

The emptiness check counts the selection *before* the shard split. So a shard that draws nothing (possible with many shards and few tests) passes, while an empty lane still fails in every shard.

### E6. `resources:memory` tags, and a premature `System.exit`

```
$ bazel query --output=build //tools/junit/spike:memory
  tags = ["resources:memory:512"],  jvm_flags = ["-Duser.timezone=GMT", "-Xmx512m"],  use_testrunner = False,
$ bazel test //tools/junit/spike:memory --test_output=all
[probe] max heap MB = 512
$ bazel test //tools/junit/spike:premature_exit       # @Test void exitsTheJvmWithZero() { System.exit(0); }
FAIL: //tools/junit/spike:premature_exit (Exit 0)
-- Test exited prematurely (TEST_PREMATURE_EXIT_FILE exists) --
```

Sizes and `resources:memory:<n>` tags are target attributes that Bazel's scheduler reads. The runner does not touch them, and they pass through the macro unchanged. Note that Bazel clamps a tag larger than `--local_resources=memory` rather than refusing it. The premature-exit file is created before the launcher starts and deleted after it returns. The runner then calls `System.exit(code)` itself, so a stray non-daemon thread cannot hang the test.

### E7. JUnit 3 suites (the PCT shape): the vintage engine cannot be filtered inside them

`tools/junit/spike/DynamicSuiteTest` builds a `TestSuite` of `TestCase`s at discovery, the shape of `PureTestBuilder`. It is run flat, or with nested sub-suites like the PCT's per-package suites.

```
flat suite:   --test_filter='#testFn_7$'  -> 1 test ran;   forced=3 -> 4 / 4 / 4
nested suite: forced=3 (first version)    -> 12 / 12 / 12   # every shard ran everything
```

Why: the vintage engine removes an excluded descriptor only if it can push a JUnit 4 `Filter` into the runner (`VintageTestDescriptor.removeFromHierarchy` → `tryToExcludeFromRunner`). `JUnit38ClassRunner.filter` filters only the top level of a plain `TestSuite`. The PCT classes return `wrapSuite(...)`, a `TestSetup` (a decorator, not a `TestSuite`), around nested suites. So neither a test-level nor a class-level post-discovery split can remove anything. Bazel would happily report N green shards that each ran the whole class. A class-level filter cannot help either, because the runner descriptor is not a leaf and the platform only filters leaves.

What B does about it:

```
$ bazel test //tools/junit/spike:vintage_two_classes --test_sharding_strategy=forced=2
[bazel] 12 tests ran that the shard split excluded: an engine could not filter them (a JUnit 3 suite under vintage).
[bazel] sharding this target would run them in EVERY shard. Shard it by class (junit_test(shard_by = "class")) or split it into one target per class.
//tools/junit/spike:vintage_two_classes     FAILED in 2 out of 2
$ bazel test //tools/junit/spike:vintage_by_class --test_sharding_strategy=forced=2     # shard_by = "class"
[bazel] class shard 0/2: 1 of 2 classes   [12 tests found]   -> only DynamicSuiteTest$1
[bazel] class shard 1/2: 1 of 2 classes   [12 tests found]   -> only OtherDynamicSuiteTest$1
$ bazel test //tools/junit/spike:vintage_two_classes --test_filter=OtherDynamicSuiteTest
[bazel] 12 tests ran that the --test_filter excluded: ...      # a warning only; still PASSED
```

`shard_by = "class"` deals the sorted `--select-class` list round-robin *before* discovery. It costs nothing and needs no engine cooperation, and it refuses a lane that selects by package. For `--test_filter` on such a suite, the guard only warns, because running more than asked is harmless. On PCT, filter at class granularity.

The probe has the same shape as the PCT classes but is not one of them. Confirming it on `pct_duckdb` / `pct_h2` (9 GB / 4.6 GB lanes, too heavy to share this machine) is open question 1.

### E8. Prerun and outputs token

The prerun and `${TEST_UNDECLARED_OUTPUTS_DIR}` expansion are kept unchanged. The prerun child now runs without `XML_OUTPUT_FILE` and `TEST_PREMATURE_EXIT_FILE`, so the second pass alone reports to Bazel. The corpus lanes were not run (heavy, and the prerun is being replaced in Phase 3.1).

### E9. Option A, measured: contrib_rules_jvm 0.34.0

```
MODULE.bazel: bazel_dep(name = "contrib_rules_jvm", version = "0.34.0")
$ diff <(bazel mod graph, before) <(after)          # module@version set
> apple_rules_lint@0.4.0
> contrib_rules_jvm@0.34.0                          # protobuf 33.4, rules_go, gazelle: already in our graph
$ bazel test //json:tests_contrib                   # test_class = com.legend.json.JsonTest
Failures: 0
Time: 5.308                                         # B: 0.252 s for the same 308 tests
308 testcases in test.xml; ids identical to E1
$ bazel test //json:tests_contrib --test_filter=NoSuchTest
Failures: 0      //json:tests_contrib   PASSED      # 0 testcases: a typo passes
$ bazel test //core:guardrails_contrib              # test_class = a hand-written @Suite class:
                                                    #   @Suite @SelectPackages("com.legend") @IncludeTags("guardrail")
80 testcases, ids identical; forced=3 -> 27 / 27 / 26, union identical; one-method filter -> 1
$ bazel test //tools/junit/spike:premature_exit_contrib
java.lang.SecurityException: System.exit is not allowed during test execution   # FAILED (Exit 2)
```

So A can reach the same selection, but only by writing the selection as Java. Its runner, `ActualRunner`, builds exactly one `selectClass(test_class)` (plus static nested classes). It adds only `JUNIT5_INCLUDE_TAGS`, `JUNIT5_EXCLUDE_TAGS`, `JUNIT5_INCLUDE_ENGINES` and `JUNIT5_EXCLUDE_ENGINES` filters, a hash-based shard filter, and the `Class#method` pattern filter. It sets no class-name filter and makes no empty check.

## Selector mapping

| Our selector (today, in `select`/`exclude_tags`) | Used by | B (extended JUnitMain) | A (contrib `java_junit5_test`) | Cost under A |
|---|---|---|---|---|
| `--select-class=C` (one) | 17 targets (corpus_*, reference_lane, scale_* ×6, stress_suites, pct_discipline, pct_h2, postgres_live*, launcher_test, tools/deps ×5) | same | `test_class = "C"` | none |
| `--select-class` (several) | pct_duckdb (6), pct_postgres (5), diagnostics (11) | same | none: one class per target | a `@Suite @SelectClasses` class per lane, or one target per class plus a `test_suite` |
| `--select-package=P` | core_tests, guardrails, census, spec_tests, pct_channel_b, parser_parity, warehouse tests and tests_native, json tests | same | none | a `@Suite @SelectPackages` class per lane (needs `junit-platform-suite` on the classpath; it is in console-standalone) |
| `--include-tag=E` | guardrails, census | same (`TagFilter.includeTags`) | `include_tags = [...]` | none (works on a suite's tree, E9) |
| `--exclude-tag=E` (via `exclude_tags`) | core_tests, stress_suites, spec_tests | same | `exclude_tags = [...]` | none |
| `--exclude-classname=R` | parser_parity (11 patterns) | same (`ClassNameFilter.excludeClassNamePatterns`) | none | `@ExcludeClassNamePatterns` on a suite class |
| `--include-classname=R` (and the implicit default pattern) | scale_* (`.*`); every package lane relies on the default | same; the default is applied when none is given | none; contrib applies no class-name filter | `@IncludeClassNamePatterns`; a `@Suite` with `@SelectPackages` applies the same default |
| `--fail-if-no-tests` | all (macro) | same, plus "a filter that matches nothing fails" | none | `@Suite(failIfNoTests = true)` (the default since 1.9) covers suites only; a filter typo and single-class lanes still pass |
| `--details` / `--disable-banner` / `--disable-ansi-colors` | all (macro) | dropped from the macro (accepted and ignored if present) | n/a | none |
| engines | implicit (all on the classpath) | implicit | `include_engines` / `exclude_engines` | none |
| `-Dlegend.prerun=…`, `${TEST_UNDECLARED_OUTPUTS_DIR}` in `jvm_flags` | corpus_duckdb, corpus_h2, corpus_warehouse | kept | none (fixed main class) | those lanes wait for Phase 3.1 |

Under A, **11 of 34 targets need a hand-written suite class**, and 3 more wait for Phase 3.1. Every lane would also pick up a 5 s agent startup.

## Migration recipe (B)

All 34 `junit_test` targets keep their BUILD text, because the selection vocabulary is unchanged. The work is in `tools/junit`:

1. **`tools/junit/JUnitMain.java`**: replace `ConsoleLauncher.main` with the prototype's `run()`:
   - an argument parser for the 7 selectors plus `--select-method`, where any unknown argument fails;
   - one `LauncherDiscoveryRequest` with the standard class-name default;
   - the tag filters folded into a single `BazelFilter` (test filter, shard split, pre-shard count, excluded-id record);
   - `SummaryGeneratingListener` printed as today's counts;
   - `LegacyXmlReportGeneratingListener` merged to `$XML_OUTPUT_FILE`;
   - the overrun guard;
   - the premature-exit file and an explicit `System.exit`.

   Drop `--reports-dir` (test.xml replaces it). Keep `expandOutputs` until Phase 1.2/3.1 decide. Delete the prerun in Phase 3.1, as planned.
2. **`tools/junit/defs.bzl`**:
   - drop the three presentation flags;
   - add `shard_by = "test" | "class"` (emits `-Dlegend.shard=class`).

   Phase 1.4's `memory_mb` goes in the same macro (it emits both `resources:memory:<n>` and `-Xmx`), and `shard_count` passes through `**kwargs`.
3. **`tools/junit/BUILD.bazel`**: depend on `junit-platform-launcher`, `junit-platform-reporting` and the engines (jupiter, vintage, suite) instead of `junit-platform-console-standalone`. The runner no longer uses the console launcher, and the fat jar re-exports picocli and shaded copies onto every test classpath. Repin `@maven_test`.
4. **Unit-test the runner** (`//tools/junit:runner_test`) with the spike's probes turned into fixtures: an empty selection, premature exit, filter-no-match, the shard union on a flat and a nested JUnit 3 suite, and class shards. These are cheap (about 1 s each).
5. **BUILD files**, only where wanted:
   - `pct_duckdb` and `pct_postgres`: `shard_count = 3, shard_by = "class"`, or (preferred, better caching) one target per suite class plus a `test_suite` named as today;
   - `core_tests` (about 4,000 tests, `enormous`): `shard_count = 4`;
   - `spec_tests` / `parser_parity`: measure first.

   Nothing else changes.
6. **Prove it** before merge: an identity diff (E1's method) on every non-manual lane in CI, comparing the old runner's `TEST-*.xml` with the new `test.xml`, plus the forced-shard union check on `core_tests` and `pct_duckdb`.

Phase 6 guard: a `bazel query` test that every `java_test` in the repo is created by `junit_test` (no `use_testrunner = False` elsewhere).

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| JUnit 3 suites (PCT) silently ignore a test-level split or filter | **certain** (E7) | The overrun guard fails sharded runs; `shard_by = "class"`, or one target per PCT class |
| Round-robin depends on the selected set being the same in every shard (discovery must be deterministic) | low (sorted by unique id, not discovery order) | The union check in the runner's unit tests and in step 6 |
| A class `@BeforeAll` setup repeats in every shard that draws one of its tests | medium for heavy classes (rcorpus, stress) | `shard_by = "class"` for those lanes, or no sharding |
| We own about 350 lines (446 with comments) of runner code instead of a community rule | certain | It sits on public, stable Launcher APIs (`LauncherDiscoveryRequestBuilder`, `PostDiscoveryFilter`, `LegacyXmlReportGeneratingListener`, `SummaryGeneratingListener`). Covered by step 4's tests. |
| `--test_filter` semantics differ from the console launcher's `--select-method` | low | Documented: a regex found in `Class#method`, the same as Bazel's JUnit 4 runner and contrib |
| A test that calls `System.exit(0)` mid-run under B is detected but not prevented, so later tests in that JVM do not run | low | The target fails (E6). That is enough. A's agent-based prevention can be added later if wanted. |
| Bumping JUnit to 6.x | medium-term | The Launcher APIs used are stable in 6.0. The legacy XML listener is still shipped. |

## Effort

Based on this spike: the runner code is written and verified on three real lanes and six probes (about half a day including dead ends). What remains:
- dependency cleanup (step 3): S;
- runner unit tests (step 4): S;
- the identity diff on every non-manual lane (step 6): M, about a day, mostly machine time on `core_tests`, `spec_tests`, `parser_parity` and the PCT lanes;
- PCT per-class targets or `shard_by`: S.

**Total M (2 to 3 days)**, matching the plan's estimate. A would be M to L: 11 suite classes, upstream work or a wrapper for the empty-selection check, and agent overhead on 34 targets, with the PCT problem unsolved anyway.

## Open questions

0. **Finish E1 on the heavy lanes** (not run, resource limits): the identity diff on `parser_parity` (killed by SIGTERM mid-run), `core_tests`, `spec_tests`, `stress_suites` and the PCT lanes. Migration step 6 does exactly this in CI.
1. **Confirm E7 on a real PCT class.** Run `bazel test //pct:pct_h2 --test_sharding_strategy=forced=2` with B. The guard should fail it, and `shard_by = "class"` on `pct_duckdb` should split it 6 classes over N. This was not run here because the lanes need 4.6 to 9 GB each.
2. **Per-class PCT targets or `shard_by = "class"`?** Per-class targets cache independently and need no runner mode. `shard_by` keeps one label per lane. The Phase 5 CI shape decides.
3. **Should `--test_filter` that excludes nothing useful on a JUnit 3 suite fail instead of warn?** Today it warns, because running more than asked is harmless.
4. **Keep `expandOutputs` (the `${TEST_UNDECLARED_OUTPUTS_DIR}` token in `jvm_flags`)?** Or make tests read the environment variable (Phase 1.2/3.1)? The prerun goes away in 3.1 either way.
5. **Is test.xml without `<properties>` acceptable** to whatever CI reporting Phase 5 picks? Engine-level `<testsuite>`s are named by engine ("JUnit Jupiter"), not by class. Bazel and most viewers group by `classname`, but a per-class `<testsuite>` (contrib's layout) is a small change if wanted.
6. **`core_tests` shard count.** It needs a timing run. Class-heavy setup in `com.legend.integration` may favour `shard_by = "class"`, which today needs `--select-class` lanes. A class mode for package lanes (hash of the class name, as a `ClassNameFilter`) is about 10 more lines if needed.
