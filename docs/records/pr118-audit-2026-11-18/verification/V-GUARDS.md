# V-GUARDS verification

Scope: GUARDS-1, GUARDS-3, GUARDS-4, GUARDS-5 / ARCH-5, GUARDS-6, GUARDS-8, GUARDS-11 (findings/GUARDS.md, findings/ARCH.md).
PR head `68f20eb4d` (read-only checkout `scratchpad/pr118`), main `4dfbb5b7`. The final train's base is `815de5810`
(handoff §7.1, §8.1). No Bazel run. The scripts I wrote are in `audit/verify/scripts/` (`ubt_widen.py`, a port of
`UpstreamBranchesTest.found()`).

**The rubric's question for every finding:** is this "a guard made vacuous or weakened so that it would hide a
regression"? For each finding below the answer is **no**.
- Every guard here is new in this PR. None of them exists at main (`git cat-file -e 4dfbb5b7:<file>` fails for each),
  so this PR weakened none of them relative to main.
- None of them was weakened during the PR's own history either. Vocabularies and scopes are unchanged since each guard
  was created; pins only went down.
- What is real: these guards are much narrower than the handoff says. The handoff's §2.4 table and "census 0" claim
  meaning properties that the guards do not check.

The one deletion of a check that main has is GUARDS-11, the corpus database judge. It is covered by the user's judge
model ruling. One condition attached to that ruling is not yet tracked.

| Finding | Claimed | Verdict | New in PR? | Ruled? | Covered by a test? | Fix size |
|---|---|---|---|---|---|---|
| GUARDS-1 | UpstreamBranchesTest matches nothing that exists, scans 3 of ~40 packages, has no floor, its bite test runs on strings only: LENS 0 is vacuous for "branches on upstream's shape" | **FIX-AFTER** (facts confirmed; the guard is not vacuous but a narrow tripwire; the final train did not make it so) | Yes: the whole guard (created train 17, `f2fabb090`, 2026-11-18 07:09; absent at main) | No. Its scope is the orchestrator's (06:40 ruling); no user ruling. The handoff overclaims what it proves | No: nothing checks its coverage or its import | ~25 lines (a floor, a bite on the real `found()`, PACKAGES = every product package plus one RULED or moved lineage row). A derived vocabulary is GUARDS-3's fix |
| GUARDS-3 | The TEXT bucket sees ~6% of the edge's own strings; a product `"classBuilder"` passes every guard | **FIX-AFTER** (confirmed) | Yes (CompatSpellingGuardTest from `eec0b7916`, 2026-11-12) | The limit is stated in the guard's own javadoc ("a fixed list catches only what is on it -- this is the drift alarm") | No | ~60-100 lines (vocabulary from the edge's constant pools minus a stoplist, matched against the product pools) |
| GUARDS-4 | L-A's first half sees only function arguments | **FIX-AFTER** (confirmed) | Yes | No | No | ~60 lines (an ASM per-parameter "never loaded" scan, pinned like RULED) |
| GUARDS-5 / ARCH-5 | The net pins lite's SQL in `activities` and in pure/v1 plans; no PCT client reads `activities` | **FIX-AFTER** (confirmed for PCT; for pure/v1, `activities` is part of the answer bytes but holds lite's SQL, not legend-engine's) | Yes (the net is new; `activities` recorded since `ba3d0a635`/`719953eb3`, 2026-11-17) | Recorded by the orchestrator's Phase 6 order (2026-11-17), not the user's | n/a (it is the net) | ~10 lines in PctCaseRecorder or EdgeNormalize, plus one re-record |
| GUARDS-6 | The id mask erases every number of 16 or more digits; relation row order is unchecked | **FIX-AFTER** (confirmed; the net is weaker than the handoff says; no hidden regression found or constructible from the record) | Yes (the mask since the net's birth, `17eeb38aa`, 2026-11-16) | No | Partly: channel A outcomes (CI and the net), core tests and pure/v1 (UUID-only mask) catch exact-asserted values; channel B's 2-ULP compat tolerance does not | ~5 lines (UUID-only mask), plus a re-record and a twice-run determinism check |
| GUARDS-8 | `shrink_only` cannot judge this PR (17 of 19 watched files do not exist at main); vocabularies unwatched for removal | **NOTE / FIX-AFTER** (confirmed; also, the checker itself is absent at main, so CI runs the PR's own copy) | Yes (the script and 17 of the files) | Its limit is stated in its docstring (:42) | n/a | ~10 lines (GROW_ONLY entries; WATCHED `CODECS`), plus a handoff sentence |
| GUARDS-11 | The corpus database-judge pass and its registers were deleted (`b523c767d`) with a dated reason | **NOTE** (ruled), with two FIX-AFTER items: the ruling's own "either way" port is untracked, and G2's text is stale | Yes: main ran it (`4dfbb5b7:spec/corpus.bzl:64,105`; the registers exist at main) | Yes: the user's judge model, 2026-11-17 01:30 ("Love it"), and the user's own conditional (quoted below) | G2 is still measured on core's judge matrix (CI) and PCT channel B (BOTH isolated). The corpus is no longer measured anywhere | Docs: ~5 lines (G2's guard text; a follow-up row for the shape port) |

**Blockers confirmed: none.**

## Details

### GUARDS-1: UpstreamBranchesTest

**Facts (all confirmed at the head):**
- `core/src/test/java/com/legend/UpstreamBranchesTest.java:39` `PACKAGES = List.of("lowering", "resolver", "testexec")`.
  Those are 3 of the 36 top-level product packages (`ls -d core/src/main/java/com/legend/*/`). They hold 190 files:
  lowering 102, resolver 59, testexec 29. In lines that is 31,379 + 36,436 + 5,628, against 412,673 under
  `com/legend`.
- `:42` UPSTREAM_SHAPES = `"unionAlias"`, matched as a whole literal (`:194`).
- `:49` EDGE_SHAPE_READERS = `foldedAggregate`, matched as a qualified call.
- `:52` and `:57` RULE_READ and RULE_BRANCH match `floatUlps`, `decimalScale` and `floatTolerance`. A call on a
  receiver declared with a two-form kind type is exempt (`:93-135`).
- `:66` `SITES = Map.of()`.

**The vocabulary at the head (`grep -rnw` over `core/src/main/java`):**
- `foldedAggregate`: comments only (`SnapshotEnvelope.java:32`, `VerdictSql.java:487`).
- `floatUlps`: comments only.
- `decimalScale`: comments, plus a private helper in `sql/dialect/ClickHouse.java:2304` (unscanned, and an unrelated
  meaning).
- `floatTolerance`: `VerdictNumbers` and its two forms. Every call site in VerdictSql is `numbers.floatTolerance()` on
  a `VerdictNumbers numbers` receiver, which the guard exempts by design.
- `"unionAlias"`, whole literal: in the edge, and in **`lineage/ScanRelations.java:1536`**
  (`? "unionAlias" + a.tCol() + "_" + jx : "SQLNull"`). The lineage site is product code outside PACKAGES; it is also
  on main, at `ScanRelations.java:1612`.

My port of `found()` (`audit/verify/scripts/ubt_widen.py`) reproduces the guard's results:
- at `815de5810` it finds exactly the 9 pinned counts: Fold 1, Lowerer 7, SubselectPrune 1;
- at the head it finds 0;
- with PACKAGES widened to every product package at the head, it finds one site:
  `lineage/ScanRelations.java | shape unionAlias: 1`.

The two remaining claims also hold:
- **No floor.** There is no `GuardCoverage.assertFloor` in the file.
- **The bite never reaches the import.** `theScanBites` (`:272-306`) calls `stripComments`, the two patterns,
  `hookReceivers`, `edgeShapeReads`, `lineOf` and `ruleSites` on string snippets. It never calls `found()`, so the file
  enumeration (`SourceFiles.under`, the `.java` filter, the relative path) is never bitten.

**Is it vacuous? No: it is a narrow tripwire.** With SITES empty, `theProductBranchesOnUpstreamOnlyWherePinned` (`:266`)
fails on any return of the following in lowering, resolver or testexec:
- a `"unionAlias"` literal;
- a `.floatUlps()`, `.decimalScale()` or `.floatTolerance()` call on a receiver that is not a two-form kind;
- a branch on a local of those names;
- a `X.foldedAggregate(` call.

It cannot fire on anything else, and that was true from the day it was written.

**Before the final train, or made so by it?**
- The guard was born in train 17 (`f2fabb090`, 07:09), well before the final train's base `815de5810`
  (`git merge-base --is-ancestor`: yes).
- Its rule vocabularies were already dead at that base:
  - the judging rules' 16 sites went at `a5ca7def8` (train 17);
  - `foldedAggregate` went at `612b19470` (train 18).
- At `815de5810` only UPSTREAM_SHAPES still matched live code, the 9 sites.
- The final train's only change to the file (`git diff 815de5810 68f20eb4d`) empties SITES, with a dated note
  (`61e5205d1`, F's L10). Vocabulary, scope and tests are byte-identical.

So "matches nothing that exists" became true in the final train, but by the guard's designed shrink direction, not by
loosening it. Whether L10's way of reaching 0 is legitimate is GUARDS-2, a question about the meaning rule, not about
this guard. L10 replaced each literal with `styled().forms().unionFrame()`, and the union-frame decisions stay in
Lowerer, Fold and SubselectPrune. The user approved L10's inclusion ("Include both", 13:05) but did not rule on that
meaning question.

**What regression would it hide now?** The same ones it always hid:
1. A product branch on an upstream shape under any other name. Examples:
   - `"SQLNull"` or `"unionBase"`;
   - legend-engine's union-frame logic behind a two-form answer (by design, the meaning rule's "designed mechanism").
2. Any such branch outside the three packages. The live example is `lineage/ScanRelations.java:1536` today.
3. A scope rot. Renaming `lowering` would make the scan read 0 files and pass. An empty declared source list would
   not slip through, because sibling guards in the same `:guardrails` JVM read the same `-Dlegend.sources` and have
   floors: `CompatSpellingGuardTest.java:504` requires at least 1,000 files.

None of these is a regression introduced by this PR. Product answers did not move in the area it was closing: the lean
baseline is unchanged, the edge contract and the corpus are byte-identical per the handoff §8.1, and GUARDS'
measurement agrees.

**Why GuardNamesExistTest does not catch it** (`core/src/test/java/com/legend/GuardNamesExistTest.java`). It reads three
kinds of name, and none of UpstreamBranchesTest's vocabulary is one of them:
- string literals that spell a fully qualified class, `com.legend(.pkg)*.Class(#member)?` (`NAME`, :132-133), in every
  census and guardrail source;
- `haveName(Not)Matching` patterns, in `ArchitectureTest.java` only (`if (file.equals("ArchitectureTest.java"))`, the
  `stale()` method);
- `resideIn*/resideOutsideOf*Package` literals, also in `ArchitectureTest.java` only.

UpstreamBranchesTest's vocabulary is bare text:
- `"lowering"`, a path fragment joined to `JAVA_ROOT`;
- `"unionAlias"`, a SQL frame name;
- `"foldedAggregate"`, a bare method name;
- three accessor names inside a `Pattern.compile`.

The javadoc names the limit itself: "a name ... written without its com.legend prefix ... is not read". And even a
check that did read these names would not settle vacuity:
- `"unionAlias"` exists (in the edge and in lineage);
- `floatTolerance` exists (on VerdictNumbers).

The guard reads nothing because of where it looks and whom it exempts, not because the names it holds are absent. The
missing check is a coverage floor plus a bite through the real `found()`.

**Probe** (add to `UpstreamBranchesTest`; passes today, and fails if the scope rots):
```java
@Test
void theScanReadsItsPackages() {
    long files = PACKAGES.stream().mapToLong(p -> SourceFiles.under(JAVA_ROOT + p).stream()
            .filter(f -> SourceFiles.path(f).endsWith(".java")).count()).sum();
    GuardCoverage.assertFloor("UpstreamBranchesTest", files, 180);   // 190 at 68f20eb4d
}
```
The widening probe: `python3 -I audit/verify/scripts/ubt_widen.py <pr118>` prints the lineage site.

**Fix (FIX-AFTER), about 25 lines plus one ruling:**
- add the floor above;
- add a bite that runs `found()`'s per-file body on a planted path;
- set PACKAGES to every product package except `edge/`. That surfaces `lineage/ScanRelations.java:1536`, which needs a
  RULED row under C8 ("Lineage rebuilt upstream's way"), or a move.

Correct the handoff §2.4 row: L-A's second half is "a tripwire on five names in three packages", not "branches on no
upstream shape".

### GUARDS-3: the TEXT bucket's fixed list

**Facts:**
- `CompatSpellingGuardTest.java:54-100`: 54 LITERALS and 22 SYMBOLS (re-parsed).
- `hits()` (`:509-533`) does a substring match on each literal and a whole-word match on each symbol in code.
- `CompatConstantPoolTest.java:100,111` reuses the same two lists against the bytecode.
- `"classBuilder"` appears only at `edge/EngineResultEnvelopes.java:88` in main sources. It is not on either list.

I re-ran `audit/verify/scripts/csg_coverage.py` and reproduced the auditor's numbers:
- 93 edge files;
- 1,426 literal occurrences of 4 or more characters that are not bare identifiers, 1,125 of them distinct;
- 119 occurrences (65 distinct) caught.

That script skips bare identifiers, so `"classBuilder"` and `"tdsBuilder"` are not even inside the 1,125; the 6% is
measured on sentence-like literals.

Running the guard's `hits` logic on planted literals gives:

| Planted literal | Hit |
|---|---|
| `classBuilder` | none |
| `tdsBuilder` | `tdsBuilder` |
| `SQLNull` | none |
| `unionAlias` | none |
| `equal_unionAlias` | none |
| `Query project group ID is invalid` | none |
| `assertTdsEquivalent: cell ` | none |

**Planted-violation claim: confirmed by reading every guard that could see it.** Take a new product class (say in
`com.legend.result`) with `static final String ENVELOPE = "classBuilder";`, read by product code:
- **TEXT and its constant-pool twin:** not on the list.
- **CODE / BUILD:** no edge dependency.
- **EdgeOnlyMembersTest:** the member has product callers.
- **EdgeOnlyDataTest:** not a component of the MIR or the plan.
- **ProductFormsUseTheirArgumentsTest:** not a function argument.
- **UpstreamBranchesTest:** not in its vocabulary.
- **JavaEvalLedgerTest.EVICT_SIZE:** pins only 29 named judge and test-planner files (listed by parsing it), so a new
  class or a line in Lowerer moves no pin.

**New or old:** new. Not weakened during the PR:
- the list grew from 0 to 54 LITERALS and 22 SYMBOLS;
- the per-commit list history shows removals only as side-branch merge churn that was re-added, plus one dated
  narrowing (`9ae8f6f86`, 2026-11-12: "index out of bounds" became the whole old sentence).

**Ruled:** the limit is stated in the class javadoc (:42-45, 2026-11-12). **Covered:** no.

**Probe** (fails today):
```java
@Test void aProductEnvelopeLiteralIsSeen() {
    assertEquals(Map.of("classBuilder", 1), CompatSpellingGuardTest.hits("final class P { static final String S = \"classBuilder\"; }"));
}
```

**Fix:** derive the vocabulary from `com.legend.edge` constant pools, minus a short stoplist, and match it against the
product pools (`ConstantPools.jars` exists), about 60-100 lines. Register lineage's legend-engine label grammar
(`ScanRelations.java:1451,1536,1609`; `LineageTreeLines.java:149`) under C8, or move it.

### GUARDS-4: L-A's first half sees only function arguments

**Facts:**
- `ProductFormsUseTheirArgumentsTest.java:324-334`, `uncalled()`: a hook method is skipped unless one of its parameters
  `carriesFunction` (:186-189: a `java.util.function` type, Runnable, Callable, `@FunctionalInterface`, or a record
  with such a component). Every other parameter gets a sample (`samples`/`sample`, :206-247) and is never checked for
  use.
- The two cited examples are as described:
  - `ProductLoweringForms.java:26-28` `raise(kind, checked, plain)` returns `checked.get()`; `plain` is unused.
  - `:31-33` `view(body, alias, view)` returns `body`.
  - Both are benign. At `Scalars.java:3024` `plain` is the `value` already inside the checked CASE; at
    `Lowerer.java:1236` `view` is `v.view()`, the user's view name.

**Planted violation (confirmed by reading):**
1. Add to the two-form kind `LoweringForms` a method `SqlExpr cell(SqlExpr own, SqlExpr upstream)`. The product's form
   returns `own`.
2. Product lowering calls `styled().forms().cell(x, new SqlExpr.StringLit("classBuilder"))`, eagerly building
   upstream's alternative.

What each guard does:
- **ProductFormsUseTheirArgumentsTest:** skips the method (no function parameter).
- **HookRulingTest criterion (a)** (`:265-280`): checks only that the interface holds bare signatures.
- **PlanStyleSurfaceTest:** pins PlanStyle's own methods (`STYLE_METHODS`), not a two-form kind's.
- **HookLambdasTest.TWO_FORM_CALLERS:** pins callers of `PlanStyle.twoForm`, not `forms().x()` calls.
- **TEXT:** does not see `classBuilder` (GUARDS-3).

Nothing fails.

**Also:** `HookRulingTest.aHooksArgumentTypesAreThoseBesideIt` asserts that `String`, `SqlExpr` and `Type` are not hook
argument types. But `hookArgumentTypes()` only ever adds types from the hook's own package, and no hook lives in
`java.lang`, `com.legend.sql` or `com.legend.compiler.element.type` (`PlanStyleFacets.KINDS`/`TWO_FORMS`). So the
`assertFalse` cannot fail. That is a small dead assertion of the same family, NOTE.

**Probe** (fails today: empty set):
```java
interface PlantedPlain { com.legend.sql.SqlExpr pick(com.legend.sql.SqlExpr own, com.legend.sql.SqlExpr upstream); }
@Test void aPlainArgumentTheFormNeverReadsIsSeen() {
    PlantedPlain form = (own, upstream) -> own;
    assertFalse(uncalled(form, PlantedPlain.class, new java.util.TreeMap<>()).isEmpty());
}
```

**Fix:** scan each product form's bytecode for parameter slots it never loads (`ALOAD`/`ILOAD n`), and pin each slot
with its reason, as RULED is today; about 60 lines. New in PR, not ruled, FIX-AFTER.

### GUARDS-6: the net's id mask and row sort

**Where the mask is applied:** `pct/src/test/java/org/finos/legend/lite/pct/extension/PctCaseRecorder.java`:
- `:64-65` defines `IDS` as a UUID, or `\b[0-9a-fA-F]{16,}\b`.
- `flat()` (`:67-68`) applies it to every line's text before recording (`edge()`, :87-98). So the mask applies to
  VALUE lines, RELATION columns, activities and rows alike, and to ERROR lines.
- The mask has been there since the net's first commit, `17eeb38aa` (2026-11-16 15:38).
- pure/v1's net masks UUIDs only (`core/src/test/java/com/legend/server/PureV1EdgeGoldenTest.java`, its `IDS`).

**Where the sort is applied:** `tools/edgenet/EdgeNormalize.java`:
- `normalized()` (:47-59) splits a RELATION line's rows on U+001E and sorts them;
- `:38` sorts the file's lines.

VALUE lines are not reordered, so a collection's order is still checked. The pct/BUILD.bazel lines 836-856
(`edge_norm_*`) feed the normalizer's output into `update_edge_contract` (`write_source_files`, :859-863).

**The mask behaves as claimed.** A Python run of the same regex (Java's `\b` is the same for ASCII):

| Input | Masked |
|---|---|
| `Long:9223372036854775807` | `Long:<id>` (and `...806` identical) |
| `Double:0.30000000000000004` | `Double:0.<id>` (and `...0009` identical) |
| `Double:0.8414709848078965` | `Double:0.<id>` (and `...966` identical) |
| `Double:1.2345678901234567E-5` | `Double:1.<id>-5` (`E` is a hex digit) |
| `SUM(x) > 9223372036854775807` | `SUM(x) > <id>` |
| `Double:1.414213562373095` (15 digits) | kept |

**What is masked in the goldens:** 591 masked fragments are not trace ids.
- 171 are on VALUE lines: Double 118, Long 40, BigInteger 5, String 8. The String cases are the GUID and the
  MD5/SHA1/SHA256 hashes.
- 18 are inside relation rows.
- 402 are inside the activity SQL. These are deterministic constants, chiefly the DuckDB overflow guards
  `SUM(..) > <id> OR SUM(..) < -<id> THEN json_e…` and a frame offset `1.<id>-5`.

The 1,486 trace ids are all UUIDs (`PureV1Writer.traceComment(java.util.UUID)`, :141-143), so the UUID branch alone
masks them; the 16-or-more-digit branch masks no non-deterministic id that I could find.

**Would the change pass the net? Yes, for:**
- the low-order digits of any Double printed with 16 or more consecutive digits;
- any digit of a Long or BigInteger of 16 or more digits;
- a changed hash;
- a changed overflow bound or long constant in the activity SQL;
- any relation row order.

A change from a long number to a short one (`0.30000000000000004` → `0.3`) is still seen.

**Which other checks would still catch it:**
- **PCT channel A.** The test's own legend-pure assert runs over lite's value, so a value the assert rejects flips the
  outcome. The flip shows in the net's own `*_outcomes.tsv` and in CI's `pct_duckdb`/`pct_h2`/`pct_postgres` lanes
  (gates/BUILD.bazel:395-410) against the `Test_LegendLite_*_PCT.java` ledgers. The tests in question pass today and
  their outcomes are pinned: testLargePlus, testBigIntAbs, testBitNot_LargeNumbers, testSine, testCosine, testMD5Hash
  (`*_outcomes.tsv`).
- **Row order** of a sorted query is caught where the PCT assert compares ordered text.
- **PCT channel B does not catch a small Float drift.** Compatibility mode compares Floats within 2 ULP
  (SEMANTICS_REGISTER S132, "the 2-ULP tolerance is backwards-compatibility mode's alone").
- **pure/v1's net** keeps full digits (`cell.float` records `0.3333333333333333`) and does not sort rows, but it has
  only 14 requests.
- **Core's own tests** pin exact values on many such paths.

**Weaker than the handoff says:** yes. Handoff §2.4 reads "every upstream-facing output ... matches goldens", and the
recorder's javadoc (:39) says "Trace and execution ids are masked". In fact the net also masks values and SQL
constants, and ignores relation order.

**Does it hide a regression in this PR?** I could not construct one from the record, and found no evidence of one.
- The goldens date from 26717de291 (inside this PR's history; `5a103dc8a`). They were re-recorded 3 times:
  - `719953eb3` added `activities` only. I checked this: the five re-recorded files are identical once activities are
    stripped.
  - `ad6b9a199` changed 3 SQLite SQL texts with identical answers.
  - `cc1c4d6e9` changed channel B outcomes.
- So a hidden change would have to be a masked-digit move between 26717de291 and the head that no exact PCT assert,
  core test or lean count notices. Proving its absence takes a run.

**Probe (run):**
1. In a scratch worktree at the head, drop the `|\b[0-9a-fA-F]{16,}\b` branch from `PctCaseRecorder.IDS` and from
   `EdgeNormalize`'s inputs.
2. `bazel run //pct:update_edge_contract` twice. The two runs must be identical (G10).
3. Do the same at `26717de291`.
4. `diff` the two golden sets. Any VALUE or row line that differs beyond the activities' SQL is a hidden move.

**Fix:** mask UUIDs only (about 5 lines); keep row order for queries whose plan sorts; re-record once. New in PR, not
ruled, FIX-AFTER.

### GUARDS-5 / ARCH-5: the net pins lite's SQL text

**Confirmed for PCT:**
- `PctCaseRecorder.java:109-114` writes ` activities=` before the rows.
- Every one of the 1,486 RELATION lines in `pct/src/test/resources/edge/*_answers.tsv` carries it.
- `ad6b9a199` re-recorded 3 SQLite lines whose rows, columns and shape were identical, because the product SQL improved.
  Its message: "legend-engine has no SQLite extension, so the edge's SQLite is the product's".

**Confirmed for pure/v1:** `core/src/test/resources/edge/pure_v1_answers.tsv` has 28 lines, of which 19 carry `"sql"`:
- the execute answers' `activities`, which hold lite's executed H2 SQL (`tds`: `SELECT "t0"."YEAR" AS "year" ...`);
- the 14 `plan.*` generatePlan lines.

**Who reads `activities`:**
- **Lite's PCT adapter does not.** `pct/.../extension/ValueBridge.java:550-578` `engineTDS()` reads `builder.columns`
  and `result.rows` only (verified). `grep activities pct/src/test/java/.../extension/*.java` hits only the recorder.
- **legend-engine's own PCT adapter (unverified here).** ValueBridge's javadoc says it reads the answer "as
  legend-engine's own PCT adapter reads it (pct_relational.pure: parseJSON, then fromJSON into its RelationalTDSResult
  ... getTDSResultFromProtocol)". That file is not in this environment (no checkout, no jar), so I could not read it.
- **legend-pure's PCT framework** judges only the test's asserts over the adapter's returned value. So no PCT verdict on
  either side reads `activities`. ARCH-5's claim holds, with the citation corrected: ValueBridge is lite's adapter,
  modelled on legend-engine's.
- **For pure/v1, `activities` is real answer bytes.** `PureV1Writer.java:132-150` reproduces legend-engine's
  `RelationalExecutionActivity(sqlQuery, sqlComment)`, citing `RelationalExecutor.java:332,351`. Legend Studio's query
  builder shows the executed SQL from `activities`, as I recall; I have not verified that here. But those bytes are
  lite's SQL, which no client can compare with legend-engine's.

**Fix:** pinning them makes every product SQL improvement read as an edge regression (handoff §4.3). Drop
`activities[].sql` from the PCT answer lines in `EdgeNormalize` or the recorder, about 10 lines plus one re-record. For
pure/v1, keep only structure and columns or move the SQL to an informational file. FIX-AFTER, the portability
program's first commit, as ARCH-5 says.

### GUARDS-8: shrink_only cannot judge this PR

**Confirmed:**
- `tools/guards/shrink_only.py:90-130` lists 19 WATCHED files. Only `JavaEvalLedgerTest.java` and `ArchitectureTest.java`
  exist at `4dfbb5b7`; I checked each with `git cat-file -e`.
- `:42` and `:792-793` say "A pin new at the head ... is not checked".
- Run read-only with `--repo /home/user/legend-lite`, it exits 0 ("45 pins in 19 files") with `--base 4dfbb5b7` and
  with `--base 815de5810`.

**Also confirmed: the checker is absent at main.** `tools/guards/shrink_only.py` does not exist at `4dfbb5b7`, and
`gate.yml:151-154` falls back to the head's copy ("a base without one runs the head's (the check's first landing)").
So on this PR's CI, the PR's own checker judges the PR, against a base where 17 of its 19 files do not exist.

**Vocabularies unwatched: confirmed.**
- GROW_ONLY (`:134-136`) holds only EdgeSeparationTest's `COMPAT_CLASSES`, `EDGE_ONLY_SQLFN` and `REFEREE`.
- These are not watched for removal:
  - `CompatSpellingGuardTest.LITERALS` and `SYMBOLS`;
  - `UpstreamBranchesTest.UPSTREAM_SHAPES`, `PACKAGES` and `EDGE_SHAPE_READERS`;
  - `EdgeOnlyDataTest.PACKAGES` (:55).
- `EdgeOnlyDataTest.CODECS` (:59), whose growth exempts more, is not WATCHED.
- `RULE_READ` and `RULE_BRANCH` are `Pattern.compile` initializers, which the script refuses to read (docstring
  "Anything else ... is REFUSED"), so they could not be watched as written.

**The real evidence for "no pin grew undated":** after merge, the 45 pins become live for every later change, because
the 17 files will exist at the merge-base. The vocabulary gap remains. The PR's own evidence is the manual
raised-pin check against main: 6 raised, all dated. I spot-checked DatabaseJudge `790 -> ... -> 851` at
`JavaEvalLedgerTest.java:592`; every step is dated.

**Fix:** add the vocabularies to GROW_ONLY and `CODECS` to WATCHED, about 10 lines. Say in the handoff that
`shrink_only` on PR #118's CI does not judge the PR's own pins. NOTE/FIX-AFTER.

### GUARDS-11: the corpus database judge deleted

**Facts:**
- `b523c767d` (2026-11-17 02:26) deletes:
  - the corpus database pass;
  - 8 registers per lane, plus 2 ceilings;
  - the net's database half, 4 files;
  - the CI log upload in `gates-run.yml`.
  That is 30 files, +33/-12,683 lines.
- The registers and the pass exist at main: `4dfbb5b7:spec/corpus.bzl:64` defines `judge_database_<lane>`, and `:105`
  passes `-Dlegend.judge.mode=database`.
- At the head:
  - `spec/corpus.bzl:40-81` builds `judge_host_<lane>` only.
  - `MinimalCorpus.java:336-341` still accepts `both`, but no target passes it
    (`grep -rn "judge.mode=both|judge.mode=database"` over spec/, scripts/, .github/, tools/ and gates/: none).

**The user's ruling** (plan of record, log 2026-11-17 01:30, verbatim):
> **RULED by the user, the judge model ("Love it"):** HOST is the COMPATIBILITY judge ... the verdict of record on PCT
> and the corpus ... DATABASE is the PRODUCT judge ... The blocking interchangeability check is CORE: both judges,
> isolated, under lite's rules, must agree on verdict and message ... **The user's follow-up:** is the database judge on
> PCT/corpus worth running at all? If not, compatibility is HOST only and core must flex both judges ... **Decision
> rule, set up front:** at least 95% agreement and mostly real-bug signal means a nightly non-blocking sanity check
> with a one-way ratchet; otherwise the database judge is dropped from compatibility runs. Either way, the shapes only
> PCT/corpus cover are ported into core.

Then at 02:30:
> **DECIDED (the rule set up front, plus the user's leaning):** The corpus runs HOST only NOW ... PCT runs HOST only
> after the 36 combos are ported into core ... CORE is the blocking check ... The ~8 possible real bugs go on F40.

The measurement: 61% agreement on the corpus, about 95% of the disagreements compatibility noise; 100% on PCT channel
B.

So there is no separate user "go" for the deletion. The orchestrator applied a decision rule that sits inside the
user's ruling, on the user's own conditional. That counts as ruled.

**Is corpus judge parity unmeasured anywhere? Yes.** G2 is still measured in two places:
- **Core's judge matrix** (`//core:core_tests_judges`): every judged package under DATABASE and under BOTH,
  `core/BUILD.bazel:1098-1102`, in the `//gates:core` CI suite (`gates/BUILD.bazel:38`).
- **PCT channel B**: BOTH isolated by default (`ChannelB.java:398-399`, `-Dpct.channelb.judge` defaults to `BOTH`).
  DuckDB runs in the CI lanes (`gates/BUILD.bazel:407`); the other databases run in manual or lane sessions.

**What the deletion cost, from the record:**
- On 2026-11-16 20:20 the corpus BOTH check had just found 9 real failures that DATABASE-alone hid (plan-of-record
  log).
- The measurement found about 8 possible real bugs (F40).
- With the corpus pass gone, a product-judge defect in a shape that only the corpus exercises shows nowhere.

**Two loose ends, FIX-AFTER (not blockers):**
1. **The port is untracked.** The ruling's "Either way, the shapes only PCT/corpus cover are ported into core" has no
   follow-up row, and no entry in `edge-follow-ups-2026-11-18.md` mentions combos, shapes or a port. PCT's shapes are
   still covered by channel B's BOTH; the corpus-only ones are not covered at all. Add the row, owner E-Java, before
   "PCT runs HOST only".
2. **G2's text is stale.** `docs/GOALS_OF_RECORD_2026_11_07.md:40-52` still says "Guard: the agreement judge (BOTH) on
   every lane; the gained and lost registers empty", and this PR does not touch the file. Amend it to the 2026-11-17
   judge model, citing the ruling.

Follow-up F16 ("Retire the DATABASE-alone CI pass once it differs from BOTH nowhere") is also stale.

**Verdict:** NOTE (ruled), not a blocker.

## Summary of grades

- GUARDS-1: FIX-AFTER. The facts are right. The guard is a narrow tripwire, not vacuous, and the final train only
  emptied its pin; it never existed at main. The handoff overclaims it. Its own vocabulary has a live product site
  outside its scope: `lineage/ScanRelations.java:1536`.
- GUARDS-3: FIX-AFTER. Confirmed: `"classBuilder"` in a product class passes every guard. The limit is stated in the
  guard's own javadoc.
- GUARDS-4: FIX-AFTER. Confirmed: a new two-form method with a plain, eagerly built upstream argument passes every
  guard. HookRulingTest's String/SqlExpr `assertFalse` cannot fail (NOTE).
- GUARDS-5 / ARCH-5: FIX-AFTER. Confirmed: no PCT verdict reads `activities`. In pure/v1 they are answer bytes, but
  they hold lite's SQL.
- GUARDS-6: FIX-AFTER. Confirmed: the mask hides 591 deterministic fragments, and relation order is unchecked. Channel A
  and core cover exact-asserted values; channel B's 2-ULP tolerance does not. No hidden regression was found; the probe
  above settles it.
- GUARDS-8: NOTE/FIX-AFTER. Confirmed, plus the checker itself is new, so CI runs the PR's own copy.
- GUARDS-11: NOTE. Ruled (the judge model, "Love it", and the user's conditional). Corpus parity is now unmeasured.
  The ruling's port condition and G2's text need follow-ups.

**Blockers: none.**
