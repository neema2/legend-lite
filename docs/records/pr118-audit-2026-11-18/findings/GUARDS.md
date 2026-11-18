# GUARDS: verdict SOUND-WITH-DEBT

## Summary (at most 12 lines; answer your brief's questions first)

1. **Census soundness.** The separation guards are real. BUILD, CODE, TEST, EDGE-ONLY members, L-B data, shipped roots, the hook criteria and PLANNER_ENTRIES are each exact in both directions, each bitten by a planted violator, and each runs in CI's `checks` lane. GuardNamesExistTest directly closes the failure class that main's review found (rule targets and allowlists naming deleted things). The meaning lenses are much narrower than the handoff claims: **UpstreamBranchesTest now matches nothing that exists**. Its five names are deleted or moved, it scans 3 packages, and it has no coverage floor. The TEXT guard sees 65 of the edge's 1,125 distinctive literals (6%). A product `"classBuilder"` envelope passes. L-A sees function arguments only. So "census 0" means "no edge dependency, no edge-only member or datum, nothing on the lists", not "no upstream meaning". One real residue that no guard sees: legend-engine's union-frame logic is still in product lowering. Its line, L-A.unionAlias, closed when the literal was swapped for a hook answer (GUARDS-2).
2. **Weakening.** I found no pin raised or entry added without a dated note, against `main` or in the final train. But `shrink_only` cannot judge this PR: 17 of its 19 watched files do not exist at `main`, and it skips pins that are new at the head. Two smaller gaps: dated notes can be false (four "deleted in train 17" rows still stand), and removals from the vocabulary lists are not watched.
3. **Safety net.** About 94% of the +23.2K lines under `pct/` and `spec/` are recorded data: 21.8K lines of goldens. The net compares outcomes, verdicts, values and messages. But every RELATION answer also embeds the SQL that ran (1,490 of 5,891 answer lines). A SQL-only change therefore reads as a regression, which confirms handoff §4.3. Its id mask also hides any number of 16 or more digits (171 VALUE lines). The net is not in CI.
4. **Proportionality.** The PR adds +17.2K lines of guard code for a 19.8K-line edge. That code contains 9 hand-written Java lexers, jar import repeated in 14 files, and history kept as comments; it is heavy but readable.
No blockers: nothing here hides a wrong answer today, and every item is cheap to fix after merge.

## Blockers (if any; each one also appears in Findings)

None.

## Strengths

- **Exact both ways, with planted bites.**
  - BUILD: a planted root, `//core:planted_edge_reach` (EdgeSeparationTest.java:449).
  - CODE and TEST SIDE: planted product classes, plus 19 planted shapes that try to launder an edge-only SqlFn (EdgeSeparationTest.java:453-520).
  - EdgeOnlyMembersTest: a planted API covering a static, a constructor, a field, a constant read in source, and transitive and lambda reaches (EdgeOnlyMembersTest.java:404).
  - L-A: a planted two-form kind (ProductFormsUseTheirArgumentsTest.java:501).
  - L-B: a planted MIR, including a rewriter that decides versus one that copies (EdgeOnlyDataTest.java:417).
  - Shipped roots: a planted edge-rooted artifact and an opt-in carrier (ShippedArtifactRootsTest.java:80).
  - The hook rules: one bite per host-door rule (ArchitectureTest).
- **Empty imports fail.** Every exact map holds pins that must be found, so an import that silently read nothing fails: HOOKS_RULED has 29 entries, READ_API 95, EdgeOnlyDataTest.OPEN 16 and ProductForms RULED 7.
- **GuardNamesExistTest (791 lines)** reads every census and guardrail source for three kinds of name, and checks that each exists:
  - class and member literals;
  - `haveName(Not)Matching` patterns;
  - package literals, including those starting with `..`.

  Tripwires are listed on purpose with dates (`ABSENT_ON_PURPOSE`). This is the right fix for main's "rule names a deleted package" defect.
- **TaggedClassesHaveATargetTest** checks that every census and guardrail class runs in a target the lanes reach, so a guard cannot exist and never run.
- **CompatConstantPoolTest** closes the source scan's split-literal and borrowed-constant holes by reading the product jars' constant pools.
- **HookRulingTest's K10 re-cut** kept a planted bite (`theRaisePinBitesWordsBesideTheToken`, :794) when its old bite target (the edge dialects) stopped differing, so the pin was never left unproven.
- **No undated growth.** Every pin raised against `main` carries a dated note. JavaEvalLedgerTest: DatabaseJudge 790→851, HostJudgments 633→679, BodyDriver 522→545 and AggAwareActivities 211→226 (the user's word). Also FixtureWriteCensusTest 950→951 and CatalogUpstreamDiffTest 70→72 (the user's word, 2026-11-16).
- **The final train's lean SQL claim holds.** In the core lean baseline, SQL columns (statements, chars, subselects, repeated, refused) are unchanged in all 3,777 rows from `815de5810` to the head. Plan JSON rose by 6,523 characters across 66 rows and fell by 3,519 across 40. Every rise is a multiple of 31 (v57) or one of L14's three rows (+8, +25, +42), all approved by the user.
- **`shrink_only.py` is careful in its own terms.** It refuses initializer shapes it cannot read, by name. It runs the merge-base's copy of itself. It treats a vocabulary removal as a loosening (GROW_ONLY). It names its limits honestly in its docstring.

## Findings

### GUARDS-1 [WRONG-DESIGN] [FIX-AFTER] L-A's second half (UpstreamBranchesTest) matches nothing that exists, so LENS 0 is vacuous for "branches on upstream's shape"
- Where:
  - core/src/test/java/com/legend/UpstreamBranchesTest.java:39 (PACKAGES), :42 (UPSTREAM_SHAPES = "unionAlias"), :49 (EDGE_SHAPE_READERS = "foldedAggregate"), :52 (RULE_READ: floatUlps, decimalScale, floatTolerance), :66 (SITES = {}).
  - The handoff's check table: "L-A ... the product computes no upstream alternative and branches on no upstream shape".
- What: the whole vocabulary names code that is gone from the scanned packages.
  - `foldedAggregate` is deleted (SnapshotEnvelope.java:32).
  - The JudgingRules accessors are deleted. `floatUlps` and `decimalScale` survive only in testexecutionplan/, edge/ and the two-form kind VerdictNumbers, whose receivers the scan exempts by design.
  - The `"unionAlias"` literal moved to the edge in the final train (L10).
  - The scan reads 3 of about 40 product packages: lowering (31K lines), resolver (36K) and testexec (5.6K) of about 430K.
  - It has no `GuardCoverage.assertFloor`, unlike CompatSpellingGuardTest.java:504, so a scan that reads zero files also passes.
  - Its bite test (`theScanBites`) runs on string snippets, never on the real import.
- Why it matters: the census's LENS bucket reads 0 partly because this guard can only fire if one of five exact deleted names returns. A new product branch on an upstream shape passes, whether it uses another frame name, a `"SQLNull"` arm or a method reference. That is the same "a rule that matches nothing passes silently" failure that main's review found. The handoff states a property the guard does not check.
- Instead:
  - Either retire the guard and say in the handoff that L-A's second half is a tripwire, or give it a mechanical vocabulary.
  - Derive the vocabulary from the edge: every distinctive string constant of `com.legend.edge` classes, read from their constant pools, minus a short generic stoplist. Match it against product constant pools in every package, not three.
  - Add a coverage floor.
- Confidence: verified (greps over core/src/main at the head; SITES and vocabulary read).

### GUARDS-2 [WRONG-DESIGN] [FIX-AFTER] L-A.unionAlias closed by swapping the literal for a hook answer: legend-engine's union-frame logic stays in product lowering
- Where:
  - core/src/main/java/com/legend/lowering/Lowerer.java:2589, 2623, 2973-2978, 3004-3011 ("a join CONTAINING a union frame isolates as one more union frame ... (engine model)"), 3026-3029.
  - lowering/Fold.java:1662 ("reads of a union frame's outputs ... keep it (tds union goldens: "unionalias_0"...)").
  - lowering/SubselectPrune.java:82, 408-409 ("the style's kept union frame (legend-engine's: its user unions list their projected columns, read or not)").
  - At `815de5810`, Lowerer.java held the literal `"unionAlias"` at lines 1473, 2618, 2964, 2969, 3002, 3009 and 3014. At the head each became `styled().forms().unionFrame()`, which the product answers null (ProductLoweringForms.java:105) and the edge answers "unionAlias".
- What: the final train moved one string. The decisions that reproduce legend-engine's union-frame SQL stay in product code, dead under the product's style and live only under the edge's:
  - wrapping a join that holds a union frame in another frame;
  - keeping a union frame's aliases;
  - never pruning a framed union.

  Three guards miss this:
  - UpstreamBranchesTest went 9 → 0 sites, because it sees the literal only.
  - EdgeOnlyMembersTest misses `SubselectPrune.prune(SqlQuery, boolean, String)`, the 3-argument form whose third argument only the edge passes non-null. The product's 2-argument overload delegates to it with `null` (SubselectPrune.java:74-76), so it has a "product caller".
  - EdgeOnlyDataTest names this shape as a limit ("a component the product always sets to null ... is not seen", :47).
- Why it matters: the meaning rule says "renaming a thing does not close a finding; changing what it means does". This is legend-engine's SQL-shape logic in the product, the kind the plan of record puts in the edge's MIR passes. The edge already has four such passes (`PurePrintForms`, `TdsNullCells`, `UnguardedParses`, `StableScanOrder`). There is no wrong answer: the product's style takes the null path.
- Instead:
  - Make legend-engine's union framing a fifth edge MIR rewrite over the product's unframed SQL, and delete the frame-name parameter from Lowerer, Fold and SubselectPrune.
  - Or reopen L-A.unionAlias as a counted line until that is done.
  - Teach a lens the "parameter only the edge passes non-null" shape: in the bytecode, a call site loading `aconst_null` or a constant into a parameter that only `com.legend.edge` call sites pass a live value for.
- Confidence: verified (code at the head and at `815de5810` read). Whether the meaning rule's "a branch on a two-form kind's answer is the designed mechanism" was meant to cover a whole upstream algorithm behind a branch is for the owner to rule.

### GUARDS-3 [WRONG-DESIGN] [FIX-AFTER] The TEXT bucket's fixed list sees about 6% of the edge's own strings; a product class holding legend-engine's class-result envelope passes
- Where:
  - core/src/test/java/com/legend/CompatSpellingGuardTest.java:54-100 (54 LITERALS, 22 SYMBOLS); CompatConstantPoolTest reuses the same lists.
  - The edge's envelope literal: core/src/main/java/com/legend/edge/EngineResultEnvelopes.java:88 (`"classBuilder"`).
  - edge/StoreWords.java.
  - lineage/ScanRelations.java:1451, 1536, 1609 and compiler/spec/LineageTreeLines.java:149.
- What:
  - A Python re-implementation of the guard's `hits()`, run over the edge's 1,125 distinct non-identifier literals (4 or more characters), matches 65 of them (119 of 1,426 occurrences).
  - A product class holding `"classBuilder"` is caught by no guard: it is not on the list, it creates no edge dependency, it is not a function argument and it is not plan data. `"tdsBuilder"` is caught.
  - Of StoreWords' 33 sentence-like literals, such as "Query project group ID is invalid" and "Query with ID '", only 3 are on the list. J9 ("SavedQueries hold none") was closed by a session reading the code; no guard learned to see it.
  - The product's lineage builds legend-engine's labels: `"equal_unionAlias" + name + "_root"`, `"unionAlias" + col + "_" + j` and `"SQLNull"`. No register row exists for them, though the user's ruling C8 ("Lineage rebuilt upstream's way") may cover them.
- Why it matters: EdgeAuditOpenTest says an item leaves either when it closes or "when a structural guard learns to see it". Several closed items (J9, J18, H5) left by reading alone. After merge, a regression that moves an upstream sentence back into the product turns nothing red unless that sentence is one of the 54.
- Instead:
  - Generate the vocabulary from the edge's constant pools, filtered by a short generic stoplist, and match it against product pools. Moving a string back then fails by construction.
  - Register lineage's rows as RULED under C8, or move them.
- Confidence: verified (script `audit/verify/scripts/csg_coverage.py`). The 6% includes many lite refusal strings, so the share of genuinely upstream strings missed is lower than 94%, but the named examples are upstream's.

### GUARDS-4 [WRONG-DESIGN] [FIX-AFTER] L-A's first half sees only function arguments: an eagerly computed upstream value handed to a hook is invisible
- Where: core/src/test/java/com/legend/ProductFormsUseTheirArgumentsTest.java:178-191 (`isFunction`, `carriesFunction`). Examples of the shape: lowering/ProductLoweringForms.java:26 `raise(kind, checked, plain)` ignores `plain`; :31 `view(body, alias, view)` ignores `view`.
- What: the guard calls each product form with sentinels in its function slots and reports slots never called. It does not look at non-function parameters. If the product built upstream's text or SQL as a value and passed it as a `SqlExpr`, `SqlSelect` or `String`, the product's form would ignore it unseen. The two ignored plain arguments above are benign: `plain` is the bare value that `checked` wraps (Scalars.java:3024). But no guard records that they are ignored or why.
- Why it matters: "a product method that computes upstream's alternative and hands it to a hook IS a finding". This guard proves that only for lambdas.
- Instead: report every parameter that a product form's method body never loads (an ASM scan of the method's `ALOAD n`/`ILOAD n` per parameter slot), and pin each with its reason, as RULED is today.
- Confidence: verified (guard and forms read).

### GUARDS-5 [WRONG-DESIGN] [FIX-AFTER] The edge golden net pins lite's own SQL text inside every relation answer and pure/v1 plan
- Where:
  - pct/src/test/java/org/finos/legend/lite/pct/extension/PctCaseRecorder.java:108-114 ("the plan text the edge prints (its activities ...), so a move of the edge's plan text is a visible line too").
  - core/src/test/resources/edge/pure_v1_answers.tsv (the `plan.*` lines are legend-engine plan JSON with SQL).
  - Commit ad6b9a199.
- What:
  - 1,490 of the 5,891 PCT answer lines carry the executed SQL in `activities`.
  - ad6b9a199 re-recorded 3 SQLite lines whose rows, columns and shape were identical, because product SQL improved (S20, "the waste look"). Its message says "the edge's SQLite is the product's".
  - The SQL in `activities` is lite's, never legend-engine's, so it is not an upstream contract.
  - The corpus goldens are verdicts only: judge-host.tsv and outcomes.tsv. The corpus's engine-text asserts do compare upstream's own SQL strings, which is upstream's real contract.
- Why it matters: this is handoff §4.3's cause ("the safety net rewarded copying"). Under the approved portability program (presentation in Java writers over typed rows), every SQL-shape change re-records goldens and needs "a ruling". That is friction that reads as regression.
- Instead: record `activities` in a separate informational file that no diff test reads, and keep the answers file to columns, rows, values and messages. This is a one-method change in PctCaseRecorder plus one regeneration. The pure/v1 plan lines likewise compare node structure, not SQL text.
- Confidence: verified.

### GUARDS-6 [WRONG-DESIGN] [FIX-AFTER] The net's id mask erases every 16-or-more-digit number, so low-order digits of Floats and big integers are invisible to the answers golden
- Where: PctCaseRecorder.java:64-65, `[0-9a-fA-F]{8}-...{12}|\b[0-9a-fA-F]{16,}\b`. Compare PureV1EdgeGoldenTest.java:66, which masks UUIDs only.
- What: decimal digits are hex digits, so the mask also hits values:
  - `testLargePlus` → `BigInteger:<id>`;
  - `testBigIntAbs` → `Long:<id>`;
  - full-precision Doubles such as `Double:0.<id>` (17 significant digits).

  171 VALUE answer lines have masked values; 591 masked fragments exist outside trace ids, some inside SQL constants. EdgeNormalize also sorts every relation's rows, so an ordered result's order is unchecked too.
- Why it matters: S311 and S20 (Float text, DOUBLE literals, the DuckDB overflow fix) are exactly where this program moved semantics. A change in the 16th digit of a Double, or in a 64-bit integer, passes the answers diff. Only an outcome flip (a failing PCT assert) or channel B would catch it.
- Instead: mask the trace id by position (the `executionTraceID` comment) and UUIDs only. Record order for relations whose query sorts, and keep the row sort for the rest.
- Confidence: verified (goldens grepped).

### GUARDS-7 [NOTE] [FIX-AFTER] The edge golden net is not in CI
- Where: gates/BUILD.bazel:489-491 (`//pct:edge_contract_test` in `//gates:heavy`, manual). Not referenced by .github/workflows.
- What: after merge, nothing runs the net automatically. It is in-process (DuckDB, H2, SQLite) and could run in a lane. This is a known follow-up (D6/F33).
- Why it matters: every claim of "edge contract byte-identical" rests on hand runs by integrator sessions.
- Instead: add it to a CI lane once GUARDS-5 and GUARDS-6 make it cheap and meaningful.
- Confidence: verified.

### GUARDS-8 [NOTE] [FIX-AFTER] `shrink_only` cannot judge this PR against main, and its vocabularies are not watched for removal
- Where: tools/guards/shrink_only.py:42 ("A pin new at the head ... is not checked"); WATCHED at :90-130; GROW_ONLY at :132.
- What:
  - Of the 19 watched files, 17 do not exist at `main` (`4dfbb5b7`). Only JavaEvalLedgerTest.EVICT_SIZE and ArchitectureTest.PLANNER_ENTRIES are compared. The run reports "45 pins in 19 files" and exits 0, which overstates its reach.
  - Against `815de5810` it also exits 0, and that run is meaningful.
  - The vocabularies whose removal loosens a guard are not GROW_ONLY: CompatSpellingGuardTest.LITERALS and SYMBOLS, UpstreamBranchesTest.UPSTREAM_SHAPES and PACKAGES, and EdgeOnlyDataTest.PACKAGES. Adding to EdgeOnlyDataTest.CODECS also loosens.
- Why it matters: "`shrink_only` green on PR #118's CI" does not mean "no pin grew during the program". This audit's manual check is the actual evidence (see Strengths). After merge, a later change could drop a literal from LITERALS and pass.
- Instead: add the vocabularies to GROW_ONLY and CODECS to WATCHED, and say in the handoff that the PR's own pins were judged per train, not by the PR's CI.
- Confidence: verified (ran the script at both bases; `audit/verify/scripts/raised_pins.py`).

### GUARDS-9 [COSMETIC] [NOTE] Dated notes that are false, and uncounted categories inside "OPEN"
- Where: core/src/test/java/com/legend/EdgeOnlyDataTest.java:104-110 (four rows "DEAD (2026-11-18) ... deleted in train 17 by its owner"); :118-121 (DecimalDigits "RULED_PENDING ... the size before the user").
- What:
  - The DEAD pieces still exist at the head: compiler/spec/typed/Evaluation.java:105 `Site(Object origin, List<Integer> path)`, and sql/SqlDml.java:33 `InsertFromTable` and :47 `LoadCsv`. Train 17 landed at `3c790ac9aa`.
  - The v57 rows say pending, but the handoff records the user's "keep it" at 11:58.
  - READ_API, DEAD and RULED_PENDING rows sit in `OPEN` and are not counted (`lines()` counts "L-B." only).
- Why it matters: `shrink_only` proves that a dated note exists, not that it is true (its own docstring says so). The census is only as honest as these labels.
- Instead: delete the four dead pieces or correct the note, update the v57 rows to RULED (the user, 2026-11-18), and move the uncounted rows out of a map named OPEN.
- Confidence: verified.

### GUARDS-10 [NOTE] [FIX-AFTER] The edge's targets live in hand lists in several places; one names a deleted target, and the member and data guards do not read native's edge entry
- Where:
  - core/BUILD.bazel:2718 (`_EDGE_TARGETS`), :2802 (`_EDGE_TARGET_LIST`) and :2688 (`edge_jars`: :edge, :edge_server, :edge_service_tests and :edge_store_words only).
  - tools/deps/WasmPlannerClosureTest.java:28 (names `//query-store:edge_page_entry`, deleted with H6).
  - EdgePackageBoundaryTest.java:36-40 (a named limit "after the host train"; the host train has landed).
- What: EdgeOnlyMembersTest and EdgeOnlyDataTest import product, authored and edge jars. `//native:edge_entry` (com.legend.edge.nativelib) is in none of them. A product member used only by native's edge entry therefore reads as unused and is not counted (counting less). A new edge target added without editing each list is invisible to the BUILD and shipped-roots queries. The class-level CODE guard is the backstop.
- Why it matters: this is the stale-list failure class again, just not triggered yet. C.11's "one list of the edge's targets" is still pending.
- Instead: derive the edge target set by one query, for example targets tagged `edge` or whose srcs are under `com/legend/edge/`. Feed every guard from it.
- Confidence: verified (lists read); the counting-less effect is suspected (no instance constructed).

### GUARDS-11 [NOTE] The corpus database judge pass, its per-assert differential and 8 registers per lane were deleted, with a dated reason
- Where: commit b523c767d (2026-11-17). It deleted spec/src/test/resources/rcorpus/*-database-*-register.txt and *-judge-unjudged-ceiling.txt, and the database half of the edge corpus net.
- What: the corpus now runs on the HOST judge only. The justification follows the user's judge model (plan-of-record log 2026-11-17 01:30, "Love it") and a decision rule set up front (at least 95% agreement, or drop). The measurement was 61% agreement, about 95% of it compatibility noise. About 8 possible real bugs were parked as F40.
- Why it matters: G2 (judge parity) is no longer measured on the corpus. Core's isolated BOTH and PCT channel B (100% agreement) carry it. This is ruled and dated, not a silent weakening.
- Instead: keep F40 on the queue.
- Confidence: verified.

### GUARDS-12 [NOTE] Product-side residue pins grew net against main under the orchestrator's rulings
- Where: core/src/test/java/com/legend/JavaEvalLedgerTest.java:592 (DatabaseJudge 790→851), :1053 (HostJudgments 633→679), :847 (BodyDriver 522→545) and :1027 (EngineTextPlanning 151, a new facade).
- What: each move carries a dated note, mostly "isolated BOTH and #28, the orchestrator's ruling: lite's sentence table beside the compat words ... re-pins DOWN". They did re-pin down, but not back to `main`'s values. This meets AGENTS.md's letter (a dated justification). The handoff's stricter rule ("a pin grows only with the user's word, or ... under the user's delegation") is met only for AggAwareActivities and EnginePlans.
- Why it matters: lite's own sentence tables now live in the product judges, which is the designed outcome. The owner may want to confirm these rulings.
- Instead: list them for the user.
- Confidence: verified.

### GUARDS-13 [COSMETIC] [NOTE] The guard code is heavy and duplicated, and keeps its history in comments
- Where:
  - The 23 new guard classes total 10,517 lines; the GUARDS file list adds +17,228 lines.
  - Hand-written Java lexers in 9 files: CompatSpellingGuardTest.lex, UpstreamBranchesTest.stripComments/blankLiterals, GuardNamesExistTest.literals/javaCode, OpenLineMarkedTest, EdgeOnlyNodesCensusTest, PlannerSurfaceTest, DialectLeakRatchetTest, IdentityGuardrailTest and SqlTextRatchetTest.
  - Jar import re-implemented in 14 files, although ConstantPools.jars exists.
  - JavaEvalLedgerTest grew from 238 KB to 311 KB; its longest line is 24,297 characters.
- What: emptied registers are kept as tombstone histories. EdgeAuditOpenTest is about 70% comment lines, and every map in EdgeSeparationTest is now comments only. Three examples: EdgeSeparationTest.java:90-105 (CODE_OPEN), :343-349 (OPEN_LINES) and EdgeAuditOpenTest.java:63-64 with the items' tombstones.
- Why it matters: handoff §4.8 names guard weight as a cost of the program. Each lexer is a place for the next blind spot.
- Instead: one shared source lexer and one jar importer in the test support package; history in git; delete maps that are empty for good, or keep them to one line.
- Confidence: verified (measured).

## Cross-area observations

- **LOWERSQL:** GUARDS-2. legend-engine's union-frame algorithm sits behind `LoweringForms.unionFrame()` in Lowerer, Fold and SubselectPrune; it is the edge's in meaning.
- **PRODUCT:** lineage prints legend-engine's alias grammar in product code: ScanRelations.java:1451 (`"equal_unionAlias"...`), :1536 and :1609 (`"SQLNull"`), and LineageTreeLines.java:149. It has no register row. Check whether ruling C8 ("Lineage rebuilt upstream's way", 2026-11-14) covers it and record it.
- **PRODUCT (suspected):** EdgeOnlyMembersTest's READ_API row `BodyDriver$ExecEnv#appealOn()` ("lite's test planner's own appeal environment") labels the appeal plumbing. By the goals of record (P1.2), "a compatibility appeal is the edge's".
- **HOSTS:**
  - No spelling scan covers the TypeScript or Python product modules; only layering and closure guards do. Closing H5 relied on reading.
  - Suspected: //core:server ships libedge.jar on its classpath through :edge_server. `TestPlanJson.Codecs.load()` (testexecutionplan/TestPlanJson.java:293-297) runs `ServiceLoader.load(ExtensionCodec)`, whose one provider is `com.legend.edge.EdgeCodec` (ServiceLoaderSitesTest PROVIDERS). If any product server path reads a test plan's JSON, an edge-off server loads an edge class. No test asserts "edge off loads no edge class" for the JVM server.
- **JUDGES (suspected):** the sentence `"assertTdsEquivalent: cell " + i + " expected "` appears both in the product host judge (testexec/TdsCompare.java:150) and in the edge's legend-pure words (edge/UpstreamVerdictWords.java:124). If it is legend-pure's sentence, the product judge writes upstream's words, and the TEXT list misses it.

## Measurements (numbers with the command that produced them)

- **The edge's literals as the TEXT guard sees them:** 93 edge files; 1,426 literal occurrences (1,125 distinct); the guard's LITERALS catch 119 occurrences (65 distinct). Command: `python3 -I audit/verify/scripts/csg_coverage.py <pr118>`.
- **Sentence literals caught, per edge file:** StoreWords 3/33, PureMessages 72/142, EdgeHostWords 10/22, UpstreamVerdictWords 12/28. Command: an inline script reusing csg_coverage's lexer (transcript).
- **`shrink_only` at both bases:** `python3 -I pr118/tools/guards/shrink_only.py --repo /home/user/legend-lite --base 4dfbb5b7 --head 68f20eb4d` exits 0 ("45 pins in 19 files"). The same against `815de5810` exits 0. Of the 19 watched files, only 2 exist at `main` (`git cat-file -e 4dfbb5b7:<file>` per file).
- **Pins raised against `main`:** 6 found; 5 have a date within 3 lines, and the sixth (CatalogUpstreamDiffTest) is dated in its javadoc. Command: `python3 -I audit/verify/scripts/raised_pins.py 4dfbb5b7 68f20eb4d`.
- **EVICT_SIZE:** 28 → 29 entries; total 10,498 → 10,135. Command: `python3 -I audit/verify/scripts/evict.py 4dfbb5b7 68f20eb4d`.
- **Lean baseline, `815de5810` → head:** 3,777 rows; statements, chars, subselects, repeated and refused all unchanged. Plan JSON +6,523 over 66 rows and −3,519 over 40 rows. Rises by delta: 31 ×28, 93 ×11, 217 ×11, 186 ×11, 62 ×2, and 8, 25 and 42 once each. Command: an inline script over `git show <rev>:core/src/test/resources/lean-sql/baseline.tsv`.
- **The +23,222 lines under pct/ and spec/:**
  - corpus host-judge goldens 11,319;
  - PCT answer goldens 5,891;
  - PCT and corpus outcome goldens 2,733;
  - channel B outcome goldens 1,249;
  - the lean SQL corpus baseline 628;
  - Java 1,053 (−570);
  - BUILD and .bzl files 324;
  - deleted corpus registers −952.

  Command: awk over `audit/units/SAFETYNET.txt`.
- **SQL inside the PCT answer goldens:** 1,490 of 5,891 lines contain `select ` (relation and standard files). Command: `grep -ic 'select ' pct/src/test/resources/edge/*answers.tsv`.
- **The id mask:** 591 masked fragments outside trace ids; 171 VALUE lines have masked values (`Long:<id>`, `BigInteger:<id>`, `Double:0.<id>`). Command: `grep -h -P '\tVALUE\t.*values=.*<id>' pct/src/test/resources/edge/*answers.tsv | wc -l`.
- **Guard code:** the 23 new guard classes total 10,517 lines, 23% of them comment lines; the GUARDS list adds +17,228/−694 lines. Planted fixture files: 8 (+402). Command: `wc -l` and `grep -cE '^\s*(//|\*|/\*\*)'` per file, and awk over `audit/units/GUARDS.txt`.
- **Pins and register rows at the head:**
  - EdgeOnlyMembersTest READ_API: 95 entries;
  - EdgeSeparationTest HOOKS_RULED: 29;
  - HookRulingTest HOOK_ENTRIES: 10;
  - CompatSpellingGuardTest REGISTER: 15 rows, all terminal (STAYS or RULED);
  - EdgeOnlyDataTest.OPEN: 16 (9 READ_API, 4 DEAD, 3 RULED_PENDING), none counted.

  Command: sed and grep on each file (transcript).
- **UpstreamBranchesTest's vocabulary in core/src/main:**
  - `foldedAggregate`: comments only;
  - `"unionAlias"` as a whole literal: edge only, apart from `lineage/ScanRelations.java:1536`, which lies outside the scanned packages;
  - `floatUlps`: testexecutionplan, edge and VerdictNumbers, the last a hook receiver the guard exempts.

  Command: `grep -rlw <word> core/src/main/java`.
