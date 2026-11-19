# The compatibility program: the re-cut plan (2026-11-19)

**Status: r7, RATIFIED to start (phase 0) on 2026-11-19 ~10:45 UTC; phases 1-3 are confirmed at the phase-0 checkpoint. r6 and r7 fold in the independent review (`session_0195rQEnCA5MRQEk6iGfgDnb`, verdict: sound with changes; its record `docs/records/compat-independent-review-2026-11-19.md` on `claude/compat-review` at `838e280c`).**
- **r1** (`71aa0fc7`) was reviewed by B1's lead (2026-11-19 ~04:08 UTC).
- **r2** recorded the user's first rulings and revised CP-5.
- **r3** folds in B1's review: the strand map corrected; the judges split before 1.3; 2.3 widened by the appeal stack and
  executed DDL; sizes corrected.
- **r4** folds in B1's review of r2: what "feeds" means; prints inside returned structures; Floats in
  database-written JSON; CP-5's cost counted in 0.4 (j); the print passes deleted in 1.3.
- **r5:** E-R2 (the user switched E-R1); the SQL-text appeal and referee KEPT (r3 wrongly deleted them; the user's
  correction); §12, the implications to go through. Owner: the compatibility session
(`session_0195rQEoSpv1FxC3WndEk5ZH`), handed the program by the previous orchestrator
(`docs/records/compat-program-handoff-2026-11-19.md` on `origin/claude/refactor-code-quality-review-9fqhug`). Branch
`claude/compat-program`. Nothing here moves a pin, a ceiling, a ledger or an allowlist. Each guard move it names waits
for the user's word and a dated note.

**Written fresh from the evidence**, not from the three-mode design. Where that design's work carries over, §2 says
so. The sources, read at these heads:

| Source | Branch | Head |
|---|---|---|
| The product | `main` | `b75d6c73a` |
| The handoff; the architecture under test (`compat-homework-2026-11-19.md`); the PR #118 audit and its rulings (`pr118-audit-2026-11-18/FOLLOW_UPS.md` §6-§8) | `claude/refactor-code-quality-review-9fqhug` | `a73e7c2bd` |
| H1, every known difference classed | `claude/compat-homework-h1` | `b3e3902ea` |
| H2, text built in a query and then used | `claude/compat-homework-h2` | `a0d5f1eaf` |
| H3, the three prototypes | `claude/compat-homework-h3` | `086292ef3` |
| H4, the three hosts | `claude/compat-homework-h4` | `a915eec28` |
| The census (B2) | `claude/census-b2` | `bd9ba3476` |
| The legend-engine oracle (slice R) | `claude/presentation-r` | `6e283934c` |
| B1's slices S0, S1m, S1 | `claude/presentation-b1` | `00cd4295d` |
| The three-mode design | `claude/presentation-design` | `810eb3c96` |

The counts of §3 were re-measured at `main` for this plan (commands in §11).

---

## 0. The plan on one page

**The goal.** Lite's core (compiler, planner, plan, exec) has one meaning and no mode. Everything that means
legend-pure or legend-engine lives in two places at the edge:
- **a translator on the way in**, the input adapter, which rewrites an upstream program into the lite program that
  computes upstream's answer, or refuses it by name;
- **formatters on the way out**, the output presenters, which turn a typed result, an error record and the plan into
  upstream's bytes, after the run.

The evidence says this holds (H1: 281 differences, every one fits a class; H3: all three prototypes matched upstream
itself; H4: every host already runs Java, so one presenter per audience serves all three).

**Six phases.** Phases 1, 2 and 3 run in parallel tracks once phase 0 is done.

| Phase | What it does | Biggest deletion | Size (session-days) | Track |
|---|---|---|---|---|
| **0. Rulings and the net** | the user's rulings; B1's safety net lands; the measurements the later slices need; the legend-engine PCT lane; the translator's ledger (every rewrite and refusal, before any is built) | none | 6.5 | all |
| **1. Formatters (output)** | one presenter per audience over typed results and the plan; error records as kind, place and literal operands; the hosts call the Java presenter | the SQL-built text and envelopes, about 2,000 edge lines; 3 facets and 4 two-form hooks | 21 | P (B1) and H (a host session) |
| **2. Deletions that need no replacement** | compatibility judging becomes host-only (the SQL-text appeal kept, now deciding every SQL-text assert by answer); upstream's SQL text and shapes go, checked by answers | the compatibility database judge (51 of the census's 142 hits); the engine-style SQL printers, about 2,950 lines | 14.5 | J (new) and G (B2) |
| **3. Translator (input)** | a model-level, target-aware rewrite at the edge's doors, using only functions every lite user has; no copying of legend-engine's refusals (E-R2) | the lowering's two-form choices (`LoweringForms`, 17 members) and the remaining two-form hooks | 14.5 | A (new) and O (B1-R) |
| **4. Close-out** | `PlanStyle` deleted; the guards and the census blocking | `PlanStyle`, `PureText` and its 12 dialect subclasses | 3.5 | G (B2) |
| **5. Portability** | export lite plans to legend-engine and import its plans, both checked by legend-engine running them | none | 6.5 | O (B1-R) |

**In all: about 64 session-days of work.** That is about 5 to 6 weeks of calendar time on the critical path
(phase 0, then the translator, then the close-out), with five or six sessions in parallel. Inside phase 1, the
judges split (2.1, 2.2) runs first beside 1.1, 1.2 and 1.5, because 1.3 and 1.4 delete text the compatibility
database judge still uses. The old three-mode design
was 36 days. It did not include the translator's model-level rewrite, the hosts, the SQL-shape deletions or the
judges' code; it also kept the modes, which this plan removes.

**What the user rules (§5).** Five decisions need the user's call:
- **CP-1:** translator in and formatters out replace the three modes.
- **CP-2 (R4):** lite's own `substring` and `indexOf` follow Pure.
- **CP-3 (E-R2, switched by the user):** the legend-engine audience answers everything lite can.
- **CP-4:** upstream's SQL text is never promised.
- **CP-5:** no special functions that only the translator calls (revised after the user's question, 2026-11-19).

Six smaller ones (CP-6 to CP-11) take the recommendation unless the user objects. R-a stays with the previous
orchestrator's PR #121.

---

## 1. "Done", measurably

The program is done when all six hold, each checked by a test that runs per push:

1. **No mode in the core.** `PlanStyle`, the facets (`facet(...)`), the two-form hooks (`twoForm(...)`),
   `LoweringForms`, `PureText`, B1's `Mode` records, upstream's `Feature` flags inside the core
   (`Lowerer.withFeatures`), and the test runner's per-decision seams (`testexec` `EdgeRun`, `EdgeHearing`, `HostWords`,
   `HostForms`) do not exist. A door chooses a whole translator, presenter and judge; nothing inside the core asks which. The one
   allowed seam: the test planner and the test runner each call the judge the door handed in through one narrow
   typed interface (planning, run), at exactly three call sites (`TestDoor`; the test planner's per-assert entry,
   where the engine-text arm claims an assert, `AssertVerdicts` and `EngineTextPlanning`; `TestRunner`), which the
   SQL-text appeal needs (the judges session's map, 2026-11-19). The planning interface writes into the test plan's typed extension point, so the plan stays data (the
   2026-11-08 ruling). The two call sites and the two interfaces' method sets are pinned, shrink-only, and the run
   interface never receives a planning object. An ArchUnit rule forbids their return, and B1's
   `ModeBranchCensusTest` reads 0 and blocks.
2. **No upstream meaning in the product.** The census (`MeaningCensusTest`) runs with `BLOCKING = true`. Its (a) and
   (p) counts are 0; every remaining hit is a (b) row citing the user's ruling.
3. **No upstream word in a plan.** No plan the suites make, product or test plan, carries an upstream sentence, an upstream envelope or a
   presentation instruction. This is checked over every plan made by PCT, the corpus, pure/v1 and core's suites (the
   census's M1 lens, run over plans).
4. **One presenter per rule.** Every presentation rule (words, kinds, statuses, envelopes, metadata keys, cell text)
   exists once, in Java. The hosts call it. H4's three guards hold: the host meaning lens at 0, cross-host byte
   parity, and one differential per presenter export.
5. **Upstream answered as well as before, or better, measured against upstream itself:**
   - **PURE audience:** PCT channels A and B against PCT's expected values;
   - **ENGINE audience:** PCT channel A against legend-engine's own manifests (DuckDB's 5 suites and H2's relation
     suite per push; the other 5 manifest databases in lane rounds); the corpus against its rosters; pure/v1 against
     the golden net and the oracle;
   - every move is toward the oracle. Each move away is a listed, ruled register row.
6. **The product unchanged except as ruled.** LITE's suites, the lean-SQL baseline and the product plan's JSON move
   only where a ruling says (R4, R5, CP-7), each move listed.

---

## 2. What carries over, what does not

| Work | Status | Why |
|---|---|---|
| **B1's S0: the net** (`6a1e0f69`: PCT masking by field, ordered rows, activities out of the answer line, corpus messages, channel B on H2 and SQLite, `EdgeCasesGoldenTest`, the planted-change proof; G1, G3, G6 report-only) | **carries over whole.** It lands first (slice 0.2) | It is the safety net every later slice is judged by. G6 (`ModeBranchCensusTest`) becomes done-when 1's guard |
| **B1's S1m: modes as data** (`7e8e4cb8`) | **split.** The parser's level-less entries defaulting to `LEGEND_LITE` (20:10-R7) carries over. The `Mode` and `Modes` records and the plan's `mode` field do not | The architecture has no modes. Which grammar to parse and which audience to serve is the door's knowledge, held by the edge, never a core parameter |
| **B1's S1: words of a raise site** (`70592a5b`, plan v59) | **split.** Its deletions carry over: `SiteWordsRecorder`, `PlanStyle.siteWords`, the recorder plumbing, the hosts' unread `siteWords`, and a raise carrying its whole site (`RaisedErrors.Raised#site`). Its words-in-the-plan extension (`Site#extensions` carrying `edge.SiteWords`) does not | Architecture rule 2: an error site is its kind, its place and its literal operands, never words. Slice 1.2 re-cuts it. **B1's three pending guard items** (`EdgeOnlyDataTest`'s `Site#extensions` row, `EdgeOnlyMembersTest`'s two codec entries, wasm `engine_routes`' two re-pinned hashes) are therefore withdrawn, not ruled (CP-10) |
| **The census (B2)** | **carries over whole.** Report-only now, shrink-only during the program, blocking at the end | It is the only way to see the real count, and done-when 2's guard |
| **The oracle (B1-R, slice R)** | **carries over whole**, and grows: H3's batteries become permanent checks | legend-engine itself answers every ENGINE question this plan asks |
| **The three-mode design's rulings** | **kept where they are about meaning or words:** C4 (compatibility database judge deleted), A3 (no run-time operand value in a message), 20:10-R1, R5, R6, R7, R10, R11(a), (e) E1 (import and export are ENGINE's), (f) (retire `EngineResultEnvelopes`). **Superseded:** "reframe as 3 modes" (19:45) and (b) "kind-B rows re-cut per mode", by CP-1 and H1's classes; (h) K1's "an upstream mode's words in an edge extension", by CP-7 | The new architecture changes where compatibility lives, not what the user ruled about meaning |
| **The old design's slices** | Re-cut here. S0 and S0e survive almost as they were; S4 and S6 survive as presenter slices; S2b survives as phase 2's judges slice; S5a and S5b become the translator's phase; S1m, S1's extension and S7's mode records do not survive | |

---

## 3. The spine: every mode-dependent behaviour today, and where it goes

The program is a set of **strands**. Each strand is one behaviour the core changes today when it serves an upstream
audience, and each gets a destination:
- **P:** a formatter, after the run;
- **R0:** an input rewrite over what the core already has;
- **R1:** a rewrite needing a function the core lacks. Under CP-5 none is built, so each R1 row becomes P, a Pure
  answer with a register row, or X;
- **X:** a refusal by name;
- **J:** host-only judging;
- **S:** deletion, because only upstream's SQL text or shape depended on it.

When a strand's destination is built and its doors are switched, its hook is deleted. **`PlanStyle` dies when the
last strand closes (slice 4.1), not before.**

Counted at `main` `b75d6c73a`:
- `PlanStyle` has 10 methods (`database/PlanStyle.java`, 173 lines) and is named by 43 product files outside
  `com.legend.edge`;
- its facet kinds are 5 (`PlanStyleFacets.KINDS`);
- its two-form hooks are 16 (`PlanStyleFacets.TWO_FORMS`; B1's S1 adds `SiteFacts`, which slice 1.2 does not keep);
- `LoweringForms` has 15 members (`lowering/LoweringForms.java:45-107`);
- the edge is 93 files and 21,988 lines.

**The ratchet's start, measured by B2 (0.4 (a), from the product's bytecode at `b75d6c73a`):**
- `PlanStyle`: 10 methods; 49 parameters in 19 classes (`PlanMaker` 14, `ProductStatements` 5, `TypedQuery` 4,
  `EngineTextGates` 3, `GeneratedTestData` 3, ...); 4 fields;
- 50 calls of its methods: `facet` 16, `twoForm` 16, `dialect` 8, `judgesEngineText` 4, `siteWords` 2, `errorWords` 1,
  `traceCell` 1, and `graphKeyDate` and `graphKeyString` 1 each, by method reference;
- 112 calls carrying a style, in 22 classes; 36 classes naming it;
- `LoweringForms`: 15 members, 25 calls, 6 parameters;
- the census's (a) at `main`: 142, plus 2 (p).
B2 confirmed every row of the map below against the code, body to body (`docs/records/compat-homework/H8-phase0-measures.md`
on `claude/census-b2` at `ece476172`).

**Two families inside `LoweringForms`, with different consequences** (B1's review, 2026-11-19):
- the edge's own forms (`EdgeLoweringForms`) run in every edge plan's EXECUTED SQL;
- `EngineShape` (`EdgeLoweringForms.java:186`) is used only for legend-engine's TEXT, through `engineSql` (`:156`),
  which only `EnginePlans.java:174` calls (`toSQLString`, generatePlan, activities).

A member whose executed edge form already equals the product's differs only in that text. It is therefore S, and is
deleted with the text in 2.3.

**This map was corrected by B1's review of r1** (against `00cd4295d`). Slice 0.4 confirms each row against the code
and H1's TSV, and the confirmed map is the ratchet. Paths are under `core/src/main/java/com/legend/`.

| Strand (today's hook) | What it does today | Class (H1 rows) | Slice |
|---|---|---|---|
| facet `RelationTexts` | a relation's `toString`, `toCSV`, a TDS's `csv`, built in SQL (`RelationTextLowering`, 791 lines) | P at a root or side; X if it feeds or is composed | 1.3 |
| facet `PureOnlyTexts` | `format`'s `%0Nd` and `%r`, `toRepresentation`, an instance's `@_` id (`EdgePureOnlyTexts`) | P; R0 for `%r` in a query (D04); X if it feeds or is composed | 1.3 |
| facet `ResultEnvelopes` | legend-engine's `tdsBuilder`/`classBuilder` envelopes by the database's JSON (`EngineResultEnvelopes`) | P (H3 (c): byte-identical) | 1.4 |
| facet `UpstreamPlans` | `toSQLString`, `planToString`, the engine-text asserts | S under CP-4: the SQL text goes; legend-engine's plan FORMAT stays as an ENGINE presenter | 2.3, 1.4 |
| facet `EngineTestText` | the engine-text assert's planned comparison, golden against ours (`EdgeEngineTestText`) | KEPT as part of the appeal: it moves into the edge judge's planning interface | 2.2 |
| two-form `DateTimeTexts` | a DateTime's text in a message or a print | P; X if it feeds or is composed | 1.3 |
| two-form `ListTexts` | a list's `[a, b]` vs the product's JSON array | P; R0 in a query (D06) | 1.3, 3.4 |
| two-form `ExecutedValues` | legend-engine's json-builder `values` envelope | P | 1.4 |
| two-form `JsonGrammar` | json-simple 1.1.1's grammar in the judge (S144) | J | 2.2 |
| two-form `RelationCounting` | a relation counted as one value by asserts (S137) | J | 2.2 |
| two-form `CrossKindNumbers` | compiled legend-pure's Integer/Decimal-by-text (S127) | J half 2.2; R0 half 3.4 | 2.2, 3.4 |
| two-form `VerdictNumbers` | legend-pure's Float/Decimal comparison in the database judge (S15, S132, S139) | J | 2.2 |
| two-form `RenderedTextRule`, `DateLiteralTexts`, `GoldenNullWord`, `VerdictWords` | the compatibility database judge's rules and words | J (deleted with that judge, C4) | 2.2 |
| two-form `RoutedQueries`, `ActivityTexts` | legend-engine's routed-query print and activity SQL in an answer | S under CP-4 (activities carry lite's SQL) | 2.3 |
| two-form `TestDataPlanner.Printing` | the test-data generator's legend-engine CSV printing (S191, S306) | P, a presenter after the run; 20:10-R2's "for now" revisited there | 3.6 |
| two-form `DatabaseSettingsForm` | `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` on the edge's H2 (`edge/PureText.java`, the H2 constructor) | lite's sorts already stamp Pure's null placement, so "R0 by sort keys" adds nothing. The settings matter only where lite writes no ORDER BY (raw `executeInDb` statements written for H2) and for LEGACY's semantics. Under CP-8, deleted by default; 0.4 measures the corpus's H2 lane under H2's regular mode. If the move is material, the translator declares the target's settings as data (`ExecutionPlan.Target.settings`, which exec already verifies), and the count goes to the user | 0.4, 3.2 |
| `LoweringForms.raise` | the plain set: no lite check where legend-engine plants none (B7; ENGINE raise class A) | R0 per target where an ordinary-function guard reproduces it (`if($x < 0, \|[], \|sqrt($x))`); else a Pure answer with a register row | 3.6 |
| `LoweringForms.executeValues` | the JsonBuilder cardinality fold (`oneBare`) | P | 1.4 |
| `LoweringForms.objectReferenceConnection` | a protocol constant inside an object reference (C1 row) | P, with the fact "this cell is an object reference" (S362) | 1.4 |
| `LoweringForms.isNumeric`, `parsedDecimal`, `parsedJson` | legend-engine's H2/DuckDB forms (S370, S365, S366) | R0 | 3.4 |
| `LoweringForms.relations`, `keyedCounts`, `computedPruning` | in EXECUTED edge SQL already equal to the product's (`EdgeLoweringForms.java:104/111/118` against `ProductLoweringForms.java:58/64/70`); only `EngineShape` switches them off (`:189/:195/:203`), for legend-engine's text | S: deleted with the engine text. S147 and S360 already record the edge's executed answers as lite's; no answer moves and nothing is refused | 2.3 |
| `LoweringForms.view`, `wireCast`, `unionFrame` | executed edge forms equal the product's (`EdgeLoweringForms.java:44/49` against `ProductLoweringForms.java:31/36`; `unionFrame` null in both); only `EngineShape` differs (`:223`, `:234`, `UNION_FRAME`) | S, pure text, no answer risk | 2.3 |
| `LoweringForms.existsJoin`, `whereZones` | run in EXECUTED SQL on every edge style (`EdgeLoweringForms.java:143`, `:176`): legend-engine's exists-join rule (C2, LOWERSQL-4), and its WHERE conjunct order feeding it (user conditions, then correlation, then temporal; `EngineWhereZones`) | S: answer-equal by S367's own measurement; but they change the executed statement and with it H2's row order for unordered results, so they go after 2.2's multiset compare | 2.3 (after 2.2) |
| `LoweringForms.assertFailed` | "Assert failed" vs lite's "expected true, got false" (20:10-R1, R10) | P | 1.2 |
| upstream's `Feature` flags in the core: `Execution.run`, `TypedQuery`, `Lowerer.withFeatures` (`lowering/Lowerer.java:216-235`); `LEGACY_SQL_NULL_UNSAFE_EQUALS` switches the core's equality; `CORRECT_SQL_SUBSTRING_INDEXING` switches `substring` (the independent review) | a mode by another name | the translator's input, or not supported, under CP-12 | 3.2 |
| `PlanStyle.siteWords(text, sites)` | records legend-pure's raise-site words per planning call (`SiteWordsRecorder`); called twice in the product (B2's 0.4 (a)) | P: deleted with the recorder | 1.2 |
| `PlanStyle.errorWords` | a planning failure's upstream sentence (also the test plan's `Failure.message`, `testplan/AssertErrorNative.java:81`) | P (a kind, operands and lite's wrapper as data; worded after) | 1.2 |
| the test plan's words: `TestPlan.words`, `answerSizeWords` (`testexecutionplan/TestPlan.java:45-48`), `SideShape.sizeWords` (`SideShape.java:37`) | upstream words planned into the test plan | P | 1.2 |
| `PlanStyle.traceCell` | an ANSWER cell: the activity rows' comment, `-- "executionTraceID" : "<id>"`, wrapped around the execution id in a TDS cell (`planner/PlanEnv.java:173-177`, `Lowerer.withTraceText`) | P: the ENGINE presenter writes the comment around the activity's (lite's) SQL | 1.4 |
| `PlanStyle.graphKeyDate`, `graphKeyString` | ANSWERS: the JSON key of a qualified property in graph-fetch output (`resolver/GraphEmission.java:1846`, `:2938`; `planner/BodyPlanner.java:72`), written by the database at planning | P: the presenter renames the keys from the typed graph tree, after the run (real work, budgeted in 1.4) | 1.4 |
| `PlanStyle.judgesEngineText` (`EngineTextGates`) | "is this the edge" in the product test planner (JUDGES-6, 13 hits) | J: deleted | 2.2 |
| the test runner's hooks: `testexec` `EdgeRun`, `EdgeHearing`, `HostWords`, `HostForms`; `testexecutionplan` `Edge`, `NoEdge`; `ExecuteOptions.recording` | per-decision seams by which the edge changes the product's judges and runner | J: the door hands the runner a whole judge (the product's, or the edge's compatibility judge, appeal included), never per-decision two-form seams inside the product's | 2.2 |
| the engine-text appeal, referee and ledger stack (`edge/`: `SqlTextVerdicts` 1,675, `Appeals` 401, `AppealJson` 273, `SqlTextRuns` 291, `SqlTextReferee` 231, `Appeal` 191, `SqlReplayReferee` 173, `PlannedAppeals` 56, `AppealRun` 40, `RefereeLoad` 34, `EffectTexts` 28, `EngineTextJudgment` 18; 3,411 lines (B2's count); `CompatEdge`'s ledger half; `EffectPlanner`'s referee CSV copies) | turns an engine-text assert failure into a pass by replaying the golden statement against a referee database | **KEPT** (G6, E3; the 2026-09-01 charter; the user, 2026-11-19: "one of the crown jewels"). Under CP-4 it decides every SQL-text assert by answer. Its seams into the product's runner move into the edge's whole judge (2.2) | 2.2 |
| `PlanStyle.dialect(type)` = `PureText`'s 12 subclasses | the edge's dialect wrapper. Its passes: | per pass, below; then `dialect` is `Databases.dialect` | 4.1 |
| … `spellingPasses` (`PurePrintForms.passes`, all 12), `floatToText` | legend-pure's Float and DateTime prints inside EXECUTED SQL | P at roots and sides; X where composed or feeding (CP-5) | 1.3 |
| … `floatLiteralKind` (H2 `:406`, DuckDB `:350`, Postgres, Trino, Databricks) | DECIMAL literals, which change arithmetic (S20) | a Pure answer with a register row (CP-5), deleted | 3.6 |
| … `raw` (DuckDB `edge/PureText.java:359`, Postgres `:447`) | translates legend-engine's H2 `executeInDb` text for the target: INPUT | generated text: target-aware lite DDL in 2.3; user-written H2 text on another target: the translator, or X by 0.4's count | 2.3, 3.2 |
| … `refusesDecimalArithmetic` (SQLite `:567`) | the edge answers with REAL arithmetic where the product refuses (ruling (c), D16, C27) | a translator rewrite on SQLite (CP-11), or X | 3.4 |
| … `dateTimeFormat` (MongoDB) | a DateTime print | P | 1.3 |
| … `scanOrderPasses`; `edge.StableScanOrder` (598 lines) | legend-engine's H2 scan order for unordered corpus asserts (S14, D14) | J: the compatibility host judge compares an unordered actual as a multiset (S131's rule) | 2.2 |
| `RelationalRootForm.apply` (`resolver/RelationalRootForm.java:51`, called at `EnginePlans.java:164`) | legend-engine's root form, for its text only | S, `apply` alone. Its `primaryKeyColumns` is the product resolver's key rule on executed paths (`resolver/ObjectReferenceDecode.java:133`, `ClassSources.java:797`, `CorrelatedSubselects.java:321/486/1094`, `GraphEmission.java:533`) and stays the product's | 2.3 |
| The engine-text printers `EngineStyleH2` (2,507), `EngineStyleDB2` (365), `EngineStyleComposite` (79) | legend-engine's SQL text formatting, plus statements a program EXECUTES: `toDDL`'s create and drop texts (`edge/EnginePlans.java:958-995`) and `setUpDataSQLs` (`:754`, `:767`), written for H2 whatever the target, run elsewhere through `raw` | S for the text; the executed generated statements become target-aware lite DDL in the same slice | 2.3 |
| The census's word groups (C05, C08, C10, C11, C14-C17, C19; 24 hits) | an upstream sentence inside the product | P | 1.2 |
| The Cypher translator's `substring` (`CypherToPure.java`, H3 item 5); the `###Relational` dynaFunction `substring` sharing the Pure function's rule (H3 item 6) | the product's own two dependencies on today's meaning | split from the Pure function first | 3.1 |

## 4. The phases and their slices

**Every slice:**
- the owning session sends a shape note to this session before building;
- the local loop is the touched packages' tests, `//core:guardrails`, `//core:guardrails_edge`, `//core:census`,
  `//pct:edge_net`, the census and the mode-branch counts;
- it lands through a PR with CI on every OS, after an independent xhigh review, and merges only on the user's word
  in this session;
- a slice that moves a pin, a ceiling, a ledger or an allowlist names each move in its shape note and waits for the
  user's word;
- "the lanes move only toward their oracle" means: a PURE answer may move only toward PCT's expected value; an ENGINE
  answer only toward legend-engine's manifest, roster or oracle answer; a LITE answer only where a ruling says. Every
  move is listed in the PR.

Sizes are session-days at xhigh, the unit the three-mode design used, plus lane latency.

### Phase 0: rulings, the net, the measurements, the translator's ledger (about 6.5 days)

**0.1 The rulings.** The user rules CP-1 to CP-11 (§5). The slices that wait on each are named there. Phase 1 and
slice 2.2 need only CP-1 and CP-7. **Size:** 0. **Owner:** the user, with this session.

**0.2 The net lands** (B1's S0 alone: a fresh branch from `main` with `6a1e0f695` and the requests commit `b7ec3649c`
cherry-picked; S1m is not carried, and S1's deletions are re-done in 1.2).
- **Changes:** what S0 changed: test code, tools and goldens only. Also: `//pct:edge_net` and `EdgeCasesGoldenTest`
  join the per-push lanes (CP-9; FOLLOW_UPS §8's C1).
- **Deletes:** nothing.
- **Check:** S0's planted-change proof (10 cases). The re-recorded goldens map back to the old ones line for line
  (0 answer moves).
- **Guards:** G1, G3, G6 report-only.
- **Size:** 1 day (cherry-pick, re-record, PR).
- **Depends on:** nothing. **Owner:** B1.

**0.3 The ENGINE PCT lane** (the old S0e, report-only).
- **Changes:** PCT channel A runs as the ENGINE audience on DuckDB's 5 suites and H2's relation suite. Each answer is
  compared with legend-engine's own manifests (`@legend_engine_src`'s `pct-manifests`, spec input only). The lane
  writes, per test: legend-engine passes, refuses ("No SQL translation exists ..."), or fails by defect, and what lite
  answers.
- **The E-R1 refusal table** is derived from the "No SQL translation" entries, never by hand, and keyed by function
  signature AND database: the manifests are per database and differ.
- **The lane compares status and refusal class,** not words, which are the presenter's (B1-R's note, 2026-11-19).
- **Deletes:** nothing.
- **Check:** the lane runs, and its differences from the manifests are the baseline for phase 3. A manifest records
  an expectation, not a run, so a sample of about 10 rows per database is confirmed against legend-engine itself
  through the oracle.
- **Guards:** the derived table pinned, regenerated by a diff test.
- **Size:** 1.5 days for H2 and DuckDB, plus half a day for each further manifest database.
- **Depends on:** 0.2. **Owner:** B1-R.

**0.4 The measurements.** Each is a number one or more later slices or rulings need:
- (a) **The strand map confirmed** (§3): every hook, facet, two-form and `LoweringForms` member, its class and its
  slice; `PlanStyle`'s parameters and call sites counted. This becomes the program's ratchet: a shrink-only count
  that ends at 0.
- (b) **CP-4's cost:** how many corpus tests pass today only through legend-engine's SQL text (`toSQLString`,
  `assertSqlEquals`, `planToString` and the test-data generator's `sqls`). Measured by running the corpus with the
  engine-text printers made to refuse.
- (c) **legend-engine's unchecked operations:** which of lite's checked natives legend-engine leaves unchecked (its
  raise class A), so 3.6 can count the register rows CP-5 creates.
- (d) **The strands' doors:** which door reaches each strand (pure/v1, the corpus, PCT A, PCT B, the hosts' edge
  routes, the test-data generator).
- (e) **CP-4's cost, split three ways** (B1's review): tests that pass by ASSERTED legend-engine text the appeal
  cannot decide (B2 measured 143); tests the appeal decides (they stay passing, by answer); programs that EXECUTE
  generated text (`toDDL`, `setUpDataSQLs` passed to `executeInDb`). Also the corpus lanes' run time with every
  SQL-text assert going to the appeal, against today's.
- (f) **The prints, by position and by use:**
  - of H2's 98 BARE query-side sites, how many sit at the literal root (`x->toString()` returned) and how many inside
    a returned structure (`map(x|$x->toString())`, `extend(~s: ...)`, a Pair, `^C(s = ...)`);
  - of the 8 COMPOSED and 14 FEEDS sites, which print a Float, a Decimal or a DateTime and have their CONTENT read
    (1.3's rule). Those are refused under CP-5, so the count is listed before 1.3 lands.
- (i) **Floats inside database-written JSON:** whether each dialect's JSON writer (serialize, graph fetch) keeps a
  Float's round-trip digits. If one rounds, no formatter can recover them, and the fix is a product correctness fix
  (lite writes JSON floats round-trip), not a special function.
- (k) **E-R2 against the corpus:** corpus tests that EXPECT legend-engine's refusal (an asserted error) and that lite
  would now answer; counted, then classed (the independent review).
- (j) **CP-5's honest cost** (B1's review): PCT channel A as ENGINE on H2 and DuckDB, the relational corpus (the suite
  S20's literal affects most; the independent review), plus pure/v1, run with S20's
  bare-DECIMAL literal and `LoweringForms.raise`'s plain set switched off. The 3.4 guard rewrites
  (`if($x < 0, |[], |sqrt($x))`) are counted where they apply. The result is "N answers that match legend-engine
  today would stop matching", by case. It goes to the user before 3.6 deletes either. Today pure/v1's
  `raise.sqrtOfNegative` answers 200 with a null, as legend-engine does, through that plain set.
- (g) **G7 over the net:** the computed-operand raises PCT and the corpus reach. Deleting `PureMessages.sentence` in
  1.2 changes each one under A3. `InterimEdgeSitesTest` pins 4 in its own set: rem by a column, rem by 0, sqrt of a
  cast literal, at by a column.
- (h) **H2 without `MODE=LEGACY`:** the corpus's H2 lane run under H2's regular mode and default null ordering (CP-8).
- **Deletes:** nothing. **Check:** each number reproducible from a committed probe.
- **Size:** 2 days.
- **Depends on:** 0.2. **Owner:** B2, with this session.

**0.6 The translator's ledger: exactly what is rewritten and what is refused** (the user's question, 2026-11-19).
- **Changes:** one checked table, `edge/translator-ledger.tsv`. Each row: the function or construct, the audience,
  the database, its action (pass, rewrite, refuse by name, Pure's answer recorded, lite answers where upstream refuses), the
  rewrite's text or the refusal's words, its evidence (a pinned source line, a manifest entry, an oracle answer), and
  the test that proves it. It is built from:
  - H1's rows: today 35 rewrites, 48 refusals and the 24 rows that needed a special function, each decided under
    CP-5;
  - 0.3's refusal table, derived from legend-engine's manifests per database, which H1 does not hold beyond its 17
    known rows;
  - 0.4's counts, so each row carries how many PCT, corpus and pure/v1 cases it touches.
- **Deletes:** nothing.
- **Check:** every row has evidence and a test; each rewrite's battery agrees with legend-engine (the oracle); the
  table is put to the user before the translator's first rewrite lands.
- **Guards:** the ledger is the translator's only source. A rewrite or refusal the translator performs that is not a
  row fails a test, and a row without its test fails another. A new row is posted to the user before it lands, as a
  register row is.
- **Size:** 1.5 days.
- **Depends on:** 0.3, 0.4, CP-5. **Owner:** the translator session, with B1-R.

**0.5 Register housekeeping.**
- **Changes:** S138 corrected (compiled legend-pure raises on `acos(2.0)`, `CoreHelper.java:141-148`), S63 recited
  (the live refusal is `pureToSQLQuery.pure:10056`), the prelude's register row (the user's 2026-11-19 ruling: the
  prelude and the parser stay), and the unrecorded differences found so far: legend-engine's H2 `parseJson` UDF and
  S110's DuckDB residual (H1); legend-engine's `joinStrings` SQL appending the separator (B2's CP-4 count).
- **Check:** `RegisterRowsTest` and the census's (b) rows citing them.
- **Size:** 0.5 day.
- **Depends on:** CP-10. **Owner:** this session.

### Phase 1: formatters on the way out (about 21 days; tracks P and H)

The proven half (H3 (c): legend-engine's TDS envelope byte-identical from a presenter after the run, 0 product
lines). It comes first because it is the lowest risk and deletes the most text-building code. It also defines the
error record the translator's refusals are worded through.

**The order inside it** (B1's review):
- 1.1, 1.2 and 1.5 run in parallel with the judges split (2.1, 2.2);
- 1.3 and 1.4 wait for 2.2, because the compatibility DATABASE judge writes its failure messages through the very
  text facets 1.3 deletes (`lowering/VerdictSql.java:2651-2657` calls `styled.pureOnly()...representationOf`;
  `testplan/DatabaseJudge.java:301` and `:952` read `env.plan().styled()`).

**1.1 The presenter seam.**
- **Changes:**
  - one `Presenter` per audience: LITE's in the product library (today's writers: `result.HostRows`,
    `LiteV1Planning.Answered`, `ArrowWriter`); ENGINE's and PURE's in `com.legend.edge` (`PureV1Writer`,
    `EngineAnswers`, `HostAnswers`, `RaisedWords`);
  - each presenter reads three inputs and nothing else: the typed result stream, the plan, and the typed query's
    facts (H3 (c)'s finding: `activities[].sql` comes from the plan, `relationalType` from the result type);
  - a presentation instruction (for example "print this root as `#TDS` text") is held by the edge's door and handed
    to the presenter; it is never written into the plan;
  - the typed facts CP-6 rules in are added to the plan's column record, written only when they differ from their
    default, so a LITE plan's JSON stays as small as it can. Any move of `lean-sql-baseline.tsv`'s plan column is
    listed for the user's word.
- **Deletes:** nothing yet.
- **Check:** pure/v1's net byte-identical; the oracle's answers unchanged.
- **Guards:** a new ArchUnit rule: a presenter class depends only on `result`, `executionplan`, `error`, `values`
  and `json`; it never reaches `exec`, `planner`, `lowering` or `java.sql`.
- **Size:** 2 days.
- **Depends on:** CP-1, CP-6. **Owner:** B1.

**1.2 Error records: kind, place and literal operands; words after the run** (re-cuts B1's S1, plan v59).
- **Changes:**
  - a raise site carries its kind, its place and its literal operands as typed data (A3: never a run-time value).
    Shape (B1's review): `Site` gains `List<Operand>`, a sealed type in `executionplan` with a typed literal per value
    kind (Integer, Float, Decimal, String, date text, Boolean) or `Computed`, so A3 keeps the positions without the
    values. `PlanMaker.sites` builds it from `sql.dialect.RaiseSite.operands()`; `Raised#site` already reaches both
    presenters whole;
  - a planning failure carries a kind, its operands and lite's wrapper as data (prefix, kinded part, suffix: today
    `PureMessages.compile` splices upstream words into lite's "in function 'f': ... [inlined via ...]" by position,
    `edge/PureMessages.java:358-366`), replacing `PlanStyle.errorWords`;
  - the test plan's words go the same way: `TestPlan.words`, `answerSizeWords`, `SideShape.sizeWords` and a planned
    `Failure.message` (test plan v34);
  - each audience's presenter words them: LITE's sentence by lite's presenter, legend-pure's by the PURE presenter
    (exact over a literal operand, without the value over a computed one, under one register row), legend-engine's by
    the ENGINE presenter;
  - S1's deletions land: the recorder and `PlanStyle.siteWords`;
  - the six in-body guards' LITE words (R10);
  - "Assert failed" leaves the product (`LoweringForms.assertFailed`, 20:10-R1);
  - lite's own sentence stays in the plan's `sites[].message` until slice 1.6 makes the hosts word through Java
    (CP-7); upstream words never enter the plan.
- **Deletes:** `PureMessages.sentence` (the SQL half, 57 lines; 93 SQL code lines in the file),
  `SiteWordsRecorder` (124), `PlanStyle.siteWords` and `errorWords`, `LoweringForms.assertFailed`; S1's
  `SiteFacts`, `SiteWords`, `Site#extensions` and the explicit-codec overloads never land (the operands are product
  data that `PlanJson` writes natively, with no ServiceLoader and no WebAssembly workaround); the census's 9 word
  groups (24 hits) leave (a).
- **Check:** the net's messages unchanged (PCT channel A's `ERROR` lines, channel B's ledger substrings, pure/v1's 5
  error bodies, the corpus's messages), except the computed-operand raises 0.4 (g) counts, each listed (A3: legend-pure's
  sentence without the value); the product plan's JSON byte-identical but its version; literal-operand raises checked
  on the tab and Python.
- **Guards:** a plan-scan rule over product AND test plans: no upstream sentence in any plan the suites make
  (done-when 3; report-only, and it cannot reach 0 before 2.2 deletes the compatibility database judge's
  `VerdictWords`); G7's computed-operand count pinned (ENGINE at 0); `EdgeOnlyDataTest` gains one row,
  `Site#operands: by the edge` (CP-7), for the user's word.
- **Size:** 4.5 days.
- **Depends on:** 1.1, CP-7. **Owner:** B1.

**1.3 Value and relation text, at roots and sides, in Java.**
- **Changes:**
  - legend-pure's prints (a relation's `#TDS` text, `toCSV`, a TDS's `csv`, a list's `[a, b]`, a DateTime's print,
    `format`'s `%0Nd` and `%r`, `toRepresentation`, an instance's id) are written by the PURE and ENGINE presenters
    over typed rows;
  - the edge's door rewrites a root `toString` (or `toCSV`) into "answer the value; present it as text";
  - a judge side's print is the compatibility host judge's (rule 9);
  - **the presented positions:** a print at the literal root, and a print inside a returned structure (a mapped
    list's elements, an extended column, a Pair's side, a property of a new instance). The translator rewrites the
    print to the value plus an instruction naming its position. The presenter prints there and changes that
    position's declared type in the envelope (a TDS column's `Float` becomes `String`, and its `relationalType` with
    it);
  - **"feeds" means the text's CONTENT is read:**
    - operations that read it: compare, concatenate, `joinStrings` or `makeString` over prints, `length`, sort, group,
      `distinct`, join keys, filter;
    - operations that only carry it do not feed: select, rename, limit, slice, drop, a `let` passed to the root, a
      union of like prints;
  - a print whose content is read, or which is COMPOSED into a returned text (no presenter can re-spell an inner
    value from the outer string), is refused by name where its type's text differs (CP-5); 0.4 (f) lists them first; H2 counted it: 0 upstream sites over Float, Decimal or dates; 14 over Integer or an enum, which R0
    covers;
  - a Pair is its JSON object in LITE and `<a, b>` in PURE and ENGINE (R5). The prelude's print bodies stay (the
    2026-11-19 ruling); 1.3 checks whether LITE ever reaches one.
- **Deletes:** `RelationTextLowering` (791), `EdgePureOnlyTexts` (136), `PureListTexts` (49), `PureDateTimePrint`
  (43), `EdgeDateTimeTexts` (28), the print half of `PurePrintForms` (about 150 of 184), `PureText.floatToText`, the
  print spelling passes and MongoDB's `dateTimeFormat`; facets `RelationTexts`, `PureOnlyTexts`; two-forms
  `DateTimeTexts`, `ListTexts`. Their last other users are the compatibility database judge's messages, which 2.2
  deletes first. Q5 (`toCSV`'s DateTime) and Q6 (`%r` and a newline) are
  fixed in Java as part of it.
- **Check:** PCT channels A and B in PURE move only toward PCT's expected values; `EdgeCasesGoldenTest` (EDGE-1's
  Float values, EDGE-2, EDGE-3) re-pinned to upstream's answers; the corpus rosters unchanged; every refused feeding
  case listed (expected: none in PCT or the corpus beyond H2's 14 Integer and enum sites).
- **Guards:** G1 (text-producing MIR built by the edge) shrinks; the census's M3 rows for these hooks leave (a).
- **Size:** 4 days if most BARE prints sit at the literal root; up to 6 if many are nested (0.4 (f) decides).
- **Depends on:** 1.1, 2.2, 0.4 (f). **Owner:** B1.

**1.4 Envelopes.**
- **Changes:**
  - legend-engine's `tdsBuilder`, `classBuilder` and json-builder `values` envelopes, the cardinality fold, the
    object-reference constant and the graph keys are written by the ENGINE presenter over typed rows;
  - legend-engine's plan FORMAT (generatePlan's `SingleExecutionPlan`) is phase 5's export by another name. It moves
    to 5.1, and until then generatePlan keeps today's answer (the independent review);
  - the activity rows' trace comment (`-- "executionTraceID" : "<id>"`, today `PlanStyle.traceCell`) and the
    graph-fetch output's qualified-property keys (today `graphKeyDate`/`graphKeyString`, written by the database at
    planning) are written by the presenter after the run; the keys are renamed from the typed graph tree;
  - envelope fields that are a function of upstream's SQL (the scalar envelope's `COUNT(*)` label, O3; a database's
    error text over upstream's statement, H3 (c) item 3) carry lite's own value in upstream's envelope, under one
    register row (CP-4).
- **Deletes:** `EngineResultEnvelopes` (92), `EngineExecutedValues` (44), `EdgeLoweringForms.oneBare`; facet
  `ResultEnvelopes`; two-form `ExecutedValues`; `LoweringForms.executeValues` and `objectReferenceConnection`;
  `PlanStyle.traceCell`, `graphKeyDate` and `graphKeyString`. Q7 (EDGE-8, rows aggregated with no ORDER BY)
  goes with them.
- **Check:** the corpus rosters; pure/v1's net; the oracle's answers; EDGE-8's ordered probe; the scalar `value`
  request against legend-engine's own answer (the oracle). `pure_v1_plan_text.tsv` and the oracle's pinned answers
  move where generatePlan now carries lite's SQL: listed golden moves.
- **Guards:** the census's envelope M1 hits leave (a).
- **Size:** 3 days.
- **Depends on:** 1.1, 2.2, CP-4. **Owner:** B1.

**1.5 The hosts' pilot and tables** (H4 steps 1 and 2; track H).
- **Changes:**
  - **the pilot, HOSTS-9 (R11(a)):** `SavedQueries` and `SavedCubes` answer lite's `{kind, message}` and status
    inside the Java call each host already makes (H4's C1);
  - **lite's error kinds, their statuses and `PLAN_VERSION`** are generated from Java for TypeScript and Python
    (`TypeFacts`' pattern).
- **Deletes:** the four store adapters (`LegendHttpServer.java:244-314`, `engine.py:416-446`,
  `wasm-server.ts:66-89`, `cube-store.ts:197-219`; about 150 lines out, 40 in), `LITE_ERROR_KINDS`' hand list,
  `compiler.py:164`'s statuses, the test's `STATUS` table, `PlanVersionsAgreeTest`'s regex.
- **Check:** the cross-host byte-parity pilot: the store cases of `lite-api-cases.json` asked of all three hosts'
  real routes, the status, content type and body compared byte for byte. It must go red on today's Python bytes
  first.
- **Guards:** H4 §8 (2), the byte-parity suite, in a per-push lane.
- **Size:** 2.5 days.
- **Depends on:** 1.1 for the seam's names only. **Owner:** a host session (new, or B1 once 1.4 is done).

**1.6 The hosts word through Java** (H4 steps 3, 4 and 6).
- **Changes:**
  - Python's lite `query/run` answer: JSON through a native entry over `HostRows`; Arrow by Java's metadata,
    attached by pyarrow (H4's C2);
  - ENGINE's remaining host pieces: the error half, Python's zstd frame, `FormatNotSet` and the ARROW_IPC parse move
    into Java; `currentUser` ruled (P11);
  - lite's raise sentences on the tab and in Python come from a table generated from Java (`TypeFacts`' pattern:
    one rule, Java-sourced, no WebAssembly or native call on the failure path), and Java's exec words `DataError`
    from `ErrorKind` directly, so lite's sentence leaves the plan (CP-7, plan v60): `PlanJson.readSites` drops its
    message check, the plan-level `raises` map goes, and `declared-raises.ts`, `raised.ts`, `_raised.py` and
    `raise-envelopes.json` change with it;
  - exec's own readers stay per language (H4's B, honestly used): P3's three stale Python readers fixed, HOSTS-11's
    two inputs and HOSTS-10's three divergences in the shared cases.
- **Deletes:** `_carried_float`, `_lite_cell`, `_lite_rows_json` (about 60 lines); Python's ENGINE framing; the plan's
  `sites[].message` and `raises` sentence fields; C3's and HOSTS-4's Python halves.
- **Check:** byte parity across the three hosts, Arrow included; the differential per new export (JVM vs WASM vs
  native).
- **Guards:** H4 §8 (1), the host meaning lens (census-b2's `HostSources`), shrink-only to 0, with its planted
  TypeScript and Python examples; (3), the differentials.
- **Size:** 3 days.
- **Depends on:** 1.2, 1.5. **Owner:** the host session.

**1.7 DataCube's legend-engine workarounds to its ENGINE plane** (DC1, 20:10-R6).
- **Changes:** the four withheld boolean operators, the `__all__` partition and the `__root__` group column move into
  `datacube/src/edge/`; lite's plane sends the product's forms.
- **Deletes:** C6 from the product.
- **Check:** DataCube's tests on both planes; the ENGINE plane's queries identical to today's; lite's plane's answers
  unchanged.
- **Size:** 2 days, in parallel.
- **Depends on:** nothing. **Owner:** the host session.

### Phase 2: deletions that need no replacement (about 14.5 days; tracks J and G)

**2.1 The 36 combinations ported into core** (C4, part 1).
- **Changes:**
  - the 36 judge shapes the compatibility database judge covers are ported as core LITE tests under both judges
    (BOTH), so parity (G2) is proven in the product without PCT's database judge;
  - **a LITE-PCT lane:** PCT runs under LITE's rules with BOTH judges, with its own known-failure ledger (where LITE's
    exact rules differ from the S15/S131/S132 leniencies). G2's guard therefore stays on PCT (the independent review;
    §12 I5). Measured (H6): about 75 s for channel B, plus channel A under LITE (129 s) as the relation tests' value
    check. **Built by the product's judges track as its C3** (`session_0195rQEn49s2BEcy7BVaoAL4`, from H6's prototype
    at `df5a1a37`), agreed with the product orchestrator 2026-11-19 ~11:21 UTC. So 2.1 ports the 30 remaining
    combinations.
- **Deletes:** nothing yet.
- **Check:** core's isolated BOTH green on the ported shapes.
- **Size:** 2.5 days. Measured by the judges session: none of the 36 is covered by core at `main`; 6 are judged by
  the LITE-PCT lane in passing tests, so 30 remain to port; all are small test bodies needing no new mechanism.
- **Depends on:** CP-1, the phase-0 LITE-PCT measurement. **Owner:** the judges session. **Coordination:** the previous orchestrator's A5 (Q3, Q3b,
  J1-J4) touches the same judges. The two sessions agree by message before either builds.

**2.2 Compatibility judging becomes host-only** (C4, part 2; rule 9). **Split in two** (the independent review): 2.2a
deletes the compatibility database judge's half and switches PCT to host-only, which unblocks 1.3 and 1.4; 2.2b
replaces the appeal's run seams. They run on two sessions, and 2.2a goes first. **2.1's LITE-PCT lane is a
precondition of 2.2a**, unless the user amends G2 ("the agreement judge on every lane").
- **Changes:**
  - legend-pure's comparison rules and words for PURE and ENGINE live only in the edge's host judge: compiled
    legend-pure's `eq` (S15), the multiset compare of an unordered actual (S131, which retires `StableScanOrder`'s
    reason, S14 and D14), the 2-ULP Float rule (S132), json-simple's grammar (S144, S366), a relation counted as one
    value (S137), the failure words (S195);
  - PCT runs HOST-only for compatibility judging; LITE keeps both judges with parity, and its exceptions go
    (JUDGES-3: `IsolatedRun#difference`);
  - the door hands the test runner a WHOLE judge: the product's, or the edge's compatibility host judge. The
    per-decision seams by which the edge reaches into the product's judges and runner go (`testexec` `EdgeRun`,
    `EdgeHearing`, `HostWords`, `HostForms`; `ExecuteOptions.recording` becomes the door's choice of judge);
  - **the SQL-text appeal and the referee move with the edge's judge, unchanged in behaviour** (B1's design,
    2026-11-19). The appeal needs things the door cannot hand over in advance:
    - **planning-time artifacts:** each effect step's statement texts and query flags (`testplan/TestDoor.java:144`);
      each seed's referee copy, built from the CSV load's MIR in the referee's dialect (`testplan/EffectPlanner.java`,
      consumed at `TestDoor.java:148`), which the plan does not carry; the engine-text assert's planned comparison
      (`EngineTestText`);
    - **live run state:** the body's session and the system database leases after the body runs or fails
      (`testexec/TestRunner.java:126`, `:132`); per-step events (`:440`, `:466`); the verdict callbacks;
  - so the edge judge is ONE object handed in at the door, with TWO narrow typed interfaces, one for planning and one
    for the run. The product keeps exactly two call sites: `TestDoor` (planning) and `TestRunner` (verdicts and
    steps);
  - **the planning half stays DATA** (the independent review, 2026-11-19; the user's ruling of 2026-11-08, plan as
    data). Today the edge plans its effect and seed records into the test plan's typed extension point
    (`TestDoor.java:141-149`, `TestPlan.extensionsByStep`, `testexecutionplan/Extension.java`). That stays: the
    planning interface is `testexecutionplan.Edge`, which writes into the plan, and the corpus's decoded-plan guard
    keeps seeing it. Only the per-decision RUN seams are replaced by the one run interface: `EdgeRun`, `EdgeHearing`,
    `HostWords`, `HostForms`.
- **Deletes:**
  - `VerdictSql`'s two-form branches (the census's N-JUDGE, 38 hits);
  - `UpstreamVerdictWords` (198), `CompiledPureVerdictNumbers` (54);
  - `EngineTextGates` (110) and `PlanStyle.judgesEngineText` (JUDGES-6, 13 hits);
  - two-forms `JsonGrammar`, `RelationCounting`, `VerdictNumbers`, `RenderedTextRule`, `DateLiteralTexts`,
    `GoldenNullWord`, `VerdictWords` (the edge's form), and `CrossKindNumbers`' judge half;
  - `edge.StableScanOrder` (598) and `sql.ScanOrder`'s edge use;
  - the four residue pins ratified at 20:10-R3 shrink (`DatabaseJudge` 852 at `main` after R9, `HostJudgments` 679,
    `BodyDriver` 545, `EngineTextPlanning` 151), each re-pinned with the user's word and a dated note;
  - the guard lists that name today's seams move, for the user's word: `HookRulingTest.RULED`,
    `ProductFormsUseTheirArgumentsTest.RULED` (`NoEdgeRun$Hearing#decide`/`afterFailure`), `HookLambdasTest`,
    `EdgeOnlyMembersTest`.
- **Check:**
  - channel B under HOST unchanged;
  - channel A's PURE and ENGINE outcomes move only toward their oracles;
  - the corpus rosters unchanged, except unordered-assert lines that now pass as multisets (listed);
  - core's BOTH green.
- **Guards:** the census's 52 J hits leave (a); `JavaEvalLedgerTest`'s pins shrink.
- **Size:** 8.5 days under CP-14's option (ii), 11.5 under (i) (the judges session's map); less under (iii) if the
  leniencies are unused.
- **More is deleted than moved:** the database judge's appeal paths (`rowAppealed`, `RootLoops`' row appeals,
  `EngineTestText.batchTexts` and its gate, the database gate) go with the compatibility database judge. The
  `EngineTestText` facet's 13 methods reshape into one claim at the test planner's per-assert entry.
- **Depends on:** 2.1. **Owner:** the judges session.

**2.3 Upstream's SQL text and shapes go, checked by answers** (rule 4, CP-4). **Split in two** (the independent
review):
- **2.3a, early:** the engine-text printers alone (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`,
  `EngineShape`, the plan-text family's SQL spellings, `RelationalRootForm.apply`). They are already measured by B2's
  probe (152 tests moved, 3 answer differences), so 2.3a depends only on CP-4 and the appeal staying;
- **2.3b, after 2.2:** the shapes that run in executed SQL (`existsJoin`, `whereZones`), the executed generated DDL and
  `raw`.
- **Changes:**
  - lite's own SQL everywhere an answer carries SQL text (`toSQLString`, `planToString`, `activities[].sql`,
    generatePlan's `sqlQuery`, the test-data generator's `sqls`);
  - **the SQL-text appeal and the referee STAY** (ruled: GOALS_OF_RECORD G6 and §2 E3, 2026-11-07; the charter
    `docs/SQLTEXT_ROW_VERDICT_CHARTER.md`, 2026-09-01, and its 2026-09-21 ruling). Under CP-4 lite's text never equals
    legend-engine's, so EVERY SQL-text assert goes to the appeal, which checks the answer: the golden statement
    replayed on the referee against lite's rows. That is the charter's own first rule, "a SQL-text assert's verdict
    is ALWAYS rows". The user, 2026-11-19 ~04:30 UTC: "one of the crown jewels of this project";
  - corpus tests whose appeal cannot decide (B2: a golden the referee cannot replay, lite's rows underivable there, a
    printed string compared with no appeal) form the listed "not promised" class: counted, never silently dropped;
  - `toSQLString` for a database lite has no dialect for (DB2, Composite, Sybase, Oracle, SqlServer and the rest:
    today `EngineStyleDB2` serves DB2 and Composite, and plan text refuses the rest, `edge/PureText.java:530-535`) is a
    refusal by name, counted (B2 found 23 DB2 and Composite goldens among the declined appeals);
  - **statements a program EXECUTES are not text** (B1's review): `toDDL`'s create and drop texts
    (`edge/EnginePlans.java:958-995`) and `setUpDataSQLs` (`:754`, `:767`) are written for H2 whatever the target, and
    other targets run them through `PureText.raw`'s translation. They become target-aware lite DDL in this slice, and
    `raw`'s translation of generated text goes with them;
  - each shape-only hook is deleted where the answers are equal; where an answer changes, the strand becomes an R0
    rewrite or an X refusal (rule 4's exception), named in the PR.
- **Deletes:**
  - `EngineStyleH2` (2,507), `EngineStyleDB2` (365), `EngineStyleComposite` (79);
  - `RelationalRootForm.apply` alone (N-ROOTFORM). Its `primaryKeyColumns` is the product resolver's own key rule on
    executed paths (also `resolver/StoreResolver.java:3636`, `Substitution.java:1837`) and stays;
  - the SQL spellings of the plan-text family (`EnginePlans`, `PlanText`, `PlanEnumForm`, `InProtocol`,
    `PlanVariables`; its plan FORMAT stays as 1.4's presenter);
  - `EdgeLoweringForms.EngineShape` and with it the text-only halves of `LoweringForms.view`, `wireCast`,
    `unionFrame` (C1), `relations`, `keyedCounts` and `computedPruning`;
  - `LoweringForms.existsJoin` (C2) and `whereZones` (`EngineWhereZones`), which run in executed SQL: answer-equal by
    S367's measurement, but they set H2's row order for unordered results, so they go after 2.2's multiset compare;
  - two-forms `ActivityTexts`, `RoutedQueries`;
  - facet `UpstreamPlans`' SQL half.
- **Check:**
  - the corpus on H2 and DuckDB: every test that passed by answer still passes;
  - the "not promised" class equals 0.4 (e)'s count of tests the appeal cannot decide (B2 measured 143);
  - the appeal decides every other SQL-text assert, and none of them regresses;
  - programs that execute generated DDL seed the same data on every target (their answers unchanged);
  - pure/v1's net, with `activities[].sql` lite's own (moved lines listed);
  - the lean-SQL baseline (`lean-sql-baseline.tsv`) only shrinks.
- **Guards:** the census's SQL-shape hits leave (a); `DialectLeakRatchetTest` and the edge's line count shrink.
- **Size:** 5 days.
- **Depends on:** CP-4, 0.4 (e), 1.4, 2.2. **Owner:** B2, or the judges session after 2.2.

### Phase 3: the translator on the way in (about 14.5 days; tracks A and O)

H3 (a) and (b) proved the pattern and found three conditions, which shape this phase:
- **the rewrite must know the target database** (legend-engine's meaning is its database's: target-blind, DuckDB
  scored 17 of 25; target-aware, 24 of 25);
- **the ENGINE audience is whole models**, not request lambdas (the corpus's 15 newly failing tests);
- **a capability must be one SQL node** each dialect writes once (as a lowering tree it cost 350 times the SQL). Under
  CP-5's revised recommendation no capability is built, so this condition holds by having none.

**3.1 The product's own two dependencies on today's meaning, split first** (H3 items 5, 6).
- **Changes:**
  - lite's Cypher translator writes Pure in the core's meaning, not legend-engine's 1-based `substring`
    (`CypherToPure.java`);
  - the `###Relational` dynaFunction `substring` gets its own rule (SQL's 1-based function, upstream's DSL meaning in
    every audience), separate from the Pure function's;
  - lite's `#SQL` island (`protocol/sql/PostgresFunctions.java:447-458`) and `#IQ` island (about 13 `substring`/`indexOf`
    emissions in `IqFunctions`, plus `IqAggregates:1489`) write Pure in today's meaning too: each is checked and moved
    to Pure's (the independent review).
- **Deletes:** the shared rule.
- **Check:** `CypherIslandIntegrationTest`, the DynaFunction property-mapping tests, the corpus's `propertyfunc` tests
  unchanged.
- **Size:** 2 to 2.5 days (H5: 47 call sites in 4 files, `CypherToPure` 21, `PostgresFunctions` 10, `IqFunctions` 14,
  `IqAggregates` 2; without the split, Pure's meaning breaks about 1,240 of their parity cases), plus 0.5 day and one
  `NativeCatalogGovernanceTest` ceiling move (the user's word) when 3.3's "a negative start raises" lands, since the
  dynaFunction `substring(col, 0, n)` then needs its own verbatim rule.
- **Depends on:** nothing (it is right under either R4 answer). **It must land before 3.3.** **Owner:** the
  translator session.

**The translator's rule, in order** (for each place where upstream's meaning differs from lite's):
1. **the same meaning:** the code passes through untouched (most code);
2. **a different meaning that lite's ordinary functions can compute:** the code is rewritten to compute upstream's
   answer (R0), for the target database;
3. **upstream itself refuses it:** refused, in upstream's words (E-R1);
4. **otherwise:** lite gives Pure's answer and records it as a register row, or refuses by name where Pure's answer
   would mislead.

**3.2 The translator's skeleton: model-level and target-aware.**
- **Changes:**
  - **one translator per upstream audience** at the edge's doors: pure/v1, the corpus, PCT channel A (ENGINE and
    PURE), PCT channel B (PURE), the test-data generator, PURE's direct entry;
  - **its input:** the parsed model, the query, and the target database read from the request's runtime (knowledge,
    not a mode);
  - **its output:** a lite model and query, or a refusal by name worded by 1.2's presenter;
  - **rewrites are lite programs:** an edge-owned compatibility library of Pure source per audience and target (H3's
    `compat-engine-<target>.pure`), plus call retargeting. A parsed model has no types: which overload a call means
    (String `indexOf` or the collection one) is decided at G. So the rewrite works on RESOLVED calls, through today's
    public entries (`Compiler.parseSources`, `buildModule`, `resolveQuery`, `queryResolved`), or overloads every
    upstream overload as H3 did (the independent review);
  - **a model compiles once per (audience, target):** what that costs the server's pure/v1 projects and the
    `HeldModels` caches is measured in the phase-0 spike;
  - **the appeal's "our rows" come from the translated program,** or the appeal judges the wrong meaning. The appeal's
    derivation is one of the translator's doors: a dependency between 2.2b and 3.2 (the independent review);
  - **upstream's flags are the translator's input,** never the core's: S130's `CORRECT_SQL_SUBSTRING_INDEXING`
    first;
  - **the target's settings are the translator's knowledge:** by default (CP-8) the edge's H2 runs H2's own settings,
    and `DatabaseSettingsForm` goes. If 0.4 (h) shows material moves, the door declares the target's settings as data
    (`ExecutionPlan.Target.settings`, which exec already verifies), and the count goes to the user first;
  - **legend-engine's H2 text that a user wrote for `executeInDb`,** run on another target: the translator converts
    it, or refuses by name, by 0.4's count (today `PureText.raw`; generated text leaves in 2.3);
  - **the translator reaches the core only through the public doors** (`Compiler.query`, `TypedQuery`).
- **Deletes:** `FeatureRules`' flag-keyed substring choice from the core (S130's flag moves); `DatabaseSettingsForm`
  (unless 0.4 (h) says otherwise); `PureText.raw`'s remaining half.
- **Check:** every door's suite byte-identical with the translator in place and no rewrite registered (a pass-through
  first).
- **Guards:**
  - ArchUnit: the translator package reaches the planner only by `ArchitectureTest.PLANNER_ENTRIES_ONLY`'s public
    entries;
  - the translator evaluates nothing: no `values` arithmetic, rewrites only;
  - 1.3's print-position analysis (carried or read) is a typed flow analysis: it reads types and computes nothing,
    and its operation lists are pinned;
  - the compatibility library's Pure sources are edge resources, refused by the product loader
    (`LibraryPlatformNamespaceGuardTest`'s pattern).
- **Size:** 3.5 days.
- **Depends on:** CP-1. Its pass-through starts at once; its refusals are worded through 1.2 once that lands. **Owner:**
  the translator session.

**3.3 R4 lands.** Under CP-2's recommendation (Pure):
- **Changes:**
  - the core's `substring` is always Pure's (0-based, end-exclusive), and its `indexOf` is 0-based with -1 on a miss
    (H3's Scalars +21/-26);
  - the ENGINE translator rewrites both per target (H3's target-aware library);
  - the 15 corpus tests run through the model-level rewrite;
  - the core's tests that pin the old meaning move, listed;
  - saved queries and cubes in deployed stores cannot be scanned from the repository, so a saved artifact carries
    the meaning version it was written under, and an old `substring` is detected, not silently re-read (the
    independent review);
  - the product's parity bug, DuckDB's `SubstringClamp` against H2's count-from-end (H3 item 4), is fixed in the
    core: a negative start raises, as Pure does.
- **Deletes:** S130's flag path; `SubstringClamp`'s divergence.
- **Check:**
  - PCT essential on DuckDB: S134's 3 known failures pass against PCT's expected values (ledger rows move, with the
    user's word);
  - the oracle: H3's 25-request batteries, made permanent, at H2 25 of 25 and DuckDB 24 of 25 (the one miss, a
    negative length, refused by name);
  - the corpus on H2: the 15 tests pass again through the rewrite.
- **Guards:** the batteries in the oracle lane.
- **Size:** 2.5 days. Under the other answer (legend-engine), the PURE audience gets the rewrite instead: 1.5 days.
- **Depends on:** CP-2, 3.1, 3.2. **Owner:** the translator session, with B1-R for the oracle.

**3.4 The R0 rewrites, in two batches.**
- **Changes:** each R0 row of H1 (35) becomes a rewrite in the compatibility library, or a target-aware rule:
  - batch 1, the lowering's forms: `isNumeric` (S370, D12), `parsedDecimal` (S365, D10), `parsedJson` (S366, D11),
    a list's text in a query (D06), `%r` (D04), cross-kind numbers (S127's product half), and SQLite's Decimal
    arithmetic as REAL for the legend-pure audience (ruling (c), D16, C27; `PureText.refusesDecimalArithmetic`) by
    CP-11;
  - batch 2, the rest: dispatch by legend-engine's registration order (S3), `average([])` (S19), a class mapping with
    no `~src` (S21), the precise primitives' constraints (S140), the pivot column names (S107), Legend SQL's rows
    (S43, S44, S46, S47), the protocol's lenient reading (S29), and the other R0 rows 0.4 assigns.
- **Deletes:** `LoweringForms.isNumeric`, `parsedDecimal`, `parsedJson`; `CrossKindNumbers`' product half;
  `PureText.refusesDecimalArithmetic`.
- **Check:** the ENGINE PCT lane (0.3) and the corpus move only toward the manifests and rosters; the oracle on each
  rewrite's battery; PURE's lanes toward PCT's expected values.
- **Guards:** the census's matching M3 rows leave (a); the strand ratchet shrinks.
- **Size:** 4.5 days (2.5 + 2).
- **Depends on:** 3.2. **Owner:** the translator session; B1-R writes each battery.

**3.5 Where legend-engine refuses, lite answers** (E-R2, CP-3).
- **Changes:**
  - the ENGINE lane's referee, where the manifest says legend-engine refuses ("No SQL translation exists"), is PCT's
    expected value. 0.3's derived table now classifies the lane's cases; it no longer drives refusals;
  - one blanket register row: "where legend-engine refuses at planning, lite answers";
  - the translator refuses only what lite itself cannot do, by name;
  - H1's 17 upstream-refuses rows and 4 ENGINE cells become S under that row. S1's whole-model compile failure is not
    copied either.
- **Deletes:** nothing; no refusal table is built into the translator.
- **Check:** the ENGINE PCT lane. Where legend-engine passes, lite passes; where it refuses, lite's answer matches PCT's
  expected value or is a listed row; where it fails by defect, lite answers correctly.
- **Guards:** the derived classification table is regenerated by its diff test.
- **Size:** 0.5 day.
- **Depends on:** CP-3, 0.3. **Owner:** the translator session.

**3.6 No special functions: the R1 rows decided without them** (CP-5, recommendation revised 2026-11-19 after the
user's question "is this a sneaky way to have two sql paths").
- **Changes:** the translator uses only functions every lite user has. H1's 24 R1 rows are re-classed in three ways:
  - a print at the end of a query is the presenter's (P), in Java after the run. This covers PCT's `toString` and
    `format` tests (H2: 84 of PCT essential's 95 query-side sites return the text as it is);
  - where lite's ordinary functions can compute legend-engine's answer, it is an R0 rewrite, per target. For
    example, legend-engine's unchecked `sqrt` of a negative answers null on H2: `if($x < 0, |[], |sqrt($x))` (D08,
    K6's cases, one by one);
  - where they cannot, lite answers Pure's way, as a register row (E-R1's defect rule, CP-3). Example: Float
    arithmetic as exact decimals on H2 and DuckDB (S9, S20);
  - text built inside a query whose CONTENT is then read (1.3's rule), over a type whose text differs, is refused by
    name (X). H2 counted 0 such sites over Float, Decimal or dates in PCT and the corpus.
- **Deletes:** the special-purpose machinery these rows have today:
  - S20's bare-DECIMAL literal (`SqlDialect.floatLiteralKind`'s edge use, `PureText.floatLiteralKind`);
  - `LoweringForms.raise`'s plain set (B7), each class-A raise first given its 3.4 guard rewrite where one exists;
  - two-form `TestDataPlanner.Printing` and `ExecuteOptions.withEngineCompat`, moved to a presenter. 20:10-R2's
    "for now" spellings are revisited there.
- **Check:** every lane change listed with its reason; the PURE lanes may move only toward PCT's expected values,
  except the listed refusals; the ENGINE lane's new register rows counted from the manifests and the oracle.
- **Guards:** `NativeCatalogGovernanceTest`'s verified sets do not grow. A future special function needs the user's
  word, case by case, with its measured gain.
- **Size:** 1.5 days.
- **Depends on:** CP-5, 0.4 (j), 1.3, 3.5. **Owner:** the translator session.

**3.7 PURE's direct entry** (the user's 2026-11-18 19:45 ruling "planning mode/direct entry for now").
- **Changes:** one Java door, `edge.PureEntry`, that runs the PURE translator and the PURE presenter over a model and
  a lambda.
- **Check:** PURE tests on DuckDB, H2 and SQLite.
- **Size:** 0.5 day.
- **Depends on:** 3.2, 1.3. **Owner:** the translator session.

### Phase 4: the close-out (about 3.5 days; track G)

The r1 slice "4.1 the optimiser switches" is gone (B1's review). The key fold, pruning and the let's relation read
twice already run as the product's in executed edge SQL, so their text-only halves go in 2.3. The let splice (S129),
savepoints (S135) and scan order (S14, now J) have no hook to delete. They are register rows, settled by 3.6.

**4.1 `PlanStyle` deleted; the census blocking.**
- **Changes:**
  - `PlanStyle`, `PlanStyleFacets`, `LoweringForms`, `PureText` and its 12 dialect subclasses, and the hooks'
    plumbing through the 43 product files are deleted;
  - `dialect(type)` becomes `Databases.dialect(type)`;
  - the strand ratchet and `ModeBranchCensusTest` read 0 and block;
  - the census's (a) and (p) rows are closed or ruled to (b), and `MeaningCensusTest.BLOCKING = true` (B2's §10);
  - the plan-scan rule (done-when 3) blocks.
- **Deletes:** about 900 lines of `PureText` and the hook plumbing (0.4 measures it); the guard tests whose subject is
  gone (`PlanStyleSurfaceTest`, `HookRulingTest`, `HookLambdasTest`'s hook lists, `EdgeOnlyMembersTest`'s style rows),
  replaced by the "no mode in the core" ArchUnit rule.
- **Check:** every lane byte-identical against the slice before (the deletion is mechanical by then); javac's
  exhaustiveness.
- **Guards:** done-when 1, 2 and 3, all blocking.
- **Size:** 3.5 days.
- **Depends on:** every strand closed. **Owner:** B2, with this session.

### Phase 5: portability (about 6.5 days; track O)

**5.1 Export.**
- **Changes:** `edge.PlanExport` writes a lite plan as legend-engine's `SingleExecutionPlan`, carrying lite's own
  SQL. It is reached from the ENGINE presenter's plan format (1.4).
- **Check:** legend-engine 4.145.0's `PlanExecutor` runs the exported plan (the oracle's `execute_plan`). Its answer
  bytes equal lite's ENGINE answer on the pure/v1 golden requests and a fixed set of parameterized corpus service
  queries. Refusals are named and counted.
- **Size:** 2.5 days.
- **Depends on:** 1.4, 2.3. **Owner:** B1-R.

**5.2 Import.**
- **Changes:** first a census of legend-engine's own plans for the same queries, by node type (`plan` target); then
  `edge.PlanImport` for every plan whose nodes map.
- **Check:** lite runs and presents each imported plan with answer bytes identical to legend-engine executing it; the
  refused share stated, with its node types.
- **Size:** 4 days.
- **Depends on:** 5.1. **Owner:** B1-R.

### Throughout

- **The census** runs report-only in the checks lane. From 0.4 its (a) count is a shrink-only pin, a new pin added by
  the user's word, and each slice's PR states its delta. It blocks at 4.1.
- **The strand ratchet** (0.4 (a)) shrinks with every strand closed.
- **The per-push lanes** (CP-9): the edge net, the ENGINE PCT lane, the SQLite PCT recording, the host byte-parity
  suite. The oracle test stays in `//gates:heavy`, and every program PR runs it.
- **The register** gets a row for every ledgered difference a slice creates, posted to the user before it lands
  (the upstream-fidelity tenet).

---

## 5. The rulings

Each ruling below gives the question, the options, the recommendation and what moves under it. The first five need
the user's call; CP-6 to CP-10 take the recommendation unless the user objects.

**The rulings record.**
- **2026-11-19 ~04:02 UTC, the user's words:** "1-3 yes", "Explain 4 clearly, what's the alternative? Is this a
  sneaky way to have two sql paths", "Let's not worry or even support older database versions".
- **The numbers are read against this session's first note to the user,** whose list was:
  1. R4: Pure;
  2. E-R1;
  3. tests that compare legend-engine's SQL text: never promised;
  4. the special functions;
  5. the per-push lanes and the database versions.
- **So:** CP-2, CP-3 and CP-4 are RULED (option A each), and CP-8 is RULED (not supported). CP-5's recommendation was
  revised in answer to the question, and is with the user. CP-1 is confirmed with the plan's ratification.
- **~04:30 UTC:** "Switch e-r1", so CP-3 is E-R2. The user also ruled that the SQL-text appeal is kept: "We definitively
  should not delete the fallback that checks answers when SQL doesn't match right? That's one of the crown jewels of
  this project".
- **~10:45 UTC:** "yes to 1-5 and the independent review, do as much homework, research, measurements right now up
  front as possible and write down all the learning/outcomes/details immediately". The five were this session's start
  list, so these are RULED:
  1. CP-1, the architecture: translator in, formatters out, no modes in lite's core;
  2. program sessions may create and push their own `claude/compat-*` branches;
  3. two new sessions now (judges, translator) and a third for the hosts in week 2, all at xhigh;
  4. CP-9: the compatibility checks on every push, and the oracle's `jars_table` entry for the heavy lane;
  5. CP-10: the housekeeping.
- **Also started:** an independent review of the design and the plan. Phase 0 proceeds now. CP-6, CP-7, the 6
  plan-structure tests and PCT under the LITE judges (§12 I5) wait for the phase-0 checkpoint; CP-5 and CP-11 come
  later, each with its numbers.
- **The two checkpoints the user accepted:**
  - after phase 0: the exact lists, the counts and the independent review; the user confirms or changes phases 1-3;
  - after the translator's first real rewrite, moved by the independent review to the end of the phase-0 translator
    spike (H5 and task 0): if it needed a change inside the core, or took far longer than
    planned, stop and re-plan.
- **A standing rule:** a slice that needs a new hook in the core stops and goes to the user.

**CP-1. Translator in, formatters out, instead of three modes.** *Gates every phase.*
- **The question:** replace the three modes (ruled 2026-11-18 19:45) with this design? The core has one meaning; an
  upstream program is translated on the way in, and its answer formatted on the way out.
- **Recommendation: yes.** H1 classed all 281 known differences without a mode. H3 showed each half working against
  upstream itself. A mode is a parameter the whole core must carry, and today that costs 43 product files.
- **What moves:** B1's `Mode` records and S1's words-in-the-plan are withdrawn (§2). The rulings about meaning and
  words stand.

**CP-2. R4: lite's own `substring` and `indexOf`.** *Gates 3.3.* **RULED: option A, Pure** (the user, 2026-11-19 ~04:02 UTC,
"1-3 yes", item 1 of this session's first note).
- **Option A, Pure (recommended).** `'abcdef'->substring(1, 3)` is `'bc'`, and `indexOf` is 0-based with -1 on a
  miss, the same on every database. Legend-engine's answers come from its translator.
  - **For:** it follows the principle already applied in S365, S366 and S370 (Pure's definition in the product,
    legend-engine's quirks at the edge). Legend-engine's meaning is really each database's (start 0, a negative
    length, DuckDB's folder: H3 §3), so the product cannot follow it on every database anyway: today's code disagrees
    with legend-engine on DuckDB in 9 of 25 checks. PCT's 3 `indexOf` known failures pass.
  - **Against:** 15 corpus tests need the model-level rewrite, and the core's tests that pin today's meaning change.
- **Option B, legend-engine.** Today's behaviour stays; PURE gets a rewrite.
  - **For:** the core does not change.
  - **Against:** the product keeps a meaning that differs by database and from Pure; the DuckDB clamp answers
    neither database's way.

**CP-3. (e′) What the legend-engine audience refuses.** *Gates 3.5.* **RULED: option B, E-R2: the legend-engine route
answers everything lite can** (the user, 2026-11-19 ~04:30 UTC, "Switch e-r1", replacing the ~04:02 ruling of E-R1
after the trade-off was put: E-R2 is smaller, and the translator never copies legend-engine's compile-time
rejections, which is how it would grow into a second compiler).
- **What it means:**
  - where legend-engine refuses a program and lite can answer it, lite answers. One blanket register row records it,
    and the lane judges those cases against PCT's expected values;
  - where legend-engine answers, lite still matches it (the rewrites);
  - where legend-engine answers wrongly, lite answers correctly;
  - programs written against lite's legend-engine route may not run back on legend-engine itself. Plan export is not
    affected: an exported plan carries lite's SQL.
- **Option A, E-R1 (recommended).** It refuses exactly what legend-engine refuses at planning ("No SQL translation
  exists"), per database, from a table derived from legend-engine's manifests, in its words. Where legend-engine
  fails by its own defect, lite answers correctly, with a register row.
  - **For:** the legend-engine lane then compares exactly against legend-engine's own manifests; Studio clients never
    get an answer legend-engine would not give, except a defect fixed.
  - **Against:** the translator must re-implement two of legend-engine's compile-time rules (S146's function id rule,
    S25's static `Number` typing).
- **Option B, E-R2.** It answers everything lite can.
  - **For:** less work.
  - **Against:** a large "better than legend-engine" ledger, and answers legend-engine never gives.

**CP-4. Upstream's SQL text is never promised.** *Gates 1.4 and 2.3.* **RULED: option A, never promised** (the same
words, item 3). B2's count of the corpus tests it moves (0.4 (b)) is reported to the user before 2.3 lands.
- **Measured 2026-11-19 ~04:20 UTC by B2** (every engine-text printer answering lite's own SQL; judging unchanged;
  the baseline the committed rosters):
  - **143 distinct corpus tests move to "not promised"** (DuckDB 143, H2 139). They were judged on legend-engine's
    text alone: a declined appeal (64), a printed SQL, plan or DDL string compared (22), a construct lite's printer
    has no form for (53, mostly `planToString` goldens), or SQL read from legend-engine's plan (4);
  - **6 tests assert legend-engine's PLAN STRUCTURE**, not SQL text (`testSupportStreamFlag*`: a
    `FunctionParametersValidationNode`'s `supportsStream`). Recommendation: not promised, as an internal shape (rule
    4); phase 5's export decides whether it comes back naturally;
  - **3 DIVERGED appeals, the only answer differences:**
    - `testToSQLStringForTDSStringJoin` and `testHashFunctions`: legend-engine's golden SQL writes `joinStrings` with
      the separator appended (`concat(a, ' ', b, '|')`), against Pure's `joinStrings`. Lite answers Pure. That is
      upstream's defect: a register row (0.5);
    - `testToSqlGenerationFirstDayOfWeek`: lite answers Monday on DuckDB (Pure's `firstDayOfWeek`, legend-engine
      `dateExtension.pure:202`) and Sunday on H2. That is a lite parity bug (G2), handed to the product owner;
  - no test moved from fail to pass; the runtime was 10.5 minutes for both lanes;
  - the record: `docs/records/census-b2-sqltext-probe-2026-11-19.md` on `claude/census-b2` at `32ef19118`;
  - the 143 were measured with the rows appeal running, which is how it stays: the appeal is kept (the user,
    ~04:30 UTC). So 143 is the cost, not a floor.
- **The question:** legend-engine's own suite calls `toSQLString` 588 times in 64 files (H1); its test-data
  generator's `sqls` are pinned by 45 asserts (S193); its answers carry SQL in `activities` and `sqlQuery`.
- **Option A, never promised (recommended).**
  - Lite answers its own SQL wherever upstream returns SQL as a value.
  - Corpus tests that judge SQL text become a counted "not promised" class, never silent failures.
  - Envelope fields that are a function of upstream's SQL carry lite's own value in upstream's envelope, under one
    register row: the `COUNT(*)` label of `size()` (O3), and the database's error text over upstream's statement.
  - **For:** it deletes about 2,950 lines of engine-style printers plus legend-engine's root form, union framing and
    exists-join shapes from the product. Plan export does not need them (it carries lite's SQL).
  - **Against:** the corpus's SQL-text tests stop counting as passes. 0.4 (b) gives their exact number before 2.3
    deletes anything, and I will report it before that slice lands.
- **Option B, keep reproducing legend-engine's SQL text.**
  - **For:** those tests keep passing.
  - **Against:** a second SQL printer and upstream's SQL shapes stay in the planner for good, and it is text, not
    answers.

**CP-5. Special functions only the translator calls.** *Gates 3.6 and 4.1.* **Recommendation revised
2026-11-19.**
- **The question:** H1 found 24 upstream answers that the core cannot compute with what it has, such as legend-pure's
  exact print of a Float inside a query, or legend-engine's unchecked `sqrt`. Should the core gain special functions
  (H1's K1 to K19) that only the translator calls?
- **Option A, none (recommended now).** The translator uses only functions every lite user has:
  - a print at the end of a query is the formatter's job (Java, after the run);
  - where legend-engine computes differently from Pure's own definition, lite gives Pure's answer and records it
    (CP-3's defect rule);
  - the rest is refused by name.
  - **For:** one vocabulary and one SQL path, with nothing in the core that exists only for upstream. H2 says real
    upstream programs essentially never need these: 0 sites print a Float, Decimal or date and then compute with the
    text. The demand came from PCT's unit tests, which the formatter covers.
  - **Against:** answers that match legend-engine today through S20's bare-DECIMAL literal (Float arithmetic as
    exact decimals on H2 and DuckDB) or the plain set (an unchecked operation answering null) stop matching where no
    ordinary-function rewrite reproduces them. Each is a register row, and the lanes move away from legend-engine
    there. **The count comes from 0.4 (j), by case, before anything is deleted.** Many can be rewritten
    (`sqrt(-2.0)` answering null is `if($x < 0, |[], |sqrt($x))`). S20's bare-DECIMAL literal (built
    2026-11-17) is deleted.
- **Option B, a short pinned list.** Each special function is one SQL node and one meaning, called only by the
  translator, approved one by one.
  - **For:** those quirks are matched exactly.
  - **Against:** it is a second vocabulary that exists only for upstream: the user's "two SQL paths" by the back
    door, and a list that tends to grow. S20's literal kind (a dialect property) and B7's plain set (a lowering
    choice) are the same thing in another form.
- **Either way:** the four that would switch a product optimisation off (K7, K10, K12, K16) are never built.

**CP-6. The typed facts the result must carry.** *Gates 1.1.*
- **Carry, because they are lite's own meaning:**
  - a DateTime's written precision (D02, S118);
  - a column's Pure type, and whether it is stored or computed (legend-engine's `relationalType`; slice R's
    `VARCHAR(8192)`);
  - which cells are object references (S362);
  - the parse's kept spans (S369);
  - the declared static type (S25, S41).
- **Do not carry, because they are facts of upstream's SQL:** legend-engine's Decimal scale (S104), the JDBC metadata
  of upstream's statement (S125). Those rows stay ledgered.

**CP-7. Error words leave the plan.** *Gates 1.2 and 1.6. It re-cuts (h) K1.*
- **Upstream words never enter the plan:** the presenter words them from the site's kind and literal operands.
- **Lite's own sentence leaves the plan in 1.6.** The tab and Python then read lite's sentences from a table generated
  from Java (one rule, no copy by hand, no call into Java on the failure path). Until then it stays, as K1 ruled.
- **One guard row, the user's word:** `EdgeOnlyDataTest` will report `Site#operands: by the edge`, because only the
  upstream presenters read a site's literal operands. Lite's own sentence stays without operand values (the
  2026-11-10 ruling). Recommendation: accept the row, rather than change lite's words.
- **Why:** architecture rule 2, and H4's finding that a rule held in two places drifts.

**CP-8. legend-engine's database versions: not supported.** **RULED by the user, 2026-11-19 ~04:02 UTC:** "Let's not
worry or even support older database versions".
- legend-engine runs a patched H2 2.1.214 (`MODE=LEGACY`) and DuckDB 1.3.0; lite runs H2 2.5.252 and DuckDB 1.5.6.
- **What it means:**
  - lite never emulates them, and an answer that differs only for that reason is not chased (`cell.float`'s 8
    digits, O5; a Decimal division's scale, S141, whose K15 is dropped; legend-engine's H2 UDFs);
  - one blanket register row records the ruling, so the upstream-fidelity tenet holds without a row per case. The
    oracle's diff file still lists each version-caused difference by case, so differences stay attributable (the
    independent review);
  - `DatabaseSettingsForm` is deleted (3.2). Lite's own sorts already stamp Pure's null placement, so S368 needs no
    rewrite. `DEFAULT_NULL_ORDERING=HIGH` changes only raw `executeInDb` SQL a program authored, which no rewrite can
    reach: a register row, unless 0.4 (h) shows material moves (then the target's settings become data, 3.2).

**CP-9. (d′) The per-push lanes.**
- **Add to every push:** the edge net, the ENGINE PCT lane (DuckDB's 5 suites and H2's relation suite), the SQLite
  PCT recording, and the host byte-parity suite.
- **Cost:** about +2 to +4 minutes per push, by the old design's figures; 0.2 and 0.3 re-measure it.
- **The oracle's batteries run in two halves** (B1-R's design, 2026-11-19):
  - **per push:** lite's rows are compared with legend-engine's answers, pinned as files. No legend-engine JVM runs,
    so it is cheap and needs no allowlist entry;
  - **heavy lane:** an update target re-asks legend-engine (one JVM per database, about a minute each) and fails on
    drift. Every program PR runs it.
  - **What is compared:** rows with the SQL masked; refusals by status plus a mapped refusal class.
  - **Operands:** column operands, plus a separate literal set, since DuckDB's constant folder and its runtime
    disagree (H3 §3).
  - **The lite side's runner** is a real Bazel target, an `EDGE_TEST_SRCS` entry, so that guarded-list entry is named
    in its PR for the user's word.
- **One allowlist entry, the user's word:** `tools/engine-runner` as a `duckdb_jdbc` user in
  `tools/deps/jars_table.bzl` (B1-R's pending item), so the oracle's DuckDB half builds. Only the heavy lane needs it. It is test
  tooling only (`testonly`, its pool usable from that package alone).

**CP-10. Housekeeping.**
- The register corrections S138 and S63, the prelude's register row, and the three unrecorded differences found so
  far (0.5).
- B1's three pending guard items are withdrawn, because the extension they guard is not built (§2).
- The census's 23-hit calendar group (legend-engine's `LegendCalendarSchema.<type>_Calendar` names and fiscal
  columns in product lowering, `CalendarAgg`) becomes a ruled (b) row. H1 classed it S `same`: the names are the
  calendar functions' own store contract (`calendarStore.pure:19`), not a presentation.

**CP-11. SQLite's Decimal arithmetic for the legend-pure audience.** *Gates 3.4.*
- **Today:** the edge answers Decimal arithmetic on SQLite with REAL (a double) where the product refuses (ruling (c),
  D16, C27; `PureText.refusesDecimalArithmetic`).
- **Recommendation:** keep ruling (c) as a translator rewrite on SQLite (ordinary functions, a ledgered
  approximation), not a hook. The alternative is to refuse by name, as rule 5 would by default.

**CP-12. When legend-engine differs from Pure's own definition: copy it, or answer Pure's way?** *Gates 0.6, 3.3, 3.4.*
Raised by the independent review: the plan contradicts itself here.
- **The tension:**
  - CP-3 says "where legend-engine answers wrongly, lite answers correctly", rule 5 says upstream's bugs are never
    copied, and §12 I11 says the product follows Pure;
  - yet 3.3 rewrites legend-engine's uncorrected `substring` (legend-engine itself calls its flag a CORRECTION kept
    for old queries), and 3.4 rewrites `isNumeric`'s `lower(s) = upper(s)`, `parseDecimal`'s `DECIMAL(5,2)` and
    `parseJson`'s `}{` tolerance, all departures from Pure's definition;
  - S22 and `joinStrings`' separator are classed as defects and not copied;
  - the user's 2026-11-17 23:05 rule ("the product follows Pure's own definition ... legend-engine's quirk stays at
    the edge") kept those quirks at the edge.
- **Option A, strict (the review's reading):** where Pure defines the answer and legend-engine departs from it, the
  legend-engine route answers Pure's way, with a register row. lite copies legend-engine only where Pure is silent.
  Most of 3.3 and 3.4 becomes register rows, and the translator shrinks to almost nothing: perhaps only the short
  rename list, or nothing at all. This revises the 2026-11-17 rule for the legend-engine route.
- **Option B, fidelity:** where legend-engine's behaviour is its documented, tested meaning, copy it (with ordinary
  functions); answer Pure's way only for its outright defects. More translator work; more answers match legend-engine.
- **Decided with task 0's numbers:** how many tests each strand loses (the translator session, phase 0).

**CP-13. A smaller plan (the independent review's Q4): defer what removes no upstream meaning.** *For the phase-0
checkpoint.*
- **Defer phase 5** (export and import, 6.5 days). It is a new capability and removes no upstream meaning from the
  core. But the user named it a goal on 2026-11-18 ("import upstream plans to run on our platform and export plans
  from us to run in upstream"), so it is the user's call.
- **Hand the LITE-host parts of 1.5 and 1.6 to the product track** (about 4 days): Python's lite `query/run` writer
  and the other copies of lite's own rules are lite drift, not upstream. Keep HOSTS-9's store pilot (R11(a) put it in
  this program), the CP-7 part, and 1.7.
- **Shrink phase 3** under CP-12's strict reading, and 1.4 if plan structure is not promised.
- **Together about 20 to 25 session-days**, without giving up done-when 1 to 3.

**CP-14. What the "whole" compatibility host judge is.** *Gates 2.2.* Found by the judges session's map.
- **(i) The edge's own copy** of the product's host judge (about 2,765 lines). It has no seam in the product, but it
  is a second judge to keep in step: +3 days, and drift risk.
- **(ii) The product's host judge stays the one library**, and the edge passes its rules as a typed parameter. There
  is no copy, but that parameter is today's `HostForms` by another name, so done-when 1 would be reworded to allow it.
- **(iii) The host judge emits verdict RECORDS (kind and operands), and the words are a presenter's** (lite's or
  legend-pure's), with no rules parameter at all. This holds only if legend-pure's comparison leniencies (S15, S131,
  S132, S144, S366, S137) change no verdict. On channel B DuckDB they changed none (the judges session's I5); the
  other lanes are being measured.
- **Recommendation:** (iii) if the measurement holds, since it matches the architecture (words are presentation);
  otherwise (ii), with the parameter pinned.

**Held by the previous orchestrator:** R-a, a fractional cell under an Integer property (PR #121). Its ENGINE
answer, when ruled, becomes a translator rewrite or a refusal in phase 3.

---

## 6. The guards for the end state

| Guard | What it proves | Lands | Blocks |
|---|---|---|---|
| "No mode in the core" (ArchUnit) + `ModeBranchCensusTest` (B1's G6) + the strand ratchet | done-when 1 | 0.2 report-only; the ratchet from 0.4 | 4.1 |
| The census, `MeaningCensusTest` | done-when 2 | today, report-only; shrink-only pin from 0.4 | 4.1 |
| The plan scan over product and test plans (no upstream sentence, envelope or presentation instruction) | done-when 3 | 1.2, report-only | 4.1 |
| The presenter rule (a presenter reaches only records, never exec, the planner or JDBC) | rule 3: presentation after the run | 1.1 | 1.1 |
| The host meaning lens, the cross-host byte-parity suite, one differential per export (H4 §8) | done-when 4 | 1.5, 1.6 | 1.6 |
| The translator rule (public doors only; rewrites, never evaluation; its library refused by the product loader) | rule 5: the core never knows | 3.2 | 3.2 |
| `NativeCatalogGovernanceTest`'s verified sets do not grow (CP-5) | no second vocabulary for upstream | today | today |
| G7, the computed-operand count (ENGINE at 0) | A3 | 1.2 | 1.2 |
| The per-audience lanes, each moving only toward its oracle | done-when 5 | 0.2, 0.3 | CP-9 |

Every guard ships with a planted violator it must catch. No guard depends on a clock.

---

## 7. The risks

| Risk | What it would look like | How it is caught |
|---|---|---|
| **The translator grows into a second compiler** | it starts typing, evaluating or planning on its own | it reaches the core only through the public doors; it rewrites and refuses, never evaluates (3.2's guard); a case with no exact rewrite is refused by name, not approximated |
| **A second vocabulary creeps in** | a function that exists only for upstream, or one that means "switch the optimiser off for this subtree" | CP-5: none is built; the native catalog's verified sets do not grow without the user's word |
| **An answer moves silently while a hook moves** | an upstream audience loses an answer it had | the net (0.2), the oracle, the per-audience lanes; each slice proves its lanes moved only toward their oracle, every move listed |
| **SQL size** (G3) | a rewrite that expands into a large lowering tree (H3: 350 times the SQL for one print) | prints at the end are the formatter's, not SQL; the lean-SQL baseline only shrinks |
| **legend-engine's database versions** | answers differ because legend-engine runs H2 2.1.214 LEGACY and DuckDB 1.3.0 | CP-8: one family of register rows, measured by the oracle on demand, not emulated |
| **The corpus's SQL-text tests** | CP-4 turns many passes into "not promised" | 0.4 (b) counts them before 2.3 lands, and the number goes to the user |
| **Host cost** | rows crossing into Java cost the tab about 5 µs a row (H4); the tab's module doubled since September | presenters ride the modules each host already loads; Arrow answers cross no rows (C2); module size reported per host slice |
| **Rulings arrive late** | slices wait | phase 1 and 2.1-2.2 need only CP-1 and CP-7; the translator's skeleton (3.1, 3.2) needs only CP-1 |
| **Collision with the previous orchestrator's tracks** | the judges (A5: Q3, Q3b, J1-J4) or Float literals (Q1, beside S20's literal) change under a slice | §9: a message before either side builds |
| **Merge latency** | about 20 PRs each waiting for the user's word | slices grouped into about 12 PRs (§8); each PR summarised in plain words for the merge decision |

---

## 8. Sessions, tracks, calendar, landings

| Track | Session | Slices | Calendar (weeks from ratification) |
|---|---|---|---|
| P, formatters | B1 (`session_0195rQEnAa5CYXHygAxEFh3Y`) | 0.2, 1.1, 1.2, then (after 2.2) 1.3, 1.4 | weeks 1-4 |
| H, hosts | new host session (xhigh) | 1.5-1.7 | weeks 2-4 |
| J, judges | new judges session (xhigh) | 2.1, 2.2 first (they unblock 1.3), then 2.3 if B2 is busy | weeks 1-2 |
| A, translator | new translator session (xhigh); a second one from 3.3, so the rewrites (3.3, 3.4) and the refusals and R1 decisions (3.5, 3.6) run in parallel | 3.1-3.7 | weeks 1-5 |
| O, oracle | B1-R (`session_0195rQEkcevkNQ66fixkk4FF`) | 0.3, the batteries for phase 3, 5.1, 5.2 | weeks 1-6 |
| G, guards and close-out | B2 (`session_0195rQEjuF2imym274M9Qvva`) | 0.4, 2.3, 4.1; B3's items as they fall into slices | weeks 1-6 |
| orchestration | this session | rulings, shape notes, reviews, PRs, the user | throughout |

**Restated after the independent review** (it recomputed the critical path from the plan's own sizes and "Depends
on" lines):
- the path is 2.1 -> 2.2 -> 1.4 -> 2.3 -> 4.1, not the translator;
- phase 0 is about 4.5 days by its own dependencies (0.4 (j) needs 0.3; CP-5 needs 0.4 (j); 0.6 comes before the first
  rewrite);
- with one session per track and the checkpoint gate, the work ends near day 30. So the realistic calendar is **7 to 9
  weeks**, not 5 to 6;
- the remedies, applied: 2.2 split into 2.2a and 2.2b on two sessions; 1.3 and 1.4 on two sessions; and CP-12 may
  shrink phase 3 a great deal.

The earlier estimate, kept for the record: phase 0 (about 2.5 days, its slices in parallel), then 3.1 and 3.2 (4.5), then the longer of
the two translator lanes (3.3 + 3.4, 7.5; 3.5 + 3.6, 3), then 4.1 (3.5). That is about
18 session-days, so about 5 to 6 weeks of calendar time with lane rounds, reviews and merge waits.

**B3** (the separation close-out, not started) is not a separate track. Its items fall into slices:
- C1, C2 into 2.3;
- C3's TypeScript and Python halves and HOSTS-4 into 1.6;
- JUDGES-3 and JUDGES-6 into 2.2;
- LOWERSQL-7, PRODUCT-7, PRODUCT-11 and COMPLETENESS-8 into the slice 0.4 assigns;
- COMPLETENESS-7 is ruled (b).

**Landings: about 12 PRs.**

| PR | Slices |
|---|---|
| 1 | 0.2 |
| 2 | 0.3 + 0.4's probes |
| 3 | 0.5 |
| 4 | 1.1 + 1.2 |
| 5 | 1.3 |
| 6 | 1.4 |
| 7 | 1.5 |
| 8 | 1.6 + 1.7 |
| 9 | 2.1 + 2.2 |
| 10 | 2.3 |
| 11 | 3.1 + 3.2 + 3.3 |
| 12+ | 3.4-3.7 in two or three PRs; then 4.1; then 5.1 + 5.2 |

Each PR:
- runs CI on every OS;
- gets an independent xhigh review;
- merges only on the user's word in this session.

There is no force push. A pin, a ceiling, a ledger or an allowlist moves only with the user's word and a dated note.

---

## 9. Coordination with the previous orchestrator

| Their track | Where it touches this program | Agreement needed |
|---|---|---|
| PR #121 (Q8, Q9, R11(b)) and R-a | Q9's Python error answers (1.6 builds on them); R-a's ENGINE answer (phase 3) | 1.6 starts after #121 merges; R-a's ruling relayed here |
| A5, judge parity (Q3b fixed, J3 pending a pin, Q3's design) | 2.1 and 2.2 change the same judges | a message before 2.1's shape note; whoever lands second rebases |
| Q1, Float literals and Decimal scale on five databases | S20's bare-DECIMAL literal, deleted in 3.6 | 3.6 waits for Q1's shape |
| Q2, the warehouse binds | none | none |
| main's CI | every PR here | a red `main` pauses this program's merges |

---

## 10. What this plan does not do

- It does not change the prelude or the parser (the user's 2026-11-19 ruling); upstream input acceptance stays in the
  product (architecture rule 7).
- It does not promise legend-engine's SQL text or internal shapes (CP-4), or emulate its database versions (CP-8).
- It does not copy upstream's bugs: a defect stays a register row (rule 5).
- It does not build the deferred test execution (`docs/DEFERRED_TEST_EXECUTION.md`), cross-store queries or external
  formats (H1's lite-scope X rows).

---

## 11. Measurements behind §3 (at `main` `b75d6c73a`)

- `PlanStyle`: `wc -l core/src/main/java/com/legend/database/PlanStyle.java` (173); its methods read from the file
  (10: `dialect`, `traceCell`, `twoForm`, `graphKeyDate`, `graphKeyString`, `errorWords`, `siteWords`, two `facet`,
  `judgesEngineText`).
- Product files naming it:
  `grep -rln PlanStyle --include=*.java core/src/main/java/com/legend | grep -v /edge/ | wc -l` (43).
- The hook lists: `core/src/main/java/com/legend/testplan/PlanStyleFacets.java` (`KINDS` 5, `TWO_FORMS` 16).
- `LoweringForms`: its 15 members read from `core/src/main/java/com/legend/lowering/LoweringForms.java`.
- The edge: `cat core/src/main/java/com/legend/edge/*.java | wc -l` (21,988 lines; 93 files).
- The files' sizes: `wc -l` on each file named (§4).
- The verified native sets: `core/src/test/java/com/legend/builtin/NativeCatalogGovernanceTest.java`
  (`everyLiteInternalNativeIsNamedAndVerified`, `theVerifiedSetsOnlyShrink`).

---

## 12. Implications to go through (the user's question, 2026-11-19 ~04:30 UTC)

Each item is a consequence of the rulings or the design that is easy to miss, with what the plan does about it.

| # | Implication | What the plan does |
|---|---|---|
| I1 | **R4 changes answers for lite's own users**, not just upstream's: any saved query, saved cube or DataCube calculated column using `substring` or `indexOf`, and lite's Cypher translator. DataCube's own help example (`datacube/src/calc.ts:88`, `substring(0, 2)`) is already written in Pure's meaning, so R4 makes it right | 3.3 lists every lite emitter and every saved-artifact use it can find; the change is announced as a meaning change; 3.1 fixes the Cypher translator first |
| I2 | **E-R2 is one-way:** a program written against lite's legend-engine route may not run back on legend-engine. Studio users see answers legend-engine would refuse | accepted by the ruling; one blanket register row; plan export unaffected |
| I3 | **The appeal becomes load-bearing for every SQL-text assert** (CP-4): lite's text never matches, so each one replays legend-engine's golden on the referee. The corpus lanes may get slower | 0.4 (e) measures the lanes' run time against today's; the appeal's decline reasons (B2: 64 declines) are a place to win tests back later (a referee that can replay more goldens) |
| I4 | **The referee may need legend-engine's H2 settings** (`MODE=LEGACY`) to replay its goldens, while CP-8 removes those settings from lite's executed plans | the referee is the judge's tool, not the product: it keeps whatever settings replaying legend-engine's SQL needs; 0.4 (h) confirms which settings it uses |
| I5 | **The judges split shrinks the evidence for judge parity (G2):** today every PCT test is judged by both judges; afterwards compatibility judging is host-only, and parity is proven on the 36 ported shapes | an opportunity: since lite now follows Pure (R4), most of PCT is lite's own meaning. Run PCT as a LITE suite under BOTH judges as well, which makes PCT a product test and widens the parity evidence. Proposed as an addition to 2.1, for the user's word |
| I6 | **Rewritten code must keep the user's source positions,** so a compile error or a raise points at what the user wrote, not at the rewrite | a 3.2 guard: every rewritten node carries its original span; a planted rewrite whose error points elsewhere fails |
| I7 | **"No special functions" (CP-5) moves some ENGINE answers away from legend-engine** (S20's exact-decimal Float arithmetic; unchecked operations without an ordinary-function rewrite) | 0.4 (j) counts them by case before anything is deleted |
| I8 | **Clients see lite's SQL** in `activities`, generatePlan and the test-data generator's statements (CP-4) | accepted by the ruling; cosmetic for answers |
| I9 | **Plan format bumps** (v59 in 1.2, v60 in 1.6): anything holding a serialized plan must re-plan; the three hosts move in lockstep | `PLAN_VERSION` generated from Java (1.5), so the hosts cannot drift |
| I10 | **The census blocking at the end changes how everyone works:** any new upstream spelling or branch in the product fails the build, so later compatibility work must go through the translator or a presenter | intended (done-when 2); the census's (b) rows name each ruled exception |
| I11 | **The product follows Pure, not legend-engine, from now on** (R4 plus E-R2 plus CP-5): wherever the two differ and no ordinary-function rewrite exists, the legend-engine route answers Pure's way | stated in the register's blanket rows, so the rule is visible to every later session |
