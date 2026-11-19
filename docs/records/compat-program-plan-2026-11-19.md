# The compatibility program: the re-cut plan (2026-11-19)

**Status: DRAFT r1, for review by B1's lead, then for the user's ratification.** Owner: the compatibility session
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
| **0. Rulings and the net** | the user's rulings; B1's safety net lands; the measurements the later slices need; the legend-engine PCT lane | none | 4.5 | all |
| **1. Formatters (output)** | one presenter per audience over typed results and the plan; error records as kind, place and literal operands; the hosts call the Java presenter | the SQL-built text and envelopes, about 2,000 edge lines; 3 facets and 4 two-form hooks | 19 | P (B1) and H (a host session) |
| **2. Deletions that need no replacement** | compatibility judging becomes host-only; upstream's SQL shapes go, checked by answers | the compatibility database judge (51 of the census's 142 hits); the engine-style SQL printers, about 2,950 lines | 10 | J (new) and G (B2) |
| **3. Translator (input)** | a model-level, target-aware rewrite at the edge's doors; the named capabilities, each one SQL node; legend-engine's refusals derived from its manifests | the lowering's two-form choices (`LoweringForms`, 17 members) and the remaining two-form hooks | 19 to 23 | A (one, then two new sessions) and O (B1-R) |
| **4. Close-out** | the optimiser switches decided; `PlanStyle` deleted; the guards and the census blocking | `PlanStyle`, `PureText` and its 12 dialect subclasses | 5 | G (B2) |
| **5. Portability** | export lite plans to legend-engine and import its plans, both checked by legend-engine running them | none | 6.5 | O (B1-R) |

**In all: about 64 to 68 session-days of work.** That is about 5 to 6 weeks of calendar time on the critical path
(phase 0, then the translator, then the close-out), with five or six sessions in parallel. The old three-mode design
was 36 days. It did not include the translator's model-level rewrite, the hosts, the SQL-shape deletions or the
judges' code; it also kept the modes, which this plan removes.

**What the user rules first (§5).** Five decisions need the user's call:
- **CP-1:** translator in and formatters out replace the three modes.
- **CP-2 (R4):** lite's own `substring` and `indexOf` follow Pure.
- **CP-3 (E-R1):** the legend-engine audience refuses exactly what legend-engine refuses.
- **CP-4:** upstream's SQL text is never promised.
- **CP-5:** the named capabilities: each is a function with its own meaning, built only on measured demand.

Five smaller ones (CP-6 to CP-10) take the recommendation unless the user objects. R-a stays with the previous
orchestrator's PR #121.

---

## 1. "Done", measurably

The program is done when all six hold, each checked by a test that runs per push:

1. **No mode in the core.** `PlanStyle`, the facets (`facet(...)`), the two-form hooks (`twoForm(...)`),
   `LoweringForms`, `PureText` and B1's `Mode` records do not exist. An ArchUnit rule forbids their return, and B1's
   `ModeBranchCensusTest` reads 0 and blocks.
2. **No upstream meaning in the product.** The census (`MeaningCensusTest`) runs with `BLOCKING = true`. Its (a) and
   (p) counts are 0; every remaining hit is a (b) row citing the user's ruling.
3. **No upstream word in a plan.** No plan the suites make carries an upstream sentence, an upstream envelope or a
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
audience. Each strand gets a destination: a formatter (P), an input rewrite over what the core already has (R0), a
rewrite calling a named capability (R1), a refusal by name (X), host-only judging (J), or deletion because only
upstream's SQL text or shape depended on it (S). When a strand's destination is built and its doors switched, its hook
is deleted. **`PlanStyle` dies when the last strand closes (slice 4.3), not before.**

Counted at `main` `b75d6c73a`:
- `PlanStyle` has 10 methods (`database/PlanStyle.java`, 173 lines) and is named by 43 product files outside
  `com.legend.edge`;
- its facet kinds are 5 (`PlanStyleFacets.KINDS`);
- its two-form hooks are 16 (`PlanStyleFacets.TWO_FORMS`; B1's S1 adds `SiteFacts`, which slice 1.2 does not keep);
- `LoweringForms` has 17 members;
- the edge is 93 files and 21,988 lines.

The handoff counts 45 parameters and 37 call sites carrying the style; slice 0.4 re-measures them as the ratchet's
start.

**This map is a first cut.** It is drawn from H1's table and from reading each hook's declaration. Slice 0.4 confirms
each row against the code and H1's TSV, and the confirmed map is the ratchet.

| Strand (today's hook) | What it does today | Class (H1 rows) | Slice |
|---|---|---|---|
| facet `RelationTexts` | a relation's `toString`, `toCSV`, a TDS's `csv`, built in SQL (`RelationTextLowering`, 791 lines) | P at a root or side; X if it feeds | 1.3 |
| facet `PureOnlyTexts` | `format`'s `%0Nd` and `%r`, `toRepresentation`, an instance's `@_` id (`EdgePureOnlyTexts`) | P; R0 for `%r` in a query (D04); K1 or K3 if it feeds | 1.3, 3.6 |
| facet `ResultEnvelopes` | legend-engine's `tdsBuilder`/`classBuilder` envelopes by the database's JSON (`EngineResultEnvelopes`) | P (H3 (c): byte-identical) | 1.4 |
| facet `UpstreamPlans` | `toSQLString`, `planToString`, the engine-text asserts | S under CP-4: the SQL text goes; legend-engine's plan FORMAT stays as an ENGINE presenter | 2.3, 1.4 |
| facet `EngineTestText` | the engine-text judge arm | J | 2.2 |
| two-form `DateTimeTexts` | a DateTime's text in a message or a print | P; K2 if it feeds | 1.3, 3.6 |
| two-form `ListTexts` | a list's `[a, b]` vs the product's JSON array | P; R0 in a query (D06) | 1.3, 3.4 |
| two-form `ExecutedValues` | legend-engine's json-builder `values` envelope | P | 1.4 |
| two-form `JsonGrammar` | json-simple 1.1.1's grammar in the judge (S144) | J | 2.2 |
| two-form `RelationCounting` | a relation counted as one value by asserts (S137) | J | 2.2 |
| two-form `CrossKindNumbers` | compiled legend-pure's Integer/Decimal-by-text (S127) | J half 2.2; R0 half 3.4 | 2.2, 3.4 |
| two-form `VerdictNumbers` | legend-pure's Float/Decimal comparison in the database judge (S15, S132, S139) | J | 2.2 |
| two-form `RenderedTextRule`, `DateLiteralTexts`, `GoldenNullWord`, `VerdictWords` | the compatibility database judge's rules and words | J (deleted with that judge, C4) | 2.2 |
| two-form `RoutedQueries`, `ActivityTexts` | legend-engine's routed-query print and activity SQL in an answer | S under CP-4 (activities carry lite's SQL); K19 only on measured demand | 2.3 |
| two-form `TestDataPlanner.Printing` | the test-data generator's legend-engine CSV printing (S191, S306) | R1 K18, or kept in the edge as a presenter; 20:10-R2 keeps it "for now" | 3.7 |
| two-form `DatabaseSettingsForm` | H2's `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH` (S368, D15) | R0: null placement by sort keys; the rest is version residue (CP-8) | 3.4 |
| `LoweringForms.raise` | the plain set: no lite check where legend-engine plants none (B7; ENGINE raise class A) | R1 K6 (the unguarded operations) | 3.6 |
| `LoweringForms.executeValues` | the JsonBuilder cardinality fold (`oneBare`) | P | 1.4 |
| `LoweringForms.assertFailed` | "Assert failed" vs lite's "expected true, got false" (20:10-R1, R10) | P | 1.2 |
| `LoweringForms.objectReferenceConnection` | a protocol constant inside an object reference (C1 row) | P, with the fact "this cell is an object reference" (S362) | 1.4 |
| `LoweringForms.isNumeric`, `parsedDecimal`, `parsedJson` | legend-engine's H2/DuckDB forms (S370, S365, S366) | R0 | 3.4 |
| `LoweringForms.relations` | a let's relation read twice (S360) | R0 | 3.4 |
| `LoweringForms.whereZones` | a connection's time zone (S183) | R0 | 3.4 |
| `LoweringForms.view`, `wireCast`, `unionFrame`, `existsJoin` | legend-engine's view nesting, wire cast, union framing (C1, LOWERSQL-1), exists-join rule (C2, LOWERSQL-4) | S if the answers are equal; R0 or X where an answer changes (rule 4) | 2.3 |
| `LoweringForms.keyedCounts`, `computedPruning` | the product's key fold and pruning switched off for the edge (S147 K16, D18 K10) | switches: X by default (CP-5) | 4.2 |
| `PlanStyle.errorWords` | a planning failure's upstream sentence | P (a kind and operands, worded after) | 1.2 |
| `PlanStyle.traceCell` | legend-engine's trace comment around the execution id, written into SQL | S (SQL text) | 2.3 |
| `PlanStyle.graphKeyDate`, `graphKeyString` | a key's literal inside the database-built graph JSON (C2 row) | P, or S where the key is an internal shape (0.4 decides) | 1.4 |
| `PlanStyle.judgesEngineText` (`EngineTextGates`) | "is this the edge" in the product test planner (JUDGES-6, 13 hits) | J: deleted | 2.2 |
| `PlanStyle.dialect(type)` (`PureText`'s 12 subclasses: spelling passes, `floatToText`, `raiseMessage`, `floatLiteralKind`, `raw`, `scanOrderPasses`, `refusesDecimalArithmetic`, `dateTimeFormat`) | the edge's dialect wrapper | each pass to its strand: prints P/K1/K2, `raiseMessage` P, `floatLiteralKind` K5, `scanOrderPasses` J (S14; K7 only by ruling), the rest per 0.4 | 4.3 (`dialect` becomes `Databases.dialect`) |
| `edge.StableScanOrder` (598 lines, a SQL rewrite of the plan, test-only) | legend-engine's H2 scan order for unordered corpus asserts (S14, D14) | J: the compatibility host judge compares an unordered actual as a multiset (S131's rule) | 2.2 |
| The engine-text printers `EngineStyleH2` (2,507), `EngineStyleDB2` (365), `EngineStyleComposite` (79); `RelationalRootForm` (N-ROOTFORM) | legend-engine's SQL text formatting and root form | S under CP-4 | 2.3 |
| The census's word groups (C05, C08, C10, C11, C14-C17, C19; 24 hits) | an upstream sentence inside the product | P | 1.2 |
| The Cypher translator's `substring` (`CypherToPure.java`, H3 item 5); the `###Relational` dynaFunction `substring` sharing the Pure function's rule (H3 item 6) | the product's own two dependencies on today's meaning | split from the Pure function first | 3.1 |

---

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

### Phase 0: rulings, the net, the measurements (about 4.5 days)

**0.1 The rulings.** The user rules CP-1 to CP-10 (§5). The slices that wait on each are named there. Phase 1 and
slice 2.2 need only CP-1 and CP-7. **Size:** 0. **Owner:** the user, with this session.

**0.2 The net lands** (B1's S0, rebased on `main`).
- **Changes:** what S0 changed: test code, tools and goldens only. Also: `//pct:edge_net` and `EdgeCasesGoldenTest`
  join the per-push lanes (CP-9; FOLLOW_UPS §8's C1).
- **Deletes:** nothing.
- **Check:** S0's planted-change proof (10 cases). The re-recorded goldens map back to the old ones line for line
  (0 answer moves).
- **Guards:** G1, G3, G6 report-only.
- **Size:** 1 day (rebase, re-record, PR).
- **Depends on:** nothing. **Owner:** B1.

**0.3 The ENGINE PCT lane** (the old S0e, report-only).
- **Changes:** PCT channel A runs as the ENGINE audience on DuckDB's 5 suites and H2's relation suite. Each answer is
  compared with legend-engine's own manifests (`@legend_engine_src`'s `pct-manifests`, spec input only). The lane
  writes, per test: legend-engine passes, refuses ("No SQL translation exists ..."), or fails by defect, and what lite
  answers. The E-R1 refusal table is derived from the "No SQL translation" entries, per database, never by hand.
- **Deletes:** nothing.
- **Check:** the lane runs; its differences from the manifests are the baseline for phase 3.
- **Guards:** the derived table pinned, regenerated by a diff test.
- **Size:** 1.5 days.
- **Depends on:** 0.2. **Owner:** B1-R.

**0.4 The measurements.** Each is a number one or more later slices or rulings need:
- (a) **The strand map confirmed** (§3): every hook, facet, two-form and `LoweringForms` member, its class and its
  slice; `PlanStyle`'s parameters and call sites counted. This becomes the program's ratchet: a shrink-only count
  that ends at 0.
- (b) **CP-4's cost:** how many corpus tests pass today only through legend-engine's SQL text (`toSQLString`,
  `assertSqlEquals`, `planToString` and the test-data generator's `sqls`). Measured by running the corpus with the
  engine-text printers made to refuse.
- (c) **K6's family:** the guarded natives that would need an unguarded twin for legend-engine's raise class A.
- (d) **The strands' doors:** which door reaches each strand (pure/v1, the corpus, PCT A, PCT B, the hosts' edge
  routes, the test-data generator).
- **Deletes:** nothing. **Check:** each number reproducible from a committed probe.
- **Size:** 1.5 days.
- **Depends on:** 0.2. **Owner:** B2, with this session.

**0.5 Register housekeeping.**
- **Changes:** S138 corrected (compiled legend-pure raises on `acos(2.0)`, `CoreHelper.java:141-148`), S63 recited
  (the live refusal is `pureToSQLQuery.pure:10056`), the prelude's register row (the user's 2026-11-19 ruling: the
  prelude and the parser stay), and the two unrecorded differences H1 found: legend-engine's H2 `parseJson` UDF, and
  S110's DuckDB residual.
- **Check:** `RegisterRowsTest` and the census's (b) rows citing them.
- **Size:** 0.5 day.
- **Depends on:** CP-10. **Owner:** this session.

### Phase 1: formatters on the way out (about 19 days; tracks P and H)

The proven half (H3 (c): legend-engine's TDS envelope byte-identical from a presenter after the run, 0 product
lines). It comes first because it is the lowest risk and deletes the most text-building code. It also defines the
error record the translator's refusals are worded through.

**1.1 The presenter seam.**
- **Changes:**
  - one `Presenter` per audience: LITE's in the product library (today's writers: `result.HostRows`,
    `LiteV1Planning.Answered`, `ArrowWriter`); ENGINE's and PURE's in `com.legend.edge` (`PureV1Writer`,
    `EngineAnswers`, `HostAnswers`, `RaisedWords`);
  - each presenter reads three inputs and nothing else: the typed result stream, the plan, and the typed query's
    facts (H3 (c)'s finding: `activities[].sql` comes from the plan, `relationalType` from the result type);
  - a presentation instruction (for example "print this root as `#TDS` text") is held by the edge's door and handed
    to the presenter; it is never written into the plan;
  - the typed facts CP-6 rules in are added to the plan's column record.
- **Deletes:** nothing yet.
- **Check:** pure/v1's net byte-identical; the oracle's answers unchanged.
- **Guards:** a new ArchUnit rule: a presenter class depends only on `result`, `executionplan`, `error`, `values`
  and `json`; it never reaches `exec`, `planner`, `lowering` or `java.sql`.
- **Size:** 2 days.
- **Depends on:** CP-1, CP-6. **Owner:** B1.

**1.2 Error records: kind, place and literal operands; words after the run** (re-cuts B1's S1, plan v59).
- **Changes:**
  - a raise site carries its kind, its place and its literal operands as typed data (A3: never a run-time value);
  - a planning failure carries a kind and its operands, replacing `PlanStyle.errorWords`
    (`Failure(kind, operands)`);
  - each audience's presenter words them: LITE's sentence by lite's presenter, legend-pure's by the PURE presenter
    (exact over a literal operand, without the value over a computed one, under one register row), legend-engine's by
    the ENGINE presenter;
  - S1's deletions land: the recorder and `PlanStyle.siteWords`;
  - the six in-body guards' LITE words (R10);
  - "Assert failed" leaves the product (`LoweringForms.assertFailed`, 20:10-R1);
  - lite's own sentence stays in the plan's `sites[].message` until slice 1.6 makes the hosts word through Java
    (CP-7); upstream words never enter the plan.
- **Deletes:** `PureMessages.sentence` (the SQL half, 57 lines; 93 SQL code lines in the file),
  `SiteWordsRecorder` (124), `PlanStyle.siteWords` and `errorWords`, `LoweringForms.assertFailed`; the census's 9
  word groups (24 hits) leave (a).
- **Check:** the net's messages unchanged (PCT channel A's `ERROR` lines, channel B's ledger substrings, pure/v1's 5
  error bodies, the corpus's messages); the product plan's JSON byte-identical but its version; literal-operand raises
  checked on the tab and Python.
- **Guards:** a plan-scan rule: no upstream sentence in any plan the suites make (done-when 3, report-only until 4.3);
  G7's computed-operand count pinned (ENGINE at 0).
- **Size:** 3 days.
- **Depends on:** 1.1, CP-7. **Owner:** B1.

**1.3 Value and relation text, at roots and sides, in Java.**
- **Changes:**
  - legend-pure's prints (a relation's `#TDS` text, `toCSV`, a TDS's `csv`, a list's `[a, b]`, a DateTime's print,
    `format`'s `%0Nd` and `%r`, `toRepresentation`, an instance's id) are written by the PURE and ENGINE presenters
    over typed rows;
  - the edge's door rewrites a root `toString` (or `toCSV`) into "answer the value; present it as text";
  - a judge side's print is the compatibility host judge's (rule 9);
  - a print that FEEDS a further computation inside the query is refused by name until its capability exists
    (phase 3). H2 counted it: 0 upstream sites over Float, Decimal or dates; 14 over Integer or an enum, which R0
    covers;
  - a Pair is its JSON object in LITE and `<a, b>` in PURE and ENGINE (R5). The prelude's print bodies stay (the
    2026-11-19 ruling); 1.3 checks whether LITE ever reaches one.
- **Deletes:** `RelationTextLowering` (791), `EdgePureOnlyTexts` (136), `PureListTexts` (49), `PureDateTimePrint`
  (43), `EdgeDateTimeTexts` (28), the print half of `PurePrintForms` (about 150 of 184); facets `RelationTexts`,
  `PureOnlyTexts`; two-forms `DateTimeTexts`, `ListTexts`. Q5 (`toCSV`'s DateTime) and Q6 (`%r` and a newline) are
  fixed in Java as part of it.
- **Check:** PCT channels A and B in PURE move only toward PCT's expected values; `EdgeCasesGoldenTest` (EDGE-1's
  Float values, EDGE-2, EDGE-3) re-pinned to upstream's answers; the corpus rosters unchanged; every refused feeding
  case listed (expected: none in PCT or the corpus beyond H2's 14 Integer and enum sites).
- **Guards:** G1 (text-producing MIR built by the edge) shrinks; the census's M3 rows for these hooks leave (a).
- **Size:** 4 days.
- **Depends on:** 1.1. **Owner:** B1.

**1.4 Envelopes.**
- **Changes:**
  - legend-engine's `tdsBuilder`, `classBuilder` and json-builder `values` envelopes, the cardinality fold, the
    object-reference constant and the graph keys are written by the ENGINE presenter over typed rows;
  - legend-engine's plan FORMAT (generatePlan's `SingleExecutionPlan`, the plan-text family's envelope) becomes an
    ENGINE presenter carrying lite's own SQL (CP-4);
  - envelope fields that are a function of upstream's SQL (the scalar envelope's `COUNT(*)` label, O3; a database's
    error text over upstream's statement, H3 (c) item 3) carry lite's own value in upstream's envelope, under one
    register row (CP-4).
- **Deletes:** `EngineResultEnvelopes` (92), `EngineExecutedValues` (44), `EdgeLoweringForms.oneBare`; facet
  `ResultEnvelopes`; two-form `ExecutedValues`; `LoweringForms.executeValues` and `objectReferenceConnection`;
  `PlanStyle.graphKeyDate`/`graphKeyString` where 0.4 finds them P. Q7 (EDGE-8, rows aggregated with no ORDER BY)
  goes with them.
- **Check:** the corpus rosters; pure/v1's net; the oracle's answers; EDGE-8's ordered probe; the scalar `value`
  request against legend-engine's own answer (the oracle).
- **Guards:** the census's envelope M1 hits leave (a).
- **Size:** 2 days.
- **Depends on:** 1.1, CP-4. **Owner:** B1.

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
  - a raise's words on the tab and in Python come from the Java presenter (exec's readers return a record: kind and
    site), so lite's sentence leaves the plan (CP-7, plan v60);
  - exec's own readers stay per language (H4's B, honestly used): P3's three stale Python readers fixed, HOSTS-11's
    two inputs and HOSTS-10's three divergences in the shared cases.
- **Deletes:** `_carried_float`, `_lite_cell`, `_lite_rows_json` (about 60 lines); Python's ENGINE framing; the plan's
  `sites[].message` and `raises` sentence fields; C3's and HOSTS-4's Python halves.
- **Check:** byte parity across the three hosts, Arrow included; the differential per new export (JVM vs WASM vs
  native).
- **Guards:** H4 §8 (1), the host meaning lens (census-b2's `HostSources`), shrink-only to 0, with its planted
  TypeScript and Python examples; (3), the differentials.
- **Size:** 3.5 days.
- **Depends on:** 1.2, 1.5. **Owner:** the host session.

**1.7 DataCube's legend-engine workarounds to its ENGINE plane** (DC1, 20:10-R6).
- **Changes:** the four withheld boolean operators, the `__all__` partition and the `__root__` group column move into
  `datacube/src/edge/`; lite's plane sends the product's forms.
- **Deletes:** C6 from the product.
- **Check:** DataCube's tests on both planes; the ENGINE plane's queries identical to today's; lite's plane's answers
  unchanged.
- **Size:** 2 days, in parallel.
- **Depends on:** nothing. **Owner:** the host session.

### Phase 2: deletions that need no replacement (about 10 days; tracks J and G)

**2.1 The 36 combinations ported into core** (C4, part 1).
- **Changes:** the 36 judge shapes the compatibility database judge covers are ported as core LITE tests under both
  judges (BOTH), so parity (G2) is proven in the product without PCT's database judge.
- **Deletes:** nothing yet.
- **Check:** core's isolated BOTH green on the ported shapes.
- **Size:** 2 days.
- **Depends on:** CP-1. **Owner:** a judges session (new). **Coordination:** the previous orchestrator's A5 (Q3, Q3b,
  J1-J4) touches the same judges. The two sessions agree by message before either builds.

**2.2 Compatibility judging becomes host-only** (C4, part 2; rule 9).
- **Changes:**
  - legend-pure's comparison rules and words for PURE and ENGINE live only in the edge's host judge: compiled
    legend-pure's `eq` (S15), the multiset compare of an unordered actual (S131, which retires `StableScanOrder`'s
    reason, S14 and D14), the 2-ULP Float rule (S132), json-simple's grammar (S144, S366), a relation counted as one
    value (S137), the failure words (S195);
  - PCT runs HOST-only for compatibility judging; LITE keeps both judges with parity, and its exceptions go
    (JUDGES-3: `IsolatedRun#difference`).
- **Deletes:**
  - `VerdictSql`'s two-form branches (the census's N-JUDGE, 38 hits);
  - `UpstreamVerdictWords` (198), `CompiledPureVerdictNumbers` (54);
  - `EngineTextGates` (110) and `PlanStyle.judgesEngineText` (JUDGES-6, 13 hits);
  - facet `EngineTestText`;
  - two-forms `JsonGrammar`, `RelationCounting`, `VerdictNumbers`, `RenderedTextRule`, `DateLiteralTexts`,
    `GoldenNullWord`, `VerdictWords` (the edge's form), and `CrossKindNumbers`' judge half;
  - `edge.StableScanOrder` (598) and `sql.ScanOrder`'s edge use;
  - the four residue pins ratified at 20:10-R3 shrink (`DatabaseJudge` 851, `HostJudgments` 679, `BodyDriver` 545,
    `EngineTextPlanning` 151), each re-pinned with the user's word and a dated note.
- **Check:**
  - channel B under HOST unchanged;
  - channel A's PURE and ENGINE outcomes move only toward their oracles;
  - the corpus rosters unchanged, except unordered-assert lines that now pass as multisets (listed);
  - core's BOTH green.
- **Guards:** the census's 52 J hits leave (a); `JavaEvalLedgerTest`'s pins shrink.
- **Size:** 4 days.
- **Depends on:** 2.1. **Owner:** the judges session.

**2.3 Upstream's SQL shapes go, checked by answers** (rule 4, CP-4).
- **Changes:**
  - lite's own SQL everywhere an answer carries SQL text (`toSQLString`, `planToString`, `activities[].sql`,
    generatePlan's `sqlQuery`, the test-data generator's `sqls`);
  - corpus tests that judge upstream's SQL text form a listed "not promised" class: counted, never silently dropped;
  - each shape-only hook is deleted where the answers are equal; where an answer changes, the strand becomes an R0
    rewrite or an X refusal (rule 4's exception), named in the PR.
- **Deletes:**
  - `EngineStyleH2` (2,507), `EngineStyleDB2` (365), `EngineStyleComposite` (79);
  - `RelationalRootForm` (N-ROOTFORM);
  - the SQL spellings of the plan-text family (`EnginePlans`, `PlanText`, `PlanEnumForm`, `InProtocol`,
    `PlanVariables`; its plan FORMAT stays as 1.4's presenter);
  - `LoweringForms.view`, `wireCast`, `unionFrame` (C1), `existsJoin` (C2);
  - `PlanStyle.traceCell`;
  - two-forms `ActivityTexts`, `RoutedQueries` (unless CP-5's measured demand keeps K19);
  - facet `UpstreamPlans`' SQL half.
- **Check:**
  - the corpus on H2 and DuckDB: every test that passed by answer still passes;
  - the "not promised" class equals 0.4 (b)'s count;
  - pure/v1's net, with `activities[].sql` lite's own (moved lines listed);
  - the lean-SQL baseline (`lean-sql-baseline.tsv`) only shrinks.
- **Guards:** the census's SQL-shape hits leave (a); `DialectLeakRatchetTest` and the edge's line count shrink.
- **Size:** 4 days.
- **Depends on:** CP-4, 0.4 (b), 1.4. **Owner:** B2, or the judges session after 2.2.

### Phase 3: the translator on the way in (about 19 to 23 days; tracks A and O)

H3 (a) and (b) proved the pattern and found three conditions, which shape this phase:
- **the rewrite must know the target database** (legend-engine's meaning is its database's: target-blind, DuckDB
  scored 17 of 25; target-aware, 24 of 25);
- **the ENGINE audience is whole models**, not request lambdas (the corpus's 15 newly failing tests);
- **a capability must be one SQL node** each dialect writes once (as a lowering tree it cost 350 times the SQL).

**3.1 The product's own two dependencies on today's meaning, split first** (H3 items 5, 6).
- **Changes:**
  - lite's Cypher translator writes Pure in the core's meaning, not legend-engine's 1-based `substring`
    (`CypherToPure.java`);
  - the `###Relational` dynaFunction `substring` gets its own rule (SQL's 1-based function, upstream's DSL meaning in
    every audience), separate from the Pure function's.
- **Deletes:** the shared rule.
- **Check:** `CypherIslandIntegrationTest`, the DynaFunction property-mapping tests, the corpus's `propertyfunc` tests
  unchanged.
- **Size:** 1.5 days.
- **Depends on:** nothing (it is right under either R4 answer). **Owner:** the translator session (new).

**3.2 The translator's skeleton: model-level and target-aware.**
- **Changes:**
  - **one translator per upstream audience** at the edge's doors: pure/v1, the corpus, PCT channel A (ENGINE and
    PURE), PCT channel B (PURE), the test-data generator, PURE's direct entry;
  - **its input:** the parsed model, the query, and the target database read from the request's runtime (knowledge,
    not a mode);
  - **its output:** a lite model and query, or a refusal by name worded by 1.2's presenter;
  - **rewrites are lite programs:** an edge-owned compatibility library of Pure source per audience and target (H3's
    `compat-engine-<target>.pure`), plus typed call retargeting over the parsed model. A JSON rename is not enough: H3
    found it untyped and refusing by the wrong name;
  - **upstream's flags are the translator's input,** never the core's: S130's `CORRECT_SQL_SUBSTRING_INDEXING`
    first;
  - **the translator reaches the core only through the public doors** (`Compiler.query`, `TypedQuery`).
- **Deletes:** `FeatureRules`' flag-keyed substring choice from the core (S130's flag moves).
- **Check:** every door's suite byte-identical with the translator in place and no rewrite registered (a pass-through
  first).
- **Guards:**
  - ArchUnit: the translator package reaches the planner only by `ArchitectureTest.PLANNER_ENTRIES_ONLY`'s public
    entries;
  - the translator evaluates nothing: no `values` arithmetic, rewrites only;
  - the compatibility library's Pure sources are edge resources, refused by the product loader
    (`LibraryPlatformNamespaceGuardTest`'s pattern).
- **Size:** 3 days.
- **Depends on:** CP-1. Its pass-through starts at once; its refusals are worded through 1.2 once that lands. **Owner:**
  the translator session.

**3.3 R4 lands.** Under CP-2's recommendation (Pure):
- **Changes:**
  - the core's `substring` is always Pure's (0-based, end-exclusive), and its `indexOf` is 0-based with -1 on a miss
    (H3's Scalars +21/-26);
  - the ENGINE translator rewrites both per target (H3's target-aware library);
  - the 15 corpus tests run through the model-level rewrite;
  - the core's tests that pin the old meaning move, listed;
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
    null placement by sort keys (S368, D15), a let's relation read twice (S360), a connection's time zone (S183), a
    list's text in a query (D06), `%r` (D04), cross-kind numbers (S127's product half);
  - batch 2, the rest: dispatch by legend-engine's registration order (S3), `average([])` (S19), a class mapping with
    no `~src` (S21), the precise primitives' constraints (S140), the pivot column names (S107), Legend SQL's rows
    (S43, S44, S46, S47), the protocol's lenient reading (S29), and the other R0 rows 0.4 assigns.
- **Deletes:** `LoweringForms.isNumeric`, `parsedDecimal`, `parsedJson`, `relations`, `whereZones`; two-form
  `DatabaseSettingsForm`'s null-order half (its other settings recorded as version residue, CP-8);
  `CrossKindNumbers`' product half.
- **Check:** the ENGINE PCT lane (0.3) and the corpus move only toward the manifests and rosters; the oracle on each
  rewrite's battery; PURE's lanes toward PCT's expected values.
- **Guards:** the census's matching M3 rows leave (a); the strand ratchet shrinks.
- **Size:** 5 days (3 + 2).
- **Depends on:** 3.2. **Owner:** the translator session; B1-R writes each battery.

**3.5 legend-engine's refusals** (E-R1, CP-3).
- **Changes:**
  - the ENGINE translator refuses exactly what legend-engine refuses at planning, per database, by 0.3's derived
    table, worded in legend-engine's words by the presenter;
  - where legend-engine fails by its own defect, lite answers correctly, and each case is a register row counted from
    the manifests;
  - the 17 "upstream-refuses" register rows and 4 ENGINE cells of H1 become refusals; S1 (whole-model compile) uses
    `Compiler.compileErrors`.
- **Deletes:** nothing.
- **Check:** the ENGINE PCT lane: lite passes where legend-engine passes, refuses in its words where it refuses, and
  answers its defects as ledgered rows.
- **Guards:** the table regenerated by its diff test; growth only by the manifests.
- **Size:** 1.5 days.
- **Depends on:** CP-3, 0.3, 3.2. **Owner:** the translator session.

**3.6 The capabilities, tier 1** (CP-5).
- **Changes:** each is a Pure-callable native with its own meaning, lowered as ONE MIR node with one render arm per
  dialect (no `default ->`), called only by the translator:
  - **K1** `pureFloatText`: legend-pure's print of a Float (H3 (b): H2 26 of 26, DuckDB 25 of 26);
  - **K2** `pureDateTimeText(x, precision)`;
  - **K4** `databaseCast(x, relationalType)`: legend-engine's `cast(%s as varchar)`, `as integer`, `as float`;
  - **K5** `bareNumericLiteral`: already built for S20; re-homed as a native, the style's `floatLiteralKind` deleted;
  - **K6**, the unguarded operations: legend-engine's raise class A, so pure/v1's `sqrt(-2.0)` answers as
    legend-engine does (H3 (c)); its family is sized by 0.4 (c).
- **Deletes:** `LoweringForms.raise`'s plain set; `PureText`'s `floatToText`, `floatLiteralKind` and the print
  spelling passes; the K-rows' current edge hooks.
- **Check:**
  - K1 and K2 against legend-pure's own jar (H3's `PurePrint` probe, permanent) and PCT's `toString` tests;
  - K4 and K6 against the oracle on H2 and DuckDB;
  - each capability's SQL size measured: no answer's SQL grows past 2 times for one call (G3).
- **Guards:**
  - each native listed in a new verified set, `COMPAT_CAPABILITIES`, in `NativeCatalogGovernanceTest` (a ceiling
    move: the user's word, CP-5);
  - an ArchUnit and census rule: no product code path emits a call to a compatibility capability; only the
    translator's library does;
  - a planted product caller must fail.
- **Size:** 5 days.
- **Depends on:** CP-5, 3.2, 0.4 (c). **Owner:** the translator session.

**3.7 The capabilities, tier 2, on measured demand only.**
- **Changes:** K3 (`format`'s `%.Nf`), K8 (`splitPart` by character set), K9 (a column as stored), K14 (Decimal
  equality by scale), K17 (the strict Integer cell), K18 (legend-engine's test-data printing, today a mode behind
  `withEngineCompat`), K19 (the routed set). Each is built only if a PCT lane, the corpus or the oracle shows a case it
  fixes. Otherwise it is a refusal by name with a register row.
- **Deletes:** two-form `TestDataPlanner.Printing` (K18 built, or refused) and `ExecuteOptions.withEngineCompat`.
- **Check:** as 3.6, per capability.
- **Size:** 0 to 4 days, by the demand found.
- **Depends on:** 3.6. **Owner:** the translator session.

**3.8 PURE's direct entry** (the user's 2026-11-18 19:45 ruling "planning mode/direct entry for now").
- **Changes:** one Java door, `edge.PureEntry`, that runs the PURE translator and the PURE presenter over a model and
  a lambda.
- **Check:** PURE tests on DuckDB, H2 and SQLite.
- **Size:** 0.5 day.
- **Depends on:** 3.2, 1.3. **Owner:** the translator session.

### Phase 4: the close-out (about 5 days; track G)

**4.1 The optimiser switches** (K7, K10, K12, K13, K15, K16, CP-5's tier 3).
- **Changes:**
  - the product's key fold (S147), pruning (D18), let splice (S129), savepoint behaviour (S135) and scan order
    (S14, now J) stay as the product's meaning;
  - the upstream audience is refused by name where it would observe the difference, with a register row each;
  - K15 (H2 2.1.214's division scale) is not in the pinned trees, so its row is X.
- **Deletes:** `LoweringForms.keyedCounts`, `computedPruning`.
- **Check:** the corpus and PCT lanes: every newly refused case listed; the expected count is small (the corpus's
  `KeyedCounts` cases, measured in 0.4).
- **Size:** 1.5 days.
- **Depends on:** CP-5, phase 3's tier-2 decisions. **Owner:** B2.

**4.2 `PlanStyle` deleted; the census blocking.**
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
  the user's word, and each slice's PR states its delta. It blocks at 4.2.
- **The strand ratchet** (0.4 (a)) shrinks with every strand closed.
- **The per-push lanes** (CP-9): the edge net, the ENGINE PCT lane, the SQLite PCT recording, the host byte-parity
  suite. The oracle test stays in `//gates:heavy`, and every program PR runs it.
- **The register** gets a row for every ledgered difference a slice creates, posted to the user before it lands
  (the upstream-fidelity tenet).

---

## 5. The rulings

Each ruling below gives the question, the options, the recommendation and what moves under it. The first five need
the user's call; CP-6 to CP-10 take the recommendation unless the user objects.

**CP-1. Translator in, formatters out, instead of three modes.** *Gates every phase.*
- **The question:** replace the three modes (ruled 2026-11-18 19:45) with this design? The core has one meaning; an
  upstream program is translated on the way in, and its answer formatted on the way out.
- **Recommendation: yes.** H1 classed all 281 known differences without a mode. H3 showed each half working against
  upstream itself. A mode is a parameter the whole core must carry, and today that costs 43 product files.
- **What moves:** B1's `Mode` records and S1's words-in-the-plan are withdrawn (§2). The rulings about meaning and
  words stand.

**CP-2. R4: lite's own `substring` and `indexOf`.** *Gates 3.3.*
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

**CP-3. (e′) E-R1: what the legend-engine audience refuses.** *Gates 3.5.*
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

**CP-4. Upstream's SQL text is never promised.** *Gates 1.4 and 2.3.*
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

**CP-5. The named capabilities.** *Gates 3.6, 3.7 and 4.1.*
- **The question:** some upstream answers need a small function the core does not have, such as "print a Float the
  way legend-pure does" (H1 named 18, plus K7).
- **Recommendation:**
  - each capability is a Pure-callable native with its own meaning, written as ONE SQL node, called only by the
    translator, and listed in a new pinned set `COMPAT_CAPABILITIES` (moving `NativeCatalogGovernanceTest`'s verified
    sets needs the user's word);
  - **tier 1 is built** (K1, K2, K4, K5, K6): PCT and pure/v1 need them today;
  - **tier 2 only on measured demand** (K3, K8, K9, K14, K17, K18, K19);
  - **tier 3 is never built:** the four that switch a product optimisation off (K10 pruning, K12 let splice, K16 key
    fold, K7 scan order), plus K11, K13 and K15 (whose rule is not in the pinned trees). The upstream audience is
    refused by name there, with a register row each.
- **Why:** H2 found upstream programs almost never build text inside a query and use it (0 Float, Decimal or date
  sites); the demand is PCT's unit tests. A switch built as "turn the optimiser off" would be a mode by another name.

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
- **Lite's own sentence leaves the plan in 1.6,** once the tab and Python word errors through the Java presenter. Until
  then it stays, as K1 ruled.
- **Why:** architecture rule 2, and H4's finding that a rule held in two places drifts.

**CP-8. legend-engine's database versions are recorded, not chased.**
- legend-engine runs a patched H2 2.1.214 (`MODE=LEGACY`) and DuckDB 1.3.0; lite runs H2 2.5.252 and DuckDB 1.5.6.
- **Recommendation:** where an answer differs only for that reason (`cell.float`'s 8 digits, O5; a Decimal division's
  scale, S141; legend-engine's H2 UDFs), it is one family of register rows. The oracle measures a case when one is
  reported. Lite does not emulate old database versions.

**CP-9. (d′) The per-push lanes.**
- **Add to every push:** the edge net, the ENGINE PCT lane (DuckDB's 5 suites and H2's relation suite), the SQLite
  PCT recording, and the host byte-parity suite.
- **Cost:** about +2 to +4 minutes per push, by the old design's figures; 0.2 and 0.3 re-measure it.
- **The oracle stays in the heavy lane,** and every program PR runs it.

**CP-10. Housekeeping.**
- The register corrections S138 and S63, the prelude's register row, and H1's two unrecorded differences (0.5).
- B1's three pending guard items are withdrawn, because the extension they guard is not built (§2).

**Held by the previous orchestrator:** R-a, a fractional cell under an Integer property (PR #121). Its ENGINE
answer, when ruled, becomes a translator rewrite or a refusal in phase 3.

---

## 6. The guards for the end state

| Guard | What it proves | Lands | Blocks |
|---|---|---|---|
| "No mode in the core" (ArchUnit) + `ModeBranchCensusTest` (B1's G6) + the strand ratchet | done-when 1 | 0.2 report-only; the ratchet from 0.4 | 4.2 |
| The census, `MeaningCensusTest` | done-when 2 | today, report-only; shrink-only pin from 0.4 | 4.2 |
| The plan scan (no upstream sentence, envelope or presentation instruction in a plan) | done-when 3 | 1.2, report-only | 4.2 |
| The presenter rule (a presenter reaches only records, never exec, the planner or JDBC) | rule 3: presentation after the run | 1.1 | 1.1 |
| The host meaning lens, the cross-host byte-parity suite, one differential per export (H4 §8) | done-when 4 | 1.5, 1.6 | 1.6 |
| The translator rule (public doors only; rewrites, never evaluation; its library refused by the product loader) | rule 5: the core never knows | 3.2 | 3.2 |
| `COMPAT_CAPABILITIES` + "no product caller of a capability" (with a planted caller) | a capability never becomes a mode | 3.6 | 3.6 |
| G7, the computed-operand count (ENGINE at 0) | A3 | 1.2 | 1.2 |
| The per-audience lanes, each moving only toward its oracle | done-when 5 | 0.2, 0.3 | CP-9 |

Every guard ships with a planted violator it must catch. No guard depends on a clock.

---

## 7. The risks

| Risk | What it would look like | How it is caught |
|---|---|---|
| **The translator grows into a second compiler** | it starts typing, evaluating or planning on its own | it reaches the core only through the public doors; it rewrites and refuses, never evaluates (3.2's guard); a case with no exact rewrite is refused by name, not approximated |
| **A capability becomes a mode** | a native that means "switch the optimiser off for this subtree" | CP-5's tiers (no switch is built); one SQL node each; the no-product-caller rule; the pinned list |
| **An answer moves silently while a hook moves** | an upstream audience loses an answer it had | the net (0.2), the oracle, the per-audience lanes; each slice proves its lanes moved only toward their oracle, every move listed |
| **SQL size** (G3) | a capability as a lowering tree (H3: 350 times the SQL) | one MIR node per capability; the lean-SQL baseline only shrinks; a 2-times ceiling per call in 3.6's check |
| **legend-engine's database versions** | answers differ because legend-engine runs H2 2.1.214 LEGACY and DuckDB 1.3.0 | CP-8: one family of register rows, measured by the oracle on demand, not emulated |
| **The corpus's SQL-text tests** | CP-4 turns many passes into "not promised" | 0.4 (b) counts them before 2.3 lands, and the number goes to the user |
| **Host cost** | rows crossing into Java cost the tab about 5 µs a row (H4); the tab's module doubled since September | presenters ride the modules each host already loads; Arrow answers cross no rows (C2); module size reported per host slice |
| **Rulings arrive late** | slices wait | phase 1 and 2.1-2.2 need only CP-1 and CP-7; the translator's skeleton (3.1, 3.2) needs only CP-1 |
| **Collision with the previous orchestrator's tracks** | the judges (A5: Q3, Q3b, J1-J4) or Float literals (Q1, beside K5) change under a slice | §9: a message before either side builds |
| **Merge latency** | about 20 PRs each waiting for the user's word | slices grouped into about 12 PRs (§8); each PR summarised in plain words for the merge decision |

---

## 8. Sessions, tracks, calendar, landings

| Track | Session | Slices | Calendar (weeks from ratification) |
|---|---|---|---|
| P, formatters | B1 (`session_0195rQEnAa5CYXHygAxEFh3Y`) | 0.2, 1.1-1.4 | weeks 1-3 |
| H, hosts | new host session (xhigh) | 1.5-1.7 | weeks 2-4 |
| J, judges | new judges session (xhigh) | 2.1, 2.2 (then 2.3 if B2 is busy) | weeks 1-2 |
| A, translator | new translator session (xhigh); a second one from 3.3, so the rewrites (3.3, 3.4) and the capabilities and refusals (3.5-3.7) run in parallel | 3.1-3.8 | weeks 1-5 |
| O, oracle | B1-R (`session_0195rQEkcevkNQ66fixkk4FF`) | 0.3, the batteries for phase 3, 5.1, 5.2 | weeks 1-6 |
| G, guards and close-out | B2 (`session_0195rQEjuF2imym274M9Qvva`) | 0.4, 2.3, 4.1, 4.2; B3's items as they fall into slices | weeks 1-6 |
| orchestration | this session | rulings, shape notes, reviews, PRs, the user | throughout |

**The critical path:** phase 0 (about 2.5 days, its slices in parallel), then 3.1 and 3.2 (4.5), then the longer of
the two translator lanes (3.3 + 3.4, 7.5; 3.5 + 3.6, 6.5), then 3.7 (0 to 4), then 4.1 and 4.2 (5). That is about
20 to 24 session-days, so about 5 to 6 weeks of calendar time with lane rounds, reviews and merge waits.

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
| 12+ | 3.4-3.8 in two or three PRs; then 4.1 + 4.2; then 5.1 + 5.2 |

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
| Q1, Float literals and Decimal scale on five databases | K5 (`bareNumericLiteral`, S20) | K5's re-homing in 3.6 waits for Q1's shape |
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
- `LoweringForms`: its 17 members read from `core/src/main/java/com/legend/lowering/LoweringForms.java`.
- The edge: `cat core/src/main/java/com/legend/edge/*.java | wc -l` (21,988 lines; 93 files).
- The files' sizes: `wc -l` on each file named (§4).
- The verified native sets: `core/src/test/java/com/legend/builtin/NativeCatalogGovernanceTest.java`
  (`everyLiteInternalNativeIsNamedAndVerified`, `theVerifiedSetsOnlyShrink`).
