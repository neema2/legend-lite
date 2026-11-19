# The compatibility program: the final plan (2026-11-19)

> **Governed by `compat-rules-of-record-2026-11-19.md`** (the user's four rules of ~15:20 UTC: the database computes
> everything, compiler folds for constant String and Boolean only; product presentation only serializes; compatibility
> presentation fixes upstream's output in Java from lite's typed values only; the host judge is the only judge in
> compatibility, both judges in the product). Checked against them at ~15:25: every step below holds. The ~15:10
> withdrawal of part 3 and of A4 was this session's misreading and is undone; the one rule-breaking call (the AdjustBy
> date literal as a constant) stays reversed.

Written fresh after phase 0, once the user had ruled the goal, D1 and D2 (~13:25 UTC: "Okay agree with all"). It
replaces the working plan (`compat-program-plan-2026-11-19.md`, history now) as the thing to build from.
- **The evidence:** `compat-design-and-plan-plain-2026-11-19.md` (the design, what we lose, every measurement in plain
  English) and `compat-program-log-2026-11-19.md` (every ruling verbatim, every finding, every session), both on
  `claude/compat-program`.
- **Status:** revised after the independent reviewer's check (`compat-final-plan-review-2026-11-19.md` on
  `claude/compat-review` at `fd88b9c8`: 5 must-change, 4 should-change, 3 minor, all folded in below), for the user to
  ratify **stage A**. Stage B is ratified separately, after stage A's stop.

## 1. The goal (ruled)

**Lite follows Pure. legend-engine's formats are matched, and its answers where they agree with Pure. The rest is
recorded.**

So, at the end:
- lite's core (compiler, planner, plan, exec) means one thing, Pure's definitions, and holds no mode, no switch and no
  upstream-specific code. A guard keeps it that way;
- everything that serves legend-engine's and legend-pure's clients lives at the edges: the door that reads their
  input, and the formatters that write their output after the run;
- every place lite's answer differs from legend-engine's is a register row, listed for the user before it lands, and
  collected on one "differences from legend-engine" page.

**The rulebook change.** AGENTS.md's upstream-fidelity tenet and G8 of `docs/GOALS_OF_RECORD_2026_11_07.md` are
amended to say this, in A3's PR (the wording drafted in the log at ~13:15 UTC). No PR of its own.

## 2. The design in four parts

1. **The core means one thing: Pure's definitions.**
2. **The door accepts legend-engine's input unchanged.** It does not translate programs. It keeps a short, fixed list
   of typed rules: the corpus's H2 setup SQL converted for DuckDB; SQLite's Decimal as floating point for legend-pure
   programs; four small rewrites of legend-engine's corpus test bodies (stage B, at the corpus's own door: lite's test
   runner split into two public halves, no hook). It adds one detector (D1).
3. **Formatters after the run** write legend-engine's and legend-pure's output from typed results: result envelopes,
   printed tables and values, error words.
4. **legend-pure-style checking at the edge.** Lite's judge emits plain verdict records; a formatter words them. The
   SQL-text fallback and its referee (legend-engine's own H2) are kept unchanged and decide every SQL-text assert by
   its answer.

## 3. The rulings this plan carries

| Ruling | What it means for the build |
|---|---|
| The goal (above) | the rulebook amendment; every departure a register row |
| R4 / D1: `substring` and `indexOf` follow Pure, **loud** | A3 switches the core and adds a detector at the legend-engine server door (its design in §4a). No translator. D1 also covers legend-engine's `isNumeric`, `parseDecimal` and `parseJson` quirks, removed in B4 |
| D2: remove both number shortcuts | B4 deletes the exact-decimal literal and the unchecked-maths set (9 legend-engine answers stop matching, each reproducible by the user; 10 PCT and 2 corpus tests gained) |
| SQL text never promised (CP-4) | A2 deletes legend-engine's SQL spelling; the fallback decides SQL-text asserts by answer |
| The fallback and referee are kept | untouched by every step |
| No legacy database versions (CP-8) | B4 drops legend-engine's H2 legacy settings (1 corpus test); the referee keeps its own H2 2.1.214 |
| E-R2 | the legend-engine route answers everything lite can |
| CP-13 | plan export and import deferred; lite's own host drift is the product track's |
| CP-14 (iii), CP-6, CP-7, CP-11 | the judge as records plus a formatter; typed facts on results; error words never in the plan; SQLite Decimal kept as a door rule |
| The differences page | drafted (`differences-page-draft.md` on `claude/census-b2`, `d13d1d4ba`: 11 entries, 10 measured; a gaps section; a "what is the same" section); lands with A3, kept current by every later step. The register rows it needs land with the step that creates each difference: A2 the SQL-text row (CP-4); A3 the `substring`/`indexOf` rows, and the two differences that exist today without a row (`toOne` of an empty list; lite answering where legend-engine refuses); B4 the number-shortcut and `parseJson` rows |

## 4. Stage A: measured and low risk (about 3 weeks; 2 at best; 4 if the product's gates slip)

**Order:** A1, then A2, then A3, then A4. A fixed order, so each step's roster of moved tests is exact.
**Proven together already:** the combined dry run (H12) applied A2's and A3's changes and D2's at once on the real
suites. Every moved test was predicted from the single measurements, except 3 per corpus database (text-only tests
hidden behind an old `indexOf` assert, now counted). On legend-engine's PCT route, stage A moves 12 tests (7 on
DuckDB, 5 on H2) and D2's removal in B4 moves 10 more, every one toward PCT's expected answer and none the other way.
**The size** (the reviewer's count): one builder in order, 11 to 15 working days, plus about a day per PR for CI on
every OS, an independent review and the user's word.

| Step | What | Done when | Size | Needs the user's word on | Builder |
|---|---|---|---|---|---|
| **A1** | **The safety nets.** The edge net (every answer on the legend-engine and legend-pure routes, recorded: PR #123). The legend-engine check (PCT classified against legend-engine's own results on DuckDB's five suites and H2's three, plus the batteries: PR #124). PCT under lite's own two judges (the product's judges track) | each PR green on every OS and reviewed; each net shown to catch a planted mistake; the Maven download fix on `main` first (it needs the user's word too) | built; review fixes in, CI red on the Maven 404 and a Windows maths-digit difference (cause being confirmed) | each PR's guarded-file moves (named in its body); whether masks number distinct ids; the Windows maths digits (an overlay, if that is the cause) | B1, B1-R, the product's judges track |
| **A2** | **Delete legend-engine's SQL spelling.** Four whole files (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`, `EngineTextDialect`: 2,990 lines, reached only through two factories), the printer seams of the plan-printout family (17 call sites), the dead flag on `Compiler.lowerResolved`. (`PureMessages.sentence` has 13 callers today, 12 of them the dialects' interim wording; it goes in B1, after A2.) Afterwards `toSQLString` and plan text answer lite's own SQL on all 12 of lite's dialects (today only H2's, DB2's and Composite's engine text), and a database lite has no dialect for is refused by name. Shape note: `A2-shape-note.md` and `A2-roster.tsv` on `claude/census-b2` (`1e8e817ee`) | the corpus moves exactly the roster's tests, by name (H2 147, DuckDB 152), each with its reason, reconciled with the dry run (0 mismatches); lite's SQL size only shrinks; a planted Studio "generate plan" request shows an ordinary query's plan byte-identical; a legend-engine server request that itself asks for SQL text (`toSQLString`, plan text, DDL, or an execute's activities' SQL) answers lite's own SQL under CP-4, pinned as a golden line with a register row | two PRs, about 5 days (the printers out with every move, 2 to 3 days; then the family's spelling-only code, about 2) | the roster; 9 guard tests (each shrinks; one ratchet from 17 to 14), the rosters, the edge nets and the lean-SQL baseline, each with its dated note; two planner signatures | to be briefed |
| **A3** | **`substring` and `indexOf` follow Pure, loud.** First lite's Cypher, SQL and IQ frontends (47 places) and the mapping language's own functions keep their meaning explicitly. Then the core switches. Then lite's own PCT adapter stops setting legend-engine's corrected-substring option, which it has set on every query since 2026-09-12 (so PCT never saw the old `substring`; upstream's adapter sets none). Then the detector (§4a). Then the differences page | lite's frontend checks unchanged (Cypher 4,267, IQ 5,984, SQL 1,651 cases); PCT channels A and B unchanged without the adapter's option, but PCT's 3 known `indexOf` failures pass; `substring`'s index arithmetic over literals stays a compile-time fold (the user's fold ruling of ~16:40: constants and facts fixed by the types, never row data); on legend-engine's PCT route 12 tests move to PCT's answer (7 DuckDB, 5 H2) and none the other way; the corpus moves exactly 7 tests per database, by name and measured (`testToSQLStringSubstringIsUncorrectedByDefault`, `testFilterUsing{Substring,ParseInteger,ParseDecimal}Function`, `testPlanForDateTimeConstantParameter{No,GMT,EST}TimeZone`), each unordered register -3; the detector's tests (§4a) pass | about 6 to 6.5 days with the detector's report (revision 3 of the shape note, `f4a2dd53`; **superseded ~19:20: no report, refuse by default, and the work split across three sessions, about 3 days**), as two PRs (the frontends keep SQL's meaning through two lite-internal functions, with byte-identical SQL, about 1.5 days; then the core switch and the detector). Shape note: `A3-shape-note.md` on `claude/compat-translator-spike` (`13212fb2`) | `substring`'s errors as legend-pure raises them (a negative start, an end past the length, an end before the start); one native-catalog ceiling (38 to 40); 17 of lite's own tests that pin the old meaning; PCT's known-failure rows for the 3 `indexOf` tests (63 rows: each test is ledgered per lane); the corpus rosters' and registers' lines; 9 edge-net golden lines; two register rows rewritten and one new; reversing the 2026-09-12 adapter deviation (and the 2026-11-07 "our tests should use the option"); the detector's default (ruled ~19:20: refuse, `warn` an operator's choice, no report) | to be briefed |
| **A4** | **legend-pure-style checking leaves lite's database judge.** Its comparison rules stay in the Java-side check until stage B (the corpus needs them: 69 DuckDB and 29 H2 tests). **Gated:** the product's 5 judge-parity fixes and the PCT-under-both-judges lane land first. The S131 narrowing does not ride in it | PCT unchanged; lite's two judges agree on the new PCT suite; both corpus rosters byte-identical across A4; **a guard, shown to catch a plant, that no legend-pure or legend-engine audience plans a database-judge verdict** (rule 4: the host judge is the only judge in compatibility) | about 2 days | none expected | to be briefed |

### 4a. The detector (D1, loud), as it will be built

- **Where:** the legend-engine server door only (execute and generate plan). A request there is the code sent to the
  database. The corpus needs none: its tests pass or fail.
- **What it flags:** a call typed as the String `substring` (2 or 3 arguments) or the 2-argument String `indexOf`, in
  the query or anything the query reaches: the user functions it calls, the qualified properties it reads, and the
  runtime's mapping (its transforms and the constraints that reach SQL). Not the `###Relational` mapping functions,
  which mean SQL's meaning everywhere.
- **When it is quiet:** for `substring` only, when the request sets legend-engine's corrected option. **`indexOf` is always
  flagged** (refused by default since ~19:20; warned under `warn`): legend-engine's option corrects `substring` alone (read at the pin: `pureToSQLQuery.pure:4662` against
  `:10644`). A literal start of 0 is exempt where it is measured equal to Pure's answer: on H2 both forms (measured on
  2.1.214 and 2.5.252, regular and legacy mode), on DuckDB the 2-argument form only.
- **What it does:** by default (the user, ~19:20) a refusal in legend-engine's error envelope, naming the call with
  its positions, and a response header (`x-legend-lite-pure-meaning`) with at most 4 codes and their counts, never the
  query's text; under the operator's setting `LEGEND_LITE_PURE_MEANING=warn`, Pure's answer with a server log line (with
  positions) and the same header. A call it cannot verify (a function value, an untypeable body) is reported as "unverified", never as
  clean. It covers every legend-engine route that runs a query: execute (Arrow included), generate plan, and the three
  test-data routes. It never changes the query.
- **The report at load (the reviewer's condition for "warn" as the default; NOT BUILT, the user's ruling of ~19:20, so this bullet is history):** when a project loads, one scan of its
  bodies, qualified properties and mapping transforms, and of the saved queries in the legend-engine query store, lists
  every flagged call. Lite's own saved queries are scanned the same way. So a changed answer is visible on day one, not
  only in the operator's log. As designed: at the edge only; run once per model, on a background thread, never on an
  answer's path, when Studio compiles the project, on the first query per model, or on demand; it types every body once,
  as Studio's compile already does (about 4 seconds on the largest model measured, legend-engine's whole corpus; it
  lists 48 calls and 83 bodies it cannot verify there); a log block and a read-only endpoint, no file; about 1.5 days.
  ~~**Ruled by the user (~14:55): warn by default**, with this report; refusal by the server setting.~~ **Superseded (the user, ~19:20, "I think it has to be refuse then for now"): refuse by default; `warn` stays a value of the setting; the load-time report is not built now**, so no body is typed ahead of time.
- **Its tests:** a planted request refused by default, and warned under `warn`; the corrected option quiet for `substring` and NOT for `indexOf`; a mapping
  function not flagged; host-side code not flagged; the header's value bounded; Studio's and DataCube's clients shown to
  ignore the header.

**Stop. The results go to the user before stage B.**

## 5. Stage B: the bigger moves (about 4 to 5 weeks), ratified after the stop

**Order:** B1 and B3 (they meet on one shape, so either may land first), then B2 (the formatters ride the connections
B3 reshapes), then B4, then B5.
**The size, from the prototypes:** B1 6.5 to 7 days; B3 about 8 (3.5 and 4.5); B2 6 to 8 plus 1 to 1.5 for the defect
cell; B4 and B5 a few days each. About 6 weeks of one builder's work; 4 to 5 with B1 and B3 built side by side.

| Step | What | Prototype | Open before it is built |
|---|---|---|---|
| **B1** | **Errors become plain records** (kind, place, literal values). Lite's own sentence is plan data (its kind's sentence and its place, a constant) that the product's writer only serializes; **compatibility's formatter is the only formatter**, wording legend-pure's and legend-engine's sentences in Java from the record (rule 3). Option, for stage B's ratification: typed raise slots, where the database returns each withheld value with the raise | H13: **every pinned message identical on every lane of the net** (PCT DuckDB, SQLite, H2; channel B; the server route), with 33 lines in lite's own lowering for 6 tests and 15 in the database judge for 4. Five switches deleted, no core hook. Lite's plan JSON +0.06%, its SQL unchanged | H13 is complete (§11-22 at `3bb9a2bd`): every pinned message on every lane identical, the corpus rosters unchanged, lite's plan JSON +0.03% and its SQL smaller. Real slice about +1,450 / -880 lines, 6.5 to 7 days; it lists its guarded files. It deletes `PureMessages.sentence` (its last callers go with A2 and this step). Two defects it found land here: the legend-engine route answers a raised error with its bare code today (`Raised: DATE_COMPONENT_INVALID`; fixed in the prototype in 6 lines, so it reads `Raised: Invalid month: 13`); and a missing store is worded with lite's sentence where a comment claims legend-engine's (`The store 'nope::DB' can't be found.`), worded by the legend-engine presenter |
| **B3** | **The fallback and legend-pure-style checking move behind three narrow connections**, with the four corpus test-body rewrites at the door (the S15 rewrite before the S131 one) | H10: PCT and the H2 corpus identical; DuckDB moves only the 2 expected; the S131 rewrite fixes 21 of 22 DuckDB and 3 of 4 H2 tests (the one left is a JSON root no rewrite reaches) | For the user's word at its PR: a size refusal becomes catchable by `assertError`, as upstream's is (reach 0 on PCT and the corpus). **Settled, no hook:** lite's test runner's step splits into two public halves (`PureTestRunner.prepare` returns the typed body and its facts; `run` runs it), as the test door already is; the corpus harness composes prepare, the four rewrites, run. S137's two-form is deleted in the same step. About 30 lines of public surface; 2.2a 3.5 days, 2.2b about 4.5 |
| **B2** | **Compatibility's formatters replace SQL-built upstream text** (about 2,000 lines deleted): legend-pure's printed tables and values, legend-engine's envelopes, in Java from lite's typed rows (rule 3). **For lite's own audience the database spells `toString`/`toCSV`, never a Java presenter** (rule 1). **Lands with it (the user, 2026-11-19 ~18:05):** lite's own route prints a relation's `toString` in the database, replacing the B17 refusal, whole Floats as `2.0`, dates with a time in lite's standard `...000000Z` (H14: 390 of PCT's 423 on DuckDB, one text on every database; the product track builds it beside B2, so no two-form switch). **Compatibility's table text (the user, ~18:12):** at the program's last step the legend-pure route takes the `toString`/`toCSV` over (the database returns typed rows, Java prints legend-pure's text); where the text is used further in the query, the database computes lite's own text and the difference is recorded; no SQL is kept for legend-pure's spelling. The nested cases are counted first. **A printed table's empty cell reads `null`** (the user, ~18:36). **`toCSV` too (the user, ~20:05):** lite's route prints a relation's `toCSV` in the database, like `toString` or another simple SQL that makes sense (a standard CSV layout, lite's value texts), replacing the refusal; S316 amended (lite keeps owning the name; compiling legend-engine's own `toCSV` program, H15 §11, waits on four product gaps). The slice's shape: `compat-homework/H14-slice-shape-note.md` on `claude/compat-table-text-db-spike` | H11: 431 of 431 printed tables byte-identical on DuckDB, 342 of 342 on H2, 419 of 420 SQLite, 427 of 430 Postgres; no core hook | the product's window-in-list fix first; the corpus half (done: DuckDB byte-identical to `main`, H2 three gains toward upstream); single-value prints; the checked graph's defect cell, worded by the legend-engine presenter (12 planned in the DuckDB corpus, none answered; about 120 lines, 1 to 1.5 days), and the checked graph's default violation message, legend-engine's sentence in the product today (`resolver/GraphEmission.java:653`). The design (the judges' H10 §9.6): the plan names the program's own `toString`/`toCSV` call at a side's root as a product judgment kind, for every assert family (22 corpus `assertSameElements` sides need it), and each audience's run spells it; done-when 3 amended to allow exactly that. The corpus half: 0 differences caused by the printer on H2 and DuckDB, 3 H2 gains toward upstream. 6 to 8 days |
| **B4** | **Remove what is left:** H2's legacy settings (1 corpus test), the two number shortcuts (D2: 9 answers, 10 PCT and 2 corpus tests gained), legend-engine's `isNumeric`, `parseDecimal` and `parseJson` quirks (D1: `parseDecimal` -2 per database, `parseJson` -1 DuckDB and +1 H2, `isNumeric` 0), DataCube's legend-engine workarounds moved to its legend-engine side | measured (B2's (j), H12, H5 task 0) | each removal's moved tests listed by name before it lands |
| **B5** | **Delete the switch machinery** (legend-engine's feature flags in the core included) **and turn on the guards**; **the switch rule (the user, ~20:35): the one door-switch registry, its ceiling (5), its reader test and removal dates become B5's lasting guard; nothing in the core reads a switch**; the row names the `engine` seed rewrite, which runs inside the edge's own lowering callback that the test-data planner calls (the reviewer's note, 285f98e18: within the rule's letter, as the core reads no switch) | the ratchet is being prototyped (B2) | the counts reach zero: 49 parameters and 50 calls of the switch today; 142 places of upstream-specific code. The door keeps refusing, by name, every legend-engine flag but the two it reads |

## 6. Not in this program

- **Deferred:** exporting plans to legend-engine and importing its plans.
- **The product track's, found by phase 0:** the 33 H2 function gaps (and H2's essential and standard suites joining
  lite's own PCT); the window-in-list defect (gates B2); lite's own rules copied across Python and the browser tab.
  **Lite's `sqlSubstring` follows the SQL standard** (the user, ~21:10): today it clamps a start below 1 like H2
  (`'Cr'` for `('Credit', 0, 2)`); the standard, DuckDB and Postgres give `'C'`; measured before it changes.
  (The Maven download fix and the judge-parity work are taken into stage A1 by the user's word of ~14:30.)
- **A later meaning decision, not a gate:** comparing text with a number (`'x' == -1`). It was thought a defect fix
  gating A3; measured (~15:15), it changes answers in 26 H2 and 30 DuckDB corpus tests, where legend-engine lets the
  database coerce, and A3 does not need it. The scoped version is being measured; then it goes to the user with numbers.
- **Not measured by anyone:** the other legend-engine databases (Snowflake, Postgres, Databricks and the rest) for the
  answers that change; real deployed legend-engine projects; Studio end to end; very large results through the Java
  formatters; query cost on users' own warehouses.

## 7. The safeguards

- Nothing merges without the user's word. Every step is a PR, green on every OS, reviewed by a session that wrote none
  of it.
- A pin, ceiling, ledger, golden or allowlist entry moves only with the user's word and a dated note; each PR names
  its moves.
- A step that would need a new switch or hook in lite's core stops and comes to the user.
- Every difference from legend-engine or legend-pure a step creates is a register row listed for the user before it
  lands, and goes on the differences page.
- Upstream's answers are read from the pinned sources or asked of legend-engine itself, never recalled.
- The stop after stage A.
