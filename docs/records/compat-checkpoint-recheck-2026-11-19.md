# Independent re-check of the compatibility program at its phase-0 checkpoint (2026-11-19)

The same independent reviewer as `compat-independent-review-2026-11-19.md` (branch `claude/compat-review`). This
pass judges the plain-English design and plan against phase 0's measurements. It is anchored on my own F1-F13, not on
the orchestrator's summaries. Review only: nothing in the product, and no pin, ceiling, ledger or allowlist, moved.

**Read at these heads:**

| Record | Branch | Head |
|---|---|---|
| `compat-design-and-plan-plain-2026-11-19.md`, `compat-phase0-checkpoint-2026-11-19.md`, the log | `claude/compat-program` | `a73a5e9d` |
| H5 | `claude/compat-translator-spike` | `b4a16fbe` |
| H6 | `claude/compat-judges-homework` | `491f6464` |
| H7 | `claude/compat-engine-lane` | `f922740b` |
| H8 | `claude/census-b2` | `0b720d27` |
| H11 | `claude/compat-relation-text-spike` | `a9df8c39` |
| the code | `main` | `b75d6c73a` |

## 1. Verdict

**Proceed to Stage A: the design holds, and phase 0 did what the review asked.**
- The riskiest leg was spiked first and measured (H5).
- G2's replacement was measured (H6).
- The relation printer was proven (H11).
- The critical path was re-cut.
- The 13 findings were folded in.

**Four conditions before stage A starts:**
1. The user rules the change of goal explicitly, before D1 and D2 (finding 1).
2. The "what we lose" table is completed (finding 3).
3. A1's nets are actually sound and green on every OS (finding 4).
4. H6 §6.3's account of the referee is corrected (finding 5: settled in the code, H8 is right).

**On D1 and D2:** the recommendation (Pure's way, no translator; remove the two shortcuts) is supported by every number
measured. But every number measures legend-engine's TEST suites, not its users' code. The decision should be taken
knowing that, with a cheap "loud, not silent" safeguard (finding 2).

## 2. Findings, most severe first

### R1. The goal has changed. The user must rule that first, and D1/D2 follow from it

**Evidence.**
- AGENTS.md's upstream-fidelity tenet: "wherever possible, lite does EXACTLY what legend-pure and legend-engine do ...
  where exactness is too expensive, lite does the closest thing".
- G8 of the goals of record says the same. The brief's goal: "answer upstream's clients ... as well as before, or
  better, measured against upstream itself".
- D1 (Pure's way) and D2 (remove the shortcuts) are measured moves AWAY from legend-engine:
  - -4 corpus tests per lane for `substring`/`indexOf`;
  - -4 to -1 per lane for the other strands;
  - 1 pure/v1 answer;
  - 5 PCT SQLite tests.
  They are justified by "Pure's own definition", not by cost. That is a different goal.
- The owner's own review (log, ~12:30 UTC) says so: "The goal has quietly changed ... amending them is the user's
  word".

**Change.**
- One sentence for the user, ruled before D1/D2: lite follows Pure; legend-engine's formats, and its answers where they
  agree with Pure, are matched; the rest is recorded.
- Amend G8 and the AGENTS.md tenet to match.
- D1/D2 then follow from that ruling, rather than each being argued as an exception.

### R2. The evidence for D1/D2 is test-suite-shaped. The strongest case against is "silent", and there is a cheap fix

**What the counts are.** Every count behind D1/D2 (H5 Task 0, H7, H8) is legend-engine's corpus, PCT or the pure/v1 net.

**What we know about users' code:**
- lite's own 62 projects: 0 uses (the plain plan);
- my count of the six pinned Legend showcase projects (`MODULE.bazel:455-462`, the GitLab archives at their pinned
  commits, 99 `.pure` files): **0 calls of `substring`, `indexOf`, `isNumeric`, `parseDecimal`, `parseJson` or
  `sqrt`**.

That supports D1. But demos are not deployments. No warehouse database is measured at all; real legend-engine
projects run on Snowflake, Databricks, BigQuery and the like, while the ENGINE lane covers DuckDB and H2 only.

**The strongest case against D1: the change is SILENT.** A saved query or service that calls `substring(1, 3)` in a
store-bound lambda returns different rows, with no error.

**A nuance that narrows it.** H3's grid: on H2, a start of 0 behaves as 1, so `substring(0, n)` already equals Pure's
`substring(0, n)` there. If Snowflake treats a start of 0 the same way (not checked here), the commonest idiom, a
prefix, is unaffected on those databases. The silent change concentrates on starts of 1 or more, and on DuckDB's start
of 0.

**The strongest case against D2.** pure/v1 is Studio's live route:
- `0.1 + 0.2` and `sqrt(-2.0)` answer differently;
- equality filters or joins over literal arithmetic can flip;
- PCT's count for D2 is still running (the plain plan's rows 4 and 5).

**Change.**
- Rule D1 as "Pure's way, LOUD where it differs": the ENGINE door marks (a warning in the answer's log or trace) or
  refuses by a switch any store-bound String `substring`/`indexOf` whose start is not a literal 0, in a request that
  does not set `CORRECT_SQL_SUBSTRING_INDEXING`. It is a detector, not a rewrite. H5's typed use-site analysis already
  finds these calls (584 lines; the detector is a subset).
- Hold D2 until the ENGINE-lane PCT count with both shortcuts off lands.

### R3. The "what we lose" table is incomplete, and one of its rulings contradicts D1

Missing rows, each from a record:

**(a) PCT SQLite loses 5 Decimal-arithmetic tests.** These are refused by name once `refusesDecimalArithmetic` goes
(H5 Task 0, PCT table). The checkpoint's CP-11 recommends keeping ruling (c) "as a translator rule", but D1 means there
is no translator. Either CP-11 becomes "refuse by name" (5 tests lost, a row) or it is a door rewrite (then D1 is not
"no translator").

**(b) legend-engine's feature flags.** `LEGACY_SQL_NULL_UNSAFE_EQUALS`:
- costs 1 corpus test per lane (H5 Task 0; plan text only);
- 7 corpus tests set a flag (H8 (a.3)).
The table does not say what happens to a request that sets a flag once its consumer is gone: silently ignored, or
refused by name. `FeatureRules.requireConsumed` makes an unconsumed flag loud today. The plain plan's steps B4/B5 do
not name the Feature strand at all; my F6 is still open in the plan's text.

**(c) Errors, for legend-engine clients.**
- B1 words errors from records. legend-engine's text for a database error is the database's message over
  legend-engine's own SQL (H3 (c), item 3), which cannot be reproduced.
- H5 found that no lite error on these doors carries a source position.
- Studio underlines code from an error's position. The table has no row for error words or positions.

**(d) generatePlan's answer after A2.** Studio's plan view reads generatePlan. A2 deletes the engine-text printers, and
export (which r7 made the home of the plan FORMAT) is deferred. The plain plan does not say what generatePlan returns
after A2:
- legend-engine's node structure with lite's SQL;
- lite's own plan;
- or a refusal.

**(e) Lite's own users under R4.**
- A negative start will raise (3.3), where DuckDB's clamp answers today.
- Saved queries and cubes in deployed stores are not countable from here; my F12 asked for a meaning version on saved
  artifacts.
- The table's "lite's own 62 projects have 0" covers the repository only.

**(f) E-R2 is one-way.** A program written against lite's legend-engine route may not run on legend-engine (§12 I2 of
the old plan). The table has it only as "what gets better".

**(g) Performance on users' databases.** Lite's SQL shape replaces legend-engine's: exists-joins, root forms, union
frames. Nothing measures query cost on a real warehouse.

**(h) A label to fix.** "153 fixes" on DuckDB's PCT includes about 51 rows legend-engine REFUSES in other words
(DEFECT.UNSUPPORTED: "is supported only after ...", "Any is not managed yet!"; my review of PR #124). H2's "0
refusals" is likewise 36. The total is right; the labels are not.

**Change.** Add rows (a) to (g), correct (h), and decide CP-11 consistently with D1.

### R4. A1's nets are not yet sound, and the crown jewel depends on a jar that 404'd today

**The three nets:**
- **PR #123 (the edge net): HOLD** (my review):
  - 3 of 10 plants are vacuous;
  - PCT's library now depends on a sorting helper, which is the "dodge" `PctDisciplineTest`'s own javadoc forbids;
  - two guarded-list moves are not named;
  - its CI was red.
- **PR #124 (the legend-engine check): HOLD.** Its per-push batteries test writes a Windows temp path into a Pure string
  literal unescaped (`LiteBatteries.java:41-43`; lite unescapes `\t`), and it collides with #123's change of the pure/v1
  golden format.
- **The LITE-PCT lane (C3):** on the product track, not landed.

**The jar.** Both #123's Linux `checks` and `corpus_duckdb` failed on a Maven Central 404 for
`com/h2database/h2/2.1.214/h2-2.1.214.jar` (`@+product_jars+h2_engine`). That is the referee's database: legend-engine's
H2, which the user ruled "Engine's 2.1.214, isolated" (`EngineH2Referee` javadoc). The SQL-text appeal ("one of the
crown jewels") depends on an old artifact fetched from an outside host at build time.

**Change.**
- Stage A waits for all three nets green on every OS.
- Vendor or mirror the referee's H2 jar under the repository's own control, sha-pinned as today.

### R5. H6 and H8 contradict each other on where the referee replays. H8 is right, and a plan step falls away

**The two records.**
- H6 §6.3: "The referee has no session of its own ... on the H2 lanes the replay runs on the body's H2 session, which
  ... opens with the edge's settings". From this the plan adds a step: a declared LEGACY referee target before CP-8.
- H8 §(h): "It replays goldens on legend-engine's own H2, 2.1.214, opened `MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH`
  (`testbackend/EngineH2Referee.openReferee`)". The checkpoint's Q12 repeats this.

**What the code shows (settled; H8 is right, H6 §6.3 is wrong).**
- `edge/SqlTextRuns.java:188-193` passes the body's session to the referee SPI. That is the half H6 read.
- The SPI's one implementation is the harness's `spec/src/test/java/com/legend/harness/ReplayReferee.java`. Its
  `verifyArmed` takes the body's connection, and `verifyAuto` (about line 362) picks the replay's session:
  `TestBackends.of(session) == TestBackends.referee() ? verifyOnSession(...) : verify(seeds, ...)`.
- `TestBackends.of(Connection)` (`core/src/test/java/com/legend/testbackend/TestBackends.java:95-103`) answers a lane's
  backend: `H2Backend.INSTANCE`, lite's 2.5.252, for an H2 session. `referee()` (`:88-90`) answers
  `EngineH2Referee.INSTANCE`, a different singleton. The test is never true for a lane's session. So every golden replays
  on the referee's own H2: the family mirror (`mirrorBegin`, opened by `openReferee`) or a fresh referee database,
  legend-engine's 2.1.214 opened `MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH` with its extension functions
  (`EngineH2Referee.java:67-70`). The body's session supplies only "our" rows and the recorded seeds.
- Consistent with that, H8 (h) measured the H2 corpus with the edge's settings off: it lost 1 test, the BIT column, and
  no appeal moved.

**Change.**
- Correct H6 §6.3, and drop the plan's added "declared LEGACY referee target before CP-8" step. B4's removal of the H2
  lane's legacy settings does not touch the referee.
- Minor: `verifyOnSession` looks unreachable from every lane, a dead branch in the referee. Name it in B4's leftovers or
  delete it.
- R4's jar point stands with more force: the one database every SQL-text appeal replays on is that 404'd 2.1.214 jar.

### R6. A2's scope and proof are underspecified

"About 3,000 lines" is the three engine-text printers (2,951). The plan-text family (`EnginePlans`, `PlanText`, about
3,500 lines) builds pure/v1's generatePlan and `planToString`. A2 must say what happens to it (R3 (d)).

**Required before A2 merges:**
- the exact roster of the 143 "not promised" tests and the 6 plan-structure tests, committed;
- the 3 DIVERGED appeals rostered: S372, and `firstDayOfWeek`, fixed or rostered;
- pure/v1's golden moves listed;
- `toSQLString` on a database without a lite dialect refused by name, with its count;
- the corpus's passed-by-answer set byte-identical;
- the lean-SQL baseline only shrinking.

### R7. A3 and A4 each have a dependency or a boundary the plain plan does not state

**A3 needs, before it lands:**
- the cross-kind `!=` fix (H5: the vacuous DuckDB pass becomes a real check and raises);
- the user's word on 3.3's negative-start raise and its `NativeCatalogGovernanceTest` ceiling move;
- the 17 moved core tests listed (9 old-meaning pins, 8 generated-text goldens);
- PCT's 3 ledger rows moved with the user's word.

**A4's boundary.** A4 removes the DATABASE judge's compatibility half only.
- The corpus needs legend-pure's leniencies: 69 tests on DuckDB and 29 on H2 (H6 §6).
- The corpus is judged by the HOST judge, so the host judge's compatibility rules must stay until B3's four door
  rewrites land.
- Proof required: both corpus rosters byte-identical across A4, PCT channel B outcomes byte-identical, and the LITE-PCT
  lane green under BOTH.
- S131's multiset is LIVE in LITE today (H6 §6); its narrowing is a product item that must not land inside A4 unseen.

**The order A1 → A2 → A3 → A4 is right.** A1 first. A2 and A3 are independent. A4 waits for the product's judge
fixes, as stated.

### R8. Stage B's least proven step is B1. B2 is proven only for relation tables, and builds on seams B3 reshapes

**B1, error records through three hosts.**
- No prototype.
- Two plan-format bumps (v59, v60).
- The tab's and Python's readers.
- PCT's words for computed-operand raises (A3).
It is the least proven step.

**B3, the appeal's move.** Second, with H10 running.

**B2.**
- H11 proves legend-pure's relation table: 431 of 431 byte-identical on DuckDB, and 4 H2 tests gained.
- Not proven:
  - value prints (`format`'s `%r` and `%0Nd`, `toRepresentation`, an instance's id, DateTime and list texts in
    messages);
  - legend-engine's class, json-builder and graph-fetch envelopes;
  - graph-fetch keys and trace cells (H3 (c) proved the TDS envelope only).
- H11's own cautions:
  1. it rides `EngineTestText`, `EdgeJudged` and `EdgeHearing`, the three hooks B3 reshapes, so B2 before B3 means
     writing it twice;
  2. its judgment record in the TEST PLAN is an instruction ("present this side as `#TDS` text"). Done-when 3 says no
     plan carries "a presentation instruction". Either amend done-when 3 for the edge's typed extension point, or keep
     the instruction out of the plan.

**Change.**
- Prototype B1 as the next spike, on one route end to end, including the tab and Python.
- Order B3 before B2's real implementation, or write B2 against B3's interface.
- Resolve done-when 3's wording.

### R9. B5's mode guard counts four method names

`ModeBranchCensusTest.BRANCHES` (PR #123) names `isEdgeStyle`, `judgesEngineText`, `PureText.recording` and
`Boolean.getBoolean`. It does not count `twoForm`/`facet`, `LoweringForms` or the Feature flags.

B5's "49 parameters, 50 calls" (H8 (a)) is the right count, and should be the guard. Widen the census, or name H8's
ratchet as the end condition.

### R10. What the effort has still not looked at

- **Warehouse databases** (Snowflake, Databricks, BigQuery) for every meaning row. The ENGINE lane covers DuckDB and H2;
  the other 9 manifest databases are unmeasured.
- **Studio end to end against lite's pure/v1:** compile errors' source positions, the plan view, the grammar round trip
  in the UI.
- **Large results:** presenters run after the run in Java. Streaming and memory for a large TDS are unmeasured (H4
  measured per-row cost only).
- **An upstream version bump:** how the manifests, the oracle's pins and the batteries are re-baselined when
  legend-engine moves past 4.145.0.
- **The edge net's CI time:** 568 s locally; CI's time is PENDING (Q10).

## 3. F1-F13: what answered each

| # | Finding | Status | What answered it, or what is open |
|---|---|---|---|
| F1 | the translator unproven at model scale | answered | H5: V2-V6. Typed use-site retargeting works through public classes; the translator wins +4 per lane. Moot under D1. Per-target compile memory is unmeasured (moot) |
| F2 | 2.2 would undo plan-as-data | answered | H6 §6-§7: the `Edge` records stay data. New: H11's instruction record against done-when 3 (R8) |
| F3 | G2's guard leaves PCT | in part | H6 measured the LITE-PCT lane (786 pass, 5 parity bugs found). C3 is not landed; it is an A1 precondition |
| F4 | the critical path and calendar | in part | stages sized (2-3 and 3-4 weeks). No dependency graph; the checkpoint's §4 is PENDING |
| F5 | "wrong" versus "different" | measured | H5 Task 0's table classes each strand against Pure's definition. D1 is the decision (R1, R2) |
| F6 | the Feature flags | measured, not planned | H8 (a.3). The plain plan's steps do not name the strand (R3 (b)) |
| F7 | R4 reaches the islands | answered | H5 §3: 47 sites split, parity unchanged (A3) |
| F8 | 0.4 (j) without the corpus | in part | corpus done (S20 +2/0; the plain set pure/v1 1). PCT pending (rows 4, 5) |
| F9 | the plan format, and export | in part | export deferred; the 6 plan-structure tests not promised. generatePlan after A2 undefined (R3 (d), R6) |
| F10 | CP-8's blanket row | open | S374 now says the oracle's file "will" class differences; no slice owns it |
| F11 | the referee's settings | answered, after correcting H6 | the referee replays on legend-engine's isolated 2.1.214 with its settings (R5). H6 §6.3 needs a correction |
| F12 | the rulings' other consequences | mostly answered | E-R2 against the corpus: H8 (k), 0 runnable error asserts. `toSQLString` with no dialect: plan r7. The translator in the appeal: moot under D1. Saved artifacts' meaning version: not in the plain plan (R3 (e)) |
| F13 | numbers | answered | corrected in r6 and r7 (H8 (a) confirmed the map) |

## 4. The plain plan's numbers, checked

**Each number in the plain plan's §4 and §5, and where it was found:**

| The number | Found in | Holds? |
|---|---|---|
| 1,212 against 1,005 (DuckDB); 448 against 411 (H2) | H7 §0 | yes. legend-engine's side is a manifest expectation, sampled 21 of 21 |
| row 1: 5 store-bound calls, 4 tests per database | H5 F1, V2 | yes |
| row 2: 143 tests | B2's probe | yes |
| row 3: 1 test | H8 (h), H5 Task 0 | yes |
| row 4: 1 pure/v1 answer, 0 corpus, 16 PCT cells on 8 databases | H5 Task 0, H8 (c) | yes. Lite's own PCT count is pending |
| row 5: +2 on H2, 0 elsewhere | H5 Task 0 | yes |
| row 6: 0; -2 and -2; -1 and +1 | H5 Task 0 | yes |
| `raw`: -2,452 | H5 Task 0 | yes |
| B2: 431 of 431, +4 on H2 | H11 §4 | yes |
| B5: 49 parameters, 50 calls, 142 hits | H8 (a); the census | yes |
| A3: 47 sites; Cypher 4,267, IQ 5,984, SQL 1,651 | H5 §3 | yes |
| A4: 0 PCT verdicts | H6 §6 | yes, for PCT. The corpus needs the leniencies (R7) |

**Missing:** R3's rows.
