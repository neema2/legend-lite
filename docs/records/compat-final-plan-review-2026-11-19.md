# Independent review of the compatibility program's final plan (2026-11-19)

The same independent reviewer as `compat-independent-review-2026-11-19.md` and
`compat-checkpoint-recheck-2026-11-19.md`. Review only: nothing in the product, and no pin, ceiling, ledger or
allowlist, moved. I checked the records and the code myself, not the orchestrator's summary.

**Read at these heads:**

| Record | Branch | Head |
|---|---|---|
| `compat-final-plan-2026-11-19.md`, the plain plan, the log since 12:54 | `claude/compat-program` | `0a9303e5` |
| `H12-combined-dry-run.md` and `h12/corpus/attribution-{h2,duckdb}.tsv` | `claude/compat-translator-spike` | `326b5def` |
| H6 | `claude/compat-judges-homework` | `1062df31` |
| the code | `main` | `b75d6c73a` |
| legend-engine | the pinned 4.145.0 clone | `230c1591` |

## 1. Counts and verdict

**12 findings:**
- 5 must change before the user ratifies stage A (M1-M5);
- 4 should change (S1-S4);
- 3 minor (N1-N3).

**Verdict: not yet ready to ratify, but close.** The plan carries the rulings correctly, and stage A's shape is right. Every
must-fix is a change to the plan's text; none needs a new measurement. But:
- one ruled feature is unsound as specified (M1);
- one done-when cannot pass in either order (M2);
- one scope item that needs the user's word is missing (M3).

The user should see those three at ratification, not after it.

**Did the plan carry my four conditions?**
1. **The goal ruled:** done.
2. **The "what we lose" table:** mostly done. Rows 8-12 were added and the labels corrected. Row 1 is now wrong (S3),
   and query cost on warehouses is still not listed (N3).
3. **A1's nets green on every OS:** written as A1's done-when. Not met yet, which is right for a gate.
4. **The referee:** corrected in the plan. H6 §6.3 itself is not corrected (N2).

## 2. Must change before ratification

### M1. The detector goes quiet on `indexOf` when it must not. legend-engine's corrected option fixes `substring` only

**The spec.** The plan (§3, and A3's done-when) and the log's ~13:15 entry say the detector stays quiet "when the
request has set legend-engine's corrected option".

**What legend-engine does (the pinned 4.145.0 source):**
- `pureToSQLQuery.pure:4660-4665`: `processSubstring` alone reads `$state.correctSubstringIndexing`, which is set from
  `Feature.CORRECT_SQL_SUBSTRING_INDEXING` at `:369`.
- `indexOf_String_1__String_1__Integer_1_` maps to `processDynaFunction` unconditionally (`:10644`). On H2 that is
  `LOCATE(%s)`, 1-based and 0 on a miss, with no flag (`h2Extension2_1_214.pure:231`).
- H6's own record says the same (line 410: "no feature corrects indexOf today").

So a request that sets the option still gets legend-engine's `indexOf` meaning from legend-engine. Lite will answer
Pure's, and the detector as specified stays silent: exactly the silent difference D1-loud was ruled to prevent.

**Change.**
- Quiet only for `substring` under the option.
- Every store-bound String `indexOf` (2 arguments) warns whatever the request sets. The 3-argument form is refused by
  legend-engine, so there is nothing to warn about.
- Add a planted request with the option set and an `indexOf` call to A3's done-when.

### M2. A3's corpus done-when ("loses exactly its 4 per database") fails in either order

**What H12 measured** (H12's attribution TSVs, counted by me): the corpus tests that fail under the `substring`/`indexOf`
change (1) are:
- (1) only: 4 per database;
- **"1+2"**: 4 on H2 and 5 on DuckDB, which fail under (1) alone AND under the SQL-text change (2) alone. These are text
  tests whose printed SQL changes with the new lowering;
- **INTERACTION**: 3 per database, which fail only with both changes.

**What each step then moves, by order:**

| Order | A3 moves | A2 moves |
|---|---|---|
| A2, then A3 | 4 + 3 = **7 per database** | 142 + 4 + 1 = **147 on H2**; 146 + 5 + 1 = **152 on DuckDB** (B2's own TSV: 147 and 152) |
| A3, then A2 | 4 + 4 = **8 on H2**; 4 + 5 = **9 on DuckDB** | 142 + 1 + 3 = **146 on H2**; 146 + 1 + 3 = **150 on DuckDB** |

**Change.**
- Fix the order: A2 then A3, because then A3's move is only its own 4 plus the 3 named interactions.
- Write both rosters into the done-whens, by test name, from `h12/corpus/attribution-*.tsv`.
- "Either order" makes every roster ambiguous.

### M3. Lite's own PCT forces the corrected option on every query. A3 must remove it, and that needs the user's word

**The code.**
- `pct/.../extension/PctPlanRoute.java:45-46` sets the flag.
- So do `channelb/ChannelB.java:393` and `PctExecuteNative.java:140`.
- `PctPlanRoute`'s javadoc (`:20-21`) calls it "the user's ruling of 2026-09-12, a written deviation from upstream's PCT
  adapter, which sets none".

**Why it matters.** Lite's own PCT has never run lite's default `substring`. A3's `substring` half is invisible to it,
which is why H12 saw only the 3 `indexOf` rows move in PCT channels A and B.

**Change.**
- Add to A3: drop the flag from lite's PCT adapter, after the core switches.
- The proof that the core switched: PCT channels A and B unchanged with the flag gone.
- Add to A3's user's-word column: reversing the 2026-09-12 deviation (and the 2026-11-07 "our tests should use the
  option").

### M4. A2's "What" contains a withdrawn claim, its answer for pure/v1 is undecided, and it is unsized

**The withdrawn claim.** A2's "What" names "the plan-printout family only the corpus reaches (about 3,500)". The log
(~13:12) records B2's bytecode finding that a pure/v1 query calling a plan-text native reaches
`EnginePlans.ddlText` → `PureText.engineText` → `EngineStyleH2` (`BodyPlanner.prepare:311`). The orchestrator
withdrew "unchanged, checked" for the plain plan, but the final plan restates the same claim.

**Change.**
- Strike "only the corpus reaches" until B2's run.
- Decide now what such a request answers after A2. Lite's own SQL text is CP-4's promise; a refusal would be new. Make
  it a register row and a line of the edge net's golden net.
- Size A2 from the shape note before ratification. "From B2's shape note (in progress)" leaves stage A's largest
  deletion, about 6,000 lines, unsized inside a "2 to 3 weeks" claim.

### M5. A4 lost its gate and half its proof

**The gate.** The plain plan had "the product track's judge fixes land before A4". The final plan's A4 row says "none
expected", and §6 lists the 5 judge-parity bugs as the product's without tying them to A4. "Lite's two judges agree on
the new PCT suite" cannot pass until those land, together with the LITE-PCT lane (C3).

**Change.**
- Restore the gate: A4 waits for the 5 parity fixes and the LITE-PCT lane on `main`.
- Add to A4's done-when: both corpus rosters byte-identical across A4. The host judge still applies legend-pure's
  leniencies to 69 DuckDB and 29 H2 tests (H6 §6).
- Add to A4's done-when: S131's multiset narrowing does not ride inside A4.

## 3. Should change

### S1. D1's other three strands have no step

D1 covers the plain plan's rows 1 **and 6**: `isNumeric`, `parseDecimal` and `parseJson` answered Pure's way.
- The corpus moves: `parseDecimal` -2 per database; `parseJson` -1 on DuckDB and +1 on H2.
- The final plan names no step that lands them. A3 is `substring`/`indexOf`; B4 lists H2's settings, D2 and DataCube.

**Change.** Put them in B4 by name, with their moved tests.

### S2. The detector's reach and its default

**Reach.** "The typed query and the user functions it reaches through `ModelContext.findFunction`" misses Pure code
reached through the model rather than through function calls:
- qualified (derived) properties on classes;
- model-to-model mapping property transforms;
- constraints, if any reach SQL.

The walk must follow property access to qualified-property bodies, and the runtime's mapping to its Pure transforms.
The `###Relational` dynaFunctions stay unflagged, as specified.

**Precision.** If H3's grid holds (on H2 a start of 0 behaves as 1), `substring(0, n)` answers the same on H2 under both
meanings. The commonest idiom, a prefix, would then warn on every H2 query. Exempt a literal 0 start per target, by the
grid. On DuckDB the 0 start differs, so it is not exempt there.

**The header.** "Shown harmless to Studio" is in the user's-word column, but it is a test, not a decision. HTTP
clients ignore unknown header fields. Make it a done-when, with a bounded value: a code and a count, never the call
text.

**The default.**
- "Warn" reaches the server operator's log only. Studio, service clients and DataCube surface neither the log nor the
  header, so the person who gets the changed answer sees nothing.
- Warn is the right default for legend-engine's clients ONLY if it is paired with a one-time report when a project is
  loaded. The scan covers:
  - every function body;
  - every saved query in the edge's query store (`EdgeServer.QUERY_STORE`, `/api/pure/v1/query`);
  - every qualified property and mapping transform, through `Compiler.compileAllBodies`.

  The report lists each flagged call to the operator on day one. That is eager Knowledge, the north-star tenet. With
  the report, warning per request is fine; without it, refuse by default, and the server setting downgrades to warn.
- Lite's own route has no detector. Lite's own saved queries change meaning silently (plain plan row 12). The same
  load-time scan of lite's own store would make that loud as well.

### S3. The plain plan's row 1 is wrong in one claim and stale in another

**The wrong claim.** "Corrected by a flag that PCT always sets" is false:
- legend-engine's PCT adapter passes `^ExecutionContext()` with no flags (`pct_relational.pure:108`);
- legend-engine's own H2 manifest lists `substring::testStart`, `substring::testStartEnd` and `indexOf::testSimple` as
  expected failures, for example "expected: 'he quick …' actual: 'the quick …'".

It is lite's adapter that sets the flag (M3).

**The stale line.** "The answer changes silently; there is no error" predates D1-loud. It should say:
- warned at the legend-engine door, and refused under the setting;
- still silent on lite's own route and in lite's own saved queries.

### S4. "Stage A about 2 to 3 weeks" is the best case, not the estimate

**The state.**
- **A1:** #123 and #124 are red: the Maven 404, whose fix needs the user's word on `main`; the Windows `edge_net`
  digits, whose cause is unconfirmed; `markets_test`'s timeout. Each has taken two review rounds so far.
- **A2:** unsized; I estimate 3 to 5 days for about 6,000 lines with the guard files and the pure/v1 decision.
- **A3:** 4 to 5 days, gated by the product's text-and-number fix and, for H2's own route, by lite's H2 essential and
  standard PCT lanes (B1-R's finding: lite's own H2 PCT runs the relation suite only).
- **A4:** 2 days, gated by M5's fixes.

**The sum.**
- One builder, in order: 11 to 15 working days of work, plus about a day per PR for CI on every OS, independent review
  and the user's word. That is about 3 to 4 weeks.
- A2 and A3 in parallel (two builders, which M2's order forbids anyway, unless the rosters are written per order): 2.5
  to 3 weeks.

**Honest wording:** "about 3 weeks; 2 at best; 4 if the product-track gates slip".

## 4. Minor

### N1. "22 tests move" is stage A plus D2

H12's ENGINE-route 22 is:
- stage A's change (1): 12 (7 on DuckDB and 5 on H2);
- D2's plain set, which lands in B4: 10.

The log's "stage A's whole ENGINE-route effect: 22 rows" is wrong. The final plan's §4 sentence is accurate only
because it says H12 applied D2's changes too. Split it: 12 and 10.

### N2. H6 §6.3 still says the referee replays on the body's session

At `1062df31` it still says "The referee has no session of its own". The code says otherwise: `ReplayReferee.verifyAuto`
replays on the session only if `TestBackends.of(session) == TestBackends.referee()`, which no lane's session meets
(my re-check, R5). Correct the record, so no later step re-adds the dropped "referee target" step.

### N3. Small gaps

- **Where the rulebook amendment rides.** "In the first stage-A code PR": #123 and #124 are stage-A code PRs. Say A3's
  PR, as the log does.
- **Other legend-engine flags after B5.** Today an unconsumed flag is refused (`FeatureRules.requireConsumed`). The
  door must keep refusing every member but the two the plain plan's row 9 names. Say so in B5.
- **"Not measured by anyone yet"** still lacks query cost on users' warehouses under lite's SQL shapes.

## 5. The five questions, answered

1. **The rulings:** matched exactly, except the detector's quiet rule (M1).
   - Stage A's table holds conditions 1 and 3, and condition 4 in the plan.
   - Condition 2 has row 1 wrong (S3), and A2's whole scope is not yet there (M4).
   - Dropped from the working plan and still mattering: A4's gate (M5), and the row-6 strands' landing (S1).
2. **Real proofs:**
   - A1's and A4's are real once M5 is added.
   - A2's is real once its roster is per order (M2) and its claim is struck (M4).
   - A3's corpus line cannot pass (M2), and its `substring` proof is masked by the PCT adapter's flag (M3).
   - Stated as measured but not measured: "the plan-printout family only the corpus reaches" (contradicted by B2's
     bytecode); "22 tests move" for stage A (N1); "Studio ignores the header" (S2).
3. **The detector:** unsound for `indexOf` under the option (M1). Its reach is incomplete, and it is noisy on H2 prefixes
   (S2). "Warn by default" is right only with a load-time report; otherwise refuse by default (S2).
4. **"2 to 3 weeks":** the best case. About 3, and 4 if the product-track gates slip (S4).
5. **The verdict:** not yet. Write M1-M5 into the plan, then it is ready for the user to ratify stage A. S1-S4 can ride
   in the same edit.

## 6. Re-check of `51888e28` (~13:40 UTC)

I read the final plan's §3-§5 and §4a, the plain plan's row 1 and H6 §6.3 at their new heads.

**Must-fix:**
- **M1, met.** `indexOf` always warns. The option quiets `substring` only, citing `pureToSQLQuery.pure:4662` against
  `:10644`. The test for it is planted: "quiet for `substring` and NOT for `indexOf`".
- **M2, met.** The order is fixed (A1, A2, A3, A4). A2 moves its roster by name (H2 147, DuckDB 152) with 0 mismatches
  against the dry run. A3 moves exactly 7 per database by name: 4 answers and the 3 hidden text tests.
- **M3, met.** A3 drops lite's PCT adapter's option after the core switches. Its proof is channels A and B unchanged
  without the option, and the user's word on reversing the 2026-09-12 and 2026-11-07 rulings is listed.
- **M4, met.**
  - A2 is rebuilt from B2's shape note (`1e8e817ee`): 2,990 lines in four files, the 17 printer seams, two PRs, about 5
    days.
  - The pure/v1 SQL-text answer is decided (lite's own SQL under CP-4), pinned as a golden line with a register row.
  - Studio's ordinary generate-plan answer is proven byte-identical by a planted request.
- **M5, met.** A4 is gated on the 5 parity fixes and the both-judges PCT lane. Its done-when adds both corpus rosters
  byte-identical, and S131 stays out.

**Should-fix and minor:** S1-S4 and N1-N3 are folded in:
- the row-6 strands are in B4 with their counts;
- §4a has the reach, the H2 literal-0 exemption (conditional on a measurement), the bounded header with its client
  test, and the load-time report;
- row 1 is corrected;
- the size reads "about 3 weeks; 2 at best; 4 if the product's gates slip";
- the 22 is split into 12 and 10;
- H6 §6.3 is marked CORRECTED;
- the amendment rides A3's PR;
- B5 keeps refusing the other flags by name;
- warehouse query cost is listed as not measured.

**Verdict: stage A is ready for the user to ratify.** One decision is the user's at ratification: the detector's
default. My view stands: warn, with the load-time report; refuse by default if the report is not built.
