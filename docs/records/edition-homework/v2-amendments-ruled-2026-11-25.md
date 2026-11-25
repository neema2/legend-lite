# Amendments to design v2, to fold in after the second review lands (2026-11-25, late)

Ruled by the user, in order:
1. "one place for compiler rules and one place for presentation/format java rules would be an okay place to land":
   - meaning: only the native-call site calls the rule set;
   - format: only the door's presenter, chosen by the door, never by the edition;
   - folds call nothing (they stop folding cross-kind comparisons and value texts);
   - forArgument deleted.
2. Placement as a part the typer passes to the one call site. The user's insight: "engine is only for the database part and pure is for the upstream non-database part".
   - Parts: database part (row lambdas, mapping code, graph-fetch-pushed properties); legend-engine's own part (statements reading no store, in-memory M2M); legend-pure's part (the corpus harness).
   - The memo is keyed by (function, rule set, part).
   - A3 is deleted after a shadow comparison on every test (pure/v1 goldens, A3's tests, the corpus).
   - The corpus switch is deleted.
3. The model, confirmed: "yes so pure edition is really just everything pure, engine edition is really engine for database, pure for outside?"
   - The refinement the user accepted: "outside" is compiled legend-pure for the corpus harness (exactly Pure), and legend-engine's own Java for pure/v1's host-side code, which is Pure's meaning except a measured exception list (so far: `==` across number kinds is true, B2's m2m measurement). Each entry is asked of the engine runner and recorded.
   - "great nuance about what outside means".
4. "pure should be compiled legend-pure agreed": `pure` = compiled legend-pure (the PCT reference runtime, core-compiled manifest: 3 known failures); interpreted legend-pure's differences (e.g. 1D == 1) are not followed.

Earlier the same evening (already in v2 §1): engine mode on DuckDB only; lite may answer where engine refuses; the go for R0/R1; the referee kept; PMCD/plans after.

Measurement added: M7, a typed census of governed constructs in the corpus harness (now informational, since the part model handles the harness).

## After the second review (2026-11-25, late), the user's rulings

5. The meaning pass moves from the typer's call site to one pass after inlining and store resolution, before lowering: "moving the pass makes sense". The user asked whether physical space matters. Answer: the governed calls stay Pure calls with Pure types after resolution. The resolver labels provenance (mapping code, constraint, M2M under graph fetch or under TDS). Function boundaries are already gone after G½.
6. "tdsnull is fine": `TDSNull` is a property of legend-engine's TDS type, printed at the edge; `pure` and `engine` both print it; lite's product keeps S405.
7. "comparison in engine database yes good to match": comparisons under `engine` take the database's promotion, including types known only at run time (Any, Number).
8. "pure mode PCT yes i think we have that already in heavy lanes": M6 confirms which lanes.
9. **RULE ZERO**, the user's emphasis: "these modes should not be changing sql - they should be fixing up things at the edge in java except where they absolutely cannot - we dont want to do old way were we had a pure plan that had javas double formatting algo rewritten in sql - why does this really important piece keep getting lost?" And: "pures exact float - why do we keep relitigating this? should be done in java to match".
   - A mode changes the SQL only when the difference changes what the database returns (rows, whether an error happens, a value the query keeps computing with) and Java can't fix it afterwards. Everything else is Java at the edge, from the typed value.
   - The corner: a text the query keeps computing with in SQL is the database's lite text, with the difference recorded. Java's algorithm never goes into SQL.
   - Guard: the SQL under pure/engine equals the product's SQL except at the listed meaning rows.

## R1 results in

- **M1, the Float literal** (B2, 4b6f632bf, docs/records/edition-homework/m1-float-literal.md on B2's branch). With the edge's DECIMAL override removed:
  - PCT A DuckDB: 2 answers move toward compiled Pure, still within tolerance, so no outcome moves.
  - Corpus H2: 2 newly PASS (legend-engine's own H2 literal processor writes CAST(x AS FLOAT), so the edge's DECIMAL on H2 was never legend-engine's).
  - Corpus DuckDB: 0. pure/v1: 0 answers (1 plan text). Engine lanes: 0.
  - One edge assertion moves away from legend-engine on DuckDB (`0.1 + 0.2`: legend-engine 0.3 DECIMAL, lite 0.30000000000000004, Pure's).
  - SQL: compat pins grow (+2 to +4 characters per literal on DuckDB, +26 on H2); the product doesn't move. No new errors.
  - Recommendation: delete; one register row (legend-engine on DuckDB computes Float literal arithmetic as DECIMAL; lite answers IEEE, Pure's way). It fits rule zero: compat SQL equals product SQL.
10. Tenet 1 amended by the user (2026-11-25): "Tenet 1 should say for the PRODUCT, database computes every value in database including toString(), for compatibility everything presentation that can be should be done at the edge as much as possible (excluding toString in middle of chain for example)". Written into AGENTS.md and compat-rules-of-record at 5c9aaa44.
11. RULED by the user, 2026-11-25: "yes delete the float decimal thing". The edge's DECIMAL Float-literal override goes (DuckDB, H2, Postgres, Databricks, Trino; PureText.java:263/310/333/370/481), with one register row: legend-engine on DuckDB computes Float literal arithmetic as DECIMAL; lite answers IEEE, Pure's way. It lands in the first build slice after v3's go (R2), with M1's measured moves as the expected re-pins: corpus H2 +2 PASS, compat lean pins up, the edge test EqualityCompatEdgeTest's 0.1 + 0.2.
- **M3-M7** (docs/records/edition-homework/r1/, 842d2917):
  - **M3:** the corpus with plain raises, 0 verdicts moved, so one `engine` (plain raises for the corpus too).
  - **M4:** engine-mode parity on DuckDB, 1,249 tests: (1) 1,004, (2) 1, (2b) 3, (3) 215, (4) 25, (5) 1 (testBigFloatAbs: ledgered S120, but upstream's DuckDB manifest doesn't list it, so S120's claim is suspect).
    - Of upstream's 47 wrong values, lite reproduces 1; 46 are bucket 3. They split into presentation/type (DateTime precision, Decimal `2` vs `2.0`) and real wrong answers (indexOf and substring off by one, dateDiff weeks, hour/minute 0, pow, half-even rounding, splitPart).
    - That needs the user's ruling: reproduce legend-engine's own known failures, or not.
  - **M5:** the referee decides 1,594 of 1,658 DuckDB SQL-text asserts (1,550 of 1,613 on H2) by matching rows; 12/11 differ; 28 declined; 20 underivable; 3 held; 1 text equal.
  - **M6:** Pure-mode parity; each lane's failures equal its ledger. Reference passes, lite fails: DuckDB 24, SQLite 28, H2 (Relation only) 20. Channel A and B disagree in 36 + 23 tests.
  - **M7:** the corpus harness: 418 tests use a governed construct and 372 reach a verdict. 77 text (Float, date), 1 real cross-kind comparison, substring 3, indexOf 7, toOne 337 (Pure raise). So the harness/query split is real; the pass handles it per planned query.
12. PCT's two runs, the user's framing (2026-11-25, late):
    - "Pure reference implementation, that should match as close as possible to real answers in PCT, i think on DuckDb we are very close if you look at Channel B PCT on Duckdb - and all the things that do not pass are database limitations that we just have to live with eg years outside of range database can handle".
    - "Running the engine version of PCT, I dont think we should try to reproduce all the engine bugs but we also should NOT try to correct in java things that engine does NOT do - so we should stay close to the spirit of what engine does (in engine sql and engine java) so it gives back what the intention of engine was without trying reproducing the bugs".
    - The proposed reading, awaiting the user's confirmation:
      (1) Lite never adds a check or correction legend-engine doesn't do, in SQL or Java.
      (2) Where legend-engine deliberately leaves a decision to the database (unchecked raises, comparison promotion), lite does too.
      (3) Where its translation is wrong (its own manifest lists it as a failure: indexOf, substring's start, dateDiff weeks, hour/minute, pow, half-even rounding), lite writes the correct translation.
    - Evidence for deliberate versus bug comes from legend-engine's code and manifest; unclear cases go to the user.
    - Consequences: `substring` indexing is a bug (the CORRECT_SQL_SUBSTRING_INDEXING flag); out of range is deliberately unchecked in the database part. A3 is deleted.
    - Homework: sort M4's 215 bucket-3 tests (fine: refusal, database error, translation bug; wrong: lite added a check). Confirm every Pure-mode ledger row is a database limitation.
13. The user, 2026-11-25 (night), on keeping lite's checks in `engine` versus full compatibility: "if lite/pure checks something in db that engine does not, like divide by zero as one example, i might even consider keeping all of those checks even in engine ... im just not that excited about changing product/lite SQL that much for either pure or engine - maybe we need an inventory of what sql changes we would make that are not just different typing or different presentation". Then: "if we are going to be doing some rewriting anyway, maybe worth going as full compatibility as possible so users dont freak out - but we should do inventory and see what is cheap vs expensive in the rewrites and make a call".
    - Running: the SQL-difference inventory (typing / presentation / genuine SQL; per genuine row, keep lite's versus full compatibility, cheap or expensive, tests affected; the raises; an appendix on legend-engine's known wrong answers), output r2/sql-inventory.md.
    - Running: the sort of M4's bucket 3 and 4, testBigFloatAbs, and M6's Pure-mode failures (database limitation / lite gap / ruled / channel-A artifact), output r2/pct-sort.md.
14. **The meaning pass's one seam, located (B3, 2026-11-25, night; read in da231244).** Every resolve overload funnels into `StoreResolver.resolve(body, runtime, mapping, ...)`, whose exit walks every resolved statement (`StoreResolver.java:244-251`, `assertNoStoreOnlyEscapees`, invariant 7). The resolver is constructed in one place (`BodyPlanner.resolver`, `BodyPlanner.java:68`). Its callers:
    - `ProductStatements:150` (the product's query path);
    - `BodyPlanner:213` (`resolvedToExecute`: the test body's values and an execute's query);
    - `PlanAllocations:204` (test tooling);
    - `edge/EnginePlans:162` (the edge's SQL-text facet);
    - `FramePlanner:262` (a pipeline check, no resolve).

    So the pass can sit at the resolver's exit, one place every planned statement passes after store resolution and before lowering. The caller supplies the body's role (the program's query, an `execute()` query, a harness value), which is how the corpus's harness/query split reaches the pass. The lowering call sites (`PlanMaker:599,637`, `ProductStatements:350,397`, `BodyPlanner:128`) all lower resolved bodies. Still to check: whether any lowered body skips the resolver (a storeless constant query).

    Also: if lite's checks are kept in every edition, the raises need no pass, and the lowering's implicit guards (`Coercions`, `ToOneGuard`) stay as they are. That removes the second review's must-fix 3 for raises.
15. **The user's rulings on the small questions (2026-11-25, night):**
    - **S7b** "fine": keep refusing a single value on pure/v1 execute until a client needs it.
    - **The `.csv` of a `#TDS` literal** "yes": the product keeps S405 (same as toCSV); legend-pure's door answers Pure's source text, as presentation at the edge.
    - **A raw grid's last-step CSV:** "yes keep lite but in compatibility we dont want to match? arent we doing the csv in java anyway in compatibility?" Answer: yes, compatibility matches legend-engine through the door's presenter, from the columns the run announces (B2's option C), in R4; the product keeps lite's.
    - **toCSV's Float digits** "okay make exact". "Exact" means the database's own full-precision text of the double (its cast), not six decimals, and not Java's algorithm ported into SQL.
    - **ServiceTestRunner:** "sure keep on edge for now, eventually we will need real one in lite". It stays at the edge with legend-engine's exact JSON comparison; a lite-native service test runner is future work (docs/DEFERRED_TEST_EXECUTION.md).
    - **S7a:** not yet answered, asked again.
16. **M2, the H2 settings** (B2, 418553e9d on claude/compat-b2-nufdlk, docs/records/edition-homework/m2-h2-settings.md):
    - `DEFAULT_NULL_ORDERING=HIGH` has no effect on any lane: the H2 dialect writes explicit null placement (`H2.java:1181-1184`).
    - `MODE=LEGACY` matters to 1 corpus test (`graphFetch::tests::isolation::testIsolationOfPropertyTargetFilter`): a store filter compares a BOOLEAN column with an integer (`active = 1`), which H2 REGULAR refuses and LEGACY accepts. Without it, the test newly fails, away from legend-engine (an error appears).
    - PCT A/B on H2, pure/v1's golden net, the H2 engine lane and the edge's H2 integration lane: 0 moves.
    - Two edge tests pin the settings' identity.
    - Recommendation: keep legend-engine's H2 session settings for the `engine` door as connection data. There's no SQL change, it's already built, and it is faithful for user #SQL islands too. PCT (`pure`) uses the product's settings (0 moves).
    - Pre-existing, not M2's: `//pct:pct_channel_b_twice_h2_relation` (a manual lane) fails 4 cases on a clean tree ("DialectCapability: LIST_GET reached a dialect without a list encoding"); needs an owner.
    - M1's addendum: every M1 move is arithmetic, none is a Float's text.
17. **S7a RULED: A** (the user, 2026-11-25, night: "go with A").
    - A checked graph fetch's UNEVALUABLE defect sentence becomes a plain text literal in the SQL, like CONSTRAINT_VIOLATED's (S3b). `SqlExpr.RaiseText.asData`'s unread record goes, and `PlanTypesAreDataTest.PROSE_RESIDUE`'s entry becomes a declared fact: "a defect's sentence, a model constant inside a successful answer".
    - The SQL is unchanged; the database keeps building and streaming the checked JSON.
    - Reasons:
      - B (the writer words defects from typed rows) would make Java touch every checked row on the way out, for no audience difference: both sentences are worded the same for every door today.
      - The user's option D (the message as a plan parameter) was considered. Bound at planning, it only moves the prose into the parameter list. Supplied by the door at run time, it is the recorded fallback if these sentences ever need to differ per audience, because it keeps the database-built JSON streaming.
    - Owner: B2, with a slice after toCSV exact.
18. **H2 settings RULED** (the user, 2026-11-25, night: "yes keep h2 settings"): legend-engine's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` stay for the `engine` door's H2 sessions as connection data; PCT uses the product's. Downsides, as told to the user:
    - two H2 session kinds exist and never serve each other's plans (already enforced, EdgeH2SessionTest);
    - LEGACY accepts SQL regular H2 refuses (`active = 1` on a BOOLEAN), at the engine door only;
    - a future H2 dropping LEGACY would reopen it.
19. **The user's standing word on landing code** (2026-11-25, night): "if you think makes sense to land code, land it dont have to ask me these micro questions". Small, ruled or approved changes land on the program's branches after a full round with `//gates:local` and an independent review, without asking. Never to `main` and never a PR merge without the user's word.
