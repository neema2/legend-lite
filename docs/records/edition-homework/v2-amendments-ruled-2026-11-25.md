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
