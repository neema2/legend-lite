# C2. M2's census re-run: the product's statement road against the test road (2026-10-31)

The plan's M2 (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B11) gave the product planner the test road's front half
(`planner/ProductStatements`). Its brief promised two measurements: every core test package's sent statements (GATES,
the M2 entry) and M0's parity census re-run, which this file reports.

**How.** M0's research patch (`probes/m0-hooks.diff.txt`, never committed) applied to M2's code (ac813c50), with the
census's product road replaced by the product's own (`probes/M2Census.java.txt`: `productSql` is
`ProductStatements.prepare` -> `constant` / `planned` -> `ProductStatements.lower`, one minter per body, the streaming
root). A second run refined two things: a product constant against the test road's lowered literal is its own class
(`constant-vs-literal`), and every front-half difference is written out in full (M0 kept three examples per key). The
patch was removed afterwards; the tree carries none of it. Pass/fail and the statement-origins census were unchanged
under the patch, but for M0's known artefact (the two DuckDB engine-order registers, `update_rcorpus_duckdb_3/4`, move
because the probe's extra renders count as scan-order firings).

## 1. Where both roads plan

| | chB | duckdb/database | duckdb/host | h2/database | h2/host |
|---|---|---|---|---|---|
| db-side planned | 5,025 | 7,911 | -- | 8,055 | -- |
| ... same | 3,753 | 3,540 | -- | 3,493 | -- |
| ... numbering-only | 622 | 189 | -- | 186 | -- |
| ... diff lowering (the known factors) | 127 | 99 | -- | 11 | -- |
| ... constant-vs-literal | 523 | 451 | -- | 597 | -- |
| ... diff front | **0** | **0** | -- | **0** | -- |
| ... product refuses | 0 | 3,632 (M0 3,694) | -- | 3,768 (M0 3,829) | -- |
| host-side planned | 4,934 | 1,368 | 7,742 | 1,352 | 7,516 |
| ... same | 3,658 | 95 | 2,460 | 95 | 2,390 |
| ... numbering-only | 622 | 4 | 191 | 4 | 188 |
| ... diff lowering | 127 | 6 | 106 | 6 | 17 |
| ... constant-vs-literal | 527 | 203 | 602 | 203 | 589 |
| ... diff front | **0** | 2 | 2 | 2 | 2 |
| ... product refuses | 0 | 1,058 (M0 1,304) | 4,381 (M0 4,686) | 1,042 (M0 1,288) | 4,330 (M0 4,632) |

(Channel B is one run of its five suites with the refined census; M0's channel B columns counted both judges' passes,
so its totals are not comparable row by row. The corpus columns are one pass each, as in M0.)

- **constant-vs-literal**: the product answers a String or Boolean literal as the plan's own constant (D26, MV12) where
  the test road's database side lowers it (`SELECT 'key1' AS value`). By design; the host side folds the same literal
  (`LiteralFold`), so on a host pass these are statements the test road's census pairs before its own fold.
- **diff front**: two statements per corpus pass, each in a body that reads an `execute(...)` frame:
  1. a graph fetch read through the frame: the test road aggregates the objects into one array, the product streams one
     per row -- the known STREAM factor (D7), which the census cannot attribute inside a frame (frames are M3's B10);
  2. `testToSqlStringReplaceTablesPostProcessor`'s `toSQLStringPretty(..., runtimeWithTableReplace([...]), ...)`: the
     test road's text named the replaced tables (`otherFirmTable`, `differentPersonTable`) and the product's did not.
     The test road's renames came from its `assertSameSQL` arm (`SqlTextVerdicts` puts the call's runtime's renames
     in the environment before it evaluates); the product stages the call with no such environment. **Fixed in M2**:
     `EnginePlans.toSqlString` applies the call's own runtime's table renames wherever it is planned (the engine's
     golden in that test names them). On the assertSameSQL arm the two maps are the same map, so its text does not
     move.
- No other front-half difference on any column: every other pair is the same, differs in numbering only, or differs by
  a lowering factor M0 already named (the identity layout, STREAM, TZ, POST, EGRESS).

## 2. The refusals that went (DuckDB host pass; the other columns alike)

| refusal (M0 -> M2) | feature |
|---|---|
| `MappingResolutionException: runtime has N mappings binding class ...` 105 -> 0 | the let bindings in the resolver (D13): the execute call's mapping in scope |
| `lowering not yet implemented for TypedGetAll` 63 -> 0; `scalar lowering ... TypedGetAll` 38 -> 0; `store resolution left getAll(...)` 9 -> 0 | D13 |
| `class query under TypedMap is not resolvable yet` 374 -> 292 | D13 (the rest is D15) |
| `no SQL type for Pure class ...TableAlias` 22 -> 18 | `$plan` handles (D13's plan rows) |
| `scalar lowering ... TypedSerialize` 3 -> 1; `... TypedTestDataGen` 6 -> 1 | D14 staging; the generator stays the test library's (`a test-data generator in a query` 0 -> 5) |

The refusals that remain are M3's and later steps': `execute(...)` read as a value (D15, about 3,500 per pass), the
test-data generator (T8), dynamic slice bounds, and the probe's own render of a keyed pivot.

## 3. A fault in M0's census, found here

The census counted a front-half difference under `diff front` twice when the body had none of the four environment
flags (`count(".diff front" + "")` and `count(".diff front")`). M0 reported zero front differences, so its numbers are
unaffected; M2's first run read 964 for channel B where the refined run, counting each class once, reads 523 constants
and nothing else.
