# Independent review of step 5's batch (2026-11-26)

*An independent reviewer (given the diff and the rules, not the author's conclusions) read `9c751659..1d5b6374` plus
the two guard commits: the `toDecimal` row, the plain parses as rows, `isNumeric` per database, `acos`/`asin`
raising. It built nothing; database answers are its own runs on the pinned jars (H2 2.5.252, DuckDB 1.5.6, SQLite
3.53.1). This is its report, set out, with what was done about each finding.*

## Blockers (both fixed before the batch's verification run)

1. **A product test still expected `acos(2.0)` to answer NaN** (`PlanMakerTest.assertNoNumberPrintsAsPure`, which
   `PlanMakerH2Test` calls too). The product raises now. *Fixed:* the test's NaN is a parsed text's
   (`'NaN'->parseFloat()`).
2. **`CAST_INT` and `CAST_FLOAT` were not in `SqlFn.RAISING`,** so a failing parse could disappear: until this batch
   the lowering wrote `PARSE_INT` (a raising function) and a cast from text (treated as raising), and the edge's pass
   rewrote them only when the SQL was written. With the rows the lowering writes the two new functions, which the
   pruning and the common-subexpression passes took for functions that raise nothing. Failing input: at pure/v1 on
   DuckDB, `extend(~a: r|$r.S->toOne()->parseInteger())->select(~ID)` over `S = 'abc'`: the unread column could be
   pruned and rows returned where the database refused before. *Fixed:* both are in the raising set.

## Should-fix

| # | Finding | Done |
|---|---|---|
| S1 | the deleted pass also unguarded a user's `cast(@Float)` of a String; inside a store query at a compatibility door that cast is the checked parse now, and no record said so | recorded in register S424 (legend-engine's own `processCast` writes no conversion at all, so neither form was its answer) |
| S2 | the parse rows answered on every database, including ones where a cast refuses nothing: SQLite's `CAST('abc' AS INTEGER)` is 0, MariaDB's and MySQL's the same with a warning; legend-engine has none of those databases | the rows answer on the eight databases legend-engine has; elsewhere the call keeps the checked parse |
| S3 | three records claimed more than was measured (S119's "pass on every database"; "keeps the bare function"; the differences page's `acos` row named no database) | corrected |
| S4 | at legend-engine's door a NULL argument of `acos`/`asin` answered NaN on DuckDB, Postgres and Snowflake (their guards' ELSE arm; H2's was fixed 2026-11-06) | the three guards test for NULL as H2's does |
| S5 | recorded files that follow the build (channel B's outcomes, the edge's answers, the size pins); four orphaned comments in channel B's ledger | regenerated with the batch; the comments removed |

## Notes

- `PureText.engineAnswers` was dead in main: deleted, its one test reads the set.
- H2's clamp inside the in-domain form may be unneeded (the reviewer could not reproduce H2 folding the untaken
  branch for the guarded form); harmless, left.
- The plain-decimal pattern held a backslash, which some databases' literals escape: written `[.]`.
- Four dialects' `call` methods are exactly at the 250-line limit (Snowflake, MongoDb, Postgres, BigQuery): the next
  arm must go into a helper.
- Not verifiable without a lane: whether Snowflake's or BigQuery's compiler folds `ACOS(2.0)` in an untaken branch
  and raises the database's error instead of lite's.

## Checked and found sound

Every dialect's arms for the five new functions (eleven SQL dialects, MongoDb, MySql's delegation); the `acos`/`asin`
rule at exactly 1 and -1, NULL, NaN, Integer and Decimal operands, and its message; the table's new shape (the same
sets for `toOne` of empty and the corrected `substring`, the two `isNumeric` sets disjoint, no call that answered
before refused except as intended); lite's own doors unchanged by the three rows; the upstream templates at the pin;
each guard edit.
