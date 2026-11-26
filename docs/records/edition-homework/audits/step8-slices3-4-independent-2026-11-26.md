# Audit: step 8, slices 3 and 4 (`git diff 4d44bf54 90d2759b5`)

**Verdict: 2 BLOCKER, 4 SHOULD-FIX, 5 NIT.**

I read every main-source hunk, every guard hunk, the register rows S428 and S429, DIFFERENCES sections 23 and 24, and
both slice records. I ran no build and no test.

The deletions are clean: `FLOAT_SHORTEST_TEXT`, `FLOOR_RAW`, `FloatShortestText`, `PureDateTimePrint`, the twelve
`floatToText` overrides, MongoDB's `dateTimeFormat` override and the spelling hooks leave no code reader. No `default ->`
was added; `NamedRules.lower` stays an exhaustive switch.

The new rows are MIR: `SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, USER)`, rendered by each dialect's own Cast arm. No SQL
text or function name is spelled in lowering, and no database is named outside `PureText`. The new set
`ENGINE_FLOAT_LITERAL_AS_DOUBLE = ON_H2` aliases an existing line, so `DialectBoundaryTest` is unchanged and its count
did not go up.

For a Float-typed operand inside a store query on the eight databases, the Float row writes exactly what the deleted
pass wrote. `shortestDigits` is byte-equal to `CellText.carriedFloat`; the H2 double cast and the `FloatLit` finite
check are the same. So no answer moves there.

The problems are where the code does less than the register says, or where the register is silent about an answer that
moved.

---

## BLOCKERS

### B1. At the body door, a let-bound computed DateTime's `toString` is never lifted (`UpstreamPrints.anyPrinted`)

- **Where:** `core/src/main/java/com/legend/edge/UpstreamPrints.java`, `anyPrinted` (about line 133): `side(n, true, List.of())`.
- **Cause:** `PrintedRelations.printed` now lifts a DateTime only when `Lets.bound(operand, lets)` is a
  `TypedNativeCall` or a `TypedCast`. With the empty list a `TypedVariable` stays a variable, so `anyPrinted` answers
  false and `read()` returns the body untouched before `substituted` (which does pass the lets) ever runs. A Float is
  not affected, because its check reads only the declared type.
- **Input** (PCT channel B, or the corpus's test code; DuckDB):
  `{|let d = %2014-01-01T10:20:30.250+0000->adjust(1, DurationUnit.HOURS)->cast(@DateTime); assertEquals('2014-01-01T11:20:30.25+0000', $d->toString());}`
  - Base: `PureDateTimePrint` wrote `2014-01-01T11:20:30.25+0000`, so the assert passed.
  - New: not lifted, so lite's `2014-01-01T11:20:30.250000000Z` is compared, and the assert fails.
- **Inconsistent by construction:** the same statement IS lifted if any other print (a Float `toString`, a relation
  print) appears anywhere in the body.
- **Contradicts the record:** S429 says "where the door can read the operand as a computation (a native's call or a
  cast, where it stands or through its `let`)". DIFFERENCES section 24 says "directly or through a `let`".
- **Not tested:** the query door (`PrintedRelations.lift`, which passes `body.subList(...)`) is tested through a let.
  The body door is tested only with `now()` through a let, which is left anyway.
- **Fix:** make `anyPrinted` carry the statement prefix, accumulating `TypedLet`s as `read` does, or resolve the
  variables against the body's lets. Add the input above to `UpstreamPrintsTest`.

### B2. A last-step `format('%r'|'%s'|'%f')` or `toRepresentation` of a computed Float at legend-pure's doors lost legend-pure's text, and no row says so

- **Where:** `edge/EdgePureOnlyTexts.java` (the deleted Float branch) and `edge/PrintedRelations.printed`, which lifts
  only `toString`.
- **Inputs** (PCT channel A, DuckDB, each as the query's last statement):

  | Input | Base | New | legend-pure |
  |---|---|---|---|
  | `\|format('%r', [1.0/3.0])` | `0.3333333333333333` (`FLOAT_SHORTEST_TEXT`) | `0.333333` | `0.3333333333333333` |
  | `\|toRepresentation(0.1 + 0.2)` | `0.30000000000000004` | `0.3` | `0.30000000000000004` |
  | `\|format('%s', [1.0e-5 * 1.0])` | `1e-05` (the cast) | `0.00001` | `0.00001` |
  | `\|format('%s', [1.5e-8 * 1.0])` | `1.5e-08` (the cast) | `0.0` | `0.000000015` |

  The last row loses the value entirely.
- **What was ruled:** the user ruled the draft row in `moves-if-float-text-used-further-is-lites.md`, which reads
  "a Float's **text** at a LAST STEP ... is legend-pure's, written in Java". It names `format`'s `%s`/`%f`/`%r` and
  `toRepresentation` in the same breath.
- **What the final row says:** S428 narrows this to "a Float's `toString` at a LAST STEP". It gives only a used-further
  `format` example and says nothing about last-step `format` or `toRepresentation`.
- **The user-facing page misleads:** DIFFERENCES section 23 says lite's text applies when you "go on computing with the
  text", which implies a last-step `format('%r')` keeps legend-pure's.
- **Known but not carried:** slice 3's record lists "a last-step `format` lift" under "later slices", but the interim
  answer is in no register row.
- **Not tested:** `PctPlanRoutePrintTest` has no `format` or `toRepresentation` case.
- **Fix:** either lift these at a last step (as `toString` is), or amend S428 and DIFFERENCES section 23 to say that
  last-step `format` and `toRepresentation` of a computed Float are lite's six decimals, and take that to the user
  (the ruled wording says otherwise).

---

## SHOULD-FIX

### S1. S428 does not name the Float cells of a list's or a relation's text

- **Where:** `lowering/ProductRelationText.valueText` and `edge/PureListTexts` both use `Scalars.floatRepr`
  (`FLOAT_TO_TEXT`). At base the edge pass rewrote those cells to the database's cast; now they are lite's.
- **Input:** a relation with a Float cell of `1.0E-5`, `->toString()`, joined further:
  - base: `1e-05` (DuckDB) or `1.0E-5` (H2);
  - new: `0.00001`.
- **Evidence the answer moved:** `FloatTextTest.anEdgeTableTextUsedFurther...` was flipped from `assertNotEquals` to
  `assertEquals` against lite's text.
- **The gap:** S429 names "a list's or a relation's text used further" for a DateTime; S428 has no such clause.
- **Fix:** add it to S428 and DIFFERENCES section 23. Also add the integer-held Float's `%r` (base: its digits,
  `1000000`; new: `1000000.0`).

### S2. The DateTime row answers on six databases where its text was never run beside legend-engine's

- **Where:** `EngineTable.DATE_TIME_TO_STRING` on `PureText.ENGINE_DATABASES`.
- **The conflict:** `PureText`'s own section rule reads: "a set grows only when a lane has read legend-engine's SQL
  for the function on the database and run it beside lite's rule". Only DuckDB and H2 were run. Slice 4's first
  version had `ENGINE_DATE_TIME_CAST_READ` for exactly this reason.
- **Why it matters for DateTime and not for Float:** the Float row is no change (the base already cast on all eight).
  The DateTime row replaces the `+0000` composition on Postgres, Trino, Snowflake, ClickHouse, Databricks and BigQuery
  with a cast whose text is unknown. It could carry a `+00` suffix on BigQuery's TIMESTAMP, Snowflake's session
  `TIMESTAMP_OUTPUT_FORMAT` (often milliseconds), or nine digits on ClickHouse `DateTime64(9)`. That text may not be
  legend-engine's where lite's SQL type differs. S429 says "not measured".
- **Fix:** run the server and cloud lanes before landing, or narrow the row's set to the two databases measured until
  they are.

### S3. A year outside 1000 to 9999 in the Java print is recorded only in the slice record

- **Where:** `edge/PureValueText.dateTime` uses `toNineDigits`, which writes the year bare.
- **Input:** a computed DateTime in year 999 at a last step:
  - Java writes `999-01-01T...+0000`;
  - the deleted SQL (STRFTIME `%Y`) padded it on some databases, so the answer may have moved;
  - legend-pure's own padding was not checked.
- **The gap:** slice 4 says "Left, recorded", but S429 does not carry it.
- **Fix:** read legend-pure's print for such a year and either pad, or add the difference to S429.

### S4. At legend-engine's door, inside a store query, the other DateTime texts moved and are not stated

- **What moved:** on the eight databases, a computed DateTime's `format('%s', ...)` and its cell in a list's or a
  relation's text went from `...25+0000` (base, `PureDateTimePrint`) to lite's `...250000000Z`.
- **The gap:** S429's "every other text of a computed DateTime is lite's own" paragraph is scoped to "At legend-pure's
  door". At legend-engine's door it covers only "off the eight databases" and "outside a store query".
- **Fix:** add one clause to S429 and DIFFERENCES section 24. legend-engine has no `format` in a store query, so no
  engine answer is lost.

---

## NIT

1. **Stale comments.**
   - `ArchitectureTest.PLANNER_ENTRIES` says "a DateTime's that is no literal", and `core/edge_tests.bzl` says
     "where it is no literal". The row answers literals too since the ruling.
   - Slice 4's "The tests" section still describes `EngineDateTimeTextRowTest` as keeping a literal as written. The
     test (correctly) asserts that a literal is cast.
2. **`docs/dialect-capability/reach.tsv` still lists `FLOOR_RAW` (11 lines, 55 cases each).** It is generated from a
   PCT run and was not regenerated. Harmless, but stale.
3. **The javadocs disagree on the product's DateTime digits.**
   - `NamedRule.DATE_TIME_TEXT_BY_CAST` and S429 say "nine fraction digits and Z on every database"; MongoDB's product
     format is `%L000Z` (six).
   - `SqlFn.DATETIME_TO_TEXT`'s javadoc still says six; `DateTimeText` says nine.
4. **A runtime with no single database (`database == null`) gets lite's text silently** for `toString`, where
   `AS_GIVEN` rows refuse by name. This matches the `WHERE_WRITTEN` pattern, so it is acceptable. An invariant 4 reader
   may want a comment.
5. **No core test runs the Float row against a database.** `EngineFloatTextRowTest` compares SQL text only. The
   DB-run answers (`1.3421E-8` on H2) were removed from `FloatTextTest` and now rest on PCT's legend-engine lane and
   the corpus alone. A DateTime twin of that DB-run test exists (`EngineDateTimeTextTest`); a Float one does not.

---

## Answers that move, and whether the records and register carry them

| Door / place | Expression | Base | New | Carried? |
|---|---|---|---|---|
| legend-engine's door, in store, 8 DBs | `$x.f->toString()` (Float) | cast | cast | n/a (unchanged) |
| same | Float literal `1.3421e-8->toString()` | bare / H2 CAST DOUBLE | same | n/a |
| same | `$x.n->toString()`, n: Number/Any holding a Float | cast (pass) | lite's 6 decimals | S428 yes |
| same | `format('%s'/'%f', [$x.f])` | cast | lite's | S428 yes |
| legend-engine's door, SQLite/MariaDB/MySQL/MongoDB | Float `toString` | cast | lite's | S428 yes |
| legend-engine's door, outside a store query | Float `toString` | cast | lite's | S428 yes |
| legend-pure's door, used further | `(0.1+0.2)->toString() + 'a'` (DuckDB) | `0.30000000000000004` (cast) | `0.3` | S428 yes |
| legend-pure's door, used further | `format('%r',[1.0/3.0]) + ';'` | `0.3333333333333333;` | `0.333333;` | S428 yes |
| legend-pure's door, LAST step | `format('%r',[1.0/3.0])`, `toRepresentation(0.1+0.2)`, `format('%s',[1.5e-8*1.0])` | legend-pure's / cast | lite's (`0.333333`, `0.3`, `0.0`) | **NO (B2)** |
| legend-pure's door | relation or list text with Float cells, used further | cast (`1e-05`) | lite's (`0.00001`) | **NO in S428 (S1)** |
| legend-pure's door | `%r` of an integer-held Float column | `1000000` | `1000000.0` | implicit only (S1) |
| SQLite edge | `toRepresentation(1.0e20)` | `9223372036854775807.0` (wrong) | `100000000000000000000.0` | slice 3 record and test; register implicit |
| legend-engine's door, in store, 8 DBs | DateTime literal `toString` | written text and `+0000` | DB cast `2014-01-01 00:00:00` | S429 yes; PCT 3 tests, LITE_FIXES to BOTH_FAIL |
| same | computed or column DateTime `toString` | `...25+0000` | DB cast | S429 yes (6 DBs unmeasured: S2) |
| same | computed DateTime in `format('%s')`, list or relation text | `+0000` | `...Z` | **NO (S4)** |
| legend-pure's door, last step, native or cast root | `...->cast(@DateTime)->toString()` | `...25+0000` (SQL) | same text (Java) | S429 yes |
| legend-pure's body door, last step through a let | `let d = <computed>; assertEquals(..., $d->toString())` | `...25+0000` | `...250000000Z` (not lifted) | **contradicts S429 (B1)** |
| legend-pure's door | property, user function, used further, `format`, `toRepresentation` of a computed DateTime | `+0000` | `...Z` | S429 yes |
| MongoDB edge | every date-time text | `%L+0000` | `%L000Z` | S429 yes |
| legend-pure's door | year < 1000 computed DateTime at a last step | DB `%Y` (maybe padded) | `999-...` | record only (S3) |
| corpus plan JSON | 3 families x 2 lanes | | +39/+78/+117 plan chars | yes, "step 8 yes" |

`lean-sql-baseline.tsv`: exactly 12 lines, all in the `plan` column, all up. DuckDB and H2 each move +39, +78 and +117,
234 a lane. `statements`, `chars`, `subselects` and `repeated` are unchanged. This matches the record and S428.

Engine-lane files: exactly the three DateTime `toString` tests on each of DuckDB and H2 move, `PASS` to
`FAIL WRONG_ANSWER` and `LITE_FIXES` to `BOTH_FAIL`. The summaries move by exactly ±3 (DuckDB `lite.PASS` 314 to 311;
H2 248 to 245). Nothing else moves.

Caveat: the files record verdicts only. That lite's wrong text equals legend-engine's wrong text (both
`2014-01-01 00:00:00`) rests on the probes recorded in `r2/sql-inventory.md`, not on the lane.

---

## Guard edits

| File | Hunk | Kind | Dated and justified? | Could product code slip? |
|---|---|---|---|---|
| ArchitectureTest | PLANNER_ENTRIES +3 `NamedRule#...` to EngineTable | like-for-like registration (each row's rule is listed) | yes, 2026-11-26 (comment stale: NIT 1) | no; ties each rule to EngineTable |
| SqlFnSortedTest | PRODUCT list -2 names; BOTH_WRITERS: FloatShortestText out of 9 entries, `FLOAT_SHORTEST_TEXT` entry out, `FLOAT_TO_TEXT` and `RTRIM` entries out | shrink and tighten (the edge may no longer write `FLOAT_TO_TEXT` or `RTRIM`) | yes | no |
| EdgeSpellingHookTest | `floatToText` out of every pin; `dateTimeFormat` out of MongoDB's | shrink | yes | no |
| HookRulingTest | SPELLING_HOOKS 5 to 4; the product-default check of `dateTimeFormat` removed | shrink; the check goes because the method is now `private static` and cannot be overridden | yes | no |
| JavaEvalLedgerTest | `PureDateTimePrint.java` entry out; two comments | shrink | yes | no |
| EdgeOnlyNodesCensusTest | comment only | none | yes | no |
| CompatSpellingGuardTest | comments only (scanned symbols kept) | none | yes | no |
| OpenClassMembersTest | comment only | none | yes | no |
| AuditRound3Test | `0.000000013421->toString()` at the edge: `0.000000013421` to `0.0`; DisplayName changed | re-point to ruled behaviour (an edge-style plan without the door's lift); legend-pure's text is now held by `PctPlanRoutePrintTest` | yes | no, but it now pins a lossy text |
| DialectBoundaryTest | not touched | n/a | n/a | the new set aliases `ON_H2`, so the count is unchanged |
| RaiseTextTest | `FLOAT_SHORTEST_TEXT(a)` to `str(a)` | like-for-like (the value side is only checked for being no StringLit) | implicit | no |
| core/edge_tests.bzl | +3 tests | addition | yes | no |
| EngineTablePlacementTest | `AS_WRITTEN` private to package | test visibility | n/a | no |

Re-pointed tests (not guards):

- `FloatTextTest`: its window test flipped from `assertNotEquals(lite)` to `assertEquals(lite)`, which is the intended
  re-point. `anIntegerHeldFloatsTextAtTheEdgeIsLegendEnginesCast` was replaced by
  `EngineFloatTextRowTest.aFloatHeldAsAnIntegerIsCastAsItIsHeld`, which is also SQL-only but covers all eight
  databases.
- `TypeInferenceIntegrationTest`: re-pointed to `0.0`.
- `BigQueryDialectTest`, `SnowflakeDialectTest`, `MongoDbPlanTest`: tests of deleted code removed.

None of these was weakened to hide a regression in surviving behaviour.

Database or Java only:

- Run against a database: `EngineDateTimeTextTest`, `PureRelationTextParityTest`, `PctPlanRoutePrintTest`,
  `PureOnlyTextTest`, `UpstreamPrintsTest`.
- SQL text only: `EngineFloatTextRowTest`, `EngineDateTimeTextRowTest`.
- Java only: `NamedRulesFloatTextTest`, `PrintedRelationsTest`, `PureRelationTextTest`.

---

## What I could not determine without running

1. **A literal the lift cannot see** (the opposite direction of the first review's must-fix). The lift reads the
   tree before inlining; the lowering reads it after. If a DateTime literal under a native call or a cast reaches the
   lowering as a bare `TypedCDate` (for example `[%2014-01-01T00:00:00.00]->toOne()`, or a same-type cast the typer
   or `G½` drops), the base printed its written `.00+0000` (legend-pure's), while the lift now prints `...00+0000`. I
   found no fold that does this, but did not prove there is none.
2. **The cast's text on the six unmeasured databases** (S2), beside legend-engine's text there.
3. **The cell carriers.** Whether every database's decoder hands `PureValueText.value` a `PureDateLiteral` for a
   DateTime column: SQLite's text, MongoDB's date. Anything else is refused by name, which is safe; a lane would show
   whether any lane now refuses where the base answered.
4. **Year padding.** legend-pure's padding of a year below 1000, and each database's `%Y` (S3).
5. **Later folds over the new cast.** Whether any lowering pass after the named rule folds
   `Cast(DecimalLit, VARCHAR)` into a Java-made text. The base had the cast only at render time; it now sits in the
   MIR earlier.
6. **The pass and green claims** in both records (PCT, the corpus, the gates).
