# Final review A: the unreviewed delta `d791f5242..6dde398b0` (step 6's closing batch, after the two audits)

Read-only review of worktree `/home/user/ll-final` at `6dde398b0`. I read the code and ran nothing. Every finding comes from
reading the code and is marked with my confidence. The delta to `core/src/main` is 14 files. `UserCallInliner` and
`OrderView` are not in it: their last change predates `d791f5242`, and review 3 covered them.

---

## BLOCKERS

### X1. A list side that reads the row is now tested for emptiness by its declared multiplicity
A per-row scalar gets `LIST_LENGTH`: a wrong TRUE on SQLite, and a raw database error elsewhere where there used to be a
named refusal. This came in with the fix for review 3's S3. Medium confidence; the shape is narrow; read, not run.

**Where:**
- `core/src/main/java/com/legend/lowering/Lowerer.java:3839-3845` (`none`).
- `Lowerer.java:3816-3821`: the read path does the same thing for a computed many side.
- Both call `Scalars.emptiness` (`Scalars.java:2492-2499`). That function decides "list" by `listValued(spec)`, which is
  the declared multiplicity (`Scalars.java:2506-2508`), and writes `COALESCE(LIST_LENGTH(x), 0) = 0`.

**Why it misfires:**
- The same package already knows a declared-many side can be one SQL value per row:
  - `Equalities.mayHoldNoValue`'s comment (`Equalities.java:78-82`): "a row's column read under a forAll over the rows
    is typed many, measured 2026-11-27".
  - The empty ladder (`Equalities.java:129-136`) therefore decides by the SQL fact (`aList(other)`), not by the
    declaration.
- `none` lacks that check. It is reached exactly where the planner has no statement for the side, which is the per-row
  case (`RootComparison.java:200-203`: null for a side that reads a lambda-bound name).

**Expression:** `x::sameNums(a: Number[*], b: Number[*]) { $a == $b }`, called over a table with `I INTEGER` and
`F DOUBLE` columns:
`#>{db.T}#->filter(r | #>{db.T}#->forAll(o | x::sameNums($o.I, $o.F)))`
(or the `exists` form, or `x::sameAnys` over two typed columns).

**Trace:**
1. `LiteCrossTypeRule.atTheCall`: Integer and Float, both lower bound 0, so the call becomes a `TypedRunTimeComparison`
   with `neverEqual` (`LiteCrossTypeRule.java:179-181`).
2. `runTimeComparison`: `bothMayBeEmpty`, so `sameOnlyWhereBothEmpty(none(I), none(F))`.
3. `none`: `isMany`, the planner returns null, so `COALESCE(LIST_LENGTH(I),0)=0`.

**Results, by database:**
- **SQLite** (`json_array_length`, `SQLite.java:1762`): a number argument is a JSON number, so its length is 0. Both
  sides read as "none" (`'nn'`) and the comparison is TRUE for I=1, F=1.5. Pure: an exact number and a Float are never
  equal, so false. The `forAll` verdict flips.
- **DuckDB** (`len`, `DuckDb.java:728`) and **H2** (`CARDINALITY`, `H2.java:1473`): `len(INTEGER)` and
  `CARDINALITY(INTEGER)` are binder or conversion errors. At `d791f5242` this shape was a named `NotImplementedException`.
- **DuckDB, VARCHAR columns:** `len(VARCHAR)` is the string length, so `''` reads as "no value".

**Tests:** no test covers this path. Every `RunTimeComparisonTest` case uses lists the planner can read
(`RunTimeComparisonTest.java:286-320`).

**Fix:** do what the empty ladder does: `lowered instanceof TaggedList || aList(lowered) ? Scalars.emptiness(...) :
isNone(lowered)`, in `none` and in the read path. Add a test for the relation `forAll`/`exists` form on DuckDB, H2 and
SQLite.

### X2. The door-coverage guard cannot fail for the change it says it catches
High confidence.

**Where:** `core/src/test/java/com/legend/edge/UpstreamTiesTest.java:367-381`
(`everyComparisonTheProductRefusesOverColumnsIsTheDoorsOwnAtADoor`).

- **What it claims** (its javadoc, and row C7 of the record): "A comparison added to the product's refusal with no row
  at a door fails here."
- **What it does:** it iterates a hard-coded list of 8 written comparisons. It never reads `HeldKinds.READ`
  (`HeldKinds.java:62-78`) or the doors' rows (`UpstreamTies.java:52-74`).
- **A change that should fail it and would not:** add `Pure.AT_COLLECTION_INDEX_OF` (row B20 says `indexOf` has no door
  row), `compare`, or `max`/`min` to `HeldKinds.read()`. The doors would then refuse those comparisons over two column
  types, and this test stays green.
- **What it does catch:** a door losing its row for one of the 8 listed comparisons.

**Fix:** check `READ.keySet()` against each door's table rows (it needs a package-visible accessor or a test in
`lowering`), or build the query list from `READ`.

---

## SHOULD-FIX

### S1. B1: the per-row refusal was removed, so a known wrong answer is back while the record says "refused"
High confidence on the behaviour.

**Where:** `SQLite.java:1916-1921` and `MariaDb.java:2138-2144`, now without the refusal; `SameValuesForm` lost
`refuseTextOnlyLists`.

**What happened:**
- The audit's complaint was that the refusal also caught WRITTEN lists. The author removed the refusal entirely instead
  of narrowing it to sides that read the row.
- The planner already knows when a side reads the row: it is the same branch that refuses maps,
  `RootComparison.java:179-195`.

**What that changes:**
- `|[1, 2]->map(x | [$x->toDecimal(), 2.0D] == [1.00D, 2.00D])` on SQLite, MariaDB or MySQL is back to `json(a) = json(b)`
  over texts: `[false, false]`, where Pure answers `[true, false]`.
- The test case that pinned the refusal was deleted (`RootComparisonTest.java:493-541`). Nothing records the wrong answer
  that replaced it.

**What the record says:**
- The user ruled B1 "per row refused okay".
- `docs/records/database-recheck-2026-11-26/START_HERE.md:62` (D8 (c)) still says step 6 "refuses it by name elsewhere",
  which is now false for Decimal, Float and date lists.

**Needed:** either narrow the refusal (per-row sides on a JSON-list database), or take the reversal to the user, with a
register row and a pinned wrong-answer test.

### S2. The doors' hour/minute rule applies inside a store query at legend-engine's door, unmeasured
Medium confidence that it differs from legend-engine.

**Where:** `Equalities.upstreamsPartialDateTime` (`Equalities.java:284-300`), reached from
`Scalars.upstreamsComparison` (`Scalars.java:3874-3875`).

**What the code does:**
- Against "any other date-time" (a TIMESTAMP column included) it answers `neverEqualButEvaluated`.
- So `filter(r | $r.ts == date(2015, 4, 16, 14))` at legend-engine's door returns no rows.

**What was measured, and what was not:**
- The upstream run that justified the rule (group D) was outside any store query: legend-engine's own Java.
- Review 3's B2 said: "Inside a store query at legend-engine's door the new false was never measured upstream". The
  record does not answer that.

**Inconsistent with:** the empty-side rule at the same door, which follows where the comparison runs
(`EngineTable.emptySideIsFalse = outsideStore()`).

**Needed:** run it on the engine runner. Then either key the rule on `outsideStore` as well, or add a register row.

### S3. The edition-read guard can be bypassed without asking `Edition` a question
High confidence on the bypasses.

**Where:** `ArchitectureTest.java:3645-3709`. The rule matches only accesses whose target owner `isEquivalentTo`
`planner.Edition` or `lowering.EditionTable`, and it exempts every class in `com.legend.lowering`.

**Changes that decide by the door and would not fail it:**
- in `planner/RootComparison`, `if (edition instanceof LiteEdition)` or `== LiteEdition.INSTANCE` (no method call);
- a call through the concrete type: `LiteEdition.INSTANCE.stampsTestBodies()`, or `LiteEditionTable.INSTANCE.lowersAs(...)`
  (the target owner is the concrete class);
- a new public method in `com.legend.lowering` that returns `styled.edition().lowersAs(call, f) != null` and that the
  planner calls (laundered, and the lowering is exempt).

**Also:**
- The planner still reads other per-door two-forms: `ActivityTexts`, `RoutedQueries`, `ExecutedValues`
  (`PlanAllocations.java:187,258`, `FramePlanner.java:136`). "The planner knows nothing of doors" holds only for `Edition`.
- The javadoc's named limit covers part of this. The record (C7) should say which doors stay open.

**Does it bite?** The planted class does make it fail (`theEditionReadRuleBitesAPlantedReader`).
`everyListedEditionReadIsStillMade` is sound as a shrink check; nothing stops entries being added except review.

### S4. The audit's "all places" fix has a hole at `exists` and `find`
High confidence on the mechanism; an unlikely shape.

**Where:** `Lowerer.java:447`: `asCondition(LambdaBinding.last(test), () -> LambdaBinding.lowerNativeArgs(n, ...))`.

**What goes wrong:**
- Places are counted over the lambda's last statement only (`countPlaces(condition, ...)`, `Lowerer.java:378`).
- The supplier lowers ALL of `n`'s arguments, including the source list, while the marks are on.
- So a node shared by reference between the list and the lambda keeps its mark at a value place.

**Example:**
- Definition: `function p::g(b: Boolean[1]): Boolean[1] { [$b]->exists(x | $x == false || $b) }`
- Call: `filter(o | p::g($o.nick == 'a'))`.
- Lite: `[NULL]` and a NULL predicate, so the row is dropped.
- Pure: `[false]->exists(x | true)`, which is true.

**Fix:** count places over `n` there.

The rest of the fix is correct (see N3).

### S5. S4's claim "no text is made of a value" is true for the folds only
Medium confidence.

- `Equalities.neverEqualButEvaluated` (`Equalities.java:222-228`) and `neverAmongButEvaluated` (`:256-266`) still write
  `CAST(single AS VARCHAR)` for single values of any type.
- That includes an `Any` value that may be an instance, which design 5.7 forbids. Review 4 flagged it under
  invariants 3/3a.
- This predates the delta. The record (S4, "No text is made of a value") overstates what was fixed.

### S6. Tests that cannot tell the fix from the old code, or that only read SQL text
- `RootComparisonTest.aMapNoStatementComparesIsRefusedByName` (`:500-521`): the per-element case accepts ANY
  `NotImplementedException` or `DialectCapability` (`:509`). Review 3's note 7 is not addressed.
- `RunTimeComparisonTest.java:298`: the new case (`sameNums([1, 2]->filter(..), [1.5, 2.5])` is false) traces to the
  same SQL at `d791f5242`. It is a pin, not a test of the delta.
- `UpstreamTiesTest.aDateBuiltToTheHourIsComparedByItsWrittenPrecisionAtTheDoors` (`:385-409`) checks SQL substrings,
  never an answer. The "true where the two are written alike" is never run.
- `OneValueWithinOneTypeTest.aWrittenYearEqualsAComputedOne` runs on the platform database only. The record says three
  databases.
- Commit `fba1bd300` says "not yet run". The record shows no run of the touched test classes after it. Only the
  acceptance measure at `534c788d3` and probes are recorded.

### S7. The S5 fix (`OrderedAsserts.java:74-85`) changes door verdicts that nobody has listed
- `assert(t1 == t2)` over two relation-typed sides at the doors is no longer read as the any-order assert. The old
  `UpstreamTiesTest` case (`assert(rows == rows)` over `#>{T}#->select(~[ID])`) was deleted, not inverted.
- Review 3 said this "needs the user's ruling". The record states the outcome but not the ruling.
- `RootComparison`'s own comments say a frame's values and a navigation's rows are relation-typed too. Upstream bodies
  that `assert(<frame values> == [...])` over unsorted rows would move as well.
- Not measured. List it from the corpus and PCT rounds.

---

## NOTES: checked and found right (read, not run)

**N1. The new partial-date branch** (`Equalities.java:151-162`). I found no wrong TRUE.
- It fires only when exactly one side is a year or year-month LITERAL. The other side must be typed the abstract `Date`
  and carried as `TEMPORAL_TEXT`. The guard `p0 == null || p1 == null` keeps two literals of two precisions false.
- The texts cannot collide across precisions:
  - a year prints `YYYY`, a year-month `YYYY-MM`;
  - `date(y)` and `date(y,m)` are LPAD-built (`DateCtorRule.java:129-145`);
  - a day prints `YYYY-MM-DD`;
  - a date-time text has a `T`.
- Each case asked about:

  | Expression | Answer | Why |
  |---|---|---|
  | `%1973 == date(1973, 11)` | `'1973' = '1973-11'`, false | |
  | year vs `date(2015,4,16,14)` | constant false | typed `DateTime`, not `Date` (`Pure.java:1134`), so it never reaches the branch |
  | year vs a sub-second value typed `Date` (`adjust`'s result) | false by text | |
  | year vs a column typed `Date` | constant false, as before | carried as DATE or TIMESTAMP, never `TEMPORAL_TEXT` |

- **Remaining FALSE-wrongly cases (older, low):**
  - spelling mismatches: `Year.canonicalText()` is unpadded (`PureDateLiteral.java:406`), so `%73 == date(73)` compares
    `'73'` with `'0073'`;
  - `LPAD` cuts years of five or more digits;
  - a year read through a list (`[date(1973)]->at(0)`) loses `TEMPORAL_TEXT` and stays false.
- **At the doors:** the same branch now answers TRUE. That is legend-pure's answer (`AbstractPureDate.equals`).
  legend-engine's answer inside a store query is not measured (S399 says it refuses partials in SQL). List it as a door
  answer that moved.

**N2. The B2 fix** (`Equalities.java:284-306`).
- What it handles correctly:
  - it reads the WRITTEN constructor parts;
  - `date(2015,4,16,14) == %2015-04-16T14` gives `text = '2015-04-16T14'`, so true, in either operand order;
  - two different precisions are never equal, both sides read;
  - two built sides are compared as texts;
  - the fractional-second constructor is compared by its instant;
  - a zone-shifted literal is still compared by its written text.
- Small gaps:
  - a constructor behind an upcast or a seeded let is not recognised, and is compared by the instant (true where upstream
    says false);
  - for years below 1000, `canonicalText` is not ISO-padded;
  - sub-second digits at legend-pure's door: by S399's own reading of `AbstractPureDate`, `.5` is not `.50`, but
    `date(...,15.5) == %...T14:30:15.50` is true at the doors (by the instant). Record it beside section D's
    literal-pair item.
- When `built == 1`, the operands' order in `EQUAL(args[built], literal)` is the reverse of `n.args()`, so
  `emptySidesAsPure` pairs each SQL operand with the wrong typed side. Harmless today (both are `[1]`), but fragile.

**N3. `asCondition`, apart from S4** (`Lowerer.java:365-413`).
- Correct inside a condition.
- `TypedSubst` substitutes the argument node by reference (`TypedSubst.java:126-127`). So `p::either`'s two reads
  really are one node, and the new `EmptySideEqualityTest` case can tell the fix from the old code.
- Guarding more never changes a positive condition's answer (NULL and FALSE reject alike).
- Marks are removed in `finally`, including on the isolate retries.
- `whereLambda`'s negation (`:5117-5122`): for a guarded equality it is "is distinct from".
- `neverEqualButEvaluated`'s COALESCE also matches `guardedSides`, but its negation is right.
- I checked DuckDB and H2 `concat()` (`AnsiSqlRenderer.java:1294-1314`, never NULL) and `||` (NULL): both give the
  right answer.

**N4. `SideEqualities.everyValueRead` and `sameOnlyWhereBothEmpty`** (`:171-204`) are equivalent to the old forms.
- `valueRows` drops NULL values in its WHERE (`:1464-1467`, `:1485`), which also evaluates every value. So the `'n'`
  arm is dead and the fold is NULL only for zero rows.
- The concat form `'nn'` is never NULL, so it is safe under `!` and `forAll`.

**N5. The `runTimeComparison` reorder** (`Lowerer.java:3806-3825`). When both sides may be empty, each side is now
evaluated by `none`, a list through the planner's fold, a harmless list included. That is more evaluation than before,
and no answer changes, apart from X1.

**N6. `drawsAfresh`** (`RootComparison.java:209-229`) fixes S2: lets the comparison never reads no longer count.
- Residual 1: one seeded let reading another seeded let is not followed.
- Residual 2: a comparison that DOES read a draw still returns `unplanned`, which skips `refuseUnordered`. That is a
  narrower invariant-4 fallback than before.

**N7. The map refusal** (`RootComparison.java:113-121`) closes the architecture audit's disagreement 3, at the root and
inside an expression, at lite's door. Not verified: whether `$map == []` (an empty side whose plan types no column) now
reaches `values(...) == null` and is refused, where the empty ladder used to answer it.

**N8. The rename `NUMBERS_OF_THREE_TYPES` to `UPSTREAMS_COMPARISON`** changes no behaviour.
- Every reference is renamed; no `valueOf` or name string remains.
- The `ArchitectureTest` allowlist entry was renamed with a dated reason (`:1445-1448`).
- But `NamedRule`'s own contract (`NamedRule.java:6-8`) says a rule is named "by WHAT IT WRITES and never by who wants
  it". The new name names its audience. The user ruled the rename; the contract sentence is now false until step 6B.

**N9. `EngineTestsStyleTest.anAssertOfEqualsOverAnUnsortedListIsTheAnyOrderAssert`** can tell the fix from the old
code: two tables were rewritten before and are as written now. The rewrite runs on the typed body before the store is
resolved, so a store-mapped list is still a list there and is still rewritten.

**N10. Two written lists on JSON-list databases** are compared again (`twoWrittenListsOfFloatsOrDatesAreCompared`). This
is right for the Floats and dates listed. Two Decimal lists at two scales remain wrong there (recorded, B15 group).

**Not verified (cannot be settled by reading):**
- whether `$o.COL` under a relation `forAll`/`exists` is typed `[*]` (X1 rests on the code's own comment saying so);
- SQLite's `json_array_length` on an integer (from its documentation: 0);
- legend-engine's answer for an hour-built date against a column inside a store query (S2);
- whether `LIST_LENGTH` in place forces each element of a computed list on every database (a NULL where Pure raises, if
  a database computes the length lazily);
- the touched test classes' outcomes at `6dde398b0`.
