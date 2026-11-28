# Independent structural review of step 6B's design (`docs/records/edition-homework/step-6b/design-2026-11-27.md`)

Read-only, at `a61329f66` in `/home/user/ll-final`. Nothing was built or run. Every claim below was read in the code
unless it is marked **not verified**. Paths are under `core/src/main/java/com/legend/` unless written otherwise.

---

## (A) Verdict

**No.** Built as written, this design leaves **six to eight** product SQL-level comparison codes, not two (my count is
in B0 below). The design does not build "one comparison code". It builds one *pairwise* code and keeps or adds the
others under new names.

**Only if** these six things change:
1. The "normal form" table has three readings per (kind, holding): the pair test, the key and the order key. A
   property test must tie the three readings together.
2. A comparison that reads the row uses the same shapes code as the whole-side statement, or it is refused.
3. Instance canon text, ordering and ties, negation and TDS cells are given slices.
4. "Not judged here" becomes a typed waiver that the user rules on. It reverses A4.
5. The doors' meaning arrives as a typed call from the typer, not as a policy record passed after it.
6. The "done" checks are behavioural (section E), not ledgers of class names.

### B0. How many codes survive the design as written (item 1)

| # | Survives | Where | Why it survives |
|---|---|---|---|
| 1 | The new element rule (`SameValue`) for a pair inside a row | new | the design's one code. It keeps pairwise rewrites that are not normal forms: the `[]` ladder (`lowering/Equalities.java:126-144`), boxing one value against a list (`:208-216`), `CastPolicy.comparisonWireOperand`'s raw-store-space compare (`:194-198`), `VariantShapes.alignLiteralToJson`'s JSON `=` (`:201`), `EnumSourceValues.decodeInvert` (`:202-207`) |
| 2 | Shapes as a statement over rows | `lowering/SideEqualities` and `sql/dialect/SameValuesForm` | kept. It gains membership and "all in" (2.2), which are the judge's `contains*` and `subset` moved, not deleted |
| 3 | Shapes as an expression over list values | new in B3, plus `lowering/TaggedLists` (`sameList`, `sameListByKinds`, `sameWrittenLists`, `contains`, `dedup`; `TaggedLists.java:600-740`) | 2.2 adds a per-row array form (normalized arrays, then the dialect's list `=`, sort or map entries). 2.1 deletes only the per-channel `=` in TaggedLists. Its list logic stays. This makes a second shapes layer |
| 4 | Instances by canon text | `lowering/InstanceEquality.contains`, `indexOf`, `removeDuplicates`, `isDistinct`, the `==` fallback (`InstanceEquality.java:177-186`, `217-232`); `CanonicalRenderSql.instanceEqualityCanon` | not in any deletion list. Today `==` uses typed leaves (`:168-175`) and `contains` uses `LIST_TRANSFORM` to a VARCHAR canon plus `Membership` (`:224-232`). Inside the value form this is already a pair test and a key that differ |
| 5 | The database's own key for plain lists and for relations | `Scalars.inRule` and `containsRule`'s "the database's Membership" branch; `ListEncodings.orderedDedup` (`Scalars.java:3611`); relational `distinct`, `groupBy`, join keys, window partitions | 2.1 says the table "gives the KEY". No slice changes these call sites, and doing so changes every `contains` and `distinct`'s SQL |
| 6 | Ordering, ties and extremes | `TaggedLists.sort`, `Scalars`' ordering rule and `dateArg`, the extremes (`Scalars.java:~3889-4140`), `MixedEncoding.comparables` | the tagged order key is "every number a DOUBLE, every date a TIMESTAMP" (`MixedEncoding.java:31-36`, `:635-641`). That is a different form from equality's exact channels. No slice moves these sites |
| 7 | Negation of an equality | `lowering/NullSemantics.negate` (`:151-175` per the map; pattern-matches `Equalities.guardedSides`, `Equalities.java:104-109`) | not mentioned. A new `SameValue` node breaks the shape match unless negation becomes structural |
| 8 | TDS-equivalence cells | `VerdictSql.typedTdsEquivalent` and `toleranceCells` (`lowering/VerdictSql.java:796-907`) | kept as "not equality". Its non-numeric cells are compared as **VARCHAR text** with `NULL_SAFE_EQUAL` (`:811-826`, the `C` column is VARCHAR, `:863-907`). Decimals and dates are cast to DOUBLE. That is Pure value equality done by text |
| 9 | The dialects' composite `equalArm`s | `BigQuery.equalArm` and `compositeEquality` (TO_JSON_STRING), `ClickHouse` tuples, `H2` Boolean against String (research map 1G) | B3 deletes "the dialects' text comparison of two lists" on JSON-list databases. BigQuery holds typed arrays but cannot `=` them, so its text form is neither "typed list" nor "refused" |
| — | The Java checker; the type-checker's compile-time rule | `testexec/*`; `compiler/spec/EqualityClasses`, `LiteCrossTypeRule`, `NumberLiterals` | allowed (tenet 5; the typer). Not counted |

So the target ledger in 6.2 ("one element rule; one shapes layer; the Java checker; the named non-equality forms") is
not what the slices produce.

### (a) Can one normal-form table serve pairs, keys and statements?

No. Here are the kinds where the pair test and the key (or the order key) cannot be the same expression:
- **A value known only at run time.** The pair test is "tags equal, and the values equal in that family's form". A
  key for dedup, GROUP BY or IN needs every family's form in one column. That means one column per family
  (`SideEqualities.kindedRows`: `k`, then `v0..vn`), a struct, or text.
  - Where the database holds a struct as JSON text (SQLite, MariaDB, MySQL, H2), the key decays to text. That is the
    canon `CanonicalRenderSql` writes, and it keeps date-time written digits (`CanonicalRenderSql.java:1043-1046`,
    which conflicts with S399; **not verified** whether product equality reaches it).
  - For an `Any` carried as JSON, only String, Integer, Float and Boolean have a "tag" (`SideEqualities.java:755-756`).
    A date, a Decimal or an enumeration inside JSON has none (`Lowerer.java:3945-3948`: "the SAME bytes"). The row
    "a tag naming the family" has no carrier there.
- **Ordering of numbers.** Equality never crosses the two families. Ordering does: "a mixed list orders by value,
  then by type" (AGENTS.md). The order key merges families by value (today a DOUBLE comparable, lossy). The equality
  normal form keeps them apart. They are different functions.
- **An enumeration.** Equality is in the store's space (`decodeInvert`). Ordering is by name (the carriers map, C11).
  One form cannot be both.
- **A year or a year-month.** The design's row says "the text it prints". `SideEqualities.DATE_KINDS` compares them as
  BIGINT and as `y*12+m`. That form was adopted *because* text comparison was wrong (the precision audit's H1,
  2026-11-22: `SideEqualities.java:747-752`). The value form still compares them as text (`Equalities.java:155-169`).
  The table picks the form an audit rejected, so one of the two codes changes meaning without a row.
- **A Decimal on SQLite.** There is no exact type (carriers C5). "The database's exact numeric type" does not exist
  there. A canonical-text form works for a pair and for a key, but not for an order.
- **Exact numbers over a wide scale spread.** A pair can be cast per pair. One key column for a whole list needs a
  type wide enough for all of its elements, which may not exist (the batch record's B9: "Decimals more than 38 digits
  apart").
- **Empty and NULL.** The pair test depends on position (`Lowerer.asCondition`, the positive mark). A key does not:
  GROUP BY and DISTINCT treat NULL as one key, and IN treats it as unknown.
- **An instance.** The pair test is typed leaves (`SideEqualities.sameInstance`). The key is a struct of normalized
  keys, which is text on JSON-struct databases (the case of `InstanceEquality.contains` today).

### (b) Does 2.2's "expression over two list values" re-create a second shapes layer?

Yes. `RootComparison.insideAnExpression` chooses the code by whether `FreeVars` is contained in the readable names
(`planner/RootComparison.java:179-195`):
- When it is, the comparison is a statement over rows (`SameValuesForm`, `IS NOT DISTINCT FROM` per row, signed
  `UNION` for a multiset).
- When it is not (B3), it is normalized arrays compared by the database's array `=`, or sorted, plus a map read per
  row.

These are two encodings of "in order", "in any order" and "a map's entries", chosen by **where the comparison
stands**. AGENTS.md's "Wherever it stands" rule exists to forbid exactly that. TaggedLists' positional list logic is
a third. NULL inside a list, duplicates in a multiset, NaN in `list_sort` against GROUP BY, and BigQuery's arrays with
no NULLs can all drift between them.

### (c) After 2.3, does the test planner still decide anything about equality?

Yes:
- **"Not judged here."** The planner must classify a product refusal as either a capability refusal ("not judged") or
  a meaning refusal (the body is wrong for both checkers). That classification is equality knowledge.
- **`forAll` semantics.** Rewriting `forAll(x | assertEquals(...))` into a Boolean `forAll` changes where evaluation
  stops (B6 below).
- **The record.** The planner still owns which statements are re-read, the per-element record and its index.
- **The TDSNull sentinel and the enum frame.** These rewrite what is compared (`DatabaseJudge.java:99-114`). The
  design does not say where they go.
- **`assertIs` over enumerations.** It is `databaseVerdict` (`DatabaseJudge.java:896-901`), which is deleted. Yet 2.3
  lists `assertIs` under "stay as they are".
- **The tolerance's arithmetic type.** This is a user ruling of 2026-11-03 (`DatabaseJudge.java:677-690`). See B8.

### (d) Is there another Java-side comparison of values?

None that I found beyond the host checker:
- `JudgmentFacts.kindsDiffer` and `KindClass` compare types, not values.
- The literal unroll's folds are the compiler's, allowed by the fold rule.
- `IsolatedRun.difference` compares verdicts, records and texts, not Pure values.
- `LiteWords.pairSide` only formats (`testexec/LiteWords.java:149-152`).
- `VerdictBatch` deduplicates hoisted CTEs by MIR equality (`VerdictSql.java:~1035-1050`), which is not a value
  comparison.

The extra comparison codes are all in SQL (B0 above).

---

## (B) Structural objections, most serious first

### B1. "Not judged here" does not exist under BOTH. It reverses a ruled decision, and it is where the second code would come back.

The design (2.3) says: "the database checker reports 'not judged here' and the Java checker judges alone, *as the two
already do for several forms*." The code says otherwise:
- **Under BOTH, unjudged is a disagreement.**
  - `testexec/IsolatedRun.difference` compares the two runs' reports one by one. `h.report() != d.report()` is a
    `JudgesDisagree` (`:158-163`). A host VERDICT against a database UNJUDGED fails the body.
  - The comment at `:146-148` records that the comparison which let "an assert one judge declined not compared"
    pass **went with A4, ratified by the user on 2026-11-19**.
- **A planning refusal refuses the whole BOTH body.**
  - `TestPlanner.planIsolated` plans the database half with `plan(... DATABASE)` (`testplan/TestPlanner.java:102`).
  - A product refusal inside `adjudicate` becomes a wall (`AssertVerdicts.planDatabase`, `testplan/AssertVerdicts.java:94-99`).
  - An UNPLANNED assert refuses the body ("the program road does not plan this body", `TestPlanner.java:187-194`).
- **No "decline" mechanism exists.** Nothing in `testexec` ever emits `declined` (grep: only pass-throughs).
- **Today's judged forms would regress.** Forms the design moves to the list are judged today:
  - a grid against a list (`DatabaseJudge.gridColumnPair`, `:211-244`);
  - host constants (a TDG seed, a rendered DDL, a folded literal) bound as VALUES (`DatabaseJudge.java:120-127`,
    `692-695`).
- **The list starts where B1 puts it.** "Shrink-only from B1's end" lets B1 park any hard form in the list.
- **It hides B2's work list.** With the waiver, a *refusal-type* difference between the product and the Java checker
  becomes "not judged" instead of a visible disagreement. Only wrong *answers* surface. That defeats section 4's
  rationale for doing B1 first.

**What to change.**
- Make "not judged" a typed plan fact: a closed enum of reasons on the verdict.
- Change `IsolatedRun` to accept it only for listed reasons, with a dated justification. It is a guard.
- Seed the list from today's measured UNJUDGED forms only (7, 3 and 7 bodies). Never seed it with forms that are
  judged today.
- Put decision 8.1 to the user as "reverse A4's rule for lite's own door", not as "already the case".

### B2. One "normal form" table cannot give the pair test, the key and the order key. Ordering and instances have no slice.

Evidence is in A(a) and B0, rows 4, 5 and 6. The design asserts "the same table gives two more things, so they cannot
drift". The code already shows them drifting inside one class (InstanceEquality's leaves against its canon), and in
the tagged order key (DOUBLE and TIMESTAMP) against the tagged equality (exact channels).

**What to change.**
- Make the table explicitly three-columned per (kind, holding): `same(l, r)`, `key(x)`, `order(x)`. Each cell is
  "refused" where no form exists: SQLite Decimal ordering, a Date, Decimal or enumeration inside JSON, an exact-number
  key past 38 digits.
- Add a generated property test: `key(a) = key(b)` if and only if `same(a, b)`; and order ties within a family if and
  only if `same`.
- Name the call sites B2 moves onto each column. Today B2 names "Equalities, SideEqualities, TaggedLists and the
  Scalars rules". It must also name InstanceEquality, ordering and extremes, NullSemantics, and decide relational
  `distinct`, `groupBy` and join keys (in, or explicitly out).

### B3. "How this database holds the value" is invisible to the lowering.

- **What the lowering can see.** Its facts are the database-independent logical SQL types of `sql/SqlType.java`:
  `TEMPORAL_TEXT` and `DECIMAL_TEXT` are labels that "never render".
- **What it cannot see.** SQLite's text date-times and REAL Decimals, the JSON-text lists of H2, SQLite, MariaDB and
  MySQL, BigQuery's lack of composite `=`, and MariaDB's PAD SPACE are known only to dialects:
  - `SQLite.equalArm` and `variantAwareCast`;
  - `MariaDb.equalArm`;
  - `BigQuery.equalArm`;
  - `SameValuesForm.refuseExactNumbersOfTwoScales`.
- **No channel exists.** No package under `lowering/` imports a dialect, and no capability object reaches the lowering
  (grep: the only dialect imports are in `planner/`).
- **So the table's per-database rows cannot live in `lowering.SameValue`.** "SQLite's text", "the database's exact
  numeric type" and "a Float literal written as a decimal (three databases)" are rows of that kind. Either:
  - (i) the lowering emits typed normal-form MIR nodes and each of 11 dialects writes them. Invariant 3 says so. Then
    the "one table" is a lowering table plus 11 dialects' arms, which the design neither counts nor estimates (no
    `default ->`; each arm a probe on its lane). Or:
  - (ii) a new database-facts record is handed to the lowering. The `EditionTable` precedent is per-database *door*
    data. A capability channel needs a ruling and a guard.
- **2.2's written-side rule needs the dialect in the planner.** "Wherever its elements are not in normal form as the
  database holds the list" makes `RootComparison` ask the dialect. Its signature has no dialect
  (`RootComparison.java:147-148`). Doing it for every database instead makes DuckDB's SQL grow.

**What to change.** Choose (i). Write the closed set of normal-form MIR nodes. Count their render arms in the
estimate. Add the dialect question to `RootComparison` as a named planner-to-dialect capability.

### B4. A second shapes layer, chosen by position (2.2, B3).

See A(b).

**What to change.** Pick one:
- (1) One shapes code: a comparison that reads the row is the same `SameValuesForm` statement, correlated over the
  row's arrays (lateral UNNEST), refused where the database has no lateral UNNEST. This is "a subquery per value",
  which AGENTS.md names as disproportionate, so it needs the user's word.
- (2) Refuse per-row list comparisons wherever the database's native array `=` is not value-exact for the element
  kind (the "per row refused okay" ruling, extended).
- (3) Build B3 as written, and state plainly that it is a second shapes code, with a test pinning the two against
  each other.

Under (1) or (2), make TaggedLists' list logic call the shapes layer. Today the design leaves it in place.

### B5. The record on failure: `Branch` is the wrong mechanism, and the hard parts are unlisted.

- **`Branch` cannot carry the record.** It holds only `Verdicts` and `Branch` steps (`TestPlan.java:685-689`). Its
  condition is a `Side` (one statement's row or a constant), not "verdict *i* of a fused group".
- **The real precedent is the failure-only message.** It is `RootLoop.Message`, read by `RootLoops` only where the
  root fails (`testexec/RootLoops.java:262-300`) and registered by `VerdictBatch.message` (`:280-295`). The record
  should be modelled the same way, as failure-only sides on `TestPlan.Verdict`.
- **Not in the design:**
  - **`assertError` around an assert.** `TestRun` composes "what the function raised" from the inner typed row's
    record values (`testexec/TestRun.java:249-260`; `TestPlan.Verdict.inner`, `:501-503`, `538-540`). With no record
    columns, that path needs record reads under the group's savepoint *before* the wrapper runs.
  - **Unrolled elements.** Each element has its own report (`report.atElement`, `VerdictBatch.java:345-347`), so each
    needs its own record sides.
  - **Savepoints on databases that abort a transaction** (`TestPlan.Verdicts.Savepoint`, `:444-475`). A re-read that
    raises inside a transaction must be guarded.
  - **The PCT judge-SQL ratchet counts what a step *may* send.** That includes failure-only message sides
    (`TestPlan.sent`, `:287-303`; `pct/.../ChannelB.java:348-364`; `ratchets.tsv`). Two record statements per assert
    raise `judge.statements`, so done-check 5 fails unless the counting changes, with a dated justification.
  - **BOTH compares the two failures' records as data** (`IsolatedRun.java:165-170`). The database record must be built
    from the same side reads as the host's (the host side planner) or BOTH disagrees on every failing assert.
- **`typedRow` does not "go".** It is used by assertSize's product pattern (`VerdictSql.java:214`), JSON (`:466`),
  instanceOf (`:338-353`, `502`) and TDS equivalence (`:850`), all kept by 2.3. So two record mechanisms survive.
  `AssertRecords.ofTypedRow` stays for those families.

**What to change.** Model the record as failure-only sides on `Verdict` (the `Message` pattern). List assertError's
inner asserts, unrolled elements, savepoints and the ratchet as B1's work. Either move the kept families too, or say
they keep row records.

### B6. "An assert inside `forAll` becomes the `forAll` of that expression" changes meaning.

- **Pure stops at the first failing element.** A later element whose computation raises is never evaluated in Pure.
  A Boolean `forAll` statement is set-wise, so the database evaluates every element and raises. Example:
  `[1, 0]->forAll(x | assertEquals(1, 10 / $x))` fails at element 0 in Pure and divides by zero in SQL.
- **Today's code knows this.** It splits raising runs (`VerdictBatch.raisingRuns` and `MayRaise`, `:738-779`) and
  orders "a raise is the first element's" (`AssertVerdicts.java:118-142`).
- **The third statement is `perElement` under a new name.** It "names the first element that failed", so it compares
  elements again, unless it is written as the product's `map` vector plus "the first false".
- **Per-element messages need run-time binding.** A message that reads or raises (`VerdictBatch.Scope.elementIndex`,
  `oneMessagePerRoot`, `:282-318`) needs a statement bound to the run-time index. That is exec's keyed-region pattern
  (A3a), and the design does not name it.

**What to change.**
- Keep the unroll for literal sources: each element its own product `==`.
- For computed sources, plan the product's vector and a first-false read. Refuse by name where a later element may
  raise (`MayRaise` already tells).
- Name the exec mechanism for per-element records and messages.

### B7. 2.4's "policy as data" is a mode read after the typer, dressed as data.

- **Today one dispatch replaces the rule.** A door's row replaces the product's rule at one dispatch
  (`Scalars.lower`, `:2409-2416`) and switches off two Lowerer paths (`wholeSides`, `:480-482`; `wholeAnswer`,
  `:506`).
- **The policy would have to reach many more sites:**
  - every decision point of `SameValue`, HeldKinds, TaggedLists and the order and key columns;
  - InstanceEquality, which the Lowerer reaches *before* `Scalars.lower` (`Lowerer.java:4596-4608`, per the map);
  - `Lowerer.runTimeComparison`;
  - the planner's shapes layer. B4 turns on whole sides at the doors, so `RootComparison` and `SideEqualities` would
    have to receive the door's policy through `WholeComparisons`.
- **The planner would branch on the door.** The user ruled "We shouldn't land with the planner knowing editions".
  `ArchitectureTest.thePlannersReadsOfTheEditionAreItsListedOnes` checks only calls on `planner.Edition` and
  `lowering.EditionTable` (`core/src/test/java/com/legend/ArchitectureTest.java:3651`, `~3674-3678`). A policy record
  passed as an argument is not caught.
- **It contradicts tenet 6's edition clause.** "Only the typer reads it ... nothing after the typer can see it."

So the door mechanism becomes *wider*, from one dispatch to every comparison decision and into the planner, while 2.4
says it becomes narrower.

**The compliant shape.** The door's typer already chooses a `CrossTypeRule` (`Typer.ruleFor`; `edge/PureComparisons`,
`edge/EngineComparisons`). Let it write distinct calls whose meaning is fixed by the call:
- a strict "same type and value" native for three number types;
- an "as written" date equality.

The element rule then has them as rows keyed by the native. Nothing after the typer, and nothing in the planner, sees
a door.

If that is the edition program's later step, then in 6B keep today's NamedRule replacement exactly as it is (no
wider). Leave the doors off the whole-side plan, recorded.

### B8. Rows of the 2.3 table are not plannable as written.

**Tolerance.**
- `abs($a - $e) <= $t`, typed by lite's equality rule like any `==`, is a compile error for ordinary inputs. For
  example `assertEqWithinTolerance(1, $i, 0.5)` gives Integer `<=` 0.5, which is not exact as an Integer. Pure
  declares the three parameters `Number`, so Pure accepts it.
- The product's arithmetic promotion contradicts the user's ruling "never DOUBLE for exact kinds"
  (`testplan/DatabaseJudge.java:677-690`).
- "Spelled so already inside a `forAll`" refers to `VerdictQueries.assertAsPredicate` (`compiler/spec/VerdictQueries.java:484-490`).
  It builds through `native2`, the first overload by arity (`:559-563`), and so bypasses `comparedAsInAQuery`. That
  is the invariant-4 pattern the 2026-11-26 audit removed from `declared` (`:523-528`). It is not the type-checker's
  rule.

**`assertEq` over instances.**
- Identity needs `__id`, which "rides only the verdict/identity lane" (`lowering/InstanceEquality.java:139-149`).
- There are two lanes with different class layouts: `BodyPlanner.lowererFor`'s `identity` flag
  (`planner/BodyPlanner.java:147-166`), and the database judge's sides always on it (`planner/SidePlanner.java:65-83`).
- The design does not say which lane "the product's one road" means. A query on the other lane answers instance
  `==` differently (`InstanceEquality.java:151-165` returns null and falls to the generic rule). **Not verified** in
  full which lane `TypedQuery.executionPlan` uses.

**`assertIs` over enumerations.** It is the deleted `databaseVerdict` (see A(c)).

**The enum frame and the `^TDSNull()` golden sentinel.** Both are silently dropped (`DatabaseJudge.java:99-114`).

**`csvStrings`.** A String column's empty string and NULL are judged alike, "toCSV's own equivalence"
(`DatabaseJudge.java:52-61`). It is not Pure equality and the design does not mention it. **Not verified** which arm
sets it today.

### B9. "Planned like any query" names no single road.

- **assertSize's pattern.** The judge plans the sides (CanonRider, the verdict lane), and the product's value-form
  `==` runs over two bound columns (`SidePlanner.over`, `:90-94`; `VerdictSql.typedOverValues`, `:191-215`).
- **`assert(cond)`'s pattern.** `BodyDriver.planValue` runs the body planner's lowerer with `RootComparison.asking`
  (`RootComparison.java:253-258`): the test road, with frames (`VerdictBatch.defineFrame`), system routes and the
  verdict lane. It is not `TypedQuery.executionPlan`.

Done-check 3's "the planner's entry for a query" is therefore ambiguous, and the claim "the pattern exists for two
asserts already" overstates what exists.

**What to change.** Say that asserts are planned by the body planner's road (`assert(cond)`'s), and prove its
statements equal the product's statements for the same `|$a == $e` (E3 below).

### B10. After B1 the measure is vacuous, and "B1 before B2" finds little.

**The measure's DATABASE rows.**
- After B1, `assertEquals(a, b)` and its rewrite `assert(a == b)` are the same statement under the database judge, so
  the DATABASE rows read zero by construction (the design says so).
- Only the HOST rows (the Java checker against the product's plain comparison) still measure anything, and they need
  no B1 to run.

**Who will see B1's disagreements.**
- BOTH runs only where asked: the LITE-PCT lane, on DuckDB only (`pct/BUILD.bazel:512-534`; BOTH by default in
  `ChannelB.java:468-471`), and a handful of core classes.
- Most core tests are HOST (`ExecuteOptions.java:32`).
- On H2 and SQLite, B1's "found by the tests" is the measure again, which already names them.

**What B1 turns red** (the batch record's figures, not run):
- **DuckDB, about 3 bodies:** the S399 `at(0)` gap and two `hasSameElements` refusals.
- **H2, about 45:** 38 for no list form, 3 number types, 1 S399, 1 `ascii`, and some of the 7 "other".
- **SQLite, about 33:** 16 for no list form, 12 date-time texts, 3 numbers, 1 JSON date-time, 1 empty side.
- **Unknown:** the 12, 7 and 12 "other refusals and harness errors" were never read one by one.

These go red wherever a DATABASE or BOTH run reaches them. Otherwise they disappear into "not judged" (B1 above).

The design's "all but a handful are a database with no list form or a checker that cannot judge" is contradicted by
the same table: on DuckDB 21 of 28 bodies are in other rows, on H2 18 of 59, on SQLite 31 of 54 (before the year fix).

### B11. Smaller structural points

- **Invariant 3a and the "evaluate these, then false" node.** A typed node is right. But each dialect needs a spelling
  its optimizer will not fold away, or a side that should raise does not. Today's text trick also casts any value to
  VARCHAR, which can itself raise (`Equalities.java:229-235`). That is a per-dialect probe, not a given.
- **Invariant 2.** "The database's exact numeric type, wide enough for both" is SQL type computation in the lowering.
  `SideEqualities.wideEnough` already strains invariant 2 this way, and the "refuse by name" inside `SameValue` is
  HeldKinds' ruled strain, extended. Both should be named.
- **The Float row** ("a Float literal written as a decimal (three databases: recorded)") contradicts section 3, which
  excludes it.
- **The deletion arithmetic hides a move.** 2.1 deletes `SideEqualities.contains*` and `subset` (2.3) while 2.2 adds
  product membership and "all in" over whole sides. The net is a move, which is the pattern that drew the user's
  objection to A. Count it as a move.

---

## (C) Per-slice difficulties

| Slice | What the code makes harder than the note says |
|---|---|
| **B0** (half a day) | **The ledger cannot be mechanical yet.** `SqlFn.EQUAL` is constructed in 32 files of `lowering`, `planner`, `testplan` and `compiler` (124 uses); `NULL_SAFE_EQUAL` in 7. No MIR node separates value equality from structural `=` (tags, row numbers, sizes). So "B0's ledger fails on a new comparison site" (the risk table's last row) is a convention until a `SameValue` MIR node exists. Move node creation into B0. **The one list:** the three lists differ on purpose. `EqualityClasses.COMPARISONS` holds `hasSameElements`, `indexOf`, `compare`, two-argument `max` and `min`, and the asserts (`compiler/spec/EqualityClasses.java:70-101`). `HeldKinds.READ` holds one `contains` overload (`lowering/HeldKinds.java:62-78`). `UpstreamTies` holds `removeDuplicates`, `sort` and the extremes. One table with attributes per function is fine, but it is not "delete two copies" |
| **B1** (two days) | The test plan's data model changes: failure-only record sides on `Verdict`, `TestPlanJson`'s round trip (`TestPlanner.java:215`), `RootLoops`, `AssertRecords`, and `VerdictBatch`'s fused layout (`VerdictSql.batch`, `:1015-1100`). Also: assertError's inner asserts (`TestRun.java:249-260`); unrolled and nested quantifiers with raise order and per-element messages (B6); savepoints; BOTH's record equality; `IsolatedRun`'s UNJUDGED rule (B1); the exact PCT LITE ledger (`channelb/ledger.tsv`, lane duckdb-lite) and the judge-SQL ratchets; the identity lane for `eq`; the TDSNull and enum frames; the rendered-text arm (`AssertVerdicts.renderedArm`, `:1714-1749`). The design admits the batch question is "read, not designed" (section 7). Three to five days is more honest |
| **B2** (two days) | **Three side representations** to normalize: a scalar `SqlExpr` with its `TypedSpec` (Equalities); a side's rows with `__o` and `__rn`, kinded rows and CTEs (SideEqualities); tagged channels and position arrays with lambdas (TaggedLists). **A new normal-form MIR family** and its arms in 11 dialects (B3 above). **Three columns** (pair test, key, order key) per kind (B2 above). InstanceEquality, NullSemantics' shape-matched negation, and the ordering and extremes. **Every filter's SQL** across 11 lanes, and the "no product row up" size pins. The architecture audit sized this "one typed comparison IR" at **1-2 weeks on its own** (review 4, E.1) |
| **B3** (one day) | Per-row normalization needs a list-transform per dialect. **Not verified** per dialect, but Postgres, BigQuery and H2 have no array lambda, so it becomes a subquery per row, which needs a proportionality ruling. BigQuery's composite `=` is text. A map read per row needs sorted entries per dialect. It is also a second shapes code (B4 above) |
| **B4** (one day) | Turning whole-side plans on at the doors changes the doors' answers. legend-engine's door answers "the database's list `=`" today. Every moved answer has to be **run** upstream and posted to the user before it lands (the fidelity tenet). 2.4 as designed needs a tenet-6 ruling (B7 above). The COMPAT lanes move |

**Is "B1 before B2" sound?** Only on DuckDB, where about 3 bodies move. It is sound there only if those bodies are
ledgered as disagreements the user has seen.

On H2 and SQLite, B1 reddens about 45 and about 33 bodies wherever a DATABASE or BOTH run reaches them, plus an
unknown share of the 26 unread "other" bodies. Of those, about 54 are "no list form", which section 3 assigns to the
database follow-up and not to B2. They would either stay red after B2 or become "not judged". The latter is a
regression: SideEqualities' row form judges many of them today.

So B1 cannot land green on its own unless the B1-above waiver lands with it, and that waiver hides B2's refusal-type
work list. Either land B1 and B2 together, or define B1's done state as "red, ledgered, user-seen", with the ledger
rows written.

---

## (D) The two walk-throughs (item 4)

### Section 6's eleven gaps (`research-comparison-sites.md`)

| # | Gap | In the design |
|---|---|---|
| 1 | The failure record, apart from `same` | **Closed in direction** (2.3). The cited mechanism (`Branch`) is wrong. assertError's inner asserts, unrolled elements and the kept row-record families are **silently dropped** (B5 above) |
| 2 | A frame or grid against a flat list (`gridColumnPair`) | **Explicitly moved to "not judged"**: a regression of what the database checker judges today |
| 3 | Membership and subset of whole sides | **Closed** (2.2 "Added here"), as a move of the judge's `contains*` and `subset` into the product |
| 4 | The first failing element of a quantified `assertEquals` | **Claimed closed** ("a third names..."). Stopping at the first failure, raise order and per-element messages are **silently dropped** (B6 above) |
| 5 | Tolerance | `assertEqWithinTolerance`: **claimed closed, but the row does not type as written** and conflicts with the 2026-11-03 ruling (B8 above). `assertTdsEquivalent`: **explicitly kept**, its cells compared as VARCHAR text |
| 6 | JSON-document equality | **Explicitly out** (kept as a named form) |
| 7 | The TDSNull golden sentinel; the enum frame for an untyped wire | **Silently dropped** |
| 8 | Sides on two databases; host constants bound as VALUES | **Explicitly "not judged"**. Host constants are judged today, so this is a regression |
| 9 | The kind gate's "unjudged" cases | **Closed by construction** (the product's rule decides), where the product does not refuse |
| 10 | The measured movers | **B2's work list**. Part of it is excluded by section 3 (SQLite date-times; H2 and SQLite list forms, D8), so **partly out** |
| 11 | The doors' whole-side form is turned off | **Claimed closed** by 2.4's policy, which is the tenet problem in B7 above |

### The fifteen carriers (`research-carriers.md`)

| # | Carrier | In the design |
|---|---|---|
| C1 | TEMPORAL_TEXT | **Partly closed.** `=` against a TIMESTAMP is the date-time row. Partial dates take "the text it prints", which contradicts `DATE_KINDS`' BIGINT (A(a) above). Ordering of a computed partial, `in`, `contains` and dedup are claimed through "the same table gives the key and the order" with **no slice**, so **silently dropped** |
| C2 | DECIMAL_TEXT | **Closed for `=`** (a Decimal held as text). Ordering is under the unsliced order key. The print-as-meaning debt (carry DECIMAL, write text at the edge) is not addressed |
| C3 | A list as JSON text | Whole sides: **closed** (rows). Per row: **refused** (B3). Dedup and GROUP BY of list values, and BigQuery's composite text `=`: **silently dropped** |
| C4 | SQLite date-time text | **Explicitly out** (section 3, "only if it falls out") |
| C5 | SQLite Decimal as REAL | **Silently dropped.** The table assumes an exact type exists |
| C6 | Variant or Any as JSON | **Partly.** "A JSON value by its JSON kind" covers four kinds. Date, Decimal and enumeration inside JSON have no kind: **silently dropped** (needs a refusal, or the tagged carrier) |
| C7 | Tagged value or list | **Closed in principle** for `=`. The DOUBLE and TIMESTAMP rank comparable used for ordering (`MixedEncoding.java:31-36`) is **silently dropped** |
| C8 | A list of days and date-times as one kind | **Not mentioned.** Stays refused (`refuseMixedDays`). The `at(0)` gap (S399) is **silently dropped** |
| C9 | A computed Number list held as one numeric type (`[$x.i, $x.f]`) | **Silently dropped.** The row "a tag naming the family" has no carrier: computed elements stay a plain array (`Lowerer.java:3981-3985`). No slice makes them tagged |
| C10 | A Float literal as DECIMAL | **Contradictory:** a table row ("recorded") against section 3 ("not done"). Treat as **explicitly out** |
| C11 | An enumeration as a name, or a DECODE chain | **Partly**: a row, and section 7 says it is unknown. The equality form (store space) and the order (name) differ: B2 above |
| C12 | A mapping wire coercion | **Silently dropped.** `comparisonWireOperand` compares in raw store space, which is not a normal form. The key in Pure type and the pair test in store type will differ |
| C13 | MariaDB and MySQL PAD SPACE | **Explicitly out** (section 3) |
| C14 | Collation ordering | **Explicitly out** (section 3) |
| C15 | MongoDB day and date-time as BSON date | **Silently dropped** (not in section 3) |

**Deletions that a non-test caller still needs:**
- `VerdictSql.typedRow`: needed by assertSize, JSON, instanceOf and TDS equivalence.
- `DatabaseJudge.databaseVerdict`: needed by `ARM.is` over enumerations (`:896-901`), `ARM.eq` over primitives and
  keyless instances (`:961-975`), `assertNotEq` and `assertIsNot` (`AssertVerdicts.java:648-660`), and the
  rendered-text road (`:1744-1749`).
- `SideEqualities.contains*` and `subset`: their logic is needed by 2.2's product membership.
- "The canon-text machinery": if this includes `CanonicalRenderSql`'s canon, the product's `InstanceEquality` needs it
  (`:177-186`, `217-232`). If it means only `VerdictSql.Side`, `canonRows` and `gridRowCanons`, it is fine. The text
  is ambiguous.
- `CanonRider`: `planner/SidePlanner.staged` and `wrap` take it (`:65-83`, `114-160`). Its removal changes a planner
  signature. That is fine, but it is not judge-only.
- `VerdictSql`'s `classExtentRows` and `graphCount` (FramePlanner, SnapshotEnvelope) stay, per the map.

**"Checked" (or "measured") claims the code contradicts:**
1. "Steps that run only on a Boolean the database computed (checked: `TestPlan.Branch`)": true of `Branch`, but
   `Branch` cannot carry a record or key off a fused verdict (`TestPlan.java:672-690`).
2. "Only assertSize and assert(condition) are planned as the product's own expression (checked)": assertSize's sides
   are planned by the judge and bound. The forAll vector already uses product predicates through the arity-picked
   `native2` (B8, B9 above).
3. "As the two already do for several forms" (not marked checked, but stated as fact): false under BOTH (B1 above).
4. "`Compared.alone`, `Held`, `VerdictSql.typed` and `typedRow` go": `typedRow` has 15 call sites, kept forms among
   them.
5. "Measured: all but a handful are a database with no list form or a checker that cannot judge": the record's own
   table says otherwise (B10 above).
6. The table row for years: contradicts the audited `DATE_KINDS` (A(a) above).

---

## (E) "Done" checks that could not be passed with two codes

How each of the design's checks could pass with two codes:

| Section 6 check | How it passes with two codes |
|---|---|
| 1. The measure | Its DATABASE rows compare the product with itself after B1. It never moves a comparison's **position** (root, inside a filter, inside a lambda), so the statement form and the per-row form are never set against each other |
| 2. A ledger of codes | Two codes in one class, or renamed, pass a ledger of class names. A had this exact shape |
| 3. The test planner cannot build a comparison | Passes by routing through a planner entry with a judge flag, or by moving `VerdictSql` to another package. As written it **fails anyway**: `typedTdsEquivalent` stays in `VerdictSql` with its own cell `=` |
| 4. Deletions gone, smaller than at A | Passes by moving code, as A did. "Smaller than A" still allows about +1,250 to +2,050 lines against the pre-step-6 baseline |
| 5. SQL size | "The test checker's SQL down" passes by marking more forms not judged. Product rows will rise honestly (B3's transforms, written lists made whole sides) |

Proposed replacements:

1. **A position and operation matrix, generated.** Take pairs and lists of values over every (kind, carrier) of the
   two maps. Ask each the same question in about 12 positions and operations:
   - as the root, `if(...)`, `&&`, `!`, a projected column and a filter over a one-row table;
   - inside `map` over the element, and `forAll`;
   - through `[a]->contains(b)`, `b->in([a])`, `[a, b]->removeDuplicates()->size() == 1`, `indexOf`,
     `hasSameElements([a], [b])`, `sort` ties, and `distinct` over a two-row table;
   - as `assertEquals` under DATABASE and under HOST.

   Run on DuckDB, H2 and SQLite (lanes at milestones). Every cell agrees, or every cell refuses with the same words. A
   second code in any position shows up as a disagreeing cell. This is the check the step lacked.
2. **A mechanical node guard.**
   - Value equality, keys and order keys are distinct MIR nodes, built only by `lowering.SameValue` (an ArchUnit rule
     on constructor calls).
   - `SameValuesForm` takes element forms only from those nodes.
   - Every other construction of `SqlFn.EQUAL`, `NULL_SAFE_EQUAL`, `Membership`, `LIST_POSITION` or a DISTINCT over a
     Pure value sits on a shrink-only allowlist with its reason. Today's 124 plus 21 uses are the baseline.
3. **A plan witness.** For every equality assert of core and PCT on lite's door under the DATABASE judge, the
   verdict's sent statement equals the statement the body planner plans for `|$a == $e` (and for
   `|if($a == $e, ...)`), modulo the verdict projection, in the style of `SentStatementsAreThePlansTest`. Also:
   `testplan` depends on no `lowering` class except data records (ArchUnit), and `VerdictSql` constructs no
   `SqlFn.EQUAL` except to read a Boolean.
4. **The baseline.** Net lines of `lowering`, `planner` and `testplan`'s comparison code below the commit **before
   step 6**, not below A. Plus a census of statement-shape constructions (`SAME_IN_ORDER`, membership EXISTS) that
   are all the shapes layer's.
5. **Not judged as a typed waiver.**
   - The plan carries the reason, from a closed enum, and `IsolatedRun` accepts only the listed reasons.
   - The list starts no larger than today's measured UNJUDGED forms.
   - A diff of the measure's DATABASE rows shows zero bodies moving from JUDGED to UNJUDGED.
6. **SQL as sent.** Judge statements per *judged* assert on a passing run (sent, not "may send"), with failure-only
   statements counted on their own line. Product rows either not up, or each one up with a ruling.
7. **Doors.** No class of `planner` can reach the policy type (or the door-chosen natives' rule enum) through any
   method, beyond calls on `Edition` and `EditionTable`. Extend `thePlannersReadsOfTheEditionAreItsListedOnes` to the
   new type.
8. **The measure re-aimed.** HOST-assert against HOST-plain (the Java checker against the product) on every body and
   every database is the gate. After B1, the DATABASE rows are evidence of nothing.

---

## (F) Checked and found sound

- At a compatibility door the database judge is never planned (`TestPlanner.refuseTheDatabaseJudgeInCompatibility`,
  `:73-80`).
- A failed assert's record rides in the verdict row today (`testexec/AssertRecords.ofTypedRow`, `:88-138`).
- `Lowerer.written` is `Scalars.lower`'s only caller (`Lowerer.java:438`), and the guard exists
  (`CodeShapeGuardrailTest.java:594`).
- `Lowerer.computes` (`:3880-3897`) duplicates `LiteCrossTypeRule.harmless` (`:199-218`). Merging them is safe.
- `HeldKinds.readsAs` (`:170-183`) is a narrower copy of the literal rule and the cause of record C8. Deleting it in
  favour of `NumberLiterals` is right.
- `neverEqualButEvaluated`'s text trick (`Equalities.java:229-235`) should become a typed node. The diagnosis is
  correct; the per-dialect spelling needs probes.
- A failure-only read already exists (`RootLoop.Message`; `RootLoops.java:262-300`; `VerdictBatch.message`,
  `:280-295`), so "the record read on failure" is buildable on it.
- Removing record columns would shrink the fused verdict statement (`VerdictSql.batch`'s shared slots,
  `:1015-1100`). The direction is plausible. It was not measured.
- Three lists of comparing functions do exist (EqualityClasses, HeldKinds, UpstreamTies). One attributed table is
  worth having.
- assertSize already uses the product's `size` and `==` (`DatabaseJudge.databaseSize`, `:414-433`).
- `quantifiedVector`'s refusal of the product's `==` for `ASSERT_EQUALS` (`DatabaseJudge.java:820-827`) is exactly the
  distrust 6B must remove. The design is right to target it.
- The doors' own test checker stays Java alone. That matches rule 4 of the rules of record (`TestPlanner.java:65-80`).

## Estimate (item 6)

Six to eight working days is not believable.
- **B1:** three to five days (B5 and B6 above).
- **B2:** one to two weeks, by the architecture audit's own sizing of the same idea.
- **B3 and B4:** two to three days each, B4 with upstream runs.

Verification rounds and lanes come on top. **Three to four weeks** is the honest figure. Writing will be closer to
1,500-2,500 lines than 600-900 once the dialect arms, the record sides and the tests are counted.

**The single thing most likely to blow it up:** B1's premise that an assert "is" a product query. The assert lives
inside test-plan structure: fused verdict groups, unrolled and nested quantifiers with first-failure and raise order,
raising verdicts inside `assertError`, branches, frames, system routes and the verdict lane. Under BOTH its record must
equal the Java checker's as data. That changes the plan's data model (`TestPlan`, `TestPlanJson`, `RootLoops`,
`AssertRecords`, `VerdictBatch`) and every database and BOTH failure path. It also needs the user to reverse A4's rule
before B1 can land green. The design marks the batch half of this "read, not designed".
