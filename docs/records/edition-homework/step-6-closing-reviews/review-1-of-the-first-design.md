# The first independent review of the closing design (of `4d98b6709`, sections 0 to 4)

Read-only, by an independent reviewer (a subagent given the design and the rules, never the author's
conclusions), 2026-11-27. Kept verbatim as evidence; what was done with each finding is in
`step-6-closing-design-2026-11-27.md` (sections 5 and 6) and `step-6-closing-batch-2026-11-27.md`. Paths the report
writes under `/home/user/ll-audit...` are a read-only worktree of this repository.

---

## Review of the step-6 closing design: findings by question

All paths are under `/home/user/ll-audit3/core/src/main/java/com/legend/`. I read the code and ran nothing.

**Summary.** 2.1 is mostly sound: the planner hands one interface to the lowering, and every compatibility door has a row for `==`. Four things would ship a wrong answer or a bad design if built as written:
- **2.3 is too narrow.** `OrderView` (the "is this side's order defined?" rule) answers "defined" for far more than lite's internal natives.
- **2.4 makes `hasSameElements` mean different things at different doors.** That contradicts 2.1 and register row S430.
- **Merging `plan` with `inside` changes answers at the root** for comparisons that read a query parameter.
- **2.2's fallback alternative brings back the side signal that A1 removes.**

### 1. Door-dependence after the type-checker

**Added in step 6:**
- `planner/RootComparison.java:53-85,152-189,213-219` and `planner/ProductStatements.java:420-423,464-468` (the `numbersOf` path). 2.1 removes these.
- `lowering/Lowerer.java:314-316` (`numbersOf`) and `lowering/NamedRules.java:26-28` (`numbers`). Removed.
- `Lowerer.java:381` → `EditionTable.emptySideIsFalse` (`EditionTable.java:50-58`, overridden at `edge/EngineTable.java:260-265`). Removed only if the user rules A3 as recommended.
- `testexec/HostForms.java:29`, read at `testexec/Equality.java:170,302`. Kept by A7.
- The HeldKinds refusal (`lowering/Scalars.java:2396-2403`). Sits behind the lookup, as the design says.

**Older, and still there after the batch:**
- The planner reads the door's edition:
  - `planner/PlanEnv.java:183-201` (`edition()`, `editionTable()`).
  - `ProductStatements.java:140-146,172,193-210` (written flags, the stamps, `stampedCode`, `sideStamp`).
  - `planner/BodyPlanner.java:202,215` and `planner/FramePlanner.java:132,173`.
- The lowering reads the door's lowering forms through `Styled.forms()` (`Lowerer.java:890,1677,2302,2386,4582`) and its text facets.
- The resolver carries `outsideStore` through.

**SHOULD-FIX**
- Section 0's "the planner asks nothing about the door" is not true of the code after the batch. The planner still builds the door's table and reads its stamping policy (step 2/3's design, gone only at E7). Read literally, the user's "We shouldn't land with the planner knowing editions" is not met. Say exactly what remains and let the user rule on it.
- The proposed guard cannot be checked as worded. ArchUnit sees calls, not "for a comparison". "The three existing reads" also miscounts: there are at least nine planner reads of `Edition`. Whitelist the read sites by name instead.
- With 2.1 the lookup is asked in three places, not "the one place in `Scalars.lower`": `Scalars.lower`, `wholeSides`, and the new root check. Say so, or route the other two through one helper.

### 2. Is "the door named no rule" a sound stand-in?

**How it behaves today: sound.**
- `edge/UpstreamTies.java:68-73` gives a row to every overload of `equal`, `eq`, `contains`, `removeDuplicates`, `in`, `<`, `<=`, `>`, `>=`. It also has rows for `sort` and the number/collection `min`, `max`, `greatest`, `least` (`:51-61`; the date `min`/`max` are left out on purpose).
- `PureTable.lowersAs` returns these rows on every database (`edge/PureTable.java:40-42`).
- `EngineTable.lowersAs` returns the tie row before it reads `outsideStore` (`EngineTable.java:217-221`), so the stamp and the database never remove it.
- `HeldKinds.READ` (`lowering/HeldKinds.java:62-78`) is a subset of these rows, so a door never reaches the refusal.
- Comparisons the resolver builds copy their callee (`resolver/ChainNormalizer.java:145,187` and others), so they keep their rows.

**SHOULD-FIX**
- **The stand-in works by coincidence.** `NamedRule.NUMBERS_OF_THREE_TYPES` (`lowering/NamedRule.java:113-121`) is documented as a numbers rule. Nothing says it also means "no whole-side plan, no refusal of column types". The lowering would now infer that from the number row, which is A1's side signal moved down one layer.
  - It is legitimate under step 5 ("a row replaces the call's own rule") only if the docs say the whole-side plan and HeldKinds are part of `==`'s own rule.
  - Add a test that every compatibility table returns a row for every id in `HeldKinds.READ` plus `equal`.

**Shapes that reach the lowering at a door with lite's table, or with no planner (all older than step 6):**
- (a) A test body's seed queries, `GeneratedTestData.queries` (`testplan/GeneratedTestData.java:127-135`), use `Compiler.query` with lite's setting and `PlanStyle.PRODUCT`, whatever door admitted the test.
- (b) Lowerers made with no planner take the database's plain list comparison silently (invariant 4): `Compiler.java:962-976` (`lowerResolved`), `edge/EnginePlans.java:168-181`, and the `SeedableLets` probe.
- (c) `indexOf` has no door row and no rule for mixed lists (`Scalars.java:1323-1355`).
- (d) `sortBy` and keyed sorts are `TypedSort`/`TypedSortBy` nodes and never reach the lookup, so a door gets lite's order for ties.

`hasSameElements`: see 6.

### 3. The root entry in `Lowerer.lower(List)`

**SHOULD-FIX (a real behaviour change)**
- Today's root path, `plan` (`RootComparison.java:53-133`), has no "readable names" check. `inside` (`:158-169,183`) only plans sides whose free names are the statement's lets or the seeded lets, minus every name a lambda binds.
- So a comparison that reads a **query parameter** is planned at the root today and left as the database's list comparison inside an `if`. Example: `{names: String[*] | $names == ['a','b']}`. Parameters are plan parameters (`bindPlanParam`), not lets.
- The same happens when a let's name is reused by a lambda parameter.
- If the merged function keeps `inside`'s check, these roots fall back to the database's comparison (an error on H2, JSON text on SQLite). That contradicts "exactly today's root SQL".
- Add the plan parameters to the readable names, and state the shadowing rule.
- I could not tell from the code whether today's root plans a parameter side correctly. Probe it.

**SHOULD-FIX (the test road's SQL changes)**
- The design says `BodyPlanner` "gets the same root behaviour for free". In practice that means the judges' side plans (`planner/SidePlanner.java:76`, through `BodyPlanner.lowerAndPrepare`).
- Today a side `$a == $b` lowers as a scalar root wrapping a scalar subquery (`Lowerer.java:963,4429`), then goes through `LetBinding`, `KeyedCounts` and `SubselectPrune` (`:600-606`). With the entry it becomes the side-equality statement itself, a top-level `WITH`.
- Probe the database judge's reading of a side plan of that shape, and a `WITH` nested as a derived table on H2, MySQL and MariaDB.
- Say that the entry returns before `sharedLower` (`:592-597`) and before pruning.

**Sound**
- Table-valued lets: the check runs before `withQueryLets` (`:590`), and the inliner normally dissolves the lets anyway.
- Store writes are unaffected (`lowerStoreWrite` calls `withQueryLets` directly, `:632-634`).
- `EnginePlans` and `lowerResolved` have no planner, so nothing changes there.
- The seeded lets: the closure keeps the unseeded body's lets, as today. The design should say which lets the merged function reads; reading the seeded ones would also settle B8.

### 4. The empty side (2.2)

**SHOULD-FIX: values kept that `markKept` does not reach.** `markKept` (`Lowerer.java:378-393`) is called from only two places: the scalar root (`:963`) and computed columns (`:2218`). It descends only through `&&`, `||`, `!` and the branches of an `if`. Not reached:
- An aggregate's input (`:2333-2344`, `:2368`). `COUNT` of a NULL comparison drops the row where Pure counts `false`.
- A window column (`:3551-3554`).
- A list lambda's result. `[..]->map(x|$x.nick=='a')` can lose the NULL element at egress: data lost.
- A comparison used as an operand. `($p.nick=='a') == false` answers NULL where Pure says true.
- A comparison inside `toString`, a collection element, a struct or pair member.
- A `TypedRunTimeComparison` wrapper: it is no `TypedNativeCall`, and `runTimeComparison` lowers through `scalarInner`, skipping the guard (`:3736`).
- A query-level let's value (`:834`).

**SHOULD-FIX: cases the guard itself misses**
- `isOptional` matches `[0..1]` only (`lowering/NullSemantics.java:57-61`). A `[*]` side that is one value a row (a to-many navigation) is never guarded.
- Two `[0..1]` sides that are not both plain columns stay a bare `=`. `$p.nick == $p.alias->toUpper()` as a column answers NULL where Pure says false, or true when both are empty. This is a wrong answer on ordinary input, and it is not on the list.

I found no place where it marks a comparison that is really a condition and changes the answer.

At a door, the guard still recognises the comparison: `NamedRules` → `numbersOfThreeTypes` → `Equalities.equal(n, args, THREE)` (`Scalars.java:3848-3849`) ends in the same `SqlFn.EQUAL` (`lowering/Equalities.java:366`).

**BLOCKER for the fallback alternative.** "The guard applied only where the lookup named no rule" would:
- Key the empty-side meaning on whether a number row exists. That is A1's side signal again.
- Take the fix away from legend-pure's door, which has it today: `PureTable` inherits the default `true`. legend-pure's door would answer NULL where legend-pure says false.

If the user rules against A3, give the empty-side guard its own named rule or check at legend-engine's door.

### 5. The order of a side (2.3)

**BLOCKER.** `OrderView.of` falls to "defined" for every unknown native (`compiler/spec/OrderView.java:169`) and every unlisted node (`:261`). That is far more than `INTERNAL_DESUGAR`:
- **Natives that are not internal desugar:** `reverse`, `removeDuplicates` (it is not in `ORDER_PRESERVING`), `tail`/`init`, legacy TDS natives, lite's surface natives (`joinWithPrefix`, `without`). Example: `p::Party.all()->map(p|$p.name)->removeDuplicates() == ['A','B']` is compared in the order the rows arrived.
- **Typed nodes, not natives:**
  - store reads: `TypedViewRelation` (a store view), `TypedFrameRef` (a frame read);
  - unordered sources: `TypedRecurse` (a recursive UNION ALL), `TypedJoinSlot` (a join), `TypedSourceUrl`;
  - pass-through or branching nodes: `TypedFlatten`, `TypedIf`, `TypedMatch`, `TypedEval`, and a `TypedCollection` with many-valued elements.
- **Answered "sorted" when it is not:** a to-many property or a navigation after a sorted parent (`:251-254` descend to the source). Example: `Party.all()->sortBy(p|$p.id)->toOne().addresses->map(a|$a.street) == [...]`.
- **Inconsistency:** DISTINCT is listed as order-preserving (`:86-87,245`), but `Lowerer.distinct` can wrap a sorted source (`:2694`), which drops the order. `RootComparison`'s own javadoc (`:302-303`) calls a DISTINCT unordered.

The fix: default to "the first child's order" for nodes that carry rows or many values. Test every `TypedSpec` class and every native whose first parameter is many-valued or a relation, not only the `INTERNAL_DESUGAR` members. Add `sort()->distinct()`, `reverse`, `removeDuplicates`, a view, a frame read, a join, an `if` and to-many-after-sort to the matrix.

**SHOULD-FIX.** `OrderView` also drives the assert refusal and the compatibility door's rewrite to `assertSameElements` (`compiler/spec/OrderedAsserts.java:58-118`; `UpstreamTestBodies.rowsOrderFree`). Reclassifying moves corpus and PCT outcomes at the doors. Run the asserts in the matrix and list what moves.

### 6. `PureComparisons` and `EngineComparisons` for a non-`equal` equality (2.4)

**NOTE.** Neither builds a call of `equal`, so the split the design anticipates is not needed for that reason.
- `PureComparisons.answer` (`edge/PureComparisons.java:112-133`) returns `false` when a side is never empty. Otherwise it keeps the marker in a query, refused by `asPures` (`:50-57`) with words about "legend-pure's answer", or the call itself in a body (`:93-96`).
- `EngineComparisons` always returns the call (`edge/EngineComparisons.java:52-65`).

**BLOCKER as written.** Routing `hasSameElements` through each door's comparison setting makes lite's own function mean different things by door. That contradicts 2.1 ("the product's own function… at every door") and S430 (`docs/SEMANTICS_REGISTER.md:290`: two known types are a compile error).
- `hasSameElements([1,2],[1D,2D])` is true at lite's door and false at legend-pure's.
- At legend-pure's door, in a body with both sides maybe empty, it takes the product's two-family plan instead.
- At legend-engine's door the call passes through. With `RootComparison`'s check (`:96-107`) deleted, the branch at `:108-117` returns null for `hasSameElements`. The result is the misleading refusal "over values of the row or element it stands in" (`Scalars.java:3946-3950`).
- Fix: the type-checker applies lite's rule to `hasSameElements` at every door, and `:108-117` uses `unplanned` with a message about the two types.

### 7. Refusal at the dialect (2.5)

**SHOULD-FIX.**
- **Escapes:** most expressions carry a type fact (`sql/TypeFact.java`), so the dialect usually can see the list's element type. But an operand whose fact is `Unknown` or `Bottom`, or a JSON-typed operand, would escape the refusal. So would a struct element with a Decimal field, and a nested list.
- **Where:** the refusal belongs in `sql/dialect` next to `SameValuesForm.java:40-49`. It must cover `NULL_SAFE_EQUAL`, `IS_DISTINCT_FROM`, not-equal, and `IN`/`LIST_CONTAINS` over lists, not only `EQUAL`.
- **Unmeasured claim:** "Integer/String/Boolean text equality is the rule" on JSON-text databases is not measured. Possible failures: Boolean spelled `1`/`0` against `true`/`false`, SQLite storing a REAL in an INTEGER column, a written literal's spacing.
- **Other databases:**
  - BigQuery has no ARRAY `=`: that is a database error, not a refusal by name.
  - Snowflake arrays hold VARIANTs.
- Probe each database first.

### 8. The batch list

**SHOULD-FIX: rows marked "record"**
- **B2 is a value where Pure raises.** AGENTS.md's "never accepted for cost" list includes "a NULL where Pure raises". A quiet false is the same class. Put it to the user as an exception to that rule, not as an ordinary limit.
- **B9 ("which of two equal exact numbers `max` keeps")** needs Pure's answer stated beside lite's (a left fold keeps the last written, `NamedRule.java:90-98`) and a register row.
- **B10** must be a ruled register row: it departs from legend-engine's answer.
- B6, B7 (S430 covers it) and B13 are fine as recorded.

**Missing from the list:**
1. **Door answers moved by the shared `Equalities.equal`**, which `numbersOfThreeTypes` uses at both doors:
   - the empty ladder for computed lists (`Equalities.java:99-107`);
   - a date-time carried as text, compared by instant (`:136-146`);
   - one value against a list (`:161-169`).

   This contradicts "what changes" §2, "keep the answers they had… for whole lists".
2. **HeldKinds does not apply to whole sides.** `RootComparison.kind` (`:365-374`) reads the held INTEGER and FLOAT columns of Number-declared list sides and answers false. The same columns compared as single values are refused (`HeldKinds.java:139-143`). The ruled rule says "refused at planning".
3. The `markKept` gaps and the `[0..1]` both-sides NULL (from 4).
4. `OrderView`'s permissive default, and the knock-on moves at the doors (from 5).
5. The root versus inside difference for parameters (from 3).

### 9. Other problems

- **SHOULD-FIX: 2.6's `COALESCE(('l'||text(l))=('r'||text(r)), l IS NULL AND r IS NULL)`.**
  - It writes each side twice. A side that is a subquery doubles the SQL, against the size pins; `falseWhereASideIsEmpty` deliberately writes once.
  - It casts to VARCHAR an `Any` side that may hold an instance, which dialects refuse. Restrict it to primitive kinds, or write each side once with `CASE`.
- **NOTE: dead parameters.** After `numbersOf` goes, the `NumberFamilies` parameters threaded through `SideEqualities` (`Pair.under`, `tables`, `kindsOf`) only ever receive `TWO_FAMILIES` from the planner. Delete them or document them.

**Sound:** the interface direction (the planner implementing a lowering interface), deleting `numbersOf`, `HeldKinds` staying behind the lookup, and the design's own probe for table-valued lets.

**Could not determine by reading:** whether today's root plans a parameter side correctly; how a nested `WITH` renders in judge side plans on H2, MySQL and MariaDB; the JSON text spellings on SQLite, MySQL and MariaDB; how DuckDB, Postgres and Trino compare lists holding NULLs or Decimals of different scales.
