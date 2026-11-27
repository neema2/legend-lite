# The second independent review (of `268591b9c`, section 5 alone)

Read-only, by an independent reviewer (a subagent given the design and the rules, never the author's
conclusions), 2026-11-27. Kept verbatim as evidence; what was done with each finding is in
`step-6-closing-design-2026-11-27.md` (sections 5 and 6) and `step-6-closing-batch-2026-11-27.md`. Paths the report
writes under `/home/user/ll-audit...` are a read-only worktree of this repository.

---

**Verdict: section 5 cannot be built as written. Three blockers would ship wrong answers on ordinary input, and several claims need correcting.** I only read code and docs at 268591b9c; nothing was run.

## BLOCKERS

**1. In 5.3, "`!` beneath a condition" is not a safe condition position, and neither is `forAll`'s predicate.**
- Under positive polarity the database's NULL is the same as false. Under a negation it is not.
- `!` directly over `==` is safe today: `Scalars.java:200-212` calls `NullSemantics.negate`, which at `NullSemantics.java:157-163` rewrites `EQUAL` to `NULL_SAFE_NOT_EQUAL`.
- `!` over `&&` or `||` falls through to a bare `NOT` (`Scalars.java:211`). So `filter(r | !($r.nick == 'a' || $r.id == 7))` drops a row whose `nick` is empty. Pure keeps it. This is already wrong today, and marking those equalities as conditions would lock it in.
- The relation `forAll` writes `NOT pred` in its WHERE (`Lowerer.java:4984-4986`) and gets that predicate from the same `tryPredicate` as `filter` (`Lowerer.java:4978` and `2273`). The natural place to mark conditions would therefore mark a negated one. `NOT NULL` excludes the row, so `NOT EXISTS` answers true where Pure says false.
- The list `forAll` ignores NULL on every dialect:
  - DuckDB `list_bool_and` (`DuckDb.java:1024`)
  - Trino `all_match` with `coalesce(..., TRUE)` (`Trino.java:701`)
  - Snowflake's `FILTER(NOT p)` count (`Snowflake.java:1089`)
  - SQLite's `min(CASE ...)` (`SQLite.java:1146`)
- **Rule to write:** condition-ness passes through `&&` and `||` only. `!` directly over an equality may stay unguarded, because `negate` rewrites it. Anything else under `!`, and every `forAll` predicate, is guarded. `filter` and `exists` are positive and safe.

**2. "Guard every other equality" will not reach several lowering paths.**
- The guard is applied only inside `Lowerer.scalar` (`Lowerer.java:3699-3704`).
- These paths call `Scalars.lower` directly and never pass through `scalar()`:
  - window columns: `windowScalar` (`Lowerer.java:3551-3554`), which is A9's own probe row
  - `overReducers` (`Lowerer.java:2362-2368`)
  - `aroundReducer` (`Lowerer.java:2344`)
- The guard has to sit where the equality rule is invoked, not in `scalar()`.

**3. The B2 form, as sized, answers false when both list sides are empty.**
- Today `SideEqualities.values` answers "both empty" as true (`SideEqualities.java:128-129`, `noRows AND noRows`). Pure's `[] == []` is true.
- `COALESCE(MIN('l'||v) = MIN('r'||v), FALSE)` turns that into false.
- Keep the both-empty arm, as 5.7 does for single values.
- `neverEqualButEvaluated` (`Equalities.java:182-188`) has the same flaw for single values; that is B3's case.
- Where the constant comes from today: `RootComparison.kind` (`RootComparison.java:365-374`) reads the held kind from the plan's output. `values` then takes `bothEmptyOnly`, which writes `NOT EXISTS`. An `EXISTS` prunes the value columns, so no element is evaluated.
- `LiteCrossTypeRule.atTheCall` answers without evaluating at three places:
  - line 174-176, `manyValued` for an equality
  - line 182-185, membership and `indexOf`, which filter out many-valued sides
  - line 197-215, `harmless`, which counts a variable as harmless even when its `let` value raises
- An aggregate like MIN has to read every row, so it is a sound vehicle. I did not verify whether optimizers rewrite `COUNT(v)` to `COUNT(*)`; if they do, COUNT would not be a safe alternative.

## SHOULD-FIX

- **5.3's null-safe form for two optional sides must hang off the door's question, not go into `nullArms`.** Both doors' rule reaches `Equalities.equal` (`Scalars.java:3848-3849`). Putting it in `nullArms` would make legend-engine's door write null-safe for a computed side, where 5.9 measured a plain `=`.
- **5.9's three cases are met today** inside a store query at legend-engine's door:
  - two plain optional columns are null-safe (`Equalities.java:349-354`)
  - anything else is a plain `=` (line 366)
  - the negation is null-safe (`NullSemantics.java:84`)
  - Exceptions:
    - Two literals give a bare `<>`.
    - An enumeration takes the OR expansion, which is not null-safe when both sides are empty (`NullSemantics.java:122-143`).
    - `isOptional` requires an upper bound of 1 (`NullSemantics.java:57-61`), while the code's own comment says upstream's rule is "both lower bounds 0" (`Equalities.java:327-330`).
    - A column wrapped in a cast is no longer a `Column`.
- **"A miss costs a few characters" understates the cost.** Many recognisers read WHERE, ON and CASE equalities:
  - `KeyedCounts.java:211`, `494`, `560`
  - `ExistsJoins.java:195`
  - `GraphAggDecorrelate.java:91`
  - `Lowerer.java:3108`
  - `DecodeShapes.java:72`, `114`
  - `OuterJoinResidualsAsMatches.java:81`
  - `ClickHouse.java:295`, `396`
  - `Snowflake.java:3266`
- If one of those equalities were wrapped, Snowflake and ClickHouse as-of joins would be refused, an outer join would be rewritten, and hash joins would be lost. The size pins would not catch any of it.
- The backed-out version 1 also hit `exists` conditions, enumeration decodes and a union's route-key join (`step-6-one-equality-2026-11-27.md:316-319`). The condition list must name those.
- **5.4, "weaker, never a new failure", is wrong for `assertNotEquals`.** At the doors it is left as written, so lite refuses it (`UpstreamTestBodies.java:87-89`). Every newly unordered side there becomes a new refusal.
- **5.4, "nothing else moves", is wrong.** These also read `OrderView` and will move:
  - `UpstreamTestBodies.java:348`
  - `UpstreamPrints.java:265`
  - `ReplayGates.java:23`
  - `AssertVerdicts.java:1794`
  - `DatabaseJudge.java:1061`
  - `HostJudge.java:335`, `349`
- **5.4, the new rule lets SORTED flow through unnamed functions such as `reverse`.** `ReplayGates` and `HostJudge.planRendered` read SORTED as "the tail sort holds". Map SORTED to DEFINED there.
- **5.4, a sort's ties.** A sort over store rows on a non-unique key leaves its ties in arrival order. The code says so itself: `HostJudge.java:345-346`, "even sorted queries legally tie-flip". Treating that as ordered can still return a wrong `==` quietly.
- **5.4, telling a parameter from an execution frame (2c).**
  - Nothing tells them apart today: every unbound variable is treated as a frame (`OrderView.java:180-202`).
  - At `RootComparison` the lets are only the statement's (`RootComparison.java:92`, `158`), so the query's parameter names are not passed in.
  - The pattern to copy is `OrderedAsserts.java:72-77`, which seeds lambda parameters as empty lets.
- **5.4, two gaps in the fallback rule.**
  - A function with many values but no many-valued input (`range`, `split`, a map's `keys`, a collection of single values) needs DEFINED stated outright.
  - A map whose mapper reads a to-many property is classified by its source (`OrderView.java:249`); it is the same hole as correction 2.
- **5.2 item 6, lowerers with no planner.**
  - Three build a `Lowerer` with no planner: `Compiler.java:962`, `EnginePlans.java:167`, `SeedableLets.java:39`.
  - `lowerResolved`'s lineage caller turns lowering failures into "no rows" (`PlanAllocations.java:337-341`). The refusal would become a quietly empty lineage, against invariant 4.
  - The seedable-lets probe catches every exception, so a refusal there silently changes which lets seed.
- **5.2 item 6 contradicts 2.5 rows 1-2.** Whether a side reads the row is decided in the planner (`RootComparison.java:183`). A lowerer with no planner cannot tell, so it would refuse the per-row typed-list comparisons 2.5 keeps.
- **5.9, the hour-precision date.** By reading, both compatibility doors answer **true** today, where upstream says false:
  - `date(2015,4,16,14)` lowers to `TEMPORAL_TEXT` (`DateCtorRule.java:129-145`).
  - The partial-precision arm covers only YEAR and MONTH (`Scalars.java:3331-3339`).
  - Both doors reach `Equalities.equal`, whose instant arm (`Equalities.java:141-146`) compares the two as instants.
  - No door rewrite handles date precision.
  - This is an unruled divergence now.
- **5.5, where to write it.**
  - The check must sit before `Typer.compared`'s early returns (`Typer.java:293-301`) and call `EqualityClasses.neverEqual` (`EqualityClasses.java:194`) directly, never `crossTypes`.
  - It reads no door, so it does not conflict with the edition design.
  - If `hasSameElements` is added to `COMPARISONS` as an equality, it also picks up the `atRunTime`/`atTheCall` handling (`EqualityClasses.java:202-215`; `LiteCrossTypeRule.java:174-176`). Lite's rule must then apply at the call too.

## NOTES

- **Null-safe spellings by dialect** (null-safe equality is Pure's answer in all three cases for single values):
  - Most dialects write `IS NOT DISTINCT FROM`.
  - MariaDB and MySQL write `<=>`.
  - MongoDB writes `$eq`.
  - BigQuery compares composites by `TO_JSON_STRING`.
  - ClickHouse writes `coalesce(a = b, isNull(a) AND isNull(b))`, so each side is written twice (`ClickHouse.java:2013`).
- **One null-safe form could replace `COALESCE(l = r, FALSE)`**, except where a one-value side is SQL NULL. `NullSemantics.verbatim` and the legacy null-unsafe feature (`Lowerer.java:232`) rewrite it back to `=`.
- **The stamp at legend-engine's door.**
  - It is set in the inliner and `ClassSources`, kept on rebuilds, and pinned by `NativeCallStampGuardTest`.
  - Comparisons made after the inliner, such as an assert's synthesised `==`, read as inside a store.
  - At its PCT door nothing is stamped (`EngineTable.java:89`). That is consistent with legend-engine sending every PCT case to the database.

## SOUND

- `&&` and `||` under a positive condition, `if`/CASE, WHERE, ON, HAVING, QUALIFY, list `filter` and list `exists`: an unguarded comparison is equivalent there.
- 5.2 item 2: `UpstreamTies` gives `equal`, `eq` and the orderings a row at both doors (`UpstreamTies.java:68-72`).

## Could not determine by reading

- Whether the window `extend`, the relation `distinct` or a sorted source isolated in a subselect keep order in the SQL written.
- Whether every TDS sort becomes a `TypedSort`.
- MongoDB's `$eq` for a missing field against null.
- The lowering path of the union's route-key join.
