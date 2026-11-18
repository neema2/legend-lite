# LOWERSQL: verdict INCOMPLETE

## Summary (at most 12 lines; answer your brief's questions first)

1. **Final train SQL shapes: I found no wrong answer on any dialect.** L10: no product dialect reads a frame name (`AS_OF_MATCHED` is the only frameName compare, `CarrierStrategies.java:738`), so dropping the name changes only structure. The one real structural change is that a join with a dedup-union side loses its `SELECT * FROM (join)` wrapper. That is the shape main already wrote for `concatenate` (UNION ALL, frame null) in joins. The derived table around the UNION itself stays. The new `!u.all()` prune rule is correct, and it is what keeps L10 answer-neutral. executeValues does not touch product answers (the product form is the identity). L14 leaves statement and window ORDER BY unchanged on every dialect, because bare and explicit keys render alike. Aggregate-internal ORDER BY renders bare keys differently, but no product relation-sort key reaches one.
2. **The one product change nobody measured is WHERE conjunct order (LOWERSQL-3).** On main the product ordered conjuncts by zone (user, correlation, temporal). It now appends them in encounter order, so temporal and correlation predicates come first. The "lean baseline unchanged" claim rests on character counts, which cannot see a reorder. The PCT lanes run the edge style (`PureText.UPSTREAM`), which keeps engine zones, so no lane measures this either. By my analysis it can only remove spurious raises on databases that short-circuit left to right.
3. **Upstream meaning still in lowering (question 2): yes.** L10 moved the union frame's *name* to the edge, not its *shape*. legend-engine's union framing (the re-wrap, the post-join wrapper, self-alias reads, the prune exemption) still runs in the product's `Lowerer`, `Fold` and `SubselectPrune`, switched by a nullable String (LOWERSQL-1). The census closed it by turning literal compares into compares against the form's answer. Smaller items:
   - the planner computes legend-engine's exists-join chooser (`!temporal`) (LOWERSQL-4);
   - H2's legend-engine `NON_KEYWORDS` session list stays in the product (ruled S5; LOWERSQL-7);
   - `VerdictSql` builds legend-pure's assertContains bracket rule (cross-area).
4. **Layer rules: kept.** Five `default ->` arms were added, none in a render dispatch. No SQL text was added to lowering. The MIR lost its edge-only nodes. Quality debt: dead code (LOWERSQL-2, -5), an inaccurate comment (LOWERSQL-3), a false comment in the H2 settings (LOWERSQL-6), and about 9 new orphaned javadocs.

## Blockers (if any; each one also appears in Findings)

None. No finding is a wrong answer, a broken goal of record, a weakened guard, or a commitment that conflicts with the next program. Every structural finding is local to the lowering and cheap to move later.

## Strengths

- **Most two-form kinds meet the hook criteria cleanly.** `LoweringForms` (`LoweringForms.java:43-105`), `WhereZones` and `VerdictWords` are interfaces with no statics or defaults. `ProductLoweringForms` (`ProductLoweringForms.java:25-130`) answers lite's form as an identity or a single product call for 14 members. That makes "no product answer moves" true by construction for executeValues, computedPruning, existsJoin and whereZones' `keysWhere`. The edge composes legend-engine's form from product building blocks (`EdgeLoweringForms.java:111-139`: `SubselectPrune.prune`, `ExistsJoins.rewrite`, `oneBare`). Verified.
- **A real correctness invariant came out of L10.** `SubselectPrune.java:402-407` never prunes a UNION DISTINCT's columns, whatever its frame. `LowerRelationDuckDbTest.dedupUnionKeepsUnreadColumns` (test line 258) shows 3 rows without the rule and 4 with it. I checked every `SqlUnion` constructor in `core/src/main/java`. The only dedup union is `Lowerer.distinct`'s (`Lowerer.java:2621`), so the rule keeps main's behavior exactly and extends it safely. Verified.
- **L14 is SQL-neutral where it applies.** Every statement and window sort key renders bare as null-largest:
  - `AnsiSqlRenderer.sortKey` (`AnsiSqlRenderer.java:640-663`) and `MariaDb.sortKey` (`MariaDb.java:1039-1048`);
  - the ClickHouse and Databricks `sortKeyOf` (`ClickHouse.java:2565`, `Databricks.java:1689`);
  - `BigQuery.windowKey` and `nullsFirst` (`BigQuery.java:2757-2762`) and MongoDB (`MongoDb.java:2390`, `MongoDbPipeline.java:1148`, `1295`).

  So the explicit stamp writes the same SQL on all 13 product dialects. Deleting `TypedSort.pureNullOrder` removes a provenance flag from typed HIR. Verified.
- **The MIR got purer.** `VarSetPlaceholder`, `TempTableInSplice` and `DeferredTdsString` (engine plan-text vocabulary) left the sealed roots. `TDS_NULL_CELL = "TDSNull"` became `NULL_CELL_WIRE = "␀"`. The `EngineStyle*` dialects left `sql/dialect` for `edge`. Raises are typed data: `RaiseKind`, `SqlExpr.RaiseText`. Verified.
- **Layer rules held.**
  - None of the 5 added `default ->` arms is in a render dispatch: `LetBinding` helper switches, `Scalars.tieKey`, `AnsiSqlRenderer.alreadyWhole`, `SQLite.holdsADecimal`.
  - No SQL keyword appears in a string literal added to `lowering/`.
  - `CastPolicy.lower` now asks `forms.wireCast`, which replaces the `engineText` boolean.

  Verified.
- **`ProductFormsUseTheirArgumentsTest` is a well-built guard.** It calls every product form with sentinel functions. Its `RULED` entries carry reasons (for example `relations` arg 1 and `view` arg 1), and `UNCALLED` is empty.

## Findings

### LOWERSQL-1 [WRONG-DESIGN] [FIX-AFTER] L10 moved the union frame's name to the edge; legend-engine's union framing shape still runs in the product lowering
- Where:
  - `core/src/main/java/com/legend/lowering/Lowerer.java:2968-2979` (the left re-wrap and the right frame name), `:3002-3012` (the post-join `SELECT * FROM (join) AS "unionalias_N"` wrapper), `:3016-3029` (`unionFramed`, `unionSide`, `unionFrame`) and `:1468-1478` (group-by keys keep the self-alias for union reads);
  - `Fold.java:1631-1666` (`tryProjectAll`'s `unionFrame` parameter, the same self-alias rule);
  - `SubselectPrune.java:78-90`, `:408-411` (the `keptUnionFrame` exemption);
  - `LoweringForms.java:96-99`, `EdgeLoweringForms.java:193-208` (`EngineShape.UNION_FRAME = "unionAlias"`).
- What: `unionFrame()` returns a String: null for the product, `"unionAlias"` for legend-engine's statement shape. Every legend-engine-only behavior tied to that frame is still product code, run when the String is non-null:
  - re-wrapping a union-framed left operand;
  - wrapping a join that contains a union frame in one more frame;
  - keeping the `"unionalias_0"."lastName" as "lastName"` self-alias in projections and group-by keys;
  - the pruning exemption.

  The census line (L-A.unionAlias, `UpstreamBranchesTest` SITES 9 -> 0) closed because `"unionAlias".equals(sub.frameName())` became `frame != null && frame.equals(sub.frameName())`. The branches and their bodies are the same code they were on main.
- Why it matters: hook criterion (c) says every non-product form lives in the edge, and the meaning rule says renaming does not close a finding. Here only the name moved; the form is in the product. Compare `view()`, where the edge writes `SqlSelect.starOf(new SqlSource.Subselect(body, alias, view))` itself (`EdgeLoweringForms.java:216-218`). That is the design done right. The edge golden net is byte-for-byte, so the edge's union SQL cannot be redesigned without it reading as a regression. That is the same trap the handoff's §4 item 3 describes.
- Instead: give the form the decision, not a name. For example, `SqlSource unionJoinSide(SqlSelect side, Supplier<String> alias)` and `SqlSelect unionJoined(SqlSelect joined, Supplier<String> alias)`, where the product's form is `asLeftJoinSide` and the identity, and the edge's form wraps. The self-alias rule would become an edge post-pass over the MIR (like `PurePrintForms`) or an `EngineStyleH2` render rule, which already reads `"unionAlias"` at `EngineStyleH2.java:1053, 1277, 1332`. Then delete `unionFrame()`, `Fold.tryProjectAll`'s parameter and `SubselectPrune`'s `keptUnionFrame` (see LOWERSQL-2).
- Confidence: verified (read at the head, diffed against main's `Lowerer.java:2860-2936` and `SubselectPrune.java:401`).

### LOWERSQL-2 [COSMETIC] [FIX-AFTER] `SubselectPrune`'s `keptUnionFrame` exemption is unreachable, so the edge's `prune(q, false, UNION_FRAME)` and its planner-entry pin hold a dead parameter
- Where: `core/src/main/java/com/legend/lowering/SubselectPrune.java:402-411` (`if (!u.all()) return null;` comes before `if (r.keptUnionFrame() != null && ...)`), `:48-54`, `:82-90`; `EdgeLoweringForms.java:197-198`; `ArchitectureTest.PLANNER_ENTRIES` lists `prune(SqlQuery, boolean, String)` (L10 commit `61e5205d1`).
- What: the only `Subselect` with frame `"unionAlias"` around a `SqlUnion` is `Lowerer.distinct`'s dedup union (`Lowerer.java:2621-2623`, `all == false`). The other `"unionAlias"` subselects wrap a `SqlSelect` (the join wrappers). Every other `SqlUnion` constructor in `core/src/main/java` builds `all == true` with a null frame. The new `!u.all()` early return therefore always fires first, and the `keptUnionFrame` branch can never return.
- Why it matters: it is a pinned public entry, a record field and an edge argument that do nothing. Readers and the edge golden net believe the engine shape depends on it.
- Instead: delete `prune(SqlQuery, boolean, String)`, `Refs.keptUnionFrame` and the edge's third argument, and un-pin the entry with a dated note.
- Confidence: verified (enumerated with `grep -rn "new SqlUnion(\|new com.legend.sql.SqlUnion(\|SqlUnion.ofBranches("`).

### LOWERSQL-3 [WRONG-DESIGN] [FIX-AFTER] The product's WHERE conjunct order moved (zone order to encounter order), unmeasured and unregistered; the javadoc says order is "no answer's"
- Where:
  - `core/src/main/java/com/legend/lowering/ProductLoweringForms.java:110-123` (`APPENDED.merge`); on main, `lowering/WhereMerge.java` was applied unconditionally from `Lowerer.mergeWhere`;
  - `Lowerer.java:2326-2340`;
  - the stamps come from `resolver/TemporalFrame.java:1593`, `:1998` (TEMPORAL, the inner filter over the class pipeline) and `resolver/Substitution.java:2819-2824` (`filter(filter(exPipe, corr, CORRELATION), memberPred, NONE)`).
- What: main's product wrote `user AND correlation AND temporal` whatever the order the filters folded in. The PR's product writes them in encounter order, inner filter first, so temporal and correlation predicates now come before the user predicate. Examples:
  - `Person.all(%2020-01-01)->filter(p | $p.name == 'x')`: main wrote `WHERE name = 'x' AND from_z <= .. AND thru_z > ..`; the PR writes `WHERE from_z <= .. AND thru_z > .. AND name = 'x'`.
  - The correlated EXISTS subquery of a to-many membership: `t.fk = outer.pk AND t.x = 'v'` instead of the reverse.

  The commit's evidence ("the lean SQL baseline is unchanged in every row", `61e5205d1`) cannot see this: `LeanSqlBaseline` and `LeanSqlCorpusTest` measure character, statement and plan-JSON counts, and a reorder is character-neutral. The temporal tests use `sql.contains(...)` (for example `ResolveTemporalContextTest.java:133`), which is order-blind. The PCT lanes cannot see it either: they run `PureText.UPSTREAM` (`pct/.../PctBackend.java:73`), whose `EdgeLoweringForms.whereZones()` keeps engine zones.
- Why it matters: in value, AND commutes (three-valued). Order can still decide whether an erroring operand runs, on databases that evaluate left to right with short-circuit (SQLite, MySQL and MariaDB in practice; H2). The product writes checked raises inside user predicates. Concrete input: a milestoned `Person` with one row outside the business date where `x = 0`, and `Person.all(%2020-01-01)->filter(p | 10 / $p.x > 1)`.
  - On main, the user zone came first, so `CASE WHEN x = 0 THEN raise ... END > 1` can run on the out-of-range row and raise DIVIDE_BY_ZERO for a row Pure never sees.
  - With the PR's order, the temporal predicate can exclude it first.

  The user predicate never moves earlier than on main: TEMPORAL and CORRELATION predicates are column comparisons with no raise. So by my analysis the change can only turn a spurious error into the right rows; I found no case that makes an answer wrong. Still, it ships under a stated claim of "no product SQL moved", and the comment at `ProductLoweringForms.java:110` ("conjunct order is no answer's") is inaccurate.
- Instead:
  - Reword the javadoc: "AND commutes in value; an operand that can raise is guarded by its own CASE, never by conjunct order".
  - Add one SQL-text test that pins the product's order for a temporal filter and for an exists correlation.
  - Add the input above to `//core:product_raise_probe` on SQLite and MariaDB.
  - Decide with the user whether this needs a register row. It moves toward Pure's semantics and away from legend-engine's order, which the product does not owe.
- Confidence: verified for the reorder (read both code paths); suspected for the raise example (not run).

### LOWERSQL-4 [WRONG-DESIGN] [NOTE] The product planner still computes legend-engine's exists-join chooser rule (`!temporal`) and hands it to the lowering as `existsJoinable`
- Where: `core/src/main/java/com/legend/planner/ProductStatements.java:385-404` (javadoc: "the engine's exists-join form unless a temporal class is read"), `Compiler.java:977-980`, `Lowerer.java:2242-2247`.
- What: the product's form ignores `existsJoinable` (`ProductLoweringForms.java:84-86`). The flag exists only so legend-engine's form knows when legend-engine would write its second exists emission. The product computes that decision (not milestoned, therefore joinable) and passes it in.
- Why it matters: by the meaning rule this is a small piece of upstream's alternative computed in the product. It is closed in the census as "the planner's own fact" (J40), but the fact here is "a temporal class is read", and the rule is legend-engine's.
- Instead: carry the neutral fact ("this statement reads a temporal class") in `ExistsSites` and let `EdgeLoweringForms.existsJoin` apply legend-engine's rule.
- Confidence: verified.

### LOWERSQL-5 [COSMETIC] [FIX-AFTER] Product halves that nothing calls: `CastPolicy.castByPolicy(SqlExpr, Type, Type)` hardcodes the product form; the product's `WhereZones.keysWhere` is never called
- Where: `core/src/main/java/com/legend/lowering/CastPolicy.java:175-177`; `ProductLoweringForms.java:125-129`.
- What:
  - The three-argument `castByPolicy` hardcodes `ProductLoweringForms.INSTANCE` and has no caller. The only callers use the five-argument form with `styled().forms()` (`Lowerer.java:1824`, `:1974`).
  - The product's `keysWhere` is reachable only through `ExistsSites`, which the product's `existsJoin` ignores. The edge's `whereZones()` returns its own `EngineWhereZones`, and ClickHouse calls `ExistsJoins::encounterOrder` directly.
- Why it matters: a hardcoded product form in a lowering helper is the "implicit no-edge default" the audit's K6 removed elsewhere. As dead code it is a trap for the next caller.
- Instead: delete the three-argument overload. Leave the product's `keysWhere` as the interface requires, but say in its javadoc that the product never reaches it.
- Confidence: verified (`grep -rn castByPolicy`).

### LOWERSQL-6 [COSMETIC] [NOTE] `ProductDatabaseSettings` says null placement is "written on every sort key", but H2's ordered `JSON_ARRAYAGG` leaves a bare key to the new `DEFAULT_NULL_ORDERING=LOW`
- Where: `core/src/main/java/com/legend/sql/dialect/ProductDatabaseSettings.java:9-11`; `H2.java:1213-1222` (`listOfValues`: `k.nullOrder() == null ? ""`); compare `H2.java:1171-1176` (`h2Aggregate` pins null-largest for a bare key).
- What: J22, user-ruled 2026-11-17 and part of this PR, moved the product's H2 from legend-engine's `DEFAULT_NULL_ORDERING=HIGH` to H2's `LOW`. Statement, window and `h2Aggregate` keys are all explicit, but `listOfValues` writes a bare key with no NULLS clause.
- Why it matters: today no product LIST reducer gets a nullable bare order key. The ordered aggregates are `joinStrings` (STRING_AGG, which is pinned) and `gridReport`'s non-null ordinals. So no answer moves now, but the stated invariant is false and the next ordered LIST would silently sort nulls first on H2.
- Instead: make `listOfValues` pin null-largest for a bare key, as `h2Aggregate` does, or correct the javadoc.
- Confidence: verified (read); no failing input found.

### LOWERSQL-7 [WRONG-DESIGN] [NOTE] H2's product dialect still sets legend-engine's `NON_KEYWORDS` on every session ("so a golden replayed here reads as it did in the engine's run")
- Where: `core/src/main/java/com/legend/sql/dialect/H2.java:33-43` (`NON_KEYWORDS`, `SESSION_SETUP`, made `public` by this PR), `:166-219` (`ENGINE_LEXICON`, "the GOLDEN TEXT channel's set", and `FREED_BY_THE_ENGINE`).
- What: J22 moved the sibling `DATABASE_SETTINGS` (`MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH`) to a two-form. The session half (H2Manager's non-keyword list) stays in the product, and the product's `LEXICON` derives from legend-engine's reserved list.
- Why it matters: this is upstream meaning on every product H2 connection, though RULED (register S5, 2026-11-16; `CompatConstantPoolTest.java:73`), so it is a known residue, not a defect of this PR.
- Instead: when the portability program re-rules kind B, make the session setting a `DatabaseSettingsForm`-style two-form. The product's H2 already quotes every keyword it needs (`H2.ident`, `:216-220`).
- Confidence: verified.

### LOWERSQL-8 [COSMETIC] [FIX-AFTER] Comment debris: orphaned or back-to-back javadocs, a stranded comment, tombstones, blank runs
- Where (count: back-to-back javadoc blocks in the area's files rose from 87 on main to 96 at the head, roughly 9 new):
  - `sql/ExistsJoins.java:298-299` (two javadocs on `mergeAnd`, the first orphaned);
  - `sql/dialect/AnsiSqlRenderer.java:913` (the `/** Whether {@code c} is a parseFloat */` javadoc sits on `alreadyWhole`, and `isParseFloat` has none);
  - `H2.java:1291-1298` (`parseInteger`'s javadoc follows `variantAwareCast`'s, which is now orphaned).
- Also:
  - `Lowerer.java:537` ("J-tail: demand-driven subselect column pruning") now sits above the unrelated `scalarRootShape` block;
  - 19 `// (X: gone/moved 2026-11-..)` tombstones in `lowering/` and `sql/` (7 in `SqlFn.java`), where main had 0;
  - blank runs at `RelationTexts.java:53-56`, `LoweringForms.java:100-101` and `ProductLoweringForms.java:108-109`;
  - 316 added lines with inline `com.legend.x.Y` FQNs;
  - 233 added lines over 120 characters.
- Why it matters: readability of the two classes a redo will read first (`Lowerer`, `LoweringForms`).
- Instead: one cleanup pass. Put tombstones in the commit log, not the source.
- Confidence: verified (`scratchpad/audit/verify/dupdoc.py`, the counts below).

## Cross-area observations

- **EDGE, L14:** `edge/RelationTextLowering.java:618-650` (`hoistOrder`) copies the relation's sort keys, `k.nullOrder()` included, into the text aggregate's ORDER BY. Before L14, a legacy string-key sort (`sort('a', SortDirection.DESC)`) had bare keys. Those rendered with the database default inside an aggregate (`AnsiSqlRenderer.aggOrderNullPlacement` gives `""`; BigQuery's `aggregateKey` puts nulls last). They now carry an explicit null-largest clause. So on the edge style (PCT, channel B), `...->sort('a', SortDirection.DESC)->toString()` with a null in `a` on DuckDB (default NULLS LAST) or BigQuery likely moves the null row from last to first. That probably fixes a disagreement with the relation's own ORDER BY. But it is an edge output change that the edge golden net evidently did not cover ("byte-identical"). `EngineStyleH2` suppresses the clause, so the corpus is unaffected. Suspected; check on a lane.
- **EDGE, executeValues:** `EdgeLoweringForms.executeValues` (`:69-74`) now folds `serializeGraph(g, false)` where main folded `serializeGraph(g.asArrayWrapped(), false)`. For a root whose `arrayWrap` is false and which yields 0 rows or more than 1 row, `oneBare` returns the unaggregated rows unchanged (no `JsonArrayAgg`). Main would have given the array. The edge contract (0-26) said byte-identical, so no recorded case covers it. Suspected.
- **JUDGES, the meaning rule:** `lowering/VerdictSql.java:588-596` builds legend-pure's assertContains bracket rule in product lowering ("upstream's own words (assertContains.pure ...)": a one-element list re-bracketed) and passes it to `words.notContainedCanons`, which `LiteVerdictWords:76-78` also uses. Whether lite's sentence reaches it (when `valueWritten == null`) or only the edge's does, this is product code computing upstream's form.
- **GUARDS:** `PureOnlyTexts`, `RelationTexts` and `ResultEnvelopes` still carry `@OpenLine` and are counted as `HOOK-RULED` in `EdgeSeparationTest.java:151-175`. `RelationTexts` holds two static forwarders on a hook interface (`RelationTexts.java:50-60`), ruled as a facade. These are fine as ruled, but "census 0" includes ruled open lines.
- **PRODUCT, verification gap:** the PCT lanes (channels A and B) plan with `PureText.UPSTREAM`. So the database lanes in handoff §7.2 do not exercise these product-only shapes:
  - L10's appended WHERE order;
  - J19's correlated EXISTS (on main, the product wrote legend-engine's join form unless temporal: `main:ProductStatements.java:397-399`);
  - J22's H2 settings;
  - S20's double Float literals.

  Only CI's core tests (DuckDB, H2, Postgres) and `//core:product_raise_probe` (raises and text forms) do. J19 in particular changed the product's EXISTS shape on Trino, BigQuery, Databricks, Snowflake and MongoDB with no product-style lane run. That is earlier than the final train, but in this PR.
- **PRODUCT:** S20 leaves the product's Float literal kind per dialect: DOUBLE on most, DECIMAL on ClickHouse, BigQuery and Snowflake (`AnsiSqlRenderer.java:2827-2830`, follow-up F39). Product answers for `0.1 + 0.2` therefore differ across databases until F39 lands. This is ruled and queued; noted for G2.

## Measurements (numbers with the command that produced them)

- Area size: 119 files, +6256 -3204. Command: `awk -F'\t' 'NR>1{a+=$1;d+=$2;n++}END{print n,a,d}' audit/units/LOWERSQL.txt`.
- Added lines: 5793. Inline `com.legend.x.Y` FQNs in added code: 316. Added lines over 120 characters: 233. Command: `git diff main...68f20eb4d -- core/src/main/java/com/legend/sql core/src/main/java/com/legend/lowering > lowersql.diff`, then `grep -c "^+[^+]"` and the filters shown in the session.
- `default ->` arms added in `lowering/` and `sql/`: 5, all in helpers or predicates, none in a render dispatch. 14 removed. Command: `grep -n "^+.*default ->" lowersql.diff`; `grep -c "^-.*default ->"`.
- The `"unionAlias"` literal outside `edge/` and `lineage/`: 0. Command: `grep -rn '"unionAlias"' core/src/main/java | grep -v "edge/\|lineage/"`.
- frameName compares in product dialects: 1 (`AS_OF_MATCHED`, `CarrierStrategies.java:738`). Command: `grep -rn "frameName" core/src/main/java/com/legend/sql/dialect/*.java` with rebuild sites excluded.
- `sortKey` overrides: 2 (`AnsiSqlRenderer`, `MariaDb`). Command: `grep -ln "SqlWriter sortKey(SqlWriter" sql/dialect/*.java`.
- `styled()` calls in `Lowerer`: 30, of which `styled().forms()`: 20. `Lowerer.java`: 4,942 lines. Command: `grep -c`, `wc -l`.
- Back-to-back javadoc blocks in the area's files: 87 on main, 96 at the head. Command: `python3 -I audit/verify/dupdoc.py <files>` over `git show main:` copies and the head.
- Tombstone comments `// (... gone/moved/deleted ...)` in `lowering/` and `sql/`: 19 at the head (7 in `SqlFn.java`), 0 on main. Command: `grep -rnE "^\s*//\s*\(.*(gone|moved|deleted)"`.
- `SqlUnion` constructions with `all == false`: 1 (`Lowerer.java:2621`). Command: `grep -rn "new SqlUnion(\|new com.legend.sql.SqlUnion(\|SqlUnion.ofBranches("`.
