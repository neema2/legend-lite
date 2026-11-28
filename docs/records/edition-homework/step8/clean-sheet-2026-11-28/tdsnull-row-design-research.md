*A research or audit subagent's report to the step 8 session, 2026-11-28, kept as it was written (read-only work; its scratch files were in the session's container and are not in the repository).*

TDSNull as a table row: design research (read-only; nothing edited, nothing built)

Paths: M = /home/user/s8-edit/core/src/main/java/com/legend, T = /home/user/s8-edit/core/src/test/java/com/legend. Everything cited below was read unless marked "grep".

## 1. Inventory

**Where CELL_TEXT is emitted (three sites, no others in main; grep)**
- (a) M/lowering/Coercions.java:165-167: the rule of `Pure.Lite.CELL_TEXT` (`AT_LEGEND_LITE_CELL_TEXT`) writes `CELL_TEXT(args[0])`.
  - Typed shape: `cellText(toString(trustOne(cell)))`, minted in M/compiler/spec/TdsDesugars.java:154-170 for `TDSRow.get('c')->toString()`.
  - It reaches SQL through `Scalars.lower`.
- (b) M/lowering/Scalars.java:901-946: makeString's rule (`AT_STRING_MAKE_STRING`, a `StyledRule`).
  - Lines 931-934 write `LIST_TRANSFORM(coll, x -> CELL_TEXT(elementText))`; lines 936-939 wrap it in `COALESCE(STRING_AGG(.., sep), '')`.
  - No CELL_TEXT in the mixed-list branch (921-923) or the honest `[0..1]` branch (933).
- (c) M/lowering/Lowerer.java:4527-4547 (`scalarValueTailArms`): makeString or joinStrings whose first operand is a `TypedCollection` with `rowCells()`.
  - That flag is set only by TdsDesugars.rowCells (TdsDesugars.java:788-814), called from Typer.java:1550-1558.
  - Each cell is `CELL_TEXT(Fold.cellText(type, scalar(cell)))` (Fold.java:1588-1604), then `ValueCollections.rowCellsJoin` (M/lowering/ValueCollections.java:398-412) writes `COALESCE(STRING_AGG(ArrayLit(cells), sep), '')`.
  - This arm never reaches `Scalars.lower`, so no table is asked today.

**What the pass does (M/edge/PurePrintForms.java)**
- Shape (b), lines 55-60: the lambda body becomes `COALESCE(x,'TDSNull')`; the outer COALESCE stays.
- Shape (c), lines 61-77: the whole `COALESCE(STRING_AGG(ArrayLit..))` becomes `CONCAT(c1', sep, c2', ..)`, or the one cell alone; an empty-literal separator is left out (68-69).
- `TdsNullCells` (42-47): any CELL_TEXT left becomes `COALESCE(x,'TDSNull')`.
- The walk is bottom-up (M/sql/SqlRewriter.java, class doc).

**Who runs it**
- M/sql/dialect/AnsiSqlRenderer.java:260-263 runs `spellingPasses()` first, before `CellTexts.Expansion`/`Rest` (271-272).
- All 12 edge dialects override it: M/edge/PureText.java:450, 468, 494, 506, 517, 528, 547, 558, 569, 581, 592, 607. That includes SQLite, MariaDB, MySQL and MongoDB.
- All four edge styles use those dialects: UPSTREAM (PureText.java:105-107), PURE_V1 (244-246), ENGINE_TESTS (304), ENGINE_PCT (312).
- So do the direct `PureText.dialect(..)` renders of plan text: M/edge/EnginePlans.java:88, 264-294, 436, 451, 531, 695, 703, 838 and EngineAllocations.java:133. Those lower with `env.styled()` (EnginePlans.java:171), so they see the style's table.
- The pass is blind to the stamp and to the database.

**How rows are looked up**
- M/planner/PlanEnv.java:178-205: `styled()` holds `edition().table(db)`.
- M/lowering/Scalars.java:2403-2419: `doorsRule` calls `styled.edition().lowersAs(call, features)`; a non-null answer goes to `NamedRules.lower(named, call, loweredArgs)` and replaces the call's own rule.
- The only caller of `Scalars.lower` is `Lowerer.written` (Lowerer.java:438). The Lowerer also asks `doorsRule` itself at 443, 488 and 513.
- EngineTable.lowersAs (M/edge/EngineTable.java:242-278) answers in this order:
  1. the two-number rule (252);
  2. `UpstreamTies.rule(call)` (257-260), before the stamp and the database, never refused;
  3. `if (call.outsideStore()) return null` (261): the "inside a store query" gate;
  4. `rows()` (311-338): `WHERE_WRITTEN` (157, 192-199, 327-330) is a row only where its database set holds, otherwise the call's own rule; `AS_GIVEN`/`CORRECTED` rows are refused by name on another database (274).
- PureTable.lowersAs (M/edge/PureTable.java) is `UpstreamTies.rule(call)` alone, with `rowsByDatabase()` false.

**What a rule sees**
- `NamedRules.lower(rule, call, args)` (M/lowering/NamedRules.java:25-51) gets the typed call and its lowered args.
- It gets no `Styled`. Rules that need more delegate to `Scalars` helpers (`sortTiesAsWritten`, `extremeKeepingTheFoldsTie`, Scalars.java:3896-3921), which fall back to `ownRule(n)` (3984-3990).
- So (a) is expressible as it stands.
- (b) is expressible only if `NamedRules.lower` is handed `styled`, because makeString is a `StyledRule` (Scalars.java:64-77, 902).
- (c) is not reachable at all today, because the Lowerer arm answers before any lookup.

## 2. Is TDSNull needed at legend-pure's door, and outside a store query?

Yes to both.
- **legend-pure's door:** T/integration/JoinedTextTest.java:46-50 pins `["a","TDSNull"]` and `["1,a","2,TDSNull"]` under `PureText.UPSTREAM`, with a storeless runtime, on the lane's database (DuckDB, H2, SQLite). PCT itself has no such case (e3-design.md ~163: taking TDSNull off "moved no PCT case").
- **Outside `execute()`:** second-review-v2 M7 (lines 89-92) says the corpus harness joins `$r.values->makeString` outside `execute()` in 303 files, and 112 DuckDB / 107 H2 tests depend on it. ENGINE_TESTS stamps test bodies (EngineTable.java:92, 354), and a stamped call gets no ordinary row (261).
- **Any database:** the pass runs on dialects outside `ENGINE_DATABASES` (PureText.java:53-55).

So the rows must be answered the way UpstreamTies' are: read by both EngineTable and PureTable, before the stamp, with no database set, never refused. A `Row(.., on)` or a `WHERE_WRITTEN` entry would lose reach.

## 3. Minimal design

**(c) can be a row without a typer change.** The e3 note's obstacle is real but smaller than stated. The typed tree already carries the fact on a typed call (makeString/joinStrings over `TypedCollection.rowCells()`, construction-declared). What is missing is only that the Lowerer arm does not ask the door. The smallest change is in Lowerer.java:4527-4547. That method is not on your forbidden list (only `asCondition` and `written` are), but it is not on your allowed list either, so it needs your word.

The typer route is strictly larger, and it would move SQL:
- it needs a new native (Pure.java, native-claims.tsv) and its pins (ArchitectureTest.java:4709-4710 pins TdsDesugars at 55; TdsNullTypingPinTest, InternalNativeCallTest, NativeCatalogGovernanceTest);
- a generically lowered rowCells collection rides the variant carrier, not `Fold.cellText`, so the cell texts would change.

**One new NamedRule** (M/lowering/NamedRule.java), named by what it writes. Suggested: `EMPTY_CELL_TEXT_IS_ITS_CLASS_NAME`: an empty TDS cell's text is `TDSNull`, the name of the class of Pure's null-cell value, so a join skips no cell.
- a cell's text: `COALESCE(x, 'TDSNull')`;
- a list's join: each element's text so, inside the same `STRING_AGG`;
- a row's cells joined: the cells and the separator concatenated.
- Alternative: two rules (the cell text; the join). One is enough; `EXTREME_KEEPS_THE_FOLDS_TIE` is the precedent for a rule that dispatches on the callee.

**Rows** in a new edge class beside UpstreamTies, e.g. M/edge/UpstreamEmptyCells.java, a `Map<FunctionId, NamedRule>`:
- keys: `AT_LEGEND_LITE_CELL_TEXT`, `AT_STRING_MAKE_STRING`, and `AT_STRING_JOIN_STRINGS` + `AT_RELATION_JOIN_STRINGS` (the same families the arm tests, Lowerer.java:4528);
- EngineTable.lowersAs asks it right after UpstreamTies (after line 260, before 261);
- PureTable.lowersAs returns `tie != null ? tie : UpstreamEmptyCells.rule(call)`.

**Lowering changes**
1. NamedRules.java: `lower` gains a `Styled styled` parameter; the one call site is Scalars.java:2418. The new arm calls `Scalars.emptyCellsByClassName(call, args, styled)`.
2. Scalars.java (print rules only):
   - move makeString's body (902-945) into a static helper that takes the element wrapper; the product passes `t -> CELL_TEXT(t)`, the rule passes `t -> COALESCE(t, name)`;
   - `emptyCellsByClassName` dispatches: cellText gives `COALESCE(args[0], name)`; makeString gives the helper; joinStrings gives `ownRule(n).apply(n, args)` (its own rule writes no CELL_TEXT); anything else throws `IllegalStateException`, as 3891 and 3920 do.
   - The trees are the ones the pass produces today, including the kept outer `COALESCE(.., '')` for (b).
3. Lowerer.java:4537-4546: compute the cell texts without the wrapper, then ask `Scalars.doorsRule(n, features, styled())`.
   - null: wrap each in CELL_TEXT and call `ValueCollections.rowCellsJoin`, exactly today's code, so the product is bit-identical;
   - otherwise: `NamedRules.rowCells(named, n, cellTexts, rest)`, a second package-private entry that refuses any rule but the new one and calls a new `ValueCollections.rowCellsInterleaved(cells, rest)`.
   - `rowCellsInterleaved` reproduces PurePrintForms.java:68-77 (separator left out when it is the empty `StringLit`; one cell alone; otherwise `CONCAT(parts)`) and, for three extra args, the `CONCAT(start, CONCAT(joined, end))` wrap of ValueCollections.java:407-410.
   - Use a separate entry, not the typed flag inside `NamedRules.lower`: a makeString over rowCells in a position that bypasses `scalar` arrives with generically lowered args and must get form (b), as it does today.
   - The Lowerer names no NamedRule constant, so ArchitectureTest's NAMED_RULES_ARE_THE_TABLES (T/ArchitectureTest.java:3622-3657) holds.
4. The text `TDSNull`: no new string literal in lowering.
   - Scalars' register row is frozen at 2 (T/CompatSpellingGuardTest.java:433; T/TerminalRowsFrozenTest.java:106, 178-182).
   - Derive it in Scalars from `PlatformTypes.TDS_NULL_FQN` (PlatformTypes.java:46, its simple name). Scalars already has the borrowed-constant row (T/CompatConstantPoolTest.java:74), so no entry is needed.
   - In NamedRules it would need a new POOL_ONLY row (CompatConstantPoolTest.java:68-75); a literal there would need a new register row.
   - `edge.Presenter.TDS_NULL` cannot be used: lowering may not import the edge.

**Deleted**
- M/edge/PurePrintForms.java, whole file.
- The 12 `spellingPasses()` overrides in PureText.java (447-452, 465-470, 491-496, 503-508, 514-519, 525-530, 544-549, 555-560, 566-571, 578-583, 589-594, 604-609).
- Optional: the hook itself (AnsiSqlRenderer.java:260-263, 286-301). That file is not forbidden but not on your allowed list.
- Comments only: SqlFn.java:226-227, CellTexts.java:16-17, Coercions.java:126, Scalars.java:899-900 and 929-930, Lowerer.java:4524-4525, TdsDesugars.java:150-153, Pure.java:487-488.

**Guard and pin entries (dated)**
- T/ArchitectureTest.java:1419-1455: add `"lowering.NamedRule#<NEW>", "com.legend.edge.UpstreamEmptyCells"`.
- T/JavaEvalLedgerTest.java:1954-1957: remove `"PurePrintForms.java"`; add the new edge file (precedent 1968-1974). No new entry if the rows go into UpstreamTies.java instead, but its name and doc would then be wrong.
- T/SqlFnSortedTest.java:234-236: CELL_TEXT's `BOTH_WRITERS` entry goes, since the edge no longer reads it (precedent 302-304).
  - COALESCE (243-255), CONCAT (256-266) and LIST_TRANSFORM (373-381) stay: the edge still writes them (grep: PureListTexts, EngineResultEnvelopes), and Scalars and ValueCollections are already listed. Add NamedRules only if a helper writes there.
- T/integration/EdgeSpellingHookTest.java:160-212: `spellingPasses` leaves every override set. `D1_2` and `D1_2_MONGO` become empty, DuckDb `{scanOrderPasses, raw}`, Postgres `{raw}`, SQLite `ROW_22`.
- T/HookRulingTest.java:577, 717: unchanged if the hook stays (raiseMessage is listed with no overrider); both lose `spellingPasses` if it is deleted (dateTimeFormat precedent, 578-580).
- T/integration/JoinedTextTest.java:19-23: javadoc only; its answers (46-50) are the proof at legend-pure's door.
- No entry needed: WriterCarriageGuardTest (grep: only `result/CellText.java`), exec/InterimEdgeSitesTest (grep: no reference), TdsNullTypingPinTest, InternalNativeCallTest, NativeCatalogGovernanceTest (no typer change).
- Suggested new pins, on the pattern of T/edge/UpstreamTiesTest and EngineFloatTextRowTest: a row test over all four styles, stamped and unstamped; and "no CELL_TEXT in a tree lowered under an edge style".

## 4. Risks

1. **Scope:** `Lowerer.scalarValueTailArms` (4527-4547) and the one-argument change at Scalars.java:2418 are outside your stated allowed list, though neither is forbidden. Without the Lowerer edit, (c) cannot be a row; a small pass would have to stay for it.
2. **Type facts move earlier for (a) and the inside of (b).** Today enclosing rules see `CELL_TEXT(x)`: VARCHAR, nullable as `x` is (M/sql/SqlTyping.java:372, 407). After, they see `COALESCE(x, lit)`: never null (366), kind `uniform(args)` (518).
   - An enclosing rule that reads nullability or kind may write different SQL (a dropped guard), and a plan column's nullable fact may flip (M/executionplan/MirJson.java:160).
   - The answer cannot move by that: the fact is true of the SQL sent today. But "its SQL stays" is not guaranteed for enclosing nodes; measure by a SQL diff of the corpus.
   - (c)'s node, and (b)'s outer node, are VARCHAR and never null before and after.
   - No COALESCE-shape reader in a forbidden file matches the new node: Equalities.java:149-153 needs a `BoolLit`; MixedEncoding.java:1081-1088 needs `TO_VARIANT`.
3. **Trees lowered under `Styled.PRODUCT` but rendered by an edge dialect** get no row, and CellTexts would print the product's empty text silently. Three such sites exist: PlanMaker.java:955 (the count statement, no cell text), Compiler.java:968 (lineage, never rendered), SeedableLets.java:43 (a discarded probe). None carries a cell text today; the suggested guard keeps it so.
4. **A row replaces the call's whole own rule** (Scalars.java:2415-2419 returns before `HeldKinds` and `FeatureRules`). Harmless here: no flagged rule exists for these calls (grep of FeatureRules), and `HeldKinds` concerns comparisons.
5. **No forbidden file needs an edit.** CarrierStrategies and H2 see the same trees as today, because the spelling passes already ran first.

### Critical Files for Implementation
- /home/user/s8-edit/core/src/main/java/com/legend/lowering/Lowerer.java (4527-4547)
- /home/user/s8-edit/core/src/main/java/com/legend/lowering/Scalars.java (901-946, 2403-2419, 3896-3990)
- /home/user/s8-edit/core/src/main/java/com/legend/lowering/NamedRules.java (with NamedRule.java and ValueCollections.java:398-412)
- /home/user/s8-edit/core/src/main/java/com/legend/edge/EngineTable.java (242-262), with PureTable.java, UpstreamTies.java and the new UpstreamEmptyCells.java
- /home/user/s8-edit/core/src/main/java/com/legend/edge/PureText.java (447-609), with PurePrintForms.java deleted
