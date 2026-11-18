# V-EDGE verification

Scope: wrong answers that a legend-engine or legend-pure client, or a PCT or corpus verdict, can get from the edge. Head is
`68f20eb4d`, main is `4dfbb5b7`, and the lanes' base is `815de5810`. Paths are at the head unless marked `main:`. Upstream
was read from the auditor's saved sources (`verify/upstream/`). DuckDB values were measured with DuckDB 2.0.0 in
`verify/pylib`; lite pins 1.5.6.0, and its own javadoc and round 67 of the plan record the same exponent form there. H2's
text is Java's `Double.toString`; lite's own `FloatTextTest:66-69` pins `1.3421E8` and `1.3421E-8` for H2's cast. Scripts
are in `audit/vedge/`.

| Finding | Claimed | Verdict | New in PR? | Ruled? | Covered by a test? | Fix size |
|---|---|---|---|---|---|---|
| EDGE-1: Float cell in a relation's `toString`/`toCSV` text | WRONG-ANSWER, FIX-AFTER ("arguably a blocker"), regression from main | **BLOCKER** (confirmed; small fold-in fix) | **Yes.** `237bd8e5f` (Phase 5 row 8). Main printed legend-pure's form. The lanes' base `815de5810` already has it, and nothing on the path changes between the base and the head. | **No.** §3 gives the cast to "toString inside an expression" only. It also says "legend-pure's print of a value is Java at the edge" (row 1: legend-pure's print). No register row covers it; S306 is the test-data generator's. | **No.** No golden holds a relation text. Round 67's PCT cast measurement moved only a literal's toString. S132's `cellEquals` hides H2's forms. `FloatTextTest` cuts such values out (`subList(0, 3)`). | About 8 lines in `RelationTextLowering`, 2 javadoc lines in `SqlFn`, and 1 test. A dated PCT ratchet raise may be needed. |
| EDGE-2: toCSV DateTime is ISO `T…+0000`, upstream `yyyy-MM-dd HH:mm:ss` | WRONG-ANSWER, FIX-AFTER, pre-PR | **FIX-AFTER** (confirmed) | **No.** `main:lowering/Render.java:778-782` is identical. | **No.** No row exists. The source is a design doc (OUTPUT_LAYER_DESIGN, 2026-10-10, made when toCSV was "lite's own output"). S306's ruling ("in backwards compat we can chop the timestamp") points the other way. | **No.** `RenderCsvIntegrationTest` has no DateTime column. | About 4 lines, plus a test. Corpus rosters may move. |
| EDGE-3: `format('%r', s)` leaves a newline raw | WRONG-ANSWER, FIX-AFTER, pre-PR | **FIX-AFTER** (confirmed) | **No.** `main:lowering/Repr.of(TypedSpec, SqlExpr)` (`Repr.java:73-95`) is identical. | **No.** S314 puts `%r` at the edge, which must be upstream's. | **No.** `testFormatRepr` passes with no newline. `PureOnlyTextTest` pins only `'a'`. | 1 line replacing 5, plus a test. The golden net's `%r` SQL may need re-recording. |
| EDGE-8: SQL tdsBuilder envelope, unordered rows and database JSON spacing | WRONG-ANSWER, NOTE, suspected | **NOTE** (order real on DuckDB for large inputs; spacing unreachable) | **No.** `main:lowering/JsonEmission.java:178-187` is identical. | No | No | (the portability program; or carry the sort keys, about 10 lines) |
| L14 (a): legacy `sort('a', DESC)->toString()` null row moves last to first | suspected edge output change | **REFUTED as a wrong answer; NOTE.** The move is real, and it goes *toward* upstream and the statement's own order. | Yes, in the final train (`9cccabbc8`; the base still has `pureNullOrder`) | Upstream evidence: GATES.md batch 8 (legend-engine 4.145.0 `processSortItem`: DESC NULLS FIRST). No ruling needed. | **No.** `AggOrderNullPlacementTest` covers only the colspec sort. | A test of about 10 lines |
| executeValues (b): non-array root returns unaggregated rows | suspected | **REFUTED** (unreachable) | Yes, in the final train (`7591f37b7`), but it writes SQL identical to before for every reachable root | n/a | n/a: no input reaches the case | Optional 3-line assertion |

Confirmed blocker: **EDGE-1**.

## Details

### EDGE-1: a Float cell in a relation's text is the database's cast (confirmed, BLOCKER)

**The walk at the head.** Input, under the edge on DuckDB (PCT channel B, the corpus, or pure/v1 through
`EdgeServer.runner`, `EdgeServer.java:37-41` → `PureText.PURE_V1`, whose facets are UPSTREAM's, `PureText.java:259-262`):
`|#TDS\nx:Float\n0.00001\n#->toString()`.

1. `Lowerer.java:4317` meets a relation's `toString`. It asks the style's `RelationTexts` facet, which is
   `PureText.java:88-89`, `new RelationTextLowering()`.
2. `RelationTextLowering.java:46-49` → `lowerToString` (`:313`) → `tdsCell(styled, Column.of(rowAlias, col), Float,
   DOUBLE)` at `:419-421`.
3. The Float arm at `:545-567`: `RelationTexts.pureToString(styled, FLOAT, Cast(c AS DOUBLE, PRINT))`. That goes
   `RelationTexts.java:50-52` → `Scalars.pureToString:2836`, then the FLOAT arm → `floatRepr:3400` →
   `LiteralSpelling.floatPrint:485-497`. `heldAsInteger` is false because the operand is a Cast, so the result is
   `Call(FLOAT_TO_TEXT, CAST(c AS DOUBLE))`.
4. Render. `PureText.DuckDb.spellingPasses` (`PureText.java:372-376`) gives `PurePrintForms.passes()` (`:30-38`).
   `PurePrintForms.expr:175-176` rewrites FLOAT_TO_TEXT to `floatOf(x, PRINT_BARE)` (`:120-123`), which is
   `Cast(x AS VARCHAR, USER)`. The literal processor does not apply, because the operand is a Cast, not a `FloatLit`.
5. DuckDB evaluates `CAST(CAST(c AS DOUBLE) AS VARCHAR)` to `1e-05`.

- **Output:** `#TDS\n   x\n   1e-05\n#`.
- **Upstream:** `#TDS\n   x\n   0.00001\n#`. `relation/functions/toString.pure` calls `s($c->eval($x)...)`, and `s.pure` has
  `o : Any[1] | $o->toString()`. legend-pure prints a double with `DecimalFormat("0.0")` and 340 fraction digits
  (`CompiledSupport.java:132-137, 1271-1273`).

**toCSV** follows the same path. `RelationTextLowering.java:694` writes `escapeCsv(pureToString(styled, FLOAT, c))`, which
is `FLOAT_TO_TEXT(c)` and then the cast. Upstream is `helperFunctions.pure:234`, `$a->toString()->escapeCSVString()`.

Measured values (`vedge/mainprint.py`, `vedge/j/P.java`):

| Value | Head on DuckDB | Head on H2 (`Double.toString`) | Main on DuckDB | Upstream |
|---|---|---|---|---|
| 0.00001 | `1e-05` | `1.0E-5` | `0.00001` | `0.00001` |
| 0.0005 | `0.0005` | `5.0E-4` | `0.0005` | `0.0005` |
| 25000000.0 | `25000000.0` | `2.5E7` | `25000000.0` | `25000000.0` |
| 1e16 | `1e+16` | `1.0E16` | `10000000000000000.0` | `10000000000000000.0` |
| 1e20 | `1e+20` | `1.0E20` | `100000000000000000000.0` | `100000000000000000000.0` |

- DuckDB writes an exponent below 1e-4 and from 1e16.
- H2 writes one below 1e-3 and from 1e7. Those are ordinary magnitudes for amounts and rates, not only extremes.
- Postgres writes one from 1e15 and below 1e-4. Its edge cast only appends `.0` to integer text (`Postgres.java` variantAwareCast).

**Main** wrote upstream's form:
- `main:lowering/Render.java:642-643` → `Scalars.pureToString(FLOAT, Cast)` → `main:LiteralSpelling.floatPrint:485-492` →
  `FLOAT_PRINT`;
- `main:DuckDb.java:581` `floatPrintByText` → `main:sql/dialect/FloatPrintText.of` turns an exponent text into plain digits
  (via DECIMAL(38,18) or HUGEINT, then `.0`);
- the CSV cell is the same: `main:Render.java:784`.

**New or old.** The change was introduced by `237bd8e5f`, Phase 5 row 8, "WIP (unmeasured, not for hand-off)". It changed
`PurePrintForms.floatOf` from `FLOAT_PRINT` to the cast, and its one pass rewrites every `FLOAT_TO_TEXT`, relation-text
cells included. Before it (D1.2, 2026-11-15), the edge wrote FLOAT_PRINT for that node.

`237bd8e5f` is an ancestor of `815de5810`, and
`git diff --stat 815de5810 68f20eb4d -- edge/PurePrintForms.java edge/RelationTextLowering.java lowering/LiteralSpelling.java lowering/Scalars.java`
is empty. So the final train's base-against-head lane comparison cannot see this; only main against head can.

**Ruled?** No.
- The plan of record's §3: "for toString inside an expression use engine cast( as varchar)". It was "Settled the same day":
  "An edge expression's Float `toString` is legend-engine's cast. legend-pure's print of a value is Java at the edge."
- §6 row 1: "a Float value's text … legend-pure's print". Row 8 is "a Float's `toString` inside an expression".
- A relation-text cell is printed by legend-pure after the run, so it is row 1's meaning, not row 8's.
- No `SEMANTICS_REGISTER.md` row covers it. S306 rules the *test-data generator's* float as a known divergence, a different
  path.
- The PR's own docs claim the edge writes legend-pure's print for this node: `sql/SqlFn.java:148-152`, and
  `EdgeSpellingHookTest.java:169-170` ("`FLOAT_SHORTEST_TEXT`, edge.PurePrintForms").

**Covered?** No.
- *Edge golden net* (`pct/src/test/resources/edge/*_answers.tsv`): it holds only RELATION, VALUE or ERROR answers from
  channel A, where legend-pure itself prints. No `#TDS` text is recorded (grep finds only test names), and no
  exponent-form token appears.
- *PCT:* round 67 applied the cast to every FLOAT_TO_TEXT and moved one test, `testFloatToStringWithNegativeExponent`, a
  literal's toString. The narrow form landed with "0 moves". So no PCT relation text holds a Float in the exponent range on
  any lane. On H2, `UpstreamRenderedTexts.cellEquals` (`:156-173`) would hide `1.0E-5` against `0.00001` anyway, because both
  contain a `.` and parse to the same value.
- *Corpus:* the cases cannot be read here. H2's forms are hidden by the same leniency, and DuckDB's thresholds are extreme.
- *core tests:* `FloatTextTest.aFloatsTextIsSixDecimalsTrimmed` cuts the edge column to `subList(0, 3)`, which drops
  `1234567.89` and `1e20`. `RenderCsvIntegrationTest` has no Float column.

So nothing that would turn red reaches the regression. A legend-engine client gets the wrong bytes today through pure/v1
`toString`/`toCSV` on H2 at magnitudes of 1e7 and above or below 1e-3. A DuckDB verdict at an extreme magnitude would fail
where upstream passes.

**Why BLOCKER.** The rubric's first BLOCKER item is a wrong answer, and this one is introduced by the PR: main was right, so
merging makes main worse. It contradicts the user's settled ruling (§3) and the PR's own javadoc. It is hidden from every
lane, by the base already carrying it and by S132. The fix is small and commits to nothing.

**Fix** (`core/src/main/java/com/legend/edge/RelationTextLowering.java`, about 8 lines). Print a relation-text Float cell
with `SqlFn.FLOAT_SHORTEST_TEXT`. That is main's `FLOAT_PRINT` renamed: diffing `main:FloatPrintText` against the head's
`FloatShortestText` shows only `Reason` tags and comments. `PureMessages` and `EdgePureOnlyTexts.representationOf` already
use it as "legend-pure's print".

```java
/** legend-pure's print of a Float CELL, written after the run (s.pure's $o->toString(), helperFunctions.pure:234:
 *  DecimalFormat "0.0", never an exponent): FLOAT_SHORTEST_TEXT. Never FLOAT_TO_TEXT, which PurePrintForms writes as
 *  legend-engine's IN-QUERY cast (row 8). */
private static SqlExpr printedCell(SqlExpr text) {
    return text instanceof SqlExpr.Call c && c.fn() == SqlFn.FLOAT_TO_TEXT
            ? SqlExpr.Call.of(SqlFn.FLOAT_SHORTEST_TEXT, c.args().get(0)) : text;
}
```

- Wrap the three cell prints: `:566-567`, `:569` and `:694`. Mixed-slot CSV cells recurse through `perSlot` → `cell` →
  `:694`.
- Correct `SqlFn.java:148-152` to say the edge writes legend-engine's cast for an in-query toString and
  FLOAT_SHORTEST_TEXT for a relation-text cell.
- Expect the PCT channel-B judge and plan character ratchets to grow back part of `3cafa495a`'s shrink. That needs a dated
  raise in `pct/.../channelb/ratchets.tsv`.
- Run `//pct:edge_contract_test`; I expect no golden moves, since the goldens hold no relation text.
- Longer term (EDGE-6): give the relation cell its own typed fact, so the row-8 pass cannot reach it.

**The test that proves it** (add to `core/src/test/java/com/legend/integration/FloatTextTest.java`; it runs on each lane's
DuckDB, H2 and SQLite):

```java
@Test
void anEdgeRelationsTextPrintsAFloatCellAsLegendPure() throws Exception {
    TestBackend backend = LaneFloor.backend();
    String model = backend.withStorelessRuntime("");
    String rows = "#TDS\nx:Float\n0.00001\n0.0005\n25000000.0\n100000000000000000000.0\n#";
    assertEquals("#TDS\n   x\n   0.00001\n   0.0005\n   25000000.0\n   100000000000000000000.0\n#",
            Lean.runInStyle(model, "|" + rows + "->sort(~x->ascending())->toString()", StorelessRuntime.RUNTIME,
                    backend, com.legend.edge.PureText.UPSTREAM).rows().get(0).get(0).toString(), backend.name());
}
```

At the head this fails: DuckDB writes `1e-05` and `1e+20`, and H2 writes `1.0E-5`, `5.0E-4`, `2.5E7` and `1.0E20`. It should
pass with the fix on DuckDB and H2; SQLite should be measured. A toCSV twin belongs in `RenderCsvIntegrationTest`, with a
DOUBLE column holding 0.00001.

### EDGE-2: toCSV writes a DateTime as ISO text (confirmed, FIX-AFTER, old)

**The walk.** `|test::Event.all()->project([e|$e.id, e|$e.at], ['id','at'])->meta::relational::tests::csv::toCSV()`, with
`at` a TIMESTAMP holding `2024-01-02 10:30:00.123`:

1. `Lowerer.java:4303` → `RelationTextLowering.lowerToCsv` (`:80-107`). It admits only the default formats, and it knows
   the format is `%t{yyyy-MM-dd HH:mm:ss}` (`:95-99`, `:144-157`).
2. `cell` `:688-692`: `DATE_TIME` → `dateTimeText(c)` (`:452-457`) → `EdgeDateTimeTexts.dateTimeText` → `DATETIME_TO_TEXT` →
   `PurePrintForms:178-180` → `PureDateTimePrint.of`, which writes `T`, the fraction and `+0000`.

- **Output:** `2024-01-02T10:30:00.123+0000`.
- **Upstream:** `2024-01-02 10:30:00`. `helperFunctions.pure:203-205, 238-243` gives
  `format(SimpleDateTimeFormat(), $d)->escapeCSVString()`, and `dateExtension.pure:384-387` defines that format as
  `'%t{yyyy-MM-dd HH:mm:ss}'`.
- The abstract `Date` arm (`:683-687`) does the same for a timed value.
- The edge already has upstream's print, `EnginePlans.testDataTimestampText` (`EnginePlans.java:222-233`), so two edge
  implementations of one upstream function disagree.

**Old:** `main:lowering/Render.java:778-782` is identical.

**Ruled?** No row. The comment at `:690-691` cites `docs/OUTPUT_LAYER_DESIGN_2026_10_10.md` §3 gap 4 and §4, a design
written while toCSV was "lite's own output". toCSV is now legend-pure's text at the edge. The upstream-fidelity tenet says
a divergence that is not a row is a defect. S306 (the user, 2026-11-12, "in backwards compat we can chop the timestamp") is
the nearest ruling, and it points to upstream's cut.

**Covered?** No: `RenderCsvIntegrationTest`'s only date is a `DATE`.

**Fix:** at `:688-692` and inside `:686`, write `escapeCsv(EnginePlans.testDataTimestampText(c))`. That is about 4 lines;
the text-carried date arm `:669-672` needs its own look.

**Probe:** a test in `RenderCsvIntegrationTest` with its own `T_EVENT(ID INTEGER, AT TIMESTAMP)` table and the query above,
asserting `"id,at\n1,2024-01-02 10:30:00\n"`.

**Impact:** corpus rosters may move, so re-measure them. Grade FIX-AFTER: it is real, but main has it too.

### EDGE-3: `format('%r', s)` does not escape a newline (confirmed, FIX-AFTER, old)

**The walk.** `|format('%r', ['a\nb'])` under UPSTREAM:

1. `Scalars.java:3446-3450` → `EdgePureOnlyTexts.formatR` (`:106-124`).
2. `typed` is a String, so the fall-through escapes only `\` and `'` (`:119-123`).

- **Output:** `'a<LF>b'`.
- **Upstream:** `'a\nb'`, with a backslash and an n. `%r` is `toRepresentation` (`Format.java:95-101`, interpreted;
  `PureStringFormat.java:59-62`, compiled), and `toRepresentation.pure:43` adds `->replace('\n', '\\n')`. Upstream's own
  test is `testStringToRepresentation:104`.
- The same class's `representationOf` (`:63-71`) does escape the newline.

**Old:** `main:lowering/Repr.java:73-95` is identical.

**Covered?** No. `PureOnlyTextTest.java:39` pins `format('%r', ['a'])` only. The PCT's `testFormatRepr` passes.
`UpstreamVerdictWords.reprNewlines` (`:193-197`) patches assert messages alone.

**Fix:** replace `:118-123` with `return representationOf(styled, STRING, e);`. That is 1 line replacing 5, and leaves one
owner.

**Probe:** in `PureOnlyTextTest`, assert that
`scalar(Lean.runInStyle(model, "|format('%r', ['a\\nb'])", StorelessRuntime.RUNTIME, backend, PureText.UPSTREAM))` equals
`"'a\\nb'"`. The edge golden net's SQL for `format('%r')` gains one REPLACE, so re-record it if needed.

### EDGE-8: the SQL tdsBuilder envelope (NOTE, old)

**The path.** Only a test body's `meta::legend::executeLegendQuery` string entry reaches it:
- `planner/FramePlanner.java:126-141` → `EngineExecutedValues.relation` (`:25-28`, kind STYLED);
- → `JsonEmission.result:91-114` → `EngineResultEnvelopes.tds:55-83`.

The rows are `STRING_AGG(rowText, ',')` with no sort keys (`:72-75`), over a subselect that keeps the inner ORDER BY
(`JsonEmission.java:139-141`).

**Order.** Measured with `vedge/duckorder.py` (DuckDB 2.0.0, 8 threads): `string_agg` over an ordered subquery keeps the
order at 10 and 1,000 rows, and loses it at 200,000 and 2,000,000 rows. So a sorted chain's envelope can come back
reordered on DuckDB for a large result. Corpus data is small, which keeps it latent. H2 runs a query single-threaded.

**Spacing.** Postgres's `json_build_object` writes `{"a" : 1}`, while DuckDB writes `{"a":1}` (measured). But no corpus lane
runs Postgres, PCT never calls executeLegendQuery, and pure/v1 answers are written in Java (`PureV1Writer`). So the spacing
is unreachable today.

**Old:** `main:lowering/JsonEmission.java:178-187` is identical. No ruling, no test.

**Fix:** when the portability program writes the envelope in Java, or now by passing `hoistOrder`'s keys into the reducer
(about 10 lines).

**Probe:** a DuckDB test planning executeLegendQuery over a sorted relation of 200,000 or more rows, or a structural
assertion that the reducer carries the chain's sort keys.

### L14 (a): the legacy string-key sort's text (REFUTED as a wrong answer; NOTE)

**Facts.** L14 (`9cccabbc8`, in the final train; the base still has `TypedSort.pureNullOrder`) stamps `Fold.sortNulls` on
every relation sort key (`Sorts.java:43, 197`). `RelationTextLowering.hoistOrder` (`:618-650`) copies `k.nullOrder()` into
the text aggregate's ORDER BY, and `AnsiSqlRenderer.aggOrderNullPlacement` (`:1871-1875`) writes it.

**What moves.**
- Before L14, a legacy `sort('o', SortDirection.DESC)` key was bare inside the aggregate. That took each database's
  aggregate default. DuckDB defaults to `NULLS_LAST`: measured, `string_agg(t, ',' ORDER BY k DESC)` gives `B,A,N`. BigQuery's
  `aggregateKey` (`BigQuery.java:2751-2754`) puts nulls last. SQLite, Trino, ClickHouse and Databricks use their own defaults.
- After L14 the key is `DESC NULLS FIRST`, which gives `N,B,A`.
- H2 does not move, because edge sessions open with `DEFAULT_NULL_ORDERING=HIGH` (`EngineDatabaseSettings.java:13-21`).
  MariaDB does not move either: its aggregate key is `sortKey`.

**Direction: toward upstream.**
- The statement's own ORDER BY already rendered a bare DESC as NULLS FIRST, at main and at the head
  (`AnsiSqlRenderer.sortKey:640-663`; `main:` the same, `:597-616`).
- legend-engine 4.145.0's printer places nulls largest. `docs/GATES.md` batch 8 measured this on the bump: "Eleven
  testGroupBy.pure desc sorts moved their null group to index 0".
- So main and the base wrote a DuckDB text whose row order disagreed with their own statement and with upstream. The head
  fixes that. I could not read `dbExtension.pure` here, so the upstream side rests on that measured record.

**Covered?** No. L14 measured the edge contract and edge corpus as byte-identical, so no recorded case reaches it.
`AggOrderNullPlacementTest` pins only the colspec sort.

**Probe** (add to `core/src/test/java/com/legend/lowering/AggOrderNullPlacementTest.java`; it passes at the head and fails at
the base and at main on DuckDB):

```java
@Test
void legacyDescNullsFirstInsideAggregate() throws Exception {
    try (Connection conn = TestBackends.platform().open()) {
        Lean.Result r = Lean.runInStyle(StorelessRuntime.with("", Databases.PLATFORM),
                "{|#TDS\n  o, i\n  1, 10\n  null, 20\n  3, 30\n#->sort('o', SortDirection.DESC)->toString()}",
                StorelessRuntime.RUNTIME, conn, com.legend.edge.PureText.UPSTREAM);
        assertEquals("#TDS\n   o,i\n   null,20\n   3,30\n   1,10\n#", r.one());
    }
}
```

### executeValues (b): a root that is not array-wrapped (REFUTED, unreachable)

**Facts.** `7591f37b7` (final train) changed the edge's input at `Lowerer.java:830` from
`serializeGraph(g.asArrayWrapped(), false)` to `serializeGraph(g, false)`. `EdgeLoweringForms.oneBare` (`:79-93`) folds only
a `JsonArrayAgg` projection, and returns anything else unchanged.

**Why no input reaches the claimed case.**
- The only ROOT graph construction is `StoreResolver.java:3302`, which passes `/*arrayWrap*/ true`.
- The other constructions are children: `GraphEmission.java:1426` (to-one children `false`), `:908`, and `:2087`. Children
  are nested and never reach `Lowerer:829-830` as `g`.
- The wrappers keep the flag: `withTypeKey` (`:3177-3181`), `readThroughExecute` (`TypedSerializeGraph.java:96-102`) and
  `withChecked`.
- `asArrayWrapped()` returns `this` when `arrayWrap` is true (`TypedSerializeGraph.java:137-138`).

So `oneBare` receives the identical node as before, and the SQL is the same. That agrees with the commit's byte-identical
edge contract. The second site (`Lowerer.java:4470-4471`) still passes `asArrayWrapped()`.

**Latent only.** The form's contract comment ("the object where the graph is to-one", `LoweringForms.java:55-59`) describes
a root no construction builds. If one is ever built, the edge would return one bare object per row. A guard of about 3
lines would make the assumption explicit: at `Lowerer.java:829`, `if (!g.arrayWrap()) throw new IllegalStateException(...)`.
