# D, Phase 3: the Java presenter at the edge (design note, 2026-11-16)

The edge program (`docs/records/total-wall-edge-program-2026-11-16.md`), Phase 3, table rows 1, 6 and 10. **Not built:
sent to the orchestrator to check against the other phases' files.**

## What changes
1. **One presenter family, `edge.Presenter`** (it absorbs `EngineCells`' value rules). It is a typed value plus its
   Pure type, giving legend-pure's or legend-engine's text, in Java:
   - a Float: legend-pure's print, the existing W8 Java print, moved here;
   - a DateTime or StrictDate: the written precision, with `+0000` from a minute on (`EngineCells.LEGEND_PURE`'s
     rule);
   - Decimal, Boolean and String;
   - an empty TDS cell as `TDSNull`;
   - lists and instances, and pure/v1's JSON tokens.
   One entry point per form: `legendPure(value, type)`, `engineJson(value, type)`, `engineCell(value, type)`.
2. **Every edge writer calls it:** `PureV1Writer`, `TdsObjectsWriter`, the Arrow route's `DateText`, `HostAnswers`,
   `ServiceTestRunner` and the PCT bridge's answer. No edge writer asks the database for a printed value.
3. **The product refuses `toRepresentation` and `format('%r')` by name** (§10 item 8, "refuse in product but not in
   compatibility"). The edge keeps legend-pure's exact text. A value's print is the presenter's. An IN-QUERY `%r` is
   Phase 5's (E-Java's), settled against upstream there.
4. **D1.3's join form:** one `CONCAT_WS` where it skips NULLs (the orchestrator's assignment, rule 8). Each database
   is measured; the gaps get their own correct forms: SQLite before 3.44, BigQuery's `ARRAY_TO_STRING`, ClickHouse and
   MongoDB. The lean delta is shown.
5. **Already done:** ClickHouse's product `format('%s', Float)`, now S311's `FLOAT_TO_TEXT` (8a718b128, with
   FLOAT_CANON's deletion).

## Files
- **New:** `core/src/main/java/com/legend/edge/Presenter.java`.
- **Edge:** `EngineCells.java`, `PureV1Writer.java`, `TdsObjectsWriter.java`, `HostAnswers.java`,
  `ServiceTestRunner.java`, `PurePrintForms.java` (the value-print half only).
- **Product:**
  - `lowering/Repr.java` and `lowering/Scalars.java` (the `%r`/`toRepresentation` refusals);
  - `sql/dialect/CellTexts.java`, plus the `CONCAT_WS` arm in each dialect that has it (AnsiSqlRenderer, SQLite,
    BigQuery, ClickHouse, MongoDB).
- **PCT:** the bridge's value text, `pct/.../PctCaseRecorder.java` (E-SQL's file: only if the net needs it, and E-SQL
  makes it).
- **Tests:** `PresenterTest` (new), `PureDatesArrowTest`, `OutputMatrixTest`, the probe.

## Contracts
- **With Phase 2 (mine):** a FIXED Float or date operand under an edge template is printed at planning by
  `Presenter.legendPure`. There is no formatter in exec.
- **With E-Java's Phase 4b:** a failed verdict's message (`%r`, `TDS_NULL_CELL`, `SideShapes`) calls
  `Presenter.legendPure` at the edge, the same entry point.
- **With E-Java's Phase 5:** the presenter is for VALUES only. An in-query `toString`/`%r` is legend-engine's SQL,
  Phase 5's. The two never share a file except `PurePrintForms` (Phase 3 takes its value-print half; Phase 5 its
  in-query half: one owner per method).
- **With E-SQL's Phase 8:** the presenter reads typed values the carriers deliver exactly. It never parses a printed
  text. Phase 8's exact carriers are its input contract.
- **SQL `FLOAT_PRINT` and `DATETIME_PRINT` arms:** they stay until Phase 4b and 5 have no user. Then whoever empties
  them deletes them (a one-line handoff, as with FLOAT_CANON).

## Risks
- **legend-pure's Float print in Java against the SQL print:** the golden net is the proof (36/36 empty). Any byte that
  moves stops the line.
- **`CONCAT_WS` gaps per database:** measured on the lanes. Where the native form is absent, the current `CASE` form
  stays for that database.
- **The `%r` refusal moves PCT** where the product answered it: the moves are listed and sent before landing (§5.3).

## Size
About 8 hours. In order:
1. the presenter and the writers;
2. the refusals;
3. `CONCAT_WS`.
Net-empty after each step.
