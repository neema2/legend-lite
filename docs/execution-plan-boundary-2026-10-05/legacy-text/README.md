# legend-engine's own SQL text, for the legacy printer (E-4b, 2026-10-09)

The legacy engine-text printer (`EngineStyleH2`: what `toSQLString(..., DatabaseType.H2, ...)` returns, and the plan
text) is lite's backwards-compatibility mode: it must write exactly what legend-engine writes. These files are
legend-engine's answer for 14 query shapes that exercise windows and ordered aggregates, the shapes the printer used to
reach by editing its own text.

- `model.pure`: the store (`test::DB.T`) and an H2 runtime.
- `l1.pure` … `l14.pure`: the queries (relation API, `->from(test::RT)`).
- `engine-l1.sql` … `engine-l14.sql`: the SQL text legend-engine 4.145.0 (server commit 230c159, the
  `legend-engine-4.145.0` tag) answers, from `POST /api/pure/v1/execution/generatePlan` (the `sql` node's `sqlQuery`).
  The engine's API does not expose `toSQLString` itself; `generatePlan` reaches the same printer
  (`sqlQueryToString`, through its SQL dialect translation on H2).
- `plan.py`: how they were captured: `python3 -I plan.py <model as PureModelContextData JSON> lN.pure req.json`.

What they show, against the source (`h2SqlDialect.pure`, `sqlDialect.pure`, `sqlDialectDefaults.pure`):
keywords lowercase (`over`, `partition by`, `order by`, `asc`/`desc`, `rows between ... and ...`, `distinct`);
function names lowercase; the string aggregate is `listagg`, an ordered one `listagg(x, sep) within group (order by k)`
— over a window the engine writes the `within group` AFTER the `over (...)` (l6, l8); an aggregate's own ordering
`sum(x order by k desc)`; `nulls first`/`nulls last` only where the query says `emptyFirst()`/`emptyLast()` (l2, l4,
l8, l13).

`LegacyTextTest` (core) holds lite's printer to these spellings. The shapes lite cannot yet lower (`reduce`, a
`joinStrings` over a window's partition: l6, l8, l10, l11, l13) and the explicit null placement (PARK-18,
`docs/PARKED_WORK_LEDGER.md`) are recorded where they are owned. The statement STRUCTURE of a relation-API query differs
too (the engine's dialect translation names its alias `t_0` and lists every column; lite's printer follows the
TDS goldens' `root` and `root.*`): that is the compatibility mode's (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §8,
phase 4), not a spelling.
