# Building the timeseries language: `#TS{…}#`, v0 and v1 (2026-10-30)

The build plan for docs/TIMESERIES_LANGUAGE_2026_10_29.md (the language, version 3.1) and the quants' guide
(docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md). Stacked on PR #81 (`claude/timeseries-platform`), on the branch
`claude/timeseries-viewer-datacube-tsm0nb`. Each phase is one or more commits held to the gates below; a later session
starts at §5's status.

## 1. The user's rulings (2026-10-30)

1. **The island is `#TS{…}#`** (the spec's open decision 1; `#Quant{…}#` was its placeholder). A lite extension, as
   `#IQ{…}#` and `#Cypher{…}#` are: the LEGEND_ENGINE surface refuses it as an unknown island type.
2. **A query becomes Pure directly**, not through Postgres SQL text. The spec's §1 said a query's meaning is its
   expansion into plain SQL; that is replaced:
   - every figure is a call of the timeseries library (§2), never its maths re-spelled as SQL windows, so the tile's
     numbers and the language's are the same calls;
   - every per-point expression (`ln(x)`, `x / y`, `CASE`, casts, `coalesce`, a `FILTER`'s or a source's condition) and
     every plain-SQL subquery source means what Postgres means, through the `#SQL{…}#` translator (`SqlToPure`), which
     already holds Postgres's rules (an untyped string read as the column's type, integer division, nulls, ~100
     functions);
   - the language's own clauses (`ON`, `BY`, `SAMPLE BY`, `FILL`, `FILTER`, subscripts, windows, spans) are the
     builder's, written as the Pure composition the library's functions take.
3. **Island languages through one registry, not branches** (P0): the parser's island names, the wire reader and
   writer, the island seam's dispatch. Postgres and IQ keep sharing `SqlToPure` through `InputDialect` (no dialect
   branch is in the translator today: its four mentions of a dialect are comments).
4. **The library is not DataCube's**: `datacube::ts` becomes the project `projects/timeseries`, package `timeseries::`
   (projects/CONTRACT.md's naming: the package root is the project's name). **`#TS` imports it**: a function a query
   names (`volatility`) is found in `timeseries::` first.
5. **v0, then v1, and v1 draws every chart of the gallery** (`datacube/src/examples/timeseries-gallery.ts`: 69 tiles,
   173 series, about 80 of the tile's functions): each series written in `#TS`, planned and run, the same answer as
   the tile's TypeScript writer gives today (P5).

**Open, to the user before it lands:** whether a model that names `#TS` must depend on the `timeseries` project (as a
DataCube page sends the library with its model today), or the platform loads it for every model. The world-map tenet
(docs/WORLD_MAP.md, TENET_CHARTER clause 6) has the platform ship no Pure bodies, so loading it everywhere is a tenet
change. **Default until ruled: a dependency**; `#TS` refuses by name a model without it ("#TS uses the timeseries
library: add the timeseries project to the model's dependencies").

## 2. The pieces

| Piece | Where | What |
|---|---|---|
| Island registry | `protocol/spec`, `parser`, `protocol` (reader, emitter), `compiler/spec` | one `IslandLanguage` per language: its opener (and profile rule), wire key, the engine surface's refusal, how it becomes Pure |
| The library | `projects/timeseries/model.pure`, package `timeseries::` | today's `datacube/src/pure/datacube_ts.pure`, renamed; DataCube's generated text and enumeration table read it |
| Expression seam | `protocol/sql/SqlToPure` | one public entry: a Postgres expression over named, typed columns as a Pure lambda body; a subquery as a relation |
| `#TS` builder | `protocol/ts` (parse) and `compiler/spec` (the island's seam) | the query's clauses parsed (the SQL parser for its SQL-shaped parts), then the Pure: the source, each item's library calls and extends, the filter, the projection |

## 3. Phases

**P0. The island registry.** No behaviour change: every island test, the IQ syntax cases, the Cypher lanes, the wire
round-trips and parser equivalence unmoved.

**P1. The library renamed.** `datacube::ts` → `timeseries::`, the file to `projects/timeseries/model.pure` with its
MANIFEST.md; the generators (`datacube/tools/library-text.mts`, `library-enums.mts`), the tile's writer, the gallery,
the Python API, `//core:timeseries_library_test` and the docs follow. Every number unchanged
(`timeseries_queries_test`, the library test, `timeseries_sql_measure`'s twelve queries byte for byte but the name).

**P2. `#TS` v0.**
- `SELECT item, … FROM <table | (table WHERE …) | (subquery)> [alias] ON <date> [BY <instrument>] [FILTER …]`.
- Along time, a window of N observations or `all`: `avg`/`mean`, `sum`, `min`, `max`, `count`, `std`/`stddev`,
  `var`/`variance`; `lag`, `diff`, `returns` (`obs = 1`). Each the library's call.
- R4 (a value once its window is full), R7 (names: `AS`, a later item reads an earlier one, an item not named like a
  source column, the result's date and instrument), R8 (`FILTER` after everything; `WHERE` only inside its source).
- Refusals that say the fix (spec §9) for what v0 parses; whatever v1 builds refused by name ("… is v1's").
- Tests: each query's answer equal to the same library calls written in Pure, on DuckDB, H2 and SQLite where the
  library answers there.

**P3. v1's clauses.** Subscripts and lines meeting on dates (R2, R6), several sources (R1), options (`type => 'log'`),
spans and date windows (R4, §5), `SAMPLE BY` and `FILL` (R5), kinds and units (R11, R12) as the functions need them,
the warm-up report (R4).

**P4. v1's functions.** §6's catalog, as far as the gallery uses it and the rest of each family with it.

**P5. The gallery in `#TS`.** A writer of a tile's `#TS` query from its spec (the thin writer step D of
docs/TIMESERIES_ON_THE_PLATFORM_2026_10_28.md needs), and a test that plans and runs every gallery series both ways on
DuckDB-WASM (as `//datacube:timeseries_sql_measure` runs its twelve), the same answers required. Every series that
cannot be written is listed with why.

**After v1 (not in this plan):** the tile and Python send `#TS` (step D), and `timeseries-query.ts`'s writer is deleted.

## 4. Gates for each phase

`//core:core_tests` (the integration lanes on the platform, H2 and SQLite), `//core:timeseries_library_test`,
`//parser-equivalence:parser_parity` and `update_ratchets_test`, `//gates:checks`, and from P1 the DataCube lanes
(`//datacube:...` tests, the typecheck, `update_generated`), `//pct:pct_duckdb_*` where the compiler changes. A guard
that moves moves with its reason in the commit (AGENTS.md).

## 5. Status

- 2026-10-30: the plan; P0 next.
