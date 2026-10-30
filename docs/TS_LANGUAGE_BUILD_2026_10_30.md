# Building the timeseries language: `#TS{…}#`, v0 and v1 (2026-10-30)

The build plan for docs/TIMESERIES_LANGUAGE_2026_10_29.md (the language, version 3.1) and the quants' guide
(docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md). Stacked on PR #81 (`claude/timeseries-platform`), on the branch
`claude/timeseries-language` (the user, 2026-10-30). Each phase is one or more commits held to the gates below; a later session
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
5. **Class models as sources** (the spec's R1 and R13, version 3.2): a class read as objects with dotted paths
   (`FROM MarketQuote ON quoteTime BY instrument.ticker`); a flat subquery over it, named once by `WITH` ("I especially
   love the WITH"); a table; a relation of the enclosing Pure (`FROM $quotes`, the tile's way). Model names by their
   simple name, `::` only where a source is named; an enumeration's value written as a string.
6. **v0, then v1, and v1 draws every chart of the gallery** (`datacube/src/examples/timeseries-gallery.ts`: 69 tiles,
   173 series, about 80 of the tile's functions): each series written in `#TS`, planned and run, the same answer as
   the tile's TypeScript writer gives today (P5).
7. **The Python DSL stays; it writes `#TS`** (the user, 2026-10-30: "i would keep the nice Python DSL ... that DSL
   then maybe behind can write the #TS{} ... the pythonic way that matches gs-quant is very nice"). `Timeseries.plot`,
   `edit`, `frame` and gs-quant's vocabulary (`returns(px:close[SPX], 1, log)`, `volatility($A, 63)`) are the user's
   surface, unchanged; what changes is underneath: a series' expression becomes a `#TS{…}#` query (the thin writer of
   P5), planned by the compiler, instead of the Pure `timeseries-query.ts` writes today. The tile's expression language
   is the same surface on the screen. So "the TypeScript writer is deleted" means the Pure writer, not the vocabulary.

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
- `[WITH …] SELECT item, … FROM <class | (subquery) | table | $var> [alias] ON <date> [BY <instrument>] [FILTER …]`,
  the four source forms of R1 and the names of R13.
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

**P6. The surfaces switched** (the user, 2026-10-30: "replace the charts and TS code with our new parsing/DSL"): the
tile and the Python DSL send the `#TS` P5's writer makes from a series' expression (ruling 7: gs-quant's vocabulary and
`Timeseries.plot` stay as they are), and `timeseries-query.ts`'s Pure writer is deleted once P5's test passes.

## 4. Gates for each phase

`//core:core_tests` (the integration lanes on the platform, H2 and SQLite), `//core:timeseries_library_test`,
`//parser-equivalence:parser_parity` and `update_ratchets_test`, `//gates:checks`, and from P1 the DataCube lanes
(`//datacube:...` tests, the typecheck, `update_generated`), `//pct:pct_duckdb_*` where the compiler changes. A guard
that moves moves with its reason in the commit (AGENTS.md).

## 5. Status

- 2026-10-30: the plan; P0 next.
- 2026-10-30: P0 done (`IslandLanguage`, 35 of 35 targets). Real examples over the markets project's classes
  (`//datacube:ts_examples_measure`): five charts, their Pure and numbers; found the tile's `side="BUY"` matching no
  trade (the stored code is 'B'), which R13 settles for `#TS`. The spec at 3.2 (class sources, `WITH`, names,
  enumerations). P1 next.
- 2026-10-30: P1 done: the library is `projects/timeseries/model.pure`, package `timeseries::`; every caller renamed,
  the page's text and option table regenerated; 162 of 163 targets green (1 skipped), the projects harness compiling it
  alone and with every project, `timeseries_queries_test` and the library test with every number unchanged. P2 next.
- 2026-10-30: P2 (v0) and P3a built. `IslandLanguage.TS` (`#TS{…}#`, wire `{"ts": …}`, refused on the engine
  surface); the parser `protocol/ts/TsParser` (the clauses found among Postgres's tokens, every expression read by
  Postgres's grammar, `SqlParser.expression`, where the text has it; `$name`, `all` as a window and a subscript marked
  in place); the builder `protocol/ts/TsToPure` with its line model `TsLines`, in the protocol layer as `SqlToPure`
  and `CypherToPure` are (ArchUnit's L7 rule and the protocol-node ratchet keep Pure-building out of `compiler/spec`),
  asking the model what it needs through `TsToPure.Model`, which the compiler seam `compiler/spec/TsIslands` answers.
  - **Lines (R2, R4, R6), the tile's model** (`timeseries-query.ts` columnPart and joined): every column is a line of
    its own -- the source's rows with a date and a value, a subscript's matches (`close['SPX', venue = 'NYSE']`,
    `close[sector = 'Energy']`); a figure along time runs over its line's own rows; lines meet on the dates every one
    has (an INNER join on the period, and on the instrument where both are per instrument; a single line meets each
    instrument's, R2's broadcast); the answer is every period any item has a value in (a FULL join).
  - Several sources, each by its alias (`px.close`, or `close` where one source has it).
  - Decisions it took, each in the spec: the answer is in date order, then the instrument's; a line without SAMPLE BY
    is its column's rows (R5: several rows a date need SAMPLE BY's aggregate), so no period is grouped and no
    `maxBy` written until SAMPLE BY (H2 has no `ARG_MAX`); R13 is written on the translated condition, structurally:
    a string beside an enumeration's column in a call is that enumeration's value (`ts::Sector.TECH`), a string it has
    not refused naming its values.
  - Guards moved with their reasons: `TsParser.at` and `TsToPure.fresh` allowlisted as the other islands' cursors
    and counters are; `TsParser`'s 11 SQL-shaped literals registered (its refusals quote its own queries); the
    simple-name cut reused from `CypherIslands.Labels`; the own corpus's matched models 3011 -> 3017 (the island's
    test models joined and matched).
  Found on the way, not #TS's (a task offered): lite compares an enumeration column with a string by its stored code
  under `==` and by its value's name under `in()`.
  `//core:ts_island_test`: ten tests, every in-process database (DuckDB, H2, SQLite), each answer checked against
  plain arithmetic and v0's against the same library calls written in Pure; 26 refusals with their fixes.
  `TsIslandRoundTripTest`: text, wire, text byte for byte, and the parser's clauses. P3b (SAMPLE BY, FILL) next.
- 2026-10-30: P3b built. `SAMPLE BY 1d | 1w | 1mo | 1q | 1y` (R5): a period's aggregate (`last`, `first`, `max`,
  `min`, `sum`, `avg`) of a source's column is its line by period (the tile's grouping, periods starting Monday), a
  bare line under it refused with the fix, an aggregate with no window and no SAMPLE BY refused; a window then counts
  periods. `FILL(PREV | LINEAR | NULL | 0)` (R6) where lines meet in an expression, through the library's alignment
  functions (`lastValued`, `nextValued`, `valueAt`, `linearAt`, `fillZero`); `PREV` never takes a later value, as R6
  says (the tile's `step` back-fills a line's start: P5 counts that difference). Spans (`3mo`) are read and marked in
  place; their windows are P3c's. R6's rows applied: a row only where an item has a value.
  - **Every period any line has is a spine**: the lines' keys concatenated and grouped once (each dated at its latest
    line's date: R5's one date for every line), each line LEFT-joined onto it -- for the answer's items and for lines
    meeting under FILL. Each line is read twice, linearly; the nested FULL joins the tile writes double at each level
    on a database that writes FULL as a UNION (one in-process planner ran out of memory on them).
  - The SAMPLE BY and FILL tests run on the platform's database (DuckDB): `last`/`first` need `ARG_MAX`/`ARG_MIN`,
    and one in-process planner runs out of memory on the nesting a FILL writes.
  `//core:ts_island_test`: thirteen tests. P3c (spans and date windows, options, `latest`) next.
- 2026-10-30: P3c built. A window of a span (`avg(x, 3d)`, `std(x, 3mo)`, `count(x, 1y)`, R4: the dates after the one
  that far back, from the line's first date that span on) is the library's `overPeriod`; a lag of a span
  (`lag(x, 1y)`, `diff`, `returns(x, 1w)`, R9: as of that date) its `lagPeriod`, `diffPeriod`, `returnsPeriod` (an
  as-of join: on the databases whose dialect writes one); a window from a date (`sum(x, '2024-01-25')`, R4) the
  running figure of the values on or after it. Options by place or by name (R10: `returns(x, 1, 'log')`,
  `returns(x, type => 'log')`), a name the function has not refused naming its own. `latest` (R9) in FILTER, the
  answer's last date, and spans in a condition Postgres's intervals (`DAY > latest - 3d`). `//core:ts_island_test`:
  seventeen tests. P4 (the tile's functions) next.
- 2026-10-30: P4 built. Every function of §6's catalog the tile has (`TsFunctions`: its signatures the tile's
  `TS_SIGNATURES`, each by its language name -- `range_of`, `first_of`, `filter_values`, `union_of`, `day_of`... where
  SQL or a period's aggregate has the tile's -- dispatched on a typed `TsFunctions.Fn`) is the same library call the
  tile's writer makes over the same line: the one-line functions, the pairs over each one's returns on the dates both
  have (`correlation`, `beta`, `alpha`, ...; `hedge_ratio`, `residual`, `cov` of levels), the benchmarked ones and
  `lead_lag`, the technicals (`atr`, `stochastic`, `keltner_channels`, `donchian_channels`, `obv`, `vwap`), `if`,
  `compare`, `union_of`, `append`, `prepend`, `interpolate` (gs-quant's four methods), `total_return`,
  `excess_returns`, `weighted_sum`, `basket`, `trend` and `seasonally_adjusted` (their cycle counted in SAMPLE BY's
  periods, the tile's), and `value_at` (R9). Parameters by place after the lines or by name (R10), each checked to
  its kind (a span, a whole number, a number, a word of its own, a date). A year's periods are SAMPLE BY's (252, 52,
  12, 4, 1) or the ones the line's dates infer (`periodsPerYear`), as the tile's.
  - **Results with several values (R10)**: `bollinger_bands`, `keltner_channels`, `donchian_channels` (`lower`,
    `upper`, `width`) and `ohlc` (`open`, `high`, `low`, `close`) name an item whose fields a later item reads
    (`bb.upper - bb.lower`) and FILTER reads (`bb.width > 0`); the answer has a column per field, `bb_lower`,
    `bb_upper`, `bb_width`. A struct read whole, or inside an expression, is refused with the fix.
  - `if(condition, x, y)`: a condition's flags 1 where it holds, 0 where it does not, none where it is none (the
    library's `ifThen` reads a figure's flags). Under SAMPLE BY a candle or a technical reads each period's own
    aggregate as written: `ohlc(first(open['X']), max(high['X']), min(low['X']), last(close['X']))` (the tile rolls
    its daily rows to each period's candle; P5 writes these).
  - Lines that read one line (a struct's fields) join it once: the spine and the inner join each take a line once,
    with every value read of it.
  - `rank` (across instruments, a date's) stays refused as still to come: it is no tile function, and P5 does not need
    it.
  `//core:ts_island_test`: twenty tests -- every function plans and runs on the platform's database (with spans, by
  name, under SAMPLE BY), and arithmetic for a pair's correlation and beta, `compare`, `if`, a band's fields, `value_at`
  and a candle's columns. P5 (the gallery both ways) next.
