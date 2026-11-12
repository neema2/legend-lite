# The test-data generator's SYNTHESIZE mode: design (2026-11-12)

**Status:** a design for the user's rulings (D1-D8), each with a recommendation. Nothing is built. It is to be built
after the rulings, in the slices of §8.

**The go:** the user, 2026-11-12: "Can we do this now instead of parquet", after "Isn't this very close to our test data
generator?". It came by the orchestrator's brief to the B4 worker, which follows B2d.3 (branch `claude/total-wall-b2d3`).

**Its first user:** DataCube's demo sample, the 200,000 trades of `datacube/demo/boot.ts`. Today they are made in the
tab as Arrow data and put in by `DuckDbEngine.loadArrow`: the last data entering outside a plan. The B3 worker wires the
demo once the mode exists.

## 0. What it is

The generator has one mode today, **EXTRACT** (`generateTestData`, `TestDataPlanner`): from given starting rows it
follows the query's joins over real rows and writes the slice it reached. The database does every step, and the plan
holds one read-only statement.

**SYNTHESIZE** is the second mode, over the same planner. Instead of fetching rows it generates them:
- N rows for a root, a class through its mapping or a table;
- child rows whose join columns hold their parents' keys;
- one planned statement the database runs: a row series of N, and a seeded value expression per column.

Its rows land in one of two places:
- **into tables**, by a planned load step: `CREATE TABLE` then `INSERT INTO t SELECT <the generator>`;
- **as test-data text**, the format EXTRACT writes and every test-data reader reads.

There are no data bytes in the plan: the plan carries the generator statement, which is its own content (G3, G6, G10,
G11).

**What it shares with EXTRACT:**
- the relation tree (`ScanRelations.relTree`: which tables a query needs, and how they join);
- the column map (which columns of each table);
- the text writer (`TestDataPlanner.csv`, S191's cells);
- the tables' DDL (`Ddl`, from the store's declarations).

**What is new:**
- the row source, a series of N;
- the value expression per column;
- the wiring of keys and foreign keys.

## 1. D1: the API

**Options:**
- **A. A lite-only Pure function** in lite's own namespace, where lite's other lite-only functions live
  (`meta::legend::lite::tds`, `::recurse`, `::merge`, ...). For example:

  ```
  meta::legend::lite::synthesize(query: FunctionDefinition<Any>[1], mapping: Mapping[1], count: Integer[1],
      seed: Integer[1], profile: meta::legend::lite::synth::Column[*]): meta::relational::testDataGeneration::TestDataGenResult[1]
  ```

  It mirrors `generateTestData`'s signature and result (`dataCsvString`), so a test body plans it on the program road
  as it plans `generateTestData` today (`GeneratorSplice`).
- **B. A planner entry with a host route.** `TestDataPlanner.synthesize(ctx, root, mapping, count, seed, profile)`
  returns a `Planned`. It is exposed twice:
  - to the server as a lite route, `/api/lite/v1/testData/synthesize`: JSON in; out, the load's statements or the text;
  - to the WebAssembly planner, the demo's tab, as one planning call that returns the load for the tab's DuckDB to run.
- **C. A model `Data` element kind**, the model's own synthesized data. For example:

  ```
  Data x::Trades { Synthesized #{ table: trades::DB.TRADES; rows: 200000; seed: 7; ... }# }
  ```

  It loads as a project's `Data` loads (cadence `ONCE`, L3 of the data-in-the-plan program).

**The root:**
- a class through a mapping: its relation tree, as EXTRACT's;
- or a table: the demo's `TRADES` has no class; its root is the relation accessor `#>{trades::DB.TRADES}#`, a query
  like any other.

Children are reached only through the query's joins, so SYNTHESIZE generates exactly the tables a query reads.

**How a host asks:**
- a request: the model, the root (a query), the mapping where there is one, `count`, `seed` and the profile (D2);
- the answer: the load (statements for a target) or the text.

**Recommended:** B first, since the demo needs a host call: one planning entry and one load. Build A as its Pure face in
the same slice; it is cheap, because a generator call in a body is already planned by `GeneratorSplice`. Build C later,
if the user wants synthesized data to live in a model.

## 2. D2: values per type, and who steers them

**Defaults per type** (no profile), each a function of the seed, the column and the row (D3):

| Type | Default |
|---|---|
| Integer | 0..9999 |
| Float | 0.00..9999.99, written as an exact two-place decimal and cast to Float (D3) |
| Decimal(p, s) | 0..10^(p-s)-1, with s places |
| String, VARCHAR(n) | `<COLUMN>_<k>`, k from a domain of 100 values, cut to n characters |
| Boolean | true or false |
| StrictDate | the 5 years ending 2025-12-31 (a fixed date, never today: the rows must not depend on the day) |
| DateTime | the same 5 years, to the second |
| Enumeration | a class property of an enumeration: its mapped source values, from the `EnumerationMapping` |
| A nullable column | NULL at a rate of 10% |

**Steering:**
- **a request profile, per column:**
  - `values: [...]` (one of, optionally weighted);
  - `range: lo..hi`;
  - `scale`;
  - `pattern` (a prefix and the digits of k);
  - `distinct: k` (the domain's size);
  - `nullRate`.
- **model-level steering, later:** tagged values on class properties (a lite profile, for example
  `meta::legend::lite::synth` with tags `values`, `range`, `nullRate`). Relational columns carry no tagged values in
  Pure, so a table root, the demo's, is steered by the request alone.

**Recommended:** the type defaults and a request profile now; model tags later, if the user wants them.

## 3. D3: determinism, the same seed giving the same rows on every database

**Options:**
- **(a) A portable seeded hash, in integer arithmetic every database computes exactly.**
  - `h(seed, column, i)` uses BIGINT `+`, `*` and `MOD` alone. For example, rounds of a multiplicative hash modulo the
    Mersenne prime 2^31 - 1, with every operand below 2^31, so every product fits a signed 64-bit integer.
  - Each value is integer arithmetic over `h`: an index into a list, an offset in a range, a decimal's digits.
  - A Float is generated as an exact decimal `k / 10^s` and cast. Every IEEE database makes the same double from it,
    so no float arithmetic decides a value.
- **(b) Each database's `random()` with a seed** (`setseed` in DuckDB and Postgres). Each database's generator differs,
  so the rows are not the same across databases; H2 and SQLite seed differently or not at all; a row's value would
  depend on its scan order.

**Recommended: (a)**, on every dialect the series renders (§8, S1).
- DuckDB comes first in the build order, for the demo.
- The pin: one test compares DuckDB, H2, SQLite and Postgres cell for cell.
- The hash's spread is measured in S1 (each column's values against its domain). A better integer mix replaces the
  first one if the measurement asks; the rule stays integers only, every intermediate below 2^63.

## 4. D4: keys and joins

**Primary keys are unique by construction:**
- an integer key is the row's index, 1..N;
- a text key is `<prefix><index>`;
- a composite key's first column is the index, and the others come from the hash;
- a table with no key needs no uniqueness. The demo's `TRADES` has none.

**Foreign keys:** a child's join columns take a parent's key. The relation tree's join condition (an equality of
columns, the condition EXTRACT reads, `TestDataPlanner.condition`) names the columns.
- **Uniform many-to-one (the default):** child row i's parent is `1 + h(seed, fk, i) mod N_parent`, always a generated
  parent.
- **A fan-out, by the profile's `perParent: k`:** `N_child = k * N_parent`, and child i's parent is `ceil(i / k)`.
- **A self-join** (a manager): the key points at a lower index, so the rows form a tree. The first rows' key is NULL
  where the column is nullable, else refused by name.
- **Refused by name:**
  - a join that is not an equality of columns (a function, a filter, a condition across more than the two tables);
  - a milestoned table beyond one live version per key.

**Recommended:** as above. Uniform many-to-one by default; a fan-out by the profile.

## 5. D5: Pure constraints

A class's constraints, on the root or its children:
- **Satisfied where expressible:** a property compared with a literal (`>`, `>=`, `<`, `<=`, `==`, `!=`, `in`),
  `isNotEmpty`, and a String's length narrow that column's domain at planning, a model fact.
- **Refused by name otherwise** (two properties, a navigation, a function). For example, "the constraint c1 of
  model::Trade is not synthesized: it reads two properties". A profile may give the column's domain explicitly; the
  constraint is still checked against it at planning.
- **Generate-then-filter is rejected:** the count would no longer be N, and the rows would depend on the filter's
  selectivity.

**Recommended:** narrow by simple bounds; refuse the rest by name.

## 6. D6: what the demo's sample needs

**Today** (`datacube/src/sample-trades.ts`): 200,000 rows, Arrow made in the tab, put in by `DuckDbEngine.loadArrow`.
Its CSV twin, `DESK_TRADES_FILE`, is pinned by size (10,217,418) and SHA-256 (`test/sample-trades.test.ts`). The columns
are closed forms over the row index i:

| Column | Today |
|---|---|
| region | `EMEA, AMER, APAC`, i mod 3 |
| desk | `Rates, Credit, FX, Equity, Commodities`, floor(i/3) mod 5 |
| year | 2021..2025 |
| qtr | Q1..Q4 |
| book | `Book 1`..`Book 4` |
| notional | (i * 7919 mod 10^6) / 100: 0..9999.99 |
| pnl | (i * 104729 mod 200000) / 100 - 1000: -1000..999.99 |
| qty | (i * 31 mod 97) + 1: 1..97 |

**With SYNTHESIZE,** a profile for `trades::DB.TRADES`:
- region: values `[EMEA, AMER, APAC]`;
- desk: values `[Rates, Credit, FX, Equity, Commodities]`;
- book: values `[Book 1, Book 2, Book 3, Book 4]`;
- qtr: values `[Q1, Q2, Q3, Q4]`;
- year: range 2021..2025;
- notional: range 0..9999.99, scale 2;
- pnl: range -1000..999.99, scale 2;
- qty: range 1..97.

Each column spreads over the same domain, uniformly, so the demo's grids look as they do today.

**The numbers change.** Each row's values come from the hash, not today's closed forms. The `DESK_TRADES_FILE` pins
(size and SHA-256) and `test/sample-trades.test.ts` move with them. The user accepted this implicitly; it is posted
here so it is explicit. (A profile `expression` per column could reproduce today's values exactly, since the closed
forms are integer arithmetic over the row index. Not recommended: it makes the profile a language.)

**How the tab runs it:**
- The WebAssembly planner plans the load: `CREATE TABLE trades (...)`, then
  `INSERT INTO trades SELECT ... FROM range(1, 200001)`.
- The tab's DuckDB runs those statements. There are no Arrow bytes, and `loadArrow` goes from the demo.
- DuckDB makes 200,000 rows of integer arithmetic in milliseconds. S6 measures it against today's Arrow build in
  JavaScript.

## 7. D7: upstream

legend-engine has no synthesizer.
- **Checked 2026-11-12 in the pinned sources:** no synthesize, random-data or fake-data function in the relational store
  or the core.
- **Its DataCube REPL's samples** (`DataCubeSampleData`: `sport`, `tree`) are CSV files shipped in its resources and
  loaded with the database's file load.

SYNTHESIZE is therefore lite-only, in lite's own namespace (`meta::legend::lite::synthesize`, never under
`meta::pure::` or `meta::relational::`), so no upstream behaviour is shadowed. It is a scope row: lite offers what
upstream does not. The proposed row, S303, is not landed:

> | S303 | scope (PROPOSED, for the user's ruling) | legend-engine has no data synthesizer: its test-data generator
> extracts rows (`generateTestData`), and its DataCube samples are shipped CSV files (`DataCubeSampleData`) | lite's
> test-data generator has a SYNTHESIZE mode (`meta::legend::lite::synthesize`, the planner's
> `TestDataPlanner.synthesize`): N generated rows for a root and their children, one planned statement, the same rows
> for the same seed on every database | the user's go of 2026-11-12 | (the S1-S6 tests) | SYNTHESIZE |

## 8. D8: the slices and their tests

1. **S1, the row source and the hash.**
   - A new MIR source, `SqlSource.Series(count, alias, column)`, with a render arm per dialect: DuckDB `range`,
     Postgres `generate_series`, H2 `SYSTEM_RANGE`, SQLite a recursive common table expression; any other refused by
     name.
   - The hash as typed MIR over `PLUS`, `TIMES` and `MOD`.
   - Tests: the same 1,000 hashes on DuckDB, H2, SQLite and Postgres; the spread per domain measured.
2. **S2, one table.**
   - Values per type and per profile, as text and as a load.
   - Tests: each type; nulls at their rate; `VARCHAR(n)` lengths; the same rows on every database, cell for cell.
3. **S3, keys and joins.**
   - Tests: primary keys unique; every foreign key a generated parent's; a fan-out; a self-join; a join that is not
     column equality refused by name.
4. **S4, constraints.** Tests: simple bounds narrow a domain; every other constraint refused by name.
5. **S5, the doors.**
   - The planner route: the server, and the WebAssembly planner.
   - The Pure face, `meta::legend::lite::synthesize`, on the program road.
   - The load: a `Load` of `Statement` steps, cadence `ONCE`, its identity the canonical statements'.
6. **S6, the demo** (the B3 worker): the profile for `TRADES`; `boot.ts` runs the planned load; `loadArrow` deleted;
   the pins updated.

**Guards:**
- The data-in-the-plan census gains no mechanism: a synthesized load is a `Load` of `Statement` steps.
- A guard holds that a synthesized load carries no `Rows` or `File` step: no data bytes in the plan.

**The decisions, collected for the user:**
- D1: B now, with A as its Pure face; C later.
- D2: type defaults plus a request profile; model tags later.
- D3: the portable integer hash on every dialect, DuckDB first.
- D4: keys by index; uniform foreign keys, or a fan-out by profile; other joins refused.
- D5: simple bounds narrow a domain; other constraints refused.
- D6: the demo's numbers change, with the same domains and spread.
- D7: lite-only, row S303.
- D8: the slices above.
