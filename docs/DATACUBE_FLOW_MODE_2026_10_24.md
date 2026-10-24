# DataCube's flow mode: a visual ETL tool as a mode of DataCube (2026-10-24)

**The goal (the user, 2026-10-24).** A visual ETL and data prep tool that feels like a slick, modern Alteryx, with as
many of Alteryx's features as possible. legend-lite compiles every step and pushes it down to the database, with a
DuckDB copy (a "cache") when wanted. It is a **mode of DataCube**: the same logic and the same API, drawn as a
pipeline (a DAG) instead of a grid. The strict UI/API split DataCube keeps holds here too.

**Where this comes from.** Two earlier plans already point here, and neither was built:

- `docs/BI_AND_ETL_PLAN_2026_09_29.md` track C: a pipeline is a Pure program, the canvas edits it, the database runs it,
  and every node's preview is a DataCube on that node.
- `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md` §17.6: an Alteryx-like prep flow, in which "the boxes are the page's grids,
  and the lines are their join inputs. No second model."

This note puts the two together. It records the user's decisions (§1) and proposes the rest (§3 to §5). Nothing in it
is built yet.

---

## 1. Decisions (the user, 2026-10-24)

- **FL1. A mode of DataCube, not a second app.** The flow is another way of drawing the page document. Every gesture on
  the canvas is a `PageCommand` or `GridCommand`, applied by the one function, as every other editor's is
  (`docs/DATACUBE_ONE_API_PLAN_2026_10_21.md`). So undo, save, share and Python come with it, and the canvas builds no
  command itself: screen code builds none (One API S8, ledger L11).
- **FL2. A Cross Tab's columns are fixed by a cast.** A Cross Tab box's output is typed statically: `pivot(...)` is
  followed by `->cast(@Relation<(...)>)`, the pattern core already knows (`compiler/spec/typed/TypedPivot.java`). The
  cast's columns come from **Load values**: the values query (`pivotValuesLambda`, `datacube/src/query.ts:345`) run
  once and written into the cast. The person can then edit the list. Boxes downstream are typed from the cast.
- **FL3. A value the cast lacks is said, never dropped in silence.** A run compares the values query with the cast and
  names every value the cast does not list. With a cast alone, such a value would get no column and nobody would know;
  that is a fallback, and invariant 4 refuses fallbacks.

**Open, from FL3:** a warning or a refusal? Proposed: a preview warns (the box is marked, the new values named, with
"Add to the cast" offered), and a run that writes (an Output box, §4) refuses, so no table is written short of
columns.

---

## 2. What exists, and what the flow reuses

| The flow needs | What DataCube has today |
|---|---|
| Types on every box, as it is connected | The tab's planner (`//wasm:planner`), the same compiler as the server: every query's relation type, with no server |
| A preview of any box | A grid: the selected box's rows shown as a DataCube, as Alteryx's results pane shows them |
| Edits as data | `GridCommand` (`datacube/src/grid-commands.ts`) and `PageCommand` (`datacube/src/page-commands.ts`), one apply function each, a generated Python builder each (D17) |
| The tools' logic | The filter editor (`FilterNode`), calculated columns checked by the compiler (row and window), aggregates (`wavg` and `unique` included), sorts, joins (`join.ts`: any condition, as of, keep my rows with a match or with none, append), Expand and JSON fields |
| A cache | Snap: a grid's rows copied into a store and read from there (§18.8 of the model design: Snap is a command) |
| A second way of drawing the same state | Ad Hoc mode (`datacube/src/adhoc/mode.ts`): "turns a gesture into ONE session call", and "nothing here computes a number" |
| Output | The grid's exports, charts, KPIs and pages. Not a table write (§4) |

---

## 3. The design (proposed)

### 3.1 Live links: the one missing primitive

Today a join keeps its two inputs **by value** (`JoinSource`, `datacube/src/cube-document.ts:170`): "an input is what
its grid was when it was joined". So the page is not a graph: an edit to a grid upstream does not reach the grids that
read it.

The flow needs a new kind of source: **the output of another grid** (`{grid: 'grid-3'}`, as §17.6 has it). It fits the
model design's §18 rule ("a grid is a query, not a table"): the new kind denotes the upstream grid's output query, and
its leaves are that grid's leaves. Nothing else needs to know what kind of source a grid has.

What comes with it:

- **A cycle is refused** by the page command that would make it.
- **Grids open in dependency order** (class grids already open last, §16.4).
- **A removed upstream grid** turns the link into a copy of what that grid was, and this is said (§17.6).
- **A rename upstream** is followed downstream by the page's rebase (`datacube/src/page-rebase.ts`).
- **A box's output** is its rows at their finest level: `classLambda` (`datacube/src/query.ts:380`), the rows a join
  input already reads. A grouped grid outputs its groups, one row per group with its measures beside its keys, which is
  Alteryx's Summarize. A pivoted grid outputs its cast (FL2); `classLambda`'s refusal of a pivot is Create a Class's
  rule (a class's properties are fixed), not a link's.

### 3.2 One box is one tool, and each tool is a grid

A grid applies its steps in a fixed order: calculated columns, then the filter, the grouping, group-level calculations,
the pivot and the sorts. Alteryx users expect one tool per box, in any order.

**Proposed:** each tool box is a grid that does one thing. A Filter box is a grid with only a filter, a Summarize box a
grid with only a grouping and its measures, a Formula box a grid with only calculated columns. Boxes chained by live
links give any order: a filter after a summarize is two boxes. The canvas never shows the word "grid"; the results pane
is the grid.

**Rejected:** a list of steps inside one box. It would be a second model of a query beside the grid's, which is what
the One API program removed.

### 3.3 Tools with several outputs: ports

Some Alteryx tools have more than one output:

| Tool | Outputs | Each output's query |
|---|---|---|
| Filter | True, False | the filter; its negation |
| Join | L, J, R | left rows with no match; the join; right rows with no match (join.ts has both "no match" forms) |
| Unique | Unique, Duplicates | the first row of each key; the rest (a row number over the key) |
| Create Samples (later) | Estimation, Validation, Holdout | three random slices |

Each port denotes a query of its own, worked out from the box: nothing is stored per port. A live link names its port
(`{grid: 'grid-3', port: 'false'}`). A link that names no port reads the box's main output.

### 3.4 The canvas's own state

- **A box's place on the canvas** is page state, set by a page command (as a sheet's `layout` is), so undo, save,
  share and Python treat it like any other edit. Auto-layout is a command too.
- **Annotations and containers** (Alteryx's Comment and Tool Container) are page state the same way.
- **The selected box** is the screen's, not the page's (as a selected cell is today).

### 3.5 New command kinds ship whole

Each new command kind (the live link, a port's link, a box's place) ships with its apply rule, its JSON reader and
writer, its rebase arm, a UI producer, a Python builder and a parity row (One API rule 6). Each one raises `L8_KINDS` in
`datacube/test/one-api-ledger.test.ts`, which needs the user's dated sign-off (D9).

---

## 4. Alteryx's tools, one by one

How to read the status column:

- **Built**: DataCube does it on a grid today. The flow only draws it.
- **Step**: DataCube work only (a preset, a port, a command) over what the compiler already has. Where it names a
  Pure function, the function is in the platform's registry (`core/src/main/resources/com/legend/builtin/native-claims.tsv`);
  whether every database lowers it is checked when the step is built, not assumed here.
- **Core**: needs a change in core first.
- **Later**: possible, not in the first slices.
- **Out**: refused, with the reason.

### In and out

| Tool | Status | How |
|---|---|---|
| Input Data | Built | A grid's sources: a file, a remote file, a warehouse table, a class, a saved query, a frame |
| Text Input | Step | A small table typed in, as a source (Pure's `#TDS` literal) |
| Browse | Built | The results pane: the selected box's grid |
| Output Data (to a table) | Core | `write` to a store table throws "not yet implemented" (`core/.../lowering/Lowerer.java:3295`): foundation F1 of the BI and ETL plan, with create, replace, append and merge by key |
| Output Data (to a file) | Built | The grid's exports, for the rows shown. The whole result streamed is A5 of the BI and ETL plan |
| Date Time Now | Built | `now()` and `today()` in a Formula |
| Directory | Out | It lists a file system, which is no relation a database holds |
| Dynamic Input | Later | A source chosen at run time; needs runs (F3) and parameters (F5) |

### Preparation

| Tool | Status | How |
|---|---|---|
| Filter (True, False) | Step | The grid's filter; the False port (§3.3) |
| Formula | Built | A calculated column, checked by the compiler as it is typed |
| Multi-Row Formula | Built | A window column: `lag`, `lead`, `first`, `last` |
| Multi-Field Formula | Step | One formula over several columns, made as one calculated column each |
| Select (choose, rename, retype) | Step | Choosing is the grid's shown columns. Renaming is a display name today, not the relation's `rename`; both as relation steps (§17.8). Retyping is `cast` |
| Sort | Built | The grid's sorts |
| Sample (first, last, every Nth, N per group) | Step | `limit`, `drop`, `slice`, and a row number for N per group |
| Random % Sample | Step | `random()` (DuckDB writes `random()`) |
| Unique (Unique, Duplicates) | Step | `removeDuplicatesBy`; the Duplicates port (§3.3) |
| Record ID | Built | A window column: `rowNumber` |
| Tile | Built | A window column: `ntile` |
| Data Cleansing | Step | Presets of calculated columns: `trim`, `toUpper`, `toLower`, `replace`, `regexpReplace`, empties to a value (§17.6) |
| Imputation | Step | A preset: `coalesce` with the column's average, median or a value |
| Date Filter | Built | The filter, on a date column |
| Generate Rows | Later | Core has no relation that makes rows from a rule; needs a design |
| Auto Field | Out | Every column already has its type from the compiler; there is nothing to guess |
| Create Samples, Oversample, Multi-Field Binning | Later | `random()` and the bins preset are the parts |

### Join

| Tool | Status | How |
|---|---|---|
| Join (L, J, R) | Step | `join.ts`: inner, left, right, full, any condition, as of. The L and R ports are its "no match" forms (§3.3) |
| Join Multiple | Built, once linked | Joins chained by live links |
| Union | Built | A join's append |
| Append Fields | Step | Every row with every row: a join whose condition is always true |
| Find Replace | Step | A join, then `replace` |
| Fuzzy Match | Step | A join whose condition is `levenshteinDistance` or `jaroWinklerSimilarity` against a threshold |
| Make Group | Out for now | Groups made from pairs need a transitive closure, which one relation cannot say |

### Parse

| Tool | Status | How |
|---|---|---|
| Text To Columns (to columns) | Step | `splitPart`, one calculated column per part |
| Text To Columns (to rows) | Step | `split`, then Expand |
| RegEx | Step | `regexpExtract`, `regexpReplace`, `regexpLike` in a Formula, offered as one tool |
| DateTime | Step | `parseDate` and `formatDate` in a Formula, offered as one tool |
| JSON Parse | Built | JSON fields (`datacube/src/ui/json-fields.ts`) and Expand |
| XML Parse | Out | No XML function in the platform |

### Transform

| Tool | Status | How |
|---|---|---|
| Summarize | Built | A grid's grouping and measures: sum, count, average, min, max, median, quartiles, standard deviation, variance, concatenate (`joinStrings`), weighted average, unique |
| Cross Tab | Step | The grid's pivot, typed by its cast (FL2, FL3) |
| Transpose | Core | No `unpivot` function in the platform |
| Running Total | Built | A window column, running frame |
| Weighted Average | Built | `wavg` |
| Count Records | Built | `count` |
| Arrange, Make Columns | Later | Layout tools for reports; the pages do most of what they are for |

### Investigate and report

| Tool | Status | How |
|---|---|---|
| Field Summary, Basic Data Profile | Step | **Profile** (§17.6): per column, the count, empties, distinct values and top values |
| Frequency Table, Histogram, Scatterplot | Built | Charts on a box's grid: bars over bins, scatter, box plot |
| Pearson Correlation | Step | `corr` as an aggregate |
| Charts, Table, Layout, Render, Email | Built | Charts, KPIs, pages, exports and the email draft |

### Developer, apps and macros

| Tool | Status | How |
|---|---|---|
| Test | Later | Checks as boxes (not null, unique, in range, row count), compiled to SQL; a failing check stops the write (track C5) |
| Block Until Done | Later | Run order, from runs and schedules (F3) |
| Message | Later | The run's log |
| Base 64 Encoder | Built | `encodeBase64` in a Formula |
| Dynamic Rename, Select, Replace | Later | |
| Interface tools (an analytic app's inputs) | Core | Query parameters: execute takes no parameter values yet (F5) |
| Standard macro | Later | A sub-flow saved as a Pure function with parameters, used as a box; needs a function with parameters as a source (§17.6, item 8) |
| Batch and iterative macros | Out for now | A loop is not one query; it would be orchestration |
| Python, R | Out | They compute outside the database ("Java orchestrates, the database executes") |
| Download, Run Command | Out | Not a database's work |

### Not ours

- **Spatial, predictive, time series, prescriptive, address and demographic tools:** a different product. A function
  the database offers (a spatial or statistical one) can be added as a native when asked for.
- **Connectors to SaaS applications:** out of scope, as the BI and ETL plan's §5 says.
- **The In-Database palette:** every tool here runs in the database, so there is no separate palette for it.

---

## 5. Order (proposed)

1. **After D19.** The canvas is a second editor of the page, so it waits for D19 (the client side of the single
   writer: pending edits kept and re-applied, Save on a served page). Built before it, it would meet the races between
   two editors that D19 fixes.
2. **Live links** (§3.1), each command kind whole (§3.5).
3. **The flow view over today's grids:** boxes and lines drawn from the page document, the selected box's grid below,
   a box in red with the compiler's reason, auto-layout.
4. **The palette:** each tool makes a one-tool grid (§3.2), grouped as Alteryx groups them; then ports (§3.3).
5. **Cross Tab with its cast** (FL2, FL3), Profile, text to columns, sample, the cleansing presets; then Import Pure.
6. **In core, beside DataCube's steps:** F1 (`write` into a table) and the `unpivot` native, as slices of core's own
   line, not edits inside DataCube's.

---

## 6. Questions for the user

1. **FL3:** a preview warns and a write refuses (proposed), or the same rule for both?
2. **The canvas's place in the app:** a mode the whole page switches into (as Ad Hoc mode is), or a sheet of the page
   drawn as a flow, beside sheets drawn as boards?
3. **Ports:** as in §3.3, or one output per box, with the other outputs as separate boxes?
