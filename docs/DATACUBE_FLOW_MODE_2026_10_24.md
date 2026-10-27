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

This note puts the two together and records the user's decisions (§1). §3 to §5 are the design those decisions make.
Nothing in it is built yet.

---

## 1. Decisions (the user, 2026-10-24)

Each was put to the user with its options, pros, cons and cost; the user took the recommended option every time ("Agree
with all"). The options not taken are in §7, so the reasons are kept.

**What it is**

- **FL1. A mode of DataCube, not a second app.** The flow is another way of drawing the page document. Every gesture on
  the canvas is a `PageCommand` or `GridCommand`, applied by the one function, as every other editor's is
  (`docs/DATACUBE_ONE_API_PLAN_2026_10_21.md`). So undo, save, share and Python come with it, and the canvas builds no
  command itself: screen code builds none (One API S8, ledger L11).
- **FL4. The flow is a sheet of the page.** A sheet's layout is a tagged type (`PageLayout`, `kind: 'bands'`,
  `datacube/src/page-document.ts:67`); a flow sheet is `kind: 'flow'`, its boxes placed on a canvas. Prep sheets and
  dashboard sheets are sheets of one page, and a dashboard sheet reads a box through a link.
- **FL5. A box is a grid that does one thing.** A Filter box is a grid with only a filter; a Summarize box a grid with
  only a grouping and its measures. Boxes chained by links give any order. No second model of a query.

**How boxes connect**

- **FL6. An input is live or a frozen copy, chosen per input.** A live input reads another grid's output as it is now
  (`{grid, port}`); a frozen input is a copy of what that grid was, today's `JoinSource` input
  (`datacube/src/cube-document.ts:170`). New joins are live. Joins saved before this read as frozen copies, so no saved
  page changes or stops opening. A live input whose grid is removed becomes frozen, and this is said.
- **FL7. Tools with several outputs have ports**, as Alteryx's anchors are (§3.3). Each port is a query worked out from
  its box; an unconnected port runs nothing. A line's record count is shown after a run, or when asked for, not kept
  live.
- **FL9. A gesture in the results pane adds a box** after the selected one (filter by a value, sort, group), so every
  box keeps doing one thing.

**Typing and checks**

- **FL2. A Cross Tab's columns are fixed by a cast.** A Cross Tab box's output is typed statically: `pivot(...)` is
  followed by `->cast(@Relation<(...)>)`, the pattern core already knows (`compiler/spec/typed/TypedPivot.java`). The
  cast's columns come from **Load values**: the values query (`pivotValuesLambda`, `datacube/src/query.ts:345`) run
  once and written into the cast. The person can then edit the list. Boxes downstream are typed from the cast.
- **FL3. A value the cast lacks is said, never dropped in silence.** A run compares the values query with the cast and
  names every value the cast does not list. With a cast alone, such a value would get no column and nobody would know;
  that is a fallback, and invariant 4 refuses fallbacks.
- **FL8. Each check has a mode: warn or refuse.** The checks: a Cross Tab's values against its cast (FL3); a source
  whose columns changed; the data-quality checks (Alteryx's Test tool, track C5). The mode is set on each check, as
  dbt's `severity` is. A new check is made with **refuse**, written into the document: the document always says the
  mode, and a check read with none is refused, never defaulted. A **preview** always shows the rows and marks the box
  (amber for warn, red for refuse) with the values or columns named and the fix offered ("Add to the cast"). A **run**
  stops before it writes anything on a refusal, and goes on with the finding in its log on a warning. A third mode,
  **follow** (each run reloads the values, rewrites the cast and recompiles what is downstream), is for later.

**Data, databases and Pure**

- **FL10. A Cache box moves data between databases, visibly.** A join over two databases is refused with an offer to
  put a Cache box on one side: a box that copies its input into a chosen database, built on Snap (`snap` and `live`,
  the model design's §18.8). Nothing is copied without a box saying so. First target: the host's own DuckDB (the tab's,
  or Python's on a page Python serves).
- **FL11. The cache lasts as long as its session**, as a snap does today (§18.8, item 7) and as Alteryx's Cache and Run
  does. A reopened flow reads its sources again, and says so.
- **FL12. The page document is the source; Pure is imported, not edited in place.** Each box's Pure is shown and can be
  exported. **Import Pure** turns a lambda into boxes; whatever does not become boxes becomes a **Pure box**: a box
  whose query is a Pure expression over its inputs, checked by the compiler. The Pure box is also "filter by a Pure
  expression" (§17.6, item 6). Editing the Pure and having the canvas redraw (track C2's "both directions") is for
  later, if wanted.

**Scope and timing**

- **FL13. The first version writes no tables.** Its outputs are the Cache box, Snap and the grid's exports. F1 (`write`
  into a table: create, replace, append, merge by key) is built in core in parallel, as a slice of core's own line, not
  inside DataCube's. Output boxes come when it lands. *Since (2026-10-25): the writes line landed store writes on main
  (`docs/WRITES_DESIGN_2026_10_18.md` legs 2 and 7: `write`, `update`, `delete` and `merge` over a store table on
  DuckDB, H2 and Postgres, and write plans run by the model-free runner), so F1 is no longer waited for; Output boxes
  are the roadmap's M6 (§5).*
- **FL14. Transpose is composed, not a new native.** One `select` per chosen column (the keys, that column renamed to a
  name and a value), the selections concatenated. It runs on every database and needs no core change. The value
  columns must share a type; where they do not, the compiler refuses in its words, and a cast is offered.
- **FL15. No Python or R boxes in the first version.** They compute outside the database ("Java orchestrates, the
  database executes"). A Python box run by the page's Python host, as a named exception ruled under
  `docs/TENET_CHARTER.md`, can be asked for later.
- **FL16. The logic now, the canvas after D19.** The link and port commands (pure functions: apply rules, readers,
  rebase, Python builders), Transpose and core's F1 start now, beside D19. The canvas, a second editor of the page,
  waits for D19 (`docs/DATACUBE_ONE_API_PLAN_2026_10_21.md`: the client side of the single writer), so it is not built
  on the races between two editors that D19 fixes.

**Decisions (the user, 2026-10-25)**

- **FL17. The roadmap's order stands** (§5), and so does the reuse: each box's settings are DataCube's own editors.
- **FL18. The Filter window, sleeker, the same view.** As the Join window was rebuilt in the Create a Class window's
  style and the Properties window cleaned up, the Filter window is restyled the same way, keeping its tree of
  conditions and groups as it is: the same view, slightly sleeker. Its one editor (`datacube/src/ui/filter-editor.ts`)
  is the grid's Filter window and the flow's Filter box alike.
- **FL19. Union goes back into DataCube.** The flow's Union (M2) is built as DataCube's own, so a grid can union as it
  can join. DataCube has half of it already: the Join window's **Append** form (`join-how.ts`, `form: 'append'`) puts
  two grids' rows together, columns matched by name, a column one side lacks empty on its rows. What Union adds, to the
  grid and the flow both: a Union entry of its own beside Join, and any number of inputs. Proposed, as Alteryx's Union
  offers it: matching by position as well as by name.

**Decision (the user, 2026-10-25)**

- **FL20. Python's names for flows** (D6): `page.flow()` adds a Flow sheet; `flow.grid(...)` an Input box, with
  `page.grid`'s arguments; `flow.grid(input=box)` (and `page.grid(input=grid)`) a box after another, reading its rows
  live, as `page.grid(cls=..., query=..., warehouse=...)` names its sources; `box.place(x, y)` a box's place. Every
  tool's settings are the grid's own methods (`filter`, `calculate`, `sort`, `group`, ...): a box is a grid. *(The
  same day: `rows=` was proposed first and approved, then found taken -- `page.grid`'s `rows=` is its row groups -- and
  the user chose `input=`, Alteryx's word for what a tool reads.)*

---

## 2. What exists, and what the flow reuses

| The flow needs | What DataCube has today |
|---|---|
| Types on every box, as it is connected | The tab's planner (`//wasm:planner`), the same compiler as the server: every query's relation type, with no server |
| A preview of any box or port | A grid: the selected box's rows shown as a DataCube, as Alteryx's results pane shows them |
| Edits as data | `GridCommand` (`datacube/src/grid-commands.ts`) and `PageCommand` (`datacube/src/page-commands.ts`), one apply function each, a generated Python builder each (D17) |
| The tools' logic | The filter editor (`FilterNode`), calculated columns checked by the compiler (row and window), aggregates (`wavg` and `unique` included), sorts, joins (`join.ts`: any condition, as of, keep my rows with a match or with none, append), Expand and JSON fields |
| A cache | Snap: a grid's rows copied into a store and read from there (§18.8 of the model design: Snap is a command) |
| A second way of drawing the same state | Ad Hoc mode (`datacube/src/adhoc/mode.ts`): "turns a gesture into ONE session call", and "nothing here computes a number" |
| Sheets of one page | `PageSheet` and its tagged `PageLayout` (`datacube/src/page-document.ts:67`) |
| Output | The grid's exports, charts, KPIs and pages. Not a table write (FL13) |

---

## 3. The design

### 3.1 Live inputs: the one missing primitive

Today a join keeps its two inputs by value: "an input is what its grid was when it was joined". So the page is not a
graph: an edit to a grid upstream does not reach the grids that read it.

The flow adds the live input (FL6): the output of another grid, `{grid: 'grid-3'}`, or one of its ports,
`{grid: 'grid-3', port: 'false'}`. It fits the model design's §18 rule ("a grid is a query, not a table"): a live input
denotes the upstream grid's output query, and its leaves are that grid's leaves. Nothing else needs to know what kind
of source a grid has.

What comes with it:

- **A cycle is refused** by the page command that would make it.
- **Grids open in dependency order** (class grids already open last, §16.4).
- **A removed upstream grid** turns the live input into a frozen one, a copy of what that grid was, and this is said.
- **Freeze and unfreeze** are commands: freezing copies the upstream's source and query into the input; unfreezing an
  input whose grid is still on the page makes it live again.
- **A rename upstream** is followed downstream by the page's rebase (`datacube/src/page-rebase.ts`).
- **A box's output** is its rows at their finest level: `classLambda` (`datacube/src/query.ts:380`), what a join input
  already reads. A grouped grid outputs its groups, one row per group with its measures beside its keys: Alteryx's
  Summarize. A pivoted grid outputs its cast (FL2); `classLambda`'s refusal of a pivot is Create a Class's rule (a
  class's properties are fixed), not a link's.

### 3.2 One box, one tool

A grid applies its steps in a fixed order: calculated columns, then the filter, the grouping, group-level calculations,
the pivot and the sorts. Alteryx users expect one tool per box, in any order. Each tool box is therefore a grid that
does one thing (FL5): a filter after a summarize is two boxes. The canvas never shows the word "grid"; the results pane
is the grid. A grid stores only what differs from the defaults (`CubeDocument.configuration`), so a one-tool grid is a
small document.

Tools that are not yet a grid's feature (Union, Sample, Unique, rename, Transpose, the Pure box) each become a grid
feature or a source kind, one at a time, each as its own command kind (§3.5).

### 3.3 Ports

| Tool | Outputs | Each output's query |
|---|---|---|
| Filter | True, False | the filter; its negation |
| Join | L, J, R | left rows with no match; the join; right rows with no match (join.ts has both "no match" forms) |
| Unique | Unique, Duplicates | the first row of each key; the rest (a row number over the key) |
| Create Samples (later) | Estimation, Validation, Holdout | three random slices |

Inputs are anchors too, as in Alteryx: a Join's L and R are its two inputs; a Union takes any number. Each port's query
is worked out from its box: nothing is stored per port. A link that names no port reads the box's main output. Clicking
a port shows its rows in the results pane.

### 3.4 The flow sheet's own state

- **The screen:** the palette on the left, the canvas in the middle, the selected box's settings on the right (the
  editors DataCube has: filter, column, join, pivot), the results pane below.
- **A box's place on the canvas** is the sheet's layout, set by a page command (as a board sheet's `layout` is), so
  undo, save, share and Python treat it like any other edit. Auto-layout is a command too.
- **Annotations and containers** (Alteryx's Comment and Tool Container) are the sheet's state the same way.
- **The selected box** is the screen's, not the page's (as a selected cell is today).

### 3.5 New command kinds ship whole

Each new command kind (a live input, freeze and unfreeze, a box's place, a check's mode) ships with its apply rule, its
JSON reader and writer, its rebase arm, a UI producer, a Python builder and a parity row (One API rule 6). The ledger
L8 in `datacube/test/one-api-ledger.test.ts` enforces it: a kind the screen or Python does not make, or a source kind
Python cannot read and add, fails the build. Its lists (`L8_KINDS`, `L8_SOURCES`) are the gaps, so a kind shipped
whole adds nothing to them; an entry there is a gap left, which only the user's dated sign-off allows (D9).
*(Corrected 2026-10-25: this said every new kind raised `L8_KINDS`.)* A new source kind also joins `PYTHON_ADDS`,
the headless function by which Python adds a grid over it, with its test.

---

## 4. Alteryx's tools, one by one

How to read the status column:

- **Built**: DataCube does it on a grid today. The flow only draws it.
- **Step**: DataCube work only (a preset, a port, a command) over what the compiler already has. Where it names a Pure
  function, the function is in the platform's registry (`core/src/main/resources/com/legend/builtin/native-claims.tsv`)
  and DuckDB's dialect renders it; other databases are checked when the step is built, not assumed here.
- **Core**: needs a change in core first.
- **Later**: possible, not in the first slices.
- **Out**: refused, with the reason.

### In and out

| Tool | Status | How |
|---|---|---|
| Input Data | Built | A grid's sources: a file, a remote file, a warehouse table, a class, a saved query, a frame |
| Text Input | Built (2026-10-25) | A small table typed in or pasted from a sheet, written as a CSV file: an Input over it, as a file dropped on the canvas is. Pure's `#TDS` literal remains the way to keep one in a model |
| Browse | Built | The results pane: the selected box's grid, its Profile, its Chart |
| Output Data (to a table) | Core, after v1 | `write` to a store table throws "not yet implemented" (`core/.../lowering/Lowerer.java:3295`): F1, built in core in parallel (FL13) |
| Output Data (to a file) | Built (2026-10-25) | An Output box: its input's every row (the columns it shows) streamed from the database and written as CSV, by Write now and each time the flow runs; Python's `grid.output(name)` and `flow.run(folder)`. The grid's exports still write the rows shown |
| Data Stream In / Out | Step | The Cache box (FL10), on Snap |
| Date Time Now | Built | `now()` and `today()` in a Formula |
| Directory | Out | It lists a file system, which is no relation a database holds |
| Dynamic Input | Later | A source chosen at run time; needs runs (F3) and parameters (F5) |

### Preparation

| Tool | Status | How |
|---|---|---|
| Filter (True, False) | Built (2026-10-25) | The grid's filter; the False port (§3.3, FL7): the rows its condition leaves out, empties among them |
| Formula | Built | A calculated column, checked by the compiler as it is typed |
| Multi-Row Formula | Built | A window column: `lag`, `lead`, `first`, `last` |
| Multi-Field Formula | Built (2026-10-25) | One expression over `$v`, each column's value, parsed by the compiler: a calculated column of each (`<column><suffix>`), or in place, as Alteryx's "change existing fields" |
| Select (choose, rename, retype) | Built (2026-10-25) | Its own window: each column kept or left out (hidden), renamed (its display name, which a flow's box hands on as the relation's `rename`) and retyped (a calculated column of its conversion -- `parseInteger`, `toString`, `round`, `toFloat`... -- its column hidden, it handed on by the column's name) |
| Sort | Built | The grid's sorts |
| Sample (first, last, every Nth, N per group) | Built (2026-10-25) | The first or last N of all the rows, the grid's Top N; of each group, all but the first N, or one of every N, a row number (`rowNumber`, per group) and a filter on it, hidden -- `mod` of it for one of every N |
| Select Records | Built (2026-10-25) | A Sample's way: a row number and a filter of its ranges (`1-10, 20, 30+`), or-ed |
| Random % Sample | Built (2026-10-25) | A Sample's way, the same rows for a seed: a row number in an order of every column, its chance the database's `hashCode` of it and the seed, kept under the percent |
| Unique (Unique, Duplicates) | Built (2026-10-25) | A row number per key (`rowNumber` partitioned by its columns, in an order), a filter keeping the first, the number hidden; its Duplicates the filter's False port (§3.3), drawn U and D |
| Record ID | Built | A window column: `rowNumber` |
| Tile | Built | A window column: `ntile` |
| Data Cleansing | Built (2026-10-25) | Calculated columns, a `<column>_clean` each: empties made blank (`coalesce`), punctuation out and repeated spaces one (`regexpReplace`), `trim`, `toUpper` / `toLower` (§17.6). In place, as Alteryx's: each handed on by its column's name |
| Imputation | Built (2026-10-25) | `coalesce` with a value, or with a window column of the column's average, smallest or largest across every row (hidden): a `<column>_filled` each. A median waits for a window `median` |
| Date Filter | Built | Its own tool: a date column in a range relative to today, or between two days (date-ranges.ts) |
| Filter's Custom filter | Built (2026-10-25) | Its own tool, Custom Filter: a condition written in Pure over `$x`, parsed and typed by the compiler (a Boolean), a hidden calculated column and the grid's filter on it alone; its T and F (FL12's "filter by a Pure expression") |
| Generate Rows | Built (2026-10-26) | Each row repeated for each whole number from one to another (numbers given or its columns, by a step), or each day between two of its dates: an exploded calculated column of the database's `range`, a day `adjust`ed by each |
| Auto Field | Out | Every column already has its type from the compiler; there is nothing to guess |
| Create Samples | Built | A Sample's two outputs: its rows (S) and the rest (R), its False -- a split by a seed |
| Multi-Field Binning | Built (2026-10-26) | Binning: number columns each into N bins of equal width over its range -- the grid's bins preset, the database placing each value; equal records are the Tile's |
| Oversample | Later | `random()` and a Sample's seed are the parts |

### Join

| Tool | Status | How |
|---|---|---|
| Join (L, J, R) | Built (2026-10-25) | `join.ts`: inner, left, right, full, any condition, as of. Its L and R ports are each side's rows matched against the other's: anti-joins (§3.3) |
| Join Multiple | Built, once linked | Joins chained by live inputs |
| Union | Built | A join's append, of any number of inputs (`JoinSource.more`) |
| Append Fields | Built (2026-10-25) | Every row with every row: a join, matching rows only, on no condition -- the Join window's Every pair |
| Find Replace | Built (2026-10-25) | A Join box, all of mine on my column equal to theirs, its column then handed on as theirs where found (`coalesce`, in place) |
| Fuzzy Match | Built (2026-10-25) | A join row comparing by similar text (`~`): both a value, at most N edits apart by `levenshteinDistance`; a Fuzzy Match box opens the Join window at it. `jaroWinklerSimilarity`, a score, waits for a row of its own |
| Make Group | Out for now | Groups made from pairs need a transitive closure, which one relation cannot say |

### Parse

| Tool | Status | How |
|---|---|---|
| Text To Columns (to columns) | Built (2026-10-25) | `splitPart`, one calculated column per part |
| Text To Columns (to rows) | Built (2026-10-25) | `split`, exploded: one column, each row once a part |
| RegEx | Built (2026-10-25) | `regexpLike` (Match), `regexpReplace` (Replace), `regexpExtract`'s first (Extract), one tool. Tokenize (each match a row) is Expand's, later |
| DateTime | Built (2026-10-25) | One tool, three ways: a date to its period (the grain preset), a date's part (`year`, `monthNumber`, `dayOfMonth`, `weekOfYear`, `hour`), text read as a date by a format (`convertDateFormat` / `convertDateTimeFormat`: strptime). A date written as text waits for a format the compiler takes as text (`formatDate` takes an enum) |
| JSON Parse | Built | Its own tool: the column editor's JSON fields (`datacube/src/ui/json-fields.ts`) and Expand, on the box's JSON column |
| XML Parse | Out | No XML function in the platform |

### Transform

| Tool | Status | How |
|---|---|---|
| Summarize | Built | A grid's grouping and measures: sum, count, average, min, max, median, quartiles, standard deviation, variance, concatenate (`joinStrings`), weighted average, unique -- set in Alteryx's window, a row a column (2026-10-25) |
| Cross Tab | Built (2026-10-25) | The grid's pivot; Load values pins its values, so its columns are fixed and a box after it reads them, each named by its value (FL2); a value its rows gain that the pin lacks asked of the database and said on its box, a run stopped by it in refuse mode (FL3, FL8) |
| Transpose | Built (2026-10-25) | An exploded column over the list of the columns' names (`lateral(...->flatten(~Name))`), a column choosing each row's value by its name (`if`), every column but the keys hidden; columns of one kind of value |
| Running Total | Built | A window column, running frame |
| Weighted Average | Built | `wavg` |
| Count Records | Built (2026-10-25) | One row, its count: a column of one value grouped by and hidden, a 1 for each row summed (`count-records.ts`) |
| Arrange, Make Columns | Later | Layout tools for reports; the pages do most of what they are for |

### Investigate and report

| Tool | Status | How |
|---|---|---|
| Field Summary, Basic Data Profile | Built (2026-10-25) | **Profile** (§17.6): per column, the count, empties, distinct values, smallest and largest, one row of aggregates the database computes; top values later |
| Frequency Table, Histogram, Scatterplot | Built | Charts on a box's grid: bars over bins, scatter, box plot |
| Pearson Correlation | Built (2026-10-25) | In every box's Profile, under its columns: each pair of its measures' `corr`, a matrix asked of the database as one row of aggregates |
| Charts, Table, Layout, Render, Email | Built | Charts, KPIs, pages, exports and the email draft |

### Developer, apps, macros and the canvas

| Tool | Status | How |
|---|---|---|
| Comment, Tool Container | Built (2026-10-25) | The flow sheet's own state (§3.4): a comment a text tile; a container one with a frame, its boxes moved with it (`layout/flow.ts containerMoves`, Python's `text.place`) |
| Test | Built | Checks as a box (no empty value, no value repeated, a number within bounds, a count of rows within bounds, values one of a list, a text matching a pattern), counted by the database; a mode that stops a run (FL8) |
| Block Until Done | Later | Run order, from runs and schedules (F3) |
| Message | Later | The run's log |
| Base 64 Encoder | Built | `encodeBase64` in a Formula |
| Dynamic Rename, Select | Built (2026-10-25) | Select's window (Preparation, above); Dynamic Rename, a prefix and a suffix given each kept column in sight |
| Dynamic Replace | Later | |
| Interface tools (an analytic app's inputs) | Core | Query parameters: execute takes no parameter values yet (F5) |
| Standard macro | Later | A sub-flow saved as a Pure function with parameters, used as a box; needs a function with parameters as a source (§17.6, item 8) |
| Batch and iterative macros | Out for now | A loop is not one query; it would be orchestration |
| Python, R | Out in v1 | They compute outside the database (FL15) |
| Download, Run Command | Out | Not a database's work |

### Not ours

- **Spatial, predictive, time series, prescriptive, address and demographic tools:** a different product. A function
  the database offers (a spatial or statistical one) can be added as a native when asked for.
- **Connectors to SaaS applications:** out of scope, as the BI and ETL plan's §5 says.
- **The In-Database palette:** every tool here runs in the database, so there is no separate palette for it.

---

## 5. Roadmap: simple first, then built up (2026-10-25)

D19 landed on main (neema2/legend-lite#71), so FL16's wait is over and the canvas can start. The user's three rules
for all of it (2026-10-25):

1. **Reuse DataCube's logic and APIs**, so there is one set of rules, never a second.
2. **The API stays separate from the UI**: every gesture a command, applied by the one function, the same from Python.
3. **Close to Alteryx's experience, features and workflow**, but nicer, more modern and sleeker.

Every milestone ends usable: something a person does from start to finish, saved, shared, and the same from Python.

### M1. A first flow: an Input, five tools, Browse

- **What a person does:** adds a Flow sheet; drops a file onto it, or picks any DataCube source, as an Input; adds
  Filter, Formula, Select, Sort and Summarize after it; clicks any box to see its rows; saves, reopens and shares it.
- **The canvas:** boxes and lines. A tool is dragged from the palette onto the canvas, onto a box's output, or onto a
  line to insert it there. Boxes move, are deleted, and every step undoes and redoes.
- **Settings:** each box's in the pane on the right, made of DataCube's own editors: the filter editor (Filter), the
  column editor (Formula), the shown columns (Select), the sorts (Sort), the grouping and its measures (Summarize).
- **Results:** the selected box's rows below the canvas, as a full DataCube, live: there is no Run button. Only the
  shown box queries; every other box is a document, not a running grid (demand-driven work).
- **Types:** each box says its output columns as it is connected; a box the compiler refuses is red, in its words.
- **Underneath:** the `rows` source (a grid over another grid's rows, §3.1) with live inputs; the `flow` layout kind
  (FL4); a box with no running grid until it is shown.
- **Not yet:** joins, ports, writes.
- **Done when:** a browser harness builds Input, Filter, Formula and Summarize by drag and drop alone, previews each
  box, saves, reopens and shares the flow; Python builds the same flow and reads the same rows.

### M2. Combine: Join, Union and ports

- A tool dropped onto a line, inserted there (moved from M1: a link changed by a command, with Freeze and Unfreeze).

- Join with its L, J and R ports; Union of any number of inputs; Append Fields; Find Replace; Fuzzy Match.
- Ports (FL7): Filter's False, and Unique with its Duplicates.
- Live or frozen inputs, with Freeze and Unfreeze (FL6).
- **Nicer than Alteryx:** join keys suggested with their reasons, and what a join will do counted before it is
  applied (join.ts `suggestedKeys` and join-open.ts `summaryCounts`, both built).
- **Done when:** two inputs are joined, each port is browsed, and an edit upstream reaches the join and every box after
  it.

### M3. Prepare and parse

- Preparation: Sample, Random % Sample, Record ID, Tile, Multi-Row Formula, Running Total, Multi-Field Formula, Data
  Cleansing, Imputation, Date Filter.
- Parse: Text To Columns, RegEx, DateTime, JSON Parse.
- Most are presets of the calculated columns and window columns DataCube has (§4).
- **Done when:** each tool has a harness case over a fixture, its rows checked.

### M4. Reshape

- Cross Tab, with Load values, the cast, its check and the check's mode (FL2, FL3, FL8); Transpose, composed (FL14).
  Built (2026-10-25): Load values (its values pinned), its check (a value its pin lacks, said), Stop Run On Failure.
- The check modes on screen: amber and red boxes, Add to the cast.
- **Done when:** a Cross Tab whose data gains a value is marked in a preview and refused by a run in refuse mode.

### M5. Understand the data

- Profile on any box or port: the count, empties, distinct values and top values (§17.6).
- Record counts on lines after a run (FL7); results-pane gestures that add a box (FL9); a box's Pure and SQL shown;
  charts on any box.

### M6. Output and run

- **Output Data to a table:** create, replace, append and merge by key, on the store writes now on main (FL13's note).
- The Cache box (FL10); the grid's exports from any box.
- **Run:** the whole flow in order, with a run log; check modes enforced (FL8); a dry run (the plan and row counts,
  nothing written).
- **Done when:** a flow writes a table in one transaction, and a failed check writes nothing and says why.

### M7. Big flows

- Comments and containers that collapse, auto-layout, a minimap, search, a command palette, copy and paste of boxes,
  keyboard operation, accessible announcements.
- Import Pure and the Pure box (FL12). *The Pure box built (2026-10-26, its note above); Import Pure not yet.*

### M8. Production (platform work)

- Schedules and run history (F3); parameters and analytic apps (F5); standard macros (a sub-flow as a function with
  parameters); Dynamic Input; FL15's Python box, if asked for.

### What makes it nicer than Alteryx, all the way through

- No Run to see data: every box previews live, computed in the database.
- Types and the compiler's errors live, as boxes are connected and settings typed.
- Every box is a full DataCube: pivot, chart or export at any step.
- Undo for everything, a share link for every flow, the flow as diffable Pure.
- The same flow from Python, by the same commands.
- One design language (`docs/DATACUBE_UI_REVIEW_2026_10_24.md`), dark mode, the keyboard first.

### M1's own steps, each a commit with its tests

1. **The `rows` source and live inputs in the cube document:** read and written; resolved against the page (a cycle
   refused, a missing grid said); its leaves; the order a page opens its grids in.
2. **Its base query** over the host's tables, on the tab and on Python's host (headless), and the L8 source rows.
3. **The `flow` layout kind** and its page commands: a box placed, moved and removed; every reader of a layout taught
   it.
4. **The canvas**, the palette, the settings pane and the results pane.
5. **The five tools' settings.**
6. **Python's methods**, by the names agreed (D6).
7. **The browser harness** and a tour step.

### M1's design, as the code shapes it (2026-10-25)

Read while building step 1, against main at `1e91768d`.

1. **A box is a document until it is shown.** M1 runs only the shown box. So the box before it is usually not an
   open grid, and a box's base cannot be taken from an open grid upstream. A box's base is worked out from its chain's
   first source down, one rule at every step: **an input's rows** (join-open.ts `inputRows`, the rule a join's input
   already followed, now shared) over the base of what that input reads. Python's host already works a base out this
   way, per source (headless host-tables.ts `onHost`, grid.ts `gridBase`), so there the rule is one more arm.
2. **The tab's openers split in two.** Each kind's opener in `datacube/src/app/sources.ts` today both opens a source and
   makes its grid. A box needs the first half alone: the base (its relation, its columns, the planner and plane it is
   read through) with no grid made. The opener becomes "open the base", then "make a grid over it"; every kind keeps
   its one opener.
3. **The page holds a box's document while it is not open.** The page today reads each grid's document off its open
   grid (page-app.ts `#seen`), and keeps the documents of the grids it could not open (`#withUnopened`). A box not
   shown is the second case by design: its document lives in the page's state and is edited by commands, and its grid
   is made when it is shown and let go when it is not.
4. **A link is resolved, never guessed.** Whatever asks what a grid reads (its leaves, its base) asks a source whose
   live inputs are resolved against the page first (live-inputs.ts `resolvedSource`); `leavesOf` refuses an unresolved
   one rather than answering for it. A page whose link names no grid of it, or goes round, is refused when read.
5. **The Join window's copy holds no link.** A grid joined is copied with its links resolved (page-commands.ts
   `joinInputOf`, now live-inputs.ts `gridInput`, the one rule for a grid as an input): a frozen input never changes.

**Built in step 1** (`datacube/src/cube-document.ts` `RowsSource`, `LiveInput`; `datacube/src/live-inputs.ts`): the
source written and read, refused by name when malformed; links resolved against the page, a cycle and a missing grid
refused in words; the order a page opens its grids in (`page-document.ts openingOrder`, now given the page's views);
the keeper's reads (`page-rebase.ts pageReads`); the save and share words. Tested by `datacube/test/live-inputs.test.ts`.

**Built in step 2, Python's host** (`datacube/src/headless/grid.ts`, `host-tables.ts`, `page.ts`; `python/legend_lite/page.py`):
a box's base worked out from its chain's first source (headless `baseOver`: a leaf's, a join's, or its input's rows over
its input's base, through what that base is read through); every base call given its page, so a link is resolved there
(`gridBase`, `gridBaseOn`, `gridOpened`, `cubeLeaves`, the keeper's `pageRebase`), and refused in words without it;
`rowsCube`, a box added after a grid, which Python's `page.grid(input=grid)` asks (FL20). The source kind is in the L8
ledger's reads and adds (`one-api-ledger.test.ts`, dated), with Python's test (`python/tests/test_page.py Flowed`: a
box's rows live, a box after a box and after a grouped one, refusals, saved and loaded, joined as a copy).

**Built in step 2, the tab** (`datacube/src/app/sources.ts`, `join-sources.ts`, `join-open.ts`): a box opened where its
page reads (`rowsWhere`) -- on a page Python serves, by Python, the page handed to it (`opened.json`'s `document` and
`id`); in the tab, over the tab's own tables as a join's are read (`openRows`: each file once, a table read for a grid
on the page taken, never asked for again), or, its chain over a source read elsewhere (a warehouse table, a class, a
saved query), over that first source opened as a grid over it opens, that grid never made (`openedAfterStart`, the
opener's start kept by its maker: point 2 above, without changing any opener). What a grid reads is kept as it opened
(`GridPlace.reads`). Checked in the running app: a page file with a box after its grid (notional over 1,000) opened
with 179,996 of the grid's 200,000 rows. The L12 host register lists the new branches (the `input` ability, its proofs
on both hosts). Not yet: an edit upstream re-reading the boxes after it in the tab (step 4, with the canvas).

**Built in step 3, the `flow` layout kind** (`datacube/src/layout/flow.ts`, `page-document.ts`, `page-commands.ts`,
`page-rebase.ts`, `headless/layout.ts`): a sheet's layout is a board's bands or a flow's boxes (`PageLayout`,
`layoutTiles`), read and written, its rules held (each box once, at whole places, neither below zero; a flow's tile a
grid). Two command kinds: `addFlow` (Create ▸ Flow, or the sheet tabs' + ▸ Flow: 2026-10-26) and `placeBox` (a box let go, its arrow keys). `addGrid` on a flow
puts the box after `near`, the box it reads (the next column on its row, else the first free row there: `placeAfter`).
What only a board takes is refused in words: a chart, a text tile, a stack, an arrangement, a band layout; `moveTile`
goes from board to board or flow to flow. A grid taken off leaves every grid that read it live reading a copy of what it
was (FL6, `withoutTile`). The rebase renames both kinds and keeps a flow told before a box came (`rearranged`). An
unnamed flow is "Flow n" (`sheetLabel`). Tested by `datacube/test/flow-sheet.test.ts`.

**Built in step 4, the canvas, the palette and the results pane** (`datacube/src/layout/flow-board.ts`, `flow-canvas.ts`,
`flow-boxes.ts`, `page/cube-page.ts`, `page/page-app.ts`, `app/datacube-app.ts`): a flow sheet's board is a `FlowBoard`,
its drawing fetched with the first flow (a page with no flow downloads none of it): the palette (Input, Filter, Formula,
Select, Sort, Summarize, by Alteryx's groups and colours), the canvas (boxes on a dot grid, a curved line into each box
from each box it reads), and the results pane below, the shown box's grid, a full DataCube. A box says what it does from
its document alone (`boxSays`: its tool, the steps it takes besides, what it is set to). **A box is a document until it
is shown** (point 3 above): a page opens none of a flow's boxes (`page-document.ts boxCubes`: the tab's Open…, the
keeper's page converged) and writes each box's cube as the page has it; a click (or Enter) shows one, its grid made by
the host over the page as it is now (`PageAppOptions.openBox`: `sources.reopened` with the page), the box shown before
closed with what its grid did kept (`CubePage #closeBox`). A box whose source a step changes while it is shown (a box it
read taken off) is shown again over it. A tool clicked adds a box after the box shown, dropped on a box after that box
(`onFlowTool`: an Input, a grid over a source picked now; any other tool, a grid over that box's rows, read live, over the
tables its grid reads). Moves, removals and renames are page commands, each a step of the page's history; which box is
shown is the screen's. Python: `page.flow()`, `sheet.grid(...)`, `sheet.flow`, `box.place(x, y)` (FL20), tested by
`python/tests/test_page.py Flows`. Tested by `datacube/test/flow-page.test.ts` (real grids on a page of its own), and
checked in the running app: Create ▸ Flow, an Input of the examples, a Filter after it, each box shown, a box dragged.

**Built in step 5, the five tools' settings** (`datacube/src/flow-boxes.ts` `TOOL_EDITORS`, `app.ts` `dock`,
`openStep`, `layout/flow-canvas.ts`): the settings pane beside the canvas (Alteryx's Configuration window) holds the
window of the shown box's grid that sets what it does, docked rather than floating (`CubeAppOptions.dock`: Properties,
the Filter window, a calculated column's editor, one at a time) -- a Filter's filter, a Formula's new calculated column,
a Select's shown columns (Properties ▸ Columns), a Sort's sorts (Properties ▸ Sorts), a Summarize's row groups
(Properties ▸ Row Groups). A tool added opens its window; a box clicked opens its own. With none docked, the pane says
what the box does and offers to set it. A box added as a tool is drawn as that tool, "not set yet", until its grid does
something (the screen's, never the page's). Found on the way: a grid's undo key JSON-stringified its snapshot, and a
grid over another grid's rows (or a join) carries its input's literals as the protocol's exact numbers, so its first
edit threw; the keys are written exactly now (`cube-state.ts`, `toJson`). Checked in the running app, both themes:
Input, Filter (notional over 500,000), Summarize by region after it, each box shown again with its settings.

**Built in step 7, the browser harness and a tour step** (`datacube/demo/verify-flow.mjs`, `//datacube:verify_flow_test`;
`demo/tour.mjs` step 17): Create ▸ Flow; an Input dragged onto the canvas (the examples' trades, 5,000 rows), a Filter
dragged onto it (notional over 500,000 set in the docked Filter window: 2,404 rows), a Formula onto the Filter (`fee`,
in the docked column editor: the same rows, the column in them), a Summarize onto the Formula (region, in Properties'
Row Groups: a row per region); each box shown again, its rows as before, only the shown box's grid made; a box moved by
hand and the move undone; the flow saved, the tab reloaded and the flow opened again from Open… (each box as it was,
none opened until shown, the last box's rows as before); and shared, the link opening the same flow in a fresh tab.
With the Python tests of step 3 (`test_page.py Flows`, `Flowed`), M1's "done when" holds. Found on the way: the column
editor's sections shrank below their content in a short window, the preview's heading over its own builder's buttons
(`app.css` `.dc-xc-main > *`).

**Built after step 7, the rest of M1's list** (`layout/flow-canvas.ts`, `flow-board.ts`, `app/datacube-app.ts`): a file
dropped from the desktop onto the canvas is an Input over it, opened as the picker opens a file it is given; a box
whose source was not opened the last time it was clicked is drawn red, why said on it and in the settings pane, until
it is shown; each box says the columns it hands on (on its tooltip, and in the settings pane), read from its document,
no query run for it. Tested by `flow-page.test.ts` and the harness (a file dropped, an Input over it, its rows).

**Moved to M2, and built there:** a tool dropped onto a line, inserted there (M2 step 2, below).

**Not yet, said:** a chart or a text box of a box's rows (refused, said: they go on a board sheet); a tool added from
the tab on a page Python serves (refused in words: Python adds one, `flow.grid(input=box)`); a grid on a board reading a
box that is shown re-read when that box is edited.

### M2's own steps (2026-10-25)

**Built in step 1, a box's input set** (`datacube/src/page-commands.ts` `setInput`, `cube-document.ts` `inputOn`,
`withInputOn`, `inputSides`, `readCubeInput`, `live-inputs.ts` `relinkedInput`, `page-between.ts` 2c, `page-rebase.ts`):
one page command sets what a grid reads on a side -- another grid's rows' one `input`, a join's `left` or `right` -- to
a grid of the page read live, a copy of what a grid was, or, `input: null`, a copy of what it reads live now (Freeze).
Its query is kept; what its input answers now is read when it opens again. Refused in words: a source with no such side,
the input it reads already, a box frozen twice, a snapped grid, and a link that would read its own rows (the page
reader's rule, live-inputs.ts `liveProblem`). A join's inputs may be live now (`JoinSource.left`, `right`: FL6's "new
joins are live"), resolved against the page wherever they are read (`frozenInput`, the tab's `openedJoin` and the
host's `gridOpened` given the page), the source keeping its links. The page's history covers a flow's lines: the
commands between two pages set each input that differs, so **an undo of a box's removal links the boxes it froze again**
(a swap of two boxes undone freezes each first where setting it at once would read its own rows). Found on the way: a
flow sheet's removal undone made a board (page-between.ts made every sheet by `addSheet`), its boxes' layout then
refused; it makes a flow now. Python: `grid.relink(box)` (a join's `side='left'`/`'right'`), `grid.freeze()`, and
`grid.input` / `grid.inputs`, the grids it reads live (`python/tests/test_page.py Flows`). Tested by
`datacube/test/flow-sheet.test.ts` and `page-commands.test.ts`.

**Built in step 2, a flow's lines** (`layout/flow-canvas.ts`, `flow-board.ts`, `flow-boxes.ts` `boxInputs`, `readersOf`;
`page/cube-page.ts`, `app/datacube-app.ts`): a line drawn by hand from a box's output port onto another box links it
(`Link Box`; a join's left or right by the half it is let go on, its L and R ports above and below); a line clicked is
selected, its × at its middle, and Delete (or the ×) takes it off -- the box it went into then reads a copy of what it
read, drawn frozen (a dashed input, ❄ before what it says; `Freeze Box`); a tool dropped onto a line goes between its two
boxes (`Insert Box`: the box added and the box after it reading it, one step, one undo). The settings pane says what the
shown box reads on each side -- a box of the flow (none that reads it), or the frozen copy, named as the box it copied
-- and sets it there too, the keyboard's way to every line. Tested by the gestures ledger (`a box frozen`, `a box linked
to another`), `flow-page.test.ts` and the harness: a line drawn and undone, a line taken off and linked again from the
settings, a Sort dropped on a line and the insert undone in one step (`//datacube:verify_flow_test`).

**Built in step 3, Join and Union boxes** (`app/join-flow.ts` `flowJoin`, `app/join-sources.ts` `openedInputsOf`,
`ui/join-dialog.ts`, `page-commands.ts` `setJoin`, `flow-boxes.ts`, `headless/join.ts`): Join and Union in the palette.
Dropped on a box (or clicked with one shown), the Join window opens over it -- its Left -- and the flow's other boxes,
one chosen as its Right, each read as it is now (its leaves' tables found or read in as a saved join's are: the boxes'
grids are not open); applied ("Add Join", "Add Union": a Union's window starts at Append), a box after the Left that
reads both live, drawn with its L and R ports and a line into each, saying its join in words (`join-how.ts joinWords`,
moved there from join.ts so a box says it without the join's chunk). An edit upstream reaches it: the Filter's
threshold raised, the Join's rows follow. Set again from its settings ("Open its settings": the window at its own join,
its condition, match, prefix and columns kept; "Apply"): one step, `setJoin` with `setInput` when its Right changed,
undone by one undo (page-between.ts 2c sets a join's how too). Python: `page.join(...)` of two boxes of a flow is live
(`live=None`: live on a flow, a copy on a board, as before), `grid.rejoin(how)`. **A box passes on the columns its
input gains** (`cube-document.ts reopened`): a grid reading another live shows a new column of its input rather than
hiding it as a saved grid over a changed file does -- found when a Join set from "with a match" back to "matching rows
only" hid their columns (Alteryx's *Unknown passes them by default; a Select hides by name). Tested by the harness (a
Join of the Filter and the Input on trade_id, its rows, an edit upstream, set again and undone, a Union of both),
`flow-sheet.test.ts`, `page-commands.test.ts` and Python's `Flows`.

**Built, Append Fields** (2026-10-25; `join-how.ts isEveryPair`, `ui/join-dialog.ts`, `app/join-flow.ts`): Alteryx's
Append Fields -- each row of one input beside every row of another, a small table's values on each row of a big one. It
is a join, matching rows only, on no condition, so it is no new kind of source: the Join window gained the form **Every
pair** (its condition hidden; their columns kept and prefixed as a join's; what it will do said as "Each of my 503 rows
beside each of the 4 rows of theirs: 2,012 rows."), and the palette's Append Fields opens the window there ("Add Append
Fields"). A box whose join is every pair says "each row with every row of theirs" and is drawn as Append Fields
(`flow-boxes.ts joinTool`); set again, the window opens at Every pair. The window's forms are now three across, nine
of them. Python: `page.append_fields(mine, theirs, prefix=..., keep=...)`, `page.join` with no condition. Tested by
`flow-sheet.test.ts` (the box's tool; all of mine on no condition stays a Join), `join.test.ts` (its words), Python's
`test_every_row_beside_every_row_as_append_fields` and the harness (the Filter's rows beside the dropped targets' four:
four times its rows).

**Built, a Union of any number** (2026-10-25; `cube-document.ts` `JoinSource.more`, `join.ts appended`,
`page-gestures.ts takeOffCommands`, `layout/flow-canvas.ts`): Alteryx's Union takes as many inputs as are wired to it, and
so does a Union box now. A join source whose form is an append holds its inputs after its right in `more` (the sides
`more1`, `more2`, ...; a join of two refuses one, as its reader and `setJoin` say). Every rule that walks a join's
inputs walks them too (`joinInputs`: its leaves, the grids it reads live, a source resolved against its page, relinked,
frozen when a box it reads goes), and the database appends them all in one query: the first input's columns in its
order, then each next one's it lacks, every input extended by the columns only the others have, empty of the type the
first to have it gave it. The commands are the ones a join already had: `setInput` on a Union's next side (one past its
last) adds an input, and `setJoin` takes `more` whole (left out, kept). Undo is `pageBetween`'s: a change in the count
is told as `setJoin` with `more`. On the canvas a Union's ports are numbered, one an input, with a **+** below them; a
line let go on a Union fills a side read from a copy, or adds an input. Deleting a line into a Union of more than two
takes that input off and moves the ones after it up (`takeOffCommands`; a Union of two freezes it, as a join does). Its
settings list each input with a ×, and **Add an input…**. A Union of more says "the rows of all 3, columns by name".
Python: `page.union(*grids)`, `grid.relink(g, side='more1')`, `grid.take_off(side)` (headless `pageGesture`
`flow.takeOff`, the canvas's own rule), and `grid.inputs` names every side. Tested by `flow-sheet.test.ts` (added, set
whole, taken off, frozen, undone and redone, read and renamed), `live-inputs.test.ts` (read back; refused on a join of
two), `join-open.test.ts` (four inputs over DuckDB, a table read twice once), Python's `test_a_union_of_more_than_two`
and the harness (a third line drawn onto the Union, its ports, its rows, one taken off and one added from its settings,
undone).

### M3's own steps (2026-10-25)

**Built, the window tools and Cross Tab** (`flow-boxes.ts`, `ui/column-editor.ts` `ColumnEditorStart.window`, `app.ts`
`openStep`): Record ID, Multi-Row, Tile and Running Total are boxes whose one column is a window column, each started as
its tool's in the column editor docked in the settings pane (`rowNumber` named RecordID, `lag` by one named Previous,
`ntile` of four named Tile, a running `sum` named RunningTotal) -- the person picks its order and its column there, the
editor's own window form. A box says the tool that made its column (`boxSays`: a window column by its function; the
columns of one tool together, in their order). Cross Tab (M4's, here because it is the grid's own column pivot) opens
Properties at its Column Labels; a box with a column pivot says "desk across, by region". The palette is grouped under
headings (In, Prepare, Join, Transform). Tested by `flow-sheet.test.ts` and the harness (a Record ID ordered by trade_id,
its column in its rows; a Cross Tab's Properties docked at Column Labels).

**Built, Sample** (`ui/sample-form.ts`, `app.ts` `#openSample`): a box's first or last rows in the order of a column --
the grid's own Top N (`filter.topN`), said as a sample is ("Keep the first 5 rows, in the order of trade_id"; ties kept,
as the Top N keeps them), in a window of its own docked in the settings pane, fetched with the first. A box with a Top N
says "first 5 by trade_id". Tested by the harness (its five rows).

**Built, DateTime** (`ui/date-form.ts`, `app.ts` `#openDate`, `calc-presets.ts` `grainOf`): a date column taken to its
day, week, month, quarter or year -- the grid's own By Day/Week/... preset (`grid-presets.ts`, the one rule Python's
`grid.grain` asks), in a window of its own ("Date Period") docked in the settings pane, fetched with the first. A box
says what it takes to what ("trade_date by month"), read off its column's expression by its shape -- the preset's, or
the same expression typed into a Formula -- never by its name. Tested by `flow-sheet.test.ts` and the harness (its
column in its rows).

**Built, the text tools** (`text-tools.ts`, `ui/text-tool-form.ts`, `app.ts` `#openTextTool`): Text To Columns (a text
column split on a delimiter into its first N parts, `<name>_1`..`<name>_N`), RegEx (a pattern over a text column: Match,
a true/false column; Replace, each match replaced; Extract, its first match -- `first`, so a value with no match is
empty: `toOne` made DuckDB refuse the "FX" desk that has none, found by the harness) and Data Cleansing (text columns
tidied, a `<column>_clean` each: empties blank, punctuation out, repeated spaces one, the ends trimmed, the case
changed). Each is the calculated columns DataCube has, added in one step as the column editor's Add adds one, in a
window docked in the settings pane; Parse is a palette group of its own. A box says what it does from its columns'
expressions by their shape (`textToolOf`: "book split on ' '", "/^[A-Z][a-z]+/ of desk", "region cleansed") -- the same
expression typed into a Formula draws the same. Python asks the same rule (`text-tool-columns.ts` `textToolCommands`,
through headless `gridPreset`): `grid.split(column, delimiter, parts)`, `grid.regex(column, pattern, mode)`,
`grid.cleanse(columns, ...)`. A part a value has not got, and a pattern it does not match, are empty (`splitPart`'s
`String[0..1]`, `first` of the matches). Tested by `flow-sheet.test.ts` (the expressions, the words, the refusals),
Python's `test_text_tools_as_a_flow_box_adds_them` (the values DuckDB computes) and the harness (each tool set in its
window, its columns planned and run on DuckDB).

**Built, Unique** (`unique.ts`, `ui/unique-form.ts`, `app.ts` `#openUnique`): Alteryx's Unique, with its two outputs --
a row of each key, its first in an order the window picks ("Keep the first row of each, by notional, largest first"),
said as what a grid already does: a row number restarting for each key (`rowNumber` partitioned by the key's columns),
a filter keeping the rows numbered 1, and the number's column hidden. Its U output is its rows; its D output -- every row
after a key's first -- is that filter's False port (FL7), so nothing new carries it: the canvas draws a Unique's two
ports as U and D where a Filter's are T and F. A box says "by region", read back off its query (`uniqueOf`: the number,
its filter alone, its column hidden -- not a Record ID, a Filter and a Select). Python: `grid.unique(keys, by=...)`
(headless `gridPreset`, the same rule), its duplicates `other.relink(box, port='false')`.

**Fixed with it, a box's rows are the columns it shows** (`live-inputs.ts` `gridInput`, `cube-document.ts`
`JoinInput.omit`, `join-open.ts` `inputRows`): a box reading another read every column of its query, the ones that box
hid too -- so a Select box left out nothing for the box after it, and Unique's row number would have reached the next
box. A live input of a flow's box now leaves out the columns its box hides (`omit`, kept in a frozen copy), selected
away where its rows are read (the tab's and Python's, one rule); a grid of a board still gives every column, as a Join
of two grids always has. Tested by `flow-sheet.test.ts` (Unique's commands, its words, its refusals),
`live-inputs.test.ts` (omit on a flow, none on a board, its copy read and refused), Python's
`test_unique_and_its_duplicates` (each region's largest, its duplicates, the columns the next box reads for a Unique and
a Select) and the harness (a Unique of the Input by region: 3 rows, U and D, a Browse of its Duplicates: 5000 - 3).

**Built, Imputation** (`impute.ts`, `ui/impute-form.ts`, `app.ts` `#openImpute`): Alteryx's Imputation -- number
columns' empty values filled, a `<column>_filled` each: with a value (a whole number for a whole-number column), or with
the column's own average, smallest or largest across every row -- a window column the database computes (`average`,
`min`, `max` over every row, its frame the whole of them), hidden -- then `coalesce($x.c, ...)`. A box says
"notional's empties → its average", read back off its columns (`imputedOf`: the window column said with the column it
fills, never a Formula's or a Select's). Python: `grid.impute(columns, fill='average' | 'smallest' | 'largest' | n)`
(headless `gridPreset`, the same rule). Tested by `flow-sheet.test.ts` (its expressions, its words, its refusals),
Python's `test_empties_imputed_as_a_flow_box_fills_them` (the values DuckDB fills: an average of 200 in two empties, a
0) and the harness (an Imputation of the Filter's notional, its column in its rows, its average hidden).

**Built, the DateTime's other two ways** (`date-tools.ts`, `ui/date-form.ts`): the DateTime window's way picked first --
a date to its period (as before), a date's PART (its year, its month's number, its day of the month, its week of the
year, its hour: the compiler's `year`, `monthNumber`, `dayOfMonth`, `weekOfYear`, `hour`), or TEXT READ AS A DATE by a
format in the engine's tokens (`yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy-MM-dd HH:mm:ss`, or typed: legend-lite's
`convertDateFormat` / `convertDateTimeFormat`, by their paths, which the compiler lowers to the database's strptime).
A value the format cannot read refuses the box's rows in the database's words -- strptime's, not a try's: an empty for
unreadable text would need a native of its own (found by Python's test). Boxes say "year of trade_date", "booked read as
dd/MM/yyyy". Python: `grid.date_part(column, part)`, `grid.parse_date(column, format)`. Tested by `flow-sheet.test.ts`,
Python's `test_dates_read_and_taken_apart_as_a_flow_box_does` (DuckDB's dates and months; the refusal) and the harness
(a year of the trade date).

**Built, Multi-Field Formula** (`multi-field.ts`, `multi-field-columns.ts`, `ui/multi-field-form.ts`, `app.ts`
`#openMultiField`): Alteryx's Multi-Field Formula -- one expression over several columns, written once over `$v`, each
column's value in turn (Alteryx's `[_CurrentField_]`; `$x` is the row, for its other columns: `$v / $x.total`), parsed
by the compiler as `v|...` and made a calculated column per column (`<column><suffix>`, `_new` unless named), `$v` made
`$x.<column>` in each, its kind its column's. The window shows the columns by kind (Text, Numbers, Dates, All, as
Alteryx's field types) and starts the expression with an example for the kind; what the compiler refuses -- `trim` of a
number -- it says in its words ("argument 1: expected String, got Float") and adds nothing. A box says "over region,
desk", read back off its columns' shapes (`multiFieldOf`: two or more side by side whose expressions are one with a
different column in each place; a lone one is a Formula's, and a shape a tool of its own makes is that tool's -- a trim
over two columns is Data Cleansing's). The commands are a chunk of their own, fetched with the first, so a page without
one loads only the reading. Python: `grid.multi_field(columns, expression, suffix='_new')` (headless `gridPreset`, the
expression parsed by the same compiler). Tested by `flow-sheet.test.ts` (its columns, its reading, its refusals),
Python's `test_one_formula_over_several_columns_as_a_flow_box_does` (DuckDB's trimmed and upper-cased text, doubled
numbers plus another column; the compiler's refusal) and the harness (a refused one, then one over region and desk).

**Built, Sample's other ways** (`row-sample.ts`, `ui/sample-form.ts`): Alteryx's Sample whole -- its first or last N
rows, all but its first N (Skip), or one row of every N, in the order of a column, of all its rows or of each group of
some columns (Group by). The first or last N of all the rows stay the grid's Top N (rows that tie kept); every other
way is said as Unique's is: a row number in the order (`rowNumber`, partitioned by the groups, falling for the last
ones), a filter on it (`<= n`, `> n`), and for one of every N its remainder (`mod($x.SampleRow, n) == 1`), the numbers
hidden. `sampleCommands` goes from whatever sample the grid has to the one asked (its Top N or its row sample taken off
first, the new numbers under the same names), and Keep every row takes it off. A box says "first 1 of each region by
trade_id", "all but the first 10 by trade_id", "one of every 100 by trade_id", read back off its query (`rowSampleOf`).
Python: `grid.sample(mode, n, by, groups=...)`, `grid.sample(None)`. Tested by `flow-sheet.test.ts` (its columns, its
reading, a sample changed twice), Python's `test_rows_sampled_as_a_flow_box_samples_them` (DuckDB's numbering: the last
of each desk, all but the first two, one of every two of each desk) and the harness (a Sample set again to the first of
each region, 3 rows, then one of every 100 of the Filter's 503, 6).

**Built, Transpose** (M4; `transpose.ts`, `ui/transpose-form.ts`, `app.ts` `#openTranspose`): Alteryx's Transpose --
columns made rows, each row once for each data column, a Name column saying which and a Value column holding its
value, the key columns beside them and every other column left out. No new kind of source: an exploded calculated
column over the list of the columns' names (`lateral(x|['notional', 'pnl']->flatten(~Name))`, DataCube's Explode), a
calculated column choosing each row's value by its name (`if($x.Name == 'notional', |$x.notional, |$x.pnl)`), and every
column but the keys hidden, so the box after it reads the keys, Name and Value; the database repeats the rows and
chooses the values. One Value holds one kind: columns of numbers and text together are refused in words (make them
text first, with a Formula); whole numbers with fractions come out as numbers, a measure. Its window lists each column
as a key, made rows, or left out (the number columns made rows and the first other column a key, to start). A box says
"notional, pnl to rows", read back off its two columns' shapes (`transposeOf`). Python: `grid.transpose(columns,
keys=..., name='Name', value='Value')`. Tested by `flow-sheet.test.ts` (its columns, its reading, its refusals),
Python's `test_columns_made_rows_as_a_flow_box_transposes_them` (DuckDB's rows, a desk's quarters) and the harness
(the Filter's 503 rows twice, its key, Name and Value alone). Composed of select-per-column unions (FL14) it would
need a source kind of its own; an explode needs none.

**Built, Text To Columns into rows** (`text-tools.ts`, `text-tool-columns.ts`, `ui/text-tool-form.ts`): the split's other
way, Alteryx's Split to rows -- each part of a text its own row: one column, `split($x.c->toOne(), ',')` exploded
(DataCube's Explode), each row once for each part. The window's "into N columns" gained "into rows"; a box says "book
split on ' ' to rows". Python: `grid.split(column, delimiter, name=..., rows=True)`. Tested by `flow-sheet.test.ts`,
Python's text tools test (a, b, c three rows, d one) and the harness (the Filter's books split on a space: more rows
than the Filter's).

**Built, Select's window and its renames** (`select-columns.ts`, `ui/select-form.ts`, `box-columns.ts`): Alteryx's
Select laid out as Alteryx lays it -- a row a column: kept or not, its name, its type, its new name; a find box, All and
None. Kept and left out are the grid's own Hide; a rename is the column's display name, which a box of a flow now hands
on as the relation's `rename` (`JoinInput.rename`, written with a frozen copy; `join-open.ts inputRows` renames after
it selects), so the box after it reads `area` where it read `region`. What a box hands on is one rule now
(`box-columns.ts`: hidden and a Summarize's ungrouped dimensions left out, renames applied), read alike by live inputs,
the settings' columns out, the Profile and the Output (its CSV's head). Two columns handed on by one name are refused
in words. A box says "region → area; 1 of 12 columns left out". Python: `grid.select(keep, rename={...})`. Tested by
`flow-sheet.test.ts` (its command, its words, its refusals), `live-inputs.test.ts` (a rename carried, written, read),
Python's `test_a_select_keeps_and_renames_for_what_reads_it` and the harness (a Select of the Filter, the Sort after it
reading `area`, no `region`, no `book`).

**Built, a Select's column order** (`select-columns.ts`, `ui/select-form.ts`, `box-columns.ts` `boxOrder`, `cube-document.ts`
`JoinInput.order`, `join-open.ts inputRows`; 2026-10-25): Alteryx's fields moved up and down -- each row of the Select
window has ↑ and ↓ (Alt+Up, Alt+Down from anywhere on its row), and the box hands its columns on in that order: its
grid's own column order (`columnOrder`, what its grid shows in), which a box of a flow hands on as the relation's
`select` in that order (`JoinInput.order`, written with a copy). A retyped column's conversion takes its column's place
-- found when a retype sent its column to the end. A box says "columns reordered" when its columns were moved, its
conversions in their places aside. Python: `grid.select(keep, ..., order=[...])`. Tested by `flow-sheet.test.ts`,
`live-inputs.test.ts`, `join-open.test.ts` (DuckDB: the columns selected in that order), Python's Select test and the
harness (desk moved above region: the box after it reads desk before area, quantity where it was).

**Built, Summarize's aggregates of two columns and Select's Dynamic Rename** (2026-10-25): a Summarize offers every
aggregate its column is offered -- a weighted average, a distinct count, a correlation, max by and min by among them --
an aggregate of two columns with its second chosen beside it (a weighted average's weight: the grid's own Aggregation
and its second column, `snapshot.ts SECOND_COLUMN`; one that must name it refused until it does). Python:
`grid.summarize(group, {'px': ('wavg', 'qty')})`. A Select renames many at once, Alteryx's Dynamic Rename: a prefix and
a suffix given to each kept column in sight ("Rename"), or each its own name again ("Own names"). Tested by
`flow-sheet.test.ts`, Python's Summarize test (a desk's prices weighted by their quantities: 17.5) and the harness.

**Built, Count Records** (`count-records.ts`, 2026-10-25): Alteryx's Count Records -- how many rows a box's input has, one
row, one column. No new kind of query: a column of one value its rows are grouped by (hidden, so the box after it reads
none of it), a column of 1 for each row summed, every other column hidden; the database counts. A tool with no settings:
made, it counts (its step opened as the box is made: `StepEditor` `countRecords`, no window), saying "its rows counted:
Count", read back off its columns (`countRecordsOf`). Python: `grid.count_records(name='Count')`. Tested by
`flow-sheet.test.ts`, Python's `test_rows_counted_as_count_records` and the harness (the Filter's rows counted: one row,
its count).

**Built, Select Records** (`row-sample.ts`, `ui/sample-form.ts`, 2026-10-25): Alteryx's Select Records -- the rows at some
places, written as Alteryx writes them (`1-10, 20, 30+`, `-5` the first five) -- one more of the Sample's ways ("the
rows at"): its row number (in the order of a column, of each group when it has some) and a filter of its ranges, or-ed
(one row an equal, from one on a `>=`, between two a `>=` and a `<=`), read back off it (`rowSampleOf`) and said "rows
1-3, 10 by trade_id". The database numbers and keeps the rows. Python: `grid.sample('records', by=..., ranges='1-10,
20')`. Tested by `flow-sheet.test.ts`, Python's Sample test and the harness (the first three and the tenth: four rows).

**Built, Random % Sample** (`row-sample.ts`, `ui/sample-form.ts`, 2026-10-25): Alteryx's Random % Sample, repeatable --
about a percent of the rows, the same rows each time it runs for the same seed, others for another (`random()` answers
again each time a box is read, so it waits; the database's `hashCode` does not). Each row's number in an order of every
column that sorts (the same number each run), its chance `mod(abs(hashCode([n, seed])), 100)` (hidden), kept under the
percent: one more of the Sample's ways ("a random"), read back off it and said "10% at random, seed 7". The database
numbers, hashes and keeps the rows. Of all the rows only: the same percent of each group is the same percent of all.
Python: `grid.sample('random', 25, seed=7)`. Tested by `flow-sheet.test.ts`, Python's Sample test (the same rows for the
same seed) and the harness (about a tenth of the Filter's rows).

**Built, Create Samples** (`layout/flow-canvas.ts` `#outsOf`, 2026-10-25): Alteryx's Create Samples splits its rows
into an estimation sample and the rest. A Sample a filter makes -- at random by its seed, one of every N, at some places,
the first N of each group by row number -- now draws two outputs, its rows (**S**) and the rest (**R**): the rest is its
False (FL7, the port a Filter has), its filter turned round, so the same seed gives the same split every time and the two
are every row, none in both. A Sample of the first N by an order is a Top N, with no filter: one output, and a box
linked to its rest is refused in words. Each part a box after it; a three-way split, the rest sampled again. The ports
are drawn by a class of their own (`dc-flow-two`: a Filter's, a Unique's and a Sample's alike). Python:
`rest.relink(sampled, port='false')`. Tested by `flow-sheet.test.ts` (the rest's query: the filter turned round, its
columns as they are; a Top N refused), Python's `test_a_sample_hands_on_the_rest` (the rows and the rest are every row,
set again and followed) and the harness (a Random % Sample's S and R, a Browse of the rest: the Filter's rows less the
tenth).

**Built, Date Filter** (`date-ranges.ts` `datesBetweenConditions`, `datesKept`; `ui/date-filter-form.ts`; `app.ts`
`#openDateFilter`; 2026-10-25): Alteryx's Date Filter as a tool of its own, in Prepare after Filter. Its window asks a
date column and a range: one of the eighteen the header menu's Date Range offers (today, this month, the last 30 days,
year to date, ... -- relative to when the query runs, the database computing today), or the days between two, both kept.
Both are one grid gesture, `filter.dates` (new: the two days checked, the first not after the last): its filter made
those dates alone, in place of every condition it had -- the box's one step. A box whose filter is those two conditions
alone is drawn as a Date Filter ("trade_date: Last 30 Days", "trade_date from 2024-01-01 to 2024-06-30", read back by
`page-filters.ts datesKept`, beside a Filter's words: a module of its own read by the canvas's chunk would be a startup
file of its own), and has a Filter's T and F. Python: `grid.date_filter(column, range)` or `grid.date_filter(column,
start=..., end=...)`, the same gesture. Tested by `grid-gestures.test.ts` (the days' two conditions, every condition it
had replaced, so already, a day that is none, a first after the last, both or neither), `flow-sheet.test.ts` (each range
and the days read back, the box's words, a filter of any other shape a Filter's), Python's date range test (between two
days, a range, refused) and the harness (a Date Filter over the Filter's trades: 2023's, its T and F, set again to the
last 12 months).

**Built, JSON Parse** (`flow-boxes.ts` `jsonFieldOf`, `app.ts` `#openJsonParse`, 2026-10-25): Alteryx's JSON Parse as a
tool of its own, in Parse. It opens the column editor on its box's JSON column's fields, docked (`ColumnEditorStart.json`,
the grid's own picker: a value, a nested object or array as a JSON column, an array's count or values, each element a
row) -- each a calculated column the compiler checks; its box's first JSON column, and refused in words when it has
none. A box whose calculated column reads a field of a JSON column (`$x.c->get('key')...`, anywhere in its body) is
drawn as a JSON Parse, saying "tier from customer". Python makes the same column by `grid.calculate`. Tested by
`flow-sheet.test.ts` (a nested field read back, its words; a text's own `get` and a formula with none, not one).

**Built, Test** (FL8's checks, a first slice; `checks.ts`, `ui/test-form.ts`, `app.ts` `#openTest`,
`layout/flow-canvas.ts` `#testOf`; 2026-10-25): Alteryx's Test tool, as a box (In and Out, after Output). Its window
lists its checks, a row each -- a column with no empty value, a column no value of which repeats, a number column within
bounds (its smallest, its largest, or both; an empty value fails no bound) -- added and taken off. Its query is Count
Records' way: a column of one value its rows are grouped by, and for each check a column of 1 for each row that fails it
(a repeat found by a row number among its value's rows), summed, beside a count of its rows; every column it read
hidden. So its one row is each check's failing rows, counted by the database -- `region_empty`, `trade_id_repeated`,
`notional_outside`, `Rows`. Read back by rebuilding each check by the rule that made it and comparing it whole
(`checksOf`), never by its name alone; set again, its own columns are taken off first in the same step. Once its rows
land for what it is now (shown, or the flow run), the canvas reads its row (`firstRowOf`, the box's grid's) and marks
the box: amber, saying "1 of 2 checks fail" with each failing check and its rows in its title, or its words green when
every check passes; the result is kept for what its rows are made of (its chain), so it shows after another box is
shown, and a run's log says it ("2 tests failing"). A Test's mode and its count of rows (below) are
built. Python: `grid.test([{'kind': 'notEmpty', 'column': ...}, ...])`, its row by `to_pandas`.
Tested by `flow-sheet.test.ts` (the counts made and read back whole, one bound, set again, which fail, refusals, read
from JSON), Python's `test_rows_checked_as_a_test_box_checks_them` (over DuckDB: an empty region, a repeated id, a
notional outside its bounds, each 1; set again, passing) and the harness (a Test over the Filter: its trade ids unique,
its notionals to 950,000 -- one of two failing, marked; set again, passing).

**Built, a Test that stops a run** (FL8's refuse; `layout/flow.ts` `FlowBox.stops`, `setStops`; `page-commands.ts`
`setStops`; `page/cube-page.ts` `#runFlow`; 2026-10-25): a Test box's settings have **Stop a run when a check fails**.
Its mode is the box's, on the flow (`stops: true`; absent, it warns), set by its own page command, undone as any is. A
run now shows and counts every box first, then writes its Outputs -- and writes none when a Test that stops a run
fails: each Output's line says "not written: Orders checked failed", the log's title "stopped by Orders checked: nothing
written", and the Test is drawn red, as a refusal is (amber is a warning's). The canvas tells the board each Test's
result as it reads its row (`tested`, kept for its chain), which is what the run asks. Python: `grid.stops_run()`
(False: warn), `grid.failing` (the headless `gridFailing`: its row read by the same rule), and `flow.run(folder)`
writes no file and is refused, its failing checks said, when a stopping Test fails. Also fixed on the way: Tidy Up
(`arranged`) rebuilt each box from its place alone, dropping an Output's file; each box is now kept as it was but for
its place. Tested by `flow-sheet.test.ts` (set, read, written, renamed, undone, refused; Tidy keeping an Output and a
stop), Python's `test_a_test_that_stops_a_run_writes_no_output` (refused, nothing written; set to warn, written; saved
and loaded) and the harness (the example's Checks sheet: run, 2024's orders written; its Test set to stop a run and run
again: nothing written, said, the Test red).

**Built, the Pure box** (FL12, its "Pure box"; `pure-box.ts`, `ui/pure-form.ts`, `app.ts` `#openPure`, `page-commands.ts`
`setPure`, headless `gridPure`; 2026-10-26): a box whose rows are a function of its input's, written in Pure over
`$input` -- `$input->filter(x|$x.region == 'EMEA')->extend(~fee:x|$x.notional * 0.001)`, a `groupBy`, a `limit` --
whatever no other tool says. The function is its source's (`RowsSource.pure`: a lambda of one parameter, its body one
expression that reads it), so it is the page's to set, by a page command (`setPure`, its undo the page's, `page-between.ts`
and `page-rebase.ts` carrying it); its text is never kept, only the compiler's print of it shown when it is set again.
Every host opens it by one rule (`boxRows`): its input's relation put where the function reads its parameter (a lambda of
its own naming a parameter the same reads its own: pure-protocol's new top-down `rewrite`), the whole typed by the
compiler as the box's columns -- the tab's two chains (`join-open.ts`, `app/join-sources.ts`) and the headless host's
(`headless/grid.ts`). Before it is set its window and Python ask one rule (`pureFrom`): parsed, its shape judged, its
rows typed as a relation -- a value (`$input->count()`, which the compiler types as one column, `value`) refused in
words, as is a function that never reads its input (a Text Input's work) and one of two parameters. Its window docks in
the settings pane: the text, its steps a click away (filter, extend, groupBy, select, sort, distinct, limit, rename, each
put in where the cursor is, to be finished there), its input's columns, and Take Its Pure Off. A box says the functions
its rows go through ("filter → limit"), and a box after it reads its rows as any other box's. Python:
`grid.pure("$input->...")`, `grid.pure(None)`. Not yet: Import Pure (a lambda turned into boxes), a Pure box of two
inputs. Tested by `flow-sheet.test.ts` (its text both ways, the substitution and its inverse, shadowing, its shape's
refusals, its words, `pureFrom`'s steps and refusals, the command applied, refused, read, renamed, written and read back,
the page between both ways), Python's `test_a_pure_box_makes_its_rows_a_function_of_its_inputs` (a filter and an extend,
a groupBy's columns as the compiler types them, taken off, each refusal) and the harness (a step put in, a value refused,
set, its rows the Filter's EMEA trades ten at most, set again as printed, taken off, undone).

**Built, Generate Rows** (Alteryx's Generate Rows; `generate-rows.ts`, `ui/generate-rows-form.ts`, `app.ts`
`#openGenerateRows`; 2026-10-26): the tool the table had as needing a design: core has no relation that makes rows from a
rule, but the grid already explodes a calculated column that yields a list (`unnest`: `lateral(x|<list>->flatten(~name))`,
Text To Columns' rows), and the platform has `range`. So each row is repeated once for each whole number from one to
another -- each a number given or one of the row's whole-number columns, by a step (`range(from, to + 1, step)`) -- or
once for each day between two of its date columns (`range(0, dateDiff(from, to, DAYS) + 1)`, each day the first
`adjust`ed by it, that number hidden); both ends kept, a row whose bound is empty repeated no time (`if(isEmpty(...),
|[], |range(...))`). Its new column is the number or the day. Numbers given the wrong way round, a step below 1 and more
than 100,000 repeats a row are refused in words. Read back off its columns (rebuilt and compared whole: `generatedOf`), a
box says "N from 1 to 3" or "day: each day from opened to closed"; set again, its own columns replaced. Python:
`grid.generate_rows('k', 1, 'n')`, `grid.generate_rows('day', days=('opened', 'closed'))`. Tested by
`flow-sheet.test.ts` (numbers given and a column's, its step, days, read back, said, set again, refusals, its JSON),
Python's `test_rows_generated_as_a_generate_rows_box_makes_them` (over DuckDB: 1 to n per row; by a step; four and three
days, a leap year's; refusals) and the harness (the Filter's rows, each three times).

**Built, Binning** (Alteryx's Multi-Field Binning, its equal intervals; `binning.ts`, `ui/binning-form.ts`, `app.ts`
`#openBinning`; 2026-10-26): a tool in Prepare whose window picks number columns and a count of bins. Each column is put
into that many bins of equal width over its own range by the grid's own bins preset (`grid-presets.ts`, `calc-presets.ts
binColumns`, what the header menu's Bins and Python's `grid.bins` make): the database finds the column's smallest and
largest across every row and places each value, the largest in the last bin. Its box hands on a column of each value's
bin, its lower edge (`notional_bins`), beside the column; the range and the upper edge it reads are hidden. Read back by
rebuilding the preset's columns and comparing them whole (`binsOf`), a box says "notional in 5 bins". Equal records
are the Tile's (`ntile`). Python: `grid.binning(['notional'], 5)`. Tested by `flow-sheet.test.ts` (made, read back,
said, a name taken moving on, refusals, its JSON), Python's `test_number_columns_binned_as_a_binning_box_bins_them`
(over DuckDB: two bins of 87.25 from 75.5, the largest in the last; refusals) and the harness (the Filter's notionals
into five bins: the bin beside its column, its helpers hidden).

**Built, New Box After** (FL9; `ui/menu.ts` `newBoxItems`, `app.ts` `onStepAfter`, `page/cube-page.ts` `#stepAfter`,
`app/datacube-app.ts` flowTool's `step`; 2026-10-25): a box's rows in the results pane are its full grid, and its own
menus still edit it; its right-click menu now has **New Box After** too -- the cell's value filtered by ("Filter: region
= 'EMEA'", or `!=`; an empty value's "is null" and "is not null"), its column sorted ascending or descending, or grouped
by ("Summarize: by region"). Each is the grid's own gesture (`filter.add`, `sort.asc`, `pivot.vertical`), made in a new
box after the shown one -- the box a Filter, Sort or Summarize, added as the palette's tool is added after a box, then
the gesture made on its grid as its one step -- so the shown box keeps doing its one thing (FL5). A grid on a board has
no such entry. Python adds the box and makes the gesture on it, as ever (`flow.grid(input=box)`, then the gesture's
method). Tested by `menu-more.test.ts` (its entries, each the grid's gesture marked to be made after; an empty value's;
none off a column, none on a board) and the harness (a cell of the Filter's rows right-clicked: a Filter box after it,
its region's rows, the Filter as it was).

**Built, a Dry Run** (M6; `page/cube-page.ts` `#runFlow(dry)`, `layout/flow-canvas.ts`, `layout/flow-board.ts`
`RunLog.dry`; 2026-10-25): a flow with an Output has **Dry Run** beside Run (Ctrl+Shift+Enter, and in the command
palette). It runs every box as Run does -- each counted as its rows land, each Test and fixed Cross Tab judged, a run
stopped by a stopping one that fails -- and writes no Output: each Output's line says "would write orders of
2024.csv", or "would not be written: Orders checked failed", and the log's title "Dry run: ran 9 boxes in 2.1 s ...;
nothing written". Python: `flow.dry_run()`, `{'rows': {box: n}, 'would_write': [...], 'stopped': [...]}` -- each box
counted as the canvas counts it (its view's rows, the row cap's total when it cut them), the stopping boxes judged by
the rule `flow.run` refuses by (`_stopped`). Tested by Python's `test_a_dry_run_counts_every_box_and_writes_nothing`
(over DuckDB: each box's rows, the file it would write; a stopping Test failing, none) and the harness (the example's
Checks sheet: a dry run stopped by its Test, its Output not written, said why; set to warn, a dry run that would write
2024's orders and writes nothing).

**Built, Custom Filter** (FL12's "filter by a Pure expression", §17.6 item 6; `custom-filter.ts`,
`ui/custom-filter-form.ts`, `app.ts` `#openCustomFilter`, `cube.ts` `typed`; 2026-10-25): Alteryx's Filter, its Custom
filter, as a tool of its own in Prepare. Its window takes a condition written in Pure over `$x`, the row -- each of its
input's columns a click away, put in where the cursor is -- which the compiler parses and types when OK is pressed: one
that is no Boolean is refused in words ("a condition is true or false, a Boolean: this one is Float"), as is the
compiler's own refusal. Its query is what a grid already does: a calculated column of the condition (`CustomFilter`,
hidden, so the box after it never reads it) and the grid's filter keeping the rows it is true for, in place of every
condition it had; set again, its column edited. So its False port is a filter's (T and F), the rows the condition is not
true for. Read back off its column and its filter (`customFilterOf`), a box says the columns its condition reads ("a
condition on region"); set again, its window shows the condition as the compiler prints it. The type is the compiler's
before anything is set: the draft typed as a refresh types it (`CubeController.typed`, step 0's `typeColumns`), nothing
run. Python: `grid.custom_filter("$x.region == 'EMEA'")` (headless `gridPreset`, the same parse and typing). Tested by
`flow-sheet.test.ts` (its column and filter, read back, its words, set again, refusals, its text and JSON),
Python's `test_a_custom_filter_keeps_the_rows_its_condition_is_true_for` (over DuckDB: its rows, its False the rest, set
again, a Float refused, an unknown column refused) and the harness (a Custom Filter over the Filter: a Float refused in
the window, a column put in by a click, EMEA's trades, its T and F; set again, its condition printed).

**Built, a Cross Tab's columns fixed** (FL2; `grid-edits.ts` `pinPivot`, `grid-commands.ts` `pinPivot`,
`grid-gestures.ts` `pivot.pin`, `query.ts` `crossTabRows`, `app.ts` `pinPivotValues`, `layout/flow-canvas.ts` `#paintPin`;
2026-10-25): a pivoted box's columns are its values, found as it runs, so a box after it could not read them (refused in
words). Its settings now say so, with **Load values**: its values as its rows hold them now, asked of the database by
the pivot's own values query, pinned -- the grid's long-standing `pivotValues`, now set by a grid command of its own
(`pinPivot`, undone as any is: `grid-between.ts`; a pivot set on another column takes the pin off) and its gesture
(`pivot.pin`: each value typed as its column is, an empty value refused -- it names no column). Pinned, its rows at
their finest level are a relation with fixed columns (`classLambda` over `crossTabRows`): a row per group, its keys, a
column for each value named by it (`EMEA`; `EMEA_notional` when it spreads several measures; its total `Total`), the
measures it keeps out of the pivot beside them. So a box after it reads them -- in the flow, its Profile, its Output and
Create a Class alike -- where FL2's design asked for a `cast`: the grid writes a pivot as one aggregate per value
(`levelRelation`), so a pin types its columns without one. Its strip lists them, with **Load values again** and
**Unfix**. A value its rows gain that the pin lacks has no column of its own until loaded again, and is said (below).
Python: `grid.load_pivot_values()` (the headless `gridPivotValues`, the screen's question),
`grid.pin_pivot(values)`, `grid.pin_pivot(None)`. Tested by `grid-gestures.test.ts` (typed, the same again nothing,
taken off, kept by its own pivot and dropped by another, refusals, read from JSON), `create-class.test.ts` (its rows: no
order, renamed, selected; a value naming a key refused), the random walks (`grid-random.ts`: pins among the edits, every
pair of states reached by commands), Python's `test_a_cross_tab_with_its_columns_fixed_is_read_by_the_box_after_it`
(over DuckDB: Load values, a box after it reading `AMER_notional`... ; pinned to one, fewer; unpinned, refused) and the
harness (a Cross Tab pivoted on region and grouped by desk by the header menu, Load values, a Browse after it reading
its regions' columns).

**Built, a Cross Tab's missing values said** (FL3, FL8's mode; `query.ts` `unpinnedValuesLambda`, `cross-tab.ts`,
`page/cube-page.ts` `#askLacking`, `layout/flow-board.ts` `lacking`; 2026-10-25): M4's done-when. A Cross Tab whose
columns are fixed asks the database, as its rows land (shown, or the flow run), for the values its rows hold that its
pin lacks -- its pivot's own values query with a filter keeping the empty value and those `not in` the pin (SQL's
`not in` leaves an empty value out, so it is asked for by name) -- once for each thing it is (its chain). Some found,
its box is drawn as a failing Test is -- amber, saying "1 value has no column", its title and its settings naming them
("region APAC has no column of its own: Load values again to add it") -- and the run's log counts it apart from the
Tests ("1 Cross Tab missing values"). Its settings offer a Test's mode (**Stop a run when a value has no column**, the
same `setStops`): set, a run waits for its answer before any Output is written and writes none, drawn red. Load
values again adds the value: every value a column, passing. Python: `grid.failing` answers for a Cross Tab whose columns
are fixed by the same question (headless `gridFailing`: `{check: 'every region has a column', rows: <values>, says}`), so
`grid.stops_run()` and `flow.run(folder)` refuse a run as the canvas does. The example flow's Checks sheet has one:
revenue by channel across the regions, fixed to AMER and EMEA, its APAC said. Tested by Python's
`test_a_cross_tab_with_its_columns_fixed_is_read_by_the_box_after_it` (its AMER said when pinned to EMEA; a stopping
one refusing a run, nothing written; loaded again, written), `examples.test.ts` (its fixed values, its words) and the
harness (the Checks sheet run: its box amber, said, counted in the log apart from the Test; its settings naming APAC;
Load values again: passing, all three its columns) and the tour.

**Built, a Test's list of values and pattern** (`checks.ts` kinds `oneOf` and `matches`; 2026-10-26): dbt's accepted
values and a pattern check, counted as every check is. "region is one of EMEA, APAC, AMER" counts the rows whose value
the list lacks (`<column>_unexpected`: `not(in(...))`, a text's or a number's values, a whole-number column's whole);
"code matches ^[A-Z]{3}$" the rows whose text the pattern does not match (`<column>_unmatched`: `regexpLike`, as the
RegEx tool's Match reads a pattern). An empty value fails neither. Its window takes the list (a comma between) or the
pattern as text; read back off its column, rebuilt and compared whole. Python: `{'kind': 'oneOf', 'column': c,
'values': [...]}`, `{'kind': 'matches', 'column': c, 'pattern': p}`. Tested by `flow-sheet.test.ts` (made and read
back, said, refusals, its JSON), Python's `test_rows_checked_as_a_test_box_checks_them` (over DuckDB: APAC failing ^E)
and the harness (the Test's second check made a list of the three regions: both passing, said, read back).

**Built, a Test's count of rows** (FL8's row-count check; `checks.ts` kind `rows`; 2026-10-25): a check of no column --
its input's count of rows from a smallest to a largest (whole numbers, either left out, a smallest 1 at least: every
count is 0 at least). It is checked after grouping: a column over the group's `Rows` (`Rows_outside`, 1 when the count
is outside its bounds, the database comparing), read back from the grid's calculated columns after grouping, rebuilt and
compared whole as every check is. Its failing is said with the rows its input has -- "rows from 1 to 500: it has 900
rows", "at least 1 row: it has 0 rows" -- in the box's title, the run's log and Python's `grid.failing` (`says`, the one
set of words for the canvas and Python; `flow.run`'s refusal says them too). A Test of an input of no rows is grouped
into no row: every check's count is 0, and a count of rows fails its smallest, read off the row's absence (the canvas's
`firstRowOf` now gives a reader of no values for a grid of no rows, rather than none). Its window offers "rows: a count
within bounds", its column picker hidden, once a Test. Python: `grid.test([{'kind': 'rows', 'min': 1, 'max': 500}])`.
Tested by `flow-sheet.test.ts` (made after grouping, read back, said, set again, failing with and without a row,
refusals, read from JSON), Python's `test_rows_checked_as_a_test_box_checks_them` (over DuckDB: 4 rows failing at least
5; a Test of no rows failing at least 1) and the harness (the Test's second check made a count of rows at most 1: its
trades failing it, their count said; read back as a count of rows).

**The example flow's Reshape sheet** (`examples/prep-flow.ts`): a second sheet showing the tools that reshape rows over the
same orders -- the two biggest orders of each region (a Sample: last 2 of each region by revenue), their columns chosen
and revenue renamed sales (a Select), their money made rows (a Transpose: sales, cost, profit to rows, beside each
order's id and region) and summed by measure (a Summarize) -- each box made by its tool's own rule, applied as the
screen applies it (`grid-commands.ts applyGridCommands`), so the example cannot drift from what the tools make. Tested by
`examples.test.ts` (each box's tool and words, each read live), the harness (run: 6, 6, 18, 3; the Transpose reading the
Select's `sales`) and the tour (its picture). A third sheet, **Match** (2026-10-25), shows what a join leaves and finds:
the big orders whose country code finds no country -- the Join's L -- and how many (a Count Records), the countries no
big order is of (its R), and every order's country named as the countries file names it (a Find Replace: a Join of the
orders and the countries by code, their name alone brought, replacing the order's own) -- boxes on one sheet reading a
box on another, live. Tested by `examples.test.ts`, the harness (run: the orders left, counted in one row; every one of
the 6,000 orders named) and the tour (its picture). A page of two flow sheets keeps both canvases on the page, so the harness
and the tour count and click what is shown.

**Built, Fuzzy Match** (M2; `join-how.ts`, `join.ts`, `ui/join-dialog.ts`): Alteryx's Fuzzy Match as a join -- a row
of a join's condition gained the comparison `~`, similar text: both a value and at most `within` edits apart (2 when
not said), by the database's `levenshteinDistance` (`$a.who->toOne()->levenshteinDistance($b.name->toOne()) <= 1`). The
Join window offers it beside =, ≠, <, …, as "≈ similar", with its edits apart beside it; the palette's Fuzzy Match opens
the window at matching rows only on a similar row ("Add Fuzzy Match"), and a box whose join has one is drawn and set
again as a Fuzzy Match ("matching rows only, on region ≈ region (within 1)"). A number in a saved page's join is read
as the page holds one (an ExactNumber: found by Python's test). Python: `page.fuzzy_match(mine, theirs, ('who', 'name'),
within=1)`, or a join row `{'op': '~', 'within': 1}`. Tested by `join.test.ts` (its condition, its reading, its words,
its refusals), `flow-sheet.test.ts` (the box's tool), Python's `test_alike_text_matched_as_fuzzy_match` (Jon Smith with
John Smith, Ana Diaz with Anna Diaz, none at no edits) and the harness (the Filter's regions with the targets').

**Built, the minimap** (M7; `layout/flow-canvas.ts`): the toolbar's Map shows the whole flow small in the canvas's
corner -- each box a mark, the one shown in its colour, each container its frame -- and the part in view framed; a
press or a drag on it moves the view there. Asked for, never unasked (shown always, it covered the boxes in its corner:
found by the harness, a Filter's port under it), and its button off while every box is in view. Tested by the harness
(asked for, a press at its top and at its bottom moving the view, asked away).

**Built, the command palette** (M7; `layout/flow-canvas.ts` `#allCommands`, 2026-10-25): Ctrl+K on the canvas opens a
field over a list of every command there is now, found as it is typed -- each toolbar button a person could press (Run,
Tidy Up, Undo and Redo saying what they would do, the zoom, Fit, the Map, the keys), each tool of the palette (added
after the box shown, as its click adds it; a locked page lists none) and each box (shown, and scrolled into view). Each
word typed is looked for in a command's name and its words; a name that starts with what is typed comes first, then
one with a word that does, then the rest, actions before tools before boxes. Arrows move, Enter runs, a click runs,
Escape or a click elsewhere closes. No command of its own: each is its button's, its palette entry's or a click's
gesture, so it adds nothing to undo or to Python. Listed among the keys (?). Tested by the harness (zoomed in and back,
a Browse added after the box shown and undone, a box shown by its name, Escape).

**Built, what changed said** (M7, accessible announcements; `layout/flow-canvas.ts` `#announce`, 2026-10-25): after each
time the canvas draws, it compares its boxes -- each one's name and what it reads -- with what it told last, and says
what changed in the board's live region (polite): a box added ("Added Sort, reading Filter"), taken off, renamed, or
reading other boxes now ("Union reads Filter and Input and Input now", "Formula reads a copy of Filter now"), three
changes at most and how many more. It says whatever made the change -- a gesture, an undo, a redo, Python's edit -- since
it reads the boxes, not the gestures; nothing the first time a flow is drawn, nor when only how it looks changed. Tested
by the harness (a third line onto the Union, said).

**Run from the keys** (`layout/flow-canvas.ts`): Ctrl+Enter (⌘+Enter) anywhere on the canvas runs the flow, as its
button does, while no run is going; the keys panel and the button's title say so. Tested by the harness (a run from a
box's keys writes its Output).

**Built, in place** (`box-columns.ts inPlaceCommand`, `inPlaceOf`): Alteryx's "change existing fields" for Data
Cleansing, Multi-Field Formula and Imputation -- each made column handed on by the name of the column it is made of,
that column hidden: the box after a Data Cleansing of `region` reads `region`, cleansed. No new mechanism: the hiding
and the display names a Select uses, handed on as the relation's `rename`. Each window has "In place" (on to start for
Data Cleansing and Imputation, as Alteryx's are; off for Multi-Field); a box says "region cleansed in place", its
hidden columns and renames none of a Select's. Python: `in_place=True` on `grid.cleanse`, `grid.impute` and
`grid.multi_field` (off unless asked, so a script's `_clean` columns stay). Tested by `flow-sheet.test.ts` (an
Imputation in place, its words), Python's `test_tools_in_place_hand_their_columns_on_by_their_own_names` (three tools
in place, the box after them reading who, n and m changed) and the harness (a Data Cleansing and an Imputation in place).

**Built, correlations in the Profile** (M5; `profile.ts correlationLambda`, `layout/flow-canvas.ts`): Alteryx's Pearson
Correlation without a box of its own -- a box's Profile shows, under its columns, how its measures move together: each
pair's correlation, from -1 to 1, the database's `corr` over the rows mapped a pair each (as a grid's paired measure
is), asked as one more row of aggregates when the Profile is shown, its first ten measures at most. A matrix, each
column with itself 1, empty where one does not vary, each cell tinted by its sign and strength (blue together, red
against). Measures only: an id's or a dimension's correlation says nothing, and a column's kind is the grid's to set.
Tested by `flow-sheet.test.ts` (its question, its matrix read back) and the harness (the trades' measures, each with
itself 1, every pair between -1 and 1; a picture).

**Built, Select's retype** (`select-columns.ts`, `ui/select-form.ts`): the Select window's Type is a choice wherever a
column can be made something else -- text a whole number, a fraction, a decimal, true or false, a date (`parseInteger`,
`parseFloat`, `parseDecimal`, `parseBoolean`, `parseDate`); a number text, a whole number (`round`), a fraction
(`toFloat`) or a decimal; a date or true-or-false text (`toString`). A column retyped is a calculated column of its
conversion (`$x.code->toOne()->parseInteger()`, named `code__Integer`), its column hidden and the conversion given the
column's name, so the box after it reads `code`, a whole number -- the renames' own machinery, nothing new below it.
Read back by its name and its shape together (`retypesOf`: a person's own formula is never taken for one); set again,
a retype dropped is taken off. A box says "region → area; quantity as String; 1 of 12 columns left out". Python:
`grid.select(keep, rename=..., retype={'code': 'Integer'})`. Tested by `flow-sheet.test.ts` (its command, its reading,
its words, its refusal of a number made true or false), Python's Select test (text made numbers, the box after it
reading integers) and the harness (quantity made text, the box after it profiling it as String).

**A Summarize gives its keys and its aggregates** (`snapshot.ts ungroupedDimensions`): a box after a grouped box read
a row per group with every dimension beside it, each its "unique value" (empty where a group's rows differ) -- the
example flow hid them by hand. Now, as Alteryx's Summarize, a grouped flow box hands on its group keys and its
aggregates alone: the dimensions it is not grouped by are left out of what the box after it reads (`live-inputs.ts
boxOmits`), of its settings' columns out, its Profile and its Output -- one rule for each. A grid of a board, and a
pivoted grid, are as before. Tested by `live-inputs.test.ts` and Python's `test_a_summarize_gives_its_keys_and_aggregates`.

**Built, Summarize's window** (`summarize.ts`, `ui/summarize-form.ts`, 2026-10-25): Alteryx's Summarize laid out as
Alteryx lays it -- a row a column: its name, its type, its action (Group by, an aggregate of one column its type is
offered, or left out), a find box -- in place of Properties' Row Groups as the Summarize box's settings. Its actions are
what a grid already does, one step of commands (`summarizeCommands`): its grouping taken off, each column made what it is
used as (a group a dimension, an aggregate a measure: Column Properties' rule), its grouping put on, each aggregate set,
every column it does not use hidden. What it is now is read back off the grid (`grid-edits.ts summaryOf`, beside the
offers it reads): a fresh box shows each measure aggregated as its column says until told otherwise. Refused in words:
no group (a summary of every row is its Profile), a column used twice, an aggregate of two columns, a calculated column
asked to be what its editor did not make it -- and an aggregate its column is not offered, by Column Properties' own
rule as the commands are applied. A box says "by region"; the columns it leaves out are its own, no Select beside it
(`flow-boxes.ts`). The aggregates' words are Column Properties' (`ui/panel-column.ts AGGREGATES`), handed to the window
by its opener. Found on the way: a lazily fetched module that imports some of a startup chunk's modules splits that
chunk in two (here, the rule importing the grid's edits: two more startup files, +1,362 bytes); the rule reads the
snapshot alone, and the startup grew 296 bytes. Python:
`grid.summarize(group, aggregate={'notional': 'average'})`. Tested by `flow-sheet.test.ts` (its commands, read back,
set again, its refusals, its words), Python's `test_rows_summarised_as_a_flow_box_summarises_them` and the harness and
the tour (region grouped by in the docked window: a row per region).

Fixed with it: what opening a box left out (a column no longer in its input, a calculated column that read one) was
said on the page's status line, where it stayed under every box shown after it; it is said on the box's own status bar
now (its grid's note, put away with it), as a Join's notes are. The harness holds the page's line clear.

Still to come in M3: a date written as text by a format. (A random sample came with a seed: Random % Sample, below.)

**Built, an example flow** (`examples/prep-flow.ts`, in Open…'s examples as "A data flow"): a flow to open rather than
one to build first -- the sales sample's orders and its countries in an Inputs container; the orders over $1,000, joined
live to their countries by ISO code and given a margin, in a Prepare one; a summary by continent (the continent and its
sums alone shown, so its Output writes them) and its Output writing "revenue by continent.csv", in a Report one; the top
order of each country kept by a Unique; and a comment saying where to start -- laid out three columns wide, so the whole
of it is in sight beside the settings. Tested by `examples.test.ts` (each box the tool it is, each read live, the
containers holding their boxes, the Output's file) and the harness (opened from Open…, run: 6,000 orders, 4,778 over
$1,000, a row a continent and one for the orders with no country, 10 countries' top orders; its Output's file written).
Fixed with it: a note a flow's box left on the status line (columns gone from its input) outlived its page; a page
put on screen now starts with none of it.

**Built, the example's Checks sheet** (`examples/prep-flow.ts`, 2026-10-25): a fourth sheet shows the tools added since --
the orders taken apart by region (three Filters) and put together again (a Union of three, `JoinSource.more`), 2024's
kept (a Date Filter between two days), and, beside it, every order checked (a Test: its id unique, its country never
empty, its discount at most 15%). Run, every order comes back (6,000), 2024's about half, and the Test's one row says its discount
check fails, marked on its box and in the run's log ("1 test failing"). Tested by `examples.test.ts` (each box's tool,
the Union's three inputs read live, the words of the Union, the Date Filter and the Test) and the harness (the sheet
run: its counts, the failing check said).

### M7's first steps (2026-10-25)

**Built, the canvas's own tools** (`layout/flow.ts` `arranged`, `page-gestures.ts` `tidyCommands` and the `flow.tidy`
gesture, `layout/flow-canvas.ts`, `page/cube-page.ts`, `page/page-app.ts`): **Tidy Up**, at the canvas's corner, places
every box a column after the furthest box it reads and, down a column, level with the boxes it reads, else in the order
it was down the canvas -- one step, nothing when it is tidy already; Python's `flow.tidy()` asks the same rule (headless
`pageGesture`). **Duplicate** (Ctrl+D on a box, or its menu): a copy of its cube, reading what it reads, after the box it
reads first -- a document until shown. **A box's menu** (a right-click): Show its rows, Open its settings, Rename,
Duplicate, Freeze each live input, Remove -- each the same as its gesture elsewhere. **The palette's find field**: its
tools narrowed to the ones whose name or words hold what is typed, Enter adding the first after the box shown. Tested
by `flow-sheet.test.ts` (the rule), Python's `Flows` (`flow.tidy`) and the harness (a box pushed out of place and tidied,
the Formula duplicated from its menu, the palette narrowed to Join).

**Built, comments** (`page-document.ts`, `page-commands.ts` `addText`, `page-gestures.ts` `tidyCommands`,
`layout/flow-board.ts` `addNote`, `layout/flow-canvas.ts` `#makeNote`, `page/cube-page.ts` `#addComment`): a text tile on
a flow is a comment -- Alteryx's Comment tool, under Notes in the palette: dropped on the canvas it lands where it is let
go, its editor open there (the board's own text editor: Markdown, OK or Ctrl+Enter); drawn as a note card, moved by its
head, its words edited by a double-click, taken off by its × or Delete. Tidy Up and Run leave comments as they are. M1's
rule that a flow holds grids alone is a flow of grids and comments now (a chart still goes on a board). Python:
`flow.text(...)` adds one, `note.place(x, y)` moves it. Tested by `flow-sheet.test.ts`, Python's `Flows` and the
harness (a comment dropped on the canvas, its words typed, a text tile of the page).

**Built, zoom and a palette that fits** (`layout/flow-canvas.ts`): −, the level (back to its own size), + and Fit at the
canvas's corner, and Ctrl+wheel around the pointer -- the screen's, never the page's; a box dragged at a zoom moves by
the pointer's way across the window in the canvas's own units (110 pixels at 110%, 100 of the canvas). The palette lays
its tools two by two under their headings, so the whole of it fits a window's height. Tested by the harness.

**Built, Undo and Redo, copy and paste** (`layout/flow-canvas.ts` `#paintHistory`, `#copied`, `page/cube-page.ts`
`#copyBox`, `#pasteBox`): the page's own Undo and Redo on the canvas's toolbar, each titled with the step it would undo or
redo ("Undo Paste Box") and off while there is none -- the page's history, the same as Ctrl+Z's. Ctrl+C on a box keeps
its cube (reading what it reads), on a note its words; Ctrl+X cuts it (its removal, its own step, its readers frozen as
any removal freezes them); Ctrl+V on the canvas pastes it where the pointer is, else after the box it reads -- one step,
Paste Box (Paste Comment), on any flow of the page. A copy is the page's alone: a box reads grids of its own page, so a
paste on another page would read nothing (a page's whole flow carried between pages is M7's "flows from other pages").
Tested by the harness (a Formula copied and pasted at the pointer, reading what it reads; undone and redone by the
buttons).

**Built, many boxes at once** (`layout/flow-canvas.ts` `#picked`, `#startBand`, `#group`; `page/cube-page.ts`
`#copyBoxes`, `#pasteBox`, `#removeTiles`): boxes picked together -- a click picks a box, Shift (or Ctrl) and a click
adds one or lets it go, a band dragged from the empty canvas picks the boxes it touches, Ctrl+A every box, Escape none --
drawn with a dashed ring beside the shown box's solid one; the screen's, never the page's. A drag of one moves them all
(none past the canvas's edge, so their places keep their shape), as do the arrow keys -- one step, Move Boxes; Delete
takes them all off, one step (Remove 2 Boxes), each as its × does (a box reading one reading a copy from then on: FL6);
Ctrl+C copies them, in the order they read each other, and Ctrl+V pastes them where the pointer is, placed as they were
placed together -- a pasted box reading another of them reads its pasted copy, any other what it read -- one step (Paste 2
Boxes), the pasted ones picked. Tested by the harness (two boxes moved as one, copied and pasted with their link between
them, taken off together, Ctrl+A and Escape, a band round the Input).

**Built, containers** (`layout/flow.ts` `Frame`, `framedBy`; `page-commands.ts` `placeBox`'s `frame`;
`layout/flow-canvas.ts` `#paintContainer`, `#startContainer`, `#startFrameSize`, `#wrapPicked`; `page/cube-page.ts`
`#addContainer`): Alteryx's Tool Container, as a comment with a frame -- a flow box's place may carry a frame (its size,
folded or not), a text tile's only, at least 200 by 100; `placeBox` sets it (null takes it off, a comment again; left
out, it keeps the one it has). The boxes in a container are never written: they are the boxes whose whole place is
inside its frame, below its head (`framedBy`). Drawn behind the lines and the boxes -- its inside the canvas's own (a band
starts there, a tool is let go there) -- its head its words' first line, how many boxes it holds, its fold and its ×;
dragged by its head with the boxes in it (one step, Move Container), sized by its corner (Size Container), folded to its
head (Fold Container: its boxes out of sight, a line from outside drawn to its head, one inside it not drawn) and
unfolded; double-click its head to name it. Made from the palette's Container (under Notes), or by Ctrl+G round the
boxes picked. Python: `note.place(x, y, width=..., height=..., folded=...)`, `note.unframe()`. Tested by
`flow-sheet.test.ts` (the frame kept, folded, taken off, refused on a grid and too small, read again, the boxes in it),
Python's `test_a_container_frames_boxes` and the harness (Ctrl+G round two boxes, moved with them, folded, unfolded).

**Built, a line's words and the canvas's keys** (`layout/flow-canvas.ts` `#lineWords`, `KEYS`): the pointer resting on
a line says what goes along it, as Alteryx's connection says its metadata -- from which box (its port, a Filter's False or
a Unique's Duplicates) into which, its rows when they were counted, and its columns as the box's document says them; the
toolbar's ? opens what the canvas's keys and gestures do. Tested by the harness.

**Built, Text Input** (`ui/text-input-dialog.ts`, `app/datacube-app.ts` `flowTool`): Alteryx's Text Input, under In in
the palette -- dropped on the canvas, a window of a small table: its columns named in its head row, a value a cell, Enter
down a row (a new one at the last), + for a column and a row, a column's × and a row's number to take them off; a block
of cells pasted from a sheet (tabs between values, a line a row) fills the table from the cell it is pasted in, growing
it to hold the block. Add Input writes it as a CSV file (`tableCsv`: a value quoted only where it must be) and adds an
Input over that file where the tool was let go -- one step, Add Text Input -- the same as a file dropped on the canvas,
so its types are the database's reading of its values and the page saves and shares it as it does any file. Tested by
`flow-sheet.test.ts` (its CSV, its refusals, a pasted block's cells) and the harness (a block pasted into its head row,
an Input of three rows). Still to come: setting it again from the box's settings (today a new Text Input replaces it).

### M5 and M6, first steps (2026-10-25)

**Built, record counts and Run** (`layout/flow-board.ts` `counted`, `count`, `layout/flow-canvas.ts`, `page/cube-page.ts`
`#runFlow`): a box's rows are counted as its grid's view lands (a flat grid's all of them, its total when the row cap
cut it; a grouped one's groups) and drawn beside its output, under the lines out of it, as Alteryx counts a connection
(FL7: after a run, never kept live) -- kept only while the box's chain is as it was then (its source and query, each
live input the chain of the box it reads): an edit up the chain drops the counts after it until they run again. **Run**,
at the canvas's corner beside Tidy Up, shows each box in turn, left to right, each counted, then the box shown before
shown again ("Running 3 of 9…" meanwhile). The screen's: no step of the page's. Tested by `flow-page.test.ts` (a count
dropped by an edit up its chain) and the harness (Run: each box counted -- the Input's 5,000, the Filter's, the Union's
both). **Its log** (`flow-board.ts` `RunLog`): when Run is done, a panel at the canvas's corner says how many boxes it
ran and how long it took, then each box -- its rows, its time, or why it was refused, red -- each a button that shows
it; closed by its ×, until the next run. Tested by the harness (a line a box). Still to come: Run on a page Python
serves counting by Python's engine without showing each box.

**Built, Profile** (`profile.ts`, `app.ts` `profile`, `layout/flow-canvas.ts` `#paintProfile`): Alteryx's Browse
profile (its Field Summary) -- the results pane's Rows and Profile, the profile a row a column of the shown box's rows:
its type, how many rows have a value (a bar of it, its empties said on it), its different values, its smallest and its
largest (none for a Boolean or JSON). Asked of the database when it is shown, never before: the box's columns typed
first (none of its rows, `limit(0)`), then one row of aggregates over the rows the box after it reads (`classLambda`,
its hidden columns left out) -- `count`, `distinct` then `count`, `min`, `max`, each by the database. Asked once for
what its rows are made of (the box and every box it reads: the board's `chain`), again when that changes; the grid kept
in place, out of sight, so its rows are not asked again when Rows comes back. A pivoted box is refused in words (its
columns are its values). Tested by `flow-sheet.test.ts` (its question, its reading) and the harness (the Input's
profile: 5,000 rows, region 3 values from AMER to EMEA, notional's smallest and largest; its rows again).

**Built, a box's Chart** (`layout/flow-canvas.ts` `#paintChart`, `layout/flow-board.ts` `chartOf`, `page/cube-page.ts`;
2026-10-25): the results pane's third view, beside Rows and Profile -- the shown box's rows drawn by its grid's own
chart panel (`ui/chart-panel.ts`, a chart tile's: a chart of what it groups by as DataCube starts one, its kind, columns
and look in its Options, beside the pane's title). Its panel is the page's to make (`chartOf`), so the canvas's chunk
fetches no chart code and ECharts comes with the first chart, as on a board. Mounted once for what its rows are made of
(the board's `chain`), again when that changes, taken down when Rows or Profile is chosen; the grid kept in place, out of
sight, as the Profile keeps it. A mark clicked filters nothing: a box's query is its step. Tested by the harness (a
Summarize's regions drawn, its Options beside the title; its rows again, the chart taken down).

**Built, a box's Code** (M5 and FL12's "each box's Pure is shown"; `layout/flow-canvas.ts` `#paintCode`,
`layout/flow-board.ts` `codeOf`, `viewed`; `app.ts` `code`; 2026-10-25): the results pane's fourth view -- the query the
shown box's grid ran last, as the compiler prints it, and the SQL the database ran, each with its Copy: what the app's
Generated Pure & SQL window shows for a grid, for any box of a flow. A box over others' rows reads them in its own
query, so its Pure is the whole chain to it, one expression. Asked again each time its grid lands a view (`viewed`, the
board's count of them: a step changed upstream moves its query), nothing run for it; refused in words before its rows
are first read. Python's counterpart of its Pure is `grid.relation` (its rows at their finest level, as Pure). Tested by
the harness (a Summarize's Pure, by region over notional, and its SQL grouped; Copy said; its rows again).

**Built, Output** (`layout/flow.ts` `Output`, `setOutput`; `page-commands.ts` `setOutput`; `page-gestures.ts`
`outputCommand`; `layout/flow-canvas.ts` `#paintOutput`; `page/cube-page.ts` `#writeBox`; `app.ts` `writeOutput`):
Alteryx's Output Data, the flow's end -- a box whose place on the flow says it writes its rows to a CSV file, by its name
(no folder: where a file lands is its host's -- the browser's downloads, Python's folder). The palette's Output (under
In and Out) dropped on a box is a box after it that writes "<what it reads> out.csv", in the step that adds it; its
settings name the file again (Set Output), Write now writes it, Write no file makes it a box that writes none. Writing
is every row the box gives -- the columns it shows, the rows the box after it reads -- streamed from the database a chunk
at a time, each chunk written as CSV (export.ts `toCsv`, the grid's own raw export) and let go; Run writes each Output
after counting it, its log saying so. A new command, `setOutput`, shipped whole: its reader, its rebase, its round trip,
the ledgers (a person's typed name passed through; the palette's default name a page gesture's), and Python --
`grid.output('big trades.csv')`, then `flow.run(folder)` writes each Output's rows there and gives back the files. Tested
by `page-commands.test.ts`, Python's `test_outputs_written_when_the_flow_runs` (the file read back, a hidden column left
out, the refusals, saved and loaded) and the harness (the palette's Output, named again, Write now's file read back: its
head row and its 503 rows; Run writes it too, its log saying it wrote it).

**Built, a Filter's False** (FL7; `cube-document.ts` `LiveInput.port`, `live-inputs.ts` `gridInput`'s port,
`layout/flow-canvas.ts`): a live input may name a box's port -- `{grid, port: 'false'}`, its False: the same query with
its filter turned round, the rows of its input its condition does not hold for (a row its condition is unknown for, a
null compared, is in neither, as SQL's NOT says). Resolved, frozen (a removal's copy of the port, not the rows), relinked
and diffed by the same rules as any input; refused in words for a box with no filter. A Filter box draws its True above
its False (T and F, as Alteryx's anchors); a line drawn from F links the box let go on to the False, and the settings'
Input offers each Filter's True and False. **A line let go on the canvas, away from every box, makes a Browse of the
rows it came out with, where it was let go** -- and a tool dropped on the canvas lands where it is let go too (a
`placeBox` in the same step). Python: `grid.relink(box, port='false')`. Tested by `flow-sheet.test.ts`, `live-inputs`,
Python's `Flows` and the harness (the Filter's F drawn onto the canvas: a Browse of the 4,497 rows it leaves out, made
where it was let go).

**Built, a Join's L and R** (§3.3; `cube-document.ts` `JoinInput.unmatched`, `join-how.ts` `unmatchedOn`,
`live-inputs.ts` `gridInput`, `join-open.ts`, `layout/flow-canvas.ts`; 2026-10-25): a live input may name a Join's `left`
or `right` port -- the rows of its Left (or Right) its condition pairs with no row of the other, Alteryx's L and R. No
new kind of source: resolved, the port is that input itself (its own rows, its columns as it hands them on) with its
rows matched against the other's (`unmatched: [{by, on}]`), each an anti-join the database runs over one model of both
(`oneModel`'s side; the host's `rowsOpenedOn`), on the join's own condition -- turned round for its Right (each
row's columns swapped, its comparison mirrored: `<` is `>`). The join's own query is its J's and reaches neither. The
leaves of a port are both inputs' (`leavesOf`), so a chain read elsewhere than the page's own tables refuses it as a
join refuses its inputs. Refused in words: a grid that is no join, a Union (it keeps every row of both), an as-of join,
a join's Pure condition (it names their columns as the join brings them out; an anti-join by their own: a semi- or
anti-join's Pure condition is turned round by naming its two rows the other way); and a join set again so that a box
reading its L or R could not (`setJoin` holds the page to the reader's rule, `liveProblem`, as `setInput` does). A Join
box (a Fuzzy Match's too) draws its L above its J above its R on its right edge; a line from L or R links what it is
let go on, the settings' Input offers each Join's Joined, Left unmatched and Right unmatched, and a line's words name
the port and its side's columns. Found on the way: an Append Fields and a Fuzzy Match box drew one input port where
their two lines came in (the CSS knew a Join's and a Union's two alone); the Join group's tools draw L and R now.
Python: `grid.relink(join, port='left' | 'right')`. Tested by `flow-sheet.test.ts` (resolved, frozen, written and read
back, the refusals, `unmatchedOn`), `join-open.test.ts` (DuckDB: trades with no customer, the customer with no trade),
Python's `test_a_join_box_hands_on_the_rows_each_side_pairs_with_none` and the harness (the Join's R drawn onto the
canvas: a Browse of the trades the Filter left out; set to its L in the settings: none).

**Built, Find Replace** (`find-replace.ts`, `ui/find-replace-form.ts`, `app/join-flow.ts`, 2026-10-25): Alteryx's Find
Replace in its whole-field form -- each row's value of a column found among another input's, replaced by theirs where
found. No new kind of source: a Join box -- the palette's Find Replace opens the Join window at All of mine ("Add Find
Replace"), the key a column of mine equal to one of theirs -- whose column is then handed on replaced, set in its own
window docked as the box is made (Replace each `region` found with its `name`): a calculated column,
`coalesce($x.name, $x.region)`, handed on in place of the column (`box-columns.ts`), theirs hidden. A row not found
keeps its own. The database joins and chooses. A box says "region replaced by name", read back off its column
(`findReplaceOf`) -- a Join replacing nothing is a Join; set again, its replacing before is taken off first. Python:
`page.find_replace(mine, theirs, column, find, replace)` (a left join bringing `replace` alone, then the replacing).
Tested by `flow-sheet.test.ts`, Python's `test_a_column_found_among_another_grid_and_replaced_as_find_replace` and the
harness (a file of region names dropped, the Filter's regions found and replaced). Still to come: finding within a
field (any part, its start) and case left out of the comparison -- each a comparison a join row does not have yet.

**Still open in M2:** a Union of more than two inputs; a Join's L and R on a Pure condition (its
their-names turned back to their own); and the board's own Join… making live joins too (FL6's "new joins are live": the
flow's are; the board's still copies, as Python's `page.join` of two board grids). Built since: a Unique's Duplicates,
Append Fields, Fuzzy Match, a Join's L and R, Find Replace.


---

## 6. Still open

1. **Whether legend-pure has an `unpivot`.** Nothing in this repository shows one, and no legend-pure checkout was in
   the session that wrote this. If it has one, Transpose uses it instead of being composed (FL14).
2. **Python's method names** for flows, put to the user before they are built (D6), at M1's step 6.
3. **An Output box in the tab:** the tab plans with its WebAssembly planner and runs on its own DuckDB; whether its
   runner takes a write plan's `Update` and `Sequence` nodes is to be read before M6. *Read (2026-10-25): no, and
   neither does Python's.* A write is Pure (`->write(#>{db::DB.T}#)`, `update`, `delete`, `merge` by key; a replace is a
   `delete` then a `write`), into a table a Database declares and that exists; planned, it is an execution plan --
   a `Sequence` of `Update` steps, `Check`s and a `TextResult` (`core/.../ExecutionPlan.java`, `PlanMaker.program`) --
   run in one transaction by Java's `PlanRunner` alone. The query path both hosts plan through is the single-SQL one:
   `Plan` is `{sql, columns}` (`engine-client/src/relation-type.ts`), the tab's runner and Python's each run one
   `plan.sql`, and the lowering refuses a store write in a query's plan (`Lowerer.java`, `TypedWrite`). So an Output to
   a table needs, in each host, the execution plan from its planner (a wasm and a native export of `PlanJson`) and a
   runner of its steps (Python's: a port of `PlanRunner.sequence`; the tab's: one in TypeScript, its connection
   binding no parameters today), over a model that declares the target beside the sources. A host operation
   (`CREATE OR REPLACE TABLE ... AS` the box's SQL, as Snap copies) would write one today, but outside Pure: not the
   way, by the execution tenet. A table the tab writes lives in its in-memory DuckDB and goes when the tab does.

---

## 7. The options not taken (2026-10-24)

Kept so a later reader knows why. Sizes: S days, M a week, L a few weeks, XL a month or more.

| Decision | Not taken | Why not |
|---|---|---|
| FL4 | A whole-page mode, like Ad Hoc | Dashboard grids and prep boxes in one picture; one-tool grids as tiles on dashboard sheets |
| FL4 | A separate flow document | A second open, save and share path and a second Python surface, against FL1 (L) |
| FL5 | Step records, the grid only previewing | A second model of a query beside the grid's: the duplicates the One API program removed (L-XL, and upkeep) |
| FL5 | Grids that absorb neighbouring tools | Boxes merge and split without being asked |
| FL6 | Live inputs everywhere, joins included | Saved joins change shape, and older documents are not read (the user, 2026-10-17) |
| FL6 | Live in flows, copies for the grid's Join… | Two kinds of input people must tell apart, with no choice between them |
| FL7 | One output per box | Unfamiliar to Alteryx users; settings repeated |
| FL7 | Counts kept live | One count query per line per refresh |
| FL8 | One mode per flow | Cannot be strict on one check and lenient on another |
| FL8 | Warn by default | A table can be written short, said only in a log |
| FL8 | Follow mode now | The SQL that runs differs from the plan reviewed; two queries a run (M); later |
| FL9 | A gesture edits the selected box | A box stops doing one thing |
| FL10 | Refusal alone | Alteryx users expect a way through |
| FL10 | An automatic copy of the smaller side | Data moved without anyone seeing it (M-L) |
| FL10 | DuckDB attaching the other database | DuckDB doing the other database's work; credentials (L) |
| FL11 | The cache saved with the page, or in the warehouse | Stale data and governed rows in a browser (M); the warehouse needs F2's grants (L) |
| FL12 | Pure read-only, no import | No escape hatch for what the palette cannot say |
| FL12 | Two-way Pure now | Any Pure turned back into boxes, layout kept across edits (XL); later, if wanted |
| FL13 | F1 before the first version | Holds the first version on core work (L) |
| FL13 | Files only, through DuckDB's `COPY` | No tables |
| FL14 | A new `unpivot` native | Not shown in Pure's relation API: a platform invention, a `SEMANTICS_REGISTER.md` row, against signatures verified from legend-pure |
| FL15 | A Python box now | A named exception to the tenet; only on pages Python serves; nothing pushes down through it |
| FL16 | Wait for D19 entirely | Slower, with nothing gained for the pure-function parts |
| FL16 | Everything now | The canvas built on the races D19 fixes |
