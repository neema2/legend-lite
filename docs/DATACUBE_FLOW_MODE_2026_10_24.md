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
  inside DataCube's. Output boxes come when it lands.
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
JSON reader and writer, its rebase arm, a UI producer, a Python builder and a parity row (One API rule 6). Each one
raises `L8_KINDS` in `datacube/test/one-api-ledger.test.ts`, which needs the user's dated sign-off when it is built
(D9).

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
| Text Input | Step | A small table typed in, as a source (Pure's `#TDS` literal) |
| Browse | Built | The results pane: the selected box's or port's grid |
| Output Data (to a table) | Core, after v1 | `write` to a store table throws "not yet implemented" (`core/.../lowering/Lowerer.java:3295`): F1, built in core in parallel (FL13) |
| Output Data (to a file) | Built | The grid's exports, for the rows shown. The whole result streamed is A5 of the BI and ETL plan |
| Data Stream In / Out | Step | The Cache box (FL10), on Snap |
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
| Random % Sample | Step | `random()` |
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
| Join Multiple | Built, once linked | Joins chained by live inputs |
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
| Cross Tab | Step | The grid's pivot, typed by its cast, its values checked (FL2, FL3, FL8) |
| Transpose | Step | Composed: one `select` per column, concatenated (FL14) |
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

### Developer, apps, macros and the canvas

| Tool | Status | How |
|---|---|---|
| Comment, Tool Container | Step | The flow sheet's own state (§3.4) |
| Test | Later | Checks as boxes (not null, unique, in range, row count), compiled to SQL, each with its mode (FL8; track C5) |
| Block Until Done | Later | Run order, from runs and schedules (F3) |
| Message | Later | The run's log |
| Base 64 Encoder | Built | `encodeBase64` in a Formula |
| Dynamic Rename, Select, Replace | Later | |
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

## 5. Order

1. **Now, beside D19 (FL16):**
   - the live input, freeze and unfreeze, and ports, as page and grid commands, each whole (§3.5);
   - Transpose, composed (FL14);
   - in core, F1, as core's own slice (FL13).
2. **After D19: the flow sheet** (`kind: 'flow'`, FL4): boxes and lines drawn from the page document, the results pane,
   boxes marked with the compiler's reason, auto-layout.
3. **The palette:** each tool makes a one-tool grid (FL5), grouped as Alteryx groups them; ports on the canvas (FL7);
   counts after a run; results-pane gestures that add a box (FL9).
4. **Cross Tab with its cast and checks** (FL2, FL3, FL8); the Cache box (FL10); Profile, text to columns, sample, the
   cleansing presets.
5. **Import Pure and the Pure box** (FL12).
6. **When F1 lands:** Output boxes; then runs and schedules (F3).

---

## 6. Still open

1. **F1's scheduling:** which session builds it in core, and when (FL13 says in parallel, as core's slice).
2. **Whether legend-pure has an `unpivot`.** Nothing in this repository shows one, and no legend-pure checkout was in
   the session that wrote this. If it has one, Transpose uses it instead of being composed (FL14).
3. **`L8_KINDS`:** each new command kind's raise is signed off, dated, when it is built (D9).
4. **Python's method names** for flows are put to the user before they are built (D6).

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
