# DataCube commands, stage 1: the grid half

This is the command program's stage 1 (`docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md` §9), the grid half first.
It is built on branch `claude/datacube-commands`, stacked on stage 1a's tests (`claude/datacube-command-tests`), with no
PR until the user asks (the user, 2026-10-18).

## The goal

Every edit of a grid becomes a value, a `GridCommand`. One pure function applies it:
`applyGridCommand(draft, command)`.

- **The screen** builds the command from what a person did: a header click, a menu entry, a drag zone, Column
  Properties, the filter editor, the column editor.
- **Python** builds the same command from its call: `grid.group(...)`, `grid.sort(...)`, `grid.column(...)`,
  `grid.configure(...)`, `grid.filter(...)`, `grid.calculate(...)`.
- **One function decides.** Neither side holds a rule of its own. The command is the edit, so the screen and Python
  cannot do one edit two ways.

## The vocabulary

The vocabulary is the edits `grid-edits.ts` already holds, named as a person names them. Each command is plain JSON, so
Python sends it and the stage-3 live channel can carry it.

| Command | What it is | Applied by |
|---|---|---|
| `group {rows}` | the row groups, whole | `setGrouping` |
| `pivot {columns}` | the pivot, whole | `setPivot` |
| `sorts {sorts}` | the sorts, whole (the Sorts panel) | `setSorts` |
| `addSort {column, direction}` | a sort added last (the menu's Add, Python's `sort`) | `addSort` |
| `clickSort {column, first, chained}` | a header click; `first` is the direction it starts with, which the screen reads off the type the compiler gave the header | `clickSort` |
| `clickTreeSort {chained}` | a click on the tree column's header | `clickTreeSort` |
| `column {name, kind?, aggregate?, weight?}` | Column Properties | `setColumn` |
| `configure {patch}` | settings, an appearance, a column's presentation (a pivot total's, a pivot column's, the tree column's too: presentation only) | `withConfiguration` |
| `filter {filter}` | the filter, whole (the filter editor, Python's `filter`) | the snapshot's filter |
| `addFilter {condition}` | one condition added to the filter (a cell's menu) | and-ed in, as the menu does |
| `calculate {column}` | a calculated column added, its lambda already parsed | `draftCandidate`'s lists |
| `measures {measures}` | Python's `measure`: an Aggregation, or a copy of the column under a new name | `measureAsColumn` |

### Rules

- **Plain data.** A command is data and nothing else. Anything that needs the compiler happens before the command is
  built, on both sides. A calculated column's expression is parsed first, so `calculate` carries the lambda, and
  `apply` stays pure and synchronous.
- **Refusals.** `apply` refuses in words (`CubeRefusal`), as grid-edits does today. Neither side checks anything
  before it.
- **Reading JSON.** `readGridCommand(json)` reads a command and refuses an unknown kind or field in words. Python
  sends JSON, and a misspelt key must never pass unnoticed.
- **What only the view knows is in the command.** A header click's first direction depends on the type the compiler
  gave the header: a pivot's header, or a count of a text column, is a number though no column of the query is. The
  screen has the view, so it puts the direction in the command (`first`). Python never builds `clickSort`: its `sort`
  is `addSort`.

## Where it lands

- **The module:** `datacube/src/grid-commands.ts`, with no DOM. It holds `GridCommand`, `applyGridCommand` and
  `readGridCommand`.
- **Headless:** `gridApply(spec, commands)` replaces `gridEdit`. It applies a list of commands, compiles the grid as
  Apply compiles it, and writes the cube document. `gridCalculate` and `gridFilter` become steps that build a command:
  they parse, and they turn Python's filter into the editor's normal form.
- **Python:** each `Grid` method builds its commands and sends them through one `_apply`. `page.grid(...)` sends its
  starting edits as one list.
- **The screen** (step 3):
  - the edit handlers in `app.ts` build commands and pass them to one `#apply`, which replaced `#edit`: the header
    clicks, the drag zones, the menu's query entries (`ui/menu.ts menuCommand`; `applyMenuAction` is its command
    applied), the menu's heatmap and hide entries (`configure`), and the column editor's add and the Extended Columns
    presets (`calculate`);
  - an `onCommand` option tells a host each command the cube kept, once its change landed or was taken over by the next
    (the parity test, and the live channel later); one refused or cancelled is not told.
- **The Properties window's Apply** (step 3b): its draft's changes are the commands `draftCommands(base, edited)`
  builds -- a column made a dimension, the row groups and the pivot, the other kinds, Aggregations and weights
  (`column`), the settings beyond those (`configure`, null clearing one), the sorts -- applied to the cube as it is now,
  so another window's edit made meanwhile stays, as the three-way merge (`mergeDraft`, gone) kept it. Column
  Properties' Apply is then the `column` command Python's `column(...)` builds.
- **The columns' presentation** (step 3b): the columns panel's visibility and order, a width dragged, Auto-Size,
  Minimize and Size to Fit, a pin, Remove All Pinnings, a pivot exclusion, the zones and title bar folded, Measures
  First -- each a `configure` command (`#configureColumns`: a setting cleared is null).
- **Not commands:** the column editor's edit, rename and delete, which offer the cube's two lists whole -- they wait
  for a vocabulary of their own (no call of Python's makes them yet); and a host's `applyConfiguration`, which is its
  set-up of the grid, not a person's edit.

## How it is tested

- **Unit tests** in `grid-commands.test.ts`:
  - each command gives exactly what its grid-edits function gives;
  - `readGridCommand` refuses an unknown kind and a misspelt field, each in words.
- **The parity file is unchanged** (`datacube/test/fixtures/parity/parity.json`). Its 29 documents must still match on
  both sides, so moving to commands changes no edit.
- **Commands are compared too.** Each case action that is one edit on both sides -- a sort added, a heatmap, a hidden
  column, a calculated column, a grouping or a pivot on one column, a column's Aggregation, kind or weighted average --
  also holds the command both sides must build (`command`, written from the screen's by PARITY_UPDATE). The screen's
  comes from `onCommand`, Python's from what its page records. Held to the document alone: an action that is two
  steps on the screen and one call in Python (`group region, desk`: two menu entries, one `group` call), and one that
  is a different call on each side (a header click, which Python makes as an added sort; a cell's filter, added on the
  screen and set whole by Python).
- **The round trips** generate commands instead of calling grid-edits directly, and read each one back from its JSON
  (`readGridCommand`) as the same command: rebuilt field by field for every kind but `configure`, whose patch is
  judged when it is applied (its setting names, its columns), never when it is read.

### Where it stands (2026-10-18)

- Steps 1 to 4 are on the branch: the module and its tests; headless `gridApply` and Python's calls; the screen's
  `#apply` and `onCommand`; the parity file's commands (45 actions, each built alike by the screen and by Python, Column
  Properties' among them since step 3b's Properties Apply) and the round trips over commands.
- `onCommand` tells a command once the cube keeps it (stage 1's review, 2026-10-18): a change in flight's when it
  commits; a presentation change's at once; never one refused or cancelled, nor one carried by a change refused or
  cancelled (`app.ts #followCommands`, the owner's events).
- A command read from JSON is read field by field (the review): a filter's condition by its operator's operand, a
  calculated column as a formula's (name, lambda, kind, type) -- a window or child-aggregate column is not read from
  JSON yet.
- A command read from JSON keeps JSON's numbers: a calculated column's lambda sent as JSON (none is today: Python's
  `calculate` sends its text, parsed headless) would carry plain numbers where a parsed lambda carries exact ones. Safe
  for the integers and short decimals an expression holds; one past 2^53 is not, and the live channel (stage 3) reads
  its commands exactly.
- Step 3b's rest: the column editor's edit, rename and delete, with commands of their own when something asks for
  them (Python, or stage 3's live channel).
- A difference from the three-way merge it replaced: a column's setting that is an object (its format, its
  appearance) is set whole by `configure`, where the merge went key by key inside it. Two windows changing different
  keys of one column's format at once keep the later window's whole format.

## Not in stage 1's grid half

- **The page half:** sheets, tiles, charts, titles, layout. It waits on the page's parity, which compares views first,
  so chart edits get their tests before their commands (`docs/DATACUBE_COVERAGE_2026_10_18.md`).
- **One page state and page-level undo:** stage 2.
- **The live channel:** stage 3.
