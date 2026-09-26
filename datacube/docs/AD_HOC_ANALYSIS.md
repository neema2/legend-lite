# Ad Hoc Analysis mode — the spec, for review before building (2026-09-25)

**What "full ad hoc analysis mode" means here:** classic OLAP **ad hoc
analysis** against a cube — the way multidimensional-database users work
in a spreadsheet-style client — built on DataCube's grid, over any
relational source through legend-lite. Not upstream's `MULTIDIMENSIONAL`
grid mode (WIP, and far short of this: one drill level, Zoom Out, no POV,
no Keep/Remove Only, no member selection, no suppression). The classic
ad hoc feature set is the bar.

The behaviour matched is the long-established ad hoc feature set of OLAP
spreadsheet clients (zoom, keep/remove only, pivot, POV, member selection,
suppression), described here in generic terms.

## 1. The model: an outline

| OLAP concept | Here |
|---|---|
| **Dimension** with a **hierarchy** of generations (Year > Quarter > Month) | A *named dimension* (exists: `panel-dimensions.ts`): a name + an ordered list of columns, top generation first |
| **Member** | A value at a generation, identified by its path (`2021 / Q1`) — the tree's `RowPath` |
| **Top member** ("All") | The dimension with nothing selected: every row |
| **Measures dimension** | The cube's measures (sum of notional, …) as members of a "Measures" axis |
| Stored / dynamic members | Aggregates computed by the database, as today — never in the browser |

A column that belongs to no named dimension becomes a one-generation
dimension of its own, so every column can be placed.

## 2. The grid: three axes

- **Rows** and **columns** hold dimensions (several nested on each, as
  classic ad hoc grids allow), each showing some of its members.
- **POV** (point of view) holds every other dimension, each pinned to ONE
  member (initially its top). Shown as a bar above the grid: one
  drop-down per dimension; choosing a member filters the whole grid.
- A cell is the measure aggregated over the intersection of its row
  member, column member and the POV — one query per visible
  intersection set, grouped by the members on screen.

## 3. Operations (the core of the mode)

| Operation | Behaviour | Built on |
|---|---|---|
| **Zoom In** (double-click a member, or menu) | Replace/expand the member by its children. Levels: **Next Level**, **All Levels** (every descendant), **Bottom Level** (leaves only) — as the Zoom In level option | Tree expand + detail rows (landed today) |
| **Zoom Out** | Replace the member (and its siblings) by its parent | Tree collapse / drill up |
| **Keep Only** | Keep only the selected members of that dimension (multi-select) | an `in` filter at that generation |
| **Remove Only** | Remove the selected members | `not in` |
| **Pivot** | Move a dimension rows ↔ columns | zone moves (exist) |
| **Pivot to POV** | Move a dimension off the grid onto the POV (at its current member) | new POV zone |
| **Member Selection** | A dialog per dimension: the hierarchy as a searchable tree; pick members; *Children*, *Descendants*, *Level 0* (bottom), *Same Level* shortcuts; the picks become the axis's members | new dialog over a distinct-values query per generation |
| **Refresh** | Re-run with the current grid | exists |
| **Undo / Redo** | Every ad hoc step | exists (history) |

## 4. Options (the classic ad hoc options that apply)

- **Zoom In level:** Next / All / Bottom.
- **Ancestor position:** Top (parents above children) or **Bottom**
  (children above parents, the classic default for totals).
- **Indentation:** subitems, totals, or none.
- **Suppress rows:** No Data/Missing, Zero, Repeated Members.
- **Suppress columns:** No Data/Missing, Zero.
- **Member display:** name / alias / both — alias from a column's display
  name.
- **Navigate without data:** move members around without querying,
  refresh once at the end.

## 5. Out of scope (said, not dropped)

- **Submit Data / write-back** — the cube is read-only over its source.
- **Calculation scripts, business rules, cell comments, supporting
  detail, saved slices, report designer, macros** — OLAP server
  and Office features, not grid analysis.
- **Attribute dimensions and UDAs** — later, if the model carries them.

## 6. Build order (each step tested in unit tests and the browser harness)

1. The outline model: named dimensions as hierarchies, every other column
   a one-generation dimension, Measures as a dimension.
2. The three axes and the **POV bar**; a cell's query = row members ×
   column members × POV filter.
3. **Zoom In / Zoom Out** with the three levels, **Keep Only / Remove
   Only**, **Pivot / Pivot to POV** — on the grid's right-click menu and
   double-click, as classic ad hoc clients have them.
4. **Member Selection** dialog.
5. **Options:** ancestor position, indentation, suppression, member
   display, navigate without data.
6. The mode switch: *Ad Hoc Analysis* in the title bar menu, the
   current cube carried over (rows → row axis, pivots → column axis,
   filters → POV where a filter is one member).

## Decisions (user, 2026-09-25)

1. **Measures are a dimension** ("Measures"), placeable on rows, columns
   or the POV and subject to Keep Only / Remove Only like any other.
2. **Ancestor position is a user option, default TOP** (parents above
   their children, as our tree does today); BOTTOM available.
3. **Suppress missing rows is ON by default**; zero and repeated-member
   suppression start off.
