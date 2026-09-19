# How DataCube actually renders the row tree

Read out of `legend-studio`, not inferred. The first implementation
here gave each row dimension its own column and stepped the labels
diagonally; real DataCube uses exactly **one** tree column for the
whole pivot, however many dimensions are grouped.

## The configuration

`stores/view/grid/DataCubeGridConfigurationBuilder.tsx`:

    groupDisplayType: 'singleColumn'

    autoGroupColumnDef: {
      colId: INTERNAL__GRID_CLIENT_TREE_COLUMN_ID,  // 'ag-Grid-AutoColumn'
      headerName: '',
      cellRendererParams: { suppressCount: !configuration.showLeafCount },
      ..._groupDisplaySpec(snapshot, configuration),
      minWidth: 200,
      sortable: true,
      showRowGroup: true,
      suppressSpanHeaderHeight: true,
    }

and `_groupDisplaySpec` adds:

    cellDataType: false,   // "no point specifying a type here since it
                           //  can be of multiple types"
    hide: !snapshot.data.groupBy,
    lockPosition: true,
    lockPinned: true,
    pinned: LEFT

## What that means

- **One column, not one per dimension.** The grid's width is
  independent of how deep the cube goes.
- **The header is empty.** No single name is truthful for a column
  that holds `region` on one row and `desk` on the next.
- **The column is heterogeneous by construction**, which is precisely
  why `cellDataType: false` is set. This is the detail that makes the
  single-column design coherent rather than a shortcut: a type would
  be a lie.
- **It disappears when nothing is grouped** (`hide: !groupBy`), rather
  than sitting empty.
- **Locked left, locked in position, minimum 200px.** The labels are
  the only thing identifying a row, so they must not scroll away or be
  dragged elsewhere.

## Defaults worth knowing

`stores/core/model/DataCubeConfiguration.ts`:

    showLeafCount = true          // group rows show "(n)" after the label
    showRootAggregation = false   // the GRAND TOTAL IS OFF by default

The second is a real divergence from what was built here, which shows
the grand total by default. When `showRootAggregation` is on, DataCube
shifts every `rowGroupIndex` by one so the root occupies index 0.

Also `INTERNAL__GRID_CLIENT_MISSING_VALUE = '__MISSING'` — a sentinel
for a group whose value is absent, which must never reach the screen.
This implementation has the same hazard with its own NULL sentinel and
a test pinning that it is rendered as blank rather than leaked.

## What was implemented

`treeColumn: 'single'` is now the default and matches the above: one
synthetic `__tree` column, blank header, depth by indentation, a
disclosure chevron on group rows and a same-width spacer on leaves so
sibling labels line up.

`treeColumn: 'perDimension'` keeps the original stepped layout as an
option. It is genuinely easier to scan on a shallow cube and puts a
real header on every level, but it widens without bound, so it is not
the default.

Leaf counts are not implemented. Rendering them needs a count
aggregate in every level query, and adding one silently changes the
query the user asked for; it should be an explicit measure rather than
a display toggle that rewrites SQL behind the scenes.
