// The grid's context menu.
//
// DataCube's has 66 entries, and the thing that makes that tractable
// is that almost all of them are the same few verbs applied to
// whatever was right-clicked. So the menu is DATA -- built from the
// context, tested as data -- and the renderer is thin. A menu built
// by conditional DOM is a menu nobody can test.
//
// Two rules that matter more than the list:
//
//  - An action that cannot apply is ABSENT, not disabled-and-silent.
//    "Remove Vertical Pivot" on a column that is not pivoted would be
//    a lie; a disabled entry at least says so, but omitting it says
//    it better.
//  - Every entry names its target. "Vertical Pivot on desk" rather
//    than "Vertical Pivot", because a context menu is read after the
//    right-click, when which column was clicked is already fading.

import type { CubeSnapshot, SortDirection } from '../snapshot.ts';
import { PIVOT_SEPARATOR } from '../grid/columns.ts';

/**
 * A pivoted column's name is a PATH -- `2021__|__notional` -- and a
 * menu that shows it raw asks the user to read the separator. It
 * reads back as "2021 / notional", the same presentation the sorts
 * panel uses.
 */
export function columnLabel(name: string): string {
  return name.split(PIVOT_SEPARATOR).join(' / ');
}

export interface MenuContext {
  readonly snapshot: CubeSnapshot;
  /** The column under the pointer, if any. */
  readonly column?: string;
  /** Whether that column is a row dimension already. */
  readonly isRowDimension?: boolean;
  /** Whether a selection exists, which gates the copy entries. */
  readonly hasSelection?: boolean;
  /** Whether any group is currently open, which gates Collapse All. */
  readonly hasExpanded?: boolean;
  /** Whether the column under the pointer already has a heatmap. */
  readonly hasHeatmap?: boolean;
  /**
   * Whether the column may be grouped or pivoted by at all.
   *
   * A pivoted measure cannot: "Vertical Pivot on 2021 / notional"
   * is an action with no meaning, and offering it is worse than
   * omitting it -- the menu already omits actions that cannot
   * apply rather than disabling them.
   */
  readonly canGroup?: boolean;
}

export type MenuActionId =
  | 'sort.asc'
  | 'sort.desc'
  | 'sort.addAsc'
  | 'sort.addDesc'
  | 'sort.clearColumn'
  | 'sort.clearAll'
  | 'filter.column'
  | 'filter.clearAll'
  | 'pivot.vertical'
  | 'pivot.addVertical'
  | 'pivot.removeVertical'
  | 'pivot.clearVertical'
  | 'pivot.horizontal'
  | 'pivot.addHorizontal'
  | 'pivot.removeHorizontal'
  | 'pivot.clearHorizontal'
  | 'column.hide'
  | 'column.autoSize'
  | 'column.autoSizeAll'
  | 'column.pinLeft'
  | 'column.pinRight'
  | 'column.unpin'
  | 'column.unpinAll'
  | 'tree.collapseAll'
  | 'copy.selection'
  | 'copy.column'
  | 'export.csv'
  | 'export.excel'
  | 'export.html'
  | 'export.specification'
  | 'heatmap.add'
  | 'heatmap.remove'
  | 'view.properties'
  // Host-level entries, which live in the title bar's menu rather
  // than the grid's. DataCube reserves that menu for the embedding
  // application the same way.
  | 'view.save'
  | 'view.load'
  | 'view.dimension';

export interface MenuItem {
  readonly id: MenuActionId;
  readonly label: string;
  /** Column the action applies to, when it is column-specific. */
  readonly column?: string;
  readonly direction?: SortDirection;
}

export interface MenuGroup {
  readonly label: string;
  readonly items: readonly MenuItem[];
}

/**
 * Build the menu for a context.
 *
 * Groups with no applicable items are dropped, so a right-click on
 * empty space does not open a menu of headings.
 */
export function buildMenu(ctx: MenuContext): MenuGroup[] {
  const { snapshot: s, column } = ctx;
  const groups: MenuGroup[] = [];
  const push = (label: string, items: (MenuItem | null)[]): void => {
    const present = items.filter((i): i is MenuItem => i !== null);
    if (present.length > 0) groups.push({ label, items: present });
  };

  const sorted = column
    ? s.sorts.find((x) => x.column === column)
    : undefined;

  if (column) {
    push('Sort', [
      { id: 'sort.asc', label: 'Ascending', column, direction: 'asc' },
      { id: 'sort.desc', label: 'Descending', column, direction: 'desc' },
      // "Add" only means something once there is a sort to add to.
      s.sorts.length > 0
        ? { id: 'sort.addAsc', label: 'Add Ascending', column, direction: 'asc' }
        : null,
      s.sorts.length > 0
        ? {
            id: 'sort.addDesc',
            label: 'Add Descending',
            column,
            direction: 'desc',
          }
        : null,
      sorted ? { id: 'sort.clearColumn', label: 'Clear Sort', column } : null,
      s.sorts.length > 0
        ? { id: 'sort.clearAll', label: 'Clear All Sorts' }
        : null,
    ]);
  }

  // Their two entries, verbatim. The editor opens unscoped -- there
  // is no per-column filter dialog in DataCube, because the filter
  // is one tree over the whole cube rather than a set of per-column
  // widgets that have to be reconciled.
  push('Filter', [
    { id: 'filter.column', label: 'Filters...', ...(column ? { column } : {}) },
    s.filter ? { id: 'filter.clearAll', label: 'Clear All Filters' } : null,
  ]);

  if (column && ctx.canGroup !== false) {
    const isVertical = s.rows.includes(column);
    const isHorizontal = s.pivotOn.includes(column);
    push('Pivot', [
      // Replace, or add alongside: two different intentions, so two
      // entries rather than one that guesses.
      !isVertical
        ? { id: 'pivot.vertical', label: `Vertical Pivot on ${columnLabel(column)}`, column }
        : null,
      !isVertical && s.rows.length > 0
        ? {
            id: 'pivot.addVertical',
            label: `Add Vertical Pivot on ${columnLabel(column)}`,
            column,
          }
        : null,
      isVertical
        ? {
            id: 'pivot.removeVertical',
            label: `Remove Vertical Pivot on ${columnLabel(column)}`,
            column,
          }
        : null,
      s.rows.length > 0
        ? { id: 'pivot.clearVertical', label: 'Clear All Vertical Pivots' }
        : null,
      !isHorizontal
        ? {
            id: 'pivot.horizontal',
            label: `Horizontal Pivot on ${columnLabel(column)}`,
            column,
          }
        : null,
      !isHorizontal && s.pivotOn.length > 0
        ? {
            id: 'pivot.addHorizontal',
            label: `Add Horizontal Pivot on ${columnLabel(column)}`,
            column,
          }
        : null,
      isHorizontal
        ? {
            id: 'pivot.removeHorizontal',
            label: `Remove Horizontal Pivot on ${columnLabel(column)}`,
            column,
          }
        : null,
      s.pivotOn.length > 0
        ? { id: 'pivot.clearHorizontal', label: 'Clear All Horizontal Pivots' }
        : null,
    ]);

  }

  // Layout actions apply to ANY column, pivoted measures included:
  // hiding or pinning `2021 / notional` is meaningful where
  // grouping by it is not. These sat inside the pivot block, so
  // gating that block on canGroup took them away too.
  if (column) {
    push('Column', [
      { id: 'column.hide', label: `Hide ${columnLabel(column)}`, column },
      { id: 'column.autoSize', label: 'Auto-size to Fit Content', column },
      { id: 'column.autoSizeAll', label: 'Auto-size All Columns' },
      { id: 'column.pinLeft', label: 'Pin Left', column },
      { id: 'column.pinRight', label: 'Pin Right', column },
      { id: 'column.unpin', label: 'Unpin', column },
      { id: 'column.unpinAll', label: 'Remove All Pinnings' },
    ]);
  }

  push('Tree', [
    ctx.hasExpanded
      ? { id: 'tree.collapseAll', label: 'Collapse All' }
      : null,
  ]);

  push('Copy', [
    ctx.hasSelection
      ? { id: 'copy.selection', label: 'Selected Cells as Plain Text' }
      : null,
    column
      ? { id: 'copy.column', label: `Column ${columnLabel(column)} as Plain Text`, column }
      : null,
  ]);

  push('Export', [
    { id: 'export.html', label: 'HTML' },
    { id: 'export.excel', label: 'Excel (Grid)' },
    { id: 'export.csv', label: 'CSV (Grid)' },
    { id: 'export.specification', label: 'DataCube Specification' },
  ]);

  // Heatmap is per COLUMN, because per column is the only scale that
  // means anything -- see style.ts.
  if (column) {
    push('Heatmap', [
      ctx.hasHeatmap
        ? { id: 'heatmap.remove', label: 'Remove Heatmap', column }
        : { id: 'heatmap.add', label: 'Add Heatmap', column },
    ]);
  }

  // Their menu is where the editor opens from. A toolbar button for
  // it would be a second door to the same room, and not one their
  // users would look for.
  push('', [{ id: 'view.properties', label: 'Properties...' }]);

  return groups;
}

/** Every item in a menu, flattened — for tests and for keyboard use. */
export function menuItems(groups: readonly MenuGroup[]): MenuItem[] {
  return groups.flatMap((g) => [...g.items]);
}

// -- applying an action to the snapshot -------------------------------

/**
 * Apply a menu action, returning a NEW snapshot.
 *
 * Actions that change nothing return the snapshot unchanged by
 * identity, so a caller can skip a refresh rather than re-running the
 * same query.
 */
export function applyMenuAction(
  s: CubeSnapshot,
  item: MenuItem,
): CubeSnapshot {
  const col = item.column;
  const without = <T>(xs: readonly T[], p: (x: T) => boolean): T[] =>
    xs.filter((x) => !p(x));

  switch (item.id) {
    case 'sort.asc':
    case 'sort.desc':
      return col
        ? { ...s, sorts: [{ column: col, direction: item.direction ?? 'asc' }] }
        : s;
    case 'sort.addAsc':
    case 'sort.addDesc':
      return col
        ? {
            ...s,
            sorts: [
              ...without(s.sorts, (x) => x.column === col),
              { column: col, direction: item.direction ?? 'asc' },
            ],
          }
        : s;
    case 'sort.clearColumn':
      return col
        ? { ...s, sorts: without(s.sorts, (x) => x.column === col) }
        : s;
    case 'sort.clearAll':
      return { ...s, sorts: [] };

    case 'filter.clearAll': {
      const { filter: _drop, ...rest } = s;
      return rest;
    }

    case 'pivot.vertical':
      return col ? { ...s, rows: [col] } : s;
    case 'pivot.addVertical':
      return col && !s.rows.includes(col)
        ? { ...s, rows: [...s.rows, col] }
        : s;
    case 'pivot.removeVertical':
      return col ? { ...s, rows: without(s.rows, (x) => x === col) } : s;
    case 'pivot.clearVertical':
      return { ...s, rows: [] };

    case 'pivot.horizontal':
      return col ? { ...s, pivotOn: [col] } : s;
    case 'pivot.addHorizontal':
      return col && !s.pivotOn.includes(col)
        ? { ...s, pivotOn: [...s.pivotOn, col] }
        : s;
    case 'pivot.removeHorizontal':
      return col ? { ...s, pivotOn: without(s.pivotOn, (x) => x === col) } : s;
    case 'pivot.clearHorizontal':
      return { ...s, pivotOn: [] };

    default:
      // Layout, copy and export actions do not touch the query.
      return s;
  }
}
