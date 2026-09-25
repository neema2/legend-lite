// The grid's context menu.
//
// DataCube's has 66 entries, and the thing that makes that tractable
// is that almost all of them are the same few verbs applied to
// whatever was right-clicked. So the menu is DATA -- built from the
// context, tested as data -- and the renderer is thin. A menu built
// by conditional DOM is a menu nobody can test.
//
// It NESTS. Reading their DataCubeGridMenuBuilder properly rather
// than grepping its labels turned up three levels: Export, Copy,
// Sort, Filter, Pivot, Resize, Pin and Heatmap are all submenus,
// and Filter's own "More Filters on X..." is a submenu inside a
// submenu. A flat list of the same entries is a different product:
// sixteen top-level items scan as a wall, where eight verbs with
// their variants tucked underneath scan as a sentence.
//
// The rules the shape enforces:
//
//  - An inapplicable action is DISABLED, not omitted. This reverses
//    an earlier decision here, and DataCube is right: a menu whose
//    entries move depending on context cannot be learned, and the
//    whole value of a context menu is that the third time you use
//    it you no longer read it. "Remove Vertical Pivot", greyed,
//    still says the column is not pivoted.
//  - Every entry names its target. "Vertical Pivot on desk" rather
//    than "Vertical Pivot", because a context menu is read after the
//    right-click, when which column was clicked is already fading.
//  - The FILTER entries know the value that was clicked. Right-click
//    a cell reading EMEA and the menu offers "Add Filter: region =
//    EMEA" directly, with every other operator for that column's
//    type one level down. This is the single most useful thing in
//    their menu and it is the part a labels-only reading misses.

import type {
  CubeSnapshot,
  FilterNode,
  FilterOperator,
  FilterValue,
  SortDirection,
} from '../snapshot.ts';
import type { CalcStage } from '../calc.ts';
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
  /**
   * Whether the host can send mail.
   *
   * A browser cannot attach a file to a mailto: link, so emailing an
   * export is something only the embedding application can do. The
   * entries are shown DISABLED when it cannot, rather than hidden:
   * "this build cannot email" is information, and a menu that
   * silently lacks an action a colleague's build has is confusing.
   */
  readonly canEmail?: boolean;
  /**
   * The value in the cell that was right-clicked, if any.
   *
   * This is what turns the Filter submenu from a door to a dialog
   * into a one-click action, and it is the part a labels-only
   * reading of their menu misses entirely. Absent when the menu
   * came from a header, where there is no value to filter on;
   * null means the cell was blank.
   */
  readonly value?: FilterValue | null;
  /** The type of the column that value belongs to. */
  readonly columnType?: string;
  /**
   * Whether a new calculated column can be seeded from this one:
   * true for a column that exists before aggregation, source or
   * calculated. A pivot's generated column or a group-stage one is
   * not something a row-stage expression can see.
   */
  readonly extendable?: boolean;
  /** The stage, when the column is itself a calculated one. */
  readonly calcStage?: CalcStage;
}

/**
 * Whether a column is a calculated one, and at which stage.
 *
 * Spread into the menu context, so an ordinary column adds nothing.
 */
export function calcStageOf(
  s: CubeSnapshot,
  column: string,
): { calcStage?: CalcStage } {
  if (s.derived.some((d) => d.name === column)) return { calcStage: 'row' };
  if ((s.groupDerived ?? []).some((d) => d.name === column)) {
    return { calcStage: 'group' };
  }
  return {};
}

export type MenuActionId =
  /**
   * A HOST's own entry, which must be prefixed.
   *
   * The cube's ids stay a closed union, so `applyMenuAction` and the
   * dispatch in app.ts remain exhaustive over them and
   * `menu-ids.test.ts` can still tell a dead entry from a live one.
   * A host adds items to the title bar menu and gets them back
   * through `onHostMenu`; the prefix is what keeps the two sets from
   * ever being confused for one another.
   */
  | `host.${string}`
  | 'sort.asc'
  | 'sort.desc'
  | 'sort.addAsc'
  | 'sort.addDesc'
  | 'sort.clearColumn'
  | 'sort.clearAll'
  | 'filter.add'
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
  | 'export.text'
  | 'export.pdf'
  | 'export.specification'
  | 'email.html'
  | 'email.excel'
  | 'email.csv'
  | 'email.text'
  | 'email.pdf'
  | 'heatmap.add'
  | 'heatmap.remove'
  | 'chart.plot'
  | 'chart.treemap'
  | 'view.properties'
  // Upstream's Extended Columns submenu.
  | 'calc.add'
  | 'calc.extend'
  | 'calc.edit'
  | 'calc.delete'
  // The chrome, toggled from the title bar's menu. Not a snapshot
  // change and not a column operation: what is on SCREEN. It left the
  // grid's menu by the user's direction (2026-09-25); a folded title
  // bar leaves a lip that restores it.
  | 'view.zones'
  | 'view.titleBar'
  // Host-level entries, which live in the title bar's menu rather
  // than the grid's. DataCube reserves that menu for the embedding
  // application the same way.
  | 'view.save'
  | 'view.load'
  | 'view.dimension'
  | 'view.undo'
  | 'view.redo';

export interface MenuItem {
  /** Absent on a pure submenu parent, which does nothing itself. */
  readonly id?: MenuActionId;
  readonly label: string;
  /** Column the action applies to, when it is column-specific. */
  readonly column?: string;
  readonly direction?: SortDirection;
  /** For a value-aware filter entry. */
  readonly operator?: FilterOperator;
  readonly value?: FilterValue;
  /**
   * Shown but not actionable. DataCube disables rather than omits,
   * so the menu keeps its shape and can be learned.
   */
  readonly disabled?: boolean;
  readonly submenu?: readonly MenuItem[];
}

export interface MenuGroup {
  readonly label: string;
  readonly items: readonly MenuItem[];
}

/**
 * Operators offered for a column's type, as theirs are.
 *
 * A string takes the comparisons AND the text predicates; a number
 * or a date takes the comparisons only; anything else -- a boolean
 * -- takes none, because "starts with true" is not a question.
 */
export function filterOperatorsFor(type: string): FilterOperator[] {
  switch (type) {
    case 'String':
      return [
        'equal',
        'notEqual',
        'lessThan',
        'lessThanEqual',
        'greaterThan',
        'greaterThanEqual',
        'contains',
        'notContains',
        'startsWith',
        'notStartsWith',
        'endsWith',
        'notEndsWith',
      ];
    case 'Integer':
    case 'Float':
    case 'Number':
    case 'Decimal':
    case 'Date':
    case 'StrictDate':
    case 'DateTime':
      return [
        'equal',
        'notEqual',
        'lessThan',
        'lessThanEqual',
        'greaterThan',
        'greaterThanEqual',
      ];
    // Upstream's Equal and NotEqual are the only operations that
    // accept BOOLEAN. Without this arm a Boolean column -- `settled`,
    // or any calculated flag -- got no value filter from the menu.
    case 'Boolean':
      return ['equal', 'notEqual'];
    default:
      return [];
  }
}

/** Their label for each operator, for the value-aware filter items. */
const OPERATOR_LABEL: Readonly<Partial<Record<FilterOperator, string>>> = {
  equal: '=',
  notEqual: '!=',
  lessThan: '<',
  lessThanEqual: '<=',
  greaterThan: '>',
  greaterThanEqual: '>=',
  contains: 'contains',
  notContains: 'does not contain',
  startsWith: 'starts with',
  notStartsWith: 'does not start with',
  endsWith: 'ends with',
  notEndsWith: 'does not end with',
  isEmpty: 'is null',
  isNotEmpty: 'is not null',
};

/**
 * "Add Filter: region = EMEA" -- their wording exactly.
 *
 * The value is in the label because the menu is read after the
 * click, and by then which cell was under the pointer has gone.
 */
function filterItem(
  column: string,
  operator: FilterOperator,
  value: FilterValue | undefined,
): MenuItem {
  const shown = value === undefined ? '' : ` ${String(value)}`;
  return {
    id: 'filter.add',
    label: `Add Filter: ${columnLabel(column)} ${OPERATOR_LABEL[operator] ?? operator}${shown}`,
    column,
    operator,
    ...(value !== undefined ? { value } : {}),
  };
}

/**
 * Build the menu for a context.
 *
 * One list of top-level verbs, most of them carrying a submenu --
 * their structure, not a flattening of it. Groups here are the
 * separated blocks their menu draws rules between.
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
  const isVertical = column ? s.rows.includes(column) : false;
  const isHorizontal = column ? s.pivotOn.includes(column) : false;
  const groupable = column !== undefined && ctx.canGroup !== false;

  // ---- Export / Copy ----------------------------------------------
  push('', [
    {
      label: 'Export',
      submenu: [
        { id: 'export.html', label: 'HTML' },
        { id: 'export.excel', label: 'Excel (Grid)' },
        { id: 'export.csv', label: 'CSV (Grid)' },
        { id: 'export.text', label: 'Plain Text' },
        { id: 'export.pdf', label: 'PDF' },
        { id: 'export.specification', label: 'DataCube Specification' },
      ],
    },
    {
      label: 'Email',
      submenu: [
        { id: 'email.html', label: 'HTML' },
        { id: 'email.excel', label: 'Excel (Grid)' },
        { id: 'email.csv', label: 'CSV (Grid)' },
        { id: 'email.text', label: 'Plain Text' },
        { id: 'email.pdf', label: 'PDF' },
      ].map((i) => ({
        ...i,
        ...(ctx.canEmail ? {} : { disabled: true }),
      })) as MenuItem[],
    },
    {
      label: 'Copy',
      submenu: [
        {
          id: 'copy.selection',
          label: 'Selected Cells as Plain Text',
          disabled: !ctx.hasSelection,
        },
        {
          id: 'copy.column',
          label: column
            ? `Column ${columnLabel(column)} as Plain Text`
            : 'Selected Column as Plain Text',
          disabled: !column,
          ...(column ? { column } : {}),
        },
      ],
    },
  ]);

  // ---- Sort / Filter / Pivot ----------------------------------------
  push('', [
    {
      label: 'Sort',
      submenu: [
        {
          id: 'sort.asc',
          label: 'Ascending',
          disabled: !column,
          ...(column ? { column, direction: 'asc' as const } : {}),
        },
        {
          id: 'sort.desc',
          label: 'Descending',
          disabled: !column,
          ...(column ? { column, direction: 'desc' as const } : {}),
        },
        {
          id: 'sort.addAsc',
          label: 'Add Ascending',
          disabled: !column,
          ...(column ? { column, direction: 'asc' as const } : {}),
        },
        {
          id: 'sort.addDesc',
          label: 'Add Descending',
          disabled: !column,
          ...(column ? { column, direction: 'desc' as const } : {}),
        },
        {
          id: 'sort.clearColumn',
          label: 'Clear Sort',
          disabled: !sorted,
          ...(column ? { column } : {}),
        },
        {
          id: 'sort.clearAll',
          label: 'Clear All Sorts',
          disabled: s.sorts.length === 0,
        },
      ],
    },
    { label: 'Filter', submenu: filterSubmenu(ctx) },
    {
      label: 'Pivot',
      submenu: [
        {
          id: 'pivot.vertical',
          label: groupable
            ? `Vertical Pivot on ${columnLabel(column)}`
            : 'Vertical Pivot',
          disabled: !groupable,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.addVertical',
          label: groupable
            ? `Add Vertical Pivot on ${columnLabel(column)}`
            : 'Add Vertical Pivot',
          disabled: !groupable || isVertical,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.removeVertical',
          label: groupable
            ? `Remove Vertical Pivot on ${columnLabel(column)}`
            : 'Remove Vertical Pivot',
          disabled: !isVertical,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.clearVertical',
          label: 'Clear All Vertical Pivots',
          disabled: s.rows.length === 0,
        },
        {
          id: 'pivot.horizontal',
          label: groupable
            ? `Horizontal Pivot on ${columnLabel(column)}`
            : 'Horizontal Pivot',
          disabled: !groupable,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.addHorizontal',
          label: groupable
            ? `Add Horizontal Pivot on ${columnLabel(column)}`
            : 'Add Horizontal Pivot',
          disabled: !groupable || isHorizontal,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.removeHorizontal',
          label: groupable
            ? `Remove Horizontal Pivot on ${columnLabel(column)}`
            : 'Remove Horizontal Pivot',
          disabled: !isHorizontal,
          ...(column ? { column } : {}),
        },
        {
          id: 'pivot.clearHorizontal',
          label: 'Clear All Horizontal Pivots',
          disabled: s.pivotOn.length === 0,
        },
      ],
    },
  ]);

  // ---- Extended Columns -----------------------------------------------
  //
  // Upstream's entries and wording (DataCubeGridMenuBuilder): add one,
  // seed one from the column under the pointer, and edit or delete
  // the one under the pointer when it is itself calculated.
  const named = column === undefined ? '' : columnLabel(column);
  push('', [
    {
      label: 'Extended Columns',
      submenu: [
        { id: 'calc.add', label: 'Add New Column...' },
        ...(column !== undefined && ctx.extendable
          ? [{ id: 'calc.extend' as const, label: `Extend Column ${named}...`,
            column }]
          : []),
        ...(column !== undefined && ctx.calcStage !== undefined
          ? [
            { id: 'calc.edit' as const, label: `Edit Column ${named}...`,
              column },
            { id: 'calc.delete' as const, label: `Delete Column ${named}`,
              column },
          ]
          : []),
      ],
    },
  ]);

  // ---- Resize / Pin / Hide / Collapse / Heatmap -----------------------
  push('', [
    {
      label: 'Resize',
      submenu: [
        {
          id: 'column.autoSize',
          label: 'Auto-size to Fit Content',
          disabled: !column,
          ...(column ? { column } : {}),
        },
        { id: 'column.autoSizeAll', label: 'Auto-size All Columns' },
      ],
    },
    {
      label: 'Pin',
      submenu: [
        {
          id: 'column.pinLeft',
          label: 'Pin Left',
          disabled: !column,
          ...(column ? { column } : {}),
        },
        {
          id: 'column.pinRight',
          label: 'Pin Right',
          disabled: !column,
          ...(column ? { column } : {}),
        },
        {
          id: 'column.unpin',
          label: 'Unpin',
          disabled: !column,
          ...(column ? { column } : {}),
        },
        { id: 'column.unpinAll', label: 'Remove All Pinnings' },
      ],
    },
    {
      id: 'column.hide',
      label: column ? `Hide ${columnLabel(column)}` : 'Hide',
      disabled: !column,
      ...(column ? { column } : {}),
    },
    {
      id: 'tree.collapseAll',
      label: 'Collapse All',
      disabled: !ctx.hasExpanded,
    },
    {
      label: 'Heatmap',
      submenu: [
        {
          id: 'heatmap.add',
          label: column
            ? `Add Heatmap to ${columnLabel(column)}`
            : 'Add Heatmap',
          disabled: !column || Boolean(ctx.hasHeatmap),
          ...(column ? { column } : {}),
        },
        {
          id: 'heatmap.remove',
          label: 'Remove Heatmap',
          disabled: !column || !ctx.hasHeatmap,
          ...(column ? { column } : {}),
        },
      ],
    },
  ]);

  // Plot and treemap sit beside Properties, as theirs do: they are
  // views OF the cube rather than operations ON a column, so they do
  // not belong in the column-scoped groups above.
  push('', [
    { id: 'chart.plot', label: 'Plot' },
    { id: 'chart.treemap', label: 'Treemap' },
  ]);

  push('', [{ id: 'view.properties', label: 'Properties...' }]);

  return groups;
}

/**
 * The Filter submenu, which is the value-aware one.
 *
 * With a value under the pointer it leads with the equality filter
 * on that value and tucks every other operator for the column's
 * type into a submenu of its own -- their three-level shape. A
 * blank cell gets the two null predicates instead, because "= "
 * against nothing is not a filter anyone means.
 */
function filterSubmenu(ctx: MenuContext): MenuItem[] {
  const { column, value } = ctx;
  const items: MenuItem[] = [];

  if (column !== undefined && value !== undefined) {
    if (value === null) {
      items.push(
        filterItem(column, 'isEmpty', undefined),
        filterItem(column, 'isNotEmpty', undefined),
      );
    } else {
      const operators = filterOperatorsFor(ctx.columnType ?? 'String');
      if (operators.includes('equal')) {
        items.push(filterItem(column, 'equal', value));
        const more = operators.filter((op) => op !== 'equal');
        if (more.length > 0) {
          items.push({
            label: `More Filters on ${columnLabel(column)}...`,
            submenu: more.map((op) => filterItem(column, op, value)),
          });
        }
      }
    }
  }

  items.push(
    { id: 'filter.column', label: 'Filters...' },
    {
      id: 'filter.clearAll',
      label: 'Clear All Filters',
      disabled: !ctx.snapshot.filter,
    },
  );
  return items;
}

/** Every item in a menu, submenus included -- for tests and search. */
export function menuItems(groups: readonly MenuGroup[]): MenuItem[] {
  const walk = (items: readonly MenuItem[]): MenuItem[] =>
    items.flatMap((i) => [i, ...(i.submenu ? walk(i.submenu) : [])]);
  return groups.flatMap((g) => walk(g.items));
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

    case 'filter.add': {
      // ADDS to the filter rather than replacing it, which is what
      // "Add Filter" says and what makes the entry usable twice in
      // a row. The new condition joins the existing tree with AND;
      // an OR is the filter editor's job.
      if (!col || !item.operator) return s;
      const condition: FilterNode = {
        kind: 'condition',
        column: col,
        operator: item.operator,
        ...(item.value !== undefined ? { value: item.value } : {}),
      };
      const filter: FilterNode = s.filter
        ? s.filter.kind === 'and'
          ? { kind: 'and', children: [...s.filter.children, condition] }
          : { kind: 'and', children: [s.filter, condition] }
        : condition;
      return { ...s, filter };
    }

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
