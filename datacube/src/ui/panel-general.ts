// General Properties: the cube-wide settings.
//
// Every label here is DataCube's own, read off
// DataCubeEditorGeneralPropertiesPanel rather than invented, down to
// "Initially expand to level" and the four-slot colour grid. The
// grouping is its grouping too: report title, tree column, row
// limit, grid lines, highlighted rows, default font, default
// colours.
//
// One deliberate difference. DataCube's font controls are a dropdown
// of nine named families; a named family that the viewer does not
// have falls back silently and the grid then measures wrong. So the
// families here are stacks with a real fallback, and the dropdown
// shows the head of each stack.

import { SCALES } from '../format.ts';
import type { FontCase } from '../format.ts';
import type {
  CellAppearance,
  ColourSet,
  GridAppearance,
  TextAlign,
} from '../style.ts';
import {
  type CubeConfiguration,
  type Patch,
  withAppearance,
  withSettings,
} from '../config.ts';
import {
  badge,
  button,
  checkbox,
  colorPicker,
  dropdown,
  field,
  numberInput,
  section,
  textInput,
  toggle,
  toggleGroup,
} from './form.ts';
import {
  SORT_DIRECTIONS,
  panelShell,
  type PanelBuilder,
  type PanelContext,
} from './panel-kit.ts';

/**
 * Font stacks, each named for its head.
 *
 * DataCube names a bare family. A bare family the viewer lacks falls
 * back to whatever the browser picks, silently, and a grid that has
 * measured its column widths against one metric then renders in
 * another. A stack makes the fallback a decision.
 */
export const FONT_FAMILIES: readonly { value: string; label: string }[] = [
  { value: 'Arial, Helvetica, sans-serif', label: 'Arial' },
  { value: 'Roboto, Arial, sans-serif', label: 'Roboto' },
  { value: '"Helvetica Neue", Helvetica, Arial, sans-serif', label: 'Helvetica' },
  { value: 'Verdana, Geneva, sans-serif', label: 'Verdana' },
  { value: 'Tahoma, Geneva, sans-serif', label: 'Tahoma' },
  { value: 'Georgia, "Times New Roman", serif', label: 'Georgia' },
  { value: '"Times New Roman", Times, serif', label: 'Times New Roman' },
  { value: '"Courier New", Courier, monospace', label: 'Courier New' },
  { value: 'ui-monospace, "SF Mono", Menlo, monospace', label: 'Monospace' },
];

export const FONT_CASES: readonly { value: FontCase; label: string }[] = [
  { value: 'lowercase', label: 'lowercase' },
  { value: 'uppercase', label: 'UPPERCASE' },
  { value: 'capitalize', label: 'Capitalize' },
];

export const ALIGNMENTS: readonly {
  value: TextAlign;
  label: string;
  title: string;
}[] = [
  { value: 'left', label: '⇤', title: 'Align Left' },
  { value: 'center', label: '↔', title: 'Align Center' },
  { value: 'right', label: '⇥', title: 'Align Right' },
];

/** The four colour slots, in DataCube's order. */
export const COLOUR_SLOTS: readonly {
  state: 'normal' | 'negative' | 'zero' | 'error';
  label: string;
}[] = [
  { state: 'normal', label: 'Normal' },
  { state: 'negative', label: 'Negative' },
  { state: 'zero', label: 'Zero' },
  { state: 'error', label: 'Error' },
];

type Slot = (typeof COLOUR_SLOTS)[number]['state'];

export function foregroundKey(slot: Slot): keyof ColourSet {
  return `${slot}Foreground` as keyof ColourSet;
}
export function backgroundKey(slot: Slot): keyof ColourSet {
  return `${slot}Background` as keyof ColourSet;
}

/**
 * The font controls, shared by both property panels.
 *
 * Bold, italic, underline and strikethrough are INDEPENDENT toggles
 * and case and alignment are exclusive groups, which is why they are
 * different controls rather than one row of buttons: a user can want
 * bold italic, and cannot want left-aligned right-aligned.
 */
export function fontControls(
  doc: Document,
  style: CellAppearance,
  patch: (change: Patch<CellAppearance>) => void,
): HTMLElement[] {
  const bar = doc.createElement('div');
  bar.className = 'dc-font-bar';
  bar.append(
    dropdown(
      doc,
      style.fontFamily,
      FONT_FAMILIES,
      (fontFamily) => patch({ fontFamily }),
      { allowNone: true, width: 180 },
    ),
    numberInput(doc, style.fontSize, (fontSize) => patch({ fontSize }), {
      min: 6,
      max: 48,
      width: 60,
    }),
    toggle(doc, 'B', style.bold, (bold) => patch({ bold }), { title: 'Bold' }),
    toggle(doc, 'I', style.italic, (italic) => patch({ italic }), {
      title: 'Italic',
    }),
    toggle(doc, 'U', style.underline, (underline) => patch({ underline }), {
      title: 'Underline',
    }),
    toggle(
      doc,
      'S',
      style.strikethrough,
      (strikethrough) => patch({ strikethrough }),
      { title: 'Strikethrough' },
    ),
    toggleGroup(
      doc,
      style.textAlign,
      ALIGNMENTS,
      (textAlign) => patch({ textAlign }),
      { allowNone: true },
    ),
  );
  return [bar];
}

/**
 * The four-by-two colour grid.
 *
 * Foreground and background for each value state, because the state
 * a cell is in is what decides both and pairing them is how a user
 * checks that a negative stays readable on its own background.
 */
export function colourGrid(
  doc: Document,
  colours: ColourSet,
  patch: (change: Patch<ColourSet>) => void,
): HTMLElement {
  const grid = doc.createElement('div');
  grid.className = 'dc-colour-grid';

  const corner = doc.createElement('div');
  grid.append(corner);
  for (const slot of COLOUR_SLOTS) {
    const h = doc.createElement('div');
    h.className = 'dc-colour-head';
    h.textContent = slot.label;
    grid.append(h);
  }

  for (const [label, key] of [
    ['Foreground:', foregroundKey],
    ['Background:', backgroundKey],
  ] as const) {
    const l = doc.createElement('div');
    l.className = 'dc-colour-row-label';
    l.textContent = label;
    grid.append(l);
    for (const slot of COLOUR_SLOTS) {
      const k = key(slot.state);
      grid.append(
        colorPicker(doc, colours[k], (value) => patch({ [k]: value }), {
          title: `${slot.label} ${label.replace(':', '').toLowerCase()}`,
        }),
      );
    }
  }
  return grid;
}

export const generalPropertiesPanel: PanelBuilder = (ctx) => {
  const doc = ctx.doc;
  const config = () => ctx.draft().config;
  const appearance = (): GridAppearance => config().appearance;

  const setConfig = (change: Patch<CubeConfiguration>): void => {
    ctx.setConfig(withSettings(config(), change));
  };
  const setAppearance = (change: Patch<GridAppearance>): void => {
    ctx.setConfig(withAppearance(config(), change));
  };

  const c = config();

  const title = section(
    doc,
    '',
    field(
      doc,
      'Report Title:',
      textInput(
        doc,
        c.reportTitle,
        (reportTitle) => setConfig({ reportTitle }),
        { width: 320 },
      ),
    ),
  );

  const tree = section(
    doc,
    'Tree Column',
    field(
      doc,
      '',
      checkbox(doc, 'Show root aggregation', c.showRootAggregation, (v) =>
        setConfig({ showRootAggregation: v }),
      ),
      // OFF by default, which is what DataCube shows: it leaves
      // ag-grid's `suppressRowGroupHidesColumns` alone, so grouping a
      // column hides it -- its values are the tree's now. Upstream
      // exposes no setting for it; this one exists because a person
      // who has just watched three columns vanish should be able to
      // put them back.
      checkbox(doc, 'Keep grouped columns in the grid',
        c.showGroupedColumns, (v) => setConfig({ showGroupedColumns: v }),
      ),
    ),
    // ITS OWN ROW, so the badge can only be read as belonging to the
    // leaf count. A marker at the end of a row of several checkboxes
    // attaches itself to whichever one happens to be last: adding a
    // working setting in front of it made that setting announce
    // itself as "Not wired", which is worse than no marker at all.
    field(
      doc,
      '',
      checkbox(doc, 'Show leaf count', c.showLeafCount, (v) =>
        setConfig({ showLeafCount: v }),
      ),
      // Honest marker: a leaf count needs a count aggregate added to
      // every level query, which is a query change rather than a
      // display one, and this build does not make it.
      badge(doc, 'Not wired'),
    ),
    field(
      doc,
      'Sort:',
      dropdown(
        doc,
        c.treeColumnSort,
        SORT_DIRECTIONS,
        (d) => setConfig({ treeColumnSort: d ?? 'asc' } as Patch<CubeConfiguration>),
        { width: 130 },
      ),
    ),
    field(
      doc,
      'Initially expand to level:',
      numberInput(
        doc,
        c.initialExpandToLevel,
        (initialExpandToLevel) => setConfig({ initialExpandToLevel }),
        { min: 1, max: 10 },
      ),
    ),
  );

  const rows = section(
    doc,
    'Rows',
    field(
      doc,
      'Row Limit:',
      numberInput(doc, c.maxRows, (v) => setConfig({ maxRows: v ?? 1000 }), {
        min: 1,
        max: 1_000_000,
        width: 110,
      }),
      checkbox(
        doc,
        'Display warning when truncated',
        c.showTruncationWarning,
        (v) => setConfig({ showTruncationWarning: v }),
      ),
    ),
  );

  const a = appearance();
  const lines = section(
    doc,
    'Grid Lines',
    field(
      doc,
      '',
      checkbox(doc, 'Horizontal', a.showHorizontalGridLines, (v) =>
        setAppearance({ showHorizontalGridLines: v }),
      ),
      checkbox(doc, 'Vertical', a.showVerticalGridLines, (v) =>
        setAppearance({ showVerticalGridLines: v }),
      ),
    ),
    field(
      doc,
      'Color:',
      colorPicker(doc, a.gridLineColor, (gridLineColor) =>
        setAppearance({ gridLineColor }),
      ),
    ),
  );

  const highlight = section(
    doc,
    'Highlight Rows',
    field(
      doc,
      '',
      checkbox(doc, 'Standard mode', a.alternateRows, (v) =>
        setAppearance({ alternateRows: v }),
      ),
    ),
    field(
      doc,
      'Custom: Alternate color:',
      colorPicker(doc, a.alternateRowsColor, (alternateRowsColor) =>
        setAppearance({ alternateRowsColor }),
      ),
      numberInput(
        doc,
        a.alternateRowsCount,
        (n) => setAppearance({ alternateRowsCount: n ?? 1 }),
        { min: 1, max: 100 },
      ),
    ),
  );

  const font = section(
    doc,
    'Default Font',
    ...fontControls(doc, a, (change) => setAppearance(change)),
    field(
      doc,
      'Case:',
      dropdown(
        doc,
        undefined,
        FONT_CASES,
        () => {
          /* case is per column; the grid-wide default is deliberately
             absent because a cube-wide UPPERCASE also shouts at the
             tree column, which is the one place it is never wanted */
        },
        { allowNone: true, width: 150, disabled: true },
      ),
      badge(doc, 'Per column'),
    ),
  );

  const colours = section(
    doc,
    'Default Colors',
    colourGrid(doc, a, (change) => setAppearance(change)),
    button(doc, 'Use Default Styling', () => {
      // Reset the APPEARANCE only. Wiping the row limit or the tree
      // sort from a button labelled "styling" is the kind of
      // surprise that makes people stop pressing buttons.
      ctx.setConfig({ ...config(), appearance: DEFAULT_APPEARANCE });
      ctx.refresh();
    }),
  );

  const misc = section(
    doc,
    'Miscellaneous',
    field(
      doc,
      'Pivot Statistic Column Name:',
      textInput(
        doc,
        c.pivotStatisticColumnName,
        (pivotStatisticColumnName) => setConfig({ pivotStatisticColumnName }),
        { placeholder: 'Total', width: 180 },
      ),
    ),
    field(
      doc,
      '',
      checkbox(doc, 'Show selection statistics', c.showSelectionStats, (v) =>
        setConfig({ showSelectionStats: v }),
      ),
    ),
    // THE CHROME, where the rest of "what is on screen" is set.
    //
    // The chevrons on the bars themselves are how a person folds
    // them in passing; this is where the setting LIVES, and it is
    // the only place that shows both at once -- a cube opened with
    // both folded is a legitimate way to hand someone a report, and
    // there has to be somewhere to say so.
    field(
      doc,
      '',
      checkbox(doc, 'Show drag zones', c.showDragZones, (v) =>
        setConfig({ showDragZones: v }),
      ),
      checkbox(doc, 'Show title bar', c.showTitleBar, (v) =>
        setConfig({ showTitleBar: v }),
      ),
    ),
    field(
      doc,
      'Grid Mode:',
      dropdown(
        doc,
        c.gridMode,
        [
          { value: 'standard' as const, label: 'Standard' },
          { value: 'dimensional' as const, label: 'Dimensional' },
        ],
        (gridMode) => {
          setConfig({ gridMode: gridMode ?? 'standard' });
          ctx.refresh();
        },
        { width: 150 },
      ),
    ),
  );

  return panelShell(
    doc,
    'General Properties',
    title,
    tree,
    rows,
    lines,
    highlight,
    font,
    colours,
    misc,
  );
};

/** What "Use Default Styling" restores. Appearance only -- see below. */
export const DEFAULT_APPEARANCE: GridAppearance = {
  showHorizontalGridLines: false,
  showVerticalGridLines: true,
  alternateRows: true,
  alternateRowsCount: 1,
};

/** Re-exported so the column panel shows the same scale names. */
export { SCALES };
export type { PanelContext };
