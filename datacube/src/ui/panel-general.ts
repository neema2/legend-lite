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
import {
  FONT_STACKS,
  UNDERLINE_VARIANTS,
  type CellAppearance,
  type ColourSet,
  type GridAppearance,
  type TextAlign,
} from '../style.ts';
import {
  type CubeConfiguration,
  type Patch,
  withAppearance,
  withSettings,
} from '../config.ts';
import {
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
import { docHint } from './docs.ts';

/**
 * Upstream's font families (DataCubeFont) in its order -- sans-serif,
 * serif, monospace -- by the name a cube saves; `fontStack` renders
 * each with a fallback.
 */
export const FONT_FAMILIES: readonly { value: string; label: string }[] =
  Object.keys(FONT_STACKS).map((name) => ({ value: name, label: name }));

/** Upstream's font sizes: a list, not a free number. */
export const FONT_SIZES: readonly number[] = [
  4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18, 20, 22, 24, 26, 28, 32, 36, 48, 72,
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
 * Bold and italic are INDEPENDENT toggles; underline (with its
 * variant) and strikethrough EXCLUDE each other, as upstream; case and
 * alignment are exclusive groups. `refresh` redraws the panel, for the
 * one control that changes another.
 */
export function fontControls(
  doc: Document,
  style: CellAppearance,
  patch: (change: Patch<CellAppearance>) => void,
  refresh: () => void,
): HTMLElement[] {
  const bar = doc.createElement('div');
  bar.className = 'dc-font-bar';
  const known = style.fontFamily === undefined
    || FONT_FAMILIES.some((f) => f.value === style.fontFamily);
  const sizes = FONT_SIZES.map((n) => ({ value: String(n), label: String(n) }));
  const size = style.fontSize === undefined ? undefined : String(style.fontSize);
  const underline = dropdown(
    doc,
    style.underline,
    UNDERLINE_VARIANTS.map((v) => ({ value: v, label: v })),
    (variant) => {
      patch({ underline: variant, ...(variant ? { strikethrough: undefined } : {}) });
      refresh();
    },
    { allowNone: true, width: 76 },
  );
  underline.title = 'Underline style';
  underline.classList.add('dc-underline-variant');
  bar.append(
    dropdown(
      doc,
      style.fontFamily,
      // A family a cube saved elsewhere names stays visible, not blank.
      known ? FONT_FAMILIES
        : [...FONT_FAMILIES, { value: style.fontFamily as string, label: style.fontFamily as string }],
      (fontFamily) => patch({ fontFamily }),
      { allowNone: true, width: 160 },
    ),
    dropdown(
      doc,
      size,
      size === undefined || FONT_SIZES.includes(Number(size))
        ? sizes : [...sizes, { value: size, label: size }],
      (n) => patch({ fontSize: n === undefined ? undefined : Number(n) }),
      { allowNone: true, width: 60 },
    ),
    toggle(doc, 'B', style.bold, (bold) => patch({ bold }), { title: 'Bold' }),
    toggle(doc, 'I', style.italic, (italic) => patch({ italic }), {
      title: 'Italic',
    }),
    toggle(doc, 'U', style.underline !== undefined, (on) => {
      patch({
        underline: on ? 'solid' : undefined,
        ...(on ? { strikethrough: undefined } : {}),
      });
      refresh();
    }, { title: 'Underline' }),
    underline,
    toggle(
      doc,
      'S',
      style.strikethrough,
      (strikethrough) => {
        patch({ strikethrough, ...(strikethrough ? { underline: undefined } : {}) });
        refresh();
      },
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
    // The leaf count: every group query carries a count and the tree
    // label shows it -- "EMEA (1234)" (wired 2026-09-25; it carried a
    // "Not wired" badge until then).
    field(
      doc,
      '',
      checkbox(doc, 'Show leaf count', c.showLeafCount, (v) =>
        setConfig({ showLeafCount: v }),
      ),
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
      // Empty means no limit, as upstream's Row Limit field.
      numberInput(doc, c.maxRows, (v) => setConfig({ maxRows: v }), {
        min: 1,
        max: 1_000_000,
        width: 110,
      }),
      docHint(doc, 'data-cube.grid-configuration.row-limit'),
      checkbox(
        doc,
        'Display warning when truncated',
        c.showTruncationWarning,
        (v) => setConfig({ showTruncationWarning: v }),
      ),
    ),
  );

  // THE PIVOT TOTAL: where it sits, and what its header says. Upstream
  // stores both and draws nothing (a bug, by the user's ruling); the
  // column exists here, so the settings have somewhere to live.
  const total = section(
    doc,
    'Pivot Total',
    field(
      doc,
      'Placement:',
      dropdown(
        doc,
        c.pivotStatisticColumnPlacement,
        [
          { value: 'left' as const, label: 'Left of the pivot' },
          { value: 'right' as const, label: 'Right of the pivot' },
        ],
        (pivotStatisticColumnPlacement) =>
          setConfig({ pivotStatisticColumnPlacement }),
        { allowNone: true, width: 190 },
      ),
    ),
    field(
      doc,
      'Column Name:',
      textInput(
        doc,
        c.pivotStatisticColumnName,
        (pivotStatisticColumnName) => setConfig({ pivotStatisticColumnName }),
        { placeholder: 'Total', width: 180 },
      ),
    ),
    // Ours: which comes first in a column pivot's header. Upstream
    // always puts the pivot's values over the measures.
    field(
      doc,
      'Column Headers:',
      dropdown(
        doc,
        c.pivotMeasuresFirst ? 'measures' : 'values',
        [
          { value: 'values' as const, label: 'Values, then measures' },
          { value: 'measures' as const, label: 'Measures, then values' },
        ],
        (v) => setConfig({ pivotMeasuresFirst: v === 'measures' ? true : undefined }),
        { width: 190 },
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

  const custom = a.alternateRows === true;
  const standard = !custom && a.alternateRowsStandardMode !== false;
  const highlight = section(
    doc,
    'Highlight Rows',
    // Upstream's two modes, exclusive: ticking one clears the other,
    // and both may be off.
    field(
      doc,
      '',
      checkbox(doc, 'Standard mode', standard, (v) => {
        setAppearance(v
          ? { alternateRowsStandardMode: true, alternateRows: false }
          : { alternateRowsStandardMode: false });
        ctx.refresh();
      }),
    ),
    field(
      doc,
      'Custom: Alternate color:',
      checkbox(doc, 'Custom', custom, (v) => {
        setAppearance(v
          ? { alternateRows: true, alternateRowsStandardMode: false }
          : { alternateRows: false });
        ctx.refresh();
      }),
      disable(colorPicker(doc, a.alternateRowsColor, (alternateRowsColor) =>
        setAppearance({ alternateRowsColor }),
      ), !custom),
      label(doc, 'every:'),
      disable(numberInput(
        doc,
        a.alternateRowsCount,
        (n) => setAppearance({ alternateRowsCount: n ?? 1 }),
        { min: 1, max: 100 },
      ), !custom),
      label(doc, 'rows'),
    ),
  );

  const font = section(
    doc,
    'Default Font',
    ...fontControls(doc, a, (change) => setAppearance(change), () => ctx.refresh()),
    field(
      doc,
      'Case:',
      // Cube-wide, as upstream's DataCubeConfiguration.fontCase. It was
      // a disabled control ("Per column") until 2026-09-25; a column's
      // own Case still applies on top.
      dropdown(
        doc,
        a.fontCase,
        FONT_CASES,
        (fontCase) => setAppearance({ fontCase }),
        { allowNone: true, width: 150 },
      ),
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
  );

  return panelShell(
    doc,
    'General Properties',
    title,
    tree,
    rows,
    total,
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
  alternateRowsStandardMode: true,
  alternateRows: false,
  alternateRowsCount: 1,
};

/** Disable every input a control holds (a picker is a wrapper). */
function disable<T extends HTMLElement>(el: T, off: boolean): T {
  if (!off) return el;
  const inputs = el.matches('input, select, button') ? [el] : [...el.querySelectorAll('input, select, button')];
  for (const i of inputs) (i as HTMLInputElement).disabled = true;
  return el;
}

function label(doc: Document, text: string): HTMLElement {
  const el = doc.createElement('span');
  el.className = 'dc-inline-label';
  el.textContent = text;
  return el;
}

/** Re-exported so the column panel shows the same scale names. */
export { SCALES };
export type { PanelContext };
