// What an export carries: the grid AS SHOWN, once, for every format.
//
// Every exporter used to take `view.rows` -- every column the query returned -- so a hidden
// column was exported, a BLURRED column went out in the clear, the grouping columns the tree
// hides came back, the user's order was lost and headers were internal names (`__tree`,
// `2021__|__total`). A file that says something the screen does not is the one kind of export
// bug nobody catches by looking (2026-09-29 audit, docs/BI_AND_ETL_PLAN_2026_09_29.md §1.1).
//
// So the table is built HERE from the grid's own column model -- the leaves it draws, in its
// order, with its labels and its header rows -- and the view's tree rows, and every format
// renders this and nothing else. Redaction happens here too, once: a blurred column's cells
// become REDACTED before any format sees a value, so no renderer can forget it.

import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import type { ColumnModel, HeaderCell, LeafColumn } from './grid/columns.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import {
  coloursFor, highlightBand, isAlternateRow, mergeAppearance, valueState,
  type CellAppearance, type GridAppearance, type TextAlign, type UnderlineVariant,
} from './style.ts';
import type { FontCase } from './format.ts';
import type { TreeRow } from './tree.ts';
import { TREE_COLUMN } from './treeview.ts';

/** What a blurred cell becomes in every export: plainly not data, and the same everywhere. */
export const REDACTED = '[REDACTED]';

export interface ExportColumn {
  /** The column's identity (a format is looked up by it). */
  readonly name: string;
  /** The header as the grid shows it, one segment per header level (a pivot's year, then its measure). */
  readonly path: readonly string[];
  /** The compiler's type; a redacted column is text, whatever it was. */
  readonly type: string | undefined;
  /** Blurred on screen: every cell is REDACTED. */
  readonly redacted: boolean;
  /** The tree column: its cells are group labels, indented by the row's depth. */
  readonly tree: boolean;
}

/** What a row is, which a flat file must say in words and a workbook as an outline level. */
export type ExportRowKind = 'row' | 'group' | 'total' | 'detail';

/**
 * One cell's look, resolved exactly as the grid resolves it (style.ts): the cube's appearance
 * with the column's over it, the colour its VALUE earns (normal, negative, zero), a heatmap
 * over that, the total row's shading and the band under everything. Every rich format draws
 * this, so a workbook, a page and a PDF look like the grid and like each other.
 */
export interface ExportStyle {
  readonly color?: string;
  readonly background?: string;
  readonly fontFamily?: string;
  /** In CSS pixels, as the grid sets it. */
  readonly fontSize?: number;
  readonly bold?: boolean;
  readonly italic?: boolean;
  readonly underline?: UnderlineVariant;
  readonly strike?: boolean;
  readonly align: TextAlign;
  readonly fontCase?: FontCase;
}

/** The grid-wide look: its font, its lines, its header (plain gray, whatever the grid's is). */
export interface ExportLook {
  readonly fontFamily: string;
  readonly fontSize: number;
  readonly color: string;
  readonly headerBackground: string;
  readonly headerColor: string;
  readonly horizontalLines: boolean;
  readonly verticalLines: boolean;
  readonly lineColor: string;
}

/** The grid's own defaults (grid.css, theme.css), for what an appearance leaves unsaid. */
export const GRID_LOOK = {
  fontFamily: 'Roboto',
  fontSize: 12,
  color: '#181d1f',
  headerBackground: '#f5f5f5',
  headerColor: '#000000',
  lineColor: '#d4d4d4',
  totalBackground: '#fafafa',
  bandColor: '#d7e0eb',
} as const;

export interface ExportRow {
  readonly cells: readonly Scalar[];
  /** Each cell's look, parallel to `cells`. */
  readonly styles: readonly ExportStyle[];
  /** 1-based presentation depth in the tree; 1 for a flat cube. */
  readonly depth: number;
  readonly kind: ExportRowKind;
}

export interface ExportTable {
  readonly title: string;
  readonly columns: readonly ExportColumn[];
  /** The header as the grid draws it: one row per level, spans included (HTML, the workbook). */
  readonly headerRows: readonly (readonly HeaderCell[])[];
  readonly rows: readonly ExportRow[];
  /** Whether the tree column is present (a grouped cube). */
  readonly grouped: boolean;
  /** What a reader must be told about this file, in words: truncation, redaction. */
  readonly notes: readonly string[];
  readonly look: ExportLook;
}

export interface ExportSource {
  readonly title: string;
  /** The rows the grid holds (the view's). */
  readonly rows: ResultTable;
  /** The column model the grid DRAWS: visible leaves, in order, with labels and header rows. */
  readonly model: ColumnModel;
  /** Tree metadata, parallel to `rows`; empty for a flat cube. */
  readonly treeRows: readonly TreeRow[];
  /** The row dimensions' labels, for the tree column's header ("region / desk"). */
  readonly groupLabels: readonly string[];
  /** Levels cut at the row limit, when any. */
  readonly truncated: boolean;
  /** The row limit, for the note. */
  readonly maxRows?: number;
  /** The grid's appearance, as it draws the cells (absent: the grid's defaults, uncoloured). */
  readonly appearance?: GridAppearance;
  readonly columnAppearance?: Readonly<Record<string, CellAppearance>>;
  /** A cell's heatmap colour, as the grid asks for it: (leaf, row, value). */
  readonly cellBackground?: (leaf: LeafColumn, row: number, value: Scalar) => string | null;
}

/** The label the grid shows over one leaf, per header level (from the header cells that cover it). */
function leafPath(model: ColumnModel, at: number, leaf: LeafColumn): string[] {
  const out: string[] = [];
  for (const row of model.headerRows) {
    const cell = row.find((c) => at >= c.colStart && at < c.colStart + c.colSpan);
    if (cell && out[out.length - 1] !== cell.label) out.push(cell.label);
  }
  if (out.length === 0) out.push(leaf.label ?? leaf.name);
  return out;
}

export function exportTable(source: ExportSource): ExportTable {
  const { rows, model, treeRows } = source;
  const index = new Map(rows.columns.map((c, i) => [c.name, i]));
  const grouped = model.leaves.some((l) => l.name === TREE_COLUMN);
  const treeHeader = source.groupLabels.join(' / ') || 'Group';

  const columns: ExportColumn[] = model.leaves.map((leaf, i) => {
    const tree = leaf.name === TREE_COLUMN;
    const redacted = leaf.blurred === true && !tree;
    const path = tree ? [treeHeader] : leafPath(model, i, leaf).filter((s) => s !== '');
    return {
      name: leaf.name,
      path: path.length > 0 ? path : [leaf.label ?? leaf.name],
      type: redacted ? 'String' : leaf.type,
      redacted,
      tree,
    };
  });

  const headerRows = model.headerRows.map((row) => row.map((cell) =>
    (cell.leafIndex !== undefined && model.leaves[cell.leafIndex]?.name === TREE_COLUMN)
      || (cell.colStart === 0 && grouped && cell.label === '')
      ? { ...cell, label: treeHeader }
      : cell));

  const out: ExportRow[] = [];
  for (let r = 0; r < rows.rowCount; r++) {
    const meta = treeRows[r];
    const kind: ExportRowKind = !meta ? 'row'
      : meta.isTotal ? 'total'
        : meta.isDetail ? 'detail'
          : meta.isGroup ? 'group' : 'row';
    const cells = columns.map((c): Scalar => {
      if (c.redacted) return REDACTED;
      const at = index.get(c.name);
      const v = at === undefined ? null : (rows.columns[at]?.values[r] ?? null);
      if (c.tree && kind === 'total' && meta?.level === 0 && (v === null || v === '')) return 'Total';
      return v;
    });
    const band = highlightBand(source.appearance ?? {});
    const banded = band > 0 && isAlternateRow(r, band)
      ? (source.appearance?.alternateRows ? source.appearance.alternateRowsColor : undefined) ?? GRID_LOOK.bandColor
      : undefined;
    const styles = model.leaves.map((leaf, i): ExportStyle => {
      const c = columns[i]!;
      const a = mergeAppearance(source.appearance ?? {}, source.columnAppearance?.[leaf.name]);
      const v = c.redacted ? null : cells[i] ?? null;
      const { foreground, background } = c.redacted ? {} : coloursFor(a, valueState(v, c.type));
      // the grid's order: the band, the total's shading over it, the value's colour, a heatmap last
      const heat = c.redacted || !source.cellBackground ? null : source.cellBackground(leaf, r, v);
      const bg = heat ?? background ?? (kind === 'total' ? GRID_LOOK.totalBackground : banded);
      return {
        ...(c.redacted ? { color: '#a3a3a3', italic: true } : foreground ? { color: foreground } : {}),
        ...(bg ? { background: bg } : {}),
        ...(a.fontFamily ? { fontFamily: a.fontFamily } : {}),
        ...(a.fontSize !== undefined ? { fontSize: a.fontSize } : {}),
        ...(a.bold || kind === 'total' ? { bold: true } : {}),
        ...(a.italic && !c.redacted ? { italic: true } : {}),
        ...(a.underline ? { underline: a.underline } : {}),
        ...(a.strikethrough ? { strike: true } : {}),
        // the grid's own default is left for every column, figures included
        align: c.tree ? 'left' : a.textAlign ?? 'left',
        ...(a.fontCase ? { fontCase: a.fontCase } : {}),
      };
    });
    out.push({ cells, styles, depth: meta?.depth ?? 1, kind });
  }

  const notes: string[] = [];
  const hidden = columns.filter((c) => c.redacted).map((c) => c.path.join(' '));
  if (hidden.length > 0) {
    notes.push(`Blurred on screen, so ${REDACTED} here: ${hidden.join(', ')}.`);
  }
  if (source.truncated) {
    notes.push(source.maxRows !== undefined
      ? `Truncated: showing the first ${source.maxRows.toLocaleString(UI_LOCALE)} rows of a level, as the grid does.`
      : 'Truncated at the row limit, as the grid is.');
  }
  const g = source.appearance ?? {};
  const look: ExportLook = {
    fontFamily: g.fontFamily ?? GRID_LOOK.fontFamily,
    fontSize: g.fontSize ?? GRID_LOOK.fontSize,
    color: g.normalForeground ?? GRID_LOOK.color,
    headerBackground: GRID_LOOK.headerBackground,
    headerColor: GRID_LOOK.headerColor,
    horizontalLines: g.showHorizontalGridLines === true,
    verticalLines: g.showVerticalGridLines !== false,
    lineColor: g.gridLineColor ?? GRID_LOOK.lineColor,
  };
  return { title: source.title, columns, headerRows, rows: out, grouped, notes, look };
}

/** One header line for a format with a single header row: the levels joined ("2021 notional"). */
export function flatHeader(column: ExportColumn): string {
  return column.path.join(' ');
}

/** Text as the grid shows it after its letter case (CSS text-transform, done here for files). */
export function cased(text: string, fontCase: FontCase | undefined): string {
  if (fontCase === 'uppercase') return text.toUpperCase();
  if (fontCase === 'lowercase') return text.toLowerCase();
  if (fontCase === 'capitalize') return text.replace(/(^|\s)(\S)/g, (_m, sp: string, ch: string) => sp + ch.toUpperCase());
  return text;
}

/**
 * THE PAGE, when the board holds charts: every tile where the board puts it (in its column and
 * row units), each chart as the picture it shows. An export of a page with charts carries them
 * all, arranged as the board arranges them, and the WHOLE grid -- never the part of it a tile
 * happened to show. Absent when the board is the grid alone.
 */
export interface ExportPage {
  /** The board's columns (12). */
  readonly cols: number;
  readonly tiles: readonly ExportTile[];
}

export interface ExportTile {
  readonly id: string;
  readonly kind: 'grid' | 'chart';
  readonly title: string;
  readonly x: number;
  readonly y: number;
  readonly w: number;
  readonly h: number;
  /** A chart's picture (absent when it has not drawn, or could not be read). */
  readonly picture?: {
    readonly width: number;
    readonly height: number;
    readonly pixelWidth: number;
    readonly pixelHeight: number;
    readonly png: Uint8Array;
    readonly jpeg: Uint8Array;
  };
}
