// A GRID EXPORTED, as its Export menu makes the file (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D): the core every way
// of asking shares -- the screen's menu (app.ts), and Python's `grid.export()`, which runs it headless
// (headless/grid.ts `gridExport`). A function of the grid's view, its column model, its configuration and the page's
// board; the window only asks the person (the leakage warning) and hands the file over.

import type { Scalar } from '../../engine-client/src/result.ts';
import { columnConfig, labelFor, leafFormats, renderFormats, toColumnAppearance, type CubeConfiguration } from './config.ts';
import type { CubeView } from './cube.ts';
import { toPdf, toPlainText } from './export-doc.ts';
import { exportTable, type ExportPage } from './export-model.ts';
import { toHtml } from './export-rich.ts';
import { exportCsv, exportFileName } from './export.ts';
import { toXlsx, XLSX_MIME } from './export-xlsx.ts';
import type { FormatterCache, ColumnFormat } from './format.ts';
import type { ColumnModel, LeafColumn } from './grid/columns.ts';
import { isPivotTotalColumn, rowColumns, type CubeSnapshot } from './snapshot.ts';
import { columnRange, heatColour, type HeatmapRange, type HeatmapSpec } from './style.ts';
import type { TreeRow } from './tree.ts';

/** The files a grid exports to, as its Export menu offers them. */
export type ExportKind = 'csv' | 'excel' | 'html' | 'text' | 'pdf';
const EVERY_EXPORT: Readonly<Record<ExportKind, true>> = { csv: true, excel: true, html: true, text: true, pdf: true };
/** Every kind, once (a Record over ExportKind: a kind added there is refused here until it is listed). */
export const EXPORT_KINDS: readonly ExportKind[] = Object.keys(EVERY_EXPORT) as ExportKind[];
/** Whether `kind` names a file a grid exports to. */
export const isExportKind = (kind: string): kind is ExportKind => Object.hasOwn(EVERY_EXPORT, kind);

/** A file, named and typed, its content text or bytes. */
export interface ExportedFile {
  readonly name: string;
  readonly mime: string;
  readonly content: string | Uint8Array;
}

/**
 * Each column's format as the grid renders it: the configured ones, then what is ON SCREEN by its own leaf's type -- a
 * measure's result type, which aggregation changes (the average of an Integer is a Float), never its source's -- and
 * by its kind (a numeric dimension reads as written, no thousands separators).
 */
export function columnFormats(config: CubeConfiguration, snapshot: CubeSnapshot, shown: CubeSnapshot,
  view: CubeView | undefined): Record<string, ColumnFormat> {
  const out: Record<string, ColumnFormat> = { ...renderFormats(config, snapshot) };
  if (!view) return out;
  const kinds = new Map(rowColumns(shown).map((c) => [c.name, c.kind]));
  const byLeaf = leafFormats(config, view.columns.leaves.map((leaf) => ({
    ...leaf, type: view.rows.columns[leaf.index]?.type, kind: kinds.get(leaf.name),
  })));
  for (const [name, format] of Object.entries(byLeaf)) {
    if (format) out[name] = format;
    else delete out[name];
  }
  return out;
}

/** A leaf's measured heatmap scale, by depth (`-1`: one fixed scale for every depth). */
export interface Heatmap {
  readonly spec: HeatmapSpec;
  readonly byDepth: Map<number, HeatmapRange>;
  readonly type: string | undefined;
}

/**
 * A leaf's heatmap setting: its own, else its measure's -- a pivoted leaf is named for its measure, so a setting made
 * on `notional` reaches every `2021__|__notional` the pivot produced. A pivot TOTAL is a column of its own (the user,
 * 2026-10-18): it sums every pivot value, on its own scale, so it takes only its own heatmap, never its measure's.
 */
export function heatmapSetting(config: CubeConfiguration, view: CubeView | undefined, leafName: string): HeatmapSpec | undefined {
  const own = columnConfig(config, leafName).heatmap;
  if (own || isPivotTotalColumn(leafName)) return own;
  const measure = view?.pivot?.columns.find((c) => c.name === leafName)?.measure.name;
  return measure !== undefined ? columnConfig(config, measure).heatmap : undefined;
}

/**
 * The heatmaps of a view, each measured over its WHOLE column at each depth (so a cell is coloured against its
 * siblings, and the colours never change as the grid scrolls); a fixed range the person set applied whole.
 */
export function measureHeatmaps(view: CubeView, treeRows: readonly TreeRow[],
  setting: (leafName: string) => HeatmapSpec | undefined): Map<number, Heatmap> {
  const out = new Map<number, Heatmap>();
  for (const leaf of view.columns.leaves) {
    const spec = setting(leaf.name);
    if (!spec) continue;
    const values = view.rows.columns[leaf.index]?.values ?? [];
    // placed on the scale by the leaf's COMPILER type: a date or a text column has none
    const type = view.rows.columns[leaf.index]?.type;
    if (spec.range) {
      out.set(leaf.index, { spec, byDepth: new Map([[-1, spec.range]]), type });
      continue;
    }
    const atDepth = new Map<number, Scalar[]>();
    values.forEach((value, row) => {
      const depth = treeRows[row]?.depth ?? 0;
      const bucket = atDepth.get(depth);
      if (bucket) bucket.push(value);
      else atDepth.set(depth, [value]);
    });
    const byDepth = new Map<number, HeatmapRange>();
    for (const [depth, bucket] of atDepth) {
      const range = columnRange(bucket, type);
      if (range) byDepth.set(depth, range);
    }
    if (byDepth.size > 0) out.set(leaf.index, { spec, byDepth, type });
  }
  return out;
}

/** A cell's heatmap colour, measured against its own depth's scale; null when it has none. */
export function heatmapColour(heatmaps: ReadonlyMap<number, Heatmap>, treeRows: readonly TreeRow[], leaf: LeafColumn,
  row: number, value: Scalar): string | null {
  const heat = heatmaps.get(leaf.index);
  if (!heat) return null;
  const range = heat.byDepth.get(-1) ?? heat.byDepth.get(treeRows[row]?.depth ?? 0);
  return heatColour(value, heat.spec, range ?? null, heat.type);
}

/** What an export is made from: the grid as it is now. */
export interface ExportOf {
  readonly view: CubeView;
  /** The column model the grid DRAWS (shown-columns.ts). */
  readonly model: ColumnModel;
  readonly config: CubeConfiguration;
  readonly formats: Readonly<Record<string, ColumnFormat>>;
  readonly formatters: FormatterCache;
  /** A cell's heatmap colour, as the grid draws it. */
  readonly cellBackground: (leaf: LeafColumn, row: number, value: Scalar) => string | null;
  /** The page's board, when it has charts: each format carries them where the board puts them. */
  readonly page?: ExportPage;
  /** The moment it is exported: its file's name says it, so a second export never overwrites the first. */
  readonly at: Date;
}

/** THE EXPORT: what the grid shows, once, for every format (export-model.ts), then written as `kind`. */
export function renderExport(kind: ExportKind, o: ExportOf): ExportedFile {
  const title = o.config.reportTitle ?? 'cube';
  const base = exportFileName(title, o.at);
  // its leaves in its order under its headers, the tree's depth, blurred columns REDACTED, the notes a reader needs
  const table = exportTable({
    title,
    rows: o.view.rows,
    model: o.model,
    treeRows: o.view.treeRows,
    groupLabels: o.view.snapshot.rows.map((name) => labelFor(o.config, name)),
    truncated: o.view.truncated.length > 0,
    ...(o.config.maxRows !== undefined ? { maxRows: o.config.maxRows } : {}),
    // and how the grid DRAWS it: the same appearance and heatmaps, so the file looks like it
    appearance: o.config.appearance,
    columnAppearance: toColumnAppearance(o.config),
    cellBackground: o.cellBackground,
  });
  const doc = { formatters: o.formatters, formats: o.formats, ...(o.page ? { page: o.page } : {}) };
  switch (kind) {
    case 'csv':
      // raw values: CSV is the one that is computed on again
      return { name: `${base}.csv`, mime: 'text/csv', content: exportCsv(table) };
    case 'excel':
      // a real workbook: numbers as numbers in the column's own format, the tree as outlines
      return { name: `${base}.xlsx`, mime: XLSX_MIME, content: toXlsx(table, { formats: o.formats, at: o.at, ...(o.page ? { page: o.page } : {}) }) };
    case 'html':
      // Formatted: a page exists to be read, so it says what the screen says.
      return { name: `${base}.html`, mime: 'text/html', content: toHtml(table, doc) };
    case 'text':
      // Formatted, not raw: plain text exists to be READ -- pasted into a message or a ticket.
      return { name: `${base}.txt`, mime: 'text/plain', content: toPlainText(table, doc) };
    case 'pdf':
      return { name: `${base}.pdf`, mime: 'application/pdf', content: toPdf(table, doc) };
  }
}
