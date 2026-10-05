// HTML export, from the export model (export-model.ts): the grid as shown.
//
// The header is the grid's own: one row per level, with the spans the grid draws, so a pivot's
// years sit over their measures exactly as on screen. The tree reads as a tree (indented by
// depth, totals in bold); a blurred column is REDACTED; the notes (truncation, redaction) head
// the page. Styles are inline: the file is opened from disk or pasted into an email, where no
// stylesheet follows it.
//
// (Excel is export-xlsx.ts: a real OOXML workbook. The SpreadsheetML-2003 file this module
// used to write was named .xls, which Excel warns about and Excel Online and Google Sheets
// will not open -- 2026-09-29 audit.)

import type { ColumnFormat, FormatterCache } from './format.ts';
import { cased, type ExportPage, type ExportStyle, type ExportTable } from './export-model.ts';
import type { Scalar } from '../../engine-client/src/result.ts';
import { fontStack } from './style.ts';

export interface RichExportOptions {
  readonly formatters?: FormatterCache;
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** The board, when it holds charts: the page is laid out as it is, the grid in its tile, whole. */
  readonly page?: ExportPage;
}

/** Bytes as base64, for a data: URL. */
function base64(bytes: Uint8Array): string {
  let bin = '';
  for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(bin);
}

/** Escape text for an XML or HTML text node or attribute. */
export function escapeXml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/** A CSS declaration list for one cell's look. */
export function cssOf(style: ExportStyle): string {
  const out: string[] = [];
  if (style.color) out.push(`color:${style.color}`);
  if (style.background) out.push(`background:${style.background}`);
  if (style.fontFamily) out.push(`font-family:${fontStack(style.fontFamily)}`);
  if (style.fontSize !== undefined) out.push(`font-size:${style.fontSize}px`);
  if (style.bold) out.push('font-weight:600');
  if (style.italic) out.push('font-style:italic');
  const deco = [style.underline ? 'underline' : '', style.strike ? 'line-through' : ''].filter(Boolean);
  if (deco.length) out.push(`text-decoration:${deco.join(' ')}${style.underline && style.underline !== 'solid' ? ` ${style.underline}` : ''}`);
  if (style.align !== 'left') out.push(`text-align:${style.align}`);
  return out.join(';');
}

/**
 * The grid as a standalone HTML document that LOOKS like the grid: each cell's colours, font,
 * alignment and decoration as the grid resolves them (export-model.ts), its grid lines, its
 * banded and total rows, its header levels with their spans (a plain gray header). Styles are
 * inline: the file is opened from disk or pasted into an email, where no stylesheet follows.
 */
export function toHtml(table: ExportTable, options: RichExportOptions = {}): string {
  const fmt = (v: Scalar, name: string, type: string | undefined): string =>
    options.formatters ? options.formatters.format(v, options.formats?.[name], type)
      : v === null ? '' : String(v);
  const look = table.look;
  const line = `1px solid ${look.lineColor}`;

  const head = table.headerRows.length > 0
    ? table.headerRows.map((row) => `<tr>${row.map((cell) => {
      const span = (cell.colSpan > 1 ? ` colspan="${cell.colSpan}"` : '')
        + (cell.rowSpan > 1 ? ` rowspan="${cell.rowSpan}"` : '');
      return `<th scope="col"${span}>${escapeXml(cell.label)}</th>`;
    }).join('')}</tr>`).join('\n')
    : `<tr>${table.columns.map((c) => `<th scope="col">${escapeXml(c.path.join(' '))}</th>`).join('')}</tr>`;

  // ONE CLASS PER LOOK: a cube of a few thousand rows has a handful of distinct looks, and
  // writing each inline made a 5,000-row page 6MB instead of 1.3MB (2026-09-30).
  const classes = new Map<string, string>();
  const classOf = (css: string): string => {
    let name = classes.get(css);
    if (name === undefined) {
      name = `s${classes.size}`;
      classes.set(css, name);
    }
    return name;
  };
  const body = table.rows.map((row) => {
    const cells = table.columns.map((c, i) => {
      const v = row.cells[i] ?? null;
      const style = row.styles[i] ?? { align: 'left' as const };
      const text = cased(c.redacted ? String(v) : fmt(v, c.name, c.type), style.fontCase);
      const indent = c.tree && row.depth > 1 ? `;padding-left:${4 + (row.depth - 1) * 16}px` : '';
      const css = (cssOf(style) + indent).replace(/^;/, '');
      return `<td${css ? ` class="${classOf(css)}"` : ''}>${escapeXml(text)}</td>`;
    }).join('');
    return `<tr>${cells}</tr>`;
  });
  const looks = [...classes].map(([css, name]) => `  td.${name} { ${css.replace(/</g, '')}; }`).join('\n');

  const notes = table.notes.map((n) => `<p class="note">${escapeXml(n)}</p>`).join('\n');
  // THE PAGE AS THE BOARD LAYS IT OUT: a CSS grid of the board's columns, each tile at its place;
  // a chart as its picture, the grid in its tile, whole (its rows grow the tile, never clip it)
  const page = options.page;
  const layout = (grid: string): string => (page ? `<div class="page" style="grid-template-columns:repeat(${page.cols},minmax(0,1fr))">
${page.tiles.map((t) => `<section class="tile" style="grid-column:${t.x + 1} / span ${t.w};grid-row:${t.y + 1} / span ${t.h}">
<h2>${escapeXml(t.title)}</h2>
${t.kind === 'grid' ? grid : t.picture
    ? `<img alt="${escapeXml(t.title)}" width="${t.picture.width}" height="${t.picture.height}" src="data:image/png;base64,${base64(t.picture.png)}">`
    : '<p class="note">(this chart had not drawn)</p>'}
</section>`).join('\n')}
</div>` : grid);
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${escapeXml(table.title)}</title>
<style>
  body { font: 13px/1.4 ${fontStack(look.fontFamily)}; margin: 24px; color: ${look.color}; }
  table { border-collapse: collapse; font-family: ${fontStack(look.fontFamily)}; font-size: ${look.fontSize}px;
    font-variant-numeric: tabular-nums; border: 1px solid #e5e5e5; }
  th { background: ${look.headerBackground}; color: ${look.headerColor}; font-weight: 500; text-align: center;
    padding: 2px 6px; border: 1px solid #e5e5e5; }
  td { padding: 1px 6px; white-space: nowrap;${look.verticalLines ? ` border-right: ${line};` : ''}${look.horizontalLines ? ` border-bottom: ${line};` : ''} }
  p.note { color: #555; margin: 4px 0; }
  .page { display: grid; gap: 12px; grid-auto-rows: minmax(4px, auto); align-items: start; }
  .tile { border: 1px solid #e5e5e5; padding: 6px; overflow: visible; min-width: 0; }
  .tile h2 { font-size: 13px; font-weight: 600; margin: 0 0 6px; }
  .tile img { width: 100%; height: auto; display: block; }
${looks}
</style>
</head>
<body>
<h1>${escapeXml(table.title)}</h1>
${notes}
${layout(`<table>
<thead>
${head}
</thead>
<tbody>
${body.join('\n')}
</tbody>
</table>`)}
</body>
</html>
`;
}
