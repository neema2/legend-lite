// HTML and Excel export.
//
// DataCube offers HTML, plain text, PDF, Excel and CSV. CSV and
// plain text already exist in export.ts; this adds the two that carry
// structure.
//
// Excel is written as SpreadsheetML -- a single XML document Excel
// opens natively -- rather than as .xlsx. An .xlsx is a zip of
// several XML parts plus a relationship graph, which needs a zip
// implementation in the browser to produce something Excel reads no
// better. SpreadsheetML keeps types and number formats, which is the
// part that actually matters: a CSV turns every figure back into
// text for Excel to re-guess.
//
// Both exporters take the RENDERED text for labels and the RAW value
// for numbers. That is deliberate: a header should read as the user
// configured it, while a number must arrive as a number or the
// spreadsheet cannot sum it.

import type { ColumnFormat, FormatterCache } from './format.ts';
import type { ResultTable, Scalar } from './result.ts';

export interface RichExportOptions {
  readonly title?: string;
  readonly formatters?: FormatterCache;
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** Restrict and order columns; defaults to all, in table order. */
  readonly columns?: readonly string[];
  /** Header labels, where they differ from the column names. */
  readonly labels?: Readonly<Record<string, string>>;
}

function chosen(table: ResultTable, options: RichExportOptions) {
  return options.columns
    ? options.columns
        .map((n) => table.columns.find((c) => c.name === n))
        .filter((c): c is NonNullable<typeof c> => c !== undefined)
    : table.columns;
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

function isNumber(v: Scalar): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

/**
 * The grid as a standalone HTML document.
 *
 * Styles are inlined rather than linked, because the file is opened
 * from disk or pasted into an email where no stylesheet follows it.
 */
export function toHtml(
  table: ResultTable,
  options: RichExportOptions = {},
): string {
  const cols = chosen(table, options);
  const title = options.title ?? 'DataCube export';
  const fmt = (v: Scalar, name: string): string =>
    options.formatters
      ? options.formatters.format(v, options.formats?.[name])
      : v === null
        ? ''
        : String(v);

  const head = cols
    .map(
      (c) =>
        `<th scope="col">${escapeXml(options.labels?.[c.name] ?? c.name)}</th>`,
    )
    .join('');

  const rows: string[] = [];
  for (let r = 0; r < table.rowCount; r++) {
    const cells = cols
      .map((c) => {
        const v = c.values[r] ?? null;
        // Numbers right-align; text does not. Doing it per cell
        // rather than per column keeps a mixed column readable.
        const cls = isNumber(v) ? ' class="n"' : '';
        return `<td${cls}>${escapeXml(fmt(v, c.name))}</td>`;
      })
      .join('');
    rows.push(`<tr>${cells}</tr>`);
  }

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>${escapeXml(title)}</title>
<style>
  body { font: 13px/1.4 ui-sans-serif, system-ui, sans-serif; margin: 24px; }
  table { border-collapse: collapse; font-variant-numeric: tabular-nums; }
  th, td { border: 1px solid #ddd; padding: 4px 8px; text-align: left; }
  th { background: #f4f4f4; font-weight: 600; }
  td.n { text-align: right; }
</style>
</head>
<body>
<h1>${escapeXml(title)}</h1>
<table>
<thead><tr>${head}</tr></thead>
<tbody>
${rows.join('\n')}
</tbody>
</table>
</body>
</html>
`;
}

/**
 * The grid as SpreadsheetML, which Excel opens natively.
 *
 * Numbers are written with a Number type and their RAW value, so the
 * spreadsheet can sum them; only labels carry rendered text. A CSV
 * cannot make that distinction, which is why "export to Excel" and
 * "export to CSV" are genuinely different actions rather than the
 * same file with a different extension.
 */
export function toSpreadsheetML(
  table: ResultTable,
  options: RichExportOptions = {},
): string {
  const cols = chosen(table, options);
  const title = options.title ?? 'DataCube';

  const cell = (v: Scalar, name: string): string => {
    if (v === null) return '<Cell/>';
    if (isNumber(v)) {
      return `<Cell><Data ss:Type="Number">${v}</Data></Cell>`;
    }
    if (v instanceof Date) {
      return (
        `<Cell ss:StyleID="d"><Data ss:Type="DateTime">` +
        `${v.toISOString().replace(/\.\d+Z$/, '')}</Data></Cell>`
      );
    }
    const text = options.formatters
      ? options.formatters.format(v, options.formats?.[name])
      : String(v);
    return `<Cell><Data ss:Type="String">${escapeXml(text)}</Data></Cell>`;
  };

  const header = cols
    .map(
      (c) =>
        `<Cell ss:StyleID="h"><Data ss:Type="String">` +
        `${escapeXml(options.labels?.[c.name] ?? c.name)}</Data></Cell>`,
    )
    .join('');

  const body: string[] = [];
  for (let r = 0; r < table.rowCount; r++) {
    body.push(
      `<Row>${cols.map((c) => cell(c.values[r] ?? null, c.name)).join('')}</Row>`,
    );
  }

  return `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<Styles>
 <Style ss:ID="h"><Font ss:Bold="1"/>
  <Interior ss:Color="#F4F4F4" ss:Pattern="Solid"/></Style>
 <Style ss:ID="d"><NumberFormat ss:Format="Short Date"/></Style>
</Styles>
<Worksheet ss:Name="${escapeXml(title).slice(0, 31)}">
<Table>
<Row>${header}</Row>
${body.join('\n')}
</Table>
</Worksheet>
</Workbook>
`;
}
