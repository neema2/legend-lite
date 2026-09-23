// Plot and treemap, the two visualisations in DataCube's menu.
//
// Recorded once as "a charting surface, not a grid feature", and
// that was the wrong call for the same reason the PDF was: what a
// CUBE needs is not a charting library, it is two specific pictures
// of one aggregated result that is already small -- the row cap
// means a thousand rows at most, and a readable chart wants far
// fewer. Hand-written SVG covers it with no dependency, renders in
// node and the browser alike, and is inspectable, so the tests can
// assert geometry instead of trusting a library to have drawn
// something.
//
// Both take the CURRENT VIEW, which is the point: they visualise
// what the grid is showing, filters and pivots and all, rather than
// re-querying and risking a picture that disagrees with the numbers
// beside it.

import type { ResultTable, Scalar } from './result.ts';
import { TREE_COLUMN } from './treeview.ts';

export interface ChartPoint {
  readonly label: string;
  readonly value: number;
}

export interface ChartOptions {
  readonly title?: string;
  /** Column to take labels from. Defaults to the tree/first text column. */
  readonly labelColumn?: string;
  /** Column to take values from. Defaults to the first numeric column. */
  readonly valueColumn?: string;
  readonly width?: number;
  readonly height?: number;
  /** Cap on the number of points drawn. */
  readonly limit?: number;
}

const WIDTH = 760;
const HEIGHT = 420;
const PAD = { top: 36, right: 16, bottom: 72, left: 72 };
const PALETTE = [
  '#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
  '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac',
];

function isNumeric(v: Scalar): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

/**
 * Pick the label and value columns out of a result.
 *
 * Chosen rather than demanded, because a chart reached from a
 * right-click has no opportunity to ask. The tree column is the
 * label when there is one -- in a grouped cube it is the only column
 * carrying a dimension -- and the first column with real numbers in
 * it is the value.
 */
export function chartData(
  table: ResultTable,
  options: ChartOptions = {},
): ChartPoint[] {
  const byName = (n: string | undefined) =>
    n === undefined ? undefined : table.columns.find((c) => c.name === n);

  const labelCol =
    byName(options.labelColumn)
    ?? table.columns.find((c) => c.name === TREE_COLUMN)
    ?? table.columns.find((c) => c.values.some((v) => typeof v === 'string'));

  const valueCol =
    byName(options.valueColumn)
    ?? table.columns.find(
      (c) => c.name !== labelCol?.name && c.values.some(isNumeric),
    );

  if (!valueCol) return [];

  const out: ChartPoint[] = [];
  const limit = options.limit ?? 50;
  for (let r = 0; r < table.rowCount && out.length < limit; r++) {
    const v = valueCol.values[r] ?? null;
    if (!isNumeric(v)) continue;
    const raw = labelCol?.values[r] ?? null;
    out.push({ label: raw === null ? '' : String(raw), value: v });
  }
  return out;
}

function esc(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/** Trim a label to fit, since SVG will not do it. */
function clip(s: string, chars: number): string {
  return s.length <= chars ? s : `${s.slice(0, Math.max(1, chars - 1))}…`;
}

function svg(w: number, h: number, body: string, title?: string): string {
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}"`,
    ` width="${w}" height="${h}" font-family="Roboto, system-ui, sans-serif"`,
    ` role="img" aria-label="${esc(title ?? 'chart')}">`,
    title
      ? `<text x="${w / 2}" y="22" text-anchor="middle" font-size="14"`
        + ` fill="#181d1f">${esc(title)}</text>`
      : '',
    body,
    '</svg>',
  ].join('');
}

/**
 * A bar chart.
 *
 * Bars grow from a ZERO baseline rather than from the bottom of the
 * plot, so a negative value reads as negative. A chart whose axis
 * silently starts at the minimum is the classic way to make a small
 * difference look enormous, and in a financial grid that is not a
 * cosmetic issue.
 */
export function toBarChart(
  table: ResultTable,
  options: ChartOptions = {},
): string {
  const data = chartData(table, options);
  const w = options.width ?? WIDTH;
  const h = options.height ?? HEIGHT;
  const plotW = w - PAD.left - PAD.right;
  const plotH = h - PAD.top - PAD.bottom;

  if (data.length === 0) {
    return svg(
      w,
      h,
      `<text x="${w / 2}" y="${h / 2}" text-anchor="middle" font-size="12"`
      + ` fill="#6b7280">nothing numeric to plot</text>`,
      options.title,
    );
  }

  const values = data.map((d) => d.value);
  // The domain always INCLUDES zero, for the reason above.
  const max = Math.max(0, ...values);
  const min = Math.min(0, ...values);
  const span = max - min || 1;
  const y = (v: number): number => PAD.top + ((max - v) / span) * plotH;
  const zeroY = y(0);

  const slot = plotW / data.length;
  const barW = Math.max(1, slot * 0.7);
  const parts: string[] = [];

  // Axis and zero line.
  parts.push(
    `<line x1="${PAD.left}" y1="${PAD.top}" x2="${PAD.left}"`
    + ` y2="${PAD.top + plotH}" stroke="#d4d4d4"/>`,
    `<line x1="${PAD.left}" y1="${zeroY.toFixed(2)}"`
    + ` x2="${PAD.left + plotW}" y2="${zeroY.toFixed(2)}" stroke="#a3a3a3"/>`,
  );
  // Value labels at the extremes, so the scale is readable.
  parts.push(
    `<text x="${PAD.left - 6}" y="${(PAD.top + 4).toFixed(2)}"`
    + ` text-anchor="end" font-size="10" fill="#6b7280">${esc(fmt(max))}</text>`,
    `<text x="${PAD.left - 6}" y="${(PAD.top + plotH).toFixed(2)}"`
    + ` text-anchor="end" font-size="10" fill="#6b7280">${esc(fmt(min))}</text>`,
  );

  data.forEach((d, i) => {
    const x = PAD.left + i * slot + (slot - barW) / 2;
    const top = Math.min(y(d.value), zeroY);
    const height = Math.abs(y(d.value) - zeroY);
    const colour = d.value < 0 ? '#e15759' : PALETTE[i % PALETTE.length];
    parts.push(
      `<rect x="${x.toFixed(2)}" y="${top.toFixed(2)}"`
      + ` width="${barW.toFixed(2)}" height="${Math.max(height, 0.5).toFixed(2)}"`
      + ` fill="${colour}"><title>${esc(`${d.label}: ${fmt(d.value)}`)}</title>`
      + `</rect>`,
    );
    parts.push(
      `<text x="${(x + barW / 2).toFixed(2)}"`
      + ` y="${(PAD.top + plotH + 14).toFixed(2)}" text-anchor="end"`
      + ` font-size="10" fill="#6b7280"`
      + ` transform="rotate(-45 ${(x + barW / 2).toFixed(2)}`
      + ` ${(PAD.top + plotH + 14).toFixed(2)})">`
      + `${esc(clip(d.label, 18))}</text>`,
    );
  });

  return svg(w, h, parts.join(''), options.title);
}

function fmt(n: number): string {
  if (!Number.isFinite(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}b`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}m`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return String(Math.round(n * 100) / 100);
}

interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

/**
 * Squarified treemap layout.
 *
 * Slice-and-dice is three lines and produces slivers nobody can
 * compare; squarified keeps rectangles close to square, which is the
 * entire reason a treemap is readable. The algorithm packs a row
 * until adding another item would make the worst aspect ratio worse,
 * then commits it and recurses on what is left.
 *
 * NON-POSITIVE VALUES ARE DROPPED, not clamped. A treemap encodes
 * magnitude as AREA, and a negative area does not exist -- drawing a
 * zero-size box would silently hide the row, and drawing its
 * absolute value would state the opposite of the truth. Dropping is
 * the only honest option, and the caller is told how many went.
 */
export function squarify(
  values: readonly number[],
  bounds: Rect,
): Rect[] {
  const out: Rect[] = new Array<Rect>(values.length);
  const total = values.reduce((a, b) => a + b, 0);
  if (total <= 0) return out.fill({ x: 0, y: 0, w: 0, h: 0 });

  // Work in AREA units scaled to the bounds.
  const scale = (bounds.w * bounds.h) / total;
  const items = values
    .map((v, i) => ({ area: v * scale, i }))
    .sort((a, b) => b.area - a.area);

  let rect: Rect = { ...bounds };
  let row: { area: number; i: number }[] = [];

  const worst = (
    candidate: readonly { area: number }[],
    side: number,
  ): number => {
    if (candidate.length === 0 || side <= 0) return Infinity;
    const sum = candidate.reduce((a, b) => a + b.area, 0);
    if (sum <= 0) return Infinity;
    const max = Math.max(...candidate.map((c) => c.area));
    const min = Math.min(...candidate.map((c) => c.area));
    const s2 = sum * sum;
    const w2 = side * side;
    return Math.max((w2 * max) / s2, s2 / (w2 * min));
  };

  const commit = (): void => {
    const side = Math.min(rect.w, rect.h);
    const sum = row.reduce((a, b) => a + b.area, 0);
    if (sum <= 0) {
      row = [];
      return;
    }
    const thickness = sum / (side || 1);
    let offset = 0;
    const horizontal = rect.w >= rect.h;
    for (const item of row) {
      const length = item.area / (thickness || 1);
      out[item.i] = horizontal
        ? { x: rect.x, y: rect.y + offset, w: thickness, h: length }
        : { x: rect.x + offset, y: rect.y, w: length, h: thickness };
      offset += length;
    }
    // Shrink the remaining area by the strip just placed.
    rect = horizontal
      ? { x: rect.x + thickness, y: rect.y, w: rect.w - thickness, h: rect.h }
      : { x: rect.x, y: rect.y + thickness, w: rect.w, h: rect.h - thickness };
    row = [];
  };

  for (const item of items) {
    const side = Math.min(rect.w, rect.h);
    if (row.length > 0 && worst([...row, item], side) > worst(row, side)) {
      commit();
    }
    row.push(item);
  }
  commit();

  // Anything the layout could not place (zero area) gets an empty box
  // rather than an undefined hole.
  for (let i = 0; i < out.length; i++) {
    out[i] ??= { x: bounds.x, y: bounds.y, w: 0, h: 0 };
  }
  return out;
}

export function toTreemap(
  table: ResultTable,
  options: ChartOptions = {},
): string {
  const all = chartData(table, options);
  const data = all.filter((d) => d.value > 0);
  const dropped = all.length - data.length;
  const w = options.width ?? WIDTH;
  const h = options.height ?? HEIGHT;
  const top = options.title ? 32 : 8;
  const foot = dropped > 0 ? 20 : 8;

  if (data.length === 0) {
    return svg(
      w,
      h,
      `<text x="${w / 2}" y="${h / 2}" text-anchor="middle" font-size="12"`
      + ` fill="#6b7280">nothing positive to lay out</text>`,
      options.title,
    );
  }

  const bounds: Rect = { x: 8, y: top, w: w - 16, h: h - top - foot };
  const rects = squarify(data.map((d) => d.value), bounds);
  const parts: string[] = [];

  rects.forEach((r, i) => {
    const d = data[i];
    if (!d || r.w <= 0 || r.h <= 0) return;
    const colour = PALETTE[i % PALETTE.length];
    parts.push(
      `<rect x="${r.x.toFixed(2)}" y="${r.y.toFixed(2)}"`
      + ` width="${r.w.toFixed(2)}" height="${r.h.toFixed(2)}"`
      + ` fill="${colour}" stroke="#ffffff" stroke-width="1">`
      + `<title>${esc(`${d.label}: ${fmt(d.value)}`)}</title></rect>`,
    );
    // A label only where it fits; a clipped word in a sliver is noise.
    if (r.w > 46 && r.h > 18) {
      parts.push(
        `<text x="${(r.x + 4).toFixed(2)}" y="${(r.y + 14).toFixed(2)}"`
        + ` font-size="10" fill="#ffffff">`
        + `${esc(clip(d.label, Math.floor(r.w / 6)))}</text>`,
      );
    }
  });

  if (dropped > 0) {
    // Said out loud: a treemap encodes magnitude as area, so rows
    // that cannot have an area are absent, and silence about that
    // would make the picture disagree with the grid beside it.
    parts.push(
      `<text x="8" y="${(h - 6).toFixed(2)}" font-size="10" fill="#6b7280">`
      + `${dropped} row${dropped === 1 ? '' : 's'} not shown`
      + ` (zero or negative cannot have an area)</text>`,
    );
  }

  return svg(w, h, parts.join(''), options.title);
}
