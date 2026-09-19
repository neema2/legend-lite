// Cell appearance: fonts, alignment, grid lines, and colour by value.
//
// DataCube carries four foreground and four background colours --
// normal, negative, zero, error -- at both the cube and the column
// level. That is more useful than a colour scale, for the reason the
// research gave: a scale spanning a nested pivot compares a subtotal
// against a leaf, so the strongest colour lands on whichever row
// happens to aggregate the most rather than on whatever is
// interesting. Colouring by what a value IS avoids that entirely.
//
// Resolution is pure and separate from the DOM so it can be tested
// without a browser, and so the grid can apply it without deciding
// anything.

import type { Scalar } from './result.ts';

/** Which of the four colour slots a value falls into. */
export type ValueState = 'normal' | 'negative' | 'zero' | 'error';

export type TextAlign = 'left' | 'center' | 'right';

export interface FontStyle {
  readonly fontFamily?: string;
  readonly fontSize?: number;
  readonly bold?: boolean;
  readonly italic?: boolean;
  readonly underline?: boolean;
  readonly strikethrough?: boolean;
  readonly textAlign?: TextAlign;
}

/** The four-slot colour set, foreground and background. */
export interface ColourSet {
  readonly normalForeground?: string;
  readonly negativeForeground?: string;
  readonly zeroForeground?: string;
  readonly errorForeground?: string;
  readonly normalBackground?: string;
  readonly negativeBackground?: string;
  readonly zeroBackground?: string;
  readonly errorBackground?: string;
}

export interface CellAppearance extends FontStyle, ColourSet {}

export interface GridAppearance extends CellAppearance {
  readonly showHorizontalGridLines?: boolean;
  readonly showVerticalGridLines?: boolean;
  readonly gridLineColor?: string;
  /** Shade every Nth row. */
  readonly alternateRows?: boolean;
  readonly alternateRowsColor?: string;
  /** How many rows per band; 1 means every other row. */
  readonly alternateRowsCount?: number;
}

/**
 * Which colour slot a value belongs to.
 *
 * `error` is a distinct state rather than a kind of normal: a cell
 * that failed to compute must not be coloured as though it held a
 * value, because a blank that looks normal is how a broken measure
 * goes unnoticed.
 *
 * Null is NOT an error -- it is an absent value, which in a pivot is
 * ordinary and usually means "no rows in this combination".
 */
export function valueState(value: Scalar, isError = false): ValueState {
  if (isError) return 'error';
  if (typeof value !== 'number') return 'normal';
  if (Number.isNaN(value)) return 'error';
  if (value === 0) return 'zero';
  return value < 0 ? 'negative' : 'normal';
}

/**
 * Resolve the colours for a state, falling back to normal.
 *
 * A cube that sets only a negative colour should still render
 * everything else, so an unset slot inherits rather than blanking.
 */
export function coloursFor(
  set: ColourSet,
  state: ValueState,
): { foreground?: string; background?: string } {
  const fg =
    state === 'negative'
      ? set.negativeForeground
      : state === 'zero'
        ? set.zeroForeground
        : state === 'error'
          ? set.errorForeground
          : undefined;
  const bg =
    state === 'negative'
      ? set.negativeBackground
      : state === 'zero'
        ? set.zeroBackground
        : state === 'error'
          ? set.errorBackground
          : undefined;
  return {
    ...(fg ?? set.normalForeground
      ? { foreground: fg ?? set.normalForeground }
      : {}),
    ...(bg ?? set.normalBackground
      ? { background: bg ?? set.normalBackground }
      : {}),
  };
}

/**
 * Merge a column's appearance over the cube's.
 *
 * Per-column wins, field by field rather than wholesale: a column
 * that sets only its alignment should keep the cube's fonts and
 * colours rather than resetting them.
 */
export function mergeAppearance(
  cube: CellAppearance,
  column: CellAppearance | undefined,
): CellAppearance {
  if (!column) return cube;
  const out: Record<string, unknown> = { ...cube };
  for (const [k, v] of Object.entries(column)) {
    if (v !== undefined) out[k] = v;
  }
  return out as CellAppearance;
}

/** Inline style for one cell, or an empty object for "nothing to say". */
export function cellStyle(
  appearance: CellAppearance,
  value: Scalar,
  isError = false,
): Record<string, string> {
  const { foreground, background } = coloursFor(
    appearance,
    valueState(value, isError),
  );
  const style: Record<string, string> = {};
  if (foreground) style['color'] = foreground;
  if (background) style['background-color'] = background;
  if (appearance.fontFamily) style['font-family'] = appearance.fontFamily;
  if (appearance.fontSize !== undefined) {
    style['font-size'] = `${appearance.fontSize}px`;
  }
  if (appearance.bold) style['font-weight'] = '700';
  if (appearance.italic) style['font-style'] = 'italic';
  if (appearance.textAlign) style['justify-content'] = justify(appearance.textAlign);

  // Underline and strikethrough combine rather than replacing each
  // other, which a single assignment would get wrong.
  const decorations: string[] = [];
  if (appearance.underline) decorations.push('underline');
  if (appearance.strikethrough) decorations.push('line-through');
  if (decorations.length > 0) {
    style['text-decoration'] = decorations.join(' ');
  }
  return style;
}

function justify(align: TextAlign): string {
  return align === 'left'
    ? 'flex-start'
    : align === 'center'
      ? 'center'
      : 'flex-end';
}

/**
 * Whether a row index falls in a shaded band.
 *
 * `count` is the band SIZE, not a modulus: a count of 2 shades two
 * rows then leaves two, which is what "alternate every 2" means to
 * someone reading a printed report.
 */
export function isAlternateRow(index: number, count = 1): boolean {
  const size = Math.max(1, count);
  return Math.floor(index / size) % 2 === 1;
}

/** CSS custom properties for the grid container. */
export function gridVariables(a: GridAppearance): Record<string, string> {
  const vars: Record<string, string> = {};
  // The user's GRID LINES, which are not the structural frame.
  // DataCube keeps these apart -- --ag-border-color is neutral-200
  // and DEFAULT_GRID_LINE_COLOR is neutral-300 -- and folding them
  // together makes the frame move whenever a user recolours the
  // lines, which is not what the setting says it does.
  if (a.gridLineColor) vars['--dc-grid-line'] = a.gridLineColor;
  vars['--dc-hgrid'] = a.showHorizontalGridLines === false ? '0' : '1';
  vars['--dc-vgrid'] = a.showVerticalGridLines === false ? '0' : '1';
  if (a.alternateRowsColor) vars['--dc-alt-row'] = a.alternateRowsColor;
  if (a.fontFamily) vars['--dc-font'] = a.fontFamily;
  if (a.fontSize !== undefined) vars['--dc-font-size'] = `${a.fontSize}px`;
  return vars;
}


// -- heatmaps ---------------------------------------------------------

export interface HeatmapRange {
  readonly min: number;
  readonly max: number;
}

export interface HeatmapSpec {
  /** Colour at the low end. */
  readonly from: string;
  /** Colour at the high end. */
  readonly to: string;
  /**
   * Fix the scale rather than deriving it from the data.
   *
   * Useful when two cubes must be comparable, and necessary when the
   * visible rows are a windowed prefix -- deriving the range from
   * what happens to be on screen makes the colours change as the
   * user scrolls.
   */
  readonly range?: HeatmapRange;
}

/**
 * The numeric range of a column, ignoring blanks and non-numbers.
 *
 * Returns null when there is nothing to scale, so a caller renders no
 * heatmap rather than a uniform block of the low colour.
 */
export function columnRange(values: readonly Scalar[]): HeatmapRange | null {
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let seen = false;
  for (const v of values) {
    if (typeof v !== 'number' || !Number.isFinite(v)) continue;
    seen = true;
    if (v < min) min = v;
    if (v > max) max = v;
  }
  return seen ? { min, max } : null;
}

/**
 * Where a value sits in a range, from 0 to 1.
 *
 * A zero-width range maps everything to the TOP rather than dividing
 * by zero: when every value is identical they are all the maximum,
 * and rendering them all as the minimum reads as "all low", which is
 * the opposite of true.
 */
export function heatPosition(value: number, range: HeatmapRange): number {
  const span = range.max - range.min;
  if (span <= 0) return 1;
  const t = (value - range.min) / span;
  return t < 0 ? 0 : t > 1 ? 1 : t;
}

/** Mix two `#rrggbb` colours. Returns `from` when t is 0. */
export function mixHex(from: string, to: string, t: number): string {
  const parse = (hex: string): [number, number, number] => {
    const h = hex.replace('#', '');
    const full =
      h.length === 3
        ? h
            .split('')
            .map((c) => c + c)
            .join('')
        : h;
    return [
      Number.parseInt(full.slice(0, 2), 16),
      Number.parseInt(full.slice(2, 4), 16),
      Number.parseInt(full.slice(4, 6), 16),
    ];
  };
  const [r1, g1, b1] = parse(from);
  const [r2, g2, b2] = parse(to);
  const c = (a: number, b: number): number => Math.round(a + (b - a) * t);
  const hex = (n: number): string => n.toString(16).padStart(2, '0');
  return `#${hex(c(r1, r2))}${hex(c(g1, g2))}${hex(c(b1, b2))}`;
}

/**
 * The background colour for one heatmapped cell, or null when the
 * value cannot be placed on the scale.
 */
export function heatColour(
  value: Scalar,
  spec: HeatmapSpec,
  range: HeatmapRange | null,
): string | null {
  if (typeof value !== 'number' || !Number.isFinite(value)) return null;
  const r = spec.range ?? range;
  if (!r) return null;
  return mixHex(spec.from, spec.to, heatPosition(value, r));
}
