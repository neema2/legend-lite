// CONDITIONAL FORMATS (docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md P3; Excel's Conditional Formatting): a column's cells
// drawn by their values -- a DATA BAR, its length the value's place in the column's range; an ICON by the third of the
// range the value falls in; a RULE's colours where its condition holds (greater than, between, text contains, blank, the
// top or bottom N, above or below the average) -- beside the heatmap's colour scale (style.ts), which takes a midpoint
// colour for Excel's three-colour scale. Each a column setting (config.ts), so it survives a refresh and is saved with
// the page, set by the `configure` command as every setting is; each measured over the WHOLE column at the cell's depth
// (export-render.ts measureHeatmaps: a cell against its siblings, never what happens to be on screen). Presentation of
// values the database answered, never a value of its own. No DOM: the grid paints what this says.

import type { Scalar } from '../../engine-client/src/result.ts';
import { numberOf } from '../../engine-client/src/values.ts';
import { CubeRefusal } from './snapshot.ts';
import type { HeatmapRange, HeatmapSpec } from './style.ts';

/** A bar in each cell (Excel's Data Bars): from zero to the value, a negative one leftward in its own colour. */
export interface DataBarSpec {
  readonly color: string;
  /** The colour of a bar below zero; absent, Excel's red. */
  readonly negativeColor?: string;
  /** A fixed range, as a heatmap's: absent, the column's at the cell's depth. */
  readonly range?: HeatmapRange;
}

/** The icon sets (Excel's three-icon sets), each low to high. */
export const ICON_SETS = {
  arrows: [{ glyph: '▼', color: '#c62828', label: 'low' }, { glyph: '▶', color: '#ef8f00', label: 'middle' }, { glyph: '▲', color: '#2e7d32', label: 'high' }],
  trafficLights: [{ glyph: '●', color: '#c62828', label: 'low' }, { glyph: '●', color: '#f2b600', label: 'middle' }, { glyph: '●', color: '#2e7d32', label: 'high' }],
  symbols: [{ glyph: '✖', color: '#c62828', label: 'low' }, { glyph: '!', color: '#ef8f00', label: 'middle' }, { glyph: '✔', color: '#2e7d32', label: 'high' }],
  flags: [{ glyph: '⚑', color: '#c62828', label: 'low' }, { glyph: '⚑', color: '#f2b600', label: 'middle' }, { glyph: '⚑', color: '#2e7d32', label: 'high' }],
} as const satisfies Record<string, readonly [Icon, Icon, Icon]>;
export type IconSetName = keyof typeof ICON_SETS;
export const ICON_SET_NAMES = Object.keys(ICON_SETS) as IconSetName[];
export const ICON_SET_LABELS: Readonly<Record<IconSetName, string>> = {
  arrows: 'Arrows', trafficLights: 'Traffic Lights', symbols: 'Symbols', flags: 'Flags',
};

export interface Icon {
  readonly glyph: string;
  readonly color: string;
  /** What it says of the value, for a reader that does not see colour. */
  readonly label: string;
}

/** An icon before each value by the third of the range it falls in (Excel's Icon Sets, its 33% and 67% thresholds). */
export interface IconSetSpec {
  readonly set: IconSetName;
  /** The high icon for the low values: a cost's, a risk's. */
  readonly reverse?: boolean;
  readonly range?: HeatmapRange;
}

/** When a rule holds of a value. */
export type RuleWhen =
  | { readonly op: 'greaterThan' | 'lessThan' | 'atLeast' | 'atMost'; readonly value: number }
  | { readonly op: 'equal' | 'notEqual'; readonly value: number | string }
  | { readonly op: 'between'; readonly from: number; readonly to: number }
  | { readonly op: 'contains' | 'startsWith'; readonly text: string }
  | { readonly op: 'top' | 'bottom'; readonly count: number }
  | { readonly op: 'blank' | 'notBlank' | 'aboveAverage' | 'belowAverage' };
export type RuleOp = RuleWhen['op'];

/** What a rule draws where it holds. */
export interface RuleStyle {
  readonly foreground?: string;
  readonly background?: string;
  readonly bold?: boolean;
}

/** A rule (Excel's Highlight Cells and Top/Bottom Rules): the first that holds draws its style. */
export interface FormatRule {
  readonly when: RuleWhen;
  readonly style: RuleStyle;
}

/** A rule as a gesture is given one (readRule): its style one of Excel's by name, or its own colours. */
export interface FormatRuleGiven {
  readonly when: RuleWhen;
  readonly style: RuleStyle | RuleStyleName;
}

/** Excel's own presets for a rule's style. */
export const RULE_STYLES = {
  lightRed: { foreground: '#9c0006', background: '#ffc7ce' },
  yellow: { foreground: '#9c5700', background: '#ffeb9c' },
  green: { foreground: '#006100', background: '#c6efce' },
  redText: { foreground: '#c62828' },
  bold: { bold: true },
} as const satisfies Record<string, RuleStyle>;

export type RuleStyleName = keyof typeof RULE_STYLES;

/** A column's measured figures at one depth: what a bar, an icon and a rule are placed against. */
export interface ColumnStats {
  readonly range: HeatmapRange;
  readonly mean: number;
  /** The values, largest first: the top and bottom N's thresholds. */
  readonly sorted: readonly number[];
}

/** A column's figures over `values` (blanks and non-numbers left out); none when it has no number. */
export function columnStats(values: readonly Scalar[], type: string | undefined): ColumnStats | null {
  const nums: number[] = [];
  for (const v of values) {
    const n = numberOf(v, type);
    if (n !== null) nums.push(n);
  }
  if (nums.length === 0) return null;
  nums.sort((a, b) => b - a);
  return { range: { min: nums.at(-1)!, max: nums[0]! }, mean: nums.reduce((a, b) => a + b, 0) / nums.length, sorted: nums };
}

/**
 * WHERE A VALUE'S BAR IS DRAWN, as fractions of the cell's width: from the zero line to the value (Excel's automatic
 * axis), the zero line at the left when no value is negative, at the right when none is positive, else where zero
 * falls; `negative`, a bar left of the line. Null where the value is no number.
 */
export function barExtent(value: Scalar, type: string | undefined, range: HeatmapRange): { start: number; end: number; negative: boolean } | null {
  const n = numberOf(value, type);
  if (n === null) return null;
  const lo = Math.min(0, range.min);
  const hi = Math.max(0, range.max);
  const span = hi - lo;
  if (span <= 0) return { start: 0, end: 0, negative: false };
  const clamp = (x: number): number => (x < 0 ? 0 : x > 1 ? 1 : x);
  // the zero line: at the left when no value is below zero (never a -0 of 0 / span)
  const axis = lo < 0 ? clamp(-lo / span) : 0;
  const at = clamp((n - lo) / span);
  return { start: Math.min(axis, at), end: Math.max(axis, at), negative: n < 0 };
}

/** A value's icon: its third of the range, low to high (reversed, high to low). Null where the value is no number. */
export function iconFor(value: Scalar, type: string | undefined, spec: IconSetSpec, range: HeatmapRange): Icon | null {
  const n = numberOf(value, type);
  if (n === null) return null;
  const span = range.max - range.min;
  const t = span <= 0 ? 1 : (n - range.min) / span;
  const third = t >= 2 / 3 ? 2 : t >= 1 / 3 ? 1 : 0;
  return ICON_SETS[spec.set][spec.reverse ? 2 - third : third]!;
}

/** Whether a rule holds of a value, against its column's figures at the value's depth (`stats`, null: no numbers). */
export function ruleHolds(when: RuleWhen, value: Scalar, type: string | undefined, stats: ColumnStats | null): boolean {
  const blank = value === null || value === undefined || value === '';
  switch (when.op) {
    case 'blank': return blank;
    case 'notBlank': return !blank;
    case 'contains': return !blank && String(value).toLowerCase().includes(when.text.toLowerCase());
    case 'startsWith': return !blank && String(value).toLowerCase().startsWith(when.text.toLowerCase());
    case 'equal':
    case 'notEqual': {
      const n = numberOf(value, type);
      const same = typeof when.value === 'number' ? n !== null && n === when.value : !blank && String(value) === when.value;
      return when.op === 'equal' ? same : !blank && !same;
    }
    case 'greaterThan':
    case 'lessThan':
    case 'atLeast':
    case 'atMost':
    case 'between':
    case 'top':
    case 'bottom':
    case 'aboveAverage':
    case 'belowAverage': {
      const n = numberOf(value, type);
      if (n === null) return false;
      switch (when.op) {
        case 'greaterThan': return n > when.value;
        case 'lessThan': return n < when.value;
        case 'atLeast': return n >= when.value;
        case 'atMost': return n <= when.value;
        case 'between': return n >= Math.min(when.from, when.to) && n <= Math.max(when.from, when.to);
        // ties at the Nth value are in, as Excel's are
        case 'top': return stats !== null && n >= stats.sorted[Math.min(when.count, stats.sorted.length) - 1]!;
        case 'bottom': return stats !== null && n <= stats.sorted[Math.max(0, stats.sorted.length - when.count)]!;
        case 'aboveAverage': return stats !== null && n > stats.mean;
        case 'belowAverage': return stats !== null && n < stats.mean;
      }
    }
  }
}

/** The style of the first rule that holds of a value, or none. */
export function ruleStyle(rules: readonly FormatRule[], value: Scalar, type: string | undefined, stats: ColumnStats | null): RuleStyle | null {
  return rules.find((r) => ruleHolds(r.when, value, type, stats))?.style ?? null;
}

/** The words a person reads for a rule: `Greater than 100`, `Top 10`, `Contains "EM"`. */
export function ruleLabel(when: RuleWhen): string {
  switch (when.op) {
    case 'greaterThan': return `Greater than ${when.value}`;
    case 'lessThan': return `Less than ${when.value}`;
    case 'atLeast': return `At least ${when.value}`;
    case 'atMost': return `At most ${when.value}`;
    case 'equal': return `Equal to ${JSON.stringify(when.value)}`;
    case 'notEqual': return `Not equal to ${JSON.stringify(when.value)}`;
    case 'between': return `Between ${when.from} and ${when.to}`;
    case 'contains': return `Contains ${JSON.stringify(when.text)}`;
    case 'startsWith': return `Starts with ${JSON.stringify(when.text)}`;
    case 'top': return `Top ${when.count}`;
    case 'bottom': return `Bottom ${when.count}`;
    case 'blank': return 'Blank';
    case 'notBlank': return 'Not blank';
    case 'aboveAverage': return 'Above average';
    case 'belowAverage': return 'Below average';
  }
}

// ---------------------------------------------------------------- reading them from JSON (a saved page, a configure)

const isObject = (x: unknown): x is Record<string, unknown> => typeof x === 'object' && x !== null && !Array.isArray(x);
const COLOUR = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;

function colour(x: unknown, what: string): string {
  if (typeof x !== 'string' || !COLOUR.test(x)) throw new CubeRefusal(`${what} is a colour as #rrggbb, not ${JSON.stringify(x)}`);
  return x;
}

function finite(x: unknown, what: string): number {
  if (typeof x !== 'number' || !Number.isFinite(x)) throw new CubeRefusal(`${what} is a number, not ${JSON.stringify(x)}`);
  return x;
}

function only(o: Record<string, unknown>, keys: readonly string[], what: string): void {
  const odd = Object.keys(o).find((k) => !keys.includes(k));
  if (odd !== undefined) throw new CubeRefusal(`${what} takes ${keys.join(', ')}, not ${odd}`);
}

function rangeFrom(x: unknown, what: string): HeatmapRange | undefined {
  if (x === undefined) return undefined;
  if (!isObject(x)) throw new CubeRefusal(`${what}'s range is {min, max}, not ${JSON.stringify(x)}`);
  only(x, ['min', 'max'], `${what}'s range`);
  const min = finite(x['min'], `${what}'s range's min`);
  const max = finite(x['max'], `${what}'s range's max`);
  if (min > max) throw new CubeRefusal(`${what}'s range runs from its min to its max, and ${min} is over ${max}`);
  return { min, max };
}

/** A data bar from JSON, or refused in words. */
export function readDataBar(x: unknown): DataBarSpec {
  if (!isObject(x)) throw new CubeRefusal(`a data bar is {color, negativeColor?, range?}, not ${JSON.stringify(x)}`);
  only(x, ['color', 'negativeColor', 'range'], 'a data bar');
  const range = rangeFrom(x['range'], 'a data bar');
  return {
    color: colour(x['color'], "a data bar's color"),
    ...(x['negativeColor'] !== undefined ? { negativeColor: colour(x['negativeColor'], "a data bar's negativeColor") } : {}),
    ...(range ? { range } : {}),
  };
}

/** An icon set from JSON, or refused in words. */
export function readIconSet(x: unknown): IconSetSpec {
  if (!isObject(x)) throw new CubeRefusal(`an icon set is {set, reverse?, range?}, not ${JSON.stringify(x)}`);
  only(x, ['set', 'reverse', 'range'], 'an icon set');
  const set = x['set'];
  if (typeof set !== 'string' || !(ICON_SET_NAMES as readonly string[]).includes(set)) {
    throw new CubeRefusal(`an icon set is one of ${ICON_SET_NAMES.join(', ')}, not ${JSON.stringify(set)}`);
  }
  if (x['reverse'] !== undefined && typeof x['reverse'] !== 'boolean') {
    throw new CubeRefusal(`an icon set's reverse is true or false, not ${JSON.stringify(x['reverse'])}`);
  }
  const range = rangeFrom(x['range'], 'an icon set');
  return { set: set as IconSetName, ...(x['reverse'] === true ? { reverse: true } : {}), ...(range ? { range } : {}) };
}

const OPS: Readonly<Record<RuleOp, readonly string[]>> = {
  greaterThan: ['value'], lessThan: ['value'], atLeast: ['value'], atMost: ['value'], equal: ['value'], notEqual: ['value'],
  between: ['from', 'to'], contains: ['text'], startsWith: ['text'], top: ['count'], bottom: ['count'],
  blank: [], notBlank: [], aboveAverage: [], belowAverage: [],
};

/** A rule from JSON, or refused in words. */
export function readRule(x: unknown): FormatRule {
  if (!isObject(x)) throw new CubeRefusal(`a rule is {when, style}, not ${JSON.stringify(x)}`);
  only(x, ['when', 'style'], 'a rule');
  const w = x['when'];
  if (!isObject(w) || typeof w['op'] !== 'string' || !Object.hasOwn(OPS, w['op'])) {
    throw new CubeRefusal(`a rule's when is {op, ...}, op one of ${Object.keys(OPS).join(', ')}, not ${JSON.stringify(w)}`);
  }
  const op = w['op'] as RuleOp;
  only(w, ['op', ...OPS[op]], `a rule's ${op}`);
  for (const k of OPS[op]) if (w[k] === undefined) throw new CubeRefusal(`a rule's ${op} takes ${OPS[op].join(', ')}: ${k} is missing`);
  let when: RuleWhen;
  switch (op) {
    case 'greaterThan': case 'lessThan': case 'atLeast': case 'atMost':
      when = { op, value: finite(w['value'], `a rule's ${op} value`) };
      break;
    case 'equal': case 'notEqual':
      if (typeof w['value'] !== 'string') when = { op, value: finite(w['value'], `a rule's ${op} value`) };
      else when = { op, value: w['value'] };
      break;
    case 'between':
      when = { op, from: finite(w['from'], "a rule's between from"), to: finite(w['to'], "a rule's between to") };
      break;
    case 'contains': case 'startsWith':
      if (typeof w['text'] !== 'string' || w['text'] === '') throw new CubeRefusal(`a rule's ${op} text is some text, not ${JSON.stringify(w['text'])}`);
      when = { op, text: w['text'] };
      break;
    case 'top': case 'bottom': {
      const count = w['count'];
      if (typeof count !== 'number' || !Number.isInteger(count) || count < 1) throw new CubeRefusal(`a rule's ${op} count is a whole number from 1, not ${JSON.stringify(count)}`);
      when = { op, count };
      break;
    }
    case 'blank': case 'notBlank': case 'aboveAverage': case 'belowAverage':
      when = { op };
      break;
  }
  const s = x['style'];
  // one of Excel's own by its name, as the menu's and Python's highlight name them
  if (typeof s === 'string') {
    if (!Object.hasOwn(RULE_STYLES, s)) throw new CubeRefusal(`a rule's style is one of ${Object.keys(RULE_STYLES).join(', ')} or {foreground?, background?, bold?}, not ${JSON.stringify(s)}`);
    return { when, style: RULE_STYLES[s as keyof typeof RULE_STYLES] };
  }
  if (!isObject(s)) throw new CubeRefusal(`a rule's style is {foreground?, background?, bold?}, not ${JSON.stringify(s)}`);
  only(s, ['foreground', 'background', 'bold'], "a rule's style");
  if (s['bold'] !== undefined && typeof s['bold'] !== 'boolean') throw new CubeRefusal(`a rule's bold is true or false, not ${JSON.stringify(s['bold'])}`);
  const style: RuleStyle = {
    ...(s['foreground'] !== undefined ? { foreground: colour(s['foreground'], "a rule's foreground") } : {}),
    ...(s['background'] !== undefined ? { background: colour(s['background'], "a rule's background") } : {}),
    ...(s['bold'] === true ? { bold: true } : {}),
  };
  if (Object.keys(style).length === 0) throw new CubeRefusal("a rule's style draws something: a foreground, a background or bold");
  return { when, style };
}

/** A colour scale from JSON (the heatmap's, style.ts HeatmapSpec: two colours or Excel's three), or refused in words. */
export function readColorScale(x: unknown): HeatmapSpec {
  if (!isObject(x)) throw new CubeRefusal(`a colour scale is {from, to, mid?}, not ${JSON.stringify(x)}`);
  only(x, ['from', 'to', 'mid', 'range'], 'a colour scale');
  const range = rangeFrom(x['range'], 'a colour scale');
  return {
    from: colour(x['from'], "a colour scale's from"),
    to: colour(x['to'], "a colour scale's to"),
    ...(x['mid'] !== undefined ? { mid: colour(x['mid'], "a colour scale's mid") } : {}),
    ...(range ? { range } : {}),
  };
}

// ---------------------------------------------------------------- what a cell draws

/** A column's conditional formats, as its settings hold them (config.ts ColumnConfiguration). */
export interface ConditionalSetting {
  readonly dataBar?: DataBarSpec;
  readonly iconSet?: IconSetSpec;
  readonly rules?: readonly FormatRule[];
}

/** What a cell draws of its column's conditional formats (grid.ts paints it): a bar behind, an icon before, a style. */
export interface CellDecoration {
  readonly bar?: { readonly start: number; readonly end: number; readonly color: string };
  readonly icon?: Icon;
  readonly rule?: RuleStyle;
}

/** Excel's red for a bar below zero. */
export const NEGATIVE_BAR = '#f4a3a3';

/** A cell's decoration: its bar, its icon, its first rule's style, against its column's figures at its depth (`stats`). */
export function decorate(setting: ConditionalSetting, value: Scalar, type: string | undefined, stats: ColumnStats | null): CellDecoration | null {
  const barRange = setting.dataBar?.range ?? stats?.range;
  const extent = setting.dataBar && barRange ? barExtent(value, type, barRange) : null;
  const iconRange = setting.iconSet?.range ?? stats?.range;
  const icon = setting.iconSet && iconRange ? iconFor(value, type, setting.iconSet, iconRange) : null;
  const rule = setting.rules && setting.rules.length > 0 ? ruleStyle(setting.rules, value, type, stats) : null;
  if (extent === null && icon === null && rule === null) return null;
  return {
    ...(extent !== null && extent.end > extent.start
      ? { bar: { start: extent.start, end: extent.end, color: extent.negative ? (setting.dataBar!.negativeColor ?? NEGATIVE_BAR) : setting.dataBar!.color } }
      : {}),
    ...(icon !== null ? { icon } : {}),
    ...(rule !== null ? { rule } : {}),
  };
}

// ---------------------------------------------------------------- the menu's presets (Excel's own)

/** A data bar as the menu's Data Bars draws it: Excel's light blue. */
export const DATA_BAR: DataBarSpec = { color: '#9fc3ea' };

/** The colour scales the menu offers, Excel's three-colour ones and the heatmap's own two. */
export const COLOR_SCALES: readonly { readonly label: string; readonly scale: HeatmapSpec }[] = [
  { label: 'Green - Yellow - Red', scale: { from: '#f8696b', mid: '#ffeb84', to: '#63be7b' } },
  { label: 'Red - Yellow - Green', scale: { from: '#63be7b', mid: '#ffeb84', to: '#f8696b' } },
  { label: 'Blue - White - Red', scale: { from: '#5a8ac6', mid: '#fcfcff', to: '#f8696b' } },
  { label: 'White - Orange', scale: { from: '#ffffff', to: '#ff8a65' } },
];

/** The rules the menu offers in one click (Excel's Top/Bottom Rules and its plainest highlights); a rule of a value of
 *  one's own is the column's Properties' or Python's. */
export const RULE_PRESETS: readonly { readonly label: string; readonly rule: FormatRule }[] = [
  { label: 'Top 10 Items', rule: { when: { op: 'top', count: 10 }, style: RULE_STYLES.green } },
  { label: 'Bottom 10 Items', rule: { when: { op: 'bottom', count: 10 }, style: RULE_STYLES.lightRed } },
  { label: 'Above Average', rule: { when: { op: 'aboveAverage' }, style: RULE_STYLES.green } },
  { label: 'Below Average', rule: { when: { op: 'belowAverage' }, style: RULE_STYLES.lightRed } },
  { label: 'Negative Values in Red', rule: { when: { op: 'lessThan', value: 0 }, style: RULE_STYLES.redText } },
  { label: 'Blank Cells', rule: { when: { op: 'blank' }, style: RULE_STYLES.yellow } },
];
