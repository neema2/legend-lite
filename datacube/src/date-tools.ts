// THE DATETIME TOOL'S OTHER TWO WAYS (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's DateTime box, Alteryx's DateTime
// -- the first, a date taken to its period, is the grid's own grain preset, grid-presets.ts): a date's PART (its year,
// its month's number, its day of the month, its week of the year, its hour), and TEXT READ AS A DATE by a format
// (`convertDateFormat`, `convertDateTimeFormat`: the database's strptime, the engine's tokens -- `yyyy-MM-dd` --
// translated by the compiler). A value the format cannot read refuses the box's rows, in the database's words (the
// platform's natives are strptime's, not a try's: an empty for unreadable text would need a native of its own). Each a calculated column, as calc-presets.ts's are; what a box says is read back off
// its expression's shape (`dateToolOf`), never off a column's name. And a DATE WRITTEN AS TEXT by a format (`format`'s
// `%t{...}`, the engine's tokens the compiler translates to the database's strftime): an empty date, an empty text.
//
// One rule for the box's window and Python's grid.date_part and grid.parse_date (headless gridPreset).
//
// PURE: no DOM, no planner.

import { collection, fn, lambda, lit, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, type DerivedColumn } from './snapshot.ts';

/** A date's part, as the tool offers it. */
export type DatePart = 'year' | 'month' | 'day' | 'week' | 'hour';

/** The compiler's function for each part; `hour` a date-time's alone. */
const PART_FUNCTION: Readonly<Record<DatePart, string>> = {
  year: 'year', month: 'monthNumber', day: 'dayOfMonth', week: 'weekOfYear', hour: 'hour',
};

/** The parts whose function takes an empty value (`Date[0..1]`); the others are given the value (`toOne`). */
const TAKES_EMPTY: ReadonlySet<DatePart> = new Set(['year', 'month', 'week']);

/** Words for a part, as the window and a box say it. */
export const PART_WORDS: Readonly<Record<DatePart, string>> = {
  year: 'year', month: 'month number', day: 'day of the month', week: 'week of the year', hour: 'hour',
};

/** Formats offered for reading text as a date, in the engine's tokens (the compiler translates them). */
export const DATE_FORMATS: readonly string[] = ['yyyy-MM-dd', 'MM/dd/yyyy', 'dd/MM/yyyy', 'yyyyMMdd', 'dd-MMM-yyyy', 'yyyy-MM-dd HH:mm:ss'];

/** Formats offered for writing a date as text, in the engine's tokens (the compiler's: yyyy, MM, dd, HH, hh, mm, ss, SSS,
 *  a -- a month's name it has not, and refuses in words). */
export const TEXT_FORMATS: readonly string[] = ['dd/MM/yyyy', 'MM/dd/yyyy', 'yyyy-MM-dd', 'dd.MM.yyyy', 'yyyyMMdd', 'yyyy-MM-dd HH:mm:ss'];

/** The three ways, as the window and Python set them. */
export type DateTool =
  | { readonly kind: 'datePart'; readonly column: string; readonly part: DatePart; readonly name: string }
  /** `time`: the text holds a time of day too (a DateTime), else a day (a StrictDate). */
  | { readonly kind: 'dateParse'; readonly column: string; readonly format: string; readonly time: boolean; readonly name: string }
  | { readonly kind: 'dateFormat'; readonly column: string; readonly format: string; readonly name: string };

/** The kinds a date tool is, as Python names them. */
export const DATE_TOOL_KINDS: readonly DateTool['kind'][] = ['datePart', 'dateParse', 'dateFormat'];

/** The compiler's readers of text by a format, by their paths (legend-lite's own natives: no import names them). */
const READERS = { false: 'meta::legend::lite::convertDateFormat', true: 'meta::legend::lite::convertDateTimeFormat' } as const;

/** What is wrong with a date tool's settings, in words, if anything. */
export function dateToolProblem(t: DateTool): string | undefined {
  if (t.name.trim() === '') return 'its column needs a name';
  if (t.kind === 'datePart' && !(t.part in PART_FUNCTION)) return `a part is ${Object.keys(PART_FUNCTION).join(', ')}, not ${JSON.stringify(t.part)}`;
  if (t.kind === 'dateParse' && t.format.trim() === '') return 'it reads by a format: type one, as yyyy-MM-dd';
  if (t.kind === 'dateFormat' && t.format.trim() === '') return 'it writes by a format: type one, as dd/MM/yyyy';
  // a brace would end the format's directive (`%t{...}`) before the format does
  if (t.kind === 'dateFormat' && /[{}]/.test(t.format)) return `a format has no { or }: ${JSON.stringify(t.format)}`;
  return undefined;
}

/** The command that adds a date tool's column: a row-stage `calculate`, a dimension. Its settings refused in words. */
export function dateToolCommands(t: DateTool): GridCommand[] {
  const problem = dateToolProblem(t);
  if (problem !== undefined) throw new CubeRefusal(problem);
  const value = property(variable('x'), t.column);
  const body: ValueSpecification = t.kind === 'datePart'
    ? fn(PART_FUNCTION[t.part], TAKES_EMPTY.has(t.part) ? value : fn('toOne', value))
    : t.kind === 'dateParse' ? fn(READERS[`${t.time}`], value, lit.string(t.format))
      : fn('format', lit.string(`%t{${t.format}}`), collection([value]));
  return [{ kind: 'calculate', level: 'row', column: { name: t.name.trim(), lambda: lambda(['x'], body), kind: 'dimension' } }];
}

/** What a column made by a date tool does, read off its expression: a part of a column, or a column read by a format. */
export function dateToolOf(d: DerivedColumn): { readonly says: string } | undefined {
  const param = d.lambda?.parameters[0]?.name;
  const body = d.lambda?.body;
  if (param === undefined || body?.length !== 1) return undefined;
  const call = body[0]!;
  if (call._type !== 'func') return undefined;
  const read = (v: ValueSpecification | undefined): string | undefined => {
    const p = v?._type === 'func' && v.function === 'toOne' && v.parameters.length === 1 ? v.parameters[0] : v;
    if (p?._type !== 'property' || p.parameters.length !== 1) return undefined;
    const owner = p.parameters[0]!;
    return owner._type === 'var' && owner.name === param ? p.property : undefined;
  };
  const part = (Object.keys(PART_FUNCTION) as DatePart[]).find((k) => PART_FUNCTION[k] === call.function);
  if (part !== undefined && call.parameters.length === 1) {
    const column = read(call.parameters[0]);
    return column === undefined ? undefined : { says: `${PART_WORDS[part]} of ${column}` };
  }
  if ((call.function === READERS.false || call.function === READERS.true) && call.parameters.length === 2) {
    const column = read(call.parameters[0]);
    const format = call.parameters[1];
    return column !== undefined && format?._type === 'string' ? { says: `${column} read as ${format.value}` } : undefined;
  }
  // a date written as text: `format('%t{<format>}', [$x.c])`
  const [directive, args] = call.parameters;
  if (call.function === 'format' && call.parameters.length === 2 && directive?._type === 'string' && args?._type === 'collection') {
    const written = /^%t\{([^{}]+)\}$/.exec(directive.value);
    const column = args.values.length === 1 ? read(args.values[0]) : undefined;
    return written !== null && column !== undefined ? { says: `${column} written as ${written[1]}` } : undefined;
  }
  return undefined;
}

/** The name a date tool's column is given unless one is named -- `<column>_<part>`, `<column>_date`, `<column>_text` --
 *  its window's and Python's alike (the review of 2026-10-26: each spelt it). */
export function dateToolName(kind: DateTool['kind'], column: string, part?: DatePart): string {
  return kind === 'datePart' ? `${column}_${part}` : kind === 'dateParse' ? `${column}_date` : `${column}_text`;
}

/** Whether a format reads a time of day (an hour, a minute, a second): what it reads is a date-time, not a date. */
export function formatHasTime(format: string): boolean {
  return /[Hhms]/.test(format);
}

/** A date tool read from JSON (Python's): its kind one of DATE_TOOL_KINDS, its fields those the kind takes -- or refused
 *  in words. Its columns are the grid's to judge. */
export function readDateTool(raw: unknown): DateTool {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined) throw new CubeRefusal(`a date tool is {kind, ...}, not ${JSON.stringify(raw)}`);
  const only = (keys: readonly string[]): void => {
    const odd = Object.keys(o).filter((k) => !['kind', ...keys].includes(k));
    if (odd.length > 0) throw new CubeRefusal(`a '${String(o['kind'])}' takes ${keys.join(', ')}, not ${odd.join(', ')}`);
  };
  const text = (key: string): string => {
    const v = o[key];
    if (typeof v !== 'string') throw new CubeRefusal(`its ${key} is text, not ${JSON.stringify(v)}`);
    return v;
  };
  let tool: DateTool;
  // its name, unless given, its column's (dateToolName); a parse's time, unless given, its format's (formatHasTime)
  if (o['kind'] === 'datePart') {
    only(['column', 'part', 'name']);
    const part = text('part') as DatePart;
    tool = { kind: 'datePart', column: text('column'), part, name: o['name'] === undefined ? dateToolName('datePart', text('column'), part) : text('name') };
  } else if (o['kind'] === 'dateParse') {
    only(['column', 'format', 'time', 'name']);
    const time = o['time'] ?? formatHasTime(text('format'));
    if (typeof time !== 'boolean') throw new CubeRefusal(`its time is true or false, not ${JSON.stringify(o['time'])}`);
    tool = { kind: 'dateParse', column: text('column'), format: text('format'), time,
      name: o['name'] === undefined ? dateToolName('dateParse', text('column')) : text('name') };
  } else if (o['kind'] === 'dateFormat') {
    only(['column', 'format', 'name']);
    tool = { kind: 'dateFormat', column: text('column'), format: text('format'),
      name: o['name'] === undefined ? dateToolName('dateFormat', text('column')) : text('name') };
  } else {
    throw new CubeRefusal(`a date tool is ${DATE_TOOL_KINDS.join(', ')}, not ${JSON.stringify(o['kind'])}`);
  }
  const problem = dateToolProblem(tool);
  if (problem !== undefined) throw new CubeRefusal(problem);
  return tool;
}
