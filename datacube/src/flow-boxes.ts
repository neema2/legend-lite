// WHAT A FLOW'S BOX DOES, SAID (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL5, §3.2): a box is a grid that does one thing,
// and what it does is what its grid's query does -- read off its cube document, never stored beside it, so a box says
// what Python's page and the screen's editors made of it alike. The canvas draws a box by these words: its tool's icon
// and name, and a line of what it is set to (as Alteryx's annotation under a tool says it).
//
// PURE: no DOM, no planner. The tools a person adds from the palette are here too, by the words the canvas shows.

import {
  derivedReads, inputCube, inputOn, inputSides, isLive, type CubeDocument, type InputPort, type InputSide, type JoinInput,
} from './cube-document.ts';
import { grainOf } from './calc-presets.ts';
import { binHelpers, binsOf } from './binning.ts';
import { textToolOf, type TextTool } from './text-tools.ts';
import { uniqueOf } from './unique.ts';
import { rowSampleOf, rowSampleWords } from './row-sample.ts';
import { transposeOf } from './transpose.ts';
import { boxOmitted, boxOrder, boxRenamed, inPlaceOf } from './box-columns.ts';
import { retypesOf } from './select-columns.ts';
import { imputedOf } from './impute.ts';
import { replacedOf, replacedWords } from './dynamic-replace.ts';
import { findReplaceOf } from './find-replace.ts';
import { countRecordsOf } from './count-records.ts';
import { checksOf, checkWords } from './checks.ts';
import { multiFieldOf, type MultiFieldColumn } from './multi-field.ts';
import { dateToolOf } from './date-tools.ts';
import { conditionWords, datesKept, datesKeptWords } from './page-filters.ts';
import { CUSTOM_FILTER_COLUMN, customFilterOf, customFilterWords } from './custom-filter.ts';
import { generatedHelpers, generatedOf, generatedWords } from './generate-rows.ts';
import { pureWords } from './pure-box.ts';
import { gridTitle } from './page-ids.ts';
import type { PageDocument } from './page-document.ts';
import { isEveryPair, isFuzzy, type JoinHow } from './join-how.ts';
import { joinWords } from './join-words.ts';
import { rowColumns, type DerivedColumn } from './snapshot.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { ColumnEditorStart } from './ui/column-editor.ts';
import type { EditorTab } from './ui/editor.ts';

/** A tool of the flow's palette, and what a box is drawn as. */
export type FlowTool = 'input' | 'textInput' | 'dynamicInput' | 'output' | 'test' | 'join' | 'union' | 'appendFields' | 'fuzzyMatch' | 'findReplace' | 'filter' | 'dateFilter' | 'customFilter' | 'formula' | 'multiField' | 'select' | 'sort' | 'sample' | 'unique' | 'recordId'
  | 'multiRow' | 'tile' | 'binning' | 'generateRows'
  | 'datetime' | 'cleanse' | 'impute' | 'dynamicReplace' | 'textToColumns' | 'regex' | 'jsonParse' | 'runningTotal' | 'summarize' | 'countRecords' | 'crosstab' | 'transpose' | 'browse'
  | 'pure' | 'macro' | 'parameter' | 'blockUntilDone' | 'message' | 'comment' | 'container';

/** Each tool's name, its group in the palette (Alteryx's: In/Out, Preparation, Join, Transform), and what it does. */
export const FLOW_TOOLS: Readonly<Record<FlowTool, { readonly name: string; readonly group: FlowGroup; readonly does: string }>> = {
  input: { name: 'Input', group: 'in', does: 'rows from a file, a table, a class or a saved query' },
  textInput: { name: 'Text Input', group: 'in', does: 'a small table of your own, typed in or pasted from a sheet' },
  dynamicInput: { name: 'Dynamic Input', group: 'in', does: 'the rows of one of several inputs, picked as it runs by a parameter of the page' },
  output: { name: 'Output', group: 'in', does: 'its input\'s rows written to a CSV file, now and each time the flow runs' },
  test: { name: 'Test', group: 'in', does: 'checks of its input\'s rows: no empty value, no value repeated, numbers within bounds, how many rows' },
  join: { name: 'Join', group: 'join', does: 'two inputs joined on their keys: Left above, Right below' },
  union: { name: 'Union', group: 'join', does: 'the rows of two inputs or more, one under another, columns by name: a line let go on it adds one' },
  appendFields: { name: 'Append Fields', group: 'join', does: 'each row of one input beside every row of another: a small table\'s values on each' },
  fuzzyMatch: { name: 'Fuzzy Match', group: 'join', does: 'two inputs joined where their text is alike: a few letters apart at most' },
  findReplace: { name: 'Find Replace', group: 'join', does: 'a column\'s values found in another input and replaced by theirs: a lookup' },
  filter: { name: 'Filter', group: 'prepare', does: 'the rows that meet its conditions' },
  dateFilter: { name: 'Date Filter', group: 'prepare', does: 'the rows whose date is in a range: this month, the last 30 days, or between two days' },
  customFilter: { name: 'Custom Filter', group: 'prepare', does: 'the rows a condition written in Pure is true for, the compiler checking it' },
  formula: { name: 'Formula', group: 'prepare', does: 'columns calculated from each row' },
  multiField: { name: 'Multi-Field', group: 'prepare', does: 'one formula over several columns, a new column of each' },
  select: { name: 'Select', group: 'prepare', does: 'the columns it keeps, and the names it hands them on by' },
  sort: { name: 'Sort', group: 'prepare', does: 'its rows in the order it sorts them' },
  sample: { name: 'Sample', group: 'prepare', does: 'its first or last rows (of each group), all but its first, or one of every N' },
  unique: { name: 'Unique', group: 'prepare', does: 'a row of each key, its first: U the unique rows, D the duplicates' },
  recordId: { name: 'Record ID', group: 'prepare', does: 'a number for each row, in an order it is given' },
  multiRow: { name: 'Multi-Row', group: 'prepare', does: 'a column of the row before or after each one' },
  tile: { name: 'Tile', group: 'prepare', does: 'each row put in one of a number of equal buckets, by an order' },
  binning: { name: 'Binning', group: 'prepare', does: 'number columns put into bins of equal width over each one\'s range' },
  generateRows: { name: 'Generate Rows', group: 'prepare', does: 'each row repeated for each number from one to another, or each day between two dates' },
  datetime: { name: 'DateTime', group: 'prepare', does: 'a date taken to its day, week, month, quarter or year' },
  cleanse: { name: 'Data Cleansing', group: 'prepare', does: 'text tidied: blanks, punctuation, spaces, its case' },
  impute: { name: 'Imputation', group: 'prepare', does: 'empty numbers filled: a value, or the column\'s average, median, smallest or largest' },
  dynamicReplace: { name: 'Dynamic Replace', group: 'prepare', does: 'values replaced by rules read from another box: a column, a condition and a value each' },
  textToColumns: { name: 'Text To Columns', group: 'parse', does: 'a text column split on a delimiter, a column a part' },
  regex: { name: 'RegEx', group: 'parse', does: 'a pattern over text: whether it matches, its matches replaced or taken out' },
  jsonParse: { name: 'JSON Parse', group: 'parse', does: 'a JSON column\'s fields made columns of their own: a value, a count, each element a row' },
  runningTotal: { name: 'Running Total', group: 'transform', does: 'a column\'s total so far, row by row' },
  summarize: { name: 'Summarize', group: 'transform', does: 'a row per group, its measures beside its keys' },
  countRecords: { name: 'Count Records', group: 'transform', does: 'how many rows it has: one row, one column, its count' },
  crosstab: { name: 'Cross Tab', group: 'transform', does: 'a column\'s values made columns across, a row per group' },
  transpose: { name: 'Transpose', group: 'transform', does: 'columns made rows: a Name and a Value for each, its keys beside them' },
  browse: { name: 'Browse', group: 'in', does: 'its input\'s rows, as they come: to look at, profile or chart' },
  pure: { name: 'Pure', group: 'transform', does: 'its rows a function of its input\'s, written in Pure: whatever no other tool says, the compiler checking it' },
  macro: { name: 'Macro', group: 'transform', does: 'a sub-flow saved as one box (a box\'s menu, Save as Macro…): its steps, used after any box' },
  parameter: { name: 'Parameter', group: 'in', does: 'a value of the flow\'s app, set above the canvas, that any Pure on the page reads as $name' },
  blockUntilDone: { name: 'Block Until Done', group: 'in', does: 'its input\'s rows, the boxes after it run -- and their Outputs written -- once every Output before it is written' },
  message: { name: 'Message', group: 'in', does: 'its input\'s rows, and words in the run\'s log when they are as it asks: a note, a warning, or an error that stops the run' },
  comment: { name: 'Comment', group: 'docs', does: 'a note on the canvas, in words of your own' },
  container: { name: 'Container', group: 'docs', does: 'a frame round boxes: moved together, folded away' },
};

/** A palette group (Alteryx's: In/Out, Preparation, Join, Parse, Transform, Documentation). */
export type FlowGroup = 'in' | 'prepare' | 'join' | 'parse' | 'transform' | 'docs';

/** The tools a person adds after a box, in the palette's order. */
export const PALETTE: readonly FlowTool[] = [
  'input', 'textInput', 'dynamicInput', 'output', 'test', 'browse', 'filter', 'dateFilter', 'customFilter', 'formula', 'multiField', 'select', 'sort', 'sample', 'unique', 'recordId', 'multiRow', 'tile', 'binning', 'generateRows', 'datetime', 'cleanse', 'impute', 'dynamicReplace',
  'join', 'union', 'appendFields', 'fuzzyMatch', 'findReplace', 'textToColumns', 'regex', 'jsonParse', 'summarize', 'countRecords', 'crosstab', 'transpose', 'runningTotal', 'pure', 'macro', 'parameter', 'blockUntilDone', 'message', 'comment', 'container',
];

/**
 * WHERE A TOOL IS SET (§3.4: the settings pane, made of DataCube's own editors): the grid's window that sets what the
 * tool does -- its filter, a calculated column, or a section of its Properties: the columns it shows, its sorts, its
 * row groups. An Input and a Browse set nothing of their own.
 */
export type StepEditor =
  | { readonly window: 'filters' }
  /** The calculated column's editor -- a new column, or (`start`) one that starts as a window column of the tool's. */
  | { readonly window: 'column'; readonly start?: ColumnEditorStart }
  | { readonly window: 'properties'; readonly tab: EditorTab }
  /** The Sample window (ui/sample-form.ts): the grid's Top N, said as its first or last rows by a column. */
  | { readonly window: 'sample' }
  /** The Imputation window (ui/impute-form.ts): the number columns filled, and with what. */
  | { readonly window: 'impute' }
  /** The Dynamic Replace window (ui/dynamic-replace-form.ts): the box its rules are loaded from, and their columns --
   *  opened again on a box's rows as its page read them (`rules`: the rows, or why they could not be read). */
  | { readonly window: 'dynamicReplace'; readonly rules?: { readonly box: string; readonly rows?: ResultTable; readonly problem?: string; readonly expressions?: boolean } }
  /** The Date Filter window (ui/date-filter-form.ts): a date column, and the range of its dates kept. */
  | { readonly window: 'dateFilter' }
  /** The Test window (ui/test-form.ts): its checks, each of a column. */
  | { readonly window: 'test' }
  /** The Custom Filter window (ui/custom-filter-form.ts): its condition in Pure, over `$x`, the row. */
  | { readonly window: 'customFilter' }
  /** The Binning window (ui/binning-form.ts): its number columns, and how many bins each is put in. */
  | { readonly window: 'binning' }
  /** The Generate Rows window (ui/generate-rows-form.ts): its new column, and its numbers or its days. */
  | { readonly window: 'generateRows' }
  /** The Pure window (ui/pure-form.ts): its rows' function of its input's, in Pure over `$input`. */
  | { readonly window: 'pure' }
  /** The column editor opened on a JSON column's fields (ui/json-fields.ts), the box's first JSON column's. */
  | { readonly window: 'jsonParse' }
  /** The Unique window (ui/unique-form.ts): the columns a key is, and the order a key's first row is first in. */
  | { readonly window: 'unique' }
  /** The Multi-Field Formula window (ui/multi-field-form.ts): its columns, its expression over each one's value. */
  | { readonly window: 'multiField' }
  /** The Transpose window (ui/transpose-form.ts): the columns made rows, the keys kept beside them. */
  | { readonly window: 'transpose' }
  /** The Select window (ui/select-form.ts): each column kept or left out, and renamed. */
  | { readonly window: 'select' }
  /** The Summarize window (ui/summarize-form.ts): each column grouped by, aggregated, or left out. */
  | { readonly window: 'summarize' }
  /** The Find Replace window (ui/find-replace-form.ts): the column its Join found among theirs, and theirs it becomes. */
  | { readonly window: 'findReplace' }
  /** Count Records (count-records.ts), which has no settings: opened, its grid counts its rows -- one step. */
  | { readonly window: 'countRecords' }
  /** The DateTime window (ui/date-form.ts): a date column taken to its period, the grid's own period preset. */
  | { readonly window: 'datetime' }
  /** A text tool's window (ui/text-tool-form.ts): Text To Columns, RegEx or Data Cleansing (text-tools.ts). */
  | { readonly window: 'text'; readonly kind: TextTool['kind'] }
  /** The Join window (ui/join-dialog.ts), over the box's two inputs: a Join's, a Union's (its Append). Opened when asked,
   *  never by a click on the box -- a window over the page, not docked. */
  | { readonly window: 'join' };

export const TOOL_EDITORS: Readonly<Partial<Record<FlowTool, StepEditor>>> = {
  filter: { window: 'filters' },
  dateFilter: { window: 'dateFilter' },
  customFilter: { window: 'customFilter' },
  test: { window: 'test' },
  jsonParse: { window: 'jsonParse' },
  formula: { window: 'column' },
  multiField: { window: 'multiField' },
  transpose: { window: 'transpose' },
  select: { window: 'select' },
  sort: { window: 'properties', tab: 'Sorts' },
  summarize: { window: 'summarize' },
  crosstab: { window: 'properties', tab: 'Horizontal Pivots' },
  sample: { window: 'sample' },
  unique: { window: 'unique' },
  impute: { window: 'impute' },
  dynamicReplace: { window: 'dynamicReplace' },
  datetime: { window: 'datetime' },
  cleanse: { window: 'text', kind: 'cleanse' },
  textToColumns: { window: 'text', kind: 'split' },
  regex: { window: 'text', kind: 'regex' },
  // a window column, started as the tool's (snapshot.ts WindowSpec): the person picks its order, its column
  recordId: { window: 'column', start: { name: 'RecordID', window: { fn: 'rowNumber', partition: [], order: [] } } },
  multiRow: { window: 'column', start: { name: 'Previous', window: { fn: 'lag', partition: [], order: [], offset: 1 } } },
  tile: { window: 'column', start: { name: 'Tile', window: { fn: 'ntile', partition: [], order: [], buckets: 4 } } },
  binning: { window: 'binning' },
  generateRows: { window: 'generateRows' },
  pure: { window: 'pure' },
  runningTotal: { window: 'column', start: { name: 'RunningTotal', window: { fn: 'sum', partition: [], order: [], frame: 'running' } } },
  join: { window: 'join' },
  union: { window: 'join' },
  dynamicInput: { window: 'join' },
  appendFields: { window: 'join' },
  fuzzyMatch: { window: 'join' },
  findReplace: { window: 'findReplace' },
};

/** What a box does, as the canvas says it: its tool (the first its query applies), any others it applies besides,
 *  and a line of what it is set to. */
export interface BoxSays {
  readonly tool: FlowTool;
  readonly also: readonly FlowTool[];
  readonly says: string;
}

/**
 * WHAT A BOX DOES, from its cube document: a box over no other grid's rows is an Input (a join's, a Join); over another
 * grid's rows, each step its query takes, in the order a grid takes them (calculated columns, the filter, the grouping,
 * the sorts; the columns it hides beside them) -- none, a Browse.
 */
export function boxSays(cube: CubeDocument): BoxSays {
  const source = cube.source;
  // a join's form says it: an Append is a Union, every pair an Append Fields, any other a Join (join-how.ts joinWords)
  if (source._type === 'join') {
    // a Join whose column is handed on replaced by one of theirs (find-replace.ts): a Find Replace
    const replaced = cube.query.derived.map(findReplaceOf).find((r) => r !== undefined
      && inPlaceOf(cube.query.derived, cube.configuration, () => [r.column]).some((p) => p.column === r.column));
    if (replaced !== undefined) return { tool: 'findReplace', also: [], says: `${replaced.column} replaced by ${replaced.with}` };
    // a Union of more than two: the rows of each
    const n = 2 + (source.more?.length ?? 0);
    return { tool: joinTool(source.how), also: [], says: n > 2 && !(source.how.form === 'append' && source.how.pick !== undefined)
      ? `the rows of all ${n}, columns by name` : joinWords(source.how) };
  }
  if (source._type !== 'rows') return { tool: 'input', also: [], says: source.name };
  const q = cube.query;
  // a Pure box's function (pure-box.ts): its source's, before any step its query takes
  const steps: { tool: FlowTool; says: string }[] = source.pure !== undefined ? [{ tool: 'pure', says: pureWords(source.pure) }] : [];
  // a Test: its checks' failing rows counted, a column of one value grouped by (checks.ts) -- its own, all of it
  const checks = checksOf(q);
  if (checks !== undefined) return { tool: 'test', also: [], says: checks.map(checkWords).join(', ') };
  // Count Records: a column of one value grouped by, a 1 for each row summed (count-records.ts) -- its own, all of it
  const counted = countRecordsOf(q);
  if (counted !== undefined) return { tool: 'countRecords', also: [], says: `its rows counted: ${counted.name}` };
  // Unique: a row number per key, its first rows kept, the number hidden (unique.ts) -- one step, before the others
  const unique = uniqueOf(q);
  if (unique !== undefined) steps.push({ tool: 'unique', says: `by ${unique.keys.join(', ')}` });
  // a Sample by row number (row-sample.ts): its numbers and its filter one step, as Unique's are
  const sampled = unique === undefined ? rowSampleOf(q) : undefined;
  if (sampled !== undefined) steps.push({ tool: 'sample', says: rowSampleWords(sampled) });
  // Transpose: its Name and Value one step, the columns it hides its own (transpose.ts)
  const transposed = transposeOf(q.derived);
  if (transposed !== undefined) steps.push({ tool: 'transpose', says: `${transposed.columns.join(', ')} to rows` });
  // a Select's retypes: a conversion of a column it hides (select-columns.ts), said as the Select's
  const hiddenNow = new Set(hiddenColumns(cube));
  const retyped = retypesOf(q.derived).filter((r) => hiddenNow.has(r.column));
  // a tool's columns made in place of the columns they are made of (box-columns.ts): said so, theirs no Select's
  const placed = inPlaceOf(q.derived, cube.configuration, (d) => [...derivedReads(d)])
    .filter((p) => !retyped.some((r) => r.name === p.made));
  const placedMade = new Set(placed.map((p) => p.made));
  // a Summarize leaves out what it neither groups by nor aggregates (summarize.ts): hidden, its own, no Select's
  const summarized = q.rows.length > 0 && q.pivotOn.length === 0;
  // a Custom Filter's condition: its column and its filter one step (custom-filter.ts)
  const custom = customFilterOf(q);
  const own = new Set([...(unique?.columns ?? []), ...(sampled?.columns ?? []), ...(custom !== undefined ? [CUSTOM_FILTER_COLUMN] : []),
    ...(transposed !== undefined || summarized ? [...hiddenColumns(cube)] : []),
    ...(transposed !== undefined ? [transposed.name, transposed.value] : []), ...retyped.map((r) => r.name)]);
  // Imputation's window columns (an average across every row) are its own, said with the column they fill
  const helpers = new Set(q.derived.flatMap((d) => imputedOf(d, q.derived)?.helper ?? []));
  // a Multi-Field Formula's columns: one formula, each over its own column
  const multi = multiFieldOf(q.derived);
  // a Binning's: each bin of a column, its range and upper edge its own (binning.ts binsOf)
  const binned = binsOf(q.derived);
  for (const h of binHelpers(binned)) helpers.add(h);
  // a Generate Rows': its repeats' column, a days' number its own (generate-rows.ts)
  const generated = generatedOf(q.derived);
  for (const h of generatedHelpers(generated)) helpers.add(h);
  // its calculated columns, each by the tool that makes one so -- the ones of a tool together, in the order they come
  for (const d of q.derived) {
    if (own.has(d.name) || helpers.has(d.name)) continue;
    const bin = binned.get(d.name);
    const made = generated !== undefined && generated.name === d.name ? generated : undefined;
    const tool = made !== undefined ? 'generateRows' : bin !== undefined ? 'binning' : columnTool(d, q.derived, multi.get(d.name));
    const last = steps.at(-1);
    const said = made !== undefined ? generatedWords(made) : bin !== undefined ? `${bin.column} in ${bin.bins} bins`
      : `${columnWords(d, q.derived, multi.get(d.name))}${placedMade.has(d.name) ? ' in place' : ''}`;
    // a tool's columns said once each way: a split's parts are one split, a Multi-Field's columns one formula
    if (last?.tool === tool) last.says = last.says === said || last.says.split(', ').includes(said) ? last.says : `${last.says}, ${said}`;
    else steps.push({ tool, says: said });
  }
  // a Unique's filter and a Sample's are their own; one column's dates in a range a Date Filter's; any other, a Filter's
  if (q.filter !== undefined && unique === undefined && sampled === undefined) {
    const dated = datesKept(q.filter);
    steps.push(custom !== undefined ? { tool: 'customFilter', says: customFilterWords(custom) }
      : dated !== undefined ? { tool: 'dateFilter', says: datesKeptWords(dated) } : { tool: 'filter', says: conditionWords(q.filter) });
  }
  if (q.topN !== undefined) steps.push({ tool: 'sample', says: `${q.topN.bottom === true ? 'first' : 'last'} ${q.topN.n} by ${q.topN.by}` });
  if (q.pivotOn.length > 0) {
    steps.push({ tool: 'crosstab', says: `${q.pivotOn.join(', ')} across${q.rows.length > 0 ? `, by ${q.rows.join(', ')}` : ''}` });
  } else if (q.rows.length > 0) steps.push({ tool: 'summarize', says: `by ${q.rows.join(', ')}` });
  if (q.sorts.length > 0) steps.push({ tool: 'sort', says: q.sorts.map((s) => `${s.column} ${s.direction === 'desc' ? '↓' : '↑'}`).join(', ') });
  const hidden = hiddenColumns(cube).filter((c) => !own.has(c) && !helpers.has(c) && !retyped.some((r) => r.column === c)
    && !placed.some((p) => p.column === c));
  // a Select's renames: the names it hands its columns on by (box-columns.ts); its retypes, by the names they are given
  const as = new Map(boxRenamed(q, cube.configuration).map((r) => [r.from, r.to] as const));
  const renames = [...as].filter(([from]) => !retyped.some((r) => r.name === from) && !placedMade.has(from));
  // a Select's order (box-columns.ts boxOrder): its columns moved -- its conversions, and a tool's columns made in place
  // (box-columns.ts inTheirPlace), in their columns' places aside
  const order = boxOrder(q, cube.configuration);
  const moved = order !== undefined && (() => {
    const back = new Map([...retyped.map((r) => [r.name, r.column] as const), ...placed.map((p) => [p.made, p.column] as const)]);
    const omitted = new Set(boxOmitted(q, cube.configuration));
    const asIs = rowColumns(q).map((c) => c.name).filter((c) => !back.has(c)
      && (!omitted.has(c) || retyped.some((r) => r.column === c) || placed.some((p) => p.column === c)));
    return order.map((c) => back.get(c) ?? c).some((c, i) => c !== asIs[i]);
  })();
  if (hidden.length > 0 || renames.length > 0 || retyped.length > 0 || moved) {
    const renamed = renames.length === 0 ? undefined
      : renames.length <= 2 ? renames.map(([from, to]) => `${from} → ${to}`).join(', ') : `${renames.length} renamed`;
    const types = retyped.length === 0 ? undefined : retyped.length <= 2
      ? retyped.map((r) => `${r.column}${(as.get(r.name) ?? r.column) !== r.column ? ` → ${as.get(r.name)}` : ''} as ${r.to}`).join(', ')
      : `${retyped.length} retyped`;
    const left = hidden.length === 0 ? undefined : `${hidden.length} of ${q.columns.length} columns left out`;
    steps.push({ tool: 'select', says: [renamed, types, left, moved ? 'columns reordered' : undefined].filter((w) => w !== undefined).join('; ') });
  }
  const first = steps[0];
  if (first === undefined) return { tool: 'browse', also: [], says: 'every row' };
  return { tool: first.tool, also: steps.slice(1).map((s) => s.tool), says: first.says };
}

/** The tool a join is: an Append a Union -- one that keeps one input's rows, picked as it runs, a Dynamic Input --
 *  every row with every row an Append Fields (join-how.ts isEveryPair), one on similar text a Fuzzy Match (isFuzzy), any
 *  other a Join: the box's, and the Join window's start when it is set again. */
export type JoinTool = 'join' | 'union' | 'dynamicInput' | 'appendFields' | 'fuzzyMatch';
export function joinTool(how: JoinHow): JoinTool {
  return how.form === 'append' ? how.pick !== undefined ? 'dynamicInput' : 'union'
    : isEveryPair(how) ? 'appendFields' : isFuzzy(how) ? 'fuzzyMatch' : 'join';
}

/** The tool a calculated column is made by: a window column's by its function (a row number, the row before or after,
 *  a bucket, a running total); one of a formula over several columns (`multi`: multiFieldOf), a Multi-Field's; any
 *  other a Formula's. */
function columnTool(d: DerivedColumn, derived: readonly DerivedColumn[], multi: MultiFieldColumn | undefined): FlowTool {
  const w = d.window;
  if (w === undefined) {
    return grainOf(d) !== undefined || dateToolOf(d) !== undefined ? 'datetime'
      : imputedOf(d, derived) !== undefined ? 'impute' : replacedOf(d) !== undefined ? 'dynamicReplace' : textToolOf(d)?.tool
        ?? (jsonFieldOf(d) !== undefined ? 'jsonParse' : multi !== undefined ? 'multiField' : 'formula');
  }
  if (w.fn === 'rowNumber') return 'recordId';
  if (w.fn === 'lag' || w.fn === 'lead') return 'multiRow';
  if (w.fn === 'ntile') return 'tile';
  return w.frame === 'running' ? 'runningTotal' : 'formula';
}

/** A calculated column, as its box says it: its name -- a period's, what it takes to what (`trade_date by month`); a
 *  text tool's, what it does to which (text-tools.ts textToolOf: `desk split on ','`). */
function columnWords(d: DerivedColumn, derived: readonly DerivedColumn[], multi: MultiFieldColumn | undefined): string {
  const grain = grainOf(d);
  if (grain !== undefined) return `${grain.column} by ${grain.grain}`;
  const dated = dateToolOf(d);
  if (dated !== undefined) return dated.says;
  const filled = imputedOf(d, derived);
  if (filled !== undefined) return `${filled.column}'s empties → ${filled.with.kind === 'value' ? filled.with.value : `its ${filled.with.kind}`}`;
  const replaced = replacedOf(d);
  if (replaced !== undefined) return replacedWords(replaced);
  const tool = textToolOf(d);
  if (tool !== undefined) return tool.says;
  const json = jsonFieldOf(d);
  if (json !== undefined) return `${d.name} from ${json}`;
  // a Multi-Field Formula said once, by the columns it is over
  return multi !== undefined ? `over ${multi.all.join(', ')}` : d.name;
}

/**
 * The JSON column a calculated column reads a field of (a JSON Parse's: the column editor's JSON fields, json-shape.ts,
 * write `$x.c->get('key')...`): the column whose `get` of a key it holds, anywhere in its body -- undefined for any other.
 */
export function jsonFieldOf(d: DerivedColumn): string | undefined {
  type Node = { readonly _type?: string; readonly function?: string; readonly property?: string; readonly name?: string;
    readonly parameters?: readonly Node[]; readonly body?: readonly Node[]; readonly value?: unknown };
  // `$x.c->get('a')->get('b')`: its keys each a text, its root a column of the row
  const root = (n: Node | undefined): string | undefined => {
    if (n?._type === 'property' && n.parameters?.[0]?._type === 'var' && typeof n.property === 'string') return n.property;
    return n?._type === 'func' && n.function === 'get' && n.parameters?.[1]?._type === 'string' ? root(n.parameters[0]) : undefined;
  };
  const find = (n: Node | undefined): string | undefined => {
    if (n === undefined || n === null || typeof n !== 'object') return undefined;
    if (n._type === 'func' && n.function === 'get' && n.parameters?.[1]?._type === 'string') {
      const c = root(n.parameters[0]);
      if (c !== undefined) return c;
    }
    for (const p of [...(n.parameters ?? []), ...(n.body ?? [])]) {
      const c = find(p);
      if (c !== undefined) return c;
    }
    return undefined;
  };
  return d.lambda === undefined ? undefined : find(d.lambda as unknown as Node);
}

/** The columns a cube's configuration hides (Column Properties' Hide): what a Select box leaves out. */
function hiddenColumns(cube: CubeDocument): string[] {
  const columns = cube.configuration['columns'];
  if (columns === null || typeof columns !== 'object' || Array.isArray(columns)) return [];
  return Object.entries(columns as Record<string, unknown>)
    .filter(([, c]) => c !== null && typeof c === 'object' && (c as Record<string, unknown>)['hidden'] === true)
    .map(([name]) => name);
}

/** AN INPUT OF A BOX (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2): the side of its source that reads another grid's rows --
 *  a box's one input, a join's left and right -- and the box it reads there live, or the name of the copy it reads
 *  (frozen: FL6). The line into it, and its settings' Input. */
export type BoxInput =
  | { readonly side: InputSide; readonly grid: string; readonly port?: InputPort }
  | { readonly side: InputSide; readonly frozen: string };

/** A box's inputs, from its cube document: none for an Input (it reads a source of its own). */
export function boxInputs(cube: CubeDocument): BoxInput[] {
  return inputSides(cube.source).map((side) => {
    const input = inputOn(cube.source, side);
    return isLive(input) ? { side, grid: input.grid, ...(input.port !== undefined ? { port: input.port } : {}) } : { side, frozen: copyName(input) };
  });
}

/** A BOX'S NAME, as the canvas says it (its box, its run's log) and a Join's title is made of: its title -- unless it is
 *  the one every new grid is given (page-ids.ts gridTitle) -- else its tool's (`tool`, boxSays'). One rule for the
 *  canvas, the Join window (app/join-flow.ts) and Python's flow join (headless joinCube; the review of 2026-10-26's B3). */
export function boxNamed(id: string, title: string | undefined, tool: FlowTool): string {
  return title === undefined || title === gridTitle(id) ? FLOW_TOOLS[tool].name : title;
}

/** Box `id`'s name on page `page`'s canvas (boxNamed): its title, else its tool's, read off its cube document. */
export function pageBoxName(page: Pick<PageDocument, 'views' | 'cubes'>, id: string): string {
  const view = page.views.find((v) => v.id === id);
  const cube = view?.kind === 'grid' ? page.cubes.find((c) => c.id === view.cube)?.cube : undefined;
  return boxNamed(id, view?.title, cube === undefined ? 'browse' : boxSays(cube).tool);
}

/** What a frozen copy is called on a flow: the box it copies, as the canvas named it -- its title, else its tool's
 *  name (a copy of a Filter is "Filter", never the file under it); an Input's, its source's. */
function copyName(input: JoinInput): string {
  if (input.name !== input.source.name) return input.name;
  const tool = boxSays(inputCube(input)).tool;
  return tool === 'input' ? input.name : FLOW_TOOLS[tool].name;
}

/**
 * THE BOXES THAT READ `id`, through others too (`documentOf`: each box's document; `boxes`, the flow's): what `id` can
 * never be linked to, a box reading its own rows.
 */
export function readersOf(id: string, boxes: readonly string[], documentOf: (id: string) => CubeDocument | undefined): Set<string> {
  const readers = new Set<string>();
  const reads = new Map(boxes.map((b) => {
    const doc = documentOf(b);
    return [b, doc === undefined ? [] : boxInputs(doc).flatMap((i) => ('grid' in i ? [i.grid] : []))] as const;
  }));
  for (let grown = true; grown;) {
    grown = false;
    for (const [b, inputs] of reads) {
      if (readers.has(b) || !inputs.some((g) => g === id || readers.has(g))) continue;
      readers.add(b);
      grown = true;
    }
  }
  return readers;
}
