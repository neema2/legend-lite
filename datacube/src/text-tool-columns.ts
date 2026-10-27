// THE TEXT TOOLS' COLUMNS (text-tools.ts, docs/DATACUBE_FLOW_MODE_2026_10_24.md M3): the calculated columns Text To
// Columns, RegEx and Data Cleansing add, and the commands that add them -- each a `calculate` at the row stage, as the
// column editor's Add adds a column. Fetched with the first tool set, never at startup.
//
// PURE: no DOM, no planner.

import { fn, lambda, lit, property, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { inPlaceCommand } from './box-columns.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, type DerivedColumn } from './snapshot.ts';
import { CLEAN_SUFFIX, PUNCTUATION, SPACES, type Cleansing, type RegexMode, type TextTool } from './text-tools.ts';

const x = (): ValueSpecification => variable('x');
const col = (name: string): ValueSpecification => property(x(), name);
const one = (name: string): ValueSpecification => fn('toOne', col(name));
const row = (body: ValueSpecification): Lambda => lambda(['x'], body);

/** What is wrong with a tool's settings, in words, if anything. */
export function textToolProblem(t: TextTool): string | undefined {
  switch (t.kind) {
    case 'split':
      if (t.delimiter === '') return 'it splits on a delimiter: type one';
      if (t.rows !== true && (!Number.isInteger(t.parts) || t.parts < 1 || t.parts > 50)) return 'it makes from 1 to 50 columns';
      return t.name.trim() === '' ? 'its columns need a name' : undefined;
    case 'regex':
      if (t.pattern === '') return 'it needs a pattern';
      return t.name.trim() === '' ? 'its column needs a name' : undefined;
    case 'cleanse': {
      if (t.columns.length === 0) return 'pick a column to cleanse';
      const c = t.cleansing;
      return c.blanks || c.punctuation || c.spaces || c.trim || c.letterCase !== undefined ? undefined : 'pick what to do to them';
    }
  }
}

/**
 * The calculated columns a tool adds: Text To Columns a column a part, `<name>_1`..`<name>_N` (its parts counted from
 * 1, as a person counts them; `splitPart` counts from 0), or into rows one, `name`, exploded; RegEx one, `name`; Data
 * Cleansing a column each,
 * `<column>_clean`. Each a dimension. Its settings refused in words when they make no column.
 */
export function textToolColumns(t: TextTool): DerivedColumn[] {
  const problem = textToolProblem(t);
  if (problem !== undefined) throw new CubeRefusal(problem);
  switch (t.kind) {
    case 'split':
      // into rows: one column, each row once for each of its parts (an explode, as the column editor's Explode makes) --
      // a row whose text is empty kept, its one part empty text, as an empty text's is (an explode of no parts is no row:
      // the correctness audit's 7, 2026-10-26)
      if (t.rows === true) {
        return [{
          name: t.name.trim(), lambda: row(fn('split', fn('coalesce', col(t.column), lit.string('')), lit.string(t.delimiter))),
          kind: 'dimension', unnest: true,
        }];
      }
      return Array.from({ length: t.parts }, (_, i) => ({
        name: `${t.name.trim()}_${i + 1}`,
        lambda: row(fn('splitPart', col(t.column), lit.string(t.delimiter), lit.integer(i))),
        kind: 'dimension' as const,
      }));
    case 'regex':
      return [{ name: t.name.trim(), lambda: row(regex(t)), kind: 'dimension' }];
    case 'cleanse':
      return t.columns.map((c) => ({ name: `${c}${CLEAN_SUFFIX}`, lambda: row(cleansed(c, t.cleansing)), kind: 'dimension' as const }));
  }
}

function regex(t: Extract<TextTool, { kind: 'regex' }>): ValueSpecification {
  const value = one(t.column);
  const pattern = lit.string(t.pattern);
  switch (t.mode) {
    case 'match':
      return fn('regexpLike', value, pattern);
    case 'replace':
      return fn('regexpReplace', value, pattern, lit.string(t.replacement ?? ''), lit.boolean(true));
    case 'extract':
      // its first match, or none: a value with no match is empty, never an error
      return fn('first', fn('regexpExtract', value, pattern, lit.boolean(false)));
  }
}

/** A column cleansed: its empties blank first, then its punctuation out, its spaces one, its ends trimmed, its case. */
function cleansed(column: string, c: Cleansing): ValueSpecification {
  let v = c.blanks ? fn('coalesce', col(column), lit.string('')) : one(column);
  if (c.punctuation) v = fn('regexpReplace', v, lit.string(PUNCTUATION), lit.string(''), lit.boolean(true));
  if (c.spaces) v = fn('regexpReplace', v, lit.string(SPACES), lit.string(' '), lit.boolean(true));
  if (c.trim) v = fn('trim', v);
  if (c.letterCase === 'upper') v = fn('toUpper', v);
  if (c.letterCase === 'lower') v = fn('toLower', v);
  return v;
}

/** The commands a tool adds its columns by, in one step: a row-stage `calculate` each, in order. */
export function textToolCommands(t: TextTool): GridCommand[] {
  const commands = textToolColumns(t).map((column): GridCommand => ({ kind: 'calculate', level: 'row', column }));
  // in place: each cleansed column handed on by its column's name
  return t.kind === 'cleanse' && t.inPlace === true
    ? [...commands, inPlaceCommand(t.columns.map((c) => ({ column: c, made: `${c}${CLEAN_SUFFIX}` })))] : commands;
}

/** The name a RegEx's column is given unless one is named -- `<column>_matches`, `_replaced`, `_extracted` -- its
 *  window's and Python's alike (the review of 2026-10-26: each spelt it). */
export function regexName(column: string, mode: RegexMode): string {
  return `${column}_${mode === 'match' ? 'matches' : mode === 'replace' ? 'replaced' : 'extracted'}`;
}

/** The kinds a text tool is, as Python names them (`grid.split`, `grid.regex`, `grid.cleanse`). */
export const TEXT_TOOL_KINDS: readonly TextTool['kind'][] = ['split', 'regex', 'cleanse'];

/** A text tool read from JSON (Python's): its kind one of TEXT_TOOL_KINDS, its fields those the kind takes, each of its
 *  type -- or refused in words. Its settings are textToolProblem's to judge, its columns the grid's. */
export function readTextTool(raw: unknown): TextTool {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined) throw new CubeRefusal(`a text tool is {kind, ...}, not ${JSON.stringify(raw)}`);
  const only = (keys: readonly string[]): void => {
    const odd = Object.keys(o).filter((k) => !['kind', ...keys].includes(k));
    if (odd.length > 0) throw new CubeRefusal(`a '${String(o['kind'])}' tool takes ${keys.join(', ')}, not ${odd.join(', ')}`);
  };
  const text = (key: string): string => {
    const v = o[key];
    if (typeof v !== 'string') throw new CubeRefusal(`its ${key} is text, not ${JSON.stringify(v)}`);
    return v;
  };
  const yes = (key: string): boolean => {
    const v = o[key];
    if (v !== undefined && typeof v !== 'boolean') throw new CubeRefusal(`its ${key} is true or false, not ${JSON.stringify(v)}`);
    return v === true;
  };
  let tool: TextTool;
  switch (o['kind']) {
    case 'split': {
      only(['column', 'delimiter', 'parts', 'name', 'rows']);
      const rows = yes('rows');
      // into rows, no number of parts asked
      const parts = o['parts'] ?? (rows ? 1 : undefined);
      if (typeof parts !== 'number') throw new CubeRefusal(`its parts is a number, not ${JSON.stringify(parts)}`);
      tool = { kind: 'split', column: text('column'), delimiter: text('delimiter'), parts, name: text('name'), ...(rows ? { rows } : {}) };
      break;
    }
    case 'regex': {
      only(['column', 'pattern', 'mode', 'replacement', 'name']);
      const mode = o['mode'];
      if (mode !== 'match' && mode !== 'replace' && mode !== 'extract') {
        throw new CubeRefusal(`its mode is match, replace or extract, not ${JSON.stringify(mode)}`);
      }
      if (o['replacement'] !== undefined && mode !== 'replace') throw new CubeRefusal(`a replacement is a replace's, not a ${mode}'s`);
      // its name, unless given, its column's (regexName)
      tool = {
        kind: 'regex', column: text('column'), pattern: text('pattern'), mode, name: o['name'] === undefined ? regexName(text('column'), mode) : text('name'),
        ...(o['replacement'] !== undefined ? { replacement: text('replacement') } : {}),
      };
      break;
    }
    case 'cleanse': {
      only(['columns', 'blanks', 'punctuation', 'spaces', 'trim', 'letterCase', 'inPlace']);
      const columns = o['columns'];
      if (!Array.isArray(columns) || !columns.every((c): c is string => typeof c === 'string')) {
        throw new CubeRefusal(`its columns are a list of names, not ${JSON.stringify(columns)}`);
      }
      const letterCase = o['letterCase'];
      if (letterCase !== undefined && letterCase !== 'upper' && letterCase !== 'lower') {
        throw new CubeRefusal(`its letterCase is upper or lower, not ${JSON.stringify(letterCase)}`);
      }
      const cleansing: Cleansing = {
        ...(yes('blanks') ? { blanks: true } : {}), ...(yes('punctuation') ? { punctuation: true } : {}),
        ...(yes('spaces') ? { spaces: true } : {}), ...(yes('trim') ? { trim: true } : {}),
        ...(letterCase !== undefined ? { letterCase } : {}),
      };
      tool = { kind: 'cleanse', columns, cleansing, ...(yes('inPlace') ? { inPlace: true } : {}) };
      break;
    }
    default:
      throw new CubeRefusal(`a text tool is ${TEXT_TOOL_KINDS.join(', ')}, not ${JSON.stringify(o['kind'])}`);
  }
  const problem = textToolProblem(tool);
  if (problem !== undefined) throw new CubeRefusal(problem);
  return tool;
}
