// A TIMESERIES TILE AS CODE (the user, 2026-10-26: "an option that shows how exactly to build that chart
// programmatically from TS and python"; 2026-10-28: "give a generic snippet of python about how to create your own
// chart that a user can copy paste to just create exactly that tile in their own datacube"): the tile as a program of
// its own -- in Python, a page of its own (`ll.Page()`), a grid over the rows each source's grid reads (opened as that
// grid was: a file, a class, a warehouse table; else a frame of the person's), and the grid's `timeseries`
// (python/legend_lite/page.py), which asks DataCube's own reader (headless timeseriesMade) and applies the page command
// the screen's Create ▸ Timeseries sends; in TypeScript, that command itself (page-commands.ts addTimeseries) over the
// person's own cubes and sheet, applied by the one function every change to a page goes through (applyPageCommands).
// Each written from the tile's spec, each series its #TS text as the tile keeps it, so the program builds the tile it
// was written of -- the tests run both and read back the spec they made. Each formula is #TS, for the Pure its tile
// writes (askQuery): the Python prints each one's, the screen's Code shows them under the program (the user's "A",
// 2026-10-28).
//
// One rule for the viewer's Code window and Python's Timeseries.code (headless timeseriesCode).
//
// PURE: no DOM, no planner.

import type { CubeSource } from './cube-document.ts';
import { layoutTiles, type PageDocument } from './page-document.ts';
import { gridTitle } from './page-ids.ts';
import { CubeRefusal } from './snapshot.ts';
import { columnsOf, nextSeriesId } from './timeseries-expr.ts';
import { treeOf } from './timeseries-text.ts';
import { builderSource, type BuilderLanguage } from './timeseries-builder-source.ts';
import type { TimeseriesSpec, TsSeries, TsSource } from './timeseries-spec.ts';

/** The languages a tile's code is written in. */
export type TsCodeLanguage = 'python' | 'typescript';
export const TS_CODE_LANGUAGES: readonly TsCodeLanguage[] = ['python', 'typescript'];

/** What its sources' grids are, as its code names them: each grid's title and where its rows come from. */
export interface TsCodeOf {
  /** Its title, as its tile says it (null: the tile's own default). */
  readonly title: string | null;
  /** Each source's grid on the page, by the cube the source reads: its title and its cube's source (how the program
   *  opens a grid of its own over the same rows). A cube no grid shows (its grid removed, kept for the tile) has none:
   *  its program reads a frame of the person's. */
  readonly grids: Readonly<Record<string, { readonly title: string; readonly source?: CubeSource } | undefined>>;
}

/** A tile's settings besides its sources and series, in the order a program tells them. */
const SLOTS = ['grain', 'align', 'range', 'panes', 'annotations', 'view'] as const;

/** A series' keys besides its id and its expression, in the order a program tells them. */
const SERIES_KEYS = ['name', 'split', 'style', 'pane', 'axis', 'hidden'] as const;

/** A tile of a page document as its code names it: its title, its sources' grids and where their rows come from. */
export function codeOfPage(page: PageDocument, tile: string): { readonly spec: TimeseriesSpec; readonly of: TsCodeOf } {
  const view = page.views.find((v) => v.id === tile);
  if (!view || view.kind !== 'timeseries') throw new CubeRefusal(`'${tile}' is no timeseries tile of the page`);
  if (!page.sheets.some((s) => layoutTiles(s.layout).includes(tile))) throw new CubeRefusal(`'${tile}' is on no sheet of the page`);
  const grids: Record<string, { readonly title: string; readonly source?: CubeSource }> = {};
  for (const s of view.spec.sources) {
    const g = page.views.find((v) => v.kind === 'grid' && v.cube === s.cube);
    const source = page.cubes.find((c) => c.id === s.cube)?.cube.source;
    if (g !== undefined) grids[s.cube] = { title: g.title ?? gridTitle(g.id), ...(source !== undefined ? { source } : {}) };
  }
  return { spec: view.spec, of: { title: view.title ?? null, grids } };
}

/** The columns a source's rows must have for the tile: its date, its instrument column, each column a formula reads. */
function columnsRead(spec: TimeseriesSpec, alias: string): string[] {
  const src = spec.sources.find((s) => s.alias === alias);
  const out = [src?.date, src?.key].filter((c): c is string => c !== undefined);
  for (const s of spec.series) for (const c of columnsOf(spec, treeOf(s, spec.series)).get(alias) ?? []) if (!out.includes(c)) out.push(c);
  return out;
}

/** Words in a list: `a`, `a and b`, `a, b and c`. */
function listed(words: readonly string[]): string {
  return words.length <= 1 ? (words[0] ?? '') : `${words.slice(0, -1).join(', ')} and ${words.at(-1)!}`;
}

/** Every line end, control character and direction mark: none may stand in a program as written -- a line end ends a
 *  comment, a direction mark shows the code otherwise than it runs. */
const UNWRITTEN = /[\u0000-\u001f\u007f\u200e\u200f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g;

/** Text a comment says on one line: each of those a space, so a title, a name or a column (a person's words) ends no
 *  comment, in either language (the review, 2026-10-28: a title with a line end in it wrote a line of code). */
function oneLine(text: string): string {
  return text.replace(UNWRITTEN, ' ');
}

/** A string's characters as both languages' quoted strings take them: `\` and `'` escaped, a line end `\n`, every other
 *  control character by its code. */
function quoted(text: string): string {
  const escaped = text.replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n')
    .replace(UNWRITTEN, (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`);
  return `'${escaped}'`;
}

/** THE PROGRAM THAT BUILDS A TILE: `spec` on the sheet and over the grids `of` says, in `language`. */
export function tileCode(spec: TimeseriesSpec, of: TsCodeOf, language: TsCodeLanguage): string {
  switch (language) {
    case 'python':
      return pythonCode(spec, of);
    case 'typescript':
      return typescriptCode(spec, of);
  }
}

// ------------------------------------------------------------------ Python

/** A value as Python writes it: a dict, a list, a str, a number, True, False, None. */
function py(v: unknown): string {
  if (v === null || v === undefined) return 'None';
  if (v === true) return 'True';
  if (v === false) return 'False';
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) throw new Error(`${v} is no number a page holds`);
    return String(v);
  }
  if (typeof v === 'string') return quoted(v);
  if (Array.isArray(v)) return `[${v.map(py).join(', ')}]`;
  if (typeof v === 'object') return `{${Object.entries(v as Record<string, unknown>).map(([k, x]) => `${py(k)}: ${py(x)}`).join(', ')}}`;
  throw new Error(`a ${typeof v} is no value a page holds`);
}

/** Python's words no name may be. */
const PY_KEYWORDS = new Set(['False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue', 'def', 'del',
  'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass',
  'raise', 'return', 'try', 'while', 'with', 'yield']);

/** A grid's variable, named after its title: lower case, words joined by `_`, none the program's own names. */
function pyName(title: string, taken: Set<string>): string {
  const words = title.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  const base = words === '' ? 'grid' : /^[0-9]/.test(words) ? `grid_${words}` : PY_KEYWORDS.has(words) ? `${words}_grid` : words;
  let name = base;
  for (let n = 2; taken.has(name); n++) name = `${base}_${n}`;
  taken.add(name);
  return name;
}

/** EACH SERIES AS THE BUILDER'S CODE (B4: Show TypeScript, Show Python; timeseries-builder-source.ts) -- gs-quant's call
 *  of it -- or, where gs-quant has none of its meaning (a z-score to date, a compare by the tile's align), undefined: its
 *  #TS text stands in the program. */
function builderCode(spec: TimeseriesSpec, language: BuilderLanguage): (string | undefined)[] {
  return spec.series.map((s) => {
    try {
      return builderSource(treeOf(s, spec.series), language);
    } catch {
      return undefined;
    }
  });
}

/** The builder's names a program's code calls (its words, column, series, Window, an enumeration): what it imports, and
 *  what no variable of its own may be named. */
function namesUsed(code: readonly (string | undefined)[]): string[] {
  const out = new Set<string>();
  for (const c of code) {
    if (c === undefined) continue;
    // each name before a call or an enumeration's member, not a method's (a lookbehind: a call in a call is read too)
    for (const m of c.matchAll(/(?<![.\w$'])([A-Za-z_$][\w$]*)\s*\(/g)) out.add(m[1]!);
    for (const m of c.matchAll(/(?<![.\w$'])([A-Z][A-Za-z]*)\.[A-Za-z_][\w$]*(?![\w$]|\s*\()/g)) out.add(m[1]!);
    if (/\bnew Window\(|(^|[^.\w])Window\(/.test(c)) out.add('Window');
  }
  out.delete('new');
  return [...out].sort();
}

/** Each series as `page.timeseries` takes it: its term (the builder's code) or its #TS text alone, or a mapping of it and
 *  how it is drawn -- its id told where it is not the one the reader would give it (the next free letter, nextSeriesId). */
function pySeries(series: readonly TsSeries[], code: readonly (string | undefined)[]): string[] {
  return series.map((s, i) => {
    const natural = nextSeriesId(series.slice(0, i)) === s.id;
    const rest = SERIES_KEYS.filter((k) => s[k] !== undefined);
    const term = code[i];
    if (natural && rest.length === 0) return term ?? py(s.text);
    const told = [...(natural ? [] : [`'id': ${py(s.id)}`]), term !== undefined ? `'term': ${term}` : `'text': ${py(s.text)}`,
      ...rest.map((k) => `${py(k)}: ${py(s[k])}`)];
    return `{${told.join(', ')}}`;
  });
}

/** A grid of a program's own page over the rows a source's grid reads: opened as that grid was (a file, a remote file, a
 *  warehouse table, a class at its revision), else over a frame of the person's (a dataframe's, a saved query's, a
 *  join's or another grid's rows: none a program opens by name), `frame` its variable: its line. */
function pyGrid(name: string, frame: string, title: string, source: CubeSource | undefined, reads: readonly string[]): string {
  const rows = reads.length > 0 ? `, with ${listed(reads)}` : '';
  const said = `the grid "${title}"`;
  const line = (code: string, comment: string): string => `${name} = ${code}  # ${oneLine(comment)}`;
  switch (source?._type) {
    case 'file':
      return line(`page.grid(file=${py(source.name)})`, `${said}${source.sample !== undefined ? ` (DataCube's sample ${source.sample.id})` : ''}: your copy of its file${rows}`);
    case 'remoteFile':
      return line(`page.grid(remote=${py(source.url)})`, `${said}${rows}`);
    case 'warehouseTable':
      return line(`page.grid(warehouse=${py(source.warehouse)}, table=${py(`${source.schema}.${source.table}`)}, catalog=${py(source.catalog)})`, `${said}${rows}`);
    case 'class':
      return line(`page.grid(cls=${py(source.class)}, project=${py(source.project)}, revision=${py(source.revision)})`, `${said}${rows}`);
    case 'frame':
      return line(`page.grid(${frame}, name=${py(source.name)})`, `${said}: ${frame} your DataFrame of it${rows}`);
    case 'savedQuery':
    case 'join':
    case 'rows':
    case undefined: {
      const was = source === undefined ? 'its grid removed'
        : source._type === 'savedQuery' ? `the saved query "${source.name}" (or page.grid(query=its saved id))`
          : source._type === 'join' ? `a join, ${source.name}` : 'another grid\'s rows';
      return line(`page.grid(${frame})`, `${said}, ${was}: ${frame} your DataFrame of its rows${rows}`);
    }
  }
}

function pythonCode(spec: TimeseriesSpec, of: TsCodeOf): string {
  // each series the builder's code, where gs-quant has a call of it (B4)
  const code = builderCode(spec, 'python');
  const used = namesUsed(code);
  // the program's own names, and Python's and the builder's that it calls
  const taken = new Set(['ll', 'page', 'tile', 's', 'print', ...used]);
  const grids = new Map<string, string>();
  // a frame's variable by the name it is served under: two grids over one frame read the one DataFrame
  const frames = new Map<string, string>();
  const lines: string[] = [];
  for (const s of spec.sources) {
    if (grids.has(s.cube)) continue;
    const g = of.grids[s.cube];
    // its variable named after the frame it reads (the name the person gave it), else after its grid's title
    const served = g?.source?._type === 'frame' ? g.source.name : undefined;
    const name = pyName(served ?? g?.title ?? s.alias, taken);
    const frame = (served !== undefined ? frames.get(served) : undefined) ?? pyName(`${name}_frame`, taken);
    if (served !== undefined) frames.set(served, frame);
    grids.set(s.cube, name);
    const reads = spec.sources.filter((x) => x.cube === s.cube).flatMap((x) => columnsRead(spec, x.alias));
    lines.push(pyGrid(name, frame, g?.title ?? s.alias, g?.source, [...new Set(reads)]));
  }
  const slots = SLOTS.filter((k) => spec[k] !== undefined).map((k) => `    ${k}=${py(spec[k])},`);
  const title = of.title !== null ? [`    title=${py(of.title)},`] : [];
  // one source and no state kept: its grid's own timeseries; else the page's, each source a mapping
  const one = spec.sources.length === 1 && spec.sources[0]!.kept === undefined;
  const call = one ? (() => {
    const s = spec.sources[0]!;
    // no key said as none, never left for DataCube to choose one
    const told = [`alias=${py(s.alias)}`, `date=${py(s.date)}`, `key=${py(s.key ?? null)}`];
    return [`tile = ${grids.get(s.cube)!}.timeseries(`, ...pySeries(spec.series, code).map((x) => `    ${x},`), `    ${told.join(', ')},`, ...title, ...slots, ')'];
  })() : [
    'tile = page.timeseries(',
    `    [${spec.sources.map((s) => `{'alias': ${py(s.alias)}, 'grid': ${grids.get(s.cube)!}, 'date': ${py(s.date)}, 'key': ${py(s.key ?? null)}${s.kept !== undefined ? `, 'kept': ${py(s.kept)}` : ''}}`).join(', ')}],`,
    '    [',
    ...pySeries(spec.series, code).map((x) => `        ${x},`),
    '    ],',
    ...title,
    ...slots,
    ')',
  ];
  // its first two lines its own words alone: Python reads a `coding=` in either as the file's encoding (the second
  // review, 2026-10-28: a title `coding=utf-7 ...` decoded the rest, `title='...'` included, as other code)
  return [
    '# A timeseries tile built in Python on a page of your own -- run it, and DataCube shows the same tile. Each series is',
    '# gs-quant\'s call of it (legend_lite.timeseries: its names, its Window, its enumerations), or its #TS formula where',
    '# gs-quant has none -- each a Pure query the tile writes over its grid and the database runs: the loop at the end',
    `# prints each one's Pure. The tile: ${oneLine(of.title ?? 'untitled')}.`,
    'import legend_lite as ll',
    ...(used.length > 0 ? [`from legend_lite.timeseries import ${used.join(', ')}`] : []),
    '',
    'page = ll.Page()',
    ...lines,
    '',
    ...call,
    '',
    ...(spec.series.length > 0 ? [`for s in ${py(spec.series.map((x) => x.id))}:`, '    print(s, tile.query(s)[\'pure\'])  # each formula as the Pure the tile writes for it'] : []),
    'll.show(page)  # the page, with its tile, in DataCube',
    '',
  ].join('\n');
}

// ------------------------------------------------------------------ TypeScript

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

/** A value as TypeScript writes it: an object, an array, a string, a number, true, false, null. */
function ts(v: unknown): string {
  if (v === null) return 'null';
  if (typeof v === 'boolean') return String(v);
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) throw new Error(`${v} is no number a page holds`);
    return String(v);
  }
  if (typeof v === 'string') return quoted(v);
  if (Array.isArray(v)) return `[${v.map(ts).join(', ')}]`;
  if (typeof v === 'object') {
    const fields = Object.entries(v as Record<string, unknown>).filter(([, x]) => x !== undefined).map(([k, x]) => `${IDENTIFIER.test(k) ? k : ts(k)}: ${ts(x)}`);
    return fields.length === 0 ? '{}' : `{ ${fields.join(', ')} }`;
  }
  throw new Error(`a ${typeof v} is no value a page holds`);
}

/** A series as its spec holds it: its text, which core reads -- the builder's term's (B4), else its #TS formula. */
function tsSeries(s: TsSeries, term: string | undefined): string {
  const rest = SERIES_KEYS.filter((k) => s[k] !== undefined).map((k) => `, ${k}: ${ts(s[k])}`).join('');
  return `{ id: ${ts(s.id)}, text: ${term !== undefined ? `${term}.text()` : ts(s.text)}${rest} }`;
}

function typescriptCode(spec: TimeseriesSpec, of: TsCodeOf): string {
  const code = builderCode(spec, 'typescript');
  const used = namesUsed(code);
  const slots = SLOTS.filter((k) => spec[k] !== undefined).map((k) => `  ${k}: ${ts(spec[k])},`);
  const key = (alias: string): string => (IDENTIFIER.test(alias) ? alias : ts(alias));
  const at = (alias: string): string => (IDENTIFIER.test(alias) ? `cubes.${alias}` : `cubes[${ts(alias)}]`);
  const cubes = spec.sources.map((s) => {
    const g = of.grids[s.cube];
    const reads = columnsRead(spec, s.alias);
    return `  ${key(s.alias)}: ${ts(s.cube)}, // ${oneLine(`${g !== undefined ? `the grid "${g.title}"` : 'a grid'}${reads.length > 0 ? `: with ${listed(reads)}` : ''}`)}`;
  });
  const source = (s: TsSource): string => {
    const rest = { date: s.date, ...(s.key !== undefined ? { key: s.key } : {}), ...(s.kept !== undefined ? { kept: s.kept } : {}) };
    return `{ alias: ${ts(s.alias)}, cube: ${at(s.alias)}, ${ts(rest).slice(2, -2)} }`;
  };
  return [
    `// ${oneLine(of.title ?? 'A timeseries tile')}, built in TypeScript on a page of your own: its spec over your grids, and the page`,
    '// command the screen\'s Create ▸ Timeseries sends (Python\'s timeseries too) -- every change to a page is a command, applied by',
    '// one function (page-commands.ts). Each series is gs-quant\'s call of it (timeseries-builder.ts), or its #TS formula where',
    '// gs-quant has none -- its text, which the page reads by legend-lite\'s own reader, for the Pure query the tile writes for it',
    '// (timeseries-query.ts askQuery); the paths are from the repository\'s root.',
    'import { applyPageCommands, idsOf, type PageCommand } from \'./datacube/src/page-commands.ts\';',
    ...(used.length > 0 ? [`import { ${used.join(', ')} } from ` + '\'./datacube/src/timeseries-builder.ts\';'] : []),
    'import type { PageDocument } from \'./datacube/src/page-document.ts\';',
    'import { tileAsks } from \'./datacube/src/timeseries-query.ts\';',
    'import type { TimeseriesSpec } from \'./datacube/src/timeseries-spec.ts\';',
    '',
    '/** The grids it reads, each by the cube it is on your page: yours to set. */',
    'export const cubes = {',
    ...cubes,
    '};',
    '',
    'export const spec: TimeseriesSpec = {',
    `  sources: [${spec.sources.map(source).join(', ')}],`,
    '  series: [',
    ...spec.series.map((s, i) => `    ${tsSeries(s, code[i])},`),
    '  ],',
    ...slots,
    '};',
    '',
    '/** `page` with the tile added on `sheet`, a band of its own at the bottom: the screen\'s own command, applied. */',
    'export function withTile(page: PageDocument, sheet: string): PageDocument {',
    `  const add: PageCommand = { kind: 'addTimeseries', sheet, near: null, spec, title: ${ts(of.title)}, columns: null };`,
    '  return applyPageCommands({ page, held: idsOf(page) }, [add]).draft.page;',
    '}',
    '',
    '/** Every question it asks the database, as its Query window lists them. */',
    'export const questions: string[] = tileAsks(spec).map((a) => a.what);',
    '',
  ].join('\n');
}
