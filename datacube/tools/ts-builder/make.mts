// THE #TS BUILDER'S WORDS, GENERATED (docs/TS_DECISIONS_2026_11_07.md §4, the builders; the order's step 2): each word a
// tile draws -- but SQL's per point, the builder's own (timeseries-builder-core.ts) -- as a typed function of gs-quant
// 2.2.3's name, its parameters gs-quant's names, order, defaults and types, read from gs-quant itself
// (projects/timeseries/gs-quant-signatures.json, tools/ts-oracle/signatures.py) and joined to the library's by core's
// catalog (generated/ts-signatures.gen.ts, generated/ts-words.gen.ts: the library's gsParam and gsValue tags, where
// gs-quant names a parameter or spells an option otherwise). A word gs-quant has not keeps the library's name and
// parameters. The tile's words are the tile's (src/timeseries-words.ts: its list and its whole-line rule).
//
// Python's twin (B3) from the same shapes: each word a function of gs-quant's name and Python signature, its defaults
// shown as gs-quant writes them, its call told to DataCube's builder by name (python/legend_lite/_ts_words.py).
//
// Usage: make.mts <out.ts> <ts-signatures.gen.ts> <ts-words.gen.ts> <gs-quant-signatures.json> [<out.py>]
import { readFileSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { SCALARS, tileSignatures, TS_FUNCTION_NAMES, type TsParamShape, type TsSignature } from '../../src/timeseries-words.ts';

const [out, sigPath, wordsPath, gsPath, pyOut] = process.argv.slice(2);
if (out === undefined || sigPath === undefined || wordsPath === undefined || gsPath === undefined) {
  throw new Error('usage: make.mts <out.ts> <ts-signatures.gen.ts> <ts-words.gen.ts> <gs-quant-signatures.json>');
}
const load = async <T,>(p: string): Promise<T> => (await import(pathToFileURL(resolve(p)).href)) as T;
const { WORD_SIGNATURES } = await load<{ WORD_SIGNATURES: Readonly<Record<string, TsSignature>> }>(sigPath);
interface CatalogParam { readonly name: string; readonly kind: string }
interface CatalogWord { readonly params: readonly CatalogParam[]; readonly facts: Readonly<Record<string, readonly string[]>> }
const { TS_WORDS } = await load<{ TS_WORDS: Readonly<Record<string, CatalogWord>> }>(wordsPath);
type GsDefault = null | { value: unknown } | { window: [unknown, unknown] } | { enum: string; member: string } | { repr: string };
interface GsParam { readonly name: string; readonly kind: string; readonly default: GsDefault; readonly annotation: string | null }
interface Gs { readonly words: Readonly<Record<string, readonly { function: string; params: readonly GsParam[] }[]>>; readonly enums: Readonly<Record<string, Readonly<Record<string, string>>>> }
const GS = JSON.parse(readFileSync(gsPath, 'utf8')) as Gs;

const SIGS = tileSignatures(WORD_SIGNATURES);
const LINE_KINDS = new Set(['line', 'line_or_number', 'members']);
/** TypeScript's reserved words, which no function is named. */
const RESERVED = new Set(['break', 'case', 'catch', 'class', 'const', 'continue', 'debugger', 'default', 'delete', 'do', 'else',
  'enum', 'export', 'extends', 'false', 'finally', 'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'null',
  'return', 'super', 'switch', 'this', 'throw', 'true', 'try', 'typeof', 'var', 'void', 'while', 'with', 'yield', 'let',
  'static', 'implements', 'interface', 'package', 'private', 'protected', 'public', 'await']);
/** An enumeration of so many members is a type no builder parameter takes as its own (gs-quant's Currency). */
const SMALL_ENUM = 30;

const q = (s: string): string => `'${s.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
const lit = (v: unknown): string => (typeof v === 'string' ? q(v) : v === null || v === undefined ? 'null' : String(v));
const doc = (s: string): string => s.replace(/\*\//g, '* /');

// -- Python's twin --
/** Python's keywords, which no function or parameter is named (one takes a trailing underscore, as gs-quant's if_). */
const PY_KEYWORDS = new Set(['False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await', 'break', 'class', 'continue', 'def',
  'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or',
  'pass', 'raise', 'return', 'try', 'while', 'with', 'yield']);
const pyName = (n: string): string => (PY_KEYWORDS.has(n) ? `${n}_` : n);
const pyLit = (v: unknown): string => (v === null || v === undefined ? 'None' : typeof v === 'boolean' ? (v ? 'True' : 'False')
  : typeof v === 'string' ? q(v) : String(v));
const pyEnums = new Set<string>();
const pyIdent = (k: string): boolean => /^[A-Za-z_][A-Za-z0-9_]*$/.test(k) && !PY_KEYWORDS.has(k);
/** gs-quant's default as Python writes it (`Window(None, 0)`, `Returns.SIMPLE`, `None`). */
function pyDefault(d: GsDefault): string {
  if (d === null) return 'None';
  if ('window' in d) return `Window(${pyLit(d.window[0])}, ${pyLit(d.window[1])})`;
  if ('enum' in d) {
    // an enumeration the builder carries none of (an `only` parameter's): its default shown as gs-quant writes it
    if (GS.enums[d.enum] === undefined) return `${d.enum}.${d.member}`;
    pyEnums.add(d.enum);
    return pyIdent(d.member) ? `${d.enum}.${d.member}` : `${d.enum}[${q(d.member)}]`;
  }
  if ('value' in d) return pyLit(d.value);
  throw new Error(`a default gs-quant writes as ${d.repr}`);
}
const pyDoc = (s: string): string => s.replace(/\\/g, '\\\\').replace(/"""/g, '\\"\\"\\"');
const pyDefs: string[] = [];
const pyAll: string[] = [];

/** A tag's values of a word, `name: rest`, by name. */
function tagged(w: CatalogWord, tag: string): Map<string, string> {
  const out = new Map<string, string>();
  for (const v of w.facts[tag] ?? []) out.set(v.slice(0, v.indexOf(': ')), v.slice(v.indexOf(': ') + 2));
  return out;
}

const enumsUsed = new Set<string>();
const shapes: string[] = [];
const names = new Set<string>();
const lines: string[] = [
  '// GENERATED by tools/ts-builder/make.mts from core\'s catalog (generated/ts-signatures.ts, ts-words.ts) and gs-quant\'s',
  '// own signatures (projects/timeseries/gs-quant-signatures.json) -- DO NOT EDIT. Regenerate: bazel run',
  '// //datacube:update_generated; its diff test fails the build if this copy drifts.',
  '//',
  '// THE #TS BUILDER\'S WORDS (docs/TS_DECISIONS_2026_11_07.md §4, the builders): each a function of gs-quant 2.2.3\'s name,',
  '// its parameters gs-quant\'s names, order, defaults and types -- its keyword arguments a trailing object -- each made',
  '// the library\'s by timeseries-builder-core.ts word(); a word gs-quant has not, the library\'s name and parameters.',
  '',
  'import { word, type Args, type Line, type Term, type WindowLike, type WordShape } from \'../timeseries-builder-core.ts\';',
  '',
];
const body: string[] = [];

/** A parameter's TypeScript type, from gs-quant's annotation where it has one, else the library's kind. */
function typeOf(annotation: string | null, p: TsParamShape | undefined, optional: boolean): string {
  let t: string;
  const a = annotation ?? '';
  const named = Object.keys(GS.enums).find((e) => new RegExp(`\\b${e}\\b`).test(a) && Object.keys(GS.enums[e]!).length <= SMALL_ENUM);
  if (/\bWindow\b/.test(a)) t = 'WindowLike';
  else if (named !== undefined) {
    enumsUsed.add(named);
    t = named;
  } else if (/\bbool\b/.test(a)) t = 'boolean';
  else if (/\b(int|float|Real)\b/.test(a)) t = 'number';
  else if (p !== undefined) t = p.kind === 'span' ? 'WindowLike' : p.kind === 'word' ? (p.words ?? []).map(q).join(' | ') : 'number';
  else t = 'unknown';
  return optional ? `${t} | null` : t;
}

/** gs-quant's default as the builder holds it: a value, a window, an enumeration member's value; none a required one. */
function defaultOf(d: GsDefault): { readonly has: boolean; readonly text: string; readonly value: unknown } {
  if (d === null) return { has: false, text: '', value: undefined };
  if ('window' in d) return { has: true, text: `{ window: [${lit(d.window[0])}, ${lit(d.window[1])}] }`, value: d };
  if ('enum' in d) {
    const v = GS.enums[d.enum]?.[d.member] ?? d.member;
    return { has: true, text: lit(v), value: v };
  }
  if ('value' in d) return { has: true, text: lit(d.value), value: d.value };
  throw new Error(`a default gs-quant writes as ${d.repr}`);
}

/** An `only` parameter's type: gs-quant's default alone. */
function onlyType(d: GsDefault): string {
  if (d !== null && 'enum' in d && GS.enums[d.enum] !== undefined && Object.keys(GS.enums[d.enum]!).length <= SMALL_ENUM) {
    enumsUsed.add(d.enum);
    return `typeof ${d.enum}.${d.member}`;
  }
  const { text } = defaultOf(d);
  return text.startsWith('{') ? 'WindowLike' : text;
}

for (const name of TS_FUNCTION_NAMES) {
  if (SCALARS[name] !== undefined) continue;
  const sig = SIGS[name];
  const cat = TS_WORDS[name];
  if (cat === undefined) throw new Error(`${name} is no word of the catalog`);
  const label = cat.facts['label']?.[0] ?? name;
  const about = cat.facts['about']?.[0] ?? '';
  const catLines = cat.params.filter((p) => LINE_KINDS.has(p.kind));
  const members = catLines.some((p) => p.kind === 'members');
  const gsFn = GS.words[name]?.[0];
  // a name TypeScript reserves (gs-quant's var) takes gs-quant's own way with Python's: a trailing underscore (sum_, if_)
  const spelled = gsFn ? gsFn.function.slice(gsFn.function.lastIndexOf('.') + 1) : name;
  const fnName = RESERVED.has(spelled) ? `${spelled}_` : spelled;
  if (names.has(fnName)) throw new Error(`two words are the builder's ${fnName}`);
  names.add(fnName);
  const shapeName = `${name.toUpperCase()}_SHAPE`;
  const gsEntries: string[] = [];
  // Python's: its parameters by place (gs-quant's default shown, none where required) and those by name alone
  const pyPos: { name: string; def?: string }[] = [];
  const pyKw: { name: string; def: string }[] = [];
  const positional: { name: string; type: string; required: boolean }[] = [];
  const kwOnly: { name: string; type: string }[] = [];
  const lineParams: { name: string; type: string }[] = [];
  let listOf = 0;
  if (gsFn) {
    const gsLib = new Map([...tagged(cat, 'gsParam')].map(([lib, gs]) => [gs, lib]));
    const values = tagged(cat, 'gsValue');
    const params = gsFn.params.filter((p) => p.kind !== 'VAR_POSITIONAL' && p.kind !== 'VAR_KEYWORD');
    // its lines: gs-quant's first parameters, one each -- or one list of them (union's, append's), or a basket's list
    const first = params[0];
    const takesList = first !== undefined && /\blist\b/.test(first.annotation ?? '') && !members && catLines.length > 1;
    const nLines = members || takesList ? 1 : catLines.length;
    if (takesList) listOf = catLines.length;
    params.slice(0, nLines).forEach((p, i) => {
      if (/\b(int|float|bool|Window)\b/.test(p.annotation ?? '') && !/\b(Series|Asset)\b/.test(p.annotation ?? '')) {
        throw new Error(`${name}: gs-quant's ${p.name} (${p.annotation}) is no line, where the library reads ${catLines.length}`);
      }
      const kind = catLines[i]?.kind;
      lineParams.push({ name: p.name, type: members || takesList ? 'readonly Line[]' : kind === 'line_or_number' ? 'Line | number' : 'Line' });
    });
    const mapped = new Set<string>();
    for (const p of params.slice(nLines)) {
      const libName = gsLib.get(p.name) ?? p.name;
      const lib = sig.params.find((x) => x.name === libName);
      // gs-quant's default of every observation, where the library's is a whole number of them (smoothed_moving_average's
      // whole-series seed), is no value the language takes: the parameter is told (the test's ledger)
      // the library's own default in its place (the user, 2026-11-08: "Should we not default moving to 14?")
      const unsaid = lib?.kind === 'whole' && p.default !== null && 'window' in p.default && p.default.window[0] === null;
      if (unsaid && lib.default === undefined) throw new Error(`${name}'s ${p.name}: gs-quant's default the language has no way to say, and the library none`);
      // a gs-quant default the language does not say, the parameter told in every call (the library's gsTold; none
      // since 2026-11-08, when vol_swap_volatility's default became gs-quant's one figure at the line's last date)
      const toldAlways = tagged(cat, 'gsTold').has(p.name);
      const d = toldAlways ? { has: false, text: '', value: undefined }
        : unsaid ? { has: true, text: lit(lib.default), value: lib.default } : defaultOf(p.default);
      const isWeights = members && p.name === 'weights';
      const entry: string[] = [`name: ${q(p.name)}`];
      let type: string;
      if (isWeights) {
        entry.push('weights: true');
        if (d.has) entry.push(`default: ${d.text}`);
        type = d.has ? 'readonly number[] | null' : 'readonly number[]';
      } else if (lib !== undefined) {
        mapped.add(libName);
        if (libName !== p.name) entry.push(`lib: ${q(libName)}`);
        if (d.has) entry.push(`default: ${d.text}`);
        // a window gs-quant takes as the whole line (the library's gsWhole: zscores', percentile's default; winsorize's)
        const whole = tagged(cat, 'gsWhole').get(libName);
        if (whole !== undefined) entry.push(`whole: ${q(whole)}`);
        // the enumeration its values are members of (gs-quant's Returns): what a printer writes a value as
        const named = Object.keys(GS.enums).find((e) => new RegExp(`\\b${e}\\b`).test(p.annotation ?? '') && Object.keys(GS.enums[e]!).length <= SMALL_ENUM);
        if (named !== undefined) entry.push(`enum: ${q(named)}`);
        const v = values.get(libName);
        if (v !== undefined) {
          const [gsValue, libWord] = v.split(' ');
          entry.push(`values: { ${gsValue}: ${q(libWord!)} }`);
        }
        type = typeOf(p.annotation, lib, d.has && d.value === null);
      } else {
        entry.push('only: true');
        entry.push(`default: ${d.has ? d.text : 'null'}`);
        type = onlyType(p.default);
        // gs-quant's risk-free cash (item 13b, the builder's takesCash): its currency the page's cash line, its curve
        // type whether the line is a price -- each any member of its enumeration, not the default alone
        const cashed = params.some((x) => x.name === 'currency') && params.some((x) => x.name === 'curve_type');
        if (cashed && (p.name === 'currency' || p.name === 'curve_type') && p.default !== null && 'enum' in p.default) {
          enumsUsed.add(p.default.enum);
          type = p.default.enum;
        }
        // a value beside gs-quant's default the word computes (the library's gsAlso: lag's truncate)
        const also = tagged(cat, 'gsAlso').get(p.name);
        if (also !== undefined) {
          const values = also.split(' ');
          entry.push(`also: [${values.map(q).join(', ')}]`);
          const e = Object.keys(GS.enums).find((k) => new RegExp(`\\b${k}\\b`).test(p.annotation ?? ''));
          if (e === undefined) throw new Error(`${name}'s ${p.name}: gsAlso of no enumeration`);
          enumsUsed.add(e);
          type = `${type} | ${values.map((v) => `typeof ${e}.${Object.entries(GS.enums[e]!).find(([, x]) => x === v)?.[0] ?? (() => { throw new Error(`${e} has no ${v}`); })()}`).join(' | ')}`;
        }
      }
      const pyDef = unsaid ? pyLit(lib!.default) : isWeights || lib !== undefined ? (d.has ? pyDefault(p.default) : undefined)
        : (d.has ? pyDefault(p.default) : 'None');
      if (p.kind === 'KEYWORD_ONLY') {
        entry.push('kwOnly: true');
        kwOnly.push({ name: p.name, type });
        pyKw.push({ name: p.name, def: pyDef ?? 'None' });
      } else {
        positional.push({ name: p.name, type, required: !d.has });
        pyPos.push({ name: p.name, ...(pyDef !== undefined ? { def: pyDef } : {}) });
      }
      gsEntries.push(`{ ${entry.join(', ')} }`);
    }
    const extra = sig.params.filter((p) => !mapped.has(p.name)).map((p) => p.name);
    for (const e of extra) {
      kwOnly.push({ name: e, type: typeOf(null, sig.params.find((p) => p.name === e), true) });
      pyKw.push({ name: e, def: 'None' });
    }
    body.push(`const ${shapeName}: WordShape = { word: ${q(name)}, gsFunction: ${q(gsFn.function)}, lines: [${lineParams.map((p) => q(p.name)).join(', ')}], gs: [${gsEntries.join(', ')}], extra: [${extra.map(q).join(', ')}]${members ? `, members: { evenWeights: ${String(params.find((p) => p.name === 'weights')?.default !== null)} }` : ''}${listOf ? `, listOf: ${listOf}` : ''} };`);
  } else {
    // DataCube's own: the library's lines and parameters, its defaults
    const reads = sig.reads ?? catLines.map((p) => p.name);
    catLines.forEach((p, i) => lineParams.push({ name: reads[i] ?? p.name, type: members ? 'readonly Line[]' : p.kind === 'line_or_number' ? 'Line | number' : 'Line' }));
    for (const p of sig.params) {
      const has = p.default !== undefined;
      // an optional one without a default is the language's own where left out (null): by name, never by place (item 13's r)
      const entry = [`name: ${q(p.name)}`, ...(has ? [`default: ${lit(p.default)}`] : p.required ? [] : ['default: null'])];
      gsEntries.push(`{ ${entry.join(', ')} }`);
      positional.push({ name: p.name, type: typeOf(null, p, !has && !p.required), required: p.required && !has });
      pyPos.push({ name: p.name, ...(has ? { def: pyLit(p.default) } : p.required ? {} : { def: 'None' }) });
    }
    body.push(`const ${shapeName}: WordShape = { word: ${q(name)}, gsFunction: null, lines: [${lineParams.map((p) => q(p.name)).join(', ')}], gs: [${gsEntries.join(', ')}], extra: [] };`);
  }
  // the required ones by place before the rest; the rest by place or by name
  const required = positional.filter((p) => p.required);
  const optional = positional.filter((p) => !p.required);
  if (positional.some((p, i) => p.required && positional.slice(0, i).some((x) => !x.required))) {
    throw new Error(`${name}: gs-quant's required ${positional.find((p) => p.required)!.name} after an optional one`);
  }
  const kw = [...optional, ...kwOnly].map((p) => `${p.name}?: ${p.type}`).join('; ');
  const tuple = optional.map((p) => `${p.name}: ${p.type}`).join(', ');
  // a word of no parameter but its required ones takes nothing more
  const more = kw.length > 0;
  const params = [
    ...lineParams.map((p) => `${p.name}: ${p.type}`),
    ...required.map((p) => `${p.name}: ${p.type}`),
    ...(more ? [`...args: Args<[${tuple}], { ${kw} }>`] : []),
  ];
  const callLines = listOf ? `${lineParams[0]!.name}` : `[${lineParams.map((p) => p.name).join(', ')}]`;
  const callArgs = required.length === 0 ? (more ? 'args' : '[]') : `[${[...required.map((p) => p.name), ...(more ? ['...args'] : [])].join(', ')}]`;
  const said = gsFn ? `gs-quant's ${gsFn.function}` : 'DataCube\'s own: gs-quant has none';
  body.push(`/** ${doc(label)}: ${doc(about)} (${said}.) */`);
  body.push(`export function ${fnName}(${params.join(', ')}): Term {`);
  body.push(`  return word(${shapeName}, ${callLines}, ${callArgs});`);
  body.push('}');
  body.push('');
  shapes.push(`  ${fnName}: ${shapeName},`);
  // Python's: gs-quant's name and signature, its call told to the builder by the builder's own name
  const py = pyName(gsFn ? spelled : name);
  const pyParams = [
    ...lineParams.map((p) => pyName(p.name)),
    ...pyPos.map((p) => (p.def === undefined ? pyName(p.name) : `${pyName(p.name)}=_Default(${q(p.def)})`)),
    ...(pyKw.length > 0 ? ['*', ...pyKw.map((p) => `${pyName(p.name)}=_Default(${q(p.def)})`)] : []),
  ];
  const told = [...pyPos, ...pyKw].map((p) => `${q(p.name)}: ${pyName(p.name)}`).join(', ');
  pyDefs.push(`def ${py}(${pyParams.join(', ')}) -> Term:`);
  pyDefs.push(`    """${pyDoc(label)}: ${pyDoc(about)} (${pyDoc(said)}.)"""`);
  pyDefs.push(`    return _word(${q(fnName)}, [${lineParams.map((p) => pyName(p.name)).join(', ')}], **{${told}})`);
  pyDefs.push('', '');
  pyAll.push(py);
}
body.push('/** Each builder function\'s shape, by its name: what the fidelity test holds to gs-quant\'s own signatures. */');
body.push('export const BUILDER_WORDS: Readonly<Record<string, WordShape>> = {', ...shapes, '};', '');

for (const e of [...enumsUsed].sort()) {
  const members = GS.enums[e]!;
  lines.push(`/** gs-quant's ${e} (gs-quant 2.2.3's, its members and values). */`);
  lines.push(`export const ${e} = { ${Object.entries(members).map(([k, v]) => `${/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(k) ? k : q(k)}: ${q(v)}`).join(', ')} } as const;`);
  lines.push(`export type ${e} = (typeof ${e})[keyof typeof ${e}];`);
  lines.push('');
}
writeFileSync(out, [...lines, ...body].join('\n'));

if (pyOut !== undefined) {
  const enums = [...new Set([...enumsUsed, ...pyEnums])].sort();
  const py: string[] = [
    '# GENERATED by datacube/tools/ts-builder/make.mts from core\'s catalog and gs-quant\'s own signatures',
    '# (projects/timeseries/gs-quant-signatures.json) -- DO NOT EDIT. Regenerate: bazel run //python:update_generated; its',
    '# diff test fails the build if this copy drifts.',
    '# ruff: noqa: E501',
    '"""THE #TS BUILDER\'S WORDS FOR PYTHON (B3\'s Python twin, docs/TS_DECISIONS_2026_11_07.md §4, the builders): each word a',
    'function of gs-quant 2.2.3\'s name and signature, its defaults shown as gs-quant writes them -- a default left out is',
    'DataCube\'s to take -- its call made the tile\'s by DataCube\'s builder (datacube/src/timeseries-builder-wire.ts), the',
    'same functions TypeScript\'s builder calls. A word gs-quant has not keeps the library\'s name and parameters."""',
    '',
    'from __future__ import annotations',
    '',
    'from enum import Enum',
    '',
    'from ._ts_terms import Term, _Default, _word',
    '',
    '',
  ];
  for (const e of enums) {
    py.push(`#: gs-quant's ${e} (gs-quant 2.2.3's members and values).`);
    py.push(`${e} = Enum(${q(e)}, {${Object.entries(GS.enums[e]!).map(([k, v]) => `${q(k)}: ${q(v)}`).join(', ')}})`);
  }
  py.push('', '', ...pyDefs);
  py.push(`__all__ = [${[...enums, ...pyAll].map(q).join(', ')}]`, '');
  writeFileSync(pyOut, py.join('\n'));
}
