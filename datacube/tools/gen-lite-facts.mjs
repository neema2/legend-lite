// Generate the facts legend-lite owns into TypeScript, rather than
// re-authoring them here.
//
// THE PROBLEM THIS SOLVES. A cube has to know two things lite already
// decides: how a pivot column's name is put together, and what Pure
// type a SQL column type is. Both were re-typed by hand in src/, and
// both had already drifted:
//
//   * `PIVOT_SEPARATOR = '__|__'` was a literal copy of
//     Type.java's PIVOT_SEPARATOR. Every test used OUR copy on both
//     sides, so a change in lite could not fail anything here.
//   * `pureTypeOf` answered Float for DECIMAL where lite answers
//     Decimal -- the SAME divergence lite's own audit (2026-09-15
//     P3-5) had just found and fixed in one of its readers.
//
// So these are read OUT of lite's source instead. Drift becomes a
// build failure (`npm run verify:lite-facts`) rather than a wrong
// number in a grid.
//
// WHY PARSE JAVA RATHER THAN CALL IT. Calling would mean a running
// planner, which makes schema inference async and drags a 4 MB wasm
// module into a pure function that tests use without one. These are
// compile-time constants in lite; reading them at build time keeps
// them compile-time constants here.
//
// Usage:
//   node tools/gen-lite-facts.mjs            # write the file
//   node tools/gen-lite-facts.mjs --check    # fail if it would change

import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const CORE = resolve(HERE, '../../core/src/main/java/com/legend');
const OUT = resolve(HERE, '../src/generated/lite-facts.ts');

/** Read a file, saying which fact went missing rather than throwing ENOENT. */
function source(rel, fact) {
  try {
    return readFileSync(join(CORE, rel), 'utf8');
  } catch {
    throw new Error(`cannot read ${rel} — ${fact} comes from there. `
      + `Is this a full legend-lite checkout?`);
  }
}

// ---- 1. the pivot separator ----------------------------------------
function pivotSeparator() {
  const text = source('compiler/element/type/Type.java',
    'the pivot separator');
  const m = /PIVOT_SEPARATOR\s*=\s*"((?:[^"\\]|\\.)*)"/.exec(text);
  if (!m) {
    throw new Error('Type.java no longer declares PIVOT_SEPARATOR as a '
      + 'string literal — the generator needs updating, which is the '
      + 'point of it failing here.');
  }
  // A Java string literal, not a JS one: unescape what Java escapes.
  return m[1].replace(/\\(.)/g, (_, c) =>
    ({ n: '\n', t: '\t', r: '\r', '\\': '\\', '"': '"' })[c] ?? c);
}

// ---- 2. SQL type name -> RelationalDataType record ------------------
function sqlNameToRecord() {
  const text = source('model/RelationalDataType.java',
    'the SQL type-name table');
  const body = between(text, 'RelationalDataType fromName(String name)',
    'RelationalDataType.fromName');
  const out = {};
  // `case "A", "B" -> new Rec();` — arms that throw are not mappings.
  const arm = /case\s+((?:"[^"]+"\s*,?\s*)+)->\s*new\s+(\w+)\s*\(/g;
  for (let m; (m = arm.exec(body));) {
    const record = m[2];
    for (const name of m[1].match(/"([^"]+)"/g) ?? []) {
      out[name.slice(1, -1)] = record;
    }
  }
  if (Object.keys(out).length < 10) {
    throw new Error(`only ${Object.keys(out).length} arms parsed out of `
      + `fromName — the shape changed and the generator must be updated`);
  }
  return out;
}

// ---- 2b. the Database grammar's own keyword table -------------------
//
// The grammar is what our generated Database declaration has to satisfy,
// so this table is both a name->record mapping (covering the sized types
// `fromName` refuses) and the set of spellings we are allowed to emit.
function grammarKeywordToRecord(records) {
  const text = source('parser/DatabaseProtocolParser.java',
    "the Database grammar's type keywords");
  const body = between(text, 'String kind = switch (kindWord)',
    "DatabaseProtocolParser's type keyword table");
  const out = {};
  // `case "INTEGER", "INT" -> "Integer";`
  const arm = /case\s+((?:"[^"]+"\s*,?\s*)+)->\s*"(\w+)"/g;
  for (let m; (m = arm.exec(body));) {
    const record = resolveRecord(m[2], records);
    for (const name of m[1].match(/"([^"]+)"/g) ?? []) {
      out[name.slice(1, -1)] = record;
    }
  }
  if (Object.keys(out).length < 15) {
    throw new Error(`only ${Object.keys(out).length} keyword arms parsed `
      + `— the grammar's shape changed and the generator must be updated`);
  }
  return out;
}

/**
 * A grammar kind WORD to the record that implements it.
 *
 * Lite spells records that would clash with a java.lang type or a Java
 * keyword with a trailing underscore (Integer_, Double_, Date_, Char_,
 * Object_) -- documented in RelationalDataType's javadoc. The grammar
 * uses the bare word, so the convention is applied here rather than
 * guessed at: a word that resolves to no record is an error.
 */
function resolveRecord(word, records) {
  if (records[word] !== undefined) return word;
  if (records[`${word}_`] !== undefined) return `${word}_`;
  // The grammar spells the semi-structured carrier both ways; lite's
  // own fromName says JSON and SEMISTRUCTURED are one record.
  if (word === 'Json') return 'SemiStructured';
  throw new Error(`the grammar produces kind '${word}', which is no `
    + `RelationalDataType record — lite's grammar and its type `
    + `hierarchy have diverged`);
}

// ---- 3. RelationalDataType record -> Pure kind ----------------------
function recordToPureKind() {
  const text = source('compiler/RelationalKinds.java', 'the Pure kinds');
  const body = between(text, 'String pureKindOf(RelationalDataType',
    'RelationalKinds.pureKindOf');
  const out = {};
  // `case RelationalDataType.Varchar v -> "String";`
  const arm = /case\s+RelationalDataType\.(\w+)\s+\w+\s*->\s*"([^"]+)"/g;
  for (let m; (m = arm.exec(body));) out[m[1]] = m[2];
  if (Object.keys(out).length < 15) {
    throw new Error(`only ${Object.keys(out).length} arms parsed out of `
      + `pureKindOf — the shape changed and the generator must be updated`);
  }
  return out;
}

/** The brace-balanced body of the method whose signature contains `needle`. */
function between(text, needle, what) {
  const at = text.indexOf(needle);
  if (at < 0) throw new Error(`${what} is no longer declared as expected`);
  const open = text.indexOf('{', at);
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    if (text[i] === '{') depth += 1;
    else if (text[i] === '}') {
      depth -= 1;
      if (depth === 0) return text.slice(open + 1, i);
    }
  }
  throw new Error(`${what}'s body does not close`);
}

// ---- assemble -------------------------------------------------------
const separator = pivotSeparator();
const toKind = recordToPureKind();
const fromNameTable = sqlNameToRecord();
const grammarTable = grammarKeywordToRecord(toKind);
// `fromName` wins where both speak: it is the reader lite documents as
// canonical, and it resolves JSON to the semi-structured carrier where
// the grammar keeps a separate word for it.
const toRecord = { ...grammarTable, ...fromNameTable };

// The composition is what a caller actually wants: a SQL type name to
// the Pure type. Done here so no caller has to know there are two
// tables in lite, and so a name that maps to a record with no kind is
// caught NOW rather than at a lookup.
const composed = {};
for (const [name, record] of Object.entries(toRecord)) {
  const kind = toKind[record];
  if (kind === undefined) {
    throw new Error(`fromName maps ${name} to ${record}, which `
      + `pureKindOf does not answer — lite's two tables disagree, `
      + `which is exactly what this generator exists to surface`);
  }
  composed[name] = kind;
}

const entries = (o) => Object.keys(o).sort()
  .map((k) => `  ${JSON.stringify(k)}: ${JSON.stringify(o[k])},`)
  .join('\n');

const file = `// GENERATED by tools/gen-lite-facts.mjs — DO NOT EDIT.
//
// These are legend-lite's facts, read out of its source so that this
// cube cannot hold a second opinion about them. Regenerate with
// \`npm run gen:lite-facts\`; \`npm run verify:lite-facts\` fails the
// build if the checked-in copy has drifted from lite.
//
// Sources:
//   compiler/element/type/Type.java          PIVOT_SEPARATOR
//   model/RelationalDataType.java            fromName
//   compiler/RelationalKinds.java            pureKindOf

/**
 * How a pivot's generated column name joins its parts.
 *
 * Both planners and legend-engine spell it this way; DuckDB's native
 * PIVOT is normalised to it by lite's own dialect (DuckDb.java names
 * the aggregate alias so DuckDB's '_' joiner lands here).
 */
export const PIVOT_SEPARATOR = ${JSON.stringify(separator)};

/** A SQL type name, as lite's grammar spells it, to its canonical record. */
export const SQL_NAME_TO_RECORD: Readonly<Record<string, string>> = {
${entries(toRecord)}
};

/**
 * Every type keyword lite's Database grammar accepts.
 *
 * A generated Database declaration that names anything else does not
 * compile, so this is the vocabulary \`sqlTypeOf\` must emit into.
 */
export const GRAMMAR_TYPE_KEYWORDS: readonly string[] = [
${Object.keys(grammarTable).sort().map((k) => `  ${JSON.stringify(k)},`).join('\n')}
];

/** A canonical record to the Pure type lite reads it as. */
export const RECORD_TO_PURE_KIND: Readonly<Record<string, string>> = {
${entries(toKind)}
};

/**
 * A SQL type name to its Pure type — the composition of the two tables
 * above, which is what callers want.
 *
 * Note DECIMAL answers 'Decimal', not 'Float'. A hand-written copy of
 * this table here said Float, the same divergence lite's 2026-09-15
 * audit found in one of its own readers.
 */
export const PURE_KIND_BY_SQL_NAME: Readonly<Record<string, string>> = {
${entries(composed)}
};
`;

if (process.argv.includes('--check')) {
  let current = '';
  try {
    current = readFileSync(OUT, 'utf8');
  } catch {
    console.error('src/generated/lite-facts.ts is missing — run '
      + '`npm run gen:lite-facts`');
    process.exit(1);
  }
  if (current !== file) {
    console.error('*** src/generated/lite-facts.ts has DRIFTED from '
      + 'legend-lite ***');
    console.error('legend-lite changed a fact this cube mirrors. Run '
      + '`npm run gen:lite-facts`, then read the diff: a changed Pure '
      + 'kind or pivot separator changes what the grid shows.');
    process.exit(1);
  }
  console.log('lite facts: in step with legend-lite');
  console.log(`  separator ${JSON.stringify(separator)}, `
    + `${Object.keys(composed).length} SQL type names, `
    + `${Object.keys(toKind).length} records`);
} else {
  writeFileSync(OUT, file);
  console.log(`wrote src/generated/lite-facts.ts — `
    + `separator ${JSON.stringify(separator)}, `
    + `${Object.keys(composed).length} SQL type names, `
    + `${Object.keys(toKind).length} records`);
}
