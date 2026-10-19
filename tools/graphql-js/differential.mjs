// The graphql-js differential (docs/GRAPHQL_STUDY_2026_10_18.md §9.3, G1; PR1b): documents over a generated
// schema, graphql-js's verdict on each, written as JSON lines for core's GqlValidityTest, which asserts that lite
// rejects everything graphql-js rejects (lite's acceptance a subset of graphql-js's, never more) and counts where
// lite is stricter (the rules of §6.3).
//
//   node differential.mjs --schema <sdl> --documents <json array> --out <jsonl> [--seed 1] [--mutations 6]
//
// The base documents are hand-written against the schema; each is written as is, then mutated by a seeded
// generator: a near-miss field name, Hasura's `_gt`, a bare value for a condition, an object field without a
// selection, an unknown argument, a fragment on the wrong type, a string for an Int, two keys in one sort
// object (G16: lite refuses, graphql-js accepts), a selection on a scalar, two fields under one key, an
// undefined variable, an unused fragment, a mutation operation. A hand tool: Node and graphql pinned in
// package.json; nothing of it enters core's build.
import { readFileSync, writeFileSync } from 'node:fs';
import { buildSchema, parse, validate } from 'graphql';

const arg = (name, def) => {
  const i = process.argv.indexOf(`--${name}`);
  return i < 0 ? def : process.argv[i + 1];
};
const schemaText = readFileSync(arg('schema'), 'utf8');
const documents = JSON.parse(readFileSync(arg('documents'), 'utf8'));
const out = arg('out');
let seed = Number(arg('seed', '1')) >>> 0;
const perDocument = Number(arg('mutations', '6'));
const schema = buildSchema(schemaText);

// a small seeded generator (mulberry32): the same seed, the same documents
const random = () => {
  seed = (seed + 0x6d2b79f5) >>> 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const pick = (items) => items[Math.floor(random() * items.length)];

/** Each mutation: a name, and a function from a document to a mutated document or null when it does not apply. */
const MUTATIONS = [
  ['nearMissField', (d) => {
    const m = [...d.matchAll(/\b(fullName|firstName|lastName|legalName|city|country|salary|budget|age|born|count)\b(?![:(])/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + hit[1] + 'x' + d.slice(hit.index + hit[1].length);
  }],
  ['hasuraOperator', (d) => {
    const m = [...d.matchAll(/\b(gt|gte|lt|lte|equals|in|contains):/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + '_' + d.slice(hit.index);
  }],
  ['bareValue', (d) => {
    const m = [...d.matchAll(/\b(age|salary|budget): \{ (gt|gte|lt|lte|equals): (\d+) \}/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + `${hit[1]}: ${hit[3]}` + d.slice(hit.index + hit[0].length);
  }],
  ['objectWithoutSelection', (d) => {
    const m = [...d.matchAll(/\b(firm|address|manager) \{ [^{}]* \}/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + hit[1] + d.slice(hit.index + hit[0].length);
  }],
  ['unknownArgument', (d) => {
    const m = [...d.matchAll(/\b(Person|Firm|Address|Employee|Manager)(\(| \{)/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    const tail = hit[2] === '(' ? '(foo: 1, ' : '(foo: 1) {';
    return d.slice(0, hit.index) + hit[1] + tail + d.slice(hit.index + hit[0].length);
  }],
  ['fragmentOnTheWrongType', (d) => {
    const m = [...d.matchAll(/\bPerson(\([^)]*\))? \{ /g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index + hit[0].length) + '... on Firm { legalName } ' + d.slice(hit.index + hit[0].length);
  }],
  ['stringForInt', (d) => {
    const m = [...d.matchAll(/\b(limit|offset): (\d+)/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + `${hit[1]}: "${hit[2]}"` + d.slice(hit.index + hit[0].length);
  }],
  ['twoKeysInOneSortObject', (d) => {
    const m = [...d.matchAll(/orderBy: \[\{ (\w+): (ASC|DESC) \}/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    const other = hit[1] === 'age' ? 'firstName' : 'age';
    return d.slice(0, hit.index) + `orderBy: [{ ${hit[1]}: ${hit[2]}, ${other}: ASC }` + d.slice(hit.index + hit[0].length);
  }],
  ['selectionOnAScalar', (d) => {
    const m = [...d.matchAll(/\b(fullName|legalName|city|age|salary)\b(?= \}| [a-z_]|\n)/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + hit[1] + ' { x }' + d.slice(hit.index + hit[1].length);
  }],
  ['twoFieldsUnderOneKey', (d) => {
    const m = [...d.matchAll(/\bfirm \{ legalName \}/g)];
    if (m.length === 0) return null;
    const hit = m[0];
    return d.slice(0, hit.index) + 'firm { legalName } firm: address { city }' + d.slice(hit.index + hit[0].length);
  }],
  ['undefinedVariable', (d) => {
    if (d.includes('$')) return null;
    const m = [...d.matchAll(/\b(Person|Firm)(\(| \{)/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    const tail = hit[2] === '(' ? '(where: $w, ' : '(where: $w) {';
    return d.slice(0, hit.index) + hit[1] + tail + d.slice(hit.index + hit[0].length);
  }],
  ['unusedFragment', (d) => (d.includes('fragment ') ? null : d + ' fragment unused on Firm { legalName }')],
  ['mutationOperation', (d) => (d.startsWith('{') ? 'mutation ' + d : null)],
  ['emptySelection', (d) => {
    const m = [...d.matchAll(/\{ (fullName|legalName|city) \}/g)];
    if (m.length === 0) return null;
    const hit = pick(m);
    return d.slice(0, hit.index) + '{ }' + d.slice(hit.index + hit[0].length);
  }],
  ['argumentTwice', (d) => {
    const m = [...d.matchAll(/\blimit: (\d+)/g)];
    if (m.length === 0) return null;
    const hit = m[0];
    return d.slice(0, hit.index) + `limit: ${hit[1]}, limit: ${hit[1]}` + d.slice(hit.index + hit[0].length);
  }],
  ['introspectionNearMiss', (d) => (d.includes('__schema') ? d.replace('queryType', 'queryTypes') : null)],
];

const lines = [];
let id = 0;
const verdict = (document) => {
  let doc;
  try {
    doc = parse(document);
  } catch (e) {
    return { valid: false, errors: [{ rule: 'SYNTAX', message: e.message.split('\n')[0] }] };
  }
  const errors = validate(schema, doc).map((e) => ({ message: e.message }));
  return { valid: errors.length === 0, errors };
};
const emit = (base, mutation, document) => {
  id += 1;
  lines.push(JSON.stringify({ id: `D${String(id).padStart(4, '0')}`, base, mutation, document, ...verdict(document) }));
};
documents.forEach((document, b) => {
  const base = `B${String(b + 1).padStart(2, '0')}`;
  emit(base, 'none', document);
  const seen = new Set([document]);
  let tries = 0;
  let made = 0;
  while (made < perDocument && tries < perDocument * 6) {
    tries += 1;
    const [name, fn] = pick(MUTATIONS);
    const mutated = fn(document);
    if (mutated === null || seen.has(mutated)) continue;
    seen.add(mutated);
    emit(base, name, mutated);
    made += 1;
  }
});
writeFileSync(out, lines.join('\n') + '\n');
const valid = lines.filter((l) => JSON.parse(l).valid).length;
console.log(`${lines.length} documents over ${documents.length} bases: ${valid} valid, ${lines.length - valid} invalid by graphql-js -> ${out}`);
