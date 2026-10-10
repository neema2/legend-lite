// SPIKE (2026-10-10): wasm/differential.mjs, unchanged in what it compares, with the module swapped for the GraalVM
// Web Image build. Arguments: <web image dir> <corpus/model.pure> <corpus/queries.tsv> <jvm_answers.txt>
const { load } = await import(process.env.LOADER ?? './webimage-loader.mjs');
import { readFileSync } from 'node:fs';

const [dir, modelPath, queriesPath, answersPath] = process.argv.slice(2);
const RUNTIME = 'trades::RT';

const model = readFileSync(modelPath, 'utf8');
const queries = new Map(
  readFileSync(queriesPath, 'utf8')
    .split('\n')
    .filter((l) => l !== '')
    .map((l) => [l.slice(0, l.indexOf('\t')), l.slice(l.indexOf('\t') + 1)]),
);

function parseBlocks(text) {
  const map = new Map();
  const re = /<<<([^>]+)>>>\n([\s\S]*?)\n<<<END>>>\n/g;
  let m;
  while ((m = re.exec(text)) !== null) map.set(m[1], m[2]);
  return map;
}
const jvm = parseBlocks(readFileSync(answersPath, 'utf8'));

const t0 = performance.now();
const module = await load(dir);
const t1 = performance.now();

const wasm = new Map();
let first;
for (const [n, q] of queries) {
  if (wasm.size === 1) first = performance.now() - t1;
  let v;
  try {
    v = module.exports.planOrError(model, q, RUNTIME);
  } catch (e) {
    v = `THREW-THROUGH-JS\n${e && e.message}`;
  }
  wasm.set(n, v);
}
const t2 = performance.now();

let planned = 0;
let refused = 0;
const diffs = [];
for (const n of queries.keys()) {
  const a = jvm.get(n);
  const b = wasm.get(n);
  if (a !== undefined && a === b) {
    if (a.startsWith('OK')) planned++; else refused++;
  } else {
    diffs.push({ n, jvm: a, wasm: b });
  }
}
console.log(`load ${(t1 - t0).toFixed(0)} ms, first plan ${first.toFixed(0)} ms, `
  + `next ${queries.size - 1} plans ${(t2 - t1 - first).toFixed(0)} ms`);
// a second pass: every model and plan cache warm
const t3 = performance.now();
for (const q of queries.values()) module.exports.planOrError(model, q, RUNTIME);
console.log(`second pass ${queries.size} plans ${(performance.now() - t3).toFixed(0)} ms`);
console.log(`matched ${planned + refused}/${queries.size}  (${planned} planned, ${refused} refused identically)`);
if (diffs.length) {
  console.log(`\n${diffs.length} DIFFERENCES:`);
  for (const d of diffs) {
    console.log(`\n### ${d.n}`);
    console.log(`  JVM : ${JSON.stringify(d.jvm)}`);
    console.log(`  WASM: ${JSON.stringify(d.wasm)}`);
  }
  process.exitCode = 1;
}
