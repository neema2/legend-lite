/**
 * Where does cold start actually go?
 *
 * "The first plan is ~550ms" is not actionable. This splits it into
 * the phases that can be attacked separately: fetching the module,
 * compiling it, instantiating it, parsing the 300 KB Pure prelude in
 * Prelude's static initialiser, and everything the first plan does
 * after that. Whether pre-baking the boot layer is worth building
 * depends entirely on which of those holds the milliseconds.
 *
 *   node --experimental-wasm-exnref startup.mjs
 */
import { readFileSync } from 'node:fs';
import { load } from './target/wasm-runtime/org/teavm/backend/wasm/wasm-gc-module-runtime.js';

const HERE = new URL('.', import.meta.url).pathname;
const WASM = `${HERE}target/wasm/classes.wasm`;
const corpus = JSON.parse(readFileSync(`${HERE}corpus.json`, 'utf8'));
const QUERY = corpus.queries.pipeline;

const ms = (a, b) => Number(b - a) / 1e6;
const now = () => process.hrtime.bigint();

// --- raw module cost, before any Java runs ---------------------------
const t0 = now();
const bytes = readFileSync(WASM);
const t1 = now();
const compiled = await WebAssembly.compile(bytes, { builtins: ['js-string'] });
const t2 = now();
void compiled;

// --- instantiate through TeaVM's loader ------------------------------
const t3 = now();
const teavm = await load(WASM, {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = o.teavmConsole || {};
    o.teavmConsole.putcharStdout = () => {};
    o.teavmConsole.putcharStderr = () => {};
  },
});
const t4 = now();

// --- the prelude's static initialiser, alone -------------------------
const t5 = now();
const elements = teavm.exports.touchPrelude();
const t6 = now();

// --- the system metamodel's, alone -----------------------------------
const tm0 = now();
const sysElements = teavm.exports.touchSystemMetamodel();
const tm1 = now();

// --- the boot layer: resolve + normalize both, index a graph ---------
const tb0 = now();
teavm.exports.warmModel(corpus.model);
const tb1 = now();

// --- the first plan, with EVERYTHING above already warm --------------
const t7 = now();
teavm.exports.planOrError(corpus.model, QUERY, corpus.runtime);
const t8 = now();

// --- warm ------------------------------------------------------------
const warm = [];
for (let i = 0; i < 200; i++) {
  const a = now();
  teavm.exports.planOrError(corpus.model, QUERY, corpus.runtime);
  warm.push(ms(a, now()));
}
warm.sort((x, y) => x - y);

const rows = [
  ['read 4.2 MB from disk', ms(t0, t1)],
  ['WebAssembly.compile', ms(t1, t2)],
  ['TeaVM load + instantiate', ms(t3, t4)],
  [`parse prelude.pure (${elements} elements)`, ms(t5, t6)],
  [`system metamodel (${sysElements} elements)`, ms(tm0, tm1)],
  ['boot layer: resolve + normalize + index', ms(tb0, tb1)],
  ['first plan, everything above warm', ms(t7, t8)],
  ['warm plan p50', warm[Math.floor(warm.length / 2)]],
];
const total = ms(t3, t8);
const width = Math.max(...rows.map((r) => r[0].length));
console.log('cold start, attributed\n');
for (const [label, v] of rows) {
  const share = label.startsWith('warm') || label.startsWith('read')
    || label.startsWith('WebAssembly')
    ? '' : `  ${((v / total) * 100).toFixed(0).padStart(3)}% of first-answer`;
  console.log(`  ${label.padEnd(width)}  ${v.toFixed(1).padStart(7)} ms${share}`);
}
console.log(`\n  ${'first answer (instantiate → planned)'.padEnd(width)}  `
  + `${total.toFixed(1).padStart(7)} ms`);
