/**
 * Differential test: the SAME planner source, compiled two ways.
 *
 * TeaVM compiles `planner.Wasm` to WebAssembly (//wasm:planner); `planner.JvmMain`
 * calls the very same class on the JVM (//wasm:jvm_answers, a build action). So
 * this compares two BUILDS OF ONE SOURCE rather than two hand-kept copies that
 * could drift apart silently. Every answer is compared — refusals included,
 * because a refusal is an answer the planner is expected to give.
 *
 *   bazel test //wasm:differential_test
 *
 * Node 22 needs --experimental-wasm-exnref (the BUILD passes it): TeaVM emits
 * the newer exception-handling opcodes (0x1f), and without the flag the module
 * does not even compile ("Invalid opcode 0x1f"). Browsers that have shipped
 * WASM-GC need no flag.
 *
 * Every path is beside this file, where Bazel lays the declared inputs out.
 */
import { load } from './planner/wasm-gc-module-runtime.js';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const RUNTIME = 'trades::RT';

const model = readFileSync(here('./corpus/model.pure'), 'utf8');
const queries = new Map(
  readFileSync(here('./corpus/queries.tsv'), 'utf8')
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
const jvm = parseBlocks(readFileSync(here('./jvm_answers.txt'), 'utf8'));

// ---- WASM side ------------------------------------------------------
// TeaVM's loader wants a FILESYSTEM PATH under Node (a file: URL fails with an
// ENOENT that reads like a missing WASM-GC feature).
const teavm = await load(here('./planner/classes.wasm'), {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = o.teavmConsole || {};
    o.teavmConsole.putcharStdout = () => {};
    o.teavmConsole.putcharStderr = () => {};
  },
});

const wasm = new Map();
for (const [n, q] of queries) {
  let v;
  try {
    v = teavm.exports.planOrError(model, q, RUNTIME);
  } catch (e) {
    // planOrError folds Java failures into its return value, so reaching
    // here means the module itself broke — worth seeing, not swallowing.
    v = `THREW-THROUGH-JS\n${e && e.message}`;
  }
  wasm.set(n, v);
}

// ---- compare --------------------------------------------------------
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

const bodies = new Set([...wasm.values()].filter((v) => v.startsWith('OK')));
console.log(`matched ${planned + refused}/${queries.size}  `
  + `(${planned} planned, ${refused} refused identically)`);
console.log(`distinct SQL bodies: ${bodies.size}`);
// A corpus that plans nothing compares refusals only, and two builds that both
// refuse everything would agree perfectly while the planner is broken.
if (planned < queries.size / 2) {
  console.log(`only ${planned} of ${queries.size} planned: the corpus no longer exercises the planner`);
  process.exitCode = 1;
}
if (diffs.length) {
  console.log(`\n${diffs.length} DIFFERENCES:`);
  for (const d of diffs) {
    console.log(`\n### ${d.n}`);
    console.log(`  JVM : ${JSON.stringify(d.jvm)}`);
    console.log(`  WASM: ${JSON.stringify(d.wasm)}`);
  }
  process.exitCode = 1;
}
