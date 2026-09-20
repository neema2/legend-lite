/**
 * Differential test: the SAME planner source, compiled two ways.
 *
 * TeaVM compiles `planner.Wasm` to WebAssembly; `planner.JvmMain` calls
 * the very same class on the JVM. So this compares two BUILDS OF ONE
 * SOURCE rather than two hand-kept copies that could drift apart
 * silently. Every answer is compared — refusals included, because a
 * refusal is an answer the planner is expected to give.
 *
 *   node --experimental-wasm-exnref differential.mjs
 *
 * The flag is required on Node 22: TeaVM emits the newer
 * exception-handling opcodes (0x1f) and without it the module will not
 * even compile ("Invalid opcode 0x1f"). Browsers that have shipped
 * WASM-GC need no flag.
 *
 * Override JAVA_HOME / LEGEND_CORE_JAR if yours live elsewhere.
 */
import { load } from './target/wasm-runtime/org/teavm/backend/wasm/wasm-gc-module-runtime.js';
import { readFileSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { homedir } from 'node:os';

const HERE = new URL('.', import.meta.url).pathname;
const JAVA = (process.env.JAVA_HOME ?? `${homedir()}/jdk/jdk-21.0.11+10/Contents/Home`) + '/bin/java';
const CORE = process.env.LEGEND_CORE_JAR
  ?? `${homedir()}/.m2/repository/com/legend/legend-lite-core/1.0.0-SNAPSHOT/legend-lite-core-1.0.0-SNAPSHOT.jar`;
const CLASSES = HERE + 'target/classes';

const corpus = JSON.parse(readFileSync(HERE + 'corpus.json', 'utf8'));
const names = Object.keys(corpus.queries);

writeFileSync(HERE + 'target/corpus-model.txt', corpus.model);
writeFileSync(
  HERE + 'target/corpus-queries.tsv',
  names.map((n) => `${n}\t${corpus.queries[n]}`).join('\n') + '\n',
);

// ---- JVM side -------------------------------------------------------
console.log(`corpus: ${names.length} queries\n`);
execFileSync(
  JAVA,
  ['-cp', `${CORE}:${CLASSES}`, 'planner.JvmMain',
    HERE + 'target/corpus-model.txt', HERE + 'target/corpus-queries.tsv',
    corpus.runtime, HERE + 'target/jvm-out.txt'],
  { encoding: 'utf8', stdio: ['ignore', 'inherit', 'inherit'] },
);

function parseBlocks(text) {
  const map = new Map();
  const re = /<<<([^>]+)>>>\n([\s\S]*?)\n<<<END>>>\n/g;
  let m;
  while ((m = re.exec(text)) !== null) map.set(m[1], m[2]);
  return map;
}
const jvm = parseBlocks(readFileSync(HERE + 'target/jvm-out.txt', 'utf8'));

// ---- WASM side ------------------------------------------------------
const teavm = await load(HERE + 'target/wasm/classes.wasm', {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = o.teavmConsole || {};
    o.teavmConsole.putcharStdout = () => {};
    o.teavmConsole.putcharStderr = () => {};
  },
});

// The first plan pays for class init plus parsing the 300 KB prelude.
const tb0 = process.hrtime.bigint();
teavm.exports.planOrError(corpus.model, corpus.queries[names[0]], corpus.runtime);
const bootMs = Number(process.hrtime.bigint() - tb0) / 1e6;

const wasm = new Map();
for (const n of names) {
  let v;
  try {
    v = teavm.exports.planOrError(corpus.model, corpus.queries[n], corpus.runtime);
  } catch (e) {
    // planOrError folds Java failures into its return value, so reaching
    // here means the module itself broke — worth seeing, not swallowing.
    v = `THREW-THROUGH-JS\n${e && e.message}`;
  }
  wasm.set(n, v);
}

const warmQ = corpus.queries.pipeline ?? corpus.queries[names[0]];
const s = [];
for (let i = 0; i < 500; i++) {
  const a = process.hrtime.bigint();
  teavm.exports.planOrError(corpus.model, warmQ, corpus.runtime);
  s.push(Number(process.hrtime.bigint() - a) / 1e6);
}
s.sort((x, y) => x - y);
const p = (q) => s[Math.min(s.length - 1, Math.floor(s.length * q))].toFixed(3);
console.error(`WASM bootMs=${bootMs.toFixed(2)} p50=${p(0.5)} p95=${p(0.95)} `
  + `p99=${p(0.99)} min=${s[0].toFixed(3)} max=${s[s.length - 1].toFixed(3)} n=${s.length}`);

// ---- compare --------------------------------------------------------
let match = 0;
let planned = 0;
let refused = 0;
const diffs = [];
for (const n of names) {
  const a = jvm.get(n);
  const b = wasm.get(n);
  if (a === b) {
    match++;
    if (a?.startsWith('OK')) planned++; else refused++;
  } else {
    diffs.push({ n, jvm: a, wasm: b });
  }
}

const bodies = new Set([...wasm.values()].filter((v) => v.startsWith('OK')));
console.log(`\nmatched ${match}/${names.length}  `
  + `(${planned} planned, ${refused} refused identically)`);
console.log(`distinct SQL bodies: ${bodies.size}`);
if (diffs.length) {
  console.log(`\n${diffs.length} DIFFERENCES:`);
  for (const d of diffs) {
    console.log(`\n### ${d.n}`);
    console.log(`  JVM : ${JSON.stringify(d.jvm)}`);
    console.log(`  WASM: ${JSON.stringify(d.wasm)}`);
  }
}
writeFileSync(HERE + 'target/wasm-out.json',
  JSON.stringify(Object.fromEntries(wasm), null, 2));
process.exit(diffs.length ? 1 : 0);
