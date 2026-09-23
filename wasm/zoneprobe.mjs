/**
 * The TIMEZONE differential — see planner/ZoneMain.java.
 *
 *   bazel test //wasm:zone_test
 *
 * A timezone database is a RESOURCE, not code, so whether it survives the WASM
 * build is a question only the built module can answer. ZoneMain asks the JVM
 * (//wasm:zone_jvm, a build output: `iso<TAB>zone<TAB>answer` per line); this
 * asks the module the SAME questions — read from those lines, so the two sides
 * cannot hold different case lists — and compares.
 */
import { load } from './planner/wasm-gc-module-runtime.js';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));

// println ends a line with CRLF on Windows
const jvm = readFileSync(here('./zone_jvm.txt'), 'utf8').split(/\r?\n/).filter((l) => l !== '');
const teavm = await load(here('./planner/classes.wasm'), {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = o.teavmConsole || {};
    o.teavmConsole.putcharStdout = () => {};
    o.teavmConsole.putcharStderr = () => {};
  },
});

let differ = 0;
for (const line of jvm) {
  const [iso, zone] = line.split('\t');
  const wasm = `${iso}\t${zone}\t${teavm.exports.zoneProbe(iso, zone)}`;
  const same = wasm === line;
  if (!same) differ++;
  console.log(`${same ? 'MATCH ' : 'DIFFER'}  ${line}${same ? '' : `\n    wasm: ${wasm}`}`);
}
// ZoneMain asks eight questions; fewer means the JVM side broke, not that
// the module agrees.
if (jvm.length < 8) {
  console.log(`only ${jvm.length} answers from the JVM`);
  process.exitCode = 1;
}
if (differ) process.exitCode = 1;
