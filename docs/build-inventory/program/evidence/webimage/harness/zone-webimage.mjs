// SPIKE (2026-10-10): wasm/zoneprobe.mjs with the module swapped for the GraalVM Web Image build.
// Arguments: <web image dir> <zone_jvm.txt>
import { load } from './webimage-loader.mjs';
import { readFileSync } from 'node:fs';

const [dir, answers] = process.argv.slice(2);
const jvm = readFileSync(answers, 'utf8').split(/\r?\n/).filter((l) => l !== '');
const module = await load(dir);

let differ = 0;
for (const line of jvm) {
  const [iso, zone] = line.split('\t');
  const wasm = `${iso}\t${zone}\t${module.exports.zoneProbe(iso, zone)}`;
  const same = wasm === line;
  if (!same) differ++;
  console.log(`${same ? 'MATCH ' : 'DIFFER'}  ${line}${same ? '' : `\n    wasm: ${wasm}`}`);
}
if (jvm.length < 8) {
  console.log(`only ${jvm.length} answers from the JVM`);
  process.exitCode = 1;
}
if (differ) process.exitCode = 1;
