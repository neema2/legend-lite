// SPIKE (2026-10-10): what lambda classes cost in a Web Image module: the binary bytes of every function that exists
// only for a lambda class (its method, its allocator, its field accessors), split by owner (lite's classes against
// the JDK's and the runtime's), raw and Brotli. Arguments: <file.wasm> <funcsizes.tsv> <our classes>
import { readFileSync } from 'node:fs';
import zlib from 'node:zlib';

const [wasmFile, namesFile, oursFile] = process.argv.slice(2);
const buf = readFileSync(wasmFile);
function leb(at) {
  let result = 0, shift = 0, b;
  do { b = buf[at++]; result += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80);
  return [result, at];
}
const names = readFileSync(namesFile, 'utf8').split('\n').filter((l) => l).map((l) => l.split('\t')[0]);
const ours = new Set(readFileSync(oursFile, 'utf8').split('\n').filter((l) => l));
const bodies = [];
let at = 8;
while (at < buf.length) {
  const id = buf[at++];
  let size;
  [size, at] = leb(at);
  if (id === 10) {
    let [count, p] = leb(at);
    for (let i = 0; i < count; i++) { let s; [s, p] = leb(p); bodies.push(buf.subarray(p, p + s)); p += s; }
  }
  at += size;
}
const groups = { lite: [], other: [] };
let all = 0;
for (let i = 0; i < names.length; i++) {
  all += bodies[i].length;
  const n = names[i];
  if (!n.includes('$$Lambda')) continue;
  // the class that wrote the lambda: the simple name just before "$$Lambda"
  const before = n.slice(0, n.indexOf('$$Lambda'));
  const owner = before.split(/[._]/).pop();
  groups[ours.has(owner) ? 'lite' : 'other'].push(bodies[i]);
}
const br = (x) => zlib.brotliCompressSync(x, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11 } }).length;
for (const [k, chunks] of Object.entries(groups)) {
  const b = Buffer.concat(chunks);
  console.log(`${k.padEnd(6)} lambda-class functions: ${String(chunks.length).padStart(6)}, raw ${(b.length / 1e6).toFixed(2)} MB `
    + `(${(100 * b.length / all).toFixed(1)}% of code), brotli ${(br(b) / 1e6).toFixed(2)} MB`);
}
