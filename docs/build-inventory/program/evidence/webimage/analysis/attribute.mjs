// SPIKE (2026-10-10): each defined function's BINARY body size, named from the .wat's function order, grouped: the
// generated heap builders, allocators and field accessors, then methods by owner (lite's classes, by the jars' simple
// names, against everything else: the JDK and Web Image's runtime). Arguments: <file.wasm> <funcsizes.tsv> <our classes>
import { readFileSync } from 'node:fs';

const [wasmFile, namesFile, oursFile] = process.argv.slice(2);
const buf = readFileSync(wasmFile);
function leb(at) {
  let result = 0, shift = 0, b;
  do { b = buf[at++]; result += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80);
  return [result, at];
}
let at = 8;
const bodies = [];
while (at < buf.length) {
  const id = buf[at++];
  let size;
  [size, at] = leb(at);
  if (id === 10) {
    let [count, p] = leb(at);
    for (let i = 0; i < count; i++) {
      let s;
      [s, p] = leb(p);
      bodies.push(s);
      p += s;
    }
  }
  at += size;
}
const names = readFileSync(namesFile, 'utf8').split('\n').filter((l) => l).map((l) => l.split('\t')[0]);
if (names.length !== bodies.length) throw new Error(`${names.length} names, ${bodies.length} bodies`);
const ours = new Set(readFileSync(oursFile, 'utf8').split('\n').filter((l) => l));

const groups = new Map();
const classes = new Map();
const add = (map, k, s) => map.set(k, (map.get(k) ?? 0) + s);
let total = 0;
for (let i = 0; i < names.length; i++) {
  const n = names[i];
  const s = bodies[i];
  total += s;
  let g;
  if (/^\$func\.fillHeapObjects|^\$func\.initializeImageHeap|^\$func\.heap\.init/.test(n)) g = 'image heap builders';
  else if (/^\$func\.struct\./.test(n)) g = 'allocators';
  else if (/^\$func\.unsafe\./.test(n)) g = 'unsafe field accessors';
  else if (/^\$func\./.test(n)) g = 'other generated';
  else {
    const cls = n.slice(2).split('_')[0];
    const outer = cls.split('$')[0];
    const owner = ours.has(outer) ? 'lite' : 'jdk+runtime';
    g = `methods: ${owner}`;
    if (/___volatile___V$/.test(n)) g = `static initialisers: ${owner}`;
    add(classes, `${owner}\t${outer}`, s);
  }
  add(groups, g, s);
}
console.log(`code section bodies: ${(total / 1e6).toFixed(2)} MB`);
for (const [g, s] of [...groups].sort((a, b) => b[1] - a[1])) {
  console.log(`  ${g.padEnd(32)} ${(s / 1e6).toFixed(2).padStart(6)} MB ${(100 * s / total).toFixed(1).padStart(5)}%`);
}
console.log('largest classes:');
for (const [c, s] of [...classes].sort((a, b) => b[1] - a[1]).slice(0, Number(process.env.TOP ?? 30))) {
  console.log(`  ${(s / 1e3).toFixed(0).padStart(6)} KB  ${c}`);
}
