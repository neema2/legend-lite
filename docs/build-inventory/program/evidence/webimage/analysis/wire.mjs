// SPIKE (2026-10-10): what each piece of a Web Image module costs ON THE WIRE -- every group of function bodies, and
// every other section, Brotli-compressed on its own (quality 11), beside its raw size. The groups compress apart, so
// their sum runs a little above the whole file's Brotli size. Arguments: <file.wasm> <funcsizes.tsv> <our classes>
// [<extra file>...] (extra files, e.g. prelude.pure, are compressed alone for comparison)
import { readFileSync } from 'node:fs';
import zlib from 'node:zlib';

const [wasmFile, namesFile, oursFile, ...extra] = process.argv.slice(2);
const buf = readFileSync(wasmFile);
const br = (b) => zlib.brotliCompressSync(b, { params: {
  [zlib.constants.BROTLI_PARAM_QUALITY]: 11, [zlib.constants.BROTLI_PARAM_LGWIN]: 24,
  [zlib.constants.BROTLI_PARAM_SIZE_HINT]: b.length } }).length;
function leb(at) {
  let result = 0, shift = 0, b;
  do { b = buf[at++]; result += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80);
  return [result, at];
}
const NAMES = ['custom', 'type', 'import', 'function', 'table', 'memory', 'global', 'export', 'start', 'element', 'code',
  'data', 'datacount', 'tag'];
const names = readFileSync(namesFile, 'utf8').split('\n').filter((l) => l).map((l) => l.split('\t')[0]);
const ours = new Set(readFileSync(oursFile, 'utf8').split('\n').filter((l) => l));
const groups = new Map();
const put = (g, chunk) => { if (!groups.has(g)) groups.set(g, []); groups.get(g).push(chunk); };
let at = 8;
while (at < buf.length) {
  const id = buf[at++];
  let size;
  [size, at] = leb(at);
  const section = buf.subarray(at, at + size);
  if (id === 10) {
    let [count, p] = leb(at);
    for (let i = 0; i < count; i++) {
      let s;
      [s, p] = leb(p);
      const body = buf.subarray(p, p + s);
      p += s;
      const n = names[i];
      let g;
      if (/^\$func\.fillHeapObjects|^\$func\.initializeImageHeap|^\$func\.heap\.init/.test(n)) g = 'code: startup heap builders';
      else if (/^\$func\./.test(n)) g = 'code: generated helpers';
      else g = ours.has(n.slice(2).split('_')[0].split('$')[0]) ? 'code: lite methods' : 'code: jdk + runtime methods';
      put(g, body);
    }
  } else if (id === 11) {
    // the data segment: the Pure text in it (prelude.pure and Pure source held in Java strings) apart from the rest
    const text = section.toString('latin1');
    const prelude = readFileSync(extra[0] ?? '/dev/null', 'latin1');
    const pureRuns = [];
    let rest = text;
    const at0 = prelude.length > 0 ? text.indexOf(prelude.slice(0, 200)) : -1;
    if (at0 >= 0) {
      put('data: prelude.pure (resource)', section.subarray(at0, at0 + prelude.length));
      rest = text.slice(0, at0) + text.slice(at0 + prelude.length);
    }
    // Pure source held in Java string constants: long runs that read as Pure (a '::' path and a Pure keyword)
    const re = /[\x09\x0a\x0d\x20-\x7e]{400,}/g;
    let m;
    const keep = [];
    let last = 0;
    while ((m = re.exec(rest)) !== null) {
      if (/::/.test(m[0]) && /(Class |Database |Mapping|function |Association |Enum |Table )/.test(m[0])) {
        pureRuns.push(m[0]);
        keep.push(rest.slice(last, m.index));
        last = m.index + m[0].length;
      }
    }
    keep.push(rest.slice(last));
    put('data: Pure source in Java strings', Buffer.from(pureRuns.join(''), 'latin1'));
    put('data: other (strings, names, tables)', Buffer.from(keep.join(''), 'latin1'));
  } else {
    put(`section: ${NAMES[id] ?? id}`, section);
  }
  at += size;
}
let rawTotal = 0, brTotal = 0;
const rows = [];
for (const [g, chunks] of groups) {
  const b = Buffer.concat(chunks);
  const c = br(b);
  rawTotal += b.length;
  brTotal += c;
  rows.push([g, b.length, c]);
}
rows.sort((a, b) => b[2] - a[2]);
console.log(`${wasmFile.split('/').slice(-2).join('/')}: whole file Brotli ${(br(buf) / 1e6).toFixed(2)} MB`);
console.log('piece'.padEnd(40), 'raw MB'.padStart(8), 'brotli MB'.padStart(10), 'share'.padStart(7));
for (const [g, r, c] of rows) {
  console.log(g.padEnd(40), (r / 1e6).toFixed(2).padStart(8), (c / 1e6).toFixed(2).padStart(10),
    `${(100 * c / brTotal).toFixed(1)}%`.padStart(7));
}
console.log('sum of pieces'.padEnd(40), (rawTotal / 1e6).toFixed(2).padStart(8), (brTotal / 1e6).toFixed(2).padStart(10));
for (const f of extra) {
  const b = readFileSync(f);
  console.log(`alone: ${f.split('/').pop()} raw ${(b.length / 1e3).toFixed(0)} KB, brotli ${(br(b) / 1e3).toFixed(0)} KB`);
}
