// SPIKE (2026-10-10): a wasm file's size by section (and custom sections by name). Arguments: <file.wasm>...
import { readFileSync } from 'node:fs';

const NAMES = ['custom', 'type', 'import', 'function', 'table', 'memory', 'global', 'export', 'start', 'element', 'code',
  'data', 'datacount', 'tag'];

function leb(buf, at) {
  let result = 0, shift = 0, b;
  do { b = buf[at++]; result += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80);
  return [result, at];
}

for (const file of process.argv.slice(2)) {
  const buf = readFileSync(file);
  const sizes = new Map();
  let at = 8;
  let functions = 0, dataSegments = 0;
  while (at < buf.length) {
    const id = buf[at++];
    let size;
    [size, at] = leb(buf, at);
    let name = NAMES[id] ?? `id${id}`;
    if (id === 0) {
      const [len, p] = leb(buf, at);
      name = `custom:${buf.subarray(p, p + len).toString('utf8')}`;
    }
    if (id === 10) functions = leb(buf, at)[0];
    if (id === 11) dataSegments = leb(buf, at)[0];
    sizes.set(name, (sizes.get(name) ?? 0) + size);
    at += size;
  }
  console.log(`${file.split('/').slice(-2).join('/')}: ${(buf.length / 1e6).toFixed(2)} MB, ${functions} functions, `
    + `${dataSegments} data segments`);
  for (const [n, s] of [...sizes].sort((a, b) => b[1] - a[1])) {
    console.log(`  ${n.padEnd(28)} ${(s / 1e6).toFixed(2).padStart(7)} MB  ${(100 * s / buf.length).toFixed(1).padStart(5)}%`);
  }
}
