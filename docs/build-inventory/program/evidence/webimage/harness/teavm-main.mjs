// SPIKE (2026-10-10): run a TeaVM module's main(args), printing what it writes to stdout.
// Arguments: <teavm dir> ...args
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [dir, ...args] = process.argv.slice(2);
const { load } = await import(pathToFileURL(join(dir, 'wasm-gc-module-runtime.js')).href);
let line = '';
const m = await load(join(dir, 'classes.wasm'), {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = o.teavmConsole || {};
    o.teavmConsole.putcharStdout = (c) => { if (c === 10) { console.log(line); line = ''; } else line += String.fromCharCode(c); };
    o.teavmConsole.putcharStderr = () => {};
  },
});
m.exports.main(args);
if (line) console.log(line);
