// SPIKE (2026-10-10): TeaVM's module behind the same load(dir) the Web Image loader has, so one harness times both.
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

export async function load(dir) {
  const { load: teavmLoad } = await import(pathToFileURL(join(dir, 'wasm-gc-module-runtime.js')).href);
  return teavmLoad(join(dir, 'classes.wasm'), {
    stackDeobfuscator: { enabled: false },
    installImports(o) {
      o.teavmConsole = o.teavmConsole || {};
      o.teavmConsole.putcharStdout = () => {};
      o.teavmConsole.putcharStderr = () => {};
    },
  });
}
