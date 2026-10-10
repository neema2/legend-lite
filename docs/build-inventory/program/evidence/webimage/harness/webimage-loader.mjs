// SPIKE (2026-10-10): load the GraalVM Web Image build of the tab's adapter and present it as TeaVM's loader does --
// { exports: { planOrError(...), pureV1OrError(...), ... } } -- so the differentials run unchanged against it.
import { createRequire } from 'node:module';
import { join } from 'node:path';

const NAMES = [
  'pureV1OrError', 'plan', 'planOrError', 'relationTypeOrError', 'planJsonOrError', 'relationTypeJsonOrError',
  'compileOrError', 'zoneProbe', 'warmModel', 'touchPrelude', 'touchSystemMetamodel', 'hashBootSource',
  'resolveBootLayer',
];
const NUMERIC = new Set(['warmModel', 'touchPrelude', 'touchSystemMetamodel', 'hashBootSource', 'resolveBootLayer']);

export async function load(dir) {
  const require = createRequire(import.meta.url);
  require(join(dir, 'planner.js'));
  const config = new globalThis.GraalVM.Config();
  config.wasm_path = join(dir, 'planner.js.wasm');
  await globalThis.GraalVM.run([], config);
  const call = globalThis.legendPlanner;
  const exports = {};
  for (const n of NAMES) {
    exports[n] = NUMERIC.has(n)
      ? (a, b, c) => Number(call(n, a, b, c))
      : (a, b, c) => call(n, a, b, c);
  }
  return { exports };
}
