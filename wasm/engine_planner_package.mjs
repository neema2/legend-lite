/**
 * The engine module's package (//wasm:engine_planner_package) as a host outside this repository uses it: every file
 * its package.json names is in it, and the module, loaded through the package's own entry, answers a pure/v1 route
 * with the one export the package's types declare.
 *
 *   bazel test //wasm:engine_planner_package_test
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { runfileDirUrl } from '../tools/js/runfiles.mts';

const pkg = runfileDirUrl('ENGINE_PLANNER_PACKAGE');
const manifest = JSON.parse(readFileSync(new URL('package.json', pkg), 'utf8'));

/** Every file an `exports` entry leads to: a string, or an object of conditions. */
function targets(entry) {
  return typeof entry === 'string' ? [entry] : Object.values(entry).flatMap(targets);
}

test('every file the package names is in it', () => {
  const named = new Set([...manifest.files, manifest.types, ...targets(manifest.exports)]);
  for (const file of named) {
    assert.ok(existsSync(new URL(file, pkg)), `${file} is named by package.json and is not in the package`);
  }
});

test('the module, loaded through the package entry, answers a pure/v1 route', async () => {
  const { load } = await import(new URL(manifest.exports['.'].default, pkg).href);
  // TeaVM's loader wants a filesystem path under Node (a file: URL fails with an ENOENT)
  const planner = await load(fileURLToPath(new URL(manifest.exports['./classes.wasm'], pkg)), {
    stackDeobfuscator: { enabled: false },
    installImports(imports) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
  const answer = planner.exports.pureV1OrError(
    '/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', '|1 + 1');
  assert.ok(answer.startsWith('OK\n200\n'), `not a 200: ${answer.slice(0, 300)}`);
  const body = JSON.parse(answer.slice(answer.indexOf('\n', 'OK\n200\n'.length) + 1));
  assert.equal(body._type, 'lambda');
});
