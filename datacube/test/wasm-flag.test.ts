// THE WASM LIST CANNOT DRIFT (Bazel workplan P1-23, A28): BUILD.bazel's _WASM_TESTS, the tests node_test gives the
// WebAssembly planner (wasm = True), is exactly the set of test files that import the helpers which load it
// (catalog-builder, lite-compiler). A test added to one side only fails here, naming it.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';

test('the wasm = True tests are exactly the ones importing catalog-builder or lite-compiler', () => {
  const build = readFileSync('BUILD.bazel', 'utf8');
  const body = /_WASM_TESTS = \[([^\]]*)\]/.exec(build)?.[1];
  assert.ok(body !== undefined, 'BUILD.bazel has no _WASM_TESTS list');
  const listed = [...body.matchAll(/"([^"]+)"/g)].map((m) => m[1]).sort();
  const importing = readdirSync('test')
    .filter((f) => f.endsWith('.test.ts'))
    .filter((f) => /from '\.\/(catalog-builder|lite-compiler)(\.ts)?'/.test(readFileSync(`test/${f}`, 'utf8')))
    .map((f) => f.slice(0, -'.test.ts'.length))
    .sort();
  assert.ok(importing.length > 0, 'no test file imports the planner helpers: the scan is not looking');
  assert.deepEqual(listed, importing);
});
