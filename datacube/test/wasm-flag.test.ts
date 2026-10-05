// THE WASM LIST CANNOT DRIFT (Bazel workplan P1-23, A28): BUILD.bazel's _WASM_TESTS, the tests node_test gives the
// WebAssembly planner (wasm = True), is exactly the set of test files that import the helpers which load it
// (catalog-builder, lite-compiler). A test added to one side only fails here, naming it.
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { Sources } from '../../tools/js/runfiles.mts';

// what this reads, as the BUILD target declares it (Bazel workplan P3-29)
const SOURCES = new Sources('SOURCES', 'datacube');

test('the wasm = True tests are exactly the ones importing catalog-builder or lite-compiler', () => {
  const build = SOURCES.read('BUILD.bazel');
  const body = /_WASM_TESTS = \[([^\]]*)\]/.exec(build)?.[1];
  assert.ok(body !== undefined, 'BUILD.bazel has no _WASM_TESTS list');
  const listed = [...body.matchAll(/"([^"]+)"/g)].map((m) => m[1]).sort();
  const importing = SOURCES.under('test', '.test.ts')
    .filter((f) => !f.slice('test/'.length).includes('/'))
    .filter((f) => /from '\.\/(catalog-builder|lite-compiler)(\.ts)?'/.test(SOURCES.read(f)))
    .map((f) => f.slice('test/'.length, -'.test.ts'.length))
    .sort();
  assert.ok(importing.length > 0, 'no test file imports the planner helpers: the scan is not looking');
  assert.deepEqual(listed, importing);
});
