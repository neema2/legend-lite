// A part of the app fetched when first wanted (src/lazily.ts): a part whose file is gone -- the site updated under an
// open page -- says so in words a person can act on (the user, 2026-10-23: "a cryptic error on the open dialog").

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { lazily, partGone, UPDATED } from '../src/lazily.ts';

describe('a part fetched when first wanted', () => {
  it('a module the browser could not fetch, in any browser\'s words, is told as the page being older than the site', async () => {
    for (const words of [
      'Failed to fetch dynamically imported module: https://x/datacube/demo/chunks-bundle/charts-dashboard-OHVH3L56.js',
      'error loading dynamically imported module: https://x/chunk.js',
      'Importing a module script failed.',
    ]) {
      const gone = new TypeError(words);
      assert.equal(partGone(gone), true, words);
      await assert.rejects(lazily(() => Promise.reject(gone)), (e: unknown) => e instanceof Error && e.message === UPDATED && e.cause === gone);
    }
    assert.match(UPDATED, /reload the page/);
  });

  it('anything else as it is, and a part fetched is the part', async () => {
    const other = new TypeError('x is not a function');
    assert.equal(partGone(other), false);
    assert.equal(partGone(new Error('Failed to fetch dynamically imported module: y')), false, 'the browser\'s is a TypeError');
    await assert.rejects(lazily(() => Promise.reject(other)), (e: unknown) => e === other);
    assert.deepEqual(await lazily(async () => ({ part: 1 })), { part: 1 });
  });
});
