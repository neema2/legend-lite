// A start that fails is begun again at the next ask; one that succeeds, or is under way, is shared (2026-11-15, the
// TypeScript hunt's T-B: Studio held a failed DuckDB start for the page's life).

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';
import { startOnce } from '../src/app/start-once.ts';

describe('a start made once', () => {
  it('is shared while it runs and once it succeeds, and begun again after it fails', async () => {
    const outcomes = [new Error("DuckDB's worker stopped: it, or its WebAssembly, did not load"), 'the tab'];
    let begun = 0;
    const start = startOnce(async () => {
      const outcome = outcomes[begun++]!;
      if (outcome instanceof Error) throw outcome;
      return outcome;
    });
    // two asks at once share the first start, which fails
    const [a, b] = [start(), start()];
    await assert.rejects(a, /did not load/);
    await assert.rejects(b, /did not load/);
    assert.equal(begun, 1);
    // the next ask begins it again, and that one is kept
    assert.equal(await start(), 'the tab');
    assert.equal(await start(), 'the tab');
    assert.equal(begun, 2);
  });
});
