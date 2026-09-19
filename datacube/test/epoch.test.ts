import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { EpochGuard, STALE, isStale } from '../src/epoch.ts';

/** A promise plus the handles to settle it, for controlling ordering. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('EpochGuard', () => {
  it('returns the value when nothing supersedes it', async () => {
    const g = new EpochGuard();
    const out = await g.issue(async () => 'rows');
    assert.equal(out, 'rows');
    assert.equal(isStale(out), false);
  });

  it('discards a result superseded while in flight', async () => {
    const g = new EpochGuard();
    const first = deferred<string>();

    const pending = g.issue(async () => first.promise);
    g.advance(); // a second interaction arrives
    first.resolve('old rows');

    assert.equal(await pending, STALE);
  });

  it('keeps the newest answer when answers arrive out of order', async () => {
    const g = new EpochGuard();
    const slowOld = deferred<string>();
    const fastNew = deferred<string>();

    const oldRun = g.issue(async () => slowOld.promise);
    const newRun = g.issue(async () => fastNew.promise);

    // The NEW query finishes first, then the old one straggles in.
    fastNew.resolve('new rows');
    slowOld.resolve('old rows');

    assert.equal(await newRun, 'new rows');
    assert.equal(await oldRun, STALE);
  });

  it('never starts work for an already-superseded epoch', async () => {
    const g = new EpochGuard();
    const epoch = g.advance();
    g.advance(); // superseded before the task is handed over

    let started = false;
    const out = await g.run(epoch, async () => {
      started = true;
      return 'rows';
    });

    assert.equal(out, STALE);
    assert.equal(started, false, 'task must not run');
  });

  it('swallows a failure from a superseded query', async () => {
    const seen: unknown[] = [];
    const g = new EpochGuard({
      onDiscardedError: (e) => seen.push(e),
    });
    const first = deferred<string>();

    const pending = g.issue(async () => first.promise);
    g.advance();
    // DuckDB interrupts the loser when a second query starts on the
    // same connection; that must not reach the user as an error.
    first.reject(new Error('EXECUTION_CANCELLED'));

    assert.equal(await pending, STALE);
    assert.equal(seen.length, 1);
    assert.match(String(seen[0]), /EXECUTION_CANCELLED/);
  });

  it('still surfaces a failure from the current query', async () => {
    const g = new EpochGuard();
    await assert.rejects(
      () => g.issue(async () => Promise.reject(new Error('syntax error'))),
      /syntax error/,
    );
  });

  it('tracks inflight count back to zero', async () => {
    const g = new EpochGuard();
    const d = deferred<string>();
    const p = g.issue(async () => d.promise);
    assert.equal(g.inflight, 1);
    d.resolve('rows');
    await p;
    assert.equal(g.inflight, 0);
  });

  it('advances monotonically', () => {
    const g = new EpochGuard();
    assert.equal(g.current, 0);
    assert.equal(g.advance(), 1);
    assert.equal(g.advance(), 2);
    assert.equal(g.isCurrent(2), true);
    assert.equal(g.isCurrent(1), false);
  });
});
