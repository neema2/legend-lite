// The DuckDB streaming path, and what cancellation really buys.
//
// `query()` runs to completion inside one C++ call and cannot be
// interrupted. `send()` streams batches, so a superseded query can be
// stopped between them and DuckDB abandons the rest of the work. That
// is the difference between a grid that SAYS it cancelled and one that
// did -- and a UI claiming idle while a worker is still pegged is
// worse than one admitting it is busy, because the next interaction is
// then slow for no reason the user can see.
//
// A fake connection, deliberately: the subject is CONTROL FLOW -- which
// batches get consumed, whether cancelSent is called -- and a real
// database cannot be made to pause between batches on cue. The real
// database is covered by duckdb.test.ts.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishTable } from '../src/duckdb.ts';
import { isSuperseded, Superseded } from '../src/epoch.ts';

function batch(regions: string[], amounts: number[]): ArrowishTable {
  const cols: Record<number, unknown[]> = { 0: regions, 1: amounts };
  return {
    numRows: regions.length,
    schema: {
      fields: [
        { name: 'region', type: 'Utf8' },
        { name: 'm', type: 'Float64' },
      ],
    },
    getChildAt: (i: number) => {
      const col = cols[i];
      return col ? { length: col.length, get: (r: number) => col[r] } : null;
    },
  };
}

describe('DuckDB streaming and cancellation', () => {
  it('stops consuming batches and cancels the pending query', async () => {
    const consumed: number[] = [];
    let cancelled = false;
    const ac = new AbortController();

    const engine = new DuckDbEngine({
      query: () => batch([], []),
      async send() {
        return {
          async *[Symbol.asyncIterator]() {
            for (let i = 0; i < 5; i++) {
              consumed.push(i);
              // Superseded while the second batch is in flight.
              if (i === 1) ac.abort(new Superseded(3));
              yield batch([`R${i}`], [i]);
            }
          },
        };
      },
      async cancelSent() {
        cancelled = true;
        return true;
      },
    });

    await assert.rejects(
      () => engine.execute('SELECT 1', 1, ac.signal),
      (e: unknown) => isSuperseded(e),
    );
    assert.equal(cancelled, true, 'the pending query was cancelled');
    assert.ok(
      consumed.length < 5,
      `stopped early: consumed ${consumed.length} of 5 batches`,
    );
  });

  it('materialises a streamed result exactly like a whole-table one', async () => {
    // Two materialisation paths that disagree would be a worse bug
    // than the waste streaming was added to remove, so they are
    // compared directly on the same rows.
    const whole = batch(['EMEA', 'AMER', 'APAC'], [1, 2, 3]);
    const viaQuery = await new DuckDbEngine({ query: () => whole })
      .execute('SELECT 1', 7);

    const viaStream = await new DuckDbEngine({
      query: () => whole,
      async send() {
        return {
          async *[Symbol.asyncIterator]() {
            // The same rows, arriving in three batches instead of one.
            yield batch(['EMEA'], [1]);
            yield batch(['AMER'], [2]);
            yield batch(['APAC'], [3]);
          },
        };
      },
      async cancelSent() {
        return false;
      },
    }).execute('SELECT 1', 7, new AbortController().signal);

    assert.deepEqual(
      viaStream.columns.map((c) => ({ name: c.name, values: c.values })),
      viaQuery.columns.map((c) => ({ name: c.name, values: c.values })),
    );
    assert.equal(viaStream.rowCount, viaQuery.rowCount);
    assert.equal(viaStream.epoch, 7);
  });

  it('keeps the columns of an empty streamed result', async () => {
    // No rows must still mean headers: an empty grid WITH columns,
    // not a grid with none.
    const engine = new DuckDbEngine({
      query: () => batch([], []),
      async send() {
        return {
          async *[Symbol.asyncIterator]() {
            yield batch([], []);
          },
        };
      },
      async cancelSent() {
        return false;
      },
    });
    const out = await engine.execute(
      'SELECT 1',
      1,
      new AbortController().signal,
    );
    assert.equal(out.rowCount, 0);
    assert.deepEqual(out.columns.map((c) => c.name), ['region', 'm']);
  });

  it('falls back to query() when the connection cannot stream', async () => {
    let usedQuery = false;
    const engine = new DuckDbEngine({
      query: () => {
        usedQuery = true;
        return batch(['EMEA'], [1]);
      },
    });
    const out = await engine.execute(
      'SELECT 1',
      1,
      new AbortController().signal,
    );
    assert.equal(usedQuery, true, 'no send(), so query() is used');
    assert.equal(out.rowCount, 1);
  });

  it('does not report a cancelled stream as a query error', async () => {
    // The stream throws on abort, and the abort reason must survive
    // rather than being wrapped as a database failure.
    const ac = new AbortController();
    const engine = new DuckDbEngine({
      query: () => batch([], []),
      async send() {
        return {
          async *[Symbol.asyncIterator]() {
            ac.abort(new Superseded(2));
            throw new Error('query was cancelled by the runtime');
            // eslint-disable-next-line no-unreachable
            yield batch([], []);
          },
        };
      },
      async cancelSent() {
        return true;
      },
    });

    await assert.rejects(
      () => engine.execute('SELECT 1', 1, ac.signal),
      (e: unknown) => isSuperseded(e),
    );
  });
});
