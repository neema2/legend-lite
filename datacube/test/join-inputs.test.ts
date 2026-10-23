// The Join window's inputs while it is open (src/join-open.ts JoinInputs) and its questions' answers read
// (summaryCounts): a grid's inputs opened once however many ask at once, a question's planner held until it is answered
// and every one let go when the window closes, after what is being answered; an answer read strictly -- a count over no
// rows is none, an answer missing its column is refused, never taken for none.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { RawTable } from '../../engine-client/src/engine.ts';
import { JoinInputs, summaryCounts } from '../src/join-open.ts';
import { TOTAL_ROWS_COLUMN } from '../src/query.ts';

/** Inputs whose letting go is recorded. */
function opener(): { open: (their: string) => Promise<{ their: string; dispose(): void }>; opened: string[]; disposed: string[]; release: () => void } {
  const opened: string[] = [];
  const disposed: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  return {
    opened, disposed, release,
    open: async (their) => {
      opened.push(their);
      await gate;
      return { their, dispose: () => disposed.push(their) };
    },
  };
}

describe('the Join window\'s inputs', () => {
  it('opens a grid\'s once, however many ask at once, and each grid\'s its own', async () => {
    const o = opener();
    const inputs = new JoinInputs(o.open);
    const both = Promise.all([inputs.get('grid-2'), inputs.get('grid-2'), inputs.get('grid-3')]);
    o.release();
    const [a, b, c] = await both;
    assert.equal(a, b);
    assert.equal(c.their, 'grid-3');
    assert.deepEqual(o.opened, ['grid-2', 'grid-3']);
  });

  it('lets every one go when it closes -- after a question still being answered over one', async () => {
    const o = opener();
    o.release();
    const inputs = new JoinInputs(o.open);
    let answer!: () => void;
    const answering = inputs.use('grid-2', () => new Promise<string>((r) => { answer = () => r('3 rows'); }));
    await new Promise((r) => setTimeout(r, 0));
    const closing = inputs.close();
    await new Promise((r) => setTimeout(r, 0));
    assert.deepEqual(o.disposed, [], 'not under the question being answered');
    answer();
    assert.equal(await answering, '3 rows');
    await closing;
    assert.deepEqual(o.disposed, ['grid-2']);
    await assert.rejects(inputs.get('grid-3'), /the Join window was closed/);
  });

  it('opens a grid whose opening failed again when it is asked again', async () => {
    let tries = 0;
    const inputs = new JoinInputs(async (their) => {
      tries += 1;
      if (tries === 1) throw new Error('not read');
      return { their, dispose: () => {} };
    });
    await assert.rejects(inputs.get('grid-2'), /not read/);
    assert.equal((await inputs.get('grid-2')).their, 'grid-2');
  });
});

const table = (columns: { name: string; values: unknown[] }[], rowCount = columns[0]?.values.length ?? 0): RawTable =>
  ({ columns, rowCount, epoch: 0, elapsedMs: 0 }) as unknown as RawTable;

describe('a join question\'s answer, read strictly', () => {
  it('a count over no rows is none; one missing its column, or its value, is refused', async () => {
    const mine = { _type: 'lambda', parameters: [], body: [] } as never;
    assert.deepEqual(await summaryCounts({ mine }, async () => table([{ name: TOTAL_ROWS_COLUMN, values: [] }], 0)), { mine: 0 });
    assert.deepEqual(await summaryCounts({ mine }, async () => table([{ name: TOTAL_ROWS_COLUMN, values: [5n] }])), { mine: 5 });
    await assert.rejects(summaryCounts({ mine }, async () => table([{ name: 'other', values: [5] }])), /answered no __rows__ \(it answered other\)/);
    await assert.rejects(summaryCounts({ mine }, async () => table([{ name: TOTAL_ROWS_COLUMN, values: [null] }])), /answered no value of __rows__/);
  });
});
