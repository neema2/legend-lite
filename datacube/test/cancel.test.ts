// Stale work STOPS, rather than finishing and being thrown away.
//
// Epoch discipline already guaranteed the right answer: a superseded
// result is discarded on arrival, so the grid never shows stale
// numbers. What it did not do was stop the work. Expanding five groups
// and collapsing them computed all five, and the user waited behind
// queries whose answers were already unwanted.
//
// These tests are about the difference between those two things, so
// they assert what was NOT done -- calls that never happened, requests
// that never went out -- which is the only way to tell a cancellation
// that works from one that is merely wired up.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import type { QueryEngine } from '../src/engine.ts';
import { EpochGuard, isStale, isSuperseded, Superseded } from '../src/epoch.ts';
import { LegendLitePlanner } from '../src/planner.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { fetchTree } from '../src/treeview.ts';
import { TreeState } from '../src/tree.ts';

const GROUPED: CubeSnapshot = {
  source: { expression: '#>{db.T}#' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'country', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region', 'country'],
  pivotOn: [],
  measures: [{ name: 'm', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

function rows(epoch: number, values: string[]): ResultTable {
  return {
    columns: [
      { name: 'region', type: 'String', values },
      { name: 'm', type: 'Float', values: values.map((_, i) => i + 1) },
    ],
    rowCount: values.length,
    epoch,
    elapsedMs: 0,
  };
}

describe('the epoch guard as a cancellation source', () => {
  it('aborts the previous epoch when a new one starts', () => {
    const guard = new EpochGuard();
    const first = guard.signal;
    assert.equal(first.aborted, false);
    guard.advance();
    assert.equal(first.aborted, true, 'the old signal must be aborted');
    assert.ok(isSuperseded(first.reason), `reason was ${first.reason}`);
    assert.equal(guard.signal.aborted, false, 'the new one is live');
  });

  it('hands a task the signal for ITS epoch, not a later one', async () => {
    // Reading `guard.signal` inside a task is a race: by the time the
    // line runs, a newer interaction may have swapped the controller,
    // and the task would then hold a signal that never aborts for it.
    const guard = new EpochGuard();
    let captured: AbortSignal | undefined;
    const started = guard.issue(async (_epoch, signal) => {
      captured = signal;
      await new Promise((r) => setTimeout(r, 20));
      return 'first';
    });
    await new Promise((r) => setTimeout(r, 5));
    const second = guard.issue(async () => 'second');

    assert.equal(await second, 'second');
    assert.ok(isStale(await started), 'the first is superseded');
    assert.equal(captured?.aborted, true, 'and its own signal aborted');
  });

  it('reports a superseded failure to telemetry, never to the caller', async () => {
    const seen: unknown[] = [];
    const guard = new EpochGuard({ onDiscardedError: (e) => seen.push(e) });
    const first = guard.issue(async (_e, signal) => {
      await new Promise((r) => setTimeout(r, 20));
      throw signal.reason ?? new Error('boom');
    });
    await new Promise((r) => setTimeout(r, 5));
    guard.advance();

    assert.ok(isStale(await first), 'the caller gets STALE, not a rejection');
    assert.equal(seen.length, 1);
    assert.ok(isSuperseded(seen[0]), 'and telemetry sees why');
  });
});

describe('a tree fetch that is already obsolete', () => {
  it('stops at the next level instead of walking the whole tree', async () => {
    // Levels are fetched in SEQUENCE, so the query in flight cannot be
    // recalled but every one behind it can be skipped. That is most of
    // the wasted work in a burst.
    const guard = new EpochGuard();
    const controller = new AbortController();
    let planned = 0;
    let executed = 0;

    const planner: Planner = {
      async plan() {
        planned += 1;
        // The first level lands; the interaction is superseded while
        // the caller is looking at it.
        if (planned === 1) controller.abort(new Superseded(1));
        if (controller.signal.aborted) throw controller.signal.reason;
        return `SELECT ${planned}`;
      },
    };
    const engine: QueryEngine = {
      name: 'stub',
      async execute(_sql, epoch, signal) {
        if (signal?.aborted) throw signal.reason ?? new Error('aborted');
        executed += 1;
        return rows(epoch, ['EMEA', 'AMER']);
      },
      async close() {},
    };

    const state = TreeState.fromPaths([['EMEA']]);
    await assert.rejects(
      () => fetchTree(GROUPED, state, {
        planner,
        engine,
        guard,
        epoch: guard.current,
        signal: controller.signal,
      }),
      (e: unknown) => isSuperseded(e),
    );

    // The abort landed during the FIRST plan, so nothing executed and
    // no later level was planned.
    assert.equal(executed, 0, 'no SQL ran after the abort');
    assert.equal(planned, 1, 'and no further level was planned');
  });
});

describe('the planner client', () => {
  it('passes the signal to fetch', async () => {
    let sawSignal: AbortSignal | undefined;
    const planner = new LegendLitePlanner({
      baseUrl: 'http://example',
      model: 'model',
      runtime: 'rt',
      cache: false,
      fetch: (async (_url: string, init?: RequestInit) => {
        sawSignal = init?.signal ?? undefined;
        return new Response(JSON.stringify({ sql: 'SELECT 1' }), {
          status: 200,
        });
      }) as unknown as typeof fetch,
    });
    await planner.plan('grammar', GROUPED);
    assert.ok(sawSignal === undefined, 'no signal passed, none forwarded');

    const ac = new AbortController();
    await planner.plan('grammar2', GROUPED, undefined, ac.signal);
    assert.equal(sawSignal, ac.signal, 'the caller signal reaches fetch');
  });

  it('reports an abort as superseded, not as an unreachable planner', async () => {
    // The distinction matters to telemetry: a responsive grid cancels
    // constantly, and reporting each one as "could not reach the
    // planner" turns normal behaviour into a fake outage.
    const ac = new AbortController();
    const planner = new LegendLitePlanner({
      baseUrl: 'http://example',
      model: 'model',
      runtime: 'rt',
      cache: false,
      fetch: (async () => {
        ac.abort(new Superseded(7));
        throw new Error('The operation was aborted');
      }) as unknown as typeof fetch,
    });

    await assert.rejects(
      () => planner.plan('grammar', GROUPED, undefined, ac.signal),
      (e: unknown) => isSuperseded(e),
    );
  });
});

describe('the controller under a burst', () => {
  it('abandons the first refresh when a second arrives', async () => {
    const planned: string[] = [];
    let release: (() => void) | undefined;
    const planner: Planner = {
      async plan(pure, _s, _scope, signal) {
        planned.push(pure);
        if (planned.length === 1) {
          // Hold the first interaction open until the second has run.
          await new Promise<void>((r) => { release = r; });
          if (signal?.aborted) throw signal.reason;
        }
        return `SELECT ${planned.length}`;
      },
    };
    const engine: QueryEngine = {
      name: 'stub',
      async execute(_sql, epoch, signal) {
        if (signal?.aborted) throw signal.reason ?? new Error('aborted');
        return rows(epoch, ['EMEA']);
      },
      async close() {},
    };

    const flat: CubeSnapshot = { ...GROUPED, rows: [] };
    const c = new CubeController(engine, planner);
    const first = c.update(flat);
    await new Promise((r) => setTimeout(r, 5));
    const second = c.update({ ...flat, epoch: 2 });
    release?.();

    const secondView = await second;
    assert.ok(!isStale(secondView), 'the newer interaction wins');
    assert.ok(isStale(await first), 'and the older one is stale, not an error');
  });
});
