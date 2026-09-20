import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { PlanError } from '../src/planner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import {
  PlannerUnavailableError,
  WasmPlanner,
} from '../src/wasm-planner.ts';

const SNAPSHOT = {
  source: { expression: '$trades' },
  columns: [],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
} satisfies CubeSnapshot;

/**
 * A stand-in for the 4 MB module.
 *
 * These tests are about the SEAM -- how the answer string is parsed,
 * what is cached, which error type surfaces -- not about the planner,
 * which has its own 69-query differential against the JVM. Loading
 * the real module here would make a fast unit suite slow and would
 * test legend-lite twice.
 */
function fakeRuntime(
  answer: (model: string, query: string, runtime: string) => string,
  onLoad?: () => void,
  onWarm?: (model: string) => void,
) {
  return async () => ({
    async load() {
      onLoad?.();
      return {
        exports: {
          planOrError: (m: string, q: string, r: string) => answer(m, q, r),
          warmModel: (m: string) => { onWarm?.(m); return 1; },
        },
      };
    },
  });
}

function planner(
  answer: (model: string, query: string, runtime: string) => string,
  extra: {
    cache?: boolean;
    onLoad?: () => void;
    onWarm?: (model: string) => void;
  } = {},
) {
  return new WasmPlanner({
    model: '###Relational\nDatabase trades::DB ( Table T ( a VARCHAR(1) ) )',
    runtime: 'trades::RT',
    ...(extra.cache === undefined ? {} : { cache: extra.cache }),
    loadRuntime: fakeRuntime(answer, extra.onLoad, extra.onWarm),
  });
}

describe('WasmPlanner', () => {
  it('returns the SQL an OK answer carries', async () => {
    const p = planner(() => 'OK\nSELECT t0.a FROM T AS t0');
    assert.equal(
      await p.plan('grammar', SNAPSHOT),
      'SELECT t0.a FROM T AS t0',
    );
  });

  it('passes the model and runtime through to the module', async () => {
    let seen: string[] = [];
    const p = planner((m, q, r) => {
      seen = [m, q, r];
      return 'OK\nSELECT 1';
    });
    await p.plan('the-grammar', SNAPSHOT);
    assert.match(seen[0]!, /Database trades::DB/);
    assert.equal(seen[1], 'the-grammar');
    assert.equal(seen[2], 'trades::RT');
  });

  it('keeps multi-line SQL intact', async () => {
    // The OK tag is stripped by finding the FIRST newline; every
    // newline after that belongs to the SQL.
    const sql = 'SELECT t0.a\nFROM T AS t0\nWHERE t0.a = 1';
    const p = planner(() => `OK\n${sql}`);
    assert.equal(await p.plan('g', SNAPSHOT), sql);
  });

  it('raises a PlanError carrying the compiler message on ERR', async () => {
    const p = planner(() =>
      'ERR\ncom.legend.compiler.spec.TypeInferenceException\n'
      + "unknown column 'nope'");
    await assert.rejects(() => p.plan('g2', SNAPSHOT), (e: unknown) => {
      assert.ok(e instanceof PlanError);
      // The exception CLASS is dropped; the message is what a user
      // can act on, and it must match the HTTP planner's text.
      assert.equal(e.message, "unknown column 'nope'");
      assert.equal(e.grammar, 'g2');
      return true;
    });
  });

  it('keeps a multi-line compiler message whole', async () => {
    const p = planner(() =>
      'ERR\ncom.legend.parser.ParseException\n[1:40] expected expression,'
      + '\n  got end of input');
    await assert.rejects(() => p.plan('g', SNAPSHOT), (e: unknown) => {
      assert.ok(e instanceof PlanError);
      assert.equal(
        e.message,
        '[1:40] expected expression,\n  got end of input',
      );
      return true;
    });
  });

  it('does not cache a refusal', async () => {
    // A refusal is about the query, not the module, so re-asking is
    // cheap and correct -- but caching it would also mean a cache
    // entry whose value is an exception, which the cache cannot hold.
    let calls = 0;
    const p = planner(() => {
      calls++;
      return 'ERR\nX\nnope';
    });
    await assert.rejects(() => p.plan('g', SNAPSHOT));
    await assert.rejects(() => p.plan('g', SNAPSHOT));
    assert.equal(calls, 2);
    assert.equal(p.cacheSize, 0);
  });

  it('caches by grammar, and can be told not to', async () => {
    let calls = 0;
    const cached = planner(() => {
      calls++;
      return 'OK\nSELECT 1';
    });
    await cached.plan('g', SNAPSHOT);
    await cached.plan('g', SNAPSHOT);
    assert.equal(calls, 1);
    assert.equal(cached.cacheSize, 1);

    calls = 0;
    const uncached = planner(() => {
      calls++;
      return 'OK\nSELECT 1';
    }, { cache: false });
    await uncached.plan('g', SNAPSHOT);
    await uncached.plan('g', SNAPSHOT);
    assert.equal(calls, 2);
  });

  it('loads the module once however many plans race', async () => {
    // An initial render issues several plans at once. Memoising a
    // boolean rather than the promise would start several 4 MB
    // fetches; this pins that it does not.
    let loads = 0;
    const p = planner(() => 'OK\nSELECT 1', { onLoad: () => { loads++; } });
    await Promise.all([
      p.plan('a', SNAPSHOT),
      p.plan('b', SNAPSHOT),
      p.plan('c', SNAPSHOT),
    ]);
    assert.equal(loads, 1);
  });

  it('warmUp loads the module without planning', async () => {
    let loads = 0;
    let plans = 0;
    const p = planner(() => {
      plans++;
      return 'OK\nSELECT 1';
    }, { onLoad: () => { loads++; } });
    await p.warmUp();
    assert.equal(loads, 1);
    assert.equal(plans, 0);
    await p.plan('g', SNAPSHOT);
    assert.equal(loads, 1, 'warmUp must satisfy the later load');
  });

  it('warmUp BUILDS THE BOOT LAYER, not just the module', async () => {
    // The first version only loaded the module, and browser timings
    // showed why that is useless: instantiate is ~45ms and the ~1.1s
    // that makes a first plan slow is boot-layer construction, which
    // stayed on the critical path. warmUp must force that work, with
    // the real model, or it warms nothing.
    const warmed: string[] = [];
    const p = planner(() => 'OK\nSELECT 1',
      { onWarm: (m) => { warmed.push(m); } });
    await p.warmUp();
    assert.equal(warmed.length, 1, 'warmUp must call warmModel');
    assert.match(warmed[0]!, /Database trades::DB/,
      'warmModel must get the REAL model, so the graph it builds is'
      + ' the one the first plan wants');
  });

  it('honours an abort raised before the call', async () => {
    const ctl = new AbortController();
    const reason = new Error('superseded');
    ctl.abort(reason);
    let planned = false;
    const p = planner(() => {
      planned = true;
      return 'OK\nSELECT 1';
    });
    await assert.rejects(
      () => p.plan('g', SNAPSHOT, undefined, ctl.signal),
      (e: unknown) => e === reason,
    );
    assert.equal(planned, false, 'an aborted plan must not do the work');
  });

  it('discards an answer aborted while planning', async () => {
    // The module is synchronous, so the abort cannot interrupt it --
    // but a stale answer must still not reach the grid.
    const ctl = new AbortController();
    const reason = new Error('superseded');
    const p = planner(() => {
      ctl.abort(reason);
      return 'OK\nSELECT 1';
    });
    await assert.rejects(
      () => p.plan('g', SNAPSHOT, undefined, ctl.signal),
      (e: unknown) => e === reason,
    );
  });

  it('reports a missing runtime asset as unavailable, not a bad query',
    async () => {
      // PlanError means "your query is wrong" and is shown to the
      // user. A missing asset is an operational fault; conflating
      // them files every failed deploy as a bad query.
      const p = new WasmPlanner({
        model: 'm',
        runtime: 'r',
        loadRuntime: () => Promise.reject(new Error('404')),
      });
      await assert.rejects(() => p.plan('g', SNAPSHOT), (e: unknown) => {
        assert.ok(e instanceof PlannerUnavailableError);
        assert.ok(!(e instanceof PlanError));
        assert.match(e.message, /planner:vendor/);
        return true;
      });
    });

  it('lets a retry succeed after a transient load failure', async () => {
    let attempt = 0;
    const p = new WasmPlanner({
      model: 'm',
      runtime: 'r',
      loadRuntime: async () => {
        attempt++;
        if (attempt === 1) throw new Error('network blip');
        return {
          async load() {
            return {
              exports: {
                planOrError: () => 'OK\nSELECT 1',
                warmModel: () => 1,
              },
            };
          },
        };
      },
    });
    await assert.rejects(() => p.plan('g', SNAPSHOT), PlannerUnavailableError);
    assert.equal(await p.plan('g', SNAPSHOT), 'SELECT 1');
  });

  it('rejects an answer with neither tag rather than guessing', async () => {
    const p = planner(() => 'SELECT 1');
    await assert.rejects(() => p.plan('g', SNAPSHOT),
      PlannerUnavailableError);
  });
});
