import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { PlanError } from '../src/planner.ts';
import { fromElement, toJson } from '../../pure-protocol/src/index.ts';
import {
  PlannerUnavailableError,
  LitePlanner,
} from '../src/lite-planner.ts';
import type { PlannerWorker } from '../../engine-client/src/legend/planner-protocol.ts';

/** A module's OK answer: the SQL and its result type (here, typing nothing). */
function ok(sql: string): string {
  return `OK\n${JSON.stringify({ sql, type: { _type: 'relationType', columns: [] } })}`;
}

/**
 * A stand-in for the 4 MB module.
 *
 * These tests are about the SEAM -- how the answer string is parsed,
 * what is cached, which error type surfaces -- not about the planner,
 * which has its own 69-query differential against the JVM. Loading
 * the real module here would make a fast unit suite slow and would
 * test legend-lite twice.
 */
/** What the fake module's table of models held saw: each model held, each handle let go. */
interface HeldLog {
  readonly holds: string[];
  readonly releases: number[];
}

/** The module's exports, held models and all, answering a plan with `answer` over the model its handle names. */
function fakeExports(
  answer: (model: string, query: string, runtime: string) => string,
  onWarm?: (model: string) => void,
  log: HeldLog = { holds: [], releases: [] },
) {
  const models = new Map<number, string>();
  let next = 1;
  const over = (h: number, q: string, r: string): string =>
    (models.has(h) ? answer(models.get(h)!, q, r) : `ERR\n500\nno model is held under handle ${h}`);
  return {
    holdModelOrError: (m: string) => {
      onWarm?.(m);
      log.holds.push(m);
      if (m === 'does not compile') return 'ERR\nCOMPILATION\nthe model does not compile';
      const handle = next++;
      models.set(handle, m);
      return `OK\n${handle}`;
    },
    releaseModelOrError: (h: number) => { log.releases.push(h); models.delete(h); return 'OK\n'; },
    planHeldOrError: over,
    planJsonHeldOrError: over,
    executionPlanJsonHeldOrError: () => 'ERR\nfake\nnot in this fake',
    relationTypeHeldOrError: () => 'OK\n{"_type":"relationType","columns":[]}',
    tableModelOrError: () => 'ERR\nfake\nnot in this fake',
    catalogColumnsSqlOrError: () => 'ERR\nfake\nnot in this fake',
    pureV1OrError: () => 'ERR\nfake\nnot in this fake',
    testDataSqlOrError: () => 'ERR\nfake\nnot in this fake',
    sessionSetupOrError: () => 'ERR\nfake\nnot in this fake',
    projectDataOrError: () => 'ERR\nfake\nnot in this fake',
    hostOperationsOrError: () => 'ERR\nfake\nnot in this fake',
    planJsonOrError: () => 'ERR\nfake\nnot in this fake',
    executeJsonPlanOrError: () => 'ERR\nfake\nnot in this fake',
    executeJsonAnswerOrError: () => 'ERR\nfake\nnot in this fake',
    warmModelOrError: () => 'OK\n',
  };
}

function fakeRuntime(
  answer: (model: string, query: string, runtime: string) => string,
  onLoad?: () => void,
  onWarm?: (model: string) => void,
  log?: HeldLog,
) {
  return async () => ({
    async load() {
      onLoad?.();
      return { exports: fakeExports(answer, onWarm, log) };
    },
  });
}

function planner(
  answer: (model: string, query: string, runtime: string) => string,
  extra: {
    cache?: boolean;
    onLoad?: () => void;
    onWarm?: (model: string) => void;
    log?: HeldLog;
    model?: string;
  } = {},
) {
  return new LitePlanner({
    model: extra.model ?? '###Relational\nDatabase trades::DB ( Table T ( a VARCHAR(1) ) )',
    runtime: 'trades::RT',
    ...(extra.cache === undefined ? {} : { cache: extra.cache }),
    loadRuntime: fakeRuntime(answer, extra.onLoad, extra.onWarm, extra.log),
  });
}

describe('LitePlanner', () => {
  it('returns the SQL an OK answer carries', async () => {
    const p = planner(() => ok('SELECT t0.a FROM T AS t0'));
    assert.equal((await p.planText('grammar')).sql,
      'SELECT t0.a FROM T AS t0',
    );
  });

  it('carries the compiler\'s result type beside the SQL', async () => {
    const type = {
      _type: 'relationType',
      columns: [
        { name: 'region', genericType: { rawType: { _type: 'packageableType', fullPath: 'String' } } },
        { name: 'total', genericType: { rawType: { _type: 'packageableType', fullPath: 'Decimal' } } },
      ],
    };
    const p = planner(() => `OK\n${JSON.stringify({ sql: 'SELECT 1', type })}`);
    assert.deepEqual(await p.planText('g'), {
      sql: 'SELECT 1',
      columns: [{ name: 'region', type: 'String' }, { name: 'total', type: 'Decimal' }],
    });
  });

  it('passes the model and runtime through to the module', async () => {
    let seen: string[] = [];
    const p = planner((m, q, r) => {
      seen = [m, q, r];
      return ok('SELECT 1');
    });
    await p.planText('the-grammar');
    assert.match(seen[0]!, /Database trades::DB/);
    assert.equal(seen[1], 'the-grammar');
    assert.equal(seen[2], 'trades::RT');
  });

  it('keeps multi-line SQL intact', async () => {
    // The OK tag is stripped by finding the FIRST newline; every
    // newline after that belongs to the SQL.
    const sql = 'SELECT t0.a\nFROM T AS t0\nWHERE t0.a = 1';
    const p = planner(() => ok(sql));
    assert.equal((await p.planText('g')).sql, sql);
  });

  it('raises a PlanError carrying the compiler message on ERR', async () => {
    const p = planner(() =>
      'ERR\nCOMPILATION\n'
      + "unknown column 'nope'");
    await assert.rejects(() => p.planText('g2'), (e: unknown) => {
      assert.ok(e instanceof PlanError);
      // The refusal's KIND is dropped; the message is what a user
      // can act on, and it must match the HTTP planner's text.
      assert.equal(e.message, "unknown column 'nope'");
      assert.equal(e.subject, 'g2');
      return true;
    });
  });

  it('keeps a multi-line compiler message whole', async () => {
    const p = planner(() =>
      'ERR\nPARSER\n[1:40] expected expression,'
      + '\n  got end of input');
    await assert.rejects(() => p.planText('g'), (e: unknown) => {
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
    await assert.rejects(() => p.planText('g'));
    await assert.rejects(() => p.planText('g'));
    assert.equal(calls, 2);
    assert.equal(p.cacheSize, 0);
  });

  it('caches by grammar, and can be told not to', async () => {
    let calls = 0;
    const cached = planner(() => {
      calls++;
      return ok('SELECT 1');
    });
    await cached.planText('g');
    await cached.planText('g');
    assert.equal(calls, 1);
    assert.equal(cached.cacheSize, 1);

    calls = 0;
    const uncached = planner(() => {
      calls++;
      return ok('SELECT 1');
    }, { cache: false });
    await uncached.planText('g');
    await uncached.planText('g');
    assert.equal(calls, 2);
  });

  it('loads the module once however many plans race', async () => {
    // An initial render issues several plans at once. Memoising a
    // boolean rather than the promise would start several 4 MB
    // fetches; this pins that it does not.
    let loads = 0;
    const p = planner(() => ok('SELECT 1'), { onLoad: () => { loads++; } });
    await Promise.all([
      p.planText('a'),
      p.planText('b'),
      p.planText('c'),
    ]);
    assert.equal(loads, 1);
  });

  it('warmUp loads the module without planning', async () => {
    let loads = 0;
    let plans = 0;
    const p = planner(() => {
      plans++;
      return ok('SELECT 1');
    }, { onLoad: () => { loads++; } });
    await p.warmUp();
    assert.equal(loads, 1);
    assert.equal(plans, 0);
    await p.planText('g');
    assert.equal(loads, 1, 'warmUp must satisfy the later load');
  });

  it('warmUp BUILDS THE BOOT LAYER, not just the module', async () => {
    // The first version only loaded the module, and browser timings
    // showed why that is useless: instantiate is ~45ms and the ~1.1s
    // that makes a first plan slow is boot-layer construction, which
    // stayed on the critical path. warmUp must force that work, with
    // the real model, or it warms nothing.
    const warmed: string[] = [];
    const p = planner(() => ok('SELECT 1'),
      { onWarm: (m) => { warmed.push(m); } });
    await p.warmUp();
    assert.equal(warmed.length, 1, 'warmUp must hold the model');
    assert.match(warmed[0]!, /Database trades::DB/,
      'the module must hold the REAL model, so the graph it builds is'
      + ' the one the first plan wants');
    await p.planText('g');
    assert.equal(warmed.length, 1, 'the first plan uses the model warmUp held');
  });

  it('holds its model once, however many plans it makes, and lets it go when done', async () => {
    const log: HeldLog = { holds: [], releases: [] };
    const p = planner(() => ok('SELECT 1'), { cache: false, log });
    for (const q of ['a', 'b', 'c']) await p.planText(q);
    await p.plan(fromElement('trades::T').select(['a']).lambda());
    assert.equal(log.holds.length, 1, 'compiled once: every plan over the model held');
    p.dispose();
    await new Promise((done) => setTimeout(done, 0));
    assert.deepEqual(log.releases, [1], 'dispose lets the model go');
  });

  it('a model changed: the old one let go, the new one held on its first plan', async () => {
    const log: HeldLog = { holds: [], releases: [] };
    const models: string[] = [];
    const p = planner((m) => { models.push(m); return ok('SELECT 1'); }, { cache: false, log });
    await p.planText('a');
    p.useModel('another model', 'trades::RT');
    await p.planText('a');
    assert.deepEqual(models, [log.holds[0], 'another model'], 'each plan over the model the planner has now');
    assert.equal(log.holds.length, 2);
    assert.deepEqual(log.releases, [1]);
  });

  it('a model that does not compile is refused as a plan is, held by no one, and tried again next time', async () => {
    const log: HeldLog = { holds: [], releases: [] };
    const p = planner(() => ok('SELECT 1'), { cache: false, log, model: 'does not compile' });
    await assert.rejects(() => p.planText('a'), PlanError);
    await assert.rejects(() => p.planText('a'), PlanError);
    assert.equal(log.holds.length, 2, 'a refusal is not kept as the verdict');
  });

  it('honours an abort raised before the call', async () => {
    const ctl = new AbortController();
    const reason = new Error('superseded');
    ctl.abort(reason);
    let planned = false;
    const p = planner(() => {
      planned = true;
      return ok('SELECT 1');
    });
    await assert.rejects(
      () => p.planText('g', ctl.signal),
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
      return ok('SELECT 1');
    });
    await assert.rejects(
      () => p.planText('g', ctl.signal),
      (e: unknown) => e === reason,
    );
  });

  it('reports a missing runtime asset as unavailable, not a bad query',
    async () => {
      // PlanError means "your query is wrong" and is shown to the
      // user. A missing asset is an operational fault; conflating
      // them files every failed deploy as a bad query.
      const p = new LitePlanner({
        model: 'm',
        runtime: 'r',
        loadRuntime: () => Promise.reject(new Error('404')),
      });
      await assert.rejects(() => p.planText('g'), (e: unknown) => {
        assert.ok(e instanceof PlannerUnavailableError);
        assert.ok(!(e instanceof PlanError));
        assert.match(e.message, /bazel build \/\/datacube:site/);
        return true;
      });
    });

  it('lets a retry succeed after a transient load failure', async () => {
    let attempt = 0;
    const p = new LitePlanner({
      model: 'm',
      runtime: 'r',
      loadRuntime: async () => {
        attempt++;
        if (attempt === 1) throw new Error('network blip');
        return {
          async load() {
            return {
              exports: fakeExports(() => ok('SELECT 1')),
            };
          },
        };
      },
    });
    await assert.rejects(() => p.planText('g'), PlannerUnavailableError);
    assert.equal((await p.planText('g')).sql, 'SELECT 1');
  });

  it('plans a TREE through the JSON entry, sending the query\'s exact JSON, cached by it', async () => {
    const seen: string[] = [];
    const p = planner((_m, q) => {
      seen.push(q);
      return ok('SELECT 1');
    });
    const query = fromElement('trades::T').select(['a']).lambda();
    assert.equal((await p.plan(query)).sql, 'SELECT 1');
    await p.plan(fromElement('trades::T').select(['a']).lambda());
    assert.deepEqual(seen, [toJson(query)], 'an equal tree is the same query: planned once');
  });

  it('keeps the text entry and the tree entry apart in its cache', async () => {
    let calls = 0;
    const p = planner(() => {
      calls++;
      return ok('SELECT 1');
    });
    const query = fromElement('trades::T').select(['a']).lambda();
    await p.plan(query);
    await p.planText(toJson(query));
    assert.equal(calls, 2);
  });

  it('rejects an answer with neither tag rather than guessing', async () => {
    const p = planner(() => 'SELECT 1');
    await assert.rejects(() => p.planText('g'),
      PlannerUnavailableError);
  });
});

describe('LitePlanner off the main thread: the models its worker holds', () => {
  /**
   * A stand-in Worker answering as planner-worker.ts does, over a fake module of its own: each worker its own table of
   * models held, as a real worker's module is. Node has no Worker; the planner uses one when `Worker` exists.
   */
  class FakeWorker {
    static made: FakeWorker[] = [];
    readonly exports = fakeExports((_m) => ok('SELECT 1'), undefined, { holds: [], releases: [] });
    readonly holds: string[] = [];
    readonly releases: number[] = [];
    /** What it was told, in order: each release, and its end. */
    readonly told: string[] = [];
    terminated = false;
    onmessage: ((e: { data: unknown }) => void) | null = null;
    onerror: ((e: { message: string }) => void) | null = null;
    constructor() {
      FakeWorker.made.push(this);
    }
    postMessage(msg: Record<string, unknown>): void {
      const x = this.exports;
      const answer = msg['kind'] === 'hold' ? (this.holds.push(msg['model'] as string), x.holdModelOrError(msg['model'] as string))
        : msg['kind'] === 'release' ? (this.releases.push(msg['handle'] as number), this.told.push('release'), x.releaseModelOrError(msg['handle'] as number))
        : msg['kind'] === 'plan' ? x.planHeldOrError(msg['handle'] as number, msg['query'] as string, msg['runtime'] as string)
        : 'ERR\nfake\nnot in this fake';
      queueMicrotask(() => this.onmessage?.({ data: { id: msg['id'], ok: true, answer } }));
    }
    terminate(): void {
      this.terminated = true;
      this.told.push('terminate');
    }
    die(): void {
      this.onerror?.({ message: 'gone' });
    }
  }
  const g = globalThis as unknown as { Worker?: unknown };
  const workerPlanner = (): LitePlanner => new LitePlanner({ model: 'm', runtime: 'r', workerUrl: 'worker.js', cache: false });

  it('dispose lets its model go to the worker running, and starts none to do it', async () => {
    FakeWorker.made = [];
    g.Worker = FakeWorker;
    try {
      const p = workerPlanner();
      await p.planText('a');
      p.dispose();
      await new Promise((done) => setTimeout(done, 0));
      assert.equal(FakeWorker.made.length, 1, 'no worker started to let a model go');
      assert.equal(FakeWorker.made[0]!.terminated, true);
    } finally {
      delete g.Worker;
    }
  });

  it('a worker that dies takes its models with it: the next plan holds its model again, on the new worker', async () => {
    FakeWorker.made = [];
    g.Worker = FakeWorker;
    try {
      const p = workerPlanner();
      await p.planText('a');
      FakeWorker.made[0]!.die();
      assert.equal((await p.planText('b')).sql, 'SELECT 1', 'planned, no stale handle sent');
      assert.equal(FakeWorker.made.length, 2);
      assert.deepEqual(FakeWorker.made[1]!.holds, ['m'], 'held again on the new worker');
    } finally {
      delete g.Worker;
    }
  });

  it('a worker its host makes is used, with no Worker in the global scope: the same messages, answered as a tab answers them', async () => {
    FakeWorker.made = [];
    assert.equal(g.Worker, undefined, 'no Worker here, as in an embedded engine');
    const p = new LitePlanner({ model: 'm', runtime: 'r', worker: () => new FakeWorker() as unknown as PlannerWorker, cache: false });
    assert.equal((await p.planText('a')).sql, 'SELECT 1');
    assert.equal(FakeWorker.made.length, 1);
    assert.deepEqual(FakeWorker.made[0]!.holds, ['m'], 'its model held on the host\'s worker');
    p.dispose();
    await new Promise((done) => setTimeout(done, 0));
    assert.equal(FakeWorker.made[0]!.terminated, true, 'and let go with the planner');
  });

  // the Phase 6 review's 3: a host's worker (Python's) keeps the models it holds in the host, not in itself -- a typer's
  // hold of a 4 MB model outlived it, the release never sent once the worker was ended
  it('dispose lets its model go on a host\'s worker before it ends the worker', async () => {
    FakeWorker.made = [];
    const p = new LitePlanner({ model: 'm', runtime: 'r', worker: () => new FakeWorker() as unknown as PlannerWorker, cache: false });
    await p.planText('a');
    p.dispose();
    await new Promise((done) => setTimeout(done, 0));
    assert.deepEqual(FakeWorker.made[0]!.told, ['release', 'terminate'], 'its model let go, then the worker ended');
  });
});

describe('LitePlanner: a plan in flight while its model changes', () => {
  it('is planned against the model and runtime it began with, and not kept for the new model', async () => {
    const asked: string[] = [];
    const p = planner((m, q, r) => { asked.push(`${m}|${q}|${r}`); return ok('SELECT 1'); }, { model: 'old' });
    const first = p.planText('a');
    p.useModel('new', 'other::RT');
    await first;
    assert.deepEqual(asked, ['old|a|trades::RT'], 'the old model with its own runtime');
    await p.planText('a');
    assert.deepEqual(asked.at(-1), 'new|a|other::RT', 'the new model planned afresh, the old answer not kept for it');
  });
});

describe('LitePlanner.withModel: another source on the page, the same module', () => {
  it('plans over its own model and runtime, loading the module once', async () => {
    let loads = 0;
    const asked: string[] = [];
    const first = planner((m, _q, r) => { asked.push(`${m}|${r}`); return ok('SELECT 1'); }, { onLoad: () => { loads += 1; } });
    const second = first.withModel('model B', 'b::RT');
    await second.planText('g');
    await first.planText('g');
    assert.equal(loads, 1, 'one module for both');
    assert.deepEqual(asked.map((a) => a.split('|')[1]), ['b::RT', 'trades::RT']);
    assert.equal(asked[0]!.split('|')[0], 'model B');
  });

  it('a borrowed planner\'s dispose leaves its maker working', async () => {
    const first = planner(() => ok('SELECT 1'));
    first.withModel('model B', 'b::RT').dispose();
    assert.equal((await first.planText('g')).sql, 'SELECT 1');
  });
});
