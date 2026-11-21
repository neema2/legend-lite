// THE RUN FRAME (run-frame.ts, the witness, 2026-11-13): a planner entry asked inside a run of its own flow is refused by
// name; outside any run, after a run ends, and on another flow while one flow's run is open, it is not. The planner's
// in-thread answer and a worker's port are entries.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { currentRun, frameKept, inRun, sending } from '../src/run-frame.ts';
import { NOT_PLANNING, planning, readRunsWith } from '../src/plan-guard.ts';
import { DuckDbEngine, type ArrowishTable } from '../src/duckdb.ts';
import { WarehouseBinding, WarehouseEngine } from '../src/warehouse.ts';
import { openedInTab, synthesizedInTab } from '../src/doors/tab-duckdb.ts';
import { dataLoaded } from '../src/doors/project-data.ts';
import { PLAN_VERSION } from '../src/plan-runner.ts';
import type { PlannedSql } from '../src/planned-sql.ts';
import { unplanned, unplannedPlan } from './unplanned.ts';
import { answer, type PlannerModule } from '../src/legend/planner-answer.ts';
import { WorkerPort } from '../src/legend/wasm-grammar.ts';
import { HttpEngine, RoutedEngine } from '../src/legend/engine.ts';

const REFUSED = /the planner was asked '(?:plan|queryPlanHeld)' inside a run \(a test's run\): a run sends its plan's statements and plans nothing \(G4\)/;

/** A module whose exports answer a plan and let a model go. */
const released: number[] = [];
const module = { exports: { queryPlanHeldOrError: () => 'OK\nSELECT 1',
  releaseModelOrError: (handle: number) => { released.push(handle); return 'OK\n'; } } } as unknown as PlannerModule;

describe('the run frame', () => {
  it('is kept under Node: the witness is not blind here', () => {
    assert.equal(frameKept, true);
  });

  it("reads the run through run-frame.ts's reader, installed once: a second install is refused by name (MUST 1)", () => {
    // run-frame.ts, which opens a run, installed its reader as it loaded; the planner's side (plan-guard.ts, a record
    // the planner reaches) holds no way to open one, and no other module may put a blind reader in its place
    assert.throws(() => readRunsWith(() => undefined), /the run frame reader is installed once/);
  });

  it('refuses a planner entry inside a run, across its awaits, and not outside it or after it', async () => {
    planning('plan');
    await assert.rejects(inRun("a test's run", async () => {
      await new Promise((done) => setTimeout(done, 0));
      assert.equal(currentRun(), "a test's run");
      planning('plan');
    }), REFUSED);
    assert.equal(currentRun(), undefined, 'the run ended with it');
    planning('plan');
  });

  it('is the flow\'s: a second flow plans while the first flow\'s run waits', async () => {
    let open!: () => void;
    const gate = new Promise<void>((done) => { open = done; });
    const first = inRun("a test's run", async () => {
      await gate;
      planning('plan');
    });
    await new Promise((done) => setTimeout(done, 0));
    planning('plan');
    open();
    await assert.rejects(first, REFUSED);
  });

  it('lets a held model go inside a run: a release plans nothing (T6), and nothing else is let through', async () => {
    // the final audit of 9af16fbe58's T6: a release inside a run was refused, LitePlanner's let-go swallowed it, and
    // the model stayed held
    assert.deepEqual([...NOT_PLANNING], ['release']);
    await inRun("a test's run", async () => {
      planning('release');
      assert.equal(answer(module, { kind: 'release', handle: 7 }), 'OK\n');
    });
    assert.deepEqual(released, [7], 'the model was let go');
    await assert.rejects(inRun("a test's run", async () => answer(module, { kind: 'hold', model: 'm' })),
      /the planner was asked 'hold' inside a run/);
  });

  it('refuses a server engine\'s generatePlan inside a run, before anything is sent (T2)', async () => {
    const sent: string[] = [];
    const fetcher = (async (url: string) => {
      sent.push(String(url));
      return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
    }) as unknown as typeof fetch;
    const http = new HttpEngine('http://engine.invalid', fetcher);
    const routed = new RoutedEngine({} as never, http);
    const input = { function: {}, model: {}, runtime: {} } as never;
    for (const engine of [http, routed]) {
      await assert.rejects(inRun("a test's run", async () => engine.generatePlan(input)),
        /the planner was asked 'generatePlan' inside a run \(a test's run\)/);
    }
    assert.deepEqual(sent, [], 'nothing sent to the server inside the run');
  });

  it('refuses a server engine\'s typing inside a run, before anything is sent (the last list, T2\'s neighbours)', async () => {
    const sent: string[] = [];
    const fetcher = (async (url: string) => {
      sent.push(String(url));
      return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
    }) as unknown as typeof fetch;
    const http = new HttpEngine('http://engine.invalid', fetcher);
    const routed = new RoutedEngine({} as never, http);
    const model = { _type: 'text', code: '' } as never;
    const lambda = {} as never;
    for (const engine of [http, routed]) {
      const asks: [string, () => Promise<unknown>][] = [
        ['relationType', () => engine.relationType(model, lambda)],
        ['compile', () => engine.compile(model)],
        ['returnType', () => engine.returnType(model, lambda)],
      ];
      for (const [kind, ask] of asks) {
        await assert.rejects(inRun("a test's run", async () => ask()),
          new RegExp(`the planner was asked '${kind}' inside a run \\(a test's run\\)`), kind);
      }
    }
    assert.deepEqual(sent, [], 'nothing sent to the server inside the run');
  });

  it('a refused entry that promises a promise rejects it, never throws (the C4 final fix round\'s nit)', async () => {
    // each called bare, not inside an async callback: a throw would escape the call itself (until 2026-11-14 the
    // non-async methods threw where they promised a promise)
    const fetcher = (async () => new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } })) as
      unknown as typeof fetch;
    const http = new HttpEngine('http://engine.invalid', fetcher);
    const routed = new RoutedEngine({} as never, http);
    const model = { _type: 'text', code: '' } as never;
    const lambda = {} as never;
    const input = { function: {}, model: {}, runtime: {} } as never;
    const g = globalThis as unknown as { Worker?: unknown };
    g.Worker = class {
      onmessage: unknown = null;
      postMessage(): void {}
    };
    try {
      const port = new WorkerPort('worker.js', 'http://planner.invalid/');
      const calls: [string, () => Promise<unknown>][] = [
        ['HttpEngine.relationType', () => http.relationType(model, lambda)],
        ['HttpEngine.compile', () => http.compile(model)],
        ['HttpEngine.generatePlan', () => http.generatePlan(input)],
        ['RoutedEngine.relationType', () => routed.relationType(model, lambda)],
        ['RoutedEngine.compile', () => routed.compile(model)],
        ['RoutedEngine.returnType', () => routed.returnType(model, lambda)],
        ['RoutedEngine.generatePlan', () => routed.generatePlan(input)],
        ['WorkerPort.ask', () => port.ask({ kind: 'queryPlanHeld', handle: 1, request: '{}' })],
      ];
      await inRun("a test's run", async () => {
        for (const [name, call] of calls) {
          let promised: Promise<unknown> | undefined;
          assert.doesNotThrow(() => { promised = call(); }, `${name} threw where it promised a promise`);
          await assert.rejects(promised!, /inside a run \(a test's run\)/, name);
        }
      });
    } finally {
      delete g.Worker;
    }
  });

  it('refuses the planner\'s in-thread answer inside a run, and answers it outside one', async () => {
    const asked = { kind: 'queryPlanHeld', handle: 1, request: '{}' } as const;
    await assert.rejects(inRun("a test's run", async () => answer(module, asked)), REFUSED);
    assert.equal(answer(module, asked), 'OK\nSELECT 1');
  });

  it('refuses a worker\'s port inside a run, before the request is posted', async () => {
    const g = globalThis as unknown as { Worker?: unknown };
    const posted: unknown[] = [];
    g.Worker = class {
      onmessage: unknown = null;
      postMessage(m: unknown): void {
        posted.push(m);
      }
    };
    try {
      const port = new WorkerPort('worker.js', 'http://planner.invalid/');
      await assert.rejects(inRun("a test's run", async () => port.ask({ kind: 'queryPlanHeld', handle: 1, request: '{}' })),
        REFUSED);
      assert.deepEqual(posted, [], 'nothing posted to the worker');
      void port.ask({ kind: 'queryPlanHeld', handle: 1, request: '{}' });
      assert.equal(posted.length, 1, 'outside the run, posted');
    } finally {
      delete g.Worker;
    }
  });
});

// EVERY SEND ON NODE FRAMED (C5-T, 2026-11-15): the two choke points -- DuckDB's connection (duckdb.ts) and the
// warehouse's submit (warehouse.ts) -- refuse a statement sent outside a run, before anything reaches the database,
// and send it inside one. A browser and the headless V8 keep no frame (frameKept): the layers' rule holds the line
// there (//tools/deps:layers_alone).
describe('a statement is sent inside a run, on Node (the choke points, C5-T)', () => {
  const OUTSIDE = (where: string): RegExp => new RegExp(`${where} was sent a statement outside a run: a statement is sent by a run`);

  it('refuses a send outside a run by name, and lets one inside a run through', async () => {
    assert.throws(() => sending("DuckDB's connection"), OUTSIDE("DuckDB's connection"));
    await inRun("a test's run", async () => sending("DuckDB's connection"));
  });

  it("DuckDB's connection: a run, a stream, and the held connection's rows, table and Arrow import, refused outside a run before anything is sent", async () => {
    const sent: string[] = [];
    const empty: ArrowishTable = { numRows: 0, schema: { fields: [] }, getChildAt: () => null };
    const engine = new DuckDbEngine({
      query: (sql) => { sent.push(sql); return empty; },
      prepare: async (sql) => ({ query: async () => { sent.push(sql); return empty; } }),
      insertArrowFromIPCStream: async () => { sent.push('an Arrow import'); },
    });
    const refused = OUTSIDE("DuckDB's connection");
    await assert.rejects(engine.run(unplanned('SELECT 1'), 0), refused);
    await assert.rejects(engine.stream({ sql: unplanned('SELECT 2'), columns: [] }, 0, () => {}), refused);
    await assert.rejects(engine.exclusive((held) => held.rows(unplanned('SELECT 3'), [])), refused);
    // a promise refused as one: the held table rejects, never throws where a promise is promised (the C5-T audit's nit)
    let threw = false;
    await assert.rejects(engine.exclusive(async (held) => {
      let answered: Promise<unknown>;
      try {
        answered = held.table(unplanned('SELECT 4'));
      } catch (error) {
        threw = true;
        throw error;
      }
      return answered;
    }), refused);
    assert.equal(threw, false, 'the held table threw where a promise is promised');
    await assert.rejects(engine.exclusive((held) => held.insertArrow(new Uint8Array(), { table: 't' }, true)), refused);
    assert.deepEqual(sent, [], 'a statement refused is never sent');
    // inside a run, each is sent
    await inRun("a test's run", async () => {
      await engine.run(unplanned('SELECT 1'), 0);
      await engine.exclusive(async (held) => {
        await held.rows(unplanned('SELECT 3'), []);
        await held.table(unplanned('SELECT 4'));
        await held.insertArrow(new Uint8Array(), { table: 't' }, true);
      });
    });
    assert.deepEqual(sent, ['SELECT 1', 'SELECT 3', 'SELECT 4', 'an Arrow import']);
  });

  it("DuckDB's streaming connection -- Node's real path (send, #batches): a run and a stream refused outside a run, sent inside one (the C5-T final audit's item 5)", async () => {
    const sent: string[] = [];
    const empty: ArrowishTable = { numRows: 0, schema: { fields: [] }, getChildAt: () => null };
    const batches = (): AsyncIterable<ArrowishTable> => (async function* () { yield empty; })();
    const engine = new DuckDbEngine({
      query: (sql) => { sent.push(`query ${sql}`); return empty; },
      // DuckDB-WASM's Node connection streams: a run and a stream go by send, batch by batch (duckdb.ts #batches)
      send: async (sql) => { sent.push(`send ${sql}`); return batches(); },
      prepare: async (sql) => ({ query: async () => { sent.push(`prepared ${sql}`); return empty; } }),
    });
    const refused = OUTSIDE("DuckDB's connection");
    await assert.rejects(engine.run(unplanned('SELECT 1'), 0), refused);
    await assert.rejects(engine.stream({ sql: unplanned('SELECT 2'), columns: [] }, 0, () => {}), refused);
    assert.deepEqual(sent, [], 'a statement refused is never sent');
    await inRun("a test's run", async () => {
      await engine.run(unplanned('SELECT 1'), 0);
      await engine.stream({ sql: unplanned('SELECT 2'), columns: [] }, 0, () => {});
    });
    assert.deepEqual(sent, ['send SELECT 1', 'send SELECT 2'], 'each sent by send, inside the run');
  });

  it("the warehouse's submit: refused outside a run before the binding is asked for it, and asked inside one", async () => {
    const asked: string[] = [];
    const binding = new WarehouseBinding({ exports: {
      call: (op: string): string => { asked.push(op); throw new Error('the test stops at the call'); },
      read: (): string => '{}',
    } });
    // a session already expired: no refresh is scheduled (the token is a sentinel, never a credential)
    const engine = new WarehouseEngine(binding,
      { baseUrl: 'http://warehouse.invalid', token: 'sentinel-token', principal: 'tester', expiresAt: '2000-01-01T00:00:00Z' },
      'main', 'DuckDB');
    await assert.rejects(engine.run(unplanned('SELECT 1'), 0), OUTSIDE("the warehouse's submit"));
    assert.deepEqual(asked, [], 'a statement refused is never submitted');
    await assert.rejects(inRun("a test's run", () => engine.run(unplanned('SELECT 1'), 0)), /the test stops at the call/);
    assert.deepEqual(asked, ['submit']);
  });
});

// THE FRAME'S COST ON NODE (C5-T, 2026-11-15; docs/witness-cost-homework/README.md): Node 22's AsyncLocalStorage turns
// promise hooks on at its first run and keeps them, so once a test had entered a run every promise in its process paid
// for the frame; on Node's AsyncContextFrame (22's --experimental-async-context-frame, Node 24's default) none does.
// tools/js/defs.bzl sets the flag for every node_test, which this pins.
describe("the frame's cost on Node", () => {
  it('AsyncLocalStorage runs on AsyncContextFrame: no promise hooks, so a promise pays nothing for the frame', () => {
    const major = Number(process.versions.node.split('.')[0]);
    assert.ok(major >= 24 || process.execArgv.includes('--experimental-async-context-frame'),
      `Node ${process.versions.node} runs this test without --experimental-async-context-frame (execArgv: `
      + `${JSON.stringify(process.execArgv)}): every promise pays the run frame's hooks; node_test sets it (tools/js/defs.bzl)`);
  });
});

// THE TAB'S START OVERLAPS THE PLANNER'S (the C5-T audit's MUST 1, 2026-11-15): DuckDB's download and instantiate send no
// statement, so the door begins them at once, alongside the planner's asks -- which, on a page, wait for the planner's
// own start -- and connects with the setup answered inside a run once both are in. A page then waits for the slower of
// the planner's start and DuckDB's, never their sum (DataCube's boot.ts).
describe("the tab's DuckDB starts alongside the planner's asks", () => {
  it("instantiate begins before the planner answers; the asks are outside a run, the connection inside one", async () => {
    const order: string[] = [];
    let answer!: () => void;
    const plannerStarted = new Promise<void>((resolve) => { answer = resolve; });
    const statement = unplanned("SET TimeZone = 'UTC'");
    const planner = {
      hostOperations: async (databaseType: string): Promise<PlannedSql[][]> => {
        order.push(`asked hostOperations(${databaseType})${currentRun() === undefined ? '' : ' INSIDE A RUN'}`);
        await plannerStarted;
        return [[statement]];
      },
      sessionSetup: async (databaseType: string): Promise<readonly PlannedSql[]> => {
        order.push(`asked sessionSetup(${databaseType})${currentRun() === undefined ? '' : ' INSIDE A RUN'}`);
        return [statement];
      },
    };
    const opened = openedInTab(
      async () => { order.push('instantiate begun'); return 'the database'; },
      'https://site/vendor/duckdb-extensions',
      planner,
      async (database, setup) => {
        order.push(`connected ${database} in ${currentRun() ?? 'no run'} with ${setup.extensions.length}+${setup.session.length}`);
        return 'the tab';
      },
    );
    // the planner has not answered: DuckDB's instantiate is already under way
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(order, ['asked hostOperations(DuckDB)', 'instantiate begun']);
    answer();
    assert.equal(await opened, 'the tab');
    assert.deepEqual(order, ['asked hostOperations(DuckDB)', 'instantiate begun', 'asked sessionSetup(DuckDB)',
      "connected the database in the tab's DuckDB started with 1+1"]);
  });
});

// THE PAGES' TWO DOORS, WITNESSED (2026-11-15, the C5-T review's S2, as openedInTab is above): a project's Data rows
// loaded where an engine runs (Query's warehouse seed) and a table synthesized in the tab's DuckDB (DataCube's demo
// sample) -- each asks its planner outside any run, then sends the answer's statements inside one, on Node's frame.
describe("the pages' doors ask outside a run and send inside one", () => {
  /** A one-statement load, as a planner answers one (a ONCE load at the plan's root). */
  const loadPlan = unplannedPlan(JSON.stringify({ format: 'legend-lite-plan', version: PLAN_VERSION, raises: [], root: {
    _type: 'load', identity: "the test's rows", cadence: 'ONCE', steps: [{ _type: 'statement', sql: 'CREATE TABLE t (n INT)' }],
  } }));
  /** A DuckDB engine over a connection that records each statement it is sent, and the run it was sent in. */
  const recording = (order: string[]): DuckDbEngine => {
    const empty: ArrowishTable = { numRows: 0, schema: { fields: [] }, getChildAt: () => null };
    const sent = (how: string, sql: string): void => { order.push(`${how} ${sql} in ${currentRun() ?? 'no run'}`); };
    return new DuckDbEngine({
      query: (sql) => { sent('ran', sql); return empty; },
      prepare: async (sql) => ({ query: async () => { sent('held rows', sql); return empty; } }),
      insertArrowFromIPCStream: async () => { sent('imported', 'Arrow'); },
    });
  };
  const asked = (order: string[], what: string): void => {
    order.push(`asked ${what}${currentRun() === undefined ? '' : ` INSIDE A RUN (${currentRun()})`}`);
  };

  it("dataLoaded (Query's warehouse seed): projectData asked outside a run, the load's statement run inside one", async () => {
    const order: string[] = [];
    const engine = recording(order);
    await dataLoaded(engine, {
      projectData: async (model, databaseType) => {
        asked(order, `projectData(${model}, ${databaseType})`);
        return { name: 'p', open: [], use: [], close: [], load: loadPlan };
      },
    }, 'the model', "the test's seed");
    assert.deepEqual(order, ['asked projectData(the model, DuckDB)', 'ran CREATE TABLE t (n INT) in a load']);
    // a model with no Data rows is refused by name, nothing sent
    const none: string[] = [];
    await assert.rejects(dataLoaded(recording(none), { projectData: async () => ({ name: 'p', open: [], use: [], close: [] }) },
      'the model', "the test's seed"), /the test's seed holds no Data rows to load/);
    assert.deepEqual(none, []);
  });

  it("synthesizedInTab (DataCube's demo sample): synthesize asked outside a run, the plan's statement sent on the held connection inside one", async () => {
    const order: string[] = [];
    const engine = recording(order);
    await synthesizedInTab(engine, {
      synthesize: async (request) => {
        asked(order, `synthesize(${request.table})`);
        return loadPlan;
      },
    }, { databaseType: 'DuckDB', model: 'the model', database: 'trades::DB', schema: 'default', table: 'TRADES', count: 10 });
    // held.rows with no value bound sends by the connection's query, as engine.run does
    assert.deepEqual(order, ['asked synthesize(TRADES)', 'ran CREATE TABLE t (n INT) in a load']);
  });
});
