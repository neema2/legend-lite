// The two bugs the first real end-to-end run found.
//
// Both were invisible to the whole unit suite, and both were
// invisible to the demo, because the demo's shim planner made the
// broken thing accidentally work: its source was the bare SQL
// identifier `trades`, so hand-built SQL parsed and a redirect that
// never happened changed nothing anyone could see.
//
// Against the real planner the source is `#>{trades::DB.TRADES}#`
// and both failures are immediate.

import { plannerFor } from './catalog-builder.ts';
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { SnapTarget } from '../src/cube.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { PlanThenRun, type QueryRunner } from '../src/runner.ts';
import type { HostFrozen } from '../../engine-client/src/snap.ts';
import { TreeState } from '../src/tree.ts';
import { FakeEngine } from './fake-engine.ts';
import { fakeParse, fakePrint } from './fake-planner.ts';
import { toJson, type ColSpecArrayInstance, type Lambda } from '../../pure-protocol/src/index.ts';
import { accessor, from } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: accessor('trades::DB', 'TRADES') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

function result(epoch: number, n = 2): ResultTable {
  return {
    columns: [
      { name: 'region', type: 'String', values: ['EMEA', 'AMER'].slice(0, n) },
      { name: 'notional', type: 'Float', values: [600, 400].slice(0, n) },
    ],
    rowCount: n,
    epoch,
    elapsedMs: 1,
  };
}

/** Records every query it is asked to plan (as its JSON), and every SQL it emits. */
class RecordingPlanner implements Planner {
  readonly pure: string[] = [];
  readonly queries: Lambda[] = [];
  async plan(query: Lambda): Promise<Plan> {
    this.queries.push(query);
    this.pure.push(toJson(query));
    // Deliberately does NOT echo the query: a test here asserts that
    // no Pure reaches the engine, and a stub that pasted the
    // query into its own output would fail that for the wrong
    // reason.
    return { sql: `SELECT * FROM planned_${this.pure.length}`, columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

/** A `source->select(~[...])` query's source (as the stand-in parse holds it) and columns. */
function selectOf(q: Lambda): { readonly source: unknown; readonly columns: readonly string[] } | undefined {
  const f = q.body[0];
  if (f?._type !== 'func' || f.function !== 'select') return undefined;
  const [source, columns] = f.parameters;
  return {
    source: source === undefined ? undefined : toJson(source),
    columns: (columns as ColSpecArrayInstance).value.colSpecs.map((c) => c.name),
  };
}

/** legend-lite's planner, which writes the copy's statements (D5): in-thread, or Python's library in the headless lane. */
const host = plannerFor('', '');

/** What a snap target made: each copy's table, and each let go. */
interface Copies { readonly tables: string[]; readonly disposed: string[] }

/**
 * Where these cubes snap (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): each snap a copy of its own in the tab
 * (`snap_1`, `snap_2`, ...), read once landed through the relation the app's model of it gives, and planned by the
 * planner the app made over that model (one of its own here).
 */
const snapInto = (planner: Planner = new RecordingPlanner(), made: Copies = { tables: [], disposed: [] }): SnapTarget => ({
  host,
  copy: () => {
    const table = `snap_${made.tables.length + 1}`;
    made.tables.push(table);
    return {
      table,
      landed: async () => ({ source: accessor('local::DB', table), planner }),
      dispose: () => { made.disposed.push(table); },
    };
  },
});

class RecordingEngine extends FakeEngine {
  readonly name = 'recording';
  readonly sql: string[] = [];
  /** The tables Arrow was loaded into (DuckDbEngine.loadArrow). */
  readonly loaded: string[] = [];
  async loadArrow(target: { readonly schema?: string; readonly table: string }, chunks: AsyncIterable<Uint8Array>,
    prepare: readonly string[] = []): Promise<void> {
    this.sql.push(...prepare);
    for await (const _ of chunks) void _;
    this.loaded.push(target.table);
  }
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    // `SELECT count(*)` is the snap preflight; it wants one number.
    if (/count\(\*\)/i.test(sql)) {
      return {
        columns: [{ name: 'n', type: 'Integer', values: [2] }],
        rowCount: 1,
        epoch,
        elapsedMs: 0,
      };
    }
    return result(epoch);
  }
}

/** A warehouse: counts, and streams the rows of whatever SQL it is asked to pull. */
class PullingEngine extends RecordingEngine {
  readonly pulled: string[] = [];
  async *arrowChunks(sql: string): AsyncIterable<Uint8Array> {
    this.pulled.push(sql);
    yield new Uint8Array([1]);
  }
}

/** A state to run: the snapshot, no groups open. */
const at = (snapshot: CubeSnapshot) => ({ snapshot, tree: TreeState.empty() });

describe('snapping goes through the planner', () => {
  it('plans a select rather than building SQL by hand', async () => {
    // It used to emit `SELECT "region", "notional" FROM <source>`,
    // which is only SQL when the source is a table name. With a
    // Pure accessor it produced `... FROM #>{trades::DB.TRADES}#`
    // and DuckDB rejected it.
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner, { snapTarget: snapInto() });
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    planner.pure.length = 0;
    planner.queries.length = 0;

    await c.snap(ran, 'test');

    // A select over the LIVE source, as a query -- the columns being
    // whatever the snapshot actually references.
    assert.ok(
      planner.queries.some((q) => {
        const s = selectOf(q);
        return s?.source === toJson(accessor('trades::DB', 'TRADES')) && s.columns.length > 0;
      }),
      `planner saw: ${planner.pure.join(' ;; ')}`,
    );
    assert.equal(
      engine.sql.some((q) => q.includes('#>{')),
      false,
      'no Pure may reach the engine as SQL',
    );
  });

  it('snaps EVERY source column, so any later view answers from the snap', async () => {
    const planner = new RecordingPlanner();
    const c = new CubeController(new RecordingEngine(), planner, { snapTarget: snapInto() });
    // a measure over one column: the other is still copied
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    planner.queries.length = 0;
    await c.snap(ran, 'test');
    assert.ok(planner.queries.some((q) => JSON.stringify(selectOf(q)) === JSON.stringify(
      { source: toJson(accessor('trades::DB', 'TRADES')), columns: ['region', 'notional'] })),
    `planner saw: ${planner.pure.join(' ;; ')}`);
  });

  it('snaps a FRESH cube, one that groups and measures nothing', async () => {
    // It used to select only the columns the view referenced -- none, for a
    // freshly opened table -- and the planner refused `select(~[])`.
    const planner = new RecordingPlanner();
    const c = new CubeController(new RecordingEngine(), planner, { snapTarget: snapInto() });
    const ran: CubeSnapshot = { ...SNAPSHOT, columns: [...SNAPSHOT.columns, { name: 'trade date', type: 'StrictDate' }] };
    await c.run(at(ran));
    planner.queries.length = 0;
    await c.snap(ran, 'test');
    assert.ok(planner.queries.some((q) => JSON.stringify(selectOf(q)) === JSON.stringify(
      { source: toJson(accessor('trades::DB', 'TRADES')), columns: ['region', 'notional', 'trade date'] })),
    `planner saw: ${planner.pure.join(' ;; ')}`);
    assert.equal(c.snaps.isSnapped, true);
  });

  it('materialises what the planner returned', async () => {
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner, { snapTarget: snapInto() });
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    await c.snap(ran, 'test');

    assert.ok(
      engine.sql.some(
        (q) => /CREATE OR REPLACE TABLE/i.test(q) && q.includes('planned_'),
      ),
      engine.sql.join(' ;; '),
    );
  });
});

describe('a snap is a copy of its own (§18.3)', () => {
  it('lands the pull as it is in a table of its own; the app makes it hold what its model declares, after', async () => {
    // a copy's conversions are the app's, once the rows are in (upload.ts modelLanded): the pull is never rewritten
    const engine = new RecordingEngine();
    let landedAfter: string[] | undefined;
    const target: SnapTarget = {
      host,
      copy: () => ({
        table: 'snap_1',
        landed: async () => {
          landedAfter = [...engine.sql];
          return { source: accessor('local::DB', 'snap_1'), planner: new RecordingPlanner() };
        },
        dispose: () => {},
      }),
    };
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: target });
    await c.run(at(SNAPSHOT));
    engine.sql.length = 0;
    await c.snap(SNAPSHOT, 'test');
    assert.ok(engine.sql.some((q) => /CREATE OR REPLACE TABLE "snap_1" AS /.test(q) && !q.includes('REPLACE (')), engine.sql.join(' ;; '));
    assert.ok(landedAfter?.some((q) => /"snap_1"/.test(q)), 'modelled once the rows are in');
  });

  it('two grids snap into two copies, and taking one live leaves the other its own', async () => {
    // the warehouse table's own name was every grid's copy: a grid and its Copy of Grid snapped into one table, and
    // taking one live dropped the table the other still read
    const made: Copies = { tables: [], disposed: [] };
    const engine = new RecordingEngine();
    const second = new RecordingPlanner();
    const one = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto(new RecordingPlanner(), made) });
    const two = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto(second, made) });
    await one.run(at(SNAPSHOT));
    await two.run(at(SNAPSHOT));
    await one.snap(SNAPSHOT, 'one');
    await two.snap(SNAPSHOT, 'two');
    assert.deepEqual(made.tables, ['snap_1', 'snap_2']);
    engine.sql.length = 0;
    await one.release();
    assert.ok(engine.sql.some((q) => /DROP TABLE IF EXISTS "snap_1"/.test(q)), engine.sql.join(' ;; '));
    assert.ok(!engine.sql.some((q) => /"snap_2"/.test(q)), 'the other grid\'s copy is left alone');
    assert.deepEqual(made.disposed, ['snap_1']);
    second.pure.length = 0;
    await two.run(at(SNAPSHOT));
    assert.ok(second.pure.length > 0 && second.pure.every((q) => q.includes('snap_2')), second.pure.join(' ;; '));
  });

  it('lets its copy go with the snap -- released, gone live, or the cube closed -- and drops one the app cannot model', async () => {
    const made: Copies = { tables: [], disposed: [] };
    const engine = new RecordingEngine();
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto(new RecordingPlanner(), made) });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'a');
    await c.goLive(async () => ({ kind: 'applied' }));
    await c.snap(SNAPSHOT, 'b');
    engine.sql.length = 0;
    await c.close();
    assert.ok(engine.sql.some((q) => /DROP TABLE IF EXISTS "snap_2"/.test(q)), 'a grid closed drops its copy');
    assert.deepEqual(made.disposed, ['snap_1', 'snap_2']);
    const refusing: SnapTarget = {
      host,
      copy: () => ({ table: 'snap_x', landed: async () => { throw new Error('no model of it'); }, dispose: () => { made.disposed.push('snap_x'); } }),
    };
    const d = new CubeController(engine, new RecordingPlanner(), { snapTarget: refusing });
    await d.run(at(SNAPSHOT));
    engine.sql.length = 0;
    await assert.rejects(d.snap(SNAPSHOT, 'x'), /no model of it/);
    assert.equal(d.snaps.isSnapped, false);
    assert.ok(engine.sql.some((q) => /DROP TABLE IF EXISTS "snap_x"/.test(q)), engine.sql.join(' ;; '));
    assert.equal(made.disposed.at(-1), 'snap_x');
  });
});

describe('the snapped plane actually redirects', () => {
  it('reads the SNAP after snapping, not the live source', async () => {
    // `sourceFor` existed and nothing called it, so the snap was
    // cosmetic: a table was materialised and every later query
    // still went to the live source.
    const planner = new RecordingPlanner();
    const copy = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner, { snapTarget: snapInto(copy) });
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    await c.snap(ran, 'test');
    planner.pure.length = 0;

    await c.run(at(ran));
    assert.ok(copy.pure.length > 0 && copy.pure.every((p) => p.includes('snap_1')),
      `still reading live: ${copy.pure.join(' ;; ')}`);
    assert.deepEqual(planner.pure, [], 'the live planner plans nothing on the copy');
  });

  it('pulls with the LIVE plan and plans the copy with the snap target\'s planner (leg C)', async () => {
    // A Postgres table: the pull runs on the warehouse, in Postgres SQL (the live runtime); the
    // copy is in the tab's DuckDB, and every query on it is planned against the tab's runtime.
    const live = new RecordingPlanner();
    const copy = new RecordingPlanner();
    const tab = new RecordingEngine();
    const warehouse = new PullingEngine();
    const c = new CubeController(tab, live, { live: warehouse, snapTarget: snapInto(copy) });
    await c.run(at(SNAPSHOT));
    live.queries.length = 0;
    live.pure.length = 0;
    await c.snap(SNAPSHOT, 'test');
    assert.equal(live.queries.length, 1, `the pull is planned once, live: ${live.pure.join(' ;; ')}`);
    assert.equal(selectOf(live.queries[0]!)?.source, toJson(accessor('trades::DB', 'TRADES')));
    assert.deepEqual(warehouse.pulled, ['SELECT * FROM planned_1'], 'the live plan is what the warehouse ran');
    assert.equal(copy.pure.length, 0, 'nothing is planned for the copy before it exists');
    tab.sql.length = 0;
    await c.run(at(SNAPSHOT));
    assert.ok(copy.pure.length > 0 && copy.pure.every((p) => p.includes('snap_1')), copy.pure.join(' ;; '));
    assert.equal(live.pure.length, 1, 'the live planner plans nothing on the copy');
    assert.ok(tab.sql.length > 0 && tab.sql.every((q) => /planned_/.test(q)), 'the copy\'s plans run in the tab');
  });

  it('lands the pull from the warehouse in the copy\'s own table: the warehouse runs only the live plan', async () => {
    const tab = new RecordingEngine();
    const warehouse = new PullingEngine();
    const c = new CubeController(tab, new RecordingPlanner(), { live: warehouse, snapTarget: snapInto() });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'test');
    assert.equal(warehouse.pulled.length, 1);
    assert.deepEqual(tab.loaded, ['snap_1']);
  });

  it('goes back to the live source on release', async () => {
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner, { snapTarget: snapInto() });
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    await c.snap(ran, 'test');
    await c.release();
    planner.pure.length = 0;

    await c.run(at(ran));
    assert.ok(
      planner.pure.every(
        (p) => p.includes('"TRADES"') && !p.includes('snap_1'),
      ),
      planner.pure.join(' ;; '),
    );
  });

  it('refuses to snap when the host has no store to snap into', async () => {
    const c = new CubeController(new RecordingEngine(), new RecordingPlanner());
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    await assert.rejects(() => c.snap(ran, 'test'), /has no store to freeze a snapshot into/);
    assert.equal(c.snaps.isSnapped, false);
  });
});

describe('a snapped grid\'s every query reads its copy (§18.3)', () => {
  // the copy has a model of its own, declaring the copy alone: a query a host builds from the grid's live base -- a
  // chart's, Ad Hoc mode's level, a drill-through -- read the live table on the copy's planner, which refused it
  const overLive = from(SNAPSHOT.source.query).select(['region']).lambda();
  const readsCopy = (planner: RecordingPlanner): void => {
    const last = planner.pure.at(-1) ?? '';
    assert.ok(last.includes('snap_1') && !last.includes('TRADES'), `the copy's planner was asked: ${last}`);
  };

  it('in the tab: a query over the live base, and a level, read the copy', async () => {
    const copy = new RecordingPlanner();
    const c = new CubeController(new RecordingEngine(), new RecordingPlanner(), { snapTarget: snapInto(copy) });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'test');
    await c.runQuery(overLive, SNAPSHOT);
    readsCopy(copy);
    await c.level(SNAPSHOT);
    readsCopy(copy);
  });

  it('on its host: the same, through the host\'s runner over its model of the copy', async () => {
    const copy = new RecordingPlanner();
    const c = new CubeController(new PlanThenRun(new RecordingPlanner(), new RecordingEngine()) as unknown as QueryRunner, {
      hostSnap: {
        store: 'Python',
        snap: async () => ({ rowCount: 2, table: 'snap_1', source: accessor('python::DB', 'snap_1'),
          runner: new PlanThenRun(copy, new RecordingEngine()) }),
        release: async () => {},
      },
    });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'test');
    await c.runQuery(overLive, SNAPSHOT);
    readsCopy(copy);
  });
});

describe('a snap being made (§18.3)', () => {
  /** A target whose copy lands only when `land` is called. */
  const gated = () => {
    let land: () => void = () => undefined;
    const gate = new Promise<void>((done) => { land = done; });
    const made: Copies = { tables: [], disposed: [] };
    const target: SnapTarget = {
      host,
      copy: () => {
        const table = `snap_${made.tables.length + 1}`;
        made.tables.push(table);
        return {
          table,
          landed: async () => { await gate; return { source: accessor('local::DB', table), planner: new RecordingPlanner() }; },
          dispose: () => { made.disposed.push(table); },
        };
      },
    };
    return { target, made, land: () => land() };
  };

  it('a grid closed while its copy is made lets the copy go as it lands', async () => {
    const g = gated();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: g.target });
    await c.run(at(SNAPSHOT));
    const snapping = c.snap(SNAPSHOT, 'test');
    await c.close();
    g.land();
    await assert.rejects(snapping, /closed while its snap was made/);
    assert.deepEqual(g.made.disposed, ['snap_1'], 'its model and its name let go');
    assert.ok(engine.sql.some((q) => /DROP TABLE.*snap_1/i.test(q)), `its table dropped: ${engine.sql.join(' ;; ')}`);
    assert.equal(c.snaps.isSnapped, false);
  });

  it('a second snap while one is made is refused, never a second copy', async () => {
    const g = gated();
    const c = new CubeController(new RecordingEngine(), new RecordingPlanner(), { snapTarget: g.target });
    await c.run(at(SNAPSHOT));
    const snapping = c.snap(SNAPSHOT, 'one');
    await assert.rejects(c.snap(SNAPSHOT, 'two'), /snap is being made/);
    g.land();
    await snapping;
    assert.deepEqual(g.made.tables, ['snap_1']);
    assert.equal(c.snaps.isSnapped, true);
  });
});

describe('a plane holds only once it has answered', () => {
  const refused = async () => ({ kind: 'refused', error: new Error('cannot reach the warehouse at http://wh') });
  const applied = async () => ({ kind: 'applied' });

  it('stays on the snap, its table kept, when live does not answer', async () => {
    // Going live dropped the snap first: a dead warehouse then left the dropped
    // copy's rows on screen under "Live" (2026-09-29).
    const engine = new RecordingEngine();
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto() });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'test');
    engine.sql.length = 0;
    await assert.rejects(c.goLive(refused), /could not go live — cannot reach the warehouse at http:\/\/wh\. Still on the snap/);
    assert.equal(c.snaps.isSnapped, true);
    assert.equal(engine.sql.some((q) => /DROP TABLE/i.test(q)), false, engine.sql.join(' ;; '));
  });

  it('drops the snap once live has answered', async () => {
    const engine = new RecordingEngine();
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto() });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'test');
    await c.goLive(applied);
    assert.equal(c.snaps.isSnapped, false);
    assert.ok(engine.sql.some((q) => /DROP TABLE IF EXISTS "snap_1"/.test(q)), engine.sql.join(' ;; '));
  });

  it('is live again when the snap it took cannot be read', async () => {
    // A file's model with no snap table: the badge said Snapped over a failed query.
    const engine = new RecordingEngine();
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto() });
    await c.run(at(SNAPSHOT));
    await assert.rejects(c.snapAndRun(SNAPSHOT, refused), /could not snap — .*Still live\./);
    assert.equal(c.snaps.isSnapped, false);
    assert.ok(engine.sql.some((q) => /DROP TABLE/i.test(q)), 'the unreadable snap is dropped');
  });
});

describe('a snap its host makes (Python\'s page: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3)', () => {
  const applied = async () => ({ kind: 'applied' });
  const refused = async () => ({ kind: 'refused', error: new Error('cannot reach the warehouse at http://wh') });
  /** A cube read through a runner, its host snapping: what it was sent, and how often it let a copy go. */
  const hosted = () => {
    const live = new RecordingPlanner();
    const copy = new RecordingPlanner();
    const pulls: Lambda[] = [];
    let released = 0;
    const runner = new PlanThenRun(copy, new RecordingEngine());
    const c = new CubeController(new PlanThenRun(live, new RecordingEngine()) as unknown as QueryRunner, {
      hostSnap: {
        store: 'Python',
        // a copy of its own, in a table Python names, read through Python's runner over its model of it
        snap: async (pull) => { pulls.push(pull); return { rowCount: 7, table: 'snap_1', source: accessor('python::DB', 'snap_1'), runner }; },
        release: async (table) => { assert.equal(table, 'snap_1'); released += 1; },
      },
    });
    return { c, live, copy, pulls, released: () => released };
  };

  it('sends the pull as Pure -- every source column -- and reads the copy through the host\'s runner', async () => {
    const h = hosted();
    await h.c.run(at(SNAPSHOT));
    await h.c.snap(SNAPSHOT, 'test');
    assert.deepEqual(h.pulls.map(selectOf), [{ source: toJson(SNAPSHOT.source.query), columns: ['region', 'notional'] }]);
    const state = h.c.snaps.state;
    assert.ok(state.mode === 'snapped' && state.snap.rowCount === 7 && state.snap.label === 'test', JSON.stringify(state));
    h.live.pure.length = 0;
    await h.c.run(at(SNAPSHOT));
    assert.ok(h.copy.pure.length > 0, 'the copy\'s runner answers while snapped');
    assert.deepEqual(h.live.pure, [], 'the live runner answers nothing on the copy');
  });

  it('lets its copy go when the cube closes, one being made too, once it is', async () => {
    type Made = HostFrozen & { readonly runner: QueryRunner };
    let finish: (v: Made) => void = () => undefined;
    let released = 0;
    const c = new CubeController(new PlanThenRun(new RecordingPlanner(), new RecordingEngine()) as unknown as QueryRunner, {
      hostSnap: {
        store: 'Python',
        snap: () => new Promise<Made>((done) => { finish = done; }),
        release: async () => { released += 1; },
      },
    });
    await c.run(at(SNAPSHOT));
    const snapping = c.snap(SNAPSHOT, 'test');
    await c.close();
    assert.equal(released, 0, 'nothing to let go before the copy is made');
    finish({ rowCount: 7, table: 'snap_1', source: accessor('python::DB', 'snap_1'), runner: new PlanThenRun(new RecordingPlanner(), new RecordingEngine()) as unknown as QueryRunner });
    await assert.rejects(snapping, /closed while its snap was made/);
    assert.equal(released, 1, 'the copy made after the cube closed is let go');
    assert.equal(c.snaps.isSnapped, false);
  });

  it('lets the copy go only once live has answered', async () => {
    const h = hosted();
    await h.c.run(at(SNAPSHOT));
    await h.c.snap(SNAPSHOT, 'test');
    await assert.rejects(h.c.goLive(refused), /could not go live — cannot reach the warehouse at http:\/\/wh\. Still on the snap/);
    assert.equal(h.c.snaps.isSnapped, true);
    assert.equal(h.released(), 0, 'the copy is kept while live does not answer');
    await h.c.goLive(applied);
    assert.equal(h.c.snaps.isSnapped, false);
    assert.equal(h.released(), 1);
  });
});

describe('a copy the page\'s other editor made (Python\'s grid.snap(): docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8)', () => {
  const applied = async () => ({ kind: 'applied' });
  const TAKEN = { table: 'snap_4', label: 'Snap 14:02', takenAt: new Date('2026-10-20T14:02:00Z'), rowCount: 9 };
  /** A cube read through a runner whose host holds a copy Python made: what it let go, and which copy it was asked for. */
  const following = (withHeld = true) => {
    const copy = new RecordingPlanner();
    const runner = new PlanThenRun(copy, new RecordingEngine()) as unknown as QueryRunner;
    const live = new RecordingPlanner();
    const released: string[] = [];
    const asked: string[] = [];
    const c = new CubeController(new PlanThenRun(live, new RecordingEngine()) as unknown as QueryRunner, {
      hostSnap: {
        store: 'Python',
        snap: async () => ({ rowCount: 3, table: 'snap_5', source: accessor('python::DB', 'snap_5'), runner }),
        release: async (table) => { released.push(table); },
        ...(withHeld ? {
          held: async (table: string) => { asked.push(table); return { table, source: accessor('python::DB', table), runner }; },
        } : {}),
      },
    });
    return { c, copy, live, released, asked };
  };

  it('reads the copy through the host\'s runner, as the page wrote it down', async () => {
    const f = following();
    await f.c.run(at(SNAPSHOT));
    await f.c.follow(TAKEN, SNAPSHOT.source.query);
    assert.deepEqual(f.asked, ['snap_4']);
    const state = f.c.snaps.state;
    assert.ok(state.mode === 'snapped' && state.snap.table === 'snap_4' && state.snap.rowCount === 9
      && state.snap.label === 'Snap 14:02' && state.snap.takenAt.getTime() === TAKEN.takenAt.getTime(), JSON.stringify(state));
    f.live.pure.length = 0;
    await f.c.run(at(SNAPSHOT));
    assert.ok(f.copy.pure.length > 0, 'the copy\'s runner answers');
    assert.deepEqual(f.live.pure, [], 'the live runner answers nothing on the copy');
  });

  it('whose copy it is is the host\'s to say (Z17): its Go Live, its close and its tab going let go of whichever it reads', async () => {
    // the host keeps one a page its keeper keeps reads (engine.py _release_copy): the grid keeps no book of whose it is
    const f = following();
    await f.c.follow(TAKEN, SNAPSHOT.source.query);
    await f.c.goLive(applied);
    assert.equal(f.c.snaps.isSnapped, false);
    assert.deepEqual(f.released, ['snap_4']);
    await f.c.follow(TAKEN, SNAPSHOT.source.query);
    await f.c.close();
    assert.deepEqual(f.released, ['snap_4', 'snap_4']);
    const own = following();
    await own.c.snap(SNAPSHOT, 'mine');
    own.c.leaving();
    const followed = following();
    await followed.c.follow(TAKEN, SNAPSHOT.source.query);
    followed.c.leaving();
    assert.deepEqual([own.released, followed.released], [['snap_5'], ['snap_4']]);
  });

  it('the other editor\'s live command: live again, nothing let go here (the page no longer reads it: its host lets it go)', async () => {
    const f = following();
    await f.c.follow(TAKEN, SNAPSHOT.source.query);
    f.c.forget();
    assert.equal(f.c.snaps.isSnapped, false);
    assert.deepEqual(f.released, []);
    await f.c.snap(SNAPSHOT, 'mine');
    await f.c.close();
    assert.deepEqual(f.released, ['snap_5']);
  });

  it('a host whose copies only this page makes refuses another editor\'s, in words', async () => {
    const f = following(false);
    await assert.rejects(f.c.follow(TAKEN, SNAPSHOT.source.query), /reads no copy its host makes/);
    assert.equal(f.c.snaps.isSnapped, false);
  });
});

describe('receipts', () => {
  /** An engine that signs what it runs, as a server would. */
  class SigningEngine extends RecordingEngine {
    #n = 0;
    override async answer(sql: string, epoch: number): Promise<ResultTable> {
      const r = await super.answer(sql, epoch);
      return { ...r, receipt: { plane: 'warehouse', where: 'the warehouse at wh:1', as: 'rita', statementId: `s${++this.#n}` } };
    }
  }

  it('the view carries the receipt of every query that answered it', async () => {
    const c = new CubeController(new SigningEngine(), new RecordingPlanner(), { snapTarget: snapInto() });
    const view = await c.run(at(SNAPSHOT));
    assert.ok(typeof view !== 'symbol');
    assert.ok(view.receipts.length > 0);
    assert.deepEqual(view.receipts.map((r) => r.as), view.receipts.map(() => 'rita'));
    assert.match(view.receipts.at(-1)?.statementId ?? '', /^s\d+$/);
  });

  it('while snapped, each receipt names the copy it read', async () => {
    const c = new CubeController(new SigningEngine(), new RecordingPlanner(), { snapTarget: snapInto() });
    await c.run(at(SNAPSHOT));
    await c.snap(SNAPSHOT, 'test');
    const view = await c.run(at(SNAPSHOT));
    assert.ok(typeof view !== 'symbol');
    const taken = c.snaps.state.mode === 'snapped' ? c.snaps.state.snap.takenAt : undefined;
    assert.ok(view.receipts.length > 0 && view.receipts.every((r) => r.copy?.takenAt === taken));
  });

  it('a result with no receipt adds none: nothing is made up for an engine that says nothing', async () => {
    const c = new CubeController(new RecordingEngine(), new RecordingPlanner(), { snapTarget: snapInto() });
    const view = await c.run(at(SNAPSHOT));
    assert.ok(typeof view !== 'symbol');
    assert.deepEqual(view.receipts, []);
  });
});

describe('a query a host asked for itself', () => {
  it('reports its failure to the host (a chart has no other way to say the sign-in expired)', async () => {
    class Failing extends RecordingEngine {
      override async answer(sql: string, epoch: number): Promise<ResultTable> {
        if (sql.includes('planned_')) throw new Error('the warehouse session for rita has expired');
        return super.answer(sql, epoch);
      }
    }
    const seen: unknown[] = [];
    const c = new CubeController(new Failing(), new RecordingPlanner(), { onQueryFailure: (e) => seen.push(e) });
    await assert.rejects(c.runQuery(SNAPSHOT.source.query as never, SNAPSHOT), /expired/);
    assert.equal(seen.length, 1);
    const stopped = new AbortController();
    stopped.abort();
    await assert.rejects(c.runQuery(SNAPSHOT.source.query as never, SNAPSHOT, undefined, stopped.signal));
    assert.equal(seen.length, 1, 'a query the host cancelled is not a failure to report');
  });
});
