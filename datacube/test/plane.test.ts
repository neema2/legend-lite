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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { CubeController, type Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { SnapTarget } from '../src/snap.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { FakeEngine } from './fake-engine.ts';
import { fakeParse, fakePrint } from './fake-planner.ts';
import { toJson, type ColSpecArrayInstance, type Lambda } from '../../pure-protocol/src/index.ts';
import { accessor } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: accessor('trades::DB', 'TRADES') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
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

/**
 * Where these cubes snap: a table the model would declare, and the planner its copy is planned
 * with (the same model against the tab engine's runtime; a planner of its own here).
 */
const snapInto = (planner: Planner = new RecordingPlanner()): SnapTarget =>
  ({ table: 'TRADES_SNAP', source: accessor('trades::DB', 'TRADES_SNAP'), conversions: [], planner });

class RecordingEngine extends FakeEngine {
  readonly name = 'recording';
  readonly sql: string[] = [];
  /** The tables Arrow was loaded into (DuckDbEngine.loadArrow). */
  readonly loaded: string[] = [];
  async loadArrow(target: { readonly schema?: string; readonly table: string }, chunks: AsyncIterable<Uint8Array>): Promise<void> {
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
    const ran: CubeSnapshot = { ...SNAPSHOT, measures: [], columns: [...SNAPSHOT.columns, { name: 'trade date', type: 'StrictDate' }] };
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

describe('a snap is a copy: it holds the declared types', () => {
  it('reads the source through the copy conversions, where the rows are read', async () => {
    // A zoned timestamp is read in place under the source's UTC session; the tab's store has
    // no such session, so the copy holds its UTC wall time (leg B, docs/DATACUBE_APP_PLAN_2026_10_02.md)
    const engine = new RecordingEngine();
    const conversions = [{ column: 'at', sql: 'CAST(timezone(\'UTC\', "at") AS TIMESTAMP)' }];
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: { ...snapInto(), conversions } });
    await c.run(at(SNAPSHOT));
    engine.sql.length = 0;
    await c.snap(SNAPSHOT, 'test');
    const created = engine.sql.find((q) => /CREATE OR REPLACE TABLE "TRADES_SNAP"/.test(q));
    assert.ok(created, engine.sql.join(' ;; '));
    assert.match(created, /AS SELECT \* REPLACE \(CAST\(timezone\('UTC', "at"\) AS TIMESTAMP\) AS "at"\) FROM \(/);
  });

  it('copies the rows as they are when nothing needs converting', async () => {
    const engine = new RecordingEngine();
    const c = new CubeController(engine, new RecordingPlanner(), { snapTarget: snapInto() });
    await c.run(at(SNAPSHOT));
    engine.sql.length = 0;
    await c.snap(SNAPSHOT, 'test');
    assert.ok(engine.sql.some((q) => /CREATE OR REPLACE TABLE "TRADES_SNAP" AS /.test(q) && !q.includes('REPLACE (')),
      engine.sql.join(' ;; '));
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
    assert.ok(copy.pure.length > 0 && copy.pure.every((p) => p.includes('TRADES_SNAP')),
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
    assert.ok(copy.pure.length > 0 && copy.pure.every((p) => p.includes('TRADES_SNAP')), copy.pure.join(' ;; '));
    assert.equal(live.pure.length, 1, 'the live planner plans nothing on the copy');
    assert.ok(tab.sql.length > 0 && tab.sql.every((q) => /planned_/.test(q)), 'the copy\'s plans run in the tab');
  });

  it('converts the pulled copy in the tab, after the pull: the warehouse runs only the live plan', async () => {
    // A copy conversion is SQL in the catalog's (DuckDB's) terms; the pull may run in another
    // database's SQL (a Postgres catalog), so the tab rewrites what landed
    const tab = new RecordingEngine();
    const warehouse = new PullingEngine();
    const conversions = [{ column: 'at', sql: 'CAST(timezone(\'UTC\', "at") AS TIMESTAMP)' }];
    const c = new CubeController(tab, new RecordingPlanner(), { live: warehouse, snapTarget: { ...snapInto(), conversions } });
    await c.run(at(SNAPSHOT));
    tab.sql.length = 0;
    await c.snap(SNAPSHOT, 'test');
    assert.equal(warehouse.pulled.length, 1);
    assert.doesNotMatch(warehouse.pulled[0]!, /REPLACE|timezone/);
    assert.deepEqual(tab.loaded, ['TRADES_SNAP__pulled']);
    assert.ok(tab.sql.some((q) => q === 'CREATE OR REPLACE TABLE "TRADES_SNAP" AS SELECT * REPLACE (CAST(timezone(\'UTC\', "at") AS TIMESTAMP) AS "at") FROM "TRADES_SNAP__pulled"'),
      tab.sql.join(' ;; '));
    assert.ok(tab.sql.some((q) => q === 'DROP TABLE IF EXISTS "TRADES_SNAP__pulled"'), tab.sql.join(' ;; '));
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
        (p) => p.includes('"TRADES"') && !p.includes('TRADES_SNAP'),
      ),
      planner.pure.join(' ;; '),
    );
  });

  it('refuses to snap when the host names no table to snap into', async () => {
    // A generated table name was a bare SQL identifier: no compiler can
    // read it as a relation, so the snapped cube could not be planned.
    const c = new CubeController(new RecordingEngine(), new RecordingPlanner());
    const ran: CubeSnapshot = SNAPSHOT;
    await c.run(at(ran));
    await assert.rejects(() => c.snap(ran, 'test'), /names no table to freeze a snapshot into/);
    assert.equal(c.snaps.isSnapped, false);
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
    assert.ok(engine.sql.some((q) => /DROP TABLE IF EXISTS "TRADES_SNAP"/.test(q)), engine.sql.join(' ;; '));
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
