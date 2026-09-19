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
import type { QueryEngine } from '../src/engine.ts';
import type { ResultTable } from '../src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: '#>{trades::DB.TRADES}#' },
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

/** Records every Pure it is asked to plan, and every SQL it emits. */
class RecordingPlanner implements Planner {
  readonly pure: string[] = [];
  async plan(pureGrammar: string): Promise<string> {
    this.pure.push(pureGrammar);
    // Deliberately does NOT echo the Pure: a test here asserts that
    // no Pure reaches the engine, and a stub that pasted the
    // grammar into its own output would fail that for the wrong
    // reason.
    return `SELECT * FROM planned_${this.pure.length}`;
  }
}

class RecordingEngine implements QueryEngine {
  readonly name = 'recording';
  readonly sql: string[] = [];
  async execute(sql: string, epoch: number): Promise<ResultTable> {
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
  async close(): Promise<void> {}
}

describe('snapping goes through the planner', () => {
  it('plans a select rather than building SQL by hand', async () => {
    // It used to emit `SELECT "region", "notional" FROM <source>`,
    // which is only SQL when the source is a table name. With a
    // Pure accessor it produced `... FROM #>{trades::DB.TRADES}#`
    // and DuckDB rejected it.
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner);
    await c.update(SNAPSHOT);
    planner.pure.length = 0;

    await c.snap('test');

    // A select over the LIVE source, in Pure -- the columns being
    // whatever the snapshot actually references.
    assert.ok(
      planner.pure.some((p) =>
        /^#>\{trades::DB\.TRADES\}#->select\(~\[[^\]]+\]\)$/.test(p),
      ),
      `planner saw: ${planner.pure.join(' ;; ')}`,
    );
    assert.equal(
      engine.sql.some((q) => q.includes('#>{')),
      false,
      'no Pure may reach the engine as SQL',
    );
  });

  it('materialises what the planner returned', async () => {
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner);
    await c.update(SNAPSHOT);
    await c.snap('test');

    assert.ok(
      engine.sql.some(
        (q) => /CREATE OR REPLACE TABLE/i.test(q) && q.includes('planned_'),
      ),
      engine.sql.join(' ;; '),
    );
  });
});

describe('the snapped plane actually redirects', () => {
  it('reads the SNAP after snapping, not the live source', async () => {
    // `sourceFor` existed and nothing called it, so the snap was
    // cosmetic: a table was materialised and every later query
    // still went to the live source.
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner, {
      snapTarget: {
        table: 'TRADES_SNAP',
        expression: '#>{trades::DB.TRADES_SNAP}#',
      },
    });
    await c.update(SNAPSHOT);
    await c.snap('test');
    planner.pure.length = 0;

    await c.refresh();
    assert.ok(
      planner.pure.every((p) => p.includes('TRADES_SNAP')),
      `still reading live: ${planner.pure.join(' ;; ')}`,
    );
  });

  it('goes back to the live source on release', async () => {
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner, {
      snapTarget: {
        table: 'TRADES_SNAP',
        expression: '#>{trades::DB.TRADES_SNAP}#',
      },
    });
    await c.update(SNAPSHOT);
    await c.snap('test');
    await c.release();
    planner.pure.length = 0;

    await c.refresh();
    assert.ok(
      planner.pure.every(
        (p) => p.includes('TRADES}#') && !p.includes('TRADES_SNAP'),
      ),
      planner.pure.join(' ;; '),
    );
  });

  it('defaults to the generated table when no target is named', async () => {
    // The shim's source IS a SQL identifier, so a quoted generated
    // name is the right redirect there.
    const planner = new RecordingPlanner();
    const engine = new RecordingEngine();
    const c = new CubeController(engine, planner);
    await c.update({ ...SNAPSHOT, source: { expression: 'trades' } });
    await c.snap('test');
    planner.pure.length = 0;

    await c.refresh();
    assert.ok(
      planner.pure.every((p) => p.includes('"dc_snap_1"')),
      planner.pure.join(' ;; '),
    );
  });
});
