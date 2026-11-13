// A GRID'S RUNNER OVER THE ENGINE CONTRACT (src/engine-run.ts; the engine contract's step 4a): a grid's queries read
// through an EngineSession -- the tab's engine here -- answer what the tab's own plan-then-run answers: the same rows,
// the same compiler types; a query calling DataCube's Pure library sends it beside the model; a refusal is a
// QueryFailure carrying the engine's typed error.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { isEngineError } from '../../engine-client/src/engine-contract.ts';
import { colSpecs, fn, lambda } from '../../pure-protocol/src/index.ts';
import { EngineRun } from '../src/engine-run.ts';
import { localPlane, tabEngine } from '../src/engines/tab-engine.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { isQueryFailure, PlanThenRun } from '../src/runner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { catalogColumns } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

const SNAPSHOT = { epoch: 0 } as unknown as CubeSnapshot;

describe("a grid's runner over the engine contract", () => {
  let engine: DuckDbEngine;
  let planner: LitePlanner;
  let db: Awaited<ReturnType<typeof openDatabase>>;

  before(async () => {
    db = await openDatabase();
    engine = new DuckDbEngine(db.connection);
    planner = plannerFor('', 'local::RT');
    await engine.run(unplanned('DROP TABLE IF EXISTS er_t'), 0);
    await engine.run(unplanned('CREATE TABLE er_t (id BIGINT, name VARCHAR, amount DOUBLE)'), 0);
    await engine.run(unplanned("INSERT INTO er_t VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', NULL)"), 0);
  });

  after(async () => {
    planner?.dispose();
    await engine?.close();
  });

  it("answers the tab's own plan-then-run's rows and types, and says a refusal as a QueryFailure", async () => {
    const written = await planner.tableModel(await catalogColumns(engine, planner, 'er_t'),
      { table: 'er_t', pkg: 'er_t', convertible: true, databaseType: 'DuckDB' });
    const tab = tabEngine({ database: engine, plane: localPlane(planner), writer: planner, owns: () => false, files: db.files });
    const served = await tab.served(written.model, written.runtime);
    const session = await tab.open();
    const ours = new EngineRun(session, served, 'tab');
    const theirs = new PlanThenRun(planner.withModel(written.model, written.runtime), engine);
    const query = lambda([], written.source);
    const a = await ours.run(query, SNAPSHOT);
    const b = await theirs.run(query, SNAPSHOT);
    assert.deepEqual(a.rows.columns.map((c) => [c.name, c.type, c.values.map(String)]),
      b.rows.columns.map((c) => [c.name, c.type, c.values.map(String)]));
    assert.ok(a.sql.includes('SELECT'), 'the explain text, read-only');
    assert.deepEqual(await ours.relationType(query), await theirs.relationType(query));
    const chunks: number[] = [];
    await ours.stream(query, SNAPSHOT, (c) => chunks.push(c.rowCount));
    assert.equal(chunks.reduce((x, y) => x + y, 0), 3);
    await assert.rejects(() => ours.compile(lambda([], fn('select', written.source, colSpecs(['no_such']))), SNAPSHOT),
      (e: unknown) => isQueryFailure(e) && isEngineError((e as Error).cause) && ((e as Error).cause as { kind: string }).kind === 'compile');
    assert.equal((await ours.parse('|1 + 1'))._type, 'lambda');
    await session.close();
  });
});
