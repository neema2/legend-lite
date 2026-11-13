// THE TAB'S ENGINE HELD TO THE CONFORMANCE SUITE (src/engines/tab-engine.ts; engine-client/test/engine-conformance.ts,
// the engine contract's R7, step 2, 2026-11-13): the same asks Python headless answers (//python:contract_test), answered
// in-thread by the tab's planner and DuckDB-WASM -- every case passes. Its source is a table of the tab's database, its
// model the one the tab's planner writes, served to the suite as the contract hands it DataCube (R3a (c)).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { runConformance, type ConformanceFixture } from '../../engine-client/test/engine-conformance.ts';
import { tabEngine } from '../src/engines/tab-engine.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { catalogColumns } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

describe("the tab's engine, held to the engine contract's conformance suite", () => {
  let engine: DuckDbEngine;
  let planner: LitePlanner;

  before(async () => {
    const db = await openDatabase();
    engine = new DuckDbEngine(db.connection);
    planner = plannerFor('', 'local::RT');
    for (const t of ['conformance_t', 'conformance_out']) await engine.run(unplanned(`DROP TABLE IF EXISTS ${t}`), 0);
    await engine.run(unplanned('CREATE TABLE conformance_t (id BIGINT, name VARCHAR)'), 0);
    await engine.run(unplanned("INSERT INTO conformance_t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"), 0);
  });

  after(async () => {
    planner?.dispose();
    await engine?.close();
  });

  it('passes every case', async () => {
    const tab = tabEngine({ database: engine, planner, owns: (t) => t.toLowerCase() === 'conformance_t' });
    const written = await planner.tableModel(await catalogColumns(engine, planner, 'conformance_t'),
      { table: 'conformance_t', pkg: 'conformance_t', convertible: true, databaseType: 'DuckDB' });
    const { model, runtime } = await tab.served(written.model, written.runtime);
    assert.ok(!model.includes('###Connection') && !model.includes('###Runtime'), 'R3a (c): the handed model names its runtime alone');
    const fixture: ConformanceFixture = {
      model, runtime, source: written.source,
      columns: [{ name: 'id', type: 'Integer' }, { name: 'name', type: 'String' }],
      rowCount: 4, textColumn: 'name', keyColumn: 'id', ownTable: 'conformance_t', store: 'conformance_t::DB',
      writeTarget: 'conformance_out',
    };
    const results = await runConformance(tab, fixture);
    assert.ok(results.length >= 13, `${results.length} cases ran`);
    assert.deepEqual(results.filter((r) => !r.ok), []);
    const rows = await engine.exclusive((held) => held.rows(unplanned('SELECT id, name FROM conformance_out ORDER BY id'), []));
    assert.deepEqual(rows.map((r) => [Number(r[0]), r[1]]), [[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd']], 'the last write, merged by its key');
  });
});
