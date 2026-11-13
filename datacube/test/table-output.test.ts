// AN OUTPUT INTO A TABLE (table-output.ts writeTable; docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): a box's rows written
// into a table of the host's database by the planner's write and the host's plan runner, in one transaction -- each way
// it writes (made, made again, appended, its rows deleted first, merged by a key), and what it refuses. Run on DuckDB-WASM
// and, in the headless lane, on Python's DuckDB through the host's calls (#test-host).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { fn, lambda, lit, property, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import type { TableOutput } from '../src/layout/flow.ts';
import { heldTableOutput, writeTable, type TableWriteHost } from '../src/table-output.ts';
import { catalogColumns } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

describe('a box written into a table of its host\'s database, in one transaction', () => {
  let engine: DuckDbEngine;
  let models: LitePlanner;
  let source: ValueSpecification;
  // the Output's ONE plan, asked of the planner before anything runs (Boundary.outputPlan, the final audit's item 2): the
  // run asks the planner nothing, so the planner each run holds is recorded and asked nothing more
  const asked: string[] = [];
  /** Whether the database is held for a write now: the planner asked meanwhile is the run asking it (refused here). */
  let holding = false;
  const host = (): TableWriteHost => ({
    databaseType: 'DuckDB',
    sources: [{ table: 'tw_src', convertible: true }],
    held: (t) => t === 'tw_src',
    ...heldTableOutput({
      databaseType: 'DuckDB',
      exclusive: async (work) => {
        holding = true;
        try {
          return await engine.exclusive(work);
        } finally {
          holding = false;
        }
      },
    }, {
      catalogColumnsSql: (schema, table) => models.catalogColumnsSql(schema, table),
      outputPlan: (request) => {
        asked.push(holding ? `${request.output.mode}, asked while the database was held` : request.output.mode);
        return models.outputPlan(request);
      },
    }),
  });
  /** The box's rows: the source's, or those `keep` holds for. */
  const rowsOf = (keep?: (x: ValueSpecification) => ValueSpecification): Lambda =>
    lambda([], keep === undefined ? source : fn('filter', source, lambda(['x'], keep(variable('x')))));
  const out = (name: string, mode: TableOutput['mode'], keys?: string[]): TableOutput => ({ format: 'table', name, mode, ...(keys ? { keys } : {}) });
  const rows = async (table: string): Promise<string> => {
    const t = await engine.exclusive((held) => held.rows(unplanned(`SELECT id, name, amount FROM ${table} ORDER BY id`), []));
    return t.map((r) => r.map(String).join(':')).join(' ');
  };

  before(async () => {
    const db = await openDatabase();
    engine = new DuckDbEngine(db.connection);
    models = plannerFor('', 'local::RT');
    for (const t of ['tw_src', 'tw_out', 'tw_none']) await engine.run(unplanned(`DROP TABLE IF EXISTS ${t}`), 0);
    await engine.run(unplanned('CREATE TABLE tw_src (id INTEGER, name VARCHAR, amount DOUBLE)'), 0);
    await engine.run(unplanned("INSERT INTO tw_src VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5)"), 0);
    const written = await models.tablesModel([{ table: 'tw_src', convertible: true, columns: await catalogColumns(engine, models, 'tw_src') }], 'DuckDB');
    source = written.tables[0]!.source;
  });

  after(async () => {
    models?.dispose();
    await engine?.close();
  });

  it('makes its table for its rows, then appends, deletes and appends, makes it again', async () => {
    assert.equal(await writeTable(rowsOf((x) => fn('lessThan', property(x, 'id'), lit.integer(3))), out('tw_out', 'create'), host()), 2);
    assert.equal(await rows('tw_out'), '1:a:1.5 2:b:2.5');
    // made already: the database's own words, and the table as it was
    await assert.rejects(() => writeTable(rowsOf(), out('tw_out', 'create'), host()), /already exists/i);
    assert.equal(await rows('tw_out'), '1:a:1.5 2:b:2.5');
    assert.equal(await writeTable(rowsOf((x) => fn('equal', property(x, 'id'), lit.integer(3))), out('tw_out', 'append'), host()), 1);
    assert.equal(await rows('tw_out'), '1:a:1.5 2:b:2.5 3:c:3.5');
    assert.equal(await writeTable(rowsOf((x) => fn('equal', property(x, 'id'), lit.integer(2))), out('tw_out', 'deleteAppend'), host()), 1);
    assert.equal(await rows('tw_out'), '2:b:2.5');
    assert.equal(await writeTable(rowsOf(), out('tw_out', 'overwrite'), host()), 3);
    assert.equal(await rows('tw_out'), '1:a:1.5 2:b:2.5 3:c:3.5');
    // each write one plan, asked once before it ran -- the made-already create's too, refused by the database
    assert.deepEqual(asked.splice(0), ['create', 'create', 'append', 'deleteAppend', 'overwrite']);
  });

  it('merges by its keys: a row of the same key updated, a new one added', async () => {
    await engine.run(unplanned("UPDATE tw_src SET amount = amount * 10 WHERE id = 2"), 0);
    await engine.run(unplanned("INSERT INTO tw_src VALUES (4, 'd', 4.5)"), 0);
    await engine.run(unplanned('DELETE FROM tw_out WHERE id >= 3'), 0);
    assert.equal(await writeTable(rowsOf(), out('tw_out', 'merge', ['id']), host()), 4);
    assert.equal(await rows('tw_out'), '1:a:1.5 2:b:25 3:c:3.5 4:d:4.5');
  });

  it('refuses in words a table of the page\'s own, and one an append has not', async () => {
    await assert.rejects(() => writeTable(rowsOf(), out('tw_src', 'overwrite'), host()), /one of this page's own tables/);
    await assert.rejects(() => writeTable(rowsOf(), out('tw_none', 'append'), host()), /has no table 'tw_none'/);
    const made = await engine.exclusive((held) => held.rows(unplanned("SELECT count(*) FROM information_schema.tables WHERE table_name = 'tw_none'"), []));
    assert.equal(Number(made[0]?.[0]), 0, 'nothing made');
  });
});
