// THE PRODUCT'S DATA HOST, DRIVEN (the H7 floor's lane tests, the orchestrator's correction of 2026-11-13: the headless
// lane must provably run the product's bridge): each of DataCube's data asks -- a load, operations, a catalog, a table
// output -- made through the product's own DataHost, and a refusal of each. In Node, the tab's (data-host.ts
// heldDataHost, table-output.ts heldTableOutput, on DuckDB-WASM); in the headless lane, Python's (headless/host.ts
// hostData, hostTableOutput: each ask one of the product's intent calls, planned and run by Python), counted by the lane's
// runner against its floor (test/headless/run.py).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase, type TestDatabase } from '#test-host';
import { productDataHost } from '#test-data-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { sha256Hex } from '../../engine-client/src/sha256.ts';
import { lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
// the data host's own sources, imported here so this test's closure carries them (test_imports.bzl follows relative
// imports alone: #test-data-host's are not followed)
import type { DataHost } from '../src/data-host.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { writeTable } from '../src/table-output.ts';
import { catalogColumns } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

const CSV = 'id,name\n1,a\n2,b\n3,c\n';

describe("DataCube's data asks, made through the product's DataHost", () => {
  let db: TestDatabase;
  let engine: DuckDbEngine;
  let planner: LitePlanner;
  let data: ReturnType<typeof productDataHost> & { readonly host: DataHost };
  const count = async (table: string): Promise<number> => {
    const [rows] = await data.host.operations([{ op: 'count', from: { relation: { name: table } } }]);
    return Number(rows?.[0]?.[0]);
  };

  before(async () => {
    db = await openDatabase();
    engine = new DuckDbEngine(db.connection);
    planner = plannerFor('', 'local::RT');
    data = productDataHost(db, planner);
    for (const t of ['dh_in', 'dh_src', 'dh_out', 'dh_gone']) await engine.run(unplanned(`DROP TABLE IF EXISTS ${t}`), 0);
  });

  after(async () => {
    planner?.dispose();
    await engine?.close();
  });

  it('loads a file it holds by its handle, and refuses one the load does not hold', async () => {
    await db.files.registerFileText('dh_in.csv', CSV);
    const file = { name: 'dh_in.csv', digest: sha256Hex(CSV), size: new TextEncoder().encode(CSV).length };
    const handle = await data.handle('dh_in.csv');
    const request = { fileLoad: { databaseType: data.host.databaseType, file: { ...file, format: 'csv' as const }, into: { table: 'dh_in' } } };
    await data.host.load(request, { files: [{ ...file, handle }] });
    assert.equal(await count('dh_in'), 3);
    await assert.rejects(() => data.host.load({ fileLoad: { ...request.fileLoad, into: { table: 'dh_gone' } } },
      { files: [{ ...file, digest: '0'.repeat(64), handle }] }), /is not one this load holds/);
  });

  it("runs operations in order, and refuses one that carries SQL (the platform's words, on every host)", async () => {
    await engine.run(unplanned('CREATE TABLE dh_gone AS SELECT 1 AS n'), 0);
    assert.deepEqual(await data.host.operations([{ op: 'drop', kind: 'table', target: { name: 'dh_gone' } }]), [[]]);
    await assert.rejects(() => data.host.operations([{ op: 'count', from: { query: 'SELECT 1' } } as never]), /never handed to a host operation/);
  });

  it("reads a table's columns off the catalog, and answers none for a table it has not", async () => {
    await engine.run(unplanned('CREATE TABLE dh_src (id INTEGER, name VARCHAR)'), 0);
    await engine.run(unplanned("INSERT INTO dh_src VALUES (1, 'a'), (2, 'b')"), 0);
    assert.deepEqual((await data.host.catalog('main', 'dh_src')).map((c) => c.name), ['id', 'name']);
    assert.deepEqual(await data.host.catalog('main', 'dh_no_such_table'), []);
  });

  it("writes a box's rows as one output, and a refused create keeps nothing", async () => {
    const written = await planner.tablesModel([{ table: 'dh_src', convertible: true, columns: await catalogColumns(engine, planner, 'dh_src') }], 'DuckDB');
    const source: ValueSpecification = written.tables[0]!.source;
    const host = { databaseType: 'DuckDB', sources: [{ table: 'dh_src', convertible: true }], held: () => false, ...data.output };
    assert.equal(await writeTable(lambda([], source), { format: 'table', name: 'dh_out', mode: 'create' }, host), 2);
    await assert.rejects(() => writeTable(lambda([], source), { format: 'table', name: 'dh_out', mode: 'create' }, host));
    assert.equal(await count('dh_out'), 2);
  });
});
