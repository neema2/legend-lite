// A model's test data in DuckDB (plan A2): the tables of a relational Data element, as legend-lite's model JSON gives
// them (party-model.json: grammarToJson/model of a Database and a Data element, written by the planner itself), made
// tables typed as the Database declares, and read back.

import assert from 'node:assert/strict';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { readFileSync } from 'node:fs';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { dataTables, loadDataTables } from '../src/model-data.ts';
import { engineClientRequire } from '../src/node-require.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

const model = JSON.parse(readFileSync(runfileFromEnv('PARTY_MODEL'), 'utf8')) as { elements: Parameters<typeof dataTables>[0] };

let engine: DuckDbEngine;
let db: { registerFileText(name: string, text: string): void; connect(): unknown; instantiate(): Promise<unknown> };

before(async () => {
  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
  db = await duckdb.createDuckDB(
    {
      mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
      eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate();
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
});

describe("a model's test data, in DuckDB", () => {
  it('reads each table of a relational Data element, typed as its Database declares', () => {
    assert.deepEqual(dataTables(model.elements), [{
      schema: 'PARTY',
      table: 'PARTY',
      columns: [{ name: 'ID', type: 'INTEGER' }, { name: 'NAME', type: 'VARCHAR(200)' }, { name: 'COUNTRY', type: 'VARCHAR(2)' }],
      csv: 'ID,NAME,COUNTRY\n1,Meridian Capital,US\n2,Halberd Securities,GB\n',
    }]);
  });

  it('loads them where the model says, and they read back typed', async () => {
    await loadDataTables({
      registerFileText: async (name, text) => db.registerFileText(name, text),
      run: (sql) => engine.run(sql, 0),
    }, dataTables(model.elements));
    const t = await engine.run('SELECT ID, NAME, COUNTRY FROM "PARTY"."PARTY" ORDER BY ID', 0);
    assert.deepEqual(t.columns.map((c) => c.name), ['ID', 'NAME', 'COUNTRY']);
    assert.equal(t.rowCount, 2);
    assert.deepEqual(t.columns.map((c) => c.values[1]), [2, 'Halberd Securities', 'GB']);
  });

  it('refuses a table no Database declares; DuckDB refuses a column type it does not know', async () => {
    assert.throws(() => dataTables(model.elements.filter((e) => e._type !== 'relational')), /which no Database of the model declares/);
    const odd = structuredClone(model.elements) as { _type: string; schemas?: { tables: { columns: { type: { _type: string } }[] }[] }[] }[];
    odd[0]!.schemas![0]!.tables[0]!.columns[0]!.type = { _type: 'Semistructured' };
    await assert.rejects(loadDataTables({
      registerFileText: async (name, text) => db.registerFileText(name, text),
      run: (sql) => engine.run(sql, 0),
    }, dataTables(odd as never)), /SEMISTRUCTURED/i);
  });
});
