// A JSON file, loaded the way the picker loads it, against a real
// DuckDB. What is under test is the promise the model makes: a column
// declared SEMISTRUCTURED holds JSON, whatever shape it arrived in --
// the planner navigates it with JSON operators, which a STRUCT or a
// LIST does not take.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { sampleById, sampleFileName } from '../src/samples.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';

let engine: DuckDbEngine;
let files: DuckDbFiles;

before(async () => {
  const require = createRequire(import.meta.url);
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB(
    {
      mvp: {
        mainModule: path.join(dist, 'duckdb-mvp.wasm'),
        mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs'),
      },
      eh: {
        mainModule: path.join(dist, 'duckdb-eh.wasm'),
        mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs'),
      },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME,
  );
  await db.instantiate();
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  // The blocking bindings register synchronously; the picker's handle
  // is the async one.
  files = {
    async registerFileText(name, text) { db.registerFileText(name, text); },
    async registerFileBuffer(name, buffer) { db.registerFileBuffer(name, buffer); },
  };
});

after(async () => {
  await engine?.close();
});

/** A picked file, as `ingestFile` reads one. */
function picked(name: string, text: string) {
  return {
    name,
    text: async () => text,
    arrayBuffer: async () => new TextEncoder().encode(text).buffer,
  };
}

const ORDERS = [
  { id: 1, customer: 'acme',
    items: [{ sku: 'ABC', qty: 2 }, { sku: 'XYZ', qty: 1 }],
    shipping: { city: 'Leeds', express: true } },
  { id: 2, customer: 'globex',
    items: [{ sku: 'DEF', qty: 3 }],
    shipping: { city: 'Paris', express: false } },
];

describe('ingestFile with JSON', () => {
  for (const [label, name, text] of [
    ['an array of records', 'orders.json', JSON.stringify(ORDERS)],
    ['newline-delimited records', 'orders.jsonl',
      ORDERS.map((o) => JSON.stringify(o)).join('\n')],
  ] as const) {
    it(`reads ${label}, nested fields as Variant`, async () => {
      const r = await ingestFile(engine, files, picked(name, text));

      assert.equal(r.rowCount, 2);
      const typeOf = new Map(r.columns.map((c) => [c.name, c.type]));
      assert.equal(typeOf.get('customer'), 'String');
      assert.equal(typeOf.get('items'), 'Variant');
      assert.equal(typeOf.get('shipping'), 'Variant');
      assert.match(r.model, /items SEMISTRUCTURED/);
      assert.match(r.model, /shipping SEMISTRUCTURED/);

      // The table holds what the model declares: JSON, not a STRUCT.
      const described = await engine.execute(`DESCRIBE "${r.table}"`, 0);
      const names = described.columns.find((c) => c.name === 'column_name')!;
      const types = described.columns.find((c) => c.name === 'column_type')!;
      const duck = new Map(names.values.map((n, i) => [n, types.values[i]]));
      assert.equal(duck.get('items'), 'JSON');
      assert.equal(duck.get('shipping'), 'JSON');

      // ... and the JSON operators the planner emits work on it.
      const skus = await engine.execute(
        `SELECT u ->> 'sku' AS sku FROM "${r.table}",
           UNNEST(CAST(items AS JSON[])) t(u) ORDER BY sku`, 0);
      assert.deepEqual(skus.columns[0]!.values, ['ABC', 'DEF', 'XYZ']);
    });
  }

  it('converts a nested Parquet-style column too, not only JSON input', async () => {
    // A CSV cannot carry a LIST, so build the nested column the way
    // Parquet delivers one: typed, not text.
    await engine.execute(
      `COPY (SELECT 1 AS id, [1, 2, 3] AS xs, {'a': 1} AS s)
         TO 'nested.json' (FORMAT JSON)`, 0);
    const text = String((await engine.execute(
      `SELECT content FROM read_text('nested.json')`, 0)).columns[0]!.values[0]);
    const r = await ingestFile(engine, files, picked('nested.json', text));
    const typeOf = new Map(r.columns.map((c) => [c.name, c.type]));
    assert.equal(typeOf.get('id'), 'Integer');
    assert.equal(typeOf.get('xs'), 'Variant');
    assert.equal(typeOf.get('s'), 'Variant');
  });

  it('opens the offered orders sample with its nested fields as Variant', async () => {
    const sample = sampleById('orders-json')!;
    const r = await ingestFile(engine, files,
      picked(sampleFileName(sample), sample.build(200)));
    assert.equal(r.rowCount, 200);
    assert.deepEqual(
      Object.fromEntries(r.columns.map((c) => [c.name, c.type])),
      { order_id: 'Integer', region: 'String', placed_on: 'StrictDate',
        customer: 'Variant', items: 'Variant', tags: 'Variant',
        total: 'Float' });
  });
});
