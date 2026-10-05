// A JSON file, loaded the way the picker loads it, against a real
// DuckDB. What is under test is the promise the model makes: a column
// declared SEMISTRUCTURED holds JSON, whatever shape it arrived in --
// the planner navigates it with JSON operators, which a STRUCT or a
// LIST does not take.

import assert from 'node:assert/strict';
import { engineClientRequire } from '../../engine-client/src/node-require.ts';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { DuckDbEngine, type ArrowishConnection } from '../../engine-client/src/duckdb.ts';
import { sampleById, sampleFileName } from '../src/samples.ts';
import { forgetUpload, ingestFile, type DuckDbFiles } from '../src/upload.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { plannerFor } from './catalog-builder.ts';
import {
  asc, col as column, derive, fn, from, lambda, lit, to, toMany, type, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';

let engine: DuckDbEngine;
let files: DuckDbFiles;
let copyOut: (name: string) => Uint8Array;

before(async () => {
  const duckdb = engineClientRequire('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(engineClientRequire.resolve('@duckdb/duckdb-wasm/blocking'));
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
  copyOut = (name) => db.copyFileToBuffer(name);
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
      // the model's declarations; the compiler types them (Variant for SEMISTRUCTURED)
      assert.match(r.model, /customer VARCHAR/);
      assert.match(r.model, /items SEMISTRUCTURED/);
      assert.match(r.model, /shipping SEMISTRUCTURED/);

      // The table keeps DuckDB's own nested storage -- no rewrite at ingest
      // (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md) ...
      const described = await engine.run(`DESCRIBE "${r.table}"`, 0);
      const names = described.columns.find((c) => c.name === 'column_name')!;
      const types = described.columns.find((c) => c.name === 'column_type')!;
      const duck = new Map(names.values.map((n, i) => [n, String(types.values[i])]));
      assert.match(duck.get('items')!, /^STRUCT\(.*\)\[\]$/);
      assert.match(duck.get('shipping')!, /^STRUCT\(/);

      // ... and the planner's own SQL reads it as a Variant: an index, a key, a whole value.
      const planner = plannerFor(r.model, r.runtime);
      const get = (v: ValueSpecification, key: string | number): ValueSpecification =>
        fn('get', v, typeof key === 'number' ? lit.integer(key) : lit.string(key));
      const plan = await planner.plan(from(r.source)
        .extend([
          derive('first_sku', lambda(['x'], to(get(get(column('x', 'items'), 0), 'sku'), type('String')))),
          derive('city', lambda(['x'], to(get(column('x', 'shipping'), 'city'), type('String')))),
        ])
        .select(['id', 'first_sku', 'city', 'shipping'])
        .sort([asc('id')])
        .lambda());
      const out = await engine.execute(plan, 0);
      const col = (n: string) => out.columns.find((c) => c.name === n)!;
      assert.deepEqual(col('first_sku').values, ['ABC', 'DEF']);
      assert.deepEqual(col('city').values, ['Leeds', 'Paris']);
      assert.equal(col('shipping').type, 'Variant');
      assert.deepEqual(col('shipping').values.map((v) => JSON.parse(String(v))),
        ORDERS.map((o) => o.shipping));
    });
  }

  it('converts a nested Parquet-style column too, not only JSON input', async () => {
    // A CSV cannot carry a LIST, so build the nested column the way
    // Parquet delivers one: typed, not text.
    await engine.run(
      `COPY (SELECT 1 AS id, [1, 2, 3] AS xs, {'a': 1} AS s)
         TO 'nested.json' (FORMAT JSON)`, 0);
    const text = String((await engine.run(
      `SELECT content FROM read_text('nested.json')`, 0)).columns[0]!.values[0]);
    const r = await ingestFile(engine, files, picked('nested.json', text));
    assert.match(r.model, /id BIGINT/);
    assert.match(r.model, /xs SEMISTRUCTURED/);
    assert.match(r.model, /s SEMISTRUCTURED/);
  });

  it('converts what the compiler says to, as it loads: exact, never truncated (S3b); keeps a type Pure cannot name as stored', async () => {
    // (No file carries a HUGEINT: Parquet has no 128-bit integer and DuckDB writes one as a
    // DOUBLE. Its declaration, DECIMAL(38,0), is pinned by core's CatalogModelTest.)
    await engine.run(
      `COPY (SELECT TIMESTAMPTZ '2024-01-02 03:04:05.123456+02' AS stamp,
                    18446744073709551615::UBIGINT AS big,
                    '4ac7a9e2-5b8f-4c1e-9a3d-2f6b8c0d1e7f'::UUID AS id,
                    TIME '13:14:15.678901' AS tod)
         TO 'zoned.parquet' (FORMAT PARQUET)`, 0);
    const bytes = copyOut('zoned.parquet');
    const r = await ingestFile(engine, files, {
      name: 'zoned.parquet',
      text: async () => '',
      arrayBuffer: async () => bytes.slice().buffer,
    });
    for (const [column, sql] of [['stamp', 'TIMESTAMP'], ['big', 'DECIMAL\\(20,0\\)'],
      ['id', 'OTHER'], ['tod', 'OTHER']] as const) {
      assert.match(r.model, new RegExp(`${column} ${sql}`), column);
    }
    const row = await engine.run(`SELECT CAST(stamp AS VARCHAR), CAST(big AS VARCHAR),
        id, tod, typeof(stamp), typeof(big), typeof(id) FROM "${r.table}"`, 0);
    assert.deepEqual(row.columns.map((c) => c.values[0]),
      ['2024-01-02 01:04:05.123456', '18446744073709551615',
        // a uuid and a time of day are declared OTHER and kept as stored: every query reads them as text
        '4ac7a9e2-5b8f-4c1e-9a3d-2f6b8c0d1e7f', '13:14:15.678901', 'TIMESTAMP', 'DECIMAL(20,0)', 'UUID']);
  });

  it('opens the offered orders sample with its nested fields as Variant', async () => {
    const sample = sampleById('orders-json')!;
    const r = await ingestFile(engine, files,
      picked(sampleFileName(sample), sample.build(200)));
    assert.equal(r.rowCount, 200);
    for (const [column, sql] of [['order_id', 'BIGINT'], ['region', 'VARCHAR'], ['placed_on', 'DATE'],
      ['customer', 'SEMISTRUCTURED'], ['items', 'SEMISTRUCTURED'], ['tags', 'SEMISTRUCTURED'],
      ['shipments', 'SEMISTRUCTURED'], ['total', 'DOUBLE']] as const) {
      assert.match(r.model, new RegExp(`${column} ${sql}`), column);
    }
  });

  it('reaches through the orders sample\'s nested arrays: in an object, in elements, in elements of elements', async () => {
    const sample = sampleById('orders-json')!;
    const text = sample.build(50);
    const r = await ingestFile(engine, files, picked(sampleFileName(sample), text));
    const planner = plannerFor(r.model, r.runtime);
    const get = (v: ValueSpecification, key: string | number): ValueSpecification =>
      fn('get', v, typeof key === 'number' ? lit.integer(key) : lit.string(key));
    const x = (c: string) => column('x', c);
    const plan = await planner.plan(from(r.source)
      .extend([
        derive('first_event', lambda(['x'], to(get(get(get(get(x('shipments'), 0), 'events'), 0), 'status'), type('String')))),
        derive('billing_city', lambda(['x'], to(get(get(get(x('customer'), 'addresses'), 0), 'city'), type('String')))),
        derive('first_sizes', lambda(['x'], fn('size', toMany(get(get(get(x('items'), 0), 'attributes'), 'sizes'), type('Variant'))))),
      ])
      .select(['order_id', 'first_event', 'billing_city', 'first_sizes'])
      .sort([asc('order_id')])
      .lambda());
    const out = await engine.execute(plan, 0);
    const col = (n: string) => out.columns.find((c) => c.name === n)!.values;
    const docs = text.trim().split('\n').map((l) => JSON.parse(l) as {
      shipments: { events: { status: string }[] }[];
      customer: { addresses: { city: string }[] };
      items: { attributes: { sizes: string[] } }[];
    });
    assert.deepEqual(col('first_event'), docs.map((d) => d.shipments[0]!.events[0]!.status));
    assert.deepEqual(col('billing_city'), docs.map((d) => d.customer.addresses[0]!.city));
    assert.deepEqual(col('first_sizes').map(Number), docs.map((d) => d.items[0]!.attributes.sizes.length));
  });
});

describe('an upload overtaken before it was used is forgotten', () => {
  it('drops its table and lets go of its file bytes', async () => {
    const sql: string[] = [];
    const dropped: string[] = [];
    const engine = { run: async (q: string) => { sql.push(q); return { columns: [], rowCount: 0 }; } } as unknown as QueryEngine;
    await forgetUpload(engine, {
      registerFileText: async () => {},
      registerFileBuffer: async () => {},
      dropFile: async (name) => { dropped.push(name); },
    }, 'big trades.csv');
    assert.deepEqual(sql, ['DROP TABLE IF EXISTS "big_trades"']);
    assert.deepEqual(dropped, ['upload_big_trades.csv']);
  });
});
