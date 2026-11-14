// A JSON file, loaded the way the picker loads it, against a real
// DuckDB. What is under test is the promise the model makes: a column
// declared SEMISTRUCTURED holds JSON, whatever shape it arrived in --
// the planner navigates it with JSON operators, which a STRUCT or a
// LIST does not take.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { sampleById, sampleFileName } from '../src/samples.ts';
import { gzipSync } from 'node:zlib';
import { fileBytes, fileParts, forgetUpload, formatOf, ingestFile, tableNameOf, type DuckDbFiles } from '../src/upload.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import { plannerFor } from './catalog-builder.ts';

import {
  asc, col as column, derive, fn, from, lambda, lit, to, toMany, type, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

// legend-lite's one model writer, in the module the tab loads (infer.ts `TableModels`)
const writer = plannerFor('', 'local::RT');

let engine: DuckDbEngine;
let files: DuckDbFiles;
/** A file the database wrote, read back by the database itself (read_blob): its bytes, on any host. */
let copyOut: (name: string) => Promise<Uint8Array>;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  copyOut = async (name) => {
    const read = await db.connection.query(unplanned(`SELECT content FROM read_blob('${name}')`));
    return read.getChildAt(0)!.get(0) as Uint8Array;
  };
  files = db.files;
});

after(async () => {
  await engine?.close();
});

/** A picked file, as `ingestFile` reads one. */
function picked(name: string, text: string): File {
  return new File([text], name);
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
      const r = await ingestFile(engine, files, picked(name, text), writer);

      assert.equal(r.rowCount, 2);
      // the model's declarations; the compiler types them (Variant for SEMISTRUCTURED)
      assert.match(r.model, /customer VARCHAR/);
      assert.match(r.model, /items SEMISTRUCTURED/);
      assert.match(r.model, /shipping SEMISTRUCTURED/);

      // The table keeps DuckDB's own nested storage -- no rewrite at ingest
      // (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md) ...
      const described = await engine.run(unplanned(`DESCRIBE "${r.table}"`), 0);
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
      unplanned(`COPY (SELECT 1 AS id, [1, 2, 3] AS xs, {'a': 1} AS s)
         TO 'nested.json' (FORMAT JSON)`), 0);
    const text = String((await engine.run(
      unplanned(`SELECT content FROM read_text('nested.json')`), 0)).columns[0]!.values[0]);
    const r = await ingestFile(engine, files, picked('nested.json', text), writer);
    assert.match(r.model, /id BIGINT/);
    assert.match(r.model, /xs SEMISTRUCTURED/);
    assert.match(r.model, /s SEMISTRUCTURED/);
  });

  it('converts what the compiler says to, as it loads: exact, never truncated (S3b); keeps a type Pure cannot name as stored', async () => {
    // (No file carries a HUGEINT: Parquet has no 128-bit integer and DuckDB writes one as a
    // DOUBLE. Its declaration, DECIMAL(38,0), is pinned by core's CatalogModelTest.)
    await engine.run(
      unplanned(`COPY (SELECT TIMESTAMPTZ '2024-01-02 03:04:05.123456+02' AS stamp,
                    18446744073709551615::UBIGINT AS big,
                    '4ac7a9e2-5b8f-4c1e-9a3d-2f6b8c0d1e7f'::UUID AS id,
                    TIME '13:14:15.678901' AS tod)
         TO 'zoned.parquet' (FORMAT PARQUET)`), 0);
    const bytes = await copyOut('zoned.parquet');
    const r = await ingestFile(engine, files, new File([bytes.slice()], 'zoned.parquet'), writer);
    for (const [column, sql] of [['stamp', 'TIMESTAMP'], ['big', 'DECIMAL\\(20,0\\)'],
      ['id', 'OTHER'], ['tod', 'OTHER']] as const) {
      assert.match(r.model, new RegExp(`${column} ${sql}`), column);
    }
    const row = await engine.run(unplanned(`SELECT CAST(stamp AS VARCHAR), CAST(big AS VARCHAR),
        id, tod, typeof(stamp), typeof(big), typeof(id) FROM "${r.table}"`), 0);
    assert.deepEqual(row.columns.map((c) => c.values[0]),
      ['2024-01-02 01:04:05.123456', '18446744073709551615',
        // a uuid and a time of day are declared OTHER and kept as stored: every query reads them as text
        '4ac7a9e2-5b8f-4c1e-9a3d-2f6b8c0d1e7f', '13:14:15.678901', 'TIMESTAMP', 'DECIMAL(20,0)', 'UUID']);
  });

  it('opens the offered orders sample with its nested fields as Variant', async () => {
    const sample = sampleById('orders-json')!;
    const r = await ingestFile(engine, files,
      picked(sampleFileName(sample), sample.build(200)), writer);
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
    const r = await ingestFile(engine, files, picked(sampleFileName(sample), text), writer);
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

describe('a text file read by its encoding: the bytes every host\'s DuckDB is given (fileParts, fileBytes; G4)', () => {
  /** A picked file of these bytes. */
  function pickedBytes(name: string, bytes: Uint8Array): File {
    return new File([bytes.slice()], name);
  }

  /** Every part `fileParts` hands over, at this size. */
  async function parts(file: File, size: number): Promise<number[][]> {
    const out: number[][] = [];
    for await (const part of fileParts(file, size)) out.push([...part]);
    return out;
  }

  it('a Latin-1 CSV opens with its letters: a file whose first bytes are not UTF-8 is read as Latin-1', async () => {
    // "name,city\nJosé,München\n" in Latin-1: é and ü are single bytes no UTF-8 reader takes
    const latin1 = Uint8Array.from([...'name,city\nJos'].map((c) => c.charCodeAt(0)).concat(
      [0xe9], [...',M'].map((c) => c.charCodeAt(0)), [0xfc], [...'nchen\n'].map((c) => c.charCodeAt(0))));
    const r = await ingestFile(engine, files, pickedBytes('latin1.csv', latin1), writer);
    assert.equal(r.rowCount, 1);
    const row = await engine.run(unplanned(`SELECT name, city FROM "${r.table}"`), 0);
    assert.deepEqual(row.columns.map((c) => c.values[0]), ['José', 'München']);
  });

  it('UTF-16 by its mark, either order; a gzipped file unpacked first, whatever is inside', async () => {
    const text = 'name,city\nJosé,München\n';
    const le = new Uint8Array([0xff, 0xfe, ...[...text].flatMap((c) => [c.charCodeAt(0) & 0xff, c.charCodeAt(0) >> 8])]);
    const be = new Uint8Array([0xfe, 0xff, ...[...text].flatMap((c) => [c.charCodeAt(0) >> 8, c.charCodeAt(0) & 0xff])]);
    for (const [name, bytes] of [['le.csv', le], ['be.csv', be], ['zipped.csv.gz', gzipSync(new TextEncoder().encode(text))],
      ['zipped16.csv.gz', gzipSync(le)]] as const) {
      assert.deepEqual([...await fileBytes(pickedBytes(name, bytes))], [...new TextEncoder().encode(text)], name);
    }
    const json = gzipSync(new TextEncoder().encode('[{"a": 1}, {"a": 2}]'));
    const r = await ingestFile(engine, files, pickedBytes('records.json.gz', json), writer);
    assert.equal(r.rowCount, 2);
    assert.equal(r.table, 'records');
    // a gzipped file's format and table are its name's within
    assert.deepEqual([formatOf('a.parquet.gz'), formatOf('b.jsonl.gz'), formatOf('c.csv.gz'), tableNameOf('trades.csv.gz')],
      ['parquet', 'json', 'csv', 'trades']);
  });

  it('a UTF-8 file with a stray byte stays UTF-8: its letters kept, the stray byte replaced, never read as Latin-1', async () => {
    const text = new Uint8Array([...new TextEncoder().encode('name\nJosé\n'), 0xe9, 0x0a]);
    assert.deepEqual([...await fileBytes(pickedBytes('mixed.csv', text))], [...new TextEncoder().encode('name\nJosé\n\uFFFD\n')]);
  });

  it('a character cut at the first 64 KB\'s end is not a fault of the file\'s: it stays UTF-8', async () => {
    const lead = new TextEncoder().encode('a\n' + 'x'.repeat(64 * 1024 - 3));
    const bytes = new Uint8Array([...lead, ...new TextEncoder().encode('é\n')]);
    assert.equal(lead.length, 64 * 1024 - 1, 'é begins on the sample\'s last byte');
    assert.deepEqual([...await fileBytes(pickedBytes('edge.csv', bytes))], [...bytes]);
  });

  it('UTF-8 by its first 64 KB: a Latin-1 byte after them is replaced, as a File\'s text() replaces it', async () => {
    const head = new TextEncoder().encode(`a\n${'x'.repeat(70 * 1024)}\n`);
    const tail = new Uint8Array([...new TextEncoder().encode('caf'), 0xe9, 0x0a]);
    const both = new Uint8Array([...head, ...tail]);
    const read = await fileBytes(pickedBytes('late.csv', both));
    assert.deepEqual([...read.subarray(head.length)], [...new TextEncoder().encode('caf\uFFFD\n')]);
  });

  it('a JSON array with a byte-order mark reads as its records\' columns', async () => {
    const bom = new Uint8Array([0xef, 0xbb, 0xbf, ...new TextEncoder().encode('[{"a": 1}, {"a": 2}]')]);
    const r = await ingestFile(engine, files, pickedBytes('bom.json', bom), writer);
    assert.equal(r.rowCount, 2);
    assert.match(r.model, /\ba INTEGER|\ba BIGINT/);
  });

  it('gives a Parquet file\'s bytes as they are, and a text file\'s as UTF-8 without its mark', async () => {
    const raw = new Uint8Array([0xef, 0xbb, 0xbf, 0x61, 0x0a]);
    assert.deepEqual([...await fileBytes(pickedBytes('x.parquet', raw))], [...raw]);
    assert.deepEqual([...await fileBytes(pickedBytes('x.csv', raw))], [0x61, 0x0a]);
  });

  it('hands the same bytes over a part at a time: each its size but the last, decoded as the whole file is', async () => {
    // a mark, a two-byte é split between two parts, a Latin-1 byte, and a part boundary inside a four-byte emoji
    const text = new Uint8Array([0xef, 0xbb, 0xbf, ...new TextEncoder().encode('a,b\nJos\u00e9,'), 0xfc,
      ...new TextEncoder().encode('\n\u{1F600}\n')]);
    const whole = [...await fileBytes(pickedBytes('t.csv', text))];
    // what a File's text() is, written out again: the tab's reading, on every host
    assert.deepEqual(whole, [...new TextEncoder().encode(await pickedBytes('t.csv', text).text())]);
    for (const size of [1, 2, 3, 5, 64]) {
      const got = await parts(pickedBytes('t.csv', text), size);
      assert.deepEqual(got.flat(), whole, `size ${size}`);
      assert.ok(got.slice(0, -1).every((p) => p.length === size) && got.at(-1)!.length <= size, `size ${size}`);
    }
    // Parquet as it is, a part at a time; an empty file, no part
    const raw = new Uint8Array([0xef, 0xbb, 0xbf, 1, 2, 3, 4]);
    assert.deepEqual(await parts(pickedBytes('x.parquet', raw), 3), [[0xef, 0xbb, 0xbf], [1, 2, 3], [4]]);
    assert.deepEqual(await parts(pickedBytes('e.csv', new Uint8Array(0)), 3), []);
  });
});

describe('an upload overtaken before it was used is forgotten', () => {
  it('drops its table and lets go of its file bytes', async () => {
    const sql: string[] = [];
    const dropped: string[] = [];
    const engine = { databaseType: 'DuckDB', run: async (q: string) => { sql.push(q); return { columns: [], rowCount: 0 }; } } as unknown as
      QueryEngine & { readonly databaseType: string };
    await forgetUpload(engine, writer, {
      registerFileText: async () => {},
      registerFileBuffer: async () => {},
      dropFile: async (name) => { dropped.push(name); },
    }, 'big trades.csv');
    assert.deepEqual(sql, ['DROP TABLE IF EXISTS "big_trades"']);
    assert.deepEqual(dropped, ['upload_big_trades.csv']);
  });
});
