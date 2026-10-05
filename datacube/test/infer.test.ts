import assert from 'node:assert/strict';
import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import { describe, it } from 'node:test';

import { inferModel } from '../src/infer.ts';
import type { CatalogColumn } from '../src/catalog-model.ts';
import { formatOf, tableNameOf } from '../src/upload.ts';
import { agg, col, derive, fn, from, lambda, lit, times, variable } from '../../pure-protocol/src/index.ts';
import { print } from './lite-compiler.ts';
import { plannerFor } from './catalog-builder.ts';

describe('tableNameOf', () => {
  it('derives an identifier from a filename', () => {
    assert.equal(tableNameOf('trades.csv'), 'trades');
    assert.equal(tableNameOf('my trades (2024).parquet'), 'my_trades_2024');
  });

  it('never starts with a digit', () => {
    assert.equal(tableNameOf('2024.csv'), 't_2024');
  });

  it('always yields something', () => {
    assert.equal(tableNameOf('...'), 'data');
    assert.equal(tableNameOf('!!!.csv'), 'data');
  });
});

describe('formatOf', () => {
  it('reads the extension, defaulting to csv', () => {
    assert.equal(formatOf('a.parquet'), 'parquet');
    assert.equal(formatOf('A.PARQUET'), 'parquet');
    assert.equal(formatOf('a.csv'), 'csv');
    assert.equal(formatOf('a.txt'), 'csv');
    assert.equal(formatOf('a.json'), 'json');
    assert.equal(formatOf('events.JSONL'), 'json');
    assert.equal(formatOf('a.ndjson'), 'json');
  });
});

// Each column's declared type is legend-lite's reading of DuckDB's catalog (CatalogModelTest, every
// DuckDB type; catalog-model.test.ts, the TypeScript writer against it; typed-values.ts through the
// real app); what is pinned here is the model around it.

/** A catalog column of a canonical type (its own name the same, unless given). */
function col_(name: string, logicalType: string, dataType = logicalType): CatalogColumn {
  return { name, dataType, logicalType, precision: null, scale: null, notNull: false };
}

describe('inferModel', () => {
  const described = [
    col_('region', 'VARCHAR'),
    col_('year', 'BIGINT'),
    col_('notional', 'DOUBLE'),
    col_('booked', 'DATE'),
  ];

  it('writes a model the planner can compile', async () => {
    const m = inferModel(described, { table: 'trades', convertible: true, databaseType: 'DuckDB' });
    assert.match(m.model, /###Relational/);
    assert.match(m.model, /Database local::DB/);
    assert.match(m.model, /Table trades/);
    assert.match(m.model, /region VARCHAR\(4096\)/);
    assert.match(m.model, /year BIGINT/);
    // A dialect comes from the Connection element, so the Runtime and
    // Connection have to be there too or the planner cannot pick one.
    assert.match(m.model, /###Connection/);
    assert.match(m.model, /type: DuckDB;/);
    assert.match(m.model, /###Runtime/);
    assert.equal(m.runtime, 'local::RT');
    assert.equal(print(lambda([], m.source)), '|#>{local::DB.trades}#');
    assert.deepEqual(m.conversions, []);
  });

  it('quotes a column name that needs it, and leaves a keyword bare', async () => {
    const m = inferModel([col_('total pnl', 'DOUBLE'),
      col_('select', 'VARCHAR')], { table: 't', convertible: true, databaseType: 'DuckDB' });
    assert.match(m.model, /"total pnl" DOUBLE/);
    assert.match(m.model, /\bselect VARCHAR/);
  });

  it('quotes an awkward table name, and refuses a dotted one (upstream splits the accessor on dots)', async () => {
    const m = inferModel([col_('a', 'VARCHAR')],
      { table: 'my table', convertible: true, databaseType: 'DuckDB' });
    assert.match(m.model, /Table "my table"/);
    assert.equal(print(lambda([], m.source)), '|#>{local::DB."my table"}#');
    assert.throws(() => inferModel([col_('a', 'VARCHAR')],
      { table: 'a.b', convertible: true, databaseType: 'DuckDB' }), /cannot be carried/);
  });

  it('refuses an empty schema and duplicate column names', async () => {
    assert.throws(() => inferModel([], { table: 't', convertible: true, databaseType: 'DuckDB' }), /no columns/);
    assert.throws(() => inferModel([
      col_('a', 'VARCHAR'),
      col_('A', 'VARCHAR'),
    ], { table: 't', convertible: true, databaseType: 'DuckDB' }), /two columns named/);
  });

  it('names what a copy must convert, and keeps a type Pure cannot name on any source', async () => {
    const cols = [col_('id', 'BIGINT'), col_('at', 'TIMESTAMP WITH TIME ZONE'), col_('ref', 'UUID')];
    const upload = inferModel(cols, { table: 't', convertible: true, databaseType: 'DuckDB' });
    assert.deepEqual(upload.conversions, [{ column: 'at', sql: `CAST(timezone('UTC', "at") AS TIMESTAMP)` }]);
    assert.match(upload.model, /at TIMESTAMP/);
    // a zoned timestamp is read in place, as its UTC instant under the UTC session; its conversion is
    // still the copy's (a Snap). A UUID is OTHER: read as its text wherever it is used, on every source.
    const warehouse = inferModel(cols, { table: 't', schema: 's', convertible: false, databaseType: 'DuckDB' });
    assert.deepEqual(warehouse.excluded, []);
    assert.match(warehouse.model, / at TIMESTAMP/);
    assert.match(warehouse.model, / ref OTHER/);
    assert.deepEqual(warehouse.conversions, [{ column: 'at', sql: `CAST(timezone('UTC', "at") AS TIMESTAMP)` }]);
  });

  it('reads a Postgres table by Postgres\'s rules: an array and an inet are text, json a Variant, bytes left out', async () => {
    const pg = (name: string, dataType: string, logicalType: string) =>
      ({ name, dataType, logicalType, precision: null, scale: null, notNull: false });
    const m = inferModel([pg('id', 'integer', 'int4'), pg('ia', 'integer[]', 'ARRAY'), pg('ip', 'inet', 'inet'),
      pg('doc', 'jsonb', 'jsonb'), pg('photo', 'bytea', 'bytea')],
    { table: 'kinds', schema: 'probe', convertible: false, databaseType: 'Postgres' });
    assert.match(m.model, /id INTEGER,\n\s*ia OTHER,\n\s*ip OTHER,\n\s*doc SEMISTRUCTURED\n/);
    assert.deepEqual(m.excluded, ['photo']);
    const sql = (await plannerFor(m.model, m.runtime).plan(from(m.source).lambda())).sql;
    assert.match(sql, /CAST\("t0"\."ia" AS VARCHAR\) AS "ia"/);
    assert.match(sql, /CAST\("t0"\."doc" AS JSONB\) AS "doc"/);
  });

  it('declares the database type it is given: a warehouse Postgres catalog plans Postgres SQL', () => {
    const m = inferModel([col_('id', 'int8', 'bigint')], { table: 'orders', schema: 'sales', convertible: false, databaseType: 'Postgres' });
    assert.match(m.model, /type: Postgres;/);
    assert.match(inferModel([col_('id', 'BIGINT')], { table: 't', convertible: true, databaseType: 'DuckDB' }).model, /type: DuckDB;/);
  });

  it('carries a snap runtime over the SAME Database when the copy\'s store is named (leg C)', () => {
    const m = inferModel([col_('id', 'int8', 'bigint')],
      { table: 'orders', schema: 'sales', convertible: false, databaseType: 'Postgres', snapDatabaseType: 'DuckDB' });
    assert.equal(m.runtime, 'local::RT');
    assert.equal(m.snapRuntime, 'local::SnapRT');
    assert.match(m.model, /RelationalDatabaseConnection local::Conn\n\{\n {4}type: Postgres;/);
    assert.match(m.model, /RelationalDatabaseConnection local::SnapConn\n\{\n {4}type: DuckDB;/);
    assert.match(m.model, /Runtime local::SnapRT[\s\S]*local::DB: \[ c1: local::SnapConn \]/);
    assert.equal((m.model.match(/^Database /gm) ?? []).length, 1, 'one Database, read through either runtime');
    // not asked for, none
    assert.equal(inferModel([col_('id', 'BIGINT')], { table: 't', convertible: true, databaseType: 'DuckDB' }).snapRuntime, undefined);
  });

  it('plans the SAME query in each runtime\'s SQL: Postgres live, DuckDB on the copy', async () => {
    // a Postgres table, as its own catalog describes it
    const m = inferModel([col_('region', 'text'), col_('n', 'int8', 'bigint')],
      { table: 'orders', schema: 'sales', convertible: false, databaseType: 'Postgres', snapDatabaseType: 'DuckDB' });
    // the root row's constant group key: Postgres needs it typed, DuckDB takes it bare
    const query = from(m.source).extend([derive('k', lambda(['x'], lit.string('[ROOT]')))])
      .groupBy(['k'], [agg('n', lambda(['x'], col('x', 'n')), lambda(['y'], fn('sum', variable('y'))))]).lambda();
    const live = (await plannerFor(m.model, m.runtime).plan(query)).sql;
    const copy = (await plannerFor(m.model, m.snapRuntime).plan(query)).sql;
    assert.match(live, /GROUP BY CAST\('\[ROOT\]' AS VARCHAR\)/);
    assert.match(copy, /GROUP BY '\[ROOT\]'/);
    assert.match(live, /FROM "sales"\."orders"/);
    assert.match(copy, /FROM sales\.orders|FROM "sales"\."orders"/);
  });

  it('leaves bytes out by name, on every source, and opens the rest', async () => {
    for (const convertible of [true, false]) {
      const m = inferModel([col_('id', 'BIGINT'), col_('photo', 'BLOB')], { table: 't', convertible, databaseType: 'DuckDB' });
      assert.deepEqual([m.excluded, m.conversions], [['photo'], []]);
      assert.doesNotMatch(m.model, /photo/);
    }
  });

  it('declares a nested column a Variant as stored, on any source (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md)', async () => {
    const cols = [col_('items', 'LIST', 'STRUCT(sku VARCHAR)[]'), col_('attrs', 'MAP', 'MAP(VARCHAR, INTEGER)')];
    for (const convertible of [true, false]) {
      const m = inferModel(cols, { table: 't', convertible, databaseType: 'DuckDB' });
      assert.deepEqual([m.conversions, m.excluded], [[], []]);
      assert.match(m.model, /items SEMISTRUCTURED,\n\s*attrs SEMISTRUCTURED/);
    }
  });
});

describe('a column the catalog says holds no NULL', () => {
  // declared NOT NULL, the compiler types it [1] (as legend-engine does), so arithmetic over it
  // needs no ->toOne(); a nullable column's still does (Typer.collection, as engine and pure)
  it('is declared NOT NULL, and plain arithmetic compiles over it -- not over a nullable one', async () => {
    const m = inferModel([{ ...col_('n', 'DOUBLE'), notNull: true }, col_('maybe', 'DOUBLE')], { table: 't', convertible: true, databaseType: 'DuckDB' });
    assert.match(m.model, /n DOUBLE NOT NULL,\n\s*maybe DOUBLE\n/);
    const planner = plannerFor(m.model, m.runtime);
    const uplift = (c: string) => from(m.source).extend([derive('u', lambda(['x'], times(col('x', c), lit.float(1.1))))]).lambda();
    const typed = await planner.relationType(uplift('n'));
    assert.equal(typed.find((c) => c.name === 'u')?.type, 'Float');
    await assert.rejects(planner.relationType(uplift('maybe')), /Collection element must have a multiplicity \[1\], found \[0\.\.1\]/);
  });
});

describe('the facts that belong to legend-lite', () => {
  it('takes the pivot separator from lite, not from a literal', () => {
    // If this is ever not '__|__', it is because lite changed
    // Type.java and the generator picked it up -- which is the point.
    assert.equal(PIVOT_SEPARATOR, '__|__');
  });
});

describe('inferModel with a schema (a warehouse table)', () => {
  it('declares the table inside its schema and reads it by the qualified name', async () => {
    const m = inferModel([col_('id', 'INTEGER'), col_('region', 'VARCHAR')],
      { table: 'v_orders', schema: 'sales', convertible: false, databaseType: 'DuckDB' });
    assert.match(m.model, /Schema sales\n {4}\(\n {8}Table v_orders\n {8}\(\n {12}id INTEGER,\n {12}region VARCHAR\(4096\)\n {8}\)\n {4}\)/);
    assert.equal(print(lambda([], m.source)), '|#>{local::DB.sales.v_orders}#');
  });

  it('declares no schema when there is none', async () => {
    const m = inferModel([col_('id', 'INTEGER')], { table: 't', convertible: true, databaseType: 'DuckDB' });
    assert.doesNotMatch(m.model, /Schema/);
  });
});
