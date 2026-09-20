import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { inferModel, pureTypeOf, quoteIdent, sqlTypeOf } from '../src/infer.ts';
import { formatOf, tableNameOf } from '../src/upload.ts';

describe('sqlTypeOf', () => {
  it('maps what DuckDB actually reports', () => {
    assert.equal(sqlTypeOf('VARCHAR'), 'VARCHAR(4096)');
    assert.equal(sqlTypeOf('BIGINT'), 'BIGINT');
    assert.equal(sqlTypeOf('INTEGER'), 'INTEGER');
    assert.equal(sqlTypeOf('DOUBLE'), 'DOUBLE');
    assert.equal(sqlTypeOf('BOOLEAN'), 'BIT');
    assert.equal(sqlTypeOf('DATE'), 'DATE');
    assert.equal(sqlTypeOf('TIMESTAMP'), 'TIMESTAMP');
  });

  it('keeps a DECIMAL\'s precision rather than flattening it', () => {
    // Rounding someone's money column to a default scale is the kind
    // of wrong that looks right.
    assert.equal(sqlTypeOf('DECIMAL(9,2)'), 'DECIMAL(9,2)');
    assert.equal(sqlTypeOf('NUMERIC(18,4)'), 'DECIMAL(18,4)');
  });

  it('falls back to VARCHAR for anything unrecognised', () => {
    // A column you can only group by is far less harmful than one
    // whose arithmetic silently means something else.
    for (const t of ['STRUCT(a INT)', 'INTERVAL', 'UUID', 'BLOB', 'nonsense']) {
      assert.equal(sqlTypeOf(t), 'VARCHAR(4096)', t);
    }
  });

  it('is case- and space-insensitive', () => {
    assert.equal(sqlTypeOf('  double '), 'DOUBLE');
  });
});

describe('pureTypeOf', () => {
  it('maps the SQL types a generated Database can carry', () => {
    assert.equal(pureTypeOf('VARCHAR(4096)'), 'String');
    assert.equal(pureTypeOf('INTEGER'), 'Integer');
    assert.equal(pureTypeOf('BIGINT'), 'Integer');
    assert.equal(pureTypeOf('DOUBLE'), 'Float');
    assert.equal(pureTypeOf('DECIMAL(9,2)'), 'Float');
    assert.equal(pureTypeOf('BIT'), 'Boolean');
    assert.equal(pureTypeOf('DATE'), 'StrictDate');
    assert.equal(pureTypeOf('TIMESTAMP'), 'DateTime');
  });
});

describe('quoteIdent', () => {
  it('leaves a plain identifier alone', () => {
    assert.equal(quoteIdent('region'), 'region');
    assert.equal(quoteIdent('_x9'), '_x9');
  });

  it('quotes anything that is not one', () => {
    assert.equal(quoteIdent('total pnl'), '"total pnl"');
    assert.equal(quoteIdent('2024'), '"2024"');
    assert.equal(quoteIdent('a-b'), '"a-b"');
  });

  it('doubles an embedded quote, so a header cannot become grammar', () => {
    // A CSV header arrives from outside. Without this, a column named
    //   a" VARCHAR(1)) Table Evil (b
    // stops being a name and starts being a Database declaration.
    assert.equal(quoteIdent('a"b'), '"a""b"');
    const hostile = 'a" VARCHAR(1)) Table Evil (b';
    const q = quoteIdent(hostile);
    assert.equal(q, '"a"" VARCHAR(1)) Table Evil (b"');
    // Every quote in the result is either the delimiter or doubled.
    assert.equal((q.slice(1, -1).match(/"/g) ?? []).length % 2, 0);
  });
});

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
  });
});

describe('inferModel', () => {
  const described = [
    { name: 'region', type: 'VARCHAR' },
    { name: 'year', type: 'BIGINT' },
    { name: 'notional', type: 'DOUBLE' },
    { name: 'booked', type: 'DATE' },
  ];

  it('writes a model the planner can compile', () => {
    const m = inferModel(described, { table: 'trades' });
    assert.match(m.model, /###Relational/);
    assert.match(m.model, /Database local::DB/);
    assert.match(m.model, /Table trades/);
    assert.match(m.model, /region VARCHAR\(4096\)/);
    assert.match(m.model, /year BIGINT/);
    assert.match(m.model, /notional DOUBLE/);
    assert.match(m.model, /booked DATE/);
    // A dialect comes from the Connection element, so the Runtime and
    // Connection have to be there too or the planner cannot pick one.
    assert.match(m.model, /###Connection/);
    assert.match(m.model, /type: DuckDB;/);
    assert.match(m.model, /###Runtime/);
    assert.equal(m.runtime, 'local::RT');
    assert.equal(m.source, '#>{local::DB.trades}#');
  });

  it('defaults numbers to measures but keeps year a dimension', () => {
    const m = inferModel(described, { table: 'trades' });
    const kind = (n: string) => m.columns.find((c) => c.name === n)?.kind;
    assert.equal(kind('notional'), 'measure');
    assert.equal(kind('year'), 'dimension', 'a year is numeric and a key');
    assert.equal(kind('region'), 'dimension');
    assert.equal(kind('booked'), 'dimension');
  });

  it('treats id- and code-like numbers as dimensions', () => {
    const m = inferModel([
      { name: 'trade_id', type: 'BIGINT' },
      { name: 'zip', type: 'INTEGER' },
      { name: 'account_number', type: 'BIGINT' },
      { name: 'qty', type: 'INTEGER' },
    ], { table: 't' });
    const kind = (n: string) => m.columns.find((c) => c.name === n)?.kind;
    assert.equal(kind('trade_id'), 'dimension');
    assert.equal(kind('zip'), 'dimension');
    assert.equal(kind('account_number'), 'dimension');
    assert.equal(kind('qty'), 'measure');
  });

  it('quotes a hostile header instead of emitting it raw', () => {
    const m = inferModel(
      [{ name: 'a" VARCHAR(1)) Table Evil (b', type: 'VARCHAR' }],
      { table: 'trades' },
    );
    assert.ok(!/Table Evil \(b VARCHAR/.test(m.model),
      'the header must not close the declaration and open another');
    assert.match(m.model, /"a"" VARCHAR\(1\)\) Table Evil \(b" VARCHAR\(4096\)/);
  });

  it('quotes a table name that needs it', () => {
    const m = inferModel([{ name: 'a', type: 'VARCHAR' }],
      { table: 'my table' });
    assert.match(m.model, /Table "my table"/);
    assert.equal(m.source, '#>{local::DB."my table"}#');
  });

  it('refuses an empty schema with a message about the FILE', () => {
    // The planner's own error for a column-less Database is about a
    // malformed model, which is a baffling thing to show someone who
    // just picked a file.
    assert.throws(() => inferModel([], { table: 't' }),
      /no columns.*empty.*header/s);
  });

  it('refuses duplicate column names', () => {
    assert.throws(
      () => inferModel([
        { name: 'a', type: 'VARCHAR' },
        { name: 'A', type: 'VARCHAR' },
      ], { table: 't' }),
      /two columns named/,
    );
  });
});
