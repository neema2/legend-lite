import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  inferModel,
  pureTypeOf,
  quoteIdent,
  sqlTypeOf,
} from '../src/infer.ts';
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

  it('leaves a SQL keyword bare, because the PURE grammar allows it', () => {
    // `select` is a valid Pure identifier; the lowerer is what quotes
    // it for SQL, emitting t0."select". Quoting it here as well would
    // make the wire name literally '"select"', since a quoted
    // relational identifier keeps its quotes. Measured: a file with
    // select/from headers planned 38 operations successfully.
    assert.equal(quoteIdent('select'), 'select');
    assert.equal(quoteIdent('from'), 'from');
  });

  it('quotes the headers that need it, which the grammar accepts', () => {
    assert.equal(quoteIdent('x,y'), '"x,y"');
    assert.equal(quoteIdent('  padded  '), '"  padded  "');
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

  it('escapes a quote in a header with a BACKSLASH', () => {
    // The lexer's escape inside a quoted identifier is the backslash,
    // and it ends the token at the first unescaped `"`. Doubling
    // instead -- `"a""b"` -- lexes as `"a"` then `"b"` and broke the
    // whole Database declaration: one such header made all 51
    // operations on that file refuse. Core now decodes this
    // spelling (Fold.bareIdent), so the header survives intact
    // rather than being renamed.
    assert.equal(quoteIdent('a"b'), '"a\\"b"');
    assert.equal(quoteIdent('back\\slash'), '"back\\\\slash"');
    // Backslash first: a name ending in one must not escape the
    // closing quote.
    assert.equal(quoteIdent('ends\\'), '"ends\\\\"');
  });

  it('keeps a hostile header a NAME, not grammar', () => {
    const m = inferModel(
      [{ name: 'a" VARCHAR(1)) Table Evil (b', type: 'VARCHAR' }],
      { table: 'trades' },
    );
    // Every quote inside the declaration is escaped, so nothing can
    // close the column list early.
    const line = m.model.split('\n').find((l) => l.includes('Table Evil'))!;
    assert.match(line, /\\"/);
    assert.ok(!/[^\\]" VARCHAR\(1\)\)/.test(line), line);
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
