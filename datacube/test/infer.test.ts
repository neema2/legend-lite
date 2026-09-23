import assert from 'node:assert/strict';
import {
  GRAMMAR_TYPE_KEYWORDS,
  PIVOT_SEPARATOR,
  PURE_KIND_BY_SQL_NAME,
} from '../src/generated/lite-facts.ts';
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
    // 'Decimal', not 'Float'. This test asserted Float, which was
    // our hand-written table's answer and not legend-lite's
    // (RelationalKinds.pureKindOf spells Decimal/Numeric as Decimal).
    // The old answer still reached the right KIND for a money column,
    // but only because 'Float' happened to be what the measure
    // default tested for -- see isFractionalType.
    assert.equal(pureTypeOf('DECIMAL(9,2)'), 'Decimal');
    assert.equal(pureTypeOf('NUMERIC(18,4)'), 'Decimal');
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

  it('sums only FRACTIONAL types; integers take their unique value', () => {
    // The harm is asymmetric. Summing an id, a year or a postcode
    // gives a plausible number that is meaningless and says nothing
    // about being wrong; taking a quantity's unique value gives a
    // blank, which reads as "no aggregate chosen". Money and rates
    // arrive as DOUBLE or DECIMAL, which still sum.
    const m = inferModel([
      { name: 'notional', type: 'DOUBLE' },
      { name: 'rate', type: 'DECIMAL(9,4)' },
      { name: 'trade_id', type: 'BIGINT' },
      { name: 'year', type: 'BIGINT' },
      { name: 'qty', type: 'INTEGER' },
      { name: 'region', type: 'VARCHAR' },
      { name: 'booked', type: 'DATE' },
    ], { table: 't' });
    const kind = (n: string) => m.columns.find((c) => c.name === n)?.kind;
    assert.equal(kind('notional'), 'measure');
    assert.equal(kind('rate'), 'measure', 'DECIMAL is money too');
    assert.equal(kind('trade_id'), 'dimension');
    assert.equal(kind('year'), 'dimension');
    assert.equal(kind('region'), 'dimension');
    assert.equal(kind('booked'), 'dimension');
    // THE COST, stated rather than hidden: an integer quantity is a
    // sum the user has to ask for. A blank is recoverable in one
    // click; a wrong total is not noticed at all.
    assert.equal(kind('qty'), 'dimension');
  });

  it('classifies by TYPE alone, with no name matching', () => {
    // The previous rule read column names, matched `.*id$`, and so
    // called `bid` -- a price -- a key, along with paid, valid, void
    // and grid. It still missed cusip, isin, sedol, sku and account.
    // Nothing here looks at spelling, so none of that can recur.
    const m = inferModel([
      { name: 'bid', type: 'DOUBLE' },
      { name: 'paid', type: 'DOUBLE' },
      { name: 'void', type: 'DOUBLE' },
      { name: 'cusip', type: 'BIGINT' },
      { name: 'account', type: 'BIGINT' },
    ], { table: 't' });
    const kind = (n: string) => m.columns.find((c) => c.name === n)?.kind;
    for (const n of ['bid', 'paid', 'void']) {
      assert.equal(kind(n), 'measure', `${n} is a price, not a key`);
    }
    for (const n of ['cusip', 'account']) {
      assert.equal(kind(n), 'dimension',
        `${n} is an identifier no name list would have caught`);
    }
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

describe('the facts that belong to legend-lite', () => {
  it('emits only type keywords lite\'s Database grammar accepts', () => {
    // A generated Database that names a type the grammar does not
    // parse does not compile, and the failure surfaces as a planner
    // error on someone's first query after picking a file. The
    // keyword set is generated from DatabaseProtocolParser, so this
    // checks our output against lite's actual grammar rather than
    // against a list someone typed here.
    const emitted = [
      'VARCHAR', 'BIGINT', 'HUGEINT', 'UBIGINT', 'INTEGER', 'INT',
      'TINYINT', 'SMALLINT', 'DOUBLE', 'FLOAT', 'REAL', 'BOOLEAN',
      'BOOL', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'DECIMAL', 'NUMERIC',
      'BLOB', 'UUID', 'INTERVAL', 'STRUCT(a INTEGER)',
    ].map((t) => sqlTypeOf(t));
    for (const out of emitted) {
      const bare = out.includes('(') ? out.slice(0, out.indexOf('(')) : out;
      assert.ok(
        GRAMMAR_TYPE_KEYWORDS.includes(bare),
        `sqlTypeOf produced '${out}', whose keyword '${bare}' is not one `
          + `lite's Database grammar accepts: `
          + `${GRAMMAR_TYPE_KEYWORDS.join(', ')}`,
      );
    }
  });

  it('answers what lite answers, for every keyword lite maps', () => {
    // pureTypeOf is a LOOKUP into the generated table, so this asserts
    // the lookup does not lose or rewrite an entry -- the table itself
    // is kept in step by `bazel test //datacube:update_generated_test`.
    for (const [name, kind] of Object.entries(PURE_KIND_BY_SQL_NAME)) {
      assert.equal(pureTypeOf(name), kind, name);
    }
  });

  it('keeps the parameters off the lookup but not off the model', () => {
    // The table is keyed on the bare name; a declared type keeps its
    // precision, because that is what the Database carries.
    assert.equal(pureTypeOf('DECIMAL(38,6)'), 'Decimal');
    assert.equal(pureTypeOf('VARCHAR(4096)'), 'String');
    assert.equal(sqlTypeOf('DECIMAL(9,2)'), 'DECIMAL(9,2)');
  });

  it('falls back to String for a type lite does not map', () => {
    // Deliberate: a column the planner can only group by is far less
    // harmful than one whose arithmetic silently means something else.
    assert.equal(pureTypeOf('NOT_A_TYPE'), 'String');
    assert.equal(pureTypeOf(''), 'String');
  });

  it('takes the pivot separator from lite, not from a literal', () => {
    // If this is ever not '__|__', it is because lite changed
    // Type.java and the generator picked it up -- which is the point.
    assert.equal(PIVOT_SEPARATOR, '__|__');
  });
});
