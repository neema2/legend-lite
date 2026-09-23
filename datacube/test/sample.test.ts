import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { csvCell, SAMPLE_COLUMNS, sampleCsv } from '../src/samples.ts';
import { inferModel } from '../src/infer.ts';

describe('csvCell', () => {
  it('leaves an ordinary value alone', () => {
    assert.equal(csvCell('EMEA'), 'EMEA');
    assert.equal(csvCell('123.45'), '123.45');
  });

  it('quotes and doubles, per RFC 4180', () => {
    assert.equal(csvCell('a,b'), '"a,b"');
    assert.equal(csvCell('say "hi"'), '"say ""hi"""');
    assert.equal(csvCell('two\nlines'), '"two\nlines"');
  });

  it('does not quote an apostrophe, which needs no escaping', () => {
    assert.equal(csvCell("O'Brien"), "O'Brien");
  });
});

describe('sampleCsv', () => {
  it('has a header and the asked-for number of rows', () => {
    const lines = sampleCsv({ rows: 10 }).trimEnd().split('\n');
    assert.equal(lines[0], SAMPLE_COLUMNS.join(','));
    // The hostile book name has no newline in it, so one row is one
    // line and this count is meaningful.
    assert.equal(lines.length, 11);
  });

  it('is deterministic for a seed, and different across seeds', () => {
    assert.equal(sampleCsv({ rows: 20, seed: 1 }),
      sampleCsv({ rows: 20, seed: 1 }));
    assert.notEqual(sampleCsv({ rows: 20, seed: 1 }),
      sampleCsv({ rows: 20, seed: 2 }));
  });

  it('ends with a newline', () => {
    assert.ok(sampleCsv({ rows: 3 }).endsWith('\n'));
  });

  it('carries the hard CSV case, correctly escaped', () => {
    // The point of the sample is to exercise what it is opened with,
    // and a comma inside a quoted field is where CSV readers fail.
    const csv = sampleCsv({ rows: 400 });
    assert.ok(csv.includes('"Smith, ""Big"" Fund"'),
      'the quoted-comma book name must appear, escaped');
    assert.ok(csv.includes("O'Brien Holdings"));
  });

  it('every row has the same number of fields as the header', () => {
    // Counting commas would be wrong -- a quoted field contains one.
    const csv = sampleCsv({ rows: 200 });
    const lines = csv.trimEnd().split('\n');
    for (const [i, line] of lines.entries()) {
      let fields = 1;
      let quoted = false;
      for (let c = 0; c < line.length; c++) {
        const ch = line[c];
        if (ch === '"') quoted = !quoted;
        else if (ch === ',' && !quoted) fields++;
      }
      assert.equal(fields, SAMPLE_COLUMNS.length, `line ${i}: ${line}`);
    }
  });

  it('covers every branch the schema inference has', () => {
    // A sample that only had strings and doubles would let the
    // interesting half of inferModel rot untested in practice.
    const described = [
      { name: 'trade_id', type: 'BIGINT' },
      { name: 'trade_date', type: 'DATE' },
      { name: 'booked_at', type: 'TIMESTAMP' },
      { name: 'region', type: 'VARCHAR' },
      { name: 'year', type: 'BIGINT' },
      { name: 'notional', type: 'DOUBLE' },
      { name: 'settled', type: 'BOOLEAN' },
    ];
    const m = inferModel(described, { table: 'sample_trades' });
    const by = (n: string) => m.columns.find((c) => c.name === n);
    assert.equal(by('trade_date')?.type, 'StrictDate');
    assert.equal(by('booked_at')?.type, 'DateTime');
    assert.equal(by('settled')?.type, 'Boolean');
    assert.equal(by('notional')?.kind, 'measure');
    assert.equal(by('trade_id')?.kind, 'dimension');
    assert.equal(by('year')?.kind, 'dimension');
  });

  it('scales to a size worth playing with', () => {
    const csv = sampleCsv({ rows: 50_000 });
    assert.ok(csv.length > 1_000_000, `only ${csv.length} bytes`);
  });
});
