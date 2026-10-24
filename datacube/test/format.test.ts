import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FormatterCache, headlineFormat, type ColumnFormat } from '../src/format.ts';

const EN: Partial<ColumnFormat> = { locale: 'en-US' };

describe('FormatterCache', () => {
  it('reuses one Intl object across many cells', () => {
    const c = new FormatterCache();
    const f: ColumnFormat = { kind: 'number', ...EN };
    for (let i = 0; i < 1000; i++) c.format(i, f);
    // The whole point: 1,000 cells, one constructed formatter.
    assert.equal(c.stats.misses, 1);
    assert.equal(c.stats.hits, 999);
    assert.equal(c.stats.size, 1);
  });

  it('keys the cache on what actually affects Intl', () => {
    const c = new FormatterCache();
    const base: ColumnFormat = { kind: 'number', ...EN };
    c.format(1, base);
    // nullText and negativeParens are applied by us, not by Intl, so
    // they must not split the cache.
    c.format(1, { ...base, nullText: '-' });
    c.format(1, { ...base, negativeParens: true });
    assert.equal(c.stats.size, 1);
    // maximumFractionDigits does affect Intl, so it must.
    c.format(1, { ...base, maximumFractionDigits: 4 });
    assert.equal(c.stats.size, 2);
  });

  it('renders null as empty by default, never the word null', () => {
    const c = new FormatterCache();
    assert.equal(c.format(null, { kind: 'number', ...EN }), '');
    assert.equal(c.format(null, { kind: 'number', nullText: '—' }), '—');
  });

  it('formats currency and percent', () => {
    const c = new FormatterCache();
    assert.equal(
      c.format(1234.5, { kind: 'currency', currency: 'USD', ...EN }),
      '$1,234.50',
    );
    assert.equal(
      c.format(0.0725, { kind: 'percent', ...EN }),
      '7.3%',
    );
  });

  it('scales, which is how basis points work', () => {
    const c = new FormatterCache();
    // 0.0025 as bps = 25. One measure, a different column format --
    // the measure underneath stays a single numeric value.
    assert.equal(
      c.format(0.0025, {
        kind: 'number',
        scale: 1e-4,
        maximumFractionDigits: 0,
        ...EN,
      }),
      '25',
    );
  });

  it('wraps negatives in parentheses when asked', () => {
    const c = new FormatterCache();
    const f: ColumnFormat = {
      kind: 'currency',
      currency: 'USD',
      negativeParens: true,
      ...EN,
    };
    assert.equal(c.format(-42, f), '($42.00)');
    assert.equal(c.format(42, f), '$42.00');
  });

  it('auto renders by the column\'s compiler type so an unconfigured cube looks sane', () => {
    const c = new FormatterCache();
    assert.equal(c.format('EMEA', undefined, 'String'), 'EMEA');
    assert.equal(c.format(1234.567, { kind: 'auto', ...EN }, 'Float'), '1,234.57');
    // a decimal's value is its exact text: a number because its column is numeric
    assert.equal(c.format('1234.50', { kind: 'auto', ...EN }, 'Decimal'), '1,234.5');
    // and text that looks like a number is text when its column is: a zip code stays '00501'
    assert.equal(c.format('00501', { kind: 'auto', ...EN }, 'String'), '00501');
    // a date's value is its calendar text; the column's compiler type says it is a date
    assert.equal(c.format('2024-03-01', { kind: 'auto', ...EN }, 'StrictDate'), 'Mar 01, 2024');
  });

  it('falls back to the raw value rather than throwing on bad input', () => {
    const c = new FormatterCache();
    assert.equal(c.format('not-a-date', { kind: 'date', ...EN }), 'not-a-date');
    assert.equal(c.format('abc', { kind: 'number', ...EN }), 'abc');
  });

  it('clear() releases formatters and resets stats', () => {
    const c = new FormatterCache();
    c.format(1, { kind: 'number', ...EN });
    c.clear();
    assert.deepEqual(c.stats, { hits: 0, misses: 0, size: 0 });
  });
});

describe('negative zero', () => {
  // -0.0012 at two places read "-0" (a group-stage ratio, 2026-09-25).
  it('a value that rounds to zero shows no minus', () => {
    const f = new FormatterCache();
    assert.equal(f.format(-0.0012, { kind: 'number', locale: 'en-US', decimals: 2 }), '0.00');
    assert.equal(f.format(-0.0012, { kind: 'auto', locale: 'en-US' }, 'Float'), '0');
    assert.equal(f.format(-1.5, { kind: 'number', locale: 'en-US', decimals: 2 }), '-1.50');
  });
});

// the user, 2026-10-23: KPIs "nicer, more sleek" -- a stat tile's number at a glance
describe('headlineFormat: a KPI\'s number', () => {
  const c = new FormatterCache();
  const money: ColumnFormat = { kind: 'currency', currency: 'USD', decimals: 0, ...EN };
  it('scales money and numbers from 100,000 to their magnitude, one place at most, keeping the currency; below, as they are', () => {
    const headline = (v: number, f: ColumnFormat, type?: string) => c.format(v, headlineFormat(f, type, v), type);
    assert.equal(headline(35_732_015, money), '$35.7m');
    assert.equal(headline(40_000_000, money), '$40m', 'no place it does not need');
    assert.equal(headline(123_456, { kind: 'auto', ...EN }, 'Integer'), '123.5k', 'a number by its type');
    assert.equal(headline(5955, money), '$5,955', 'below 100,000 in full: "$6k" would say less');
    assert.equal(headline(-250_000, money), '-$250k');
  });
  it('a percentage to one place; a date and a text as they are', () => {
    const percent: ColumnFormat = { kind: 'percent', decimals: 0, ...EN };
    assert.equal(c.format(0.0820, percent), '8%');
    assert.equal(c.format(0.0820, headlineFormat(percent)), '8.2%', 'never a whole 8% against a target of 8%');
    assert.equal(c.format(0.35, headlineFormat(percent)), '35%');
    const date: ColumnFormat = { kind: 'date', ...EN };
    assert.equal(headlineFormat(date), date);
    assert.equal(headlineFormat({ kind: 'auto' }, 'String').kind, 'auto');
  });
});
