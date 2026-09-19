import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FormatterCache, type ColumnFormat } from '../src/format.ts';

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

  it('auto renders by the value type so an unconfigured cube looks sane', () => {
    const c = new FormatterCache();
    assert.equal(c.format('EMEA'), 'EMEA');
    assert.equal(c.format(1234.567, { kind: 'auto', ...EN }), '1,234.57');
    assert.match(
      c.format(new Date('2024-03-01T00:00:00Z'), { kind: 'auto', ...EN }),
      /2024/,
    );
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
