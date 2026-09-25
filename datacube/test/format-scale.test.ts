import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FormatterCache, autoScale, type ColumnFormat } from '../src/format.ts';

const EN = { locale: 'en-US' } as const;

describe('number scales', () => {
  const c = new FormatterCache();
  const fmt = (v: number, f: Partial<ColumnFormat>) =>
    c.format(v, { kind: 'number', ...EN, ...f });

  it('divides AND suffixes, which is the whole point', () => {
    // "1.2m" is readable; "1.2" is a different number.
    assert.equal(fmt(1_200_000, { numberScale: 'millions', decimals: 1 }), '1.2m');
    assert.equal(fmt(2_500, { numberScale: 'thousands', decimals: 1 }), '2.5k');
    assert.equal(fmt(3e9, { numberScale: 'billions', decimals: 0 }), '3b');
    assert.equal(fmt(7e12, { numberScale: 'trillions', decimals: 0 }), '7t');
  });

  it('renders ratios as percent and basis points', () => {
    assert.equal(fmt(0.075, { numberScale: 'percent', decimals: 1 }), '7.5%');
    assert.equal(fmt(0.0025, { numberScale: 'basisPoints', decimals: 0 }), '25bp');
  });

  it('auto picks per VALUE, so a mixed column stays readable', () => {
    // A column holding both 900 and 4bn is unreadable at any single
    // fixed scale; the suffix says which one each cell used.
    assert.equal(fmt(900, { numberScale: 'auto', decimals: 0 }), '900');
    assert.equal(fmt(4_000, { numberScale: 'auto', decimals: 0 }), '4k');
    assert.equal(fmt(4e6, { numberScale: 'auto', decimals: 0 }), '4m');
    assert.equal(fmt(4e9, { numberScale: 'auto', decimals: 0 }), '4b');
    assert.equal(fmt(4e12, { numberScale: 'auto', decimals: 0 }), '4t');
  });

  it('auto reads magnitude, not sign', () => {
    assert.equal(autoScale(-4e6).suffix, 'm');
    assert.equal(fmt(-4e6, { numberScale: 'auto', decimals: 0 }), '-4m');
  });

  it('wraps the WHOLE rendering in parens, suffix included', () => {
    // "($1.2m)" -- "($1.2)m" reads as a different number.
    assert.equal(
      c.format(-1_200_000, {
        kind: 'currency',
        currency: 'USD',
        ...EN,
        numberScale: 'millions',
        decimals: 1,
        negativeParens: true,
      }),
      '($1.2m)',
    );
  });
});

describe('decimals and separators', () => {
  const c = new FormatterCache();

  it('decimals pins both bounds, so 1.5 shows as 1.50', () => {
    assert.equal(c.format(1.5, { kind: 'number', ...EN, decimals: 2 }), '1.50');
    assert.equal(c.format(1.567, { kind: 'number', ...EN, decimals: 2 }), '1.57');
  });

  it('separators can be turned off for an id-like number', () => {
    assert.equal(c.format(12345, { kind: 'number', ...EN }), '12,345');
    assert.equal(
      c.format(12345, { kind: 'number', ...EN, displayCommas: false }),
      '12345',
    );
  });

  it('splits the cache on settings Intl sees', () => {
    const c2 = new FormatterCache();
    c2.format(1, { kind: 'number', ...EN, decimals: 2 });
    c2.format(1, { kind: 'number', ...EN, decimals: 3 });
    c2.format(1, { kind: 'number', ...EN, displayCommas: false });
    assert.equal(c2.stats.size, 3);
  });
});

describe('unit and case', () => {
  const c = new FormatterCache();

  it('glues a unit on, which is not a scale', () => {
    // Upstream's rendering exactly: no space.
    assert.equal(
      c.format(12.5, { kind: 'number', ...EN, decimals: 1, unit: 'kg' }),
      '12.5kg',
    );
  });

  it('puts a unit that starts with _ FIRST, without the _', () => {
    const f = { kind: 'number', ...EN, decimals: 0, unit: '_$' } as const;
    assert.equal(c.format(1234, f), '$1,234');
    assert.equal(c.format(-1234, { ...f, negativeParens: true }), '($1,234)');
    assert.equal(c.format(-1234, f), '$-1,234');
  });

  it('combines a scale and a unit', () => {
    assert.equal(
      c.format(1_500, {
        kind: 'number',
        ...EN,
        numberScale: 'thousands',
        decimals: 1,
        unit: 'bbl',
      }),
      '1.5kbbl',
    );
  });

  it('applies font case to text', () => {
    assert.equal(c.format('emea', { kind: 'text', fontCase: 'uppercase' }), 'EMEA');
    assert.equal(c.format('EMEA', { kind: 'text', fontCase: 'lowercase' }), 'emea');
    assert.equal(
      c.format('rates desk', { kind: 'text', fontCase: 'capitalize' }),
      'Rates Desk',
    );
  });

  it('applies case last, so it cannot disturb a number', () => {
    // Upper-casing '1.2m' gives '1.2M', not a broken number.
    assert.equal(
      c.format(1_200_000, {
        kind: 'number',
        ...EN,
        numberScale: 'millions',
        decimals: 1,
        fontCase: 'uppercase',
      }),
      '1.2M',
    );
  });

  it('still renders null as the configured text', () => {
    assert.equal(
      c.format(null, { kind: 'number', numberScale: 'millions', nullText: '—' }),
      '—',
    );
  });
});
