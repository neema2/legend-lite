// Exact cells (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T3): what the database stored,
// never rounded, never shifted into the viewer's time zone.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  dayText, decimalText, exactInteger, exactSum, parseExact, timeText, timestampFromText, timestampText,
} from '../../engine-client/src/values.ts';

describe('calendar days and timestamps, exactly', () => {
  it('a day is its calendar day: year 45 stays year 45, and before the epoch is fine', () => {
    assert.equal(dayText(19724), '2024-01-02');
    assert.equal(dayText(-703029), '0045-03-04');
    assert.equal(dayText(-1), '1969-12-31');
  });

  it('a timestamp keeps its microseconds, before the epoch too', () => {
    assert.equal(timestampText(1704164645123456n), '2024-01-02T03:04:05.123456');
    assert.equal(timestampText(1704164645000000n), '2024-01-02T03:04:05');
    assert.equal(timestampText(1704164645500000n), '2024-01-02T03:04:05.5');
    assert.equal(timestampText(-1n), '1969-12-31T23:59:59.999999');
  });

  it('a time of day keeps its fraction', () => {
    assert.equal(timeText(36672500000n), '10:11:12.5');
  });

  it('a server timestamp is read as stored: its zone dropped, to the microsecond', () => {
    assert.equal(timestampFromText('2024-01-02T03:04:05.123456000+0000'), '2024-01-02T03:04:05.123456');
    assert.equal(timestampFromText('2024-01-02T03:04:05.000Z'), '2024-01-02T03:04:05');
    assert.equal(timestampFromText('not a timestamp'), 'not a timestamp');
  });
});

describe('numbers, exactly', () => {
  it('a decimal is its exact text at its scale', () => {
    assert.equal(decimalText(1234567890123456789n, 2), '12345678901234567.89');
    assert.equal(decimalText(-5n, 3), '-0.005');
  });

  it('an integer is a number while it is one, a bigint beyond 2^53', () => {
    assert.equal(exactInteger(42n), 42);
    assert.equal(exactInteger(9007199254740993n), 9007199254740993n);
  });

  it('sums exactly, at the widest scale', () => {
    assert.equal(exactSum(['12345678901234567.89', '1.01']), '12345678901234568.90');
    assert.equal(exactSum([9007199254740993n, 1]), '9007199254740994');
    assert.equal(exactSum([0.1, 0.2]), '0.3');
  });

  it('JSON keeps every digit: a big integer is a bigint, a long decimal its text', () => {
    const v = parseExact('{"a":9007199254740993,"b":1.5,"c":12345678901234567.89,"d":1.0,"e":7}') as Record<string, unknown>;
    assert.equal(v.a, 9007199254740993n);
    assert.equal(v.b, 1.5);
    assert.equal(v.c, '12345678901234567.89');
    assert.equal(v.d, 1);
    assert.equal(v.e, 7);
  });
});
