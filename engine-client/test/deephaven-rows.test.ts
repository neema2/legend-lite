// A Deephaven server's rows (deephaven-rows.ts, docs/DEEPHAVEN_PLAN_2026_10_22.md DH23), read from an answer the
// pinned server sent (test/deephaven/doget-answer.hex: Arrow Flight's DoGet of a three-row table holding a column of
// every kind lite reads, recorded 2026-10-23, one FlightData message a line): laid out as an Arrow IPC stream, every
// value exactly as values.ts writes it -- Deephaven's own BigDecimal and BigInteger included -- and named and typed by
// the plan by position, a result that is not the plan's refused.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { ipcStream } from '../src/deephaven.ts';
import { byPosition, deephavenRows, followedTable, followedValue } from '../src/deephaven-rows.ts';
import type { Plan } from '../src/relation-type.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';

const messages = readFileSync(runfileFromEnv('DOGET_ANSWER'), 'utf8').trim().split('\n')
  .map((hex) => Uint8Array.from(hex.match(/../g) ?? [], (b) => parseInt(b, 16)));

const raw = deephavenRows(ipcStream(messages), 7, 1);

const PLAN: Plan = {
  sql: '{"ops": []}',
  columns: [
    { name: 'x', type: 'Integer' }, { name: 'l', type: 'Integer' }, { name: 'd', type: 'Float' },
    { name: 's', type: 'String' }, { name: 'b', type: 'Boolean' }, { name: 'ts', type: 'DateTime' },
    { name: 'dt', type: 'StrictDate' }, { name: 'tm', type: 'StrictTime' }, { name: 'bd', type: 'Decimal' },
    { name: 'bi', type: 'Integer' }, { name: '2023__|__total', type: 'String' },
  ],
};

describe("a Deephaven server's rows", () => {
  it('every value as values.ts writes it, read from the answer the server sent', () => {
    assert.equal(raw.rowCount, 3);
    assert.equal(raw.epoch, 7);
    const values = Object.fromEntries(raw.columns.map((c) => [c.name, c.values]));
    assert.deepEqual(values, {
      x: [0, 1, 2],
      // past 2^53 a bigint, exactly; Deephaven's null a null
      l: [9007199254740993n, 18014398509481986n, null],
      d: [0, 1.5, 3],
      s: ['x0', null, 'x2'],
      b: [true, null, true],
      // an Instant to the microsecond, UTC; a calendar day; a time of day
      ts: ['2024-01-02T03:04:05.123456', '2024-01-02T03:04:06.123456', '2024-01-02T03:04:07.123456'],
      dt: ['2024-02-29', '2024-03-01', '2024-03-02'],
      tm: ['03:04:05.123456', '03:04:05.123456', '03:04:05.123456'],
      // Deephaven's own forms: a BigDecimal's scale and digits, a BigInteger's two's complement
      bd: ['-12.345', '87.655', '187.655'],
      bi: [-700000000000000000000n, -700000000000000000000n, -700000000000000000000n],
      __c10: ['pivoted', 'pivoted', 'pivoted'],
    });
  });

  it("named and typed by the plan, by position: a pivot's column by its place", () => {
    const typed = byPosition(raw, PLAN);
    assert.deepEqual(typed.columns.map((c) => `${c.name}:${c.type}`), PLAN.columns.map((c) => `${c.name}:${c.type}`));
    assert.deepEqual(typed.columns[10]?.values, ['pivoted', 'pivoted', 'pivoted']);
  });

  it('a result that is not the plan’s is refused', () => {
    assert.throws(() => byPosition(raw, { ...PLAN, columns: PLAN.columns.slice(1) }),
      /the Deephaven result has 11 columns, the plan 10/);
    const renamed = PLAN.columns.map((c, i) => (i === 3 ? { ...c, name: 'region' } : c));
    assert.throws(() => byPosition(raw, { ...PLAN, columns: renamed }),
      /column 3 is 's', where the plan has 'region'/);
  });
});

/** The JS library's own value forms (dh-core.js): a wrapper whose text is exact, as its LongWrapper, DateWrapper
 *  (nanoseconds), BigDecimalWrapper and BigIntegerWrapper are; a LocalDateWrapper's text its day. */
const wrapped = (text: string): { toString(): string; valueOf(): string } => ({ toString: () => text, valueOf: () => text });
const timeOfDay = (h: number, m: number, sec: number, nano: number) => ({
  getHour: () => h, getMinute: () => m, getSecond: () => sec, getNano: () => nano,
});

describe("a followed table's rows (Deephaven's JS library)", () => {
  it('every value as values.ts writes it: the same as the Arrow rows of the same table', () => {
    // the recorded answer's table, as the library hands its values over
    const followed = {
      columns: ['x', 'l', 'd', 's', 'b', 'ts', 'dt', 'tm', 'bd', 'bi', '__c10'],
      values: [
        [0, 1, 2],
        [wrapped('9007199254740993'), wrapped('18014398509481986'), null],
        [0, 1.5, 3],
        ['x0', null, 'x2'],
        [true, null, true],
        ['1704164645123456000', '1704164646123456000', '1704164647123456000'].map(wrapped),
        ['2024-02-29', '2024-03-01', '2024-03-02'].map(wrapped),
        [timeOfDay(3, 4, 5, 123456000), timeOfDay(3, 4, 5, 123456000), timeOfDay(3, 4, 5, 123456000)],
        ['-12.345', '87.655', '187.655'].map(wrapped),
        ['-700000000000000000000', '-700000000000000000000', '-700000000000000000000'].map(wrapped),
        ['pivoted', 'pivoted', 'pivoted'],
      ],
    };
    const live = followedTable(followed, PLAN, 7, 0);
    const fetched = byPosition(raw, PLAN);
    assert.deepEqual(live.columns, fetched.columns);
    assert.equal(live.rowCount, 3);
  });

  it("Java's decimal text, exponent and all, as exact decimal text", () => {
    assert.equal(followedValue(wrapped('1E+3'), 'Decimal', 'd'), '1000');
    assert.equal(followedValue(wrapped('-1.5E-7'), 'Decimal', 'd'), '-0.00000015');
    assert.equal(followedValue(wrapped('12.300'), 'Decimal', 'd'), '12.300');
  });

  it('a value the plan does not type so is refused', () => {
    assert.throws(() => followedValue(42, 'String', 'desk'), /'desk' \(String\) holds number 42/);
    assert.throws(() => followedValue(wrapped('1.5'), 'Integer', 'n'), SyntaxError);
  });
});
