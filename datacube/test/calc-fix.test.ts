// The quick fix for arithmetic over a possibly-empty column (src/calc-fix.ts), through the real
// compiler: what it finds, what it writes, and that what it writes compiles -- where the person's
// own expression is refused.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { emptyOperands, isEmptyOperandRefusal, sayEmpty } from '../src/calc-fix.ts';
import { derive, from } from '../../pure-protocol/src/index.ts';
import { liteParse, litePrint } from './lite-compiler.ts';
import { plannerFor } from './catalog-builder.ts';

const m = await plannerFor('', 'local::RT').tableModel([
  { name: 'notional', dataType: 'DOUBLE', logicalType: 'DOUBLE', precision: 53, scale: 0, notNull: false },
  { name: 'qty', dataType: 'INTEGER', logicalType: 'INTEGER', precision: 32, scale: 0, notNull: false },
  { name: 'px', dataType: 'DECIMAL(9,2)', logicalType: 'DECIMAL', precision: 9, scale: 2, notNull: false },
], { table: 't', convertible: true, databaseType: 'DuckDB' });
const TYPES: Record<string, string> = { notional: 'Float', qty: 'Integer', px: 'Decimal' };
const planner = plannerFor(m.model, m.runtime);
const typeOf = async (text: string): Promise<string | undefined> => {
  const typed = await planner.relationType(from(m.source).extend([derive('u', await liteParse(text))]).lambda());
  return typed.find((c) => c.name === 'u')?.type;
};

describe('arithmetic over a possibly-empty column: the quick fix', () => {
  it('is the refusal it answers, in either compiler\'s words', async () => {
    const refused = await typeOf('x|$x.notional * 1.1').then(() => '', (e: Error) => e.message);
    assert.ok(isEmptyOperandRefusal(refused), refused);
    assert.ok(isEmptyOperandRefusal('Collection element must have a multiplicity [1] - Context:[Applying times], multiplicity:[0..1]'));
    assert.ok(!isEmptyOperandRefusal("the source has no column 'nope'"));
  });

  it('names each column read bare in a run, once', async () => {
    assert.deepEqual(emptyOperands(await liteParse('x|$x.notional * 1.1 + $x.qty - $x.notional')), ['notional', 'qty']);
    assert.deepEqual(emptyOperands(await liteParse('x|$x.notional->toOne() * 2')), []);
  });

  it('"Empty": ->toOne() on each, and the compiler takes it', async () => {
    const text = await litePrint(sayEmpty(await liteParse('x|$x.notional * 1.1 + $x.qty'), 'blank', (c) => TYPES[c]));
    assert.match(text, /\$x\.notional->toOne\(\) \* 1\.1/);
    assert.match(text, /\$x\.qty->toOne\(\)/);
    assert.equal(await typeOf(text), 'Float');
  });

  it('"As if zero": coalesce with a zero of the column\'s own type, and the compiler takes it', async () => {
    for (const [col, zero] of [['notional', /coalesce\(0\.0\)/], ['qty', /coalesce\(0\)/], ['px', /coalesce\(0D\)/]] as const) {
      const text = await litePrint(sayEmpty(await liteParse(`x|$x.${col} * 2`), 'zero', (c) => TYPES[c]));
      assert.match(text, zero, text);
      assert.ok(await typeOf(text), `${text} compiles`);
    }
  });
});
