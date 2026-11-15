// WHAT A SERVER MUST HOLD WHEN A RUNTIME CONNECTS (src/deephaven-held.ts): every table tried, any refused refusing
// the whole and naming each with its reason; the planner's refusal said. No server here: `read` stands for a session's
// keepAlive (the lanes read a real server: //datacube:deephaven_market_test).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { checkHeld, DeephavenNotHeld, heldTablesOf, type HeldTable } from '../src/deephaven-held.ts';

const table = (name: string): HeldTable => ({ connection: 'm::Server', deployment: 'm::OnServer', table: name,
  sql: `{"ops": [${name}]}` });

describe('what a runtime\'s server must hold', () => {
  it('reads every table once and lets each go', async () => {
    const read: string[] = [];
    let released = 0;
    await checkHeld([table('A'), table('B')], (sql) => {
      read.push(sql);
      return Promise.resolve({ release: () => { released++; } });
    });
    assert.deepEqual(read, ['{"ops": [A]}', '{"ops": [B]}']);
    assert.equal(released, 2);
  });

  it('refuses the whole, naming every table refused and why', async () => {
    const refused = await checkHeld([table('A'), table('B'), table('C')], (sql) => sql.includes('B')
      ? Promise.resolve({ release: () => undefined })
      : Promise.reject(new Error(sql.includes('A') ? 'no table A' : 'C is held as int')))
      .then(() => null, (e: unknown) => e);
    assert.ok(refused instanceof DeephavenNotHeld);
    assert.deepEqual(refused.missing.map((m) => [m.table, m.why]), [['A', 'no table A'], ['C', 'C is held as int']]);
    assert.match(refused.message, /does not hold what its deployment lands: A \(m::OnServer\): no table A; C/);
  });

  it('reads the planner\'s answer, and says its refusal', () => {
    assert.deepEqual(heldTablesOf(`OK\n${JSON.stringify({ tables: [table('A')] })}`), [table('A')]);
    assert.throws(() => heldTablesOf('ERR\nIllegalArgumentException\nthe model declares no Runtime m::Nowhere'),
      /must hold: the model declares no Runtime m::Nowhere/);
  });
});
