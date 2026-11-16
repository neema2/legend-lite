// Query's results grid plans on the tab's planner (CubePlanner): a connection's declared statement timeout, read beside
// the plan's values, is carried on the plan for the engine to stop the statement then, or refuse it (2026-11-16, the
// TypeScript batch's T-H); a value that is not a whole number of seconds is refused by name.

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';
import { CubePlanner } from '../src/backend/cube-planner.ts';
import type { WasmGrammar } from '../../engine-client/src/legend/wasm-grammar.ts';
import type { PureModelContext } from '../src/backend/wire.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';
import { lambda, lit } from '../../pure-protocol/src/index.ts';

/** A planner whose answer's target declares `declared` (none: undefined). */
const planning = (declared: unknown): Pick<WasmGrammar, 'plan'> => ({
  plan: async () => ({ sql: unplanned('SELECT 1 AS n'), type: { _type: 'relationType', columns: [] }, plan: { root: { _type: 'tdsResult',
    sql: { statement: 'SELECT 1 AS n', slots: [], target: { database: { _type: 'declared', connection: { name: 'q::Conn', databaseType: 'DuckDB',
      ...(declared === undefined ? {} : { queryTimeOutInSeconds: declared }) } } } } } } }),
});
const cube = (declared: unknown): CubePlanner =>
  new CubePlanner(planning(declared) as WasmGrammar, { _type: 'text', code: '' } as PureModelContext, 'q::RT', () => false);

describe("Query's cube planner, a connection's declared statement timeout", () => {
  it('carries it on the plan, none where none is declared, and refuses one that is not a whole number of seconds', async () => {
    const query = lambda([], lit.integer(1));
    assert.equal((await cube(12).plan(query)).timeout, 12);
    assert.equal('timeout' in (await cube(undefined).plan(query)), false);
    await assert.rejects(cube(-3).plan(query), /queryTimeOutInSeconds is a whole number of seconds, not -3/);
  });
});
