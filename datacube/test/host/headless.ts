// Where a test's planner and database come from, in the headless lane (//datacube:headless_test): the host's -- the
// planner's worker answered by legend-lite's native library, the database Python's DuckDB -- the planner through the
// same module DataCube's headless bundle runs on (src/headless/host.ts). Named by the `headless` condition (package.json
// "imports", #test-host); host/node.ts says why.
//
// THE TEST'S DATABASE IS THE LANE'S OWN (2026-11-12, the total wall: the product host's bridge admits only the
// statements its planner answered, _planned.py across_the_bridge, and refused the tests' own setup SQL -- the lane went
// red unseen): a test runs its own SQL on it -- a fixture's table, a planner's statement it checks -- which the product's
// host never takes. So the lane's runner (test/headless/run.py, TestHost) adds calls of its own for it, `testQuery`,
// `testStatement` and `testRegisterFile`, on the same global; the product's bundle has none of them.
//
// AND THE PRODUCT'S STATEMENTS CROSS THE PRODUCT'S BRIDGE (the C final audit's H7, 2026-11-12: every statement went to
// the test calls, so the lane no longer exercised the bridge it was made to test): a statement the test typed as its
// own (engine-client/test/unplanned.ts) goes to the test calls; every other one -- a plan's, the planner's answer -- to
// the host's own `query` and `statement` (as src/headless/host.ts hostDatabase calls them), which admit only what the planner
// answered (_planned.py across_the_bridge), exactly as the product's bundle sends it.

import { arrowIpcTable } from '../../../engine-client/src/arrow-ipc.ts';
import type { ArrowishConnection, ArrowishTable } from '../../../engine-client/src/duckdb.ts';
import type { PlannedSql } from '../../../engine-client/src/planned-sql.ts';
import { hostCalls, hostWorker } from '../../src/headless/host.ts';
import { base64, fromBase64 } from '../../src/headless/web.ts';
import type { LitePlannerOptions } from '../../src/lite-planner.ts';
import type { TestDatabase } from './node.ts';
import { isTestsOwn } from '../../../engine-client/test/unplanned.ts';

export type { TestDatabase };

export const HOST: 'node' | 'headless' = 'headless';

/** The lane's own calls of its host (run.py TestHost): a test's statement run as written (its answer Arrow IPC, base64),
 *  one with its `?`s bound (`params`: a JSON list), and a file the database reads by name (its bytes, base64). */
interface TestHostCalls {
  testQuery(sql: string): Promise<string>;
  testStatement(sql: string, params: string): Promise<string>;
  testRegisterFile(name: string, bytes: string): Promise<void>;
}

function testCalls(): TestHostCalls {
  const calls = (globalThis as { legendLiteHost?: Partial<TestHostCalls> }).legendLiteHost;
  if (calls?.testQuery === undefined || calls.testStatement === undefined || calls.testRegisterFile === undefined) {
    throw new Error("no test host: the headless lane's runner (test/headless/run.py) defines a test's database calls");
  }
  return calls as TestHostCalls;
}

/** What a test's LitePlanner is given beside its model: a worker of the host's. */
export function plannerOptions(): Pick<LitePlannerOptions, 'assetBaseUrl' | 'worker'> {
  const calls = hostCalls();
  return { worker: () => hostWorker(calls) };
}

/** The host's database, as the lane's runner lends it to a test. One per host: a test's tables share it, as DuckDB-WASM's
 *  per-test databases did not. */
export async function openDatabase(): Promise<TestDatabase> {
  const calls = testCalls();
  // the product's own calls of the host's database (hostDatabase's): its statements cross the bridge, as the bundle's do
  const product = hostCalls();
  const connection: ArrowishConnection = {
    async query(sql: PlannedSql): Promise<ArrowishTable> {
      return arrowIpcTable(fromBase64(await (isTestsOwn(sql) ? calls.testQuery(sql) : product.query(sql))));
    },
    prepare: (sql: PlannedSql) => ({
      query: async (...params: unknown[]): Promise<ArrowishTable> => arrowIpcTable(fromBase64(await (isTestsOwn(sql)
        ? calls.testStatement(sql, JSON.stringify(params)) : product.statement(sql, JSON.stringify(params))))),
    }),
  };
  return {
    connection,
    files: {
      registerFileText: (name, text) => calls.testRegisterFile(name, base64(new TextEncoder().encode(text))),
      registerFileBuffer: (name, bytes) => calls.testRegisterFile(name, base64(bytes)),
    },
  };
}
