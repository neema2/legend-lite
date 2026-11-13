// THE PRODUCT'S DATA HOST, headless (test/data-host.test.ts; the H7 floor's lane tests, 2026-11-13): Python's -- each ask
// one of the product's intent calls (headless/host.ts hostData, hostTableOutput), planned and run by Python on its
// database, which the test's own is; a file the test registered handed out by the host, by a handle (Headless.hand_out,
// the lane's testHandOut). Named by the `headless` condition (package.json "imports", #test-data-host).

import type { DataHost } from '../../src/data-host.ts';
import { hostCalls, hostData, hostTableOutput } from '../../src/headless/host.ts';
import type { TableModels } from '../../src/infer.ts';
import type { TableWriteHost } from '../../src/table-output.ts';
import { testCalls, type TestDatabase } from './headless.ts';

export function productDataHost(_db: TestDatabase, _planner: Pick<TableModels, 'fileLoad' | 'remoteMount' | 'hostOperations' | 'catalogColumnsSql' | 'outputPlan'>): {
  readonly host: DataHost;
  readonly output: Pick<TableWriteHost, 'catalog' | 'output'>;
  handle(name: string): Promise<string>;
} {
  const calls = hostCalls();
  return { host: hostData(calls), output: hostTableOutput(calls), handle: (name) => testCalls().testHandOut(name) };
}
