// THE PRODUCT'S DATA HOST over a test's database, in Node (test/data-host.test.ts; the H7 floor's lane tests,
// 2026-11-13): the tab's own (data-host.ts heldDataHost, table-output.ts heldTableOutput) on the test's DuckDB-WASM; a
// file the test registered is held by the name it was registered under, as the tab holds a picked file. The headless lane
// builds the same test with the `headless` condition, which names data-host-headless.ts (package.json "imports",
// #test-data-host) -- its own import, so no other test's closure carries the data host.

import { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import { heldDataHost, type DataHost } from '../../src/data-host.ts';
import type { TableModels } from '../../src/infer.ts';
import { heldTableOutput, type TableWriteHost } from '../../src/table-output.ts';
import type { TestDatabase } from './node.ts';

export function productDataHost(db: TestDatabase, planner: Pick<TableModels, 'fileLoad' | 'remoteMount' | 'hostOperations' | 'catalogColumnsSql' | 'outputPlan'>): {
  readonly host: DataHost;
  readonly output: Pick<TableWriteHost, 'catalog' | 'output'>;
  handle(name: string): Promise<string>;
} {
  const engine = new DuckDbEngine(db.connection);
  return { host: heldDataHost(engine, planner), output: heldTableOutput(engine, planner), handle: async (name) => name };
}
