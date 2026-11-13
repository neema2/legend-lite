// THE ENGINE CONFORMANCE SUITE IN PYTHON'S V8 (engine-client/test/engine-conformance.ts; python/tests/test_contract.py):
// the suite run against Python's engine through the V8 bridge (engine-wire.ts bridge: the host's `engine` call), loaded
// as one test script into a headless engine whose product bundle has already installed the web layer. The global
// `legendLiteConformance.run(fixture)` answers each case's outcome, as JSON.

import { bridge } from '../../engine-client/src/engine-wire.ts';
import { runConformance, type ConformanceFixture } from '../../engine-client/test/engine-conformance.ts';
import { fromBase64 } from '../src/headless/web.ts';

export async function run(fixture: string): Promise<string> {
  const host = (globalThis as { legendLiteHost?: { engine(path: string, body: string): Promise<string> } }).legendLiteHost;
  if (host === undefined) throw new Error("no host: the conformance suite runs inside legend-lite's Python host");
  const engine = bridge((path, body) => host.engine(path, body), fromBase64);
  return JSON.stringify(await runConformance(engine, JSON.parse(fixture) as ConformanceFixture));
}
