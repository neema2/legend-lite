// Tests in the tab (Studio plan A4): a service's test suites run where its queries run in the browser, by the engine's
// testable framework as core implements it once -- the plan from legend-lite's planner (core's TestPlan: each atomic
// test's provisioned tables, runtime, serialization format and assertions), the rows loaded into the tab's DuckDB as
// the model's Database declares them (a fresh database per test, as the engine's test runtime is), the service's query
// executed by the in-tab engine with the test's parameters bound as variables (`let`, never text), the answer
// serialized as the engine does, and each assertion judged by core's rules (`judge`). The same steps as core's
// ServiceTestRunner on a server; a test this runner cannot provide for is SKIPPED with why, never passed.

import { element, fn, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { databaseTables, dropTable, loadDataTables, type DataSink } from '../model-data.ts';
import type { Engine } from './engine.ts';
import type { PureModelContextData } from './pmcd.ts';
import { isTds, type ExecutionResult } from './wire.ts';

/** One atomic test as core plans it (TestPlan.toJson). */
export interface PlannedTest {
  readonly suite: string;
  readonly test: string;
  readonly skipped?: string;
  readonly runtime?: string;
  readonly format: string;
  readonly tables: readonly { readonly store: string; readonly schema: string; readonly table: string; readonly csv: string }[];
  readonly assertions: readonly { readonly id: string; readonly expectedJson?: string; readonly skipped?: string }[];
}

export type TestStatus = 'PASS' | 'FAIL' | 'SKIPPED';

export interface TestResult {
  readonly element: string;
  readonly suite: string;
  readonly test: string;
  readonly status: TestStatus;
  /** The first failed assertion's difference, the skip's cause, the phase that raised -- or how many passed. */
  readonly reason: string;
  readonly ms: number;
}

/** What a run needs: core's plan and judgment, the in-tab engine, and where rows go. */
export interface TestHost {
  testPlan(code: string, service: string): Promise<readonly PlannedTest[]>;
  /**
   * An EqualToJson judged by core's rules: undefined when equal, else the difference. Absent while core's judging is
   * not yet on the plan side (its design is being agreed): every test that would be judged is then SKIPPED, saying so
   * -- never judged here by rules of the tab's own.
   */
  judge?(expectedJson: string, actual: unknown): Promise<string | undefined>;
  readonly engine: Engine;
  readonly data: DataSink;
}

/** The parts of a service's model JSON a run reads: its query, mapping, and each test's parameters. */
interface ServiceJson {
  readonly _type: string;
  readonly package: string;
  readonly name: string;
  readonly execution?: { readonly func?: Lambda; readonly mapping?: string };
  readonly testSuites?: readonly {
    readonly id: string;
    readonly tests: readonly { readonly id: string; readonly parameters?: readonly { readonly name: string; readonly value: ValueSpecification }[] }[];
  }[];
}

class Skip extends Error {}

/** Every atomic test of `service` (its path), run in the tab, in declaration order. */
export async function runServiceTests(host: TestHost, code: string, model: PureModelContextData, service: string): Promise<TestResult[]> {
  const svc = (model.elements as unknown as ServiceJson[]).find((e) => e._type === 'service' && `${e.package}::${e.name}` === service);
  if (!svc) throw new Error(`no service ${service} in the model`);
  const declared = databaseTables(model.elements as Parameters<typeof databaseTables>[0]);
  const out: TestResult[] = [];
  for (const planned of await host.testPlan(code, service)) {
    const started = performance.now();
    const result = (status: TestStatus, reason: string): TestResult =>
      ({ element: service, suite: planned.suite, test: planned.test, status, reason, ms: Math.round(performance.now() - started) });
    try {
      out.push(await runOne(host, code, svc, declared, planned, result));
    } catch (e) {
      out.push(e instanceof Skip ? result('SKIPPED', e.message) : result('FAIL', `harness: ${e instanceof Error ? e.message : String(e)}`));
    }
  }
  return out;
}

async function runOne(host: TestHost, code: string, svc: ServiceJson, declared: ReturnType<typeof databaseTables>, planned: PlannedTest,
  result: (status: TestStatus, reason: string) => TestResult): Promise<TestResult> {
  if (planned.skipped !== undefined) throw new Skip(planned.skipped);
  const func = svc.execution?.func;
  const mapping = svc.execution?.mapping;
  if (!func || !mapping || !planned.runtime) throw new Skip('the service has no single execution with a mapping and a runtime');

  // a fresh database for the test: the provisioned stores' declared tables emptied, then the test's rows loaded
  for (const store of new Set(planned.tables.map((t) => t.store))) {
    for (const t of declared.filter((d) => d.database === store)) await dropTable(host.data, t.schema, t.table);
  }
  await loadDataTables(host.data, planned.tables.map((t) => {
    const table = declared.find((d) => d.database === t.store && d.schema === t.schema && d.table === t.table);
    if (!table) throw new Error(`the test provisions ${t.schema}.${t.table}, which ${t.store} does not declare`);
    return { schema: t.schema, table: t.table, columns: table.columns, csv: t.csv, element: `${planned.suite}.${planned.test}` };
  }));

  // the program: each parameter a let-bound variable, then the query, on the test's mapping and runtime
  const parameters = svc.testSuites?.find((s) => s.id === planned.suite)?.tests.find((t) => t.id === planned.test)?.parameters ?? [];
  const lets = parameters.map((p) => fn('letFunction', { _type: 'string', value: p.name } as ValueSpecification, p.value));
  const body = func.body;
  const last = fn('from', body[body.length - 1]!, element(mapping), element(planned.runtime));
  const program: Lambda = { _type: 'lambda', parameters: [], body: [...lets, ...body.slice(0, -1), last] } as Lambda;

  let answer: ExecutionResult;
  try {
    answer = await host.engine.execute({ function: program, model: { _type: 'text', code }, parameterValues: [] });
  } catch (e) {
    return result('FAIL', `execute: ${e instanceof Error ? e.message : String(e)}`);
  }
  const actual = serialize(answer, planned.format);
  for (const a of planned.assertions) {
    if (a.expectedJson === undefined) throw new Skip(a.skipped ?? `assertion '${a.id}' is not judged by this runner`);
    if (!host.judge) throw new Skip(`assertion '${a.id}': the query ran; judging in the tab waits on core's judging library`);
    const diff = await host.judge(a.expectedJson, actual);
    if (diff !== undefined) return result('FAIL', `${a.id}: ${diff}`);
  }
  return result('PASS', `${planned.assertions.length} assertion(s)`);
}

/** The answer as its serialization format renders it (core's ServiceTestRunner.serialize). */
function serialize(r: ExecutionResult, format: string): unknown {
  // a graph fetch's objects: the envelope the platform produces, the engine's DEFAULT for a serialize root
  if (!isTds(r)) return { builder: { _type: 'json' }, values: r.values };
  if (format !== 'PURE_TDSOBJECT' && format !== 'RAW') throw new Skip(`serialization format ${format} of a tabular result is not rendered by this runner`);
  const columns = r.result.columns;
  return r.result.rows.map((row) => Object.fromEntries(columns.map((c, i) => [c, row.values[i] ?? null])));
}
