// THE TAB'S ENGINE HELD TO THE CONFORMANCE SUITE (src/engines/tab-engine.ts; engine-client/test/engine-conformance.ts,
// the engine contract's R7, step 2, 2026-11-13): the same asks Python headless answers (//python:contract_test), answered
// in-thread by the tab's planner and DuckDB-WASM -- every case passes. Its source is a table of the tab's database, its
// model the one the tab's planner writes, served to the suite as the contract hands it DataCube (R3a (c)).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { readFileSync } from 'node:fs';

import { DuckDbEngine, type ArrowishConnection } from '../../engine-client/src/duckdb.ts';
import { startNodeDuckDb } from '../../engine-client/src/node-require.ts';
import { ProjectDatabases } from '../../engine-client/src/project-data.ts';
import { runfileFromEnv, runfilesFromEnv } from '../../tools/js/runfiles.mts';
import { isEngineError } from '../../engine-client/src/engine-contract.ts';
import { PlanRefusal, runPlan } from '../../engine-client/src/plan-runner.ts';
import { unplannedPlan } from '../../engine-client/test/unplanned.ts';
import { runConformance, type ConformanceFixture } from '../../engine-client/test/engine-conformance.ts';
import { localPlane, tabEngine } from '../src/engines/tab-engine.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { catalogColumns } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import { unplanned } from '../../engine-client/test/unplanned.ts';

describe("the tab's engine, held to the engine contract's conformance suite", () => {
  let engine: DuckDbEngine;
  let planner: LitePlanner;
  // the tab's own DuckDB-WASM (this test is Node's alone): a project's database opens connections of its own (C5)
  let duck: Awaited<ReturnType<typeof startNodeDuckDb>>;
  let db: { readonly files: { registerFileBuffer(name: string, bytes: Uint8Array): Promise<void> } };
  // the party project (the run-load fixture's model), resolved by its coordinates as the tab's configured projects are
  const party = (JSON.parse(readFileSync(runfileFromEnv('RUN_LOAD_FIXTURE'), 'utf8')) as { model: string }).model;
  const projects = () => ({
    model: async (p: { groupId: string; artifactId: string; versionId: string }) =>
      (p.groupId === 'demo' && p.artifactId === 'party' && p.versionId === '1.0.0' ? party
        // version 2.0.0 declares a function generic over relations (S112): Q31's case
        : p.groupId === 'demo' && p.artifactId === 'party' && p.versionId === '2.0.0' ? `${party}\n###Pure\nfunction demo::party::firstRows<T>(t: Relation<T>[1], n: Integer[1]): Relation<T>[1]\n{\n  $t->limit($n)\n}\n`
          : undefined),
    // and a workspace of it at a revision, as its model home's SDLC answers it (Q23)
    revision: async (r: { project: string; workspace: string; revision: string }) =>
      (r.project === 'demo:party' && r.workspace === 'w2' && r.revision === 'r-2' ? party : undefined),
    databases: async () => new ProjectDatabases({ databaseType: 'DuckDB', run: (sql) => engine.run(sql, 0),
      connect: async () => new DuckDbEngine(duck.connect() as ArrowishConnection) }, planner),
  });

  before(async () => {
    duck = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
    db = { files: { registerFileBuffer: async (name, bytes) => { duck.registerFileBuffer(name, bytes); } } };
    engine = new DuckDbEngine(duck.connect() as ArrowishConnection);
    planner = plannerFor('', 'local::RT');
    for (const t of ['conformance_t', 'conformance_out']) await engine.run(unplanned(`DROP TABLE IF EXISTS ${t}`), 0);
  });

  after(async () => {
    planner?.dispose();
    await engine?.close();
  });

  it('passes every case', async () => {
    const tab = tabEngine({ database: engine, plane: localPlane(planner), writer: planner, files: db.files, projects: projects() });
    // the suite's table, one of the engine's own (Q14, Q30: exactly the tables it issued handles for): an upload by the
    // name asked (Q28), its model the one the engine handed back (R3a (c)); the session held while the suite runs
    const setup = await tab.open();
    const added = await setup.addSource({ upload: { name: 'conformance.csv', bytes: new TextEncoder().encode('id,name\n1,a\n2,b\n3,c\n4,d\n') } }, 'snap', { name: 'conformance_t' });
    assert.equal(added.name, 'conformance_t', 'the name asked');
    const { model, runtime } = added;
    assert.ok(!model.includes('###Connection') && !model.includes('###Runtime'), 'R3a (c): the handed model names its runtime alone');
    const fixture: ConformanceFixture = {
      model, runtime, source: added.relation,
      columns: [{ name: 'id', type: 'Integer' }, { name: 'name', type: 'String' }],
      rowCount: 4, textColumn: 'name', keyColumn: 'id', ownTable: 'conformance_t', store: 'conformance_t::DB',
      writeTarget: 'conformance_out',
      upload: { name: 'up.csv', text: 'id,name\n1,a\n2,b\n3,c\n', rows: 3 },
      sentinel: 'SENTINEL-held-4f1c',
      project: { coordinates: { groupId: 'demo', artifactId: 'party', versionId: '1.0.0' }, runtime: 'demo::party::Runtime',
        mapping: 'demo::party::PartyMapping', cls: 'demo::party::Party', rows: 2, text: party,
        query: '|demo::party::Party.all()->project(~[id: x|$x.id, name: x|$x.name])',
        revision: { coordinates: { project: 'demo:party', workspace: 'w2', revision: 'r-2' }, text: party },
        extended: { groupId: 'demo', artifactId: 'party', versionId: '2.0.0' } },
      // no `served`: DuckDB-WASM in Node makes no request (engine-client node-require.ts), so the remote case says so
    };
    const results = await runConformance(tab, fixture);
    assert.ok(results.length >= 30, `${results.length} cases ran`);
    assert.deepEqual(results.filter((r) => !r.ok), []);
    // the one case not run here, by name and why: a remote file over HTTP (run against Python headless instead)
    assert.deepEqual(results.filter((r) => r.notRun !== undefined).map((r) => [r.name.slice(0, 3), r.notRun]),
      [['C17', "the engine's database makes no request from this test: no served file to read"]]);
    const rows = await engine.exclusive((held) => held.rows(unplanned('SELECT id, name FROM conformance_out ORDER BY id'), []));
    assert.deepEqual(rows.map((r) => [Number(r[0]), r[1]]), [[1, 'a'], [2, 'b'], [3, 'c'], [4, 'd']], 'the last write, merged by its key');
  });

  it("binds a stamped plan's stamp to its door's execution id, and refuses one run with none (step 3b)", async () => {
    const tab = tabEngine({ database: engine, plane: localPlane(planner), writer: planner, files: db.files });
    const written = await planner.tableModel(await catalogColumns(engine, planner, 'conformance_t'),
      { table: 'conformance_t', pkg: 'conformance_t', convertible: true, databaseType: 'DuckDB' });
    const { model, runtime } = await tab.served(written.model, written.runtime);
    // the real output plan made to read its execution's stamp: its final count `SELECT ? AS value`, a #stamp slot
    let made: string | undefined;
    const real = planner.outputPlan.bind(planner);
    const stamped = async (request: Parameters<typeof real>[0]) => {
      const plan = JSON.parse(await real(request)) as { root: { steps: { _type: string; sql: Record<string, unknown> }[] } };
      const last = plan.root.steps.at(-1)!;
      assert.equal(last._type, 'tdsResult');
      last.sql = { ...last.sql, statement: 'SELECT ? AS value', slots: [{ parameter: '#stamp0', binding: { _type: 'one' } }] };
      made = JSON.stringify(plan);
      return unplannedPlan(made);
    };
    (planner as { outputPlan: typeof real }).outputPlan = stamped;
    try {
      const s = await tab.open();
      await assert.rejects(() => s.write({ box: { model, runtime, query: { _type: 'lambda', parameters: [], body: [written.source] } },
        target: { table: 'stamped_t' }, mode: 'create' }), (e: unknown) => isEngineError(e) && e.kind === 'run'
        && e.executionId !== undefined && e.message.includes(`executionTraceID\\" : \\"${e.executionId}`) || assert.fail(String(e)));
      await s.close();
    } finally {
      delete (planner as { outputPlan?: unknown }).outputPlan;
    }
    // run with no id from a door: refused by name, nothing defaulted
    await assert.rejects(() => engine.exclusive((held) => runPlan(unplannedPlan(made!), { databaseType: 'DuckDB', run: held.rows })),
      (e: unknown) => e instanceof PlanRefusal && /'#stamp0' reads its execution's stamp, and the run was given no id by its door/.test(e.message));
  });
});
