// THE APPS' PLAN-AND-RUN SITES, A SHRINK-ONLY CENSUS (the C5-T audit's MUST 2, 2026-11-15: the interim run wall's
// ratchet restored, over what the layers' rule leaves to the apps). The layers hold the run side apart from the planner
// (tools/deps/ts-layers.txt, //tools/deps:layers_alone): a run layer reaches records and npm alone, a planner nothing
// that executes, and a door -- engine-client's, DataCube's (//datacube:doors) -- asks the planner, then runs its answer
// in a run frame. What no layer holds is an app's own code: DataCube's app, Query's and Studio's. Here, every site there
// that runs -- a call of runPlan, runLoad, establish, runAuthored or inRun, or a run, a stream or an execute on an
// engine, a runner, a host, a session, a client, an executor or a server (the wall's RUN_CALL) -- outside the doors' and
// the run targets' own files, is named by file and count, with its reason. A new app module that plans and runs fails
// here; a site that moves into a door leaves the census in the same push. Each that remains is a DOOR call -- a server's
// execute or BrowserEngine's, which plan and run in one request, or DataCube's runner, a door -- named as one.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { runfileFromEnv, Sources } from '../js/runfiles.mts';

/** The apps' sources, by their paths from the repository. */
const SOURCES = new Sources('SOURCES', '');

/** The apps whose own code the census reads. */
const APPS = ['datacube/src', 'query/src', 'studio/src'];

/** The doors' and the run targets' own files (a genquery of DataCube's :run and :doors): not the apps' own code. */
const LAYERED: ReadonlySet<string> = new Set(readFileSync(runfileFromEnv('LAYERED_SRCS'), 'utf8').split('\n')
  .map((l) => l.trim()).filter((l) => l).map((label) => label.replace(/^\/\/([^:]+):/, '$1/')));

const DATACUBE_RUNNER = "a call of DataCube's runner, a DOOR (runner.ts, //datacube:doors: it asks the planner, then runs"
  + ' the answer in a run frame)';
const ENGINE_EXECUTE = "a DOOR call: an Engine's execute -- a server's (pure/v1's execute) or BrowserEngine's, the"
  + ' in-tab engine -- which plans and runs in one request';

/** The census: each app file's plan-and-run sites, by call and count, with its reason. Shrink-only. */
const CENSUS = new Map<string, { readonly calls: Readonly<Record<string, number>>; readonly why: string }>([
  // 2026-11-15 (the C5-T audit's MUST 2, measured once DataCube's doors were a target of their own, and the project
  // database's and the playground's sends had moved into their doors -- engine-client's doors/project-data.ts takes the
  // host's engine, doors/authored.ts runs the playground's statement)
  ['datacube/src/cube.ts', { calls: { 'runner.run(': 5, 'runner.stream(': 2 }, why: `DataCube's cube: ${DATACUBE_RUNNER}` }],
  ['datacube/src/plan.ts', { calls: { 'runner.run(': 1 }, why: `a page's plan: ${DATACUBE_RUNNER}` }],
  ['datacube/src/treeview.ts', { calls: { 'runner.run(': 2 }, why: `the tree view's rows: ${DATACUBE_RUNNER}` }],
  ['query/src/app/cube.ts', { calls: { 'server.execute(': 1 }, why: `Query's cube, its rows: ${ENGINE_EXECUTE}` }],
  ['query/src/app/probe.ts', { calls: { 'engine.execute(': 1 }, why: `Query's probe, a query's first rows: ${ENGINE_EXECUTE}` }],
  ['query/src/app/run.ts', { calls: { 'engine.execute(': 1 }, why: `Query's run, the query's rows: ${ENGINE_EXECUTE}` }],
  ['query/src/ui/results.ts', { calls: { 'engine.execute(': 1 }, why: `Query's results panel, a page of rows: ${ENGINE_EXECUTE}` }],
  ['studio/src/backend/run.ts', { calls: { 'engine.execute(': 1 }, why: `Studio's Run, a function's or a service's rows: ${ENGINE_EXECUTE}` }],
]);

/** A call of a run: the plan runner's, the frame's, or a run, an execute or a stream on a session, an engine, a runner,
 *  an executor, a server, a client or a host -- however the receiver is reached (`engine!.run(`, `rows.engine?.run(`,
 *  `this.#runner.stream(`). The run wall's RUN_CALL, with establish and runAuthored. */
const RUN_CALL = /\b(?:runPlan|runLoad|establish|runAuthored|inRun)\s*\(|\b[\w$]*(?:[sS]ession|[eE]ngine|[rR]unner|[eE]xecutor|[sS]erver|[cC]lient|[hH]ost)\s*[!?]?\s*\??\.\s*(?:run|execute|stream)\s*\(/g;

/** `text`'s plan-and-run sites, by call and count (comments and strings aside). */
export function runSites(text: string): Record<string, number> {
  const code = withoutStrings(withoutComments(text));
  const out: Record<string, number> = {};
  for (const m of code.matchAll(RUN_CALL)) {
    const call = m[0].replace(/\s+/g, '').replace(/^.*?([\w$]+[!?]?\??\.(?:run|execute|stream)\()$/, '$1')
      .replace(/[!?]+\./, '.');
    out[call] = (out[call] ?? 0) + 1;
  }
  return out;
}

/** `text` with its comments blanked (each kept as spaces, so offsets hold). */
function withoutComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '))
    .replace(/(^|[^:'"`\\])\/\/[^\n]*/g, (m, lead: string) => lead + ' '.repeat(m.length - lead.length));
}

/** `code` with each string literal blanked (a template's text too). */
function withoutStrings(code: string): string {
  return code.replace(/'(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*"|`(?:[^`\\]|\\.)*`/g, '""');
}

describe("the apps' plan-and-run sites, outside every door and run target (shrink-only)", () => {
  const appFiles = APPS.flatMap((dir) => SOURCES.under(dir, '.ts')).filter((f) => !f.endsWith('.d.ts'));

  it('reads every app, and leaves out exactly the doors\' and the run targets\' own files', () => {
    for (const dir of APPS) {
      assert.ok(appFiles.filter((f) => f.startsWith(`${dir}/`)).length >= 10, `the census reads too few of ${dir}: the guard is not looking`);
    }
    for (const f of ['datacube/src/runner.ts', 'datacube/src/doors.ts', 'datacube/src/upload.ts', 'datacube/src/headless/connection.ts']) {
      assert.ok(LAYERED.has(f), `${f} is not among the doors' and the run targets' files: ${[...LAYERED].join(', ')}`);
    }
    // the doors run, and only their target keeps them out of the census
    assert.ok(Object.keys(runSites(SOURCES.read('datacube/src/runner.ts'))).length > 0, "runner.ts's runs are not seen");
  });

  it('names each site by file and count, with its reason: a new one fails, a moved one leaves in the same push', () => {
    const found: Record<string, Record<string, number>> = {};
    for (const f of appFiles) {
      if (LAYERED.has(f)) continue;
      const sites = runSites(SOURCES.read(f));
      if (Object.keys(sites).length > 0) found[f] = sites;
    }
    const pinned = Object.fromEntries([...CENSUS].map(([f, { calls }]) => [f, { ...calls }]));
    assert.deepEqual(found, pinned, "an app's own plan-and-run sites (tools/deps/app-runs.test.ts CENSUS): an app plans and"
      + ' runs through a door (engine-client/src/doors/, //datacube:doors), never by itself');
    for (const [f, { why }] of CENSUS) assert.ok(why.trim().length > 20, `${f} is pinned without its reason`);
  });

  it('names a planted site however its receiver is reached, and reads none in a comment or a string', () => {
    const planted = [
      'await engine.run(sql, 0);',
      'await rows.engine?.run(sql, 0);',
      'await this.#runner.stream(q, s, (c) => c);',
      'await session!.execute(input);',
      'await this.host.run(sql);',
      'await client.execute(input);',
      'await runPlan(plan, held);',
      'await runLoad(load, send);',
      'await establish(plan, session);',
      'await runAuthored(plan, on);',
      "await inRun('a run', work);",
      '// engine.run(sql) in a comment',
      "const said = 'runner.run(q)';",
      'engine.running(); planner.plan(q);',
    ].join('\n');
    assert.deepEqual(runSites(planted), {
      'engine.run(': 2, 'runner.stream(': 1, 'session.execute(': 1, 'host.run(': 1, 'client.execute(': 1,
      'runPlan(': 1, 'runLoad(': 1, 'establish(': 1, 'runAuthored(': 1, 'inRun(': 1,
    });
  });
});
