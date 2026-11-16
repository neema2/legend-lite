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
//
// AND NO APP MODULE REACHES A RUN MODULE (the C5-T last round's T1, 2026-11-15, Java's onlyTheDoorsCompose): the apps'
// targets depend on no run layer (tools/deps/ts-layers.txt, TsLayeringTest.APP_LAYERS), but a doors' target carries the
// run side's files with it, so an app module could still import one by its path. Here none does -- by an import of any
// kind (`import type`, a dynamic `import()`, a `typeof import()`, an `export ... from`), or through a re-export of a
// module it may import, followed to its end. What an app needs of the run side it takes from the doors, or as records.
// The census above stays beside it: it reads what an app CALLS on what a door hands it, which no import shows.
//
// 2026-11-15 (the C5-T final audit's MUST 1 and items 6 and 8): the census reads engine-client's own modules too, outside
// its run side and doors -- the records, the planner and the server: a layer's rule says what it IMPORTS, and a module
// that declares an engine's shape itself and calls its run (TypeScript's types are structural) imports nothing of the
// run side. Its calls include the held connection's (`exclusive(`, `.rows(`) and the receivers a database goes by
// (`database`, `db`, `conn`, `held`). The re-export walk reads every package an app's modules import, fails on a module it
// cannot read, and follows an import that a module exports again (`import { X } from ...; export { X }`).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { posix } from 'node:path';

import { runfileFromEnv, Sources } from '../js/runfiles.mts';
import { computedDoors } from './computed-doors.mts';

/** The apps' sources, by their paths from the repository. */
const SOURCES = new Sources('SOURCES', '');

/** The apps whose own code the census reads. */
const APPS = ['datacube/src', 'query/src', 'studio/src'];

/** The engine client's own modules: the census reads those outside its run side and doors (LAYERED) -- its records, its
 *  planner and its server (2026-11-15, the C5-T final audit's MUST 1). */
const ENGINE_CLIENT = 'engine-client/src';

/** The apps' pages: each app's entries (demo/), bundled beside its src/ -- the app's code too, which no rule read until
 *  the C5-T last round's T4 (2026-11-15). */
const PAGES = ['datacube/demo', 'query/demo', 'studio/demo'];

/** The run side's and the doors' own files -- DataCube's :run and :doors, engine-client's :run, :doors, :engine_api and
 *  :node_host (a genquery): they run by design, so the census reads none of them. */
const LAYERED: ReadonlySet<string> = new Set(readFileSync(runfileFromEnv('LAYERED_SRCS'), 'utf8').split('\n')
  .map((l) => l.trim()).filter((l) => l).map((label) => label.replace(/^\/\/([^:]+):/, '$1/')));

/** The planner layers' own files (a genquery): their computed loads are //tools/deps:layers_alone's to name. */
const PLANNER: ReadonlySet<string> = new Set(readFileSync(runfileFromEnv('PLANNER_SRCS'), 'utf8').split('\n')
  .map((l) => l.trim()).filter((l) => l).map((label) => label.replace(/^\/\/([^:]+):/, '$1/')));

/**
 * The computed loads of the apps' and their pages' own modules -- each an import door whose module is chosen at run time
 * (tools/deps/computed-doors.mts) -- by module and door, with the reason each is allowed (2026-11-15, the C5-T review's
 * B5: until then no rule read an app's or a page's). A layer's are //tools/deps:layers_alone's. Shrink-only; a list, as
 * one module may open the same door twice.
 */
const COMPUTED_PAGES: readonly (readonly [string, string])[] = [
  ['datacube/demo/widget-loader.ts import(url)',
    "a notebook cube's own chunk, fetched over the widget's channel and imported from its blob URL (a module from a blob"
      + ' URL resolves no import of its own): its own bundle, never a planner or the run side'],
  ['datacube/demo/widget-loader.ts import(url)',
    "a notebook cube's own module, widget.js, fetched over the widget's channel and imported from its blob URL"],
  ["studio/src/app/app.ts new Worker(new URL('./editor.worker.js', globalThis.location.href), ...)",
    "Monaco's editor worker, from the page's own site (Studio's ./editor.worker.js bundle): the editor's language"
      + ' services, no database (PlannedSqlTest.WORKERS pins it too)'],
];

const DATACUBE_RUNNER = "a call of DataCube's runner, a DOOR (runner.ts, //datacube:doors: it asks the planner, then runs"
  + ' the answer in a run frame)';
const SERVICES_ROWS = "the app's services asked for a model's rows: they hand a planner and DataCube's runner, a DOOR"
  + ' (app/services.ts PlaneService.rows), and send nothing themselves';
const ENGINE_EXECUTE = "a DOOR call: an Engine's execute -- a server's (pure/v1's execute) or BrowserEngine's, the"
  + ' in-tab engine -- which plans and runs in one request';

/** The run layers' own files (a genquery of //engine-client:run and //datacube:run): what no app module reaches. */
const RUN_SRCS: ReadonlySet<string> = new Set(readFileSync(runfileFromEnv('RUN_SRCS'), 'utf8').split('\n')
  .map((l) => l.trim()).filter((l) => l).map((label) => label.replace(/^\/\/([^:]+):/, '$1/')));

/** Each module `text` (read at `file`) names by a relative literal: a static `import` or `export ... from` (a type's
 *  too), a bare `import '...'`, a dynamic `import('...')` and a `typeof import('...')` -- each resolved to its path. A
 *  file that is no module of code (a stylesheet, JSON, an image: an extension of its own) is not one. */
export function importedBy(file: string, text: string): string[] {
  const code = withoutComments(text);
  const specs = [
    ...code.matchAll(/(?:^|[\n;])\s*(?:import|export)\s+(?:type\s+)?(?:[\w*{}\s,$]+?\s+from\s+)?(['"])([^'"\n]+)\1/g),
    ...code.matchAll(/\bimport\s*\(\s*(['"`])([^'"`$\n]+)\1\s*\)/g),
  ].map((m) => m[2]!);
  return specs.filter((spec) => spec.startsWith('.') && isCode(spec)).map((spec) => moduleAt(file, spec));
}

/** Whether a specifier names a module of code: a TypeScript or JavaScript extension, or none. */
function isCode(spec: string): boolean {
  const ext = /\.([\w]+)$/.exec(posix.basename(spec))?.[1];
  return ext === undefined || /^[cm]?[jt]sx?$/.test(ext);
}

/** The module a relative specifier in `file` names, as the compiler and the bundler find it: a `.js` spelling (`.jsx`,
 *  `.mjs`, `.cjs`) or none is the TypeScript module beside it (the C5-T last round's T4, 2026-11-15: an app's
 *  `import ... from '.../duckdb.js'` typechecked and bundled the run module, and this census read another file). */
export function moduleAt(file: string, spec: string): string {
  const path = posix.normalize(posix.join(posix.dirname(file), spec));
  const ext = /\.(?:[cm]?[jt]sx?)$/.exec(path)?.[0];
  if (ext === undefined) return `${path}.ts`;
  return path.slice(0, -ext.length) + ext.replace('j', 't');
}

/** Each module `text` re-exports from, resolved: by `export ... from '...'`, or by an import of it whose binding the
 *  module exports again (`import { X } from '...'; export { X }`, `export default X`; the C5-T final audit's item 6). */
export function reExportedBy(file: string, text: string): string[] {
  const code = withoutComments(text);
  const from = [...code.matchAll(/(?:^|[\n;])\s*export\s+(?:type\s+)?(?:[\w*{}\s,$]+?\s+from\s+)(['"])([^'"\n]+)\1/g)]
    .map((m) => m[2]!);
  // the names the module exports by itself: `export { a, b as c }` (no `from`) and `export default a`
  const exported = new Set<string>([
    ...[...code.matchAll(/(?:^|[\n;])\s*export\s+(?:type\s+)?\{([^}]*)\}(?!\s*from)/g)]
      .flatMap((m) => m[1]!.split(',').map((n) => n.trim().replace(/^type\s+/, '').split(/\s+as\s+/)[0]!.trim())),
    ...[...code.matchAll(/(?:^|[\n;])\s*export\s+default\s+([\w$]+)\s*;/g)].map((m) => m[1]!),
  ].filter((n) => n.length > 0));
  // each import's local bindings, by the module it is from
  const again = [...code.matchAll(/(?:^|[\n;])\s*import\s+(?:type\s+)?([\w*{}\s,$]+?)\s+from\s+(['"])([^'"\n]+)\2/g)]
    .filter((m) => localBindings(m[1]!).some((b) => exported.has(b))).map((m) => m[3]!);
  return [...from, ...again].filter((spec) => spec.startsWith('.') && isCode(spec)).map((spec) => moduleAt(file, spec));
}

/** An import clause's local names: `d`, `* as ns`, `{ a, b as c, type d }`. */
function localBindings(clause: string): string[] {
  const names: string[] = [];
  const braces = /\{([^}]*)\}/.exec(clause);
  if (braces) {
    names.push(...braces[1]!.split(',').map((n) => n.trim().replace(/^type\s+/, '')).filter((n) => n.length > 0)
      .map((n) => n.split(/\s+as\s+/).pop()!.trim()));
  }
  const rest = clause.replace(/\{[^}]*\}/, '');
  const ns = /\*\s+as\s+([\w$]+)/.exec(rest);
  if (ns) names.push(ns[1]!);
  const def = /^\s*([\w$]+)/.exec(rest.replace(/\*\s+as\s+[\w$]+/, ''));
  if (def) names.push(def[1]!);
  return names;
}

/** Each way an app module of `apps` reaches a run module: one it imports, or one a module it imports re-exports, followed
 *  through re-exports to its end (`read` gives a module's text, undefined where none is declared) -- and each module on
 *  the way the walk cannot read, which it names rather than skip (the C5-T final audit's item 6). */
export function runReaches(apps: readonly string[], read: (file: string) => string | undefined, run: ReadonlySet<string>): string[] {
  const out: string[] = [];
  for (const file of apps) {
    for (const imported of new Set(importedBy(file, read(file) ?? ''))) {
      if (run.has(imported)) {
        out.push(`${file} imports ${imported}`);
        continue;
      }
      const seen = new Set<string>([imported]);
      const stack = [[imported]];
      while (stack.length > 0) {
        const path = stack.pop()!;
        const at = path[path.length - 1]!;
        const text = read(at);
        if (text === undefined) {
          out.push(`${file} imports ${path.join(', which re-exports ')}, which no source read holds: the walk cannot follow it`);
          continue;
        }
        for (const next of reExportedBy(at, text)) {
          if (run.has(next)) out.push(`${file} imports ${path.join(', which re-exports ')}, which re-exports ${next}`);
          else if (!seen.has(next)) {
            seen.add(next);
            stack.push([...path, next]);
          }
        }
      }
    }
  }
  return out.sort();
}

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
  // 2026-11-15 (the C5-T final audit's MUST 1 and item 8: the held connection's calls, and engine-client's own modules)
  ['datacube/src/app/datacube-app.ts', { calls: { 'engine.rows(': 1 }, why: SERVICES_ROWS }],
  ['datacube/src/app/join-flow.ts', { calls: { 'engine.rows(': 1 }, why: SERVICES_ROWS }],
  ['datacube/src/app/join-sources.ts', { calls: { 'engine.rows(': 1 }, why: SERVICES_ROWS }],
  ['datacube/src/app/sources.ts', { calls: { 'engine.rows(': 9 }, why: SERVICES_ROWS }],
  ['datacube/src/app/timeseries-together.ts', { calls: { 'engine.rows(': 1 }, why: SERVICES_ROWS }],
  ['datacube/src/headless/grid.ts', { calls: { 'engine.exclusive(': 1 },
    why: "the headless grid's table output: the host's database held for writeTable, a DOOR (table-output.ts), which runs its one plan there" }],
  ['engine-client/src/legend/engine.ts', { calls: { 'server.execute(': 1 },
    why: "legend-engine's execute, the SERVER's door (pure/v1's execute: the server plans and runs in one request), passed on by the server layer's engine" }],
  // the pages (2026-11-15, the C5-T last round's T4)
  ['datacube/demo/remote-harness.ts', { calls: { 'engine.execute(': 1 },
    why: "the remote harness page's query: a plan its planner answered, executed on the engine the tab's door handed" }],
  ['datacube/demo/stress.ts', { calls: { 'engine.execute(': 2 },
    why: "the stress page's queries: plans its planner answered, executed on the engine the tab's door handed" }],
]);

/** A call of a run: the plan runner's, the frame's, or a run, an execute, a stream, a hold (`exclusive`) or a held
 *  connection's rows on a session, an engine, a runner, an executor, a server, a client, a host, a database, a db, a
 *  connection or a held one -- however the receiver is reached (`engine!.run(`, `rows.engine?.run(`,
 *  `this.#runner.stream(`). The run wall's RUN_CALL, with establish and runAuthored; the held connection's calls and the
 *  database's receivers since the C5-T final audit's item 8 (2026-11-15). */
const RUN_CALL = /\b(?:runPlan|runLoad|establish|runAuthored|inRun)\s*\(|\b[\w$]*(?:[sS]ession|[eE]ngine|[rR]unner|[eE]xecutor|[sS]erver|[cC]lient|[hH]ost|[dD]atabase|[dD]b|[cC]onn|[hH]eld)\s*[!?]?\s*\??\.\s*(?:run|execute|stream|exclusive|rows)\s*\(/g;

/** `text`'s plan-and-run sites, by call and count (comments and strings aside). */
export function runSites(text: string): Record<string, number> {
  const code = withoutStrings(withoutComments(text));
  const out: Record<string, number> = {};
  for (const m of code.matchAll(RUN_CALL)) {
    const call = m[0].replace(/\s+/g, '').replace(/^.*?([\w$]+[!?]?\??\.(?:run|execute|stream|exclusive|rows)\()$/, '$1')
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
  const appFiles = [...APPS, ...PAGES].flatMap((dir) => SOURCES.under(dir, '.ts')).filter((f) => !f.endsWith('.d.ts'));
  /** What the census reads: the apps, their pages, and engine-client's own modules outside its run side and doors. */
  const censused = [...appFiles, ...SOURCES.under(ENGINE_CLIENT, '.ts').filter((f) => !f.endsWith('.d.ts'))]
    .filter((f) => !LAYERED.has(f));

  it("opens no import door whose module is chosen at run time but those named: an app's or a page's own module (B5)", () => {
    // the apps' and the pages' own modules, a layer's aside (each layer's are //tools/deps:layers_alone's)
    const own = appFiles.filter((f) => !LAYERED.has(f) && !PLANNER.has(f));
    assert.ok(PLANNER.has('datacube/src/lite-planner.ts'), 'PLANNER_SRCS does not name the planner: the guard is not looking');
    for (const [key, why] of COMPUTED_PAGES) {
      assert.ok(own.includes(key.slice(0, key.indexOf(' '))), `${key} is not an app's or a page's own module`);
      assert.ok(why.length > 20, `${key} is named without its reason`);
    }
    const found = own.flatMap((f) => computedDoors(SOURCES.read(f)).map((d) => `${f} ${d}`)).sort();
    assert.deepEqual(found, COMPUTED_PAGES.map(([key]) => key).sort(), "an app's or a page's own module chooses a module"
      + ' at run time: load it through a door, or name it with its reason in COMPUTED_PAGES');
  });

  it('reads every app, and leaves out exactly the doors\' and the run targets\' own files', () => {
    for (const dir of APPS) {
      assert.ok(appFiles.filter((f) => f.startsWith(`${dir}/`)).length >= 10, `the census reads too few of ${dir}: the guard is not looking`);
    }
    for (const f of ['datacube/demo/boot.ts', 'datacube/demo/main.ts', 'query/demo/main.ts', 'studio/demo/main.ts']) {
      assert.ok(appFiles.includes(f), `the census does not read the page ${f}: the guard is not looking`);
    }
    for (const f of ['datacube/src/runner.ts', 'datacube/src/doors.ts', 'datacube/src/upload.ts', 'datacube/src/headless/connection.ts',
      'engine-client/src/duckdb.ts', 'engine-client/src/run-frame.ts', 'engine-client/src/doors/tab-duckdb.ts', 'engine-client/src/node-require.ts']) {
      assert.ok(LAYERED.has(f), `${f} is not among the doors' and the run targets' files: ${[...LAYERED].join(', ')}`);
    }
    // engine-client's records, planner and server are read (the C5-T final audit's MUST 1)
    for (const f of ['engine-client/src/legend/engine.ts', 'engine-client/src/legend/wasm-grammar.ts', 'engine-client/src/plan-guard.ts']) {
      assert.ok(censused.includes(f), `the census does not read ${f}: the guard is not looking`);
    }
    // the doors run, and only their target keeps them out of the census
    assert.ok(Object.keys(runSites(SOURCES.read('datacube/src/runner.ts'))).length > 0, "runner.ts's runs are not seen");
  });

  it('names each site by file and count, with its reason: a new one fails, a moved one leaves in the same push', () => {
    const found: Record<string, Record<string, number>> = {};
    for (const f of censused) {
      const sites = runSites(SOURCES.read(f));
      if (Object.keys(sites).length > 0) found[f] = sites;
    }
    const pinned = Object.fromEntries([...CENSUS].map(([f, { calls }]) => [f, { ...calls }]));
    assert.deepEqual(found, pinned, "an app's own plan-and-run sites (tools/deps/app-runs.test.ts CENSUS): an app plans and"
      + ' runs through a door (engine-client/src/doors/, //datacube:doors), never by itself');
    for (const [f, { why }] of CENSUS) assert.ok(why.trim().length > 20, `${f} is pinned without its reason`);
  });

  it('no app module reaches a run module: by an import of any kind, or through a re-export of one it imports', () => {
    assert.ok(RUN_SRCS.has('engine-client/src/duckdb.ts') && RUN_SRCS.has('datacube/src/headless/connection.ts'),
      `the run layers' files are not read: ${[...RUN_SRCS].join(', ')}`);
    const apps = appFiles.filter((f) => !LAYERED.has(f));
    const read = (f: string): string | undefined => (SOURCES.has(f) ? SOURCES.read(f) : undefined);
    // none, a page's boot included (2026-11-15, the C5-T fold-in: the three page boots, DataCube's demo sample and
    // Query's warehouse seed and sign-in, went through doors, and their list, PAGE_REACHES, went with them)
    assert.deepEqual(runReaches(apps, read, RUN_SRCS), [], 'an app module reaches the run side past its doors: take what'
      + ' it needs from a door (engine-client/src/doors/, //datacube:doors), or its shape from the records');
  });

  it('names a planted app module that imports a run module, by any import, or through a chain of re-exports', () => {
    const [imp, exp, from] = [['im', 'port'].join(''), ['ex', 'port'].join(''), ['fr', 'om'].join('')];
    const files = new Map<string, string>([
      ['datacube/src/planted.ts', [
        `${imp} type { QueryEngine } ${from} '../../engine-client/src/engine.ts';`,
        `type Driver = typeof ${imp}('../../engine-client/src/warehouse.ts');`,
        `const late = () => ${imp}('../../engine-client/src/plan-runner.ts');`,
        `${imp} { rows } ${from} '../../engine-client/src/doors/planted-door.ts';`,
        `${exp} { held } ${from} './planted-records.ts';`,
        // a comment's import names a run module no other line names: absent from the answer (the C5-T final audit's item 9)
        `// ${imp} { putFile } ${from} '../../engine-client/src/model-data.ts';`,
        // the module by its `.js` spelling, and by none: the compiler and the bundler load the `.ts` beside it
        `${imp} { DuckDbEngine as Spelled } ${from} '../../engine-client/src/duckdb.js';`,
        `${exp} * as unspelled ${from} '../../engine-client/src/arrow-ipc';`,
        // an import exported again, by name and by default; and a module no source read holds (the final audit's item 6)
        `${imp} { bridged } ${from} './planted-bridge.ts';`,
        `${imp} defaulted ${from} './planted-default.ts';`,
        `${imp} { lost } ${from} './planted-lost.ts';`,
      ].join('\n')],
      ['datacube/src/planted-bridge.ts', `${imp} type { HeldConnection as Held } ${from} '../../engine-client/src/warehouse.ts';\n`
        + `${exp} type { Held as bridged };`],
      ['datacube/src/planted-default.ts', `${imp} { runLoad } ${from} '../../engine-client/src/model-data.ts';\n${exp} default runLoad;`],
      ['engine-client/src/doors/planted-door.ts', `${exp} { rows } ${from} './planted-relay.ts';`],
      ['engine-client/src/doors/planted-relay.ts', `${exp} type { rows } ${from} '../duckdb.ts';`],
      ['datacube/src/planted-records.ts', `${exp} const held = 1;`],
    ]);
    const run = new Set(['engine-client/src/engine.ts', 'engine-client/src/warehouse.ts', 'engine-client/src/plan-runner.ts',
      'engine-client/src/duckdb.ts', 'engine-client/src/arrow-ipc.ts', 'engine-client/src/model-data.ts']);
    assert.deepEqual(runReaches(['datacube/src/planted.ts'], (f) => files.get(f), run), [
      'datacube/src/planted.ts imports datacube/src/planted-bridge.ts, which re-exports engine-client/src/warehouse.ts',
      'datacube/src/planted.ts imports datacube/src/planted-default.ts, which re-exports engine-client/src/model-data.ts',
      'datacube/src/planted.ts imports datacube/src/planted-lost.ts, which no source read holds: the walk cannot follow it',
      'datacube/src/planted.ts imports engine-client/src/arrow-ipc.ts',
      'datacube/src/planted.ts imports engine-client/src/doors/planted-door.ts, which re-exports'
        + ' engine-client/src/doors/planted-relay.ts, which re-exports engine-client/src/duckdb.ts',
      'datacube/src/planted.ts imports engine-client/src/duckdb.ts',
      'datacube/src/planted.ts imports engine-client/src/engine.ts',
      'datacube/src/planted.ts imports engine-client/src/plan-runner.ts',
      'datacube/src/planted.ts imports engine-client/src/warehouse.ts',
    ]);
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
      // the held connection's calls and a database's receivers (the C5-T final audit's item 8)
      'await engine.exclusive((held) => held.rows(sql, []));',
      'await database.engine.run(sql, 0);',
      'await this.#db.run(sql);',
      'await conn!.execute(q);',
      'await held?.rows(sql, []);',
      'await table.rows(); grid.exclusive();',
      '// engine.run(sql) in a comment',
      "const said = 'runner.run(q)';",
      'engine.running(); planner.plan(q);',
    ].join('\n');
    assert.deepEqual(runSites(planted), {
      'engine.run(': 3, 'runner.stream(': 1, 'session.execute(': 1, 'host.run(': 1, 'client.execute(': 1,
      'runPlan(': 1, 'runLoad(': 1, 'establish(': 1, 'runAuthored(': 1, 'inRun(': 1,
      'engine.exclusive(': 1, 'held.rows(': 2, 'db.run(': 1, 'conn.execute(': 1,
    });
  });
});
