// A TYPESCRIPT LAYER ALONE (C5-T, 2026-11-15: the TypeScript split of plan and run, the rule that replaces the interim
// run wall, datacube/test/run-wall.test.ts). One layer's whole closure -- its files and its deps' files, exactly what
// Bazel puts in this test's runfiles for it (LAYER), nothing else -- is copied into a fresh directory, and the pinned
// strict TypeScript compiler (DataCube's tsconfig's options) checks it there. A module of the layer that imports a module
// of a layer it may not reach -- `import type` and a literal `import()` included -- names a file that is not there, and
// fails by name (TS2307): the build's own dependency rule, as //core:exec's deps are for Java. The layers' deps are
// pinned by tools/deps/ts-layers.txt (//tools/deps:ts_layering_test): a run layer reaches records and npm alone, a
// planner nothing that executes.
//
// Beside it, on the layer's closure:
// - no module of the layers it must never hold is in it (NOT_LAYER_SRCS, a genquery of their files; for a run layer the
//   planner's, the server's and the doors' -- the orchestrator's note 4, 2026-11-15 -- and no app's);
// - a planted module of the layer that imports one of them (PLANT_AT, PLANT_IMPORTS) fails by name: the bite;
// - on a RUN layer, NO computed specifier (the orchestrator's note 2; 2026-11-15, the C5-T fold-in: no list): an
//   `import()`, a `require()`, an `import.meta.resolve()`, an `importScripts()`, a worker's module or a `new URL(...,
//   import.meta.url)` whose specifier is not a literal chooses its module at run time, where no compiler follows it.
//   Loading the deployment's configured vendor binaries is a door's wiring, and starting the planner's own worker and
//   runtime from the page's URL a planner's: on a DOOR, PLANNER or SERVER layer, exactly the loads named in its closure
//   with their reason (COMPUTED_DOORS, COMPUTED_PLANNER), each a door's or a planner's by the target that holds its
//   module, and each read where its module is the layer's own (no dead row); the run side takes the loaded object,
//   never a loader (the C5-T review's S1, B1, B2, 2026-11-15);
// - on a RUN or PLANNER layer, each of those doors whose specifier IS a literal names no module of a layer it must never reach,
//   nor any module outside its closure: the compiler resolves an `import()`, never a worker's module, a URL's, a
//   `require()`'s or an `importScripts()`'s (the C5-T audit's item 6, 2026-11-15);
// - on a RUN layer, no function, method, member or interface TAKES anything that answers a planner's statement, plan or
//   planned record -- a source of the planner's answers, an inline object type's method, an alias, a re-export or an
//   import's rename followed (carried from the run wall's signature rule: the C5-T audit's item 4, 2026-11-15, the
//   proof of step 2's split, that a run function takes the planner's answer and never its source);
// - on every layer, no `@ts-nocheck`, `@ts-ignore` or `@ts-expect-error`: each would hush the compiler's TS2307 on an
//   import of a layer the layer may not reach, while a bundler still followed it (the C5-T audit's item 5);
// - on a RUN or a DOOR layer, no explicit `any` (`: any`, `as any`, `<any>`, `any[]`, any type written `any`): one would
//   carry an untyped string past the PlannedSql brand to a send (the C5-T last round's T2, 2026-11-15; none today, so no
//   list: a new one fails).
// The door layers (2026-11-15, the C5-T last round's T2) are checked alone too: they reach every layer below them and no
// app (an app module named by a planted import fails by name). So are the server layer -- legend-engine's clients, which
// reach no engine's API, run layer or door -- and the engine's API itself, a run-side layer (2026-11-15, the C5-T final
// audit's MUST 1).
//
// It reads the runfiles tree because that tree IS the layer's closure: what this test's BUILD target declares.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, posix, relative, sep } from 'node:path';
import { after, describe, it } from 'node:test';

import { runfile, runfileFromEnv, runfilesFromEnv, runfilesRoot } from '../js/runfiles.mts';
import { blankStrings, CHAIN, closing, computedDoors, RECEIVER, withoutComments } from './computed-doors.mts';

/** The main repository's directory in the runfiles tree. */
const MAIN = join(runfilesRoot(), '_main');

/** What the BUILD target says of the layer. */
const env = (name: string): string => {
  const value = process.env[name];
  if (value === undefined) throw new Error(`${name} is not set: the test's BUILD target names it`);
  return value;
};
const LAYER = env('LAYER');
const RUN_LAYER = env('KIND') === 'run';
const DOOR_LAYER = env('KIND') === 'doors';

/** The doors' computed specifiers, by module and door, with the reason each is allowed: the deployment's configured
 *  vendor binaries, loaded by their doors (the orchestrator's note 2, 2026-11-15, carried from the run wall's
 *  COMPUTED_DOORS of the final audit of 9af16fbe58's T4; 2026-11-15, the C5-T fold-in: moved out of the run layer into
 *  the doors, which hand the run side the loaded object). Shrink-only; each a door module's. */
const COMPUTED_DOORS = new Map([
  ["engine-client/src/doors/tab-duckdb.ts new Worker(vendorAsset(vendor, 'duckdb-browser-eh.worker.js'))",
    "DuckDB's own worker, started by its door from the page's vendor directory, once the browser is known to run it:"
      + ' the run side instantiates DuckDB in the worker it is handed'],
  // 2026-11-16 (claude/deephaven-3-browser, the merge of main 4dfbb5b7): out of the run layer's live reader
  ['engine-client/src/doors/deephaven-api.ts import(url)',
    "Deephaven's JS library, served by the Deephaven server at its own version (never bundled: DH11), loaded by its"
      + ' door from the server it reads: the run side takes the loaded library'],
  ['engine-client/src/doors/warehouse.ts import(runtimeUrl)',
    "TeaVM's runtime for the warehouse's wire binding (vendor/warehouse/, //warehouse:sqlapi_page), loaded by its door:"
      + " the run side takes the loaded binding (2026-11-15, the C5-T fold-in)"],
]);

/** The planner layers' computed specifiers, by module and door, with the reason each is allowed: the planner's own
 *  worker and TeaVM runtime, from the page's URL (2026-11-15, the C5-T review's S1: until then the planner kind was not
 *  read, and two of these were pinned nowhere). Shrink-only; each a planner module's. */
const COMPUTED_PLANNER = new Map([
  ['engine-client/src/legend/wasm-grammar.ts new Worker(workerUrl, ...)',
    "the planner's worker (//engine-client:planner_worker), from the URL the page serves it at: it plans and holds no database"],
  ['engine-client/src/legend/planner-worker.ts import(`${base}wasm-gc-module-runtime.js`)',
    "the planner worker's TeaVM runtime, from the page's vendor folder (//wasm:planner): the planner's own module"],
  ['datacube/src/lite-planner.ts new Worker(this.#options.workerUrl!, ...)',
    "DataCube's planner on its worker (the same bundle), from the URL the page gives: it plans and holds no database"],
  ['datacube/src/lite-planner.ts import(runtimeUrl)',
    "DataCube's in-thread planner's TeaVM runtime, from its asset folder alone -- no loader handed in (its loadRuntime"
      + " option deleted, the C5-T review's S1, as T3 took warehouse.ts's)"],
]);

/** A named load's module: the part of its key before the door. */
const fileOf = (key: string): string => key.slice(0, key.indexOf(' '));

/** A genquery's labels, by their paths from the repository. */
const filesOf = (name: string): ReadonlySet<string> => new Set(readFileSync(runfileFromEnv(name), 'utf8').split('\n')
  .map((l) => l.trim()).filter((l) => l).map((label) => label.replace(/^\/\/([^:]+):/, '$1/')));

/** Every .ts file of the main repository in the runfiles, by its path from the repository (`/`-separated). */
function runfilesSources(dir = MAIN): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    if (name === 'node_modules' || name.startsWith('.')) continue;
    const path = join(dir, name);
    if (statSync(path).isDirectory()) out.push(...runfilesSources(path));
    else if (name.endsWith('.ts') && !name.endsWith('.d.ts')) out.push(relative(MAIN, path).split(sep).join('/'));
  }
  return out.sort();
}

/** The pinned compiler (DataCube's typescript, 7.0.2): its package link's bin/tsc. */
function compiler(): string {
  const links = runfilesFromEnv('TYPESCRIPT').filter((p) => p.split(sep).join('/').endsWith('/datacube/node_modules/typescript'));
  assert.equal(links.length, 1, `TYPESCRIPT names ${links.length} links of the package, not one`);
  return join(links[0]!, 'bin', 'tsc');
}

/** The layer's closure: every source but this test's own (the tools: its entry point, the runfiles helper). */
const CLOSURE = runfilesSources().filter((f) => !f.startsWith('tools/'));

/** The packages the closure's files are in: each `<package>/src`, what the compiler is given. */
const ROOTS = [...new Set(CLOSURE.map((f) => f.slice(0, f.indexOf('/src/') + '/src'.length)))].sort();

const made: string[] = [];
after(() => {
  for (const d of made) rmSync(d, { recursive: true, force: true });
});

/** The closure copied into a fresh directory, with `planted` files added, and the strict compiler's answer there. */
function typecheck(planted: ReadonlyMap<string, string> = new Map()): { readonly status: number | null; readonly output: string } {
  const root = mkdtempSync(join(tmpdir(), 'layer-alone-'));
  made.push(root);
  // each file's TEXT, never the runfiles' link to it: a link's real directory holds the files the closure does not
  const write = (f: string, text: string): void => {
    mkdirSync(dirname(join(root, f)), { recursive: true });
    writeFileSync(join(root, f), text);
  };
  for (const f of CLOSURE) write(f, readFileSync(join(MAIN, f), 'utf8'));
  for (const [f, text] of planted) write(f, text);
  // each package's npm packages, as Bazel linked them for it (a junction, so Windows needs no privilege)
  for (const pkg of ROOTS.map((r) => r.slice(0, -'/src'.length))) {
    if (existsSync(join(MAIN, pkg, 'node_modules'))) symlinkSync(join(MAIN, pkg, 'node_modules'), join(root, pkg, 'node_modules'), 'junction');
  }
  writeFileSync(join(root, 'tsconfig.json'), JSON.stringify({
    // DataCube's tsconfig's options (datacube/tsconfig.json: the one strict check every app's sources pass)
    compilerOptions: {
      target: 'ES2023', module: 'NodeNext', moduleResolution: 'nodenext', lib: ['ES2023', 'DOM'],
      types: ['node'], typeRoots: [runfile('_main/datacube/node_modules/@types')],
      strict: true, noUncheckedIndexedAccess: true, exactOptionalPropertyTypes: true, noImplicitOverride: true,
      noFallthroughCasesInSwitch: true, noImplicitReturns: true, noUnusedLocals: true, noUnusedParameters: true,
      isolatedModules: true, verbatimModuleSyntax: true, allowImportingTsExtensions: true, erasableSyntaxOnly: true,
      noEmit: true, skipLibCheck: true, forceConsistentCasingInFileNames: true,
    },
    include: ROOTS,
  }));
  writeFileSync(join(root, 'package.json'), JSON.stringify({ type: 'module' }));
  const r = spawnSync(process.execPath, [compiler(), '-p', join(root, 'tsconfig.json'), '--pretty', 'false'],
    { cwd: root, encoding: 'utf8', timeout: 120_000 });
  return { status: r.status, output: `${r.stdout ?? ''}${r.stderr ?? ''}${r.error ? String(r.error) : ''}` };
}

describe(`${LAYER} alone: the layer with its deps and nothing else`, () => {
  it("holds its own files and its deps', and no module of a layer it must never reach", () => {
    // looking: the layer's own files the BUILD target names, each in the closure, and the closure no smaller
    const expected = env('EXPECT_FILES').split(' ').filter((f) => f);
    assert.ok(expected.length > 0, 'EXPECT_FILES names none of the layer\'s files: the guard is not looking');
    for (const f of expected) assert.ok(CLOSURE.includes(f), `${f} is not in ${LAYER}'s closure: ${CLOSURE.join(', ')}`);
    const floor = Number(env('MIN_FILES'));
    assert.ok(CLOSURE.length >= floor, `${LAYER}'s closure is ${CLOSURE.length} files, under ${floor}: the guard is not looking`);
    const never = readFileSync(runfileFromEnv('NOT_LAYER_SRCS'), 'utf8').split('\n').map((l) => l.trim()).filter((l) => l)
      .map((label) => label.replace(/^\/\/([^:]+):/, '$1/'));
    assert.ok(never.length > 0, 'NOT_LAYER_SRCS names no file: the guard is not looking');
    assert.deepEqual(CLOSURE.filter((f) => never.includes(f)), [], `a module of a layer ${LAYER} must never reach is in its closure`);
  });

  it('typechecks alone, strict: every module it imports is in its closure', () => {
    const { status, output } = typecheck();
    assert.equal(status, 0, `${LAYER} does not typecheck alone:\n${output}`);
  });

  it('a planted module of the layer that reaches a layer it may not fails by the module it names', () => {
    // written in pieces: the repository's import scanners read this file's text
    const [imp, from] = [['im', 'port'].join(''), ['fr', 'om'].join('')];
    const specs = env('PLANT_IMPORTS').split(' ').filter((s) => s);
    assert.ok(specs.length >= 2, 'PLANT_IMPORTS names too few modules to plant');
    const [first, ...rest] = specs;
    const planted = new Map([[env('PLANT_AT'), [
      // as a type, as a value, and opened at run time by a literal
      `${imp} type * as typed ${from} '${first}';`,
      ...rest.slice(0, -1).map((s, i) => `${imp} * as value${i} ${from} '${s}';`),
      `export const kept = (t: typeof typed | undefined): unknown => [t, ${rest.slice(0, -1).map((_, i) => `value${i}`).join(', ')}];`,
      `export const door = async (): Promise<unknown> => ${imp}('${rest[rest.length - 1]}');`,
    ].join('\n')]]);
    const { status, output } = typecheck(planted);
    assert.notEqual(status, 0, `the planted module typechecked: ${LAYER} is not alone\n${output}`);
    for (const spec of specs) {
      assert.ok(new RegExp(`planted.*TS2307.*'${spec.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&')}'`).test(output),
        `the planted import of ${spec} is not refused by name:\n${output}`);
    }
  });

  it('opens no import door whose module is chosen at run time but those named: a run layer none (note 2; the review\'s S1)', () => {
    // each named load is a door's or a planner's, by the target that holds its module (B1): a run layer can never be
    // named, so never grow one
    const doors = filesOf('DOOR_SRCS');
    const planners = filesOf('PLANNER_SRCS');
    assert.ok(doors.size > 0 && planners.size > 0, 'DOOR_SRCS or PLANNER_SRCS names no file: the guard is not looking');
    for (const d of COMPUTED_DOORS.keys()) assert.ok(doors.has(fileOf(d)), `${d} is not a door's`);
    for (const d of COMPUTED_PLANNER.keys()) assert.ok(planners.has(fileOf(d)), `${d} is not a planner's`);
    const named = [...COMPUTED_DOORS.keys(), ...COMPUTED_PLANNER.keys()];
    const found = CLOSURE.flatMap((f) => computedDoors(readFileSync(join(MAIN, f), 'utf8')).map((d) => `${f} ${d}`)).sort();
    // no dead row (B2): each named load of a module this layer holds as its own is read here
    const own = filesOf('OWN_SRCS');
    for (const d of named.filter((k) => own.has(fileOf(k)))) assert.ok(found.includes(d), `${d} is named, and read nowhere`);
    // a run layer: none; a door, planner or server layer: the named loads of the modules in its closure, each exactly
    assert.deepEqual(found, RUN_LAYER ? [] : named.filter((d) => CLOSURE.includes(fileOf(d))).sort());
  });

  it("names no module it may not reach by a literal worker's, URL's, require's or importScripts' specifier (a run or planner layer)", (t) => {
    if (!RUN_LAYER && env('KIND') !== 'planner') {
      t.skip('a door or server layer: the run and planner layers hold this rule');
      return;
    }
    const files = closureTexts();
    const found = CLOSURE.flatMap((f) => literalDoorBreaches(f, files.get(f)!, files, NEVER));
    assert.deepEqual(found, []);
  });

  it("names a planted literal specifier of a module the layer may not reach, by its door", () => {
    const [imp, req, meta] = [['im', 'port'].join(''), ['re', 'quire'].join(''), ['im', 'port.meta'].join('')];
    const [first, second, third] = env('PLANT_IMPORTS').split(' ');
    const planted = [
      `export const a = () => new Worker('${first}', { type: 'module' });`,
      `export const b = () => new Worker(new URL('${second}', ${meta}.url));`,
      `export const c = () => ${meta}.resolve('${third}');`,
      `export const d = () => self.importScripts('${first}');`,
      `export const e = () => ${req}('${second}');`,
      `export const f = () => ${imp}('./nowhere-at-all.ts');`,
      // read, and let be: a URL that is no module's, a package's specifier
      `export const g = () => new URL('https://example.com/x.ts');`,
      `export const h = () => ${req}('node:fs');`,
    ].join('\n');
    const at = env('PLANT_AT');
    const found = literalDoorBreaches(at, planted, closureTexts(), NEVER).map((b) => b.slice(at.length + 1));
    assert.deepEqual(found, [
      `new Worker('${first}'): ${resolved(at, first!)}, a module of a layer it may not reach`,
      `new URL('${second}', import.meta.url): ${resolved(at, second!)}, a module of a layer it may not reach`,
      `import.meta.resolve('${third}'): ${resolved(at, third!)}, a module of a layer it may not reach`,
      `importScripts('${first}'): ${resolved(at, first!)}, a module of a layer it may not reach`,
      `require('${second}'): ${resolved(at, second!)}, a module of a layer it may not reach`,
      `import('./nowhere-at-all.ts'): ${resolved(at, './nowhere-at-all.ts')}, a module outside its closure`,
    ]);
  });

  it("no run signature takes anything that answers a planner's statement, plan or planned record (a run layer)", (t) => {
    if (!RUN_LAYER) {
      t.skip('a planner layer: the run layers hold this rule');
      return;
    }
    const files = closureTexts();
    // looking: the closure's records name the planned records the rule reads (relation-type.ts's Plan)
    assert.ok(plannedRecords(files).has('Plan'), `no planned record read: ${[...plannedRecords(files)].join(', ')}`);
    assert.deepEqual(signatureBreaches(files, CLOSURE), []);
  });

  it("names a planted run signature that takes a source of the planner's answers, however spelled", () => {
    const files = closureTexts();
    const [at, again] = ['engine-client/src/planted-signatures.ts', 'engine-client/src/planted-renamed.ts'];
    const [imp, exp, from] = [['im', 'port'].join(''), ['ex', 'port'].join(''), ['fr', 'om'].join('')];
    files.set(again, `${exp} type { PlannedSql as Statement } ${from} './planned-sql.ts';\n`);
    files.set(at, `${imp} type { PlannedSql as Sql, PlanText } ${from} './planned-sql.ts';
${imp} type { Statement } ${from} './planted-renamed.ts';
${imp} type { Plan } ${from} './relation-type.ts';
interface PlantedSource { plan(query: string): Promise<readonly Sql[]> }
type PlantedMaker = () => Promise<Statement>;
interface PlantedHolder { readonly plan: Plan; readonly at: number }
${exp} function plantedSource(source: PlantedSource): void { void source; }
${exp} function plantedInline(source: { load(): Promise<PlanText> }): void { void source; }
${exp} function plantedAlias(make: PlantedMaker): void { void make; }
${exp} function plantedRenamed(make: () => AsyncIterable<Statement>): void { void make; }
${exp} function plantedHeld(make: () => Promise<readonly PlantedHolder[]>): void { void make; }
const plantedArrow = (make: (q: string) => Promise<Plan>): void => { void make; };
${exp} { plantedArrow as plantedPlan };
${exp} class PlantedRunner {
  ask(source: { plan(): Sql }): void { void source; }
  run(sql: Sql, plan: PlanText, held: PlantedHolder, all: readonly Plan[]): void { void sql; void plan; void held; void all; }
  private hidden(source: PlantedSource): void { void source; }
}
${exp} interface PlantedSession { readonly source: PlantedSource; run(sql: Sql): Promise<void> }
`);
    // a statement, a plan or a record holding one TAKEN is the run's own work (PlantedRunner.run): not named
    assert.deepEqual(signatureBreaches(files, [at]), [
      `${at} PlantedRunner.ask: takes something that answers a planner's statement or plan`,
      `${at} PlantedSession.source: takes PlantedSource, which answers a planner's statement or plan`,
      `${at} plantedAlias: takes PlantedMaker, which answers a planner's statement or plan`,
      `${at} plantedArrow: takes something that answers a planner's statement or plan`,
      `${at} plantedHeld: takes something that answers a planner's statement or plan`,
      `${at} plantedInline: takes something that answers a planner's statement or plan`,
      `${at} plantedRenamed: takes something that answers a planner's statement or plan`,
      `${at} plantedSource: takes PlantedSource, which answers a planner's statement or plan`,
    ]);
  });

  it('writes no explicit any: none reaches a send past the brand (a run or a door layer)', (t) => {
    if (!RUN_LAYER && !DOOR_LAYER) {
      t.skip('a planner layer: the run and door layers hold this rule');
      return;
    }
    const files = closureTexts();
    assert.deepEqual(CLOSURE.flatMap((f) => explicitAnys(f, files.get(f)!)), []);
  });

  it('names a planted explicit any, however it is written, and reads none in a name, a comment or a string', () => {
    const planted = [
      'export const a = (x: any): void => { void x; };',
      'export const b = (x: unknown) => x as any;',
      'export const c = (x: unknown) => <any>x;',
      'export const d: any[] = [];',
      'export const e: Record<string, any> = {};',
      'export const f = (x: Promise<Array<any>>) => x;',
      // in a template's substitution, nested, after a template's lines (line 9), and after a regex holding a quote
      'export const g = (x: unknown) => `a ${`b ${(x as any).s}`} c`;',
      'export const h = `one',
      'two ${1} three`;',
      "export const i = (x: unknown): any => String(x).replace(/'/g, \"''\");",
      // read, and let be: a member named any, a call of one, a word in a comment, a string, a template or a regex
      'export const j = (s: { any(): boolean }) => s.any();',
      'export const k = AbortSignal.any([]);',
      "// any here is a word\nexport const l = 'any' + `any ${'any'}` + /any/.source;",
    ].join('\n');
    assert.deepEqual(explicitAnys('planted.ts', planted).map((l) => l.split(':')[1]), ['1', '2', '3', '4', '5', '6', '7', '10']);
  });

  it('no module of the layer hushes the compiler: no @ts-nocheck, @ts-ignore or @ts-expect-error', () => {
    const files = closureTexts();
    assert.deepEqual(CLOSURE.flatMap((f) => suppressions(f, files.get(f)!)), []);
    const [nocheck, ignore, expect] = ['@ts-' + 'nocheck', '@ts-' + 'ignore', '@ts-' + 'expect-error'];
    assert.deepEqual(suppressions('planted.ts', `// ${nocheck}\nconst a = 1;\n/* ${ignore} */ const b = 2;\n// ${expect} why\n`), [
      `planted.ts:1: // ${nocheck}`, `planted.ts:3: /* ${ignore} */ const b = 2;`, `planted.ts:4: // ${expect} why`,
    ]);
  });

  it('names a planted door whose module is chosen at run time, and reads a literal as no door', () => {
    const [imp, req, meta] = [['im', 'port'].join(''), ['re', 'quire'].join(''), ['im', 'port.meta'].join('')];
    const planted = [
      `export const a = (name: string) => ${imp}(\`./legend/\${name}.ts\`);`,
      `export const b = (spec: string) => ${imp}(/* @vite-ignore */ spec);`,
      `export const c = (spec: string) => ${req}(spec);`,
      `export const d = (spec: string) => ${meta}.resolve(spec);`,
      `export const e = (base: string) => new Worker(new URL(\`\${base}w.js\`, ${meta}.url));`,
      `export const f = (spec: string) => new URL(spec, ${meta}.url);`,
      `export const g = (spec: string) => self.importScripts('./a.js', spec);`,
      `export const h = (spec: string) => new globalThis.Worker(spec);`,
      // read, not computed: a literal, a template with no substitution, a URL that is no module's
      `export const i = () => ${imp}(\`./legend/planner-answer.ts\`);`,
      `export const j = () => ${meta}.resolve('./legend/planner-protocol.ts');`,
      `export const k = (spec: string) => new URL(spec);`,
      `export const l = () => new Worker(new URL('./legend/planner-worker.ts', ${meta}.url), { type: 'module' });`,
    ].join('\n');
    assert.deepEqual(computedDoors(planted), [
      `${imp}(\`./legend/\${name}.ts\`)`,
      `${imp}(spec)`,
      `${req}(spec)`,
      `${meta}.resolve(spec)`,
      `new Worker(new URL(\`\${base}w.js\`, ${meta}.url))`,
      'new URL(`${base}w.js`, ...)',
      'new URL(spec, ...)',
      "self.importScripts('./a.js', spec)",
      'new globalThis.Worker(spec)',
    ]);
  });
});

// ---- the closure as text, and the layers it may never reach ----

/** Each closure file's text, by its path. */
function closureTexts(): Map<string, string> {
  return new Map(CLOSURE.map((f) => [f, readFileSync(join(MAIN, f), 'utf8')]));
}

/** The files of the layers this one must never reach (NOT_LAYER_SRCS), by their paths. */
const NEVER: ReadonlySet<string> = new Set(readFileSync(runfileFromEnv('NOT_LAYER_SRCS'), 'utf8').split('\n')
  .map((l) => l.trim()).filter((l) => l).map((label) => label.replace(/^\/\/([^:]+):/, '$1/')));

/** `spec`, read in `file`, as a path from the repository. */
function resolved(file: string, spec: string): string {
  return posix.normalize(posix.join(posix.dirname(file), spec));
}

// ---- literal specifiers outside an import statement (the C5-T audit's item 6) ----

/** Each door `text` opens by a LITERAL specifier that names a module the layer may not reach: a worker's module, a
 *  `new URL(..., import.meta.url)`'s, an `import.meta.resolve()`'s, an `importScripts()`'s (each argument), a
 *  `require()`'s or an `import()`'s -- a relative one resolved, and refused when it is a module of a layer it must never
 *  reach, or a module (.ts, .mts, .js, .mjs, .cjs) outside its closure. A package's specifier, or a URL no module's, is
 *  let be: the compiler and the deps' pins hold those. */
function literalDoorBreaches(file: string, text: string, files: ReadonlyMap<string, string>, never: ReadonlySet<string>): string[] {
  const code = withoutComments(text);
  const blank = blankStrings(code);
  const lit = /^\s*(['"`])([^'"`$\n]+)\1\s*$/;
  const out: string[] = [];
  const doors = new RegExp(`${RECEIVER}(?:import|require|importScripts)\\s*\\(|\\bimport\\.meta\\.resolve\\s*\\(`
    + `|\\bnew\\s+${CHAIN}(?:Shared)?Worker\\s*\\(|\\bnew\\s+${CHAIN}URL\\s*\\(`, 'g');
  for (const m of blank.matchAll(doors)) {
    const open = m.index + m[0].length - 1;
    const args = code.slice(open + 1, closing(blank, open) - 1);
    const called = m[0].replace(/\s+/g, '');
    const door = /SharedWorker\($/.test(called) ? 'new SharedWorker' : /Worker\($/.test(called) ? 'new Worker'
      : /URL\($/.test(called) ? 'new URL' : /importScripts\($/.test(called) ? 'importScripts'
        : /resolve\($/.test(called) ? 'import.meta.resolve' : /require\($/.test(called) ? 'require' : 'import';
    // the doors' specifiers: an importScripts' every argument, a URL's first when it is a module's, the others' first
    // (a worker built from a URL read at the URL)
    const parts = splitArgs(args, blank.slice(open + 1, closing(blank, open) - 1));
    const specs = door === 'importScripts' ? parts
      : door === 'new URL' ? (/,\s*import\.meta\.url\s*$/.test(args) ? [parts[0] ?? ''] : [])
      : /Worker$/.test(door) && /^\s*new\s+URL\s*\(/.test(parts[0] ?? '') ? []
      : [parts[0] ?? ''];
    for (const arg of specs) {
      const spec = lit.exec(arg)?.[2];
      if (spec === undefined || !spec.startsWith('.')) continue;
      const path = resolved(file, spec);
      const shown = door === 'new URL' ? `new URL('${spec}', import.meta.url)` : `${door}('${spec}')`;
      if (never.has(path)) out.push(`${file} ${shown}: ${path}, a module of a layer it may not reach`);
      else if (/\.(?:ts|mts|js|mjs|cjs)$/.test(path) && !files.has(path)) out.push(`${file} ${shown}: ${path}, a module outside its closure`);
    }
  }
  return out;
}

/** A call's arguments, split at each comma outside brackets (`blank`: the same text with its strings blanked). */
function splitArgs(args: string, blank: string): string[] {
  const each: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i <= blank.length; i++) {
    const c = i < blank.length ? blank[i]! : ',';
    if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') depth--;
    else if (c === ',' && depth === 0) {
      each.push(args.slice(start, i));
      start = i + 1;
    }
  }
  return each;
}

// ---- the compiler hushed (the C5-T audit's item 5) ----

/** Each line of `text` that hushes the compiler: a `@ts-nocheck`, a `@ts-ignore` or a `@ts-expect-error`. */
function suppressions(file: string, text: string): string[] {
  return text.split('\n').flatMap((line, i) => (/@ts-(?:nocheck|ignore|expect-error)\b/.test(line) ? [`${file}:${i + 1}: ${line.trim()}`] : []));
}

// ---- the run signatures (carried from the run wall's signature rule: the C5-T audit's item 4) ----

/** Whether `text` names a planner's answer: a statement (PlannedSql), a plan (PlanText), or a planned record. */
function namesPlanned(text: string, records: ReadonlySet<string>): boolean {
  return [...text.matchAll(/[\w$]+/g)].some((w) => w[0] === 'PlannedSql' || w[0] === 'PlanText' || records.has(w[0]));
}

/** One name a module's import or export clause names: `[type] A [as B]`, from a module (`from '...'`) or its own. */
interface Named { readonly kind: 'import' | 'export'; readonly from: string | null; readonly name: string; readonly as: string }

/** Each name `file`'s import and export clauses name, each from the module it reads (its path), or its own. */
function namedIn(file: string, files: ReadonlyMap<string, string>): Named[] {
  const out: Named[] = [];
  const clauses = /\b(import|export)\s+(?:type\s+)?(?:[\w$]+\s*,\s*)?\{([^}]*)\}(?:\s*from\s*(['"])([^'"\n]+)\3)?/g;
  for (const m of withoutComments(files.get(file) ?? '').matchAll(clauses)) {
    const spec = m[4];
    const from = spec === undefined ? null : spec.startsWith('.') ? resolved(file, spec) : `package:${spec}`;
    for (const item of m[2]!.split(',')) {
      const r = /^\s*(?:type\s+)?([\w$]+)(?:\s+as\s+([\w$]+))?\s*$/.exec(item);
      if (r !== null) out.push({ kind: m[1] as 'import' | 'export', from, name: r[1]!, as: r[2] ?? r[1]! });
    }
  }
  return out;
}

/** What `name`, read in `file`, first named: through the module's re-exports, its own export renames and its imports,
 *  to a fixed point. A name no clause binds is its own. */
function origin(file: string, name: string, files: ReadonlyMap<string, string>, seen: ReadonlySet<string> = new Set()): string {
  const key = `${file}#${name}`;
  if (!files.has(file) || seen.has(key)) return name;
  const next = new Set([...seen, key]);
  for (const n of namedIn(file, files)) {
    if (n.as !== name) continue;
    if (n.kind === 'export' && n.from === null && n.name === name) continue;
    if (n.from === null) return origin(file, n.name, files, next);
    return n.from.startsWith('package:') ? n.name : origin(n.from, n.name, files, next);
  }
  return name;
}

/** `file`'s text with each name its imports bind read as the name it first named. */
function unaliased(file: string, files: ReadonlyMap<string, string>): string {
  const renames = new Map<string, string>();
  for (const n of namedIn(file, files)) {
    if (n.kind !== 'import' || n.from === null) continue;
    const first = n.from.startsWith('package:') ? n.name : origin(n.from, n.name, files);
    if (first !== n.as) renames.set(n.as, first);
  }
  const text = files.get(file) ?? '';
  return renames.size === 0 ? text : text.replace(/[\w$]+/g, (w) => renames.get(w) ?? w);
}

/** `code` with each string literal blanked (a template's text too). */
function withoutStrings(code: string): string {
  return code.replace(/'(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*"|`(?:[^`\\]|\\.)*`/g, '""');
}

/** Each signature a module declares, exported or not, by name: a function's parameters and result, an arrow const's,
 *  a class's non-private methods', constructor's and fields', an interface's members. */
function declaredSignatures(text: string): Map<string, string> {
  const code = withoutStrings(withoutComments(text));
  const out = new Map<string, string>();
  for (const m of code.matchAll(/\b(?:async\s+)?function\s*\*?\s*([\w$]+)\s*(?:<[^(]*>)?\s*\(/g)) {
    const paren = m.index + m[0].length - 1;
    const end = code.indexOf('{', closing(code, paren));
    out.set(m[1]!, code.slice(paren, end < 0 ? code.length : end));
  }
  for (const m of code.matchAll(/\b(?:const|let|var)\s+([\w$]+)\s*(?::[^=;]+)?=\s*(?:async\s*)?(?:<[^(=]*>\s*)?\(/g)) {
    const paren = m.index + m[0].length - 1;
    const close = closing(code, paren);
    const arrow = /^\s*(?::[^=;{]+)?=>/.exec(code.slice(close));
    if (arrow !== null) out.set(m[1]!, code.slice(paren, close) + arrow[0].replace(/=>$/, ''));
  }
  for (const m of code.matchAll(/\b(?:const|let|var)\s+([\w$]+)\s*:\s*([^=;]+?=>[^=;]+?)\s*=[^=>]/g)) {
    if (!out.has(m[1]!)) out.set(m[1]!, `: ${m[2]!}`);
  }
  for (const m of code.matchAll(/\b(?:abstract\s+)?(class|interface)\s+([\w$]+)[^{]*\{/g)) {
    const open = m.index + m[0].length - 1;
    const isClass = m[1] === 'class';
    const body = code.slice(open + 1, closing(code, open) - 1);
    const add = (member: string): void => {
      const text = member.trim();
      const name = /^(?:(?:public|static|readonly|async|get|set|override|declare)\s+)*([\w$]+)\??\s*[(<:]/.exec(text)?.[1];
      if (text === '' || name === undefined || /^(?:private|protected)\b|^#/.test(text)) return;
      out.set(`${m[2]}.${name}`, (out.get(`${m[2]}.${name}`) ?? '') + text);
    };
    let depth = 0;
    let start = 0;
    for (let i = 0; i <= body.length; i++) {
      const c = i < body.length ? body[i]! : ';';
      if (c === '{' && isClass && depth === 0) {
        add(body.slice(start, i));
        i = closing(body, i) - 1;
        start = i + 1;
        continue;
      }
      if (c === '(' || c === '[' || c === '<' || c === '{') depth++;
      else if (c === ')' || c === ']' || c === '}' || (c === '>' && body[i - 1] !== '=')) depth--;
      if (depth === 0 && (c === ';' || (c === '\n' && !isClass))) {
        add(body.slice(start, i));
        start = i + 1;
      }
    }
  }
  return out;
}

/** Whether `text` ANSWERS a planner's statement, plan or planned record: one named in a result -- after an arrow's `=>`
 *  or a method's `):`, to that result's end. One TAKEN is the run's own work, and no answer. */
function answersPlanned(text: string, records: ReadonlySet<string>): boolean {
  for (const m of text.matchAll(/=>|\)\s*:/g)) {
    const from = m.index + m[0].length;
    let depth = 0;
    let i = from;
    for (; i < text.length; i++) {
      const c = text[i]!;
      if (c === '(' || c === '[' || c === '{' || c === '<') depth++;
      else if (c === ')' || c === ']' || c === '}' || (c === '>' && text[i - 1] !== '=')) {
        if (--depth < 0) break;
      } else if (depth === 0 && (c === ',' || c === ';' || (c === '=' && text[i + 1] !== '>'))) break;
    }
    if (namesPlanned(text.slice(from, i), records)) return true;
  }
  return false;
}

/** Each type alias's and interface's body the modules declare (their imports' renames read), by name. */
function declaredTypes(files: ReadonlyMap<string, string>): Map<string, string> {
  const declared = new Map<string, string>();
  const add = (name: string, body: string): void => { declared.set(name, `${declared.get(name) ?? ''}\n${body}`); };
  for (const file of files.keys()) {
    const code = withoutStrings(withoutComments(unaliased(file, files)));
    for (const m of code.matchAll(/\btype\s+([\w$]+)\s*(?:<[^=]*>)?\s*=\s*([^;]+);/g)) add(m[1]!, m[2]!);
    for (const m of code.matchAll(/\binterface\s+([\w$]+)[^{]*\{/g)) {
      const open = m.index + m[0].length - 1;
      add(m[1]!, code.slice(open, closing(code, open)));
    }
  }
  return declared;
}

/** `text` with each parameter list blanked: what remains of a type is its fields and its results. */
function withoutParameters(text: string): string {
  let out = '';
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '(') {
      const close = closing(text, i);
      if (/^\s*(?:=>|:)/.test(text.slice(close))) {
        out += '()';
        i = close - 1;
        continue;
      }
    }
    out += text[i]!;
  }
  return out;
}

/** The planned records: each type whose text outside its parameter lists names a statement, a plan or another planned
 *  record, to a fixed point (relation-type.ts's Plan: a statement with its columns). */
function plannedRecords(files: ReadonlyMap<string, string>): Set<string> {
  const declared = declaredTypes(files);
  const out = new Set<string>();
  for (let grew = true; grew;) {
    grew = false;
    for (const [name, body] of declared) {
      if (!out.has(name) && namesPlanned(withoutParameters(body), out)) {
        out.add(name);
        grew = true;
      }
    }
  }
  return out;
}

/** The sources of the planner's answers: each declared type that answers a statement, a plan or a planned record, or
 *  names such a type, to a fixed point (a planner under another name). */
function answeringTypes(files: ReadonlyMap<string, string>): Set<string> {
  const declared = declaredTypes(files);
  const records = plannedRecords(files);
  const out = new Set<string>();
  for (let grew = true; grew;) {
    grew = false;
    for (const [name, body] of declared) {
      if (out.has(name)) continue;
      if (answersPlanned(body, records) || [...body.matchAll(/\b([A-Z][\w$]*)\b/g)].some((t) => out.has(t[1]!))) {
        out.add(name);
        grew = true;
      }
    }
  }
  return out;
}

/** What a signature TAKES: a function's or a method's parameter list, a property's type -- never its own result. */
function takes(signature: string): string {
  const s = signature.trim();
  const paren = s.indexOf('(');
  const colon = s.indexOf(':');
  if (paren >= 0 && (colon < 0 || paren < colon)) return s.slice(paren, closing(s, paren));
  return colon >= 0 ? s.slice(colon + 1) : '';
}

/** Each signature of `run`'s modules that takes a source of the planner's answers: a type that answers one, by name,
 *  or anything else -- an inline type, a function -- that answers a statement, a plan or a planned record. */
function signatureBreaches(files: ReadonlyMap<string, string>, run: readonly string[]): string[] {
  const records = plannedRecords(files);
  const types = answeringTypes(files);
  const out: string[] = [];
  for (const file of run) {
    for (const [name, signature] of declaredSignatures(unaliased(file, files))) {
      const taken = takes(signature);
      const typed = [...taken.matchAll(/\b([A-Z][\w$]*)\b/g)].map((m) => m[1]!).find((t) => types.has(t));
      if (typed !== undefined) out.push(`${file} ${name}: takes ${typed}, which answers a planner's statement or plan`);
      else if (answersPlanned(taken, records)) out.push(`${file} ${name}: takes something that answers a planner's statement or plan`);
    }
  }
  return out.sort();
}

// ---- an explicit any (the C5-T last round's T2) ----

/** Each line of `text` that writes the type `any` (strings and comments aside, a template's substitutions read as code;
 *  a member or a call named `any` is a name). */
function explicitAnys(file: string, text: string): string[] {
  const code = codeAlone(text);
  const lines = text.split('\n');
  return [...code.matchAll(/(?<![\w$.])any(?![\w$]|\s*\()/g)].map((m) => {
    const line = code.slice(0, m.index).split('\n').length;
    return `${file}:${line}: ${lines[line - 1]!.trim()}`;
  });
}

/** `text` with every comment's, string's, template text's and regular expression's characters blanked -- each line kept,
 *  a template's `${...}` substitutions kept as code (nested ones too). A quote or a regular expression left open ends
 *  with its line, so a misread spends one line at most. */
function codeAlone(text: string): string {
  const out = text.split('');
  const blank = (from: number, to: number): void => {
    for (let k = from; k < to; k++) if (out[k] !== '\n') out[k] = ' ';
  };
  const templates: number[] = []; // for each open template's substitution, its brace depth
  let depth = 0;
  let i = 0;
  const lastCode = (at: number): string => {
    for (let k = at - 1; k >= 0; k--) if (!/\s/.test(out[k]!)) return out[k]!;
    return '';
  };
  /** From inside a template's text at `from`, to its close or its next substitution; the offset past what it read. */
  const templateText = (from: number): number => {
    let k = from;
    while (k < text.length && text[k] !== '`' && !(text[k] === '$' && text[k + 1] === '{')) k += text[k] === '\\' ? 2 : 1;
    blank(from, Math.min(k, text.length));
    if (text[k] === '$') {
      templates.push(depth);
      return k + 2;
    }
    return k + 1;
  };
  while (i < text.length) {
    const c = text[i]!;
    if (c === '/' && text[i + 1] === '/') {
      const end = text.indexOf('\n', i);
      blank(i, end < 0 ? text.length : end);
      i = end < 0 ? text.length : end;
    } else if (c === '/' && text[i + 1] === '*') {
      const end = text.indexOf('*/', i + 2);
      blank(i, end < 0 ? text.length : end + 2);
      i = end < 0 ? text.length : end + 2;
    } else if (c === "'" || c === '"') {
      let k = i + 1;
      while (k < text.length && text[k] !== c && text[k] !== '\n') k += text[k] === '\\' ? 2 : 1;
      blank(i + 1, Math.min(k, text.length));
      i = k + 1;
    } else if (c === '`') {
      i = templateText(i + 1);
    } else if (c === '/' && (/[(,=:[!&|?{};+\-*%<>~^]/.test(lastCode(i)) || lastCode(i) === '' || /\breturn$/.test(out.slice(0, i).join('').trimEnd()))) {
      let k = i + 1;
      let inClass = false;
      while (k < text.length && text[k] !== '\n' && (inClass || text[k] !== '/')) {
        if (text[k] === '\\') k++;
        else if (text[k] === '[') inClass = true;
        else if (text[k] === ']') inClass = false;
        k++;
      }
      blank(i + 1, Math.min(k, text.length));
      i = k + 1;
    } else if (c === '{') {
      depth++;
      i++;
    } else if (c === '}') {
      if (templates.length > 0 && templates[templates.length - 1] === depth) {
        templates.pop();
        i = templateText(i + 1);
      } else {
        depth--;
        i++;
      }
    } else {
      i++;
    }
  }
  return out.join('');
}
