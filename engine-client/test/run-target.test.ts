// THE RUN TARGET ALONE (C5-T, 2026-11-15: the TypeScript split of plan and run, the rule that replaces the interim run
// wall, datacube/test/run-wall.test.ts). //engine-client:run's whole closure -- its files and its deps' files, exactly
// what Bazel puts in this test's runfiles for it, nothing else -- is copied into a fresh directory, and the pinned
// strict TypeScript compiler (DataCube's tsconfig's options) checks it there. A run module that imports a planner, a
// server or a door module -- `import type` and a literal `import()` included -- names a file that is not there, and
// fails by name (TS2307): the build's own dependency rule, as //core:exec's deps are for Java. Its deps are pinned by
// tools/deps/ts-layers.txt (run: records and npm alone, //tools/deps:ts_layering_test).
//
// Beside it, on the run target's closure:
// - no planner, server or door module is in it, nor any app's (the orchestrator's note 4, 2026-11-15), by the layers'
//   own file lists (//engine-client:not_run_srcs, a genquery);
// - DEFAULT-DENY on computed specifiers (the orchestrator's note 2): an `import()`, a `require()`, an
//   `import.meta.resolve()`, an `importScripts()`, a worker's module or a `new URL(..., import.meta.url)` whose
//   specifier is not a literal chooses its module at run time, where no compiler follows it -- each refused but the two
//   vendor loads named with their reason (COMPUTED_DOORS).
//
// It reads the runfiles tree because that tree IS the run target's closure: what this test's BUILD target declares.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, posix, relative, sep } from 'node:path';
import { after, describe, it } from 'node:test';

import { runfile, runfileFromEnv, runfilesFromEnv, runfilesRoot } from '../../tools/js/runfiles.mts';

/** The main repository's directory in the runfiles tree. */
const MAIN = join(runfilesRoot(), '_main');

/** The run target's computed specifiers, by module and door, with the reason each is allowed (the orchestrator's note
 *  2, 2026-11-15: carried from the run wall's COMPUTED_DOORS of the final audit of 9af16fbe58's T4). Shrink-only. */
const COMPUTED_DOORS = new Map([
  ['engine-client/src/duckdb-tab.ts new Worker(bundle.mainWorker!)',
    "DuckDB's own worker, its bundle's duckdb-browser-eh.worker.js from the page's vendor directory: DuckDB runs there"],
  ['engine-client/src/warehouse.ts import(runtimeUrl)',
    "TeaVM's runtime for the warehouse's wire binding (vendor/warehouse/, //warehouse:sqlapi_page): it names the"
      + " warehouse's calls and reads their answers, and plans nothing"],
]);

/** Every .ts file of the main repository in the runfiles, by its path from the repository (`/`-separated): the run
 *  target's closure, and this test's own files beside it. */
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

/** The run target's closure: every source but this test's own (its entry point, the runfiles helper). */
const CLOSURE = runfilesSources().filter((f) => !f.startsWith('engine-client/test/') && !f.startsWith('tools/'));

const made: string[] = [];
after(() => {
  for (const d of made) rmSync(d, { recursive: true, force: true });
});

/** The closure copied into a fresh directory, with `planted` files added, and the strict compiler's answer there. */
function typecheck(planted: ReadonlyMap<string, string> = new Map()): { readonly status: number | null; readonly output: string } {
  const root = mkdtempSync(join(tmpdir(), 'run-target-'));
  made.push(root);
  // each file's TEXT, never the runfiles' link to it: a link's real directory holds the files the closure does not
  const write = (f: string, text: string): void => {
    mkdirSync(dirname(join(root, f)), { recursive: true });
    writeFileSync(join(root, f), text);
  };
  for (const f of CLOSURE) write(f, readFileSync(join(MAIN, f), 'utf8'));
  for (const [f, text] of planted) write(f, text);
  // the run target's npm packages, as Bazel linked them for it (a junction, so Windows needs no privilege)
  symlinkSync(join(MAIN, 'engine-client', 'node_modules'), join(root, 'engine-client', 'node_modules'), 'junction');
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
    include: ['engine-client/src', 'pure-protocol/src'],
  }));
  writeFileSync(join(root, 'package.json'), JSON.stringify({ type: 'module' }));
  const tsc = compiler();
  const r = spawnSync(process.execPath, [tsc, '-p', join(root, 'tsconfig.json'), '--pretty', 'false'],
    { cwd: root, encoding: 'utf8', timeout: 120_000 });
  return { status: r.status, output: `${r.stdout ?? ''}${r.stderr ?? ''}${r.error ? String(r.error) : ''}` };
}

describe('the run target alone: //engine-client:run with its deps and nothing else', () => {
  it('holds the run, its records and the protocol, and no planner, server, door or app module (note 4)', () => {
    // looking: the run's 12 modules, the records' 18 and the protocol's (2026-11-15), and the runner, the requests (a
    // record) and the protocol's index among them
    assert.ok(CLOSURE.length >= 35, `the run target's closure is ${CLOSURE.length} files: the guard is not looking`);
    for (const f of ['engine-client/src/plan-runner.ts', 'engine-client/src/requests.ts', 'pure-protocol/src/index.ts']) {
      assert.ok(CLOSURE.includes(f), `${f} is not in the run target's closure: ${CLOSURE.join(', ')}`);
    }
    assert.deepEqual(CLOSURE.filter((f) => !f.startsWith('engine-client/src/') && !f.startsWith('pure-protocol/src/')), [],
      "the run target's closure holds a file of neither the engine client nor the protocol (an app's?)");
    const notRun = readFileSync(runfileFromEnv('NOT_RUN_SRCS'), 'utf8').split('\n').map((l) => l.trim()).filter((l) => l)
      .map((label) => label.replace(/^\/\/([^:]+):/, '$1/'));
    assert.ok(notRun.includes('engine-client/src/legend/wasm-grammar.ts') && notRun.includes('engine-client/src/doors/tab-duckdb.ts'),
      `the planner's, server's and doors' files are not read: ${notRun.join(', ')}`);
    assert.deepEqual(CLOSURE.filter((f) => notRun.includes(f)), [], "a planner, server or door module is in the run target's closure");
  });

  it('typechecks alone, strict: every module it imports is in its closure', () => {
    const { status, output } = typecheck();
    assert.equal(status, 0, `the run target does not typecheck alone:\n${output}`);
  });

  it('a planted run module that reaches a planner, a server or a door fails by the module it names', () => {
    // written in pieces: the package's import scanners read this file's text
    const [imp, from] = [['im', 'port'].join(''), ['fr', 'om'].join('')];
    const planted = new Map([['engine-client/src/planted-run.ts', [
      `${imp} type { WasmGrammar } ${from} './legend/wasm-grammar.ts';`,
      `${imp} { HttpEngine } ${from} './legend/engine.ts';`,
      `export const grammar = (g: WasmGrammar): unknown => g;`,
      `export const engine = (): unknown => HttpEngine;`,
      `export const door = async (): Promise<unknown> => ${imp}('./doors/tab-duckdb.ts');`,
    ].join('\n')]]);
    const { status, output } = typecheck(planted);
    assert.notEqual(status, 0, `the planted run module typechecked: the run target is not alone\n${output}`);
    for (const spec of ['./legend/wasm-grammar.ts', './legend/engine.ts', './doors/tab-duckdb.ts']) {
      assert.ok(new RegExp(`planted-run\\.ts.*TS2307.*'${spec.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&')}'`).test(output),
        `the planted import of ${spec} is not refused by name:\n${output}`);
    }
  });

  it('opens no import door whose module is chosen at run time, but the two vendor loads named (note 2, default-deny)', () => {
    const found = CLOSURE.flatMap((f) => computedDoors(readFileSync(join(MAIN, f), 'utf8')).map((d) => `${f} ${d}`)).sort();
    assert.deepEqual(found, [...COMPUTED_DOORS.keys()].sort());
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

  it('reads the runfiles it was given: the compiler and the layers it holds the run apart from', () => {
    assert.ok(existsSync(compiler()), 'the pinned compiler is not in the runfiles');
    assert.ok(posix.basename(runfileFromEnv('NOT_RUN_SRCS')) === 'not_run_srcs');
  });
});

// ---- the computed-specifier scanner (carried from the run wall's computedDoors, the final audit of 9af16fbe58's T4) ----

/** `text` with its comments blanked (each kept as spaces, so offsets hold). */
function withoutComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '))
    .replace(/(^|[^:'"`\\])\/\/[^\n]*/g, (m, lead: string) => lead + ' '.repeat(m.length - lead.length));
}

/** `code` with each string literal's text blanked, its quotes and every offset kept (a template's substitutions too). */
function blankStrings(code: string): string {
  return code.replace(/'(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*"|`(?:[^`\\]|\\.)*`/g,
    (m) => m[0] + ' '.repeat(m.length - 2) + m[m.length - 1]);
}

/** The offset just past the bracket that closes the one at `open`. */
function closing(code: string, open: number): number {
  const pairs: Record<string, string> = { '(': ')', '{': '}', '[': ']' };
  const stack: string[] = [];
  for (let i = open; i < code.length; i++) {
    const c = code[i]!;
    if (c === '(' || c === '{' || c === '[') stack.push(pairs[c]!);
    else if (c === ')' || c === '}' || c === ']') {
      stack.pop();
      if (stack.length === 0) return i + 1;
    } else if (c === "'" || c === '"' || c === '`') {
      const end = code.indexOf(c, i + 1);
      i = end < 0 ? code.length : end;
    }
  }
  return code.length;
}

/** A chain of receivers before a door's name, any length (`self.`, `globalThis.`, `a.b?.`). */
const CHAIN = '(?:[\\w$]+\\s*\\??\\.\\s*)*';

/** A door's start: its receivers' chain from its first name, or its name alone. */
const RECEIVER = `(?<![\\w$.])${CHAIN}`;

/** A specifier read as a literal: a quoted one, or a template with no substitution. */
const LITERAL_SPEC = /^\s*(?:'[^'\\\n]*'|"[^"\\\n]*"|`[^`\\$]*`)\s*$/;

/** Each import door `text` opens whose specifier is NOT a literal: a dynamic `import()`, a `require()`, an
 *  `import.meta.resolve()`, an `importScripts()` (each argument), a worker's module, or a `new URL(...,
 *  import.meta.url)`, through any receiver -- its first argument a template with a substitution, a name, a call, or any
 *  other expression. A worker built from a literal `new URL('...', import.meta.url)` is read. */
export function computedDoors(text: string): string[] {
  const code = withoutComments(text);
  const blank = blankStrings(code);
  const out: string[] = [];
  const doors = new RegExp(`${RECEIVER}(?:import|require|importScripts)\\s*\\(|\\bimport\\.meta\\.resolve\\s*\\(`
    + `|\\bnew\\s+${CHAIN}(?:Shared)?Worker\\s*\\(|\\bnew\\s+${CHAIN}URL\\s*\\(`, 'g');
  for (const m of blank.matchAll(doors)) {
    const open = m.index + m[0].length - 1;
    const close = closing(blank, open);
    const args = code.slice(open + 1, close - 1);
    // the arguments: split at each comma outside brackets (strings blanked, so theirs never count)
    const blankArgs = blank.slice(open + 1, close - 1);
    const each: string[] = [];
    let depth = 0;
    let start = 0;
    for (let i = 0; i <= blankArgs.length; i++) {
      const c = i < blankArgs.length ? blankArgs[i]! : ',';
      if (c === '(' || c === '[' || c === '{') depth++;
      else if (c === ')' || c === ']' || c === '}') depth--;
      else if (c === ',' && depth === 0) {
        each.push(args.slice(start, i));
        start = i + 1;
      }
    }
    const first = each[0] ?? '';
    const door = m[0].replace(/\s+/g, ' ').replace(/\s*\.\s*/g, '.').replace(/\s*\($/, '(');
    if (/URL\($/.test(door) && !/,\s*import\.meta\.url\s*$/.test(args)) continue; // a URL, not a module's
    if (/importScripts\($/.test(door)) {
      if (each.every((a) => LITERAL_SPEC.test(a))) continue;
      out.push(`${door}${each.map((a) => a.replace(/\s+/g, ' ').trim()).join(', ')})`);
      continue;
    }
    if (LITERAL_SPEC.test(first)) continue;
    if (/(?:Shared)?Worker\($/.test(door)
      && /^\s*new\s+URL\(\s*(?:'[^'\\\n]*'|"[^"\\\n]*"|`[^`\\$]*`)\s*,\s*import\.meta\.url\s*\)\s*$/.test(first)) continue;
    out.push(`${door}${first.replace(/\s+/g, ' ').trim()}${each.length > 1 ? ', ...' : ''})`);
  }
  return out;
}
