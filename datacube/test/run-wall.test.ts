// THE RUN WALL, TypeScript's half (the user's ruling M2, 2026-11-13: "a compile-time wall on both hosts"; its Python
// half is python/tests/test_run_wall.py). At run time the run frame (run-frame.ts) refuses a planner entry asked inside a
// run, where a frame is kept (Node; not a browser, not the headless V8). This holds, on every host, what the source
// itself says:
//
// - THE RUN SIDE IS COMPUTED (the follow-up audit's G2, 2026-11-14: until then eight hand-listed roots): every module
//   that calls a run -- runPlan, runLoad, inRun, a run, an execute or a stream on a session, an engine, a runner, an
//   executor, a server, a client or a host (RUN_CALL) -- or has a function that takes a PlannedSql or a PlanText (a
//   statement or a plan to send) is a run entry (runEntries), and a root of the RUN set beside the named roots
//   (RUN_ROOTS). A run entry that also plans -- its imports reach the planner, or it is a planner module -- is a
//   composer, named by module and reason (COMPOSER_MODULES): shrink-only, each C5's to split.
// - THE RUN SIDE IMPORTS NO PLANNER: the RUN set -- the roots and every module they import, `import type` included,
//   `export ... from`, a dynamic `import()`, a worker's or a `new URL(..., import.meta.url)`'s module and a `require`,
//   transitively -- holds no module of the planner's (PLANNER). A module it imports this guard does not read (another
//   package's, a `#subpath` import) is named too: the wall has no door it cannot see.
// - A RUN SIGNATURE TAKES NO PLANNER: no function, class member or interface member a RUN module declares (exported or
//   not, so an `export { f }` list hides nothing; a private member is the class's own and is skipped) takes a planner's
//   type -- any type a PLANNER module exports, or a type alias or interface declared anywhere that answers a PlannedSql
//   or names a planner's type -- or anything else that answers a PlannedSql (a function's result, a method's, a stream
//   or an array of them, a record holding one). Taking a statement to send is the run's own work and is not named. One
//   composer function is named, by its reason (COMPOSER_SIGNATURES): shrink-only.
// - THE RUN SIDE WRITES NO `any`: the word in any type position (`: any`, `as any`, `=> any`, a union's `| any`, a
//   type argument), its strings and comments aside. A planner smuggled in untyped would need one, and no lint bans it
//   (no ESLint runs on these packages; tsconfig's strict refuses an IMPLICIT any only).
//
// ITS LIMIT, said plainly: this reads text, not types (the TypeScript checker was weighed for the final audit's T1 and
// not used: the package's TypeScript is 7.0.2, the native compiler, whose program API this node test does not load).
// A run is seen by its callee's spelling (runPlan, runLoad, inRun, or run, execute or stream on a receiver named for a
// session, an engine, a runner, an executor, a server, a client or a host, however reached: `!.`, `?.`): a run reached
// through another name -- a callback handed in, an object whose method sends -- is seen only where its parameter is
// typed with a PlannedSql or a PlanText. An untyped closure slips through,
// a planner reached through a value the run is given at run time (an object literal's method, a value from a map), a
// type inferred rather than written, and a type that reaches a PlannedSql through a generic parameter. The `any` ban
// narrows the first; the run frame is the backstop at run time where a frame is kept. Each list moves only with a dated
// justification naming the task, and shrinks.

import assert from 'node:assert/strict';
import { posix } from 'node:path';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';

const SOURCES = new Sources('SOURCES', 'datacube');

/** Every .ts source this guard reads, by its path from the datacube package. */
function realSources(): Map<string, string> {
  const out = new Map<string, string>();
  for (const f of [...SOURCES.under('src', '.ts'), ...SOURCES.under('../engine-client/src', '.ts'),
    // the protocol library, imported by the snapshot's runner (the follow-up audit's G2, 2026-11-14)
    ...SOURCES.under('../pure-protocol/src', '.ts')]) {
    if (!f.endsWith('.d.ts')) out.set(f, SOURCES.read(f));
  }
  return out;
}

// The modules that run a plan, named: the plan runner, the tab's DuckDB engines, the result's readers (2026-11-13, M2);
// the model's data, the host's operations, the warehouse, the tab's DuckDB and its data (the follow-up audit's G2,
// 2026-11-14: five modules that run and passed as roots). The rest of the run side is computed (runEntries). The
// branded statement's own module is in the run set through the plan runner's and DuckDB's imports of it, never named
// here: PlannedSqlTest pins every reference to it as an import (the gate's finding on b915e29405, 2026-11-14).
const RUN_ROOTS = [
  '../engine-client/src/plan-runner.ts',
  '../engine-client/src/duckdb.ts',
  '../engine-client/src/duckdb-wasm.ts',
  '../engine-client/src/arrow-ipc.ts',
  '../engine-client/src/connection-settings.ts',
  '../engine-client/src/values.ts',
  '../engine-client/src/result.ts',
  '../engine-client/src/model-data.ts',
  '../engine-client/src/host-operations.ts',
  '../engine-client/src/warehouse.ts',
  '../engine-client/src/duckdb-tab.ts',
  '../engine-client/src/tab-data.ts',
];

// The planner's modules: the WebAssembly planner's protocol, its answer, its worker and its grammar; the engine with no
// server (it plans, then runs); DataCube's planner and the Planner it is. 2026-11-13 (M2).
const PLANNER = new Set([
  '../engine-client/src/legend/wasm-grammar.ts',
  '../engine-client/src/legend/planner-answer.ts',
  '../engine-client/src/legend/planner-protocol.ts',
  '../engine-client/src/legend/planner-worker.ts',
  '../engine-client/src/legend/browser-engine.ts',
  // the server engines, HttpEngine and RoutedEngine (generatePlan): the final audit of 9af16fbe58's T2, 2026-11-14
  '../engine-client/src/legend/engine.ts',
  'src/lite-planner.ts',
  'src/cube.ts',
  'src/planner.ts',
]);

// A RUN entry that plans too: its imports reach the planner, or it is a planner module that runs (the follow-up audit's
// G2, 2026-11-14, measured: each C5's to split, so the run takes its statements alone). Shrink-only.
const COMPOSER_MODULES = new Map([
  ['src/runner.ts', "DataCube's runner: runs a page's statements in a run frame and imports its cube (C5)"],
  ['src/app/services.ts', "the app's services: an engine that runs a PlannedSql beside the runner that plans (C5)"],
  ['src/app/sources.ts', "the app's sources: an engine's run beside the page model that plans (C5)"],
  ['src/services/browser.ts', "the browser's services: DuckDB's engine run beside the app's services (C5)"],
  ['src/headless/host.ts', "the headless host: its query takes a PlannedSql beside the Python host's services (C5)"],
  ['../engine-client/src/legend/browser-engine.ts', 'the engine with no server: plans, then runs on its engine (C5)'],
  // 2026-11-14 (the final audit of 9af16fbe58's T1: `!.run(`, `.execute(` and `.stream(` now seen): each calls a run
  // and reaches the planner
  ['src/app/join-flow.ts', "the join flow: a summary's query planned by the grid's planner, then run on its engine (C5)"],
  ['src/cube.ts', "DataCube's cube, a planner module: runs and streams a page's queries through its runner (C5)"],
  ['src/plan.ts', "a page's plan: its query run by DataCube's runner, which plans it (C5)"],
  ['src/treeview.ts', "the tree view: its rows run by DataCube's runner, which plans them (C5)"],
  // 2026-11-14 (the final audit of 9af16fbe58's T2: the server engines joined the planner set)
  ['../engine-client/src/legend/engine.ts', "the server engines: generatePlan plans on a server, execute plans and runs"
    + ' there in one request (C5)'],
]);

// A RUN module's function that takes a planner, by module and name, with its reason. Shrink-only.
// 2026-11-13 (M2): setUpSession asks the planner's session setup for EVERY statement before it sends the first (plan
// first, then run); C5 moves the ask to its caller, so the run takes the statements alone.
// 2026-11-14 (the follow-up audit's G2, measured once the guard read every declaration and every type that answers a
// statement or a plan): each takes a source of the planner's answers -- an object whose method answers a PlannedSql or
// a PlanText -- and runs what it answers. C5 moves each ask to its caller, so the run takes the statements alone.
const ASKS = (type: string): string => `takes the planner's ${type} and runs what it answers (C5 moves the ask out)`;
const COMPOSER_SIGNATURES = new Map([
  ['../engine-client/src/engine.ts setUpSession', "asks the planner's session setup whole, then runs it (C5 moves the ask out)"],
  ['../engine-client/src/duckdb-tab.ts ownExtensions', ASKS('HostOperationsSource')],
  ['../engine-client/src/duckdb-tab.ts startDuckDbInTab', ASKS('TabDuckDbPlanner')],
  ['../engine-client/src/host-operations.ts runHostOperations', ASKS('HostOperationsSource')],
  ['../engine-client/src/model-data.ts loadFileTable', ASKS('FileLoadSource')],
  ['../engine-client/src/snap.ts SnapManager.constructor', ASKS('HostOperationsSource')],
  ['../engine-client/src/tab-data.ts TabTables.constructor', ASKS('FileLoadSource')],
  ['src/remote.ts accessRemote', ASKS('RemoteMountSource')],
  ['src/remote.ts mountRemote', ASKS('RemoteMountSource')],
  ['src/remote.ts runMount', ASKS('RemoteMountSource')],
  ['src/table-output.ts TableWriteHost.models', ASKS('TableModels')],
  ['src/table-output.ts writeTable', ASKS('TableWriteHost')],
  ['src/upload.ts catalogColumns', ASKS('TableModels')],
  ['src/upload.ts catalogColumnsOf', ASKS('TableModels')],
  ['src/upload.ts dropFileTable', ASKS('TableModels')],
  ['src/upload.ts forgetUpload', ASKS('TableModels')],
  ['src/upload.ts ingestFile', ASKS('TableModels')],
  ['src/upload.ts modelLanded', ASKS('TableModels')],
  ['src/upload.ts mountRemoteView', ASKS('TableModels')],
  ['src/upload.ts mountedView', ASKS('TableModels')],
  ['src/upload.ts readFileInto', ASKS('TableModels')],
]);

/** A call of a run: the plan runner's, the frame's, or a run, an execute or a stream on a session, an engine, a
 *  runner, an executor, a server, a client or a host -- however the receiver is reached (`engine!.run(`,
 *  `rows.engine?.run(`, `this.#runner.stream(`). The final audit of 9af16fbe58's T1 (2026-11-14): `!.run(`,
 *  QueryEngine's `execute(` and the runners' `stream(` were not seen. */
const RUN_CALL = /\b(?:runPlan|runLoad|inRun)\s*\(|\b[\w$]*(?:[sS]ession|[eE]ngine|[rR]unner|[eE]xecutor|[sS]erver|[cC]lient|[hH]ost)\s*[!?]?\s*\??\.\s*(?:run|execute|stream)\s*\(/g;

/** The planner's answers: a statement (PlannedSql) and a plan (PlanText, the plan's JSON), each a brand only the
 *  planned-sql module mints. Taking one is a run's work; answering one is planning. */
const PLANNED_NAMES = 'PlannedSql|PlanText';
const PLANNED = `(?:${PLANNED_NAMES})`;

/** A word naming a planner's answer. */
const NAMES_PLANNED = new RegExp(`\\b${PLANNED}\\b`);

/** The run entries: each module that calls a run or has a function taking a statement or a plan, with why. */
export function runEntries(files: Map<string, string>): Map<string, string[]> {
  const out = new Map<string, string[]>();
  for (const [file, text] of files) {
    const code = withoutStrings(withoutComments(text));
    const why = [...new Set([...code.matchAll(RUN_CALL)].map((m) => `calls ${m[0].replace(/\s+/g, '')}`))];
    // a parameter list's statement or plan (a record's field holding one is data, not a run)
    const taken = [...code.matchAll(new RegExp(`[(,]\\s*(?:readonly\\s+)?[\\w$]+\\??\\s*:\\s*(?:readonly\\s+)?(?:\\w+<\\s*)?(${PLANNED_NAMES})\\b`, 'g'))];
    for (const m of new Set(taken.map((t) => t[1]!))) why.push(`takes a ${m}`);
    if (why.length > 0) out.set(file, why.sort());
  }
  return out;
}

/** `code` with each string literal blanked (a template's text too). */
function withoutStrings(code: string): string {
  return code.replace(/'(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*"|`(?:[^`\\]|\\.)*`/g, '""');
}

/** `text` with its comments blanked (each kept as spaces, so offsets hold). */
function withoutComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' ')).replace(/(^|[^:'"`\\])\/\/[^\n]*/g, (m, lead: string) => lead + ' '.repeat(m.length - lead.length));
}

/** The modules `file` imports -- `import`, `import type`, `export ... from`, a bare `import '...'`, a dynamic
 *  `import('...')`, a `require('...')`, a worker's module (`new Worker('...')`) and a `new URL('...', import.meta.url)`'s
 *  (a worker's too) -- each relative one resolved to its path from the package; a `#subpath` import as written (a
 *  package's own map: this guard reads none, so it is named). A bare package name is a dependency's, not this tree's. */
export function importsOf(file: string, text: string): string[] {
  const code = withoutComments(text);
  const specs = [
    ...code.matchAll(/(?:^|[\n;])\s*(?:import|export)\s+(?:type\s+)?(?:[\w*{}\s,$]+?\s+from\s+)?['"]([^'"\n]+)['"]/g),
    ...code.matchAll(/\b(?:import|require)\(\s*['"]([^'"\n]+)['"]\s*\)/g),
    ...code.matchAll(/\bnew\s+(?:Shared)?Worker\(\s*['"]([^'"\n]+)['"]/g),
    ...code.matchAll(/\bnew\s+URL\(\s*['"]([^'"\n]+)['"]\s*,\s*import\.meta\.url\s*\)/g),
  ].map((m) => m[1]!);
  // a path back into this package (`../datacube/src/x.ts`, from the engine client) is its own: `src/x.ts`
  return specs.filter((s) => s.startsWith('.') || s.startsWith('#'))
    .map((s) => (s.startsWith('#') ? s
      : posix.normalize(posix.join(posix.dirname(file), s)).replace(/^\.\.\/datacube\//, '')));
}

/** The RUN set's breaches: a planner module reached (the import path to it), or a module it imports this guard cannot
 *  read. */
export function importBreaches(files: Map<string, string>, roots: readonly string[], planner: ReadonlySet<string>): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const stack = roots.map((r) => [r]);
  while (stack.length > 0) {
    const path = stack.pop()!;
    const file = path[path.length - 1]!;
    if (seen.has(file)) continue;
    seen.add(file);
    if (planner.has(file)) {
      out.push(`the planner's ${file}, reached by ${path.join(' -> ')}`);
      continue;
    }
    const text = files.get(file);
    if (text === undefined) {
      out.push(`${file}, which this guard does not read, reached by ${path.join(' -> ')}`);
      continue;
    }
    for (const m of importsOf(file, text)) stack.push([...path, m]);
  }
  return out.sort();
}

/** The RUN set itself: the roots and every module they import. */
export function runSet(files: Map<string, string>, roots: readonly string[]): string[] {
  const seen = new Set<string>();
  const stack = [...roots];
  while (stack.length > 0) {
    const f = stack.pop()!;
    if (seen.has(f) || !files.has(f)) continue;
    seen.add(f);
    stack.push(...importsOf(f, files.get(f)!));
  }
  return [...seen].sort();
}

/** The index just past the bracket that closes the one at `open` (`(`, `{` or `[`), counting the others inside. */
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

/** Each signature a module declares, exported or not (an `export { f }` list names a declared one), by name: a
 *  function's parameters and return type, an arrow const's, a class's non-private methods' and constructor's and its
 *  fields', an interface's members. */
export function declaredSignatures(text: string): Map<string, string> {
  const code = withoutStrings(withoutComments(text));
  const out = new Map<string, string>();
  for (const m of code.matchAll(/\b(?:async\s+)?function\s*\*?\s*([\w$]+)\s*(?:<[^(]*>)?\s*\(/g)) {
    const paren = m.index! + m[0].length - 1;
    const end = code.indexOf('{', closing(code, paren));
    out.set(m[1]!, code.slice(paren, end < 0 ? code.length : end));
  }
  // an arrow bound to a name: its parameters and its return type (a parenthesised value that is no arrow is skipped)
  for (const m of code.matchAll(/\b(?:const|let|var)\s+([\w$]+)\s*(?::[^=;]+)?=\s*(?:async\s*)?(?:<[^(=]*>\s*)?\(/g)) {
    const paren = m.index! + m[0].length - 1;
    const close = closing(code, paren);
    const arrow = /^\s*(?::[^=;{]+)?=>/.exec(code.slice(close));
    if (arrow !== null) out.set(m[1]!, code.slice(paren, close) + arrow[0].replace(/=>$/, ''));
  }
  // a name bound with a declared function type: `const run: (sql: PlannedSql) => ... = ...`
  for (const m of code.matchAll(/\b(?:const|let|var)\s+([\w$]+)\s*:\s*([^=;]+?=>[^=;]+?)\s*=[^=>]/g)) {
    if (!out.has(m[1]!)) out.set(m[1]!, `: ${m[2]!}`);
  }
  for (const m of code.matchAll(/\b(?:abstract\s+)?(class|interface)\s+([\w$]+)[^{]*\{/g)) {
    const open = m.index! + m[0].length - 1;
    const isClass = m[1] === 'class';
    const body = code.slice(open + 1, closing(code, open) - 1);
    let depth = 0;
    let start = 0;
    for (let i = 0; i <= body.length; i++) {
      const c = i < body.length ? body[i]! : ';';
      // a class's method body ends its member's signature (and is skipped); an interface's braces are a type's
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
    function add(text: string): void {
      const member = text.trim();
      const name = /^(?:(?:public|static|readonly|async|get|set|override|declare)\s+)*([\w$]+)\??\s*[(<:]/.exec(member)?.[1];
      if (member === '' || name === undefined || /^(?:private|protected)\b|^#/.test(member)) return;
      out.set(`${m![2]}.${name}`, (out.get(`${m![2]}.${name}`) ?? '') + member);
    }
  }
  return out;
}

/** Whether `text` (a type, or a parameter list) ANSWERS a planner's statement or plan: one is named in a result -- after
 *  an arrow's `=>` or a method's `):`, to the end of that result's type (a stream or an array of them, a promise, a
 *  record holding one). A statement or a plan TAKEN -- a parameter's, a field's, an arrow's parameter -- is the run's
 *  own work and is not an answer. */
function answersPlanned(text: string): boolean {
  for (const m of text.matchAll(/=>|\)\s*:/g)) {
    const from = m.index! + m[0].length;
    let depth = 0;
    let i = from;
    for (; i < text.length; i++) {
      const c = text[i]!;
      if (c === '(' || c === '[' || c === '{' || c === '<') depth++;
      else if (c === ')' || c === ']' || c === '}' || (c === '>' && text[i - 1] !== '=')) {
        if (--depth < 0) break;
      } else if (depth === 0 && (c === ',' || c === ';' || (c === '=' && text[i + 1] !== '>'))) break;
    }
    if (NAMES_PLANNED.test(text.slice(from, i))) return true;
  }
  return false;
}

/** A planner's types: the classes, interfaces and type aliases the planner's modules export; and every type alias or
 *  interface any module declares that answers a PlannedSql (a function's result, a method's, a record holding one: a
 *  planner under another name) or names a planner's type, to a fixed point. */
export function plannerTypes(files: Map<string, string>, planner: ReadonlySet<string>): Set<string> {
  const out = new Set<string>();
  const declared = new Map<string, string>();
  for (const [f, text] of files) {
    const code = withoutStrings(withoutComments(text));
    if (planner.has(f)) {
      for (const m of code.matchAll(/export\s+(?:abstract\s+)?(?:class|interface|type)\s+(\w+)/g)) out.add(m[1]!);
    }
    for (const m of code.matchAll(/\btype\s+([\w$]+)\s*(?:<[^=]*>)?\s*=\s*([^;]+);/g)) declared.set(m[1]!, m[2]!);
    for (const m of code.matchAll(/\binterface\s+([\w$]+)[^{]*\{/g)) {
      const open = m.index! + m[0].length - 1;
      declared.set(m[1]!, code.slice(open, closing(code, open)));
    }
  }
  for (let grew = true; grew;) {
    grew = false;
    for (const [name, body] of declared) {
      if (out.has(name)) continue;
      if (answersPlanned(body) || [...body.matchAll(/\b([A-Z][\w$]*)\b/g)].some((t) => out.has(t[1]!))) {
        out.add(name);
        grew = true;
      }
    }
  }
  return out;
}

/** What a signature TAKES: a function's or a method's parameter list, a property's type -- never its own return type
 *  (a mint answers a PlannedSql, and may). */
function takes(signature: string): string {
  const s = signature.trim();
  const paren = s.indexOf('(');
  const colon = s.indexOf(':');
  if (paren >= 0 && (colon < 0 || paren < colon)) return s.slice(paren, closing(s, paren));
  return colon >= 0 ? s.slice(colon + 1) : '';
}

/** Each RUN signature that takes a planner: a planner's type by name, or anything else that answers a PlannedSql or a
 *  PlanText. */
export function signatureBreaches(files: Map<string, string>, run: readonly string[], types: ReadonlySet<string>): string[] {
  const out: string[] = [];
  for (const file of run) {
    for (const [name, signature] of declaredSignatures(files.get(file) ?? '')) {
      const taken = takes(signature);
      const typed = [...taken.matchAll(/\b([A-Z][\w$]*)\b/g)].map((m) => m[1]!).find((t) => types.has(t));
      if (typed !== undefined) out.push(`${file} ${name}: takes the planner's ${typed}`);
      else if (answersPlanned(taken)) out.push(`${file} ${name}: takes something that answers a planner's statement or plan`);
    }
  }
  return out.sort();
}

/** Each RUN module's `any`: the word wherever a type is written (strings and comments aside). */
export function anyBreaches(files: Map<string, string>, run: readonly string[]): string[] {
  const out: string[] = [];
  for (const file of run) {
    const code = withoutStrings(withoutComments(files.get(file) ?? ''));
    // (a member or a function named `any` is a name, never a type: `static any(...)`, `AbortSignal.any(...)`)
    for (const m of code.matchAll(/(?<![\w$.])any(?![\w$]|\s*\()/g)) {
      const line = code.slice(0, m.index).split('\n').length;
      out.push(`${file}:${line}: ${code.split('\n')[line - 1]!.trim()}`);
    }
  }
  return out;
}

/** The run entries that plan too: a planner module that runs, or one whose imports reach the planner. */
export function planningEntries(files: Map<string, string>, planner: ReadonlySet<string>): string[] {
  return [...runEntries(files).keys()]
    .filter((f) => planner.has(f) || importBreaches(files, [f], planner).length > 0).sort();
}

/** The roots of the RUN set: the named roots and every run entry but a composer. */
export function runRoots(files: Map<string, string>, planner: ReadonlySet<string>): string[] {
  return [...new Set([...RUN_ROOTS, ...runEntries(files).keys()])]
    .filter((f) => !COMPOSER_MODULES.has(f) && !planner.has(f)).sort();
}

describe('the run wall (TypeScript)', () => {
  const files = realSources();
  const roots = runRoots(files, PLANNER);
  const run = runSet(files, roots);

  it('reads the run side: the named roots, the computed entries and what they import', () => {
    for (const root of RUN_ROOTS) assert.ok(files.has(root), `${root} is not among the sources read`);
    for (const p of PLANNER) assert.ok(files.has(p), `${p} is not among the sources read`);
    assert.ok(files.has('../pure-protocol/src/index.ts'), 'the protocol library is not among the sources read');
    const entries = runEntries(files);
    // the plan runner is found by its calls, a session by the statement it takes: the computation is looking
    assert.ok(entries.get('../engine-client/src/plan-runner.ts')?.includes('calls runPlan('), [...entries.keys()].join(', '));
    assert.ok(entries.get('../engine-client/src/duckdb.ts')?.includes('takes a PlannedSql'), [...entries.keys()].join(', '));
    assert.ok(roots.length > RUN_ROOTS.length + 5, `the RUN roots are ${roots.length}: the computation is not looking`);
    assert.ok(run.length >= roots.length + 5, `the RUN set is ${run.length} modules: the guard is not looking`);
  });

  it('each composer is a run entry that plans, and each run entry that plans is a named composer (shrink-only)', () => {
    assert.deepEqual(planningEntries(files, PLANNER), [...COMPOSER_MODULES.keys()].sort());
  });

  it('the run side imports no planner module, import type included, transitively', () => {
    assert.deepEqual(importBreaches(files, roots, PLANNER), []);
  });

  it("no run signature takes a planner's type or anything that answers a statement or a plan, but the named composers", () => {
    const types = plannerTypes(files, PLANNER);
    assert.ok(types.has('Planner') && types.has('WasmGrammar') && types.has('PlannerModule'), [...types].join(', '));
    const found = signatureBreaches(files, run, types);
    const named = found.filter((f) => COMPOSER_SIGNATURES.has(f.split(':')[0]!));
    assert.deepEqual(found.filter((f) => !named.includes(f)), []);
    // each named composer still is one (shrink-only: a fixed one leaves the list), and takes the type its reason names
    assert.deepEqual([...new Set(named.map((f) => f.split(':')[0]))].sort(), [...COMPOSER_SIGNATURES.keys()].sort());
    for (const f of named) {
      const type = /takes the planner's (\w+)/.exec(f)?.[1];
      if (type !== undefined) assert.ok(COMPOSER_SIGNATURES.get(f.split(':')[0]!)!.includes(`planner's ${type}`), f);
    }
  });

  it('the run side writes no any', () => {
    assert.deepEqual(anyBreaches(files, run), []);
  });

  // ---- the guard held to planted violators

  // A planted module's import line, written in pieces: the package's own import scanner (tools/test-imports.mts) reads
  // this file's text, and would take a whole line for this test's own import.
  const line = (what: string, spec: string): string => ['im', 'port ', what, ' fr', 'om ', `'${spec}'`, ';'].join('');

  it('names a planted run module that imports the planner, as a type too', () => {
    const planted = new Map(files);
    planted.set('../engine-client/src/plan-runner.ts', `${files.get('../engine-client/src/plan-runner.ts')}\n${line('type { Planner }', '../../datacube/src/cube.ts')}\n`);
    planted.set('../engine-client/src/values.ts', `${files.get('../engine-client/src/values.ts')}\n${line('{ helper }', './planted-helper.ts')}\n`);
    planted.set('../engine-client/src/planted-helper.ts', `${['ex', 'port { answer } fr', 'om '].join('')}'./legend/planner-answer.ts';\n`);
    const found = importBreaches(planted, RUN_ROOTS, PLANNER);
    assert.ok(found.some((f) => f.startsWith("the planner's src/cube.ts, reached by ") && f.endsWith('plan-runner.ts -> src/cube.ts')), found.join('\n'));
    assert.ok(found.some((f) => f.includes('legend/planner-answer.ts') && f.includes('planted-helper.ts')), found.join('\n'));
    planted.set('../engine-client/src/result.ts', `${files.get('../engine-client/src/result.ts')}\nconst late = () => ${['im', 'port('].join('')}'../../outside/unread.ts');\n`);
    assert.ok(importBreaches(planted, RUN_ROOTS, PLANNER).some((f) => f.includes('which this guard does not read')));
  });

  it("names a planted run signature that takes a planner's type or a function answering a PlannedSql", () => {
    const planted = new Map(files);
    planted.set('../engine-client/src/values.ts', `${files.get('../engine-client/src/values.ts')}
export function plantedPlanner(p: Planner): void { void p; }
export async function plantedMaker(make: (kind: string) => Promise<readonly PlannedSql[]>): Promise<void> { await make('x'); }
export class PlantedRunner {
  run(sql: PlannedSql): void { void sql; }
  ask(source: { plan(): PlannedSql }, again: () => PlannedSql): void { void source; void again; }
  private hidden(p: Planner): void { void p; }
}
export interface PlantedSession {
  readonly planner: WasmGrammar;
  run(sql: PlannedSql): Promise<void>;
}
`);
    const found = signatureBreaches(planted, ['../engine-client/src/values.ts'], plannerTypes(files, PLANNER));
    // run(sql: PlannedSql) takes a statement, never a planner: not named
    assert.deepEqual(found, [
      "../engine-client/src/values.ts PlantedRunner.ask: takes something that answers a planner's statement or plan",
      "../engine-client/src/values.ts PlantedSession.planner: takes the planner's WasmGrammar",
      "../engine-client/src/values.ts plantedMaker: takes something that answers a planner's statement or plan",
      "../engine-client/src/values.ts plantedPlanner: takes the planner's Planner",
    ]);
  });

  it('names the typed evasions: an alias, an export list, a stream or an array answered, a plan answered', () => {
    const planted = new Map(files);
    planted.set('../engine-client/src/values.ts', `${files.get('../engine-client/src/values.ts')}
type PlantedMaker = () => PlannedSql;
type PlantedMakers = PlantedMaker[];
export function plantedAlias(make: PlantedMaker, makers: PlantedMakers): void { void make; void makers; }
function hidden(p: Planner): void { void p; }
export { hidden };
const plantedArrow = (make: () => Array<PlannedSql>): void => { void make; };
export { plantedArrow as plantedArray };
export function plantedStream(make: () => AsyncIterable<PlannedSql>): void { void make; }
export function plantedLoad(source: { load(): Promise<PlanText> }): void { void source; }
export function plantedRecord(make: () => Promise<{ readonly sql: PlannedSql }>): void { void make; }
export function plantedRuns(sqls: AsyncIterable<PlannedSql>, all: Array<PlannedSql>, plan: PlanText): void { void sqls; void all; void plan; }
export interface PlantedHeld { readonly sql: PlannedSql; readonly plans: readonly PlanText[] }
`);
    const found = signatureBreaches(planted, ['../engine-client/src/values.ts'], plannerTypes(planted, PLANNER));
    // a statement, a stream of them or a plan TAKEN is the run's own (plantedRuns, PlantedHeld): not named
    assert.deepEqual(found, [
      "../engine-client/src/values.ts hidden: takes the planner's Planner",
      "../engine-client/src/values.ts plantedAlias: takes the planner's PlantedMaker",
      "../engine-client/src/values.ts plantedArrow: takes something that answers a planner's statement or plan",
      "../engine-client/src/values.ts plantedLoad: takes something that answers a planner's statement or plan",
      "../engine-client/src/values.ts plantedRecord: takes something that answers a planner's statement or plan",
      "../engine-client/src/values.ts plantedStream: takes something that answers a planner's statement or plan",
    ]);
  });

  it('names a planted #subpath import and a worker reached by a URL', () => {
    const planted = new Map(files);
    planted.set('../engine-client/src/values.ts', `${files.get('../engine-client/src/values.ts')}\n${line('{ host }', '#test-host')}\n`);
    planted.set('../engine-client/src/result.ts', `${files.get('../engine-client/src/result.ts')}
export const plantedWorker = (): Worker => new Worker(new URL('./legend/planner-worker.ts', import.meta.url), { type: 'module' });
`);
    const found = importBreaches(planted, runRoots(planted, PLANNER), PLANNER);
    assert.ok(found.some((f) => f.startsWith('#test-host, which this guard does not read, reached by ')), found.join('\n'));
    assert.ok(found.some((f) => f.startsWith("the planner's ../engine-client/src/legend/planner-worker.ts, reached by ")
      && f.endsWith('result.ts -> ../engine-client/src/legend/planner-worker.ts')), found.join('\n'));
  });

  it('computes a planted run: a new module that runs is a root, and one that plans too is a composer to name', () => {
    const planted = new Map(files);
    planted.set('src/planted-run.ts', `${line('{ runPlan }', '../../engine-client/src/plan-runner.ts')}
export async function plantedRun(plan: unknown, session: unknown): Promise<void> { await runPlan(plan, session); }
`);
    planted.set('src/planted-session.ts', 'export interface PlantedSession { run(sql: PlannedSql): Promise<void> }\n');
    planted.set('src/planted-composer.ts', `${line('{ cube }', './cube.ts')}
export async function plantedCompose(session: { run(sql: unknown): Promise<void> }): Promise<void> { await session.run(cube); }
`);
    const entries = runEntries(planted);
    assert.deepEqual(entries.get('src/planted-run.ts'), ['calls runPlan(']);
    assert.deepEqual(entries.get('src/planted-session.ts'), ['takes a PlannedSql']);
    assert.ok(runRoots(planted, PLANNER).includes('src/planted-run.ts'));
    assert.ok(planningEntries(planted, PLANNER).includes('src/planted-composer.ts'));
    assert.notDeepEqual(planningEntries(planted, PLANNER), [...COMPOSER_MODULES.keys()].sort());
  });

  it('sees each spelling of a run: a non-null or optional receiver, an execute, a stream (T1)', () => {
    const planted = new Map(files);
    planted.set('src/planted-spellings.ts', `export async function a(rows: { engine?: { run(s: string): Promise<void> } }): Promise<void> { await rows.engine!.run('x'); }
export async function b(rows: { engine?: { run(s: string): Promise<void> } }): Promise<void> { await rows.engine?.run('x'); }
export async function c(engine: { execute(s: string): Promise<void> }): Promise<void> { await engine.execute('x'); }
export async function d(runner: { stream(s: string): Promise<void> }): Promise<void> { await runner.stream('x'); }
export async function e(self: { readonly client: { execute(s: string): Promise<void> } }): Promise<void> { await self.client . execute('x'); }
`);
    assert.deepEqual(runEntries(planted).get('src/planted-spellings.ts'), [
      'calls client.execute(', 'calls engine!.run(', 'calls engine.execute(', 'calls engine?.run(', 'calls runner.stream(',
    ]);
  });

  it('names a planted any on the run side', () => {
    const planted = new Map(files);
    planted.set('../engine-client/src/values.ts', `${files.get('../engine-client/src/values.ts')}
export const planted = (x: any) => x as any;
export type PlantedAnswer = () => any;
export function plantedUnion(x: string | any, y: Map<string, any>): void { void x; void y; }
export class PlantedSignals { static any(signals: unknown[]): unknown { return AbortSignal.any(signals as AbortSignal[]); } }
`);
    // a member or a call named any is a name, never a type: not named
    assert.equal(anyBreaches(planted, ['../engine-client/src/values.ts']).length, 5);
  });
});
