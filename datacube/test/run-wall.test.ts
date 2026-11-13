// THE RUN WALL, TypeScript's half (the user's ruling M2, 2026-11-13: "a compile-time wall on both hosts"; its Python
// half is python/tests/test_run_wall.py). At run time the run frame (run-frame.ts) refuses a planner entry asked inside a
// run, where a frame is kept (Node; not a browser, not the headless V8). This holds, on every host, what the source
// itself says:
//
// - THE RUN SIDE IMPORTS NO PLANNER: the modules that run a plan (RUN_ROOTS) and every module they import, `import type`
//   included, transitively -- the RUN set -- hold no module of the planner's (PLANNER). A RUN module importing a file
//   this guard does not read is named too: the wall has no door it cannot see.
// - A RUN SIGNATURE TAKES NO PLANNER: no exported function, class method or interface member of a RUN module takes a
//   planner's type (any class or interface a PLANNER module exports) or a function that answers a PlannedSql (a
//   planner handed in under another name). One composer is named, by its reason (KNOWN_COMPOSERS): shrink-only.
// - THE RUN SIDE WRITES NO `any`: a planner smuggled in untyped would need one, and no lint bans it (no ESLint runs on
//   these packages; tsconfig's strict refuses an IMPLICIT any only). Measured 2026-11-13: none in either src.
//
// ITS LIMIT, said plainly: an untyped closure slips through -- a function of no declared type handed to a run, or a
// planner reached through a value the run is given at run time (an object whose method plans). The `any` ban narrows
// the first; the run frame is the backstop at run time where a frame is kept. Each list moves only with a dated
// justification naming the task, and shrinks.

import assert from 'node:assert/strict';
import { posix } from 'node:path';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';

const SOURCES = new Sources('SOURCES', 'datacube');

/** Every .ts source this guard reads, by its path from the datacube package. */
function realSources(): Map<string, string> {
  const out = new Map<string, string>();
  for (const f of [...SOURCES.under('src', '.ts'), ...SOURCES.under('../engine-client/src', '.ts')]) {
    if (!f.endsWith('.d.ts')) out.set(f, SOURCES.read(f));
  }
  return out;
}

// The modules that run a plan: the plan runner, the tab's DuckDB engines, the result's readers, the statement's type.
// 2026-11-13 (M2).
const RUN_ROOTS = [
  '../engine-client/src/plan-runner.ts',
  '../engine-client/src/duckdb.ts',
  '../engine-client/src/duckdb-wasm.ts',
  '../engine-client/src/arrow-ipc.ts',
  '../engine-client/src/connection-settings.ts',
  '../engine-client/src/planned-sql.ts',
  '../engine-client/src/values.ts',
  '../engine-client/src/result.ts',
];

// The planner's modules: the WebAssembly planner's protocol, its answer, its worker and its grammar; the engine with no
// server (it plans, then runs); DataCube's planner and the Planner it is. 2026-11-13 (M2).
const PLANNER = new Set([
  '../engine-client/src/legend/wasm-grammar.ts',
  '../engine-client/src/legend/planner-answer.ts',
  '../engine-client/src/legend/planner-protocol.ts',
  '../engine-client/src/legend/planner-worker.ts',
  '../engine-client/src/legend/browser-engine.ts',
  'src/lite-planner.ts',
  'src/cube.ts',
  'src/planner.ts',
]);

// A RUN module's function that takes a planner, by module and name, with its reason. Shrink-only.
// 2026-11-13 (M2): setUpSession asks the planner's session setup for EVERY statement before it sends the first (plan
// first, then run); C5 moves the ask to its caller, so the run takes the statements alone.
const KNOWN_COMPOSERS = new Map([
  ['../engine-client/src/engine.ts setUpSession', "asks the planner's session setup whole, then runs it (C5 moves the ask out)"],
]);

/** `text` with its comments blanked (each kept as spaces, so offsets hold). */
function withoutComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' ')).replace(/(^|[^:'"`\\])\/\/[^\n]*/g, (m, lead: string) => lead + ' '.repeat(m.length - lead.length));
}

/** The relative modules `file` imports -- `import`, `import type`, `export ... from`, a bare `import '...'`, a dynamic
 *  `import('...')` -- each resolved to its path from the package. */
export function importsOf(file: string, text: string): string[] {
  const code = withoutComments(text);
  const specs = [
    ...code.matchAll(/(?:^|[\n;])\s*(?:import|export)\s+(?:type\s+)?(?:[\w*{}\s,$]+?\s+from\s+)?['"]([^'"\n]+)['"]/g),
    ...code.matchAll(/\bimport\(\s*['"]([^'"\n]+)['"]\s*\)/g),
  ].map((m) => m[1]!);
  // a path back into this package (`../datacube/src/x.ts`, from the engine client) is its own: `src/x.ts`
  return specs.filter((s) => s.startsWith('.'))
    .map((s) => posix.normalize(posix.join(posix.dirname(file), s)).replace(/^\.\.\/datacube\//, ''));
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

/** Each exported signature of a module, by name: a function's parameters and return type, a class's non-private
 *  methods' and constructor's, an interface's members. */
export function exportedSignatures(text: string): Map<string, string> {
  const code = withoutComments(text);
  const out = new Map<string, string>();
  for (const m of code.matchAll(/export\s+(?:default\s+)?(?:async\s+)?function\s*\*?\s*(\w+)\s*(?:<[^(]*>)?\s*\(/g)) {
    const paren = m.index! + m[0].length - 1;
    const end = code.indexOf('{', closing(code, paren));
    out.set(m[1]!, code.slice(paren, end < 0 ? code.length : end));
  }
  for (const m of code.matchAll(/export\s+const\s+(\w+)\s*(?::[^=]+)?=\s*(?:async\s*)?\(/g)) {
    const paren = m.index! + m[0].length - 1;
    out.set(m[1]!, code.slice(paren, closing(code, paren)) + code.slice(closing(code, paren)).split('=>')[0]);
  }
  for (const m of code.matchAll(/export\s+(?:abstract\s+)?(class|interface)\s+(\w+)[^{]*\{/g)) {
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
      const name = /^(?:(?:public|static|readonly|async|get|set|override)\s+)*([\w$]+)\??\s*[(<:]/.exec(member)?.[1];
      if (member === '' || name === undefined || /^(?:private|protected)\b|^#/.test(member)) return;
      out.set(`${m![2]}.${name}`, (out.get(`${m![2]}.${name}`) ?? '') + member);
    }
  }
  return out;
}

/** The class and interface names the planner's modules export: a planner's types. */
export function plannerTypes(files: Map<string, string>, planner: ReadonlySet<string>): Set<string> {
  const out = new Set<string>();
  for (const f of planner) {
    for (const m of withoutComments(files.get(f) ?? '').matchAll(/export\s+(?:abstract\s+)?(?:class|interface|type)\s+(\w+)/g)) out.add(m[1]!);
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

/** Each RUN signature that takes a planner: a planner's type by name, or a function -- an arrow's type or a method's --
 *  that answers a PlannedSql. */
export function signatureBreaches(files: Map<string, string>, run: readonly string[], types: ReadonlySet<string>): string[] {
  const out: string[] = [];
  for (const file of run) {
    for (const [name, signature] of exportedSignatures(files.get(file) ?? '')) {
      const taken = takes(signature);
      const typed = [...taken.matchAll(/\b([A-Z]\w*)\b/g)].map((m) => m[1]!).find((t) => types.has(t));
      const answers = /(?:=>|\)\s*:)\s*(?:Promise<\s*)?(?:readonly\s+)?(?:\(\s*readonly\s+)?PlannedSql\b/.test(taken);
      if (typed !== undefined) out.push(`${file} ${name}: takes the planner's ${typed}`);
      else if (answers) out.push(`${file} ${name}: takes a function that answers a PlannedSql`);
    }
  }
  return out.sort();
}

/** Each RUN module's `any`. */
export function anyBreaches(files: Map<string, string>, run: readonly string[]): string[] {
  const out: string[] = [];
  for (const file of run) {
    const code = withoutComments(files.get(file) ?? '').replace(/'(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*"|`(?:[^`\\]|\\.)*`/g, '""');
    for (const m of code.matchAll(/:\s*any\b|\bas\s+any\b|<any>|\bany\s*\[\]|[<,]\s*any\s*[,>]/g)) {
      out.push(`${file}:${code.slice(0, m.index).split('\n').length}: ${m[0].trim()}`);
    }
  }
  return out;
}

describe('the run wall (TypeScript)', () => {
  const files = realSources();
  const run = runSet(files, RUN_ROOTS);

  it('reads the run side: the roots and what they import', () => {
    for (const root of RUN_ROOTS) assert.ok(files.has(root), `${root} is not among the sources read`);
    for (const p of PLANNER) assert.ok(files.has(p), `${p} is not among the sources read`);
    assert.ok(run.length >= RUN_ROOTS.length + 5, `the RUN set is ${run.length} modules: the guard is not looking`);
  });

  it('the run side imports no planner module, import type included, transitively', () => {
    assert.deepEqual(importBreaches(files, RUN_ROOTS, PLANNER), []);
  });

  it("no run signature takes a planner's type or a function that answers a PlannedSql, but the named composers", () => {
    const types = plannerTypes(files, PLANNER);
    assert.ok(types.has('Planner') && types.has('WasmGrammar') && types.has('PlannerModule'), [...types].join(', '));
    const found = signatureBreaches(files, run, types);
    const named = found.filter((f) => KNOWN_COMPOSERS.has(f.split(':')[0]!));
    assert.deepEqual(found.filter((f) => !named.includes(f)), []);
    // each named composer still is one (shrink-only: a fixed one leaves the list)
    assert.deepEqual(named.map((f) => f.split(':')[0]).sort(), [...KNOWN_COMPOSERS.keys()].sort());
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
      "../engine-client/src/values.ts PlantedRunner.ask: takes a function that answers a PlannedSql",
      "../engine-client/src/values.ts PlantedSession.planner: takes the planner's WasmGrammar",
      "../engine-client/src/values.ts plantedMaker: takes a function that answers a PlannedSql",
      "../engine-client/src/values.ts plantedPlanner: takes the planner's Planner",
    ]);
  });

  it('names a planted any on the run side', () => {
    const planted = new Map(files);
    planted.set('../engine-client/src/values.ts', `${files.get('../engine-client/src/values.ts')}\nexport const planted = (x: any) => x as any;\n`);
    assert.equal(anyBreaches(planted, ['../engine-client/src/values.ts']).length, 2);
  });
});
