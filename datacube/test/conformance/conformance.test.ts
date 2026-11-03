// THE CONFORMANCE SUITE (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, R1): one model and its rows (conformance/model.pure),
// one list of queries (conformance/queries.tsv), each asked of every host through legend-engine's own execute -- the JVM
// server (Java's planner and PlanRunner), Python's engine (its native planner and its DuckDB), the tab's own
// (BrowserEngine: the WebAssembly planner and DuckDB-WASM) -- and of the tab's grid path (DuckDbEngine.execute), and the
// answers compared:
//   json   legend-engine's JSON text, byte for byte, each trace id masked: Java's against Python's and the tab's;
//   arrow  the ARROW_IPC answer, its schema and its values: Java's against Python's (the tab answers no Arrow execute);
//   grid   the tab's grid path's values against Java's JSON values, by their meaning (a number by its value, a decimal
//          by its value whatever its scale, a timestamp by its instant), for the cases that are not refusals;
//   handoff  the tab's rows handed to the planner's writers as DuckDB-WASM's own Arrow IPC (R3: the module's
//          `...FromArrowOrError` exports) against the same rows handed as JSON text: legend-engine's JSON byte for
//          byte, and the Arrow answer, its schema and its values, against Java's.
// Every difference found is an entry of the ledger (conformance/ledger.json) or the suite fails; an entry that no longer
// differs fails it too, so the ledger only shrinks (D10). A failure prints the differences as the ledger writes them.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { arrowIpcAnswer } from '../../../engine-client/src/arrow-ipc.ts';
import { DuckDbEngine, toRawTable, type ArrowishConnection } from '../../../engine-client/src/duckdb.ts';
import { setUpSession, type QueryEngine } from '../../../engine-client/src/engine.ts';
import { BrowserEngine } from '../../../engine-client/src/legend/browser-engine.ts';
import { answer, type PlannerModule } from '../../../engine-client/src/legend/planner-answer.ts';
import type { RequestBody } from '../../../engine-client/src/legend/planner-protocol.ts';
import { WasmGrammar } from '../../../engine-client/src/legend/wasm-grammar.ts';
import type { ExecuteInput } from '../../../engine-client/src/legend/wire.ts';
import { ProjectDatabases } from '../../../engine-client/src/project-data.ts';
import { arrowIpcStream } from '../../../engine-client/src/pure-v1.ts';
import { answerStatement, keyedStatement } from '../../../engine-client/src/plan-record.ts';
import { recordPlan } from '../../../engine-client/src/relation-type.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import { liteServer } from '../../../query-store/test/lite-server.ts';
import { startNodeDuckDb } from '../../../engine-client/src/node-require.ts';
import { runfileDirUrl, runfileFromEnv, runfilesFromEnv } from '../../../tools/js/runfiles.mts';

/** One of the corpus's files, by its name (CONFORMANCE_CORPUS: //conformance:corpus's three). */
function corpus(name: string): string {
  const path = runfilesFromEnv('CONFORMANCE_CORPUS').find((p) => p.replaceAll('\\', '/').endsWith(`/${name}`));
  if (path === undefined) throw new Error(`//conformance:corpus has no ${name}`);
  return readFileSync(path, 'utf8');
}

const MODEL = corpus('model.pure');
const RUNTIME = 'conf::RT';
const CASES = corpus('queries.tsv').split(/\r?\n/)
  .filter((line) => line.trim() !== '' && !line.startsWith('#'))
  .map((line) => ({ name: line.slice(0, line.indexOf('\t')), query: line.slice(line.indexOf('\t') + 1) }));

/** One difference: the case, the check, the two hosts compared, and what differs, in words. */
interface Difference { readonly case: string; readonly check: string; readonly hosts: string; readonly detail: string }
const LEDGER = JSON.parse(corpus('ledger.json')) as Difference[];

const java = liteServer();

const home = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? '.', 'legend-lite-home-'));
const python = spawn(runfileFromEnv('CONFORMANCE_PYTHON_ENGINE'), [], {
  stdio: ['pipe', 'pipe', 'inherit'],
  env: { ...process.env, LEGEND_LITE_LIBRARY: runfileFromEnv('LEGEND_LITE_LIBRARY'), LEGEND_LITE_HOME: home,
    PYTHONUTF8: '1', PYTHONDONTWRITEBYTECODE: '1' },
});

let pythonAt: { readonly url: string; readonly authorization: string };
let grammar: WasmGrammar;
/** The planner's module itself, for the exports no protocol kind asks yet (R3's Arrow handoff). */
let planner: PlannerModule;
/** The project database's own connection (the grid's), for its Arrow. */
let projectConnection: ArrowishConnection;
let tab: BrowserEngine;
let grid: QueryEngine;

/** legend-lite's planner, in this process, as the tab's worker runs it. */
async function loadPlanner(): Promise<WasmGrammar> {
  const dir = new URL(runfileDirUrl('WASM_PLANNER'));
  const runtime = await import(new URL('wasm-gc-module-runtime.js', dir).href) as {
    load(src: string, options: unknown): Promise<PlannerModule>;
  };
  const m = await runtime.load(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(i: Record<string, unknown>) {
      i.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
  planner = m;
  return new WasmGrammar({ async ask(r: RequestBody): Promise<string> { return answer(m, r); } });
}

/** A bytes answer (`Folded.httpBytes`): its status, media type and body, or the refusal it folds. */
function unfoldBytes(folded: Int8Array): { readonly status: number; readonly type: string; readonly body: Uint8Array } {
  const bytes = new Uint8Array(folded.buffer, folded.byteOffset, folded.byteLength);
  const lines: number[] = [];
  for (let i = 0; i < bytes.length && lines.length < 3; i++) if (bytes[i] === 10) lines.push(i);
  const text = (from: number, to: number): string => new TextDecoder().decode(bytes.subarray(from, to));
  if (text(0, lines[0] ?? 0) !== 'OK' || lines.length < 3) {
    throw new Error(`the planner refused the Arrow handoff: ${text(0, bytes.length)}`);
  }
  return { status: Number(text(lines[0]! + 1, lines[1]!)), type: text(lines[1]! + 1, lines[2]!), body: bytes.subarray(lines[2]! + 1) };
}

/** The tab's statement run on its database, its Arrow handed to the planner's writers (R3): both answers. */
async function handedAsArrow(input: ExecuteInput): Promise<{ readonly json: string; readonly arrow: Uint8Array } | undefined> {
  const plan = await grammar.executePlan(input);
  if (keyedStatement(plan) !== undefined) return undefined;
  // DuckDB's own Arrow IPC stream (the bindings' runQuery: the database's bytes, no JavaScript re-encoding)
  const stream = (projectConnection as unknown as {
    useUnsafe<R>(f: (bindings: { runQuery(conn: number, text: string): Uint8Array }, conn: number) => R): R;
  }).useUnsafe((bindings, conn) => bindings.runQuery(conn, answerStatement(plan)));
  const arrow = new Int8Array(stream.buffer, stream.byteOffset, stream.byteLength);
  const json = unfoldBytes(planner.exports.executeJsonAnswerFromArrowOrError(plan, arrow));
  const zipped = unfoldBytes(planner.exports.executeArrowAnswerFromArrowOrError(plan, arrow));
  return { json: new TextDecoder().decode(json.body), arrow: zipped.body };
}

before(async () => {
  pythonAt = await new Promise((done, fail) => {
    python.once('exit', (code) => fail(new Error(`Python's engine exited (${String(code)})`)));
    createInterface({ input: python.stdout! }).on('line', (line) => {
      if (line.startsWith('{')) done(JSON.parse(line) as typeof pythonAt);
    });
  });
  grammar = await loadPlanner();
  const db = await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'));
  let made: ArrowishConnection | undefined;
  const connected = async (): Promise<DuckDbEngine> => {
    made = db.connect() as ArrowishConnection;
    const e = new DuckDbEngine(made);
    await setUpSession(e, (t) => grammar.sessionSetup(t));
    return e;
  };
  const host = await connected();
  // the model's rows in a database of their own, as the tab holds a project's (ProjectDatabases: the planner's
  // projectData statements), on a connection of its own set up as every tab connection is
  const projects = new ProjectDatabases({ databaseType: 'DuckDB', run: (sql) => host.run(sql, 0), connect: connected },
    grammar);
  grid = (await projects.engineFor(MODEL, host)).engine;
  // the project's connection: the last made (engineFor connects once for the model's database)
  projectConnection = made!;
  tab = new BrowserEngine(grammar, grid, 'the conformance suite');
});

after(() => { python.stdin?.end(); });

/** An answer as text; a refusal its status and, from its body, its error type and its message. */
interface Said { readonly status: number; readonly text: string; readonly refusal?: Refusal }
interface Refusal { readonly errorType: string; readonly message: string }

/** An HTTP refusal's body as legend-engine writes one (`{"code", "errorType"?, "message"}`). */
function refusalOf(text: string): Refusal {
  try {
    const said = JSON.parse(text) as { errorType?: string; message?: string };
    return { errorType: said.errorType ?? '', message: said.message ?? text };
  } catch {
    return { errorType: '', message: text };
  }
}

async function post(url: string, body: string, headers: Record<string, string> = {}): Promise<Response> {
  return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body });
}

function body(input: ExecuteInput): string {
  return toJson({ clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' }, ...input });
}

const javaExecute = (): string => `${java.api()}/pure/v1/execution/execute`;
const pythonExecute = (): string => `${pythonAt.url}/api/pure/v1/execution/execute`;

async function textOf(url: string, input: ExecuteInput, headers: Record<string, string> = {}): Promise<Said> {
  const r = await post(url, body(input), headers);
  const text = await r.text();
  return r.ok ? { status: r.status, text } : { status: r.status, text, refusal: refusalOf(text) };
}

/** An ARROW_IPC answer: its schema in words and its rows, or its refusal. */
async function arrowOf(url: string, input: ExecuteInput, headers: Record<string, string> = {}):
    Promise<{ readonly status: number; readonly schema: string; readonly rows: string; readonly text?: string }> {
  const r = await post(`${url}?serializationFormat=ARROW_IPC`, body(input), headers);
  if (!r.ok) return { status: r.status, schema: '', rows: '', text: await r.text() };
  const stream = await arrowIpcStream(new Uint8Array(await r.arrayBuffer()));
  const { table, metadata } = arrowIpcAnswer(stream);
  const schema = table.schema.fields.map((f) => `${f.name}: ${String(f.type)}`).join(', ')
    + ` | metadata ${[...metadata.keys()].sort().join(', ')}`;
  return { status: r.status, schema, rows: cellsText(toRawTable(table, 0, 0)) };
}

/** An ARROW_IPC answer's stream as its schema in words and its rows (arrowOf's reading of a stream in hand). */
function answerOfArrow(stream: Uint8Array): { readonly schema: string; readonly rows: string } {
  const { table, metadata } = arrowIpcAnswer(stream);
  const schema = table.schema.fields.map((f) => `${f.name}: ${String(f.type)}`).join(', ')
    + ` | metadata ${[...metadata.keys()].sort().join(', ')}`;
  return { schema, rows: cellsText(toRawTable(table, 0, 0)) };
}

/** A decoded table's values, each as its kind and text (a bigint marked), row by row. */
function cellsText(t: { readonly columns: readonly { readonly name: string; readonly values: readonly Scalar[] }[]; readonly rowCount: number }): string {
  const rows: string[] = [];
  for (let r = 0; r < t.rowCount; r++) {
    rows.push(t.columns.map((c) => {
      const v = c.values[r];
      return typeof v === 'bigint' ? `${v}n` : JSON.stringify(v ?? null);
    }).join(','));
  }
  return rows.join('\n');
}

/** The trace comment each answer writes afresh. */
const traced = (text: string): string => text.replace(/executionTraceID\\" : \\"[0-9a-f-]{36}/g, 'executionTraceID');

/** Where two texts first differ, in words: the position and a little of each side. */
function firstDifference(a: string, b: string): string {
  let i = 0;
  while (i < a.length && i < b.length && a[i] === b[i]) i++;
  const around = (s: string): string => JSON.stringify(s.slice(Math.max(0, i - 30), i + 50));
  return `at ${i}: ${around(a)} vs ${around(b)}`;
}

/** A JSON answer's numbers kept as their source text, so a value is read as written (JSON.parse's source text). */
function exactJson(text: string): unknown {
  return JSON.parse(text, function reviver(_key: string, value: unknown, context?: { source?: string }) {
    return typeof value === 'number' ? { number: context?.source ?? String(value) } : value;
  } as (this: unknown, key: string, value: unknown) => unknown);
}

/** A value, by its meaning under its Pure type, as a comparable text: what two hosts must agree on. */
function meaning(v: unknown, type: string): string {
  if (v === null || v === undefined) return 'null';
  const raw = typeof v === 'object' && v !== null && 'number' in v ? (v as { number: string }).number : v;
  const text = typeof raw === 'bigint' ? raw.toString() : String(raw);
  switch (type) {
    case 'Integer': return BigInt(text).toString();
    case 'Float': return String(Number(text));
    case 'Decimal': case 'Number': {
      // the value, whatever its scale: digits without trailing zeros, no "-0"
      if (!/^-?\d+(\.\d+)?$/.test(text)) return `?${text}`;
      const negative = text.startsWith('-');
      let [whole, frac = ''] = text.replace('-', '').split('.') as [string, string?];
      whole = whole.replace(/^0+(?=\d)/, '');
      frac = (frac ?? '').replace(/0+$/, '');
      const digits = frac ? `${whole}.${frac}` : whole;
      return negative && digits !== '0' ? `-${digits}` : digits;
    }
    case 'DateTime': {
      // the instant: the fields, an offset applied, the fraction to nanoseconds
      const m = /^(-?\d{4,})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:([+-])(\d{2}):?(\d{2})|Z)?$/.exec(text);
      if (!m) return `?${text}`;
      const ms = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));
      const offset = m[8] ? (m[8] === '-' ? -1 : 1) * (Number(m[9]) * 60 + Number(m[10])) * 60_000 : 0;
      return `${BigInt(ms - offset) * 1_000_000n + BigInt((m[7] ?? '').padEnd(9, '0').slice(0, 9) || '0')}ns`;
    }
    default: return text;
  }
}

/** Java's JSON answer's rows by meaning, under the grid plan's column types. */
function javaMeanings(text: string, types: readonly string[]): string {
  const answer = exactJson(text) as { result: { rows: { values: unknown[] }[] } };
  return answer.result.rows.map((row) => row.values.map((v, i) => meaning(v, types[i]!)).join(',')).join('\n');
}

function gridMeanings(t: ResultTable): string {
  const rows: string[] = [];
  for (let r = 0; r < t.rowCount; r++) rows.push(t.columns.map((c) => meaning(c.values[r], c.type)).join(','));
  return rows.join('\n');
}

describe('the conformance suite: one plan, every host, the same answer', () => {
  it('answers every case alike on Java, Python and the tab, but for the ledger\'s differences', async () => {
    const found: Difference[] = [];
    const differ = (c: string, check: string, hosts: string, detail: string): void => {
      found.push({ case: c, check, hosts, detail });
    };
    const auth = { Authorization: pythonAt.authorization };
    let handoffs = 0;
    for (const { name, query } of CASES) {
      const lambda = await grammar.lambda(`{| ${query}->from(${RUNTIME})}`);
      const input: ExecuteInput = { function: lambda, model: { _type: 'text', code: MODEL } };
      // json: Java's, Python's, the tab's
      const j = await textOf(javaExecute(), input);
      const p = await textOf(pythonExecute(), input, auth);
      // the tab's refusal is thrown: a planning refusal an EngineError (its status, its error type), a database's
      // failure a QueryError (no status: the tab answers no HTTP)
      let t: Said;
      try {
        t = { status: 200, text: await tab.executeText(input) };
      } catch (e) {
        const thrown = e as Error & { status?: number; errorType?: string };
        t = { status: thrown.status ?? 0, text: thrown.message,
          refusal: { errorType: thrown.errorType ?? '', message: thrown.message } };
      }
      for (const [hosts, other] of [['java/python', p], ['java/tab', t]] as const) {
        if (j.refusal !== undefined || other.refusal !== undefined) {
          // a refusal against a refusal: its status, its error type, its words
          if (j.status !== other.status) differ(name, 'refusal-status', hosts, `${j.status} vs ${other.status}`);
          if ((j.refusal?.errorType ?? '-') !== (other.refusal?.errorType ?? '-')) {
            differ(name, 'refusal-type', hosts, `${j.refusal?.errorType ?? '(answered)'} vs ${other.refusal?.errorType ?? '(answered)'}`);
          }
          const mine = j.refusal?.message ?? '(answered)';
          const theirs = other.refusal?.message ?? '(answered)';
          if (mine !== theirs) differ(name, 'refusal-message', hosts, firstDifference(mine, theirs));
        } else if (traced(j.text) !== traced(other.text)) {
          differ(name, 'json', hosts, firstDifference(traced(j.text), traced(other.text)));
        }
      }
      // handoff: the tab's rows as Arrow, against its rows as text (its JSON) and against Java's (its Arrow answer)
      if (t.refusal === undefined) {
        const handed = await handedAsArrow(input);
        if (handed !== undefined) {
          handoffs++;
          if (traced(handed.json) !== traced(t.text)) differ(name, 'handoff', 'tab', firstDifference(traced(t.text), traced(handed.json)));
          const mine = answerOfArrow(await arrowIpcStream(handed.arrow));
          const javas = await arrowOf(javaExecute(), input);
          if (javas.schema !== mine.schema) differ(name, 'handoff-arrow-schema', 'java/tab', `${javas.schema} vs ${mine.schema}`);
          else if (javas.rows !== mine.rows) differ(name, 'handoff-arrow-values', 'java/tab', firstDifference(javas.rows, mine.rows));
        }
      }
      // arrow: Java's against Python's
      const ja = await arrowOf(javaExecute(), input);
      const pa = await arrowOf(pythonExecute(), input, auth);
      if (ja.status !== pa.status) differ(name, 'arrow', 'java/python', `status ${ja.status} vs ${pa.status}`);
      else if (ja.schema !== pa.schema) differ(name, 'arrow-schema', 'java/python', `${ja.schema} vs ${pa.schema}`);
      else if (ja.rows !== pa.rows) differ(name, 'arrow-values', 'java/python', firstDifference(ja.rows, pa.rows));
      // grid: the tab's own path against Java's values, by meaning
      if (!name.startsWith('err') && j.status === 200) {
        let planned: string;
        try {
          planned = await grammar.plan({ _type: 'text', code: MODEL }, await grammar.lambda(`{| ${query}}`), RUNTIME);
        } catch (e) {
          // a plan the grid does not run (a pivot whose columns its data names: DataCube plans its pivots static),
          // refused by name at planning (HostPlans.read)
          differ(name, 'grid-refused', 'java/tab', (e as Error).message);
          continue;
        }
        // the plan record (R2), read as the grid's planners read it
        const plan = recordPlan(planned);
        const rows = await grid.execute(plan, 0);
        const theirs = javaMeanings(j.text, plan.columns.map((c) => c.type));
        const mine = gridMeanings(rows);
        if (mine !== theirs) differ(name, 'grid', 'java/tab', firstDifference(theirs, mine));
      }
    }
    // every case the tab answers with rows was handed over as Arrow too: all but the refusals and the one pivot whose
    // columns its data names (a run of its own, D18 D)
    assert.equal(handoffs, CASES.filter((c) => !c.name.startsWith('err')).length - 1, 'the cases handed over as Arrow');
    const key = (d: Difference): string => JSON.stringify([d.case, d.check, d.hosts, d.detail]);
    const expected = new Set(LEDGER.map(key));
    const seen = new Set(found.map(key));
    const unexpected = found.filter((d) => !expected.has(key(d)));
    const gone = LEDGER.filter((d) => !seen.has(key(d)));
    if (unexpected.length > 0 || gone.length > 0) {
      console.log(`[ledger] the differences found, as the ledger writes them:\n${JSON.stringify(found, null, 2)}`);
    }
    // both at once: a difference not in the ledger, and an entry that no longer differs (take it out: the ledger only
    // shrinks)
    assert.deepEqual({ unexpected, gone }, { unexpected: [], gone: [] },
      `${unexpected.length} differences not in the ledger, ${gone.length} ledger entries no longer differing`);
  });
});
