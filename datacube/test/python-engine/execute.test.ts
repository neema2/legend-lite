// THE TAB'S EXECUTE, ONE IMPLEMENTATION (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): an execute answered in the tab --
// legend-lite's planner (WebAssembly) reading, binding and planning it with the Java the server and Python answer it
// with (PureV1Api.jsonPlan: the typed statement), DuckDB-WASM answering its rows, handed back as its decoder read them,
// and the server's own writer spelling them (PureV1Api.jsonAnswer; engine-client's BrowserEngine) -- against the same
// execute answered by Python's engine over the same frame, its DuckDB answering the rows and the same writer spelling
// them: the answer's TEXT, byte for byte (each trace id its own), a whole Float, a Decimal, a DateTime and a value of
// the session's zone (`now()->hour()`) among its columns. The tab's DuckDB starts in a zone that is not UTC (as a
// browser in Kathmandu would: +05:45), so a tab whose session was not set up as the server's answers otherwise.
// Python's ARROW_IPC answer (its plan PureV1Api.arrowPlan) carries the same builder.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { arrowIpcAnswer } from '../../../engine-client/src/arrow-ipc.ts';
import { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import { setUpSession } from '../../../engine-client/src/engine.ts';
import { arrowResultTable } from '../../../engine-client/src/engine-remote.ts';
import { BrowserEngine } from '../../../engine-client/src/legend/browser-engine.ts';
import { EngineError } from '../../../engine-client/src/legend/engine.ts';
import { answer, type PlannerModule } from '../../../engine-client/src/legend/planner-answer.ts';
import type { RequestBody } from '../../../engine-client/src/legend/planner-protocol.ts';
import { WasmGrammar } from '../../../engine-client/src/legend/wasm-grammar.ts';
import type { ExecuteInput, TdsResult } from '../../../engine-client/src/legend/wire.ts';
import { toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import { arrowIpcStream } from '../../../engine-client/src/pure-v1.ts';
import { runfileDirUrl, runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { openDatabase } from '../host/node.ts';

const home = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? '.', 'legend-lite-home-'));
const engine = spawn(runfileFromEnv('ENGINE_FIXTURE'), ['--site', runfileFromEnv('DATACUBE_SITE')], {
  stdio: ['pipe', 'pipe', 'inherit'],
  env: { ...process.env, LEGEND_LITE_LIBRARY: runfileFromEnv('LEGEND_LITE_LIBRARY'),
    LEGEND_LITE_DATACUBE: runfileFromEnv('LEGEND_LITE_DATACUBE'), LEGEND_LITE_HOME: home, PYTHONUTF8: '1',
    PYTHONDONTWRITEBYTECODE: '1' },
});

interface Served {
  readonly url: string;
  readonly authorization: string;
  readonly model: string;
  readonly runtime: string;
  /** The frame's relation, as the protocol's JSON. */
  readonly source: unknown;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly unknown[])[];
}

let served: Served;
let tab: BrowserEngine;
let grammar: WasmGrammar;

/** legend-lite's planner, in this process, answering as the tab's worker does. */
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
  return new WasmGrammar({ async ask(r: RequestBody): Promise<string> { return answer(m, r); } });
}

/** The frame's table as the model declares it: its name (schema-qualified as declared) and its columns' SQL types. */
function declaredTable(model: string): { readonly name: string; readonly columns: readonly [string, string][] } {
  const schema = /Schema\s+(\w+)/.exec(model)?.[1];
  const table = /Table\s+(\w+)\s*\(/.exec(model);
  if (!table) throw new Error(`the frame's model declares no table:\n${model}`);
  // the column list: to the parenthesis that closes the table's own
  let depth = 0;
  let end = table.index + table[0].length;
  for (; end < model.length; end++) {
    if (model[end] === '(') depth++;
    else if (model[end] === ')') { if (depth === 0) break; depth--; }
  }
  const list = model.slice(table.index + table[0].length, end);
  const columns: [string, string][] = [];
  let part = '';
  depth = 0;
  for (const ch of `${list},`) {
    if (ch === '(') depth++;
    if (ch === ')') depth--;
    if (ch === ',' && depth === 0) {
      const m = /^\s*"?([^"\s]+)"?\s+(.+?)\s*$/.exec(part);
      if (m) columns.push([m[1]!, m[2]!.replace(/\s+PRIMARY KEY$/i, '')]);
      part = '';
    } else part += ch;
  }
  return { name: schema ? `"${schema}"."${table[1]}"` : `"${table[1]}"`, columns };
}

const sqlValue = (v: unknown): string => v === null ? 'NULL' : typeof v === 'string' ? `'${v.replace(/'/g, "''")}'` : String(v);

before(async () => {
  // the engine's first JSON line says where it is, its token, and the frame it serves (its model, its rows)
  const lines = createInterface({ input: engine.stdout! });
  served = await new Promise<Served>((done, fail) => {
    engine.once('exit', (code) => fail(new Error(`the engine exited (${String(code)})`)));
    lines.on('line', (line) => { if (line.startsWith('{')) done(JSON.parse(line) as Served); });
  });
  grammar = await loadPlanner();
  const database = await openDatabase();
  const duck = new DuckDbEngine(database.connection);
  // the same frame in the tab's DuckDB: its table as the model declares it, the same rows
  const t = declaredTable(served.model);
  if (t.name.includes('.')) await duck.run(`CREATE SCHEMA IF NOT EXISTS ${t.name.split('.')[0]}`, 0);
  await duck.run(`CREATE TABLE ${t.name} (${t.columns.map(([n, ty]) => `"${n}" ${ty}`).join(', ')})`, 0);
  await duck.run(`INSERT INTO ${t.name} VALUES ${served.rows.map((r) => `(${r.map(sqlValue).join(', ')})`).join(', ')}`, 0);
  // the zone a browser in Kathmandu gives DuckDB-WASM, then the session set up as every tab's DuckDB is as it opens
  // (engine-client's setUpSession, which startDuckDbInTab runs): UTC, the server's
  await duck.run("SET TimeZone = 'Asia/Kathmandu'", 0);
  await setUpSession(duck, (databaseType) => grammar.sessionSetup(databaseType));
  tab = new BrowserEngine(grammar, duck, 'this browser');
});

after(() => { engine.stdin?.end(); });

/** An ExecuteInput over the frame: `query` the Pure after its source, with `parameters` (`{p: T[1]| ...}`). */
async function input(query: string, parameters = '', values?: ExecuteInput['parameterValues']): Promise<ExecuteInput> {
  // the frame's relation as Pure (its protocol JSON printed by the planner), the query after it
  const source = (await grammar.lambdaText({ _type: 'lambda', parameters: [], body: [served.source] } as unknown as Lambda, 'STANDARD'))
    .replace(/^\s*\|\s*/, '');
  const lambda = await grammar.lambda(`{${parameters}| ${source}${query}->from(${served.runtime})}`);
  return { function: lambda, model: { _type: 'text', code: served.model }, ...(values ? { parameterValues: values } : {}) };
}

/** Python's engine's answer to `asked`, read as the tab reads it: its rows, its builder, its activities; or its refusal. */
async function onPython(asked: ExecuteInput): Promise<{ builder: unknown; activities: readonly { sql?: string; comment?: string }[]; columns: string[]; rows: unknown[][] }> {
  const r = await fetch(`${served.url}/api/pure/v1/execution/execute?serializationFormat=ARROW_IPC`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: served.authorization },
    body: JSON.stringify({ clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' }, ...asked }),
  });
  if (!r.ok) {
    const said = await r.json() as { message: string };
    throw new EngineError(said.message, r.status);
  }
  const stream = await arrowIpcStream(new Uint8Array(await r.arrayBuffer()));
  const { rows, activities } = arrowResultTable(stream, 0, 0);
  const builder = JSON.parse(arrowIpcAnswer(stream).metadata.get('legend.builder')!) as unknown;
  const columns = rows.columns.map((c) => c.name);
  return { builder, activities, columns, rows: Array.from({ length: rows.rowCount }, (_, i) => rows.columns.map((c) => c.values[i] ?? null)) };
}

/** The trace comment's form: `-- "executionTraceID" : "<uuid>"`, each answer's own id. */
const TRACE = /^-- "executionTraceID" : "[0-9a-f-]{36}"$/;

/** Python's engine's answer to `asked` in legend-engine's JSON, as its text; a refusal its status and text. */
async function onPythonText(asked: ExecuteInput): Promise<{ status: number; text: string }> {
  const r = await fetch(`${served.url}/api/pure/v1/execution/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: served.authorization },
    body: toJson({ clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' }, ...asked }),
  });
  return { status: r.status, text: await r.text() };
}

/** An answer's text with each trace id masked: the one part each answer writes afresh. */
const traced = (text: string): string => text.replace(/executionTraceID\\" : \\"[0-9a-f-]{36}/g, 'executionTraceID');

describe("the tab's execute answers as Python's, the same Java planning both", () => {
  it("a relation's answer text, byte for byte: a whole Float, a Decimal, a DateTime and the session's zone", async () => {
    const asked = await input("->select(~[region, desk, notional])->filter(x|$x.desk == 'FX')->sort([~region->ascending(), ~notional->ascending()])"
      + '->extend(~[at: x|%2024-03-10T08:00:00+0500, amount: x|5.00D, whole: x|3.0, hour: x|now()->hour()])');
    // the hour is the clock's: asked again if the two answers straddle an hour
    for (let attempt = 0; ; attempt++) {
      const mine = await tab.executeText(asked);
      const theirs = await onPythonText(asked);
      assert.equal(theirs.status, 200, theirs.text);
      if (attempt === 0 && traced(mine) !== traced(theirs.text) && new Date().getUTCMinutes() === 0) continue;
      assert.equal(traced(mine), traced(theirs.text));
      // and it is the session's zone that says the hour: UTC's, not Kathmandu's
      const hours = (JSON.parse(mine) as TdsResult).result.rows.map((row) => row.values[row.values.length - 1]);
      assert.ok(hours.every((h) => h === new Date().getUTCHours() || h === (new Date().getUTCHours() + 23) % 24), JSON.stringify(hours));
      break;
    }
  });

  it('a relation: the same builder (each column relationalType), activity SQL, columns and rows', async () => {
    const asked = await input("->select(~[region, desk, notional, qty])->filter(x|$x.desk == 'Rates')->sort(~notional->descending())");
    const mine = await tab.execute(asked) as TdsResult;
    const theirs = await onPython(asked);
    // Python's Arrow answer carries the JSON answer's builder
    assert.deepEqual(mine.builder, theirs.builder);
    assert.ok(mine.builder.columns.every((c) => typeof c.relationalType === 'string'), JSON.stringify(mine.builder));
    assert.equal(mine.activities?.length, 1);
    const [activity] = mine.activities as { _type: string; sql: string; comment: string }[];
    assert.equal(activity!._type, 'relational');
    assert.equal(activity!.sql, theirs.activities[0]!.sql);
    assert.match(activity!.comment, TRACE);
    assert.match(theirs.activities[0]!.comment ?? '', TRACE);
    assert.deepEqual(mine.result.columns, theirs.columns);
    assert.deepEqual(mine.result.rows.map((r) => r.values), theirs.rows);
    assert.ok(mine.result.rows.length > 0);
  });

  it("a Variant column (PARK-34): legend-engine's builder, SEMISTRUCTURED, its cells strings of JSON; the tab's as Python's", async () => {
    // the rows with a quantity: an empty one's toVariant() is SQL's NULL here, where the engine answers JSON's null
    // (its ifnull(to_json(x), 'null'::json)), a difference of the value, not of the spelling (register S116, pending)
    const asked = await input("->filter(x|$x.qty->isNotEmpty())->select(~[book, qty])->sort([~book->ascending(), ~qty->ascending()])"
      + '->extend(~[n: x|$x.qty->toVariant(), s: x|$x.book->toVariant()])');
    const mine = await tab.executeText(asked);
    const theirs = await onPythonText(asked);
    assert.equal(theirs.status, 200, theirs.text);
    assert.equal(traced(mine), traced(theirs.text));
    // the builder legend-engine 4.145.0 answers for a Variant (measured 2026-10-28: core's upstream-api/e8-variant-*.json)
    const answer = JSON.parse(mine) as TdsResult;
    const variant = { type: 'meta::pure::metamodel::variant::Variant', relationalType: 'SEMISTRUCTURED' };
    assert.deepEqual(answer.builder.columns.slice(2), [{ name: 'n', ...variant }, { name: 's', ...variant }]);
    assert.ok(answer.result.rows.length > 0);
    for (const { values: [book, qty, n, s] } of answer.result.rows) {
      assert.equal(n, JSON.stringify(qty));
      assert.equal(s, JSON.stringify(book));
    }
    // and Python's Arrow answer: the same builder, the same cells
    const arrow = await onPython(asked);
    assert.deepEqual(arrow.builder, answer.builder);
    assert.deepEqual(arrow.rows, answer.result.rows.map((r) => r.values));
  });

  it('a parameter bound by its value, as Python binds it', async () => {
    // sorted to one order: two rows share a book, so the quantity breaks the tie (DuckDB promises no order for ties)
    const asked = await input('->filter(x|$x.qty > $minQty)->select(~[book, qty])->sort([~book->ascending(), ~qty->ascending()])', 'minQty: Integer[1]',
      [{ name: 'minQty', value: { _type: 'integer', value: 6 } } as unknown as NonNullable<ExecuteInput['parameterValues']>[number]]);
    const mine = await tab.executeText(asked);
    const theirs = await onPythonText(asked);
    assert.equal(traced(mine), traced(theirs.text));
    assert.deepEqual((JSON.parse(mine) as TdsResult).result.rows.map((r) => r.values[0]), ['B1', 'B1', 'B2', 'B4']);
  });

  it("a lambda's return type and an execution plan: the tab's pure/v1 answers as Python's", async () => {
    const asked = await input("->select(~[region, notional])->filter(x|$x.region == 'EMEA')");
    const python = async (path: string, body: unknown): Promise<unknown> => {
      const r = await fetch(`${served.url}/api/pure/v1/${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: served.authorization },
        body: JSON.stringify(body),
      });
      assert.equal(r.status, 200, await r.clone().text());
      return r.json();
    };
    const returned = await tab.returnType(asked.model, asked.function);
    assert.deepEqual({ returnType: returned },
      await python('compilation/lambdaReturnType', { model: asked.model, lambda: asked.function }));
    const plan = await tab.generatePlan(asked);
    assert.deepEqual(plan, await python('execution/generatePlan', { clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' }, ...asked }));
    assert.match(JSON.stringify(plan), /SELECT/i);
  });

  it('a parameter given no value: refused in the same words, its type named', async () => {
    const asked = await input('->filter(x|$x.qty > $minQty)->select(~[book])', 'minQty: Integer[1]');
    const refusal = async (f: Promise<unknown>): Promise<string> => {
      try {
        await f;
      } catch (e) {
        return e instanceof Error ? e.message : String(e);
      }
      return 'answered';
    };
    const mine = await refusal(tab.execute(asked));
    assert.equal(mine, await refusal(onPython(asked)));
    const theirs = await onPythonText(asked);
    assert.equal(theirs.status, 500);
    assert.equal((JSON.parse(theirs.text) as { message: string }).message, mine);
    assert.match(mine, /Missing external parameter\(s\): minQty:Integer\[1\]/);
  });
});
