// THE LITE ADAPTER (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D20): a connection talked to as lite asks lite's own
// execute (`/api/lite/v1/execution/execute`, legend-engine's request) and reads its answer -- one Arrow IPC stream in
// the plan's types, the plan record in its schema's metadata -- into the apps' one typed result, each column typed by
// the plan record (engine-remote.ts liteResultTable, LegendEngineExecutor). Over a stubbed fetch: the route asked and
// the request sent, the rows and their types, the SQL reported, and each thing refused by name -- a stream without its
// plan record, a plan of another shape, an answer that is no Arrow stream, a lite connection naming a serialization
// format. The servers' own answers through both adapters are the conformance suite's (//datacube:conformance_test,
// its `adapters` check).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Float64, RecordBatch, Schema, Table, Utf8, tableToIPC, vectorFromArray } from 'apache-arrow';

import { LegendEngineExecutor, liteResultTable } from '../src/engine-remote.ts';
import { PLAN_VERSION } from '../src/plan-record.ts';
import { fromElement } from '../../pure-protocol/src/index.ts';

const SQL = 'select "t".region as "region", "t".notional as "notional" from TRADES as "t"';

/** A plan record of one typed result over `SQL`, as the core writes one (PlanJson). */
function record(root: object = {
  _type: 'tdsResult',
  sql: { statement: SQL, slots: [] },
  columns: [
    { name: 'region', type: 'String', enumeration: false },
    { name: 'notional', type: 'Float', enumeration: false },
  ],
}): string {
  return JSON.stringify({ format: 'legend-lite-plan', version: PLAN_VERSION, root });
}

/** Lite's own answer: the rows as one Arrow IPC stream, `metadata` its schema's. */
function stream(metadata: ReadonlyMap<string, string> = new Map([['legend.plan', record()]])): Uint8Array {
  const rows = new Table({
    region: vectorFromArray(['AMER', null], new Utf8()),
    notional: vectorFromArray([300.5, -0.25], new Float64()),
  });
  const schema = new Schema(rows.schema.fields, new Map(metadata));
  return tableToIPC(new Table(schema, rows.batches.map((b) => new RecordBatch(schema, b.data))), 'stream');
}

const QUERY = fromElement('trades::T').select(['region', 'notional']).lambda();

/** A fetch that records what it was asked and answers `body` (`status` 200 unless said). */
function stub(body: Uint8Array | string, status = 200) {
  const calls: { url: string; body: string }[] = [];
  const fetchStub = (async (url: string | URL, init?: RequestInit) => {
    calls.push({ url: String(url), body: String(init?.body ?? '') });
    return new Response(typeof body === 'string' ? body : new Uint8Array(body), { status });
  }) as unknown as typeof fetch;
  return { calls, fetchStub };
}

const lite = (fetchStub: typeof fetch): LegendEngineExecutor => new LegendEngineExecutor({
  baseUrl: 'http://lite:8080/', model: '###Relational\nDatabase trades::DB()', runtime: 'trades::RT', protocol: 'lite', fetch: fetchStub,
});

describe('the lite adapter', () => {
  it('reads lite\'s own answer: each column typed by the plan record, each value as the stream holds it', () => {
    const { plan, rows } = liteResultTable(stream(), 3, 9);
    assert.equal(plan.sql, SQL);
    assert.deepEqual(rows.columns.map((c) => [c.name, c.type, c.values]),
      [['region', 'String', ['AMER', null]], ['notional', 'Float', [300.5, -0.25]]]);
    assert.equal(rows.rowCount, 2);
    assert.equal(rows.epoch, 3);
  });

  it('refuses a stream without its plan record, and a plan that is not one typed result, by name', () => {
    assert.throws(() => liteResultTable(stream(new Map()), 0, 0), /without its legend\.plan: its columns have no types/);
    assert.throws(() => liteResultTable(stream(new Map([['legend.plan', record({ _type: 'keyed' })]])), 0, 0),
      /a host's plan is one typed result over a statement of text, not a keyed/);
    assert.throws(() => liteResultTable(stream(new Map([['legend.plan', JSON.stringify({ format: 'legend-lite-plan', version: 1 })]])), 0, 0),
      /not a legend-lite execution plan of version/);
  });

  it('asks lite\'s own execute with legend-engine\'s request, and reports the plan\'s statement as what ran', async () => {
    const { calls, fetchStub } = stub(stream());
    const out = await lite(fetchStub).execute(QUERY, 7);
    assert.deepEqual(calls.map((c) => new URL(c.url).pathname), ['/api/lite/v1/execution/execute']);
    const sent = JSON.parse(calls[0]!.body) as { clientVersion: string; model: unknown; context: { _type: string }; function: unknown };
    assert.equal(sent.clientVersion, 'vX_X_X');
    assert.deepEqual(sent.model, { _type: 'text', code: '###Relational\nDatabase trades::DB()' });
    assert.equal(sent.context._type, 'BaseExecutionContext');
    assert.deepEqual(out.rows.columns.map((c) => [c.name, c.type]), [['region', 'String'], ['notional', 'Float']]);
    assert.equal(out.sql, SQL);
    assert.deepEqual(out.rows.receipt, { plane: 'engine', where: 'the engine at lite:8080', serverSql: SQL });
  });

  it('refuses an answer that is no Arrow stream, naming what came, and a server\'s refusal in its own words', async () => {
    await assert.rejects(lite(stub('{"builder":{"_type":"tdsBuilder"}}').fetchStub).execute(QUERY, 0),
      /answered lite's execute with no Arrow stream; it begins: \{"builder"/);
    await assert.rejects(lite(stub('{"code":-1,"status":"error","message":"no such route"}', 404).fetchStub).execute(QUERY, 0),
      /^RemoteExecutionError: no such route$/);
  });

  it('a lite connection names no serialization format: lite\'s answer has one form', () => {
    assert.throws(() => new LegendEngineExecutor({
      baseUrl: 'http://lite:8080', model: '', runtime: 'trades::RT', protocol: 'lite', serializationFormat: 'ARROW_IPC',
    }), /talked to as lite answers lite's own Arrow: it names no serializationFormat \(ARROW_IPC\)/);
  });
});
