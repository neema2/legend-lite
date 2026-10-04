import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  LegendEngineExecutor,
  RemoteExecutionError,
  toResultTable,
} from '../../engine-client/src/engine-remote.ts';
import { pureType } from '../../engine-client/src/relation-type.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { element, fn, fromElement, lambda, toJson } from '../../pure-protocol/src/index.ts';
import { accessor } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: accessor('trades::h2::DB', 'TRADES_SCHEMA', 'TRADES') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 7,
};

/** A cube query, as DataCube builds one: a relation expression, no runtime. */
const QUERY = fromElement('trades::T').select(['region']).lambda();

/** A TDS exactly as legend-engine 4.138.5 sends one. */
const ANSWER = {
  builder: {
    _type: 'tdsBuilder',
    columns: [
      {
        name: 'region',
        type: 'meta::pure::precisePrimitives::Varchar',
        relationalType: 'VARCHAR(1024)',
      },
      { name: 'notional', type: 'Float', relationalType: 'FLOAT' },
    ],
  },
  activities: [
    {
      _type: 'relational',
      comment: '-- "executionTraceID" : "abc"',
      sql: 'select "trades_0".region as "region" from TRADES as "trades_0"',
    },
  ],
  result: {
    columns: ['region', 'notional'],
    rows: [
      { values: ['AMER', 300] },
      { values: ['EMEA', 301] },
    ],
  },
};

/** Records what was asked of the engine, and answers in order. */
function stub(answers: (object | { status: number; body: string })[]) {
  const calls: { url: string; body: string; type: string }[] = [];
  let next = 0;
  const fetchStub = (async (url: string | URL, init?: RequestInit) => {
    const headers = (init?.headers ?? {}) as Record<string, string>;
    calls.push({
      url: String(url),
      body: String(init?.body ?? ''),
      type: headers['Content-Type'] ?? '',
    });
    const answer = answers[Math.min(next++, answers.length - 1)];
    if (answer && 'status' in answer && 'body' in answer) {
      return new Response(answer.body as string,
        { status: answer.status as number });
    }
    return new Response(JSON.stringify(answer), { status: 200 });
  }) as unknown as typeof fetch;
  return { calls, fetchStub };
}

describe('the engine type vocabulary', () => {
  it('reads both vocabularies into one, paths and all, and refuses what it does not know', () => {
    // legend-engine names a relational column with its precise primitive;
    // legend-lite's typer with the plain type. The column model reads one name.
    assert.equal(pureType('meta::pure::precisePrimitives::Varchar'), 'String');
    assert.equal(pureType('Float'), 'Float');
    assert.equal(pureType('meta::pure::precisePrimitives::BigInt'), 'Integer');
    assert.equal(pureType('meta::pure::precisePrimitives::Numeric'), 'Decimal');
    assert.equal(pureType('meta::pure::precisePrimitives::Timestamp'), 'DateTime');
    assert.equal(pureType('Boolean'), 'Boolean');
    assert.equal(pureType('meta::pure::metamodel::type::StrictDate'), 'StrictDate');
    assert.throws(() => pureType('meta::pure::precisePrimitives::Blob'), /does not read/);
  });
});

describe('a TDS as a result table', () => {
  it('reads the columns from the BUILDER and the cells by position', () => {
    const table = toResultTable(ANSWER, 7, 12);
    assert.deepEqual(table.columns.map((c) => [c.name, c.type]), [
      ['region', 'String'],
      ['notional', 'Float'],
    ]);
    assert.deepEqual(table.columns[0]?.values, ['AMER', 'EMEA']);
    assert.deepEqual(table.columns[1]?.values, [300, 301]);
    assert.equal(table.rowCount, 2);
    assert.equal(table.epoch, 7);
    assert.equal(table.elapsedMs, 12);
  });

  it('fills a missing cell with null rather than shifting the row', () => {
    // A short `values` array would otherwise pull every later column
    // one place left, which is the fault that looks like bad data.
    const table = toResultTable({
      ...ANSWER,
      result: { columns: ['region', 'notional'], rows: [{ values: ['X'] }] },
    }, 1, 0);
    assert.deepEqual(table.columns[0]?.values, ['X']);
    assert.deepEqual(table.columns[1]?.values, [null]);
  });

  it('reads the engine\'s timestamps as stored, to the microsecond', () => {
    const table = toResultTable({
      builder: { columns: [{ name: 'ts', type: 'DateTime' }] },
      result: { columns: ['ts'], rows: [{ values: ['2024-01-02T03:04:05.123456000+0000'] }] },
    }, 1, 0);
    assert.equal(table.columns[0]?.values[0], '2024-01-02T03:04:05.123456');
  });

  it('refuses a response with no builder: its columns would have no types', () => {
    assert.throws(() => toResultTable({
      result: { columns: ['a'], rows: [{ values: [1] }] },
    }, 1, 0), /without a result builder/);
  });
});

describe('the engine executor', () => {
  const executor = (answers: object[], fetchStub?: typeof fetch) =>
    new LegendEngineExecutor({
      baseUrl: 'http://engine:6300/',
      model: '###Relational\nDatabase trades::h2::DB()',
      runtime: 'trades::h2::RT',
      fetch: fetchStub ?? stub(answers).fetchStub,
    });

  it('receipts only what the engine sent: where it answered from, the SQL and note its activities report', async () => {
    // The pure/v1 API issues no statement id and keeps no history: the receipt has neither,
    // and nothing is added to what the engine returns.
    const out = await executor([ANSWER]).execute(QUERY, SNAPSHOT.epoch);
    assert.deepEqual(out.rows.receipt, {
      plane: 'engine',
      where: 'the engine at engine:6300',
      serverSql: 'select "trades_0".region as "region" from TRADES as "trades_0"',
      serverNote: '-- "executionTraceID" : "abc"',
    });
    const bare = await executor([{ ...ANSWER, activities: [] }]).execute(QUERY, SNAPSHOT.epoch);
    assert.deepEqual(bare.rows.receipt, { plane: 'engine', where: 'the engine at engine:6300' });
  });

  it('names the runtime in the query, sends the model as text, and asks the one endpoint', async () => {
    const { calls, fetchStub } = stub([ANSWER]);
    const out = await executor([], fetchStub)
      .execute(QUERY, SNAPSHOT.epoch);

    // The query is already protocol: nothing to parse first.
    assert.deepEqual(calls.map((c) => new URL(c.url).pathname), [
      '/api/pure/v1/execution/execute',
    ]);
    assert.equal(calls[0]?.type, 'application/json');
    // The model travels as text, which every server accepts on every
    // call: nothing to parse first, nothing to cache.
    const sent = JSON.parse(calls[0]?.body ?? '{}');
    assert.deepEqual(sent.model, {
      _type: 'text',
      code: '###Relational\nDatabase trades::h2::DB()',
    });
    // The query carries its runtime: our own planners take it
    // out-of-band, the engine does not.
    assert.deepEqual(sent.function, JSON.parse(toJson(
      lambda([], fn('from', QUERY.body[0]!, element('trades::h2::RT'))))));
    // And the execute call carries the context the engine requires.
    assert.equal(sent.context._type, 'BaseExecutionContext');
    assert.equal(sent.clientVersion, 'vX_X_X');

    assert.equal(out.rows.rowCount, 2);
    // The SQL is REPORTED, not run: it comes back as an activity.
    assert.match(out.sql, /^select "trades_0"\.region/);
  });

  it('reports the engine’s own words, not its stack', async () => {
    // Its errors arrive as {code, message, status, trace} with a Java
    // stack hundreds of lines long. The message is the actionable
    // part -- "Can't find a match for function 'toLower(...)'" is what
    // told us what to change.
    const body = JSON.stringify({
      code: -1,
      message: "Can't find a match for function 'toLower(Varchar(32)[0..1])'",
      status: 'error',
      trace: 'java.lang.Exception\n\tat one\n\tat two\n'.repeat(50),
    });
    const fetchStub = (async () =>
      new Response(body, { status: 500 })) as unknown as typeof fetch;
    await assert.rejects(
      () => executor([], fetchStub).execute(QUERY, SNAPSHOT.epoch),
      (err: Error) => {
        assert.ok(err instanceof RemoteExecutionError);
        assert.match(err.message, /Can't find a match for function/);
        assert.equal(/java\.lang/.test(err.message), false,
          'the stack came with it');
        return true;
      },
    );
  });

  it('reports an unreachable engine as unreachable', async () => {
    const fetchStub = (async () => {
      throw new TypeError('fetch failed');
    }) as unknown as typeof fetch;
    await assert.rejects(
      () => executor([], fetchStub).execute(QUERY, SNAPSHOT.epoch),
      (err: Error) => err instanceof RemoteExecutionError
        && /could not reach the server at http:\/\/engine:6300/.test(
          err.message),
    );
  });

  it('passes an ABORT through as the abort it is', async () => {
    // Not as a planner outage: telemetry would read a responsive grid
    // as an engine that keeps failing.
    const controller = new AbortController();
    const reason = new Error('superseded');
    const fetchStub = (async () => {
      controller.abort(reason);
      throw new Error('aborted by the network layer');
    }) as unknown as typeof fetch;
    await assert.rejects(
      () => executor([], fetchStub)
        .execute(QUERY, SNAPSHOT.epoch, controller.signal),
      (err: Error) => err === reason,
    );
  });
});

describe('what a person typed, parsed by the server (E1)', () => {
  it('sends the text, without source positions, and reads the lambda', async () => {
    const { calls, fetchStub } = stub([JSON.parse(toJson(QUERY))]);
    const parsed = await new LegendEngineExecutor({
      baseUrl: 'http://engine:6300/', model: '', runtime: 'trades::h2::RT', fetch: fetchStub,
    }).parse("|trades::T->select(~[region])");
    const url = new URL(calls[0]?.url ?? '');
    assert.equal(url.pathname, '/api/pure/v1/grammar/grammarToJson/lambda');
    assert.equal(url.searchParams.get('returnSourceInformation'), 'false');
    assert.equal(calls[0]?.body, '|trades::T->select(~[region])');
    assert.equal(calls[0]?.type, 'text/plain');
    assert.deepEqual(parsed, QUERY);
  });
});
