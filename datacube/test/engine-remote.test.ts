import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  LegendEngineExecutor,
  RemoteExecutionError,
  pureTypeName,
  toResultTable,
} from '../src/engine-remote.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { expression: '#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#' },
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
  it('normalises the engine’s names, paths and all', () => {
    // It answers with `meta::pure::precisePrimitives::Varchar` for a
    // string and a bare `Float` for a float; the column model reads
    // one name, so the spelling stops here.
    assert.equal(pureTypeName('meta::pure::precisePrimitives::Varchar'),
      'String');
    assert.equal(pureTypeName('Float'), 'Float');
    assert.equal(pureTypeName('meta::pure::precisePrimitives::BigInt'),
      'Integer');
    assert.equal(pureTypeName('Boolean'), 'Boolean');
    assert.equal(pureTypeName('meta::pure::metamodel::type::StrictDate'),
      'StrictDate');
    assert.equal(pureTypeName(undefined), 'Unknown');
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

  it('survives a response with no builder at all', () => {
    const table = toResultTable({
      result: { columns: ['a'], rows: [{ values: [1] }] },
    }, 1, 0);
    assert.deepEqual(table.columns.map((c) => c.name), ['a']);
    assert.equal(table.columns[0]?.type, 'Unknown');
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

  it('names the runtime in the query, and asks the three endpoints', async () => {
    const { calls, fetchStub } = stub([{ parsed: true }, { parsed: true },
      ANSWER]);
    const out = await executor([], fetchStub)
      .execute('$t->select(~[region])', SNAPSHOT);

    assert.deepEqual(calls.map((c) => new URL(c.url).pathname), [
      '/api/pure/v1/grammar/grammarToJson/model',
      '/api/pure/v1/grammar/grammarToJson/lambda',
      '/api/pure/v1/execution/execute',
    ]);
    // The query carries its runtime: our own planners take it
    // out-of-band, the engine does not.
    assert.match(calls[1]?.body ?? '', /->from\(trades::h2::RT\)$/);
    assert.equal(calls[1]?.type, 'text/plain');
    // And the execute call carries the context the engine requires.
    const sent = JSON.parse(calls[2]?.body ?? '{}');
    assert.equal(sent.context._type, 'BaseExecutionContext');
    assert.equal(sent.clientVersion, 'vX_X_X');

    assert.equal(out.rows.rowCount, 2);
    // The SQL is REPORTED, not run: it comes back as an activity.
    assert.match(out.sql, /^select "trades_0"\.region/);
  });

  it('parses the model ONCE across queries', async () => {
    const { calls, fetchStub } = stub([{ parsed: true }, { parsed: true },
      ANSWER]);
    const e = executor([], fetchStub);
    await e.execute('$t->select(~[region])', SNAPSHOT);
    await e.execute('$t->select(~[notional])', SNAPSHOT);
    const models = calls.filter((c) => c.url.endsWith('/model'));
    assert.equal(models.length, 1,
      'the model is the biggest part of the payload and does not change');
  });

  it('retries the model after a failed parse', async () => {
    // Caching a REJECTION would leave the cube permanently broken by
    // one bad moment: every later query would be refused without an
    // attempt.
    let first = true;
    const fetchStub = (async () => {
      if (first) {
        first = false;
        return new Response('{"message":"parse failed"}', { status: 500 });
      }
      return new Response(JSON.stringify(ANSWER), { status: 200 });
    }) as unknown as typeof fetch;
    const e = executor([], fetchStub);
    await assert.rejects(
      () => e.execute('$t->select(~[region])', SNAPSHOT),
      (err: Error) => err instanceof RemoteExecutionError
        && /parse failed/.test(err.message),
    );
    const out = await e.execute('$t->select(~[region])', SNAPSHOT);
    assert.equal(out.rows.rowCount, 2);
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
      () => executor([], fetchStub).execute('$t->select(~[x])', SNAPSHOT),
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
      () => executor([], fetchStub).execute('$t->select(~[x])', SNAPSHOT),
      (err: Error) => err instanceof RemoteExecutionError
        && /could not reach the engine at http:\/\/engine:6300/.test(
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
        .execute('$t->select(~[x])', SNAPSHOT, undefined, controller.signal),
      (err: Error) => err === reason,
    );
  });
});
