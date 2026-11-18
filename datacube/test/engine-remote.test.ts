import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { LegendEngineExecutor, toResultTable } from '../../engine-client/src/engine-remote.ts';
import { RemoteExecutionError } from '../../engine-client/src/remote-executor.ts';
import { pureType } from '../../engine-client/src/relation-type.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { fromElement } from '../../pure-protocol/src/index.ts';
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

