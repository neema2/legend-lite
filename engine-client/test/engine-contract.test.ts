// THE CLIENT SIDE OF DATACUBE'S ENGINE CONTRACT (engine-contract.ts, engine-wire.ts; docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md):
// the version refused by name; a call or option the capabilities omit refused by name BEFORE anything is sent (R6);
// an engine's failure read back as the typed error it is, its execution id kept; a closed session refusing its calls;
// a query's Arrow answer and its contract facts read. Over a fake wire, so what crosses is counted.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { tableFromArrays, tableToIPC } from 'apache-arrow';

import {
  ENGINE_CONTRACT_VERSION, isEngineError, refusalOf, requireContract, Refused,
  type Capabilities,
} from '../src/engine-contract.ts';
import { CONTRACT_PATH, wireEngine, type WireSend } from '../src/engine-wire.ts';

const CAPS: Capabilities = {
  contract: ENGINE_CONTRACT_VERSION,
  engine: { name: 'Fake', databaseType: 'DuckDB' },
  calls: { typeOf: true, query: true, cancel: true, describe: false, drop: false, parse: true, print: true, elements: true,
    write: { modes: ['create'] } },
};

const json = (status: number, body: unknown) => ({ status, contentType: 'application/json', body: new TextEncoder().encode(JSON.stringify(body)) });

/** A fake engine across a wire: each call's path recorded, answered by `answer` (capabilities and open answered here). */
function fake(answer: (call: string, body: Record<string, unknown>) => Awaited<ReturnType<WireSend>>, caps: unknown = CAPS) {
  const sent: string[] = [];
  const send: WireSend = async (path, body) => {
    const call = path.slice(CONTRACT_PATH.length);
    sent.push(call);
    if (call === 'capabilities') return json(200, caps);
    if (call === 'open') return json(200, { session: 's-1' });
    if (call === 'close') return json(200, {});
    return answer(call, JSON.parse(body) as Record<string, unknown>);
  };
  return { engine: wireEngine(send, 'the fake'), sent };
}

const ASK = { model: '###Relational\nDatabase a::DB ( Table T ( ID INTEGER ) )\n', runtime: 'a::RT', query: { _type: 'lambda' as const, parameters: [], body: [] } };

describe("DataCube's engine contract, the client's side", () => {
  it('refuses a contract version it does not speak, by name', async () => {
    assert.throws(() => requireContract({ ...CAPS, contract: 2 }), (e: unknown) => e instanceof Refused && /contract version 2/.test(e.message));
    const { engine } = fake(() => json(200, {}), { ...CAPS, contract: 2 });
    await assert.rejects(() => engine.open(), /contract version 2/);
  });

  it('refuses a call or an option the capabilities omit, before anything is sent', async () => {
    const { engine, sent } = fake(() => assert.fail('nothing is sent'));
    const s = await engine.open();
    const before = sent.length;
    await assert.rejects(() => s.describe({ handle: 'h' }), (e: unknown) => isEngineError(e) && e.kind === 'refused' && /describe/.test(e.message));
    await assert.rejects(() => s.addSource({ url: 'https://x/a.csv' }, 'live'), /addSource/);
    await assert.rejects(() => s.write({ box: ASK, target: { table: 't' }, mode: 'append' }), /mode 'append'/);
    assert.equal(sent.length, before, 'no call crossed');
    assert.equal(refusalOf(CAPS, 'addSource', { source: 'query', how: 'live' })?.kind, 'refused');
  });

  it("reads an engine's failure back as its typed error, its execution id kept", async () => {
    const { engine } = fake((call) => (call === 'typeOf'
      ? json(422, { error: { kind: 'compile', call: 'typeOf', message: 'no such column' } })
      : json(422, { error: { kind: 'run', call: 'query', message: 'Conversion Error', executionId: 'x-1' } })));
    const s = await engine.open();
    await assert.rejects(() => s.typeOf(ASK), (e: unknown) => isEngineError(e) && e.kind === 'compile' && e.message === 'no such column');
    await assert.rejects(() => s.query(ASK), (e: unknown) => isEngineError(e) && e.kind === 'run' && e.executionId === 'x-1');
  });

  it("says a link's failure as a transport error, and an answer of no kind as one too", async () => {
    const down = wireEngine(async () => { throw new Error('connection refused'); }, 'the fake');
    await assert.rejects(() => down.capabilities(), (e: unknown) => isEngineError(e) && e.kind === 'transport');
    const { engine } = fake(() => ({ status: 502, contentType: 'text/html', body: new TextEncoder().encode('<html>bad gateway') }));
    const s = await engine.open();
    await assert.rejects(() => s.typeOf(ASK), (e: unknown) => isEngineError(e) && e.kind === 'transport' && /502/.test(e.message));
  });

  it("reads a query's Arrow rows and its contract facts, and refuses rows without them", async () => {
    const table = tableFromArrays({ ID: Int32Array.from([1, 2, 3]) });
    const facts = { executionId: 'e-7', schema: [{ name: 'ID', type: 'Integer' }], explain: 'SELECT …' };
    const withFacts = (meta: Map<string, string>) => {
      const t = tableFromArrays({ ID: Int32Array.from([1, 2, 3]) });
      for (const [k, v] of meta) t.schema.metadata.set(k, v);
      return tableToIPC(t, 'stream');
    };
    const { engine } = fake((_call, body) => ({ status: 200, contentType: 'application/vnd.apache.arrow.stream',
      body: (body['ask'] as { runtime: string }).runtime === 'bare' ? tableToIPC(table, 'stream')
        : withFacts(new Map([['legend.contract', JSON.stringify(facts)]])) }));
    const s = await engine.open();
    const answer = await s.query(ASK);
    assert.equal(answer.executionId, 'e-7');
    assert.deepEqual(answer.schema, facts.schema);
    assert.equal(answer.explain, 'SELECT …');
    let rows = 0;
    for await (const b of answer.rows) rows += b.numRows;
    assert.equal(rows, 3);
    await assert.rejects(() => s.query({ ...ASK, runtime: 'bare' }), /without the contract's facts/);
  });

  it('refuses every call of a closed session, by name, nothing sent', async () => {
    const { engine, sent } = fake(() => assert.fail('nothing is sent'));
    const s = await engine.open();
    await s.close();
    const before = sent.length;
    await assert.rejects(() => s.typeOf(ASK), /closed/);
    assert.equal(sent.length, before);
  });
});
