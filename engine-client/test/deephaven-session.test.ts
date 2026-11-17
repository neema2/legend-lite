// WHAT A DEEPHAVEN SESSION SENDS IS THE PLAN'S (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §9): every request the session
// sends is one the plan declares, byte for byte, only the places it lists filled with the session's export ids; a
// kept step the session holds is not sent again; a table held otherwise than its store declares is refused by name
// (DH16). A capturing stand-in for the server (no Deephaven here: the lanes read a real one), under Node's run frame
// (frameKept): a send outside a run would be refused by the session's own send.

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import { Field, Int32, Schema, Table, Utf8, tableToIPC } from 'apache-arrow';

import { DeephavenMismatch, DeephavenSession } from '../src/deephaven.ts';
import { filled, stepPlan, type Step } from '../src/deephaven-steps.ts';
import { frameKept } from '../src/run-frame.ts';
import { loadPlannerModule } from './planner.ts';

const MODEL = `###Relational
Database risk::Ref
(
  Table trades (id INTEGER PRIMARY KEY, desk VARCHAR(16) NOT NULL, qty BIGINT)
)
###Connection
RelationalDatabaseConnection risk::Conn
{
  store: risk::Ref;
  type: Deephaven;
  specification: Deephaven { host: 'localhost'; port: 8443; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime risk::RT
{
  mappings: [];
  connections: [ risk::Ref: [ c: risk::Conn ] ];
}`;

const BATCH = 'io.deephaven.proto.backplane.grpc.TableService/Batch';
const RELEASE = 'io.deephaven.proto.backplane.grpc.SessionService/Release';

/** A protobuf length-delimited field. */
function lengthDelimited(number: number, bytes: Uint8Array): Uint8Array {
  const out = [(number << 3) | 2];
  let n = bytes.length;
  do {
    out.push((n & 0x7f) | (n > 0x7f ? 0x80 : 0));
    n >>>= 7;
  } while (n > 0);
  return Uint8Array.from([...out, ...bytes]);
}

/** A schema message whose columns are `columns`, each with its `deephaven:type`, as the server writes one. */
function schema(columns: readonly { readonly name: string; readonly type: string }[]): Uint8Array {
  const fields = columns.map((c) => new Field(c.name, c.type === 'java.lang.String' ? new Utf8() : new Int32(), true,
    new Map([['deephaven:type', c.type]])));
  const stream = tableToIPC(new Table(new Schema(fields)), 'stream');
  return stream.subarray(0, stream.length - 8);
}

/** One `ExportedTableCreationResponse`: the export `id` made, with `columns`. */
function made(id: number, columns: readonly { readonly name: string; readonly type: string }[]): Uint8Array {
  const ticket = new Uint8Array(5);
  ticket[0] = 0x65;
  new DataView(ticket.buffer).setInt32(1, id, true);
  const reference = lengthDelimited(1, lengthDelimited(1, ticket));
  return Uint8Array.from([...lengthDelimited(1, reference), 0x10, 0x01, ...lengthDelimited(4, schema(columns))]);
}

/** A gRPC-web answer: each message framed, then the status trailer. */
function answer(messages: readonly Uint8Array[]): Response {
  const parts: number[] = [];
  for (const m of messages) {
    const head = new Uint8Array(5);
    new DataView(head.buffer).setUint32(1, m.length);
    parts.push(...head, ...m);
  }
  const trailer = new TextEncoder().encode('grpc-status: 0\r\n');
  const head = new Uint8Array(5);
  head[0] = 0x80;
  new DataView(head.buffer).setUint32(1, trailer.length);
  parts.push(...head, ...trailer);
  return new Response(Uint8Array.from(parts), { status: 200, headers: { authorization: 'Bearer t' } });
}

/** A request's message, its gRPC-web frame taken off. */
function unframed(body: Uint8Array): Uint8Array {
  return body.subarray(5);
}

/** A stand-in server: each request kept by its method; a batch answered for each export the step lists, as `held`
 *  says the server holds each store's column (its declaration, unless told otherwise). */
function server(step: () => Step, held: (name: string, type: string) => string = (_n, t) => t) {
  const sent: { readonly method: string; readonly message: Uint8Array }[] = [];
  const fetcher = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = String(input);
    const method = url.slice(url.indexOf('/', 'https://'.length) + 1);
    const message = unframed(new Uint8Array(init!.body as ArrayBuffer | Uint8Array as ArrayBuffer));
    sent.push({ method, message });
    if (method !== BATCH) return answer([]);
    const s = step();
    const ids = s.exports.map((e) => new DataView(message.buffer, message.byteOffset + e.at, 4).getInt32(0, true));
    return answer(s.exports.map((e, i) => made(ids[i]!, e.declared?.columns.map((c) => ({ name: c.name,
      type: held(c.column, c.type) })) ?? [])));
  }) as typeof fetch;
  return { sent, fetcher };
}

describe('what a Deephaven session sends is the plan\'s', () => {
  let plan: string;
  let step: Step;
  before(async () => {
    const m = await loadPlannerModule() as unknown as { exports: { planOrError(model: string, query: string, runtime: string): string } };
    const a = m.exports.planOrError(MODEL, "#>{risk::Ref.trades}#->filter(x|$x.desk == 'FX')->select(~[id, desk])",
      'risk::RT');
    assert.ok(a.startsWith('OK\n'), a);
    plan = (JSON.parse(a.slice(3)) as { sql: string }).sql;
    step = stepPlan(plan).steps[0]!;
  });

  it('runs under the run frame here, where a send outside a run is refused', () => {
    assert.equal(frameKept, true);
  });

  it('sends the step\'s bytes with its export ids in the places listed, and nothing else differs', async () => {
    const { sent, fetcher } = server(() => step);
    const session = await DeephavenSession.open({ url: 'https://dh', signIn: { type: 'psk', token: 'k' } },
      { fetch: fetcher, refreshMs: 1e9 });
    const kept = await session.keepAlive(plan);
    const batches = sent.filter((r) => r.method === BATCH);
    assert.equal(batches.length, 1);
    const ids = step.exports.map((_e, i) => i + 1);
    assert.deepEqual(batches[0]!.message, filled(step.request, step.exports.map((e, i) => ({ at: e.at, id: ids[i]! }))));
    // only the listed places differ from the planner's bytes
    const planned = Buffer.from(step.request, 'base64');
    const places = new Set(step.exports.flatMap((e) => [e.at, e.at + 1, e.at + 2, e.at + 3]));
    for (let i = 0; i < planned.length; i++) {
      if (!places.has(i)) assert.equal(batches[0]!.message[i], planned[i], `byte ${i}`);
    }
    // the store's read, exported for its check alone, is let go by the plan's declared release
    const requests = stepPlan(plan).requests;
    const releases = sent.filter((r) => r.method === RELEASE).map((r) => r.message);
    assert.deepEqual(releases, ids.slice(0, -1).map((id) => filled(requests.release.request,
      [{ at: requests.release.export, id }])));
    assert.equal(kept.exportId, ids[ids.length - 1]);
    assert.equal(kept.sent, step.operations);
    kept.release();
    // the same plan again: its kept step read from the session, nothing sent
    const again = await session.keepAlive(plan);
    assert.equal(sent.filter((r) => r.method === BATCH).length, 1);
    assert.equal(again.exportId, kept.exportId);
    assert.equal(again.sent, 0);
    again.release();
    await session.close();
  });

  it('refuses a store\'s table the server holds otherwise than its store declares, by name (DH16)', async () => {
    const { fetcher } = server(() => step, (name, type) => (name === 'id' ? 'long' : type));
    const session = await DeephavenSession.open({ url: 'https://dh', signIn: { type: 'psk', token: 'k' } },
      { fetch: fetcher, refreshMs: 1e9 });
    await assert.rejects(session.keepAlive(plan), (e: unknown) => e instanceof DeephavenMismatch
      && /the server's table trades is not what its store declares: id is held as long; its store declares INTEGER/
        .test(e.message));
    await session.close();
  });
});
