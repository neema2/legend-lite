// THE TAB'S DEEPHAVEN SESSION (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §7.1; DH21, DH23): a plan the planner wrote, sent
// straight to a Deephaven server -- no server of legend-lite's in between -- the tables its steps make kept by the
// session for the next plan to read, its rows read back as Arrow, and a result published for the live reader to follow
// (deephaven-live.ts). Every request is one the plan declares (deephaven-steps.ts: a step's bytes, or the session's own
// requests): this writes its session's export ids into the places the plan lists, sends, reads the answers and checks
// them against the declared tables (DH16). It is given no planner, so it can ask none; it encodes nothing. It chooses
// only when: a table let go when nothing reads it (the keep, a cache bound), and how the caller takes the rows (fetch
// once, or follow live). Over Deephaven's browser protocol -- gRPC-web: each message a 5-byte frame, the status in a
// trailer frame, over HTTP/2, which a browser speaks only over HTTPS. Every send is a run's (run-frame.ts inRun).

import { made as readAnswer, filled, publishRequest, stepPlan, NAME_LENGTH, NAME_PREFIX, type SessionRequests, type Step } from './deephaven-steps.ts';
import { inRun, sending } from './run-frame.ts';
import { measured, timed } from './timeline.ts';

/** How the tab signs in: the person's own credential, never a server's secret (DH21) -- an authentication handler's
 *  name and its token (a test server's pre-shared key: `io.deephaven.authentication.psk.PskAuthenticationHandler`). */
export interface DeephavenSignIn {
  readonly type: string;
  readonly token: string;
}

/** A Deephaven server: its HTTPS origin, and how the tab signs in to it. */
export interface DeephavenServer {
  readonly url: string;
  readonly signIn: DeephavenSignIn;
}

/** What a Deephaven server refused: the call, gRPC's status and the server's own words. */
export class DeephavenRefused extends Error {
  readonly call: string;
  readonly status: number;
  constructor(call: string, status: number, message: string) {
    super(`Deephaven refused ${call} (status ${status}): ${message}`);
    this.name = 'DeephavenRefused';
    this.call = call;
    this.status = status;
  }
}

/**
 * A plan's result, KEPT ALIVE for its reader (DH23): the export the session reads its rows by, pinned until `release`.
 * Released, it goes IDLE, not away: the session keeps a kept step's table, to be read again by the next plan that
 * names it -- a grid's base by every query over it, a result by the same query asked again -- and lets idle tables go,
 * the least recently used first, past its `keep`.
 */
export interface KeptResult {
  readonly exportId: number;
  /** The plan's operations, and those sent to make it: none when the session kept it alive already. */
  readonly ops: number;
  readonly sent: number;
  /** The result's columns as the server holds them: each one's name and Deephaven type (`deephaven:type`). */
  readonly columns: readonly DeephavenColumn[];
  release(): void;
}

/** A column of a table the server made, as its schema names it: its name and the Java type the server holds it as
 *  (`int`, `long`, `java.lang.String`; null when the server wrote none). */
export interface DeephavenColumn {
  readonly name: string;
  readonly type: string | null;
}

/** A table the server made otherwise than its store declares (DH16, decided strict): refused by name, never read as if
 *  it held what the store says. */
export class DeephavenMismatch extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DeephavenMismatch';
  }
}

/** What a session needs of its page: how it fetches (the page's own `fetch`), how often it keeps the session's token
 *  fresh (Deephaven ends a session left unrefreshed; a minute is well inside its five), and how many idle tables it
 *  keeps alive to be read again (128: a grid's last several views; an idle table still ticks on the server, so they
 *  are not free). */
export interface DeephavenSessionOptions {
  readonly fetch?: typeof fetch;
  readonly refreshMs?: number;
  readonly keep?: number;
  /** Told when the server no longer knows the session (gRPC's UNAUTHENTICATED on any call after sign-in: it expired --
   *  a laptop asleep past Deephaven's five minutes -- or the server forgot it): its host signs in again (A1). */
  readonly onForgotten?: (why: string) => void;
}

const CONFIG = 'io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants';
const BATCH = 'io.deephaven.proto.backplane.grpc.TableService/Batch';
const PUBLISH = 'io.deephaven.proto.backplane.grpc.SessionService/PublishFromTicket';
const RELEASE = 'io.deephaven.proto.backplane.grpc.SessionService/Release';
const CLOSE = 'io.deephaven.proto.backplane.grpc.SessionService/CloseSession';
const DO_GET = 'arrow.flight.protocol.FlightService/DoGet';

/** A table the session keeps alive: its export, its columns as the server holds them, how many readers pin it now,
 *  and when it was last read (a count). */
interface Kept {
  readonly exportId: number;
  readonly columns: readonly DeephavenColumn[];
  pins: number;
  used: number;
}

/** One session of the tab's on a Deephaven server: the plans it sends, and the results it publishes. */
export class DeephavenSession {
  private readonly server: DeephavenServer;
  private readonly fetcher: typeof fetch;
  private auth: string;
  private nextExport = 1;
  private readonly timer: ReturnType<typeof setInterval>;
  private closed = false;
  private readonly keep: number;
  /** The kept steps' tables, by the identity the planner named each by. */
  private readonly tables = new Map<string, Kept>();
  /** The steps a send of the session's is making now, by identity: another send that reads one waits for it. */
  private readonly making = new Map<string, Promise<void>>();
  private clock = 0;
  /** Every step the session has made, by identity, and how many of them it made again after letting them go. */
  private readonly made = new Set<string>();
  private madeAgain = 0;
  /** The name each export is published under (`publishKept`), as it is asked: one name an export, for as long as it
   *  lives. */
  private readonly published = new Map<number, Promise<string>>();
  /** The session's own requests, as the plans declare them (every plan the same): its releases, row reads and
   *  publishes. None before its first plan, when it has nothing to release, read or publish. */
  private requests: SessionRequests | null = null;
  private readonly options: DeephavenSessionOptions;
  /** Signed in (the server answered a token), and the server since answered that it no longer knows the session. */
  private signedIn = false;
  private forgotten = false;

  private constructor(server: DeephavenServer, fetcher: typeof fetch, auth: string, options: DeephavenSessionOptions) {
    this.options = options;
    this.server = server;
    this.fetcher = fetcher;
    this.auth = auth;
    this.keep = options.keep ?? 128;
    this.timer = setInterval(() => {
      inRun("a Deephaven session's refresh", () => this.call(CONFIG, new Uint8Array()))
        .catch(() => { /* the next call reports a session that ended */ });
    }, options.refreshMs ?? 60_000);
  }

  /** Signs in to `server`: its first call carries the credential, and the server answers with the session's token. */
  static async open(server: DeephavenServer, options: DeephavenSessionOptions = {}): Promise<DeephavenSession> {
    // the page's own fetch, called as a function: a method call would bind it to the session ("Illegal invocation")
    const fetcher = options.fetch ?? ((input: RequestInfo | URL, init?: RequestInit) => fetch(input, init));
    const session = new DeephavenSession(server, fetcher, `${server.signIn.type} ${server.signIn.token}`, options);
    try {
      await inRun('a Deephaven sign-in', () => session.call(CONFIG, new Uint8Array()));
      if (!session.auth.startsWith('Bearer ')) {
        throw new DeephavenRefused('sign-in', 16, 'the server answered no session token');
      }
      session.signedIn = true;
    } catch (e) {
      clearInterval(session.timer);
      throw e;
    }
    return session;
  }

  /**
   * `plan`'s result, kept alive (DH23): each of the plan's steps the session does not hold made -- its request sent as
   * the planner wrote it, the session's ids in the places it lists -- and checked against the store tables it declares
   * (DH16); a step another send is making now is waited for, not made twice. `onSent` is told the result's export as
   * soon as its request is written, before the server has made it: a read of it then waits for it on the server, so a
   * reader asks while it is made (G1).
   */
  keepAlive(plan: string, onSent?: (exportId: number) => void): Promise<KeptResult> {
    this.open();
    return inRun('a Deephaven plan', () => this.run(plan, onSent));
  }

  private async run(planText: string, onSent?: (exportId: number) => void): Promise<KeptResult> {
    const plan = stepPlan(planText);
    this.requests ??= plan.requests;
    const results: number[] = [];
    const pinned: Kept[] = [];
    let ops = 0;
    let sent = 0;
    try {
      for (const [i, step] of plan.steps.entries()) {
        ops += step.operations;
        const last = i === plan.steps.length - 1;
        const kept = await this.step(step, results, last ? onSent : undefined);
        if (kept.sent) sent += step.operations;
        kept.table.pins++;
        pinned.push(kept.table);
        results.push(kept.table.exportId);
      }
    } finally {
      // every step but the result is let go of now; the result stays pinned until its reader lets it go
      for (const t of pinned.slice(0, -1)) {
        t.pins--;
        t.used = ++this.clock;
      }
    }
    const result = pinned[pinned.length - 1]!;
    let released = false;
    this.letGo();
    return {
      exportId: result.exportId,
      ops,
      sent,
      columns: result.columns,
      release: () => {
        if (released) return;
        released = true;
        result.pins--;
        result.used = ++this.clock;
        this.letGo();
      },
    };
  }

  /** One step's table: the one the session keeps under its identity, or made now -- `reads`, the export of each step
   *  before it, written where the step reads them. */
  private async step(step: Step, reads: readonly number[], onSent?: (exportId: number) => void): Promise<{
    readonly table: Kept;
    readonly sent: boolean;
  }> {
    for (;;) {
      const making = this.making.get(step.identity);
      if (making === undefined) break;
      await making.catch(() => undefined);
    }
    const held = this.tables.get(step.identity);
    if (held !== undefined) {
      onSent?.(held.exportId);
      return { table: held, sent: false };
    }
    let done!: () => void;
    const making = new Promise<void>((resolve) => { done = resolve; });
    this.making.set(step.identity, making);
    try {
      const ids = step.exports.map(() => this.nextExport++);
      const places = step.exports.map((e, i) => ({ at: e.at, id: ids[i]! }));
      for (const r of step.reads ?? []) {
        const id = reads[r.step];
        if (id === undefined) throw new Error(`a Deephaven step reads step ${r.step}, which the plan has not made`);
        places.push({ at: r.at, id });
      }
      const request = filled(step.request, places);
      const result = ids[ids.length - 1]!;
      onSent?.(result);
      let answer: Uint8Array[];
      try {
        answer = await timed('batch', () => this.call(BATCH, request));
      } catch (e) {
        // no answer: what the server made of it, if anything, is let go -- never an export the session does not keep
        for (const id of ids) this.releaseExport(id);
        throw e;
      }
      const reading = performance.now();
      const read = readAnswer(answer, step, ids);
      measured('created', reading);
      // the store tables' reads exported for their check alone: let go now
      for (const id of ids.slice(0, -1)) this.releaseExport(id);
      if (read.refused !== null || read.mismatch !== null) {
        this.releaseExport(result);
        if (read.refused !== null) throw new DeephavenRefused('Batch', 3, read.refused);
        throw new DeephavenMismatch(read.mismatch!);
      }
      const table: Kept = { exportId: result, columns: read.columns.get(result) ?? [], pins: 0, used: ++this.clock };
      if (step.kept) {
        this.tables.set(step.identity, table);
        if (this.made.has(step.identity)) this.madeAgain++;
        this.made.add(step.identity);
      }
      return { table, sent: true };
    } finally {
      if (this.making.get(step.identity) === making) this.making.delete(step.identity);
      done();
    }
  }

  /** How many tables the session made again after letting them go (past `keep`): none while it keeps every one. */
  get remade(): number {
    return this.madeAgain;
  }

  /** How many tables the session keeps alive now, read or idle: each one the server keeps current. */
  get kept(): number {
    return this.tables.size;
  }

  /** Lets the least recently read idle tables go, past `keep`. */
  private letGo(): void {
    const idle = [...this.tables].filter(([, t]) => t.pins === 0).sort(([, a], [, b]) => a.used - b.used);
    for (const [identity, t] of idle.slice(0, Math.max(0, idle.length - this.keep))) {
      this.tables.delete(identity);
      this.releaseExport(t.exportId);
    }
  }

  /** The export let go, by the plan's declared release (its id in the request's place). */
  private releaseExport(exportId: number): void {
    this.published.delete(exportId);
    const requests = this.requests;
    if (this.closed || requests === null) return;
    const request = filled(requests.release.request, [{ at: requests.release.export, id: exportId }]);
    inRun('a Deephaven release', () => this.call(RELEASE, request))
      .catch(() => { /* a session that ended released it */ });
  }

  /** The rows of the export `exportId` as they are now, an Arrow IPC stream: Arrow Flight's DoGet by the plan's
   *  declared ticket, its answer's messages (Flight's `FlightData`, each an Arrow message's header and body) laid end
   *  to end as the stream format lays them. */
  async rows(exportId: number): Promise<Uint8Array> {
    this.open();
    const requests = this.requests;
    if (requests === null) throw new Error('the Deephaven session has run no plan whose rows it could read');
    const ticket = filled(requests.fetch.ticket, [{ at: requests.fetch.export, id: exportId }]);
    return ipcStream(await inRun("a Deephaven result's read", () => timed('doGet', () => this.call(DO_GET, ticket))));
  }

  /** Publishes the session's export `exportId` -- a result it keeps alive -- under a fresh name, for another session
   *  to open (deephaven-live.ts): the name lives as long as the export does (measured on Deephaven 42.7,
   *  2026-10-23), and a reader that opened it keeps its own copy, so an export is published once, and asked again
   *  answers the same name. It may be asked as soon as the export is named, before the server has made it: the
   *  server publishes it once it has (N1: a round trip less before a view is followed). */
  publishKept(exportId: number): Promise<string> {
    this.open();
    const asked = this.published.get(exportId);
    if (asked !== undefined) return asked;
    const requests = this.requests;
    if (requests === null) return Promise.reject(new Error('the Deephaven session has run no plan whose result it could publish'));
    const chars = randomName(NAME_LENGTH);
    const name = NAME_PREFIX + chars;
    const publishing = inRun("a Deephaven result's publish", () => this.call(PUBLISH, publishRequest(requests, exportId, chars)))
      .then(() => name);
    this.published.set(exportId, publishing);
    // a refused publish is asked again by the next reader, never answered refused for good
    publishing.catch(() => { if (this.published.get(exportId) === publishing) this.published.delete(exportId); });
    return publishing;
  }

  /** One call that keeps the session's token fresh; it throws when the server no longer answers this session (a
   *  restart: Deephaven keeps no session across one). */
  async ping(): Promise<void> {
    this.open();
    await inRun("a Deephaven session's refresh", () => this.call(CONFIG, new Uint8Array()));
  }

  /** Ends the session: the server releases every table it keeps alive. The request outlives the page (`keepalive`): a
   *  tab closed or reloaded ends its session as it goes, never leaving its tables kept current on the server until
   *  Deephaven expires the session (A1: measured, a reloaded page's old session still cost the server its tables). */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.timer);
    await inRun("a Deephaven session's close", () => this.call(CLOSE, new Uint8Array(), true));
  }

  private open(): void {
    if (this.closed) throw new Error('the Deephaven session is closed');
  }

  private headers(): Record<string, string> {
    return { 'content-type': 'application/grpc-web+proto', 'x-grpc-web': '1', authorization: this.auth };
  }

  /** One gRPC-web call: its request message framed, its answer's messages unframed, and its status read. */
  private async call(method: string, message: Uint8Array, keepalive = false): Promise<Uint8Array[]> {
    const name = method.slice(method.lastIndexOf('/') + 1);
    // the session's one send: refused outside a run where the host keeps a flow's frame (run-frame.ts, Node)
    sending(`a Deephaven session's ${name}`);
    const response = await this.fetcher(`${this.server.url}/${method}`, {
      method: 'POST',
      body: frame(message),
      headers: this.headers(),
      ...(keepalive ? { keepalive: true } : {}),
    });
    if (!response.ok) throw new DeephavenRefused(name, 14, `HTTP ${response.status} ${await response.text()}`);
    const token = response.headers.get('authorization');
    if (token) this.auth = token;
    const bytes = new Uint8Array(await response.arrayBuffer());
    const messages: Uint8Array[] = [];
    let status = response.headers.get('grpc-status');
    let why = response.headers.get('grpc-message') ?? '';
    for (let at = 0; at < bytes.length;) {
      const flag = bytes[at];
      if (flag === undefined || at + 5 > bytes.length) {
        throw new DeephavenRefused(name, 13, 'the answer ended inside a frame');
      }
      const length = new DataView(bytes.buffer, bytes.byteOffset + at + 1, 4).getUint32(0);
      const frame = bytes.slice(at + 5, at + 5 + length);
      at += 5 + length;
      if (flag & 0x80) {
        const said = trailer(frame);
        status = said.status ?? status;
        why = said.message ?? why;
      } else {
        messages.push(frame);
      }
    }
    if (status === null) throw new DeephavenRefused(name, 13, 'the answer carried no status');
    // a session the server no longer knows: said once, to the host, which signs in again (never at sign-in itself:
    // a credential refused is the opener's to say)
    if (status === '16' && this.signedIn && !this.forgotten) {
      this.forgotten = true;
      this.options.onForgotten?.(`the Deephaven server no longer knows this session (${decodeURIComponent(why)})`);
    }
    if (status !== '0') throw new DeephavenRefused(name, Number(status), decodeURIComponent(why));
    return messages;
  }
}

/** A trailer frame's status and message (gRPC-web's trailers: `key: value` lines), each null when it has none. */
function trailer(frame: Uint8Array): { readonly status: string | null; readonly message: string | null } {
  let status: string | null = null;
  let message: string | null = null;
  for (const line of new TextDecoder().decode(frame).split('\r\n')) {
    const colon = line.indexOf(':');
    if (colon < 0) continue;
    const key = line.slice(0, colon).trim().toLowerCase();
    if (key === 'grpc-status') status = line.slice(colon + 1).trim();
    if (key === 'grpc-message') message = line.slice(colon + 1).trim();
  }
  return { status, message };
}

/** One gRPC-web frame: a flag byte, the message's length (32 bits, big-endian), the message. */
function frame(message: Uint8Array): Uint8Array<ArrayBuffer> {
  const body = new Uint8Array(5 + message.length);
  new DataView(body.buffer).setUint32(1, message.length);
  body.set(message, 5);
  return body;
}

/**
 * Arrow Flight's `FlightData` messages as one Arrow IPC stream: each message's Arrow header (field 2) and body (field
 * 1000) as the stream format lays a message out -- the continuation marker, the header's length padded to 8 bytes, the
 * header, its padding, the body -- and the end-of-stream marker. The rows' envelope, not an answer the planner judges:
 * only those two fields are read, each by protobuf's own rule (a field number and wire type, then its length).
 */
export function ipcStream(messages: readonly Uint8Array[]): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const m of messages) {
    let header: Uint8Array | null = null;
    let body: Uint8Array = new Uint8Array();
    for (let at = 0; at < m.length;) {
      const [key, afterKey] = varint(m, at);
      const number = Number(key >> 3n);
      const wire = Number(key & 7n);
      if (wire === 0) {
        at = varint(m, afterKey)[1];
      } else if (wire === 2) {
        const [length, start] = varint(m, afterKey);
        const end = start + Number(length);
        if (end > m.length) throw new Error('a Deephaven row message ended inside a field');
        if (number === 2) header = m.subarray(start, end);
        if (number === 1000) body = m.subarray(start, end);
        at = end;
      } else {
        throw new Error(`a Deephaven row message holds wire type ${wire}, which FlightData does not use`);
      }
    }
    // a message without a header carries no Arrow message (Flight's metadata alone)
    if (header === null) continue;
    const pad = (8 - (header.length % 8)) % 8;
    const prefix = new Uint8Array(8);
    const view = new DataView(prefix.buffer);
    view.setUint32(0, 0xffffffff, true);
    view.setInt32(4, header.length + pad, true);
    parts.push(prefix, header, new Uint8Array(pad), body);
  }
  const end = new Uint8Array(8);
  new DataView(end.buffer).setUint32(0, 0xffffffff, true);
  parts.push(end);
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

/** A base-128 varint at `at`: its value, and where the next field starts. */
function varint(bytes: Uint8Array, at: number): [bigint, number] {
  let value = 0n;
  for (let shift = 0n, i = at; shift < 64n; shift += 7n, i++) {
    const b = bytes[i];
    if (b === undefined) throw new Error('a Deephaven row message ended inside a number');
    value |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) return [value, i + 1];
  }
  throw new Error('a Deephaven row message holds a number longer than 64 bits');
}

/** A name no other result of this or any tab's is given: `length` random letters and digits. */
function randomName(length: number): string {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => 'abcdefghijklmnopqrstuvwxyz0123456789'[b % 36]).join('');
}
