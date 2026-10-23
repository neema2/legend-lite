// THE TAB'S DEEPHAVEN SENDER (docs/DEEPHAVEN_PLAN_2026_10_22.md DH21, DH23): a plan the tab's own planner wrote, sent
// straight to a Deephaven server -- no server of legend-lite's in between -- the tables it makes kept alive by the session
// for the next plan to build on, its rows read back as Arrow, and a table published for the live reader to follow
// (deephaven-live.ts). The planner
// writes every byte sent and reads every answer (the protocol's `deephaven` kind: com.legend.deephaven.DeephavenWire),
// the rows aside: they are Arrow's, read by apache-arrow as the tab's DuckDB answers are. This only carries the bytes
// over Deephaven's browser protocol -- gRPC-web: each message a 5-byte frame, the status in a trailer frame, over
// HTTP/2, which a browser speaks only over HTTPS -- keeps the session's tables, and reads the status. It decides
// nothing about the query.

/** The planner's half: one `deephaven` operation answered, as the module tags it (`OK\n...`, `ERR\n<kind>\n<why>`). */
export type DeephavenPlanner = (op: string, args: Readonly<Record<string, unknown>>) => Promise<string>;

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
 * Released, it goes IDLE, not away: the session keeps alive the tables it made, to be read again -- a grid's next query
 * over the same source reads the source's tables rather than making them again -- and lets idle tables go, the least
 * recently used first, past its `keep`.
 */
export interface KeptResult {
  readonly exportId: number;
  /** The plan's operations, and those sent to make it: none when the session kept it alive already. */
  readonly ops: number;
  readonly sent: number;
  release(): void;
}

/** What a session needs of its page: how it fetches (the page's own `fetch`), how often it keeps the session's token
 *  fresh (Deephaven ends a session left unrefreshed; a minute is well inside its five), and how many idle tables it
 *  keeps alive to be read again (128: a grid's last several views; an idle table still ticks on the server, so they
 *  are not free). */
export interface DeephavenSessionOptions {
  readonly fetch?: typeof fetch;
  readonly refreshMs?: number;
  readonly keep?: number;
}

const CONFIG = 'io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants';
const BATCH = 'io.deephaven.proto.backplane.grpc.TableService/Batch';
const PUBLISH = 'io.deephaven.proto.backplane.grpc.SessionService/PublishFromTicket';
const RELEASE = 'io.deephaven.proto.backplane.grpc.SessionService/Release';
const CLOSE = 'io.deephaven.proto.backplane.grpc.SessionService/CloseSession';
const DO_GET = 'arrow.flight.protocol.FlightService/DoGet';

/** A table the session keeps alive: its export, how many readers pin it now, and when it was last read (a count). */
interface Kept {
  readonly exportId: number;
  pins: number;
  used: number;
}

/** One session of the tab's on a Deephaven server: the plans it sends, and the results it publishes. */
export class DeephavenSession {
  private readonly server: DeephavenServer;
  private readonly planner: DeephavenPlanner;
  private readonly fetcher: typeof fetch;
  private auth: string;
  private nextExport = 1;
  private readonly timer: ReturnType<typeof setInterval>;
  private closed = false;
  private readonly keep: number;
  /** The tables the session keeps alive, by their key (DeephavenWire.keys). */
  private readonly tables = new Map<string, Kept>();
  /** The tables a send of the session's is making now, by key: another send that reads one waits for it. */
  private readonly making = new Map<string, Promise<void>>();
  private clock = 0;
  /** Every table the session has made, by key, and how many of them it made again after letting them go. */
  private readonly made = new Set<string>();
  private madeAgain = 0;

  private constructor(server: DeephavenServer, planner: DeephavenPlanner, fetcher: typeof fetch, auth: string,
    options: DeephavenSessionOptions) {
    this.server = server;
    this.planner = planner;
    this.fetcher = fetcher;
    this.auth = auth;
    this.keep = options.keep ?? 128;
    this.timer = setInterval(() => {
      this.call(CONFIG, new Uint8Array()).catch(() => { /* the next call reports a session that ended */ });
    }, options.refreshMs ?? 60_000);
  }

  /** Signs in to `server`: its first call carries the credential, and the server answers with the session's token. */
  static async open(server: DeephavenServer, planner: DeephavenPlanner,
    options: DeephavenSessionOptions = {}): Promise<DeephavenSession> {
    // the page's own fetch, called as a function: a method call would bind it to the session ("Illegal invocation")
    const fetcher = options.fetch ?? ((input: RequestInfo | URL, init?: RequestInit) => fetch(input, init));
    const session = new DeephavenSession(server, planner, fetcher, `${server.signIn.type} ${server.signIn.token}`,
      options);
    try {
      await session.call(CONFIG, new Uint8Array());
      if (!session.auth.startsWith('Bearer ')) {
        throw new DeephavenRefused('sign-in', 16, 'the server answered no session token');
      }
    } catch (e) {
      clearInterval(session.timer);
      throw e;
    }
    return session;
  }

  /**
   * `plan`'s result, kept alive (DH23): the planner names each of its operations' tables by key and writes the request
   * that makes what the session does not keep alive already -- each table it makes exported, to be read again -- and
   * the server's answer for each is checked; a table another send is making now is waited for, not made twice.
   * `supplied`: the tables the session supplies itself, a name to the export that is it (a grid's base, B1): a plan
   * reading one by that name reads the export.
   */
  async keepAlive(plan: string, supplied: Readonly<Record<string, number>> = {}): Promise<KeptResult> {
    this.open();
    const { keys } = JSON.parse(await this.ask('keys', { plan })) as { readonly keys: readonly string[] };
    const resultKey = keys[keys.length - 1];
    if (resultKey === undefined) throw new Error('the planner named no operation of the Deephaven plan');
    for (;;) {
      const making = keys.flatMap((k) => this.making.get(k) ?? []);
      if (making.length === 0) break;
      await Promise.allSettled(making);
    }
    // what is alive is pinned while this reads it; what is not is marked as being made, and its ids set aside
    const pinned = keys.flatMap((k) => this.tables.get(k) ?? []);
    for (const t of pinned) t.pins++;
    const missing = [...new Set(keys.filter((k) => !this.tables.has(k)))];
    let made!: () => void;
    const making = new Promise<void>((done) => { made = done; });
    for (const k of missing) this.making.set(k, making);
    const nextExportId = this.nextExport;
    this.nextExport += missing.length;
    try {
      const alive = Object.fromEntries(keys.flatMap((k) => {
        const t = this.tables.get(k);
        return t ? [[k, t.exportId]] : [];
      }));
      const send = JSON.parse(await this.ask('send', { plan, alive, supplied, nextExportId })) as {
        readonly request: string;
        readonly exports: readonly { readonly key: string; readonly exportId: number }[];
        readonly result: number;
      };
      if (send.exports.length > 0) await this.make(send.request, send.exports);
      const result = this.tables.get(resultKey);
      if (result === undefined || result.exportId !== send.result) {
        throw new Error(`the Deephaven session keeps no table alive for its result (export ${send.result})`);
      }
      result.pins++;
      let released = false;
      return {
        exportId: result.exportId,
        ops: keys.length,
        sent: send.exports.length,
        release: () => {
          if (released) return;
          released = true;
          result.pins--;
          result.used = ++this.clock;
          this.letGo();
        },
      };
    } finally {
      for (const k of missing) if (this.making.get(k) === making) this.making.delete(k);
      made();
      for (const t of pinned) {
        t.pins--;
        t.used = ++this.clock;
      }
      this.letGo();
    }
  }

  /** Sends a batch that makes `exports`, checks the server made each, and keeps them alive; a refusal lets them all
   *  go. */
  private async make(request: string, exports: readonly { readonly key: string; readonly exportId: number }[]):
  Promise<void> {
    const answer = await this.call(BATCH, fromBase64(request));
    let refused: string | null = answer.length === exports.length ? null
      : `the server answered ${answer.length} of the batch's ${exports.length} operations`;
    for (const message of answer) {
      const created = JSON.parse(await this.ask('created', { message: toBase64(message) })) as {
        readonly success: boolean;
        readonly errorInfo: string;
      };
      // the first refusal is the cause: the operations after it that read it fail for it
      if (!created.success && refused === null) refused = created.errorInfo;
    }
    if (refused !== null) {
      for (const e of exports) this.releaseExport(e.exportId);
      throw new DeephavenRefused('Batch', 3, refused);
    }
    for (const e of exports) {
      this.tables.set(e.key, { exportId: e.exportId, pins: 0, used: ++this.clock });
      if (this.made.has(e.key)) this.madeAgain++;
      this.made.add(e.key);
    }
  }

  /** How many tables the session made again after letting them go (past `keep`): none while it keeps every one. */
  get remade(): number {
    return this.madeAgain;
  }

  /** Lets the least recently read idle tables go, past `keep`. */
  private letGo(): void {
    const idle = [...this.tables].filter(([, t]) => t.pins === 0).sort(([, a], [, b]) => a.used - b.used);
    for (const [key, t] of idle.slice(0, Math.max(0, idle.length - this.keep))) {
      this.tables.delete(key);
      this.releaseExport(t.exportId);
    }
  }

  private releaseExport(exportId: number): void {
    if (this.closed) return;
    void this.ask('releaseExport', { exportId })
      .then((request) => this.call(RELEASE, fromBase64(request)))
      .catch(() => { /* a session that ended released it */ });
  }

  /** The rows of the export `exportId` as they are now, an Arrow IPC stream: Arrow Flight's DoGet, its answer's
   *  messages (Flight's `FlightData`, each an Arrow message's header and body) laid end to end as the stream format
   *  lays them. */
  async rows(exportId: number): Promise<Uint8Array> {
    this.open();
    return ipcStream(await this.call(DO_GET, fromBase64(await this.ask('doGet', { exportId }))));
  }

  /**
   * `plan` with its result STORED (B1: its last view a select of the same columns, so the server computes each column
   * once a change and keeps it), the name a query reads it by when the session supplies it -- `legend_base_` and the
   * stored result's key, so one base is one name in any session -- and its columns as the plan names them.
   */
  async stored(plan: string): Promise<{ readonly plan: string; readonly name: string; readonly columns: readonly string[] }> {
    const stored = JSON.parse(await this.ask('stored', { plan })) as { readonly plan: string; readonly columns: string[] };
    const { keys } = JSON.parse(await this.ask('keys', { plan: stored.plan })) as { readonly keys: readonly string[] };
    const key = keys[keys.length - 1];
    if (key === undefined) throw new Error('the planner named no operation of the stored Deephaven plan');
    return { plan: stored.plan, name: `legend_base_${key.slice(0, 16)}`, columns: stored.columns };
  }

  /** Publishes the session's export `exportId` -- a result it keeps alive -- under a fresh name, for another session
   *  to open (deephaven-live.ts): the name lives as long as the export does (measured on Deephaven 42.7,
   *  2026-10-23), and a reader that opened it keeps its own copy. */
  async publishKept(exportId: number): Promise<string> {
    this.open();
    const name = `legend-${randomName()}`;
    await this.call(PUBLISH, fromBase64(await this.ask('publish', { exportId, name })));
    return name;
  }

  /** One call that keeps the session's token fresh; it throws when the server no longer answers this session (a
   *  restart: Deephaven keeps no session across one). */
  async ping(): Promise<void> {
    this.open();
    await this.call(CONFIG, new Uint8Array());
  }

  /** Ends the session: the server releases every table it keeps alive. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.timer);
    await this.call(CLOSE, new Uint8Array());
  }

  private open(): void {
    if (this.closed) throw new Error('the Deephaven session is closed');
  }

  private async ask(op: string, args: Readonly<Record<string, unknown>>): Promise<string> {
    return untag(await this.planner(op, args), op);
  }

  private headers(): Record<string, string> {
    return { 'content-type': 'application/grpc-web+proto', 'x-grpc-web': '1', authorization: this.auth };
  }

  /** One gRPC-web call: its request message framed, its answer's messages unframed, and its status read. */
  private async call(method: string, message: Uint8Array): Promise<Uint8Array[]> {
    const name = method.slice(method.lastIndexOf('/') + 1);
    const response = await this.fetcher(`${this.server.url}/${method}`, {
      method: 'POST',
      body: frame(message),
      headers: this.headers(),
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

/** The module's tagged answer's payload, or its refusal thrown. */
function untag(tagged: string, op: string): string {
  if (tagged.startsWith('OK\n')) return tagged.slice(3);
  const [, kind, ...why] = tagged.split('\n');
  throw new Error(`the planner refused the Deephaven ${op} (${kind}): ${why.join('\n')}`);
}

function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function toBase64(bytes: Uint8Array): string {
  let binary = '';
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}

/** A name no other result of this or any tab's is given: twelve random letters and digits. */
function randomName(): string {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => 'abcdefghijklmnopqrstuvwxyz0123456789'[b % 36]).join('');
}
