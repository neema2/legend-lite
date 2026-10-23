// THE TAB'S DEEPHAVEN SENDER (docs/DEEPHAVEN_PLAN_2026_10_22.md DH21): a plan the tab's own planner wrote, sent
// straight to a Deephaven server -- no server of legend-lite's in between -- and its live result published under a
// shared name that the JS library the Deephaven server serves opens (deephaven-live.ts). The planner writes every byte
// sent and reads every answer (the protocol's `deephaven` kind: com.legend.deephaven.DeephavenWire); this only carries
// them over Deephaven's browser protocol -- gRPC-web: each message a 5-byte frame, the status in a trailer frame, over
// HTTP/2, which a browser speaks only over HTTPS -- and reads the status. It decides nothing about the query.

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

/** A plan's live result on the server, published under `name` until it is released (releasing its export ends the
 *  name too: measured on Deephaven 42.7, 2026-10-23). A watch already open keeps its own copy. */
export interface LiveTable {
  readonly name: string;
  release(): Promise<void>;
}

/** What a session needs of its page: how it fetches (the page's own `fetch`), and how often it keeps the session's
 *  token fresh (Deephaven ends a session left unrefreshed; a minute is well inside its five). */
export interface DeephavenSessionOptions {
  readonly fetch?: typeof fetch;
  readonly refreshMs?: number;
}

const CONFIG = 'io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants';
const BATCH = 'io.deephaven.proto.backplane.grpc.TableService/Batch';
const PUBLISH = 'io.deephaven.proto.backplane.grpc.SessionService/PublishFromTicket';
const RELEASE = 'io.deephaven.proto.backplane.grpc.SessionService/Release';
const CLOSE = 'io.deephaven.proto.backplane.grpc.SessionService/CloseSession';

/** One session of the tab's on a Deephaven server: the plans it sends, and the results it publishes. */
export class DeephavenSession {
  private readonly server: DeephavenServer;
  private readonly planner: DeephavenPlanner;
  private readonly fetcher: typeof fetch;
  private auth: string;
  private nextExport = 1;
  private readonly timer: ReturnType<typeof setInterval>;
  private closed = false;

  private constructor(server: DeephavenServer, planner: DeephavenPlanner, fetcher: typeof fetch, auth: string,
    refreshMs: number) {
    this.server = server;
    this.planner = planner;
    this.fetcher = fetcher;
    this.auth = auth;
    this.timer = setInterval(() => {
      this.call(CONFIG, new Uint8Array()).catch(() => { /* the next call reports a session that ended */ });
    }, refreshMs);
  }

  /** Signs in to `server`: its first call carries the credential, and the server answers with the session's token. */
  static async open(server: DeephavenServer, planner: DeephavenPlanner,
    options: DeephavenSessionOptions = {}): Promise<DeephavenSession> {
    // the page's own fetch, called as a function: a method call would bind it to the session ("Illegal invocation")
    const fetcher = options.fetch ?? ((input: RequestInfo | URL, init?: RequestInit) => fetch(input, init));
    const session = new DeephavenSession(server, planner, fetcher, `${server.signIn.type} ${server.signIn.token}`,
      options.refreshMs ?? 60_000);
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

  /** Sends `plan` (a Deephaven plan's text, as the planner wrote it) and publishes its result under a fresh name. */
  async publish(plan: string): Promise<LiveTable> {
    this.open();
    const exportId = this.nextExport++;
    const answer = await this.call(BATCH, fromBase64(await this.ask('batch', { plan, exportId })));
    if (answer.length === 0) throw new DeephavenRefused('Batch', 2, 'the server answered no operation');
    for (const message of answer) {
      const created = JSON.parse(await this.ask('created', { message: toBase64(message) })) as {
        readonly success: boolean;
        readonly errorInfo: string;
      };
      if (!created.success) throw new DeephavenRefused('Batch', 3, created.errorInfo);
    }
    const name = `legend-${randomName()}`;
    await this.call(PUBLISH, fromBase64(await this.ask('publish', { exportId, name })));
    let released = false;
    return {
      name,
      release: async () => {
        if (released || this.closed) return;
        released = true;
        await this.call(RELEASE, fromBase64(await this.ask('releaseExport', { exportId })));
      },
    };
  }

  /** Ends the session: the server releases every table it holds. */
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

  /** One gRPC-web call: its request message framed, its answer's messages unframed, and its status read. */
  private async call(method: string, message: Uint8Array): Promise<Uint8Array[]> {
    const body = new Uint8Array(5 + message.length);
    new DataView(body.buffer).setUint32(1, message.length);
    body.set(message, 5);
    const name = method.slice(method.lastIndexOf('/') + 1);
    const response = await this.fetcher(`${this.server.url}/${method}`, {
      method: 'POST',
      body,
      headers: { 'content-type': 'application/grpc-web+proto', 'x-grpc-web': '1', authorization: this.auth },
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
        for (const line of new TextDecoder().decode(frame).split('\r\n')) {
          const colon = line.indexOf(':');
          if (colon < 0) continue;
          const key = line.slice(0, colon).trim().toLowerCase();
          if (key === 'grpc-status') status = line.slice(colon + 1).trim();
          if (key === 'grpc-message') why = line.slice(colon + 1).trim();
        }
      } else {
        messages.push(frame);
      }
    }
    if (status === null) throw new DeephavenRefused(name, 13, 'the answer carried no status');
    if (status !== '0') throw new DeephavenRefused(name, Number(status), decodeURIComponent(why));
    return messages;
  }
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
