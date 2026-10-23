// THE DEMO SERVER'S OWN ACCOUNT OF A GESTURE (docs/DEEPHAVEN_PLAN_2026_10_22.md P1, O1): Deephaven's query operation
// performance log -- every operation the server made, its time, and which were compilations -- read through Deephaven's
// JS library and a console session of the demo's server (a test's diagnostic: a locked-down server has no console). A
// test asks what the server did since a moment: its operations' time and its compiling; and has the console change the
// demo's data (a book moved to another region).

/** One operation the server logged: what it was, when it started (ms since the epoch), how long it took, how deep it
 *  sat under another, and whether it was a compilation. */
export interface ServerOp {
  readonly description: string;
  readonly startMs: number;
  readonly ms: number;
  readonly depth: number;
  readonly compiling: boolean;
}

interface DhColumn { readonly name: string }
interface DhRow { get(column: DhColumn): unknown }
interface DhTable {
  readonly columns: readonly DhColumn[];
  createSubscription(o: { readonly columns: readonly DhColumn[]; readonly updateIntervalMs: number }): {
    addEventListener(name: string, listener: (e: { readonly detail: { readonly rows: readonly DhRow[] } }) => void): void;
    close(): void;
  };
}
interface Dh {
  readonly CoreClient: new (url: string) => {
    login(c: { readonly type: string; readonly token: string }): Promise<void>;
    getAsIdeConnection(): Promise<{
      startSession(type: string): Promise<{ runCode(code: string): Promise<{ readonly error?: string }> }>;
      getObject(o: { readonly type: string; readonly name: string }): Promise<DhTable>;
    }>;
  };
  readonly Table: { readonly EVENT_UPDATED: string };
}

const plain = (v: unknown): unknown => (v !== null && typeof v === 'object' && 'asNumber' in v
  ? (v as { asNumber(): number }).asNumber() : v);

/** The server's operation log, opened on the server at `url` with its (test) key; `since(ms)`: what it made since;
 *  `run(code)`: the code run in the console (a test's change to the demo's data). */
export async function openServerLog(url: string, psk: string): Promise<{
  since(ms: number): Promise<ServerOp[]>;
  run(code: string): Promise<void>;
}> {
  const dh = ((await import(/* @vite-ignore */ `${url}/jsapi/dh-core.js`)) as { default: Dh }).default;
  const client = new dh.CoreClient(url);
  await client.login({ type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: psk });
  const ide = await client.getAsIdeConnection();
  const console_ = await ide.startSession('groovy');
  const ran = await console_.runCode('__qopl = io.deephaven.engine.table.impl.util.TableLoggers.queryOperationPerformanceLog()');
  if (ran.error) throw new Error(`the server's operation log: ${ran.error}`);
  const log = await ide.getObject({ type: 'Table', name: '__qopl' });
  const col = (name: string): DhColumn => {
    const c = log.columns.find((x) => x.name === name);
    if (!c) throw new Error(`the operation log has no ${name}: ${log.columns.map((x) => x.name).join(', ')}`);
    return c;
  };
  const [description, start, duration, depth, compiling] = ['Description', 'StartTime', 'UsageNanos', 'Depth',
    'IsCompilation'].map(col) as [DhColumn, DhColumn, DhColumn, DhColumn, DhColumn];
  return {
    run: async (code) => {
      const done = await console_.runCode(code);
      if (done.error) throw new Error(`the server's console: ${done.error}`);
    },
    since: (ms) => new Promise((resolve) => {
      const sub = log.createSubscription({ columns: log.columns, updateIntervalMs: 50 });
      sub.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
        sub.close();
        resolve(e.detail.rows.map((r) => ({
          description: String(r.get(description)),
          startMs: Number(plain(r.get(start))) / 1e6,
          ms: Number(plain(r.get(duration))) / 1e6,
          depth: Number(plain(r.get(depth))),
          compiling: r.get(compiling) === true,
        })).filter((op) => op.startMs >= ms));
      });
    }),
  };
}
