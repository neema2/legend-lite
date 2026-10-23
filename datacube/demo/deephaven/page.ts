// THE LIVE RISK GRID (docs/DEEPHAVEN_PLAN_2026_10_22.md D0): a plain page standing in for DataCube until its ticking
// mode lands (L4). The browser does all of it -- the WebAssembly planner plans the risk view on the Deephaven runtime,
// the tab's sender sends it straight to the Deephaven server, and the watch streams the grid's rows -- with no server of
// legend-lite's in between. When the server goes away (a restart: Deephaven keeps nothing across one, and its start-up
// script seeds the data again), the page says so, and once the server answers again it sends the plan again and
// watches the new result.

import { DeephavenSession, type DeephavenServer, type LiveTable } from '../../../engine-client/src/deephaven.ts';
import { watch, type LiveRows, type LiveWatch } from '../../../engine-client/src/deephaven-live.ts';
// @ts-ignore the module's runtime, served beside the page from //wasm:planner
import { load } from './wasm-gc-module-runtime.js';

const MODEL = (host: string, port: number): string => `###Relational
Database risk::Store
(
  Table instrument (iid VARCHAR(8) PRIMARY KEY, sector VARCHAR(16) NOT NULL, ccy VARCHAR(3) NOT NULL)
  Table book (bid VARCHAR(8) PRIMARY KEY, desk VARCHAR(16) NOT NULL, region VARCHAR(8) NOT NULL)
  Table position (pid INTEGER PRIMARY KEY, book_id VARCHAR(8) NOT NULL, inst_id VARCHAR(8) NOT NULL, qty BIGINT NOT NULL)
  Table price (px_iid VARCHAR(8) PRIMARY KEY, px DOUBLE NOT NULL)
  Table fx (fx_ccy VARCHAR(3) PRIMARY KEY, rate DOUBLE NOT NULL)
)
###Connection
RelationalDatabaseConnection risk::Live
{
  store: risk::Store;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime risk::LiveRuntime
{
  mappings: [];
  connections: [ risk::Store: [ live: risk::Live ] ];
}`;

const JOIN = 'meta::pure::functions::relation::JoinKind.LEFT';
/** The risk view: every position valued at its instrument's live price in dollars, by desk and sector. */
export const QUERY = `#>{risk::Store.position}#`
  + `->join(#>{risk::Store.book}#, ${JOIN}, {p, b | $p.book_id == $b.bid})`
  + `->join(#>{risk::Store.instrument}#, ${JOIN}, {p, i | $p.inst_id == $i.iid})`
  + `->join(#>{risk::Store.price}#, ${JOIN}, {p, q | $p.inst_id == $q.px_iid})`
  + `->join(#>{risk::Store.fx}#, ${JOIN}, {p, f | $p.ccy == $f.fx_ccy})`
  + `->extend(~[mv:x|$x.qty * $x.px * $x.rate])`
  + `->groupBy(~[desk, sector], ~[mv:x|$x.mv:y|$y->sum(), positions:x|$x.pid:y|$y->count()])`
  + `->sort([~desk->ascending(), ~sector->ascending()])`;

/** What the page shows, for a person and for the demo's test. */
interface Shown {
  status: 'starting' | 'live' | 'reconnecting';
  updates: number;
  reconnects: number;
  columns: readonly string[];
  rows: readonly (readonly unknown[])[];
  plan: string;
  error: string;
}
const shown: Shown = { status: 'starting', updates: 0, reconnects: 0, columns: [], rows: [], plan: '', error: '' };
(window as unknown as { __demo: Shown }).__demo = shown;

const plain = (v: unknown): unknown => (v !== null && typeof v === 'object' && 'asNumber' in v
  ? (v as { asNumber(): number }).asNumber() : v);
const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const byId = (id: string): HTMLElement => {
  const e = document.getElementById(id);
  if (!e) throw new Error(`the page has no #${id}`);
  return e;
};

let previous = new Map<string, number>();

function render(rows: LiveRows): void {
  shown.updates++;
  shown.columns = rows.columns;
  shown.rows = rows.rows.map((r) => r.map(plain));
  const body = byId('rows');
  const next = new Map<string, number>();
  body.replaceChildren(...shown.rows.map((r) => {
    const [desk, sector, mv, positions] = r as [string, string, number, number];
    const key = `${desk}|${sector}`;
    next.set(key, mv);
    const was = previous.get(key);
    const tr = document.createElement('tr');
    for (const [text, cls] of [[desk, ''], [sector, ''], [money.format(mv), 'num'], [String(positions), 'num']] as const) {
      const td = document.createElement('td');
      td.textContent = text;
      if (cls) td.className = cls;
      tr.append(td);
    }
    if (was !== undefined && was !== mv) tr.children[2]!.classList.add(mv > was ? 'up' : 'down');
    return tr;
  }));
  previous = next;
  const total = shown.rows.reduce((s, r) => s + (r[2] as number), 0);
  byId('total').textContent = money.format(total);
  byId('updates').textContent = `${shown.updates} updates`;
}

function status(s: Shown['status'], why = ''): void {
  shown.status = s;
  shown.error = why;
  const e = byId('status');
  // a server that went away is the browser's "Failed to fetch": said plainly
  const said = /Failed to fetch|NetworkError|ERR_CONNECTION/.test(why) ? 'Deephaven is not answering' : why;
  e.textContent = s === 'live' ? 'live' : s === 'reconnecting' ? `reconnecting${said ? `: ${said}` : ''}` : 'starting';
  e.className = s;
  // the last rows stay on screen, dimmed, until the server answers again
  document.body.classList.toggle('stale', s !== 'live');
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string };
  const server: DeephavenServer = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  const at = new URL(config.url);
  const teavm = await load('classes.wasm', { stackDeobfuscator: { enabled: false } });
  const exports = teavm.exports as {
    planOrError(model: string, query: string, runtime: string): string;
    deephavenOrError(op: string, args: string): string;
  };
  const planned = exports.planOrError(MODEL(at.hostname, Number(at.port)), QUERY, 'risk::LiveRuntime');
  if (!planned.startsWith('OK\n')) throw new Error(`the planner refused the risk view: ${planned}`);
  shown.plan = (JSON.parse(planned.slice(3)) as { sql: string }).sql;
  byId('query').textContent = QUERY.replace(/->/g, '\n  ->');
  byId('plan').textContent = JSON.stringify(JSON.parse(shown.plan), null, 1);
  const planner = async (op: string, args: Readonly<Record<string, unknown>>): Promise<string> =>
    exports.deephavenOrError(op, JSON.stringify(args));

  for (;;) {
    let session: DeephavenSession | null = null;
    let table: LiveTable | null = null;
    let watching: LiveWatch | null = null;
    try {
      session = await DeephavenSession.open(server, planner);
      table = await session.publish(shown.plan);
      watching = await watch(server, table.name, (rows) => {
        render(rows);
        if (shown.status !== 'live') status('live');
      }, { first: 0, last: 99, intervalMs: 200 });
      // the server's answer to a call is the page's only sign it is still there
      for (;;) {
        await sleep(1000);
        await session.ping();
      }
    } catch (e) {
      status('reconnecting', String(e instanceof Error ? e.message : e).slice(0, 160));
    }
    watching?.close();
    await table?.release().catch(() => { /* the server that held it is gone */ });
    await session?.close().catch(() => { /* likewise */ });
    shown.reconnects++;
    await sleep(1000);
  }
}

main().catch((e) => status('reconnecting', String(e)));
