// A runnable demo: real DuckDB-WASM in the browser, real snap mode,
// the real grid.
//
// The planner is the one piece that needs the legend-lite server. If it
// is reachable the demo uses it; otherwise it falls back to a shim that
// emits SQL directly, clearly labelled in the UI so nobody mistakes the
// fallback for the product. That shim lives HERE, in demo/, and not in
// src/, because "one planner" is an architectural commitment and a
// convenient second planner is exactly how such commitments rot.

import * as duckdb from '@duckdb/duckdb-wasm';

import { CubeApp } from '../src/app.ts';
import {
  DEFAULT_CONFIGURATION,
  type CubeConfiguration,
} from '../src/config.ts';
import { CubeController, type Planner } from '../src/cube.ts';
import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { mountRemote } from '../src/remote.ts';
import type { ColumnFormat } from '../src/format.ts';
import { LegendLitePlanner } from '../src/planner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const ROWS = 200_000;
const LEGEND_LITE = 'http://localhost:8080';

// -- sample data ----------------------------------------------------

const REGIONS = ['EMEA', 'AMER', 'APAC'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equity', 'Commodities'];

async function boot(): Promise<void> {
  const status = must('status');
  status.textContent = 'starting DuckDB…';

  // Bundles are served from OUR origin, copied out of node_modules by
  // `npm run demo:vendor`. Loading them from a CDN instead forces a
  // cross-origin Worker, which the platform forbids outright and which
  // is then usually worked around with a blob that importScripts the
  // CDN url. That workaround exists to solve a problem worth not
  // having: a deployment behind a firewall is not fetching its query
  // engine from a CDN anyway.
  // Absolute URLs, not relative ones. The WORKER resolves mainModule
  // against its own location, so './vendor/x.wasm' becomes
  // '/demo/vendor/vendor/x.wasm' and fails as an opaque
  // "WebAssembly.compile: HTTP status code is not ok".
  const asset = (f: string) => new URL(`./vendor/${f}`, location.href).href;
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: asset('duckdb-mvp.wasm'),
      mainWorker: asset('duckdb-browser-mvp.worker.js'),
    },
    eh: {
      mainModule: asset('duckdb-eh.wasm'),
      mainWorker: asset('duckdb-browser-eh.worker.js'),
    },
  });
  const worker = new Worker(bundle.mainWorker!);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();
  const engine = new DuckDbEngine(conn as unknown as ArrowishConnection);

  // A REMOTE SOURCE, when one is named.
  //
  //   ?remote=https://host/trades.parquet
  //   ?remote=s3://bucket/table&format=iceberg
  //
  // The data stays where it is: DuckDB reads it over HTTP range
  // requests, so the cube pulls the bytes a query needs rather than
  // the file. Everything downstream is unchanged, because the remote
  // file is mounted as a VIEW called `trades` -- the same name the
  // generated table would have had, and the name the model already
  // refers to.
  const params = new URLSearchParams(location.search);
  const remote = params.get('remote');
  if (remote) {
    status.textContent = `mounting ${remote}…`;
    const format = params.get('format');
    await mountRemote(engine, {
      sources: [{
        name: 'trades',
        url: remote,
        ...(format === 'parquet' || format === 'csv' || format === 'iceberg'
          ? { format }
          : {}),
      }],
      // Credentials come from the host, never from the URL bar: a
      // query string lands in history, logs and shoulder-surfing
      // range. A bucket that needs them is configured by the
      // embedding application.
    });
    status.textContent = `reading ${remote}`;
  } else {

  status.textContent = `generating ${ROWS.toLocaleString()} rows…`;
  await engine.execute(
    `CREATE OR REPLACE TABLE trades AS
     SELECT
       ${sqlPick(REGIONS, 'i % 3')}            AS region,
       ${sqlPick(DESKS, '(i // 3) % 5')}       AS desk,
       (2021 + ((i // 15) % 5))                AS year,
       ('Q' || (1 + ((i // 75) % 4)))          AS qtr,
       ('Book ' || (1 + ((i // 300) % 4)))     AS book,
       ((i * 7919) % 1000000) / 100.0  AS notional,
       ((i * 104729) % 200000) / 100.0 - 1000.0 AS pnl,
       ((i * 31) % 97) + 1             AS qty
     FROM range(${ROWS}) t(i)`,
    0,
  );
  }

  // -- the cube ------------------------------------------------------

  const { planner, source, snapTarget } = await requireEngine(status);

  const snapshot: CubeSnapshot = {
    source: { expression: source },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'desk', type: 'String' },
      { name: 'book', type: 'String' },
      { name: 'year', type: 'Integer', kind: 'dimension' },
      { name: 'qtr', type: 'String' },
      { name: 'notional', type: 'Float' },
      { name: 'pnl', type: 'Float' },
      { name: 'qty', type: 'Integer' },
    ],
    derived: [],
    rows: ['region', 'desk', 'book'],
    pivotOn: ['year'],
    measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
    sorts: [],
    epoch: 1,
  };


  // The page builds the APP, not a grid and a pile of checkboxes.
  // Those checkboxes were the demo standing in for a product; what
  // they reached is now reachable from the toolbar, the drag zones,
  // the context menu and the properties editor -- which is the whole
  // reason src/app.ts exists.
  // The host knows what the snapshot cannot: that notional and pnl
  // are money and qty is a count. Rendering a trade count as $10,005
  // is the kind of wrong that looks plausible.
  const MONEY: ColumnFormat = {
    kind: 'currency',
    currency: 'USD',
    locale: 'en-US',
    maximumFractionDigits: 0,
    negativeParens: true,
  };
  const configuration: CubeConfiguration = {
    ...DEFAULT_CONFIGURATION,
    reportTitle: 'Trades',
    showSelectionStats: true,
    columns: {
      notional: { format: MONEY },
      pnl: { format: MONEY },
      qty: {
        format: { kind: 'number', locale: 'en-US', maximumFractionDigits: 0 },
      },
    },
  };

  must('plannerreal').hidden = false;

  const app = new CubeApp(must('app'), snapshot, {
    engine,
    planner,
    configuration,
    snapTarget,
    storage: window.localStorage,
    showColumnZone: true,
    dimensions: [
      { name: 'Geography', columns: ['region', 'desk', 'book'] },
      { name: 'Calendar', columns: ['year', 'qtr'] },
    ],
    writeClipboard: (text) => navigator.clipboard?.writeText(text),
    download: (name, mime, text) => {
      const url = URL.createObjectURL(new Blob([text], { type: mime }));
      const a = document.createElement('a');
      a.href = url;
      a.download = name;
      a.click();
      URL.revokeObjectURL(url);
    },
    onStatus: (text, kind) => {
      status.textContent = text;
      status.classList.toggle('bad', kind === 'error');
      status.classList.toggle('warn-text', kind === 'warn');
    },
    onView: (view) => {
      // The Pure this product emitted, and the SQL the planner made
      // of it. Both, because they answer different questions -- and
      // because the SQL panel showed Pure until the real planner
      // started returning SQL worth reading.
      must('pure').textContent = view.pure;
      must('sql').textContent =
        view.sql || '(the demo shim plans per level; expand a row)';
    },
    onPlane: () => renderPlaneBadge(app.controller),
  });

  await app.open();
  renderPlaneBadge(app.controller);
}

/** Rule 1 of snap mode: what you are looking at is never inferable. */
function renderPlaneBadge(controller: CubeController): void {
  // The BADGE says which plane you are on; the snap button lives on
  // the app's own toolbar now. A mode indicator that is only a
  // button label is a mode indicator people miss.
  const badge = must('plane');
  const state = controller.snaps.state;
  if (state.mode === 'snapped') {
    const t = state.snap.takenAt.toLocaleTimeString();
    badge.textContent =
      `${state.snap.label} · frozen at ${t} · ` +
      `${state.snap.rowCount.toLocaleString()} rows`;
    badge.className = 'plane snapped';
  } else {
    badge.textContent = 'Live — data may move while you work';
    badge.className = 'plane live';
  }
}

/**
 * The planner. There is exactly one, and it is the real one.
 *
 * This used to CHOOSE between legend-lite and a demo-only shim
 * depending on whether the server answered a health check, and that
 * one line of convenience hid three real bugs for the whole life of
 * the project: snap building SQL by hand, the snapped plane never
 * redirecting, and the "Generated SQL" panel showing Pure. Every one
 * of them was invisible because the shim's source was a bare SQL
 * identifier and the broken path happened to work against it.
 *
 * So there is no fallback. A test may INJECT a stub planner -- that
 * choice is static, in code that never ships -- but the product
 * cannot select one at runtime. If legend-lite is not running, this
 * throws and the page says so; it does not quietly show fake numbers
 * that look exactly like real ones.
 */
async function requireEngine(status: HTMLElement): Promise<{
  readonly planner: Planner;
  readonly source: string;
  readonly snapTarget: { readonly table: string; readonly expression: string };
}> {
  let reachable = false;
  try {
    const health = await fetch(`${LEGEND_LITE}/health`, {
      signal: AbortSignal.timeout(1500),
    });
    reachable = health.ok;
  } catch {
    reachable = false;
  }
  if (!reachable) {
    must('plannermissing').hidden = false;
    throw new Error(
      `legend-lite is not answering on ${LEGEND_LITE}. ` +
        'Start it with `npm run engine` and reload.',
    );
  }

  // The model is fetched rather than inlined so the SAME text is what
  // the server compiles and what a reader opens -- one copy, in
  // demo/trades.pure.
  const model = await (await fetch('./trades.pure')).text();
  status.textContent = 'planner: legend-lite';
  return {
    planner: new LegendLitePlanner({
      baseUrl: LEGEND_LITE,
      model,
      runtime: 'trades::RT',
    }),
    source: '#>{trades::DB.TRADES}#',
    snapTarget: {
      table: 'TRADES_SNAP',
      expression: '#>{trades::DB.TRADES_SNAP}#',
    },
  };
}

/**
 * Pick a label by an explicit index expression.
 *
 * The index is passed in rather than derived from the value count,
 * because deriving it gave every dimension the same `i % n` and made
 * them perfectly correlated: each desk then had exactly one year, so
 * four of five pivot columns were legitimately null and the grid
 * looked broken. Independent divisors make every combination occur.
 */
function sqlPick(values: readonly string[], indexExpr: string): string {
  const cases = values.map((v, i) => `WHEN ${i} THEN '${v}'`).join(' ');
  return `CASE (${indexExpr}) ${cases} END`;
}

function must(id: string): HTMLElement {
  const el = document.getElementById(id);
  if (!el) throw new Error(`missing #${id}`);
  return el;
}

void boot().catch((e) => {
  const s = document.getElementById('status');
  if (s) {
    s.textContent = `failed to start: ${e instanceof Error ? e.message : e}`;
    s.classList.add('bad');
  }
});
