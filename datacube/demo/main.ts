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

import { CubeController, type Planner } from '../src/cube.ts';
import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { FormatterCache, type ColumnFormat } from '../src/format.ts';
import { DataGrid } from '../src/grid/grid.ts';
import { LegendLitePlanner } from '../src/planner.ts';
import {
  NULL_GROUP,
  serialize,
  type LevelScope,
} from '../src/serialize.ts';
import { parsePathKey, pathKey, type TreeRow } from '../src/tree.ts';
import { DEFAULT_MAX_ROWS } from '../src/treeview.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { referencedColumns, totalOrderSorts } from '../src/snapshot.ts';

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

  // -- the cube ------------------------------------------------------

  let snapshot: CubeSnapshot = {
    source: { expression: 'trades' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'desk', type: 'String' },
      { name: 'book', type: 'String' },
      { name: 'year', type: 'Integer' },
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

  const planner = await choosePlanner(status);
  const formatters = new FormatterCache();

  // Formats are keyed by the column names the ENGINE returned, not by
  // a name we predicted. Guessing '2021__|__notional' when DuckDB
  // actually emits '2021_notional' is exactly how every measure
  // silently rendered unformatted.
  const MONEY: ColumnFormat = {
    kind: 'currency',
    currency: 'USD',
    locale: 'en-US',
    maximumFractionDigits: 0,
    negativeParens: true,
  };
  const COUNT: ColumnFormat = {
    kind: 'number',
    locale: 'en-US',
    maximumFractionDigits: 0,
  };
  const formats: Record<string, ColumnFormat> = {};

  // Tree rows come from the controller's last view, so the grid's
  // per-row metadata and the data it renders can never disagree.
  let treeRows: readonly TreeRow[] = [];
  let measureFn: 'sum' | 'average' | 'count' = 'sum';

  const grid = new DataGrid(must('grid'), formatters, {
    rowHeight: 24,
    formats,
    rowMeta: (abs) => {
      const row = treeRows[abs];
      if (!row) return { level: 1, key: String(abs) };
      return {
        level: row.depth,
        key: pathKey(row.path),
        ...(row.isGroup ? { expanded: row.expanded } : {}),
        ...(row.isTotal || row.level === 0 ? { isTotal: true } : {}),
      };
    },
    onToggleExpand: (key) => {
      void controller.toggle(parsePathKey(key));
    },
    onActivateCell: (r, c) => {
      status.textContent = `cell r${r} c${c} — drill-through would open here`;
    },
  });

  const controller = new CubeController(engine, planner, {
    onBusy: (busy) => {
      must('busy').hidden = !busy;
    },
    onError: (e) => {
      status.textContent = `error: ${e instanceof Error ? e.message : String(e)}`;
      status.classList.add('bad');
    },
    onView: (view) => {
      status.classList.remove('bad');
      // Keyed off the MEASURE, not off "is it a value column": a
      // trade count is not money, and rendering it as $10,005 is the
      // kind of wrong that looks plausible.
      for (const leaf of view.columns.leaves) {
        if (leaf.isDimension) continue;
        const measure = leaf.path[leaf.path.length - 1];
        formats[leaf.name] = measure === 'trades' ? COUNT : MONEY;
      }
      treeRows = view.treeRows;
      grid.setColumns(view.columns);
      grid.setRows(view.rows, 0, view.rows.rowCount);
      must('sql').textContent = view.sql;
      must('pure').textContent = serialize(view.snapshot);
      const base =
        `${view.rows.rowCount.toLocaleString()} rows × ` +
        `${view.columns.leaves.length} cols in ` +
        `${view.rows.elapsedMs.toFixed(0)}ms`;
      // Saying WHICH level was cut matters: "some rows are missing"
      // sends someone hunting through the whole cube.
      status.textContent =
        view.truncated.length > 0
          ? `${base} — showing the first ` +
            `${(view.snapshot.maxRows ?? DEFAULT_MAX_ROWS).toLocaleString()} ` +
            `of ${view.truncated.length} level` +
            `${view.truncated.length > 1 ? 's' : ''}; narrow the filter to see the rest`
          : base;
      status.classList.toggle('warn-text', view.truncated.length > 0);
    },
  });

  await controller.update(snapshot);
  renderPlaneBadge(controller);

  // -- controls ------------------------------------------------------

  must('measure').addEventListener('change', (e) => {
    measureFn = (e.target as HTMLSelectElement).value as typeof measureFn;
    repivot();
  });

  const repivot = () => {
    const byYear = (must('pivoted') as HTMLInputElement).checked;
    const byQtr = (must('byqtr') as HTMLInputElement).checked;
    const pivotOn = [
      ...(byYear ? ['year'] : []),
      ...(byQtr ? ['qtr'] : []),
    ];
    const twoMeasures = (must('twomeasures') as HTMLInputElement).checked;
    snapshot = {
      ...snapshot,
      pivotOn,
      measures: twoMeasures
        ? [
            { name: 'notional', column: 'notional', fn: measureFn },
            { name: 'trades', column: 'notional', fn: 'count' },
          ]
        : [{ name: 'notional', column: 'notional', fn: measureFn }],
    };
    void controller.update(snapshot);
  };

  must('pivoted').addEventListener('change', repivot);
  must('byqtr').addEventListener('change', repivot);
  must('twomeasures').addEventListener('change', repivot);

  must('sortdesc').addEventListener('change', (e) => {
    const desc = (e.target as HTMLInputElement).checked;
    snapshot = {
      ...snapshot,
      sorts: desc ? [{ column: 'region', direction: 'desc' }] : [],
    };
    void controller.update(snapshot);
  });

  must('maxrows').addEventListener('change', (e) => {
    const n = Number((e.target as HTMLInputElement).value);
    snapshot = {
      ...snapshot,
      ...(Number.isFinite(n) && n > 0 ? { maxRows: n } : {}),
    };
    void controller.update(snapshot);
  });

  must('totals').addEventListener('change', (e) => {
    const show = (e.target as HTMLInputElement).checked;
    void controller.setTree(controller.tree.withTotals(show));
  });

  must('snap').addEventListener('click', async () => {
    const btn = must('snap') as HTMLButtonElement;
    btn.disabled = true;
    try {
      if (controller.snaps.isSnapped) await controller.release();
      else await controller.snap();
      renderPlaneBadge(controller);
    } catch (e) {
      status.textContent = e instanceof Error ? e.message : String(e);
      status.classList.add('bad');
    } finally {
      btn.disabled = false;
    }
  });
}

/** Rule 1 of snap mode: what you are looking at is never inferable. */
function renderPlaneBadge(controller: CubeController): void {
  const badge = must('plane');
  const btn = must('snap');
  const state = controller.snaps.state;
  if (state.mode === 'snapped') {
    const t = state.snap.takenAt.toLocaleTimeString();
    badge.textContent =
      `${state.snap.label} · frozen at ${t} · ` +
      `${state.snap.rowCount.toLocaleString()} rows`;
    badge.className = 'plane snapped';
    btn.textContent = 'Go live';
  } else {
    badge.textContent = 'Live — data may move while you work';
    badge.className = 'plane live';
    btn.textContent = 'Snap';
  }
}

async function choosePlanner(status: HTMLElement): Promise<Planner> {
  try {
    const r = await fetch(`${LEGEND_LITE}/health`, {
      signal: AbortSignal.timeout(700),
    });
    if (r.ok) {
      status.textContent = 'planner: legend-lite';
      return new LegendLitePlanner({
        baseUrl: LEGEND_LITE,
        model: '',
        runtime: 'demo::RT',
      });
    }
  } catch {
    // Not running; fall through to the shim.
  }
  const note = must('plannernote');
  note.hidden = false;
  return new DemoOnlyPlanner();
}

/**
 * DEMO ONLY. Emits SQL straight from the snapshot so the page runs with
 * no server. It is not the product's planner and must never move into
 * src/ -- legend-lite is the single planner, and this exists purely so
 * the grid and snap mode can be seen without a JVM.
 */
class DemoOnlyPlanner implements Planner {
  async plan(
    _grammar: string,
    s: CubeSnapshot,
    scope?: LevelScope,
  ): Promise<string> {
    const q = (n: string) => `"${n.replace(/"/g, '""')}"`;
    const lit = (v: string) => `'${v.replace(/'/g, "''")}'`;
    // Which dimensions group at THIS level, and which branch is pinned.
    const dims = scope ? s.rows.slice(0, scope.level) : s.rows;
    const where = (scope?.parent ?? [])
      .map((v, i) => {
        const col = s.rows[i];
        if (col === undefined) return null;
        return v === NULL_GROUP
          ? `${q(col)} IS NULL`
          : `${q(col)} = ${lit(v)}`;
      })
      .filter((c): c is string => c !== null);
    const filter = where.length > 0 ? ` WHERE ${where.join(' AND ')}` : '';
    const agg = s.measures
      .map((m) =>
        m.fn === 'count'
          ? `count(*) AS ${q(m.name)}`
          : `${m.fn === 'average' ? 'avg' : m.fn}(${q(m.column)}) AS ${q(m.name)}`,
      )
      .join(', ');
    const by = dims.map(q).join(', ');
    const order = totalOrderSorts(s, dims)
      .map((x) => `${q(x.column)} ${x.direction === 'asc' ? 'ASC' : 'DESC'}`)
      .join(', ');
    const cols = referencedColumns(s, dims).map(q).join(', ');
    // A COMPOSITE key, built the way legend-lite does, rather than
    // DuckDB's native multi-column PIVOT: it names columns the same
    // way and, because the key only takes values that occur, it does
    // not emit the cross product of every dimension's values.
    const key = '__pivotkey';
    const keyExpr = s.pivotOn
      .map((c) => `CAST(${q(c)} AS VARCHAR)`)
      .join(" || '__|__' || ");
    const src =
      s.pivotOn.length > 0
        ? `(SELECT * EXCLUDE (${s.pivotOn.map(q).join(', ')}), ` +
          `${keyExpr} AS ${q(key)} ` +
          `FROM (SELECT ${cols} FROM ${s.source.expression}${filter}))`
        : `(SELECT ${cols} FROM ${s.source.expression}${filter})`;
    const orderBy = order ? ` ORDER BY ${order}` : '';

    if (s.pivotOn.length === 0) {
      return dims.length === 0
        ? `SELECT ${agg} FROM ${src}`
        : `SELECT ${by}, ${agg} FROM ${src} GROUP BY ${by}${orderBy}`;
    }
    const on = q(key);
    // DuckDB joins the pivot value to the alias with a single '_', so
    // the alias is pre-compensated to land on the canonical five-
    // character separator -- exactly what legend-lite emits.
    const alias = (n: string) => q(`_|__${n}`);
    const using = s.measures
      .map((m) =>
        m.fn === 'count'
          ? `count(*) AS ${alias(m.name)}`
          : `${m.fn === 'average' ? 'avg' : m.fn}(${q(m.column)}) ` +
            `AS ${alias(m.name)}`,
      )
      .join(', ');
    // With no grouping columns the pivot is the grand total: one row,
    // no GROUP BY and nothing to order.
    return dims.length === 0
      ? `SELECT * FROM (PIVOT ${src} ON ${on} USING ${using})`
      : `SELECT * FROM (PIVOT ${src} ON ${on} USING ${using} ` +
        `GROUP BY ${by})${orderBy}`;
  }
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
