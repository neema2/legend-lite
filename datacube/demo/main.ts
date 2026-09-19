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
import type { ColumnFormat } from '../src/format.ts';
import { LegendLitePlanner } from '../src/planner.ts';
import {
  NULL_GROUP,
  serialize,
  type LevelScope,
} from '../src/serialize.ts';
import type { CubeSnapshot, FilterNode } from '../src/snapshot.ts';
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

  const snapshot: CubeSnapshot = {
    source: { expression: 'trades' },
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

  const planner = await choosePlanner(status);

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

  const app = new CubeApp(must('app'), snapshot, {
    engine,
    planner,
    configuration,
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
      must('sql').textContent = view.sql;
      must('pure').textContent = serialize(view.snapshot);
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
/**
 * FilterNode to SQL, for the demo shim only.
 *
 * Mirrors src/serialize.ts's filterExpression, which renders the same
 * tree to Pure. Two renderers of one tree is exactly the duplication
 * the single-planner rule exists to prevent -- which is why this one
 * lives in demo/ and is never imported by the product.
 */
function filterToSql(node: FilterNode, q: (n: string) => string): string {
  const lit = (v: unknown): string =>
    typeof v === 'number' || typeof v === 'boolean'
      ? String(v)
      : `'${String(v).replace(/'/g, "''")}'`;

  switch (node.kind) {
    case 'and':
    case 'or': {
      if (node.children.length === 0) return node.kind === 'and' ? 'TRUE' : 'FALSE';
      const op = node.kind === 'and' ? ' AND ' : ' OR ';
      return `(${node.children.map((c) => filterToSql(c, q)).join(op)})`;
    }
    case 'not':
      return `NOT (${filterToSql(node.child, q)})`;
    case 'condition': {
      const col = q(node.column);
      const lower = `lower(${col})`;
      const one = () => lit(node.value);
      const many = () =>
        ((node.value as unknown[]) ?? []).map(lit).join(', ');
      const lowerMany = () =>
        ((node.value as unknown[]) ?? [])
          .map((v) => lit(String(v).toLowerCase()))
          .join(', ');
      const right = node.rightColumn ? q(node.rightColumn) : col;

      switch (node.operator) {
        case 'equal': return `${col} = ${one()}`;
        case 'notEqual': return `${col} <> ${one()}`;
        case 'lessThan': return `${col} < ${one()}`;
        case 'lessThanEqual': return `${col} <= ${one()}`;
        case 'greaterThan': return `${col} > ${one()}`;
        case 'greaterThanEqual': return `${col} >= ${one()}`;
        case 'isEmpty': return `${col} IS NULL`;
        case 'isNotEmpty': return `${col} IS NOT NULL`;
        case 'in': return `${col} IN (${many()})`;
        case 'notIn': return `${col} NOT IN (${many()})`;
        case 'contains': return `${col} LIKE ${lit('%' + String(node.value) + '%')}`;
        case 'notContains': return `${col} NOT LIKE ${lit('%' + String(node.value) + '%')}`;
        case 'startsWith': return `${col} LIKE ${lit(String(node.value) + '%')}`;
        case 'notStartsWith': return `${col} NOT LIKE ${lit(String(node.value) + '%')}`;
        case 'endsWith': return `${col} LIKE ${lit('%' + String(node.value))}`;
        case 'notEndsWith': return `${col} NOT LIKE ${lit('%' + String(node.value))}`;
        case 'equalCaseInsensitive':
          return `${lower} = ${lit(String(node.value).toLowerCase())}`;
        case 'notEqualCaseInsensitive':
          return `${lower} <> ${lit(String(node.value).toLowerCase())}`;
        case 'containsCaseInsensitive':
          return `${lower} LIKE ${lit('%' + String(node.value).toLowerCase() + '%')}`;
        case 'startsWithCaseInsensitive':
          return `${lower} LIKE ${lit(String(node.value).toLowerCase() + '%')}`;
        case 'endsWithCaseInsensitive':
          return `${lower} LIKE ${lit('%' + String(node.value).toLowerCase())}`;
        case 'inCaseInsensitive': return `${lower} IN (${lowerMany()})`;
        case 'notInCaseInsensitive': return `${lower} NOT IN (${lowerMany()})`;
        case 'equalColumn': return `${col} = ${right}`;
        case 'notEqualColumn': return `${col} <> ${right}`;
        case 'lessThanColumn': return `${col} < ${right}`;
        case 'lessThanEqualColumn': return `${col} <= ${right}`;
        case 'greaterThanColumn': return `${col} > ${right}`;
        case 'greaterThanEqualColumn': return `${col} >= ${right}`;
        case 'equalCaseInsensitiveColumn':
          return `${lower} = lower(${right})`;
        case 'notEqualCaseInsensitiveColumn':
          return `${lower} <> lower(${right})`;
        default: return 'TRUE';
      }
    }
  }
}

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
    const where = [
      // The cube's own filter comes first, then the branch pins.
      ...(s.filter ? [filterToSql(s.filter, q)] : []),
      ...(scope?.parent ?? [])
        .map((v, i) => {
          const col = s.rows[i];
          if (col === undefined) return null;
          return v === NULL_GROUP
            ? `${q(col)} IS NULL`
            : `${q(col)} = ${lit(v)}`;
        })
        .filter((c): c is string => c !== null),
    ];
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
