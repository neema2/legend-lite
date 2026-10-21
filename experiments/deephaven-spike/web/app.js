// The spike's viewer. It asks the connector (standing in for legend-lite) to run a plan and gets
// back a shared ticket; from then on it talks to Deephaven only: it fetches the live table by that
// ticket and subscribes to the rows in view. Deephaven sends a snapshot of the viewport, then
// batches of changes no more often than the chosen interval.
//
// Everything a test reads is on `window.spikeMetrics`.

import dh from '/jsapi/dh-core.js';

const ROW_HEIGHT = 22;
const OVERSCAN = 10;
const FLASH_MS = 700;

const metrics = { connected: false, connectMillis: null, panels: {} };
window.spikeMetrics = metrics;

const statusEl = document.getElementById('status');
const intervalEl = document.getElementById('interval');
const panelsEl = document.getElementById('panels');
const plansEl = document.getElementById('plans');

const started = performance.now();
const config = await (await fetch('/api/config')).json();
const client = new dh.CoreClient(config.deephaven);
await client.login(config.login);
const ide = await client.getAsIdeConnection();
metrics.connected = true;
metrics.connectMillis = Math.round(performance.now() - started);
statusEl.textContent = `connected to ${config.deephaven} in ${metrics.connectMillis} ms`;

const panels = new Map();
for (const name of config.plans) {
  const b = document.createElement('button');
  b.textContent = name;
  b.addEventListener('click', () => openPanel(name));
  plansEl.append(b);
}
intervalEl.addEventListener('change', () => panels.forEach((p) => p.subscribe()));

/** Opens a plan's panel: run (or reuse) the plan, fetch its live table, subscribe to the view. */
async function openPanel(name) {
  if (panels.has(name)) return panels.get(name);
  const clicked = performance.now();
  const handle = await (await fetch(`/api/live?plan=${encodeURIComponent(name)}`, { method: 'POST' })).json();
  if (handle.error) throw new Error(handle.error);
  const answered = performance.now();
  const table = await ide.getSharedObject(handle.sharedTicket, dh.VariableType.TABLE);
  const panel = new Panel(handle, table, { clicked, answered, fetched: performance.now() });
  panels.set(name, panel);
  return panel;
}
window.openPanel = openPanel;
/** For measurement: subscribe to fixed rows (e.g. the whole table) instead of the rows in view; null undoes it. */
window.forceWindow = (name, first, last) => {
  const p = panels.get(name);
  p.forced = first === null ? null : [first, last];
  p.subscribe();
};
window.releaseFromConnector = async (name) => {
  const p = panels.get(name);
  const r = await fetch(`/api/release?ticket=${encodeURIComponent(p.handle.sharedTicket)}`, { method: 'POST' });
  return r.json();
};

class Panel {
  constructor(handle, table, times) {
    this.handle = handle;
    this.table = table;
    this.columns = table.columns;
    this.keyColumns = this.columns.filter((c) => c.type === 'java.lang.String');
    this.tsColumn = this.columns.find((c) => c.name === 'LastTs');
    this.previous = new Map();
    this.flashes = new Map();
    this.subscription = null;
    this.m = {
      plan: handle.plan,
      sharedTicket: handle.sharedTicket,
      reused: handle.reused,
      publishMillis: handle.publishMillis,
      handleMillis: Math.round(times.answered - times.clicked),
      fetchMillis: Math.round(times.fetched - times.answered),
      firstDataMillis: null,
      size: table.size,
      viewport: null,
      events: 0,
      changedCells: 0,
      freshness: [],
      lastEventAt: null,
    };
    metrics.panels[handle.plan] = this.m;
    this.times = times;
    this.render();
    table.addEventListener(dh.Table.EVENT_SIZECHANGED, () => {
      this.m.size = table.size;
      this.body.style.height = `${table.size * ROW_HEIGHT}px`;
      this.subscribe();
    });
    table.addEventListener(dh.Table.EVENT_UPDATED, (e) => this.onData(e.detail));
    this.subscribe();
  }

  render() {
    const el = document.createElement('section');
    el.className = 'panel';
    el.dataset.plan = this.handle.plan;
    const names = this.handle.names || {};
    el.innerHTML = `
      <header>
        <h2>${this.handle.plan}</h2>
        <div class="shape"></div>
        <details><summary>Pure</summary><code></code></details>
        <div class="stats"></div>
      </header>
      <div class="grid"><div class="cols"></div><div class="body"></div></div>`;
    el.querySelector('.shape').textContent = this.handle.shape;
    el.querySelector('code').textContent = this.handle.pure;
    this.statsEl = el.querySelector('.stats');
    this.grid = el.querySelector('.grid');
    this.body = el.querySelector('.body');
    const cols = el.querySelector('.cols');
    for (const c of this.columns) {
      const h = document.createElement('div');
      h.className = `colhead${isNumeric(c) ? ' num' : ''}`;
      h.textContent = names[c.name] ?? c.name;
      h.title = `${c.name}: ${c.type}`;
      cols.append(h);
    }
    const width = this.columns.length * 130;
    cols.style.width = `${width}px`;
    this.body.style.width = `${width}px`;
    this.body.style.height = `${this.table.size * ROW_HEIGHT}px`;
    let pending = false;
    this.grid.addEventListener('scroll', () => {
      if (pending) return;
      pending = true;
      requestAnimationFrame(() => { pending = false; this.subscribe(); });
    });
    panelsEl.append(el);
  }

  /** The rows in view, plus some either side: only these are on the wire. */
  window() {
    if (this.forced) return this.forced;
    const visible = Math.ceil(this.grid.clientHeight / ROW_HEIGHT);
    const first = Math.max(0, Math.floor(this.grid.scrollTop / ROW_HEIGHT) - OVERSCAN);
    const last = Math.max(first, Math.min(this.table.size - 1, first + visible + 2 * OVERSCAN));
    return [first, last];
  }

  subscribe() {
    const [first, last] = this.window();
    const interval = Number(intervalEl.value);
    const key = `${first}:${last}:${interval}`;
    if (key === this.subscribedKey) return;
    this.subscribedKey = key;
    this.m.viewport = [first, last];
    this.m.interval = interval;
    // A subscription's update interval is fixed when it is made (TableViewportSubscription.update
    // refuses a different one), so a new interval is a new subscription.
    if (this.subscription !== null && this.subscriptionInterval !== interval) {
      this.subscription.close();
      this.subscription = null;
    }
    if (this.subscription === null) {
      this.subscription = this.table.setViewport(first, last, null, interval);
      this.subscriptionInterval = interval;
    } else {
      this.subscription.setViewport(first, last, null, interval);
    }
  }

  onData(data) {
    const now = Date.now();
    if (this.m.firstDataMillis === null) {
      this.m.firstDataMillis = Math.round(performance.now() - this.times.clicked);
    }
    this.m.events += 1;
    this.m.lastEventAt = now;
    this.m.offset = data.offset;
    this.m.rowsInEvent = data.rows.length;
    let newest = -Infinity;
    const rows = [];
    data.rows.forEach((row, i) => {
      const values = this.columns.map((c) => plain(row.get(c)));
      const key = this.keyColumns.map((c) => values[this.columns.indexOf(c)]).join('\u0000');
      const before = this.previous.get(key);
      if (before) {
        values.forEach((v, j) => {
          if (v !== before[j]) {
            this.m.changedCells += 1;
            const dir = typeof v === 'number' && typeof before[j] === 'number' ? (v > before[j] ? 'up' : 'down') : 'changed';
            this.flashes.set(`${key}\u0000${j}`, { dir, until: now + FLASH_MS });
          }
        });
      }
      this.previous.set(key, values);
      if (this.tsColumn) newest = Math.max(newest, values[this.columns.indexOf(this.tsColumn)]);
      rows.push({ position: data.offset + i, key, values });
    });
    if (Number.isFinite(newest)) {
      this.m.freshness.push(now - newest);
      if (this.m.freshness.length > 2000) this.m.freshness.shift();
    }
    this.draw(rows, now);
  }

  draw(rows, now) {
    const html = rows.map(({ position, key, values }) => {
      const cells = values.map((v, j) => {
        const c = this.columns[j];
        const f = this.flashes.get(`${key}\u0000${j}`);
        const flash = f && f.until > now ? ` ${f.dir}` : '';
        return `<div class="cell${isNumeric(c) ? ' num' : ''}${flash}">${format(c, v)}</div>`;
      }).join('');
      return `<div class="row" style="top:${position * ROW_HEIGHT}px">${cells}</div>`;
    }).join('');
    this.body.innerHTML = html;
    const fr = this.m.freshness;
    const recent = fr.slice(-200).slice().sort((a, b) => a - b);
    const pct = (p) => (recent.length ? recent[Math.min(recent.length - 1, Math.floor(p * recent.length))] : null);
    this.statsEl.innerHTML = [
      `<span>rows <b>${this.m.size}</b></span>`,
      `<span>viewport <b>${this.m.viewport[0]}–${this.m.viewport[1]}</b></span>`,
      `<span>updates <b>${this.m.events}</b></span>`,
      `<span>cells changed <b>${this.m.changedCells}</b></span>`,
      this.tsColumn ? `<span>freshness p50 <b>${pct(0.5)} ms</b> p95 <b>${pct(0.95)} ms</b></span>` : '',
      `<span>published in <b>${this.m.publishMillis} ms</b>${this.m.reused ? ' (reused)' : ''}</span>`,
    ].join('');
  }
}

function isNumeric(c) {
  return ['int', 'long', 'double', 'float', 'short', 'java.math.BigDecimal'].includes(c.type);
}

/** A cell as a plain JS value: Deephaven hands longs and timestamps over as wrapper objects. */
function plain(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'object') {
    if (typeof v.asDate === 'function') return v.asDate().getTime();
    if (typeof v.asNumber === 'function') return v.asNumber();
    return String(v);
  }
  return v;
}

function format(c, v) {
  if (v === null) return '';
  if (c.type === 'java.time.Instant') return new Date(v).toISOString().slice(11, 23);
  if (c.type === 'double' || c.type === 'float') return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 });
  if (typeof v === 'number') return v.toLocaleString('en-US');
  return String(v).replace(/[&<>]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[ch]);
}

const params = new URLSearchParams(location.search);
for (const name of (params.get('open') || '').split(',').filter(Boolean)) {
  await openPanel(name);
}
