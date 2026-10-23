// THE TREE BENCHMARK'S PAGE (README.md; tree-bench.mjs drives it, docs/DEEPHAVEN_PLAN_2026_10_22.md R1): a tree by
// region, desk, sector and currency over the tick world's positions (tick.groovy `flat`, the stored base), its path
// EMEA > Rates > Tech open, read two ways -- `levels`, one aggregation a level as DataCube's level queries make them
// (each followed whole), and `rollup`, Deephaven's own hierarchical table made from this page with its JS library, the
// same path opened in its viewport. For each: a tick's latency (the newest quote's stamp in an update, from the cycle
// that made it), how many rows an update carried and the page's time to read them, and the server's update graph's busy
// share of the window (its own state log). For the rollup also: its making, its first rows, and each group's opening;
// and a probe of which aggregates a rollup takes, and whether a column computed from each level's aggregates applies.

const PSK = 'io.deephaven.authentication.psk.PskAuthenticationHandler';
const config = await (await fetch('config.json')).json();
const dh = (await import(`${config.url}/jsapi/dh-core.js`)).default;
const client = new dh.CoreClient(config.url);
await client.login({ type: PSK, token: config.psk });
const ide = await client.getAsIdeConnection();

const now = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const plain = (v) => (v !== null && typeof v === 'object' && 'asNumber' in v ? v.asNumber() : v);
const table = (name) => ide.getObject({ type: 'Table', name });
const message = (e) => String(e?.message ?? e).slice(0, 300);

/** A table's rows as they are now: its first update of a subscription, every column by name. */
async function rowsOf(t) {
  const sub = t.createSubscription({ columns: t.columns, updateIntervalMs: 10 });
  const rows = await new Promise((resolve) => {
    sub.addEventListener(dh.Table.EVENT_UPDATED, (e) => resolve(e.detail.rows.map((r) => Object.fromEntries(
      t.columns.map((c) => [c.name, plain(r.get(c))])))));
  });
  sub.close();
  return rows;
}

/** The server's update graph over a window: its state log (one row an interval, kept briefly) followed from now, every
 *  interval's row kept as it lands, then read -- every cycle's time in the intervals that started in the window, and
 *  their busy share (the cycles' time over the intervals' length). */
async function busyWatch() {
  const console_ = await ide.startSession('groovy');
  const ran = await console_.runCode('__ssl = io.deephaven.engine.table.impl.util.TableLoggers.serverStateLog()');
  if (ran.error) throw new Error(`the server state log: ${ran.error}`);
  const log = await table('__ssl');
  const seen = new Map();
  const sub = log.createSubscription({ columns: log.columns, updateIntervalMs: 100 });
  sub.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
    for (const r of e.detail.rows) {
      const row = Object.fromEntries(log.columns.map((c) => [c.name, plain(r.get(c))]));
      seen.set(row.IntervalStartTime, row);
    }
  });
  const since = Date.now();
  return {
    async stop() {
      await sleep(1500);
      sub.close();
      const rows = [...seen.values()].filter((r) => r.IntervalStartTime / 1e6 >= since);
      const cycles = rows.flatMap((r) => Array.from(r.IntervalUGPCyclesTimeMicros ?? [], Number));
      const length = rows.reduce((t, r) => t + r.IntervalDurationMicros, 0);
      const sorted = [...cycles].sort((a, b) => a - b);
      const at = (q) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] / 1000 : NaN);
      return { intervals: rows.length, cycles: cycles.length, share: length > 0 ? cycles.reduce((t, c) => t + c, 0) / length : NaN,
        cycleP50: at(0.5), cycleP95: at(0.95) };
    },
  };
}

const modes = {
  /** Nothing read: the feed and the base alone, the server's floor. */
  async none(seconds) {
    const watch = await busyWatch();
    await sleep(seconds * 1000);
    return { events: [], busy: await watch.stop() };
  },

  /** One aggregation a level (tick.groovy's level0..level4, made when the server started), each followed whole. */
  async levels(seconds) {
    const tables = await Promise.all([0, 1, 2, 3, 4].map((i) => table(`level${i}`)));
    const events = [];
    const subs = tables.map((t, i) => {
      const stamp = t.findColumn('pxMs');
      const sub = t.createSubscription({ columns: t.columns, updateIntervalMs: 10 });
      sub.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
        const t0 = performance.now();
        let s = 0;
        for (const row of e.detail.rows) s = Math.max(s, plain(row.get(stamp)));
        events.push([now(), s, e.detail.rows.length, performance.now() - t0, i]);
      });
      return sub;
    });
    const watch = await busyWatch();
    await sleep(seconds * 1000);
    const busy = await watch.stop();
    for (const s of subs) s.close();
    return { events, rows: tables.map((t) => t.size), busy };
  },

  /** Deephaven's rollup of the same tree, made from this page, the same path opened in its viewport. */
  async rollup(seconds) {
    const flat = await table('flat');
    let t0 = performance.now();
    const tree = await flat.rollup({
      groupingColumns: ['region', 'desk', 'sector', 'ccy'],
      aggregations: { Sum: ['mv', 'qty'], Max: ['pxMs'] },
    });
    const makeMs = performance.now() - t0;
    const stamp = tree.findColumn('pxMs');
    const events = [];
    let last = null;
    let waiting = [];
    tree.addEventListener(dh.TreeTable.EVENT_UPDATED, (e) => {
      const t1 = performance.now();
      const data = e.detail;
      let s = 0;
      for (const row of data.rows) s = Math.max(s, plain(row.get(stamp)));
      events.push([now(), s, data.rows.length, performance.now() - t1, 0]);
      last = data;
      const still = [];
      for (const w of waiting) if (!w(data)) still.push(w);
      waiting = still;
    });
    const until = (test) => new Promise((resolve) => {
      const w = (d) => {
        if (!test(d)) return false;
        resolve(d);
        return true;
      };
      if (last === null || !w(last)) waiting.push(w);
    });
    t0 = performance.now();
    tree.setViewport(0, 199, tree.columns, 10);
    await until(() => true);
    const firstMs = performance.now() - t0;
    const keys = ['region', 'desk', 'sector', 'ccy'].map((n) => tree.findColumn(n));
    const describe = (r) => `${r.depth}:${keys.map((k) => plain(r.get(k)) ?? '.').join('/')}${r.hasChildren ? (r.isExpanded ? '-' : '+') : ''}`;
    const shape = last.rows.slice(0, 6).map(describe);
    // open EMEA, then its Rates, then their Tech: the row whose deepest key is the one asked
    const openMs = [];
    for (const [k, value] of [[0, 'EMEA'], [1, 'Rates'], [2, 'Tech']]) {
      const at = last.rows.findIndex((r) => plain(r.get(keys[k])) === value
        && (k === 3 || plain(r.get(keys[k + 1])) === null) && r.hasChildren && !r.isExpanded);
      if (at < 0) throw new Error(`no closed row ${value} at level ${k}: ${last.rows.map(describe).join(' ')}`);
      const before = last.rows.length;
      t0 = performance.now();
      tree.setExpanded(last.offset + at, true);
      await until((d) => d.rows.length > before);
      openMs.push(performance.now() - t0);
    }
    const opened = last.rows.length;
    events.length = 0;
    const watch = await busyWatch();
    await sleep(seconds * 1000);
    const busy = await watch.stop();
    const measured = [...events];
    // a column from each level's aggregates (a group-level calculated column), and a sort, applied to the tree
    const probes = {};
    try {
      t0 = performance.now();
      tree.applyCustomColumns(['mvk = mv / 1000']);
      // the viewport names its columns: asked again with the tree's columns once mvk is one of them
      for (let i = 0; i < 50 && !tree.columns.some((c) => c.name === 'mvk'); i++) await sleep(100);
      probes.treeColumns = tree.columns.map((c) => c.name).join(', ');
      tree.setViewport(0, 199, tree.columns, 10);
      const d = await Promise.race([until((x) => x.columns.some((c) => c.name === 'mvk')),
        sleep(5000).then(() => null)]);
      if (d === null) probes.customColumns = 'no column mvk in 5 s';
      else {
        const mvk = d.columns.find((c) => c.name === 'mvk');
        const mv = tree.findColumn('mv');
        const wrong = d.rows.filter((r) => Math.abs(plain(r.get(mvk)) - plain(r.get(mv)) / 1000) > 1e-6 * Math.abs(plain(r.get(mv))));
        probes.customColumns = `${(performance.now() - t0).toFixed(0)} ms; ${d.rows.length - wrong.length} of ${d.rows.length} rows (depths ${[...new Set(d.rows.map((r) => r.depth))].join(',')}) mv / 1000`;
      }
    } catch (e) {
      probes.customColumns = `refused: ${message(e)}`;
    }
    tree.close();
    // a rollup made again, warm: a regroup (other keys), and the same keys over a filtered base (a filter on a column
    // the rollup does not group by), each to its first rows
    const firstRows = async (made) => {
      const t = await made;
      const shown = new Promise((resolve) => t.addEventListener(dh.TreeTable.EVENT_UPDATED, resolve));
      t.setViewport(0, 49, t.columns, 10);
      await shown;
      t.close();
    };
    try {
      t0 = performance.now();
      await firstRows(flat.rollup({ groupingColumns: ['desk', 'region'], aggregations: { Sum: ['mv', 'qty'], Max: ['pxMs'] } }));
      probes.regroupMs = (performance.now() - t0).toFixed(0);
      const fx = await flat.copy();
      t0 = performance.now();
      fx.applyFilter([fx.findColumn('desk').filter().eq(dh.FilterValue.ofString('FX'))]);
      await firstRows(fx.rollup({ groupingColumns: ['region', 'sector'], aggregations: { Sum: ['mv', 'qty'], Max: ['pxMs'] } }));
      probes.filteredMs = (performance.now() - t0).toFixed(0);
      fx.close();
    } catch (e) {
      probes.remade = `refused: ${message(e)}`;
    }
    return { events: measured, makeMs, firstMs, openMs, opened, shape, probes, busy };
  },

  /** Which aggregates a rollup takes: each asked of a one-level rollup of the base, through the JS library and, for
   *  those it does not carry, of the server itself (its console). */
  async aggregates() {
    const flat = await table('flat');
    const out = {};
    const console_ = await ide.startSession('groovy');
    for (const [name, agg] of [['server Median', 'AggMed("mv")'], ['server Percentile', 'AggPct(0.9, "mv")'],
      ['server WeightedAvg', 'AggWAvg("qty", "mv")'], ['server WeightedSum', 'AggWSum("qty", "mv")'],
      ['server CountDistinct', 'AggCountDistinct("mv")'], ['server Formula', 'AggFormula("each / 2").as("mv")']]) {
      const ran = await console_.runCode(`__probe = flat.rollup([${agg}], "region")`);
      out[name] = ran.error ? `refused: ${String(ran.error).split('\n')[0].slice(0, 200)}` : 'ok';
    }
    for (const op of ['Count', 'CountDistinct', 'Distinct', 'Min', 'Max', 'Sum', 'AbsSum', 'Var', 'Avg', 'Median',
      'Std', 'First', 'Last', 'Unique', 'WeightedAvg', 'WeightedSum']) {
      try {
        const t = await flat.rollup({ groupingColumns: ['region'], aggregations: { [op]: ['mv'] } });
        t.setViewport(0, 9, t.columns, 10);
        await new Promise((resolve, reject) => {
          t.addEventListener(dh.TreeTable.EVENT_UPDATED, resolve);
          setTimeout(() => reject(new Error('no rows in 5 s')), 5000);
        });
        out[op] = `ok (${t.columns.map((c) => c.name).join(', ')})`;
        t.close();
      } catch (e) {
        out[op] = `refused: ${message(e)}`;
      }
    }
    return out;
  },
};

window.bench = {
  ready: true,
  async run(mode, seconds) {
    try {
      return await modes[mode](seconds);
    } catch (e) {
      return { error: message(e) };
    }
  },
};
