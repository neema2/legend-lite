// THE TICK BENCHMARK'S PAGE (README.md): one transport at a time reads a ticking result of the benchmark's server
// (tick.groovy) for a few seconds, and for every update it records when it arrived, the newest quote's stamp in it
// (the server's time the quote was made: `pxMs`), how many rows it carried, and how long the page took to read them.
// The transports: Deephaven's own JS library, loaded from the server -- a viewport subscription, a full subscription,
// the older setViewport. A tick's latency is when the page saw a newer stamp less the stamp.

const PSK = 'io.deephaven.authentication.psk.PskAuthenticationHandler';
const config = await (await fetch('config.json')).json();
const dh = (await import(`${config.url}/jsapi/dh-core.js`)).default;
const client = new dh.CoreClient(config.url);
await client.login({ type: PSK, token: config.psk });
const ide = await client.getAsIdeConnection();

const now = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const plain = (v) => (v !== null && typeof v === 'object' && 'asNumber' in v ? v.asNumber() : v);

/** One transport's events: [arrival ms, newest stamp, rows read, page ms to read them]. */
async function record(start, seconds) {
  const events = [];
  const stop = await start((stamp, rows, readMs) => events.push([now(), stamp, rows, readMs]));
  await sleep(seconds * 1000);
  await stop();
  return events;
}

const transports = {
  /** A viewport subscription (the grid's: the visible rows only). */
  async viewport(table, { rows, intervalMs }) {
    const t = await ide.getObject({ type: 'Table', name: table });
    const stampCol = t.findColumn('pxMs');
    const sub = t.createViewportSubscription({
      columns: t.columns, rows: dh.RangeSet.ofRange(0, rows - 1), updateIntervalMs: intervalMs,
    });
    return (on) => {
      sub.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
        const t0 = performance.now();
        let stamp = 0;
        const data = e.detail;
        for (const row of data.rows) stamp = Math.max(stamp, plain(row.get(stampCol)));
        on(stamp, data.rows.length, performance.now() - t0);
      });
      return async () => { sub.close(); t.close(); };
    };
  },

  /** A full subscription: every row kept in the page, each update read whole (its rows, as the library keeps them). */
  async full(table, { intervalMs }) {
    const t = await ide.getObject({ type: 'Table', name: table });
    const stampCol = t.findColumn('pxMs');
    const sub = t.createSubscription({ columns: t.columns, updateIntervalMs: intervalMs });
    return (on) => {
      sub.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
        const t0 = performance.now();
        let stamp = 0;
        const rows = e.detail.rows;
        for (const row of rows) stamp = Math.max(stamp, plain(row.get(stampCol)));
        on(stamp, rows.length, performance.now() - t0);
      });
      return async () => { sub.close(); t.close(); };
    };
  },

  /** The table's own setViewport (what the plain demo page uses). */
  async setViewport(table, { rows, intervalMs }) {
    const t = await ide.getObject({ type: 'Table', name: table });
    const stampCol = t.findColumn('pxMs');
    return (on) => {
      const off = t.addEventListener(dh.Table.EVENT_UPDATED, (e) => {
        const t0 = performance.now();
        let stamp = 0;
        for (const row of e.detail.rows) stamp = Math.max(stamp, plain(row.get(stampCol)));
        on(stamp, e.detail.rows.length, performance.now() - t0);
      });
      t.setViewport(0, rows - 1, t.columns, intervalMs);
      return async () => { off(); t.close(); };
    };
  },
};

window.bench = {
  ready: true,
  /** One run: `transport` on `table` for `seconds`; the events after the first `warmup` seconds. */
  async run({ transport, table, rows = 50, intervalMs = 100, seconds = 6, warmup = 2 }) {
    const start = await transports[transport](table, { rows, intervalMs });
    const events = await record(start, seconds + warmup);
    const from = events.length > 0 ? events[0][0] + warmup * 1000 : 0;
    return events.filter((e) => e[0] >= from);
  },
};
