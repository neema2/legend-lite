# Deephaven spike: ticking DataCube levels, end to end (2026-10-21)

The question: can legend-lite use Deephaven as a streaming backend, with a browser grid (DataCube)
that really ticks, without Java sitting on the tick path? This spike builds the thinnest version of
every piece and measures it. Nothing here touches `core/`; it is evidence for a design, not the
design.

**Answer: yes.** A plan written as JSON (no Deephaven library) is run once by a small Java
connector; Deephaven keeps the result current; the browser fetches it by a shared ticket and
subscribes to the rows in view. A Python process can subscribe to the same live table. DataCube's
own pivot (conditional aggregates) works on Deephaven Community: Enterprise pivot tables are not
needed.

## The setup

```
 trades, quotes (ticking)            plans/*.json  (what legend-lite's planner would write)
 ┌──────────────────────────┐              │
 │ Deephaven 42.7 :10000    │◀── Batch ────┤  connector/ (Java: gRPC + Deephaven's protocol jar only)
 │  simulated feed          │◀── Publish ──┘   runs a plan once, publishes the result to a shared
 │  live derived tables     │                  ticket, answers the page {sharedTicket, names, ...}
 └──────────┬───────────────┘                         ▲
            │ Barrage viewport subscription           │ POST /api/live?plan=by_symbol
            │ over one WebSocket (JS API)             │
            ▼                                         │
 web/ (browser page)  ────────────────────────────────┘
 python_viewer.py (pydeephaven-ticking, whole-table subscription)
```

- **`server/start_server.py`**: Deephaven Community Core embedded in Python (`pip install
  deephaven-server==42.7`), PSK auth, update-graph cycle 100 ms. A simulated feed: `trades`
  (200 a second over 500 symbols, four venues, BUY/SELL) and `quotes` (1,000 a second).
- **`plans/*.json`**: four DataCube-shaped queries, each with the Pure it stands for and its
  Deephaven plan: a `BatchTableRequest` in protobuf's JSON form. `by_symbol` is DataCube's level 1
  (group by symbol, Qty pivoted on Side, sorted, `limit(1001)`); `grand_total` is level 0 (the
  `ROOT` groupBy `query.ts` writes); `sym_venue` is level 2 (two open groups' children);
  `slippage_aj` is an as-of join of trades to quotes, then a grouped measure.
- **`connector/`**: `DeephavenConnector` (the execution seam: authenticate, run a plan, publish,
  release, keep the session alive) and `SpikeServer` (stands in for legend-lite's server: serves
  the page, answers handles, caches one live table per plan text). A standalone Maven project,
  outside the Bazel build, as `experiments/backend-probes/harness` is.
- **`web/`**: the viewer. It logs into Deephaven with the JS API, fetches the live table by its
  shared ticket, subscribes to the rows in view (`setViewport` with an update interval), keys rows
  by their group columns, flashes changed cells.
- **`measure.mjs`**: the measurements below, in headless Chromium. **`python_viewer.py`**: the
  same handoff from Python.

## Running it

```sh
export SPIKE_STATE=/some/dir/outside/the/tree
./run.sh setup        # venv (deephaven-server, pydeephaven) and Maven 3.9.11, about 750 MB
./run.sh build        # the connector
./run.sh server       # Deephaven on :10000; needs a JDK 17+ on PATH
./run.sh connector    # the page on http://localhost:8090/?open=by_symbol,grand_total
node measure.mjs results/<date> --server-pid "$(cat "$SPIKE_STATE/server.pid")" \
  --playwright /path/to/playwright/index.mjs
"$SPIKE_STATE/venv/bin/pip" install pydeephaven-ticking==42.7
"$SPIKE_STATE/venv/bin/python" python_viewer.py --plan by_symbol
./run.sh stop
```

## What was measured (`results/2026-10-21/`)

One 4-core cloud container, server, connector and browser on the same host. Four panels open,
every one a live table subscribed by viewport.

**Opening** (click to first rows on screen; "publish" is the connector's Batch + Publish):

| plan | rows | publish | click → first data |
|---|---|---|---|
| `by_symbol` | 500 | 164 ms | 433 ms |
| `grand_total` | 1 | 116 ms | 168 ms |
| `sym_venue` | 8 | 40 ms | 130 ms |
| `slippage_aj` | 500 | 457 ms | 535 ms |

Logging in took 99 ms. A plan's first publish includes Deephaven compiling its formulas; a second
viewer of the same plan got the already-built live table (`reused: true` for all four).

**Steady state.** Freshness is the age of the newest trade in an update when it reaches the page
(browser clock minus the `LastTs` cell). `grand_total` is the cleanest latency measure, since every
trade changes it. A change can wait up to one interval before it is sent, on top of this.

| interval | updates/s per panel | `grand_total` freshness p50 / p95 | `by_symbol` p50 / p95 | `slippage_aj` p50 / p95 |
|---|---|---|---|---|
| 1000 ms | 1 | 80 / 102 ms | 94 / 410 ms | 126 / 347 ms |
| 250 ms | 4 | 57 / 103 ms | 107 / 277 ms | 98 / 190 ms |
| 100 ms | 9.9 | 47 / 63 ms | 82 / 230 ms | 107 / 256 ms |

The whole Deephaven process (the feed, four live queries, two browser viewers) used 0.10 to 0.13
of a core.

**Only the rows in view are sent.** A page holding `by_symbol` alone at 250 ms received 5.0 KB/s
subscribed to the 38 rows in view, and 18.1 KB/s subscribed to all 500. Scrolling to row 300
moved the subscription and painted the new rows 45 ms after the scroll.

**Python** (`python_viewer.py`, the Python client's default 1 s interval, whole table): 10
updates in 10 s; freshness p50 82 to 86 ms, p95 105 to 111 ms.

## Findings

1. **The handoff works, and Java is off the tick path.** The connector runs a plan once and
   returns `{sharedTicket, names, ...}`; the page talks to Deephaven directly from then on. Every
   value on every tick is computed by Deephaven, as "Java orchestrates, the database executes"
   asks.
2. **A plan can be plain JSON.** `BatchTableRequest`'s protobuf JSON form is a complete,
   session-free plan: the planner can write it with no Deephaven library (including the
   WebAssembly planner DataCube runs in the tab). The connector parses it with protobuf's
   `JsonFormat` and only fills in the last operation's `result_id`, since tickets belong to a
   session. It needs gRPC, protobuf and one Deephaven jar (the generated protocol classes): no
   Deephaven Java client, no QST, no engine.
3. **Shared tickets are spelled two ways.** The JS API's `getSharedObject("x")` asks for ticket
   bytes `h/x` (web client-api `Tickets.sharedTicket`), while the server takes every byte after `h`
   as the id and the Java and Python helpers write `hx`. The connector publishes `h/` + id, which
   all viewers can reach; Python asks for `b"h/" + id`.
4. **A shared ticket lives as long as the publisher's export.** After the connector released its
   export, viewers already subscribed kept ticking (20 updates in the next 5 s), but a new viewer's
   fetch failed (`Export in state DEPENDENCY_FAILED`). A connector that dies leaves its session,
   and its live tables, until the server expires the session (`http.session.durationMs`); a
   restarted connector reusing the same ids was refused (`ALREADY_EXISTS`). So ids carry a
   per-process nonce, and legend-lite needs a lease manager: count viewers per live table, release
   after the last one leaves.
5. **A subscription's update interval is fixed when it is made.** The JS API refuses a different
   interval on a later `setViewport` (`TableViewportSubscription.update`); changing it is a new
   subscription.
6. **On a sorted level, a changed row arrives as remove + add.** For `by_symbol` (sorted by
   Notional), Python saw 1,976 adds, 1,476 removes and no modifies in 10 s; for `grand_total`
   (unsorted), 9 modifies. A delta-applying grid must match rows by their group key, not by row key
   or position (the page does).
7. **Column names must be Java identifiers.** DataCube's pivot columns are named `BUY__|__Qty`; the
   plan names them `BUY__Qty` and carries `names` to map back. The target owns that legalization.
8. **DataCube's pivot runs on Community.** Each pivot cell is a conditional aggregate:
   ``BUY__Qty = Side == `BUY` ? Qty : NULL_LONG``, then `sum`. Sums skip `NULL_LONG`, so an empty
   cell is null, as SQL's `SUM(CASE ...)` is. What Enterprise pivot tables add is a column set that
   grows by itself when a new pivot value appears; with our pivot a new value means a re-plan,
   which a live `selectDistinct` of the pivot columns could trigger (not built here).
9. **The browser transport.** The JS API is one ES module (`/jsapi/dh-core.js`, 8 MB uncompressed),
   multiplexing every call over one WebSocket. Deephaven answers cross-origin gRPC-web preflights
   by default; the page loads the module through the connector's proxy (loading it cross-origin
   was not tried).

## Not tested, and what is open

- **Auth.** The page logs in with the server's PSK, which the connector hands it: a spike-only
  shortcut. A real deployment logs the viewer in as themselves: Deephaven's OIDC/Keycloak handler
  on the same identity provider as legend-lite, or a custom Deephaven auth handler that checks a
  token legend-lite issues. Not tried.
- **Scale.** One machine, 500 symbols, 1,200 rows a second, two viewers. Not tried: cross-machine
  latency (and the clock offset freshness then needs), high rates, large detail tables, many
  viewers, a Deephaven restart.
- **Rollups and trees** (`rollup()`, `tree()`), a Deephaven-native alternative to DataCube's
  query-per-level tree. Not tried.
- **Licensing.** `deephaven-proto-backplane-grpc` (the one Deephaven jar the connector uses) is
  under the Deephaven Community License 1.0, as is the server; the JS API types, Barrage format and
  web UI are Apache-2.0. Internal use is permitted; shipping a connector in an Apache-2.0 project
  needs a legal read.

## What it means for legend-lite

- **The target** writes `BatchTableRequest` JSON from the SQL IR, as Mongo's target writes a
  pipeline: operations for the relation shapes, Deephaven's formula language for scalars, legal
  column names with a map back, `NULL_*` sentinels for nulls.
- **The plan/execution boundary** gains a live result: executing it answers a handle (server,
  shared ticket, column map), not rows; a snapshot of it answers rows as today.
- **The connector** is small: about 200 lines here. What it lacks is the lease manager (finding
  4) and real auth.
- **DataCube** gains a subscribe path beside `execute`: one live table per open level, a viewport
  per level driven by scrolling, deltas applied by group key, and ticks that bypass the
  latest-wins guard (they are not state changes).
- **Python** can follow the same handle (`python_viewer.py`); a notebook DataCube would use the
  browser path.
