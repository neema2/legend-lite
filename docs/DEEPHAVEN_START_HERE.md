# Deephaven: start here (handoff, 2026-10-24)

For a session picking up the Deephaven line with no context. Read this, then the parts of the plan it points to. The
plan (`docs/DEEPHAVEN_PLAN_2026_10_22.md`) is the record: its §5 the decisions (DH1-DH27), its §9 every landing with
what it measured. This page is where things stand and what is open.

## What the line is

A Deephaven server as just another legend-lite backend: its tables an ordinary `###Relational` store's,
`RelationalDatabaseConnection { type: Deephaven; ... }`, a target that writes Deephaven's table operations, and
**DataCube on Deephaven, straight from the browser** -- the tab plans (the WebAssembly planner), sends the plan to the
user's own Deephaven server, reads the rows, and follows the results live with Deephaven's own JS library loaded from
that server. No server of legend-lite's in between. The user's tenet (DH27): **a change may take time; a tick may
not.**

## Branches (stacked, no PR until the user asks)

| Branch | Holds |
|---|---|
| `claude/deephaven-1-model` | L1: the database type, connection and pre-shared key auth |
| `claude/deephaven-2-target` | L2: the dialect (`Deephaven.java`, `DeephavenBatch.java`) |
| **`claude/deephaven-3-browser`** | **everything since, on top of both: work here.** `main` merged in 2026-10-24, pushed |

None is on `main`, and **none goes there before the licence answer** (open decision 1; the rule below). `main` is merged
INTO the browser branch, never the reverse, and never rebased (the branch is pushed): on 2026-10-24 at `18db1e9a` (PRs
#65-#71), merge commit `5125f1d8`. Its one textual conflict was the bundle budgets (the grid-only page re-measured,
334,805: 333,500 -> 335,000; the other two fit main's); one conflict the text did not show: D19 removed
`PageApp.follow`, which `board.ts` used to add its charts (`PageApp.reapply` now).

**PR #72** (DataCube BI and Excel parity) merged in 2026-10-24, merge commit `7a604f2e`. Its six conflicts were each kept
both ways; the one that mattered was `grid.ts`: #72's conditional formats (data bars, icon sets, rules), a format
code's colour and pinned columns' offsets moved into `DataGrid.#cell`, the one painter a tick redraws a cell with. A
tick redraws every cell when a conditional reads them all, as a heatmap's scale does (both measured again at a tick),
and the in-place text update stands aside when a format code's colour changes. The menu's copies are #72's clipboard
text, made from the whole view where a window reads it (V1). The bundle budgets are main's with the line's growth on
top (grid 358,500, engine page 347,000, notebook module 366,000; each measured, dated). #72's correlation, covariance
and dates relative to today are still refused on Deephaven (below); its max-by and min-by run there (K3a), and its Top/
Bottom N's rank window (K6).

## What is built (the plan's §9 table; each row's paragraph says what it measured)

- **L1, L2:** the model and the target. **L3b:** the browser route -- an encoder of our own (`core/.../deephaven/`)
  in the WebAssembly planner, the tab's sender (`engine-client/src/deephaven.ts`), the pinned test server.
- **D0 -> J1 -> T1:** the demo world; DataCube's own app on Deephaven; real ticking (`engine-client/src/deephaven-engine.ts`,
  `deephaven-live.ts`).
- **F1, B1, G1, R1, P1, O1:** compile-free filters; one base per grid stored on the server; gesture budgets; the rollup
  spike (measured, not adopted); pivots that compile nothing; a gesture's time on the browser's timeline.
- **V1-V3:** a flat grid of any size (a viewport), a page of several grids, charts that tick.
- **N1:** over a network -- a warm gesture waits on one round trip.
- **B2, C2:** the page saved, opened and restarted; a chart's scale held while it ticks.
- **A1:** the single-user audit (`docs/DEEPHAVEN_SINGLE_USER_AUDIT_2026_10_24.md`): 15 fixes, 12 recorded items (R1-R12).

## Run it

```
bazel run //datacube:deephaven_desk -- --port=8080        # prints the Deephaven URL and the desk's
MARKET_NAMES=2000 MARKET_TRADES=4000 bazel run //datacube:deephaven_desk -- --port=8080   # a larger world (market.groovy's knobs)
```

**The one demo is the trading desk** (2026-10-26, the user: "why we need 3 different demos instead of single one in
real datacube"): DataCube's own page, the desk one saved page of ten tabs (Open… ▸ Examples lists it first), each tab
read when first shown (its charts and its saved queries' typing too). The risk demo's and the trader screens' hand
tools are deleted; the risk pages (`cube.html`, `board.html`) are the engine's test benches, and the trader screens'
own page (`traders.ts`) is deleted, its checks the desk test's (`//datacube:deephaven_desk_test`, manual). The
server's certificate is mkcert's where mkcert is installed:
run `mkcert -install` once and the browser asks nothing (`datacube/demo/deephaven/mkcert.mjs`); without mkcert it is
the test server's own, made by `keytool` at each start, and the printed Deephaven `https://` address must be opened
and accepted first (HTTPS because Deephaven's browser protocol needs HTTP/2). `restart` + Enter restarts Deephaven.

**Checks** (the browser ones start the pinned server): `//datacube:deephaven_test` (answers worked out by hand, and
K1's and K6's DuckDB's), `deephaven_market_test` (the trader screens' market world, K2, and windows over its tape),
`deephaven_cube_test`, `deephaven_flat_test`, `deephaven_board_test`; `//core:core_tests_deephaven`,
`//core:core_tests_sql` (`DeephavenDialectTest`), `//core:deephaven_messages_test` (the message table against
Deephaven's own definitions); then `//datacube:all //engine-client:all`. **Manual tools:**
`deephaven_walkthrough` (both pages as a person drives them), `deephaven_session_bench` (a long session's cost),
`deephaven_network_bench` (a link with a round trip), `deephaven_record` (frames of the pages ticking),
`deephaven_tick_bench`, `deephaven_tree_bench`, `deephaven_first_bench`.

**In a cloud session as root** (`docs/CLOUD_SESSIONS.md`): `verify_app_test` and the Postgres tests refuse root -- run
them with `chmod o+x /root` and `--strategy=TestRunner=standalone --run_under="<a script that runs the test as an
unprivileged user> ubuntu"`; `upload_test` and `model_data_test` then fail (they write into their directory: run them
normally); `browser_guard_test` fails behind the session's proxy certificate.

## Rules this line keeps (beside AGENTS.md)

- **Nothing reaches `main` before the licence answer** (the user, 2026-10-24: "worried about merging to main without
  licensing discussion first"): no PR, no push to `main`, no release from the three Deephaven branches until open
  decision 1 is answered, however green the gates. The demo's server-side scripts (Groovy against Deephaven's own API:
  `risk.groovy`, and any simulator after it) are under the same question: listed for counsel, kept on the branch.
- **No Deephaven code in the build (DH11):** every Deephaven jar is under the Deephaven Community License. Its JS library
  is loaded at run time from the user's own server, never bundled; the server and jars are fetched for tests only
  (testonly, never shipped).
- The pre-shared key is a test server's: never sent to a browser in production.
- Guard pins, ratchets, budgets and allowlists move only with a dated justification naming the task.
- Commits end with the session's attribution lines; no model names in anything pushed; no PR unless asked.
- The user prefers the work done directly over sub-agents.

## Open: decisions waiting on the user or counsel

1. **The licence question** (raised 2026-10-24). `core/.../deephaven/DeephavenMessages.java` (about 105 fields: Deephaven's
   message and field names, numbers, types) and the target's JSON vocabulary were written from Deephaven's `.proto`
   definitions, which are under the Community License, and are published under Apache-2.0 in this public repository.
   The licence's own definition of a Derivative Work ("based on or derived from the Software") is wider than copyright
   law's. Not a clear violation; counsel's call. Proposed: ask Deephaven (license@deephaven.io) to confirm an independent
   client is fine or to license the definitions under Apache; failing that, split the Deephaven backend into a separately
   licensed add-on. Meanwhile: no release with the Deephaven backend, and the table holds only what we read or write.
2. **Section 4 of the licence:** no third party may define or change the schema of input tables. Internal users are
   unlimited. The target can already write table creation (input tables, DH15); nothing enforces "internal only" yet. A
   deployment setting that refuses it must land before writes or uploads reach Deephaven.
3. **Sign-in and authorization for the whole system** (the user: "needs a bigger auth design"). The fact it must start
   from: Deephaven Community has no per-table permissions -- whoever signs in reads every table, and runs code if the
   console is on (the demo's key can) -- so restricting it means an authorization plugin, which is Deephaven-licensed code.
4. ~~Merge `main`~~: done 2026-10-24 (above), PR #72 with it.
5. **DH16, a published table's types checked against the store's declaration** (found 2026-10-27). **Decided (the
   user, 2026-10-29): strict, always** ("i think always strict"). **Built 2026-10-29:** the target writes beside each
   batch, after its operations, every store table the batch reads as its store declares it (`"declared"`: the
   operation that reads it, and each column's Java type, `DeephavenBatch.javaTypeOf` -- one table with the DDL's typed
   nulls); a send hands each export that reads one its declaration; and the planner reads the server's whole answer at
   once (`DeephavenWire.made`, the module's `made`): each answer's export by its ticket, its Arrow schema
   (`schema_header`, field 4, read by `ArrowSchema` -- a flatbuffer, no Arrow library in the planner) and each column's
   `deephaven:type` compared with the declaration. A column held otherwise is refused by name, both types said: "the
   server's table kinds is not what its store declares: I is held as int; its store declares BIGINT (long on
   Deephaven)" (`DeephavenMismatch` in the tab). The publishers conform: the corpus fixture now publishes a SMALLINT as
   a `short`, a TINYINT a `byte`, a REAL a `float` (`StressOnDeephaven.kind`); the market world's first run under the
   check refused EXCHANGE (`IS_ELECTRONIC` held as `java.lang.Object`: Groovy hands `TableTools.col`'s varargs an
   `Object[]`), and a probe of all 20 tables it publishes against the store found two more, BUSINESS_UNIT's `IS_ACTIVE`
   and `ESTABLISHED_DATE` -- each now a typed array. A grid's base is, for now, declared as the server holds it
   (`server-base.ts`: an `int` an INTEGER, a `long` a BIGINT -- it was BIGINT for every integer), read off its own
   answer, so its check passes by construction; D6 (docs/ONE_CONNECTION_DESK_2026_10_28.md, the user, 2026-10-29)
   replaces that: the platform declares a copy's columns and creates it to match, as it creates every table on
   Deephaven. The declarations are not part of the operations' keys: a table kept alive is checked once, when made.
   Not checked: NOT NULL (Deephaven's schema says nothing of it, and a live table may gain a null later; only reading
   rows could).

## The desk under load (measured on the user's Mac, 2026-10-27/28)

What the roadmap's last item works from (docs/ONE_CONNECTION_DESK_2026_10_28.md: the demo runs without leaks, its bugs
fixed) -- found in a debugging and stress-testing session: the desk held open in headless pinned Chromium, `jcmd GC.class_histogram`
live sets, GC logs, JFR and Deephaven's UpdatePerformanceLog. Measured on `6adeaf49` unless noted; `589dcd96` checked
for where the code moved. On `589dcd96` the desk opens in 3.1 s, its tabs fill in 0.3-1.5 s, ticks reach the screen in
about 31-36 ms, and `//datacube:deephaven_desk_test` passes on macOS. The pointers below were checked against
`589dcd96` (2026-10-28).

1. **Memory grows with every trade: the desk's main limit.** The tape (`MARKET_TRADE`) is unbounded by default
   (`market.groovy`: `MARKET_HISTORY` 0, every trade kept), and 108 server operations behind the desk keep a row for
   each trade: 26 where, 20 View, 15 aj plus 15 aj right-side states, 9 by, 8 naturalJoin, 5 UpdateView, 2 sorts,
   2 Select. That is about 860-960 bytes per trade, about 39 MB/min at 800 trades/s, ~64% of it `long[]`. At
   `DESK_HEAP` 4g, GC degrades from ~40 min (live heap ~2.6 GB): pauses go from 2-5 s to 18-37 s per 5 min, the update
   cycle's p50 from 30 to 170 ms. The server then limps on rather than crashing, and the page shows no warning.
   `MARKET_NAMES=2000 MARKET_TRADES=4000` hits the same wall in ~5 min (61% of the time in GC by minute 16, cycles
   1.2-1.5 s). A 12g heap runs for a few hours.
2. **A history cap is expensive because of the Stock tab's EMA.** `market.pure:140`,
   `extend(over(~ticker, ~seq->ascending(), rows(unbounded(), 0)), ~[ema: ...->ema($alpha)])`, is computed for every
   ticker over the whole tape, though the Stock tab shows one name. On a ring (`MARKET_HISTORY`), each removal
   recomputes that ticker's whole EMA (`BucketedPartitionedUpdateByManager` at 210% of a thread, feeding a Select with
   ~2.03 M rows modified/s). `MARKET_HISTORY=1000000`: CPU 52% -> 235%, cycle p50 29 -> 220-280 ms (over the 250 ms
   budget). `MARKET_HISTORY=200000`: CPU 80-90%, cycle p50 ~41 ms, live heap ~1.1-1.3 GB, but only ~4 min of tape.
   Suggested: compute it for the picked name only, or an operator that removals do not recompute. **No cap becomes the
   default until this is fixed** (the user, 2026-10-28) -- see 5 for a commit that would make one.
   *Measured 2026-10-29 on the pinned server (Deephaven 42.7):* the stream pattern -- the EMA computed on the blink
   table, its state carried per symbol, the history capped after it -- is not available. A keyed `updateBy` on a blink
   table is refused ("Blink tables do not support partitionedAggBy; use BlinkTableTools.blinkToAppendOnly to accumulate
   full history"), and so is `partitionBy`; an unkeyed `updateBy` is accepted but restarts at every cycle (prices 10,
   20, 40, 0 in separate cycles gave 10, 20, 40, 0; the append-only reference 10, 15, 27.5, 13.75). Aggregations
   (`lastBy`) do keep their state across cycles. So a running per-symbol EMA needs the add-only history here.
   *Read 2026-10-28, not changed:* the Stock tab's grids read `stock(alpha)` through a server base
   (`src/server-base.ts`): the whole relation, every name's EMA, kept on the server, each pick a filter over it. The
   picked name's alone needs the pick's filter below the window (a filter on the window's partition commutes with it)
   and the base made per pick -- a DataCube design change, not a model edit.
3. **Residual growth even when capped:** ~9-12 MB/min with the browser parked on one tab, `long[]` plus a growing
   number of `LongSegmentedSortedArray`. Likely the as-of joins' right sides on the 1 s / 5 s / 30 s marks (`t7__pt1`,
   `t10__pt5`, `t13__pt30`); not confirmed.
4. **`restart` does not resume the page** (reproduced 2 of 2 on `6adeaf49`). The page signs in again
   (`counts().reconnects` 0 -> 4), then nothing ticks for minutes; it throws an uncaught "the window is no longer
   followed" (`deephaven-engine.ts` `stopFollowing`), and `#status` stays 'local', not bad. A reload works. The code is
   now `datacube/src/deephaven-grids.ts:163-237`, the same per-grid `reconnected().then(grid.refresh)` pattern; not
   retested on `589dcd96`. Suspect: one reconnect per ended grid, each resetting every reader and undoing the grids
   already refreshed. (Read on `589dcd96`, not run: the reconnect is shared by every grid of a server only while it
   runs -- `Server.#reconnecting` is cleared when it settles -- so a grid whose end arrives after that starts another,
   and `DeephavenEngine.reconnect` resets every reader and stops every shared following, `deephaven-engine.ts:351`.)
   *Fixed 2026-10-28* (`engine-client/src/deephaven-engine.ts`): a session's or live connection's end is the server gone
   only while it is the engine's own -- one a reconnect let go of ends as it closes, which had started another reconnect,
   each resetting every reader -- and a window's wait refused before it is awaited is handled. Checked by
   `//datacube:deephaven_desk_test`'s new restart: the page signs in again once, its first tab drawn and ticking 23 s
   after the restart, nothing thrown.
5. **The Coinbase feed splices messages after a sleep** (`market.groovy` `CoinbaseFeed`, line 838). `connect()` opens a
   new WebSocket every minute the feed looks quiet and never closes the old one; every socket shares one `partial`
   `StringBuilder` (line 842), so messages get spliced (timestamps like '2026-10-26T20:35:24.1est_4Z'). A splice that
   fails to parse sets the feed to 'refused', which opens yet another socket: 26 sockets on the Mac after real sleeps.
   Beat failures (NPE, NumberFormatException) drop the whole beat, equities included. Reproduced by SIGSTOP of the
   server JVM for 90 s (1 of 2 tries); `restart` clears it. **A fix exists, unpushed:** session
   `session_01UxtBX84d8QYAbp2ccEjL44` ("Fix desk demo: Coinbase reconnect splices messages, OOM", 2026-10-26) has two
   commits on this branch's then-head, waiting on the user's choice of where to push them (its own branch
   `claude/friendly-shannon-ecn7p8` holds only `main`'s head; nothing of it is on the remote, checked 2026-10-28):
   `d9ebe018` (one socket at a time, a buffer per socket, a bad message skipped and counted rather than failing the
   beat, the world row's crypto status saying why, and a new `//datacube:deephaven_feed_test` with a fake feed that
   splits, interleaves and corrupts messages: 4 of 6 checks fail on the old code, 6 of 6 pass) and `7f8b0ab0` (the
   hand tool keeps the latest 200,000 trades unless `MARKET_HISTORY` is set, its heap 1 GB). The second contradicts 2
   above: take `d9ebe018`, not `7f8b0ab0`, until the EMA is fixed.
   *Fixed 2026-10-28, re-implemented here* (`market.groovy` `CoinbaseFeed`, `World.drain`): one socket at a time, the
   one before let go first; each socket's message parts its own; a message unreadable or not placed skipped and counted
   (the world's crypto status: "Coinbase Exchange (2 messages skipped)"), never the feed refused nor the beat lost. Not
   taken: `7f8b0ab0`'s cap (2, above). Checked by a new `//datacube:deephaven_feed_test`: a hand-written fake feed
   (`test/deephaven/fake-feed.mjs`) sends each message in three parts, one not JSON and a price not a number, then goes
   quiet mid-message.
6. **Each open desk page holds ~0.5 GB of server state**, and keeps it ~5 min after the page closes (the session
   timeout), so a reload briefly doubles it.
7. **Minor:** `market.groovy`'s 148 `list as String[]` conversions each throw and catch a `GroovyRuntimeException`
   (~88/s; `toArray(new String[0])` avoids it). Allocation runs at ~540 MB/s, 60% of it `DoubleSparseArraySource`
   previous-value blocks under `SelectColumnLayer`: short pauses, but a tuning target.
   *Fixed 2026-10-28:* the per-beat tables' 76 columns (quotes, trades, news, orders, fills, routes, FX, RFQs) are
   `toArray(new String[0])`; the reference tables' conversions run once, at start.
8. **Ops:** mkcert works on macOS: install it with Homebrew from the account that owns `/opt/homebrew`, then run
   `mkcert -install` from the demo's account, and the launcher needs no certificate accept. The desk launcher's 4g
   default (`DESK_HEAP`, `desk.mjs:27`) makes a long show fail: a larger default, or a warning when the heap nears full.
   *Fixed 2026-10-28* (`desk.mjs`): the heap is half the machine's memory, 4g to 16g, unless `DESK_HEAP` says, and the
   launcher says at start about how long a show runs on it at the world's defaults (from 1's rates: 4g ~44 min, 8g
   ~2 h, 16g ~4 h).
9. **Run On's switch crashes the page's renderer, intermittently** (found 2026-10-28 in this container, not on the
   Mac): `//datacube:deephaven_desk_test`'s Run On (the saved desk on Deephaven, switched to the tab's DuckDB and
   back) failed "Target crashed" in 3 of 11 runs before D5 and 4 of 5 since -- it got worse with D5. Not memory: the renderer at 1.5-1.8 GB,
   no kernel or cgroup kill (`/proc/vmstat` oom_kill 0). The browser's own output (the lane's `browser.log` since
   2026-10-28) says `Received signal 11 SEGV_ACCERR`, in a worker thread's JIT code (frames 3-12 generated code, the
   thread started by libc), the faulting address a 4 GB-aligned base plus ~980 MB -- the shape of V8's pointer cage,
   so a heap access, not a WebAssembly bounds trap. When: as the tab's DuckDB starts its market (the switch to the
   tab) or just after. Not chased further: a browser engine fault on that path; the next step is the same switch in a
   smaller page (DuckDB started beside a Deephaven reader), then a Chromium bug with the trace.
   *Gone with the switch (2026-10-28, the user):* Run On ▸ is replaced by Open With ▸, which opens the saved page on
   the other context in a tab of its own (docs/ONE_CONNECTION_DESK_2026_10_28.md, step 4): 3 runs of 3 passed, no
   crash, the other tab in its own renderer. The fault itself was not found: a page that starts the tab's DuckDB
   beside a live Deephaven reader may still meet it.
10. **A linked view not following a pick within 30 s, sometimes** (2026-10-28, 1 run of 3, the first after a container
   restart, the desk opened slowly: 12.8 s): the Stock tab's news cards stayed 9 against their grid's 8 rows, and
   News's chart beside the panes did not move to the ticker clicked. No page error. Not chased yet.

## Gaps measured (2026-10-24)

Seventy-four Pure queries of a trading desk's kind, each planned with this branch's planner on a DuckDB runtime and on a
Deephaven runtime over the same store (a scratch probe, not committed). DuckDB plans every one but two (the write to a
store table, and a first spelling of a join that a rewrite replaced); what Deephaven refuses, by name, each with the
operation Deephaven has for it:

| Area | Refused on Deephaven | Deephaven's own | Blocks |
|---|---|---|---|
| Time (**built: K1**, except `now()` and `dateDiff`) | a DateTime literal; `now()->adjust`; `timeBucket`; `hour()` (EXTRACT); `datePart` (DATE_TRUNC_DAY); `firstDayOfMonth` (DATE_TRUNC); `dateDiff` | `lowerBin`, `now()`, calendar functions (a time zone each: a design point); a sliding window needs Deephaven's time-window helper, since `now()` in a filter is not evaluated again as time passes | candles, "the last 5 minutes", every DataCube time grain |
| Windows (**built: K6**, except EMA, delta and NTH_VALUE) | every window function (`updateBy` shape not built), and a filter over one (QUALIFY) | `updateBy` (cumulative, rolling by rows or time, EMA, delta); `headBy` for top-N | moving averages, tick change, running totals; all 15 of DataCube's window functions and its 6 show-as |
| As-of join | both forms | `aj` (DH20) | quote at trade time, markouts, TCA |
| The runtime error | `sqrt` and Integer `/`: the lowering's guard (Pure fails on `sqrt` of a negative and on division by zero) writes an `ERROR`, which has no shape | a design: how a Deephaven formula fails by name | volatility, ratios, Black-Scholes |
| Aggregates | percentile, `stdDevPopulation`, `variancePopulation`, `joinStrings`, `mode` (and #72's corr, covariance, max-by, min-by) | `pct`; the others computed or refused for good | 5 of DataCube's 15 aggregates |
| Scalars | `round`, `mod`, `substring`, `contains` (STRPOS), a number to text, a decimal literal (audit R9) | Java's own | |
| Relations | RIGHT and FULL joins, a join with no equality, the Relation API's `pivot()`, `drop` with no limit, `size()` | | DataCube's Join window offers right and full |

Planned on both: grouping with sum, count, average, min, max, median, the sample deviations, `wavg`, distinct counts and
`unique`; LEFT joins to a keyed table and INNER joins; `if`; sort and limit, slice, union, distinct, rename and select;
filters by a list, `startsWith`, nulls and ranges; `log`, `exp`, `pow`, `abs`, Float division; class queries through a
mapping. Refused on both: a write to a store table (the Cypher writes line, not on `main`).

**And in the integration:** no server-side execution (`Connectors` answers `Unbuilt`: no Python, Studio or REST over
Deephaven, and no semantic lanes to find the formula differences the register expects, L3s); the browser route trusts
the store's declaration for nullability (DH16's type check is built, item 5 above; DH19's refusal is L3s's: a `NOT NULL`
column that holds nulls loses the guards the lowering writes); tables are found by their query-scope name only (a Deephaven
application's fields and a Python server's tables not tried); no check of the server's version at sign-in (42.7 only is
tested); and this branch carries the MongoDB line's commits, which are not on `main` either.

## Now: one connection (the user, 2026-10-28)

The desk the same on DuckDB and Deephaven but for one connection, every table made and every row written by the
platform, the market simulator alone a program of its own: **docs/ONE_CONNECTION_DESK_2026_10_28.md** (revision 2: D1,
lite's connection words as JSON with the drop-in surface still refusing; D2, environments as a data space's execution
contexts, a saved page's context a default it can switch, the engine the connection's; D3, Market data a source of
DataCube's, its feeds per part; D4, Deephaven in production a remote database; its steps and their status).

## Next, as proposed to the user (2026-10-24; not yet chosen)

Settle 1-3, then **make it real for one desk**: the person's own sign-in, a real Deephaven with its real tables through
the model, counsel's review. Later, in the plan's order: the server-side sender (L3s); writes (W, Wr, X, L3w); the
modelled publisher (L5); views shared between viewers (S1, which revisits the idle cache's cost, audit R3).

**Chosen 2026-10-24: trader screens at realistic speeds** -- `docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md` (decisions
DH28-DH32, landings K1-K13, the bespoke ledger). The user: this line owns every missing Deephaven feature and whatever
its demos need, charts included (DH31). First the ticker: time on Deephaven (K1), the market world with a crypto feed
and a simulator in its place (K2), a candlestick chart (K3), the page (K4).

## Where the code is

| Part | Files |
|---|---|
| Target | `core/src/main/java/com/legend/sql/dialect/Deephaven.java`, `DeephavenBatch.java` |
| Encoder | `core/src/main/java/com/legend/deephaven/` (`DeephavenWire`, `DeephavenMessages`, `ProtoWire`) |
| Tab's sender and engine | `engine-client/src/deephaven.ts` (session), `deephaven-engine.ts` (engine, readers, following), `deephaven-live.ts` (the JS library), `deephaven-rows.ts`, `timeline.ts` |
| DataCube | `datacube/src/deephaven-grids.ts` (a server's engine, each grid its own reader, warming, reconnection: chosen by the runtime's connections, `runtime-engines.ts`, `runtime-connections.ts`), `server-base.ts` (bases), `runner.ts`, `app.ts` (tick, window), `grid/grid.ts` (`tickRows`, flash), `chart-render.ts` (held axes), `treeview.ts` |
| Demo | `datacube/demo/deephaven/market/` (`desk.mjs`, the hand tool; `desk.ts`, `screens.ts`, `environments.pure`, `market.groovy`); `mkcert.mjs` |
| Test benches | `datacube/demo/deephaven/` (`cube.ts`, `board.ts`, `risk.ts`, `risk.groovy`, `site.mjs`) |
| Tests and tools | `datacube/test/deephaven/`, `datacube/bench/deephaven/`, `testing/.../EmbeddedDeephaven.java`, `tools/deephaven/` |
