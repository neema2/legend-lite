# Streaming risk spike: Deephaven, Feldera and Proton on a trader's risk view (2026-10-21)

The question: for live risk (ticking prices and FX rates joined to positions and reference data,
then aggregated up a desk > trader > book hierarchy), which engine could be legend-lite's
streaming backend? Deephaven Community (the baseline, from `../deephaven-spike`), Feldera
(MIT, DBSP) and Timeplus Proton (Apache-2.0, a ClickHouse fork) each ran the same scenario, alone
on the machine, fed the same deterministic events, and were checked against an oracle.

**Answer.** All three compute the risk correctly and keep up at about 4,400 price updates a second.
They differ in what a legend-lite target would have to carry:

- **Feldera** has the lowest latency at 1,000 ticks a second, and its SQL is the ANSI text this
  spike also runs on DuckDB unchanged. But its views are compiled programs: a new live grouping
  is a recompile (22 s warm, 169 s cold here) and a restart; ad-hoc SQL answers only snapshots.
- **Proton** answers a new live query in about 250 ms and reuses our ClickHouse dialect's
  spelling. But its join performance depends on the join order (the obvious order could not
  keep up with 200 ticks a second; a reordered one handled 4,400), its default emit cadence is
  2 s, and each live query computes its own join (2.6 to 3.0 cores here).
- **Deephaven** does it on under a core with live ad-hoc queries, but carries the license and
  patent questions in `../deephaven-spike/README.md` and the review of 2026-10-21.

## The scenario (`scenario.py`, `views.py`)

| input | rows | changes |
|---|---|---|
| books (book, trader, desk) | 48 = 4 desks x 3 traders x 4 books | reference |
| instruments (sector, ccy, beta, multiplier) | 2,000 | reference |
| closes | 2,000 | reference |
| positions (book, instrument, qty) | 19,200 at the start | 20 bookings a second (10% new positions) |
| prices (instrument, px, ts) | 2,000 | 1,000 or 5,000 ticks a second |
| fx (ccy, usd_rate, ts) | 5 | 5 ticks a second (EUR, GBP, JPY, CHF) |

`position_risk` joins each position to its instrument, book, live price, live FX rate and close:
`mv_usd = qty * px * multiplier * usd_rate`, `pnl_usd = qty * (px - close_px) * multiplier *
usd_rate`, `beta_delta_usd = mv_usd * beta`. Four live views over it are DataCube's levels: the
grand total (a groupBy on a constant `ROOT`), desks, books, and P&L by desk pivoted on currency
(conditional sums). A price tick changes about 10 positions; an FX tick changes every position in
that currency (2,000 to 4,000) and every book. Every input is a keyed upsert in every engine.

## What was measured

`run.py` per engine: create the inputs and the four live views, subscribe to all four, load the
opening state, then send one batch every 100 ms for the run, prices and rates stamped with the send
time. Then:

- **tick → total**: for every batch, the time from sending it to the grand total reflecting its
  prices (what a trader watching the total sees);
- **FX tick → all 48 books**: from an FX tick to the last book reflecting it (the fan-out);
- **correctness**: after the last batch, both the subscriber's state (built only from the change
  sets the engine pushed) and a fresh snapshot of each view, against DuckDB running the same SQL
  over the final inputs (relative tolerance 1e-9; the largest difference seen was 2e-13);
- **a new live grouping while running** (DataCube's user regrouping by sector): time to first rows;
- **CPU and memory** of the engine (Deephaven's process; Feldera's and Proton's containers).

One 4-core cloud container; the generator, the subscribers (Python) and one engine at a time.

### 1,000 price ticks a second, 60 s

| | Deephaven | Feldera | Proton |
|---|---|---|---|
| tick → total, p50 / p95 / p99 | 63 / 111 / 115 ms | **43 / 66 / 83 ms** | 67 / 141 / 210 ms |
| FX tick → all 48 books, p50 / p95 | **74 / 126 ms** | 99 / 127 ms | 196 / 338 ms |
| engine CPU | **0.71 cores** | 1.97 cores | 2.56 cores |
| engine memory | 749 MB | 1,546 MB (container; the pipeline process alone is 225 to 360 MB, the rest is the compiler) | 1,033 MB (container) |
| setup: create or compile | 3.9 s | 21.9 s compile (169 s cold) | 0.1 s |
| new live grouping: first rows | 18 ms, live | 15 ms, **a snapshot**; live needs a recompile | 245 ms, live |
| correct (subscriber / snapshot) | yes / yes | yes / yes | yes / yes |

### About 4,400 distinct price updates a second, 30 s

(5,000 ticks a second scheduled; repeats on one instrument within a batch are one upsert.)

| | Deephaven | Feldera | Proton |
|---|---|---|---|
| tick → total, p50 / p95 / p99 | 70 / 111 / 118 ms | 86 / 128 / 148 ms | 71 / 130 / 209 ms |
| FX tick → all 48 books, p50 / p95 | 80 / 126 ms | 130 / 153 ms | 270 / 449 ms |
| engine CPU | 0.84 cores | 2.27 cores | 2.96 cores |
| correct | yes / yes | yes / yes | yes / yes |

### Proton's join order (`results/2026-10-21/proton-flat-10-200-book.json`)

The same view with the joins in the order `views.position_risk` writes them (positions to
instruments, books, prices, FX, closes), one live view, **200** ticks a second: tick → total p50
1,892 ms and growing, 3.5 s to drain after the last batch, one core saturated. Joining instruments
to FX first and then positions by instrument (`views.position_risk_fx_first`) is what the tables
above ran. The working guess is that a join stage keyed by currency holds thousands of rows under
one key, and each retraction searches them; Deephaven and Feldera ran the first order as written.

## Findings

1. **All three are correct**, through retractions and a 4,000-row FX fan-out, from both the pushed
   change sets and a snapshot.
2. **Feldera: lowest latency at 1,000 a second, and the cleanest SQL for us.** `TABLES_ANSI` and
   the view SQL run unchanged on Feldera and on DuckDB (the oracle). Its cost is compile-time
   views: the program compiles SQL to Rust and then to a native binary (169 s cold in this
   container, 12 to 22 s with the compiler's cache warm), and changing a view means a new program
   and a restart. Its ad-hoc SQL answers snapshots of materialized views in 15 ms, not live
   results. It also folds unquoted identifiers to lower case (`pnl_USD` comes back as `pnl_usd`).
3. **Proton: live ad-hoc queries, but the target must plan for it.**
   - Join order decides whether it keeps up (above).
   - Without `PERIODIC`, `EMIT CHANGELOG` emits every 2 s; `EMIT CHANGELOG PERIODIC 100ms` is
     accepted by the open-source build.
   - Each streaming query computes its own join: four views cost 2.6 cores where one cost 0.6.
     Sharing the join through a materialized view into a `changelog_kv` stream failed on the first
     try (`_tp_delta` is not visible through the join subquery) and was not pursued.
   - Like ClickHouse, an alias shadows a source column (`SUM(...pnl_usd...) AS pnl_usd` nests the
     sum in itself).
   - Operationally its defaults assume a dedicated disk: it refuses writes when the filesystem
     reads over 90% used (this container's allowance does), and each stream preallocates a 2 GB
     log segment (`proton-spike.xml` turns that off for the tmpfs the spike ran it on).
4. **Deephaven used the least CPU by a wide margin** and needed no join-order or emit tuning beyond
   the 100 ms cycle and subscription interval set in `dh_server.py` (its defaults are 1,000 ms).
5. **Change sets differ in shape.** Feldera pushes one change set per step with every changed row
   (15 to 19 a second for the books view); Deephaven one per update cycle (10 a second); Proton one
   row at a time (-1 then +1 per changed group, about 850 rows a second). A view server sitting in
   front of a grid has to coalesce Proton's.
6. **Neither Feldera nor Proton offers sorted viewport subscriptions.** A view server (sorted,
   position-aware viewports over a delta stream, as `../deephaven-spike` measured from Deephaven
   directly) is still to build for either.

## What it means for legend-lite

- **Feldera fits the compiler best** (an ANSI dialect, close to what `AnsiSqlRenderer` writes) and
  fits **declared live models**: a risk model deployed as a pipeline, its views DataCube's live
  levels. Interactive regrouping as live queries needs the follow-up below: groupings as data in
  one compiled program, which works but slows past a few live groupings in this shape.
- **Proton fits interactive live queries** and our ClickHouse dialect, but its target would have
  to own a join planner for streaming (orders, shared materialized views, emit policy), and a
  user-shaped query can fall off a performance cliff the compiler did not foresee.
- **Deephaven fits both** (ad hoc and cheap) but has the license and patent questions.

## Follow-up: every grouping compiled up front, run only when asked

Can Feldera's compile be paid once, with every dimension in the program, and a grouping run only
when DataCube asks for it? `dynamic_groupings.py` makes the grouping data: a `subscriptions` table
whose rows say which of desk, trader, book, sector and currency to group by and what to filter on.
The view joins every subscription with the risk and groups by the subscription id plus, per
dimension, the value or NULL. Inserting a row starts a grouping (its first answer comes from the
state the pipeline already holds), deleting it retracts its rows. Two shapes: **direct** joins the
19,200 position rows with the subscriptions; **leaf** first aggregates positions to the finest grain
of all five dimensions (2,363 rows) and joins that, every measure re-aggregating (sums of sums,
a sum of counts, a max of maxes). `dynamic_deephaven.py` asks Deephaven for the same 16 groupings
as new aggregations on its live risk table. 1,000 ticks a second; the 16 are DataCube-shaped
(levels, regroupings, an expanded group as a filter, the full five-dimension grain).

| active groupings | Deephaven: CPU / tick → subscriber p50, p95 | Feldera leaf: CPU / tick → subscriber p50, p95 | Feldera direct: CPU / tick → subscriber p50, p95 |
|---|---|---|---|
| 0 | 0.70 cores | 1.85 cores | 1.32 cores |
| 1 | 0.75 / 56, 117 ms | 1.96 / 41, 54 ms | 1.40 / 32, 40 ms |
| 4 | 0.86 / 63, 113 ms | 1.90 / 59, 87 ms | 1.60 / 48, 102 ms |
| 16 | 1.36 / 78, 104 ms | 1.95 / **310, 467 ms** | 1.63 / **1,100, 1,447 ms** |
| a new grouping: first rows | 10 to 327 ms (most under 50) | 30 to 286 ms | 54 to 1,184 ms |
| compile, once | none | 51 s | 11 s warm, 301 s cold |
| correct (16 groupings) | yes, subscriber and snapshot | yes | yes |

(Feldera's CPU and memory here are the pipeline process's own, from its statistics.)

- **Yes, it works:** one compiled program serves any grouping and filter over the declared
  dimensions, started and stopped by a row, correct, with no recompile.
- **It holds up to a few groupings and degrades at 16.** Feldera's own view (read directly, not
  through the subscriber) was 363 ms old at p50 with the leaf shape and 1.1 s with the direct one,
  with input waiting to be processed, while CPU stayed at about 2 of 4 cores. The working guess:
  the join with the subscriptions has no key, so one worker does it. Deephaven, attaching each
  grouping as its own aggregation, stayed at 78 ms p50 with 16 on 1.36 cores.
- **The leaf matters**: three times less latency at 16 than joining positions directly.
- Not tried: spreading the subscription join over workers (a bucketed key), a fixed menu of
  grouping sets, or rolling up the Feldera leaf outside Feldera (DuckDB in the DataCube tab
  re-running the open levels over the leaf on each tick, which keeps per-grouping work out of the
  engine entirely).

**The Python ticking client (pydeephaven-ticking 42.7), twice:** rebuilding rows from an update's
`added()`/`modified()` gave stale values when an update modified only some columns (up to 81 of
240 rows of a book x currency grouping), while the client's own table (`update.current`) and the
server's snapshot were right, so the adapter reads the client's table; and once, a 2,363-row
subscription failed inside the client ("Chunks have inconsistent sizes", `barrage_processor.cc`;
`results/2026-10-21/deephaven-dynamic-client-error.json`) and the rerun did not repeat it. The
server's tables were correct throughout. Python is the path DataCube's notebooks would take.

## Tuning series: what moves Feldera at 16 groupings (`feldera_tuning.py`, `results/2026-10-21/tuning/`)

The question behind it: is 16 groupings slow on Feldera only when they are data (a subscriptions
table), or also when they are ordinary views compiled into the program? Then: which program shapes
and runtime settings move it. One variant per run, 1,000 ticks a second, 30 s after a warm-up,
everything else as above; every run correct against DuckDB. "Feldera lag" is the age of the desk
grouping's newest data read straight from the view; "subscriber" is tick to the subscriber.

**The first measurements** (latency from the newest `last_px_ts`, so with the MAX columns):

| variant | subscriber p50 / p95 | Feldera lag p50 / p95 | pipeline CPU |
|---|---|---|---|
| 4 views compiled in | 40 / 53 ms | 93 / 138 ms | 1.86 cores |
| 8 views | 80 / 124 ms | 133 / 202 ms | 2.31 cores |
| 16 views | 257 / 369 ms | 322 / 426 ms | 2.58 cores |
| 16 views, egress of the probe view only | 234 / 347 ms | 295 / 420 ms | 2.60 cores |
| 16 views, storage off | 261 / 361 ms | 313 / 431 ms | 2.53 cores |
| 16 views, CPU profiler off | 241 / 352 ms | 301 / 417 ms | 2.54 cores |
| 16 views, both off | 249 / 419 ms | 303 / 445 ms | 2.52 cores |
| 16 views, no FX ticks | 199 / 279 ms | 252 / 339 ms | 2.59 cores |
| 16 views, 2 workers | 308 / 433 ms | 366 / 508 ms | 1.58 cores |
| 16 views, 8 workers (4 cores) | 382 / 545 ms | 428 / 597 ms | 3.08 cores |
| 16 views over the leaf | 230 / 326 ms | 286 / 402 ms | 2.62 cores |

**Then MAX against sums only**, on both engines, latency from a probe (`probe.py`: an instrument
priced at each batch's send time, so the Probe desk's market value is the timestamp; no MAX needed):

| variant (16 groupings) | with the two MAX columns | sums and count only |
|---|---|---|
| Feldera, views over positions | 217 / 277 ms, 2.55 cores | 127 / 170 ms, 2.55 cores |
| Feldera, views over the leaf | (230 / 326 ms above) | 122 / 161 ms, 2.54 cores |
| Feldera, groupings as data, leaf | (310 / 467 ms, the follow-up) | 122 / 162 ms, 1.87 cores |
| Feldera, groupings as data, leaf in one copy per worker | | 133 / 171 ms, 2.04 cores |
| Feldera, groupings as data, positions joined directly | (1,100 / 1,447 ms, the follow-up) | 257 / 374 ms, 1.69 cores |
| Deephaven, 16 aggregations attached at runtime | 81 / 107 ms, 1.35 cores | 94 / 112 ms, 0.91 cores |

What it says:

1. **16 groupings is slow on Feldera however they are written**: compiled views (257 ms) cost about
   what groupings as data over the leaf cost (310 ms). Latency grows with the groupings (40, 80,
   257 ms for 4, 8, 16) while CPU levels off at about 2.6 of 4 cores.
2. **MAX is the largest single cost.** Dropping the two MAX columns takes 16 views from 217 to
   127 ms. Feldera's documentation says why: SUM, COUNT and AVG are linear and keep no input; MIN
   and MAX must keep the whole input to find the next value when one is retracted (they are cheap
   only on append-only data). Deephaven kept the same MAX columns for 0.4 of a core and no
   latency.
3. **Without MAX, groupings as data cost the same as compiled views** (122 ms either way, on less
   CPU), so one compiled program can serve any grouping and filter over the declared dimensions,
   started and stopped by inserting and deleting a row, with no recompile. The leaf is what makes
   it work: joining positions with the subscriptions directly is twice as slow (257 ms).
4. **Not the cause:** the JSON output (one view's egress or 16 cost the same), storage, the CPU
   profiler, the subscription join's lack of a key (spreading it over the workers did not help),
   and the worker count (4 is best on 4 cores; 2 is slower, 8 slower still).
5. **Deephaven stays ahead at 16 groupings**: 81 to 94 ms p50 on 0.9 to 1.35 cores, its floor set
   by the 100 ms update cycle and subscription interval, against Feldera's 122 ms on 1.9 to 2.5
   cores at best.
6. **The Python ticking client failed on the largest subscription (2,364 rows) in 2 of 4 runs**
   ("Chunks have inconsistent sizes" in `barrage_processor.cc`); the server's snapshot was right
   both times.

## Running it

```sh
SP=/some/dir                                      # outside the source tree
python3 -m venv $SP/venv && $SP/venv/bin/pip install deephaven-server==42.7 pydeephaven==42.7 \
  pydeephaven-ticking==42.7 proton-driver duckdb
dockerd &                                         # if no daemon runs
docker run -d --name proton --tmpfs /var/lib/proton:size=6g,exec,uid=101,gid=101,mode=0755 \
  -v $PWD/proton-spike.xml:/etc/proton-server/config.d/zz-spike.xml:ro \
  -p 8123:8123 -p 8463:8463 ghcr.io/timeplus-io/proton:latest      # 3.0.31, sha256:85736f6d5c5c
docker run -d --name feldera -p 8080:8080 ghcr.io/feldera/pipeline-manager:latest  # 0.367.0, sha256:42819714d16f
$SP/venv/bin/python dh_server.py &               # Deephaven 42.7 on :10000
# one engine at a time (stop the others):
$SP/venv/bin/python run.py --engine feldera --seconds 60 --rate 1000 --out results/feldera.json
$SP/venv/bin/python run.py --engine proton --proton-shape fx_first --seconds 60 --rate 1000 --out results/proton.json
$SP/venv/bin/python run.py --engine deephaven --deephaven-pid <pid> --seconds 60 --rate 1000 --out results/deephaven.json
$SP/venv/bin/python summarize.py results/*.json
$SP/venv/bin/python dynamic_groupings.py --shape leaf --out results/feldera-dynamic-leaf.json
$SP/venv/bin/python dynamic_deephaven.py --pid <pid> --out results/deephaven-dynamic.json
```

Feldera's image is 10.9 GB unpacked. The Proton container's data lives on a tmpfs because its disk
check reads this container's allowance as full.
