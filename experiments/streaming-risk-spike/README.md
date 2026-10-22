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
  2 s, and each live query computes its own join (2.6 to 3.0 cores here). The homework of
  2026-10-22 (below: deletes, restarts, 5 times the book, DataCube's query shapes) found it
  correct only when the target plans for it: a leaf-cube plan ran 16 live groupings over 96,000
  positions on 0.73 cores, while the obvious plans fail without an error.
- **Deephaven** does it on under a core with live ad-hoc queries, but carries the license and
  patent questions in `../deephaven-spike/README.md` and the review of 2026-10-21. The same
  homework (below) found it right on every test as written, to 20 times the book, where Proton
  needed a plan for each; its own questions are reload on restart, a Python client bug, and the
  license.

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
     Sharing the join through a materialized view works (the first try lacked `EMIT CHANGELOG`;
     a `versioned_kv` target needs nothing extra, but cannot carry a deleted position: see the
     Proton homework) and changes the picture: see "Proton at 16 groupings" below.
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
  user-shaped query can fall off a performance cliff the compiler did not foresee. The homework
  below lists what that target owns; each item has a tested answer, and the risk left is that
  Proton's failures are silent.
- **Deephaven fits both** (ad hoc and cheap) but has the license and patent questions. The
  homework of 2026-10-22 adds that it was right on every test without planning around it.

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

## Proton at 16 groupings (`proton_groupings.py`, `results/2026-10-22/proton-groupings/`)

The same 16 groupings, probe and phases as the Feldera and Deephaven tests above, 1,000 ticks a
second, in two modes. **perquery**: each grouping is its own streaming query over the whole join
(instruments to FX first). **shared**: one materialized view runs the join once and writes each
position's risk into a `versioned_kv` stream keyed by book and instrument
(`CREATE MATERIALIZED VIEW ... INTO position_risk_s AS <join>`); each grouping is a streaming
aggregation over that stream with `EMIT CHANGELOG PERIODIC 100ms`.

| 16 groupings live | CPU | tick → subscriber p50 / p95 | a new grouping: first rows | correct |
|---|---|---|---|---|
| Proton, shared join, with MAX | 1.26 cores | 24 / 121 ms | 113 to 192 ms | yes |
| Proton, shared join, sums only | 1.17 cores | 24 / 121 ms | 109 to 202 ms | yes |
| Proton, a join per grouping, with MAX | 3.70 cores (saturated) | 62 / 108 ms (the probe's grouping) | 304 to 1,586 ms | after a 60 s drain (5 groupings still behind after 5 s) |
| Proton, a join per grouping, sums only | 3.69 cores (saturated) | 73 / 119 ms | 260 to 1,111 ms | yes |
| Deephaven, with MAX (above) | 1.35 cores | 81 / 107 ms | 14 to 372 ms | yes |
| Feldera, groupings as data over the leaf, sums only (above) | 1.87 cores | 122 / 162 ms | 30 to 290 ms | yes |

With the join shared, each extra grouping costs Proton little (0.69, 0.79, 1.26 cores for 1, 4,
16 live), MAX costs it almost nothing, and a new grouping is live in about 0.2 s with no compile.
Run as a join per grouping it saturates four cores at 16 and some groupings fall behind (the
check with a 5 s drain failed for 5 of them; with 60 s every grouping matched, so they lagged,
they were not wrong).

What this makes the Proton target: the model's join, declared once per source, as a materialized
view (its join order chosen by the target, as `fx_first` was here by hand), and DataCube's levels
and regroupings as ad-hoc streaming aggregations over it. That is the shape DataCube needs: live,
ad hoc, no compile, cheap per grouping. Still open then: many viewers of one grouping (each a
query of its own, or a view server fanning one out), the join order (planned by the target, not by
hand), and the operational defaults above; the Proton homework below answers them, and finds that
this shape (FX applied per position, a `versioned_kv` target) breaks once positions close or the
book grows. The 24 ms p50 against Deephaven's 81 ms is configuration
(Proton emits every 100 ms here; Deephaven's 100 ms update cycle and 100 ms subscription interval
add up), not a difference between the engines.

## Proton homework (2026-10-22, `proton_*.py`, `results/2026-10-22/proton-homework/`)

Everything we would want to know before making Proton (3.0.31, open source) the engine behind live
DataCube: correctness under a real feed, the query shapes DataCube asks for, the performance
envelope, operations, and how results leave it. Each script checks its result against DuckDB (or
against the same join run as a batch query in Proton) and is in `proton_lab.py`'s harness.
Statements about Proton's internals were read from its source at the 3.0.31 tag, not run; they say
so.

**The verdict.**

- **Proton computes the risk correctly when the target plans for it**, and silently wrong when it
  does not. Five ways to a wrong answer with no error: a `versioned_kv` derived stream when
  positions close; a `changelog_kv` derived stream behind a large backlog; a server killed with
  SIGKILL; a nested aggregation over a join to FX; an as-of join with late quotes. And one way to
  stale data that looks fresh: at 5 times the book a latency probe read 18 ms while the join was
  minutes behind. Every one has a workaround the target can own.
- **The plan that works:** the join in local currency as one materialized view into a
  `changelog_kv` stream; a leaf cube (`desk, trader, book, sector, ccy`) as a second view; FX
  applied to the leaf cube by a third; DataCube's groupings as ad-hoc streaming sums over it. At 5
  times the book (96,000 positions) that ran all 16 groupings correct on 0.73 cores, a new grouping
  live in 115 to 145 ms, ticks reaching a grouping in 147 to 250 ms depending on the emit policy.
- **Operations need work around it:** a graceful restart is clean; a SIGKILL corrupts every derived
  stream (rebuild them: 2.4 s here); the disk check and log preallocation need configuring; the log
  and the historical store stay bounded.
- **Delivery:** HTTP streaming on 3218 works from a browser; our ClickHouse dialect is a start (127 of
  its 158 function names parse in Proton's compatibility mode) but a Proton target is a dialect of
  its own.

### Correctness under a real feed (`proton_correctness.py`)

Positions as a `changelog_kv` stream so a position can close: an update is sent as the old row
retracted (`_tp_delta` -1) and the new one added; a close is the old row retracted. Five groupings
live, 1,000 ticks a second for 40 s; mid-run a book moves desk (batch 100), an instrument's
multiplier is corrected tenfold and another is re-currencied (batch 150), 50 positions close
(batch 200). After a 10 s drain, every grouping against DuckDB over the final inputs:

| shared join's target | groupings correct | rows in the shared stream (expected 19,230) |
|---|---|---|
| `changelog_kv` (view with `EMIT CHANGELOG`) | 5 of 5, to 1e-13 | 19,230 |
| `versioned_kv` (view, no EMIT) | 0 of 5 (desk off by up to 22%) | 19,278: the closed positions stay |

- A `versioned_kv` stream upserts but cannot delete: the join's retraction of a closed position has
  nowhere to go, so it stays in the shared stream and in every grouping over it. Reference
  corrections and the desk move were handled by both (they are upserts). **The shared join must
  write a `changelog_kv` stream** (but see the backlog result below). The 16-grouping result above
  used `versioned_kv`; its feed closed no positions.
- A `changelog_kv` input is a changelog, not a table: a plain insert for a key already there adds a
  second row, it does not replace the first. A feed into it sends -old/+new (what a CDC source
  sends), so the source of record, or an adapter in front of Proton, must know the old row. Proton's
  source documents the changelog as assumed "totally ordered and exactly once"; a wrong old row
  corrupts every sum over it.

### As-of join (`proton_asof.py`)

Each trade (an append stream, 200 a second) with the quote in force when it printed (a
`versioned_kv` stream of 1,000 quotes a second over 200 instruments, `keep_versions = 100`),
slippage averaged per instrument live, against DuckDB's `ASOF JOIN` over the same rows:

| quotes arriving | instruments matching DuckDB |
|---|---|
| in order | 200 of 200, exactly (6,000 trades) |
| 5% of quotes 500 ms late (event time unchanged) | 47 of 200 |

(Every quote has a unique time, so the oracle needs no tie-break; a first version shared one
millisecond per batch and gave the same 47.) A streaming as-of join matches each trade against the quotes present when the trade arrives and
never revisits it (`ASOF` and `LATEST` joins emit append rows, not a changelog): a late quote is
never matched. Event-time correctness needs the feed in order per instrument, or a reorder buffer
the target owns.

### The query shapes DataCube asks for (`proton_shapes.py`, `proton_delivery.py`)

Each shape live over the shared `changelog_kv` stream (`EMIT CHANGELOG PERIODIC 100ms`) while the
feed ran, then against DuckDB:

| shape | result |
|---|---|
| avg; sum/sum ratio; `count(DISTINCT x)`; max; min; IN filter; calculated columns (`sum(if(...))`); pivot by currency; HAVING; detail rows | correct (first rows 32 to 203 ms) |
| `unique_exact(x)`, `count_distinct(x)` | correct |
| stddev written as sums (`sqrt(sum(x*x)/n - (sum(x)/n)^2)`) | correct |
| top 10 by \|P&L\| as `max_k(abs(total), 10, instrument, total)` over a nested grouping, `EMIT PERIODIC 1s` | correct, in order |
| `uniq(x)`, `uniq_exact(x)` (the ClickHouse names) | refused: "Method negate is not supported" |
| `stddev_pop(x)` | refused: negate not supported |
| `median(x)` | refused: not supported in changelog query processing |
| `rank() OVER (ORDER BY v)` | refused: window functions do not run in streaming queries |
| top 10 as `ORDER BY ... LIMIT 10` | ends the query after 10 rows: a streaming `LIMIT` counts rows across the query and stops it |

Over a changelog every aggregate has to retract. Proton rewrites the ones it can (count, sum, avg,
min, max, arg_min/arg_max, min_k/max_k, unique/unique_exact, group_array, with `_if` and
`_distinct`) to retracting variants and refuses the rest, which is the right failure. The target
needs that list (a map in Proton's `SubstituteStreamingFunction.cpp`) and lowers around it:
variance as sums, top-N as `max_k`, distinct counts in Proton's spelling, median and window ranks
not live.

### Performance (`proton_perf.py`)

The shared join (into `changelog_kv`) and the 16 groupings over it, 30 s per row, every grouping
checked against DuckDB after the feed. "Ticks" are draws of a random instrument; a 100 ms batch
keeps the last price per instrument, so 5,000 ticks a second is about 4,400 distinct updates and
20,000 about 12,600. RSS is the server process's resident memory (the container's own figure also
counts the tmpfs the data lives on).

| variant | CPU | probe p50 / p95 | correct after the feed |
|---|---|---|---|
| 1,000 ticks/s, joins in the obvious order (`flat`) | 1.25 cores | 18 s / 29 s | no: still behind after 15 s |
| 1,000 ticks/s, FX joined to instruments first (`fx_first`) | 1.01 cores | 31 / 117 ms | yes |
| 5,000 ticks/s | 1.37 cores | 29 / 112 ms | yes |
| 10,000 ticks/s | 1.67 cores | 42 / 128 ms | yes |
| 20,000 ticks/s | 2.08 cores | 57 / 129 ms | yes |
| `EMIT ON UPDATE` instead of `PERIODIC 100ms` | 1.12 cores (RSS 0.98 GB) | 27 / 103 ms | yes |
| `EMIT PERIODIC 1s` | 1.00 cores (RSS 0.85 GB) | (1 s cadence) | yes |
| + 50 viewers of the desk grouping, each its own query | 1.45 cores (RSS 1.19 GB) | 64 / 138 ms (a viewer) | yes; a viewer's first rows 390 to 730 ms |
| + 50 viewers tailing one materialized desk grouping | 1.16 cores (RSS 0.91 GB) | 57 / 131 ms (a viewer) | yes; first rows 127 to 131 ms |
| book x5 (96,000 positions), 1,000 ticks/s | 1.36 cores (RSS 1.05 GB) | 18 / 110 ms | **no** (below) |
| book x5, 5,000 ticks/s | 1.76 cores (RSS 1.27 GB) | 30 / 52 ms | **no** |

- The emit cadence barely changes the cost (1.00 to 1.12 cores): the join and the aggregations are
  the work, not the emits.
- Many viewers of one grouping are cheapest as a materialized grouping (a view into a
  `changelog_kv` stream) that each viewer tails: a plain streaming `SELECT` of a `changelog_kv`
  stream returns its current rows, then its -1/+1 changes. 50 viewers cost 0.15 cores that way and
  0.44 as 50 queries, and a viewer's first rows come 3 to 5 times sooner.
- **The probe hid the failure at x5.** Its tick reached the desk grouping in 18 ms while the rest of
  the join was minutes behind: Proton's join takes whichever input has data, with no ordering
  across inputs (read from source: `JoinTransform.cpp`), so the probe's price path never waited
  behind the FX fan-out. A latency probe measures one path; only the check against DuckDB caught
  it. A live view over a lagging join shows current prices with minutes-old FX, and nothing says so.

### Where the per-position join breaks (`proton_scale_check.py`)

After 30 s of feed, once the shared stream had stopped changing, every row of it against the same
join run as a batch query over the inputs' current state (`table()`):

| book | positions | last write after the feed ended | rows with a stale price / stale FX / wrong value |
|---|---|---|---|
| x1 | 19,254 | 2 s | 0 / 0 / 0 |
| x2 | 38,455 | 37 s | 0 / 0 / 0 |
| x3 | 57,648 | 128 s | 0 / 0 / 0 |
| x5 | 96,035 | 380 s | 33,039 / 19,219 / 42,698 |
| x5, 100 ticks/s (FX unchanged) | 96,062 | 384 s | 19,927 / 28,570 / 41,135 |
| x5, `versioned_kv` target | 96,035 | 387 s | 0 / 0 / 0 |
| x5, a plain `changelog` target (no key), summed by `_tp_delta` | 96,035 (4.24 million change rows) | 383 s | every key nets to one row; desk totals exact |
| x5, no view: a desk grouping straight over the join | | 255 s | desk totals exact |

(From x2 down, these runs shared the machine with tests on a second container; their catch-up
times are upper bounds. The first x5 row ran alone.)

- **The driver is the FX fan-out, not the price rate.** Each of the 5 FX ticks a second changes a
  fifth of the instruments, so a fifth of all positions is retracted and re-inserted through every
  join stage (about 19,000 pairs per tick at x5; 4.24 million change rows for a 30 s feed). Past
  about 40,000 positions the view cannot keep up with 5 FX ticks a second.
- **The join is right; the `changelog_kv` stream holding its output is not.** The same changes
  written to a `versioned_kv` stream, to a plain `changelog` stream, or aggregated directly all
  end exact; written to a `changelog_kv` stream behind a large backlog, a third of its rows end
  stale, permanently, with nothing in the server log. Proton's source points at the stream's
  collapsing merge: a `changelog_kv` stream is a VersionedCollapsingMergeTree whose merge Proton
  changed to cancel a retraction against the last row of opposite sign for the key, and depending
  on the merge (horizontal or vertical) the version is or is not compared, so a retraction and
  its replacement written in the same millisecond can cancel each other (read from source:
  `VersionedCollapsingAlgorithm.cpp`; that it explains these rows is our inference).
- So for a large or bursty derived stream the safe sinks are `versioned_kv` (no deletes) or a
  plain `changelog` (exact, but it grows without compaction and a snapshot is a sum by
  `_tp_delta`). Keeping the derived streams small, as below, keeps `changelog_kv` out of trouble.

**What fixes the scale problem is a plan, not a setting** (`proton_fx_late.py`). USD values are
linear in the rate, so FX can be applied after aggregation: one view joins positions to
instruments, books, prices and closes in local currency; a second sums that by (desk, trader,
book, sector, ccy) into a `changelog_kv` leaf cube (2,401 rows at x5); a third joins the leaf cube
to FX. An FX tick then changes 480 leaf rows instead of 19,000 positions. Every one of the 16
groupings is a sum over the leaf cube.

| x5 book, 1,000 ticks/s | CPU | probe p50 / p95 | last write after the feed | a new grouping: first rows | correct |
|---|---|---|---|---|---|
| per-position FX (above) | 1.36 cores | 18 / 110 ms (misleading) | 380 s | 179 to 327 ms | no |
| FX after aggregation, leaf cube, 16 groupings | **0.73 cores** | 249 / 275 ms | 1 s | 115 to 145 ms | **yes** |
| the same, the leaf view on `EMIT CHANGELOG ON UPDATE` | 1.19 cores | **147 / 162 ms** | 1 s | | **yes** |
| the leaf cube at x1 (19,200 positions), 16 groupings | 0.72 cores | 244 / 270 ms | 1 s | 113 to 131 ms | yes |
| FX after aggregation as one nested query per grouping | | | 3 s | | **no: totals about 16 times too large** |

The nested form (`SELECT k, sum(mv_l * usd_rate) FROM (SELECT k, ccy, sum(...) ... GROUP BY k, ccy)
JOIN fx ... GROUP BY k`) is accepted and silently wrong: the inner aggregation's retractions do
not survive the join to `fx`, so the outer sum adds every update. Materializing each step as a
view into its own stream works. The leaf cube's cost is latency (three views in a row); at x1 it
costs less CPU than the per-position join (0.72 against 1.01 cores) and adds about 200 ms.

### Operations (`proton_ops.py`, a second container with its data on disk: `proton-ops.xml`)

**Restart in the middle of the feed.** The feed (1,000 ticks a second) holds each insert and
retries it until the server answers; the desk grouping reconnects when its query dies; 15 s in,
the container restarts. After the feed, everything against DuckDB.

| restart | inserts refused | after it |
|---|---|---|
| graceful (`docker restart`, SIGTERM) | 10 s (the server takes 9 to 10 s to stop) | correct: inputs, shared stream (no duplicate keys), the reconnected grouping (its first tick 0.4 s after the server was up, then 49 / 108 ms), five fresh groupings, a `table()` aggregate; twice |
| hard (SIGKILL), 15 s in | 1 s | **wrong**: the shared stream holds 38,503 rows for 19,278 positions (every key twice); every grouping wrong; 3 times out of 3 |
| hard, 80 s in (after the view's first checkpoint) | 1 s | **wrong**: 36 keys twice, 4 of 5 desks wrong |
| hard, the view with `SETTINGS checkpoint_interval=5`, 15 s in | 1 s | **wrong**: once 949 rows missing, once 1 key twice (the results file is the second run) |
| any hard case, then the shared stream and its view dropped and created again | | correct, rebuilt in 2.4 s |

- The inputs (`versioned_kv` streams) came through every restart exact. What breaks is the
  materialized view's output: after a SIGKILL it replays its inputs from its last checkpoint (or,
  before its first, from the start: the first automatic checkpoint came about 60 s after the view
  started) and writes the replayed changes again into a stream that is not idempotent, so the
  derived stream drifts and nothing reports it. A graceful stop takes a final checkpoint and is
  clean.
- So the target treats every derived stream as a cache: after any stop that was not a graceful
  shutdown, it drops and rebuilds the views from the inputs. At this book size that took
  2.4 s; it grows with the book.

**Disk.**

- Proton refuses inserts when its disk reads over 90% used (`max_local_disk_usage_ratio`, a
  top-level key read at startup), measured as `1 - available/total` of the whole filesystem. In a
  container with a disk allowance this fires with the disk nearly empty; `proton-ops.xml` sets 0.98.
- Each stream shard's log segment is preallocated at 2 GB (`preallocate: true`, `segment_size`
  2 GB): seven small streams reserve 14 GB before a row arrives. `proton-ops.xml` uses 16 MB
  segments without preallocation.
- The log is bounded by default: the server's retention (`retention_size: 1`) deletes closed
  segments once their rows are in the historical store and the log is over `min_size_to_keep` (2 GB
  by default, 16 MB here). Over 4 minutes at 1,000 ticks a second the shared stream's log rose to
  79 MB and fell back to 21 to 33 MB, and every check against DuckDB afterwards passed (a query that
  starts after deletion reads what it needs from the historical store).
- The historical store collapses: after those 4 minutes the shared stream's one active part held
  exactly its 19,582 live rows (1 MB); the other 335 MB on disk were merged-away parts, which the
  server deletes later (we saw them gone within minutes; not timed). Dropped streams' logs linger
  until the next retention check (5 minutes by default).

### Delivery and dialect fit (`proton_delivery.py`)

- **HTTP streaming works** on 3218: a changelog grouping posted to `/?default_format=JSONEachRow`
  answers chunked NDJSON, first bytes in 151 ms, then each emit as it happens (15 reads a second
  for 70 rows a second here). `add_http_cors_header=1` adds `Access-Control-Allow-Origin: *` when
  the request carries an `Origin`, as a browser's does. 8123 (the ClickHouse-compatible port) runs
  queries as snapshots only; the same streaming query fails there.
- **Our ClickHouse dialect against Proton.** Every function name `ClickHouse.java` writes (158),
  called under `SET is_clickhouse_compatible=1`: 127 known (105 of them only with the setting:
  Proton's own names are snake_case), 31 unknown, among them `argMax`/`argMin`, every `*OrNull`
  aggregate, `ntile`, `printf`, `regexpExtract`, `toDate32`, `toDecimalString`, `mapFromArrays`.
  ClickHouse type names (`Nullable(String)`, `Float64`, `LowCardinality(String)`, `Array(...)`)
  are accepted with the setting and refused without it, and the setting works on the streaming
  port too. So a Proton target is a dialect of its own (snake_case names, the retraction rules,
  `EMIT`, stream modes), sharing much with `ClickHouse.java` but not a flag on it.

### What a Proton target in legend-lite would have to own

1. **A streaming join planner:** join order (FX to instruments first), FX applied after
   aggregation through a leaf cube, and each derived step materialized as its own view, never a
   nested aggregation over a join.
2. **Stream modes by role:** inputs that can delete as `changelog_kv` fed -old/+new, upsert-only
   inputs as `versioned_kv`, derived streams sized so a backlog never builds (or a sink that is
   exact under one).
3. **The retraction list:** which aggregates retract, the rewrites (variance as sums, top-N as
   `max_k`, Proton's spelling of distinct counts), and which DataCube features are not live
   (median, window ranks).
4. **Freshness it can see:** Proton has no cross-input watermark, so the target stamps its own
   (a probe per input, compared at the grouping) rather than trusting one path's latency.
5. **Recovery:** detect an unclean stop and rebuild the derived streams from the inputs.
6. **Configuration:** the disk ratio, segment size and preallocation, emit policies.

That is a real compiler target, and every item has a tested answer here. What it cannot fix from
outside is the class of failure: five times Proton gave a wrong answer without an error, and once
stale data looked fresh. Deephaven has been put through the same tests: next section. Feldera has
not.

## Deephaven homework (2026-10-22, `dh_*.py`, `results/2026-10-22/deephaven-homework/`)

The Proton homework's questions put to Deephaven Community 42.7, with the same scenario, feed,
probe and DuckDB oracle (`dh_lab.py` is the harness; the server is `dh_server.py`, one process per
test, started by the test). The join is the obvious one, written as `views.position_risk` writes
it: positions natural-joined to instruments, books, prices, FX and closes, FX applied per
position. Nothing was planned around Deephaven.

**The verdict.** Everything Proton needed a plan for, Deephaven did as written, and every result
was right. Its open questions are elsewhere: it keeps nothing across a restart (the target reloads
from the sources of record, which took 3 to 7 s here), its SQL front end is too narrow to be the
target's language (the target sends table operations instead), its Python ticking client has a
bug, and its license (`../deephaven-spike/README.md`).

| | Proton 3.0.31 | Deephaven 42.7 |
|---|---|---|
| positions closed, books moved, instruments corrected (`*_correctness.py`) | right only with a `changelog_kv` derived stream and a feed sending -old/+new; a `versioned_kv` one keeps closed positions | right, as written: a close is a delete of the key |
| as-of join, 5% of quotes 500 ms late (`*_asof.py`) | 47 of 200 instruments right: a trade is never matched again | 200 of 200 right: trades are matched again when a late quote lands |
| DataCube's 14 query shapes live (`proton_shapes.py`, `dh_shapes.py`) | 10 right as written, 2 more through rewrites (standard deviation as sums, top-N as `max_k`); median and window rank refused; a nested aggregation over a join silently 16 times too large | 14 of 14 right as table operations, median, standard deviation, a live top 10 and a rank included |
| 16 groupings, today's book, 1,000 / 20,000 ticks a second | 1.01 / 2.08 cores, probe p50 31 / 57 ms | 0.83 / 1.45 cores, probe p50 78 / 89 ms |
| the book x5 (96,000 positions), FX applied per position | 380 s behind a 30 s feed, then a third of the rows stale for good | 1.30 cores, 67 / 111 ms, every row right |
| the book x5, Proton's leaf-cube plan (FX after aggregation) | 0.73 cores, 249 / 275 ms, right | (not needed) |
| the book x10 / x20 (192,000 / 384,000 positions) | (not run: x5 already failed) | 1.73 / 2.80 cores, every row right |
| 50 more viewers of the desk grouping | +0.15 cores (one materialized grouping), +0.44 (50 queries) | +0 (50 identical aggregations are one table) |
| restart in the middle of the feed | graceful: clean; SIGKILL: every derived stream wrong until rebuilt (2.4 s) | either: empty; rebuilt and reloaded in 2.8 s, then right |
| what it needs on disk | its own disk settings; log and store bounded | none (memory only: 0.6 to 1.5 GB resident here) |
| license | Apache-2.0 | Deephaven Community License (`../deephaven-spike/README.md`) |

### Correctness (`dh_correctness.py`, `dh_asof.py`)

- **Reference changes and deletes:** proton_correctness.py's feed (a book moved, a multiplier
  corrected tenfold, an instrument re-currencied, 50 positions closed) at 1,000 ticks a second for
  40 s. All five groupings right, to 7e-14, both the subscriber's state and a server snapshot; the
  risk table held exactly the 19,230 live positions. Positions are a keyed input table: an update
  is an upsert, a close a `delete` of the key. The feed needs no old row.
- **As-of join:** trades and quotes as append-only input tables, `trades.aj(quotes, on=["instrument",
  "tts >= qts"])`, slippage averaged live per instrument. In order: 200 of 200 instruments right. With
  5% of quotes 500 ms late: 200 of 200 right; Proton, the same test, 47. (Both tests give every
  quote a unique time; with millisecond times shared within a batch, the oracle's tie-break, not
  the engine, decided some rows.)

### The query shapes (`dh_shapes.py`)

Each shape two ways over the per-position risk table while the feed ran 30 s, then against
DuckDB: as table operations (what a legend-lite plan would send), and as the ANSI SQL DuckDB ran,
through Deephaven's SQL (`deephaven.experimental.sql.evaluate`, Calcite-based).

| shape | table operations | SQL |
|---|---|---|
| avg; sum/sum ratio; count distinct; max; min; IN filter; top 10 by \|P&L\| (`ORDER BY ... LIMIT 10`); detail rows | right, live | right, live |
| calculated columns and the currency pivot (conditional sums) | right, live (`update_view` with a ternary, then sums) | refused: `Unsupported operator CASE` |
| HAVING | right, live (aggregate, then `where`) | refused: `IllegalStateException` |
| standard deviation | right, live (`agg.std`) | refused: `Unsupported aggregation STDDEV_SAMP` |
| median | right, live (`agg.median`) | refused: no function `MEDIAN` |
| rank (`RANK() OVER (ORDER BY v DESC)`) | right, live (sort, then `update_by(cum_sum)`) | refused: window functions unsupported |

First rows in 7 to 85 ms. A live top 10 is a sorted table's head, so it stays right as instruments
move in and out of it. The SQL front end refuses loudly what it cannot do, but 8 of 14 shapes is
too few for legend-lite to target it: a Deephaven target sends table operations (the plan route
of `../deephaven-spike`), not SQL.

### Performance and scale (`dh_perf.py`)

The 16 groupings of dynamic_groupings_spec.py over the per-position risk table, each subscribed, 30 s
per row; afterwards every grouping (subscriber and snapshot) against DuckDB, and every row of the
risk table against the final inputs (stale price, stale FX rate, missing or extra position).
"Ticks" as in Proton's table. Memory is the server process's resident set.

| variant | positions | CPU | memory | probe p50 / p95 | last batch reached the desk grouping in | right |
|---|---|---|---|---|---|---|
| 1,000 ticks/s | 19,201 | 0.83 cores | 0.79 GB | 78 / 109 ms | 87 ms | yes |
| 5,000 ticks/s | 19,201 | 1.15 cores | 0.81 GB | 63 / 118 ms | 0 ms | yes |
| 10,000 ticks/s | 19,201 | 1.31 cores | 0.97 GB | 84 / 114 ms | 71 ms | yes |
| 20,000 ticks/s | 19,201 | 1.45 cores | 0.78 GB | 89 / 124 ms | 20 ms | yes |
| book x5, 1,000 ticks/s | 96,001 | 1.30 cores | 0.83 GB | 67 / 111 ms | 10 ms | yes |
| book x5, 5,000 ticks/s | 96,001 | 1.54 cores | 0.90 GB | 76 / 116 ms | 65 ms | yes |
| book x10, 1,000 ticks/s | 192,001 | 1.73 cores | 1.15 GB | 64 / 104 ms | 61 ms | yes |
| book x20, 1,000 ticks/s | 384,001 | 2.80 cores | 1.50 GB | 61 / 111 ms (195 updates seen, of about 295) | 0 ms | yes |
| update cycle 1 s instead of 100 ms | 19,201 | 0.77 cores | 0.74 GB | (1 s cycle) | 751 ms | yes |
| + 50 viewers, each its own desk aggregation | 19,201 | 0.79 cores | 0.77 GB | 68 / 113 ms | 0 ms | yes; first rows 10 to 23 ms |
| + 50 viewers subscribed to the one desk grouping | 19,201 | 0.78 cores | 0.77 GB | 71 / 114 ms | 0 ms | yes; first rows 85 to 122 ms |

- **The per-position FX fan-out that broke Proton at x5 is routine here**: 20 times today's book, a
  fifth of it re-priced 5 times a second, every row right, the probe within the 100 ms cycle. At x20
  the probe saw fewer distinct updates than batches sent: some cycles took longer than 100 ms and
  merged batches, the first sign of the limit, still without falling behind.
- **The probe means what it says.** Deephaven computes each update cycle over all inputs at once, so
  a probe's latency is the whole view's latency (Proton's was one path's).
- **Viewers are free.** 50 identical desk aggregations of one table are one table (Deephaven reuses
  an operation it has already built on the same parent), and 50 subscriptions to one table cost
  nothing measurable.
- **Its Python ticking client has a bug.** In 6 of these 11 runs a subscription failed with "Chunks
  have inconsistent sizes" (pydeephaven-ticking 42.7, the C++ core under it), where recorded on the
  largest grouping (2,364 rows), and each time with the server's table right; the harness
  resubscribes and counts it. The browser client (`../deephaven-spike`) showed no such error, but a
  Python consumer of legend-lite would need the fix or a workaround.

### Operations (`dh_ops.py`)

Deephaven Community keeps its tables in memory only: a restarted server is empty. The feed (1,000
ticks a second) holds each batch and retries it; 15 s in, the server process is stopped and
started again, and the target does what it would have to: create the inputs and view again, load
the current state from the sources of record (here the feed's last state), subscribe again.

| restart | stop | start | rebuild and reload | inserts refused | after it |
|---|---|---|---|---|---|
| graceful (SIGTERM) | 10.2 s | 3.2 s | 2.8 s | 16.4 s | right: five groupings, the resubscribed desk grouping (first rows 72 ms after the reload), the risk table's 19,278 rows |
| hard (SIGKILL) | 0.1 s | 3.5 s | 2.8 s | 6.4 s | right (first rows 139 ms after the reload) |

- A restart loses nothing that matters only if the sources of record can be read again: positions
  from their system, the last price and rate per instrument from the market-data cache. For an
  append-only input (the as-of test's quote history) that means replaying it, from Kafka or a log.
  That is the target's job, and it is the same job whether the stop was clean or not. Proton keeps
  its inputs across a restart, which Deephaven does not, but after a SIGKILL its derived streams had
  to be rebuilt anyway.
- The first server start in a fresh container took 18 s; later ones 3 to 4 s.
- Memory: 640 MB 10 s into the feed, 701 MB after 4 minutes at 1,000 ticks a second, the result still
  right. Four minutes does not tell a leak from a heap still warming up.

### What it means

On every question the Proton homework asked, Deephaven gave the right answer without a plan around
it. It used less CPU than Proton's per-position join at every rate; Proton's leaf-cube plan used
less than Deephaven at x5 (0.73 cores against 1.30) at about three times the latency, and only
after the planning above. What a Deephaven target in legend-lite owns instead:

1. **Table operations, not SQL:** a plan of natural joins, `agg_by`, `update_view`, `where`, `sort` /
   `head`, `update_by`, `aj`, sent as `../deephaven-spike` sends them.
2. **Reload on restart:** the inputs from their sources of record, append-only history from a
   replayable log; 3 to 7 s at this size, more for history.
3. **A ticking client that works:** the browser's does; the Python one needs the bug above fixed or
   worked around.
4. **The license:** what `../deephaven-spike/README.md` and the review of 2026-10-21 cover. That is
   now the deciding question, not the engine.

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
# the Proton homework (Proton alone on the machine; -p 3218:3218 for proton_delivery.py)
$SP/venv/bin/python proton_correctness.py --target changelog --out results/x.json   # and --target versioned
$SP/venv/bin/python proton_asof.py --late-share 0.05 --out results/x.json
$SP/venv/bin/python proton_shapes.py --out results/x.json
$SP/venv/bin/python proton_perf.py --rate 5000 --out results/x.json                 # see the table for the variants
$SP/venv/bin/python proton_scale_check.py --scale 5 --target plain --out results/x.json
$SP/venv/bin/python proton_fx_late.py --scale 5 --shape leaf --all16 --out results/x.json
$SP/venv/bin/python proton_delivery.py --dialect ../../core/src/main/java/com/legend/sql/dialect/ClickHouse.java --out results/x.json
# the Deephaven homework (each starts and stops its own dh_server.py; nothing else on :10000)
$SP/venv/bin/python dh_correctness.py --out results/x.json
$SP/venv/bin/python dh_asof.py --late-share 0.05 --out results/x.json
$SP/venv/bin/python dh_shapes.py --out results/x.json
$SP/venv/bin/python dh_perf.py --scale 10 --heap 8g --out results/x.json            # see the table for the variants
$SP/venv/bin/python dh_ops.py recovery --kill hard --out results/x.json
# Proton operations: a second container with its data on disk, on other ports
docker run -d --name proton-disk -v $SP/proton-data:/var/lib/proton \
  -v $PWD/proton-ops.xml:/etc/proton-server/config.d/zz-ops.xml:ro -p 18463:8463 ghcr.io/timeplus-io/proton:latest
PROTON_PORT=18463 $SP/venv/bin/python proton_ops.py recovery --kill hard --container proton-disk --out results/x.json
PROTON_PORT=18463 $SP/venv/bin/python proton_ops.py log --seconds 240 --container proton-disk --out results/x.json
```

Feldera's image is 10.9 GB unpacked. The Proton container's data lives on a tmpfs because its disk
check reads this container's allowance as full.
