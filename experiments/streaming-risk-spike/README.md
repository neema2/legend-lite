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
| tick → total, p50 / p95 / p99 | 63 / 116 / 130 ms | **43 / 66 / 83 ms** | 67 / 141 / 210 ms |
| FX tick → all 48 books, p50 / p95 | **88 / 140 ms** | 99 / 127 ms | 196 / 338 ms |
| engine CPU | **0.84 cores** | 1.97 cores | 2.56 cores |
| engine memory | 789 MB | 1,546 MB (container) | 1,033 MB (container) |
| setup: create or compile | 3.4 s | 21.9 s compile (169 s cold) | 0.1 s |
| new live grouping: first rows | 19 ms, live | 15 ms, **a snapshot**; live needs a recompile | 245 ms, live |
| correct (subscriber / snapshot) | yes / yes | yes / yes | yes / yes |

### About 4,400 distinct price updates a second, 30 s

(5,000 ticks a second scheduled; repeats on one instrument within a batch are one upsert.)

| | Deephaven | Feldera | Proton |
|---|---|---|---|
| tick → total, p50 / p95 / p99 | 68 / 118 / 125 ms | 86 / 128 / 148 ms | 71 / 130 / 209 ms |
| FX tick → all 48 books, p50 / p95 | 82 / 133 ms | 130 / 153 ms | 270 / 449 ms |
| engine CPU | 0.88 cores | 2.27 cores | 2.96 cores |
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
  levels. It does not fit DataCube's interactive regrouping as live queries: a regrouping would be
  a snapshot, or a recompile of seconds to minutes.
- **Proton fits interactive live queries** and our ClickHouse dialect, but its target would have
  to own a join planner for streaming (orders, shared materialized views, emit policy), and a
  user-shaped query can fall off a performance cliff the compiler did not foresee.
- **Deephaven fits both** (ad hoc and cheap) but has the license and patent questions.

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
```

Feldera's image is 10.9 GB unpacked. The Proton container's data lives on a tmpfs because its disk
check reads this container's allowance as full.
