# Deephaven: how fast a change, and a new view, reach the page

Two benchmarks, both manual: the tick benchmark (a change on the server to the page seeing it) and the first-rows
benchmark (a result the grid has not shown before, to its first rows in the page).

## Ticks

`bazel test //datacube:deephaven_tick_bench --test_output=streamed` (manual; `--test_env=BENCH_WORLDS=0,2` picks
worlds, `--test_env=BENCH_SECONDS=6` the window). The pinned Deephaven runs the benchmark's world (`tick.groovy`):
positions joined to books, instruments, a price feed and fx, valued (`mv = qty * px * rate`), and two results a grid
shows -- `agg`, market value by desk and sector (16 rows), and `flat`, every position. Every quote is stamped with the
server's clock in the update cycle that computes it, so **latency is from the cycle that made a change to the page
seeing it**: the server finishing that cycle's work (the joins, the grouping), sending the change, and the page reading
it. The wait for a cycle to start is the cycle's own setting, below.

The transports, each in a fresh page:

- `viewport` -- Deephaven's JS library (`dh-core.js`, loaded from the server), a viewport subscription: the rows a grid
  shows (16 or 50), every update read whole.
- `full` -- the library's full subscription: every row.
- `setViewport` -- the library's older viewport API (what the plain demo page used).
- `fetch` -- the baseline it replaced: lite's own session's update stream said a result changed, and Arrow Flight's
  DoGet read all of it again (removed with the update stream, 2026-10-23; its numbers kept here).

"ask" is how often the transport asks the server for updates: at 10 ms it gets each cycle's change as soon as the
cycle has it; asked at the cycle itself it can wait up to a cycle more.

### Results (2026-10-23, the container's 4 cores, Deephaven 42.7, headless Chromium)

Latency in ms; p50 / p95.

**Small, one quote a tick** (400 positions, 40 instruments, a 100 ms cycle):

| transport | result | ask | updates/s | latency | rows | page read |
|---|---|---|---|---|---|---|
| viewport | agg | 100 | 9.3 | 53 / 112 | 16 | 0.0 |
| viewport | agg | 10 | 9.8 | **10 / 15** | 16 | 0.0 |
| viewport | flat | 10 | 9.8 | 9 / 12 | 50 | 0.0 |
| full | agg | 10 | 9.8 | 9 / 12 | 16 | 0.0 |
| full | flat | 10 | 9.8 | 8 / 16 | 400 | 0.4 |
| setViewport | agg | 10 | 10.0 | 8 / 11 | 16 | 0.0 |
| fetch | agg | 10 | 10.0 | 22 / 28 | 16 | 10.3 |
| fetch | flat | 10 | 9.8 | 25 / 32 | 400 | 13.7 |

**Big, storm** (100,000 positions over 2,000 instruments, every instrument re-quoted every cycle: every position's
value changes every cycle):

| cycle | transport | result | ask | updates/s | latency | page read |
|---|---|---|---|---|---|---|
| 100 | viewport | agg | 100 | 9.4 | 65 / 89 | 0.0 |
| 100 | viewport | agg | 10 | 10.0 | **23 / 31** | 0.0 |
| 100 | viewport | flat (50) | 10 | 9.8 | 14 / 17 | 0.0 |
| 100 | full | agg | 10 | 10.0 | 25 / 28 | 0.0 |
| 100 | setViewport | agg | 10 | 10.0 | 25 / 33 | 0.0 |
| 100 | fetch | agg | 10 | 9.8 | 37 / 48 | 10.5 |
| 50 | viewport | agg | 50 | 18.4 | 56 / 72 | 0.0 |
| 50 | viewport | agg | 10 | 19.4 | **22 / 28** | 0.0 |
| 50 | viewport | flat (50) | 10 | 19.4 | 13 / 17 | 0.0 |
| 50 | fetch | agg | 10 | 19.6 | 35 / 44 | 9.2 |
| 20 | viewport | agg | 20 | 45.2 | 25 / 35 | 0.0 |
| 20 | viewport | agg | 10 | 46.0 | **20 / 26** | 0.0 |
| 20 | viewport | flat (50) | 10 | 46.4 | 13 / 18 | 0.0 |
| 20 | fetch | agg | 10 | 46.6 | 31 / 41 | 7.6 |

What it says:

1. **Ask at 10 ms.** Asked at the cycle, a change waits up to another cycle (53-65 ms at a 100 ms cycle); asked at 10
   ms, it arrives 10-23 ms after its cycle. The library's three APIs are within a few ms of each other.
2. **The library beats fetching** by about 15 ms and a 10 ms read: a viewport or subscription receives only the
   changed rows (Deephaven's Barrage), where the baseline asked for the whole result again after being told.
3. **The server's work is the floor.** In the storm, the 16-row grouping arrives 8-10 ms after the 50-row viewport of
   the positions: the cost of re-grouping 100,000 changed rows in that cycle. A faster cycle is more updates a second
   (46 at 20 ms), not a lower latency per change.
4. **Never subscribe a big result whole.** A full subscription of the 100,000 positions in the storm fell 15 s behind
   (2 updates a second, 193 ms to read each) and took the server down with it: a grid reads a viewport, or a
   result its query bounded (a level is capped at its row limit).

## First rows: a new view

`bazel test //datacube:deephaven_first_bench --test_output=streamed` (manual; `--test_env=BENCH_TRIALS=20`,
`--test_env=BENCH_CYCLES=100,20`). Over the risk demo's world (`demo/deephaven/risk.groovy`: 400 positions joined to
books, instruments, ticking prices and fx, valued), each trial plans two new queries -- new by a literal no other trial
has -- and reads one's first rows each way: **`doGet`**, Arrow Flight's DoGet of the session's export, as
`DeephavenEngine.execute` reads a view; **`subscribe`**, the result published and subscribed with Deephaven's JS
library, as `DeephavenEngine.follow` reads it after. Two results, a grid level (4 desks) and a flat page (400 rows);
`warm`, over the source the session keeps alive (a regroup's or a filter's case), and `cold`, every join made again.
Planning is outside the clock. Steps are p50s: `make` is the batch that makes the table, the rest each way's own.

Results (2026-10-23, the container's 4 cores, Deephaven 42.7, headless Chromium), ms, a 100 ms cycle (a 20 ms cycle
within a few ms of it):

| shape | source | how | sent | first p50 | first p95 | steps (p50) |
|---|---|---|---|---|---|---|
| level | warm | doGet | 7 | **60.7** | 239.8 | make 43.2, read 16.1 |
| level | warm | subscribe | 7 | 94.5 | 240.3 | make 40.2, publish 16.1, open 15.1, first 18.4 |
| level | cold | doGet | 13 | **84.4** | 281.7 | make 56.0, read 26.6 |
| level | cold | subscribe | 13 | 124.7 | 190.5 | make 55.7, publish 26.9, open 20.1, first 20.9 |
| flat | warm | doGet | 4 | **47.1** | 97.1 | make 30.6, read 14.8, type 1.0 |
| flat | warm | subscribe | 4 | 86.5 | 145.6 | make 29.6, publish 12.6, open 16.5, first 24.2 |
| flat | cold | doGet | 10 | **70.1** | 110.8 | make 45.3, read 26.3, type 1.0 |
| flat | cold | subscribe | 10 | 107.0 | 168.4 | make 33.5, publish 23.1, open 17.3, first 26.0 |

Before DH25 (the same day, the filter written as a formula, `(t0__pid < 1004L)`): every `make` was 247 to 270 ms and
every first p50 270 to 345 -- the server compiling the new formula to a Java class. A filter written as the server's
filter parser reads a match or a range (`t0__pid < 1004`) makes in 44 ms against 281 (measured side by side).

What it says:

1. **`DoGet` keeps the first rows**: 35 to 40 ms sooner than a subscription's first update -- a publish, the library
   opening the table and its first update are three steps where a read is one. The subscription follows once the view
   is painted.
2. **Making the table is most of a new view**, and a compile was most of that: a formula's text is compiled once a
   server (Deephaven keeps the classes by text), so a value in a formula is a compile for each new value. What still
   compiles a first time: a formula's first appearance on a server, and a pivot's conditional sums, whose formulas carry
   its values (DataCube's pivot gesture 1,784 ms on a fresh server, 473 ms once compiled; opening the tree's first group
   689 against 283).
3. The p95s are a trial's first appearance of a formula the source's queries share (compiled once, then kept).
