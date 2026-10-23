# Deephaven ticks: how fast a change reaches the page

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
- `setViewport` -- the library's older viewport API (what the plain demo page uses).
- `fetch` -- the baseline it replaced: lite's own session's update stream said a result changed, and Arrow Flight's
  DoGet read all of it again (removed with the update stream, 2026-10-23; its numbers kept here).

"ask" is how often the transport asks the server for updates: at 10 ms it gets each cycle's change as soon as the
cycle has it; asked at the cycle itself it can wait up to a cycle more.

## Results (2026-10-23, the container's 4 cores, Deephaven 42.7, headless Chromium)

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
