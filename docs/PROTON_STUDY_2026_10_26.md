# How Proton does it, and what the tab's DuckDB could take from it (2026-10-26)

> The user, 2026-10-26: "Shouldn't we read proton code and approach now first to understand what we might be able to
> do". Read before choosing how to fix the live DuckDB's heavy tabs (docs/DUCKDB_LIVE_2026_10_25.md §2: Stock, News,
> Order flow and TCA re-run queries whose cost grows with the tape). Proton is not used, run or copied: its source is
> read for its approach, and every idea below is one we would write ourselves, in our own architecture.

**What was read.** `timeplus-io/proton` (Apache-2.0) at `7bcc214b`, its streaming parts: `src/Processors/Transforms/
Streaming/` (the stateful operators), `src/Interpreters/Streaming/` (emit, windows, joins, the aggregator),
`src/AggregateFunctions/Streaming/` (retractable aggregates), `src/Functions/Streaming/` (stateful row functions),
`src/Storages/Stream/` (the stream store, history and live), `src/Common/ExponentiallySmoothedCounter.h`, and the
maintainers' SQL references (`.claude/skills/sql-usage/references/`: stream types, emit policies, join patterns).

## 1. Proton's model, from its code

**A live query never runs again: it is a pipeline of stateful operators, each fed only the new rows.** Proton is
ClickHouse with a streaming layer. A streaming `SELECT` compiles once into a long-running pipeline (source → filter and
projection → join → aggregation → emit), and each operator keeps its state between chunks:

- **Aggregation** (`AggregatingTransform::consume`, `.cpp:169`): each arriving chunk is folded into the groups' states
  (`executeOnBlock`); when the emit policy fires, `finalize` converts the states to rows **without clearing them**
  (`GlobalAggregatingTransform::finalize`, `mergeAndConvertToChunks`), and only when new rows arrived since the last
  emit (`many_data->hasNewData()`).
- **Emit policies** decide when a result leaves (`EmitInterpreter`, `EmitParams`): `EMIT PERIODIC <interval>` (the
  default for a global aggregation, **every 2 s**; only if new events arrived; `REPEAT` to emit anyway), `EMIT ON
  UPDATE` (only the groups updated since the last emit: the keys a chunk touched are cached,
  `processed_keys_columns`, and only they are converted), `EMIT CHANGELOG` (each changed group's old row retracted,
  `_tp_delta = -1`, before its new one), `EMIT AFTER WINDOW CLOSE` (a window once the watermark passes its end).
- **Windows** (`tumble`, `hop`, `session`; `WindowAggregatingTransform`): state is kept in time buckets; a window is
  finalized once when the watermark passes its end, and its state then cleared (`clearExpiredState` at the finalized
  watermark): a closed window is never computed again.
- **History, then live** (`backfill`, `isHistoricalDataStart/End`): a query over a stream can first read its history
  (between two marks), folding it into the same states, then continue with live rows -- one state, built once.
- **Keyed data.** `versioned_kv` keeps the latest row a key (and `keep_versions` older ones for as-of joins);
  `changelog_kv` carries changes as rows with `_tp_delta` +1/-1, and an aggregation over them retracts the old value
  (`executeAndRetractOnBlock`; retractable `sum`, `count`, `avg`, `min`/`max` over a kept multiset in
  `AggregateFunctions/Streaming/`).
- **Joins.** A stream joined to a keyed table keeps the keyed side in a hash table (the latest a key, or the last N
  versions for `ASOF`), and probes each new row against it; a stream joined to a stream must be bounded
  (`date_diff_within(2m)`), its buffered rows kept in time buckets reclaimed as the watermark passes
  (`RangeAsofJoinContext`). The work is the new rows', never the history's.
- **Mergeable state.** ClickHouse's exponential moving average (kept in Proton) is a state of two numbers, a value and
  its reference time (`ExponentiallySmoothedAverage`): a new value is added by moving the state to the new reference
  time (`remap`: scale by the decay) and adding -- O(1) a row, and never overflowing, because the state is always
  expressed relative to the latest time. Two states merge the same way.
- **Every state bounded.** `lag` keeps at most the rows it looks back (`StreamingNeighbor`'s `ColumnsCache`,
  `max_prev_cache_rows`); `keep_versions`; range buckets reclaimed by watermark or count; TTLs on streams.
- **What it does not do.** SQL window functions (`OVER (...)`) are not streaming operators: they run over historical
  `table(...)` queries. A running per-row computation over a live stream is a stateful function whose output is a
  derived stream (`EMIT PER EVENT`, or a materialized view into a stream): each row's value computed once, when the row
  arrives, and kept -- never recomputed from the history.

## 2. The one difference that matters

**Proton never touches an old row twice; our live DuckDB re-runs every followed query over all of them at every emit.**
Every mechanism in §1 exists to avoid that, and it is exactly what the measurement found: the light tabs are cheap at
any tape size, and the heavy ones re-derive the whole tape each time -- worst of all the `ema`, whose DuckDB rendering
re-derives each print's whole history for every print (quadratic).

What we already share with Proton: keyed tables with upserts (`versioned_kv`); bounded streams (retention); a snapshot
per emit; telling a reader only when a result changed (its `hasNewData` and `ON UPDATE`).

What cannot carry over: Proton's operators are native code holding state in memory between chunks, many threads. The
tab's DuckDB-WASM is a batch SQL engine on one thread: no operator outlives its query. Any incremental scheme here keeps
its state **in tables**, updated each batch by **statements the planner writes** -- so the database still computes
every value (AGENTS.md's execution tenet: Java orchestrates, the database executes; no value computed in TypeScript).

## 3. Our heavy tabs, mapped

| Measured problem (100k prints) | Proton's way | On the tab's DuckDB |
|---|---|---|
| **Stock, News: the `ema`**, quadratic (4.4 s; 250k out of memory) | Mergeable per-key state; a per-row value computed once as the row arrives, kept as a derived stream | **(a)** keep each print's EMA in a table of its own, appended per batch from each name's last EMA (a derived stream: the planner's statements, the database's arithmetic), the query reading it; **(b)** make the batch query linear without state: a frame bounded where older prints' weight falls below a double's precision (~400 rows at α = 0.1; equal within rounding, a SEMANTICS_REGISTER row), or blocks of rows folded in a recursive CTE (exact, linear, more SQL) |
| **Order flow's time and sales, the prints grids**: the whole tape to the grid each refresh (1 s at 104k rows) | A non-aggregation query emits only the new rows as they arrive; the client appends them | a **tail** for an append-only stream sorted newest first: only the rows since the last emit sent, the grid prepending them; or a **viewport** (Deephaven's: the rows on screen, and a count) |
| **TCA's markouts**: every fill joined to later prices, each refresh (~0.2 s a tile) | A bounded range join, buckets reclaimed; a result final once its window passes | a fill's markouts are **final 30 s after it**: computed once into a table (as a closed window), the open ones only computed again |
| **Every tile at every 250 ms** | `EMIT PERIODIC` per query (default **2 s**), `ON UPDATE`, `WITH BATCH` | an **emit policy per followed result**: its period set by its cost (an expensive result run at most every few seconds), the cheap ones every cycle |
| **Candles, flow by minute** (cheap now, linear in the tape) | Tumble windows: closed windows final, only the open one live | only the open minute computed again; the closed ones kept (a planner rewrite: the window split at the current bucket) |
| **The grid redrawn from a whole result** | `ON UPDATE` / `CHANGELOG`: only changed groups, as retract and insert | a keyed diff of a result to the grid (later: the grid's own work, not the measured problem) |
| **The morning** | Backfill, then live, into one state | any kept state built from the morning once, then folded per batch |

## 4. The options, ranked

1. **Quick, no incremental state** -- an emit policy per result (by its cost), a tail or viewport for the flat grids,
   and a linear `ema` in the dialect (bounded frame, or blocks). Each a contained change; the `ema` fix also helps every
   non-live DuckDB grid. Likely enough for the desk as a demo; the heavy tabs tick every second or two, not every 250 ms.
2. **Incremental views, Proton's core idea, in our architecture** -- derived tables kept per batch by the planner's
   statements for the desk's costly shapes: a print's EMA, a fill's markouts, closed windows. Every tile live at the
   cycle's pace, at any tape size. But the planner must learn to read a query's costly part from a kept table (an
   incremental lowering of a query over a stream): real compiler work, and a design of its own.
3. **Both, in order** (recommended): 1 now, for the demo; 2 designed as the planner's live lowering, piloted on one
   shape (the `ema`, where its gain is largest and its state is two numbers a name).

What each would need decided: whether the dialect's `ema` may be equal within rounding rather than exactly (1's bounded
frame), or must stay exact (blocks, or 2's kept state); whether a tile may tick slower than the cycle (1's emit policy);
and whether the planner takes on an incremental lowering (2).
