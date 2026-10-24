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

**Next from `main`: PR #72** (DataCube BI and Excel parity), not merged on 2026-10-24. Its trial merge with this branch
conflicts in `app.ts`, `grid/grid.ts`, `test/app-dom.test.ts` and the budgets. The one that matters is `grid.ts`: this
branch moved a cell's painting into `DataGrid.#cell` so a tick repaints a cell in place, and #72 added conditional
formats (data bars, colour scales, icon sets, rules, a format code's colour) in the old inline painting. Their code moves
into `#cell`, and the conditionals (`measureConditionals`, measured over the grid's rows as heatmaps are) must be measured
again at a tick, or a ticking cell keeps a stale bar or colour; on a flat grid read by a window (V1) the rows are the
window's, as a heatmap's are. #72 also adds DataCube features the Deephaven target refuses today (below): correlation,
covariance and dates relative to today; its max-by and min-by (K3a) and Top/Bottom N's rank window (K6) are built.

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
bazel run //datacube:deephaven_demo -- --port=8080        # prints the Deephaven URL and the page's
DEMO_STORM=1 DEMO_POSITIONS=100000 DEMO_INSTRUMENTS=2000 bazel run //datacube:deephaven_demo -- --port=8080
```

Open the printed Deephaven `https://` address first and accept its certificate (the test server's own, made by
`keytool` at each start: `testing/.../EmbeddedDeephaven.java`; HTTPS because Deephaven's browser protocol needs HTTP/2),
then `http://127.0.0.1:8080/` (one cube) and `/board.html` (three grids, two charts). `restart` + Enter restarts
Deephaven. Verified on Linux only.

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
4. ~~Merge `main`~~: done 2026-10-24 (above); PR #72 next.

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
the store's declaration (DH16's check and DH19's refusal are L3s's: a wrong type surfaces late; a `NOT NULL` column that
holds nulls loses the guards the lowering writes); tables are found by their query-scope name only (a Deephaven
application's fields and a Python server's tables not tried); no check of the server's version at sign-in (42.7 only is
tested); and this branch carries the MongoDB line's commits, which are not on `main` either.

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
| DataCube | `datacube/src/server-base.ts` (bases), `runner.ts`, `app.ts` (tick, window), `grid/grid.ts` (`tickRows`, flash), `chart-render.ts` (held axes), `treeview.ts` |
| Demos | `datacube/demo/deephaven/` (`cube.ts`, `board.ts`, `risk.ts`, `risk.groovy`, `site.mjs`, `serve.mjs`) |
| Tests and tools | `datacube/test/deephaven/`, `datacube/bench/deephaven/`, `testing/.../EmbeddedDeephaven.java`, `tools/deephaven/` |
