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
| **`claude/deephaven-3-browser`** | **everything since, on top of both: work here.** Head `285126324` (2026-10-24), pushed |

None is on `main`. On 2026-10-24 the browser branch was 22 commits behind `main` (other lines' DataCube PRs #65-#70); a
trial merge conflicted only in `datacube/test/bundle-budget.test.ts` (both sides raised budgets: keep both dated notes,
re-measure, set each budget to the merged measure). Merge `main` in (never rebase: the branch is pushed) before the next
landing.

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

**Checks** (the browser ones start the pinned server): `//datacube:deephaven_test` (answers worked out by hand),
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
4. **Merge `main`** (above).

## Next, as proposed to the user (2026-10-24; not yet chosen)

Settle 1-3, then **make it real for one desk**: the person's own sign-in, a real Deephaven with its real tables through
the model, counsel's review. Later, in the plan's order: the server-side sender (L3s); writes (W, Wr, X, L3w); the
modelled publisher (L5); views shared between viewers (S1, which revisits the idle cache's cost, audit R3).

## Where the code is

| Part | Files |
|---|---|
| Target | `core/src/main/java/com/legend/sql/dialect/Deephaven.java`, `DeephavenBatch.java` |
| Encoder | `core/src/main/java/com/legend/deephaven/` (`DeephavenWire`, `DeephavenMessages`, `ProtoWire`) |
| Tab's sender and engine | `engine-client/src/deephaven.ts` (session), `deephaven-engine.ts` (engine, readers, following), `deephaven-live.ts` (the JS library), `deephaven-rows.ts`, `timeline.ts` |
| DataCube | `datacube/src/server-base.ts` (bases), `runner.ts`, `app.ts` (tick, window), `grid/grid.ts` (`tickRows`, flash), `chart-render.ts` (held axes), `treeview.ts` |
| Demos | `datacube/demo/deephaven/` (`cube.ts`, `board.ts`, `risk.ts`, `risk.groovy`, `site.mjs`, `serve.mjs`) |
| Tests and tools | `datacube/test/deephaven/`, `datacube/bench/deephaven/`, `testing/.../EmbeddedDeephaven.java`, `tools/deephaven/` |
