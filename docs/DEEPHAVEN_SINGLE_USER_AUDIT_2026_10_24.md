# The single-user experience on Deephaven: an audit (2026-10-24)

**Scope.** One person, a browser tab straight to a Deephaven server -- no server of ours in between: DataCube's own
app (one cube, `demo/deephaven/cube.ts`) and DataCube's own page of several grids and charts
(`demo/deephaven/board.ts`) over the demo's risk world, planned in the tab (the WebAssembly planner) and run on the
pinned Deephaven 42.7 (docs/DEEPHAVEN_PLAN_2026_10_22.md, A1). The tenet judged against is DH27: *a change may take
time; a tick may not.*

**Method.** Four parts, each with the evidence it left:

1. **A walk-through as a person does it** (`//datacube:deephaven_walkthrough`, manual, new): both pages driven by
   clicks, keys and the page's menu -- a chevron, a header sort, a pivot, a regroup, a filter, a calculated column, a
   change the server cannot run, Ctrl+Z, a right-click, Ctrl+C, the Properties window, a flat view, a server restart,
   a session the server forgot; a grid repivoted and regrouped on the page, Save and Open from its menu -- every step
   timed and screenshotted, every page error and console error collected.
2. **A long session on the server** (`//datacube:deephaven_session_bench`, manual, new): the storm world (100,000
   positions over 2,000 instruments, every instrument re-priced ten times a second), the server's CPU (its process
   tree, from /proc), its memory, the page's ticks and its JavaScript heap -- on the first view, after 59 distinct
   views, after a minute more of ticks, and on a reloaded page.
3. **A code review** of the path, read again whole: the session (`engine-client/src/deephaven.ts`), the engine and its
   readers (`deephaven-engine.ts`), the live reader (`deephaven-live.ts`), the rows (`deephaven-rows.ts`), the bases
   (`datacube/src/server-base.ts`), the tick paths in the app, the grid and the charts, the tree's fetch, and the two
   demo pages.
4. **What earlier measures said**, run again where the code moved: the network bench (N1), the page test (V2, B2,
   C2), the cube, flat and answers tests.

## Findings fixed

| # | Finding | Evidence | Fix |
|---|---|---|---|
| 1 | **A closed or reloaded tab left its whole session on the server** -- every followed table and up to 128 idle ones kept current -- until Deephaven expired it minutes later. | Session bench: a reloaded page's server at 0.6 cores, against 0.3 for the first view. | The session's close outlives the page (`keepalive`); the demos close the engine at `pagehide`; the engine ends the session first. After: 0.3 cores. |
| 2 | **A session the server forgot** (expired: a laptop asleep past Deephaven's five minutes) failed every gesture with "status 16", and nothing signed in again while the live side still looked connected. | Code review; the walk-through simulates it (the page's `fetch` answering UNAUTHENTICATED). | UNAUTHENTICATED on any call after sign-in ends the live side (`DeephavenSessionOptions.onForgotten`), so the page's reconnect runs: back in 1.3 s. |
| 3 | **Live rows the tab does not read** were thrown inside Deephaven's JS library's listener: the following stopped unsaid, and a view waiting on its first rows waited for ever. | Code review. | Said (`failed`, the reader's `unfollowed`), the waiting runs refused, the result let go. |
| 4 | A whole result whose making failed stayed as a following no one makes. | Code review. | Let go, so the next read follows it again. |
| 5 | A batch request with no answer (the network gone) left the exports it named unreleased. | Code review. | Released. |
| 6 | **A refused tick was silent**: a grid could stop moving and say nothing. | Code review. | Said in the status once, until a tick lands again (`CubeApp.tick`); never an alert over the grid. |
| 7 | The status bar's row count went stale when a tick brought or took rows. | Code review. | Drawn again when the count changes. |
| 8 | The selection's figures (sum, average, least, greatest) went stale under ticks. | A new test, which fails without the fix. | Drawn again at each tick while a selection is held. |
| 9 | **DataCube's windows took no focus when opened** (Properties, Filters ...), so Escape -- heard only inside a window -- did nothing until it was clicked. Every engine's. | The walk-through: Properties still open after Escape. | An opened window takes focus unless something in it already has; a new test. |
| 10 | The page of grids passed none of its grids' messages anywhere: #6's message would have reached nothing. | Code review. | Each grid's warning or error said in the page's header, with the grid's name. |
| 11 | The demos' header kept an error after the page had moved on (ten steps later it still said a refusal). | The walk-through. | A passing message gives way to the page's state after eight seconds. |
| 12 | A ticked change had no visual. Added, then corrected: a flat view sorted by a ticking value moves rows under the grid, so a cell flashed for another row (a position id "fell"). | The first recording (frame by frame). | A number that rose flashes green, one that fell red -- only where a row is the same row each tick (a group's; a flat view in the source's order); none under reduced motion. |
| 13 | After a restart, the page said "live" before its grids had run again, so a tick window counted the refresh's own sends. | The page test (B2). | Live once every grid ran again (both pages). |
| 14 | The reconnect count read 0 in the cube test's message. | -- | Not a bug: a snapshot read before the page refreshed its counts. The engine counts the sign-in (1). |
| 15 | My own regression: the timeline's measures (O1) broke the headless runner (no `performance.measure`). | `//datacube:headless_test` red on `a6e456b21`. | `engine-client/src/timeline.ts`: recorded only where the runtime keeps a timeline. |

## Numbers

**Gestures** (the walk-through, the demo's own world, a fresh server; first time each): opened in 2.6-2.8 s; a group
opened by its chevron 340-515 ms; a header sort 0.54-0.70 s and a pivot 1.06-1.59 s (each the first of its kind on the
server); a regroup 175-223 ms, a filter 141-176 ms, a calculated column 349-421 ms, Ctrl+Z 211-256 ms, a flat view
146-302 ms. On the page of grids, a grid repivoted or regrouped in 119-232 ms; Save 157-205 ms; Open 414-469 ms. A
server restart: back by itself in 8.7-9.1 s, the server's own start included; a forgotten session: 1.3 s. Warm (the
tables kept alive), a gesture waits on one round trip: 80 ms away, an expand 148 ms, a regroup 124 ms (N1).

**Ticks.** The storm on one cube: ten ticks a second, each on screen 6-9 ms after its rows; nothing sent while
ticking. The page of three grids and two charts: on screen 5-18 ms (p50, across runs), each chart's value axis held
through every draw (C2).

**A long session.** The session keeps the tables of the views it moved on from (up to 128 idle ones), and the server
keeps each current: after 59 distinct views (40 filters, 19 regroupings of all 100,000 rows) the server went from 0.3
to 0.5 cores and from 779 to 851 MB; the page's ticks did not change. The page's heap: 54 MB on the first view, 57
after the 59 views, 56 after a minute more (600 ticks) -- no growth.

## Recorded, not fixed

| # | What | Why it stays |
|---|---|---|
| R1 | A pivot waits on two round trips: its values, then the level that reads them. | One trip needs the level read long (one row per key and pivot value) and reshaped in the page, or the values kept followed past the next view: a design step, not a fix. |
| R2 | Over a 20-80 ms link a view is live 0.3-0.6 s after it lands. | Deephaven's JS library's own four sequential calls (the shared ticket, its schema, its preview columns, the subscription); ours are already cut to the minimum (N1). The view is shown meanwhile; its first rows tick it. |
| R3 | The idle cache costs the server about 0.2 cores at its cap in the storm, for one person. | Worth it for one person (a view gone back to is not made again). A shared server (S1) revisits it: a cap or an age per person. |
| R4 | Pivoted and grouped grids show empty dimension columns at the group levels. | DataCube's own behaviour on every engine; a host or the person hides them. |
| R5 | A chart's value axis writes billions with cents. | The charts' rule: every value written as the grid writes it (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §6). A compact axis is the DataCube line's call. |
| R6 | Deephaven's JS library logs about 20 console errors a page session (streams it closed itself; a server away during a restart). | Its own; nothing ours fails. Counted by the walk-through. |
| R7 | A grid gives no sign of its own while the server is away: the page's header says "reconnecting". | A product host puts it in each grid's status (`CubeAppOptions.hostStatus`); the demos have one header. |
| R8 | A gesture refused because the session was forgotten is not done again after the sign-in: the person repeats it. | Repeating it silently could apply a change twice to a different view; saying it and coming back is the honest half. |
| R9 | A decimal literal in a calculated column (Pure `1.5d`) is refused on Deephaven, by name; `1.5` works. | The dialect's named refusal (Deephaven's BigDecimal arithmetic not shaped yet). |
| R10 | The first pivot and first sort of a kind on a fresh server take 0.6-1.6 s. | The server's first compile and its JVM's warm-up; the next ones 120-300 ms. |
| R11 | The demos serve the test server's key to the page. | A demo's only. A real deployment signs in with the person's own credential: the next step. |
| R12 | The demo was run on Linux only. | Java and JavaScript throughout; macOS and Windows unconfirmed. |

## Tools this audit leaves

- `//datacube:deephaven_walkthrough` -- both pages, as a person drives them, every step timed and screenshotted.
- `//datacube:deephaven_session_bench` -- a long session's cost on the server and in the page.
- `//datacube:deephaven_record` -- the pages recorded ticking, as frames for a video.
- The demos' hooks a walk-through drives: `__cube.edit` (any change as grid commands), `__cube.change('calc:<lambda>')`,
  `__board.edit(grid, commands)`; the fixture's process id (`startDeephaven().pid`); the engine's `kept` count.
