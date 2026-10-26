# Timeseries tile review (2026-10-25)

The independent audit of branch `claude/timeseries-viewer-datacube-tsm0nb` (the DataCube timeseries tile, its page
commands and Python API, the Markets and timeseries-gallery examples, the browser guard behind a proxy: `1e91768d..`
`ad2a6e40`, 55 files, +8.6k lines), run before its PR (the owner, 2026-10-25: "did we do an independent audit first?
Especially to make sure we held to our tenets everything through platform and everything with API"). Four reviewers
read the diff independently, each against the repository's own rules rather than the author's account of them, and
each probed what it found -- on the real planner and DuckDB-WASM, the Python engine's runfiles, and Chromium:

| Reviewer | Question | Judged by |
|---|---|---|
| P | Is every value the database's, through the platform's queries -- the snapped and Python paths too? | AGENTS.md's execution tenet and invariant 4; TENET_CHARTER C1-C5; WORLD_MAP (compare, never compute); TENETS (lazy work); the owner's words for this feature |
| A | Is the tile one API: the screen and Python making the same page commands? | DATACUBE_ONE_API_PLAN_2026_10_21 (rules, L1-L12), its audit, the headless Python design; the ledgers |
| Q | Are the numbers right? | each function's documented formula (`timeseries-expr.ts` WORDS `about`, `differs`), gs-quant; hand values on adversarial inputs |
| G | Are the repository's written rules and guards kept? | AGENTS.md whole (guard files, invariant 4, common mistakes); every guard the diff moved |

A finding is listed as confirmed when a probe showed it (a reviewer's, and where marked re-run by the author) or the
code shows it directly; the author re-ran Q1, Q2 and Q5 on the real planner and DuckDB, and read A1, A2 and P2 in the
code. Probes are in the session scratchpad (`audit/{platform,api,queries,guards}`).

## 1. Confirmed: blocking

| # | Finding | Evidence | Planned fix |
|---|---|---|---|
| P1 / A4 | **Python's `Timeseries.frame()` reads a snapped grid live**, while every other Python read reads its copy: one page answers two sets of rows. Page filters are not applied; a grid over a class, saved query or warehouse table is refused (a raw `CubeRefusal`) though the tab plots it | two independent probes: after `px.snap()` and `px.update(...)`, `px.to_pandas()` the copy, `tile.frame()` the live rows; `headless/timeseries.ts:148-163` reads `cube.cube.source`, never `gridBaseOn` | a single-grid series opened over `gridBaseOn`'s base (copy, lens); a combination's grids each over their copy (see decision 1) |
| P2 | **A grid's rows changed under the same query: the tile keeps its old answer** -- in effect the results cache the owner ruled out (D4). Python serving a frame again, a remote file re-read | probe (legend 105.00 after the rows doubled; 210.00 only with `refresh(true)`); `ui/timeseries-view.ts:236-237` returns while its input key is unchanged; charts always ask again (`ui/chart-panel.ts:162`) | asked again whenever the grid's view lands, as a chart is |
| A1 | **Undo of a step that brings back a grid and a timeseries reading it builds a wrong page** (refused with an editor's tag, as every live page has; silently the wrong grid otherwise) | two probes; `page-between.ts:168` writes the simulation's ids into `addTimeseries`, renamed again by `applyOne` (`setTimeseries` at :173 is right) | `spec: v.spec`; an L2 gesture: a sheet of a grid and its timeseries deleted, undone, with a tag |
| A2 | **The tile's editing rules are screen code**: `firstTimeseries`, an alias renamed with every expression following, Read another grid, the remove-source guard, the series editor's composition, panes, views, annotations -- all in `ui/timeseries-view.ts`, which headless cannot import. Python edits the spec dict by hand (`plot`, `unplot`, `options`); the charts' equivalent is `chart-spec.ts editChart`, shared | read; probe: Python removes the first source while an unqualified series reads it -- accepted, the series silently re-pointed at another grid; `one-api-ledger.test.ts:504`'s PASS_THROUGH entry ("nothing to decide") is untrue | `timeseries-edit.ts` (no screen's): `editTimeseries(spec, change, columns)` and the first-tile rule, called by the viewer and, headless, by Python's methods; the ledger entries corrected |
| Q1 | **Dates are intersected before any function runs**: each operand of `a - b` (or a two-series function) is computed over the dates both have, not its own -- returns, volatility, means over another instrument's calendar | re-run: AAA daily, CCC every other day, `returns(close[AAA]) - returns(close[CCC])` -0.11 and 0 where each on its own dates then intersected (gs-quant) gives -0.20 and -0.191; `timeseries-query.ts:672-679` | each operand's subtree written over its own rows, joined on the period at the operator or the two-series function |
| Q2 | **Compare mode and the level-only statistics take every `same`-unit result for a level**: `change`, `diff`, `std`, `var`, `range`, spreads | re-run: under compare rebase (`firstPlot`'s default for a split price grid) `change(close)` is null throughout, `diff(close)` drawn 100, -300, 600; `diff`'s statistics a volatility of 2011%, max drawdown -400% | "is a level" tracked apart from the unit; compare and the level statistics for levels alone |

## 2. Confirmed: should fix

| # | Finding | Evidence | Planned fix |
|---|---|---|---|
| Q3 | No frequency ("As the rows are", the default): every figure annualized by 252 -- monthly levels' volatility 29.72 where 6.49 | probe | see decision 2 |
| Q4 | `100 * returns($A)` (the design note's own example) drawn a hundred times too large; `volatility(x)/100` written 0.20% | probe; `timeseries-expr.ts:380` | a ratio times 100 a percent, a percent over 100 a ratio, either times another number a number |
| Q5 | A split series whose base matches on the split or key column (`close / close[SPY]`, `beta(close, close[SPY])`, split by symbol) keeps only SPY's line, the others gone without a word | re-run | that base its own relation joined on the period (falls out of Q1's fix) |
| Q6 | A week or month is dated at its start but valued at its last row: a range or anchor drops the period holding its start (1Y at month grain an 11-month change; YTD loses January) | probe | the anchor and the statistics' window compared with each period's last observation |
| Q7 | Several rows per line per day: which is "the latest" is arbitrary, and said only for the first source with no split or match | probe | every base's rows counted, the drawing saying so |
| P3 | A combination never reads a snapped grid's copy (the comments say it does): a snapped warehouse grid refused, a snapped file read live beside the same tile's single-grid series read from the copy | read | see decision 1 |
| P4 | Failures shown as absence: a refused bounds query makes 1Y show everything, refused statistics blank every cell, with no word (invariant 4; C2.4) | probes | each refusal in the banner or on its line, as a series' is |
| P5 | `frame()` silently cut at 100,000 points (the drawing's limit), its sentinel row returned | probe | see decision 3 |
| A3 | Create ▸ Timeseries and Python's first tile are two rules (a date and no number: the screen refuses, Python makes a tile of no series) | probe | one function (A2) |
| A5 / G2 | Python is not refused what the viewer cannot draw (a text date, a text value, a missing column or series); `timeseriesMade` swaps a wrong-typed `date`, `key`, `alias`, `id` for the default; `periodOf` guesses `StrictDate` for an unknown type | probes | both headless entries run `timeseriesProblems`; wrong types refused by name; `periodOf` refuses |
| A6 | The screen/Python parity gaps are stated nowhere; the series editor rebuilds `split` from the first source's key, so editing a Python-made series rewrites it | read | closed by A2 (every edit one function, both sides); what stays one side's is a parity row |
| A7 | The L12 `both:` proofs never run the code they vouch for: `tabSources`, `openTogether` and Python's `together` client have no test | read | an end-to-end test of a combined series on a page of each host |
| G3 | A log pane draws zero and negative values by leaving them out (`[1, 10]`), with no word | probe (ECharts SVG in node) | refused in `timeseriesProblems` for a non-level on a log axis |
| G4 | A saved timeseries reading a cube the page does not hold is read without complaint (a chart on it is refused); after a reopen "Undo brings it back" is false | probe | see decision 4 |
| G5 | No test draws a timeseries with real ECharts (the renderer, `canvasChart`, the candlestick-registration order whose bug the candles commit fixed); no harness opens the two example pages, though the design note says they are drawn in Chromium | read; node render probe | a node render test (as `chart-echarts`); both examples in verify-charts' example loop |
| G6 | `proxyArgs`: a proxy URL with credentials or a trailing slash is refused by Chromium's flag (every host then direct); a NO_PROXY entry matches exactly, not as a suffix; `ALL_PROXY` loses to `HTTPS_PROXY` (Chromium's own reading: the reverse). The guard itself holds (probed); CI sets no proxy | probes | userinfo and slash stripped, `x` as `x;*.x`, Chromium's precedence; a unit test |

## 3. Notes (registered; fixed where small)

- P6: `frame()` pivots and sorts in pandas (no value computed; the database could order oldest first). P7: the viewer
  cuts a date-time range start to its day; a Decimal is read as a double for the legend, statistics and CSV. P8: split
  lines ordered by a numeric-aware compare on the client, the database's 20-line cut by its own string order. P9: a
  combined series rewrites its joint model on every query (series, statistics, each zoom).
- Q8: `prices` skips a return of -100% (shows 1 after a total loss; gs-quant 0); its `differs` wording is wrong. Q9: a
  candle series' statistics Low and High are its closes'; `candled` takes first/max/min of direct columns only. Q10:
  undeclared divergences from gs-quant: default windows, the ramp one point earlier, correlation and beta's returns after
  the intersect (Q1's fix changes the last). Q11: `pct` of a negative start, CAGR over two days, `drawdown(x, w)` never
  ramped, `std`/`var`/`corr` at w=0 hiding a point beside a null. Q12: the point limit cuts a split's lines unevenly.
- A8: the L2 gesture catalogue holds two timeseries gestures; none for frequency, compare, log, stats, hide, the
  series editor, settings, annotations, remove, move, undo. A9: `_HEADLESS_LEFT_OUT`'s entry and the tab's L12 proof
  line undated. A10: Python's own small rules (`unplot`'s existence check, `frame`'s first-series default, the tile
  placed near the first grid, `_timeseries_sources` misreading an alias named `grid`).
- G7: silent catches and guesses (`safeInline`, `?? []` on the code's own columns, `?? 'close'`, the editor's
  `'close'`, `returns`' `default:` arm doubling as simple, `readsAnchor`'s catch, `relations[i]!` unchecked). G9: the
  hidden-sheet test never checks the tile asks once shown; `tickText` writes "-0%"; a dangling doc comment in
  `chart-render.ts`; a tile on a sheet not shown exports no picture.

## 4. Checked and holding

Every figure is written in Pure through the pure-protocol builders and planned by legend-lite's planner -- no SQL, no
direct engine call, no gap filling or join on the client (`timeseries-query.ts`; a grep of the diff). A series of
several grids is one query, each grid grouped by its period and inner-joined in Pure, planned over one model as the
existing Join builds one (`join-open.ts`). On the tab a single-grid series runs through its grid's own runner, the snap
rewrite included. Range starts, anchors, statistics, the legend's change are the database's; the client only compares
the dates it was given. Every gesture reaches the one command path (`setTimeseries`, `addTimeseries`, the generic
tile commands), both command kinds are whole (union, readers, rebase, `pageBetween`, Python's generated builders), and
Python changes the page only through them; expressions are parsed by DataCube's parser alone. The budgets moved are
dated, named and needed; the generated files match their tools; every path that changes the sheet shown draws its
tiles; the guard holds behind a proxy and CI is unchanged.

## 5. Decisions for the owner (decided, the owner, 2026-10-25)

1. **A combination over a snapped grid (P1, P3): built now.** Each snapped grid's copy table declared in the one model
   a combination is planned over, on the tab and on Python's host; a grid in another store than the copies refused by
   name until the cross-store join.
2. **No frequency (Q3): inferred from the dates.** The database infers the periods a year from the series' own
   spacing (gs-quant's rule: daily 252, weekly 52, monthly 12, ...).
3. **`frame()` past the drawing's limit (P5): no limit for Python.** The window it asks for, all of it; the limit is
   the drawing's.
4. **A removed grid that a timeseries reads (G4): its cube kept.** The removed grid's cube stays in the page while a
   tile reads it, as a frozen chart's is kept, so the tile still draws after a reopen.

## 6. Fixed (2026-10-25)

Every confirmed finding of §1 and §2, fixed on the branch before the PR, each pinned by a test that failed on the code
before it. The notes of §3 fixed with them are listed too; the rest are follow-ups (§9).

| # | What changed | Commit | Pinned by |
|---|---|---|---|
| A1 | An undo bringing back a grid and a timeseries reading it renames the tile's spec once (the plan names `to`'s ids) | `a0d145a5` | `page-state.test.ts` (with and without an editor's tag) |
| P2 | A grid's view landing asks the tile again, always (no answer kept across it) | `a0d145a5` | `timeseries-view-dom.test.ts` |
| Q1, Q5 | Each operand computed on its own dates; operands joined on the period (and the line) where they meet | `b3f047f3` | `timeseries-queries.test.ts` "the review's findings on the numbers" |
| Q2, Q4 | A level apart from its unit (`levelOf`, `unitOf`): compare, log and level statistics on levels alone; a ratio times 100 a percent | `b3f047f3` | `timeseries-spec.test.ts`, `timeseries-queries.test.ts` |
| Q3 | No frequency: periods a year inferred from the dates' spacing, by the database (decision 2) | `b3f047f3` | `timeseries-queries.test.ts` |
| Q6, Q7 | A period dated by its last observation; tied rows counted for every column | `b3f047f3` | `timeseries-queries.test.ts` |
| Q8, Q9, Q11 | `prices` compounds every factor; a candle's extremes its lows' and highs'; % change over the start's size; drawdown ramped | `b3f047f3` | `timeseries-queries.test.ts` |
| G2, G3 | A date of no date type refused by name; a log pane straight where a line is no level, said | `b3f047f3` | `timeseries-queries.test.ts`, `timeseries-option.test.ts` |
| A2, A3, A6 | One rule for every change of a tile (`timeseries-edit.ts`), the viewer's and Python's; one first-tile rule; the editor keeps a split | `d0b0e6e0` | `timeseries-edit.test.ts`, `python/tests/test_page.py` |
| A5 / G2 | Python refused what the viewer cannot draw, in the viewer's words | `d0b0e6e0` | `python/tests/test_page.py` |
| A9, A10 | The left-out entry dated; Python's own small rules gone (an alias named `grid` read rightly) | `d0b0e6e0` | `python/tests/test_page.py` |
| P1 / A4 | Python's `frame()` of one grid asks the grid's own query (headless `gridTimeseries`: its copy, snapped; its lens) | `2f99dcae` | `python/tests/test_page.py` |
| P3 | A combination reads a snapped grid's copy, on the tab and on Python's host (held copies) | `2f99dcae` | `python/tests/test_page.py` (the tab's branch: by code, §9) |
| P5 | `frame()` has no point limit (decision 3) | `2f99dcae` | `python/tests/test_page.py` |
| G4 | A removed grid's cube kept while a tile reads it; the timeseries commands carry it (`kept`), so an undo brings the tile back on it | `6addbd2f` | `page-commands.test.ts`, `page-between.test.ts` (and its random walk), `cube-page-shell.test.ts` |
| A8 | Every change of a tile a gesture, held to the one-API checks; every change kind made by both sides, or listed | `a65b8078` | `one-api-gestures.test.ts`, `one-api-ledger.test.ts` |
| P4 | Refused dates, statistics and instruments said in the banner, on the row, on the picker | `ed060c4c` | `timeseries-view-dom.test.ts` "a refusal said where it lands" |
| G7 | No guessed column, no swallowed inline, each relation checked against its source | `ed060c4c` | `timeseries-view-dom.test.ts`, `join-open.test.ts` |
| G6 | The proxy read as Chromium reads it: no credentials or path, NO_PROXY's domains and the hosts under them, ALL_PROXY first | `63bde1a6` | `//tools/browser:proxy_args_test`, `//datacube:browser_guard_test` behind this container's proxy |
| G5 | Drawn by real ECharts in node; both example pages opened in Chromium, every tile drawn | `aa3ea2b2` | `timeseries-echarts.test.ts`, `verify-charts.mjs` |
| A7 | A combined series end to end on each host: the gallery's on the tab, one built with ll.Page on Python's | `3bf84dd4` | `verify-charts.mjs`, `python-engine.mjs`, cited as L12's proofs |
| G8 | The guard reviewer's budget note: the timeseries chunk's headroom kept near 2% (it was 5.7%), each move dated | `d0b0e6e0` | `bundle-budget.test.ts` |

## 7. The second audit (2026-10-26)

The fixes of §6 audited again by four reviewers, the same four questions, before the PR (the owner: "Need to do full
audit of all this new code too"). They found two blocking findings and thirteen to fix; main was merged (PR #73: one
design language for the page) between.

| # | Finding | Severity |
|---|---|---|
| Q-B1 | `levelOf` took a level plus or less a number for a level: `index(close) - 1` compared from a base of zero, every value none; its statistics nonsense | blocking |
| A-B1 | Two of the page's writers took its cubes from each view's one `cube`, which a timeseries has none of: a removed grid's kept cube written off the page, a file its reader refused | blocking |
| P-1 | A tile on a sheet not shown asked every query again on each view its grid landed (the P2 fix's regression) | should fix |
| P-2 | A split line of no frequency drawn empty unsaid; a level's volatility and Sharpe blank unsaid | should fix |
| P-3 | Python's `frame()` dropped what the drawing says (several rows a date, no frequency) | should fix |
| P-4 | A combined series reading a kept cube refused on a page Python serves | should fix |
| Q-S1 | A level at or below zero: compare and Sharpe signs turned, the statistics' % change the other way; off a log axis unsaid | should fix |
| Q-S2 | Split by a column not the instrument's, `close / close[SPY]` kept SPY's own group's line alone | should fix |
| Q-S3 | A range starting just after a period's last observation lost that period's move (YTD lost January, monthly) | should fix |
| Q-S4 | A combination's periods a year were its first part's, so a part of no frequency was hidden | should fix |
| Q-S5 | `prices` gave none at the first date (gs-quant's product gives `initial`); two tests asserted the wrong value | should fix |
| A-S1 | The screen kept changes Python refused, and Python refused a change for a problem the tile had already | should fix |
| A-S2 | Python's `Timeseries.spec` setter went past the one rule | should fix |
| A-S3, G-1 | `options()` dropped a slot it has not without a word | should fix |
| G-2 | = Q-S1's log axis | should fix |
| G-3 | An ECharts test's "its lines drawn" passed with no line drawn | should fix |
| G-4 | L12's proofs took `seen(` in every file, an assertion's message passing for a test | should fix |

## 8. Fixed after the second audit (2026-10-26)

| # | What changed | Commit | Pinned by |
|---|---|---|---|
| Q-B1 | A level plus or less a number, or turned negative, is no level | `ced3a738` | `timeseries-spec.test.ts`, `timeseries-queries.test.ts` |
| A-B1 | One rule for the cubes a page holds (`page-document.ts cubesRead`: every tile kind answers, no default; `cubesHeld`, `stillHeld`), every writer and command through it; a page written out refused without a cube a view reads | `50df3a2d`, `0a97264e` | `page-document.test.ts`, `cube-page-shell.test.ts`, `chart-tiles.test.ts` |
| Q-S1..S5 | The value as of a range's start; an operand of one instrument unsplit; `prices` from `initial`; a combination's periods a year none when any part's is; compare over its start's size, and no returns' figures at or below zero | `8a66d855` | `timeseries-queries.test.ts` (each failed before), the `prices` tests corrected |
| P-2, G-2 | A split line of no frequency said by name; a level at or below zero keeps its pane's axis straight, said; the statistics' blanks said | `8a66d855` | `timeseries-option.test.ts`, `timeseries-view-dom.test.ts` |
| A-S1 | One judge of a change for both sides (`timeseries-edit.ts judgedEdit`): a problem it makes refused, one the tile had not; a tile of no series either may leave | `dd6b08bd` | `timeseries-edit.test.ts`, `python/tests/test_page.py` |
| A-S2, A-S3 | `Timeseries.spec` read only; a slot it has not refused by name | `dd6b08bd` | `python/tests/test_page.py`, `timeseries-edit.test.ts` |
| P-3 | The rows' notes one rule (`timeseries-query.ts seriesNotes`), the drawing's and Python's frame's (headless `timeseriesNotes`, as warnings) | `dd6b08bd` | `python/tests/test_page.py` |
| P-4 | `joinTogether` reads the page's cubes, a kept one among them | `dd6b08bd` | `python/tests/test_page.py` |
| P-1 | A hidden tile marked stale, asked when its sheet is shown | `dd6b08bd` | `cube-page-shell.test.ts` (failed on the old code) |
| G-3, G-4 | The ECharts test counts its lines against a drawing of none; `seen(` only in the Python engine harness | `dd6b08bd` | the tests themselves |
| Notes | G9's "-0%"; N5's units (x0.01, /0.01, a variance a number); N1, N2 declared in the catalog (the ramp, beta's calendars); `readBy`'s catch typed; `#window`'s `?? ''` gone; "Undo brings it back" said no more; busy until the statistics answer; proxyArgs read as Chromium's GetVar (an empty lowercase variable is none) and refusing schemes the flag drops; N8's test compares figures; GATES.md lists proxy_args_test | `dd6b08bd` | `timeseries-option.test.ts`, `timeseries-spec.test.ts`, `proxy_args_test.mjs` |

With them, the owner's asks of 2026-10-26: a timeseries' states in a chart's words -- Dynamic, Frozen (each grid's
filter kept, read by the compiler for every query: `timeseries-spec.ts filterRead`), Detached -- on the screen and in
Python (`4f74006f`); and the tile in PR #73's design: its state in its header, a chart's right-click, its editors a
window beside it, both themes (`30aad6a6`).

## 8½. A third review (2026-10-26)

Everything after the second audit's B1 (`ced3a738..HEAD`: the kept cube saved, the cubes a page holds, the states,
the fixes of §8, the tile in PR #73's design) read again by one reviewer who wrote none of it. Nothing blocking; five to
fix, each fixed and pinned (each test failed before):

| # | Finding | Fixed | Pinned by |
|---|---|---|---|
| 1 | A change the judge refused still closed its editor, what was typed lost; in Settings the refused value stayed shown | `#set` says whether it kept the change; Settings painted again on a refusal | `timeseries-view-dom.test.ts` |
| 2 | Escape never closed the editor's window (the first field focused, Escape a field's) | the window itself focused, as a chart's options are | `timeseries-view-dom.test.ts` |
| 3 | The focus fell to the body when the window closed; the window had no name | given back to the tile's focusable frame; named by its title | by code (a chart's options' rule) |
| 4 | The judge compared problems by their text: a source renamed "made" every problem the tile had of it | the tile as it was renamed the same way before it is compared | `timeseries-edit.test.ts` |
| 5 | Compare "% change" took a price's volatility, drawdown, growth and Sharpe ratio of its change from its start (zero there), blanked as "at or below zero" | a level's figures of the level itself (`Built.levelValue`; its least `TS_STAT_LEVEL_LOW`) | `timeseries-queries.test.ts` |

Its notes fixed with them: the catalog's index, rebase, change and % change read "as of the first date shown"; a tile
whose every grid is gone may still change state, as Python's may (one rule); its right-click's Create reads the first
of its grids still on the board; the editor's selects sized as the toolbar's; the Frozen gesture asserts its button.

## 9. Follow-ups (not fixed here)

- Q (numbers): `candled` sets a period's aggregate on bare columns
  alone (`ohlc(close / 2, ...)` weekly draws flat candles, N4); `($A + $B) / 2` is no level (no compare, no level
  statistics, N6); a month dated at its last row though that row's value is none (N7); the point limit cuts a split's
  lines unevenly (Q12).
- P (paths): `frame()` pivots and sorts in pandas, no value computed (P6); a date-time range start cut to its day and
  a Decimal read as a double for the legend, statistics and CSV (P7); split lines ordered on the client while the
  database cuts them at 20 by its own order (P8); a combination's joint model written again on every query, and on
  every view landing since P2 (P9); the statistics asked with Stats and the legend both off; the tab's snapped-copy
  branch of a combination run by no test (its shared copy path is: `join-open.ts`).
- A (one API, suspected): a cube the screen keeps already is not replaced when a later `setTimeseries` carries its
  `kept` document or copy; another editor's undo bringing back a cube this tab never held is refused ("maker is
  gone"); a detached chart shares both. Python's `frame()` defaults to the first series by a rule of its own; a
  source told as `{'grid': g}` is read with no alias.
- A cube alone (not a page) saves no grid it added, so neither their charts nor a timeseries of them (a limitation of
  the cube alone's save, said in the A-B1 commit).
- G9's rest: a dangling doc comment in `chart-render.ts`; a tile on a sheet not shown is left out of the page's
  picture. The cube-page-shell G4 test reads a date of no date type, so its tile never draws (the drawing is the
  chart-tiles A-B1 test's).
- The screen refuses to freeze a tile one of whose grids it could not open (a page reopened without that source: no
  filter of it to keep, said), where Python freezes it from the page's own cube document.
- Not run in this container: `//datacube:verify_app_test` (its embedded Postgres refuses root; CI runs it).

## 10. Follow-ups fixed (2026-10-26)

- N3, CAGR over a short span: compound annual growth only over a calendar year or more, the database's date arithmetic
  deciding (`adjust(from, 1, YEARS) <= to`; a leap year's 366 days a year); under a year none, its % change its growth,
  and the statistics say so. Pinned: `timeseries-queries` N3 (a day short of a year, a year to the day, over a leap
  day, two years, compared), `timeseries-view-dom` (the note).
