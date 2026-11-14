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
| Q8, Q9, Q11 | `prices` compounds every factor; a candle's extremes its lows' and highs'; % change over the start's size (superseded 2026-10-28, the user's "match gsquant": gs-quant's x / x0 - 1); drawdown ramped | `b3f047f3` | `timeseries-queries.test.ts` |
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
| Q-S1..S5 | The value as of a range's start; an operand of one instrument unsplit; `prices` from `initial`; a combination's periods a year none when any part's is; compare over its start's size (superseded 2026-10-28: gs-quant's x / x0 - 1, the user's "match gsquant"), and no returns' figures at or below zero | `8a66d855` | `timeseries-queries.test.ts` (each failed before), the `prices` tests corrected |
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

- Q (numbers): a month dated at its last row though that row's value is none (N7); the point limit cuts a split's
  lines unevenly (Q12).
- P (paths): `frame()` pivots and sorts in pandas, no value computed (P6); a date-time range start cut to its day and
  a Decimal read as a double for the legend, statistics and CSV (P7); split lines ordered on the client while the
  database cuts them at 20 by its own order (P8); the tab's snapped-copy branch of a combination run by no test (its shared copy path is: `join-open.ts`).
- A (one API, suspected): Python's `frame()` defaults to the first series by a rule of its own; a source told as
  `{'grid': g}` is read with no alias.
- A cube alone (not a page) saves no grid it added, so neither their charts nor a timeseries of them (a limitation of
  the cube alone's save, said in the A-B1 commit).
- G9's rest: a dangling doc comment in `chart-render.ts`; a tile on a sheet not shown is left out of the page's
  picture. The cube-page-shell G4 test reads a date of no date type, so its tile never draws (the drawing is the
  chart-tiles A-B1 test's).
- Not run in this container: `//datacube:verify_app_test` (its embedded Postgres refuses root; CI runs it).

## 10. Follow-ups fixed (2026-10-26)

- N3, CAGR over a short span: compound annual growth only over a calendar year or more, the database's date arithmetic
  deciding (`adjust(from, 1, YEARS) <= to`; a leap year's 366 days a year); under a year none, its % change its growth,
  and the statistics say so. Pinned: `timeseries-queries` N3 (a day short of a year, a year to the day, over a leap
  day, two years, compared), `timeseries-view-dom` (the note).
- N4, candles of a computed series: at a frequency, a candle's four series are computed on their sources' own dates
  (an inner pass with no frequency: its functions, its arithmetic, its joins), and each period then takes the first
  open, the highest high, the lowest low and the last close of those dates, the database's (`candle`). Pinned:
  `timeseries-queries` N4 (a ratio's weekly candles, a 5-day mean's), the weekly-candles and Q9 tests unchanged.
- N6, `($A + $B) / 2` no level: did not reproduce -- written against the rule before the second audit's B1
  (`ced3a738`: a level plus a number alone), which already made a basket of levels a level. Pinned now by its query:
  `timeseries-queries` N6 (rebased from its start, its volatility).
- P9, a combination's joint model written again for every query: opened together once a round of asking (a fetch, the
  window moved: its series and their statistics), by the set of cubes, and put away when the round is done; a view
  landing still opens it once again (the tables may have moved: no model kept across rounds). Pinned:
  `timeseries-view-dom` P9 (two series and their statistics, one opening; asked again, one more).
- The statistics asked with Stats and the legend both off: none asked while nothing shows them, asked when one does.
  Pinned: `timeseries-view-dom` (none asked; Stats on, each line's once).
- A, a cube kept for a tile not reaching a screen already holding it: reproduced (another editor's `setTimeseries`
  replacing the `kept` document left the kept grid, and the page written here, as they were) -- the converge worked
  over the board's grids alone. It now holds, puts at their cubes there, follows the copies of, and waits on every grid
  of the page, one kept off the board for a tile too (`app/live-converge.ts`, `cubeThere`). Pinned: `page-app`
  ("another editor's setTimeseries replacing a cube kept here").
- A, another editor's change bringing a cube this tab never held: reproduced ("timeseries ... cannot be drawn again:
  grid ...'s maker is gone") -- the converge opened another editor's grids alone. It now opens every cube a view there
  reads that no grid shows and this tab does not know (a timeseries', a detached chart's: `cubesHeld`). Pinned:
  `page-app` ("another editor's timeseries over a cube this tab never held").
- A, the screen refusing to freeze a tile over a grid it could not open: reproduced ("its filter cannot be kept"),
  where Python froze it from the page's cube. One rule now: each source's filter as the page holds its cube -- its
  grid's, else as the page saved it (`TimeseriesHost.heldFilter`, `CubePageOptions.unopenedCube`). Pinned:
  `one-api-gestures` P9 (the screen's spec is Python's).

## 11. The expert functions' review (2026-10-26, E9)

An independent review of the expert functions (§5.3½ of the design note: calendar periods, risk statistics, the
regression, drawdowns, exponential averages, percentiles, conditions, correlations, the tables in Python), read with
probes of its own against DuckDB, pandas and gs-quant 2.2.1's source. Each fix pinned by a test that failed first.

- 1, alpha and residual of two samples: their means were each column's own while the covariance's were the pairs', so
  a second series with leading empties (a lag, a ramped mean) gave an intercept of two samples (`alpha(close[AAA],
  lag(close[BBB], 3), 8)`: its sign wrong). Every regression's means, variance and ramp are now of the window's pairs
  -- beta's variance too. Pinned: `timeseries-queries` ("a regression on a second series that starts later").
- 2, an exponential deviation at beta 0 refused by the database (its log of 0): refused in words by the reader
  (`SPREAD_BETA`, above 0). Pinned: `timeseries-spec`.
- 3 and 8, the Drawdowns and Correlations tabs asked of the lines shown, so a line shown again was left out (or "—"),
  and Python's tables read another rule: the tabs ask by `tableSeries`, hidden series included. Pinned:
  `timeseries-view-dom` ("a hidden line shown again").
- 4, a condition's shading a period short, a run of one date none: shaded to the line's next date after the run.
  Pinned: `timeseries-queries` (conditions), `timeseries-view-dom` (the drawing's areas).
- 5, a percentile of every value a self-join of every row to every other (4.5 s over 4,000 rows): one aggregate a
  line, joined back. Pinned: `timeseries-queries` (one join).
- 6, each exponential average joined the query to itself, doubling it with every nested one (1-4 of them: 32,969,
  86,469, 193,469, 407,469 characters): its block carried by windows, each adding the same steps. Pinned:
  `timeseries-queries` ("averages of averages"), the recursions over 700 days unchanged, a split and a weekly one.
- 7, Python's tables read every date where the screen's read the tile's range: one rule (`rangeWindow`, headless
  `timeseriesWindow` over each source's bounds). Pinned: `test_page` (a fixed range; the last month).
- 9, the Drawdowns tab said a level at or below zero "never fell": it says it is at or below zero. Pinned:
  `timeseries-view-dom`.
- 10, Python's statistics gave NaN where the screen says why: the screen's words as warnings (`statBlanks`, headless
  `timeseriesStatNotes`). Pinned: `test_page`.
- 11, a correlation's count of dates counted one side's alone: of the dates both have. Pinned: `timeseries-queries`.
- 12, an ask in flight answering after the round that asked again: marked stale and asked again (a timing window: no
  test).
- 13, the matrix's diagonal a written 1: the database's (none for a line that does not vary). Pinned:
  `timeseries-queries`, `test_page`.
- Nits: a smoothed average and an RSI of 1 drop the first value as gs-quant's ramp does; a calendar window's volatility
  and Sharpe ratio drawn from the first price's date that period on (gs-quant's), the pair functions' from the first
  date both have (a deliberate difference: the design note §5.3½); two series of one name each their id after it in
  Python's tables. Weak tests made strong: a regression's later-starting benchmark, an exponential average of a split
  and at a frequency.
- Fixed alongside (`75fc5395`): the legend's change in percent is a level's alone (a spread's start may be near zero).
- Not changed: `percentiles` from the start stays a self-join (a value's rank among those before it is no window
  aggregate).

## 12. The next phases' 1 review (2026-10-26)

An independent review of Phase 1 (the tile's `align`, `interpolate` and gs-quant's remaining functions), read with
probes of its own against pandas and gs-quant 2.2.1's source on an ingested table of three calendars. Each fix pinned
by a test that failed first, in `timeseries-queries` unless said.

- 1 (high), every align but intersect grew the query threefold a part: each pair read each side three times -- a spine
  of both and an as-of join of each, backward and forward (five parts under step: 544k characters, 4.5 s). A pair is
  now one FULL join on line and period (`merged`), each side read once, each part's values filled by its own windows
  over that one relation (`fill`); a one-line part beside a split's goes under the source's lines (`splitLines`, a leaf
  of the query), the lines the split has not dropped. Five parts under step: 130k. Found with it, two of the same
  kind: the most rows a period's parts took (`when(c > most, c, most)`, the review's Q7 note) wrote `most` twice --
  eight parts, the first part's count 130 times -- now the database's max of two (GREATEST); and a filled date no
  longer carries a part's row counts (it took none of its rows). Pinned: "a chain of parts under each align grows by
  the same steps each", every align, split or not; "three parts under step".
- 2, `append` and `prepend` kept the other series' dates as empty rows, drawn as gaps and read by every function after
  (`returns(append(...))` none where gs-quant has 0.25): gs-quant's dates alone -- x's to its last row and y's after
  (append), x's before y's first row and y's (prepend) -- by rows, not values. A filter of a window's figure, kept by
  the database's QUALIFY -- and a window after it now reads the rows it kept: the compiler had folded the next window
  beside the QUALIFY, which drops rows after the select's windows (`Fold.windowFolds`: a QUALIFY is a boundary, as
  legend-engine's nested filter is; `FoldTest`). Pinned: "union, append and prepend", a return of the spliced series.
- 3, `if` on the dates the tile's align combines: on the flags' dates (gs-quant's `if_` has no method), the branch
  taken none where it has none. Pinned: "if: on the flags' dates".
- 4, a decomposition under two whole cycles gave figures (gs-quant refuses): none. Its places in the cycle were row
  numbers, so a missing month moved every month after it: the calendar's at months, quarters and weeks. Its daily
  cycle counts business days; `differs` now says so, and that gs-quant fills a daily series to calendar days (7, 30,
  91, 365). Pinned: "a decomposition: none under two whole cycles; each place ... a month missing".
- 5, `interpolate`'s default was step (gs-quant's is intersect), and its step carried x's last observation where
  gs-quant's walks the dates asked: from x's value as of the first one, each carried to the next -- an x date between
  two asked never read. Both as gs-quant's. A filter of a plain column folds under its select's windows (the engine's
  relational rule, `Fold.filterSlot`, PCT `testExtendFilterOutNull`), so y's dates are kept by one reading a window
  (QUALIFY), after the windows that read every date. Pinned: "interpolate: as gs-quant's", y begun on a date x lacks.
- 6, small edges as gs-quant's: `smooth_spikes` keeps a value beside none (a comparison with NaN never holds), its
  first and last rows alone dropped; `compare` beside none is 0; `filter(..., not_equals, v)` drops the dates of none
  too (NaN != v); `first`, `last` and the date parts on every date, those of none too. Pinned in `timeseries-queries`.
- 7, `vol_swap_volatility` said gs-quant's ramp starts one observation later: its `Window(n, n - 1)` starts where this
  one does -- the text taken off. Pinned: `timeseries-spec`.
- 8, a weighted sum whose weights sum to a float near 0 (0.1 + 0.2 - 0.3) was taken; one of exactly 0 was refused as
  not "in pairs": both refused, saying the weights sum to 0. A number where a series is named by its place (its third
  argument, not "second"). The weights' sum and smooth_spikes' 1 + threshold are the database's. Pinned:
  `timeseries-spec`, `timeseries-queries`.
- 9, intersect told as none by the viewer and as 'intersect' by Python -- two spellings, and a query asked again: the
  edit rule keeps it as none whoever tells it; Python's `options` names `align`. Pinned: `timeseries-edit`.
- Found by the review's probes and kept: a plain filter after a window folds under it -- the engine's own semantics,
  pinned by `FoldTest`, so the query writer reads a window where it filters after windows.

## 13. The next phases' 2 review (2026-10-26)

An independent review of Phase 2 (swap, the Data tab, pictures, a pane's axis by hand, the Query window, sliders), read
with probes of its own: the real viewer, the edit rule and the drawing in jsdom, a picture made by ECharts' SVG painter.
Each fix pinned by a test that failed first, in `timeseries-view-dom` unless said.

- 1 (high), a slider's stop outlived Cancel, Escape and Remove: a drag then Cancel told the dragged value 400 ms later,
  one paused past 400 ms was kept whatever Cancel said, a drag then Remove said "there is no series B", and each pause
  was its own step to undo. A drag is now a try (the viewer's `#tried`): judged by the tile's one rule, drawn, never told;
  OK tells it once; Cancel, Escape, the window's close, Remove or another editor take it back (the editor's
  `#leaving`); a change told meanwhile (Log, a range) is made to the tile as the page has it, never with the try.
  Pinned: "a parameter dragged ... kept by OK alone", "... then Cancel, Escape or Remove".
- 2, a pane's ends by hand were dropped without a word: a bounded line's axis (an RSI's 0 to 100) drew its bounds over
  them, a pane of right-axis lines set them on its empty left axis, and a log pane clamped a least at or below 0. The
  ends now win over the bounds and land on the axis the pane's lines are read on; the reader refuses a log scale's end
  at or below 0. Pinned: `timeseries-option` "a pane's ends by hand", `timeseries-spec`.
- 3, Python's `table()` was not the Data tab: every date, an index's base the first date ever, hidden series and
  conditions among its columns, named by ids; `query()` and `frame()` read no base where the screen reads the first
  date shown. `table()` reads the tile's range (`_window`), skips what the tab skips, and names each column by the
  screen's rule (headless `timeseriesLineNames`, `splitName`); `frame()` and `query()` read a tile as the screen draws
  it (headless `anchor`: every date, the base the first date shown). Pinned: `test_page.py`.
- 4, a picture named no line (the screen's legend is the tile's chips) and drew the navigator: a picture now carries its
  lines' legend under its heading and no navigator. Pinned: `timeseries-option` "a picture", the picture test here,
  and `timeseries-echarts` (11).
- 5, swap's edges: a series reading the instrument through `$A` kept its old name; a `$` in the new name was read as a
  replacement's pattern; a number from Python failed as no refusal, and spaces round a name matched nothing; the S of
  "S&P" was a word. A series reading it through a reference is renamed; the new name is written as told; names are
  words (Python's `swap` sends them as text, the edit refuses anything else), trimmed; a `&`, or an inner `.` or `-`,
  joins a word. Pinned: `timeseries-edit` "a swap's edges", `test_page.py` (an instrument column of numbers).
- 6, the Swap window's list, painted again before the database answered the first, took both answers (the instrument
  read among them, others twice): each paint's answer its own. Pinned: "Swap's list painted again".
- 7, the Data tab kept one of a line's two values on a date (Copy lost what the chart drew) and split one day written
  as a date and as a midnight date-time into two rows: two values on a date refused in words, the day one row.
  Pinned: `timeseries-option`.
- 8, Settings' Least and Most took a number the box could not read ("5,000") for none, taking the end off; 1e400 did
  nothing, the box still showing it: both said, the end kept, the box as the tile has it. Pinned: "Settings: ...".
- 9, a number of no bound had its slider from 0: a filter's -0.02 could go no lower: either side of 0 now. Pinned: "a
  number of no least or most".
- 10, a picture was taller than its panes needed by the screen's heading, counted twice; an empty title kept a blank
  line; a note told with a line break ran into the panes: `tsPictureHeight`, a title of nothing none, each heading one
  line (`headingLine`). `pngUrl`, unused, taken off. Pinned: `timeseries-option` "a picture", the picture test here.
- 11, tests the review found missing: the real export through ECharts' SVG painter (`timeseries-echarts`: the
  picture's heading and its lines' names in the SVG), the numeric swap, Cancel and Remove after a drag, ends on a
  bounded pane -- each above.

## 14. The next phases' 3 review (2026-10-26)

An independent review of Phase 3 (a tile's views and the Calendar tab), read with probes of its own: the commit's
queries planned by the WASM planner over DuckDB-WASM on a sample of gaps (a month missing, a constant, a negative
series, a tenor grid), each against numpy and pandas, and the real viewer in jsdom over a database slowed so asks
overlap. The scatter, histogram, seasonality and curve values matched their references; the findings were in what a
figure is labelled, what is dropped without a word, and the viewer's state. Each fix pinned by a test that failed first,
in `timeseries-queries` unless said.

- 1 (high), the calendar's month before was the last month present: over a missing month, two months' return under
  one; a quarterly or yearly tile's quarters and years written under months. The month before is now the one numbered
  one less (none after a gap, as pandas' resample), the year before likewise; a quarterly or yearly tile's calendar is
  refused. Pinned: "a calendar over missing months".
- 2, a daily seasonality placed each date by its day of the year: each place held the years that traded that day (3.4
  of 5 on the probe), a leap year a day off. A daily series (or one of no frequency) is now by the week of the year,
  each year its week's last value: every year before at every place. Pinned: "a daily seasonality".
- 3, a curve drew a line's value of months before as of the date asked, and a date no line had a value by as an empty
  line, neither said: each point its own day (hollow where earlier, in its tooltip), said in the banner (`curveNotes`);
  Python's `attrs['line_dates']`. Pinned: `timeseries-option` "a curve's points of earlier days".
- 4, one `of` serves every mode, and a mode took one it could not draw (a scatter's x for a curve: "series B is one
  line -- split it" on a tile with a split series): `viewSeries` reads `of` where it fits the mode, else the tile's
  first; one told to a scatter is its x. Settings offers what a view can draw (`viewChoices`: no candles). Pinned: "the
  series each view draws".
- 5, the chart's aria-busy: an ask superseded left it set, another's end cleared it while one still asked -- now a count
  of the rounds asking (`#busyOn`), each released when done. Pinned: `timeseries-view-dom` "busy while it asks".
- 6, a histogram listed the bins a value fell in alone, so the normal was drawn across empty ones and Python's frame
  was not numpy's: every bin, an empty one 0. Pinned: "a histogram", 40 bins against numpy's.
- 7, the tile's compare rebased each tenor of a curve from its own first value: a curve's lines as they are. Pinned:
  "a curve".
- 8, one key no tenor put every key by its text (1Y before 3M): the tenors by their span, ON, TN and SN among them,
  then the rest. A split's first 20 lines are by their text (every split series, before this phase): a curve of 20
  says it may have lost one. Pinned: "a curve's keys in order".
- 9, the Calendar tab: a level at or below zero gave none in every cell, its footnote saying "the first of each"; a
  column the database answered without read as no months. Now each said: the line's least level answered with it
  (`TS_CAL_LOW`), a missing column refused by name. Pinned: "a calendar of returns".
- 10, a curve's day no calendar has (2024-02-30) passed the reader and was refused by the database; a day asked of a
  date-time source left that day's own observations out. The reader takes calendar days alone (its every date: the
  range's, an event's), and a date asked twice is refused; a day asked is the whole of it. Pinned: `timeseries-spec`.
- 11, Python: a scatter of a series against itself one column (now `(x)` and `(y)`); `calendar()`'s first level a
  hidden one (now the first shown, as the tab's). Kept as they are: a month with no row and a month of no return both
  None in Python (the tab writes '' and '—'), and the toolbar's CSV the time view's lines in a view mode (Python's
  views each a table) -- follow-ups.
- 12, the viewer: the range buttons of a seasonality and a curve pressed to no effect (now off, saying why); the Query
  window showed the mode before's SQL while the new answer was asked (now the answer of what is asked alone); a
  scatter's first pick lost where the tile had no pair yet (now kept till the pair is whole). Pinned:
  `timeseries-view-dom`.


## 15. The next phases' 4 review (2026-10-26)

An independent review of Phase 4 (an analyst's packs), read against TA-Lib, StockCharts, Morningstar, gs-quant's
`backtesting.basket_series` and statsmodels' `adfuller`, with a probe of its own on the pack's sample at a weekly
frequency. ATR, %K, Donchian, VWAP, tracking error, the information ratio, lead_lag, total_return, the basket's
rebalance dates and the Dickey-Fuller algebra matched at the source's own frequency; the findings were at a frequency,
in what the words claimed, and in what was refused. Each fix pinned by a test that failed first, in
`timeseries-queries` unless said.

- 1 (blocking), at a frequency (or of several rows a date) the technicals read each period's last value: a weekly ATR
  of each week's last high and low (4.1033 where its bars give 4.6933), an OBV of a week's last day's volume (1259
  where the week traded 5925), a total return of a period's last dividend. Each series a function reads is now taken by
  its role (`TsSignature.reads`): a high its most, a low its least, a close or a price its last, a volume and a dividend
  their total -- of a bare column its period's aggregate (`candled`), and under a frequency each read on its own dates
  and rolled up to the period (`rolled`, the candle's way, which the candle now uses); a VWAP's price times its volume
  on its own dates, totalled. Pinned: "at a weekly frequency, each week a bar".
- 2, the words called `dickey_fuller(residual(y, x, 0))` Engle and Granger's cointegration test: a running residual
  is each date's own fit, not one fit of the whole, so its −3.34 does not apply. The words now say so, and test a spread
  written with its ratio as it is.
- 3, a long and a short (`basket(x, 1, y, -1)`) was refused as weights summing to 0: a weighted sum's check, which a
  basket (dividing by nothing, as gs-quant's) does not need. It is the weighted sum's alone. Pinned: "basket: a long
  and a short", and the parser's own.
- 4, Dickey-Fuller and the half-life said "the last w observations" over w differences (w + 1 values), and −2.86 at 5%
  where 30 differences give about −2.97: both said as they are.
- 5 and 6, capture is of arithmetic means (Morningstar's compounds each) and OBV starts at 0 (StockCharts'; TA-Lib's at
  the first volume): said in the words, and the test's header no longer names Morningstar's or TA-Lib's for them.
- 7, Keltner's band was drawn from the ATR's first (11 closes into an average of 20): now once its average holds w.
- 8, against invariant 4: a Dickey-Fuller of 1 or 2 differences drew nothing and said nothing (refused now); the
  basket read any word but daily and weekly as monthly (it throws); a price of none mid-basket was taken as flat (now
  none from there on, as gs-quant's NaN carries). Pinned: "dickey_fuller over 1 or 2 differences refused", "basket:
  ... none from a price of none on".
- 9, ATR's words described a first true range that enters no value: they say TA-Lib's seed.

Not changed: the tests' gaps the review named beyond these (a capture's benchmark exactly 0, stochastic's flat window,
a date-time source's weekly basket and yield) are boundaries the queries state in words; they are left as they are.

## 16. §9's follow-ups, closed (2026-10-27)

Each of §9's rows, fixed and pinned by a test that failed first, or decided and said why.

- N7, a period dated at its last row though that row's value was none: a column's periods are of its rows with a
  value (`columnPart`), so a month whose last row has none is dated at the day its value is of. Pinned:
  `timeseries-queries` ("a period of a column dated at its last row with a value").
- Q12, the point limit cut a split's lines unevenly: a cut keeps whole dates -- each date's rows while the rows of the
  dates after it are at most the limit, the date it crosses at kept too -- the database's (`seriesQuery`), and the
  drawing leaves that date out, every line ending at the same date (`tsLines`). Pinned: `timeseries-queries` ("a series
  cut at its most points keeps whole dates"), `timeseries-option` ("a split series over the point limit").
- A, Python's `frame()` chose its first series by a rule of its own: none asked, DataCube's headless `seriesOf` takes
  the tile's first and its answer names it (`series`); Python asks no series and reads the name back. Pinned:
  `test_page` (`tile.frame()` is `tile.frame('A')`).
- A, a source told as `{'grid': g}` read with no alias: checked, holding -- its alias is DataCube's (headless
  `timeseriesMade`'s `aliasFor`, the rule the screen's Create ▸ Timeseries names a source by, `timeseries-edit.ts`).
- G9's rest: the dangling doc comment in `chart-render.ts` is `ChartPicture`'s again; the cube-page-shell G4 test
  holds that the tile says the same over the kept cube as over its grid (it had read only that it did not say "not on
  the page"); its date stays a column of no date type, as the stub planner draws nothing (the drawing is the chart-tiles
  A-B1 test's).
- Decided, kept:
  - P6, `frame()` pivots and sorts in pandas: the shape of rows the database computed, no figure.
  - P7, a date-time range start cut to its day: the range buttons name days (1M, YTD); a Decimal read as a double for
    the legend, the statistics and the CSV: what JavaScript holds, its digits beyond a double's said by no figure
    (the database computes each, in its own type).
  - P8, a split's lines kept at 20 by the database's own text order (deterministic, said in the views' note: "The
    first 20 lines by their text") and drawn in a person's reading order (2Y before 10Y): which lines is the database's,
    their order on screen the drawing's.
  - A cube alone (not a page) saves no grid it added: the cube alone's save, said in A-B1's commit.
  - A tile on a sheet not shown exports no picture: an export draws what its sheet shows, as a chart on it does.
- Registered, not built: the tab's snapped-copy branch of a combination (`tabSources`, a grid's copy read as
  `{_type: 'copy'}`) is run only by the browser harness without a snapped grid; its shared path (`join-open.ts`) is
  tested. A step in `demo/verify-charts.mjs` that snaps a grid of a combination would run it.
- `//datacube:verify_app_test`: run as an unprivileged user (its embedded Postgres refuses root), green.
- The planning cost of the library (the roadmap's C), measured: `//python:engine_test`'s 320 tests ran in 310 s in a
  cloud container (unittest's durations), the timeseries tests about 16 s of it and the keeper's convergence test the
  longest (36 s); main's CI ran the lane's test in about 137 s (run 40502527150), under its 300 s. Sending a query
  only the functions it calls would compile a model again for each set where today it compiles once a model: not done.

## 17. Phase 6's review (2026-10-27)

An independent review of Phase 6 (the charts from the trading model's classes, `61aa0a64^..0fed599e`: the markets
project, the class example, the ported held-model answer, marks from a grid's rows, Python over the example), each
finding confirmed by a scratch test over the commits' own tree. Each fixed with a test that failed first:

- **1 (blocking) marks added once a tile was drawn were never asked; an annotation before them taken off broke every
  drawing after.** The fetch's key left the annotations out, and the answers were kept by their place. The key holds
  the marks now, each answer kept by what the marks are (`marksKey`): `timeseries-view-dom.test.ts` adds marks to a drawn
  tile, takes a level before them off, then the marks.
- **2 marks at a value on the pane's left axis whatever their line was drawn on, and among lines compare rebased.** On
  the axis of their pane's line of their source (else its first line's); under compare left out and said
  (`marksUnplaced`: "... are drawn at their price, the lines from one start (Compare): left out."): `timeseries-option.test.ts`.
- **3 a typer's hold outlived it on Python's host.** `dispose` ended its own worker before the release was sent, and a
  host's worker keeps its models in the host: each model it holds -- its own, its library planner's -- is let go first
  now, then the worker ended (`lite-planner.ts #letGoAll`): `lite-planner.test.ts` sees `release` before `terminate`.
  Its smaller note, a model that does not compile compiled twice before it is refused (the hold, then the request's
  own words), is kept: the refusal's words are the request's.
- **4 a source renamed refused its marks.** Marks follow their source renamed; and Swap moves their match on the
  instrument column with the series (the review's nit: the other instrument's close with the first's trades on it),
  their label saying it as a name does: `timeseries-edit.test.ts`.
- Nits fixed: the example's trades tile by day (its quotes' times at the close, its trades' days, on one axis); marks
  past the cut chosen in an order of every column answered; a mark's value written as its grid writes the column it is
  drawn at; a picture's legend names the marks drawn, each by its own shape.
- Kept: marks are asked while a view (a scatter, a histogram) is shown -- one query of a grid's rows, drawn again when
  the view goes, as the bounds are.

## 18. The Pure conversion's review (2026-10-28)

An independent review, read-only, of the focused Pure audit's conversion (`9c46f514`, `b6cd856b`, `09ac1c6a`: % change as
gs-quant's, a chain of generic calls typed once each, the tile's tables and views as `datacube::ts` functions), with
its own scripts (the expected shortfall's rank rule against `quantile_cont` over 200,000 random draws, ties among them:
none differed). Its findings, each fixed and pinned:

- **1 (blocking) the core fix broke the static fold's `.columns`.** A relation argument rode the body's fold as a hole
  the fold could not type, so `$tds.columns->map(..)` no longer unrolled: twelve DuckDB corpus tests failed
  (`rowValueDifference`, the `columnValueDifference` five, `zScore`, `iqrClassify`, the digests), the host pass exiting
  1. The fold keeps a typed argument as it is, its `.columns` read from its typing (`Overloads.inlineNormalized`,
  `StaticFold`, `Typer.pretyped`; first by typing the holes, then -- the checks lane's protocol-node ledger, below --
  with no hole at all): `//spec:corpus_duckdb` and `//spec:corpus_h2` green again, their rosters unchanged.
- **2 a correlation's guard reached one form.** The rolling `correlation` gave none where a side does not vary (DuckDB
  says NaN, Postgres none), its calendar form, `leadLag` and theirs NaN still: every correlation of the library now
  none so (`pairPeriod`, `leadLag`; design note §5.6). `timeseries-queries.test.ts` "a correlation where a side does not
  vary is none, in every form" failed on the library before it (`correlation(..., 1w)`: NaN).
- **3 `Typer.withPretyped` was not re-entrant**: a call inlined in another's body, handed the outer's table, took the
  outer's typing of it away when it ended. It restores what it replaced.
- **4 the holes' counter was static mutable state** (`ArchitectureTest.staticFieldsAreFinal`'s limit): the holes are
  gone (1). **5** the parameter-argument default is a check that throws (a typer bug, AGENTS invariant 4); the
  expansion depth's javadoc says the inner call is typed before the outer expands; the chain test is twenty calls,
  past the sixteen nested expansions the old typing refused.
- **6** `performanceStatistics`' count of returns shares its mean's window: three windows, as the note says.
- **7** checked: the scatter's and the projection's `regression` cost what the writer's did (scatter 30 ms against 36,
  projection 32 against 17, its SQL 7 KB against 4).
- **8 tests**: a split series' every figure of each line its own tile's (the figures joined to the line's own); the
  expected shortfall where (n - 1)q is whole (the quantile a value) and where returns tie at it.

Two checks of this branch that had gone unrun failed in the same round and are fixed: `guardrails_test` (a date-time read
by its type's name where `hasTimeOfDay` reads it; the markets example's saved queries' text built from their columns,
now written out as Query saved it, an allowance of the guard's with its date and its count pinned) and the first tile's
bundle budget (97,000 -> 100,000, measured 98,043, its note dated).

The pushed head's `checks` lane (`//core:guardrails`, never run locally before the push) then failed on three of this
branch's own rows, each fixed: `ColumnHygiene.fresh` a mutable field (allowlisted, dated: `AlphaRename.nrFresh`'s
lifecycle); two string compares of a raw type in `ColumnHygiene` (through `Overloads.genericRawIs`, the package's one
reader); and protocol nodes built below the parser -- the holes (gone), hygiene's renamed column specs and `read`'s
property read (two dated ledger rows: an expansion's columns are known only at each call, S112, S114).


## 19. The second pass: `read` typed directly, the last sums the library's (2026-10-28)

The user, the same day: "Fix this and all the typescript math we found that should be in pure instead so that these
functions are useable from platform without TS". Three commits:

- **`read` (6a4ea973).** `ReadChecker` built the syntax `$r.col` while typing and typed it again (the protocol-node
  ledger's row). The row arm of `Typer.accessProperty` is one method, `Typer.rowColumn`, that `$r.col` and `read` both
  type by: the same column, multiplicity and typed node, so the SQL is unchanged (`ExtendCheckerTest$ReadTests`). A
  receiver that is not a row is refused (new test). The ledger row is deleted, dated.
- **The last sums (0928eb51).** `summaryStatistics` (the statistics' line figures with days, change and % change),
  `seasonality` and `projection` (each view whole), `tradedValue` (VWAP's price times volume), `linearAt` (the time
  fill's value between two in time) and `periodIndex` (a decomposition's place); `performanceStatistics` also says the
  periods a year it annualized by and the level's least. A person's own Pure calls each by name and answers as the tile
  does (`timeseries-queries.test.ts` "a person's own Pure ..."). What stays the writer's is shaping, the design note
  §5.6 says what.

An independent review, read-only, of both: every moved piece gives the old writer's numbers on every input the tile
sends (each compared by reading; the days place's start, `rowNumber - 1` against `rowNumber`, by a script of 200
cycles: no difference, a place is read modulo its cycle). Its findings:

- **2 (fixed, 9daf9390) an option the new functions did not know was read as another** (AGENTS invariant 4): a
  seasonality's grain `'Day'` as weeks unsampled, a compare `'rebased'` as none, `periodIndex`'s `'months'` as weeks, a
  projection's `perDay` 24 as seconds. Each checks its options once in its opening filter and says the ones it takes
  (a `fail` there is the Boolean it is; in a value's branch it typed the column Boolean, and a two-statement block the
  static fold refuses); a valid option folds away as before. The older functions' options (`returns`' type, a period
  statistic, `filterValues`' operator, a decomposition's method, a basket's rebalance) default as they did: the tile
  checks them before it calls, a person's own Pure does not -- the next PR's, put to the user. (Superseded the same
  day, §20: the user asked for every option checked when the query compiles, these included; the opening filters'
  `fail`s are gone.)
- **1 (checked)** the first tile's bundle budget holds (`bundle_budget_test` green).
- **3 (accepted)** a seasonality of a whole number with no compare types its value, least and most as a number where
  they kept the column's type (`if`'s branches); the values are the same (new test: a volume's).
- **4 (accepted)** a split line whose key is none never matched the figures' join, and now loses the two columns its
  blanks are said by with them.
- **5 (fixed)** `rowColumn`'s javadoc sat between `compose` and its own; `periodIndex`'s comment said "from 1970" of
  months; the census did not say `read`'s new refusal.
- **Test gaps (closed)**: a candle's least and most from lows and highs apart from its closes, and their weeks; a
  seasonality's % compare and its span; a projection of date-times (seconds, a week of them ahead, a day's slope); a
  daily decomposition against statsmodels; each refusal by its message.

Run on 9daf9390: `timeseries_queries_test` (161), typecheck, the library spike, headless, stress markets, the bundle
budget, `verify_charts_test`, `//core:core_tests_integration`, `//core:guardrails`, Python's `headless_test`; on
0928eb51 `//core:core_tests` (25 packages), `//spec:corpus_duckdb` and `//spec:corpus_h2` (rosters unchanged), and
DataCube's and Python's 23 timeseries targets. The statistics query: 61-102 ms, 24.4 KB.

## 20. Options checked when the query compiles; the tile as a program of its own (2026-10-28)

The user, on §19's open question: "Are you actually trying to bake the errors into sql or write the query in a way that
compile time checks the parameters? I hope it is the second (even for the ones you already fixed)". §19's checks were
the first: a `fail` in the opening filter, so a wrong option was refused by the database when the query ran. Now the
second, for every function:

- **Every option an enumeration of `datacube::ts`** (a35c28f1): `Statistic`, `PairStatistic`, `Returns`,
  `FilterOperator`, `ThresholdType`, `Direction`, `SeasonalModel`, `Rebalance`, `Grain`, `Compare`, declared at the
  top of the library; twelve functions' `String` options retyped, each comparison `$stat == datacube::ts::Statistic.mean`.
  A value the enumeration has not is refused by the type checker, by name ("enumeration datacube::ts::Grain has no
  value 'Day'"), before any SQL is written; §19's `fail`s are deleted. A projection's `perDay` is the platform's
  `DurationUnit` (`dateDiff` in it); `periodIndex` is total (a day's place, its days from 1970-01-01). The tile's writer
  passes enumeration values and refuses a word it has not before writing (`timeseries-query.ts libraryOption`).
- **Core: the static fold folds an enumeration's value** (`StaticFold.EnumToken`), as it did a spelled string, so an
  `if` on an option folds to the branch the call names: a mean writes `AVG` alone, no `CASE` (`UserFunctionIntegrationTest`
  "an option an enumeration").
- **The tile as a program of its own** (53e2f78f, the user's "A"): the Python and TypeScript the Code window shows
  make a page of their own -- a grid opened as each source's grid was (a file, a remote file, a warehouse table, a
  class; else a DataFrame of the person's), the tile over it -- and look nothing up on the page they were written from;
  the Python prints each formula's Pure, and the screen shows each one's under the program. "B" (a series written in
  Pure), a `#Quant{...}#` grammar for the formulas, and the SQL's size are follow-ups beside the brush (design note §11).

An independent review, read-only, of the three commits. Its findings:

- **1 (blocking, fixed) an enumeration's value passed on whole was left unbound.** The fold binds a static argument
  of a bodied helper, and each element of a map it unrolls, as a fact of its scope, and writes back each use the value
  reifies to; an enumeration's value reified to nothing, so `hash($digestHashType)` kept a variable no binder held. The
  corpus run confirmed it: `//spec:corpus_duckdb`'s roster lost `testExtendDigest_Relational`
  (`extendWithDigestOnColumns` passes `HashType.MD5` to `hash`). `EnumToken` now carries the node it was read from and
  reifies to it. New test, failing first ("unbound variable '$h'"): "an enumeration's value a body passes on whole".
- **2 (fixed) the generated code's comments wrote a person's names raw**: a title, a grid's title or a saved query's
  name with a line end in it ended its comment, and the rest was code. Every comment is one line
  (`timeseries-code.ts oneLine`, every control character and line separator a space), and every string escapes its
  control characters (`quoted`). New tests in both languages: the program with such titles parses, imports, writes
  no other line, and builds the tile with its title as given.
- **3 (fixed)** a grid named `print` would have shadowed the loop's `print` (now taken); a source of no instrument
  column left DataCube to choose one when run (now `key=None`); two grids over one frame named two frames of the
  person's (now one variable a frame). Tests in `test_page.py`, run end to end.
- **4 (fixed)** stale words: the library's comments said "a word compared when the call is expanded", the design
  note showed `'simple'`, and `Timeseries.code`'s docstring spoke of the page's grids.
- **5 (accepted)** the fold says an enumeration's value and a string are unequal, where SQL compared their names:
  Pure's own answer. The program does not carry a grid's own filter or calculated columns: it opens the rows and says
  the columns the tile reads; the grid is the person's to shape. A tile of no series gets DataCube's first, as the
  screen's Create does.
- **Test gaps (closed)**: `filterValues`' other operators, and `prices` of log returns.

The first tile's bundle grew past its budget with the enumerations and the program (100,296 of 100,000): moved to
102,000 with a dated note (`bundle-budget.test.ts`).

Run on the fixes: `//core:core_tests` (25 packages), `//core:guardrails`, `//spec:corpus_duckdb` and `//spec:corpus_h2`
(rosters unchanged, `testExtendDigest_Relational` passing again); DataCube's typecheck, `timeseries_queries_test`,
`timeseries_code_test`, the library spike, headless, examples, stress markets, guardrails, the one-API ledger, the
Python engine, the view DOM, the bundle budget and `verify_charts_test` (the browser); Python's `engine_test`,
`frames_test`, `headless_test` and `markets_test`.


CI on f668cc45 then found what the local run had not covered: `//parser-equivalence`'s own-corpus ledgers grew with
the two new tests' generic models (signatures 21 -> 23, `UserFunctionIntegrationTest`'s host 13 -> 15), re-pinned
with dated notes in 5ce601ce.

### 20.1 Enumeration mappings, and a second review of the fixes

The user asked whether an enumeration whose database value differs (`'A'` for `ACTIVE`) is in scope. It is the
mapping's, not the fold's: the store resolver reads `$x.status == Status.ACTIVE` as the database's codes, and the fold
leaves a comparison with a column as written. A test pins it through the path the fix touched
(`anEnumerationMappedToTheDatabasesCodes`: a generic body filters a class's projected column through a helper given
`Family.SMITH`; the SQL filters `LAST_NAME = 'Smith'` and reads the column back as `SMITH`; it failed before c0f887dc,
"unbound variable '$wanted'").

A second independent review, of the four fix commits alone (c0f887dc, 79159403, f668cc45, 5ce601ce), with its own
probes:

- **1 (blocking, fixed) a title on the Python's first line was read as its encoding** (PEP 263): `coding=utf-7 ...`
  decoded `+ACc-` as a quote and `+AAo-` as a line end inside the title's string, and the program ran code the title
  carried; `compile()` of a string ignores the declaration, so the exec-based test could not see it. The program's
  first two lines are its own words now, the title said on the third. New test: the reviewer's title compiled from
  bytes and run, its tile titled as given and nothing else run.
- **2 (fixed) a type or function reference had EnumToken's gap**: `TypeToken` reified to nothing, so a function a
  helper evaluates whole (`$f->eval($s)`) was left unbound, in a helper and in a map unrolled. It carries the node it
  was read from (equality by path alone, since a column's `.type` has none). Corpus rosters unchanged.
- **3 (fixed, older than this PR) a helper's parameter read as another's**: the fold read each argument in the scope
  it was filling with the callee's parameters, so `swap(a, b)` calling `cat($b, $a)` answered `y-y` in a body
  (`y-x` in a query), and a caller's variable spelled as a parameter was captured. The callee's parameters are
  renamed apart (`AlphaRename.apart`, from their own reads: no protocol node built, the guardrails' ledger unmoved)
  and each argument read in the caller's scope.
- **4 (fixed) an enumeration's value against a string answered two ways**: `Stat.mean == 'mean'` folded false in a
  body where the database, comparing the name (S6), answers true in a query. The fold compares values of one
  enumeration and leaves the rest to the database. That an enumeration's value equals its name's string is lite's
  (S6's representation), not legend-pure's; it predates this PR and is put to the user to register or tighten.
- **5 (fixed)** the `key=None` test's run half would have passed before its fix (no text column to take as a key):
  its frame has one now. **6 (fixed)** direction marks (U+200E/F, U+202A-E, U+2066-9) are escaped in strings and
  spaces in comments; `EnumToken` holds its node alone.
- **7, 8 (accepted)** the budget's measure (100,296) and the ledger moves; the reviewer suggested writing such tests in
  the reference dialect, which the second review's own tests are (`UserFunctionIntegrationTest$FoldOfABody`, functions
  over a TDS), but for the one helper that takes a function-typed parameter (function-types 19 -> 20, the host
  16 -> 17); the mapped-enumeration test's generic model is the library's shape (signatures 23 -> 24, host 15 -> 16).
  The own corpus's matched snippets rose 2998 -> 3004 (`ratchets.tsv`, regenerated).

Each new core test failed before its fix ("unbound variable '$f'", `y-y`, `ne` where the query says `eq`); the
Python coding-line test fails on the old generator by construction (the title was its first line).
