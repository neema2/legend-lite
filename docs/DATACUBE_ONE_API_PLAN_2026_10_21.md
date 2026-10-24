# One API, for real: the plan (2026-10-21)

**Goal (the user's tenet).** DataCube's logic is TypeScript APIs. The TS UI and headless Python call exactly the same
ones. Every change to a saved page, made in the browser or in Python, is a command, applied by one function. Python
keeps no DataCube rule of its own. Both sides can do the same things to a page.

**Evidence.** docs/DATACUBE_ONE_COMMAND_API_AUDIT_2026_10_21.md (four read-only audits checked against the code). Every
item below has its `file:line` there.

**Decisions** (the user, 2026-10-21):
- **D1** Open…, open in place and New ▸ Blank Page on a page Python serves open a new page and leave the served one
  alone.
- **D2** Empty pages are allowed on both sides.
- **D3** Every kind of source on both sides: Python reads, edits, saves and adds file, remote file, warehouse,
  saved-query and class grids, as the tab does.
- **D4** Python asks the TypeScript `model-changes/` library headless; `model.py` goes.

**Decisions** (the user, 2026-10-22):
- **D5** A model action (Create a Class, a Data Space, an Association) is not undoable: what it writes into its project
  cannot be undone, so the page's record of it (`modelInto`, `modelKeep`, `modelPin`) is applied by the one function in
  the browser as it is in Python, but is no undo step. The browser's half is done with S5's Z4-Z7, where the model
  actions move out of their dialogs. Registering S1's audit finding 10 as an item waits on the user's dated sign-off (it
  grows a ledger).
- **D6** S6 builds what a Python user would do; anything else is a named exception, with its reason. Every Python
  ability calls the same TypeScript function the browser's gesture calls; where the browser's logic still sits in screen
  code, it moves out into that function first.
- **D7** One **Show Grid** on a chart replaces Open in grid and its Update: a chart following its grid -- that grid
  shown, the page unchanged; a frozen or detached chart -- one step, undoable: a grid of the chart's own grouping and
  query added, the chart following it, shown. The chart looks the same after; it follows its new grid from then on (a
  frozen chart no longer takes its old grid's filter). The temporary editing grid and its Update go.
- **D8** Python's chart API: `chart.select(value, split=...)` (a mark named by its value across, and its series'),
  `chart.select(None)`, `chart.selection`; `chart.drill(value)`, `chart.drill_up(levels=1)`, `chart.drilled` (the
  browser's own `drillDown` and `drillUp`); `chart.grid()` -- the chart's grid, the same function as the browser's Show
  Grid: the grid a following chart follows, or, for a frozen or detached chart, one made first (a method, as
  `page.grid()` and `grid.chart()` are: the read-only `chart.grid` property it replaces goes). Explicit in Python, no
  toggle: a click's toggling is the screen's; a range across an axis not at first.
- **D9** Every ledger's growth waits on the user's dated sign-off, the growth this plan itself calls for included (rule
  1 as written: no standing approval).
- **D10** Signed off: the S1 raises of `L8_KINDS` (13 -> 15 for `select` and `refilter`, K7; 12 -> 13 for
  `setChartCube`, K11) and S4's of `L9_KEEPER` (6 -> 8, the restore writes N1 and N2); and S1's audit finding 10
  registered as P15 (`MARKERS` 9 -> 10). Each is fixed for real, not only listed: K7 and K11 in S6 (D7, D8), P15 in S5
  with Z4-Z7 (D5).

- **D11** (the user, 2026-10-22, B3/B9) A page nobody named is unnamed on both sides, as the tab keeps it: no name in the
  page (''), shown as "Untitled page", Save suggesting its first sheet's name; `ll.Page()` makes one so. A new page's
  skeleton is one function, which `pageNew` and the tab's Blank Page both use.
- **D12** (the user, 2026-10-22, Z12) Python only tags the filter values JSON cannot carry (`{"$date"}`,
  `{"$decimal"}`, `{"$timestamp"}`, as a grid's cells come back tagged `$int`/`$float`); DataCube's reader reads the
  tuple shape, every value and its column's type -- a timestamp included -- and Python checks nothing.
- **D13** (the user, 2026-10-22, X1) A warehouse table on a Python page reads through a warehouse session the engine
  holds, signed in by a tab; with none, it is refused in words (sign in to it in DataCube first). Python carries no
  credentials.
- **D14** (the user, 2026-10-22, X3) A page over a file is given its files by the caller (`Page.load(...,
  files={name: path})`), each checked against the hash the page names; one not given is refused in words, as the tab asks
  the person for it. A remote file is mounted again by its URL. The keeper keeps each file's table while a page reads
  it, as it keeps copies.
- **D15** (the user, 2026-10-22, after S5) No lock held across a call into DataCube's code and back: the `engine`
  host call answered on its caller's thread, holding the frames' lock (S5's way of asking Python's engine from inside
  V8), is replaced -- each call into DataCube works from what Python serves as the call starts (a snapshot of its own),
  so a callback it makes is answered with no lock its caller holds and no plan on the caller's thread. Built after S5,
  before S6.
- **D16** (the user, 2026-10-22, after S5) One rows path: a grid's rows planned and run one way, whatever its base and
  whoever asks (the tab, a widget, Python's `view` and `to_pandas`), not a frame's path and an engine leaf's chosen by
  the base's `runs`. Built after S5, before S6.
- **D17** (the user, 2026-10-22, after D15/D16) S6 generates Python's command layer from DataCube's TypeScript command
  types (grid-commands.ts, page-commands.ts), with a thin hand-written layer for Pythonic names on top: Python has
  nowhere to write a rule. The method names are put to the user for approval before S6 builds them (D6).
- **D18** (the user, 2026-10-22) An independent review of S5, D15 and D16 before S6: a reviewer that has not seen this
  line's reasoning, looking for tests made to pass, items closed too early, and the stand-ins; what it finds goes into the
  register (rule 9) and is fixed or said.
- **D19** (the user, 2026-10-22) The client side of the single writer (the tab's pending edits kept and re-applied, undo
  as inverse commands, whole-page writes gone) is its own step after S7, as §8 has it.
- **D20** (the user, 2026-10-22) This line answers the Deephaven streaming line once: it stacks its branch on this one
  (at D16's commit), this line reviews; what is stable and what is still moving said.
- **D21** (the user, 2026-10-22, "all the python names are fine") S6's names as proposed: `grid.sorts([...])` replaces
  the sort, `grid.clear_sort(col)`; `grid.add_filter`; `grid.calculate(..., level='group')`; `grid.window`,
  `grid.child_aggregate`, `grid.explode`; `grid.show_as`, `grid.grain`, `grid.bins`; `grid.dimension`, `grid.drill`,
  `grid.drill_up`; `grid.copy()`; `page.filter(cls, condition)`, `page.filters`, `page.remove_filter(id)`;
  `sheet.move(position)`, `page.remove(sheet)`; `ll.Page.open(id)`; `page.grid(cls=..., project=..., revision=...)`,
  `page.grid(query=...)`, `page.grid(warehouse=..., table=..., catalog=...)`; the charts' as D7/D8. A class is named by
  `cls=`; option values are DataCube's own spellings (`'greaterThan'`), no Python aliases.
- **D23** (the user, 2026-10-23, "Why wouldn't python get save as and delete and saved pages and class grid work? Names
  and size are fine"; then "Fix the python stuff first, then PR, then live edit") The S7 audits' four asked items are
  built, not declared: Python's Save As, delete and list of saved pages (K13-K15: `page.save_as(name)`,
  `ll.Page.delete(id)`, `ll.Page.saved()`), and the tab's class grid on a Python host and a class of another project
  (K16, K17). S8's bundle budgets and its Python names stand. D19 follows the PR in its smaller form (undo stays a
  whole-page restore, N2; the tab keeps and re-applies the edits not yet confirmed).
- **D22** (the user, 2026-10-22) A query over a table dropped since its read began is refused, in words: that is the
  rule, not a limit (F31). A call never reads rows the session no longer serves.
- **D24** (the user, 2026-10-24: finish D19 "right", to the standard that live shared editing of a page would work, as
  the proof it was done correctly) D19 is built in its full form, not D23's smaller one:
  docs/DATACUBE_LIVE_EDITING_2026_10_24.md. Undo and redo are inverse commands produced by two total diffs
  (`gridBetween`, `pageBetween`); the restore write goes and N2 retires when its item (W5) lands; the tab converges on
  a combined or refused write by replaying the keeper's steps over its confirmed page, never by a reopen; a follow
  writes pending edits first and waits for a gesture in flight; a gesture's commands are told on one schedule; every
  write's answer carries the keeper's page hash, checked by the tab; the proof is a two-editor soak (two real tabs and
  Python, undo in the mix) as a gate. Items W1-W18 (its §7), closed here as each lands. Out of scope, said there:
  presence, CRDTs, a keeper outside Python, Python undo.

## 0. Why the nine attempts before this one did not finish

This is roughly the tenth program aimed at "Python and the browser the same": the headless design (10-11), the
one-app design (10-11), the twins program (10-15), one grid edit path (10-17), the command homework (10-17), command
stages 1-3 (10-18), page state (10-18), live commands (10-18), the page keeper (10-20). Each did real work. Each left
gaps the audit found, for the same six reasons:

1. **They fixed lists, not mechanisms.** Each inventoried what it saw that day and fixed it. Nothing failed when a new
   path appeared: `#change` stayed callable from anywhere in `app.ts`; `applyConfiguration` kept a rule of its own; the
   Show As presets (added later) send `calculate` commands no reader accepts, and no test noticed.
2. **They designed twins in for local reasons.** Page state (stage 2) chose "the screen makes the edit, then says so"
   so the screen's bundle would not carry `page-commands.ts` (`page-state.ts:5-8`). That one choice is root cause R1.
3. **They accepted "rare enough" exceptions.** Page undo, the page's model, Snap from the screen: each was left a
   whole-page write, and the exceptions multiplied.
4. **"Done" was declared per step, not against a measured end state.** The task "screen Snap/Go Live as page
   commands" was closed by a commit that built *following* Python's snaps and made the screen's Snap page state
   (534915c21); the screen still sends no `snap` command. The coverage report was "a
   report, not a gate" (`DATACUBE_COVERAGE_2026_10_18.md`).
5. **Python parity was nobody's test.** Capability gaps (page filters, sheet removal, calculated-column edits) were
   never a failing row anywhere.
6. **Old paths lived beside new ones "for now"**: `applyConfiguration` beside `configure`, the board's removal rules
   beside `withoutTile`, the host's whole page beside the commands it carries.

## 1. Rules of this program (what makes it different)

1. **Ledgers before code.** Step 0 puts every known gap into shrink-only ledger tests in the build (§3). A new gap
   turns the build red. An entry leaves a ledger only with the fix and its test. A ledger grows only with the user's
   dated sign-off.
2. **Done is measured.** The program is done when every ledger is empty (except the named exceptions in §2) and a
   fresh audit (step 7) finds nothing the ledgers missed.
3. **No designed-in twins.** A size or speed concern is met by raising a budget with a dated note, or by loading
   lazily, never by a second implementation.
4. **No step closes half-built.** Each step's acceptance is checked item by item by an independent reviewer against the
   register (§4). "Follows but does not send" is not done.
5. **The old path dies in the same step as the new one is born.** No two paths side by side.
6. **Every new command kind ships whole:** its apply rule, its JSON reader and writer, its rebase (renaming) arm, a UI
   producer, a Python builder, a parity row. The capability matrix test (ledger L8) enforces it, so a feature added
   after this program cannot reopen the gap.
7. **Every fix has a test that fails without it.** Every commit names its step and item ids, and updates the register
   in this file in the same commit.
8. **Gates.** The DataCube and Python suites green before every push. At the end of each step: the full local set,
   one CI run, an independent audit of the step, then an "As built" section appended here.
9. **Scope is held.** Anything found that is not in the register is added to it (with its ledger entry) before it is
   fixed; nothing is fixed silently, nothing found is dropped.

## 2. The end state

| | What is true | Measured by |
|---|---|---|
| E1 | Every change to saved grid or page state, on either side, is a `GridCommand` or `PageCommand`, applied by `applyGridCommands` / `applyPageCommand`. | L1, L2, L3, L4 empty |
| E2 | The screen's page and grids are *produced by* those functions; no screen code changes saved state another way. | L1, L2 empty; page-state guard |
| E3 | The host's page is what the commands make, not what a writer sent. | L9 |
| E4 | Derived state (a following chart, a selection, the tree's totals, column types) is computed by one TypeScript rule on every path. | L7 empty |
| E5 | Python holds no DataCube rule: its page document only ever comes from a headless answer; `model.py` is gone. | L5, L6 empty |
| E6 | One rule for every job both sides do. | L10 empty |
| E7 | Every command kind has a UI producer and a Python builder (or a recorded "by nature" reason); every source kind works on both sides. | L8 empty |

**Named exceptions** (allowed at the end; the user confirms them with this plan):
- **N1** A page is *born* whole: `Page()`, `Page.load`, opening a saved page.
- **N2** ~~Undo and redo (grid and page) write the page whole as a *restore*, taken only at the current version. They
  become commands only if the proper live-editing model (out of scope, §8) is built.~~ **Retired 2026-10-24 (D19 W3-W5,
  docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3):** an undo or a redo is the commands between the step and the page as
  it is now (`gridBetween`, `pageBetween`), told and applied as any edit; the restore write is gone from the tab, the
  headless engine and `Engine._page_said`, which refuses a page written whole in words.
- **N3** The snap row limit stays a value per host (the user and the DataCube + Python line, 2026-10-15); its refusal
  sentence comes from one TypeScript function.

## 3. The ledgers (step 0)

Each is a test in the build, listing today's gaps by item id. It fails on a gap not listed, and on a listed entry that
no longer exists (so the list can only shrink, and stays true).

| Ledger | Holds | How it detects |
|---|---|---|
| **L1** grid state changes | every call in `datacube/src/app.ts` that changes the cube's state other than `#applyCommands`; every call of the command path (the allowed ones' text pinned); every caller across the app of the grid's public ways in (`state`, `change`, `refilter`, `applyGridCommands`, `followSnap`, the controller); every caller of each listed gap; Snap's emits and controller calls, and the host's page writes, counted only in a function that tells no command | static, by enclosing function (indentation scope), as `state-guardrail.test.ts` reads sources |
| **L2** gestures | a catalogue of gestures on the real PageApp + CubeApp (jsdom) and the tab's live channel; each made on a quiet page; the commands told, as the host read them, replayed on the page before by the shared functions (as headless gridApply applies a grid's), give the page the screen wrote; no change written whole; a view-only gesture writes nothing; every command kind told by some gesture or listed | behavioural; ledger = each failing check with how it fails (the host's words, the parts of the page that differ) |
| **L3** wire | every command a gesture tells is read by headless pageWritten as it was sent | part of L2's run |
| **L4** Python whole writes | every `_unnoted()` call in `python/legend_lite/page.py`, and every assignment of the write's flags (`_unserved`, `_dirty`) | static, by Python's own parser |
| **L5** Python funnel | every write into a page's document -- whole, under it, through a handle's part (`_view`, `_cube`, `_query`) or a local name bound to one; deletes, `setattr`, `__dict__` -- each allowed one's right-hand side pinned (a headless answer) | static, by Python's own parser |
| **L6** Python copies | the Python functions in audit §5, by name; fails when one is gone but still listed | static (a register: new copies are caught by L5 and review) |
| **L7** convergence | at S0: derived state Python leaves as it was (V1, V2, G3, asserted as today) and every gesture's replay check (the screen against the shared functions); S2 first extends the convergence soak (Python, headless and the tab over one vocabulary, charts, selections and tree included), its divergences listed | behavioural |
| **L8** capability matrix | every command kind x {its kind list, both readers, the rebase's renaming, a screen producer, a Python builder}; every source kind x {Python reads it, Python adds it}; headless exports Python never asks; the capabilities with no kind of their own (K1, K3-K6, K9-K12) listed as unchecked until S6 agrees their names | static over the kinds' union types (a per-kind parity row is not detectable: the parity tests are by action) |
| **L9** host writes | what the keeper takes whole, through the engine's own write (H1-H3); who writes the keeper at all (page.py, engine.py, datacube.py) | behavioural, and static by Python's parser |
| **L10** same rule | one parity test per behaviour difference (audit §6); ledger = rows still differing | behavioural |
| **L11** screen builds no command (S8) | no command written out in a screen file; `pageCommand` only for the person's input; every function that makes commands the screen calls asked by headless too, or the screen's own, said why | static, by the command types' kinds, closed at 0 |
| **L12** branches on the host (D23) | every place screen code branches on its host -- the page's own engine, a database ability, a service, an address, a grid's runner, or a name holding one -- by function and capability, each with what the other host does there (`both:` its own path, or `by nature:` why none); each `both:` an ability, proved on each host by named tests, and -- done by an implementation each (`two`) -- held to one result by a parity test | static, by enclosing function; a new branch fails until it is listed and said; a proof whose test is gone fails |

Every ledger's size is pinned (`CEILINGS`): a fix lowers it; it grows only with a dated note. The register is pinned too:
every item it has held is in it, open or closed.

Acceptance of step 0: every ledger green with today's entries, each entry naming an item id from §4; adding a dummy
`#change` call, a dummy `_unnoted()` and a dummy untold gesture each turns its ledger red (checked once, not committed).
No product change. Also: the program's line in `docs/IN_FLIGHT.md`.

## 4. The register

Every item, its step, its ledger. `[ ]` open, `[x]` closed (with the commit). Evidence: the audit, by section.

### Grid edits that are no command (audit §2, grid)
- [x] **G1** expand / collapse a row → `open` command — closed S1: `{kind:'open', path, open}` (set, never toggled; a path
  deeper than the row groups opens nothing), the chevron's handler builds it, Python `grid.expand` / `grid.collapse`; the
  gesture "a group opened" is told, read and replays
- [x] **G2** Collapse All → `collapseAll` — closed S1: the menu builds it, Python `grid.collapse_all`; the gesture "every
  group collapsed" is told, read and replays
- [x] **G3** Properties' tree effect (totals, expand level) → the tree rule inside `configure` — closed S2: `configure`
  moves the tree by the settings it sets (grid-commands.ts `treed`); Properties Apply is its commands alone, and
  Python's configure moves the tree as Properties does (grid-commands.test.ts, test_page.py); a setting given again
  as it is moves nothing (F3)
- [x] **G4** calculated-columns editor: edit and rename, references carried → `editCalculated`; the rename rule moved
  into `grid-commands.ts` from `snapshot.ts:803` + `config.ts:886-899` — closed S1: `{kind:'editCalculated', name, level,
  column}`; the two lists by one rule (snapshot.ts `withCalculated`, the editor's candidate and the commands alike), the
  settings carried by the command alone; Edit Column's OK builds it, Python `grid.edit_calculated` (headless
  gridCalculate, `replacing`); the gesture "a calculated column renamed in its Edit Column window" is told, read, replays
- [x] **G5** delete a calculated column (written twice today) → `removeCalculated` — closed S1: the editor's Delete and
  the header menu's both build it, Python `grid.remove_calculated`; the gesture "a calculated column deleted in its Edit
  Column window" passes
- [x] **G6** Filters window → `filter` — closed S1: its OK and Apply build `filter` after the compile check; the gesture
  "a filter changed in the Filters window" passes (and `filter` is told by a gesture now)
- [x] **G7** chart click on a grid's chart → grid `refilter {remove, add}` + page `select {chart, conditions}`; the
  rule moved out of `app.ts:2741` and `cube-page.ts:1462` — closed S1: the refilter rule is grid-commands.ts's, the
  chart's selection page-commands.ts's; a click tells both (a chart removed tells its grid's refilter, removeTile its
  page part); the gesture "a chart's mark clicked" is told, read, replays; Python's chart selection is K7 (S6)
- [x] **G8** restore heals a filter missing a chart's selection → the document kept consistent by G7's rule — closed S3:
  the board heals nothing (CubePage.sync puts it at the page and takes no rule of its own); a step of the page's history
  that brings back a chart with a selection its grid's filter lost meanwhile (the grids keep their own history) has it
  put back by that grid's own `refilter` command, told -- the owner's, by one rule (page-commands.ts
  `selectionRefilters`; page-app.ts #move; cube-page-shell.test.ts, a chart removed while it filters, undone). Not
  done, and said in As built (S3): the chart panel still regroups and the screen still reconciles a selection by the
  same rules on their own (followCube, the strict `withoutConditions`), against the grid's state -- a cube alone over
  rows its host names no source for has no cube in its page's state to follow (the S2 audit's 7)
- [x] **G9** use a dimension → `group` — closed S1: `#useDimension` (no longer public) builds `group`
- [x] **G10** `applyConfiguration` → deleted; its tests use `configure` — closed S1: a host edits a grid by
  `CubeApp.edit(commands, label)` (the screen's own path, told); the gesture "a grid's settings set by its host" passes
- [x] **G11** public `change()` / `state` → removed or read-only — closed S1: `change()` and the `controller` getter gone;
  `state` is `CubeStateView` (busy, historyDepth); a refresh and a debug print are `refresh()` and `printQuery()`; L1
  pins that neither the owner nor the controller is handed out again
- [x] **G12** grid undo / redo → N2 restore write — closed S4: an undo or a redo that lands (the owner's `moved`) is told
  as `onRestore`, and the page's channel writes the page whole, a restore its keeper takes only at the version it was
  made at (live-channel.test.ts, a real page: a grid's undo written whole, a stale one refused; test_one_api.py, a
  restore behind the page refused); its L1 entries are item-less N2 entries. **2026-10-24 (D19 W3, W5):** superseded --
  a grid's undo is told as the commands between (`gridBetween`), the restore write deleted
- [x] **G13** an edit retried after signing in again → its commands kept and told — closed S1: a refused change's
  commands are kept (app.ts `#refusedCommands`) and told when it is run again and lands (cube-transactions.test.ts)
- [x] **G14** a grid made while the page bar is folded → `showTitleBar` in the `addGrid` document — closed S0: not a gap (the
  `addGrid` document carries the title bar as the grid was made; the gesture "a grid added while the page bar is folded"
  passes told, wire and replays)
- [x] **G15** standalone New ▸ Source cap rule (unreachable in the app) → deleted or shared — closed S1: shared, one
  rule (cube-document.ts `freshConfiguration`, capped by `gridCapped`); L1 pins no second spelling

### Telling exactly what landed (audit §2, after the grid table; each a marker until S1 writes its failing behavioural test first)
- [x] **T1** a command refused on the committed state but kept on the pending one — closed S1: judged on the state on
  screen; one the committed state cannot take is told after the change in flight lands (cube-transactions.test.ts)
- [x] **T2** a presentation command told before a running transaction's — closed S1: told at once (the committed
  state's own transition) and again after the transaction's when it commits, the order that state took them in
- [x] **T3** a change made on top of an undo in flight — closed S1: the owner's `committed` says `moved`; a transition
  an undo or a redo is part of tells no command, so the grid is written whole (G12's named exception)
- [x] **T4** Python's commands and the user's mixed during a follow — closed S1: another editor's commands
  (applyGridCommands) never enter what is told; the person's are told as they land

### Commands the host cannot read (audit §4)
- [x] **C1** window, child-aggregate and explode columns in the `calculate` JSON reader and writer — S1, L3 (closed S1:
  `calculatedFrom` reads every column kind strictly; grid-commands.test.ts round-trips each; the Show As gesture's wire passes)

### Page edits that are no command (audit §2, page)
- [x] **P1** page name box → `renamePage` — closed S1: page-app.ts `rename` tells it (no step of the history); the host's
  renamePage calls it; the gesture "the page renamed in its name box" is told, read, replays
- [x] **P2** Save / Save As under a name → `renamePage` — closed S1: saveTo renames by `page.rename` (L1 holds the host's
  code: no host write of the name outside a function that tells)
- [x] **P3** Snap / Go Live in the tab → `snap` / `live` — closed S1: a grid that moves its rows itself (its toggle,
  snapRows) says so (`moved`); its page tells `snap {grid, copy}` or `live {grid}`, no step of its history; two gestures
  (snapped, live again) are told, read, replay
- [x] **P4** Create a Class / Association / Data Space → model page commands (`modelInto`, `modelPin`, `modelKeep`)
  applied by `page-model.ts`'s functions — closed S1: `modelMade`, `modelPinned`, `modelKept` are the rules, in page-commands.ts
  beside the commands they apply (PageModel uses them; kept there, a lazy chunk, so the page loads none of them at start); the tab tells `modelCommands(before, after)` after each model action (page-commands.test.ts)
- [x] **P5** page undo / redo → N2 restore write — closed S4 as G12: PageApp's `#move` tells `onRestore` (the gesture
  ledger's undo and redo gestures write a restore and nothing else). **2026-10-24 (D19 W4, W5):** superseded -- a page's
  undo is its step's inverse commands (page-state.ts), told as any edit; the undo and redo gestures are told gestures
- [x] **P6** chart options told before the chart re-follows → falls out of S2 — closed S2: `setChart`'s rule keeps a live
  chart followed (followedPage), so the spec told before the panel follows replays to the one it keeps
  (page-commands.test.ts)
- [x] **P7** Open in grid resets the chart's band shares after a resize → an editor tile never touches the saved layout — closed
  S3: the board keeps each sheet's layout as the page has it apart from its own drawing (cube-page.ts `#pageLayouts`, set
  only when the board is put at a page: `sync`, `restore`, a new sheet), and writes that; the editing grid, placed
  beside its chart, shares its band evenly on the board alone, and closed, the board is put back at the page's shares
  (cube-page-shell.test.ts: a band shared 0.7/0.3, Open in grid, closed -- fails both ways without it). Corrected by the
  S3 audit (F9): an editing grid sharing its chart's band gave its share to a neighbour when a gesture was made while it
  was open; it is drawn in a band of its own below its chart's, and an arrangement made meanwhile is told as `arrange`
  of the page's tiles
- [x] **P8** Update a detached chart from its editing grid → `setChartCube` — closed S1: `setChartCube {chart, document}`
  (the chart reads `<chart>-query`, the cube it read before let go when unread), told with its setChart as one step; the
  gesture "a detached chart updated from its editing grid" is told, read, replays; Python's is K11 (S6)
- [x] **P9** restore drops views it cannot open → kept in the document as unavailable tiles, never dropped — closed S1:
  a cube whose source was not opened is kept as saved (page-app `#unopened`), its views placeholder tiles that say why
  (cube-page `placeUnavailable`), written back in their places; removed as removeTile removes a view, undone back
  (one-api-gestures.test.ts P9)
- [x] **P10** Open…, in place, Blank Page on a served page → a new page (D1) — closed S4: on a page another editor serves,
  each puts a page of the tab's own on screen and leaves the served one as it is -- its channel stopped, its moves no
  longer followed, said once, a blank page on its card (datacube-app.ts `leaveServed`; the browser harness: Blank Page
  says so, the tab asks Python's versions no more, and Python's change to its page is Python's alone; jsdom,
  datacube-app-live.test.ts: only Blank Page leaves it, and a follow on its way lands nothing). Open… and open in place
  call the same `leaveServed`; neither has a test of its own
- [x] **P11** the tab reopens cubes over frames as they are now → `rebuilt {cube, document}` page command made by one
  side (with Y2) — closed S1: whichever side opens a cube again over its frames tells it, once, by one rule
  (cube-document.ts `cubeTaken`): the tab opening a host's page tells `rebuilt` for each cube it opened as another
  (page-commands.ts `rebuiltCommands`, a snapped one not; test/python-engine: a frame changed in place, and nothing
  told when nothing changed), Python for each it opens (Y2); the tab follows a `rebuilt` from Python by opening the
  page again (live-follow.ts). After the S1 audit, `rebuilt` carries the source as it is now (`{cube, source}`, not the
  document) and is applied by one rule to the cube the page holds then (cube-document.ts `cubeOpenedAgain`), so made
  again on top of another writer's edit of the grid it keeps that edit
- [x] **P12** `ll.show(df)` page unkept; a frame's column change loses grid edits → a kept page of one grid — closed S5:
  `ll.show(df)` (a tab's `Cube`, a notebook's `DataCube`, marimo's) is a page of one grid over the frame, kept as every
  Python page is (datacube.py `frame_page`: its name's page when it shows already, its frame replaced; snapped in
  Python when `snap=True`) and served as one (`engine.html?page=`, the widget's `page_key`); the tab's frame start
  (`AppStart` 'frame', `openFrame`, `CubeApp.snapRows`, `?table=`/`&snap=1`), the widget's `table`/`version`/`snap`
  and `version.json?table=` are deleted (test_show.py, test_notebook.py, the browser harness)
- [x] **P15** a model action's page record (`modelInto`, `modelKeep`, `modelPin`) is set outside the tab's page state and
  told by its difference, not applied as commands by the one function -- S1's audit finding 10, registered 2026-10-22
  (D10) → applied as commands, no undo step (D5) — closed S5: the page's state holds its model (PageApp `#model`, as
  it holds its filters: made and kept by the model commands, its own and its other editor's, written with the page);
  a window's record of the page (page-model.ts PageModel) is the commands it made, in order, which the tab applies by
  the one function, told, no step (PageApp.applyModel); across an undo or a redo the model stays as the actions left
  it, a grid brought back bringing its own (page-document.ts `modelAcross`). The tab's `current.model` and
  `modelCommands` (a diff) are deleted (page-app.test.ts, page-commands.test.ts)
- [x] **P13** charts on a cube alone (embedded CubeApp) tell nothing → the same state and commands — closed S3: a cube
  alone's board keeps a page state (page-state.ts) and applies each edit by the same one step as a page of its own
  (page-commands.ts applyToState), then puts the board at what it makes (CubePage.sync), and tells the commands
  (CubeAppOptions.onPageCommand); the rules come with the board's module, never with a grid alone
  (cube-page-shell.test.ts: added, clicked, a second, removed -- each replays to the board shown). The S3 audit's F6-F8,
  F12: its state kept as its grid's own changes leave it, its ids held across its board's lifetimes, its empty board
  said once the step has done; what it saves is its state less what a cube alone cannot save (its added grids)
- [x] **P14** stack and arrange gestures told as a whole `layout` → told as `stack` / `arrange` — closed S3: the board
  says what the hand did (band-board.ts `LayoutMove`): a lone tile let go on another's middle is told as `stack {tiles:
  [onto, tile]}` (stackTiles stacks as a drop does; a tab dragged out of its stack is not one), a preset picked as
  `arrange {sheet, preset, order}`; any other change, or one moving an editing grid, is the sheet's layout
  (band-board.test.ts; the gestures "a chart stacked on its grid by dragging", "a sheet arranged", and "a grid dragged
  to the left of another", which still tells `layout`)

### Python writes that are no command (audit §2, Python)
- [x] **Y1** `_into` / `_keep` → P4's model commands — closed S1: Python's model actions apply `modelInto`, `modelPin`,
  `modelKeep` through pageApply (test_model.py)
- [x] **Y2** `_reopened` → `rebuilt`; the take-or-not rule moved to TypeScript — closed S1: headless gridOpened says
  `taken` (cubeTaken) and `_reopened` applies `rebuilt` through pageApply (test_page.py)
- [x] **Y3** `_kept` with notes → `rebuilt` — closed S1: a grid edit opens its cube again first (`_current`: its
  `rebuilt`), so `_kept` notes the edit's commands alone (test_page.py, a Live frame changed in place)
- [x] **Y6** `serve_page` works out reads in Python → `pageReads`, or deleted — closed S5: deleted, with the engine's
  `close_page` -- test-only hooks in product code, no product caller; the engine's tests serve a page through the keeper
  themselves (test_engine.py `Page.serve_page`)

### Derived state (audit §3)
- [x] **V1** a following chart re-followed when its grid changes, by one rule on every path — closed S2: page-commands.ts
  `followedPage` (followCube over the cube as its document holds it), applied by `rebuilt`, `setChartCube`, `setChart`,
  the rebase after a grid's edit and headless `pageWithCube` (Python's grid edits); the gesture "a grid with a following
  chart ungrouped" replays (page-commands.test.ts, test_page.py). The screen's chart panel still follows by the same
  `followCube` on its own, against the grid's state on screen (G8's note; As built S3; the S2 audit's 7)
- [x] **V2** a chart's selection reconciled with its grid's filter, on every path — closed S2: the same rule takes off a
  selection its cube's filter no longer holds, as the screen's reconcile does (page-commands.test.ts, test_page.py),
  judged when that filter moves (F1), a chart's removal among what moves it (F2)
- [x] **V4** group-level column types written the same by both sides (spike first: type at compile, or stop storing
  run-derived types) — closed S2: the spike found typing at compile incomplete (a pivoted level's columns need the
  pivot's values, which only a query gives), so no side writes the type a run learned: `writeCube` leaves it out, the
  screen after a run and a host never run write the same cube (cube-document.test.ts); the corpus regenerated
- [x] **V5** expand level and the groups closed under it saved and read back — closed S2: `treeDocument` writes
  `expandTo` and `closed` (when it has a level), `readTree` reads them in words, `treeOf` and `openCube` open with them
  (`TreeState.saved`; regrouped, the closed paths go and the level stays)

### The host (audit root cause R2)
- [x] **H1** the keeper applies a write's commands and keeps what they make — closed S4: a write is its base version
  and its edits; the keeper has DataCube apply them to the page as it is (headless `pageRebase`: at the base nothing to
  combine) and keeps what they make; the tab sends no page, Python's page writes its commands the same way
  (keeper.py `settle`; test_one_api.py, test_keeper.py)
- [x] **H2** the no-command path deleted: the tab's `#told/#seen` guess and `edits: null`, the keeper's refusal for
  it, `pageWritten`'s null — closed S4: the one write that is no commands is a restore (N1, N2: keeper.py `restore`,
  headless `pageWritten(text)`); the tab writes its edits, or an undo's page whole, and nothing else
  (live-channel.test.ts, one-api-gestures.test.ts: a change no command made writes nothing)
- [x] **H3** the ids a page has held come from the applied commands (an id given and removed within one write is never
  given again) — closed S4: the keeper holds what the commands gave out, and an open page reads them with the page;
  and a page filter two writers made under one id: the keeper keeps the ids each version held, and the rebase is told
  the ones a write knew (`rebaseEdits` `known`), so a filter the writer made is given out afresh, never put in the
  other's place (page-rebase.test.ts, test_one_api.py)

### Python's copies of TypeScript rules (audit §5)
- [x] **Z1** `_with_cube` — closed S2 (early): a grid's edit is kept by headless `pageWithCube` (the cube spliced and
  its charts following it by DataCube's rule: V1, V2); `_with_cube` deleted
- [x] **Z2** `_ids` — closed S5: deleted; a page's ids are DataCube's (pageReads), as its keeper keeps them with each
  write -- every id it has held (`keeper.held`) at its start and each pull, the ids it holds now (`keeper.now`) for the
  handles it forgets, a loaded page's from pageReads -- a page filter's id among them, which Python's own left out
- [x] **Z3** `_into` / `_keep` (with Y1) — closed S1: where a model action writes is TS's (headless `pageModelInto`:
  PageModel.into); `_keep` is gone, `_into` is `_model_action`, glue that asks and applies
- [x] **Z4** `_class` action — closed S5: headless `modelClass` is Create a Class… of a grid, the window's own plan
  (model-plans.ts `classSamples`, `classAction`, over headless `gridClassed`) and model-changes' `createClass`, into
  the page's project (page-model.ts `PageModel.into`) -- its record of the page answered as the commands `modelInto`,
  `modelPin`, `modelKeep`, which Python's `grid.create_class` applies (even when refused after its project was made);
  `_class`, `gridClass`, `pageModelInto`, `pageModelNames` deleted (test_model.py OnThePage)
- [x] **Z5** `_facts` association uniqueness — closed S5: headless `modelAssociation` presets each end as the window
  does (model-plans.ts `keyUnique`: the key read through the class's relation function over the project, planned by
  the native library and run on the host's database); `_facts` and `classDuplicate` deleted (test_model.py
  `test_a_grid_made_a_class_then_an_association_and_a_data_space`)
- [x] **Z6** Create a Data Space orchestration — closed S5: headless `modelDataSpace` is the window's whole-page action
  (each grid not yet a class made one, a grid over no table of its own left out, a sample a class cannot hold refusing
  the whole), each made class kept by its full path as the window keeps it -- Python matched paths by their last
  segment; that marker is gone with it (test_model.py `test_a_whole_page_made_in_one_go`)
- [x] **Z7** `model.py` (D4) — closed S5: deleted, with `_json.protocol`; the SDLC and the writer are reached through
  the host's `fetch` (headless calls.ts, host.ts `hostFetch`), which Python answers (`_headless.answer_fetch`: its model
  home and legend-lite's writer at a path of its own, a server's address over HTTP: `sdlc=`, `writer=` are addresses
  now); model-changes' cases are recorded from the headless actions through it (test_model.py `record`;
  model-changes/test/replay.test.ts), and the writer's refusal from the host's own root says its route alone
- [x] **Z8** `Page.grid`'s command order → TypeScript builds a grid's commands — closed S5: headless `gridSetUp`
  builds them in a person's order (as `draftCommands` orders an editor's) and refuses a part it cannot read, in words;
  Python's `page.grid(...)` applies what it gives (test_page.py `test_a_grid_in_one_call_is_the_commands_datacube_orders_and_reads`)
- [x] **Z9** join input names and placement → `inputOf`, placed near its first input — closed S5: page-commands.ts
  `joinInputOf(page, grid)` reads a grid as a join does, under its tile's given title else its source's name -- the
  tab's Join… (join-flow.ts, over the page it would save) and headless `joinCube` (over the host's page and the two
  grids' ids) ask it; `joinCube` gives the addGrid command, placed beside its first input as the tab places it, which
  Python's `page.join` applies (page-commands.test.ts, test_headless.py Joins, test_page.py
  `test_placed_beside_its_first_input_and_named_as_its_grids_show`)
- [x] **Z10** take-a-reopened-cube rule (with Y2) — closed S1: cube-document.ts `cubeTaken`
- [x] **Z11** command-or-whole rule in `_kept` (with Y3) — closed S1: gone; an edit is its commands
- [x] **Z12** filter value conversion and pre-checks → the TypeScript reader — closed S5 (D12): headless grid.ts
  `readGivenFilter` reads a filter as a host writes one (a condition, a group, a negation, a list of them), each value
  untagged where JSON has no form for it (`$date`, `$decimal`, `$timestamp`: their exact text; a `$set` and a
  `$float` refused in words) -- so a timestamp is taken too; Python's `_tagged` only tags, and `_value` and
  `_given_filter` are deleted (test_page.py `test_names_and_values_outside_datacubes_words_are_refused_where_they_are_written`)
- [x] **Z13** stack needs two tiles (duplicate check) — closed S5: Python sends the stack and DataCube refuses it in its
  own words (page-commands.ts `stack`; test_page.py)
- [x] **Z14** frames-only rule (D3) — closed S5 (with X3, D14): Python's own check (`_read_page`: "which is no frame")
  deleted; what a page on the host reads is DataCube's one rule, headless host-tables.ts `leafOnHost` -- frames, files
  and remote files, any other source refused in its words until X1's rest (B7) (test_page.py `OverFiles`, test_one_api.py
  `test_a_grid_the_host_cannot_hold_is_refused_as_the_write_adds_it`)
- [x] **Z15** a snapped grid's base and refusal → exported from headless — closed S5: headless grid.ts `baseOnHost`,
  the one rule -- a snapped grid reads its copy as the host holds it, one no longer held refused in one wording (go live,
  or snap it anew), never read live in its place; any other grid, or a snapped one's live base, over the host's frames
  -- which the keeper's rebase asks (pageRebase) and Python asks for every grid call (headless `gridBaseOn`, given the
  page's copy for the grid and the copies the session holds). `_copy_base_of`, `Page._copy_base`, the session's
  `copy_of` and the `gridBase` export deleted (test_page.py
  `test_a_snapped_grid_whose_copy_is_no_longer_held_is_refused_in_datacubes_words`)
- [x] **Z16** snap refusal sentence from one TypeScript function (N3 keeps the values) — closed S5: snap.ts
  `snapRefusal(rows, limit)`, the preflight's, which Python's engine says through headless `snapRefused` with its own
  limit (snap.test.ts; test_engine.py `test_the_snap_limit_in_the_tabs_words`)
- [x] **Z17** copy ownership: one model (the keeper's), `HostSnaps` adopt / handOver and `_claimed` gone; keeper
  known limit 6 closed — closed S5: a copy is its maker's until a page the keeper keeps reads it, the keeper's from then
  on (keeper.py `has_read`, `letting_go`): it goes when no page reads it as it is, nor at a version a page open on it
  shows or any since -- each open page says the version it shows as it asks, follows and writes (`follower`, `at`:
  engine.py `_followed`, python-host.ts `PageFollower`; a hidden tab says so once a minute), silent for five minutes it
  keeps none. The grid lets go of whichever copy it reads and the host decides (engine.py `_release_copy`); the tab's
  `adopt` is `follow`, its `handOver`/`adopted` and the engine's `_claimed` are deleted (test_keeper.py, test_engine.py
  `test_a_copy_lives_until_every_page_open_on_it_moved_past_it`, plane.test.ts, live-follow.test.ts,
  datacube-app-live.test.ts)
- [x] **Z18** the saved-page record — closed S5: page-document.ts `savedRecord(id, name, page)`, the Save window's,
  which Python's `save()` asks through headless `pageRecord` (page-document.test.ts, test_page.py)
- [x] **Z19** headless exports Python never calls (`pageFilter`, `joinQuestions`) used by S6's API, stale comments
  fixed — S6, L8 (S5, B11: `joinQuestions` is asked now, by engine.py's `join.json` for the tab's Join…) — closed S6:
  `page.filter` asks `pageFilter`, which now reads the condition as Python writes a grid's (grid.ts `readGivenFilter`,
  its tests moved to that form: no caller ever sent the editor's tree it took); L8_UNASKED is empty (test_page.py
  `OverClasses`, page-filters.test.ts)

### Same job, different rule (audit §6)
- [x] **B1** page filters reach Python's `view()`, `to_pandas()`, `export()` (headless through `gridLens`) — closed S5:
  headless `gridBaseOn`, given the grid's page and id, says what the page's filters are to it (its filters and the grid's
  charts) with its base, and every grid call reads its rows through page-filters.ts `gridLens`, the screen's own rule --
  its controller's lens, its relation's (test_page.py
  `OverClasses.test_the_pages_filters_reach_its_rows_as_they_reach_the_screens_grid`)
- [x] **B2** a tile renamed to its made title stores no title: the rule inside `renameTile` — closed S1: renameTile drops a
  grid's title equal to `gridTitle(id)`; the screen sends what was typed (page-commands.test.ts)
- [x] **B3** one default page name — closed S5 (D11): a page nobody named is unnamed on both sides (''), shown as
  "Untitled page" and saved under what Save suggests (page-document.ts `suggestedName`: its own name, else its first
  sheet's `sheetLabel`, which the tab's sheet tabs ask too); the tab's host keeps no second name (`current.name` gone,
  the page's own read), its model named by one rule (`modelPageName`, the tab's windows' and the headless actions');
  `ll.Page()` makes one, Python's `save()` naming nothing asks headless `pageRecord` for the suggestion
  (page-document.test.ts, page-app.test.ts, test_page.py `test_a_page_nobody_named_is_unnamed_and_saved_under_its_first_sheets_name`)
- [x] **B4** saving under a name renames the page, on both sides — closed S5: the record names the page's content as
  it is saved (`savedRecord`), and Python, as the tab does, renames the page once it is kept (renamePage) (test_page.py)
- [x] **B5** one page-file rule — closed S5: page-document.ts `pageFile`, Export ▸ Page File's, which Python's
  `save(path)` asks through headless `pageFile`: DataCube's one-line text with a newer writer's fields kept (the tab's
  export now writes the page as the tab keeps it, as Save does), refused in one wording when there is nothing on it or
  its classes are kept in a project (page-document.test.ts, test_page.py)
- [x] **B6** empty pages allowed on both sides (D2) — closed S5: Python shows a page of no grid yet and takes a shown
  page's last grid off, followed empty as the tab's blank page is (`_showable` and `remove`'s refusal deleted); neither
  side saves one (B14) (test_page.py `test_an_empty_page_is_shown_and_never_saved`, test_notebook.py)
- [x] **B7** sources on a Python page (D3, with X1-X5) — closed S5 (X1, D13): every source kind is read on a Python
  page by one rule, headless host-tables.ts `leafOnHost`; the refusal of the rest is gone (test_page.py `OverFiles`,
  `OverClasses`, `OverSavedQueries`, `OverWarehouse`)
- [x] **B8** one rule for a detached chart going live — closed S5: DataCube's `setChart` refuses a detached chart
  (no grid shows its cube) told to follow, in its words, for Python as for the screen, whose pill is off for it
  (page-commands.ts; page-commands.test.ts, test_one_api.py `OneRule`)
- [x] **B9** one new-page skeleton (`pageNew` for the tab's Blank Page too) — closed S5 (D11): page-document.ts
  `newPage`, which headless `pageNew` and the tab's Blank Page and open-in-place (PageApp.clear, naming the page as it)
  both start from (page-document.test.ts, page-app.test.ts `cleared (Blank Page), is the new page DataCube starts`)
- [x] **B10** a frame's column change keeps grid edits everywhere (with P12) — closed S5: every view of a frame is a
  kept page, opened again over the frame's new columns as the page has it -- in Python by the page, in a tab or a
  widget by `followed` opening the page (datacube-app.ts) -- the grid's edits kept (test_show.py
  `test_a_frame_s_new_columns_keep_the_grid_s_edits`)
- [x] **B11** Join on both sides over every source kind — closed S5: the tab's Join… on a Python page (which has no
  database of its own there) asks Python each of its window's answers -- the two sides, what the join will do, the
  join's grid -- and Python answers each by DataCube's own code over its tables (engine.py `join.json`: headless
  `joinSides`, `joinQuestions`, `joinCube`, as `page.join` asks them; join-flow.ts `hostJoinFlow`), the grid it made
  opened as a saved join of the page is (`opened.json`) and added beside the grid joined. Each side joins the tables of
  the database it plans on by one rule (join-open.ts `leafRefusal` over `TAB_TABLES`, `HOST_TABLES`): a class, a saved
  query or a warehouse table is refused as a join's input on both, in the same words (§17.3: one model, one
  connection) -- a limit both share, said, not a difference between them (host-join-flow.test.ts, test_page.py
  `Joined.test_a_tab_on_the_page_asks_python_each_of_its_join_windows_answers`)
- [x] **B12** ids held (with H3) — closed S4: one held set -- the keeper's, from the applied commands, which the tab
  reads with the page and its state holds (`PageApp.restore`'s `held`) and Python takes with it -- so neither side gives
  out an id the keeper has said it held (test_one_api.py, page-app.test.ts, datacube-app-live.test.ts: F23). A write
  that leaves the page as it was holds its ids with no new version; an editor that then gives one out has it given out
  afresh by the keeper
- [x] **B13** one saved record per page: the record's id kept in the page document, so both sides save to it — closed
  S5: page-document.ts `record` (read, written, kept by every command, never in a page file), set by the `savedAs` page
  command -- no step of the history -- which the tab's Save and Python's `save()` tell; `savedRecord` saves over it
  (Save As: a new one); the tab's `current.cubeId` and Python's `_saved` deleted (page-document.test.ts; test_one_api.py
  `OneRule`)
- [x] **B14** one "nothing to save" wording — closed S5: page-document.ts `NOTHING_TO_SAVE`, the screen's Save and
  Page File's, which Python's `save` and `to_json` say through headless `pageSaved`; Python's own wording and the
  reader's `PageNoCubeError` (its one use) deleted (page-document.test.ts, test_page.py)

### Every source on both sides (D3)
- [x] **X1** headless `gridBase` for file, remote file, warehouse table, saved query, class (the planning code the
  browser uses) — closed S5 (D13, D14): host-tables.ts `leafOnHost`, the one rule -- a file and a remote file on the
  host's database; a class through its project at its revision in the host's model home (class-source.ts `classOpened`,
  which the tab's openedClass asks too), a saved query through its project's version in the host's depot
  (saved-queries.ts `savedQueryOpened`, the tab's openedRecord's rules 1-4), a warehouse table under the session the
  host's engine holds, signed in by a tab (D13). Every base's rows are read by the host's engine's plan-and-run (D16:
  the tab's own `pythonRunner` over host.ts `hostEngine`, answered on DataCube's thread over the call's read, D15) --
  a class's from the tables the host serves, a saved query's from its project's own (F28); Python's `to_pandas` asks
  the same plan-and-run (test_page.py `OverClasses`, `OverSavedQueries`, `OverWarehouse`)
- [x] **X2** Python reads, edits and saves a page over any source — closed S5: `Page.load` opens each cube over its
  source as the host reads it (X1, X3), its grids read, edited by DataCube's commands and saved over the same source
  (test_page.py `OverFiles`, `OverClasses`, `OverSavedQueries`, `OverWarehouse`)
- [x] **X3** the keeper keeps the file and remote tables a page reads, as it keeps copies (`pageReads` says them) —
  closed S5 (D14): page-rebase.ts `pageReads` says a page's files (by their bytes' hash) and remote files (by URL);
  keeper.py's `HOLDS` (copies, files, remotes) each the reader's until a kept page reads it, the keeper's after; the
  engine's `/forget.json` keeps a file a kept page reads, and its sweep drops it when nothing reads it any more; a remote
  view is kept, never dropped (as before). A cube over a file or a remote file opens on the host through headless
  host-tables.ts (`HostTables`: frames, files, remotes; `onHost`), and `Page.load(..., files={name: path})` reads each
  file a page reads, checked against its hash, a file not given refused in words, a remote file mounted again by URL
  (page-rebase.test.ts, test_keeper.py `test_each_file_a_page_read_is_kept_by_the_same_rule_as_a_copy`, test_page.py
  `OverFiles`)
- [x] **X4** Python adds such grids: `page.grid(file=…)`, remote, warehouse, saved query, class — closed S6 (S5: `file=`
  and `remote=` added, each read in once — for its bytes, for its URL — test_page.py `OverFiles`, test_headless.py
  `Remote`; S6: `page.grid(cls=..., project=..., revision=...)`, `page.grid(query=id)` and `page.grid(warehouse=...,
  table='schema.table', catalog=...)`, each named as the tab records its source -- headless `givenSource`: a class by
  class-source.ts `classAt` (the page's model at its pin, or another project's class by its path at a revision), a
  saved query's record by query-store `sharedPart` -- and made by headless `sourceCube` where the host reads it
  (host-tables.ts `leafOnHost`) (test_page.py `OverClasses`, `OverWarehouse`, `OverSavedQueries`, `PublishedQueries`))
- [x] **X5** saved queries and classes through Python's project routes — closed S5: a class's project read from the
  host's model home (`/sdlc/api`) and a saved query's version from its depot (`/depot/api`), by the host's fetch, as its
  engine answers a tab; their rows the engine's plan-and-run (the frames' database, a project's own: C5)

### Capability parity (audit §7; Python's API, names reviewed with the user before S6)
- [x] **K1** sorts: replace, clear one, clear all — closed S6: `grid.sorts([...])` (a `sorts` command) and
  `grid.clear_sort(col)`, a `clearSort` command the header menu's Clear Sort now builds too (grid-edits.ts `clearSort`,
  the menu's list-making moved out: D6) (test_page.py `Abilities`, grid-commands.test.ts, menu-more.test.ts)
- [x] **K2** add one condition to a filter — closed S6: `grid.add_filter(condition)`, built by the filter editor's own
  conversion (headless `gridAddFilter`, sharing `gridFilter`'s `editorFilter`) as one `addFilter` (test_page.py `Abilities`)
- [x] **K3** calculated columns: edit, rename, remove — closed S1 with G4/G5: Python `grid.edit_calculated` (rename=) and
  `grid.remove_calculated`, test_page.py
- [x] **K4** group-level, window, child-aggregate, explode columns; the Show As and period presets — closed S6:
  `grid.calculate(..., level='group')`, `grid.window`, `grid.child_aggregate`, `grid.explode` (headless `gridColumn`: the
  column editor's whole form by column-draft.ts, a window's and a child aggregate's fields read by readGridCommand);
  `grid.grain`, `grid.show_as`, `grid.bins` by grid-presets.ts `presetCommands`, which the header menu's By Month and Show
  As now ask too (its naming moved out of app.ts `#addPreset`: D6); the screen's Bins still opens the column editor on
  the same expression (test_page.py `Abilities`, grid-presets.test.ts)
- [x] **K5** expand, collapse, collapse all — closed S1 with G1/G2: Python `grid.expand`, `grid.collapse`,
  `grid.collapse_all`, test_page.py
- [x] **K6** dimensions — closed S6: `grid.dimension(name)` as View > Dimensions uses one, `grid.drill`/`grid.drill_up`
  by dimensions.ts `drillDown`/`drillUp` (no screen gesture drills a grid's dimension today: the rule is DataCube's all
  the same), over the configuration's `dimensions` (headless `gridDimension`) (test_page.py `Abilities`)
- [x] **K7** a chart's selection; page filters — closed S6: page filters (S6 B) `page.filter`, `page.filters`,
  `page.remove_filter`; the chart's selection (S6 D) `chart.select(value, split=...)`, `chart.select(None)`,
  `chart.selection` -- the mark named as a click names it (chart-spec.ts `namedMark`), made the click's conditions
  (`markConditions`) and what the click does with them (page-filters.ts `classPick`, moved out of page-app.ts: D6),
  the grid's `refilter` then the page's `select` (headless `chartPick`); named, never toggled (test_page.py
  `Abilities`, `OverClasses`; one-api-gestures.test.ts holds the screen's click to `chartPick`)
- [x] **K8** sheets: reorder, remove — closed S6: `sheet.move(position)` (`moveSheet`), `page.remove(sheet)`
  (`removeSheet`, its tiles with it) (test_page.py `Abilities`)
- [x] **K9** drill a chart, and back up — closed S6: `chart.drill(value)`, `chart.drill_up(levels=1)`, `chart.drilled`,
  by chart-spec.ts `drillDown` and `drillUp` (the double-click's and the crumbs'), refused in the words of a new
  `drillRefusal` beside `drillLevels` (headless `chartDrill`, as `setChart`) (test_page.py `Abilities`)
- [x] **K10** Copy of Grid — closed S6: `grid.copy()`, its cube by cube-document.ts `cubeCopied` (headless `gridCopied`),
  the one the screen's copy writes, held the same by one-api-gestures.test.ts (test_page.py `Abilities`)
- [x] **K11** a chart's Open in grid and Update — closed S6: D7's one **Show Grid** replaces them, on the screen and
  in Python (`chart.grid()`, the read-only `chart.grid` property gone): a following chart's grid shown, the page as it
  is; a frozen or detached chart's grid of its own grouping and query made by chart-spec.ts `chartGrid` (its column
  across and split the rows, its values the Column Properties, its selection off the filter) in one step -- `addGrid`,
  a new `followGrid` (the chart on the grid's cube, its selection off the cube it read, a detached chart's kept cube
  let go), `setChart` (the chart following it, drawn as it was). A cube alone saves its own grid alone, so there Show
  Grid only shows the grid a following chart follows (a chart moved onto another grid would be lost on save: found by
  the test rewrite). `setChartCube`, the editing grid, its Update and the board's kept `<chart>-query` grids are deleted (headless `chartOwnGrid`; test_page.py `Abilities`,
  one-api-gestures.test.ts's four Show Grid gestures, page-commands.test.ts, page-app.test.ts, chart-tiles.test.ts,
  cube-page-shell.test.ts, verify-charts.mjs, tour.mjs)
- [x] **K12** open a saved page by id — closed S6: `ll.Page.open(id)`, the record read as the tab's Open… reads it
  (page-document.ts `savedPage`, moved out of the app: D6; headless `pageOfRecord`) (test_page.py `Abilities`)
- [x] **R1** L12's scan missed branches on the host (D23's independent review): a runner read off a source's rows or a
  grid's place, abilities destructured off the database, the database's `place` and `forgetFile`, a call made only where
  an ability is (`?.(`), a capability in a template's `${...}`, a name holding the database -- and it took only a line's
  first read, so K16's host half on the tab's line was unseen -- fixed: every read on a line, each form; 30 more places
  listed and said; self-tests for each form and for K16's half taken off (one-api-ledger.test.ts)
- [x] **R2** K16 had no test on the tab's side (the review) -- fixed: datacube-app-live.test.ts adds a grid over a class
  from a grid's own menu on Python's page, the host asked to make it and to open it; it fails with the old guard
- [x] **R3** model actions, a class and a saved query were `shared`, though they read and write through a model home or
  a query store that is legend-lite's in two builds (the review) -- fixed: `two`, held to one result by the SDLC's and
  the query store's conformance run against each build
- [x] **R4** proofs cited for what they did not run (the review: model, remote, reopen, class) -- fixed: a test of
  `classOpened` itself (class-source.test.ts), Create a Class's writing for model, the remote parity check for the tab's
  remote, the tab's own reopen (cube-page-shell, the saved-page corpus) for reopen
- [x] **R5** the remote parity check left the tab's view mounted (the review) -- fixed: dropped; Python keeps a remote
  view by its URL, as for its pages, and offers a page no way to drop one (said in the check)
- [x] **R6** the fixture's file server answered a range past the file's end with a malformed 206 (the review) -- fixed:
  416, as HTTP says (python-engine.mjs asks a range past the end, from Node -- a page's own fetch of another loopback port is refused by
  the browser's local-network rule: a 206 fails it)
- [x] **R7** `Page.delete` stopped telling the session's pages at the first that failed, and read them without a lock
  (the review) -- fixed: each told, the first failure raised after; the set added to and read under its own lock (D15:
  never held across a call into DataCube) (test_page.py: a page that fails to hear it, the page after it told; the set
  read while another thread adds to it -- each fails without its half: the second with "Set changed size during
  iteration")
- [x] **R8** declared by the review: `Page.delete` tells this session's pages only -- another session's page (another
  marimo viewer's) keeps its record, its next save refused as the tab's is (Y-8): safe; a class grid made by the host
  carries no "not shown" note of the properties it leaves out, as Python's `page.grid(cls=...)` carries none
- [x] **K13** Save As (the S7 audits' Y-6, H-9; D23) — closed: `page.save_as(name)`, the record by the Save window's one
  rule, page-document.ts `pageSaving` (Save, or Save As: the record the page was saved as left alone), which the tab's
  Save As now asks too in place of the screen's own (headless `pageRecord` takes `asNew`) (page-document.test.ts,
  test_page.py `Abilities`)
- [x] **K14** delete a saved page (Y-6, H-9; D23) — closed: `ll.Page.delete(id)`, by the store's own rules (only its
  owner's), and each page of the session saved as it told `savedAs` of none, as Open… tells the tab's page (the
  session's `page_objects`) (test_page.py `Abilities`)
- [x] **K15** list the saved pages (Y-6, H-9; D23) — closed: `ll.Page.saved(search, sort=, limit=)`, the search Open…
  asks, cube-store.ts `savedSearch` and `SAVED_SORTS` (the library's own moved out of the window; headless
  `pageSearch`), answered by the store's rules (cube-library.test.ts, test_page.py `Abilities`)
- [x] **K16** the tab's Add a Grid over a class on a page Python serves (Y-7, H-8; D23) — closed: the stale guard
  (`services.engine.local`) gone; the class's cube made by the host where it reads the class (engine.py `source.json`:
  headless `sourceCube`, page.grid(cls=...)'s own; python-host.ts `made`), then opened as every grid of that page is
  (`opened.json`) (test_page.py `OverClasses`, python-runner.test.ts)
- [x] **K17** a class of another project, at a revision, in the tab (Y-7, H-8; D23) — closed: the source picker's Classes
  (a class by its path or name, a project, a revision), named by the one rule Python's `cls=` is, class-source.ts
  `classGiven` (moved out of headless givenSource); opened here where the page plans its own SQL, by the host on
  Python's page (class-source.test.ts, datacube-app-live.test.ts)

### Found by the audits (rule 9: each added here, fixed with a test that fails without it, closed in that commit)
- [x] **F1** a chart's selection taken off by a grid edit made while its click's refilter was in flight (the page's
  `select` is told at once, the grid's `refilter` when it commits): made again on top (`rebaseEdits`), or a host's
  `setChart` meanwhile, dropped it, the filter then holding a condition no chart owns — closed S2: `followedPage` judges
  a selection only when its cube's filter moved (page-commands.test.ts) — the S2 audit's 1
- [x] **F2** a chart removed (or its sheet) took its selection off its cube's filter without judging that cube's other
  charts by it — closed S2: `withoutTile` follows the cube after (page-commands.test.ts) — the S2 audit's 5
- [x] **F3** `configure` giving `initialExpandToLevel` again as it is reopened the groups closed under it (Python's
  configure names every setting it is given; the screen's Properties sends only what changed) — closed S2: the tree
  moves only when a setting that decides it changes (grid-commands.test.ts) — the S2 audit's 4
- [x] **F4** Python's handles given out afresh in a chain (sheet-2 -> sheet-3, sheet-3 -> sheet-4) swapped ids and one
  left the page's handles — closed S2: every moving handle taken off before any is put back (test_page.py) — the S2
  audit's 6
- [x] **F5** a tile or sheet Python made inside a call an open page wrote under was handled under the id the write took,
  not the one the page gave it afresh — closed S2 (a147d256c): `Page._apply_page` answers with the ids the commit gave
  (test_page.py, the sheet made while the open page made one) — found by the keeper's soak at 60 seeds
- [x] **F6** a cube alone's page state went stale as its grid's own changes landed (a view, a selection its filter lost),
  and the next edit of its board brought back a selection the filter no longer held — closed S3: the board's state kept
  as each view lands and each presentation change (app.ts `#keepBoard`; cube-page-shell.test.ts F6) — the S3 audit's 1
- [x] **F7** a cube alone's last chart removed threw (its board taken down while the step that removed the chart was
  still being drawn) — closed S3: the board says it is empty once `sync` has done (cube-page.ts `#afterRemove`;
  cube-page-shell.test.ts F7, F8, a listener's error caught) — the S3 audit's 2
- [x] **F8** a cube alone gave an id out again after its board was emptied — closed S3: the ids its board has held are
  kept when it goes (app.ts `#boardHeld`; cube-page-shell.test.ts F7, F8) — the S3 audit's 3
- [x] **F9** a layout gesture made while an editing grid was open gave the editor's share to a neighbour; an
  arrangement made then was told as a whole `layout` — closed S3: the editing grid is drawn in a band of its own below
  its chart's (cube-page.ts `#drawn`), and an arrangement is told as `arrange` of the page's tiles
  (cube-page-shell.test.ts F9, two: the same page with the editor open or not) — the S3 audit's 4
- [x] **F10** an edit made while `follow()` waited for another editor's snap was lost from the page's state (the state
  set after the wait) — closed S3: the state is the commands' page before the grids read their rows (page-app.test.ts
  F10) — the S3 audit's 5
- [x] **F11** a detached chart's Update changed the board itself (its kept query, the chart's link) before telling,
  and undone left the page mixed -- the spec back, the query not — closed S3: Update tells `setChartCube` and
  `setChart`, the grid it made waiting; `sync` has the chart read the query the step holds (the Update's, one kept, or
  one made again from the step's document: an Update again, undone); a page state's reachable ids hold its cubes
  (cube-page-shell.test.ts F11) — the S3 audit's 6
- [x] **F12** a cube alone's told commands replayed to another page than it saves — closed S3, declared: they replay to
  the board it shows; what it saves is that less what a cube alone cannot save (its added grids and the charts that
  read them: a cube alone saves one cube) (cube-page-shell.test.ts F12, pinned) — the S3 audit's 7
- [x] **F13** Go Live refused for a grid whose cube the page cannot hold (its snap applied, its copy dropped) — closed
  S3: snap and live are told only for a grid whose cube the page holds; a cube alone's grids have their own documents
  (cube-page.ts `documentOf`; page-app.test.ts F13) — the S3 audit's 8
- [x] **F14** a cube alone's added grid renamed to its id's number was put back to its count's name — closed S3: one
  rule names a grid with no title of its own, its id's number (page-ids.ts gridTitle), on a cube alone's board too
  (cube-page-shell.test.ts F14) — the S3 audit's 9
- [x] **F15** undoing an edit unrelated to a chart put back a selection its grid's own filter edit had taken off — closed
  S3, declared under N2: an undo puts the page at a whole step, so a grid's own change made since is not carried into
  the steps; it goes with the proper live-editing model (§8: undo as inverse commands). Not new since S2 -- the S3
  audit's 10
- [x] **F16** a chart with no spec yet moved to the bottom of its sheet at each step; a drag in progress ended at
  another editor's step that left its sheet as it was — closed S3: drawn beside its grid (`#drawn`); a sheet is drawn
  again only when it moves (cube-page-shell.test.ts F16, two) — the S3 audit's 11. A chart with no spec yet whose id
  another editor's step gives a chart of its own is shown as theirs (its id was never told): left, an edge with no
  path to it in practice
- [x] **F17** a grid whose addition was refused threw after letting it go — closed S3: let go, its owner told, nothing
  thrown (cube-page-shell.test.ts F17) — the S3 audit's 12
- [x] **F18** a chart's selection was dropped when its grid's filter, read back from another writer, held the
  selection's condition with its keys in another order (and a refilter could not take that condition off): two filter
  nodes alike but for the order of their keys were told apart — closed S4: nodes are compared with their keys in order
  (snapshot.ts `sameValue`; page-commands.test.ts F18) — found by S4's keeper soak
- [x] **F19** the tab left the page Python serves when its start opened nothing -- an empty page (`ll.Page` shown before
  its first grid), a first read that failed -- and so followed it no more — closed S4: only New ▸ Blank Page leaves it
  (datacube-app.ts `blankPage`'s `asked`; datacube-app-live.test.ts) — the S4 audit's 1
- [x] **F20** a grid over a source the host cannot base its edits on (a file, a warehouse table, a saved query, a class)
  was taken onto a page Python serves, and its every edit then refused — closed S4: one rule of what a page on the host
  holds (headless grid.ts `onHost`), which the keeper asks of each cube a write adds or moves, refusing it as it is added
  (page-rebase.test.ts, test_one_api.py) — the S4 audit's 2. Such grids come to a page on Python with S5 (B7, X1-X5)
- [x] **F21** a page filter no command made under its keeper (a page loaded or served whole), set again by a writer,
  became a second filter — closed S4: what a page reads names its page filters' ids too (page-rebase.ts `pageReads`), so
  the ids a write knew carry them (page-rebase.test.ts) — the S4 audit's 3
- [x] **F22** a follow on its way when the page was left threw, or put Python's page back over the tab's own with no
  channel — closed S4: the follow writes through the channel it began with and lands nothing once the page is left, and
  Blank Page overtakes an open on its way (datacube-app-live.test.ts, three) — the S4 audit's 4
- [x] **F23** B12's tab half had no test, and its Python test could not fail — closed S4: each has one that fails without
  it (page-app.test.ts, datacube-app-live.test.ts, test_one_api.py); a write that leaves the page as it was holds its
  ids with no new version, which neither editor hears of -- one either gives out is given out afresh by the keeper (said,
  in As built) — the S4 audit's 5
- [x] **F24** a write made more than STEPS_KEPT versions behind is refused, where before S4 it was made again on top —
  closed S4, declared: the keeper keeps the ids each version held only as far back as it keeps its steps, and a write
  from further back cannot be told what it knew (test_keeper.py, pinned) — the S4 audit's 6. Lifted 2026-10-24 by the
  middle version: a write needs nothing of its version now (each editor's ids are its own), so one from any version the
  page had is made again on top
- [x] **F25** an undo or a redo that lands while the tab follows the other editor is not written; edits made while a
  write is on its way are dropped when the keeper makes another page of it — closed S4: the dropped edits are said
  (live-channel.ts; live-channel.test.ts); the undo while following declared (the channel takes nothing done while
  following for its own; pinned) — the S4 audit's 7, not new since the keeper's step 2
- [x] **F26** comments and docs stale since S4 (the app's, python-host.ts's, the headless entry's, the keeper's As
  built) — closed S4 — the S4 audit's 8
- [x] **F27** a failure of the engine's while it took an open page's write was said as no such page (a KeyError) or as a
  refusal (any other) — closed S4: said as a failure, 500, naming it (engine.py `_page_said`; test_one_api.py) — found
  reviewing S4's own code
- [x] **F28** a class's grid on a Python page read its project's sample rows (Create a Class's `Data` elements) when its
  table was not served -- plan-and-run fell back to a project's database whenever the frames refused a model, a
  fallback (AGENTS.md invariant 4) the tab never makes; model actions' key facts the same — closed (D18's review, 1):
  a saved query's base says its rows are its project's (planner.ts `ModelOptions.project`, carried by plan-and-run and
  the dry run -- from the tab, from a headless grid's runner, and from Python's own `to_pandas`, which sends what
  DataCube's base says); any other model reads only the tables the engine serves, refused in words naming each one not
  served, of any schema (engine.py `_plan_and_run`, `_plan`, `_unserved`; frames.py `unserved`) (test_page.py
  `OverClasses` `test_a_class_whose_table_is_not_served_is_refused_never_read_from_its_sample_rows`, test_engine.py
  `ProjectQueries`, python-runner.test.ts)
- [x] **F29** opening a warehouse table waited on the compiler's pool from DataCube's thread (`_warehouse_table`'s
  direct pool calls): four tab requests waiting for DataCube's turn would hang the process — closed (review, 2): on the
  compiler's threads in place (test_page.py `OverWarehouse` `test_a_warehouse_grid_is_read_while_every_compiler_thread_waits`)
- [x] **F30** a read that began before a frame changed noted the frame's old model, saying it changed back (and on again
  at a tab's next query) — closed (review, 3): plan-and-run notes the frames' models as last read (`Frames.models`,
  which reads no frame again: a live frame is still read once a query)
  (test_headless.py `OneRowsPath` `test_a_read_that_began_before_a_frame_changed_says_no_change_of_it`)
- [x] **F31** As built (D15, D16) said a read keeps reading a table dropped after it began; plan-and-run plans against the
  models served now, so such a query is refused — declared (review, 4): the claim corrected; a read sees the rows of the
  tables it began with, and a query over a table dropped since is refused in words (F28's); the copies a grid's base is
  told of are those held now. The refusal is the rule (D22)
- [x] **F32** the ledger's "a source kind is read on the host" check passed for every case but the last whatever it did
  (it read on into the next case's `return`) — closed (review, 5): each case read up to the next (one-api-ledger.test.ts;
  a `class` case that throws now fails it, checked)
- [x] **F33** the warehouse stand-in answered the same rows for any SQL; the saved-query test never runs Python's own depot
  route — closed in part (review, 6): the stand-in runs the planned SQL on a DuckDB of its own, and its grid's view and
  filter are checked (test_page.py `OverWarehouse`); the saved-query half closed S6: test_page.py `PublishedQueries`
  makes a project by Create a Class…, reviews, approves and commits its workspace, publishes a version, saves a query
  of it and adds a grid over the query (X4), its model read through the host's own `/depot/api`, nothing stood in for
- [x] **F34** every Python grid call over a warehouse table opens it again (lists the warehouse's objects, writes and
  holds its model) — declared (review, 7): a cost, kept so a grid reads the table's columns as they are now; S7 measures it
- [x] **F35** a Python call's queries waited on a tab's query in flight, which held the frames' lock while its SQL ran —
  closed (review, 8): a tab's plan-and-run runs in a read of its own (`Engine._plan_and_run`), the lock held to begin it
  and while a query plans, never while its SQL runs; said in As built (D15, D16)
- [x] **F36** a query that failed as it ran (a value that does not convert) aborted the read's transaction, and every later
  query of the call failed — closed (review, 9): the read begins and pins another (`Reading.execute`) (test_headless.py
  `OneRead` `test_a_query_that_fails_as_it_runs_leaves_the_reads_next_query_able_to_run`)
- [x] **F37** B11's wire between the tab and Python (python-host.ts `join`, its body and its answers read) had no test —
  closed (review, 10): python-runner.test.ts (each ask's path, token, body, `how` as the protocol writes it, answers read,
  a refusal said); the item's narrower scope is said in B11
- [x] **F38** a call into DataCube from DataCube's own thread waited for ever on its caller's turn instead of being refused
  — closed (review, 11): refused before the turn is asked for (test_headless.py `OneRead`
  `test_a_call_made_from_datacubes_own_thread_is_refused_never_left_waiting`)
- [x] **F39** stale words since D15 (the lock order, X1's text, `_answer_here`, docstrings), a write inside a read refused
  only by two of the writers, and no test of that refusal or of `shown_engine`'s — closed (review, 12): corrected;
  `register` and `unregister` refuse too (test_headless.py `OneRead`
  `test_nothing_is_written_inside_a_read_on_its_thread_and_a_page_is_shown_only_with_datacubes_site`)
- [x] **F40** a join's dead `engine` field (headless join.ts); `ll.show(df)` on an install with no DataCube site made a cube
  that failed only when asked its link — closed (review, 13): the field gone; showing is refused in words first (the
  test of F39); 669caa699's message claimed every target green where As built (S5) says two failed and pass since: said
- [x] **F41** chart-tiles.test.ts opened charts and grids without waiting for them (`app.openChart()`, `app.newGrid()`
  not awaited, then a fixed number of turns): the first opens the board, whose code loads lazily, and on a slow machine
  (CI's macOS, 821615817) the load outlasted the wait, so the first test read one chart where it made two — closed
  (PR #64's CI): each call awaited; with the board's code made 300 ms slower to load the old test fails as CI did and
  the new one passes
- [x] **F42** a page filter with a number in it (`notional > 90`) broke every read of the page's grids from Python: the
  headless lens read the filters exactly (an `ExactNumber`), the filter reader refused it, and the refusal's own words
  failed to print it — S5's B1, tested with text values only — closed (S6, found testing K7): the lens reads them with
  plain numbers, as a page's filters are read (page-document.ts `plainNumbers`, grid.ts `lensOf`) (test_page.py
  `OverClasses` `test_a_page_filter_is_set_read_and_taken_off_as_the_screens_add_a_filter_sets_one`, failing without it)
- [x] **F43** a chart's click on a grid over a class compared the chart's page filter with the mark as JSON text: a page
  read back writes a condition's keys in its reader's order, so after a reopen the same mark clicked again was taken as
  another -- its filter set again, never taken off -- closed (S6, found building K7): page-filters.ts `classPick` (the
  click's rule, moved out of page-app.ts) compares them as data (snapshot.ts `sameValue`), and so does headless
  `chartPick` with the chart's selection (page-filters.test.ts, failing without it)
- [x] **F44** a saved query of a DataCube-made project answers no rows on Python: the version's own database is made by
  the DuckDB dialect's DDL, which spells an unquoted column upper case (`DuckDb.ddlIdentifier`, 2026-09-22), so a
  grid's query selecting `t0.desk` unaliased is answered `DESK`, which its plan does not type (`UntypedColumn`) —
  declared (S6, found by F33's end-to-end test): core's dialect, not this line's; said in IN_FLIGHT for the dialect
  line; test_page.py `PublishedQueries` asserts the refusal, and fails when it is fixed
- [x] **F45** a grid's undo or redo of a presentation change (a width, a pin, a format) told nothing: the keeper, Python
  and any other tab kept the change the screen undid (S7 audit G-1) — fixed S7: the owner's presentation event says
  when an undo or a redo landed it (`moved`) and the grid writes the page whole, a restore (N2), as a move that queries
  does (one-api-gestures.test.ts: a presentation edit undone, and undone and redone)
- [x] **F46** following another editor's removal of a chart with a selection, the board made up a grid `refilter` and told
  it, so the host took the selection off twice (and the person's own Remove sent `select` and a `refilter` beside
  `removeTile`, whose rule takes it off too) (S7 audit P-1) — fixed S7: the screen's Remove tells `removeTile` alone,
  as Python's does; the board's grid follows a step that removed or moved a chart (ChartSource.unselected: another
  editor's change, as it were, never told back) (page-app.test.ts, cube-page-shell.test.ts)
- [x] **F47** a page its other editor serves with no grid was never put on the tab's screen, and the tab then wrote its own
  blank page against Python's version (S7 audit H-1) — fixed S7: a served page opens whole as its keeper has it, a page
  of no grid too, and a start that opened it blanks nothing (datacube-app-live.test.ts)
- [x] **F48** on a host's page the tab opened a saved file grid by asking the person for the file the host already holds
  (the start hung on a hidden button), and told `rebuilt` only for frames and joins (S7 audit H-3, H-2) — fixed S7: on a
  host's page every saved cube but a frame's is opened by the host (`DatabaseService.opened`: headless gridOpened, its
  `taken` and `how`), a file over the table it holds (cube-page-shell.test.ts)
- [x] **F49** a new grid's tree was made three ways: the screen's grid opened to its settings' level, Python's copy and
  Show Grid not (S7 audit G-2, P-2) — fixed S7: cube-document.ts `newGridTree`, the one rule (one-api-gestures.test.ts:
  a copy of a grid opened to a level)
- [x] **F50** Show Grid's step was spelled twice and differed: on a grid over a class the screen kept the chart's page
  filter, Python took it off (S7 audit Y-1, P-3) — fixed S7: `followGrid`'s rule takes the chart's selection and its
  page filter off, as a click on nothing; the step on both sides is addGrid, followGrid, setChart (page-commands.test.ts,
  test_page.py OverClasses)
- [x] **F51** the screen's click compared a chart's selection as JSON text, its own rule beside Python's (S7 audit Y-9) —
  fixed S7: page-filters.ts `chartPicked`, the one rule the click, the chip and headless chartPick ask (page-filters.test.ts;
  page-app.test.ts: the same mark again, its keys in another order)
- [x] **F52** the page's saved record was kept by the host beside the page, `savedAs` told but never applied, and a
  rename set then told (S7 audit H-4, P-4, P-5) — fixed S7: PageApp applies `savedAs` and `renamePage` by the one rule
  (no step) and its state holds the record; the host's copy is gone (page-app.test.ts)
- [x] **F53** a grid another editor added while the tab's bar was folded was folded in the tab's state, by no command (S7
  audit G-3) — fixed S7: only a grid the tab makes is folded (page-app.test.ts)
- [x] **F54** a snapped grid whose copy the tab could not read at open read live, said, with no `live` told (S7 audit G-3)
  — fixed S7: told once the page is open
- [x] **F55** Python's join went on the first sheet, not beside its first input (S7 audit Y-3) — fixed S7: joinCube places
  it beside its first input unless a sheet is named; the tab asks the same (test_page.py Joined)
- [x] **F56** Python's page file of an unnamed page said '' and ended in a newline of its own; the tab named it in screen
  code (S7 audit Y-4) — fixed S7: page-document.ts `pageFile` names it as Save does and names the file; Python writes it
  as given (page-document.test.ts, test_page.py)
- [x] **F57** Python's load refused a Cube File the tab's Open… opens (S7 audit Y-5) — fixed S7: headless pageRead reads a
  saved file as the tab does (readSaved, savedPage) (test_page.py)
- [x] **F58** Python's save made again a record deleted in Open…; a cube saved alone, opened by its id, saved as a second
  record (S7 audit Y-8, H-7) — fixed S7: refused as the tab's Save is; pageOfRecord holds the record it was opened from
  (test_page.py)
- [x] **F59** the keeper admitted a tab's write by the frames it serves, never Python's (S7 audit Y-10) — fixed S7:
  Frames.unadmitted for both; test_one_api.py's L9 now checks every settle passes it
- [x] **F60** Python kept the rule that a snapped grid is not opened again over its frames (S7 audit Y-2) — fixed S7:
  page-commands.ts `opensAgain` (rebuiltCommands' rule), headless cubeOpensAgain (page-commands.test.ts)
- [x] **F61** the grid's Export ▸ Cube File wrote the page by its own code, not pageFile's (S7 audit G-4) — fixed S7: by
  pageFile (no behaviour a cube alone reaches today differed: one rule now)
- [x] **F62** a sheet renamed to the label it shows told nothing on the screen and pinned the name in Python (S7 audit P-7)
  — fixed S7: inside renameSheet, as renameTile drops a grid's made title (B2) (page-commands.test.ts)
- [x] **F63** a copy of removeTile's rule in the board for tiles it could not open (S7 audit P-8) — deleted S7
- [x] **F64** a dead `replacing` parameter of the tab's open-in-place, which would have cleared a served page and left it
  served (S7 audit H-12) — deleted S7
- [x] **F65** a page opened with a class or join it cannot open: the tab left it out, Python refused the whole page (S7
  audit H-6) — fixed S7: page-document.ts `leftOutWhenUnopened` and `leftOutNote`, the tab's rule and headless
  gridOpened's `leaveOut`, which Python's load asks (test_page.py OverFiles, OverClasses)
- [x] **F66** Collapse All was offered only for a group opened by hand, never a grid opened by its level (S7 audit G-5) —
  fixed S7 (one-api-gestures.test.ts)
- [x] **F67** Python's Sheet.move spelled moveSheet's refusal itself (S7 audit Y-11) — fixed S7: DataCube's words
- [x] **F68** what the ledgers missed (S7 audits H-10, G, P, Y): a host write hidden by an inner function that tells, any
  assignment of the host's `current`, an optional call of a listed gap, savedAs as a told command, the grid's owner
  reached by another name, `ChartSource.unselected`'s callers, the keeper's admission argument — fixed S7: the scans see
  them (one-api/scan.ts ownBodyAt; one-api-ledger.test.ts; test_one_api.py L9), reasons worded wrong corrected; no
  ledger grew
- [x] **F69** what the S7 audits found that is D19's own (the single writer's client side): the tab's Save on a served page
  saves the screen's page, not the keeper's (H-5); a reopen within the channel's wait drops the person's last edit,
  unsaid (H-11); an undo's restore is judged by fewer rules than a write's and carries no held ids (H-13) — moved to D19,
  whose scope they are
- [x] **F70** declared by the S7 audits, each by nature or a known limit: Python's export is the grid alone (G-4: the
  screen's carries charts the browser draws); presentation gestures are the screen's, Python sets them by `configure`
  (G-5); a dimension drilled and Bins' preset columns are Python's, the column editor's JSON picker the screen's (G-6,
  K4, K6); `grid.expand` takes a group's keys as the database writes them, unchecked (G-7); a refused board edit puts the
  board back only at its next sync, no refusal being reachable today (P-6); PageApp's name, filters and model are fields
  beside its state, set from it at every step (P-9). Asked of the user at S7's close: Python's Save As, delete and list
  of saved pages (Y-6, H-9: their names), and the tab's class grid on a Python host and a class of another project (Y-7,
  H-8: a gesture, or an exception) -- built, D23: K13-K17
- [x] **F71** a header's sort click was listed "by nature" the screen's, Python reaching its states another way (the
  user, 2026-10-22: "Why are clickSort exempt? ... There should be single sort API that UI actions call that also can be
  called programmatic") -- and the click's first direction was worked out in screen code -- fixed S7: header-sort.ts
  `headerSortCommand`, the one rule the screen's header click and Python's `grid.toggle_sort` / `grid.toggle_group_sort`
  ask (headless gridHeaderSort); L8_KINDS' two "nature" entries gone (grid-commands.test.ts; test_page.py)
- [x] **F72** `measures`, a command kind only Python sent, no screen gesture making it -- fixed S7: deleted; Python's
  measure and page.grid's measures are made by the steps a person makes one by (grid-measures.ts `measureCommands`:
  Column Properties' `column`, or the column editor's `calculate` and then `column`), judged by the one rule Show Grid's
  measures are made by too (grid-edits.ts `measurePlan`); page.grid's set-up judges them against the grid its steps
  before made (headless gridSetUp, gridMeasure); L8_KINDS 1 -> 0 (grid-commands.test.ts; test_page.py)
- [x] **F73** F48's closure sent every saved grid of a page on a Python host to the host, the tab's own pages too, and the
  host refuses a file it does not hold: such a page no longer opened (the S7 confirmation audit's H2-1) -- fixed S7: only
  a grid of the page its other editor serves is opened by the host (`reopened`'s `served`); a page of the tab's own opens
  its sources the tab's way (cube-page-shell.test.ts)
- [x] **F74** F52's closure put the saved record and the name in the page's history, so an undo past a Save took them
  back -- the next Save made a second saved page (the S7 confirmation audit's H2-2, B2-1) -- fixed S7: neither is a step,
  both carried across an undo and a redo as the page's model is (page-app.test.ts)
- [x] **F75** F46's closure followed the person's own removal of a selected chart as another editor's change: the grid's
  whole undo history wiped, and a click still running told its selection after the removal had taken it off, so the host
  kept it (the S7 confirmation audit's G2-2, B2-2, B2-3) -- fixed S7: a step of the person's own (Remove, Show Grid, a
  page undo) takes the selection off the grid as a step of its own history, told nothing more (a second removal would
  take a condition put there by hand off too: the S1 audit's 5) -- but while an edit of the grid's own still runs, told
  after it, as the grid's own; another editor's step is followed as before (ChartSource.unselected's `theirs`)
  (page-app.test.ts; one-api-gestures.test.ts)
- [x] **F76** the grid's own calls of `applyGridCommands` were invisible to every scan (the S7 confirmation audit's G2-3)
  -- fixed S7: pinned (one-api-ledger.test.ts)
- [x] **F77** F45's closure left what a presentation change laid on a change in flight kept to tell again after the
  restore (the S7 confirmation audit's G2-1) -- fixed S7 (cube-transactions.test.ts)
- [x] **F78** F66's closure offered Collapse All where nothing is open (a grid of one row group opened to a level: the
  level never opens the deepest), and its gesture drove that case (the S7 confirmation audit's G2-4) -- fixed S7:
  tree.ts `opensAny` (tree.test.ts; one-api-gestures.test.ts: two row groups)
- [x] **F79** F49's headless half had no test that fails without it, and the gesture floor had not followed the
  catalogue (40, 51 held) (the S7 confirmation audit's G2-5) -- fixed S7 (test_page.py: Show Grid's own grid opened to
  its source's level; GESTURES_AT_LEAST 51)
- [x] **F80** the screen's sheet label left out a grid the page could not open, the rule counted it: a rename to the label
  shown was dropped (the S7 confirmation audit's B2-4) -- fixed S7 (page-app.test.ts)
- [x] **F81** a share link carried the saved record of the person's store (the S7 confirmation audit's H2-3) -- fixed S7:
  none, as a page file carries none (share-link.test.ts)
- [x] **F82** F54's closure told `live` judged against the copy, so a grid over a frame changed meanwhile was never told
  `rebuilt`; and a detached chart's cube was told `live`, refused, the page opened again, told again (the S7 confirmation
  audit's H2-4) -- fixed S7: `live` first, `rebuilt` judged after it, as Python's Grid.live tells both; no `live` for a
  cube no grid shows (datacube-app-live.test.ts)
- [x] **F83** the empty page's Add a data source, on the page Python serves, opened a page of the tab's own in its place
  (the S7 confirmation audit's H2-6) -- fixed S7: added to it, as New ▸ Data Source… adds (datacube-app-live.test.ts)
- [x] **F84** F65's closure left a source out on a refusal only; a join that no longer joins fails as its plan does:
  Python refused the page the tab opens (the S7 confirmation audit's Y2-1, H2-7) -- fixed S7: any failure, as the tab
  (test_page.py Joined)
- [x] **F85** F65's closure opened in Python a page none of whose sources opened, which the tab refuses (the S7
  confirmation audit's Y2-2) -- fixed S7: page-document.ts `noneOpened`, the tab's rule, headless pageNoneOpened, which
  Python's load asks (test_page.py OverClasses)
- [x] **F86** Python's load kept a document's saved record, so a page loaded from another's document saved over that
  one's (the S7 confirmation audit's Y2-3) -- fixed S7: a page read from a document holds none, as the tab's Open… of a
  file; Page.open holds the one it opened by its `savedAs` (test_page.py)
- [x] **F87** Python refused a name that is not text in words of its own (the S7 confirmation audit's Y2-4) -- fixed S7:
  page-names.ts givenName and the commands' readers refuse it, and arrange's refusal is DataCube's alone (test_page.py)
- [x] **F88** Python's save decided over-or-new itself (the S7 confirmation audit's Y2-5) -- fixed S7: page-document.ts
  `savesOver`, the tab's Save's rule, headless pageRecord's `over` (no behaviour differed: one rule now)
- [x] **F89** Python's own page written whole was admitted by no rule, and no scan saw it (the S7 confirmation audit's
  Y2-6) -- fixed S7: Frames.unadmitted, as every write; test_one_api.py's L9 checks every restore asks it
- [x] **F90** declared by the S7 confirmation audit: a file grid of the page Python serves, opened again on the host,
  offers Snap and Go Live where one added shows Snapped -- the host's answer says no read time or row count to show
  (H2-5); a host's refusal said on the tab carries the exception's kind (`CubeRefusal: `), as every 422 of the host's
  does (H2-7); another editor's removal followed while the person's click still runs is two editors at once: D19's own
- [x] **F91** F75's closure kept the person's own removal out of the grid's undo stack but did not record it: an undo
  past it put the removed chart's selection back and took the edit before it away (the S7 confirmation audit's third
  round's 1) -- fixed S7: a recorded step of the grid's history, told nothing (`#applyCommands`' `said`) (page-app.test.ts)
- [x] **F92** F71's one sort rule looked headers up in the view's raw columns, which leave a grouped column out that Show
  Grouped Columns paints: its header no longer sorted (the third round's 2) -- fixed S7: the headers the screen paints
  (shownColumns), on the screen and in headless gridHeaderSort alike (app-dom.test.ts)
- [x] **F93** F75's race fix told the removal's refilter whenever the grid ran any edit, so a condition put there by hand
  came off twice on the host (the third round's 3) -- fixed S7: told only while the click that put the selection there
  is still to be told (page-app.test.ts)
- [x] **F94** F87 left Python refusing an arrangement's shape and a calculated column's name in its own words (the third
  round's P2) -- fixed S7: DataCube's readers refuse them; an expression, which Python makes its lambda's text of, alone
  is checked in Python (test_page.py)
- [x] **F95** an unnamed measure was named after its column by Python and by headless gridSetUp, each (the third round's
  P3) -- fixed S7: grid-commands.ts readMeasure's default, the one rule
- [x] **F96** declared by the third round: page.grid's set-up read the frames even with no measure to judge (fixed: only
  then); a value JSON cannot carry (a path, bytes) is refused by the transport, a TypeError, before DataCube can judge
  it (P1); the L9 restore scan now holds each restore to an admission of its own reads (P5); two doc comments put back
  above their functions
- [x] **F97** a selected chart removed while the click that selected it still runs: the click's `select` is told at once,
  its grid's refilter when its query lands, so a condition put on the grid by hand too, or another chart's equal
  selection, comes off twice on the host (the S7 audit's fourth round's 1, 2); and a page undo's restore written before
  its grid's removal lands, or a removal refused and not run again after signing in, leaves the host the selection --
  the tab tells its page and its grids on two schedules: moved to D19, the single writer's client side, whose scope it is
- [x] **F98** an edit of a calculated column naming none (Python's `edit_calculated(None, ...)`) added a new column (the
  fourth round's 3) -- fixed S7: headless gridColumn refuses it in words (test_page.py)
- [x] **F99** F92's header lookup laid the headers out by a change still running, not as painted (the fourth round's 4)
  -- fixed S7: the headers #paintView painted; and the fourth round's lows: L9 holds each restore's admission acted on
  (raised or returned), an unnamed measure of a padded column is its own Aggregation (the fix itself, said), the
  measure's form in gridSetUp's doc, F90's text finished
- [x] **A1** the columns panel's pick added a column to the row groups by a list the screen made (`[...rows, c]`, the
  column twice when grouped by already) -- fixed S8: the gesture `pivot.addVertical`, the menu's Add Vertical Pivot
  (grid-gestures.test.ts; test_page.py `add_group`)
- [x] **A2** the columns panel's tick hid or showed a column by a configure written in screen code -- fixed S8:
  `column.show`
- [x] **A3** the columns panel's reorder merged the columns' order in screen code (app.ts #columnOrder, mergeColumnOrder)
  -- fixed S8: `columns.reorder` (grid-gestures.ts columnOrderOf)
- [x] **A4** a column dragged out of a zone was taken off by a list the screen filtered -- fixed S8:
  `pivot.removeVertical`, `pivot.removeHorizontal` (test_page.py `remove_group`, `remove_pivot`)
- [x] **A5** a drop in the zones wrote its group and pivot commands in screen code -- fixed S8: `zones.set`
- [x] **A6** the grid's header drag merged the order, and showed a column dragged in from the panel, in screen code --
  fixed S8: `columns.reorder`'s `added`
- [x] **A7** a group's chevron wrote its `open` in screen code -- fixed S8: `tree.open`
- [x] **A8** a dragged column edge wrote its width's configure in screen code -- fixed S8: `column.resize`
- [x] **A9** the zones and the title bar folded by a configure written in screen code -- fixed S8: `view.chrome`
- [x] **A10** the header menu's query entries -- fourteen commands: its sorts, filters, row groups and pivots -- made in a
  screen module (ui/menu.ts menuCommand) -- fixed S8: each entry its gesture, named by the menu's own id (ui/menu.ts
  menuGesture: the id and what the entry carries of what the gesture takes) (menu.test.ts, menu-more.test.ts)
- [x] **A11** Collapse All wrote its command in screen code -- fixed S8: `tree.collapseAll`
- [x] **A12** Hide Column wrote its configure in screen code -- fixed S8: `column.hide`
- [x] **A13** Pin Left, Pin Right and Unpin wrote their configures in screen code -- fixed S8: `column.pinLeft`,
  `column.pinRight`, `column.unpin` (test_page.py `gesture`)
- [x] **A14** Measures First turned its setting in screen code -- fixed S8: `pivot.measuresFirst`
- [x] **A15** Unpin All found the pinned columns in screen code -- fixed S8: `column.unpinAll` (test_page.py `gesture`)
- [x] **A16** Auto-size's floor, ceiling and padding were screen code -- fixed S8: `columns.autoSize`, given the content's
  widths the screen measures
- [x] **A17** Minimize's widths (a fixed width kept, upstream's 50) were screen code -- fixed S8: `column.minimize`,
  `column.minimizeAll` (every header the grid paints)
- [x] **A18** Size to Fit's scaling was screen code -- fixed S8: `columns.sizeToFit`, given the widths drawn and the
  grid's own
- [x] **A19** Exclude from and Include in the pivot wrote their configures in screen code -- fixed S8: `pivot.exclude`,
  `pivot.include`
- [x] **A20** the header menu's Delete of a calculated column wrote `removeCalculated` in screen code -- fixed S8:
  `calc.delete`
- [x] **A21** a heatmap went on a pivoted leaf's measure by screen code, a rule Python could not reach -- fixed S8:
  `heatmap.add`, `heatmap.remove` (test_page.py `heatmap`)
- [x] **A22** a header click made its command in app.ts by calling header-sort.ts itself, Python by headless
  gridHeaderSort -- fixed S8: `header.sort`, the one gesture both ask (headless gridGesture; gridHeaderSort gone)
- [x] **A23** a chart's mark on its grid's filter wrote its refilter in screen code -- fixed S8: `chart.refilter`
- [x] **A24** a chart's selection taken off its grid wrote its refilter in screen code -- fixed S8: `chart.refilter`
- [x] **A25** a named dimension used grouped the grid in screen code, and Python's dimension, drill and drill_up by a
  headless function of their own (gridDimension) -- fixed S8: `view.dimension`, the one gesture both ask (gridDimension
  gone)
- [x] **A26** the column editor's Add wrote `calculate` in screen code -- fixed S8: `column.calculate`
- [x] **A27** the column editor's OK wrote `editCalculated` in screen code -- fixed S8: `column.editCalculated`
- [x] **A28** the column editor's Delete wrote `removeCalculated` in screen code -- fixed S8: `calc.delete`
- [x] **A29** the filter editor's Apply wrote its filter command in screen code -- fixed S8: `filter.set`
- [x] **A30** a tile renamed wrote `renameTile` in screen code -- fixed S8: page-gestures.ts pageCommand (the title as
  typed)
- [x] **A31** a sheet's layout told, its saved tiles alone, by a rule written in screen code -- twice: views() wrote a
  page's layouts by its own copy -- fixed S8: page-gestures.ts savedLayout and layoutCommand, the one rule both write by
  (page-gestures.test.ts)
- [x] **A32** a hand's move on the board -- a stack of saved tiles, a preset picked and the order it arranges them in --
  decided in screen code -- fixed S8: movedCommand
- [x] **A33** Add Sheet wrote `addSheet` in screen code -- fixed S8: pageCommand
- [x] **A34** a sheet's rename judged by the names' rule in screen code -- fixed S8: renameSheetCommand
- [x] **A35** a sheet's drag held to the order's ends in screen code -- fixed S8: moveSheetCommand
- [x] **A36** Delete Sheet wrote `removeSheet` in screen code -- fixed S8: pageCommand
- [x] **A37** Move to Sheet -- a new sheet made first, in the same step -- composed in screen code -- fixed S8:
  moveToSheetCommands
- [x] **A38** Snap and Go Live told by commands written in screen code -- fixed S8: pageCommand
- [x] **A39** Add Grid wrote `addGrid` in screen code -- fixed S8: pageCommand
- [x] **A40** a chart told wrote `addChart` in screen code -- fixed S8: pageCommand
- [x] **A41** a chart's spec told wrote `setChart` in screen code -- fixed S8: pageCommand
- [x] **A42** a tile's x wrote `removeTile` in screen code -- fixed S8: pageCommand
- [x] **A43** Show Grid's steps (the chart following its new grid, then drawn as it was) written in screen code, and
  again in Python's chart.grid() -- fixed S8: showGridCommands, the one rule both ask (headless pageGesture;
  test_page.py's Show Grid)
- [x] **A44** the page renamed wrote `renamePage` in screen code -- fixed S8: pageCommand
- [x] **A45** the page saved as a record wrote `savedAs` in screen code -- fixed S8: pageCommand
- [x] **A46** a page filter set wrote `setPageFilter` in screen code -- fixed S8: pageCommand
- [x] **A47** a page filter's x, in two places, wrote `removePageFilter` in screen code -- fixed S8: pageCommand
- [x] **A48** a page opened live, a snapped grid's copy unread, told `live` written in screen code -- fixed S8:
  pageCommand
- [x] **A49** no scan saw where a gesture's meaning is decided: L8 asks whether each command kind is made on both sides,
  never where (the class of F71 and F72, found by hand twice) -- fixed S8: L11, closed at 0 -- no command written out in
  a screen file; pageCommand only for the person's input; every function that makes commands the screen calls asked by
  headless too, or the screen's own, said why. Its first run found three the inventory missed: page-commands.ts
  selectionRefilters (the tab's page undo), rebuiltCommands (the tab opening a served page) -- the screen's own, listed
  -- and applyToState, an applier, not a maker (one-api-ledger.test.ts)
- [x] **A50** a gesture's arguments in screen code were an untyped record, values passed on cast: a misnamed or mistyped
  argument compiled (S8's review, 2026-10-23) -- fixed: GridGestureArgs, a control's gesture typed (gridGesture), one
  named by text checked as it comes (gridGestureGiven) (grid-gestures.test.ts)
- [x] **A51** L11 could not see a gesture's meaning decided before the call -- a list made, the grid's state read, in its
  arguments (S8's review) -- fixed: L11's fourth check (one-api-ledger.test.ts)
- [x] **A52** pageCommand wrote its fields unchecked, not as Python's builder does (S8's review) -- fixed: written from
  PAGE_COMMAND_FIELDS, in order, a field the list has not thrown (page-gestures.test.ts)
- [x] **A53** Python's grid.gesture sent a value JSON has no form for (a decimal, a date) untagged (S8's review) -- fixed:
  tagged as add_filter's are, untagged by headless gridGesture (test_page.py)
- [x] **A54** a sheet renamed to the label it shows, with no name of its own, told a rename that changes nothing (S8's
  review) -- fixed: renameSheetCommand takes the label shown (page-gestures.test.ts)
- [x] **A55** L8 credited headless pageGesture with every page command written in page-gestures.ts, not the two its
  table makes (the S8 audit's 1) -- fixed: the table and the functions it calls alone, pinned (one-api-ledger.test.ts)
- [x] **A56** L11 did not read an async function that makes commands, its last check passed whatever headless called (a
  declaration or a comment counted), and its screen files and spellings left gaps (the S8 audit's 2, 3, 4) -- fixed:
  async makers read, headless read as code, the grid's DOM, Ad Hoc mode's screens, the charts' drawing and the model
  actions' flows screen files, any quoting of a kind, a kind chosen in screen code refused (one-api-ledger.test.ts)
- [x] **A57** Python's grid.gesture could not give a gesture its own `name` argument (`view.dimension`,
  `column.editCalculated`): a TypeError (the S8 audit's 5) -- fixed: the gesture's name positional (test_page.py)
- [x] **A58** a gesture that changes nothing made a command all the same -- a group it is not in taken out, a heatmap it
  has not taken off, a pin or a width it has: recorded on Python's page and followed by an open tab as another editor's
  edit, its undo history cleared (the S8 audit's 6) -- fixed: none made (grid-gestures.test.ts; test_page.py)
- [x] **A59** Python's heatmap ran the grid's whole query for a view it needs only on a pivoted leaf (the S8 audit's 8)
  -- fixed: the view read only where a pivoted leaf can be named (grid-gestures.test.ts)
- [x] **A60** a comment and a test said what is no longer so (the menu's exhaustiveness by applyMenuAction, a test of
  one gesture titled as of each, As built's claim every gesture's commands are read), and menu-ids counted a gesture a
  menu entry cannot carry the arguments of as handled (the S8 audit's 9) -- fixed (menu-ids.test.ts)
- [x] **A61** declared by the S8 audit: menu steps named by their entries, Minimize All of the columns shown, no step for
  a gesture that changes nothing, a disabled entry's refusal said (its 7: As built's behaviour changes); Chart.grid()'s
  one more call for Show Grid's two steps, a moment's (its 8); the races, the ordering and Python's locking clean
- [x] **A62** the columns panel's pick had no test through the app (S8's review) -- fixed: app-dom.test.ts

### D19, the single writer's client side (D24; docs/DATACUBE_LIVE_EDITING_2026_10_24.md §7)
- [x] **W1** `gridBetween`, total over a grid's saved form (src/grid-between.ts): the commands that take one state of a
  grid to another, an undo's among them; the `tree` command (the open groups set whole, what puts a collapsed tree back);
  `before` on `calculate` and `editCalculated` (a column back in its place); a kind and a pivot exclusion cleared by
  `configure`; a removed calculated column's settings gone with it; a grouped calculated column keeps its kind and level
  -- closed D19 slice 1: grid-between.test.ts (300 walks of every command kind, every pair of states both ways),
  grid-commands.test.ts; Python's `grid.tree`, `calculate(before=)`, `edit_calculated(before=)` (test_page.py)
- [x] **W2** `pageBetween`, total over a page in its `history` and `whole` scopes, and `applyPlanned` with fresh or
  honoured ids (src/page-between.ts); a live chart follows its grid from the start (`addChart`, as every later change
  follows it) -- closed D19 slice 1: page-between.test.ts (three corpus pages, 120 walks each of every page command kind,
  every pair of states both ways, both scopes; an undo of a grid removal; what no command makes refused in words),
  page-commands.test.ts. The D19 audit's W2 limit (2026-10-24), closed the same day: a detached chart over a snapped
  kept cube, removed and brought back, came back over the cube live, no command setting a copy on a cube no grid shows;
  `detach` carries the copy now (`copy?`: given, the copy; null, its live rows; the field absent, the cube's copy as it
  is, so a detach that says nothing of it un-snaps nothing -- the third round's 4; Python's `page_detach(copy=)`
  generated, absent or None), the diff says it always wherever it plans a `detach` and where a kept cube's copy changes
  -- page-between.test.ts (the walk detaches with a copy at times; the case itself), page-commands.test.ts
- [x] **W3** the grid's undo produced by commands (state-owner.ts `toward`: the history's states are what this kind's own
  commands make), told as the commands between the state before and the one landed (`StateRules.between`:
  `gridBetween`), by `onCommand` as a change is; T3 ends (a change landing with an undo is told with it) -- closed D19
  slice 2: cube-transactions.test.ts (T3, G2-1: the commands told replay to the grid on screen), one-api-gestures.test.ts
  (a grid's undo, Collapse All undone -- `tree` told -- a presentation edit undone and redone: told, read by the host,
  replaying), live-channel.test.ts (a grid's undo made again on top of the other editor's write), one-api-ledger.test.ts
  (`L1_GRID`'s `#undo` / `#redo` reasons)
- [x] **W4** the page's history is commands (page-state.ts): a step `{label, forward, inverse}`, its inverse
  `pageBetween(after, before, 'history')` made with the step; an undo applies the inverse to the page as it is now, a redo
  the forward commands, each told as any edit (page-app.ts `#move`); a step whose target is gone is dead, dropped and
  said; a tile brought back is a new tile (B12), its maker and kept document re-keyed to the id it was given, every
  step's commands renamed to follow (`renamedCommand`, a maker's planned id too) -- closed D19 slice 2:
  page-state.test.ts (set semantics past another editor's change, a dead step, a tile brought back, the keeper's
  rename), cube-page-shell.test.ts (every structural edit undone and redone, the handles following the new ids),
  page-app.test.ts, one-api-gestures.test.ts (a page edit undone, undone and redone)
- [x] **W5** the restore write deleted end to end: `pageWritten` and `LiveWrite.restore` gone, `onRestore` gone,
  `LiveChannel.restored` gone, `Engine._page_said` refuses `{restore}` in words (400: "never the page whole"); N2
  retired; `L9_KEEPER` 6 -> 5 (keeper.py `restore` stays for N1, a page loaded) -- closed D19 slice 2: test_one_api.py
  (H2/W5: a page written whole is no write), test_page.py (the Live tests write edits: an undo is its commands, a tile
  brought back a new tile, a stale write made again on top), live-channel.test.ts, one-api-gestures.test.ts (the
  `restores` gestures are told gestures)
- [x] **W13** the page's history survives another editor's change (page-app.ts `follow` keeps the state: `keep`, no new
  `PageState`); the grid's starts again when another editor edits that grid (declared, app.ts) -- closed D19 slice 2:
  page-app.test.ts (follows another editor's page commands, its history kept), page-state.test.ts. The D19 audit's 4
  (2026-10-24): a converge put every touched grid back at the confirmed cube as another editor's change, so the tab's
  own combined write restarted its grid's history; now a grid only this screen's edits changed is put back at its sent
  cube with its history kept (`keepHistory`), and only a grid the keeper's steps, the refused batch or a rename touch
  starts again -- live-channel.test.ts (the follow's grid keeps its undo). The second round (below): a grid whose cube
  a PAGE step of the keeper's changes starts again too; the grids kept are put back after the steps, at their sent
  cube, their pending edits made again as no new step (`CubeApp.edit` `again`), so an undo never goes forward
- [x] **W18** H-13 closed by W5: an undo is judged by every rule a write is (its commands applied by `rebaseEdits`, its
  ids held) -- closed D19 slice 2 with W5
- [x] **W19** `detach {chart, cube, document}`, the page command that makes a detached chart reachable (a chart over a
  cube no grid shows, the id it had): the inverse of Show Grid on a detached chart (`detach`, then the grid's
  `removeTile`: the chart keeps its id) and of a detached chart's removal (`addChart` of a grid here, or of one made for
  it and taken off, then `detach`); `pageBetween` total over detached charts; Python's `page_detach` generated; no
  gesture but an undo tells it -- closed D19 slice 2: page-between.test.ts (the three cases, and every walk), page-commands.test.ts,
  one-api-gestures.test.ts (a detached chart shown in a grid of its own, undone), cube-page-shell.test.ts (D7's Show Grid
  steps, F11), one-api-ledger.test.ts (`L8_KINDS`: Python makes none); `copy?` added 2026-10-24 (W2's row)
- [x] **W6** the write's answer carries what the writer converges by: `Settled` gains `since` (the other editors' steps
  between the writer's version and the one before its own, or None), `edits` (the writer's as applied) and `hash`
  (keeper.py `page_hash`: SHA-256 of the page's canonical JSON, `_json.canonical`); the engine's 200 carries `hash`
  always and `since`, `edits` when combined; `GET /page.json` carries `hash` too; the tab reads them (python-host.ts
  `livePage`, services.ts `LiveSettled`, `LiveRead`, `LiveMoved`) -- closed D19 slice 3: test_keeper.py (Settle),
  test_page.py (Live: a stale write's since and edits are what moved the page since; the hash with every answer),
  test_engine.py, live-follow.test.ts (the host's answers read, a page said with no hash refused)
- [x] **W7** the tab converges on a combined write without a read and without a reopen (live-channel.ts; live-converge.ts
  `convergeOn`, the one rule DataCube's app and the tests use): the screen put back at the page as the keeper had it at
  the version shown (PageApp.putBack: the board by `#syncTo`, the ids given out here since given back, the history kept),
  the keeper's renames followed (PageApp.renamedBy: the grids' makers, the kept documents, every step of the history;
  a renamed grid's old id keeps nothing when its tile drops), each grid the keeper's steps touched put back at the
  keeper's cube by its own commands (`gridBetween`, as another editor's), the other editors' steps applied as theirs,
  the screen's own as the keeper applied them (a grid's as steps said; a grid added here made again under the keeper's
  id by its own maker: PageApp.knows), then the pending edits made again, renamed (`LiveScreen.reapply`: a page's and a
  touched grid's through the ordinary edit path, told again; one on a grid the steps left alone told again only); a
  history step's layout applied to the page as it is now keeps the tiles the step did not know (page-between.ts
  `applyOne` by page-rebase.ts `rearranged`, the keeper's own rule) -- closed D19 slice 3: live-channel.test.ts (the
  channel: a combined write converged, an id given out afresh with pending edits made again under it, a touched grid's
  pending edit made again; a real page: converging on top of the other editor's write, on an id given out afresh, its
  history following), datacube-app-live.test.ts
- [x] **W8** a refused write converges the same way, over what moved the page since (`changes(shown)`), the refusal said;
  the page whole (past what the keeper keeps) opens it again, said -- closed D19 slice 3: live-channel.test.ts. The D19
  audit's 1 (2026-10-24, fixed with its test there): the refused batch's grids are put back too (`Converge.refused`),
  nothing of the batch made again -- before, a refused grid edit stayed on screen and the hash check reopened the page
- [x] **W9** one write per gesture: the channel writes only once the screen is quiet (no grid's change in flight:
  `LiveScreen.busy`), so a gesture's page commands and its grid's are written together once they have all landed, and the
  keeper never holds a selected chart whose grid's filter lacks the selection (F97) -- closed D19 slice 3 in this form
  (the write waits for the gesture, in place of reordering what is told): live-channel.test.ts (waits for the screen to
  be quiet before a write)
- [x] **W10** a follow writes what is pending first and waits for the screen to be quiet (`LiveChannel.flush(true)`, in
  the channel's own queue: datacube-app.ts followLive; F90) -- closed D19 slice 3: live-channel.test.ts (a flush writes
  once quiet), datacube-app-live.test.ts (a follow after the flush). The D19 audit's 2 (2026-10-24, fixed with its test in
  live-channel.test.ts): a follow is a converge with no edits of the screen's own (`LiveChannel.follow`): an edit made
  while the keeper was asked is taken off for the hash check, kept pending and made again -- before, `opened` dropped
  it and the hash check reopened the page, the edit unsaid
- [x] **W11** Save on a served page writes first (datacube-app.ts saveTo: `channel.flush()`), then saves the page on
  screen, which is the keeper's (H-5) -- closed D19 slice 3: the one call, held by the channel's flush test
  (live-channel.test.ts); no app-level Save test on a served page (the harness drives none)
- [x] **W12** no edit dropped unsaid before any reopen (H-11): a reopen says how many changes made meanwhile were not kept;
  the two reopen causes that remain (a `rebuilt`, a write from further back than the keeper keeps), a converge that could
  not be made, and the hash mismatch are each said -- closed D19 slice 3: live-channel.test.ts (says what it drops; the
  page opened again when the keeper no longer keeps the steps), datacube-app.ts `reopen`; a follow drops nothing since
  the D19 audit's 2 (W10)
- [x] **W14** `rebaseEdits` applies a grid's run of consecutive commands in one `applyGrid` call (one open, one compile
  per run), the same reduce, refused whole, each edit listed as applied -- closed D19 slice 3: page-rebase.test.ts
- [x] **W16** a frame moved with the page is followed by its commands, then every grid over the frame refreshed, as a
  frame that moved alone is (live-follow.ts `followBy` loses its `open` arm; datacube-app.ts `followed`) -- closed D19
  slice 3: live-follow.test.ts
- [x] **W17** the hash check at every write's answer and every follow (page-hash.ts: SHA-256 over the canonical JSON,
  computed here, synchronously, held to the platform's digest; keeper.py `page_hash` the same over `_json.canonical`); a
  mismatch is the one message that must never show (live-channel.ts `HASH_MISMATCH`): the page is opened again as the
  keeper has it and the status line says so, once -- a second mismatch before a write hashes alike is said and opens
  nothing (no reopen loop) -- closed D19 slice 3: page-hash.test.ts, live-channel.test.ts (the channel; a real page's
  converge hashing to the keeper's), datacube-app-live.test.ts (a follow's hash). The D19 audit (2026-10-24): the follow's
  mismatch is said once too, now that a follow is the channel's converge (W10); and the keeper's hash is the page's at
  the version it answers for, read with the write under its lock (keeper.py `_write`; `moved`, `snapshot` for the
  engine's answers) -- before, another writer's write between the write and the hash's read made a false mismatch and
  a reopen (the audit's 3; test_keeper.py)
- [x] **W15** the two-editor soak as a gate, three times over. TypeScript: live-soak.test.ts -- two real `PageApp`s
  (jsdom, real `CubeApp`s over the fixture engine) on one keeper as Python's (test/fake-keeper.ts: every write applied
  by `rebaseEdits`, the steps kept, `since` / `edits` / `renamed` / `hash` answered, a write that lands on a moved page
  made again as keeper.py `settle` retries on `Stale`), Python's stand-in writing between them, grid edits, sheets and
  grids added, tiles taken off, sheet renames, undo and redo on each tab, each tab's writes held at random so writes
  combine and are refused; after every quiet point (every write landed, every tab followed to the keeper's version with
  nothing pending) the three hold one page to the hash, no tab opened the page again, none said the message that must
  never show; 6 seeds of 14 steps in the build (`LEGEND_LITE_SOAK_SEEDS` for more, `LEGEND_LITE_SOAK_SEED` for one
  alone, its writes logged), 60 run green. Python: test_keeper.py `Converges` -- the `OpenPage` keeps a history as the
  tab does (each step's inverse planned by `pageBetween` / `gridBetween` when it is made, Undo and Redo through
  `pagePlanned`: fresh ids, B12 -- the tab's own functions, bundled for the test alone as `legendLiteHeadlessTab`,
  datacube/test/headless-tab.ts and `//datacube:headless_tab_bundle`, loaded into the headless engine the test has;
  Python's product asks none of them, so the product's bundle exports none and `L8_UNASKED` stays empty), a second
  `OpenPage` joins Python and the first (edits alone, both at once, undo then redo), every open page followed after
  every step, the `Intent` model grown by `removeSheet`, `addGrid`, `addChart` facts. The browser: python-engine.mjs -- two tabs on one `ll.Page`, a sheet renamed in one and added in the
  other at once with Python's grouping between, then Undo through the first tab's menu; both tabs hold Python's page to
  its keeper's hash (`window.__dataPageHash`, the harness's hook) and neither says the message that must never show.
  The soak's catches, each fixed with its unit test: `LiveTarget.grids` and `cubes` read the board as each run of
  commands is applied, so a grid a step added is on it when its own commands come (live-follow.test.ts), and the
  converge puts every grid a pending edit changed back at the keeper's cube too, so the page on screen is the keeper's
  to the hash before the pending edits are made again (`Converge.pending`; `reapply(pending)` makes every pending edit
  again, the `touched` split gone) -- closed D19 slice 4

## 5. The steps

Each step: the full local set green, one CI run, an independent audit against its items, "As built" appended here.

### S0. Guards (no product change)
The ten ledgers of §3, each listing today's entries by item id; the IN_FLIGHT line. Size: about 1k lines of tests.

### S1. Every edit a command
Items: G1 G2 G4-G11 G13-G15, T1-T4, C1, P1-P4 P8 P9 P11, Y1-Y3, Z3 Z10 Z11, B2.

- **The grid's draft gains its tree.** `GridDraft` (`grid-edits.ts:21`) is `{snapshot, config}` today; it becomes
  `{snapshot, config, tree}` with the saved tree (open paths, totals, expand level, closed groups). CubeApp's
  `#applyCommands` passes its tree; headless `applied` (`headless/grid.ts:440`) applies to the saved tree instead of
  keeping it.
- **New grid commands:** `open {path, open}`, `collapseAll`, `editCalculated {level, name, column}`,
  `removeCalculated {level, name}`, `refilter {remove, add}`. Each with its rule in `grid-commands.ts`, reader, writer,
  parity row.
- **`calculate` reads and writes every column kind** (window, child aggregate, explode), so the host reads what the
  screen sends and Python can build them.
- **New page commands:** `select {chart, conditions}`, `modelInto`, `modelPin`, `modelKeep` (applied by
  `page-model.ts`'s `into` / `pin` / `keepTables` / `keepClass`), `setChartCube {chart, document}`,
  `rebuilt {cube, document}`. Each with its rule in `page-commands.ts`, its `renamedCommand` arm, reader, writer.
- **The screen tells what it does:** the name box and Save tell `renamePage`; Snap and Go Live tell `snap` / `live`;
  the model actions tell the model commands; the Filters window, a dimension and a chart click go through `#apply`;
  `retryRefused` tells the refused change's commands; CubeApp tells exactly the commands of each committed transition,
  in commit order, with who made them (T1-T4). `applyConfiguration` and public `change()` deleted.
- **Python:** `_into` / `_keep` become model commands through `pageApply`; a reopened cube becomes `rebuilt` through
  `pageApply`, with whether to take it decided in TypeScript (headless `gridReopened`).
- **Restore keeps what it cannot open** as an unavailable tile (P9), so no document loses a view silently.

Size: about 3k lines. Risk: the tree in the draft changes how open-group changes move through the state owner (today a
presentation change); keep them presentation changes, pinned by the existing undo-coverage test.

### S2. Derived state by one rule
Items: G3, V1 V2 V4 V5, P6.

- **`followedPage(page, cube)`** (pure TypeScript): after a cube changes, each live chart of it re-followed
  (`followCube`) and each chart's selection reconciled. Called by `applyPageCommand` for every command that changes a
  cube, by headless after a grid edit on a page (a new `pageGridApplied`, which Python's `Grid._apply` calls instead of
  `gridApply` + `_with_cube`), by `rebaseEdits`, and by the screen: a chart panel draws the followed spec from the page
  rather than following on its own.
- **The tree follows `configure`** (totals, expand level) inside the command's rule.
- **Expand level and closed groups saved** by `treeDocument` and read back.
- **Group-level column types:** a spike decides between typing at compile on both sides and not storing run-derived
  types; then one function.

Size: about 1.2k lines. Risk: chart following (the B1/B2 bugs lived here); the chart corpus and L7's seeds guard it.

### S3. The board produced by the commands
Items: P7 P13 P14, and the board's copies of command rules (tile and sheet removal, a chart's selection on removal,
sheet move, tile placement, ids held, `follow()` dropping `held` and `cubes`).

- Every gesture on `CubePage` builds its command; `PageApp` applies it to its state (`applyPageCommands`); the board is
  put at the result by `CubePage.sync` (already what undo, redo and follow use). The board's own mutation code is
  deleted; it keeps the gestures, the previews and the drawing.
- Stack and arrange gestures tell `stack` / `arrange`. An editing grid never touches the saved layout. `follow()`
  keeps `to.held` and `to.cubes`. The embedded CubeApp's charts use the same state.
- `page-state.ts`'s header rewritten (the screen applies commands); the bundle budget raised with a dated note naming
  this step (the screen now carries `page-commands.ts`).
- Guard: a page-state guard test (as `state-guardrail.test.ts`): no page state changes in `cube-page.ts` /
  `page-app.ts` except through the apply.

Size: about 2.5k lines changed, a net deletion. Risk: view state across `sync` (scroll, the shown sheet, an open chart
panel, its editing grid, a maximised tile, a stack's front tab, focus). Each gets a test that it survives an edit; the
screenshot tour runs at the end of the step.

### S4. The host applies the commands
Items: H1-H3, G12 P5 (N2), P10 (D1), B12.

- A write is its base version and its commands. The keeper applies them through headless DataCube (the same apply
  `pageRebase` runs; at the current version there is nothing to combine) and keeps what they make. The tab no longer
  sends its page.
- One other write: **restore** (N1, N2), a whole page taken only at the current version.
- Deleted: the tab's `#told/#seen` and `edits: null`, the keeper's refusal of a no-command write, `pageWritten`'s null,
  the tab-sent page.
- D1: Open…, in place and Blank Page on a served page open a new page.
- Measured: the time of one write on a 60-grid page, recorded here with a budget set from it.

Size: about 1k lines, about 400 deleted.

### S5. Python's copies gone; one rule each; every source
Items: Z1 Z2 Z4-Z9 Z12-Z18, Y6, B1 B3-B11 B13 B14, X1-X3 X5, P12.

- **Spikes first:** (a) `model-changes/` running headless, which needs one more host call (HTTP to the SDLC and the
  model writer, answered by Python); (b) a warehouse table, a saved query and a class as a Python grid's base.
- Every §5 copy replaced by its TypeScript function asked headless; where the function lives inside a dialog, its
  logic is moved out first, so the dialog and Python call the same one (`createClassOf`, `createDataSpaceOf`, the
  association check, `inputOf`).
- One rule for each §6 row, in TypeScript, both sides calling it.
- D3: `gridBase` for every source kind; Python's frames-only rule deleted; `pageReads` says the file and remote tables
  a page reads and the keeper keeps them like copies.
- `ll.show(df)` becomes a kept page of one grid (P12). Copy ownership becomes the keeper's alone (Z17).

Size: about 3k lines; `model.py` and Python's rule code deleted. Risk: the two spikes; if either finds a blocker, it is
brought to the user before the step goes on.

### S6. Capability parity
Items: K1-K12, X4, Z19. A Python method for what a Python user would do, calling the browser's own function (D6), the
rest named exceptions; the names agreed with the user before building (the charts': D7, D8). Size: about 1.5k lines.

### S7. The final audit
The four audits run again, fresh, with one extra instruction: find what the ledgers miss. Anything found goes into the
register and a ledger, and is fixed. Done when every ledger is empty but N1-N3 and the audit finds nothing new.

### S8. Screen code builds no command (the user, 2026-10-22)
The audits found the screen deciding what a gesture means in screen code twice (F71: the header click's first
direction; F72: a command kind only Python sent) -- and L8 could not see it: it asks whether each command *kind* is made
on both sides, never where a gesture's meaning is decided. The user: "Most important piece of the audits are if they
are finding new places where we don't have real API for datacube functionality and have somehow coupled the UI to the
implementation instead ... Is there any realistic way to find all and guard against this", then "Shouldn't we do all
60 so we close ledger completely?" -- yes: every command the screen tells is made by one of two things, and screen code
makes none itself.

- *A generated constructor*, where the gesture is the person's input passed through (a tile renamed, a sheet removed):
  the TypeScript twin of Python's generated builders (D17), written from the same field lists
  (`GRID_COMMAND_FIELDS`, `PAGE_COMMAND_FIELDS`), so both sides build the same command by the same generated code.
- *A gesture function*, where the gesture decides what it means (a column added to the row groups, a header clicked, a
  column hidden, a tile dropped beside another): in a module of no screen's, taking what the person did and the state,
  exported headless and asked by a Python method (its name agreed with the user first, as D21's). A drag splits there:
  pixels to "tile A, at tile B's left edge" are the screen's; the rest is the gesture function's.

Ledger L11 (in one-api-ledger.test.ts), closed at 0:
1. no command literal in a screen file (src/app.ts, src/page/**, src/app/**, src/ui/**, src/layout/**), found by the
   command types, not by text;
2. every gesture function a screen file imports is exported headless and asked by Python (or, a pixel's, listed);
3. a screen file imports no rule internal (grid-edits.ts, page-commands.ts' rules) -- only gesture functions and the
   generated constructors (F71's leak was such an import: firstSortDirection).

Items: A1-A60, one per site, classified at the step's start (constructor or gesture function); each closed with the
commit that moves it, its gesture's test through the function the screen calls and the one Python calls. (As built:
A1-A49 -- the menu's fourteen one item, A10 -- and what changed from this plan, As built (S8).)

## 6. How it is tested

- **Unit:** every new rule in `grid-commands.ts` / `page-commands.ts` / `page-rebase.ts`, including refusals in words.
- **Round trip:** every command kind through JSON and back (extends `page-commands.test.ts` "every kind").
- **Parity:** Python's builder and the screen's produce the same command for the same edit (extends the stage-1
  parity tests).
- **Gestures (L2):** the real PageApp and CubeApp in jsdom; every gesture's told commands replay to the page it made.
- **Convergence (L7):** seeded runs of the whole vocabulary through Python, headless and the tab; same page after each
  step, charts, selections and tree included.
- **Live seam:** the browser harness (`datacube/test/python-engine`) on one path per command family.
- **Mutation:** the scratch mutation harness over each step's new rules (as in keeper step 1b); a surviving mutant is
  a missing test.

## 7. Size and order

About 12k lines changed, a few thousand deleted, in eight steps, done in order: S0, S1, S2, S3, S4, S5, S6, S7. S0
first because it is what makes this attempt different: from its first commit, a new gap fails the build. S5 is the
largest; S3 the riskiest.

## 8. Out of scope (said, so it is not mistaken for done)

- **The proper live-editing model** (each view holding confirmed and pending edits, re-applying its own with
  `rebaseEdits`, no reopen on a collision; undo as inverse commands). This plan is its foundation; it is a separate
  decision after S7. **Decided 2026-10-24 (D24): built as D19, docs/DATACUBE_LIVE_EDITING_2026_10_24.md.**
- **Graph mode, chart batches, the backlog PR** (task 79), **§18 steps 5-7** (blocked on the user's A4 decision).

## 9. Tracking

- The register (§4) is the checklist; the ledgers are the truth it mirrors.
- Each commit: `One API S<n> <ids>: <what>`; the register updated in the same commit.
- Each step ends with "As built (S<n>)" here: what was built, what the audit found, the ledger counts before and after.
- `docs/IN_FLIGHT.md` carries the program's line from S0 to S7.

## As built (S0, 2026-10-21)

**Built** (commits 5eb17fc42, d598bf146, and the audit's round): `datacube/test/one-api-ledger.test.ts` (L1, L8, the
markers, C1, V5, the register), `datacube/test/one-api/scan.ts`, `datacube/test/one-api-gestures.test.ts` (L2, L3:
33 gestures and a control), `python/tests/test_one_api.py` (L4-L7, L9, L10), the BUILD wiring (the plan and Python's
sources copied to the output tree for the JavaScript test), the program's line in `docs/IN_FLIGHT.md`. No product code.

**What the gestures found on the real page.** Every page and grid gesture driven passes told, wire and replays except the
listed ones: the name box (P1), page undo (P5), a chart's mark (G7), a detached chart's Update (P8, written whole -- worse
than the audit said), a group opened (G1), Collapse All (G2), `applyConfiguration` (G10), Show As (C1, refused by the
host's reader), a following chart regrouped (V1: the chart's `split` differs). Opening a frozen chart in a grid writes
nothing, as a view change should. G14 is no gap (closed above). Every command kind is told by some gesture, but those the
screen does not make (`renamePage`, `stack`, `arrange`, `snap`, `live`: P1, P2, P14, P3) and two not driven yet (`measures`
by nature; `filter`, the Filters window being G6).

**Checked red.** Round one: a stray `#change`, a stray `_unnoted()`, a kind nobody makes, an untold gesture. Round two, the
audit's own escapes: a new caller of `refilter` on the board, a new action reaching a listed gap, a Python write through a
tile's view, a kind no reader reads, an item taken out of the register unclosed, a ledger entry added past its ceiling.
Each turned its ledger red with a message naming it; each was taken out again.

**The independent audit** (23 findings) and what became of each:
- Fixed: the grid's public ways in and their callers across the app (1); every caller of each listed gap (2); more
  gestures (sheets renamed, moved, deleted; arranging; page filters on a class grid; sorts by menu and click, the tree
  column's sort; hide; Column Properties; a filter from a cell; groups opened and collapsed; Open in grid) and every
  command kind told or listed (3); the gaps' names held to the gestures', the gesture list only grows (4); every ledger's
  size pinned (5); Python's funnel through handles, aliases, deletes, `setattr`, `__dict__`, and the write's flags (6);
  the kind lists, both readers and the rebase's renaming checked per kind (7); host writes and Snap counted only where no
  command is told, so the fix is what takes them off, P2 by its own entries (9); replays of the commands as the host read
  them (13); a quiet page before each gesture, and a control (14); failures pinned by how they fail (12); producers read
  only in the modules that make commands, Python's docstrings left out, headless functions cut at their brace (16); the
  source kinds' keys and refusals, `PYTHON_ADDS` pinned (17); each allowed Python assignment's right-hand side pinned (18);
  every call of the command path, the allowed ones' text pinned (19); Snap's controller calls (20); C1's three column
  kinds, V5's closed groups, H2's tab halves, H1 through the engine (21); the register pinned whole (23); a comment's code
  after `*/` read (low).
- Said rather than faked: the nine capabilities with no kind of their own are listed as unchecked until S6 agrees their
  names (8); G12/P5 close in S4 by the restore write and a stale-restore test (10); the gesture harness stands in for the
  host's rename, P1's check on the host's own code being L1's (11); T1-T4, G13, P6 and P7 stay markers until S1/S3 write
  their failing behavioural tests first (15); L7 at S0 is three Python assertions and the gestures' replays, S2 extends the
  convergence soak first (22); markers pin wording and L6 names by function: re-pinned when moved (low).
- Not done: a per-kind parity row (the parity tests are by action, not by kind: no cell can be read from them).

**Gates.** `//datacube/... //python/...`: green (four environment reruns: three tests the disk filled under, `verify_app`
as the container's unprivileged user). CI: not run -- the gate runs on a pull request, a push to main or a dispatch; this
branch has no pull request and the session's GitHub access may not dispatch (403). Waiting on the user: a draft pull
request (CI on every push) or a dispatch of `gate.yml` on `claude/datacube-query-base`.

## As built (S1, 2026-10-21)

**Built** (commits 6ef92b374 through this step's audit round): every S1 item closed in the register above, G8 moved to
S3 (a page step does not restore the grids' documents, so the restore heal waits for the board produced by the
commands). New grid commands `refilter`, `editCalculated`, `removeCalculated`, `open`, `collapseAll`; new page commands
`select`, `setChartCube`, `modelInto`, `modelPin`, `modelKeep`, `rebuilt`; `calculate` reads every column kind (C1);
the grid's draft carries its tree; CubeApp tells the commands of each transition of its committed state in the order
it took them (T1-T4), a retry by the refused change's commands (G13); `applyConfiguration`, public `change()`, the
`controller` getter gone and `state` a view of what it reads (G10, G11); restore keeps what it cannot open (P9);
Python's model actions and reopened cubes are commands (Y1-Y3); the page's model rules live in page-commands.ts, so the
page does not load page-model.ts at start (P4).

**Deviations from the plan's wording, each kept on purpose.** `removeCalculated` has no level (a calculated column's name
is one across both levels: `nameProblem`). `gridReopened` became `gridOpened`'s `taken` and `opened`. "With who made them"
(T4) is another editor's commands never entering what CubeApp tells, rather than a label on each. `GridDraft.tree` is
optional (an editor's draft holds no open groups; `configure` moves a tree only where there is one). `rebuilt` carries
a source, not a document (the audit's 1, below).

**The independent audit** (15 findings) and what became of each:
- Fixed, each with a test that fails without it: (1, high) a `rebuilt` made again on top of another writer's grid
  edit overwrote it -- now applied by one rule over the source (`cubeOpenedAgain`; page-commands.test.ts, test_page.py
  with the open page's regroup); (2, high) the person's grid edits told while the tab followed the other editor were
  dropped -- the channel keeps a grid's (live-channel.test.ts); (3) another editor's commands that changed nothing were
  told back (cube-transactions.test.ts); (4) Python's edit of a calculated column reset its kind and level -- it starts
  from the column as it is, the editor's own rule moved to column-draft.ts `existingDraft` (test_page.py); (5) removing
  a chart with a selection took its condition off twice on the host, and was written whole -- the screen tells the
  selection cleared and the removal as one edit, said once (a gesture with the condition on the grid by hand, which
  fails both ways without the fix); (6) a snapped cube told as `rebuilt` -- `rebuiltCommands` leaves it out
  (page-commands.test.ts); (8, 9) a retry replaced the commands of a change in flight and ran the refused state, losing
  what was made since -- it applies the refused change's commands to the grid as it is then, or, an undo part of it,
  runs it again as asked (the owner's `refused` says `moved`; cube-transactions.test.ts); (11) a kept cube's snap record
  dropped on write (one-api-gestures.test.ts P9); (13) `state` handed out the owner itself (cube-transactions.test.ts);
  (14) no guard against a cube told back and forth -- a page loaded over frames as they were tells no `rebuilt`
  (test_page.py), and the browser reopened with nothing changed writes nothing (test/python-engine); (15) two stale
  comments.
- Said rather than fixed, waiting on the user: (7) `L8_KINDS` was raised twice in S1 (13 -> 15 for `select` and
  `refilter`, 12 -> 13 for `setChartCube`) with dated notes but without the user's sign-off that rule 1 asks for; those
  kinds have no Python builder yet (K7, K11: S6, whose names the user agrees first). (10) The tab's model actions still
  change the model and then tell what changed (`modelCommands` of before and after) rather than applying commands first;
  Python applies `modelPin` and `modelKeep` always, the tab only when they changed -- the same page either way. Not yet a
  register item: registering it grows a ledger, which also needs the user's sign-off.
- Noted: (12) a presentation command told twice (T2) is right because every presentation command today sets values
  (`configure`); nothing pins that. G9 and G15 are held by static pins, no behavioural test.

**Gates.** `//datacube/... //python:engine_test //python:model_test`: green at P11 and at G13/T1-T4 (191 of 191 the
second time), `verify_app` as the container's unprivileged user green; the audit round's targeted tests green, its
full run below. CI: not run -- still waiting on the user (a draft pull request, or a dispatch of `gate.yml` on
`claude/datacube-query-base`).

## As built (S2, 2026-10-21)

**Built** (a9ec10e7f, b659ca010, a147d256c, and this step's audit round): every S2 item closed in the register --
`configure` moves the grid's tree by the settings that decide it (G3); a page's charts follow their cube by one rule,
`followedPage`, wherever a cube changes -- `setChart`, `setChartCube`, `rebuilt`, a writer's grid edits made again on
top, Python's grid edits (headless `pageWithCube`) -- live charts regrouped, a selection its filter no longer holds
taken off (V1, V2, P6); the tree's expand level and the groups closed under it saved and read back (V5); no side writes
the type a run learned for a group-level column (V4). Z1 closed with V1 (Python's `_with_cube` deleted). The keeper's
soak now builds a page with a following chart; at 60 seeds it found F5, fixed with its test.

**Deviations from the plan's wording, each kept on purpose.** `pageGridApplied` became `pageWithCube` (the grid's edit
is applied first by `gridApply`, as before; the page part is one call). V4's spike chose "stop storing run-derived
types": typing at compile cannot type a pivoted level's columns (they need the pivot's values, which only a query
gives). The screen's chart panel still follows by the same `followCube` on its own, and the screen's reconcile still
judges a selection, both against the grid's state on screen and told by no command -- the S2 bullet "a chart panel
draws the followed spec from the page" is carried to S3 inside G8 (the board produced by the commands).

**The independent audit** (10 findings, and one outside S2) and what became of each:
- Fixed, each with a test that fails without it, each registered (rule 9) and closed: (1, medium) a selection told
  before its click's refilter lands was dropped by an edit in between -- F1; (4) a configure giving the expand level
  again reopened closed groups -- F3; (5) a chart's removal took its selection off the filter without judging the
  cube's other charts -- F2; (6) Python's chained renames of handles -- F4; (8) the soak's handle bug fixed with no
  register item -- F5.
- Fixed as S2's own: (2, medium) the soak did not hold S2's claims -- it now makes chart clicks (a `select` and its
  grid's `refilter`, written together) and tree edits on both sides (`open`, `collapseAll`, the expand level), and
  checks each settled page against an oracle: every cube's page a fixed point of following (headless `pageWithCube`),
  every chart's selection on its grid's filter; (3) `rebuilt` and `setChartCube` following had no test -- now each has
  one that fails without it (page-commands.test.ts); (10) nits: G3's note named the wrong test file, `followedPage`'s
  doc comment had orphaned `cubeOf`'s, `treeOf` had no product caller (`openCube` now opens a saved tree through it),
  `readTree` dropped closed groups under no level (now refused in words), `pageWithCube` threw where its siblings
  answer `{problems}`, `Page._given` shared a name with a module function (`_regiven`), the round-trip seeds never set
  an expand level (they do, and fail without the writer's `closed`).
- Registered rather than fixed: (7) the screen's own following and reconcile -- folded into G8 (S3); the outside
  finding (a page filter both writers make under one id is replaced, not given afresh: the rebase has no write's base)
  -- folded into H3 (S4, with H1's base version).
- Noted: (9) pages saved before V5 open at level 0 whatever their `initialExpandToLevel` (no reader of older pages: the
  user's standing rule), and before G3 a Properties Apply re-synced the tree whatever changed; Python's copy of an old
  cube keeps a learned `groupDerived[].type` until that cube's next edit -- harmless, every reader learns it again.

**Ledgers.** S2 closed G3 V1 V2 V4 V5 P6 Z1 and shrank MARKERS 17 -> 16 (V4's marker); no ledger grew. The register
gained F1-F5, each closed in the commit that fixed it.

**Gates.** `//datacube/... //python:engine_test //python:model_test` (191 of 191) and `verify_app` as the container's
unprivileged user green at a147d256c; the keeper's soak at 60 seeds green (196 tests); the audit round's run below. CI:
not run -- still waiting on the user (a draft pull request, or a dispatch of `gate.yml` on `claude/datacube-query-base`).

## As built (S3, 2026-10-21)

**Built** (ae126e5fe, c328449ba, e47b3dbe5, 3c595311f, 90fb56efc, 6f89fd1be; the audit round 02622bf0d, 85d80333c): every
gesture on the board tells its commands and changes nothing of the page itself; its owner -- `PageApp`, or a cube
alone's `CubeApp` -- applies them to its `PageState` by the one step (page-commands.ts `applyToState`) and puts the board
at the page they make (`CubePage.sync`), which is also what undo, redo and another editor's commands use. A tile a
gesture makes (a grid's engine, a chart's panel) waits, pending, until its command places it; refused, it is let go.
Ids come from the state (`takenIds`) and what is on the board. Stack and arrange gestures tell `stack` / `arrange`
(P14); an editing grid is the board's alone and never touches the page's layout (P7); a cube alone's charts keep a page
state of their own and tell the same commands (P13); the board heals no selection -- a step that brings back a chart
with a selection its grid's filter lost has it put back by that grid's own `refilter` (G8). `follow()` keeps the
commands' page whole, its held ids and cubes. What is looked at survives an edit: the sheet shown, a stack's front tab,
focus, an open chart panel and its editing grid, Move to Sheet keeping the sheet shown. A guard
(state-guardrail.test.ts, S3) pins every place the board's structure changes and every place a page's state changes,
with why and how many times.

**Deviations from the plan's wording, each kept on purpose.** G8 is closed for the board only: the chart panel still
follows its cube by `followCube` on its own, and the screen still reconciles a selection against its grid's filter, both
told by no command -- a cube alone over rows its host names no source for has no cube in its page's state to follow
(the S2 audit's 7). The editing grid opens in a band of its own below its chart's, not beside it, so a gesture made
while it is open changes the page as it would with it closed (the audit's 4). A cube alone saves one cube: its added
grids, and the charts that read them, are in its state and its told commands but not in what it saves (F12, declared).
An undo puts the page at a whole step (N2), so a grid's own change made since is not carried into the steps (F15,
declared; the proper live-editing model of §8). The bundle budgets were raised with dated notes (the screen carries
page-commands.ts; the audit round's fixes).

**The independent audit** (15 findings) and what became of each:
- Fixed, each with a test that fails without it (run against the step's head), registered (rule 9) and closed: (1,
  high) a cube alone's state went stale as its grid's own changes landed -- F6; (2, high) a cube alone's last chart
  removed threw -- F7; (3) a cube alone gave an id out again after its board went -- F8; (4) a gesture with an editing
  grid open moved the page's shares, and an arrangement was told as a layout -- F9; (5) an edit made while `follow()`
  waited for a snap was lost -- F10; (6) a detached chart's Update was a board-side copy of `setChartCube`'s rule, and
  undone left the page mixed -- F11; (8) Go Live refused for a grid whose cube the page cannot hold -- F13; (9) a cube
  alone's added grid renamed to its id's number was put back -- F14; (11) a chart with no spec yet moved at each step,
  and a drag in progress ended at another editor's step -- F16; (12) a refused grid threw -- F17.
- Declared, with a test that pins what is true: (7) a cube alone's saved page is its state less its added grids --
  F12; (10) an undo of an unrelated step puts back a selection a grid's own edit took off -- F15 (not new since S2).
- Fixed as S3's own: (13) the guard pinned only where, not how many times, and missed the kept grids, the charts'
  links, the chart and grid maps, renames and spec sets -- now it pins each, with a count; (14) stale or overclaiming
  docs -- `sync`'s, page-app.ts's header, `onPageEdit`'s, the export (now the page's layout, each stack's front as
  shown), P7's and P13's register lines; (15) claims with no failing test -- `follow` keeping the ids its commands held
  and their cubes, a refused gesture letting go, a cube alone's ids, its last tile, P7 under a gesture: each has one.

**Ledgers.** S3 closed P7 P13 P14 G8 and the board's rule copies; L1_ROUTES 5, L8_KINDS 11, MARKERS 14; no ledger grew.
The register gained F6-F17, each closed in the commit that fixed it (F12 and F15 declared).

**Gates.** `//datacube/... //python:engine_test //python:model_test` (191 of 191) and `verify_app` as the container's
unprivileged user green at 85d80333c; the screenshot tour green (17 of 17), its editing grid below its chart. CI: not run -- still waiting on the user (a draft
pull request, or a dispatch of `gate.yml` on `claude/datacube-query-base`).

## As built (S4, 2026-10-21)

**Built** (the step's commits after 68ce957dc): a write to a page Python keeps is the version it was made at and the
edits it made there, each a command; the keeper (keeper.py `settle`) has DataCube apply them to the page as it is now --
headless `pageRebase`, nothing to combine at the writer's version, made again on top past it -- and keeps what they
make, or refuses them in DataCube's words, nothing kept (H1). Python's own page writes the same way (page.py `_commit`),
so the two editors' writes meet one rule. The one write that is no commands is a restore -- a page loaded (N1), an open
page's undo or redo (N2: `onRestore`, told by the owner's `moved`) -- kept only at the version it was made at (keeper.py
`restore`, headless `pageWritten(text)`); the tab's `#told/#seen` guess, `edits: null`, the keeper's refusal of a
no-command write and `pageWritten`'s null are deleted (H2, G12, P5). The keeper keeps the ids each version held and
tells the rebase the ones a write knew (`rebaseEdits` `known`): a page filter two writers made under one id is given out
afresh, never put in the other's place (H3). One held set: the keeper's, read by the tab with the page (`page.json`
`held`; `PageApp.restore`'s `held`), so neither side gives out an id the other has held (B12). On a page Python serves,
Open…, open in place and Blank Page put a page of the tab's own on screen and leave Python's as it is, said once -- a
blank page on its card (D1, P10). The keeper's soak (60 seeds) found F18, fixed with its test.

**Measured** (the step's last bullet: one write on a page of 60 grids, in the container, `LEGEND_LITE_WRITE_COST=1`): a
page edit 0.13s, a grid edit (its grid planned again) 0.36s -- each Python's call applied twice over (its own, then the
keeper's `settle`), growing with the page. The budget (test_keeper.py `WriteCost`): 1.5s and 3s, about ten times that,
for CI's slower runners.

**Deviations from the plan's wording, each kept on purpose.** Python's page applies its own commands as its call runs
(its handles read the page they make) and the keeper applies them again as it settles: one rule run twice, no rule
copied -- the measured cost is that; left for the final audit to judge. The engine's `serve_page` -- a page the host
replaces whole, which only tests serve (the engine's, and the browser harness's) -- is still a write of a page whole; it
is Y6, open, S5's. `L9_KEEPER` grew 6 -> 8 with a dated note: its two new entries are the restore writes S4's own bullet
names (N1, N2), both allowed, but rule 1 counts any growth as a ledger growing, so it waits on the user's sign-off as
S1's finding 7 does. The engine page's bundle budget was raised 500 bytes with a dated note.

**The independent audit** (8 findings; one more found reviewing S4's own code) and what became of each:
- Fixed, each with a test that fails without it (run against the step's head), registered (rule 9) and closed: (1, high)
  the tab left the page Python serves when its start opened nothing -- an empty page, a failed first read -- and
  followed it no more -- F19; (2, high) a grid over a source the host cannot base its edits on was taken onto a page
  Python serves and its every edit then refused -- F20: one rule of what a page on the host holds (headless `onHost`),
  asked by `gridBase`, `gridOpened` and the keeper of each cube a write adds or moves, which refuses it as it is added.
  The tab still offers New ▸ Data Source… on such a page; the grid it adds is shown and then taken off, the refusal
  said, until S5 brings every source to a page on Python (B7, X1-X5) -- a copy of the rule in the tab would be a twin;
  (3) a page filter no command made under its keeper, set again, became a second -- F21; (4) a follow on its way when
  the page was left threw, or put Python's page back with no channel -- F22; (5) B12 untested on the tab, its Python
  test unable to fail -- F23; (7) edits made while a write was on its way dropped silently when the keeper made another
  page of it -- now said -- F25; and the engine said a failure of its own while taking a write as no such page or as a
  refusal -- F27, found reviewing S4's own code.
- Declared, with a test that pins what is true: (6) a write from further back than the keeper keeps (64 versions) is
  refused -- F24; (7) an undo or a redo that lands while the tab follows the other editor is not written -- F25.
- Fixed as S4's own: (8) stale comments and docs -- F26; As built's claims made exact (B12's, P10's: Open… and open in
  place call the same `leaveServed` and have no test of their own).
- Checked and left: F18 changed `withoutConditions` to compare data, not text (`sameValue`). The other places that
  compare document data as text (the chart click's toggle, draftCommands and settingsPatch, the board's spec, selection
  and layout checks, band-board's layout, chart-spec's showAs) each compare values written in one key order -- one
  reader's, or the same code's (a chart's mark conditions are built in the order the filter reader writes them) -- so
  key order does not differ there; a cube document's filter, kept as read, against a command's, read by the filter
  reader, is the case F18 was.

**Ledgers.** S4 closed G12 P5 P10 H1 H2 H3 B12; MARKERS 9; the B7 marker's count 3 -> 2 (the rule in one place,
`onHost`). The register gained F18-F27, each closed in its fix (F24 declared, F25 in part).

**Gates.** `//datacube/... //python:engine_test //python:model_test` (191 of 191 at the step: the full run found
live-follow's page answer still said without the ids held, fixed in the step; 192 of 192 after the audit round, its
jsdom test of the app on a served page added) and `verify_app` as the container's unprivileged user green, each time;
the keeper's soak at 60 seeds and the screenshot tour (17 of 17) green at the step. Each audit fix's test was run
against the code without it and failed. CI: not run -- still waiting on the user (a draft pull request, or a dispatch of
`gate.yml` on `claude/datacube-query-base`).

**Next.** S5.

## As built (S5, 2026-10-22)

**Built** (the step's commits after ad5e07aca, each closing its items with a test that fails without it):

- *Python's copies gone.* Each rule Python kept of its own is DataCube's function, asked headless: the model actions
  over `model-changes/`, `model.py` deleted (Z4-Z7, D4), the page's state holding its model and a model action's record
  applied as commands, no undo step (P15, D5); a grid's base on the host (Z15); a filter's shape and values -- Python
  only tags what JSON cannot carry (Z12, D12); a page's ids (Z2); a new grid's commands in a person's order (Z8); a
  join's inputs' names and places (Z9); the snap limit's sentence (Z16); the saved-page record (Z18, B4); whose a copy
  is, the keeper's one model (Z17: known limit 6 closed); a stack and a detached chart (Z13, B8). Python's
  `L6_FUNCTIONS` 8 -> 0.
- *One rule for each §6 row.* One unnamed new page and one skeleton (B3, B9, D11); one page-file rule (B5); one
  "nothing to save" sentence, empty pages shown from Python (B14, B6); one saved record per page, kept in the page
  (B13); `ll.show(df)` a kept page of one grid whose frame's new columns keep its edits (P12, B10); the engine's
  `serve_page` and `close_page` deleted (Y6); the tab's Join… on a Python page (B11).
- *Every source on a Python page* (D3; X1-X3, X5, B7, Z14; D13, D14). headless host-tables.ts `leafOnHost`, the one
  rule of where a cube's leaf is read: a frame, a file (by its bytes' hash) or a remote file (by URL) on the host's
  database; a class through its project at its revision in the host's model home; a saved query through its project's
  version in the host's depot; a warehouse table under the session the host's engine holds, signed in by a tab (D13).
  The class and saved-query rules are the tab's own, moved out to be shared (class-source.ts `classOpened`,
  saved-queries.ts `savedQueryOpened`). Python's frames-only check is deleted; the keeper keeps the files and remote
  files a page reads as it keeps copies (`pageReads`, keeper.py `HOLDS`); `Page.load(..., files=)` is given a page's
  files, each checked against the hash the page names (D14); `page.grid(file=, remote=)` adds such grids. A page's
  filters reach Python's `view`, rows and export by the screen's own rule (B1: page-filters.ts `gridLens`).

**Deviations from the plan's wording, and limits, each said.**

- *Callbacks under the frames' lock.* A class, saved-query or warehouse leaf's rows are planned by Python's engine,
  asked from inside DataCube's code in V8 (a new host call, `engine`). Python answers it on the thread of the caller
  whose call asked it, which holds the frames' lock (re-entrant), so nothing waits on a lock its caller holds. It works
  and is tested, but it plans some queries on the caller's thread rather than the compiler's pool, and the lock is held
  across calls into V8 and back. The user approved replacing it after S5 (D15).
- *Two rows paths.* A frame's, a file's or a remote file's grid is planned by DataCube's PlanThenRun over the host's
  DuckDB; an engine leaf's by the tab's own `pythonRunner` over Python's engine; Python's `to_pandas` chooses between
  them by the base's `runs`. One rows path for every caller is D16, after S5.
- *Added from Python is S6's.* S5 reads, edits and saves a page over a warehouse table, a saved query or a class (loaded
  from a saved page, or added by the tab); a Python method that adds such a grid is X4, open, S6's.
- *Stand-ins.* The container has no warehouse and no depot holding saved queries: `OverWarehouse` runs against a
  stand-in driver, and `OverSavedQueries` patches the host's fetch to serve a saved query's project as the depot's API
  does. X5's saved-query half is so tested against DataCube's client, not Python's depot route (F33).
- *A join reads one database.* A class, a saved query or a warehouse table is refused as a join's input, on both sides,
  in the same words (§17.3) -- B11 closed on the same rule both sides run, not on every kind joined.
- *Rewritten tests.* Tests that pinned the old refusals ("Trade is not read here", the frames-only rule) now pin the
  model home's refusal and the leaves read; `cubeOpenedAgain`'s guard against non-frame sources is deleted (which
  sources a host reads is `onHost`'s to say), with a test of a cube over a file opened again.
- *Exit crash fixed on the way.* The session handed its engine a function of the module to answer through; the cycle
  through the module's globals crashed the interpreter as it ended (model_test exited 139 after passing). It is a
  `weakref.WeakMethod` now, with a subprocess test that fails without it (test_model.py `AtTheEnd`).

**Ledgers** (from the step's start, after D10's sign-off). TS: `L1_REACH` 9 -> 6, `L1_PLANE` 4 -> 3, `L1_HOST`
12 -> 8, `L8_SOURCES` 5 -> 3, `L8_UNASKED` 2 -> 1, `MARKERS` 10 -> 0. Python: `L6_FUNCTIONS` 8 -> 0, `MARKERS` 15 -> 1
(the one left pins `_apply_page`'s binding, allowed), `BEHAVIOUR` 1 -> 0, `L9_KEEPER` 8 -> 6. Each move dated in its
file. Nothing grew.

**Gates.** `//datacube/... //python:engine_test //python:headless_test //python:notebook_test //python:model_test //model-changes:replay_test` (196 targets: at the step's last run 194 passed, and the two that failed -- B11's own new tests, one missing its fixture's Bazel data, one asserting the wrong protocol node -- pass since), `verify_app` as the container's unprivileged user and `//python:wheel_test` green at each of the step's commits. CI on pull request #64: f77075161 green on every job; 0ab550e5b green but one lane, parser equivalence, which failed fetching a reference file from gitlab.com (a 404 from outside this change; a re-run not permitted to this session), re-run by the next push.

**Not done in S5.** The independent audit of S5's commits has not run; whether to run it now (a fresh reviewer looking
for test-gaming, overclaimed items and the stand-ins above) is with the user.

**Next.** D15 and D16, then S6.

## As built (D15, D16, 2026-10-22)

**D15: no lock across a call into DataCube.** A read of the frames (frames.py `Frames.reading`) is a DuckDB cursor of
its own, its session set as the planner's SQL expects, each frame read at each query registered on it as the frame is
when the read begins, and a transaction begun and pinned by one catalog read (DuckDB begins a transaction on a database
lazily, at its first read). The frames' lock is held only to begin it. Every read through DataCube on Python is one:
page.py's grid calls, the keeper's rebase, the engine's `opened.json` and `join.json`. A call into DataCube made in a
read queries through the read's cursor, and what it asks of Python's engine is answered on DataCube's own thread over
the same read (engine.py `answer_over`, `_plan_and_run_over`). That thread counts as a compiler thread
(`_pool.library_thread`), so a library call made there, a model home's included, runs in place and never waits on the
pool. A write through DataCube's code (a file read in, a remote file mounted, a table forgotten, a snap's copy landed)
takes DataCube's turn (`Headless.turn`) before it holds the frames, so no one holds the frames while waiting for a
turn. The caller-thread queue, its serving loop and `answer_here` are deleted. Tests: test_headless.py `OneRead`. Run
against the code before D15 (its old API spelled as it was), the same three scenarios fail: a frame registered during a
call waits for the call; the engine's answer runs on the caller's thread; a library call made while the pool is busy
times out.

**D16: one rows path on the Python host.** Every grid's rows are read by the engine's plan-and-run, whatever the base:
what a grid shows and exports in DataCube's code (headless grid.ts `runnerOf`: the tab's own `pythonRunner` over the
host's engine, as a tab on the page reads every grid), Python's `to_pandas`, a join's questions and a model action's key
facts. The host's database is queried directly only for what DataCube writes into it and its catalog. `HostBase.runs`
is gone: nothing in DataCube says where a base's rows are. A session's engine is made with DataCube's (it reads every
grid's rows), with no site where an install carries none; what shows a page asks `shown_engine`, refused in words
without one. The tab keeps its own host's path (its planner and DuckDB-WASM, or Python's engine over HTTP). Test:
test_headless.py `OneRowsPath`. With D16's rows-path change taken out and the rest kept, it fails (`0 not greater than
0 : its view`).

**Said, not changed.** A read sees the frames as they were when it began. A frame registered during a long call is read
by the next read, not by this one. A tab's query no longer waits for a Python call to end, nor a call's query for a
tab's SQL: a tab's plan-and-run runs in a read of its own too (F35); the frames' lock is held a moment, to begin a read
and while a query plans. A query over a table dropped after its read began is refused in words (F31). A query that
fails as it runs begins the read again (F36). The engine's legend-engine routes (`execute`, `cube.json`) still take the
frames' lock while they plan and run (`serving`); no call into DataCube is made inside one.

**The independent review (D18)** of S5, D15 and D16 found thirteen things, registered as F28-F40 and each closed in the
same commit or declared: F28 (a class's grid read its project's sample rows for a table not served: the one high), F29,
F30, F32, F35-F40 fixed with a test that fails without the fix; F31, F33's saved-query half and F34 declared.

## As built (S6, 2026-10-22)

**Built** (the step's commits after a66b75c89, each closing its items with a test that fails without it):

- *Python's command layer, generated (D17).* Each command module lists its kinds' fields as data (`GRID_COMMAND_FIELDS`,
  `PAGE_COMMAND_FIELDS`, held to the command types both ways by the compiler: command-fields.ts), and
  datacube/tools/python-commands/emit.ts writes `python/legend_lite/_commands.py` from them: one keyword-only builder
  per kind, diff-tested (`bazel run //python:update_generated`). Python writes a command no other way (a new L8 check
  refuses a command dict outside `_commands.py`).
- *A Python method for every gesture (D21's names, D6: each calls the screen's own function, moved out of screen code
  first where it sat there).* K1 sorts (a new `clearSort` command the header menu builds too); K2 a condition added; K3
  K5 (S5); K4 every column the column editor makes and the header menu's presets (grid-presets.ts `presetCommands`, the
  menu's own rule now); K6 named dimensions; K7 page filters and a chart's selection (page-filters.ts `classPick`,
  moved out of page-app.ts); K8 sheets moved and removed; K9 a chart drilled and back up (`drillRefusal`); K10 Copy of
  Grid (cube-document.ts `cubeCopied`); K11 Show Grid (below); K12 a saved page opened by id (page-document.ts
  `savedPage`, moved out of the app); Z19 `pageFilter` reads Python's filter form.
- *Show Grid (D7)* replaces Open in grid and its Update: a following chart's grid shown; a frozen or detached chart's
  grid of its own grouping made in one step -- `addGrid`, a new `followGrid`, `setChart` -- by chart-spec.ts
  `chartGrid`, the screen's gesture and Python's `chart.grid()` (headless `chartOwnGrid`) alike. `setChartCube`, the
  editing grid, its Update and the board's kept `<chart>-query` grids are deleted.
- *Python adds a grid over every source the tab does (X4):* a class (`cls=`, `project=`, `revision=`), a saved query
  (`query=`), a warehouse table (`warehouse=`, `table=`, `catalog=`), each named as the tab records it (headless
  `givenSource`) and made where the host reads it (headless `sourceCube`).
- *F33 closed end to end:* a project made by Create a Class…, reviewed, approved, committed and published by the model
  home's SDLC, a query of its version saved and a grid added over it, read through the host's own `/depot/api`.

**Found and fixed on the way** (rule 9): F41 (chart-tiles awaited the board it opened: CI's macOS lane), F42 (a page
filter with a number broke every Python read of the page's grids), F43 (a chart's click compared a reopened page's
filter as JSON text, so the same mark again set it instead of taking it off). **Found and declared:** F44 (core's
DuckDB DDL folds an unquoted column to upper case, so a grid over a DataCube-made project's saved query is refused on
Python: the dialect line's, said in IN_FLIGHT; `PublishedQueries` asserts the refusal until it is fixed).

**Deviations, and limits, each said.**

- *A cube alone gives no chart a grid of its own.* It saves its own grid and its charts alone, so a chart moved onto
  another grid there would be lost on save: Show Grid only shows a following chart's grid there (found by the test
  rewrite; tested by chart-tiles.test.ts and cube-page-shell.test.ts). Since 2026-10-23 the greyed-out entry says so
  on hover (`MenuItem.hint`).
- *Show Grid on every chart kind (the user, 2026-10-23).* It was refused on a scatter, a scatter matrix, a map, a
  gauge, a KPI, a treemap, a sunburst, a tree and a chart with nothing across ("no grouping of its own"), so those
  could never show the grid behind them. One rule now decides, chart-spec.ts `showGridWay`, asked by the screen and by
  headless `chartOwnGrid`: the grid the chart reads is shown when it holds what the chart draws -- a following chart;
  a treemap, sunburst or tree (each draws the grid as shown); a pinned chart that takes nothing of its grid's grouping
  (a scatter's rows, a map's places, a gauge's or KPI's one figure). Otherwise `chartGrid` makes the chart's own grid,
  grouped as the chart groups it: a scatter's rows as they are with only its plotted columns shown; a map by its
  places (a bubble map's latitude and longitude too); a gauge's or KPI's one figure as the grid's total; a treemap's
  grid as it drew it. Tested by chart-rules.test.ts (`Show Grid on every kind`), cube-page-shell.test.ts and
  test_page.py.
- *Python names, never toggles.* `chart.select(value)` keeps a mark selected when named again; a click's toggle is the
  screen's (D8). A period or a bin across (a range ending where the database says) is not selected from Python, nor a
  mark of a chart whose marks are not values across (a scatter, a treemap, a stream): refused in words.
- *A class of another project* is read through what DataCube's writer makes beside it (`<package>::domain::<Name>`, its
  package's `mapping::Model`): a class of a project DataCube did not write is not added by name.
- *Show Grid's first selection step.* A selected frozen chart's selection comes off its old grid's filter through the
  click's own `#select` (its grid's refilter), told as the step's `select`, so the one-API ledger's routes did not grow.

**Ledgers** (from the step's start). TS: `L8_KINDS` 11 -> 3 (the three left are by nature, reviewed at S7),
`L8_UNASKED` 1 -> 0, `L8_SOURCES` 3 -> 0, `UNCHECKED` 7 -> 0. Each move dated in its file. Nothing grew.

**Gates.** The full local set (`//datacube/...` and every `//python` test target, `//model-changes:replay_test`: 203
targets) green at each of the step's commits but the last, where two failed: `create_class_test` and
`update_generated_1_test`, both the generated test imports missing class-source.ts, which model-actions.ts imports
now; regenerated in the close-out, both pass. `verify_app` as the container's unprivileged user and
`//python:wheel_test` green. CI on pull request #64: a66b75c89 and 243a0fa8a green on every job, 24196c82d green.

**Next.** S7, the final audit; then D19, the single writer's client side.

## As built (S7, 2026-10-22)

**Built.** The four audits (host shell, grid, board and page, Python) ran again, fresh, each told to find what the
ledgers miss: 40 findings (H-1..H-13, G-1..G-7, P-1..P-9, Y-1..Y-11), F45-F70 -- fixed with a test that fails without
each (F45-F67), the ledgers' blind spots closed with no ceiling moved (F68), D19's own moved there (F69), the rest
declared (F70). A confirmation round of the same four audits, over those fixes, found 21 more -- five of them wrong
closures or regressions of the first round's fixes -- and the user asked why a header click was exempt (F71-F90); a
third round, over those, found six more, three of them its own fixes' wrong closures or regressions (F91-F96); a
fourth, over those, seven -- two races of the tab telling its page and its grids on two schedules (D19's, F97), one
regression and lows (F98, F99) -- and no new place where the screen decides a DataCube rule:

- *One sort API* (F71, the user's question): header-sort.ts `headerSortCommand` is the one rule the screen's header
  click and Python's `grid.toggle_sort` / `grid.toggle_group_sort` ask (headless gridHeaderSort). The "by nature"
  entries were wrong: a gesture's rule is an API like any other.
- *No command kind of one side's own* (F72): `measures`, which only Python sent, is deleted; a measure is made by the
  steps a person makes one by (grid-measures.ts `measureCommands`, over grid-edits.ts `measurePlan`, the rule Show
  Grid's measures are made by too). L8_KINDS 3 -> 0: every command kind is made by the screen and by Python.
- *What a person sees, fixed*: an undo of a width or a format told (F45, F77); a selected chart's removal taken off its
  grid once, the grid's undo kept, a click still running told in order (F46, F75); a page Python serves with no grid
  opened, and its Add a data source adding to it (F47, F83); a host's page opening its own grids on the host and a page
  of the tab's own opening its files the tab's way (F48, F73); the saved record and the name kept through an undo
  (F52, F74); Collapse All offered when something is open (F66, F78); a snapped grid read live told `live` then
  `rebuilt` (F54, F82); Python's join beside its first input (F55), a Cube File loaded (F57), a deleted record refused
  (F58), a page with a source it cannot open left out as the tab leaves it, refused when none opened (F65, F84, F85), a
  page loaded from another's document saving as its own (F86).
- *One rule each* where two spelled it: a new grid's tree (F49), Show Grid's step (F50), a chart's click (F51), a page
  file's name (F56), a snapped grid opened again (F60), the keeper's admission, on every write (F59, F89), a sheet's
  label (F80), a name that is not text (F87), Save's over-or-new (F88).

**Deviations, and limits, each said.**

- *Three first-round items fix no behaviour a person reaches today*: F60, F61, F63 (one rule now; no test can fail
  without them, none claimed). F88 likewise.
- *D19's own* (F69): Save on a served page saves the screen's page (H-5); a reopen within the channel's wait drops the
  last edit unsaid (H-11); an undo's restore is judged by fewer rules than a write (H-13); and another editor's removal
  followed while the person's click still runs (F90).
- *Declared* (F70, F90): Python's export is the grid alone; presentation gestures are the screen's, Python sets them by
  `configure`; a drilled dimension and Bins' preset columns are Python's, the column editor's JSON picker the screen's;
  `grid.expand` takes a group's keys unchecked; a refused board edit puts the board back at its next sync; PageApp's
  name, filters and model are fields beside its state; a served file grid opened again on the host offers Snap and Go
  Live (the host's answer says no read time to show); a host's refusal carries the exception's kind.
- *Open for the user*: Python's Save As, delete and list of saved pages (Y-6, H-9: their names); the tab's class grid
  on a Python host and a class of another project (Y-7, H-8: a gesture, or an exception).

**Ledgers.** Every ledger is empty but N1-N3 and the entries that are no gaps (the command path, its plumbing, following
another editor, a kind tested elsewhere): `L8_KINDS` 3 -> 0 (F71, F72); `L8_SOURCES`, `L8_UNASKED`, `MARKERS`,
`UNCHECKED` 0; `L1_*` as at S6. The scans see more with no ceiling raised (F68, F76, F89); the gesture floor 40 -> 51
(F79); two dated pins in state-guardrail.test.ts. Each move dated in its file.

**Gates.** The full local set after the first round: 202 of 203 targets green, the one red the register pin (F45-F70
not listed), fixed and pushed (17c513e72). After the confirmation round (749bf4e0c): 203 of 203 green. After the third
round (7cf6eab6b): 202 of 203, the one red F92's new test clicking a column its grid does not group by -- fixed
(d15a7ba24), green, red without F92's fix. After the fourth round: run with S8's, below (the user, 2026-10-22: no more
CI rounds; the whole local set once, at S8's end).

**The done condition.** The rounds found 40, 21, 6 and 7, each fewer and smaller; the fourth found no new place where
the screen decides what a gesture means, and its races are D19's. S7 closes on that -- and the class of finding the
audits kept turning up by hand (F71, F72: a gesture's meaning decided in screen code) gets a mechanical guard of its own,
S8, the user's ask.

**Next.** S8 (screen code builds no command), then D19, the single writer's client side, with H-5, H-11, H-13, F90's
race and F97.

## As built (S8, 2026-10-22)

**What S8 is.** The user's ask, after the audits twice found a gesture's meaning decided in screen code (F71, F72): find
every place, move each out, and guard it mechanically. Every command the screen applies is now made outside its files
(app.ts, `page/`, `app/`, `ui/`, `layout/band-board.ts`): A1-A49, each closed.

**The grid's gestures** (`datacube/src/grid-gestures.ts`): one table, `GRID_GESTURES`, named by the header menu's own ids
where the menu has the gesture (`pivot.addVertical`, `column.unpinAll`, `heatmap.add`, `sort.clearColumn`, ...) and by
names of the same form where it has none (`zones.set`, `columns.reorder`, `tree.open`, `column.resize`, `view.chrome`,
`header.sort`, `chart.refilter`, `view.dimension`, `column.calculate`, `filter.set`). Each takes the grid as it is
(`GestureGrid`: its state, its dimensions, and -- for the four that read it, flagged -- the headers it paints and its
pivot's columns) and its arguments by name, refuses an argument it does not take or is not given; given from Python's
JSON, its commands are read by the commands' own reader (readGridCommand), as a command Python builds is (the screen's,
typed, are not: the reader stays out of the page's first download). The screen keeps pixels only: Auto-size
measures the content and Size to Fit the widths drawn, then hand the numbers to `columns.autoSize` and
`columns.sizeToFit`, whose floor, ceiling and scaling moved out of app.ts. The menu's dispatch is one line
(`ui/menu.ts menuGesture`: the entry's id and what it carries of what the gesture takes); `menuCommand` and
`applyMenuAction` are gone, their tests moved onto the gestures.

**The page's gestures** (`datacube/src/page-gestures.ts`): what a gesture means where there is something to decide -- a
layout as the page saves it (`savedLayout`, now also what views() writes a page's layouts by: one rule, not two), a hand's
move (`movedCommand`), a sheet's rename and drag (`renameSheetCommand`, `moveSheetCommand`), Move to Sheet
(`moveToSheetCommands`), Show Grid's steps (`showGridCommands`). Where every field is the person's input (a tile renamed as
typed, a sheet deleted, a chart's spec as its panel made it), `pageCommand(kind, fields)`: the kind and the fields, as
Python's generated builder writes them.

**Python.** One way in for every grid gesture, `grid.gesture(name, **args)` (headless `gridGesture`, which runs the grid
first only for a gesture that reads its view), and named methods where a Python user would reach for one: `add_group`,
`remove_group`, `add_pivot`, `remove_pivot`, `heatmap` (the user's answer on the surface was not in when this was built:
built as recommended, (a); the names are the user's to change). `toggle_sort`, `toggle_group_sort`, `dimension`, `drill`
and `drill_up` now ask `gridGesture` too: headless `gridHeaderSort` and `gridDimension` are gone, one function each
behind the screen's control and Python's call. `chart.grid()` asks Show Grid's steps of headless `pageGesture` instead of
writing them itself.

**What changed from the plan.**
- *No generated constructor.* The grid side needed none: every grid command the screen made was a gesture with a meaning,
  so each is a gesture. The page side's pass-through constructor is `pageCommand`, typed by the command union; it is not
  read by `readPageCommand`, whose readers take a page document's JSON (`addGrid`'s cube), not the in-memory forms the
  board holds. What keeps it honest is L11's second check: it builds only the kinds `PASS_THROUGH` lists, each said why.
- *L11 reads text, not types.* The build's TypeScript has no compiler API to ask types of (TypeScript 7, native), so the
  first check finds a command by every kind's `kind: '<kind>'` spelling in a screen file, the three spellings that are
  no command listed by file (the board's LayoutMove, the filter editor's draft group). The inventory itself was taken by
  types (TypeScript 6's compiler API, outside the build), the menu's entries -- typed a command or null -- by reading.
- *Check 3 is wider than planned.* Not "a screen imports no rule internal" (the screen reads many rule modules to
  paint), but: every function that returns commands and is given none, called anywhere in screen code (imported, loaded
  later, handed over), is asked by headless too -- directly, or through a gesture table headless dispatches -- or is the
  screen's own, said why (`SCREEN_OWN_MAKERS`: a form's edits, the board's hand, a sheet tab's drag, the tab's page undo,
  the tab opening a served page, the person's input passed through). Its first run found three the inventory had missed
  (A49).
- *Items A1-A49, not A1-A60*: the menu's fourteen commands are one item (A10); a page filter's two x's one (A47).

**Ledgers.** L11 closed at 0 (no ceiling: nothing in it is a gap); L8 reads a kind made by `pageCommand` and by the
gesture tables (`exportMakes`); the register's item pattern takes `A`; menu-ids.test.ts counts an entry its gesture
makes as handled. No ledger ceiling moved. Three bundle budgets did, each dated (bundle-budget.test.ts): the grid page
316,000 -> 319,000, the engine page 306,500 -> 310,000, a notebook's module 324,500 -> 328,000 -- grid-gestures.ts
(about 2.8 KB gzipped, its refusals in words) and page-gestures.ts (0.8 KB) load with the page, less what app.ts and
the menu lost (about 1 KB). Loading the gestures later was weighed and refused: the selection-off path would wait on
an import, and F93's order with it.

**Gates.** Run locally, no CI (the user, 2026-10-22). The first full set ran out of disk mid-way (marimo's and
JupyterLab's tests never ran) and found three reds: menu-ids.test.ts (a gesture's menu entry counted dead: it now counts
one menuGesture makes), the headless roster (the two new test files listed) and the bundle budgets (raised, above).
After them, the whole local set: 205 of 205 targets green, and `//datacube:verify_app_test` and `//python:wheel_test`
green. This run is also S7's fourth round's (its own was stopped by the user's no-more-CI word).

**Behaviour changes, deliberate.** Minimize All minimizes the columns the grid shows (it took hidden ones too); a column
picked in the columns panel that is a row group already changes nothing (it was refused, named twice); a gesture that
changes nothing makes no undo step (Unpin All with none pinned did not either; Minimize with every column fixed did);
a menu entry's undo step is named as the entry is ("Collapse All", not "collapse all").

**The review's fixes (2026-10-23, the user: "do the fixes").** After a look back at S8 found its own weak spots:
- *Arguments typed.* `gridGesture` takes each gesture's arguments as `GridGestureArgs` types them, so a misnamed or
  mistyped argument in screen code does not compile (it had been an untyped record, checked only when run); a gesture
  named by text -- a menu entry's, Python's -- goes through `gridGestureGiven`, checked as it comes. One table behind
  both (`made`); headless asks the second, and L11 counts the two as one.
- *L11's fourth check:* a screen's gesture arguments make no list and read no state of the grid -- the blind spot the
  first three left (`zones.set` given `[...rows, c]` decides in screen code what `pivot.addVertical` decides).
- *pageCommand written from the field lists,* as Python's builders are: the fields in `PAGE_COMMAND_FIELDS`' order, an
  optional one not given left out, a field the list has not thrown.
- *Python's gesture values tagged* (a decimal, a date) as `add_filter`'s are, and untagged by headless gridGesture.
- *A sheet renamed to the label it shows,* with no name of its own, tells nothing (renameSheet would take it as none:
  an empty undo step).
- *An independent audit of S8* (the program's rule for every step), read-only over the commit: the races, the ordering
  of a click's refilter against `#untold` (F91, F93) and Python's locking (D15) clean; every converted page-board site the
  same commands; nine findings -- guard holes, Python's `name`, commands for nothing, a query the heatmap did not need,
  words gone stale -- each fixed or declared (A55-A61). The review's own: A50-A54, A62. After them, the whole local set
  again: 204 of 205 green, the one red the new Python test's own last check (written before its dimension step changed
  the row groups), fixed and green; `//datacube:verify_app_test` and `//python:wheel_test` green.

**Next.** D19, the single writer's client side, with H-5, H-11, H-13, F90's race and F97; and the user's answers on the
Python names (S8's surface; Y-6, H-9: Save As, delete and list; Y-7, H-8: the tab's class grid on a Python host).

## As built (D23, 2026-10-23)

The S7 audits' four asked items, built (D23): what the tab does with saved pages and classes, Python does too, and the
reverse -- each by one rule both call.

- **Save As (K13).** page-document.ts `pageSaving`: the Save window's one rule -- over the saved page it is, or, Save As,
  a new record under the id it is handed, the record it was saved as left alone. The tab's Save As asked it in place of
  its own two lines (`const { record: _was, ...asItself } = doc`); headless `pageRecord` takes `asNew`; Python's
  `page.save_as(name)` and `save()` share one path (`_saved`).
- **Delete (K14).** `ll.Page.delete(id)`: the store's DELETE, its rules (only the owner's); then every page of the
  session saved as it is told `savedAs` of none -- as Open… tells the tab's page -- so its next save is a new record,
  never a refusal. The session keeps a weak set of its pages (`page_objects`) for it.
- **List (K15).** cube-store.ts `savedSearch` and `SAVED_SORTS`, moved out of the Open… window (its term trimmed, its
  menu's order, its first the default); headless `pageSearch` reads Python's arguments and refuses in words;
  `ll.Page.saved(search, sort=, limit=)` posts it to the store, which answers by its rules (this user's first, newest
  first, upstream's cap of 100).
- **The tab's class grid on a Python host (K16).** The guard `if (services.engine.local)` (its comment stale since X1,
  X4) is gone. A class the tab names is opened by sources' new `class` kind: here, where the page plans its own SQL
  (class-source.ts `classGiven`, then `openedClass`); on Python's page, its cube made by the host (engine.py
  `source.json`: headless `sourceCube`, page.grid(cls=...)'s own; python-host.ts `made`) and then opened as every
  grid of that page is (`opened.json`).
- **A class of another project, in the tab (K17).** The source picker's **Classes**: a class by its path (or name), a
  project, a revision. class-source.ts `classGiven` is the one rule naming it -- moved out of headless `givenSource`,
  which asks it too.

**Tests (each fails without its change):** page-document.test.ts (Save As: the new record, `over`, the refusals),
cube-library.test.ts (the window and Python ask the same search; `pageSearch`'s refusals), class-source.test.ts
(`classGiven`'s cases and refusals), python-runner.test.ts (`made` posts `source.json`, its refusal said),
datacube-app-live.test.ts (on Python's page, Classes asks the host to make the cube, then to open it), test_page.py
(Save As, list, delete and the pages saved as it; `source.json` makes page.grid's cube, opened by `opened.json`, its
refusals).

**Ledgers.** No ceiling moved; K13-K17 added to the register and closed (one-api-ledger.test.ts `ALL`). And a new one,
**L12** (the user: "Why do we keep finding more places where ... the two hosts diverge?", then "Check host branch now"):
K16 was a branch on the host -- `if (services.engine.local)` -- that no ledger saw, since every earlier one asks about
commands, and an ability gated to one host is none. L12 lists every place screen code branches on its host (94 reads at 77 places once the
review's widened scan ran -- R1 below --, by function and capability: the page's own engine, an ability of its database,
a service, its address, a grid's runner, or a name holding one) with what the other host does there: `both:` its own
path to the same thing (67 places), or `by nature:` why it has none (10: a dataframe lives in Python; a Share link needs an address others can open; a page another editor
serves is followed). A new branch fails the build until it is listed and said, so whether the other host needs it is
decided as it is written. Its self-test scans an added branch, by the capability and by a name holding one, and a plain
use that is no branch; adding `if (services.engine.local)` to sources.ts fails it, as it should.

**L12, strict (the user: "Do the strict and missing tests").** L12 proved each branch on the host was decided, not that
the two hosts do the same thing. Now each `both:` entry names the ability it is a way to (11: rows, saved pages, model
actions, class, join, warehouse, remote file, file, Snap, saved query, reopening a grid), and each ability names the
tests that run it on the tab and on the Python host (Python's own tests, at least one) -- checked to exist, by file and
test name. An ability the two hosts do by one implementation, DataCube's own run headless on the host (`shared`: model
join and reopening -- the host joins by join-open.ts `openJoin` and opens a saved grid by cube-document.ts `rebuilt`,
as the tab does), needs no more. One each host does by an implementation of its own (`two`: rows -- one compiler in two
builds over two DuckDBs --, saved pages -- the store's Java in two builds --, model actions, a class and a saved query
-- DataCube's own reading, `classOpened` and `savedQueryOpened`, over a model home or a query store that is
legend-lite's in two builds, R3 --, the warehouse's driver, a remote file, a file, Snap) names the test that runs the
same input both ways and holds the two to one result. Most had one: the cube corpus through PlanAndRun on Python and in
the tab, every reader file on both, the conformance suite of the saved pages, the SDLC and the query store run against
each build, and both warehouse drivers held to one recorded corpus. Two had none, and have now, in the browser harness (`//datacube:python_engine_test`, a real tab beside a real Python engine):

- *a remote file reads the same on Python's page and in the tab*: one Parquet file, served by a server on this machine,
  mounted by the one implementation (upload.ts `mountRemoteView`) in the tab's DuckDB-WASM and in Python's DuckDB, every
  row planned and run on each -- the same columns, types and values (the fixture's server now answers ranges and across
  origins, as DuckDB-WASM asks);
- *a grid snaps the same copy on Python and in the tab*: the frame's every column copied by the tab's SnapManager into
  its DuckDB and by Python's snap.json into its own, each copy modelled by the one writer (upload.ts `modelLanded`) and
  read through that model -- the same columns, types and rows, every row of the frame.

A `both:` naming no ability, an ability no branch is a way to, one proved on one host only, a `two` with no parity
test, or a proof whose test is gone: each fails the build.

**Gates.** The full local set, 205 of 205.

**Next (as written 2026-10-23).** The PR (S8 and D23), then D19 in its smaller form (D23) -- superseded by D24: D19
was built in its full form, below.

## As built (D19, 2026-10-24)

D19 in its full form (D24; the design `docs/DATACUBE_LIVE_EDITING_2026_10_24.md`, written from file:line homework
before any code), built in four slices on `claude/datacube-d19` (c3c784d9 the note; b94d5cb9 slices 1 and 2; 1f58955b
slice 3; 696d9867 slice 4), each green on the datacube and Python sets before the next. The register's rows W1-W19
above say what each item is and which tests hold it; this is the whole.

- **Undo and redo are commands (W1-W5, W18, W19).** Two total diffs, `grid-between.ts gridBetween` (over the saved
  form: the query, the configuration, the tree; `tree` a command, `before` on `calculate` / `editCalculated`) and
  `page-between.ts pageBetween` (the page's structure in two scopes, total over detached charts by the new `detach`
  command); a step of either history is `{forward, inverse}` commands, planned when the step is made; an undo is told
  as the commands between, which the keeper judges by every rule a write is; a tile brought back is a new tile (B12).
  The restore write is gone end to end (`L9_KEEPER` 6 -> 5; N2 retired), so no editor ever writes the page whole.
- **The tab converges (W6-W14, W16, W17).** A write's answer carries `since`, `edits`, `renamed`, `hash`; a combined or
  refused write is converged on by one function (`live-converge.ts convergeOn`: the board put back at the confirmed
  page, the keeper's renames followed, each touched grid put back by its own commands, the other editors' steps then
  this screen's applied, the hash checked, the pending edits made again) -- no read, no reopen. The two reopen causes
  that remain (a `rebuilt`, a write from further back than the keeper keeps) and the hash mismatch are each said, and
  no edit is dropped unsaid. A follow and a Save write first; a write waits for the gesture in flight.
- **The proof (W15).** Three soaks as gates: two real tabs on a keeper as Python's (`live-soak.test.ts`), Python with
  two open pages undoing and redoing (test_keeper.py `Converges`), and two browser tabs on one `ll.Page`
  (python-engine.mjs). The TypeScript soak caught two bugs in its first sixty seeds, both fixed with unit tests before
  it landed (W15's row): the follow's board read once per move, and the converge's hash checked over a screen still
  carrying pending edits.

**Ledgers, before and after.** `L9_KEEPER` 6 -> 5 (shrink: the restore write). `L1_ROUTES` 5 -> 7 (two live-converge
entries, dated: the converge's own path puts the board and the grids back, told nothing -- the keeper's page is what
it makes), then 7 -> 6 (the audit's 2: the app's own follow route is gone, a follow being the channel's converge). `L8_KINDS` 0 -> 1 (`detach`: Python makes none; no gesture but an undo tells it). `L1_GRID`'s `#undo` /
`#redo` reasons rewritten (the owner's moves are told). `SCREEN_OWN_MAKERS` gains `gridBetween`. `ITEM` gains `W`;
`ALL` gains W1-W19, every one closed. `L8_UNASKED` stays empty: the three tab functions the Python soak undoes by are
bundled for that test alone (`//datacube:headless_tab_bundle`), not exported by the product's headless. The gestures
ledger's `restores` arm is gone (the five restore gestures are told gestures). `PAGE_STATE` (state-guardrail) gains
`step | putBack | rename` with the sites that call them. No budget moved.

**Out of scope, as the note says (§9):** presence, CRDTs, a keeper outside Python, Python's own undo (Python has no
history; its undo would be `pageBetween` over two documents it holds, a program's to write), the mutation harness as a
gate, the two-thread test with two open pages.

**The independent audit (2026-10-24, after slice 4; an agent that wrote none of it, against the note's §7 and the
rows above).** Thirteen items built as claimed, six with a difference, none not found. Its findings, each fixed the same
day with a test that fails without the fix, are on the rows (W2, W8, W10, W12, W13, W17): a refused batch's grid edits
left on screen, so the hash check reopened the page (1, HIGH); an edit made during a follow dropped unsaid, the same
way (2, HIGH) -- a follow is now the channel's converge; the keeper's hash read after another writer's write, a false
mismatch (3, MEDIUM-HIGH); a converge restarting a grid's history the other editor never touched (4, MEDIUM); the
confirmed ids shrinking across a follow (6, LOW; gone with 2). Declared, not built: a gesture during an async converge
is dropped by `edit()` as the other editor's (5, LOW; pre-existing, now visible as the never-show message), and W2's
known limit. The audit also found no deadlock in the channel's queue, every ledger move dated and justified, and two
doc nits (fixed: the note's `SCREEN_OWN_MAKERS` line, the guardrail's N2 wording).

**The audit's second round (2026-10-24, on the commit that fixed the first: an agent that wrote none of it, with
reproductions over the commit's own harness).** It found one regression and three defects in that commit, two older
ones, and one design point; all but the design point are fixed the same day, each with a test in live-channel.test.ts
that fails without the fix. (1, a regression) a grid only this screen's edits changed was put back at its SENT cube,
but a PAGE step of the keeper's can change a grid's cube too -- a selected chart's removal takes its selection off its
grid's filter -- and the two editors then held two pages, silently: the converge now applies the keeper's page steps to
the confirmed page, purely, and any grid whose cube they change starts again (put back at the confirmed cube, the
steps replayed). (2) the hash was read while a grid's change was still in flight (a chart's removal refilters its grid
asynchronously; `page.ready()` waits for grids to open, not to settle), so the check could pass on a state passing: the
converge waits for every grid to be quiet before it reads the page. (3) `keepHistory` with two or more pending edits
on one grid recorded each again on top of its old step, and an undo went forward: a pending grid edit is made again as
no new step (`CubeApp.edit(..., { again: true })`, its step being in the history still), a restarted grid's as new
steps (`Converged.restarted`). (4, older) a grid added here with an edit pending at a combined write was remade from
its current state, pending and all, so the screen held more than the keeper and reopened: the grids kept are now put
back at their sent cube after the steps, remade or not. (6, older) a put-back synced the board as the person's own, so
a chart taken off by the keeper recorded a step on its grid: it syncs as the keeper's. (7, older) a write that could
not say its page left the confirmed page stale: it is none until the next write, and a converge meanwhile opens the
page again, said. (5, design, kept) a second mismatch in a row keeps the page as opened rather than reopening again --
the one reopen loop guard -- so a page that systematically differs is said, not chased; W17's row says so. The gesture
during the converge itself (the first audit's 5), declared then, is built too: the page is held through a converge
(`PageApp.hold`), a gesture meanwhile doing nothing, neither applied nor told, as a locked page's Undo does nothing --
page-app.test.ts, live-channel.test.ts. And the second audit verified what held: a `rebuilt` step still reopens, the
frame-moved check still applies, the saved-name bookkeeping runs only on a follow that succeeded, writes and follows
are serialized in the channel's queue, the keeper's hash is read with the write everywhere, and the first round's
tests fail without their fixes.

**The audit's third round (2026-10-24, on the second round's fixes), and the mutation pass.** The third auditor found
the hold covering the page only: a GRID gesture during a converge landed on a grid being put back, was told, written,
and made the page reopen with the never-show message (1, HIGH) -- the converge holds every grid too (`CubeApp.hold`,
a person's edit refused meanwhile, said: "the page is catching up with its keeper: a moment"; a grid made again by a
step held as it appears); the put-back's drop of a chart added here refiltered its grid as another editor's and wiped
the grid's history (2) -- the put-back drops without refiltering, the grid being put back whole by its own commands;
a grid let go by the put-back and made again by a step is a new grid whose history was empty, its pending edits made
again as no step (3) -- a remade grid counts as restarted, its pending edits steps of its new history; and a `detach`
without a copy un-snapped a kept cube another chart read (4) -- the field absent leaves the copy as it is, null is its
live rows, and the diff says it always. It verified the second round's fixes otherwise: the pure simulation is sound
(the only page arms that change a grid's cube are `unselected`, `addGrid` and `rebuilt`), the remade grid is the one
the converge sees, `record: false` leaves the redo branch and the stack sane, a grid's refilter is `busy` from before
the put-back returns, the keeper's hash is read with the write everywhere. The mutation pass (the note's §6, run by
hand in a scratch copy: each fix reverted, the channel suite and the thirty-seed chart soak against it) killed every
mutant but one: the second round's regression test used an own edit that overwrote the very field the keeper's page
step changed (the whole filter), so it passed without the fix, and the soak's mark clicks were too rare to reach the
case -- the test groups instead now, and the soak clicks a mark on most charts it makes and has Python take a
selected chart off first; the mutant is killed by both since. The quiet wait is killed by the soak alone (seed 6), the
others by the channel suite; the control (pending grids not put back) by both. The table is the proof's: a test that
has never failed against a known bug is not evidence. The rounds' budgets, each with a dated note for the user's
sign-off (D9): the notebook module 341,000 -> 341,500 (the second round, measured 341,063) and the engine page
322,500 -> 323,000 (the third, measured 322,567). Beside the round, main's CI on Windows went red on the second
round's commit by one pre-existing test of cube-page-shell.test.ts (`the layouts` null): the layouts are fetched on
the page's first Arrange..., and that first dynamic import outlasted `settle()`'s twenty ticks on the Windows runner;
every test that opens the layouts now waits for them, bounded (cube-fixture's `shown`).

**The audit's fourth round (2026-10-24, on the third's fixes), the faulted three-tab soak, and the long session.** The
user asked whether the program was bulletproof enough to build on; the honest answer was that three rounds in a row had
each found defects, so a fourth round was run with "nothing to fix" as its exit, and the soak was widened at the same
time. Neither came back clean. The fourth auditor found six defects (two HIGH) and reproduced four against a real page:
(1) a PENDING page step that changes a grid's cube (a selected chart removed while a write was on its way) was not put
back -- the grid lacked the condition the confirmed cube holds, the hash differed, the page reopened and the removal
was dropped; now the pending page steps are applied to the sent page purely and the grids they change join the own
put-back; (2) a pending maker whose planned id another editor's tile got meanwhile was dropped at the reapply (its id
"held already"), and its step left in the history then took the OTHER editor's tile off on Undo; now `PageApp.reapply`
makes it again under a fresh id (page-between.ts `applyPlanned`, the keeper's own rule) and the history follows the
rename, and a pending GRID maker that collides is moved to a fresh id before the keeper's steps are followed
(`renamePending`, the converge's step 0, the renames back to the channel through `Converged.renamed`); (3) a grid the
keeper renamed was made again from the screen's current state, not the keeper's `addGrid` document, so a non-idempotent
sent command (a header sort) applied twice; now a page's sync makes a grid again from the cube the page holds for it;
(4) a grid's undo and redo, Properties and the snap toggle bypassed the hold; now every way of changing a grid is
refused while its page converges; (5) the grids the put-back made again, and those a step added, were unheld until after
step 3; now held as they appear; (6) this screen's own chart removal, replayed as its sent edit, wiped an untouched
grid's history (a no-op refilter applied as another editor's cleared it); now a sent page step is made again as the
screen's own, and a change that changed nothing restarts nothing; and (7, LOW) a poll stale by the time its work ran
converged on nothing, holds and all; now a follow with nothing moved since the version shown does nothing. The
three-tab soak (a third tab, and the keeper's faults: a write it never sees, an answer lost, in the mix) found four
more in its first forty seeds: a REFUSED batch's page step that changed a grid's cube was not put back either (seed 10:
the same hole as the auditor's 1, for the refused batch); a grid only this screen's edits changed was put back at its
sent cube but its charts' specs in the page's state stayed the confirmed grouping (seed 17: the state now follows the
own cubes, page-commands.ts `followedPage`); a grid brought back by an undo took its old maker to its new id, and a
converge that still needed the old id had none (seed 25: a grid of the keeper's page this page cannot make again is
opened from its cube, as another editor's is); and a refused addition's id lingered as a maker the page "knows", so
another editor's grid under that id was made from this page's maker over this page's source (seed 27: another
editor's grid is never made from a maker this page keeps, and a refused batch's steps come off the history --
PageState `dropSteps` -- so an undo of a refused addition never takes off what the keeper gave that id to since). A
test-only finding beside them: the two harnesses opened another editor's grid from its source alone, at the fixture's
default cube, never the document's (fixed in both). The long session (300 steps, one seed) found one more: another
editor's mark on a chart and the grid's undo of its refilter, followed in ONE move, left the selection the keeper took
off (the keeper judges a chart's selection where the filter moves, step by step; a run applied as one change shows the
filter where it ends); now a run that moves the grid's filter twice is applied a command at a time (live-follow.ts).
Every one of these has a test that failed before its fix (live-channel.test.ts, "the D19 audit's fourth round",
tests 1-10, and "a real page and its keeper"). The soak runs 24 seeds in CI now (6 before; `medium`), three tabs, the
faults in; forty seeds with and without faults and three 300-step sessions pass by hand. Beside the round, a page
rename by another editor counted as a change of every grid's cube (each cube's `name` is the page's) and restarted
every edited grid's history (W13); the converge compares cubes less the page's name now. Open, LOW, for the user: a
pending chart removal made again after a converge records its grid's unselect as a step once more (the grid was put
back at its sent cube, which holds the condition), so one Undo lands on a like state; and the words a tab says on a
lost answer ("not kept") when the keeper kept the write. **What this round says about the program:** the defects are
all interactions of the converge's own/whole put-back with pending edits, renames and refusals; each is small, each has
its test, and the soak now reaches them, but four rounds finding defects is not a foundation proved sound. The user's
question stands answered: not bulletproof yet; the next round, with the soak as it now is, decides.

**The full local gate (`//gates:local`, 2026-10-24, in the cloud session after `tools/cloud-distdir.sh`).** 405 passed,
11 failed: eight `initdb refuses root` (the seven core Postgres tests and `verify_app_test`: the container runs as root,
docs/CLOUD_SESSIONS.md; CI runs as a user) and `browser_guard_test` (the proxy's certificate on extensions.duckdb.org:
the session's, not the code's) are the environment's; two were this program's and are fixed: `headless_roster_test`
(grid-between.test.ts joins `_HEADLESS_TESTS`) and `bundle_budget_test`, the budgets raised with dated notes for the
user's sign-off (D9): the grid-only page 323,500 -> 332,000 (measured 331,197), the engine page 314,000 -> 322,500
(measured 321,768), the notebook module 332,500 -> 341,000 (measured 340,482) -- the two total diffs, the converge and
the synchronous SHA-256, about 8 KB gzipped each.

**The middle version (2026-10-24, the user's decision on `docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md`).**
Unique ids under each tab's tag (nothing renamed: the keeper refuses an id the page holds), the keeper answering a
combined write with its page and the screen set to it in one pass (the six-step converge, the `since=` follow and the
keeper's steps gone), and write ids taken once, a lost answer resolved by asking. Its As built is that note's §8.

**Next.** The PR; the GATES entry at landing.

