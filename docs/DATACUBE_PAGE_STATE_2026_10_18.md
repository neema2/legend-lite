# DataCube commands, stage 2: one page state, page-level undo

The command program's stage 2 (`docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md` §9), phase 2 of the roadmap run
(`docs/DATACUBE_ROADMAP_RUN_2026_10_18.md`), on branch `claude/datacube-page-state`, stacked on phase 1
(`docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md`).

## The goal

A page of its own has one state: its document as the page commands edit it (page-commands.ts `PageDraft`), held by one
owner, with one history. A page edit is undone and redone as a step of that history, as a grid's edit is a step of its
own owner's (state-owner.ts). Until now the page's only undo was a sheet's layout ("Undo Layout"); a tile removed, a
sheet deleted, a tile moved to another sheet or renamed could not be undone.

## The owner

`datacube/src/page-state.ts`, `PageState`: the page's draft and its history. Pure, no DOM, tested alone.
- Commands applied as one step (undo takes them all back): `applyToState(state, commands, label)` in page-commands.ts,
  the commands as applied returned -- what a host editing the page by commands uses (stage 3's live channel, tests).
  It lives beside the function, not in the state, so the screen's page, which only begins and keeps, does not
  download the page commands' rules.
- `begin(label)` and `keep(draft)`: an edit its owner made itself (the screen moves a tile, then says so) is a step
  begun from the page as it is, and `keep` brings the draft to the page the owner made. `keep` is also how what the
  page's own owners change reaches it with no step: a grid's own edits (its cube: the grid's own owner undoes them), a
  chart's spec as it follows its grid, the page's name (its host's).
- `undo()` / `redo()`: the draft a step back or forward, with the step's label.
- The ids held only grow: a step undone gives its ids back to no one, so a handle never meets another tile under its
  old one's id; a step redone brings its own tiles back under theirs.

## The screen

- **The page on screen is the state's draft.** PageApp holds the `PageState`, seeded from the page it shows, and keeps
  it as the screen is after every change (`keep`, on each `onChange`: the page's views and sheets as `views()` writes
  them, each grid's cube as it writes one, the ids held). Every structural edit the screen makes is a step: CubePage
  tells it as its commands and their label (`onPageEdit`) before the change it makes reaches its host, so the step goes
  back to the page as it was kept before the edit.
- **Why the screen's own page and not the function's:** some of what the page shows changes by no page command -- a
  chart's selection (its grid's filter, the grid's own step), a following chart's spec as its grid is pivoted, the
  query a detached chart keeps after Open in Grid's Update. Kept from the screen, the state cannot drift from it; the
  tests hold the screen's steps to the function (each told edit, applied to the page before it, is the page after it:
  phase 1's replay property, extended to every edit the screen now tells).
- **What the screen now tells too** (phase 1 left them to this stage): a grid added (`addGrid`, its cube document as
  its grid writes one -- or `null`, a grid whose source the page cannot write down, a page that cannot be saved either
  -- placed where the screen put it), a chart added (`addChart`, with its spec), and a chart's spec changed by hand
  (`setChart`: its options, its freeze, a drill, Open in Grid's Update).
- **Undo and Redo** -- the page's menu, in place of Undo Layout and Redo Layout ("Undo Delete Sheet"), and Ctrl+Z,
  Ctrl+Shift+Z on a tile's frame -- move the state a step and put the screen at it: `CubePage.sync(to, from)`, going
  from the page on screen to the step's page. Sheets added or removed, named and ordered as it says; tiles removed (a
  chart's selection taken off its grid's filter as its own remove does); tiles added; a tile's title, its sheet, each
  sheet's layout; a chart's spec, its selection, which grid it reads. A tile the state never held (a chart with no spec
  yet) is left where it is; a chart's editing grid goes back beside its chart, and closes when an undo makes its chart
  follow its grid again (as the pill's own click closes it).
- **A grid brought back** by an undo is made again by the host's maker that made it, starting where it was when it
  went -- its query, its configuration and its open groups: the same grid, not a copy. PageApp keeps each grid's maker
  and start while a step of the history can bring it back, and says which grids those are (`recoverable`): the host
  keeps what each reads -- a file's table, its planner -- until none can (DataCube's app drops a file's table with its
  last grid). A grid kept off the board for a detached chart is replaced by the one made again, and its detached
  charts follow it once more, their selections with them.
- **A chart brought back** with a selection puts that selection back on its grid's filter, by the grid's own change
  (`refilter`, as a click on its mark does): one more step of the grid's own history.
- A locked page (view mode) undoes nothing, as Undo Layout did not.

## Behaviour changes

- **The page's menu:** Undo and Redo, each naming its step, in place of Undo Layout and Redo Layout; one history for
  every sheet (a sheet's arrangement was undone only from that sheet).
- **A page the commands edit writes its views in the screen's order** (page-commands.ts `inScreenOrder`): the grids in
  the sheets' reading order, then the charts in the order they came -- so a page Python edits is written as the screen
  writes it (a grid added after a chart was written after it; a grid moved kept its old place among the views).
- **`addGrid`'s document may be null:** a grid whose source the page cannot write down (the screen's, over rows it
  cannot name) -- its view held with no cube, a page that cannot be saved, as the screen's Save already says.
- **A chart removed that has no spec yet, or a chart's editing grid renamed or moved,** is no step: the page never held
  it.

## How it is tested

- `page-state.test.ts`: apply, undo, redo, a new step clearing what was undone, a refused step changing nothing,
  begin and keep, ids only growing, the history's limit.
- `cube-page-shell.test.ts`: every edit the screen tells, undone and redone, puts the page back as it was (`views()`
  and the cubes), the state the page on screen after each, each told edit one step that replays to the page it made
  (its views and its cubes); and the independent review's findings, each pinned (a grid's source kept while it can
  come back, a frozen chart's selection, a chart following again told as it draws, a grid's open groups, a detached
  chart's Update undone past its grid's removal, an editing grid beside its chart, a chart's form one step).

## Not in this phase

- **One undo for a grid's own edits and the page's together:** a grid's edits stay its own owner's steps (its own
  Ctrl+Z, its own menu).
- **The cube app's own page** (a cube alone with charts beside it) keeps Undo Layout: it has no page document.
- **A detached chart's Update** is undone as its spec; the query it keeps after the Update stays (no page command says
  which query a detached chart reads).
- **Stage 3**, the live channel: commands from Python applied to the open page's state, the screen synced to it.
- **An undo or a redo told as commands** to a host following the page by them (`onPageCommand`): the state holds the
  pages, not the inverse commands. No host follows the page by them yet; the reverse channel (the screen's edits sent
  to Python) will need it, or will send the page whole after an undo.
