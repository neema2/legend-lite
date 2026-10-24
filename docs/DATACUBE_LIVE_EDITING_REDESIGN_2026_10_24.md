# DataCube live editing, redesigned: one store, the keeper's state, the screen a projection

**Status:** proposal for the user's decision, 2026-10-24. Written after the fourth audit round of D19
(`docs/DATACUBE_LIVE_EDITING_2026_10_24.md`, PR #71) still found defects. **The user chose the middle version the same
day** -- unique ids, the keeper answering with its page, write ids -- and it is built (§8). The full design (the store,
undo at the keeper, patches) is not built; it stays the plan if the middle version proves short.

**The decision asked:** replace D19's converge with the architecture below (§3), in the phases of §6, or keep
patching the current one. The recommendation is to replace it. §7 lists the product decisions that come with it.

**Corrections from an independent review (2026-10-24), which supersede the sections they name.** The review
confirmed the direction: the keeper as authority, state sent as patches, mutation ids, client-made ids, and a
client store that replays its pending mutations. It found four mechanisms wrong as written:

- **§3.8 is wrong.** Python cannot write under the keeper's lock: the lock is not reentrant and every Python call
  reads the keeper again (`keeper.py:128`, `page.py:1307`, `page.py:1336`). Python stays an ordinary client that
  pushes mutations, without its rename path.
- **§3.6's undo moves to the keeper.** A comparison made at press time on the client races with other writes and
  compares against a prediction. Undo is a guarded `undo {of: mutation}` evaluated by the keeper against its own
  record of what that mutation changed.
- **§3.2 needs a resurrection rule.** An undo brings a removed tile back under a fresh id today (B12), so renames
  survive unique ids. A tile brought back by an undo should keep its id; tags already prevent an id going to a
  different object.
- **§6's phase A must be additive.** The new routes sit beside the old ones until phase B removes the old client.
  The duplicated side effects (R3) are switched off in phase B, not D.

It also recommended: grid edits keep CubeApp's existing pending pipeline and join the store's view when the keeper
confirms them, which keeps today's transaction and removes the "checked before dispatch" contradiction of §3.5;
and phase E is dropped, since absolute-only ops lose concurrent intent. The full review is not in the repository;
its findings are these.

---

## 1. Verdict on the current implementation

### 1.1 What is right and stays

The domain layer is sound and the redesign keeps all of it:

- **One reducer per kind of edit.** `applyPageCommand` (`page-commands.ts`) and `applyGridCommand`
  (`grid-commands.ts:113`) are pure and synchronous. The keeper runs the same TypeScript through headless
  `pageRebase`. Every side effect of a page step on the document (a removed chart's selection off its grid's
  filter, charts following their grid: `followedPage`, `page-commands.ts:701`) is already in the reducer.
- **Total diffs.** `pageBetween` and `gridBetween` turn any two documents into the commands between them. They
  are the right tool for undo and for putting a screen at a document.
- **The keeper as authority.** One copy of each page, versions, every write applied by DataCube's own rule
  (`keeper.py:223`, `settle`), a canonical hash.
- **Gestures resolved to commands** in one place (`grid-gestures.ts`, `page-gestures.ts`, ledger L11).
- **The test harnesses**: the soak, the fake keeper, the two-browser test.

### 1.2 What is structurally wrong

Four audit rounds plus the widened soak found about 25 defects. Every one traces to one of five causes. None is
a slip that one more fix closes; each is a property of the design that keeps producing new interactions.

| # | Cause | Where it lives |
|---|---|---|
| R1 | **Two states of record.** A grid's state of record is its widget's `CubeStateOwner`, committed only when its query answers (`state-owner.ts:1-30`, `cube-state.ts:82`). The page document is read off the widgets (`PageApp.#seen`, `page-app.ts:933`). So converging means driving live widgets through a choreography until they happen to show the keeper's page: put back, replay theirs, replay mine, put back mine again, wait for quiet, hold everything meanwhile (`live-converge.ts:61-255`). | client |
| R2 | **Ids planned from a shared counter, then renamed.** `nextPageId` takes max+1 over the ids the page holds (`page-ids.ts`). Two editors plan the same id. The keeper gives one a fresh id (`page-rebase.ts:182-190`), and everything keyed by id must follow: makers, kept grids, the history, pending edits, Python's handles (`page.py:1389`). | both |
| R3 | **Side effects implemented twice.** The reducer takes a removed chart's selection off its grid. So does the widget, through a callback, at another moment (`cube-page.ts:1532-1537`, `app.ts:2758`). The chart widget follows its grid itself too (`chart-panel.ts:164`). The two copies must agree at every interleaving. | client |
| R4 | **Two histories and relative edits.** The page's steps (`page-state.ts`) and each grid's stack of committed states, with a rule that a grid's history restarts when another editor touches it (`app.ts:1063`, W13). Some commands are relative (`clickSort` toggles, `addFilter` appends, `detach` without its copy), so a replay on a moved base applies them differently, or twice. | client |
| R5 | **Writes have no identity.** A lost answer cannot be told from a refusal. There are two answer paths (combined, not combined). The hash had a read race. | protocol |

The defects, each by cause:

| Found by | Defect | Cause |
|---|---|---|
| Audit 1 | A refused batch's grid edits left on screen | R1 |
| Audit 1 | An edit made during a follow dropped | R1 |
| Audit 1 | The keeper's hash read after another writer's write | R5 |
| Audit 2 | A page step that changes a grid's cube missed by the put-back | R1, R3 |
| Audit 2 | A grid added here with a pending edit remade pending and all | R1, R2 |
| Audit 2 | The put-back synced as the person's own | R1, R3 |
| Audit 2 | A write with no page to say left the confirmed page stale | R1 |
| Audit 2 | A gesture during a converge (declared gap) | R1 |
| Audit 2 | `detach` without the cube's copy (declared gap) | R4 |
| Audit 3 | The hold covered the page, not its grids | R1 |
| Audit 3 | The put-back's chart drop wiped a grid's history | R1, R3, R4 |
| Audit 3 | A remade grid's history empty | R2, R4 |
| Audit 3 | `detach` un-snapped a shared cube | R4 |
| Audit 4 | A pending page step that changes a grid's cube not put back | R3 |
| Audit 4 | A pending maker under an id the other editor got: dropped, then Undo removed the other's tile | R2, R4 |
| Audit 4 | A renamed grid remade from the screen's state: a sort applied twice | R1, R2, R4 |
| Audit 4 | Grid undo, Properties and Snap bypassed the hold | R1 |
| Audit 4 | Grids remade by the put-back unheld | R1 |
| Audit 4 | The screen's own chart removal, replayed, wiped a grid's history | R3, R4 |
| Audit 4 | A stale poll converged on nothing | R5 |
| Soak seed 10 | A refused batch's page step that changed a grid's cube | R3 |
| Soak seed 17 | An own grid's charts kept the old grouping in the page state | R1, R3 |
| Soak seed 25 | A grid brought back by an undo left no maker for its old id | R2 |
| Soak seed 27 | Another editor's grid made from this page's maker | R2 |
| Long session | A run of another editor's commands applied as one change | R1, R3 |
| Audit 4 aside | A page rename restarted every edited grid's history | R4 |

**Why patching does not converge.** The converge must reproduce, step by step, what the widgets do on their own.
The state it must reason about is the product of widget state, converge step, pending edits, renames and the
history. Each audit and each wider soak samples that product and finds a new corner. The soak can only search the
space. It cannot prove there is no next corner, and four rounds say there is one.

---

## 2. What experts build for this problem

The problem is a document edited by a handful of clients through one central authority, with optimistic local
edits, per-user undo, and a server-side editor (Python). This is the problem Figma's multiplayer, Replicache and
Linear's sync engine solve, and they converge on one architecture:

1. **The server is the authority** and orders every change. No peer merging.
2. **Each client holds the server's state as last received**, plus a queue of its own **pending mutations**. What
   it shows is those mutations applied, in order, to the server's state. When the server's state moves, the
   client drops the mutations the server has taken and **replays the rest** on the new state. That is the whole
   of conflict handling (Replicache's model, by name: "rebase").
3. **The server sends state, not instructions.** A pull answers with a patch to the client's last state, and the
   ids of the client's mutations it has applied. The client never has to reproduce the server's computation to
   converge.
4. **Mutations carry ids.** A retried push is applied once. A lost answer costs nothing.
5. **Object ids are made by the client and unique** (Figma prefixes each object id with the client's id). Nothing
   is ever renamed.
6. **The UI is a projection of the client's state** (unidirectional data flow). Gestures emit mutations. Nothing a
   widget renders writes back to the state.
7. **Undo is per user, as new mutations** that put back the fields that user's step changed.

**Why not a CRDT** (Yjs, Automerge). A CRDT buys peer-to-peer merge without an authority. DataCube has an authority
already, the keeper, and needs it: Python edits in process, and the keeper's apply is semantic (§3.4). A CRDT
would add tens of KB to bundles held to 500-byte budgets, and its generic merge does not know DataCube's rules
(a removed chart unselects its grid). **Why not operational transformation:** there is no text, and the server's
ordering already decides every conflict.

---

## 3. The design

### 3.1 The document and the reducer

The page document stays as it is. The client's reducer is `apply(doc, ops) -> doc`, the existing page and grid
reducers composed. It is synchronous and structural. Every side effect lives in it and nowhere else.

The keeper's apply stays semantic: it types columns and compiles each grid's query (`headless/grid.ts:880-903`).
The two need not agree exactly. The client's result is a prediction. The keeper's is the truth, and it reaches
the client as state (§3.4).

### 3.2 Ids: unique when made, never renamed

Every editor has a short tag (a tab: from its follower id; Python: its own). A tile or sheet made by an editor gets
`kind-<tag>.<n>`, `n` counted by that editor. Existing pages keep `grid-3` and friends: both forms are valid ids.
The keeper's id renaming, the `held`-at-version bookkeeping for collisions (H3), the client's rename following and
Python's handle moving all go.

A made title ("Grid 3") is today computed from the id's number (`page-ids.ts`). It is written at creation
instead, numbered by the grids on the page. Two editors may both make "Grid 3": titles are labels, not identity.

### 3.3 Mutations

A mutation is `{id, ops, label}`: `id` the editor's own counter, `ops` page and grid commands, `label` what Undo
names it. A tab sends **absolute** ops only: a header click becomes `sorts` with the full list, a cell's filter
becomes `filter` with the whole tree, resolved against the view when the gesture is made. Relative kinds stay
for Python's in-process API, which applies at the keeper's current state and never replays. A ledger pins which
kinds a tab may send.

### 3.4 The protocol

```
push(client, mutations)  -> pull answer      // applies each mutation whose id > lastId[client], in order,
                                             // at the keeper's current page; refused ones recorded with words
pull(client, since)      -> { version, patch | page, lastId, refused: [{id, words}], hash }
```

- **push** applies each new mutation at the page as it is now, with the semantic apply (today's `settle`, without
  renaming). A mutation already applied is skipped, so a retry after a lost answer is harmless.
- **pull** answers the change since the version the client has: a JSON patch of the document, or the page whole
  when the version is older than the keeper keeps. It answers the last mutation id taken from this client, and
  the words of any of its mutations refused.
- The hash checks the patch arrived intact. A mismatch asks for the page whole. Nothing is lost by that, since
  pending mutations replay on whatever state arrives, so the message "must never show" stops existing.

The keeper keeps, per version, the patch from the version before (or the document), and per client its last
mutation id. Following by commands (`changes`, `since`) and `Settled.combined/renamed/since/edits` go.

### 3.5 The client store

```ts
class PageStore {
  confirmed: { version: number; page: PageDocument };   // the keeper's, exactly
  pending: Mutation[];                                   // mine, not yet taken, in order
  view: PageDocument;                                    // pending applied to confirmed
  history: History;                                      // mine (§3.6)

  dispatch(ops, label) {               // a gesture, an undo
    const before = this.view;
    this.view = apply(this.view, ops); // refused in words: nothing changes
    this.pending.push({ id: ++this.seq, ops, label });
    this.history.record(label, between(this.view, before)); // the inverse, total diff
    this.publish();
  }
  onPull(a) {                          // every answer, from push or poll
    this.confirmed = patched(this.confirmed, a);       // or the page whole
    this.pending = this.pending.filter((m) => m.id > a.lastId);
    for (const r of a.refused) this.say(r.words);
    this.view = this.pending.reduce(applyOrDrop, this.confirmed.page);
    this.publish();
  }
}
```

Properties that follow from the definition, not from testing:

- **Convergence.** With nothing pending, `view == confirmed == the keeper's page`.
- **No edit lost unsaid.** A mutation leaves `pending` only when the keeper took it or refused it in words.
- **At most once.** A mutation applies once at the keeper, however often it is pushed.
- **No hold, no quiet wait.** `onPull` is synchronous. A gesture made at any moment is a mutation on the latest
  view.
- **A write and a follow are the same path.** Push answers with a pull.

A gesture whose query the planner must accept is checked before it is dispatched, as today's transaction is
checked before it commits. A mutation the planner refuses is never sent. One the keeper later refuses, because
another editor moved the page, comes back in `refused` with the keeper's words.

### 3.6 History and undo

One history per editor, in the store. Each entry is a dispatched mutation and its inverse, the total diff from
the view after to the view before (`pageBetween`, `gridBetween`).

- **Page Undo** takes the latest entry. **A grid's Undo** takes the latest entry that touched that grid. Both
  dispatch the inverse as a new mutation, which pending and the keeper treat like any other.
- **Undo puts back only what is still as the step left it.** At press time, each field the inverse would set is
  compared with the value the step left there. A field another editor changed since is left as they made it, and
  the Undo says so. So two editors undoing never fight, and undo-then-redo leaves the page as it was.
- A history never restarts. W13's rule, the grid owners' stacks of states, `again`, `keepHistory` and
  `restarted` go.

### 3.7 The screen as a projection

The screen subscribes to `view`. On every change it **reconciles**: the board through `CubePage.sync` from the old
view to the new, and each grid whose cube changed set to it in one call. The grid's state owner stays as the
render pipeline: the query in flight, the rows last answered, latest wins. It holds no history and is no longer
the state of record.

One rule holds every widget: **it emits a mutation from a user gesture handler only, never from rendering,
reconciling or a query's answer.** The chart's own follow of its grid (`chart-panel.ts:164`) and the board's
unselect callback (`cube-page.ts:1537`) become renders of what the reducer already wrote. A guard test pins the
rule, as L11 pins gestures today.

During migration a widget may still apply a local gesture to itself first. Reconciling after every dispatch then
corrects any disagreement with the reducer at once, and a development assertion reports it.

### 3.8 Python

Python's page reads and writes the keeper directly, in process: each API call is a push at the current state,
under the keeper's lock. Its own copy of the document, its pending list and its handle renaming
(`page.py:1355-1400`) go. A notebook widget and a browser tab are both clients of §3.4.

### 3.9 What goes, what stays

**Goes** (estimated 1,000 to 1,200 lines across TypeScript and Python):

- `live-converge.ts`, whole.
- `LiveChannel`'s converge, refusal handling, hash reopen and following.
- `PageApp`: `putBack`, `renamedBy`, `renamePending`, `reapply`, `knows`, `#gone`, `#movedAway`, the live paths of
  `#remake`, and the hold.
- `CubeApp`: hold, `again`, `keepHistory`, `applyGridCommands`'s own and history rules.
- `PageState`'s `putBack`, `rename` and `dropSteps`.
- `page-rebase.ts`'s renaming and held-id bookkeeping.
- The board's `theirs` and `refilter` flags.
- The keeper's combined, renamed and since answers.
- Python's `_renamed`, `_regiven` and combine path.

**Stays:** both reducers, both total diffs, the keeper and its semantic apply, the hash, the gesture modules,
`CubePage.sync`, the commands added by D19 (`tree`, `before`, `detach`), the test harnesses.

**Added** (estimated 400 to 600 lines): the store, the protocol's two routes, the reconciler, the undo comparison,
the id tags.

---

## 4. How it is proven

The proof moves from search to construction, with one property test per piece.

1. **The store, by deterministic simulation.** N simulated clients and a simulated keeper over the real reducers,
   with no DOM. The network delays, drops, duplicates and reorders messages, and clients go offline and come back
   past what the keeper keeps. Thousands of seeds a second. Asserted at every quiet point: every view equals the
   keeper's page, every mutation was applied once or refused in words, and no history step undoes another
   editor's later change.
2. **The screen is a projection.** For pages from the saved-page corpus and random op sequences: reconcile the
   screen to a view, read the screen's document back, and it equals the view. For each gesture: the mutation it
   dispatches, applied to the view, is what the screen then shows.
3. **The keeper.** A patch applied to the page at its version gives the page at the next, over the corpus. A
   mutation pushed twice applies once.
4. **End to end,** kept: the soak with three tabs and faults, the long session, the two-browser test.

The mutation pass of D19 (§6 of its note) runs over 1 and 2.

---

## 5. What this costs and risks

- **The largest piece is §3.7's inversion of CubeApp** (4,400 lines), from state of record to rendered view. It
  goes last (phase D) and can go grid feature by grid feature, since §3.7's reconcile-after-dispatch keeps the
  screen right meanwhile.
- **Optimistic flicker.** A grid op's prediction lacks the keeper's typing until the answer comes, then the
  answer replaces it. Today's screen shows the same thing as a pending state.
- **Bundles shrink.** More code goes than comes. The converge, the rename machinery and the hold paths are in
  every bundle today.
- **One id format more.** Saved pages and Python code that hold `grid-3` keep working.

---

## 6. Phases

Each phase ships on its own, with the product working and the gates green.

| Phase | Builds | Deletes | Proven by |
|---|---|---|---|
| A | The protocol (§3.4): mutation ids, pull with patch and last id, refused words. Unique ids (§3.2). Python direct (§3.8). | Keeper renaming, combined answers, Python's rename path | Keeper tests 3; the existing soaks |
| B | The store (§3.5); the converge replaced by "compute the view, reconcile the screen to it" | `live-converge.ts`, the channel's converge, holds, quiet waits, `PageApp`'s live paths | Simulation 1; projection 2; soaks |
| C | One history per editor with the undo comparison (§3.6) | Grid owners' history on a page, W13, `again`, `restarted` | Simulation 1 with undo; the undo gestures' tests |
| D | Widgets as projections (§3.7), one grid feature at a time | The widgets' side-effect callbacks | Projection 2; the guard |
| E | Absolute ops only from a tab (§3.3) | Relative kinds on the tab's wire | Ledger pin |

A to C give the correctness. D and E make it clean.

---

## 7. Decisions for the user

1. **Adopt this design** as D19's successor, replacing the converge. *Recommended.*
2. **PR #71.** Land it first: it is better than main, CI is green, and phases A to C reuse most of it (the diffs,
   the commands, the hash, the tests). Or hold it and build from main. *Landing it is recommended.*
3. **Ids** (§3.2): tagged ids for new tiles, titles written at creation.
4. **Undo semantics** (§3.6): one history per editor, a grid's Undo scoped to its steps, and an undo that leaves
   a field another editor changed since. This replaces W13's "the grid's history restarts".
5. **Refusals** (§3.5): a local gesture is still checked before it is sent. A page made unplannable by another
   editor's change shows the planner's words in that grid, since it cannot be refused after the fact.
6. **Shared view state.** Open groups (`tree`) are shared through the page today. This design keeps that. Say if
   they should become per-tab.

## 8. As built: the middle version (2026-10-24)

The user's decision: the three changes that remove the root causes R1 (ids renamed after the fact) and R2 (a six-step
converge that replays what widgets do), plus a write id for the lost answer -- without the store or undo at the keeper.

**Unique ids, nothing renamed.** Each page open live gets a tag (`page-ids.ts idTag`: `t` and five base-36 letters at
random), and every id it makes is `kind-<tag>-n`, `n` after every held id of the kind (`nextPageId`); Python, the one
editor without a tag, keeps `kind-n`. A tab names what a person makes before it applies it (`page-between.ts
namedUnder`, `PageApp.#named`); an undo bringing a tile back gives it a fresh id under the tag (B12 stays: the review's
resurrection rule was not adopted). The keeper renames nothing: a command making an id the page holds is refused in
words (`'sheet-3' is an id the page has held already`). Gone: the keeper's `renamed`, `rebaseEdits`' `known` ids and its
renaming, `PageApp.renamedBy` and `renamePending`, Python's `_renamed` and `_regiven`. Renaming stays only inside a tab's
own history, where an undo's fresh id is followed by the later steps (`PageState.rename`).

**The keeper answers with its page; the screen is set to it.** A write's answer is `{version, hash}`, and `page` and
`held` when the keeper made another page than the tab sent. A follow reads the page whole (`page.json`); the `since=`
route, the keeper's per-version steps and `keeper.changes` / `moved` are gone. The converge (`live-converge.ts`) is one
pass inside the channel's `following`: the page and its grids held; each grid the board and the keeper's page both have
set to its cube there by its own commands (`gridBetween`); the board put at the keeper's page (`PageApp.putBack`), a grid
of another editor's opened from its cube; any grid still off set again; each grid's rows where the keeper's page says
(its copy, or its live rows: `CubeApp.followSnap`, `followLive`); every grid's change in flight waited for; the page read
and checked against the keeper's hash; the pending edits made again. Another editor's change reaches a page by this
alone: `PageApp.follow`, which replayed the editor's commands, is gone, and its tests go through the converge
(`test/theirs.ts`). A grid's history restarts when another
editor changed its cube (its cube there differs from the one this screen last knew there, the page's name aside) or the
refused batch touched it (W13). **A change of behaviour the user should know:** when another editor changes a grid at
the same moment you edit it, that grid's undo history starts again after your edit.

**Write ids.** Each write carries `{writer, seq}` (the writer random per channel). The keeper takes each once, checked
under its lock as the write is kept (`keeper.py _write`), and a read by a writer answers `taken`, the last of its writes
kept, read with the page under one lock. A 409 is a refusal (`LiveRefused`); any other failure means the answer never
came, and the tab asks: taken, the screen is set to the page, which holds it; not taken, its edits go back in front of
the pending ones. A read that says no `taken` is no keeper's answer and is refused in words (no default).

**What else moved.** A write from any version the page had is made again on top now (F24's limit, a write more than 64
versions behind refused, is lifted: nothing the keeper needs from that version remains); a write at a version the page
never had, 0 among them, is refused. Python's side is storage and wire only -- the keeper's write ids and the answer
with the page -- and net smaller (138 lines added, 168 removed); every combining rule is DataCube's, asked headless.

**Found while building.** Seed 31 of the three-tab soak: a chart put on the board over its grid before the grid reached
the keeper's cube followed the grid's grouping here (`followCube`), not the keeper's. Fixed by setting the grids first,
then the board. And, found answering the user's question on the ledger: the converge set the page's copies but not its
grids', so another editor's snap left the grid here live and the page opened again (said); the copy step above fixes
it, held by page-app.test.ts's snap test through the converge. The guards moved by the change, each with a dated note:
`L1_ROUTES` 6 -> 5; the state guardrail's `renamedBy` and `follow` pins removed.

**The one API ledger's five routes (`L1_ROUTES`), each a place outside a grid that changes it through its public
methods, every one accounted for (no open item):** a chart's click refilters its grid (`cube-page.ts #select`, a command
told to the keeper); a saved page opened reads its snapped grids' copies (`datacube-app.ts openSaved`, an open, not an
edit); and the converge's three -- each grid at its cube on the keeper's page by its own commands, another editor's snap
followed, another's going live followed (`live-converge.ts`). They stay: each is either a command or the screen being
set to what the keeper already holds.

**Evidence.** The datacube node suites (the channel 45, follow 5, rebase 14, app-live 12, gestures 61, page-state 10,
page-between 8); the soak by hand at 60 seeds with faults, 30 without, and 12 sessions of 40 steps; `//python:engine_test`
(255, the keeper's convergence soak with two open pages under their own tags among them); the guards; and
`//gates:local`: 408 passed and 9 failed, every one the container's -- the eight Postgres targets, which refuse root and
pass run as an unprivileged user (`tools/postgres/as_unprivileged.sh`, docs/CLOUD_SESSIONS.md), and `browser_guard_test`
(the proxy's certificate on extensions.duckdb.org). A first run met the disk allowance (an IO error and two browser
timeouts, gone once space was freed) and the import list the new test helper changed (regenerated); the bundle budgets
held unmoved.

