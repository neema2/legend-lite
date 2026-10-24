// DATACUBE'S APP (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.1; step 1c): everything a DataCube page does, opened
// over a host's services -- the page and its grids (src/page/page-app.ts), each grid's status line and its Generated
// Pure & SQL window, the sources (src/app/sources.ts), the saved pages (Open…, Save, Save As…), share links and the page
// file, the start (a sample, a link, a launch key), the blank page, "changed since saved" and asking before leaving,
// the planes' menu, and the model actions (Create a Class…, Create an Association…, Create a Data Space…). It reads
// nothing of its host but what it is handed: the services, the element it fills, its status line, where its windows
// float, and what to open first. The tab opens it (demo/boot.ts), and Python's tab and notebook widget (demo/engine.ts,
// demo/widget.ts, over src/services/python-host.ts: step 2a), whose grids Python plans and runs, and whose page Python
// changes and reads back (the live page, §5).
//
// Moved from demo/boot.ts (step 1c): the code is the tab's, the lines that read the tab's markup, address and title
// pointed at the app's own windows and the services.

import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import { fn } from '../../../pure-protocol/src/index.ts';
import { CubeApp, type CubeAppQuerySource } from '../app.ts';
import type { CubeConfiguration } from '../config.ts';
import { gridCapped } from '../cube-document.ts';
import { handleKey } from '../file-handles.ts';
import { Latest, TabWork, mayLeave } from '../host.ts';
import { PlanThenRun, type PlanAndRun } from '../runner.ts';
import { PageApp, type GridMaker } from '../page/page-app.ts';
import {
  NOTHING_TO_SAVE,
  modelPageName,
  openedLive,
  openingOrder,
  pageContent,
  pageDefinitionText,
  pageToJson,
  readSaved,
  leftOutNote,
  leftOutWhenUnopened,
  noneOpened,
  pageSaving,
  savedPage,
  type PageDocument,
  type SavedDocument,
} from '../page-document.ts';
import { idTag, joinInputOf, type PageCommand } from '../page-commands.ts';
import type { JoinAsk } from './join-flow.ts';
import { JOIN_SOURCE, joinMenu } from './join-menu.ts';
import type { PageModelRef } from '../page-model.ts';
import { classBasis, duplicateLambda } from '../query.ts';
import { isPageFragment, readPageFragment, shareLink } from '../share/link.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import { CubeLibrary } from '../ui/cube-library.ts';
import { EXAMPLE_PAGES } from '../examples/catalog.ts';
import type { MenuItem } from '../ui/menu.ts';
import { saveDialog } from '../ui/save-dialog.ts';
import type { SectionId } from '../ui/source-picker.ts';
import { makeWindow, type WindowSpec } from '../ui/window.ts';
import type { Sample, Services, Versions } from './services.ts';
import { followBy } from './live-follow.ts';
import { LiveChannel, type Converge, type Converged } from './live-channel.ts';
import { convergeOn } from './live-converge.ts';
import { pageHash } from '../page-hash.ts';
import { pageSources, picksSources, type Chosen, type GridPlace, type Opened, type SignedIn } from './sources.ts';
import { lazily } from '../lazily.ts';

// the query store's code, fetched the first time it is needed -- a query link -- as charts are: a grid-only page never
// downloads it (test/bundle-budget.test.ts)
const queryStores = () => lazily(() => import('../../../query-store/src/index.ts'));

/** CREATE ▸ CLASS… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1): a grid's own entry, under every menu's Create ▸
 *  (the user, 2026-10-23: one Create, in every menu; its window is still Create a Class). */
const CREATE_CLASS = { id: 'host.createClass', label: 'Class\u2026', section: 'create' } as const;
/** CREATE ▸ ASSOCIATION… and DATA SPACE… (§15.1, §15.6a): the page's and each grid's -- a grid's class one end of the
 *  first. */
const CREATE_ASSOCIATION = { id: 'host.createAssociation', label: 'Association\u2026', section: 'create' } as const;
const CREATE_DATA_SPACE = { id: 'host.createDataSpace', label: 'Data Space\u2026', section: 'create' } as const;
/** CREATE ▸ GRID OVER <CLASS> (§16.1): a grid made a class, its class read beside it -- a dashboard's tile; first of the
 *  model's entries under Create ▸ (the user, 2026-10-23: "Add Grid over Class is great under Create"). */
const CLASS_GRID_ID = 'host.classGrid' as const;

/** What the app opens first. */
export type AppStart =
  /** The host's sample: its rows, under its own model, laid out as it says. */
  | { readonly kind: 'sample'; readonly sample: Sample }
  /** A link the page was opened from (a shared page, a saved query's link): `forget` takes it out of the address, so a
   *  reload never reopens it over what the person has done since. */
  | { readonly kind: 'link'; readonly hash: string; readonly forget: () => void }
  /** The single-user app's launch key (docs/DATACUBE_APP_PLAN_2026_10_02.md, A3), and the table it asks for. */
  | { readonly kind: 'warehouse'; readonly key: string; readonly table?: string }
  /** The page another editor serves (`services.live.page`: Python's `ll.Page`, and `ll.show(df)`'s page of one grid:
   *  docs/DATACUBE_ONE_API_PLAN_2026_10_21.md P12), followed and reported back to it. */
  | { readonly kind: 'live' };


/** The app open on its host: what takes it down (a notebook's cell re-run, its view closed). */
export interface DataCubeApp {
  dispose(): void;
}

export interface OpenDataCube {
  readonly services: Services;
  /** The element the page fills. */
  readonly host: HTMLElement;
  /** Its status line: what the planner says as it starts, and the page's errors; shown in the first grid's status bar. */
  readonly status: HTMLElement;
  /** Where its windows float (Open…, Share, the Generated Pure & SQL). */
  readonly windows: HTMLElement;
  readonly start: AppStart;
  /** The tables the host's database holds for the start already (the tab's sample: `trades`). */
  readonly taken: readonly string[];
  /** Told how to show a start that failed, once the page can say it (its empty card). */
  readonly settled: { show: (reason: string) => void };
}

/** DataCube's app, opened in `o.host` over the host's services. */
export async function openDataCube(o: OpenDataCube): Promise<DataCubeApp> {
  const { services, status, start, settled } = o;
  const host = o.host;
  const { label } = services.engine;
  /** The page's own planner and database, where it has them (the tab); on a Python host every grid's rows are Python's. */
  const local = services.engine.local;
  /**
   * A GRID'S ROWS WHERE THEY ARE PLANNED AND RUN, on whichever host: its PlanAndRun -- the host's (Python's), or the
   * tab's over the grid's planner and the database its rows run on. What a model action reads of a grid (its sample,
   * its keys' facts) is read here, as its rows are: never the page's own engine, which a Python host has not.
   */
  const rowsOf = (place: GridPlace): PlanAndRun | undefined => {
    if (place.runner) return place.runner;
    const planner = place.planner ?? local?.planner;
    const engine = place.engine ?? local?.engine;
    return planner && engine ? new PlanThenRun(planner, engine) : undefined;
  };
  const windows = appWindows(o.windows);
  // the status line is the app's, in its look: whatever element the host handed it (app.css)
  status.classList.add('dc-app-status');
  /** The source picker has something to offer on this host. */
  const picks = picksSources(services);
  /**
   * WHAT THE STATUS LINE SAYS AT REST, between errors: the planner's word, or -- the page no longer following its other
   * editor (Python), or failing to -- why. An error says itself over it, and the next view to land brings it back.
   */
  let resting: { text: string; warn: boolean } = { text: label, warn: false };
  const sayResting = (next?: { text: string; warn: boolean }): void => {
    if (next) resting = next;
    if (status.classList.contains('bad')) return;
    status.textContent = resting.text;
    status.classList.toggle('warn-text', resting.warn);
  };
  /** What taking the app down undoes, besides the page: the windows, a watch, a timer, a listener. */
  const undo: (() => void)[] = [windows.remove];
  let disposed = false;

  /** The planes, as entries: the one the page is ON disabled, not hidden. */
  const planeMenu = (): MenuItem[] => services.engine.plane.planes.map((plane) => ({
    id: plane.id,
    label: plane.label,
    // where the planner runs: the status bar's readout opens these
    section: 'plane' as const,
    ...(plane.word === services.engine.plane.word ? { disabled: true as const } : {}),
  }));

  // The cube, built so it can be built AGAIN.
  //
  // Opening a file replaces the model, and therefore the columns and
  // the source relation, so the app is recreated rather than mutated
  // -- a cube whose snapshot no longer matches its model is not a
  // state worth supporting. Everything the construction needs is a
  // parameter so there is only one copy of it.

  /**
   * WORK THIS TAB HOLDS that leaving would lose: each part of the page adds what it knows (an
   * opened file lives only in this tab's database; a warehouse session only in memory; unsaved
   * changes). Asked before leaving the page and before switching plane (P2-337).
   */
  const work = new TabWork();
  /** Set once the person has agreed to leave: the browser's own question is not asked twice. */
  let leaving = false;

  /**
   * FOR THE BROWSER HARNESSES: when the page has finished what an action started, as a fact
   * rather than a guess. `changes` counts every change the cube reports (a view landing, a
   * presentation change, a refusal, the Pure pane written); `printing` is the Pure prints still
   * out. With the cube's own `busy`, a harness waits until nothing is in flight and nothing has
   * changed for a moment -- the time the action took, not a fixed sleep (a 4s fall-through on
   * every change that runs no query made verify-features take 8 minutes, and a 150ms sleep read
   * the page before a slower machine had re-queried). Not product code: the demo page.
   */
  const harnessSignal = { changes: 0, printing: 0 };
  /** Each grid the app made, by its tile's id: what following a frame re-runs (`page.cubes` says which are on it). */
  const madeCubes = new Map<string, CubeApp>();
  /** The page's channel to the page's keeper, on a page another editor serves (live-channel.ts): its writes carry the
   *  edits made here, each grid's and the page's. */
  let channel: LiveChannel | undefined;
  (window as unknown as { __dataCubeSignal?: typeof harnessSignal }).__dataCubeSignal = harnessSignal;

  /**
   * A GRID ON THE PAGE, as DataCube's app makes one (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md §6): a compact cube in a
   * tile, its windows floating over the whole page, wired to the page by `spawned` (its charts, copies, removal,
   * export, settings). Every grid is made here, the first as the fifth.
   */
  function gridMaker(
    snap: CubeSnapshot,
    config: CubeConfiguration,
    dims: { name: string; columns: string[] }[],
    place: GridPlace = {},
  ): GridMaker {
    // `start`: a copy of a grid of this source (Copy of Grid), starting where that grid is now
    return (gridHost, spawned, start) => {
      let printed = 0;
      /** What each entry of its last right-click's Join ▸ asks (joinMenuOf), until one is picked. */
      let joinAsks = new Map<string, JoinAsk>();
      const kept = services.browser.readSettings();
      // THE ONE BRANCH ON THE ROWS' SHAPE (§4.5): planned here and run here, or planned and run by the host
      const rows: CubeAppQuerySource = place.runner
        ? { runner: place.runner }
        : {
          engine: place.engine ?? must(local, 'a grid over the page\'s own database').engine,
          planner: place.planner ?? must(local, 'a grid over the page\'s own planner').planner,
          // Where the rows are (Live on a warehouse, or Snapped in this tab) is the grid's plane
          // button, and who read them the status bar's receipt.
          ...(place.live ? { live: place.live } : {}),
        };
      const created: CubeApp = new CubeApp(gridHost, start?.snapshot ?? snap, {
        ...rows,
        configuration: start?.configuration ?? config,
        // Snap only where the place says what to copy: no other source's target stands in for it
        ...(place.snapTarget ? { snapTarget: place.snapTarget } : {}),
        ...(place.hostSnap ? { hostSnap: place.hostSnap } : {}),
        ...(place.signInAgain ? { signInAgain: place.signInAgain } : {}),
        ...(place.cubeSource ? { cubeSource: place.cubeSource } : {}),
        ...(place.classTree ? { classTree: place.classTree } : {}),
        // the groups a saved cube had open: its own grid's, not a copy's (a copy starts collapsed, as a new grid) -- or, the
        // same grid made again by an undo, its own as they were
        ...(start?.tree ? { tree: start.tree } : place.tree && !start ? { tree: place.tree } : {}),
        ...(place.heldCopy ? { heldCopy: place.heldCopy } : {}),
        ...(place.label !== undefined ? { sourceLabel: place.label } : {}),
        compact: true,
        // a copy is a grid added beside others; the same grid made again folds as it did
        foldPanel: (start !== undefined && start.tree === undefined) || place.foldPanel === true,
        // each edit it keeps, written to the page's keeper, on a page another editor serves; an undo or a redo, the page
        // whole (a restore, N2)
        onCommand: (command) => {
          if (spawned.id !== undefined) channel?.edit({ grid: spawned.id, command });
        },
        // WHERE THE PLANNER RUNS, changed where it is read: the status bar's readout offers the planes; and, where the
        // page has a model home and the host writes models, Create a Class… of the grid's rows
        hostMenu: () => {
          const cls = spawned.id === undefined ? undefined : pageModelNow?.()?.classes?.[spawned.id];
          return [
            ...planeMenu(),
            ...(cls !== undefined && classGrid
              ? [{ id: CLASS_GRID_ID, label: `Grid over ${cls.slice(cls.lastIndexOf(':') + 1)}`, section: 'create' as const }] : []),
            // of any grid: a class is made over every table its rows read, a join's each (§18.3); refused in words when
            // it cannot be one
            ...(createClass && rowsOf(place)?.host.planner.source ? [CREATE_CLASS, CREATE_ASSOCIATION] : []),
            ...(createDataSpace ? [CREATE_DATA_SPACE] : []),
          ];
        },
        // its Create ▸ Data Source…: a grid over a source picked now, beside this one (the page's own Data Source…'s way)
        ...(picks ? { onOpenSource: () => { if (spawned.id !== undefined) void page.addSource(spawned.id); } } : {}),
        // ITS RIGHT-CLICK JOIN ▸ (the user, 2026-10-23), built as it opens: what each entry asks kept until one is picked
        // -- on a host's page that makes no joins (no local engine, no HostJoin), still there, saying so
        joinMenu: (column) => {
          const built = spawned.id !== undefined && joinMenuOf ? joinMenuOf(spawned.id, column) : undefined;
          joinAsks = built?.asks ?? new Map();
          return built?.items ?? [{ label: 'Join…', disabled: true, hint: 'This page\'s host makes no joins' }];
        },
        onHostMenu: (item) => {
          const ask = item.id !== undefined ? joinAsks.get(item.id) : undefined;
          if (ask !== undefined) joinGrid?.open(spawned.id, ask);
          else if (item.id === JOIN_SOURCE) joinSource?.(spawned.id);
          else if (item.id === CLASS_GRID_ID) classGrid?.(spawned.id);
          else if (item.id === CREATE_CLASS.id) createClass?.(created, place, spawned.id);
          else if (item.id === CREATE_ASSOCIATION.id) createAssociation?.(spawned.id === undefined ? undefined : pageModelNow?.()?.classes?.[spawned.id]);
          else onHostItem(item);
        },
        // its windows float over the page, not squeezed into its tile
        windowHost: page.root,
        // "Changed since saved" is re-read on every change of the cube's state,
        // a presentation change (a width, a colour) included: it runs no query.
        onChange: () => {
          harnessSignal.changes += 1;
          onCubeView?.();
        },
        showColumnZone: true,
        dimensions: dims,
        // the grid says it copied whatever the clipboard did, as it always has (src/app.ts #copy): a refusal is
        // not left unhandled
        writeClipboard: (text) => services.browser.writeClipboard(text).catch(() => {}),
        // Settings kept between visits, as upstream's hosts keep them
        // (settingsData.values / onSettingsChanged); the page keeps them for every grid.
        ...(kept ? { settings: kept } : {}),
        download,
        onStatus: (text, kind) => {
          // ERRORS ONLY. The cube's own status bar carries the result
          // and the timing; a host that echoed the same line beside it
          // said one fact twice in a 20px strip. What a host is for is
          // saying what the cube cannot -- a planner that failed.
          if (kind !== 'error') return;
          harnessSignal.changes += 1;
          status.textContent = text;
          status.classList.add('bad');
          status.classList.remove('warn-text');
        },
        onView: (view) => {
          // For the browser harness: how many views have landed.
          const w = window as unknown as { __dataCubeViews?: number };
          w.__dataCubeViews = (w.__dataCubeViews ?? 0) + 1;
          harnessSignal.changes += 1;
          // A VIEW LANDED, SO THE LAST ERROR IS OVER: a status line says what is true NOW.
          if (status.classList.contains('bad')) {
            status.classList.remove('bad');
            sayResting();
          }
          // The query this product built, as the compiler prints it, and
          // the SQL the planner made of it. Both, because they answer
          // different questions -- the last view of any grid on the page.
          // The print is asked for; a later view's print wins.
          const printing = (printed += 1);
          harnessSignal.printing += 1;
          void created.printQuery(view.query, 'STANDARD').then(
            (text) => { if (printing === printed) windows.pure.textContent = text; },
            (error: unknown) => { if (printing === printed) windows.pure.textContent = String(error); },
          ).finally(() => {
            harnessSignal.printing -= 1;
            harnessSignal.changes += 1;
          });
          windows.sql.textContent =
            view.sql || '(no SQL for this view)';
        },
        ...spawned,
      });
      if (spawned.id !== undefined) {
        madeCubes.set(spawned.id, created);
        madeGrid?.(spawned.id, place);
      }
      // a grid on the page: the start's reason for a blank one is over
      sayWhyBlank(undefined);
      return created;
    };
  }

  /** The host's own entries, from the page's menu or a grid's status bar. */
  function onHostItem(item: MenuItem): void {
    if (item.id === 'host.open') showCubes?.();
    if (item.id === 'host.save') saveCube?.(false);
    if (item.id === 'host.saveAs') saveCube?.(true);
    if (item.id === 'host.share') void copyShareLink?.();
    if (item.id === 'host.query') windows.toggle(windows.query);
    if (item.id === CREATE_ASSOCIATION.id) createAssociation?.();
    if (item.id === CREATE_DATA_SPACE.id) createDataSpace?.();
    // A NAVIGATION, not a switch: the same page loads again with the chosen ?planner= (a cube
    // never changes planner while it runs). Asked first when leaving would lose work here.
    const plane = services.engine.plane.planes.find((p) => p.id === item.id);
    if (plane) {
      if (!mayLeave(work, (q) => window.confirm(q), 'Choosing another planner reloads the page')) return;
      leaving = true;
      services.engine.plane.choose(plane);
    }
  }

  /** Hand a file to the user. */
  const download = (name: string, mime: string, text: string | Uint8Array): void => services.browser.download(name, mime, text);

  /** Opens the saved cubes' window; set once the page can open files. */
  let showCubes: (() => void) | undefined;
  /** Close the Open… window, when it is open. */
  let closeCubes: (() => void) | undefined;
  /** Save the page (over the one it was opened from), or Save As a new one. */
  let saveCube: ((asNew: boolean) => void) | undefined;
  /** Copies the page's share link; set once the page can open files. */
  let copyShareLink: (() => Promise<void>) | undefined;
  /** Told when a view lands: "changed since saved" is re-read then. */
  let onCubeView: (() => void) | undefined;
  /**
   * CREATE A CLASS… of a grid (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15): set once the page has a model home to
   * write its classes to (config.json's `depot`); offered only on a grid whose planner writes classes.
   */
  let createClass: ((grid: CubeApp, place: GridPlace, cube: string | undefined) => void) | undefined;
  /** CREATE AN ASSOCIATION… (from a grid: its class one end) and CREATE A DATA SPACE… of the page, once it has a model
   *  home. */
  let createAssociation: ((first?: string) => void) | undefined;
  let createDataSpace: (() => void) | undefined;
  /** ADD A GRID OVER <CLASS> (§16.1): the class grid `cube` was made, read beside it at the page's pin. */
  let classGrid: ((cube: string | undefined) => void) | undefined;
  /** JOIN… (§17.5): grid `cube` joined to another of the page -- in the tab, where a grid reads tables of its database; by
   *  the host, on a host's page (B11). Offered on a grid's menu where `offered` says. */
  let joinGrid: { readonly offered: (place: GridPlace) => boolean; readonly open: (cube: string | undefined, ask?: JoinAsk) => void } | undefined;
  /** A grid's right-click Join ▸ on a column: its entries, and what each asks (join-flow.ts JoinAsk). */
  let joinMenuOf: ((cube: string, column: string | undefined) => { readonly items: MenuItem[]; readonly asks: Map<string, JoinAsk> }) | undefined;
  /** Join ▸ ▸ Join a Data Source…: a grid over a source picked now, then the window joining grid `cube` to it. */
  let joinSource: ((cube: string | undefined) => void) | undefined;
  /** The page's model now (where its classes are), once the page can make them. */
  let pageModelNow: (() => PageModelRef | undefined) | undefined;
  /** The page as this tab keeps it -- a newer writer's fields and its model with the board's -- once its sources are set
   *  up (below): what Export ▸ Page File writes. */
  let pageKept: ((name: string) => PageDocument | undefined) | undefined;
  /** The page renamed in its name box: its name from now (once the page can be saved, a change until it is). */
  let renamePage = (name: string): void => page.rename(name);
  /** Told when a grid is made on the page, by its tile's id: what the page keeps of it (its file). */
  let madeGrid: ((id: string, place: GridPlace) => void) | undefined;
  /** Told of every change on the page: the tables no grid reads any more are dropped. */
  let dropUnread: (() => void) | undefined;
  /** The source picker, once the page can open sources: New ▸ Data Source… adds a grid over one; a blank page opens one. */
  let picker: ((purpose: 'add' | 'open', start?: SectionId) => Promise<GridMaker | undefined>) | undefined;
  /** A blank page, everything gone, for a first data source: as asked (New ▸ Blank Page: `asked`, leaving a page another
   *  editor serves, D1), or where a start opened nothing (`reason`: why). */
  let blankPage: ((o: { readonly reason: string | undefined } | { readonly asked: true }) => void) | undefined;
  /** Why the page's start opened nothing (a link that failed, a key refused): the empty page says it. */
  let startProblem: string | undefined;
  /** The empty page's card: hidden until the start has settled -- a page still signing in, or opening a link, is not blank. */
  let blankCard: HTMLElement | undefined;
  /** Where the card says why the start opened nothing: after its lead. */
  let blankLead: HTMLElement | undefined;
  /** The card's line saying why, while it says it. */
  let blankWhy: HTMLElement | undefined;
  /** The empty page's card says why the start opened nothing, or -- undefined -- says nothing of it. */
  const sayWhyBlank = (reason: string | undefined): void => {
    blankWhy?.remove();
    blankWhy = undefined;
    if (!reason || !blankLead) return;
    const why = document.createElement('p');
    why.className = 'dc-blank-reason';
    why.setAttribute('role', 'alert');
    why.textContent = reason;
    blankLead.after(why);
    blankWhy = why;
  };

  /**
   * THE PAGE (src/page/page-app.ts): its bar -- the page's menu, its name, its fold -- and its grids.
   * The page's menu carries the host's entries: Save, Open, Share, the generated Pure and SQL, the planner planes.
   */
  const page = new PageApp({
    host,
    hostMenu: () => [
      // saved pages: where the host keeps them (in this browser), over the sources they were built on
      ...(services.pages ? [
        { id: 'host.save' as const, label: 'Save', section: 'file' as const },
        { id: 'host.saveAs' as const, label: 'Save As\u2026', section: 'file' as const },
        { id: 'host.open' as const, label: 'Open\u2026', section: 'file' as const },
      ] : []),
      // the page's settings in a link: never a row of data (src/share/link.ts)
      ...(services.browser.address ? [{ id: 'host.share' as const, label: 'Share\u2026', section: 'file' as const }] : []),
      { id: 'host.query', label: 'Generated Pure & SQL\u2026', section: 'view' as const },
      // THE PAGE'S MODEL, under its Create ▸: its associations and its data space, once the page has a model home
      ...(createDataSpace ? [CREATE_ASSOCIATION, CREATE_DATA_SPACE] : []),
      // The planes, as entries rather than a control: the bar is
      // for what you watch, the menu for what you do occasionally.
      // The one you are ON is disabled rather than hidden, so the
      // menu still says where the work happens.
      ...planeMenu(),
    ],
    onHostMenu: (item) => onHostItem(item),
    // A PAGE FILE: the page as this tab keeps it, which the page writes by DataCube's one rule (pageFile) -- refused while
    // its classes are in a project (§15.3; a file carrying its model comes with the Studio line's export, S23)
    pageNow: (name) => pageKept!(name),
    refuse: (text) => window.alert(text),
    // THE HOST'S TEXT, IN THE STATUS BAR (planner progress, errors, the planner's word): the page's first grid's, at the
    // right of its status bar -- a readout belongs there, not in the bar that says what is on screen. MOVED rather than
    // copied, on each render of that bar: `status` is the same node the planner writes to.
    hostStatus: (slot) => slot.append(status),
    // THE NAME BOX renamed: the page's name from now, saved by Save (a change of the page until then)
    onRename: (name) => renamePage(name),
    // each edit of the page's structure, written to its keeper, on a page another editor serves; an undo or a redo, the
    // page whole (a restore, N2)
    onPageCommand: (command) => channel?.edit({ page: command }),
    // THE EMPTY PAGE (New ▸ Blank Page, the last grid removed, a start that opened nothing): what to do first -- a
    // data source (the picker, opening in place) or a saved page
    empty: (slot) => {
      const doc = document;
      const blank = doc.createElement('div');
      blank.className = 'dc-blank';
      const card = doc.createElement('div');
      card.className = 'dc-blank-card';
      const title = doc.createElement('h2');
      title.className = 'dc-blank-title';
      title.textContent = 'A blank page';
      const lead = doc.createElement('p');
      lead.className = 'dc-blank-lead';
      // what the page can start from, as its host offers it
      lead.textContent = picks
        ? 'Start with a data source: a file from your computer, an example, a table in a warehouse, or a remote Parquet, CSV or Iceberg file.'
        : services.live ? 'Nothing is on this page: Python shows what is on it.' : 'Nothing is on this page.';
      blankLead = lead;
      card.append(title, lead);
      const actions = doc.createElement('div');
      actions.className = 'dc-blank-actions';
      if (picks) {
        const add = doc.createElement('button');
        add.type = 'button';
        add.className = 'dc-picker-button dc-primary';
        add.textContent = 'Add a data source';
        // on the page its other editor serves, the first source is added to it, as New ▸ Data Source… adds one -- opened
        // in place, it would leave that page for one of the tab's own (the S7 confirmation audit's H2-6)
        add.addEventListener('click', () => void picker?.(channel ? 'add' : 'open'));
        actions.append(add);
      }
      if (services.pages) {
        const saved = doc.createElement('button');
        saved.type = 'button';
        saved.className = `dc-picker-button ${picks ? 'dc-quiet' : 'dc-primary'}`;
        saved.textContent = 'Open a saved cube';
        saved.addEventListener('click', () => showCubes?.());
        actions.append(saved);
      }
      if (actions.childElementCount > 0) card.append(actions);
      blank.append(card);
      blank.hidden = true;
      blankCard = blank;
      slot.append(blank);
    },
    // New ▸ Data Source…: a grid over another source, through the picker, beside the others -- where the host offers one
    ...(picks ? { openSource: () => picker?.('add') ?? Promise.resolve(undefined) } : {}),
    // where the page can start again from a blank one: a data source, or a saved page
    ...(picks || services.pages ? { onBlankPage: () => blankPage?.({ asked: true }) } : {}),
    onChange: () => {
      harnessSignal.changes += 1;
      // a grid gone from the page is followed no more
      for (const id of [...madeCubes.keys()]) if (!page.cubes.includes(id)) madeCubes.delete(id);
      dropUnread?.();
      onCubeView?.();
    },
    onSettingsChanged: (values) => services.browser.writeSettings(values),
    download: (name, mime, text) => download(name, mime, text),
  });
  // a start that throws, from here on: the empty page shows its card, saying why
  settled.show = (reason) => {
    if (!page.empty) return;
    sayWhyBlank(reason);
    if (blankCard) blankCard.hidden = false;
  };
  // For the browser harness ONLY: the page, its views and its layout; and its first grid (a CubeApp), so a check can
  // read that grid's own configuration and snapshot when what it sees on screen disagrees. Not product code.
  (window as unknown as { __dataPage?: PageApp }).__dataPage = page;
  Object.defineProperty(window, '__dataCube', {
    configurable: true,
    get: () => (page.grids[0] !== undefined ? page.grid(page.grids[0]) : undefined),
  });

  // OPENING A FILE.
  //
  // DuckDB reads it and sniffs the schema, legend-lite's writer (`tables.tableModel`) writes the
  // Pure model from what its catalog found, and the cube is rebuilt against that.
  // Nothing downstream learns the data was uploaded: the planner
  // compiles an ordinary model over an ordinary table, which is why
  // the SQL panel, the tree and the snap plane all keep working
  // without a second code path.
  {
    // THE PAGE'S SOURCES (src/app/sources.ts): each kind opened through the services, a grid's file and planners kept
    const sources = pageSources({
      services, page, gridMaker, taken: o.taken, ask: (content) => library?.ask(content),
    });
    // each grid's place told to the sources as it is made -- the start's grid, below, included: its tables are then
    // what a join of it reads
    madeGrid = (id, place) => sources.madeGrid(id, place);
    dropUnread = () => sources.dropUnread();

    if (start.kind === 'sample') {
      const { sample } = start;
      // generated rows are a copy in the tab from the start; a remote file read through the sample's model is live, and
      // snaps into a copy of its own, as any grid's rows not in the tab do (§18.3)
      const snapTarget = sources.snapTarget();
      page.addGrid(gridMaker(await sample.snapshot(), sample.configuration, sample.dimensions, {
        ...(sample.heldCopy ? { heldCopy: sample.heldCopy } : snapTarget ? { snapTarget } : {}),
        // what its rows are, written down: a page it starts is saved, shared and joined as any file's is
        ...(sample.source ? { cubeSource: sample.source.cube, tables: sample.source.tables } : {}),
      }));
      await page.ready();
      // the page the app starts with: no step of a person's to undo
      page.started();
    }
    // WHAT LEAVING WOULD LOSE, asked in this order: the warehouse's session, unsaved changes, the files opened
    work.add(() => sources.sessionHeld());

    // THE PAGE ON SCREEN, as a saved page sees it: the saved page it came from (a Save then saves over it), its name,
    // what a newer writer put in it (written back as it was), the definition it had when saved or first opened
    // ("changed since saved" compares to it), and what opening left out of it. And each grid's file (by the grid's
    // id): the File itself, so a page saved over the same file reopens without asking, and the handle the browser gave
    // for it, to reopen it from where it was picked.
    let library: CubeLibrary | undefined;
    let current: {
      /** The name it was saved (or opened) under: a name given in the name box since is a change of the page (its own
       *  name, the page's: D11, never one of the host's). */
      savedName?: string;
      /** Fields of the saved PAGE this reader does not know, and of each of its cubes, written back as they were. */
      pageUnknown?: Readonly<Record<string, unknown>>;
      cubeUnknown?: ReadonlyMap<string, Readonly<Record<string, unknown>>>;
      /** The definition as saved, or as first opened: "changed since saved" compares to it. */
      baseline?: string;
      /** What opening left out of the saved page (a file changed, a source not opened): saving over it loses them. */
      lost?: readonly string[];
    } = {};
    renamePage = (name) => {
      page.rename(name);
      onCubeView?.();
    };

    /**
     * What saving now would write -- always ONE kind of thing, the page (page-document.ts): one cube per grid, its
     * charts and layout -- and its definition, for "changed since saved".
     */
    const pageNow = (name: string, empty = false): PageDocument | undefined => page.document(name, {
      ...(current.pageUnknown ? { page: current.pageUnknown } : {}),
      ...(current.cubeUnknown ? { cubes: current.cubeUnknown } : {}),
    }, empty);
    pageKept = (name) => pageNow(name);
    // For the browser harness ONLY (test/python-engine/python-engine.mjs, two tabs on one page): the page's hash as its
    // keeper computes it (page-hash.ts, W17), to compare with Python's. Not product code.
    Object.defineProperty(window, '__dataPageHash', {
      configurable: true,
      get: () => { const now = pageNow(page.title, true); return now === undefined ? undefined : pageHash(now); },
    });
    const savedForm = (name: string): { content: Record<string, unknown>; definition: string } | undefined => {
      const doc = pageNow(name);
      return doc && { content: pageContent(doc), definition: pageDefinitionText(doc) };
    };

    /** The page on screen differs from what was saved (or first opened). */
    const dirty = (): boolean => {
      if (current.baseline === undefined) return false;
      if ((current.lost?.length ?? 0) > 0) return true;
      if (page.title !== (current.savedName ?? '')) return true;
      const now = savedForm(page.suggestedName);
      return now !== undefined && now.definition !== current.baseline;
    };
    const baseTitle = services.browser.title?.base ?? '';
    /** Why the page no longer follows its other editor (Python), when it does not: its title says so. Set, for good. */
    let notFollowed: string | undefined;
    /** Why following it failed the last time it moved (it is tried again at its next move): said as not following. */
    let followFailed: string | undefined;
    /** The page another editor serves, left: a new page opened here in its place (D1). For good. */
    let left = false;
    /** Stops watching the other editor (services.live.watch). */
    let unwatch: (() => void) | undefined;
    onCubeView = () => {
      const changed = dirty();
      page.setChanged(changed);
      services.browser.title?.set(page.empty
        ? baseTitle
        : `${changed ? '\u2022 ' : ''}${page.suggestedName}${notFollowed ?? followFailed ? ` (not followed: ${notFollowed ?? followFailed})` : ''} \u2013 ${baseTitle}`);
      library?.sync();
      // what the page is now, to its other editor once the change settles
      liveChanged();
    };
    // Leaving the page with unsaved changes asks, the browser's way -- where it could have been saved
    work.add(() => (services.pages && dirty() ? 'unsaved changes' : undefined));
    work.add(() => sources.filesHeld());
    const onLeave = (event: BeforeUnloadEvent): void => {
      if (leaving || work.what() === undefined) return;
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', onLeave);
    undo.push(() => window.removeEventListener('beforeunload', onLeave));
    // the tab going (closed, navigated away): each copy its grids' host keeps for them asked to be let go -- the host
    // keeps one its page's other editor reads (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8)
    const onGone = (): void => {
      for (const cube of madeCubes.values()) cube.leaving();
    };
    window.addEventListener('pagehide', onGone);
    undo.push(() => window.removeEventListener('pagehide', onGone));

    // THE PAGE ANOTHER EDITOR SERVES (services.live: Python's `ll.Page`, §5), kept by its keeper
    // (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md) and written and followed through its one channel (live-channel.ts): written
    // once a change settles, against the version on screen, as the edits made here -- which the keeper has DataCube apply
    // to the page as it has it (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4), the page then opened as the keeper made it
    // when it made another, refused in words when one does not apply -- or, undone or redone, whole (a restore, kept only
    // at the version it was made at); and followed as its other editor changes it (below). A page of no grid is a page too.
    const live = services.live;
    if (live?.page) {
      // the ids this page makes are this tab's own while the page is its keeper's (page-ids.ts nextPageId): no other
      // editor of it makes the same, and nothing is given out afresh (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md)
      page.idTag = idTag();
      channel = new LiveChannel(live.page, {
        // the page as it is, its own name with it ('' unnamed: D11)
        page: () => pageNow(page.title, true),
        busy: () => [...madeCubes.values()].some((cube) => cube.busy),
        converge: (on) => converge(on),
        reapply: async (pending, restarted) => {
          for (const edit of pending) {
            if ('page' in edit) {
              if (!page.reapply([edit.page], 'made again')) sayResting({ text: `not kept: ${edit.page.kind} no longer applies to the page as its keeper has it`, warn: true });
              continue;
            }
            const cube = madeCubes.get(edit.grid);
            const refused = cube === undefined ? `no grid ${edit.grid} here` : await cube.edit([edit.command], 'made again', { again: !restarted.has(edit.grid) });
            if (refused !== null) sayResting({ text: `not kept: ${refused}`, warn: true });
          }
        },
        // the two reopens that remain, a converge that could not be made, and the message that must never show
        // (live-channel.ts HASH_MISMATCH), each said
        reopen: async (why) => {
          const failed = await openLive();
          if (failed !== undefined) sayResting({ text: `not following the page: ${failed}`, warn: true });
          else sayResting({ text: `opened again as its keeper has it: ${why}`, warn: true });
        },
        say: (text) => sayResting({ text, warn: true }),
      });
      undo.push(() => channel?.stop());
    }
    /** The screen put at its keeper's page (live-converge.ts; docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md): the
     *  page, its grids, and how a grid another editor added is opened, handed to the one rule. */
    function converge(on: Converge): Promise<Converged> {
      return convergeOn(on, {
        page,
        grid: (id) => madeCubes.get(id),
        pageNow: () => pageNow(page.title, true),
        // left while a grid of the keeper's page was opened (New ▸ Blank Page, Open…): nothing of it lands on the page
        // of this tab's own (the S4 audit's 4)
        open: async (document, grid, several) => {
          const opened = await sources.reopened(document, undefined, several, page.model?.tables[grid], true);
          if (left) throw new Error("the page Python serves was left: nothing of it lands on this tab's own");
          return opened;
        },
      }).then((converged) => {
        onCubeView?.();
        return converged;
      });
    }
    function liveChanged(): void {
      if (notFollowed === undefined) channel?.changed();
    }
    /**
     * A NEW PAGE ON A PAGE ANOTHER EDITOR SERVES (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D1, P10): Open..., open in place
     * and Blank Page put a page of this tab's own on screen and leave the served one as it is -- nothing of the new page
     * written to it, none of its moves followed here -- said once, in the status line; what is said, for a page that has
     * no grid to show that line (a blank page says it on its card), or undefined: no page served is left.
     */
    function leaveServed(): string | undefined {
      if (!live?.page || left) return undefined;
      left = true;
      channel?.stop();
      channel = undefined;
      page.idTag = undefined;
      unwatch?.();
      const said = "a page of this tab's own: the page Python serves is left as it was";
      sayResting({ text: said, warn: false });
      return said;
    }
    /** Work in the live queue: the channel's (writes and follows in order), or, following a frame alone, one of its own. */
    let frameQueue: Promise<void> = Promise.resolve();
    const enqueue = (work: () => Promise<void>): Promise<void> => {
      if (channel) return channel.enqueue(work);
      const run = frameQueue.then(work);
      frameQueue = run.catch(() => undefined);
      return run;
    };

    /** LATEST WINS: each open in place takes a number, and one overtaken by a newer open lands nothing (P2-330). */
    const opens = new Latest();
    /**
     * WHICH PAGE IS ON SCREEN: each page put there (opened in place, a saved one restored, a blank one) takes a number,
     * and `current` -- what Save writes over -- is set with it. One that lands after another page was put there keeps
     * no baseline of the newer page.
     */
    let placed = 0;
    const place = (): (() => boolean) => {
      const mine = (placed += 1);
      return () => placed === mine;
    };

    /**
     * A SOURCE IN PLACE of the page: everything on it goes (asked first when there are unsaved changes), then one grid
     * over the source -- fresh, the page's name its. An open overtaken by a newer one lands nothing.
     */
    async function inPlace(open: () => Promise<Opened>, what: string): Promise<readonly string[]> {
      if (dirty() && !window.confirm(`The page has unsaved changes. Open ${what} anyway?`)) return [];
      const newest = opens.start();
      const o = await open();
      if (!newest()) {
        await o.discard?.();
        return [];
      }
      const onScreen = place();
      // a page of this tab's own, in place of the one its other editor serves (D1; the S7 audit's H-12: a `replacing`
      // that no caller passed, which would have cleared the page and left it served, is gone)
      leaveServed();
      // a new page, unnamed until saved (PageApp.clear: D11): its bar says the grid's report title (its source's name)
      page.clear();
      current = {};
      page.addGrid(o.make);
      await page.ready();
      // another page put on screen while this one landed: that page is the one to keep a baseline of
      if (!onScreen()) return [];
      // The baseline is the page as it LANDED (normalized by its first refresh); a name given in the name box while it
      // landed stays (a change, as a rename is)
      const landed = savedForm(page.suggestedName);
      current = { ...(landed ? { baseline: landed.definition } : {}), lost: [] };
      onCubeView?.();
      library?.sync();
      return o.notes;
    }

    // SAVED PAGES (the services' page store): in this browser, IndexedDB when the browser gives it, else memory (this
    // visit only, and said so); and their files' handles. A host with no store offers no Save or Open (§3.4).
    const pages = services.pages ? await services.pages : undefined;
    let persistent = false;
    /** Ask that what is saved be kept, once. */
    const persist = async (): Promise<boolean> => (persistent ||= (await pages?.persist()) ?? false);

    /**
     * OPEN A SAVED PAGE (from the store, a file someone handed over, a share link): each of its cubes through its own
     * source, in turn, then the page laid out as it was saved -- its grids under their saved ids, its charts, its
     * layout. A cube whose source was not opened is kept as the page saved it, its views placeholders that say why
     * (P9), and said. The cubes opened again as another than the page held (a host's frame whose columns moved:
     * Opened.taken), by their ids. `held`: every id the page has held where its other editor keeps it (B12).
     */
    async function openSaved(saved: SavedDocument, id: string | undefined, replacing = false,
      held: readonly string[] = [], unread: string[] = []): Promise<readonly string[]> {
      // a page, or a cube saved alone before pages: the page it opens as (page-document.ts savedPage, Python's Page.open's)
      const given = savedPage(saved);
      // a saved page opens live, each grid that was snapped said: a copy lasts only as long as the session that made it
      // (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.8). The page its other editor serves (`replacing`) keeps them: its
      // host holds them now.
      const opened = replacing ? { page: given, notes: [] } : openedLive(given);
      const doc = opened.page;
      // `replacing`: the page's other editor replaced it -- nothing of the person's is asked about
      if (!replacing && dirty() && !window.confirm(`The page has unsaved changes. Open "${doc.name}" anyway?`)) return [];
      // LATEST WINS (P2-330), as an open in place: each source is a wait (a file asked for, a sign-in), and an open
      // overtaken meanwhile lands nothing -- what it read is dropped
      const newest = opens.start();
      const makers = new Map<string, GridMaker>();
      const read: Opened[] = [];
      const taken: string[] = [];
      const notes: string[] = [...opened.notes];
      sources.reserve(Object.values(doc.model?.tables ?? {}).flat());
      try {
        // a class's grid after the grids whose tables it reads (page-document.ts openingOrder)
        for (const { id: cubeId, cube } of openingOrder(doc.cubes)) {
          let o: Opened | undefined;
          let why = 'not opened';
          try {
            o = await sources.reopened(cube, id, gridCapped(doc.cubes.length), doc.model?.tables[cubeId], replacing);
          } catch (e) {
            // a class whose rows are not here (its table read by no grid of the tab), or a join that no longer joins (an
            // input's file changed under it, a remote file that asks for keys) is left out and said, as a file not
            // given is: the page's other grids open (page-document.ts leftOutWhenUnopened, Python's load's rule too)
            if (!leftOutWhenUnopened(cube.source)) throw e;
            why = e instanceof Error ? e.message : String(e);
          }
          if (o) read.push(o);
          if (!newest()) {
            for (const r of read) await r.discard?.();
            return [];
          }
          if (!o) {
            notes.push(leftOutNote(cube.source.name, why));
            continue;
          }
          if (o.taken) taken.push(cubeId);
          makers.set(cubeId, o.make);
          notes.push(...o.notes);
        }
      } catch (e) {
        // a source that could not be opened: what the others read goes, and the page on screen stays
        for (const r of read) await r.discard?.();
        throw e;
      } finally {
        sources.release();
      }
      // a page its other editor serves opens as its keeper has it, a page of no grid (or none opened) too: its name, its
      // sheets and the ids it holds are the page's (the S7 audit's H-1)
      const unopened = replacing ? undefined : noneOpened(doc.name, makers.size);
      if (unopened !== undefined) {
        library?.say(unopened, 'warn');
        return [];
      }
      // the page on screen is this one from now: Save writes over it, never over the page it replaced
      const onScreen = place();
      // a page of this tab's own, in place of the one its other editor serves (D1)
      if (!replacing) leaveServed();
      // the saved page it is, born with it (N1): the store's record it was opened from, else the one its other editor
      // saved it as (a page file is none: pageFile writes no record) -- the page's state holds it (the S7 audit's H-4)
      const record = id ?? (replacing ? doc.record : undefined);
      const { record: _given, ...unsaved } = doc;
      page.restore(record !== undefined ? { ...unsaved, record } : unsaved, makers, held);
      current = {
        savedName: doc.name,
        ...(doc.unknown ? { pageUnknown: doc.unknown } : {}),
        cubeUnknown: new Map(doc.cubes.filter((c) => c.cube.unknown).map((c) => [gridOfCube(doc, c.id), c.cube.unknown!])),
      };
      page.setTitle(doc.name);
      await page.ready();
      // another page put on screen while this one landed: that page is the one to keep
      if (!onScreen()) return [];
      // each snapped grid of the page its other editor serves reads that editor's copy (§18.8); one its host no longer
      // holds reads its rows live, said
      for (const [cube, copy] of Object.entries(doc.copies ?? {})) {
        const grid = page.grid(cube);
        try {
          if (!grid?.followSnap) throw new Error('this grid reads no copy its host makes');
          await grid.followSnap(copy);
        } catch (e) {
          // read live here: said, and -- its grid's id in `unread` -- told as its live command once the page is open, so
          // its keeper's page is live too (the S7 audit's G-3); a detached chart's cube has no grid a live command names:
          // said alone, its keeper's copy kept (the S7 confirmation audit's H2-4: told, it was refused, and the page
          // opened again, and told again)
          const shownIn = doc.views.find((v) => v.kind === 'grid' && v.cube === cube)?.id;
          if (shownIn !== undefined) unread.push(shownIn);
          notes.push(`${doc.cubes.find((c) => c.id === cube)?.cube.source.name ?? cube} reads its rows live: its snap `
            + `(${copy.label}) was not read: ${e instanceof Error ? e.message : String(e)}`);
        }
        if (!onScreen()) return [];
      }
      // each file the page reads, where it was picked: kept for the next time it is opened
      if (id !== undefined) await keepHandles(id);
      // The baseline is the page as it LANDED (normalized by its first refresh); a page opened with parts left out is
      // changed from the start.
      const landed = savedForm(doc.name);
      current = { ...current, ...(landed ? { baseline: landed.definition } : {}), lost: notes.filter((n) => n.startsWith('left out')) };
      onCubeView?.();
      library?.sync();
      library?.say(notes.length === 0
        ? `opened "${doc.name}"`
        : `opened "${doc.name}", with changes since it was saved:\n${notes.map((n) => `- ${n}`).join('\n')}`,
      notes.length === 0 ? 'ok' : 'warn');
      // opened cleanly: the window goes, the page is what the person wanted to see
      if (notes.length === 0) closeCubes?.();
      return taken;
    }

    /**
     * Each file the page's grids read, where it was picked (a join's leaves' among them, a grid kept off the board for a
     * detached chart included): kept by the page and the file's contents (file-handles.ts handleKey), so opening the
     * page again reads it from there.
     */
    async function keepHandles(pageId: string): Promise<void> {
      for (const f of sources.filesNow()) if (f.handle) await pages?.handles?.put(handleKey(pageId, f.sha256), f.handle);
    }

    /** The grid a saved page's cube is shown in (its grid view's id), or the cube's own id (a detached chart's). */
    const gridOfCube = (doc: PageDocument, cube: string): string =>
      doc.views.find((v) => v.kind === 'grid' && v.cube === cube)?.id ?? cube;

    /** Save the page on screen: over its saved copy, or as a new one (the Save window's act). */
    async function saveTo(name: string, asNew: boolean): Promise<void> {
      if (!pages) throw new Error('This page has nowhere to be saved.');
      const { store } = pages;
      const refused = page.saveRefusal();
      if (refused) throw new Error(refused);
      // on a page another editor serves: what is pending written first, so the page saved is its keeper's (H-5)
      await channel?.flush();
      const doc = pageNow(name);
      if (!doc) throw new Error('This page cannot be saved yet: a grid on it does not know its source.');
      // the record by DataCube's one rule (pageSaving: Python's save() and save_as() ask it too): over the saved page it
      // is, or, Save As, a new one; the page named as it is saved
      const saving = pageSaving(name, doc, crypto.randomUUID(), asNew);
      if ('refused' in saving) throw new Error(saving.refused);
      const { record } = saving;
      const id = record.id;
      if (saving.over) await store.update(id, record);
      else await store.create(record);
      await keepHandles(id);
      current = { ...current, savedName: record.name, baseline: pageDefinitionText(doc), lost: [] };
      // the saved page it is, applied to the page's state by its command and told: its other editor saves over the same
      // record (B13)
      page.savedAs(id);
      // saved under a name: the page renamed, told as the name box tells it
      page.rename(record.name);
      onCubeView?.();
      await persist();
      library?.sync();
    }

    if (pages) library = new CubeLibrary(windows.library, pages.store, {
      currentId: () => page.record,
      dirty,
      open: async (id) => { await openSaved(readSaved((await pages.store.get(id)).content), id); },
      openText: async (text) => { await openSaved(readSaved(text), undefined); },
      forget: async (id) => {
        // every handle the page kept, whichever page is on screen
        await pages.handles?.removePage(id);
        // that record is gone: the page is no saved page any more, for its other editor too
        if (page.record === id) page.savedAs(null);
      },
    });
    // OPEN… (the user, 2026-10-01: "fix the Open dialog now too"): the saved cubes in a window of
    // their own, as the source picker and Save are -- searched, sorted, opened, deleted, or a cube
    // file opened. It closes once a cube opens cleanly; what an open has to say (a file to choose,
    // changes since it was saved) keeps it open.
    if (pages) showCubes = () => {
      if (!closeCubes) {
        const backdrop = document.createElement('div');
        backdrop.id = 'cubeswin';
        backdrop.className = 'dc-picker-backdrop';
        const win = document.createElement('div');
        win.className = 'dc-picker dc-open dc-app-floating';
        win.setAttribute('role', 'dialog');
        win.setAttribute('aria-modal', 'true');
        win.setAttribute('aria-labelledby', 'dc-open-title');
        const head = document.createElement('div');
        head.className = 'dc-picker-head';
        const titles = document.createElement('div');
        titles.className = 'dc-picker-titles';
        const title = document.createElement('h2');
        title.className = 'dc-picker-title';
        title.id = 'dc-open-title';
        title.textContent = 'Open a saved cube';
        const sub = document.createElement('p');
        sub.className = 'dc-picker-subtitle';
        sub.textContent = pages.memoryOnly
          ? 'Kept for this visit only (this browser refused storage). Each reopens over its own file, or regenerates its example.'
          : `Saved ${pages.where}. Each reopens over its own source (a file asked for when needed), or regenerates its example.`;
        titles.append(title, sub);
        const close = document.createElement('button');
        close.type = 'button';
        close.className = 'dc-picker-close';
        close.setAttribute('aria-label', 'Close');
        close.textContent = '\u00d7';
        head.append(titles, close);
        const body = document.createElement('div');
        body.className = 'dc-open-body';
        body.append(windows.library, examplesList());
        win.append(head, body);
        backdrop.append(win);
        o.windows.append(backdrop);
        const onKey = (e: KeyboardEvent): void => {
          if (e.key === 'Escape') { e.preventDefault(); closeCubes?.(); }
        };
        document.addEventListener('keydown', onKey, true);
        closeCubes = () => {
          windows.libraryHome.append(windows.library);
          document.removeEventListener('keydown', onKey, true);
          backdrop.remove();
          closeCubes = undefined;
        };
        close.addEventListener('click', () => closeCubes?.());
        backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop) closeCubes?.(); });
      }
      library?.sync();
      void library?.refresh();
      (document.querySelector('#cubelib .dc-lib-search') as HTMLElement | null)?.focus();
    };
    /**
     * THE EXAMPLE PAGES (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3), under the saved ones: each built when it is
     * picked (its own chunk) and opened as a saved page is, over a sample made here -- nothing saved until it is.
     */
    const examplesList = (): HTMLElement => {
      const box = document.createElement('section');
      box.className = 'dc-open-examples';
      box.setAttribute('aria-label', 'Examples');
      const heading = document.createElement('h3');
      heading.className = 'dc-open-examples-title';
      heading.textContent = 'Examples';
      box.append(heading);
      for (const example of EXAMPLE_PAGES) {
        const item = document.createElement('button');
        item.type = 'button';
        item.className = 'dc-open-example';
        item.dataset['example'] = example.id;
        const name = document.createElement('span');
        name.className = 'dc-open-example-name';
        name.textContent = example.label;
        const about = document.createElement('span');
        about.className = 'dc-open-example-about';
        about.textContent = example.about;
        item.append(name, about);
        item.addEventListener('click', () => {
          item.disabled = true;
          void (async () => {
            try {
              await openSaved(readSaved(pageToJson(await example.build())), undefined);
            } catch (e) {
              library?.say(`Could not open the example: ${e instanceof Error ? e.message : String(e)}`, 'warn');
            } finally {
              item.disabled = false;
            }
          })();
        });
        box.append(item);
      }
      return box;
    };

    // THE MENU'S SAVE AND SAVE AS: a window of their own (src/ui/save-dialog.ts) -- the name, where
    // it is kept, what is saved and what is not (the rows), and what saving over would drop
    /** What each grid's source needs to be read again, in words, once per kind of source on the page. */
    const sourcesSaid = (doc: PageDocument | undefined, how: 'save' | 'share'): string => {
      const said = new Set<string>();
      for (const { cube } of doc?.cubes ?? []) {
        const src = cube.source;
        said.add(how === 'save'
          ? src._type === 'savedQuery' ? `opening it runs the saved query “${src.name}” again`
            : src._type === 'warehouseTable' ? `opening it reads ${src.name} on the warehouse again, signed in as whoever opens it`
              : src._type === 'remoteFile' ? `opening it reads ${src.name} again from its URL`
                : src._type === 'frame' ? `it reads the dataframe ${src.name}: it opens where Python serves it`
                : src._type === 'class' ? `opening it reads the class ${src.name} from ${src.project} again, at the revision it was read at, its rows where its table is open`
                : src._type === 'join' ? `opening it reads the files ${src.name} joins again, and joins them`
                : src.sample ? `the example (${src.sample.rows.toLocaleString(UI_LOCALE)} rows) is generated again when it opens`
                  : `opening it reads ${src.name} again, from your computer`
          : src._type === 'savedQuery' ? `it reads the saved query “${src.name}” again, where its project (${src.query.groupId}:${src.query.artifactId}) is known`
            : src._type === 'warehouseTable' ? `whoever opens it signs in to the warehouse as themselves and needs to be granted ${src.name}; no sign-in is in the link`
              : src._type === 'remoteFile' ? `it reads ${src.url} again; no keys are in the link, a private bucket asks for them`
                : src._type === 'frame' ? `it reads the dataframe ${src.name}, which only the Python that serves it has`
                : src._type === 'class' ? `it reads the class ${src.name} from ${src.project}, which only this browser's model home has`
                : src._type === 'join' ? `whoever opens it needs the files ${src.name} joins`
                : src.sample ? 'it rebuilds its sample on its own'
                  : `whoever opens it needs ${src.name}`);
      }
      return [...said].join('; ');
    };

    // THE MENU'S SAVE AND SAVE AS: a window of their own (src/ui/save-dialog.ts) -- the name, where
    // it is kept, what is saved and what is not (the rows), and what saving over would drop
    if (pages) saveCube = (asNew) => {
      if (page.empty) {
        library?.say(NOTHING_TO_SAVE, 'warn');
        return;
      }
      void (async () => {
        const refused = page.saveRefusal();
        const offered = page.suggestedName;
        const savedAt = page.record !== undefined ? (await pages.store.get(page.record).catch(() => undefined))?.lastUpdatedAt : undefined;
        const doc = page.document(offered);
        const views = page.views().views;
        const charts = views.filter((v) => v.kind === 'chart').length;
        const grids = page.grids.map((id) => ({ title: views.find((v) => v.id === id && v.kind === 'grid')?.title ?? id, grid: page.grid(id)! }));
        const shape = (s: CubeSnapshot): string => [
          s.rows.length > 0 ? `grouped by ${s.rows.join(', ')}` : 'its rows as they are, grouped by nothing',
          ...(s.pivotOn.length > 0 ? [`pivoted on ${s.pivotOn.join(', ')}`] : []),
          ...(s.filter ? ['its filter'] : []),
          ...(s.derived.length + (s.groupDerived?.length ?? 0) > 0
            ? [`${s.derived.length + (s.groupDerived?.length ?? 0)} calculated column${s.derived.length + (s.groupDerived?.length ?? 0) === 1 ? '' : 's'}`] : []),
        ].join(', ');
        const keeps = [
          ...(grids.length === 1
            ? [shape(grids[0]!.grid.snapshot).replace(/^./, (c) => c.toUpperCase())]
            : grids.map(({ title, grid }) => `${title}: ${shape(grid.snapshot)}`)),
          ...(charts > 0 ? [`${charts} visualization${charts === 1 ? '' : 's'}, and the page's layout`] : []),
          grids.length > 1 ? 'Each grid\'s formats, widths, colours and settings' : 'Its formats, widths, colours and settings',
        ];
        const leaves = `Not the rows: ${sourcesSaid(doc, 'save') || 'opening it reads its sources again'}.`;
        await saveDialog(document, {
          purpose: asNew ? 'saveAs' : 'save',
          name: offered,
          ...(page.record !== undefined
            ? { over: { name: current.savedName ?? offered, ...(savedAt !== undefined ? { savedAt } : {}) } } : {}),
          where: pages.where,
          keeps,
          leaves,
          ...(current.lost?.length
            ? { warning: `The saved “${current.savedName ?? offered}” has parts this page cannot show: ${current.lost.join('; ')}.` } : {}),
          save: async (name, saveAsNew) => {
            if (refused) throw new Error(refused);
            await saveTo(name, saveAsNew);
          },
        });
      })();
    };

    // THE SHARE LINK: the page's settings, never its data. It is said HOW LONG it is, and a long
    // one -- which some mail and chat tools cut -- is said so, with the file suggested instead.
    // SHARE: the link in a window of its own, shown and copied to the clipboard at once, with a
    // way to copy it again -- and what it holds (the page's settings, never its data) said there.
    copyShareLink = async () => {
      // a link needs an address others can open: the menu offers Share only where the page has one
      const address = services.browser.address;
      if (!address) return;
      // nothing on screen is said the way any unsharable page is: in the window, not as a dead button
      const refused = page.saveRefusal();
      const name = page.suggestedName;
      const shared = refused ? undefined : page.document(name, {
        ...(current.pageUnknown ? { page: current.pageUnknown } : {}),
        ...(current.cubeUnknown ? { cubes: current.cubeUnknown } : {}),
      });
      const { link: field, note, copy } = windows.shareParts;
      windows.show(windows.share, { width: 560, height: 200 });
      // nothing to copy: the window says why, without an empty link and a dead button
      field.hidden = !shared;
      copy.hidden = !shared;
      if (!shared) {
        field.value = '';
        note.textContent = refused ?? 'This page cannot be shared yet: a grid on it does not know its source.';
        note.className = 'sharenote bad';
        return;
      }
      const link = shareLink(address(), shared);
      field.value = link.url;
      const about = `It holds the page's settings, filter values included, not its data; ${sourcesSaid(shared, 'share')}.`
        // the page's classes are in this browser's model home (Studio's): a link carries none of them
        + (page.model ? ` Its classes stay in ${page.model.project}, in this browser: the link does not carry them.` : '');
      const put = async (): Promise<void> => {
        field.select();
        try {
          await services.browser.writeClipboard(link.url);
        } catch {
          note.textContent = `The browser did not allow copying: select the link and copy it. ${about}`;
          note.className = 'sharenote bad';
          return;
        }
        note.textContent = link.long
          ? `Copied, but it is long (${link.length.toLocaleString(UI_LOCALE)} characters): some mail and chat tools cut links this long. ${about}`
          : `Copied to the clipboard (${link.length.toLocaleString(UI_LOCALE)} characters). ${about}`;
        note.className = link.long ? 'sharenote warn' : 'sharenote';
      };
      copy.onclick = () => void put();
      await put();
    };

    /** The model home (the page's own SDLC and Depot, or servers), when the page has one: the services'. */
    const modelHome = services.modelHome;

    /** CREATE A CLASS… of a grid (src/create-class.ts, fetched the first time it is asked for). */
    const makeClass = async (grid: CubeApp, place: GridPlace, cube: string | undefined): Promise<void> => {
      // the host's writer and database (the tab's module and DuckDB, Python's native library and DuckDB), and the grid's
      // rows where they are read: what the action needs, on every host that gives it
      const writing = services.engine.modelWriting;
      const rows = rowsOf(place);
      const source = rows?.host.planner.source?.();
      if (!modelHome || !writing || !rows || !source) return;
      const sourceName = place.label ?? 'this source';
      // what the class is made over: its rows at their finest level and every table they read -- a file's grid one, a
      // join its every leaf's (§18.3); refused before the window opens, in words: a pivot, rows read from no table
      let basis;
      try {
        basis = classBasis(grid.snapshot);
      } catch (e) {
        window.alert(`Cannot create a class of this grid: ${e instanceof Error ? e.message : String(e)}`);
        return;
      }
      const [{ createClassOf }, home] = await Promise.all([lazily(() => import('../create-class.ts')), modelHome.connect()]);
      const commands = await createClassOf({
        basis,
        // the names its leaves' tables have in this tab, one per leaf (a file's, a join's each), which reopening reads
        // them under; a grid whose tables are read under the store's own names (a frame's, a warehouse table's, a saved
        // query's) keeps those
        kept: place.tables ?? basis.tables.map((t) => t.table),
        rows,
        source,
        writer: writing.writer,
        place: writing.place,
        sourceName,
        cube,
        // the page's name, or -- never named -- what it is called, without a file's extension (modelPageName, D11)
        pageName: modelPageName(page.title, page.suggestedName),
        model: page.model,
        client: home.client,
        where: home.where,
        ...(modelHome.studio ? { studio: (project: string, workspace: string) => modelHome.studio!(project, workspace) } : {}),
        persist,
      });
      landed(commands);
    };
    pageModelNow = () => page.model;
    /**
     * THE PAGE'S MODEL ACTIONS (src/model-actions.ts, fetched the first time either is asked for): each grid of the
     * page as they read it -- its class when it is one, else its rows as a class's, or why they cannot be one.
     */
    const modelActions = async (): Promise<{ actions: typeof import('../model-actions.ts'); of: import('../model-actions.ts').ModelActionsOf } | undefined> => {
      const writing = services.engine.modelWriting;
      if (!modelHome || !writing) return undefined;
      const [actions, home] = await Promise.all([lazily(() => import('../model-actions.ts')), modelHome.connect()]);
      const grids = actions.pageGrids(page.grids, (id) => {
        const place = sources.place(id);
        const grid = page.grid(id);
        const rows = place ? rowsOf(place) : undefined;
        return place && grid && rows
          ? { snapshot: grid.snapshot, label: place.label ?? id, rows, ...(place.tables ? { tables: place.tables } : {}) }
          : undefined;
      }, (id) => page.model?.classes?.[id], { classBasis });
      return {
        actions,
        of: {
          writer: writing.writer,
          place: writing.place,
          rows: (model, runtime) => writing.rows(model, runtime),
          pageName: modelPageName(page.title, page.suggestedName),
          model: page.model,
          client: home.client,
          where: home.where,
          grids,
          ...(modelHome.studio ? { studio: (project: string, workspace: string) => modelHome.studio!(project, workspace) } : {}),
          persist,
          duplicate: duplicateLambda,
          call: (path) => fn(path),
        },
      };
    };
    /** What a model action changed of the page's model, applied by the one function -- told, no step of its history
     *  (D5, P15). */
    const landed = (commands: readonly PageCommand[]): void => {
      if (commands.length === 0) return;
      page.applyModel(commands);
      onCubeView?.();
    };
    const said = (e: unknown): void => window.alert(e instanceof Error ? e.message : String(e));
    createAssociation = (first) => {
      void modelActions().then(async (m) => m && landed(await m.actions.createAssociationOf(m.of, first))).catch(said);
    };
    createDataSpace = () => {
      void modelActions().then(async (m) => m && landed(await m.actions.createDataSpaceOf(m.of))).catch(said);
    };
    createClass = (grid, place, cube) => {
      void makeClass(grid, place, cube).catch((e: unknown) => window.alert(`Create a Class: ${e instanceof Error ? e.message : String(e)}`));
    };
    // ADD A GRID OVER <class>: the class this grid was made (Create a Class…), read where the page reads a class -- here,
    // where it plans and runs its own SQL, or, on a page its other editor serves (Python's), by that host (K16) -- by the
    // one rule Python's page.grid(cls=...) names it by (sources' class kind), beside the grid
    if (services.engine.local || services.engine.database.made) classGrid = (cube) => {
      void (async () => {
        const cls = cube === undefined ? undefined : page.model?.classes?.[cube];
        if (cls === undefined || cube === undefined) throw new Error('this grid was made no class of the page');
        // the page on screen now: one put in its place meanwhile (another opened, a blank page) gets no grid of this one
        const shown = placed;
        const opened = await sources.opened({ kind: 'class', given: { class: cls } }, gridCapped(page.cubes.length + 1));
        if (placed !== shown) {
          await opened.discard?.();
          return;
        }
        page.addGrid(opened.make, { near: cube });
      })().catch((e: unknown) => window.alert(`Create ▸ Grid over a class: ${e instanceof Error ? e.message : String(e)}`));
    };
    // JOIN… (§17.5): where the page plans and runs its own SQL, its grids' tables in its own database (§17.3); on a
    // host's page (Python's), which has none there, the host answers its window's asks over its tables (HostJoin: B11).
    // Its flow fetched with the first Join… (app/join-app.ts)
    const localEngine = services.engine.local;
    const hostJoin = services.engine.database.join;
    if (localEngine || hostJoin) joinGrid = {
      // a grid of the tab's tables; on a host's page any grid written down by a source (what it reads is the host's to
      // refuse, in words, before the window opens: join-flow.ts hostJoinFlow)
      offered: (place) => (localEngine ? place.tables !== undefined : place.cubeSource !== undefined),
      open: (cube, ask) => {
        if (cube === undefined) return;
        const shown = placed;
        const stillShown = () => placed === shown;
        const asked = ask !== undefined ? { ask } : {};
        void lazily(() => import('./join-app.ts')).then(({ joinFlow, hostJoinFlow }) => (localEngine
          ? joinFlow({ cube, page, sources, services: { ...services, engine: { ...services.engine, local: localEngine } }, stillShown, ...asked })
          : hostJoinFlow({ cube, page, sources, join: hostJoin!, parse: (text) => services.engine.reading.parse(text), stillShown, ...asked })))
          .catch((e: unknown) => window.alert(`Join: ${e instanceof Error ? e.message : String(e)}`));
      },
    };
    // A GRID'S RIGHT-CLICK JOIN ▸, as its Filter ▸ (join-menu.ts): the page's grids it can join gathered here, as it
    // opens -- the grid's own and every other's columns, a pivoted one (no fixed columns) left out. A pivoted grid's own
    // Join ▸ is the window alone, as on no column: no quick join of columns it does not have fixed, but Join… still
    // there (the start's grid is pivoted; its menu button, which offered it, went 2026-10-23)
    if (joinGrid) {
      const joins = joinGrid;
      const joinable = (id: string): boolean => { const p = sources.place(id); return p !== undefined && joins.offered(p); };
      joinMenuOf = (cube, column) => {
        const now = page.document(page.title);
        // a grid whose rows are no table of this page's (a class's, a saved query's): Join ▸ still there, saying why
        if (!now || !joinable(cube)) {
          return { items: [{ label: 'Join…', disabled: true, hint: 'A join reads tables this page holds, and this grid\'s rows are none of them' }], asks: new Map() };
        }
        const input = (id: string) => {
          const i = joinInputOf(now, id);
          return { id, name: i.name, columns: i.query.columns, pivoted: i.query.pivotOn.length > 0 };
        };
        const mine = input(cube);
        const others = page.grids.filter((id) => id !== cube && joinable(id)).map(input).filter((o) => !o.pivoted);
        return joinMenu({ column: mine.pivoted ? undefined : column, mine, others, source: picker !== undefined });
      };
      joinSource = (cube) => {
        if (cube === undefined) return;
        void page.addSource(cube).then(async (their) => {
          if (their === undefined) return;
          await page.ready();
          joins.open(cube, { their });
        }).catch((e: unknown) => window.alert(`Join: ${e instanceof Error ? e.message : String(e)}`));
      };
    }

    /** The chosen source IN PLACE of the page: it empties, then one grid over the source (its only one: gridCapped). */
    async function openInPlace(chosen: Chosen): Promise<void> {
      await inPlace(() => sources.opened(chosen, gridCapped(1)), sources.named(chosen));
    }

    // THE SOURCE PICKER (the sources'): "add" makes a grid over the chosen source, beside the others; "open" puts it in
    // place of the page
    picker = (purpose: 'add' | 'open', start?: SectionId): Promise<GridMaker | undefined> => sources.pick<GridMaker>(purpose,
      async (chosen) => {
        // beside the page's grids: capped when it is not the page's only one (gridCapped, counting it)
        if (purpose === 'add') return (await sources.opened(chosen, gridCapped(page.cubes.length + 1))).make;
        await openInPlace(chosen);
        return null;
      }, start);

    // A BLANK PAGE (New ▸ Blank Page; the user, 2026-10-01): every grid and chart goes, and the page
    // says what to do first -- a data source (the picker, opening in place) or a saved page (PageApp's empty page,
    // `empty` above). Asked first when there are unsaved changes; the page stays until the person agrees.
    blankPage = (how) => {
      if (dirty() && !window.confirm('The page has unsaved changes. Start a blank page anyway?')) return;
      // an open on its way (a source, a saved page, the other editor's page) lands nothing over it
      opens.start();
      place();
      // asked for, on a page its other editor serves: a page of this tab's own in its place (D1), said on the card -- the
      // page has no grid whose status line would say it. A start that opened nothing leaves nothing: an empty page Python
      // serves is followed, and a first read that failed is tried again at its next move (the S4 audit's 1)
      const servedLeft = 'asked' in how ? leaveServed() : undefined;
      page.clear();
      current = {};
      sayWhyBlank('reason' in how ? how.reason : servedLeft);
      if (blankCard) blankCard.hidden = false;
      services.browser.title?.set('New page');
      (host.querySelector('.dc-blank-actions .dc-primary') as HTMLElement | null)?.focus();
    };

    // A SHARE LINK in the address opens its page, found the way a saved one is (a sample rebuilt,
    // a file asked for). Then the link is taken out of the address: a reload never reopens it over
    // what the person has done since. Last in the setup: opening one reaches everything above
    // (a sample's rebuild included).
    // A SAVED QUERY'S SHARE LINK (query-store/src/share.ts): `#q1.<data>`, or Legend Query's own
    // `#/shared/q1.<data>` -- the query, opened as the cube's source; taken out of the address, as above
    const hash = start.kind === 'link' ? start.hash : '';
    const queryLink = hash.replace(/^#\/shared\//, '#');
    const links = queryLink.length > 1 ? await queryStores() : undefined;
    if (start.kind === 'link' && links?.isQueryFragment(queryLink)) {
      start.forget();
      try {
        await openInPlace({ kind: 'saved', query: await sources.openedRecord(await links.readQueryFragment(queryLink)) });
      } catch (e) {
        failedStart(e);
      }
    }
    if (start.kind === 'link' && isPageFragment(hash)) {
      const fragment = hash;
      start.forget();
      showCubes?.();
      try {
        await openSaved({ kind: 'page', page: readPageFragment(fragment) }, undefined);
        // a page someone shared opens locked: nothing moves by accident (docs/DATACUBE_PAGES_DESIGN_2026_10_09.md
        // §3.3, 5); its menu's Edit Layout unlocks it
        page.setLayoutEditing(false);
      } catch (e) {
        library?.say(e instanceof Error ? e.message : String(e), 'error');
      }
    }

    // THE SINGLE-USER APP'S START (docs/DATACUBE_APP_PLAN_2026_10_02.md, A3): the launch key signs in
    // to the warehouse that served this page (its config.json names it), then the table asked for is
    // opened, or the tables are offered. The key stays in the address: a reload signs in again.
    if (start.kind === 'warehouse') {
      let on: SignedIn | undefined;
      try {
        on = await sources.signInWithKey(start.key);
      } catch (e) {
        failedStart(e);
      }
      const asked = start.table;
      const found = on && asked ? on.objects.filter((o) => `${o.schema}.${o.name}` === asked) : [];
      if (on && found.length === 1 && found[0]) {
        await openInPlace({ kind: 'table', session: on.session, object: found[0] });
      } else if (on) {
        if (asked) {
          failedStart(new Error(found.length === 0
            ? `${asked} is not a table you may read here`
            : `${asked} is in ${found.length} catalogs (${found.map((o) => o.catalog).join(', ')}): choose one`));
        }
        blankPage({ reason: startProblem });
        void picker('open', 'database');
      }
    }
    /**
     * THE PAGE THE OTHER EDITOR SERVES (Python's `ll.Page`), opened -- again, when Python changed it -- as it is now, on
     * the sheet shown when that sheet is still there. Nothing is reported while it opens; what it reports from then on is
     * of this version. Undefined, or why it did not open.
     */
    async function openLive(): Promise<string | undefined> {
      const editor = live?.page;
      if (!editor) return 'this page has no other editor';
      let said: Awaited<ReturnType<typeof editor.read>>;
      try {
        said = await editor.read();
      } catch (e) {
        return e instanceof Error ? e.message : String(e);
      }
      // left while it was read (New ▸ Blank Page, Open…): the page on screen is this tab's own (D1, the S4 audit's 4)
      if (left) return undefined;
      if (channel) channel.shown = undefined;
      const shown = page.shownSheet;
      // opened as the keeper has it: nothing the screen does meanwhile is an edit of its own
      let taken: readonly string[] = [];
      const unread: string[] = [];
      const open = async (): Promise<void> => {
        taken = await openSaved({ kind: 'page', page: said.page }, undefined, true, said.held, unread);
        if (page.sheets.includes(shown)) page.showSheet(shown);
      };
      await (channel ? channel.following(open) : open());
      channel?.opened(said.version, said.page, said.held);
      // a cube opened again over its host's frames as they are now, another than the keeper's (a frame changed in place
      // since the keeper last opened it): told as its `rebuilt`, the cube as the screen writes it (P11)
      const opened = taken.length > 0 ? pageNow(said.page.name, true) : undefined;
      const [{ applyPageCommands, rebuiltCommands }, { pageCommand }] = await Promise.all([lazily(() => import('../page-commands.ts')), lazily(() => import('../page-gestures.ts'))]);
      // a snapped grid whose copy was not read here reads its rows live: told so (the S7 audit's G-3) -- first, so a
      // grid it leaves over frames that changed meanwhile is told rebuilt too, as Python's Grid.live tells both (the S7
      // confirmation audit's H2-4: judged against the copy, it never was)
      const lives = unread.map((grid): PageCommand => pageCommand('live', { grid }));
      for (const command of lives) channel?.edit({ page: command });
      const base = lives.length > 0 ? applyPageCommands({ page: said.page, held: said.held }, lives).draft.page : said.page;
      for (const command of opened ? rebuiltCommands(base, opened, taken) : []) channel?.edit({ page: command });
      // the page as it opened, written once it settles (restoring is a change): Python's page is what is on screen
      liveChanged();
      return undefined;
    }

    /**
     * THE OTHER EDITOR'S CHANGE FOLLOWED BY ITS COMMANDS (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md): what moved its page
     * since the version on screen, applied to the page as it is here -- no grid made again, none queried again but those
     * a command changes, what was changed here meanwhile kept. When the editor says the page instead (a change by no
     * command), or its commands do not apply here (a tile closed meanwhile, an id given since), the page is opened again
     * as the editor has it, as every move was followed before. Undefined, or why it was not followed.
     */
    async function followLive(): Promise<string | undefined> {
      const editor = live?.page;
      // the channel this follow writes through: none once the page is left (D1)
      const through = channel;
      if (!editor || !through || through.shown === undefined) return openLive();
      // what is pending here written first, the screen waited for to be quiet (a gesture in flight lands and is told): a
      // follow never meets an un-rebased edit on screen, and another editor's removal never lands under a gesture still
      // running (F90; docs/DATACUBE_LIVE_EDITING_2026_10_24.md §2)
      await through.flush(true);
      if (left || disposed) return undefined;
      if (through.shown === undefined) return openLive();
      let now: Awaited<ReturnType<typeof editor.read>>;
      try {
        now = await editor.read();
      } catch {
        return openLive();
      }
      // left while it was asked (New ▸ Blank Page, Open…): the page on screen is this tab's own (the S4 audit's 4)
      if (left) return undefined;
      const wasChanged = dirty();
      // the screen put at the keeper's page (live-channel.ts follow, live-converge.ts: an edit made here while the keeper
      // was asked kept and made again after, the hash checked -- W17, said once); not followed: the page was opened
      // again, said (the channel's reopen is openLive)
      const followed = await through.follow(now);
      if (left || disposed || !followed) return undefined;
      // the editor's page is the page now, as opening it again made it: its name (a rename in Python), and -- unless
      // the person had changed it here -- "changed since saved" from here
      const name = page.title;
      if (!wasChanged) {
        const landed = savedForm(page.suggestedName);
        current = { ...current, savedName: name, ...(landed ? { baseline: landed.definition } : {}) };
      }
      onCubeView?.();
      // the page as it is now, written once it settles: Python's page is what is on screen
      liveChanged();
      return undefined;
    }

    // PYTHON'S START: the page Python serves (`ll.Page`; `ll.show(df)`, a page of one grid over the frame: P12)
    if (start.kind === 'live') {
      const why = await openLive().catch((e: unknown) => (e instanceof Error ? e.message : String(e)));
      if (why !== undefined) failedStart(new Error(why));
    }

    // FOLLOWING THE OTHER EDITOR (services.live, §5): Python moves what the page shows -- the page itself (its version:
    // followed by its commands, or opened again, on the sheet shown) or a frame (its version: the grids over it query
    // again, their views as they stand; a frame whose model changed, its columns, opens the page again as Python has it,
    // each grid's edits kept: B10) -- one move at a time, in order. A move that fails to be followed (a frame not read, a
    // page not opened) is said, and tried again at the next move: a success says nothing more of it. Told that Python no
    // longer says (the page or the frame closed, Python gone), the page keeps what it shows, says it no longer follows,
    // and reports nothing more.
    if (live) {
      const what = 'the page';
      const stop = (why: string): void => {
        if (notFollowed !== undefined) return;
        notFollowed = why;
        channel?.stop();
        harnessSignal.changes += 1;
        sayResting({ text: `not following ${what}: ${why}`, warn: true });
        onCubeView?.();
      };
      const failed = (why: string | undefined): void => {
        if (why === followFailed) return;
        followFailed = why;
        harnessSignal.changes += 1;
        sayResting(why === undefined ? { text: label, warn: false } : { text: `not following ${what}: ${why}`, warn: true });
        onCubeView?.();
      };
      /** What Python says now, followed: undefined, or why it was not. */
      const followed = async (now: Versions): Promise<string | undefined> => {
        // a start that did not open (a page not read, a frame not there yet) is tried again
        if (channel?.shown === undefined) return openLive();
        // each step below waits: the app taken down meanwhile (a notebook's cell run again) goes no further
        // a page moved: by its commands, or -- a frame moved with it -- opened again, every grid reading its frame now
        // a page moved: by its commands; a frame that moved with it is then read again by every grid over it, below (W16)
        const shown = channel?.shown;
        if (shown !== undefined && now.version !== undefined && now.version !== shown) {
          if (followBy(now, shown) === 'page') {
            const why = await followLive();
            if (why !== undefined || disposed) return why;
          }
        }
        for (const [frame, version] of Object.entries(now.frames)) {
          if (disposed) return undefined;
          const read = sources.frameVersion(frame);
          if (read === undefined || read === version) continue;
          const again = await sources.frameAgain(frame);
          if (disposed) return undefined;
          // its columns changed: the page as Python has it, opened over the frame as it is now, each grid's edits kept
          if (again === 'changed') return openLive();
          for (const id of page.cubes) {
            if (disposed) return undefined;
            if (sources.framesOf(id).includes(frame)) await madeCubes.get(id)?.refresh();
          }
        }
        return undefined;
      };
      unwatch = live.watch((now) => {
        enqueue(async () => {
          if (disposed || left || notFollowed !== undefined) return;
          if (typeof now === 'string') return stop(now);
          failed(await followed(now));
        }).catch((e: unknown) => failed(e instanceof Error ? e.message : String(e)));
      }, () => channel?.shown);
      undo.push(() => unwatch?.());
    }

    // A START THAT OPENED NOTHING (a link that failed, a key refused) leaves the blank page, saying
    // why: never an empty page without a word -- but the page its other editor serves, opened, which is shown as its keeper
    // has it, a page of no grid too (the S7 audit's H-1)
    if (page.empty && channel?.shown === undefined) blankPage({ reason: startProblem });
  }
  // the start has settled: from now on an empty page is a blank one
  if (blankCard) blankCard.hidden = false;

  return {
    dispose: () => {
      if (disposed) return;
      disposed = true;
      for (const u of undo.splice(0)) u();
      page.dispose();
    },
  };

  /** The reason a start opened nothing, said where the page says what is wrong. */
  function failedStart(e: unknown): void {
    harnessSignal.changes += 1;
    startProblem = e instanceof Error ? e.message : String(e);
    status.textContent = startProblem;
    status.classList.add('bad');
  }
}

/** A part of the services a grid's place needs, or -- absent -- the app's own bug, said: a place with nothing to run on. */
function must<T>(part: T | undefined, what: string): T {
  if (part === undefined) throw new Error(`${what}, which this host does not have: the grid's place names no runner`);
  return part;
}

/** The app's windows: the saved pages' list (shown in Open…), Share, and the Generated Pure & SQL. */
interface AppWindows {
  /** The saved pages' list, and where it lives between showings. */
  readonly library: HTMLElement;
  readonly libraryHome: HTMLElement;
  readonly share: HTMLElement;
  readonly shareParts: { readonly link: HTMLTextAreaElement; readonly note: HTMLElement; readonly copy: HTMLButtonElement };
  readonly query: HTMLElement;
  readonly pure: HTMLElement;
  readonly sql: HTMLElement;
  /** A window shown (left where it is if already shown), at `size` the first time. */
  show(win: HTMLElement, size?: { width: number; height: number }): void;
  toggle(win: HTMLElement): void;
  /** Every window taken out of the page (the app taken down). */
  remove(): void;
}

/**
 * The app's windows, built in `container` (they were the tab's markup): floating as the cube's own dialogs are
 * (`src/ui/window.ts`) -- dragged by the header, resized from any edge, remembering where they were left -- hidden until
 * asked for. Their ids are the ones the browser harnesses read.
 */
function appWindows(container: HTMLElement): AppWindows {
  const doc = container.ownerDocument;
  const el = <K extends keyof HTMLElementTagNameMap>(tag: K, props: Partial<HTMLElementTagNameMap[K]> = {}, ...kids: (Node | string)[]): HTMLElementTagNameMap[K] => {
    const e = doc.createElement(tag);
    Object.assign(e, props);
    e.append(...kids);
    return e;
  };
  const specs = new Map<HTMLElement, WindowSpec>();
  /** Each window's header, which it is dragged by. */
  const heads = new Map<HTMLElement, HTMLElement>();
  const hostWindow = (id: string, title: string, ...body: Node[]): HTMLElement => {
    const close = el('button', { type: 'button', className: 'hostwin-close', textContent: '\u00d7' });
    close.setAttribute('aria-label', 'Close');
    const head = el('div', { className: 'hostwin-head' }, el('span', { textContent: title }), close);
    const win = el('div', { id, className: 'hostwin', hidden: true }, head, el('div', { className: 'hostwin-body' }, ...body));
    close.addEventListener('click', () => { win.hidden = true; });
    heads.set(win, head);
    container.append(win);
    return win;
  };
  const library = el('div', { id: 'cubelib' });
  const libraryHome = el('div', { id: 'cubeholder', hidden: true }, library);
  container.append(libraryHome);
  const link = el('textarea', { id: 'sharelink', className: 'sharelink', rows: 4, readOnly: true });
  link.setAttribute('aria-label', 'The link to this page');
  const copy = el('button', { type: 'button', id: 'sharecopy', textContent: 'Copy' });
  const note = el('span', { id: 'sharenote', className: 'sharenote' });
  note.setAttribute('role', 'status');
  const share = hostWindow('sharewin', 'Share', link, el('div', { className: 'sharerow' }, copy, note));
  const pure = el('pre', { id: 'pure' });
  const sql = el('pre', { id: 'sql' });
  const query = hostWindow('querywin', 'Generated Pure & SQL', el('h2', { textContent: 'Pure' }), pure, el('h2', { textContent: 'SQL' }), sql);
  const show = (win: HTMLElement, size: { width: number; height: number } = { width: 720, height: 420 }): void => {
    if (!win.hidden) return;
    win.hidden = false;
    const head = heads.get(win);
    if (!head) return;
    const spec = specs.get(win);
    specs.set(win, makeWindow(win, head, container, { ...size, ...(spec ? { spec } : {}), onChange: (next) => specs.set(win, next) }));
  };
  return {
    library, libraryHome, share, shareParts: { link, note, copy }, query, pure, sql, show,
    remove: () => { for (const w of [libraryHome, share, query]) w.remove(); },
    toggle: (win) => {
      if (!win.hidden) {
        win.hidden = true;
        return;
      }
      show(win);
    },
  };
}
