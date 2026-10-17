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
import { gridCapped, type CubeDocument } from '../cube-document.ts';
import { Latest, TabWork, mayLeave } from '../host.ts';
import { PlanThenRun, type PlanAndRun } from '../runner.ts';
import { PageApp, type GridMaker } from '../page/page-app.ts';
import {
  pageContent,
  pageDefinitionText,
  pageToJson,
  readSaved,
  oneSheet,
  PAGE_VERSION,
  type PageDocument,
  type SavedDocument,
} from '../page-document.ts';
import type { PageModelRef } from '../page-model.ts';
import { classLambda, duplicateLambda, sampleLambda, storeTableOf } from '../query.ts';
import { isPageFragment, readPageFragment, shareLink } from '../share/link.ts';
import type { CubeSnapshot } from '../snapshot.ts';
import { CubeLibrary } from '../ui/cube-library.ts';
import { EXAMPLE_PAGES } from '../examples/catalog.ts';
import type { MenuItem } from '../ui/menu.ts';
import { saveDialog } from '../ui/save-dialog.ts';
import type { SectionId } from '../ui/source-picker.ts';
import { makeWindow, type WindowSpec } from '../ui/window.ts';
import type { Sample, Services, Versions } from './services.ts';
import { pageSources, picksSources, type Chosen, type GridPlace, type Opened, type SignedIn } from './sources.ts';

// the query store's code, fetched the first time it is needed -- a query link -- as charts are: a grid-only page never
// downloads it (test/bundle-budget.test.ts)
const queryStores = () => import('../../../query-store/src/index.ts');

/** CREATE A CLASS… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1): a grid's own entry, in its menu's data group. */
const CREATE_CLASS = { id: 'host.createClass', label: 'Create a Class\u2026', section: 'data' } as const;
/** CREATE AN ASSOCIATION… and A DATA SPACE… (§15.1, §15.6a): the page's, in its menu's Model group; the first a grid's
 *  too, its class one end. */
const CREATE_ASSOCIATION = { id: 'host.createAssociation', label: 'Create an Association\u2026', section: 'data' } as const;
const CREATE_DATA_SPACE = { id: 'host.createDataSpace', label: 'Create a Data Space\u2026', section: 'data' } as const;

/** What the app opens first. */
export type AppStart =
  /** The host's sample: its rows, under its own model, laid out as it says. */
  | { readonly kind: 'sample'; readonly sample: Sample }
  /** A link the page was opened from (a shared page, a saved query's link): `forget` takes it out of the address, so a
   *  reload never reopens it over what the person has done since. */
  | { readonly kind: 'link'; readonly hash: string; readonly forget: () => void }
  /** The single-user app's launch key (docs/DATACUBE_APP_PLAN_2026_10_02.md, A3), and the table it asks for. */
  | { readonly kind: 'warehouse'; readonly key: string; readonly table?: string }
  /** One frame of the host's (Python's `ll.show(df)`): a page of one grid over it, following the frame as it changes. */
  | { readonly kind: 'frame'; readonly name: string }
  /** The page another editor serves (`services.live.page`: Python's `ll.Page`), followed and reported back to it. */
  | { readonly kind: 'live' };

/** How long a change waits to settle before the page reports its document to another editor (a drag, a typed name:
 *  one report, ms). */
const REPORT_AFTER = 500;

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
        // the groups a saved cube had open: its own grid's, not a copy's (a copy starts collapsed, as a new grid)
        ...(place.tree && !start ? { tree: place.tree } : {}),
        ...(place.heldCopy ? { heldCopy: place.heldCopy } : {}),
        ...(place.label !== undefined ? { sourceLabel: place.label } : {}),
        compact: true,
        // a copy is a grid added beside others
        foldPanel: start !== undefined || place.foldPanel === true,
        // WHERE THE PLANNER RUNS, changed where it is read: the status bar's readout offers the planes; and, where the
        // page has a model home and the host writes models, Create a Class… of the grid's rows
        hostMenu: () => [
          ...planeMenu(),
          ...(createClass && rowsOf(place)?.host.planner.source ? [CREATE_CLASS, CREATE_ASSOCIATION] : []),
        ],
        onHostMenu: (item) => {
          if (item.id === CREATE_CLASS.id) createClass?.(created, place, spawned.id);
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
          void created.controller.print(view.query, 'STANDARD').then(
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
  /** The page's model now (where its classes are), once the page can make them. */
  let pageModelNow: (() => PageModelRef | undefined) | undefined;
  /** The page renamed in its name box: its name from now (once the page can be saved, a change until it is). */
  let renamePage = (name: string): void => page.setTitle(name);
  /** Told when a grid is made on the page, by its tile's id: what the page keeps of it (its file). */
  let madeGrid: ((id: string, place: GridPlace) => void) | undefined;
  /** Told of every change on the page: the tables no grid reads any more are dropped. */
  let dropUnread: (() => void) | undefined;
  /** The source picker, once the page can open sources: New ▸ Data Source… adds a grid over one; a blank page opens one. */
  let picker: ((purpose: 'add' | 'open', start?: SectionId) => Promise<GridMaker | undefined>) | undefined;
  /** New ▸ Blank Page: everything goes, for a first data source. */
  let blankPage: ((reason?: string) => void) | undefined;
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
      // THE PAGE'S MODEL: its associations and its data space, once the page has a model home
      ...(createDataSpace ? [{ label: 'Model', section: 'data' as const, submenu: [CREATE_ASSOCIATION, CREATE_DATA_SPACE] }] : []),
      // The planes, as entries rather than a control: the bar is
      // for what you watch, the menu for what you do occasionally.
      // The one you are ON is disabled rather than hidden, so the
      // menu still says where the work happens.
      ...planeMenu(),
    ],
    onHostMenu: (item) => onHostItem(item),
    // A PAGE FILE carries the page, not the classes of its project in this browser's model home: refused while it has
    // them (§15.3; a file carrying its model comes with the Studio line's export, S23)
    exportRefusal: () => (pageModelNow?.()
      ? `This page's classes are in ${pageModelNow()!.project}, in this browser: a page file cannot carry them yet. `
        + 'Save the page here, or open the project in Studio.'
      : undefined),
    refuse: (text) => window.alert(text),
    // THE HOST'S TEXT, IN THE STATUS BAR (planner progress, errors, the planner's word): the page's first grid's, at the
    // right of its status bar -- a readout belongs there, not in the bar that says what is on screen. MOVED rather than
    // copied, on each render of that bar: `status` is the same node the planner writes to.
    hostStatus: (slot) => slot.append(status),
    // THE NAME BOX renamed: the page's name from now, saved by Save (a change of the page until then)
    onRename: (name) => renamePage(name),
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
        add.addEventListener('click', () => void picker?.('open'));
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
    ...(picks || services.pages ? { onBlankPage: () => blankPage?.() } : {}),
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

  if (start.kind === 'sample') {
    const { sample } = start;
    page.addGrid(gridMaker(await sample.snapshot(), sample.configuration, sample.dimensions,
      { snapTarget: sample.snapTarget, ...(sample.heldCopy ? { heldCopy: sample.heldCopy } : {}) }));
    await page.ready();
  }

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
    // WHAT LEAVING WOULD LOSE, asked in this order: the warehouse's session, unsaved changes, the files opened
    work.add(() => sources.sessionHeld());

    // THE PAGE ON SCREEN, as a saved page sees it: the saved page it came from (a Save then saves over it), its name,
    // what a newer writer put in it (written back as it was), the definition it had when saved or first opened
    // ("changed since saved" compares to it), and what opening left out of it. And each grid's file (by the grid's
    // id): the File itself, so a page saved over the same file reopens without asking, and the handle the browser gave
    // for it, to reopen it from where it was picked.
    let library: CubeLibrary | undefined;
    let current: {
      cubeId?: string;
      name?: string;
      /** The name it was saved (or opened) under: a name given in the name box since is a change of the page. */
      savedName?: string;
      /** Fields of the saved PAGE this reader does not know, and of each of its cubes, written back as they were. */
      pageUnknown?: Readonly<Record<string, unknown>>;
      cubeUnknown?: ReadonlyMap<string, Readonly<Record<string, unknown>>>;
      /** The definition as saved, or as first opened: "changed since saved" compares to it. */
      baseline?: string;
      /** What opening left out of the saved page (a file changed, a source not opened): saving over it loses them. */
      lost?: readonly string[];
      /** Where the page's classes are (Create a Class…): its project in the model home, made with its first class. */
      model?: PageModelRef;
    } = {};
    renamePage = (name) => {
      current = { ...current, name };
      page.setTitle(name);
      onCubeView?.();
    };
    madeGrid = (id, place) => sources.madeGrid(id, place);
    dropUnread = () => sources.dropUnread();

    /**
     * What saving now would write -- always ONE kind of thing, the page (page-document.ts): one cube per grid, its
     * charts and layout -- and its definition, for "changed since saved".
     */
    const pageNow = (name: string): PageDocument | undefined => page.document(name, {
      ...(current.pageUnknown ? { page: current.pageUnknown } : {}),
      ...(current.cubeUnknown ? { cubes: current.cubeUnknown } : {}),
      ...(current.model ? { model: current.model } : {}),
    });
    const savedForm = (name: string): { content: Record<string, unknown>; definition: string } | undefined => {
      const doc = pageNow(name);
      return doc && { content: pageContent(doc), definition: pageDefinitionText(doc) };
    };

    /** The page on screen differs from what was saved (or first opened). */
    const dirty = (): boolean => {
      if (current.baseline === undefined) return false;
      if ((current.lost?.length ?? 0) > 0) return true;
      if (current.name !== current.savedName) return true;
      const now = savedForm(current.name ?? (page.suggestedName || 'page'));
      return now !== undefined && now.definition !== current.baseline;
    };
    const baseTitle = services.browser.title?.base ?? '';
    /** Why the page no longer follows its other editor (Python), when it does not: its title says so. Set, for good. */
    let notFollowed: string | undefined;
    /** Why following it failed the last time it moved (it is tried again at its next move): said as not following. */
    let followFailed: string | undefined;
    onCubeView = () => {
      const changed = dirty();
      page.setChanged(changed);
      services.browser.title?.set(page.empty
        ? baseTitle
        : `${changed ? '\u2022 ' : ''}${current.name ?? (page.suggestedName || 'page')}${notFollowed ?? followFailed ? ` (not followed: ${notFollowed ?? followFailed})` : ''} \u2013 ${baseTitle}`);
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

    // THE PAGE ANOTHER EDITOR SERVES (services.live: Python's `ll.Page`, §5): what it is now, reported once a change
    // settles, with the version it opened -- what it says of a version Python has since replaced is refused, never read
    // back as now -- and followed as Python changes it (below).
    const live = services.live;
    /** The version of the editor's page on screen, which its reports name; undefined while none is. */
    let liveShown: number | undefined;
    /** Its next report, while a change waits to settle. */
    let reporting: ReturnType<typeof setTimeout> | undefined;
    undo.push(() => clearTimeout(reporting));
    function liveChanged(): void {
      if (!live?.page || liveShown === undefined || notFollowed !== undefined) return;
      clearTimeout(reporting);
      const at = liveShown;
      reporting = setTimeout(() => {
        if (disposed || notFollowed !== undefined || liveShown !== at) return;
        const doc = pageNow(current.name ?? (page.suggestedName || 'page'));
        // a report the editor refuses (a page it replaced, an editor gone) is followed by what it says next
        if (doc) void live.page?.report(at, doc).catch(() => undefined);
      }, REPORT_AFTER);
    }

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
    async function inPlace(open: () => Promise<Opened>, what: string, replacing = false): Promise<readonly string[]> {
      // `replacing`: the page's other editor replaced what it shows -- nothing of the person's is asked about
      if (!replacing && dirty() && !window.confirm(`The page has unsaved changes. Open ${what} anyway?`)) return [];
      const newest = opens.start();
      const o = await open();
      if (!newest()) {
        await o.discard?.();
        return [];
      }
      const onScreen = place();
      page.clear();
      current = {};
      // the page has no name of its own until saved: its bar says the grid's report title (its source's name)
      page.setTitle('');
      page.addGrid(o.make);
      await page.ready();
      // another page put on screen while this one landed: that page is the one to keep a baseline of
      if (!onScreen()) return [];
      // The baseline is the page as it LANDED (normalized by its first refresh); a name given in the name box while it
      // landed stays (a change, as a rename is)
      const landed = savedForm(page.suggestedName || 'page');
      current = { ...(current.name !== undefined ? { name: current.name } : {}), ...(landed ? { baseline: landed.definition } : {}), lost: [] };
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
     * Where a saved page keeps the handle of the file a grid reads: by the page and the grid -- or, saved before a page
     * held several grids, by the page alone.
     */
    const handleKeys = (pageId: string | undefined, grid: string): string[] => (pageId === undefined ? [] : [`${pageId}#${grid}`, pageId]);

    /** A cube saved alone, before pages: a page of its one grid, the whole board. */
    const pageOf = (cube: CubeDocument): PageDocument => ({
      kind: 'datacube.page',
      version: PAGE_VERSION,
      name: cube.name,
      cubes: [{ id: 'cube', cube }],
      views: [{ id: 'grid', kind: 'grid', cube: 'cube' }],
      sheets: oneSheet({ kind: 'bands', fit: true, bands: [{ height: 1, node: { tile: 'grid' } }] }),
    });

    /**
     * OPEN A SAVED PAGE (from the store, a file someone handed over, a share link): each of its cubes through its own
     * source, in turn, then the page laid out as it was saved -- its grids under their saved ids, its charts, its
     * layout. A cube whose source was not opened is left out, and said; saving over the page would lose it.
     */
    async function openSaved(saved: SavedDocument, id: string | undefined, replacing = false): Promise<void> {
      const doc = saved.kind === 'page' ? saved.page : pageOf(saved.cube);
      // `replacing`: the page's other editor replaced it -- nothing of the person's is asked about
      if (!replacing && dirty() && !window.confirm(`The page has unsaved changes. Open "${doc.name}" anyway?`)) return;
      // LATEST WINS (P2-330), as an open in place: each source is a wait (a file asked for, a sign-in), and an open
      // overtaken meanwhile lands nothing -- what it read is dropped
      const newest = opens.start();
      const makers = new Map<string, GridMaker>();
      const read: Opened[] = [];
      const notes: string[] = [];
      sources.reserve(Object.values(doc.model?.tables ?? {}));
      try {
        for (const { id: cubeId, cube } of doc.cubes) {
          const o = await sources.reopened(cube, handleKeys(id, gridOfCube(doc, cubeId)), gridCapped(doc.cubes.length), doc.model?.tables[cubeId]);
          if (o) read.push(o);
          if (!newest()) {
            for (const r of read) await r.discard?.();
            return;
          }
          if (!o) {
            notes.push(`left out: ${cube.source.name} (not opened)`);
            continue;
          }
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
      if (makers.size === 0) {
        library?.say(`not opened: "${doc.name}" -- none of its sources was opened`, 'warn');
        return;
      }
      // the page on screen is this one from now: Save writes over it, never over the page it replaced
      const onScreen = place();
      page.restore(doc, makers);
      current = {
        ...(id !== undefined ? { cubeId: id } : {}),
        name: doc.name,
        savedName: doc.name,
        ...(doc.unknown ? { pageUnknown: doc.unknown } : {}),
        cubeUnknown: new Map(doc.cubes.filter((c) => c.cube.unknown).map((c) => [gridOfCube(doc, c.id), c.cube.unknown!])),
        ...(doc.model ? { model: doc.model } : {}),
      };
      page.setTitle(doc.name);
      await page.ready();
      // another page put on screen while this one landed: that page is the one to keep
      if (!onScreen()) return;
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
    }

    /**
     * Each grid's file, where it was picked: kept by the page and the grid (a grid kept off the board for a detached
     * chart included), so opening the page again reads it from there.
     */
    async function keepHandles(pageId: string): Promise<void> {
      for (const f of sources.filesNow()) if (f.handle) await pages?.handles?.put(`${pageId}#${f.grid}`, f.handle);
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
      const form = savedForm(name);
      if (!form) throw new Error('This page cannot be saved yet: a grid on it does not know its source.');
      const id = !asNew && current.cubeId !== undefined ? current.cubeId : crypto.randomUUID();
      const record = { id, name, content: form.content };
      if (id === current.cubeId) await store.update(id, record);
      else await store.create(record);
      await keepHandles(id);
      current = { ...current, cubeId: id, name, savedName: name, baseline: form.definition, lost: [] };
      page.setTitle(name);
      onCubeView?.();
      await persist();
      library?.sync();
    }

    if (pages) library = new CubeLibrary(windows.library, pages.store, {
      currentId: () => current.cubeId,
      dirty,
      open: async (id) => openSaved(readSaved((await pages.store.get(id)).content), id),
      openText: async (text) => openSaved(readSaved(text), undefined),
      forget: async (id) => {
        // every handle the page kept, whichever page is on screen
        await pages.handles?.removePage(id);
        if (current.cubeId === id) {
          const { cubeId: _gone, ...rest } = current;
          current = rest;
        }
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
                : src.sample ? `the example (${src.sample.rows.toLocaleString(UI_LOCALE)} rows) is generated again when it opens`
                  : `opening it reads ${src.name} again, from your computer`
          : src._type === 'savedQuery' ? `it reads the saved query “${src.name}” again, where its project (${src.query.groupId}:${src.query.artifactId}) is known`
            : src._type === 'warehouseTable' ? `whoever opens it signs in to the warehouse as themselves and needs to be granted ${src.name}; no sign-in is in the link`
              : src._type === 'remoteFile' ? `it reads ${src.url} again; no keys are in the link, a private bucket asks for them`
                : src._type === 'frame' ? `it reads the dataframe ${src.name}, which only the Python that serves it has`
                : src.sample ? 'it rebuilds its sample on its own'
                  : `whoever opens it needs ${src.name}`);
      }
      return [...said].join('; ');
    };

    // THE MENU'S SAVE AND SAVE AS: a window of their own (src/ui/save-dialog.ts) -- the name, where
    // it is kept, what is saved and what is not (the rows), and what saving over would drop
    if (pages) saveCube = (asNew) => {
      if (page.empty) {
        library?.say('There is nothing on this page to save: add a data source first.', 'warn');
        return;
      }
      void (async () => {
        const refused = page.saveRefusal();
        const offered = current.name ?? (page.suggestedName || 'page');
        const savedAt = current.cubeId !== undefined ? (await pages.store.get(current.cubeId).catch(() => undefined))?.lastUpdatedAt : undefined;
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
          ...(current.cubeId !== undefined
            ? { over: { name: current.name ?? offered, ...(savedAt !== undefined ? { savedAt } : {}) } } : {}),
          where: pages.where,
          keeps,
          leaves,
          ...(current.lost?.length
            ? { warning: `The saved “${current.name ?? 'page'}” has parts this page cannot show: ${current.lost.join('; ')}.` } : {}),
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
      const name = current.name ?? (page.suggestedName || 'page');
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
        + (current.model ? ` Its classes stay in ${current.model.project}, in this browser: the link does not carry them.` : '');
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
      const snapshot = grid.snapshot;
      // refused before the window opens, in words: a source read from no table of its own, a pivot, a lone total
      const table = storeTableOf(snapshot.source.query);
      if (!table) {
        window.alert(`Create a Class makes a class of a table's rows; ${sourceName} is not read from a table of its own.`);
        return;
      }
      let query;
      try {
        query = classLambda(snapshot);
      } catch (e) {
        window.alert(`Cannot create a class of this grid: ${e instanceof Error ? e.message : String(e)}`);
        return;
      }
      const [{ createClassOf }, home] = await Promise.all([import('../create-class.ts'), modelHome.connect()]);
      const model = await createClassOf({
        query,
        table,
        sample: sampleLambda(snapshot.source.query),
        rows,
        source,
        writer: writing.writer,
        place: writing.place,
        sourceName,
        cube,
        // the page's name, or -- never saved -- its grid's source, without its file's extension (`trades.csv` trades)
        pageName: current.name ?? ((page.suggestedName || sourceName).replace(/\.[A-Za-z0-9]+$/, '') || 'page'),
        model: current.model,
        client: home.client,
        where: home.where,
        ...(modelHome.studio ? { studio: (project: string, workspace: string) => modelHome.studio!(project, workspace) } : {}),
        persist,
      });
      if (!model) return;
      // the page's model, made with its first class or a grid's table kept: the page has changed
      current = { ...current, model };
      onCubeView?.();
    };
    pageModelNow = () => current.model;
    /**
     * THE PAGE'S MODEL ACTIONS (src/model-actions.ts, fetched the first time either is asked for): each grid of the
     * page as they read it -- its class when it is one, else its rows as a class's, or why they cannot be one.
     */
    const modelActions = async (): Promise<{ actions: typeof import('../model-actions.ts'); of: import('../model-actions.ts').ModelActionsOf } | undefined> => {
      const writing = services.engine.modelWriting;
      if (!modelHome || !writing) return undefined;
      const [actions, home] = await Promise.all([import('../model-actions.ts'), modelHome.connect()]);
      const grids = actions.pageGrids(page.grids, (id) => {
        const place = sources.place(id);
        const grid = page.grid(id);
        const rows = place ? rowsOf(place) : undefined;
        return place && grid && rows ? { snapshot: grid.snapshot, label: place.label ?? id, rows } : undefined;
      }, (id) => current.model?.classes?.[id], { classLambda, storeTableOf, sampleLambda });
      const sourceName = grids[0]?.label ?? 'page';
      return {
        actions,
        of: {
          writer: writing.writer,
          place: writing.place,
          rows: (model, runtime) => writing.rows(model, runtime),
          pageName: current.name ?? ((page.suggestedName || sourceName).replace(/\.[A-Za-z0-9]+$/, '') || 'page'),
          model: current.model,
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
    const landed = (model: PageModelRef | undefined): void => {
      if (!model) return;
      current = { ...current, model };
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
    blankPage = (reason?: string) => {
      if (dirty() && !window.confirm('The page has unsaved changes. Start a blank page anyway?')) return;
      place();
      page.clear();
      current = {};
      page.setTitle('');
      sayWhyBlank(reason);
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
        blankPage(startProblem);
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
      clearTimeout(reporting);
      liveShown = undefined;
      const shown = page.shownSheet;
      await openSaved({ kind: 'page', page: said.page }, undefined, true);
      if (page.sheets.includes(shown)) page.showSheet(shown);
      liveShown = said.version;
      // the page as it opened, said once it settles: `page.read()` has it before anything changes (restoring is a change)
      liveChanged();
      return undefined;
    }

    /** One frame in place of the page, the page named by it as Python named it (`ll.show(df, name)`): not a change. */
    async function openFrame(name: string, replacing: boolean): Promise<void> {
      await inPlace(() => sources.openedFrame(name), name, replacing);
      current = { ...current, name, savedName: name };
      page.setTitle(name);
      onCubeView?.();
    }

    // PYTHON'S START: one frame, a page of one grid over it (`ll.show(df)`); or the page Python serves (`ll.Page`)
    if (start.kind === 'frame') {
      try {
        await openFrame(start.name, false);
      } catch (e) {
        failedStart(e);
      }
    }
    if (start.kind === 'live') {
      const why = await openLive().catch((e: unknown) => (e instanceof Error ? e.message : String(e)));
      if (why !== undefined) failedStart(new Error(why));
    }

    // FOLLOWING THE OTHER EDITOR (services.live, §5): Python moves what the page shows -- the page itself (its version:
    // opened again, on the sheet shown) or a frame (its version: the grids over it query again, their views as they
    // stand; a frame whose model changed, its columns, opens again over the new model) -- one move at a time, in order.
    // A move that fails to be followed (a frame not read, a page not opened) is said, and tried again at the next move:
    // a success says nothing more of it. Told that Python no longer says (the page or the frame closed, Python gone),
    // the page keeps what it shows, says it no longer follows, and reports nothing more.
    if (live) {
      const what = live.page ? 'the page' : 'the frame';
      const stop = (why: string): void => {
        if (notFollowed !== undefined) return;
        notFollowed = why;
        clearTimeout(reporting);
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
        if (live.page && liveShown === undefined) return openLive();
        // each step below waits: the app taken down meanwhile (a notebook's cell run again) goes no further
        if (start.kind === 'frame' && sources.frameVersion(start.name) === undefined) {
          await openFrame(start.name, true);
          return undefined;
        }
        if (now.version !== undefined && liveShown !== undefined && now.version !== liveShown) return openLive();
        for (const [frame, version] of Object.entries(now.frames)) {
          if (disposed) return undefined;
          const read = sources.frameVersion(frame);
          if (read === undefined || read === version) continue;
          const again = await sources.frameAgain(frame);
          if (disposed) return undefined;
          if (again === 'changed') {
            if (live.page) return openLive();
            await openFrame(frame, true);
            continue;
          }
          for (const id of page.cubes) {
            if (disposed) return undefined;
            if (sources.frameOf(id) === frame) await madeCubes.get(id)?.state.refresh();
          }
        }
        return undefined;
      };
      let following: Promise<void> = Promise.resolve();
      undo.push(live.watch((now) => {
        following = following.then(async () => {
          if (disposed || notFollowed !== undefined) return;
          if (typeof now === 'string') return stop(now);
          failed(await followed(now));
        }).catch((e: unknown) => failed(e instanceof Error ? e.message : String(e)));
      }));
    }

    // A START THAT OPENED NOTHING (a link that failed, a key refused) leaves the blank page, saying
    // why: never an empty page without a word
    if (page.empty) blankPage(startProblem);
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
