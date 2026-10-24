// DATACUBE'S APP ON A PAGE ANOTHER EDITOR SERVES (src/app/datacube-app.ts, Python's `ll.Page`): the app as a tab opens
// it -- over a stand-in editor that says the page, its moves and the writes it takes -- following the page from its
// start: a page of no grid yet followed as Python adds to it, a first read that failed tried again at its next move (the
// S4 audit's 1: only New ▸ Blank Page leaves the page, D1), and a follow on its way when the page is left landing
// nothing (the S4 audit's 4).

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { openDataCube } from '../src/app/datacube-app.ts';
import type { LivePage, LiveRead } from '../src/app/services.ts';
import { pageNew } from '../src/headless/page.ts';
import { gridCopyOf, readPage, type PageDocument } from '../src/page-document.ts';
import { applyPageCommands, idsOf } from '../src/page-commands.ts';
import type { LiveWrite } from '../src/app/services.ts';
import { corpus } from './fixtures/pages/make.ts';
import { FakeKeeper } from './fake-keeper.ts';
import { pageHash } from '../src/page-hash.ts';
import { GateEngine, StubPlanner } from './cube-fixture.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { cubeToJson, writeCube } from '../src/cube-document.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';

/** A promise held until the test settles it. */
function held<T>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((res) => { resolve = res; });
  return { promise, resolve };
}

const settle = (): Promise<void> => new Promise((done) => { setTimeout(done, 0); });

/** Python's page before its first grid (`ll.Page('Q3')` shown): a page of no grid, as pageNew makes one. */
const EMPTY: PageDocument = readPage(JSON.parse((JSON.parse(pageNew('Q3')) as { page: string }).page), { empty: true });

/** The editor Python is, as the tab sees it (test/fake-keeper.ts: a keeper applying a write's edits by DataCube's rule,
 *  answering the hash of the page it keeps): what it is asked, and what it answers, the test's to say. */
class Editor extends FakeKeeper {
  constructor(page: PageDocument = EMPTY, held: readonly string[] = ['sheet-1']) {
    super(page, held);
  }
}

/** The page as the keeper has it, read: at version 1, with every id it holds and its hash. */
const readOf = async (page: PageDocument, held: readonly string[] = idsOf(page), version = 1): Promise<LiveRead> =>
  ({ version, page, held, hash: pageHash(page) });

let dom: JSDOM;

beforeEach(() => {
  dom = new JSDOM('<!doctype html><div id="host"></div><div id="status"></div><div id="windows"></div>', { pretendToBeVisual: true });
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => {
    cb(0);
    return 1;
  };
  g['cancelAnimationFrame'] = () => {};
});

afterEach(() => {
  dom.window.close();
});

/** The app opened on `editor`'s page, as a tab opens it: what it watches through, and its page's text. `host`: more of
 *  the host's database (its asks a test holds). */
async function opened(editor: Editor, frame?: (name: string) => Promise<unknown>, host: Record<string, unknown> = {},
  engine: Record<string, unknown> = {}):
  Promise<{ moved: (version: number) => void; unwatched: () => boolean; text: () => string; shown: () => number | undefined }> {
  const watchers: ((now: { version: number; frames: Record<string, number> } | string) => void)[] = [];
  let unwatched = false;
  let shown: () => number | undefined = () => { throw new Error('the app said no version on screen to its editor'); };
  const services = {
    engine: {
      plane: { word: 'engine', planes: [], choose() {} },
      label: 'engine',
      reading: {},
      // a frame's rows, planned and run here over the fixture's planner and engine: what opening a page's grid asks
      rows: () => ({ planner: new StubPlanner(), engine: new GateEngine() }),
      // a source to pick: the page offers New ▸ Blank Page; the host's frames, as the test says them
      database: { ingestFile: async () => { throw new Error('no file here'); }, ...(frame ? { frame } : {}), ...host },
      ...engine,
    },
    live: {
      page: editor,
      watch: (fn: (now: { version: number; frames: Record<string, number> } | string) => void, onScreen?: () => number | undefined) => {
        watchers.push(fn);
        if (onScreen) shown = onScreen;
        return () => { unwatched = true; };
      },
    },
    browser: {
      download() {}, writeClipboard: async () => {}, readSettings: () => undefined, writeSettings() {},
      title: { base: '', set() {} },
    },
  };
  const doc = dom.window.document;
  await openDataCube({
    services: services as never,
    host: doc.getElementById('host')!,
    status: doc.getElementById('status')!,
    windows: doc.getElementById('windows')!,
    start: { kind: 'live' },
    taken: [],
    settled: { show() {} },
  } as never);
  return {
    moved: (version) => { for (const w of watchers) w({ version, frames: {} }); },
    unwatched: () => unwatched,
    text: () => (doc.body.textContent ?? '').replace(/\s+/g, ' '),
    shown: () => shown(),
  };
}

/** New ▸ Blank Page, from the page's menu. */
function blankPage(): void {
  const doc = dom.window.document;
  (doc.querySelector('.dc-page-bar .dc-titlebar-menu') as HTMLElement).click();
  const item = (label: string): HTMLElement => {
    const items = [...doc.querySelectorAll<HTMLElement>('.dc-menu [role="menuitem"]')];
    const found = items.find((i) => i.querySelector('.dc-menu-label')?.textContent === label);
    assert.ok(found, `no entry '${label}': ${items.map((i) => i.textContent).join(' | ')}`);
    return found;
  };
  const opener = item('Create');
  opener.dispatchEvent(new dom.window.MouseEvent('mouseenter', { bubbles: true }));
  opener.click();
  item('Blank Page').click();
}

describe("the app on a page Python serves, from its start (the S4 audit's 1)", () => {
  it('a page of no grid yet is followed as Python moves it: only New ▸ Blank Page leaves it', async () => {
    const editor = new Editor();
    const app = await opened(editor);
    assert.equal(app.unwatched(), false, 'still watched: a start that opened nothing leaves nothing');
    assert.doesNotMatch(app.text(), /a page of this tab's own/);
    app.moved(2);
    await settle();
    assert.equal(editor.reads, 2, "its move followed: the keeper's page read (the middle version)");
  });

  it("a page of no grid yet is shown as its keeper has it: its name, its sheets, the ids it holds (the S7 audit's H-1)", async () => {
    // Python's ll.Page('Q3') with a second sheet, before its first grid
    const page = applyPageCommands({ page: EMPTY, held: idsOf(EMPTY) }, [
      { kind: 'addSheet', id: 'sheet-2', name: 'Second' },
    ]).draft.page;
    const editor = new Editor(page, idsOf(page));
    const written: LiveWrite[] = [];
    const settled = editor.report.bind(editor);
    editor.report = async (at, write) => {
      written.push(write);
      return settled(at, write);
    };
    await opened(editor);
    await settle();
    const doc = dom.window.document;
    const tabs = [...doc.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((t) => t.textContent);
    assert.deepEqual(tabs.slice(-1), ['Second'], 'its sheets as its keeper has them');
    assert.equal(tabs.length, 2);
    assert.match(doc.querySelector('.dc-page-name')?.textContent ?? '', /Q3/, 'its name');
    // a sheet the person adds is given an id the keeper has not held
    (doc.querySelector('.dc-sheet-add') as HTMLElement).click();
    await new Promise((done) => { setTimeout(done, 700); });
    const added = written.flatMap((w) => ('edits' in w ? w.edits : [])).flatMap((e) => ('page' in e && e.page.kind === 'addSheet' ? [e.page] : []));
    assert.equal(added.length, 1, 'the sheet added, written to its keeper');
    assert.match(added[0]!.id ?? '', /^sheet-t[a-z0-9]{5}-3$/, "never an id its keeper held: after them, under this tab's tag");
  });

  it("the empty page's Add a data source adds to the page Python serves, as New ▸ Data Source… does (the S7 confirmation audit's H2-6)", async () => {
    const editor = new Editor();
    const app = await opened(editor);
    await settle();
    const doc = dom.window.document;
    const add = [...doc.querySelectorAll<HTMLButtonElement>('.dc-blank-actions button')].find((b) => b.textContent === 'Add a data source');
    assert.ok(add, 'the card offers a data source');
    add.click();
    await settle();
    assert.equal(doc.querySelector('.dc-picker-title')?.textContent, 'Add a source', 'added to the page, not opened in place of it');
    assert.equal(app.unwatched(), false, 'the page Python serves is still the one on screen');
  });

  it("a class added from the picker's Classes is made by the host and opened there, as page.grid(cls=...) makes one (K16, K17)", async () => {
    const asked: unknown[][] = [];
    const cube = cubeToJson(writeCube({
      name: 'Q3', source: { _type: 'class', name: 'Trade', class: 'p::domain::Trade', project: 'p:q', workspace: 'datacube',
        revision: 'r1', mapping: 'p::mapping::Model', runtime: 'p::runtime::Python', columns: [{ name: 'desk', type: 'String' }] },
      snapshot: { source: { query: { _type: 'var', name: 'x' } }, columns: [{ name: 'desk', type: 'String' }], derived: [],
        rows: [], pivotOn: [], sorts: [], epoch: 0 } as unknown as CubeSnapshot,
      configuration: DEFAULT_CONFIGURATION, tree: TreeState.empty(),
    }));
    await opened(new Editor(), undefined, {
      made: async (...args: unknown[]) => { asked.push(['made', ...args]); return cube; },
      opened: async (...args: unknown[]) => { asked.push(['opened', ...args]); throw new Error('the host read no rows'); },
    });
    await settle();
    const doc = dom.window.document;
    [...doc.querySelectorAll<HTMLButtonElement>('.dc-blank-actions button')].find((b) => b.textContent === 'Add a data source')!.click();
    await settle();
    (doc.querySelector('[data-section="classes"]') as HTMLElement).click();
    const [cls, project, revision] = [...doc.querySelectorAll<HTMLInputElement>('.dc-picker-panel .dc-picker-field input')];
    cls!.value = ' p::domain::Trade ';
    project!.value = 'p:q';
    revision!.value = 'r1';
    (doc.querySelector('.dc-picker-panel form') as HTMLFormElement).requestSubmit();
    for (let i = 0; i < 5; i += 1) await settle();
    // made by the host -- the page's model (none yet), the class as class-source.ts classGiven takes it -- then opened as
    // every grid of the page Python serves is
    assert.deepEqual(asked[0], ['made', null, '{"class":"p::domain::Trade","project":"p:q","revision":"r1"}', 'Q3', 1]);
    assert.deepEqual(asked[1]!.slice(0, 1), ['opened']);
    assert.equal(JSON.parse(asked[1]![1] as string).source.class, 'p::domain::Trade');
    assert.equal(doc.querySelector('.dc-picker-status')?.textContent, 'the host read no rows', 'its refusal said in the window');
  });

  it("a grid made a class on Python's page adds a grid over its class from its right-click's Create ▸, made by the host and opened there (K16)", async () => {
    const editor = new Editor();
    const trades = (await corpus())['trades.page.json']!;
    const grid = trades.views.find((v) => v.kind === 'grid')!;
    const model = { project: 'p:q', workspace: 'datacube', package: 'p', tables: {}, revision: 'r1', classes: { [grid.cube]: 'p::domain::Trade' } };
    const page: PageDocument = { ...trades, model };
    editor.page = page;
    editor.held = idsOf(page);
    const asked: unknown[][] = [];
    const alerts: string[] = [];
    dom.window.alert = (m?: unknown) => { alerts.push(String(m)); };
    await opened(editor, async () => ({ version: 0, model: '', runtime: '', source: { _type: 'packageableElementPtr', fullPath: 'trades' } }), {
      made: async (...args: unknown[]) => { asked.push(['made', ...args]); return 'not read: the host is asked to open it next'; },
      opened: async (...args: unknown[]) => { asked.push(['opened', ...args]); throw new Error('the host read no rows'); },
    });
    for (let i = 0; i < 5; i += 1) await settle();
    const doc = dom.window.document;
    // the grid's right-click: Create ▸ Grid over Trade, first of the model's entries (the user, 2026-10-23)
    const cell = doc.querySelector<HTMLElement>('[data-tile] .dc-row .dc-cell');
    assert.ok(cell, "the grid's rows");
    cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    const item = [...doc.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')]
      .find((i) => i.querySelector('.dc-menu-label')?.textContent === 'Grid over Trade'
        && i.parentElement?.closest('.dc-menu-item')?.querySelector(':scope > .dc-menu-label')?.textContent === 'Create');
    assert.ok(item, `offered on Python's page: ${[...doc.querySelectorAll('.dc-menu [role="menuitem"]')].map((i) => i.textContent).join(' | ')}`);
    // its Join ▸, on a host's page that makes no joins: there all the same, greyed, saying why (the user, 2026-10-24)
    assert.deepEqual([...doc.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')]
      .filter((i) => i.parentElement?.closest('.dc-menu-item')?.querySelector(':scope > .dc-menu-label')?.textContent === 'Join')
      .map((i) => [i.querySelector('.dc-menu-label')?.textContent, i.getAttribute('aria-disabled'), i.title]),
    [['Join…', 'true', "This page's host makes no joins"]]);
    item.click();
    for (let i = 0; i < 5; i += 1) await settle();
    // made by the host -- the page's model, the class as class-source.ts classGiven takes it, beside the grid's one other
    assert.deepEqual(asked[0]?.slice(0, 1), ['made']);
    assert.deepEqual(JSON.parse(asked[0]![1] as string).classes, model.classes);
    assert.equal(asked[0]![2], '{"class":"p::domain::Trade"}');
    assert.equal(asked[0]![4], 2, 'a grid beside another: capped');
  });

  // the user, 2026-10-24: a grid's Join ▸ is always there -- what it can join, or greyed saying why
  describe("a grid's right-click Join ▸, on every page", () => {
    /** The page's one grid right-clicked: its Join ▸'s entries, each its label, whether greyed, and its hint. */
    async function joinMenuOf(host: Record<string, unknown>, engine: Record<string, unknown> = {}): Promise<(string | null | undefined)[][]> {
      // the keeper holds the page (test/fake-keeper.ts): a read made to say another would leave the tab converging on it
      const trades = (await corpus())['trades.page.json']!;
      const editor = new Editor(trades, idsOf(trades));
      await opened(editor, async () => ({ version: 0, model: '', runtime: '', source: { _type: 'packageableElementPtr', fullPath: 'trades' } }), host, engine);
      for (let i = 0; i < 5; i += 1) await settle();
      const doc = dom.window.document;
      const cell = doc.querySelector<HTMLElement>('[data-tile] .dc-row .dc-cell');
      assert.ok(cell, "the grid's rows");
      cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
      return [...doc.querySelectorAll<HTMLElement>('.dc-menu [role^="menuitem"]')]
        .filter((i) => i.parentElement?.closest('.dc-menu-item')?.querySelector(':scope > .dc-menu-label')?.textContent === 'Join')
        .map((i) => [i.querySelector('.dc-menu-label')?.textContent, i.getAttribute('aria-disabled'), i.title]);
    }

    it("a host that makes joins: its window, and a data source's, to join to", async () => {
      const no = async (): Promise<never> => { throw new Error('not asked here'); };
      const items = await joinMenuOf({ join: { sides: no, questions: no, cube: no } });
      assert.deepEqual(items.map(([label, disabled]) => [label, disabled]),
        [['No other grid has a column region', 'true'], ['Join…', null], ['Join a Data Source…', null]]);
    });

    it("a grid whose rows are no table the page's own database holds: greyed, saying so", async () => {
      assert.deepEqual(await joinMenuOf({}, { local: { planner: new StubPlanner(), engine: new GateEngine() } }),
        [['Join…', 'true', "A join reads tables this page holds, and this grid's rows are none of them"]]);
    });
  });

  it('the editor is told, as it is asked, the version on screen: the copies it reads stay until it moves past them (Z17)', async () => {
    const editor = new Editor();
    const app = await opened(editor);
    assert.equal(app.shown(), 1, 'opened at version 1');
    editor.version = 2;
    app.moved(2);
    await settle();
    assert.equal(app.shown(), 2, 'followed to version 2');
    blankPage();
    assert.equal(app.shown(), undefined, 'left: no page of the editor\'s on screen');
  });

  it('a first read that failed is tried again at the next move', async () => {
    const editor = new Editor();
    const read = editor.read;
    editor.read = async () => {
      editor.reads += 1;
      throw new Error('the engine is not answering yet');
    };
    const app = await opened(editor);
    assert.equal(editor.reads, 1);
    assert.equal(app.unwatched(), false);
    editor.read = read;
    app.moved(1);
    await settle();
    assert.equal(editor.reads, 2, 'read again');
  });
});

describe("New ▸ Blank Page while a follow is on its way (the S4 audit's 4)", () => {
  it('its page, answered once the page is left, lands nothing: the page is the tab\'s own, said', async () => {
    const editor = new Editor();
    const app = await opened(editor);
    const asked = held<Awaited<ReturnType<LivePage['read']>>>();
    let asks = 0;
    editor.read = () => {
      asks += 1;
      return asked.promise;
    };
    app.moved(2);
    await settle();
    assert.equal(asks, 1, "the keeper's page asked");
    blankPage();
    assert.equal(app.unwatched(), true, 'left: watched no more');
    // Python added a sheet: its page is Python's, never the tab's own
    const added = { ...EMPTY, sheets: [...EMPTY.sheets, { id: 'sheet-2', name: 'Added in Python', layout: EMPTY.sheets[0]!.layout }] } as PageDocument;
    asked.resolve(await readOf(added, ['sheet-1', 'sheet-2'], 2));
    await settle();
    await settle();
    assert.match(app.text(), /a page of this tab's own: the page Python serves is left as it was/);
    assert.doesNotMatch(app.text(), /not following the page|Added in Python/);
    assert.equal(asks, 1, 'the page Python serves never read again over the tab\'s own');
  });

  it('the page Python serves, read once the page is left, is not opened over the tab\'s own', async () => {
    const editor = new Editor();
    const app = await opened(editor);
    // Python's move: its page is read, and the read is on its way
    const reading = held<Awaited<ReturnType<LivePage['read']>>>();
    editor.read = () => {
      editor.reads += 1;
      return reading.promise;
    };
    app.moved(2);
    await settle();
    assert.equal(editor.reads, 2, 'read again');
    blankPage();
    // the page as Python has it now: a grid over a frame this tab cannot open -- opening it would say so
    const trades = (await corpus())['trades.page.json']!;
    reading.resolve(await readOf(trades, [], 2));
    await settle();
    await settle();
    assert.match(app.text(), /a page of this tab's own: the page Python serves is left as it was/);
    assert.doesNotMatch(app.text(), /not following the page/);
  });

  it('the page Python serves, opening its grids once the page is left, lands nothing over the tab\'s own', async () => {
    const editor = new Editor();
    const framed = held<unknown>();
    const app = await opened(editor, () => framed.promise);
    // Python's move says its page, a grid over a frame: opening it waits for the frame
    const trades = (await corpus())['trades.page.json']!;
    editor.read = async () => {
      editor.reads += 1;
      return readOf(trades, [], 2);
    };
    app.moved(2);
    await settle();
    blankPage();
    framed.resolve({ version: 0, model: '', runtime: '', source: { _type: 'packageableElementPtr', fullPath: 'trades' } });
    await settle();
    await settle();
    assert.doesNotMatch(app.text(), /Positions/, "Python's page never put over the tab's own");
    assert.match(app.text(), /a page of this tab's own: the page Python serves is left as it was/);
  });
});

describe("a snapped grid whose copy is not read here (the S7 audit's G-3)", () => {
  it("is told live once the page is open, and rebuilt when what it reads changed, as Python's Grid.live tells both (the S7 confirmation audit's H2-4)", async () => {
    const editor = new Editor();
    const trades = (await corpus())['trades.page.json']!;
    const grid = trades.views.find((v) => v.kind === 'grid')!;
    const copy = gridCopyOf({ table: 'snap_1', rowCount: 9, takenAt: new Date('2026-10-20T14:02:00Z'), label: 'Snap 14:02' });
    const page: PageDocument = { ...trades, copies: { [grid.cube]: copy } };
    editor.page = page;
    editor.held = idsOf(page);
    const written: LiveWrite[] = [];
    const settled = editor.report.bind(editor);
    editor.report = async (at, write) => {
      written.push(write);
      return settled(at, write);
    };
    await opened(editor, async () => ({ version: 0, model: '', runtime: '', source: { _type: 'packageableElementPtr', fullPath: 'trades' } }));
    await settle();
    await new Promise((done) => { setTimeout(done, 700); });
    const told = written.flatMap((w) => ('edits' in w ? w.edits : [])).flatMap((e) => ('page' in e ? [e.page] : []));
    assert.deepEqual(told.filter((c) => c.kind === 'live'), [{ kind: 'live', grid: grid.id }], 'its keeper told it reads live');
    // the frame it reads opened again as it is now (the fixture's): its rebuilt told too, after its live
    const at = (kind: string, cube: string): number => told.findIndex((c) => c.kind === kind && (c.kind === 'live' ? c.grid === grid.id : 'cube' in c && c.cube === cube));
    assert.ok(at('rebuilt', grid.cube) > at('live', grid.cube), `rebuilt after its live (${JSON.stringify(told.map((c) => c.kind))})`);
  });
});

describe("the ids the page has held, read with it (B12: the S4 audit's 5)", () => {
  it('none is given out by a command made here', async () => {
    const editor = new Editor();
    const trades = (await corpus())['trades.page.json']!;
    // the page as Python's keeper has it: every id it has held, sheet-9 among them (made and taken off in Python)
    editor.page = trades;
    editor.held = ['chart', 'chart-2', 'grid', 'grid-2', 'sheet-1', 'sheet-2', 'sheet-9'];
    const written: unknown[] = [];
    const settled = editor.report.bind(editor);
    editor.report = async (at, write) => {
      written.push(write);
      return settled(at, write);
    };
    await opened(editor, async () => ({ version: 0, model: '', runtime: '', source: { _type: 'packageableElementPtr', fullPath: 'trades' } }));
    await settle();
    (dom.window.document.querySelector('.dc-sheet-add') as HTMLElement).click();
    // written once the change settles (live-channel.ts WRITE_AFTER)
    await new Promise((done) => { setTimeout(done, 700); });
    const added = written.flatMap((w) => ((w as { edits?: { page?: { kind: string; id?: string } }[] }).edits ?? []))
      .flatMap((e) => (e.page?.kind === 'addSheet' ? [e.page.id] : []));
    assert.equal(added.length, 1);
    assert.match(added[0] ?? '', /^sheet-t[a-z0-9]{5}-10$/, "past every id the page has held, under this tab's tag: sheet-3 would be one Python holds as sheet-9 is not");
  });
});

