// A PAGE ON AN ENGINE THAT RUNS ITS QUERIES (docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md, step 1): what Python's
// `ll.Page` shows -- a whole DataCube page (its sheets, grids, charts, stacks and layouts) whose grids are over the
// engine's frames, every query run by the engine, in remote-run mode as one cube is (engine-cube.ts). A tab
// (engine.ts) and a notebook's cube (widget.ts) open it the same way, each its own way to reach the engine.
//
// What the page is told, and how:
//   - the page: `page.json?page=<name>`, the page's document (page-document.ts, version 3) and its version, as Python
//     wrote it; each cube in it over a frame (`{ _type: 'frame', name }`, cube-document.ts);
//   - each frame: `cube.json?table=<name>`, its model, runtime and source, as for one cube;
//   - that something changed: the page's version (Python changed the page: it is opened again, on the sheet shown) and
//     each frame's (a frame updated: the grids over it query again), from `version.json?page=<name>` or the widget.
// And it says what it is now: its document, posted to `page.json?page=<name>&version=<n>` as it changes (a sheet
// arranged, a chart added in DataCube), for Python's `page.read()` -- with the version it opened, so what it says of a
// document Python has since replaced is refused, never read back as now. Both ways the document is the protocol's exact
// JSON (a calculated column's `12.30` stays `12.30`), and what a newer writer put in it is written back as it was.

import { CubeApp, RemoteRun, sourceColumns } from '../src/embed.ts';
import { LegendEngineExecutor } from '../../engine-client/src/engine-remote.ts';
import { openCube, type CubeDocument, type FrameSource } from '../src/cube-document.ts';
import { pageToJson, readPage, type PageDocument } from '../src/page-document.ts';
import { ExactNumber, fromJson } from '../../pure-protocol/src/index.ts';
import { PageApp, type GridMaker } from '../src/page/page-app.ts';
import { asked, type CubeConfig, type EngineLink } from './engine-cube.ts';

/** How the page reaches its engine, and the page it shows. */
export interface PageLink extends Omit<EngineLink, 'table'> {
  readonly page: string;
}

/** What the engine says the page is, as the protocol's exact JSON reads it: its document and its version (how often
 * Python changed it). */
interface PageAnswer {
  readonly version: unknown;
  readonly page: unknown;
}

/** The versions the engine says now: the page's, and each frame's by its name. */
export interface Versions {
  readonly version: number;
  readonly frames: Readonly<Record<string, number>>;
}

/** How long a change waits to settle before the page reports its document (a drag, a typed name: one report, ms). */
const REPORT_AFTER = 500;

const headers = (link: PageLink): HeadersInit => (link.authorization === undefined ? {} : { Authorization: link.authorization });

/** The page as the engine says it is now, or why it does not say. */
async function askedPage(link: PageLink): Promise<{ version: number; page: PageDocument } | string> {
  const answer = await link.fetch(`${link.baseUrl}/page.json?page=${encodeURIComponent(link.page)}`, { headers: headers(link) })
    .catch((e: unknown) => String(e));
  if (typeof answer === 'string') return `the engine did not answer: ${answer}`;
  if (!answer.ok) return `the engine did not say what to show: ${answer.status} ${await answer.text()}`;
  try {
    const said = fromJson(await answer.text()) as PageAnswer;
    const version = said.version instanceof ExactNumber && said.version.isInteger ? Number(said.version.text) : NaN;
    if (!Number.isSafeInteger(version)) return 'the engine said no version of the page';
    return { version, page: readPage(said.page) };
  } catch (e) {
    return e instanceof Error ? e.message : String(e);
  }
}

/** The versions the engine says now, or why it does not say (the page closed, the engine gone). */
export async function askedVersions(link: PageLink): Promise<Versions | string> {
  const answer = await link.fetch(`${link.baseUrl}/version.json?page=${encodeURIComponent(link.page)}`, { headers: headers(link) })
    .catch((e: unknown) => String(e));
  if (typeof answer === 'string') return 'the engine stopped';
  if (answer.status === 404) return 'the page was closed';
  if (!answer.ok) return `the engine answered ${answer.status}`;
  return (await answer.json()) as Versions;
}

/** Hand a file to the user (an export, the page's document). */
function download(name: string, mime: string, content: string | Uint8Array): void {
  const a = document.createElement('a');
  const part: BlobPart = typeof content === 'string' ? content : new Uint8Array(content);
  a.href = URL.createObjectURL(new Blob([part], { type: mime }));
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 5000);
}

/** A grid made on the page over a frame: its cube (to query again when the frame changes) and the frame's name. */
interface FrameGrid {
  readonly frame: string;
  readonly cube: CubeApp;
}

/** The engine's page in `host`, kept as the engine says it is (`follow`). */
export class EnginePage {
  readonly #link: PageLink;
  readonly #page: PageApp;
  /** The grids on the page, by their tile's id. */
  readonly #grids = new Map<string, FrameGrid>();
  #versions: Versions;
  /** Each frame as the engine said it was when the page was opened (a model that changes opens the page again). */
  #frames = new Map<string, CubeConfig>();
  #title = '';
  /** The version of the page it shows (the document opened last), which its reports name. */
  #shown = 0;
  /** What the document opened had that this page does not read -- the page's own fields, each grid's cube's -- written
   * back in its reports as it was (boot.ts keeps a saved page's the same way). */
  #unknown: { page?: Readonly<Record<string, unknown>>; cubes: Map<string, Readonly<Record<string, unknown>>> } = { cubes: new Map() };
  /** Its document's next report to the engine, while a change waits to settle. */
  #reporting: ReturnType<typeof setTimeout> | undefined;
  #disposed = false;

  private constructor(link: PageLink, page: PageApp, versions: Versions) {
    this.#link = link;
    this.#page = page;
    this.#versions = versions;
  }

  /** The page the engine says, opened in `host`; refused with the engine's reason when it says none. */
  static async open(host: HTMLElement, link: PageLink): Promise<EnginePage> {
    const doc = await askedPage(link);
    if (typeof doc === 'string') throw new Error(doc);
    let changed = (): void => {};
    const page = new PageApp({
      host,
      title: doc.page.name,
      empty: (slot) => {
        slot.textContent = 'Nothing is on this page.';
      },
      download: (name, mime, content) => download(name, mime, content),
      onChange: () => changed(),
    });
    const opened = new EnginePage(link, page, { version: doc.version, frames: {} });
    changed = () => opened.#changed();
    await opened.#show(doc.page, doc.version);
    // the frames' versions as the page opened over them: a later move is a change to follow
    const versions = await askedVersions(link);
    if (typeof versions !== 'string') opened.#versions = { version: doc.version, frames: versions.frames };
    return opened;
  }

  /** The page's name, as Python gave it. */
  get title(): string {
    return this.#title;
  }

  /** The page: its sheets, grids and charts. */
  get page(): PageApp {
    return this.#page;
  }

  /**
   * The page as the engine says it is now: Python changed the page (its version moved) -- opened again, on the sheet
   * it shows -- or a frame changed (its version moved) -- the grids over it query again (a frame whose model changed,
   * its columns, opens the page again). Undefined, or why the engine no longer says what to show.
   */
  async follow(versions: Versions): Promise<string | undefined> {
    const was = this.#versions;
    this.#versions = versions;
    if (versions.version !== was.version) {
      // a report waiting to go is of the document being replaced: it goes no more
      clearTimeout(this.#reporting);
      return this.#reopen();
    }
    for (const [frame, version] of Object.entries(versions.frames)) {
      if (was.frames[frame] === version) continue;
      const now = await asked({ ...this.#link, table: frame });
      if (typeof now === 'string') return now;
      const before = this.#frames.get(frame);
      if (!before || before.model !== now.model || JSON.stringify(before.source) !== JSON.stringify(now.source)) return this.#reopen();
      for (const grid of this.#grids.values()) if (grid.frame === frame) await grid.cube.state.refresh();
    }
    return undefined;
  }

  dispose(): void {
    this.#disposed = true;
    clearTimeout(this.#reporting);
    this.#page.dispose();
  }

  /** The page changed (in DataCube, or opened again): its document reported once the change settles. */
  #changed(): void {
    clearTimeout(this.#reporting);
    this.#reporting = setTimeout(() => void this.#report(), REPORT_AFTER);
  }

  /** Its document as it is now, to the engine (Python's `page.read()`): a page the engine no longer serves says nothing. */
  async #report(): Promise<void> {
    if (this.#disposed) return;
    const doc = this.#page.document(this.#title, this.#unknown);
    if (!doc) return;
    const at = `page=${encodeURIComponent(this.#link.page)}&version=${this.#shown}`;
    await this.#link.fetch(`${this.#link.baseUrl}/page.json?${at}`, {
      method: 'POST',
      headers: { ...headers(this.#link), 'Content-Type': 'application/json' },
      body: pageToJson(doc),
    }).catch(() => undefined);
  }

  /** The page opened again as the engine says it is now, on the sheet it shows (when that sheet is still there). */
  async #reopen(): Promise<string | undefined> {
    const doc = await askedPage(this.#link);
    if (typeof doc === 'string') return doc;
    const shown = this.#page.shownSheet;
    await this.#show(doc.page, doc.version);
    if (this.#page.sheets.includes(shown)) this.#page.showSheet(shown);
    return undefined;
  }

  /** `doc` (the page's `version`) on the page: each cube's frame asked for, its grid made over it, the page restored. */
  async #show(doc: PageDocument, version: number): Promise<void> {
    this.#title = doc.name;
    const makers = new Map<string, GridMaker>();
    const frames = new Map<string, CubeConfig>();
    const several = doc.cubes.length > 1;
    for (const { id, cube } of doc.cubes) {
      const source = cube.source;
      if (source._type !== 'frame') throw new Error(`the page's cube ${id} reads ${source.name}, not a frame this engine serves`);
      const config = frames.get(source.name) ?? await asked({ ...this.#link, table: source.name });
      if (typeof config === 'string') throw new Error(`the frame ${source.name}: ${config}`);
      frames.set(source.name, config);
      makers.set(id, await this.#maker(cube, source, config, several));
    }
    this.#frames = frames;
    this.#grids.clear();
    // each cube's fields this page does not read, by the grid it is shown in (its grid view's id, or its own)
    const gridOf = (cube: string): string => doc.views.find((v) => v.kind === 'grid' && v.cube === cube)?.id ?? cube;
    this.#unknown = {
      ...(doc.unknown ? { page: doc.unknown } : {}),
      cubes: new Map(doc.cubes.filter((c) => c.cube.unknown).map((c) => [gridOf(c.id), c.cube.unknown!])),
    };
    // what it reports from now on is of this version (restoring it is a change, reported once it settles)
    this.#shown = version;
    this.#page.restore(doc, makers);
    this.#page.setTitle(doc.name);
    await this.#page.ready();
  }

  /** How a grid is made over a frame, as the saved cube had it (its query, configuration and open groups). */
  async #maker(saved: CubeDocument, source: FrameSource, config: CubeConfig, several: boolean): Promise<GridMaker> {
    const runner = new RemoteRun(new LegendEngineExecutor({
      baseUrl: this.#link.baseUrl,
      model: config.model,
      runtime: config.runtime,
      serializationFormat: 'ARROW_IPC',
      ...(this.#link.authorization === undefined ? {} : { authorization: this.#link.authorization }),
      fetch: this.#link.fetch,
    }));
    const columns = await sourceColumns(runner, config.source);
    const opened = openCube(saved, { query: config.source }, columns);
    const cubeSource: FrameSource = { _type: 'frame', name: source.name, columns: columns.map((c) => ({ name: c.name, type: c.type })) };
    return (host, spawned, start) => {
      const cube = new CubeApp(host, start?.snapshot ?? opened.snapshot, {
        runner,
        configuration: start?.configuration ?? opened.configuration,
        ...(start ? {} : { tree: opened.tree }),
        cubeSource,
        sourceLabel: source.name,
        compact: true,
        // a grid beside others starts with its columns panel folded, as one added to a page
        foldPanel: several || start !== undefined,
        windowHost: this.#page.root,
        showColumnZone: true,
        writeClipboard: (text) => navigator.clipboard?.writeText(text),
        download: (name, mime, content) => download(name, mime, content),
        ...spawned,
      });
      if (spawned.id !== undefined) this.#grids.set(spawned.id, { frame: source.name, cube });
      return cube;
    };
  }
}
