// THE TAB'S SIDE OF THE PAGE KEEPER (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): a page another editor serves (Python's
// `ll.Page`), open here. One copy of the page is kept by the host; this screen writes it against the version it is at,
// with the edits it made there, each a command (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4) -- the host applies them
// by DataCube's one rule to the page as it has it and keeps what they make (made again on top of what moved the page
// meanwhile: the screen then opens the page as the host made it) -- and follows the other editor's moves. An undo or a
// redo is the one write of the page whole: a restore, kept only at the version it was made at (N2). Writes and follows
// run one at a time, in order: a move told while a write is on its way is followed once the write has said its
// version, never taken for the other editor's.

import type { PageDocument } from '../page-document.ts';
import type { LiveCommand, LivePage, LiveWrite } from './services.ts';

/** How long a change waits to settle before it is written (a drag, a typed name: one write, ms). */
export const WRITE_AFTER = 500;

/** The screen a channel writes from and opens into. */
export interface LiveScreen {
  /** The page as the screen holds it now -- a page of no grid too -- or undefined: nothing it can say yet. */
  page(): PageDocument | undefined;
  /** The page as the host has it, opened again whole: a write the host made again on top, which the screen then opens
   *  as the host made it -- or one it refused (`refused`, its words), what the screen made then not the page's. */
  reopen(refused?: string): Promise<void>;
}

export class LiveChannel {
  /** The version of the page on screen, which its writes name: undefined while none is (opening it, following it). */
  shown: number | undefined;
  /** The edits made on screen since it was last written, as applied there. */
  #edits: LiveCommand[] = [];
  /** The page on screen put back whole since (an undo, a redo): written whole, as a restore (N2). */
  #restore = false;
  /** How deep in following the other editor: what the screen does meanwhile is that editor's, not its own. */
  #following = 0;
  #queue: Promise<void> = Promise.resolve();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stopped = false;
  readonly #editor: LivePage;
  readonly #screen: LiveScreen;
  readonly #after: number;

  constructor(editor: LivePage, screen: LiveScreen, after = WRITE_AFTER) {
    this.#editor = editor;
    this.#screen = screen;
    this.#after = after;
  }

  /** An edit made on screen (a grid's command, a page's), as applied there: written with the next write. While following
   *  the other editor, a page's command told is that editor's (the page put at its commands); a grid's is the person's
   *  own -- a grid tells none of another editor's (CubeApp.applyGridCommands) -- and is kept (the S1 audit's 2). */
  edit(command: LiveCommand): void {
    if (this.#following > 0 && 'page' in command) return;
    this.#edits.push(command);
  }

  /** The page on screen put back whole by an undo or a redo (the page's, a grid's): the next write is the page whole,
   *  a restore (N2) -- the edits made before it are in it. Not while following the other editor. */
  restored(): void {
    if (this.#following === 0) this.#restore = true;
  }

  /** The screen changed: written once it settles. */
  changed(): void {
    if (this.shown === undefined || this.#stopped) return;
    clearTimeout(this.#timer);
    this.#timer = setTimeout(() => { void this.enqueue(() => this.#write()); }, this.#after);
  }

  /** The other editor followed by `work`: nothing the screen does meanwhile is its own. */
  async following<T>(work: () => Promise<T>): Promise<T> {
    this.#following += 1;
    try {
      return await work();
    } finally {
      this.#following -= 1;
    }
  }

  /** The page opened whole at `version` (as it starts, or opened again): nothing of the screen's own pending. */
  opened(version: number): void {
    this.shown = version;
    this.#edits = [];
    this.#restore = false;
  }

  /** Work in the channel's one queue, after every write and follow before it; its failure is its caller's. */
  enqueue(work: () => Promise<void>): Promise<void> {
    const run = this.#queue.then(work);
    this.#queue = run.catch(() => undefined);
    return run;
  }

  /** Nothing more written (the other editor gone, the app taken down). */
  stop(): void {
    this.#stopped = true;
    clearTimeout(this.#timer);
  }

  /** Every write and follow queued so far, done. */
  idle(): Promise<void> {
    return this.#queue;
  }

  async #write(): Promise<void> {
    const at = this.shown;
    if (at === undefined || this.#stopped) return;
    // nothing made on screen since it was last written or opened: nothing to write
    if (!this.#restore && this.#edits.length === 0) return;
    let write: LiveWrite;
    if (this.#restore) {
      const page = this.#screen.page();
      if (page === undefined) return;
      write = { restore: page };
    } else {
      write = { edits: this.#edits };
    }
    this.#edits = [];
    this.#restore = false;
    let said: { readonly version: number; readonly combined: boolean };
    try {
      said = await this.#editor.report(at, write);
    } catch (e) {
      // refused -- an edit that does not apply where the page now is, a restore made against a version it moved on from
      // -- or the editor gone: what was made here is not the page; the page is opened as its keeper has it, said
      if (this.#stopped || this.shown !== at) return;
      this.shown = undefined;
      await this.#screen.reopen(e instanceof Error ? e.message : String(e));
      return;
    }
    if (this.shown !== at || this.#stopped) return;
    if (said.combined) {
      // the keeper made another page than the screen's (made again on top of what moved it meanwhile, an id given out
      // afresh): the page as the keeper made it -- what was made here while the write was on its way, made on the
      // screen's page and not the keeper's, is not kept, and is said (the S4 audit's 7)
      const dropped = this.#edits.length > 0 || this.#restore;
      this.shown = undefined;
      await this.#screen.reopen(dropped ? 'a change made while the page was written was made on a page its keeper no longer has' : undefined);
      return;
    }
    this.shown = said.version;
  }
}
