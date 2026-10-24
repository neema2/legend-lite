// THE TAB'S SIDE OF THE PAGE KEEPER (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md; docs/DATACUBE_LIVE_EDITING_2026_10_24.md
// §2; docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version): a page another editor serves (Python's
// `ll.Page`), open here. One copy of the page is kept by the host; this screen writes it against the version it is at,
// with the edits it made there, each a command (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4) -- an undo or a redo the
// commands between (W5) -- and the host applies them by DataCube's one rule to the page as it has it. Every write has an
// id (this channel's, and its number): the keeper applies it once. The screen holds CONFIRMED, the page as the keeper has
// it at the version shown, and PENDING, the edits made since the last write was sent. A write answered at its version
// moves `shown` and nothing on screen. Every other answer -- the keeper made another page of the write (it had moved on
// meanwhile), refused it in words, or its answer never came and the keeper took it, or did not -- and every follow end
// the same way: the screen is put at the keeper's page, which the answer or a read carries (live-converge.ts), the
// keeper's hash checked (W17: a mismatch is the one message that must never show, and opens the page again), and the
// pending edits made again, so they are told and written next. A follow writes what is pending first (`flush`) and waits
// for the screen to be quiet (F90). Writes and follows run one at a time, in order.

import type { PageDocument } from '../page-document.ts';
import { pageHash } from '../page-hash.ts';
import { LiveRefused, type LiveCommand, type LivePage, type LiveRead, type LiveSettled, type LiveWrite } from './services.ts';

/** How long a change waits to settle before it is written (a drag, a typed name: one write, ms). */
export const WRITE_AFTER = 500;
/** How long a write waits for a grid's change in flight before looking again (ms). */
const QUIET_AFTER = 25;
/** How long a flush waits for the screen to be quiet at most (ms): a query that never answers does not hold a follow
 *  for ever. */
const QUIET_AT_MOST = 10_000;
/** How many writes in a row may go unanswered, each asked of the keeper and its edits made again, before the page is
 *  opened again, said: a keeper that never answers a write (and answers its reads) is never written to for ever,
 *  unsaid (the middle version's audit's 3). */
const UNANSWERED_AT_MOST = 3;

/** The message that must never show (W17): the two editors held two pages. */
export const HASH_MISMATCH = 'the page here differed from its keeper\'s: opened again as its keeper has it';

/** What the screen is put at: the keeper's page as it is now (`target`) with every id it has held; the page this screen
 *  last knew the keeper to hold, with what it sent (`base`: a grid whose cube there is the one on `target` was changed by
 *  no other editor, and keeps its history -- W13); the batch the keeper refused, if any (its grids' history starts
 *  again, its page steps come off the page's history); and the edits made here meanwhile, not yet written (pending),
 *  made again after the screen is at `target`. */
export interface Converge {
  readonly target: PageDocument;
  readonly held: readonly string[];
  readonly base: PageDocument;
  readonly refused: readonly LiveCommand[];
  readonly pending: readonly LiveCommand[];
}

/** The screen at the keeper's page: the page on screen then, before any pending edit is made again -- the keeper's
 *  page -- and the grids whose history started again (another editor changed them, the refused batch touched them, or
 *  they were made again: their pending edits are new steps when made again; every other grid's are no step, their steps
 *  being in its history still). */
export interface Converged {
  readonly page: PageDocument;
  readonly restarted: ReadonlySet<string>;
}

/** The screen a channel writes from and converges. */
export interface LiveScreen {
  /** The page as the screen holds it now -- a page of no grid too -- or undefined: nothing it can say yet. */
  page(): PageDocument | undefined;
  /** A grid's change is in flight (its query not answered yet): a write waits for it, so a gesture's commands are written
   *  together once they have all landed (F97), and a follow meets no gesture still running (F90). */
  busy(): boolean;
  /** The screen put at the keeper's page (live-converge.ts), inside `following`: throws when it cannot, and the page is
   *  then opened again. */
  converge(on: Converge): Promise<Converged>;
  /** Pending edits made again, through the ordinary edit path, so each is told again and written next; one that no
   *  longer applies is dropped and said (H-11). A grid's is a new step of its history only where that history started
   *  again (`restarted`); elsewhere its step is in it still (CubeApp.edit `again`). */
  reapply(pending: readonly LiveCommand[], restarted: ReadonlySet<string>): Promise<void>;
  /** The page as its keeper has it, opened again whole, and why, said: a `rebuilt`, the keeper out of reach, a screen
   *  that could not be put at the keeper's page, or the hash mismatch. */
  reopen(why: string): Promise<void>;
  /** Said in the status line: a write refused (its words), pending edits dropped. */
  say(text: string): void;
}

/** A writer's id: twelve letters and digits, at random -- one per channel, so its writes are its own at the keeper. */
function writerId(): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = new Uint8Array(12);
  globalThis.crypto.getRandomValues(bytes);
  return [...bytes].map((b) => alphabet[b % alphabet.length]).join('');
}

export class LiveChannel {
  /** The version of the page on screen, which its writes name: undefined while none is (opening it, following it,
   *  converging). */
  shown: number | undefined;
  /** The page as the keeper has it at `shown`, and the ids it held then: what the screen converges from. */
  #confirmed: { readonly page: PageDocument; readonly held: readonly string[] } | undefined;
  /** The edits made on screen since it was last written, as applied there (pending). */
  #edits: LiveCommand[] = [];
  /** How deep in following the other editor: what the screen does meanwhile is that editor's, not its own. */
  #following = 0;
  #queue: Promise<void> = Promise.resolve();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stopped = false;
  /** A hash mismatch was met and the page opened again: a second, before a write hashes alike, is said once more and
   *  opens nothing (the page as the keeper has it is on screen already: no reopen loop). */
  #mismatched = false;
  /** This channel's writer id, and the number of its last write (each write's id: the keeper applies it once). */
  readonly #writer = writerId();
  #seq = 0;
  /** The writes in a row whose answer never came (UNANSWERED_AT_MOST). */
  #unanswered = 0;
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

  /** The page opened whole at `version` (as it starts, or opened again), as its keeper has it (`page`, `held`: the
   *  screen's own page when not given): nothing of the screen's own pending. */
  opened(version: number, page?: PageDocument, held: readonly string[] = []): void {
    this.shown = version;
    this.#edits = [];
    // a page opened afresh: the writes before it that went unanswered count no more (the confirmation audit's 3)
    this.#unanswered = 0;
    const confirmed = page ?? this.#screen.page();
    this.#confirmed = confirmed === undefined ? undefined : { page: confirmed, held };
  }

  /** The page as the keeper has it at `shown`, as the screen knows it (undefined: none opened yet). */
  get confirmed(): PageDocument | undefined {
    return this.#confirmed?.page;
  }

  /** The edits made on screen that are not written yet. */
  get pending(): readonly LiveCommand[] {
    return this.#edits;
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

  /** What is pending written now, the screen waited for to be quiet first (a gesture in flight lands and is told), and
   *  the write awaited -- converged when the keeper made another page of it. Before a follow (F90) and a Save (H-5).
   *  `inQueue`: the caller runs in the channel's queue already (a follow): written there and then, never queued behind
   *  itself. */
  flush(inQueue = false): Promise<void> {
    clearTimeout(this.#timer);
    const work = async (): Promise<void> => {
      await this.#quiet();
      await this.#write(true);
    };
    return inQueue ? work() : this.enqueue(work);
  }

  /** The screen quiet: no grid's change in flight, waited for up to QUIET_AT_MOST. */
  async #quiet(): Promise<void> {
    const until = Date.now() + QUIET_AT_MOST;
    while (this.#screen.busy() && Date.now() < until && !this.#stopped) await new Promise((r) => setTimeout(r, QUIET_AFTER));
  }

  async #write(flushing = false): Promise<void> {
    const at = this.shown;
    if (at === undefined || this.#stopped) return;
    // nothing made on screen since it was last written or opened: nothing to write
    if (this.#edits.length === 0) return;
    // a gesture in flight (a grid's query not answered yet): its commands are written together once they have landed
    // (F97) -- the write waits for it, unless flushing, which waited already
    if (!flushing && this.#screen.busy()) {
      this.changed();
      return;
    }
    this.#seq += 1;
    const write: LiveWrite = { edits: this.#edits, writer: this.#writer, seq: this.#seq };
    this.#edits = [];
    const sentPage = this.#screen.page();
    let said: LiveSettled;
    try {
      said = await this.#editor.report(at, write);
    } catch (e) {
      if (this.#stopped || this.shown !== at) return;
      const why = e instanceof Error ? e.message : String(e);
      // refused, in the keeper's words: nothing of it kept, said, and the screen put at the keeper's page; any other
      // failure is no refusal -- the answer never came, and the keeper is asked whether it took the write
      if (e instanceof LiveRefused) {
        this.#unanswered = 0;
        await this.#refused(why, write.edits);
      } else {
        await this.#unansweredWrite(why, write, sentPage);
      }
      return;
    }
    this.#unanswered = 0;
    if (this.shown !== at || this.#stopped) return;
    if (said.page === undefined) {
      // at its version: the page as it was sent is the keeper's
      this.shown = said.version;
      const held = this.#confirmed?.held ?? [];
      // the page as sent is the keeper's; none to be had (a grid that cannot say its source): nothing to converge from
      // until the next is, so a converge meanwhile opens the page again, said (the D19 audit's second round, 7)
      this.#confirmed = sentPage === undefined ? undefined : { page: sentPage, held: grown(held, makerIds(write.edits)) };
      await this.#checked(sentPage, said.hash);
      return;
    }
    // the keeper made another page of it (another editor's write landed first): the screen put at that page
    await this.#converge(said.version, said.hash, { target: said.page, held: said.held ?? [], refused: [] }, sentPage);
  }

  /**
   * THE KEEPER'S PAGE FOLLOWED (datacube-app.ts followLive, after its flush): the screen put at it, as at any answer --
   * an edit made here meanwhile kept, made again after, the hash checked (W17, a mismatch said once) -- so a follow never
   * drops an edit unsaid (the audit's 2). Nothing moved since the version shown (a poll stale by the time its work ran):
   * nothing to do. False when the page was opened again instead (said).
   */
  follow(now: LiveRead): Promise<boolean> {
    if (this.#stopped) return Promise.resolve(false);
    if (now.version === this.shown) return Promise.resolve(true);
    return this.#converge(now.version, now.hash, { target: now.page, held: now.held, refused: [] });
  }

  /** A write refused: said, nothing of it made again, and the screen put at the keeper's page as it is now (W8). */
  async #refused(why: string, refused: readonly LiveCommand[]): Promise<void> {
    this.#screen.say(`not kept: ${why} -- the page is as its keeper has it`);
    let now: LiveRead;
    try {
      now = await this.#editor.read(this.#writer);
    } catch (e) {
      await this.#reopen(e instanceof Error ? e.message : String(e));
      return;
    }
    if (this.#stopped) return;
    await this.#converge(now.version, now.hash, { target: now.page, held: now.held, refused });
  }

  /** A write whose answer never came (the keeper out of reach, the answer lost on the way): the keeper asked whether it
   *  took it, the ask a fence -- from it on the keeper takes the write no more, so one still on its way there (a slow
   *  combine, a proxy that gave up first) is never applied after the answer said it was not (the audit's 4). Taken:
   *  the screen put at the keeper's page, which holds it. Not: its edits pending again, in front of the ones made since,
   *  so they are made again on the keeper's page and written next, under a new number -- nothing said, nothing lost. The
   *  keeper still out of reach, or writes going unanswered UNANSWERED_AT_MOST times in a row: the page opened again,
   *  said. */
  async #unansweredWrite(why: string, write: LiveWrite, sentPage: PageDocument | undefined): Promise<void> {
    let now: LiveRead;
    try {
      now = await this.#editor.read(this.#writer, write.seq);
    } catch (e) {
      // neither the answer nor the keeper's word on it came: whether the write was kept is not known, so it is not said
      // dropped -- the page opened as its keeper has it holds it if it was (the audit's 7)
      await this.#reopen(`its keeper did not answer: ${why}; ${e instanceof Error ? e.message : String(e)}; whether the last change sent was kept is not known`);
      return;
    }
    if (this.#stopped) return;
    if (now.taken === undefined) {
      // asked by this writer, the keeper says which of its writes it took: an answer that does not is no keeper's
      await this.#reopen(`its keeper did not answer: ${why}; it said not whether it took the write`);
      return;
    }
    if (now.taken >= write.seq) {
      this.#unanswered = 0;
      await this.#converge(now.version, now.hash, { target: now.page, held: now.held, refused: [] }, sentPage);
      return;
    }
    // not taken -- and, fenced, never to be
    this.#edits = [...write.edits, ...this.#edits];
    this.#unanswered += 1;
    if (this.#unanswered >= UNANSWERED_AT_MOST) {
      this.#unanswered = 0;
      await this.#reopen(`its keeper took none of ${UNANSWERED_AT_MOST} writes in a row: ${why}`);
      return;
    }
    await this.#converge(now.version, now.hash, { target: now.page, held: now.held, refused: [] });
  }

  /** The screen put at the keeper's page at `version` (live-converge.ts), inside following; then its hash checked against
   *  the keeper's (W17); then the pending edits made again. `base`: the page as this screen last sent it, or -- none
   *  sent -- the confirmed one. True when the screen holds the keeper's page; false when it was opened again instead,
   *  said. */
  async #converge(version: number, hash: string, on: Omit<Converge, 'base' | 'pending'>, sent?: PageDocument): Promise<boolean> {
    const base = sent ?? this.#confirmed?.page;
    if (base === undefined) {
      await this.#reopen('the page it converges from is not known here');
      return false;
    }
    const pending = this.#edits;
    this.#edits = [];
    this.shown = undefined;
    let converged: Converged;
    try {
      converged = await this.following(() => this.#screen.converge({ ...on, base, pending }));
    } catch (e) {
      // stopped meanwhile (the page left, the app taken down): nothing opened again
      if (this.#stopped) return false;
      await this.#reopen(`${e instanceof Error ? e.message : String(e)}${dropped(pending)}`);
      return false;
    }
    if (this.#stopped) return false;
    this.#confirmed = { page: on.target, held: on.held };
    this.shown = version;
    if (!(await this.#checked(converged.page, hash, pending))) return false;
    // its pending edits made again on the page as the keeper has it (the screen's: told again, so they are written next)
    if (pending.length > 0) await this.#screen.reapply(pending, converged.restarted);
    if (this.#edits.length > 0) this.changed();
    return true;
  }

  /** The page on screen hashes to the keeper's (W17): true; else the one message that must never show, and the page
   *  opened again. */
  async #checked(page: PageDocument | undefined, hash: string, pending: readonly LiveCommand[] = []): Promise<boolean> {
    if (page === undefined) return true;
    const mine = pageHash(page);
    if (mine === hash) {
      this.#mismatched = false;
      return true;
    }
    if (this.#mismatched) {
      // opened again already and still differing: said, and the page kept as it is (the keeper's, as opened)
      this.#screen.say(HASH_MISMATCH);
      return true;
    }
    this.#mismatched = true;
    await this.#reopen(`${HASH_MISMATCH}${dropped(pending)}`);
    return false;
  }

  async #reopen(why: string): Promise<void> {
    const pending = this.#edits;
    this.#edits = [];
    this.shown = undefined;
    await this.#screen.reopen(`${why}${dropped(pending)}`);
  }
}

/** Said with a reopen or a refusal: the pending edits it dropped, if any (H-11: no edit is dropped unsaid). */
function dropped(pending: readonly LiveCommand[]): string {
  return pending.length === 0 ? '' : `; ${pending.length} change${pending.length === 1 ? '' : 's'} made here meanwhile ${pending.length === 1 ? 'was' : 'were'} not kept`;
}

/** The ids the commands gave out (their makers': a sheet's, a grid's, a chart's, a page filter's). */
function makerIds(commands: readonly LiveCommand[]): string[] {
  return commands.flatMap((c) => {
    if (!('page' in c)) return [];
    const id = (c.page as { readonly id?: string }).id;
    return id === undefined ? [] : [id];
  });
}

function grown(held: readonly string[], more: readonly string[]): readonly string[] {
  const had = new Set(held);
  const added = more.filter((id) => !had.has(id));
  return added.length === 0 ? held : [...held, ...added];
}
