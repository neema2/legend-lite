// ONE PAGE STATE (docs/DATACUBE_PAGE_STATE_2026_10_18.md; the command program's stage 2): the page's document as its
// commands edit it (page-commands.ts PageDraft), held in one place, with one history. A page edit is a step of that
// history -- undone and redone as a whole -- as a grid's edit is a step of its own owner's (state-owner.ts).
//
// THE BOARD IS PRODUCED BY THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3): every edit of the page's structure
// is commands, applied here by the one step every owner shares (page-commands.ts applyToState: the commands applied,
// then `step` or `keep` the page they make) -- a page of its own's (page/page-app.ts) and a cube alone's board (app.ts)
// alike -- and the screen is then put at what they make (page/cube-page.ts sync). The screen changes nothing of the page
// itself: a guard holds it (test/state-guardrail.test.ts).
//
// THE HISTORY IS COMMANDS (docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3, W4): a step keeps the commands that made it
// (`forward`, as applied) and the commands that take the page back (`inverse`: page-between.ts pageBetween, made as the
// step is, in the history's scope -- the page's structure, not its name, its saved record or its model). An undo applies
// the inverse to the page AS IT IS NOW, a redo the forward commands: each an edit like any other, told as such and
// applied by the keeper as such, so the page's history survives another editor's change -- an undo past it puts back
// what the step changed, by set semantics, and a step whose target is gone is dead: dropped, said, never guessed at.
// A tile a step brings back is a new tile (B12: an id the page has held is never given out again): `applyPlanned` gives
// it a fresh id and every step's commands follow the rename (`rename`), as they follow the keeper's.
//
// `keep` alone is how what the page's own owners change reaches it with no step: a grid's own edits (its cube: the
// grid's own owner undoes them), a chart's spec as it follows its grid, the page's name (its host's).
//
// The ids a page has held only grow (page-commands.ts nextPageId), so a handle a host kept never meets another tile
// under its old one's id.
//
// No DOM: every rule here is tested alone (test/page-state.test.ts).

import { applyPlanned, pageBetween } from './page-between.ts';
import { idsOf, type PageCommand, type PageDraft } from './page-commands.ts';
import { renamedCommand } from './page-rebase.ts';
import { CubeRefusal, sameValue } from './snapshot.ts';
import { DEFAULT_HISTORY_LIMIT } from './state-owner.ts';

/** A step of the page's history: what it was called, the commands that made it, and the ones that take it back. */
export interface PageStep {
  readonly label: string;
  readonly forward: readonly PageCommand[];
  readonly inverse: readonly PageCommand[];
}

/** A move of the history landed: the step's label, the commands applied (their ids as given), and each id a command
 *  planned that was given out afresh (planned -> given: a tile brought back as a new tile). */
export interface PageMoved {
  readonly label: string;
  readonly applied: readonly PageCommand[];
  readonly renamed: Readonly<Record<string, string>>;
}

/** A step that no longer applies (its tile taken off by another editor since): its label, and why, in the commands'
 *  words. The step is dropped. */
export interface PageDead {
  readonly label: string;
  readonly dead: string;
}

export class PageState {
  #draft: PageDraft;
  /** The steps done, oldest first. */
  #past: PageStep[] = [];
  /** The steps undone, the last undone last. */
  #future: PageStep[] = [];
  readonly #limit: number;
  /** The tag of the ids this page makes (page-ids.ts nextPageId): a tile an undo brings back is given its new id under
   *  it, so no other editor makes the same (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version). */
  tag: string | undefined;

  constructor(draft: PageDraft, limit = DEFAULT_HISTORY_LIMIT) {
    this.#draft = draft;
    this.#limit = Math.max(1, limit);
  }

  /** The page as it is now. */
  get draft(): PageDraft {
    return this.#draft;
  }

  /** A step named `label` made (page-commands.ts applyToState): `forward`, the commands as applied, took the page from
   *  `before` to `after`; its inverse is made now, by the one rule, and the page is `after`. */
  step(label: string, forward: readonly PageCommand[], before: PageDraft, after: PageDraft): void {
    const inverse = pageBetween(after.page, before.page, 'history');
    this.#past.push({ label, forward, inverse });
    if (this.#past.length > this.#limit) this.#past.shift();
    this.#future = [];
    this.keep(after);
  }

  /** The page as its owners have it now, with no step of its own. */
  keep(draft: PageDraft): void {
    this.#draft = { page: draft.page, held: grown(this.#draft.held, draft.held) };
  }

  /** The page put back at `draft` as its keeper has it, the ids it holds exactly `draft.held` (a screen converging on
   *  its keeper's page, docs/DATACUBE_LIVE_EDITING_2026_10_24.md §2 step 1: the ids given out here since the version
   *  shown, which the keeper never held under those names, given back); its history kept. */
  putBack(draft: PageDraft): void {
    this.#draft = { page: draft.page, held: [...new Set([...draft.held, ...idsOf(draft.page)])] };
  }

  /** The last step undone: its inverse applied to the page as it is now (undefined: none to undo). */
  undo(): PageMoved | PageDead | undefined {
    const step = this.#past.pop();
    if (!step) return undefined;
    return this.#move(step, step.inverse, this.#future);
  }

  /** The last step undone done again: its commands applied to the page as it is now. */
  redo(): PageMoved | PageDead | undefined {
    const step = this.#future.pop();
    if (!step) return undefined;
    return this.#move(step, step.forward, this.#past);
  }

  /** `plan` applied to the page as it is now, a fresh id for what it makes (its planned one is held), and `step` put
   *  onto `onto`, its commands and every other step's renamed as the ids were; or the step dead, dropped. */
  #move(step: PageStep, plan: readonly PageCommand[], onto: PageStep[]): PageMoved | PageDead {
    let done: ReturnType<typeof applyPlanned>;
    try {
      done = applyPlanned(this.#draft, plan, true, this.tag);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw e;
      return { label: step.label, dead: e.message };
    }
    this.#draft = { page: done.draft.page, held: grown(this.#draft.held, done.draft.held) };
    onto.push(step);
    this.rename(done.renamed);
    return { label: step.label, applied: done.applied, renamed: done.renamed };
  }

  /** The steps whose every forward command is one of `commands` dropped, from the past and the future: a refused
   *  batch's, which never made the keeper's page (a converge's put-back, live-converge.ts). */
  dropSteps(commands: readonly PageCommand[]): void {
    if (commands.length === 0) return;
    const refused = (s: PageStep): boolean => s.forward.length > 0 && s.forward.every((c) => commands.some((r) => sameValue(r, c)));
    this.#past = this.#past.filter((s) => !refused(s));
    this.#future = this.#future.filter((s) => !refused(s));
  }

  /** Every step's commands renamed as `renamed` says (planned -> given): an id given out afresh here (a tile brought
   *  back as a new tile, B12), so a later move names the tile as it is now. */
  rename(renamed: Readonly<Record<string, string>>): void {
    const map = new Map(Object.entries(renamed));
    if (map.size === 0) return;
    const named = (commands: readonly PageCommand[]): PageCommand[] => commands.map((c) => renamedCommand(c, map));
    const renamedStep = (s: PageStep): PageStep => ({ label: s.label, forward: named(s.forward), inverse: named(s.inverse) });
    this.#past = this.#past.map(renamedStep);
    this.#future = this.#future.map(renamedStep);
  }

  /** Every grid a step of the history (back or forward) can bring back, by the id it had -- and every kept cube a chart
   *  can be put back on (`detach`) or a timeseries tile read again (its command's `kept`): what its host keeps the maker
   *  and the tables of while it can (page-app.ts recoverable). */
  get reachable(): ReadonlySet<string> {
    const ids = new Set<string>();
    for (const step of [...this.#past, ...this.#future]) {
      for (const c of [...step.forward, ...step.inverse]) {
        if (c.kind === 'addGrid' && c.id !== undefined) ids.add(c.id);
        if (c.kind === 'detach') ids.add(c.cube);
        if (c.kind === 'addTimeseries' || c.kind === 'setTimeseries') for (const k of c.kept ?? []) ids.add(k.cube);
      }
    }
    return ids;
  }

  /** The step Undo would undo, by its label. */
  get undoLabel(): string | undefined {
    return this.#past.at(-1)?.label;
  }

  /** The step Redo would do again, by its label. */
  get redoLabel(): string | undefined {
    return this.#future.at(-1)?.label;
  }
}

/** The ids held, with those of `more` the page had not held (kept in the order they came). */
function grown(held: readonly string[], more: readonly string[]): readonly string[] {
  const had = new Set(held);
  const added = more.filter((id) => !had.has(id));
  return added.length === 0 ? held : [...held, ...added];
}
