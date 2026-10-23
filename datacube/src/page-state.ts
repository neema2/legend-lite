// ONE PAGE STATE (docs/DATACUBE_PAGE_STATE_2026_10_18.md; the command program's stage 2): the page's document as its
// commands edit it (page-commands.ts PageDraft), held in one place, with one history. A page edit is a step of that
// history -- undone and redone as a whole -- as a grid's edit is a step of its own owner's (state-owner.ts).
//
// THE BOARD IS PRODUCED BY THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S3): every edit of the page's structure
// is commands, applied here by the one step every owner shares (page-commands.ts applyToState: `begin`, then `keep` the
// page they make) -- a page of its own's (page/page-app.ts) and a cube alone's board (app.ts) alike -- and the screen is
// then put at what they make (page/cube-page.ts sync). The screen changes nothing of the page itself: a guard holds it
// (test/state-guardrail.test.ts).
//
// `keep` alone is how what the page's own owners change reaches it with no step: a grid's own edits (its cube: the
// grid's own owner undoes them), a chart's spec as it follows its grid, the page's name (its host's).
//
// The ids a page has held only grow: a step undone gives its ids back to no one (page-commands.ts nextPageId), so a
// handle a host kept never meets another tile under its old one's id; a step redone brings its own tiles back.
//
// No DOM: every rule here is tested alone (test/page-state.test.ts).

import type { PageDraft } from './page-commands.ts';
import { DEFAULT_HISTORY_LIMIT } from './state-owner.ts';

/** A step of the page's history: what it was called, and the page on the other side of it. */
export interface PageStep {
  readonly label: string;
  readonly draft: PageDraft;
}

export class PageState {
  #draft: PageDraft;
  /** The drafts before each step done, oldest first, each with the step's label. */
  #past: PageStep[] = [];
  /** The drafts after each step undone, the last undone last. */
  #future: PageStep[] = [];
  readonly #limit: number;

  constructor(draft: PageDraft, limit = DEFAULT_HISTORY_LIMIT) {
    this.#draft = draft;
    this.#limit = Math.max(1, limit);
  }

  /** The page as it is now. */
  get draft(): PageDraft {
    return this.#draft;
  }

  /** An edit named `label` begins: the page as it is now is the step back from it. */
  begin(label: string): void {
    this.#record(label);
  }

  /** The page as its owners have it now, with no step of its own (and, after `begin`, the edit begun). */
  keep(draft: PageDraft): void {
    this.#draft = { page: draft.page, held: grown(this.#draft.held, draft.held) };
  }

  /** The last step undone: the page as it was before it, returned with the step's label (undefined: none to undo). */
  undo(): PageStep | undefined {
    const step = this.#past.pop();
    if (!step) return undefined;
    this.#future.push({ label: step.label, draft: this.#draft });
    this.#draft = { page: step.draft.page, held: grown(this.#draft.held, step.draft.held) };
    return { label: step.label, draft: this.#draft };
  }

  /** The last step undone done again. */
  redo(): PageStep | undefined {
    const step = this.#future.pop();
    if (!step) return undefined;
    this.#past.push({ label: step.label, draft: this.#draft });
    this.#draft = { page: step.draft.page, held: grown(this.#draft.held, step.draft.held) };
    return { label: step.label, draft: this.#draft };
  }

  /** Every tile the history's other pages hold (its steps back and forward): a tile gone now that an undo or a redo can
   *  bring back. */
  get reachable(): ReadonlySet<string> {
    // its tiles, and its cubes: a detached chart's is a cube with no tile of its own
    return new Set([...this.#past, ...this.#future].flatMap((step) => [...step.draft.page.views.map((v) => v.id),
      ...step.draft.page.cubes.map((c) => c.id)]));
  }

  /** The step Undo would undo, by its label. */
  get undoLabel(): string | undefined {
    return this.#past.at(-1)?.label;
  }

  /** The step Redo would do again, by its label. */
  get redoLabel(): string | undefined {
    return this.#future.at(-1)?.label;
  }

  #record(label: string): void {
    this.#past.push({ label, draft: this.#draft });
    if (this.#past.length > this.#limit) this.#past.shift();
    this.#future = [];
  }
}

/** The ids held, with those of `more` the page had not held (kept in the order they came). */
function grown(held: readonly string[], more: readonly string[]): readonly string[] {
  const had = new Set(held);
  const added = more.filter((id) => !had.has(id));
  return added.length === 0 ? held : [...held, ...added];
}
