// A KEEPER AS PYTHON'S IS, FOR THE TESTS (keeper.py settle; docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the
// middle version): a write's edits applied by DataCube's one rule (page-rebase.ts rebaseEdits) to the page as it is --
// made again on top when it moved on since -- and what they make kept, each write applied once by its id; the answer a
// write gets carries the keeper's page when it made another than the tab sent, and the page's hash (page-hash.ts), as
// Python's engine answers; a refusal is a LiveRefused, in its words. A grid's commands are applied to its saved cube as headless gridApply applies them -- no
// planner: what a run alone adds (column types) is no command's.

import { applyToSnapshot } from '../src/config.ts';
import { configurationOf, treeOf, writeCube } from '../src/cube-document.ts';
import { fold } from '../src/cube-state.ts';
import { applyGridCommands } from '../src/grid-commands.ts';
import { LiveRefused, type LiveCommand, type LivePage, type LiveRead, type LiveSettled, type LiveWrite } from '../src/app/services.ts';
import { applyPageCommands, idsOf, type PageCommand } from '../src/page-commands.ts';
import type { PageDocument } from '../src/page-document.ts';
import { pageHash } from '../src/page-hash.ts';
import { rebaseEdits, type GridApplier } from '../src/page-rebase.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { SNAPSHOT } from './cube-fixture.ts';

/** A grid's commands applied to its saved cube as headless gridApply applies them (headless/grid.ts applied: the
 *  commands, the settings laid on the query, folded as the state lands, written as the screen writes it). */
export const applyGridToCube: GridApplier = async (_page, cube, _id, commands) => {
  const snapshot = { ...cube.query, source: SNAPSHOT.source, epoch: 0 } as CubeSnapshot;
  const d = applyGridCommands({ snapshot, config: configurationOf(cube), tree: treeOf(cube) }, commands);
  const landed = fold({ snapshot: applyToSnapshot(d.snapshot, d.config), configuration: d.config, tree: d.tree });
  return writeCube({
    name: cube.name, source: cube.source, snapshot: landed.snapshot, configuration: landed.configuration, tree: landed.tree,
    ...(cube.unknown ? { unknown: cube.unknown } : {}),
  });
};

export class FakeKeeper implements LivePage {
  version = 1;
  page: PageDocument;
  held: string[];
  /** Each write, as sent. */
  readonly written: LiveWrite[] = [];
  /** The edits that moved the page to each version (none for version 1, the page born whole). */
  readonly steps = new Map<number, readonly LiveCommand[]>();
  reads = 0;
  /** How many writes it made another page of (combined), and how many it refused. */
  combinedCount = 0;
  refusedCount = 0;
  /** A write held until the test lets it go (the keeper slow to answer): each pending write's release. */
  hold = false;
  readonly #held: (() => void)[] = [];
  /** The keeper unreachable: how many of the next writes fail before it sees them (the browser's fetch rejecting). */
  unreachable = 0;
  /** How many of the next reads fail the same way. */
  readsFail = 0;
  /** The next write's answer lost: the keeper keeps the write, the tab hears a failure. */
  loseAnswer = false;
  /** The last write taken from each writer (its number): a write sent again is applied once. */
  readonly #taken = new Map<string, number>();

  constructor(page: PageDocument, held: readonly string[] = idsOf(page)) {
    this.page = page;
    this.held = [...held];
  }

  async read(writer?: string): Promise<LiveRead> {
    if (this.readsFail > 0) {
      this.readsFail -= 1;
      throw new TypeError('fetch failed');
    }
    this.reads += 1;
    return {
      version: this.version, page: this.page, held: this.held, hash: pageHash(this.page),
      ...(writer === undefined ? {} : { taken: this.#taken.get(writer) ?? 0 }),
    };
  }

  /** The other editor (Python) writes commands it made at the version now. */
  pythonWrites(commands: readonly PageCommand[]): void {
    const done = applyPageCommands({ page: this.page, held: this.held }, commands);
    this.page = done.draft.page;
    this.held = [...done.draft.held];
    this.version += 1;
    this.steps.set(this.version, done.applied.map((c): LiveCommand => ({ page: c })));
  }

  /** The other editor writes a page whole (a page loaded: N1). */
  pythonLoads(page: PageDocument): void {
    this.page = page;
    this.held = [...new Set([...this.held, ...idsOf(page)])];
    this.version += 1;
  }

  /** Every write held is let go, and none is held from now on. */
  release(): void {
    this.hold = false;
    for (const go of this.#held.splice(0)) go();
  }

  async report(at: number, write: LiveWrite): Promise<LiveSettled> {
    if (this.unreachable > 0) {
      this.unreachable -= 1;
      throw new TypeError('fetch failed');
    }
    this.written.push(write);
    if (this.hold) await new Promise<void>((r) => { this.#held.push(r); });
    // taken already (sent again after its answer was lost): applied once, the page as it is now answered
    if ((this.#taken.get(write.writer) ?? 0) >= write.seq) return { version: this.version, hash: pageHash(this.page), page: this.page, held: this.held };
    // as keeper.py settle: the page as it is now taken, the edits applied to it, and what they make kept only when the
    // page is still at that version -- another write landed meanwhile (two tabs' writes let go together), made again
    // on top of what it made
    for (;;) {
      if (at > this.version) {
        this.refusedCount += 1;
        throw new LiveRefused('409: the page is not at that version');
      }
      const before = this.version;
      const done = await rebaseEdits({ page: this.page, held: this.held }, write.edits, applyGridToCube).catch((e: unknown) => {
        this.refusedCount += 1;
        throw new LiveRefused(`409: ${at !== before ? 'the page changed where it is open while this change was made: ' : ''}${e instanceof Error ? e.message : String(e)}`);
      });
      if (this.version !== before) continue;
      const combined = at !== before;
      if (combined) this.combinedCount += 1;
      this.page = done.page;
      this.held = [...done.held];
      this.version += 1;
      this.steps.set(this.version, done.edits);
      this.#taken.set(write.writer, write.seq);
      const hash = pageHash(this.page);
      if (this.loseAnswer) {
        // kept, but the answer never reaches the tab
        this.loseAnswer = false;
        throw new TypeError('fetch failed');
      }
      return combined ? { version: this.version, hash, page: this.page, held: this.held } : { version: this.version, hash };
    }
  }
}
