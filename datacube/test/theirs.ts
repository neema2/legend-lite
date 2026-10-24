// ANOTHER EDITOR'S CHANGE, FOR THE PAGE'S TESTS (page-app.test.ts, cube-page-shell.test.ts): the one way it reaches a
// page now, the converge (src/app/live-converge.ts), so a test of a page's screen under another editor's change goes
// through the code the live page runs.

import { convergeOn, type ConvergeGrid } from '../src/app/live-converge.ts';
import { applyPageCommands, type PageCommand } from '../src/page-commands.ts';
import type { GridMaker, PageApp } from '../src/page/page-app.ts';

/**
 * ANOTHER EDITOR'S CHANGE, AS IT REACHES A PAGE (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version):
 * the screen set to the page `commands` make of it, by the converge (live-converge.ts convergeOn) -- a grid they add
 * opened by its maker in `makers`. Refused in the commands' words (CubeRefusal) when they do not apply, nothing changed.
 */
export async function theirs(page: PageApp, commands: readonly PageCommand[], makers: ReadonlyMap<string, GridMaker> = new Map()): Promise<void> {
  const target = applyPageCommands(page.state, commands).draft;
  await convergeOn({ target: target.page, held: target.held, base: page.state.page, refused: [], pending: [] }, {
    page,
    grid: (id) => page.grid(id) as unknown as ConvergeGrid | undefined,
    pageNow: () => page.document(page.title, undefined, true),
    open: async (_document, grid) => {
      const make = makers.get(grid);
      return make === undefined ? undefined : { make };
    },
  });
}
