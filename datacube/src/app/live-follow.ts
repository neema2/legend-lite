// FOLLOWING ANOTHER EDITOR'S PAGE (docs/DATACUBE_LIVE_COMMANDS_2026_10_18.md; docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md,
// the middle version): a move of the page itself is followed by putting the screen at the keeper's page, read whole
// (live-channel.ts follow, live-converge.ts); a move of a frame alone, by the grids over it reading it again. No DOM:
// every rule here is tested alone (test/live-follow.test.ts).

import type { Versions } from './services.ts';

/**
 * How a move the editor says is followed: the page moved -- the screen put at the keeper's page (a frame that moved with
 * it is then read again by every grid over it: docs/DATACUBE_LIVE_EDITING_2026_10_24.md W16) -- or, the page not moved,
 * by its frames alone.
 */
export function followBy(now: Versions, shown: number): 'page' | 'frames' {
  return now.version === undefined || now.version === shown ? 'frames' : 'page';
}
