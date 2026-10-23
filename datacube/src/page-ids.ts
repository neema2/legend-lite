// A PAGE'S IDS AND MADE TITLES (docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md): one allocator -- a new tile's or sheet's id
// after every one of its kind the page has held -- and a new tile's title from its id. The screen's page asks them as
// it makes a tile, and the page commands (page-commands.ts) by the same rules; kept apart from those, so the screen's
// page does not download the commands' rules until another editor sends it commands.

/** The number an id ends with (`chart-3`: 3), or 0. */
const numberOf = (id: string): number => Number(/-(\d+)$/.exec(id)?.[1] ?? 0);

/** A new id of `kind` (`chart-4`): after every one of its kind the page has held, so never one a handle had. */
export function nextPageId(held: readonly string[], kind: 'grid' | 'chart' | 'sheet' | 'filter'): string {
  return `${kind}-${Math.max(0, ...held.filter((h) => h.startsWith(`${kind}-`)).map(numberOf)) + 1}`;
}

/** A new chart's title, as the screen titles one: its number (`chart-4`: "Chart 4"). */
export const chartTitle = (id: string): string => `Chart ${numberOf(id)}`;

/** A page of its own's new grid's title, unless given one: its number. A grid's made title is not written. */
export const gridTitle = (id: string): string => `Grid ${numberOf(id)}`;
