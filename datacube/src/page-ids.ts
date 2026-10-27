// A PAGE'S IDS AND MADE TITLES (docs/DATACUBE_COMMANDS_PAGE_2026_10_18.md): one allocator -- a new tile's or sheet's id
// after every one of its kind the page has held -- and a new tile's title from its id. The screen's page asks them as
// it makes a tile, and the page commands (page-commands.ts) by the same rules; kept apart from those, so the screen's
// page does not download the commands' rules until another editor sends it commands.

/** The number an id ends with (`chart-3`: 3), or 0. */
const numberOf = (id: string): number => Number(/-(\d+)$/.exec(id)?.[1] ?? 0);

/** A new id of `kind` (`chart-4`): after every one of its kind the page has held, so never one a handle had. `tag`: the
 *  making editor's own (`chart-tk3f9-4`), so two editors of one page never make the same id, and nothing is ever given
 *  out afresh (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version) -- its number still after every
 *  held one, so a tag a past session had never meets an id it made. */
export function nextPageId(held: readonly string[], kind: 'grid' | 'chart' | 'text' | 'timeseries' | 'sheet' | 'filter', tag?: string): string {
  const n = Math.max(0, ...held.filter((h) => h.startsWith(`${kind}-`)).map(numberOf)) + 1;
  return tag === undefined ? `${kind}-${n}` : `${kind}-${tag}-${n}`;
}

/** The shape of every id of `kind` a page holds: `kind-N`, or `kind-<tag>-N` under an editor's tag (idTag) -- what a
 *  command may make (page-commands.ts freshId) and what a page is read with (page-filters.ts FILTER_ID). */
export const idShape = (kind: 'grid' | 'chart' | 'text' | 'timeseries' | 'sheet' | 'filter'): RegExp => new RegExp(`^${kind}-(?:[a-z][a-z0-9]*-)?\\d+$`);

/** An editor's tag for the ids it makes: a letter and five letters or digits, at random. */
export function idTag(): string {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = new Uint8Array(5);
  globalThis.crypto.getRandomValues(bytes);
  return `t${[...bytes].map((b) => alphabet[b % alphabet.length]).join('')}`;
}

/** A new chart's title, as the screen titles one: its number (`chart-4`: "Chart 4"). */
export const chartTitle = (id: string): string => `Chart ${numberOf(id)}`;

/** A text tile's title, unless given one: its number (`text-2`: "Text 2"), not written, as a grid's made title is not. */
export const textTitle = (id: string): string => `Text ${numberOf(id)}`;

/** A timeseries tile's title, unless given one: its number (`timeseries-2`: "Timeseries 2"), not written. */
export const timeseriesTitle = (id: string): string => `Timeseries ${numberOf(id)}`;

/** A page of its own's new grid's title, unless given one: its number. A grid's made title is not written. */
export const gridTitle = (id: string): string => `Grid ${numberOf(id)}`;
