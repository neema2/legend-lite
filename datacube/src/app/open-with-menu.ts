// OPEN WITH ▸ (docs/ONE_CONNECTION_DESK_2026_10_28.md D2, revised by the user 2026-10-28: "Open With is only for Saved
// things - its basically an Open for a saved Page that has a parameter"): a saved page -- one the store keeps, or an
// example the site ships (the trading desk) -- opened again, in a tab of its own, with its saved queries over a data
// space on another of the data space's execution contexts -- the context a parameter of that opening (the address's
// `#open=<id>&space=<path>&context=<name>`, or `#example=<id>&…`), never an edit of the page: it is not changed by it,
// and Save writes the page's own contexts. Each data space the page's saved queries were read
// through offers its contexts as its model declares them, the one this tab runs on checked (sources.ts `dataSpaces`).
// This code is a chunk of its own, fetched when the page first reads a saved query over a data space (datacube-app.ts).

import type { DataSpaceRead } from '../saved-queries.ts';
import type { MenuItem } from '../ui/menu.ts';

/** A context an entry names. */
export interface OpenWithChoice {
  readonly dataSpace: string;
  readonly context: string;
  readonly title: string;
}

/** The page's Open With ▸ entries, `host.openWith.<n>` naming `choices[n]`: one submenu, or one for each data space
 *  where several were read. */
export function openWithEntries(spaces: ReadonlyMap<string, DataSpaceRead>):
  { readonly items: MenuItem[]; readonly choices: readonly OpenWithChoice[] } {
  const choices: OpenWithChoice[] = [];
  const items = [...spaces.values()].map((space): MenuItem => ({
    label: spaces.size > 1 ? `Open ${space.title} With` : 'Open With',
    section: 'data',
    submenu: space.contexts.map((c): MenuItem => {
      choices.push({ dataSpace: space.path, context: c.name, title: c.title });
      return { id: `host.openWith.${choices.length - 1}`, label: c.title, checked: c.name === space.context };
    }),
  }));
  return { items, choices };
}

/** The saved page Open With ▸ opens: one the store keeps (`open`, by its record's id), or an example the site ships
 *  (`example`, by its id: the trading desk) -- opened as saved, or as shipped. */
export interface OpenWithPage {
  /** The address's key for it: `#open=<id>` or `#example=<id>`. */
  readonly fragment: 'open' | 'example';
  readonly id: string;
}

/** The address that opens `target` with `choice`: this page's own, its fragment the opening's. */
export function openWithAddress(page: string, target: OpenWithPage, choice: OpenWithChoice): string {
  const at = new URL(page);
  at.hash = `${target.fragment}=${encodeURIComponent(target.id)}&space=${encodeURIComponent(choice.dataSpace)}`
    + `&context=${encodeURIComponent(choice.context)}`;
  return at.href;
}
