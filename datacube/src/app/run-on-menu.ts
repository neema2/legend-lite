// RUN ON ▸ (docs/ONE_CONNECTION_DESK_2026_10_28.md D2): each data space a page's saved queries run through, its
// execution contexts as its model declares them, the one they run on now checked -- read with the queries
// (sources.ts `dataSpaces`), never asked for. Another chosen, the page is written again with every saved query over that
// data space on the context chosen (run-on.ts: legend's executionKey on each) and opened as it is. This code is a chunk
// of its own, fetched when the page first reads a saved query over a data space (datacube-app.ts).

import type { PageDocument } from '../page-document.ts';
import { runOn, runningOn } from '../run-on.ts';
import type { DataSpaceRead } from '../saved-queries.ts';
import type { MenuItem } from '../ui/menu.ts';

/** A context an entry names. */
export interface RunOnChoice {
  readonly dataSpace: string;
  readonly context: string;
  readonly title: string;
}

/** The page's Run On ▸ entries, `host.runOn.<n>` naming `choices[n]`: one submenu, or one for each data space where
 *  several are offered. A data space whose queries were not read here offers nothing. */
export function runOnEntries(page: PageDocument, spaces: ReadonlyMap<string, DataSpaceRead>):
  { readonly items: MenuItem[]; readonly choices: readonly RunOnChoice[] } {
  const choices: RunOnChoice[] = [];
  const offered = [...runningOn(page)].flatMap(([path, keys]) => {
    const space = spaces.get(path);
    return space === undefined ? [] : [{ space, keys }];
  });
  const items = offered.map(({ space, keys }): MenuItem => {
    const on = new Set([...keys].map((k) => k ?? space.defaultContext));
    return {
      label: offered.length > 1 ? `Run ${space.title} On` : 'Run On',
      section: 'data',
      submenu: space.contexts.map((c): MenuItem => {
        choices.push({ dataSpace: space.path, context: c.name, title: c.title });
        return { id: `host.runOn.${choices.length - 1}`, label: c.title, checked: on.has(c.name) };
      }),
    };
  });
  return { items, choices };
}

/** `page` with its saved queries over the choice's data space on the choice's context. */
export const runningOnChoice = (page: PageDocument, choice: RunOnChoice): PageDocument =>
  runOn(page, choice.dataSpace, choice.context);
