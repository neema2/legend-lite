// A HEADER'S SORT CLICK AS ITS COMMAND (the user, 2026-10-18: the click's rule; 2026-10-22: "there should be a single
// sort API that UI actions call that also can be called programmatically"). One rule the screen's header click and
// Python's grid.toggle_sort and grid.toggle_group_sort ask (grid-gestures.ts header.sort, headless gridGesture): the
// command each clicks makes,
// applied by the one rule every sort is (grid-edits.ts clickSort, clickTreeSort).

import { firstSortDirection } from './grid-edits.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal } from './snapshot.ts';
import { TREE_COLUMN } from './treeview.ts';

/** A header as the grid's view shows it: its column, and the type the compiler gave it (a pivot's is its aggregate's). */
export interface SortedHeader {
  readonly name: string;
  readonly type?: string | undefined;
}

/**
 * The command a click on `column`'s header makes, `chained` (Ctrl, or ⌘ on a Mac) or not: the tree column's turns the
 * groups' order (clickTreeSort); any other's sorts by it, first in its direction by the type its header has -- which
 * only the grid's view has (`headers`: the leaves it paints, shown-columns.ts shownColumns; a pivot's header and a count
 * of text are numbers) -- then the other way, then none (clickSort). A column the grid shows no header of is refused,
 * naming those it does.
 */
export function headerSortCommand(headers: readonly SortedHeader[], column: string, chained: boolean): GridCommand {
  if (column === TREE_COLUMN) return { kind: 'clickTreeSort', chained };
  const header = headers.find((h) => h.name === column);
  if (header === undefined) {
    throw new CubeRefusal(`'${column}' is no header of the grid (${headers.map((h) => h.name).join(', ') || 'it shows none'})`);
  }
  return { kind: 'clickSort', column, first: firstSortDirection(header.type), chained };
}
