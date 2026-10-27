// A GRID'S RIGHT-CLICK JOIN ▸, as its Filter ▸ is (the user, 2026-10-23: "a fast option to join to another grid with the
// same column name and type, then more complex options, then the full join"; "like filter -- only if clicked on a
// column, otherwise just the option to open the join dialog"): on a column, all of mine joined at once to each grid of
// the page with that column (join-how.ts quickJoins) -- made unless it would repeat rows of mine, when the window opens on
// it (join-flow.ts) -- the other ways of each, and the window on that column; on none, the window alone. And a join to a
// data source not on the page yet. The entries and what each asks; the page's grids are its caller's to gather.

import { quickJoins, type JoinColumns, type JoinRow, type QuickForm } from '../join-how.ts';
import type { MenuItem } from '../ui/menu.ts';
import type { JoinAsk } from './join-flow.ts';

/** Join ▸ ▸ Join…, and Join a Data Source…: the host's ids. */
export const JOIN_WINDOW = 'host.join.window' as const;
export const JOIN_SOURCE = 'host.join.source' as const;
/** How many grids' quick joins it lists at most. */
const QUICK_JOINS = 6;

/** A key in words: `country` where both sides name it alike, else `customer_id = id`. */
const on = (row: JoinRow): string => (row.left === row.right ? row.left : `${row.left} = ${row.right}`);

/** The other ways of a quick join, after all of mine: each form and its words. */
const WAYS: readonly (readonly [QuickForm, (name: string) => string])[] = [
  ['inner', (n) => `Matching only, with ${n}`], ['semi', (n) => `Mine with a match in ${n}`], ['anti', (n) => `Mine with none in ${n}`],
];

/**
 * Join ▸ on `column` (none: right-clicked where no column is) of grid `mine`, the page's other grids `others` it can join
 * (none pivoted), and whether a data source can be picked: its entries, and what each asks (JoinAsk) by its id.
 */
export function joinMenu(o: {
  readonly column: string | undefined;
  readonly mine: JoinColumns;
  readonly others: readonly (JoinColumns & { readonly id: string })[];
  readonly source: boolean;
}): { readonly items: MenuItem[]; readonly asks: Map<string, JoinAsk> } {
  const asks = new Map<string, JoinAsk>();
  const items: MenuItem[] = [];
  const quick = o.column !== undefined ? quickJoins(o.mine, o.others, o.column).slice(0, QUICK_JOINS) : [];
  quick.forEach((q, i) => {
    const id = `host.join.with.${i}` as const;
    asks.set(id, { their: q.their, row: q.row, quick: 'left' });
    items.push({ id, label: `With ${q.name} on ${on(q.row)}` });
  });
  if (o.column !== undefined && quick.length === 0) {
    items.push({ label: o.others.length > 0 ? `No other grid has a column ${o.column}` : 'No other grid on the page to join', disabled: true });
  }
  const other: MenuItem[] = [];
  quick.forEach((q, i) => WAYS.forEach(([form, words], w) => {
    const id = `host.join.way.${i}.${form}` as const;
    asks.set(id, { their: q.their, row: q.row, quick: form });
    other.push({ id, label: `${words(q.name)} on ${on(q.row)}`, ...(w === 0 && i > 0 ? { separated: true } : {}) });
  }));
  if (other.length > 0) items.push({ label: 'Other Ways', submenu: other });
  // the window: at the first grid with the column clicked, on its key, when there is one
  const first = quick[0];
  asks.set(JOIN_WINDOW, first ? { their: first.their, row: first.row } : {});
  // off, saying why: the grid's status bar greys its Join with it (app.ts #joinTool)
  items.push({ id: JOIN_WINDOW, label: 'Join…', ...(items.length > 0 ? { separated: true } : {}),
    ...(o.others.length > 0 ? {} : { disabled: true, hint: 'There is no other grid on this page to join' }) });
  if (o.source) items.push({ id: JOIN_SOURCE, label: 'Join a Data Source…' });
  return { items, asks };
}
