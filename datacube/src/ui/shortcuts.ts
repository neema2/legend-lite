// THE KEYBOARD SHORTCUTS, as the grid and the cube take them (grid/grid.ts #onKeyDown, app.ts's document keys): the list
// View ▸ Keyboard Shortcuts… and the ? key show. Written beside the keys' own code's words, so a shortcut added there is
// added here; Ctrl is Cmd on a Mac wherever it is named.

/** One shortcut: the keys, and what they do. */
export interface Shortcut {
  readonly keys: string;
  readonly does: string;
}

/** The shortcuts by what they are for, in the order a person learns them. */
export const SHORTCUTS: readonly { readonly title: string; readonly shortcuts: readonly Shortcut[] }[] = [
  {
    title: 'Moving',
    shortcuts: [
      { keys: 'Arrow', does: 'the next cell' },
      { keys: 'Ctrl+Arrow', does: 'the first or last row or column' },
      { keys: 'Home / End', does: 'the first or last column' },
      { keys: 'Ctrl+Home / Ctrl+End', does: 'the first or last row' },
      { keys: 'Page Up / Page Down', does: 'a screen up or down' },
    ],
  },
  {
    title: 'Selecting',
    shortcuts: [
      { keys: 'Shift+Arrow', does: 'grow the selection from where it started' },
      { keys: 'Shift+click', does: 'select to the cell clicked' },
      { keys: 'Drag', does: 'select the cells dragged across' },
      { keys: 'Ctrl+A', does: 'select every cell' },
      { keys: 'Ctrl+C', does: 'copy the selection, to paste into a spreadsheet' },
    ],
  },
  {
    title: 'Groups and rows',
    shortcuts: [
      { keys: 'Right / Left', does: 'open or close the group' },
      { keys: 'Double-click a group', does: 'open or close it' },
      { keys: 'Enter / Space', does: 'show the rows behind the cell' },
      { keys: 'Double-click a value', does: 'show the rows behind it' },
    ],
  },
  {
    title: 'The grid',
    shortcuts: [
      { keys: 'Ctrl+Z', does: 'undo' },
      { keys: 'Ctrl+Shift+Z / Ctrl+Y', does: 'redo' },
      { keys: 'Ctrl+E', does: 'the grid\'s properties' },
      { keys: 'Click a header', does: 'sort by it; Ctrl+click adds it to the sort' },
      { keys: '?', does: 'this list' },
    ],
  },
];

/** The list as a table: a heading row per group, then a row per shortcut. */
export function shortcutsTable(doc: Document): HTMLTableElement {
  const table = doc.createElement('table');
  table.className = 'dc-shortcuts';
  for (const group of SHORTCUTS) {
    const head = table.insertRow();
    const th = doc.createElement('th');
    th.colSpan = 2;
    th.textContent = group.title;
    head.append(th);
    for (const s of group.shortcuts) {
      const row = table.insertRow();
      const keys = row.insertCell();
      keys.className = 'dc-shortcut-keys';
      keys.textContent = s.keys;
      row.insertCell().textContent = s.does;
    }
  }
  return table;
}
