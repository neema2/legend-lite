// DataCube's documentation hints: upstream's `docs/*.md` (legend-data-
// cube, by the same ids), shown from a (?) beside the field in a
// Documentation window, as upstream's FormDocumentation opens its
// documentation panel.
//
// The (?) does not reach into the app: it dispatches a bubbling
// `dc-doc` event with the id, and the app -- listening at its root --
// opens the window. So a panel needs no callback threaded through it.

export type DocKey =
  | 'data-cube.extended-column.levels'
  | 'data-cube.grid-configuration.row-limit'
  | 'data-cube.column-configuration.kind'
  | 'data-cube.column-configuration.unit'
  | 'data-cube.column-configuration.display-value-as-link'
  | 'data-cube.column-configuration.missing-value-format';

type Block =
  | { readonly heading: string }
  | { readonly text: string }
  | { readonly note: string };

export interface DocEntry {
  readonly title: string;
  readonly blocks: readonly Block[];
}

export const DOCS: Readonly<Record<DocKey, DocEntry>> = {
  'data-cube.extended-column.levels': {
    title: 'Extended Column: Level',
    blocks: [
      { heading: 'Leaf Level' },
      { text: 'The value in the extended column is computed at the lowest (most granular) level.' },
      { heading: 'Group Level' },
      { text: "The value in the extended column is computed for each row in the table, no matter whether it's a leaf-level row or an aggregate." },
      { note: 'This is used for operating on aggregated values, e.g., computing a yield (total credits / total volume) or a percentage change. A group-level extended column does not support grouping (pivoting), because it is computed after those operations have been applied. Values are not aggregated; instead, the value on an aggregate row is computed from other aggregate values on the row.' },
    ],
  },
  'data-cube.grid-configuration.row-limit': {
    title: 'Grid Configuration: Row Limit',
    blocks: [{ text: 'Truncate result to the specified number of rows at every level.' }],
  },
  'data-cube.column-configuration.kind': {
    title: 'Column: Kind',
    blocks: [
      { text: 'A column represents either a `dimension` or a `measure`.' },
      { note: 'Column kind cannot be changed while the column is used in pivot.' },
      { heading: 'Dimension' },
      { text: 'Descriptions that help group and filter data. Typically associated with string columns where each value represents a category.' },
      { heading: 'Measure' },
      { text: 'Numeric values that detail or aggregate dimensions. Typically associated with numeric columns.' },
    ],
  },
  'data-cube.column-configuration.unit': {
    title: 'Column: Unit',
    blocks: [{ text: "The unit which would be displayed next to the measure value in each cell. Default to show after the value, use prefix '_' to display before the value, e.g. _$ would show value 4 as $4" }],
  },
  'data-cube.column-configuration.display-value-as-link': {
    title: 'Column: Display Value as Link',
    blocks: [{ text: 'Each value, if it is a valid URL, will be displayed as a hyperlink. The label of the hyperlink will be extracted from a configurable query parameter (`dataCube.linkLabel` by default), if no label value can be extracted, the whole link will be used as the label.' }],
  },
  'data-cube.column-configuration.missing-value-format': {
    title: 'Column: Missing Value Format',
    blocks: [{ text: "The missing value format is the text that will be displayed when the value of the cell is NULL.\nThe default value is the empty string ''." }],
  },
};

/** The (?) beside a field: asks the app to open this entry. */
export function docHint(doc: Document, key: DocKey): HTMLButtonElement {
  const b = doc.createElement('button');
  b.type = 'button';
  b.className = 'dc-doc-hint';
  b.textContent = '?';
  b.title = `About: ${DOCS[key].title}`;
  b.setAttribute('aria-label', `Documentation: ${DOCS[key].title}`);
  b.dataset['doc'] = key;
  b.addEventListener('click', (event) => {
    event.preventDefault();
    b.dispatchEvent(new (doc.defaultView as Window & typeof globalThis).CustomEvent(
      'dc-doc', { bubbles: true, detail: key }));
  });
  return b;
}

/** Fill a Documentation window with an entry. */
export function buildDocumentation(host: HTMLElement, key: DocKey): void {
  const doc = host.ownerDocument;
  const entry = DOCS[key];
  host.classList.add('dc-docs');
  const title = doc.createElement('div');
  title.className = 'dc-docs-title';
  title.textContent = entry.title;
  host.append(title);
  for (const block of entry.blocks) {
    const el = doc.createElement('heading' in block ? 'h4' : 'note' in block ? 'blockquote' : 'p');
    el.className = 'heading' in block ? 'dc-docs-heading' : 'note' in block ? 'dc-docs-note' : 'dc-docs-text';
    el.textContent = 'heading' in block ? block.heading : 'note' in block ? block.note : block.text;
    host.append(el);
  }
}

export function isDocKey(value: unknown): value is DocKey {
  return typeof value === 'string' && Object.hasOwn(DOCS, value);
}
