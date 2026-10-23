// The saved cubes, as a window: find one, open it, delete it, or open a cube file someone handed
// you. Saving is its own window (ui/save-dialog.ts, the menu's Save and Save As). The STORE decides what a save is (upstream's rules,
// `cube-store.ts`); the HOST decides what opening one means (it owns the data: re-reading the
// file, rebuilding the cube) -- this window only asks.

import { SAVED_SORTS, savedSearch, type CubeStore, type LightDataCubeQuery, type QuerySearchSortBy } from '../cube-store.ts';

export interface CubeLibraryHost {
  /** The id of the saved cube on screen, when it came from the store: its row is marked. */
  currentId(): string | undefined;
  /** Open a saved cube. The host may come back through `ask` for the file. */
  open(id: string): Promise<void>;
  /** Open a cube file (the document's JSON text). */
  openText(text: string, fileName: string): Promise<void>;
  /** Forget what the host keeps for a deleted cube (its file handle). */
  forget(id: string): Promise<void>;
  /** Whether the cube on screen differs from what was saved (or first opened): opening another asks first. */
  dirty?(): boolean;
}

export class CubeLibrary {
  readonly #root: HTMLElement;
  readonly #doc: Document;
  readonly #store: CubeStore;
  readonly #host: CubeLibraryHost;
  readonly #search: HTMLInputElement;
  readonly #sort: HTMLSelectElement;
  readonly #list: HTMLElement;
  readonly #message: HTMLElement;
  readonly #ask: HTMLElement;
  #searching = 0;

  constructor(root: HTMLElement, store: CubeStore, host: CubeLibraryHost) {
    this.#root = root;
    this.#doc = root.ownerDocument;
    this.#store = store;
    this.#host = host;
    root.replaceChildren();
    root.classList.add('dc-lib');

    // -- the ask for a file (the host's, when opening needs one)
    this.#ask = this.#el('div', 'dc-lib-ask');
    this.#ask.hidden = true;

    // -- find
    const findRow = this.#el('div', 'dc-lib-find');
    this.#search = this.#el('input', 'dc-lib-search');
    this.#search.type = 'search';
    this.#search.placeholder = 'Search by name or id';
    this.#search.setAttribute('aria-label', 'Search saved cubes');
    let pending: ReturnType<typeof setTimeout> | undefined;
    this.#search.addEventListener('input', () => {
      if (pending !== undefined) clearTimeout(pending);
      pending = setTimeout(() => void this.refresh(), 200);
    });
    this.#sort = this.#el('select', 'dc-lib-sort');
    this.#sort.setAttribute('aria-label', 'Sort saved cubes');
    for (const [value, label] of SAVED_SORTS) {
      const o = this.#el('option');
      o.value = value;
      o.textContent = label;
      this.#sort.append(o);
    }
    this.#sort.addEventListener('change', () => void this.refresh());
    const importInput = this.#el('input', 'dc-lib-import');
    importInput.type = 'file';
    importInput.accept = '.json,application/json';
    importInput.hidden = true;
    importInput.addEventListener('change', () => {
      const file = importInput.files?.[0];
      importInput.value = '';
      if (file) void this.#run(`opening ${file.name}`, async () => this.#host.openText(await file.text(), file.name));
    });
    const importButton = this.#button('Open a cube file…', () => importInput.click());
    importButton.title = 'Open a cube file (JSON) exported from DataCube';
    findRow.append(this.#search, this.#sort, importButton, importInput);

    this.#message = this.#el('div', 'dc-lib-message');
    this.#message.setAttribute('role', 'status');
    this.#list = this.#el('div', 'dc-lib-list');
    this.#list.setAttribute('role', 'list');
    root.append(this.#ask, findRow, this.#message, this.#list);
    this.sync();
  }

  /** Bring the list in line with the cube on screen: its row is marked. */
  sync(): void {
    const current = this.#host.currentId();
    for (const row of this.#list.querySelectorAll<HTMLElement>('.dc-lib-row')) {
      row.classList.toggle('dc-lib-current', row.dataset['id'] === current);
    }
  }

  /** Say something in the window (what opening found, or what failed). */
  say(text: string, kind: 'ok' | 'warn' | 'error' = 'ok'): void {
    this.#message.textContent = text;
    this.#message.dataset['kind'] = kind;
  }

  /**
   * Ask for something in the window: the host's question when opening needs the user
   * (a file to choose, a click the browser requires). Undefined clears it.
   */
  ask(content: readonly (string | Node)[] | undefined): void {
    this.#ask.replaceChildren(...(content ?? []));
    this.#ask.hidden = content === undefined;
  }

  /** Re-read the saved cubes. */
  async refresh(): Promise<void> {
    const mine = (this.#searching += 1);
    const term = this.#search.value.trim();
    let found: LightDataCubeQuery[];
    try {
      // as many as the window lists (Python's ll.Page.saved() asks the same search, of as many as the store answers)
      found = await this.#store.search(savedSearch(term, this.#sort.value as QuerySearchSortBy, 50));
    } catch (e) {
      this.say(e instanceof Error ? e.message : String(e), 'error');
      return;
    }
    if (mine !== this.#searching) return; // a later search answered first
    this.#list.replaceChildren(...found.map((q) => this.#row(q)));
    if (found.length === 0) {
      const empty = this.#el('div', 'dc-lib-empty');
      empty.textContent = term ? `No saved cube matches "${term}"` : 'No saved cubes yet';
      this.#list.append(empty);
    }
    this.sync();
  }

  #row(q: LightDataCubeQuery): HTMLElement {
    const row = this.#el('div', 'dc-lib-row');
    row.setAttribute('role', 'listitem');
    row.dataset['id'] = q.id;
    if (q.id === this.#host.currentId()) row.classList.add('dc-lib-current');
    const name = this.#el('span', 'dc-lib-row-name');
    name.textContent = q.name;
    name.title = `id ${q.id}`;
    const when = this.#el('span', 'dc-lib-row-when');
    when.textContent = q.lastUpdatedAt !== undefined ? `saved ${ago(q.lastUpdatedAt)}` : '';
    const open = this.#button('Open', () => this.#openOver(row, q));
    const del = this.#button('Delete', () => this.#confirmDelete(row, q));
    row.append(name, when, open, del);
    return row;
  }

  /** Opening another cube over unsaved changes asks first, in place. */
  #openOver(row: HTMLElement, q: LightDataCubeQuery): void {
    const go = (): void => void this.#run(`opening ${q.name}`, () => this.#host.open(q.id));
    if (this.#host.dirty?.() !== true) {
      go();
      return;
    }
    const ask = this.#el('span', 'dc-lib-confirm');
    ask.textContent = `The cube on screen has unsaved changes. Open "${q.name}" anyway?`;
    const yes = this.#button('Open anyway', go);
    const no = this.#button('Cancel', () => void this.refresh());
    ask.append(yes, no);
    row.replaceChildren(ask);
  }

  /** Delete asks once, in place: typing is for irreversible acts on shared things. */
  #confirmDelete(row: HTMLElement, q: LightDataCubeQuery): void {
    const ask = this.#el('span', 'dc-lib-confirm');
    ask.textContent = `Delete "${q.name}"?`;
    const yes = this.#button('Delete', () => void this.#run(`deleting ${q.name}`, async () => {
      await this.#store.delete(q.id);
      await this.#host.forget(q.id);
      this.say(`deleted "${q.name}"`);
      await this.refresh();
    }));
    yes.classList.add('dc-lib-danger');
    const no = this.#button('Keep', () => void this.refresh());
    ask.append(yes, no);
    row.replaceChildren(ask);
  }

  /** One action at a time, its failure said in the window, never thrown past the user. */
  async #run(what: string, go: () => Promise<void>): Promise<void> {
    this.#root.classList.add('dc-lib-busy');
    this.say(`${what}…`);
    try {
      await go();
      if (this.#message.textContent === `${what}…`) this.say('');
    } catch (e) {
      this.say(e instanceof Error ? e.message : String(e), 'error');
    } finally {
      this.#root.classList.remove('dc-lib-busy');
    }
  }

  #el<K extends keyof HTMLElementTagNameMap>(tag: K, className?: string): HTMLElementTagNameMap[K] {
    const el = this.#doc.createElement(tag);
    if (className) el.className = className;
    return el;
  }

  #button(label: string, onClick: () => void): HTMLButtonElement {
    const b = this.#el('button', 'dc-lib-button');
    b.type = 'button';
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  }
}

/** "2 minutes ago", "yesterday": when a cube was saved, in words. */
export function ago(at: number, now: number = Date.now()): string {
  const s = Math.max(0, Math.round((now - at) / 1000));
  if (s < 60) return 'just now';
  const m = Math.round(s / 60);
  if (m < 60) return `${m} minute${m === 1 ? '' : 's'} ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h} hour${h === 1 ? '' : 's'} ago`;
  const d = Math.round(h / 24);
  if (d === 1) return 'yesterday';
  if (d < 30) return `${d} days ago`;
  return new Date(at).toISOString().slice(0, 10);
}
