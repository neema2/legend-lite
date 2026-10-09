// WHERE THE ROWS COME FROM: the source picker (the user, 2026-10-01: "a beautiful section for
// files, beautiful section for examples, beautiful section for connecting to remote database,
// beautiful section for remote parquet/iceberg"). One window, a section per kind of source, each
// answered by the HOST: it reads the file, signs in to the warehouse, fetches the saved query.
// The picker only asks and shows; what a choice becomes (a grid's source) is the host's `open`.
//
// It carries `dc-app-floating`, so DataCube's tokens -- and the dark theme's -- apply to it.

import { UI_LOCALE } from '../../../engine-client/src/locale.ts';
import { focusedElement } from '../focus.ts';

/** The sections, in the order the window lists them. */
export type SectionId = 'files' | 'examples' | 'saved' | 'database' | 'remote';

/** A sample a person can open. */
export interface ExampleCard {
  readonly id: string;
  readonly name: string;
  readonly description: string;
  readonly rows?: number;
  readonly columns?: number;
  /** What is in it worth knowing: "dates", "nested JSON", "wide". */
  readonly tags?: readonly string[];
}

/** A saved query, as the store lists it. */
export interface SavedQueryCard {
  readonly id: string;
  readonly name: string;
  readonly owner?: string;
  readonly modified?: string;
  /** The project it runs against, as the store names it (a GAV). */
  readonly project?: string;
  /** Why it cannot be a cube's source, when it cannot. */
  readonly unusable?: string;
}

/** What a signed-in database offers. */
export interface DatabaseSession {
  readonly principal: string;
  readonly where: string;
  readonly objects: readonly DatabaseObject[];
}

export interface DatabaseObject {
  /** The warehouse catalog it is in: shown when the warehouse has more than one. */
  readonly catalog?: string;
  readonly schema: string;
  readonly name: string;
  readonly kind: string;
  readonly columns: number;
}

/** S3-style credentials for a remote file. */
export interface RemoteCredentials {
  readonly region?: string;
  readonly keyId?: string;
  readonly secret?: string;
  readonly endpoint?: string;
}

/** What the host can open, section by section; a section it leaves out is not shown. */
export interface PickerSections<T> {
  readonly files?: {
    /** The `accept` of the file input, e.g. `.csv,.parquet,.json`. */
    readonly accept: string;
    /** The formats, as a person names them. */
    readonly formats: readonly string[];
    /**
     * Browse through the host instead of the file input: one that can keep a HANDLE to the file
     * (to reopen it later from where it was picked) picks it, and the handle travels to `open`.
     * Undefined: they chose nothing.
     */
    pick?(): Promise<{ readonly file: File; readonly handle?: unknown } | undefined>;
    open(file: File, handle?: unknown): Promise<T>;
  };
  readonly examples?: {
    readonly list: readonly ExampleCard[];
    /** Generate `rows` rows of it and open them. */
    open(id: string, rows: number): Promise<T>;
    /** Generate it and hand the person the file instead (absent: no download offered). */
    download?(id: string, rows: number): void;
  };
  readonly saved?: {
    /** Set when the page has no query store: said instead of a search that cannot answer. */
    readonly unavailable?: string;
    /** Where the list comes from, for the person: "in this browser", "on localhost:8080". */
    readonly where?: string;
    /** Told when the store changes (another tab saved a query): the list is read again. Returns how to stop. */
    watch?(changed: () => void): () => void;
    /** Copy a share link to the query (the host owns the clipboard): what to say once it is copied. */
    copyLink?(id: string): Promise<string>;
    search(text: string, mineOnly: boolean): Promise<readonly SavedQueryCard[]>;
    open(id: string): Promise<T>;
  };
  readonly database?: {
    /** The address to offer, when the page knows one. */
    readonly url?: string;
    /** Already signed in: its tables are listed straight away. */
    readonly session?: DatabaseSession;
    signIn(url: string, user: string, password: string): Promise<DatabaseSession>;
    open(object: DatabaseObject): Promise<T>;
    /**
     * The table wanted (reopening a cube saved over it): opened as soon as the session lists it,
     * without a click -- or, not granted, said so.
     */
    readonly want?: { readonly schema: string; readonly name: string };
  };
  readonly remote?: {
    /** The format a URL reads as, or undefined when it cannot tell. */
    detect(url: string): string | undefined;
    open(url: string, credentials?: RemoteCredentials): Promise<T>;
    /** The URL to offer (reopening a cube saved over it). */
    readonly url?: string;
    /** Show the S3 credentials open: the file was refused without them. */
    readonly keys?: boolean;
  };
}

export interface PickSourceOptions<T> {
  /** Adding a grid beside the others, or opening in place of the cube. */
  readonly purpose: 'add' | 'open';
  readonly sections: PickerSections<T>;
  /** The section to start on; the first offered otherwise. */
  readonly start?: SectionId;
  /** Why the window opened, said under its title (reopening a cube that needs a sign-in, or keys). */
  readonly reason?: string;
}

const SECTIONS: readonly {
  readonly id: SectionId;
  readonly label: string;
  readonly hint: string;
  readonly icon: string;
}[] = [
  { id: 'files', label: 'Files', hint: 'CSV, Parquet or JSON from your computer', icon: 'M6 3h7l5 5v13H6zM13 3v5h5' },
  { id: 'examples', label: 'Examples', hint: 'Sample data to explore', icon: 'M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM13 13h7v7h-7z' },
  { id: 'saved', label: 'Saved queries', hint: 'Queries saved in Legend Query', icon: 'M6 3h12v18l-6-4-6 4z' },
  { id: 'database', label: 'Database', hint: 'Tables in a warehouse you sign in to', icon: 'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3' },
  { id: 'remote', label: 'Remote files', hint: 'Parquet, CSV or Iceberg by URL', icon: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM3 12h18M12 3c2.5 2.6 3.8 5.6 3.8 9s-1.3 6.4-3.8 9c-2.5-2.6-3.8-5.6-3.8-9S9.5 5.6 12 3z' },
];

function icon(doc: Document, path: string, cls = 'dc-picker-icon'): SVGSVGElement {
  const NS = 'http://www.w3.org/2000/svg';
  const svg = doc.createElementNS(NS, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('aria-hidden', 'true');
  svg.setAttribute('class', cls);
  const p = doc.createElementNS(NS, 'path');
  p.setAttribute('d', path);
  svg.append(p);
  return svg;
}

function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls: string, parent?: Element, text?: string): HTMLElementTagNameMap[K] {
  const e = doc.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  parent?.append(e);
  return e;
}

const count = (n: number): string => n.toLocaleString(UI_LOCALE);

/**
 * Ask the person where the rows come from. Resolves with what the host's `open` made of their
 * choice, or undefined when they closed the window without one.
 */
export function pickSource<T>(doc: Document, options: PickSourceOptions<T>): Promise<T | undefined> {
  return new Promise<T | undefined>((resolve) => {
    const offered = SECTIONS.filter((s) => options.sections[s.id] !== undefined);
    const backdrop = el(doc, 'div', 'dc-picker-backdrop', doc.body);
    const win = el(doc, 'div', 'dc-picker dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-picker-title');
    const before = focusedElement(doc) as HTMLElement | null;

    let done = false;
    /** What the section on show holds open (a watch on the store): let go when it goes. */
    let leave: (() => void)[] = [];
    const letGo = (): void => {
      for (const f of leave) f();
      leave = [];
    };
    const finish = (value: T | undefined): void => {
      if (done) return;
      done = true;
      letGo();
      doc.removeEventListener('keydown', onKey, true);
      backdrop.remove();
      before?.focus?.();
      resolve(value);
    };
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === 'Escape' && !busy) {
        e.preventDefault();
        finish(undefined);
      }
    };
    doc.addEventListener('keydown', onKey, true);
    backdrop.addEventListener('mousedown', (e) => {
      if (e.target === backdrop && !busy) finish(undefined);
    });

    // -- the header --
    const head = el(doc, 'div', 'dc-picker-head', win);
    const titles = el(doc, 'div', 'dc-picker-titles', head);
    const title = el(doc, 'h2', 'dc-picker-title', titles, options.purpose === 'add' ? 'Add a source' : 'Open a source');
    title.id = 'dc-picker-title';
    el(doc, 'p', 'dc-picker-subtitle', titles, options.reason ?? (options.purpose === 'add'
      ? 'A new grid over it joins the page, beside what is there.'
      : 'The cube opens over it, in place of what it shows now.'));
    const close = el(doc, 'button', 'dc-picker-close', head, '×') as HTMLButtonElement;
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(undefined); });

    // -- the sections --
    const body = el(doc, 'div', 'dc-picker-body', win);
    const nav = el(doc, 'div', 'dc-picker-nav', body);
    nav.setAttribute('role', 'tablist');
    nav.setAttribute('aria-orientation', 'vertical');
    const panel = el(doc, 'div', 'dc-picker-panel', body);
    panel.setAttribute('role', 'tabpanel');
    const status = el(doc, 'div', 'dc-picker-status', win);
    status.setAttribute('role', 'status');

    let busy = false;
    /** Run the host's `open`: the window says what it is doing, and an error stays in view. */
    const attempt = async (what: string, open: () => Promise<T>): Promise<void> => {
      if (busy) return;
      busy = true;
      win.classList.add('dc-busy');
      status.className = 'dc-picker-status dc-working';
      status.textContent = what;
      try {
        const made = await open();
        finish(made);
      } catch (e) {
        busy = false;
        win.classList.remove('dc-busy');
        status.className = 'dc-picker-status dc-failed';
        status.textContent = e instanceof Error ? e.message : String(e);
      }
    };
    const clearStatus = (): void => {
      status.className = 'dc-picker-status';
      status.textContent = '';
    };

    const tabs = new Map<SectionId, HTMLButtonElement>();
    const show = (id: SectionId): void => {
      if (busy) return;
      for (const [sid, tab] of tabs) {
        const on = sid === id;
        tab.setAttribute('aria-selected', String(on));
        tab.tabIndex = on ? 0 : -1;
      }
      letGo();
      panel.replaceChildren();
      clearStatus();
      paint[id](panel);
    };
    offered.forEach((s, i) => {
      const tab = el(doc, 'button', 'dc-picker-tab', nav) as HTMLButtonElement;
      tab.type = 'button';
      tab.setAttribute('role', 'tab');
      tab.dataset['section'] = s.id;
      tab.append(icon(doc, s.icon));
      const words = el(doc, 'span', 'dc-picker-tab-words', tab);
      el(doc, 'span', 'dc-picker-tab-label', words, s.label);
      el(doc, 'span', 'dc-picker-tab-hint', words, s.hint);
      tab.addEventListener('click', () => show(s.id));
      tab.addEventListener('keydown', (e) => {
        const step = e.key === 'ArrowDown' ? 1 : e.key === 'ArrowUp' ? -1 : 0;
        if (step === 0) return;
        e.preventDefault();
        const next = offered[(i + step + offered.length) % offered.length]!;
        show(next.id);
        tabs.get(next.id)?.focus();
      });
      tabs.set(s.id, tab);
    });

    const heading = (host: HTMLElement, text: string, lead: string): void => {
      el(doc, 'h3', 'dc-picker-heading', host, text);
      el(doc, 'p', 'dc-picker-lead', host, lead);
    };
    const button = (host: HTMLElement, text: string, cls = ''): HTMLButtonElement => {
      const b = el(doc, 'button', `dc-picker-button ${cls}`.trim(), host, text) as HTMLButtonElement;
      b.type = 'button';
      return b;
    };
    const field = (host: HTMLElement, label: string, type = 'text', value = ''): HTMLInputElement => {
      const row = el(doc, 'label', 'dc-picker-field', host);
      el(doc, 'span', 'dc-picker-field-label', row, label);
      const input = el(doc, 'input', 'dc-picker-input', row) as HTMLInputElement;
      input.type = type;
      input.value = value;
      return input;
    };
    const empty = (host: HTMLElement, text: string): void => {
      el(doc, 'p', 'dc-picker-empty', host, text);
    };

    const paint: Record<SectionId, (host: HTMLElement) => void> = {
      files: (host) => {
        const files = options.sections.files!;
        heading(host, 'Open a file', 'It is read in this browser and stays here: nothing is uploaded.');
        const drop = el(doc, 'div', 'dc-picker-drop', host);
        drop.tabIndex = 0;
        drop.setAttribute('role', 'button');
        drop.setAttribute('aria-label', 'Choose a file, or drop one here');
        drop.append(icon(doc, 'M12 15V4M7 9l5-5 5 5M5 15v4h14v-4', 'dc-picker-drop-icon'));
        el(doc, 'span', 'dc-picker-drop-main', drop, 'Drop a file here');
        el(doc, 'span', 'dc-picker-drop-or', drop, 'or');
        const browse = button(drop, 'Browse…', 'dc-primary');
        const chips = el(doc, 'div', 'dc-picker-chips', host);
        for (const f of files.formats) el(doc, 'span', 'dc-picker-chip', chips, f);
        const input = el(doc, 'input', 'dc-picker-file', host) as HTMLInputElement;
        input.type = 'file';
        input.accept = files.accept;
        input.hidden = true;
        const take = (file: File | undefined, handle?: unknown): void => {
          if (file) void attempt(`Reading ${file.name}…`, () => files.open(file, handle));
        };
        const choose = (): void => {
          if (!files.pick) { input.click(); return; }
          void files.pick().then((picked) => { if (picked) take(picked.file, picked.handle); });
        };
        browse.addEventListener('click', (e) => { e.stopPropagation(); choose(); });
        drop.addEventListener('click', choose);
        drop.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(); }
        });
        input.addEventListener('change', () => take(input.files?.[0]));
        drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('dc-over'); });
        drop.addEventListener('dragleave', () => drop.classList.remove('dc-over'));
        drop.addEventListener('drop', (e) => {
          e.preventDefault();
          drop.classList.remove('dc-over');
          take(e.dataTransfer?.files?.[0]);
        });
      },

      examples: (host) => {
        const examples = options.sections.examples!;
        heading(host, 'Start from an example', 'Generated in this browser: a quick way to try grouping, pivots and charts.');
        if (examples.list.length === 0) { empty(host, 'No examples are bundled with this page.'); return; }
        const grid = el(doc, 'div', 'dc-picker-cards', host);
        // THE CHOICE, at the foot: what it is, how many rows, and Open (or the file itself)
        const bar = el(doc, 'div', 'dc-picker-choice');
        const chosenName = el(doc, 'span', 'dc-picker-choice-name', bar);
        const rowsLabel = el(doc, 'label', 'dc-picker-choice-rows', bar);
        rowsLabel.append(doc.createTextNode('Rows '));
        const rows = el(doc, 'input', 'dc-picker-input dc-picker-rows', rowsLabel) as HTMLInputElement;
        rows.type = 'number';
        rows.min = '1';
        rows.step = '1';
        const actions = el(doc, 'span', 'dc-picker-choice-actions', bar);
        const download = examples.download ? button(actions, 'Download file', 'dc-quiet') : undefined;
        const openIt = button(actions, 'Open', 'dc-primary');
        let current: ExampleCard | undefined;
        const count0 = (): number => Math.max(1, Math.floor(Number(rows.value) || current?.rows || 1));
        const select = (ex: ExampleCard, card: HTMLElement): void => {
          current = ex;
          for (const c of grid.querySelectorAll('.dc-picker-card')) c.classList.toggle('dc-chosen', c === card);
          chosenName.textContent = ex.name;
          rows.value = String(ex.rows ?? 1000);
          if (!bar.isConnected) host.append(bar);
        };
        const go = (): void => {
          const ex = current;
          if (ex) void attempt(`Generating ${count(count0())} rows of ${ex.name}\u2026`, () => examples.open(ex.id, count0()));
        };
        openIt.addEventListener('click', go);
        rows.addEventListener('keydown', (e) => { if (e.key === 'Enter') go(); });
        download?.addEventListener('click', () => { if (current) examples.download!(current.id, count0()); });
        for (const ex of examples.list) {
          const card = el(doc, 'button', 'dc-picker-card', grid) as HTMLButtonElement;
          card.type = 'button';
          card.dataset['example'] = ex.id;
          el(doc, 'span', 'dc-picker-card-name', card, ex.name);
          el(doc, 'span', 'dc-picker-card-text', card, ex.description);
          const meta = [
            ...(ex.rows !== undefined ? [`${count(ex.rows)} rows`] : []),
            ...(ex.columns !== undefined ? [`${count(ex.columns)} columns`] : []),
          ];
          if (meta.length > 0) el(doc, 'span', 'dc-picker-card-meta', card, meta.join(' \u00b7 '));
          if (ex.tags && ex.tags.length > 0) {
            const tags = el(doc, 'span', 'dc-picker-chips', card);
            for (const t of ex.tags) el(doc, 'span', 'dc-picker-chip', tags, t);
          }
          card.addEventListener('click', () => select(ex, card));
          card.addEventListener('dblclick', () => { select(ex, card); go(); });
        }
      },

      saved: (host) => {
        const saved = options.sections.saved!;
        heading(host, 'Open a saved query', saved.where
          ? `Saved ${saved.where}. Its rows become the cube’s source, typed by the compiler.`
          : 'Its rows become the cube’s source, typed by the compiler.');
        if (saved.unavailable) {
          el(doc, 'div', 'dc-picker-note', host, saved.unavailable);
          return;
        }
        const bar = el(doc, 'div', 'dc-picker-searchbar', host);
        const search = el(doc, 'input', 'dc-picker-input dc-picker-search', bar) as HTMLInputElement;
        search.type = 'search';
        search.placeholder = 'Search saved queries';
        search.setAttribute('aria-label', 'Search saved queries');
        const mineRow = el(doc, 'label', 'dc-picker-check', bar);
        const mine = el(doc, 'input', '', mineRow) as HTMLInputElement;
        mine.type = 'checkbox';
        mineRow.append(doc.createTextNode(' Mine only'));
        const list = el(doc, 'div', 'dc-picker-list', host);
        list.setAttribute('role', 'list');
        let asked = 0;
        const refresh = async (): Promise<void> => {
          const n = ++asked;
          list.replaceChildren();
          el(doc, 'p', 'dc-picker-empty', list, 'Searching…');
          let found: readonly SavedQueryCard[];
          try {
            found = await saved.search(search.value.trim(), mine.checked);
          } catch (e) {
            if (n !== asked) return;
            list.replaceChildren();
            el(doc, 'div', 'dc-picker-note dc-bad', list, e instanceof Error ? e.message : String(e));
            return;
          }
          if (n !== asked) return;
          list.replaceChildren();
          if (found.length === 0) {
            empty(list, search.value.trim() ? 'No saved query matches.' : 'No saved queries yet.');
            return;
          }
          for (const q of found) {
            // the row opens it; beside it (a button cannot hold one), its share link
            const entry = el(doc, 'div', 'dc-picker-entry', list);
            const row = el(doc, 'button', 'dc-picker-row', entry) as HTMLButtonElement;
            row.type = 'button';
            row.setAttribute('role', 'listitem');
            row.dataset['query'] = q.id;
            const main = el(doc, 'span', 'dc-picker-row-main', row);
            el(doc, 'span', 'dc-picker-row-name', main, q.name);
            const meta = [q.owner, q.modified, q.project].filter((x): x is string => !!x);
            if (meta.length > 0) el(doc, 'span', 'dc-picker-row-meta', main, meta.join(' · '));
            if (q.unusable) {
              row.disabled = true;
              el(doc, 'span', 'dc-picker-row-why', row, q.unusable);
            } else {
              row.addEventListener('click', () => void attempt(`Opening ${q.name}…`, () => saved.open(q.id)));
            }
            if (saved.copyLink) {
              const copy = saved.copyLink;
              const link = el(doc, 'button', 'dc-picker-button dc-quiet dc-picker-row-link', entry, 'Copy link') as HTMLButtonElement;
              link.type = 'button';
              link.dataset['queryLink'] = q.id;
              link.title = `A link to “${q.name}”: it holds the query, never its rows`;
              link.addEventListener('click', () => {
                if (busy) return;
                status.className = 'dc-picker-status dc-working';
                status.textContent = `Copying a link to ${q.name}…`;
                void copy(q.id).then((said) => {
                  status.className = 'dc-picker-status dc-done';
                  status.textContent = said;
                }, (e: unknown) => {
                  status.className = 'dc-picker-status dc-failed';
                  status.textContent = e instanceof Error ? e.message : String(e);
                });
              });
            }
          }
        };
        let timer: ReturnType<typeof setTimeout> | undefined;
        search.addEventListener('input', () => {
          clearTimeout(timer);
          timer = setTimeout(() => void refresh(), 250);
        });
        mine.addEventListener('change', () => void refresh());
        if (saved.watch) leave.push(saved.watch(() => void refresh()));
        void refresh();
        search.focus();
      },

      database: (host) => {
        const db = options.sections.database!;
        let session = db.session;
        const paintSession = (): void => {
          host.replaceChildren();
          if (!session) {
            heading(host, 'Connect to a database', 'Sign in to a warehouse: Live runs there as you, and Snap copies your rows into this tab.');
            const form = el(doc, 'form', 'dc-picker-form', host);
            const url = field(form, 'Address', 'url', db.url ?? '');
            url.placeholder = 'https://warehouse.example.com';
            const user = field(form, 'User');
            user.autocomplete = 'username';
            const password = field(form, 'Password', 'password');
            password.autocomplete = 'current-password';
            const go = el(doc, 'div', 'dc-picker-actions', form);
            const submit = el(doc, 'button', 'dc-picker-button dc-primary', go, 'Sign in') as HTMLButtonElement;
            submit.type = 'submit';
            form.addEventListener('submit', (e) => {
              e.preventDefault();
              if (busy) return;
              busy = true;
              win.classList.add('dc-busy');
              status.className = 'dc-picker-status dc-working';
              status.textContent = 'Signing in…';
              void db.signIn(url.value.trim(), user.value.trim(), password.value).then((s) => {
                busy = false;
                win.classList.remove('dc-busy');
                password.value = '';
                session = s;
                clearStatus();
                paintSession();
              }, (err: unknown) => {
                busy = false;
                win.classList.remove('dc-busy');
                status.className = 'dc-picker-status dc-failed';
                status.textContent = err instanceof Error ? err.message : String(err);
              });
            });
            (db.url ? user : url).focus();
            return;
          }
          const s = session;
          // the table wanted: opened straight away once listed, or said not to be granted
          if (db.want) {
            const w = db.want;
            const found = s.objects.find((o) => o.schema === w.schema && o.name === w.name);
            if (found) {
              // the list stays beneath it: an open that fails leaves the person choosing
              queueMicrotask(() => void attempt(`Opening ${w.schema}.${w.name}…`, () => db.open(found)));
            } else {
              queueMicrotask(() => {
                status.className = 'dc-picker-status dc-failed';
                status.textContent = `${w.schema}.${w.name} is not granted to ${s.principal} at ${s.where}: ask its owner, or sign in as someone else.`;
              });
            }
          }
          heading(host, 'Choose a table', `Signed in as ${s.principal} at ${s.where}: what you may read.`);
          const bar = el(doc, 'div', 'dc-picker-searchbar', host);
          const filter = el(doc, 'input', 'dc-picker-input dc-picker-search', bar) as HTMLInputElement;
          filter.type = 'search';
          filter.placeholder = `Filter ${count(s.objects.length)} ${s.objects.length === 1 ? 'table' : 'tables'}`;
          filter.setAttribute('aria-label', 'Filter tables');
          const other = button(bar, 'Sign in as someone else', 'dc-quiet');
          other.addEventListener('click', () => { session = undefined; clearStatus(); paintSession(); });
          const list = el(doc, 'div', 'dc-picker-list', host);
          list.setAttribute('role', 'list');
          const rows = (): void => {
            list.replaceChildren();
            const q = filter.value.trim().toLowerCase();
            const shown = s.objects.filter((o) => `${o.schema}.${o.name}`.toLowerCase().includes(q));
            if (s.objects.length === 0) { empty(list, 'Nothing is granted to you yet.'); return; }
            if (shown.length === 0) { empty(list, 'No table matches.'); return; }
            for (const o of shown) {
              const row = el(doc, 'button', 'dc-picker-row', list) as HTMLButtonElement;
              row.type = 'button';
              row.setAttribute('role', 'listitem');
              row.dataset['object'] = `${o.schema}.${o.name}`;
              const main = el(doc, 'span', 'dc-picker-row-main', row);
              const name = el(doc, 'span', 'dc-picker-row-name', main);
              el(doc, 'span', 'dc-picker-row-schema', name, `${o.catalog === undefined ? '' : `${o.catalog} · `}${o.schema}.`);
              name.append(doc.createTextNode(o.name));
              el(doc, 'span', 'dc-picker-row-meta', main, `${count(o.columns)} columns`);
              el(doc, 'span', `dc-picker-badge dc-${o.kind === 'view' ? 'view' : 'table'}`, row, o.kind);
              row.addEventListener('click', () => void attempt(`Opening ${o.schema}.${o.name}…`, () => db.open(o)));
            }
          };
          filter.addEventListener('input', rows);
          rows();
          filter.focus();
        };
        paintSession();
      },

      remote: (host) => {
        const remote = options.sections.remote!;
        heading(host, 'Read a remote file', 'DuckDB reads it straight from the URL, fetching only the parts a query needs.');
        const form = el(doc, 'form', 'dc-picker-form', host);
        const row = el(doc, 'div', 'dc-picker-urlrow', form);
        const url = el(doc, 'input', 'dc-picker-input dc-picker-url', row) as HTMLInputElement;
        url.type = 'url';
        url.placeholder = 'https://example.com/trades.parquet, s3://bucket/table/metadata';
        url.setAttribute('aria-label', 'URL');
        const kind = el(doc, 'span', 'dc-picker-badge dc-unknown', row, 'format');
        const badge = (): void => {
          const f = url.value.trim() ? remote.detect(url.value.trim()) : undefined;
          kind.textContent = f ?? 'format';
          kind.className = `dc-picker-badge ${f ? 'dc-known' : 'dc-unknown'}`;
        };
        url.addEventListener('input', badge);
        if (remote.url) {
          url.value = remote.url;
          badge();
        }
        const more = el(doc, 'details', 'dc-picker-more', form);
        if (remote.keys) more.open = true;
        el(doc, 'summary', '', more, 'S3 credentials (optional)');
        const creds = el(doc, 'div', 'dc-picker-creds', more);
        const region = field(creds, 'Region');
        const keyId = field(creds, 'Access key id');
        const secret = field(creds, 'Secret', 'password');
        const endpoint = field(creds, 'Endpoint');
        el(doc, 'p', 'dc-picker-fine', form, 'The server must allow this page to read it (CORS); Delta tables are not read.');
        const go = el(doc, 'div', 'dc-picker-actions', form);
        const submit = el(doc, 'button', 'dc-picker-button dc-primary', go, 'Open') as HTMLButtonElement;
        submit.type = 'submit';
        form.addEventListener('submit', (e) => {
          e.preventDefault();
          const where = url.value.trim();
          if (!where) { url.focus(); return; }
          const c: RemoteCredentials = {
            ...(region.value.trim() ? { region: region.value.trim() } : {}),
            ...(keyId.value.trim() ? { keyId: keyId.value.trim() } : {}),
            ...(secret.value ? { secret: secret.value } : {}),
            ...(endpoint.value.trim() ? { endpoint: endpoint.value.trim() } : {}),
          };
          void attempt(`Reading ${where}…`, () => remote.open(where, Object.keys(c).length > 0 ? c : undefined));
        });
        if (remote.keys) region.focus();
        else url.focus();
      },
    };

    const first = options.start && options.sections[options.start] ? options.start : offered[0]?.id;
    if (first) {
      show(first);
      const focused = focusedElement(doc);
      if (focused === doc.body || !win.contains(focused)) tabs.get(first)?.focus();
    } else {
      empty(panel, 'This page cannot open any source.');
    }
  });
}
