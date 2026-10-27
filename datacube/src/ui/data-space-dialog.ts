// CREATE A DATA SPACE… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1, §15.6a): the whole page as one front door, in
// one go -- its name, title and description; each of the page's grids, a class already or made one in the same window
// (named here; its columns as Create a Class makes them by default); the associations the model suggests between them,
// as checkboxes, how many of each preset from the facts; the files it writes, all one revision. The HOST asks the route
// (createDataSpace, atomic), reads the facts and saves; this window asks and shows.

import { focusedElement } from '../focus.ts';
import type { AssociationAction, ModelAnswer, Suggestion } from '../model-writer.ts';
import { isPureName } from '../model-names.ts';
import type { CreatedClass } from './class-dialog.ts';
import { closeMark } from './tile-icons.ts';

/** A grid of the page, as the window offers it: a class already, or one to make. */
export interface DataSpaceGrid {
  readonly id: string;
  readonly label: string;
  /** Its class, when its rows were made one already. */
  readonly cls?: string;
  /** The class its source's name suggests, when it is to be made. */
  readonly className: string;
}

/** What a person chose. */
export interface DataSpaceChoice {
  readonly projectName?: string;
  readonly name: string;
  readonly title?: string;
  readonly description?: string;
  /** The grids to make classes of, each with its class's name. */
  readonly made: readonly { readonly id: string; readonly className: string }[];
  readonly associations: readonly AssociationAction[];
}

export interface DataSpaceDialogOptions {
  readonly project: { readonly name: string; readonly isNew: boolean };
  readonly name: string;
  readonly grids: readonly DataSpaceGrid[];
  /** Whether `on` of `cls` is unique over its class's rows (a class to be made: its grid's); undefined when unknown. */
  unique(cls: string, on: string): Promise<boolean | undefined>;
  preview(choice: DataSpaceChoice): Promise<ModelAnswer>;
  create(choice: DataSpaceChoice): Promise<CreatedClass>;
}

function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls: string, parent?: Element, text?: string): HTMLElementTagNameMap[K] {
  const e = doc.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  parent?.append(e);
  return e;
}

const HOW_MANY: readonly { readonly value: string; readonly label: string }[] = [
  { value: '[0..1]', label: 'at most one' },
  { value: '[1]', label: 'exactly one' },
  { value: '[*]', label: 'many' },
  { value: '[1..*]', label: 'one or more' },
];
const simpleName = (cls: string): string => cls.slice(cls.lastIndexOf(':') + 1);
const keyOf = (s: Suggestion): string => s.ends.map((e) => `${e.class}.${e.on}`).join('=');

/** Open the window. Resolves with what was created, or undefined when it was closed without creating. */
export function dataSpaceDialog(doc: Document, o: DataSpaceDialogOptions): Promise<CreatedClass | undefined> {
  return new Promise((resolve) => {
    const backdrop = el(doc, 'div', 'dc-picker-backdrop', doc.body);
    const win = el(doc, 'div', 'dc-picker dc-save dc-class dc-data-space dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-data-space-title');
    const before = focusedElement(doc) as HTMLElement | null;
    let busy = false;
    let done = false;
    let created: CreatedClass | undefined;
    const finish = (): void => {
      if (done) return;
      done = true;
      doc.removeEventListener('keydown', onKey, true);
      backdrop.remove();
      before?.focus?.();
      resolve(created);
    };
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === 'Escape' && !busy) { e.preventDefault(); finish(); }
    };
    doc.addEventListener('keydown', onKey, true);
    backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop && !busy) finish(); });

    const head = el(doc, 'div', 'dc-picker-head', win);
    const titles = el(doc, 'div', 'dc-picker-titles', head);
    el(doc, 'h2', 'dc-picker-title', titles, 'Create a Data Space').id = 'dc-data-space-title';
    el(doc, 'p', 'dc-picker-subtitle', titles, o.project.isNew
      ? 'The page\'s model, as one front door Query lists: its classes and their associations, in a new project in this browser.'
      : `The page's model, as one front door Query lists: its classes and their associations, in ${o.project.name}.`);
    const close = closeMark(el(doc, 'button', 'dc-picker-close', head, '×') as HTMLButtonElement);
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(); });

    const body = el(doc, 'div', 'dc-save-body', win);
    const names = el(doc, 'div', 'dc-class-names', body);
    const field = (label: string, value: string, cls: string, parent: Element = names): HTMLInputElement => {
      const f = el(doc, 'label', `dc-picker-field ${cls}`, parent);
      el(doc, 'span', 'dc-picker-field-label', f, label);
      const input = el(doc, 'input', 'dc-picker-input', f) as HTMLInputElement;
      input.type = 'text';
      input.value = value;
      input.spellcheck = false;
      return input;
    };
    const name = field('Data space', o.name, 'dc-data-space-name');
    const title = field('Title', '', 'dc-data-space-title-field');
    const project = o.project.isNew ? field('Project', o.project.name, 'dc-class-project') : undefined;
    const description = field('Description', '', 'dc-class-doc', body);
    description.placeholder = 'optional: what this model is about';

    // ITS CLASSES: each grid, a class already or one to make (named here)
    const classes = el(doc, 'div', 'dc-save-card dc-class-columns', body);
    el(doc, 'div', 'dc-save-card-title', classes, 'Its classes');
    const table = el(doc, 'table', 'dc-class-table', classes);
    const rows = el(doc, 'tbody', '', table);
    const toMake = new Map<string, HTMLInputElement>();
    for (const g of o.grids) {
      const tr = el(doc, 'tr', '', rows);
      el(doc, 'td', 'dc-class-column', tr, g.label);
      const cell = el(doc, 'td', '', tr);
      if (g.cls !== undefined) {
        el(doc, 'span', 'dc-class-type', cell, `${simpleName(g.cls)} (a class already)`);
      } else {
        const input = el(doc, 'input', 'dc-picker-input dc-class-property', cell) as HTMLInputElement;
        input.type = 'text';
        input.value = g.className;
        input.setAttribute('aria-label', `${g.label}'s class`);
        toMake.set(g.id, input);
      }
    }

    // ITS ASSOCIATIONS: the model's suggestions, offered; a chosen one kept while it is chosen
    const relations = el(doc, 'div', 'dc-save-card dc-association-sentences', body);
    el(doc, 'div', 'dc-save-card-title', relations, 'Its associations');
    const offered = el(doc, 'div', '', relations);
    const none = el(doc, 'p', 'dc-save-where', relations, 'None suggested: a key named after another class and its property relates them.');
    type Row = { readonly s: Suggestion; readonly box: HTMLInputElement; readonly many: [HTMLSelectElement, HTMLSelectElement] };
    const shown = new Map<string, Row>();
    const drawSuggestions = (suggestions: readonly Suggestion[]): void => {
      const latest = new Map(suggestions.map((s) => [keyOf(s), s]));
      for (const [k, r] of [...shown]) {
        if (!latest.has(k) && !r.box.checked) {
          r.box.closest('.dc-association-sentence')?.remove();
          shown.delete(k);
        }
      }
      for (const [k, s] of latest) {
        if (shown.has(k)) continue;
        const line = el(doc, 'label', 'dc-association-sentence', offered);
        const box = el(doc, 'input', '', line) as HTMLInputElement;
        box.type = 'checkbox';
        // ends: [the class reached, its property], [the class with the key, its key]
        const [reached, keyed] = s.ends;
        el(doc, 'span', '', line, ` ${simpleName(keyed.class)}.${keyed.on} = ${simpleName(reached.class)}.${reached.on}: each ${simpleName(keyed.class)} has `);
        const many = (): HTMLSelectElement => {
          const m = el(doc, 'select', 'dc-picker-input dc-association-many', line) as HTMLSelectElement;
          el(doc, 'option', '', m, 'how many?').value = '';
          for (const h of HOW_MANY) (el(doc, 'option', '', m, h.label) as HTMLOptionElement).value = h.value;
          m.addEventListener('change', changed);
          return m;
        };
        const toReached = many();
        el(doc, 'span', '', line, ` ${simpleName(reached.class)}; each ${simpleName(reached.class)} has `);
        const toKeyed = many();
        el(doc, 'span', '', line, ` ${simpleName(keyed.class)}`);
        box.addEventListener('change', changed);
        shown.set(k, { s, box, many: [toReached, toKeyed] });
        void Promise.all([o.unique(reached.class, reached.on), o.unique(keyed.class, keyed.on)]).then(([ur, uk]) => {
          if (toReached.value === '' && ur !== undefined) toReached.value = ur ? '[0..1]' : '[*]';
          if (toKeyed.value === '' && uk !== undefined) toKeyed.value = uk ? '[0..1]' : '[*]';
        });
      }
      none.hidden = shown.size > 0;
    };

    const files = el(doc, 'div', 'dc-save-card dc-class-files', body);
    el(doc, 'div', 'dc-save-card-title', files, 'What it writes');
    const fileList = el(doc, 'ul', 'dc-class-file-list', files);
    const status = el(doc, 'div', 'dc-picker-status', win);
    status.setAttribute('role', 'status');
    const foot = el(doc, 'div', 'dc-save-foot', win);
    const cancel = el(doc, 'button', 'dc-picker-button dc-quiet', foot, 'Cancel') as HTMLButtonElement;
    cancel.type = 'button';
    cancel.addEventListener('click', () => { if (!busy) finish(); });
    el(doc, 'span', 'dc-save-spacer', foot).setAttribute('aria-hidden', 'true');
    const primary = el(doc, 'button', 'dc-picker-button dc-primary', foot, 'Create') as HTMLButtonElement;
    primary.type = 'button';
    primary.disabled = true;
    const say = (text: string, kind: 'working' | 'failed' | 'ok' | ''): void => {
      status.className = `dc-picker-status${kind ? ` dc-${kind}` : ''}`;
      status.textContent = text;
    };

    const choice = (): DataSpaceChoice | string => {
      if (!isPureName(name.value.trim())) return 'A data space is named as Pure names it: letters, digits and _.';
      if (project && project.value.trim() === '') return 'Give the project a name.';
      const made: { id: string; className: string }[] = [];
      for (const [id, input] of toMake) {
        if (!isPureName(input.value.trim())) return `A class is named as Pure names it: '${input.value.trim()}'.`;
        made.push({ id, className: input.value.trim() });
      }
      const associations: AssociationAction[] = [];
      for (const r of shown.values()) {
        if (!r.box.checked) continue;
        if (r.many[0].value === '' || r.many[1].value === '') return 'Say how many of each a chosen association relates.';
        associations.push({ ends: [
          { class: r.s.ends[0].class, on: r.s.ends[0].on, multiplicity: r.many[0].value },
          { class: r.s.ends[1].class, on: r.s.ends[1].on, multiplicity: r.many[1].value },
        ] });
      }
      const t = title.value.trim();
      const d = description.value.trim();
      return {
        ...(project ? { projectName: project.value.trim() } : {}),
        name: name.value.trim(),
        ...(t !== '' ? { title: t } : {}),
        ...(d !== '' ? { description: d } : {}),
        made,
        associations,
      };
    };

    const drawFiles = (answer: ModelAnswer): void => {
      fileList.replaceChildren();
      for (const c of answer.changes) {
        const li = el(doc, 'li', 'dc-class-file', fileList);
        const line = el(doc, 'button', 'dc-class-file-line', li) as HTMLButtonElement;
        line.type = 'button';
        el(doc, 'span', `dc-class-change dc-class-${c.type.toLowerCase()}`, line, c.type === 'CREATE' ? 'new' : 'changed');
        el(doc, 'span', 'dc-class-path', line, c.path.replace(/__.*$/, ''));
        const pure = el(doc, 'pre', 'dc-class-pure', li, c.pureCode ?? '');
        pure.hidden = true;
        line.addEventListener('click', () => { pure.hidden = !pure.hidden; });
      }
    };

    let asked = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const over = (): boolean => busy || created !== undefined || done;
    const ask = async (): Promise<void> => {
      if (over()) return;
      const c = choice();
      const mine = (asked += 1);
      if (typeof c === 'string') {
        primary.disabled = true;
        say(c, '');
        return;
      }
      say('Writing the data space…', 'working');
      try {
        const answer = await o.preview(c);
        if (mine !== asked || over()) return;
        drawSuggestions(answer.suggestions);
        drawFiles(answer);
        primary.disabled = false;
        say('', '');
      } catch (e) {
        if (mine !== asked || over()) return;
        primary.disabled = true;
        say(e instanceof Error ? e.message : String(e), 'failed');
      }
    };
    function changed(): void {
      if (created) return;
      clearTimeout(timer);
      timer = setTimeout(() => void ask(), 250);
    }
    for (const input of [name, title, description, ...(project ? [project] : []), ...toMake.values()]) {
      input.addEventListener('input', changed);
    }

    const run = async (): Promise<void> => {
      if (busy) return;
      if (created) { finish(); return; }
      const c = choice();
      if (typeof c === 'string') { say(c, 'failed'); return; }
      clearTimeout(timer);
      busy = true;
      win.classList.add('dc-busy');
      say(`Creating ${c.name}…`, 'working');
      try {
        created = await o.create(c);
        busy = false;
        win.classList.remove('dc-busy');
        say(`Created ${created.className} in ${created.where}.`, 'ok');
        if (created.studio) {
          const open = el(doc, 'a', 'dc-class-studio', status, 'Open it in Studio') as HTMLAnchorElement;
          open.href = created.studio;
          open.target = '_blank';
          open.rel = 'noopener';
        }
        for (const input of win.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')) input.disabled = true;
        primary.textContent = 'Done';
        cancel.hidden = true;
      } catch (e) {
        busy = false;
        win.classList.remove('dc-busy');
        say(e instanceof Error ? e.message : String(e), 'failed');
      }
    };
    primary.addEventListener('click', () => void run());
    void ask();
    name.focus();
    name.select();
  });
}
