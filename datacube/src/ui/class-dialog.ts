// CREATE A CLASS… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1), as a window of its own in the Save window's style:
// the class's name and package (and, the first time, its project's name); each of the grid's columns a property, a
// key only (kept in the mapping for an association to join on, not shown on the class) or left out, each property's
// name; the files it writes, each one's Pure shown on asking; Create. The HOST asks the route and saves (it owns the
// planner and the model home); this window asks and shows, and asks the route again as the choices change.

import { focusedElement } from '../focus.ts';
import type { ColumnChoice, ColumnRole, MadeProperty } from '../model-writer.ts';
import { isPackage, isPureName } from '../model-names.ts';
import type { PureChange } from '../../../sdlc-client/src/wire.ts';
import { closeMark } from './tile-icons.ts';

/** What a person chose: the request's names and each column's choice. */
export interface ClassChoice {
  /** The project's name, the first time (its project is made with the class); undefined after. */
  readonly projectName?: string;
  readonly package: string;
  readonly className: string;
  readonly documentation?: string;
  readonly columns: Readonly<Record<string, ColumnChoice>>;
}

/** What the window shows of the route's answer: the files it writes, the columns as the class takes them, its notes. */
export interface ClassPreview {
  readonly changes: readonly PureChange[];
  readonly properties: readonly MadeProperty[];
  readonly notes: readonly string[];
}

export interface ClassDialogOptions {
  /** The grid's source, as its tab says it: what the class is made of. */
  readonly sourceName: string;
  readonly className: string;
  readonly package: string;
  /** The page's project: its name, and whether it is made with this class (the page's first). */
  readonly project: { readonly name: string; readonly isNew: boolean };
  /** The route's answer for `choice`, its files not yet written: shown as the person chooses. */
  preview(choice: ClassChoice): Promise<ClassPreview>;
  /** Write the class; what to say once it is, or a refusal (shown in the window, which stays open). */
  create(choice: ClassChoice): Promise<CreatedClass>;
}

/** What creating answered: the class, where it went, and where to open it. */
export interface CreatedClass {
  readonly className: string;
  /** "datacube:trades, workspace datacube" */
  readonly where: string;
  /** Studio's editor on that workspace, on this page's own address. */
  readonly studio?: string;
  readonly notes: readonly string[];
}

function el<K extends keyof HTMLElementTagNameMap>(doc: Document, tag: K, cls: string, parent?: Element, text?: string): HTMLElementTagNameMap[K] {
  const e = doc.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  parent?.append(e);
  return e;
}

const ROLES: readonly { readonly value: ColumnRole; readonly label: string }[] = [
  { value: 'property', label: 'Property' },
  { value: 'key', label: 'Key only' },
  { value: 'omit', label: 'Left out' },
];

/** Open the window. Resolves with what was created, or undefined when it was closed without creating. */
export function classDialog(doc: Document, o: ClassDialogOptions): Promise<CreatedClass | undefined> {
  return new Promise((resolve) => {
    const backdrop = el(doc, 'div', 'dc-picker-backdrop', doc.body);
    const win = el(doc, 'div', 'dc-picker dc-save dc-class dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-class-title');
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
    const title = el(doc, 'h2', 'dc-picker-title', titles, 'Create a Class');
    title.id = 'dc-class-title';
    el(doc, 'p', 'dc-picker-subtitle', titles, o.project.isNew
      ? `Of ${o.sourceName}'s rows as this grid shows them, in a new project in this browser, where Studio opens it.`
      : `Of ${o.sourceName}'s rows as this grid shows them, in the page's project, ${o.project.name}.`);
    const close = closeMark(el(doc, 'button', 'dc-picker-close', head, '×') as HTMLButtonElement);
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(); });

    const body = el(doc, 'div', 'dc-save-body', win);
    const names = el(doc, 'div', 'dc-class-names', body);
    const field = (label: string, value: string, cls: string): HTMLInputElement => {
      const f = el(doc, 'label', `dc-picker-field ${cls}`, names);
      el(doc, 'span', 'dc-picker-field-label', f, label);
      const input = el(doc, 'input', 'dc-picker-input', f) as HTMLInputElement;
      input.type = 'text';
      input.value = value;
      input.spellcheck = false;
      return input;
    };
    const className = field('Class', o.className, 'dc-class-name');
    const pkg = field('Package', o.package, 'dc-class-package');
    // the page's classes are in one package, its store's: chosen with the first, kept after
    pkg.disabled = !o.project.isNew;
    const project = o.project.isNew ? field('Project', o.project.name, 'dc-class-project') : undefined;
    const docField = el(doc, 'label', 'dc-picker-field dc-class-doc', body);
    el(doc, 'span', 'dc-picker-field-label', docField, 'Documentation');
    const documentation = el(doc, 'input', 'dc-picker-input', docField) as HTMLInputElement;
    documentation.type = 'text';
    documentation.placeholder = 'optional: what one of these is';

    // THE COLUMNS: drawn from the route's first answer (its names and types are the compiler's), kept as chosen after
    const card = el(doc, 'div', 'dc-save-card dc-class-columns', body);
    el(doc, 'div', 'dc-save-card-title', card, 'Its columns');
    const table = el(doc, 'table', 'dc-class-table', card);
    const headRow = el(doc, 'tr', '', el(doc, 'thead', '', table));
    for (const h of ['Column', 'Type', 'As', 'Property']) el(doc, 'th', '', headRow, h);
    const rows = el(doc, 'tbody', '', table);
    const choices = new Map<string, { role: HTMLSelectElement; name: HTMLInputElement; type: HTMLElement; named: boolean }>();

    const files = el(doc, 'div', 'dc-save-card dc-class-files', body);
    el(doc, 'div', 'dc-save-card-title', files, 'What it writes');
    const fileList = el(doc, 'ul', 'dc-class-file-list', files);
    const notes = el(doc, 'p', 'dc-save-where dc-class-notes', body);
    notes.hidden = true;

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

    /** What the window says now, or why it cannot be asked yet (a name that is no Pure name). */
    const choice = (): ClassChoice | string => {
      const cls = className.value.trim();
      const p = pkg.value.trim();
      if (!isPureName(cls)) return 'A class is named as Pure names it: letters, digits and _, not led by a digit.';
      if (!isPackage(p)) return 'A package is Pure names joined by :: (trades, or trades::desk).';
      if (project && project.value.trim() === '') return 'Give the project a name.';
      const columns: Record<string, ColumnChoice> = {};
      for (const [column, c] of choices) {
        const as = c.role.value as ColumnRole;
        const name = c.name.value.trim();
        if (as !== 'omit' && c.named && !isPureName(name)) return `The property for ${column} is named as Pure names it.`;
        columns[column] = { ...(as !== 'property' ? { as } : {}), ...(as !== 'omit' && c.named ? { name } : {}) };
      }
      const docText = documentation.value.trim();
      return {
        ...(project ? { projectName: project.value.trim() } : {}),
        package: p,
        className: cls,
        ...(docText !== '' ? { documentation: docText } : {}),
        columns,
      };
    };

    const drawColumns = (answer: ClassPreview): void => {
      for (const p of answer.properties) {
        const known = choices.get(p.column);
        const typeText = p.type === undefined ? '' : `${p.type.replace(/^.*::/, '')}${p.multiplicity ?? ''}`;
        if (known) {
          known.type.textContent = typeText;
          if (!known.named && p.property !== undefined) known.name.value = p.property;
          continue;
        }
        const tr = el(doc, 'tr', '', rows);
        el(doc, 'td', 'dc-class-column', tr, p.column);
        const type = el(doc, 'td', 'dc-class-type', tr, typeText);
        const role = el(doc, 'select', 'dc-picker-input dc-class-role', el(doc, 'td', '', tr)) as HTMLSelectElement;
        role.setAttribute('aria-label', `${p.column} is`);
        for (const r of ROLES) {
          const option = el(doc, 'option', '', role, r.label) as HTMLOptionElement;
          option.value = r.value;
        }
        role.value = p.as;
        const name = el(doc, 'input', 'dc-picker-input dc-class-property', el(doc, 'td', '', tr)) as HTMLInputElement;
        name.type = 'text';
        name.spellcheck = false;
        name.setAttribute('aria-label', `${p.column}'s property`);
        name.value = p.property ?? '';
        const entry = { role, name, type, named: false };
        role.addEventListener('change', () => {
          name.disabled = role.value === 'omit';
          tr.classList.toggle('dc-class-left-out', role.value === 'omit');
          changed();
        });
        name.addEventListener('input', () => { entry.named = true; changed(); });
        choices.set(p.column, entry);
      }
    };

    const drawFiles = (answer: ClassPreview): void => {
      fileList.replaceChildren();
      for (const c of answer.changes) {
        const li = el(doc, 'li', 'dc-class-file', fileList);
        const line = el(doc, 'button', 'dc-class-file-line', li) as HTMLButtonElement;
        line.type = 'button';
        line.setAttribute('aria-expanded', 'false');
        el(doc, 'span', `dc-class-change dc-class-${c.type.toLowerCase()}`, line, c.type === 'CREATE' ? 'new' : 'changed');
        el(doc, 'span', 'dc-class-path', line, c.path.replace(/__.*$/, ''));
        const pure = el(doc, 'pre', 'dc-class-pure', li, c.pureCode ?? '');
        pure.hidden = true;
        line.addEventListener('click', () => {
          pure.hidden = !pure.hidden;
          line.setAttribute('aria-expanded', String(!pure.hidden));
        });
      }
      notes.hidden = answer.notes.length === 0;
      notes.textContent = answer.notes.join(' ');
    };

    // ASKED AGAIN AS THE CHOICES CHANGE: the latest ask wins, an older answer arriving later is dropped
    let asked = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // a create in flight or done is the window's last word: no preview is asked or drawn over it (the audit's S1)
    const over = (): boolean => busy || created !== undefined || done;
    const ask = async (): Promise<void> => {
      if (over()) return;
      const c = choice();
      const mine = (asked += 1);
      if (typeof c === 'string') {
        primary.disabled = true;
        say(c, 'failed');
        return;
      }
      say('Writing the class…', 'working');
      try {
        const answer = await o.preview(c);
        if (mine !== asked || over()) return;
        drawColumns(answer);
        drawFiles(answer);
        primary.disabled = false;
        say('', '');
      } catch (e) {
        if (mine !== asked || over()) return;
        primary.disabled = true;
        say(e instanceof Error ? e.message : String(e), 'failed');
      }
    };
    const changed = (): void => {
      if (created) return;
      clearTimeout(timer);
      timer = setTimeout(() => void ask(), 250);
    };
    for (const input of [className, pkg, documentation, ...(project ? [project] : [])]) {
      input.addEventListener('input', changed);
    }

    const run = async (): Promise<void> => {
      if (busy) return;
      if (created) { finish(); return; }
      const c = choice();
      if (typeof c === 'string') { say(c, 'failed'); return; }
      // no preview after this: one waiting is dropped, and one in flight is drawn by no one (`over`)
      clearTimeout(timer);
      busy = true;
      win.classList.add('dc-busy');
      say(`Creating ${c.className}…`, 'working');
      try {
        created = await o.create(c);
        busy = false;
        win.classList.remove('dc-busy');
        // CREATED: said in the window, with where it went and the way to Studio; Done closes it
        say(`Created ${created.className} in ${created.where}.`, 'ok');
        if (created.studio) {
          const open = el(doc, 'a', 'dc-class-studio', status, 'Open it in Studio') as HTMLAnchorElement;
          open.href = created.studio;
          open.target = '_blank';
          open.rel = 'noopener';
        }
        if (created.notes.length > 0) {
          notes.hidden = false;
          notes.textContent = created.notes.join(' ');
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
    className.focus();
    className.select();
  });
}
