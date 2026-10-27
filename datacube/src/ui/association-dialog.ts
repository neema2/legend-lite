// CREATE AN ASSOCIATION… (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1, §15.6a), as a window of its own in the
// Create a Class window's style: two of the project's classes, each joined on a property or a key only; how many of each
// the other has, in two sentences ("Each Trade has at most one Customer, reached as customer"), preset from the facts
// (its `on` unique over its rows) when its rows are in this tab, asked when they are not; the names the route gives the
// ends shown until a person changes them; the suggested pairs offered; the files it writes. The HOST asks the route,
// reads the facts and saves; this window asks and shows.

import { focusedElement } from '../focus.ts';
import type { AssociationAction, Joinable, ModelAnswer, Suggestion } from '../model-writer.ts';
import { isPureName } from '../model-names.ts';
import type { CreatedClass } from './class-dialog.ts';
import { closeMark } from './tile-icons.ts';

export interface AssociationDialogOptions {
  /** The page's project, as the window names it. */
  readonly project: string;
  /** The project's classes, and what each can be joined on (the route's answer). */
  readonly joinable: readonly Joinable[];
  readonly suggestions: readonly Suggestion[];
  /** A class to start one end with (a grid's own, from its menu). */
  readonly first?: string;
  /** Whether `on` of `cls` is unique over its class's rows: undefined when its rows are not in this tab. */
  unique(cls: string, on: string): Promise<boolean | undefined>;
  preview(association: AssociationAction): Promise<ModelAnswer>;
  create(association: AssociationAction): Promise<CreatedClass>;
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

/** A class path's own name: what follows its package. */
const simpleName = (cls: string): string => cls.slice(cls.lastIndexOf(':') + 1);

/** Open the window. Resolves with what was created, or undefined when it was closed without creating. */
export function associationDialog(doc: Document, o: AssociationDialogOptions): Promise<CreatedClass | undefined> {
  return new Promise((resolve) => {
    const backdrop = el(doc, 'div', 'dc-picker-backdrop', doc.body);
    const win = el(doc, 'div', 'dc-picker dc-save dc-class dc-association dc-app-floating', backdrop);
    win.setAttribute('role', 'dialog');
    win.setAttribute('aria-modal', 'true');
    win.setAttribute('aria-labelledby', 'dc-association-title');
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
    el(doc, 'h2', 'dc-picker-title', titles, 'Create an Association').id = 'dc-association-title';
    el(doc, 'p', 'dc-picker-subtitle', titles, `Between two of the page's classes, in ${o.project}: equal on a property or key of each.`);
    const close = closeMark(el(doc, 'button', 'dc-picker-close', head, '×') as HTMLButtonElement);
    close.type = 'button';
    close.setAttribute('aria-label', 'Close');
    close.addEventListener('click', () => { if (!busy) finish(); });

    const body = el(doc, 'div', 'dc-save-body', win);
    // THE SUGGESTED PAIRS: each fills both ends
    const suggested = el(doc, 'div', 'dc-association-suggested', body);
    suggested.hidden = o.suggestions.length === 0;
    el(doc, 'span', 'dc-picker-field-label', suggested, 'Suggested');

    // THE TWO ENDS: a class, and what it is joined on
    const ends = el(doc, 'div', 'dc-association-ends', body);
    const side = (label: string): { cls: HTMLSelectElement; on: HTMLSelectElement } => {
      const card = el(doc, 'div', 'dc-save-card dc-association-end', ends);
      el(doc, 'div', 'dc-save-card-title', card, label);
      const cls = el(doc, 'select', 'dc-picker-input dc-association-class', card) as HTMLSelectElement;
      cls.setAttribute('aria-label', `${label}: class`);
      for (const j of o.joinable) {
        const option = el(doc, 'option', '', cls, simpleName(j.class)) as HTMLOptionElement;
        option.value = j.class;
      }
      el(doc, 'span', 'dc-picker-field-label', card, 'joined on');
      const on = el(doc, 'select', 'dc-picker-input dc-association-on', card) as HTMLSelectElement;
      on.setAttribute('aria-label', `${label}: joined on`);
      return { cls, on };
    };
    const a = side('One class');
    const b = side('The other');
    const fillOn = (s: { cls: HTMLSelectElement; on: HTMLSelectElement }, keep?: string): void => {
      const keys = o.joinable.find((j) => j.class === s.cls.value)?.keys ?? [];
      s.on.replaceChildren();
      // nothing joined on until it is chosen (or a suggestion chose it): no join is made of a first key by default
      el(doc, 'option', '', s.on, 'choose…').value = '';
      for (const k of keys) {
        const option = el(doc, 'option', '', s.on, `${k.name}${k.shown ? '' : ' (key)'}`) as HTMLOptionElement;
        option.value = k.name;
      }
      if (keep !== undefined && keys.some((k) => k.name === keep)) s.on.value = keep;
    };

    // HOW MANY, in two sentences: the end each names is the OTHER class, reached from this one
    const sentences = el(doc, 'div', 'dc-save-card dc-association-sentences', body);
    const sentence = (): { text: HTMLElement; many: HTMLSelectElement; reached: HTMLElement; name: HTMLInputElement; why: HTMLElement } => {
      const line = el(doc, 'div', 'dc-association-sentence', sentences);
      const text = el(doc, 'span', '', line);
      const many = el(doc, 'select', 'dc-picker-input dc-association-many', line) as HTMLSelectElement;
      el(doc, 'option', '', many, 'how many?').value = '';
      for (const h of HOW_MANY) (el(doc, 'option', '', many, h.label) as HTMLOptionElement).value = h.value;
      const reached = el(doc, 'span', '', line);
      const name = el(doc, 'input', 'dc-picker-input dc-association-name', line) as HTMLInputElement;
      name.type = 'text';
      name.spellcheck = false;
      const why = el(doc, 'span', 'dc-association-why', line);
      return { text, many, reached, name, why };
    };
    // the first sentence names the end of `b`, reached from `a`; the second the end of `a`, reached from `b`
    const toB = sentence();
    const toA = sentence();

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

    /** The sentences as the ends say them now. */
    const words = (): void => {
      const an = simpleName(a.cls.value);
      const bn = simpleName(b.cls.value);
      toB.text.textContent = `Each ${an} has `;
      toB.reached.textContent = ` ${bn}, reached as `;
      toA.text.textContent = `Each ${bn} has `;
      toA.reached.textContent = ` ${an}, reached as `;
    };
    /** How many, preset from the facts: an end on a unique key is to one, else to many; unknown, asked. */
    let factsAsked = 0;
    const facts = async (): Promise<void> => {
      const mine = (factsAsked += 1);
      if (a.on.value === '' || b.on.value === '') return;
      const [ua, ub] = await Promise.all([o.unique(a.cls.value, a.on.value), o.unique(b.cls.value, b.on.value)]);
      if (mine !== factsAsked || done) return;
      const preset = (s: typeof toB, unique: boolean | undefined, cls: string, on: string): void => {
        if (s.many.dataset['chosen'] === 'yes') return;
        s.many.value = unique === undefined ? '' : unique ? '[0..1]' : '[*]';
        s.why.textContent = unique === undefined ? `(${simpleName(cls)}'s rows are not in this tab: say how many)`
          : `(${simpleName(cls)}.${on} is ${unique ? '' : 'not '}unique)`;
      };
      preset(toB, ub, b.cls.value, b.on.value);
      preset(toA, ua, a.cls.value, a.on.value);
    };

    const action = (): AssociationAction | string => {
      if (a.cls.value === '' || b.cls.value === '') return 'Choose the two classes.';
      if (a.on.value === '' || b.on.value === '') return 'Choose what each is joined on.';
      if (toB.many.value === '' || toA.many.value === '') return 'Say how many of each the other has.';
      for (const n of [toB.name.value.trim(), toA.name.value.trim()]) {
        if (n !== '' && !isPureName(n)) return `A name is a Pure name: '${n}'.`;
      }
      const end = (cls: string, on: string, s: typeof toB) => ({
        class: cls, on, multiplicity: s.many.value, ...(s.name.value.trim() !== '' ? { name: s.name.value.trim() } : {}),
      });
      return { ends: [end(b.cls.value, b.on.value, toB), end(a.cls.value, a.on.value, toA)] };
    };

    const drawFiles = (answer: ModelAnswer): void => {
      fileList.replaceChildren();
      for (const c of answer.changes) {
        const li = el(doc, 'li', 'dc-class-file', fileList);
        const line = el(doc, 'button', 'dc-class-file-line', li) as HTMLButtonElement;
        line.type = 'button';
        el(doc, 'span', `dc-class-change dc-class-${c.type.toLowerCase()}`, line, c.type === 'CREATE' ? 'new' : 'changed');
        el(doc, 'span', 'dc-class-path', line, c.path);
        const pure = el(doc, 'pre', 'dc-class-pure', li, c.pureCode ?? '');
        pure.hidden = true;
        line.addEventListener('click', () => { pure.hidden = !pure.hidden; });
      }
      // the names the route gave the ends, shown until a person names them
      const made = answer.associations[0];
      if (made) {
        toB.name.placeholder = made.ends[0]?.name ?? '';
        toA.name.placeholder = made.ends[1]?.name ?? '';
      }
    };

    let asked = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const over = (): boolean => busy || created !== undefined || done;
    const ask = async (): Promise<void> => {
      if (over()) return;
      const c = action();
      const mine = (asked += 1);
      if (typeof c === 'string') {
        primary.disabled = true;
        fileList.replaceChildren();
        say(c, '');
        return;
      }
      say('Writing the association…', 'working');
      try {
        const answer = await o.preview(c);
        if (mine !== asked || over()) return;
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
    const endChanged = (s: typeof a): void => {
      fillOn(s);
      words();
      void facts().then(changed);
    };
    a.cls.addEventListener('change', () => endChanged(a));
    b.cls.addEventListener('change', () => endChanged(b));
    for (const s of [a, b]) s.on.addEventListener('change', () => void facts().then(changed));
    for (const s of [toB, toA]) {
      s.many.addEventListener('change', () => { s.many.dataset['chosen'] = 'yes'; s.why.textContent = ''; changed(); });
      s.name.addEventListener('input', changed);
    }
    const choose = (pair: Suggestion): void => {
      // a suggestion's ends: [the class reached, its property], [the class with the key, its key]
      a.cls.value = pair.ends[1].class;
      fillOn(a, pair.ends[1].on);
      b.cls.value = pair.ends[0].class;
      fillOn(b, pair.ends[0].on);
      for (const s of [toB, toA]) delete s.many.dataset['chosen'];
      words();
      void facts().then(changed);
    };
    for (const pair of o.suggestions) {
      const chip = el(doc, 'button', 'dc-picker-button dc-association-chip', suggested,
        `${simpleName(pair.ends[1].class)}.${pair.ends[1].on} = ${simpleName(pair.ends[0].class)}.${pair.ends[0].on}`) as HTMLButtonElement;
      chip.type = 'button';
      chip.addEventListener('click', () => choose(pair));
    }

    const run = async (): Promise<void> => {
      if (busy) return;
      if (created) { finish(); return; }
      const c = action();
      if (typeof c === 'string') { say(c, 'failed'); return; }
      clearTimeout(timer);
      busy = true;
      win.classList.add('dc-busy');
      say('Creating the association…', 'working');
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

    // THE START: the first suggestion, else the grid's class and another
    if (o.suggestions[0] && (o.first === undefined || o.suggestions[0].ends.some((e) => e.class === o.first))) {
      choose(o.suggestions[0]);
    } else {
      if (o.first !== undefined) a.cls.value = o.first;
      const other = o.joinable.find((j) => j.class !== a.cls.value);
      if (other) b.cls.value = other.class;
      fillOn(a);
      fillOn(b);
      words();
      void facts().then(changed);
    }
  });
}
