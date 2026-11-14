// THE PAGE'S PARAMETERS BAR (docs/DATACUBE_FLOW_MODE_2026_10_24.md M8: an analytic app's inputs, Alteryx's interface
// tools): below the page's bar, beside its filters -- each parameter an input of its type (a text, a number, a day, a
// tick, or a pick-list's choices), its value set as it is changed; its x to take it off; and Add a Parameter: its name,
// type, value and any choices. It holds no rule: the page (page/page-app.ts) judges and applies what it asks, by the page
// commands' one rule (page-parameters.ts), and paints it again; a refusal comes back in words.

import { PARAMETER_TYPES, type PageParameter, type ParameterType, type ParameterValue } from '../page-parameters.ts';
import { hasTimeOfDay, isBoolean, isDay, isDecimal, isNumeric } from '../../../engine-client/src/types.ts';
import { chromeIcon } from './tile-icons.ts';

export interface PageParameterBarOptions {
  /** A parameter set whole -- one new, or one's value changed: null when it was, else why not. */
  readonly set: (parameter: PageParameter) => Promise<string | null>;
  /** A parameter taken off. */
  readonly remove: (name: string) => void;
  /** Its Add form closed: the page shows the bar while it has a parameter. */
  readonly closed: () => void;
}

/** What a value typed is, read as its type's own JSON: a number's digits a number, a tick true or false, any other its
 *  text -- the page's rule judges it. */
function typedValue(type: ParameterType, text: string, checked: boolean): ParameterValue {
  if (isBoolean(type)) return checked;
  if (isNumeric(type) && !isDecimal(type)) return text.trim() === '' || !Number.isFinite(Number(text)) ? text : Number(text);
  return text;
}

export class PageParameterBar {
  readonly element: HTMLElement;
  readonly #doc: Document;
  readonly #options: PageParameterBarOptions;
  readonly #inputs: HTMLElement;
  readonly #form: HTMLFormElement;
  readonly #said: HTMLElement;

  constructor(doc: Document, options: PageParameterBarOptions) {
    this.#doc = doc;
    this.#options = options;
    const bar = doc.createElement('div');
    bar.className = 'dc-page-parameters';
    bar.setAttribute('role', 'toolbar');
    bar.setAttribute('aria-label', 'The page\'s parameters');
    const label = doc.createElement('span');
    label.className = 'dc-page-filters-label';
    label.textContent = 'Parameters';
    this.#inputs = doc.createElement('span');
    this.#inputs.className = 'dc-page-filters-chips';
    const add = doc.createElement('button');
    add.type = 'button';
    add.className = 'dc-page-filter-add dc-page-parameter-add';
    add.textContent = '+ Parameter';
    add.title = 'A value of the page any Pure on it reads as $name: a condition\'s, a formula\'s, a Pure box\'s';
    add.addEventListener('click', () => this.openForm());
    this.#form = doc.createElement('form');
    this.#form.className = 'dc-page-filter-form dc-page-parameter-form';
    this.#form.hidden = true;
    this.#said = doc.createElement('span');
    this.#said.className = 'dc-page-filter-said dc-page-parameter-said';
    this.#said.setAttribute('role', 'status');
    bar.append(label, this.#inputs, add, this.#form, this.#said);
    this.element = bar;
  }

  /** Whether its Add form is open. */
  get formOpen(): boolean {
    return !this.#form.hidden;
  }

  /** The page's parameters' names, as last painted: what Add refuses to take again. */
  #names: readonly string[] = [];

  /** The page's parameters now, each an input. */
  render(parameters: readonly PageParameter[]): void {
    this.#names = parameters.map((p) => p.name);
    this.#inputs.replaceChildren(...parameters.map((p) => this.#input(p)));
  }

  /** Set through the page: its refusal said beside the bar. */
  async #set(p: PageParameter): Promise<boolean> {
    const refused = await this.#options.set(p);
    this.#said.textContent = refused ?? '';
    return refused === null;
  }

  #input(p: PageParameter): HTMLElement {
    const doc = this.#doc;
    const chip = doc.createElement('label');
    chip.className = 'dc-page-filter dc-page-parameter';
    chip.dataset['parameter'] = p.name;
    chip.title = `$${p.name}, a ${p.type}: what any Pure on the page reads by that name`;
    const words = doc.createElement('span');
    words.className = 'dc-page-filter-words';
    words.textContent = p.label ?? p.name;
    let field: HTMLInputElement | HTMLSelectElement;
    if (p.choices !== undefined) {
      const s = doc.createElement('select');
      for (const [i, c] of p.choices.entries()) s.append(new Option(String(c), String(i), false, c === p.value));
      s.addEventListener('change', () => void this.#set({ ...p, value: p.choices![Number(s.value)]! }));
      field = s;
    } else {
      const i = doc.createElement('input');
      i.type = isBoolean(p.type) ? 'checkbox' : isNumeric(p.type) && !isDecimal(p.type) ? 'number'
        : isDay(p.type) ? 'date' : hasTimeOfDay(p.type) ? 'datetime-local' : 'text';
      if (i.type === 'checkbox') i.checked = p.value === true;
      else i.value = String(p.value);
      if (hasTimeOfDay(p.type)) i.step = '1';
      i.addEventListener('change', () => void (async () => {
        if (!await this.#set({ ...p, value: typedValue(p.type, i.value, i.checked) })) {
          if (i.type === 'checkbox') i.checked = p.value === true;
          else i.value = String(p.value);
        }
      })());
      field = i;
    }
    field.className = 'dc-page-parameter-value';
    field.setAttribute('aria-label', `${p.label ?? p.name}, $${p.name}`);
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-page-filter-remove';
    remove.append(chromeIcon(doc, 'close'));
    remove.setAttribute('aria-label', `Take off the parameter ${p.name}`);
    remove.addEventListener('click', (e) => {
      e.preventDefault();
      this.#options.remove(p.name);
    });
    chip.append(words, field, remove);
    return chip;
  }

  /** ADD A PARAMETER: its name, its type, its value and -- a pick-list's -- its choices, then the page sets it. */
  openForm(): void {
    const doc = this.#doc;
    const name = doc.createElement('input');
    name.className = 'dc-page-parameter-name';
    name.placeholder = 'name';
    name.setAttribute('aria-label', 'Its name, read as $name');
    const type = doc.createElement('select');
    type.className = 'dc-page-parameter-type';
    type.setAttribute('aria-label', 'Its type');
    for (const t of PARAMETER_TYPES) type.append(new Option(t, t));
    const value = doc.createElement('input');
    value.className = 'dc-page-parameter-first';
    value.placeholder = 'value';
    value.setAttribute('aria-label', 'Its value');
    const choices = doc.createElement('input');
    choices.className = 'dc-page-parameter-choices';
    choices.placeholder = 'choices, separated by commas (or none)';
    choices.setAttribute('aria-label', 'Its choices');
    const apply = doc.createElement('button');
    apply.type = 'submit';
    apply.className = 'dc-page-filter-apply';
    apply.textContent = 'Add';
    const cancel = doc.createElement('button');
    cancel.type = 'button';
    cancel.className = 'dc-page-filter-cancel';
    cancel.textContent = 'Cancel';
    const close = (): void => {
      this.#form.hidden = true;
      this.#form.replaceChildren();
      this.#options.closed();
    };
    cancel.addEventListener('click', close);
    this.#form.onsubmit = (e) => {
      e.preventDefault();
      const t = type.value as ParameterType;
      const listed = choices.value.split(',').map((c) => c.trim()).filter((c) => c !== '');
      const asValue = (text: string): ParameterValue => (isBoolean(t) ? text === 'true' ? true : text === 'false' ? false : text : typedValue(t, text, false));
      // Add makes a new one: one of a name the page has is set on its own input, never replaced from here (the roadmap
      // audit of 2026-10-26: its type and choices were swapped under what read it)
      if (this.#names.includes(name.value.trim())) {
        this.#said.textContent = `the page has a parameter '${name.value.trim()}' already: set its value on its input, or take it off first`;
        return;
      }
      void (async () => {
        if (await this.#set({ name: name.value.trim(), type: t, value: asValue(value.value),
          ...(listed.length > 0 ? { choices: listed.map(asValue) } : {}) })) close();
      })();
    };
    this.#form.replaceChildren(name, type, value, choices, apply, cancel);
    this.#form.hidden = false;
    this.element.hidden = false;
    name.focus();
  }
}
