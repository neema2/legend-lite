// THE PAGE'S FILTER BAR (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2): below the page's bar, on a page with a grid
// over a class -- each of the page's filters in words ("Trade: region = EMEA"), what it reaches said on it, a way to take
// it off; and Add a Filter: a class, one of its properties, an operator its type takes (the compiler's, as the grid's
// filter editor offers them) and a value. It holds no rule: the page (page/page-app.ts) applies what it asks, by the
// page commands' one rule, and paints it again.

import { OPERATORS, newCondition, operandKind, operatorsFor, toFilterNode } from '../filter-operators.ts';
import type { FilterNode, FilterOperator } from '../snapshot.ts';
import { chromeIcon } from './tile-icons.ts';

/** A filter as the bar shows it. */
export interface BarFilter {
  readonly id: string;
  /** In words (page-filters.ts filterWords). */
  readonly words: string;
  /** What it reaches, in words: `3 tiles`, and any tile it cannot, with why. */
  readonly reach: string;
  /** Made by a chart's click. */
  readonly fromChart: boolean;
}

/** A class the page's grids read, and its properties with their types (its class source's columns). */
export interface BarClass {
  readonly path: string;
  readonly name: string;
  readonly properties: readonly { readonly name: string; readonly type: string }[];
}

export interface PageFilterBarOptions {
  /** Take a filter off. */
  readonly remove: (id: string) => void;
  /** Set a new one: its class and condition. */
  readonly add: (cls: string, condition: FilterNode) => void;
}

const LABEL = new Map<string, string>(OPERATORS.map((o) => [o.op, o.label]));

export class PageFilterBar {
  readonly element: HTMLElement;
  readonly #doc: Document;
  readonly #options: PageFilterBarOptions;
  readonly #chips: HTMLElement;
  readonly #addButton: HTMLButtonElement;
  readonly #form: HTMLFormElement;
  #classes: readonly BarClass[] = [];

  constructor(doc: Document, options: PageFilterBarOptions) {
    this.#doc = doc;
    this.#options = options;
    const bar = doc.createElement('div');
    bar.className = 'dc-page-filters';
    bar.setAttribute('role', 'toolbar');
    bar.setAttribute('aria-label', 'The page\'s filters');
    bar.hidden = true;
    const label = doc.createElement('span');
    label.className = 'dc-page-filters-label';
    label.textContent = 'Filters';
    this.#chips = doc.createElement('span');
    this.#chips.className = 'dc-page-filters-chips';
    this.#addButton = doc.createElement('button');
    this.#addButton.type = 'button';
    this.#addButton.className = 'dc-page-filter-add';
    this.#addButton.textContent = '+ Filter';
    this.#addButton.title = 'Add a filter on a class\'s property: every grid over that class is filtered';
    this.#addButton.addEventListener('click', () => this.#openForm());
    this.#form = doc.createElement('form');
    this.#form.className = 'dc-page-filter-form';
    this.#form.hidden = true;
    bar.append(label, this.#chips, this.#addButton, this.#form);
    this.element = bar;
  }

  /** The page's filters and classes now: shown when either is (a page with no class grid and no filter has no bar). */
  render(filters: readonly BarFilter[], classes: readonly BarClass[]): void {
    this.#classes = classes;
    this.element.hidden = filters.length === 0 && classes.length === 0;
    this.#addButton.hidden = classes.length === 0;
    this.#chips.replaceChildren(...filters.map((f) => this.#chip(f)));
    if (classes.length === 0) this.#closeForm();
  }

  #chip(f: BarFilter): HTMLElement {
    const doc = this.#doc;
    const chip = doc.createElement('span');
    chip.className = `dc-page-filter${f.fromChart ? ' dc-page-filter-click' : ''}`;
    chip.dataset['filter'] = f.id;
    chip.title = `${f.words} -- ${f.reach}${f.fromChart ? ' (a chart\'s click: that chart\'s own grid is not filtered)' : ''}`;
    const words = doc.createElement('span');
    words.className = 'dc-page-filter-words';
    words.textContent = f.words;
    const reach = doc.createElement('span');
    reach.className = 'dc-page-filter-reach';
    reach.textContent = f.reach;
    const remove = doc.createElement('button');
    remove.type = 'button';
    remove.className = 'dc-page-filter-remove';
    remove.append(chromeIcon(doc, 'close'));
    remove.setAttribute('aria-label', `Take off the filter ${f.words}`);
    remove.addEventListener('click', () => this.#options.remove(f.id));
    chip.append(words, reach, remove);
    return chip;
  }

  #closeForm(): void {
    this.#form.hidden = true;
    this.#form.replaceChildren();
  }

  /** ADD A FILTER: a class, a property, an operator the property's type takes, a value -- then the page sets it. */
  #openForm(): void {
    const doc = this.#doc;
    const classes = this.#classes;
    if (classes.length === 0) return;
    const select = (cls: string, aria: string): HTMLSelectElement => {
      const s = doc.createElement('select');
      s.className = cls;
      s.setAttribute('aria-label', aria);
      return s;
    };
    const classSelect = select('dc-page-filter-class', 'Class');
    for (const c of classes) classSelect.append(new Option(c.name, c.path));
    classSelect.hidden = classes.length === 1;
    const propertySelect = select('dc-page-filter-property', 'Property');
    const operatorSelect = select('dc-page-filter-operator', 'Operator');
    const value = doc.createElement('input');
    value.className = 'dc-page-filter-value';
    value.setAttribute('aria-label', 'Value');
    const apply = doc.createElement('button');
    apply.type = 'submit';
    apply.className = 'dc-page-filter-apply';
    apply.textContent = 'Apply';
    const cancel = doc.createElement('button');
    cancel.type = 'button';
    cancel.className = 'dc-page-filter-cancel';
    cancel.textContent = 'Cancel';
    cancel.addEventListener('click', () => this.#closeForm());
    const said = doc.createElement('span');
    said.className = 'dc-page-filter-said';

    const cls = (): BarClass => {
      // the select offers these classes alone
      const chosen = classes.find((c) => c.path === classSelect.value);
      if (!chosen) throw new Error(`no class ${classSelect.value} on the page`);
      return chosen;
    };
    const typeOf = (p: string): string | undefined => cls().properties.find((x) => x.name === p)?.type;
    const fillOperators = (): void => {
      const type = typeOf(propertySelect.value);
      // the operators its type takes with a value or none: another column's is the grid's own filter editor's
      const ops = operatorsFor(type).filter((op) => operandKind(op) !== 'column');
      operatorSelect.replaceChildren(...ops.map((op) => new Option(LABEL.get(op) ?? op, op)));
      fillValue();
    };
    const fillValue = (): void => {
      const takes = operandKind(operatorSelect.value as FilterOperator);
      value.hidden = takes === 'none';
      value.placeholder = takes === 'list' ? 'values, separated by commas' : 'value';
    };
    const fillProperties = (): void => {
      propertySelect.replaceChildren(...cls().properties.map((p) => new Option(p.name, p.name)));
      fillOperators();
    };
    classSelect.addEventListener('change', fillProperties);
    propertySelect.addEventListener('change', fillOperators);
    operatorSelect.addEventListener('change', fillValue);
    fillProperties();

    this.#form.onsubmit = (e) => {
      e.preventDefault();
      const property = propertySelect.value;
      // the grid's filter editor's own conversion: a value typed by its property's type, a list split
      const condition = toFilterNode({
        ...newCondition(property), operator: operatorSelect.value as FilterOperator, text: value.value,
      }, typeOf);
      if (condition === null) {
        said.textContent = 'Give it a value.';
        return;
      }
      this.#closeForm();
      this.#options.add(cls().path, condition);
    };
    this.#form.replaceChildren(classSelect, propertySelect, operatorSelect, value, apply, cancel, said);
    this.#form.hidden = false;
    propertySelect.focus();
  }
}
