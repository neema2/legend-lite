// A searchable selector, as upstream's (legend-art CustomSelectorInput over react-select, the dark variant;
// census UPSTREAM_STUDIO_LOOK.md 9.5, and 1.3 for the setup page's): a text field that filters its options as it is
// typed in, a clear button once something is chosen, the dropdown indicator, and a menu under it.

import { icon } from '../../../legend-art/src/icon.ts';
import { clear, h } from './dom.ts';

export interface SelectorOption {
  readonly value: string;
  readonly label: string;
  /** a second line, muted (a project's coordinates) */
  readonly detail?: string;
}

export interface Selector {
  readonly el: HTMLElement;
  /** What is chosen now. */
  value(): string | undefined;
  /** New options, a new placeholder, and whether it can be used at all. */
  reset(options: readonly SelectorOption[], placeholder: string, disabled?: boolean): void;
  /** Choose `value` (one of the options) as if picked. */
  choose(value: string | undefined): void;
}

export function selector(testId: string, onChange: (value: string | undefined) => void): Selector {
  let options: readonly SelectorOption[] = [];
  let chosen: SelectorOption | undefined;
  const input = h('input', { class: 'selector-input__input', 'data-testid': testId, autocomplete: 'off', spellcheck: 'false' });
  const menu = h('div', { class: 'selector-input__menu', role: 'listbox', 'data-testid': `${testId}-menu` });
  const clearButton = h('button', { class: 'selector-input__clear', title: 'Clear', tabindex: '-1', onclick: () => { choose(undefined); input.focus(); } }, icon('times'));
  const el = h('div', { class: 'selector-input' },
    h('div', { class: 'selector-input__control' }, input, clearButton,
      h('button', { class: 'selector-input__indicator', tabindex: '-1', title: 'Show options', onclick: () => { if (menu.isConnected) close(); else open(); input.focus(); } }, icon('caretDown'))),
    menu);

  const close = (): void => menu.remove();
  const draw = (): void => {
    clear(menu);
    const text = input.value.trim().toLowerCase();
    const shown = chosen !== undefined && input.value === chosen.label ? options
      : options.filter((o) => o.label.toLowerCase().includes(text) || (o.detail ?? '').toLowerCase().includes(text));
    if (shown.length === 0) menu.append(h('div', { class: 'selector-input__no-match' }, 'No match found'));
    for (const o of shown) {
      menu.append(h('button', {
        class: `selector-input__option${o.value === chosen?.value ? ' selector-input__option--selected' : ''}`, role: 'option', 'data-id': o.value, tabindex: '-1',
        onmousedown: (e: Event) => e.preventDefault(),     // keep the field's focus
        onclick: () => { choose(o.value); close(); },
      }, h('span', { class: 'selector-input__option__label' }, o.label), o.detail ? h('span', { class: 'selector-input__option__detail' }, o.detail) : null));
    }
  };
  const open = (): void => {
    if (input.disabled) return;
    draw();
    el.append(menu);
  };
  const choose = (value: string | undefined): void => {
    chosen = options.find((o) => o.value === value);
    input.value = chosen?.label ?? '';
    el.classList.toggle('selector-input--has-value', chosen !== undefined);
    onChange(chosen?.value);
  };

  input.addEventListener('focus', open);
  input.addEventListener('input', () => { if (!menu.isConnected) el.append(menu); draw(); });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') close();
    if (e.key === 'Enter') {
      const first = menu.querySelector<HTMLButtonElement>('.selector-input__option');
      if (first) first.click();
    }
  });
  input.addEventListener('blur', () => {
    close();
    input.value = chosen?.label ?? '';      // a half-typed search does not stay as if it were the choice
  });

  return {
    el,
    value: () => chosen?.value,
    reset(next, placeholder, disabled = false) {
      options = next;
      chosen = undefined;
      input.value = '';
      input.placeholder = placeholder;
      input.disabled = disabled;
      el.classList.toggle('selector-input--disabled', disabled);
      el.classList.remove('selector-input--has-value');
      close();
    },
    choose,
  };
}
