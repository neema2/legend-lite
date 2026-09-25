// Form primitives, matching DataCube's own control vocabulary.
//
// legend-studio has FormTextInput, FormNumberInput, FormCheckbox,
// FormDropdownMenu, FormColorPickerButton, FormButton and FormBadge,
// and every editor panel is built from those seven. Having the same
// seven here is what makes the panels a translation rather than a
// re-invention, and it is why the panels below read as layout with
// almost no widget code in them.
//
// Three rules they all keep:
//
//  - A control reports a CHANGE, it does not own state. The panel
//    holds the draft; a control that kept its own copy would drift
//    from it the moment the draft was revised from elsewhere -- which
//    is exactly what happens when a column is chosen in a dropdown
//    and every other control must follow.
//  - A blank number input means ABSENT, not zero. DataCube shows
//    `(None)` for these, and the difference matters: no row limit and
//    a row limit of zero are opposite instructions.
//  - Every control is operable from the keyboard, because a grid
//    user's hands are already on the keyboard and a settings panel
//    that needs the mouse is a settings panel nobody adjusts.

const NONE = '(None)';

export interface Disposable {
  readonly element: HTMLElement;
}

function make<K extends keyof HTMLElementTagNameMap>(
  doc: Document,
  tag: K,
  className: string,
): HTMLElementTagNameMap[K] {
  const el = doc.createElement(tag);
  el.className = className;
  return el;
}

/** A labelled row: the label on the left, the control on the right. */
export function field(
  doc: Document,
  label: string,
  ...controls: readonly HTMLElement[]
): HTMLElement {
  const row = make(doc, 'div', 'dc-field');
  const l = make(doc, 'div', 'dc-field-label');
  l.textContent = label;
  row.append(l);
  const body = make(doc, 'div', 'dc-field-body');
  body.append(...controls);
  row.append(body);
  return row;
}

/** A titled block of fields. */
export function section(
  doc: Document,
  title: string,
  ...children: readonly HTMLElement[]
): HTMLElement {
  const s = make(doc, 'div', 'dc-section');
  if (title) {
    const h = make(doc, 'div', 'dc-section-title');
    h.textContent = title;
    s.append(h);
  }
  s.append(...children);
  return s;
}

export function textInput(
  doc: Document,
  value: string | undefined,
  onChange: (value: string | undefined) => void,
  options: { placeholder?: string; width?: number } = {},
): HTMLInputElement {
  const el = make(doc, 'input', 'dc-input');
  el.type = 'text';
  el.value = value ?? '';
  el.placeholder = options.placeholder ?? NONE;
  if (options.width !== undefined) el.style.width = `${options.width}px`;
  el.addEventListener('change', () => {
    const v = el.value.trim();
    onChange(v === '' ? undefined : v);
  });
  return el;
}

export function numberInput(
  doc: Document,
  value: number | undefined,
  onChange: (value: number | undefined) => void,
  options: { min?: number; max?: number; step?: number; width?: number } = {},
): HTMLInputElement {
  const el = make(doc, 'input', 'dc-input dc-input-number');
  el.type = 'number';
  el.value = value === undefined ? '' : String(value);
  el.placeholder = NONE;
  if (options.min !== undefined) el.min = String(options.min);
  if (options.max !== undefined) el.max = String(options.max);
  if (options.step !== undefined) el.step = String(options.step);
  el.style.width = `${options.width ?? 72}px`;
  el.addEventListener('change', () => {
    // Blank is ABSENT, not zero: "no limit" and "limit 0" are
    // opposite instructions and a coerced 0 silently picks the wrong
    // one.
    if (el.value.trim() === '') {
      onChange(undefined);
      return;
    }
    const n = Number(el.value);
    if (!Number.isFinite(n)) {
      onChange(undefined);
      return;
    }
    onChange(clamp(n, options.min, options.max));
  });
  return el;
}

function clamp(n: number, min?: number, max?: number): number {
  if (min !== undefined && n < min) return min;
  if (max !== undefined && n > max) return max;
  return n;
}

export function checkbox(
  doc: Document,
  label: string,
  checked: boolean | undefined,
  onChange: (checked: boolean) => void,
  options: { disabled?: boolean } = {},
): HTMLElement {
  const wrap = make(doc, 'label', 'dc-check');
  const box = make(doc, 'input', 'dc-check-box');
  box.type = 'checkbox';
  box.checked = Boolean(checked);
  box.disabled = Boolean(options.disabled);
  const text = make(doc, 'span', 'dc-check-label');
  text.textContent = label;
  box.addEventListener('change', () => onChange(box.checked));
  wrap.append(box, text);
  return wrap;
}

export interface Choice<T> {
  readonly value: T;
  readonly label: string;
}

/**
 * A dropdown.
 *
 * A native `select` rather than the popup menu legend-studio builds,
 * because the native one is already keyboard-operable, type-ahead
 * searchable and correctly positioned at the edge of a scrolled
 * panel -- three things the popup has to re-implement and which the
 * menu renderer here only needed for the grid's context menu, where
 * a native select cannot go.
 */
export function dropdown<T extends string>(
  doc: Document,
  value: T | undefined,
  choices: readonly Choice<T>[],
  onChange: (value: T | undefined) => void,
  options: { allowNone?: boolean; width?: number; disabled?: boolean } = {},
): HTMLSelectElement {
  const el = make(doc, 'select', 'dc-select');
  if (options.width !== undefined) el.style.width = `${options.width}px`;
  el.disabled = Boolean(options.disabled);
  if (options.allowNone) {
    const o = doc.createElement('option');
    o.value = '';
    o.textContent = NONE;
    el.append(o);
  }
  for (const c of choices) {
    const o = doc.createElement('option');
    o.value = c.value;
    o.textContent = c.label;
    el.append(o);
  }
  el.value = value ?? '';
  el.addEventListener('change', () => {
    onChange(el.value === '' ? undefined : (el.value as T));
  });
  return el;
}

/**
 * A colour swatch that opens the platform picker.
 *
 * `<input type=color>` has no absent state, so the swatch carries an
 * explicit clear: a column with no colour of its own must inherit the
 * grid's, and a picker that always reports a colour would pin every
 * column to whatever the picker happened to open on.
 */
export function colorPicker(
  doc: Document,
  value: string | undefined,
  onChange: (value: string | undefined) => void,
  options: { clearable?: boolean; title?: string } = {},
): HTMLElement {
  const wrap = make(doc, 'span', 'dc-color');
  const el = make(doc, 'input', 'dc-color-input');
  el.type = 'color';
  el.value = normaliseHex(value) ?? '#000000';
  if (options.title !== undefined) el.title = options.title;
  wrap.classList.toggle('dc-color-unset', value === undefined);
  el.addEventListener('change', () => onChange(el.value));
  wrap.append(el);
  if (options.clearable !== false) {
    const clear = make(doc, 'button', 'dc-color-clear');
    clear.type = 'button';
    clear.textContent = '×';
    clear.title = 'Clear colour';
    clear.addEventListener('click', () => {
      wrap.classList.add('dc-color-unset');
      onChange(undefined);
    });
    wrap.append(clear);
  }
  return wrap;
}

/** `#abc` and `abc` both mean `#aabbcc`; anything else means absent. */
export function normaliseHex(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const hex = value.startsWith('#') ? value.slice(1) : value;
  if (/^[0-9a-fA-F]{3}$/.test(hex)) {
    const [r, g, b] = hex;
    return `#${r}${r}${g}${g}${b}${b}`.toLowerCase();
  }
  if (/^[0-9a-fA-F]{6}$/.test(hex)) return `#${hex.toLowerCase()}`;
  return undefined;
}

export function button(
  doc: Document,
  label: string,
  onClick: () => void,
  options: { disabled?: boolean; title?: string; className?: string } = {},
): HTMLButtonElement {
  const el = make(doc, 'button', `dc-button ${options.className ?? ''}`.trim());
  el.type = 'button';
  el.textContent = label;
  el.disabled = Boolean(options.disabled);
  if (options.title !== undefined) el.title = options.title;
  el.addEventListener('click', onClick);
  return el;
}

/** A small "not implemented" marker, as DataCube's FormBadge_WIP. */
export function badge(doc: Document, text: string): HTMLElement {
  const el = make(doc, 'span', 'dc-badge');
  el.textContent = text;
  return el;
}

/**
 * A row of mutually exclusive toggle buttons.
 *
 * Bold / italic / underline are NOT this -- those are independent, so
 * they are separate toggles. Alignment and case are, which is why
 * they share one control and cannot both be set.
 */
export function toggleGroup<T extends string>(
  doc: Document,
  value: T | undefined,
  choices: readonly (Choice<T> & { title?: string })[],
  onChange: (value: T | undefined) => void,
  options: { allowNone?: boolean } = {},
): HTMLElement {
  const group = make(doc, 'div', 'dc-toggles');
  group.setAttribute('role', 'group');
  for (const c of choices) {
    const b = make(doc, 'button', 'dc-toggle');
    b.type = 'button';
    b.textContent = c.label;
    if (c.title !== undefined) b.title = c.title;
    const on = value === c.value;
    b.setAttribute('aria-pressed', String(on));
    b.classList.toggle('dc-on', on);
    b.addEventListener('click', () => {
      // Clicking the active choice clears it when a none state is
      // allowed, which is the only way to say "inherit" with a
      // control that has no empty slot.
      onChange(on && options.allowNone ? undefined : c.value);
    });
    group.append(b);
  }
  return group;
}

/** An independent on/off button, e.g. bold. */
export function toggle(
  doc: Document,
  label: string,
  on: boolean | undefined,
  onChange: (on: boolean) => void,
  options: { title?: string } = {},
): HTMLButtonElement {
  const b = make(doc, 'button', 'dc-toggle');
  b.type = 'button';
  b.textContent = label;
  if (options.title !== undefined) b.title = options.title;
  // The button keeps its OWN state: a panel does not rebuild on every
  // change, and a toggle that remembered only its first value showed
  // Bold off after turning it on, and sent "on" again when clicked.
  let state = Boolean(on);
  const paint = (): void => {
    b.setAttribute('aria-pressed', String(state));
    b.classList.toggle('dc-on', state);
  };
  paint();
  b.addEventListener('click', () => {
    state = !state;
    paint();
    onChange(state);
  });
  return b;
}
