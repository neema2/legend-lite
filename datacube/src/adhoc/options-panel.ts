// The Ad Hoc Options window: how Zoom In behaves, where parents sit,
// indentation, suppression, and navigating without data. A DRAFT until
// OK, as the Settings window is; Cancel drops it.

import type { AdHocOptions } from './state.ts';
import { DEFAULT_OPTIONS } from './state.ts';

export interface OptionsPanelOptions {
  readonly values: AdHocOptions;
  /** The options that changed. */
  readonly onOk: (patch: Partial<AdHocOptions>) => void;
  readonly onClose: () => void;
}

type Choice<K extends keyof AdHocOptions> = readonly (readonly [AdHocOptions[K], string])[];

const ZOOM: Choice<'zoomLevel'> = [['next', 'Next Level'], ['all', 'All Levels'], ['bottom', 'Bottom Level']];
const ANCESTOR: Choice<'ancestorPosition'> = [['top', 'Top'], ['bottom', 'Bottom']];
const INDENT: Choice<'indentation'> = [['subitems', 'Subitems'], ['totals', 'Totals'], ['none', 'None']];

type Flag = {
  [K in keyof AdHocOptions]: AdHocOptions[K] extends boolean ? K : never
}[keyof AdHocOptions];

export function buildOptionsPanel(host: HTMLElement, options: OptionsPanelOptions): void {
  const doc = host.ownerDocument;
  let draft: AdHocOptions = { ...options.values };
  host.classList.add('dc-adhoc-options');
  const list = doc.createElement('div');
  list.className = 'dc-adhoc-options-list';
  const footer = doc.createElement('div');
  footer.className = 'dc-adhoc-options-footer';
  host.append(list, footer);

  const section = (title: string): HTMLFieldSetElement => {
    const set = doc.createElement('fieldset');
    set.className = 'dc-adhoc-options-section';
    const legend = doc.createElement('legend');
    legend.textContent = title;
    set.append(legend);
    list.append(set);
    return set;
  };

  function radios<K extends 'zoomLevel' | 'ancestorPosition' | 'indentation'>(
    title: string, field: K, choices: Choice<K>,
  ): void {
    const set = section(title);
    set.dataset['option'] = field;
    for (const [value, label] of choices) {
      const row = doc.createElement('label');
      row.className = 'dc-adhoc-option';
      const input = doc.createElement('input');
      input.type = 'radio';
      input.name = `dc-adhoc-${field}`;
      input.value = String(value);
      input.checked = draft[field] === value;
      input.addEventListener('change', () => {
        if (input.checked) draft = { ...draft, [field]: value };
      });
      row.append(input, doc.createTextNode(` ${label}`));
      set.append(row);
    }
  }

  function flags(title: string, fields: readonly (readonly [Flag, string])[]): void {
    const set = section(title);
    for (const [field, label] of fields) {
      const row = doc.createElement('label');
      row.className = 'dc-adhoc-option';
      const input = doc.createElement('input');
      input.type = 'checkbox';
      input.dataset['option'] = field;
      input.checked = draft[field];
      input.addEventListener('change', () => {
        draft = { ...draft, [field]: input.checked };
      });
      row.append(input, doc.createTextNode(` ${label}`));
      set.append(row);
    }
  }

  function paint(): void {
    list.replaceChildren();
    radios('Zoom In level', 'zoomLevel', ZOOM);
    radios('Ancestor position', 'ancestorPosition', ANCESTOR);
    radios('Indentation', 'indentation', INDENT);
    flags('Suppress rows', [
      ['suppressMissingRows', 'No Data / Missing'],
      ['suppressZeroRows', 'Zero'],
      ['suppressRepeatedMembers', 'Repeated Members'],
    ]);
    flags('Suppress columns', [
      ['suppressMissingColumns', 'No Data / Missing'],
      ['suppressZeroColumns', 'Zero'],
    ]);
    flags('Mode', [['navigateWithoutData', 'Navigate without data']]);
  }

  const button = (label: string, cls: string, onClick: () => void): HTMLButtonElement => {
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = `dc-button ${cls}`;
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  };
  const buttons = doc.createElement('div');
  buttons.className = 'dc-adhoc-options-buttons';
  buttons.append(
    button('Cancel', 'dc-adhoc-options-cancel', () => options.onClose()),
    button('OK', 'dc-adhoc-options-ok', () => {
      const patch: Record<string, unknown> = {};
      for (const k of Object.keys(draft) as (keyof AdHocOptions)[]) {
        if (draft[k] !== options.values[k]) patch[k] = draft[k];
      }
      if (Object.keys(patch).length === 0) options.onClose();
      else options.onOk(patch as Partial<AdHocOptions>);
    }),
  );
  footer.append(
    button('Restore Defaults', 'dc-adhoc-options-restore', () => {
      draft = { ...DEFAULT_OPTIONS };
      paint();
    }),
    buttons,
  );
  paint();
}
