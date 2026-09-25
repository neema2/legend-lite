// The Settings window: upstream's DataCubeSettingsPanel. Settings by
// group (Grid, Editor, Debug), each with its title and description and
// a control -- a checkbox, a number, or a button for an action. Edits
// are a DRAFT: Save applies it, OK applies and closes, Cancel drops it,
// and Restore Default Settings puts the defaults in the draft (disabled
// when they are already there).

import {
  DEFAULT_SETTINGS,
  SETTINGS,
  type SettingGroup,
  type SettingKey,
  type SettingValues,
} from '../settings.ts';

export interface SettingsPanelOptions {
  readonly values: SettingValues;
  /** The draft, applied: the cube uses it and the host may keep it. */
  readonly onSave: (values: SettingValues) => void;
  /** Run an action setting (Reload). */
  readonly onAction: (key: SettingKey) => void;
  readonly onClose: () => void;
}

/** Upstream's Settings window: 600 x 400, near the top right. */
export const SETTINGS_WINDOW = {
  width: 600,
  height: 400,
  minWidth: 300,
  minHeight: 200,
  x: -50,
  y: 50,
  center: false,
} as const;

const GROUPS: readonly SettingGroup[] = ['Grid', 'Editor', 'Debug'];

export function buildSettingsPanel(host: HTMLElement, options: SettingsPanelOptions): void {
  const doc = host.ownerDocument;
  let draft: Record<string, boolean | number> = { ...options.values };
  host.classList.add('dc-settings');

  const list = doc.createElement('div');
  list.className = 'dc-settings-list';
  const footer = doc.createElement('div');
  footer.className = 'dc-settings-footer';
  host.append(list, footer);

  const button = (label: string, onClick: () => void, cls = ''): HTMLButtonElement => {
    const b = doc.createElement('button');
    b.type = 'button';
    b.className = `dc-button ${cls}`.trim();
    b.textContent = label;
    b.addEventListener('click', onClick);
    return b;
  };
  const restore = button('Restore Default Settings', () => {
    draft = { ...DEFAULT_SETTINGS };
    paint();
  }, 'dc-settings-restore');
  const right = doc.createElement('div');
  right.className = 'dc-settings-buttons';
  right.append(
    button('Cancel', () => options.onClose(), 'dc-settings-cancel'),
    button('Save', () => options.onSave({ ...draft }), 'dc-settings-save'),
    button('OK', () => {
      options.onSave({ ...draft });
      options.onClose();
    }, 'dc-settings-ok'),
  );
  footer.append(restore, right);

  function paint(): void {
    list.replaceChildren();
    for (const group of GROUPS) {
      // Upstream sorts each group's settings by title.
      const settings = SETTINGS.filter((s) => s.group === group)
        .sort((a, b) => a.title.localeCompare(b.title));
      if (settings.length === 0) continue;
      const heading = doc.createElement('div');
      heading.className = 'dc-settings-group';
      heading.textContent = group;
      list.append(heading);
      for (const s of settings) {
        const row = doc.createElement('div');
        row.className = 'dc-settings-item';
        row.dataset['setting'] = s.key;
        const title = doc.createElement('div');
        title.className = 'dc-settings-title';
        title.textContent = s.title;
        const description = doc.createElement('div');
        description.className = 'dc-settings-description';
        description.textContent = s.description;
        row.append(title);
        switch (s.type) {
          case 'boolean': {
            const label = doc.createElement('label');
            label.className = 'dc-check';
            const box = doc.createElement('input');
            box.type = 'checkbox';
            box.className = 'dc-check-box';
            box.checked = draft[s.key] === true;
            box.addEventListener('change', () => {
              draft = { ...draft, [s.key]: box.checked };
              paintRestore();
            });
            label.append(box, description);
            row.append(label);
            break;
          }
          case 'numeric': {
            row.append(description);
            const input = doc.createElement('input');
            input.type = 'number';
            input.className = 'dc-input dc-settings-number';
            input.min = String(s.min);
            input.step = String(s.step);
            input.value = String(draft[s.key]);
            input.addEventListener('change', () => {
              const n = Number(input.value);
              // Invalid reverts, as upstream's number inputs do.
              if (!Number.isFinite(n) || n < s.min) {
                input.value = String(draft[s.key]);
                return;
              }
              draft = { ...draft, [s.key]: n };
              paintRestore();
            });
            row.append(input);
            break;
          }
          case 'action':
            row.append(description, button(s.title, () => options.onAction(s.key),
              'dc-settings-action'));
            break;
        }
        list.append(row);
      }
    }
    paintRestore();
  }

  function paintRestore(): void {
    restore.disabled = Object.entries(DEFAULT_SETTINGS)
      .every(([k, v]) => draft[k] === v);
  }

  paint();
}
