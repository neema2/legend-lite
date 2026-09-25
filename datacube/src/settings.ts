// DataCube's settings: upstream's DataCubeSettingService, by upstream's
// KEYS (so a host that persists them for upstream persists them for
// this), titles and defaults -- but only the settings that do something
// here. Upstream's cache and pagination warnings wait for those
// features; "Refresh Group Node Data" has nothing to switch, since every
// expand here fetches afresh; the dev protocol version is an engine
// client detail this cube does not have.

export type SettingGroup = 'Grid' | 'Editor' | 'Debug';

export type SettingKey =
  | 'dataCube.debugger.enableDebugMode'
  | 'dataCube.debugger.action.reload'
  | 'dataCube.editor.maxHistoryStackSize'
  | 'dataCube.grid.rowBuffer';

export type Setting =
  | {
    readonly key: SettingKey;
    readonly type: 'boolean';
    readonly title: string;
    readonly description: string;
    readonly group: SettingGroup;
    readonly defaultValue: boolean;
  }
  | {
    readonly key: SettingKey;
    readonly type: 'numeric';
    readonly title: string;
    readonly description: string;
    readonly group: SettingGroup;
    readonly defaultValue: number;
    readonly min: number;
    readonly step: number;
  }
  | {
    readonly key: SettingKey;
    readonly type: 'action';
    readonly title: string;
    readonly description: string;
    readonly group: SettingGroup;
  };

export const SETTINGS: readonly Setting[] = [
  {
    key: 'dataCube.debugger.enableDebugMode',
    type: 'boolean',
    title: 'Debug Mode: Enabled',
    description: 'Enables debug logging when running data queries, updating'
      + ' snapshots, etc.',
    group: 'Debug',
    defaultValue: false,
  },
  {
    key: 'dataCube.debugger.action.reload',
    type: 'action',
    title: 'Reload',
    description: 'Manually reload DataCube (keeping all states). This is needed'
      + ' when making changes to settings that require reloading to take effect.',
    group: 'Debug',
  },
  {
    key: 'dataCube.editor.maxHistoryStackSize',
    type: 'numeric',
    title: 'Max History Stack Size',
    description: 'Sets the number of maximum snapshots to store in edit history.',
    group: 'Editor',
    defaultValue: 100,
    min: 10,
    step: 10,
  },
  {
    key: 'dataCube.grid.rowBuffer',
    type: 'numeric',
    title: 'Row Buffer',
    description: "Sets the number of rows the grid renders outside of the viewable"
      + " area. e.g. if the buffer is 10 and your grid is showing 50 rows (as"
      + " that's all that fits on your screen without scrolling), then the grid"
      + ' will actually render 70 in total (10 extra above and 10 extra below).'
      + ' Then when you scroll, the grid will already have 10 rows ready and'
      + ' waiting to show, no redraw is needed. A low small buffer will make'
      + ' initial draws of the grid faster; whereas a big one will reduce the'
      + ' redraw visible vertically scrolling.',
    group: 'Grid',
    defaultValue: 50,
    min: 10,
    step: 10,
  },
];

/** Values by key; actions have none. */
export type SettingValues = Readonly<Record<string, boolean | number>>;

export const DEFAULT_SETTINGS: SettingValues = Object.fromEntries(
  SETTINGS.flatMap((s) => (s.type === 'action' ? [] : [[s.key, s.defaultValue]])),
);

/**
 * Defaults, overlaid with the values a host kept -- only for keys this
 * cube knows, and only of the right type, as upstream ignores unknown
 * (outdated, renamed) ones.
 */
export function readSettings(saved: Readonly<Record<string, unknown>> | undefined): SettingValues {
  const out: Record<string, boolean | number> = { ...DEFAULT_SETTINGS };
  for (const s of SETTINGS) {
    const v = saved?.[s.key];
    if (s.type === 'boolean' && typeof v === 'boolean') out[s.key] = v;
    if (s.type === 'numeric' && typeof v === 'number' && Number.isFinite(v)) {
      out[s.key] = Math.max(s.min, v);
    }
  }
  return out;
}

export function booleanSetting(values: SettingValues, key: SettingKey): boolean {
  return values[key] === true;
}

export function numericSetting(values: SettingValues, key: SettingKey): number {
  const v = values[key];
  if (typeof v !== 'number') throw new Error(`setting ${key} is not numeric`);
  return v;
}
