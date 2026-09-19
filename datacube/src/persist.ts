// Saved views: the cube's configuration, written down.
//
// In finance the pivot configuration IS the daily work product -- the
// thing that gets rebuilt every morning or screenshotted into an
// email -- so this is a first-class feature rather than a
// convenience, and it has to survive the product changing underneath
// it.
//
// Three rules, each answering a way saved state usually rots:
//
//  - Everything is VERSIONED, and readers migrate forward. A saved
//    view older than the code is the normal case, not an error case.
//  - Unknown fields are PRESERVED, not dropped. An older client that
//    opens and re-saves a view written by a newer one must not
//    silently delete the settings it did not understand.
//  - A view that cannot be read fails LOUDLY with what was wrong. A
//    silently-default cube looks like data loss to the person whose
//    work it was.

import type { ColumnFormat } from './format.ts';
import type { CubeSnapshot } from './snapshot.ts';
import { TreeState, parsePathKey } from './tree.ts';

/** Bumped whenever the shape changes in a way a reader must handle. */
export const CURRENT_VERSION = 1;

export interface ColumnSettings {
  /** Display order, outermost first. Absent means engine order. */
  readonly order?: readonly string[];
  /** Columns hidden from the grid but still available to the query. */
  readonly hidden?: readonly string[];
  /** Pixel widths by column name. */
  readonly widths?: Readonly<Record<string, number>>;
  /** Per-column display format. */
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
}

export interface SavedView {
  readonly version: number;
  readonly name: string;
  /** ISO 8601, so a saved view sorts and diffs sensibly as text. */
  readonly savedAt: string;
  readonly snapshot: CubeSnapshot;
  /** Open group paths, as produced by TreeState.openPaths. */
  readonly expanded: readonly string[];
  readonly showTotals: boolean;
  readonly columns?: ColumnSettings;
  /**
   * Fields a newer writer added that this reader does not know.
   * Round-tripped verbatim so an older client cannot silently discard
   * a colleague's settings.
   */
  readonly unknown?: Readonly<Record<string, unknown>>;
}

export class SavedViewError extends Error {
  readonly detail: string;
  constructor(detail: string) {
    super(`cannot read saved view: ${detail}`);
    this.name = 'SavedViewError';
    this.detail = detail;
  }
}

const KNOWN_KEYS = new Set([
  'version',
  'name',
  'savedAt',
  'snapshot',
  'expanded',
  'showTotals',
  'columns',
]);

export interface SaveOptions {
  readonly name: string;
  readonly snapshot: CubeSnapshot;
  readonly tree: TreeState;
  readonly columns?: ColumnSettings;
  readonly unknown?: Readonly<Record<string, unknown>>;
  /** Injectable so tests are not time-dependent. */
  readonly now?: Date;
}

export function save(options: SaveOptions): SavedView {
  // The epoch is runtime bookkeeping for discarding stale queries; it
  // means nothing tomorrow, so it is normalised rather than persisted
  // as whatever number the session happened to reach.
  const snapshot: CubeSnapshot = { ...options.snapshot, epoch: 0 };
  return {
    version: CURRENT_VERSION,
    name: options.name,
    savedAt: (options.now ?? new Date()).toISOString(),
    snapshot,
    expanded: options.tree.openPaths,
    showTotals: options.tree.showTotals,
    ...(options.columns ? { columns: options.columns } : {}),
    ...(options.unknown && Object.keys(options.unknown).length > 0
      ? { unknown: options.unknown }
      : {}),
  };
}

export function toJson(view: SavedView): string {
  const { unknown, ...rest } = view;
  // Unknown fields are written back at the top level, where they came
  // from, so a newer client sees its own settings unchanged.
  return JSON.stringify({ ...unknown, ...rest }, null, 2);
}

/**
 * Read a saved view, migrating it forward.
 *
 * Accepts either the parsed object or the raw text, because the caller
 * is as likely to have one as the other and making them convert first
 * just moves the error handling somewhere worse.
 */
export function load(input: string | unknown): SavedView {
  let raw: unknown = input;
  if (typeof input === 'string') {
    try {
      raw = JSON.parse(input);
    } catch (e) {
      throw new SavedViewError(
        `not valid JSON (${e instanceof Error ? e.message : String(e)})`,
      );
    }
  }
  if (raw === null || typeof raw !== 'object') {
    throw new SavedViewError('expected an object');
  }
  const obj = raw as Record<string, unknown>;

  const version = typeof obj['version'] === 'number' ? obj['version'] : 0;
  if (version > CURRENT_VERSION) {
    throw new SavedViewError(
      `written by a newer version (${version} > ${CURRENT_VERSION}); ` +
        `upgrade to open it`,
    );
  }

  const snapshot = obj['snapshot'];
  if (snapshot === null || typeof snapshot !== 'object') {
    throw new SavedViewError("missing 'snapshot'");
  }

  const unknown: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (!KNOWN_KEYS.has(k)) unknown[k] = v;
  }

  const view: SavedView = {
    version: CURRENT_VERSION,
    name: typeof obj['name'] === 'string' ? obj['name'] : 'Untitled',
    savedAt:
      typeof obj['savedAt'] === 'string'
        ? obj['savedAt']
        : new Date(0).toISOString(),
    snapshot: migrateSnapshot(snapshot as Record<string, unknown>, version),
    expanded: Array.isArray(obj['expanded'])
      ? (obj['expanded'] as unknown[]).filter(
          (x): x is string => typeof x === 'string',
        )
      : [],
    showTotals: obj['showTotals'] !== false,
    ...(obj['columns'] && typeof obj['columns'] === 'object'
      ? { columns: obj['columns'] as ColumnSettings }
      : {}),
    ...(Object.keys(unknown).length > 0 ? { unknown } : {}),
  };
  return view;
}

/**
 * Bring an older snapshot up to the current shape.
 *
 * Version 0 is anything written before the format carried a version
 * at all: fields are filled in rather than rejected, because a view
 * saved by an early build is exactly the case this exists to survive.
 */
function migrateSnapshot(
  raw: Record<string, unknown>,
  _from: number,
): CubeSnapshot {
  const arr = (k: string): string[] =>
    Array.isArray(raw[k])
      ? (raw[k] as unknown[]).filter((x): x is string => typeof x === 'string')
      : [];

  const source = raw['source'];
  const expression =
    source && typeof source === 'object' &&
    typeof (source as { expression?: unknown }).expression === 'string'
      ? (source as { expression: string }).expression
      : typeof source === 'string'
        ? source
        : '';
  if (expression === '') throw new SavedViewError("missing 'snapshot.source'");

  return {
    source: { expression },
    columns: Array.isArray(raw['columns'])
      ? (raw['columns'] as CubeSnapshot['columns'])
      : [],
    derived: Array.isArray(raw['derived'])
      ? (raw['derived'] as CubeSnapshot['derived'])
      : [],
    ...(raw['filter'] ? { filter: raw['filter'] as NonNullable<CubeSnapshot['filter']> } : {}),
    rows: arr('rows'),
    pivotOn: arr('pivotOn'),
    ...(Array.isArray(raw['pivotValues'])
      ? { pivotValues: raw['pivotValues'] as NonNullable<CubeSnapshot['pivotValues']> }
      : {}),
    measures: Array.isArray(raw['measures'])
      ? (raw['measures'] as CubeSnapshot['measures'])
      : [],
    sorts: Array.isArray(raw['sorts'])
      ? (raw['sorts'] as CubeSnapshot['sorts'])
      : [],
    ...(raw['window'] ? { window: raw['window'] as NonNullable<CubeSnapshot['window']> } : {}),
    // A restored view starts a fresh session, so it starts at epoch 0.
    epoch: 0,
  };
}

/** The expansion state a saved view describes. */
export function treeOf(view: SavedView): TreeState {
  return TreeState.fromPaths(
    view.expanded.map(parsePathKey),
    view.showTotals,
  );
}
