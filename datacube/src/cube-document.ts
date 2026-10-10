// A saved cube: OUR document, versioned (docs/DATACUBE_SAVE_SHARE_2026_09_28.md).
//
// The user's ruling (2026-09-28): a saved cube is the format WE think is right, and a
// legacy DataCube specification is read by a one-way translator later (milestone 2), never
// by bending this one. So this document is the cube's own definition:
//
//   source         where the rows come from, by identity (step 1: a file, by fingerprint)
//   query          the cube: columns, calculated columns, grouping, pivot, measures,
//                  filter, sorts, limits -- read WHOLE, never through an allow-list (the
//                  old saved view dropped every field its reader did not list: P2-71)
//   configuration  only what differs from the product's defaults, so a default changed
//                  later reaches every cube that never touched it
//   tree           the open rows, as typed paths
//
// Three rules, from the saved-view format this replaces:
//  - VERSIONED: a newer document is refused with a message; an older one is read forward.
//  - UNKNOWN FIELDS ARE KEPT: a field this reader does not know is written back verbatim,
//    so an older client never silently deletes a newer one's settings.
//  - A document that cannot be read fails LOUDLY, saying what was wrong.
//
// Opening a cube RECONCILES it with what its source holds now (the user's ruling on schema
// drift): a new column is available but hidden; a part that uses a column that is gone is
// left out and NAMED; a changed type is reported. Nothing is refused for drift alone.

import {
  ExactNumber,
  fromJson,
  readLambda,
  toJson as protocolJson,
} from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from './config.ts';
import type {
  ColumnSpec,
  CubeSnapshot,
  DerivedColumn,
  FilterNode,
  SourceRef,
} from './snapshot.ts';
import { TreeState, parsePathKey, type RowPath } from './tree.ts';
import type { UploadFormat } from './upload.ts';
import type { SharedQuery } from '../../query-store/src/share.ts';

/** What this document is, so a file of anything else is refused by name. */
export const CUBE_KIND = 'datacube.cube';
/** Bumped whenever the shape changes in a way a reader must handle. */
export const CUBE_VERSION = 1;

/**
 * A file the cube was built over, by IDENTITY: the model is derived from the file itself
 * (DuckDB's catalog), so the file is what a reopened cube needs back. The hash tells the
 * same file from a different one with the same name; the columns are what it held.
 */
export interface FileSource {
  readonly _type: 'file';
  readonly name: string;
  readonly format: UploadFormat;
  readonly size: number;
  /** Lower-case hex SHA-256 of the file's bytes. */
  readonly sha256: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  /**
   * A file this product GENERATED (a sample, from a fixed seed): opening rebuilds it, in any
   * browser, and the hash confirms the rebuild is the same data.
   */
  readonly sample?: { readonly id: string; readonly rows: number };
}

/**
 * A SAVED QUERY the cube was built over, by WHAT IT IS -- not where it is stored: its record as a
 * share link carries it (query-store `sharedPart`: project, execution context, content, saved
 * parameter values). Reopening reads it again through the page's model for its project, so the
 * cube opens wherever that project is known, with or without the store it was saved in.
 */
export interface QuerySource {
  readonly _type: 'savedQuery';
  /** The query's name: what a person calls this source. */
  readonly name: string;
  readonly query: SharedQuery;
  /** What it answered when the cube was saved. */
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/**
 * A WAREHOUSE TABLE the cube was built over, by where it is: the warehouse's address, the table,
 * the columns it had. Never a sign-in: whoever reopens it signs in as themselves, and the
 * warehouse's grants decide what they may read.
 */
export interface WarehouseSource {
  readonly _type: 'warehouseTable';
  /** `schema.table`: what a person calls this source. */
  readonly name: string;
  /** The warehouse's address, as its sign-in names it. */
  readonly warehouse: string;
  /**
   * The warehouse catalog the table is in (a warehouse serves several: its own DuckDB ones, attached
   * Postgres ones). A document saved before catalogs were named (2026-10-02) has none, and reads as
   * `main`: the only catalog DataCube opened then.
   */
  readonly catalog: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/**
 * A REMOTE FILE the cube was built over (Parquet, CSV or Iceberg by URL), by where it is. Never
 * its keys: a private bucket asks for them again when the cube is reopened.
 */
export interface RemoteSource {
  readonly _type: 'remoteFile';
  /** The file's name, the URL's last part: what a person calls this source. */
  readonly name: string;
  readonly url: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/**
 * A DATAFRAME the cube was built over, by its name on the engine that serves it (Python's `ll.Page`;
 * docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): reopened over the frame of that name, by whichever engine serves the
 * page. Never its rows: they are the engine's.
 */
export interface FrameSource {
  readonly _type: 'frame';
  /** The frame's table name on its engine (a plain identifier). */
  readonly name: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/** Where a cube's rows come from: a file, a saved query, a warehouse table, a remote file, a dataframe. */
export type CubeSource = FileSource | QuerySource | WarehouseSource | RemoteSource | FrameSource;

/** The cube's definition: a snapshot without its runtime state (source relation, epoch, row window). */
export type SavedQuery = Omit<CubeSnapshot, 'source' | 'epoch' | 'window'>;

export interface CubeDocument {
  readonly kind: typeof CUBE_KIND;
  readonly version: number;
  readonly name: string;
  readonly source: CubeSource;
  readonly query: SavedQuery;
  /** Differences from the product defaults; `null` means "unset" where a default is set. */
  readonly configuration: Readonly<Record<string, unknown>>;
  readonly tree: { readonly open: readonly RowPath[]; readonly showTotals: boolean };
  /** Top-level fields a newer writer added, kept verbatim. */
  readonly unknown?: Readonly<Record<string, unknown>>;
}

export class CubeDocumentError extends Error {
  constructor(detail: string) {
    super(`cannot read saved cube: ${detail}`);
    this.name = 'CubeDocumentError';
  }
}

const KNOWN = new Set(['kind', 'version', 'name', 'source', 'query', 'configuration', 'tree']);

// ---------------------------------------------------------------- writing

export interface WriteOptions {
  readonly name: string;
  readonly source: CubeSource;
  readonly snapshot: CubeSnapshot;
  readonly configuration: CubeConfiguration;
  readonly tree: TreeState;
  /** Fields carried from the document this cube was opened from. */
  readonly unknown?: Readonly<Record<string, unknown>>;
}

export function writeCube(o: WriteOptions): CubeDocument {
  const { source: _relation, epoch: _epoch, window: _window, ...query } = o.snapshot;
  return {
    kind: CUBE_KIND,
    version: CUBE_VERSION,
    name: o.name,
    source: o.source,
    query,
    configuration: (difference(o.configuration, DEFAULT_CONFIGURATION) ?? {}) as Record<string, unknown>,
    tree: {
      open: o.tree.openPaths.map(parsePathKey),
      showTotals: o.tree.showTotals,
    },
    ...(o.unknown && Object.keys(o.unknown).length > 0 ? { unknown: o.unknown } : {}),
  };
}

/**
 * The document as JSON text. Written by the protocol library: a calculated column's
 * lambda carries exact numbers (a decimal's `12.30`, an integer past 2^53) that
 * JSON.stringify cannot write.
 */
export function cubeToJson(doc: CubeDocument): string {
  const { unknown, ...rest } = doc;
  return protocolJson({ ...unknown, ...rest });
}

/**
 * What makes two cubes "the same" for "changed since saved": the definition and the settings.
 * Not the name (renaming is saving), not the open rows (looking is not changing).
 */
export function definitionText(doc: CubeDocument): string {
  return protocolJson({ query: doc.query, configuration: doc.configuration });
}

// ---------------------------------------------------------------- reading

export function readCube(input: string | unknown): CubeDocument {
  let raw: unknown = input;
  if (typeof input === 'string') {
    try {
      raw = fromJson(input);
    } catch (e) {
      throw new CubeDocumentError(`not valid JSON (${e instanceof Error ? e.message : String(e)})`);
    }
  }
  if (!isObject(raw)) throw new CubeDocumentError('expected an object');
  if (raw['kind'] !== CUBE_KIND) {
    // Said by name: a legacy DataCube specification (upstream's saved cube) is opened by the
    // translator of milestone 2, not read as one of ours.
    if (typeof raw['query'] === 'string' && 'source' in raw) {
      throw new CubeDocumentError('this is a legacy DataCube specification; opening those is not built yet');
    }
    throw new CubeDocumentError(`not a saved cube (kind ${JSON.stringify(plain(raw['kind']) ?? null)})`);
  }
  const version = numberOf(raw['version']);
  if (version === undefined || version < 1) throw new CubeDocumentError('it has no version');
  if (version > CUBE_VERSION) {
    throw new CubeDocumentError(`written by a newer version (${version} > ${CUBE_VERSION}); upgrade to open it`);
  }
  if (typeof raw['name'] !== 'string') throw new CubeDocumentError("missing 'name'");

  const unknown: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(raw)) if (!KNOWN.has(k)) unknown[k] = plain(v);

  const configuration = plain(raw['configuration'] ?? {});
  if (!isObject(configuration)) throw new CubeDocumentError("'configuration' is not an object");

  return {
    kind: CUBE_KIND,
    version: CUBE_VERSION,
    name: raw['name'],
    source: readSource(raw['source']),
    query: readQuery(raw['query']),
    configuration,
    tree: readTree(raw['tree']),
    ...(Object.keys(unknown).length > 0 ? { unknown } : {}),
  };
}

function readSource(raw: unknown): CubeSource {
  const s = plain(raw);
  if (!isObject(s)) throw new CubeDocumentError("missing 'source'");
  if (s['_type'] === 'savedQuery') return readQuerySource(s);
  if (s['_type'] === 'warehouseTable') return readWarehouseSource(s);
  if (s['_type'] === 'remoteFile') return readRemoteSource(s);
  if (s['_type'] === 'frame') return readFrameSource(s);
  if (s['_type'] !== 'file') {
    throw new CubeDocumentError(`a source of kind ${JSON.stringify(s['_type'] ?? null)} is not supported yet`);
  }
  const columns = s['columns'];
  if (typeof s['name'] !== 'string' || typeof s['format'] !== 'string'
    || typeof s['size'] !== 'number' || typeof s['sha256'] !== 'string'
    || !Array.isArray(columns)
    || !columns.every((c) => isObject(c) && typeof c['name'] === 'string' && typeof c['type'] === 'string')) {
    throw new CubeDocumentError('the file source is incomplete (name, format, size, sha256, columns)');
  }
  const sample = s['sample'];
  if (sample !== undefined
    && !(isObject(sample) && typeof sample['id'] === 'string' && typeof sample['rows'] === 'number')) {
    throw new CubeDocumentError('the sample source is incomplete (id, rows)');
  }
  if (!['csv', 'parquet', 'json'].includes(s['format'])) {
    throw new CubeDocumentError(`an unknown file format ${JSON.stringify(s['format'])}`);
  }
  return s as unknown as FileSource;
}

/** The columns a source had: each a name and a type. */
function hasColumns(s: Record<string, unknown>): boolean {
  const columns = s['columns'];
  return Array.isArray(columns)
    && columns.every((c) => isObject(c) && typeof c['name'] === 'string' && typeof c['type'] === 'string');
}

function readWarehouseSource(s: Record<string, unknown>): WarehouseSource {
  for (const f of ['name', 'warehouse', 'schema', 'table']) {
    if (typeof s[f] !== 'string' || s[f] === '') throw new CubeDocumentError(`the warehouse table source has no ${f}`);
  }
  if (!hasColumns(s)) throw new CubeDocumentError('the warehouse table source has no columns');
  if ('password' in s || 'token' in s) throw new CubeDocumentError('the warehouse table source carries a credential: it is refused');
  if ('catalog' in s && (typeof s['catalog'] !== 'string' || s['catalog'] === '')) {
    throw new CubeDocumentError('the warehouse table source has an empty catalog');
  }
  // saved before catalogs were named: `main`, the only catalog DataCube opened then (WarehouseSource.catalog)
  return { ...s, catalog: typeof s['catalog'] === 'string' ? s['catalog'] : 'main' } as unknown as WarehouseSource;
}

function readRemoteSource(s: Record<string, unknown>): RemoteSource {
  for (const f of ['name', 'url']) {
    if (typeof s[f] !== 'string' || s[f] === '') throw new CubeDocumentError(`the remote file source has no ${f}`);
  }
  if (!hasColumns(s)) throw new CubeDocumentError('the remote file source has no columns');
  if ('secretAccessKey' in s || 'secret' in s) throw new CubeDocumentError('the remote file source carries a credential: it is refused');
  return s as unknown as RemoteSource;
}

function readFrameSource(s: Record<string, unknown>): FrameSource {
  if (typeof s['name'] !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(s['name'])) {
    throw new CubeDocumentError('the frame source has no name (a plain identifier: letters, digits, \'_\')');
  }
  if (!hasColumns(s)) throw new CubeDocumentError('the frame source has no columns');
  return s as unknown as FrameSource;
}

function readQuerySource(s: Record<string, unknown>): QuerySource {
  const q = s['query'];
  const columns = s['columns'];
  if (typeof s['name'] !== 'string' || !isObject(q) || !Array.isArray(columns)
    || !columns.every((c) => isObject(c) && typeof c['name'] === 'string' && typeof c['type'] === 'string')) {
    throw new CubeDocumentError('the saved query source is incomplete (name, query, columns)');
  }
  for (const f of ['name', 'groupId', 'artifactId', 'versionId', 'content']) {
    if (typeof q[f] !== 'string' || q[f] === '') throw new CubeDocumentError(`the saved query source has no ${f}`);
  }
  return s as unknown as QuerySource;
}

/**
 * The cube, read WHOLE: every field as plain JSON, then the calculated columns' lambdas read
 * exactly. No allow-list -- a field this version does not know is carried, not dropped.
 */
function readQuery(raw: unknown): SavedQuery {
  if (!isObject(raw)) throw new CubeDocumentError("missing 'query'");
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(raw)) {
    if (k === 'derived' || k === 'groupDerived') continue;
    out[k] = plain(v);
  }
  for (const k of ['columns', 'rows', 'pivotOn', 'measures', 'sorts']) {
    if (!Array.isArray(out[k])) throw new CubeDocumentError(`'query.${k}' is not a list`);
  }
  out['derived'] = calculated(raw['derived'], 'derived');
  if (raw['groupDerived'] !== undefined) out['groupDerived'] = calculated(raw['groupDerived'], 'groupDerived');
  return out as unknown as SavedQuery;
}

/** Calculated columns, each lambda read exactly (its numbers as the protocol's exact numbers). */
function calculated(raw: unknown, field: string): DerivedColumn[] {
  if (raw === undefined) return [];
  if (!Array.isArray(raw)) throw new CubeDocumentError(`'query.${field}' is not a list`);
  return raw.map((item) => {
    if (!isObject(item)) throw new CubeDocumentError(`an entry of 'query.${field}' is not an object`);
    const { lambda, ...rest } = item;
    const fields = plain(rest) as Omit<DerivedColumn, 'lambda'>;
    return lambda === undefined ? fields : { ...fields, lambda: readLambda(lambda) };
  });
}

function readTree(raw: unknown): CubeDocument['tree'] {
  const t = plain(raw ?? { open: [], showTotals: false });
  if (!isObject(t) || !Array.isArray(t['open'])) throw new CubeDocumentError("'tree' has no open paths");
  const open = t['open'].map((p) => {
    if (!Array.isArray(p) || !p.every((k) => k === null || typeof k === 'string')) {
      throw new CubeDocumentError(`not a row path: ${JSON.stringify(p)}`);
    }
    return p as RowPath;
  });
  return { open, showTotals: t['showTotals'] === true };
}

// ---------------------------------------------------------------- opening

export interface OpenedCube {
  readonly snapshot: CubeSnapshot;
  readonly configuration: CubeConfiguration;
  readonly tree: TreeState;
  /** What drifted, in words, for the person opening it. Empty when nothing did. */
  readonly notes: readonly string[];
  /** True when a part of the cube was left out: the cube on screen is not the one saved. */
  readonly changed: boolean;
}

/**
 * The cube over its source AS IT IS NOW. `relation` is the re-ingested source's relation and
 * `columns` its columns as the COMPILER types them (sourceColumns): the saved column list
 * is only a record of what the file held, used to say what changed.
 */
export function openCube(
  doc: CubeDocument,
  relation: SourceRef,
  columns: readonly ColumnSpec[],
): OpenedCube {
  const notes: string[] = [];
  const q = doc.query;
  const saved = new Map(q.columns.map((c) => [c.name, c]));
  const now = new Map(columns.map((c) => [c.name, c]));
  let configuration = merge(DEFAULT_CONFIGURATION, doc.configuration) as CubeConfiguration;

  // New columns: available, not shown.
  const added = columns.filter((c) => !saved.has(c.name)).map((c) => c.name);
  if (added.length > 0) {
    notes.push(`${plural(added.length, 'new column')} since this cube was saved, hidden: ${added.join(', ')}`);
    const cols = { ...configuration.columns };
    for (const name of added) cols[name] = { ...cols[name], hidden: true };
    configuration = { ...configuration, columns: cols };
  }
  // Retyped columns: the compiler's type wins; said out loud.
  for (const c of columns) {
    const was = saved.get(c.name);
    if (was && was.type !== c.type) notes.push(`${c.name} is now ${c.type} (it was ${was.type})`);
  }
  // Gone columns, and every calculated column that reads one (in order: one may read another).
  const gone = new Set(q.columns.filter((c) => !now.has(c.name)).map((c) => c.name));
  if (gone.size > 0) notes.push(`${plural(gone.size, 'column')} no longer in the file: ${[...gone].join(', ')}`);
  const keepDerived = (list: readonly DerivedColumn[]): DerivedColumn[] => list.filter((d) => {
    const lost = [...derivedReads(d)].filter((n) => gone.has(n));
    if (lost.length === 0) return true;
    notes.push(`left out calculated column ${d.name} (it uses ${lost.join(', ')})`);
    gone.add(d.name);
    return false;
  });
  const derived = keepDerived(q.derived);
  const groupDerived = q.groupDerived === undefined ? undefined : keepDerived(q.groupDerived);

  const using = (what: string) => (name: string): boolean => {
    if (!gone.has(name)) return true;
    notes.push(`left out ${what} ${name}`);
    return false;
  };
  const rows = q.rows.filter(using('grouping by'));
  const pivotOn = q.pivotOn.filter(using('pivoting on'));
  const measures = q.measures.filter((m) => {
    const lost = [m.column, m.weight].filter((n): n is string => n !== undefined && gone.has(n));
    if (lost.length === 0) return true;
    notes.push(`left out measure ${m.name} (it uses ${lost.join(', ')})`);
    return false;
  });
  const sorts = q.sorts.filter((s) => using('sorting by')(s.column));
  const filter = q.filter === undefined ? undefined : pruneFilter(q.filter, gone, notes);

  // The source columns: the compiler's, carrying what the cube had set on each.
  const specs: ColumnSpec[] = columns.map((c) => {
    const was = saved.get(c.name);
    if (!was) return c;
    const { name: _n, type: _t, ...settings } = was;
    return { ...c, ...settings, name: c.name, type: c.type };
  });

  const shapeChanged = rows.length !== q.rows.length || pivotOn.length !== q.pivotOn.length;
  const {
    columns: _c, derived: _d, groupDerived: _g, rows: _r, pivotOn: _p, measures: _m, sorts: _s,
    filter: _f, pivotValues, ...rest
  } = q;
  const snapshot: CubeSnapshot = {
    ...rest,
    source: relation,
    columns: specs,
    derived,
    ...(groupDerived !== undefined ? { groupDerived } : {}),
    rows,
    pivotOn,
    // pinned pivot values belong to the keys they were pinned on
    ...(pivotValues !== undefined && pivotOn.length === q.pivotOn.length ? { pivotValues } : {}),
    measures,
    sorts,
    ...(filter !== undefined ? { filter } : {}),
    epoch: 0,
  };
  // The open rows are paths through the saved grouping; a different grouping has none.
  const tree = TreeState.fromPaths(shapeChanged ? [] : doc.tree.open, doc.tree.showTotals);
  const changed = notes.some((n) => n.startsWith('left out'));
  return { snapshot, configuration, tree, notes, changed };
}

/** The source and calculated columns a calculated column reads. */
export function derivedReads(d: DerivedColumn): Set<string> {
  const out = new Set<string>();
  if (d.lambda) {
    const params = new Set(d.lambda.parameters.map((p) => p.name));
    walk(d.lambda.body, (node) => {
      if (node['_type'] === 'property' && typeof node['property'] === 'string') {
        const receiver = (node['parameters'] as unknown[] | undefined)?.[0];
        if (isObject(receiver) && receiver['_type'] === 'var' && params.has(receiver['name'] as string)) {
          out.add(node['property']);
        }
      }
    });
  }
  if (d.window) {
    if (d.window.column !== undefined) out.add(d.window.column);
    d.window.partition.forEach((c) => out.add(c));
    d.window.order.forEach((s) => out.add(s.column));
  }
  if (d.childAggregate) out.add(d.childAggregate.of);
  return out;
}

function walk(node: unknown, visit: (n: Record<string, unknown>) => void): void {
  if (Array.isArray(node)) {
    node.forEach((n) => walk(n, visit));
  } else if (isObject(node) && !(node instanceof ExactNumber)) {
    visit(node);
    Object.values(node).forEach((v) => walk(v, visit));
  }
}

/** A filter with every condition on a gone column left out (and named), or undefined if none remain. */
function pruneFilter(node: FilterNode, gone: ReadonlySet<string>, notes: string[]): FilterNode | undefined {
  switch (node.kind) {
    case 'condition': {
      const lost = [node.column, node.rightColumn].filter((n): n is string => n !== undefined && gone.has(n));
      if (lost.length === 0) return node;
      notes.push(`left out the filter on ${lost.join(', ')}`);
      return undefined;
    }
    case 'not': {
      const child = pruneFilter(node.child, gone, notes);
      return child === undefined ? undefined : { kind: 'not', child };
    }
    default: {
      const children = node.children
        .map((c) => pruneFilter(c, gone, notes))
        .filter((c): c is FilterNode => c !== undefined);
      if (children.length === 0) return undefined;
      return children.length === 1 ? children[0] : { kind: node.kind, children };
    }
  }
}

// ---------------------------------------------------------------- the file source

/** A file's identity: its name, format, size and SHA-256, and the columns it held. */
export async function fileSource(
  file: { readonly name: string; readonly size: number; arrayBuffer(): Promise<ArrayBuffer> },
  format: UploadFormat,
  columns: readonly ColumnSpec[],
  sample?: { readonly id: string; readonly rows: number },
): Promise<FileSource> {
  return {
    _type: 'file',
    name: file.name,
    format,
    size: file.size,
    sha256: await sha256(await file.arrayBuffer()),
    columns: columns.map((c) => ({ name: c.name, type: c.type })),
    ...(sample ? { sample } : {}),
  };
}

export async function sha256(bytes: ArrayBuffer): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** The saved configuration over the product defaults. */
export function configurationOf(doc: CubeDocument): CubeConfiguration {
  return merge(DEFAULT_CONFIGURATION, doc.configuration) as CubeConfiguration;
}

/** The open rows a document describes. */
export function treeOf(doc: CubeDocument): TreeState {
  return TreeState.fromPaths(doc.tree.open, doc.tree.showTotals);
}

// ---------------------------------------------------------------- differences

/**
 * What `value` changes of `base`, recursively: plain objects by key, anything else whole.
 * A key the base sets and the value does not is recorded as null ("unset"). Undefined when
 * nothing differs.
 */
export function difference(value: unknown, base: unknown): unknown {
  if (sameJson(value, base)) return undefined;
  if (isPlain(value) && isPlain(base)) {
    const out: Record<string, unknown> = {};
    for (const k of new Set([...Object.keys(value), ...Object.keys(base)])) {
      const v = value[k];
      if (v === undefined) {
        if (base[k] !== undefined) out[k] = null;
        continue;
      }
      const d = base[k] === undefined ? v : difference(v, base[k]);
      if (d !== undefined) out[k] = d;
    }
    return Object.keys(out).length > 0 ? out : undefined;
  }
  return value;
}

/** `base` with `patch` laid over it: the inverse of {@link difference}. */
export function merge(base: unknown, patch: unknown): unknown {
  if (!isPlain(patch)) return patch;
  const out: Record<string, unknown> = isPlain(base) ? { ...base } : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) delete out[k];
    else out[k] = merge(out[k], v);
  }
  return out;
}

function sameJson(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => sameJson(x, b[i]));
  if (isPlain(a) && isPlain(b)) {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    return [...keys].every((k) => sameJson(a[k], b[k]));
  }
  return false;
}

// ---------------------------------------------------------------- helpers

function isObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function isPlain(v: unknown): v is Record<string, unknown> {
  return isObject(v) && !(v instanceof ExactNumber);
}

function numberOf(v: unknown): number | undefined {
  if (v instanceof ExactNumber) return Number(v.text);
  return typeof v === 'number' ? v : undefined;
}

/** A value outside the trees as plain JSON: its exact numbers as numbers. */
function plain(v: unknown): unknown {
  if (v instanceof ExactNumber) return Number(v.text);
  if (Array.isArray(v)) return v.map(plain);
  if (isObject(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, plain(x)]));
  return v;
}

function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? '' : 's'}`;
}
