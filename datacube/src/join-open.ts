// OPENING A JOIN (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3, §17.4): a join source made one relation over one
// model. Its leaves -- the inputs that read a table straight, depth first -- are tables of the host's database; one model
// declares them all (the writer's several tables, infer.ts tablesModel); each input is its cube's rows at their finest
// level over its source (query.ts handedOnLambda), a join inside an input opened the same way; then the two are joined
// (join.ts joinRelation), and the compiler types what comes out. What drifted since it was saved is said. No DOM: the
// tab's sources (app/sources.ts) and a test on Node's DuckDB (test/join-open.test.ts) call it alike.

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import {
  frozenInput, inputCube, joinInputs, openCube, type CubeSource, type JoinInput, type JoinSource, type LeafSource, type RowsSource,
} from './cube-document.ts';
import type { CatalogColumn, TableModels } from './infer.ts';
import { MOST_REPEATS, REPEATED_KEYS, joinRelation, type JoinHow, type JoinSide, type JoinSummaryCounts, type JoinSummaryQueries } from './join.ts';
import { TOTAL_ROWS_COLUMN, handedOnLambda, isMachinery } from './query.ts';
import { handedOnRows } from './box-columns.ts';
import type { RawTable } from '../../engine-client/src/engine.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { sourceColumns, type RelationTyper } from './source-columns.ts';
import { boxRows } from './pure-box.ts';
import { CubeRefusal, type ColumnSpec } from './snapshot.ts';

/** The kinds of source that are tables of a host's database: the tab's files and remote files; Python's frames too. */
export type TableKind = 'file' | 'remoteFile' | 'frame';
/** The tab's: what its own DuckDB holds. */
export const TAB_TABLES: readonly TableKind[] = ['file', 'remoteFile'];
/** A host's (Python's): its frames, and the files and remote files it read in (headless host-tables.ts leafTables). */
export const HOST_TABLES: readonly TableKind[] = ['frame', 'file', 'remoteFile'];

const WHAT: Readonly<Record<LeafSource['_type'], string>> = {
  file: 'a file, read in another database',
  remoteFile: 'a remote file, read in another database',
  frame: "a dataframe, read on Python's database",
  savedQuery: 'a saved query, read through its project',
  warehouseTable: 'a warehouse table, read live on the warehouse',
  class: "a class, read through its project's mapping",
};
const PLURAL: Readonly<Record<TableKind, string>> = { file: 'files', remoteFile: 'remote files', frame: 'dataframes' };

/**
 * Why a leaf cannot be read where a join is (§17.3: one model, one connection), or undefined: a source of a kind that
 * is a table of the host's database (`here`). Anything else is read somewhere else.
 */
export function leafRefusal(leaf: LeafSource, here: readonly TableKind[]): string | undefined {
  if ((here as readonly string[]).includes(leaf._type)) return undefined;
  return `a join reads both its inputs in one place, this page's own database (${here.map((k) => PLURAL[k]).join(', ')}); `
    + `${leaf.name} is ${WHAT[leaf._type]} -- cache the box that reads it, and its rows are copied here when it runs (FL10)`;
}

/** What opening a join needs of its host. */
export interface JoinOpening {
  /** The kinds of source that are tables of the host's database (TAB_TABLES in the tab). */
  readonly here: readonly TableKind[];
  /** The table of the host's database each leaf reads, in leavesOf's order (cube-document.ts) (a table read twice, named twice). */
  readonly tables: readonly string[];
  /** The host's model writer. */
  readonly models: TableModels;
  /** A table's catalog rows (upload.ts catalogColumns, on the host's database). */
  readonly catalog: (table: string) => Promise<CatalogColumn[]>;
  /** The host database's type, as a connection names it (`DuckDB`). */
  readonly databaseType: string;
  /** What types a relation over a model: a planner over it. */
  readonly typer: (model: string, runtime: string) => RelationTyper;
}

/** A join's two inputs opened over one model: what the Join dialog asks of, before it joins them. */
export interface OpenedInputs {
  readonly model: string;
  readonly runtime: string;
  readonly typer: RelationTyper;
  readonly left: JoinSide;
  readonly right: JoinSide;
  /** A Union's inputs after its right (JoinSource.more), in order; absent, two. */
  readonly more?: readonly JoinSide[];
  /** What drifted in an input since it was joined, in words, each with the input's name. */
  readonly notes: readonly string[];
}

/** A join opened: its model, its relation, its columns as the compiler types them, its inputs, what drifted. */
export interface OpenedJoin extends OpenedInputs {
  readonly relation: ValueSpecification;
  readonly columns: ColumnSpec[];
  /** How it joins as opened: a kept column of theirs gone since is left out (and said). */
  readonly how: JoinHow;
}

/**
 * AN INPUT'S ROWS, the one rule (a join's input; another grid's rows' input, docs/DATACUBE_FLOW_MODE_2026_10_24.md
 * §3.1): its cube opened over `over` -- what its source is now, as `typer` types it -- at its finest level (query.ts
 * handedOnLambda: a flat cube's rows in its sorts' order, a grouped cube's groups, a Cross Tab's groups by its pinned
 * values), what drifted since said in `notes` under its name. Refused in words when it is pivoted on values its rows
 * decide: its columns are its values, so its rows have no fixed columns. Its rows matched against another input's (JoinInput.unmatched: a Join's L or R) are not these: they are read over one model of both, its side
 * (oneModel) or openRows.
 */
export async function inputRows(input: JoinInput, over: ValueSpecification, typer: RelationTyper, notes: string[]): Promise<JoinSide> {
  const opened = openCube(inputCube(input), { query: over }, await sourceColumns(typer, over));
  for (const n of opened.notes) notes.push(`${input.name}: ${n}`);
  // a Cross Tab's columns fixed by its values pinned (FL2: query.ts crossTabRows); its rows deciding them, none fixed
  if (opened.snapshot.pivotOn.length > 0 && (opened.snapshot.pivotValues ?? []).length === 0) {
    throw new CubeRefusal(`${input.name} is pivoted: its columns are its values, so its rows have no fixed columns; Load its values to fix them, or take its pivot off first`);
  }
  const all = handedOnLambda(opened.snapshot).body[0]!;
  const typed = (await sourceColumns(typer, all)).map((c) => ({ name: c.name, type: c.type }));
  // the columns its box leaves out (JoinInput.omit): selected away, the rest in the order its box hands them on
  // (JoinInput.order: a Select's, those it does not list after them); then those it renames (JoinInput.rename), what
  // reads it reading them by their new names
  const omit = new Set(input.omit ?? []);
  const rank = new Map((input.order ?? []).map((c, i) => [c, i] as const));
  const at = (name: string): number => rank.get(name) ?? rank.size;
  // the product's own columns (grid/columns.ts isMachinery: a Top N's rank) are none of its rows'
  const kept = typed.filter((c) => !omit.has(c.name) && !isMachinery(c.name)).sort((a, b) => at(a.name) - at(b.name));
  const to = new Map((input.rename ?? []).map((r) => [r.from, r.to] as const));
  const on = kept.map((c) => ({ name: c.name, as: to.get(c.name) ?? c.name }));
  const columns = kept.map((c) => ({ name: to.get(c.name) ?? c.name, type: c.type }));
  return { name: input.name, relation: handedOnRows(all, typed.map((c) => c.name), on), columns };
}


/** A join's how with their kept columns that are gone left out, each said; the rest is joinRelation's to judge. */
function reconciled(how: JoinHow, right: JoinSide, notes: string[]): JoinHow {
  if ((how.form !== 'join' && how.form !== 'asOf') || how.keep === null) return how;
  const theirs = new Set(right.columns.map((c) => c.name));
  const gone = how.keep.filter((c) => !theirs.has(c));
  if (gone.length === 0) return how;
  notes.push(`left out ${right.name}'s ${gone.join(', ')} (no longer in it)`);
  return { ...how, keep: how.keep.filter((c) => theirs.has(c)) };
}

/**
 * A SNAPPED GRID'S COPY, as a combination reads it (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.2; the owner,
 * 2026-10-25: "can be snapped into duckdb wasm as a grid snap too if from different sources"): a table of this database
 * the snap copied the grid's base into -- one leaf, read as its table is, in its grid's place (the review's P3: a
 * combination read the grid live).
 */
export interface CopyTable {
  readonly _type: 'copy';
  readonly table: string;
  /** What its plane says of it and when it was taken, where what reads it says so (a cached box's: JoinInput.copy). */
  readonly label?: string;
  readonly takenAt?: string;
}

/** What a combination opens over one model: a grid's source, or its snapped copy's table. */
export type OpenedSource = CubeSource | CopyTable;

/** A source's leaves in order: a copy is its table's one. */
const leavesOfOpened = (s: OpenedSource): readonly (LeafSource | CopyTable)[] => (s._type === 'copy' ? [s] : openedLeaves(s));

/**
 * WHAT A SOURCE RESOLVED TO BE OPENED READS (live-inputs.ts resolvedSource's `copies`): its leaves as cube-document.ts
 * leavesOf says them, in that order -- but an input that reads its grid's copy (JoinInput.copy) is that copy's table, one
 * leaf, its own leaves not read at all.
 */
export function openedLeaves(source: CubeSource): (LeafSource | CopyTable)[] {
  switch (source._type) {
    case 'join': return joinInputs(source).flatMap((i) => openedInputLeaves(frozenInput(i, source.name)));
    case 'rows': return openedInputLeaves(frozenInput(source.input, source.name));
    default: return [source];
  }
}

/** An input's leaves as it is opened (openedLeaves): its copy's table, or its source's and its unmatched inputs' in turn. */
export function openedInputLeaves(input: JoinInput): (LeafSource | CopyTable)[] {
  if (input.copy !== undefined) return [{ _type: 'copy', table: input.copy.table, label: input.copy.label, takenAt: input.copy.takenAt }];
  return [...openedLeaves(input.source), ...(input.unmatched ?? []).flatMap((u) => openedInputLeaves(u.by))];
}

/** Sources over one model as they are opened: the model, how a relation over it is typed, and each source's relation. */
interface OneModel {
  readonly model: string;
  readonly runtime: string;
  readonly typer: RelationTyper;
  readonly notes: string[];
  /** A source's relation over the model: a table's (a copy's too), a join of its inputs, another grid's rows (its
   *  input's, each opened as `side`). Asked in order. */
  relation(s: OpenedSource): Promise<ValueSpecification>;
  /** An input as a join's side: its cube's rows at their finest level over its source. Asked in order. */
  side(input: JoinInput): Promise<JoinSide>;
}

/**
 * WHAT IS READ OVER ONE MODEL OF THE HOST'S TABLES (a join's inputs; another grid's rows; a timeseries' grids): `leaves`,
 * every leaf what is read reads, each a table of the host's database (`o.tables`, in their order) declared once -- a
 * table read twice is one table -- refused in words when a leaf is read elsewhere (leafRefusal); and each source and
 * input, asked IN ORDER (the leaves' order), its relation over the model.
 */
async function oneModel(leaves: readonly (LeafSource | CopyTable)[], o: JoinOpening): Promise<OneModel> {
  for (const leaf of leaves) {
    // a copy is a table of this database by what made it: nothing elsewhere to refuse
    if (leaf._type === 'copy') continue;
    const why = leafRefusal(leaf, o.here);
    if (why !== undefined) throw new CubeRefusal(why);
  }
  if (o.tables.length !== leaves.length) {
    throw new Error(`a join of ${leaves.length} tables was given ${o.tables.length} (${o.tables.join(', ')})`);
  }
  const distinct = [...new Set(o.tables)];
  const written = await o.models.tablesModel(await Promise.all(distinct.map(async (table) => ({
    table,
    // as each one's own model reads it: an upload rewritten at ingest, a frame and a snap's copy convert, a remote file's
    // view cannot
    convertible: leaves[o.tables.indexOf(table)]!._type !== 'remoteFile',
    columns: await o.catalog(table),
  }))), o.databaseType);
  const relationOf = new Map(distinct.map((t, i) => [t, written.tables[i]!.source]));
  const typer = o.typer(written.model, written.runtime);
  const notes: string[] = [];
  let next = 0;

  const relation = async (s: OpenedSource): Promise<ValueSpecification> => {
    // another grid's rows, a join's inputs, resolved against their page (live-inputs.ts resolvedSource): those grids'
    // rows as they are -- a Pure box's, its function of them (pure-box.ts)
    if (s._type === 'rows') return boxRows(s, (await side(frozenInput(s.input, s.name))).relation);
    if (s._type !== 'join') return relationOf.get(o.tables[next++]!)!;
    // its inputs in order, a Union's more after its right (each opened in the leaves' order)
    const sides: JoinSide[] = [];
    for (const i of joinInputs(s)) sides.push(await side(frozenInput(i, s.name)));
    const [l, r, ...more] = sides;
    return joinRelation(l!, r!, reconciled(s.how, r!, notes), more);
  };
  const side = async (input: JoinInput): Promise<JoinSide> => {
    // an input reading its grid's copy: the copy's table, its query over it (openedLeaves: one leaf, in its place)
    const base = input.copy !== undefined ? relationOf.get(o.tables[next++]!)! : await relation(input.source);
    let rows = await inputRows(input, base, typer, notes);
    // a Join's L or R (cube-document.ts JoinInput.unmatched): its rows no row of the other's pairs with, in turn
    for (const u of input.unmatched ?? []) rows = { ...rows, relation: joinRelation(rows, await side(u.by), { form: 'anti', on: u.on }) };
    return rows;
  };
  return { model: written.model, runtime: written.runtime, typer, notes, relation, side };
}

/**
 * THE INPUTS OF A JOIN over one model (oneModel): its two -- a Union's `more` after them -- each its cube's rows at
 * their finest level over its source, the leaves of each in turn (cube-document.ts inputLeaves: a Join's L or R reads
 * the other input's too) -- refused in words when a leaf is read elsewhere (leafRefusal) or an input is pivoted on
 * values its rows decide (its columns are its values).
 */
export async function openInputs(left: JoinInput, right: JoinInput, o: JoinOpening, more: readonly JoinInput[] = []): Promise<OpenedInputs> {
  const all = [left, right, ...more];
  const m = await oneModel(all.flatMap(openedInputLeaves), o);
  const sides: JoinSide[] = [];
  for (const i of all) sides.push(await m.side(i));
  const [l, r, ...after] = sides;
  return { model: m.model, runtime: m.runtime, typer: m.typer, left: l!, right: r!, ...(after.length > 0 ? { more: after } : {}), notes: m.notes };
}

/** Several grids' sources opened over one model: the model, its typer, and each source's relation over it, in order. */
export interface OpenedSources {
  readonly model: string;
  readonly runtime: string;
  readonly typer: RelationTyper;
  readonly relations: readonly ValueSpecification[];
  readonly notes: readonly string[];
}

/** Each of `items` with its source's relation, in the order the sources were opened: one each -- checked, never assumed
 *  (the timeseries review's G7). */
export function eachRelation<T>(opened: { readonly relations: readonly ValueSpecification[] }, items: readonly T[]): (readonly [T, ValueSpecification])[] {
  if (opened.relations.length !== items.length) {
    throw new Error(`the model opened ${opened.relations.length} relations for ${items.length} sources`);
  }
  return items.map((item, i) => [item, opened.relations[i] as ValueSpecification] as const);
}

/**
 * GRIDS' SOURCES OVER ONE MODEL (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.2): what a timeseries series that
 * combines grids is planned over, as a join's two inputs are (openInputs' rule, for any number of sources) -- each
 * source the relation of its tables over the model, which its grid's calculated columns and filter are then written
 * over. Refused in words when a leaf is read elsewhere (leafRefusal).
 */
export async function openSources(sources: readonly OpenedSource[], o: JoinOpening): Promise<OpenedSources> {
  const m = await oneModel(sources.flatMap(leavesOfOpened), o);
  const relations: ValueSpecification[] = [];
  for (const s of sources) relations.push(await m.relation(s));
  return { model: m.model, runtime: m.runtime, typer: m.typer, relations, notes: m.notes };
}

/** Another grid's rows opened over the host's tables (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1): its model, its
 *  relation and columns as the compiler types them, what drifted on the way. */
export interface OpenedRows {
  readonly model: string;
  readonly runtime: string;
  readonly typer: RelationTyper;
  readonly relation: ValueSpecification;
  readonly columns: ColumnSpec[];
  readonly notes: readonly string[];
}

/**
 * ANOTHER GRID'S ROWS OPENED OVER THE HOST'S TABLES, as a join is (oneModel): `source` resolved against its page
 * (live-inputs.ts resolvedSource), every leaf of its chain a table of the host's database -- its input's rows at their
 * finest level over that input's own source in turn, typed over one model of them.
 */
export async function openRows(source: RowsSource, o: JoinOpening): Promise<OpenedRows> {
  const m = await oneModel(openedLeaves(source), o);
  const relation = await m.relation(source);
  const columns = await sourceColumns(m.typer, relation);
  return { model: m.model, runtime: m.runtime, typer: m.typer, relation, columns, notes: m.notes };
}

/** A JOIN SOURCE OPENED (§17.4): its inputs over one model (openInputs), joined, typed. */
export async function openJoin(source: JoinSource, o: JoinOpening): Promise<OpenedJoin> {
  const inputs = await openInputs(frozenInput(source.left, source.name), frozenInput(source.right, source.name), o,
    (source.more ?? []).map((i) => frozenInput(i, source.name)));
  const notes = [...inputs.notes];
  const how = reconciled(source.how, inputs.right, notes);
  const relation = joinRelation(inputs.left, inputs.right, how, inputs.more);
  const columns = await sourceColumns(inputs.typer, relation);
  return { ...inputs, notes, how, relation, columns };
}

/** The dialog's questions (join.ts joinSummary) answered by `run`: each query planned over the join's model and run. */
export async function summaryCounts(q: JoinSummaryQueries, run: (query: Lambda) => Promise<RawTable>): Promise<JoinSummaryCounts> {
  const value = (t: RawTable, column: string): number => {
    const answered = t.columns.find((c) => c.name === column);
    if (!answered) throw new Error(`a join's question answered no ${column} (it answered ${t.columns.map((c) => c.name).join(', ')})`);
    // a count over no rows has no group, so no row: none
    if (t.rowCount === 0) return 0;
    const v = answered.values[0];
    if (v === undefined || v === null) throw new Error(`a join's question answered no value of ${column}`);
    return Number(v);
  };
  const count = async (query: Lambda | undefined): Promise<number | undefined> =>
    (query === undefined ? undefined : value(await run(query), TOTAL_ROWS_COLUMN));
  const [mine, matched, joined, theirs] = await Promise.all([count(q.mine), count(q.matched), count(q.joined), count(q.theirs)]);
  const repeated = q.repeated === undefined ? undefined : await run(q.repeated);
  return {
    mine: mine!,
    ...(matched !== undefined ? { matched } : {}),
    ...(joined !== undefined ? { joined } : {}),
    ...(theirs !== undefined ? { theirs } : {}),
    ...(repeated !== undefined ? { repeated: { keys: value(repeated, REPEATED_KEYS), most: value(repeated, MOST_REPEATS) } } : {}),
  };
}

/**
 * THE JOIN WINDOW'S INPUTS, while it is open: each grid it is asked of opened once (its promise kept, so two asks in
 * flight open one), its planner held while a question is answered over it, and all of them let go when the window
 * closes -- after the questions still being answered (a planner disposed under a question would hold its model again).
 */
export class JoinInputs<T extends { dispose(): void }> {
  readonly #open: (their: string) => Promise<T>;
  readonly #held = new Map<string, Promise<T>>();
  readonly #busy = new Set<Promise<unknown>>();
  #closed = false;

  constructor(open: (their: string) => Promise<T>) {
    this.#open = open;
  }

  /** Their grid's inputs: opened the first time it is asked, then the same. One that failed is opened again when asked. */
  get(their: string): Promise<T> {
    if (this.#closed) return Promise.reject(new Error('the Join window was closed'));
    let held = this.#held.get(their);
    if (!held) {
      held = this.#open(their);
      this.#held.set(their, held);
      held.catch(() => { if (this.#held.get(their) === held) this.#held.delete(their); });
    }
    return held;
  }

  /** `use` over their grid's inputs, which stay held until it is done. */
  async use<R>(their: string, use: (inputs: T) => Promise<R>): Promise<R> {
    const run = this.get(their).then(use);
    this.#busy.add(run);
    try {
      return await run;
    } finally {
      this.#busy.delete(run);
    }
  }

  /** Every input let go, once what is being answered over them is done; nothing is opened after. */
  async close(): Promise<void> {
    this.#closed = true;
    await Promise.allSettled([...this.#busy]);
    const held = await Promise.allSettled([...this.#held.values()]);
    this.#held.clear();
    for (const h of held) if (h.status === 'fulfilled') h.value.dispose();
  }
}
