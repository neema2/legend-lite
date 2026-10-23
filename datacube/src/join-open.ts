// OPENING A JOIN (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3, §17.4): a join source made one relation over one
// model. Its leaves -- the inputs that read a table straight, depth first -- are tables of the host's database; one model
// declares them all (the writer's several tables, infer.ts tablesModel); each input is its cube's rows at their finest
// level over its source (query.ts classLambda), a join inside an input opened the same way; then the two are joined
// (join.ts joinRelation), and the compiler types what comes out. What drifted since it was saved is said. No DOM: the
// tab's sources (app/sources.ts) and a test on Node's DuckDB (test/join-open.test.ts) call it alike.

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import {
  CUBE_KIND, CUBE_VERSION, leavesOf, openCube, type CubeDocument, type CubeSource, type JoinInput, type JoinSource, type LeafSource,
} from './cube-document.ts';
import type { CatalogColumn, TableModels } from './infer.ts';
import { MOST_REPEATS, REPEATED_KEYS, joinRelation, type JoinHow, type JoinSide, type JoinSummaryCounts, type JoinSummaryQueries } from './join.ts';
import { TOTAL_ROWS_COLUMN, classLambda } from './query.ts';
import type { RawTable } from '../../engine-client/src/engine.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { sourceColumns, type RelationTyper } from './source-columns.ts';
import { CubeRefusal, type ColumnSpec } from './snapshot.ts';

/** The kinds of source that are tables of a host's database: the tab's files and remote files; Python's frames too. */
export type TableKind = 'file' | 'remoteFile' | 'frame';
/** The tab's: what its own DuckDB holds. */
export const TAB_TABLES: readonly TableKind[] = ['file', 'remoteFile'];
/** A host's (Python's): its frames, and the files and remote files it read in (headless host-tables.ts leafTables). */
export const HOST_TABLES: readonly TableKind[] = ['frame', 'file', 'remoteFile'];

const WHAT: Readonly<Record<Exclude<CubeSource['_type'], 'join'>, string>> = {
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
    + `${leaf.name} is ${WHAT[leaf._type]}`;
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

/** An input as a cube document, for openCube: its configuration the product's, nothing open. */
function asCube(input: JoinInput): CubeDocument {
  return {
    kind: CUBE_KIND, version: CUBE_VERSION, name: input.name, source: input.source, query: input.query,
    configuration: {}, tree: { open: [], showTotals: false },
  };
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
 * THE TWO INPUTS OF A JOIN over one model: each leaf's table of the host's database declared once (a table read twice is
 * one table), each input its cube's rows at their finest level over its source -- refused in words when a leaf is read
 * elsewhere (leafRefusal) or an input is pivoted (its columns are its values).
 */
export async function openInputs(left: JoinInput, right: JoinInput, o: JoinOpening): Promise<OpenedInputs> {
  const leaves = [...leavesOf(left.source), ...leavesOf(right.source)];
  for (const leaf of leaves) {
    const why = leafRefusal(leaf, o.here);
    if (why !== undefined) throw new CubeRefusal(why);
  }
  if (o.tables.length !== leaves.length) {
    throw new Error(`a join of ${leaves.length} tables was given ${o.tables.length} (${o.tables.join(', ')})`);
  }
  const distinct = [...new Set(o.tables)];
  const written = await o.models.tablesModel(await Promise.all(distinct.map(async (table) => ({
    table,
    // as each one's own model reads it: an upload rewritten at ingest and a frame convert, a remote file's view cannot
    convertible: leaves[o.tables.indexOf(table)]!._type !== 'remoteFile',
    columns: await o.catalog(table),
  }))), o.databaseType);
  const relationOf = new Map(distinct.map((t, i) => [t, written.tables[i]!.source]));
  const typer = o.typer(written.model, written.runtime);
  const notes: string[] = [];
  let next = 0;

  const relation = async (s: CubeSource): Promise<ValueSpecification> => {
    if (s._type !== 'join') return relationOf.get(o.tables[next++]!)!;
    const l = await side(s.left);
    const r = await side(s.right);
    return joinRelation(l, r, reconciled(s.how, r, notes));
  };
  const side = async (input: JoinInput): Promise<JoinSide> => {
    const over = await relation(input.source);
    const opened = openCube(asCube(input), { query: over }, await sourceColumns(typer, over));
    for (const n of opened.notes) notes.push(`${input.name}: ${n}`);
    if (opened.snapshot.pivotOn.length > 0) {
      throw new CubeRefusal(`${input.name} is pivoted: its columns are its values, so it joins as no fixed columns; take its pivot off first`);
    }
    const rows = classLambda(opened.snapshot).body[0]!;
    const columns = (await sourceColumns(typer, rows)).map((c) => ({ name: c.name, type: c.type }));
    return { name: input.name, relation: rows, columns };
  };

  const l = await side(left);
  const r = await side(right);
  return { model: written.model, runtime: written.runtime, typer, left: l, right: r, notes };
}

/** A JOIN SOURCE OPENED (§17.4): its inputs over one model (openInputs), joined, typed. */
export async function openJoin(source: JoinSource, o: JoinOpening): Promise<OpenedJoin> {
  const inputs = await openInputs(source.left, source.right, o);
  const notes = [...inputs.notes];
  const how = reconciled(source.how, inputs.right, notes);
  const relation = joinRelation(inputs.left, inputs.right, how);
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
