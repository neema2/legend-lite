// WHAT A HOST SERVES A PAGE'S CUBES OVER, headless (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B7, X1, X3, D13, D14): the
// tables of its database a cube's leaves are read from -- its frames, the files it read in, the remote files it mounted
// -- and what its engine reads beside them (a class, a saved query, a warehouse table), and the one rule of where each
// leaf is read, which every grid call's base, a join's tables and the page keeper's rebase ask. No DOM, no window.

import { DepotClient } from '../../../depot-client/src/client.ts';
import { modelText } from '../../../depot-client/src/model-text.ts';
import { lambda, toJson, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { classOpened, type ClassElement } from '../class-source.ts';
import {
  isLive, type ClassSource, type CubeSource, type JoinInput, type JoinSource, type LeafSource, type QuerySource,
  type RowsSource, type WarehouseSource,
} from '../cube-document.ts';
import { HOST_TABLES, leafRefusal, openedLeaves } from '../join-open.ts';
import { LitePlanner } from '../lite-planner.ts';
import { savedQueryOpened, type QueryReading } from '../saved-queries.ts';
import { CubeRefusal } from '../snapshot.ts';
import { hostCalls, hostEngine, hostFetch, hostWorker, type HostCalls } from './host.ts';

/** A frame as its host serves it (Python's Frames table): its model, its runtime and the accessor that reads it. */
export interface HostFrame {
  readonly model: string;
  readonly runtime: string;
  readonly accessor: string;
}

/** A grid's base on the host, as every grid call is given it (GridSpec without its cube): its model and runtime, the
 *  relation it reads -- Pure text (a frame's accessor) or a lambda as the protocol writes it -- and, read through a
 *  mapping (a class, a saved query), that mapping and the model's enumerations. Its rows are the host's plan-and-run's
 *  to read (D16): from the tables the host serves -- or, a saved query's, its project's own (`project`: C5, F28). */
export type HostBase = {
  readonly model: string; readonly runtime: string;
  readonly mapping?: string; readonly enumerations?: readonly string[];
  /** Its rows its project's own (a saved query's: planner.ts ModelOptions.project, F28). */
  readonly project?: true;
} & ({ readonly source: string } | { readonly relation: string });

/** The file a host read in as a table, as a cube records it: a file's name, size and bytes' hash; a remote file's URL. */
export type HostLeaf =
  | { readonly _type: 'file'; readonly name: string; readonly size: number; readonly sha256: string }
  | { readonly _type: 'remoteFile'; readonly name: string; readonly url: string };

/** A table of the host's database a page's file was read into, or a remote file is read through (Python's Frames'
 *  files): its model and runtime, the relation that reads it as DataCube read it in (headless ingest's and mount's
 *  `source`, as the protocol writes one), its table, and the file it is. */
export interface HostFile {
  readonly model: string;
  readonly runtime: string;
  readonly source: unknown;
  readonly table: string;
  readonly leaf: HostLeaf;
}

/**
 * WHAT A HOST SERVES A PAGE'S CUBES OVER (every grid call's `host`; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1, X3):
 * its frames by name, the files it read in by their bytes' SHA-256, the remote files it mounted by URL.
 */
export interface HostTables {
  readonly frames: Readonly<Record<string, HostFrame>>;
  readonly files: Readonly<Record<string, HostFile>>;
  readonly remotes: Readonly<Record<string, HostFile>>;
}

/** Where a cube's leaf is read on its host (leafOnHost): its base, the table of the host's database it reads -- a
 *  frame's, a file's, a remote file's; none, a leaf read elsewhere (a class, a saved query, a warehouse table) -- and, a
 *  file's or remote file's, the file it is read as now. */
export interface LeafOnHost {
  readonly base: HostBase;
  readonly table?: string;
  readonly leaf?: HostLeaf;
}

/**
 * WHERE A CUBE'S LEAF IS READ ON ITS HOST, the one rule (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B7, X1, D13, D14): a
 * frame the host serves, by name; a file -- as the page opens, the file given for it under its name (`given`: a file's
 * name, the hash of the file given for it), else the one it read in of the same bytes; a remote file it mounted, by URL
 * -- each read on the host's database. A class, read through its project at its revision in the host's model home (as
 * the tab opens one: class-source.ts classOpened); a saved query, through its project's version in the host's depot
 * (saved-queries.ts savedQueryOpened); a warehouse table, under the session the host's engine holds for its warehouse,
 * signed in by a tab (D13). Every base's rows read by the host's plan-and-run, which knows where they are (D16). What
 * is not read here is said in words.
 */
export async function leafOnHost(leaf: LeafSource | HostLeaf, host: HostTables, given: Readonly<Record<string, string>> = {}):
  Promise<LeafOnHost> {
  switch (leaf._type) {
    case 'frame': {
      const frame = host.frames[leaf.name];
      if (frame === undefined) throw new CubeRefusal(`the frame ${leaf.name} is not served`);
      return { base: { model: frame.model, runtime: frame.runtime, source: frame.accessor }, table: leaf.name };
    }
    case 'file': {
      const file = (given[leaf.name] !== undefined ? host.files[given[leaf.name]!] : undefined) ?? host.files[leaf.sha256];
      if (file === undefined) throw new CubeRefusal(`the file ${leaf.name} is not read here: the page is given its file as it opens`);
      return { base: fileBase(file), table: file.table, leaf: file.leaf };
    }
    case 'remoteFile': {
      const remote = host.remotes[leaf.url];
      if (remote === undefined) throw new CubeRefusal(`the remote file ${leaf.url} is not mounted here`);
      return { base: fileBase(remote), table: remote.table, leaf: remote.leaf };
    }
    case 'class':
      return { base: await classBase(leaf) };
    case 'savedQuery':
      return { base: await savedQueryBase(leaf) };
    case 'warehouseTable':
      return { base: await warehouseBase(leaf) };
  }
}

/** A file's or remote file's base: the relation DataCube read it in by, as a lambda. */
function fileBase(file: HostFile): HostBase {
  return { model: file.model, runtime: file.runtime, relation: JSON.stringify({ _type: 'lambda', parameters: [], body: [file.source] }) };
}

/** A lambda of no parameters reading `source`, as the protocol's JSON text: a base's relation. */
const relationOf = (source: ValueSpecification): string => toJson(lambda([], source));

/** The host's compiler, for what a project's text is (its elements, a lambda parsed): no model of its own. */
function compilerOf(calls: HostCalls): { readonly reading: QueryReading; readonly dispose: () => void } {
  const planner = new LitePlanner({ model: '', runtime: '', worker: () => hostWorker(calls) });
  return {
    reading: { elements: (text) => planner.modelElements(text), parse: (text) => planner.parse(text) },
    dispose: () => planner.dispose(),
  };
}

/** A class's base: its project at its revision, read from the host's model home (its SDLC, by the host's fetch). */
async function classBase(leaf: ClassSource): Promise<HostBase> {
  const calls = hostCalls();
  const client = new SdlcClient('/sdlc/api', hostFetch(calls));
  const { reading, dispose } = compilerOf(calls);
  try {
    let files: Awaited<ReturnType<SdlcClient['pure']>>;
    try {
      files = await client.pure({ project: leaf.project, workspace: leaf.workspace, revision: leaf.revision });
    } catch (e) {
      throw new CubeRefusal(`${leaf.name} is a class of ${leaf.project}, which this host's model home cannot read at `
        + `revision ${leaf.revision}: ${e instanceof Error ? e.message : String(e)}`);
    }
    const { model, source } = await classOpened(leaf, files, async (text) => await reading.elements(text) as ClassElement[]);
    return { model, runtime: leaf.runtime, mapping: leaf.mapping, relation: relationOf(source) };
  } finally {
    dispose();
  }
}

/** A saved query's base: its project's version, read from the host's depot (by the host's fetch), typed by the host's
 *  compiler through the query's own runtime and mapping. */
async function savedQueryBase(leaf: QuerySource): Promise<HostBase> {
  const calls = hostCalls();
  const q = leaf.query;
  const { reading, dispose } = compilerOf(calls);
  const typers: LitePlanner[] = [];
  try {
    let model: string;
    try {
      model = await modelText(new DepotClient('/depot/api', hostFetch(calls)), q.groupId, q.artifactId, q.versionId);
    } catch (e) {
      throw new CubeRefusal(`${leaf.name} is a saved query of ${q.groupId}:${q.artifactId}:${q.versionId}, which this host's `
        + `depot does not hold: ${e instanceof Error ? e.message : String(e)}`);
    }
    const { runtime, how, source } = await savedQueryOpened(q, model, reading, (at, options) => {
      const typer = new LitePlanner({ model, runtime: at, worker: () => hostWorker(calls), ...options });
      typers.push(typer);
      return typer;
    });
    return { model, runtime, ...how, relation: relationOf(source) };
  } finally {
    dispose();
    for (const t of typers) t.dispose();
  }
}

/** A warehouse table's base: opened by the host's engine under the session it holds for its warehouse (D13), as the
 *  tab on the host opens one -- refused in words while no tab has signed in to it. */
async function warehouseBase(leaf: WarehouseSource): Promise<HostBase> {
  const link = hostEngine(hostCalls());
  const answer = await link.fetch('/warehouse.json', {
    method: 'POST',
    body: JSON.stringify({ op: 'table', url: leaf.warehouse, catalog: leaf.catalog, schema: leaf.schema, name: leaf.table }),
  });
  const text = await answer.text();
  if (answer.status === 401) {
    throw new CubeRefusal(`${leaf.name} is read on the warehouse at ${leaf.warehouse}, which no tab of this host has signed `
      + 'in to: sign in to it in DataCube first');
  }
  if (!answer.ok) throw new CubeRefusal(`${leaf.name} is not opened on the warehouse at ${leaf.warehouse}: ${text}`);
  const opened = JSON.parse(text) as { model: string; runtime: string; source: unknown };
  return { model: opened.model, runtime: opened.runtime, relation: JSON.stringify({ _type: 'lambda', parameters: [], body: [opened.source] }) };
}

/**
 * WHAT A PAGE ON THE HOST CAN HOLD, the one rule (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B7, X1): a cube over a leaf
 * the host reads (leafOnHost), or over a join of such leaves read on the host's database (the join, and the table each
 * leaf reads, in order: leafTables) -- else refused in words. Every grid call's base asks it (gridBase, gridOpened), and
 * so does the keeper of each cube a write adds or moves to another source (headless page.ts pageRebase: the S4 audit's 2).
 */
export async function onHost(source: CubeSource, host: HostTables, given: Readonly<Record<string, string>> = {}):
  Promise<{ readonly leaf: LeafOnHost } | { readonly join: JoinSource; readonly tables: string[] } | { readonly rows: FrozenRows }> {
  if (source._type === 'rows') {
    // another grid's rows (docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.1): held where its input's source is, its link
    // resolved against its page first (live-inputs.ts resolvedSource) -- an unresolved one is refused, never guessed
    const input = source.input;
    if (isLive(input)) throw new Error(`${source.name} reads grid '${input.grid}' live: resolve it against its page first`);
    // read by its grid's copy (a cached box, FL10): a table of the host's, wherever its own source is read
    if (input.copy === undefined) await onHost(input.source, host, given);
    return { rows: { ...source, input } };
  }
  if (source._type !== 'join') return { leaf: await leafOnHost(source, host, given) };
  return { join: source, tables: await leafTables(source, host, given) };
}

/** Another grid's rows with its input resolved: a copy of what that grid is now (live-inputs.ts gridInput). */
export type FrozenRows = RowsSource & { readonly input: JoinInput };

/** The table of the host's database each leaf of `source` reads, in its order (cube-document.ts leavesOf): a join's
 *  inputs' tables, as a join over the host's tables is opened (headless join.ts). A leaf read elsewhere is refused as a
 *  join refuses it (join-open.ts leafRefusal): a join reads both its inputs in one place. */
export async function leafTables(source: CubeSource, host: HostTables, given: Readonly<Record<string, string>> = {}): Promise<string[]> {
  const tables: string[] = [];
  for (const leaf of openedLeaves(source)) {
    // a snapped grid's copy (a cached box, FL10): a table the host holds
    if (leaf._type === 'copy') {
      tables.push(leaf.table);
      continue;
    }
    const refused = leafRefusal(leaf, HOST_TABLES);
    if (refused !== undefined) throw new CubeRefusal(refused);
    tables.push((await leafOnHost(leaf, host, given)).table!);
  }
  return tables;
}
