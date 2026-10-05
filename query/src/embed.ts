// Query's builder, embedded (plan A5): what Studio opens for a service's query (upstream's "Edit Query") and for a
// class (its "Query…") -- the same screen Query shows, over the host's model and engine, as upstream Studio embeds
// @finos/legend-query-builder. The host holds the query: Save Query hands it the query's Pure text, and nothing is
// read from or written to a query store.

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { PureModelContextData } from '../../engine-client/src/legend/pmcd.ts';
import type { WasmGrammar } from '../../engine-client/src/legend/wasm-grammar.ts';
import { QueryStoreClient } from '../../query-store/src/client.ts';
import type { Engine } from './backend/engine.ts';
import { queryOn } from './builder/milestoning.ts';
import { AppContext, gavOf, type CubeRows, type ExecutionConfig, type LoadedProject, type ProjectConfig } from './app/context.ts';
import { sessionFrom } from './app/persist.ts';
import { Session } from './app/session.ts';
import { ModelGraph } from './model/graph.ts';
import { renderEditor, type EditorHandle, type EmbedHost } from './ui/editor.ts';

export type { EditorHandle, EmbedHost };

/** What the builder opens on: a query the host holds (a service's), or a new one on a mapped class. */
export type EmbedStart =
  | { readonly kind: 'lambda'; readonly lambda: Lambda; readonly mapping: string; readonly runtime: string }
  /** A new query on `class`: its mapping and runtime the given ones, else the first the model has for it. */
  | { readonly kind: 'class'; readonly class: string; readonly mapping?: string; readonly runtime?: string }
  /** A mapping executed (upstream's mapping execution): a new query on the first class it maps, on a runtime for it. */
  | { readonly kind: 'mapping'; readonly mapping: string };

export interface EmbedOptions {
  /** Where queries run, as the host has it: in this tab, or on a legend server. */
  readonly execution: ExecutionConfig;
  readonly engine: Engine;
  /** legend-lite's planner in the tab, when the host has one: the SQL shown, the results cube's planning. */
  readonly planner: WasmGrammar | undefined;
  /** Where the results grid (a DataCube) reads its rows. */
  readonly cubeRows: CubeRows;
  readonly user: string;
  /** The host's whole model: its text (what execution sends) and its JSON (what the screens read). */
  readonly model: { readonly code: string; readonly json: PureModelContextData };
  readonly start: EmbedStart;
  readonly host: EmbedHost;
}

/** The host's model as the builder's one project: it has no coordinates of its own (it is a workspace, not a version). */
const WORKSPACE: ProjectConfig = { groupId: 'workspace', artifactId: 'workspace', versionId: 'workspace', title: 'Workspace', models: [] };

export function openBuilder(root: HTMLElement, o: EmbedOptions): EditorHandle {
  const project: LoadedProject = {
    config: WORKSPACE, gav: gavOf(WORKSPACE), context: { _type: 'text', code: o.model.code }, graph: new ModelGraph(o.model.json),
  };
  // the one client over a store that is not there: the embedded header has none of the store's actions
  const noStore = new QueryStoreClient('embedded', () => Promise.reject(new Error('an embedded query builder keeps its query in its host, not a query store')));
  const app = new AppContext({ execution: o.execution, projects: [] }, o.engine, noStore, o.planner, [project], o.user, o.cubeRows);
  return renderEditor(root, app, sessionOf(project, o.start), o.host);
}

function sessionOf(project: LoadedProject, start: EmbedStart): Session {
  if (start.kind === 'lambda') return sessionFrom(project, start.lambda, { mapping: start.mapping, runtime: start.runtime });
  const graph = project.graph;
  const cls = start.kind === 'class' ? start.class : graph.mappedClasses(start.mapping)[0];
  if (!cls) throw new Error(`the mapping ${start.kind === 'mapping' ? start.mapping : ''} maps no class`);
  const mapping = start.mapping ?? graph.mappingsFor(cls)[0];
  if (!mapping) throw new Error(`no mapping in this workspace maps ${cls}`);
  const runtime = (start.kind === 'class' ? start.runtime : undefined) ?? graph.runtimesFor(mapping)[0];
  if (!runtime) throw new Error(`no runtime in this workspace runs the mapping ${mapping}`);
  return new Session(project, queryOn(graph, { kind: 'class', class: cls, mapping, runtime }));
}
