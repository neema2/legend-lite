// A SAVED QUERY'S EXECUTION CONTEXT (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C4): the mapping and runtime it runs on,
// read alike by every app that opens one -- Query (persist.ts) and DataCube (saved-queries.ts), each handing in how it
// finds a data space in the query's project (Query's model graph, DataCube's model elements). Upstream's two contexts:
// an explicit mapping and runtime, or a data space's execution context (its key, else the data space's default). A
// context's mapping is its own, or the one its `mappingProvider` names: a data product's model access group (4.145.0;
// docs/DEPLOYMENT_DESIGN_2026_11_14.md §7).

import type { Query } from './wire.ts';

/** A data space's execution contexts, as both apps' models read one. */
export interface DataSpaceContexts {
  readonly defaultExecutionContext?: string;
  readonly executionContexts?: readonly {
    readonly name: string;
    readonly mapping?: { readonly path: string };
    readonly mappingProvider?: { readonly element: { readonly path: string }; readonly keys: readonly string[] };
    readonly defaultRuntime?: { readonly path: string };
  }[];
}

/** A data product's access point groups, as both apps' models read one: a model group's mapping by its id. */
export interface DataProductGroups {
  readonly accessPointGroups?: readonly { readonly id: string; readonly mapping?: { readonly path: string } }[];
}

/** The mapping a context's `mappingProvider` names: the one model group its one key names, else refused by name. */
function providedMapping(
  provider: { readonly element: { readonly path: string }; readonly keys: readonly string[] },
  dataProduct: ((path: string) => DataProductGroups | undefined) | undefined,
): string {
  const product = dataProduct?.(provider.element.path);
  if (!product) throw new Error(`the context's mapping provider ${provider.element.path} is no data product of its project`);
  if (provider.keys.length !== 1) {
    throw new Error(`the context's mapping provider ${provider.element.path} names ${provider.keys.length} groups: one model access group is its mapping`);
  }
  const group = product.accessPointGroups?.find((g) => g.id === provider.keys[0]);
  if (!group?.mapping) throw new Error(`the data product ${provider.element.path} has no model access group '${provider.keys[0]}'`);
  return group.mapping.path;
}

/** Where a saved query runs: its mapping and runtime, and the data space's they came from, when one. */
export interface QueryContext {
  readonly mapping: string;
  readonly runtime: string;
  readonly dataSpace?: { readonly path: string; readonly context: string };
}

/** The project a saved query belongs to, as a configuration's `projects[]` names one (`group:artifact:version`). */
export function projectOf(q: Pick<Query, 'groupId' | 'artifactId' | 'versionId'>): string {
  return `${q.groupId}:${q.artifactId}:${q.versionId}`;
}

/** The mapping and runtime a saved query runs on; `dataSpace` finds a data space of its project by its path, and
 *  `dataProduct` a data product (what a context's mapping provider names). */
export function contextOf(
  q: Pick<Query, 'executionContext' | 'groupId' | 'artifactId' | 'versionId'>,
  dataSpace: (path: string) => DataSpaceContexts | undefined,
  dataProduct?: (path: string) => DataProductGroups | undefined,
): QueryContext {
  const ctx = q.executionContext;
  if (ctx?._type === 'explicitExecutionContext') {
    if (!ctx.mapping) throw new Error("the query's explicit execution context has no mapping");
    if (!ctx.runtime) throw new Error("the query's explicit execution context has no runtime");
    return { mapping: ctx.mapping, runtime: ctx.runtime };
  }
  if (ctx?._type === 'dataSpaceExecutionContext' && ctx.dataSpacePath) {
    const path = ctx.dataSpacePath;
    const ds = dataSpace(path);
    if (!ds) throw new Error(`the query's data space ${path} is not in ${projectOf(q)}`);
    const key = ctx.executionKey ?? ds.defaultExecutionContext;
    const ec = ds.executionContexts?.find((c) => c.name === key);
    if (key === undefined || !(ec?.mapping || ec?.mappingProvider) || !ec.defaultRuntime) {
      throw new Error(`the query's data space has no execution context '${key ?? ''}' with a mapping and a runtime`);
    }
    const mapping = ec.mapping?.path ?? providedMapping(ec.mappingProvider!, dataProduct);
    return { mapping, runtime: ec.defaultRuntime.path, dataSpace: { path, context: key } };
  }
  throw new Error('the query has no execution context legend-lite serves (explicit, or a data space)');
}
