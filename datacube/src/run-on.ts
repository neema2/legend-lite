// RUN ON (docs/ONE_CONNECTION_DESK_2026_10_28.md D2): a page's saved queries over a data space run on one of its
// execution contexts -- legend's own `executionKey`, written on each saved query, so a saved page keeps the context it
// was saved on and opens on it (query-store context.ts `contextOf`; a query naming none runs on the data space's
// default). Running a page on another context writes that key on every saved query of the page over the data space;
// nothing else of the page changes. Which contexts a data space has, and whether this page can reach one, is the
// model's and the host's.

import type { PageDocument } from './page-document.ts';

/** `page` with every saved query over the data space `dataSpace` run on its execution context named `context`. */
export function runOn(page: PageDocument, dataSpace: string, context: string): PageDocument {
  if (context === '') throw new Error(`an execution context of ${dataSpace} is named`);
  return {
    ...page,
    cubes: page.cubes.map((c) => {
      const source = c.cube.source;
      if (source._type !== 'savedQuery') return c;
      const at = source.query.executionContext;
      if (at?._type !== 'dataSpaceExecutionContext' || at.dataSpacePath !== dataSpace) return c;
      const query = { ...source.query, executionContext: { ...at, executionKey: context } };
      return { ...c, cube: { ...c.cube, source: { ...source, query } } };
    }),
  };
}

/** The execution context each data space a page's saved queries name runs on now (`null`: its default), by the data
 *  space's path: what a page's Run on shows. A page whose queries of one data space name two contexts says both. */
export function runningOn(page: PageDocument): ReadonlyMap<string, ReadonlySet<string | null>> {
  const on = new Map<string, Set<string | null>>();
  for (const { cube } of page.cubes) {
    const source = cube.source;
    if (source._type !== 'savedQuery') continue;
    const at = source.query.executionContext;
    if (at?._type !== 'dataSpaceExecutionContext') continue;
    const keys = on.get(at.dataSpacePath) ?? new Set<string | null>();
    keys.add(at.executionKey ?? null);
    on.set(at.dataSpacePath, keys);
  }
  return on;
}
