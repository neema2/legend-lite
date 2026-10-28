// A PAGE ON A CONTEXT (docs/ONE_CONNECTION_DESK_2026_10_28.md D2): a page's saved queries over a data space run on one
// of its execution contexts -- legend's own `executionKey`, written on each saved query, so a saved page keeps the
// context it was saved on and opens on it (query-store context.ts `contextOf`; a query naming none runs on the data
// space's default). A page written on a context (the desk's example, on its host's) has that key on every saved query
// of the page over the data space; nothing else of the page changes. Open With ▸ opens a saved page on another context
// without writing it (app/open-with-menu.ts).

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
