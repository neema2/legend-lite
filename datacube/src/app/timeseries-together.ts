// GRIDS OPENED TOGETHER FOR A TIMESERIES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.2): a series that combines
// grids is ONE query (D2) -- over one model of the tables they read (join-open.ts openSources: a join's rule, for any
// number of sources), planned and run where the grids' rows are, as a join's grid is (join-flow.ts). Each grid is its
// source's relation over that model; the series' query writes the grid's calculated columns and filter over it
// (timeseries-query.ts), as its own query does. Where the tables are is the host's: the tab's own database
// (tabSources), or -- a page Python serves, which has none of its own there -- the host's, opened there by the same
// rule (services.ts HostJoin.together: headless joinTogether). A grid of another store is read once it is snapped into
// the tables' database (D3: its copy is a table there); until then it is refused in the join's words (leafRefusal).
// Fetched with the join's code (join-app.ts): a page that combines no grids loads none of it.

import { eachRelation, TAB_TABLES, openSources, type OpenedSource } from '../join-open.ts';
import { resolvedSource } from '../live-inputs.ts';
import type { PageApp } from '../page/page-app.ts';
import type { PlanAndRun } from '../runner.ts';
import type { TsTogether } from '../ui/timeseries-view.ts';
import { catalogColumns } from '../upload.ts';
import type { GridRows, LocalEngine, OpenedTogether, Services } from './services.ts';
import type { PageSources } from './sources.ts';

/** Where grids are opened together, and how a query over the model they make is planned and run. */
export interface TogetherOf {
  /** The grids, by their tiles' ids, over one model of the tables they read: the tab's (tabSources), or the host's. */
  readonly open: (grids: readonly string[]) => Promise<OpenedTogether>;
  /** A query over `model` planned and run where the grids' rows are, and that let go after (its planner, where the page
   *  made one). */
  readonly rows: (model: string, runtime: string) => { readonly run: PlanAndRun; letGo(): void };
}

/**
 * The grids of `grids` (by their tiles' ids) opened together, a query over their model planned and run where their rows
 * are -- refused in words when a grid reads no table of the tables' database, or one elsewhere. `dispose` lets the
 * model's planner go.
 */
export async function openTogether(grids: readonly string[], o: TogetherOf): Promise<TsTogether> {
  const opened = await o.open(grids);
  const rows = o.rows(opened.model, opened.runtime);
  return {
    relations: new Map(eachRelation(opened, grids)),
    run: async (query) => (await rows.run.host.planAndRun(query, 0)).rows,
    dispose: () => rows.letGo(),
  };
}

/** What the tab opens grids together over: the page, what it knows of each grid's source, and its own engine. */
export interface TabTablesOf {
  readonly page: PageApp;
  readonly sources: PageSources;
  readonly services: Services;
  readonly local: LocalEngine;
}

/**
 * The TAB'S OWN: the grids over one model of the tables of its database they read (openSources over TAB_TABLES) -- a
 * snapped grid its copy's table (the page's record of where its rows are: page-document.ts copies), as the grid itself
 * reads it then (the review's P3: a combination read it live, beside the same tile's series of it alone read from its
 * copy).
 */
export async function tabSources(grids: readonly string[], o: TabTablesOf): Promise<OpenedTogether> {
  const now = o.page.document(o.page.title);
  if (!now) throw new Error('the page cannot say what its grids read');
  const read = grids.map((id): { source: OpenedSource; tables: readonly string[] } => {
    const copy = now.copies?.[id];
    if (copy) return { source: { _type: 'copy', table: copy.table }, tables: [copy.table] };
    const cube = now.cubes.find((c) => c.id === id)?.cube;
    const tables = o.sources.place(id)?.tables;
    const title = o.page.views().views.find((v) => v.id === id)?.title ?? id;
    if (!cube || !tables) throw new Error(`${title} reads no table of this tab's database: a series of several grids reads them together there`);
    // a flow's box reads another's rows: its input resolved against the page first, as the box itself is read
    return { source: resolvedSource(now, cube.source), tables };
  });
  const typers: GridRows[] = [];
  try {
    const opened = await openSources(read.map((r) => r.source), {
      here: TAB_TABLES,
      tables: read.flatMap((r) => r.tables),
      models: o.local.tables,
      catalog: (table) => catalogColumns(o.local.engine, o.local.tables, table),
      databaseType: o.local.engine.databaseType,
      typer: (model, runtime) => {
        const rows = o.services.engine.rows(model, runtime);
        typers.push(rows);
        return rows.planner!;
      },
    });
    return { model: opened.model, runtime: opened.runtime, relations: opened.relations, notes: opened.notes };
  } finally {
    for (const t of typers) t.planner?.dispose?.();
  }
}
