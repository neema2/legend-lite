// CREATE AN ASSOCIATION… AND CREATE A DATA SPACE… OF THE PAGE (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1, §15.6a):
// the page's actions on its model, each one call to the model's writer (the host's: the tab's module, Python's native
// library), atomic, saved as one revision of DataCube's workspace. The facts an association's how-many is preset from
// are read where the host reads every grid's rows (PlanAndRun): a key unique over its class's rows -- the class's
// relation function over the project, or, a class still to be made, its grid's own rows. Loaded the first time either is asked for; it imports nothing the page loads at startup (its
// queries and its windows' shapes are handed in), so the page's own modules stay in its bundle.

import type { Lambda, ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { SdlcClient } from '../../sdlc-client/src/client.ts';
import type { PlanAndRun } from './runner.ts';
import type { CubeSnapshot } from './snapshot.ts';
import { createAssociation, createDataSpace, projectState } from '../../model-changes/src/index.ts';
import { PageModel, type PageModelRef } from './page-model.ts';
import type { ClassAction, GridSource, ModelAnswer, ModelWriter } from './model-writer.ts';
import { suggestedClassName, suggestedDataSpaceName, suggestedPackage } from './model-names.ts';
import { sampleOf } from './sample-rows.ts';
import { associationDialog } from './ui/association-dialog.ts';
import { dataSpaceDialog, type DataSpaceChoice, type DataSpaceGrid } from './ui/data-space-dialog.ts';
import type { CreatedClass } from './ui/class-dialog.ts';

/** A grid of the page, as the actions read it. */
export interface PageGrid {
  readonly id: string;
  readonly label: string;
  /** Its class, when its rows were made one (the page's model keeps it). */
  readonly cls?: string;
  /** Its rows at their finest level, the table it reads, its first rows and where its rows are read from, when it can be
   *  a class; else why not. */
  readonly made: {
    readonly query: Lambda;
    readonly relation: ValueSpecification;
    readonly table: { readonly database: string; readonly schema: string; readonly table: string };
    readonly sample: Lambda;
    readonly source: GridSource;
  } | { readonly refused: string };
  /** Where its rows are planned and run, on whichever host: its sample and its keys' facts are read there. */
  readonly rows: PlanAndRun;
}

/** A grid as the page hands it in: its state, what its header says, where its rows plan and run. */
export interface GridOnPage {
  readonly snapshot: CubeSnapshot;
  readonly label: string;
  readonly rows: PlanAndRun;
}

/** The query builders the page loads already (query.ts), handed in so this module imports none of the page's own. */
export interface GridQueries {
  readonly classLambda: (snapshot: CubeSnapshot) => Lambda;
  readonly storeTableOf: (source: ValueSpecification) => { readonly database: string; readonly schema: string; readonly table: string } | undefined;
  readonly sampleLambda: (source: ValueSpecification) => Lambda;
}

/** The page's grids as the actions read them: each a class already, or its rows as a class's, or why they cannot be. */
export function pageGrids(ids: readonly string[], gridOf: (id: string) => GridOnPage | undefined,
  classOf: (id: string) => string | undefined, q: GridQueries): PageGrid[] {
  return ids.flatMap((id) => {
    const g = gridOf(id);
    if (!g) return [];
    const table = q.storeTableOf(g.snapshot.source.query);
    const source = g.rows.host.planner.source?.();
    let made: PageGrid['made'];
    try {
      if (!table) throw new Error('its source is not a table of its own');
      if (!source) throw new Error('its planner does not say where its rows are read from');
      const query = q.classLambda(g.snapshot);
      made = { query, relation: query.body[0]!, table, sample: q.sampleLambda(g.snapshot.source.query), source };
    } catch (e) {
      made = { refused: e instanceof Error ? e.message : String(e) };
    }
    const cls = classOf(id);
    return [{ id, label: g.label, made, rows: g.rows, ...(cls !== undefined ? { cls } : {}) }];
  });
}

export interface ModelActionsOf {
  /** The model's writer: the host's (the tab's module, Python's native library). */
  readonly writer: ModelWriter;
  /** The name a class's model gives the host's database (its store, connection and runtime): `Browser`, `Python`. */
  readonly place: string;
  /** A project's queries planned by the writer and run on the host's database: where a class's facts are read. */
  readonly rows: (model: string, runtime: string) => PlanAndRun;
  readonly pageName: string;
  readonly model: PageModelRef | undefined;
  readonly client: SdlcClient;
  /** Where the model home keeps the projects, as a person reads it (its `where`: this browser, this user's folder). */
  readonly where: string;
  readonly grids: readonly PageGrid[];
  /** Studio's editor on a workspace, where the host serves Studio. */
  readonly studio?: (project: string, workspace: string) => string;
  readonly persist: () => Promise<boolean>;
  /** The query that finds a column's first value held twice (query.ts duplicateLambda). */
  readonly duplicate: (relation: ValueSpecification, column: string) => Lambda;
  /** A call of a relation function with no parameters, as a value. */
  readonly call: (fn: string) => ValueSpecification;
}

/** Whether the planned query answers no row: the column is unique over the rows; undefined when it cannot be run. */
async function noRow(rows: PlanAndRun, query: Lambda): Promise<boolean | undefined> {
  try {
    return (await rows.host.planAndRun(query, 0)).rows.rowCount === 0;
  } catch {
    // the rows are not in this tab (a class made in another browser, its table not opened here): asked, not guessed
    return undefined;
  }
}

/** The facts' rows over the project's files: the place's runtime, through the model's mapping. */
function projectRows(o: ModelActionsOf, project: string, pkg: string): PlanAndRun {
  return o.rows(project, `${pkg}::runtime::${o.place}`);
}

/** The page's model after the window: made or changed, or undefined when it is as it was. */
function after(model: PageModel, o: ModelActionsOf): PageModelRef | undefined {
  return model.ref !== o.model ? model.ref : undefined;
}

/**
 * CREATE AN ASSOCIATION… between two of the page's classes. `first`: a class to start one end with (a grid's own).
 * Refused in words when the page has fewer than two classes.
 */
export async function createAssociationOf(o: ModelActionsOf, first?: string): Promise<PageModelRef | undefined> {
  if (!o.model) throw new NotYet('An association relates two of the page\'s classes: make them first (Create a Class…, or Create a Data Space… for the whole page).');
  const model = new PageModel(o.model);
  const pkg = o.model.package;
  const state = await projectState(o.client, o.model);
  const start = await o.writer.createAssociation(state.text, pkg, undefined);
  if (start.joinable.length < 2) throw new NotYet('An association relates two classes, and the page\'s project has one: make another first.');
  const facts = projectRows(o, state.text, pkg);
  try {
    await associationDialog(document, {
      project: o.model.project,
      joinable: start.joinable,
      suggestions: start.suggestions,
      ...(first !== undefined ? { first } : {}),
      unique: async (cls, on) => {
        const j = start.joinable.find((x) => x.class === cls);
        const column = j?.keys.find((k) => k.name === on)?.column;
        if (j?.function === undefined || column === undefined) return undefined;
        return noRow(facts, o.duplicate(o.call(j.function), column));
      },
      preview: (a) => o.writer.createAssociation(state.text, pkg, a),
      create: async (a): Promise<CreatedClass> => {
        const made = (await createAssociation({ sdlc: o.client, writer: o.writer, ...model.into(o.pageName, pkg), association: a }))
          .answer;
        const kept = await o.persist();
        const at = model.ref!;
        return {
          className: made.associations[0]?.association ?? 'the association',
          where: `the project ${at.project} (workspace ${at.workspace}), in ${o.where}`,
          ...(o.studio ? { studio: o.studio(at.project, at.workspace) } : {}),
          notes: kept ? [] : ['This browser has not promised to keep its storage: it may clear the project.'],
        };
      },
    });
  } finally {
    facts.host.planner.dispose?.();
  }
  return after(model, o);
}

/**
 * CREATE A DATA SPACE… of the whole page, in one go: its grids not yet classes made classes in the same window, the
 * associations it suggests offered, all one revision (the project made with it, when the page has none yet).
 */
export async function createDataSpaceOf(o: ModelActionsOf): Promise<PageModelRef | undefined> {
  const model = new PageModel(o.model);
  const pkg = o.model?.package ?? suggestedPackage(o.pageName);
  const state = o.model ? await projectState(o.client, o.model) : { text: '', revision: '' };
  // each grid not yet a class, as a class action: its first rows read once
  const samples = new Map<string, Awaited<ReturnType<typeof sampleOf>>>();
  for (const g of o.grids) {
    if (g.cls === undefined && !('refused' in g.made)) samples.set(g.id, await sampleOf(g.rows.host, g.made.sample));
  }
  const actions = (choice: DataSpaceChoice): ClassAction[] => choice.made.flatMap((m) => {
    const g = o.grids.find((x) => x.id === m.id);
    if (!g || 'refused' in g.made) return [];
    return [{
      query: g.made.query, source: g.made.source, name: m.className,
      where: { from: g.made.table.database, as: o.place, samples: { [g.made.table.table]: samples.get(g.id)! } },
    }];
  });
  let last: ModelAnswer | undefined;
  let lastChoice: DataSpaceChoice | undefined;
  let previews = 0;
  const facts = projectRows(o, state.text, pkg);
  const grids: DataSpaceGrid[] = o.grids.filter((g) => g.cls !== undefined || !('refused' in g.made)).map((g) => ({
    id: g.id, label: g.label, className: suggestedClassName(g.label), ...(g.cls !== undefined ? { cls: g.cls } : {}),
  }));
  try {
    await dataSpaceDialog(document, {
      project: { name: o.model?.project ?? o.pageName, isNew: o.model === undefined },
      name: suggestedDataSpaceName(o.pageName),
      grids,
      unique: async (cls, on) => {
        // a class of the project: its relation function over the project's files
        const j = last?.joinable.find((x) => x.class === cls);
        const column = j?.keys.find((k) => k.name === on)?.column;
        const made = lastChoice?.made.find((m) => `${pkg}::domain::${m.className}` === cls);
        if (made === undefined && j?.function !== undefined && column !== undefined && state.text !== '') {
          return noRow(facts, o.duplicate(o.call(j.function), column));
        }
        // a class still to be made: its grid's own rows
        const g = made ? o.grids.find((x) => x.id === made.id) : undefined;
        const property = last?.classes.find((c) => c.class === cls)?.properties.find((p) => p.property === on);
        if (!g || 'refused' in g.made || property === undefined) return undefined;
        return noRow(g.rows, o.duplicate(g.made.relation, property.column));
      },
      preview: async (choice) => {
        // the facts read the newest answer: one overtaken while it was asked is the window's to drop, and not kept here
        const mine = ++previews;
        const answer = await o.writer.createDataSpace(state.text, pkg, dataSpaceOf(choice), actions(choice),
          choice.associations);
        if (mine === previews) {
          last = answer;
          lastChoice = choice;
        }
        return answer;
      },
      create: async (choice): Promise<CreatedClass> => {
        const into = model.into(choice.projectName ?? o.pageName, pkg);
        const done = await createDataSpace({
          sdlc: o.client, writer: o.writer, ...into,
          dataSpace: dataSpaceOf(choice), classes: actions(choice),
          ...(choice.associations ? { associations: choice.associations } : {}),
        });
        const made = done.answer;
        // each grid made a class keeps its table and its class
        for (const m of choice.made) {
          const g = o.grids.find((x) => x.id === m.id);
          if (!g || 'refused' in g.made) continue;
          model.keepTable(g.id, g.made.table.table);
          model.keepClass(g.id, `${into.package}::domain::${m.className}`);
        }
        const kept = await o.persist();
        const at = model.ref ?? { project: done.project, workspace: done.workspace };
        return {
          className: made.dataSpace ?? choice.name,
          where: `the project ${at.project} (workspace ${at.workspace}), in ${o.where}`,
          ...(o.studio ? { studio: o.studio(at.project, at.workspace) } : {}),
          notes: kept ? [] : ['This browser has not promised to keep its storage: it may clear the project.'],
        };
      },
    });
  } finally {
    facts.host.planner.dispose?.();
  }
  return after(model, o);
}

function dataSpaceOf(choice: DataSpaceChoice): { name: string; title?: string; description?: string } {
  return {
    name: choice.name,
    ...(choice.title !== undefined ? { title: choice.title } : {}),
    ...(choice.description !== undefined ? { description: choice.description } : {}),
  };
}

/** Refused before a window opens: said in words, nothing written. */
export class NotYet extends Error {
  constructor(detail: string) {
    super(detail);
    this.name = 'NotYet';
  }
}

