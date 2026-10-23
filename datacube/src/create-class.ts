// CREATE A CLASS… OF A GRID (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15): its rows at its finest level made a class
// of the page's model -- the project in the model home (the page's own SDLC: Studio's), made with the page's first
// class -- with everything the class needs to run, its source's first rows its sample. The route writes the files
// (legend-lite's createClass, asked of the host's writer: the tab's module on every plane, Python's native library), and
// the generic library (model-changes/, docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4) saves them as one revision of
// DataCube's workspace; the page's model records which grid is which class. Loaded the first time it is asked for: a page
// that makes no class never fetches it -- and it imports nothing the page loads at startup (its queries are handed in),
// so the page's own modules stay in its bundle.

import type { SdlcClient } from '../../sdlc-client/src/client.ts';
import type { PlanAndRun } from './runner.ts';
import { createClass, projectState } from '../../model-changes/src/index.ts';
import { PageModel, type PageModelRef } from './page-model.ts';
import type { ClassAction, GridSource, ModelWriter } from './model-writer.ts';
import { suggestedClassName, suggestedPackage } from './model-names.ts';
import { classAction, classSamples } from './model-plans.ts';
import { classDialog, type ClassChoice, type ClassPreview, type CreatedClass } from './ui/class-dialog.ts';
import type { ModelAnswer } from './model-writer.ts';
import type { PageCommand } from './page-commands.ts';
import type { ClassBasis } from './query.ts';

export interface CreateClassOf {
  /** What the class is made over (query.ts classBasis): the grid's rows at their finest level, the Database and every
   *  table they read, each with its sample's query. */
  readonly basis: ClassBasis;
  /** The tables the grid's leaves are read as in this tab, in their order: what the page's model keeps for the grid
   *  (its project's store reads them by those names). */
  readonly kept: readonly string[];
  /** Where the grid's rows are planned and run, on whichever host: its sample is read there. */
  readonly rows: PlanAndRun;
  /** The grid's source: its planner's model and runtime, which the class is made over. */
  readonly source: GridSource;
  /** The model's writer: the host's (the tab's module on every plane, Python's native library). */
  readonly writer: ModelWriter;
  /** The name a class's model gives the host's database (its store, connection and runtime): `Browser`, `Python`. */
  readonly place: string;
  /** What the grid's header says it reads. */
  readonly sourceName: string;
  /** The grid's cube id on the page: its table's name is kept by it. */
  readonly cube: string | undefined;
  /** The page's name, as it would be saved. */
  readonly pageName: string;
  /** The page's model, when it has one. */
  readonly model: PageModelRef | undefined;
  /** The model home's SDLC. */
  readonly client: SdlcClient;
  /** Where the model home keeps the projects, as a person reads it (its `where`: this browser, this user's folder). */
  readonly where: string;
  /** Studio's editor on a workspace, where the host serves Studio. */
  readonly studio?: (project: string, workspace: string) => string;
  /** Ask the browser to keep this origin's storage: whether it promised. */
  readonly persist: () => Promise<boolean>;
}

/**
 * Ask for the class and write it. Resolves with the page commands its record of the page takes (page-model.ts
 * PageModel: made with its first class -- kept even when its first save failed -- pinned, its grid's tables and class
 * kept), in order; none when nothing of it changed.
 */
export async function createClassOf(o: CreateClassOf): Promise<readonly PageCommand[]> {
  const { basis } = o;
  const model = new PageModel(o.model);
  // each table's first rows, as the database has them: the class's samples (a Data element each), written by the one
  // writer (model-plans.ts classSamples: a NaN or an infinity refused, naming its column and row, never written as null)
  const samples = await classSamples(o.rows, basis);
  // the grid's rows as the class the person named, as Python's create_class makes it (model-plans.ts classAction)
  const action = (c: ClassChoice): ClassAction => classAction({ basis, source: o.source }, o.place, samples, c);
  const preview = (answer: ModelAnswer): ClassPreview => ({
    changes: answer.changes, properties: answer.classes[0]?.properties ?? [], notes: answer.notes,
  });
  await classDialog(document, {
    sourceName: o.sourceName,
    className: suggestedClassName(o.sourceName),
    package: model.ref?.package ?? suggestedPackage(o.pageName),
    project: { name: model.ref?.project ?? o.pageName, isNew: model.ref === undefined },
    preview: async (c) => preview(await o.writer.createClass(model.ref ? (await projectState(o.client, model.ref)).text : '',
      c.package, action(c))),
    create: async (c): Promise<CreatedClass> => {
      const done = await createClass({
        sdlc: o.client, writer: o.writer, ...model.into(c.projectName ?? o.pageName, c.package), class: action(c),
      });
      const made = done.answer;
      // the page pinned to what it saved: its class grids are read at it (§16.1)
      model.pin(done.revision);
      // the grid's tables keep their names from now: the project's store reads them by those names
      if (o.cube !== undefined) model.keep(o.cube, o.kept, made.classes[0]?.class ?? null);
      const after = model.ref ?? { project: done.project, workspace: done.workspace };
      const kept = await o.persist();
      return {
        className: made.classes[0]?.class ?? c.className,
        where: `the project ${after.project} (workspace ${after.workspace}), in ${o.where}`,
        ...(o.studio ? { studio: o.studio(after.project, after.workspace) } : {}),
        notes: [
          ...made.notes,
          ...(kept ? [] : ['This browser has not promised to keep its storage: it may clear the project.']),
        ],
      };
    },
  });
  // what the window changed of the page's model: made (whether or not its save then went through), pinned, kept
  return model.commands;
}
