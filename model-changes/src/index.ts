// INTO A PROJECT (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4): a class, an association or a data space made in any
// project, a generic Legend capability -- not DataCube's: DataCube's windows are one caller, Python's twin
// (python/legend_lite/model.py) another, a script or another app more. Each function makes the project and its workspace
// when they are not yet, reads the project's files as one model text, asks legend-lite's model writer for the files
// (the host's own, or a legend-lite server's), and saves them as one revision, made again once over a newer one. The SDLC
// is one with lite's text routes: the page's, the model home, sdlc-server (legend-sdlc is owed: project.ts).
// test/cases holds this and its Python twin alike.

import type { SdlcClient } from '../../sdlc-client/src/client.ts';
import { makeProject, saveFresh, type NewProject } from './project.ts';
import { writerBodies, type AssociationAction, type ClassAction, type DataSpaceAction, type ModelAnswer, type ModelWriter } from './wire.ts';

export * from './project.ts';
export * from './wire.ts';

/** What each function is given: the SDLC and the writer, where to write, and what. */
export interface Into {
  readonly sdlc: SdlcClient;
  readonly writer: ModelWriter;
  /** A project's id (`groupId:artifactId`), or one to make. */
  readonly project: string | NewProject;
  readonly workspace: string;
  /** The package what is made goes in. */
  readonly package: string;
  /** Told the project's id as soon as it is made, so a later step failing leaves it the caller's, never a project the
   *  next call makes again beside it. */
  readonly made?: (project: string) => void;
}

/** What was made: where, the revision it was saved as, and the writer's answer (the files, the classes, its notes). */
export interface Created {
  readonly project: string;
  readonly workspace: string;
  readonly revision: string;
  readonly answer: ModelAnswer;
}

async function into(o: Into, message: string, make: (model: string) => Promise<ModelAnswer>): Promise<Created> {
  let project: string;
  if (typeof o.project === 'string') {
    project = o.project;
  } else {
    project = await makeProject(o.sdlc, o.project);
    o.made?.(project);
  }
  const at = { project, workspace: o.workspace };
  const { made, revision } = await saveFresh(o.sdlc, at, message, make);
  return { ...at, revision, answer: made };
}

/** A class made from a relation's rows. */
export function createClass(o: Into & { readonly class: ClassAction }): Promise<Created> {
  return into(o, `Create a Class: ${o.class.name}`, (model) => o.writer.createClass(model, o.package, o.class));
}

/** An association between two classes of the project (or of `classes`, made with it). */
export function createAssociation(o: Into & { readonly association: AssociationAction; readonly classes?: readonly ClassAction[] }):
  Promise<Created> {
  return into(o, 'Create an Association',
    (model) => o.writer.createAssociation(model, o.package, o.association, o.classes));
}

/** A data space over the project's classes, the classes and associations given made with it. */
export function createDataSpace(o: Into & {
  readonly dataSpace: DataSpaceAction;
  readonly classes?: readonly ClassAction[];
  readonly associations?: readonly AssociationAction[];
}): Promise<Created> {
  return into(o, `Create a Data Space: ${o.dataSpace.name}`,
    (model) => o.writer.createDataSpace(model, o.package, o.dataSpace, o.classes, o.associations));
}

/** legend-lite's model writer on a server: its routes over HTTP, as the tab's module answers them. */
export function serverWriter(baseUrl: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)): ModelWriter {
  const root = baseUrl.replace(/\/+$/, '');
  const write = async (route: string, body: string): Promise<ModelAnswer> => {
    const answer = await fetcher(`${root}/api/lite/v1/model/${route}`,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    const text = await answer.text();
    if (!answer.ok) {
      let message = text;
      try {
        const said = JSON.parse(text) as { message?: unknown };
        if (typeof said.message === 'string') message = said.message;
      } catch {
        // not JSON: the text is the message
      }
      // the host's own writer (a root of '': a path of its own) said by its route alone
      throw new Error(`legend-lite's ${route}${root ? ` at ${root}` : ''}: ${message}`);
    }
    return JSON.parse(text) as ModelAnswer;
  };
  return {
    createClass: (model, pkg, cls) => write('createClass', writerBodies.createClass(model, pkg, cls)),
    createAssociation: (model, pkg, association, classes) =>
      write('createAssociation', writerBodies.createAssociation(model, pkg, association, classes)),
    createDataSpace: (model, pkg, dataSpace, classes, associations) =>
      write('createDataSpace', writerBodies.createDataSpace(model, pkg, dataSpace, classes, associations)),
  };
}
