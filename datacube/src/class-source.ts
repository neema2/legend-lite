// A CLASS AS A GRID'S SOURCE (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.1): a dashboard's tile reads a class of the
// page's project as its properties projected -- `Trade.all()->project(~[notional: x|$x.notional, ...])` -- each column
// a property under its own name, and the page's filters on the class put into it before its columns are taken (§16.2:
// `Trade.all()->filter(x|...)->project(...)`). Which properties it shows is read from the project's elements as the
// host's compiler gives them (`reading.elements`), never from a name's look. No DOM, no planner: tested alone.

import {
  derive, element, fn, isIdentifier, lambda, projectObjects, property, variable, type Lambda, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { projectText } from '../../sdlc-client/src/project-text.ts';
import type { PureFile } from '../../sdlc-client/src/wire.ts';
import { PAGE_WORKSPACE, type PageModelRef } from './page-model.ts';

/** Where a class is read (cube-document.ts ClassSource, without what it shows): its project at a revision, through its
 *  mapping and runtime. */
export interface ClassAt {
  readonly class: string;
  readonly project: string;
  readonly workspace: string;
  readonly revision: string;
  readonly mapping: string;
  readonly runtime: string;
}

/**
 * Where a class of the page's model is read (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.1): the page's project at its
 * pin, through what the writer makes beside every class -- the model's mapping, `<pkg>::mapping::Model`, and the place's
 * runtime, `<pkg>::runtime::<Place>` (core's CreateClass: the layout model-actions.ts reads a project's facts through).
 * Refused while the page's model has no saved revision.
 */
export function classAt(ref: PageModelRef, cls: string, place: string): ClassAt {
  if (ref.revision === undefined) {
    throw new Error(`the page's project ${ref.project} has no saved revision yet (its first save did not go through): `
      + 'Create ▸ Class… again pins the page to the revision it reads');
  }
  return {
    class: cls, project: ref.project, workspace: ref.workspace, revision: ref.revision,
    mapping: `${ref.package}::mapping::Model`, runtime: `${ref.package}::runtime::${place}`,
  };
}

/** A class a new grid is over, as a person names it: by its path or its name, of `project` (absent, the page's) at
 *  `revision` (absent, the page's pin). */
export interface ClassGiven {
  readonly class?: unknown;
  readonly project?: unknown;
  readonly revision?: unknown;
}

/**
 * WHERE A NEW GRID'S CLASS IS READ, by the one rule every editor names it by (Python's page.grid(cls=...), headless
 * givenSource; the tab's Classes and Add a Grid over a class: K16, K17): a class of the page's model by its name (made a
 * path as the writer makes one, writerClass) or its path, at the page's pin or at `revision`; a class of another project
 * by its path as DataCube's writer makes one (package::domain::Name), at a `revision` of it, read through what the
 * writer makes beside it (its package's `mapping::Model`, the `place`'s runtime) in DataCube's workspace. Or why not, in
 * words.
 */
export function classGiven(model: PageModelRef | null, given: ClassGiven, place: string): ClassAt | string {
  const { class: cls, project, revision } = given;
  if (typeof cls !== 'string' || cls === '') return `a class is named by its path or its name, not ${JSON.stringify(cls)}`;
  if (project !== undefined && (typeof project !== 'string' || project === '')) return `a project is named group:artifact, not ${JSON.stringify(project)}`;
  if (revision !== undefined && (typeof revision !== 'string' || revision === '')) return `a revision is named by its id, not ${JSON.stringify(revision)}`;
  let ref: PageModelRef;
  let path: string;
  if (project === undefined || project === model?.project) {
    if (model === null) return "the page has no model of its own: name the class's project (project=...) and its revision";
    path = cls.includes('::') ? cls : writerClass(model.package, cls);
    ref = revision === undefined ? model : { ...model, revision };
  } else {
    const pkg = writerPackageOf(cls);
    if (pkg === undefined) {
      return `a class of ${project} is named by its path as DataCube's writer makes one (package::domain::Name), not ${JSON.stringify(cls)}`;
    }
    if (revision === undefined) return `a class of ${project} is read at a revision of it: name it (revision=...)`;
    path = cls;
    ref = { project, workspace: PAGE_WORKSPACE, package: pkg, tables: {}, revision };
  }
  try {
    return classAt(ref, path, place);
  } catch (e) {
    // its one refusal, in its words: the page's project has no saved revision to read the class at
    if (ref.revision !== undefined || !(e instanceof Error)) throw e;
    return e.message;
  }
}

/** A class DataCube's writer makes, by its name: `<package>::domain::<Name>`, where Create a Class… makes every one. */
export function writerClass(pkg: string, name: string): string {
  return `${pkg}::domain::${name}`;
}

/** The package DataCube's writer made a class in, from its path (writerClass's layout) -- undefined for a path not of it. */
export function writerPackageOf(path: string): string | undefined {
  return /^(.+)::domain::[^:]+$/.exec(path)?.[1];
}

/** A model element as the compiler gives it (PureModelContextData's): what a class source reads of one. */
export interface ClassElement {
  readonly _type: string;
  readonly package?: string;
  readonly name?: string;
  readonly superTypes?: readonly (string | { readonly fullPath?: string; readonly path?: string })[];
  readonly properties?: readonly {
    readonly name: string;
    readonly genericType: { readonly rawType: { readonly _type: string; readonly fullPath?: string } };
    readonly multiplicity: { readonly lowerBound: number; readonly upperBound?: number };
  }[];
}

const pathOf = (e: ClassElement): string => (e.package ? `${e.package}::${e.name ?? ''}` : (e.name ?? ''));

/** The class's last name: what a person calls it. */
export function className(path: string): string {
  return path.slice(path.lastIndexOf(':') + 1);
}

/**
 * The properties a grid over `cls` shows, in the class's order (its own, then each supertype's): every one of a to-one
 * multiplicity whose type is no class or enumeration of the model. `left`: the others, each with why -- a to-many
 * property, a class-typed one (an association's end: the column chooser's tree, step 5), an enumeration's. A class the
 * elements do not hold is refused by name.
 */
export function classProperties(elements: readonly ClassElement[], cls: string): {
  readonly shown: readonly string[];
  readonly left: readonly string[];
} {
  const classes = new Map(elements.filter((e) => e._type === 'class').map((e) => [pathOf(e), e]));
  const enumerations = new Set(elements.filter((e) => e._type === 'Enumeration').map(pathOf));
  if (!classes.has(cls)) throw new Error(`the project has no class ${cls}`);
  const shown: string[] = [];
  const left: string[] = [];
  const seen = new Set<string>();
  const visit = (path: string, from: ReadonlySet<string>): void => {
    const c = classes.get(path);
    // a supertype outside the project (Any, a platform class) declares nothing a grid reads
    if (!c || from.has(path)) return;
    for (const p of c.properties ?? []) {
      if (seen.has(p.name)) continue;
      seen.add(p.name);
      const type = p.genericType.rawType._type === 'packageableType' ? p.genericType.rawType.fullPath ?? '' : '';
      const many = p.multiplicity.upperBound === undefined || p.multiplicity.upperBound > 1;
      const why = many ? 'it holds many values'
        : classes.has(type) ? `it is a ${className(type)}`
          : enumerations.has(type) ? `it is an enumeration (${className(type)})`
            : type === '' ? 'its type is no type a column has'
              : !isIdentifier(p.name) ? 'its name is no column\'s'
                : undefined;
      if (why === undefined) shown.push(p.name);
      else left.push(`${p.name} (${why})`);
    }
    const next = new Set([...from, path]);
    for (const s of c.superTypes ?? []) visit(typeof s === 'string' ? s : (s.fullPath ?? s.path ?? ''), next);
  };
  visit(cls, new Set());
  if (shown.length === 0) throw new Error(`${className(cls)} has no property a grid can show${left.length ? `: ${left.join(', ')}` : ''}`);
  return { shown, left };
}

/**
 * THE CLASS'S ROWS AS A GRID READS THEM: `<cls>.all()->filter(x|<where>)->project(~[p: x|$x.p, ...])` -- `where`, the
 * page's filters on the class as one condition over `$x` (page-filters.ts), put in before the columns are taken; none,
 * no filter.
 */
export function classRelation(cls: string, properties: readonly string[], where?: ValueSpecification): ValueSpecification {
  const all = fn('getAll', element(cls));
  const rows = where === undefined ? all : fn('filter', all, lambda(['x'], where));
  return projectObjects(rows, properties.map((p) => derive(p, lambda(['x'], property(variable('x'), p))))).node;
}

/**
 * A CLASS OPENED AS A GRID'S BASE, the one rule (the tab's sources.ts openedClass; headless host-tables.ts, a page on
 * the host: docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X1): its project's files at its revision as one model text, read by
 * the host's compiler (`elements`), the properties a grid shows and those it leaves out, each with why, and its rows as
 * a grid reads them (classRelation).
 */
export async function classOpened(at: ClassAt, files: readonly Pick<PureFile, 'pureCode'>[],
  elements: (model: string) => Promise<readonly ClassElement[]>): Promise<{
  readonly model: string; readonly shown: readonly string[]; readonly left: readonly string[]; readonly source: ValueSpecification;
}> {
  const model = projectText(files);
  const { shown, left } = classProperties(await elements(model), at.class);
  return { model, shown, left, source: classRelation(at.class, shown) };
}

/** The class source's query as a lambda of no parameters: what the planner types and plans. */
export function classLambda(cls: string, properties: readonly string[], where?: ValueSpecification): Lambda {
  return lambda([], classRelation(cls, properties, where));
}
