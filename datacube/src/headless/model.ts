// THE PAGE'S MODEL ACTIONS, HEADLESS (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md
// D4, Z4-Z7): what Python's `grid.create_class()`, `page.create_association()` and `page.create_data_space()` are --
// Create a Class…, Create an Association… and Create a Data Space… run by DataCube's own code and the generic library
// (model-changes/), so Python writes none of their rules again.

import {
  createAssociation, createClass, createDataSpace, projectState, serverWriter, type ModelWriter,
} from '../../../model-changes/src/index.ts';
import { fn } from '../../../pure-protocol/src/index.ts';
import { SdlcClient } from '../../../sdlc-client/src/client.ts';
import type { LitePlanner } from '../lite-planner.ts';
import { suggestedClassName, suggestedDataSpaceName, suggestedPackage } from '../model-names.ts';
import { classAction, keyUnique } from '../model-plans.ts';
import type { AssociationAction, AssociationEnd, ClassAction, ColumnChoice, ColumnRole } from '../model-writer.ts';
import { modelPageName, readPage, suggestedName } from '../page-document.ts';
import { PageModel } from '../page-model.ts';
import { duplicateLambda } from '../query.ts';
import type { PlanAndRun } from '../runner.ts';
import { pythonRunner } from '../services/python-host.ts';
import { gridClassed, type GridSpec } from './grid.ts';
import { hostCalls, hostEngine, hostFetch, hostWorker } from './host.ts';
import { writerClass } from '../class-source.ts';

// -- THE MODEL ACTIONS, HEADLESS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D4, Z4-Z7): Create a Class…, Create an
// Association… and Create a Data Space… run in the host's V8 by the windows' own functions (model-plans.ts) and the
// generic library (model-changes/), the SDLC and the model writer reached through the host's fetch (calls.ts `fetch`):
// its own -- its model home and its writer, by a path -- or a server's, by the address given. Each answers what it
// made and the page commands its record of the page takes (page-model.ts PageModel: made, pinned, kept), which the host
// applies as the tab does (no step of the page's history: D5) -- with them when it is refused after its project was
// made, so the project stays the page's. Refusals in words (`{problems, commands}`), never thrown.

/** Where a model action writes: the SDLC's API root and the writer's root -- the host's own, unless an address is given. */
interface Reach {
  readonly sdlc?: string | null;
  readonly writer?: string | null;
}

/** The SDLC and the writer a model action asks, through the host's fetch. */
function reached(at: Reach): { readonly client: SdlcClient; readonly writer: ModelWriter } {
  const fetcher = hostFetch(hostCalls());
  return { client: new SdlcClient(at.sdlc ?? '/sdlc/api', fetcher), writer: serverWriter(at.writer ?? '', fetcher) };
}

/** A model action refused before it is asked: the host's input that does not read. */
class ModelInputError extends Error {}

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** `o`'s fields, each one `allowed` names: another refused in words. */
function fields(o: Record<string, unknown>, allowed: readonly string[], what: string): void {
  const other = Object.keys(o).filter((k) => !allowed.includes(k));
  if (other.length > 0) throw new ModelInputError(`${what} has no ${other.map((k) => JSON.stringify(k)).join(', ')}: it takes ${allowed.join(', ')}`);
}

function textOr(v: unknown, what: string): string | undefined {
  if (v === undefined || v === null) return undefined;
  if (typeof v !== 'string') throw new ModelInputError(`${what} is text, not ${JSON.stringify(v)}`);
  return v;
}

/** A person's choices for a class's columns, by column: its role (`property`, `key`, `omit`: the writer's to judge, as
 *  CreateClass refuses another by name) or `{as, name, documentation}`, as the window gives them. */
function readColumns(given: unknown): Readonly<Record<string, ColumnChoice>> | undefined {
  if (given === undefined || given === null) return undefined;
  if (!isRecord(given)) throw new ModelInputError(`a class's columns are its choices by column, not ${JSON.stringify(given)}`);
  return Object.fromEntries(Object.entries(given).map(([column, choice]): [string, ColumnChoice] => {
    if (typeof choice === 'string') return [column, { as: choice as ColumnRole }];
    if (!isRecord(choice)) throw new ModelInputError(`${column}'s choice is its role or {as, name, documentation}, not ${JSON.stringify(choice)}`);
    fields(choice, ['as', 'name', 'documentation'], `${column}'s choice`);
    const as = textOr(choice['as'], `${column}'s role`);
    const name = textOr(choice['name'], `${column}'s name`);
    const documentation = textOr(choice['documentation'], `${column}'s documentation`);
    return [column, {
      ...(as !== undefined ? { as: as as ColumnRole } : {}), ...(name !== undefined ? { name } : {}),
      ...(documentation !== undefined ? { documentation } : {}),
    }];
  }));
}

/** An association's end as the writer takes it: `(class, on)`, or `{class, on, name, multiplicity, unique}`. */
function readEnd(given: unknown): AssociationEnd {
  if (Array.isArray(given) && given.length === 2 && typeof given[0] === 'string' && typeof given[1] === 'string') {
    return { class: given[0], on: given[1] };
  }
  if (!isRecord(given) || typeof given['class'] !== 'string' || typeof given['on'] !== 'string') {
    throw new ModelInputError(`an association's end names its class and what it is joined on: ${JSON.stringify(given)}`);
  }
  fields(given, ['class', 'on', 'name', 'multiplicity', 'unique'], "an association's end");
  const name = textOr(given['name'], "an end's name");
  const multiplicity = textOr(given['multiplicity'], "an end's multiplicity");
  const unique = given['unique'];
  if (unique !== undefined && typeof unique !== 'boolean') throw new ModelInputError(`whether an end's key is unique is true or false, not ${JSON.stringify(unique)}`);
  return {
    class: given['class'], on: given['on'], ...(name !== undefined ? { name } : {}),
    ...(multiplicity !== undefined ? { multiplicity } : {}), ...(unique !== undefined ? { unique } : {}),
  };
}

/** An association as the writer takes it: a pair of ends, or `{name, ends}`. */
function readAssociation(given: unknown): AssociationAction {
  if (Array.isArray(given)) {
    if (given.length !== 2) throw new ModelInputError(`an association is two ends, not ${JSON.stringify(given)}`);
    return { ends: [readEnd(given[0]), readEnd(given[1])] };
  }
  if (!isRecord(given) || !Array.isArray(given['ends']) || given['ends'].length !== 2) {
    throw new ModelInputError(`an association is two ends, or {name, ends}: ${JSON.stringify(given)}`);
  }
  fields(given, ['name', 'ends'], 'an association');
  const name = textOr(given['name'], "an association's name");
  return { ...(name !== undefined ? { name } : {}), ends: [readEnd(given['ends'][0]), readEnd(given['ends'][1])] };
}

/** What every model action is given: where it writes (Reach) and the place its classes read the host's database as. */
interface Given extends Reach {
  readonly place: string;
}

function givenOf(given: unknown, allowed: readonly string[], what: string): Record<string, unknown> & Given {
  if (!isRecord(given) || typeof given['place'] !== 'string') throw new ModelInputError(`${what} is told the place its classes read: ${JSON.stringify(given)}`);
  fields(given, ['place', 'sdlc', 'writer', ...allowed], what);
  textOr(given['sdlc'], "an SDLC's address");
  textOr(given['writer'], "a writer's address");
  return given as Record<string, unknown> & Given;
}

/** A model action's answer: what it made, its record of the page as commands; refused, in words, with the commands its
 *  record took before (its project made). */
function answered(model: PageModel, made: Record<string, unknown>): string {
  return JSON.stringify({ ...made, commands: model.commands });
}

function refused(model: PageModel | undefined, e: unknown): string {
  return JSON.stringify({ problems: [e instanceof Error ? e.message : String(e)], commands: model?.commands ?? [] });
}

/** A project's rows planned over its model, through the place's runtime, by the host's plan-and-run -- the one way every
 *  row is read on the host (D16): where a key's facts are read. Its planner let go after `use`. */
async function projectRows<T>(text: string, runtime: string, use: (rows: PlanAndRun) => Promise<T>): Promise<T> {
  const calls = hostCalls();
  const rows = pythonRunner(hostEngine(calls), text, runtime, {}, () => hostWorker(calls));
  try {
    return await use(rows);
  } finally {
    (rows.host.planner as LitePlanner).dispose();
  }
}

/**
 * CREATE A CLASS… OF A GRID, HEADLESS (Z4): the grid (`spec`, as the host reads it) made a class of the page's model, as
 * the window makes it (create-class.ts: model-plans.ts classAction over gridClassed), into the page's project -- made
 * with its first class (named `project`, else after the page). `given`: `{place, cube, source, name?, package?,
 * documentation?, columns?, project?, sdlc?, writer?}` -- the grid's cube and what its header says it reads. JSON:
 * `{class, notes, commands}` (`modelInto` when the project is made, `modelPin`, the grid's `modelKeep`), or `{problems,
 * commands}`.
 */
export async function modelClass(page: string, spec: GridSpec, given: unknown): Promise<string> {
  let model: PageModel | undefined;
  try {
    const g = givenOf(given, ['cube', 'source', 'name', 'package', 'documentation', 'columns', 'project'], 'a class');
    const cube = textOr(g['cube'], "the grid's cube");
    const source = textOr(g['source'], 'what the grid reads');
    if (cube === undefined || source === undefined) throw new ModelInputError("a class is of a grid: its cube and what it reads");
    const columns = readColumns(g['columns']);
    const documentation = textOr(g['documentation'], "a class's documentation");
    const doc = readPage(page, { empty: true });
    model = new PageModel(doc.model);
    const classed = await gridClassed(spec);
    if ('refused' in classed) throw new ModelInputError(`'${source}' cannot be a class: ${classed.refused}`);
    if ('problems' in classed) throw new ModelInputError(`'${source}' cannot be a class: ${classed.problems.join('; ')}`);
    const action = classAction(classed, g.place, classed.samples, {
      className: textOr(g['name'], "a class's name") ?? suggestedClassName(source),
      ...(documentation ? { documentation } : {}), ...(columns ? { columns } : {}),
    });
    const named = modelPageName(doc.name, suggestedName(doc));
    const into = model.into(textOr(g['project'], "a project's name") ?? named,
      textOr(g['package'], "a class's package") ?? suggestedPackage(named));
    const { client, writer } = reached(g);
    const done = await createClass({ sdlc: client, writer, ...into, class: action });
    model.pin(done.revision);
    const cls = done.answer.classes[0]?.class;
    // the grid's tables keep their names from now: the project's store reads them by those names
    model.keep(cube, classed.basis.tables.map((t) => t.table), cls ?? null);
    return answered(model, { class: cls ?? action.name, notes: done.answer.notes });
  } catch (e) {
    return refused(model, e);
  }
}

/**
 * CREATE AN ASSOCIATION…, HEADLESS (Z5): two of the page's classes related, each end preset as the window presets it
 * (model-plans.ts keyUnique: whether its key is unique over its class's rows, read through its relation function over
 * the project, planned through the place's runtime and run on the host's database) when it says neither how many nor
 * whether. `given`: `{place, ends: [end, end], name?, sdlc?, writer?}`, each end `(class, on)` or `{class, on, name,
 * multiplicity, unique}`. JSON: `{association, commands}`, or `{problems, commands}`.
 */
export async function modelAssociation(page: string, given: unknown): Promise<string> {
  let model: PageModel | undefined;
  try {
    const g = givenOf(given, ['ends', 'name'], 'an association');
    const name = textOr(g['name'], "an association's name");
    const association = readAssociation({ ...(name !== undefined ? { name } : {}), ends: g['ends'] });
    const doc = readPage(page, { empty: true });
    if (!doc.model) {
      throw new ModelInputError("an association relates two of the page's classes: make them first (Create ▸ Class…, or Create ▸ Data Space… for the whole page)");
    }
    model = new PageModel(doc.model);
    const pkg = doc.model.package;
    const { client, writer } = reached(g);
    const open = association.ends.some((e) => e.unique === undefined && e.multiplicity === undefined);
    const ends = !open ? association.ends : await (async () => {
      const state = await projectState(client, doc.model!);
      const joinable = (await writer.createAssociation(state.text, pkg, undefined)).joinable;
      return projectRows(state.text, `${pkg}::runtime::${g.place}`, async (facts) => Promise.all(association.ends.map(async (e) => {
        if (e.unique !== undefined || e.multiplicity !== undefined) return e;
        const unique = await keyUnique(facts, joinable, e.class, e.on, { duplicate: duplicateLambda, call: (f) => fn(f) });
        return unique === undefined ? e : { ...e, unique };
      })));
    })();
    const done = await createAssociation({
      sdlc: client, writer, ...model.into(modelPageName(doc.name, suggestedName(doc)), pkg),
      association: { ...association, ends: [ends[0]!, ends[1]!] },
    });
    model.pin(done.revision);
    return answered(model, { association: done.answer.associations[0]?.association ?? name ?? 'the association' });
  } catch (e) {
    return refused(model, e);
  }
}

/**
 * CREATE A DATA SPACE…, HEADLESS (Z6): the whole page in one go, as the window makes it (model-actions.ts
 * createDataSpaceOf) -- each grid not yet a class made one, named as the window suggests (a grid over no table of its
 * own left out; a sample a class cannot hold refusing the whole, before anything is written), the associations given
 * made, a data space over the page's classes, one revision; the project made with it when the page has none (named
 * `project`, else after the page). `grids`: each `{cube, source, spec}`, the page's grids as the host reads them.
 * `given`: `{place, name?, title?, description?, associations?, project?, sdlc?, writer?}`. JSON: `{dataSpace,
 * commands}` (`modelInto`, `modelPin`, each made class's `modelKeep`), or `{problems, commands}`.
 */
export async function modelDataSpace(page: string, grids: readonly { readonly cube: string; readonly source: string; readonly spec: GridSpec }[],
  given: unknown): Promise<string> {
  let model: PageModel | undefined;
  try {
    const g = givenOf(given, ['name', 'title', 'description', 'associations', 'project'], 'a data space');
    const listed = g['associations'] ?? [];
    if (!Array.isArray(listed)) throw new ModelInputError(`a data space's associations are a list, not ${JSON.stringify(listed)}`);
    const associations = listed.map(readAssociation);
    const doc = readPage(page, { empty: true });
    model = new PageModel(doc.model);
    const taken = doc.model?.classes ?? {};
    const named = modelPageName(doc.name, suggestedName(doc));
    const pkg = doc.model?.package ?? suggestedPackage(named);
    const into = model.into(textOr(g['project'], "a project's name") ?? named, pkg);
    const classes: ClassAction[] = [];
    const kept: { readonly cube: string; readonly tables: readonly string[]; readonly name: string }[] = [];
    for (const grid of grids) {
      if (taken[grid.cube] !== undefined) continue;
      const classed = await gridClassed(grid.spec);
      // rows read from no table of its own: left out, as the window leaves it
      if ('refused' in classed) continue;
      if ('problems' in classed) throw new ModelInputError(`'${grid.source}' cannot be a class: ${classed.problems.join('; ')}`);
      const name = suggestedClassName(grid.source);
      classes.push(classAction(classed, g.place, classed.samples, { className: name }));
      kept.push({ cube: grid.cube, tables: classed.basis.tables.map((t) => t.table), name });
    }
    const title = textOr(g['title'], "a data space's title");
    const description = textOr(g['description'], "a data space's description");
    const dataSpace = {
      name: textOr(g['name'], "a data space's name") ?? suggestedDataSpaceName(named),
      ...(title ? { title } : {}), ...(description ? { description } : {}),
    };
    const { client, writer } = reached(g);
    const done = await createDataSpace({ sdlc: client, writer, ...into, dataSpace, classes, associations });
    model.pin(done.revision);
    // each grid made a class keeps its tables and its class, as the window keeps them
    for (const k of kept) model.keep(k.cube, k.tables, writerClass(into.package, k.name));
    return answered(model, { dataSpace: done.answer.dataSpace ?? dataSpace.name });
  } catch (e) {
    return refused(model, e);
  }
}
