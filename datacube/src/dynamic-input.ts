// DYNAMIC INPUT (docs/DATACUBE_FLOW_MODE_2026_10_24.md M8, Alteryx's Dynamic Input): a Union that keeps the rows of
// one of its inputs, picked as it runs by a String parameter of the page naming it -- join.ts appended: each input's
// rows filtered by `$name == '<its name>'`, the parameter's value bound where the source is resolved (live-inputs.ts
// resolvedSource), so the database keeps one input's rows. A value changed, the box is opened again with the other's.
//
// One rule for a flow's settings (page/page-app.ts pickInput), its palette's Dynamic Input (app/join-flow.ts) and
// Python's grid.pick (page-gestures.ts 'flow.pick'): the commands that set it -- its pick, and a new parameter of its
// inputs' names when it is asked for one. An input is picked by the name it is read under: a box no one has named is
// read under its source's, so two such boxes over one file are one name -- the screen names them as its canvas calls
// them (`called`: Filter, Input), in the same step, where they would be. PURE: no DOM, no planner.

import { lambda, variable } from '../../pure-protocol/src/index.ts';
import { familyOf } from '../../engine-client/src/types.ts';
import { isLive, joinInputs, type CubeInput } from './cube-document.ts';
import { pickNamesProblem, type JoinHow } from './join-how.ts';
import { gridInput } from './live-inputs.ts';
import type { PageCommand } from './page-commands.ts';
import type { PageDocument } from './page-document.ts';
import type { PageParameter } from './page-parameters.ts';
import { CubeRefusal } from './snapshot.ts';

/** What a Union keeps: the input a String parameter of the page names (`parameter`), the one a new parameter of its
 *  inputs' names names (`made`), or every input's rows (null: a Union again). */
export type InputPick = { readonly parameter: string } | { readonly made: true } | null;

/** What a new pick's parameter is called, unless the page has one so called: `source`, else `source2`, ... */
export const PICK_PARAMETER = 'source';

/** The names a Union's inputs are read under, in its order (left, right, any more): a live input's its grid's as an
 *  input (live-inputs.ts gridInput: its title, else its source's name), a frozen one's its own. `box`, the Union's (a
 *  new one, none). */
export function inputNames(page: PageDocument, box: string | undefined, inputs: readonly CubeInput[]): string[] {
  return inputs.map((i) => (isLive(i) ? gridInput(page, i.grid, box === undefined ? [] : [box], i.port).name : i.name));
}

/**
 * THE NAMES A PICK READS `inputs` BY, NAMED APART: as they are read now (inputNames) -- or, two read under one name,
 * each such box no one has named given its name on the canvas (`called`) as its title: the commands that name them
 * (renameTile, told before the pick's), and the page as they make it, the names read on it. Refused in words, by
 * newPick, when they are one name still.
 */
export function namedApart(page: PageDocument, box: string | undefined, inputs: readonly CubeInput[], called?: (grid: string) => string): {
  readonly page: PageDocument; readonly names: string[]; readonly titles: PageCommand[];
} {
  const names = inputNames(page, box, inputs);
  if (pickNamesProblem(names) === undefined || called === undefined) return { page, names, titles: [] };
  const clash = new Set(names.filter((n, i) => names.indexOf(n) !== i));
  const untitled = [...new Set(inputs.flatMap((i, k) => (isLive(i) && clash.has(names[k]!)
    && page.views.find((v) => v.id === i.grid)?.title === undefined ? [i.grid] : [])))];
  if (untitled.length === 0) return { page, names, titles: [] };
  const title = new Map(untitled.map((g) => [g, called(g)] as const));
  const titled: PageDocument = { ...page, views: page.views.map((v) => (title.has(v.id) ? { ...v, title: title.get(v.id)! } : v)) };
  return {
    page: titled, names: inputNames(titled, box, inputs),
    titles: untitled.map((tile): PageCommand => ({ kind: 'renameTile', tile, title: title.get(tile)! })),
  };
}

/** A new pick over inputs called `names`: a String parameter -- not one of `parameters`' names -- whose choices are
 *  the names, the first its value, and the append that keeps the rows of the one it names. Refused in words when two
 *  inputs are called the same: a pick names one by its name. */
export function newPick(parameters: readonly PageParameter[], names: readonly string[]): { readonly parameter: PageParameter; readonly how: JoinHow } {
  const twice = pickNamesProblem(names);
  if (twice !== undefined) throw new CubeRefusal(twice);
  const taken = new Set(parameters.map((p) => p.name));
  let name = PICK_PARAMETER;
  for (let n = 2; taken.has(name); n++) name = `${PICK_PARAMETER}${n}`;
  return {
    parameter: { name, type: 'String', value: names[0]!, choices: [...names] },
    how: { form: 'append', pick: lambda([], variable(name)) },
  };
}

/** THE COMMANDS THAT SET WHAT UNION `box` KEEPS (`pick`), over `page` as it is: its join set again (setJoin, its inputs
 *  as they are) -- a new parameter set first, when one is asked for, its inputs named apart first (namedApart, by
 *  `called`) -- or none, it keeps so already. Refused in words: a box that is no Union, inputs of one name, a parameter
 *  the page lacks or of another type. */
export function pickCommands(page: PageDocument, box: string, pick: InputPick, called?: (grid: string) => string): PageCommand[] {
  const view = page.views.find((v) => v.id === box);
  const source = view?.kind === 'grid' ? page.cubes.find((c) => c.id === view.cube)?.cube.source : undefined;
  if (source?._type !== 'join' || source.how.form !== 'append') {
    throw new CubeRefusal(`'${box}' is no Union: a Dynamic Input keeps the rows of one of a Union's inputs`);
  }
  const was = source.how.pick;
  const setJoin = (how: JoinHow): PageCommand => ({ kind: 'setJoin', grid: box, how });
  if (pick === null) return was === undefined ? [] : [setJoin({ form: 'append' })];
  if ('made' in pick) {
    const apart = namedApart(page, box, joinInputs(source), called);
    const made = newPick(page.parameters ?? [], apart.names);
    return [...apart.titles, { kind: 'setParameter', parameter: made.parameter }, setJoin(made.how)];
  }
  const p = (page.parameters ?? []).find((q) => q.name === pick.parameter);
  if (p === undefined) throw new CubeRefusal(`'${pick.parameter}' is no parameter of the page`);
  if (familyOf(p.type) !== 'text') throw new CubeRefusal(`${p.name} is no String, but ${p.type}: a Dynamic Input picks an input by its name`);
  if (was?.body[0]?._type === 'var' && was.body[0].name === p.name) return [];
  return [setJoin({ form: 'append', pick: lambda([], variable(p.name)) })];
}

/** The parameter a Union's pick reads, when it is one (`|$source`): what its settings show it picked by. */
export function pickedBy(how: JoinHow): string | undefined {
  if (how.form !== 'append' || how.pick === undefined) return undefined;
  const e = how.pick.body[0];
  return e?._type === 'var' ? e.name : undefined;
}
