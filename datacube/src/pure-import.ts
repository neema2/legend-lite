// IMPORT PURE (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL12: "Import Pure turns a lambda into boxes; whatever does not become
// boxes becomes a Pure box"): a box's Pure -- `$input->filter(...)->extend(...)->groupBy(...)` -- taken apart into its
// steps, each a box of its own over the one before: each function its rows go through in turn, its first argument the
// rows before it, the step over `$input` alone (a Pure box's function, pure-box.ts). A step that reads its input
// elsewhere too (a join of the rows with themselves) is not taken apart: it and what it reads stay one box, the steps
// after it boxes of their own. What each step does is the compiler's to judge, as every Pure box's is.
//
// One rule for the Pure window's Import as Boxes (app.ts #openPure) and Python's flow.import_pure (headless pureSteps).
//
// PURE: no DOM, no planner.

import { isFunction, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { PURE_INPUT, pureProblem, reads } from './pure-box.ts';
import { CubeRefusal } from './snapshot.ts';

/**
 * A PURE BOX'S FUNCTION AS ITS STEPS, in order: each a lambda of `$input` -- the first over the box's own input, each
 * after it over the rows of the one before. One step alone when it has no more (a function that is one step). Refused in
 * words where it is no Pure box's function (pureProblem).
 */
export function pureSteps(pure: Lambda): Lambda[] {
  const problem = pureProblem(pure);
  if (problem !== undefined) throw new CubeRefusal(problem);
  const name = pure.parameters[0]!.name;
  const steps: Lambda[] = [];
  let at: ValueSpecification = pure.body[0]!;
  // from the last step back: each a function of the rows before it (its first argument), reading its input nowhere else
  while (isFunction(at) && at.parameters.length > 0) {
    const [rows, ...rest] = at.parameters;
    if (rows === undefined || rest.some((r) => reads(r, name)) || !reads(rows, name)) break;
    steps.unshift({ _type: 'lambda', parameters: [variable(PURE_INPUT)], body: [{ ...at, parameters: [variable(PURE_INPUT), ...rest] }] });
    at = rows;
  }
  // what is left before them: its input itself (every step taken apart), or a step that stays whole
  const left = at._type === 'var' && (at as { readonly name: string }).name === name;
  if (!left) steps.unshift({ _type: 'lambda', parameters: [variable(name)], body: [at] });
  return steps;
}
