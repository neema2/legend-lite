// A FLOW'S STANDARD MACROS (docs/DATACUBE_FLOW_MODE_2026_10_24.md M8, Alteryx's standard macro; §17.6 item 8): a
// sub-flow saved as a Pure function of its input's rows -- each box of it, in order, its own steps over the rows before
// it (`boxFunction`: its rows as it hands them on, its input's rows read as `$input`), the steps composed (`macroOf`) --
// kept on the page by its name, and used as a box: a Pure box whose function it is (pure-box.ts), after any box. Its
// parameters are the page's it reads (page-parameters.ts: `$name`, bound where its box's queries are sent). Nothing is
// computed here: a function is rearranged from what its boxes' queries already say.
//
// One rule for the canvas's Save as Macro (page/cube-page.ts #saveMacro) and Python's page.macro (headless gridFunction,
// macroOf). PURE: no DOM, no planner.

import { rewrite, toJson, type Lambda, type ValueSpecification, lambda, variable } from '../../pure-protocol/src/index.ts';
import { boxRows, PURE_INPUT, pureOver, pureProblem, pureWords } from './pure-box.ts';
import { parametersRead, type PageParameter } from './page-parameters.ts';
import { CubeRefusal, sameValue } from './snapshot.ts';

/** One macro of the page: its name, and its function of its input's rows (one parameter, `$input`). */
export interface PageMacro {
  readonly name: string;
  readonly pure: Lambda;
}

/** The most boxes one macro is made of: a sub-flow, not a whole flow. */
export const MACRO_BOXES = 20;

/**
 * A BOX'S OWN STEPS AS A FUNCTION OF ITS INPUT'S ROWS: `rows` (its rows as it hands them on: flow-output.ts outputQuery's
 * query) with `base` -- its base, its input's rows or its Pure of them, where they stand in them -- read as its source's
 * own over `$input` (pure-box.ts boxRows: `source`, its document's, so a parameter its Pure reads stays `$name`). Refused
 * in words where its rows are not read from its base.
 */
export function boxFunction(rows: Lambda, base: ValueSpecification, source: { readonly name: string; readonly pure?: Lambda }): Lambda {
  const body = rows.body[0];
  if (rows.parameters.length > 0 || rows.body.length !== 1 || body === undefined) throw new Error("a box's rows are one query of no parameter");
  let found = false;
  const over = rewrite(body, (n) => {
    if (found || !sameValue(n, base)) return undefined;
    found = true;
    return boxRows(source, variable(PURE_INPUT));
  });
  if (!found) throw new CubeRefusal(`${source.name}'s rows are not read from its input's: a macro is made of boxes over one input each`);
  return lambda([PURE_INPUT], over);
}

/** A sub-flow's function: each step's (`steps`, a box's boxFunction, in the order its rows go through them) over the
 *  rows before it -- one function of the first one's input. */
export function macroOf(steps: readonly Lambda[]): Lambda {
  if (steps.length === 0) throw new CubeRefusal('a macro is made of one box at least');
  if (steps.length > MACRO_BOXES) throw new CubeRefusal(`a macro is made of ${MACRO_BOXES} boxes at most, not ${steps.length}`);
  let rows: ValueSpecification = variable(PURE_INPUT);
  for (const step of steps) rows = pureOver(step, rows);
  return lambda([PURE_INPUT], rows);
}

/** What is wrong with a macro among the page's others (`others`), in words, if anything. */
export function macroProblem(m: PageMacro, others: readonly PageMacro[]): string | undefined {
  if (m.name.trim() === '' || m.name !== m.name.trim()) return `a macro's name is words, not ${JSON.stringify(m.name)}`;
  if (others.some((o) => o.name === m.name)) return `the page has a macro '${m.name}' already`;
  const shape = pureProblem(m.pure);
  return shape === undefined ? undefined : `${m.name}: ${shape}`;
}

/** A macro in words, as the palette says it: its steps, and the page's parameters it reads. */
export function macroWords(m: PageMacro, parameters: readonly PageParameter[]): string {
  const read = parametersRead(m.pure, parameters);
  return `${pureWords(m.pure)}${read.length > 0 ? `; reads ${read.map((p) => `$${p}`).join(', ')}` : ''}`;
}

/** Two macros' functions the same: the same steps. */
export const sameMacro = (a: Lambda, b: Lambda): boolean => toJson(a) === toJson(b);

/** A page's macros read from JSON (its document's): each `{name, pure}`, judged (macroProblem), a name once. `read`, the
 *  protocol's reader of a lambda (its numbers exact). */
export function readMacros(raw: unknown, read: (v: unknown) => Lambda): PageMacro[] {
  if (raw === undefined) return [];
  if (!Array.isArray(raw)) throw new CubeRefusal(`a page's macros are a list, not ${JSON.stringify(raw)}`);
  const out: PageMacro[] = [];
  for (const r of raw) {
    const m = readMacro(r, read);
    const problem = macroProblem(m, out);
    if (problem !== undefined) throw new CubeRefusal(problem);
    out.push(m);
  }
  return out;
}

/** One macro read from JSON: `{name, pure}`. */
export function readMacro(raw: unknown, read: (v: unknown) => Lambda): PageMacro {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined) throw new CubeRefusal(`a macro is {name, pure}, not ${JSON.stringify(raw)}`);
  const odd = Object.keys(o).filter((k) => k !== 'name' && k !== 'pure');
  if (odd.length > 0) throw new CubeRefusal(`a macro takes no ${odd.join(', ')}`);
  if (typeof o['name'] !== 'string') throw new CubeRefusal(`a macro's name is words, not ${JSON.stringify(o['name'] ?? null)}`);
  return { name: o['name'], pure: read(o['pure']) };
}
