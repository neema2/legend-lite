// A FLOW'S PURE BOX (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL12): a box whose rows are a function of its input's, written
// in Pure -- `$input->filter(...)->extend(...)->groupBy(...)`, whatever the palette has no tool for -- kept by its source
// (cube-document.ts RowsSource.pure) and checked by the compiler as every query is: its input's relation read wherever
// the function reads its parameter, the whole typed as a relation when the box opens, a function that types as none
// refused in the compiler's words. The function is the document's; its text is the compiler's to print, never kept
// beside it. A grid like any other after it: a box after a Pure box reads its rows, and its own steps take its columns.
//
// One rule for the box's window, the hosts that open it (join-open.ts, the tab's and the headless host's chains) and
// Python's grid.pure (page-commands.ts setPure).
//
// PURE: no DOM, no planner.

import { children, colSpecs, fn, isFunction, rewrite, type Lambda, type ValueSpecification, type Variable } from '../../pure-protocol/src/index.ts';
import { CubeRefusal } from './snapshot.ts';

/** Its input's name where its text is written as an expression (`$input->filter(...)`). */
export const PURE_INPUT = 'input';

/** Its text as the compiler parses it: a lambda as written (`rows|$rows->...`), or an expression over `$input` -- a
 *  name and `||` its expression's `or`, never a lambda's head (`true || ...`). */
export function pureText(text: string): string {
  const t = text.trim();
  return /^[A-Za-z_]\w*\s*\|(?!\|)/.test(t) ? t : `${PURE_INPUT}|${t}`;
}

/** Its function as its window shows it, from the compiler's print of it (`printed`): the lambda's braces off, and its
 *  parameter too when it is `input` -- an expression over `$input`, as it was written (pureText's other way). */
export function pureShown(printed: string): string {
  const t = printed.trim();
  const inner = t.startsWith('{') && t.endsWith('}') ? t.slice(1, -1).trim() : t;
  const head = new RegExp(`^${PURE_INPUT}\\s*\\|\\s*`);
  return head.test(inner) ? inner.replace(head, '') : inner;
}

/** Whether `n` is a read of variable `name`. */
const isRead = (n: ValueSpecification, name: string): boolean => n._type === 'var' && (n as Variable).name === name;

/** Whether `n` is a lambda of its own that names a parameter `name`: under it, `$name` reads its own. */
const shadows = (n: ValueSpecification, name: string): boolean =>
  n._type === 'lambda' && (n as Lambda).parameters.some((p) => p.name === name);

/** Whether `n` reads variable `name` (a lambda of its own that names it reads its own). */
export function reads(n: ValueSpecification, name: string): boolean {
  if (isRead(n, name)) return true;
  if (shadows(n, name) || n._type === 'genericTypeInstance') return false;
  return children(n).some((c) => reads(c, name));
}

/**
 * What is wrong with `v` as a Pure box's function, in words -- undefined when nothing is: a lambda of one parameter, its
 * input's rows, its body one expression that reads it (one that does not is a box of rows of its own: a Text Input).
 * Its shape only: what it computes, and whether that is a relation, is the compiler's to judge.
 */
export function pureProblem(v: unknown): string | undefined {
  if (v === null || typeof v !== 'object' || (v as { _type?: unknown })._type !== 'lambda') {
    return `a Pure box's function is a lambda (${PURE_INPUT}|...), not ${JSON.stringify(v)}`;
  }
  const l = v as { parameters?: unknown; body?: unknown };
  if (!Array.isArray(l.parameters) || !Array.isArray(l.body)) return "a Pure box's function has its parameters and its body";
  const params = l.parameters as { _type?: unknown; name?: unknown }[];
  if (params.length !== 1) return `a Pure box's function takes one parameter, its input's rows: this one takes ${params.length}`;
  const p = params[0]!;
  if (p._type !== 'var' || typeof p.name !== 'string' || p.name === '') return "a Pure box's parameter is a variable with a name";
  const body = l.body as unknown[];
  if (body.length !== 1) return `a Pure box's function is one expression: this one is ${body.length}`;
  const e = body[0];
  if (e === null || typeof e !== 'object' || typeof (e as { _type?: unknown })._type !== 'string') {
    return "a Pure box's function is an expression";
  }
  if (!reads(e as ValueSpecification, p.name)) {
    return `its Pure never reads $${p.name}, its input's rows: a box of rows of its own is a Text Input`;
  }
  return undefined;
}

/** Its rows: `pure` applied to `input`, its input's relation -- read wherever its body reads its parameter, but under a
 *  lambda of its own that names a parameter the same (which reads that one). */
export function pureOver(pure: Lambda, input: ValueSpecification): ValueSpecification {
  const name = pure.parameters[0]!.name;
  return rewrite(pure.body[0]!, (n) => (isRead(n, name) ? input
    : shadows(n, name) ? n : undefined));
}

/** A box's rows over its input's (`rows`): a Pure box's, its function of them (pureOver); any other's, them as they are. */
export function boxRows(source: { readonly pure?: Lambda }, rows: ValueSpecification): ValueSpecification {
  return source.pure === undefined ? rows : pureOver(source.pure, rows);
}

/** Its input's relation read back off its rows (`over`, pureOver's of `pure`): what stands where its body first reads its
 *  parameter -- undefined when `over` is no rows of `pure`. What its window types a function set again over. */
export function pureInputOf(pure: Lambda, over: ValueSpecification): ValueSpecification | undefined {
  const name = pure.parameters[0]!.name;
  const find = (p: ValueSpecification, o: ValueSpecification): ValueSpecification | undefined => {
    if (isRead(p, name)) return o;
    if (p._type !== o._type || shadows(p, name) || p._type === 'genericTypeInstance') return undefined;
    const ps = children(p);
    const os = children(o);
    if (ps.length !== os.length) return undefined;
    for (let i = 0; i < ps.length; i++) {
      const got = find(ps[i]!, os[i]!);
      if (got !== undefined) return got;
    }
    return undefined;
  };
  return find(pure.body[0]!, over);
}

/** Its input's rows, for its function set again: `rows`, the box's own (its source's), as they are -- or, a Pure box
 *  already, what stands where its function reads its input (pureInputOf). */
export function pureInput(source: { readonly name: string; readonly pure?: Lambda }, rows: ValueSpecification): ValueSpecification {
  if (source.pure === undefined) return rows;
  const input = pureInputOf(source.pure, rows);
  if (input === undefined) throw new Error(`${source.name}'s rows are no rows of its Pure`);
  return input;
}

/** The compiler as a Pure box's function asks it: its parse of text, and its typing of a relation's columns -- each
 *  refusing in its words. */
export interface PureCompiler {
  parse(text: string): Promise<Lambda>;
  columns(relation: ValueSpecification): Promise<readonly { readonly name: string }[]>;
}

/** The column the compiler types a value as (a scalar root: legend-lite's UpstreamRelationType), one of its own. */
const VALUE_COLUMN = 'value';

/** A PURE BOX'S FUNCTION FROM ITS TEXT, the one rule its window (app.ts #openPure) and Python's grid.pure (headless
 *  gridPure) ask: parsed (pureText), its shape judged (pureProblem: a CubeRefusal), its rows over `input` typed as a
 *  relation -- what the page's setPure then sets. A value (a count, a sum) is typed as one column of its own, `value`:
 *  such rows are asked again with their columns selected, which a relation's are and a value's never, and a value is
 *  refused in words. */
export async function pureFrom(text: string, input: ValueSpecification, compiler: PureCompiler): Promise<Lambda> {
  return pureChecked(await compiler.parse(pureText(text)), input, compiler);
}

/** A Pure box's function as pureFrom judges it once parsed -- its shape, its rows over `input` a relation -- a macro's
 *  used too (macros.ts): itself, or refused in words. */
export async function pureChecked(pure: Lambda, input: ValueSpecification, compiler: Pick<PureCompiler, 'columns'>): Promise<Lambda> {
  const problem = pureProblem(pure);
  if (problem !== undefined) throw new CubeRefusal(problem);
  const rows = pureOver(pure, input);
  const columns = await compiler.columns(rows);
  // a relation's rows have columns: an expression of none is no rows a box can show
  if (columns.length === 0) throw new CubeRefusal('its Pure gives no rows with columns: a Pure box\'s rows are a relation');
  if (columns.length === 1 && columns[0]!.name === VALUE_COLUMN) {
    try {
      await compiler.columns(fn('select', rows, colSpecs([VALUE_COLUMN])));
    } catch (e) {
      throw new CubeRefusal(`its Pure gives a value, not rows: a Pure box's rows are a relation (asked for its column, the compiler said: ${e instanceof Error ? e.message : String(e)})`);
    }
  }
  return pure;
}

/** What a Pure box does, as its box says it: the functions its rows go through, in turn (`filter → extend`). */
export function pureWords(pure: Lambda): string {
  const chain: string[] = [];
  let at: ValueSpecification | undefined = pure.body[0];
  while (at !== undefined && isFunction(at)) {
    chain.unshift(at.function.slice(at.function.lastIndexOf(':') + 1));
    at = at.parameters[0];
  }
  if (chain.length === 0) return 'its input as it is';
  return chain.length <= 4 ? chain.join(' → ') : `${chain.slice(0, 3).join(' → ')} and ${chain.length - 3} more`;
}
