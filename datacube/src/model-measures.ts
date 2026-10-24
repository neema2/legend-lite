// MEASURES DECLARED IN THE MODEL (docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md §3.4, the "better DAX"): a Pure function of
// a class's instances, stereotyped `meta::legend::bi::BI.measure` --
//
//   function <<meta::legend::bi::BI.measure>> {meta::legend::bi::BI.format = '$#,##0'}
//   trading::grossNotional(t: trading::Trade[*]): Float[1] { $t.notional->sum() }
//
// -- offered on a grid over that class by its name, and added as the steps a person would make: each AGGREGATE OVER THE
// INSTANCES (`$t.<path>-><agg>()`, after `$t->filter(x|...)` or not) a row-level calculated column of its own with its
// Aggregation, and what remains -- arithmetic of those, another measure called (inlined) -- a group-level column over
// them. Typed in and out, composed by ordinary calls, its meaning the set it is given: a group's, a parent's, a whole
// page's. This file reads them, what the grid's menu offers; the steps that add one are model-measure-commands.ts's,
// fetched with the first one added. No DOM, no planner: the grid's menu asks it, and Python's grid.model_measure,
// headless.

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import { className } from './class-tree.ts';

/**
 * The profile a measure is stereotyped by: `meta::legend::bi::BI`, the platform's once core ships it (the parity doc's
 * C10) -- or, as a project cannot declare a `meta::` element (its store refuses the path, legend-sdlc's rule; found
 * 2026-10-24), a profile of its own named `BI`, wherever it puts it. Read by its name, BI: a stereotype is its
 * profile's and its value's names.
 */
export const MEASURE_PROFILE = 'meta::legend::bi::BI';
const isMeasureProfile = (path: string | undefined): boolean => path !== undefined && className(path) === className(MEASURE_PROFILE);

/** A measure as the model declares it. */
export interface ModelMeasure {
  /** Its path, how a body calls it: `trading::grossNotional`. */
  readonly path: string;
  /** What a person calls it, and the column it makes: `grossNotional`. */
  readonly name: string;
  /** The class whose instances it is of: its parameter's type. */
  readonly of: string;
  /** Its parameter's name: `t`. */
  readonly param: string;
  readonly body: ValueSpecification;
  /** Its format (`BI.format`, an Excel number format) and its words (`doc.doc`), as the model tags them. */
  readonly format?: string;
  readonly doc?: string;
}

interface FunctionElement {
  readonly _type: string;
  readonly package?: string;
  readonly name?: string;
  readonly parameters?: readonly { readonly name: string; readonly genericType?: { readonly rawType?: { readonly fullPath?: string } };
    readonly multiplicity?: { readonly lowerBound: number; readonly upperBound?: number } }[];
  readonly returnGenericType?: { readonly rawType?: { readonly fullPath?: string } };
  readonly returnMultiplicity?: { readonly lowerBound: number; readonly upperBound?: number };
  readonly body?: readonly ValueSpecification[];
  readonly stereotypes?: readonly { readonly profile?: string; readonly value?: string }[];
  readonly taggedValues?: readonly { readonly tag?: { readonly profile?: string; readonly value?: string }; readonly value?: string }[];
}

/** A multiplicity as a function's compiled name spells it: `[*]` MANY, `[1]` 1, `[0..1]` $0_1$. */
const spelled = (m: { readonly lowerBound: number; readonly upperBound?: number }): string =>
  (m.upperBound === undefined ? (m.lowerBound === 0 ? 'MANY' : `$${m.lowerBound}_MANY$`)
    : m.lowerBound === m.upperBound ? String(m.lowerBound) : `$${m.lowerBound}_${m.upperBound}$`);

/**
 * THE MODEL'S MEASURES, read from its elements as the compiler gives them: every function stereotyped `BI.measure` of one
 * parameter -- the instances, many of one class -- and one expression; its name as its body calls it (the element's
 * name less its compiled signature), its format and its words from its tags.
 */
export function modelMeasures(elements: readonly unknown[]): ModelMeasure[] {
  const out: ModelMeasure[] = [];
  for (const raw of elements) {
    const e = raw as FunctionElement;
    if (e._type !== 'function' || !e.stereotypes?.some((s) => isMeasureProfile(s.profile) && s.value === 'measure')) continue;
    const [p, ...more] = e.parameters ?? [];
    const of = p?.genericType?.rawType?.fullPath;
    const returns = e.returnGenericType?.rawType?.fullPath;
    if (p === undefined || more.length > 0 || of === undefined || p.multiplicity === undefined || p.multiplicity.upperBound !== undefined
      || returns === undefined
      || e.returnMultiplicity === undefined || e.body?.length !== 1 || e.name === undefined) continue;
    const signature = `_${className(of)}_${spelled(p.multiplicity)}__${className(returns)}_${spelled(e.returnMultiplicity)}_`;
    if (!e.name.endsWith(signature)) continue;
    const name = e.name.slice(0, -signature.length);
    const tag = (profile: (path: string | undefined) => boolean, value: string): string | undefined =>
      e.taggedValues?.find((t) => profile(t.tag?.profile) && t.tag?.value === value)?.value;
    const format = tag(isMeasureProfile, 'format');
    const doc = tag((path) => path === 'meta::pure::profiles::doc', 'doc');
    out.push({
      path: e.package ? `${e.package}::${name}` : name, name, of, param: p.name, body: e.body[0]!,
      ...(format !== undefined ? { format } : {}), ...(doc !== undefined ? { doc } : {}),
    });
  }
  return out;
}
