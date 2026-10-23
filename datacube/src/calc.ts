// What a calculated column can SAY, and what it can see when it says it.
//
// The expression itself is raw Pure, deliberately. Upstream DataCube
// does the same -- a Monaco editor over a Pure lambda -- and the reason
// is worth stating, because an Excel-like formula language is the
// obvious alternative: the PLANNER is then the validator, and the
// column's TYPE comes back from the compile rather than from a checker
// we wrote. That is the difference between a calculated column that is
// correctly a measure and one we guessed at.
//
// WHAT IT CAN SEE DEPENDS ON THE STAGE, and the two are not
// interchangeable (`derived` vs `groupDerived` on the snapshot):
//
//   derived       runs BEFORE grouping, over source rows. Sees source
//                 columns and the derived columns declared before it.
//                 `$x.notional * 1.05`
//
//   groupDerived  runs AFTER grouping and pivoting, over aggregates.
//                 Sees the row dimensions, the measure names, the
//                 pivot-generated columns, and earlier groupDerived
//                 columns -- but NOT source columns, which are gone.
//                 `$x.profit / $x.revenue`
//
// Offering a source column at the group stage, or a pivot column at
// the row stage, would suggest an expression that cannot compile. So
// the scope is computed per stage rather than from one flat list of
// "all columns" -- the completion has to be as honest as the query.

import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import { isIdentifier } from '../../pure-protocol/src/index.ts';
import { CALC_FACTS } from './generated/offer-facts.ts';
import { ROOT_COLUMN } from './grid/columns.ts';
import { groupedMeasures, pivotLabel, type PivotColumn } from './query.ts';
import {
  LEAF_COUNT_COLUMN, askedMeasures, excludedFromPivot, rowColumns, type CubeSnapshot, type QuerySnapshot,
} from './snapshot.ts';

/** Which extend stage an expression belongs to. */
export type CalcStage = 'row' | 'group';

/** One thing the editor can offer, with enough text to choose by. */
export interface Completion {
  /** What gets inserted. */
  readonly insert: string;
  /** What the list shows. */
  readonly label: string;
  /** Why you would pick it. */
  readonly detail: string;
  readonly kind: 'column' | 'function' | 'operator';
}

/**
 * A scalar function a calculated column may call.
 *
 * `example` is not documentation -- it is the PROOF. The build compiles every
 * entry's example as a calculated column (tools/offer-facts: an example the compiler
 * refuses fails generation), and reads the function's declarations from the compiler
 * (src/generated/offer-facts.ts `CALC_FACTS`): what the editor shows as its signature
 * is what legend-lite declares, never a copy. `demo/verify-calc-vocabulary.mjs` runs the
 * same examples through a live legend-engine too.
 */
export interface CalcFunction {
  readonly name: string;
  /**
   * A complete expression body using it, over the demo's own columns,
   * that the build compiles. `$x` is the row.
   */
  readonly example: string;
}

/**
 * The scalar vocabulary.
 *
 * Curated rather than generated, and that is a considered choice: WHICH functions
 * a person is offered first is an editorial decision, and each needs an example a
 * person can read. What a function accepts and returns is not: the compiler's
 * declarations, generated beside its offer facts.
 */
export const CALC_FUNCTIONS: readonly CalcFunction[] = [
  // -- strings -------------------------------------------------------
  { name: 'toUpper', example: '$x.region->toOne()->toUpper()' },
  { name: 'toLower', example: '$x.region->toOne()->toLower()' },
  { name: 'length', example: '$x.region->toOne()->length()' },
  { name: 'trim', example: '$x.region->toOne()->trim()' },
  { name: 'startsWith', example: "$x.region->toOne()->startsWith('E')" },
  { name: 'endsWith', example: "$x.region->toOne()->endsWith('A')" },
  { name: 'contains', example: "$x.region->toOne()->contains('ME')" },
  { name: 'substring', example: '$x.region->toOne()->substring(0, 2)' },
  { name: 'replace', example: "$x.region->toOne()->replace('E', 'e')" },

  // -- numbers -------------------------------------------------------
  { name: 'abs', example: '$x.pnl->toOne()->abs()' },
  { name: 'round', example: '$x.notional->toOne()->round()' },
  { name: 'floor', example: '$x.notional->toOne()->floor()' },
  { name: 'ceiling', example: '$x.notional->toOne()->ceiling()' },
  { name: 'sqrt', example: '$x.notional->toOne()->sqrt()' },
  { name: 'exp', example: '$x.qty->toOne()->exp()' },
  { name: 'log', example: '$x.qty->toOne()->log()' },
  { name: 'mod', example: '$x.qty->toOne()->mod(2)' },

  // -- dates: a period's first day, and a date's parts (over a day: the demo's TRADES has no date column)
  { name: 'firstDayOfWeek', example: '%2024-03-15->firstDayOfWeek()' },
  { name: 'firstDayOfMonth', example: '%2024-03-15->firstDayOfMonth()' },
  { name: 'firstDayOfQuarter', example: '%2024-03-15->firstDayOfQuarter()' },
  { name: 'firstDayOfYear', example: '%2024-03-15->firstDayOfYear()' },
  { name: 'datePart', example: '%2024-03-15->datePart()' },
  { name: 'year', example: '%2024-03-15->year()' },
  { name: 'monthNumber', example: '%2024-03-15->monthNumber()' },
  { name: 'quarterNumber', example: '%2024-03-15->quarterNumber()' },
  { name: 'dayOfWeekNumber', example: '%2024-03-15->dayOfWeekNumber()' },

  // -- logic ---------------------------------------------------------
  { name: 'if', example: "if($x.pnl->toOne() > 0, |'up', |'down')" },
  { name: 'isEmpty', example: '$x.desk->isEmpty()' },
  { name: 'isNotEmpty', example: '$x.desk->isNotEmpty()' },
  { name: 'toOne', example: '$x.notional->toOne()' },
];

/** A curated function's compiler facts; generation guarantees one per entry. */
function calcFact(name: string): { readonly path: string; readonly signatures: readonly string[] } {
  const fact = CALC_FACTS[name];
  if (fact === undefined) throw new Error(`no compiler facts for '${name}': bazel run //datacube:update_generated`);
  return fact;
}

/** The operators, which have no function spelling to complete. */
export const CALC_OPERATORS: readonly Completion[] = [
  { insert: ' + ', label: '+', detail: 'add', kind: 'operator' },
  { insert: ' - ', label: '-', detail: 'subtract', kind: 'operator' },
  { insert: ' * ', label: '*', detail: 'multiply', kind: 'operator' },
  { insert: ' / ', label: '/', detail: 'divide (always a Float)',
    kind: 'operator' },
  { insert: ' == ', label: '==', detail: 'equals', kind: 'operator' },
  { insert: ' != ', label: '!=', detail: 'not equals', kind: 'operator' },
  { insert: ' > ', label: '>', detail: 'greater than', kind: 'operator' },
  { insert: ' >= ', label: '>=', detail: 'greater or equal',
    kind: 'operator' },
  { insert: ' < ', label: '<', detail: 'less than', kind: 'operator' },
  { insert: ' <= ', label: '<=', detail: 'less or equal',
    kind: 'operator' },
  { insert: ' && ', label: '&&', detail: 'and', kind: 'operator' },
  { insert: ' || ', label: '||', detail: 'or', kind: 'operator' },
];

/**
 * A column reference, spelled for a Pure lambda.
 *
 * A generated pivot name carries the separator and is not a plain
 * identifier, so it needs the quoted form; so do `true` and `false`,
 * which the grammar reads as literals. (The compiler's printer still
 * writes those two bare -- a core-side gap noted in the roadmap run's
 * ledger, phase 8.)
 */
export function columnRef(name: string): string {
  // a name the grammar reads bare (pure-protocol's rule: `true` and `false` are literals, so quoted)
  return isIdentifier(name)
    ? `$x.${name}`
    : `$x.'${name.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
}

/**
 * Every column an expression at `stage` may refer to.
 *
 * `before` is the name of the calculated column being edited, if any:
 * a column cannot refer to itself, and it cannot refer to one declared
 * after it, because `extend` builds them in order.
 */
export function columnsInScope(
  snapshot: CubeSnapshot,
  stage: CalcStage,
  before?: string,
  /** The pivot's own columns, from the view's plan (cube.ts `pivot`). */
  pivot: readonly PivotColumn[] = [],
): Completion[] {
  const out: Completion[] = [];
  const upTo = (list: readonly { name: string }[]) => {
    const at = before === undefined ? -1 : list.findIndex(
      (d) => d.name === before);
    return at < 0 ? list : list.slice(0, at);
  };

  if (stage === 'row') {
    for (const c of snapshot.columns) {
      out.push({ insert: columnRef(c.name), label: c.name,
        detail: `source column, ${c.type}`, kind: 'column' });
    }
    for (const d of upTo(snapshot.derived)) {
      out.push({ insert: columnRef(d.name), label: d.name,
        detail: 'calculated column, this stage', kind: 'column' });
    }
    return out;
  }

  // The group stage. Source columns are GONE -- offering one would
  // suggest an expression the planner refuses.
  for (const r of snapshot.rows) {
    out.push({ insert: columnRef(r), label: r,
      detail: 'row dimension (a group key)', kind: 'column' });
  }
  // NO PIVOT: the groupBy's own aggregates, exactly as the level query writes them (query.ts groupedMeasures) --
  // every column on its Aggregation (upstream's _groupByAggCols). A PIVOT SPREADS ITS MEASURES into its generated
  // columns (below): under one, a measure's own name is no column here.
  if (snapshot.pivotOn.length === 0) {
    const typeOf = new Map(rowColumns(snapshot).map((c) => [c.name, c.type] as const));
    for (const m of groupedMeasures(snapshot, snapshot.rows)) {
      out.push({ insert: columnRef(m.name), label: m.name, detail: `aggregated column, ${typeOf.get(m.column)}`,
        kind: 'column' });
    }
  }
  // The pivot's own columns, from the PLAN that made them: what each
  // one is comes with it (its measure, its values), never read back out
  // of the name.
  for (const p of pivot) {
    out.push({
      insert: columnRef(p.name),
      label: p.name,
      detail: p.tuple === null
        ? `pivot total of ${p.measure.name}`
        : `pivot column of ${p.measure.name} (${p.tuple.map(pivotLabel).join(' / ')})`,
      kind: 'column',
    });
  }
  for (const d of upTo(snapshot.groupDerived ?? [])) {
    out.push({ insert: columnRef(d.name), label: d.name,
      detail: 'calculated column, this stage', kind: 'column' });
  }
  return out;
}

/** Everything offerable at `stage`: columns first, then the vocabulary. */
export function completionsFor(
  snapshot: CubeSnapshot,
  stage: CalcStage,
  before?: string,
  pivot: readonly PivotColumn[] = [],
): Completion[] {
  return [
    ...columnsInScope(snapshot, stage, before, pivot),
    ...CALC_FUNCTIONS.map((f) => ({
      insert: `->${f.name}(`,
      label: f.name,
      // the compiler's declarations of it (CALC_FACTS), every overload
      detail: calcFact(f.name).signatures.join('  |  '),
      kind: 'function' as const,
    })),
    ...CALC_OPERATORS,
  ];
}

/**
 * Every name the cube's columns have, and the two its queries reserve (the root group's, the leaf count's): what a
 * new name may not be. One list, so a calculated column (nameProblem) and a measure (measureNameProblem) are named by
 * one rule.
 */
export function takenNames(snapshot: QuerySnapshot, pivot: readonly PivotColumn[] = []): string[] {
  return [
    ...snapshot.columns.map((c) => c.name),
    ...snapshot.derived.map((d) => d.name),
    ...askedMeasures(snapshot).map((m) => m.name),
    ...(snapshot.groupDerived ?? []).map((d) => d.name),
    ...pivot.map((p) => p.name),
    ROOT_COLUMN,
    LEAF_COUNT_COLUMN,
  ];
}

const SEPARATOR_TAKEN = `'${PIVOT_SEPARATOR}' is how a pivot spells its generated columns, so a name cannot contain it`;

/**
 * Why the query's asked measure `at` (its place in `snapshot.measures`: a chart's value, never a grid's -- snapshot.ts
 * QuerySnapshot) has no name of its own, or null: its own column's,
 * when that column is no grouping key (the
 * column's aggregate, as the level query writes it), or a name nothing else of the cube has (takenNames: the measures
 * before it, so of two of one name the later is the one refused) -- a measure named after another column, or after a
 * key, would write a second column of that name. Pivoted, a measure spread under each pivot value writes no column of
 * its own name (query.ts pivotColumns: `<value>__|__<name>`), so it clashes with no column -- only with another
 * measure's name; one kept out of the pivot is written beside the keys, by the column rule (found by the round's
 * audit, 2026-10-16: the rule had refused a spread measure's name, which no column of the level writes). Judged by
 * place, never by the measure object: a cube holding one object twice is two measures of one name (the round's second
 * audit, 2026-10-16).
 */
export function measureNameProblem(snapshot: QuerySnapshot, at: number): string | null {
  const asked = askedMeasures(snapshot);
  const m = asked[at];
  if (m === undefined) throw new Error(`the query asks no measure ${at}: it asks ${asked.length}`);
  if (m.name.includes(PIVOT_SEPARATOR)) return SEPARATOR_TAKEN;
  const before = asked.slice(0, at);
  // pivoted, a measure whose column is not kept out of the pivot is spread under each pivot value (query.ts
  // spreadMeasures)
  if (snapshot.pivotOn.length > 0 && !excludedFromPivot(snapshot).has(m.column)) {
    return before.some((b) => b.name === m.name)
      ? `'${m.name}' is already a measure's name: the measure ${m.fn}(${m.column}) takes a name of its own`
      : null;
  }
  const taken = takenNames({ ...snapshot, measures: before });
  const own = m.name === m.column && !snapshot.rows.includes(m.column);
  // its own column's name is the column's aggregate: the column itself takes nothing from it
  const others = own ? taken.filter((n, i) => n !== m.column || taken.indexOf(n) !== i) : taken;
  return others.includes(m.name)
    ? `'${m.name}' is already a column: the measure ${m.fn}(${m.column}) takes a name of its own`
    : null;
}

/**
 * Why a name cannot be used for a calculated column.
 *
 * Checked before the planner sees it, because these produce confusing
 * refusals: a duplicate name makes every reference to it ambiguous,
 * and the planner's message names neither the file nor the column the
 * user just typed.
 */
export function nameProblem(
  snapshot: CubeSnapshot,
  stage: CalcStage,
  name: string,
  replacing?: string,
  pivot: readonly PivotColumn[] = [],
): string | null {
  const trimmed = name.trim();
  if (trimmed.length === 0) return 'a calculated column needs a name';
  if (trimmed.includes(PIVOT_SEPARATOR)) return SEPARATOR_TAKEN;
  const taken = takenNames(snapshot, pivot).filter((n) => n !== replacing);
  if (taken.includes(trimmed)) {
    return `'${trimmed}' is already a column`;
  }
  void stage;
  return null;
}
