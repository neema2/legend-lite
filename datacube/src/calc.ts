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

import { PIVOT_SEPARATOR } from './generated/lite-facts.ts';
import type { CubeSnapshot } from './snapshot.ts';

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
 * `example` is not documentation -- it is the PROOF. Every entry's
 * example is compiled through a real planner by
 * `demo/verify-calc-vocabulary.mjs`, so the catalogue cannot offer a
 * function that does not lower. A hand-written list nobody executes is
 * how you end up suggesting `median` to someone whose backend has no
 * spelling for it.
 */
export interface CalcFunction {
  readonly name: string;
  /** Signature, for the completion detail: `toUpper(String):String`. */
  readonly signature: string;
  readonly category: 'string' | 'number' | 'date' | 'logic';
  /**
   * A complete expression body using it, over the demo's own columns,
   * that the verifier compiles. `$x` is the row.
   */
  readonly example: string;
}

/**
 * The scalar vocabulary.
 *
 * Curated rather than generated, and that is a considered choice.
 * legend-lite's `Scalars` lowering table looked like the generatable
 * source, but its 3500 lines dispatch through a mix of `family(...)`
 * calls, `Map.entry` lists and case arms whose string literals are
 * often ARGUMENTS rather than function names (`case "MD5"` is a hash
 * kind, not a function). A regex over that would offer suggestions
 * that do not exist. A curated list whose every entry is compiled by a
 * test is the stronger guarantee -- the test is what makes it true,
 * not the authoring.
 */
export const CALC_FUNCTIONS: readonly CalcFunction[] = [
  // -- strings -------------------------------------------------------
  { name: 'toUpper', signature: 'toUpper(String):String',
    category: 'string', example: '$x.region->toOne()->toUpper()' },
  { name: 'toLower', signature: 'toLower(String):String',
    category: 'string', example: '$x.region->toOne()->toLower()' },
  { name: 'length', signature: 'length(String):Integer',
    category: 'string', example: '$x.region->toOne()->length()' },
  { name: 'trim', signature: 'trim(String):String',
    category: 'string', example: '$x.region->toOne()->trim()' },
  { name: 'startsWith', signature: 'startsWith(String, String):Boolean',
    category: 'string', example: "$x.region->toOne()->startsWith('E')" },
  { name: 'endsWith', signature: 'endsWith(String, String):Boolean',
    category: 'string', example: "$x.region->toOne()->endsWith('A')" },
  { name: 'contains', signature: 'contains(String, String):Boolean',
    category: 'string', example: "$x.region->toOne()->contains('ME')" },
  { name: 'substring', signature: 'substring(String, Integer, Integer):String',
    category: 'string', example: '$x.region->toOne()->substring(0, 2)' },
  { name: 'replace', signature: 'replace(String, String, String):String',
    category: 'string',
    example: "$x.region->toOne()->replace('E', 'e')" },

  // -- numbers -------------------------------------------------------
  { name: 'abs', signature: 'abs(Number):Number',
    category: 'number', example: '$x.pnl->toOne()->abs()' },
  { name: 'round', signature: 'round(Number):Integer',
    category: 'number', example: '$x.notional->toOne()->round()' },
  { name: 'floor', signature: 'floor(Number):Integer',
    category: 'number', example: '$x.notional->toOne()->floor()' },
  { name: 'ceiling', signature: 'ceiling(Number):Integer',
    category: 'number', example: '$x.notional->toOne()->ceiling()' },
  { name: 'sqrt', signature: 'sqrt(Number):Float',
    category: 'number', example: '$x.notional->toOne()->sqrt()' },
  { name: 'exp', signature: 'exp(Number):Float',
    category: 'number', example: '$x.qty->toOne()->exp()' },
  { name: 'log', signature: 'log(Number):Float',
    category: 'number', example: '$x.qty->toOne()->log()' },
  { name: 'mod', signature: 'mod(Integer, Integer):Integer',
    category: 'number', example: '$x.qty->toOne()->mod(2)' },

  // -- logic ---------------------------------------------------------
  { name: 'if', signature: 'if(Boolean, a, b):a',
    category: 'logic',
    example: "if($x.pnl->toOne() > 0, |'up', |'down')" },
  { name: 'isEmpty', signature: 'isEmpty(any):Boolean',
    category: 'logic', example: '$x.desk->isEmpty()' },
  { name: 'isNotEmpty', signature: 'isNotEmpty(any):Boolean',
    category: 'logic', example: '$x.desk->isNotEmpty()' },
  { name: 'toOne', signature: 'toOne(T[0..1]):T[1]',
    category: 'logic', example: '$x.notional->toOne()' },
];

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
 * identifier, so it needs the quoted form -- which is exactly the
 * spelling the serialiser uses for the same column.
 */
export function columnRef(name: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name)
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
  for (const m of snapshot.measures) {
    out.push({ insert: columnRef(m.name), label: m.name,
      detail: `measure, ${m.fn}(${m.column})`, kind: 'column' });
  }
  // The pivot's generated columns, from the CAST the snapshot asked
  // for rather than from a rendered grid: the snapshot is the source of
  // truth and is available before anything has been drawn.
  for (const p of snapshot.pivotCast ?? []) {
    out.push({
      insert: columnRef(p.name),
      label: p.name,
      detail: `pivot column of ${p.measure}`
        + ` (${p.name.split(PIVOT_SEPARATOR).slice(0, -1).join(' / ')})`,
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
): Completion[] {
  return [
    ...columnsInScope(snapshot, stage, before),
    ...CALC_FUNCTIONS.map((f) => ({
      insert: `->${f.name}(`,
      label: f.name,
      detail: `${f.signature} — ${f.category}`,
      kind: 'function' as const,
    })),
    ...CALC_OPERATORS,
  ];
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
): string | null {
  const trimmed = name.trim();
  if (trimmed.length === 0) return 'a calculated column needs a name';
  if (trimmed.includes(PIVOT_SEPARATOR)) {
    return `'${PIVOT_SEPARATOR}' is how a pivot spells its generated`
      + ' columns, so a name cannot contain it';
  }
  const taken = [
    ...snapshot.columns.map((c) => c.name),
    ...snapshot.derived.map((d) => d.name),
    ...snapshot.measures.map((m) => m.name),
    ...(snapshot.groupDerived ?? []).map((d) => d.name),
    ...(snapshot.pivotCast ?? []).map((p) => p.name),
  ].filter((n) => n !== replacing);
  if (taken.includes(trimmed)) {
    return `'${trimmed}' is already a column`;
  }
  void stage;
  return null;
}
