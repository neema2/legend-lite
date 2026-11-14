// Snapshot -> the cube's queries as PROTOCOL JSON (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md,
// T4b): every query DataCube sends, built as a tree with pure-protocol's Relation API. Pure text
// exists only where a person reads or writes it, and the compiler owns both edges: it parses what
// was typed (the snapshot holds the result: its source and each calculated column's lambda) and
// prints a query for a person to read. A tree cannot misplace an operator the way text can (the
// grammar applies < <= > >= left to right with && and ||), and no literal is ever spelled here.
//
// Literals keep the types the queries' text gave them before T4b -- an integer an integer, a
// fraction a float, exact digits exact; typing a literal by its column (T4c) is a change of
// meaning, made and proven there.

import {
  agg, and, asc, collection, colSpecs, derive, desc, enumValue, flatten, fn, from, lambda, lit, not, or, projectObjects, property, variable,
  type AppliedFunction, type ColSpec, type Lambda, type Relation, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import {
  CubeRefusal,
  columnType,
  excludedFromPivot,
  LEAF_COUNT_COLUMN,
  pivotTotalColumn,
  isJsonValue,
  isRelativeDate,
  relativeDateWords,
  referencedColumns,
  rowColumns,
  SECOND_COLUMN,
  totalOrderSorts,
  WINDOW_FUNCTIONS,
  type AggregateFn,
  askedMeasures,
  type ColumnKind,
  type CubeSnapshot,
  type DerivedColumn,
  type FilterNode,
  type FilterValue,
  type GrainAt,
  type Measure,
  type QuerySnapshot,
  type RelatedPath,
  type RelativeDate,
  type RelativeStart,
  type SortSpec,
  type WindowSpec,
  type ValueGroups,
} from './snapshot.ts';
import { cellFilterValue, filterWithout, keptCondition, MAX_FILTER_VALUES, VALUE_COUNT_COLUMN } from './value-filter.ts';
import type { GroupKey, RowPath } from './tree.ts';
import { ROOT_COLUMN, WINDOW_ALL_PREFIX } from './grid/columns.ts';
// the product's own columns, as what reads a box's rows leaves them out (join-open.ts), through this module's edge
export { isMachinery } from './grid/columns.ts';
import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { isBoolean, isNumeric, isVariant, plainType } from '../../engine-client/src/types.ts';
import { columnRef, measureNameProblem } from './calc.ts';

// ---- literals: typed by the column's COMPILER type, never by the value ----

/**
 * A filter value as a literal node. `type` is its column's compiler type, and it alone decides
 * the literal (its plain primitive, from the generated type facts): a key or a filter value is
 * the cell's EXACT text (a day, a timestamp to the microsecond, a decimal's digits, an integer
 * past 2^53), written as the column's type writes it. A value whose column the compiler has not
 * typed is refused: the text's look (a `.`, a `-`) or JavaScript's type of it is a guess.
 */
export function literalNode(v: FilterValue, type?: string): ValueSpecification {
  if (isRelativeDate(v)) return relativeDateNode(v);
  if (isJsonValue(v)) return fn('fromJson', lit.string(v.json));
  if (type === undefined) {
    throw new CubeRefusal(`the compiler has not typed the column this value ${JSON.stringify(v)} is compared with`);
  }
  if (isBoolean(type)) return lit.boolean(v === true || v === 'true');
  return lit.of(plainType(type), typeof v === 'boolean' ? String(v) : v);
}

/** The compiler's function taking a date to its period's first day. */
const START_FUNCTION: Readonly<Record<RelativeStart, string>> = {
  week: 'firstDayOfWeek', month: 'firstDayOfMonth', quarter: 'firstDayOfQuarter', year: 'firstDayOfYear',
};

/** `today()`, taken to its period's start, then moved: `adjust(firstDayOfMonth(today()), -1, DurationUnit.MONTHS)`. */
function relativeDateNode(v: RelativeDate): ValueSpecification {
  let node: ValueSpecification = fn(v.relative === 'today' ? 'today' : 'now');
  if (v.startOf !== undefined) node = fn(START_FUNCTION[v.startOf], node);
  if (v.offset !== undefined && v.unit !== undefined && v.offset !== 0) {
    node = fn('adjust', node, lit.integer(v.offset), enumValue('meta::pure::functions::date::DurationUnit', v.unit));
  }
  return node;
}

// ---- filters ----

/** `$param.column`. */
function ref(param: string, column: string): ValueSpecification {
  return property(variable(param), column);
}

/** A nullable column as `toLower` takes it: `$x.c->toOne()->toLower()`. */
function lowered(r: ValueSpecification): AppliedFunction {
  return fn('toLower', fn('toOne', r));
}

/**
 * The "contains" filter means TEXT containment, so the query names that function by its
 * path. It is the one operator function whose name Pure also declares elsewhere:
 * `collection::contains(Any[*], Any[1])` takes a number too, so the short name on a number
 * column compiles -- as membership, not what the operator means. By path, the compiler
 * itself refuses it there, and "offer what compiles" holds (T5). Every other operator's
 * short name resolves to its one function or is refused (measured by tools/offer-facts).
 */
const TEXT_CONTAINS = 'meta::pure::functions::string::contains';

const COMPARISON: Partial<Record<string, string>> = {
  equal: 'equal', notEqual: 'equal', lessThan: 'lessThan', lessThanEqual: 'lessThanEqual',
  greaterThan: 'greaterThan', greaterThanEqual: 'greaterThanEqual',
};

const COLUMN_COMPARISON: Partial<Record<string, string>> = {
  equalColumn: 'equal', equalCaseInsensitiveColumn: 'equal', notEqualColumn: 'equal',
  notEqualCaseInsensitiveColumn: 'equal', lessThanColumn: 'lessThan', lessThanEqualColumn: 'lessThanEqual',
  greaterThanColumn: 'greaterThan', greaterThanEqualColumn: 'greaterThanEqual',
};

/** `a op b`, `!=` being `!(a == b)`. */
function compare(function_: string, negate: boolean, a: ValueSpecification, b: ValueSpecification): ValueSpecification {
  const c = fn(function_, a, b);
  return negate ? not(c) : c;
}

/** A filter condition tree as a boolean expression over `$param`. */
export function filterNode(node: FilterNode, param = 'x', typeOf: TypeOf = () => undefined): ValueSpecification {
  switch (node.kind) {
    case 'and':
    case 'or': {
      // an empty group is a no-op while the user is still building it
      if (node.children.length === 0) return lit.boolean(node.kind === 'and');
      const parts = node.children.map((c) => filterNode(c, param, typeOf));
      return node.kind === 'and' ? and(...parts) : or(...parts);
    }
    case 'not':
      return not(filterNode(node.child, param, typeOf));
    case 'condition': {
      const r = ref(param, node.column);
      const type = typeOf(node.column);
      const one = (): ValueSpecification => literalNode(node.value as FilterValue, type);
      const many = (): readonly FilterValue[] => (node.value as readonly FilterValue[]) ?? [];
      // a text operator's operand is text, whatever the column: lowered here for the case-insensitive ones
      const preLowered = (v: FilterValue): ValueSpecification => lit.string(String(v).toLowerCase());

      const cmp = COMPARISON[node.operator];
      if (cmp) return compare(cmp, node.operator === 'notEqual', r, one());

      const colCmp = COLUMN_COMPARISON[node.operator];
      if (colCmp) {
        if (!node.rightColumn) {
          throw new CubeRefusal(`operator '${node.operator}' on '${node.column}' needs a rightColumn`);
        }
        const right = ref(param, node.rightColumn);
        const insensitive = node.operator.includes('CaseInsensitive');
        const negate = node.operator.startsWith('notEqual');
        return insensitive
          ? compare(colCmp, negate, lowered(r), lowered(right))
          : compare(colCmp, negate, r, right);
      }

      switch (node.operator) {
        case 'isEmpty': return fn('isEmpty', r);
        case 'isNotEmpty': return fn('isNotEmpty', r);
        case 'contains': return fn(TEXT_CONTAINS, r, one());
        case 'notContains': return not(fn(TEXT_CONTAINS, r, one()));
        case 'startsWith': return fn('startsWith', r, one());
        case 'notStartsWith': return not(fn('startsWith', r, one()));
        case 'endsWith': return fn('endsWith', r, one());
        case 'notEndsWith': return not(fn('endsWith', r, one()));
        case 'in': return fn('in', r, collection(many().map((v) => literalNode(v, type))));
        case 'notIn': return not(fn('in', r, collection(many().map((v) => literalNode(v, type)))));
        case 'equalCaseInsensitive':
        case 'notEqualCaseInsensitive': {
          const value = fn('toLower', lit.string(String(node.value as FilterValue)));
          return compare('equal', node.operator === 'notEqualCaseInsensitive', lowered(r), value);
        }
        case 'containsCaseInsensitive': return fn(TEXT_CONTAINS, lowered(r), preLowered(node.value as FilterValue));
        case 'startsWithCaseInsensitive': return fn('startsWith', lowered(r), preLowered(node.value as FilterValue));
        case 'endsWithCaseInsensitive': return fn('endsWith', lowered(r), preLowered(node.value as FilterValue));
        case 'inCaseInsensitive': return fn('in', lowered(r), collection(many().map(preLowered)));
        case 'notInCaseInsensitive': return not(fn('in', lowered(r), collection(many().map(preLowered))));
        default: {
          const never: never = node.operator as never;
          throw new Error(`unhandled filter operator: ${String(never)}`);
        }
      }
    }
  }
}

// ---- aggregates ----

/** The map and reduce of a measure; `when`, a pivot cell's condition: its rows only. */
function aggregateSpec(m: Measure, when?: ValueSpecification): ColSpec {
  const only = (value: ValueSpecification): ValueSpecification =>
    when === undefined ? value : fn('if', when, lambda([], value), lambda([], collection([])));
  const y = variable('y');
  switch (m.fn) {
    case 'count':
      return agg(m.name, lambda(['x'], only(lit.integer(1))), lambda(['y'], fn('count', y)));
    // two columns, the second Measure.weight (SECOND_COLUMN), mapped as one row each: legend-lite's natives
    case 'wavg':
    case 'corr':
    case 'covarSample':
    case 'covarPopulation':
    case 'maxBy':
    case 'minBy': {
      if (!m.weight) throw new CubeRefusal(`measure '${m.name}' uses ${m.fn} but has no ${SECOND_COLUMN[m.fn]?.role} column`);
      return agg(m.name, lambda(['x'], fn(m.fn === 'wavg' ? 'wavgRowMapper' : 'rowMapper', only(ref('x', m.column)), only(ref('x', m.weight)))),
        lambda(['y'], fn(m.fn, y)));
    }
    // the different values of the column -- or of the second column it names -- each once: COUNT(DISTINCT ...)
    case 'distinctCount':
      return agg(m.name, lambda(['x'], only(ref('x', m.weight ?? m.column))), lambda(['y'], fn('count', fn('distinct', y))));
    case 'joinStrings':
      return agg(m.name, lambda(['x'], only(ref('x', m.column))), lambda(['y'], fn('joinStrings', y, lit.string(', '))));
    case 'unique':
      return agg(m.name, lambda(['x'], only(ref('x', m.column))), lambda(['y'], fn('uniqueValueOnly', y)));
    case 'percentile25':
    case 'percentile75':
      return agg(m.name, lambda(['x'], only(ref('x', m.column))),
        lambda(['y'], fn('percentile', y, lit.float(m.fn === 'percentile25' ? 0.25 : 0.75))));
    default:
      return agg(m.name, lambda(['x'], only(ref('x', m.column))), lambda(['y'], fn(m.fn, y)));
  }
}

/** "Show leaf count": the rows under each group. */
const LEAF_COUNT_SPEC = (): ColSpec =>
  agg(LEAF_COUNT_COLUMN, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y'))));

// ---- calculated columns and windows ----

/** A level's shape for a window: the rows grouped at this level, and those present. */
interface LevelWindow {
  readonly rows: readonly string[];
  readonly present: readonly string[];
  readonly order: readonly SortSpec[];
}

/** A calculated column appended to `rel`: an expression over the row, or a window. */
/**
 * A VALUE-GROUPS COLUMN'S EXPRESSION (snapshot.ts ValueGroups): each group's values tested as the Filter Values window
 * tests them (value-filter.ts keptCondition: the blanks by isEmpty), its name where they hold, in order; the rest
 * `other`, or the value itself. Each literal typed by its column (`typeOf`), as a filter's is.
 */
export function groupsLambda(g: ValueGroups, typeOf: TypeOf = () => undefined): Lambda {
  const otherwise: ValueSpecification = g.other !== undefined ? lit.string(g.other) : ref('x', g.of);
  return lambda(['x'], g.groups.reduceRight<ValueSpecification>((rest, group) =>
    fn('if', filterNode(keptCondition(g.of, group.values), 'x', typeOf), lambda([], lit.string(group.name)), lambda([], rest)), otherwise));
}

function extendDerived(rel: Relation, d: DerivedColumn, level?: LevelWindow, typeOf?: TypeOf): Relation {
  if (d.related) throw new Error(`'${d.name}' is projected with the class's own properties (relatedSource), never extended`);
  if (d.groups) {
    if (level) throw new CubeRefusal(`'${d.name}' groups a column's values: a row-level column, before the grouping`);
    return rel.extend([derive(d.name, groupsLambda(d.groups, typeOf))]);
  }
  if (d.grain) {
    throw new CubeRefusal(`'${d.name}' is a measure at its parent's or the grand total's grain: a group-level column, after the grouping`);
  }
  if (!d.window) {
    if (!d.lambda) throw new CubeRefusal(`the calculated column '${d.name}' has no expression`);
    if (d.unnest) {
      // each row once per element of the collection: `lateral(x|<collection>->flatten(~name))`
      const [body] = d.lambda.body;
      if (body === undefined || d.lambda.body.length !== 1) {
        throw new CubeRefusal(`the exploded column '${d.name}' needs one expression, the collection to explode`);
      }
      return rel.lateral(lambda(d.lambda.parameters, flatten(body, d.name)));
    }
    return rel.extend([derive(d.name, d.lambda)]);
  }
  let w: WindowSpec = d.window;
  if (level) {
    const here = new Set(level.present);
    const has = (c: string): boolean => here.has(c) || !level.rows.includes(c);
    const order = w.order.filter((o) => has(o.column));
    w = {
      ...w,
      partition: w.partition.filter(has),
      order: order.length > 0 ? order : level.order.filter((o) => has(o.column)),
    };
  }
  return extendWindow(rel, d.name, w);
}

const sortKey = (s: SortSpec): AppliedFunction => (s.direction === 'asc' ? asc(s.column) : desc(s.column));

/** `extend(over(...), ~[name: {p,w,r|...}])` in the forms Pure's `over` overloads take. */
export function extendWindow(rel: Relation, name: string, w: WindowSpec): Relation {
  const sorts = w.order.map(sortKey);
  const meta = WINDOW_META.get(w.fn);
  if (!meta) throw new CubeRefusal(`unknown window function '${String(w.fn)}'`);
  const unbounded = (): AppliedFunction => fn('unbounded');
  const frameOf = (): AppliedFunction | undefined => {
    if (!meta.framed) return undefined;
    const f = w.frame ?? (w.fn === 'last' ? 'partition' : undefined);
    if (f === undefined) return undefined;
    if (f === 'partition') return fn('rows', unbounded(), unbounded());
    if (sorts.length === 0) return undefined;
    if (f === 'running') return fn('rows', unbounded(), lit.integer(0));
    const n = Math.max(1, Math.floor(f.lastRows));
    return fn('rows', lit.integer(-(n - 1) || 0), lit.integer(0));
  };
  const frame = frameOf();
  const partitioned = (partition: readonly string[], sortKeys: readonly AppliedFunction[], f?: AppliedFunction): AppliedFunction => {
    const args: ValueSpecification[] = [{ _type: 'classInstance', type: 'colSpecArray', value: { colSpecs: partition.map((p) => ({ name: p })) } }];
    if (sortKeys.length > 0) args.push(collection(sortKeys));
    if (f) args.push(f);
    return fn('over', ...args);
  };
  // ENGINE GAP (docs/SEMANTICS_REGISTER.md S24): legend-engine registers no `over` that takes a
  // frame with no partition -- `over([], sort, rows(..))` is a function of its over.pure that its
  // compiler never registers (Handlers.java:1594-1613), so engine refuses it; legend-lite accepts
  // it. No partition IS one partition of every row: the window is partitioned by a constant
  // column added here (machinery: the level queries select it away, the grid never shows it,
  // grid/columns.ts `isMachinery`). Exact on every planner. Delete when engine registers the form.
  const everyRow = (): string => {
    const all = `${WINDOW_ALL_PREFIX}${name}`;
    rel = rel.extend([derive(all, lambda(['x'], lit.string('all')))]);
    return all;
  };
  let over: AppliedFunction;
  if (w.partition.length > 0) {
    over = partitioned(w.partition, sorts, frame);
  } else if (sorts.length > 0) {
    over = frame ? partitioned([everyRow()], sorts, frame) : fn('over', collection(sorts));
  } else {
    const key = w.column;
    if (!key) throw new CubeRefusal(`'${name}' needs an order or a partition`);
    over = partitioned([everyRow()], [asc(key)], fn('rows', unbounded(), unbounded()));
  }
  if (meta.column && !w.column) throw new CubeRefusal(`'${name}' needs a column to read`);
  if (meta.ordered && sorts.length === 0) throw new CubeRefusal(`'${name}' needs an order`);
  const p = variable('p');
  const wv = variable('w');
  const r = variable('r');
  const read = (v: ValueSpecification): ValueSpecification => (w.column !== undefined ? property(v, w.column) : v);
  const at = (n: number | undefined): ValueSpecification[] => (n !== undefined && n !== 1 ? [lit.integer(Math.floor(n))] : []);
  const pwr = (body: ValueSpecification): Lambda => lambda(['p', 'w', 'r'], body);
  const reduce = (f: string): Lambda => lambda(['y'], fn(f, variable('y')));
  let spec: ColSpec;
  switch (w.fn) {
    case 'sum': spec = agg(name, pwr(read(r)), reduce('plus')); break;
    case 'average': spec = agg(name, pwr(read(r)), reduce('average')); break;
    case 'median': spec = agg(name, pwr(read(r)), reduce('median')); break;
    case 'min': spec = agg(name, pwr(read(r)), reduce('min')); break;
    case 'max': spec = agg(name, pwr(read(r)), reduce('max')); break;
    case 'count': spec = agg(name, pwr(read(r)), reduce('count')); break;
    case 'rank': spec = derive(name, pwr(fn('rank', p, wv, r))); break;
    case 'denseRank': spec = derive(name, pwr(fn('denseRank', p, wv, r))); break;
    case 'rowNumber': spec = derive(name, pwr(fn('rowNumber', p, r))); break;
    case 'percentRank': spec = derive(name, pwr(fn('percentRank', p, wv, r))); break;
    case 'cumeDist': spec = derive(name, pwr(fn('cumulativeDistribution', p, wv, r))); break;
    case 'ntile': spec = derive(name, pwr(fn('ntile', p, r, lit.integer(Math.max(1, Math.floor(w.buckets ?? 4)))))); break;
    case 'lag': spec = derive(name, pwr(read(fn('lag', p, r, ...at(w.offset))))); break;
    case 'lead': spec = derive(name, pwr(read(fn('lead', p, r, ...at(w.offset))))); break;
    case 'first': spec = derive(name, pwr(read(fn('first', p, wv, r)))); break;
    case 'last': spec = derive(name, pwr(read(fn('last', p, wv, r)))); break;
  }
  return rel.extend([spec], over);
}

// ---- the cube's queries ----

/** The source with the cube's row-stage calculated columns: what step 0 types, and every query of the cube starts from. */
export function sourceWithDerived(s: QuerySnapshot): Relation {
  let rel = from(relatedSource(s));
  const typeOf: TypeOf = (c) => columnType(s, c);
  for (const d of s.derived) if (!d.related) rel = extendDerived(rel, d, undefined, typeOf);
  return rel;
}

/**
 * THE SOURCE WITH THE GRID'S RELATED COLUMNS (snapshot.ts RelatedPath) projected beside the class's own properties:
 * `<cls>.all()->project(~[..., customer_region: x|$x.customer.region, trades_notional_sum: x|$x.trades.notional->sum()])`
 * -- the class's relation rearranged, never computed; the joins and the per-instance aggregates the database's. A
 * related column on rows that are no class's projection (a file's, a snapped grid's copy) is refused in words.
 */
export function relatedSource(s: QuerySnapshot): ValueSpecification {
  const related = s.derived.filter((d) => d.related !== undefined);
  if (related.length === 0) return s.source.query;
  const q = s.source.query;
  const specs = q._type === 'func' && q.function === 'project' && q.parameters.length === 2 ? q.parameters[1] : undefined;
  const value = (specs as { readonly type?: unknown; readonly value?: { readonly colSpecs?: unknown } } | undefined);
  if (value?.type !== 'colSpecArray' || !Array.isArray(value.value?.colSpecs)) {
    throw new CubeRefusal(`'${related[0]!.name}' reads a class's relationships, and this grid's rows are no class's projection `
      + '(a snapped grid reads a copy of its rows, which has none)');
  }
  return projectObjects((q as AppliedFunction).parameters[0]!, [
    ...(value.value.colSpecs as ColSpec[]), ...related.map((d) => derive(d.name, lambda(['x'], relatedValue(d.related!)))),
  ]).node;
}

/** A related column's value over the class's instance `$x`: its path, its to-many end's values aggregated. */
export function relatedValue(r: RelatedPath): ValueSpecification {
  let at: ValueSpecification = variable('x');
  for (const p of r.path) at = property(at, p);
  return r.aggregate === undefined ? at : fn(r.aggregate, at);
}

/** The column `countLambda` answers in. */
export const TOTAL_ROWS_COLUMN = '__rows__';

/**
 * How many rows a FLAT cube has, its filter applied: asked only when the row cap cut it, so the
 * status can say "the first 1,000 of 48,213" rather than only that it was cut.
 */
export function countLambda(s: QuerySnapshot): Lambda {
  let rel = sourceWithDerived(s);
  if (s.filter) rel = rel.filter(lambda(['x'], filterNode(s.filter, 'x', (c) => columnType(s, c))));
  return rowCountLambda(rel);
}

/** How many rows a relation has, in one row's `TOTAL_ROWS_COLUMN`: all of them grouped under one key, counted. */
export function rowCountLambda(rel: Relation): Lambda {
  return rel.extend([derive(ROOT_COLUMN, lambda(['x'], lit.string(ROOT_VALUE)))])
    .groupBy([ROOT_COLUMN], [agg(TOTAL_ROWS_COLUMN, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y'))))])
    .lambda();
}

/**
 * A VALUE LIST's question (value-filter.ts, Excel's autofilter): `column`'s values -- each once, with how many rows have
 * it -- in order, one past the cap. Read under the grid's filter with the column's own conditions left off (`own`
 * false), so a value they filter out is offered to put back; with them (`own` true), the values the filter keeps now,
 * which the list shows ticked.
 */
export function columnValuesLambda(s: QuerySnapshot, column: string, own = false): Lambda {
  let rel = sourceWithDerived(s);
  const filter = own ? s.filter : filterWithout(s.filter, column);
  if (filter) rel = rel.filter(lambda(['x'], filterNode(filter, 'x', (c) => columnType(s, c))));
  return rel
    .select([column])
    .groupBy([column], [agg(VALUE_COUNT_COLUMN, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y'))))])
    .sort([asc(column)])
    .limit(MAX_FILTER_VALUES + 1)
    .lambda();
}

/** Step 1 of a pivot: its value combinations, in the pivot's order, one past the cap. */
export function pivotValuesLambda(s: QuerySnapshot): Lambda | null {
  const on = s.pivotOn;
  if (on.length === 0 || (s.pivotValues !== undefined && s.pivotValues.length > 0)) return null;
  refuseUnpivotable(s);
  let rel = sourceWithDerived(s);
  if (s.filter) rel = rel.filter(lambda(['x'], filterNode(s.filter, 'x', (c) => columnType(s, c))));
  return rel
    .select(on)
    .distinct()
    .sort(on.map((column) => sortKey({
      column, direction: s.pivotSort?.[column] === 'desc' ? 'desc' : 'asc',
    })))
    .limit(MAX_PIVOT_VALUES + 1)
    .lambda();
}

/**
 * A CROSS TAB'S VALUES ITS COLUMNS LACK (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL3): the values of its pivot's column its
 * rows hold that its pinned values do not list -- an empty one among them, as SQL's `not in` leaves it out -- in its
 * pivot's order, one past what a pivot shows, the database finding them. Null when nothing is pinned.
 */
export function unpinnedValuesLambda(s: QuerySnapshot): Lambda | null {
  const pinned = s.pivotValues ?? [];
  const on = s.pivotOn[0];
  if (pinned.length === 0 || on === undefined || s.pivotOn.length !== 1) return null;
  const lacking: FilterNode = { kind: 'or', children: [
    { kind: 'condition', column: on, operator: 'isEmpty' },
    { kind: 'condition', column: on, operator: 'notIn', value: pinned },
  ] };
  const { pivotValues: _pinned, ...rest } = s;
  return pivotValuesLambda({ ...rest, filter: s.filter === undefined ? lacking : { kind: 'and', children: [s.filter, lacking] } });
}

/**
 * A PIVOT'S VALUES AS ITS QUESTION ANSWERED THEM (`answered`: pivotValuesLambda's rows -- Load values -- or
 * unpinnedValuesLambda's, those its pin lacks), the one reading the screen's Cross Tab and Python's (headless) share
 * (the review of 2026-10-26): each a filter value of its column's type (value-filter.ts cellFilterValue), and whether
 * there were more than a pivot shows (its question asks one past MAX_PIVOT_VALUES). An answer without its column is
 * refused, never read as none.
 */
export function pivotValuesOf(s: Pick<CubeSnapshot, 'pivotOn' | 'columns' | 'derived'>, answered: ResultTable):
  { readonly values: readonly (FilterValue | null)[]; readonly more: boolean } {
  const on = s.pivotOn[0];
  const column = answered.columns.find((c) => c.name === on);
  if (on === undefined || column === undefined) throw new Error(`its values' answer has no column ${on ?? '(none pivoted)'}`);
  const type = rowColumns(s).find((c) => c.name === on)?.type;
  return { values: column.values.map((v) => cellFilterValue(v, type)), more: column.values.length > MAX_PIVOT_VALUES };
}

/** Load values refused, its rows holding more values than a pivot shows (pivotValuesOf's `more`), in words. */
export const tooManyValues = (on: string): string =>
  `its rows hold more than ${MAX_PIVOT_VALUES} values of ${on}, more columns than a pivot shows: filter its rows first`;

/** The condition a pivot cell's rows meet. */
function tupleCondition(s: QuerySnapshot, tuple: readonly GroupKey[]): ValueSpecification {
  const conditions = memberConditions(s, s.pivotOn, tuple);
  return filterNode(conditions.length === 1 ? conditions[0]! : { kind: 'and', children: conditions },
    'x', (c) => columnType(s, c));
}

/** One level's query: the source, calculated columns, filter, grouping, pivot, sorts and cap. */
export function levelLambda(snapshot: QuerySnapshot, scope?: LevelScope, pivot?: PivotFacts): Lambda {
  return levelRelation(snapshot, scope, pivot).lambda();
}

/**
 * A CLASS'S ROWS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1, Create a Class…): the cube's rows at its finest
 * level, as the grid shows them -- a flat cube's rows (its calculated columns, its filter, the columns it shows), a
 * grouped cube's groups (one row per group, its measures beside its keys) -- in no order and uncapped: a class's
 * instances have neither. A pivot whose values are pinned (a Cross Tab's, crossTabRows) has fixed columns, so its rows
 * are its groups' too; one whose values its rows decide each time is refused in words (its columns are its values, and
 * a class's properties, a box's columns, are fixed). A grid asks no aggregate of its own (snapshot.ts QuerySnapshot),
 * so one grouped by nothing is its rows.
 */
export function classLambda(snapshot: CubeSnapshot): Lambda {
  if (snapshot.pivotOn.length > 0) return crossTabRows(snapshot).lambda();
  return levelRelation(snapshot, undefined, undefined, 'none').lambda();
}

/** The question that types `rows`: none of them (`limit(0)`), its columns typed by the plan -- a box's Profile's and its
 *  Output's (profile.ts, flow-output.ts). */
export function columnsLambda(rows: ValueSpecification): Lambda {
  return from(rows).limit(0).lambda();
}

/**
 * A BOX'S ROWS AS IT HANDS THEM ON (docs/DATACUBE_FLOW_MODE_2026_10_24.md: a flow's box; the review of 2026-10-26's C1):
 * classLambda's, in the order its sorts give them -- a Sort box's order reaches the box after it and its Output, as
 * Alteryx's records keep their order -- uncapped. A fixed Cross Tab's rows (crossTabRows) are its groups', in no order:
 * a sort of one is its grid's.
 */
export function handedOnLambda(snapshot: CubeSnapshot): Lambda {
  if (snapshot.pivotOn.length > 0) return crossTabRows(snapshot).lambda();
  return levelRelation(snapshot, undefined, undefined, 'sorted').lambda();
}

/** The name of a pinned pivot's column as its rows hand it on (crossTabRows): its value alone when it spreads one
 *  measure, else its value and its measure; a total `Total`, or `Total_<measure>`. */
export function crossTabName(column: PivotColumn, spread: number): string {
  const label = column.tuple === null ? 'Total' : column.tuple.map(pivotLabel).join('_');
  return spread === 1 ? label : `${label}_${column.measure.name}`;
}

/**
 * A CROSS TAB'S ROWS (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL2, Alteryx's Cross Tab): a pivot whose values are pinned
 * has fixed columns -- one per value (and measure), its total's -- so its rows at their finest level are a relation the
 * box after it reads: a row per group, its keys beside its cells, each cell named by its value (crossTabName), the
 * measures it keeps out of the pivot beside them; no order, no cap. Refused in words: values its rows decide each time
 * (Load its values to fix them), a value naming a column it has already.
 */
function crossTabRows(snapshot: CubeSnapshot): Relation {
  const facts = pinnedPivotFacts(snapshot);
  if (facts === null) {
    throw new CubeRefusal('its columns are its values, which its rows decide each time it runs: Load its values to fix them, or take its pivot off');
  }
  let rel = levelRelation(snapshot, undefined, facts, 'none');
  const cells = pivotColumns(snapshot, facts);
  const spread = spreadMeasures(snapshot).length;
  const kept = [
    ...snapshot.rows,
    ...(snapshot.rows.length > 0 ? carriedMeasures(snapshot, snapshot.rows) : []).map((m) => m.name),
    ...(snapshot.groupDerived ?? []).filter((d) => !d.childAggregate).map((d) => d.name),
  ];
  const names = new Set(kept);
  const renamed: string[] = [];
  for (const c of cells) {
    const name = crossTabName(c, spread);
    if (name === '') throw new CubeRefusal('an empty text value names no column: fill or filter its empty values first');
    if (names.has(name)) throw new CubeRefusal(`its value '${name}' names a column it has already: rename that column first`);
    names.add(name);
    renamed.push(name);
    if (name !== c.name) rel = rel.rename(c.name, name);
  }
  // its keys, its cells, the rest: no machinery (the root a pivot of no groups is grouped under)
  return rel.select([...snapshot.rows, ...renamed, ...kept.slice(snapshot.rows.length)]);
}

/**
 * The table a source reads, when it reads one straight from its store (`#>{local::DB.trades}#`, a table model's
 * accessor): its Database's path, its schema (`default` when it names none) and its name. Undefined for any other
 * source (a query, a class, a join).
 */
export function storeTableOf(source: ValueSpecification): { readonly database: string; readonly schema: string; readonly table: string } | undefined {
  const node = source as { readonly _type?: string; readonly type?: string; readonly value?: { readonly path?: unknown } };
  if (node._type !== 'classInstance' || node.type !== '>') return undefined;
  const path = node.value?.path;
  if (!Array.isArray(path) || !path.every((p) => typeof p === 'string') || path.length < 2 || path.length > 3) return undefined;
  const [database, ...rest] = path as string[];
  return rest.length === 2
    ? { database: database!, schema: rest[0]!, table: rest[1]! }
    : { database: database!, schema: 'default', table: rest[0]! };
}

/** How many of a source's rows a class's sample holds (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.2: the first 100). */
export const SAMPLE_ROWS = 100;

/** A source's first `n` rows, as they are: a class's sample (its Data element). */
export function sampleLambda(source: ValueSpecification, n: number = SAMPLE_ROWS): Lambda {
  return from(source).limit(n).lambda();
}

/**
 * WHAT A SNAP COPIES (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3, §18.8): the grid's base query, its EVERY column --
 * not the ones its view shows, so a column shown after it is snapped answers from the copy too, and a fresh grid,
 * which shows none, is no `select(~[])` the planner refuses. The screen's Snap and Python's ask it alike. A grid with a
 * related column is refused: a copy of the class's rows has none of its relationships to read it through.
 */
export function snapPull(snapshot: Pick<CubeSnapshot, 'source' | 'columns' | 'derived'>): Lambda {
  const related = snapshot.derived.find((d) => d.related !== undefined);
  if (related !== undefined) {
    throw new CubeRefusal(`'${related.name}' reads the class's relationships, which a copy of its rows has not: take the `
      + 'related columns off to snap it');
  }
  return from(snapshot.source.query).select(snapshot.columns.map((c) => c.name)).lambda();
}

/** A table a query reads straight from its store (storeTableOf), and the accessor that reads it. */
export interface StoreTable {
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly relation: ValueSpecification;
}

/**
 * THE TABLES A QUERY READS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): every accessor of a store's table in it,
 * each table once, in the order the query names them -- a file's grid's one, a join's every leaf's. What a class of the
 * query is mapped over: its store declares them, and its samples are theirs. Structure only: the query is read, never
 * typed.
 */
export function tablesOf(query: unknown): StoreTable[] {
  const found = new Map<string, StoreTable>();
  const walk = (node: unknown): void => {
    if (Array.isArray(node)) {
      for (const n of node) walk(n);
      return;
    }
    if (node === null || typeof node !== 'object') return;
    const t = storeTableOf(node as ValueSpecification);
    if (t !== undefined) {
      const key = JSON.stringify([t.database, t.schema, t.table]);
      if (!found.has(key)) found.set(key, { ...t, relation: node as ValueSpecification });
      return;
    }
    for (const v of Object.values(node)) walk(v);
  };
  walk(query);
  return [...found.values()];
}

/** What a class of a grid is made over (classBasis). */
export interface ClassBasis {
  /** Its rows at their finest level (classLambda): the class's relation. */
  readonly query: Lambda;
  /** The store's Database the tables are declared in: one, the class's store. */
  readonly database: string;
  /** Each table the query reads (tablesOf), and the query of its sample: its first rows, as they are. */
  readonly tables: readonly { readonly table: string; readonly sample: Lambda }[];
}

/**
 * WHAT A CLASS OF A GRID IS MADE OVER (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): its rows at their finest level,
 * and every table they read -- a file's grid its one, a join its every leaf's -- each with its sample, all of one
 * Database. Refused in words: a pivot (classLambda), rows read from no table of a store (a class's grid: a class
 * already), tables of two Databases.
 */
export function classBasis(snapshot: CubeSnapshot): ClassBasis {
  const query = classLambda(snapshot);
  const tables = tablesOf(query);
  if (tables.length === 0) throw new CubeRefusal('its rows are read from no table of a store');
  const database = tables[0]!.database;
  const other = tables.find((t) => t.database !== database);
  if (other !== undefined) {
    throw new CubeRefusal(`its rows are read from two databases (${database}, ${other.database}): a class's store is one`);
  }
  // a class's samples are kept by table name (the writer's Data elements): two schemas' tables of one name would share one
  const named = tables.find((t, i) => tables.findIndex((u) => u.table === t.table) !== i);
  if (named !== undefined) {
    const schemas = tables.filter((t) => t.table === named.table).map((t) => t.schema).join(', ');
    throw new CubeRefusal(`its rows are read from two tables named ${named.table} (schemas ${schemas}): a class's samples are by table name`);
  }
  return { query, database, tables: tables.map((t) => ({ table: t.table, sample: sampleLambda(t.relation) })) };
}

/**
 * A FACT FOR AN ASSOCIATION (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a): the first value of `column` that
 * `relation` holds more than once -- no row, and the column is unique over the relation's rows (an association's end
 * on it is to one).
 */
export function duplicateLambda(relation: ValueSpecification, column: string): Lambda {
  return from(relation)
    .groupBy([column], [agg('rows', lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y'))))])
    .filter(lambda(['x'], fn('greaterThan', { _type: 'property', property: 'rows', parameters: [variable('x')] }, lit.integer(1))))
    .limit(1)
    .lambda();
}

/** The literals a column of one value is (`x|'all'`): its value the same for every row, whatever the row. */
const LITERALS = new Set(['string', 'integer', 'float', 'decimal', 'boolean', 'strictDate', 'dateTime']);

/** A calculated column's one value, when it is one (`x|'all'`: its body a literal, no window); else undefined. */
function constantOf(snapshot: QuerySnapshot, column: string): ValueSpecification | undefined {
  const d = snapshot.derived.find((x) => x.name === column);
  const body = d?.lambda?.body;
  return d !== undefined && d.window === undefined && body?.length === 1 && LITERALS.has(body[0]!._type) ? body[0] : undefined;
}

/** A level's rows' order: `grid`, sorted and capped as the grid reads them; `sorted`, sorted and uncapped, as a box hands
 *  them on (handedOnLambda); `none`, neither, a class's. */
type LevelOrder = 'grid' | 'sorted' | 'none';

function levelRelation(snapshot: QuerySnapshot, scope?: LevelScope, pivot?: PivotFacts, order: LevelOrder = 'grid'): Relation {
  // each measure named a column of its own, grouped or pivoted, or the level writes two columns of one name: a
  // grouped level its measures beside the keys, a pivot its kept-out measures beside them (calc.ts measureNameProblem)
  for (let at = 0; at < askedMeasures(snapshot).length; at++) {
    const problem = measureNameProblem(snapshot, at);
    if (problem !== null) throw new CubeRefusal(problem);
  }
  let rel = sourceWithDerived(snapshot);
  const groupCols = scope ? snapshot.rows.slice(0, Math.max(0, scope.level)) : snapshot.rows;
  const grandTotal = scope !== undefined && scope.level === 0 && snapshot.rows.length > 0;

  const conditions: FilterNode[] = [];
  if (snapshot.filter) conditions.push(snapshot.filter);
  if (scope) conditions.push(...parentConditions(snapshot, scope.parent));
  const typeOf: TypeOf = (c) => columnType(snapshot, c);
  if (conditions.length === 1) {
    rel = rel.filter(lambda(['x'], filterNode(conditions[0]!, 'x', typeOf)));
  } else if (conditions.length > 1) {
    rel = rel.filter(lambda(['x'], filterNode({ kind: 'and', children: conditions }, 'x', typeOf)));
  }

  const on = snapshot.pivotOn;
  const pivoting = on.length > 0;
  const grouping = !pivoting && (groupCols.length > 0 || askedMeasures(snapshot).length > 0 || grandTotal);

  const groupedAggs = (keys: readonly string[], projected: readonly string[]): ColSpec[] => {
    const specs: ColSpec[] = groupedMeasures(snapshot, keys, projected).map((m) => aggregateSpec(m));
    if (snapshot.leafCount === true && keys.length > 0) specs.push(LEAF_COUNT_SPEC());
    if (specs.length > 0) return specs;
    // upstream's filler: a groupBy has to aggregate something (serialize.ts writes `$x.''`
    // when there is no key at all; mirrored as the node that text parses to)
    const read: ValueSpecification = { _type: 'property', property: keys[0] ?? '', parameters: [variable('x')] };
    return [agg('count', lambda(['x'], read), lambda(['y'], fn('count', variable('y'))))];
  };

  const groupByLevel = (aggs: readonly ColSpec[]): void => {
    // grouped by columns of one value alone (`x|'all'`: Count Records', a Test's): every row aggregated, no key -- one
    // row even of no rows, where a grouping gives no group of none -- each key its one value beside (the review of
    // 2026-10-26's C4: Count Records of no rows gave no row)
    const constants = groupCols.map((c) => constantOf(snapshot, c));
    if (groupCols.length === 0) {
      rel = rel.extend([derive(ROOT_COLUMN, lambda(['x'], lit.string(ROOT_VALUE)))])
        .groupBy([ROOT_COLUMN], aggs);
    } else if (constants.every((v) => v !== undefined)) {
      rel = rel.apply('aggregate', colSpecs(aggs))
        .extend(groupCols.map((c, i) => derive(c, lambda(['x'], constants[i]!))))
        .select([...groupCols, ...aggs.map((a) => a.name)]);
    } else {
      rel = rel.groupBy(groupCols, aggs);
    }
  };

  let produced: Set<string> | null = null;

  if (pivoting) {
    refuseUnpivotable(snapshot);
    if (!pivot) throw new Error('a pivoted cube is written with its pivot values: run pivotValuesLambda first');
    const columns = pivotColumns(snapshot, pivot);
    const carried = snapshot.rows.length > 0 ? carriedMeasures(snapshot, groupCols) : [];
    const reads: string[] = [];
    const read = (name: string | undefined): void => {
      if (name !== undefined && name !== '' && !reads.includes(name)) reads.push(name);
    };
    groupCols.forEach(read);
    on.forEach(read);
    for (const m of [...columns.map((c) => c.measure), ...carried]) {
      if (m.fn !== 'count') read(m.column);
      read(m.weight);
    }
    rel = rel.select(reads);
    groupByLevel([
      ...columns.map((c) => aggregateSpec({ ...c.measure, name: c.name },
        c.tuple === null ? undefined : tupleCondition(snapshot, c.tuple))),
      ...carried.map((m) => aggregateSpec(m)),
      ...(snapshot.leafCount === true && groupCols.length > 0 ? [LEAF_COUNT_SPEC()] : []),
    ]);
    produced = new Set([
      ...groupCols,
      ...(groupCols.length === 0 ? [ROOT_COLUMN] : []),
      ...columns.map((c) => c.name),
      ...carried.map((m) => m.name),
      ...(snapshot.groupDerived ?? []).map((d) => d.name),
      LEAF_COUNT_COLUMN,
    ]);
  } else {
    const needed = grouping ? detailColumns(snapshot) : referencedColumns(snapshot, groupCols);
    if (needed.length > 0) {
      rel = rel.select(needed);
    } else if (isDetail(snapshot)) {
      const all = detailColumns(snapshot);
      if (all.length > 0) rel = rel.select(all);
    }
    if (askedMeasures(snapshot).length > 0 || groupCols.length > 0 || grandTotal) {
      groupByLevel(groupedAggs(groupCols, needed));
      // what the grouped level writes, as the pivot's branch says its own: a sort on any other column -- one a
      // measure renamed (`total` of `notional`) -- is the detail rows' (treeview.ts detailSnapshot), never this
      // level's. Named here, it asked the planner for a column the groupBy did not keep, and every grouped level of
      // the grid was refused (found 2026-10-16, PR neema2/legend-lite#47)
      produced = new Set([
        ...groupCols,
        ...(groupCols.length === 0 ? [ROOT_COLUMN] : []),
        ...groupedMeasures(snapshot, groupCols, needed).map((m) => m.name),
        ...(snapshot.groupDerived ?? []).map((d) => d.name),
        LEAF_COUNT_COLUMN,
      ]);
    }
  }

  const shown = totalOrderSorts(snapshot, groupCols).filter((x) => produced === null || produced.has(x.column));
  const rooted = (grouping || pivoting) && groupCols.length === 0;
  const keys: SortSpec[] = groupCols.length > 0
    ? groupCols.map((column) => ({ column, direction: 'asc' as const }))
    : rooted ? [{ column: ROOT_COLUMN, direction: 'asc' as const }] : [];
  const levelWindow: LevelWindow = {
    rows: snapshot.rows,
    present: rooted ? [ROOT_COLUMN] : groupCols,
    order: shown.length > 0 ? shown : keys,
  };
  // MEASURES AT A COARSER GRAIN (snapshot.ts Grain): the database's, beside each group row before the group-level
  // columns that read them
  const grains = (snapshot.groupDerived ?? []).filter((d) => d.grain !== undefined);
  if (grains.length > 0) {
    const named = grains[0]!.name;
    if (pivoting) throw new CubeRefusal(`'${named}' is a measure at its parent's or the grand total's grain: a grouped grid's, not a pivot's -- take the pivot off first`);
    for (const at of ['parent', 'all'] as const) {
      const wanted = grains.filter((d) => d.grain!.at === at);
      if (wanted.length > 0) rel = joinedGrain(rel, snapshot, wanted, at, scope);
    }
  }
  for (const d of snapshot.groupDerived ?? []) {
    if (!d.childAggregate && !d.grain) rel = extendDerived(rel, d, levelWindow);
  }
  // TOP OR BOTTOM N (snapshot.ts TopN): ranked by the database after the grouping and the group-level columns -- each
  // parent's groups, as a level's rows are its parent's; a flat grid's rows -- ties kept, the rest left out. Never the
  // grand total's one row, a pivot's, or the detail rows under the deepest groups.
  const top = snapshot.topN;
  const ranked = grouping ? groupCols.length > 0 : snapshot.rows.length === 0;
  if (top !== undefined && ranked && !grandTotal && !pivoting) {
    // machinery (grid/columns.ts isMachinery): never shown -- named here, as the module loads before columns.ts's prefix
    const TOP_N_RANK = `${WINDOW_ALL_PREFIX}topN`;
    // the largest first, its empties after every value (Pure's empty is the largest: first in a falling order), so led by
    // whether it is empty, a value first (the correctness audit's 3, 2026-10-26); the smallest first puts them last already
    let order: SortSpec[] = [{ column: top.by, direction: 'asc' }];
    if (top.bottom !== true) {
      const TOP_N_EMPTY = `${WINDOW_ALL_PREFIX}topN_empty`;
      rel = rel.extend([derive(TOP_N_EMPTY, lambda(['x'], fn('isEmpty', property(variable('x'), top.by))))]);
      order = [{ column: TOP_N_EMPTY, direction: 'asc' }, { column: top.by, direction: 'desc' }];
    }
    rel = extendWindow(rel, TOP_N_RANK, { fn: 'rank', partition: [], order });
    rel = rel.filter(lambda(['x'], fn('lessThanEqual', property(variable('x'), TOP_N_RANK), lit.integer(top.n))));
  }

  if (order !== 'none' && !isSingleRow(snapshot, groupCols, grandTotal)) {
    const sorts = totalOrderSorts(snapshot, groupCols).filter((x) => produced === null || produced.has(x.column));
    if (sorts.length > 0) rel = rel.sort(sorts.map(sortKey));
    // capped as the grid reads them; a box's handed on, every row
    if (order === 'grid' && scope?.limit !== undefined) {
      rel = rel.limit(scope.limit);
    } else if (order === 'grid' && snapshot.window) {
      const { offset, limit } = snapshot.window;
      rel = rel.slice(offset, offset + limit);
    }
  }
  return rel;
}

/**
 * A LEVEL'S ROWS WITH MEASURES AT A COARSER GRAIN (snapshot.ts Grain): each measure aggregated -- by its own aggregate,
 * the one its column groups by -- over the rows of the level's parent (the level's own rows: a level's query is its
 * parent's, query.ts LevelScope), or over every row the grid's filter keeps (`all`), one row joined to each of the
 * level's: a group row stays one row. A detail query has its group's keys folded into its filter (detailSnapshot), so
 * the grand total's filter comes with its scope (`grandFilter`).
 */
function joinedGrain(rel: Relation, snapshot: QuerySnapshot, wanted: readonly DerivedColumn[], at: GrainAt, scope?: LevelScope): Relation {
  const measures = groupedMeasures(snapshot, []);
  const aggs = wanted.map((d) => {
    const m = measures.find((x) => x.name === d.grain!.of);
    if (m === undefined) {
      throw new CubeRefusal(`'${d.name}' is '${d.grain!.of}' at a coarser grain, and the grid has no measure '${d.grain!.of}' `
        + `(${measures.map((x) => x.name).join(', ')})`);
    }
    return aggregateSpec({ ...m, name: d.name });
  });
  let whole = sourceWithDerived(snapshot);
  const own = at === 'all' && scope?.grandFilter !== undefined ? scope.grandFilter ?? undefined : snapshot.filter;
  const conditions: FilterNode[] = [
    ...(own ? [own] : []),
    ...(at === 'parent' && scope ? parentConditions(snapshot, scope.parent) : []),
  ];
  const typeOf: TypeOf = (c) => columnType(snapshot, c);
  if (conditions.length > 0) {
    whole = whole.filter(lambda(['x'], filterNode(conditions.length === 1 ? conditions[0]! : { kind: 'and', children: conditions }, 'x', typeOf)));
  }
  // one row -- grouped by a constant, then the measures alone -- beside every row of the level's: no column of the
  // join's own reaches the level's rows (a class of the grid's rows, Create a Class, takes each as a property)
  const key = `${WINDOW_ALL_PREFIX}grain_${at}`;
  whole = whole.extend([derive(key, lambda(['x'], lit.integer(1)))]).groupBy([key], aggs).select(wanted.map((d) => d.name));
  return rel.join(whole, 'INNER', lambda(['a', 'b'], lit.boolean(true)));
}

/** A level's child-group aggregates: each group row's aggregate of its child groups' figures. */
export function childAggregateLambda(
  snapshot: QuerySnapshot,
  scope: LevelScope,
): { readonly query: Lambda; readonly columns: readonly string[] } | null {
  const wanted = (snapshot.groupDerived ?? []).filter((d) => d.childAggregate);
  if (wanted.length === 0 || snapshot.rows.length === 0 || snapshot.pivotOn.length > 0) return null;
  const level = Math.max(0, scope.level);
  const depth = snapshot.rows.length;
  if (level > depth) return null;
  const specOf = columnSpecs(snapshot);
  const measureOf = (of: string): Measure => {
    const configured = askedMeasures(snapshot).find((m) => m.name === of);
    return configured ?? defaultMeasure(of, specOf.get(of));
  };
  const { pivotValues: _v, pivotTotal: _t, window: _w, ...rest } = snapshot;
  void _v; void _t; void _w;
  const reads = new Set([...snapshot.rows.slice(0, level + 1),
    ...wanted.map((d) => measureOf(d.childAggregate!.of).column)]);
  const derivedNeeded = snapshot.derived.some((d) => reads.has(d.name));
  const base: QuerySnapshot = {
    ...rest,
    columns: derivedNeeded ? snapshot.columns : snapshot.columns.filter((c) => reads.has(c.name)),
    derived: derivedNeeded ? snapshot.derived : [],
    pivotOn: [],
    sorts: [],
    groupDerived: [],
    leafCount: false,
    childCount: false,
  };
  const keys = snapshot.rows.slice(0, level);
  if (level === depth) {
    // each child aggregate reads its own copy of the column, `$x.<column>`
    const copies = wanted.map((d) => ({
      name: `__child_${d.name}`,
      lambda: lambda(['x'], ref('x', measureOf(d.childAggregate!.of).column)),
      kind: 'measure' as const,
    }));
    const measures = wanted.map((d, i) => ({ name: d.name, column: copies[i]!.name, fn: d.childAggregate!.fn }));
    return {
      query: levelLambda({ ...base, derived: [...base.derived, ...copies], measures },
        { level, parent: scope.parent }),
      columns: wanted.map((d) => d.name),
    };
  }
  const inner = [...new Map(wanted.map((d) => {
    const m = measureOf(d.childAggregate!.of);
    return [m.name, m] as const;
  })).values()];
  let rel = levelRelation({ ...base, measures: inner }, { level: level + 1, parent: scope.parent });
  const aggs = wanted.map((d) => aggregateSpec({
    name: d.name,
    column: measureOf(d.childAggregate!.of).name,
    fn: d.childAggregate!.fn,
  }));
  rel = keys.length === 0
    ? rel.extend([derive(ROOT_COLUMN, lambda(['x'], lit.string(ROOT_VALUE)))]).groupBy([ROOT_COLUMN], aggs)
    : rel.groupBy(keys, aggs);
  return { query: rel.lambda(), columns: wanted.map((d) => d.name) };
}

/** The rows behind a cell: the population, not its aggregate, capped. */
export function drillLambda(snapshot: QuerySnapshot, request: DrillRequest): Lambda {
  let rel = sourceWithDerived(snapshot);
  const conditions = drillConditions(snapshot, request);
  const typeOf: TypeOf = (c) => columnType(snapshot, c);
  if (conditions.length === 1) {
    rel = rel.filter(lambda(['x'], filterNode(conditions[0]!, 'x', typeOf)));
  } else if (conditions.length > 1) {
    rel = rel.filter(lambda(['x'], filterNode({ kind: 'and', children: conditions }, 'x', typeOf)));
  }
  return rel.limit(request.limit ?? DEFAULT_DRILL_LIMIT).lambda();
}

// ---- the cube's structure: what each query is built from ----

/** What the grand total's synthetic key holds. Upstream's value. */
export const ROOT_VALUE = '[ROOT]';

/** A column's compiler type, by name: how a value's literal is spelled (T3). */
export type TypeOf = (column: string) => string | undefined;

/** Each window function's facts, by name. */
export const WINDOW_META = new Map(WINDOW_FUNCTIONS.map((f) => [f.fn, f]));

/**
 * One level of the row-group tree.
 *
 * `level` is how many row dimensions to group by: 0 is the grand
 * total, 1 the top level, and so on. `parent` pins the ancestors, so
 * expanding EMEA fetches only EMEA's children.
 *
 * A subtotal is therefore literally the same measure expression with
 * grouping columns dropped -- not a second aggregation pass that could
 * disagree with the detail underneath it.
 */
export interface LevelScope {
  readonly level: number;
  readonly parent: RowPath;
  /**
   * Cap on rows for this level. Callers pass maxRows + 1 so that the
   * presence of the extra row reports "there is more" without a
   * second counting query.
   */
  readonly limit?: number;
  /**
   * A DETAIL query's (detailSnapshot's, its group's keys folded into its filter): the grid's own filter, null for none --
   * what a measure at the grand total's grain is computed under (snapshot.ts Grain). Absent on a level's query, whose
   * filter is the grid's.
   */
  readonly grandFilter?: FilterNode | null;
}

/**
 * Conditions pinning a branch: region == 'EMEA', and so on -- each key as the value it is, its
 * literal written by the column's compiler type (`literalNode`). A NULL key cannot be matched
 * with `==`, so it becomes an isEmpty test; without that, expanding a group whose key is null
 * silently returns no children. A Variant key is its document's JSON text, as the database
 * printed it, so it matches as a document.
 */
/**
 * The conditions pinning a member of a hierarchy -- `columns[i] ==
 * path[i]` down its path, typed as the tree's keys are. Ad Hoc Analysis mode's
 * members use the same rule the tree's branches do.
 */
export function memberConditions(
  snapshot: QuerySnapshot,
  columns: readonly string[],
  path: RowPath,
): FilterNode[] {
  return parentConditions({ ...snapshot, rows: columns }, path);
}

export function parentConditions(
  snapshot: QuerySnapshot,
  parent: RowPath,
): FilterNode[] {
  const out: FilterNode[] = [];
  // Calculated columns too: a group on one has keys of its own type.
  const typeOf = new Map(rowColumns(snapshot).map((c) => [c.name, c.type]));
  parent.forEach((value, i) => {
    const column = snapshot.rows[i];
    if (column === undefined) return;
    out.push(
      value === null
        ? { kind: 'condition', column, operator: 'isEmpty' }
        : {
            kind: 'condition',
            column,
            operator: 'equal',
            value: isVariant(typeOf.get(column)) ? { json: value } : value,
          },
    );
  });
  return out;
}

/**
 * The cube for ONE group's detail rows: upstream's last drilldown level,
 * where "no groupBy() is needed" -- the group's keys become a filter
 * and the rows come back as they are, sorted and capped as the cube
 * says. Group-level calculated columns still apply ("computed for each
 * row in the table, no matter whether it's a leaf-level row or an
 * aggregate"). Sorts on names only an aggregate has (a measure) are
 * dropped: a source row has no such column.
 *
 * A PIVOTED cube keeps its pivot, grouped by every dimension -- the
 * finest rows a pivot has, as upstream's pivot without its groupBy --
 * and so its sorts: a pivot column is this query's too.
 */
export function detailSnapshot(s: QuerySnapshot, parent: RowPath): QuerySnapshot {
  const keys = parentConditions(s, parent);
  const all = [...(s.filter ? [s.filter] : []), ...keys];
  const filter: FilterNode | undefined = all.length === 0 ? undefined
    : all.length === 1 ? all[0] : { kind: 'and', children: all };
  // the detail rows ask for no aggregate: a pivot's finest rows keep what the query asks of each value
  const { filter: _old, measures: asked, ...rest } = s;
  void _old;
  const visible = new Set([
    ...detailColumns(s),
    ...(s.groupDerived ?? []).map((d) => d.name),
  ]);
  const base: QuerySnapshot = {
    ...rest,
    ...(filter ? { filter } : {}),
    leafCount: false,
  };
  if (s.pivotOn.length === 0) return { ...base, rows: [], sorts: s.sorts.filter((x) => visible.has(x.column)) };
  const isOn = new Set(s.pivotOn);
  const dims = rowColumns(s)
    .filter((c) => c.kind === 'dimension' && !isOn.has(c.name))
    .map((c) => c.name);
  // The same pivot columns as every level above it: the cube's values,
  // not the group's, so a group with no EMEA rows shows an empty EMEA
  // column rather than a missing one. Its sorts are the cube's: the level orders by only what its groupBy
  // writes (levelRelation's `produced`), so a sort on a pivot column orders the rows, as it does the groups above
  // (dropped as a source row's, an opened group's rows came back unsorted under a sorted header: found 2026-10-18)
  return { ...base, rows: dims, ...(asked !== undefined ? { measures: asked } : {}) };
}

// ---------------------------------------------------------------------------
// THE PIVOT, as two plain queries (docs/DATACUBE_CUBE_PLAN_DESIGN_2026_09_27.md).
//
// Step 1 finds the values (`pivotValuesQuery`); step 2 is one groupBy per
// level with a conditional aggregate per value and measure (`serialize`).
// Both are ordinary Pure, planned by legend-lite with static types, so a
// warehouse reader may run them and every engine answers the same SQL.
// ---------------------------------------------------------------------------

/** More value combinations than this is refused, not cut off. */
export const MAX_PIVOT_VALUES = 500;

/** The header text of a NULL pivot value's column. */
export const EMPTY_PIVOT_LABEL = '(empty)';

/**
 * Step 1's answer: the value combinations present, in header order.
 *
 * Each value is a group key -- a cell's exact text, or null -- so a pivot
 * value and a tree key become a literal by one rule (`parentConditions`).
 */
export interface PivotFacts {
  readonly tuples: readonly (readonly GroupKey[])[];
}

/**
 * A column the pivot makes: a CELL (one value combination crossed with
 * one measure) or a TOTAL (`tuple` null: the measure over every value).
 *
 * What a pivot column IS comes from here, never from parsing its name.
 */
export interface PivotColumn {
  readonly name: string;
  readonly measure: Measure;
  readonly tuple: readonly GroupKey[] | null;
}

/** A pivot value as its column's header shows it. */
export function pivotLabel(key: GroupKey): string {
  return key === null ? EMPTY_PIVOT_LABEL : key;
}

/**
 * The measures a pivot spreads across its values.
 *
 * The configured ones, minus any whose column is excluded from the
 * pivot (that setting did nothing on a cube with configured measures,
 * P2-16). None configured: every measure-kind column that is neither a
 * pivot key nor excluded, on its own aggregate -- upstream's
 * `_pivotAggCols`. Nothing at all: a filler count, as upstream's
 * `_fixEmptyAggCols`, because a pivot has to show something.
 */
export function spreadMeasures(s: QuerySnapshot): Measure[] {
  const excluded = excludedFromPivot(s);
  const on = s.pivotOn;
  if (askedMeasures(s).length > 0) {
    const kept = askedMeasures(s).filter((m) => !excluded.has(m.column));
    if (kept.length > 0) return kept;
  } else {
    const isOn = new Set(on);
    const specOf = columnSpecs(s);
    const synthesised = rowColumns(s)
      .filter((c) => !isOn.has(c.name) && !excluded.has(c.name) && c.kind === 'measure')
      .map((c) => defaultMeasure(c.name, specOf.get(c.name)));
    if (synthesised.length > 0) return synthesised;
  }
  return [{ name: 'count', column: on[0] ?? '', fn: 'count' }];
}

/**
 * The columns a pivoted cube's queries make, in header order: each value
 * combination's cells, measure by measure, then the Totals when the cube
 * shows them. A deterministic function of the cube and step 1's answer,
 * so the query and every reader of its result agree on it.
 *
 * A cell is named `value__|__measure` (upstream's spelling). A name that
 * is already taken -- by a source column, or by a real value that reads
 * like a NULL's "(empty)" -- gets a `~2` suffix: names are identifiers
 * here, and nothing reads meaning out of them.
 */
export function pivotColumns(s: QuerySnapshot, facts: PivotFacts): PivotColumn[] {
  const spread = spreadMeasures(s);
  const taken = new Set([
    ...detailColumns(s),
    ...(s.groupDerived ?? []).map((d) => d.name),
  ]);
  const out: PivotColumn[] = [];
  for (const tuple of facts.tuples) {
    for (const measure of spread) {
      const base = [...tuple.map(pivotLabel), measure.name].join(PIVOT_SEPARATOR);
      let name = base;
      for (let n = 2; taken.has(name); n++) name = `${base}~${n}`;
      taken.add(name);
      out.push({ name, measure, tuple });
    }
  }
  const total = s.pivotTotal;
  if (total) {
    for (const measure of spread) {
      const fn = total.functions?.[measure.column] ?? measure.fn;
      const { weight: _w, ...rest } = measure;
      const retargeted: Measure = SECOND_COLUMN[fn] !== undefined && measure.weight !== undefined
        ? { ...rest, fn, weight: measure.weight }
        : { ...rest, fn };
      out.push({ name: pivotTotalColumn(measure.name), measure: retargeted, tuple: null });
    }
  }
  return out;
}

/**
 * A pivot names a column after each distinct value, and a Variant's value
 * is a whole JSON document: every column would be called
 * `{"items": [...]}__|__qty`. The question is always about a value INSIDE
 * the document, so say how to get it -- before either step runs.
 */
export function refuseUnpivotable(s: QuerySnapshot): void {
  const specOf = columnSpecs(s);
  for (const name of s.pivotOn) {
    if (isVariant(specOf.get(name)?.type)) {
      throw new CubeRefusal(
        `cannot pivot on '${name}': it holds JSON. Pivot on a value `
        + `extracted from it instead -- a calculated column such as `
        + `x|${columnRef(name)}->get('key')->to(@String)`,
      );
    }
  }
}

/** Pinned values as step 1's answer (`pivotValues`, one key). */
export function pinnedPivotFacts(s: QuerySnapshot): PivotFacts | null {
  if (s.pivotValues === undefined || s.pivotValues.length === 0) return null;
  // A key as a tree key is: the value's exact text (a JSON value its document's text)
  const key = (v: FilterValue): GroupKey => (isJsonValue(v) ? v.json
    : isRelativeDate(v) ? relativeDateWords(v) : String(v));
  return { tuples: s.pivotValues.map((v) => [key(v)]) };
}

/**
 * The columns a grouped pivot carries BESIDE its cells, each on its own
 * aggregate in the same groupBy (upstream's `pivotGroupByColumns`): the
 * other dimensions take their unique value, the other measures their
 * own aggregate. A configured measure excluded from the pivot is one of
 * them, under its own name. Before, these came from a second query
 * joined in by key; now they are columns of the level's own.
 */
export function carriedMeasures(s: QuerySnapshot, groupCols: readonly string[]): Measure[] {
  const excluded = excludedFromPivot(s);
  const isKey = new Set([...groupCols, ...s.pivotOn]);
  const spread = spreadMeasures(s);
  const excludedConfigured = askedMeasures(s).filter((m) => excluded.has(m.column));
  const handled = new Set([
    ...spread.map((m) => m.column),
    ...excludedConfigured.map((m) => m.column),
  ]);
  const specOf = columnSpecs(s);
  const out: Measure[] = [...excludedConfigured];
  for (const name of detailColumns(s)) {
    if (isKey.has(name) || handled.has(name)) continue;
    out.push(defaultMeasure(name, specOf.get(name)));
  }
  return out;
}

/**
 * Serialize a snapshot to Pure relation grammar.
 *
 * The pipeline is emitted in the order legend-lite expects, and each
 * stage is omitted entirely when it would be a no-op, so a simple cube
 * produces simple text that a human can read in a bug report.
 */
/** No grouping, no pivot, no measures: rows straight through. */
export function isDetail(s: QuerySnapshot): boolean {
  return (
    s.rows.length === 0 && s.pivotOn.length === 0 && askedMeasures(s).length === 0
  );
}

/**
 * Every column an aggregate default may consult, derived included.
 *
 * A calculated column has a type once a result has landed (see
 * `DerivedColumn.type`), and the default has to see it: a numeric one
 * must sum like any other number rather than fall through to `unique`.
 * Source columns win a name collision, which cannot happen anyway --
 * `nameProblem` refuses it in the editor.
 */
export interface SpecLike {
  readonly name: string;
  readonly type?: string;
  readonly kind?: ColumnKind;
  readonly aggregate?: AggregateFn;
  readonly aggregateWeight?: string;
}

export function columnSpecs(s: Omit<QuerySnapshot, 'source'>): Map<string, SpecLike> {
  const out = new Map<string, SpecLike>();
  for (const d of [...s.derived, ...(s.groupDerived ?? [])]) {
    // The DECLARED kind wins over the type, exactly as it does for a
    // source column: `kindOf` reads an explicit kind first, and a
    // calculated column the user called a dimension must not sum
    // because its values happen to be numeric.
    out.set(d.name, {
      name: d.name,
      ...(d.type === undefined ? {} : { type: d.type }),
      ...(d.kind === undefined ? {} : { kind: d.kind }),
      ...(d.aggregate === undefined ? {} : { aggregate: d.aggregate }),
      ...(d.aggregateWeight === undefined
        ? {}
        : { aggregateWeight: d.aggregateWeight }),
    });
  }
  for (const c of s.columns) out.set(c.name, c);
  return out;
}

/**
 * THE AGGREGATE A COLUMN TAKES BY DEFAULT, the one rule every query
 * builder asks: `sum` for a numeric column not declared a dimension,
 * `unique` for any other. A declared measure that is no number is not
 * summed -- the column editor starts a new column a measure whatever its
 * type, and a saved cube can declare one, and `sum` refuses a Boolean or
 * a String. An untyped column is not numeric: every column is typed
 * before a query reads it (a refresh's and a compile's step 0, plan.ts
 * `typeColumns`), so only a runner that types nothing leaves one.
 */
export function defaultAggregate(spec: SpecLike | undefined): AggregateFn {
  return isNumeric(spec?.type) && spec?.kind !== 'dimension' ? 'sum' : 'unique';
}

/**
 * A GROUPED LEVEL'S AGGREGATES with no pivot, as its query writes them: every projected column but the keys -- on
 * each of its configured measures, under each one's name, or on its default aggregate under its own -- then each
 * measure of a grouping column (a count of its rows). Every measure is in it: keyed by column, all but a column's last
 * measure, and any of a grouping column, were left out without a word (found 2026-10-16, PR neema2/legend-lite#47).
 * The one list: the level query aggregates it, and the group stage's calculated columns are offered it (calc.ts
 * columnsInScope).
 */
export function groupedMeasures(
  snapshot: QuerySnapshot, keys: readonly string[], projected: readonly string[] = detailColumns(snapshot),
): Measure[] {
  const isKey = new Set(keys);
  const byColumn = new Map<string, Measure[]>();
  for (const m of askedMeasures(snapshot)) byColumn.set(m.column, [...(byColumn.get(m.column) ?? []), m]);
  const specOf = columnSpecs(snapshot);
  const out: Measure[] = [];
  for (const name of projected) {
    if (!isKey.has(name)) out.push(...(byColumn.get(name) ?? [defaultMeasure(name, specOf.get(name))]));
  }
  for (const m of askedMeasures(snapshot)) if (isKey.has(m.column)) out.push(m);
  return out;
}

/**
 * The aggregate a column takes when nothing configured a MEASURE for
 * it: Column Properties > Aggregation when set (census §2 -- the
 * dropdown reached no query before), else the default (`defaultAggregate`).
 */
export function defaultMeasure(name: string, spec: SpecLike | undefined): Measure {
  return {
    name,
    column: name,
    fn: spec?.aggregate ?? defaultAggregate(spec),
    ...(spec?.aggregateWeight !== undefined
      ? { weight: spec.aggregateWeight }
      : {}),
  };
}

/**
 * Every column available BEFORE aggregation: the source's, plus the
 * row-stage calculated ones.
 *
 * `groupDerived` is deliberately absent. Those are extended AFTER the
 * groupBy -- that is the whole point of the stage -- so naming one in
 * the projection asks the source for a column that does not exist
 * yet. It did, and the planner said so: "unknown column 'margin' in
 * (region:String[0..1], ...)". Every group-stage calculated column
 * was unusable for as long as that line was here.
 */
export function detailColumns(s: QuerySnapshot): string[] {
  return [
    ...s.columns.map((c) => c.name),
    ...s.derived.map((d) => d.name),
  ];
}

/**
 * Whether this query can only ever return ONE row.
 *
 * True for an aggregate with nothing to group by -- the grand total.
 * False for a detail query, which also has no grouping but returns
 * every row, and therefore very much wants a sort and a cap.
 */
export function isSingleRow(
  s: QuerySnapshot,
  groupCols: readonly string[],
  grandTotal = false,
): boolean {
  return (
    groupCols.length === 0
    && (grandTotal || askedMeasures(s).length > 0 || s.pivotOn.length > 0)
  );
}

// ---- drill-through: the rows behind a number ----
//
// It turns every subtotal from an assertion into an auditable claim: an analyst who can click a
// figure and see the records that produced it can defend it. The query (`drillLambda`) is the
// cube's own filter, plus the clicked row's group path, plus the clicked column's pivot value --
// and then NO aggregation, so the result is provably the population the aggregate was computed
// over. Against a snap it reads the frozen rows, the same data the aggregate came from.

export interface DrillRequest {
  /** The clicked row's group path; [] for the grand total. */
  readonly path: RowPath;
  /**
   * The clicked column's pivot values, outermost first. Absent for a
   * row-dimension cell or an unpivoted cube.
   */
  readonly pivotPath?: readonly GroupKey[];
  /** Safety valve: drill-through is a peek, not an export. */
  readonly limit?: number;
}

export const DEFAULT_DRILL_LIMIT = 500;

/**
 * Conditions pinning the clicked cell.
 *
 * Exported because the set of conditions is the auditable part: a
 * reviewer should be able to read exactly which rows were counted.
 */
export function drillConditions(
  snapshot: QuerySnapshot,
  request: DrillRequest,
): FilterNode[] {
  const out: FilterNode[] = [];
  if (snapshot.filter) out.push(snapshot.filter);
  // TYPED, by the rule a tree level and a pivot cell use (`parentConditions`): a
  // year is `== 2021`, not `== '2021'`, and a NULL key is `isEmpty`. So
  // the drill pins exactly the rows its cell aggregated.
  out.push(...memberConditions(snapshot, snapshot.rows.slice(0, request.path.length), request.path));
  const pivot = request.pivotPath ?? [];
  if (pivot.length > 0) {
    out.push(...memberConditions(snapshot, snapshot.pivotOn.slice(0, pivot.length), pivot));
  }
  return out;
}
