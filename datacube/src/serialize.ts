// Snapshot -> Pure relation grammar, the text legend-lite compiles.
//
// This is the only place that knows the grammar. Every syntax form
// emitted here was taken from legend-lite's own test corpus rather than
// inferred, because a plausible-looking function name that the compiler
// rejects is the easiest possible way to waste a day:
//
//   sort        sort([~id->ascending(), ~name->ascending()])
//   slice       slice(1, 3)                 -- offset and END, not count
//   limit       limit(2)
//   select      select(~[NAME, deptName])
//   extend      extend(~[deptName: __r|$__r.n1.NAME])
//   groupBy     groupBy(~[grp], ~[total:x|$x.id:y|$y->count()])
//   pivot       pivot(~[year], ~[total:x|$x.treePlanted:y|$y->plus()])
//   concatenate concatenate($b)
//
// The pivot's group-by columns are IMPLICIT: legend-lite documents them
// as "source - pivot - aggregate-value columns", so selecting exactly
// the columns we want is what sets the grouping. There is no separate
// group-by argument to get wrong.

import {
  CubeRefusal,
  LEAF_COUNT_COLUMN,
  columnType,
  isNumericType,
  isRelativeDate,
  referencedColumns,
  rowColumns,
  totalOrderSorts,
  type AggregateFn,
  type ColumnKind,
  type CubeSnapshot,
  type FilterNode,
  type FilterValue,
  type Measure,
  type SortSpec,
} from './snapshot.ts';
import type { RowPath } from './tree.ts';
import { ROOT_COLUMN } from './grid/columns.ts';

/** What the grand total's synthetic key holds. Upstream's value. */
const ROOT_VALUE = '[ROOT]';

/** Identifiers that are not plain alphanumerics need quoting. */
const PLAIN_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Escape the inside of a single-quoted Pure string.
 *
 * THE BACKSLASH GOES FIRST, and the order is the whole point. Escaping
 * quotes alone turns a trailing backslash into an escape for the
 * CLOSING quote: `C:\` became `'C:\'`, an unterminated literal, and
 * `back\'` became `'back\''`, where the user's text stops being a
 * value and starts being grammar. The first is a crash from a path
 * somebody pasted; the second is injection, and a filter travels
 * inside a saved view that one person can hand to another.
 *
 * Escaping the backslash first makes the quote escape unambiguous,
 * because by then every backslash in the text is already doubled.
 */
function escapePure(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

export function ident(name: string): string {
  return PLAIN_IDENT.test(name) ? name : `'${escapePure(name)}'`;
}

/** A column reference on the lambda parameter, e.g. `$x.'odd name'`. */
function colRef(param: string, name: string): string {
  return `$${param}.${ident(name)}`;
}

/**
 * A date or a timestamp, in LOCAL terms and to the precision it has.
 *
 * Two decisions, and both were wrong before.
 *
 * LOCAL, not UTC. Every Date in this product is built in local terms
 * -- a date column arrives as epoch milliseconds and is rebuilt at
 * LOCAL midnight, which is what the grid then displays. Reading it
 * back with `toISOString()` shifts it: local midnight in New York is
 * 05:00 UTC the same day, but local midnight in Sydney is 13:00 UTC
 * the day BEFORE, so a filter built from a displayed date would name
 * a different day than the one on screen. This is the fourth
 * timezone fault in this area; every one of them came from mixing the
 * two frames.
 *
 * FULL PRECISION when there is any. A date keeps ten characters, but
 * a timestamp keeps its time, because truncating one made a group key
 * match its whole DAY -- drilling into a single minute of trading
 * returned every trade that day. A wrong answer with no error is
 * worse than an error. Midnight still prints as a plain date, which
 * compares correctly against a timestamp column anyway, and the core
 * parses both forms (`SpecParser.parseDateOrDateTime`).
 */
export function temporalLiteral(v: Date): string {
  const p = (n: number): string => String(n).padStart(2, '0');
  const day = `${v.getFullYear()}-${p(v.getMonth() + 1)}-${p(v.getDate())}`;
  const midnight =
    v.getHours() === 0 && v.getMinutes() === 0 && v.getSeconds() === 0;
  return midnight
    ? `%${day}`
    : `%${day}T${p(v.getHours())}:${p(v.getMinutes())}:${p(v.getSeconds())}`;
}

export function literal(v: FilterValue): string {
  if (isRelativeDate(v)) return v.relative === 'today' ? 'today()' : 'now()';
  if (typeof v === 'string') return `'${escapePure(v)}'`;
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (v instanceof Date) return temporalLiteral(v);
  if (Number.isInteger(v)) return String(v);
  return String(v);
}

/**
 * The map and reduce halves of an aggregate.
 *
 * `count` maps to the constant 1 rather than to a column, which is what
 * makes it null-insensitive and lets it work on a snapshot with no
 * numeric column at all. `wavg` needs a weight column and is rejected
 * without one, rather than silently degrading to a plain average --
 * a wrong weighted average is worse than an error.
 */
function aggregateLambdas(m: Measure): { map: string; reduce: string } {
  switch (m.fn) {
    case 'count':
      return { map: 'x|1', reduce: 'y|$y->count()' };
    case 'wavg': {
      if (!m.weight) {
        throw new CubeRefusal(
          `measure '${m.name}' uses wavg but has no weight column`,
        );
      }
      // The weight has to be captured in the MAP, where the row is
      // still in scope. Reducing over the value alone and reaching
      // for the weight in the reduce -- which is what this did --
      // cannot work: by then `$y` is a collection of the mapped
      // NUMBERS and the weight column is long gone. The engine says
      // so in as many words ("cannot access 'w' on Float"), and it
      // said it the first time this ran against a real engine rather
      // than a stub.
      //
      // wavgRowMapper pairs each value with its weight as the map
      // result, so the reduce is a plain wavg() over the pairs.
      return {
        map: `x|${colRef('x', m.column)}->wavgRowMapper(${colRef('x', m.weight)})`,
        reduce: `y|$y->wavg()`,
      };
    }
    case 'joinStrings':
      return {
        map: `x|${colRef('x', m.column)}`,
        reduce: `y|$y->joinStrings(', ')`,
      };
    case 'unique':
      return {
        map: `x|${colRef('x', m.column)}`,
        reduce: `y|$y->uniqueValueOnly()`,
      };
    default: {
      const fn: AggregateFn = m.fn;
      return {
        map: `x|${colRef('x', m.column)}`,
        reduce: `y|$y->${fn}()`,
      };
    }
  }
}

function aggregateSpec(m: Measure): string {
  const { map, reduce } = aggregateLambdas(m);
  return `${ident(m.name)}:${map}:${reduce}`;
}

const COMPARISON: Partial<Record<string, string>> = {
  equal: '==',
  notEqual: '!=',
  lessThan: '<',
  lessThanEqual: '<=',
  greaterThan: '>',
  greaterThanEqual: '>=',
};

/** Column-to-column comparisons, sharing the operators above. */
const COLUMN_COMPARISON: Partial<Record<string, string>> = {
  equalColumn: '==',
  equalCaseInsensitiveColumn: '==',
  notEqualColumn: '!=',
  notEqualCaseInsensitiveColumn: '!=',
  lessThanColumn: '<',
  lessThanEqualColumn: '<=',
  greaterThanColumn: '>',
  greaterThanEqualColumn: '>=',
};

/**
 * A literal, lower-cased BY PURE rather than by us.
 *
 * `toLower('EMEA')` instead of `'emea'`. It reads as the same
 * comparison on both sides -- column and value through the same
 * function -- which is how upstream writes it
 * (DataCubeQueryFilterOperation__EqualCaseInsensitive:
 * `equal(toLower(toOne($x.col)), toLower(value))`), and it keeps the
 * lowering rule the ENGINE's rather than JavaScript's. They are not
 * the same rule: JS lower-cases by Unicode default casing, a
 * database by its collation.
 */
function lowerLiteral(v: FilterValue): string {
  return typeof v === 'string' ? `toLower(${literal(v)})` : literal(v);
}

/**
 * A column, ready for `toLower`.
 *
 * A relational column is `[0..1]` -- nullable -- and `toLower` takes
 * `String[1]`, so the real engine refuses `$x.region->toLower()`
 * outright: "Can't find a match for function
 * 'toLower(Varchar(32)[0..1])'". Nine of our filter operators were
 * unusable upstream for want of this one call. `toOne` is what
 * upstream inserts, in exactly this position.
 */
function lowerRef(ref: string): string {
  return `${ref}->toOne()->toLower()`;
}

/** A literal already lower-cased, for the `in` lists. See below. */
function preLowered(v: FilterValue): string {
  return typeof v === 'string' ? literal(v.toLowerCase()) : literal(v);
}

export function filterExpression(node: FilterNode, param = 'x'): string {
  switch (node.kind) {
    case 'and':
    case 'or': {
      if (node.children.length === 0) {
        // An empty group is a no-op, not an error: the UI can hold one
        // while the user is still building a condition.
        return node.kind === 'and' ? 'true' : 'false';
      }
      const op = node.kind === 'and' ? ' && ' : ' || ';
      const parts = node.children.map((c) => filterExpression(c, param));
      return parts.length === 1 ? parts[0]! : `(${parts.join(op)})`;
    }
    case 'not':
      return `!(${filterExpression(node.child, param)})`;
    case 'condition': {
      const ref = colRef(param, node.column);
      const lower = lowerRef(ref);
      const one = () => literal(node.value as FilterValue);
      const many = () => (node.value as readonly FilterValue[]) ?? [];

      const cmp = COMPARISON[node.operator];
      if (cmp) return `${ref} ${cmp} ${one()}`;

      const colCmp = COLUMN_COMPARISON[node.operator];
      if (colCmp) {
        if (!node.rightColumn) {
          throw new CubeRefusal(
            `operator '${node.operator}' on '${node.column}' needs a rightColumn`,
          );
        }
        // Case-insensitive column comparisons lower BOTH columns, for
        // the same reason the literal forms do: collation differs
        // between backends, and the same cube must not answer
        // differently on two engines.
        const insensitive = node.operator.includes('CaseInsensitive');
        const right = colRef(param, node.rightColumn);
        return insensitive
          ? `${lowerRef(ref)} ${colCmp} ${lowerRef(right)}`
          : `${ref} ${colCmp} ${right}`;
      }

      switch (node.operator) {
        case 'isEmpty':
          return `${ref}->isEmpty()`;
        case 'isNotEmpty':
          // isNotEmpty is a function in its own right, so use it rather
          // than negating isEmpty: the engine's own vocabulary reads
          // better in a generated query someone has to debug.
          return `${ref}->isNotEmpty()`;
        case 'contains':
          return `${ref}->contains(${one()})`;
        case 'notContains':
          return `!${ref}->contains(${one()})`;
        case 'startsWith':
          return `${ref}->startsWith(${one()})`;
        case 'notStartsWith':
          return `!${ref}->startsWith(${one()})`;
        case 'endsWith':
          return `${ref}->endsWith(${one()})`;
        case 'notEndsWith':
          return `!${ref}->endsWith(${one()})`;
        case 'in':
          return `${ref}->in([${many().map(literal).join(', ')}])`;
        case 'notIn':
          return `!${ref}->in([${many().map(literal).join(', ')}])`;

        // Both sides are lowered rather than trusting collation, which
        // differs between backends and would let the same cube answer
        // differently on two engines.
        case 'equalCaseInsensitive':
          return `${lower} == ${lowerLiteral(node.value as FilterValue)}`;
        case 'notEqualCaseInsensitive':
          return `${lower} != ${lowerLiteral(node.value as FilterValue)}`;
        // PRE-LOWERED, like the `in` lists below and for the same
        // reason: the engine's dialect translation cannot render
        // `toLower(<literal>)` in this position. Measured against a
        // running legend-engine 4.138.5 --
        // `toLower(col)->contains(toLower('rates'))` dies with a
        // StackOverflowError inside sqlDialect.pure, and
        // startsWith/endsWith with "Match failure: TypedFunction" --
        // while the identical query with a plain literal executes and
        // returns the right rows. Curiously `equal` DOES accept
        // `toLower(<literal>)`, which is why equalCaseInsensitive is
        // spelled the upstream way above; the inconsistency is the
        // engine's, not ours.
        //
        // The cost is the one the `in` lists already pay: the LITERAL
        // is folded by JavaScript's Unicode default casing while the
        // COLUMN is folded by the database's collation. Identical for
        // ASCII, and not for Turkish dotless i or German sharp s. A
        // working operator with that caveat beats one that cannot run.
        case 'containsCaseInsensitive':
          return `${lower}->contains(${preLowered(node.value as FilterValue)})`;
        case 'startsWithCaseInsensitive':
          return `${lower}->startsWith(${preLowered(node.value as FilterValue)})`;
        case 'endsWithCaseInsensitive':
          return `${lower}->endsWith(${preLowered(node.value as FilterValue)})`;
        // IN TAKES LITERALS, and only literals: the engine asserts
        // "IN is supported only for literal values or negative
        // numbers", so these two cannot wrap their values in
        // `toLower` the way every other case-insensitive comparison
        // does. The values are lowered here instead -- the one place
        // the rule is JavaScript's rather than the engine's, and the
        // reason upstream ships no builder for these two at all.
        case 'inCaseInsensitive':
          return `${lower}->in([${many().map(preLowered).join(', ')}])`;
        case 'notInCaseInsensitive':
          return `!${lower}->in([${many().map(preLowered).join(', ')}])`;

        default: {
          const never: never = node.operator as never;
          throw new Error(`unhandled filter operator: ${String(never)}`);
        }
      }
    }
  }
}

function sortClause(sorts: readonly SortSpec[]): string {
  const keys = sorts.map(
    (s) =>
      `~${ident(s.column)}->${
        s.direction === 'asc' ? 'ascending' : 'descending'
      }()`,
  );
  return `sort([${keys.join(', ')}])`;
}

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
}

/**
 * Sentinel for a group whose key is SQL NULL.
 *
 * A group label is a rendered string, so any printable sentinel could
 * collide with a real value; this one cannot be produced by
 * formatting.
 */
export const NULL_GROUP = '\u0000null';

/**
 * Conditions pinning a branch: region == 'EMEA', and so on.
 *
 * A NULL group key cannot be matched with `==`, so it becomes an
 * isEmpty test. Without that, expanding a group whose key is null
 * silently returns no children.
 */
/** The Pure types whose values are temporal. */
const TEMPORAL = new Set(['Date', 'StrictDate', 'DateTime']);

/**
 * A group key, turned back into the value it came from.
 *
 * Paths are TEXT -- one string per level -- so a temporal key
 * arrives here as the ISO form `groupValue` wrote. Comparing that
 * string against a timestamp column is what produced `Conversion
 * Error: invalid timestamp field format`, so the declared type of
 * the dimension decides how to read it back.
 *
 * A value that does not parse is left as text rather than turned
 * into `Invalid Date`: a filter that cannot be built is better than
 * one that silently matches nothing.
 */
function keyValue(type: string | undefined, value: string): FilterValue {
  if (type === undefined || !TEMPORAL.has(type)) return value;
  const at = new Date(value);
  return Number.isNaN(at.getTime()) ? value : at;
}

/**
 * The conditions pinning a member of a hierarchy -- `columns[i] ==
 * path[i]` down its path, typed as the tree's keys are. Ad Hoc Analysis mode's
 * members use the same rule the tree's branches do.
 */
export function memberConditions(
  snapshot: CubeSnapshot,
  columns: readonly string[],
  path: RowPath,
): FilterNode[] {
  return parentConditions({ ...snapshot, rows: columns }, path);
}

function parentConditions(
  snapshot: CubeSnapshot,
  parent: RowPath,
): FilterNode[] {
  const out: FilterNode[] = [];
  // Calculated columns too: a group on one has keys of its own type.
  const typeOf = new Map(rowColumns(snapshot).map((c) => [c.name, c.type]));
  parent.forEach((value, i) => {
    const column = snapshot.rows[i];
    if (column === undefined) return;
    out.push(
      value === NULL_GROUP
        ? { kind: 'condition', column, operator: 'isEmpty' }
        : {
            kind: 'condition',
            column,
            operator: 'equal',
            value: keyValue(typeOf.get(column), value),
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
 * aggregate"). Sorts on names only an aggregate has (a measure, a
 * pivot column) are dropped: a source row has no such column.
 *
 * A PIVOTED cube keeps its pivot, grouped by every dimension -- the
 * finest rows a pivot has, as upstream's pivot without its groupBy.
 */
export function detailSnapshot(s: CubeSnapshot, parent: RowPath): CubeSnapshot {
  const keys = parentConditions(s, parent);
  const all = [...(s.filter ? [s.filter] : []), ...keys];
  const filter: FilterNode | undefined = all.length === 0 ? undefined
    : all.length === 1 ? all[0] : { kind: 'and', children: all };
  const { filter: _old, pivotCast, ...rest } = s;
  void _old;
  const visible = new Set([
    ...detailColumns(s),
    ...(s.groupDerived ?? []).map((d) => d.name),
  ]);
  const base: CubeSnapshot = {
    ...rest,
    ...(filter ? { filter } : {}),
    sorts: s.sorts.filter((x) => visible.has(x.column)),
    leafCount: false,
  };
  if (s.pivotOn.length === 0) return { ...base, rows: [], measures: [] };
  const isOn = new Set(s.pivotOn);
  const dims = rowColumns(s)
    .filter((c) => c.kind === 'dimension' && !isOn.has(c.name))
    .map((c) => c.name);
  return { ...base, rows: dims, ...(pivotCast ? { pivotCast } : {}) };
}

/**
 * Serialize a snapshot to Pure relation grammar.
 *
 * The pipeline is emitted in the order legend-lite expects, and each
 * stage is omitted entirely when it would be a no-op, so a simple cube
 * produces simple text that a human can read in a bug report.
 */
/** No grouping, no pivot, no measures: rows straight through. */
function isDetail(s: CubeSnapshot): boolean {
  return (
    s.rows.length === 0 && s.pivotOn.length === 0 && s.measures.length === 0
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
interface SpecLike {
  readonly name: string;
  readonly type?: string;
  readonly kind?: ColumnKind;
  readonly aggregate?: AggregateFn;
  readonly aggregateWeight?: string;
}

function columnSpecs(s: CubeSnapshot): Map<string, SpecLike> {
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
 * The aggregate a column takes when nothing configured a MEASURE for
 * it: Column Properties > Aggregation when set (census §2 -- the
 * dropdown reached no query before), else the kind's default.
 */
function defaultMeasure(
  name: string,
  spec: SpecLike | undefined,
  fallback: AggregateFn,
): Measure {
  return {
    name,
    column: name,
    fn: spec?.aggregate ?? fallback,
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
function detailColumns(s: CubeSnapshot): string[] {
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
function isSingleRow(
  s: CubeSnapshot,
  groupCols: readonly string[],
  grandTotal = false,
): boolean {
  return (
    groupCols.length === 0
    && (grandTotal || s.measures.length > 0 || s.pivotOn.length > 0)
  );
}

export function serialize(
  snapshot: CubeSnapshot,
  scope?: LevelScope,
): string {
  const parts: string[] = [snapshot.source.expression];
  // Grouping columns for this level. With no scope the cube is flat
  // and every row dimension groups, which is the original behaviour.
  const groupCols = scope
    ? snapshot.rows.slice(0, Math.max(0, scope.level))
    : snapshot.rows;
  // THE ROOT OF A GROUPED CUBE IS A GROUP. Level 0 under row
  // dimensions is the grand total whether or not a measure was
  // configured -- and without this, a cube with no explicit measures
  // (every uploaded file) sent `t->limit(1001)` for it: the "Total"
  // row showed the FIRST TRADE's values, and the 1,001 raw rows set
  // off the truncation warning.
  const grandTotal = scope !== undefined && scope.level === 0
    && snapshot.rows.length > 0;

  for (const d of snapshot.derived) {
    parts.push(`extend(~[${ident(d.name)}: x|${d.expression}])`);
  }

  const conditions: FilterNode[] = [];
  if (snapshot.filter) conditions.push(snapshot.filter);
  if (scope) conditions.push(...parentConditions(snapshot, scope.parent));
  if (conditions.length === 1) {
    parts.push(`filter(x|${filterExpression(conditions[0]!)})`);
  } else if (conditions.length > 1) {
    parts.push(
      `filter(x|${filterExpression({ kind: 'and', children: conditions })})`,
    );
  }

  // Selecting exactly the needed columns is what sets the pivot's
  // implicit grouping, so this stage is load-bearing, not tidying.
  // Dropping a row dimension here is precisely what turns the detail
  // query into its subtotal.
  // With row groups but NO measures, `referencedColumns` narrows to
  // the keys alone -- and grouping then drops every other column,
  // which is what made them vanish from the grid. DataCube keeps
  // them: `_groupByAggCols` aggregates every SELECTED column that is
  // not a group key. So the projection has to carry them.
  // KEYS OR NONE. The grand total is a groupBy with no keys, and it
  // has to aggregate the same columns the levels below it do -- or
  // the total row sits blank under a column where every row beneath
  // it carries a figure, which reads as "no total for this" rather
  // than as a projection that dropped it.
  const grouping = snapshot.pivotOn.length === 0
    && (groupCols.length > 0 || snapshot.measures.length > 0 || grandTotal);
  const pivoting = snapshot.pivotOn.length > 0;

  /**
   * The columns a measureless pivot will aggregate.
   *
   * Measure-kind only, which is what `_pivotAggCols` selects, and
   * never a pivot key.
   */
  const measureLike = (): string[] => {
    const isOn = new Set(snapshot.pivotOn);
    // Row-stage calculated measures included: reading the source
    // columns alone dropped them from every column pivot, silently.
    return rowColumns(snapshot)
      .filter((c) => !isOn.has(c.name) && !c.excludedFromPivot
        && c.kind === 'measure')
      .map((c) => c.name);
  };

  /**
   * What to SELECT, which decides what a pivot groups by.
   *
   * A PIVOT TAKES ITS GROUPING FROM WHATEVER ELSE IS SELECTED. That
   * makes this stage load-bearing rather than tidying, and it is
   * where I broke a cube badly: to let a measureless pivot
   * synthesise its aggregates I widened the projection to every
   * column, which silently regrouped the cube BY every column.
   *
   * Grouped by region, desk and book, then pivoting year across the
   * top, the query came out as `pivot(~[year], ~[notional:sum,
   * pnl:sum])` over a projection of all twelve columns -- so the
   * implicit grouping was one row per trade. The measures split
   * across the years correctly and the row groups dissolved into a
   * thousand detail rows, while the row zone still showed region,
   * desk and book. The snapshot was right; the projection threw them
   * away.
   *
   * So a measureless pivot projects the row dimensions, the pivot
   * keys, and the synthesised measures -- and nothing else. A
   * measureless GROUP BY still projects everything, because there
   * `_groupByAggCols` aggregates every selected column and keeping
   * them is the point.
   */
  /**
   * Whether the outer groupBy can be written.
   *
   * Only with the pivot's own column names in hand, which arrive
   * from a result rather than from the snapshot the user built.
   */
  // A cast learned for a DIFFERENT set of measures is stale: it names
  // columns this pivot no longer makes (a measure since excluded) or
  // lacks ones it does (one since included), and a cast naming a
  // column the pivot did not produce is a binder error. So it is used
  // only when its measures are exactly the ones being spread;
  // otherwise the first stage runs alone and the app learns it again.
  const castFits = (): boolean => {
    const spread = new Set(snapshot.measures.length > 0
      ? snapshot.measures.map((m) => m.name)
      : measureLike());
    const named = new Set((snapshot.pivotCast ?? []).map((c) => c.measure));
    return spread.size === named.size && [...spread].every((m) => named.has(m));
  };
  const cast = pivoting && groupCols.length > 0 && castFits()
    ? (snapshot.pivotCast ?? [])
    : [];

  /**
   * Columns a pivoted cube carries THROUGH to the outer groupBy.
   *
   * DIMENSIONS only. A measure the pivot does not spread -- excluded
   * from it, or simply not a configured measure -- cannot be carried:
   * the pivot groups by everything it selects, so summing it
   * afterwards adds up the DISTINCT (row, value) pairs rather than the
   * rows. Its figure comes from the unpivoted query instead, joined by
   * key beside the pivot total (`pivotTotalQuery`).
   */
  const carried = (): string[] => {
    const isKey = new Set([...groupCols, ...snapshot.pivotOn]);
    const isMeasure = new Set(snapshot.measures.length > 0
      ? snapshot.measures.map((m) => m.column)
      : measureLike());
    const measureKind = new Set(rowColumns(snapshot)
      .filter((c) => c.kind === 'measure').map((c) => c.name));
    return detailColumns(snapshot)
      .filter((n) => !isKey.has(n) && !isMeasure.has(n) && !measureKind.has(n));
  };

  const needed = cast.length > 0
    // BOTH STAGES. The pivot spreads the measures and takes its
    // grouping from everything else selected -- a fine-grained
    // intermediate -- and the outer groupBy then collapses that to
    // the row dimensions, giving every carried column its unique
    // value. Grouping by a numeric is confined to that intermediate
    // and never reaches the answer.
    ? (() => {
        const out = referencedColumns(snapshot, groupCols);
        for (const name of [...(snapshot.measures.length === 0
          ? measureLike() : []), ...carried()]) {
          if (!out.includes(name)) out.push(name);
        }
        return out;
      })()
    : snapshot.measures.length === 0 && pivoting
    ? (() => {
        const out = referencedColumns(snapshot, groupCols);
        for (const name of measureLike()) {
          if (!out.includes(name)) out.push(name);
        }
        return out;
      })()
    : grouping
      ? detailColumns(snapshot)
      : referencedColumns(snapshot, groupCols);
  if (needed.length > 0) {
    parts.push(`select(~[${needed.map(ident).join(', ')}])`);
  } else if (isDetail(snapshot)) {
    // A DETAIL cube -- no grouping, no pivot, no measures -- is the
    // plainest thing this product can show, and it referenced no
    // columns at all, so nothing was projected and the bare relation
    // came back. Naming them makes the query say what the CUBE
    // declares rather than whatever the source happens to hold.
    const all = detailColumns(snapshot);
    if (all.length > 0) parts.push(`select(~[${all.map(ident).join(', ')}])`);
  }

  /**
   * Every column that is not a group key, aggregated.
   *
   * This is what DataCube does, and the reason grouping there does
   * not make the other columns vanish: `_groupByAggCols` takes every
   * SELECTED column that is not a group-by column and builds an
   * aggregate for it from that column's own operator, defaulting to
   * SUM for Integer/Decimal/Float and UNIQUE for everything else
   * (DataCubeConfigurationBuilder). A configured measure wins; the
   * rest fall back to those defaults rather than being dropped.
   *
   * It also never emits an empty aggregate list -- `_fixEmptyAggCols`
   * substitutes a filler count -- so neither does this.
   */
  function groupedAggs(
    keys: readonly string[],
    projected: readonly string[],
  ): string {
    const isKey = new Set(keys);
    const byMeasure = new Map(snapshot.measures.map((m) => [m.column, m]));
    const specOf = columnSpecs(snapshot);
    const specs: string[] = [];
    // Only what the SELECT kept: aggregating a column that was
    // projected away is not a wider answer, it is an unresolvable one.
    for (const name of projected) {
      if (isKey.has(name)) continue;
      const configured = byMeasure.get(name);
      const spec = specOf.get(name);
      // DataCube defaults purely on TYPE -- numbers sum, everything
      // else takes its unique value. That sums a year and an id,
      // which is nonsense, and the kind inference already knows
      // better: a numeric column it judged key-like is a dimension.
      // An explicit kind wins over the type it is carried in.
      const measures = spec?.kind === 'measure'
        || (isNumericType(spec?.type) && spec?.kind === undefined);
      specs.push(aggregateSpec(configured
        ?? defaultMeasure(name, spec, measures ? 'sum' : 'unique')));
    }
    // "Show leaf count": the rows under each group, beside its label.
    // Only where there IS a group -- the grand total has no label.
    if (snapshot.leafCount === true && keys.length > 0) {
      specs.push(`${ident(LEAF_COUNT_COLUMN)}:x|1:y|$y->count()`);
    }
    return specs.length > 0
      ? specs.join(', ')
      // Upstream's filler: a groupBy has to aggregate something.
      : `count:x|${colRef('x', keys[0] ?? '')}:y|$y->count()`;
  }

  const excludedFromPivot = new Set(
    snapshot.columns.filter((c) => c.excludedFromPivot).map((c) => c.name),
  );
  const pivotOn = snapshot.pivotOn.filter((c) => !excludedFromPivot.has(c));

  /**
   * What a PIVOT aggregates: the measures, and only the measures.
   *
   * `_pivotAggCols` takes every selected column whose kind is
   * MEASURE, minus the pivot columns themselves and anything
   * excluded from the pivot -- and its comment says why dimensions
   * are left out where `groupBy` includes them: "pivot aggregation on
   * dimension columns (e.g. unique values aggregator) are not
   * helpful". It then passes the list through `_fixEmptyAggCols`, so
   * a cube with no measures gets the filler count rather than a
   * refusal.
   *
   * This replaced a refusal of mine. Pivoting a cube with no
   * configured measure threw `CubeRefusal`, which was thrown from
   * inside a floating refresh -- so the click produced an uncaught
   * error, the grid kept the old view with no explanation, and
   * `pivotOn` stayed set, so every later query threw the same thing.
   * One click wedged the cube until a reload. Meanwhile the same
   * cube grouped happily, because `groupedAggs` has always
   * synthesised its aggregates. The inconsistency was mine, not
   * DataCube's.
   */
  function pivotAggs(on: readonly string[], projected: readonly string[]):
  string {
    // A CONFIGURED measure is the explicit ask and always aggregates,
    // whatever the projection holds. It has to come first and it has
    // to come from `measures` rather than from the projected columns:
    // a `count` does not need its source column, so that column is
    // deliberately not selected, and reading the projection instead
    // dropped the measure and substituted the filler -- which is
    // exactly what the count test caught.
    if (snapshot.measures.length > 0) {
      return snapshot.measures.map(aggregateSpec).join(', ');
    }
    // None configured: synthesise, which is the part that was
    // missing. Measures only -- `_pivotAggCols` excludes dimensions
    // where `groupBy` includes them, because "pivot aggregation on
    // dimension columns (e.g. unique values aggregator) are not
    // helpful".
    const isOn = new Set(on);
    const specOf = columnSpecs(snapshot);
    const specs: string[] = [];
    for (const name of projected) {
      if (isOn.has(name) || excludedFromPivot.has(name)) continue;
      const spec = specOf.get(name);
      // The same measure test the groupBy path uses: an explicit
      // kind wins, and a bare number defaults to a measure.
      const isMeasure = spec?.kind === 'measure'
        || (isNumericType(spec?.type) && spec?.kind === undefined);
      if (!isMeasure) continue;
      specs.push(aggregateSpec(defaultMeasure(name, spec, 'sum')));
    }
    return specs.length > 0
      ? specs.join(', ')
      // `_fixEmptyAggCols`: a pivot must aggregate something.
      : `count:x|${colRef('x', on[0] ?? '')}:y|$y->count()`;
  }

  if (pivotOn.length > 0) {
    const aggs = pivotAggs(pivotOn, needed);
    const on = pivotOn.map(ident).join(', ');
    if (snapshot.pivotValues && snapshot.pivotValues.length > 0) {
      // Pinning values also PRE-FILTERS the source, dropping groups
      // whose keys all sit outside the list. Only ever for a cube the
      // user deliberately narrowed -- never a scroll position.
      const vs = snapshot.pivotValues.map(literal).join(', ');
      parts.push(`pivot(~[${on}], [${vs}], ~[${aggs}])`);
    } else {
      parts.push(`pivot(~[${on}], ~[${aggs}])`);
    }

    // THE SECOND STAGE, when the pivot's columns are known.
    //
    // Without it a pivoted cube could only show the measures: every
    // other column had to stay out of the projection, because
    // anything projected becomes part of the pivot's own grouping and
    // one row per trade is not a grouped cube. With it, the carried
    // columns come back and take their unique value, which is what
    // `_groupByAggCols` does for `pivotGroupByColumns` -- "these are
    // the columns which are available for groupBy but not selected
    // for groupBy operation, they would be aggregated as well".
    if (cast.length > 0) {
      const byMeasure = new Map(snapshot.measures.map((m) => [m.column, m]));

      // THE CAST, which is what makes the groupBy below legal.
      //
      // A pivot's output columns do not exist in the relation's
      // TYPE, only in its data, so naming one in a later stage is
      // rejected outright: "relation has no column
      // '2021__|__notional'". DataCubeQueryBuilder emits
      // `cast(_castCols(pivot.castColumns))` between the two stages
      // for exactly this reason, and the shape legend-lite parses is
      // the structural relation annotation `@Relation<(col:Type,
      // ...)>` (SpecParser: "Type annotations (C.7)").
      //
      // It declares the WHOLE post-pivot relation -- what the pivot
      // preserved as well as what it produced -- because a cast
      // states the type of the value it is applied to, not a
      // difference from it. Upstream's `castColumns` likewise holds
      // both, which is why `_groupByAggCols` can split them apart
      // into pivot results and `pivotGroupByColumns`.
      const aggregated = new Set([
        ...snapshot.pivotOn,
        ...(snapshot.measures.length > 0
          ? snapshot.measures.map((m) => m.column)
          : measureLike()),
      ]);
      const typeOf = (name: string): string =>
        columnType(snapshot, name) ?? 'String';
      const decls = [
        ...needed.filter((n) => !aggregated.has(n))
          .map((n) => `${ident(n)}:${typeOf(n)}`),
        // A pivot result carries the type of the measure it
        // aggregates, except a count, which is a number of rows.
        ...cast.map((c) => `${ident(c.name)}:${
          byMeasure.get(c.measure)?.fn === 'count'
            ? 'Integer'
            : typeOf(c.measure)}`),
      ];
      parts.push(`cast(@Relation<(${decls.join(', ')})>)`);

      const outer: string[] = [];
      const specOfMeasure = columnSpecs(snapshot);
      for (const c of cast) {
        const base = byMeasure.get(c.measure)
          // A pivot result re-applies its measure's OWN aggregate, as
          // upstream's `_groupByAggCols` does -- configured in Column
          // Properties when there is no explicit measure.
          ?? defaultMeasure(c.measure, specOfMeasure.get(c.measure), 'sum');
        outer.push(aggregateSpec({
          name: c.name,
          column: c.name,
          fn: base.fn,
          ...(base.weight ? { weight: base.weight } : {}),
        }));
      }
      // A carried dimension takes its OWN configured aggregate, as
      // upstream's `_groupByAggCols` does; unique when none is set.
      for (const name of carried()) {
        outer.push(aggregateSpec(
          defaultMeasure(name, specOfMeasure.get(name), 'unique')));
      }
      const by = groupCols.map(ident).join(', ');
      parts.push(`groupBy(~[${by}], ~[${outer.join(', ')}])`);
    }
  } else if (snapshot.measures.length > 0 || groupCols.length > 0
    || grandTotal) {
    // No column dimension: an ordinary aggregation over the row
    // dimensions. Needs an explicit groupBy, since there is no pivot
    // to infer the grouping from.
    //
    // GROUP COLUMNS ALONE ARE ENOUGH. This used to require a measure,
    // so dragging a column into the row zone on a cube with no
    // measures emitted a plain select: the grid then showed one row
    // per SOURCE row -- "AMER" repeated down the screen -- and the
    // generated SQL had no GROUP BY in it at all. Grouping is what
    // the user asked for; an empty aggregate list is a detail of what
    // to show beside it, and `groupBy(~[region], ~[])` lowers to
    // exactly `GROUP BY t0.region`.
    // THE GRAND TOTAL IS A GROUP, NOT AN ABSENCE OF ONE.
    //
    // `groupBy(~[], ~[...])` is the obvious way to write "one group
    // over everything" and the real engine crashes on it -- a
    // NullPointerException out of the plan builder, not a refusal.
    // Upstream never writes it either: `_extendRootAggregation`
    // extends a constant column and groups by that, which is one
    // group by construction. The column is machinery and the grid
    // never shows it (`ROOT_COLUMN`).
    if (groupCols.length === 0) {
      parts.push(`extend(~[${ident(ROOT_COLUMN)}: x|${literal(ROOT_VALUE)}])`);
      parts.push(
        `groupBy(~[${ident(ROOT_COLUMN)}], ~[${
          groupedAggs(groupCols, needed)}])`,
      );
    } else {
      const by = groupCols.map(ident).join(', ');
      parts.push(`groupBy(~[${by}], ~[${groupedAggs(groupCols, needed)}])`);
    }
  }

  // Post-aggregation columns come AFTER the pivot or groupBy, which
  // is the whole point: they see the aggregates rather than the rows
  // that produced them.
  for (const d of snapshot.groupDerived ?? []) {
    parts.push(`extend(~[${ident(d.name)}: x|${d.expression}])`);
  }

  // A grand total is a single row; sorting and limiting it is noise
  // that only makes the generated text harder to read in a bug
  // report. But "no grouping columns" is NOT the same as "one row":
  // a detail cube has no grouping either and returns everything, so
  // this guard silently dropped its sort AND its row cap. The
  // plainest possible grid was the one that honoured neither.
  if (!isSingleRow(snapshot, groupCols, grandTotal)) {
    const sorts = totalOrderSorts(snapshot, groupCols);
    if (sorts.length > 0) parts.push(sortClause(sorts));

    if (scope?.limit !== undefined) {
      // The cap must come AFTER the sort, or it caps an arbitrary
      // subset and the first page is not the first page.
      parts.push(`limit(${scope.limit})`);
    } else if (snapshot.window) {
      // slice takes offset and END, not a count.
      const { offset, limit } = snapshot.window;
      parts.push(`slice(${offset}, ${offset + limit})`);
    }
  }

  return parts.join('->');
}

/**
 * The PIVOT TOTAL for one level: the level's own query with the pivot
 * key dropped.
 *
 * A row's total is its measure over the row's whole slice -- every
 * value of the pivot at once -- which is exactly what the unpivoted
 * query for the same level computes. So it comes from the database and
 * is right for every aggregate: an average of the slice, not a sum of
 * the pivot's averages; a count of rows, not of cells.
 *
 * Each measure takes its own aggregate unless its column configures a
 * different one for the total (upstream's
 * `pivotStatisticColumnFunction`).
 *
 * The rows it must cover are the pivot query's rows, and the two
 * queries may order differently (a sort on a pivot column exists only
 * in one of them), so it is never capped: `keys`, when given, names
 * the groups to fetch instead -- the pivot level's own, for a level
 * that was truncated. Group-stage calculated columns and sorts are
 * dropped: they may name pivot columns this query does not produce,
 * and the result is joined by key, not by position.
 *
 * Returns null when the cube has no pivot or no total.
 */
export function pivotTotalQuery(
  snapshot: CubeSnapshot,
  scope: LevelScope | undefined,
  keys?: readonly string[],
): {
  readonly pure: string;
  /** Measures whose pivot TOTAL this query computes. */
  readonly measures: readonly string[];
  /** Measure columns the pivot does not spread, on their own aggregate. */
  readonly carried: readonly string[];
} | null {
  const total = snapshot.pivotTotal;
  if (snapshot.pivotOn.length === 0) return null;
  const excluded = new Set(
    snapshot.columns.filter((c) => c.excludedFromPivot).map((c) => c.name),
  );
  if (snapshot.pivotOn.every((c) => excluded.has(c))) return null;

  const functions = total?.functions ?? {};
  const retarget = (m: Measure): Measure => {
    const fn = functions[m.column] ?? m.fn;
    const { weight: _w, ...rest } = m;
    return fn === 'wavg' && m.weight !== undefined
      ? { ...rest, fn, weight: m.weight }
      : { ...rest, fn };
  };
  // The same measures the pivot spreads: the configured ones, or the
  // measure-kind columns the pivot synthesises when none are.
  const isOn = new Set(snapshot.pivotOn);
  const specOf = columnSpecs(snapshot);
  const pivoted: Measure[] = snapshot.measures.length > 0
    ? [...snapshot.measures]
    : rowColumns(snapshot)
      .filter((c) => !isOn.has(c.name) && !excluded.has(c.name)
        && c.kind === 'measure')
      .map((c) => defaultMeasure(c.name, specOf.get(c.name), 'sum'));
  // Measure columns the pivot leaves alone: `carried` in `serialize`
  // keeps them out of the pivot, and they are answered here.
  const spread = new Set(pivoted.map((m) => m.column));
  const carriedMeasures: Measure[] = rowColumns(snapshot)
    .filter((c) => c.kind === 'measure' && !isOn.has(c.name)
      && !spread.has(c.name) && !snapshot.rows.includes(c.name))
    .map((c) => defaultMeasure(c.name, specOf.get(c.name), 'sum'));
  const totals = total === undefined ? [] : pivoted;
  if (totals.length === 0 && carriedMeasures.length === 0) return null;

  const conditions: FilterNode[] = [];
  if (snapshot.filter) conditions.push(snapshot.filter);
  // Pinned pivot values pre-filter the pivot's source; the total must
  // add up the same rows.
  const on = snapshot.pivotOn[0];
  if (snapshot.pivotValues && snapshot.pivotValues.length > 0
    && snapshot.pivotOn.length === 1 && on !== undefined) {
    conditions.push({
      kind: 'or',
      children: snapshot.pivotValues.map((value) => ({
        kind: 'condition' as const, column: on, operator: 'equal' as const, value,
      })),
    });
  }
  const level = scope?.level ?? 0;
  const groupColumn = level > 0 ? snapshot.rows[level - 1] : undefined;
  if (keys !== undefined && groupColumn !== undefined) {
    const typeOf = new Map(rowColumns(snapshot).map((c) => [c.name, c.type]));
    conditions.push({
      kind: 'or',
      children: keys.map((key): FilterNode => (key === NULL_GROUP
        ? { kind: 'condition', column: groupColumn, operator: 'isEmpty' }
        : {
            kind: 'condition',
            column: groupColumn,
            operator: 'equal',
            value: keyValue(typeOf.get(groupColumn), key),
          })),
    });
  }

  const {
    pivotValues: _v, pivotCast: _c, pivotTotal: _t, window: _win,
    filter: _f, ...rest
  } = snapshot;
  // Only the group keys and what the measures read: every other source
  // column would be aggregated for nothing (the grouped path keeps them
  // because the grid shows them; the total shows only its measures).
  // Filters and row-stage calculations run before the projection, so
  // they still see every column.
  const asked = [...totals.map(retarget), ...carriedMeasures];
  const keep = new Set([
    ...snapshot.rows,
    ...asked.flatMap((m) => [m.column, ...(m.weight ? [m.weight] : [])]),
  ]);
  const unpivoted: CubeSnapshot = {
    ...rest,
    columns: snapshot.columns.filter((c) => keep.has(c.name)),
    pivotOn: [],
    measures: asked,
    groupDerived: [],
    sorts: [],
    leafCount: false,
    ...(conditions.length === 1
      ? { filter: conditions[0]! }
      : conditions.length > 1
        ? { filter: { kind: 'and', children: conditions } }
        : {}),
  };
  const levelScope = scope === undefined
    ? undefined
    : { level: scope.level, parent: scope.parent };
  return {
    pure: serialize(unpivoted, levelScope),
    measures: totals.map((m) => m.name),
    carried: carriedMeasures.map((m) => m.name),
  };
}
