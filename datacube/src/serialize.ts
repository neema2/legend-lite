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
  type AggregateFn,
  type CubeSnapshot,
  type FilterNode,
  type FilterValue,
  type Measure,
  type SortSpec,
  referencedColumns,
  totalOrderSorts,
} from './snapshot.ts';
import type { RowPath } from './tree.ts';

/** Identifiers that are not plain alphanumerics need quoting. */
const PLAIN_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function ident(name: string): string {
  return PLAIN_IDENT.test(name) ? name : `'${name.replace(/'/g, "\\'")}'`;
}

/** A column reference on the lambda parameter, e.g. `$x.'odd name'`. */
function colRef(param: string, name: string): string {
  return `$${param}.${ident(name)}`;
}

export function literal(v: FilterValue): string {
  if (typeof v === 'string') return `'${v.replace(/'/g, "\\'")}'`;
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (v instanceof Date) return `%${v.toISOString().slice(0, 10)}`;
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
        throw new Error(
          `measure '${m.name}' uses wavg but has no weight column`,
        );
      }
      return {
        map: `x|${colRef('x', m.column)}`,
        reduce: `y|$y->wavg(${colRef('y', m.weight)})`,
      };
    }
    case 'joinStrings':
      return {
        map: `x|${colRef('x', m.column)}`,
        reduce: `y|$y->joinStrings(', ')`,
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
  notEqualColumn: '!=',
  lessThanColumn: '<',
  lessThanEqualColumn: '<=',
  greaterThanColumn: '>',
  greaterThanEqualColumn: '>=',
};

/** Lower-cased literal, for the case-insensitive comparisons. */
function lowerLiteral(v: FilterValue): string {
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
      const lower = `${ref}->toLower()`;
      const one = () => literal(node.value as FilterValue);
      const many = () => (node.value as readonly FilterValue[]) ?? [];

      const cmp = COMPARISON[node.operator];
      if (cmp) return `${ref} ${cmp} ${one()}`;

      const colCmp = COLUMN_COMPARISON[node.operator];
      if (colCmp) {
        if (!node.rightColumn) {
          throw new Error(
            `operator '${node.operator}' on '${node.column}' needs a rightColumn`,
          );
        }
        return `${ref} ${colCmp} ${colRef(param, node.rightColumn)}`;
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
        case 'containsCaseInsensitive':
          return `${lower}->contains(${lowerLiteral(node.value as FilterValue)})`;
        case 'startsWithCaseInsensitive':
          return `${lower}->startsWith(${lowerLiteral(node.value as FilterValue)})`;
        case 'endsWithCaseInsensitive':
          return `${lower}->endsWith(${lowerLiteral(node.value as FilterValue)})`;
        case 'inCaseInsensitive':
          return `${lower}->in([${many().map(lowerLiteral).join(', ')}])`;
        case 'notInCaseInsensitive':
          return `!${lower}->in([${many().map(lowerLiteral).join(', ')}])`;

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
function parentConditions(
  snapshot: CubeSnapshot,
  parent: RowPath,
): FilterNode[] {
  const out: FilterNode[] = [];
  parent.forEach((value, i) => {
    const column = snapshot.rows[i];
    if (column === undefined) return;
    out.push(
      value === NULL_GROUP
        ? { kind: 'condition', column, operator: 'isEmpty' }
        : { kind: 'condition', column, operator: 'equal', value },
    );
  });
  return out;
}

/**
 * Serialize a snapshot to Pure relation grammar.
 *
 * The pipeline is emitted in the order legend-lite expects, and each
 * stage is omitted entirely when it would be a no-op, so a simple cube
 * produces simple text that a human can read in a bug report.
 */
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
  const needed = referencedColumns(snapshot, groupCols);
  if (needed.length > 0) {
    parts.push(`select(~[${needed.map(ident).join(', ')}])`);
  }

  const aggs = snapshot.measures.map(aggregateSpec).join(', ');

  if (snapshot.pivotOn.length > 0) {
    const on = snapshot.pivotOn.map(ident).join(', ');
    if (snapshot.pivotValues && snapshot.pivotValues.length > 0) {
      // Pinning values also PRE-FILTERS the source, dropping groups
      // whose keys all sit outside the list. Only ever for a cube the
      // user deliberately narrowed -- never a scroll position.
      const vs = snapshot.pivotValues.map(literal).join(', ');
      parts.push(`pivot(~[${on}], [${vs}], ~[${aggs}])`);
    } else {
      parts.push(`pivot(~[${on}], ~[${aggs}])`);
    }
  } else if (snapshot.measures.length > 0) {
    // No column dimension: an ordinary aggregation over the row
    // dimensions. Needs an explicit groupBy, since there is no pivot to
    // infer the grouping from.
    const by = groupCols.map(ident).join(', ');
    parts.push(`groupBy(~[${by}], ~[${aggs}])`);
  }

  // A grand total is a single row; sorting and slicing it is noise
  // that only makes the generated text harder to read in a bug report.
  if (groupCols.length > 0) {
    const sorts = totalOrderSorts(snapshot, groupCols);
    if (sorts.length > 0) parts.push(sortClause(sorts));

    if (snapshot.window) {
      // slice takes offset and END, not a count.
      const { offset, limit } = snapshot.window;
      parts.push(`slice(${offset}, ${offset + limit})`);
    }
  }

  return parts.join('->');
}
