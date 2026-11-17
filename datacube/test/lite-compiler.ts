// legend-lite's own parser and printer -- the product's planner (LitePlanner's parse and compose:
// legend-engine's pure/v1 grammar calls), from the test's host (#test-host) -- for tests that check
// WHAT QUERY DataCube builds: fixtures parsed as the compiler parses what a person typed, and
// queries read back as the compiler prints them. A test using this runs in the WASM group
// (BUILD.bazel `_WASM_TESTS`). Every answer of the compiler is awaited, as DataCube's own planner
// API is: the compiler may answer from another thread or process
// (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §6). The query is still built synchronously,
// so a refusal of the builder throws as before.

import type { Lambda, ValueSpecification } from '../../pure-protocol/src/index.ts';
import { plannerFor } from './catalog-builder.ts';
import {
  childAggregateLambda,
  drillLambda,
  filterNode,
  levelLambda,
  pivotValuesLambda,
  type DrillRequest,
  type LevelScope,
  type PivotFacts,
  type TypeOf,
} from '../src/query.ts';
import type { QuerySnapshot, FilterNode } from '../src/snapshot.ts';
import { lambda } from '../../pure-protocol/src/index.ts';

/** The grammar's planner: no model of its own is needed to parse and print. */
const grammar = plannerFor('', '');

/** E1's twin: what a person typed, as its lambda. */
export async function liteParse(text: string): Promise<Lambda> {
  return grammar.parse(text);
}

/** E4's twin: a query as one line of Pure. */
export async function litePrint(query: Lambda): Promise<string> {
  return print(query);
}

/** A calculated column's lambda from the body a person typed, `$x.a * 2`: parsed by lite. */
export async function row(body: string): Promise<Lambda> {
  return grammar.parse(`x|${body}`);
}

/** A relation expression a person typed, `#>{db::DB.T}#`: parsed by lite. */
export async function source(text: string): Promise<ValueSpecification> {
  const parsed = (await grammar.parse(`|${text}`)).body[0];
  if (parsed === undefined) throw new Error(`not an expression: ${text}`);
  return parsed;
}

/** A query as one line of Pure, as lite prints it. */
export function print(query: Lambda): Promise<string> {
  return grammar.compose(query, 'standard');
}

/** A level's query, printed. */
export function printLevel(s: QuerySnapshot, scope?: LevelScope, pivot?: PivotFacts): Promise<string> {
  return print(levelLambda(s, scope, pivot));
}

/** A pivot's values query, printed; null when the cube does not ask one. */
export function printValues(s: QuerySnapshot): Promise<string | null> {
  const q = pivotValuesLambda(s);
  return q === null ? Promise.resolve(null) : print(q);
}

/** A level's child-group aggregates query, printed, and the columns it answers. */
export function printChildren(
  s: QuerySnapshot,
  scope: LevelScope,
): Promise<{ readonly pure: string; readonly columns: readonly string[] } | null> {
  const q = childAggregateLambda(s, scope);
  return q === null ? Promise.resolve(null) : print(q.query).then((pure) => ({ pure, columns: q.columns }));
}

/** A drill's query, printed. */
export function printDrill(s: QuerySnapshot, request: DrillRequest): Promise<string> {
  return print(drillLambda(s, request));
}

/** A filter as the lambda `x|<condition>`, printed. */
export function printFilter(node: FilterNode, typeOf?: TypeOf): Promise<string> {
  return print(lambda(['x'], filterNode(node, 'x', typeOf)));
}
