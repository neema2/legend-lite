// Running a query: the lambda for execution (with `->from()`, a preview limit one past what is
// shown so an overflow is visible), its parameter values bound at run, run on lite's engine, abortable.

import { element, fn, lambda as makeLambda, lit, type Lambda, type ValueSpecification, type Variable } from '../../../pure-protocol/src/index.ts';
import type { ParameterValue } from '../../../engine-client/src/legend/browser-engine.ts';
import { buildLambda, valueSpec } from '../builder/build.ts';
import type { Value } from '../builder/state.ts';
import type { AppContext } from './context.ts';
import type { Session } from './session.ts';

export const DEFAULT_PREVIEW = 1000;

/** The lambda a text-only query runs: its own `->from()`, or the session's mapping and runtime. */
function textLambdaForRun(session: Session, l: Lambda, previewLimit: number | undefined): Lambda {
  const last = l.body[l.body.length - 1] as ValueSpecification & { function?: string };
  let body = last;
  const hasFrom = last._type === 'func' && /(^|::)from$/.test(last.function ?? '');
  if (previewLimit !== undefined) {
    // a preview wraps the whole query: from() stays outermost
    if (hasFrom) {
      const f = last as unknown as { function: string; parameters: ValueSpecification[] };
      body = fn(f.function, fn('limit', f.parameters[0]!, { _type: 'integer', value: previewLimit }), ...f.parameters.slice(1));
    } else body = fn('limit', body, { _type: 'integer', value: previewLimit });
  }
  if (!hasFrom) {
    const src = session.query.source;
    body = fn('from', body, element(src.mapping), element(src.runtime));
  }
  return makeLambda(l.parameters, ...l.body.slice(0, -1), body);
}

/** The lambda to execute for the session's query. */
export function executionLambda(session: Session, previewLimit: number | undefined): Lambda {
  if (session.text) return textLambdaForRun(session, session.text.lambda, previewLimit);
  return buildLambda(session.project.graph, session.query, {
    withFrom: true,
    ...(previewLimit !== undefined ? { previewLimit } : {}),
  });
}

/** A parameter's value as Pure, by its name. */
export interface ParameterSpec {
  readonly name: string;
  readonly value: ValueSpecification;
}

/** Each parameter's value as Pure: what a cube's source, one expression, is written with (cube.ts cubeSource) -- never
 *  a run's values, which are bound at run (runInput). */
export function parameterSpecs(session: Session, l: Lambda): ParameterSpec[] {
  const out: ParameterSpec[] = [];
  for (const p of l.parameters) {
    const v = session.paramValues.get(p.name);
    if (v !== undefined) out.push({ name: p.name, value: valueSpec(v) });
  }
  return out;
}

/** What a run sends: the lambda, and the values its parameters are bound to at run (plan-runner.ts checkParameters). */
export interface RunInput {
  readonly lambda: Lambda;
  readonly parameters: Readonly<Record<string, ParameterValue>>;
}

/**
 * The session's query as a run sends it, as upstream's builder sends it (legend-query-builder LambdaParameterState.ts,
 * buildExecutionQueryFromLambdaFunction, read at finos/legend-studio master 2026-11-17): a parameter of one value whose
 * value is a function (`today()`, `now()`, an adjusted date) is taken out of the lambda's parameters and set by a `let`
 * ahead of its body -- the plan computes it, on the database -- and every other value is bound at run, as written.
 * A parameter with no value is left to the run, which refuses a required one by name.
 */
export function runInput(session: Session, l: Lambda): RunInput {
  const lets: ValueSpecification[] = [];
  const kept: Variable[] = [];
  const parameters: Record<string, ParameterValue> = {};
  for (const p of l.parameters) {
    const v = session.paramValues.get(p.name);
    if (v?.kind === 'dateFunction' && p.multiplicity?.upperBound === 1) {
      lets.push(fn('letFunction', lit.string(p.name) as ValueSpecification, valueSpec(v)));
      continue;
    }
    kept.push(p);
    if (v !== undefined) parameters[p.name] = boundValue(p, v);
  }
  return { lambda: lets.length === 0 ? l : makeLambda(kept, ...lets, ...l.body), parameters };
}

/** A parameter's value as its run binds it: a literal's (an Integer and a Float as numbers, a Decimal as its digits, a
 *  date as its text, an enumeration's value as its name) or a list of them. A value that is not one -- a variable, or
 *  a function among a list's values -- is refused by name: a run's value is never computed here. */
function boundValue(p: Variable, v: Value): ParameterValue {
  const scalar = (x: Value): string | number | boolean => {
    switch (x.kind) {
      case 'string': case 'boolean': case 'strictDate': case 'dateTime': case 'decimal': return x.value;
      case 'enum': return x.value;
      case 'integer': {
        const n = Number(x.value);
        if (!Number.isSafeInteger(n)) throw new Error(`$${p.name}: the Integer ${x.value} is not a whole number within 2^53, which a run's value is`);
        return n;
      }
      case 'float': return Number(x.value);
      case 'dateFunction': throw new Error(`$${p.name}: a date function among a list's values is not a value a run binds; give the dates`);
      case 'list': throw new Error(`$${p.name}: a list inside a list is not a value a run binds`);
      case 'variable': throw new Error(`$${p.name}: its value is the variable $${x.name}, not a value a run binds; give the value`);
    }
  };
  if (v.kind === 'list') return v.values.map(scalar);
  if (v.kind === 'dateFunction') throw new Error(`$${p.name}: a date function is the value of a parameter of one value only`);
  return scalar(v);
}

/** Run the query for the results panel: `limit` rows shown, one more asked for. */
export async function run(app: AppContext, session: Session, limit: number): Promise<void> {
  const abort = new AbortController();
  const started = performance.now();
  let l: Lambda;
  try {
    l = executionLambda(session, limit + 1);
  } catch (e) {
    session.setRun({ status: 'error', message: (e as Error).message });
    return;
  }
  session.setRun({ status: 'running', started, abort });
  const hash = session.hash();
  try {
    const input = runInput(session, l);
    const result = await app.engine.run(session.project.context, input.lambda, input.parameters, abort.signal);
    session.setRun({ status: 'done', result, ms: Math.round(performance.now() - started), limit, queryHash: hash });
  } catch (e) {
    if ((e as Error).name === 'AbortError') session.setRun({ status: 'error', message: 'Stopped.' });
    else session.setRun({ status: 'error', message: (e as Error).message });
  }
}

/** The SQL the query runs: its plan's statement, planned and not run (a parameter's `?` where its value binds). */
export function sqlOf(app: AppContext, session: Session): Promise<string> {
  return app.engine.statement(session.project.context, runInput(session, executionLambda(session, undefined)).lambda);
}
