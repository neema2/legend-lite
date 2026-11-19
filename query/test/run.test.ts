// A run's input (the edge program's H3, 2026-11-17): the session's query as a run sends it to lite's engine -- a
// parameter whose value is a function set by a `let` ahead of the body, as upstream's builder sends it
// (legend-query-builder LambdaParameterState.ts, buildExecutionQueryFromLambdaFunction), and every other value bound at
// run as its literal's value (plan-runner.ts checkParameters checks it against the plan's declaration).

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';

import { fn, lambda, toJson, type Lambda, type Variable } from '../../pure-protocol/src/index.ts';
import { runInput } from '../src/app/run.ts';
import { Session } from '../src/app/session.ts';
import type { LoadedProject } from '../src/app/context.ts';
import { emptyQuery } from '../src/builder/state.ts';

/** A parameter as a lambda declares it: its type and its multiplicity's bounds. */
const parameter = (name: string, type: string, lower = 1, upper: number | '*' = 1): Variable => ({
  _type: 'var', name,
  genericType: { rawType: { _type: 'packageableType', fullPath: type }, typeArguments: [], multiplicityArguments: [], typeVariableValues: [] },
  multiplicity: upper === '*' ? { lowerBound: lower } : { lowerBound: lower, upperBound: upper },
} as Variable);

/** A session over no project's model: a run's input reads only its parameters' values. */
function session(): Session {
  const project = { config: undefined, gav: 'g:a:v', context: { _type: 'text', code: '' }, graph: undefined } as unknown as LoadedProject;
  return new Session(project, emptyQuery({ kind: 'class', class: 'p::C', mapping: 'p::M', runtime: 'p::R' } as never));
}

const query = (...parameters: Variable[]): Lambda => ({ ...lambda([], fn('getAll')), parameters } as Lambda);

describe("a run's input: the lambda, and its parameters' values bound at run", () => {
  it('binds each literal as its value: numbers, a decimal\'s digits, dates as text, an enumeration\'s name, a list', () => {
    const s = session();
    s.setParam('n', { kind: 'integer', value: '42' });
    s.setParam('f', { kind: 'float', value: '4.5' });
    s.setParam('d', { kind: 'decimal', value: '1.25' });
    s.setParam('b', { kind: 'boolean', value: true });
    s.setParam('t', { kind: 'string', value: 'GB' });
    s.setParam('day', { kind: 'strictDate', value: '2024-06-01' });
    s.setParam('at', { kind: 'dateTime', value: '2024-06-01T09:30:00' });
    s.setParam('c', { kind: 'enum', enumeration: 'p::Colour', value: 'RED' });
    s.setParam('ids', { kind: 'list', values: [{ kind: 'integer', value: '1' }, { kind: 'integer', value: '2' }] });
    const l = query(parameter('n', 'Integer'), parameter('f', 'Float'), parameter('d', 'Decimal'), parameter('b', 'Boolean'),
      parameter('t', 'String'), parameter('day', 'StrictDate'), parameter('at', 'DateTime'), parameter('c', 'p::Colour'),
      parameter('ids', 'Integer', 0, '*'));
    const input = runInput(s, l);
    assert.equal(input.lambda, l, 'no function value: the lambda as it was');
    assert.deepEqual(input.parameters, {
      n: 42, f: 4.5, d: '1.25', b: true, t: 'GB', day: '2024-06-01', at: '2024-06-01T09:30:00', c: 'RED', ids: [1, 2],
    });
  });

  it("sets a one-valued parameter whose value is a function by a let ahead of the body, as upstream's builder does", () => {
    const s = session();
    s.setParam('asOf', { kind: 'dateFunction', function: { kind: 'today' } });
    s.setParam('n', { kind: 'integer', value: '7' });
    const l = query(parameter('asOf', 'StrictDate'), parameter('n', 'Integer'));
    const input = runInput(s, l);
    // the function's parameter is gone from the lambda's, its let first; the literal's is bound at run
    assert.deepEqual(input.lambda.parameters.map((p) => p.name), ['n']);
    assert.equal(toJson(input.lambda.body[0]), toJson(fn('letFunction', { _type: 'string', value: 'asOf' }, fn('today'))));
    assert.equal(toJson(input.lambda.body.at(-1)), toJson(fn('getAll')));
    assert.deepEqual(input.parameters, { n: 7 });
  });

  it('leaves a parameter with no value to the run, which refuses a required one by name', () => {
    const input = runInput(session(), query(parameter('n', 'Integer')));
    assert.deepEqual(input.parameters, {});
    assert.deepEqual(input.lambda.parameters.map((p) => p.name), ['n']);
  });

  it('refuses by name a value a run does not bind: a variable, an Integer beyond 2^53, a function among a list', () => {
    const s = session();
    s.setParam('n', { kind: 'variable', name: 'limit' });
    assert.throws(() => runInput(s, query(parameter('n', 'Integer'))), /\$n: its value is the variable \$limit/);
    s.setParam('n', { kind: 'integer', value: '9007199254740993' });
    assert.throws(() => runInput(s, query(parameter('n', 'Integer'))), /the Integer 9007199254740993 is not a whole number within 2\^53/);
    s.setParam('days', { kind: 'list', values: [{ kind: 'dateFunction', function: { kind: 'today' } }] });
    assert.throws(() => runInput(s, query(parameter('days', 'StrictDate', 0, '*'))), /a date function among a list's values/);
    s.setParam('asOf', { kind: 'dateFunction', function: { kind: 'now' } });
    assert.throws(() => runInput(s, query(parameter('asOf', 'DateTime', 0, '*'))),
      /a date function is the value of a parameter of one value only/);
  });
});
