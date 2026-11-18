// Run's parameter values (the edge program's H3, 2026-11-17): each value the person typed, parsed as Pure, read from its
// literal into the value its run binds (plan-runner.ts checkParameters checks it against the plan's declaration) --
// a literal, a list of them, a value of the parameter's own enumeration, a signed number; anything else refused by name.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ExactNumber, fn, type ValueSpecification, type Variable } from '../../pure-protocol/src/index.ts';
import { literalValue } from '../src/backend/run.ts';

const parameter = (name: string, type: string): Variable => ({
  _type: 'var', name,
  genericType: { rawType: { _type: 'packageableType', fullPath: type }, typeArguments: [], multiplicityArguments: [], typeVariableValues: [] },
  multiplicity: { lowerBound: 1, upperBound: 1 },
} as Variable);

const read = (type: string, node: ValueSpecification): unknown => literalValue(parameter('p', type), 'the text', node);

describe("Run's parameter values, read from the literal the person wrote", () => {
  it('reads each literal as its value', () => {
    assert.equal(read('String', { _type: 'string', value: 'GB' }), 'GB');
    assert.equal(read('Boolean', { _type: 'boolean', value: true }), true);
    assert.equal(read('Integer', { _type: 'integer', value: 42 }), 42);
    assert.equal(read('Integer', { _type: 'integer', value: ExactNumber.of('42') }), 42);
    assert.equal(read('Float', { _type: 'float', value: ExactNumber.of('4.5') }), 4.5);
    assert.equal(read('Decimal', { _type: 'decimal', value: ExactNumber.of('12345678901234567890.25') }), '12345678901234567890.25');
    assert.equal(read('StrictDate', { _type: 'strictDate', value: '2024-06-01' }), '2024-06-01');
    assert.equal(read('DateTime', { _type: 'dateTime', value: '2024-06-01T09:30:00' }), '2024-06-01T09:30:00');
  });

  it('reads a list as its values, and a signed number as one literal', () => {
    assert.deepEqual(read('Integer', { _type: 'collection', multiplicity: { lowerBound: 2, upperBound: 2 },
      values: [{ _type: 'integer', value: 1 }, { _type: 'integer', value: 2 }] }), [1, 2]);
    assert.equal(read('Integer', fn('minus', { _type: 'integer', value: 5 })), -5);
    assert.equal(read('Decimal', fn('minus', { _type: 'decimal', value: ExactNumber.of('1.5') })), '-1.5');
  });

  it("reads a value of the parameter's own enumeration as its name, and refuses another's by name", () => {
    const red = { _type: 'property', property: 'RED', parameters: [{ _type: 'packageableElementPtr', fullPath: 'p::Colour' }] } as ValueSpecification;
    assert.equal(read('p::Colour', red), 'RED');
    assert.equal(read('p::Colour', { _type: 'enumValue', fullPath: 'p::Colour', value: 'RED' }), 'RED');
    assert.throws(() => read('p::Shade', red), /"the text" names a value of p::Colour, not of p::Shade/);
  });

  it('refuses by name what is not a literal; an Integer past 2^53 is read exactly, as a bigint (spec §4)', () => {
    assert.throws(() => read('StrictDate', fn('today')), /a value for p is a literal .*: "the text" is a call of today/);
    assert.throws(() => read('Integer', { _type: 'var', name: 'x' } as ValueSpecification), /"the text" is a var/);
    assert.throws(() => read('Integer', fn('minus', fn('today'))), /is a call of minus/);
    // lite's API v1 (2026-11-17): an Integer parameter is a safe number or a bigint, never refused for its size
    assert.equal(read('Integer', { _type: 'integer', value: ExactNumber.of('9007199254740993') }), 9007199254740993n);
    assert.equal(read('Integer', { _type: 'integer', value: ExactNumber.of('9007199254740991') }), 9007199254740991);
  });
});
