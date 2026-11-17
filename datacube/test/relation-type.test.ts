// ENGINE DEFECT S23 (docs/SEMANTICS_REGISTER.md): legend-engine types a BIT column TinyInt. The legend-engine client
// (planner.ts, UpstreamPlanner: the edge audit's H10, 2026-11-17 -- until then the product's relation-type.ts) reads a
// column Boolean only when engine's PRECISE type is TinyInt AND the model's Database declares it BIT -- never a real
// TINYINT column, never an aggregate over the column. The shapes below are legend-engine 4.145's own answers
// (runs/bool-probe.mjs, 2026-10-01). The product's reader knows no TinyInt rule: legend-lite types BIT Boolean itself.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { relationColumns, tdsColumns } from '../../engine-client/src/relation-type.ts';
import { bitColumnsOf, carriesTinyInt, engineBooleans } from '../src/engine-booleans.ts';

const relation = (cols: Record<string, string>): unknown => ({
  columns: Object.entries(cols).map(([name, fullPath]) => ({ name, genericType: { rawType: { fullPath } } })),
});
const plan = (cols: Record<string, string>): unknown => ({
  rootExecutionNode: { resultType: { tdsColumns: Object.entries(cols).map(([name, type]) => ({ name, type })) } },
});
const PP = 'meta::pure::precisePrimitives::';

describe("a BIT column on legend-engine reads Boolean, by the legend-engine client (ENGINE DEFECT S23)", () => {
  const bits = new Set(['settled']);

  it('the column itself, as lambdaRelationType and as a plan type it', () => {
    const cols = { settled: `${PP}TinyInt`, qty: `${PP}TinyInt`, id: `${PP}Int` };
    assert.deepEqual(relationColumns(engineBooleans(relation(cols), bits)).map((c) => c.type), ['Boolean', 'Integer', 'Integer']);
    assert.deepEqual(tdsColumns(engineBooleans(plan(cols), bits)).map((c) => c.type), ['Boolean', 'Integer', 'Integer']);
  });

  it('never an aggregate over it: a count or a sum named after the column is Integer', () => {
    assert.deepEqual(tdsColumns(engineBooleans(plan({ settled: 'Integer' }), bits)).map((c) => c.type), ['Integer']);
  });

  it("never without the model's word: TinyInt alone is an integer", () => {
    assert.deepEqual(relationColumns(engineBooleans(relation({ settled: `${PP}TinyInt` }), new Set())).map((c) => c.type), ['Integer']);
  });

  it("the model's word is its Database's: a column declared BIT, in any schema and table", () => {
    const elements = [
      { _type: 'class', package: 'demo', name: 'Trade' },
      { _type: 'relational', package: 'demo', name: 'DB', schemas: [
        { name: 'default', tables: [{ name: 't', columns: [{ name: 'settled', type: { _type: 'Bit' } }, { name: 'qty', type: { _type: 'TinyInt' } }] }] },
        { name: 'other', tables: [{ name: 'u', columns: [{ name: 'live', type: { _type: 'Bit' } }] }] }] },
    ];
    assert.deepEqual([...bitColumnsOf(elements)].sort(), ['live', 'settled']);
  });

  it('only an answer that carries TinyInt asks for the model at all', () => {
    assert.equal(carriesTinyInt(relation({ id: `${PP}Int`, settled: 'Boolean' })), false);
    assert.equal(carriesTinyInt(plan({ settled: `${PP}TinyInt` })), true);
  });
});

describe("the product's reader knows no TinyInt rule", () => {
  it('legend-lite types BIT Boolean itself; a TinyInt it reads is an integer', () => {
    assert.deepEqual(relationColumns(relation({ settled: 'Boolean' })).map((c) => c.type), ['Boolean']);
    assert.deepEqual(relationColumns(relation({ settled: `${PP}TinyInt` })).map((c) => c.type), ['Integer']);
  });
});
