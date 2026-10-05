// ENGINE DEFECT S23 (docs/SEMANTICS_REGISTER.md): legend-engine types a BIT column TinyInt. A
// column is read Boolean only when engine's PRECISE type is TinyInt AND DataCube's own model
// declares it BIT -- never a real TINYINT column, never an aggregate over the column. The shapes
// below are legend-engine 4.145's own answers (runs/bool-probe.mjs, 2026-10-01).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { relationColumns, tdsColumns } from '../../engine-client/src/relation-type.ts';

const relation = (cols: Record<string, string>): unknown => ({
  columns: Object.entries(cols).map(([name, fullPath]) => ({ name, genericType: { rawType: { fullPath } } })),
});
const plan = (cols: Record<string, string>): unknown => ({
  rootExecutionNode: { resultType: { tdsColumns: Object.entries(cols).map(([name, type]) => ({ name, type })) } },
});
const PP = 'meta::pure::precisePrimitives::';

describe('a BIT column on legend-engine reads Boolean (ENGINE DEFECT S23)', () => {
  const bits = new Set(['settled']);

  it('the column itself, as lambdaRelationType and as a plan type it', () => {
    const cols = { settled: `${PP}TinyInt`, qty: `${PP}TinyInt`, id: `${PP}Int` };
    assert.deepEqual(relationColumns(relation(cols), bits).map((c) => c.type), ['Boolean', 'Integer', 'Integer']);
    assert.deepEqual(tdsColumns(plan(cols), bits).map((c) => c.type), ['Boolean', 'Integer', 'Integer']);
  });

  it('never an aggregate over it: a count or a sum named after the column is Integer', () => {
    assert.deepEqual(tdsColumns(plan({ settled: 'Integer' }), bits).map((c) => c.type), ['Integer']);
  });

  it('never without the model\'s word: TinyInt alone is an integer', () => {
    assert.deepEqual(relationColumns(relation({ settled: `${PP}TinyInt` })).map((c) => c.type), ['Integer']);
  });

  it('legend-lite types BIT Boolean itself', () => {
    assert.deepEqual(relationColumns(relation({ settled: 'Boolean' }), bits).map((c) => c.type), ['Boolean']);
  });
});
