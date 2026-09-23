// What a calculated column may refer to, per stage.
//
// The scoping is the part with real consequences: a `groupDerived`
// expression that names a source column cannot compile, because by
// then the source rows are gone. Offering one would be a suggestion
// the planner refuses -- and the user has no way to know the editor
// was wrong rather than their expression.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  CALC_FUNCTIONS,
  CALC_OPERATORS,
  columnRef,
  columnsInScope,
  completionsFor,
  nameProblem,
} from '../src/calc.ts';
import { PIVOT_SEPARATOR } from '../src/generated/lite-facts.ts';
import { pureTypeOfArrow } from '../src/result.ts';
import { isNumericType } from '../src/snapshot.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const snap = (over: Partial<CubeSnapshot> = {}): CubeSnapshot => ({
  source: { expression: '#>{db.T}#' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
    { name: 'pnl', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
  ...over,
});

const labels = (cs: { label: string }[]) => cs.map((c) => c.label);

describe('what a calculated column can see', () => {
  it('offers SOURCE columns at the row stage', () => {
    const got = columnsInScope(snap(), 'row');
    assert.deepEqual(labels(got), ['region', 'notional', 'pnl']);
  });

  it('does NOT offer source columns at the group stage', () => {
    // The source rows are gone after grouping. This is the check that
    // matters: an expression naming `pnl` here does not compile, so
    // the editor must not suggest it.
    const s = snap({
      rows: ['region'],
      measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
    });
    const got = labels(columnsInScope(s, 'group'));
    assert.ok(!got.includes('pnl'), `offered a source column: ${got}`);
    assert.ok(!got.includes('notional'), `offered a source column: ${got}`);
    assert.deepEqual(got, ['region', 'total']);
  });

  it('offers the row dimensions and measures at the group stage', () => {
    const s = snap({
      rows: ['region', 'desk'],
      measures: [
        { name: 'total', column: 'notional', fn: 'sum' },
        { name: 'trades', column: 'pnl', fn: 'count' },
      ],
    });
    assert.deepEqual(labels(columnsInScope(s, 'group')),
      ['region', 'desk', 'total', 'trades']);
  });

  it('offers the PIVOT columns at the group stage, from the cast', () => {
    // From the snapshot's own cast rather than a rendered grid: the
    // snapshot is the source of truth and is available before
    // anything has been drawn.
    const s = snap({
      rows: ['region'],
      pivotOn: ['year'],
      measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
      pivotCast: [
        { name: `2023${PIVOT_SEPARATOR}total`, measure: 'total' },
        { name: `2024${PIVOT_SEPARATOR}total`, measure: 'total' },
      ],
    });
    const got = columnsInScope(s, 'group');
    assert.deepEqual(labels(got), [
      'region', 'total',
      `2023${PIVOT_SEPARATOR}total`, `2024${PIVOT_SEPARATOR}total`,
    ]);
    // ...and it says which measure it came from, since the name alone
    // is not obvious to read.
    const pivot = got.find((c) => c.label.startsWith('2023'));
    assert.match(pivot?.detail ?? '', /pivot column of total/);
  });

  it('never offers a pivot column at the ROW stage', () => {
    // A pivot's columns do not exist until after the pivot runs.
    const s = snap({
      pivotOn: ['year'],
      pivotCast: [{ name: `2023${PIVOT_SEPARATOR}total`, measure: 'total' }],
    });
    const got = labels(columnsInScope(s, 'row'));
    assert.ok(!got.some((l) => l.includes(PIVOT_SEPARATOR)),
      `offered a pivot column too early: ${got}`);
  });

  it('offers EARLIER calculated columns but not later ones', () => {
    // `extend` builds them in order, so a column can only see the ones
    // declared before it -- and never itself.
    const s = snap({
      derived: [
        { name: 'first', expression: '1' },
        { name: 'second', expression: '2' },
        { name: 'third', expression: '3' },
      ],
    });
    const got = labels(columnsInScope(s, 'row', 'second'));
    assert.ok(got.includes('first'), got.join(','));
    assert.ok(!got.includes('second'), `offered itself: ${got}`);
    assert.ok(!got.includes('third'), `offered a later column: ${got}`);
  });

  it('offers every calculated column when adding a NEW one', () => {
    const s = snap({
      derived: [{ name: 'first', expression: '1' },
        { name: 'second', expression: '2' }],
    });
    const got = labels(columnsInScope(s, 'row'));
    assert.ok(got.includes('first') && got.includes('second'), got.join(','));
  });
});

describe('spelling a column reference', () => {
  it('uses the plain form for an identifier', () => {
    assert.equal(columnRef('notional'), '$x.notional');
  });

  it('quotes a generated pivot name', () => {
    // `2023__|__total` starts with a digit and carries the separator,
    // so the bare form does not lex.
    assert.equal(columnRef(`2023${PIVOT_SEPARATOR}total`),
      `$x.'2023${PIVOT_SEPARATOR}total'`);
  });

  it('escapes a quote in a column name rather than ending the token', () => {
    assert.equal(columnRef("it's"), "$x.'it\\'s'");
    assert.equal(columnRef('back\\slash'), "$x.'back\\\\slash'");
  });
});

describe('the offered vocabulary', () => {
  it('offers columns, then functions, then operators', () => {
    const all = completionsFor(snap(), 'row');
    const kinds = [...new Set(all.map((c) => c.kind))];
    assert.deepEqual(kinds, ['column', 'function', 'operator']);
  });

  it('inserts a function as an arrow call, ready for its argument', () => {
    const fn = completionsFor(snap(), 'row')
      .find((c) => c.kind === 'function' && c.label === 'toUpper');
    assert.equal(fn?.insert, '->toUpper(');
  });

  it('every function carries an example, which is what proves it', () => {
    // `demo/verify-calc-vocabulary.mjs` compiles each one. An entry
    // with no example would be offered without ever being checked.
    for (const f of CALC_FUNCTIONS) {
      assert.ok(f.example.length > 0, `${f.name} has no example`);
      assert.ok(f.example.includes(f.name),
        `${f.name}'s example does not use it: ${f.example}`);
      assert.ok(f.signature.startsWith(f.name),
        `${f.name}'s signature does not name it: ${f.signature}`);
    }
  });

  it('has no duplicate function names', () => {
    const names = CALC_FUNCTIONS.map((f) => f.name);
    assert.equal(new Set(names).size, names.length);
  });

  it('offers the operators Pure spells differently from SQL', () => {
    const ops = CALC_OPERATORS.map((o) => o.label);
    for (const op of ['==', '!=', '&&', '||']) {
      assert.ok(ops.includes(op), `${op} is missing: ${ops.join(' ')}`);
    }
  });
});

describe('naming a calculated column', () => {
  it('refuses an empty name', () => {
    assert.match(nameProblem(snap(), 'row', '  ') ?? '', /needs a name/);
  });

  it('refuses a name that collides with any existing column', () => {
    const s = snap({
      measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
      derived: [{ name: 'uplift', expression: '1' }],
    });
    for (const taken of ['region', 'total', 'uplift']) {
      assert.match(nameProblem(s, 'row', taken) ?? '',
        /already a column/, taken);
    }
  });

  it('allows a name to keep itself when editing', () => {
    const s = snap({ derived: [{ name: 'uplift', expression: '1' }] });
    assert.equal(nameProblem(s, 'row', 'uplift', 'uplift'), null);
  });

  it('refuses a name carrying the pivot separator', () => {
    // It would be indistinguishable from a generated pivot column, and
    // the grid's header split would tear it in two.
    assert.match(
      nameProblem(snap(), 'row', `a${PIVOT_SEPARATOR}b`) ?? '',
      /pivot/,
    );
  });

  it('accepts an ordinary new name', () => {
    assert.equal(nameProblem(snap(), 'row', 'margin'), null);
  });
});

describe('an Arrow type name as a Pure type', () => {
  it('reads every name duckdb-wasm actually produces', () => {
    // MEASURED, not guessed. These are the exact strings
    // `String(field.type)` returns from a duckdb-wasm result --
    // captured by querying one column of each type -- because the
    // first version of this matched letters ONLY, stopped at the digit
    // in `Utf8`, and reported every string column as 'Unknown'.
    const measured: Record<string, string> = {
      Utf8: 'String',
      Int32: 'Integer',
      Int64: 'Integer',
      Float64: 'Float',
      'Decimal[9e+2]': 'Decimal',
      Bool: 'Boolean',
      'Date32<DAY>': 'StrictDate',
      'Timestamp<MICROSECOND>': 'DateTime',
    };
    for (const [arrow, pure] of Object.entries(measured)) {
      assert.equal(pureTypeOfArrow(arrow), pure, arrow);
    }
  });

  it('answers a Pure type name, never an Arrow one', () => {
    // The contract on ResultColumn.type: a caller testing
    // `isNumericType` has to get a name that vocabulary knows.
    for (const arrow of ['Utf8', 'Int64', 'Float64', 'Decimal[9e+2]']) {
      const got = pureTypeOfArrow(arrow);
      assert.ok(!/[0-9]/.test(got), `${arrow} -> ${got} kept a width`);
      assert.ok(isNumericType(got) || got === 'String',
        `${arrow} -> ${got} is neither numeric nor String`);
    }
  });

  it('says Unknown rather than guessing String', () => {
    // A column whose type could not be read must not silently become
    // groupable text.
    assert.equal(pureTypeOfArrow('SomethingNew'), 'Unknown');
    assert.equal(pureTypeOfArrow(''), 'Unknown');
    assert.equal(isNumericType(pureTypeOfArrow('SomethingNew')), false);
  });
});
