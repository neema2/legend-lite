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
import { PIVOT_SEPARATOR } from '../../engine-client/src/generated/lite-facts.ts';
import { CALC_FACTS } from '../src/generated/offer-facts.ts';
import { pivotColumns } from '../src/query.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { accessor } from '../../pure-protocol/src/index.ts';
import { stand } from './fake-planner.ts';

const snap = (over: Partial<CubeSnapshot> = {}): CubeSnapshot => ({
  source: { query: accessor('db', 'T') },
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

  it('offers the PIVOT columns at the group stage, from the plan', () => {
    // From the view's plan (the columns the values query made), not a
    // rendered grid and not a name parsed apart.
    const s = snap({
      rows: ['region'],
      pivotOn: ['year'],
      measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
    });
    const got = columnsInScope(s, 'group', undefined,
      pivotColumns(s, { tuples: [['2023'], ['2024']] }));
    // NOT the measure's own name: the pivot spreads it into its
    // generated columns, and the planner refuses it at this stage
    // ("relation has no column 'notional'", the product's compile on
    // the pivoted demo cube, 2026-09-26).
    assert.deepEqual(labels(got), [
      'region',
      `2023${PIVOT_SEPARATOR}total`, `2024${PIVOT_SEPARATOR}total`,
    ]);
    // ...and it says which measure it came from, since the name alone
    // is not obvious to read.
    const pivot = got.find((c) => c.label.startsWith('2023'));
    assert.match(pivot?.detail ?? '', /pivot column of total/);
  });

  it('never offers a pivot column at the ROW stage', () => {
    // A pivot's columns do not exist until after the pivot runs.
    const s = snap({ pivotOn: ['year'], measures: [{ name: 'total', column: 'notional', fn: 'sum' }] });
    const got = labels(columnsInScope(s, 'row', undefined,
      pivotColumns(s, { tuples: [['2023']] })));
    assert.ok(!got.some((l) => l.includes(PIVOT_SEPARATOR)),
      `offered a pivot column too early: ${got}`);
  });

  it('offers EARLIER calculated columns but not later ones', () => {
    // `extend` builds them in order, so a column can only see the ones
    // declared before it -- and never itself.
    const s = snap({
      derived: [
        { name: 'first', lambda: stand('1') },
        { name: 'second', lambda: stand('2') },
        { name: 'third', lambda: stand('3') },
      ],
    });
    const got = labels(columnsInScope(s, 'row', 'second'));
    assert.ok(got.includes('first'), got.join(','));
    assert.ok(!got.includes('second'), `offered itself: ${got}`);
    assert.ok(!got.includes('third'), `offered a later column: ${got}`);
  });

  it('offers every calculated column when adding a NEW one', () => {
    const s = snap({
      derived: [{ name: 'first', lambda: stand('1') },
        { name: 'second', lambda: stand('2') }],
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
    // The build compiles each one (tools/offer-facts). An entry with no
    // example would be offered without ever being checked.
    for (const f of CALC_FUNCTIONS) {
      assert.ok(f.example.length > 0, `${f.name} has no example`);
      assert.ok(f.example.includes(f.name),
        `${f.name}'s example does not use it: ${f.example}`);
    }
  });

  it('shows what the compiler declares, not a copy', () => {
    // CALC_FACTS: the function the compiler resolves the name to, and each overload
    for (const f of CALC_FUNCTIONS) {
      const fact = CALC_FACTS[f.name];
      assert.ok(fact, `${f.name} has no compiler facts`);
      assert.ok(fact.signatures.every((s) => s.startsWith(`${f.name}(`) || s.startsWith(`${f.name}<`)),
        `${f.name}: ${fact.signatures.join(', ')}`);
    }
    const upper = completionsFor(snap(), 'row').find((c) => c.label === 'toUpper');
    assert.equal(upper?.detail, 'toUpper(source:String[1]):String[1]');
    // several overloads, all shown: a number's abs keeps its own type
    const abs = completionsFor(snap(), 'row').find((c) => c.label === 'abs');
    assert.match(abs?.detail ?? '', /abs\(int:Integer\[1\]\):Integer\[1\]/);
    assert.match(abs?.detail ?? '', /abs\(float:Float\[1\]\):Float\[1\]/);
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
      derived: [{ name: 'uplift', lambda: stand('1') }],
    });
    for (const taken of ['region', 'total', 'uplift']) {
      assert.match(nameProblem(s, 'row', taken) ?? '',
        /already a column/, taken);
    }
  });

  it('allows a name to keep itself when editing', () => {
    const s = snap({ derived: [{ name: 'uplift', lambda: stand('1') }] });
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
