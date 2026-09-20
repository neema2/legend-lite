// Randomised snapshots through the model layer.
//
// Not a replacement for the specific tests: a fuzzer finds SHAPES
// nobody thought to write down, and says nothing about whether the
// shape is right. So these assert INVARIANTS -- properties that
// must hold for every cube, however odd -- rather than expected
// strings.
//
// Seeded, so a failure is reproducible from its seed alone. The
// generator deliberately produces cubes a UI would refuse to build:
// zero columns, duplicate names, a measure over a missing column, a
// sort on something that is not there. The product's job is to
// produce a wrong-but-well-formed query or refuse, never to throw
// an unhandled error into a click handler.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { serialize } from '../src/serialize.ts';
import { buildColumnModel, PIVOT_SEPARATOR } from '../src/grid/columns.ts';
import {
  CubeRefusal,
  referencedColumns,
  totalOrderSorts,
  type AggregateFn,
  type CubeSnapshot,
  type FilterNode,
  type SortSpec,
} from '../src/snapshot.ts';
import type { ResultTable } from '../src/result.ts';

/** A tiny deterministic PRNG, so a seed reproduces a case exactly. */
function rng(seed: number): () => number {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    return s / 0x100000000;
  };
}

const NAMES = [
  'region',
  'desk',
  'book',
  'year',
  'qtr',
  'notional',
  'pnl',
  'qty',
  // Names chosen to break naive string handling.
  'a b',
  'Ünïcødé',
  "quote'name",
  'very_long_column_name_that_goes_on_and_on_and_on',
  'select',
  '1',
];
const TYPES = ['String', 'Integer', 'Float', 'Date', 'Boolean'];
const FNS: AggregateFn[] = ['sum', 'count', 'average', 'min', 'max', 'wavg'];

function makeSnapshot(r: () => number): CubeSnapshot {
  const pick = <T>(xs: readonly T[]): T =>
    xs[Math.floor(r() * xs.length)] as T;
  const upto = (n: number): number => Math.floor(r() * (n + 1));

  const columns = Array.from({ length: upto(6) }, () => ({
    name: pick(NAMES),
    type: pick(TYPES),
    ...(r() < 0.2 ? { kind: r() < 0.5 ? ('dimension' as const) : ('measure' as const) } : {}),
    ...(r() < 0.15 ? { excludedFromPivot: true } : {}),
  }));
  const names = columns.map((c) => c.name);
  const some = (n: number): string[] =>
    Array.from({ length: upto(n) }, () => pick(names.length ? names : NAMES));

  const measures = Array.from({ length: upto(3) }, (_, i) => {
    const fn = pick(FNS);
    return {
      name: `m${i}`,
      column: pick(names.length ? names : NAMES),
      fn,
      ...(fn === 'wavg' && r() < 0.7
        ? { weight: pick(names.length ? names : NAMES) }
        : {}),
    };
  });

  const sorts: SortSpec[] = Array.from({ length: upto(2) }, () => ({
    column: pick([...names, 'm0', 'nonexistent']),
    direction: r() < 0.5 ? ('asc' as const) : ('desc' as const),
  }));

  const filter: FilterNode | undefined =
    r() < 0.5
      ? undefined
      : {
          kind: 'and',
          children: [
            {
              kind: 'condition',
              column: pick(names.length ? names : NAMES),
              operator: 'equal',
              value: pick(['EMEA', '', "it's", 42, 0, true]) as never,
            },
            ...(r() < 0.5
              ? [
                  {
                    kind: 'not' as const,
                    child: {
                      kind: 'condition' as const,
                      column: pick(names.length ? names : NAMES),
                      operator: 'isEmpty' as const,
                    },
                  },
                ]
              : []),
          ],
        };

  return {
    source: { expression: 't' },
    columns,
    derived: r() < 0.3 ? [{ name: 'd0', expression: '$x.a + 1' }] : [],
    rows: some(3),
    pivotOn: some(2),
    measures,
    sorts,
    ...(filter ? { filter } : {}),
    ...(r() < 0.3 ? { maxRows: upto(1000) } : {}),
    epoch: 1,
  };
}

/** A result table shaped like something an engine might return. */
function tableFor(s: CubeSnapshot, r: () => number): ResultTable {
  const cols = referencedColumns(s).slice(0, 4);
  const values = [null, 'EMEA', '', 0, -1, 1e20, "it's", 'Ünïcødé'];
  const n = Math.floor(r() * 4);
  return {
    columns: cols.map((name) => ({
      name,
      type: 'String',
      values: Array.from({ length: n }, () => values[Math.floor(r() * values.length)] ?? null),
    })),
    rowCount: n,
    epoch: 1,
    elapsedMs: 0,
  };
}

const CASES = 400;

/**
 * Serialise, tolerating a declared refusal.
 *
 * The product is allowed to say "this cube is not a question" -- a
 * weighted average with no weight, a pivot with nothing to
 * aggregate. It is NOT allowed to throw anything else, and that is
 * the invariant worth fuzzing: a random cube must never produce an
 * error the caller cannot classify.
 */
function trySerialize(
  s: CubeSnapshot,
  scope?: Parameters<typeof serialize>[1],
  seed?: number,
): string | null {
  try {
    return serialize(s, scope);
  } catch (e) {
    if (e instanceof CubeRefusal) return null;
    assert.fail(
      `seed ${seed}: ${e instanceof Error ? `${e.name}: ${e.message}` : String(e)}`,
    );
  }
}

describe('fuzzing the model layer', () => {
  it('either serialises or REFUSES -- never anything else', () => {
    let refused = 0;
    for (let seed = 1; seed <= CASES; seed++) {
      const s = makeSnapshot(rng(seed));
      if (trySerialize(s, undefined, seed) === null) refused += 1;
      trySerialize(s, { level: 1, parent: ['x'], limit: 10 }, seed);
    }
    // The generator deliberately builds invalid cubes, so some must
    // refuse -- a run where nothing refused would mean the walls
    // stopped working, not that the cubes got better.
    assert.ok(refused > 0, 'no cube was refused; are the walls live?');
    assert.ok(
      refused < CASES,
      `every cube was refused (${refused}); the generator is broken`,
    );
  });

  it('never emits an empty or dangling pipeline stage', () => {
    // `a->->b` or a trailing `->` means some stage rendered nothing,
    // which the compiler reports far from the cause.
    for (let seed = 1; seed <= CASES; seed++) {
      const pure = trySerialize(makeSnapshot(rng(seed)), undefined, seed);
      if (pure === null) continue;
      assert.equal(pure.includes('->->'), false, `seed ${seed}: ${pure}`);
      assert.equal(pure.endsWith('->'), false, `seed ${seed}: ${pure}`);
      // `groupBy(~[], ~[m:...])` is the GRAND TOTAL and legitimate:
      // grouping by nothing. An empty AGGREGATE list is the broken
      // one, and that is what this bans.
      assert.equal(
        /,\s*~\[\]\)/.test(pure),
        false,
        `seed ${seed}: empty aggregate list in ${pure}`,
      );
    }
  });

  it('balances brackets in every generated query', () => {
    for (let seed = 1; seed <= CASES; seed++) {
      const pure = trySerialize(makeSnapshot(rng(seed)), undefined, seed);
      if (pure === null) continue;
      for (const [open, close] of [
        ['(', ')'],
        ['[', ']'],
      ] as const) {
        const a: number = pure.split(open).length;
        const b: number = pure.split(close).length;
        assert.equal(a, b, `seed ${seed} unbalanced ${open}: ${pure}`);
      }
    }
  });

  it('a level query never groups by more columns than the cube has', () => {
    for (let seed = 1; seed <= CASES; seed++) {
      const s = makeSnapshot(rng(seed));
      for (let level = 0; level <= s.rows.length + 1; level++) {
        const pure = trySerialize(s, { level, parent: [] }, seed);
        if (pure === null) continue;
        const m = /groupBy\(~\[([^\]]*)\]/.exec(pure);
        if (m && m[1]) {
          const n = m[1].split(',').filter((x) => x.trim()).length;
          assert.ok(
            n <= s.rows.length,
            `seed ${seed} level ${level}: grouped ${n} of ${s.rows.length}`,
          );
        }
      }
    }
  });

  it('builds a column model whose leaves match the table', () => {
    for (let seed = 1; seed <= CASES; seed++) {
      const r = rng(seed);
      const s = makeSnapshot(r);
      const table = tableFor(s, r);
      let model;
      try {
        model = buildColumnModel(
          table,
          s.rows,
          s.measures.map((m) => m.name),
        );
      } catch (e) {
        assert.fail(
          `seed ${seed} column model threw: ${
            e instanceof Error ? e.message : String(e)
          }`,
        );
      }
      assert.equal(
        model.leaves.length,
        table.columns.length,
        `seed ${seed}: ${model.leaves.length} leaves for ${table.columns.length} columns`,
      );
      // Every leaf must point at a real column of the table.
      for (const leaf of model.leaves) {
        assert.ok(
          leaf.index >= 0 && leaf.index < table.columns.length,
          `seed ${seed}: leaf index ${leaf.index} out of range`,
        );
      }
      // The header must cover exactly the leaves, at every level.
      for (const row of model.headerRows) {
        for (const cell of row) {
          assert.ok(cell.colStart >= 0, `seed ${seed}: negative colStart`);
          assert.ok(
            cell.colStart + cell.colSpan <= model.leaves.length,
            `seed ${seed}: header cell overruns the leaves`,
          );
          assert.ok(cell.colSpan >= 1 && cell.rowSpan >= 1, `seed ${seed}`);
        }
      }
    }
  });

  it('gives every leaf a unique name, however odd the inputs', () => {
    // Two columns with the same name make the grid render one twice
    // and lose the other -- and duplicate names are exactly what a
    // pivot over a dimension containing the measure's name produces.
    for (let seed = 1; seed <= CASES; seed++) {
      const r = rng(seed);
      const s = makeSnapshot(r);
      const table = tableFor(s, r);
      const model = buildColumnModel(
        table,
        s.rows,
        s.measures.map((m) => m.name),
      );
      const seen = new Set(model.leaves.map((l) => l.index));
      assert.equal(
        seen.size,
        model.leaves.length,
        `seed ${seed}: two leaves share a source column`,
      );
    }
  });

  it('produces a total order for sorting, or none at all', () => {
    // A partial order means two runs of the same query can disagree
    // about row order, which is how pagination tears.
    for (let seed = 1; seed <= CASES; seed++) {
      const s = makeSnapshot(rng(seed));
      const sorts = totalOrderSorts(s, s.rows);
      const names = sorts.map((x) => x.column);
      assert.equal(
        new Set(names).size,
        names.length,
        `seed ${seed}: duplicate sort column in ${names.join(', ')}`,
      );
      if (s.rows.length > 0 && sorts.length > 0) {
        for (const dim of s.rows) {
          assert.ok(
            names.includes(dim),
            `seed ${seed}: ${dim} missing from the total order`,
          );
        }
      }
    }
  });

  it('quotes or escapes every name that needs it', () => {
    // A column called `a b`, `select` or `quote'name` must not be
    // pasted raw into the grammar.
    for (let seed = 1; seed <= CASES; seed++) {
      const s = makeSnapshot(rng(seed));
      const pure = trySerialize(s, undefined, seed);
      if (pure === null) continue;
      // A bare `a b` in a column list is the failure shape.
      // Strip quoted runs, honouring backslash escapes -- a name
      // like `quote'name` renders as 'quote\'name' and a naive
      // strip splits it in half and finds phantoms.
      const bare = pure.replace(/'(?:\\.|[^'\\])*'/g, '');
      assert.equal(
        /~\[[^\]]*\ba b\b[^\]]*\]/.test(bare),
        false,
        `seed ${seed}: unquoted spaced identifier in ${pure}`,
      );
    }
  });

  it('never loses the pivot separator into a column name', () => {
    for (let seed = 1; seed <= CASES; seed++) {
      const s = makeSnapshot(rng(seed));
      if (s.columns.some((c) => c.name.includes(PIVOT_SEPARATOR))) continue;
      const pure = trySerialize(s, undefined, seed);
      if (pure === null) continue;
      assert.equal(
        pure.includes(PIVOT_SEPARATOR),
        false,
        `seed ${seed}: separator leaked into ${pure}`,
      );
    }
  });
});
