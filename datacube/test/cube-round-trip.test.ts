// ROUND TRIPS, AS PROPERTIES (docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md §11, part 2): a grid edited at random
// by the grid commands the screen and Python both build (grid-commands.ts; docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md),
// then written, read and opened again, must come back as it was -- and the state a grid keeps twice must agree with
// itself. Every bug the audits of neema2/legend-lite#47 found lived between pieces like these: the configuration
// against its copy folded into the query, a saved document against what reopens. Each command is also read back from
// its JSON (readGridCommand: what Python sends) as the same command.
//
// Seeded (mulberry32), so a failure names its seed and reproduces from it alone. A refused edit (CubeRefusal) is the
// screen's own refusal and is skipped; it must leave the draft as it was.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element } from '../../pure-protocol/src/index.ts';
import { chartProblems, defaultChart, followCube } from '../src/chart-spec.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { cubeToJson, frameSource, openCube, readCube, writeCube } from '../src/cube-document.ts';
import { fold, openedState, queryKey, stateKey, type CubeState } from '../src/cube-state.ts';
import { applyGridCommand } from '../src/grid-commands.ts';
import { allColumns, headerSorts, type GridDraft } from '../src/grid-edits.ts';
import { CubeRefusal, pivotTotalColumn, totalOrderSorts, type CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { CUBE, editedGrid, mulberry32, randomEdit, type Walked } from './grid-random.ts';

const SEEDS = 300;
const STEPS = 12;
const TREE = '__tree';

/** The grid as the screen's state owner holds it: its configuration folded into its query (cube-state.ts fold). */
function stateOf(d: GridDraft, tree = TreeState.empty()): CubeState {
  return fold({ snapshot: d.snapshot, configuration: d.config, tree });
}

/** A state written as the screen and Python write it, its text exact (cube-document.ts). */
function written(s: CubeState): string {
  return cubeToJson(writeCube({ name: 'round trip', source: frameSource('t', CUBE.columns), snapshot: s.snapshot,
    configuration: s.configuration, tree: s.tree }));
}

/** A document opened again over the same source, as the screen opens it: openCube, then the configuration read back
 *  from what the query carries (app.ts's constructor, headless/grid.ts opened). */
function reopened(text: string): { state: CubeState; notes: readonly string[]; changed: boolean } {
  const cube = openCube(readCube(text), { query: element('t') }, CUBE.columns);
  return { state: openedState(cube.snapshot, cube.configuration, cube.tree), notes: cube.notes, changed: cube.changed };
}

const why = (seed: number, steps: readonly string[]): string => `seed ${seed}: ${steps.join(' -> ')}`;
/** A state as a document keeps it: a group-level column's type is what a run learned, never written (V4), so it
 *  comes back with the next run, not with the document. */
const unlearned = (state: CubeState): CubeState => (state.snapshot.groupDerived === undefined ? state : {
  ...state, snapshot: { ...state.snapshot, groupDerived: state.snapshot.groupDerived.map(({ type: _learned, ...d }) => d) },
});

describe('a grid edited at random comes back as it was (written, read, opened again)', () => {
  it(`${SEEDS} seeds of ${STEPS} edits: the same queries, the same settings, the same document`, () => {
    let refused = 0;
    for (let seed = 1; seed <= SEEDS; seed += 1) {
      const { draft, steps, tree } = editedGrid(seed);
      refused += steps.filter((s) => s.endsWith('(refused)')).length;
      const state = stateOf(draft, tree);
      const text = written(state);
      const again = reopened(text);
      // a document it just wrote opens with nothing to say and nothing left out
      assert.deepEqual(again.notes, [], why(seed, steps));
      assert.equal(again.changed, false, why(seed, steps));
      // it runs the same queries (as content: a key's place in the text is the reader's)
      assert.deepEqual(JSON.parse(queryKey(again.state)), JSON.parse(queryKey(unlearned(state))), why(seed, steps));
      // nothing a person set is lost: presentation too (as content: a key's place in the text is the reader's)
      assert.deepEqual(JSON.parse(stateKey(again.state)), JSON.parse(stateKey(unlearned(state))), why(seed, steps));
      // and written again it is the same document, character for character
      assert.equal(written(again.state), text, why(seed, steps));
    }
    // most edits are made, so the trips are of grids edited (about one in eight is refused, 2026-10-18)
    assert.ok(refused < SEEDS * STEPS / 4, `${refused} of ${SEEDS * STEPS} edits refused`);
  });
});

describe('the state a grid keeps twice agrees with itself', () => {
  it('what a document carries only in its query survives the open path (#47\'s first blocker: openedState reads it back)', () => {
    // a document whose configuration says nothing -- an older writer's, or one that left it behind -- and whose query
    // carries the grid's settings: opened as the screen opens it, each setting the query carries is kept, never folded
    // away with a configuration that does not name it (what fromSnapshot reads back: each column's kind, pivot
    // exclusion, Aggregation and weight, the row limit, the tree's order, a pivot total and the pivot sorts)
    const carried = (s: CubeSnapshot): unknown => ({
      columns: [...s.columns, ...s.derived].map((c) => ({ name: c.name, kind: c.kind, aggregate: c.aggregate,
        aggregateWeight: c.aggregateWeight, excludedFromPivot: 'excludedFromPivot' in c ? c.excludedFromPivot : undefined })),
      maxRows: s.maxRows, treeColumnSort: s.treeColumnSort, pivotSort: s.pivotSort ?? {},
      ...(s.pivotTotal ? { pivotTotal: s.pivotTotal } : {}),
    });
    for (let seed = 1; seed <= SEEDS; seed += 1) {
      const { draft, steps } = editedGrid(seed);
      const state = stateOf(draft);
      const text = written({ ...state, configuration: DEFAULT_CONFIGURATION });
      const again = reopened(text);
      const want = carried(state.snapshot) as { pivotTotal?: unknown };
      const got = carried(again.state.snapshot) as { pivotTotal?: unknown };
      // a pivot total the query carries is read back; one it does not carry is the configuration's to decide
      if (want.pivotTotal === undefined) delete got.pivotTotal;
      assert.deepEqual(got, want, why(seed, steps));
    }
  });

  it('opened again over a source with a column more, it says so, hides it, and keeps everything else', () => {
    const wider = [...CUBE.columns, { name: 'book', type: 'String' }];
    for (let seed = 1; seed <= SEEDS; seed += 1) {
      const { draft, steps, tree } = editedGrid(seed);
      const state = stateOf(draft, tree);
      const cube = openCube(readCube(written(state)), { query: element('t') }, wider);
      assert.deepEqual(cube.notes, ['1 new column since this cube was saved, hidden: book'], why(seed, steps));
      const again = openedState(cube.snapshot, cube.configuration, cube.tree);
      assert.equal(again.configuration.columns['book']?.hidden, true, why(seed, steps));
      const { book: _book, ...columns } = again.configuration.columns;
      assert.deepEqual(JSON.parse(JSON.stringify({ ...again.configuration, columns })), JSON.parse(JSON.stringify(state.configuration)),
        why(seed, steps));
    }
  });

  it('the headers mark only sorts the query applies, each once (headerSorts within totalOrderSorts)', () => {
    for (let seed = 1; seed <= SEEDS; seed += 1) {
      const { draft, steps } = editedGrid(seed);
      const s = stateOf(draft).snapshot;
      const leaves = [TREE, ...allColumns(draft), pivotTotalColumn('notional')];
      const marks = headerSorts(s, leaves, TREE);
      const applied = totalOrderSorts(s);
      for (const m of marks) {
        const column = m.column === TREE ? s.rows[0] : m.column;
        assert.ok(applied.some((a) => a.column === column && a.direction === m.direction),
          `${why(seed, steps)}: ${m.column} ${m.direction} marked, the query orders ${JSON.stringify(applied)}`);
      }
      assert.equal(new Set(marks.map((m) => m.column)).size, marks.length, why(seed, steps));
    }
  });

  it('a live chart, followed to the grid as it is now, is where following leaves it (followCube a fixed point)', () => {
    let charted = 0;
    for (let seed = 1; seed <= SEEDS; seed += 1) {
      const rand = mulberry32(seed * 7919);
      // a chart made of the grid as it was a few edits ago
      const start = editedGrid(seed);
      const chart = defaultChart(stateOf(start.draft).snapshot);
      if (chart === null) continue;
      charted += 1;
      let d: Walked = { ...start.draft, tree: start.tree };
      for (let i = 0; i < 4; i += 1) {
        const [, edit] = randomEdit(rand, d);
        try {
          d = applyGridCommand(d, edit);
        } catch (e) {
          if (!(e instanceof CubeRefusal)) throw e;
        }
      }
      const s = stateOf(d).snapshot;
      const followed = followCube(chart, s);
      assert.deepEqual(followCube(followed, s), followed, `seed ${seed}: following again moved the chart`);
      // and judging it never throws: a chart the grid cannot draw is said in words
      assert.ok(Array.isArray(chartProblems(followed, s)));
    }
    assert.ok(charted > SEEDS / 4, `enough grids made a chart (${charted})`);
  });
});
