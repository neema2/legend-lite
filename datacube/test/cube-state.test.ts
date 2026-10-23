// The state owner (src/cube-state.ts): every rule of Leg B's design, proven against a query
// side whose answers the test hands out by hand -- so a slow change finishing after a newer
// one, a refusal on top of a pending change, an undo overtaken mid-flight are each one exact
// ordering, not a race.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { CubeView } from '../src/cube.ts';
import { STALE, type Stale } from '../src/epoch.ts';
import { CubeStateOwner, queryKey, stateKey, type CubeState, type OwnerEvent } from '../src/cube-state.ts';
import { StateOwner, UndoStack, type StateRules } from '../src/state-owner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { accessor } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: accessor('t::DB', 'T') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: [],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};
const INITIAL: CubeState = { snapshot: SNAPSHOT, configuration: DEFAULT_CONFIGURATION, tree: TreeState.empty() };

/** A query side that holds each run until the test answers it. */
class Engine {
  readonly runs: { state: CubeState; answer: (v: CubeView | Stale) => void; refuse: (e: unknown) => void }[] = [];
  readonly run = (state: CubeState): Promise<CubeView | Stale> => new Promise((answer, refuse) => {
    this.runs.push({ state, answer, refuse });
  });
  /** Answer run `i` with a view of the state it ran (types added, as step 0 does). */
  land(i: number, source?: CubeSnapshot['source']): void {
    const run = this.runs[i] ?? assert.fail(`no run ${i}`);
    const snapshot: CubeSnapshot = {
      ...run.state.snapshot,
      columns: run.state.snapshot.columns.map((c) => ({ ...c, typed: true } as typeof c)),
      ...(source ? { source } : {}),
    };
    run.answer({ snapshot } as unknown as CubeView);
  }
  refuse(i: number, why = 'refused'): void {
    (this.runs[i] ?? assert.fail(`no run ${i}`)).refuse(new Error(why));
  }
  stale(i: number): void {
    (this.runs[i] ?? assert.fail(`no run ${i}`)).answer(STALE);
  }
}

function owner(): { o: CubeStateOwner; e: Engine; events: OwnerEvent['kind'][]; aborts: number[] } {
  const e = new Engine();
  const aborts: number[] = [];
  const o = new CubeStateOwner(INITIAL, e.run, { abort: () => aborts.push(1) });
  const events: OwnerEvent['kind'][] = [];
  o.subscribe((ev) => events.push(ev.kind));
  return { o, e, events, aborts };
}

const tick = () => new Promise((r) => setTimeout(r, 0));
const group = (rows: string[]) => (s: CubeState): CubeState => ({ ...s, snapshot: { ...s.snapshot, rows } });
const colour = (c: string) => (s: CubeState): CubeState =>
  ({ ...s, configuration: { ...s.configuration, reportTitle: c } });

async function landed(o: CubeStateOwner, e: Engine): Promise<void> {
  const first = o.refresh();
  e.land(e.runs.length - 1);
  await first;
}

describe('the two kinds of change', () => {
  it('a presentation change commits at once, runs no query, and is one undo step', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const runs = e.runs.length;
    const out = await o.change(colour('red'));
    assert.equal(out.kind, 'applied');
    assert.equal(e.runs.length, runs, 'no query');
    assert.equal(o.committed.configuration.reportTitle, 'red');
    assert.equal(o.rendered.configuration.reportTitle, 'red', 'on screen at once');
    assert.equal(events.at(-1), 'presentation');
    assert.equal(o.canUndo, true);
  });

  it('a change that changes nothing is nothing: no query, no undo step', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const runs = e.runs.length;
    const heard = events.length;
    assert.equal((await o.change((s) => ({ ...s, configuration: { ...s.configuration } }))).kind, 'nothing');
    assert.equal((await o.change(group([]))).kind, 'nothing');
    assert.equal(e.runs.length, runs);
    assert.equal(events.length, heard, 'nobody told');
    assert.equal(o.canUndo, false);
  });

  it('a setting that folds into the query (the row cap) is a query change', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const runs = e.runs.length;
    const pending = o.change((s) => ({ ...s, configuration: { ...s.configuration, maxRows: 10 } }));
    assert.equal(e.runs.length, runs + 1, 'a query');
    e.land(runs);
    assert.equal((await pending).kind, 'applied');
    assert.equal(o.committed.snapshot.maxRows, 10, 'folded into the snapshot');
  });

  it('opening a group is a query change; the key sees closed groups and the expand level', () => {
    const a: CubeState = { ...INITIAL, tree: TreeState.empty().withExpandTo(1) };
    const b: CubeState = { ...a, tree: a.tree.setOpen(['EMEA'], false) };
    assert.notEqual(queryKey(a), queryKey(b), 'a group closed from the expand level is a different tree (P2-109)');
    assert.equal(a.tree.setOpen(['EMEA'], true), a.tree, 'asking for what already is changes nothing (P2-127)');
  });
});

describe('a query change is one transaction', () => {
  it('commits what the engine RETURNED, with the source that was SENT', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const out = o.change(group(['region']), { label: 'group by region' });
    assert.deepEqual(o.current.snapshot.rows, ['region'], 'painted as pending');
    assert.deepEqual(o.committed.snapshot.rows, [], 'not committed yet');
    assert.equal(o.busy, true);
    e.land(1, { query: accessor('snap::DB', 'SNAP') });
    assert.equal((await out).kind, 'applied');
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
    assert.equal((o.committed.snapshot.columns[0] as { typed?: boolean }).typed, true, 'the engine\'s types');
    assert.deepEqual(o.committed.snapshot.source, SNAPSHOT.source, 'a snap\'s table never enters the state');
    assert.equal(o.busy, false);
    assert.deepEqual(events.slice(-2), ['pending', 'committed']);
  });

  it('a refusal leaves committed, rendered and the history as they were', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const before = o.committed;
    const out = o.change(group(['region']), { label: 'group by region' });
    e.refuse(1);
    const got = await out;
    assert.equal(got.kind, 'refused');
    assert.deepEqual(got.kind === 'refused' ? got.reverted : [], ['group by region']);
    assert.equal(o.committed, before);
    assert.equal(o.current, before, 'nothing pending: the screen repaints from committed');
    assert.equal(o.canUndo, false, 'no undo step for a change that never landed');
    assert.equal(events.at(-1), 'refused');
  });

  it('a refused change can be applied again as it was asked, undo step and all', async () => {
    // Signing in again after an expired session: the sort that failed is sorted, not dropped.
    const { o, e } = owner();
    await landed(o, e);
    const out = o.change(group(['region']), { label: 'group by region' });
    e.refuse(1, 'the warehouse session for rita has expired');
    assert.equal((await out).kind, 'refused');
    const again = o.retryRefused();
    assert.deepEqual(e.runs[2]?.state.snapshot.rows, ['region'], 'the refused state runs again');
    e.land(2);
    assert.equal((await again).kind, 'applied');
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
    assert.equal(o.canUndo, true, 'it lands as the undo step it would have been');
  });

  it('with nothing refused since the last change began, a retry re-runs what is committed', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const out = o.change(group(['region']), { label: 'A' });
    e.refuse(1);
    await out;
    const later = o.change(group(['desk']), { label: 'B' });
    e.land(2);
    await later;
    const again = o.retryRefused();
    assert.deepEqual(e.runs[3]?.state.snapshot.rows, ['desk'], 'the old refusal is not resurrected');
    e.land(3);
    await again;
  });

  it('latest wins: an older change landing late is dropped', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']), { label: 'A' });
    const b = o.change(group(['desk']), { label: 'B' });
    e.land(2);
    assert.equal((await b).kind, 'applied');
    e.land(1);
    assert.equal((await a).kind, 'superseded');
    assert.deepEqual(o.committed.snapshot.rows, ['desk'], 'the late answer did not win');
    assert.equal(o.historyDepth.past, 1, 'one step: back to before both');
  });

  it('a change made on a pending one, then refused, reverts BOTH and names them', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']), { label: 'group by region' });
    const b = o.change((s) => ({ ...s, snapshot: { ...s.snapshot, pivotOn: ['desk'] } }), { label: 'pivot on desk' });
    assert.deepEqual(o.current.snapshot.rows, ['region'], 'B was built on A');
    e.refuse(2);
    const got = await b;
    assert.deepEqual(got.kind === 'refused' ? got.reverted : [], ['group by region', 'pivot on desk']);
    assert.deepEqual(o.committed.snapshot.rows, []);
    e.land(1);
    assert.equal((await a).kind, 'superseded', 'A cannot land after its own revert');
    assert.deepEqual(o.committed.snapshot.rows, []);
  });

  it('a refusal of an older change after a newer one started is not reported', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']), { label: 'A' });
    const b = o.change(group(['desk']), { label: 'B' });
    e.refuse(1);
    assert.equal((await a).kind, 'superseded');
    assert.equal(o.busy, true, 'B still in flight');
    e.land(2);
    assert.equal((await b).kind, 'applied');
  });

  it('a query side that reports the run superseded is superseded', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']));
    e.stale(1);
    assert.equal((await a).kind, 'superseded');
  });

  it('a presentation change while a query is in flight survives that query\'s refusal', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']), { label: 'A' });
    await o.change(colour('blue'));
    assert.equal(o.current.configuration.reportTitle, 'blue', 'on the pending state too');
    e.refuse(1);
    await a;
    assert.equal(o.committed.configuration.reportTitle, 'blue', 'the colour stays');
    assert.deepEqual(o.committed.snapshot.rows, []);
  });
});

describe('undo and redo', () => {
  it('step back and forward over landed states, presentation without a query', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']));
    e.land(1);
    await a;
    await o.change(colour('red'));
    const runs = e.runs.length;
    assert.equal((await o.undo()).kind, 'applied');
    assert.equal(e.runs.length, runs, 'undoing a colour runs no query');
    assert.equal(o.committed.configuration.reportTitle, undefined);
    const back = o.undo();
    e.land(e.runs.length - 1);
    assert.equal((await back).kind, 'applied');
    assert.deepEqual(o.committed.snapshot.rows, []);
    assert.deepEqual(o.historyDepth, { past: 0, future: 2 });
    const fwd = o.redo();
    e.land(e.runs.length - 1);
    await fwd;
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
  });

  it('an undo refused leaves both stacks as they were', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']));
    e.land(1);
    await a;
    const back = o.undo();
    e.refuse(e.runs.length - 1);
    assert.equal((await back).kind, 'refused');
    assert.deepEqual(o.historyDepth, { past: 1, future: 0 });
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
  });

  it('a change during an undo in flight: no mixed state, no redo pointing nowhere (P2-106)', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']));
    e.land(1);
    await a;
    const back = o.undo();
    const b = o.change((s) => ({ ...s, snapshot: { ...s.snapshot, sorts: [{ column: 'region', direction: 'desc' }] } }));
    e.land(3);
    await b;
    e.land(2);
    assert.equal((await back).kind, 'superseded', 'the undo lands only as part of the change made on it');
    assert.deepEqual(o.committed.snapshot.rows, [], 'the change was made on the undone state');
    assert.deepEqual(o.historyDepth, { past: 1, future: 0 }, 'one step back, to where the change was made; the redo is gone, as after any new change');
    const again = o.undo();
    e.land(e.runs.length - 1);
    await again;
    assert.deepEqual([o.committed.snapshot.rows, o.committed.snapshot.sorts], [[], []]);
  });

  it('undo while a change is in flight cancels THAT change, and nothing more', async () => {
    const { o, e, aborts } = owner();
    await landed(o, e);
    const a = o.change(group(['region']), { label: 'A' });
    assert.equal(o.canUndo, true);
    assert.equal((await o.undo()).kind, 'cancelled');
    assert.equal(aborts.length, 1, 'the query is stopped');
    assert.equal(o.busy, false);
    e.land(1);
    assert.equal((await a).kind, 'superseded', 'it cannot land after being cancelled');
    assert.deepEqual(o.committed.snapshot.rows, []);
    assert.equal(o.canUndo, false);
  });

  it('two quick undos are two steps, never a state that was not on the stack (P2-107)', async () => {
    const { o, e } = owner();
    await landed(o, e);
    for (const rows of [['region'], ['desk']]) {
      const c = o.change(group(rows));
      e.land(e.runs.length - 1);
      await c;
    }
    const u1 = o.undo();
    const u2 = o.undo();
    assert.deepEqual(o.current.snapshot.rows, [], 'the second press goes one step further');
    e.land(e.runs.length - 1);
    assert.equal((await u2).kind, 'applied');
    e.land(e.runs.length - 2);
    assert.equal((await u1).kind, 'superseded', 'the first press cannot land after the second');
    assert.deepEqual(o.committed.snapshot.rows, []);
    assert.deepEqual(o.historyDepth, { past: 0, future: 2 });
    for (const rows of [['region'], ['desk']]) {
      const r = o.redo();
      e.land(e.runs.length - 1);
      await r;
      assert.deepEqual(o.committed.snapshot.rows, rows, 'redo walks the passed-over steps back in order');
    }
  });

  it('two quick undos, the second refused: nothing moved, the screen is what landed', async () => {
    const { o, e } = owner();
    await landed(o, e);
    for (const rows of [['region'], ['desk']]) {
      const c = o.change(group(rows));
      e.land(e.runs.length - 1);
      await c;
    }
    const u1 = o.undo();
    const u2 = o.undo();
    e.refuse(e.runs.length - 1);
    assert.equal((await u2).kind, 'refused');
    e.land(e.runs.length - 2);
    assert.equal((await u1).kind, 'superseded');
    assert.deepEqual(o.committed.snapshot.rows, ['desk']);
    assert.deepEqual(o.historyDepth, { past: 2, future: 0 });
  });

  it('redo while an undo is in flight cancels the undo', async () => {
    const { o, e, aborts } = owner();
    await landed(o, e);
    const c = o.change(group(['region']));
    e.land(1);
    await c;
    void o.undo();
    assert.equal((await o.redo()).kind, 'cancelled');
    assert.equal(aborts.length, 1);
    assert.deepEqual(o.historyDepth, { past: 1, future: 0 });
  });

  it('a change made on an undo in flight lands as the undo AND the change, a step each', async () => {
    const { o, e } = owner();
    await landed(o, e);
    for (const rows of [['region'], ['desk']]) {
      const c = o.change(group(rows));
      e.land(e.runs.length - 1);
      await c;
    }
    void o.undo();
    const b = o.change((s) => ({ ...s, snapshot: { ...s.snapshot, pivotOn: ['desk'] } }), { label: 'pivot' });
    assert.deepEqual(o.current.snapshot.rows, ['region'], 'built on the undo');
    e.land(e.runs.length - 1);
    assert.equal((await b).kind, 'applied');
    assert.deepEqual(o.committed.snapshot.pivotOn, ['desk']);
    assert.deepEqual(o.historyDepth, { past: 2, future: 0 }, 'the undone step went, the change is a step, no redo');
    const back = o.undo();
    e.land(e.runs.length - 1);
    await back;
    assert.deepEqual([o.committed.snapshot.rows, o.committed.snapshot.pivotOn], [['region'], []], 'undo goes to the state the change was made on');
  });

  it('a change made on an undo in flight, refused, reverts both and says so', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const c = o.change(group(['region']));
    e.land(1);
    await c;
    void o.undo();
    const b = o.change((s) => ({ ...s, snapshot: { ...s.snapshot, pivotOn: ['desk'] } }), { label: 'pivot' });
    e.refuse(e.runs.length - 1);
    const got = await b;
    assert.deepEqual(got.kind === 'refused' ? got.reverted : [], ['undo', 'pivot']);
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
    assert.deepEqual(o.historyDepth, { past: 1, future: 0 });
  });

  it('a presentation change made while an undo runs rides the undo, and is kept on both sides', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const c = o.change(group(['region']));
    e.land(1);
    await c;
    const back = o.undo();
    await o.change(colour('green'));
    e.land(e.runs.length - 1);
    await back;
    assert.deepEqual(o.committed.snapshot.rows, []);
    assert.equal(o.committed.configuration.reportTitle, 'green', 'the colour landed with the undo');
    assert.deepEqual(o.historyDepth, { past: 0, future: 1 });
    const fwd = o.redo();
    e.land(e.runs.length - 1);
    await fwd;
    assert.equal(o.committed.configuration.reportTitle, 'green', 'and is on the redo side too');
  });
});

describe('presentation while a query runs', () => {
  it('lands WITH the query: the colour is not lost when the change commits', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const a = o.change(group(['region']));
    await o.change(colour('blue'));
    e.land(e.runs.length - 1);
    await a;
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
    assert.equal(o.committed.configuration.reportTitle, 'blue');
    assert.deepEqual(o.historyDepth, { past: 2, future: 0 }, 'two steps: the colour, then the change');
    const back = o.undo();
    assert.equal(o.busy, true, 'undoing the query change runs a query');
    e.land(e.runs.length - 1);
    await back;
    assert.deepEqual([o.committed.snapshot.rows, o.committed.configuration.reportTitle], [[], 'blue']);
  });
});

// Carried over from the controller's history (test/history.test.ts and test/undo-failure.test.ts,
// deleted with src/history.ts in Leg B / B1b): every rule they pinned, now on the owner.
describe('the history, as the controller\'s pinned it', () => {
  const s = (rows: string[]): CubeState => ({ ...INITIAL, snapshot: { ...SNAPSHOT, rows } });

  it('ignores a step that changed nothing', () => {
    const h = new UndoStack<CubeState>(stateKey);
    h.record(s(['region']));
    h.record(s(['region']));
    assert.equal(h.depth.past, 1);
  });

  it('does not count the epoch as a change, and counts expansion as one', () => {
    assert.equal(stateKey({ ...INITIAL, snapshot: { ...SNAPSHOT, epoch: 1 } }),
      stateKey({ ...INITIAL, snapshot: { ...SNAPSHOT, epoch: 99 } }));
    assert.notEqual(stateKey(INITIAL), stateKey({ ...INITIAL, tree: TreeState.fromPaths([['EMEA']]) }));
  });

  it('drops the oldest step past the limit', async () => {
    const e = new Engine();
    const o = new CubeStateOwner(INITIAL, e.run, { historyLimit: 2 });
    for (let i = 0; i < 6; i += 1) {
      const c = o.change(group([`c${i}`]));
      e.land(e.runs.length - 1);
      await c;
    }
    assert.equal(o.historyDepth.past, 2);
    o.setHistoryLimit(1);
    assert.equal(o.historyDepth.past, 1, 'Settings > Max History Stack Size takes effect at once');
  });

  it('clears the redo branch when a new step is recorded', () => {
    const h = new UndoStack<CubeState>(stateKey);
    h.record(s(['a']));
    h.undone(s(['b']));
    assert.equal(h.depth.future, 1);
    h.record(s(['c']));
    assert.equal(h.depth.future, 0, 'a new edit invalidates the future');
  });

  it('walks back more than one step rather than toggling', async () => {
    const { o, e } = owner();
    await landed(o, e);
    for (const rows of [['region'], ['region', 'desk']]) {
      const c = o.change(group(rows));
      e.land(e.runs.length - 1);
      await c;
    }
    for (const rows of [['region'], []]) {
      const u = o.undo();
      e.land(e.runs.length - 1);
      await u;
      assert.deepEqual(o.committed.snapshot.rows, rows);
    }
  });

  it('does not record a refresh as a step', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const c = o.change(group(['region']));
    e.land(e.runs.length - 1);
    await c;
    const before = o.historyDepth.past;
    for (let i = 0; i < 2; i += 1) {
      const r = o.refresh();
      e.land(e.runs.length - 1);
      await r;
    }
    assert.equal(o.historyDepth.past, before);
  });

  it('undoes an expand', async () => {
    const { o, e } = owner();
    await landed(o, e);
    const c = o.change((st) => ({ ...st, tree: st.tree.setOpen(['EMEA'], true) }));
    e.land(e.runs.length - 1);
    await c;
    const u = o.undo();
    e.land(e.runs.length - 1);
    await u;
    assert.equal(o.committed.tree.isOpen(['EMEA']), false, 'collapsed again');
  });

  it('says there is nothing to undo rather than throwing', async () => {
    const { o, e } = owner();
    await landed(o, e);
    assert.equal((await o.undo()).kind, 'nothing');
    assert.equal(o.canUndo, false);
  });

  it('an undo refused: the cube, its configuration and the step all stay; it works once the engine is back', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const c = o.change((st) => ({ ...st, snapshot: { ...st.snapshot, rows: ['region'] },
      configuration: { ...st.configuration, reportTitle: 'B' } }));
    e.land(e.runs.length - 1);
    await c;
    const before = o.historyDepth;
    const u = o.undo();
    e.refuse(e.runs.length - 1);
    assert.equal((await u).kind, 'refused');
    assert.equal(events.at(-1), 'refused', 'the host heard about it');
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
    assert.equal(o.committed.configuration.reportTitle, 'B', 'the configuration too');
    assert.deepEqual(o.historyDepth, before, 'the step was not spent, no redo invented');
    const again = o.undo();
    e.land(e.runs.length - 1);
    await again;
    assert.deepEqual(o.committed.snapshot.rows, [], 'the retry landed');
  });
});

describe('events', () => {
  it('unsubscribes', async () => {
    const { o, e } = owner();
    let heard = 0;
    const off = o.subscribe(() => { heard += 1; });
    off();
    await landed(o, e);
    await tick();
    assert.equal(heard, 0);
  });
});

// The owner is not the cube's alone: Ad Hoc Analysis mode's grid runs on the same rules. Proven
// here on a state with a query part (q), a presentation part (p), and a switch that defers
// queries (Navigate Without Data).
describe('the owner over any state: deferral and re-placing (StateRules)', () => {
  interface Toy { readonly q: string; readonly p: string; readonly defer: boolean }
  interface ToyView { readonly for: string; readonly p: string }
  const RULES: StateRules<Toy, ToyView> = {
    fold: (s) => s,
    queryKey: (s) => s.q,
    stateKey: (s) => JSON.stringify(s),
    land: (pending) => pending,
    represent: (state, view) => ({ ...view, p: state.p }),
    defer: (s) => s.defer,
  };
  const make = () => {
    const runs: { state: Toy; answer: (v: ToyView) => void }[] = [];
    const o = new StateOwner<Toy, ToyView>({ q: 'a', p: 'plain', defer: false },
      (state) => new Promise((answer) => runs.push({ state, answer })), RULES);
    const events: string[] = [];
    o.subscribe((e) => events.push(e.kind));
    const land = async (out: Promise<unknown>) => {
      const r = runs.at(-1) ?? assert.fail('no run');
      r.answer({ for: r.state.q, p: r.state.p });
      await out;
    };
    return { o, runs, events, land };
  };

  it('deferred: committed with no query, the view stays the old state\'s and says so; refresh queries it', async () => {
    const { o, runs, events, land } = make();
    await land(o.refresh());
    await o.change((s) => ({ ...s, defer: true }));
    assert.equal(events.at(-1), 'presentation', 'the switch itself changes no query');
    const ran = runs.length;
    assert.equal((await o.change((s) => ({ ...s, q: 'b' }))).kind, 'applied');
    assert.equal(runs.length, ran, 'no query');
    assert.equal(events.at(-1), 'deferred');
    assert.equal(o.committed.q, 'b');
    assert.equal(o.view?.for, 'a', 'the view on screen is the old state\'s');
    assert.equal(o.stale, true);
    await land(o.refresh());
    assert.equal(o.view?.for, 'b');
    assert.equal(o.stale, false);
  });

  it('deferred: undo and redo move without a query too', async () => {
    const { o, runs, land } = make();
    await land(o.refresh());
    await o.change((s) => ({ ...s, defer: true }));
    await o.change((s) => ({ ...s, q: 'b' }));
    const ran = runs.length;
    assert.equal((await o.undo()).kind, 'applied');
    assert.equal(o.committed.q, 'a');
    assert.equal(runs.length, ran, 'undo ran nothing');
    assert.equal(o.stale, false, 'back to what is on screen');
    await o.redo();
    assert.equal(o.committed.q, 'b');
    assert.equal(runs.length, ran);
  });

  it('a presentation change re-places the view it is on, never answers of another state', async () => {
    const { o, land } = make();
    await land(o.refresh());
    await o.change((s) => ({ ...s, defer: true }));
    await o.change((s) => ({ ...s, q: 'b' }));
    await o.change((s) => ({ ...s, p: 'indented' }));
    assert.deepEqual(o.view, { for: 'a', p: 'indented' }, 'the old answers, placed the new way, still marked stale');
    assert.equal(o.stale, true);
  });

  it('a presentation change made while a query runs lands on the view too', async () => {
    const { o, runs, land } = make();
    await land(o.refresh());
    const out = o.change((s) => ({ ...s, q: 'b' }));
    await o.change((s) => ({ ...s, p: 'indented' }));
    const r = runs.at(-1) ?? assert.fail('no run');
    r.answer({ for: 'b', p: r.state.p });
    await out;
    assert.deepEqual(o.view, { for: 'b', p: 'indented' }, 'the answer was laid out the way the state now says');
  });
});


describe('a tick: the view run again, as it is', () => {
  it('runs the committed state, tells `ticked` with its view, and is no step, nothing busy, nothing pending', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const out = o.rerun();
    assert.equal(o.busy, false, 'nothing in flight to show');
    assert.equal(e.runs.length, 2);
    assert.deepEqual(e.runs[1]?.state, o.committed, 'the committed state, as it is');
    e.land(1);
    const done = await out;
    assert.equal(done.kind, 'applied');
    assert.equal(events.at(-1), 'ticked');
    assert.equal(o.view, (done as { view: CubeView }).view, 'its view the one on screen');
    assert.equal(o.canUndo, false, 'no step');
  });

  it('a change made while it runs wins: the tick is superseded and its view dropped', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const ticked = o.rerun();
    const changed = o.change(group(['region']));
    e.land(1);
    assert.equal((await ticked).kind, 'superseded');
    assert.equal(events.includes('ticked'), false);
    e.land(2);
    assert.equal((await changed).kind, 'applied');
    assert.deepEqual(o.committed.snapshot.rows, ['region']);
  });

  it('nothing runs while a change is in flight, or before a view is shown', async () => {
    const { o, e } = owner();
    assert.equal((await o.rerun()).kind, 'superseded', 'no view yet');
    await landed(o, e);
    const changed = o.change(group(['region']));
    const runs = e.runs.length;
    assert.equal((await o.rerun()).kind, 'superseded');
    assert.equal(e.runs.length, runs, 'no query');
    e.land(runs - 1);
    await changed;
  });

  it('a refusal is returned, not said: the view on screen stays', async () => {
    const { o, e, events } = owner();
    await landed(o, e);
    const shown = o.view;
    const heard = events.length;
    const out = o.rerun();
    e.refuse(1, 'the live source failed');
    const done = await out;
    assert.equal(done.kind, 'refused');
    assert.equal(o.view, shown);
    assert.equal(events.length, heard, 'nobody told');
  });
});
