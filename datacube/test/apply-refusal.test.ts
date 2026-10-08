// Properties > Apply with a draft the planner refuses must leave the
// cube where it was. It did not: the refused draft stayed as the
// cube's snapshot and configuration, so EVERY later query repeated the
// refusal -- one Apply wedged the cube until a reload (census headline
// 7; upstream compiles the whole query before publishing it).

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { FakeEngine } from './fake-engine.ts';
import { fakeParse, fakePrint, limitsOf } from './fake-planner.ts';
import { toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import { element } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

class Engine extends FakeEngine {
  readonly name = 'stub';
  async answer(_sql: string, epoch: number): Promise<ResultTable> {
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: 'total', type: 'Float', values: [1] },
      ],
      rowCount: 1,
      epoch,
      elapsedMs: 1,
    };
  }
}

/** Refuses any query capped at 43 rows -- a Row Limit of 42. */
class RefusingPlanner implements Planner {
  refusals = 0;
  planned = 0;
  async plan(query: Lambda): Promise<Plan> {
    this.planned += 1;
    if (limitsOf(query).includes(43)) {
      this.refusals += 1;
      throw new Error('refused: no limit of 42 here');
    }
    return { sql: 'SELECT 1', columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

const flush = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};

describe('Properties > Apply with a refused draft', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let app: CubeApp;
  let planner: RefusingPlanner;
  let statuses: [string, string][];

  beforeEach(async () => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => { fn(); return 0; };
    root = dom.window.document.getElementById('r') as HTMLElement;
    planner = new RefusingPlanner();
    statuses = [];
    app = new CubeApp(root, SNAPSHOT, {
      engine: new Engine(),
      planner,
      onStatus: (text, kind) => statuses.push([text, kind]),
    });
    await app.open();
  });

  const applyRowLimit = async (value: string): Promise<void> => {
    app.openEditor();
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const limit = [...overlay.querySelectorAll('.dc-field')]
      .find((f) => f.querySelector('.dc-field-label')?.textContent === 'Row Limit:')
      ?.querySelector('input') as HTMLInputElement;
    limit.value = value;
    limit.dispatchEvent(new dom.window.Event('change'));
    ([...overlay.querySelectorAll('.dc-editor-footer button')]
      .find((b) => b.textContent === 'Apply') as HTMLButtonElement).click();
    await flush();
  };

  it('puts the cube back, and says why', async () => {
    await applyRowLimit('42');
    assert.equal(planner.refusals, 1);
    // COMPILED before it was published, as upstream: the refusal is
    // upstream's code-check alert, showing the query that was refused.
    const alert = root.querySelector('.dc-alert-error') as HTMLElement;
    assert.match(alert?.textContent ?? '', /Can't safely apply changes/);
    // the refused query, as the planner prints it (this double's print is its JSON): the cap of 43
    assert.match(alert?.querySelector('.dc-alert-codecheck')?.textContent ?? '', /\{"_type":"integer","value":43\}/);
    assert.notEqual(app.snapshot.maxRows, 42, 'the refused snapshot stayed');
    assert.notEqual(app.configuration.maxRows, 42, 'the refused setting stayed');
    assert.ok(statuses.some(([t, k]) => k === 'error' && /refused/.test(t)));
  });

  it('is not wedged: the next action queries the cube as it was', async () => {
    await applyRowLimit('42');
    const refusals = planner.refusals;
    await applyRowLimit('7');
    assert.equal(planner.refusals, refusals, 'a later query repeated the refusal');
    assert.equal(app.snapshot.maxRows, 7);
  });
});

/** Refuses a query holding a given value, as the compiler refuses a date that is not a day. */
class ValueRefusingPlanner implements Planner {
  planned = 0;
  readonly refused: string;
  readonly message: string;
  constructor(refused: string, message: string) {
    this.refused = refused;
    this.message = message;
  }
  async plan(query: Lambda): Promise<Plan> {
    this.planned += 1;
    if (toJson(query).includes(JSON.stringify(this.refused))) throw new Error(this.message);
    return { sql: 'SELECT 1', columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

describe('Filter > Apply with a value the compiler refuses', () => {
  it('applies nothing, runs nothing, and the window says why in the compiler\'s words', async () => {
    const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => { fn(); return 0; };
    const root = dom.window.document.getElementById('r') as HTMLElement;
    const planner = new ValueRefusingPlanner('not-a-region', 'Invalid month: 13');
    let ran = 0;
    const engine = new (class extends Engine {
      override async answer(sql: string, epoch: number): Promise<ResultTable> {
        ran += 1;
        return super.answer(sql, epoch);
      }
    })();
    const app = new CubeApp(root, SNAPSHOT, { engine, planner });
    await app.open();
    app.openFilters();
    const filters = root.querySelector('[data-window="Filters"]') as HTMLElement;
    (filters.querySelector('.dc-filter-btn') as HTMLButtonElement).click();
    const value = filters.querySelector('input.dc-filter-value') as HTMLInputElement;
    value.value = 'not-a-region';
    value.dispatchEvent(new dom.window.Event('change'));
    const before = ran;
    (filters.querySelector('.dc-filter-apply') as HTMLButtonElement).click();
    await flush();
    assert.equal(ran, before, 'the refused filter reached the engine');
    assert.equal(app.snapshot.filter, undefined, 'a refused filter applied');
    assert.match(filters.textContent ?? '', /Invalid month: 13/);
  });
});

/** Holds every plan until released, when `hold` is set: a compile check still out. */
class HeldPlanner implements Planner {
  hold = false;
  #release: () => void = () => {};
  readonly #held = new Promise<void>((resolve) => {
    this.#release = resolve;
  });
  async plan(): Promise<Plan> {
    if (this.hold) await this.#held;
    return { sql: 'SELECT 1', columns: [] };
  }
  release(): void {
    this.#release();
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

// THE CUBE IS BUSY FROM A CHANGE'S ASK ON (2026-10-08): it was busy only once the change's query ran, so while Apply's
// compile check was out it read as quiet, and a harness waiting for quiet moved on before the change landed (the cause
// of //datacube:verify_features_test's flakes on a loaded runner).
describe('Filter > Apply is in flight from the click on', () => {
  it('busy while its compile check is out, then applied and quiet', async () => {
    const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => { fn(); return 0; };
    const root = dom.window.document.getElementById('r') as HTMLElement;
    const planner = new HeldPlanner();
    const app = new CubeApp(root, SNAPSHOT, { engine: new Engine(), planner });
    await app.open();
    app.openFilters();
    const filters = root.querySelector('[data-window="Filters"]') as HTMLElement;
    (filters.querySelector('.dc-filter-btn') as HTMLButtonElement).click();
    const value = filters.querySelector('input.dc-filter-value') as HTMLInputElement;
    value.value = 'EMEA';
    value.dispatchEvent(new dom.window.Event('change'));
    planner.hold = true;
    (filters.querySelector('.dc-filter-apply') as HTMLButtonElement).click();
    await flush();
    assert.equal(app.busy, true, 'quiet while the compile check was out');
    planner.release();
    await flush();
    assert.equal(app.busy, false);
    assert.notEqual(app.snapshot.filter, undefined, 'the filter applied');
  });
});

describe('Properties > Apply is in flight from the click on', () => {
  it('busy while its compile check is out, then applied and quiet', async () => {
    const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => { fn(); return 0; };
    const root = dom.window.document.getElementById('r') as HTMLElement;
    const planner = new HeldPlanner();
    const app = new CubeApp(root, SNAPSHOT, { engine: new Engine(), planner });
    await app.open();
    app.openEditor();
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const limit = [...overlay.querySelectorAll('.dc-field')]
      .find((f) => f.querySelector('.dc-field-label')?.textContent === 'Row Limit:')
      ?.querySelector('input') as HTMLInputElement;
    limit.value = '7';
    limit.dispatchEvent(new dom.window.Event('change'));
    planner.hold = true;
    ([...overlay.querySelectorAll('.dc-editor-footer button')]
      .find((b) => b.textContent === 'Apply') as HTMLButtonElement).click();
    await flush();
    assert.equal(app.busy, true, 'quiet while the compile check was out');
    planner.release();
    await flush();
    assert.equal(app.busy, false);
    assert.equal(app.snapshot.maxRows, 7, 'the row limit applied');
  });
});
