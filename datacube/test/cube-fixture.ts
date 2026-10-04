// The cube as a person meets it, for Leg B's app-level tests (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md):
// a CubeApp in a DOM over an engine the test holds or fails (gate.ts), and the gestures -- a menu
// entry, a chevron -- by the words a person reads. The state is exported as LIVE bindings: a
// test file calls `setUp` in its beforeEach and reads `app`, `root`, ... as they are now.

import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import type { CubeAppOptions } from '../src/app.ts';
import { FakeEngine } from './fake-engine.ts';
import { Gate } from './gate.ts';
import { fakeParse, fakePrint } from './fake-planner.ts';
import { element } from '../../pure-protocol/src/index.ts';

export const SNAPSHOT: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region', 'desk'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

/** An engine the test holds or fails (`gate`): every query answers the same small table. */
export class GateEngine extends FakeEngine {
  readonly name = 'gate';
  readonly gate = new Gate();
  queries = 0;
  /** Answer with an extra column `n`: which query this was, so two answers can be told apart. */
  tag = false;
  async answer(_sql: string, epoch: number): Promise<ResultTable> {
    this.queries += 1;
    const n = this.queries;
    const tagged = this.tag;
    await this.gate.pass();
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
        { name: 'desk', type: 'String', values: ['A', 'B'] },
        { name: 'total', type: 'Float', values: [600, 400] },
        ...(tagged ? [{ name: 'n', type: 'Integer', values: [n, n] }] : []),
      ],
      rowCount: 2,
      epoch,
      elapsedMs: 1,
    };
  }
}

export class StubPlanner implements Planner {
  async plan(): Promise<Plan> {
    return { sql: 'SELECT 1', columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

export let dom: JSDOM;
export let root: HTMLElement;
export let engine: GateEngine;
export let app: CubeApp;
export let unhandled: unknown[];
export let statuses: [string, string][];

/** Let every promise the app started settle (menus and chevrons do not return theirs). */
export async function settle(): Promise<void> {
  for (let i = 0; i < 20; i += 1) await new Promise((r) => setTimeout(r, 0));
}

/** A fresh cube in a fresh DOM: call it from each test file's beforeEach. */
export async function setUp(): Promise<void> {
  dom = new JSDOM('<!doctype html><div id="r"></div>');
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = dom.window.document;
  g['requestAnimationFrame'] = (cb: FrameRequestCallback) => {
    cb(0);
    return 1;
  };
  g['cancelAnimationFrame'] = () => {};
  root = dom.window.document.getElementById('r') as unknown as HTMLElement;
  engine = new GateEngine();
  unhandled = [];
  process.removeAllListeners('unhandledRejection');
  process.on('unhandledRejection', (e) => unhandled.push(e));
  statuses = [];
  app = new CubeApp(root, SNAPSHOT, { engine, planner: new StubPlanner(),
    onStatus: (text, kind) => statuses.push([text, kind]) });
  await app.open();
}

/** Replace the cube with one built with these options (the same DOM and engine), and open it. */
export async function remount(options: Partial<CubeAppOptions> = {}): Promise<void> {
  app.dispose();
  root.replaceChildren();
  app = new CubeApp(root, SNAPSHOT, { engine, planner: new StubPlanner(),
    onStatus: (text, kind) => statuses.push([text, kind]), ...options } as CubeAppOptions);
  await app.open();
  await settle();
}

export const menuItems = (): HTMLElement[] =>
  [...dom.window.document.querySelectorAll('.dc-menu [role="menuitem"], .dc-menu [role="menuitemcheckbox"]')] as HTMLElement[];

/** Right-click a cell showing `text`, then click the entry a person reads as `label`. */
export function menu(text: string, label: string): void {
  const cell = [...root.querySelectorAll<HTMLElement>('.dc-cell')]
    .find((c) => c.textContent?.trim().replace(/^[▸▾]/, '').startsWith(text));
  assert.ok(cell, `no cell '${text}'`);
  cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true }));
  const item = menuItems().find((i) => (i.querySelector('.dc-menu-label')?.textContent ?? i.textContent) === label);
  assert.ok(item, `no entry '${label}': ${menuItems().map((i) => i.textContent).join(' | ')}`);
  item.click();
}

/** The chevron of the group row labelled `label`. */
/** A group's label is its value, with its child count when the tree shows one: `EMEA (2)`. */
export const groupRow = (label: string): HTMLElement | undefined => [...root.querySelectorAll<HTMLElement>('.dc-row')]
  .find((r) => (r.querySelector('.dc-tree-label')?.textContent ?? '').replace(/ \(\d+\)$/, '') === label);

export function chevron(label: string): HTMLElement {
  const row = groupRow(label);
  assert.ok(row, `no group row '${label}'`);
  return row.querySelector('.dc-chevron') as HTMLElement;
}

/** Each zone of the zone bar and the chips in it, as a person reads them. */
export const zoneChips = (): string[] => {
  const zones = [...root.querySelectorAll<HTMLElement>('.dc-zone-bar .dc-zone')];
  assert.ok(zones.length > 0, 'no zones on screen');
  return zones.map((z) => [...z.querySelectorAll('.dc-chip-label')].map((c) => c.textContent?.trim() ?? '').join(','));
};

export const busy = (): boolean => root.querySelector('.dc-status-progress')?.classList.contains('dc-busy') ?? false;
