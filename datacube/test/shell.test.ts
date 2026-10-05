// The shell around the grid, as upstream's: the Settings window, the
// alerts that carry a query (execution error, code check), and the (?)
// documentation hints.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { DEFAULT_SETTINGS, readSettings, type SettingValues } from '../src/settings.ts';
import { buildSettingsPanel } from '../src/ui/settings-panel.ts';
import { buildExecutionErrorAlert, markPosition } from '../src/ui/alert.ts';
import { DOCS, docHint } from '../src/ui/docs.ts';
import { FakeEngine } from './fake-engine.ts';
import { fakeParse, fakePrint } from './fake-planner.ts';
import { element } from '../../pure-protocol/src/index.ts';

let dom: JSDOM;
let root: HTMLElement;

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
  (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
    (fn: () => void) => { fn(); return 0; };
  root = dom.window.document.getElementById('r') as HTMLElement;
});

const flush = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};
const click = (el: Element | null | undefined): void => {
  assert.ok(el, 'nothing to click');
  (el as HTMLElement).dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
};
const buttonNamed = (text: string, within: ParentNode = root): HTMLButtonElement =>
  [...within.querySelectorAll('button')].find((b) => b.textContent === text) as HTMLButtonElement;

describe('settings', () => {
  it("reads a host's values by upstream's keys, ignoring what it does not know", () => {
    const v = readSettings({
      'dataCube.grid.rowBuffer': 70,
      'dataCube.editor.maxHistoryStackSize': 3, // below the minimum of 10
      'dataCube.debugger.enableDebugMode': 'yes', // wrong type
      'dataCube.someday.unknown': true,
    });
    assert.equal(v['dataCube.grid.rowBuffer'], 70);
    assert.equal(v['dataCube.editor.maxHistoryStackSize'], 10);
    assert.equal(v['dataCube.debugger.enableDebugMode'], false);
    assert.equal('dataCube.someday.unknown' in v, false);
  });

  it('the panel edits a draft: Save applies it, Cancel drops it, Restore puts back the defaults', () => {
    const saved: SettingValues[] = [];
    let closed = 0;
    const actions: string[] = [];
    buildSettingsPanel(root, {
      values: DEFAULT_SETTINGS,
      onSave: (v) => saved.push(v),
      onAction: (k) => actions.push(k),
      onClose: () => { closed += 1; },
    });
    const restore = root.querySelector('.dc-settings-restore') as HTMLButtonElement;
    assert.equal(restore.disabled, true, 'already the defaults');
    const debug = root.querySelector('[data-setting="dataCube.debugger.enableDebugMode"] input') as HTMLInputElement;
    debug.checked = true;
    debug.dispatchEvent(new dom.window.Event('change'));
    assert.equal(restore.disabled, false);
    click(buttonNamed('Save'));
    assert.equal(saved.at(-1)?.['dataCube.debugger.enableDebugMode'], true);
    assert.equal(closed, 0, 'Save keeps the window');
    click(restore);
    click(buttonNamed('OK'));
    assert.equal(saved.at(-1)?.['dataCube.debugger.enableDebugMode'], false);
    assert.equal(closed, 1);
    click(buttonNamed('Reload'));
    assert.deepEqual(actions, ['dataCube.debugger.action.reload']);
  });

  it('an invalid number reverts, as upstream inputs do', () => {
    buildSettingsPanel(root, {
      values: DEFAULT_SETTINGS, onSave: () => {}, onAction: () => {}, onClose: () => {},
    });
    const buffer = root.querySelector('[data-setting="dataCube.grid.rowBuffer"] input') as HTMLInputElement;
    buffer.value = '3';
    buffer.dispatchEvent(new dom.window.Event('change'));
    assert.equal(buffer.value, '50');
  });
});

describe('alerts that carry a query', () => {
  it('an expired sign-in is answered in the error: the password, signed in, then closed', async () => {
    const tried: string[] = [];
    let closed = 0;
    buildExecutionErrorAlert(root, {
      message: "Data Fetch Failure: Can't execute query.",
      text: 'Error: the warehouse session for rita has expired — sign in again',
      signIn: {
        who: 'rita',
        where: 'the warehouse at wh:9090',
        submit: async (pw) => {
          tried.push(pw);
          if (pw !== 'right') throw new Error('sign-in failed — UNAUTHENTICATED: bad password');
        },
      },
    }, () => { closed += 1; });
    const form = root.querySelector('.dc-alert-signin') as HTMLFormElement;
    assert.match(form.textContent ?? '', /Sign in to the warehouse at wh:9090 as rita/);
    const password = form.querySelector('input[type=password]') as HTMLInputElement;
    const said = form.querySelector('.dc-alert-signin-said') as HTMLElement;
    const settle = () => new Promise((r) => setTimeout(r, 0));

    password.value = 'wrong';
    form.dispatchEvent(new dom.window.Event('submit', { cancelable: true }));
    await settle();
    assert.match(said.textContent ?? '', /bad password/);
    assert.equal(closed, 0, 'a failed sign-in keeps the window, and says why');

    password.value = 'right';
    form.dispatchEvent(new dom.window.Event('submit', { cancelable: true }));
    await settle();
    assert.deepEqual(tried, ['wrong', 'right']);
    assert.equal(closed, 1);
    assert.equal(password.value, '', 'the password is not left in the page');
  });

  it('an error that is not an expired sign-in offers none', () => {
    buildExecutionErrorAlert(root, { message: 'x', text: 'Error: boom' }, () => {});
    assert.equal(root.querySelector('.dc-alert-signin'), null);
  });

  it('the execution error hides its debug info until asked, and downloads it', () => {
    const files: [string, string, string][] = [];
    buildExecutionErrorAlert(root, {
      message: "Data Fetch Failure: Can't execute query.",
      text: 'Error: boom',
      pure: 't->limit(5)',
      sql: 'SELECT 1',
      download: (n, m, t) => files.push([n, m, t]),
    }, () => {});
    const debug = root.querySelector('.dc-alert-debug') as HTMLElement;
    assert.equal(debug.hidden, true);
    assert.equal(buttonNamed('Download Debug Info').hidden, true);
    const box = root.querySelector('.dc-alert-actions input[type=checkbox]') as HTMLInputElement;
    box.checked = true;
    box.dispatchEvent(new dom.window.Event('change'));
    assert.equal(debug.hidden, false);
    assert.match(debug.textContent ?? '', /t->limit\(5\)/);
    assert.match(debug.textContent ?? '', /SELECT 1/);
    click(buttonNamed('Download Debug Info'));
    assert.match(files[0]?.[0] ?? '', /^DEBUG__Query__.*\.json$/);
    assert.deepEqual(JSON.parse(files[0]?.[2] ?? '{}'),
      { error: 'Error: boom', queryCode: 't->limit(5)', sql: 'SELECT 1' });
  });

  it('marks the place a refusal names under its line', () => {
    assert.equal(markPosition('a\nbcd\ne', 'no [2:3] here'), 'a\nbcd\n  ^\ne');
    assert.equal(markPosition('a', 'no position'), 'a');
    assert.equal(markPosition('a', 'outside [9:1]'), 'a');
  });
});

describe('documentation hints', () => {
  it("a (?) asks for upstream's entry by its id", () => {
    const doc = dom.window.document;
    const hint = docHint(doc, 'data-cube.column-configuration.unit');
    root.append(hint);
    let asked: unknown;
    root.addEventListener('dc-doc', (e) => { asked = (e as CustomEvent).detail; });
    click(hint);
    assert.equal(asked, 'data-cube.column-configuration.unit');
    assert.match(DOCS['data-cube.column-configuration.unit'].blocks.map((b) =>
      Object.values(b)[0]).join(' '), /_\$ would show value 4 as \$4/);
  });
});

// -- the app, end to end --------------------------------------------------

const SNAPSHOT: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }],
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};

class Engine extends FakeEngine {
  readonly name = 'stub';
  failing = false;
  async answer(_sql: string, epoch: number): Promise<ResultTable> {
    if (this.failing) throw new Error('the engine fell over');
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: 'notional', type: 'Float', values: [1] },
      ],
      rowCount: 1, epoch, elapsedMs: 1,
    };
  }
}
const planner: Planner = {
  plan: async (): Promise<Plan> => ({ sql: 'SELECT 42', columns: [] }),
  relationType: async (): Promise<PlanColumn[]> => [],
  parse: fakeParse,
  print: fakePrint,
};

function app(options: Partial<ConstructorParameters<typeof CubeApp>[2]> = {}): { app: CubeApp; engine: Engine } {
  const engine = new Engine();
  const cube = new CubeApp(root, SNAPSHOT, { engine, planner, ...options } as ConstructorParameters<typeof CubeApp>[2]);
  return { app: cube, engine };
}

describe('the app', () => {
  it("offers New first in the title bar's menu (the user, 2026-10-01), then Undo and Redo, and Settings... last", async () => {
    const { app: cube } = app();
    await cube.open();
    click(root.querySelector('.dc-titlebar-menu'));
    // the menu's own entries, not a submenu's
    const labels = [...dom.window.document.querySelectorAll('.dc-menu > .dc-menu-item > .dc-menu-label, .dc-menu > * > .dc-menu-item > .dc-menu-label')]
      .map((l) => l.textContent);
    assert.deepEqual(labels.slice(0, 3), ['New', 'Undo', 'Redo']);
    assert.equal(labels[labels.length - 1], 'Settings...');
  });

  it('saved settings reach the host, which may keep them', async () => {
    const kept: SettingValues[] = [];
    const { app: cube } = app({
      settings: { 'dataCube.grid.rowBuffer': 20 },
      onSettingsChanged: (v: SettingValues) => kept.push(v),
    });
    await cube.open();
    cube.openSettings();
    const buffer = root.querySelector('[data-setting="dataCube.grid.rowBuffer"] input') as HTMLInputElement;
    assert.equal(buffer.value, '20', 'the host value, not the default');
    buffer.value = '30';
    buffer.dispatchEvent(new dom.window.Event('change'));
    click(buttonNamed('OK'));
    assert.equal(kept.at(-1)?.['dataCube.grid.rowBuffer'], 30);
    assert.equal(root.querySelector('[data-window="Settings"]'), null);
  });

  it("the status bar shows upstream's task progress while a query runs", async () => {
    const { app: cube, engine } = app();
    await cube.open();
    let release: () => void = () => {};
    const held = new Promise<void>((r) => { release = r; });
    const execute = engine.execute.bind(engine);
    engine.execute = async (sql, epoch) => { await held; return execute(sql, epoch); };
    cube.openSettings();
    click(buttonNamed('Reload'));
    await flush();
    const progress = root.querySelector('.dc-status-progress') as HTMLElement;
    assert.equal(progress.classList.contains('dc-busy'), true, 'no progress while fetching');
    assert.equal(progress.title, 'Fetching data...');
    release();
    await flush();
    assert.equal(root.querySelector('.dc-status-progress')?.classList.contains('dc-busy'), false);
  });

  it('a failed query opens the execution-error alert, with the query behind it', async () => {
    const { app: cube, engine } = app();
    await cube.open();
    engine.failing = true;
    cube.openSettings();
    click(buttonNamed('Reload'));
    await flush();
    const alert = root.querySelector('.dc-alert-execution') as HTMLElement;
    assert.ok(alert, 'no execution-error alert');
    assert.match(alert.textContent ?? '', /Data Fetch Failure: Can't execute query\./);
    assert.match(alert.textContent ?? '', /the engine fell over/);
    assert.match(alert.querySelector('.dc-alert-debug')?.textContent ?? '', /trades/);
    assert.match(alert.querySelector('.dc-alert-debug')?.textContent ?? '', /SELECT 42/);
  });

  it('a (?) opens ONE Documentation window, even when the host rebuilt the cube in place', async () => {
    // The demo host clears its element and builds a new cube when a
    // file opens; the first cube must not answer the second's (?).
    const { app: first } = app();
    await first.open();
    root.replaceChildren();
    const { app: cube } = app();
    await cube.open();
    cube.openSettings();
    const settings = root.querySelector('[data-window="Settings"]') as HTMLElement;
    settings.append(docHint(dom.window.document, 'data-cube.grid-configuration.row-limit'));
    click(settings.querySelector('.dc-doc-hint'));
    const wins = root.querySelectorAll('[data-window="Documentation"]');
    assert.equal(wins.length, 1);
    assert.match(wins[0]?.textContent ?? '', /Grid Configuration: Row Limit/);
  });
});
