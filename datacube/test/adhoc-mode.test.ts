// Ad Hoc Analysis mode on screen: the gestures reach the session and the
// grid shows its answers. Double-click zooms, the right-click menu acts
// on the member under it, the POV chip opens Member Selection with one
// pick, and the Options window re-places the answers.
//
// Over the in-memory source (adhoc-fixture.ts); the browser harness
// runs the same gestures against the real planner and engine.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { AdHocMode } from '../src/adhoc/mode.ts';
import { FormatterCache } from '../src/format.ts';
import { session } from './adhoc-fixture.ts';

let dom: JSDOM;
let doc: Document;
let host: HTMLElement;
let windows: Map<string, { host: HTMLElement; closed: boolean }>;
let statuses: string[];
let failures: unknown[];
let exits: number;

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body><div id="m"></div></body>');
  doc = dom.window.document as unknown as Document;
  host = doc.getElementById('m') as HTMLElement;
  const win = dom.window as unknown as { requestAnimationFrame: unknown };
  win.requestAnimationFrame = (cb: FrameRequestCallback) => {
    cb(0);
    return 1;
  };
  globalThis.requestAnimationFrame = win.requestAnimationFrame as typeof requestAnimationFrame;
  globalThis.cancelAnimationFrame = () => {};
  windows = new Map();
  statuses = [];
  failures = [];
  exits = 0;
});

function mount(calls: string[] = []): AdHocMode {
  return new AdHocMode(host, session(calls), {
    formatters: new FormatterCache(),
    rowHeight: 20,
    showWindow: (title, build) => {
      const el = doc.createElement('div');
      doc.body.append(el);
      const entry = { host: el, closed: false };
      windows.set(title, entry);
      build(el, () => {
        entry.closed = true;
        el.remove();
      });
    },
    startTask: () => () => {},
    status: (text) => statuses.push(text),
    reportFailure: (e) => failures.push(e),
    onExit: () => {
      exits += 1;
    },
  });
}

/** Let every queued query answer. */
async function settle(mode: AdHocMode): Promise<void> {
  for (let i = 0; i < 50; i++) {
    await new Promise((r) => setTimeout(r, 0));
    if (!mode.busy) return;
  }
  throw new Error('the mode never settled');
}

const labels = (mode: AdHocMode) =>
  mode.view?.table.columns[0]?.values.map((v) => String(v).trim());
const values = (mode: AdHocMode, c = 1) => mode.view?.table.columns[c]?.values;

function cell(text: string): HTMLElement {
  const found = [...host.querySelectorAll<HTMLElement>('.dc-cell')]
    .find((c) => c.textContent?.trim() === text);
  assert.ok(found, `no cell '${text}'`);
  return found;
}

function header(text: string): HTMLElement {
  const found = [...host.querySelectorAll<HTMLElement>('.dc-th')]
    .find((c) => c.textContent?.trim() === text);
  assert.ok(found, `no header '${text}'`);
  return found;
}

function fire(el: Element, type: string): void {
  el.dispatchEvent(new dom.window.MouseEvent(type, { bubbles: true, cancelable: true }));
}

/**
 * Double-click a row member as a browser reports it: two clicks, the
 * second counted 2 -- on the cell found AGAIN, since the first click
 * re-renders the one it landed on.
 */
function doubleClick(text: string): void {
  for (const detail of [1, 2]) {
    cell(text).dispatchEvent(new dom.window.MouseEvent('click',
      { bubbles: true, cancelable: true, detail }));
  }
}

function menuItem(label: string): HTMLElement {
  const found = [...doc.querySelectorAll<HTMLElement>('[role="menuitem"]')]
    .find((m) => m.querySelector('.dc-menu-label, span')?.textContent === label
      || m.textContent?.startsWith(label));
  assert.ok(found, `no menu entry '${label}'`);
  return found;
}

describe('Ad Hoc Analysis mode on screen', () => {
  it('opens on the top member, the measures across, the rest on the POV bar', async () => {
    const mode = mount();
    await mode.refresh();
    assert.deepEqual(labels(mode), ['Time']);
    assert.deepEqual(values(mode), [75]);
    assert.ok(header('notional'));
    assert.ok(header('pnl'));
    const chips = [...host.querySelectorAll<HTMLElement>('.dc-adhoc-pov-chip')];
    assert.deepEqual(chips.map((c) => c.dataset['dimension']), ['region']);
    assert.match(chips[0]?.textContent ?? '', /region/);
  });

  it('double-clicking a row member zooms in on it', async () => {
    const mode = mount();
    await mode.refresh();
    doubleClick('Time');
    await settle(mode);
    assert.deepEqual(labels(mode), ['Time', '2021', '2022']);
    assert.deepEqual(values(mode), [75, 35, 40]);
    doubleClick('2021');
    await settle(mode);
    assert.deepEqual(labels(mode), ['Time', '2021', 'Q1', 'Q2', '2022']);
  });

  it('a leaf has nothing to zoom into, and says so without a query', async () => {
    const calls: string[] = [];
    const mode = mount(calls);
    await mode.refresh();
    doubleClick('Time');
    await settle(mode);
    doubleClick('2021');
    await settle(mode);
    const asked = calls.length;
    doubleClick('Q1');
    await settle(mode);
    assert.equal(calls.length, asked);
    assert.match(statuses.at(-1) ?? '', /no members beneath/);
  });

  it('the right-click menu acts on the member under it: Keep Only, Zoom Out, Remove Only', async () => {
    const mode = mount();
    await mode.refresh();
    doubleClick('Time');
    await settle(mode);

    fire(cell('2022'), 'contextmenu');
    menuItem('Keep Only').click();
    await settle(mode);
    assert.deepEqual(labels(mode), ['2022']);

    fire(cell('2022'), 'contextmenu');
    menuItem('Zoom Out').click();
    await settle(mode);
    assert.deepEqual(labels(mode), ['Time']);

    doubleClick('Time');
    await settle(mode);
    fire(cell('2021'), 'contextmenu');
    menuItem('Remove Only').click();
    await settle(mode);
    assert.deepEqual(labels(mode), ['Time', '2022']);
  });

  it('Zoom In from the menu takes the level asked for', async () => {
    const mode = mount();
    await mode.refresh();
    fire(cell('Time'), 'contextmenu');
    menuItem('Bottom Level').click();
    await settle(mode);
    assert.deepEqual(labels(mode), ['Time', 'Q1', 'Q2', 'Q1']);
  });

  it('Pivot moves the dimension across, and a column member header double-click zooms it', async () => {
    const mode = mount();
    await mode.refresh();
    // The only row dimension cannot leave: the entry says so.
    fire(cell('Time'), 'contextmenu');
    assert.equal(menuItem('Pivot to Columns').getAttribute('aria-disabled'), 'true');
    // region from the POV onto the rows, then Time across.
    fire(host.querySelector('.dc-adhoc-pov-chip') as HTMLElement, 'contextmenu');
    menuItem('Move to Rows').click();
    await settle(mode);
    assert.deepEqual(mode.session.grid.rows.map((a) => a.dimension), ['Time', 'region']);
    assert.deepEqual(host.querySelectorAll('.dc-adhoc-pov-chip').length, 0);
    fire(cell('Time'), 'contextmenu');
    menuItem('Pivot to Columns').click();
    await settle(mode);
    assert.deepEqual(mode.session.grid.columns.map((a) => a.dimension), ['Measures', 'Time']);
    // The Time top member sits under each measure.
    const tops = [...host.querySelectorAll<HTMLElement>('.dc-th')]
      .filter((h) => h.textContent === 'Time');
    assert.equal(tops.length, 2);
    fire(tops[0] as HTMLElement, 'dblclick');
    await settle(mode);
    const time = mode.session.grid.columns.find((a) => a.dimension === 'Time');
    assert.deepEqual(time?.members, [[], ['2021'], ['2022']]);
  });

  it('the POV chip opens Member Selection with ONE pick; OK filters every cell', async () => {
    const mode = mount();
    await mode.refresh();
    (host.querySelector('.dc-adhoc-pov-chip') as HTMLElement).click();
    const w = windows.get('Member Selection: region');
    assert.ok(w);
    await new Promise((r) => setTimeout(r, 0));
    await new Promise((r) => setTimeout(r, 0));
    const picks = [...w.host.querySelectorAll<HTMLInputElement>('.dc-adhoc-member-pick')];
    assert.ok(picks.every((p) => p.type === 'radio'));
    const emea = picks.find((p) => p.getAttribute('aria-label') === 'EMEA');
    assert.ok(emea, 'the source members are listed');
    emea.click();
    (w.host.querySelector('.dc-adhoc-members-ok') as HTMLButtonElement).click();
    assert.ok(w.closed);
    await settle(mode);
    assert.deepEqual(mode.session.grid.pov['region'], ['EMEA']);
    assert.deepEqual(values(mode), [30]);
    assert.match(host.querySelector('.dc-adhoc-pov-chip')?.textContent ?? '', /EMEA/);
  });

  it('Member Selection for an axis takes many, with the shortcuts, in the order picked', async () => {
    const mode = mount();
    await mode.refresh();
    mode.openMemberSelection('Time', 'axis');
    const w = windows.get('Member Selection: Time');
    assert.ok(w);
    await new Promise((r) => setTimeout(r, 0));
    // Clear, then the top's children by the shortcut.
    (w.host.querySelector('.dc-adhoc-members-clear') as HTMLButtonElement).click();
    (w.host.querySelector('.dc-adhoc-members-children') as HTMLButtonElement).click();
    await new Promise((r) => setTimeout(r, 0));
    const picked = [...w.host.querySelectorAll('.dc-adhoc-picked-label')].map((e) => e.textContent);
    assert.deepEqual(picked, ['2021', '2022']);
    (w.host.querySelector('.dc-adhoc-members-ok') as HTMLButtonElement).click();
    await settle(mode);
    assert.deepEqual(labels(mode), ['2021', '2022']);
  });

  it('Options re-place the answers: a missing row comes back with suppression off', async () => {
    const calls: string[] = [];
    const mode = mount(calls);
    await mode.refresh();
    doubleClick('Time');
    await settle(mode);
    await mode.session.setPov('region', ['EMEA']);
    assert.deepEqual(labels(mode), ['Time', '2021']);
    const asked = calls.length;
    mode.openOptions();
    const w = windows.get('Ad Hoc Options');
    assert.ok(w);
    const box = w.host.querySelector<HTMLInputElement>('[data-option="suppressMissingRows"]');
    assert.ok(box?.checked, 'missing rows are suppressed by default');
    box.click();
    (w.host.querySelector('.dc-adhoc-options-ok') as HTMLButtonElement).click();
    await settle(mode);
    assert.equal(calls.length, asked, 'an option ran a query');
    assert.deepEqual(labels(mode), ['Time', '2021', '2022']);
  });

  it('undo and redo, and Exit hands back to the app', async () => {
    const mode = mount();
    await mode.refresh();
    doubleClick('Time');
    await settle(mode);
    await mode.undo();
    assert.deepEqual(labels(mode), ['Time']);
    await mode.redo();
    assert.deepEqual(labels(mode), ['Time', '2021', '2022']);
    const exit = [...host.querySelectorAll<HTMLButtonElement>('.dc-adhoc-tool')]
      .find((b) => b.textContent === 'Exit');
    exit?.click();
    assert.equal(exits, 1);
    assert.deepEqual(failures, []);
  });
});
