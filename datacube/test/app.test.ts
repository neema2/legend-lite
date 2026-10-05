import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../src/app.ts';
import type { Planner } from '../src/cube.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { pivotTotalColumn } from '../src/snapshot.ts';
import { setHeaderDrag } from '../src/ui/pivot-panel.ts';
import { FakeEngine } from './fake-engine.ts';
import { fakeParse, fakePrint } from './fake-planner.ts';
import { toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import { element } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

/** A grand total and two regions, enough for the grid to render. */
function result(epoch: number): ResultTable {
  return {
    columns: [
      { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
      { name: 'total', type: 'Float', values: [600, 400] },
    ],
    rowCount: 2,
    epoch,
    elapsedMs: 1,
  };
}

class StubEngine extends FakeEngine {
  readonly name = 'stub';
  readonly sql: string[] = [];
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    return result(epoch);
  }
}

/**
 * A stub that answers the snap's preflight COUNT with a real number.
 *
 * `StubEngine` returns the grid fixture for every query, so a
 * preflight reads 'EMEA' as its row count and the snapshot ends up
 * NaN rows big -- which would let a tooltip that dropped the row
 * count pass.
 */
class CountingEngine extends FakeEngine {
  readonly name = 'counting';
  readonly sql: string[] = [];
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    if (/count\(\*\)/i.test(sql)) {
      return {
        columns: [{ name: 'n', type: 'Integer', values: [29] }],
        rowCount: 1,
        epoch,
        elapsedMs: 0,
      };
    }
    return result(epoch);
  }
}

/** A planner that hands the query's JSON on as the "SQL", for engines that read it. */
class EchoPlanner implements Planner {
  async plan(query: Lambda): Promise<Plan> {
    return { sql: toJson(query), columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

class StubPlanner implements Planner {
  readonly queries: Lambda[] = [];
  async plan(query: Lambda): Promise<Plan> {
    this.queries.push(query);
    return { sql: 'SELECT 1', columns: [] };
  }
  async relationType(): Promise<PlanColumn[]> {
    return [];
  }
  parse = fakeParse;
  print = fakePrint;
}

/** A cube pivoted on desk, grouped by region. */
const PIVOTED: CubeSnapshot = {
  ...SNAPSHOT,
  rows: ['region'],
  pivotOn: ['desk'],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
};

/** An engine that answers in the shape a pivot produces. */
/**
 * A pivot's two steps, answered: the values query (with `EchoPlanner`
 * the "SQL" is the query's JSON, so it can be told apart) gets the desks, and
 * every level gets its cells.
 */
class PivotEngine extends FakeEngine {
  readonly name = 'pivot';
  readonly sql: string[] = [];
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    if (sql.includes('"function":"distinct"')) {
      return {
        columns: [{ name: 'desk', type: 'String', values: ['A', 'B'] }],
        rowCount: 2,
        epoch,
        elapsedMs: 1,
      };
    }
    return {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
        { name: 'A__|__total', type: 'Float', values: [1, 2] },
        { name: 'B__|__total', type: 'Float', values: [3, 4] },
      ],
      rowCount: 2,
      epoch,
      elapsedMs: 1,
    };
  }
}

describe('the app', () => {
  let dom: JSDOM;
  let root: HTMLElement;
  let app: CubeApp;
  let engine: StubEngine;
  let planner: StubPlanner;
  let downloads: [string, string, string | Uint8Array][];
  let clipboard: string[];
  let statuses: [string, string][];

  beforeEach(async () => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => {
        fn();
        return 0;
      };
    root = dom.window.document.getElementById('r') as HTMLElement;
    engine = new StubEngine();
    planner = new StubPlanner();
    downloads = [];
    clipboard = [];
    statuses = [];
    app = new CubeApp(root, SNAPSHOT, {
      engine,
      planner,
      dimensions: [{ name: 'Geography', columns: ['region', 'desk'] }],
      showColumnZone: true,
      onStatus: (text, kind) => statuses.push([text, kind]),
      writeClipboard: (t) => {
        clipboard.push(t);
      },
      download: (n, m, t) => downloads.push([n, m, t]),
    });
    await app.open();
  });

  /** Open the grid's right-click menu, optionally over a column. */
  const rightClick = (selector = '.dc-app-grid'): void => {
    (root.querySelector(selector) as HTMLElement).dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
  };
  /** Open the title bar's hamburger. */
  const hamburger = (): void => {
    (root.querySelector('.dc-titlebar-menu') as HTMLButtonElement).click();
  };
  const menuItems = (): HTMLElement[] =>
    [
      ...dom.window.document.querySelectorAll('.dc-menu [role="menuitem"], .dc-menu [role="menuitemcheckbox"]'),
    ] as HTMLElement[];
  /** Click a menu entry by the words a user reads. */
  const pick = (label: string): void => {
    const item = menuItems().find((i) =>
      (i.querySelector('.dc-menu-label')?.textContent ?? i.textContent) ===
      label,
    );
    if (!item) {
      throw new Error(
        `no menu entry "${label}"; saw: ${menuItems()
          .map((i) => i.textContent)
          .join(', ')}`,
      );
    }
    item.click();
  };

  describe('value filters on a group row', () => {
    /**
     * A measure shown under its OWN column's name -- how every uploaded
     * file's measures appear -- so the menu knows its type and would
     * offer a value filter. (A measure named apart from its column, as
     * `total` above, has no type to offer operators for.)
     */
    class OwnNameEngine extends FakeEngine {
      readonly name = 'own-name';
      async answer(_sql: string, epoch: number): Promise<ResultTable> {
        return {
          columns: [
            { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
            { name: 'notional', type: 'Float', values: [600, 400] },
          ],
          rowCount: 2, epoch, elapsedMs: 1,
        };
      }
    }
    let host: HTMLElement;
    beforeEach(async () => {
      host = dom.window.document.createElement('div');
      dom.window.document.body.append(host);
      const own = new CubeApp(host, {
        ...SNAPSHOT,
        measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
      }, { engine: new OwnNameEngine(), planner });
      await own.open();
    });
    /** The grid cell showing `text`, right-clicked. */
    const rightClickCell = (text: string): void => {
      const cell = [...host.querySelectorAll<HTMLElement>('.dc-cell')]
        .find((c) => c.textContent?.trim().replace(/^[▸▾]/, '').startsWith(text));
      assert.ok(cell, `no cell '${text}'`);
      cell.dispatchEvent(new dom.window.MouseEvent('contextmenu', { bubbles: true }));
    };
    const filters = (): string[] => menuItems()
      .map((i) => i.querySelector('.dc-menu-label')?.textContent ?? '')
      .filter((t) => t.startsWith('Add Filter:'));

    it('offers none on a measure: the cell is the group\'s sum, filters run on rows', () => {
      // `notional = 600` would keep the trades whose own notional is 600
      // -- almost always none -- not the group showing 600.
      rightClickCell('600');
      assert.deepEqual(filters(), []);
    });

    it('still offers one on the group key', () => {
      rightClickCell('EMEA');
      assert.ok(filters().some((t) => t.includes('EMEA')), filters().join(' | '));
    });
  });

  it('renders the title bar, the drag zones, the grid and the status line', () => {
    // A TITLE BAR, not a toolbar. DataCube has no row of buttons
    // over the grid: everything lives in the right-click menu.
    assert.notEqual(root.querySelector('.dc-titlebar'), null);
    assert.equal(root.querySelector('.dc-app-toolbar'), null);
    // NO BRAND. An unnamed cube gets no title element at all --
    // "DataCube" over the grid tells a person nothing they did not
    // know from opening it, and cost a third of a 28px bar.
    assert.equal(root.querySelector('.dc-titlebar-title'), null);
    assert.equal(
      /DataCube/.test(root.querySelector('.dc-titlebar')?.textContent ?? ''),
      false,
      root.querySelector('.dc-titlebar')?.textContent ?? '',
    );
    assert.notEqual(root.querySelector('.dc-zone-rows'), null);
    assert.notEqual(root.querySelector('.dc-grid'), null);
    assert.notEqual(root.querySelector('.dc-app-stats'), null);
  });

  it("the columns panel's tick box hides the column", () => {
    // Wiring, which is the part unit tests miss: the panel had a
    // callback for this and the app never passed one, so the box was
    // not there at all. Mutating the wiring away leaves every
    // panel-side unit test green.
    const box = root.querySelector<HTMLInputElement>(
      '.dc-tool-panel-row[data-column="desk"] .dc-tool-panel-show',
    );
    assert.notEqual(box, null, 'the panel offers no tick box');
    assert.equal(box?.checked, true);
    box!.checked = false;
    box!.dispatchEvent(new dom.window.Event('change'));
    assert.equal(app.configuration.columns['desk']?.hidden, true);
    // And the row stays in the list, marked, so it can be found.
    const row = root.querySelector('.dc-tool-panel-row[data-column="desk"]');
    assert.notEqual(row, null);
    assert.equal(row?.classList.contains('dc-hidden-column'), true);
  });

  // -- folding the chrome away ---------------------------------------
  //
  // Both bars fold, the way the columns panel does, because the
  // grid is what the page is for. The rule they all follow: a bar
  // that vanishes leaves something to click, and the grid's own
  // right-click menu can restore either one -- which matters most
  // for the title bar, because the hamburger is IN it.

  const zoneBar = (): HTMLElement =>
    root.querySelector('.dc-zone-bar') as HTMLElement;
  const press = (selector: string): void => {
    const el = root.querySelector(selector);
    if (!el) throw new Error(`no ${selector} to press`);
    (el as HTMLButtonElement).click();
  };

  it('starts with both bars on screen', () => {
    // A drop target you cannot see is a feature you cannot find, so
    // the default is what upstream shows: everything visible.
    assert.equal(zoneBar().hidden, false);
    assert.equal(
      root.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed'),
      false,
    );
    assert.equal(app.configuration.showDragZones, true);
    assert.equal(app.configuration.showTitleBar, true);
  });

  it('folds the drag zones, leaving the way back in the title bar', () => {
    press('.dc-zone-fold');
    assert.equal(zoneBar().hidden, true);
    // NEVER NOTHING TO CLICK. The bar is gone, so the bar that is
    // still there carries the twin that brings it back.
    assert.notEqual(root.querySelector('.dc-titlebar-zones'), null);
    press('.dc-titlebar-zones');
    assert.equal(zoneBar().hidden, false);
    // And the control goes away again, rather than sitting there
    // doing nothing.
    assert.equal(root.querySelector('.dc-titlebar-zones'), null);
  });

  it('brings the folded zones back FOR THE LENGTH OF A DRAG', () => {
    // Folding them must not take anything away: a person who folds
    // the zones and then drags a column header has nowhere to drop
    // it, and a drag that can never land is worse than no drag.
    press('.dc-zone-fold');
    assert.equal(zoneBar().hidden, true);
    root.dispatchEvent(new dom.window.Event('dragstart', { bubbles: true }));
    assert.equal(zoneBar().hidden, false, 'nowhere to drop the column');
    assert.equal(zoneBar().classList.contains('dc-peeking'), true);
    root.dispatchEvent(new dom.window.Event('dragend', { bubbles: true }));
    assert.equal(zoneBar().hidden, true, 'the peek did not fold itself back');
    // The fold is still what the configuration says, so the peek
    // did not quietly become the setting.
    assert.equal(app.configuration.showDragZones, false);
  });

  it('folds the title bar to a LIP, which is the way back', () => {
    press('.dc-titlebar-fold');
    const bar = root.querySelector('.dc-titlebar') as HTMLElement;
    assert.equal(bar.classList.contains('dc-collapsed'), true);
    // The hamburger went with it. That is precisely why there is a
    // lip: without one, hiding this bar would be a one-way door.
    assert.equal(root.querySelector('.dc-titlebar-menu'), null);
    press('.dc-titlebar-lip');
    assert.equal(root.querySelector('.dc-titlebar-lip'), null);
    assert.notEqual(root.querySelector('.dc-titlebar-menu'), null);
  });

  it('restores both bars with both folded, WITHOUT the grid menu', () => {
    // Layout left the grid's menu by the user's direction
    // (2026-09-25): that menu is about the data. With both bars folded
    // the way back is the title bar's lip, then its hamburger -- and
    // the grid's menu offers neither toggle.
    press('.dc-zone-fold');
    press('.dc-titlebar-fold');
    rightClick();
    const offered = [...root.querySelectorAll('.dc-menu-label')]
      .map((e) => e.textContent ?? '');
    assert.equal(offered.some((l) => /Drag Zones|Title Bar|^Layout$/.test(l)),
      false, offered.join(', '));
    // Dismissed the way a user would; left open, the hamburger's
    // press would SHUT this menu rather than open its own.
    dom.window.document.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    assert.equal(menuItems().length, 0, 'the grid menu did not close');
    press('.dc-titlebar-lip');
    assert.notEqual(root.querySelector('.dc-titlebar-menu'), null);
    // the zones' way back is in the title bar, beside its fold
    press('.dc-titlebar-zones');
    assert.equal(zoneBar().hidden, false);
  });

  it('folds the title bar from the PROPERTIES editor, not only from its chevron', async () => {
    // The setting lives in General Properties, beside the rest of "what is on screen". Applying
    // the editor replaces the whole configuration, so this is also the check that the flag and
    // the DOM cannot drift apart. (The drag zones have no setting there: they fold from their
    // own chevron, the user, 2026-09-30.)
    hamburger();
    pick('Properties...');
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const labels = [...overlay.querySelectorAll('.dc-check-label')].map((l) => l.textContent);
    assert.ok(!labels.includes('Show drag zones'), labels.join(', '));
    // BY ITS OWN LABEL: a `.dc-field` holds several inputs
    const box = [...overlay.querySelectorAll('.dc-check')]
      .find((l) => l.querySelector('.dc-check-label')?.textContent === 'Show title bar')
      ?.querySelector('input') as HTMLInputElement;
    assert.equal(box.checked, true);
    box.checked = false;
    box.dispatchEvent(new dom.window.Event('change'));
    (
      [...overlay.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Apply',
      ) as HTMLButtonElement
    ).click();
    // Apply compiles the draft first, so the cube answers a tick later.
    for (let i = 0; i < 5; i += 1) await new Promise((r) => setTimeout(r, 0));
    assert.equal(app.configuration.showTitleBar, false);
    assert.equal(root.querySelector('.dc-titlebar')?.classList.contains('dc-collapsed'), true,
      'the DOM and the flag disagree');
    // and the way back is on screen: the lip
    assert.notEqual(root.querySelector('.dc-titlebar-lip'), null);
  });


  it('offers no folds in the hamburger: the bars fold from their own chevrons (the user, 2026-09-30)', () => {
    hamburger();
    const labels = menuItems().map((i) =>
      i.querySelector('.dc-menu-label')?.textContent ?? '');
    assert.equal(labels.some((l) => /Drag Zones|Title Bar/.test(l)), false, labels.join(', '));
  });

  it('switches to Ad Hoc Analysis from the hamburger, and back as it was', async () => {
    hamburger();
    pick('Ad Hoc Analysis');
    const settle = async (): Promise<void> => {
      for (let i = 0; i < 20 && (app.adhoc?.busy ?? false); i++) {
        await new Promise((r) => setTimeout(r, 0));
      }
      await new Promise((r) => setTimeout(r, 0));
    };
    await settle();
    assert.ok(app.adhoc, 'the mode is on');
    const middle = root.querySelector('.dc-app-middle') as HTMLElement;
    assert.equal(middle.hidden, true, "the cube's grid waits");
    assert.equal((root.querySelector('.dc-zone-bar') as HTMLElement).hidden, true);
    assert.ok(root.querySelector('.dc-adhoc .dc-adhoc-pov'));
    // The host's hierarchy is the outline; the opening grid is its top.
    assert.deepEqual(app.adhoc.session.grid.rows.map((a) => a.dimension), ['Geography']);
    assert.deepEqual(app.adhoc.view?.table.columns[0]?.values, ['Geography']);
    // Checked while on; choosing it again leaves.
    hamburger();
    // by its own label: View's entry holds the submenu's words too
    const entry = menuItems().find((i) => i.querySelector('.dc-menu-label')?.textContent === 'Ad Hoc Analysis');
    assert.equal(entry?.getAttribute('aria-checked'), 'true');
    pick('Ad Hoc Analysis');
    assert.equal(app.adhoc, null);
    assert.equal(root.querySelector('.dc-adhoc'), null);
    assert.equal(middle.hidden, false);
    assert.equal((root.querySelector('.dc-zone-bar') as HTMLElement).hidden, false);
  });

  it('a disposed app no longer answers the shortcuts', () => {
    const undoKey = () => dom.window.document.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', { key: 'z', ctrlKey: true, bubbles: true }));
    undoKey();
    const heard = statuses.length;
    assert.ok(heard > 0, 'the live app answers Ctrl-Z');
    app.dispose();
    undoKey();
    assert.equal(statuses.length, heard, 'a disposed app spoke');
  });

  it('shows the row grouping as chips, in BOTH surfaces', () => {
    // The bar over the grid and the sidebar's own section are two
    // renderings of one state, so a grouping shows in both -- and a
    // test that counted chips across the document would now count
    // every one of them twice.
    const chipsIn = (where: string): (string | undefined)[] =>
      [...root.querySelectorAll(`${where} .dc-zone-rows .dc-chip`)]
        .map((c) => (c as HTMLElement).dataset['column']);
    assert.deepEqual(chipsIn('.dc-zone-bar'), ['region']);
    assert.deepEqual(chipsIn('.dc-tool-panel-zones'), ['region']);
  });

  it('a column dropped in the row zone regroups the cube', async () => {
    setHeaderDrag({ column: 'desk' }, root.querySelector('.dc-zone-rows'));
    (
      root.querySelector('.dc-zone-rows') as HTMLElement
    ).dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.deepEqual(app.snapshot.rows, ['region', 'desk']);
  });

  it('a measure dropped in the row zone is REFUSED', () => {
    setHeaderDrag({ column: 'notional' }, root.querySelector('.dc-zone-rows'));
    (
      root.querySelector('.dc-zone-rows') as HTMLElement
    ).dispatchEvent(new dom.window.Event('drop', { bubbles: true }));
    assert.deepEqual(app.snapshot.rows, ['region']);
  });

  it('hides every control for the grid alone, and brings them back, from the right-click menu', () => {
    // the user, 2026-10-01: a DataCube-owned mode for the most room; "right click only" back
    const queries = engine.sql.length;
    rightClick();
    pick('Hide Controls');
    assert.equal(app.controlsHidden, true);
    assert.ok(root.classList.contains('dc-controls-hidden'));
    rightClick();
    pick('Show Controls');
    assert.equal(app.controlsHidden, false);
    assert.ok(!root.classList.contains('dc-controls-hidden'));
    assert.equal(engine.sql.length, queries, 'view state: no query ran');
  });

  it('opens with the controls hidden when the host asks', () => {
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const own = new CubeApp(host, SNAPSHOT, { engine, planner, controlsHidden: true });
    assert.equal(own.controlsHidden, true);
    assert.ok(host.classList.contains('dc-controls-hidden'));
    own.dispose();
  });

  it('opens the context menu on right-click — the audit found it unreachable', () => {
    const grid = root.querySelector('.dc-app-grid') as HTMLElement;
    grid.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
    assert.notEqual(
      dom.window.document.querySelector('.dc-menu'),
      null,
      'the menu is in the document',
    );
  });

  /** Answer upstream's export warning. */
  const answer = (label: 'Accept' | 'Decline'): void => {
    const button = [...root.querySelectorAll('.dc-alert-action')]
      .find((b) => b.textContent === label) as HTMLButtonElement | undefined;
    if (!button) throw new Error('no export warning is open');
    button.click();
  };

  it('exports every format, from the right-click menu', () => {
    for (const label of [
      'HTML',
      'Excel (Grid)',
      'CSV (Grid)',
    ]) {
      rightClick();
      pick(label);
      // The data leaves only after the attestation.
      answer('Accept');
    }
    assert.deepEqual(
      downloads.map((d) => d[1]),
      [
        'text/html',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'text/csv',
      ],
    );
    // A real workbook: an OOXML zip (its first bytes are the zip's 'PK'), named .xlsx.
    const workbook = downloads[1]?.[2];
    assert.ok(workbook instanceof Uint8Array && workbook[0] === 0x50 && workbook[1] === 0x4b);
    assert.match(downloads[1]?.[0] ?? '', /\.xlsx$/);
    // Upstream's names: the title and the moment, so nothing is overwritten.
    assert.match(downloads[0]?.[0] ?? '',
      / - (Sun|Mon|Tue|Wed|Thu|Fri|Sat) [A-Z][a-z]{2} \d{2} \d{4} \d{2}_\d{2}_\d{2}\.html$/);
  });

  it('closes the right-click menu when the grid scrolls, as upstream', () => {
    rightClick();
    const scroller = root.querySelector('.dc-scroller') as HTMLElement;
    // The right-click's OWN scroll (a cell brought into view) lands a
    // frame after the menu opens, and must not close it.
    scroller.dispatchEvent(new dom.window.Event('scroll'));
    assert.ok(dom.window.document.querySelector('.dc-menu'), 'closed by its own scroll');
    // Nor the header's, which follows the body sideways.
    (root.querySelector('.dc-app-grid *') as HTMLElement)
      .dispatchEvent(new dom.window.Event('scroll'));
    assert.ok(dom.window.document.querySelector('.dc-menu'), 'closed by the header');
    scroller.scrollTop = 40;
    scroller.dispatchEvent(new dom.window.Event('scroll'));
    assert.equal(dom.window.document.querySelector('.dc-menu'), null);
  });

  it('asks before any data leaves, and Decline sends nothing', () => {
    rightClick();
    pick('CSV (Grid)');
    const warning = root.querySelector('.dc-alert-warning') as HTMLElement;
    assert.match(warning.textContent ?? '', /Confirm you want to proceed with export/);
    assert.match(warning.textContent ?? '', /I attest that I am aware/);
    answer('Decline');
    assert.deepEqual(downloads, []);
    assert.equal(root.querySelector('.dc-alert'), null, 'the warning closed');
  });

  it('emails with no host mailer as upstream does: an unsent .eml draft', () => {
    rightClick();
    const email = menuItems().find((i) =>
      i.querySelector('.dc-menu-label')?.textContent === 'Email') as HTMLElement;
    ([...email.querySelectorAll('.dc-menu-item')].find((i) =>
      i.querySelector('.dc-menu-label')?.textContent === 'CSV (Grid)') as HTMLElement).click();
    answer('Accept');
    const [name, mime, eml] = downloads[0] ?? [];
    assert.match(name ?? '', /\.eml$/);
    assert.equal(mime, 'message/rfc822');
    assert.equal(typeof eml, 'string');
    const text = eml as string;
    // RFC 2045 and 5322: the MIME version first, every line ending CRLF, a subject
    assert.match(text, /^MIME-Version: 1\.0\r\nFrom:\r\nTo:\r\nSubject: \S/);
    assert.ok(!/[^\r]\n/.test(text), 'a bare LF');
    assert.match(text, /Content-Disposition: attachment; filename=".* - .*\.csv"/);
  });

  it('opens the editor from the right-click menu, as DataCube does', () => {
    rightClick();
    pick('Properties...');
    assert.equal(
      (root.querySelector('.dc-app-overlay') as HTMLElement).hidden,
      false,
    );
  });

  it('adds and removes a heatmap from the menu', () => {
    rightClick('.dc-cell:not(.dc-dim)');
    pick('Add Heatmap to total');
    const column = Object.entries(app.configuration.columns).find(
      ([, c]) => c.heatmap,
    );
    assert.notEqual(column, undefined);
    rightClick('.dc-cell:not(.dc-dim)');
    pick('Remove Heatmap');
    assert.equal(
      Object.values(app.configuration.columns).some((c) => c.heatmap),
      false,
    );
  });

  it('a right-click on a TREE cell resolves the dimension at that level', () => {
    // Their menu does the same, from the node's level: right-click
    // EMEA under region and the filter entries are about region.
    // Row 1, not row 0: the grand total's path is empty, so it
    // names no dimension -- and every column-specific entry
    // correctly greys out there.
    const treeCell = root
      .querySelectorAll('.dc-row')[1]
      ?.querySelector('.dc-cell.dc-tree') as HTMLElement;
    treeCell.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
    assert.ok(
      menuItems().some((i) =>
        (i.querySelector('.dc-menu-label')?.textContent ?? '').startsWith(
          'Add Filter: region =',
        ),
      ),
      menuItems()
        .map((i) => i.querySelector('.dc-menu-label')?.textContent)
        .join(' | '),
    );
  });

  it('the grand total row names no dimension, and greys what needs one', () => {
    const totalCell = root
      .querySelector('.dc-row .dc-cell.dc-tree') as HTMLElement;
    totalCell.dispatchEvent(
      new dom.window.MouseEvent('contextmenu', { bubbles: true }),
    );
    const live = menuItems().filter(
      (i) => i.getAttribute('aria-disabled') !== 'true',
    );
    assert.equal(
      live.some(
        (i) =>
          (i.querySelector('.dc-menu-label')?.textContent ?? '') === 'Hide',
      ),
      false,
      'Hide with nothing to hide must not be actionable',
    );
  });

  it('a right-click on a CELL still knows its column', () => {
    // Without data-column on body cells the menu lost every
    // column-specific entry, which is most of the menu.
    rightClick('.dc-cell');
    assert.ok(
      menuItems().some((i) => i.textContent === 'Ascending'),
      'no sort entries',
    );
  });

  it('offers the cube file only when the host says where its rows come from', () => {
    rightClick();
    const exportMenu = menuItems().find((i) =>
      i.querySelector('.dc-menu-label')?.textContent === 'Export') as HTMLElement;
    const entry = [...exportMenu.querySelectorAll('.dc-menu-item')].find((i) =>
      i.querySelector('.dc-menu-label')?.textContent === 'Cube File (JSON)') as HTMLElement;
    assert.ok(entry, 'no Cube File entry');
    assert.ok(entry.classList.contains('dc-disabled'), 'offered without a source to name');
    assert.equal(app.cubeDocument('x'), undefined);
  });

  it('writes the cube down: its definition, what the user set, the open rows -- never its data', async () => {
    root.replaceChildren();
    const withSource = new CubeApp(root, SNAPSHOT, {
      engine,
      planner,
      cubeSource: {
        _type: 'file', name: 'trades.csv', format: 'csv', size: 10, sha256: 'ab',
        columns: [{ name: 'region', type: 'String' }],
      },
      download: (n, m, t) => downloads.push([n, m, t]),
    });
    await withSource.open();
    await withSource.applyConfiguration({ maxRows: 123 });
    const doc = withSource.cubeDocument('mine');
    assert.ok(doc);
    assert.equal(doc.kind, 'datacube.cube');
    assert.deepEqual(doc.query.rows, ['region']);
    assert.deepEqual(doc.query.measures, SNAPSHOT.measures);
    assert.equal(doc.configuration['maxRows'], 123, 'what the user set');
    assert.equal(doc.configuration['showTitleBar'], undefined, 'a default is not written down');
    assert.equal('source' in doc.query, false, 'the relation is derived from the source on open');
    assert.doesNotMatch(JSON.stringify(doc), /"values"/, 'no rows in a saved cube');
    // and from the menu, as a file: no attestation, since no rows leave
    rightClick();
    pick('Cube File (JSON)');
    const [name, mime, text] = downloads.at(-1) ?? [];
    assert.equal(mime, 'application/json');
    assert.match(name ?? '', /\.json$/);
    assert.match(typeof text === 'string' ? text : '', /"kind":"datacube\.cube"/);
  });

  it('the columns PANEL follows the grid order, not the declared one', async () => {
    // The panel listed the cube's declared columns, so reordering a
    // header moved the column on screen and left the panel beside it
    // saying something else -- and the panel is the list people read
    // to find a column. Tested here rather than through a browser
    // drag: the drag is covered by the grid's own DOM tests, and what
    // broke was this hand-off.
    const named = () => [...root.querySelectorAll('.dc-tool-panel-row')]
      .map((e) => (e as HTMLElement).dataset['column']);
    const before = named();
    // The row dimensions are in their own section now, so the
    // columns section lists what the GRID shows.
    assert.ok(before.length >= 2, `only ${before.length} columns listed`);
    assert.equal(before.includes('region'), false,
      'a row group is in the Row Groups section, not the column list');

    const moved = [before[before.length - 1], ...before.slice(0, -1)]
      .filter((n): n is string => n !== undefined);
    await app.applyConfiguration({ columnOrder: moved });

    assert.deepEqual(named(), moved,
      'the panel must read in the order the grid does');
  });

  it('offers each named dimension in the title bar menu', () => {
    hamburger();
    pick('Geography');
    // Starts at ONE level: opening it fully would fetch desk-level
    // groups for every region before anyone asked.
    assert.deepEqual(app.snapshot.rows, ['region']);
  });

  it('opens the editor, and Cancel closes it', () => {
    rightClick();
    pick('Properties...');
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    assert.equal(overlay.hidden, false);
    assert.notEqual(overlay.querySelector('.dc-editor'), null);
    (
      [...overlay.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Cancel',
      ) as HTMLButtonElement
    ).click();
    assert.equal(overlay.isConnected, false, 'Cancel left the window open');
    assert.equal(root.querySelector('.dc-app-overlay'), null);
  });

  it('Properties... from a HEADER opens Column Properties on that column', () => {
    rightClick('.dc-th[data-column="total"]');
    pick('Properties...');
    const win = root.querySelector('[data-window="Properties"]') as HTMLElement;
    const active = win.querySelector('.dc-editor-tab[aria-selected="true"]');
    assert.equal(active?.textContent, 'Column Properties');
    // `total` is a measure over `notional`: the panel shows the column.
    // the column shown: the one marked in Column Properties' list
    assert.equal(win.querySelector<HTMLElement>('.dc-pe-col.dc-on')?.dataset['column'], 'notional');
  });

  it('Properties... from a CELL opens Column Properties on its column too (the user, 2026-09-30)', () => {
    rightClick('.dc-row [data-column="total"]');
    pick('Properties...');
    const win = root.querySelector('[data-window="Properties"]') as HTMLElement;
    const active = win.querySelector('.dc-editor-tab[aria-selected="true"]');
    assert.equal(active?.textContent, 'Column Properties');
    // the column shown: the one marked in Column Properties' list
    assert.equal(win.querySelector<HTMLElement>('.dc-pe-col.dc-on')?.dataset['column'], 'notional');
  });

  it('keeps several windows open at once, as upstream\'s layout', () => {
    app.openEditor();
    app.openFilters();
    const titles = [...root.querySelectorAll('.dc-app-overlay')]
      .map((w) => (w as HTMLElement).dataset['window']);
    assert.deepEqual(titles.sort(), ['Filters', 'Properties']);
  });

  it('reopening an open window RAISES it, keeping its draft', () => {
    app.openEditor();
    const first = root.querySelector('[data-window="Properties"]') as HTMLElement;
    const body = first.querySelector('.dc-editor');
    app.openFilters();
    app.openEditor();
    const again = root.querySelector('[data-window="Properties"]') as HTMLElement;
    assert.equal(again, first);
    assert.equal(again.querySelector('.dc-editor'), body, 'the draft was rebuilt');
    const filters = root.querySelector('[data-window="Filters"]') as HTMLElement;
    assert.ok(Number(again.style.zIndex) > Number(filters.style.zIndex));
  });

  it('closing one window leaves the others', () => {
    app.openEditor();
    app.openFilters();
    (root.querySelector('[data-window="Filters"] .dc-overlay-close') as HTMLElement)
      .click();
    assert.ok(root.querySelector('[data-window="Properties"]'));
    assert.equal(root.querySelector('[data-window="Filters"]'), null);
  });

  it('an older Properties draft does not undo a filter applied meanwhile', async () => {
    app.openEditor();
    const props = root.querySelector('[data-window="Properties"]') as HTMLElement;
    // Meanwhile, in the Filter window...
    app.openFilters();
    const filters = root.querySelector('[data-window="Filters"]') as HTMLElement;
    (filters.querySelector('.dc-filter-btn') as HTMLButtonElement).click();
    const value = filters.querySelector('input.dc-filter-value') as HTMLInputElement;
    value.value = 'EMEA';
    value.dispatchEvent(new dom.window.Event('change'));
    (filters.querySelector('.dc-filter-apply') as HTMLButtonElement).click();
    for (let i = 0; i < 5; i += 1) await new Promise((r) => setTimeout(r, 0));
    assert.ok(app.snapshot.filter, 'the filter did not apply');
    // ...then Properties, opened BEFORE it, applies a change of its own.
    [...props.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const limit = [...props.querySelectorAll('.dc-field')]
      .find((f) => f.querySelector('.dc-field-label')?.textContent === 'Row Limit:')
      ?.querySelector('input') as HTMLInputElement;
    limit.value = '42';
    limit.dispatchEvent(new dom.window.Event('change'));
    ([...props.querySelectorAll('.dc-editor-footer button')]
      .find((b) => b.textContent === 'Apply') as HTMLButtonElement).click();
    for (let i = 0; i < 5; i += 1) await new Promise((r) => setTimeout(r, 0));
    assert.equal(app.snapshot.maxRows, 42);
    assert.ok(app.snapshot.filter, 'the older draft put the cube back');
  });

  it('opens the filter editor showing the filter ALREADY in force', () => {
    // Without this the editor opens empty on a filtered cube, and the
    // user's first change writes that emptiness back -- silently
    // dropping a filter visible on the grid behind the dialog.
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const filtered = new CubeApp(
      host,
      {
        ...SNAPSHOT,
        filter: {
          kind: 'condition',
          column: 'region',
          operator: 'equal',
          value: 'EMEA',
        },
      },
      { engine, planner },
    );
    filtered.openFilters();
    const editor = host.querySelector('.dc-filters') as HTMLElement;
    assert.notEqual(editor, null);
    const values = [...editor.querySelectorAll('select')].map((s) => s.value);
    assert.ok(values.includes('region'), `columns offered: ${values}`);
    assert.ok(values.includes('equal'), `operators offered: ${values}`);
    const text = editor.querySelector(
      'input[type="text"]',
    ) as HTMLInputElement;
    assert.equal(text.value, 'EMEA');
  });

  it('a heatmap set on a measure reaches its PIVOTED leaves', async () => {
    // The bug this replaced looked the spec up by leaf name, so a
    // heatmap on `notional` never matched `2021__|__notional` and a
    // pivoted cube showed nothing -- the same mistake the formats
    // had. And it painted the cells that existed at the time, which
    // a virtualised grid throws away on the next scroll.
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const pivoted = new CubeApp(
      host,
      { ...SNAPSHOT, pivotOn: ['desk'] },
      {
        engine,
        planner,
        configuration: {
          ...DEFAULT_CONFIGURATION,
          columns: { total: { heatmap: { from: '#ffffff', to: '#ff0000' } } },
        },
      },
    );
    await pivoted.open();
    const painted = [...host.querySelectorAll('.dc-cell')].filter(
      (c) => (c as HTMLElement).style.backgroundColor !== '',
    );
    assert.ok(painted.length > 0, 'no cell was painted');
  });

  it('lists a pivoted measure as the columns the pivot MADE of it', async () => {
    // The panel said "notional" once while the grid showed five of
    // it. ag-grid's own tool panel nests the pivot result columns
    // under a group per value; this lists them under the measure
    // they came from, labelled by the values alone, because the
    // measure is named by the row above.
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const pivoted = new CubeApp(host, PIVOTED, {
      engine: new PivotEngine(),
      planner: new EchoPlanner(),
    });
    await pivoted.open();
    const rows = [...host.querySelectorAll('.dc-tool-panel-row')]
      .map((r) => ({
        column: (r as HTMLElement).dataset['column'],
        label: r.querySelector('.dc-tool-panel-label')?.textContent,
        child: r.classList.contains('dc-tool-panel-child'),
        locked: r.querySelector<HTMLInputElement>('.dc-tool-panel-show')
          ?.disabled,
      }));
    const children = rows.filter((r) => r.child);
    // The plan's columns: each value, then the measure's Total (the
    // default configuration shows it; it is a column of the same query).
    assert.deepEqual(
      children.map((c) => [c.column, c.label]),
      [['A__|__total', 'A'], ['B__|__total', 'B'], [pivotTotalColumn('total'), 'Total']],
    );
    // And the pivot KEY is not in this list at all: its values ARE
    // the column headers, so it cannot also be a column. It is in
    // the sidebar's Column Labels section, which is where it can be
    // dragged out of.
    assert.equal(rows.some((r) => r.column === 'desk'), false);
    const labels = [...host.querySelectorAll(
      '.dc-tool-panel-zones .dc-zone-columns .dc-chip')]
      .map((c) => (c as HTMLElement).dataset['column']);
    assert.deepEqual(labels, ['desk']);
  });

  it('hiding a pivoted column does not take it out of the QUERY', async () => {
    // Unticking `A__|__total` once narrowed the next query (the cast was
    // read off the leaves the grid was SHOWING), so the column left the
    // data as well as the screen. The columns come from the values
    // query now, whatever is hidden.
    const host = dom.window.document.createElement('div');
    dom.window.document.body.append(host);
    const engine = new PivotEngine();
    const pivoted = new CubeApp(host, PIVOTED, {
      engine,
      planner: new EchoPlanner(),
      configuration: {
        ...DEFAULT_CONFIGURATION,
        columns: { 'A__|__total': { hidden: true } },
      },
    });
    await pivoted.open();
    const level = engine.sql.find((s) => s.includes('"function":"groupBy"')) ?? '';
    assert.match(level, /"A__\|__total"/, 'the hidden column is still asked for');
    assert.match(level, /"B__\|__total"/);
    // Hidden on screen, all the same.
    const shown = [...host.querySelectorAll('.dc-th[data-column]')]
      .map((e) => (e as HTMLElement).dataset['column']);
    assert.equal(shown.includes('A__|__total'), false);
    assert.equal(shown.includes('B__|__total'), true);
  });

  it('the grid header carries the column name, so the menu knows what was clicked', () => {
    const header = root.querySelector('.dc-th[data-column]') as HTMLElement;
    assert.notEqual(header, null);
    assert.ok(typeof header.dataset['column'] === 'string');
  });

  it('Ctrl+E opens the editor', () => {
    dom.window.document.dispatchEvent(
      new dom.window.KeyboardEvent('keydown', {
        key: 'e',
        ctrlKey: true,
        bubbles: true,
      }),
    );
    assert.equal(
      (root.querySelector('.dc-app-overlay') as HTMLElement).hidden,
      false,
    );
  });

  it('folds the configuration into the query on every refresh', async () => {
    // Once, here, so a setting that shapes the query cannot reach the
    // engine through one path and not another.
    rightClick();
    pick('Properties...');
    const overlay = root.querySelector('.dc-app-overlay') as HTMLElement;
    [...overlay.querySelectorAll('.dc-editor-tab')]
      .find((b) => b.textContent === 'General Properties')
      ?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }));
    const limit = [...overlay.querySelectorAll('.dc-field')]
      .find(
        (f) => f.querySelector('.dc-field-label')?.textContent === 'Row Limit:',
      )
      ?.querySelector('input') as HTMLInputElement;
    limit.value = '42';
    limit.dispatchEvent(new dom.window.Event('change'));
    (
      [...overlay.querySelectorAll('.dc-editor-footer button')].find(
        (b) => b.textContent === 'Apply',
      ) as HTMLButtonElement
    ).click();
    for (let i = 0; i < 5; i += 1) await new Promise((r) => setTimeout(r, 0));
    assert.equal(app.snapshot.maxRows, 42);
  });
});

describe('the bar says what you are looking at, and nothing else', () => {
  let dom: JSDOM;
  let root: HTMLElement;

  beforeEach(() => {
    dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
      (fn: () => void) => {
        fn();
        return 0;
      };
    root = dom.window.document.getElementById('r') as HTMLElement;
  });

  /** Let the toggle's async click handler finish. */
  const flush = async (): Promise<void> => {
    for (let i = 0; i < 5; i += 1) {
      await new Promise((done) => setTimeout(done, 0));
    }
  };

  it('names the REPORT in the title bar', async () => {
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      configuration: { ...DEFAULT_CONFIGURATION, reportTitle: 'Trades' },
    });
    await app.open();
    assert.equal(
      root.querySelector('.dc-titlebar-title')?.textContent,
      'Trades',
    );
  });

  it('states the result and its cost in the STATUS bar', async () => {
    // The row count, the column count and the elapsed time went to
    // the host through `onStatus` and were rendered above the grid,
    // while the status bar said "Rows: 2" -- two readouts of one
    // fact, the fuller one in the wrong place.
    const statuses: string[] = [];
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      onStatus: (text) => statuses.push(text),
    });
    await app.open();
    const timing = root.querySelector('.dc-status-timing')?.textContent ?? '';
    assert.match(timing, /^2 rows × \d+ cols in \d+ms$/);
    // The same line, so a host's figure and the screen's cannot
    // drift apart.
    assert.equal(statuses.at(-1), timing);
    assert.equal(
      root.querySelector('.dc-app-stats')?.textContent?.includes('Rows:'),
      false,
    );
  });

  it('puts what you can DO at one end and what is TRUE at the other',
    async () => {
      // DataCube's own bar is `justify-between`: two link buttons at
      // the left -- Properties, then Filter -- and its readouts at
      // the right. Ours had one link with every figure crowded after
      // it, and both editors were reachable only from the grid's
      // right-click menu, two levels down.
      const app = new CubeApp(root, SNAPSHOT, {
        engine: new StubEngine(),
        planner: new StubPlanner(),
        hostStatus: (slot) => {
          slot.textContent = 'local';
        },
      });
      await app.open();
      const actions = root.querySelector('.dc-app-stats .dc-status-actions');
      const readout = root.querySelector('.dc-app-stats .dc-status-readout');
      assert.notEqual(actions, null);
      assert.notEqual(readout, null);
      // Properties FIRST, as theirs is.
      assert.deepEqual(
        [...(actions?.querySelectorAll('.dc-status-link') ?? [])]
          .map((b) => b.textContent?.replace(/^\W+\s*/, '')),
        ['Properties', 'Filter'],
      );
      // And every figure on the other side, the backend's word last.
      assert.notEqual(readout?.querySelector('.dc-status-timing'), null);
      assert.equal(
        readout?.querySelector('.dc-status-host')?.textContent,
        'local',
      );
      assert.equal(actions?.querySelector('.dc-status-timing'), null);
    });

  it('opens the properties editor from the status bar', async () => {
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
    });
    await app.open();
    assert.equal(root.querySelector('.dc-editor'), null);
    (root.querySelector('.dc-status-properties') as HTMLButtonElement)
      .click();
    assert.notEqual(root.querySelector('.dc-editor'), null,
      'the Properties link opened nothing');
  });

  it("MOVES the host's readout into the status bar, once", async () => {
    const marker = dom.window.document.createElement('span');
    marker.id = 'hoststatus';
    marker.textContent = 'planning…';
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      hostStatus: (slot) => slot.append(marker),
    });
    // BEFORE the first render: a cube whose first query fails never
    // renders a status bar, and that is exactly when the host has
    // something to say.
    assert.equal(
      root.querySelector('.dc-app-stats #hoststatus'),
      marker,
      'the host slot was not filled at build time',
    );
    await app.open();
    // Still the SAME node, and only one of it: the host keeps
    // writing to whichever node is on screen.
    assert.equal(root.querySelector('.dc-app-stats #hoststatus'), marker);
    assert.equal(root.querySelectorAll('#hoststatus').length, 1);
    assert.equal(marker.textContent, 'planning…');
  });

  it('puts the row and column zones in ONE bar, side by side', async () => {
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new StubEngine(),
      planner: new StubPlanner(),
      showColumnZone: true,
    });
    await app.open();
    // Both halves of one strip rather than two stacked strips: the
    // zones must stay visible drop targets (you cannot drag a column
    // into a menu), and two bars cost 66px of the viewport.
    const panel = root.querySelector('.dc-pivot-panel');
    assert.notEqual(panel, null);
    const zones = [...(panel?.children ?? [])].filter((c) =>
      c.classList.contains('dc-zone'),
    );
    assert.deepEqual(
      zones.map((z) => (z as HTMLElement).dataset['zone']),
      ['rows', 'columns'],
    );
  });

  it('says WHEN the snapshot was taken and how big it is', async () => {
    // The banner that said "trades — frozen at 14:02:11 — 29 rows"
    // is gone, and the toggle beside it said only "Snapped". Rule 1
    // of snap mode is that what you are looking at is never
    // inferable, so the detail moved into the toggle's tooltip.
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new CountingEngine(),
      planner: new StubPlanner(),
      snapTarget: { table: 'TRADES_SNAP', source: element('TRADES_SNAP'), conversions: [], planner: new StubPlanner() },
    });
    await app.open();
    const toggle = root.querySelector('.dc-titlebar-toggle') as HTMLButtonElement;
    assert.equal(toggle.textContent, 'Live');
    assert.match(toggle.title, /Live data/);

    toggle.click();
    await flush();

    assert.equal(toggle.textContent, 'Snapped');
    assert.match(toggle.title, /frozen at \d/);
    assert.match(toggle.title, /29 rows/);
    assert.match(toggle.title, /Click to go live/);
  });

  it('says Snapped, and offers nothing, over rows already copied into the tab', async () => {
    // An opened file or generated rows cannot move while the person works:
    // "Live data, which may move" was false, and a click copied a copy (or,
    // over a file's model with no snap table, claimed a snap that failed).
    const engine = new CountingEngine();
    const app = new CubeApp(root, SNAPSHOT, {
      engine,
      planner: new StubPlanner(),
      snapTarget: { table: 'TRADES_SNAP', source: element('TRADES_SNAP'), conversions: [], planner: new StubPlanner() },
      heldCopy: { label: 'trades.csv', takenAt: new Date(2026, 8, 29, 9, 30), rowCount: 1234 },
    });
    await app.open();
    const toggle = root.querySelector('.dc-titlebar-toggle') as HTMLButtonElement;
    assert.equal(toggle.textContent, 'Snapped');
    assert.ok(toggle.classList.contains('dc-on'));
    assert.equal(toggle.getAttribute('aria-disabled'), 'true');
    assert.match(toggle.title, /^trades\.csv — copied into this tab at \d/);
    assert.match(toggle.title, /1,234 rows/);

    const before = engine.sql.length;
    toggle.click();
    await flush();
    assert.equal(engine.sql.length, before, 'a click ran nothing');
    assert.equal(toggle.textContent, 'Snapped');
    assert.equal(app.controller.snaps.isSnapped, false);
  });

  it('shows the receipt of what answered, and an open Receipts window follows the plane', async () => {
    class SigningEngine extends CountingEngine {
      override async answer(sql: string, epoch: number): Promise<ResultTable> {
        const r = await super.answer(sql, epoch);
        return { ...r, receipt: { plane: 'warehouse', where: 'the warehouse at wh:9', as: 'rita', statementId: 'abcdef12-0000' } };
      }
    }
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new SigningEngine(),
      planner: new StubPlanner(),
      snapTarget: { table: 'TRADES_SNAP', source: element('TRADES_SNAP'), conversions: [], planner: new StubPlanner() },
    });
    await app.open();
    const chip = () => root.querySelector('.dc-status-receipt') as HTMLButtonElement;
    assert.equal(chip().textContent, 'the warehouse at wh:9 · rita · #abcdef12');
    assert.match(chip().title, /Server statement id: abcdef12-0000/);

    app.openReceipts();
    const body = () => root.querySelector('.dc-receipts')?.textContent ?? '';
    assert.match(body(), /Ran on the warehouse at wh:9 as rita/);

    (root.querySelector('.dc-titlebar-toggle') as HTMLButtonElement).click();
    await flush();
    assert.match(chip().textContent ?? '', /^this tab's copy/);
    assert.match(body(), /Read from the snap copied into this tab/);
  });

  it('says Live, and offers no snap, where there is nowhere to snap into', async () => {
    const app = new CubeApp(root, SNAPSHOT, {
      engine: new CountingEngine(),
      planner: new StubPlanner(),
    });
    await app.open();
    const toggle = root.querySelector('.dc-titlebar-toggle') as HTMLButtonElement;
    assert.equal(toggle.textContent, 'Live');
    assert.equal(toggle.getAttribute('aria-disabled'), 'true');
    assert.match(toggle.title, /no store in this tab to snap into/);
    toggle.click();
    await flush();
    assert.equal(toggle.textContent, 'Live');
  });
});

describe('applying the editor', () => {
  it("keeps the Dimensions tab's hierarchies on the configuration", async () => {
    const { mergeDraft } = await import('../src/app.ts');
    const base = { snapshot: SNAPSHOT, config: DEFAULT_CONFIGURATION, dimensions: [] };
    const geo = [{ name: 'Geography', columns: ['region', 'desk'] }];
    const merged = mergeDraft(base, base, { ...base, dimensions: geo });
    assert.deepEqual(merged.config.dimensions, geo);
    // Untouched, nothing is written: the host's stay in charge.
    assert.equal(mergeDraft(base, base, base).config.dimensions, undefined);
  });
});
