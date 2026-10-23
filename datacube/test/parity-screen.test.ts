// SCREEN AND PYTHON, ONE EDIT, ONE DOCUMENT (docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md §11, part 1): each case
// of test/fixtures/parity/parity.json done by a person on the screen -- a new grid over the frame, then its actions
// through the menus, the header and Column Properties, the app as a page opens it (app/datacube-app.ts: rebuilt, then
// CubeApp) over legend-lite's planner and a real DuckDB -- and the cube document the grid writes held to the case's
// `cube`. python/tests/test_headless.py does each case through Python's Page and Grid and holds Python's document to
// the same `cube`: so the screen and Python write one document for one edit, and the cases are tested at the edges a
// person and a program use, which the command program leaves as they are.
//
// THE COMMANDS TOO (docs/DATACUBE_COMMANDS_STAGE1_2026_10_18.md, step 4): an action that is one edit on both sides --
// a sort added, a heatmap, a hidden column, a calculated column, a grouping or a pivot on one column, a column's
// Aggregation, kind or weighted average (Column Properties' Apply) -- holds the grid command (grid-commands.ts) both
// must build for it, `command`: the screen's, told by onCommand, and Python's, which its page records. An action that
// is two steps on the screen and one call in Python (a grouping on two columns: two menu entries), or a different call
// (a header click; a cell's filter added, where Python sets the filter whole), is held to the document alone.
//
// The documents are regenerated, deliberately, by running this test with PARITY_UPDATE naming the fixture to write:
//   RUNFILES_DIR=/ WASM_PLANNER=<repo>/bazel-bin/wasm/planner_dir PARITY=<repo>/datacube/test/fixtures/parity/parity.json \
//   DUCKDB_EXTENSIONS=... PARITY_UPDATE=<repo>/datacube/test/fixtures/parity/parity.json node ... test/parity-screen.test.ts
// (the paths without their leading slash, as runfiles take them) -- and the diff read before it is committed.

import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { JSDOM } from 'jsdom';
import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { toJson, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { CubeApp } from '../src/app.ts';
import { cubeToJson, frameSource, rebuilt } from '../src/cube-document.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import type { ColumnSpec } from '../src/snapshot.ts';
import { sourceColumns } from '../src/source-columns.ts';
import { columnLabel, valueLabel } from '../src/ui/menu.ts';
import { ingestFile } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';

/** An action, as the case file names it: what a person does, and what Python's call is; and, when it is one edit on
 *  both sides (sameCommand), the grid command both build for it. */
type Action = { command?: unknown } & (
  | { readonly do: 'group'; readonly rows: readonly string[] }
  | { readonly do: 'pivot'; readonly columns: readonly string[] }
  | { readonly do: 'addSort'; readonly column: string; readonly direction: 'asc' | 'desc' }
  | { readonly do: 'headerSort'; readonly column: string; readonly direction: 'asc' | 'desc' }
  | { readonly do: 'aggregate'; readonly column: string; readonly fn: string }
  | { readonly do: 'kind'; readonly column: string; readonly kind: 'dimension' | 'measure' }
  | { readonly do: 'heatmap'; readonly column: string; readonly from: string; readonly to: string }
  | { readonly do: 'hide'; readonly column: string }
  | { readonly do: 'filter'; readonly column: string; readonly operator: 'equal'; readonly value: string | number; readonly cell?: string }
  | { readonly do: 'wavg'; readonly column: string; readonly weight: string }
  | { readonly do: 'calculate'; readonly name: string; readonly expression: string });

/** Whether an action is one edit on the screen and one call in Python, the same grid command on both sides. */
function sameCommand(a: Action): boolean {
  switch (a.do) {
    case 'group':
      return a.rows.length === 1;
    case 'pivot':
      return a.columns.length === 1;
    case 'addSort':
    case 'heatmap':
    case 'hide':
    case 'calculate':
    case 'aggregate':
    case 'kind':
    case 'wavg':
      return true;
    case 'headerSort':
    case 'filter':
      return false;
  }
}

interface Case {
  readonly name: string;
  /** A page's second grid: capped at the row limit, as the page caps a grid that is not its only one. */
  readonly capped?: boolean;
  readonly actions: readonly Action[];
  cube: unknown;
}

interface Fixture {
  readonly about: string;
  /** The columns whose values are dates, as ISO text. */
  readonly dates: readonly string[];
  readonly page: string;
  readonly frame: string;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly (string | number)[])[];
  readonly cases: Case[];
}

const fixture = JSON.parse(readFileSync(runfileFromEnv('PARITY'), 'utf8')) as Fixture;
const update = process.env['PARITY_UPDATE'];

let engine: DuckDbEngine;
let planner: LitePlanner;
let source: ValueSpecification;
let columns: ColumnSpec[];

before(async () => {
  // the frame's rows, read into the tab's DuckDB as a picked file is (upload.ts): its model legend-lite's writer's
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  // a date as ISO text, which the file's reader types as a date (as Python's frame types a datetime.date)
  const csv = [fixture.columns.join(','), ...fixture.rows.map((r) => r.join(','))].join('\n');
  const r = await ingestFile(engine, db.files, new File([csv], `${fixture.frame}.csv`), plannerFor('', 'local::RT'));
  planner = plannerFor(r.model, r.runtime);
  source = r.source;
  // the source's columns as the compiler types them now: what Python's gridCube types the frame's as (headless typed)
  columns = await sourceColumns(planner, source);
});

after(async () => {
  if (update) {
    writeFileSync(update, `${JSON.stringify(fixture, null, 2)}\n`);
  }
  await engine?.close();
});

/** A grid on the screen, as a page opens a new one over the frame, and what the person does to it. */
class Screen {
  readonly dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
  readonly statuses: [string, string][] = [];
  /** Each grid command the screen applied, as JSON (onCommand). */
  readonly commands: unknown[] = [];
  readonly app: CubeApp;

  constructor(capped: boolean) {
    (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame = (fn: () => void) => {
      fn();
      return 0;
    };
    // a grid with a height draws its rows, so a cell can be right-clicked
    Object.defineProperty(this.dom.window.HTMLElement.prototype, 'clientHeight', { configurable: true, get: () => 400 });
    const root = this.dom.window.document.getElementById('r') as HTMLElement;
    // a new grid over the frame, as sources.ts makes one (cube-document.ts rebuilt) and Python's gridCube makes one
    const made = rebuilt(undefined, source, columns, fixture.frame, capped);
    this.app = new CubeApp(root, made.snapshot, {
      engine,
      planner,
      configuration: made.configuration,
      cubeSource: frameSource(fixture.frame, columns),
      compact: true,
      showColumnZone: true,
      onStatus: (text, kind) => this.statuses.push([text, kind]),
      onCommand: (command) => this.commands.push(JSON.parse(toJson(command))),
      writeClipboard: () => {},
      download: () => {},
    });
  }

  /** Until the grid has no change in flight, a few quiet turns in a row. */
  async idle(): Promise<void> {
    let quiet = 0;
    for (let i = 0; i < 5000 && quiet < 5; i += 1) {
      await new Promise((r) => setTimeout(r, 0));
      quiet = this.app.busy ? 0 : quiet + 1;
    }
    assert.equal(this.app.busy, false, 'the grid settled');
    const said = this.statuses.filter(([, kind]) => kind === 'warn' || kind === 'error');
    assert.deepEqual(said, [], 'the screen refused nothing and failed nothing');
  }

  header(column: string): HTMLElement {
    const th = this.dom.window.document.querySelector<HTMLElement>(`.dc-th[data-column="${column}"]`);
    assert.ok(th, `the grid shows a header for ${column}`);
    return th;
  }

  /** A right-click on `target`, then the menu entry a person reads as `label`. */
  async menu(target: HTMLElement, label: string): Promise<void> {
    target.dispatchEvent(new this.dom.window.MouseEvent('contextmenu', { bubbles: true, cancelable: true }));
    const items = [...this.dom.window.document.querySelectorAll<HTMLElement>(
      '.dc-menu [role="menuitem"], .dc-menu [role="menuitemcheckbox"]')];
    const item = items.find((i) => (i.querySelector('.dc-menu-label')?.textContent ?? i.textContent) === label);
    assert.ok(item, `the menu offers "${label}"; it offers ${items.map((i) => i.textContent).join(' | ')}`);
    assert.notEqual(item.getAttribute('aria-disabled'), 'true', `"${label}" is live`);
    item.click();
    await this.idle();
  }

  /** Column Properties on `column`'s header, each field set in turn (`[label, value]`), then OK. */
  async properties(column: string, ...fields: readonly (readonly [string, string])[]): Promise<void> {
    await this.menu(this.header(column), 'Properties...');
    const win = this.dom.window.document.querySelector<HTMLElement>('[data-window="Properties"]');
    assert.ok(win, 'the Properties window');
    for (const [field, value] of fields) {
      // the panel draws again after a change (a weight column is offered once the Aggregation takes one)
      const select: HTMLSelectElement | null | undefined = [...win.querySelectorAll('.dc-field')]
        .find((f) => f.querySelector('.dc-field-label')?.textContent === field)?.querySelector('select');
      assert.ok(select, `Column Properties has "${field}"`);
      select.value = value;
      assert.equal(select.value, value, `"${field}" offers ${value}`);
      select.dispatchEvent(new this.dom.window.Event('change'));
      await new Promise((r) => setTimeout(r, 0));
    }
    const ok = [...win.querySelectorAll<HTMLButtonElement>('.dc-editor-footer button')].find((b) => b.textContent === 'OK');
    assert.ok(ok, 'the window has OK');
    ok.click();
    await this.idle();
  }

  /** Until `ready` holds: the editor checks a column live, a moment after the typing stops. */
  async until(ready: () => boolean, what: string): Promise<void> {
    for (let i = 0; i < 500 && !ready(); i += 1) await new Promise((r) => setTimeout(r, 20));
    assert.ok(ready(), what);
  }

  async act(a: Action): Promise<void> {
    switch (a.do) {
      case 'group':
        for (const [i, row] of a.rows.entries()) {
          await this.menu(this.header(row), `${i === 0 ? '' : 'Add '}Vertical Pivot on ${row}`);
        }
        return;
      case 'pivot':
        for (const [i, column] of a.columns.entries()) {
          await this.menu(this.header(column), `${i === 0 ? '' : 'Add '}Horizontal Pivot on ${column}`);
        }
        return;
      case 'addSort':
        return this.menu(this.header(a.column), a.direction === 'asc' ? 'Add Ascending' : 'Add Descending');
      case 'headerSort':
        this.header(a.column).dispatchEvent(new this.dom.window.MouseEvent('click', { bubbles: true }));
        await this.idle();
        // the direction a click sorts in first is the header's (grid-edits.ts firstSortDirection), which the case names
        assert.deepEqual(this.app.snapshot.sorts.find((s) => s.column === a.column)?.direction, a.direction);
        return;
      case 'aggregate':
        return this.properties(a.column, ['Aggregation:', a.fn]);
      case 'kind':
        return this.properties(a.column, ['Column Kind:', a.kind]);
      case 'wavg':
        return this.properties(a.column, ['Aggregation:', 'wavg'], ['Weight column:', a.weight]);
      case 'calculate': {
        // Extended Columns > Add New Column..., its name and expression typed, OK once the compiler's check passes
        await this.menu(this.header(columns[0]!.name), 'Add New Column...');
        const doc = this.dom.window.document;
        const type = (selector: string, text: string): void => {
          const input = doc.querySelector<HTMLInputElement>(selector);
          assert.ok(input, `the column editor has ${selector}`);
          input.value = text;
          input.dispatchEvent(new this.dom.window.Event('input'));
        };
        type('.dc-calc-input-name', a.name);
        type('.dc-calc-input-expr', a.expression);
        const ok = (): HTMLButtonElement | null => doc.querySelector<HTMLButtonElement>('.dc-calc-ok');
        await this.until(() => ok()?.disabled === false,
          `the editor's check passes: ${doc.querySelector('.dc-calc-check')?.textContent ?? ''}`);
        ok()!.click();
        await this.until(() => !this.app.busy && this.app.snapshot.derived.some((d) => d.name === a.name), 'the column is added');
        return this.idle();
      }
      case 'heatmap':
        await this.menu(this.header(a.column), `Add Heatmap to ${columnLabel(a.column)}`);
        // the colours the menu sets are the ones the case gives Python's call
        assert.deepEqual(this.app.configuration.columns[a.column]?.heatmap, { from: a.from, to: a.to });
        return;
      case 'hide':
        return this.menu(this.header(a.column), `Hide ${columnLabel(a.column)}`);
      case 'filter': {
        // the screen's one-click filter is "=" on the cell's own value
        assert.equal(a.operator, 'equal', "a cell's menu filters with = alone");
        const shown = a.cell ?? String(a.value);
        const cell = [...this.dom.window.document.querySelectorAll<HTMLElement>('.dc-cell')]
          .find((c) => c.textContent === shown);
        assert.ok(cell, `the grid shows a cell of ${shown}`);
        await this.menu(cell, `Add Filter: ${a.column} = ${typeof a.value === 'number' ? String(a.value) : valueLabel(a.value)}`);
        return;
      }
      default:
        // an action this driver does not do fails here, never passes as no edit (PARITY_UPDATE would keep it so)
        throw new Error(`parity.json names an action the screen does not do: ${JSON.stringify(a satisfies never)}`);
    }
  }

  dispose(): void {
    this.app.dispose();
    this.dom.window.close();
  }
}

describe('the screen writes the cube document Python writes, case by case (parity.json)', () => {
  for (const c of fixture.cases) {
    it(c.name, async () => {
      const screen = new Screen(c.capped === true);
      try {
        await screen.app.open();
        await screen.idle();
        for (const a of c.actions) {
          const before = screen.commands.length;
          await screen.act(a);
          if (!sameCommand(a)) continue;
          const built = screen.commands.slice(before);
          if (update) {
            assert.equal(built.length, 1, `${a.do}: one edit, one command (${JSON.stringify(built)})`);
            a.command = built[0];
          } else {
            assert.ok(a.command !== undefined, `${a.do} is one edit on both sides: the case holds its command`);
            assert.deepEqual(built, [a.command], `${a.do}: the command the screen built`);
          }
        }
        const doc = screen.app.cubeDocument(fixture.page);
        assert.ok(doc, 'the grid knows its source, so it writes a document');
        const written: unknown = JSON.parse(cubeToJson(doc));
        if (update) {
          c.cube = written;
          return;
        }
        assert.deepEqual(written, c.cube);
      } finally {
        screen.dispose();
      }
    });
  }
});
