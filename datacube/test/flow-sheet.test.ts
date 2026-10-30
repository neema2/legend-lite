// A FLOW SHEET (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL4, FL5, FL6): its layout's rules (layout/flow.ts), what a box
// says it does (flow-boxes.ts), and its page commands -- a flow added, a box placed after the box it reads, moved, taken
// off (what read it live frozen to a copy), what only a board takes refused in words -- read and written whole, the
// rebase's renames, the page read back, and the cubes a page opens none of as it opens (page-document.ts boxCubes).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { cubeOpenedAgain, inputSides, isLive, leavesOf, rawRows, writeCube, type CubeDocument, type CubeSource, type FileSource, type RowsSource } from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { boxInputs, boxNamed, boxSays, jsonFieldOf, pageBoxName, readersOf } from '../src/flow-boxes.ts';
import { add, NEW_SHEET } from '../src/layout/bands.ts';
import {
  BOX_HEIGHT, BOX_WIDTH, EMPTY_FLOW, FlowRefusal, GAP_X, GAP_Y, MARGIN, arranged, containerMoves, flowProblems, framedBy, messageSaid, moveBox, placeAfter,
  removeBox, runStages, type Flow,
} from '../src/layout/flow.ts';
import { liveGrids, resolvedSource } from '../src/live-inputs.ts';
import { applyPageCommand, idsOf, readPageCommand, type PageCommand, type PageDraft } from '../src/page-commands.ts';
import {
  boxCubes, layoutTiles, pageToJson, readPage, unnamedLabel, writePageOf, PageDocumentError, type PageDocument,
} from '../src/page-document.ts';
import { renamedCommand, rearranged } from '../src/page-rebase.ts';
import { PAGE_GESTURES, takeOffCommands, tidyCommands } from '../src/page-gestures.ts';
import { applyPlanned, pageBetween } from '../src/page-between.ts';
import { CubeRefusal, rowColumns, type CubeSnapshot, type DerivedColumn, type FilterNode } from '../src/snapshot.ts';
import { readUnique, uniqueCommands, uniqueOf } from '../src/unique.ts';
import { correlationLambda, correlationsOf, profileLambda, profileOf, TOP_VALUES, topValuesLambda, topValuesOf } from '../src/profile.ts';
import { imputeCommands, imputedOf, readImputation } from '../src/impute.ts';
import { dateToolCommands, dateToolOf, readDateTool } from '../src/date-tools.ts';
import { multiFieldOf } from '../src/multi-field.ts';
import { rangesWords, readRanges, readRowSample, rowSampleCommands, rowSampleOf, sampleCommands } from '../src/row-sample.ts';
import { readTranspose, transposeCommands, transposeOf } from '../src/transpose.ts';
import { readSelection, retypesOf, selectCommands, selectionOf } from '../src/select-columns.ts';
import { boxOrder, handedOn } from '../src/box-columns.ts';
import { readSummary, summarizeCommands } from '../src/summarize.ts';
import { unmatchedOn } from '../src/join-how.ts';
import { findReplaceCommands, findReplaceOf, readFindReplace } from '../src/find-replace.ts';
import { countRecordsCommands, countRecordsOf, readCountRecords } from '../src/count-records.ts';
import { summaryActions, summaryOf } from '../src/grid-edits.ts';
import { applyGridCommands } from '../src/grid-commands.ts';
import type { GridDraft } from '../src/grid-edits.ts';
import { multiFieldCommands, multiFieldText, readMultiField } from '../src/multi-field-columns.ts';
import { TreeState } from '../src/tree.ts';
import { accessor, collection, fn, gt, lambda, lit, property, toJson, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { boxRows, pureFrom, pureInput, pureInputOf, pureOver, pureProblem, pureShown, pureText, pureWords } from '../src/pure-box.ts';
import { pureSteps } from '../src/pure-import.ts';
import { PUNCTUATION, textToolOf } from '../src/text-tools.ts';
import { lackingWords, VALUES_COUNTED } from '../src/cross-tab.ts';
import { MAX_PIVOT_VALUES, pivotValuesOf, tooManyValues } from '../src/query.ts';
import { readTextTool, textToolColumns, textToolProblem } from '../src/text-tool-columns.ts';
import { grainColumn, grainOf } from '../src/calc-presets.ts';
import { CHECKS_COLUMN, checkCommands, checksOf, checkWords, failingChecks, failingWords, readChecks, testColumns, type Check } from '../src/checks.ts';
import { DATE_RANGES, dateRangeConditions, datesBetweenConditions } from '../src/date-ranges.ts';
import { datesKept } from '../src/page-filters.ts';
import { binningCommands, binsOf, readBinning } from '../src/binning.ts';
import { generatedOf, generatedWords, generateRowsCommands, readGenerateRows } from '../src/generate-rows.ts';
import { MAX_RULES, replacedOf } from '../src/dynamic-replace.ts';
import { dynamicReplaceCommands, dynamicReplaceProbe, readDynamicReplace, replacedColumnsOf, ruleText, rulesOf } from '../src/dynamic-replace-rules.ts';
import { CUSTOM_FILTER_COLUMN, customFilterCommands, customFilterOf, customFilterProbe, customFilterText, readCustomFilter } from '../src/custom-filter.ts';
import { pastedCells, tableCsv, tableProblem } from '../src/ui/text-input-dialog.ts';

const COLUMNS = [{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }];
const FILE: FileSource = {
  _type: 'file', name: 'trades.csv', format: 'csv', size: 120, sha256: 'ab'.repeat(32), columns: COLUMNS,
};
const rows = (grid: string): RowsSource => ({ _type: 'rows', name: 'trades.csv', input: { grid }, columns: COLUMNS });

/** A cube over `source`, its query changed by `query` (a box's one step). */
function cube(source: CubeSource, query: (q: CubeDocument['query']) => CubeDocument['query'] = (q) => q,
  configuration: Readonly<Record<string, unknown>> = {}): CubeDocument {
  const written = writeCube({
    name: 'flow', source, snapshot: rawRows(accessor('local::DB', 'trades'), COLUMNS),
    configuration: DEFAULT_CONFIGURATION, tree: TreeState.fromPaths([], false),
  });
  return { ...written, query: query(written.query), configuration: { ...written.configuration, ...configuration } };
}

/** A page of one grid over the file, on a board. */
const BOARD = (): PageDocument => writePageOf({
  name: 'flow', cubes: [{ id: 'grid-1', cube: cube(FILE) }],
  views: { views: [{ id: 'grid-1', kind: 'grid', cube: 'grid-1' }], sheets: [{ id: 'sheet-1', layout: { kind: 'bands', ...add(NEW_SHEET, 'grid-1') } }] },
});

const draft = (page: PageDocument): PageDraft => ({ page, held: idsOf(page) });
const apply = (d: PageDraft, ...commands: PageCommand[]): PageDraft => commands.reduce((at, c) => applyPageCommand(at, c).draft, d);
const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};
const addGrid = (document: CubeDocument, sheet: string, near: string | null): PageCommand =>
  ({ kind: 'addGrid', document, sheet, near, title: null, columns: null });

/** BOARD, then a flow: an Input (grid-2), a Filter after it (grid-3), and a Sort after the Input too (grid-4). */
function FLOW(): PageDraft {
  return apply(draft(BOARD()),
    { kind: 'addFlow', name: null },
    addGrid(cube(FILE), 'sheet-2', null),
    addGrid(cube(rows('grid-2'), (q) => ({ ...q, filter: { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 } })), 'sheet-2', 'grid-2'),
    addGrid(cube(rows('grid-2'), (q) => ({ ...q, sorts: [{ column: 'notional', direction: 'desc' }] })), 'sheet-2', 'grid-2'));
}
const flowOf = (page: PageDocument, sheet = 'sheet-2') => {
  const layout = page.sheets.find((s) => s.id === sheet)!.layout;
  if (layout.kind !== 'flow') throw new Error(`${sheet} is no flow`);
  return layout;
};
const at = (page: PageDocument, tile: string) => flowOf(page).boxes.find((b) => b.tile === tile);

describe("a flow's layout (layout/flow.ts)", () => {
  it('places a box after the box it reads, the next column on its row, else the first free row there', () => {
    const one = placeAfter(EMPTY_FLOW, 'a');
    assert.deepEqual(one.boxes, [{ tile: 'a', x: MARGIN, y: MARGIN }]);
    const two = placeAfter(one, 'b', 'a');
    assert.deepEqual(two.boxes[1], { tile: 'b', x: MARGIN + BOX_WIDTH + GAP_X, y: MARGIN });
    const three = placeAfter(two, 'c', 'a');
    assert.deepEqual(three.boxes[2], { tile: 'c', x: MARGIN + BOX_WIDTH + GAP_X, y: MARGIN + BOX_HEIGHT + GAP_Y }, 'below the one taken');
    // after none: a row of its own below every box
    assert.deepEqual(placeAfter(three, 'd').boxes[3], { tile: 'd', x: MARGIN, y: MARGIN + 2 * (BOX_HEIGHT + GAP_Y) });
    assert.throws(() => placeAfter(three, 'a'), (e: unknown) => e instanceof FlowRefusal && /'a' is on the canvas already/.test(e.message));
  });

  it('moves a box to whole units, never off the canvas; takes one off', () => {
    const flow = placeAfter(placeAfter(EMPTY_FLOW, 'a'), 'b', 'a');
    assert.deepEqual(moveBox(flow, 'b', 300.4, 99.6).boxes[1], { tile: 'b', x: 300, y: 100 });
    assert.throws(() => moveBox(flow, 'b', -1, 0), /neither below zero, not \(-1, 0\)/);
    assert.throws(() => moveBox(flow, 'b', Number.NaN, 0), /two numbers/);
    assert.throws(() => moveBox(flow, 'z', 0, 0), /'z' is no box of this flow/);
    assert.deepEqual(removeBox(flow, 'a').boxes.map((b) => b.tile), ['b']);
    assert.deepEqual(removeBox(flow, 'z'), flow, 'a box it does not have: as it is');
  });

  it('tidies a flow: each box a column after the boxes it reads, level with them, else as it was down the canvas', () => {
    const reads: Record<string, string[]> = { input: [], other: [], filter: ['input'], join: ['filter', 'other'], sort: ['input'] };
    const messy = { boxes: [
      { tile: 'join', x: 10, y: 500 }, { tile: 'input', x: 700, y: 30 }, { tile: 'sort', x: 5, y: 5 },
      { tile: 'filter', x: 300, y: 300 }, { tile: 'other', x: 0, y: 900 },
    ] };
    const tidy = arranged(messy, (t) => reads[t] ?? []);
    const at = (t: string) => tidy.boxes.find((b) => b.tile === t);
    const col = (n: number) => MARGIN + n * (BOX_WIDTH + GAP_X);
    const row = (n: number) => MARGIN + n * (BOX_HEIGHT + GAP_Y);
    assert.deepEqual(tidy.boxes.map((b) => b.tile), messy.boxes.map((b) => b.tile), 'the boxes in the order they came');
    assert.deepEqual([at('input'), at('other')], [{ tile: 'input', x: col(0), y: row(0) }, { tile: 'other', x: col(0), y: row(1) }]);
    // the Sort was higher up the canvas than the Filter: first in their column
    assert.deepEqual([at('sort'), at('filter')], [{ tile: 'sort', x: col(1), y: row(0) }, { tile: 'filter', x: col(1), y: row(1) }]);
    assert.deepEqual(at('join'), { tile: 'join', x: col(2), y: row(1) }, 'level with the boxes it reads');
    assert.deepEqual(arranged(tidy, (t) => reads[t] ?? []), tidy, 'tidy already: as it is');
    // each box as it was but for its place: an Output's file and a Test's stopping kept (they were dropped, 2026-10-25)
    const kept = arranged({ boxes: [{ tile: 'input', x: 9, y: 9, output: { format: 'csv', name: 'out.csv' } }, { tile: 'filter', x: 0, y: 0, stops: true }] },
      (t) => reads[t] ?? []);
    assert.deepEqual(kept.boxes.map((b) => [b.output?.name, b.stops]), [['out.csv', undefined], [undefined, true]]);
  });

  it('sets a Test box to stop a run (setStops), or to warn; read, written, renamed, undone', () => {
    // a Test after the Input (grid-5): its checks what a run is stopped on
    const checked = applyGridCommands({ snapshot: rawRows(accessor('local::DB', 'trades'), COLUMNS), config: DEFAULT_CONFIGURATION },
      checkCommands(rawRows(accessor('local::DB', 'trades'), COLUMNS), [{ kind: 'notEmpty', column: 'region' }])).snapshot;
    const TESTED = (): PageDraft => apply(FLOW(), addGrid(cube(rows('grid-2'), (q) => ({ ...q, derived: checked.derived, rows: checked.rows })), 'sheet-2', 'grid-2'));
    const d = apply(TESTED(), { kind: 'setStops', grid: 'grid-5', stops: true });
    assert.equal(at(d.page, 'grid-5')?.stops, true);
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(d.page)))), pageToJson(d.page), 'written and read back the same');
    refused(() => apply(d, { kind: 'setStops', grid: 'grid-5', stops: true }), /'grid-5' stops a run already/);
    refused(() => apply(TESTED(), { kind: 'setStops', grid: 'grid-5', stops: false }), /'grid-5' stops no run/);
    refused(() => apply(TESTED(), { kind: 'setStops', grid: 'grid-1', stops: true }), /'grid-1' is no box of a flow: a run is a flow's/);
    // a box that checks nothing is never judged, so it stops no run (the review of 2026-10-26: setStops took any box)
    refused(() => apply(TESTED(), { kind: 'setStops', grid: 'grid-3', stops: true }),
      /'grid-3' checks nothing: a run is stopped by a Test, or a Cross Tab with its columns fixed/);
    assert.equal(at(apply(d, { kind: 'setStops', grid: 'grid-5', stops: false }).page, 'grid-5')?.stops, undefined);
    assert.deepEqual(readPageCommand({ kind: 'setStops', grid: 'grid-5', stops: true }), { kind: 'setStops', grid: 'grid-5', stops: true });
    refused(() => readPageCommand({ kind: 'setStops', grid: 'grid-5', stops: 'yes' }), /a Test stops a run \(true\) or warns \(false\), not "yes"/);
    assert.deepEqual(renamedCommand({ kind: 'setStops', grid: 'grid-5', stops: true }, new Map([['grid-5', 'grid-9']])), { kind: 'setStops', grid: 'grid-9', stops: true });
    const back = applyPlanned({ page: d.page, held: idsOf(d.page) }, pageBetween(d.page, TESTED().page, 'history'), false).draft.page;
    assert.equal(at(back, 'grid-5')?.stops, undefined, 'undone by the commands between');
    // a page whose box stops otherwise is refused as it is read
    const bad = JSON.parse(pageToJson(d.page));
    bad.sheets[1].layout.boxes.find((b: { tile: string }) => b.tile === 'grid-5').stops = false;
    assert.throws(() => readPage(bad), /stops a run \(true\) or is without it, not false/);
  });

  it('sets a box to cache its rows (setCache, FL10), or no longer; read, written, renamed, undone, kept by Tidy Up', () => {
    const d = apply(FLOW(), { kind: 'setCache', grid: 'grid-2', cache: true });
    assert.equal(at(d.page, 'grid-2')?.cache, true);
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(d.page)))), pageToJson(d.page), 'written and read back the same');
    refused(() => apply(d, { kind: 'setCache', grid: 'grid-2', cache: true }), /'grid-2' caches its rows already/);
    refused(() => apply(FLOW(), { kind: 'setCache', grid: 'grid-2', cache: false }), /'grid-2' caches no rows/);
    refused(() => apply(FLOW(), { kind: 'setCache', grid: 'grid-1', cache: true }), /'grid-1' is no box of a flow: a cache is a flow's box's/);
    assert.equal(at(apply(d, { kind: 'setCache', grid: 'grid-2', cache: false }).page, 'grid-2')?.cache, undefined);
    assert.deepEqual(readPageCommand({ kind: 'setCache', grid: 'grid-2', cache: true }), { kind: 'setCache', grid: 'grid-2', cache: true });
    refused(() => readPageCommand({ kind: 'setCache', grid: 'grid-2', cache: 1 }), /a box caches its rows \(true\) or not \(false\), not 1/);
    assert.deepEqual(renamedCommand({ kind: 'setCache', grid: 'grid-2', cache: true }, new Map([['grid-2', 'grid-9']])), { kind: 'setCache', grid: 'grid-9', cache: true });
    const back = applyPlanned({ page: d.page, held: idsOf(d.page) }, pageBetween(d.page, FLOW().page, 'history'), false).draft.page;
    assert.equal(at(back, 'grid-2')?.cache, undefined, 'undone by the commands between');
    // moved, it is still cached (layout/flow.ts moveBox keeps it)
    assert.equal(at(apply(d, { kind: 'placeBox', tile: 'grid-2', x: 40, y: 40 }).page, 'grid-2')?.cache, true);
    // a page whose box caches otherwise is refused as it is read
    const bad = JSON.parse(pageToJson(d.page));
    bad.sheets[1].layout.boxes.find((b: { tile: string }) => b.tile === 'grid-2').cache = 'yes';
    assert.throws(() => readPage(bad), /caches its rows \(true\) or is without it, not "yes"/);
  });

  it('sets a box to block a run (setBlocks) and to say a message (setMessage); read, written, renamed, undone, kept by a move', () => {
    const MESSAGE = { text: '{rows} big trades', when: 'rows', level: 'warning' } as const;
    const d = apply(FLOW(), { kind: 'setBlocks', grid: 'grid-3', blocks: true }, { kind: 'setMessage', grid: 'grid-3', message: MESSAGE });
    assert.equal(at(d.page, 'grid-3')?.blocks, true);
    assert.deepEqual(at(d.page, 'grid-3')?.message, MESSAGE);
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(d.page)))), pageToJson(d.page), 'written and read back the same');
    refused(() => apply(d, { kind: 'setBlocks', grid: 'grid-3', blocks: true }), /'grid-3' blocks a run already/);
    refused(() => apply(FLOW(), { kind: 'setBlocks', grid: 'grid-3', blocks: false }), /'grid-3' blocks no run/);
    refused(() => apply(d, { kind: 'setMessage', grid: 'grid-3', message: MESSAGE }), /'grid-3' says so already/);
    refused(() => apply(FLOW(), { kind: 'setMessage', grid: 'grid-1', message: MESSAGE }), /'grid-1' is no box of a flow: a message is a flow's box's/);
    refused(() => apply(FLOW(), { kind: 'setMessage', grid: 'grid-3', message: { ...MESSAGE, text: ' ' } }), /a message says something/);
    refused(() => readPageCommand({ kind: 'setMessage', grid: 'grid-3', message: { ...MESSAGE, level: 'loud' } }), /a message is info, warning, error, not "loud"/);
    refused(() => readPageCommand({ kind: 'setBlocks', grid: 'grid-3', blocks: 1 }), /a box blocks a run \(true\) or not \(false\), not 1/);
    assert.deepEqual(readPageCommand({ kind: 'setMessage', grid: 'grid-3', message: MESSAGE }), { kind: 'setMessage', grid: 'grid-3', message: MESSAGE });
    assert.deepEqual(renamedCommand({ kind: 'setBlocks', grid: 'grid-3', blocks: true }, new Map([['grid-3', 'grid-9']])), { kind: 'setBlocks', grid: 'grid-9', blocks: true });
    const back = applyPlanned({ page: d.page, held: idsOf(d.page) }, pageBetween(d.page, FLOW().page, 'history'), false).draft.page;
    assert.deepEqual([at(back, 'grid-3')?.blocks, at(back, 'grid-3')?.message], [undefined, undefined], 'undone by the commands between');
    const moved = apply(d, { kind: 'placeBox', tile: 'grid-3', x: 40, y: 40 }).page;
    assert.deepEqual([at(moved, 'grid-3')?.blocks, at(moved, 'grid-3')?.message], [true, MESSAGE], 'a move keeps its settings');
    // a layout told whole keeps them as the page has them (page-rebase.ts rearranged)
    const told = rearranged({ kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: flowOf(FLOW().page).boxes } }, d.page);
    assert.ok(told.kind === 'layout' && told.layout.kind === 'flow' && told.layout.boxes.find((b) => b.tile === 'grid-3')?.blocks === true);
    const bad = JSON.parse(pageToJson(d.page));
    bad.sheets[1].layout.boxes.find((b: { tile: string }) => b.tile === 'grid-3').message.when = 'often';
    assert.throws(() => readPage(bad), /a message is said always, rows, noRows, not "often"/);
  });

  it("a layout told whole is held to its boxes' settings' rules (the roadmap audit of 2026-10-26)", () => {
    const boxes = flowOf(FLOW().page).boxes;
    refused(() => apply(FLOW(), { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: boxes.map((b) => (b.tile === 'grid-3' ? { ...b, stops: true as const } : b)) } }),
      /'grid-3' checks nothing: a run is stopped by a Test/);
    const noted = apply(FLOW(), { kind: 'addText', sheet: 'sheet-2', text: 'a note', title: null, columns: null });
    const withNote = flowOf(noted.page).boxes;
    const note = noted.page.views.find((v) => v.kind === 'text')!.id;
    refused(() => apply(noted, { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: withNote.map((b) => (b.tile === note ? { ...b, cache: true as const } : b)) } }),
      new RegExp(`${note} caches its rows: a cache is a grid's, not a comment`));
    // its places alone changed: taken
    assert.ok(apply(FLOW(), { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: boxes.map((b) => ({ ...b, x: b.x + 10 })) } }));
  });

  it("a run's stages: the boxes after one that blocks run after every other; a Message's words, when its rows are as it asks", () => {
    // Input a -> b (blocks) -> c -> d; e reads a; f reads c and e (a join)
    const flow: Flow = { boxes: [{ tile: 'a', x: 0, y: 0 }, { tile: 'b', x: 200, y: 0, blocks: true }, { tile: 'c', x: 400, y: 0 },
      { tile: 'd', x: 600, y: 0 }, { tile: 'e', x: 200, y: 100 }, { tile: 'f', x: 600, y: 100 }, { tile: 'note', x: 0, y: 300 }] };
    const reads: Record<string, string[]> = { b: ['a'], c: ['b'], d: ['c'], e: ['a'], f: ['c', 'e'] };
    assert.deepEqual(runStages(flow, (t) => reads[t] ?? [], (t) => t !== 'note'), [['a', 'b', 'e'], ['c', 'd', 'f']]);
    assert.deepEqual(runStages({ boxes: flow.boxes.map(({ blocks: _b, ...x }) => x) }, (t) => reads[t] ?? []), [['a', 'note', 'b', 'e', 'c', 'd', 'f']],
      'no block: one stage, in the canvas\'s order');
    // two blocks in a chain: three stages -- f, after c as after b, waits for both
    assert.deepEqual(runStages({ boxes: flow.boxes.map((b) => (b.tile === 'c' ? { ...b, blocks: true as const } : b)) }, (t) => reads[t] ?? [], (t) => t !== 'note'),
      [['a', 'b', 'e'], ['c'], ['d', 'f']]);
    const m = (when: 'always' | 'rows' | 'noRows') => ({ text: '{rows} rows of {rows}', when, level: 'info' as const });
    assert.equal(messageSaid(m('always'), 1200), '1,200 rows of 1,200');
    assert.equal(messageSaid(m('rows'), 0), undefined);
    assert.equal(messageSaid(m('rows'), 3), '3 rows of 3');
    assert.equal(messageSaid(m('noRows'), 0), '0 rows of 0');
    assert.equal(messageSaid(m('noRows'), 2), undefined);
    assert.equal(messageSaid(m('rows'), undefined), undefined, 'not counted: not said');
    assert.equal(messageSaid(m('always'), undefined), '? rows of ?');
  });

  it('says what is wrong with a flow read from somewhere', () => {
    assert.deepEqual(flowProblems({ boxes: [{ tile: 'a', x: 1, y: 2 }] }), []);
    assert.deepEqual(flowProblems({ boxes: [{ tile: 'a', x: 1, y: 2 }, { tile: 'a', x: 1.5, y: -2 }] }), [
      "'a' is on the canvas twice",
      "'a' is at (1.5, -2): a box's place is two whole numbers, neither below zero",
    ]);
  });
});

describe("a Cross Tab's values as its questions answer them (query.ts pivotValuesOf): the screen's and Python's", () => {
  const s = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'year', type: 'Integer' }]), pivotOn: ['year'] };
  const answer = (values: readonly unknown[]) => ({ columns: [{ name: 'year', type: 'Integer', values }], rowCount: values.length, epoch: 1, elapsedMs: 0 }) as never;
  it('each typed as its column is; past what a pivot shows, said; an answer without its column refused, never none', () => {
    assert.deepEqual(pivotValuesOf(s, answer([2023n, null])), { values: ['2023', null], more: false });
    const many = Array.from({ length: MAX_PIVOT_VALUES + 1 }, (_, i) => i);
    assert.equal(pivotValuesOf(s, answer(many)).more, true);
    assert.match(tooManyValues('year'), /more than 500 values of year/);
    assert.throws(() => pivotValuesOf(s, { columns: [], rowCount: 0, epoch: 1, elapsedMs: 0 } as never), /its values' answer has no column year/);
    // a question asked one past the cap: at least as many said, never a count it did not make (the review of 2026-10-26)
    assert.match(lackingWords('year', many), /and at least 493 more have no column of their own/);
    assert.match(lackingWords('year', [2024, 2025]), /^year 2024, 2025 have no column of their own/);
    assert.equal(VALUES_COUNTED, MAX_PIVOT_VALUES, "the canvas's words count what the question counts");
  });
});

describe("a box's name (flow-boxes.ts boxNamed, pageBoxName): the canvas's, a Join's title's, Python's", () => {
  it('its title, unless it is the one every new grid is given; else its tool\'s', () => {
    assert.equal(boxNamed('grid-3', 'Big trades', 'filter'), 'Big trades');
    assert.equal(boxNamed('grid-3', 'Grid 3', 'filter'), 'Filter', "a new grid's title is no name of its own");
    assert.equal(boxNamed('grid-3', undefined, 'sort'), 'Sort');
    const page = FLOW().page;
    assert.equal(pageBoxName(page, 'grid-3'), 'Filter', 'read off its query');
    assert.equal(pageBoxName(page, 'grid-4'), 'Sort');
  });
});

describe('what a box says it does (flow-boxes.ts boxSays): its query, never a field beside it', () => {
  it('a box over a source of its own is an Input; over another grid\'s rows, each step its query takes', () => {
    assert.deepEqual(boxSays(cube(FILE)), { tool: 'input', also: [], says: 'trades.csv' });
    assert.deepEqual(boxSays(cube(rows('grid-1'))), { tool: 'browse', also: [], says: 'every row' });
    const filtered = boxSays(cube(rows('grid-1'), (q) => ({ ...q, filter: { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 } })));
    assert.equal(filtered.tool, 'filter');
    assert.match(filtered.says, /notional .* 100/);
    // the order a grid takes its steps in: its calculated columns before its sorts
    const both = boxSays(cube(rows('grid-1'), (q) => ({
      ...q, sorts: [{ column: 'notional', direction: 'desc' }], derived: [{ name: 'big', type: 'Boolean' } as never],
    })));
    assert.deepEqual(both, { tool: 'formula', also: ['sort'], says: 'big' });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, rows: ['region'] }))), { tool: 'summarize', also: [], says: 'by region' });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => q, { columns: { region: { hidden: true } } })),
      { tool: 'select', also: [], says: '1 of 2 columns left out' });
  });

  it('a window column says the tool that makes one so; a column pivot, a Cross Tab', () => {
    const win = (name: string, window: Record<string, unknown>) => ({ name, window: { partition: [], order: [], ...window } } as never);
    const said = (derived: never[]) => boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived })));
    assert.deepEqual(said([win('RecordID', { fn: 'rowNumber' })]), { tool: 'recordId', also: [], says: 'RecordID' });
    assert.deepEqual(said([win('Previous', { fn: 'lag', column: 'notional', offset: 1 })]), { tool: 'multiRow', also: [], says: 'Previous' });
    assert.deepEqual(said([win('Tile', { fn: 'ntile', buckets: 4 })]), { tool: 'tile', also: [], says: 'Tile' });
    assert.deepEqual(said([win('RunningTotal', { fn: 'sum', column: 'notional', frame: 'running' }), win('Total', { fn: 'sum', column: 'notional', frame: 'partition' })]),
      { tool: 'runningTotal', also: ['formula'], says: 'RunningTotal' });
    // the columns of one tool together, in the order they come
    assert.deepEqual(said([win('a', { fn: 'rowNumber' }), win('b', { fn: 'rowNumber' })]), { tool: 'recordId', also: [], says: 'a, b' });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, rows: ['region'], pivotOn: ['desk'] }))),
      { tool: 'crosstab', also: [], says: 'desk across, by region' });
    // a Top N, as a Sample says it: its first (smallest) or last rows by a column
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, topN: { by: 'notional', n: 5, bottom: true } }))),
      { tool: 'sample', also: [], says: 'first 5 by notional' });
  });

  it("a date taken to its period says a DateTime, by its expression's shape: the preset's or one typed so", () => {
    const said = (derived: DerivedColumn[]) => boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived })));
    assert.deepEqual(said([grainColumn('trade_date', 'month', 'trade_date_month')]), { tool: 'datetime', also: [], says: 'trade_date by month' });
    assert.deepEqual(said([grainColumn('trade_date', 'day', 'd'), grainColumn('settle', 'year', 'y')]),
      { tool: 'datetime', also: [], says: 'trade_date by day, settle by year' });
    // the same function over anything but the row's own column, a Formula's
    const other = { name: 'odd', lambda: lambda(['x'], fn('firstDayOfMonth', fn('toOne', property(variable('y'), 'trade_date')))) };
    assert.deepEqual(said([other]), { tool: 'formula', also: [], says: 'odd' });
    assert.equal(grainOf(grainColumn('trade_date', 'quarter', 'q'))?.grain, 'quarter');
  });

  it("a box's inputs, live or a copy named as the box it copies; the boxes that read it, through others", () => {
    assert.deepEqual(boxInputs(cube(FILE)), []);
    assert.deepEqual(boxInputs(cube(rows('grid-2'))), [{ side: 'input', grid: 'grid-2' }]);
    const { page } = apply(FLOW(), { kind: 'removeTile', tile: 'grid-2' }, { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3' } });
    // grid-3 read a copy of the Input; grid-4 reads grid-3 live, then frozen: a copy of the Filter
    const documentOf = (id: string) => page.cubes.find((c) => c.id === id)?.cube;
    assert.deepEqual(boxInputs(documentOf('grid-3')!), [{ side: 'input', frozen: 'trades.csv' }]);
    const frozen = apply({ page, held: idsOf(page) }, { kind: 'setInput', grid: 'grid-4', side: 'input', input: null }).page;
    assert.deepEqual(boxInputs(frozen.cubes.find((c) => c.id === 'grid-4')!.cube), [{ side: 'input', frozen: 'Filter' }]);
    const chain = apply(FLOW(), { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3' } }).page;
    const of = (id: string) => chain.cubes.find((c) => c.id === id)?.cube;
    assert.deepEqual([...readersOf('grid-2', ['grid-2', 'grid-3', 'grid-4'], of)].sort(), ['grid-3', 'grid-4']);
    assert.deepEqual([...readersOf('grid-4', ['grid-2', 'grid-3', 'grid-4'], of)], []);
  });
});

describe('the text tools (text-tools.ts): calculated columns, read back off their expressions', () => {
  const said = (derived: DerivedColumn[]) => boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived })));
  const body = (d: DerivedColumn) => JSON.stringify(d.lambda?.body[0]);

  it('Text To Columns: a column a part, counted from 1 by name and from 0 by splitPart; the box says the split once', () => {
    const parts = textToolColumns({ kind: 'split', column: 'book', delimiter: ' ', parts: 3, name: 'book' });
    assert.deepEqual(parts.map((p) => p.name), ['book_1', 'book_2', 'book_3']);
    assert.equal(body(parts[2]!), JSON.stringify(fn('splitPart', property(variable('x'), 'book'), lit.string(' '), lit.integer(2))));
    assert.deepEqual(said(parts), { tool: 'textToColumns', also: [], says: "book split on ' '" });
    // into rows: one column, exploded over split -- a row a part
    const rowsOf = textToolColumns({ kind: 'split', column: 'book', delimiter: ',', parts: 1, name: 'part', rows: true });
    assert.deepEqual(rowsOf.map((p) => [p.name, p.unnest]), [['part', true]]);
    // an empty text kept, its one part blank: an explode of no parts would be no row (the correctness audit's 7)
    assert.equal(body(rowsOf[0]!), JSON.stringify(fn('split', fn('coalesce', property(variable('x'), 'book'), lit.string('')), lit.string(','))));
    assert.deepEqual(said(rowsOf), { tool: 'textToColumns', also: [], says: "book split on ',' to rows" });
    assert.deepEqual(readTextTool({ kind: 'split', column: 'book', delimiter: ',', name: 'part', rows: true }),
      { kind: 'split', column: 'book', delimiter: ',', parts: 1, name: 'part', rows: true });
  });

  it('RegEx: a match, a replacement or the first match, each over the column itself', () => {
    const one = (mode: 'match' | 'replace' | 'extract') => textToolColumns({ kind: 'regex', column: 'desk', pattern: '^[A-Z]', mode, replacement: '?', name: 'd' })[0]!;
    assert.deepEqual(said([one('match')]), { tool: 'regex', also: [], says: 'desk matches /^[A-Z]/' });
    assert.deepEqual(said([one('replace')]), { tool: 'regex', also: [], says: '/^[A-Z]/ replaced in desk' });
    assert.deepEqual(said([one('extract')]), { tool: 'regex', also: [], says: '/^[A-Z]/ of desk' });
    assert.match(body(one('extract')), /"function":"first".*"function":"regexpExtract"/);
    // tokenize: every match, exploded -- a row a match, an empty text's none
    const tokens = textToolColumns({ kind: 'regex', column: 'desk', pattern: '[A-Z]+', mode: 'tokenize', name: 'tok' })[0]!;
    assert.equal(tokens.unnest, true);
    assert.equal(body(tokens), JSON.stringify(fn('regexpExtract', fn('coalesce', property(variable('x'), 'desk'), lit.string('')), lit.string('[A-Z]+'), lit.boolean(true))));
    assert.deepEqual(said([tokens]), { tool: 'regex', also: [], says: '/[A-Z]+/ of desk to rows' });
    assert.deepEqual(readTextTool({ kind: 'regex', column: 'desk', pattern: 'x', mode: 'tokenize' }), { kind: 'regex', column: 'desk', pattern: 'x', mode: 'tokenize', name: 'desk_token' });
  });

  it("Data Cleansing: a chain of its own functions over each column, in its order; a Formula's other functions none of it", () => {
    const clean = textToolColumns({ kind: 'cleanse', columns: ['region', 'desk'], cleansing: { blanks: true, punctuation: true, trim: true, letterCase: 'lower' } });
    assert.deepEqual(clean.map((c) => c.name), ['region_clean', 'desk_clean']);
    assert.match(body(clean[0]!), /^\{"_type":"func","function":"toLower","parameters":\[\{"_type":"func","function":"trim"/);
    assert.deepEqual(said(clean), { tool: 'cleanse', also: [], says: 'region cleansed, desk cleansed' });
    // its punctuation is what is neither a letter nor a digit of any script, nor a space: `é`, `ü`, `東` are kept (the
    // audit's 2 of 2026-10-26: ASCII's letters alone took them out)
    const out = (text: string): string => text.replace(new RegExp(PUNCTUATION, 'gu'), '');
    assert.deepEqual(["Côte d'Ivoire", 'José  Müller!', '東京', '¿Qué? 42½'].map(out), ['Côte dIvoire', 'José  Müller', '東京', 'Qué 42½']);
    // a replacement of its own pattern is a cleansing; of any other, a RegEx
    const spaces = textToolColumns({ kind: 'cleanse', columns: ['desk'], cleansing: { spaces: true } });
    assert.equal(textToolOf(spaces[0]!)?.tool, 'cleanse');
    // a function no text tool makes, a Formula's
    const other = { name: 'n', lambda: lambda(['x'], fn('length', fn('toOne', property(variable('x'), 'desk')))) };
    assert.equal(textToolOf(other), undefined);
    assert.deepEqual(said([other]), { tool: 'formula', also: [], says: 'n' });
  });

  it('refuses settings that make no column, in words', () => {
    assert.equal(textToolProblem({ kind: 'split', column: 'a', delimiter: '', parts: 2, name: 'a' }), 'it splits on a delimiter: type one');
    assert.equal(textToolProblem({ kind: 'split', column: 'a', delimiter: ',', parts: 0, name: 'a' }), 'it makes from 1 to 50 columns');
    assert.equal(textToolProblem({ kind: 'regex', column: 'a', pattern: '', mode: 'match', name: 'a' }), 'it needs a pattern');
    assert.equal(textToolProblem({ kind: 'cleanse', columns: ['a'], cleansing: {} }), 'pick what to do to them');
    assert.throws(() => textToolColumns({ kind: 'cleanse', columns: [], cleansing: { trim: true } }), /pick a column to cleanse/);
  });
});

describe('Unique (unique.ts): a row number per key, its first kept, the number hidden -- read back as one step', () => {
  const snapshot = rawRows(accessor('local::DB', 'trades'), COLUMNS);
  const made = (keys: string[], by?: { column: string; direction: 'asc' | 'desc' }) => uniqueCommands(snapshot, { keys, ...(by ? { by } : {}) });

  it('makes its row number, its filter and its column hidden, ordered by the first key unless given an order', () => {
    const [calc, filter, configure] = made(['region']);
    // its ties broken by every other column that sorts, rising: each of its queries (U, D, its Output) numbers alike
    // (the correctness audit's 14, 2026-10-26)
    assert.deepEqual(calc, { kind: 'calculate', level: 'row', column: {
      name: 'UniqueRow', window: { fn: 'rowNumber', partition: ['region'], order: [{ column: 'region', direction: 'asc' }, { column: 'notional', direction: 'asc' }] },
    } });
    assert.deepEqual(filter, { kind: 'addFilter', condition: { kind: 'condition', column: 'UniqueRow', operator: 'equal', value: 1 } });
    assert.deepEqual(configure, { kind: 'configure', patch: { columns: { UniqueRow: { hidden: true } } } });
    // largest first: its empties after every value, led by a flag of them (Pure's empty is the largest) -- hidden too
    // (the correctness audit's 3)
    const [flag, ordered, , hide] = made(['region'], { column: 'notional', direction: 'desc' }) as [{ column: DerivedColumn }, { column: DerivedColumn }, unknown, unknown];
    assert.deepEqual(flag.column, { name: 'UniqueEmpty', kind: 'dimension', lambda: lambda(['x'], fn('isEmpty', property(variable('x'), 'notional'))) });
    assert.deepEqual(ordered.column.window?.order, [{ column: 'UniqueEmpty', direction: 'asc' }, { column: 'notional', direction: 'desc' }]);
    assert.deepEqual(hide, { kind: 'configure', patch: { columns: { UniqueRow: { hidden: true }, UniqueEmpty: { hidden: true } } } });
    const box = cube(rows('grid-1'), (q) => ({ ...q, derived: [flag.column, ordered.column], filter: { kind: 'condition', column: 'UniqueRow', operator: 'equal', value: 1 } }),
      { columns: { UniqueRow: { hidden: true }, UniqueEmpty: { hidden: true } } });
    assert.deepEqual(uniqueOf(box.query), { keys: ['region'], by: { column: 'notional', direction: 'desc' }, column: 'UniqueRow', columns: ['UniqueEmpty', 'UniqueRow'] });
    assert.deepEqual(boxSays(box), { tool: 'unique', also: [], says: 'by region' }, 'its flag its own, no Formula');
  });

  it('a box made so says Unique, by its keys: its number neither a Record ID nor its filter a Filter, nor hidden a Select', () => {
    const [calc, filter] = made(['region', 'notional']) as unknown as [{ column: DerivedColumn }, { condition: FilterNode }];
    const box = cube(rows('grid-1'), (q) => ({ ...q, derived: [calc.column], filter: filter.condition }), { columns: { UniqueRow: { hidden: true } } });
    assert.deepEqual(boxSays(box), { tool: 'unique', also: [], says: 'by region, notional' });
    assert.deepEqual(uniqueOf(box.query)?.keys, ['region', 'notional']);
    // another filter beside it: no Unique, as its words would hide the other
    assert.equal(uniqueOf({ derived: [calc.column], filter: { kind: 'and', children: [filter.condition, filter.condition] } }), undefined);
  });

  it('refuses a key it cannot make, in words; a name taken moves on', () => {
    assert.throws(() => made([]), /a key is at least one column/);
    assert.throws(() => made(['nope']), /the grid has no column 'nope'/);
    assert.throws(() => made(['region', 'region']), /a key names each column once/);
    const taken = { ...snapshot, derived: [{ name: 'UniqueRow', window: { fn: 'rowNumber', partition: [], order: [] } }] } as never;
    assert.equal((uniqueCommands(taken, { keys: ['region'] })[0] as { column: DerivedColumn }).column.name, 'UniqueRow_2');
    assert.deepEqual(readUnique({ kind: 'unique', keys: ['region'], by: 'notional' }), { keys: ['region'], by: { column: 'notional', direction: 'asc' } });
    assert.throws(() => readUnique({ kind: 'unique', keys: 'region' }), /its keys are a list of column names/);
  });
});

describe("a Test (checks.ts): each check's failing rows counted by the database, in one row", () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), COLUMNS) };
  const apply = (s: CubeSnapshot, commands: readonly unknown[]): CubeSnapshot =>
    applyGridCommands({ snapshot: s, config: DEFAULT_CONFIGURATION }, commands as never).snapshot;
  const checks: Check[] = [
    { kind: 'notEmpty', column: 'region' }, { kind: 'unique', column: 'region' }, { kind: 'between', column: 'notional', min: 0, max: 1000 },
  ];

  it('judges a test of no rows -- one row of 0 rows, every count none: its count of rows fails (the review of 2026-10-26)', () => {
    const atLeast: Check[] = [{ kind: 'notEmpty', column: 'region' }, { kind: 'rows', min: 1 }];
    const row: Record<string, unknown> = { Rows: 0, region_empty: null, Rows_outside: 1 };
    assert.deepEqual(failingChecks(atLeast, (n) => row[n]).map((f) => [f.check.kind, f.rows]), [['rows', 0]]);
    // a row of rows lacking a count is no row of this test
    assert.throws(() => failingChecks(atLeast, (n) => ({ ...row, Rows: 3 })[n]), /a test's row has no count 'region_empty'/);
  });

  it('makes a count of each check, grouped by a column of one value, every column it read hidden; read back whole', () => {
    const commands = checkCommands(typed, checks);
    const s = apply(typed, commands);
    assert.deepEqual(s.rows, [CHECKS_COLUMN]);
    assert.deepEqual(checksOf(s), checks);
    assert.deepEqual(testColumns(checks), ['region_empty', 'region_repeated', 'region__row', 'notional_outside', 'Rows', 'ChecksAll']);
    const hidden = (commands.at(-1) as unknown as { patch: { columns: Record<string, unknown> } }).patch.columns;
    assert.deepEqual(Object.keys(hidden), ['region', 'notional', 'ChecksAll', 'region__row']);
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: s.derived, rows: s.rows }))),
      { tool: 'test', also: [], says: 'region has no empty value, region repeats no value, notional from 0 to 1000' });
    // a bound on one side alone, and a fraction, read back
    const atMost = apply(typed, checkCommands(typed, [{ kind: 'between', column: 'notional', max: -5 }]));
    assert.deepEqual(checksOf(atMost), [{ kind: 'between', column: 'notional', max: -5 }]);
    const fraction = apply(typed, checkCommands(typed, [{ kind: 'between', column: 'notional', min: 0, max: 0.15 }]));
    assert.deepEqual(checksOf(fraction), [{ kind: 'between', column: 'notional', min: 0, max: 0.15 }]);
    assert.equal(checkWords({ kind: 'between', column: 'notional', min: 2 }), 'notional at least 2');
    // set again: its own columns taken off first, its checks the new ones
    const again = apply(s, checkCommands(s, [{ kind: 'notEmpty', column: 'notional' }]));
    assert.deepEqual(checksOf(again), [{ kind: 'notEmpty', column: 'notional' }]);
    assert.deepEqual(again.derived.map((d) => d.name), ['notional_empty', 'Rows', 'ChecksAll']);
  });

  it('says which checks fail, by the counts its row has; refuses in words', () => {
    const counts: Record<string, number> = { region_empty: 2, region_repeated: 0, notional_outside: 1, Rows: 5 };
    assert.deepEqual(failingChecks(checks, (c) => counts[c]).map((f) => [f.check.kind, f.rows]), [['notEmpty', 2], ['between', 1]]);
    // a row lacking a check's count is no row of this test: thrown, never read as none failing (AGENTS.md invariant 4)
    assert.throws(() => failingChecks(checks, (c) => ({ ...counts, region_repeated: undefined })[c]),
      /a test's row has no count 'region_repeated' \(region repeats no value\): it is no row of this test/);
    // a value repeated counts its rows after its first; an empty value repeats nothing (the correctness audit's 8)
    const unique = checkCommands(typed, [{ kind: 'unique', column: 'region' }])
      .flatMap((c) => (c.kind === 'calculate' && c.column.name === 'region_repeated' ? [c.column] : []));
    assert.equal(JSON.stringify(unique[0]!.lambda!.body[0]), JSON.stringify(fn('if', fn('and', fn('isNotEmpty', property(variable('x'), 'region')),
      fn('greaterThan', fn('toOne', property(variable('x'), 'region__row')), lit.integer(1))), lambda([], lit.integer(1)), lambda([], lit.integer(0)))));
    refused(() => checkCommands(typed, []), /a test checks something: add a check/);
    refused(() => checkCommands(typed, [{ kind: 'notEmpty', column: 'nope' }]), /the grid has no column 'nope'/);
    refused(() => checkCommands(typed, [{ kind: 'between', column: 'region', min: 1 }]), /'region' is String: only a number is checked within bounds/);
    refused(() => checkCommands(typed, [{ kind: 'between', column: 'notional' }]), /its smallest, its largest, or both/);
    refused(() => checkCommands(typed, [{ kind: 'between', column: 'notional', min: 5, max: 1 }]), /its smallest is above its largest/);
    refused(() => checkCommands(typed, [checks[0]!, checks[0]!]), /'region' is checked that way twice/);
    assert.deepEqual(readChecks({ kind: 'test', checks: [{ kind: 'between', column: 'notional', min: 1 }] }), [{ kind: 'between', column: 'notional', min: 1 }]);
    refused(() => readChecks({ kind: 'test', checks: [{ kind: 'unique', column: 'region', min: 1 }] }), /a unique check takes no min/);
    refused(() => readChecks({ kind: 'test', checks: [{ kind: 'sometimes', column: 'region' }] }), /a check is notEmpty, unique, between/);
  });

  it('checks a list of values and a pattern: each counted as the others are; read back, said, refused in words', () => {
    const listed: Check[] = [{ kind: 'oneOf', column: 'region', values: ['EMEA', 'APAC'] }, { kind: 'matches', column: 'region', pattern: '^[A-Z]{4}$' },
      { kind: 'oneOf', column: 'notional', values: [1, 2.5] }];
    const s = apply(typed, checkCommands(typed, listed));
    assert.deepEqual(checksOf(s), listed);
    assert.deepEqual(testColumns(listed).slice(0, 3), ['region_unexpected', 'region_unmatched', 'notional_unexpected']);
    assert.deepEqual(listed.map(checkWords), ['region is one of EMEA, APAC', 'region matches ^[A-Z]{4}$', 'notional is one of 1, 2.5']);
    refused(() => checkCommands(typed, [{ kind: 'oneOf', column: 'region', values: [] }]), /list the values 'region' may hold/);
    refused(() => checkCommands(typed, [{ kind: 'oneOf', column: 'region', values: [3] }]), /'region' holds text: 3 is none/);
    refused(() => checkCommands(typed, [{ kind: 'oneOf', column: 'region', values: ['A', 'A'] }]), /a value of 'region' is listed twice/);
    refused(() => checkCommands(typed, [{ kind: 'matches', column: 'notional', pattern: 'x' }]), /only a text matches a pattern/);
    refused(() => checkCommands(typed, [{ kind: 'matches', column: 'region', pattern: '' }]), /give the pattern 'region' matches/);
    assert.deepEqual(readChecks({ kind: 'test', checks: [{ kind: 'oneOf', column: 'region', values: ['EMEA'] }, { kind: 'matches', column: 'region', pattern: 'E' }] }),
      [{ kind: 'oneOf', column: 'region', values: ['EMEA'] }, { kind: 'matches', column: 'region', pattern: 'E' }]);
    refused(() => readChecks({ kind: 'test', checks: [{ kind: 'matches', column: 'region', values: ['E'] }] }), /a matches check takes no values/);
  });

  it('checks its count of rows after grouping: a column over its group, 1 outside its bounds; read back, said', () => {
    const counted: Check[] = [{ kind: 'notEmpty', column: 'region' }, { kind: 'rows', min: 1, max: 500 }];
    const s = apply(typed, checkCommands(typed, counted));
    assert.deepEqual(s.groupDerived?.map((d) => d.name), ['Rows_outside']);
    assert.deepEqual(checksOf(s), counted);
    assert.deepEqual(testColumns(counted), ['region_empty', 'Rows_outside', 'Rows', 'ChecksAll']);
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: s.derived, groupDerived: s.groupDerived ?? [], rows: s.rows }))),
      { tool: 'test', also: [], says: 'region has no empty value, rows from 1 to 500' });
    assert.deepEqual(checksOf(apply(typed, checkCommands(typed, [{ kind: 'rows', max: 1 }]))), [{ kind: 'rows', max: 1 }]);
    assert.equal(checkWords({ kind: 'rows', min: 1 }), 'at least 1 row');
    assert.equal(checkWords({ kind: 'rows', max: 10 }), 'at most 10 rows');
    // set again: its column over its group taken off too
    const again = apply(s, checkCommands(s, [{ kind: 'rows', min: 2 }]));
    assert.deepEqual(checksOf(again), [{ kind: 'rows', min: 2 }]);
    assert.deepEqual(again.groupDerived?.map((d) => d.name), ['Rows_outside']);
    assert.deepEqual(again.derived.map((d) => d.name), ['Rows', 'ChecksAll']);
    // failing: its count's 1, said with its rows; no row (its input has none) fails a smallest, not a largest alone
    const failing = failingChecks(counted, (c) => ({ region_empty: 0, Rows_outside: 1, Rows: 900 } as Record<string, number>)[c]);
    assert.deepEqual(failing.map((f) => failingWords(f)), ['rows from 1 to 500: it has 900 rows']);
    assert.deepEqual(failingChecks(counted, () => undefined).map((f) => failingWords(f)), ['rows from 1 to 500: it has 0 rows']);
    assert.deepEqual(failingChecks([{ kind: 'rows', max: 3 }], () => undefined), []);
    assert.equal(failingWords({ check: counted[0]!, rows: 1 }), 'region has no empty value: 1 row fails');
    refused(() => checkCommands(typed, [{ kind: 'rows' }]), /a count of rows is bounded/);
    refused(() => checkCommands(typed, [{ kind: 'rows', min: 0 }]), /a smallest is 1 or more, or left out/);
    refused(() => checkCommands(typed, [{ kind: 'rows', min: 1.5 }]), /a count of rows is a whole number, not 1.5/);
    refused(() => checkCommands(typed, [{ kind: 'rows', min: 9, max: 2 }]), /its smallest is above its largest/);
    refused(() => checkCommands(typed, [{ kind: 'rows', min: 1 }, { kind: 'rows', max: 9 }]), /its count of rows is checked twice/);
    assert.deepEqual(readChecks({ kind: 'test', checks: [{ kind: 'rows', min: 1 }] }), [{ kind: 'rows', min: 1 }]);
    refused(() => readChecks({ kind: 'test', checks: [{ kind: 'rows', column: 'region', min: 1 }] }), /a rows check takes no column/);
  });
});

describe('a Custom Filter (custom-filter.ts): the rows a condition in Pure is true for, its column hidden', () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), COLUMNS) };
  const apply = (s: CubeSnapshot, commands: readonly unknown[]): CubeSnapshot =>
    applyGridCommands({ snapshot: s, config: DEFAULT_CONFIGURATION }, commands as never).snapshot;
  const x = variable('x');
  const big = lambda(['x'], fn('and', fn('greaterThan', fn('toOne', property(x, 'notional')), lit.integer(100)),
    fn('equal', property(x, 'region'), lit.string('EMEA'))));

  it('makes its column and the filter on it alone, read back; set again, its column edited', () => {
    const had: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'APAC' };
    const commands = customFilterCommands({ ...typed, filter: had }, big, 'Boolean');
    const s = apply({ ...typed, filter: had }, commands);
    assert.deepEqual(s.filter, { kind: 'condition', column: CUSTOM_FILTER_COLUMN, operator: 'equal', value: true }, 'in place of what it had');
    assert.deepEqual(customFilterOf(s), { lambda: big, reads: ['notional', 'region'] });
    const hidden = (commands.at(-1) as unknown as { patch: { columns: Record<string, unknown> } }).patch.columns;
    assert.deepEqual(hidden, { [CUSTOM_FILTER_COLUMN]: { hidden: true } });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: s.derived, filter: s.filter! }), { columns: { [CUSTOM_FILTER_COLUMN]: { hidden: true } } })),
      { tool: 'customFilter', also: [], says: 'a condition on notional, region' });
    const small = lambda(['x'], fn('lessThan', fn('toOne', property(x, 'notional')), lit.integer(5)));
    const again = customFilterCommands(s, small, 'Boolean');
    assert.equal(again[0]!.kind, 'editCalculated');
    assert.deepEqual(customFilterOf(apply(s, again))?.reads, ['notional']);
    assert.equal(customFilterOf({ derived: s.derived, filter: had }), undefined, 'another filter: no Custom Filter');
  });

  it('refuses a condition that is no Boolean and a name taken; its text and JSON read', () => {
    refused(() => customFilterCommands(typed, big, 'Float'), /a condition is true or false, a Boolean: this one is Float/);
    const taken = { ...typed, derived: [{ name: CUSTOM_FILTER_COLUMN, window: { fn: 'rowNumber', partition: [], order: [] } }] } as never;
    refused(() => customFilterCommands(taken, big, 'Boolean'), /'CustomFilter' is already a column/);
    assert.equal(customFilterText(' $x.a > 1 '), 'x|$x.a > 1');
    assert.equal(customFilterText('r|$r.a > 1'), 'r|$r.a > 1');
    assert.equal(customFilterText('true || $x.flag'), 'x|true || $x.flag', "a name and ||: an or, no lambda's head");
    assert.deepEqual(readCustomFilter({ kind: 'customFilter', expression: '$x.a > 1' }), { expression: '$x.a > 1' });
    refused(() => readCustomFilter({ kind: 'customFilter', expression: ' ' }), /its condition in Pure/);
    refused(() => readCustomFilter({ kind: 'customFilter', expression: '$x.a', column: 'a' }), /a custom filter takes no column/);
    // the grid with its column as it would be, for the compiler to type
    assert.deepEqual(customFilterProbe(typed, big).derived.map((d) => d.name), [CUSTOM_FILTER_COLUMN]);
  });
});

describe('a Binning (binning.ts): number columns each into N bins of equal width, the bins preset', () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), COLUMNS) };
  const apply = (s: CubeSnapshot, commands: readonly unknown[]): CubeSnapshot =>
    applyGridCommands({ snapshot: s, config: DEFAULT_CONFIGURATION }, commands as never).snapshot;
  it('makes the bins preset of each, its helpers hidden; read back whole, said', () => {
    const numbers = rowColumns(typed).filter((c) => c.type === 'Float' || c.type === 'Integer').map((c) => c.name);
    assert.ok(numbers.length > 0);
    const column = numbers[0]!;
    const commands = binningCommands(typed, { columns: [column], bins: 4 });
    const s = apply(typed, commands);
    const bins = binsOf(s.derived);
    assert.deepEqual([...bins], [[`${column}_bins`, { column, bins: 4 }]]);
    const hidden = (commands.at(-1) as unknown as { patch: { columns: Record<string, unknown> } }).patch.columns;
    assert.deepEqual(Object.keys(hidden), [`${column}_bins__min`, `${column}_bins__max`, `${column}_bins__upper`]);
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: s.derived }), { columns: hidden })),
      { tool: 'binning', also: [], says: `${column} in 4 bins` });
    // its columns as the compiler types them: read back the same
    assert.deepEqual([...binsOf(s.derived.map((d) => ({ ...d, type: 'Float' })))], [[`${column}_bins`, { column, bins: 4 }]]);
    // a name taken moves on, as the preset's does
    const again = apply(s, binningCommands(s, { columns: [column], bins: 10 }));
    assert.deepEqual([...binsOf(again.derived).values()].map((b) => b.bins), [4, 10]);
  });
  it('refuses in words; reads its JSON', () => {
    refused(() => binningCommands(typed, { columns: [], bins: 3 }), /pick a number column/);
    refused(() => binningCommands(typed, { columns: ['region'], bins: 3 }), /'region' is String: only a number is put into bins/);
    refused(() => binningCommands(typed, { columns: ['nope'], bins: 3 }), /the grid has no column 'nope'/);
    refused(() => binningCommands(typed, { columns: ['region'], bins: 1.5 }), /a count of bins is a whole number from 1/);
    assert.deepEqual(readBinning({ kind: 'binning', columns: ['a'], bins: 3 }), { columns: ['a'], bins: 3 });
    refused(() => readBinning({ kind: 'binning', columns: ['a'], bins: 3, width: 2 }), /a binning takes no width/);
  });
});

describe('a Generate Rows (generate-rows.ts): each row repeated by an exploded range the database makes', () => {
  const plain = rawRows(accessor('local::DB', 'trades'), COLUMNS);
  const typed: CubeSnapshot = { ...plain, columns: [...plain.columns, { name: 'n', type: 'Integer' }, { name: 'opened', type: 'StrictDate' }] };
  const apply = (s: CubeSnapshot, commands: readonly unknown[]): CubeSnapshot =>
    applyGridCommands({ snapshot: s, config: DEFAULT_CONFIGURATION }, commands as never).snapshot;
  const whole = rowColumns(typed).find((c) => c.type === 'Integer')?.name;
  const date = rowColumns(typed).find((c) => c.type === 'StrictDate')?.name;
  it('makes its exploded column of numbers, given or its own, read back whole and said; set again, its own replaced', () => {
    const s = apply(typed, generateRowsCommands(typed, { kind: 'numbers', name: 'N', from: 1, to: 10 }));
    const made = s.derived.find((d) => d.name === 'N');
    assert.equal(made?.unnest, true);
    assert.deepEqual(generatedOf(s.derived), { kind: 'numbers', name: 'N', from: 1, to: 10 });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: s.derived }))), { tool: 'generateRows', also: [], says: 'N from 1 to 10' });
    assert.ok(whole !== undefined);
    const again = apply(s, generateRowsCommands(s, { kind: 'numbers', name: 'N', from: 0, to: { column: whole! }, step: 2 }));
    assert.deepEqual(generatedOf(again.derived), { kind: 'numbers', name: 'N', from: 0, to: { column: whole! }, step: 2 });
    assert.deepEqual(again.derived.map((d) => d.name), ['N']);
    assert.equal(generatedWords(generatedOf(again.derived)!), `N from 0 to ${whole}, by 2`);
  });
  it('makes its days between two dates, its number hidden; refuses in words; reads its JSON', () => {
    assert.ok(date !== undefined);
    const commands = generateRowsCommands(typed, { kind: 'days', name: 'day', from: date!, to: date! });
    const s = apply(typed, commands);
    assert.deepEqual(generatedOf(s.derived), { kind: 'days', name: 'day', from: date, to: date });
    assert.deepEqual((commands.at(-1) as unknown as { patch: { columns: Record<string, unknown> } }).patch.columns, { day__day: { hidden: true } });
    refused(() => generateRowsCommands(typed, { kind: 'numbers', name: 'N', from: 5, to: 1 }), /its first is after its last/);
    refused(() => generateRowsCommands(typed, { kind: 'numbers', name: 'N', from: 1.5, to: 3 }), /a bound is a whole number, not 1.5/);
    refused(() => generateRowsCommands(typed, { kind: 'numbers', name: 'N', from: 1, to: 3, step: 0 }), /a step is a whole number from 1/);
    refused(() => generateRowsCommands(typed, { kind: 'numbers', name: 'region', from: 1, to: 3 }), /'region' is already a column/);
    refused(() => generateRowsCommands(typed, { kind: 'numbers', name: 'N', from: { column: 'region' }, to: 3 }), /'region' is String: a bound is a whole number/);
    refused(() => generateRowsCommands(typed, { kind: 'days', name: 'd', from: 'region', to: date! }), /the days are between two date columns/);
    // a moment's days would run past the last (the correctness audit's 12, 2026-10-26): its day taken first
    const timed: CubeSnapshot = { ...typed, columns: [...typed.columns, { name: 'closed', type: 'DateTime' }] };
    refused(() => generateRowsCommands(timed, { kind: 'days', name: 'd', from: date!, to: 'closed' }),
      /'closed' is a date and a time: the days are between two days -- take it to its day first/);
    refused(() => generateRowsCommands(typed, { kind: 'numbers', name: 'N', from: 0, to: 1_000_000 }), /more than 100,000 rows of each/);
    assert.deepEqual(readGenerateRows({ kind: 'generateRows', name: 'N', from: 1, to: { column: 'n' }, step: 2 }),
      { kind: 'numbers', name: 'N', from: 1, to: { column: 'n' }, step: 2 });
    assert.deepEqual(readGenerateRows({ kind: 'generateRows', name: 'd', days: { from: 'a', to: 'b' } }), { kind: 'days', name: 'd', from: 'a', to: 'b' });
    refused(() => readGenerateRows({ kind: 'generateRows', name: 'N', from: 'x', to: 1 }), /a bound is a whole number or \{column\}/);
  });
});

describe('a Pure box (pure-box.ts, setPure): its rows a function of its input\'s, its source\'s own', () => {
  /** `$p->filter(x|$x.notional > 100)->limit(5)`, its parameter `p`. */
  const pure = (p = 'input') => lambda([p], fn('limit', fn('filter', variable(p), lambda(['x'], gt(property(variable('x'), 'notional'), lit.integer(100)))), lit.integer(5)));
  const input = accessor('local::DB', 'trades');
  const sourceOf = (page: PageDocument, grid: string): CubeSource => {
    const view = page.views.find((v) => v.id === grid);
    return page.cubes.find((c) => c.id === (view as { cube: string }).cube)!.cube.source;
  };

  it('its text as the compiler parses it, and as its window shows it again', () => {
    assert.equal(pureText('  $input->limit(5) '), 'input|$input->limit(5)');
    assert.equal(pureText('rows | $rows->limit(5)'), 'rows | $rows->limit(5)');
    assert.equal(pureText('true || false'), 'input|true || false', "a name and ||: an or, no lambda's head");
    assert.equal(pureShown('{input|$input->limit(5)}'), '$input->limit(5)');
    assert.equal(pureShown('{rows|$rows->limit(5)}'), 'rows|$rows->limit(5)');
    assert.equal(pureShown(' input | $input '), '$input');
  });

  it('applies its function to its input, a lambda naming its parameter the same reading its own; reads its input back', () => {
    const over = pureOver(pure(), input);
    assert.deepEqual(over, fn('limit', fn('filter', input, lambda(['x'], gt(property(variable('x'), 'notional'), lit.integer(100)))), lit.integer(5)));
    assert.deepEqual(pureInputOf(pure(), over), input);
    assert.deepEqual(boxRows({}, input), input, 'no Pure: its input as it is');
    assert.deepEqual(boxRows({ pure: pure() }, input), over);
    // shadowed: the inner lambda's $input is its own row
    const shadowed = lambda(['input'], fn('filter', variable('input'), lambda(['input'], gt(property(variable('input'), 'notional'), lit.integer(1)))));
    assert.deepEqual(pureOver(shadowed, input), fn('filter', input, lambda(['input'], gt(property(variable('input'), 'notional'), lit.integer(1)))));
    assert.deepEqual(pureInputOf(shadowed, pureOver(shadowed, input)), input);
    assert.deepEqual(pureInput({ name: 'b', pure: pure() }, over), input);
    assert.deepEqual(pureInput({ name: 'b' }, over), over, 'no Pure: its rows are its input\'s');
    assert.equal(pureInputOf(pure(), input), undefined, 'rows no Pure made');
  });

  it("is taken apart into its steps (Import Pure, FL12): each a box's function of the rows before it", () => {
    // limit(filter($rows, ...), 5): a filter, then a limit -- each over $input
    const steps = pureSteps(pure('rows'));
    assert.deepEqual(steps, [
      lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), lit.integer(100))))),
      lambda(['input'], fn('limit', variable('input'), lit.integer(5))),
    ]);
    // the steps over one another, the rows the whole gives
    const input = fn('getAll', variable('t'));
    assert.deepEqual(pureOver(steps[1]!, pureOver(steps[0]!, input)), pureOver(pure('rows'), input));
    // a step that reads its input elsewhere too stays whole with what it reads, the steps after it their own
    const self = lambda(['input'], fn('limit', fn('join', variable('input'), variable('input')), lit.integer(3)));
    assert.deepEqual(pureSteps(self), [lambda(['input'], fn('join', variable('input'), variable('input'))), lambda(['input'], fn('limit', variable('input'), lit.integer(3)))]);
    // one step: itself
    assert.deepEqual(pureSteps(lambda(['input'], fn('limit', variable('input'), lit.integer(1)))), [lambda(['input'], fn('limit', variable('input'), lit.integer(1)))]);
    assert.throws(() => pureSteps(lambda(['a', 'b'], variable('a'))), /takes one parameter/);
  });

  it('judges its shape in words: one parameter, one expression, its input read', () => {
    assert.equal(pureProblem(pure()), undefined);
    assert.match(pureProblem({ _type: 'var', name: 'x' })!, /a Pure box's function is a lambda/);
    assert.match(pureProblem(lambda(['a', 'b'], variable('a')))!, /takes one parameter, its input's rows: this one takes 2/);
    assert.match(pureProblem(lambda(['a'], variable('a'), variable('a')))!, /one expression: this one is 2/);
    assert.match(pureProblem(lambda(['input'], lit.integer(1)))!, /its Pure never reads \$input, its input's rows: a box of rows of its own is a Text Input/);
    // read only under a lambda of its own naming it: never read
    assert.match(pureProblem(lambda(['input'], lambda(['input'], variable('input'))))!, /never reads \$input/);
  });

  it('says the functions its rows go through, in turn', () => {
    assert.equal(pureWords(pure()), 'filter → limit');
    assert.equal(pureWords(lambda(['input'], variable('input'))), 'its input as it is');
    const long = lambda(['i'], fn('limit', fn('distinct', fn('sort', fn('extend', fn('filter', variable('i')))))));
    assert.equal(pureWords(long), 'filter → extend → sort and 2 more');
    assert.equal(pureWords(lambda(['i'], fn('meta::pure::functions::relation::limit', variable('i')))), 'limit');
  });

  it('parses, judges and types its rows before it is set (pureFrom): the compiler\'s words, or DataCube\'s', async () => {
    const asked: string[] = [];
    const compiler = (parsed: Lambda) => ({
      parse: async (text: string) => { asked.push(text); return parsed; },
      columns: async (r: ValueSpecification) => { asked.push(JSON.stringify(r) === JSON.stringify(pureOver(parsed, input)) ? 'typed' : 'other'); return [{ name: 'region' }]; },
    });
    assert.deepEqual(await pureFrom('$input->limit(5)', input, compiler(pure())), pure());
    assert.deepEqual(asked, ['input|$input->limit(5)', 'typed']);
    await assert.rejects(pureFrom('1', input, compiler(lambda(['input'], lit.integer(1)))), (e: unknown) => e instanceof CubeRefusal && /never reads/.test(e.message));
    await assert.rejects(pureFrom('$input', input, { ...compiler(pure()), columns: async () => [] }),
      (e: unknown) => e instanceof CubeRefusal && /its Pure gives no rows with columns/.test(e.message));
    // a value is typed as one column, `value`: asked again with it selected, which only a relation's rows take
    let asks = 0;
    const scalar = { ...compiler(pure()), columns: async () => { asks += 1; if (asks > 1) throw new Error('no select of an Integer'); return [{ name: 'value' }]; } };
    await assert.rejects(pureFrom('$input->count()', input, scalar),
      (e: unknown) => e instanceof CubeRefusal && /its Pure gives a value, not rows: .*no select of an Integer/.test(e.message));
    const relation = { ...compiler(pure()), columns: async () => [{ name: 'value' }] };
    assert.deepEqual(await pureFrom('$input', input, relation), pure(), 'rows of one column called value: a relation');
  });

  it('is set on a box over another grid\'s rows (setPure), taken off, refused in words; read, written, renamed, its box says it', () => {
    const d = apply(FLOW(), { kind: 'setPure', grid: 'grid-3', pure: pure() });
    const set = sourceOf(d.page, 'grid-3');
    assert.ok(set._type === 'rows');
    assert.deepEqual(set.pure, pure());
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(d.page)))), pageToJson(d.page), 'written and read back the same');
    // its box says its Pure first, its own filter after it
    const view = d.page.views.find((v) => v.id === 'grid-3') as { cube: string };
    assert.deepEqual(boxSays(d.page.cubes.find((c) => c.id === view.cube)!.cube), { tool: 'pure', also: ['filter'], says: 'filter → limit' });
    refused(() => apply(d, { kind: 'setPure', grid: 'grid-3', pure: pure() }), /'grid-3' is so already/);
    refused(() => apply(FLOW(), { kind: 'setPure', grid: 'grid-3', pure: null }), /'grid-3' has no Pure/);
    refused(() => apply(FLOW(), { kind: 'setPure', grid: 'grid-2', pure: pure() }), /'grid-2' reads trades.csv: a Pure box reads another grid's rows/);
    refused(() => apply(FLOW(), { kind: 'setPure', grid: 'grid-3', pure: lambda(['a', 'b'], variable('a')) }), /takes one parameter/);
    const off = apply(d, { kind: 'setPure', grid: 'grid-3', pure: null });
    assert.equal((sourceOf(off.page, 'grid-3') as RowsSource).pure, undefined);
    assert.equal(pageToJson(off.page), pageToJson(FLOW().page), 'taken off: as it was');
    // read as JSON carries it, refused in words
    const c: PageCommand = { kind: 'setPure', grid: 'grid-3', pure: pure() };
    assert.deepEqual(readPageCommand(JSON.parse(JSON.stringify(c))), c);
    assert.deepEqual(readPageCommand({ kind: 'setPure', grid: 'grid-3', pure: null }), { kind: 'setPure', grid: 'grid-3', pure: null });
    refused(() => readPageCommand({ kind: 'setPure', grid: 'grid-3', pure: 'input|$input' }), /a Pure box's function is a lambda/);
    assert.deepEqual(renamedCommand(c, new Map([['grid-3', 'grid-9']])), { ...c, grid: 'grid-9' });
    // the page between: its Pure set, and taken off again
    const back = applyPlanned({ page: d.page, held: idsOf(d.page) }, pageBetween(d.page, FLOW().page, 'history'), false).draft.page;
    assert.equal(pageToJson(back), pageToJson(FLOW().page));
    const again = applyPlanned({ page: FLOW().page, held: idsOf(FLOW().page) }, pageBetween(FLOW().page, d.page, 'history'), false).draft.page;
    assert.equal(pageToJson(again), pageToJson(d.page));
  });
});

describe("a JSON Parse: a calculated column reading a field of a JSON column", () => {
  it('is drawn as a JSON Parse, saying which field of which column; any other column a Formula', () => {
    const x = variable('x');
    const field = (body: ValueSpecification): DerivedColumn => ({ name: 'tier', lambda: lambda(['x'], body), kind: 'dimension' });
    const deep = fn('get', fn('get', property(x, 'customer'), lit.string('account')), lit.string('tier'));
    assert.equal(jsonFieldOf(field(fn('to', deep, fn('cast', lit.string('')))) ), 'customer');
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: [field(deep)] }))), { tool: 'jsonParse', also: [], says: 'tier from customer' });
    // a text's own get, of no column of the row, and a formula with none: no JSON Parse
    assert.equal(jsonFieldOf(field(fn('get', lit.string('{}'), lit.string('a')))), undefined);
    assert.equal(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: [field(fn('plus', property(x, 'notional'), lit.integer(1)))] }))).tool, 'formula');
  });
});

describe("a Date Filter (date-ranges.ts): a range relative to today, or the days between two", () => {
  it('is read back off its filter, a Date Filter box saying it; any other filter a Filter box', () => {
    for (const range of DATE_RANGES) {
      const [a, b] = dateRangeConditions('trade_date', range);
      assert.deepEqual(datesKept({ kind: 'and', children: [a!, b!] }), { column: 'trade_date', range });
    }
    const between = { kind: 'and', children: datesBetweenConditions('trade_date', '2024-01-01', '2024-06-30', 'StrictDate') } as const;
    assert.deepEqual(datesKept(between), { column: 'trade_date', from: '2024-01-01', to: '2024-06-30' });
    const box = (filter: FilterNode) => boxSays(cube(rows('grid-1'), (q) => ({ ...q, filter })));
    assert.deepEqual(box(between), { tool: 'dateFilter', also: [], says: 'trade_date from 2024-01-01 to 2024-06-30' });
    assert.deepEqual(box({ kind: 'and', children: dateRangeConditions('trade_date', 'last30Days') }),
      { tool: 'dateFilter', also: [], says: 'trade_date: Last 30 Days' });
    // one condition, two columns, or a third condition: a Filter's
    const one = datesBetweenConditions('trade_date', '2024-01-01', '2024-06-30', 'StrictDate')[0]!;
    assert.equal(box(one).tool, 'filter');
    assert.equal(datesKept({ kind: 'and', children: [one, { kind: 'condition', column: 'other', operator: 'lessThanEqual', value: '2024-06-30' }] }), undefined);
    assert.equal(datesKept({ kind: 'and', children: [...between.children, one] }), undefined);
    assert.equal(datesKept({ kind: 'or', children: [...between.children] }), undefined);
  });

  it("a date-time column's days are every moment of them: from the first's midnight to the next day's after the last", () => {
    // the correctness audit's 5, 2026-10-26: a bare day was no literal of a date-time, and `<= to` left out its last day
    assert.deepEqual(datesBetweenConditions('at', '2024-06-01', '2024-06-30', 'DateTime'), [
      { kind: 'condition', column: 'at', operator: 'greaterThanEqual', value: '2024-06-01T00:00:00' },
      { kind: 'condition', column: 'at', operator: 'lessThan', value: '2024-07-01T00:00:00' },
    ]);
    assert.equal((datesBetweenConditions('at', '2024-02-01', '2024-02-29', 'DateTime')[1] as { value: unknown }).value, '2024-03-01T00:00:00', 'a leap day');
    assert.equal((datesBetweenConditions('at', '2024-12-31', '2024-12-31', 'DateTime')[1] as { value: unknown }).value, '2025-01-01T00:00:00', 'a year on');
    // a day column's as it was: both days, as written
    assert.deepEqual(datesBetweenConditions('d', '2024-06-01', '2024-06-30', 'StrictDate'), datesBetweenConditions('d', '2024-06-01', '2024-06-30', undefined));
    // read back as the days it keeps, said so
    const timed = { kind: 'and', children: datesBetweenConditions('at', '2024-02-01', '2024-02-29', 'DateTime') } as const;
    assert.deepEqual(datesKept(timed), { column: 'at', from: '2024-02-01', to: '2024-02-29' });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, filter: timed }))), { tool: 'dateFilter', also: [], says: 'at from 2024-02-01 to 2024-02-29' });
    assert.throws(() => datesBetweenConditions('t', '2024-06-01', '2024-06-30', 'StrictTime'), /'t' is a time of day: it has no days to keep/);
  });
});

describe("Sample's other ways (row-sample.ts): a row number and a filter, as Unique's", () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), COLUMNS) };
  const columnsOf = (c: readonly unknown[]) => (c as { kind: string; column?: DerivedColumn }[]).filter((x) => x.kind === 'calculate').map((x) => x.column!);

  it('numbers its rows in an order, per group, and filters on the number: first, last, all but the first, one of every', () => {
    const first = rowSampleCommands(typed, { kind: 'first', n: 2, by: 'notional', groups: ['region'] });
    assert.deepEqual(columnsOf(first)[0]!.window, { fn: 'rowNumber', partition: ['region'], order: [{ column: 'notional', direction: 'asc' }] });
    assert.deepEqual(first.find((c) => c.kind === 'addFilter'), { kind: 'addFilter', condition: { kind: 'condition', column: 'SampleRow', operator: 'lessThanEqual', value: 2 } });
    assert.deepEqual(first.at(-1), { kind: 'configure', patch: { columns: { SampleRow: { hidden: true } } } });
    // the last: falling, its empties after every value -- a flag of them first (unique.ts rowNumberOrder; the audit's 3)
    const last = rowSampleCommands(typed, { kind: 'last', n: 1, by: 'notional', groups: ['region'] });
    assert.deepEqual(columnsOf(last).map((c) => c.name), ['SampleEmpty', 'SampleRow']);
    assert.deepEqual(columnsOf(last)[1]!.window?.order, [{ column: 'SampleEmpty', direction: 'asc' }, { column: 'notional', direction: 'desc' }]);
    assert.deepEqual(rowSampleOf({ derived: columnsOf(last), filter: (last.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition }),
      { kind: 'last', n: 1, by: 'notional', groups: ['region'], columns: ['SampleEmpty', 'SampleRow'] });
    const skip = rowSampleCommands(typed, { kind: 'skip', n: 10, by: 'notional', groups: [] });
    assert.equal((skip.find((c) => c.kind === 'addFilter') as { condition: { operator: string } }).condition.operator, 'greaterThan');
    const every = rowSampleCommands(typed, { kind: 'every', n: 3, by: 'notional', groups: [] });
    const [row, rem] = columnsOf(every);
    assert.equal(JSON.stringify(rem!.lambda?.body[0]), JSON.stringify(
      fn('mod', fn('toOne', property(variable('x'), 'SampleRow')), lit.integer(3))));
    assert.equal(row!.window?.fn, 'rowNumber');
    // its ties broken by every other column that sorts (the audit's 14): one of every 3 the same rows each query
    assert.deepEqual(row!.window?.order, [{ column: 'notional', direction: 'asc' }, { column: 'region', direction: 'asc' }]);
  });

  it('is read back off its query, a Sample box saying what it keeps; its columns none of a Formula or a Select', () => {
    const says = (commands: ReturnType<typeof rowSampleCommands>) => {
      const derived = columnsOf(commands);
      const filter = (commands.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition;
      const hidden = Object.fromEntries(derived.map((d) => [d.name, { hidden: true }]));
      return boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived, filter }), { columns: hidden }));
    };
    assert.deepEqual(says(rowSampleCommands(typed, { kind: 'first', n: 2, by: 'notional', groups: ['region'] })),
      { tool: 'sample', also: [], says: 'first 2 of each region by notional' });
    assert.deepEqual(says(rowSampleCommands(typed, { kind: 'every', n: 3, by: 'notional', groups: [] })),
      { tool: 'sample', also: [], says: 'one of every 3 by notional' });
    assert.deepEqual(says(rowSampleCommands(typed, { kind: 'skip', n: 10, by: 'notional', groups: [] })),
      { tool: 'sample', also: [], says: 'all but the first 10 by notional' });
    const every = rowSampleCommands(typed, { kind: 'every', n: 3, by: 'notional', groups: ['region'] });
    const read = rowSampleOf({ derived: columnsOf(every), filter: (every.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition });
    assert.deepEqual(read, { kind: 'every', n: 3, by: 'notional', groups: ['region'], columns: ['SampleRow', 'SampleEvery'] });
  });

  it('keeps the rows at some places (Select Records): its ranges or-ed over the number, read back and said; refused in words', () => {
    assert.deepEqual(readRanges('1-10, 20, 30+, -5'), [{ from: 1, to: 10 }, { from: 20, to: 20 }, { from: 30 }, { from: 1, to: 5 }]);
    assert.equal(rangesWords(readRanges(' 1-10,20 , 30+')), '1-10, 20, 30+');
    refused(() => readRanges('ten'), /'ten' is no range of rows/);
    refused(() => readRanges(' , '), /name the rows it keeps/);
    const t = { kind: 'records' as const, n: 3, by: 'notional', groups: [], ranges: readRanges('1-10, 20, 30+') };
    const commands = rowSampleCommands(typed, t);
    const filter = (commands.find((c) => c.kind === 'refilter') as { readonly add: readonly FilterNode[] }).add[0]!;
    assert.deepEqual(filter, { kind: 'or', children: [
      { kind: 'and', children: [{ kind: 'condition', column: 'SampleRow', operator: 'greaterThanEqual', value: 1 },
        { kind: 'condition', column: 'SampleRow', operator: 'lessThanEqual', value: 10 }] },
      { kind: 'condition', column: 'SampleRow', operator: 'equal', value: 20 },
      { kind: 'condition', column: 'SampleRow', operator: 'greaterThanEqual', value: 30 },
    ] });
    const derived = columnsOf(commands);
    assert.deepEqual(rowSampleOf({ derived, filter }), { ...t, columns: ['SampleRow'] });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived, filter }), { columns: { SampleRow: { hidden: true } } })),
      { tool: 'sample', also: [], says: 'rows 1-10, 20, 30+ by notional' });
    // one row alone: an equal, still a Select Records (a remainder's 1 is one of every N's)
    const one = rowSampleCommands(typed, { ...t, n: 1, ranges: [{ from: 7, to: 7 }] });
    assert.equal(rowSampleOf({ derived: columnsOf(one), filter: (one.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition })?.kind, 'records');
    refused(() => rowSampleCommands(typed, { ...t, ranges: [] }), /name the rows it keeps/);
    refused(() => rowSampleCommands(typed, { ...t, ranges: [{ from: 5, to: 2 }] }), /5-2 is no range of rows/);
    assert.deepEqual(readRowSample({ kind: 'sample', mode: 'records', ranges: '2-3', by: 'notional' }),
      { kind: 'records', n: 1, by: 'notional', groups: [], ranges: [{ from: 2, to: 3 }] });
    refused(() => readRowSample({ kind: 'sample', mode: 'first', n: 2, by: 'notional', ranges: '1' }), /only a Select Records/);
  });

  it('keeps a percent at random, the same rows for a seed: its chance the hash of a row number in a total order', () => {
    const t = { kind: 'random' as const, n: 25, by: '', groups: [], seed: 7 };
    const commands = rowSampleCommands(typed, t);
    const [numbered, chance] = columnsOf(commands);
    assert.deepEqual(numbered!.window?.order, [{ column: 'region', direction: 'asc' }, { column: 'notional', direction: 'asc' }], 'every column that sorts');
    assert.equal(JSON.stringify(chance!.lambda?.body[0]), JSON.stringify(fn('mod', fn('abs', fn('hashCode',
      collection([fn('toOne', property(variable('x'), 'SampleRow')), lit.integer(7)]))), lit.integer(100))));
    const filter = (commands.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition;
    assert.deepEqual(filter, { kind: 'condition', column: 'SampleChance', operator: 'lessThan', value: 25 });
    assert.deepEqual(rowSampleOf({ derived: columnsOf(commands), filter }), { ...t, columns: ['SampleRow', 'SampleChance'] });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: columnsOf(commands), filter }))).says, '25% at random, seed 7');
    refused(() => rowSampleCommands(typed, { ...t, n: 100 }), /a whole percent of the rows, from 1 to 99/);
    refused(() => rowSampleCommands(typed, { ...t, groups: ['region'] }), /a random sample is of all the rows/);
    assert.deepEqual(readRowSample({ kind: 'sample', mode: 'random', n: 10, seed: 3 }), { kind: 'random', n: 10, by: '', groups: [], seed: 3 });
  });

  it("oversamples a value (Alteryx's Oversample Field): every row of it, the others numbered by their hash and kept to its share", () => {
    const t = { kind: 'oversample' as const, n: 40, by: 'region', groups: [], seed: 2, value: 'EMEA' };
    const commands = rowSampleCommands(typed, t);
    const made = columnsOf(commands);
    assert.deepEqual(made.map((d) => d.name), ['SampleRow', 'SampleChance', 'SampleInterest', 'SampleRank', 'SampleCount', 'SampleKeep']);
    const [, , interest, rank, count, keep] = made;
    assert.equal(JSON.stringify(interest!.lambda?.body[0]), JSON.stringify(fn('if', fn('and', fn('isNotEmpty', property(variable('x'), 'region')),
      fn('equal', property(variable('x'), 'region'), lit.string('EMEA'))), lambda([], lit.integer(1)), lambda([], lit.integer(0)))));
    assert.deepEqual(rank!.window, { fn: 'rowNumber', partition: ['SampleInterest'], order: [{ column: 'SampleChance', direction: 'asc' }, { column: 'SampleRow', direction: 'asc' }] });
    assert.deepEqual(count!.window, { fn: 'sum', column: 'SampleInterest', partition: [], order: [], frame: 'partition' });
    assert.match(JSON.stringify(keep!.lambda?.body[0]), /"function":"or".*"function":"lessThanEqual".*"function":"divide".*"function":"times"/);
    const filter = (commands.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition;
    assert.deepEqual(filter, { kind: 'condition', column: 'SampleKeep', operator: 'equal', value: true });
    assert.deepEqual(rowSampleOf({ derived: made, filter }), { ...t, columns: made.map((d) => d.name) });
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: made, filter }))).says, 'region EMEA made 40% of the rows, seed 2');
    refused(() => rowSampleCommands(typed, { ...t, n: 0 }), /a whole percent of the rows, from 1 to 99/);
    refused(() => rowSampleCommands(typed, { ...t, by: 'desk' }), /the grid has no column 'desk'/);
    assert.deepEqual(readRowSample({ kind: 'sample', mode: 'oversample', n: 40, by: 'region', value: 'EMEA', seed: 2 }), t);
    refused(() => readRowSample({ kind: 'sample', mode: 'oversample', n: 40, by: 'region', value: null }), /its value is the value it keeps every row of/);
  });

  it("hands on the rest by its False (Alteryx's Create Samples): its filter turned round, its hidden columns as they are", () => {
    const commands = rowSampleCommands(typed, { kind: 'random', n: 30, by: '', groups: [], seed: 5 });
    const derived = columnsOf(commands);
    const filter = (commands.find((c) => c.kind === 'addFilter') as { condition: FilterNode }).condition;
    const hidden = Object.fromEntries(derived.map((d) => [d.name, { hidden: true }]));
    const d = apply(FLOW(), addGrid(cube(rows('grid-2'), (q) => ({ ...q, derived, filter }), { columns: hidden }), 'sheet-2', 'grid-2'),
      addGrid(cube({ ...rows('grid-5'), input: { grid: 'grid-5', port: 'false' } }), 'sheet-2', 'grid-5'));
    const rest = resolvedSource(d.page, d.page.cubes.find((c) => c.id === 'grid-6')!.cube.source, 'grid-6');
    const input = rest._type === 'rows' && !isLive(rest.input) ? rest.input : undefined;
    assert.deepEqual(input?.query.filter, { kind: 'not', child: { kind: 'condition', column: 'SampleChance', operator: 'lessThan', value: 30 } });
    assert.deepEqual(input?.query.derived, derived, 'its row number and its chance, as they are: the same seed, the same rows');
    assert.deepEqual(input?.omit, ['SampleRow', 'SampleChance'], 'hidden, so handed on by neither of its outputs');
    // a Sample of the first rows by an order (a Top N) has no filter: no rest
    const top = apply(FLOW(), addGrid(cube(rows('grid-2'), (q) => ({ ...q, topN: { by: 'notional', n: 5 } })), 'sheet-2', 'grid-2'));
    refused(() => apply(top, { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-5', port: 'false' } }),
      /'grid-5' has no filter: its False port leaves no row out/);
  });

  it("goes from the sample it has to the one asked: a Top N for the first of all, a row sample's columns made again", () => {
    let draft: GridDraft = { snapshot: typed, config: DEFAULT_CONFIGURATION };
    const step = (t: Parameters<typeof sampleCommands>[1]): CubeSnapshot => {
      draft = applyGridCommands(draft, sampleCommands(draft.snapshot, t));
      return draft.snapshot;
    };
    assert.equal(rowSampleOf(step({ kind: 'first', n: 2, by: 'notional', groups: ['region'] }))?.kind, 'first');
    // to one of every 3: the old number off, the new one under the same name
    const every = step({ kind: 'every', n: 3, by: 'notional', groups: [] });
    assert.deepEqual(every.derived.map((d) => d.name), ['SampleRow', 'SampleEvery']);
    assert.equal(rowSampleOf(every)?.kind, 'every');
    // to the first 5 of all the rows: its Top N, the row sample gone
    const top = step({ kind: 'first', n: 5, by: 'notional', groups: [] });
    assert.deepEqual([top.derived.length, top.filter, top.topN], [0, undefined, { by: 'notional', n: 5, bottom: true }]);
    assert.deepEqual(sampleCommands(top, null), [{ kind: 'topN', topN: null }]);
  });

  it('refuses in words what it cannot sample', () => {
    assert.throws(() => rowSampleCommands(typed, { kind: 'every', n: 1, by: 'notional', groups: [] }), /one row of every 1 is every row/);
    assert.throws(() => rowSampleCommands(typed, { kind: 'skip', n: 0, by: 'notional', groups: [] }), /a whole number from 1/);
    assert.throws(() => rowSampleCommands(typed, { kind: 'first', n: 2, by: 'nope', groups: [] }), /no column 'nope'/);
    assert.throws(() => rowSampleCommands(typed, { kind: 'first', n: 2, by: 'notional', groups: [] }), /its Top N/);
    assert.deepEqual(readRowSample({ kind: 'sample', mode: 'skip', n: 3, by: 'notional' }), { kind: 'skip', n: 3, by: 'notional', groups: [] });
    assert.equal(readRowSample({ kind: 'sample', mode: null }), null);
    assert.throws(() => readRowSample({ kind: 'sample', mode: 'shuffle', n: 3, by: 'notional' }), /its mode is first, last, skip, every, records, random/);
  });
});

describe("Select (select-columns.ts): the columns a box keeps and the names it hands them on by", () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'desk', type: 'String' }]) };

  it('hides what it leaves out and names what it renames, every other setting cleared; the box says both', () => {
    const [c] = selectCommands(typed, { keep: ['region', 'notional'], rename: { region: 'area' } }) as unknown as [{ patch: { columns: object } }];
    assert.deepEqual(c.patch.columns, {
      region: { hidden: null, displayName: 'area' }, notional: { hidden: null, displayName: null }, desk: { hidden: true, displayName: null },
    });
    const box = cube(rows('grid-1'), (q) => ({ ...q, columns: typed.columns }), { columns: { region: { displayName: 'area' }, desk: { hidden: true } } });
    assert.deepEqual(boxSays(box), { tool: 'select', also: [], says: 'region → area; 1 of 3 columns left out' });
  });

  it("retypes a column: a conversion of it named for it, the column hidden and the conversion handed on by its name", () => {
    const coded = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'code', type: 'String' }]) };
    const commands = selectCommands(coded, { keep: ['region', 'code'], rename: {}, retype: { code: 'Integer' } });
    const [calc, conf] = commands as unknown as [{ kind: string; column: DerivedColumn }, { patch: { columns: Record<string, unknown> } }];
    assert.equal(calc.column.name, 'code__Integer');
    assert.equal(JSON.stringify(calc.column.lambda?.body[0]), JSON.stringify(fn('parseInteger', fn('toOne', property(variable('x'), 'code')))));
    assert.equal(calc.column.kind, 'measure');
    assert.deepEqual(conf.patch.columns['code'], { hidden: true, displayName: null });
    assert.deepEqual(conf.patch.columns['code__Integer'], { hidden: null, displayName: 'code' });
    // read back: its retype, its column kept under its own name, and the box says it
    const derived = [calc.column];
    const config = { columns: { code: { hidden: true }, code__Integer: { displayName: 'code' }, notional: { hidden: true } } };
    assert.deepEqual(retypesOf(derived), [{ name: 'code__Integer', column: 'code', to: 'Integer' }]);
    const row = selectionOf({ ...coded, derived }, config).find((c) => c.name === 'code');
    assert.deepEqual([row?.kept, row?.as, row?.retype], [true, 'code', 'Integer']);
    const box = cube(rows('grid-1'), (q) => ({ ...q, columns: coded.columns, derived }), config);
    assert.deepEqual(boxSays(box), { tool: 'select', also: [], says: 'code as Integer; 1 of 3 columns left out' });
    // set again without it: the conversion taken off, the column shown again
    const again = selectCommands({ ...coded, derived }, { keep: ['region', 'code'], rename: {} });
    assert.deepEqual(again[0], { kind: 'removeCalculated', name: 'code__Integer' });
    assert.throws(() => selectCommands(coded, { keep: ['notional'], rename: {}, retype: { notional: 'Boolean' } }),
      /'notional' \(Float\) cannot be made Boolean: it can be made String, Integer, Decimal/);
  });

  it("hands its columns on in its order -- its window's rows', a conversion in its column's place -- said on its box", () => {
    const coded = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'code', type: 'String' }]) };
    const applied = (t: Parameters<typeof selectCommands>[1]) => {
      const commands = selectCommands(coded, t);
      const draft = applyGridCommands({ snapshot: coded, config: DEFAULT_CONFIGURATION }, commands);
      const config = draft.config as unknown as Record<string, unknown>;
      const box = cube(rows('grid-1'), (q) => ({ ...q, columns: draft.snapshot.columns, derived: draft.snapshot.derived }), config);
      return { commands, s: draft.snapshot, config, box };
    };
    const moved = applied({ keep: ['region', 'notional', 'code'], rename: {}, retype: { code: 'Integer' }, order: ['code', 'region', 'notional'] });
    assert.deepEqual((moved.commands.at(-1) as unknown as { patch: { columnOrder: string[] } }).patch.columnOrder, ['code__Integer', 'region', 'notional']);
    assert.deepEqual(handedOn(rowColumns(moved.s).map((c) => c.name), moved.s, moved.config).map((c) => c.as), ['code', 'region', 'notional']);
    assert.deepEqual(selectionOf(moved.s, moved.config).map((c) => c.name), ['code', 'region', 'notional'], 'its window: its rows in its order');
    assert.equal(boxSays(moved.box).says, 'code as Integer; columns reordered');
    // a retype alone keeps its column's place -- here between the two -- and says no order of its own
    const kept = applied({ keep: ['region', 'notional', 'code'], rename: {}, retype: { code: 'Integer' }, order: ['region', 'code', 'notional'] });
    assert.deepEqual(handedOn(rowColumns(kept.s).map((c) => c.name), kept.s, kept.config).map((c) => c.as), ['region', 'code', 'notional']);
    assert.equal(boxSays(kept.box).says, 'code as Integer; columns reordered', 'moved between them: an order of its own');
    const inPlace = { ...rawRows(accessor('local::DB', 'trades'), [COLUMNS[0]!, { name: 'code', type: 'String' }, COLUMNS[1]!]) };
    const draft = applyGridCommands({ snapshot: inPlace, config: DEFAULT_CONFIGURATION },
      selectCommands(inPlace, { keep: ['region', 'code', 'notional'], rename: {}, retype: { code: 'Integer' }, order: ['region', 'code', 'notional'] }));
    const config = draft.config as unknown as Record<string, unknown>;
    assert.deepEqual(handedOn(rowColumns(draft.snapshot).map((c) => c.name), draft.snapshot, config).map((c) => c.as), ['region', 'code', 'notional'],
      'its conversion where its column was');
    assert.equal(boxSays(cube(rows('grid-1'), (q) => ({ ...q, columns: draft.snapshot.columns, derived: draft.snapshot.derived }), config)).says,
      'code as Integer', 'in its place: no order of its own said');
    // no order named (Python's grid.select): the grid's own, the same commands as its window's rows as they are -- its
    // conversion in its column's place either way (the review of 2026-10-26's B4)
    const unnamed = { keep: ['region', 'code', 'notional'], rename: {}, retype: { code: 'Integer' as const } };
    assert.deepEqual(selectCommands(inPlace, unnamed), selectCommands(inPlace, { ...unnamed, order: ['region', 'code', 'notional'] }));
    refused(() => selectCommands(coded, { keep: ['region'], rename: {}, order: ['region', 'region'] }), /names 'region' twice/);
    refused(() => selectCommands(coded, { keep: ['region'], rename: {}, order: ['nope'] }), /its order names no column 'nope'/);
    assert.deepEqual(readSelection({ kind: 'selectColumns', keep: ['a'], order: ['a'] }), { keep: ['a'], rename: {}, order: ['a'] });
  });

  it('refuses in words what it cannot hand on', () => {
    assert.throws(() => selectCommands(typed, { keep: [], rename: {} }), /keep a column/);
    assert.throws(() => selectCommands(typed, { keep: ['nope'], rename: {} }), /no column 'nope'/);
    assert.throws(() => selectCommands(typed, { keep: ['region', 'desk'], rename: { region: 'desk' } }), /'region' and 'desk' would both be handed on as 'desk'/);
    assert.throws(() => selectCommands(typed, { keep: ['region'], rename: { region: ' ' } }), /renamed to nothing/);
    assert.deepEqual(readSelection({ kind: 'selectColumns', keep: ['a'] }), { keep: ['a'], rename: {} });
    assert.throws(() => readSelection({ kind: 'selectColumns', keep: ['a'], rename: { a: 1 } }), /its renames are \{column: new name\}/);
    assert.deepEqual(readSelection({ kind: 'selectColumns', keep: ['a'], retype: { a: 'Float' } }), { keep: ['a'], rename: {}, retype: { a: 'Float' } });
    assert.throws(() => readSelection({ kind: 'selectColumns', keep: ['a'], retype: { a: 'Money' } }), /its retypes are/);
  });
});

describe('Summarize (summarize.ts): a row a group, each column grouped by, aggregated or left out -- one step', () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'desk', type: 'String' }]) };

  it('groups, aggregates and hides what it leaves out; read back as it was set', () => {
    const commands = summarizeCommands(typed, { columns: [{ name: 'region', action: 'group' }, { name: 'notional', action: 'average' }] });
    assert.deepEqual(commands, [
      { kind: 'group', rows: ['region'] },
      { kind: 'column', name: 'notional', change: { aggregate: 'average', weight: null } },
      { kind: 'configure', patch: { columns: { region: { hidden: null }, notional: { hidden: null }, desk: { hidden: true } } } },
    ]);
    const done = applyGridCommands({ snapshot: typed, config: DEFAULT_CONFIGURATION }, commands);
    const read = summaryOf(done.snapshot, done.config as unknown as Record<string, unknown>);
    assert.deepEqual(read.map((c) => [c.name, c.action]), [['region', 'group'], ['notional', 'average'], ['desk', undefined]]);
    // what it leaves out its own: said as a Summarize alone, no Select beside it
    const box = cube(rows('grid-1'), (q) => ({ ...q, columns: typed.columns, rows: ['region'] }), { columns: { desk: { hidden: true } } });
    assert.deepEqual(boxSays(box), { tool: 'summarize', also: [], says: 'by region' });
    // set again: its grouping taken off first, a measure grouped by made a dimension before it is
    const again = summarizeCommands(done.snapshot, { columns: [{ name: 'desk', action: 'group' }, { name: 'notional', action: 'group' }] });
    assert.deepEqual(again.slice(0, 3), [
      { kind: 'group', rows: [] }, { kind: 'column', name: 'notional', change: { kind: 'dimension' } }, { kind: 'group', rows: ['desk', 'notional'] },
    ]);
  });

  it("offers a column its aggregates, and reads a fresh grid's measures as aggregated as they say", () => {
    assert.equal(summaryActions('Float')[0], 'group');
    assert.ok(summaryActions('Float').includes('sum') && !summaryActions('String').includes('sum'));
    assert.ok(summaryActions('Float').includes('wavg') && summaryActions('Float').includes('distinctCount'), 'aggregates of two columns too');
    const fresh = summaryOf(typed, {});
    assert.deepEqual(fresh.map((c) => [c.name, c.action]), [['region', undefined], ['notional', 'sum'], ['desk', undefined]]);
    assert.deepEqual(summaryOf(typed, { columns: { notional: { hidden: true } } })[1]?.action, undefined);
  });

  it('refuses in words what it cannot summarise', () => {
    refused(() => summarizeCommands(typed, { columns: [{ name: 'notional', action: 'sum' }] }), /group by a column/);
    refused(() => summarizeCommands(typed, { columns: [{ name: 'nope', action: 'group' }] }), /no column 'nope'/);
    refused(() => summarizeCommands(typed, { columns: [{ name: 'region', action: 'group' }, { name: 'region', action: 'count' }] }), /one thing with each column/);
    // an aggregate of two columns: its second named, its weight set; none named, refused
    assert.deepEqual(summarizeCommands(typed, { columns: [{ name: 'region', action: 'group' }, { name: 'notional', action: 'wavg', second: 'notional' }] })
      .find((c) => c.kind === 'column' && c.name === 'notional'), { kind: 'column', name: 'notional', change: { aggregate: 'wavg', weight: 'notional' } });
    refused(() => summarizeCommands(typed, { columns: [{ name: 'region', action: 'group' }, { name: 'notional', action: 'wavg' }] }), /weighted average reads its weight column/);
    refused(() => summarizeCommands(typed, { columns: [{ name: 'region', action: 'group' }, { name: 'notional', action: 'sum', second: 'x' }] }), /reads no second column/);
    assert.deepEqual(readSummary({ kind: 'summarize', group: ['desk'], aggregate: { notional: { fn: 'wavg', by: 'qty' } } }).columns[1],
      { name: 'notional', action: 'wavg', second: 'qty' });
    // an aggregate its column is not offered: refused as it is applied, in Column Properties' words
    refused(() => applyGridCommands({ snapshot: typed, config: DEFAULT_CONFIGURATION },
      summarizeCommands(typed, { columns: [{ name: 'region', action: 'group' }, { name: 'desk', action: 'sum' }] })), /'sum' is not offered on desk, a String column/);
    assert.deepEqual(readSummary({ kind: 'summarize', group: ['desk'], aggregate: { notional: 'average' } }),
      { columns: [{ name: 'desk', action: 'group' }, { name: 'notional', action: 'average' }] });
    refused(() => readSummary({ kind: 'summarize', group: 'desk' }), /a list of column names/);
    refused(() => readSummary({ kind: 'summarize', group: [], by: [] }), /takes group and aggregate, not by/);
  });
});

describe('Find Replace (find-replace.ts): a column replaced by the column of theirs its Join found, in place', () => {
  const joined = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'their_region', type: 'String' }, { name: 'name', type: 'String' }]) };

  it('replaces where theirs has a value, hands it on by its own name, leaves theirs out; read back; set again', () => {
    const commands = findReplaceCommands(joined, { column: 'region', with: 'name' });
    const [calc, hide, place] = commands as unknown as [{ column: DerivedColumn }, { patch: { columns: object } }, { patch: { columns: object } }];
    assert.equal(calc.column.name, 'region_replaced');
    assert.equal(JSON.stringify(calc.column.lambda?.body[0]), JSON.stringify(fn('coalesce', property(variable('x'), 'name'), property(variable('x'), 'region'))));
    assert.deepEqual(hide.patch.columns, { name: { hidden: true } });
    assert.deepEqual(place.patch.columns, { region: { hidden: true }, region_replaced: { displayName: 'region' } });
    assert.deepEqual(findReplaceOf(calc.column), { column: 'region', with: 'name' });
    // a Join box replacing its column so: a Find Replace box, said so
    const join: CubeSource = {
      _type: 'join', name: 'trades ⋈ names', left: { grid: 'grid-2' }, right: { grid: 'grid-3' }, columns: joined.columns,
      how: { form: 'join', kind: 'left', on: { rows: [{ left: 'region', op: '==', right: 'region' }] }, prefix: 'their_', keep: null },
    };
    const box = cube(join, (q) => ({ ...q, columns: joined.columns, derived: [calc.column] }),
      { columns: { name: { hidden: true }, region: { hidden: true }, region_replaced: { displayName: 'region' } } });
    assert.deepEqual(boxSays(box), { tool: 'findReplace', also: [], says: 'region replaced by name' });
    assert.equal(boxSays(cube(join)).tool, 'join', 'a Join replacing nothing is a Join');
    // set again: the replacing before taken off, its columns shown again, then the new one
    const again = findReplaceCommands({ ...joined, derived: [calc.column] }, { column: 'region', with: 'their_region' });
    assert.deepEqual(again.slice(0, 2), [
      { kind: 'removeCalculated', name: 'region_replaced' }, { kind: 'configure', patch: { columns: { region: { hidden: null }, name: { hidden: null } } } },
    ]);
  });

  it('refuses in words what it cannot replace', () => {
    refused(() => findReplaceCommands(joined, { column: 'nope', with: 'name' }), /no column 'nope'/);
    refused(() => findReplaceCommands(joined, { column: 'region', with: 'region' }), /not by itself/);
    assert.deepEqual(readFindReplace({ kind: 'findReplace', column: 'region', with: 'name' }), { column: 'region', with: 'name' });
    refused(() => readFindReplace({ kind: 'findReplace', column: 'region' }), /names its column and the column it is replaced with/);
    refused(() => readFindReplace({ kind: 'findReplace', column: 'a', with: 'b', by: 1 }), /takes column and with, not by/);
  });
});

describe('Count Records (count-records.ts): one row, its count -- a column of one value grouped by, a 1 summed', () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), COLUMNS) };

  it('groups by its column of one value, sums its 1s, hides the rest; read back and said; refused in words', () => {
    const commands = countRecordsCommands({ ...typed, rows: ['region'] });
    assert.deepEqual(commands.map((c) => c.kind), ['group', 'calculate', 'calculate', 'group', 'column', 'configure']);
    const done = applyGridCommands({ snapshot: typed, config: DEFAULT_CONFIGURATION }, countRecordsCommands(typed));
    assert.deepEqual(countRecordsOf(done.snapshot), { name: 'Count', all: 'CountAll' });
    const config = done.config as unknown as Record<string, unknown>;
    assert.deepEqual(handedOn(rowColumns(done.snapshot).map((c) => c.name), done.snapshot, config).map((c) => c.as), ['Count'], 'its count alone');
    const box = cube(rows('grid-1'), (q) => ({ ...q, derived: done.snapshot.derived, rows: done.snapshot.rows }), config);
    assert.deepEqual(boxSays(box), { tool: 'countRecords', also: [], says: 'its rows counted: Count' });
    refused(() => countRecordsCommands(done.snapshot), /counts its rows already/);
    refused(() => countRecordsCommands(typed, 'region'), /'region' is already a column/);
    assert.deepEqual(readCountRecords({ kind: 'countRecords' }), { name: 'Count' });
    refused(() => readCountRecords({ kind: 'countRecords', by: 1 }), /takes name, not by/);
  });
});

describe('Transpose (transpose.ts): columns made rows, a Name and a Value each, the keys beside them', () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'pnl', type: 'Float' }, { name: 'desk', type: 'String' }]) };
  const made = (t: Partial<Parameters<typeof transposeCommands>[1]> = {}) =>
    transposeCommands(typed, { columns: ['notional', 'pnl'], keys: ['region'], name: 'Name', value: 'Value', ...t });

  it('explodes a list of the names, chooses each row\'s value by its name, and hides all but the keys', () => {
    const [name, value, hide] = made() as unknown as [{ column: DerivedColumn }, { column: DerivedColumn }, { patch: { columns: object } }];
    assert.equal(name.column.unnest, true);
    assert.deepEqual((name.column.lambda?.body[0] as unknown as { values: unknown[] }).values, [lit.string('notional'), lit.string('pnl')]);
    const x = variable('x');
    assert.equal(JSON.stringify(value.column.lambda?.body[0]), JSON.stringify(
      fn('if', fn('equal', property(x, 'Name'), lit.string('notional')), lambda([], property(x, 'notional')), lambda([], property(x, 'pnl')))));
    assert.equal(value.column.kind, 'measure', 'numbers summed when grouped');
    assert.deepEqual(Object.keys(hide.patch.columns), ['notional', 'pnl', 'desk']);
  });

  it('is read back off its columns, its hidden columns its own: a Transpose box', () => {
    const commands = made() as unknown as { column?: DerivedColumn; patch?: { columns: Record<string, unknown> } }[];
    const derived = commands.flatMap((c) => (c.column ? [c.column] : []));
    const box = cube(rows('grid-1'), (q) => ({ ...q, derived }), { columns: commands[2]!.patch!.columns });
    assert.deepEqual(boxSays(box), { tool: 'transpose', also: [], says: 'notional, pnl to rows' });
    assert.deepEqual(transposeOf(derived), { name: 'Name', value: 'Value', columns: ['notional', 'pnl'] });
    // one column: its value is that column
    const one = (transposeCommands(typed, { columns: ['pnl'], keys: [], name: 'Name', value: 'Value' }) as unknown as { column?: DerivedColumn }[])
      .flatMap((c) => (c.column ? [c.column] : []));
    assert.deepEqual(transposeOf(one)?.columns, ['pnl']);
  });

  it('refuses in words what it cannot transpose', () => {
    assert.throws(() => made({ columns: [] }), /pick a column to make rows of/);
    assert.throws(() => made({ columns: ['notional', 'desk'] }), /its columns hold numbers and text: one Value holds one kind/);
    assert.throws(() => made({ keys: ['notional'] }), /'notional' is a key/);
    assert.throws(() => made({ name: 'region' }), /'region' is already a column/);
    assert.throws(() => made({ value: 'Name' }), /two names/);
    assert.deepEqual(readTranspose({ kind: 'transpose', columns: ['a'] }), { columns: ['a'], keys: [], name: 'Name', value: 'Value' });
    assert.throws(() => readTranspose({ kind: 'transpose', columns: 'a' }), /its columns are a list of column names/);
  });
});

describe("Imputation (impute.ts): a number's empties filled, a value or the column's own across every row", () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'qty', type: 'Integer' }]) };

  it("fills with a value -- a whole number's with a whole number -- or over a window column of its average, hidden", () => {
    const [byValue] = imputeCommands(typed, { columns: ['qty'], with: { kind: 'value', value: 0 } }) as unknown as [{ column: DerivedColumn }];
    assert.equal(byValue.column.name, 'qty_filled');
    assert.equal(JSON.stringify(byValue.column.lambda?.body[0]), JSON.stringify(fn('coalesce', property(variable('x'), 'qty'), lit.integer(0))));
    const avg = imputeCommands(typed, { columns: ['notional'], with: { kind: 'average' } });
    assert.deepEqual(avg.map((c) => c.kind), ['calculate', 'calculate', 'configure']);
    const [helper, filled] = avg as unknown as [{ column: DerivedColumn }, { column: DerivedColumn }];
    assert.deepEqual(helper.column.window, { fn: 'average', column: 'notional', partition: [], order: [], frame: 'partition' });
    // a box made so says Imputation, its window column none of its words, none of a Select's
    const box = cube(rows('grid-1'), (q) => ({ ...q, derived: [helper.column, filled.column] }), { columns: { notional__average: { hidden: true } } });
    assert.deepEqual(boxSays(box), { tool: 'impute', also: [], says: "notional's empties → its average" });
    assert.deepEqual(imputedOf(filled.column, [helper.column, filled.column]), { column: 'notional', with: { kind: 'average' }, helper: 'notional__average' });
    // its median, the database's (a window's median over every row): read back so, said so
    const [middle, byMedian] = imputeCommands(typed, { columns: ['notional'], with: { kind: 'median' } }) as unknown as [{ column: DerivedColumn }, { column: DerivedColumn }];
    assert.deepEqual(middle.column.window, { fn: 'median', column: 'notional', partition: [], order: [], frame: 'partition' });
    assert.deepEqual(imputedOf(byMedian.column, [middle.column, byMedian.column]), { column: 'notional', with: { kind: 'median' }, helper: 'notional__median' });
    assert.deepEqual(readImputation({ kind: 'impute', columns: ['qty'], with: 'median' }), { columns: ['qty'], with: { kind: 'median' } });
  });

  it('in place: the filled column handed on by its column\'s name, the column hidden, and the box says so', () => {
    const commands = imputeCommands(typed, { columns: ['qty'], with: { kind: 'value', value: 0 }, inPlace: true });
    assert.deepEqual(commands.at(-1), { kind: 'configure', patch: { columns: { qty: { hidden: true }, qty_filled: { displayName: 'qty' } } } });
    const [filled] = commands as unknown as [{ column: DerivedColumn }];
    const box = cube(rows('grid-1'), (q) => ({ ...q, columns: typed.columns, derived: [filled.column] }),
      { columns: { qty: { hidden: true }, qty_filled: { displayName: 'qty' } } });
    assert.deepEqual(boxSays(box), { tool: 'impute', also: [], says: "qty's empties → 0 in place" });
    assert.deepEqual(readImputation({ kind: 'impute', columns: ['qty'], with: 0, inPlace: true }).inPlace, true);
    // handed on in its column's place, not after the others (the correctness audit's 15, 2026-10-26)
    const done = applyGridCommands({ snapshot: typed, config: DEFAULT_CONFIGURATION },
      imputeCommands(typed, { columns: ['notional'], with: { kind: 'value', value: -1 }, inPlace: true }));
    const config = done.config as unknown as Record<string, unknown>;
    assert.deepEqual(handedOn(rowColumns(done.snapshot).map((c) => c.name), done.snapshot, config).map((c) => [c.name, c.as]),
      [['region', 'region'], ['notional_filled', 'notional'], ['qty', 'qty']]);
    assert.deepEqual(boxOrder(done.snapshot, config), ['region', 'notional_filled', 'qty'], "the relation's select, for what reads it");
    // in its column's place is no Select's order: the box says Imputation alone
    assert.deepEqual(boxSays(cube(rows('grid-1'), (q) => ({ ...q, columns: done.snapshot.columns, derived: done.snapshot.derived }), config)).also, []);
    // a negative filler read back: the protocol writes -1 as minus(1)
    assert.deepEqual(imputedOf(done.snapshot.derived[0]!, done.snapshot.derived)?.with, { kind: 'value', value: -1 });
  });

  it('refuses in words what it cannot fill', () => {
    assert.throws(() => imputeCommands(typed, { columns: [], with: { kind: 'average' } }), /pick a column to fill/);
    assert.throws(() => imputeCommands(typed, { columns: ['region'], with: { kind: 'average' } }), /only a number's empties are filled/);
    assert.throws(() => imputeCommands(typed, { columns: ['qty'], with: { kind: 'value', value: 0.5 } }), /holds whole numbers/);
    assert.deepEqual(readImputation({ kind: 'impute', columns: ['qty'], with: 3 }), { columns: ['qty'], with: { kind: 'value', value: 3 } });
    assert.throws(() => readImputation({ kind: 'impute', columns: ['qty'], with: 'mode' }), /not "mode"/);
  });
});

describe("a page's macros, saved and taken off by its page commands (macros.ts, M8)", () => {
  it('saves one -- judged, replaced by its name -- writes it, reads it again, and takes it off', () => {
    const big = lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), lit.integer(90)))));
    let d = apply(FLOW(), { kind: 'setMacro', name: 'Big', pure: big });
    assert.deepEqual(d.page.macros?.map((m) => m.name), ['Big']);
    d = apply(d, { kind: 'setMacro', name: 'Big', pure: lambda(['input'], fn('limit', variable('input'), lit.integer(5))) });
    assert.equal(d.page.macros?.length, 1, 'its name again: replaced');
    assert.equal(toJson(readPage(JSON.parse(pageToJson(d.page))).macros![0]!.pure), toJson(d.page.macros![0]!.pure), 'written and read again, exact');
    refused(() => apply(d, { kind: 'setMacro', name: 'One', pure: lambda(['input'], lit.integer(1)) }), /One: its Pure never reads \$input/);
    refused(() => apply(d, { kind: 'setMacro', name: 'Nope', pure: null }), /'Nope' is not a macro of the page \(Big\)/);
    d = apply(d, { kind: 'setMacro', name: 'Big', pure: null });
    assert.equal(d.page.macros, undefined, 'none, none written');
  });
});

describe("a page's parameters, set and taken off by its page commands (page-parameters.ts, M8)", () => {
  it('sets one -- judged whole, replaced by its name -- writes it, reads it again, and takes it off', () => {
    const min = { name: 'min', type: 'Float' as const, value: 90 };
    let d = apply(FLOW(), { kind: 'setParameter', parameter: min }, { kind: 'setParameter', parameter: { name: 'region', type: 'String', value: 'EMEA' } });
    assert.deepEqual(d.page.parameters?.map((p) => p.name), ['min', 'region']);
    d = apply(d, { kind: 'setParameter', parameter: { ...min, value: 200 } });
    assert.deepEqual(d.page.parameters?.map((p) => p.value), [200, 'EMEA'], 'its value changed in its place');
    assert.deepEqual(readPage(JSON.parse(pageToJson(d.page))).parameters, d.page.parameters, 'written and read again');
    refused(() => apply(d, { kind: 'setParameter', parameter: { ...min, value: 'many' } }), /min's value is no Float/);
    refused(() => apply(d, { kind: 'removeParameter', name: 'nope' }), /'nope' is not a parameter of the page \(min, region\)/);
    d = apply(d, { kind: 'removeParameter', name: 'min' }, { kind: 'removeParameter', name: 'region' });
    assert.equal(d.page.parameters, undefined, 'none, none written');
  });
});

describe("Dynamic Replace (dynamic-replace.ts): values replaced by rules read from another box's rows, pinned", () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'qty', type: 'Integer' }]) };
  /** A rule's text parsed as the compiler parses `x|<text>`: here, `$x.<column> == <value>` alone. */
  const cond = (column: string, value: string): Lambda => lambda(['x'], fn('equal', property(variable('x'), column), lit.string(value)));
  const table = (rows: (string | number | null)[][]) => ({
    rowCount: rows.length,
    columns: ['Column', 'Condition', 'Value'].map((name, i) => ({ name, type: 'String', values: rows.map((r) => r[i] ?? null) })),
  });
  const by = { column: 'Column', condition: 'Condition', value: 'Value' };

  it("reads a rule from each row of a box's rows, in order -- and refuses in words what is no rule", () => {
    const rules = rulesOf(table([['region', "$x.region == 'EMEA'", 'Europe'], ['region', "$x.region == 'APAC'", 'Asia'], ['qty', '$x.qty < 0', 0]]), by);
    assert.deepEqual(rules.map((r) => r.column), ['region', 'region', 'qty']);
    assert.equal(rules[2]!.value, 0);
    refused(() => rulesOf(table([]), by), /has no rows/);
    refused(() => rulesOf(table([['', 'true', 'a']]), by), /rule 1 names no column/);
    refused(() => rulesOf(table([['region', '  ', 'a']]), by), /rule 1 has no condition/);
    refused(() => rulesOf(table([['region', 'true', null]]), by), /rule 1 has no value/);
    refused(() => rulesOf(table([['region', 'true', 'a']]), { ...by, value: 'Nope' }), /its rules have no column 'Nope' for its value/);
    refused(() => rulesOf(table(Array.from({ length: MAX_RULES + 1 }, () => ['region', 'true', 'a'])), by), /more than 200 rows/);
    assert.equal(ruleText(" $x.qty > 1 "), 'x|$x.qty > 1');
  });

  it('pins its rules as one chain of ifs per column, handed on in its place, and reads it back', () => {
    const replace = { rules: [
      { column: 'region', condition: "$x.region == 'EMEA'", value: 'Europe' },
      { column: 'qty', condition: '$x.qty < 0', value: '0' },
      { column: 'region', condition: "$x.region == 'APAC'", value: 'Asia' },
    ], expressions: false };
    const made = replacedColumnsOf(typed, replace, { conditions: [cond('region', 'EMEA'), cond('qty', 'x'), cond('region', 'APAC')] });
    assert.deepEqual(made.map((m) => m.name), ['region_replaced', 'qty_replaced']);
    // the first rule outermost, the column itself last: of a column's rules the first that holds
    const x = variable('x');
    assert.equal(JSON.stringify(made[0]!.lambda), JSON.stringify(lambda(['x'], fn('if', cond('region', 'EMEA').body[0]!, lambda([], lit.string('Europe')),
      lambda([], fn('if', cond('region', 'APAC').body[0]!, lambda([], lit.string('Asia')), lambda([], property(x, 'region'))))))));
    // a number column's value its digits, written as its literal: never a text
    assert.equal(JSON.stringify((made[1]!.lambda!.body[0] as unknown as { parameters: { body: unknown[] }[] }).parameters[1]!.body[0]), JSON.stringify(lit.integer(0)));
    const types = new Map([['region_replaced', 'String'], ['qty_replaced', 'Integer']]);
    const commands = dynamicReplaceCommands(typed, made, types);
    assert.deepEqual(commands.map((c) => c.kind), ['calculate', 'calculate', 'configure']);
    assert.deepEqual(commands.at(-1), { kind: 'configure', patch: { columns: {
      region: { hidden: true }, region_replaced: { displayName: 'region' }, qty: { hidden: true }, qty_replaced: { displayName: 'qty' },
    } } });
    assert.deepEqual(replacedOf(made[0]!), { column: 'region', rules: 2 });
    assert.equal(replacedOf({ ...made[0]!, name: 'other' }), undefined, 'its name says it, as an Imputation\'s does');
    const done = applyGridCommands({ snapshot: typed, config: DEFAULT_CONFIGURATION }, commands);
    const box = cube(rows('grid-1'), (q) => ({ ...q, columns: done.snapshot.columns, derived: done.snapshot.derived }), done.config as unknown as Record<string, unknown>);
    assert.deepEqual(boxSays(box), { tool: 'dynamicReplace', also: [], says: 'region by 2 rules in place, qty by 1 rule in place' });
    // loaded again with rules of region alone: its column set again, qty's taken off and qty shown again
    const again = replacedColumnsOf(done.snapshot, { rules: [replace.rules[0]!], expressions: false }, { conditions: [cond('region', 'EMEA')] });
    const reloaded = dynamicReplaceCommands(done.snapshot, again, new Map([['region_replaced', 'String']]));
    assert.deepEqual(reloaded.map((c) => c.kind), ['editCalculated', 'configure', 'removeCalculated', 'configure']);
    const after = applyGridCommands(done, reloaded);
    assert.deepEqual(after.snapshot.derived.map((d) => d.name), ['region_replaced']);
    assert.equal(dynamicReplaceProbe(done.snapshot, again).derived.filter((d) => d.name === 'region_replaced').length, 1, 'probed in place of the one it has');
  });

  it('values as expressions: each parsed, its type the compiler\'s -- one of another type than its column\'s refused', () => {
    const replace = { rules: [{ column: 'notional', condition: '$x.notional < 0', value: '$x.notional * -1' }], expressions: true };
    const value = lambda(['x'], fn('times', collection([property(variable('x'), 'notional'), lit.integer(-1)])));
    const made = replacedColumnsOf(typed, replace, { conditions: [cond('notional', 'a')], values: [value] });
    assert.equal(JSON.stringify((made[0]!.lambda!.body[0] as unknown as { parameters: { body: unknown[] }[] }).parameters[1]!.body[0]), JSON.stringify(value.body[0]));
    refused(() => dynamicReplaceCommands(typed, made, new Map([['notional_replaced', 'String']])), /'notional' replaced is String: its values are Float/);
  });

  it('refuses in words a rule of a column the grid has not, a value no literal of its column\'s type', () => {
    const one = (column: string, value: string | number | boolean, condition = 'true') => ({ rules: [{ column, condition, value }], expressions: false });
    refused(() => replacedColumnsOf(typed, one('nope', 'a'), { conditions: [cond('region', 'a')] }), /rule 1 replaces 'nope', which the grid has not/);
    refused(() => replacedColumnsOf(typed, one('qty', 'many'), { conditions: [cond('qty', 'a')] }), /rule 1's value "many" is no Integer, as 'qty' is/);
    refused(() => replacedColumnsOf(typed, one('qty', 1.5), { conditions: [cond('qty', 'a')] }), /is no Integer/);
    refused(() => replacedColumnsOf(typed, one('region', 3), { conditions: [cond('region', 'a')] }), /rule 1's value 3 is no String/);
    refused(() => replacedColumnsOf(typed, one('region', 'a'), { conditions: [lambda(['y'], lit.boolean(true))] }), /rule 1's condition is one expression over \$x/);
    refused(() => replacedColumnsOf(typed, { rules: [], expressions: false }, { conditions: [] }), /it has no rules/);
    assert.deepEqual(readDynamicReplace({ kind: 'dynamicReplace', rules: [{ column: 'qty', condition: 'true', value: 1 }] }),
      { rules: [{ column: 'qty', condition: 'true', value: 1 }], expressions: false });
    refused(() => readDynamicReplace({ kind: 'dynamicReplace', rules: [{ column: 'qty', condition: 'true' }] }), /rule 1's value is a text/);
    refused(() => readDynamicReplace({ kind: 'dynamicReplace', rules: [], extra: 1 }), /takes no extra/);
  });
});

describe('Multi-Field Formula (multi-field.ts): one expression over several columns, a column each', () => {
  const typed = { ...rawRows(accessor('local::DB', 'trades'), [...COLUMNS, { name: 'desk', type: 'String' }, { name: 'qty', type: 'Integer', kind: 'measure' as const },
    { name: 'price', type: 'Float' }]) };
  const v = variable('v');
  // $v->trim(), and $v * 2 + $x.qty: the value, and the row's other column
  const trimmed = lambda(['v'], fn('trim', fn('toOne', v)));
  const doubled = lambda(['v'], fn('plus', fn('times', fn('toOne', v), lit.integer(2)), property(variable('x'), 'qty')));
  const made = (columns: string[], l = trimmed, suffix = '_new') =>
    (multiFieldCommands(typed, { columns, lambda: l, suffix }) as unknown as { column: DerivedColumn }[]).map((c) => c.column);

  it("makes a column of each, $v its column and $x the row; its kind its column's", () => {
    const [a, b] = made(['region', 'desk']);
    assert.deepEqual([a!.name, b!.name], ['region_new', 'desk_new']);
    assert.equal(JSON.stringify(b!.lambda), JSON.stringify(lambda(['x'], fn('trim', fn('toOne', property(variable('x'), 'desk'))))));
    assert.equal(a!.kind, 'dimension');
    const [q] = made(['qty'], doubled, '_x2');
    assert.equal(q!.kind, 'measure');
    assert.equal(JSON.stringify(q!.lambda?.body[0]), JSON.stringify(
      fn('plus', fn('times', fn('toOne', property(variable('x'), 'qty')), lit.integer(2)), property(variable('x'), 'qty'))));
    assert.equal(multiFieldText('$v->trim()'), 'v|$v->trim()');
    assert.equal(multiFieldText(' w | $w '), 'w | $w');
  });

  it('is read back off its columns: two or more of one shape, each over its own; a lone one a Formula', () => {
    const columns = made(['region', 'desk'], lambda(['v'], fn('length', fn('toOne', v))));
    const box = cube(rows('grid-1'), (q) => ({ ...q, derived: columns }));
    assert.deepEqual(boxSays(box), { tool: 'multiField', also: [], says: 'over region, desk' });
    // a shape a tool of its own makes is that tool's: a trim, Data Cleansing's
    assert.equal(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: made(['region', 'desk']) }))).tool, 'cleanse');
    assert.deepEqual(multiFieldOf(columns).get('desk_new'), { over: 'desk', all: ['region', 'desk'] });
    // one over $x.qty besides its own value: the column it is over is the one its name says
    const twice = made(['notional', 'price'], doubled);
    assert.deepEqual([...multiFieldOf(twice).values()].map((m) => m.over), ['notional', 'price']);
    assert.equal(boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived: made(['desk'], doubled) }))).tool, 'formula');
    // two formulas of two shapes, side by side: Formulas
    assert.equal(multiFieldOf([...made(['desk']), ...made(['qty'], doubled)]).size, 0);
  });

  it('refuses in words what it cannot make', () => {
    assert.throws(() => made([]), /pick a column/);
    assert.throws(() => made(['nope']), /the grid has no column 'nope'/);
    assert.throws(() => made(['region'], lambda(['x'], variable('x'))), /its value is \$v/);
    assert.throws(() => made(['region'], lambda(['v'], fn('map', v, lambda(['x'], variable('x'))))), /names its parameter x/);
    assert.throws(() => made(['region'], trimmed, ' '), /suffix/);
    const taken = { ...typed, derived: made(['region']) };
    assert.throws(() => multiFieldCommands(taken, { columns: ['region'], lambda: trimmed, suffix: '_new' }), /'region_new' is already a column/);
    assert.deepEqual(readMultiField({ kind: 'multiField', columns: ['a'], expression: '$v' }), { columns: ['a'], expression: '$v', suffix: '_new' });
    assert.throws(() => readMultiField({ kind: 'multiField', columns: ['a'], expression: '' }), /its expression is over each column's value/);
  });
});

describe("the DateTime's part and text read (date-tools.ts): a column each, read back off its expression", () => {
  const said = (derived: DerivedColumn[]) => boxSays(cube(rows('grid-1'), (q) => ({ ...q, derived })));
  const col = (t: Parameters<typeof dateToolCommands>[0]) => (dateToolCommands(t)[0] as unknown as { column: DerivedColumn }).column;

  it("takes a date's part, by the compiler's own function; reads text by a format, a day's or a date-time's", () => {
    const year = col({ kind: 'datePart', column: 'trade_date', part: 'year', name: 'y' });
    assert.equal(JSON.stringify(year.lambda?.body[0]), JSON.stringify(fn('year', property(variable('x'), 'trade_date'))));
    const day = col({ kind: 'datePart', column: 'trade_date', part: 'day', name: 'd' });
    assert.match(JSON.stringify(day.lambda?.body[0]), /"function":"dayOfMonth".*"function":"toOne"/);
    const read = col({ kind: 'dateParse', column: 'booked', format: 'dd/MM/yyyy', time: false, name: 'b' });
    assert.match(JSON.stringify(read.lambda?.body[0]), /meta::legend::lite::convertDateFormat/);
    assert.deepEqual(said([year, read]), { tool: 'datetime', also: [], says: 'year of trade_date, booked read as dd/MM/yyyy' });
    assert.equal(dateToolOf(col({ kind: 'dateParse', column: 'at', format: 'yyyy-MM-dd HH:mm:ss', time: true, name: 'a' }))?.says,
      'at read as yyyy-MM-dd HH:mm:ss');
    // a date written as text: format's %t{...}, the compiler's to translate
    const written = col({ kind: 'dateFormat', column: 'trade_date', format: 'dd.MM.yyyy', name: 't' });
    assert.equal(JSON.stringify(written.lambda?.body[0]),
      JSON.stringify(fn('format', lit.string('%t{dd.MM.yyyy}'), collection([property(variable('x'), 'trade_date')]))));
    assert.equal(dateToolOf(written)?.says, 'trade_date written as dd.MM.yyyy');
  });

  it('refuses in words what it cannot make', () => {
    assert.throws(() => dateToolCommands({ kind: 'datePart', column: 'd', part: 'year', name: ' ' }), /its column needs a name/);
    assert.throws(() => readDateTool({ kind: 'datePart', column: 'd', part: 'century', name: 'c' }), /a part is year, month, day, week, hour/);
    assert.throws(() => readDateTool({ kind: 'dateParse', column: 'd', format: '', time: false, name: 'c' }), /it reads by a format/);
    assert.throws(() => readDateTool({ kind: 'dateFormat', column: 'd', format: ' ' }), /it writes by a format/);
    assert.throws(() => readDateTool({ kind: 'dateFormat', column: 'd', format: 'yyyy}' }), /a format has no \{ or \}/);
    assert.deepEqual(readDateTool({ kind: 'dateFormat', column: 'd', format: 'dd/MM/yyyy' }), { kind: 'dateFormat', column: 'd', format: 'dd/MM/yyyy', name: 'd_text' });
    // unnamed (Python's): its window's name; a parse's time its format's -- one rule (the review of 2026-10-26)
    assert.deepEqual(readDateTool({ kind: 'datePart', column: 'd', part: 'year' }), { kind: 'datePart', column: 'd', part: 'year', name: 'd_year' });
    assert.deepEqual(readDateTool({ kind: 'dateParse', column: 'd', format: 'yyyy-MM-dd HH:mm' }),
      { kind: 'dateParse', column: 'd', format: 'yyyy-MM-dd HH:mm', time: true, name: 'd_date' });
    assert.deepEqual(readTextTool({ kind: 'regex', column: 'desk', pattern: 'x', mode: 'extract' }), { kind: 'regex', column: 'desk', pattern: 'x', mode: 'extract', name: 'desk_extracted' });
  });
});

describe("a box's Profile (profile.ts): one row of aggregates asked of the database, read back a column each", () => {
  const columns = [{ name: 'region', type: 'String' }, { name: 'settled', type: 'Boolean' }];

  it("asks each column's values and different values counted, an ordered column's smallest and largest, its empties and share", () => {
    const text = toJson(profileLambda(property(variable('r'), 'rows'), columns));
    // a keyless aggregate: its one row even of no rows (a grouping gives no group of none)
    assert.match(text, /"function":"aggregate"/);
    assert.doesNotMatch(text, /"function":"groupBy"/);
    for (const name of ['c0_filled', 'c0_distinct', 'c0_smallest', 'c0_largest', 'c1_filled', 'c1_distinct', 'c0_empty', 'c0_share', 'c1_empty', 'c1_share']) {
      assert.ok(text.includes(`"${name}"`), name);
    }
    assert.ok(!text.includes('"c1_smallest"'), 'a Boolean has no smallest');
    assert.match(text, /"function":"distinct"/);
    // its empties and share filled the database's: its rows less its values, its values over its rows (0 of none)
    assert.match(text, /"function":"minus"/);
    assert.match(text, /"function":"divide"/);
  });

  it('reads its one row back -- of no rows too, the database\'s counts of none; anything else thrown', () => {
    const col = (name: string, v: unknown) => ({ name, type: 'Integer', values: [v] });
    const table = { rowCount: 1, epoch: 0, elapsedMs: 0, columns: [
      col('__profile_rows__', 5), col('c0_filled', 4), col('c0_empty', 1), col('c0_share', 0.8), col('c0_distinct', 3), col('c0_smallest', 'AMER'),
      col('c0_largest', 'EMEA'), col('c1_filled', 5), col('c1_empty', 0), col('c1_share', 1), col('c1_distinct', 2),
    ] } as never;
    assert.deepEqual(profileOf(table, columns), { rows: 5, columns: [
      { name: 'region', type: 'String', filled: 4, empty: 1, share: 0.8, distinct: 3, smallest: 'AMER', largest: 'EMEA' },
      { name: 'settled', type: 'Boolean', filled: 5, empty: 0, share: 1, distinct: 2 },
    ] });
    const none = { rowCount: 1, epoch: 0, elapsedMs: 0, columns: [
      col('__profile_rows__', 0), col('c0_filled', 0), col('c0_empty', 0), col('c0_share', 0), col('c0_distinct', 0), col('c0_smallest', null),
      col('c0_largest', null), col('c1_filled', 0), col('c1_empty', 0), col('c1_share', 0), col('c1_distinct', 0),
    ] } as never;
    assert.deepEqual(profileOf(none, columns).columns[0], { name: 'region', type: 'String', filled: 0, empty: 0, share: 0, distinct: 0, smallest: null, largest: null });
    // no row, or a count of none: no answer of its question, never read as 0 (AGENTS.md invariant 4)
    assert.throws(() => profileOf({ rowCount: 0, epoch: 0, elapsedMs: 0, columns: [] } as never, columns), /the profile's answer is one row, not 0/);
    assert.throws(() => profileOf({ ...(table as object), columns: [col('__profile_rows__', null)] } as never, columns), /has no count __profile_rows__/);
  });

  it("asks a column's most frequent values -- grouped by it, counted, the most first, of as many the smallest -- and reads them back", () => {
    const text = toJson(topValuesLambda(property(variable('r'), 'rows'), 'region'));
    assert.match(text, /"function":"groupBy"/);
    assert.match(text, /"function":"count"/);
    assert.match(text, /"function":"descending".*"__top_count__".*"function":"ascending".*"region"/s);
    assert.match(text, new RegExp(`"function":"limit".*"value":${TOP_VALUES}\\b`, 's'));
    assert.match(toJson(topValuesLambda(property(variable('r'), 'rows'), 'region', 3)), /"function":"limit".*"value":3\b/s);
    const table = { rowCount: 2, epoch: 0, elapsedMs: 0, columns: [
      { name: 'region', type: 'String', values: ['EMEA', null] }, { name: '__top_count__', type: 'Integer', values: [2, 1] },
    ] } as never;
    assert.deepEqual(topValuesOf(table, 'region'), [{ value: 'EMEA', count: 2 }, { value: null, count: 1 }]);
    // a count of none, or a column missing: no answer of its question (AGENTS.md invariant 4)
    assert.throws(() => topValuesOf({ ...(table as object), columns: [{ name: 'region', values: ['x'] }, { name: '__top_count__', values: [null] }] } as never, 'region'), /no count for its row 1/);
    assert.throws(() => topValuesOf(table, 'desk'), /has no column desk/);
  });

  it("asks each pair of number columns' correlation, each with itself, and reads them back a matrix: empty where none", () => {
    const asked = JSON.stringify(correlationLambda(accessor('local::DB', 'trades'), ['a', 'b', 'c']));
    for (const p of ['r0_0', 'r0_1', 'r0_2', 'r1_1', 'r1_2', 'r2_2']) assert.ok(asked.includes(`"${p}"`), `asks ${p}`);
    assert.ok(asked.includes('"corr"') && asked.includes('"rowMapper"'), 'by corr over rows mapped a pair each');
    assert.match(asked, /"function":"aggregate"/);
    const col = (name: string, v: unknown) => ({ name, type: 'Float', values: [v] });
    // b does not vary (its own correlation none): it with itself none, as with the others; a with itself 1 to a double's
    // last place, said 1
    const table = { rowCount: 1, epoch: 0, elapsedMs: 0, columns: [col('r0_0', 0.9999999999999998), col('r0_1', null), col('r0_2', -1),
      col('r1_1', null), col('r1_2', null), col('r2_2', 1)] } as never;
    assert.deepEqual(correlationsOf(table, ['a', 'b', 'c']), { columns: ['a', 'b', 'c'], r: [[1, null, -1], [null, null, null], [-1, null, 1]] });
  });
});

describe("a Text Input's table (ui/text-input-dialog.ts): a CSV file's text, as a file dropped on the canvas is", () => {
  it('writes its head row and a line a row, quoting a value only where it must', () => {
    assert.equal(tableCsv({ name: 't', columns: ['region', 'target'], rows: [['EMEA', '10'], ['AMER', '20']] }), 'region,target\nEMEA,10\nAMER,20\n');
    assert.equal(tableCsv({ name: 't', columns: ['a', 'b'], rows: [['x, y', 'say "hi"'], [' pad', 'two\nlines']] }),
      'a,b\n"x, y","say ""hi"""\n" pad","two\nlines"\n');
  });

  it('says what keeps it from being a table: a name, a column named once each, a row with a value', () => {
    const ok = { name: 'targets', columns: ['region', 'target'], rows: [['EMEA', '10']] };
    assert.equal(tableProblem(ok), undefined);
    assert.equal(tableProblem({ ...ok, name: ' ' }), 'it needs a name');
    assert.equal(tableProblem({ ...ok, columns: ['region', ''] }), 'column 2 needs a name');
    assert.equal(tableProblem({ ...ok, columns: ['region', 'region'] }), "'region' names two columns");
    assert.equal(tableProblem({ ...ok, rows: [['', '']] }), 'it needs a row with a value');
  });

  it("reads a block of cells as a sheet copies it: a line a row, tabs between values, the last line's break none", () => {
    assert.deepEqual(pastedCells('a\tb\r\n1\t2\r\n'), [['a', 'b'], ['1', '2']]);
    assert.deepEqual(pastedCells('one'), [['one']]);
    assert.deepEqual(pastedCells('x\t\ty\n'), [['x', '', 'y']]);
  });
});

describe("a flow sheet's page commands", () => {
  it('adds a flow, its boxes each after the box it reads, as the palette adds them', () => {
    const { page } = FLOW();
    assert.deepEqual(page.sheets.map((s) => [s.id, s.layout.kind]), [['sheet-1', 'bands'], ['sheet-2', 'flow']]);
    assert.deepEqual(at(page, 'grid-2'), { tile: 'grid-2', x: MARGIN, y: MARGIN });
    assert.deepEqual(at(page, 'grid-3'), { tile: 'grid-3', x: MARGIN + BOX_WIDTH + GAP_X, y: MARGIN });
    assert.deepEqual(at(page, 'grid-4'), { tile: 'grid-4', x: MARGIN + BOX_WIDTH + GAP_X, y: MARGIN + BOX_HEIGHT + GAP_Y });
    assert.equal(unnamedLabel(page, page.sheets[1]!), 'Flow 2', 'a flow is called a flow until someone names it');
    // read back whole: the page's own rules hold it
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(page)))), pageToJson(page));
  });

  it('places a box by hand: a flow\'s alone', () => {
    const d = FLOW();
    const moved = apply(d, { kind: 'placeBox', tile: 'grid-3', x: 500.4, y: 12 }).page;
    assert.deepEqual(at(moved, 'grid-3'), { tile: 'grid-3', x: 500, y: 12 });
    refused(() => apply(d, { kind: 'placeBox', tile: 'grid-1', x: 0, y: 0 }), /a tile of a board, placed by its layout: a box is a flow sheet's/);
    refused(() => apply(d, { kind: 'placeBox', tile: 'grid-3', x: -4, y: 0 }), /neither below zero/);
    refused(() => apply(d, { kind: 'placeBox', tile: 'grid-9', x: 0, y: 0 }), /grid-9/);
  });

  it('takes a text tile as a comment, placed as a box is, and left where it is by Tidy Up (M7)', () => {
    const d = apply(FLOW(), { kind: 'addText', id: 'text-1', sheet: 'sheet-2', text: 'Trades over 100 only', title: null, columns: null },
      { kind: 'placeBox', tile: 'text-1', x: 600, y: 300 });
    assert.deepEqual(at(d.page, 'text-1'), { tile: 'text-1', x: 600, y: 300 });
    readPage(JSON.parse(pageToJson(d.page)));
    const tidy = tidyCommands(d.page, 'sheet-2');
    const after = tidy.length === 0 ? d.page : apply(d, ...tidy).page;
    assert.deepEqual(at(after, 'text-1'), { tile: 'text-1', x: 600, y: 300 }, 'a comment stays where it was put');
  });

  it('takes a framed comment as a container: its frame kept by a move, folded, taken off; a grid never framed (M7)', () => {
    const frame = { w: 600, h: 200 };
    const d = apply(FLOW(), { kind: 'addText', id: 'text-1', sheet: 'sheet-2', text: 'Prep', title: null, columns: null },
      { kind: 'placeBox', tile: 'text-1', x: 20, y: 10, frame });
    assert.deepEqual(at(d.page, 'text-1'), { tile: 'text-1', x: 20, y: 10, frame });
    // moved, its frame kept; folded; a comment again
    assert.deepEqual(at(apply(d, { kind: 'placeBox', tile: 'text-1', x: 30, y: 10 }).page, 'text-1'), { tile: 'text-1', x: 30, y: 10, frame });
    assert.deepEqual(at(apply(d, { kind: 'placeBox', tile: 'text-1', x: 20, y: 10, frame: { ...frame, collapsed: true } }).page, 'text-1')?.frame,
      { ...frame, collapsed: true });
    assert.deepEqual(at(apply(d, { kind: 'placeBox', tile: 'text-1', x: 20, y: 10, frame: null }).page, 'text-1'), { tile: 'text-1', x: 20, y: 10 });
    // saved and read again with its frame; a frame too small, or on a grid, refused
    assert.deepEqual(at(readPage(JSON.parse(pageToJson(d.page))), 'text-1')?.frame, frame);
    refused(() => apply(d, { kind: 'placeBox', tile: 'text-1', x: 0, y: 0, frame: { w: 50, h: 50 } }), /a frame is at least 200 by 100/);
    refused(() => apply(d, { kind: 'placeBox', tile: 'grid-2', x: 0, y: 0, frame }), /a container is a comment/);
    assert.throws(() => readPage(JSON.parse(pageToJson(d.page).replace('"tile":"grid-2","x":40,"y":40', '"tile":"grid-2","x":40,"y":40,"frame":{"w":300,"h":200}'))),
      (e: unknown) => e instanceof PageDocumentError && /a container is a comment, not a grid/.test(e.message));
    // the boxes in it: those whose whole place is inside it, below its head
    const flow = flowOf(d.page);
    assert.deepEqual(framedBy(flow, 'text-1').sort(), flow.boxes.filter((b) => b.tile !== 'text-1' && b.x >= 20 && b.y >= 40 && b.x + BOX_WIDTH <= 620 && b.y + BOX_HEIGHT <= 210)
      .map((b) => b.tile).sort());
    assert.deepEqual(readPageCommand({ kind: 'placeBox', tile: 'text-1', x: 1, y: 2, frame: { w: 300, h: 200, collapsed: true } }),
      { kind: 'placeBox', tile: 'text-1', x: 1, y: 2, frame: { w: 300, h: 200, collapsed: true } });
    // moved, its boxes by as much: the canvas's drag and Python's text.place (containerMoves; the review of 2026-10-26's B6)
    const inside = { boxes: [{ tile: 'c', x: 20, y: 10, frame: { w: 600, h: 200 } }, { tile: 'a', x: 40, y: 60 }, { tile: 'b', x: 700, y: 60 }] };
    assert.deepEqual(containerMoves(inside, 'c', 120, 30), [{ tile: 'c', x: 120, y: 30 }, { tile: 'a', x: 140, y: 80 }]);
    assert.deepEqual(containerMoves(inside, 'b', 5, 5), [{ tile: 'b', x: 5, y: 5 }], 'a box is itself alone');
    assert.deepEqual(PAGE_GESTURES['flow.place']({ page: JSON.parse(pageToJson(d.page)), tile: 'text-1', x: 30, y: 20 })
      .map((c) => (c as { tile: string }).tile)[0], 'text-1', 'one step: the container first, then its boxes');
    assert.throws(() => containerMoves(inside, 'nope', 0, 0), (e: unknown) => e instanceof FlowRefusal && /'nope' is no box of this flow/.test(e.message));
  });

  it('refuses in words what only a board takes: a chart, a stack, an arrangement, a tile from a board', () => {
    const d = FLOW();
    refused(() => apply(d, { kind: 'stack', tiles: ['grid-2', 'grid-3'] }), /a flow sheet's boxes are not stacked/);
    refused(() => apply(d, { kind: 'arrange', sheet: 'sheet-2', preset: 'grid', order: null }), /a flow sheet's boxes are placed on its canvas/);
    refused(() => apply(d, { kind: 'moveTile', tile: 'grid-1', sheet: 'sheet-2', columns: null }), /a flow sheet holds boxes, each a grid that does one step: a grid stays on a board/);
    refused(() => apply(d, { kind: 'moveTile', tile: 'grid-3', sheet: 'sheet-1', columns: null }), /'grid-3' is a box of a flow: it moves to another flow sheet/);
    // a box between flows: placed on the other's canvas
    const two = apply(d, { kind: 'addFlow', id: 'sheet-5', name: 'Second' }, { kind: 'moveTile', tile: 'grid-4', sheet: 'sheet-5', columns: null }).page;
    assert.deepEqual(layoutTiles(two.sheets.find((s) => s.id === 'sheet-5')!.layout), ['grid-4']);
    assert.deepEqual(layoutTiles(flowOf(two)), ['grid-2', 'grid-3']);
  });

  it('a layout told whole is its own kind: a flow\'s boxes for a flow, a board\'s bands for a board', () => {
    const d = FLOW();
    const flow = flowOf(d.page);
    const told = apply(d, { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: [...flow.boxes].reverse() } }).page;
    assert.deepEqual(layoutTiles(flowOf(told)), ['grid-4', 'grid-3', 'grid-2']);
    refused(() => apply(d, { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'bands', ...add(NEW_SHEET, 'grid-2') } }),
      /'sheet-2' is a flow sheet: its layout is boxes/);
    refused(() => apply(d, { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: flow.boxes.slice(1) } }),
      /a sheet's layout holds its tiles, each once/);
  });

  it('a box taken off leaves every box that read it live reading a copy of what it was (FL6)', () => {
    const { page } = apply(FLOW(), { kind: 'removeTile', tile: 'grid-2' });
    assert.deepEqual(layoutTiles(flowOf(page)), ['grid-3', 'grid-4']);
    for (const id of ['grid-3', 'grid-4']) {
      const source = page.cubes.find((c) => c.id === id)!.cube.source;
      assert.deepEqual(liveGrids(source), [], `${id} reads no box live`);
      if (source._type !== 'rows' || 'grid' in source.input) throw new Error(`${id} is not frozen`);
      assert.equal(source.input.name, 'trades.csv');
      assert.deepEqual(source.input.source, FILE);
    }
    // the page read back: no link left to a grid it does not have
    readPage(JSON.parse(pageToJson(page)));
  });

  it('reads and writes its commands, refused in words', () => {
    for (const c of [{ kind: 'addFlow', id: 'sheet-3', name: 'Prep' }, { kind: 'addFlow', name: null }, { kind: 'placeBox', tile: 'grid-2', x: 10, y: 20 }]) {
      assert.deepEqual(readPageCommand(JSON.parse(JSON.stringify(c))), c);
    }
    assert.throws(() => readPageCommand({ kind: 'placeBox', tile: 'grid-2', x: '10', y: 20 }), /a box's place is two numbers, not \("10", 20\)/);
    assert.throws(() => readPageCommand({ kind: 'placeBox', tile: 'grid-2', x: 1, y: 2, z: 3 }), /z/);
  });

  it("follows the page's renames, and a layout told before a box came keeps it where adding put it", () => {
    const renamed = new Map([['grid-3', 'grid-7'], ['sheet-2', 'sheet-9']]);
    assert.deepEqual(renamedCommand({ kind: 'placeBox', tile: 'grid-3', x: 1, y: 2 }, renamed), { kind: 'placeBox', tile: 'grid-7', x: 1, y: 2 });
    assert.deepEqual(renamedCommand({ kind: 'addFlow', id: 'sheet-2', name: null }, renamed), { kind: 'addFlow', id: 'sheet-9', name: null });
    const { page } = FLOW();
    const stale: PageCommand = { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: [{ tile: 'grid-2', x: 400, y: 400 }, { tile: 'gone', x: 0, y: 0 }] } };
    const now = rearranged(stale, page);
    if (now.kind !== 'layout' || now.layout.kind !== 'flow') throw new Error('not a flow layout');
    assert.deepEqual(now.layout.boxes.find((b) => b.tile === 'grid-2'), { tile: 'grid-2', x: 400, y: 400 });
    assert.deepEqual(now.layout.boxes.map((b) => b.tile).sort(), ['grid-2', 'grid-3', 'grid-4']);
    // a box's Output and stopping, set by the other writer since, kept: its settings, not its place (the review of
    // 2026-10-26: dropped); one the stale layout says it writes, and no longer does, writes nothing
    const set = apply(FLOW(), { kind: 'setOutput', grid: 'grid-3', output: { format: 'csv', name: 'big.csv' } }).page;
    const was: PageCommand = { kind: 'layout', sheet: 'sheet-2', layout: { kind: 'flow', boxes: [{ tile: 'grid-2', x: 400, y: 400, output: { format: 'csv', name: 'old.csv' } }] } };
    const again = rearranged(was, set);
    if (again.kind !== 'layout' || again.layout.kind !== 'flow') throw new Error('not a flow layout');
    assert.deepEqual(again.layout.boxes.find((b) => b.tile === 'grid-3')?.output, { format: 'csv', name: 'big.csv' });
    assert.deepEqual(again.layout.boxes.find((b) => b.tile === 'grid-2'), { tile: 'grid-2', x: 400, y: 400 });
  });
});

describe("a box's input set (setInput: docs/DATACUBE_FLOW_MODE_2026_10_24.md M2)", () => {
  const sourceOf = (page: PageDocument, id: string): CubeSource => page.cubes.find((c) => c.id === id)!.cube.source;

  it('links a box to another of the page, live; freezes it to a copy of what its input is now', () => {
    const d = FLOW();
    // the Sort after the Filter, not the Input: a line drawn from the Filter's output
    const relinked = apply(d, { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3' } });
    assert.deepEqual(liveGrids(sourceOf(relinked.page, 'grid-4')), ['grid-3']);
    // its query as it is: only what it reads moved
    assert.deepEqual(relinked.page.cubes.find((c) => c.id === 'grid-4')!.cube.query, d.page.cubes.find((c) => c.id === 'grid-4')!.cube.query);
    const frozen = apply(relinked, { kind: 'setInput', grid: 'grid-4', side: 'input', input: null }).page;
    const s = sourceOf(frozen, 'grid-4');
    if (s._type !== 'rows' || isLive(s.input)) throw new Error('not frozen');
    assert.equal(s.input.source._type, 'rows', "a copy of the Filter: its source the Filter's, resolved");
    assert.deepEqual(liveGrids(s), []);
    readPage(JSON.parse(pageToJson(frozen)));
  });

  it('refuses in words: a cycle, a grid the page has not, a side the source has not, the same input, a board grid frozen twice', () => {
    const d = FLOW();
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-2', side: 'input', input: { grid: 'grid-3' } }),
      /trades\.csv reads no other grid: it has no input/);
    const chain = apply(d, { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3' } });
    refused(() => apply(chain, { kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-4' } }), /reads its own rows/);
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-3' } }), /reads its own rows/);
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-9' } }), /'grid-9' is no grid of this page/);
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-3', side: 'left', input: { grid: 'grid-1' } }), /its one input is its input, not its left/);
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-2' } }), /reads 'grid-2' already/);
    const frozen = apply(d, { kind: 'setInput', grid: 'grid-3', side: 'input', input: null });
    refused(() => apply(frozen, { kind: 'setInput', grid: 'grid-3', side: 'input', input: null }), /reads a copy already: it is frozen/);
  });

  it("sets a join's left and right, each live or a copy", () => {
    const join = (left: { grid: string }, right: { grid: string }): CubeSource => ({
      _type: 'join', name: 'trades ⋈ trades', left, right, columns: COLUMNS,
      how: { form: 'join', kind: 'inner', on: { rows: [{ left: 'region', op: '==', right: 'region' }] }, prefix: 'their_', keep: null },
    });
    const d = apply(FLOW(), addGrid(cube(join({ grid: 'grid-3' }, { grid: 'grid-4' })), 'sheet-2', 'grid-3'));
    assert.deepEqual(liveGrids(sourceOf(d.page, 'grid-5')), ['grid-3', 'grid-4']);
    const right = apply(d, { kind: 'setInput', grid: 'grid-5', side: 'right', input: { grid: 'grid-2' } }).page;
    assert.deepEqual(liveGrids(sourceOf(right, 'grid-5')), ['grid-3', 'grid-2']);
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-5', side: 'input', input: { grid: 'grid-2' } }), /is a join: its inputs are its left and its right/);
    // a box of the join's inputs taken off: the join reads a copy of it, its other input still live (FL6)
    const gone = apply(d, { kind: 'removeTile', tile: 'grid-4' }).page;
    assert.deepEqual(liveGrids(sourceOf(gone, 'grid-5')), ['grid-3']);
    readPage(JSON.parse(pageToJson(gone)));
  });

  it('a box opened again passes on the columns its input gained, as a saved grid over a file does not', () => {
    const wider = [...COLUMNS, { name: 'desk', type: 'String' }];
    const box = cubeOpenedAgain(cube(rows('grid-2')), { ...rows('grid-2'), columns: wider });
    assert.deepEqual(box.notes, [], 'nothing hidden, nothing to say');
    assert.notEqual((box.document.configuration['columns'] as Record<string, { hidden?: boolean }> | undefined)?.['desk']?.hidden, true);
    const filed = cubeOpenedAgain(cube(FILE), { ...FILE, columns: wider });
    assert.deepEqual(filed.notes, ['1 new column since this cube was saved, hidden: desk']);
  });

  it("reads a Filter's False port (FL7): its input's rows its condition does not hold for; refused with no filter", () => {
    const d = apply(FLOW(), { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3', port: 'false' } });
    const live = sourceOf(d.page, 'grid-4');
    assert.deepEqual(live._type === 'rows' ? live.input : undefined, { grid: 'grid-3', port: 'false' });
    assert.deepEqual(liveGrids(live), ['grid-3']);
    // resolved: the Filter's query, its condition turned round
    const read = resolvedSource(d.page, live, 'grid-4');
    const filter = read._type === 'rows' && !isLive(read.input) ? read.input.query.filter : undefined;
    assert.deepEqual(filter, { kind: 'not', child: { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 } });
    // frozen when the Filter goes: a copy of its False, not its rows
    const gone = apply(d, { kind: 'removeTile', tile: 'grid-3' }).page;
    const frozen = sourceOf(gone, 'grid-4');
    assert.equal(frozen._type === 'rows' && !isLive(frozen.input) ? frozen.input.query.filter?.kind : undefined, 'not');
    // a box with no filter has no False
    refused(() => apply(FLOW(), { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-2', port: 'false' } }),
      /'grid-2' has no filter: its False port leaves no row out/);
    assert.deepEqual(readPageCommand({ kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3', port: 'false' } }),
      { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3', port: 'false' } });
    assert.throws(() => readPageCommand({ kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3', port: 'true' } }),
      /a live input's port is a Filter's 'false' or a Join's 'left' or 'right', not "true"/);
    assert.deepEqual(renamedCommand({ kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3', port: 'false' } }, new Map([['grid-3', 'grid-9']])),
      { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-9', port: 'false' } });
    assert.deepEqual(boxInputs(d.page.cubes.find((c) => c.id === 'grid-4')!.cube), [{ side: 'input', grid: 'grid-3', port: 'false' }]);
  });

  it("reads a Join's L and R (§3.3): each side's own rows, those the other's pair with none; refused where none can be", () => {
    const join: CubeSource = {
      _type: 'join', name: 'trades ⋈ trades', left: { grid: 'grid-3' }, right: { grid: 'grid-4' }, columns: COLUMNS,
      how: { form: 'join', kind: 'inner', on: { rows: [{ left: 'notional', op: '<', right: 'notional' }] }, prefix: 'their_', keep: null },
    };
    const d = apply(FLOW(), addGrid(cube(join), 'sheet-2', 'grid-3'),
      addGrid(cube({ ...rows('grid-5'), input: { grid: 'grid-5', port: 'right' } }), 'sheet-2', 'grid-5'));
    const live = sourceOf(d.page, 'grid-6');
    assert.deepEqual(liveGrids(live), ['grid-5']);
    // resolved: the Sort's rows (its Right), each the Filter's rows (its Left) pair with none -- the comparison turned round
    const read = resolvedSource(d.page, live, 'grid-6');
    const input = read._type === 'rows' && !isLive(read.input) ? read.input : undefined;
    assert.deepEqual(input?.query.sorts, [{ column: 'notional', direction: 'desc' }], "its Right's own query");
    assert.deepEqual(input?.unmatched?.map((u) => [u.by.query.filter?.kind, u.on]), [['condition', { rows: [{ left: 'notional', op: '>', right: 'notional' }] }]]);
    assert.deepEqual(liveGrids(read), [], 'a copy with no link left in it');
    // frozen when the Join goes: a copy of its R, written and read back
    const gone = apply(d, { kind: 'removeTile', tile: 'grid-5' }).page;
    const frozen = sourceOf(gone, 'grid-6');
    assert.equal(frozen._type === 'rows' && !isLive(frozen.input) ? frozen.input.unmatched?.length : undefined, 1);
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(gone)))), pageToJson(gone), 'written and read back the same');
    // a join set so that its R pairs nothing is refused while a box reads it, in its words
    refused(() => apply(d, { kind: 'setJoin', grid: 'grid-5', how: { form: 'append' } }), /grid-6: a Union keeps every row of both/);
    refused(() => apply(FLOW(), { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3', port: 'left' } }),
      /'grid-3' is no join: it has no Left and Right unmatched/);
    assert.deepEqual(readPageCommand({ kind: 'setInput', grid: 'grid-6', side: 'input', input: { grid: 'grid-5', port: 'left' } }),
      { kind: 'setInput', grid: 'grid-6', side: 'input', input: { grid: 'grid-5', port: 'left' } });
  });

  it("says what pairs a side's row with none (join-how.ts unmatchedOn): the join's own, turned round for its Right", () => {
    const rowsOn = { rows: [{ left: 'a', op: '<=' as const, right: 'b' }, { left: 'n', op: '~' as const, right: 'm', within: 1 }] };
    const how = { form: 'join', kind: 'left', on: rowsOn, prefix: 'their_', keep: null } as const;
    assert.deepEqual(unmatchedOn(how, 'left'), rowsOn);
    assert.deepEqual(unmatchedOn(how, 'right'), { rows: [{ left: 'b', op: '>=', right: 'a' }, { left: 'm', op: '~', right: 'n', within: 1 }] });
    // a semi-join's Pure condition names both sides by their own names: its two rows named the other way
    const pure = lambda(['x', 'y'], fn('equal', property(variable('x'), 'a'), property(variable('y'), 'b')));
    const turned = unmatchedOn({ form: 'semi', on: { lambda: pure } }, 'right');
    assert.deepEqual('lambda' in turned ? turned.lambda.parameters.map((p) => p.name) : undefined, ['y', 'x']);
    refused(() => unmatchedOn({ form: 'append' }, 'left'), /a Union keeps every row of both/);
    refused(() => unmatchedOn({ ...how, on: { lambda: pure } }, 'left'), /its condition is Pure/);
    refused(() => unmatchedOn({ form: 'asOf', match: { left: 'a', op: '>=', right: 'b' }, on: { rows: [] }, prefix: 'p_', keep: null }, 'left'),
      /an as-of join's rows of mine with no nearest row/);
  });

  it("sets a Join box's join again (setJoin): its inputs as they are, undone by the commands between", () => {
    const join: CubeSource = {
      _type: 'join', name: 'trades ⋈ trades', left: { grid: 'grid-3' }, right: { grid: 'grid-4' }, columns: COLUMNS,
      how: { form: 'join', kind: 'inner', on: { rows: [{ left: 'region', op: '==', right: 'region' }] }, prefix: 'their_', keep: null },
    };
    const d = apply(FLOW(), addGrid(cube(join), 'sheet-2', 'grid-3'));
    const union = apply(d, { kind: 'setJoin', grid: 'grid-5', how: { form: 'append' } }).page;
    const now = sourceOf(union, 'grid-5');
    assert.ok(now._type === 'join' && now.how.form === 'append');
    assert.deepEqual(liveGrids(now), ['grid-3', 'grid-4'], 'its inputs as they are');
    assert.equal(boxSays(union.cubes.find((c) => c.id === 'grid-5')!.cube).tool, 'union', 'an Append is a Union box');
    assert.equal(boxSays(d.page.cubes.find((c) => c.id === 'grid-5')!.cube).tool, 'join');
    // every row with every row -- matching only, on no condition -- is an Append Fields box (join-how.ts isEveryPair)
    const pairs = apply(d, { kind: 'setJoin', grid: 'grid-5', how: { form: 'join', kind: 'inner', on: { rows: [] }, prefix: 'their_', keep: null } }).page;
    assert.deepEqual(boxSays(pairs.cubes.find((c) => c.id === 'grid-5')!.cube), { tool: 'appendFields', also: [], says: 'each row with every row of theirs' });
    // a join on similar text is a Fuzzy Match box (join-how.ts isFuzzy)
    const fuzzy = apply(d, { kind: 'setJoin', grid: 'grid-5', how: { form: 'join', kind: 'inner', on: { rows: [{ left: 'region', op: '~', right: 'region', within: 1 }] }, prefix: 'their_', keep: null } }).page;
    assert.deepEqual(boxSays(fuzzy.cubes.find((c) => c.id === 'grid-5')!.cube), { tool: 'fuzzyMatch', also: [], says: 'matching rows only, on region ≈ region (within 1)' });
    const allOfMine = apply(d, { kind: 'setJoin', grid: 'grid-5', how: { form: 'join', kind: 'left', on: { rows: [] }, prefix: 'their_', keep: null } }).page;
    assert.equal(boxSays(allOfMine.cubes.find((c) => c.id === 'grid-5')!.cube).tool, 'join', 'all of mine on no condition stays a Join');
    refused(() => apply({ page: union, held: idsOf(union) }, { kind: 'setJoin', grid: 'grid-5', how: { form: 'append' } }), /joins so already/);
    refused(() => apply(d, { kind: 'setJoin', grid: 'grid-3', how: { form: 'append' } }), /is no join/);
    const back = applyPlanned({ page: union, held: idsOf(union) }, pageBetween(union, d.page, 'history'), false).draft.page;
    assert.deepEqual(sourceOf(back, 'grid-5'), sourceOf(d.page, 'grid-5'), 'undone: its join as it was');
  });

  it("takes a Union of more than two (M2): one added on its next side, its more set whole, one taken off, undone", () => {
    const union: CubeSource = {
      _type: 'join', name: 'trades ∪ trades', left: { grid: 'grid-2' }, right: { grid: 'grid-3' }, columns: COLUMNS, how: { form: 'append' },
    };
    const d = apply(FLOW(), addGrid(cube(union), 'sheet-2', 'grid-3'));
    const box = (page: PageDocument) => page.cubes.find((c) => c.id === 'grid-5')!.cube;
    // a line let go on it: its next side, one input more
    const three = apply(d, { kind: 'setInput', grid: 'grid-5', side: 'more1', input: { grid: 'grid-4' } });
    const s3 = sourceOf(three.page, 'grid-5');
    assert.deepEqual(s3._type === 'join' ? s3.more : undefined, [{ grid: 'grid-4' }]);
    assert.deepEqual(liveGrids(s3), ['grid-2', 'grid-3', 'grid-4']);
    assert.deepEqual(inputSides(s3), ['left', 'right', 'more1']);
    assert.deepEqual(boxInputs(box(three.page)).map((i) => i.side), ['left', 'right', 'more1']);
    assert.deepEqual(boxSays(box(three.page)), { tool: 'union', also: [], says: 'the rows of all 3, columns by name' });
    // resolved: a copy of each, a leaf each
    const read = resolvedSource(three.page, s3, 'grid-5');
    assert.deepEqual(liveGrids(read), []);
    assert.deepEqual(leavesOf(read), [FILE, FILE, FILE]);
    assert.equal(pageToJson(readPage(JSON.parse(pageToJson(three.page)))), pageToJson(three.page), 'written and read back the same');
    // its more1 linked again, frozen; a side past its next refused; one that reads its own rows refused
    const again = sourceOf(apply(three, { kind: 'setInput', grid: 'grid-5', side: 'more1', input: { grid: 'grid-2' } }).page, 'grid-5');
    assert.deepEqual(again._type === 'join' ? again.more : undefined, [{ grid: 'grid-2' }]);
    const frozen = sourceOf(apply(three, { kind: 'setInput', grid: 'grid-5', side: 'more1', input: null }).page, 'grid-5');
    assert.ok(frozen._type === 'join' && frozen.more !== undefined && !isLive(frozen.more[0]!), 'a copy of what it read');
    refused(() => apply(three, { kind: 'setInput', grid: 'grid-5', side: 'more3', input: { grid: 'grid-2' } }), /has 3 inputs: it has no input 5/);
    refused(() => apply(three, { kind: 'setInput', grid: 'grid-5', side: 'more2', input: { grid: 'grid-5' } }), /'grid-5' reads its own rows/);
    refused(() => apply(d, { kind: 'setInput', grid: 'grid-5', side: 'more1', input: null }), /has no more1 to freeze/);
    // a join of two takes no more
    const inner = { form: 'join', kind: 'inner', on: { rows: [] }, prefix: 'their_', keep: null } as const;
    refused(() => apply(three, { kind: 'setJoin', grid: 'grid-5', how: inner }), /would join 3 inputs: only a Union takes more than two/);
    refused(() => apply(apply(d, { kind: 'setJoin', grid: 'grid-5', how: inner }), { kind: 'setInput', grid: 'grid-5', side: 'more1', input: { grid: 'grid-4' } }),
      /only a Union takes more inputs/);
    // its more set whole (setJoin's more; left out, kept as it is)
    const four = apply(three, { kind: 'setJoin', grid: 'grid-5', how: { form: 'append' }, more: [{ grid: 'grid-4' }, { grid: 'grid-1' }] });
    assert.deepEqual(liveGrids(sourceOf(four.page, 'grid-5')), ['grid-2', 'grid-3', 'grid-4', 'grid-1']);
    refused(() => apply(four, { kind: 'setJoin', grid: 'grid-5', how: { form: 'append' } }), /joins so already/);
    // taken off (page-gestures.ts takeOffCommands): its right, those after it moved up
    const off = takeOffCommands('grid-5', sourceOf(four.page, 'grid-5'), 'right');
    assert.deepEqual(off.map((c) => c.kind), ['setInput', 'setJoin']);
    const after = sourceOf(apply(four, ...off).page, 'grid-5');
    assert.deepEqual(after._type === 'join' ? [after.left, after.right, after.more] : undefined, [{ grid: 'grid-2' }, { grid: 'grid-4' }, [{ grid: 'grid-1' }]]);
    const last = sourceOf(apply(three, ...takeOffCommands('grid-5', s3, 'more1')).page, 'grid-5');
    assert.deepEqual(last, sourceOf(d.page, 'grid-5'), 'its third taken off: a Union of two, with no more');
    refused(() => takeOffCommands('grid-5', sourceOf(d.page, 'grid-5'), 'left'), /reads two inputs: one taken off is frozen/);
    refused(() => takeOffCommands('grid-5', s3, 'more2'), /has 3 inputs: it has no input 4/);
    // a box it reads taken off the page: that input frozen, as a Join's side is
    const gone = sourceOf(apply(three, { kind: 'removeTile', tile: 'grid-4' }).page, 'grid-5');
    assert.ok(gone._type === 'join' && !isLive(gone.more![0]!));
    // undone and redone by the commands between
    const back = applyPlanned({ page: four.page, held: idsOf(four.page) }, pageBetween(four.page, d.page, 'history'), false).draft.page;
    assert.deepEqual(sourceOf(back, 'grid-5'), sourceOf(d.page, 'grid-5'), 'undone: a Union of two');
    const redone = applyPlanned({ page: d.page, held: idsOf(d.page) }, pageBetween(d.page, four.page, 'history'), false).draft.page;
    assert.deepEqual(sourceOf(redone, 'grid-5'), sourceOf(four.page, 'grid-5'), 'redone: of four');
    // read, written and renamed as every command is
    const set: PageCommand = { kind: 'setJoin', grid: 'grid-5', how: { form: 'append' }, more: [{ grid: 'grid-4' }] };
    assert.deepEqual(readPageCommand(JSON.parse(JSON.stringify(set))), set);
    assert.deepEqual(readPageCommand({ kind: 'setInput', grid: 'grid-5', side: 'more2', input: { grid: 'grid-4' } }),
      { kind: 'setInput', grid: 'grid-5', side: 'more2', input: { grid: 'grid-4' } });
    refused(() => readPageCommand({ kind: 'setInput', grid: 'grid-5', side: 'more0', input: null }), /a Union's more1, more2, \.\.\., not "more0"/);
    refused(() => readPageCommand({ kind: 'setJoin', grid: 'grid-5', how: { form: 'append' }, more: { grid: 'grid-4' } }), /are a list/);
    assert.deepEqual(renamedCommand(set, new Map([['grid-4', 'grid-9'], ['grid-5', 'grid-8']])),
      { kind: 'setJoin', grid: 'grid-8', how: { form: 'append' }, more: [{ grid: 'grid-9' }] });
  });

  it('reads and writes it, follows renames, and is the undo of a removal that froze its readers', () => {
    for (const c of [
      { kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-2' } },
      { kind: 'setInput', grid: 'grid-5', side: 'right', input: null },
    ]) assert.deepEqual(readPageCommand(JSON.parse(JSON.stringify(c))), c);
    assert.throws(() => readPageCommand({ kind: 'setInput', grid: 'grid-3', side: 'top', input: null }), /its input, a join's left or right, or a Union's more1, more2, \.\.\., not "top"/);
    assert.throws(() => readPageCommand({ kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-2', x: 1 } }), /a live input is its grid alone, and its port, not x/);
    const renamed = new Map([['grid-2', 'grid-8'], ['grid-3', 'grid-7']]);
    assert.deepEqual(renamedCommand({ kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-2' } }, renamed),
      { kind: 'setInput', grid: 'grid-7', side: 'input', input: { grid: 'grid-8' } });
    // a box taken off, and back: its readers live on it again, by the commands between the two pages
    const before = FLOW().page;
    const after = apply(FLOW(), { kind: 'removeTile', tile: 'grid-2' }).page;
    const back = pageBetween(after, before, 'history');
    assert.ok(back.some((c) => c.kind === 'setInput'), 'a setInput among the undo');
    const undone = applyPlanned({ page: after, held: idsOf(after) }, back, false).draft.page;
    for (const id of ['grid-3', 'grid-4']) assert.deepEqual(liveGrids(sourceOf(undone, id)), ['grid-2'], `${id} live on the Input again`);
  });

  it('undoes a swap of two boxes, each frozen first where setting it at once would read its own rows', () => {
    const d = apply(FLOW(), { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-3' } });
    // grid-2 → grid-3 → grid-4, then grid-2 → grid-4 → grid-3
    const swapped = apply(d, { kind: 'setInput', grid: 'grid-4', side: 'input', input: { grid: 'grid-2' } },
      { kind: 'setInput', grid: 'grid-3', side: 'input', input: { grid: 'grid-4' } });
    for (const [from, to] of [[d.page, swapped.page], [swapped.page, d.page]] as const) {
      const made = applyPlanned({ page: from, held: idsOf(from) }, pageBetween(from, to, 'history'), false).draft.page;
      for (const id of ['grid-3', 'grid-4']) assert.deepEqual(sourceOf(made, id), sourceOf(to, id), id);
    }
  });

  it("a flow sheet's removal undone makes a flow sheet, its boxes where they were", () => {
    const before = FLOW().page;
    const after = apply(FLOW(), { kind: 'removeSheet', sheet: 'sheet-2' }).page;
    const back = pageBetween(after, before, 'history');
    assert.ok(back.some((c) => c.kind === 'addFlow'), 'an addFlow among the undo');
    const undone = applyPlanned({ page: after, held: idsOf(after) }, back, false).draft.page;
    assert.deepEqual(flowOf(undone), flowOf(before));
  });
});

describe('a flow page read, and what it opens as it opens', () => {
  it('refuses a flow\'s tile that is no grid, and a box placed twice or off the canvas', () => {
    const page = FLOW().page;
    const raw = JSON.parse(pageToJson(page)) as { sheets: { layout: { boxes?: unknown[] } }[]; views: unknown[] };
    const withBoxes = (boxes: unknown[]): unknown => ({ ...raw, sheets: raw.sheets.map((s, i) => (i === 1 ? { ...s, layout: { ...s.layout, boxes } } : s)) });
    const box = (tile: string, x = 0, y = 0): unknown => ({ tile, x, y });
    assert.throws(() => readPage(withBoxes([box('grid-2'), box('grid-3'), box('grid-4'), box('grid-4', 9, 9)])),
      (e: unknown) => e instanceof PageDocumentError && /twice/.test(e.message));
    assert.throws(() => readPage(withBoxes([box('grid-2', -1), box('grid-3'), box('grid-4')])), /neither below zero/);
  });

  it('opens none of a flow\'s boxes as it opens: each is a document until it is shown (boxCubes)', () => {
    const { page } = FLOW();
    assert.deepEqual([...boxCubes(page)].sort(), ['grid-2', 'grid-3', 'grid-4']);
    assert.deepEqual([...boxCubes(BOARD())], [], 'a board\'s grids open as the page opens');
  });
});
