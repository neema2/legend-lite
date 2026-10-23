// A dashboard's page filters (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2; src/page-filters.ts): read strictly, said
// in words, put into a class's rows before a grid's columns are taken, reaching every grid over that class but the one a
// chart's click came from; set and taken off by page commands, kept by the page document, and a chart's click taken
// off with its chart.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { toJson } from '../../pure-protocol/src/index.ts';
import { classLambda } from '../src/class-source.ts';
import type { ClassSource, CubeSource } from '../src/cube-document.ts';
import { applyPageCommand, applyPageCommands, idsOf, readPageCommand, type PageCommand, type PageDraft } from '../src/page-commands.ts';
import { openingOrder, pageToJson, readPage, type PageDocument } from '../src/page-document.ts';
import { chartPicked, classPick, conditionWords, filterWords, filteredByClass, gridLens, readPageFilter, readPageFilters, type PageFilter } from '../src/page-filters.ts';
import { CubeRefusal, type FilterNode } from '../src/snapshot.ts';
import { pageApply, pageFilter } from '../src/headless/page.ts';
import { corpus } from './fixtures/pages/make.ts';

const TRADE = 'trades::domain::Trade';
const classOver = (columns: ClassSource['columns']): ClassSource => ({
  _type: 'class', name: 'Trade', class: TRADE, project: 'datacube:trades', workspace: 'datacube', revision: '3',
  mapping: 'trades::mapping::Model', runtime: 'trades::runtime::Browser', columns,
});
const EMEA: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
const BIG: FilterNode = { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 };
const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};

// the corpus's frame page, its two grids' cubes over the class Trade (grid's charts: chart; grid-2's: chart-2)
const frames = (await corpus())['trades.page.json']!;
const classPage: PageDocument = {
  ...frames,
  cubes: frames.cubes.map((c) => ({ ...c, cube: { ...c.cube, source: classOver((c.cube.source as { columns: ClassSource['columns'] }).columns) } })),
};
const draft = (page: PageDocument = classPage): PageDraft => ({ page, held: idsOf(page) });
const apply = (command: PageCommand, d: PageDraft = draft()): PageDraft => applyPageCommand(d, command).draft;

describe('a page filter, read strictly and said in words', () => {
  const charts = new Set(['chart']);

  it('its id, its class, its condition (the grid filter\'s reader) and the chart it came from', () => {
    const f = { id: 'filter-1', class: TRADE, condition: EMEA, from: 'chart' };
    assert.deepEqual(readPageFilter(f, charts), f);
    refused(() => readPageFilter({ ...f, id: 'f1' }, charts), /a page filter's id is filter-N/);
    refused(() => readPageFilter({ ...f, class: '' }, charts), /names no class/);
    refused(() => readPageFilter({ ...f, from: 'chart-9' }, charts), /came from "chart-9", which is no chart of the page/);
    refused(() => readPageFilter({ ...f, extra: 1 }, charts), /takes id, class, condition and from, not extra/);
    refused(() => readPageFilter({ ...f, condition: { kind: 'condition', column: 'region', operator: 'equal' } }, charts),
      /'equal' takes a value/);
    // a property is read `$x.p`: a name the grammar reads bare
    refused(() => readPageFilter({ ...f, condition: { ...EMEA, column: 'true' } }, charts), /names 'true', which is no property's name/);
    refused(() => readPageFilters([f, f], charts), /two page filters are filter-1/);
  });

  it('in words: the class, the property, DataCube\'s label for the operator, the values', () => {
    assert.equal(filterWords({ id: 'filter-1', class: TRADE, condition: EMEA }), 'Trade: region = EMEA');
    assert.equal(conditionWords({ kind: 'condition', column: 'region', operator: 'in', value: ['EMEA', 'AMER'] }), 'region in EMEA, AMER');
    assert.equal(conditionWords({ kind: 'and', children: [EMEA, { kind: 'or', children: [BIG, { kind: 'condition', column: 'qty', operator: 'isEmpty' }] }] }),
      'region = EMEA and (notional > 100 or qty is null)');
    assert.equal(conditionWords({ kind: 'not', child: EMEA }), 'not (region = EMEA)');
  });
});

describe('what reaches a grid: the class\'s rows filtered before its columns are taken', () => {
  const source = classOver([{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }]);
  const filters: PageFilter[] = [
    { id: 'filter-1', class: TRADE, condition: EMEA },
    { id: 'filter-2', class: TRADE, condition: BIG, from: 'chart' },
    { id: 'filter-3', class: 'other::Customer', condition: EMEA },
  ];

  it('a query that reads no getAll of the class is refused, never left unfiltered unsaid (the audit\'s 8)', () => {
    assert.throws(() => filteredByClass(classLambda('other::Customer', ['region']), TRADE, { _type: 'var', name: 'w' }),
      (e: unknown) => e instanceof CubeRefusal && /reads no Trade\.all\(\)/.test(e.message));
  });

  it('a saved page\'s cubes open with a class\'s after every other, wherever its tile sits (the audit\'s 2)', () => {
    const cubes = classPage.cubes.map((c, i) => (i === 0 ? c : { ...c, cube: frames.cubes[i]!.cube }));
    assert.deepEqual(openingOrder(cubes).map((c) => c.id), [cubes[1]!.id, cubes[0]!.id]);
  });

  it('every getAll of the class, filtered first -- the query around it as it was', () => {
    const query = classLambda(TRADE, ['region']);
    const where = { _type: 'var' as const, name: 'w' };
    const out = filteredByClass(query, TRADE, where);
    assert.equal(toJson(out), toJson(classLambda(TRADE, ['region'], where)));
  });

  it('a grid over a class: its class\'s filters ANDed, but its own chart\'s click; none over another source', () => {
    const mine = gridLens(filters, source, new Set(['chart']));
    assert.deepEqual(mine.taken, ['filter-1']);
    const theirs = gridLens(filters, source, new Set());
    assert.deepEqual(theirs.taken, ['filter-1', 'filter-2']);
    assert.notEqual(mine.key, theirs.key, 'another set of filters, another lens');
    assert.equal(gridLens(filters, source, new Set(['chart'])).key, mine.key, 'the same filters, the same key: not run again');
    const query = classLambda(TRADE, ['region', 'notional']);
    assert.match(toJson(theirs.lens!(query)), /"function":"filter"/);
    const frame: CubeSource = { _type: 'frame', name: 'trades', columns: [] };
    assert.deepEqual(gridLens(filters, frame, new Set()), { taken: [], key: '', refused: [] }, 'a frame\'s grid: not reached');
  });

  it('a filter on a property its class has not is refused for that grid, said -- never dropped unsaid', () => {
    const narrow = classOver([{ name: 'region', type: 'String' }]);
    const lens = gridLens(filters, narrow, new Set());
    assert.deepEqual(lens.taken, ['filter-1']);
    assert.deepEqual(lens.refused, [{ id: 'filter-2', why: 'Trade has no notional' }]);
  });
});

describe('page filters set and taken off by page commands, kept by the page document', () => {
  const set = (condition: FilterNode, from: string | null = null, id?: string): PageCommand =>
    ({ kind: 'setPageFilter', ...(id !== undefined ? { id } : {}), class: TRADE, condition, from });

  it('set: its id allocated after every one held; set again by its id, replaced; taken off', () => {
    const one = applyPageCommand(draft(), set(EMEA));
    assert.equal((one.command as { id: string }).id, 'filter-1');
    assert.deepEqual(one.draft.page.filters, [{ id: 'filter-1', class: TRADE, condition: EMEA }]);
    const again = apply(set(BIG, null, 'filter-1'), one.draft);
    assert.deepEqual(again.page.filters, [{ id: 'filter-1', class: TRADE, condition: BIG }], 'replaced, in its place');
    const gone = apply({ kind: 'removePageFilter', filter: 'filter-1' }, again);
    assert.equal(gone.page.filters, undefined, 'none: no field');
    const next = applyPageCommand(gone, set(EMEA));
    assert.equal((next.command as { id: string }).id, 'filter-2', 'filter-1 was held: never given out again');
  });

  it('a page opened again holds its tiles\' ids alone: a filter it has keeps its id, the next one another (the audit\'s 1)', () => {
    const saved = readPage(pageToJson(applyPageCommand(draft(), set(EMEA)).draft.page));
    // opened again: what it has held is what it holds (idsOf: its tiles, cubes and sheets)
    const again = applyPageCommand({ page: saved, held: idsOf(saved) }, set(BIG));
    assert.equal((again.command as { id: string }).id, 'filter-2');
    assert.deepEqual(readPage(pageToJson(again.draft.page)).filters?.map((f) => f.id), ['filter-1', 'filter-2']);
    // set again by its id: replaced, still one
    const replaced = apply(set(BIG, null, 'filter-1'), { page: saved, held: idsOf(saved) });
    assert.deepEqual(replaced.page.filters, [{ id: 'filter-1', class: TRADE, condition: BIG }]);
  });

  it('refused in words: a class no grid reads, a property its grids have not, a chart not over it, a filter not there', () => {
    refused(() => apply({ ...set(EMEA), class: 'other::Customer' } as PageCommand), /no grid of the page reads other::Customer/);
    refused(() => apply(set({ ...EMEA, column: 'colour' })), /Trade has no colour/);
    refused(() => apply(set(EMEA, 'chart-9')), /'chart-9' is no chart of the page/);
    refused(() => apply(set(EMEA, 'grid')), /'grid' is no chart of the page/);
    refused(() => apply({ kind: 'removePageFilter', filter: 'filter-7' }), /'filter-7' is not a filter of the page/);
    // over a frame, no class: a page filter has nothing to reach
    refused(() => applyPageCommand({ page: frames, held: idsOf(frames) }, set(EMEA)), /no grid of the page reads/);
  });

  it('a chart removed takes its click\'s filter with it; the page written and read back keeps the rest', () => {
    const { draft: d } = applyPageCommands(draft(), [set(EMEA, 'chart'), set(BIG)]);
    assert.equal(d.page.filters?.length, 2);
    const without = apply({ kind: 'removeTile', tile: 'chart' }, d);
    assert.deepEqual(without.page.filters?.map((f) => f.id), ['filter-2']);
    const back = readPage(pageToJson(d.page));
    assert.deepEqual(back.filters, d.page.filters);
    const raw = JSON.parse(pageToJson(d.page));
    assert.throws(() => readPage(JSON.stringify({ ...raw, filters: [] })), /'filters' is an empty list/);
    assert.throws(() => readPage(JSON.stringify({ ...raw, filters: [{ ...raw.filters[0], from: 'chart-9' }] })),
      /which is no chart of the page/);
  });

  it('read from JSON as the same command (Python\'s, a live channel\'s)', () => {
    for (const c of [set(EMEA, 'chart', 'filter-1'), set(BIG), { kind: 'removePageFilter', filter: 'filter-1' } as PageCommand]) {
      assert.deepEqual(readPageCommand(JSON.parse(JSON.stringify(c))), c);
    }
    refused(() => readPageCommand({ kind: 'setPageFilter', class: TRADE, condition: EMEA }), /the chart a page filter came from is text/);
  });
});

describe("a chart's mark on a grid over a class (classPick: the screen's click's rule and Python's chart.select's)", () => {
  const over = classOver([{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }]);
  const OWN: FilterNode = { kind: 'condition', column: 'double', operator: 'greaterThan', value: 1 };
  const AMER: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'AMER' };
  const fromChart = (condition: FilterNode): PageFilter => ({ id: 'filter-3', class: TRADE, condition, from: 'chart-1' });

  it("its conditions on the class's properties are the chart's page filter, replacing its last; the rest the grid's own", () => {
    const pick = classPick(over, 'chart-1', [EMEA, OWN], false);
    assert.deepEqual(pick.own, [OWN]);
    assert.deepEqual(pick.command([]), { kind: 'setPageFilter', class: TRADE, condition: EMEA, from: 'chart-1' });
    assert.deepEqual(classPick(over, 'chart-1', [AMER], false).command([fromChart(EMEA)]),
      { kind: 'setPageFilter', id: 'filter-3', class: TRADE, condition: AMER, from: 'chart-1' }, 'its last replaced');
    assert.deepEqual(classPick(over, 'chart-1', [EMEA, BIG], false).command([]),
      { kind: 'setPageFilter', class: TRADE, condition: { kind: 'and', children: [EMEA, BIG] }, from: 'chart-1' });
  });

  it('the same mark again: a click takes it off, Python keeps it; none on the class, or cleared, takes it off', () => {
    assert.deepEqual(classPick(over, 'chart-1', [EMEA], true).command([fromChart(EMEA)]), { kind: 'removePageFilter', filter: 'filter-3' });
    // the same as data, its keys in another order (F43: a page read back writes them in its reader's order)
    const reread = { value: 'EMEA', operator: 'equal', column: 'region', kind: 'condition' } as FilterNode;
    assert.deepEqual(classPick(over, 'chart-1', [EMEA], true).command([fromChart(reread)]), { kind: 'removePageFilter', filter: 'filter-3' });
    assert.equal(classPick(over, 'chart-1', [EMEA], false).command([fromChart(reread)]), undefined);
    assert.equal(classPick(over, 'chart-1', [EMEA], false).command([fromChart(EMEA)]), undefined);
    const cleared = classPick(over, 'chart-1', null, false);
    assert.equal(cleared.own, null);
    assert.deepEqual(cleared.command([fromChart(EMEA)]), { kind: 'removePageFilter', filter: 'filter-3' });
    assert.equal(cleared.command([]), undefined, 'nothing to take off');
    assert.deepEqual(classPick(over, 'chart-1', [OWN], false).command([fromChart(EMEA)]), { kind: 'removePageFilter', filter: 'filter-3' });
  });
});

describe("a chart's mark picked (chartPicked: the screen's click and chip, Python's chart.select)", () => {
  const over = classOver([{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }]);
  const OWN: FilterNode = { kind: 'condition', column: 'double', operator: 'greaterThan', value: 1 };
  const reread = (c: FilterNode): FilterNode => JSON.parse(JSON.stringify(Object.fromEntries(Object.entries(c).reverse()))) as FilterNode;
  const pick = (o: Partial<Parameters<typeof chartPicked>[0]>): ReturnType<typeof chartPicked> => chartPicked({
    source: undefined, chart: 'chart-1', conditions: null, old: [], filters: [], toggle: true, ...o,
  });

  it("on any grid: the grid's refilter (the old off, the new on) and the chart's select; nothing when nothing changes", () => {
    assert.deepEqual(pick({ conditions: [OWN] }), {
      refilter: { kind: 'refilter', remove: [], add: [OWN] }, page: [{ kind: 'select', chart: 'chart-1', conditions: [OWN] }],
    });
    assert.deepEqual(pick({ conditions: [EMEA], old: [OWN] }).refilter, { kind: 'refilter', remove: [OWN], add: [EMEA] });
    assert.deepEqual(pick({ conditions: null, old: [OWN] }), {
      refilter: { kind: 'refilter', remove: [OWN], add: [] }, page: [{ kind: 'select', chart: 'chart-1', conditions: [] }],
    });
    assert.deepEqual(pick({ conditions: null }), { refilter: undefined, page: [] }, 'nothing selected, nothing cleared');
  });

  it('the same mark again, compared as data (its keys in any order): a click takes it off, Python keeps it', () => {
    // the selection as a page read back writes it, its keys in its reader's order (the S7 audit's Y-9)
    assert.deepEqual(pick({ conditions: [OWN], old: [reread(OWN)] }).refilter, { kind: 'refilter', remove: [reread(OWN)], add: [] });
    assert.deepEqual(pick({ conditions: [OWN], old: [reread(OWN)], toggle: false }), { refilter: undefined, page: [] });
  });

  it("on a grid over a class: the properties' part the chart's page filter, the rest its grid's own", () => {
    const made = pick({ source: over, conditions: [EMEA, OWN] });
    assert.deepEqual(made.refilter, { kind: 'refilter', remove: [], add: [OWN] });
    assert.deepEqual(made.page, [
      { kind: 'setPageFilter', class: TRADE, condition: EMEA, from: 'chart-1' },
      { kind: 'select', chart: 'chart-1', conditions: [OWN] },
    ]);
    // all of it on the class: the page filter alone, the grid left whole
    assert.deepEqual(pick({ source: over, conditions: [EMEA] }), {
      refilter: undefined, page: [{ kind: 'setPageFilter', class: TRADE, condition: EMEA, from: 'chart-1' }],
    });
    const mine: PageFilter = { id: 'filter-3', class: TRADE, condition: EMEA, from: 'chart-1' };
    assert.deepEqual(pick({ source: over, conditions: null, filters: [mine] }).page, [{ kind: 'removePageFilter', filter: 'filter-3' }]);
  });
});

describe('a page filter as Python asks for one (headless pageFilter): the command the screen\'s Add a Filter makes', () => {
  const text = pageToJson(classPage);
  const ask = (given: unknown): { command?: PageCommand; problems?: string[] } => JSON.parse(pageFilter(text, JSON.stringify(given)));

  it('its class by name or path, its condition as Python writes one, typed by the class\'s properties, applied by pageApply', () => {
    const { command } = ask({ class: 'Trade', filter: ['region', 'in', ['EMEA', 'AMER']] });
    assert.deepEqual(command, { kind: 'setPageFilter', class: TRADE,
      condition: { kind: 'condition', column: 'region', operator: 'in', value: ['EMEA', 'AMER'] }, from: null });
    assert.deepEqual(ask({ class: TRADE, filter: ['qty', 'greaterThan', 5] }).command,
      { kind: 'setPageFilter', class: TRADE, condition: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 5 }, from: null });
    const done = JSON.parse(pageApply(text, JSON.stringify(idsOf(classPage)), JSON.stringify([command]))) as { page: string; commands: PageCommand[] };
    assert.deepEqual(readPage(done.page).filters?.map((f) => f.id), ['filter-1']);
  });

  it('refused in words: a class no grid reads, a property the class has not, an operator its type is not offered', () => {
    assert.match(ask({ class: 'Customer', filter: ['region', 'equal', 'EMEA'] }).problems![0]!,
      /no grid of the page reads a class "Customer" \(its grids read trades::domain::Trade\)/);
    assert.match(ask({ class: 'Trade', filter: ['colour', 'equal', 'red'] }).problems![0]!,
      /names "colour", not a column of the grid/);
    assert.match(ask({ class: 'Trade', filter: ['qty', 'startsWith', '1'] }).problems![0]!,
      /'startsWith' is not offered on qty/);
    // the filter as Python writes one (grid.ts readGivenFilter), never the editor's tree a host does not hold
    assert.match(ask({ class: 'Trade', filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', operand: 5 } }).problems![0]!,
      /a filter is a condition \(column, operator, value\)/);
    const frameText = pageToJson(frames);
    assert.match((JSON.parse(pageFilter(frameText, JSON.stringify({ class: 'Trade', filter: ['region', 'equal', 'EMEA'] }))) as { problems: string[] }).problems[0]!,
      /none reads a class/);
  });
});
