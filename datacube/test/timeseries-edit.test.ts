// A TIMESERIES TILE CHANGED (src/timeseries-edit.ts; the review's A2, docs/TIMESERIES_REVIEW_2026_10_25.md): every change
// its editors make and Python's Timeseries methods make, by one rule -- each held here, its refusals in its words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { editTimeseries, firstPlotOf, firstTimeseries, instrumentsOf, judgedEdit, TsEditRefusal, type TsGridColumns } from '../src/timeseries-edit.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import { readTimeseries, tsStateOf, type TimeseriesSpec, type TsExpr, type TsSeries } from '../src/timeseries-spec.ts';
import type { FilterNode } from '../src/snapshot.ts';
import { told, treeOf, writtenText } from '../src/timeseries-text.ts';

/** A tree as the tile told it, every parameter told: what core reads back of the text it is written as. */
const T = (formula: string): TsExpr => told(parseExpr(formula));
/** Series `i` of `spec`, as core reads its text. */
const treeAt = (spec: TimeseriesSpec, i: number): TsExpr => treeOf(spec.series[i]!, spec.series);
const S = (id: string, expr: string, rest: Omit<TsSeries, 'id' | 'text'> = {}): TsSeries => ({ id, text: writtenText(parseExpr(expr), []), ...rest });

const SPEC: TimeseriesSpec = {
  sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }, { alias: 'rates', cube: 'rates', date: 'obs', key: 'tenor' }],
  series: [S('A', 'close[SPX]'), S('B', 'returns($A)'), S('C', 'rates:yield[10Y]')],
};

const PRICES: TsGridColumns = {
  cube: 'prices2', title: 'More prices',
  columns: [{ name: 'symbol', type: 'String' }, { name: 'day', type: 'StrictDate' }, { name: 'close', type: 'Float' }], rows: ['symbol'],
  filter: undefined,
};

const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof TsEditRefusal && words.test(e.message));
};

describe('a timeseries tile changed by its one rule', () => {
  it('a series plotted: added with the next letter, or one it has replaced in place, kept hidden unless told', () => {
    const added = editTimeseries(SPEC, { kind: 'plot', series: { text: writtenText(parseExpr('mean($A, 5)'), []) } });
    assert.equal(added.series.at(-1)!.id, 'D');
    const hidden = editTimeseries(SPEC, { kind: 'hide', id: 'A', hidden: true });
    const edited = editTimeseries(hidden, { kind: 'plot', replace: true, series: { id: 'A', text: writtenText(parseExpr('close[NDX]'), []), name: 'Nasdaq' } });
    assert.deepEqual(edited.series[0], { id: 'A', text: writtenText(parseExpr('close[NDX]'), []), name: 'Nasdaq', hidden: true });
    refused(() => editTimeseries(SPEC, { kind: 'plot', replace: true, series: { id: 'Q', text: writtenText(parseExpr('close'), []) } }), /there is no series Q/);
  });

  it('a series another reads is not taken off; one no other reads is', () => {
    refused(() => editTimeseries(SPEC, { kind: 'unplot', id: 'A' }), /series B reads series A: take it off first/);
    assert.deepEqual(editTimeseries(SPEC, { kind: 'unplot', id: 'B' }).series.map((s) => s.id), ['A', 'C']);
    refused(() => editTimeseries(SPEC, { kind: 'unplot', id: 'Z' }), /there is no series Z/);
    // a view that read it reads the tile's first it can draw instead (the next phases' 3): its own of without it, the
    // view's other settings kept; a view of it alone, its of gone
    const viewing = { ...SPEC, view: { mode: 'scatter' as const, of: ['B', 'C'], bins: 20 } };
    assert.deepEqual(editTimeseries(viewing, { kind: 'unplot', id: 'B' }).view, { mode: 'scatter', of: ['C'], bins: 20 });
    assert.deepEqual(editTimeseries({ ...SPEC, view: { of: ['C'] } }, { kind: 'unplot', id: 'C' }).view, undefined);
    // a shade's name trimmed by the rule, the screen's and Python's alike; no words refused (the Phase 5 review's notes)
    assert.equal(editTimeseries(SPEC, { kind: 'shade', column: 'close', name: '  Recessions ' }).series.at(-1)!.name, 'Recessions');
    refused(() => editTimeseries(SPEC, { kind: 'shade', column: 'close', name: '  ' }), /a shade's name is no words/);
    // its projections taken off with it (Phase 5), another's kept
    const projected: TimeseriesSpec = { ...SPEC, annotations: [{ kind: 'projection', series: 'B' }, { kind: 'level', value: 1 }, { kind: 'projection', series: 'C' }] };
    assert.deepEqual(editTimeseries(projected, { kind: 'unplot', id: 'B' }).annotations, [{ kind: 'level', value: 1 }, { kind: 'projection', series: 'C' }]);
    assert.equal(editTimeseries({ ...SPEC, annotations: [{ kind: 'projection', series: 'B' }] }, { kind: 'unplot', id: 'B' }).annotations, undefined);
  });

  // the review's probe: the first source let go while a column of no alias reads it re-pointed that series at the next
  it('a source a series reads -- by its alias, or by a column of no alias -- is not let go; nor the only one', () => {
    refused(() => editTimeseries(SPEC, { kind: 'unread', alias: 'px' }), /series A, B read source px/);
    refused(() => editTimeseries(SPEC, { kind: 'unread', alias: 'rates' }), /series C reads source rates/);
    const one = editTimeseries({ ...SPEC, series: [S('A', 'close')] }, { kind: 'unread', alias: 'rates' });
    assert.deepEqual(one.sources.map((s) => s.alias), ['px']);
    refused(() => editTimeseries(one, { kind: 'unread', alias: 'px' }), /the only grid the tile reads/);
  });

  it('a source renamed: every expression naming it follows; a taken alias or a non-word refused', () => {
    const renamed = editTimeseries(SPEC, { kind: 'source', alias: 'rates', rename: 'curve' });
    assert.equal(renamed.sources[1]!.alias, 'curve');
    assert.deepEqual(treeAt(renamed, 2), T('curve:yield[10Y]'));
    assert.deepEqual(treeAt(renamed, 0), treeAt(SPEC, 0), 'a column of no alias is the first source\'s still');
    // the Phase 6 review's 4: marks from a source's rows follow it renamed (they were refused, naming the old alias)
    const marked: TimeseriesSpec = { ...SPEC, annotations: [{ kind: 'marks', source: 'rates', at: 'yield', label: 'Auctions' }, { kind: 'level', value: 0 }] };
    assert.deepEqual(editTimeseries(marked, { kind: 'source', alias: 'rates', rename: 'curve' }).annotations,
      [{ kind: 'marks', source: 'curve', at: 'yield', label: 'Auctions' }, { kind: 'level', value: 0 }]);
    refused(() => editTimeseries(SPEC, { kind: 'source', alias: 'rates', rename: 'px' }), /source px is the tile's already/);
    refused(() => editTimeseries(SPEC, { kind: 'source', alias: 'rates', rename: '2y' }), /is no alias/);
    const noKey = editTimeseries(SPEC, { kind: 'source', alias: 'px', key: null, date: 'when' });
    assert.deepEqual(noKey.sources[0], { alias: 'px', cube: 'prices', date: 'when' });
  });

  it('another grid read by DataCube\'s rule over its columns, called after its title; read twice, refused', () => {
    const more = editTimeseries(SPEC, { kind: 'read', grid: PRICES });
    assert.deepEqual(more.sources.at(-1), { alias: 'more', cube: 'prices2', date: 'day', key: 'symbol' });
    refused(() => editTimeseries(more, { kind: 'read', grid: PRICES }), /More prices is read already/);
    refused(() => editTimeseries(SPEC, { kind: 'read', grid: { ...PRICES, cube: 'x', columns: [{ name: 'close', type: 'Float' }] } }), /has no date column/);
  });

  it('the view, a pane, the frequency, the range, the annotations and the slots told whole', () => {
    const viewed = editTimeseries(SPEC, { kind: 'view', view: { compare: 'rebase', stats: true } });
    assert.deepEqual(editTimeseries(viewed, { kind: 'view', view: { compare: null } }).view, { stats: true });
    assert.equal(editTimeseries(editTimeseries(viewed, { kind: 'view', view: { compare: null, stats: null } }), { kind: 'grain', grain: null }).view, undefined);
    const logged = editTimeseries(SPEC, { kind: 'pane', index: 1, log: true, height: 2 });
    assert.deepEqual(logged.panes, [{}, { log: true, height: 2 }]);
    assert.deepEqual(editTimeseries(logged, { kind: 'pane', index: 1, log: false }).panes, [{}, { height: 2 }]);
    refused(() => editTimeseries(SPEC, { kind: 'pane', index: 9 }), /there is no pane 9/);
    assert.equal(editTimeseries(SPEC, { kind: 'grain', grain: 'week' }).grain, 'week');
    assert.deepEqual(editTimeseries(SPEC, { kind: 'range', range: { last: '1y' } }).range, { last: '1y' });
    const marked = editTimeseries(SPEC, { kind: 'annotate', annotation: { kind: 'level', value: 70, pane: 1 } });
    assert.equal(editTimeseries(marked, { kind: 'unannotate', index: 0 }).annotations, undefined);
    refused(() => editTimeseries(marked, { kind: 'unannotate', index: 3 }), /there is no annotation 4/);
    // a 0/1 column shaded in one step (Phase 5; FRED's recession bars): a condition series of it, named after it
    const shaded = editTimeseries(SPEC, { kind: 'shade', column: 'rec', source: 'rates', where: [{ value: '10Y' }] });
    assert.deepEqual(shaded.series.at(-1), { id: 'D', text: writtenText(parseExpr('rates:rec[10Y] != 0'), []), name: 'rec' });
    assert.deepEqual(editTimeseries(SPEC, { kind: 'shade', column: 'rec', name: 'Recessions' }).series.at(-1), { id: 'D', text: writtenText(parseExpr('rec != 0'), []), name: 'Recessions' });
    refused(() => editTimeseries(SPEC, { kind: 'shade', column: 'rec', source: 'fx' }), /reads no source fx/);
    const slots = editTimeseries({ ...SPEC, view: { stats: true } }, { kind: 'slots', slots: { view: { compare: 'percent' }, grain: 'month', range: null } });
    assert.deepEqual([slots.view, slots.grain, slots.range], [{ compare: 'percent' }, 'month', undefined]);
  });

  // the Phase 1 review's 9 (2026-10-26): the viewer told intersect as none, Python as 'intersect' -- two spellings of one
  // setting, and a query asked again for nothing
  it('intersect, the default, is one spelling whoever tells it: none', () => {
    assert.equal(editTimeseries(SPEC, { kind: 'align', align: 'intersect' }).align, undefined);
    assert.equal(editTimeseries({ ...SPEC, align: 'step' }, { kind: 'slots', slots: { align: 'intersect' } }).align, undefined);
    assert.equal(editTimeseries(SPEC, { kind: 'slots', slots: { align: 'step' } }).align, 'step');
  });

  // the next phases' 2 (2026-10-26): a pane upside down, its left axis' ends by hand
  it('a pane turned upside down, its ends set and taken off; ends the wrong way round refused by the reader', () => {
    const turned = editTimeseries(SPEC, { kind: 'pane', index: 0, invert: true, min: 0, max: 5 });
    assert.deepEqual(turned.panes, [{ invert: true, min: 0, max: 5 }]);
    assert.deepEqual(editTimeseries(turned, { kind: 'pane', index: 0, invert: false, min: null }).panes, [{ max: 5 }]);
    const none = (sp: TimeseriesSpec): Map<string, undefined> => new Map(sp.sources.map((x) => [x.alias, undefined]));
    refused(() => judgedEdit(SPEC, editTimeseries(SPEC, { kind: 'pane', index: 0, min: 5, max: 1 }), none), /pane 0's min 5 is not below its max 1/);
  });

  // the next phases' 2 (2026-10-26): Bloomberg's change of security, the viewer's Swap and Python's swap()
  it('one instrument for another: every series reading it, of its source alone when told, a name saying it following', () => {
    const spec: TimeseriesSpec = {
      ...SPEC,
      series: [S('A', 'close[SPX]', { name: 'SPX close' }), S('B', 'returns($A)'), S('C', 'close[SPX] - close[NDX]', { name: 'SPX spread' }),
        S('D', 'rates:yield[10Y]', { name: 'SPXW 10Y' })],
    };
    assert.deepEqual(instrumentsOf(spec), [{ alias: 'px', value: 'SPX' }, { alias: 'px', value: 'NDX' }, { alias: 'rates', value: '10Y' }]);
    const swapped = editTimeseries(spec, { kind: 'swap', from: 'SPX', to: 'RUT' });
    assert.deepEqual(swapped.series.map((_, i) => treeAt(swapped, i)),
      [T('close[RUT]'), T('returns($A)'), T('close[RUT] - close[NDX]'), T('rates:yield[10Y]')]);
    // a series that read it named again, the word alone; one that read it not kept as it was
    assert.deepEqual(swapped.series.map((x) => x.name), ['RUT close', undefined, 'RUT spread', 'SPXW 10Y']);
    assert.deepEqual(treeAt(editTimeseries(spec, { kind: 'swap', from: '10Y', to: '2Y', alias: 'rates' }), 3), T('rates:yield[2Y]'));
    // marks of the instrument's rows follow it too (the Phase 6 review: RUT's close with SPX's trades on it); a match on
    // another column, and another source's marks, kept
    const marked: TimeseriesSpec = { ...spec, annotations: [
      { kind: 'marks', source: 'px', where: [{ value: 'SPX' }, { column: 'side', value: 'SPX' }], at: 'close', label: 'SPX trades' },
      { kind: 'marks', source: 'rates', where: [{ value: 'SPX' }] }] };
    assert.deepEqual(editTimeseries(marked, { kind: 'swap', from: 'SPX', to: 'RUT', alias: 'px' }).annotations, [
      { kind: 'marks', source: 'px', where: [{ value: 'RUT' }, { column: 'side', value: 'SPX' }], at: 'close', label: 'RUT trades' },
      { kind: 'marks', source: 'rates', where: [{ value: 'SPX' }] }]);
    refused(() => editTimeseries(spec, { kind: 'swap', from: 'SPX', to: 'RUT', alias: 'rates' }), /no series reads SPX of rates/);
    refused(() => editTimeseries(spec, { kind: 'swap', from: 'DAX', to: 'RUT' }), /no series reads DAX/);
    refused(() => editTimeseries(spec, { kind: 'swap', from: 'SPX', to: ' ' }), /name it/);
    refused(() => editTimeseries(spec, { kind: 'swap', from: 'SPX', to: 'RUT', alias: 'fx' }), /reads no source fx/);
    // a match on another column is no instrument: kept as it was
    const venue = editTimeseries({ ...SPEC, series: [S('A', 'close[venue=SPX, SPX]')] }, { kind: 'swap', from: 'SPX', to: 'RUT' });
    assert.deepEqual(treeAt(venue, 0), T('close[venue=SPX, RUT]'));
  });

  // the Phase 2 review's 5: swap's edges
  it('a swap\'s edges: a name read through a reference, a $ in the new name, numbers, spaces, a word joined by &', () => {
    // a series reading it through $A plots the other too: its name follows
    const refs: TimeseriesSpec = { ...SPEC, series: [S('A', 'close[SPX]', { name: 'SPX' }), S('B', 'sma($A, 50)', { name: 'SPX 50d' }), S('C', 'close[NDX]', { name: 'SPX peer' })] };
    assert.deepEqual(editTimeseries(refs, { kind: 'swap', from: 'SPX', to: 'RUT' }).series.map((x) => x.name), ['RUT', 'RUT 50d', 'SPX peer']);
    // the new name as written, never read as a replacement's pattern
    assert.equal(editTimeseries(refs, { kind: 'swap', from: 'SPX', to: "A$'B" }).series[0]!.name, "A$'B");
    // an instrument column of numbers: a number for a number, refused for a word -- and a number told as one
    const numbered: TimeseriesSpec = { ...SPEC, series: [S('A', 'close[10]', { name: '10 close' })] };
    assert.deepEqual(treeAt(editTimeseries(numbered, { kind: 'swap', from: '10', to: '20' }), 0), T('close[20]'));
    refused(() => editTimeseries(numbered, { kind: 'swap', from: '10', to: 'RUT' }), /px's instruments are numbers: "RUT" is none/);
    refused(() => editTimeseries(numbered, { kind: 'swap', from: 10 as unknown as string, to: '20' }), /an instrument is swapped by its name, in words/);
    // spaces round a name are not of it
    assert.deepEqual(treeAt(editTimeseries(refs, { kind: 'swap', from: ' SPX ', to: ' RUT ' }), 0), T('close[RUT]'));
    // S in S&P 500 is no word of its own
    const sp: TimeseriesSpec = { ...SPEC, series: [S('A', 'close[S]', { name: 'S&P 500 close, S' })] };
    assert.equal(editTimeseries(sp, { kind: 'swap', from: 'S', to: 'T' }).series[0]!.name, 'S&P 500 close, T');
  });

  // the review's A3: Create ▸ Timeseries and Python's first tile, one rule and one refusal
  it('a first tile of a grid, and a first plot of a source told by its grid: one rule, refused in one set of words', () => {
    const made = firstTimeseries(PRICES);
    assert.ok(typeof made !== 'string');
    assert.deepEqual(made.series, [{ id: 'A', text: writtenText(parseExpr('close'), []), split: 'symbol' }]);
    const dates: TsGridColumns = { ...PRICES, columns: PRICES.columns.filter((c) => c.name !== 'close') };
    const words = firstTimeseries(dates);
    assert.equal(typeof words, 'string');
    assert.equal(firstPlotOf(dates, made.sources[0]!), words);
  });

  // a timeseries' states, a chart's words (the user, 2026-10-26): frozen, each source keeps its grid's filter as it is
  // now; dynamic, it follows them again
  it("frozen: each source keeps its grid's filter as it is now (none kept as none); dynamic again, they are let go", () => {
    const tech: FilterNode = { kind: 'condition', column: 'symbol', operator: 'equal', value: 'AAPL' };
    assert.equal(tsStateOf(SPEC), 'dynamic');
    const frozen = editTimeseries(SPEC, { kind: 'state', state: 'frozen', filters: new Map([['prices', tech], ['rates', undefined]]) });
    assert.equal(tsStateOf(frozen), 'frozen');
    assert.deepEqual(frozen.sources.map((s) => s.kept), [{ filter: tech }, {}]);
    assert.equal(typeof readTimeseries(JSON.parse(JSON.stringify(frozen))), 'object', 'its reader reads it back');
    // frozen again: as it is (the filters it keeps stay those it was frozen with)
    assert.equal(editTimeseries(frozen, { kind: 'state', state: 'frozen', filters: new Map([['prices', undefined], ['rates', undefined]]) }), frozen);
    // a grid read while frozen keeps its filter as it is now, as the rest do
    const more = editTimeseries(frozen, { kind: 'read', grid: { ...PRICES, filter: tech } });
    assert.deepEqual(more.sources.at(-1)!.kept, { filter: tech });
    assert.deepEqual(editTimeseries(more, { kind: 'state', state: 'dynamic', filters: new Map() }).sources.map((s) => s.kept), [undefined, undefined, undefined]);
    // a source whose grid is not open has no filter to keep: refused, not frozen on nothing
    refused(() => editTimeseries(SPEC, { kind: 'state', state: 'frozen', filters: new Map([['prices', tech]]) }), /source rates's grid is not open/);
  });

  // the second audit's A-S1, A-S3: one judge for both sides -- a change refused for a problem it makes, never for one
  // the tile had; a tile of no series one either may leave; a slot it has not refused by name
  it('a change judged alike on both sides: refused for a problem it makes, not one the tile had; no series allowed', () => {
    // each source's columns by the cube it reads, as the viewer and Python find them
    const byCube = new Map([['prices', PRICES.columns], ['rates', [{ name: 'tenor', type: 'String' }, { name: 'obs', type: 'StrictDate' }, { name: 'yield', type: 'Float' }]]]);
    const of = (sp: TimeseriesSpec): Map<string, typeof PRICES.columns | undefined> => new Map(sp.sources.map((x) => [x.alias, byCube.get(x.cube)]));
    const px: TimeseriesSpec = { ...SPEC, sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }, SPEC.sources[1]!], series: [S('A', 'close[SPX]')] };
    // a series of a column there is not: a problem it makes, refused
    refused(() => judgedEdit(px, editTimeseries(px, { kind: 'plot', series: { text: writtenText(parseExpr('nosuch'), []) } }), of), /reads 'nosuch', which source px has not/);
    // the tile with that problem already (its column since gone): another change is kept, the problem the tile's still
    const broken: TimeseriesSpec = { ...px, series: [...px.series, S('B', 'gone')] };
    const hidden = judgedEdit(broken, editTimeseries(broken, { kind: 'hide', id: 'A', hidden: true }), of);
    assert.equal(hidden.series[0]!.hidden, true);
    // the last series taken off: a tile of no series, on both sides
    assert.deepEqual(judgedEdit(px, editTimeseries(px, { kind: 'unplot', id: 'A' }), of).series, []);
    // a tile made new is judged whole
    refused(() => judgedEdit(null, broken, of), /reads 'gone'/);
    // a source renamed: the problem it had, said by its new name, is still the tile's, not one the rename made (the
    // third review's 4)
    const renamed = judgedEdit(broken, editTimeseries(broken, { kind: 'source', alias: 'px', rename: 'prices' }), of);
    assert.deepEqual(renamed.sources.map((x) => x.alias), ['prices', 'rates']);
    // a slot it has not, refused by name -- never dropped
    refused(() => editTimeseries(px, { kind: 'slots', slots: { grian: 'week' } as never }), /there is no "grian" among a tile's slots/);
  });

  it('read back frozen whole or not at all: a tile half keeping its filters is refused by name', () => {
    const half = { ...SPEC, sources: [{ ...SPEC.sources[0]!, kept: {} }, SPEC.sources[1]!] };
    assert.match(String(readTimeseries(JSON.parse(JSON.stringify(half)))), /a frozen timeseries keeps every source's filter/);
    const bad = { ...SPEC, sources: SPEC.sources.map((s) => ({ ...s, kept: { filter: { kind: 'nonsense' } } })) };
    assert.match(String(readTimeseries(JSON.parse(JSON.stringify(bad)))), /the filter source px keeps is not one/);
    const extra = { ...SPEC, sources: SPEC.sources.map((s) => ({ ...s, kept: { grouping: [] } })) };
    assert.match(String(readTimeseries(JSON.parse(JSON.stringify(extra)))), /grouping/);
  });
});
