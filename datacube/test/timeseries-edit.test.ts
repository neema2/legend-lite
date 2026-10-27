// A TIMESERIES TILE CHANGED (src/timeseries-edit.ts; the review's A2, docs/TIMESERIES_REVIEW_2026_10_25.md): every change
// its editors make and Python's Timeseries methods make, by one rule -- each held here, its refusals in its words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { editTimeseries, firstPlotOf, firstTimeseries, judgedEdit, TsEditRefusal, type TsGridColumns } from '../src/timeseries-edit.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import { readTimeseries, tsStateOf, type TimeseriesSpec, type TsSeries } from '../src/timeseries-spec.ts';
import type { FilterNode } from '../src/snapshot.ts';

const S = (id: string, expr: string, rest: Omit<TsSeries, 'id' | 'expr'> = {}): TsSeries => ({ id, expr: parseExpr(expr), ...rest });

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
    const added = editTimeseries(SPEC, { kind: 'plot', series: { expr: parseExpr('mean($A, 5)') } });
    assert.equal(added.series.at(-1)!.id, 'D');
    const hidden = editTimeseries(SPEC, { kind: 'hide', id: 'A', hidden: true });
    const edited = editTimeseries(hidden, { kind: 'plot', replace: true, series: { id: 'A', expr: parseExpr('close[NDX]'), name: 'Nasdaq' } });
    assert.deepEqual(edited.series[0], { id: 'A', expr: parseExpr('close[NDX]'), name: 'Nasdaq', hidden: true });
    refused(() => editTimeseries(SPEC, { kind: 'plot', replace: true, series: { id: 'Q', expr: parseExpr('close') } }), /there is no series Q/);
  });

  it('a series another reads is not taken off; one no other reads is', () => {
    refused(() => editTimeseries(SPEC, { kind: 'unplot', id: 'A' }), /series B reads \$A: take it off first/);
    assert.deepEqual(editTimeseries(SPEC, { kind: 'unplot', id: 'B' }).series.map((s) => s.id), ['A', 'C']);
    refused(() => editTimeseries(SPEC, { kind: 'unplot', id: 'Z' }), /there is no series Z/);
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
    assert.deepEqual(renamed.series[2]!.expr, parseExpr('curve:yield[10Y]'));
    assert.deepEqual(renamed.series[0]!.expr, SPEC.series[0]!.expr, 'a column of no alias is the first source\'s still');
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
    const slots = editTimeseries({ ...SPEC, view: { stats: true } }, { kind: 'slots', slots: { view: { compare: 'percent' }, grain: 'month', range: null } });
    assert.deepEqual([slots.view, slots.grain, slots.range], [{ compare: 'percent' }, 'month', undefined]);
  });

  // the review's A3: Create ▸ Timeseries and Python's first tile, one rule and one refusal
  it('a first tile of a grid, and a first plot of a source told by its grid: one rule, refused in one set of words', () => {
    const made = firstTimeseries(PRICES);
    assert.ok(typeof made !== 'string');
    assert.deepEqual(made.series, [{ id: 'A', expr: parseExpr('close'), split: 'symbol' }]);
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
    refused(() => judgedEdit(px, editTimeseries(px, { kind: 'plot', series: { expr: parseExpr('nosuch') } }), of), /reads 'nosuch', which source px has not/);
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
