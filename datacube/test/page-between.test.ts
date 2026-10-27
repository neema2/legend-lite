// THE COMMANDS BETWEEN TWO PAGES TAKE ONE TO THE OTHER (src/page-between.ts; docs/DATACUBE_LIVE_EDITING_2026_10_24.md §3,
// W2): over the saved-page corpus edited at random by every page command kind, for any two states of a walk, forward
// and back, the commands between them applied to the one make the other -- up to the ids given out afresh where
// `to`'s are held (an undo: the tile comes back as a new tile), and exactly where they are not (a page converging on
// its keeper's) -- and the same page gives none. The `history` scope leaves the name, the saved record and the model
// alone, as a step of the page's history does; `whole` carries them, and refuses in words what no command makes.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { applyPlanned, namedUnder, pageBetween } from '../src/page-between.ts';
import { applyPageCommands, idsOf, nextPageId, takenIds, type PageCommand, type PageDraft } from '../src/page-commands.ts';
import { gridCopyOf, pageToJson, readPage, type ChartView, type PageDocument, type TimeseriesView } from '../src/page-document.ts';
import { textTitle } from '../src/page-ids.ts';
import type { TimeseriesSpec } from '../src/timeseries-spec.ts';
import { CubeRefusal, type FilterNode } from '../src/snapshot.ts';
import { corpus } from './fixtures/pages/make.ts';
import { canonical, differing, renamedPage } from './page-same.ts';
import { layoutTiles } from '../src/page-document.ts';

const pages = await corpus();
const trades = pages['trades.page.json']!;
const joins = pages['prep-joins.page.json']!;
const dashboard = pages['charts-dashboard.page.json']!;

// 60 seeds (2026-10-24: 120 took two minutes, every pair of a walk's states both ways; LEGEND_LITE_BETWEEN_SEEDS for more)
const SEEDS = Number(process.env['LEGEND_LITE_BETWEEN_SEEDS'] ?? '60');
const STEPS = 8;
const EMEA: FilterNode = { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' };
const SNAP = gridCopyOf({ table: 'snap_1', rowCount: 3, takenAt: new Date('2026-10-24T09:00:00Z'), label: 'Snap 09:00' });

const draft = (page: PageDocument): PageDraft => ({ page, held: idsOf(page) });

/** A seeded random number generator (mulberry32): a failure says its seed, run alone again. */
function random(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A timeseries spec over `cubes` (each once), one series of the first. */
const seriesOver = (cubes: readonly string[]): TimeseriesSpec => ({
  sources: [...new Set(cubes)].map((cube, i) => ({ alias: `s${i}`, cube, date: 'tradeDate' })),
  series: [{ id: 'A', expr: { kind: 'column', column: 'notional' } }],
});

/** One edit of a page a person or Python could make, picked at random, and its name. */
function randomEdit(rand: () => number, d: PageDraft, n: number): [string, PageCommand] {
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!;
  const page = d.page;
  const grids = page.views.filter((v) => v.kind === 'grid');
  const charts = page.views.filter((v): v is ChartView => v.kind === 'chart');
  const sheets = page.sheets;
  // a page with no grid (every one taken off): a sheet added, the one edit every page takes
  if (grids.length === 0) return [`add sheet ${n}`, { kind: 'addSheet', name: `Tab ${n}` }];
  const series = page.views.filter((v): v is TimeseriesView => v.kind === 'timeseries');
  const addSeries = (): [string, PageCommand] => [`add timeseries ${n}`, {
    kind: 'addTimeseries', sheet: pick(sheets).id, near: null, spec: seriesOver([pick(grids).cube, pick(grids).cube]),
    title: rand() < 0.5 ? `Series ${n}` : null, columns: null,
  }];
  const choices: [string, () => [string, PageCommand]][] = [
    ['addSheet', () => [`add sheet ${n}`, { kind: 'addSheet', name: rand() < 0.5 ? `Tab ${n}` : null }]],
    ['renameSheet', () => [`rename sheet ${n}`, { kind: 'renameSheet', sheet: pick(sheets).id, name: rand() < 0.3 ? null : `Tab ${n} renamed` }]],
    ['moveSheet', () => [`move sheet ${n}`, { kind: 'moveSheet', sheet: pick(sheets).id, to: Math.floor(rand() * sheets.length) }]],
    ['removeSheet', () => [`remove sheet ${n}`, { kind: 'removeSheet', sheet: pick(sheets).id }]],
    ['renameTile', () => {
      const v = pick(page.views);
      return [`rename tile ${v.id}`, { kind: 'renameTile', tile: v.id, title: rand() < 0.3 && v.kind === 'grid' ? null : `Title ${n}` }];
    }],
    ['addGrid', () => [`add grid ${n}`, { kind: 'addGrid', document: pick(page.cubes).cube, sheet: pick(sheets).id, near: null,
      title: rand() < 0.5 ? `Grid ${n}` : null, columns: null }]],
    ['addChart', () => {
      const spec = pick(charts.length > 0 ? charts : trades.views.filter((v): v is ChartView => v.kind === 'chart')).spec;
      return [`add chart ${n}`, { kind: 'addChart', near: pick(grids).id, spec, title: rand() < 0.5 ? `Chart ${n}` : null, columns: null }];
    }],
    ['setChart', () => {
      if (charts.length === 0) return [`add chart ${n}`, { kind: 'addChart', near: pick(grids).id, spec: trades.views.find((v): v is ChartView => v.kind === 'chart')!.spec, title: null, columns: null }];
      const chart = pick(charts);
      const other = pick(charts).spec;
      const spec = rand() < 0.5 ? { ...other, frozen: !chart.spec.frozen } : other;
      return [`set chart ${chart.id}`, { kind: 'setChart', chart: chart.id, spec }];
    }],
    ['followGrid', () => (charts.length === 0 ? [`add sheet ${n}`, { kind: 'addSheet', name: `Tab ${n}` }]
      : [`follow ${n}`, { kind: 'followGrid', chart: pick(charts).id, grid: pick(grids).id }])],
    ['detach', () => {
      if (charts.length === 0) return [`add sheet ${n}`, { kind: 'addSheet', name: `Tab ${n}` }];
      const chart = pick(charts);
      // onto the cube it reads detached (its document changed), or a cube of its own under a new id, or -- refused -- a grid's
      const detached = grids.every((g) => g.cube !== chart.cube);
      const cube = detached && rand() < 0.4 ? chart.cube : rand() < 0.2 ? pick(grids).cube : nextPageId(takenIds(d), 'grid');
      // at times with a copy for the cube (a detached chart's cube snapped: no grid snaps for it)
      const copy = rand() < 0.4 ? { copy: { ...SNAP, table: `snap_d${n}` } } : {};
      return [`detach ${chart.id}`, { kind: 'detach', chart: chart.id, cube, document: pick(page.cubes).cube, ...copy }];
    }],
    ['select', () => (charts.length === 0 ? [`add sheet ${n}`, { kind: 'addSheet', name: `Tab ${n}` }]
      : [`select ${n}`, { kind: 'select', chart: pick(charts).id, conditions: rand() < 0.3 ? [] : [EMEA] }])],
    ['moveTile', () => [`move tile ${n}`, { kind: 'moveTile', tile: pick(page.views).id, sheet: pick(sheets).id, columns: null }]],
    ['stack', () => {
      const on = layoutTiles(pick(sheets).layout);
      return [`stack ${n}`, { kind: 'stack', tiles: on.length >= 2 ? on.slice(0, 2) : on }];
    }],
    ['removeTile', () => [`remove tile ${n}`, { kind: 'removeTile', tile: pick(page.views).id }]],
    // text tiles (P4 step 1): made, their words set; renamed, moved and removed as any tile above
    ['addText', () => [`add text ${n}`, { kind: 'addText', sheet: pick(sheets).id, text: `# Notes ${n}`, title: rand() < 0.5 ? `Text ${n}` : null, columns: null }]],
    ['setText', () => {
      const texts = page.views.filter((v) => v.kind === 'text');
      if (texts.length === 0) return [`add text ${n}`, { kind: 'addText', sheet: pick(sheets).id, text: `Words ${n}`, title: null, columns: null }];
      return [`set text ${n}`, { kind: 'setText', tile: pick(texts).id, text: `Words **${n}**` }];
    }],
    // timeseries tiles (the review's G4): made over a grid's cube or two, re-pointed at another, or at a cube kept for
    // them alone (at times snapped), their frequency set; renamed, moved and removed as any tile above -- a grid they
    // read removed keeps its cube for them
    ['addTimeseries', addSeries],
    ['setTimeseries', () => {
      if (series.length === 0) return addSeries();
      const tile = pick(series);
      if (rand() < 0.5) return [`set timeseries ${n}`, { kind: 'setTimeseries', tile: tile.id, spec: seriesOver([pick(grids).cube, ...tile.spec.sources.map((x) => x.cube)].slice(0, 2)) }];
      return [`set timeseries ${n}`, { kind: 'setTimeseries', tile: tile.id, spec: { ...tile.spec, grain: pick(['week', 'month'] as const) } }];
    }],
    // a cube kept for a tile alone, at times snapped -- its own choice, as detach is a chart's: a branch of another, the
    // walks never reached it (2026-10-25)
    ['keepForTimeseries', () => {
      if (series.length === 0) return addSeries();
      const tile = pick(series);
      const cube = nextPageId(takenIds(d), 'grid');
      const copy = rand() < 0.4 ? { ...SNAP, table: `snap_t${n}` } : null;
      return [`keep for timeseries ${n}`, { kind: 'setTimeseries', tile: tile.id, spec: seriesOver([cube, tile.spec.sources[0]!.cube]), kept: [{ cube, document: pick(page.cubes).cube, copy }] }];
    }],
    // its files (§8): none, or one or two pointed to by fingerprint
    ['setTextFiles', () => {
      const texts = page.views.filter((v) => v.kind === 'text');
      if (texts.length === 0) return [`add text ${n}`, { kind: 'addText', sheet: pick(sheets).id, text: `Words ${n}`, title: null, columns: null }];
      const files = Array.from({ length: Math.floor(rand() * 3) }, (_, i) => ({ name: `f${n}-${i}.md`, size: n + i, sha256: (n * 10 + i).toString(16).padStart(64, '0') }));
      return [`set text files ${n}`, { kind: 'setTextFiles', tile: pick(texts).id, files }];
    }],
    ['arrange', () => [`arrange ${n}`, { kind: 'arrange', sheet: pick(sheets).id, preset: pick(['side-by-side', 'stacked', 'focus-left', 'focus-top']), order: null }]],
    ['snap', () => [`snap ${n}`, { kind: 'snap', grid: pick(grids).id, copy: { ...SNAP, table: `snap_${n}` } }]],
    ['live', () => [`live ${n}`, { kind: 'live', grid: pick(grids).id }]],
    ['renamePage', () => [`rename page ${n}`, { kind: 'renamePage', name: `Page ${n}` }]],
    ['savedAs', () => [`saved as ${n}`, { kind: 'savedAs', record: rand() < 0.3 ? null : `record-${n}` }]],
    ['model', () => (page.model === undefined
      ? [`model into ${n}`, { kind: 'modelInto', project: 'datacube:pages', workspace: 'datacube', package: 'pages::p' }]
      : rand() < 0.3 ? [`model pin ${n}`, { kind: 'modelPin', revision: `r${n}` }]
        : [`model keep ${n}`, { kind: 'modelKeep', cube: pick(grids).cube, tables: [`t${n}`], class: rand() < 0.5 ? null : `pages::p::C${n}` }])],
  ];
  return pick(choices)[1]();
}

/** A walk of `steps` random edits from `seed` over `start`: the page after each, and the edits' names. */
function randomWalk(start: PageDocument, seed: number, steps: number): { states: PageDocument[]; names: string[] } {
  const rand = random(seed);
  let d = draft(start);
  const states = [start];
  const names: string[] = [];
  for (let i = 0; i < steps; i += 1) {
    const [name, edit] = randomEdit(rand, d, i + 1);
    try {
      d = applyPageCommands(d, [edit]).draft;
      names.push(name);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw new Error(`seed ${seed}, step ${i} (${name}): ${String(e)}`);
      names.push(`${name} (refused)`);
    }
    states.push(d.page);
  }
  return { states, names };
}

/** `page` with its ids renamed as `renamed` says (planned -> given): what a page made under fresh ids compares to. */
describe('the commands between two pages take one to the other', () => {
  for (const [title, start] of [['the frame page', trades], ['the join example', joins], ['the charts dashboard', dashboard]] as const) {
    it(`${title}: ${SEEDS} walks of ${STEPS} edits, every pair of states forward and back, in both scopes`, () => {
      let pairs = 0;
      let refusals = 0;
      let honoured = 0;
      let kept = 0;
      for (let seed = 1; seed <= SEEDS; seed += 1) {
        const { states, names } = randomWalk(start, seed, STEPS);
        const why = (i: number, j: number, scope: string): string => `seed ${seed} ${scope}, ${i} -> ${j}: ${names.slice(Math.min(i, j), Math.max(i, j)).join(' -> ')}`;
        for (let i = 0; i < states.length; i += 1) {
          for (const scope of ['history', 'whole'] as const) assert.deepEqual(pageBetween(states[i]!, states[i]!, scope), [], `seed ${seed}, state ${i}: the same page`);
          for (const j of [i + 1, i + 3, states.length - 1].filter((k) => k > i && k < states.length)) {
            for (const [a, b] of [[states[i]!, states[j]!], [states[j]!, states[i]!]] as [PageDocument, PageDocument][]) {
              for (const scope of ['history', 'whole'] as const) {
                let plan: PageCommand[];
                try {
                  plan = pageBetween(a, b, scope);
                } catch (e) {
                  // what no command makes, said in words: a model unmade, a detached chart made again
                  if (!(e instanceof CubeRefusal)) throw new Error(`${why(i, j, scope)}: ${String(e)}`);
                  refusals += 1;
                  continue;
                }
                pairs += 1;
                if (plan.some((c) => (c.kind === 'addTimeseries' || c.kind === 'setTimeseries') && c.kept !== undefined)) kept += 1;
                // fresh ids where `b`'s are held (an undo): the page up to those ids
                const planned = (fresh: boolean): ReturnType<typeof applyPlanned> => {
                  try {
                    return applyPlanned(draft(a), plan, fresh);
                  } catch (e) {
                    throw new Error(`${why(i, j, scope)} (${fresh ? 'fresh' : 'honoured'}: ${plan.map((c) => c.kind).join(', ')}): ${e instanceof Error ? e.message : String(e)}`);
                  }
                };
                const fresh = planned(true);
                assert.deepEqual(canonical(fresh.draft.page, scope), canonical(renamedPage(b, fresh.renamed), scope),
                  `${why(i, j, scope)} (fresh): ${plan.map((c) => c.kind).join(', ')}`);
                // `b`'s ids honoured where none the plan gives out is held here (a page converging on its keeper's): the
                // page exactly -- ids a keeper gives (`grid-N`: a corpus page's own names, `orders`, are never given out again)
                const taken = new Set(takenIds(draft(a)));
                const given = plan.flatMap((c) => (['addSheet', 'addGrid', 'addChart', 'addTimeseries', 'setPageFilter'].includes(c.kind) ? [(c as { id?: string }).id] : []))
                  .filter((id): id is string => id !== undefined);
                if (given.every((id) => !taken.has(id) && /^(?:grid|chart|timeseries|sheet|filter)-\d+$/.test(id))) {
                  honoured += 1;
                  const exact = planned(false);
                  assert.deepEqual(exact.renamed, {}, `${why(i, j, scope)}: no id given out afresh`);
                  assert.deepEqual(canonical(exact.draft.page, scope), canonical(b, scope), `${why(i, j, scope)} (honoured): ${plan.map((c) => c.kind).join(', ')}`);
                }
              }
            }
          }
        }
      }
      assert.ok(pairs > SEEDS * STEPS, `${pairs} pairs`);
      assert.ok(honoured > pairs / 2, `${honoured} of ${pairs} pairs with the ids honoured`);
      assert.ok(refusals < pairs / 10, `${refusals} refusals over ${pairs} pairs`);
      // a timeseries tile made again, or set, with the cube it kept (G4): the walks reach it
      assert.ok(kept > 0, `${kept} plans carrying a kept cube`);
    });
  }

  // the D19 session's check (2026-10-24, the middle version: every id a tab makes is under its tag, none renamed)
  it("a tab's text tile is made under its tag, read back as written, its words and title set by that id", () => {
    const d = draft(trades);
    const named = namedUnder(d, [{ kind: 'addText', sheet: trades.sheets[0]!.id, text: 'Hello', title: null, columns: null }], 'tabcde');
    assert.equal((named[0] as { id?: string }).id, 'text-tabcde-1');
    const made = applyPageCommands(d, named).draft;
    const text = (p: PageDocument) => p.views.find((v) => v.id === 'text-tabcde-1');
    assert.deepEqual(text(made.page), { id: 'text-tabcde-1', kind: 'text', text: 'Hello' });
    assert.equal(textTitle('text-tabcde-1'), 'Text 1');
    assert.deepEqual(text(readPage(JSON.parse(pageToJson(made.page)))), text(made.page));
    const set = applyPageCommands(made, [{ kind: 'setText', tile: 'text-tabcde-1', text: 'Bye' }, { kind: 'renameTile', tile: 'text-tabcde-1', title: 'Text 1' }]).draft;
    assert.deepEqual(text(set.page), { id: 'text-tabcde-1', kind: 'text', text: 'Bye' });
    assert.throws(() => applyPageCommands(d, [{ kind: 'addText', id: 'text-TAB-1', sheet: trades.sheets[0]!.id, text: 'x', title: null, columns: null }]),
      (e: unknown) => e instanceof CubeRefusal && /text-N or text-<tag>-N/.test(e.message));
  });

  it('an undo of a grid removal: the grid back as a new tile, its charts made of it or pointed at it, its selection back', () => {
    // the frame page: grid, grid-2, chart (live, of grid), chart-2 (frozen, of grid-2)
    const selected = applyPageCommands(draft(trades), [{ kind: 'select', chart: 'chart', conditions: [EMEA] }]).draft.page;
    const removed = applyPageCommands(draft(selected), [{ kind: 'removeTile', tile: 'grid' }]).draft.page;
    assert.ok(!removed.views.some((v) => v.id === 'chart'), 'the live chart went with its grid');
    const back = pageBetween(removed, selected, 'history');
    assert.deepEqual(back.map((c) => c.kind), ['addGrid', 'addChart', 'select', 'layout']);
    const undone = applyPlanned(draft(removed), back, true);
    // held ids are never given out again: the grid and the chart come back under fresh ids
    assert.deepEqual(undone.renamed, { grid: 'grid-3', chart: 'chart-3' });
    assert.deepEqual(canonical(undone.draft.page, 'history'), canonical(renamedPage(selected, undone.renamed), 'history'));
    // and a frozen chart left detached is pointed at the grid come back
    const frozenGone = applyPageCommands(draft(trades), [{ kind: 'removeTile', tile: 'grid-2' }]).draft.page;
    assert.ok(frozenGone.views.some((v) => v.id === 'chart-2') && frozenGone.cubes.some((c) => c.id === 'grid-2'), 'detached, its cube kept');
    const relinked = pageBetween(frozenGone, trades, 'history');
    assert.deepEqual(relinked.map((c) => c.kind).slice(0, 2), ['addGrid', 'followGrid']);
    const again = applyPlanned(draft(frozenGone), relinked, true);
    assert.deepEqual(canonical(again.draft.page, 'history'), canonical(renamedPage(trades, again.renamed), 'history'));
    assert.ok(!again.draft.page.cubes.some((c) => c.id === 'grid-2'), "the kept cube let go: the chart reads the grid's");
  });

  it('a detached chart comes back on the very cube it read (W19: detach), made of a grid here or of one made for it', () => {
    // its grid is not on the page to make it of: made of another grid, then put on its own kept cube, under the id it had
    const detached = applyPageCommands(draft(trades), [{ kind: 'removeTile', tile: 'grid-2' }]).draft.page;
    const gone = applyPageCommands(draft(detached), [{ kind: 'removeTile', tile: 'chart-2' }]).draft.page;
    const back = pageBetween(gone, detached, 'history');
    assert.deepEqual(back.map((c) => c.kind).slice(0, 2), ['addChart', 'detach']);
    assert.deepEqual(back[1], { kind: 'detach', chart: 'chart-2', cube: 'grid-2', document: detached.cubes.find((c) => c.id === 'grid-2')!.cube, copy: null });
    const made = applyPlanned(draft(gone), back, true);
    assert.deepEqual(canonical(made.draft.page, 'history'), canonical(renamedPage(detached, made.renamed), 'history'));
    assert.deepEqual(Object.keys(made.renamed), ['chart-2'], 'the chart is a new tile (B12); its cube is the one it read');
    // a page of no grid at all: a grid made for it, then taken off
    const alone = applyPageCommands(draft(detached), [{ kind: 'removeTile', tile: 'grid' }]).draft.page;
    const none = applyPageCommands(draft(alone), [{ kind: 'removeTile', tile: 'chart-2' }]).draft.page;
    assert.equal(none.views.length, 0);
    const again = pageBetween(none, alone, 'history');
    assert.deepEqual(again.map((c) => c.kind).slice(0, 4), ['addGrid', 'addChart', 'detach', 'removeTile']);
    const remade = applyPlanned(draft(none), again, true);
    assert.deepEqual(canonical(remade.draft.page, 'history'), canonical(renamedPage(alone, remade.renamed), 'history'));
    // and Show Grid on a detached chart undone: the chart on its kept cube again, the grid of its own gone
    const own = applyPageCommands(draft(detached), [
      { kind: 'addGrid', document: detached.cubes.find((c) => c.id === 'grid-2')!.cube, sheet: 'sheet-1', near: null, title: null, columns: null },
    ]).draft;
    const ownGrid = own.page.views.find((v) => !detached.views.some((x) => x.id === v.id))!.id;
    const shown = applyPageCommands(own, [{ kind: 'followGrid', chart: 'chart-2', grid: ownGrid }]).draft.page;
    const undone = pageBetween(shown, detached, 'history');
    assert.deepEqual(undone.map((c) => c.kind).slice(0, 2), ['detach', 'removeTile'], 'the chart on its cube first, then its grid off: the chart stays');
    const put = applyPlanned(draft(shown), undone, true);
    assert.deepEqual(put.renamed, {}, 'no tile made again: the chart kept its id');
    assert.deepEqual(canonical(put.draft.page, 'history'), canonical(detached, 'history'));
  });

  // the review's G4 (docs/TIMESERIES_REVIEW_2026_10_25.md): a grid removed while a timeseries reads it keeps its cube for
  // the tile, as a frozen chart's is kept; the tile then removed, an undo brings it back on that very cube -- its command
  // carries the cube, as detach carries a chart's
  it('a timeseries tile comes back on the very cube it kept (G4), its copy too; its grid brought back, it reads the grid', () => {
    const base = applyPageCommands(draft(trades), [
      { kind: 'removeTile', tile: 'chart-2' },
      { kind: 'snap', grid: 'grid-2', copy: SNAP },
      { kind: 'addTimeseries', id: 'timeseries-1', sheet: 'sheet-1', near: null, spec: seriesOver(['grid', 'grid-2']), title: null, columns: null },
    ]).draft;
    // each step on the page as the history has it: every id it held still held (an undo gives none of them out again)
    const keptDraft = applyPageCommands(base, [{ kind: 'removeTile', tile: 'grid-2' }]).draft;
    const kept = keptDraft.page;
    const document = kept.cubes.find((c) => c.id === 'grid-2')?.cube;
    assert.ok(document !== undefined && kept.copies?.['grid-2'] !== undefined, 'the grid gone, its cube and its copy kept for the tile');
    const goneDraft = applyPageCommands(keptDraft, [{ kind: 'removeTile', tile: 'timeseries-1' }]).draft;
    const gone = goneDraft.page;
    assert.ok(!gone.cubes.some((c) => c.id === 'grid-2'), 'let go with the tile');
    // the tile's removal undone: made again carrying the cube as the page held it, reading its copy
    const back = pageBetween(gone, kept, 'history');
    const add = back.find((c): c is Extract<PageCommand, { kind: 'addTimeseries' }> => c.kind === 'addTimeseries');
    assert.deepEqual(add?.kept, [{ cube: 'grid-2', document, copy: kept.copies!['grid-2'] }]);
    const made = applyPlanned(goneDraft, back, true);
    assert.deepEqual(canonical(made.draft.page, 'history'), canonical(renamedPage(kept, made.renamed), 'history'));
    assert.deepEqual(Object.keys(made.renamed), ['timeseries-1'], 'the tile is a new tile (B12); its cube is the one it read');
    assert.ok(made.draft.page.cubes.some((c) => c.id === 'grid-2'), 'under the id it had');
    // the grid's removal undone: the grid back as a new tile, the tile reading it, the cube kept for it let go
    const relinked = applyPlanned(draft(kept), pageBetween(kept, base.page, 'history'), true);
    assert.deepEqual(canonical(relinked.draft.page, 'history'), canonical(renamedPage(base.page, relinked.renamed), 'history'));
    assert.ok(!relinked.draft.page.cubes.some((c) => c.id === 'grid-2'), "the kept cube let go: the tile reads the grid's");
    // re-pointed off its kept cube (let go), and back: the set carries it
    const moved = applyPageCommands(draft(kept), [{ kind: 'setTimeseries', tile: 'timeseries-1', spec: seriesOver(['grid']) }]).draft.page;
    assert.ok(!moved.cubes.some((c) => c.id === 'grid-2'));
    const unmoved = pageBetween(moved, kept, 'history');
    assert.deepEqual(unmoved.map((c) => c.kind), ['setTimeseries']);
    const put = applyPlanned(draft(moved), unmoved, true);
    assert.deepEqual(put.renamed, {});
    assert.deepEqual(canonical(put.draft.page, 'history'), canonical(kept, 'history'));
    // a page converging on its keeper's, whose kept cube reads its live rows: the set carries the cube as the keeper has it
    const tile = kept.views.find((v): v is TimeseriesView => v.id === 'timeseries-1')!;
    const unsnapped = applyPageCommands(draft(kept), [{ kind: 'setTimeseries', tile: tile.id, spec: tile.spec, kept: [{ cube: 'grid-2', document, copy: null }] }]).draft.page;
    assert.equal(unsnapped.copies?.['grid-2'], undefined);
    const converged = pageBetween(kept, unsnapped, 'whole');
    assert.deepEqual(converged, [{ kind: 'setTimeseries', tile: 'timeseries-1', spec: tile.spec, kept: [{ cube: 'grid-2', document, copy: null }] }]);
    assert.deepEqual(canonical(applyPlanned(draft(kept), converged, false).draft.page, 'whole'), canonical(unsnapped, 'whole'));
  });

  it('what no command makes is refused in words', () => {
    // the model: never unmade, never moved, its pin never taken off
    const modelled = applyPageCommands(draft(trades), [{ kind: 'modelInto', project: 'datacube:p', workspace: 'datacube', package: 'p' }, { kind: 'modelPin', revision: 'r1' }]).draft.page;
    assert.throws(() => pageBetween(modelled, trades, 'whole'), (e: unknown) => e instanceof CubeRefusal && /cannot be unmade/.test(e.message));
    assert.deepEqual(pageBetween(modelled, trades, 'history'), [], 'the history leaves the model alone');
    assert.deepEqual(pageBetween(trades, modelled, 'whole').map((c) => c.kind), ['modelInto', 'modelPin']);
    // a cube opened again over its source as it is now
    const rebuilt = { ...trades, cubes: trades.cubes.map((c) => (c.id === 'grid' ? { id: c.id, cube: { ...c.cube, source: { ...c.cube.source, name: 'trades_v2' } } } : c)) };
    assert.throws(() => pageBetween(trades, rebuilt, 'whole'), (e: unknown) => e instanceof CubeRefusal && /opened again over its source/.test(e.message));
    assert.deepEqual(pageBetween(trades, rebuilt, 'history'), [], "the cubes' contents are the grids' own");
  });
});

describe("a detached chart over a snapped kept cube (W2; the D19 audit's limit, closed: detach carries the copy)", () => {
  const snapped = applyPageCommands(draft(trades), [{ kind: 'snap', grid: 'grid-2', copy: SNAP }]).draft.page;
  const detached = applyPageCommands(draft(snapped), [{ kind: 'removeTile', tile: 'grid-2' }]).draft.page;
  const chart = detached.views.find((v): v is ChartView => v.kind === 'chart' && v.cube === 'grid-2')!;

  it('removed and brought back, reads the copy again: the detach the diff plans carries it', () => {
    assert.ok(detached.copies?.['grid-2'], 'the kept cube keeps its copy when its grid goes');
    const gone = applyPageCommands(draft(detached), [{ kind: 'removeTile', tile: chart.id }]).draft.page;
    assert.equal(gone.copies?.['grid-2'], undefined, 'the copy goes with the last chart that read the cube');
    const back = pageBetween(gone, detached, 'history');
    const planned = back.find((c) => c.kind === 'detach');
    assert.deepEqual(planned && 'copy' in planned ? planned.copy : undefined, SNAP, 'the detach carries the copy the cube read');
    const made = applyPlanned(draft(gone), back, true);
    assert.deepEqual(differing(canonical(made.draft.page, 'history'), canonical(renamedPage(detached, made.renamed), 'history')), [], 'one page, the copy too');
  });

  it('snapped or live again where it has no grid: the chart put on its cube again with the copy as `to` has it', () => {
    // the copy let go (to: the cube live), then taken again (to: the cube snapped) -- detach both ways, no refusal
    const { copies: _snapped, ...live } = detached;
    const toLive = pageBetween(detached, live, 'history');
    assert.deepEqual(toLive.map((c) => c.kind), ['detach']);
    assert.deepEqual(canonical(applyPlanned(draft(detached), toLive, false).draft.page, 'history'), canonical(live, 'history'));
    const toSnapped = pageBetween(live, detached, 'history');
    assert.deepEqual(toSnapped.map((c) => c.kind), ['detach']);
    assert.deepEqual(canonical(applyPlanned(draft(live), toSnapped, false).draft.page, 'history'), canonical(detached, 'history'));
  });
});
