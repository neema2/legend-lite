// ONE WRITER'S EDITS MADE AGAIN ON TOP OF A PAGE THAT MOVED ON (src/page-rebase.ts;
// docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): each edit applied in order to the page as it is now, the other writer's
// changes kept; each editor's ids its own (page-ids.ts: under its tag), so nothing is renamed, and an id the page holds
// already refused (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version); one that no longer applies
// refusing them all; a grid's edit applied over the page as the edits before left it (snapped or not);
// and -- the guard -- two writers' random edits, one made again on top of the other's, keep what both said.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CubeDocument } from '../src/cube-document.ts';
import { applyPageCommands, idsOf, type PageCommand, type PageDraft } from '../src/page-commands.ts';
import { gridCopyOf, type ChartView, type PageDocument } from '../src/page-document.ts';
import { pageReads, rebaseEdits, renamedCommand, type GridApplier, type PageEdit } from '../src/page-rebase.ts';
import { namedUnder } from '../src/page-between.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { pageRebase } from '../src/headless/page.ts';
import { pageToJson } from '../src/page-document.ts';
import { toJson } from '../../pure-protocol/src/index.ts';
import { corpus } from './fixtures/pages/make.ts';
import { layoutTiles } from '../src/page-document.ts';

// the saved-page corpus's frame page: grids grid and grid-2, chart (following grid) and chart-2 (frozen, of grid-2);
// sheets sheet-1 and sheet-2
const trades = (await corpus())['trades.page.json']!;
const draft = (page: PageDocument = trades): PageDraft => ({ page, held: idsOf(page) });
const SNAP = gridCopyOf({ table: 'snap', rowCount: 3, takenAt: new Date('2026-10-20T14:02:00Z'), label: 'Snap 14:02' });

/** A grid's edit as the host applies one, here a group setting its rows -- and every call it was asked, with whether
 *  the page had the grid snapped then. */
function applier(): { apply: GridApplier; asked: { id: string; snapped: boolean }[] } {
  const asked: { id: string; snapped: boolean }[] = [];
  const apply: GridApplier = async (page, cube, id, commands) => {
    asked.push({ id, snapped: (page.copies ?? {})[id] !== undefined });
    let next: CubeDocument = cube;
    for (const c of commands) {
      if (c.kind !== 'group') throw new CubeRefusal(`this host applies groups only, not ${c.kind}`);
      next = { ...next, query: { ...next.query, rows: [...c.rows] } };
    }
    return next;
  };
  return { apply, asked };
}

/** The page `commands` make of `page`, and the commands as applied (their ids given out, under `tag` when one is given:
 *  a tab's, page-app.ts #named): one writer's edit. */
function made(page: PageDocument, commands: readonly PageCommand[], tag?: string): { page: PageDocument; edits: PageEdit[] } {
  const done = applyPageCommands(draft(page), namedUnder(draft(page), commands, tag));
  return { page: done.draft.page, edits: done.applied.map((c) => ({ page: c })) };
}

const sheetName = (page: PageDocument, id: string): string | undefined => page.sheets.find((s) => s.id === id)?.name;
const rows = (page: PageDocument, grid: string): readonly string[] => {
  const view = page.views.find((v) => v.id === grid)!;
  return page.cubes.find((c) => c.id === view.cube)!.cube.query.rows;
};

describe('edits made again on top of a page that moved on', () => {
  it('each applied in order, the other writer\'s change kept', async () => {
    const theirs = made(trades, [{ kind: 'renameSheet', sheet: 'sheet-1', name: 'Theirs' }]).page;
    const mine = made(trades, [{ kind: 'addSheet', name: 'Mine' }, { kind: 'renameTile', tile: 'grid', title: 'Mine too' }]);
    const { apply } = applier();
    const done = await rebaseEdits(draft(theirs), [...mine.edits, { grid: 'grid-2', command: { kind: 'group', rows: ['desk'] } }], apply);
    assert.equal(sheetName(done.page, 'sheet-1'), 'Theirs');
    assert.equal(sheetName(done.page, 'sheet-3'), 'Mine');
    assert.equal(done.page.views.find((v) => v.id === 'grid')!.title, 'Mine too');
    assert.deepEqual(rows(done.page, 'grid-2'), ['desk']);
    assert.deepEqual(done.edits.map((e) => ('page' in e ? e.page.kind : e.command.kind)), ['addSheet', 'renameTile', 'group']);
  });

  it("each editor's ids its own: a sheet both made, the writer's kept under its tag, every later edit naming it as given", async () => {
    const theirs = made(trades, [{ kind: 'addSheet', name: 'Theirs' }]).page;
    const mine = made(trades, [
      { kind: 'addSheet', name: 'Mine' },
      { kind: 'renameSheet', sheet: 'sheet-ta-3', name: 'Mine, renamed' },
      { kind: 'moveTile', tile: 'grid-2', sheet: 'sheet-ta-3', columns: null },
    ], 'ta');
    assert.equal((mine.edits[0] as { page: { id: string } }).page.id, 'sheet-ta-3', 'given sheet-ta-3 on its side, under its tag');
    const done = await rebaseEdits(draft(theirs), mine.edits, applier().apply);
    assert.deepEqual(done.edits, mine.edits, 'the edits applied as given: nothing renamed');
    assert.equal(sheetName(done.page, 'sheet-3'), 'Theirs', 'theirs untouched');
    assert.equal(sheetName(done.page, 'sheet-ta-3'), 'Mine, renamed');
    assert.ok(layoutTiles(done.page.sheets.find((s) => s.id === 'sheet-ta-3')!.layout).includes('grid-2'), 'moved onto its own sheet');
  });

  it('an id the page holds already, made again: refused in words, never given out afresh', async () => {
    // two editors with no tag (the page's own keeper is the one: Python) never write at once; this is the rule's floor
    const theirs = made(trades, [{ kind: 'addSheet', name: 'Theirs' }]).page;
    const mine = made(trades, [{ kind: 'addSheet', name: 'Mine' }]);
    await assert.rejects(rebaseEdits(draft(theirs), mine.edits, applier().apply), /'sheet-3' is an id the page has held already/);
  });

  it("a grid made on both sides: each under its editor's id, the writer's grid edit, chart and layout on its own", async () => {
    const doc = trades.cubes[0]!.cube;
    const add = (title: string): PageCommand => ({ kind: 'addGrid', document: doc, sheet: 'sheet-2', near: null, title, columns: null });
    const theirs = made(trades, [add('Theirs')]).page;
    const mineAt = made(trades, [add('Mine')], 'ta');
    const spec = (trades.views.find((v) => v.id === 'chart') as ChartView).spec;
    const charting: PageCommand[] = [{ kind: 'addChart', near: 'grid-ta-3', spec, title: null, columns: null }];
    const withChart = applyPageCommands(draft(mineAt.page), namedUnder(draft(mineAt.page), charting, 'ta'));
    const layout = withChart.draft.page.sheets.find((s) => s.id === 'sheet-2')!.layout;
    const edits: PageEdit[] = [
      ...mineAt.edits, { grid: 'grid-ta-3', command: { kind: 'group', rows: ['region'] } },
      ...withChart.applied.map((c) => ({ page: c })), { page: { kind: 'layout', sheet: 'sheet-2', layout } },
    ];
    const { apply, asked } = applier();
    const done = await rebaseEdits(draft(theirs), edits, apply);
    assert.deepEqual(asked, [{ id: 'grid-ta-3', snapped: false }], "the grid edit applied to the writer's grid, under its own id");
    assert.deepEqual(rows(done.page, 'grid-ta-3'), ['region']);
    assert.equal(done.page.views.find((v) => v.id === 'grid-3')!.title, 'Theirs');
    const chart = done.page.views.find((v) => v.kind === 'chart' && v.id === 'chart-ta-3') as ChartView;
    assert.equal(chart.cube, 'grid-ta-3', "the chart is of the writer's grid");
    const placed = layoutTiles(done.page.sheets.find((s) => s.id === 'sheet-2')!.layout);
    assert.ok(placed.includes('grid-3') && placed.includes('grid-ta-3'), `both grids placed: ${placed.join(', ')}`);
  });

  it("a grid's run of consecutive commands is applied in one call, each listed as applied, refused whole (W14)", async () => {
    const { apply, asked } = applier();
    const run: PageEdit[] = [
      { grid: 'grid', command: { kind: 'group', rows: ['desk'] } },
      { grid: 'grid', command: { kind: 'group', rows: ['region'] } },
      { page: { kind: 'renamePage', name: 'Between' } },
      { grid: 'grid', command: { kind: 'group', rows: ['desk', 'region'] } },
      { grid: 'grid-2', command: { kind: 'group', rows: ['desk'] } },
    ];
    const done = await rebaseEdits(draft(trades), run, apply);
    assert.deepEqual(asked.map((a) => a.id), ['grid', 'grid', 'grid-2'], 'one call per run: the two groups of grid together, then the one after the page command, then grid-2');
    assert.deepEqual(done.edits, run, 'each edit listed as applied, in order');
    assert.deepEqual([rows(done.page, 'grid'), rows(done.page, 'grid-2')], [['desk', 'region'], ['desk']]);
    // one command of a run refused refuses them all: nothing of the run applied
    const refusing: GridApplier = async (page, cube, id, commands) => {
      if (commands.some((c) => c.kind === 'sorts')) throw new CubeRefusal('no sorts here');
      return apply(page, cube, id, commands);
    };
    await assert.rejects(rebaseEdits(draft(trades), [
      { grid: 'grid', command: { kind: 'group', rows: ['desk'] } },
      { grid: 'grid', command: { kind: 'sorts', sorts: [] } },
    ], refusing), /no sorts here/);
  });

  it('one that no longer applies refuses them all, in DataCube\'s words', async () => {
    const theirs = made(trades, [{ kind: 'removeTile', tile: 'chart' }]).page;
    await assert.rejects(rebaseEdits(draft(theirs), [
      { page: { kind: 'renamePage', name: 'Mine' } }, { page: { kind: 'renameTile', tile: 'chart', title: 'Gone there' } },
    ], applier().apply), CubeRefusal);
    const noGrid = made(trades, [{ kind: 'removeTile', tile: 'grid-2' }]).page;
    await assert.rejects(rebaseEdits(draft(noGrid), [{ grid: 'grid-2', command: { kind: 'group', rows: ['desk'] } }], applier().apply),
      /grid-2 is no longer on the page/);
  });

  it("a page filter both writers made: each under its editor's id, never put in place of the other's (H3)", async () => {
    // the corpus's page, its grids' cubes over a class (a page filter's class is one a grid reads)
    const over = (c: PageDocument['cubes'][number]) => ({ ...c, cube: { ...c.cube, source: { _type: 'class' as const, name: 'Trade',
      class: 'trades::domain::Trade', project: 'datacube:trades', workspace: 'datacube', revision: '3', mapping: 'trades::mapping::Model',
      runtime: 'trades::runtime::Browser', columns: (c.cube.source as { readonly columns: readonly { readonly name: string; readonly type: string }[] }).columns } } });
    const classPage: PageDocument = { ...trades, cubes: trades.cubes.map(over) };
    const filter = (value: string): PageCommand => ({ kind: 'setPageFilter', class: 'trades::domain::Trade',
      condition: { kind: 'condition', column: 'region', operator: 'equal', value }, from: null });
    // each writer made its filter at the same version: theirs filter-1, mine under its tag
    const theirs = applyPageCommands(draft(classPage), [filter('AMER')]).draft;
    const mine = made(classPage, [filter('EMEA'), { kind: 'removePageFilter', filter: 'filter-ta-1' }, filter('APAC')], 'ta');
    assert.equal((mine.edits[0] as { page: { id: string } }).page.id, 'filter-ta-1');
    const done = await rebaseEdits(theirs, mine.edits, applier().apply);
    const values = (done.page.filters ?? []).map((f) => [f.id, (f.condition as { value: string }).value]);
    assert.deepEqual(values, [['filter-1', 'AMER'], ['filter-ta-2', 'APAC']], 'theirs kept; mine made, taken off, made again');
    // a filter the page holds, set by its id: set where it is, as the command says
    const known = await rebaseEdits(theirs, [{ page: { ...filter('LATAM'), id: 'filter-1' } as PageCommand }], applier().apply);
    assert.deepEqual((known.page.filters ?? []).map((f) => (f.condition as { value: string }).value), ['LATAM']);
  });

  it('a page filter no command made under its keeper (a page loaded or served whole), set again: set where it is (the S4 audit\'s 3)', async () => {
    const over = (c: PageDocument['cubes'][number]) => ({ ...c, cube: { ...c.cube, source: { _type: 'class' as const, name: 'Trade',
      class: 'trades::domain::Trade', project: 'datacube:trades', workspace: 'datacube', revision: '3', mapping: 'trades::mapping::Model',
      runtime: 'trades::runtime::Browser', columns: (c.cube.source as { readonly columns: readonly { readonly name: string; readonly type: string }[] }).columns } } });
    const filter = (value: string, id?: string): PageCommand => ({ kind: 'setPageFilter', ...(id ? { id } : {}),
      class: 'trades::domain::Trade', condition: { kind: 'condition', column: 'region', operator: 'equal', value }, from: null });
    // the page as its keeper took it whole: its filter in the document, the ids it held what the page reads says
    const loaded = applyPageCommands(draft({ ...trades, cubes: trades.cubes.map(over) }), [filter('EMEA')]).draft.page;
    const known = pageReads(loaded).ids;
    // the tab sets that filter again (a chart click on another mark), at the keeper's own version
    const done = await rebaseEdits({ page: loaded, held: known }, [{ page: filter('AMER', 'filter-1') }], applier().apply);
    assert.deepEqual((done.page.filters ?? []).map((f) => [f.id, (f.condition as { value: string }).value]), [['filter-1', 'AMER']]);
  });

  it('a grid\'s edit is applied over the page as the edits before it left it: snapped, or live again', async () => {
    const { apply, asked } = applier();
    await rebaseEdits(draft(), [
      { page: { kind: 'snap', grid: 'grid-2', copy: SNAP } }, { grid: 'grid-2', command: { kind: 'group', rows: ['desk'] } },
      { page: { kind: 'live', grid: 'grid-2' } }, { grid: 'grid-2', command: { kind: 'group', rows: ['region'] } },
    ], apply);
    assert.deepEqual(asked, [{ id: 'grid-2', snapped: true }, { id: 'grid-2', snapped: false }]);
  });

  it('every command that names an id names the renamed one', () => {
    // every text a command holds outside its chart's spec and its copy (which hold no tile's id)
    const names = (o: unknown): string[] => (typeof o === 'string' ? [o] : Array.isArray(o) ? o.flatMap(names)
      : typeof o === 'object' && o !== null
        ? Object.entries(o).filter(([k]) => k !== 'spec' && k !== 'copy' && k !== 'kind').flatMap(([, v]) => names(v)) : []);
    const r = new Map([['grid', 'grid-9'], ['sheet-1', 'sheet-9'], ['chart', 'chart-9']]);
    const spec = (trades.views.find((v) => v.id === 'chart') as ChartView).spec;
    const named: [PageCommand, unknown][] = [
      [{ kind: 'renameSheet', sheet: 'sheet-1', name: 'x' }, 'sheet-9'],
      [{ kind: 'moveSheet', sheet: 'sheet-1', to: 0 }, 'sheet-9'],
      [{ kind: 'removeSheet', sheet: 'sheet-1' }, 'sheet-9'],
      [{ kind: 'renameTile', tile: 'grid', title: null }, 'grid-9'],
      [{ kind: 'removeTile', tile: 'chart' }, 'chart-9'],
      [{ kind: 'addChart', near: 'grid', spec, title: null, columns: null }, 'grid-9'],
      [{ kind: 'setChart', chart: 'chart', spec }, 'chart-9'],
      [{ kind: 'moveTile', tile: 'grid', sheet: 'sheet-1', columns: null }, ['grid-9', 'sheet-9']],
      [{ kind: 'stack', tiles: ['grid', 'chart'] }, ['grid-9', 'chart-9']],
      [{ kind: 'arrange', sheet: 'sheet-1', preset: 'rows:1-1', order: ['chart', 'grid'] }, ['sheet-9', 'chart-9', 'grid-9']],
      [{ kind: 'snap', grid: 'grid', copy: SNAP }, 'grid-9'],
      [{ kind: 'live', grid: 'grid' }, 'grid-9'],
    ];
    for (const [command, expect] of named) {
      const text = JSON.stringify(renamedCommand(command, r));
      for (const id of [expect].flat()) assert.ok(text.includes(`"${id as string}"`), `${command.kind}: ${text}`);
      assert.deepEqual(names(renamedCommand(command, r)).filter((v) => r.has(v)), [], `${command.kind} names no old id: ${text}`);
    }
    const layout = trades.sheets.find((s) => s.id === 'sheet-1')!.layout;
    const renamedLayout = renamedCommand({ kind: 'layout', sheet: 'sheet-1', layout }, r) as { layout: typeof layout };
    assert.deepEqual(layoutTiles(renamedLayout.layout).sort(), layoutTiles(layout).map((t) => r.get(t) ?? t).sort());
  });
});

describe("what a page on its host holds (headless pageRebase: the S4 audit's 2)", () => {
  it('a grid over a source the host cannot base its edits on is refused as the write adds it, in DataCube\'s words', async () => {
    // the trades page over the host's frames; the dashboard page's grids read a file
    const files = (await corpus())['charts-dashboard.page.json']!;
    const table = { model: '', runtime: '', accessor: '' };
    const frames = Object.fromEntries(pageReads(trades).frames.map((f) => [f, table]));
    const host = { frames, files: {}, remotes: {} };
    const ids = JSON.stringify(pageReads(trades).ids);
    const adding = (document: PageDocument['cubes'][number]['cube']): string =>
      `[${toJson({ page: { kind: 'addGrid', document, sheet: 'sheet-1', near: null, title: null, columns: null } })}]`;
    const file = files.cubes[0]!.cube;
    assert.equal(file.source._type, 'file');
    const refused = JSON.parse(await pageRebase(pageToJson(trades), ids, adding(file), host, {})) as { problems?: string[] };
    assert.match((refused.problems ?? []).join('; '), /the file .* is not read here: the page is given its file as it opens/);
    // one over a frame the host serves is kept, and one over a file it read in (by the file's bytes' hash: X1, X3)
    const kept = JSON.parse(await pageRebase(pageToJson(trades), ids, adding(trades.cubes[0]!.cube), host, {})) as { problems?: string[] };
    assert.equal(kept.problems, undefined);
    if (file.source._type !== 'file') throw new Error('unreachable');
    const leaf = { _type: 'file' as const, name: file.source.name, size: file.source.size, sha256: file.source.sha256 };
    const read = { ...host, files: { [file.source.sha256]: { model: '', runtime: '', source: {}, table: 'trades', leaf } } };
    const fileKept = JSON.parse(await pageRebase(pageToJson(trades), ids, adding(file), read, {})) as { problems?: string[] };
    assert.equal(fileKept.problems, undefined);
  });
});

describe('what a page reads, for its keeper', () => {
  it('its frames, its copies and its ids', () => {
    const snapped = made(trades, [{ kind: 'snap', grid: 'grid-2', copy: SNAP }]).page;
    const reads = pageReads(snapped);
    assert.deepEqual(reads.copies, ['snap']);
    assert.deepEqual(reads.ids.sort(), idsOf(snapped).sort(), 'no page filter here: its tiles\', cubes\' and sheets\'');
    assert.ok(reads.frames.length > 0 && reads.frames.every((f) => typeof f === 'string'));
    assert.deepEqual([reads.files, reads.remotes], [[], []]);
  });

  it('the files it reads by their bytes\' hash, the remote files by URL (X3: what its host holds a table for)', async () => {
    const dashboard = (await corpus())['charts-dashboard.page.json']!;
    const reads = pageReads(dashboard);
    const hashes = [...new Set(dashboard.cubes.flatMap((c) => (c.cube.source._type === 'file' ? [c.cube.source.sha256] : [])))].sort();
    assert.ok(hashes.length > 0);
    assert.deepEqual(reads.files, hashes);
    assert.deepEqual(reads.frames, []);
  });
});

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

describe('two writers\' edits, one made again on top of the other\'s, keep what both said', () => {
  it('in any mix of names, sheets, grids, groups and titles', async () => {
    const SEEDS = 300;
    for (let seed = 0; seed < SEEDS; seed++) {
      const rnd = random(seed);
      const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rnd() * xs.length)]!;
      /** One writer's edits at `base`, as it made them (its ids under `tag`: a tab's), and what each said (later sayings
       *  win). */
      const write = (base: PageDocument, who: string, tag?: string) => {
        let d = draft(base);
        const edits: PageEdit[] = [];
        const said = { name: undefined as string | undefined, sheets: [] as string[], titles: new Map<string, string>(),
          rows: new Map<string, string[]>(), renamedSheets: new Map<string, string>() };
        for (let n = 1 + Math.floor(rnd() * 4); n > 0; n--) {
          const grids = d.page.views.filter((v) => v.kind === 'grid').map((v) => v.id);
          const kind = pick(['renamePage', 'addSheet', 'renameSheet', 'addGrid', 'group', 'renameTile'] as const);
          if (kind === 'group') {
            const grid = pick(grids);
            const r = [pick(['desk', 'region'])];
            const view = d.page.views.find((v) => v.id === grid)!;
            const cube = d.page.cubes.find((c) => c.id === view.cube)!.cube;
            d = { ...d, page: { ...d.page, cubes: d.page.cubes.map((c) => (c.id === view.cube ? { id: c.id, cube: { ...cube, query: { ...cube.query, rows: r } } } : c)) } };
            edits.push({ grid, command: { kind: 'group', rows: r } });
            said.rows.set(grid, r);
            continue;
          }
          const command: PageCommand = kind === 'renamePage' ? { kind, name: `${who} ${seed}-${n}` }
            : kind === 'addSheet' ? { kind, name: `${who} sheet ${seed}-${n}` }
              : kind === 'renameSheet' ? { kind, sheet: pick(base.sheets.map((s) => s.id)), name: `${who} renamed ${seed}-${n}` }
                : kind === 'addGrid' ? { kind, document: trades.cubes[0]!.cube, sheet: 'sheet-1', near: null, title: `${who} grid ${n}`, columns: null }
                  : { kind, tile: pick(grids), title: `${who} title ${seed}-${n}` };
          const one = applyPageCommands(d, namedUnder(d, [command], tag));
          d = one.draft;
          edits.push({ page: one.applied[0]! });
          if (command.kind === 'renamePage') said.name = command.name;
          if (command.kind === 'addSheet') said.sheets.push(command.name!);
          if (command.kind === 'renameSheet') said.renamedSheets.set(command.sheet, command.name!);
          if (command.kind === 'renameTile') said.titles.set(command.tile, command.title!);
        }
        return { page: d.page, edits, said };
      };
      const a = write(trades, 'A');
      const b = write(trades, 'B', 'tb');
      const where = `seed ${seed}`;
      const done = await rebaseEdits(draft(a.page), b.edits, applier().apply);
      const p = done.page;
      // what B said wins where both said it (B's edits came last); what only A said stands
      assert.equal(p.name, b.said.name ?? a.said.name ?? trades.name, `${where}: the page's name`);
      const names = p.sheets.map((s) => s.name);
      for (const s of [...a.said.sheets, ...b.said.sheets]) assert.ok(names.includes(s), `${where}: sheet ${s}`);
      for (const [id, name] of a.said.renamedSheets) if (!b.said.renamedSheets.has(id)) assert.equal(sheetName(p, id), name, `${where}: ${id}`);
      for (const [id, name] of b.said.renamedSheets) assert.equal(sheetName(p, id), name, `${where}: ${id}`);
      for (const [grid, r] of a.said.rows) if (!b.said.rows.has(grid)) assert.deepEqual(rows(p, grid), r, `${where}: ${grid} grouped`);
      for (const [grid, r] of b.said.rows) assert.deepEqual(rows(p, grid), r, `${where}: ${grid} grouped`);
      for (const [tile, t] of a.said.titles) if (!b.said.titles.has(tile)) assert.equal(p.views.find((v) => v.id === tile)!.title, t, `${where}: ${tile}`);
      for (const [tile, t] of b.said.titles) assert.equal(p.views.find((v) => v.id === tile)!.title, t, `${where}: ${tile}`);
      // every grid either writer made is on the page, each once, each placed on a sheet, its id held
      const grids = (page: PageDocument): number => page.views.filter((v) => v.kind === 'grid').length;
      const wanted = [a, b].flatMap((w) => w.edits).filter((e) => 'page' in e && e.page.kind === 'addGrid').length;
      assert.equal(grids(p), grids(trades) + wanted, `${where}: every grid made`);
      const placed = p.sheets.flatMap((s) => layoutTiles(s.layout));
      assert.deepEqual([...placed].sort(), p.views.map((v) => v.id).sort(), `${where}: every tile placed once`);
      assert.equal(new Set(p.views.map((v) => v.id)).size, p.views.length, `${where}: no id twice`);
      for (const id of idsOf(p)) assert.ok(done.held.includes(id), `${where}: ${id} held`);
    }
  });
});
