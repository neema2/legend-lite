// THE GALLERY IN #TS (docs/TS_LANGUAGE_BUILD_2026_10_30.md P5): every series of every timeseries tile of the gallery
// and the markets example, planned and answered two ways on DuckDB-WASM over the pages' own samples -- the tile's writer
// (timeseries-query.ts seriesQuery) and the #TS{...}# query timeseries-ts.ts writes from the same spec -- each date's
// (and each line's) values the same. A series that answers otherwise is listed with why (DIFFERS), and stays listed
// only while it does: one that comes to agree fails here until its row goes.

import assert from 'node:assert/strict';
import { it } from 'node:test';
import { openDatabase } from '#test-host';
import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { TAB_TABLES, openSources } from '../src/join-open.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import {
  seriesQuery, seriesSources, TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_SPLIT, TS_UPPER, TS_VALUE, TS_WIDTH,
  type TsInput, type TsInputs,
} from '../src/timeseries-query.ts';
import type { TimeseriesSpec, TsSeries, TsView } from '../src/timeseries-spec.ts';
import { TS_ITEM, tsQueryText, tsSeriesQuery } from '../src/timeseries-ts.ts';
import { catalogColumns, ingestFile } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';

const writer = plannerFor('', 'local::RT');

/** The series that answer otherwise, by `<view> <series>`, and why. */
const DIFFERS: Readonly<Record<string, string>> = {};

/** The columns compared: the tile's, and the #TS answer's of the same figure. */
function fields(band: boolean, candles: boolean): readonly (readonly [string, string])[] {
  if (band) return [[TS_LOWER, `${TS_ITEM}_lower`], [TS_UPPER, `${TS_ITEM}_upper`], [TS_WIDTH, `${TS_ITEM}_width`]];
  if (candles) return [[TS_OPEN, `${TS_ITEM}_open`], [TS_HIGH, `${TS_ITEM}_high`], [TS_LOW, `${TS_ITEM}_low`], [TS_VALUE, `${TS_ITEM}_close`]];
  return [[TS_VALUE, TS_ITEM]];
}

function column(t: ResultTable, name: string): unknown[] {
  const c = t.columns.find((x) => x.name === name);
  if (!c) throw new Error(`no column ${name} (${t.columns.map((x) => x.name).join(', ')})`);
  return c.values.map((v) => (typeof v === 'bigint' ? Number(v) : v));
}

/** Each (date, line)'s values of `names`, where it has a finite one. */
function keyed(t: ResultTable, date: string, split: string | undefined, names: readonly string[]): Map<string, (number | null)[]> {
  const dates = column(t, date);
  const lines = split === undefined ? undefined : column(t, split);
  const cols = names.map((n) => column(t, n));
  const out = new Map<string, (number | null)[]>();
  for (let i = 0; i < t.rowCount; i++) {
    const vs = cols.map((c) => (c[i] === null || c[i] === undefined || !Number.isFinite(Number(c[i])) ? null : Number(c[i])));
    if (vs.every((v) => v === null)) continue;
    out.set(`${String(dates[i])}|${lines === undefined ? '' : String(lines[i])}`, vs);
  }
  return out;
}

/** Where the two answers differ, said (none: undefined). */
function difference(tile: Map<string, (number | null)[]>, ts: Map<string, (number | null)[]>): string | undefined {
  const keys = new Set([...tile.keys(), ...ts.keys()]);
  let differ = 0;
  let first: string | undefined;
  for (const k of keys) {
    const a = tile.get(k);
    const b = ts.get(k);
    const same = a !== undefined && b !== undefined && a.every((v, i) => {
      const w = b[i];
      return v === null ? w === null : w !== null && w !== undefined && Math.abs(v - w) <= 1e-7 * Math.max(1, Math.abs(v));
    });
    if (!same) {
      differ += 1;
      first ??= `${k}: tile ${JSON.stringify(a ?? null)}, #TS ${JSON.stringify(b ?? null)}`;
    }
  }
  return differ === 0 ? undefined : `${differ} of ${keys.size} points differ (${tile.size} the tile's, ${ts.size} #TS's), first ${first}`;
}

it('every gallery series answers the same written in #TS as the tile\'s writer answers it', async () => {
  const { MACRO_SPECS, MARKET_SPECS, RATE_SPECS } = await import('../src/examples/markets.ts');
  const { macroCsv, marketsCsv, ratesCsv } = await import('../src/samples.ts');
  const pages = [
    ['markets', async () => (await import('../src/examples/markets.ts')).marketsPage()],
    ['gallery', async () => (await import('../src/examples/timeseries-gallery.ts')).timeseriesGalleryPage()],
  ] as const;
  const db = await openDatabase();
  const local = new DuckDbEngine(db.connection);
  const outcomes: string[] = [];
  const unexpected: string[] = [];
  const agreed: string[] = [];
  let ran = 0;
  try {
    for (const [what, build] of pages) {
      const page = await build();
      const grids = new Map<string, { input: TsInput; planner: LitePlanner; table: string }>();
      const samples = ([['prices', marketsCsv(), MARKET_SPECS], ['rates', ratesCsv(), RATE_SPECS], ['macro', macroCsv(), MACRO_SPECS]] as const)
        .filter(([cube]) => page.cubes.some((c) => c.id === cube));
      for (const [cube, csv] of samples) {
        const r = await ingestFile(local, db.files, new File([csv], `sample-${cube}.csv`), writer, { table: `${what}_${cube}` });
        const own = plannerFor(r.model, r.runtime);
        const saved = page.cubes.find((c) => c.id === cube)!.cube.query;
        grids.set(cube, { input: { snapshot: { ...saved, source: { query: r.source }, epoch: 1 }, filter: undefined }, planner: own, table: r.table });
      }
      const cubes = samples.map(([cube]) => cube);
      const opened = await openSources(cubes.map((c) => page.cubes.find((x) => x.id === c)!.cube.source), {
        here: TAB_TABLES, tables: cubes.map((c) => grids.get(c)!.table), models: writer, databaseType: 'DuckDB',
        catalog: (table) => catalogColumns(local, writer, table),
        typer: (model, runtime) => plannerFor(model, runtime),
      });
      const joint = plannerFor(opened.model, opened.runtime);
      const together = new Map<string, TsInput>(cubes.map((c, i) => [c, { ...grids.get(c)!.input, snapshot: { ...grids.get(c)!.input.snapshot, source: { query: opened.relations[i]! } } }] as const));
      for (const view of page.views) {
        if (view.kind !== 'timeseries') continue;
        // the series as the tile computes it, before its view's compare (the view's, around the answer: P6)
        const ts: TimeseriesSpec = view.spec.view === undefined ? view.spec : { ...view.spec, view: uncompared(view.spec.view) };
        const cubeOf = (alias: string): string => ts.sources.find((x) => x.alias === alias)!.cube;
        for (const series of ts.series) {
          const id = `${view.id} ${series.id}`;
          const aliases = seriesSources(ts, series);
          const [inputsOf, plannerOf]: [TsInputs, LitePlanner] = aliases.length > 1
            ? [new Map(aliases.map((a) => [a, together.get(cubeOf(a))!] as const)), joint]
            : [new Map([[aliases[0]!, grids.get(cubeOf(aliases[0]!))!.input]]), grids.get(cubeOf(aliases[0]!))!.planner];
          ran += 1;
          let said: string;
          try {
            const q = seriesQuery(inputsOf, ts, series, {}, {}, null);
            const tile = await local.execute(await plannerOf.plan(q.query), 0);
            const pairs = fields(q.band, q.candles === true);
            const date = ts.sources.find((s) => s.alias === aliases[0])!.date;
            const mine = await local.execute(await plannerOf.plan(tsSeriesQuery(inputsOf, ts, series)), 0);
            const d = difference(keyed(tile, TS_DATE, q.split === undefined ? undefined : TS_SPLIT, pairs.map(([a]) => a)),
              keyed(mine, date, q.split, pairs.map(([, b]) => b)));
            said = d === undefined ? 'same' : `DIFFERS: ${d}`;
          } catch (e) {
            said = `FAILS: ${(e as Error).message.split('\n')[0]!.slice(0, 300)}`;
          }
          const expected = DIFFERS[id];
          if (said === 'same' && expected !== undefined) agreed.push(id);
          if (said !== 'same' && expected === undefined) unexpected.push(id);
          outcomes.push(`${id} (${series.name ?? ''}) ${said === 'same' ? 'same' : said}\n    #TS{${tsQueryTextOrWhy(ts, series)}}#`);
        }
      }
    }
  } finally {
    await local.close();
  }
  console.log(outcomes.filter((o) => !o.includes(') same\n')).join('\n'));
  console.log(`${ran} series: ${outcomes.filter((o) => o.includes(') same\n')).length} the same`);
  assert.deepEqual(unexpected, [], 'every series answers the same, or is listed in DIFFERS with why');
  assert.deepEqual(agreed, [], 'a series listed in DIFFERS that now answers the same: delete its row');
});

/** A tile's view without its compare: the series as computed. */
function uncompared(v: TsView): TsView {
  const { compare, ...rest } = v;
  void compare;
  return rest;
}

function tsQueryTextOrWhy(ts: TimeseriesSpec, series: TsSeries): string {
  try {
    return tsQueryText(ts, series);
  } catch (e) {
    return `(not written: ${(e as Error).message})`;
  }
}
