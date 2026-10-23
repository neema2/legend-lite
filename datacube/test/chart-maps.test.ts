// BATCH 5'S MAPS (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §4): the shapes as prepared (Natural Earth's countries,
// tools/maps/prepare.py), how a row finds its country, the Equal Earth projection, the rules a map is judged by, the
// query a bubble map asks, and each map drawn -- as SVG, by ECharts itself, its map registered as the browser's is.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element } from '../../pure-protocol/src/index.ts';
import { chartFrom, chartProblems, chartSnapshot, DEFAULT_CHART_LIMIT, editChart, followCube, MAP_LIMIT, readChart, type ChartSpec } from '../src/chart-spec.ts';
import { describe as describeChart, type ChartDrawing } from '../src/chart-option.ts';
import { mapOption, WORLD } from '../src/chart-maps.ts';
import { equalEarth, equalEarthInverse, regionFinder } from '../src/maps/shapes.ts';
import { WORLD_110M } from '../src/maps/world-110m.ts';
import * as echarts from 'echarts/core';
import { JSDOM } from 'jsdom';
import { paintChartForm } from '../src/ui/chart-form.ts';
import { SVGRenderer } from 'echarts/renderers';
import '../src/chart-render.ts';
import { registerWorld } from '../src/chart-render-maps.ts';
import type { EChartsOption } from 'echarts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

echarts.use([SVGRenderer]);
registerWorld(WORLD.name, WORLD.shapes);

function svg(drawing: ChartDrawing): string {
  const chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width: 720, height: 400 });
  try {
    chart.setOption({ ...(drawing.option as EChartsOption), animation: false });
    return chart.renderToSVGString();
  } finally {
    chart.dispose();
  }
}

const CUBE: CubeSnapshot = {
  source: { query: element('sales') },
  columns: [
    { name: 'country', type: 'String' },
    { name: 'city', type: 'String' },
    { name: 'lat', type: 'Float', kind: 'dimension' },
    { name: 'lon', type: 'Float', kind: 'dimension' },
    { name: 'units', type: 'Integer' },
    { name: 'revenue', type: 'Float' },
  ],
  derived: [],
  rows: ['country'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};
const OPTIONS: ChartSpec['options'] = {
  orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' }, limit: MAP_LIMIT, labels: false, legend: 'top',
};
const MAP: ChartSpec = { version: 2, mark: 'map', x: 'country', y: [{ column: 'revenue', fn: 'sum' }], options: OPTIONS };
const BUBBLES: ChartSpec = { ...MAP, mark: 'bubblemap', x: 'city', at: { lat: 'lat', lon: 'lon' } };

function table(columns: { name: string; type: string; values: (string | number | null)[] }[]): ResultTable {
  return { columns, rowCount: columns[0]?.values.length ?? 0, epoch: 1, elapsedMs: 0 };
}

describe('the shapes as prepared: Natural Earth\'s 177 countries, each found by its code or a name', () => {
  it('each country once by name, its codes where Natural Earth gives them (France and Norway\'s too)', () => {
    const names = WORLD_110M.features.map((f) => f.properties.name);
    assert.equal(names.length, 177);
    assert.equal(new Set(names).size, names.length);
    const france = WORLD_110M.features.find((f) => f.properties.name === 'France')!;
    assert.deepEqual([france.properties.iso3, france.properties.iso2], ['FRA', 'FR']);
    assert.equal(WORLD_110M.features.find((f) => f.properties.name === 'Kosovo')?.properties.iso3, null);
  });
  it('every coordinate to two decimals: a shared border\'s vertices the same on both sides', () => {
    const flat = (c: unknown): number[] => (Array.isArray(c) ? c.flatMap(flat) : [c as number]);
    const all = WORLD_110M.features.flatMap((f) => flat(f.geometry.coordinates));
    assert.ok(all.length > 10_000);
    assert.ok(all.every((v) => Math.abs(v * 100 - Math.round(v * 100)) < 1e-6));
  });
  it('a code exactly, a name case and spaces aside; anything else is no country', () => {
    const find = regionFinder(WORLD_110M);
    assert.equal(find('USA'), 'United States of America');
    assert.equal(find('US'), 'United States of America');
    assert.equal(find(' united states of america '), 'United States of America');
    assert.equal(find('GBR'), 'United Kingdom');
    assert.equal(find('Tanzania'), 'Tanzania');
    assert.equal(find('United Republic of Tanzania'), 'Tanzania');
    assert.equal(find('usa'), undefined, 'a code is matched exactly');
    assert.equal(find('Singapore'), undefined, 'too small for 1:110m');
    assert.equal(find('Atlantis'), undefined);
  });
});

describe('the Equal Earth projection', () => {
  it('places the origin at the centre, keeps a hemisphere\'s side, and comes back to where it started', () => {
    assert.deepEqual(equalEarth([0, 0]).map((v) => Math.abs(v)), [0, 0]);
    const [x, y] = equalEarth([100, 45]);
    assert.ok(x! > 0 && y! < 0, 'east is right, north is up (y downward, as a screen)');
    for (const p of [[-74, 40.7], [139.7, 35.7], [-180, -89], [0, 90], [151.2, -33.9]]) {
      const back = equalEarthInverse(equalEarth(p));
      assert.ok(Math.abs(back[0]! - p[0]!) < 1e-6 && Math.abs(back[1]! - p[1]!) < 1e-6, `${p} came back as ${back}`);
    }
  });
});

describe('a map\'s rules', () => {
  const problems = (spec: ChartSpec) => chartProblems(spec, CUBE).join(' ');
  it('a map reads a country\'s code or name; it plots one value and has no split', () => {
    assert.deepEqual(chartProblems(MAP, CUBE), []);
    assert.match(problems({ ...MAP, x: 'units' }), /'units' is not text/);
    assert.match(problems({ ...MAP, y: [{ column: 'revenue', fn: 'sum' }, { column: 'units', fn: 'sum' }] }), /A map plots one value/);
    assert.match(problems({ ...MAP, split: 'city' }), /A map draws one series/);
  });
  it('a bubble map needs a latitude and a longitude, each a number column; no other kind places by them', () => {
    assert.deepEqual(chartProblems(BUBBLES, CUBE), []);
    const { at: _at, ...unplaced } = BUBBLES;
    assert.match(problems(unplaced), /latitude.*longitude/);
    assert.deepEqual(chartProblems({ ...BUBBLES, at: { lat: 'lat' } }, CUBE), ['A bubble map needs the column of each place\'s longitude.']);
    assert.match(problems({ ...BUBBLES, at: { lat: 'city', lon: 'lon' } }), /'city' is not a number: it places nothing/);
    assert.match(problems({ ...BUBBLES, at: { lat: 'nope', lon: 'lon' } }), /no column 'nope'/);
    assert.match(problems({ ...MAP, mark: 'bar', at: { lat: 'lat', lon: 'lon' } }), /A bar places nothing by latitude and longitude/);
  });
  it('the reader takes its places in its own words and refuses another', () => {
    assert.equal(typeof readChart(BUBBLES), 'object');
    assert.match(String(readChart({ ...BUBBLES, at: { lat: 1 } })), /not the columns of a latitude and a longitude/);
    assert.match(String(readChart({ ...BUBBLES, at: { lat: 'lat', lon: 'lon', zoom: 3 } })), /no "zoom" in its places/);
  });
  it('a chart taking a map asks for the world\'s countries; leaving a bubble map leaves its places', () => {
    const bar: ChartSpec = { ...MAP, mark: 'bar', options: { ...OPTIONS, limit: 50 } };
    assert.equal(editChart(bar, { mark: 'map' }, CUBE).options.limit, MAP_LIMIT);
    assert.equal(editChart(BUBBLES, { mark: 'bar' }, CUBE).at, undefined);
    assert.deepEqual(editChart(BUBBLES, { mark: 'bubblemap', x: 'country' }, CUBE).at, BUBBLES.at);
  });
  it('a map is placed by its own columns: it does not follow the grid\'s grouping', () => {
    assert.equal(followCube(MAP, { ...CUBE, rows: ['city'] }), MAP);
  });
  it('a bubble map\'s query groups by each place\'s latitude and longitude with its name: never an average of positions', () => {
    const s = chartSnapshot(CUBE, BUBBLES);
    assert.deepEqual(s.rows, ['city', 'lat', 'lon']);
    assert.deepEqual(s.measures?.map((m) => m.name), ['sum_revenue']);
  });
});

describe('a map, drawn', () => {
  it('each row\'s country coloured by its value; what finds none, or a country named twice, said', () => {
    const rows = table([
      { name: 'country', type: 'String', values: ['USA', 'FRA', 'Atlantis', 'united kingdom', 'US', null] },
      { name: 'sum_revenue', type: 'Float', values: [500, 300, 10, 200, 7, 1] },
    ]);
    const d = mapOption(MAP, rows);
    const series = (d.option as { series: { type: string; data: { name: string; value: number; row: number }[] }[] }).series;
    assert.equal(series[0]?.type, 'map');
    assert.deepEqual(series[0]?.data.map((r) => [r.name, r.value]), [
      ['United States of America', 500], ['France', 300], ['United Kingdom', 200],
    ]);
    assert.match(d.notes.join(' '), /2 values not on the map: Atlantis, /);
    assert.match(d.notes.join(' '), /US named a country another row named first: the first is drawn/);
    const scale = (d.option as { visualMap: { min: number; max: number } }).visualMap;
    assert.deepEqual([scale.min, scale.max], [200, 500]);
    assert.deepEqual(d.keyAt(0, 1, series[0]?.data[1]), { country: 'FRA' });
    // a country with no row: ECharts adds it to the series as a datum of its name alone, and a click on it filters nothing
    assert.equal(d.keyAt(0, 5, { name: 'Mexico' }), null);
    const out = svg(d);
    assert.ok(out.includes('<path') && out.length > 50_000, 'the countries are drawn');
    assert.match(describeChart(MAP, rows), /^Map of sum_revenue by country: 6 rows\.$/);
  });
  it('a bubble map: a bubble at each place, sized by its value; a place with no position said', () => {
    const rows = table([
      { name: 'city', type: 'String', values: ['London', 'Tokyo', 'Nowhere'] },
      { name: 'lat', type: 'Float', values: [51.51, 35.68, null] },
      { name: 'lon', type: 'Float', values: [-0.13, 139.69, 10] },
      { name: 'sum_revenue', type: 'Float', values: [100, 400, 5] },
    ]);
    const d = mapOption(BUBBLES, rows);
    const series = (d.option as { series: { type: string; coordinateSystem: string; data: { value: number[]; row: number }[] }[] }).series;
    assert.deepEqual([series[0]?.type, series[0]?.coordinateSystem], ['scatter', 'geo']);
    assert.deepEqual(series[0]?.data.map((p) => p.value), [[-0.13, 51.51, 100], [139.69, 35.68, 400]]);
    assert.match(d.notes.join(' '), /1 place not on the map: Nowhere\./);
    assert.deepEqual(d.keyAt(0, 1, series[0]?.data[1]), { city: 'Tokyo' });
    // the bubbles are drawn over the countries: more marks than the countries alone
    const marks = (out: string): number => out.split('<path').length;
    const bare = { ...d, option: { ...(d.option as object), series: [{ ...series[0]!, data: [] }] } } as ChartDrawing;
    assert.equal(marks(svg(d)) - marks(svg(bare)), 2);
  });
});

describe('the phase 6 audit: what each fix pins', () => {
  const DATED: CubeSnapshot = { ...CUBE, columns: [...CUBE.columns, { name: 'sold', type: 'StrictDate' }], rows: ['sold', 'country'] };
  it('a map keeps the world\'s limit from a date across, and a chart leaving a map takes its own default again', () => {
    // Python's grid.chart('map', x=...) on a grid grouped by a date first: DataCube's new chart is a line in time
    const made = chartFrom({ mark: 'map', x: 'country' }, DATED);
    assert.equal(typeof made === 'string' ? made : made.options.limit, MAP_LIMIT);
    const line = chartFrom({}, DATED);
    assert.ok(typeof line !== 'string');
    const viaLine = editChart(editChart(line, { mark: 'map' }, DATED), { x: 'country' }, DATED);
    assert.equal(viaLine.options.limit, MAP_LIMIT);
    assert.equal(editChart(MAP, { mark: 'bar' }, CUBE).options.limit, DEFAULT_CHART_LIMIT);
    assert.equal(editChart({ ...MAP, options: { ...OPTIONS, limit: 30 } }, { mark: 'bar' }, CUBE).options.limit, 30, 'one it was told stays');
    // a map told the time default's number keeps it as its column across changes: a map's limit is never a time default
    assert.equal(editChart({ ...MAP, options: { ...OPTIONS, limit: 2000 } }, { x: 'city' }, CUBE).options.limit, 2000);
  });
  it('a map has no grain or bins, and a bubble map names its places by a column that is not one of them', () => {
    const problems = (spec: ChartSpec, s = DATED) => chartProblems(spec, s).join(' ');
    assert.match(problems({ ...BUBBLES, x: 'sold', grain: 'month' }), /A bubble map places each row by what it names: it has no grain/);
    assert.match(problems({ ...BUBBLES, x: 'units', bins: { count: 5 } }), /it has no bins/);
    assert.match(problems({ ...BUBBLES, x: 'lat' }), /'lat' is a place's latitude: name the places by another column/);
  });
  it('a group-level column reading a place\'s position is the bubble map\'s own grouping, not refused', () => {
    const s: CubeSnapshot = { ...CUBE, groupDerived: [{ name: 'north', lambda: {
      _type: 'lambda', parameters: [{ _type: 'var', name: 'x' }],
      body: [{ _type: 'property', property: 'lat', parameters: [{ _type: 'var', name: 'x' }] }],
    } as never, type: 'Float' }] };
    assert.deepEqual(chartProblems({ ...BUBBLES, y: [{ column: 'north', fn: 'value' }] }, s), []);
  });
  it('a place with no value is said, not dropped without a word', () => {
    const rows = table([
      { name: 'city', type: 'String', values: ['London', 'Paris'] },
      { name: 'lat', type: 'Float', values: [51.51, 48.86] },
      { name: 'lon', type: 'Float', values: [-0.13, 2.35] },
      { name: 'sum_revenue', type: 'Float', values: [100, null] },
    ]);
    assert.match(mapOption(BUBBLES, rows).notes.join(' '), /1 place has no sum of revenue to size by: Paris\./);
  });
  it('the form lists each number column once for a place, and offers no legend on a map', () => {
    const doc = new JSDOM('<!doctype html><div id="form"></div>').window.document;
    const form = doc.getElementById('form')!;
    let spec: ChartSpec = BUBBLES;
    paintChartForm({ doc, spec: () => spec, snapshot: () => CUBE, setOption: () => undefined,
      set: (change) => { spec = editChart(spec, change, CUBE); } }, form);
    const field = (label: string) => [...form.querySelectorAll('.dc-field')].find((f) => f.querySelector('.dc-field-label')?.textContent === label);
    const choices = [...field('Latitude:')!.querySelectorAll('option')].map((o) => o.value).filter((v) => v !== '');
    assert.deepEqual(choices, [...new Set(choices)]);
    assert.ok(choices.includes('lat') && choices.includes('lon'));
    assert.equal(field('Legend:'), undefined);
  });
});
