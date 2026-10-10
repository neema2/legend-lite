// What Python's ll.Page writes (python/legend_lite/page.py; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md), in
// DataCube's own words: its filter operators, aggregates, sort directions and charts, the page's and the cube's
// versions, and the numbers a layout places tiles by, each as DataCube's sources spell them -- so a name added here is
// one Python offers too, and Python never writes one DataCube does not read, nor places a tile where DataCube would
// not. It reads both sides as text: the two halves are in two languages.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Sources } from '../../tools/js/runfiles.mts';

// the sources this reads, as the BUILD target declares them (_SCANNED)
const SOURCES = new Sources('SOURCES', 'datacube');
const PYTHON = SOURCES.read('../python/legend_lite/page.py');

/** The quoted names of a TypeScript union type, `export type Name = 'a' | 'b' ...;`. */
function union(file: string, name: string): string[] {
  const text = SOURCES.read(file);
  const at = text.indexOf(`export type ${name} =`);
  assert.ok(at >= 0, `${file} declares ${name}`);
  const body = text.slice(at, text.indexOf(';', at));
  return [...body.matchAll(/'([A-Za-z]+)'/g)].map((m) => m[1]!).sort();
}

/** The quoted names of a Python constant, `NAME = frozenset({'a', 'b', ...})`. */
function python(name: string): string[] {
  const at = PYTHON.indexOf(`${name} = frozenset({`);
  assert.ok(at >= 0, `page.py declares ${name}`);
  const body = PYTHON.slice(at, PYTHON.indexOf('})', at));
  return [...body.matchAll(/'([A-Za-z]+)'/g)].map((m) => m[1]!).sort();
}

/** A numeric constant: `export const NAME = 0.5;` in TypeScript, `NAME = 0.5` in Python. */
const tsNumber = (file: string, name: string): number => Number(new RegExp(`export const ${name} = ([\\d.]+);`).exec(SOURCES.read(file))?.[1]);
const pyNumber = (name: string): number => Number(new RegExp(`^${name} = ([\\d.]+)$`, 'm').exec(PYTHON)?.[1]);

describe('Python writes pages in DataCube\'s own words', () => {
  it('its filter operators are the filter editor\'s', () => {
    const operators = union('src/snapshot.ts', 'FilterOperator');
    assert.ok(operators.length > 20, 'the scan found them');
    assert.deepEqual(python('FILTER_OPERATORS'), operators);
  });

  it('its aggregates, sort directions and charts are the grid\'s and the chart\'s', () => {
    assert.deepEqual(python('AGGREGATES'), union('src/snapshot.ts', 'AggregateFn'));
    assert.deepEqual(python('DIRECTIONS'), union('src/snapshot.ts', 'SortDirection'));
    assert.deepEqual(python('MARKS'), union('src/chart-spec.ts', 'ChartMark'));
  });

  it('its page and cube versions are the documents\'', () => {
    assert.equal(pyNumber('PAGE_VERSION'), tsNumber('src/page-document.ts', 'PAGE_VERSION'));
    assert.equal(pyNumber('CUBE_VERSION'), tsNumber('src/cube-document.ts', 'CUBE_VERSION'));
  });

  it('it places a tile as the layout does: a new band\'s height, the places a band holds, the bands to a screen', () => {
    for (const name of ['BAND_HEIGHT', 'MAX_COLUMNS', 'SCREEN_BANDS']) {
      const ts = tsNumber('src/layout/bands.ts', name);
      assert.ok(ts > 0, `bands.ts declares ${name}`);
      assert.equal(pyNumber(name), ts, name);
    }
  });
});
