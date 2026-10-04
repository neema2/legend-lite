// SAVE A CUBE, OPEN IT IN A FRESH APP (docs/DATACUBE_SAVE_SHARE_2026_09_28.md, milestone 1
// step 1). The real app (jsdom) over the real WASM planner and DuckDB-WASM: a file is read the
// way the page reads one (`ingestFile`), a cube is built and saved as a document, the document
// goes through JSON text, the file is read again, and a FRESH app opens the cube over it. What
// the second app shows must be what the first showed -- typed values, not rendered text.

import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import path from 'node:path';
import { before, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { CubeApp } from '../../src/app.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../../src/config.ts';
import {
  cubeToJson,
  fileSource,
  openCube,
  readCube,
  type FileSource,
} from '../../src/cube-document.ts';
import { DuckDbEngine, type ArrowishConnection } from '../../../engine-client/src/duckdb.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import { sourceColumns } from '../../src/source-columns.ts';
import { TreeState } from '../../src/tree.ts';
import { ingestFile, type DuckDbFiles } from '../../src/upload.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { col, fn, lambda, lit, times, type ValueSpecification } from '../../../pure-protocol/src/index.ts';

const MODULE_DIR = new URL('../../../wasm/planner/', import.meta.url).href;

/** A model to start the planner on; each file read replaces it (`useModel`). */
const START = `###Relational
Database s::DB ( Table T ( id INTEGER ) )

###Connection
RelationalDatabaseConnection s::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime s::RT
{
    mappings: [];
    connections:
    [
        s::DB: [ c1: s::Conn ]
    ];
}
`;

const HEADER = 'region,desk,notional,qty\n';
const ROWS = [
  'EMEA,Rates,100.5,1', 'EMEA,Credit,200.25,2', 'EMEA,Rates,50,3',
  'AMER,Rates,300,4', 'AMER,FX,10.75,5', 'APAC,FX,99.99,6',
];
const CSV = HEADER + ROWS.join('\n') + '\n';

let engine: DuckDbEngine;
let files: DuckDbFiles;
let planner: WasmPlanner;

before(async () => {
  const require = createRequire(import.meta.url);
  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const dist = path.dirname(require.resolve('@duckdb/duckdb-wasm/blocking'));
  const db = await duckdb.createDuckDB({
    mvp: { mainModule: path.join(dist, 'duckdb-mvp.wasm'), mainWorker: path.join(dist, 'duckdb-node-mvp.worker.cjs') },
    eh: { mainModule: path.join(dist, 'duckdb-eh.wasm'), mainWorker: path.join(dist, 'duckdb-node-eh.worker.cjs') },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate();
  engine = new DuckDbEngine(db.connect() as ArrowishConnection);
  files = {
    registerFileText: async (name, text) => db.registerFileText(name, text),
    registerFileBuffer: async (name, buffer) => db.registerFileBuffer(name, buffer),
  };
  planner = new WasmPlanner({ model: START, runtime: 's::RT', assetBaseUrl: MODULE_DIR, cache: false });
});

/** Read a file the way the page does, and describe it as a saved cube names it. */
async function read(name: string, text: string): Promise<{
  relation: ValueSpecification; columns: CubeSnapshot['columns']; source: FileSource;
}> {
  const file = new File([text], name, { type: 'text/csv' });
  const opened = await ingestFile(engine, files, file);
  planner.useModel(opened.model, opened.runtime);
  const columns = await sourceColumns(planner, opened.source);
  return { relation: opened.source, columns, source: await fileSource(file, 'csv', columns) };
}

/** The real app over a cube, in jsdom, opened and settled. */
async function openApp(
  snapshot: CubeSnapshot,
  configuration: CubeConfiguration,
  source: FileSource,
  tree?: TreeState,
): Promise<{ app: CubeApp; errors: string[]; root: HTMLElement }> {
  const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
  (globalThis as { requestAnimationFrame?: unknown }).requestAnimationFrame =
    (fn: () => void) => { fn(); return 0; };
  const errors: string[] = [];
  const root = dom.window.document.getElementById('r') as HTMLElement;
  const app = new CubeApp(root, snapshot, {
    engine, planner, configuration, cubeSource: source, ...(tree ? { tree } : {}),
    onStatus: (text, kind) => { if (kind === 'error') errors.push(text); },
  });
  await app.open();
  return { app, errors, root };
}

/** What the grid holds, typed: each column's name, compiler type and values. */
function shown(app: CubeApp): { name: string; type: string; values: string[] }[] {
  const t = app.view?.rows;
  assert.ok(t, 'no view');
  return t.columns.map((c) => ({ name: c.name, type: c.type, values: c.values.map((v) => String(v)) }));
}

describe('a saved cube reopens in a fresh app over its file', () => {
  it('shows exactly what it showed: grouping, measure, calculated column, filter, formats, open rows', async () => {
    const first = await read('trades.csv', CSV);
    const cube: CubeSnapshot = {
      source: { query: first.relation },
      columns: first.columns,
      derived: [{ name: 'double', lambda: lambda(['x'], times(fn('toOne', col('x', 'notional')), lit.integer(2))) }],
      rows: ['region'],
      pivotOn: [],
      measures: [{ name: 'notional', column: 'notional', fn: 'sum' }, { name: 'double', column: 'double', fn: 'sum' }],
      sorts: [{ column: 'region', direction: 'asc' }],
      filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 1 },
      epoch: 1,
    };
    const config: CubeConfiguration = {
      ...DEFAULT_CONFIGURATION,
      reportTitle: 'Q3 review',
      maxRows: 500,
      columns: { notional: { format: { kind: 'number', decimals: 3 } } },
    };
    const a = await openApp(cube, config, first.source, TreeState.fromPaths([['EMEA']]));
    assert.deepEqual(a.errors, []);
    const saved = a.app.cubeDocument('Q3 review');
    assert.ok(saved);
    const text = cubeToJson(saved);
    assert.doesNotMatch(text, /Credit|Rates/, 'no data in a saved cube (desk values appear only in rows)');

    // the file again, a fresh app
    const again = await read('trades.csv', CSV);
    assert.equal(saved.source._type, 'file');
    assert.equal(again.source.sha256, saved.source._type === 'file' ? saved.source.sha256 : '', 'the same file, by its fingerprint');
    const opened = openCube(readCube(text), { query: again.relation }, again.columns);
    assert.deepEqual(opened.notes, []);
    const b = await openApp(opened.snapshot, opened.configuration, again.source, opened.tree);
    assert.deepEqual(b.errors, []);

    assert.deepEqual(shown(b.app), shown(a.app), 'the second app shows the first app\'s typed values');
    assert.deepEqual(b.app.snapshot.rows, ['region']);
    assert.deepEqual(b.app.snapshot.filter, cube.filter);
    assert.equal(b.app.configuration.maxRows, 500);
    assert.equal(b.app.configuration.columns['notional']?.format?.decimals, 3);
    assert.equal(b.app.configuration.reportTitle, 'Q3 review');
    assert.equal(b.app.tree.isOpen(['EMEA']), true, 'the open rows came back');
  });

  it('opens over a file that lost a column, leaving out and naming what used it', async () => {
    const first = await read('trades.csv', CSV);
    const cube: CubeSnapshot = {
      source: { query: first.relation },
      columns: first.columns,
      derived: [],
      rows: ['region'],
      pivotOn: [],
      measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
      sorts: [],
      filter: { kind: 'condition', column: 'qty', operator: 'greaterThan', value: 1 },
      epoch: 1,
    };
    const a = await openApp(cube, DEFAULT_CONFIGURATION, first.source);
    const text = cubeToJson(a.app.cubeDocument('lossy') ?? assert.fail('no document'));

    const noQty = 'region,desk,notional\n' + ROWS.map((r) => r.split(',').slice(0, 3).join(',')).join('\n') + '\n';
    const changed = await read('trades.csv', noQty);
    assert.notEqual(changed.source.sha256, first.source.sha256, 'a different file, by its fingerprint');
    const opened = openCube(readCube(text), { query: changed.relation }, changed.columns);
    assert.match(opened.notes.join('\n'), /no longer in the file: qty/);
    assert.match(opened.notes.join('\n'), /left out the filter on qty/);
    assert.equal(opened.changed, true);
    const b = await openApp(opened.snapshot, opened.configuration, changed.source);
    assert.deepEqual(b.errors, [], 'it opens, without the parts that cannot apply');
    const regions = shown(b.app).find((c) => c.name === '__tree')?.values ?? [];
    assert.ok(regions.length > 0, 'the grouping still shows');
    assert.equal(b.app.snapshot.filter, undefined);
  });

  it('opens over a file with a NEW column, hidden', async () => {
    const first = await read('trades.csv', CSV);
    const cube: CubeSnapshot = {
      source: { query: first.relation }, columns: first.columns, derived: [],
      rows: [], pivotOn: [], measures: [], sorts: [], epoch: 1,
    };
    const a = await openApp(cube, DEFAULT_CONFIGURATION, first.source);
    const text = cubeToJson(a.app.cubeDocument('flat') ?? assert.fail('no document'));
    const wider = 'region,desk,notional,qty,book\n' + ROWS.map((r) => `${r},B1`).join('\n') + '\n';
    const next = await read('trades.csv', wider);
    const opened = openCube(readCube(text), { query: next.relation }, next.columns);
    assert.match(opened.notes.join('\n'), /1 new column .*book/);
    const b = await openApp(opened.snapshot, opened.configuration, next.source);
    assert.deepEqual(b.errors, []);
    assert.equal(b.app.configuration.columns['book']?.hidden, true);
    // hidden is the GRID's: the rendered headers, not the query's column model
    const headers = [...b.root.querySelectorAll<HTMLElement>('.dc-th[data-column]')].map((h) => h.dataset['column']);
    assert.ok(headers.includes('region'), headers.join(', '));
    assert.equal(headers.includes('book'), false, `not shown: ${headers.join(', ')}`);
  });
});
