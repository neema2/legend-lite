// AN APPLICATION'S CODE, typechecked against the npm package as an install of it reads (//datacube:npm_types_tests):
// the package is named, never a path into it, so TypeScript finds its types through its package.json's entry points
// alone -- beside this file, in node_modules, are the package's package.json and its declarations as they ship.
// Strictly, with only the browser's types beside the package's: what the declarations need of an application is
// nothing more. Not part of DataCube's own typecheck (tsconfig.json leaves this folder out): there the package has
// no install to be found in.
import { mountDataCube, type DataCube, type DataCubeData } from '@legend-lite/datacube';
// an import for its effect alone, which TypeScript holds to resolve to something
import '@legend-lite/datacube/styles.css';
import '@legend-lite/datacube/fonts.css';

// records as an application has them: typed by an interface of its own, which has no index signature
interface Order {
  readonly id: bigint;
  readonly region: string;
  readonly qty: number;
  readonly placed: Date;
}
const orders: readonly Order[] = [{ id: 1n, region: 'EMEA', qty: 3, placed: new Date() }];

// each form a table is handed over in
const tables: DataCubeData[] = [
  { name: 'orders', rows: orders },
  { name: 'inline', rows: [{ a: 1, b: 'two' }] },
  { name: 'rates', format: 'csv', content: 'ccy,rate\nUSD,1\n' },
  { name: 'trades', format: 'parquet', content: new Uint8Array(0) },
  new File(['a\n1\n'], 'more.csv'),
];

const element = document.getElementById('cube');
if (!(element instanceof HTMLElement)) throw new Error('no element');
// a mount a component may stop while it loads
const stop = new AbortController();
const cube: DataCube = await mountDataCube(element, {
  assets: '/datacube-assets/', data: tables, askBeforeLeaving: true, windows: document.body, signal: stop.signal,
});
const added: void = await cube.add(new File(['a\n1\n'], 'later.csv'));
void added;
cube.dispose();

// what the types refuse, each held: were one accepted, its line would fail as an expectation not met
// @ts-expect-error -- `assets` is required
void mountDataCube(element, {});
// @ts-expect-error -- not a format DataCube reads
void cube.add({ name: 'sheet', format: 'xlsx', content: '' });
// @ts-expect-error -- records are objects
void cube.add({ name: 'numbers', rows: [1, 2, 3] });
// @ts-expect-error -- nothing else is the package's to give
void cube.page;
