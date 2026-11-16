// @legend-lite/datacube: what an application that embeds DataCube is promised, and all of it. Nothing else of DataCube
// is promised: its modules, its page's markup and its stylesheet's class names change freely.
//
// In legend-lite's repository this is datacube/demo/library-api.ts, and the package's dist/index.d.ts is this file
// copied as it is (//datacube:npm_types) -- so it holds declarations alone and imports nothing: an application reads
// these types with no other file. The implementation (datacube/demo/library.ts) takes its types from here, so the
// typecheck holds the two together (//datacube:typecheck_test).

/**
 * A table of data handed to DataCube, which it reads into its own database (DuckDB, in the page) as it reads a file a
 * person opens:
 *
 * - a `File`: CSV, JSON or Parquet, told apart by its name's ending (`.csv`, `.json`, `.jsonl`, `.ndjson`, `.parquet`,
 *   each also gzipped as `.gz`); a name with another ending is refused;
 * - `DataCubeRows`: records the application already holds;
 * - `DataCubeContent`: a file's text or bytes the application fetched, with its format.
 */
export type DataCubeData = File | DataCubeRows | DataCubeContent;

/**
 * Records the application holds, one object per row, its keys the columns. They are written as JSON and read back by
 * the database, which decides each column's type from its values: a `Date` is written as its ISO text (a timestamp),
 * a nested object or array is a column of that shape, and `NaN` and the infinities are null, as JSON has them. A
 * `bigint` is written as the digits it is, which needs a browser with `JSON.rawJSON`; without it the table is refused.
 * A large table is better handed over as Parquet or CSV (`DataCubeContent`): it is read without being built as text
 * first.
 */
export interface DataCubeRows {
  /** What the table is called: its table's name in the database (made an identifier), and, as `<name>.json`, what
   *  its grid is titled by -- the file it is read as. */
  readonly name: string;
  readonly rows: readonly object[];
}

/** A file's content the application fetched: its text or its bytes, and which format they are. */
export interface DataCubeContent {
  /** What the table is called: its table's name in the database (made an identifier), and, under its format's ending
   *  (`<name>.csv`), what its grid is titled by -- the file it is read as. */
  readonly name: string;
  readonly format: 'csv' | 'json' | 'parquet';
  readonly content: string | ArrayBuffer | ArrayBufferView | Blob;
}

export interface DataCubeOptions {
  /**
   * Where the package's `assets/` folder is served: the folder's URL, absolute or relative to the page, on the page's
   * own origin (a browser starts a worker from no other). DataCube fetches its database, its compiler and their
   * workers from there, each the first time it is needed. Checked as DataCube starts: a folder that is not there is
   * refused, saying which file was missed.
   */
  readonly assets: string;
  /** The data to open on, each table a grid of its own. None: the blank page, which offers to open a file. */
  readonly data?: readonly DataCubeData[];
  /**
   * The element DataCube's own windows float in and are kept inside: Open…, the Generated Pure & SQL, what went
   * wrong. It must be positioned. Default: the element DataCube is mounted in, which is given `position: relative`
   * when it has no position of its own. (A grid's windows -- Properties, Filter -- float in DataCube's page, and its
   * menus and dialogs are laid over the whole document.)
   */
  readonly windows?: HTMLElement;
  /**
   * Ask the browser to confirm before the page is left while DataCube holds unsaved changes or opened data, as
   * DataCube does in a tab of its own. Default: false -- leaving the page is the application's to ask about.
   */
  readonly askBeforeLeaving?: boolean;
  /**
   * Stops a mount that has not finished: aborted, the mount is rejected with the signal's reason, nothing of DataCube
   * is left in the element, and the page may mount again at once. For a component taken down while it loads. Once the
   * mount has resolved the signal does nothing: `dispose()` takes DataCube down.
   */
  readonly signal?: AbortSignal;
}

/** DataCube mounted in an element. */
export interface DataCube {
  /**
   * Another table, a grid of its own beside the page's (in place of the blank page, when the page is blank). Tables
   * added together are read one after another, in the order asked. Resolves once it is on the page. Rejected, with
   * the reason, when it cannot be read, when it did not land -- the person put another page in DataCube while it was
   * read, or kept a page with unsaved changes that it would have replaced -- or when DataCube was disposed meanwhile;
   * the page is then as it was.
   */
  add(data: DataCubeData): Promise<void>;
  /** DataCube taken out of the element: its page, its windows, menus and dialogs, its database and its compiler end,
   *  and the data they held goes. The element is left empty, to be used again. */
  dispose(): void;
}

/**
 * DataCube's app -- its page of grids and charts, its sources, joins, exports and saved pages -- mounted in `element`,
 * which it fills (give the element a height). Its queries are planned and run in the page: no server is asked.
 *
 * Resolves once the app is open and every table of `options.data` is on the page. Rejected, with the reason, when it
 * cannot start (the assets are not where `assets` says, the browser lacks what it runs on), a table cannot be read, or
 * `options.signal` is aborted; nothing of DataCube is then left in the element.
 *
 * One DataCube at a time in a page: a second mount is refused until the first is disposed, or aborted.
 */
export declare function mountDataCube(element: HTMLElement, options: DataCubeOptions): Promise<DataCube>;
