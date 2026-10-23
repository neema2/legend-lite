// A file becomes a cube: infer the schema, then WRITE THE MODEL.
//
// This is what lets someone open the page and drop a file in, rather
// than hand-authoring a Pure model that happens to match their
// columns. DuckDB sniffs the types and its catalog reports them, structured
// (the catalog question, `TableModels.catalogColumnsSql`, or a warehouse's listing); legend-lite's
// compiler writes the whole model from those rows -- the `###Relational Database`, and the
// `###Connection` + `###Runtime` the planner needs -- in its WebAssembly module
// (`planner.Boundary.tableModel`, the module's `tableModelOrError`), the ONE writer, which Python's frames use too. The cube's
// COLUMNS are not decided here: the compiler types the source once the model is in
// (`sourceColumns`, docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md).
//
// The writer is legend-lite's module whichever planner the page chose (the user, 2026-10-08,
// replacing the TypeScript copy of 2026-10-01): on the planners on a server the tab loads the module
// to write a model, and the chosen planner still compiles, types and plans it. legend-engine has
// no such writer to ask, and no server can read a database inside the tab.
//
// Generating the model rather than special-casing "uploaded" data is
// the whole point. Everything downstream -- the planner, the tree
// assembly, the SQL panel, the snap plane -- sees an ordinary model
// over an ordinary table, and none of it learns where the rows came
// from. A separate "local file mode" would be a second pipeline to
// keep in agreement with the first.
//
// Upstream's DataCube does the same thing (LocalFileDataCubeSource:
// registerFileText, insertCSVFromPath with detect:true, then
// DESCRIBE) and is CSV-only, with a warning that the format must
// have a header row and comma delimiters. We read Parquet too,
// because duckdb-wasm has it compiled in and registerFileBuffer
// makes it no harder.

import type { HostOperation } from '../../engine-client/src/host-operations.ts';
import type { ValueSpecification } from '../../pure-protocol/src/index.ts';

/** One column, as its database's catalog describes it (the catalog question's row). */
export interface CatalogColumn {
  readonly name: string;
  /** Its own type name, as the catalog writes it: for an alias and for messages, never parsed. */
  readonly dataType: string;
  /** Its canonical type (DuckDB's `duckdb_types().logical_type`; Postgres's base type name, or its kind:
   *  `ARRAY`, `ENUM`, ...), or null when the catalog names none. */
  readonly logicalType: string | null;
  /** A DECIMAL's precision and scale, as numbers; null for any other type. */
  readonly precision: number | null;
  readonly scale: number | null;
  /** The catalog says it holds no NULL: declared `NOT NULL`, so the compiler types it `[1]`. */
  readonly notNull: boolean;
}

export interface InferredModel {
  /** Pure source: database, connection, runtime. */
  readonly model: string;
  readonly runtime: string;
  /** The relation the cube reads from, as protocol. */
  readonly source: ValueSpecification;
  /**
   * SQL over a column that a COPY of the table applies so it holds its declared type: an upload's
   * rewrite, a Snap. A read-only source reads such a column (a zoned timestamp) as stored.
   */
  readonly conversions: readonly { readonly column: string; readonly sql: string }[];
  /**
   * The select list a copy applies, written by the copy's own database (`* REPLACE (<conversion> AS "<column>",
   * ...)` on DuckDB, or `*`): present when the table is copied, through the snap runtime or rewritten in place
   * (`convertible`); a read-only source with no snap has none.
   */
  readonly copySelectList?: string;
  /** Columns left out: the source cannot convert them, or no Database type holds them (bytes). */
  readonly excluded: readonly string[];
  /**
   * The columns declared BIT (DuckDB's BOOLEAN): legend-engine types them TinyInt, and a planner
   * on engine reads them Boolean (relation-type.ts, ENGINE DEFECT S23).
   */
  readonly bitColumns: readonly string[];
}

export interface InferOptions {
  /** Table name the data was ingested under. */
  readonly table: string;
  /**
   * Its schema, when it has one: a warehouse's `sales.v_orders`. The model
   * then declares `Schema sales ( Table v_orders ... )`, and the planner
   * writes `"sales"."v_orders"` -- the same name on the warehouse and in a
   * local snap, so one model reads both.
   */
  readonly schema?: string;
  /** Package for the generated elements. Must be a valid Pure path. */
  readonly pkg?: string;
  /**
   * Whether the source can apply a conversion: an upload, rewritten at ingest, can; a read-only
   * warehouse table cannot, and a column that needs one to be read at all is left out (a zoned
   * timestamp needs one only in a copy: read in place, it is its UTC instant under the UTC session).
   */
  readonly convertible: boolean;
  /**
   * The table's database type, as a Pure connection names it (`DuckDB`, `Postgres`): the runtime's
   * connection `type`, from which the planner picks its dialect. Given by where the table is -- the
   * tab's engine (`QueryEngine.databaseType`) or a warehouse catalog (`CatalogObject.databaseType`).
   */
  readonly databaseType: string;
}

/** A table of the host's database for a model of several (`TableModels.tablesModel`): its catalog rows. */
export interface ModelTable {
  readonly table: string;
  readonly schema?: string;
  /** The package of the Database it is declared in (default `local`): tables of one package share one Database. */
  readonly pkg?: string;
  /** As `InferOptions.convertible`. */
  readonly convertible: boolean;
  readonly columns: readonly CatalogColumn[];
}

/**
 * The model of several tables of one database (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3: a join's inputs, read in
 * one place): each package's Database declaring its tables as each one's own model does, one connection, and a runtime
 * binding every Database to it -- and, for each table in the order given, the relation that reads it, its conversions
 * and the columns left out.
 */
export interface TablesModel {
  readonly model: string;
  readonly runtime: string;
  readonly tables: readonly {
    readonly source: ValueSpecification;
    readonly conversions: readonly { readonly column: string; readonly sql: string }[];
    readonly excluded: readonly string[];
  }[];
}

/**
 * THE MODEL WRITER for a table (legend-lite's module: `LitePlanner` implements it). A column of a
 * type no Database declares is refused, naming it (a `PlanError` with the compiler's words). A Snap's copy is modelled
 * the same way, from what the copy holds (upload.ts modelLanded).
 */
export interface TableModels {
  tableModel(columns: readonly CatalogColumn[], options: InferOptions): Promise<InferredModel>;
  /** The model of several tables of a database of `databaseType` (`TablesModel`); a table given twice is refused. */
  tablesModel(tables: readonly ModelTable[], databaseType: string): Promise<TablesModel>;
  /** THE catalog question for one table of a DuckDB: its rows are `tableModel`'s columns (`catalogColumns`). */
  catalogColumnsSql(schema: string, table: string): Promise<string>;
  /**
   * What a session of the database runs before it is queried, so that it answers as the planner's SQL expects: the
   * dialect's own setup (`Boundary.sessionSetup`: a DuckDB session in UTC). A host runs it on the database it opens
   * (services/browser.ts `setUpSession`; Python's Frames, from the same function).
   */
  sessionSetup(databaseType: string): Promise<readonly string[]>;
  /** A host's named operations on its own database (D5, engine-client's host-operations.ts): each one's statements. */
  hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<string[][]>;
}
