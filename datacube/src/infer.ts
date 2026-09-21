// A file becomes a cube: infer the schema, then WRITE THE MODEL.
//
// This is what lets someone open the page and drop a file in, rather
// than hand-authoring a Pure model that happens to match their
// columns. DuckDB sniffs the types; this turns what it found into
// the `###Relational Database` + `###Connection` + `###Runtime` that
// the planner needs, plus the column list the cube needs.
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

/** One column, as DuckDB's `DESCRIBE` reports it. */
export interface DescribedColumn {
  readonly name: string;
  /** DuckDB's own type name, e.g. 'VARCHAR', 'BIGINT', 'DECIMAL(9,2)'. */
  readonly type: string;
}

export interface InferredModel {
  /** Pure source: database, connection, runtime. */
  readonly model: string;
  readonly runtime: string;
  /** The relation expression the cube reads from. */
  readonly source: string;
  readonly columns: readonly {
    readonly name: string;
    readonly type: string;
    readonly kind?: 'dimension' | 'measure';
  }[];
}

/**
 * A DuckDB type to the SQL type a legend-lite Database declares.
 *
 * Deliberately conservative: an unrecognised type becomes VARCHAR,
 * because a column the planner can only group by is far less harmful
 * than one whose arithmetic silently means something else. The list
 * covers what DuckDB's CSV and Parquet readers actually produce.
 */
export function sqlTypeOf(duckdbType: string): string {
  const t = duckdbType.trim().toUpperCase();
  // DECIMAL(p,s) and VARCHAR(n) carry their own precision; keep it.
  if (/^DECIMAL\s*\(/.test(t) || /^NUMERIC\s*\(/.test(t)) {
    return t.replace(/^NUMERIC/, 'DECIMAL');
  }
  switch (t) {
    case 'BOOLEAN': case 'BOOL': return 'BIT';
    case 'TINYINT': case 'SMALLINT': case 'INTEGER': case 'INT':
      return 'INTEGER';
    case 'BIGINT': case 'HUGEINT': case 'UBIGINT': return 'BIGINT';
    case 'FLOAT': case 'REAL': case 'DOUBLE': return 'DOUBLE';
    case 'DECIMAL': case 'NUMERIC': return 'DECIMAL(38,6)';
    case 'DATE': return 'DATE';
    case 'TIMESTAMP': case 'TIMESTAMP WITH TIME ZONE': case 'TIMESTAMPTZ':
      return 'TIMESTAMP';
    default: return 'VARCHAR(4096)';
  }
}

/** The SQL type a Database declares, to the Pure type the cube shows. */
export function pureTypeOf(sqlType: string): string {
  const t = sqlType.trim().toUpperCase();
  if (t.startsWith('DECIMAL')) return 'Float';
  switch (t) {
    case 'BIT': return 'Boolean';
    case 'INTEGER': case 'BIGINT': return 'Integer';
    case 'DOUBLE': return 'Float';
    case 'DATE': return 'StrictDate';
    case 'TIMESTAMP': return 'DateTime';
    default: return 'String';
  }
}

/**
 * Quote an identifier for a Pure Database declaration.
 *
 * A CSV header is arbitrary text -- spaces, a comma, a SQL keyword,
 * a leading digit, a quote character -- and it arrives from outside.
 * Anything that is not a plain identifier is quoted and escaped so
 * that the header stays a NAME and cannot become grammar.
 */
export function quoteIdent(name: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) return name;
  // BACKSLASH, not a doubled quote. The lexer's escape inside a
  // quoted identifier is the backslash, and it terminates the token
  // at the first unescaped `"` -- so `"a""b"` lexes as `"a"` then
  // `"b"` and breaks the whole Database declaration. Escape the
  // backslash first, or a name ending in one would escape the
  // closing quote.
  return `"${name.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

/**
 * Columns that should be MEASURES rather than dimensions.
 *
 * ONLY A FRACTIONAL TYPE SUMS. Double and decimal are what money,
 * rates and weights arrive as, and summing them is what the user
 * wants. An INTEGER defaults to its unique value instead.
 *
 * <h2>Why integers do not sum</h2>
 *
 * The harm is asymmetric. Summing an id, a year, a postcode or a
 * version produces a plausible-looking number that is meaningless,
 * and nothing about the grid says so. Taking the unique value of a
 * quantity produces a blank, which reads as "no aggregate chosen" --
 * unhelpful, never misleading. Either is one click to change.
 *
 * Integers are also far more often keys than sums: ids, years,
 * quarters, codes, flags, postcodes. `quantity` is the honest cost
 * of this rule, and it is a blank rather than a wrong total.
 *
 * This replaces a name heuristic that tried to spot keys by
 * spelling. It matched `.*id$`, so in a trading dataset it
 * classified `bid` -- a price -- as a key, along with `paid`,
 * `valid`, `void` and `grid`; and it still missed `cusip`, `isin`,
 * `sedol`, `sku` and `account`. A rule that is wrong in both
 * directions and needs a per-upload DISTINCT query to prop it up is
 * worse than one line of type dispatch.
 *
 * DataCube sums every numeric (DataCubeConfigurationBuilder), so
 * this is a deliberate divergence -- on the side that cannot
 * produce a confident wrong number.
 */
function kindOf(pureType: string): 'dimension' | 'measure' {
  return pureType === 'Float' ? 'measure' : 'dimension';
}

export interface InferOptions {
  /** Table name the data was ingested under. */
  readonly table: string;
  /** Package for the generated elements. Must be a valid Pure path. */
  readonly pkg?: string;
}

/**
 * Turn `DESCRIBE`'s answer into a model the planner can compile.
 *
 * Throws on an empty schema rather than emitting a Database with no
 * columns: the planner's refusal for that is about a malformed model,
 * which is a confusing thing to show someone who just picked a file.
 */
export function inferModel(
  described: readonly DescribedColumn[],
  options: InferOptions,
): InferredModel {
  if (described.length === 0) {
    throw new Error('the file has no columns — is it empty, '
      + 'or missing its header row?');
  }
  const seen = new Set<string>();
  for (const c of described) {
    const key = c.name.toLowerCase();
    if (seen.has(key)) {
      // DuckDB will have disambiguated already; if one still slips
      // through, two columns of the same name make every reference
      // to it ambiguous, and the planner's error would not explain
      // which file caused it.
      throw new Error(`the file has two columns named '${c.name}'`);
    }
    seen.add(key);
  }

  const pkg = options.pkg ?? 'local';
  const table = options.table;

  const cols = described.map((c) => {
    const sql = sqlTypeOf(c.type);
    const pure = pureTypeOf(sql);
    return { name: c.name, sql, pure, kind: kindOf(pure) };
  });

  const columnLines = cols
    .map((c) => `        ${quoteIdent(c.name)} ${c.sql}`)
    .join(',\n');

  const model = `###Relational
Database ${pkg}::DB
(
    Table ${quoteIdent(table)}
    (
${columnLines}
    )
)

###Connection
RelationalDatabaseConnection ${pkg}::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime ${pkg}::RT
{
    mappings: [];
    connections:
    [
        ${pkg}::DB: [ c1: ${pkg}::Conn ]
    ];
}
`;

  return {
    model,
    runtime: `${pkg}::RT`,
    source: `#>{${pkg}::DB.${quoteIdent(table)}}#`,
    columns: cols.map((c) => ({ name: c.name, type: c.pure, kind: c.kind })),
  };
}
