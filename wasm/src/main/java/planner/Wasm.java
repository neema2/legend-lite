package planner;

/**
 * legend-lite's PLANNER, compiled to WebAssembly.
 *
 * <p>Pure text in, SQL text out. No executor, no server, no JDBC — the
 * planner packages depend on java.base alone (jdeps), and the whole
 * pipeline was already proved to run under {@code --limit-modules
 * java.base}. This asks the next question: does it survive an AOT
 * compile to WASM, and does it then answer IDENTICALLY.
 *
 * <p>This one class is the entry point for BOTH targets — TeaVM
 * compiles it to WASM, and {@code JvmMain} calls it on the JVM — so the
 * differential test compares two builds of the same source rather than
 * two hand-kept copies that could drift apart silently.
 *
 * <h2>Why this calls {@code Compiler.plan} and nothing else</h2>
 *
 * <p>The first version of this class hand-assembled the pipeline —
 * parse, type, inline, resolve, lower, {@code new DuckDb().render} —
 * which was fine for asking "does a planner survive the compile" but
 * is exactly wrong for shipping. The server's upstream
 * {@code pure/v1/execution/generatePlan} calls {@code Compiler.plan} (on the request's
 * already-parsed lambda: the same phases from name resolution on), so
 * anything else here would be a SECOND planner that has to agree with
 * the first about dialect selection, null ordering and aggregate
 * semantics — the divergence class this project exists to avoid.
 *
 * <p>Going through {@code Compiler} also pulls the runtime in, which
 * the hand-rolled version silently skipped: the dialect comes from the
 * Pure {@code Connection} ELEMENT that the named {@code Runtime}
 * resolves to, not from a hardcoded {@code new DuckDb()}.
 *
 * <p>That makes this class a live test of a subtle property.
 * {@code Compiler} keeps {@code java.sql.Connection} in six method
 * DESCRIPTORS (its execute overloads) while carrying no
 * {@code java.sql} catch clause — the distinction
 * {@code PlannerNeedsOnlyJavaBaseTest} pins, because the JVM verifier
 * resolves handler types at link time and descriptors lazily. An
 * ahead-of-time compiler does its own whole-program analysis and need
 * not honour that distinction at all. If this module builds, it does.
 */
public final class Wasm {

    private Wasm() {
    }

    /** The demo model, verbatim from {@code datacube/demo/trades.pure}. */
    private static final String MODEL = """
            ###Relational
            Database trades::DB
            (
                Table TRADES
                (
                    region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32),
                    year INTEGER, qtr VARCHAR(8),
                    notional DOUBLE, pnl DOUBLE, qty INTEGER
                )
            )

            ###Connection
            RelationalDatabaseConnection trades::Conn
            {
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }

            ###Runtime
            Runtime trades::RT
            {
                mappings: [];
                connections:
                [
                    trades::DB: [ c1: trades::Conn ]
                ];
            }
            """;

    private static final String RUNTIME = "trades::RT";

    private static final String QUERY = "#>{trades::DB.TRADES}#"
            + "->filter(x|$x.region == 'EMEA')"
            + "->select(~[region, desk, notional])"
            + "->groupBy(~[region, desk], ~[total:x|$x.notional:y|$y->sum()])"
            + "->sort([~region->ascending()])->limit(10)";

    /**
     * The planner, end to end — the SAME {@code Compiler.plan} the server's
     * {@code pure/v1/execution/generatePlan} calls, so the browser plane and the server plane cannot drift.
     */
    @org.teavm.jso.JSExport
    public static String plan(String model, String query, String runtime) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).plan(runtime).sql();
    }

    /**
     * The plan AND its result's type: {@code {"sql": ..., "type": RelationType}}, the type
     * in legend-engine's {@code lambdaRelationType} shape through the one renderer the
     * server's {@code pure/v1} answers use ({@code UpstreamRelationType}) -- so the browser
     * reads a column's type from the compiler, never from the engine's wire
     * (docs/DATACUBE_TYPED_VALUES_DESIGN_2026_09_27.md, step 1).
     */
    public static String planTyped(String model, String query, String runtime) {
        com.legend.plan.QueryPlan p = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).plan(runtime);
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("sql", p.sql());
        out.put("type", com.legend.plan.UpstreamRelationType.of(p.rootType()));
        return com.legend.json.Json.toCompact(out);
    }

    /**
     * {@link #planTyped} with the failure path folded into the RETURN VALUE.
     *
     * <p>A refusal is an answer the planner is expected to give, so the
     * differential has to compare refusals too — and comparing them
     * through the return value rather than through a thrown exception
     * keeps the two targets honest without depending on how TeaVM
     * bridges Java throwables into JS.
     */
    @org.teavm.jso.JSExport
    public static String planOrError(String model, String query, String runtime) {
        try {
            return "OK\n" + planTyped(model, query, runtime);
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    /**
     * A query's result type, compile-only: upstream {@code lambdaRelationType}'s answer
     * ({@code UpstreamRelationType}), no runtime, no lowering. How the tab types a cube's
     * source and calculated columns before any level query runs. Failures fold into the
     * return value as {@link #planOrError}'s do.
     */
    @org.teavm.jso.JSExport
    public static String relationTypeOrError(String model, String query) {
        try {
            return "OK\n" + com.legend.json.Json.toCompact(com.legend.plan.UpstreamRelationType.of(
                    com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).resultType()));
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    // ---- the protocol-JSON entries: each the in-tab twin of a pure/v1 endpoint, the same
    // ---- core call behind it (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T4a)

    /** A request's lambda JSON: the depth pure/v1 allows ({@code PureV1Api.REQUEST}). */
    private static com.legend.protocol.spec.LambdaFunction lambdaOf(String lambdaJson) {
        com.legend.json.Json.Node n = com.legend.json.Json.parse(lambdaJson, new com.legend.json.Json.Config(1024));
        if (!(n instanceof com.legend.json.Json.Obj o)) {
            throw new IllegalArgumentException("lambda JSON: not a JSON object");
        }
        return com.legend.protocol.ProtocolReader.lambda(o);
    }

    private static String folded(Throwable e) {
        return "ERR\n" + e.getClass().getName() + "\n" + (e.getMessage() == null ? "" : e.getMessage());
    }

    /** E9's twin: a lambda's protocol JSON planned -- {@code {"sql","type"}}, as {@link #planOrError}. */
    @org.teavm.jso.JSExport
    public static String planJsonOrError(String model, String lambdaJson, String runtime) {
        try {
            com.legend.plan.QueryPlan p = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambdaOf(lambdaJson)).plan(runtime);
            java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("sql", p.sql());
            out.put("type", com.legend.plan.UpstreamRelationType.of(p.rootType()));
            return "OK\n" + com.legend.json.Json.toCompact(out);
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /** E5's twin: a lambda's protocol JSON typed, compile-only -- the {@code RelationType}. */
    @org.teavm.jso.JSExport
    public static String relationTypeJsonOrError(String model, String lambdaJson) {
        try {
            return "OK\n" + com.legend.json.Json.toCompact(com.legend.plan.UpstreamRelationType.of(
                    com.legend.Compiler.query(com.legend.Compiler.compileModel(model), lambdaOf(lambdaJson)).resultType()));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /** E4's twin: a lambda's protocol JSON as Pure text, {@code STANDARD} or {@code PRETTY}. */
    @org.teavm.jso.JSExport
    public static String composeLambdaOrError(String lambdaJson, String style) {
        try {
            com.legend.json.Json.Node n = com.legend.json.Json.parse(lambdaJson, new com.legend.json.Json.Config(1024));
            if (!(n instanceof com.legend.json.Json.Obj o)) {
                throw new IllegalArgumentException("lambda JSON: not a JSON object");
            }
            return "OK\n" + com.legend.protocol.PureComposer.lambda(o, "STANDARD".equals(style)
                    ? com.legend.protocol.PureComposer.Style.STANDARD : com.legend.protocol.PureComposer.Style.PRETTY);
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * {@code jsonToGrammar/model}'s twin (docs/STUDIO_FULL_PLAN_2026_10_04.md, B1): a model's protocol JSON
     * ({@code {"_type":"data","elements":[...]}}, with or without its section index) as Pure text, byte for
     * byte as legend-engine prints it, or the refusal naming the element kind lite cannot print yet. The JSON is
     * read first by the model reader, which refuses a field it cannot carry or one of the wrong kind, by name: the
     * composer prints from the JSON and would pass over either (the protocol program's leg 2 composes from the
     * records instead).
     */
    @org.teavm.jso.JSExport
    public static String jsonToGrammarModelOrError(String modelJson) {
        try {
            com.legend.json.Json.Node n = com.legend.json.Json.parse(modelJson, new com.legend.json.Json.Config(4096));
            if (!(n instanceof com.legend.json.Json.Obj o)) {
                throw new IllegalArgumentException("model JSON: not a JSON object");
            }
            com.legend.protocol.ModelReader.read(o);
            return "OK\n" + com.legend.protocol.ModelComposer.model(o);
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * E1's twin: Pure text to its lambda JSON, without source information (text without a
     * leading {@code |} is wrapped in a parameterless lambda, as the engine does). How a
     * user-typed fragment -- a calculated column, a custom filter -- joins a query built as JSON.
     */
    @org.teavm.jso.JSExport
    public static String lambdaJsonOrError(String text) {
        try {
            return "OK\n" + com.legend.protocol.SourceInformation.strip(
                    com.legend.protocol.ProtocolEmitter.emitLambda(com.legend.parser.SpecParser.parseLambda(text)));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * A model's test data as the SQL the server seeds a database with, for the tab's DuckDB
     * (docs/STUDIO_FULL_PLAN_2026_10_04.md A2, "The tab's tables are the server's"): {@code tablesJson}
     * is {@code [{schema, table, csv}, ...]}, each a table the Database {@code database} declares (one it
     * does not is refused, by name) and its CSV, header first. For each, in order: {@code sql}, the
     * statements {@code CsvSeed.sqls} gives DuckDB -- the schema, the table made with the server's column
     * types, its rows as one multi-row INSERT (none for a header alone); and the names the tab needs to
     * reach that table again, as the same dialect spells them, so the tab spells none itself:
     * {@code table} (no schema for the {@code default} one, as the seed makes it), {@code drop}, and
     * {@code columns}, each its declared {@code name} (what a file's header says) and its {@code sql}
     * name. The model is compiled once per call (one Database's tables). {@code "OK\n" + [...]}, or the
     * folded refusal.
     */
    @org.teavm.jso.JSExport
    public static String testDataSqlOrError(String model, String database, String tablesJson) {
        try {
            com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(model);
            com.legend.sql.dialect.DuckDb duckDb = new com.legend.sql.dialect.DuckDb();
            if (!(com.legend.json.Json.parse(tablesJson) instanceof com.legend.json.Json.Arr tables)) {
                throw new IllegalArgumentException("test data: the tables are not a JSON array");
            }
            java.util.List<Object> out = new java.util.ArrayList<>();
            for (com.legend.json.Json.Node node : tables.items()) {
                if (!(node instanceof com.legend.json.Json.Obj t)
                        || !(t.getOr("schema", null) instanceof com.legend.json.Json.Str schemaNode)
                        || !(t.getOr("table", null) instanceof com.legend.json.Json.Str tableNode)
                        || !(t.getOr("csv", null) instanceof com.legend.json.Json.Str csvNode)) {
                    throw new IllegalArgumentException("test data: a table is not {schema, table, csv}, each a string");
                }
                String schema = schemaNode.value();
                String table = tableNode.value();
                boolean defaultSchema = "default".equals(schema);
                com.legend.model.DatabaseDefinition.TableDefinition def = ctx
                        .findTableDefinition(database, defaultSchema ? table : schema + "." + table)
                        .orElseThrow(() -> new IllegalArgumentException(
                                "test data: the Database " + database + " declares no table " + schema + "." + table));
                java.util.Map<String, Object> seed = new java.util.LinkedHashMap<>();
                seed.put("sql", new java.util.ArrayList<>(com.legend.setup.CsvSeed.sqls(
                        schema + "\n" + table + "\n" + csvNode.value(), database, ctx, duckDb)));
                seed.put("table", defaultSchema ? duckDb.physicalName(table)
                        : duckDb.physicalName(schema) + "." + duckDb.physicalName(table));
                seed.put("drop", duckDb.render(com.legend.setup.Ddl.dropTable(defaultSchema ? null : schema, table)));
                java.util.List<Object> columns = new java.util.ArrayList<>();
                for (com.legend.model.DatabaseDefinition.ColumnDefinition c : def.columns()) {
                    java.util.Map<String, Object> column = new java.util.LinkedHashMap<>();
                    column.put("name", c.name());
                    column.put("sql", duckDb.physicalName(c.name()));
                    columns.add(column);
                }
                seed.put("columns", columns);
                out.add(seed);
            }
            return "OK\n" + com.legend.json.Json.toCompact(out);
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * C1's twin, for Studio's in-tab compile (docs/STUDIO_DESIGN_2026_10_02.md S4): exactly what the
     * server's {@code compilation/compile} does -- the model's elements ({@code Compiler.compileModel},
     * which refuses on the first element error), then every body in it
     * ({@code Compiler.compileAllBodies}, which collects them all). {@code "OK\n" + [message, ...]}
     * (empty when it compiles), or the folded refusal.
     */
    @org.teavm.jso.JSExport
    public static String compileOrError(String model) {
        try {
            java.util.Map<String, String> walls = com.legend.Compiler.compileAllBodies(com.legend.Compiler.compileModel(model));
            return "OK\n" + com.legend.json.Json.toCompact(new java.util.ArrayList<>(walls.values()));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * E2's twin: a model's text to its PMCD JSON ({@code {"_type":"data","elements":[...]}}),
     * without source information -- the same {@code PmcdParser.parseDocument} the server's
     * {@code grammar/grammarToJson/model} calls. How a browser app browses a model's classes,
     * mappings, data spaces and services without a server (docs/QUERY_APP_DESIGN_2026_09_30.md G11).
     */
    @org.teavm.jso.JSExport
    public static String modelJsonOrError(String text) {
        try {
            return "OK\n" + com.legend.protocol.SourceInformation.strip(
                    com.legend.parser.PmcdParser.parseDocument(text));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * A Pure Database from a DuckDB table's CATALOG (T2): the rows {@code DESCRIBE} reports,
     * read by legend-lite's DuckDB dialect -- the declared types, and the conversions the
     * source must apply, or the columns left out when it cannot.
     * {@code {"path", "schema"?, "table", "convertible", "columns": [{"name","type"}]}} in;
     * {@code "OK\n" + {"text", "source" (protocol), "conversions": [{"column","sql"}], "excluded": [name]}}
     * or the refusal out, as
     * {@link #planOrError}'s are folded.
     */
    /** A JSON number field as an Integer, or null when absent or null. */
    private static Integer intOrNull(com.legend.json.Json.Obj o, String field) {
        com.legend.json.Json.Node n = o.has(field) ? o.get(field) : null;
        return n instanceof com.legend.json.Json.Num num ? Integer.valueOf((int) num.longValue()) : null;
    }

    @org.teavm.jso.JSExport
    public static String databaseFromCatalogOrError(String catalogJson) {
        try {
            com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(catalogJson);
            com.legend.sql.dialect.CatalogModel.Database db = com.legend.sql.dialect.CatalogModel.database(
                    in.getString("path"), in.getStringOr("schema", null), in.getString("table"), catalogColumns(in),
                    dialectOf(in.getString("databaseType")), in.getBool("convertible"));
            java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("text", db.text());
            out.put("source", sourceOf(db));
            out.put("conversions", conversionsOf(db));
            out.put("excluded", db.excluded());
            return "OK\n" + com.legend.json.Json.toCompact(out);
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    /** A table's catalog rows, structured as the catalog question answers them (DuckDb.CATALOG_COLUMNS_SQL): no type
     *  string parsed. */
    private static java.util.List<com.legend.sql.dialect.CatalogModel.Column> catalogColumns(com.legend.json.Json.Obj in) {
        java.util.List<com.legend.sql.dialect.CatalogModel.Column> columns = new java.util.ArrayList<>();
        for (com.legend.json.Json.Node n : in.getArr("columns").items()) {
            com.legend.json.Json.Obj c = (com.legend.json.Json.Obj) n;
            columns.add(new com.legend.sql.dialect.CatalogModel.Column(c.getString("name"), c.getString("dataType"),
                    c.getStringOr("logicalType", null), intOrNull(c, "precision"), intOrNull(c, "scale"),
                    c.getBoolOr("notNull", false)));
        }
        return columns;
    }

    /** The table's own database reads its catalog (a Postgres table's, Postgres's rules). */
    private static com.legend.sql.dialect.SqlDialect dialectOf(String databaseType) {
        return com.legend.database.Databases.dialect(com.legend.database.Databases.named(databaseType));
    }

    /** The relation that reads the table, as protocol: the compiler's parse of its own accessor. */
    private static Object sourceOf(com.legend.sql.dialect.CatalogModel.Database db) {
        com.legend.json.Json.Obj lambda = com.legend.json.Json.parseObject(com.legend.protocol.SourceInformation.strip(
                com.legend.protocol.ProtocolEmitter.emitLambda(com.legend.parser.SpecParser.parseLambda("|" + db.accessor()))));
        return lambda.getArr("body").items().get(0);
    }

    private static java.util.List<java.util.Map<String, Object>> conversionsOf(com.legend.sql.dialect.CatalogModel.Database db) {
        java.util.List<java.util.Map<String, Object>> conversions = new java.util.ArrayList<>();
        for (com.legend.sql.dialect.CatalogModel.Conversion c : db.conversions()) {
            java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("column", c.column());
            m.put("sql", c.sql());
            conversions.add(m);
        }
        return conversions;
    }

    /**
     * THE MODEL FOR A TABLE, from its catalog rows: what a host plans a table it holds against -- DataCube's file or
     * warehouse table, a Python frame. The Database (CatalogModel), wrapped in a connection and a runtime the
     * planner compiles, and, when the table can be snapped, a second runtime over the SAME Database through a
     * connection of the copy's store's type (the rows pulled with a plan against the table's own runtime; every query
     * on the copy planned against this one).
     * {@code {"table", "schema"?, "pkg"? (default "local"), "convertible", "databaseType", "snapDatabaseType"?,
     * "columns": [catalog rows]}} in;
     * {@code "OK\n" + {"model", "runtime", "snapRuntime"?, "source" (protocol), "accessor" (its Pure text),
     * "conversions": [{"column","sql"}], "copySelectList", "excluded": [name], "bitColumns": [name]}} or the refusal
     * out. {@code bitColumns}: the
     * columns declared BIT (DuckDB's BOOLEAN), which legend-engine types TinyInt.
     */
    @org.teavm.jso.JSExport
    public static String tableModelOrError(String tableJson) {
        try {
            com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(tableJson);
            String pkg = in.getStringOr("pkg", "local");
            String databaseType = in.getString("databaseType");
            String snapDatabaseType = in.getStringOr("snapDatabaseType", null);
            java.util.List<com.legend.sql.dialect.CatalogModel.Column> columns = catalogColumns(in);
            com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
            com.legend.sql.dialect.CatalogModel.Database db = com.legend.sql.dialect.CatalogModel.database(
                    pkg + "::DB", in.getStringOr("schema", null), in.getString("table"), columns, dialect,
                    in.getBool("convertible"));
            String model = db.text() + "\n" + connectionAndRuntime(pkg, "Conn", "RT", databaseType)
                    + (snapDatabaseType == null ? "" : "\n" + connectionAndRuntime(pkg, "SnapConn", "SnapRT", snapDatabaseType));
            java.util.List<String> bitColumns = new java.util.ArrayList<>();
            for (com.legend.sql.dialect.CatalogModel.Column c : columns) {
                if (!db.excluded().contains(c.name()) && "BIT".equals(dialect.catalogType(c).declared())) {
                    bitColumns.add(c.name());
                }
            }
            java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("model", model);
            out.put("runtime", pkg + "::RT");
            if (snapDatabaseType != null) {
                out.put("snapRuntime", pkg + "::SnapRT");
            }
            out.put("source", sourceOf(db));
            out.put("accessor", db.accessor());
            out.put("conversions", conversionsOf(db));
            out.put("copySelectList", db.copySelectList());
            out.put("excluded", db.excluded());
            out.put("bitColumns", bitColumns);
            return "OK\n" + com.legend.json.Json.toCompact(out);
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    /** A connection of the given type and the runtime that binds the table's Database to it. Every connection is a
     *  DuckDB specification: the host runs the SQL on its own engine, and the planner reads only the type. */
    private static String connectionAndRuntime(String pkg, String connection, String runtime, String databaseType) {
        return "###Connection\nRelationalDatabaseConnection " + pkg + "::" + connection + "\n{\n    type: " + databaseType
                + ";\n    specification: DuckDB { };\n    auth: Test;\n}\n\n###Runtime\nRuntime " + pkg + "::" + runtime
                + "\n{\n    mappings: [];\n    connections:\n    [\n        " + pkg + "::DB: [ c1: " + pkg + "::" + connection
                + " ]\n    ];\n}\n";
    }

    /**
     * THE CATALOG QUESTION for one table of a DuckDB (DuckDb.CATALOG_COLUMNS_SQL), its schema and table filled in as
     * SQL string literals: run it, and its rows are what {@link #tableModelOrError} reads. {@code "OK\n" + sql}.
     */
    @org.teavm.jso.JSExport
    public static String catalogColumnsSqlOrError(String schema, String table) {
        try {
            // each placeholder filled by its place in the template, so a name holding "{table}" stays a name
            String template = com.legend.sql.dialect.DuckDb.CATALOG_COLUMNS_SQL;
            int s = template.indexOf("{schema}");
            int t = template.indexOf("{table}");
            if (s < 0 || t < s) {
                throw new IllegalStateException("the catalog question names no {schema} before {table}");
            }
            return "OK\n" + template.substring(0, s) + sqlLiteral(schema) + template.substring(s + "{schema}".length(), t)
                    + sqlLiteral(table) + template.substring(t + "{table}".length());
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    /**
     * What a session of the given database runs before it is queried, so it answers as the planner's SQL expects
     * (the dialect's own setup: a DuckDB session in UTC). {@code "OK\n" + [statement, ...]}.
     */
    @org.teavm.jso.JSExport
    public static String sessionSetupOrError(String databaseType) {
        try {
            return "OK\n" + com.legend.json.Json.toCompact(dialectOf(databaseType).sessionSetup());
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    // ---- legend-engine's pure/v1 API as legend-lite's server answers it (PureV1Api, the plan side): how Python's
    // ---- engine answers DataCube (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md). Not exported to the tab: the
    // ---- native library's (//native:compiler)

    /** An answer as text: {@code "OK\n" + status + "\n" + media type + "\n" + body}. */
    private static String http(com.legend.server.PureV1Api.Answer a) {
        return "OK\n" + a.status() + "\n" + a.contentType() + "\n" + a.json();
    }

    /**
     * One {@code pure/v1} call by its path and raw query string ({@code ""} for none), routed and answered as
     * legend-lite's server answers it. An execute's run is the host's (its rows come from
     * {@link #executePlanOrError}), so one asked for here is refused, naming the format that is served.
     */
    public static String pureV1OrError(String path, String rawQuery, String body) {
        try {
            return http(com.legend.server.PureV1Api.route(path, rawQuery.isEmpty() ? null : rawQuery, body,
                    (model, lambda, runtime, rows) -> {
                        throw new IllegalArgumentException("this engine answers execute in upstream's Arrow format: "
                                + "ask with ?serializationFormat=ARROW_IPC");
                    }));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /**
     * Execute's plan half in upstream's Arrow format (PureV1Api.arrowPlan): the SQL to run and the Arrow schema
     * metadata, or the refusal to send. {@code modelsJson}: the only models the host runs, a JSON array of their
     * texts.
     */
    public static String executePlanOrError(String body, String modelsJson) {
        try {
            java.util.List<String> models = new java.util.ArrayList<>();
            if (!(com.legend.json.Json.parse(modelsJson) instanceof com.legend.json.Json.Arr arr)) {
                throw new IllegalArgumentException("the models: not a JSON array");
            }
            for (com.legend.json.Json.Node n : arr.items()) {
                if (!(n instanceof com.legend.json.Json.Str text)) {
                    throw new IllegalArgumentException("the models: an entry that is not a model's text");
                }
                models.add(text.value());
            }
            return http(com.legend.server.PureV1Api.arrowPlan(body, models));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    /** A host's refusal of a call it could not finish (its database refusing the SQL), in the engine's shape. */
    public static String refusalOrError(String message) {
        try {
            return http(com.legend.server.PureV1Api.refused(message));
        } catch (RuntimeException | StackOverflowError e) {
            return folded(e);
        }
    }

    private static String sqlLiteral(String text) {
        return "'" + text.replace("'", "''") + "'";
    }

    /**
     * Force {@code Prelude}'s static initialiser and nothing else.
     *
     * <p>Cold start is ~550ms against ~10ms warm, and "the first plan
     * is slow" is not an actionable statement. Parsing
     * {@code prelude.pure} — 300 KB, 7,009 lines — happens in a
     * static initialiser, so timing a call that touches ONLY that
     * class separates it from the rest of the first plan. Whether
     * pre-baking the boot layer is worth building depends entirely on
     * which side of that split the milliseconds are on.
     *
     * @return the element count, so the call cannot be optimised away
     */
    @org.teavm.jso.JSExport
    public static int touchPrelude() {
        return com.legend.builtin.Prelude.elementFqns().size();
    }

    /** {@link #touchPrelude}'s twin for the system metamodel. */
    @org.teavm.jso.JSExport
    public static int touchSystemMetamodel() {
        return com.legend.builtin.SystemMetamodel.elements().size();
    }

    /**
     * Build everything a first plan would build, and throw it away.
     *
     * <p>Not a probe — the browser calls this. Loading the module is
     * NOT warming it: instantiate costs ~44ms, while the work that
     * actually makes a first plan slow happens in static initialisers
     * and a content-addressed cache that a plan touches on its way
     * past. Measured from the page, that is parsing the 300 KB Pure
     * prelude, the system metamodel, then resolving and normalizing
     * both into the boot layer — together ~1.1s of the ~1.3s.
     *
     * <p>`WasmPlanner.warmUp` calls this while DuckDB is still
     * starting, so the cost lands inside a wait the page was making
     * anyway instead of after it. {@code compileModel} is the public
     * door to all of it, and takes the real model so the graph it
     * builds is the one the first plan wants.
     *
     * @return 1, so the call cannot be optimised away
     */
    @org.teavm.jso.JSExport
    public static int warmModel(String model) {
        return com.legend.Compiler.compileModel(model) == null ? 0 : 1;
    }

    /**
     * Time the boot layer's own SHA-256, alone.
     *
     * <p>{@code Compiler.bootLayer} content-addresses its cache by
     * hashing ~500 KB of Pure source, and it does so on EVERY call,
     * cache hit included. The digest is {@code com.legend.cache.Sha256},
     * hand-rolled in Java because TeaVM has no {@code java.security} —
     * so unlike on a JVM there is no native implementation underneath
     * it. Worth knowing what that costs before assuming the time is
     * all in the normalizer.
     *
     * @return the hex digest's length, so nothing is optimised away
     */
    @org.teavm.jso.JSExport
    public static int hashBootSource() {
        String source = com.legend.builtin.SystemMetamodel.source() + "\n"
                + com.legend.builtin.Prelude.source();
        return com.legend.cache.Hash.ofUtf8(source).hex().length();
    }

    /**
     * Resolve the boot layer's names, WITHOUT normalizing.
     *
     * <p>Splits {@code bootLayer}'s ~570ms into its two halves. A
     * persisted cache of the normalizer's output would have to
     * reproduce whichever of them dominates, so the split decides
     * whether such a cache is worth building at all.
     */
    @org.teavm.jso.JSExport
    public static int resolveBootLayer() {
        com.legend.model.ParsedModel pre =
                com.legend.builtin.SystemMetamodel.withoutSystemShadows(
                        com.legend.builtin.Prelude.parsedModel());
        java.util.List<com.legend.model.PackageableElement> elements =
                new java.util.ArrayList<>(
                        com.legend.builtin.SystemMetamodel.elements());
        elements.addAll(pre.elements());
        com.legend.model.ParsedModel boot = new com.legend.model.ParsedModel(
                elements, com.legend.model.ImportScope.empty(), null,
                pre.elementOffsets(), pre.elementImports(), pre.elementSources());
        return com.legend.compiler.NameResolver.resolve(boot).elements().size();
    }

    /**
     * The one {@code ZoneId.of} on the planner's path
     * ({@code LiteralSpelling.inZone}, the engine's dbTimeZone literal
     * rule). A timezone database is a RESOURCE, not code, so whether it
     * survives the WASM build is a question the census cannot answer by
     * reading — only by asking the built module.
     */
    @org.teavm.jso.JSExport
    public static String zoneProbe(String utcIso, String zone) {
        try {
            return com.legend.lowering.LiteralSpelling.inZone(utcIso, zone);
        } catch (RuntimeException e) {
            return "ERR " + e.getClass().getName() + ": " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        System.out.println(plan(MODEL, QUERY, RUNTIME));
    }
}
