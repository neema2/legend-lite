package planner;

/**
 * legend-lite's BOUNDARY for the hosts that embed it -- the tab (TeaVM, {@link TabExports}) and Python (the C library,
 * native/'s {@code nativelib.Compiler}): one class, each operation once, plain Java -- strings in, a string or an
 * answer out, or an exception -- with no host's annotations and no text encoding (each host's adapter is one-line
 * delegations through {@link Folded}; docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5). legend-engine's operations
 * are offered only as {@link #pureV1}, the code legend-lite's server answers them with; the rest are lite's own.
 *
 * <p>Pure text in, SQL text out. No executor, no server, no JDBC: the planner packages depend on java.base alone
 * (jdeps), and the whole pipeline was already proved to run under {@code --limit-modules java.base}. The same source
 * is compiled to WebAssembly and to a native library, and the JVM differential compares the builds' answers
 * ({@code JvmMain}) rather than two hand-kept copies that could drift apart silently.
 *
 * <h2>Why planning calls {@code Compiler.plan} and nothing else</h2>
 *
 * <p>The first version of this boundary hand-assembled the pipeline -- parse, type, inline, resolve, lower,
 * {@code new DuckDb().render} -- which was fine for asking "does a planner survive the compile" but is exactly wrong
 * for shipping. The server's upstream {@code pure/v1/execution/generatePlan} calls {@code Compiler.plan} (on the
 * request's already-parsed lambda: the same phases from name resolution on), so anything else here would be a SECOND
 * planner that has to agree with the first about dialect selection, null ordering and aggregate semantics -- the
 * divergence class this project exists to avoid.
 *
 * <p>Going through {@code Compiler} also pulls the runtime in, which the hand-rolled version silently skipped: the
 * dialect comes from the Pure {@code Connection} ELEMENT that the named {@code Runtime} resolves to, not from a
 * hardcoded {@code new DuckDb()}.
 *
 * <p>That makes the WebAssembly build a live test of a subtle property. {@code Compiler} keeps
 * {@code java.sql.Connection} in six method DESCRIPTORS (its execute overloads) while carrying no {@code java.sql}
 * catch clause -- the distinction {@code PlannerNeedsOnlyJavaBaseTest} pins, because the JVM verifier resolves handler
 * types at link time and descriptors lazily. An ahead-of-time compiler does its own whole-program analysis and need
 * not honour that distinction at all. If the module builds, it does.
 */
public final class Boundary {

    private Boundary() {
    }

    // ---- legend-engine's pure/v1 API, as legend-lite's server answers it (PureV1Api, the plan side): how the tab's
    // ---- apps ask what they would ask a server, and how Python's engine answers DataCube
    // ---- (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md)

    /**
     * One {@code pure/v1} call by its path and raw query string ({@code ""} for none), routed and answered as
     * legend-lite's server answers it -- lite's own {@code /api/lite/v1/compilation/compile} included. An execute's
     * run is the host's (its rows come from {@link #executePlan}), so one asked for here is refused, naming the
     * format that is served.
     */
    public static com.legend.server.PureV1Api.Answer pureV1(String path, String rawQuery, String body) {
        return com.legend.server.PureV1Api.route(path, rawQuery.isEmpty() ? null : rawQuery, body,
                (query, runtime, sinkOf) -> {
                    throw new IllegalArgumentException("this engine runs no execute itself: the host runs the plan"
                            + " (executePlan) and asks for its answer (executeJsonAnswer, executeArrowAnswer)");
                }, HELD);
    }

    /** A request's model given as text: the one this host holds for that text, compiled no more, else compiled for the
     *  request (HeldModels.withText). */
    private static final com.legend.server.PureV1Api.Models HELD = new com.legend.server.PureV1Api.Models() {
        @Override
        public <T> T with(String text, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use) {
            return MODELS.withText(text, use);
        }
    };

    /**
     * Execute's plan half in upstream's Arrow format (PureV1Api.arrowPlan): the SQL to run and the Arrow schema
     * metadata, or the refusal to send. {@code modelsJson}: the only models the host runs, a JSON array of their
     * texts.
     */
    public static com.legend.server.PureV1Api.Answer executePlan(String body, String modelsJson) {
        return com.legend.server.PureV1Api.arrowPlan(body, models(modelsJson));
    }

    /**
     * Execute's plan half in the engine's JSON (PureV1Api.jsonPlan): the statement to run, the answer's shape and its
     * result columns with their type facts, and everything the answer holds but its rows -- the TDS builder, the
     * activities, the columns' names, or a graph fetch's JSON builder -- for a host whose database runs the statement
     * (the tab; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1).
     * {@code modelsJson}: the only models the host runs, a JSON array of their texts.
     */
    public static com.legend.server.PureV1Api.Answer executeJsonPlan(String body, String modelsJson) {
        return com.legend.server.PureV1Api.jsonPlan(body, models(modelsJson));
    }

    /**
     * Execute's answer for a host that ran {@link #executeJsonPlan}'s {@code run} statement (PureV1Api.jsonAnswer):
     * that plan's answer and the host's rows (a JSON array of objects, each a column's name and its cell as the
     * host's driver read it), decoded by the plan's columns and written by the writer legend-lite's server answers
     * with.
     */
    public static com.legend.server.PureV1Api.Answer executeJsonAnswer(String plan, String rows) {
        return com.legend.server.PureV1Api.jsonAnswer(plan, rows);
    }

    /**
     * Execute's answer in upstream's Arrow format for a host that ran {@link #executeJsonPlan}'s {@code run}
     * statement (PureV1Api.arrowAnswer; track A, 2026-10-29): that plan's answer and the host's rows, decoded by the
     * plan's columns and written by lite's Arrow writer as the one zstd frame upstream sends -- folded as base64
     * (Folded.http).
     */
    public static com.legend.server.PureV1Api.Answer executeArrowAnswer(String plan, String rows) {
        return com.legend.server.PureV1Api.arrowAnswer(plan, rows);
    }

    /** The models a host runs, from their JSON array of texts. */
    private static java.util.List<String> models(String modelsJson) {
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
        return models;
    }

    /** A host's refusal of a call it could not finish (its database refusing the SQL), in the engine's shape. */
    public static com.legend.server.PureV1Api.Answer refusal(String message) {
        return com.legend.server.PureV1Api.refused(message);
    }

    // ---- planning and typing

    /**
     * The planner, end to end -- the SAME {@code Compiler.plan} the server's {@code pure/v1/execution/generatePlan}
     * calls, so the browser plane and the server plane cannot drift: the SQL alone.
     */
    public static String planSql(String model, String query, String runtime) {
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).plan(runtime).sql();
    }

    /**
     * A query written as Pure TEXT planned: {@code {"sql": ..., "type": RelationType}}, the type in legend-engine's
     * {@code lambdaRelationType} shape through the one renderer the server's {@code pure/v1} answers use
     * ({@code UpstreamRelationType}) -- so a host reads a column's type from the compiler, never from the engine's
     * wire (docs/DATACUBE_TYPED_VALUES_DESIGN_2026_09_27.md, step 1).
     */
    public static String plan(String model, String query, String runtime) {
        return planned(com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).plan(runtime));
    }

    /**
     * A query written as Pure TEXT, typed compile-only: upstream {@code lambdaRelationType}'s answer
     * ({@code UpstreamRelationType}), no runtime, no lowering. How the tab types a cube's source and calculated
     * columns before any level query runs.
     */
    public static String relationType(String model, String query) {
        return com.legend.json.Json.toCompact(com.legend.plan.UpstreamRelationType.of(
                com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).resultType()));
    }

    /**
     * A lambda's protocol JSON planned -- {@code {"sql","type"}}, as {@link #plan} (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T4a).
     * Not {@code execution/generatePlan}'s twin (that answers legend-engine's execution plan), so it stays the
     * boundary's own; a lambda typed alone is {@code compilation/lambdaRelationType}, asked through {@link #pureV1},
     * and a whole model compiled is {@code compilation/compile} on either route (docs/PROTOCOL_PROGRAM_2026_10_05.md,
     * leg 6).
     */
    public static String planJson(String model, String lambdaJson, String runtime) {
        return planned(com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                com.legend.protocol.ProtocolReader.lambda(lambdaJson)).plan(runtime));
    }

    private static String planned(com.legend.plan.QueryPlan p) {
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("sql", p.sql());
        out.put("type", com.legend.plan.UpstreamRelationType.of(p.rootType()));
        return com.legend.json.Json.toCompact(out);
    }

    // ---- models held: compiled once, planned against many times (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9)

    /** This host's held models: the module's (one per module instance, a worker's own) or Python's library's. */
    private static final com.legend.HeldModels MODELS = new com.legend.HeldModels();

    /** {@link com.legend.HeldModels#hold}: the handle that names {@code model}, compiled the first time it is held. */
    public static int holdModel(String model) {
        return MODELS.hold(model);
    }

    /** {@link com.legend.HeldModels#release}. */
    public static void releaseModel(int handle) {
        MODELS.release(handle);
    }

    /** {@link #plan} over a model held: the query planned, the model compiled no more. */
    public static String planHeld(int handle, String query, String runtime) {
        return MODELS.with(handle, ctx -> planned(com.legend.Compiler.query(ctx, query).plan(runtime)));
    }

    /** {@link #planJson} over a model held. */
    public static String planJsonHeld(int handle, String lambdaJson, String runtime) {
        return MODELS.with(handle, ctx -> planned(com.legend.Compiler.query(ctx,
                com.legend.protocol.ProtocolReader.lambda(lambdaJson)).plan(runtime)));
    }

    /**
     * A lambda's protocol JSON planned over a model held as an EXECUTION PLAN (PlanJson's text, version 19): a program
     * that writes -- a store write's {@code Update}, its count bound into the query that answers it, in one
     * {@code Sequence} -- or a query's one {@code TdsResult}, its answer TYPED rows the host reads by the plan's
     * columns (the result boundary program, 2026-10-27: a plan names no output format; a writer at the edge spells
     * the rows), the plan its host's runner runs step by step (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, an Output
     * into a table; docs/WRITES_DESIGN_2026_10_18.md leg 7).
     */
    public static String executionPlanJsonHeld(int handle, String lambdaJson, String runtime) {
        return MODELS.with(handle, ctx -> com.legend.executionplan.PlanJson.write(com.legend.Compiler.query(ctx,
                com.legend.protocol.ProtocolReader.lambda(lambdaJson)).executionPlan(runtime)));
    }

    /** A lambda's protocol JSON typed, compile-only, over a model held: upstream's {@code lambdaRelationType} answer. */
    public static String relationTypeJsonHeld(int handle, String lambdaJson) {
        return MODELS.with(handle, ctx -> com.legend.json.Json.toCompact(
                com.legend.plan.UpstreamRelationType.of(com.legend.Compiler.query(ctx,
                        com.legend.protocol.ProtocolReader.lambda(lambdaJson)).resultType())));
    }

    /** {@link #relationType} over a model held. */
    public static String relationTypeHeld(int handle, String query) {
        return MODELS.with(handle, ctx -> com.legend.json.Json.toCompact(
                com.legend.plan.UpstreamRelationType.of(com.legend.Compiler.query(ctx, query).resultType())));
    }

    // ---- a model's data and its tables

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
     * name. The model is compiled once per call (one Database's tables).
     */
    public static String testDataSql(String model, String database, String tablesJson) {
        com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(model);
        // the tab's engine is the platform's own (the dialect program's Phase 7, 2026-10-18: constructed by its class
        // until then)
        com.legend.sql.dialect.SqlDialect host =
                com.legend.database.Databases.dialect(com.legend.database.Databases.PLATFORM);
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
                    schema + "\n" + table + "\n" + csvNode.value(), database, ctx, host)));
            seed.put("table", defaultSchema ? host.physicalName(table)
                    : host.physicalName(schema) + "." + host.physicalName(table));
            seed.put("drop", host.render(com.legend.setup.Ddl.dropTable(defaultSchema ? null : schema, table)));
            java.util.List<Object> columns = new java.util.ArrayList<>();
            for (com.legend.model.DatabaseDefinition.ColumnDefinition c : def.columns()) {
                java.util.Map<String, Object> column = new java.util.LinkedHashMap<>();
                column.put("name", c.name());
                column.put("sql", host.physicalName(c.name()));
                columns.add(column);
            }
            seed.put("columns", columns);
            out.add(seed);
        }
        return com.legend.json.Json.toCompact(out);
    }

    /**
     * A Pure Database from a DuckDB table's CATALOG (T2): the rows {@code DESCRIBE} reports, read by legend-lite's
     * DuckDB dialect -- the declared types, and the conversions the source must apply, or the columns left out when it
     * cannot. {@code {"path", "schema"?, "table", "convertible", "databaseType", "columns": [catalog rows]}} in;
     * {@code {"text", "source" (protocol), "conversions": [{"column","sql"}], "excluded": [name]}} out.
     */
    public static String databaseFromCatalog(String catalogJson) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(catalogJson);
        com.legend.sql.dialect.CatalogModel.Database db = com.legend.sql.dialect.CatalogModel.database(
                in.getString("path"), in.getStringOr("schema", null), in.getString("table"), catalogColumns(in),
                dialectOf(in.getString("databaseType")), in.getBool("convertible"));
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("text", db.text());
        out.put("source", sourceOf(db));
        out.put("conversions", conversionsOf(db));
        out.put("excluded", db.excluded());
        return com.legend.json.Json.toCompact(out);
    }

    /**
     * THE MODEL FOR A TABLE, from its catalog rows: what a host plans a table it holds against -- DataCube's file or
     * warehouse table, a Python frame, a Snap's copy (modelled from what the copy holds, as a file read in is:
     * docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3). The Database (CatalogModel), wrapped in a connection and a
     * runtime the planner compiles.
     * {@code {"table", "schema"?, "pkg"? (default "local"), "convertible", "databaseType", "columns": [catalog rows]}}
     * in; {@code {"model", "runtime", "source" (protocol), "accessor" (its Pure text), "conversions":
     * [{"column","sql"}], "copySelectList"?, "excluded": [name], "bitColumns": [name]}} out.
     * {@code copySelectList}: the select list the source applies when it is rewritten in place ({@code convertible}),
     * written by its own database; a read-only source is never rewritten, and has none.
     * {@code bitColumns}: the columns declared BIT (DuckDB's BOOLEAN), which legend-engine types TinyInt.
     *
     * <p>SEVERAL TABLES of one database (a join's two sides, read in one place:
     * docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): {@code {"databaseType", "connection"? (default "host"),
     * "tables": [{"table", "schema"?, "pkg"? (default "local"), "convertible", "columns"}]}} in -- each table in the
     * Database of its package, as its own model declares it, so the relation each side reads is unchanged; one
     * connection, and a runtime binding every one of those Databases to it (lite plans across stores one connection
     * serves). {@code {"model", "runtime", "tables": [{"source", "accessor", "conversions", "excluded"}]}} out, in
     * the order given.
     */
    public static String tableModel(String tableJson) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(tableJson);
        if (in.has("tables")) {
            return tablesModel(in);
        }
        String pkg = in.getStringOr("pkg", "local");
        String databaseType = in.getString("databaseType");
        java.util.List<com.legend.sql.dialect.CatalogModel.Column> columns = catalogColumns(in);
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        com.legend.sql.dialect.CatalogModel.Database db = com.legend.sql.dialect.CatalogModel.database(
                pkg + "::DB", in.getStringOr("schema", null), in.getString("table"), columns, dialect,
                in.getBool("convertible"));
        String model = db.text() + "\n" + connectionAndRuntime(pkg, "Conn", "RT", databaseType);
        java.util.List<String> bitColumns = new java.util.ArrayList<>();
        for (com.legend.sql.dialect.CatalogModel.Column c : columns) {
            if (!db.excluded().contains(c.name()) && "BIT".equals(dialect.catalogType(c).declared())) {
                bitColumns.add(c.name());
            }
        }
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("model", model);
        out.put("runtime", pkg + "::RT");
        out.put("source", sourceOf(db));
        out.put("accessor", db.accessor());
        out.put("conversions", conversionsOf(db));
        if (in.getBool("convertible")) {
            out.put("copySelectList", dialect.copySelectList(db.conversions()));
        }
        out.put("excluded", db.excluded());
        out.put("bitColumns", bitColumns);
        return com.legend.json.Json.toCompact(out);
    }

    /** {@link #tableModel} given several tables: their packages' Databases, one connection, one runtime over them. */
    private static String tablesModel(com.legend.json.Json.Obj in) {
        String databaseType = in.getString("databaseType");
        String connection = in.getStringOr("connection", "host");
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        // each package's tables, in the order the packages first came
        java.util.Map<String, java.util.List<Integer>> byPkg = new java.util.LinkedHashMap<>();
        java.util.List<com.legend.json.Json.Obj> tables = new java.util.ArrayList<>();
        for (com.legend.json.Json.Node n : in.getArr("tables").items()) {
            com.legend.json.Json.Obj t = (com.legend.json.Json.Obj) n;
            byPkg.computeIfAbsent(t.getStringOr("pkg", "local"), k -> new java.util.ArrayList<>()).add(tables.size());
            tables.add(t);
        }
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("a model of several tables has one table at least");
        }
        com.legend.sql.dialect.CatalogModel.Database[] each = new com.legend.sql.dialect.CatalogModel.Database[tables.size()];
        StringBuilder model = new StringBuilder();
        StringBuilder bindings = new StringBuilder();
        int c = 0;
        for (java.util.Map.Entry<String, java.util.List<Integer>> e : byPkg.entrySet()) {
            java.util.List<com.legend.sql.dialect.CatalogModel.Table> mine = new java.util.ArrayList<>();
            for (int i : e.getValue()) {
                com.legend.json.Json.Obj t = tables.get(i);
                mine.add(new com.legend.sql.dialect.CatalogModel.Table(t.getStringOr("schema", null), t.getString("table"),
                        catalogColumns(t), t.getBool("convertible")));
            }
            com.legend.sql.dialect.CatalogModel.Several db =
                    com.legend.sql.dialect.CatalogModel.database(e.getKey() + "::DB", mine, dialect);
            model.append(db.text()).append("\n");
            for (int k = 0; k < e.getValue().size(); k++) {
                each[e.getValue().get(k)] = db.tables().get(k);
            }
            bindings.append(c == 0 ? "" : ",\n        ").append(e.getKey()).append("::DB: [ c").append(++c).append(": ")
                    .append(connection).append("::Conn ]");
        }
        model.append("###Connection\nRelationalDatabaseConnection ").append(connection).append("::Conn\n{\n    type: ")
                .append(databaseType).append(";\n    specification: DuckDB { };\n    auth: Test;\n}\n\n###Runtime\nRuntime ")
                .append(connection).append("::RT\n{\n    mappings: [];\n    connections:\n    [\n        ")
                .append(bindings).append("\n    ];\n}\n");
        java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
        for (com.legend.sql.dialect.CatalogModel.Database db : each) {
            java.util.Map<String, Object> t = new java.util.LinkedHashMap<>();
            t.put("source", sourceOf(db));
            t.put("accessor", db.accessor());
            t.put("conversions", conversionsOf(db));
            t.put("excluded", db.excluded());
            out.add(t);
        }
        java.util.Map<String, Object> answer = new java.util.LinkedHashMap<>();
        answer.put("model", model.toString());
        answer.put("runtime", connection + "::RT");
        answer.put("tables", out);
        return com.legend.json.Json.toCompact(answer);
    }

    /**
     * THE CATALOG QUESTION for one table of the tab's engine, the platform's own
     * ({@code SqlDialect.catalogColumnsQuery}), its schema and table filled in as SQL string literals: run it, and its
     * rows are what {@link #tableModel} reads.
     */
    public static String catalogColumnsSql(String schema, String table) {
        return com.legend.database.Databases.dialect(com.legend.database.Databases.PLATFORM)
                .catalogColumnsQuery(schema, table);
    }

    /**
     * What a session of the given database runs before it is queried, so it answers as the planner's SQL expects
     * (the dialect's own setup: a DuckDB session in UTC). {@code [statement, ...]}.
     */
    public static String sessionSetup(String databaseType) {
        return com.legend.json.Json.toCompact(dialectOf(databaseType).sessionSetup());
    }

    /**
     * A host's named operations on its own database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5,
     * {@code com.legend.sql.dialect.HostOperations}): {@code operationsJson} a JSON array of them (planner's
     * HostOperationsJson reads it), answered {@code [[statement, ...], ...]}, one list per operation, in order -- what
     * the tab and Python's host run instead of spelling SQL. An operation the database has not is refused by name.
     */
    public static String hostOperations(String databaseType, String operationsJson) {
        com.legend.sql.dialect.HostOperations host = com.legend.sql.dialect.HostOperations.of(dialectOf(databaseType));
        java.util.List<Object> out = new java.util.ArrayList<>();
        for (com.legend.sql.dialect.HostOperation op : HostOperationsJson.read(operationsJson)) {
            out.add(host.statements(op));
        }
        return com.legend.json.Json.toCompact(out);
    }

    /**
     * A project's own database on a database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5,
     * {@code com.legend.setup.ProjectData}): where {@code model}'s Data rows live on a host, named by what it holds
     * (the model's Databases and Data rows). {@code {"name", "open", "use", "fill", "close"}}, each but the name the
     * statements in order: make it, point a connection of its own at it, make and fill every table a relational Data
     * element fills (on that connection), drop it. A database without the operations is refused by name. A model this
     * host holds is read as held, compiled no more (HeldModels.withText: a project's model, typed and planned by the
     * page's planner, is 4 MB of the trading model and its Data, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11).
     */
    public static String projectData(String model, String databaseType) {
        com.legend.setup.ProjectData p = MODELS.withText(model,
                ctx -> com.legend.setup.ProjectData.of(ctx, dialectOf(databaseType)));
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("name", p.name());
        out.put("open", p.open());
        out.put("use", p.use());
        out.put("fill", p.fill());
        out.put("close", p.close());
        return com.legend.json.Json.toCompact(out);
    }

    /** A JSON number field as an Integer, or null when absent or null. */
    private static Integer intOrNull(com.legend.json.Json.Obj o, String field) {
        com.legend.json.Json.Node n = o.has(field) ? o.get(field) : null;
        return n instanceof com.legend.json.Json.Num num ? Integer.valueOf((int) num.longValue()) : null;
    }

    /** A table's catalog rows, structured as the catalog question answers them ({@link #catalogColumnsSql}): no type
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
        com.legend.json.Json.Obj lambda = com.legend.json.Json.parseObject(
                com.legend.parser.SpecParser.lambdaJson("|" + db.accessor(), false));
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

    /** A connection of the given type and the runtime that binds the table's Database to it. Every connection is a
     *  DuckDB specification: the host runs the SQL on its own engine, and the planner reads only the type. */
    private static String connectionAndRuntime(String pkg, String connection, String runtime, String databaseType) {
        return "###Connection\nRelationalDatabaseConnection " + pkg + "::" + connection + "\n{\n    type: " + databaseType
                + ";\n    specification: DuckDB { };\n    auth: Test;\n}\n\n###Runtime\nRuntime " + pkg + "::" + runtime
                + "\n{\n    mappings: [];\n    connections:\n    [\n        " + pkg + "::DB: [ c1: " + pkg + "::" + connection
                + " ]\n    ];\n}\n";
    }

    // ---- warming and probes

    /**
     * Build everything a first plan would build, and throw it away.
     *
     * <p>Not a probe -- the browser calls this. Loading the module is NOT warming it: instantiate costs ~44ms, while
     * the work that actually makes a first plan slow happens in static initialisers and a content-addressed cache that
     * a plan touches on its way past. Measured from the page, that is parsing the 300 KB Pure prelude, the system
     * metamodel, then resolving and normalizing both into the boot layer -- together ~1.1s of the ~1.3s.
     *
     * <p>`LitePlanner.warmUp` calls this while DuckDB is still starting, so the cost lands inside a wait the page was
     * making anyway instead of after it. {@code compileModel} is the public door to all of it, and takes the real
     * model so the graph it builds is the one the first plan wants.
     *
     * @return 1, so the call cannot be optimised away
     */
    public static int warmModel(String model) {
        return com.legend.Compiler.compileModel(model) == null ? 0 : 1;
    }

    /**
     * Force {@code Prelude}'s static initialiser and nothing else.
     *
     * <p>Cold start is ~550ms against ~10ms warm, and "the first plan is slow" is not an actionable statement.
     * Parsing {@code prelude.pure} -- 300 KB, 7,009 lines -- happens in a static initialiser, so timing a call that
     * touches ONLY that class separates it from the rest of the first plan. Whether pre-baking the boot layer is worth
     * building depends entirely on which side of that split the milliseconds are on.
     *
     * @return the element count, so the call cannot be optimised away
     */
    public static int touchPrelude() {
        return com.legend.builtin.Prelude.elementFqns().size();
    }

    /** {@link #touchPrelude}'s twin for the system metamodel. */
    public static int touchSystemMetamodel() {
        return com.legend.builtin.SystemMetamodel.elements().size();
    }

    /**
     * Time the boot layer's own SHA-256, alone.
     *
     * <p>{@code Compiler.bootLayer} content-addresses its cache by hashing ~500 KB of Pure source, and it does so on
     * EVERY call, cache hit included. The digest is {@code com.legend.cache.Sha256}, hand-rolled in Java because TeaVM
     * has no {@code java.security} -- so unlike on a JVM there is no native implementation underneath it. Worth
     * knowing what that costs before assuming the time is all in the normalizer.
     *
     * @return the hex digest's length, so nothing is optimised away
     */
    public static int hashBootSource() {
        String source = com.legend.builtin.SystemMetamodel.source() + "\n"
                + com.legend.builtin.Prelude.source();
        return com.legend.cache.Hash.ofUtf8(source).hex().length();
    }

    /**
     * Resolve the boot layer's names, WITHOUT normalizing.
     *
     * <p>Splits {@code bootLayer}'s ~570ms into its two halves. A persisted cache of the normalizer's output would
     * have to reproduce whichever of them dominates, so the split decides whether such a cache is worth building at
     * all.
     */
    public static int resolveBootLayer() {
        // the system layer's protection only: the boot's merge of the platform's own Pure onto the prelude's
        // twins (Compiler.boot, build rebuild Phase 3b item 1b) is not part of this timing split
        com.legend.model.ParsedModel pre =
                com.legend.builtin.SystemMetamodel.requireNoSystemElementRedefined(
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
     * The one {@code ZoneId.of} on the planner's path ({@code LiteralSpelling.inZone}, the engine's dbTimeZone literal
     * rule). A timezone database is a RESOURCE, not code, so whether it survives the WASM build is a question the
     * census cannot answer by reading -- only by asking the built module. The probe's answer is the spelling, or
     * {@code "ERR <class>: <message>"} (the zone differential's own form: ZoneMain answers so on the JVM).
     */
    public static String zoneProbe(String utcIso, String zone) {
        try {
            return com.legend.lowering.LiteralSpelling.inZone(utcIso, zone);
        } catch (RuntimeException e) {
            return "ERR " + e.getClass().getName() + ": " + e.getMessage();
        }
    }
}
