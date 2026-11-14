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
                (query, runtime, output) -> {
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

    /**
     * A text plan as the tab and Python read it: {@code {"sql", "type"}}, and {@code "plan"} -- the execution plan the
     * text renders from (B3, 2026-11-11), its target's loads and its slots, a {@code data:} payload's text its
     * statement's content (which the host binds: plan-params.ts, compiler.plan_params; the tenets audit's M3) -- where
     * the text does not run alone. One a host cannot run ({@code QueryPlan.beyondItsText}) is refused by name here,
     * before any host sends it.
     */
    private static String planned(com.legend.plan.QueryPlan p) {
        String refused = p.beyondItsText();
        if (refused != null) {
            throw new com.legend.error.NotImplementedException("this host runs a plan's statement itself, and this"
                    + " query's plan does not run as its text: " + refused);
        }
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("sql", p.sql());
        out.put("type", com.legend.plan.UpstreamRelationType.of(p.rootType()));
        if (!p.textAlone()) {
            out.put("plan", com.legend.json.Json.parse(com.legend.executionplan.PlanJson.write(p.plan())));
        }
        return com.legend.json.Json.toCompact(out);
    }

    // ---- models held: compiled once, planned against many times (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9)

    /** This host's held models: the module's (one per module instance, a worker's own) or Python's library's. */
    private static final com.legend.HeldModels MODELS = new com.legend.HeldModels();

    /** {@link com.legend.HeldModels#hold}: the handle that names {@code model}, compiled the first time it is held. */
    public static int holdModel(String model) {
        return MODELS.hold(model);
    }

    /** {@link com.legend.HeldModels#hold(String, String)}: the handle that names {@code model} over {@code library}, the
     *  library compiled once per module (Compiler.compileModel's two-argument form, A5 2026-11-06). */
    public static int holdModelOver(String model, String library) {
        return MODELS.hold(model, library);
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
     * A HOST'S OWN FILE READ INTO ITS DATABASE, AS A PLAN (B3, 2026-11-11, the orchestrator's go: uploads and Studio's
     * file puts as planned loads, the file a pointer; until then {@code testDataSql}'s statements and a host's operation
     * naming the file, run outside any plan, census M14): {@code request} is
     * {@code {"databaseType", "file": {"name", "digest", "size", "format"}, "into": {"table"} | {"model", "database",
     * "schema", "table"}}} -- the file by the name the host keeps it under, its bytes' SHA-256 and size, and its format
     * ({@code csv}, {@code parquet}, {@code json}); a table of its own, or a table the model's Database declares. The
     * answer is the plan (PlanJson's), one HOST_SESSION load the host's runner runs ({@code runLoad}), its file step's
     * {@code ?} the file as the host's store binds it ({@code LoadPlanner.hostFile}). A model this host holds is read as
     * held, compiled no more.
     */
    public static String fileLoad(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        String databaseType = in.getString("databaseType");
        com.legend.json.Json.Obj file = in.getObj("file");
        com.legend.json.Json.Obj into = in.getObj("into");
        boolean declared = into.has("model");
        String format = file.getString("format");
        com.legend.executionplan.DataRef.Format read = switch (format) {
            // a table of its own detects a CSV's columns; a declared table reads its cells as text
            case "csv" -> declared ? com.legend.executionplan.DataRef.Csv.DEFAULT
                    : com.legend.executionplan.DataRef.DetectedCsv.DETECTED;
            case "parquet" -> com.legend.executionplan.DataRef.Parquet.PARQUET;
            case "json" -> com.legend.executionplan.DataRef.Json.RECORDS;
            default -> throw new IllegalArgumentException("a host's file is csv, parquet or json, not " + format);
        };
        com.legend.executionplan.DataRef ref = new com.legend.executionplan.DataRef(
                com.legend.executionplan.DataRef.Namespace.HOST, file.getString("name"), file.getString("digest"),
                file.getLong("size"), read);
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        com.legend.executionplan.DatabaseType type =
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType));
        com.legend.executionplan.ExecutionPlan plan = declared
                ? MODELS.withText(into.getString("model"), ctx -> com.legend.planner.LoadPlanner.hostFile(dialect, type,
                        ref, new com.legend.planner.LoadPlanner.DeclaredTable(ctx, into.getString("database"),
                                into.getString("schema"), into.getString("table"))))
                : com.legend.planner.LoadPlanner.hostFile(dialect, type, ref,
                        new com.legend.planner.LoadPlanner.NewTable(into.getString("table")));
        return com.legend.executionplan.PlanJson.write(plan);
    }

    /**
     * A REMOTE FILE MOUNTED, AS A PLAN (B3's Q3, 2026-11-11, the user's ruling of that day: "Yes do option 2";
     * docs/data-delivery-homework/B3_REMOTE_MOUNT_DESIGN_2026_11_11.md): {@code request} is
     * {@code {"databaseType", "view"?, "location": {"url"} | {"bearer", "shown"}, "format", "credentials"?: {"name",
     * "fields"}}} -- NAMES AND FIELDS ONLY: an address that carries no token, or a bearer URL by the name the host holds
     * it under and its address without the query string; an S3 secret by the name the host holds it under and the
     * fields it holds. Any other key is refused by name, so no value can ride in one. The answer is the plan
     * (PlanJson's): one HOST_SESSION load the host's runner runs, binding what it names from the host's store
     * ({@code LoadPlanner.remoteMount}). Until then the host ran the mount as operations, the token in a view's text.
     */
    public static String remoteMount(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a remote mount's request", "databaseType", "view", "location", "format", "credentials");
        com.legend.json.Json.Obj where = in.getObj("location");
        com.legend.planner.LoadPlanner.RemoteLocation location;
        if (where.has("url")) {
            only(where, "a remote file's address", "url");
            location = new com.legend.planner.LoadPlanner.PlainLocation(where.getString("url"));
        } else {
            only(where, "a bearer URL's name", "bearer", "shown");
            location = new com.legend.planner.LoadPlanner.BearerLocation(where.getString("bearer"),
                    where.getString("shown"));
        }
        String format = in.getString("format");
        com.legend.sql.dialect.HostOperation.RemoteFormat read = switch (format) {
            case "csv" -> com.legend.sql.dialect.HostOperation.RemoteFormat.CSV;
            case "parquet" -> com.legend.sql.dialect.HostOperation.RemoteFormat.PARQUET;
            case "iceberg" -> com.legend.sql.dialect.HostOperation.RemoteFormat.ICEBERG;
            default -> throw new IllegalArgumentException("a remote file is csv, parquet or iceberg, not " + format);
        };
        com.legend.planner.LoadPlanner.RemoteCredentials credentials = null;
        if (in.has("credentials")) {
            com.legend.json.Json.Obj c = in.getObj("credentials");
            only(c, "a remote file's credentials", "name", "fields");
            credentials = new com.legend.planner.LoadPlanner.RemoteCredentials(c.getString("name"),
                    c.getStringArray("fields"));
        }
        String databaseType = in.getString("databaseType");
        return com.legend.executionplan.PlanJson.write(com.legend.planner.LoadPlanner.remoteMount(
                dialectOf(databaseType),
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType)),
                new com.legend.planner.LoadPlanner.RemoteMount(in.getStringOr("view", null), location, read,
                        credentials)));
    }

    /**
     * A SNAPSHOT FROM A REMOTE PLANE, AS A PLAN (the user's ruling of 2026-11-11, "This is the right plan"; and of
     * 2026-11-12, "that is a real off platform problem": the request carries the PULL, never SQL): {@code request} is
     * {@code {"databaseType", "plane", "model", "runtime", "lambda", "schema"?, "table"}} -- the host's database's type,
     * the name the host holds the remote plane under, the pull (the model's text, the runtime it runs through, and the
     * grid's base query as a lambda's protocol JSON, a text), and the table of the host's database it lands in. The pull
     * is planned HERE, in this call, for the plane's own database (the model held by its text: compiled once), and its
     * statement is the one the plane runs. Any other key -- a statement ({@code sql}) above all -- is refused by name:
     * no host hands the planner the text a plane runs, and no row rides in. A pull whose plan does not run as its text
     * alone (one binding a value) is refused by name: a plane runs its statement as text. The answer is the plan
     * (PlanJson's): one HOST_SESSION load whose stream step the host's runner runs in one transaction
     * ({@code LoadPlanner.snapStream}).
     */
    public static String snapStream(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a snapshot's stream", "databaseType", "plane", "model", "runtime", "lambda", "schema", "table");
        String databaseType = in.getString("databaseType");
        String table = in.getString("table");
        String statement = MODELS.withText(in.getString("model"), ctx -> {
            com.legend.plan.QueryPlan pulled = com.legend.Compiler.query(ctx,
                    com.legend.protocol.ProtocolReader.lambda(in.getString("lambda"))).plan(in.getString("runtime"));
            String beyond = pulled.beyondItsText();
            if (beyond != null || !pulled.textAlone()) {
                throw new com.legend.error.NotImplementedException("a remote plane runs its statement as text, and the"
                        + " pull snapped into " + table + " does not run as its text alone"
                        + (beyond != null ? ": " + beyond : ": its plan binds values"));
            }
            return pulled.sql();
        });
        return com.legend.executionplan.PlanJson.write(com.legend.planner.LoadPlanner.snapStream(
                dialectOf(databaseType),
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType)),
                new com.legend.planner.LoadPlanner.SnapStream(in.getString("plane"), statement,
                        in.getStringOr("schema", null), table)));
    }

    /**
     * A SNAPSHOT'S COPY, AS A PLAN (the final audit's 1(b), 2026-11-12, the user's ruling; B3's contract): {@code request}
     * is {@code {"databaseType", "model", "runtime", "lambda", "schema"?, "table"}} -- the host's database's type, the
     * pull (the model's text, the runtime it runs through -- the host's own store's -- and the grid's base query as a
     * lambda's protocol JSON, a text), and the table of the host's database it is copied into. The pull is planned HERE,
     * in this call (the model held by its text: compiled once). Any other key -- a statement ({@code sql}) above all -- is
     * refused by name. The answer is the plan (PlanJson's): one writing {@code Sequence} on the host's database, its
     * target carrying the database's transaction -- the schema made where one is named, the table dropped, and the
     * table made AS the pull's own statement, its {@code data:} payloads bound as the pull binds them
     * ({@code com.legend.planner.SnapCopy}). Until then the host made the copy as host operations around a query it handed
     * back as text ({@code HostOperation.Source.Query}, deleted).
     */
    public static String snapCopy(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a snapshot's copy", "databaseType", "model", "runtime", "lambda", "schema", "table");
        String databaseType = in.getString("databaseType");
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        com.legend.executionplan.DatabaseType type =
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType));
        return com.legend.executionplan.PlanJson.write(MODELS.withText(in.getString("model"), ctx ->
                com.legend.planner.SnapCopy.plan(com.legend.Compiler.query(ctx,
                        com.legend.protocol.ProtocolReader.lambda(in.getString("lambda"))), in.getString("runtime"),
                        dialect, type, in.getStringOr("schema", null), in.getString("table"))));
    }

    /**
     * A TABLE OUTPUT, AS ONE PLAN (the final audit's item 2, 2026-11-12, the user's ruling (b); B3's contract): a
     * DataCube box's rows written into a table of the host's own database, made before it runs. {@code request} is
     * {@code {"databaseType", "tables": [{"table", "convertible", "pkg"?, "columns"}], "output": {"name", "mode",
     * "keys"?}, "rows", "target"?}} -- the box's tables of the host's database as their catalog answers them (read
     * before the run), the output's table and how it is written ({@code create}, {@code overwrite}, {@code append},
     * {@code deleteAppend}, {@code merge} by its keys), the box's rows (a lambda's protocol JSON, a text: one query of
     * no parameter over the tables' model) and, where the table is there already (append, deleteAppend, merge), its
     * catalog's columns. Any other key -- a statement ({@code sql}), a shape -- is refused by name. A made table is the
     * database's own {@code CREATE TABLE ... AS <the rows' statement>}, planned, carrying the rows, so the database types
     * its columns as it always did (the orchestrator's ruling (e) and proposal, 2026-11-12), its answer the table's rows
     * counted; a table there is written by the store write the planner plans over one model of the tables and the target.
     * The answer is the plan (PlanJson's): one writing {@code Sequence}, its answer the count
     * ({@code com.legend.planner.OutputPlan}). Until then the host made the table from {@code rows LIMIT 0} as a host
     * operation's query, read its catalog, and asked for the write's plan apart.
     */
    public static String outputPlan(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a table output", "databaseType", "tables", "output", "rows", "target");
        String databaseType = in.getString("databaseType");
        com.legend.json.Json.Obj output = in.getObj("output");
        only(output, "a table output's output", "name", "mode", "keys");
        String name = output.getString("name");
        com.legend.planner.OutputPlan.Mode mode = com.legend.planner.OutputPlan.Mode.named(output.getString("mode"));
        java.util.List<String> keys = output.has("keys") ? output.getStringArray("keys") : java.util.List.of();
        if (mode == com.legend.planner.OutputPlan.Mode.MERGE && keys.isEmpty()) {
            throw new IllegalArgumentException("a merge names its keys: the columns a row is matched by");
        }
        if (mode != com.legend.planner.OutputPlan.Mode.MERGE && output.has("keys")) {
            throw new IllegalArgumentException("an output's keys are a merge's: " + mode.spelled() + " takes none");
        }
        java.util.List<com.legend.json.Json.Node> tables = in.getArr("tables").items();
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("its rows are read from none of this page's database's tables: a table"
                    + " output writes its own database");
        }
        for (com.legend.json.Json.Node t : tables) {
            if (t instanceof com.legend.json.Json.Obj o && o.getString("table").equalsIgnoreCase(name)) {
                throw new IllegalArgumentException("'" + name + "' is one of this page's own tables (a file's, a"
                        + " copy's): name the output's table another way");
            }
        }
        if (mode.makes() && in.has("target")) {
            throw new IllegalArgumentException("a created table's columns are its rows' relation type: the request names"
                    + " no target");
        }
        if (!mode.makes() && !in.has("target")) {
            throw new IllegalArgumentException("this page's database has no table '" + name + "': write it with Create a"
                    + " new table first");
        }
        com.legend.protocol.spec.LambdaFunction rows = com.legend.protocol.ProtocolReader.lambda(in.getString("rows"));
        if (!rows.parameters().isEmpty() || rows.body().size() != 1) {
            throw new IllegalArgumentException("an output's rows are one query of no parameter");
        }
        String rowsBody = com.legend.json.Json.toCompact(com.legend.json.Json.parseObject(in.getString("rows"))
                .getArr("body").items().get(0));
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        java.util.List<Object> withTarget = new java.util.ArrayList<>();
        for (com.legend.json.Json.Node t : tables) {
            withTarget.add(com.legend.json.Json.parse(com.legend.json.Json.toCompact(t)));
        }
        if (mode.makes()) {
            // the create carries the data: the rows over the box's tables alone, the database typing the made table's
            // columns (no column declared, so none refused or mapped)
            com.legend.json.Json.Obj sources = com.legend.json.Json.parseObject(tablesModel(com.legend.json.Json
                    .parseObject(com.legend.json.Json.toCompact(java.util.Map.of("databaseType", databaseType,
                            "tables", withTarget)))));
            return com.legend.executionplan.PlanJson.write(MODELS.withText(sources.getString("model"), ctx ->
                    com.legend.planner.OutputPlan.make(ctx, sources.getString("runtime"), in.getString("rows"), mode,
                            name, dialect)));
        }
        java.util.List<Object> targetColumns = new java.util.ArrayList<>();
        for (com.legend.json.Json.Node c : in.getArr("target").items()) {
            targetColumns.add(com.legend.json.Json.parse(com.legend.json.Json.toCompact(c)));
        }
        java.util.Map<String, Object> target = new java.util.LinkedHashMap<>();
        target.put("table", name);
        target.put("convertible", false);
        target.put("columns", targetColumns);
        withTarget.add(target);
        com.legend.json.Json.Obj model = com.legend.json.Json.parseObject(tablesModel(com.legend.json.Json.parseObject(
                com.legend.json.Json.toCompact(java.util.Map.of("databaseType", databaseType, "tables", withTarget)))));
        java.util.List<com.legend.json.Json.Node> modelled = model.getArr("tables").items();
        String into = com.legend.json.Json.toCompact(((com.legend.json.Json.Obj) modelled.get(modelled.size() - 1))
                .get("source"));
        String program = outputProgram(rowsBody, into, mode, keys);
        return com.legend.executionplan.PlanJson.write(MODELS.withText(model.getString("model"), ctx ->
                com.legend.planner.OutputPlan.write(ctx, model.getString("runtime"), program, mode)));
    }

    /** The placeholders an output's program's template holds, each a string the template alone writes. */
    private static final String OUTPUT_ROWS = "__lite_output_rows__";
    private static final String OUTPUT_INTO = "__lite_output_into__";

    /**
     * The Pure program that writes {@code rows} into {@code into} (each a protocol node's JSON) as {@code mode} writes,
     * as a lambda's protocol JSON -- {@code write}, {@code delete} then {@code write}, or {@code merge} by {@code keys}
     * (as DataCube's table output wrote it) -- its statements the parser's own reading of the program's text, each
     * placeholder replaced by the rows' or the target's node.
     */
    private static String outputProgram(String rows, String into, com.legend.planner.OutputPlan.Mode mode,
            java.util.List<String> keys) {
        String r = "'" + OUTPUT_ROWS + "'";
        String t = "'" + OUTPUT_INTO + "'";
        String text = switch (mode) {
            case APPEND -> "|" + r + "->write(" + t + ")";
            case CREATE, OVERWRITE -> throw new IllegalStateException("an output that makes its table carries its rows"
                    + " in the making: no write program");
            case DELETE_APPEND -> "{|" + t + "->delete(r|true); " + r + "->write(" + t + ");}";
            case MERGE -> "|" + r + "->merge(" + t + ", ~[" + String.join(", ", keys.stream()
                    .map(k -> "'" + k.replace("\\", "\\\\").replace("'", "\\'") + "'").toList()) + "])";
        };
        String json = com.legend.parser.SpecParser.lambdaJson(text, false);
        return placed(placed(json, OUTPUT_ROWS, rows), OUTPUT_INTO, into);
    }

    /** {@code json} with every string node holding {@code placeholder} replaced by {@code node}. */
    private static String placed(String json, String placeholder, String node) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\{[^{}]*\"value\"\\s*:\\s*\""
                + placeholder + "\"[^{}]*\\}").matcher(json);
        if (!m.find()) {
            throw new IllegalStateException("an output's program holds no " + placeholder + ": " + json);
        }
        return m.replaceAll(java.util.regex.Matcher.quoteReplacement(node));
    }

    /**
     * A STATEMENT A PERSON WROTE, as a plan (the user's ruling of 2026-11-12: Studio's SQL playground goes through the
     * planner): {@code request} is {@code {"databaseType", "text"}}, the database the playground points at and the
     * person's one statement. The answer is the plan (PlanJson's): the text carried as written -- never parsed,
     * rewritten or split -- as the author's SQL, its answer the relation the database reports
     * ({@code com.legend.planner.Authored}).
     */
    public static String authored(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "an authored statement", "databaseType", "text");
        String databaseType = in.getString("databaseType");
        return com.legend.executionplan.PlanJson.write(com.legend.planner.Authored.plan(dialectOf(databaseType),
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType)),
                in.getString("text")));
    }

    /**
     * ROWS THE DATABASE GENERATES INTO A DECLARED TABLE, AS A PLAN (the test-data generator's SYNTHESIZE, its simple slice:
     * the user's ruling of 2026-11-12; register S303): {@code request} is {@code {"databaseType", "model", "database",
     * "schema", "table", "count", "columns"?}} -- the host's database's type (DuckDB alone in this slice), the model's
     * text, the table its Database declares, how many rows, and a knob per column by name: {@code {"values": [...]}}
     * (taken in turn) or {@code {"range": [lo, hi], "scale"?}} (numbers, or ISO dates). Any other key is refused by name,
     * so no row can ride in one. The answer is the plan (PlanJson's): one HOST_SESSION load the host's runner runs, its
     * steps the table made as declared, empty, and one statement the database fills it with
     * ({@code com.legend.planner.Synthesize}). A model this host holds is read as held, compiled no more.
     */
    public static String synthesize(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a synthesized table", "databaseType", "model", "database", "schema", "table", "count", "columns");
        String databaseType = in.getString("databaseType");
        java.util.Map<String, com.legend.planner.Synthesize.Knob> knobs = new java.util.LinkedHashMap<>();
        com.legend.json.Json.Obj columns = in.getObjOr("columns", null);
        if (columns != null) {
            for (java.util.Map.Entry<String, com.legend.json.Json.Node> e : columns.fields().entrySet()) {
                com.legend.json.Json.Obj knob = columns.getObj(e.getKey());
                only(knob, "a synthesized column's knob", "values", "range", "scale");
                if (knob.has("values") == knob.has("range")) {
                    throw new IllegalArgumentException("a synthesized column's knob is its values or its range, one of"
                            + " them: " + e.getKey());
                }
                if (knob.has("values")) {
                    if (knob.has("scale")) {
                        // a scale is a range's; beside values it would be read by nothing (the B close's audit,
                        // SHOULD-FIX 3, 2026-11-12)
                        throw new IllegalArgumentException("a synthesized column's scale is its range's, and "
                                + e.getKey() + " gives values");
                    }
                    java.util.List<String> values = new java.util.ArrayList<>();
                    for (com.legend.json.Json.Node v : knob.getArr("values").items()) {
                        values.add(scalar(v, e.getKey()));
                    }
                    knobs.put(e.getKey(), new com.legend.planner.Synthesize.Knob.Values(values));
                } else {
                    java.util.List<com.legend.json.Json.Node> range = knob.getArr("range").items();
                    if (range.size() != 2) {
                        throw new IllegalArgumentException("a synthesized column's range is [lo, hi]: " + e.getKey());
                    }
                    knobs.put(e.getKey(), new com.legend.planner.Synthesize.Knob.Range(scalar(range.get(0), e.getKey()),
                            scalar(range.get(1), e.getKey()), knob.has("scale") ? knob.getInt("scale") : null));
                }
            }
        }
        // the count a whole number as written: 2.9 is refused, never read as 2 (the B close's audit, SHOULD-FIX 3)
        if (!(in.get("count") instanceof com.legend.json.Json.Num n) || !n.isInteger()) {
            throw new IllegalArgumentException("a synthesized table's count is a whole number of rows, not "
                    + com.legend.json.Json.toCompact(in.get("count")));
        }
        long count = n.longValue();
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        com.legend.executionplan.DatabaseType type =
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType));
        return com.legend.executionplan.PlanJson.write(MODELS.withText(in.getString("model"), ctx ->
                com.legend.planner.Synthesize.plan(dialect, type, new com.legend.planner.LoadPlanner.DeclaredTable(ctx,
                        in.getString("database"), in.getString("schema"), in.getString("table")),
                        count, knobs)));
    }

    /**
     * HOW A DATASET'S ROWS LAND, AS PLANS (an ingest's plans: docs/DESK_DATA_ROADMAP_2026_10_29.md C0, the user's go of
     * 2026-11-14): {@code request} is {@code {"databaseType", "model", "database", "schema", "table", "held"}} -- the
     * host's database's type, the model's text, a table an ingest's dataset makes (reached from that Database through its
     * includes), and the name the host holds each batch under. Any other key is refused by name, so no row can ride in
     * one. The answer is {@code {"make", "land", "keep", "keepEvery"}}: the three plans (PlanJson's text; {@code keep}
     * and its cadence {@code keepEvery}, rows arrived between two runs, null for a dataset that keeps every row), each one
     * HOST_SESSION load the host's runner runs ({@code com.legend.planner.IngestPlanner}). A model this host holds is
     * read as held, compiled no more.
     */
    public static String ingestPlans(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "an ingest's plans", "databaseType", "model", "database", "schema", "table", "held");
        String databaseType = in.getString("databaseType");
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
        com.legend.executionplan.DatabaseType type =
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType));
        return MODELS.withText(in.getString("model"), ctx -> {
            com.legend.planner.IngestPlanner.Plans plans = com.legend.planner.IngestPlanner.plans(dialect, type,
                    new com.legend.planner.LoadPlanner.DeclaredTable(ctx, in.getString("database"),
                            in.getString("schema"), in.getString("table")), in.getString("held"));
            java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("make", com.legend.executionplan.PlanJson.write(plans.make()));
            out.put("land", com.legend.executionplan.PlanJson.write(plans.land()));
            out.put("keep", plans.keep() == null ? null : com.legend.executionplan.PlanJson.write(plans.keep()));
            out.put("keepEvery", plans.keepEvery());
            return com.legend.json.Json.toCompact(out);
        });
    }

    /** The topics as JSON: each one's landing table, name, columns (name and Deephaven type), key, retention. */
    private static String topics(java.util.List<com.legend.planner.IngestTopics.Topic> topics) {
        java.util.List<Object> out = new java.util.ArrayList<>();
        for (com.legend.planner.IngestTopics.Topic t : topics) {
            java.util.Map<String, Object> o = new java.util.LinkedHashMap<>();
            o.put("table", t.table());
            o.put("topic", t.topic());
            o.put("columns", t.columns().stream().map(c -> {
                java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
                col.put("name", c.name());
                col.put("type", c.type());
                return (Object) col;
            }).toList());
            o.put("key", t.key());
            o.put("keepRows", t.keepRows());
            o.put("keepFor", t.keepFor());
            out.add(o);
        }
        return com.legend.json.Json.toCompact(java.util.Map.of("topics", out));
    }

    /** A knob's value as written: a number's own token, or a text. */
    private static String scalar(com.legend.json.Json.Node v, String column) {
        return switch (v) {
            case com.legend.json.Json.Str s -> s.value();
            case com.legend.json.Json.Num n -> n.token() != null ? n.token()
                    : n.isInteger() ? Long.toString(n.longValue()) : java.math.BigDecimal.valueOf(n.doubleValue())
                            .toPlainString();
            default -> throw new IllegalArgumentException("a synthesized column's value is a number or a text: "
                    + column);
        };
    }

    /**
     * A HOST'S VALUE READ AS A RELATION, AS A PLAN (the user's ruling of 2026-11-11: a Python frame named, attached at
     * run): {@code request} is {@code {"databaseType", "held", "as", "live", "schema"?, "relation"}} -- the name the host
     * holds the value under, the relation it is attached as, whether it is read at each query (a view) or once (a
     * table), and the relation it is read through. Any other key is refused by name, so no value can ride in one. The
     * answer is the plan (PlanJson's): one HOST_SESSION load the host's runner runs ({@code LoadPlanner.frameLoad}).
     */
    public static String frameLoad(String request) {
        com.legend.json.Json.Obj in = com.legend.json.Json.parseObject(request);
        only(in, "a host value's load", "databaseType", "held", "as", "live", "schema", "relation");
        String databaseType = in.getString("databaseType");
        return com.legend.executionplan.PlanJson.write(com.legend.planner.LoadPlanner.frameLoad(
                dialectOf(databaseType),
                com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType)),
                new com.legend.planner.LoadPlanner.FrameLoad(in.getString("held"), in.getString("as"),
                        in.getBool("live"), in.getStringOr("schema", null), in.getString("relation"))));
    }

    /** {@code o}'s keys among {@code keys}, any other refused by name ({@code what} names the object). */
    private static void only(com.legend.json.Json.Obj o, String what, String... keys) {
        java.util.Set<String> allowed = java.util.Set.of(keys);
        for (String key : o.fields().keySet()) {
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException(what + " has no key " + key + " (names and fields only, never a"
                        + " value)");
            }
        }
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
     * [{"column","sql"}], "excluded": [name], "bitColumns": [name]}} out. A source rewritten in place
     * ({@code convertible}) is a host operation's copy that names these catalog columns ({@code "convert"}), its select
     * list written by the platform (the final audit's 1(d), 2026-11-12: until then this answered the select list,
     * {@code copySelectList}, which the host handed back as text).
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
     * What a host sends a Deephaven server and reads back (docs/DEEPHAVEN_PLAN_2026_10_22.md DH21, DH22;
     * {@code com.legend.deephaven.DeephavenWire}), one operation by name, its arguments a JSON object: {@code batch}
     * ({@code plan}, {@code exportId}: the plan's request, its result exported under that id in the host's session),
     * {@code publish} ({@code exportId}, {@code name}), {@code releaseExport} ({@code exportId}, which ends the name
     * too) and {@code doGet} ({@code exportId}: the Flight ticket its rows are read by) -- each answered as its bytes
     * in base64 -- {@code made} ({@code answers}: a batch's answer, each message in base64, {@code exports}: what its
     * {@code send} exports, as it answered them), answered {@code {"refused", "mismatch", "columns"}}: the server's
     * refusal, a table it holds otherwise than its store declares (DH16), each null when there is none, and each made
     * table's columns by its export, {@code [{"name", "type"}]}; {@code keys} ({@code plan}), answered
     * {@code {"keys"}}, each operation's; and {@code send} ({@code plan}, {@code alive}: the session's tables, key to
     * export id, {@code supplied}?: the tables the session supplies itself, name to export id, {@code nextExportId}),
     * answered {@code {"request", "exports": [{"key", "exportId", "declared"?}], "result"}}, {@code declared} the store's
     * table an export's operation reads, as {@code made} reads it back; and {@code stored} ({@code plan}), answered
     * {@code {"plan", "columns"}}, the
     * plan with its result stored; {@code nullable} ({@code handle}, {@code lambda}, {@code runtime}), answered
     * {@code {"nullable"}}: which of the query's result columns may be null, as the planner lowers it -- sound across
     * an outer join, where the relation type keeps a joined column's own multiplicity (a grid's base, B1). The host carries the bytes; it builds
     * none and reads none itself. An operation not listed is refused by name.
     */
    public static String deephaven(String op, String argsJson) {
        com.legend.json.Json.Obj a = com.legend.json.Json.parseObject(argsJson);
        java.util.Base64.Encoder b64 = java.util.Base64.getEncoder();
        return switch (op) {
            case "batch" -> b64.encodeToString(
                    com.legend.deephaven.DeephavenWire.batch(a.getString("plan"), a.getInt("exportId")));
            case "publish" -> b64.encodeToString(
                    com.legend.deephaven.DeephavenWire.publish(a.getInt("exportId"), a.getString("name")));
            case "releaseExport" -> b64.encodeToString(
                    com.legend.deephaven.DeephavenWire.releaseExport(a.getInt("exportId")));
            case "doGet" -> b64.encodeToString(com.legend.deephaven.DeephavenWire.doGet(a.getInt("exportId")));
            // the topics a database's streams read, from its model (the desk roadmap's B1: the world's feed writes
            // each landing table's rows to its topic, and the door keeps each as its dataset lands)
            case "topics" -> MODELS.withText(a.getString("model"), ctx -> topics(
                    com.legend.planner.IngestTopics.of(ctx, a.getString("database"))));
            case "keys" -> com.legend.json.Json.toCompact(java.util.Map.of("keys",
                    com.legend.deephaven.DeephavenWire.keys(a.getString("plan"))));
            case "send" -> {
                java.util.Map<String, Integer> alive = new java.util.LinkedHashMap<>();
                a.getObj("alive").fields().forEach((key, id) ->
                        alive.put(key, (int) ((com.legend.json.Json.Num) id).longValue()));
                java.util.Map<String, Integer> supplied = new java.util.LinkedHashMap<>();
                if (a.has("supplied")) {
                    a.getObj("supplied").fields().forEach((name, id) ->
                            supplied.put(name, (int) ((com.legend.json.Json.Num) id).longValue()));
                }
                com.legend.deephaven.DeephavenWire.Send sent = com.legend.deephaven.DeephavenWire.send(
                        a.getString("plan"), alive, supplied, a.getInt("nextExportId"));
                java.util.List<Object> exports = new java.util.ArrayList<>();
                for (com.legend.deephaven.DeephavenWire.Export e : sent.exports()) {
                    java.util.Map<String, Object> one = new java.util.LinkedHashMap<>();
                    one.put("key", e.key());
                    one.put("exportId", e.exportId());
                    if (e.declared() != null) {
                        java.util.List<Object> columns = new java.util.ArrayList<>();
                        for (com.legend.deephaven.DeephavenWire.DeclaredColumn c : e.declared().columns()) {
                            java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
                            col.put("name", c.name());
                            col.put("column", c.column());
                            col.put("type", c.type());
                            col.put("declared", c.declared());
                            columns.add(col);
                        }
                        java.util.Map<String, Object> declared = new java.util.LinkedHashMap<>();
                        declared.put("table", e.declared().table());
                        declared.put("columns", columns);
                        one.put("declared", declared);
                    }
                    exports.add(one);
                }
                java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
                out.put("request", b64.encodeToString(sent.request()));
                out.put("exports", exports);
                out.put("result", sent.result());
                yield com.legend.json.Json.toCompact(out);
            }
            case "nullable" -> MODELS.with(a.getInt("handle"), ctx -> {
                java.util.List<Boolean> nullable = new java.util.ArrayList<>();
                for (com.legend.sql.OutputCol c : com.legend.Compiler.query(ctx,
                        com.legend.protocol.ProtocolReader.lambda(a.getString("lambda")))
                        .lower(a.getString("runtime"), false).plan().outputs()) {
                    nullable.add(c.nullable());
                }
                return com.legend.json.Json.toCompact(java.util.Map.of("nullable", nullable));
            });
            case "stored" -> {
                com.legend.deephaven.DeephavenWire.Stored st = com.legend.deephaven.DeephavenWire.stored(
                        a.getString("plan"));
                java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
                out.put("plan", st.plan());
                out.put("columns", st.columns());
                yield com.legend.json.Json.toCompact(out);
            }
            case "made" -> {
                java.util.List<byte[]> answers = new java.util.ArrayList<>();
                for (com.legend.json.Json.Node n : a.getArr("answers").items()) {
                    answers.add(java.util.Base64.getDecoder().decode(((com.legend.json.Json.Str) n).value()));
                }
                java.util.List<com.legend.deephaven.DeephavenWire.Export> exports = new java.util.ArrayList<>();
                for (com.legend.json.Json.Node n : a.getArr("exports").items()) {
                    com.legend.json.Json.Obj e = (com.legend.json.Json.Obj) n;
                    com.legend.json.Json.Obj d = e.getObjOr("declared", null);
                    com.legend.deephaven.DeephavenWire.Declared declared = null;
                    if (d != null) {
                        java.util.List<com.legend.deephaven.DeephavenWire.DeclaredColumn> columns =
                                new java.util.ArrayList<>();
                        for (com.legend.json.Json.Node c : d.getArr("columns").items()) {
                            com.legend.json.Json.Obj col = (com.legend.json.Json.Obj) c;
                            columns.add(new com.legend.deephaven.DeephavenWire.DeclaredColumn(col.getString("name"),
                                    col.getString("column"), col.getString("type"), col.getString("declared")));
                        }
                        declared = new com.legend.deephaven.DeephavenWire.Declared(d.getString("table"), columns);
                    }
                    exports.add(new com.legend.deephaven.DeephavenWire.Export(e.getString("key"), e.getInt("exportId"),
                            declared));
                }
                com.legend.deephaven.DeephavenWire.Made m = com.legend.deephaven.DeephavenWire.made(answers, exports);
                java.util.Map<String, Object> columns = new java.util.LinkedHashMap<>();
                m.columns().forEach((id, cs) -> {
                    java.util.List<Object> list = new java.util.ArrayList<>();
                    for (com.legend.deephaven.ArrowSchema.Column c : cs) {
                        java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
                        col.put("name", c.name());
                        col.put("type", c.type());
                        list.add(col);
                    }
                    columns.put(String.valueOf(id), list);
                });
                java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
                out.put("refused", m.refused());
                out.put("mismatch", m.mismatch());
                out.put("columns", columns);
                yield com.legend.json.Json.toCompact(out);
            }
            default -> throw new IllegalArgumentException("no Deephaven operation '" + op
                    + "': batch, keys, send, stored, nullable, publish, releaseExport, doGet, made");
        };
    }

    /**
     * A project's own database on a database of the given type (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5,
     * {@code com.legend.setup.ProjectData}): where {@code model}'s Data rows live on a host, named by what it holds
     * (the model's Databases and Data rows). {@code {"name", "open", "use", "close", "load"?}}: the statements, in
     * order, that make it, point a connection of its own at it and drop it; and {@code load}, the plan whose root is the
     * ONCE load that makes and fills every table a relational Data element fills -- the load a server's project plan
     * carries, its statements a blob the host checks against its digest and runs on that connection (B3, 2026-11-11:
     * until then the fill's statements as text, census M14) -- absent for a model with no Data rows. A database without
     * the operations is refused by name. A model this
     * host holds is read as held, compiled no more (HeldModels.withText: a project's model, typed and planned by the
     * page's planner, is 4 MB of the trading model and its Data, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11).
     */
    public static String projectData(String model, String databaseType) {
        return MODELS.withText(model, ctx -> {
            com.legend.sql.dialect.SqlDialect dialect = dialectOf(databaseType);
            com.legend.setup.ProjectData p = com.legend.setup.ProjectData.of(ctx, dialect);
            com.legend.executionplan.ExecutionPlan load = com.legend.planner.LoadPlanner.projectData(ctx, dialect,
                    com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.named(databaseType)));
            java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("name", p.name());
            out.put("open", p.open());
            out.put("use", p.use());
            out.put("close", p.close());
            if (load != null) {
                out.put("load", com.legend.json.Json.parse(com.legend.executionplan.PlanJson.write(load)));
            }
            return com.legend.json.Json.toCompact(out);
        });
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
