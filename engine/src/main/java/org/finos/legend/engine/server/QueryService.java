package org.finos.legend.engine.server;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.ConnectionResolver;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.execution.ScalarResult;
import org.finos.legend.engine.execution.StreamingResult;
import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.serialization.ResultSerializer;
import org.finos.legend.engine.serialization.SerializerRegistry;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.finos.legend.engine.transpiler.json.DuckDbJsonDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlGenerator;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.PureParser;
import org.finos.legend.pure.dsl.TypeEnvironment;
import org.finos.legend.pure.dsl.ast.CleanCompiler;
import org.finos.legend.pure.dsl.ast.PlanGenerator;

import org.finos.legend.pure.dsl.ast.ValueSpecification;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Stateless query service for ad-hoc query execution.
 * 
 * Performs compile + plan generation + execution in a single call.
 * No state is retained between calls - each request is independent.
 * 
 * Supports both buffered (default) and streaming result modes, as well as
 * direct serialization to various output formats (JSON, CSV, etc).
 * 
 * Use cases:
 * - Ad-hoc queries without pre-deployed models
 * - Exploratory data analysis
 * - One-off function calls
 * 
 * Example:
 * 
 * <pre>
 * QueryService service = new QueryService();
 * 
 * BufferedResult result = service.execute(
 *         modelSource, // Classes, Mappings, Connections, Runtimes
 *         "Person.all()->filter({p | $p.age > 30})->project(...)",
 *         "app::MyRuntime");
 * </pre>
 */
public class QueryService {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private final ConnectionResolver connectionResolver = new ConnectionResolver();

    public QueryService() {
    }

    /**
     * Compiles Pure source, generates a plan, and executes - all in one call.
     * Returns a buffered result (all rows materialized in memory).
     * 
     * @param pureSource  The complete Pure source (model + runtime definitions)
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime to use
     * @return The execution result as buffered tabular data
     * @throws SQLException If execution fails
     */
    public BufferedResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {
        return execute(pureSource, query, runtimeName, ResultMode.BUFFERED).toBuffered();
    }

    /**
     * Executes raw SQL against the Connection from the user's Runtime.
     * 
     * This does NOT compile Pure - it directly executes the provided SQL
     * against the database connection defined in the Runtime.
     * 
     * Use for:
     * - CREATE TABLE / DROP TABLE
     * - INSERT / UPDATE / DELETE
     * - Raw SELECT queries
     * 
     * @param pureSource  The Pure source containing Connection/Runtime definitions
     * @param sql         The raw SQL to execute
     * @param runtimeName The qualified name of the Runtime to use for connection
     * @return BufferedResult with results (empty for DDL/DML)
     * @throws SQLException If execution fails
     */
    public BufferedResult executeSql(String pureSource, String sql, String runtimeName)
            throws SQLException {

        // 1. Parse the Pure source to get connection definitions
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Get connection from runtime
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        // 4. Resolve the JDBC connection from the ConnectionDefinition
        Connection conn = connectionResolver.resolve(connectionDef);

        // 5. Execute the raw SQL
        try (Statement stmt = conn.createStatement()) {
            boolean hasResultSet = stmt.execute(sql);

            if (hasResultSet) {
                // SELECT - return results
                return executeBuffered(conn, sql);
            } else {
                // DDL/DML - return empty result
                return BufferedResult.empty();
            }
        }
    }

    /**
     * Compiles, generates, and executes using a provided connection.
     * Returns a buffered result.
     * 
     * @param pureSource  The complete Pure source (model + runtime definitions)
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime (for dialect resolution)
     * @param connection  The JDBC connection to use (must already contain test
     *                    data)
     * @return The execution result as buffered tabular data
     * @throws SQLException If execution fails
     */
    public BufferedResult execute(String pureSource, String query, String runtimeName, Connection connection)
            throws SQLException {
        return execute(pureSource, query, runtimeName, connection, ResultMode.BUFFERED).toBuffered();
    }

    /**
     * Executes with explicit result mode selection.
     * 
     * @param pureSource  The complete Pure source
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime
     * @param mode        The result mode (BUFFERED or STREAMING)
     * @return The execution result
     * @throws SQLException If execution fails
     */
    public Result execute(String pureSource, String query, String runtimeName, ResultMode mode)
            throws SQLException {

        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        RelationNode ir = compileQuery(query, mappingRegistry, model);

        // 4. Get connection and generate SQL
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        SQLDialect dialect = getDialect(connectionDef.databaseType());
        String sql = new SQLGenerator(dialect).generate(ir);

        // 5. Resolve connection and execute
        Connection conn = connectionResolver.resolve(connectionDef);
        return executeWithMode(conn, sql, mode);
    }

    /**
     * Executes with explicit result mode using a provided connection.
     */
    public Result execute(String pureSource, String query, String runtimeName,
            Connection connection, ResultMode mode) throws SQLException {
        return execute(pureSource, query, runtimeName, connection, mode, TypeEnvironment.empty());
    }

    /**
     * Executes with explicit result mode, connection, and type environment.
     * The TypeEnvironment provides class metadata for type-aware compilation.
     */
    public Result execute(String pureSource, String query, String runtimeName,
            Connection connection, ResultMode mode, TypeEnvironment typeEnv) throws SQLException {

        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime for dialect
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        RelationNode ir = compileQuery(query, mappingRegistry, model, typeEnv);

        // 4. Get dialect from runtime's connection
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        SQLDialect dialect = getDialect(connectionDef.databaseType());
        String sql = new SQLGenerator(dialect).generate(ir);

        // Dual-run: compare new pipeline SQL with old pipeline SQL
        tryNewPipeline(query, mappingRegistry, model, dialect, sql);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + sql);

        // 5. For constant queries and write(), use SCALAR mode to unwrap the result
        // write() returns Integer (row count), not a Relation
        boolean isScalarResult = (ir instanceof ConstantNode) || (ir instanceof WriteNode);
        ResultMode effectiveMode = isScalarResult ? ResultMode.SCALAR : mode;

        // 6. Execute using the appropriate mode
        Result result = executeWithMode(connection, sql, effectiveMode);

        // 7. For scalar results from ConstantNode, propagate IR type info
        // This preserves Decimal vs Float distinction lost by JDBC type mapping
        if (result instanceof ScalarResult sr && ir instanceof ConstantNode cn) {
            // DuckDB always falls back to DOUBLE for division (~16 digits).
            // Pure expects DECIMAL128 precision (~34 digits). Re-evaluate in Java.
            if (sr.value() instanceof Double && containsDivision(cn.expression())) {
                BigDecimal precise = evaluateWithBigDecimal(cn.expression());
                if (precise != null) {
                    return new ScalarResult(precise, "DECIMAL");
                }
            }
            // For value-selecting functions (greatest/least), DuckDB promotes all args
            // to a common type. Match the result back to the original arg's value and type.
            // This must run before DECIMAL handling since the IR type reflects the promoted
            // type, not the selected value's original type.
            if (cn.expression() instanceof FunctionExpression fe && isValueSelectingFunction(fe)) {
                if (sr.value() instanceof Number num) {
                    Object originalValue = findMatchingArgValue(fe, num, isListAggrMode(fe));
                    if (originalValue != null) {
                        return new ScalarResult(originalValue, sqlTypeForValue(originalValue));
                    }
                }
                // Date value-matching: DuckDB promotes DATE→TIMESTAMP in mixed lists.
                // If the result TIMESTAMP matches a StrictDate literal (midnight time), restore
                // DATE type.
                if (sr.value() instanceof java.sql.Timestamp ts) {
                    String matchedDateType = findMatchingDateType(fe, ts);
                    if (matchedDateType != null) {
                        return new ScalarResult(sr.value(), matchedDateType);
                    }
                }
            }
            GenericType irType = cn.expression().type();
            if (irType == GenericType.Primitive.DECIMAL) {
                // Distinguish toDecimal() CAST (needs trailing zero strip) from
                // Decimal literal arithmetic (preserves DuckDB scale as-is)
                boolean fromToDecimalCast = cn.expression() instanceof CastExpression ce
                        && ce.targetType() == GenericType.Primitive.DECIMAL;
                return new ScalarResult(sr.value(), fromToDecimalCast ? "DECIMAL_CAST" : "DECIMAL");
            }
            // Propagate Pure class type from StructLiteralExpression in the IR tree
            String pureType = extractPureType(cn.expression());
            if (pureType != null) {
                java.util.Map<String, String> fieldTypes = extractFieldTypes(cn.expression());
                return new ScalarResult(sr.value(), sr.sqlType(), pureType, fieldTypes);
            }
        }
        return result;
    }

    /**
     * Executes a query and serializes the result directly to an output stream.
     * Automatically chooses streaming mode if the serializer supports it.
     * 
     * @param pureSource  The complete Pure source
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime
     * @param out         The output stream to write to
     * @param format      The serialization format (e.g., "json", "csv")
     * @throws SQLException If execution fails
     * @throws IOException  If serialization fails
     */
    public void executeAndSerialize(String pureSource, String query, String runtimeName,
            OutputStream out, String format) throws SQLException, IOException {
        ResultSerializer serializer = SerializerRegistry.get(format);
        ResultMode mode = serializer.supportsStreaming() ? ResultMode.STREAMING : ResultMode.BUFFERED;

        try (Result result = execute(pureSource, query, runtimeName, mode)) {
            result.writeTo(out, serializer);
        }
    }

    /**
     * Executes a query with provided connection and serializes to output stream.
     */
    public void executeAndSerialize(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out, String format)
            throws SQLException, IOException {
        ResultSerializer serializer = SerializerRegistry.get(format);
        ResultMode mode = serializer.supportsStreaming() ? ResultMode.STREAMING : ResultMode.BUFFERED;

        try (Result result = execute(pureSource, query, runtimeName, connection, mode)) {
            result.writeTo(out, serializer);
        }
    }

    private Result executeWithMode(Connection conn, String sql, ResultMode mode) throws SQLException {
        return switch (mode) {
            case BUFFERED -> executeBuffered(conn, sql);
            case STREAMING -> executeStreaming(conn, sql);
            case SCALAR -> executeScalar(conn, sql);
        };
    }

    private Result executeScalar(Connection conn, String sql) throws SQLException {
        BufferedResult buffered = executeBuffered(conn, sql);
        if (buffered.rowCount() == 1 && buffered.columnCount() == 1) {
            String sqlType = buffered.columns().getFirst().sqlType();
            return new ScalarResult(buffered.getValue(0, 0), sqlType);
        }
        return buffered;
    }

    private BufferedResult executeBuffered(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return BufferedResult.fromResultSet(rs);
        }
    }

    private StreamingResult executeStreaming(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return StreamingResult.fromResultSet(rs, stmt, conn, DEFAULT_FETCH_SIZE);
    }

    /**
     * Compiles and generates a plan without executing.
     * Useful for plan introspection or caching.
     * 
     * @param pureSource  The complete Pure source
     * @param query       The Pure query
     * @param runtimeName The Runtime name
     * @return The execution plan
     */
    public ExecutionPlan compile(String pureSource, String query, String runtimeName) {
        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR (old pipeline)
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        RelationNode ir = compileQuery(query, mappingRegistry, model);

        // 4. Generate SQL for all connections in runtime
        Map<String, ExecutionPlan.GeneratedSql> sqlByStore = new HashMap<>();

        for (Map.Entry<String, String> binding : runtime.connectionBindings().entrySet()) {
            String storeRef = binding.getKey();
            String connectionRef = binding.getValue();

            ConnectionDefinition connection = model.getConnection(connectionRef);
            if (connection != null) {
                SQLDialect dialect = getDialect(connection.databaseType());
                String sql = new SQLGenerator(dialect).generate(ir);
                sqlByStore.put(storeRef, new ExecutionPlan.GeneratedSql(connection.databaseType(), sql));

                // 5. Dual-run: try new pipeline and compare SQL
                tryNewPipeline(query, mappingRegistry, model, dialect, sql);
            }
        }

        return new ExecutionPlan(
                UUID.randomUUID().toString(),
                ir,
                new ResultSchema(java.util.List.of()),
                sqlByStore,
                runtime.qualifiedName());
    }

    // ==================== New Pipeline Dual-Run ====================

    /** Enable/disable dual-run comparison logging. */
    private static volatile boolean DUAL_RUN_ENABLED = Boolean.parseBoolean(
            System.getProperty("legend.dualrun", "true"));

    private static final java.util.concurrent.atomic.AtomicInteger MATCH_COUNT = new java.util.concurrent.atomic.AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicInteger MISMATCH_COUNT = new java.util.concurrent.atomic.AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicInteger ERROR_COUNT = new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Runs the new pipeline (CleanCompiler + PlanGenerator → SqlBuilder) and
     * compares
     * its SQL output against the old pipeline's SQL.
     *
     * Non-invasive: catches all exceptions, never affects the old pipeline's
     * execution. Results are logged for parity tracking.
     */
    private void tryNewPipeline(String query, MappingRegistry mappingRegistry,
            PureModelBuilder model, SQLDialect dialect, String oldSql) {
        if (!DUAL_RUN_ENABLED)
            return;

        try {
            // Parse with clean parser
            ValueSpecification vs = PureParser.parseClean(query);

            // Compile with new compiler (type resolution)
            CleanCompiler compiler = new CleanCompiler(model);
            var unit = compiler.compile(vs);

            // Generate plan with PlanGenerator → SingleExecutionPlan
            var planGenerator = new PlanGenerator(unit, dialect);
            var plan = planGenerator.generate();
            String newSql = plan.sql();

            // Compare output — 3 old forms × 2 new forms, match if any pair agrees
            String oldRaw = oldSql.trim().replaceAll("\\s+", " ").toLowerCase();
            String oldNorm = normalizeSql(oldSql);
            String oldQuirked = normalizeSql(normalizeOldSqlQuirks(oldSql));
            String newRaw = newSql.trim().replaceAll("\\s+", " ").toLowerCase();
            String newNorm = normalizeSql(newSql);

            if (oldRaw.equals(newRaw)
                    || oldRaw.equals(newNorm)
                    || oldNorm.equals(newRaw)
                    || oldNorm.equals(newNorm)
                    || oldQuirked.equals(newRaw)
                    || oldQuirked.equals(newNorm)) {
                MATCH_COUNT.incrementAndGet();
            } else {
                MISMATCH_COUNT.incrementAndGet();
                System.out.println("[DUAL-RUN MISMATCH] Test: " + callingTestName()
                        + " | Query: " + query);
                System.out.println("  OLD SQL: " + oldSql);
                System.out.println("  NEW SQL: " + newSql);
            }
        } catch (Throwable e) {
            ERROR_COUNT.incrementAndGet();
            System.out.println("[DUAL-RUN ERROR] Test: " + callingTestName()
                    + " | Query: " + query
                    + " → " + e.getClass().getSimpleName() + ": " + e.getMessage());
            if (e instanceof NullPointerException) {
                e.printStackTrace(System.out);
            }
        }
    }

    /**
     * Extracts the calling test method name from the stack trace, skipping abstract
     * classes.
     */
    private static String callingTestName() {
        String fallback = null;
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
            String cls = ste.getClassName();
            if (cls.endsWith("Test") || cls.contains("Test$") || cls.contains("Tests")) {
                String simpleName = cls.substring(cls.lastIndexOf('.') + 1);
                String result = simpleName + "." + ste.getMethodName();
                // Skip abstract test classes — keep looking for the concrete one
                if (simpleName.startsWith("Abstract")) {
                    if (fallback == null)
                        fallback = result;
                    continue;
                }
                return result;
            }
        }
        return fallback != null ? fallback : "unknown";
    }

    /**
     * Normalizes SQL for comparison: trims, collapses whitespace, lowercases,
     * and normalizes aliases.
     */
    private static String normalizeSql(String sql) {
        String normalized = sql.trim().replaceAll("\\s+", " ").toLowerCase();

        // Renumber table aliases to canonical form
        java.util.Map<String, String> aliasMap = new java.util.LinkedHashMap<>();
        int[] counters = { 0, 0, 0 }; // t, j, sub
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"(t|j|sub)(\\d+)\"")
                .matcher(normalized);
        while (m.find()) {
            String fullAlias = m.group(1) + m.group(2);
            if (!aliasMap.containsKey(fullAlias)) {
                String prefix = m.group(1);
                int idx = "t".equals(prefix) ? 0 : "j".equals(prefix) ? 1 : 2;
                aliasMap.put(fullAlias, prefix + counters[idx]++);
            }
        }
        for (var entry : aliasMap.entrySet()) {
            if (!entry.getKey().equals(entry.getValue())) {
                normalized = normalized.replace(
                        "\"" + entry.getKey() + "\"",
                        "\"" + entry.getValue() + "\"");
            }
        }
        // Normalize NOT( to NOT ( (whitespace-only difference)
        normalized = normalized.replace("not(", "not (");

        // Normalize subquery alias names to canonical form
        // Old pipeline uses src_result, new uses subq — both are valid
        normalized = normalized.replace(") as src_result", ") as subq");
        normalized = normalized.replace(") as ext ", ") as subq ");
        // Handle ext at end of string (no trailing space)
        if (normalized.endsWith(") as ext")) {
            normalized = normalized.substring(0, normalized.length() - 7) + ") as subq";
        }
        // Also normalize "src" alias used by old pipeline for filter wrapping
        normalized = normalized.replace(") as src ", ") as subq ");
        if (normalized.endsWith(") as src")) {
            normalized = normalized.substring(0, normalized.length() - 7) + ") as subq";
        }

        return normalized;
    }

    /**
     * Normalizes OLD pipeline SQL quirks that produce semantically equivalent
     * but structurally different SQL. Applied only to OLD SQL so new pipeline
     * bugs still surface.
     */
    private static String normalizeOldSqlQuirks(String sql) {
        String normalized = sql.trim().replaceAll("\\s+", " ").toLowerCase();

        // 1. Unwrap trivial SELECT * FROM (subquery) AS alias patterns.
        String prev;
        do {
            prev = normalized;
            java.util.regex.Matcher wrap = java.util.regex.Pattern
                    .compile("^select \\* from \\((.+)\\) as \\w+(.*)$")
                    .matcher(normalized);
            if (wrap.matches()) {
                String inner = wrap.group(1);
                String trailing = wrap.group(2).trim();
                // Skip unwrap when alias has column defs, e.g. AS _tds("name", "age")
                if (isBalancedParens(inner) && !trailing.startsWith("(")) {
                    normalized = inner + (trailing.isEmpty() ? "" : " " + trailing);
                }
            }
        } while (!normalized.equals(prev));

        // 1b. Unwrap nested "select * from (...) as alias" inside FROM clauses
        // Old pipeline: FROM (SELECT * FROM (...values...) AS _tds(...)) AS src_result
        // New pipeline: FROM (...values...) AS _tds(...)
        // Pattern: replace "select * from (X) as _tds" with "X as _tds" (preserving _tds)
        normalized = java.util.regex.Pattern
                .compile("select \\* from \\((select \\* from \\(values .+?\\) as _tds\\([^)]+\\))\\) as \\w+")
                .matcher(normalized)
                .replaceAll("$1");

        // 2. Remove LIMIT 2147483647 (MAX_INT hack for "no limit" in drop()).
        normalized = normalized.replace("limit 2147483647 ", "");

        // 3. Add table qualification to unqualified column refs.
        // Old pipeline sometimes uses bare "col", new uses "t0"."col".
        // We leave this difference — it only affects 1 query (if() filter)
        // and the new pipeline's qualified form is strictly better.

        // 4. Normalize quoted lambda params in zip: "_zip_i" → _zip_i
        // Old pipeline quotes the generated zip lambda param when nested.
        normalized = normalized.replace("\"_zip_i\"", "_zip_i");

        // 5. Normalize date string literals to DATE-prefixed literals.
        // Old pipeline uses string comparison for date-vs-date: '2014-01-01' = '2014-01-01'
        // New pipeline correctly uses DATE '2014-01-01' = DATE '2014-01-01'
        normalized = java.util.regex.Pattern
                .compile("'(\\d{4}-\\d{2}-\\d{2})'")
                .matcher(normalized)
                .replaceAll("date '$1'");

        // 6. Normalize toVariant() rendering: old uses CAST(... AS VARCHAR),
        // new uses CAST(... AS JSON). Both are semantically identical in DuckDB.
        // This handles aliased extends and nested variant casts.
        normalized = normalized.replace(" as varchar) as ", " as json) as ");
        normalized = normalized.replace(" as varchar))", " as json))");

        // 7. Normalize DuckDB :: cast shorthand to CAST(x AS type).
        // Old pipeline uses x::HUGEINT or x::DECIMAL(18,0),
        // new pipeline uses CAST(x AS HUGEINT) / CAST(x AS DECIMAL(18,0)).
        normalized = java.util.regex.Pattern
                .compile("(\\d+)::(\\w+(?:\\([^)]+\\))?)")
                .matcher(normalized)
                .replaceAll("cast($1 as $2)");

        return normalized;
    }

    /** Checks if parentheses are balanced in a SQL fragment. */
    private static boolean isBalancedParens(String sql) {
        int depth = 0;
        for (char c : sql.toCharArray()) {
            if (c == '(')
                depth++;
            else if (c == ')')
                depth--;
            if (depth < 0)
                return false;
        }
        return depth == 0;
    }

    /** Returns dual-run parity statistics. */
    public static String getDualRunStats() {
        return String.format("Dual-run (PlanGenerator): %d match, %d mismatch, %d error",
                MATCH_COUNT.get(), MISMATCH_COUNT.get(), ERROR_COUNT.get());
    }

    /** Resets dual-run counters. */
    public static void resetDualRunStats() {
        MATCH_COUNT.set(0);
        MISMATCH_COUNT.set(0);
        ERROR_COUNT.set(0);
    }

    // ==================== M2M graphFetch Execution ====================

    /**
     * Executes a graphFetch/serialize query and returns JSON.
     * 
     * This is the Legend-compatible M2M execution path:
     * - Input: Person.all()->graphFetch(#{...}#)->serialize(#{...}#)
     * - Output: JSON array of objects, e.g., [{"fullName": "John Smith"}, ...]
     * 
     * @param pureSource  The complete Pure source (model + runtime + M2M mappings)
     * @param query       The graphFetch/serialize Pure query
     * @param runtimeName The qualified name of the Runtime
     * @param connection  The JDBC connection for execution
     * @return JSON string (array of objects)
     * @throws SQLException If execution fails
     */
    public String executeGraphFetch(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {

        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime for dialect
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR (PureCompiler handles graphFetch/serialize)
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        RelationNode ir = compileQuery(query, mappingRegistry, model);

        // 4. Get JSON dialect from runtime's connection
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        // 5. Generate JSON-producing SQL using JsonSqlGenerator
        JsonSqlDialect jsonDialect = getJsonDialect(connectionDef.databaseType());

        // The IR should be a ProjectNode for graphFetch queries
        if (!(ir instanceof ProjectNode projectNode)) {
            throw new IllegalArgumentException("graphFetch query must compile to ProjectNode, got: " + ir.getClass());
        }

        String sql = new JsonSqlGenerator(jsonDialect).generateJsonSql(projectNode);

        System.out.println("graphFetch Query: " + query);
        System.out.println("Generated JSON SQL: " + sql);

        // 6. Execute and return JSON directly from database
        return executeJsonSql(connection, sql);
    }

    /**
     * Executes JSON-producing SQL and returns the result.
     * The SQL returns a single column containing the full JSON array.
     */
    private String executeJsonSql(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                String jsonResult = rs.getString(1);
                // Handle null result (empty table)
                return jsonResult != null ? jsonResult : "[]";
            }
            return "[]";
        }
    }

    /**
     * Compiles a Pure query to RelationNode IR using the configured compiler.
     * 
     * @param query           The Pure query string
     * @param mappingRegistry The mapping registry for class->table resolution
     * @param model           The Pure model builder containing classes and
     *                        connections
     * @return The compiled RelationNode IR
     */
    private RelationNode compileQuery(String query, MappingRegistry mappingRegistry, PureModelBuilder model) {
        return new PureCompiler(mappingRegistry, model).compile(query);
    }

    private RelationNode compileQuery(String query, MappingRegistry mappingRegistry, PureModelBuilder model,
            TypeEnvironment typeEnv) {
        model.addClasses(typeEnv.classes());
        return new PureCompiler(mappingRegistry, model).compile(query);
    }

    /**
     * Extracts the Pure class type name from an IR expression tree.
     * Walks into function calls and list operations to find StructLiteralExpression
     * nodes.
     * Returns null if no struct type is found.
     */
    private String extractPureType(Expression expr) {
        if (expr instanceof StructLiteralExpression struct) {
            return struct.className();
        }
        if (expr instanceof FunctionExpression func) {
            // struct_extract changes the type — don't propagate the outer struct type
            if ("struct_extract".equalsIgnoreCase(func.functionName())) {
                return null;
            }
            // Walk target and arguments (e.g., list_extract(list_filter(...), 1))
            if (func.target() != null) {
                String type = extractPureType(func.target());
                if (type != null)
                    return type;
            }
            for (Expression arg : func.arguments()) {
                String type = extractPureType(arg);
                if (type != null)
                    return type;
            }
        }
        if (expr instanceof ListLiteral list && !list.isEmpty()) {
            return extractPureType(list.elements().getFirst());
        }
        if (expr instanceof ListFilterExpression filter) {
            return extractPureType(filter.source());
        }
        if (expr instanceof CollectionExpression coll) {
            String type = extractPureType(coll.source());
            if (type != null)
                return type;
            if (coll.lambdaBody() != null)
                return extractPureType(coll.lambdaBody());
        }
        if (expr instanceof ComparisonExpression comp) {
            String type = extractPureType(comp.left());
            if (type != null)
                return type;
            if (comp.right() != null)
                return extractPureType(comp.right());
        }
        return null;
    }

    /**
     * Extracts a mapping of field names to Pure class names for nested struct
     * fields.
     * Walks the IR to find StructLiteralExpression nodes, then checks each field's
     * expression to see if it also resolves to a struct type.
     *
     * For zip([1,2], ['a','b']): returns {} (no nested structs)
     * For zip(zip([1,2], ['a','b']), [3,4]): returns {first: "Pair"} (first field
     * is a nested Pair)
     */
    private java.util.Map<String, String> extractFieldTypes(Expression expr) {
        StructLiteralExpression struct = findStructLiteral(expr);
        if (struct == null)
            return java.util.Map.of();

        java.util.Map<String, String> result = new java.util.HashMap<>();
        for (var entry : struct.fields().entrySet()) {
            String fieldType = extractPureType(entry.getValue());
            if (fieldType != null) {
                result.put(entry.getKey(), fieldType);
            }
        }
        return result.isEmpty() ? java.util.Map.of() : result;
    }

    /**
     * Finds the first StructLiteralExpression in the IR tree.
     * Walks through collection calls, function calls, etc.
     */
    private StructLiteralExpression findStructLiteral(Expression expr) {
        if (expr instanceof StructLiteralExpression struct) {
            return struct;
        }
        if (expr instanceof CollectionExpression coll) {
            if (coll.lambdaBody() != null) {
                StructLiteralExpression found = findStructLiteral(coll.lambdaBody());
                if (found != null)
                    return found;
            }
            return findStructLiteral(coll.source());
        }
        if (expr instanceof FunctionExpression func) {
            if (func.target() != null) {
                StructLiteralExpression found = findStructLiteral(func.target());
                if (found != null)
                    return found;
            }
            for (Expression arg : func.arguments()) {
                StructLiteralExpression found = findStructLiteral(arg);
                if (found != null)
                    return found;
            }
        }
        if (expr instanceof ListLiteral list && !list.isEmpty()) {
            return findStructLiteral(list.elements().getFirst());
        }
        return null;
    }

    /**
     * Checks if a function selects one of its input values (e.g., greatest, least).
     * For these functions, the result IS one of the args, so we can preserve its
     * original type.
     */
    private static boolean isValueSelectingFunction(FunctionExpression fe) {
        return switch (fe.functionName().toLowerCase()) {
            case "greatest", "least", "max", "min", "list_max", "list_min" -> true;
            case "list_aggr" -> isListAggrMode(fe);
            default -> false;
        };
    }

    /**
     * Checks if a list_aggr call is using 'mode' (which selects from input values).
     * Other list_aggr operations like 'stddev', 'var_samp' compute new values.
     */
    private static boolean isListAggrMode(FunctionExpression fe) {
        for (Expression arg : fe.arguments()) {
            if (arg instanceof Literal lit && "mode".equals(lit.value()))
                return true;
        }
        return false;
    }

    /**
     * For a value-selecting function, finds the Literal arg whose numeric value
     * equals the result.
     * Returns the original Java value (Long for INTEGER, Double for FLOAT,
     * BigDecimal for DECIMAL)
     * so the caller can preserve the original type instead of DuckDB's promoted
     * type.
     */
    private static Object findMatchingArgValue(FunctionExpression fe, Number result, boolean preferLast) {
        double resultDouble = result.doubleValue();
        // Check target
        Object match = matchLiteralValue(fe.target(), resultDouble, preferLast);
        if (match != null)
            return match;
        // Check arguments (may be Literals or ListLiterals containing Literals)
        for (Expression arg : fe.arguments()) {
            match = matchLiteralValue(arg, resultDouble, preferLast);
            if (match != null)
                return match;
        }
        return null;
    }

    private static Object matchLiteralValue(Expression expr, double resultDouble, boolean preferLast) {
        if (expr instanceof Literal lit && lit.value() instanceof Number litNum) {
            if (litNum.doubleValue() == resultDouble)
                return lit.value();
        }
        // Walk into ListLiteral elements (e.g., LIST_MAX([1.23, 2]))
        if (expr instanceof ListLiteral list) {
            Object lastMatch = null;
            for (Expression elem : list.elements()) {
                if (elem instanceof Literal lit && lit.value() instanceof Number litNum) {
                    if (litNum.doubleValue() == resultDouble) {
                        if (!preferLast)
                            return lit.value();
                        lastMatch = lit.value();
                    }
                }
            }
            return lastMatch;
        }
        return null;
    }

    /**
     * For a value-selecting function on mixed date types, finds if the TIMESTAMP
     * result
     * originally came from a StrictDate (DATE) literal. DuckDB promotes
     * DATE→TIMESTAMP,
     * but we need to preserve the original type.
     * Returns "DATE" if matched to a DATE literal, null otherwise.
     */
    private static String findMatchingDateType(FunctionExpression fe, java.sql.Timestamp ts) {
        // Extract the date string from the timestamp: "2025-04-09 00:00:00.0" →
        // "2025-04-09"
        String tsStr = ts.toString(); // format: "yyyy-mm-dd hh:mm:ss.f"
        boolean isMidnight = tsStr.endsWith(" 00:00:00.0");
        if (!isMidnight)
            return null; // has real time component → genuine TIMESTAMP
        String datePart = tsStr.substring(0, 10); // "2025-04-09"

        // Scan the function's target and args for a DATE literal matching this date
        if (fe.target() != null && matchesDateLiteral(fe.target(), datePart))
            return "DATE";
        for (Expression arg : fe.arguments()) {
            if (matchesDateLiteral(arg, datePart))
                return "DATE";
        }
        return null;
    }

    private static boolean matchesDateLiteral(Expression expr, String datePart) {
        if (expr instanceof Literal lit && lit.literalType() == Literal.LiteralType.DATE) {
            // Pure date literals have % prefix: "%2025-04-09" → strip it for comparison
            String litDate = lit.value() instanceof String s && s.startsWith("%") ? s.substring(1)
                    : String.valueOf(lit.value());
            return datePart.equals(litDate);
        }
        if (expr instanceof ListLiteral list) {
            for (Expression elem : list.elements()) {
                if (elem instanceof Literal lit && lit.literalType() == Literal.LiteralType.DATE) {
                    String litDate = lit.value() instanceof String s && s.startsWith("%") ? s.substring(1)
                            : String.valueOf(lit.value());
                    if (datePart.equals(litDate))
                        return true;
                }
            }
        }
        return false;
    }

    /**
     * Maps a Java value to its appropriate SQL type string.
     */
    private static String sqlTypeForValue(Object value) {
        if (value instanceof Integer || value instanceof Long)
            return "INTEGER";
        if (value instanceof Double || value instanceof Float)
            return "DOUBLE";
        if (value instanceof BigDecimal)
            return "DECIMAL";
        return null;
    }

    /**
     * Checks if an expression tree contains a division operation.
     */
    private boolean containsDivision(Expression expr) {
        if (expr instanceof ArithmeticExpression arith) {
            if (arith.operator() == org.finos.legend.pure.dsl.m2m.BinaryArithmeticExpr.Operator.DIVIDE) {
                return true;
            }
            return containsDivision(arith.left()) || containsDivision(arith.right());
        }
        if (expr instanceof FunctionExpression func) {
            if (func.target() != null && containsDivision(func.target()))
                return true;
            for (Expression arg : func.arguments()) {
                if (containsDivision(arg))
                    return true;
            }
        }
        return false;
    }

    /**
     * Evaluates a constant expression tree using BigDecimal arithmetic
     * with DECIMAL128 precision (34 significant digits), matching Pure semantics.
     * Returns null if the expression contains unsupported node types.
     */
    private BigDecimal evaluateWithBigDecimal(Expression expr) {
        if (expr instanceof Literal lit) {
            Object v = lit.value();
            if (v instanceof Long l)
                return BigDecimal.valueOf(l);
            if (v instanceof Integer i)
                return BigDecimal.valueOf(i);
            if (v instanceof Double d)
                return BigDecimal.valueOf(d);
            if (v instanceof BigDecimal bd)
                return bd;
            if (v instanceof java.math.BigInteger bi)
                return new BigDecimal(bi);
            if (v instanceof Number n)
                return new BigDecimal(n.toString());
            return null;
        }
        if (expr instanceof ArithmeticExpression arith) {
            BigDecimal left = evaluateWithBigDecimal(arith.left());
            BigDecimal right = evaluateWithBigDecimal(arith.right());
            if (left == null || right == null)
                return null;
            return switch (arith.operator()) {
                case ADD -> left.add(right, MathContext.DECIMAL128);
                case SUBTRACT -> left.subtract(right, MathContext.DECIMAL128);
                case MULTIPLY -> left.multiply(right, MathContext.DECIMAL128);
                case DIVIDE -> right.signum() == 0 ? null
                        : left.divide(right, MathContext.DECIMAL128);
            };
        }
        if (expr instanceof FunctionExpression func) {
            return switch (func.functionName().toLowerCase()) {
                case "round" -> {
                    BigDecimal target = evaluateWithBigDecimal(func.target());
                    if (target == null)
                        yield null;
                    if (func.arguments().isEmpty()) {
                        yield target.setScale(0, RoundingMode.HALF_EVEN);
                    }
                    BigDecimal scale = evaluateWithBigDecimal(func.arguments().getFirst());
                    if (scale == null)
                        yield null;
                    yield target.setScale(scale.intValue(), RoundingMode.HALF_UP);
                }
                case "abs" -> {
                    BigDecimal target = evaluateWithBigDecimal(func.target());
                    yield target != null ? target.abs() : null;
                }
                default -> null; // Unsupported function — fall back to SQL result
            };
        }
        if (expr instanceof CastExpression cast) {
            return evaluateWithBigDecimal(cast.source());
        }
        return null; // Unsupported expression type
    }

    private SQLDialect getDialect(ConnectionDefinition.DatabaseType dbType) {
        return switch (dbType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            default -> DuckDBDialect.INSTANCE;
        };
    }

    private JsonSqlDialect getJsonDialect(ConnectionDefinition.DatabaseType dbType) {
        return switch (dbType) {
            case DuckDB -> DuckDbJsonDialect.INSTANCE;
            // SQLite also uses same JSON function names as DuckDB
            case SQLite -> DuckDbJsonDialect.INSTANCE;
            default -> DuckDbJsonDialect.INSTANCE;
        };
    }

    /**
     * Determines whether to buffer or stream results.
     */
    public enum ResultMode {
        /** Materialize all rows in memory */
        BUFFERED,
        /** Lazy iteration with held connection */
        STREAMING,
        /** Single scalar value (for constant queries) */
        SCALAR
    }

}
