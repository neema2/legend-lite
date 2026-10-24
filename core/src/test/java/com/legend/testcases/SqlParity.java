// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.exec.ExecutionResult;
import com.legend.json.Json;
import com.legend.protocol.sql.SqlLexer;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.testbackend.TestBackend;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Upstream's Postgres parity corpus and its rules, ported (docs/LEGEND_SQL_STUDY_2026_10_11.md §1.5): legend-engine's
 * {@code legend-engine-xt-sql-e2e-tests} at the pinned 4.145.0, its {@code parity-tests/**.yaml} cases over the tables
 * of its {@code schema.yaml}. Upstream runs each case on a live Postgres 16 (Testcontainers) and through Legend, and
 * compares the two; here Postgres 16's answers are snapped once into a committed file
 * ({@code core/src/test/resources/sql-parity/postgres-answers.jsonl}), which the Postgres lane checks against Postgres
 * itself on every run, and each case runs as a {@code #SQL{...}#} island. The corpus is TEST INPUT, read from the
 * pinned checkout (AGENTS.md, the reference-checkout tenet); nothing here is a runtime component.
 *
 * <p>Upstream's rules, kept: the tables seeded as its {@code SchemaManager} seeds them (a YAML decimal is bound as a
 * double, so {@code 99999999999999.99999} is stored as {@code 100000000000000.00000}, there and here); the FROM
 * clause's tables rewritten to the Legend source ({@code AstFromRewriter}: here {@code tb('e2e::DB.persons') AS
 * persons}); both results sorted unless the SQL says ORDER BY, then compared column by column, names without regard
 * to case, numbers with a tolerance when either is a float ({@code ResultComparator}).
 */
public final class SqlParity {

    /** The database every case's tables are in, and the runtime that reaches it. */
    public static final String DATABASE = "e2e::DB";
    public static final String RUNTIME = "e2e::RT";

    private SqlParity() {
    }

    // ------------------------------------------------------------------ the corpus

    /** A case: its file (relative to parity-tests/), its id, its SQL, and why upstream skips it, if it does. */
    public record Case(String file, String id, String sql, @com.legend.base.Nullable String skip,
            /** a statement that writes a table (the writes design's leg 4): the query that reads what it wrote, its
             *  answer snapped under the case's key and {@link #CHECK} */
            @com.legend.base.Nullable String check) {
        public Case(String file, String id, String sql, @com.legend.base.Nullable String skip) {
            this(file, id, sql, skip, null);
        }
    }

    /** The suffix of a writing case's key its check query's answer is snapped under. */
    public static final String CHECK = "#check";

    /** A table of the schema: its columns (name and SQL type, as upstream writes them) and its rows. */
    public record Table(String name, List<Column> columns, List<List<Object>> rows) {
    }

    public record Column(String name, String type) {
    }

    /** The cases whose Postgres answer is no fixed fact, so none is snapped: each is run by no lane (a SKIP). */
    static final Map<String, String> VOLATILE = Map.of(
            "structural/interval_arithmetic/interval_in_where_ts", "volatile: NOW() - INTERVAL '10 years' moves",
            // an answer measured from today cannot be snapped (2026-10-12: the two AGE cases drifted a day after the
            // snap, PR neema2/legend-lite#28's CI); a clock read compared with a fixed past date stays snapped
            "structural/interval_arithmetic/interval_in_where_filter", "volatile: CURRENT_DATE - INTERVAL '365 days' moves",
            "functions/datetime_functions/age__ts__from_table", "volatile: AGE(x) is measured from today",
            "functions/datetime_functions/age__tstz__from_table", "volatile: AGE(x) is measured from today",
            "functions/system_functions/version__basic", "volatile: VERSION() names the platform",
            "functions/math_functions/random_cov", "volatile: RANDOM()",
            "functions/network_functions/inet_client_port__unsupported", "volatile: the client's port");

    /** Every case, in file order (files by path), from the parity-tests directory. */
    public static List<Case> cases(Path dir) throws IOException {
        return cases(dir, "");
    }

    /** Every case of the case files under {@code dir}, each file named {@code prefix} and its path: upstream's
     *  ({@code ""}), or legend-lite's own ({@code "lite/"}, core/src/test/resources/sql-parity/lite). */
    public static List<Case> cases(Path dir, String prefix) throws IOException {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(dir)) {
            files = walk.filter(p -> p.toString().endsWith(".yaml") && !p.getFileName().toString().equals("schema.yaml"))
                    .sorted(Comparator.comparing(p -> dir.relativize(p).toString().replace('\\', '/'))).toList();
        }
        List<Case> cases = new ArrayList<>();
        for (Path f : files) {
            String file = prefix + dir.relativize(f).toString().replace('\\', '/');
            for (Map<String, Object> m : Yaml.tests(Files.readString(f, StandardCharsets.UTF_8), file)) {
                String id = (String) m.get("id");
                String sql = (String) m.get("sql");
                if (id == null || sql == null) {
                    throw new IllegalStateException(file + ": a case without an id or sql: " + m);
                }
                String skip = (String) m.get("skip");
                if (Boolean.TRUE.equals(m.get("join_func"))) {
                    // upstream runs these against a joined model function of its own e2e-model.pure, not this SQL
                    skip = "join_func: upstream's own joined-model function";
                }
                String check = (String) m.get("check");
                Case c = new Case(file, id, sql, skip, check);
                cases.add(VOLATILE.containsKey(key(c)) ? new Case(file, id, sql, VOLATILE.get(key(c)), check) : c);
            }
        }
        return cases;
    }

    /** The tables of {@code schema.yaml}. */
    public static List<Table> schema(Path dir) throws IOException {
        return Yaml.schema(Files.readString(dir.resolve("schema.yaml"), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------ the model and the data

    /** The Pure model of the schema: its store ({@link #store}), a connection and a runtime. */
    public static String model(List<Table> tables, String connection) {
        StringBuilder m = new StringBuilder(store(tables));
        m.append("###Connection\nRelationalDatabaseConnection e2e::Conn\n{\n    ").append(connection)
                .append("\n}\n###Runtime\nRuntime ").append(RUNTIME).append("\n{\n    mappings: [];\n    connections: [ ")
                .append(DATABASE).append(": [ environment: e2e::Conn ] ];\n}\n");
        return m.toString();
    }

    /** The schema's store: one database, each table's columns typed as legend-lite's relational store types them. */
    public static String store(List<Table> tables) {
        StringBuilder m = new StringBuilder("###Relational\nDatabase " + DATABASE + "\n(\n");
        for (Table t : tables) {
            m.append("    Table ").append(t.name()).append("\n    (\n");
            for (int i = 0; i < t.columns().size(); i++) {
                Column c = t.columns().get(i);
                m.append("        ").append(c.name()).append(' ').append(relationalType(c.type()))
                        .append(i + 1 < t.columns().size() ? ",\n" : "\n");
            }
            m.append("    )\n");
        }
        return m.append(")\n").toString();
    }

    private static String relationalType(String sqlType) {
        String t = sqlType.toUpperCase(Locale.ROOT);
        if (t.startsWith("VARCHAR(") || t.startsWith("DECIMAL(")) {
            return t;
        }
        return switch (t) {
            case "INTEGER", "BIGINT", "DATE", "TIMESTAMP", "TIME" -> t;
            case "DOUBLE PRECISION" -> "DOUBLE";
            case "BOOLEAN" -> "BIT";
            case "JSON", "JSONB" -> "SEMISTRUCTURED";
            default -> throw new IllegalStateException("no relational type for the schema's " + sqlType);
        };
    }

    /** Creates and fills the tables on {@code backend}'s database as the product creates a declared table: each table as
     *  the schema's {@link #store} declares it, rendered by the backend's dialect ({@code setup.Ddl}), and each value
     *  bound by its YAML type. On Postgres, the database the answers were snapped on, the lanes seed upstream's own
     *  fixture instead ({@link PostgresSeed}). */
    public static void seed(TestBackend backend, Connection c, List<Table> tables) throws SQLException {
        com.legend.sql.dialect.SqlDialect dialect = backend.dialect();
        Map<String, com.legend.model.DatabaseDefinition.TableDefinition> declared = new LinkedHashMap<>();
        for (var element : com.legend.Compiler.parseModel(store(tables)).elements()) {
            if (element instanceof com.legend.model.DatabaseDefinition db) {
                for (var table : db.tables()) {
                    declared.put(table.name(), table);
                }
            }
        }
        for (Table t : tables) {
            var table = java.util.Objects.requireNonNull(declared.get(t.name()), t.name());
            try (Statement s = c.createStatement()) {
                s.execute(dialect.render(new com.legend.sql.SqlDdl.DropTable(null, table.name(),
                        com.legend.setup.Ddl.quoting(table, null))));
                s.execute(dialect.render(com.legend.setup.Ddl.createTable(table, null)));
            }
            fill(c, dialect.storedName(table.name(), table.quoted()), t, (ps, i, type, json) -> ps.setString(i, json));
        }
    }

    /** How a JSON value is bound: its column's declared type ({@code JSON} or {@code JSONB}) and its text. */
    public interface JsonBinding {
        void bind(PreparedStatement ps, int i, String type, String json) throws SQLException;
    }

    /** Fills {@code t}'s rows into {@code target} (the table as the session's SQL spells it), each value bound as
     *  upstream's {@code SchemaManager} binds it, by its YAML type: a JSON value by {@code json}. */
    public static void fill(Connection c, String target, Table t, JsonBinding json) throws SQLException {
        if (t.rows().isEmpty()) {
            return;
        }
        String marks = String.join(", ", java.util.Collections.nCopies(t.columns().size(), "?"));
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + target + " VALUES (" + marks + ")")) {
            for (List<Object> row : t.rows()) {
                for (int i = 0; i < row.size(); i++) {
                    bind(ps, i + 1, row.get(i), t.columns().get(i).type().toUpperCase(Locale.ROOT), json);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** One value, bound as SchemaManager binds it. */
    private static void bind(PreparedStatement ps, int i, @com.legend.base.Nullable Object val, String type,
            JsonBinding json) throws SQLException {
        if (val == null) {
            ps.setNull(i, java.sql.Types.NULL);
        } else if (val instanceof Integer n) {
            ps.setInt(i, n);
        } else if (val instanceof Long n) {
            ps.setLong(i, n);
        } else if (val instanceof Double d) {
            ps.setDouble(i, d);
        } else if (val instanceof Boolean b) {
            ps.setBoolean(i, b);
        } else if (type.contains("DATE") && !type.contains("TIMESTAMP")) {
            // (the JDK's LocalDate, its proleptic Gregorian calendar: java.sql.Date's is Julian before 1582, so the
            // year 1 would move)
            ps.setObject(i, java.time.LocalDate.parse(val.toString()));
        } else if (type.contains("TIMESTAMP")) {
            ps.setTimestamp(i, java.sql.Timestamp.valueOf(val.toString()));
        } else if (type.contains("TIME")) {
            // a time of day: bound as the JDK's (DuckDB's and pgjdbc's TIME)
            ps.setObject(i, java.time.LocalTime.parse(val.toString()));
        } else if (type.contains("JSON")) {
            json.bind(ps, i, type, val.toString());
        } else {
            ps.setString(i, val.toString());
        }
    }

    // ------------------------------------------------------------------ the rewrite

    private static final Set<String> CLAUSE_WORDS = Set.of("select", "where", "group", "having", "order", "limit",
            "offset", "fetch", "window", "union", "intersect", "except", "on", "using", "returning", "into");

    /** The words that cannot be a table's alias (Postgres's reserved and type-or-function key words). */
    private static final Set<String> NOT_AN_ALIAS = Set.of("all", "analyse", "analyze", "and", "any", "array", "as",
            "asc", "asymmetric", "both", "case", "cast", "check", "collate", "column", "constraint", "create",
            "current_catalog", "current_date", "current_role", "current_time", "current_timestamp", "current_user",
            "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for",
            "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into", "lateral", "leading",
            "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or", "order", "placing",
            "primary", "references", "returning", "select", "session_user", "some", "symmetric", "system_user",
            "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when", "where",
            "window", "with", "authorization", "binary", "collation", "concurrently", "cross", "current_schema",
            "freeze", "full", "ilike", "inner", "is", "isnull", "join", "left", "like", "natural", "notnull", "outer",
            "overlaps", "right", "similar", "tablesample", "verbose");

    /** {@code sql} with each schema table in a FROM list or after JOIN ({@code persons}, {@code public.persons})
     *  replaced by its Legend source, {@code tb('e2e::DB.persons')}, aliased by the name as written when no alias
     *  follows (Postgres names an unaliased function in FROM after the function). What is not a schema table there
     *  (a CTE's name, {@code information_schema.tables}, {@code EXTRACT(year FROM o.d)}) is left as it is; text that
     *  does not lex is returned as it is, for the island to refuse. A parenthesis opened where a table goes is a
     *  join's ({@code FROM (x LEFT JOIN y ON ...)}), so a name just inside it is a table too; so is the name of
     *  {@code TABLE name}, and a name's {@code *} (its descendants) goes with it. */
    public static String rewrite(String sql, Set<String> tables) {
        List<SqlLexer.Token> tokens;
        try {
            tokens = SqlLexer.tokenize(sql);
        } catch (SqlSyntaxException e) {
            return sql;
        }
        int[] lineStarts = lineStarts(sql);
        StringBuilder out = new StringBuilder();
        int copied = 0;
        Deque<Boolean> inFrom = new ArrayDeque<>();
        inFrom.push(false);
        // the parentheses opened where a table goes (2026-10-17: the regression suite's FROM (x LEFT JOIN y ...) read
        // its tables as bare names, 15 statements)
        Set<Integer> joinParens = new java.util.HashSet<>();
        for (int i = 0; i < tokens.size(); i++) {
            SqlLexer.Token t = tokens.get(i);
            switch (t.kind()) {
                case LPAREN -> {
                    if (i > 0 && tablePlace(tokens.get(i - 1), i - 1, inFrom, joinParens)) {
                        joinParens.add(i);
                    }
                    inFrom.push(false);
                    continue;
                }
                case RPAREN -> {
                    if (inFrom.size() > 1) {
                        inFrom.pop();
                    }
                    continue;
                }
                default -> {
                }
            }
            if (t.is("from")) {
                inFrom.pop();
                inFrom.push(true);
                continue;
            }
            if (t.kind() == SqlLexer.Kind.IDENT && CLAUSE_WORDS.contains(t.text().toLowerCase(Locale.ROOT))) {
                inFrom.pop();
                inFrom.push(false);
                continue;
            }
            if (i == 0) {
                continue;
            }
            if (!tablePlace(tokens.get(i - 1), i - 1, inFrom, joinParens) || !isName(t)) {
                continue;
            }
            int last = i;
            boolean dotted = at(tokens, i + 1).kind() == SqlLexer.Kind.DOT && isName(at(tokens, i + 2));
            // a table of another schema, named "schema.name" among the tables (2026-10-17, the regression suite)
            String qualified = dotted ? folded(t) + "." + folded(at(tokens, i + 2)) : null;
            if (qualified != null && tables.contains(qualified)
                    || dotted && t.kind() == SqlLexer.Kind.IDENT && t.text().equalsIgnoreCase("public")) {
                last = i + 2;
            }
            SqlLexer.Token name = tokens.get(last);
            SqlLexer.Token after = at(tokens, last + 1);
            String table = last > i && tables.contains(qualified) ? qualified : folded(name);
            boolean into = prev(tokens, i).is("into");
            if (!tables.contains(table) || after.kind() == SqlLexer.Kind.DOT
                    || after.kind() == SqlLexer.Kind.LPAREN && !into) {
                continue;
            }
            int start = offset(lineStarts, t);
            int end = offset(lineStarts, name) + sourceLength(sql, offset(lineStarts, name), name);
            SqlLexer.Token star = after;
            if (after.isOp("*") && last + 1 < tokens.size() - 1) {
                // FROM a_star*: the table's descendants too, which its source reads anyway
                after = at(tokens, last + 2);
            }
            out.append(sql, copied, start).append("tb('").append(DATABASE).append('.').append(table).append("')");
            if ((!hasAlias(after) || prev(tokens, i).is("update") && after.is("set")) && !prev(tokens, i).is("table")) {
                out.append(" AS ").append(sql, offset(lineStarts, name), end);
            }
            copied = end;
            if (star != after) {
                copied = offset(lineStarts, star) + 1;
                last++;
            }
            i = last;
        }
        return out.append(sql, copied, sql.length()).toString();
    }

    /** Whether the token after {@code prev} (at {@code index}) is where a FROM list's table goes. */
    private static boolean tablePlace(SqlLexer.Token prev, int index, Deque<Boolean> inFrom, Set<Integer> joinParens) {
        return prev.is("from") || prev.is("join") || prev.is("table") || joinParens.contains(index)
                || prev.kind() == SqlLexer.Kind.COMMA && Boolean.TRUE.equals(inFrom.peek())
                // (a statement that writes a table names it after INTO or UPDATE, and DELETE's other tables after
                // USING: the writes design's leg 4; a join's USING is followed by its column list)
                || prev.is("into") || index == 0 && prev.is("update") || prev.is("using");
    }

    private static SqlLexer.Token prev(List<SqlLexer.Token> tokens, int i) {
        return tokens.get(i - 1);
    }

    private static SqlLexer.Token at(List<SqlLexer.Token> tokens, int i) {
        return tokens.get(Math.min(i, tokens.size() - 1));
    }

    private static boolean isName(SqlLexer.Token t) {
        return t.kind() == SqlLexer.Kind.IDENT || t.kind() == SqlLexer.Kind.QUOTED_IDENT;
    }

    private static String folded(SqlLexer.Token t) {
        return t.kind() == SqlLexer.Kind.QUOTED_IDENT ? t.text() : t.text().toLowerCase(Locale.ROOT);
    }

    private static boolean hasAlias(SqlLexer.Token next) {
        return next.is("as") || next.kind() == SqlLexer.Kind.QUOTED_IDENT
                || next.kind() == SqlLexer.Kind.IDENT && !NOT_AN_ALIAS.contains(next.text().toLowerCase(Locale.ROOT));
    }

    private static int[] lineStarts(String sql) {
        List<Integer> starts = new ArrayList<>(List.of(0));
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '\n') {
                starts.add(i + 1);
            }
        }
        return starts.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int offset(int[] lineStarts, SqlLexer.Token t) {
        return lineStarts[t.pos().line() - 1] + t.pos().column() - 1;
    }

    /** How many characters of the source the name token spans: its text, or its quoted form with doubled quotes. */
    private static int sourceLength(String sql, int start, SqlLexer.Token t) {
        if (t.kind() == SqlLexer.Kind.IDENT) {
            return t.text().length();
        }
        int i = start + 1;
        while (true) {
            if (sql.charAt(i) == '"') {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
                    i += 2;
                    continue;
                }
                return i + 1 - start;
            }
            i++;
        }
    }

    // ------------------------------------------------------------------ results

    /** A result: its column names, and its rows of cells. A cell is null, a {@link Long}, a {@link BigInteger}, a
     *  {@link BigDecimal}, a {@link Double}, a {@link Boolean} or a {@link String} (dates and timestamps in one
     *  spelling, {@code 2020-01-15}, {@code 2023-01-10 09:00:00}, whichever side made them). */
    public record Grid(List<String> columns, List<List<Object>> rows) {
    }

    /** A JDBC result, as upstream's {@code DirectPostgresRunner} reads one: labels, and {@code getObject} cells; a
     *  Postgres date or timestamp as its java.time value, as legend-lite's executor reads one (2026-10-17, the
     *  regression suite: through java.sql.Date and Timestamp a BC year lost its era, and 'infinity' was a date in the
     *  year 292278994). */
    public static Grid grid(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        List<String> columns = new ArrayList<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            columns.add(meta.getColumnLabel(i));
        }
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columns.size(); i++) {
                // pgjdbc's type names: a timestamptz has no LocalDateTime, and another driver's names are its own
                row.add(cell(switch (meta.getColumnTypeName(i)) {
                    case "date" -> rs.getObject(i, java.time.LocalDate.class);
                    case "timestamp" -> rs.getObject(i, java.time.LocalDateTime.class);
                    default -> rs.getObject(i);
                }));
            }
            rows.add(row);
        }
        return new Grid(columns, rows);
    }

    /** A legend-lite result. */
    public static Grid grid(ExecutionResult result) {
        if (result instanceof ExecutionResult.Scalar s) {
            // (a statement that writes a table: its count of rows, as Postgres's update count is read)
            return new Grid(List.of("count"), List.of(java.util.Collections.singletonList(cell(s.value()))));
        }
        List<String> columns = result.columns().stream().map(c -> c.name()).toList();
        List<List<Object>> rows = new ArrayList<>();
        for (var row : result.rows()) {
            List<Object> cells = new ArrayList<>();
            for (Object v : row.values()) {
                cells.add(cell(v));
            }
            rows.add(cells);
        }
        return new Grid(columns, rows);
    }

    /** A value, in a grid's cell types. */
    static @com.legend.base.Nullable Object cell(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> null;
            case Integer n -> n.longValue();
            case Short n -> n.longValue();
            case Byte n -> n.longValue();
            case Long n -> n;
            case BigInteger n -> n;
            case BigDecimal n -> n;
            case Float f -> f.doubleValue();
            case Double d -> d;
            case Boolean b -> b;
            // a json or jsonb value, from Postgres (a PGobject) or from DuckDB (its JsonNode): the JSON value it holds
            case Object o when o.getClass().getName().equals("org.postgresql.util.PGobject")
                    && JSON_TYPES.contains(pgObject(o, "getType")) -> JsonValue.of(pgObject(o, "getValue"));
            // (DuckDB's JSON holds a double that is not finite, NaN or an infinity, as a bare token: NonFiniteJson)
            case Object o when o.getClass().getName().equals("org.duckdb.JsonNode") -> JsonValue.ofNonFinite(
                    o.toString());
            case java.sql.Timestamp ts -> timestamp(ts.toLocalDateTime());
            case java.sql.Date d -> d.toLocalDate().toString();
            case java.time.LocalDate d -> d.toString();
            case java.time.LocalDateTime ts -> timestamp(ts);
            // legend-lite's dates: a date-time is GMT-normalized, as the Postgres lane's session (TimeZone UTC) reads
            // a TIMESTAMP
            case com.legend.values.PureDateLiteral.StrictDate d -> java.time.LocalDate.of(d.year(), d.month(),
                    d.day()).toString();
            case com.legend.values.PureDateLiteral.DateWithHour d -> timestamp(java.time.LocalDateTime.of(d.year(),
                    d.month(), d.day(), d.hour(), 0));
            case com.legend.values.PureDateLiteral.DateWithMinute d -> timestamp(java.time.LocalDateTime.of(d.year(),
                    d.month(), d.day(), d.hour(), d.minute()));
            case com.legend.values.PureDateLiteral.DateWithSecond d -> timestamp(java.time.LocalDateTime.of(d.year(),
                    d.month(), d.day(), d.hour(), d.minute(), d.second()));
            case com.legend.values.PureDateLiteral.DateWithSubsecond d -> {
                String base = timestamp(java.time.LocalDateTime.of(d.year(), d.month(), d.day(), d.hour(), d.minute(),
                        d.second()));
                String fraction = d.subsecond().replaceAll("0+$", "");
                yield fraction.isEmpty() ? base : base + "." + fraction;
            }
            // a bytea as Postgres writes one (2026-10-12: as an array's toString it was the JVM's identity hash, which
            // matched from run to run on Linux only, PR neema2/legend-lite#28's macOS and Windows CI)
            case byte[] b -> "\\x" + java.util.HexFormat.of().formatHex(b);
            default -> v.toString();
        };
    }

    private static final Set<String> JSON_TYPES = Set.of("json", "jsonb");

    /** {@code g} with each text cell that holds a JSON object or array as that JSON's canonical text: register S49's
     *  difference, where the island writes a {@code json} value's object or array as {@code jsonb} does and Postgres
     *  keeps the text as written, so two grids that agree so differ only by it. */
    public static Grid jsonTextAsJson(Grid g) {
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : g.rows()) {
            List<Object> cells = new ArrayList<>();
            for (Object v : row) {
                cells.add(v instanceof String s && jsonContainerText(s) ? canonicalOrSelf(s) : v);
            }
            rows.add(cells);
        }
        return new Grid(g.columns(), rows);
    }

    private static String canonicalOrSelf(String s) {
        try {
            return JsonValue.of(s).toString();
        } catch (RuntimeException notJson) {
            return s;
        }
    }

    private static boolean jsonContainerText(String s) {
        String t = s.strip();
        return t.startsWith("{") && t.endsWith("}") || t.startsWith("[") && t.endsWith("]");
    }

    private static String pgObject(Object o, String getter) {
        try {
            return String.valueOf(o.getClass().getMethod(getter).invoke(o));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A JSON value in a cell, as its canonical text: an object's keys sorted, a number by its value, no spaces. Two
     * cells holding JSON agree when their values do: Postgres's jsonb writes a space after each ',' and ':' and keeps
     * its own key order (the shorter key first), DuckDB's JSON neither, and legend-lite's Variant is either, by the
     * database it runs on; a JSON object's keys have no order (RFC 8259).
     */
    public record JsonValue(String canonical) {

        static JsonValue of(String text) {
            return new JsonValue(canonical(Json.parse(text)));
        }

        /** A JSON text that may hold a double that is not finite as a bare token, as DuckDB's JSON writes one
         *  ({@link NonFiniteJson}). */
        static JsonValue ofNonFinite(String text) {
            return new JsonValue(canonical(NonFiniteJson.parse(text)));
        }

        private static String canonical(Json.Node n) {
            return switch (n) {
                case Json.Obj o -> {
                    StringBuilder b = new StringBuilder("{");
                    o.fields().keySet().stream().sorted().forEach(k -> b.append(b.length() > 1 ? "," : "")
                            .append(Json.toCompact(k)).append(':').append(canonical(o.fields().get(k))));
                    yield b.append('}').toString();
                }
                case Json.Arr a -> "[" + String.join(",", a.items().stream().map(JsonValue::canonical).toList()) + "]";
                case Json.Str str -> Json.toCompact(str.value());
                case Json.Num num when num.decimalValue() == null && num.token() == null
                        && !Double.isFinite(num.doubleValue()) -> Double.isNaN(num.doubleValue()) ? "NaN"
                                : num.doubleValue() > 0 ? "Infinity" : "-Infinity";
                case Json.Num num -> {
                    BigDecimal d = num.decimalValue() != null ? num.decimalValue()
                            : new BigDecimal(num.token() != null ? num.token() : Double.toString(num.doubleValue()));
                    yield d.signum() == 0 ? "0" : d.stripTrailingZeros().toPlainString();
                }
                case Json.Bool bool -> Boolean.toString(bool.value());
                case Json.Null ignored -> "null";
            };
        }

        @Override
        public String toString() {
            return canonical;
        }
    }

    /** {@code 2023-01-10 09:00:00}, a fraction of a second only when there is one ({@code .5}, never {@code .0}). */
    private static String timestamp(java.time.LocalDateTime ts) {
        String s = String.format(Locale.ROOT, "%s %02d:%02d:%02d", ts.toLocalDate(), ts.getHour(), ts.getMinute(),
                ts.getSecond());
        return ts.getNano() == 0 ? s
                : s + "." + String.format(Locale.ROOT, "%09d", ts.getNano()).replaceAll("0+$", "");
    }

    /** Upstream's comparison ({@code ResultComparator.compare}), each result first sorted unless the SQL has an
     *  ORDER BY ({@code TestPostgresParity}): the differences, none when the results agree. One refinement: when
     *  every ORDER BY key is an output column (by position or name), the order among rows tied on those keys is the
     *  database's, not the query's; then the rows must agree as a set and the keys in sequence. */
    public static List<String> compare(Case c, Grid expected, Grid actual) {
        boolean ordered = c.sql().toUpperCase(Locale.ROOT).contains("ORDER BY");
        return compare(ordered, ordered ? orderKeys(c.sql(), expected.columns()) : null, expected, actual);
    }

    /** {@link #compare(Case, Grid, Grid)} for a query read by another reader (the {@code #IQ{...}#} island's harness,
     *  IqParity): whether it is {@code ordered}, and its ORDER BY's output columns when each key is one (else null). */
    public static List<String> compare(boolean ordered, @com.legend.base.Nullable List<Integer> keys, Grid expected,
            Grid actual) {
        if (keys != null) {
            List<String> diffs = compare(sorted(expected), sorted(actual));
            for (int r = 0; r < Math.min(expected.rows().size(), actual.rows().size()) && diffs.isEmpty(); r++) {
                for (int k : keys) {
                    Object ev = expected.rows().get(r).get(k);
                    Object av = actual.rows().get(r).get(k);
                    if (!cellEquals(ev, av)) {
                        diffs.add("Row " + r + ", sort key '" + expected.columns().get(k) + "': expected '" + ev
                                + "' but got '" + av + "'");
                    }
                }
            }
            return diffs;
        }
        return compare(ordered ? expected : sorted(expected), ordered ? actual : sorted(actual));
    }

    /** The output columns a query's top-level ORDER BY sorts on, when each key is one (an ordinal, or an output
     *  column's name); null otherwise, or when the SQL does not parse. */
    private static @com.legend.base.Nullable List<Integer> orderKeys(String sql, List<String> columns) {
        com.legend.protocol.sql.SqlAst.Statement st;
        try {
            st = com.legend.protocol.sql.SqlParser.parse(sql);
        } catch (RuntimeException e) {
            return null;
        }
        List<com.legend.protocol.sql.SqlAst.SortItem> orderBy = switch (st) {
            case com.legend.protocol.sql.SqlAst.Select q -> q.orderBy();
            case com.legend.protocol.sql.SqlAst.SetOperation q -> q.orderBy();
            case com.legend.protocol.sql.SqlAst.With w -> List.of();
            case com.legend.protocol.sql.SqlAst.Write w -> List.of();
        };
        if (orderBy.isEmpty()) {
            return null;
        }
        List<Integer> keys = new ArrayList<>();
        for (com.legend.protocol.sql.SqlAst.SortItem item : orderBy) {
            if (item.expr() instanceof com.legend.protocol.sql.SqlAst.IntegerLit n && n.value().signum() > 0
                    && n.value().intValue() <= columns.size()) {
                keys.add(n.value().intValue() - 1);
            } else if (item.expr() instanceof com.legend.protocol.sql.SqlAst.ColumnRef ref && ref.parts().size() == 1) {
                int found = -1;
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).equalsIgnoreCase(ref.parts().get(0).text())) {
                        found = found == -1 ? i : -2;
                    }
                }
                if (found < 0) {
                    return null;
                }
                keys.add(found);
            } else {
                return null;
            }
        }
        return keys;
    }

    private static List<String> compare(Grid e, Grid a) {
        List<String> diffs = new ArrayList<>();
        if (e.columns().size() != a.columns().size()) {
            diffs.add("Column count mismatch: expected " + e.columns().size() + " but got " + a.columns().size());
            return diffs;
        }
        for (int i = 0; i < e.columns().size(); i++) {
            if (!e.columns().get(i).equalsIgnoreCase(a.columns().get(i))) {
                diffs.add("Column name mismatch at index " + i + ": expected '" + e.columns().get(i) + "' but got '"
                        + a.columns().get(i) + "'");
            }
        }
        if (e.rows().size() != a.rows().size()) {
            diffs.add("Row count mismatch: expected " + e.rows().size() + " but got " + a.rows().size());
            return diffs;
        }
        for (int r = 0; r < e.rows().size() && diffs.size() < 10; r++) {
            for (int col = 0; col < e.columns().size() && diffs.size() < 10; col++) {
                Object ev = e.rows().get(r).get(col);
                Object av = a.rows().get(r).get(col);
                if (!cellEquals(ev, av)) {
                    diffs.add("Row " + r + ", column '" + e.columns().get(col) + "': expected '" + ev + "' but got '"
                            + av + "'");
                }
            }
        }
        return diffs;
    }

    private static final double ABSOLUTE_EPSILON = 1e-9;
    private static final double RELATIVE_EPSILON = 1e-6;

    /** {@code ResultComparator.cellEquals}, as upstream wrote it. */
    public static boolean cellEquals(@com.legend.base.Nullable Object expected, @com.legend.base.Nullable Object actual) {
        if (expected == null || actual == null) {
            return expected == actual;
        }
        if (expected instanceof Number en && actual instanceof Number an) {
            double ev = en.doubleValue();
            double av = an.doubleValue();
            if (Double.isNaN(ev) && Double.isNaN(av)) {
                return true;
            }
            if (Double.isInfinite(ev) && Double.isInfinite(av)) {
                return ev == av;
            }
            if (expected instanceof BigDecimal eb && actual instanceof BigDecimal ab) {
                return eb.compareTo(ab) == 0;
            }
            if (expected instanceof Double || actual instanceof Double) {
                double diff = Math.abs(ev - av);
                double maxAbs = Math.max(Math.abs(ev), Math.abs(av));
                return diff <= Math.max(ABSOLUTE_EPSILON, RELATIVE_EPSILON * maxAbs);
            }
            return ev == av;
        }
        if (expected instanceof Boolean && actual instanceof Boolean) {
            return expected.equals(actual);
        }
        // a JSON value is only ever a JSON value: a jsonb 42 is not the integer 42, nor a jsonb "x" the text x
        if (expected instanceof JsonValue || actual instanceof JsonValue) {
            return expected.equals(actual);
        }
        // the text of a JSON object or array (->> of one, register S49): the same JSON, in DuckDB's compact spelling or
        // jsonb's. Only where Postgres's text IS jsonb's spelling of its value (a space after each ',' and ':', keys in
        // jsonb's order): any other text is compared as text
        if (expected instanceof String es && actual instanceof String as && jsonContainerText(es)
                && jsonContainerText(as)) {
            try {
                return jsonbSpelling(Json.parse(es)).equals(es) ? JsonValue.of(es).equals(JsonValue.of(as))
                        : es.equals(as);
            } catch (RuntimeException notJson) {
                return es.equals(as);
            }
        }
        return expected.toString().equals(actual.toString());
    }

    /** jsonb's text of a JSON value: {@code ", "} and {@code ": "} between items, an object's keys shorter first, then
     *  by their bytes (jsonb's order), numbers as spelled. */
    static String jsonbSpelling(Json.Node n) {
        return switch (n) {
            case Json.Obj o -> {
                List<String> keys = new ArrayList<>(o.fields().keySet());
                keys.sort(java.util.Comparator.comparingInt((String k) -> k.getBytes(StandardCharsets.UTF_8).length)
                        .thenComparing(k -> new String(k.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1)));
                yield "{" + String.join(", ", keys.stream().map(k -> Json.toCompact(k) + ": "
                        + jsonbSpelling(o.fields().get(k))).toList()) + "}";
            }
            case Json.Arr a -> "[" + String.join(", ", a.items().stream().map(SqlParity::jsonbSpelling).toList()) + "]";
            case Json.Num num -> num.token() != null ? num.token() : Json.toCompact(num);
            default -> Json.toCompact(n);
        };
    }

    /** {@code ResultMatrix.sorted}: rows ordered by every column, nulls first, numbers by value, the rest as text. */
    private static Grid sorted(Grid g) {
        List<List<Object>> rows = new ArrayList<>(g.rows());
        rows.sort((a, b) -> {
            for (int i = 0; i < a.size(); i++) {
                Object av = a.get(i);
                Object bv = b.get(i);
                if (av == null && bv == null) {
                    continue;
                }
                if (av == null) {
                    return -1;
                }
                if (bv == null) {
                    return 1;
                }
                int cmp = av instanceof Number an && bv instanceof Number bn
                        ? Double.compare(an.doubleValue(), bn.doubleValue()) : av.toString().compareTo(bv.toString());
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        });
        return new Grid(g.columns(), rows);
    }

    // ------------------------------------------------------------------ Postgres's answers, snapped

    /** What Postgres answered a case: its rows, or its error. */
    public sealed interface Answer permits Rows, Rejected {
    }

    public record Rows(Grid grid) implements Answer {
    }

    public record Rejected(String sqlState, String message) implements Answer {
    }

    /** The answers file: one JSON object per case, in corpus order, {@code {"id":…,"columns":[…],"rows":[[…]]}} or
     *  {@code {"id":…,"error":{"state":…,"message":…}}}. A cell is null or a string whose first character says its
     *  type: {@code i} integer, {@code d} decimal, {@code f} float, {@code b} boolean, {@code s} anything else. */
    public static Map<String, Answer> readAnswers(Path file) throws IOException {
        Map<String, Answer> answers = new LinkedHashMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            Json.Obj o = (Json.Obj) Json.parse(line);
            String key = ((Json.Str) o.get("id")).value();
            Json.Node error = o.getOr("error", null);
            if (error instanceof Json.Obj err) {
                answers.put(key, new Rejected(((Json.Str) err.get("state")).value(),
                        ((Json.Str) err.get("message")).value()));
                continue;
            }
            List<String> columns = new ArrayList<>();
            for (Json.Node n : ((Json.Arr) o.get("columns")).items()) {
                columns.add(((Json.Str) n).value());
            }
            List<List<Object>> rows = new ArrayList<>();
            for (Json.Node r : ((Json.Arr) o.get("rows")).items()) {
                List<Object> row = new ArrayList<>();
                for (Json.Node c : ((Json.Arr) r).items()) {
                    row.add(c instanceof Json.Str s ? decode(s.value()) : null);
                }
                rows.add(row);
            }
            answers.put(key, new Rows(new Grid(columns, rows)));
        }
        return answers;
    }

    public static void writeAnswers(Path file, List<Case> cases, Map<String, Answer> answers) throws IOException {
        StringBuilder out = new StringBuilder();
        List<String> keys = new ArrayList<>();
        for (Case c : cases) {
            keys.add(key(c));
            if (c.check() != null) {
                keys.add(key(c) + CHECK);
            }
        }
        for (String k : keys) {
            Answer a = answers.get(k);
            if (a == null) {
                continue;
            }
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("id", k);
            switch (a) {
                case Rejected r -> {
                    Map<String, Object> error = new LinkedHashMap<>();
                    error.put("state", r.sqlState());
                    error.put("message", r.message());
                    o.put("error", error);
                }
                case Rows rows -> {
                    o.put("columns", rows.grid().columns());
                    List<List<Object>> encoded = new ArrayList<>();
                    for (List<Object> row : rows.grid().rows()) {
                        List<Object> cells = new ArrayList<>();
                        for (Object v : row) {
                            cells.add(v == null ? null : encode(v));
                        }
                        encoded.add(cells);
                    }
                    o.put("rows", encoded);
                }
            }
            out.append(Json.toCompact(Json.of(o))).append('\n');
        }
        Files.writeString(file, out.toString(), StandardCharsets.UTF_8);
    }

    /** A case's key in the answers and the ledger: upstream's ids are unique only within a file. */
    public static String key(Case c) {
        return c.file().replaceAll("\\.yaml$", "") + "/" + c.id();
    }

    /** The largest relative difference between two doubles of one answer from Postgres on two platforms. */
    private static final double PLATFORM_EPSILON = 1e-12;

    /**
     * Whether the answers file's {@code snapped} answer is Postgres's {@code live} one: their encodings agree, but for
     * doubles that differ only in their last digits. Postgres's float functions are the platform's C library's, which
     * differ in the last place between Linux, macOS and Windows (2026-10-12: {@code cbrt}, {@code atan}, {@code log}
     * and others, PR neema2/legend-lite#28's CI); the judge compares doubles within a far wider tolerance.
     */
    public static boolean agrees(Answer snapped, Answer live) {
        if (snapped instanceof Rows s && live instanceof Rows l && s.grid().columns().equals(l.grid().columns())
                && s.grid().rows().size() == l.grid().rows().size()) {
            for (int r = 0; r < s.grid().rows().size(); r++) {
                List<Object> sr = s.grid().rows().get(r);
                List<Object> lr = l.grid().rows().get(r);
                if (sr.size() != lr.size()) {
                    return false;
                }
                for (int c = 0; c < sr.size(); c++) {
                    Object sv = sr.get(c);
                    Object lv = lr.get(c);
                    boolean same = sv instanceof Double sd && lv instanceof Double ld
                            ? sd.equals(ld)
                                    || Math.abs(sd - ld) <= PLATFORM_EPSILON * Math.max(Math.abs(sd), Math.abs(ld))
                            : (sv == null ? lv == null : lv != null && encode(sv).equals(encode(lv)));
                    if (!same) {
                        return false;
                    }
                }
            }
            return true;
        }
        return encoded(snapped).equals(encoded(live));
    }

    /** An answer exactly as it would be written. */
    public static String encoded(Answer a) {
        return switch (a) {
            case Rejected r -> "error " + r.sqlState() + ": " + r.message();
            case Rows rows -> {
                List<Object> cells = new ArrayList<>();
                for (List<Object> row : rows.grid().rows()) {
                    List<Object> encodedRow = new ArrayList<>();
                    for (Object v : row) {
                        encodedRow.add(v == null ? null : encode(v));
                    }
                    cells.add(encodedRow);
                }
                Map<String, Object> o = new LinkedHashMap<>();
                o.put("columns", rows.grid().columns());
                o.put("rows", cells);
                yield Json.toCompact(Json.of(o));
            }
        };
    }

    private static String encode(Object v) {
        return switch (v) {
            case Long n -> "i" + n;
            case BigInteger n -> "i" + n;
            case BigDecimal n -> "d" + n.toPlainString();
            case Double d -> "f" + d;
            case Boolean b -> "b" + b;
            case JsonValue j -> "j" + j.canonical();
            default -> "s" + v;
        };
    }

    private static Object decode(String s) {
        String v = s.substring(1);
        return switch (s.charAt(0)) {
            case 'i' -> {
                BigInteger n = new BigInteger(v);
                yield n.bitLength() < 64 ? (Object) n.longValue() : n;
            }
            case 'd' -> new BigDecimal(v);
            case 'f' -> Double.valueOf(v);
            case 'b' -> Boolean.valueOf(v);
            case 'j' -> new JsonValue(v);
            case 's' -> v;
            default -> throw new IllegalStateException("a cell of no known type: " + s);
        };
    }

    // ------------------------------------------------------------------ statuses, and the ledger that pins them

    /** A case's outcome in one lane. */
    public enum Status {
        /** the rows Postgres returns */
        PASS,
        /** other rows than Postgres's: a wrong answer */
        FAIL,
        /** refused as not supported yet */
        REFUSED,
        /** any other error where Postgres answers */
        ERROR,
        /** Postgres rejects the query, and so does legend-lite */
        BOTH_ERROR,
        /** Postgres rejects the query and legend-lite answers it */
        BUG,
        /** upstream skips the case */
        SKIP
    }

    /** The lanes, in the ledger's column order. */
    public static final List<String> LANES = List.of("duckdb", "postgres");

    /** A case's status in a lane, and what it rests on (the differences, the error). */
    public record Outcome(Status status, String detail) {
    }

    /** Runs an island ({@code #SQL{...}#}) through legend-lite. */
    @FunctionalInterface
    public interface Legend {
        ExecutionResult run(String island) throws Exception;

        /** Begins a transaction a writing case runs in ({@link #rollback} ends it). */
        default void begin() throws Exception {
        }

        default void rollback() throws Exception {
        }
    }

    /** One case in one lane, its answers in {@code answers}: a query's as {@link #outcome(Case, Answer, Set, Legend)}
     *  judges it; a writing case's statement run in a transaction rolled back after, its count and its check query's
     *  rows each judged against Postgres's. */
    public static Outcome outcome(Case c, Map<String, Answer> answers, Set<String> tables, Legend legend)
            throws Exception {
        if (c.check() == null || c.skip() != null) {
            return outcome(c, answers.get(key(c)), tables, legend);
        }
        Answer expected = answers.get(key(c));
        Answer checked = answers.get(key(c) + CHECK);
        if (expected == null || expected instanceof Rows && checked == null) {
            throw new IllegalStateException("no snapped Postgres answer for " + key(c) + " and its check");
        }
        legend.begin();
        try {
            Outcome written = outcome(c, expected, tables, legend);
            if (written.status() != Status.PASS) {
                return written;
            }
            // (Postgres wrote the rows: the table after it, read in the same transaction)
            Outcome read = outcome(new Case(c.file(), c.id() + CHECK, c.check(), null), checked, tables, legend);
            return read.status() == Status.PASS ? written : new Outcome(read.status() == Status.FAIL ? Status.FAIL
                    : Status.ERROR, "the table after it: " + read.detail());
        } finally {
            legend.rollback();
        }
    }

    /** One case in one lane: its island run through legend-lite, judged against Postgres's answer by upstream's
     *  rules. */
    public static Outcome outcome(Case c, @com.legend.base.Nullable Answer expected, Set<String> tables,
            Legend legend) {
        if (c.skip() != null) {
            return new Outcome(Status.SKIP, c.skip());
        }
        if (expected == null) {
            throw new IllegalStateException("no snapped Postgres answer for " + key(c));
        }
        Grid actual;
        try {
            actual = grid(legend.run("#SQL{" + rewrite(c.sql(), tables) + "}#"));
        } catch (com.legend.error.NotImplementedException e) {
            return expected instanceof Rejected ? new Outcome(Status.BOTH_ERROR, oneLine(e))
                    : new Outcome(Status.REFUSED, oneLine(e));
        } catch (Exception | StackOverflowError e) {
            return expected instanceof Rejected ? new Outcome(Status.BOTH_ERROR, oneLine(e))
                    : new Outcome(Status.ERROR, oneLine(e));
        }
        return switch (expected) {
            case Rejected r -> new Outcome(Status.BUG, "Postgres rejects it (" + r.sqlState() + ": " + r.message()
                    + ") and legend-lite answered " + actual.rows().size() + " rows");
            case Rows rows -> {
                List<String> diffs = compare(c, rows.grid(), actual);
                yield diffs.isEmpty() ? new Outcome(Status.PASS, "") : new Outcome(Status.FAIL, String.join("; ",
                        diffs));
            }
        };
    }

    private static String oneLine(Throwable e) {
        String m = e.getClass().getSimpleName() + ": " + e.getMessage();
        m = m.replace('\n', ' ').replace('\t', ' ');
        return m.length() > 300 ? m.substring(0, 300) + "..." : m;
    }

    /** Each case's outcome against the ledger's column for {@code lane}: the cases whose status changed, each said
     *  as upstream says it (REGRESSION, FIX DETECTED, STATUS MISMATCH). With {@code update}, the ledger is written
     *  with this lane's statuses instead and nothing is reported. A summary and a report of every non-passing case
     *  go to standard out and, under Bazel, to the test's undeclared outputs. */
    public static List<String> ratchet(List<Case> cases, Map<String, Outcome> outcomes, Path ledgerFile,
            String lane, @com.legend.base.Nullable Path update) throws IOException {
        return ratchet("upstream's Postgres parity corpus", "sql-parity", LEDGER_HEADER, cases, outcomes, ledgerFile,
                lane, update);
    }

    /** {@link #ratchet(List, Map, Path, String, Path)} for another corpus held by a ledger of the same shape (the
     *  {@code #IQ{...}#} island's, IqParity): its name in the summary, its report's file name, its ledger's header. */
    public static List<String> ratchet(String corpus, String reportName, String ledgerHeader, List<Case> cases,
            Map<String, Outcome> outcomes, Path ledgerFile, String lane, @com.legend.base.Nullable Path update)
            throws IOException {
        return ratchet(corpus, reportName, ledgerHeader, cases, outcomes, ledgerFile, LANES, lane, update);
    }

    /** {@link #ratchet(String, String, String, List, Map, Path, String, Path)} for a ledger of other lanes,
     *  {@code lanes} its columns in order (the Cypher corpus's native store has a ledger of its own). */
    public static List<String> ratchet(String corpus, String reportName, String ledgerHeader, List<Case> cases,
            Map<String, Outcome> outcomes, Path ledgerFile, List<String> lanes, String lane,
            @com.legend.base.Nullable Path update) throws IOException {
        int column = lanes.indexOf(lane);
        if (column < 0) {
            throw new IllegalStateException("the lane " + lane + " is no column of " + lanes);
        }
        Map<String, String[]> ledger = readLedger(ledgerFile, lanes);
        Map<Status, Integer> counts = new java.util.EnumMap<>(Status.class);
        StringBuilder report = new StringBuilder("case\tstatus\tdetail\n");
        List<String> problems = new ArrayList<>();
        for (Case c : cases) {
            Outcome o = outcomes.get(key(c));
            counts.merge(o.status(), 1, Integer::sum);
            if (o.status() != Status.PASS) {
                report.append(key(c)).append('\t').append(o.status()).append('\t').append(o.detail()).append('\n');
            }
            String[] pinnedLanes = ledger.get(key(c));
            String pinned = pinnedLanes == null ? null : pinnedLanes[column];
            String now = o.status().name();
            if (pinned == null || pinned.isEmpty()) {
                problems.add("NEW: " + key(c) + " is " + now + " and not in the ledger");
            } else if (!pinned.equals(now)) {
                String what = pinned.equals("PASS") ? "REGRESSION" : now.equals("PASS") ? "FIX DETECTED"
                        : "STATUS MISMATCH";
                problems.add(what + ": " + key(c) + " was " + pinned + ", now " + now + (o.detail().isEmpty() ? ""
                        : " (" + o.detail() + ")"));
            }
        }
        for (String pinnedKey : ledger.keySet()) {
            if (cases.stream().noneMatch(c -> key(c).equals(pinnedKey))) {
                problems.add("GONE: " + pinnedKey + " is in the ledger and not in the corpus");
            }
        }
        StringBuilder summary = new StringBuilder(corpus + ", " + lane + " lane: " + cases.size() + " cases");
        counts.forEach((s, n) -> summary.append(", ").append(s).append(' ').append(n));
        System.out.println(summary);
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, reportName + "-" + lane + ".tsv"), report.toString(),
                    StandardCharsets.UTF_8);
        }
        if (update != null) {
            // the other lane's column is the one in the file being written, kept as it is
            Map<String, String[]> written = Files.exists(update) ? readLedger(update, lanes) : ledger;
            for (Case c : cases) {
                String[] row = written.computeIfAbsent(key(c), k -> new String[lanes.size()]);
                row[column] = outcomes.get(key(c)).status().name();
            }
            writeLedger(update, ledgerHeader, cases, written, lanes);
            System.out.println("wrote " + update);
            return List.of();
        }
        return problems;
    }

    /** The names of the schema's tables, as the rewrite matches them. */
    public static Set<String> tableNames(List<Table> tables) {
        return tables.stream().map(t -> t.name().toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    /** The ledger: each case's status in each lane, {@code key<TAB>duckdb<TAB>postgres}, with a header line. */
    public static Map<String, String[]> readLedger(Path file) throws IOException {
        return readLedger(file, LANES);
    }

    /** {@link #readLedger(Path)} for a ledger whose columns are {@code lanes}. */
    public static Map<String, String[]> readLedger(Path file, List<String> lanes) throws IOException {
        Map<String, String[]> ledger = new LinkedHashMap<>();
        if (!Files.exists(file)) {
            return ledger;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#") || line.startsWith("case\t")) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            ledger.put(parts[0], java.util.Arrays.copyOfRange(parts, 1, 1 + lanes.size()));
        }
        return ledger;
    }

    /** The sql-parity ledger's header: what it is, and which tests hold it. */
    private static final String LEDGER_HEADER = "# upstream's Postgres parity corpus (legend-engine 4.145.0), each case's"
            + " status per lane: the ratchet SqlParityTest and\n# SqlParityPostgresTest hold (core/src/test/java/"
            + "com/legend/testcases/SqlParity.java says each status)\n";

    public static void writeLedger(Path file, List<Case> cases, Map<String, String[]> ledger) throws IOException {
        writeLedger(file, LEDGER_HEADER, cases, ledger);
    }

    /** The ledger, its comment lines {@code header} (each line starting with {@code #}). */
    public static void writeLedger(Path file, String header, List<Case> cases, Map<String, String[]> ledger)
            throws IOException {
        writeLedger(file, header, cases, ledger, LANES);
    }

    /** {@link #writeLedger(Path, String, List, Map)} for a ledger whose columns are {@code lanes}. */
    public static void writeLedger(Path file, String header, List<Case> cases, Map<String, String[]> ledger,
            List<String> lanes) throws IOException {
        StringBuilder out = new StringBuilder(header + "case\t" + String.join("\t", lanes) + "\n");
        for (Case c : cases) {
            String[] row = ledger.get(key(c));
            out.append(key(c));
            for (int i = 0; i < lanes.size(); i++) {
                out.append('\t').append(row == null || row[i] == null ? "" : row[i]);
            }
            out.append('\n');
        }
        Files.writeString(file, out.toString(), StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ the YAML the corpus is written in

    /** The YAML the corpus uses, read by the YAML rules it relies on: block maps and lists, plain, single- and
     *  double-quoted scalars (a plain scalar ends at a {@code " #"} comment, so {@code sql: SELECT j #> '{a}'} is
     *  {@code SELECT j}, as upstream's YAML reader has it too), flow lists and maps in the schema, and YAML 1.1
     *  scalar types as Jackson's YAML reader resolves them (an integer as an Integer or a Long, a decimal as a
     *  Double). Anything else is an error, never a guess. */
    static final class Yaml {

        private static final Pattern CASE_START = Pattern.compile("^(\\s*)- ([a-z_]+):\\s?(.*)$");
        private static final Pattern KEY = Pattern.compile("^(\\s*)([a-z_]+):\\s?(.*)$");

        private Yaml() {
        }

        /** The {@code tests:} list of a case file, each case a map of its keys. */
        static List<Map<String, Object>> tests(String text, String file) {
            List<Map<String, Object>> tests = new ArrayList<>();
            Map<String, Object> current = null;
            int lineNo = 0;
            for (String raw : text.split("\n", -1)) {
                lineNo++;
                String line = raw.replace("\r", "");
                if (line.isBlank() || line.stripLeading().startsWith("#")) {
                    continue;
                }
                if (line.equals("tests:") || line.equals("tests: []")) {
                    continue;
                }
                Matcher start = CASE_START.matcher(line);
                if (start.matches()) {
                    current = new LinkedHashMap<>();
                    tests.add(current);
                    current.put(start.group(2), scalar(start.group(3), file, lineNo));
                    continue;
                }
                Matcher key = KEY.matcher(line);
                if (key.matches() && current != null && !key.group(1).isEmpty()) {
                    current.put(key.group(2), scalar(key.group(3), file, lineNo));
                    continue;
                }
                throw new IllegalStateException(file + ":" + lineNo + ": not the YAML this reader reads: " + line);
            }
            return tests;
        }

        /** The tables of schema.yaml. */
        static List<Table> schema(String text) {
            List<Table> tables = new ArrayList<>();
            String name = null;
            List<Column> columns = null;
            List<List<Object>> rows = null;
            String section = "";
            int lineNo = 0;
            for (String raw : text.split("\n", -1)) {
                lineNo++;
                String line = raw.replace("\r", "").stripTrailing();
                String s = line.strip();
                if (s.isEmpty() || s.startsWith("#") || s.equals("schema:") || s.equals("tables:")) {
                    continue;
                }
                if (s.startsWith("- name:")) {
                    if (name != null) {
                        tables.add(new Table(name, columns, rows));
                    }
                    name = (String) scalar(s.substring("- name:".length()).strip(), "schema.yaml", lineNo);
                    columns = new ArrayList<>();
                    rows = new ArrayList<>();
                    section = "";
                } else if (s.equals("columns:") || s.equals("rows:")) {
                    section = s;
                } else if (s.equals("rows: []")) {
                    section = "";
                } else if (s.startsWith("- {") && section.equals("columns:") && columns != null) {
                    Map<String, Object> col = flowMap(s.substring(2), lineNo);
                    columns.add(new Column((String) col.get("name"), (String) col.get("type")));
                } else if (s.startsWith("- [") && section.equals("rows:") && rows != null) {
                    rows.add(flowList(s.substring(2), lineNo));
                } else {
                    throw new IllegalStateException("schema.yaml:" + lineNo + ": not the YAML this reader reads: "
                            + line);
                }
            }
            if (name != null) {
                tables.add(new Table(name, columns, rows));
            }
            return tables;
        }

        private static Map<String, Object> flowMap(String s, int lineNo) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (String item : flowItems(s, '{', '}', lineNo)) {
                int colon = item.indexOf(':');
                m.put(item.substring(0, colon).strip(), scalar(item.substring(colon + 1).strip(), "schema.yaml",
                        lineNo));
            }
            return m;
        }

        private static List<Object> flowList(String s, int lineNo) {
            List<Object> items = new ArrayList<>();
            for (String item : flowItems(s, '[', ']', lineNo)) {
                items.add(scalar(item.strip(), "schema.yaml", lineNo));
            }
            return items;
        }

        /** The comma-separated items of a one-line flow collection, quoted items kept whole. */
        private static List<String> flowItems(String s, char open, char close, int lineNo) {
            if (s.charAt(0) != open || s.charAt(s.length() - 1) != close) {
                throw new IllegalStateException("schema.yaml:" + lineNo + ": not a one-line flow collection: " + s);
            }
            List<String> items = new ArrayList<>();
            StringBuilder item = new StringBuilder();
            char quote = 0;
            for (int i = 1; i < s.length() - 1; i++) {
                char c = s.charAt(i);
                if (quote != 0) {
                    item.append(c);
                    if (c == quote) {
                        if (quote == '\'' && i + 1 < s.length() - 1 && s.charAt(i + 1) == '\'') {
                            item.append(s.charAt(++i));
                        } else if (quote == '"' && s.charAt(i - 1) == '\\') {
                            continue;
                        } else {
                            quote = 0;
                        }
                    }
                } else if (c == '"' || c == '\'') {
                    quote = c;
                    item.append(c);
                } else if (c == ',') {
                    items.add(item.toString());
                    item.setLength(0);
                } else {
                    item.append(c);
                }
            }
            if (!item.toString().isBlank()) {
                items.add(item.toString());
            }
            return items;
        }

        /** A scalar: quoted ones as their text, plain ones by YAML 1.1's types. */
        static @com.legend.base.Nullable Object scalar(String s, String file, int lineNo) {
            if (s.startsWith("\"")) {
                return doubleQuoted(s, file, lineNo);
            }
            if (s.startsWith("'")) {
                int end = s.lastIndexOf('\'');
                if (end <= 0 || !s.substring(end + 1).isBlank() && !s.substring(end + 1).strip().startsWith("#")) {
                    throw new IllegalStateException(file + ":" + lineNo + ": an unterminated quoted scalar: " + s);
                }
                return s.substring(1, end).replace("''", "'");
            }
            String plain = s;
            int comment = plain.indexOf(" #");
            if (comment >= 0) {
                plain = plain.substring(0, comment);
            }
            plain = plain.strip();
            if (plain.isEmpty() || plain.equals("null") || plain.equals("~")) {
                return null;
            }
            if (plain.equals("true") || plain.equals("false")) {
                return Boolean.valueOf(plain);
            }
            if (plain.matches("[-+]?[0-9]+")) {
                BigInteger n = new BigInteger(plain.startsWith("+") ? plain.substring(1) : plain);
                if (n.bitLength() < 32) {
                    return n.intValue();
                }
                return n.bitLength() < 64 ? (Object) n.longValue() : n;
            }
            if (plain.matches("[-+]?([0-9]+\\.[0-9]*|\\.[0-9]+)([eE][-+]?[0-9]+)?")) {
                return Double.valueOf(plain);
            }
            return plain;
        }

        private static String doubleQuoted(String s, String file, int lineNo) {
            StringBuilder out = new StringBuilder();
            int i = 1;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c == '"') {
                    String rest = s.substring(i + 1).strip();
                    if (!rest.isEmpty() && !rest.startsWith("#")) {
                        throw new IllegalStateException(file + ":" + lineNo + ": text after a quoted scalar: " + s);
                    }
                    return out.toString();
                }
                if (c == '\\') {
                    char e = s.charAt(i + 1);
                    switch (e) {
                        case '\\', '"', '/' -> out.append(e);
                        case 'n' -> out.append('\n');
                        case 't' -> out.append('\t');
                        case 'r' -> out.append('\r');
                        case '0' -> out.append('\0');
                        case 'u' -> {
                            out.append((char) Integer.parseInt(s.substring(i + 2, i + 6), 16));
                            i += 4;
                        }
                        default -> throw new IllegalStateException(file + ":" + lineNo + ": an escape this reader"
                                + " does not know: \\" + e);
                    }
                    i += 2;
                    continue;
                }
                out.append(c);
                i++;
            }
            throw new IllegalStateException(file + ":" + lineNo + ": an unterminated quoted scalar: " + s);
        }
    }
}
