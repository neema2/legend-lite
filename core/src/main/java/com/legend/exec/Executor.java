package com.legend.exec;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.ExprType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlQuery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes rendered SQL and shapes the rows per the ROOT's classification.
 * Cell values are raw JDBC objects; column Pure types come from the query's
 * typed outputs (never from JDBC metadata — the no-sniffing contract).
 */
public final class Executor {

    private Executor() {
    }

    /**
     * Raw-statement execution — the K-native {@code executeInDb} boundary:
     * one already-dialect-adapted statement, no plan, no result shaping.
     */
    public static void executeRaw(Connection connection, String statement)
            throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute(statement);
        }
    }

    public static ExecutionResult execute(String sql, SqlQuery plan, ExprType rootType,
                                          Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect)
            throws SQLException {
        return execute(sql, plan, rootType, ResultShape.of(rootType), connection, dialect);
    }

    /**
     * Shape-explicit entry: the driver decides the shape from the RESOLVED
     * root NODE (a class-typed root is GRAPH only under the resolver's
     * serialize envelope; bare it is an instance VALUE — the type alone
     * cannot tell them apart).
     */
    public static ExecutionResult execute(String sql, SqlQuery plan, ExprType rootType,
                                          ResultShape shape, Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect)
            throws SQLException {
        boolean anyRoot = PlatformTypes.isAny(rootType.type());
        // opt-in diagnostic: every executed statement to stderr
        if (System.getenv("LEGEND_LITE_DUMP_SQL") != null) {
            System.err.println("[sql] " + sql);
        }
        // prepareStatement, not createStatement: DuckDB JDBC 1.5 masks a
        // direct Statement's real error behind 'Attempting to execute an
        // unsuccessful or closed pending query result' (audit: 74 corpus
        // errors were unreadable); prepare() surfaces the actual message
        try (java.sql.PreparedStatement st = connection.prepareStatement(sql);
             ResultSet rs = st.executeQuery()) {
            return switch (shape) {
                case TABULAR -> tabular(rs, plan, rootType, dialect);
                case SCALAR -> new ExecutionResult.Scalar(
                        rs.next() ? latticeKind(cell(rs, plan, dialect, anyRoot),
                                rootType.type(), plan) : null,
                        rootType.type());
                case COLLECTION -> {
                    List<Object> values = new ArrayList<>();
                    while (rs.next()) {
                        Object v = latticeKind(cell(rs, plan, dialect, anyRoot),
                                rootType.type(), plan);
                        // a NULL cell is a pure EMPTY, and no pure collection
                        // holds empties — Person.all().middleName over a row
                        // with no middle name contributes nothing, not null
                        if (v != null) {
                            values.add(v);
                        }
                    }
                    yield new ExecutionResult.Collection(values, rootType.type());
                }
                case GRAPH -> new ExecutionResult.Graph(
                        rs.next() ? String.valueOf(rs.getObject(1)) : "[]", rootType.type());
            };
        }
    }

    private static Object cell(ResultSet rs, SqlQuery plan,
                               com.legend.sql.dialect.SqlDialect dialect, boolean anyRoot)
            throws SQLException {
        Object v = unwrap(fetch(rs, 1, sqlTypeOf(plan, 0)), sqlTypeOf(plan, 0), dialect);
        return anyRoot ? decodeAny(v) : v;
    }

    /**
     * LATTICE-typed roots recover their values' own kinds FROM SELF-
     * DESCRIBING WIRE ENCODINGS ONLY — the identity channel's print forms
     * (computed by the database) and the TIMESTAMP-carried midnight
     * StrictDate. Value-consulting heuristics (integral-double narrowing,
     * scale-0 decimal narrowing) were audited out: the kind must travel
     * FROM SQL, never be guessed after it.
     */
    private static Object latticeKind(Object v, Type rootType, SqlQuery plan) {
        // The MIXED-ELEMENT IDENTITY channel: selections over mixed-kind
        // Number collections return each element's pure PRINT FORM as text
        // ('2', '2.0', '7.345D') — parsed back to its own kind here. (DATE
        // identities stay strings — the wire's date convention.)
        if (rootType == Type.Primitive.NUMBER && v instanceof String s) {
            if (s.endsWith("D")) {
                return new java.math.BigDecimal(s.substring(0, s.length() - 1));
            }
            if (s.contains(".") || s.contains("e") || s.contains("E")) {
                return Double.valueOf(s);
            }
            return Long.valueOf(s);
        }
        if (rootType == Type.Primitive.DATE && v instanceof java.sql.Timestamp t
                && t.toLocalDateTime().toLocalTime()
                        .equals(java.time.LocalTime.MIDNIGHT)) {
            return t.toLocalDateTime().toLocalDate();
        }
        // the BC carrier (fetch keeps LocalDateTime where Timestamp is
        // unfaithful) gets the SAME midnight narrowing — carrier choice
        // must not change the recovered kind
        if (rootType == Type.Primitive.DATE && v instanceof java.time.LocalDateTime ldt
                && ldt.toLocalTime().equals(java.time.LocalTime.MIDNIGHT)) {
            return ldt.toLocalDate();
        }
        return v;
    }

    /**
     * An ANY-typed value travels as variant JSON (the heterogeneous-list
     * carrier); at the boundary each element decodes back to its own runtime
     * kind — a number is a Number again, not the string {@code "1"}. Variant
     * results are NOT decoded (their contract is the JSON text itself); only
     * the Any root takes this path.
     */
    private static Object decodeAny(Object v) {
        // Drivers hand JSON cells back as their own node type (DuckDB:
        // org.duckdb.JsonNode) or as text — matched by FULL class name so the
        // executor needs no driver import; the node's toString IS the JSON text.
        String s;
        if (v instanceof String str) {
            s = str;
        } else if (v != null && v.getClass().getName().equals("org.duckdb.JsonNode")) {
            s = v.toString();
        } else {
            return v;
        }
        String t = s.trim();
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            return jsonUnescape(t.substring(1, t.length() - 1));
        }
        if (t.equals("true") || t.equals("false")) {
            return Boolean.valueOf(t);
        }
        if (t.equals("null")) {
            return null;
        }
        try {
            return Long.valueOf(t);
        } catch (NumberFormatException ignored) {
            // fall through
        }
        try {
            return Double.valueOf(t);
        } catch (NumberFormatException ignored) {
            return s;
        }
    }

    /**
     * JSON string-escape decoding — the variant carrier emits PROPER JSON, so
     * a value like {@code he said "hi"} arrives as {@code "he said \"hi\""};
     * a raw quote-strip would keep the backslashes (audit finding).
     */
    private static String jsonUnescape(String s) {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c != '\\' || i == s.length() - 1) {
                out.append(c);
                continue;
            }
            char e = s.charAt(++i);
            switch (e) {
                case '"' -> out.append('"');
                case '\\' -> out.append('\\');
                case '/' -> out.append('/');
                case 'n' -> out.append('\n');
                case 't' -> out.append('\t');
                case 'r' -> out.append('\r');
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'u' -> {
                    if (i + 4 < s.length()) {
                        out.append((char) Integer.parseInt(s.substring(i + 1, i + 5), 16));
                        i += 4;
                    }
                }
                default -> out.append('\\').append(e);
            }
        }
        return out.toString();
    }

    /**
     * A composite JDBC cell unwraps by its DECLARED layout: a struct cell
     * becomes an ordered field map (names from the plan's {@link SqlType.Struct}
     * — the model's canonical layout, positional values), an array cell a list;
     * leaves normalize through the dialect. Attribute-count drift from the
     * declared layout is a contract violation — loud, never zipped short.
     */
    /**
     * Typed cell retrieval. TIMESTAMP columns fetch through {@code java.time}:
     * the driver's {@code java.sql.Timestamp} construction DROPS the BC era
     * (year -21457 surfaces as +21458 — irrecoverably, the epoch itself is
     * wrong). Timestamp stays the carrier where it is faithful (AD years);
     * a BC value keeps its LocalDateTime.
     */
    private static Object fetch(ResultSet rs, int i, com.legend.sql.SqlType type)
            throws SQLException {
        Object o = rs.getObject(i);
        if (o instanceof java.sql.Timestamp) {
            // (a TIMESTAMP-typed output may still surface a VARCHAR cell —
            // the precision-faithful string convention — so gate on the
            // actual driver object, not the declared type)
            java.time.LocalDateTime ldt = rs.getObject(i, java.time.LocalDateTime.class);
            if (ldt != null && ldt.getYear() < 1) {
                return ldt;
            }
        }
        return o;
    }

    private static Object unwrap(Object v, com.legend.sql.SqlType type,
                                 com.legend.sql.dialect.SqlDialect dialect) throws SQLException {
        if (v == null) {
            return null;
        }
        if (type instanceof com.legend.sql.SqlType.Struct st && v instanceof java.sql.Struct s) {
            Object[] attrs = s.getAttributes();
            if (attrs.length != st.fields().size()) {
                throw new IllegalStateException("struct cell has " + attrs.length
                        + " attribute(s) but the declared layout has " + st.fields().size());
            }
            java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
            for (int i = 0; i < attrs.length; i++) {
                m.put(st.fields().get(i).name(),
                        unwrap(attrs[i], st.fields().get(i).type(), dialect));
            }
            return m;
        }
        if (type instanceof com.legend.sql.SqlType.Array at && v instanceof java.sql.Array a) {
            Object[] elements = (Object[]) a.getArray();
            List<Object> out = new ArrayList<>(elements.length);
            for (Object e : elements) {
                out.add(unwrap(e, at.element(), dialect));
            }
            return out;
        }
        return dialect.normalize(v, type);
    }

    /**
     * PURE column types come from the TYPED HIR ROOT's schema (the frontend's
     * truth); SQL types for driver normalization come from the plan's
     * outputs. The two type systems meet only here, each on its own side.
     */
    private static ExecutionResult.Tabular tabular(ResultSet rs, SqlQuery plan, ExprType rootType,
                                                    com.legend.sql.dialect.SqlDialect dialect)
            throws SQLException {
        if (!(rootType.type() instanceof Type.RelationType typedSchema)) {
            throw new IllegalStateException("TABULAR result without a relation root type: "
                    + rootType.type().typeName());
        }
        // A ROW-STRUCT column (a user navigate's slot) is typed nesting over
        // a FLAT physical reality — expand to the prefixed columns the join
        // emitted (alias_COL), mirroring the lowerer's output flattening.
        final Type.RelationType schema = flattenStructColumns(typedSchema);
        int n = rs.getMetaData().getColumnCount();
        List<Column> columns = new ArrayList<>();
        if (n == schema.columns().size()) {
            // POSITIONAL on both sides (schemas are ordered); no null types.
            for (int i = 1; i <= n; i++) {
                Type.Column sc = schema.columns().get(i - 1);
                columns.add(new Column(sc.name(),
                        rs.getMetaData().getColumnTypeName(i), sc.type()));
            }
        } else if (hasPivot(plan)) {
            // DYNAMIC PIVOT: one result column per pivoted VALUE — the static
            // schema cannot enumerate them (the checker keeps only the
            // group-by half). Statically known names match by NAME; a
            // pivot-generated '<value>__|__<agg>' column inherits its
            // aggregate TEMPLATE's type (schema.dynamicColumns(), the
            // engine-lite DynamicPivotColumn design) — the name is
            // data-dependent, the type is not. SQL-type derivation remains
            // only for schemas rebuilt downstream of the pivot, where the
            // templates no longer ride.
            for (int i = 1; i <= n; i++) {
                String name = rs.getMetaData().getColumnName(i);
                String sqlType = rs.getMetaData().getColumnTypeName(i);
                columns.add(new Column(name, sqlType, pivotColumnType(schema, name, sqlType)));
            }
        } else {
            throw new IllegalStateException("result has " + n + " columns but the typed"
                    + " schema has " + schema.columns().size() + " — plan/schema mismatch");
        }
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> cells = new ArrayList<>(n);
            for (int i = 1; i <= n; i++) {
                cells.add(unwrap(fetch(rs, i, sqlTypeOf(plan, i - 1)), sqlTypeOf(plan, i - 1), dialect));
            }
            rows.add(new Row(cells));
        }
        return new ExecutionResult.Tabular(columns, rows, rootType.type());
    }

    private static com.legend.sql.SqlType sqlTypeOf(SqlQuery plan, int index) {
        List<OutputCol> outputs = plan.outputs();
        if (index >= outputs.size()) {
            if (hasPivot(plan)) {
                return null; // dynamic pivot column: no static SQL type exists
            }
            throw new IllegalStateException("result column " + index
                    + " has no plan output — plan/result mismatch");
        }
        return outputs.get(index).type();
    }

    /** Whether the plan's source tree contains a (dynamic-columned) PIVOT. */
    private static boolean hasPivot(SqlQuery plan) {
        return plan instanceof com.legend.sql.SqlSelect s && hasPivot(s.from());
    }

    private static boolean hasPivot(com.legend.sql.SqlSource src) {
        return switch (src) {
            case null -> false;
            case com.legend.sql.SqlSource.Pivot p -> true;
            case com.legend.sql.SqlSource.Subselect sub -> hasPivot(sub.inner());
            case com.legend.sql.SqlSource.Join j -> hasPivot(j.left()) || hasPivot(j.right());
            default -> false;
        };
    }
    /**
     * The Pure type of one column of a pivot result. Static (group-by) names
     * match the schema; a dynamic {@code <value>__|__<agg>} name inherits its
     * aggregate template's type. A suffixed name that matches NO template while
     * templates are present is a naming-contract bug — loud, never guessed.
     */
    private static Type pivotColumnType(Type.RelationType schema, String name, String sqlType) {
        var byName = schema.columns().stream()
                .filter(c -> c.name().equals(name)).findFirst();
        if (byName.isPresent()) {
            return byName.get().type();
        }
        int sep = name.lastIndexOf(Type.RelationType.PIVOT_SEPARATOR);
        if (sep >= 0 && !schema.dynamicColumns().isEmpty()) {
            String template = name.substring(sep + Type.RelationType.PIVOT_SEPARATOR.length());
            return schema.dynamicColumns().stream()
                    .filter(c -> c.name().equals(template)).findFirst()
                    .map(Type.Column::type)
                    .orElseThrow(() -> new IllegalStateException("pivot column '" + name
                            + "' matches no aggregate template " + schema.dynamicColumns().stream()
                                    .map(Type.Column::name).toList()));
        }
        return pureOfSqlType(sqlType);
    }

    /** The Pure primitive a DYNAMIC (pivot-generated) SQL column carries.
     * Every known name is EXPLICIT — an unrecognized SQL type is a gap in
     * this table, not a String (audit 15: the silent String default
     * corrupted result typing invisibly). */
    private static Type pureOfSqlType(String sqlType) {
        String t = sqlType.toUpperCase();
        return switch (t) {
            case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT" ->
                    Type.Primitive.INTEGER;
            case "FLOAT", "DOUBLE", "REAL" -> Type.Primitive.FLOAT;
            case "BOOLEAN" -> Type.Primitive.BOOLEAN;
            case "DATE" -> Type.Primitive.STRICT_DATE;
            case "TIMESTAMP" -> Type.Primitive.DATE_TIME;
            case "VARCHAR", "CHAR", "TEXT", "STRING", "BPCHAR" ->
                    Type.Primitive.STRING;
            default -> {
                if (t.startsWith("DECIMAL")) {
                    yield Type.Primitive.DECIMAL;
                }
                if (t.startsWith("VARCHAR") || t.startsWith("CHAR")) {
                    yield Type.Primitive.STRING;
                }
                throw new IllegalStateException(
                        "no Pure primitive mapped for SQL type '" + sqlType
                        + "' (pivot-generated column) — add it to"
                        + " Executor.pureOfSqlType");
            }
        };
    }

    /** Expand row-struct columns (navigate slots) to their prefixed flat set. */
    private static Type.RelationType flattenStructColumns(Type.RelationType schema) {
        if (schema.columns().stream().noneMatch(c -> c.type() instanceof Type.RelationType)) {
            return schema;
        }
        List<Type.Column> flat = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            if (c.type() instanceof Type.RelationType sub) {
                for (Type.Column sc : sub.columns()) {
                    flat.add(new Type.Column(c.name() + "_" + sc.name(),
                            sc.type(), sc.multiplicity()));
                }
            } else {
                flat.add(c);
            }
        }
        return new Type.RelationType(flat);
    }

}
