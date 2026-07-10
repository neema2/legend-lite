package com.legend.exec;

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

    public static ExecutionResult execute(String sql, SqlQuery plan, ExprType rootType,
                                          Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect)
            throws SQLException {
        ResultShape shape = ResultShape.of(rootType);
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return switch (shape) {
                case TABULAR -> tabular(rs, plan, rootType, dialect);
                case SCALAR -> new ExecutionResult.Scalar(
                        rs.next() ? dialect.normalize(rs.getObject(1), sqlTypeOf(plan, 0)) : null,
                        rootType.type());
                case COLLECTION -> {
                    List<Object> values = new ArrayList<>();
                    while (rs.next()) {
                        values.add(dialect.normalize(rs.getObject(1), sqlTypeOf(plan, 0)));
                    }
                    yield new ExecutionResult.Collection(values, rootType.type());
                }
                case GRAPH -> new ExecutionResult.Graph(
                        rs.next() ? String.valueOf(rs.getObject(1)) : "[]", rootType.type());
            };
        }
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
                cells.add(dialect.normalize(rs.getObject(i), sqlTypeOf(plan, i - 1)));
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

    /** The Pure primitive a DYNAMIC (pivot-generated) SQL column carries. */
    private static Type pureOfSqlType(String sqlType) {
        return switch (sqlType.toUpperCase()) {
            case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT" ->
                    Type.Primitive.INTEGER;
            case "FLOAT", "DOUBLE", "REAL" -> Type.Primitive.FLOAT;
            case "BOOLEAN" -> Type.Primitive.BOOLEAN;
            case "DATE" -> Type.Primitive.STRICT_DATE;
            case "TIMESTAMP" -> Type.Primitive.DATE_TIME;
            default -> sqlType.toUpperCase().startsWith("DECIMAL")
                    ? Type.Primitive.DECIMAL
                    : Type.Primitive.STRING;
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
