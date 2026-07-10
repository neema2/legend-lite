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
            // schema cannot enumerate them (engine's DynamicPivotColumn
            // concept). Statically-known names match by NAME; every other
            // column is a pivot-generated value column and carries the
            // schema's single aggregate type (multi-agg pivots suffix-match).
            Type dynamicType = schema.columns().get(schema.columns().size() - 1).type();
            for (int i = 1; i <= n; i++) {
                String name = rs.getMetaData().getColumnName(i);
                Type pure = schema.columns().stream()
                        .filter(c -> c.name().equals(name)).findFirst()
                        .map(Type.Column::type)
                        .orElseGet(() -> schema.columns().stream()
                                .filter(c -> name.endsWith("_" + c.name()))
                                .findFirst().map(Type.Column::type)
                                .orElse(dynamicType));
                columns.add(new Column(name, rs.getMetaData().getColumnTypeName(i), pure));
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
