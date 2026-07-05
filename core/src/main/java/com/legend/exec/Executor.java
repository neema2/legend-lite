package com.legend.exec;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.ExprType;
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
        Type.RelationType schema = rootType.type() instanceof Type.RelationType rt ? rt : null;
        List<Column> columns = new ArrayList<>();
        int n = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= n; i++) {
            String name = rs.getMetaData().getColumnName(i);
            Type pure = schema == null ? null : schema.columns().stream()
                    .filter(c -> c.name().equals(name)).findFirst()
                    .map(Type.Column::type).orElse(null);
            columns.add(new Column(name, rs.getMetaData().getColumnTypeName(i), pure));
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
        return index < outputs.size() ? outputs.get(index).type() : null;
    }
}
