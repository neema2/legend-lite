package com.legend.exec;

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
                                          Connection connection) throws SQLException {
        ResultShape shape = ResultShape.of(rootType);
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return switch (shape) {
                case TABULAR -> tabular(rs, plan, rootType);
                case SCALAR -> new ExecutionResult.Scalar(
                        rs.next() ? rs.getObject(1) : null, rootType.type());
                case COLLECTION -> {
                    List<Object> values = new ArrayList<>();
                    while (rs.next()) {
                        values.add(rs.getObject(1));
                    }
                    yield new ExecutionResult.Collection(values, rootType.type());
                }
                case GRAPH -> new ExecutionResult.Graph(
                        rs.next() ? String.valueOf(rs.getObject(1)) : "[]", rootType.type());
            };
        }
    }

    /** Columns from the plan's TYPED outputs; JDBC supplies only the informational sqlType. */
    private static ExecutionResult.Tabular tabular(ResultSet rs, SqlQuery plan, ExprType rootType)
            throws SQLException {
        List<OutputCol> outputs = plan.outputs();
        List<Column> columns = new ArrayList<>();
        int n = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= n; i++) {
            String name = rs.getMetaData().getColumnName(i);
            String pure = outputs.stream().filter(o -> o.name().equals(name)).findFirst()
                    .map(o -> o.type().typeName()).orElse(null);
            columns.add(new Column(name, rs.getMetaData().getColumnTypeName(i), pure));
        }
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> cells = new ArrayList<>(n);
            for (int i = 1; i <= n; i++) {
                cells.add(rs.getObject(i));
            }
            rows.add(new Row(cells));
        }
        return new ExecutionResult.Tabular(columns, rows, rootType.type());
    }
}
