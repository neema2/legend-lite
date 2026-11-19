package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** SCRATCH (not to commit): which SqlFn each dialect writes with a nested query. */
class NestedQueryCensusScratchTest {

    static final SqlSource.Table T = new SqlSource.Table("T", "t0", List.of(), false, Map.of(),
            com.legend.sql.NameQuoting.BARE);

    static SqlExpr col(String n, SqlType t) {
        return SqlExpr.Column.of("t0", n, t, true, OutputCol.Origin.PHYSICAL);
    }

    static boolean nested(String sql) {
        String u = sql.toUpperCase(java.util.Locale.ROOT);
        int selects = u.split("SELECT", -1).length - 1;
        return selects > 1 || u.contains("WITH ") || u.contains("JSON_EACH") || u.contains("UNNEST");
    }

    @Test
    void shapesSql() {
        StringBuilder out = new StringBuilder();
        for (var t : List.of(com.legend.model.ConnectionDefinition.DatabaseType.SQLite,
                com.legend.model.ConnectionDefinition.DatabaseType.Postgres,
                com.legend.model.ConnectionDefinition.DatabaseType.DuckDB)) {
            for (String q : List.of(com.legend.integration.WindowReadThroughAListProbe.SPLIT_SORTED,
                    com.legend.integration.WindowReadThroughAListProbe.CHUNKED,
                    com.legend.integration.WindowReadThroughAListProbe.GROUPED)) {
                String model;
                try {
                    model = com.legend.testbackend.StorelessRuntime.with("", t);
                } catch (RuntimeException notStoreless) {
                    model = com.legend.testbackend.StorelessRuntime.onServer("", t, "localhost", 1, "legend");
                }
                String sql;
                try {
                    sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), q)
                            .plan(com.legend.testbackend.StorelessRuntime.RUNTIME).sql();
                } catch (RuntimeException e) {
                    sql = "REFUSED " + e;
                }
                out.append("== ").append(t).append("\n").append(sql).append("\n");
            }
        }
        System.out.println("SHAPES-BEGIN\n" + out + "SHAPES-END");
    }

    @Test
    void census() {
        List<SqlType> types = List.of(SqlType.Scalar.VARCHAR, SqlType.Scalar.BIGINT, SqlType.Scalar.DOUBLE,
                SqlType.Scalar.BOOLEAN, SqlType.Scalar.DATE, SqlType.Scalar.TIMESTAMP, SqlType.Scalar.JSON,
                new SqlType.Array(SqlType.Scalar.VARCHAR), new SqlType.Array(SqlType.Scalar.BIGINT));
        Map<String, AnsiSqlRenderer> dialects = new LinkedHashMap<>();
        dialects.put("SQLite", new SQLite());
        dialects.put("Postgres", new Postgres());
        dialects.put("DuckDb", new DuckDb());
        dialects.put("H2", new H2());
        dialects.put("ClickHouse", new ClickHouse());
        dialects.put("Snowflake", new Snowflake());
        StringBuilder out = new StringBuilder();
        for (var d : dialects.entrySet()) {
            List<String> nestedFns = new ArrayList<>();
            List<String> unrendered = new ArrayList<>();
            List<String> inPlace = new ArrayList<>();
            for (SqlFn fn : SqlFn.values()) {
                boolean any = false;
                boolean nest = false;
                String example = null;
                for (int arity = 0; arity <= 4; arity++) {
                    for (SqlType t : types) {
                        for (int mode = 0; mode < 3; mode++) {
                            List<SqlExpr> args = new ArrayList<>();
                            for (int i = 0; i < arity; i++) {
                                if (i == 0 || mode == 0) {
                                    args.add(col("a" + i, t));
                                } else if (mode == 1) {
                                    args.add(new SqlExpr.StringLit("x"));
                                } else {
                                    args.add(new SqlExpr.IntLit(1));
                                }
                            }
                            try {
                                SqlSelect s = SqlSelect.starOf(T).withProjections(List.of(
                                        new SqlSelect.Projection(new SqlExpr.Call(fn, args), "x", null)));
                                String sql = d.getValue().render(s);
                                any = true;
                                if (nested(sql)) {
                                    nest = true;
                                    if (example == null) {
                                        example = sql;
                                    }
                                }
                            } catch (RuntimeException | StackOverflowError e) {
                                // not this arity or type
                            }
                        }
                    }
                }
                if (nest) {
                    nestedFns.add(fn.name());
                } else if (any) {
                    inPlace.add(fn.name());
                } else {
                    unrendered.add(fn.name());
                }
            }
            out.append("== ").append(d.getKey()).append("\nNESTED ").append(nestedFns.size()).append(": ")
                    .append(String.join(" ", nestedFns)).append("\nUNRENDERED ").append(unrendered.size())
                    .append(": ").append(String.join(" ", unrendered)).append("\nINPLACE ").append(inPlace.size())
                    .append(": ").append(String.join(" ", inPlace)).append("\n");
        }
        System.out.println("CENSUS-BEGIN\n" + out + "CENSUS-END");
    }
}
