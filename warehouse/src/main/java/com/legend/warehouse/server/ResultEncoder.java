package com.legend.warehouse.server;

import com.legend.warehouse.server.duck.Result;
import com.legend.warehouse.sqlapi.DuckType;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import java.util.ArrayList;
import java.util.List;

/**
 * A result's columns as the API's (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §3):
 * each column's API type is DuckDB's own type name, unchanged, and must be one
 * the API carries. A type outside that list is refused with its name, never
 * guessed. (The values themselves are encoded in {@code server.duck}.)
 */
final class ResultEncoder {

    /** A type the API does not carry. */
    static final class Unsupported extends Exception {
        Unsupported(String column, String type) {
            super("column '" + column + "' has type " + type + ", which the API does not carry yet");
        }
    }

    private ResultEncoder() {
    }

    /** The columns as the API reports them, once every type is one it carries. */
    static List<Column> api(List<Result.Column> cols) throws Unsupported {
        List<Column> out = new ArrayList<>(cols.size());
        for (Result.Column c : cols) {
            DuckType t;
            try {
                t = DuckType.parse(c.typeName());
            } catch (IllegalArgumentException bad) {
                throw new Unsupported(c.name(), c.typeName());
            }
            if (!supported(t)) throw new Unsupported(c.name(), c.typeName());
            out.add(new Column(c.name(), c.typeName(), true));
        }
        return out;
    }

    private static boolean supported(DuckType t) {
        return switch (t) {
            case DuckType.Scalar s -> SCALARS.contains(s.base());
            case DuckType.ListOf l -> supported(l.element());
            case DuckType.StructOf st -> st.fields().stream().allMatch(f -> supported(f.type()));
            case DuckType.MapOf m -> supported(m.key()) && supported(m.value());
        };
    }

    private static final java.util.Set<String> SCALARS = java.util.Set.of(
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
            "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
            "FLOAT", "DOUBLE", "DECIMAL", "VARCHAR", "UUID", "INTERVAL", "ENUM", "JSON", "BLOB",
            "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
            "TIMESTAMP WITH TIME ZONE");
}
