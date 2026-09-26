package com.legend.warehouse.client.jdbc;

import com.legend.warehouse.sqlapi.DuckType;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.LinkedHashMap;
import com.legend.base.Nullable;
import java.util.List;
import java.util.Map;

/** A struct value, as DuckDB's driver hands one back: its attributes, and {@code {a=1, b=x}}. */
final class WhStruct implements Struct {

    private final DuckType.StructOf type;
    private final List<@Nullable Object> attrs;

    WhStruct(DuckType.StructOf type, List<@Nullable Object> attrs) {
        this.type = type;
        this.attrs = attrs;
    }

    @Override
    public String getSQLTypeName() {
        return type.name();
    }

    @Override
    public Object[] getAttributes() {
        return attrs.toArray();
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        throw Unsupported.of("Struct.getAttributes(Map)");
    }

    @Override
    public String toString() {
        Map<String, @Nullable Object> m = new LinkedHashMap<>();
        for (int i = 0; i < attrs.size(); i++) m.put(type.fields().get(i).name(), attrs.get(i));
        return m.toString();
    }
}
