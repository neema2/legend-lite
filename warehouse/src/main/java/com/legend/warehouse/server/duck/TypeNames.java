package com.legend.warehouse.server.duck;

import java.lang.foreign.MemorySegment;
import java.util.Locale;
import java.util.Set;

/**
 * A logical type's name, spelled as DuckDB's JDBC driver reports it
 * ({@code getColumnTypeName}): DuckDB's own type grammar, which the API
 * carries and {@code DuckType} parses. The C API has no function for it, so it
 * is read off the type through its accessors; a STRUCT field name is quoted by
 * DuckDB's rule (KeywordHelper::WriteOptionallyQuoted, capitals allowed), with
 * DuckDB's own keyword list ({@code duckdb_keywords()}).
 */
final class TypeNames {

    private TypeNames() {
    }

    /** The name; a type outside DuckDB's usual grammar (never one the API carries) is {@code TYPE<id>}. */
    static String of(Duck d, MemorySegment type, Set<String> keywords) {
        return of(d, type, keywords, true);
    }

    /**
     * {@code top}: a result column's own type. DuckDB's driver names a top-level ENUM just {@code ENUM},
     * and a nested one with its values, {@code ENUM('sad', 'ok')} (measured).
     */
    private static String of(Duck d, MemorySegment type, Set<String> keywords, boolean top) {
        try {
            int id = (int) d.typeId.invokeExact(type);
            return switch (id) {
                case Duck.BOOLEAN -> "BOOLEAN";
                case Duck.TINYINT -> "TINYINT";
                case Duck.SMALLINT -> "SMALLINT";
                case Duck.INTEGER -> "INTEGER";
                case Duck.BIGINT -> "BIGINT";
                case Duck.UTINYINT -> "UTINYINT";
                case Duck.USMALLINT -> "USMALLINT";
                case Duck.UINTEGER -> "UINTEGER";
                case Duck.UBIGINT -> "UBIGINT";
                case Duck.HUGEINT_T -> "HUGEINT";
                case Duck.UHUGEINT -> "UHUGEINT";
                case Duck.FLOAT -> "FLOAT";
                case Duck.DOUBLE -> "DOUBLE";
                case Duck.DECIMAL_T -> "DECIMAL(" + Byte.toUnsignedInt((byte) d.decimalWidth.invokeExact(type)) + ","
                        + Byte.toUnsignedInt((byte) d.decimalScale.invokeExact(type)) + ")";
                case Duck.VARCHAR -> alias(d, type).equals("JSON") ? "JSON" : "VARCHAR";
                case Duck.BLOB -> "BLOB";
                case Duck.UUID -> "UUID";
                case Duck.INTERVAL_T -> "INTERVAL";
                case Duck.ENUM -> top ? "ENUM" : enumValues(d, type);
                case Duck.DATE -> "DATE";
                case Duck.TIME -> "TIME";
                case Duck.TIMESTAMP -> "TIMESTAMP";
                case Duck.TIMESTAMP_S -> "TIMESTAMP_S";
                case Duck.TIMESTAMP_MS -> "TIMESTAMP_MS";
                case Duck.TIMESTAMP_NS -> "TIMESTAMP_NS";
                case Duck.TIMESTAMP_TZ -> "TIMESTAMP WITH TIME ZONE";
                case Duck.TIME_TZ -> "TIME WITH TIME ZONE";
                case Duck.BIT -> "BIT";
                case Duck.UNION -> "UNION";
                case Duck.LIST -> child(d, (MemorySegment) d.listChild.invokeExact(type), keywords) + "[]";
                case Duck.ARRAY -> child(d, (MemorySegment) d.arrayChild.invokeExact(type), keywords)
                        + "[" + (long) d.arraySize.invokeExact(type) + "]";
                case Duck.MAP -> "MAP(" + child(d, (MemorySegment) d.mapKey.invokeExact(type), keywords) + ", "
                        + child(d, (MemorySegment) d.mapValue.invokeExact(type), keywords) + ")";
                case Duck.STRUCT -> {
                    long n = (long) d.structCount.invokeExact(type);
                    StringBuilder sb = new StringBuilder("STRUCT(");
                    for (long i = 0; i < n; i++) {
                        if (i > 0) sb.append(", ");
                        sb.append(quoted(d.owned((MemorySegment) d.structName.invokeExact(type, i)), keywords))
                                .append(' ')
                                .append(child(d, (MemorySegment) d.structType.invokeExact(type, i), keywords));
                    }
                    yield sb.append(')').toString();
                }
                default -> "TYPE" + id;
            };
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /** A child type's name; the child handle is DuckDB's to destroy, here. */
    private static String child(Duck d, MemorySegment type, Set<String> keywords) throws Throwable {
        try {
            return of(d, type, keywords, false);
        } finally {
            try (java.lang.foreign.Arena a = java.lang.foreign.Arena.ofConfined()) {
                MemorySegment p = a.allocate(java.lang.foreign.ValueLayout.ADDRESS);
                p.set(java.lang.foreign.ValueLayout.ADDRESS, 0, type);
                d.destroyLogicalType.invokeExact(p);
            }
        }
    }

    /** ENUM('a', 'b'): each value quoted, a quote inside doubled. */
    private static String enumValues(Duck d, MemorySegment type) throws Throwable {
        long n = Integer.toUnsignedLong((int) d.enumSize.invokeExact(type));
        StringBuilder sb = new StringBuilder("ENUM(");
        for (long i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append('\'').append(d.owned((MemorySegment) d.enumValue.invokeExact(type, i)).replace("'", "''")).append('\'');
        }
        return sb.append(')').toString();
    }

    private static String alias(Duck d, MemorySegment type) throws Throwable {
        MemorySegment a = (MemorySegment) d.alias.invokeExact(type);
        return a.equals(MemorySegment.NULL) ? "" : d.owned(a);
    }

    /** DuckDB's rule: quote unless letters, digits (not first) and underscores, and not a keyword. */
    static String quoted(String name, Set<String> keywords) {
        boolean plain = true;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (i > 0 && c >= '0' && c <= '9');
            if (!ok) {
                plain = false;
                break;
            }
        }
        if (plain && !keywords.contains(name.toLowerCase(Locale.ROOT))) return name;
        return '"' + name.replace("\"", "\"\"") + '"';
    }
}
