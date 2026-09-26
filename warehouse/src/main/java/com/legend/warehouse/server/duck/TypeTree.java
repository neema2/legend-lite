package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;

import com.legend.warehouse.sqlapi.DuckType;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * A result column's type, read once per result: the API's {@link DuckType}, the
 * DuckDB logical type handle (a nested cell's value is built against it), an
 * ENUM's index width, and the children's trees. Child handles are this tree's
 * to destroy; the root's belongs to the result.
 */
final class TypeTree {

    final DuckType type;
    final MemorySegment logical;
    final int id;
    final int enumWidth;
    /** DECIMAL(p,s): p and s; 0 otherwise. */
    final int precision;
    final int scale;
    final List<TypeTree> children;
    private final boolean ownsLogical;

    private TypeTree(DuckType type, MemorySegment logical, int id, int enumWidth, List<TypeTree> children, boolean owns) {
        this.type = type;
        this.logical = logical;
        this.id = id;
        this.enumWidth = enumWidth;
        int[] ps = type instanceof DuckType.Scalar s && s.base().equals("DECIMAL") ? decimal(s.name()) : new int[2];
        this.precision = ps[0];
        this.scale = ps[1];
        this.children = children;
        this.ownsLogical = owns;
    }

    static TypeTree of(Duck d, DuckType type, MemorySegment logical, boolean owns) {
        try {
            int id = (int) d.typeId.invokeExact(logical);
            int enumWidth = 0;
            if (id == Duck.ENUM) {
                enumWidth = switch ((int) d.enumInternalType.invokeExact(logical)) {
                    case Duck.UTINYINT -> 1;
                    case Duck.USMALLINT -> 2;
                    default -> 4;
                };
            }
            List<TypeTree> kids = new ArrayList<>();
            switch (type) {
                case DuckType.ListOf l -> kids.add(of(d, l.element(), (MemorySegment) (id == Duck.ARRAY
                        ? (MemorySegment) d.arrayChild.invokeExact(logical)
                        : (MemorySegment) d.listChild.invokeExact(logical)), true));
                case DuckType.StructOf s -> {
                    for (int i = 0; i < s.fields().size(); i++) {
                        kids.add(of(d, s.fields().get(i).type(), (MemorySegment) d.structType.invokeExact(logical, (long) i), true));
                    }
                }
                case DuckType.MapOf m -> {
                    kids.add(of(d, m.key(), (MemorySegment) d.mapKey.invokeExact(logical), true));
                    kids.add(of(d, m.value(), (MemorySegment) d.mapValue.invokeExact(logical), true));
                }
                case DuckType.Scalar s -> {
                }
            }
            return new TypeTree(type, logical, id, enumWidth, List.copyOf(kids), owns);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /** DECIMAL(p,s)'s p and s: DuckDB always spells both. */
    private static int[] decimal(String name) {
        int open = name.indexOf('('), comma = name.indexOf(',', open), close = name.indexOf(')', comma);
        return new int[] {Integer.parseInt(name.substring(open + 1, comma).strip()),
                Integer.parseInt(name.substring(comma + 1, close).strip())};
    }

    /** A fixed-size array ({@code T[n]}): its size; 0 for a list. */
    long arraySize(Duck d) {
        if (id != Duck.ARRAY) return 0;
        try {
            return (long) d.arraySize.invokeExact(logical);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    void destroy(Duck d) {
        for (TypeTree c : children) c.destroy(d);
        if (!ownsLogical) return;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            p.set(ADDRESS, 0, logical);
            d.destroyLogicalType.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }
}
