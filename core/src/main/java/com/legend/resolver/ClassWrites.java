// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedDelete;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMerge;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.error.MappingResolutionException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The class writes over DIRECT mappings (docs/WRITES_DESIGN_2026_10_18.md §4, L2; 2026-10-20, the Cypher island's
 * writes on a graph of a domain model): each a class write's call ({@code writeInstances}, {@code mergeInstances},
 * {@code deleteInstances}, {@code link}, {@code unlink}; NativeFn.ResolverForm) written as L1's statement over the
 * table its class's mapping reads, the instances' properties renamed to their columns:
 * <ul>
 *   <li>{@code rows->writeInstances(C, ~k)}: the rows inserted into C's table, the key column into its primary key
 *       ({@link TypedWrite});</li>
 *   <li>{@code rows->mergeInstances(C, ~k)}: the rows merged into it by its primary key ({@link TypedMerge});</li>
 *   <li>{@code rows->deleteInstances(C, ~k)}: its rows whose primary key a row holds deleted ({@link TypedDelete});</li>
 *   <li>{@code pairs->link(C, ~end, ~k)} and {@code unlink}: an association end of C held by a foreign key, written
 *       as the merge of the key column of the table that holds it (the other instance's key, or none).</li>
 * </ul>
 * A mapping is DIRECT when its class's rows are one table's (its pipeline the table under navigation slots alone:
 * no filter, no distinct, no group-by, no union), each property written is one of its columns, and its key is one
 * column (the mapping's {@code ~primaryKey}, else the table's PRIMARY KEY: the identity elementId reads). Anything
 * else is refused by name, never assumed (the view-update problem: a mapping that is not direct has no one write).
 */
final class ClassWrites {

    private ClassWrites() {
    }

    /** The L1 statement the class write {@code nc} ({@code form}) means over {@code cs}, its class's source under the
     *  query's mapping; {@code sourceOf} the source of another class (an association's other end). */
    static TypedSpec lower(TypedNativeCall nc, NativeFn.ResolverForm form, ClassSource cs,
            java.util.function.Function<String, ClassSource> sourceOf,
            com.legend.compiler.element.ModelContext ctx, Callees callees, AssociationJoins associations) {
        TypedSpec rows = nc.args().get(0);
        String key = colSpec(nc.args().get(nc.args().size() - 1), form);
        Type.RelationType rowType = rowType(rows, form);
        TypedTableReference table = mainTable(cs);
        String pk = keyColumn(cs);
        return switch (form) {
            case WRITE_INSTANCES -> new TypedWrite(renamed(rows, rowType, key, pk, cs), Optional.of(table),
                    nc.info());
            case MERGE_INSTANCES -> new TypedMerge(renamed(rows, rowType, key, pk, cs), table, keys(pk), nc.info());
            case DELETE_INSTANCES -> new TypedDelete(table, held(rows, rowType, key, table, pk, callees), nc.info());
            case LINK, UNLINK -> linked(nc, form, rows, rowType, key, colSpec(nc.args().get(2), form), cs, sourceOf,
                    ctx, associations);
            default -> throw new IllegalStateException("resolver bug: " + form + " is not a class write");
        };
    }

    /** {@code pairs->link(C, ~end, ~k)}: the end held by a foreign key, on C's table (C's row of key {@code k} set
     *  to the other instance's key) or on the other end's class's (the other instance's row set to C's key); unlinked,
     *  set to none. The association's condition must be the one equality of a foreign key and the other table's key
     *  (AssociationJoins.associationParentSide); an end over a junction table, or of any other shape, is refused by
     *  name. */
    private static TypedSpec linked(TypedNativeCall nc, NativeFn.ResolverForm form, TypedSpec pairs,
            Type.RelationType rowType, String key, String end, ClassSource cs,
            java.util.function.Function<String, ClassSource> sourceOf,
            com.legend.compiler.element.ModelContext ctx, AssociationJoins associations) {
        boolean link = form == NativeFn.ResolverForm.LINK;
        if (rowType.columns().stream().noneMatch(c -> c.name().equals(end))) {
            throw new MappingResolutionException(form.fqn() + ": the end " + end + " of " + cs.classFqn()
                    + " names no column of the pairs " + names(rowType), cs.classFqn());
        }
        var association = ctx.findAssociationOf(cs.classFqn(), end).orElseThrow(() -> new MappingResolutionException(
                form.fqn() + ": " + end + " is no association end of " + cs.classFqn(), cs.classFqn()));
        // (the end's instances: the class the end property holds)
        String target = (association.property1().propertyName().equals(end) ? association.property1()
                : association.property2()).targetClassFqn();
        ClassSource ts = sourceOf.apply(target);
        AssociationJoins.ParentSide side = associations.associationParentSide(cs, end);
        String[] columns = side == null ? null : equated(side);
        if (columns == null) {
            throw new com.legend.error.NotImplementedException(form.fqn() + ": the association end " + end + " of "
                    + cs.classFqn() + " is no one foreign key's (a junction table, or a join of another shape):"
                    + " writing it is not supported yet");
        }
        String parentColumn = columns[0];
        String targetColumn = columns[1];
        if (targetColumn.equals(keyColumn(ts)) && !parentColumn.equals(keyColumn(cs))) {
            // (C's own row holds the other instance's key)
            return new TypedMerge(pairsOnto(pairs, rowType, key, keyColumn(cs), end, parentColumn, link),
                    mainTable(cs), keys(keyColumn(cs)), nc.info());
        }
        if (parentColumn.equals(keyColumn(cs)) && !targetColumn.equals(keyColumn(ts))) {
            // (the other instance's row holds C's key: the pairs' end column is its key, the key column its value)
            return new TypedMerge(pairsOnto(pairs, rowType, end, keyColumn(ts), key, targetColumn, link),
                    mainTable(ts), keys(keyColumn(ts)), nc.info());
        }
        throw new com.legend.error.NotImplementedException(form.fqn() + ": the association end " + end + " of "
                + cs.classFqn() + " equates " + parentColumn + " and " + targetColumn + ", no foreign key and the other"
                + " table's key: writing it is not supported yet");
    }

    /** The parent's and the target's column the association's condition equates (its one {@code ==} of a column of
     *  each row); null for a condition of any other shape. */
    private static String @com.legend.base.Nullable [] equated(AssociationJoins.ParentSide side) {
        TypedLambda cond = side.cond();
        if (cond.parameters().size() != 2 || cond.body().size() != 1
                || !(cond.body().get(0) instanceof TypedNativeCall eq) || eq.args().size() != 2
                || !com.legend.builtin.Pure.AT_BOOLEAN_EQUAL.contains(eq.callee().id())) {
            return null;
        }
        String parent = cond.parameters().get(side.parentParam());
        String target = cond.parameters().get(1 - side.parentParam());
        String parentColumn = null;
        String targetColumn = null;
        for (TypedSpec arg : eq.args()) {
            TypedSpec a = arg;
            while (a instanceof TypedNativeCall c && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                a = c.args().get(0);
            }
            if (!(a instanceof TypedPropertyAccess pa) || !(pa.source() instanceof TypedVariable v)) {
                return null;
            }
            if (v.name().equals(parent)) {
                parentColumn = pa.property();
            } else if (v.name().equals(target)) {
                targetColumn = pa.property();
            }
        }
        return parentColumn == null || targetColumn == null ? null : new String[] {parentColumn, targetColumn};
    }

    /** The pairs as the rows of a table holding a foreign key: {@code rowKey} its primary key {@code pk}, and
     *  {@code value} (when linking; else none) its foreign-key column {@code fk}. */
    private static TypedSpec pairsOnto(TypedSpec pairs, Type.RelationType rowType, String rowKey, String pk,
            String value, String fk, boolean link) {
        List<TypedFuncCol> cols = new ArrayList<>();
        List<Type.Column> out = new ArrayList<>();
        Type.Column keyCol = columnOf(rowType, rowKey);
        cols.add(read(pk, rowType, keyCol));
        out.add(new Type.Column(pk, keyCol.type(), keyCol.multiplicity()));
        Type.Column valueCol = columnOf(rowType, value);
        if (link) {
            cols.add(read(fk, rowType, valueCol));
        } else {
            cols.add(none(fk, rowType, valueCol.type()));
        }
        out.add(new Type.Column(fk, valueCol.type(), Multiplicity.Bounded.ZERO_ONE));
        return new TypedProject(pairs, cols, ExprType.one(Type.relation(new Type.RelationType(out))));
    }

    /** The rows renamed to the table's columns: the key column to the primary key, each other column (a property of
     *  the class) to the column its mapping reads it from. */
    private static TypedSpec renamed(TypedSpec rows, Type.RelationType rowType, String key, String pk,
            ClassSource cs) {
        List<TypedFuncCol> cols = new ArrayList<>();
        List<Type.Column> out = new ArrayList<>();
        Map<String, String> seen = new LinkedHashMap<>();
        for (Type.Column c : rowType.columns()) {
            String into;
            if (c.name().equals(key)) {
                into = pk;
            } else {
                TypedSpec binding = cs.bindings().get(c.name());
                if (binding == null && c.name().indexOf('.') > 0) {
                    // an embedded property's sub-property ({@code p.sub}): its binding inside the embedded instance's
                    binding = embedded(cs, c.name());
                }
                if (binding == null) {
                    throw new MappingResolutionException("writing " + cs.classFqn() + ": the rows' column " + c.name()
                            + " is no property its mapping " + cs.mappingFqn() + " maps", cs.classFqn());
                }
                into = column(binding, cs, c.name());
            }
            String before = seen.put(into, c.name());
            if (before != null) {
                throw new MappingResolutionException("writing " + cs.classFqn() + ": " + before + " and " + c.name()
                        + " are one column, " + into, cs.classFqn());
            }
            cols.add(read(into, rowType, c));
            out.add(new Type.Column(into, c.type(), c.multiplicity()));
        }
        return new TypedProject(rows, cols, ExprType.one(Type.relation(new Type.RelationType(out))));
    }

    /** {@code t | $rows->filter(r | $r.key == $t.pk)->isNotEmpty()}: the table's row is one a row of the rows keys. */
    private static TypedLambda held(TypedSpec rows, Type.RelationType rowType, String key, TypedTableReference table,
            String pk, Callees callees) {
        Type.RelationType tableRow = Type.schemaView(table.info().type());
        if (tableRow == null) {
            throw new IllegalStateException("resolver bug: the table " + table.table() + " has no columns");
        }
        Type.Column pkCol = columnOf(tableRow, pk);
        Type.Column keyCol = columnOf(rowType, key);
        ExprType one = ExprType.one(Type.Primitive.BOOLEAN);
        TypedSpec eq = new TypedNativeCall(java.util.Objects.requireNonNull(callees.equal(),
                "resolver bug: no equal registration"), List.of(
                new TypedPropertyAccess(new TypedVariable("_r", ExprType.one(rowType)), key,
                        new ExprType(keyCol.type(), keyCol.multiplicity())),
                new TypedPropertyAccess(new TypedVariable("_t", ExprType.one(tableRow)), pk,
                        new ExprType(pkCol.type(), pkCol.multiplicity()))), one);
        TypedLambda match = lambda("_r", rowType, eq, one);
        // (a filter correlated with the table's row: the delete's own predicate reads it)
        TypedSpec found = new com.legend.compiler.spec.typed.TypedFilter(rows, match, rows.info(),
                com.legend.compiler.spec.typed.TypedFilter.Stamp.CORRELATION);
        TypedSpec some = new TypedNativeCall(callees.isNotEmpty(), List.of(found), one);
        return lambda("_t", tableRow, some, one);
    }

    private static TypedLambda lambda(String param, Type.RelationType row, TypedSpec body, ExprType result) {
        return new TypedLambda(List.of(param), List.of(body), ExprType.one(new Type.FunctionType(List.of(
                new Type.Param(row, Multiplicity.Bounded.ONE)), new Type.Param(result.type(), result.multiplicity()))));
    }

    /** The column {@code name} of the projection: the row's column {@code from} read as it is. */
    private static TypedFuncCol read(String name, Type.RelationType row, Type.Column from) {
        TypedSpec value = new TypedPropertyAccess(new TypedVariable("_w", ExprType.one(row)), from.name(),
                new ExprType(from.type(), from.multiplicity()));
        return new TypedFuncCol(name, new TypedLambda(List.of("_w"), List.of(value), ExprType.one(
                new Type.FunctionType(List.of(new Type.Param(row, Multiplicity.Bounded.ONE)),
                        new Type.Param(from.type(), from.multiplicity())))));
    }

    /** The column {@code name} of the projection: no value, of {@code type}. */
    private static TypedFuncCol none(String name, Type.RelationType row, Type type) {
        TypedSpec value = new com.legend.compiler.spec.typed.TypedCollection(List.of(),
                new ExprType(type, Multiplicity.Bounded.ZERO_ONE));
        return new TypedFuncCol(name, new TypedLambda(List.of("_w"), List.of(value), ExprType.one(
                new Type.FunctionType(List.of(new Type.Param(row, Multiplicity.Bounded.ONE)),
                        new Type.Param(type, Multiplicity.Bounded.ZERO_ONE)))));
    }

    private static TypedColSpecArray keys(String pk) {
        Type.RelationType row = new Type.RelationType(List.of(new Type.Column(pk,
                com.legend.compiler.spec.InferenceKernel.UNKNOWN_COLUMN_TYPE, Multiplicity.Bounded.ONE)));
        return new TypedColSpecArray(List.of(pk), ExprType.one(new Type.GenericType(PlatformTypes.COL_SPEC_ARRAY,
                List.of(row))));
    }

    /** The class's table: its pipeline's table under navigation slots alone, as a store table's accessor. A pipeline
     *  of any other shape (a filter, a distinct, a group-by, a union, a model-to-model source) is not direct. */
    private static TypedTableReference mainTable(ClassSource cs) {
        TypedSpec p = cs.pipeline();
        while (true) {
            if (p instanceof TypedNavigate nav) {
                p = nav.source();
            } else if (p instanceof TypedJoinSlot js) {
                p = js.source();
            } else {
                break;
            }
        }
        if (!(p instanceof TypedTableReference t) || t.call() || t.frame() != null) {
            throw new MappingResolutionException(cs.classFqn() + " is not writable: its mapping " + cs.mappingFqn()
                    + " reads its instances through " + p.getClass().getSimpleName() + ", not one table's rows (a"
                    + " write needs a direct mapping, docs/WRITES_DESIGN_2026_10_18.md §4, L2)", cs.classFqn());
        }
        return new TypedTableReference(t.store(), t.table(), t.info(), true, null, t.quotedColumns(), false,
                t.storedTypes(), t.quotedTable(), t.quotedSchema());
    }

    /** The class's one key column: its primary-key pseudo-binding (the mapping's {@code ~primaryKey}, else the main
     *  table's PRIMARY KEY), refused by name when it has none or several. */
    private static String keyColumn(ClassSource cs) {
        List<String> keys = cs.bindings().keySet().stream()
                .filter(com.legend.model.ClassMapping::isPrimaryKeyBinding).toList();
        if (keys.size() != 1) {
            throw new MappingResolutionException(cs.classFqn() + " is not writable: its mapping " + cs.mappingFqn()
                    + " gives it " + keys.size() + " key columns, a write needs one (its ~primaryKey, else its"
                    + " table's PRIMARY KEY)", cs.classFqn());
        }
        return column(java.util.Objects.requireNonNull(cs.bindings().get(keys.get(0))), cs, keys.get(0));
    }

    /** The binding of the sub-property {@code path} ({@code p.sub}) of an embedded property {@code p}: the
     *  sub-property's inside the instance its mapping builds ({@code ^V(sub = ..., ...)}, under a guard where the
     *  property may be none); null for any other shape. */
    private static @com.legend.base.Nullable TypedSpec embedded(ClassSource cs, String path) {
        int dot = path.indexOf('.');
        TypedSpec b = cs.bindings().get(path.substring(0, dot));
        while (b instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
            b = c.args().get(0);
        }
        if (b instanceof com.legend.compiler.spec.typed.TypedIf i) {
            b = i.thenBranch() instanceof com.legend.compiler.spec.typed.TypedNewInstance ? i.thenBranch()
                    : i.elseBranch().orElse(null);
        }
        return b instanceof com.legend.compiler.spec.typed.TypedNewInstance ni ? ni.properties().get(path.substring(
                dot + 1)) : null;
    }

    /** The column of the class's row a binding reads as it is ({@code $row.col}, through a {@code toOne}); refused by
     *  name when it computes its value. */
    private static String column(TypedSpec binding, ClassSource cs, String what) {
        TypedSpec b = binding;
        while (b instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
            b = c.args().get(0);
        }
        if (b instanceof TypedPropertyAccess pa && pa.source() instanceof TypedVariable v
                && v.name().equals(cs.rowVar())) {
            return pa.property();
        }
        throw new MappingResolutionException(cs.classFqn() + " is not writable: its mapping " + cs.mappingFqn()
                + " computes " + what + " (" + b.getClass().getSimpleName() + "), not one column's value (a write"
                + " needs a direct mapping)", cs.classFqn());
    }

    private static String colSpec(TypedSpec arg, NativeFn.ResolverForm form) {
        if (arg instanceof TypedColSpec c) {
            return c.name();
        }
        throw new IllegalStateException("resolver bug: " + form.fqn() + " names a column by ~col, not "
                + arg.getClass().getSimpleName());
    }

    private static Type.RelationType rowType(TypedSpec rows, NativeFn.ResolverForm form) {
        Type.RelationType t = Type.relationSchema(rows.info().type());
        if (t == null) {
            throw new IllegalStateException("resolver bug: " + form.fqn() + "'s rows are no relation: "
                    + rows.info().type().typeName());
        }
        return t;
    }

    private static Type.Column columnOf(Type.RelationType row, String name) {
        return row.columns().stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow(() ->
                new IllegalStateException("resolver bug: no column " + name + " in " + names(row)));
    }

    private static List<String> names(Type.RelationType row) {
        return row.columns().stream().map(Type.Column::name).toList();
    }

    /** The class a class write writes: its second argument's reference. */
    static String classOf(TypedNativeCall nc) {
        if (nc.args().size() >= 2 && nc.args().get(1) instanceof TypedPackageableRef ref) {
            return ref.fullPath();
        }
        throw new IllegalStateException("resolver bug: a class write names its class by reference, not "
                + nc.args().get(1).getClass().getSimpleName());
    }
}
