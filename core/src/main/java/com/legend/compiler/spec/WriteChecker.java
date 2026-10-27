package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedDelete;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedMerge;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedUpdate;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.protocol.spec.AppliedFunction;

import java.util.List;
import java.util.Optional;

/**
 * Relation {@code write} (engine {@code WriteChecker}) &mdash; fully generic
 * ({@code write<T>(Relation<T>[1] [, target:Any[1]]):Integer[1]}); the optional
 * destination reference rides the node for the back-end.
 *
 * <p>A write INTO A STORE TABLE ({@code rel->write(#>{db.t}#)}) is typed by the
 * table's columns BY NAME (docs/WRITES_DESIGN_2026_10_18.md §4, L1; upstream
 * aligns them by position): each of the relation's columns names a column of the
 * table, of the same type or both numeric; the table's other columns take their
 * defaults. A column's multiplicity is not compared: a null into a NOT NULL
 * column is the database's error, as is a number it cannot hold
 * (SEMANTICS_REGISTER S97). The relation rule is applied here, never by loosening
 * the generic {@code T}-binding, as {@link ConcatenateChecker} applies its own.
 *
 * <p>Lite's other store writes (2026-10-18, the same leg) are checked here too, each over a store table:
 * {@code delete(r | pred)}, {@code update(r | pred, ~[c: r | expr])} (each column set named as the table's, its
 * expression of the column's type or both numeric, at most one value) and {@code merge(#>{db.t}#, ~[keys])} (the
 * rows by name as {@code write}'s, the keys columns of both).
 */
final class WriteChecker {

    private WriteChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 2
                && af.parameters().stream().noneMatch(DeferredArgs::deferredArg)) {
            // each argument types exactly ONCE (a second synth would re-register typer state)
            TypedSpec rows = t.synth(af.parameters().get(0), env);
            TypedSpec destination = t.synth(af.parameters().get(1), env);
            Type.RelationType schema = Type.relationSchema(rows.info().type());
            if (destination instanceof TypedTableReference table && schema != null) {
                byName(schema, table);
                return new TypedWrite(rows, Optional.of(destination), ExprType.one(Type.Primitive.INTEGER));
            }
            Application a = t.checkGenericTyped(af, List.of(rows, destination));
            return new TypedWrite(a.args().get(0), Optional.of(a.args().get(1)), a.out());
        }
        Application a = t.checkGeneric(af, env);
        Optional<TypedSpec> destination = a.args().size() > 1
                ? Optional.of(a.args().get(1)) : Optional.empty();
        return new TypedWrite(a.args().get(0), destination, a.out());
    }

    /** {@code #>{db.t}#->delete(r | pred)}: the table's rows the predicate holds for. */
    static TypedSpec delete(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedDelete(storeTable(a.args().get(0), "delete"), Args.lambda(a, 1), a.out());
    }

    /** {@code #>{db.t}#->update(r | pred, ~[c: r | expr])}: each column set is the table's, its value one of its type. */
    static TypedSpec update(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        TypedTableReference table = storeTable(a.args().get(0), "update");
        if (!(a.args().get(2) instanceof TypedFuncColSpecArray set) || set.cols().isEmpty()) {
            throw new TypeInferenceException("update: its third argument sets the table's columns, ~[c: r | expr, ...]");
        }
        java.util.Map<String, Type.Column> columns = columnsOf(table, "update");
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (TypedFuncCol c : set.cols()) {
            Type.Column into = column(columns, c.name(), table, "update", "the column set");
            if (!seen.add(c.name())) {
                throw new TypeInferenceException("update: the column '" + c.name() + "' is set twice");
            }
            var value = c.fn().functionType().result();
            assignable("update", "the value set into '" + c.name() + "'", value.type(), into, table);
            if (value.multiplicity().requireBounded("update").isMany()) {
                throw new TypeInferenceException("update: the value set into '" + c.name() + "' is "
                        + value.multiplicity().text() + ": a column holds at most one value");
            }
        }
        return new TypedUpdate(table, Args.lambda(a, 1), set, a.out());
    }

    /** {@code rel->merge(#>{db.t}#, ~[keys])}: the rows by name as {@code write}'s, the keys columns of both. */
    static TypedSpec merge(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        TypedSpec rows = a.args().get(0);
        TypedTableReference table = storeTable(a.args().get(1), "merge");
        Type.RelationType schema = Type.relationSchema(rows.info().type());
        if (schema == null) {
            throw new TypeInferenceException("merge: its first argument must be a relation, not "
                    + rows.info().type().typeName());
        }
        byName(schema, table);
        if (!(a.args().get(2) instanceof TypedColSpecArray keys) || keys.names().isEmpty()) {
            throw new TypeInferenceException("merge: its third argument names the key columns, ~[k, ...]");
        }
        java.util.Set<String> written = new java.util.HashSet<>();
        schema.columns().forEach(c -> written.add(c.name()));
        for (String k : keys.names()) {
            if (!written.contains(k)) {
                throw new TypeInferenceException("merge: the key '" + k + "' is not a column of the rows merged "
                        + written);
            }
        }
        return new TypedMerge(rows, table, keys, a.out());
    }

    /** {@code spec} as the store table a write names, refused by name otherwise (a tabular function, a frame). */
    private static TypedTableReference storeTable(TypedSpec spec, String op) {
        if (spec instanceof TypedTableReference t && !t.call() && t.frame() == null) {
            return t;
        }
        throw new TypeInferenceException(op + ": it writes a store table (#>{db.t}#), not "
                + spec.getClass().getSimpleName());
    }

    private static java.util.Map<String, Type.Column> columnsOf(TypedTableReference table, String op) {
        Type.RelationType columns = Type.schemaView(table.info().type());
        if (columns == null) {
            throw new TypeInferenceException(op + ": the table " + table.store() + "." + table.table()
                    + " has no columns (" + table.info().type().typeName() + ")");
        }
        java.util.Map<String, Type.Column> byName = new java.util.LinkedHashMap<>();
        columns.columns().forEach(c -> byName.put(c.name(), c));
        return byName;
    }

    private static Type.Column column(java.util.Map<String, Type.Column> columns, String name,
            TypedTableReference table, String op, String what) {
        Type.Column into = columns.get(name);
        if (into == null) {
            throw new TypeInferenceException(op + ": " + what + " '" + name + "' is not a column of " + table.store()
                    + "." + table.table() + " " + columns.keySet() + " (a write names the table's columns)");
        }
        return into;
    }

    private static void assignable(String op, String what, Type value, Type.Column into, TypedTableReference table) {
        if (!value.equals(into.type()) && !(numeric(value) && numeric(into.type()))) {
            throw new TypeInferenceException(op + ": " + what + " is " + value.typeName() + ", which cannot be"
                    + " written into " + table.store() + "." + table.table() + "'s column '" + into.name() + "' "
                    + into.type().typeName());
        }
    }

    /** Each of the rows' columns is a column of {@code table} by name, of its type or both numeric. */
    private static void byName(Type.RelationType rows, TypedTableReference table) {
        java.util.Map<String, Type.Column> columns = columnsOf(table, "write");
        for (Type.Column c : rows.columns()) {
            Type.Column into = column(columns, c.name(), table, "write", "the relation's column");
            assignable("write", "the relation's column '" + c.name() + "'", c.type(), into, table);
        }
    }

    private static boolean numeric(Type t) {
        return t instanceof Type.PrecisionDecimal || (t instanceof Type.Primitive p && p.isNumeric());
    }
}
