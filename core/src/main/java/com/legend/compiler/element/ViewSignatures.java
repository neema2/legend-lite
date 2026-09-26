// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;
import com.legend.compiler.StoreLookups;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * THE signature of a store VIEW as a relation &mdash; the declared return type
 * of its lifted function (E.5), read off STORE FACTS the way the engine
 * types view columns ({@code inferRelationalType} over {@code columnMappings}):
 * <ul>
 *   <li>a plain column carries its table column's Pure type and its
 *       NOT NULL / PRIMARY KEY multiplicity (the table lookup is the index's,
 *       include closure); a plain column of an inner VIEW carries that
 *       view's signature column (view-on-view);</li>
 *   <li>an aggregate column carries its reducer's Pure result over the
 *       argument's store type ({@code count} Integer[1]; {@code sum} and
 *       {@code average} [1]; {@code max} / {@code min} [0..1]; Integer, Float
 *       and the dates keep their type, every other number is Number &mdash;
 *       Pure declares no Decimal overload for these reducers);</li>
 *   <li>any other expression carries the engine's inferred relational type
 *       at [0..1], or Any when the engine's own rule has none (a case over
 *       an INT and a VARCHAR branch);</li>
 *   <li>a column the view declares PRIMARY KEY is [1] whatever its source.</li>
 * </ul>
 * The compiler checks the lifted body against this signature when it
 * compiles the function: a disagreement is a bug in one of the two, loud,
 * never a silent widening. Measured 2026-09-22 on 52 views / 153 columns.
 */
public final class ViewSignatures {

    private ViewSignatures() {
    }

    public static Type.RelationType of(StoreLookups store, String dbFqn,
            DatabaseDefinition.ViewDefinition view) {
        return of(store, dbFqn, view, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static Type.RelationType of(StoreLookups store, String dbFqn,
            DatabaseDefinition.ViewDefinition view,
            Set<DatabaseDefinition.ViewDefinition> expanding) {
        if (!expanding.add(view)) {
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.MODEL,
                    "view '" + view.name() + "' of '" + dbFqn + "' reads its own columns"
                    + " (cyclic view-on-view chain)");
        }
        try {
            DatabaseDefinition db = store.findDatabase(dbFqn).orElse(null);
            List<Type.Column> columns = new ArrayList<>(view.columnMappings().size());
            for (DatabaseDefinition.ViewDefinition.ViewColumnMapping cm : view.columnMappings()) {
                Type.Column c = column(store, dbFqn, db, cm, expanding);
                columns.add(cm.primaryKey() && !c.multiplicity().equals(Multiplicity.Bounded.ONE)
                        ? new Type.Column(c.name(), c.type(), Multiplicity.Bounded.ONE) : c);
            }
            return new Type.RelationType(columns);
        } finally {
            expanding.remove(view);
        }
    }

    private static Type.Column column(StoreLookups store, String dbFqn,
            @com.legend.base.Nullable DatabaseDefinition db,
            DatabaseDefinition.ViewDefinition.ViewColumnMapping cm,
            Set<DatabaseDefinition.ViewDefinition> expanding) {
        RelationalOperation expr = cm.expression();
        if (expr instanceof RelationalOperation.ColumnRef cr) {
            String srcDb = cr.databaseName() != null ? cr.databaseName() : dbFqn;
            var td = store.findTableDefinition(srcDb, cr.table());
            if (td.isPresent()) {
                for (var col : td.get().columns()) {
                    if (col.name().equals(cr.column())) {
                        return new Type.Column(cm.name(), pureType(col.dataType()),
                                col.notNull() || col.primaryKey()
                                        ? Multiplicity.Bounded.ONE : Multiplicity.Bounded.ZERO_ONE);
                    }
                }
            }
            var inner = store.findView(srcDb, cr.table());
            if (inner.isPresent()) {
                for (Type.Column ic : of(store, srcDb, inner.get(), expanding).columns()) {
                    if (ic.name().equals(cr.column())) {
                        return new Type.Column(cm.name(), ic.type(), ic.multiplicity());
                    }
                }
            }
            return any(cm.name());
        }
        if (expr instanceof RelationalOperation.FunctionCall fc && fc.args().size() == 1) {
            String fn = fc.name().toLowerCase(Locale.ROOT);
            switch (fn) {
                case "count" -> {
                    return new Type.Column(cm.name(), Type.Primitive.INTEGER, Multiplicity.Bounded.ONE);
                }
                case "average" -> {
                    return new Type.Column(cm.name(), Type.Primitive.FLOAT, Multiplicity.Bounded.ONE);
                }
                case "sum", "max", "min" -> {
                    // Pure's reducer overloads: Integer and Float keep their type;
                    // every other number (Decimal, Number) is Number; max / min
                    // over a date keep the date type, over anything else the
                    // generic collection reducer keeps the element type
                    Type arg = inferred(fc.args().get(0), db, store);
                    boolean numeric = arg instanceof Type.PrecisionDecimal
                            || arg instanceof Type.Primitive p && p.family() == Type.Primitive.Family.NUMERIC;
                    Type out = arg == Type.Primitive.INTEGER || arg == Type.Primitive.FLOAT || !numeric
                            ? arg : Type.Primitive.NUMBER;
                    return new Type.Column(cm.name(), out, fn.equals("sum")
                            ? Multiplicity.Bounded.ONE : Multiplicity.Bounded.ZERO_ONE);
                }
                default -> { }
            }
        }
        return new Type.Column(cm.name(), inferred(expr, db, store), Multiplicity.Bounded.ZERO_ONE);
    }

    /** The engine's inferred relational type as a Pure type; Any when the
     *  engine's rule has none or the kind has no scalar Pure type. */
    private static Type inferred(RelationalOperation expr, @com.legend.base.Nullable DatabaseDefinition db,
            StoreLookups store) {
        RelationalDataType dt = RelationalTypeInference.infer(expr, db, store);
        return dt == null ? new Type.ClassType(PlatformTypes.ANY) : pureType(dt);
    }

    private static Type pureType(RelationalDataType dt) {
        return StoreCompiler.scalarType(dt).orElseGet(() -> new Type.ClassType(PlatformTypes.ANY));
    }

    private static Type.Column any(String name) {
        return new Type.Column(name, new Type.ClassType(PlatformTypes.ANY),
                Multiplicity.Bounded.ZERO_ONE);
    }
}
