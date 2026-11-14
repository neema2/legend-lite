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
 *   <li>a column the operation reads that its database does not have, plain or
 *       nested, refuses the view in the engine's words, as the engine refuses it
 *       when the database compiles (S376, 2026-11-13, ruling 32); a join
 *       navigation's terminal too, once its joins resolve (2026-11-13, ruling 34);</li>
 *   <li>an aggregate column, at any arity, carries its reducer's Pure result over the
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
        // every column the operation reads exists, or the view is refused in the engine's words, as the engine refuses
        // it when the database compiles (processDatabaseViewSecondPass: each TableAliasColumn through getRelation and
        // getColumn, HelperRelationalBuilder.java:160-165, :284-293); until 2026-11-13 a plain column found nowhere was
        // typed Any[0..1] and a nested one read as no type (S376, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 32)
        requireColumns(store, dbFqn, expr);
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
            throw cantFindColumn(cr.column());
        }
        // an aggregate at any arity, typed as the engine's inferRelationalType types it (relationalExtension.pure:
        // count BIGINT and average DOUBLE whatever their arguments, :291-300, :543-551; sum by its first, :1717-1741;
        // max and min over several the safe type when one exists, else the first's, :1151-1197); until 2026-11-13 at
        // exactly one argument only (S376, ruling 32)
        if (expr instanceof RelationalOperation.FunctionCall fc) {
            String fn = fc.name().toLowerCase(Locale.ROOT);
            switch (fn) {
                case "count" -> {
                    return new Type.Column(cm.name(), Type.Primitive.INTEGER, Multiplicity.Bounded.ONE);
                }
                case "average" -> {
                    return new Type.Column(cm.name(), Type.Primitive.FLOAT, Multiplicity.Bounded.ONE);
                }
                case "sum", "max", "min" -> {
                    if (fc.args().isEmpty()) {
                        break;
                    }
                    // Pure's reducer overloads: Integer and Float keep their type;
                    // every other number (Decimal, Number) is Number; max / min
                    // over a date keep the date type, over anything else the
                    // generic collection reducer keeps the element type
                    Type arg = inferred(fn.equals("sum") || fc.args().size() == 1 ? fc.args().get(0) : fc, db, store);
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
        return StoreCompiler.columnType(dt);
    }

    /** Every column reference of {@code op} (a function's arguments, a comparison's and a boolean's sides, a group, a
     *  null test, an array literal, a lambda's body, a join navigation's terminal once its joins resolve) names a table
     *  or view of its database (the include closure, the index's spellings) and a column of it. */
    private static void requireColumns(StoreLookups store, String dbFqn, RelationalOperation op) {
        switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                String srcDb = cr.databaseName() != null ? cr.databaseName() : dbFqn;
                var td = store.findTableDefinition(srcDb, cr.table());
                if (td.isPresent()) {
                    if (td.get().columns().stream().noneMatch(c -> c.name().equals(cr.column()))) {
                        throw cantFindColumn(cr.column());
                    }
                    return;
                }
                var view = store.findView(srcDb, cr.table());
                if (view.isPresent()) {
                    if (view.get().columnMappings().stream().noneMatch(c -> c.name().equals(cr.column()))) {
                        throw cantFindColumn(cr.column());
                    }
                    return;
                }
                throw cantFindTable(cr.table(), srcDb);
            }
            case RelationalOperation.FunctionCall f -> f.args().forEach(a -> requireColumns(store, dbFqn, a));
            case RelationalOperation.Comparison c -> {
                requireColumns(store, dbFqn, c.left());
                requireColumns(store, dbFqn, c.right());
            }
            case RelationalOperation.BooleanOp b -> {
                requireColumns(store, dbFqn, b.left());
                requireColumns(store, dbFqn, b.right());
            }
            case RelationalOperation.IsNull n -> requireColumns(store, dbFqn, n.operand());
            case RelationalOperation.IsNotNull n -> requireColumns(store, dbFqn, n.operand());
            case RelationalOperation.Group g -> requireColumns(store, dbFqn, g.inner());
            case RelationalOperation.ArrayLiteral a -> a.elements().forEach(e -> requireColumns(store, dbFqn, e));
            case RelationalOperation.Lambda l -> requireColumns(store, dbFqn, l.body());
            // a join navigation's terminal is compiled as any other operation, after the navigation's joins
            // (HelperRelationalBuilder.java:1181-1186: buildElementWithJoinsJoinTreeNode's getJoin first, :1255,
            // :167-192, then processRelationalOperationElement over the terminal, each TableAliasColumn through getRelation
            // and getColumn, :1170-1171; a {target} column a self-join's, unchecked, :1162-1168), in the database of the
            // navigation's last join (RelationalParseTreeWalker.java:1024-1044; the model writes the terminal relative to
            // the navigation's own database, RelOpFromProtocol). So a terminal reading a column or table its database
            // lacks refuses the view in the same words, once every join resolves: a missing join is refused first, by the
            // lift in lite's own words (JoinChainEmission). Until 2026-11-13 the terminal was not checked (S376's join
            // terminal, the names follow-ups, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 34)
            case RelationalOperation.JoinNavigation j -> {
                String navDb = j.databaseName() != null ? j.databaseName() : dbFqn;
                if (j.terminal() != null && j.chain().stream().allMatch(hop -> store.findJoin(
                        hop.databaseName() != null ? hop.databaseName() : navDb, hop.joinName()).isPresent())) {
                    requireColumns(store, navDb, j.terminal());
                }
            }
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored -> { }
            case RelationalOperation.LambdaParam ignored -> { }
        }
    }

    /** The engine's getColumn refusal (HelperRelationalBuilder.java:160-165). */
    private static com.legend.error.ModelException cantFindColumn(String column) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                "Can't find column '" + column + "'");
    }

    /** The engine's getRelation refusal (HelperRelationalBuilder.java:284-293): the table, its schema ({@code default}
     *  when unwritten) and the database's own name. */
    private static com.legend.error.ModelException cantFindTable(String spelled, String dbFqn) {
        int dot = spelled.lastIndexOf('.');
        String schema = dot < 0 ? "default" : spelled.substring(0, dot);
        String table = dot < 0 ? spelled : spelled.substring(dot + 1);
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                "Can't find table '" + table + "' in schema '" + schema + "' and database '"
                        + com.legend.protocol.Protocol.splitFqn(dbFqn)[1] + "'");
    }
}
