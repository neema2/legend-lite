// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * The SQL type of a relational operation element — the compiler's typing
 * rule for mapping expressions and view column expressions (the engine's
 * {@code inferRelationalType} / {@code getSafeType} spec,
 * relationalExtension.pure:111-190 and :1906-2010). The metamodel store
 * STAMPS the result on every relational-operation row it seeds
 * ({@code relational_elements.type_id}), the include-closure precedent: the
 * engine recurses over the tree per query; ours reads the compile-time
 * fact as a row. Moved from the legacy walk ({@code MetamodelWalk.inferOp},
 * group F burn 2026-09-02) — the walk keeps a delegating shim for the
 * constructed-instance tests until those instances are rows too.
 *
 * <p>Null = the element has no inferable type (the engine's
 * {@code failOnMatchFailure=false} branch) — a seeded row then has no
 * type row, and {@code inferRelationalType} over it is EMPTY, loud in
 * every {@code toOne()} the corpus writes after it.
 */
public final class RelationalTypeInference {

    private RelationalTypeInference() {
    }

    /** The type of {@code op}; {@code db} scopes bare column references
     * (a view's column expressions read their own database), {@code ctx}
     * resolves an op's own {@code [db]} qualifier (mapping expressions
     * carry it). */
    public static @com.legend.base.Nullable RelationalDataType infer(RelationalOperation op,
            @com.legend.base.Nullable DatabaseDefinition db,
            @com.legend.base.Nullable StoreLookups ctx) {
        return switch (op) {
            case RelationalOperation.ColumnRef c ->
                    columnType(db, ctx, c.databaseName(), c.table(), c.column());
            case RelationalOperation.FunctionCall f -> switch (
                    f.name().toLowerCase(java.util.Locale.ROOT)) {
                // aggregates carry their argument's type (engine
                // inferDynaFunctionReturnType: max/min/sum/avg family)
                case "max", "min", "distinct" ->
                        f.args().isEmpty() ? null
                                : infer(f.args().get(0), db, ctx);
                // sum/avg PROMOTE float-family to DOUBLE (engine
                // inferDynaFunctionReturnType aggregation rule)
                case "sum", "average" -> {
                    RelationalDataType at = f.args().isEmpty() ? null
                            : infer(f.args().get(0), db, ctx);
                    yield at instanceof RelationalDataType.Float_
                            || at instanceof RelationalDataType.Real
                            ? new RelationalDataType.Double_() : at;
                }
                case "count" -> new RelationalDataType.Integer_();
                // boolean dynafunctions spell BIT (engine H2)
                case "and", "or", "not", "equal", "notequal", "in",
                        "isnull", "isnotnull", "greaterthan", "lessthan",
                        "greaterthanequal", "lessthanequal", "isempty",
                        "isnotempty", "like", "startswith", "endswith",
                        "contains" -> new RelationalDataType.Bit();
                // sqlNull carries the engine's OTHER type
                case "sqlnull" -> new RelationalDataType.Other();
                // engine inferDynaFunctionReturnType: sqlTrue/sqlFalse
                // are BIT literals (ledger cluster 56)
                case "sqltrue", "sqlfalse" -> new RelationalDataType.Bit();
                // string transforms keep their input's type
                case "substring", "left", "right", "trim", "ltrim",
                        "rtrim", "toupper", "tolower", "upper", "lower" ->
                        f.args().isEmpty() ? null
                                : infer(f.args().get(0), db, ctx);
                case "position", "length", "charindex", "locate",
                        "indexof" -> new RelationalDataType.Integer_();
                case "sub" -> {
                    RelationalDataType acc2 = null;
                    for (var arg : f.args()) {
                        acc2 = safe(acc2, infer(arg, db, ctx));
                    }
                    yield acc2 == null
                            ? new RelationalDataType.Integer_() : acc2;
                }
                // joinStrings AGGREGATES: the engine assigns the fixed
                // 4000 buffer size
                case "joinstrings" -> new RelationalDataType.Varchar(4000);
                // string concatenation SUMS the operand sizes (engine
                // getSize over concat)
                case "concat", "group_concat" -> {
                    int size = 0;
                    for (var arg : f.args()) {
                        RelationalDataType at2 = infer(arg, db, ctx);
                        if (at2 instanceof RelationalDataType.Varchar v2) {
                            size += v2.size();
                        } else if (at2 instanceof RelationalDataType.Char_ c2) {
                            size += c2.size();
                        }
                    }
                    yield new RelationalDataType.Varchar(size);
                }
                // case(c1, v1, ..., else): the SAFE type over the value
                // branches (engine getSafeType lattice)
                case "case" -> {
                    RelationalDataType acc = null;
                    for (int i = 1; i < f.args().size(); i += 2) {
                        acc = safe(acc, infer(f.args().get(i), db, ctx));
                    }
                    if (f.args().size() % 2 == 1) {
                        acc = safe(acc, infer(
                                f.args().get(f.args().size() - 1), db, ctx));
                    }
                    yield acc;
                }
                // numeric operators widen positionally (engine math
                // compatibility: DOUBLE beats INT; DECIMAL beats both)
                case "plus", "minus", "times", "divide" -> {
                    RelationalDataType acc = null;
                    for (var arg : f.args()) {
                        acc = safe(acc, infer(arg, db, ctx));
                    }
                    yield acc;
                }
                default -> null;
            };
            case RelationalOperation.JoinNavigation j -> {
                if (j.terminal() == null) {
                    yield null;
                }
                // the nav's own [DB] qualifier scopes its TERMINAL (the
                // terminal ColumnRef is stored db-relative — adjudication
                // ledger cluster 8: dropping it left the column untyped
                // and concat inferred VARCHAR(n+0))
                DatabaseDefinition sub = db;
                if (j.databaseName() != null && ctx != null) {
                    var jdb = ctx.findDatabase(j.databaseName()).orElse(null);
                    if (jdb != null) {
                        sub = jdb;
                    }
                }
                yield infer(j.terminal(), sub, ctx);
            }
            case RelationalOperation.Group g -> infer(g.inner(), db, ctx);
            case RelationalOperation.Literal l -> switch (l.value()) {
                case String str ->
                        new RelationalDataType.Varchar(str.length());
                case Integer ignored -> new RelationalDataType.Integer_();
                case Long ignored -> new RelationalDataType.Integer_();
                case Double ignored -> new RelationalDataType.Double_();
                default -> null;
            };
            // boolean expressions spell BIT (engine H2 boolean SQL type)
            case RelationalOperation.Comparison ignored ->
                    new RelationalDataType.Bit();
            case RelationalOperation.BooleanOp ignored ->
                    new RelationalDataType.Bit();
            case RelationalOperation.IsNull ignored ->
                    new RelationalDataType.Bit();
            case RelationalOperation.IsNotNull ignored ->
                    new RelationalDataType.Bit();
            default -> null;
        };
    }

    /** The engine's getSafeType pair rule (typeInference.pure): equal
     * types keep; DECIMAL beats int/double/float as-is; two decimals
     * widen to DECIMAL(maxIntDigits+maxScale, maxScale); DOUBLE beats
     * integers. Null operands pass the other side through. */
    private static @com.legend.base.Nullable RelationalDataType safe(
            @com.legend.base.Nullable RelationalDataType a,
            @com.legend.base.Nullable RelationalDataType b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a.equals(b)) {
            return a;
        }
        // engine getSafeType: OTHER absorbs into the other operand
        // (getSafeType(Other, X) = X; an Other operand contributes size 0
        // to the Char/Varchar max-size rule) — ledger cluster 56
        if (a instanceof RelationalDataType.Other) {
            return b;
        }
        if (b instanceof RelationalDataType.Other) {
            return a;
        }
        Integer[] da = decimalOf(a);
        Integer[] db2 = decimalOf(b);
        if (da != null && db2 != null) {
            int intDigits = Math.max(da[0] - da[1], db2[0] - db2[1]);
            int scale = Math.max(da[1], db2[1]);
            return new RelationalDataType.Decimal(intDigits + scale, scale);
        }
        if (da != null) {
            return new RelationalDataType.Decimal(da[0], da[1]);
        }
        if (db2 != null) {
            return new RelationalDataType.Decimal(db2[0], db2[1]);
        }
        boolean aFloat = a instanceof RelationalDataType.Double_
                || a instanceof RelationalDataType.Float_
                || a instanceof RelationalDataType.Real;
        boolean bFloat = b instanceof RelationalDataType.Double_
                || b instanceof RelationalDataType.Float_
                || b instanceof RelationalDataType.Real;
        if (aFloat || bFloat) {
            return new RelationalDataType.Double_();
        }
        if (a instanceof RelationalDataType.Varchar va
                && b instanceof RelationalDataType.Varchar vb) {
            return new RelationalDataType.Varchar(
                    Math.max(va.size(), vb.size()));
        }
        int ra = INT_WIDENING.indexOf(a.getClass());
        int rb = INT_WIDENING.indexOf(b.getClass());
        if (ra >= 0 && rb >= 0) {
            return ra >= rb ? a : b;
        }
        // NO safe type (the engine's isSafeTypePossible is false: an INT
        // branch against a VARCHAR branch, a DATE against a TIMESTAMP) —
        // the engine ASSERTS there; this platform types the element as
        // nothing (Any), never as the first operand (an earlier fallback)
        return null;
    }

    /** The engine's integer widening order (safeTypeMap): a pair of integer
     *  kinds is safe at the wider. */
    private static final List<Class<? extends RelationalDataType>> INT_WIDENING = List.of(
            RelationalDataType.TinyInt.class, RelationalDataType.SmallInt.class,
            RelationalDataType.Integer_.class, RelationalDataType.BigInt.class);

    private static Integer @com.legend.base.Nullable [] decimalOf(
            RelationalDataType t) {
        if (t instanceof RelationalDataType.Decimal d) {
            return new Integer[]{d.precision(), d.scale()};
        }
        if (t instanceof RelationalDataType.Numeric n) {
            return new Integer[]{n.precision(), n.scale()};
        }
        return null;
    }

    /** The declared type of {@code table.column}: in the op's OWN database
     * when its {@code [db]} names one, else the scoping db — through the
     * one table lookup (include closure, schema-qualified spellings, exact
     * names); a VIEW's column resolves THROUGH its column expression
     * (view-on-view). The op's own name is tried first as spelled: a
     * legacy surface (the metamodel seed's input) still carries an
     * UNQUALIFIED [db], which resolves to nothing and falls to the scope. */
    private static @com.legend.base.Nullable RelationalDataType columnType(
            @com.legend.base.Nullable DatabaseDefinition scope,
            @com.legend.base.Nullable StoreLookups ctx,
            @com.legend.base.Nullable String opDb,
            String table, String column) {
        DatabaseDefinition db = scope;
        if (opDb != null && ctx != null) {
            db = ctx.findDatabase(opDb).orElse(db);
        }
        if (db == null || ctx == null) {
            return null;
        }
        String dbFqn = db.qualifiedName();
        var td = ctx.findTableDefinition(dbFqn, table);
        if (td.isPresent()) {
            for (var c : td.get().columns()) {
                if (c.name().equals(column)) {
                    return c.dataType();
                }
            }
            return null;
        }
        var view = ctx.findView(dbFqn, table);
        if (view.isPresent()) {
            for (var cm : view.get().columnMappings()) {
                if (cm.name().equals(column)) {
                    return infer(cm.expression(), db, ctx);
                }
            }
        }
        return null;
    }
}
