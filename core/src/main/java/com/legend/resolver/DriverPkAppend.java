// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.model.DatabaseDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's {@code addDriverTablePkForProject} execution-context flag
 * (#45): each projection in the RESOLVED query gains the DRIVER TABLE's
 * primary-key columns — physical names, appended AFTER the user columns
 * (validation showcase golden: {@code "root".ID as "ID"}). The driver
 * table is the deepest-left scan of the projection's resolved source;
 * its PK columns still ride the materialized row (the base scan is the
 * whole table), so the append is a plain column read.
 */
public final class DriverPkAppend {

    private DriverPkAppend() {
    }

    public static List<TypedSpec> apply(List<TypedSpec> body,
            ModelContext ctx) {
        List<TypedSpec> out = new ArrayList<>(body);
        int last = out.size() - 1;
        out.set(last, appendTo(out.get(last), ctx));
        return out;
    }

    private static TypedSpec appendTo(TypedSpec n, ModelContext ctx) {
        if (n instanceof com.legend.compiler.spec.typed.TypedFrom f) {
            TypedSpec src = appendTo(f.source(), ctx);
            return f.withSource(src, src.info());
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedLet l) {
            TypedSpec v = appendTo(l.value(), ctx);
            return new com.legend.compiler.spec.typed.TypedLet(l.name(), v,
                    v.info());
        }
        if (n instanceof TypedConcatenate c) {
            TypedSpec l = appendTo(c.left(), ctx);
            TypedSpec r = appendTo(c.right(), ctx);
            return new TypedConcatenate(l, r, l.info());
        }
        if (n instanceof TypedProject p) {
            return appendPk(p, ctx);
        }
        // F4.3 (CSV differential, mechanism 1): a RENDER tail is
        // TRANSPARENT to the option — the engine applies
        // addDriverTablePkForProject to the projections regardless of the
        // text form around them. Before the platform owned toCSV the
        // harness's strip removed the tail first, so this arm never
        // existed; with the tail lowered, the append must see through it
        // (the probe's 14 validation rows: side A had the golden's ID
        // column, side B lost it here).
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall tc
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(tc.callee().qualifiedName())
                && !tc.args().isEmpty()) {
            TypedSpec src = appendTo(tc.args().get(0), ctx);
            List<TypedSpec> args = new ArrayList<>(tc.args());
            args.set(0, src);
            return new com.legend.compiler.spec.typed.TypedNativeCall(
                    tc.callee(), args, tc.info());
        }
        // the option only affects PROJECTIONS (engine: addDriverTablePk
        // ForProject) — any other statement passes through unchanged
        return n;
    }

    private static TypedSpec appendPk(TypedProject p, ModelContext ctx) {
        TypedTableReference driver = deepestLeftScan(p.source());
        if (driver == null) {
            throw new com.legend.error.NotImplementedException(
                    "addDriverTablePkForProject: no driver table scan"
                    + " under the projection source");
        }
        DatabaseDefinition.TableDefinition td = ctx.findTableDefinition(
                driver.store(), driver.table()).orElseThrow(
                        () -> new com.legend.error.NotImplementedException(
                                "addDriverTablePkForProject: table '"
                                + driver.table() + "' has no definition"));
        Type.RelationType srcRow =
                Type.requireRelationSchema(p.source().info().type());
        String rowVar = p.columns().isEmpty() ? "row"
                : p.columns().get(0).fn().parameters().get(0);
        ExprType rowInfo = new ExprType(srcRow, Multiplicity.Bounded.ONE);
        List<TypedFuncCol> cols = new ArrayList<>(p.columns());
        List<Type.Column> outCols = new ArrayList<>(
                (Type.requireRelationSchema(p.info().type())).columns());
        for (DatabaseDefinition.ColumnDefinition cd : td.columns()) {
            if (!cd.primaryKey()) {
                continue;
            }
            // engine getRootPrimaryKeyCols: a PK whose PHYSICAL column
            // NAME is already read by an existing projection is NOT
            // re-appended — the engine compares TableAliasColumn.column
            // .name regardless of which TABLE the read comes from
            // (validateComplexValidation3: 'addressId' reads addressTable
            // .ID, blocking the firmTable.ID append). At the typed level
            // a joined read carries the join's column prefix — strip the
            // source's join prefixes to recover the physical name.
            java.util.List<String> joinPrefixes = new ArrayList<>();
            collectJoinPrefixes(p.source(), joinPrefixes);
            if (p.columns().stream().anyMatch(fc -> {
                List<TypedSpec> b = fc.fn().body();
                return !b.isEmpty()
                        && unwrapErasure(b.get(b.size() - 1))
                                instanceof TypedPropertyAccess pa
                        && pa.source() instanceof TypedVariable
                        && physicalName(pa.property(), joinPrefixes)
                                .equalsIgnoreCase(cd.name());
            })) {
                continue;
            }
            Type.Column src = srcRow.columns().stream()
                    .filter(c -> c.name().equalsIgnoreCase(cd.name()))
                    .findFirst().orElseThrow(
                            () -> new com.legend.error.NotImplementedException(
                                    "addDriverTablePkForProject: PK column '"
                                    + cd.name() + "' is not on the resolved"
                                    + " projection source row"));
            TypedSpec read = new TypedPropertyAccess(
                    new TypedVariable(rowVar, rowInfo), src.name(),
                    new ExprType(src.type(), src.multiplicity()));
            // the lambda's info is its CLASSIFIER over a real {row->col}
            // signature — never the bare column type (audit R1: this was
            // the one body-typed mint the tolerant readers existed for)
            cols.add(new TypedFuncCol(cd.name(), new TypedLambda(
                    List.of(rowVar), List.of(read),
                    com.legend.compiler.element.type.ExprType.one(
                            new Type.FunctionType(
                                    List.of(new Type.Param(rowInfo.type(),
                                            rowInfo.multiplicity())),
                                    new Type.Param(src.type(),
                                            src.multiplicity()))))));
            outCols.add(new Type.Column(cd.name(), src.type(),
                    src.multiplicity()));
        }
        return new TypedProject(p.source(), cols,
                new ExprType(Type.relation(new Type.RelationType(outCols)),
                        p.info().multiplicity()));
    }

    /** Multiplicity-erasure wrappers (toOne) and conformance CASTS
     * vanish at lowering — the engine's dedup matches the underlying
     * TableAliasColumn, so a wrapped column read is still a plain
     * column read. */
    private static TypedSpec unwrapErasure(TypedSpec n) {
        while (true) {
            if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                    && nc.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())) {
                n = nc.args().get(0);
            } else if (n instanceof com.legend.compiler.spec.typed
                    .TypedCast c) {
                n = c.source();
            } else {
                return n;
            }
        }
    }

    /** Every join prefix in the resolved source (each prefixed join
     * renames its right side's physical columns {@code prefix + name}). */
    private static void collectJoinPrefixes(TypedSpec n, List<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedJoin j) {
            j.prefix().ifPresent(out::add);
        }
        for (TypedSpec c : n.children()) {
            collectJoinPrefixes(c, out);
        }
    }

    /** The physical column behind a resolved row-column name: the
     * longest matching join prefix stripped (unprefixed reads are the
     * physical name already). */
    private static String physicalName(String col, List<String> prefixes) {
        String best = "";
        for (String p : prefixes) {
            if (col.startsWith(p) && p.length() > best.length()
                    && col.length() > p.length()) {
                best = p;
            }
        }
        return col.substring(best.length());
    }

    private static @com.legend.base.Nullable TypedTableReference deepestLeftScan(TypedSpec n) {
        if (n instanceof TypedTableReference tr) {
            return tr;
        }
        List<TypedSpec> ch = n.children();
        return ch.isEmpty() ? null : deepestLeftScan(ch.get(0));
    }
}
