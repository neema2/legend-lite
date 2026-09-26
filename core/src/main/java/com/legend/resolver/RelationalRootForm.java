// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.model.MappingDefinition;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The ENGINE's flat relational form of a BARE class root — the SELECT it
 * assembles objects from host-side: primary-key columns ({@code pk_0}..)
 * plus the property leaves. Consumed ONLY by the {@code toSQLString}
 * golden-text surface (execution keeps the JSON envelope — Java
 * orchestrates, the database executes). Lives in the RESOLVER because it
 * un-builds the resolver's own {@link TypedSerializeGraph} and needs
 * mapping identity ({@code ~primaryKey}) and store (table PK) knowledge
 * (audit 19: this was driver code, and it skipped the mapping's declared
 * keys — the engine's resolvePrimaryKey consults them FIRST, table PK
 * flags only as the fallback).
 */
public final class RelationalRootForm {

    private RelationalRootForm() {
    }

    public static List<TypedSpec> apply(List<TypedSpec> body, ModelContext ctx) {
        return apply(body, ctx, null);
    }

    /** {@code mappingFqn}: the ~primaryKey source when the body carries no
     * from() wrapper (the toSQLString K-native resolves with an explicit
     * mapping argument instead). */
    public static List<TypedSpec> apply(List<TypedSpec> body, ModelContext ctx,
            @com.legend.base.Nullable String mappingFqn) {
        if (body.isEmpty()) {
            return body;
        }
        // the from() wrapper survives resolution (it flows through the
        // Lowerer untouched) — look through it, and keep its mapping as
        // the ~primaryKey source
        TypedSpec root = body.get(body.size() - 1);
        if (root instanceof TypedFrom fr) {
            if (mappingFqn == null) {
                mappingFqn = fr.mapping().map(m -> m.fullPath()).orElse(null);
            }
            root = fr.source();
        }
        if (!(root instanceof TypedSerializeGraph g)
                || !g.nested().isEmpty() || g.bareValue()) {
            return body;
        }
        Type.RelationType rowType = Type.relationSchema(g.source().info().type());
        if (rowType == null) {
            return body;
        }
        var one = Multiplicity.Bounded.ONE;
        List<TypedFuncCol> cols = new ArrayList<>();
        int i = 0;
        for (String pk : primaryKeyColumns(g, mappingFqn, ctx)) {
            Type.Column col = rowType.columns().stream()
                    .filter(c -> c.name().equals(pk)
                            || stripQ(c.name()).equals(pk))
                    .findFirst().orElse(null);
            if (col == null) {
                continue;
            }
            TypedSpec read = new TypedPropertyAccess(
                    new TypedVariable(g.rowVar(), new ExprType(rowType, one)),
                    col.name(), new ExprType(col.type(), col.multiplicity()));
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(rowType, one)),
                    new Type.Param(col.type(), col.multiplicity()));
            cols.add(new TypedFuncCol("pk_" + i++,
                    new TypedLambda(List.of(g.rowVar()), List.of(read),
                            new ExprType(fnType, one))));
        }
        // ENUM-typed leaves project the RAW source column in the flat
        // relational form — the engine's enum transform is host-side
        // object assembly, never part of this SELECT (plan goldens spell
        // "root".TYPE with the column's own dataType)
        com.legend.compiler.element.MilestoningStrategy strat =
                g.classFqn() == null ? null
                        : com.legend.compiler.element.Temporal
                                .strategyOf(ctx, g.classFqn());
        for (TypedFuncCol leaf : g.leaves()) {
            TypedFuncCol c = enumRawColumn(leaf).orElse(leaf);
            // GENERATED temporal date leaves spell the engine's alias
            // literals in the FLAT form (milestoning.pure
            // getProcessingDateAliasLiteral/getBusinessDateAliasLiteral:
            // 'k_processingDate'/'k_businessDate'); the JSON envelope
            // keeps the property name.
            if (strat != null && com.legend.compiler.element.Temporal
                    .isGeneratedDateProperty(c.name(), strat)) {
                c = new TypedFuncCol("k_" + c.name(), c.fn());
            }
            cols.add(c);
        }
        // sortBy PATH ALIASES materialize as o_<alias> sort-key columns
        // (engine buildColumnNameOutOfPath — the flat form projects the
        // sort key; column order: pk_, props, o_* last)
        TypedSpec spine = g.source();
        while (true) {
            if (spine instanceof com.legend.compiler.spec.typed.TypedSortBy sb) {
                if (sb.keyAlias() != null) {
                    cols.add(new TypedFuncCol("o_" + sb.keyAlias(), sb.key()));
                }
                spine = sb.source();
            } else if (spine instanceof TypedLimit l2) {
                spine = l2.source();
            } else if (spine instanceof TypedSlice s2) {
                spine = s2.source();
            } else {
                break;
            }
        }
        List<Type.Column> outCols = new ArrayList<>();
        for (TypedFuncCol c : cols) {
            var last = c.fn().body().get(c.fn().body().size() - 1).info();
            outCols.add(new Type.Column(c.name(), last.type(),
                    last.multiplicity()));
        }
        var outInfo = new ExprType(Type.relation(new Type.RelationType(outCols)),
                g.source().info().multiplicity());
        // Scalar projection COMMUTES with truncation: project BENEATH a
        // trailing limit/slice so the fold keeps TOP/OFFSET-FETCH in the
        // projecting select (the engine's flat form) — a MODE of this
        // pass, deliberately not general fold policy (execution isolates).
        TypedSpec proj = switch (g.source()) {
            case TypedLimit lim -> new TypedLimit(
                    new TypedProject(lim.source(), cols, outInfo, true),
                    lim.count(), outInfo);
            case TypedSlice sl -> new TypedSlice(
                    new TypedProject(sl.source(), cols, outInfo, true),
                    sl.start(), sl.stop(), outInfo);
            default -> new TypedProject(g.source(), cols, outInfo, true);
        };
        List<TypedSpec> out = new ArrayList<>(body);
        out.set(out.size() - 1, proj);
        return out;
    }

    /** An enum-decode leaf reduced to its single source-column read; empty
     * when the leaf is not enum-typed or reads several columns. */
    private static java.util.Optional<TypedFuncCol> enumRawColumn(
            TypedFuncCol leaf) {
        var body = leaf.fn().body();
        if (body.isEmpty() || !(body.get(body.size() - 1).info().type()
                instanceof Type.EnumType)) {
            return java.util.Optional.empty();
        }
        String rowVar = leaf.fn().parameters().isEmpty() ? null
                : leaf.fn().parameters().get(0);
        java.util.List<TypedPropertyAccess> reads = new ArrayList<>();
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(body);
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (t instanceof TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable v
                    && v.name().equals(rowVar)) {
                if (reads.stream().noneMatch(
                        r -> r.property().equals(pa.property()))) {
                    reads.add(pa);
                }
                continue;
            }
            work.addAll(t.children());
        }
        if (reads.size() != 1) {
            return java.util.Optional.empty();
        }
        TypedPropertyAccess read = reads.get(0);
        var fnType = new Type.FunctionType(
                java.util.List.of(new Type.Param(
                        leaf.fn().functionType().params().get(0).type(),
                        Multiplicity.Bounded.ONE)),
                new Type.Param(read.info().type(),
                        read.info().multiplicity()));
        return java.util.Optional.of(new TypedFuncCol(leaf.name(),
                new TypedLambda(leaf.fn().parameters(),
                        java.util.List.of(read),
                        new ExprType(fnType, Multiplicity.Bounded.ONE))));
    }

    /**
     * Engine resolvePrimaryKey order: the mapping's declared
     * {@code ~primaryKey} columns first; the root table's PRIMARY KEY
     * flags only when the mapping declares none.
     */
    private static List<String> primaryKeyColumns(TypedSerializeGraph g,
            @com.legend.base.Nullable String mappingFqn, ModelContext ctx) {
        return primaryKeyColumns(g.classFqn(), g.source(), mappingFqn, ctx);
    }

    /** The GraphEmission order-key entry (same rule, pre-node). */
    static List<String> primaryKeyColumns(@com.legend.base.Nullable String classFqn, TypedSpec source,
            @com.legend.base.Nullable String mappingFqn, ModelContext ctx) {
        if (mappingFqn != null && classFqn != null) {
            var mapping = ctx.findMapping(mappingFqn).orElse(null);
            if (mapping != null) {
                // through the INCLUDES (own first): a class bound by an
                // included mapping declares its ~primaryKey there, and this
                // walked the queried mapping's own bindings only — the
                // fourth "find the binding" rule of audit 2026-09-15 P2-3
                for (MappingDefinition.ClassBinding cb
                        : mapping.classBindingsWithIncludes(classFqn, ctx::findMapping)) {
                    if (!cb.primaryKeyColumns().isEmpty()) {
                        return dedup(cb.primaryKeyColumns());
                    }
                }
            }
        }
        TypedSpec cur = source;
        TypedTableReference tref = null;
        while (tref == null) {
            if (cur instanceof TypedTableReference tr) {
                tref = tr;
            } else if (cur.children().isEmpty()) {
                return List.of();
            } else {
                cur = cur.children().get(0);
            }
        }
        var td = ctx.findTableDefinition(tref.store(), tref.table())
                .orElse(null);
        if (td == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (var cd : td.columns()) {
            if (cd.primaryKey()) {
                out.add(cd.name());
            }
        }
        return out;
    }

    /** Quote-bearing row columns match their bare store pk spelling. */
    static String stripQ(String n) {
        return n.length() > 1 && n.startsWith("\"") && n.endsWith("\"")
                ? n.substring(1, n.length() - 1) : n;
    }

    private static List<String> dedup(List<String> names) {
        Set<String> seen = new LinkedHashSet<>(names);
        return new ArrayList<>(seen);
    }
}
