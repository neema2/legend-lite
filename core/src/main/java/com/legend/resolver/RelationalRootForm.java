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
        if (body.isEmpty()) {
            return body;
        }
        // the from() wrapper survives resolution (it flows through the
        // Lowerer untouched) — look through it, and keep its mapping as
        // the ~primaryKey source
        TypedSpec root = body.get(body.size() - 1);
        String mappingFqn = null;
        if (root instanceof TypedFrom fr) {
            mappingFqn = fr.mapping().map(m -> m.fullPath()).orElse(null);
            root = fr.source();
        }
        if (!(root instanceof TypedSerializeGraph g)
                || !g.nested().isEmpty() || g.bareValue()
                || !(g.source().info().type() instanceof Type.RelationType rowType)) {
            return body;
        }
        var one = Multiplicity.Bounded.ONE;
        List<TypedFuncCol> cols = new ArrayList<>();
        int i = 0;
        for (String pk : primaryKeyColumns(g, mappingFqn, ctx)) {
            Type.Column col = rowType.columns().stream()
                    .filter(c -> c.name().equals(pk))
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
        cols.addAll(g.leaves());
        List<Type.Column> outCols = new ArrayList<>();
        for (TypedFuncCol c : cols) {
            var last = c.fn().body().get(c.fn().body().size() - 1).info();
            outCols.add(new Type.Column(c.name(), last.type(),
                    last.multiplicity()));
        }
        var outInfo = new ExprType(new Type.RelationType(outCols),
                g.source().info().multiplicity());
        // Scalar projection COMMUTES with truncation: project BENEATH a
        // trailing limit/slice so the fold keeps TOP/OFFSET-FETCH in the
        // projecting select (the engine's flat form) — a MODE of this
        // pass, deliberately not general fold policy (execution isolates).
        TypedSpec proj = switch (g.source()) {
            case TypedLimit lim -> new TypedLimit(
                    new TypedProject(lim.source(), cols, outInfo),
                    lim.count(), outInfo);
            case TypedSlice sl -> new TypedSlice(
                    new TypedProject(sl.source(), cols, outInfo),
                    sl.start(), sl.stop(), outInfo);
            default -> new TypedProject(g.source(), cols, outInfo);
        };
        List<TypedSpec> out = new ArrayList<>(body);
        out.set(out.size() - 1, proj);
        return out;
    }

    /**
     * Engine resolvePrimaryKey order: the mapping's declared
     * {@code ~primaryKey} columns first; the root table's PRIMARY KEY
     * flags only when the mapping declares none.
     */
    private static List<String> primaryKeyColumns(TypedSerializeGraph g,
            String mappingFqn, ModelContext ctx) {
        if (mappingFqn != null && g.classFqn() != null) {
            var mapping = ctx.findMapping(mappingFqn).orElse(null);
            if (mapping != null) {
                for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
                    if (cb.classFqn().equals(g.classFqn())
                            && !cb.primaryKeyColumns().isEmpty()) {
                        return dedup(cb.primaryKeyColumns());
                    }
                }
            }
        }
        TypedSpec cur = g.source();
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

    private static List<String> dedup(List<String> names) {
        Set<String> seen = new LinkedHashSet<>(names);
        return new ArrayList<>(seen);
    }
}
