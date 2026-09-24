// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.lowering.Resolvers.ColumnResolver;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Lower {@code <instances>->project(~[alias: x|$x.path…])}: one SELECT per
 * instance (UNION ALL for a collection). A colspec whose path crosses a
 * TO-MANY property becomes a {@code LEFT JOIN LATERAL
 * (SELECT unnest(<array literal>) AS elem)} off a one-row anchor — LEFT
 * so an empty array yields NULL, lateral chaining so independent arrays
 * CROSS-multiply (real pure's project semantics over instances).
 * (Extracted whole from {@link Lowerer} at the file-size guardrail;
 * the driver supplies its scalar recursion and alias mint as handles.)
 */
final class InstanceProjection {

    private InstanceProjection() {
    }

    static SqlSelect lower(TypedProject p, List<OutputCol> outputs,
            BiFunction<TypedSpec, ColumnResolver, SqlExpr> scalar,
            ColumnResolver noScope, Supplier<String> fresh,
            java.util.function.Function<com.legend.compiler.element.type.Type,
                    SqlType> sqlTypeOf) {
        List<TypedNewInstance> instances =
                p.source() instanceof TypedCollection c
                        ? c.elements().stream()
                                .map(e -> (TypedNewInstance) e)
                                .toList()
                        : List.of((TypedNewInstance) p.source());
        List<SqlQuery> branches = new ArrayList<>(instances.size());
        for (var inst : instances) {
            branches.add(instanceSelect(inst, p.columns(), outputs,
                    scalar, noScope, fresh, sqlTypeOf));
        }
        if (branches.size() == 1) {
            return (SqlSelect) branches.get(0);
        }
        return SqlSelect.starOf(new SqlSource.Subselect(
                new SqlUnion(branches, true, outputs), fresh.get(), null));
    }

    private static SqlSelect instanceSelect(TypedNewInstance inst,
            List<TypedFuncCol> columns, List<OutputCol> outputs,
            BiFunction<TypedSpec, ColumnResolver, SqlExpr> scalar,
            ColumnResolver noScope, Supplier<String> fresh,
            java.util.function.Function<com.legend.compiler.element.type.Type,
                    SqlType> sqlTypeOf) {
        SqlSource src = null;
        // ONE unnest per to-many PATH PREFIX (the NavPath-registry rule):
        // two colspecs over $x.addresses iterate the SAME collection — real
        // pure yields (city, zip) pairs, never their cross product. Only
        // INDEPENDENT collections cross-multiply.
        Map<String, Unnest> unnestByPrefix = new LinkedHashMap<>();
        List<SqlSelect.Projection> ps = new ArrayList<>(columns.size());
        for (TypedFuncCol col : columns) {
            List<Seg> path = pathOf(col);
            if (path == null) {
                // COMPUTED column ($v.a + $v.b, coalesce($x.f, ...)): the row
                // param's property accesses resolve to the instance's literal
                // values; the body lowers as an ordinary scalar. A MANY-valued
                // body has no relational cell shape (bare to-many paths
                // explode via unnest; a computed one must be loud, never a
                // silent list-in-a-cell).
                TypedSpec bodyLast = LambdaBinding.last(col.fn());
                if (bodyLast.info().multiplicity().isMany()) {
                    throw new NotImplementedException(
                            "instance-literal project: computed column '" + col.name()
                                    + "' is collection-valued — only bare to-many"
                                    + " property paths explode");
                }
                String param = col.fn().parameters().get(0);
                SqlExpr computed = scalar.apply(LambdaBinding.last(col.fn()), (v, name) -> {
                    if (!param.equals(v)) {
                        throw new IllegalStateException("instance-literal project:"
                                + " unresolved variable $" + v);
                    }
                    TypedSpec pv = inst.properties().get(name);
                    return pv == null ? new SqlExpr.NullLit()
                            : scalar.apply(pv, noScope);
                });
                ps.add(new SqlSelect.Projection(computed, col.name(),
                        Fold.named(outputs, col.name())));
                continue;
            }
            // Walk the path over the literal: to-one instance hops recurse;
            // the first TO-MANY value becomes the unnest source.
            TypedSpec cur = inst;
            SqlExpr value = null;
            for (int i = 0; i < path.size(); i++) {
                if (!(cur instanceof TypedNewInstance ni)) {
                    throw new NotImplementedException(
                            "instance-literal project: '" + col.name()
                                    + "' navigates through a non-instance value");
                }
                TypedSpec v = ni.properties().get(path.get(i).name());
                if (v == null) {
                    value = new SqlExpr.NullLit();   // unset property: NULL column
                    break;
                }
                if (v instanceof TypedCollection many) {
                    // TO-MANY: explode via lateral unnest (shared per path
                    // prefix); the residual path reads fields off the element.
                    String prefix = path.subList(0, i + 1).stream()
                            .map(Seg::name)
                            .collect(java.util.stream.Collectors.joining("."));
                    Unnest un = unnestByPrefix.get(prefix);
                    if (un == null) {
                        SqlExpr array = many.elements().isEmpty()
                                ? new SqlExpr.NullLit()
                                : new SqlExpr.ArrayLit(many.elements().stream()
                                        .map(e -> scalar.apply(e, noScope)).toList());
                        // the element type is the ARRAY's own (§4bZ-U:
                        // instance elements are structs — the old
                        // hardcoded VARCHAR left every elem read blind);
                        // an EMPTY collection takes the segment's
                        // DECLARED type from the colspec BODY (the
                        // walked Seg — an empty literal itself types
                        // Nil, which re-guessed VARCHAR:
                        // testSimpleProject's empty `values` side, the
                        // last §4bZ-U pct row)
                        SqlType elemT = array.type()
                                instanceof com.legend.sql.TypeFact.Typed t
                                && t.type() instanceof SqlType.Array at
                                ? at.element()
                                : sqlTypeOf.apply(path.get(i).type());
                        un = new Unnest(fresh.get(), elemT);
                        unnestByPrefix.put(prefix, un);
                        SqlSource right = Fold.lateralElem(array,
                                elemT, fresh.get(), un.alias());
                        src = src == null
                                ? anchorJoin(right, fresh)
                                : new SqlSource.Join(src, right,
                                        SqlSource.Join.Kind.LEFT_LATERAL,
                                        new SqlExpr.BoolLit(true));
                    }
                    // §E3: LEFT_LATERAL pads with NULL on an empty
                    // array — nullable slot
                    value = SqlExpr.Column.of(un.alias(), "elem",
                            un.elemT(), true, com.legend.sql.OutputCol.Origin.DERIVED);
                    for (int r = i + 1; r < path.size(); r++) {
                        value = new SqlExpr.StructGet(value, path.get(r).name());
                    }
                    break;
                }
                if (i == path.size() - 1) {
                    value = scalar.apply(v, noScope);
                } else {
                    cur = v;
                }
            }
            ps.add(new SqlSelect.Projection(
                    Objects.requireNonNull(value, "value"), col.name(),
                    Fold.named(outputs, col.name())));
        }
        return new SqlSelect(ps, false,
                src == null ? new SqlSource.Dual() : src, null, List.of(),
                null, null, List.of(), null, null, List.of());
    }

    /** The 1-row anchor a lateral chain hangs off (an empty array must NULL, not kill, the row). */
    private static SqlSource anchorJoin(SqlSource right, Supplier<String> fresh) {
        SqlSource anchor = new SqlSource.Values(
                List.of(List.of(new SqlExpr.IntLit(1))), List.of("_anchor"), fresh.get(),
                List.of(new OutputCol("_anchor", SqlType.Scalar.BIGINT, false)));
        return new SqlSource.Join(anchor, right,
                SqlSource.Join.Kind.LEFT_LATERAL, new SqlExpr.BoolLit(true));
    }

    /** A colspec body as a bare property path (null = computed). A
     * path IS a chain of auto-maps: plain access OR single-hop map
     * node (ValueCollections.autoMapHop). */
    /** One path segment: the property name AND its DECLARED pure type,
     * read off the colspec BODY's own node info ({@code $x.values}
     * types as the property's element class). ONE walk carries both —
     * the instance-value walk cannot supply the type: an EMPTY
     * collection literal types Nil, which VARCHAR-guessed the lateral
     * element (the last §4bZ-U pct row, testSimpleProject's empty
     * side; a first cut as a SECOND parallel walk was the audit's
     * two-owners smell). */
    private record Seg(String name,
            com.legend.compiler.element.type.Type type) {
    }

    /** A shared per-prefix lateral unnest: its alias and the unnested
     * element's SQL type (the elem-read stamp) — one registry entry,
     * minted once. */
    private record Unnest(String alias, SqlType elemT) {
    }

    private static @com.legend.Nullable List<Seg> pathOf(TypedFuncCol col) {
        String param = col.fn().parameters().get(0);
        ArrayDeque<Seg> path = new ArrayDeque<>();
        TypedSpec cur = col.fn().body().get(col.fn().body().size() - 1);
        while (true) {
            if (cur instanceof TypedPropertyAccess pa) {
                path.addFirst(new Seg(pa.property(), pa.info().type()));
                cur = pa.source();
            } else if (ValueCollections.autoMapHop(cur) instanceof String hop) {
                path.addFirst(new Seg(hop, cur.info().type()));
                cur = ((TypedMap) cur).source();
            } else break;
        }
        if (!(cur instanceof TypedVariable v) || !v.name().equals(param) || path.isEmpty()) {
            return null;   // COMPUTED body — the caller lowers it as a scalar
        }
        return List.copyOf(path);
    }
}
