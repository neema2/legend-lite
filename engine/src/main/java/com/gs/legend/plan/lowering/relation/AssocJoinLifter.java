package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.StoreResolution.JoinResolution;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedPropertyAccess;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedVariable;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Lifts association navigations from scalar lambda bodies into physical LEFT
 * JOINs on the enclosing relation.
 *
 * <p>Stage-6 wired embedded association paths through
 * {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering} using the
 * same source alias (no JOIN). Non-embedded hops require a physical JOIN — a
 * decision that can only be taken at the relation level where the
 * {@link SqlRelation} tree lives. Stage-7 introduces this pass, run by each
 * single-parameter-lambda rule (Filter / Project / GroupBy / Sort) before
 * scalar lowering:
 *
 * <ol>
 *   <li>Collect every {@link TypedPropertyAccess#associationPath()} rooted on
 *       the lambda's single parameter via {@link #collectPaths}.</li>
 *   <li>Walk each path, chaining {@link JoinResolution}s from the source
 *       store. Embedded hops keep the same alias; non-embedded hops allocate
 *       a fresh alias and install a {@code LEFT JOIN} whose {@code ON} clause
 *       is the lowered {@link JoinResolution#joinCondition}.</li>
 *   <li>Return the joined relation plus a {@code path-prefix → (alias, store)}
 *       map so {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
 *       can resolve the non-embedded hop's column on the correct alias.</li>
 * </ol>
 *
 * <p>The collector is intentionally generic — it uses the record-component
 * layout of {@link TypedSpec} subclasses rather than hard-coding a visitor,
 * so newly added HIR shapes transparently expose their property accesses.
 */
public final class AssocJoinLifter {
    private AssocJoinLifter() {}

    /** Alias + store-resolution pair bound to a fully-qualified path prefix. */
    public record Binding(String alias, StoreResolution store) {}

    /**
     * Result of lifting.
     *
     * @param relation  join-extended relation (may equal {@code source} when no lifting was needed)
     * @param bindings  prefix → binding map; {@code []} always maps to the root source
     */
    public record Lifted(SqlRelation relation, Map<List<String>, Binding> bindings) {}

    /**
     * Lifts all non-embedded association paths rooted on {@code paramName} out
     * of {@code body}. Walks each path from the source store, installing the
     * minimum set of LEFT JOINs (shared prefixes reuse aliases).
     */
    public static Lifted lift(SqlRelation source, String srcAlias, StoreResolution srcStore,
                              String paramName, TypedSpec body, LoweringContext ctx) {
        if (srcStore == null) {
            return new Lifted(source, Map.of(List.of(), new Binding(srcAlias, null)));
        }
        return liftPaths(source, srcAlias, srcStore, collectPaths(body, paramName), ctx);
    }

    /**
     * Lifts the explicit set of paths onto {@code source}. Used by rules that
     * collect paths from several lambda bodies (e.g., multi-column
     * projections) and want them to share a single JOIN chain.
     */
    public static Lifted liftPaths(SqlRelation source, String srcAlias, StoreResolution srcStore,
                                   Set<List<String>> paths, LoweringContext ctx) {
        if (srcStore == null) {
            return new Lifted(source, Map.of(List.of(), new Binding(srcAlias, null)));
        }
        Map<List<String>, Binding> bindings = new LinkedHashMap<>();
        bindings.put(List.of(), new Binding(srcAlias, srcStore));
        SqlRelation rel = source;

        // Walk shortest prefixes first so longer paths can reuse intermediate
        // bindings without duplicating JOINs.
        List<List<String>> sortedPaths = new ArrayList<>(paths);
        sortedPaths.sort((a, b) -> Integer.compare(a.size(), b.size()));

        for (List<String> fullPath : sortedPaths) {
            int hopCount = fullPath.size() - 1; // last element is the terminal property
            for (int i = 0; i < hopCount; i++) {
                List<String> prefix = List.copyOf(fullPath.subList(0, i + 1));
                if (bindings.containsKey(prefix)) continue;

                Binding parent = bindings.get(List.copyOf(fullPath.subList(0, i)));
                if (parent == null || parent.store() == null) break;

                String assoc = fullPath.get(i);
                JoinResolution jr = parent.store().joins() == null
                        ? null : parent.store().joins().get(assoc);
                if (jr == null) break; // Not an association — leave for PropertyAccessLowering to raise.

                if (jr.embedded()) {
                    bindings.put(prefix, new Binding(parent.alias(), jr.targetResolution()));
                    continue;
                }

                String joinAlias = ctx.nextAlias();
                SqlRelation target = new SqlRelation.TableRef(
                        null, jr.targetTable(), joinAlias, List.of());
                SqlExpr on = lowerJoinCondition(jr, parent.alias(), joinAlias, ctx);
                rel = new SqlRelation.Join(rel, target, SqlRelation.JoinType.LEFT, on);
                bindings.put(prefix, new Binding(joinAlias, jr.targetResolution()));
            }
        }
        return new Lifted(rel, bindings);
    }

    /**
     * Lowers a {@link JoinResolution#joinCondition} (a bare 2-param lambda
     * body) with {@code sourceParam} bound to {@code srcAlias} and
     * {@code targetParam} bound to {@code tgtAlias}, and the target store
     * installed so nested property accesses resolve.
     */
    private static SqlExpr lowerJoinCondition(JoinResolution jr, String srcAlias,
                                              String tgtAlias, LoweringContext ctx) {
        if (jr.joinCondition() == null) {
            throw new PlanGenNotPortedException(jr.joinCondition(),
                    "assoc-join:missing-condition",
                    "join to " + jr.targetTable() + " has no joinCondition");
        }
        LoweringContext inner = ctx
                .withVar(jr.sourceParam(), new SqlExpr.Identifier(srcAlias))
                .withVar(jr.targetParam(), new SqlExpr.Identifier(tgtAlias))
                .withStore(jr.targetResolution());
        return Lowerer.lowerScalar(jr.joinCondition(), inner);
    }

    // ==================== path collection ====================

    /**
     * Returns the set of {@link TypedPropertyAccess#associationPath()} values
     * rooted on {@code $paramName} anywhere inside {@code root}. Uses record
     * reflection to walk the arbitrary typed-HIR shape so new node types are
     * handled without code changes.
     */
    static Set<List<String>> collectPaths(TypedSpec root, String paramName) {
        Set<List<String>> out = new LinkedHashSet<>();
        walk(root, paramName, out);
        return out;
    }

    private static void walk(Object o, String paramName, Set<List<String>> out) {
        if (o == null) return;
        if (o instanceof TypedPropertyAccess tpa) {
            if (tpa.associationPath().isPresent()
                    && rootsOnParam(tpa, paramName)
                    && tpa.associationPath().get().size() > 1) {
                out.add(tpa.associationPath().get());
            }
            walk(tpa.source(), paramName, out);
            return;
        }
        // {@link com.gs.legend.compiler.typed.TypedStructExtract} on a
        // {@link TypedPropertyAccess} is how the compiler represents a terminal
        // hop onto a class-typed property (e.g., {@code $p.firm.legalName}).
        // {@link com.gs.legend.plan.lowering.scalar.StructLowering} rewrites
        // this at scalar-lowering time into a deeper {@code TypedPropertyAccess}
        // with the struct field appended to the association path — we mirror
        // that rewrite here so the lifter sees the same path.
        if (o instanceof com.gs.legend.compiler.typed.TypedStructExtract sx
                && sx.source() instanceof TypedPropertyAccess inner
                && rootsOnParam(inner, paramName)
                && inner.associationPath().isPresent()) {
            List<String> base = inner.associationPath().get();
            List<String> extended = new ArrayList<>(base);
            extended.add(sx.field());
            out.add(List.copyOf(extended));
            walk(inner.source(), paramName, out);
            return;
        }
        // Don't descend into lambda bodies: their $row is a different binding
        // (could shadow paramName). Embedded lambdas are handled by their own
        // relational rules.
        if (o instanceof TypedLambda) return;

        if (o instanceof List<?> list) {
            for (Object e : list) walk(e, paramName, out);
            return;
        }
        if (o instanceof Optional<?> opt) {
            opt.ifPresent(v -> walk(v, paramName, out));
            return;
        }
        if (o instanceof Map<?, ?> map) {
            for (Object v : map.values()) walk(v, paramName, out);
            return;
        }
        Class<?> c = o.getClass();
        if (c.isRecord()) {
            for (var comp : c.getRecordComponents()) {
                Class<?> ct = comp.getType();
                // Skip primitives / strings / type metadata that can't carry HIR.
                if (ct.isPrimitive() || ct == String.class || Number.class.isAssignableFrom(ct)
                        || ct == Boolean.class) continue;
                try {
                    walk(comp.getAccessor().invoke(o), paramName, out);
                } catch (IllegalAccessException | InvocationTargetException ignored) {
                    // Record accessors are public; invocation failures here would be a
                    // JVM-level invariant break — swallow to avoid derailing plan gen.
                }
            }
        }
    }

    private static boolean rootsOnParam(TypedPropertyAccess tpa, String paramName) {
        TypedSpec s = tpa.source();
        while (s instanceof TypedPropertyAccess inner) {
            s = inner.source();
        }
        return s instanceof TypedVariable tv && tv.name().equals(paramName);
    }
}
