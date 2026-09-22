// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.model.MappingDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The AGGREGATION-AWARE rewrite decision — the engine's
 * {@code meta::pure::mapping::aggregationAware} routing (aggregationAware
 * .pure: potentiallyRewriteObjectGroupBy / canRewrite / doRewrite) over
 * the typed query and the compiled mapping's view FACTS
 * ({@link MappingDefinition.ClassBinding.AggregateViewFacts}):
 *
 * <ul>
 * <li>only a {@code groupBy} whose collection is a (filtered) getAll is a
 * rewrite candidate (getSupportedTopLevelCollectionOperations);</li>
 * <li>every expression — the query's group keys, aggregate map/reduce
 * functions and filter predicates, and the view's specification lambdas —
 * canonicalizes to a PROJECT PATH ({@code generateProjectPath}: property
 * steps over the class root, no-op functions elided, automap folded, other
 * functions by name);</li>
 * <li>a query expression {@code canRewrite} against a view when its path is
 * one of the view's group-by paths or every sub-expression can (a property
 * read off a grouped object, a function over rewritable arguments, a
 * literal);</li>
 * <li>the FIRST view (declaration order) whose group-by and aggregate
 * matches hold wins; a view with {@code canAggregate=false} demands that
 * the query's aggregate paths cover the view's group-by paths and the
 * view's aggregates cover the query's.</li>
 * </ul>
 *
 * The decision is a pure function of (query, mapping): the resolver
 * re-roots the class source at the chosen set, the activity row records
 * the same choice.
 */
public final class AggregationAwareRouting {

    private AggregationAwareRouting() {
    }

    private static final String ROOT = "this";

    private static final Set<String> NO_OP_FNS = Set.of(
            "meta::pure::functions::multiplicity::toOne",
            "meta::pure::functions::multiplicity::toOneMany",
            "meta::pure::functions::collection::first",
            "meta::pure::functions::lang::cast",
            com.legend.builtin.NativeFn.SubtypeForm.SUB_TYPE.fqn(),
            com.legend.builtin.NativeFn.SubtypeForm.WHEN_SUB_TYPE.fqn());

    private static final Set<String> MAP_FNS = Set.of(
            "meta::pure::functions::collection::map",
            "meta::pure::functions::collection::exists");

    /** The aggregate view set id the query rewrites to, or null when the
     * root class is not aggregation-aware-mapped, the query is not a
     * rewritable groupBy, or no view matches (the main set serves). */
    public static @com.legend.base.Nullable String chooseSet(ModelContext ctx, SpecCompiler specs,
            String mappingFqn, TypedGetAll root, List<TypedSpec> ops, TypedSpec top) {
        List<MappingDefinition.ClassBinding.AggregateViewFacts> views =
                viewsOf(ctx, mappingFqn, root.classFqn());
        if (views.isEmpty() || !(top instanceof TypedGroupBy gb)) {
            return null;
        }
        // the collection: getAll + filters only (engine
        // getSupportedTopLevelCollectionOperations)
        List<TypedLambda> filters = new ArrayList<>();
        for (TypedSpec op : ops) {
            if (!(op instanceof TypedFilter f)) {
                return null;
            }
            filters.add(f.predicate());
        }
        Type.ClassType rootType = new Type.ClassType(root.classFqn());
        List<TypedLambda> keyFns = new ArrayList<>();
        for (TypedGroupBy.GroupKey k : gb.keys()) {
            if (k.fn().isEmpty()) {
                return null;
            }
            keyFns.add(k.fn().get());
        }
        List<String[]> aggPaths = new ArrayList<>();   // [mapPath, reducePath]
        for (TypedAggCol a : gb.aggs()) {
            String mapPath = lambdaPath(a.map(), ROOT);
            aggPaths.add(new String[]{mapPath, lambdaPath(a.reduce(), mapPath)});
        }
        for (MappingDefinition.ClassBinding.AggregateViewFacts view : views) {
            List<String> specGroupBy = new ArrayList<>();
            for (var g : view.groupByFunctions()) {
                specGroupBy.add(path(specs.typeExpression(g,
                        Map.of(ROOT, ExprType.one(rootType))), Map.of(ROOT, ROOT)));
            }
            List<String[]> specAggs = new ArrayList<>();
            for (var av : view.aggregateValues()) {
                TypedSpec mapT = specs.typeExpression(av.mapFn(),
                        Map.of(ROOT, ExprType.one(rootType)));
                String mapPath = path(mapT, Map.of(ROOT, ROOT));
                TypedSpec aggT = specs.typeExpression(av.aggregateFn(),
                        Map.of("mapped", new ExprType(mapT.info().type(),
                                Multiplicity.Bounded.ZERO_MANY)));
                specAggs.add(new String[]{mapPath, path(aggT, Map.of("mapped", mapPath))});
            }
            boolean collectionOk = true;
            for (TypedLambda f : filters) {
                collectionOk &= canRewrite(f.body().get(0),
                        Map.of(f.parameters().get(0), ROOT), specGroupBy);
            }
            if (!collectionOk) {
                continue;
            }
            boolean groupByMatch = true;
            for (TypedLambda l : keyFns) {
                groupByMatch &= canRewrite(l.body().get(0),
                        Map.of(l.parameters().get(0), ROOT), specGroupBy);
            }
            List<String> queryAggMapPaths = aggPaths.stream().map(p -> p[0]).toList();
            groupByMatch &= view.canAggregate() || queryAggMapPaths.containsAll(specGroupBy);
            boolean aggValueMatch = true;
            for (int i = 0; i < gb.aggs().size(); i++) {
                TypedAggCol a = gb.aggs().get(i);
                String[] q = aggPaths.get(i);
                boolean fromGroupByOnly = canRewrite(a.map().body().get(0),
                        Map.of(a.map().parameters().get(0), ROOT), specGroupBy);
                boolean any = fromGroupByOnly;
                for (String[] sp : specAggs) {
                    any |= sp[0].equals(q[0]) && sp[1].equals(q[1]);
                }
                aggValueMatch &= any;
            }
            List<String> specAggMapPaths = specAggs.stream().map(p -> p[0]).toList();
            aggValueMatch &= view.canAggregate() || specAggMapPaths.containsAll(queryAggMapPaths);
            if (groupByMatch && aggValueMatch) {
                return view.setId();
            }
        }
        return null;
    }

    /** The AggregationAware main set's id for a class in a mapping (its
     * includes closed), or null when the class is not so mapped. */
    public static @com.legend.base.Nullable String mainSetId(ModelContext ctx, String mappingFqn,
            String classFqn) {
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return null;
        }
        for (var cb : md.classBindingsWithIncludes(ctx::findMapping)) {
            if (cb.classFqn().equals(classFqn)
                    && cb instanceof MappingDefinition.ClassBinding.Relational rb
                    && rb.source() instanceof MappingDefinition.RelationalSource.Table t
                    && t.aggregationAwareMain()) {
                return cb.setId();
            }
        }
        return null;
    }

    /** The main binding's view facts for a class in a mapping (its
     * includes closed), or empty. */
    public static List<MappingDefinition.ClassBinding.AggregateViewFacts> viewsOf(
            ModelContext ctx, String mappingFqn, String classFqn) {
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return List.of();
        }
        for (var cb : md.classBindingsWithIncludes(ctx::findMapping)) {
            if (cb.classFqn().equals(classFqn)
                    && cb instanceof MappingDefinition.ClassBinding.Relational rb
                    && !rb.aggregateViews().isEmpty()) {
                return rb.aggregateViews();
            }
        }
        return List.of();
    }

    private static String lambdaPath(TypedLambda l, String paramPath) {
        Map<String, String> env = new HashMap<>();
        if (!l.parameters().isEmpty()) {
            env.put(l.parameters().get(0), paramPath);
        }
        return path(l.body().get(l.body().size() - 1), env);
    }

    /** generateProjectPath: the canonical path text of a typed expression
     * under a variable-to-path environment. */
    static String path(TypedSpec n, Map<String, String> env) {
        return switch (n) {
            case TypedVariable v -> env.getOrDefault(v.name(), "Var->" + v.name());
            case TypedPropertyAccess pa -> "Property->" + pa.property()
                    + "(" + path(pa.source(), env) + ")";
            // a milestoned qualified property ($x.description(date)): the
            // engine's QualifiedProperty arm paths the property over its
            // source (the date arguments are not part of the path)
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    "Property->" + ma.property() + "(" + path(ma.source(), env) + ")";
            case TypedCast c -> path(c.source(), env);
            case TypedMap m -> mapPath(m.source(), m.mapper(), env);
            case TypedNativeCall nc -> {
                String fqn = nc.callee().qualifiedName();
                if (NO_OP_FNS.contains(fqn) && !nc.args().isEmpty()) {
                    yield path(nc.args().get(0), env);
                }
                if (MAP_FNS.contains(fqn) && nc.args().size() == 2
                        && nc.args().get(1) instanceof TypedLambda l) {
                    yield mapPath(nc.args().get(0), l, env);
                }
                yield "NativeFunction->" + fqn + "(" + args(nc.args(), env) + ")";
            }
            case TypedUserCall uc -> "Function->" + uc.callee().qualifiedName()
                    + "(" + args(uc.args(), env) + ")";
            case TypedLambda l -> path(l.body().get(l.body().size() - 1), env);
            case TypedCString s -> "InstanceValue->String->" + s.value();
            case TypedCInteger i -> "InstanceValue->Integer->" + i.value();
            case TypedCFloat f -> "InstanceValue->Float->" + f.value();
            case TypedCBoolean b -> "InstanceValue->Boolean->" + b.value();
            case TypedCollection c -> "InstanceValue(" + args(c.elements(), env) + ")";
            // an unmatched kind is a routing shape this walk does not know —
            // loud, never a plausible path (charter C2.4)
            default -> throw new com.legend.error.NotImplementedException(
                    "aggregation-aware project path for "
                    + n.getClass().getSimpleName() + " pending");
        };
    }

    private static String mapPath(TypedSpec source, TypedLambda mapper, Map<String, String> env) {
        Map<String, String> inner = new HashMap<>(env);
        if (!mapper.parameters().isEmpty()) {
            inner.put(mapper.parameters().get(0), path(source, env));
        }
        return path(mapper.body().get(mapper.body().size() - 1), inner);
    }

    private static String args(List<TypedSpec> args, Map<String, String> env) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(path(args.get(i), env));
        }
        return sb.toString();
    }

    /** canRewrite: the expression's path is a group-by path, or every
     * sub-expression can rewrite (a read off a grouped object, a function
     * over rewritable arguments, a literal, the root itself never). */
    static boolean canRewrite(TypedSpec n, Map<String, String> env, List<String> groupByPaths) {
        if (groupByPaths.contains(path(n, env))) {
            return true;
        }
        return switch (n) {
            case TypedVariable v -> !env.containsKey(v.name());   // an open var; the root is not a path
            case TypedPropertyAccess pa -> canRewrite(pa.source(), env, groupByPaths);
            // QualifiedProperty arm: the SOURCE decides
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    canRewrite(ma.source(), env, groupByPaths);
            case TypedCast c -> canRewrite(c.source(), env, groupByPaths);
            case TypedMap m -> {
                Map<String, String> inner = new HashMap<>(env);
                if (!m.mapper().parameters().isEmpty()) {
                    inner.put(m.mapper().parameters().get(0), path(m.source(), env));
                }
                yield canRewrite(m.source(), env, groupByPaths)
                        || canRewrite(m.mapper().body().get(0), inner, groupByPaths);
            }
            case TypedNativeCall nc -> {
                String fqn = nc.callee().qualifiedName();
                if (MAP_FNS.contains(fqn) && nc.args().size() == 2
                        && nc.args().get(1) instanceof TypedLambda l) {
                    Map<String, String> inner = new HashMap<>(env);
                    if (!l.parameters().isEmpty()) {
                        inner.put(l.parameters().get(0), path(nc.args().get(0), env));
                    }
                    yield canRewrite(nc.args().get(0), env, groupByPaths)
                            || canRewrite(l.body().get(0), inner, groupByPaths);
                }
                boolean all = true;
                for (TypedSpec a : nc.args()) {
                    all &= canRewrite(a, env, groupByPaths);
                }
                yield all;
            }
            case TypedUserCall uc -> {
                boolean all = true;
                for (TypedSpec a : uc.args()) {
                    all &= canRewrite(a, env, groupByPaths);
                }
                yield all;
            }
            case TypedLambda l -> canRewrite(l.body().get(l.body().size() - 1), env, groupByPaths);
            case TypedCString ignored -> true;
            case TypedCInteger ignored -> true;
            case TypedCFloat ignored -> true;
            case TypedCBoolean ignored -> true;
            case TypedCollection c -> {
                boolean all = true;
                for (TypedSpec e : c.elements()) {
                    all &= canRewrite(e, env, groupByPaths);
                }
                yield all;
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "aggregation-aware canRewrite over "
                    + n.getClass().getSimpleName() + " pending");
        };
    }
}
