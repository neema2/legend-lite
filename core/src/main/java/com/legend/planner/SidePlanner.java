// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.CanonicalRenderSql;
import com.legend.plan.ResultShape;
import com.legend.sql.JsonKeyOrder;
import com.legend.sql.SqlQuery;

import java.util.List;
import java.util.Set;

/**
 * A statement's or a verdict SIDE's plan, made with no session (the planner-driver seam program's S4a, 2026-10-31;
 * the body driver's prelude, {@code planStaged} and {@code wrapSide} until then, which opened the system route's
 * session while they planned): the body prepared under the system route it reads ({@link SystemRoutes}), lowered and
 * prepared as the judge that reads it asks, and wrapped as a side -- its shape, its shape's type and, where a canon
 * rider asks (the database judge's verdict planning), the canon columns the verdict statement composes. The route is
 * a fact of the plan: the driver opens its session and runs what runs (a prepared kind with no plan).
 */
public final class SidePlanner {

    private SidePlanner() {
    }

    /** A body prepared ({@link BodyPlanner#prepare}) under its system route: null where it reads no system store,
     *  so it runs on the body's own session. */
    public record Routed(Prepared prepared, SystemRoutes.@Nullable Route route) {
    }

    /** {@code body} prepared under the system route it reads, written in the route's catalog's dialect (B4). */
    static Routed prepare(List<TypedSpec> body, PlanEnv env) {
        SystemRoutes.Route route = SystemRoutes.of(body, env);
        PlanEnv routed = route == null ? env : SystemRoutes.onCatalog(env, route);
        return new Routed(BodyPlanner.prepare(body, routed), route);
    }

    /** A side planned before the canon wrap: a body the database queries ({@link Bare}), or a prepared kind given
     *  its answer with no plan ({@link Unplanned}: an effect, a store write, a DDL text, a handle, an effectful map),
     *  which the driver answers. */
    public sealed interface Staged {
        SystemRoutes.@Nullable Route route();
    }

    /** A prepared kind with no plan, under its route. */
    public record Unplanned(Prepared prepared, SystemRoutes.@Nullable Route route) implements Staged {
    }

    /** A side's plan, its planned root, the collection declaration (a primitive collection declared over a
     *  relation root), the planning half it was planned on, its route, and whether it reads no store. */
    public record Bare(SqlQuery plan, TypedSpec root,
            com.legend.compiler.element.type.@Nullable ExprType declaredInfo, PlanEnv env,
            SystemRoutes.@Nullable Route route, boolean storeFree) implements Staged {
    }

    /** {@code body} (a side already inlined, staged and resolved) planned: {@code identityLane} the identity lane
     *  without a rider (a rider implies it); {@code hostSide} the host judge's preparation (the identity lane's flag
     *  alone, no wire reconciliation) where the database judge's side takes the verdict lane's. */
    public static Staged staged(List<TypedSpec> body, PlanEnv env,
            com.legend.lowering.@Nullable CanonRider rider, boolean identityLane, boolean hostSide) {
        Set<String> stores = new java.util.TreeSet<>();
        for (TypedSpec n : body) {
            BodyPlanner.collectStores(n, stores);
        }
        Routed r = prepare(body, env);
        if (!(r.prepared() instanceof Prepared.Planned p)) {
            return new Unplanned(r.prepared(), r.route());
        }
        PlanEnv penv = p.env();
        SqlQuery plan = BodyPlanner.lowerAndPrepare(p.body(), penv, penv.ctx(), penv.writtenIn(),
                hostSide ? identityLane : true);
        if (rider != null && rider.canonicalJsonKeys()) {
            plan = JsonKeyOrder.sort(plan);
        }
        return new Bare(plan, p.root(), collectionDeclared(p.declaredInfo(), p.root()) ? p.declaredInfo() : null,
                penv, r.route(), stores.isEmpty());
    }

    /** Whether {@code declaredInfo} declares a primitive collection over a relation-rooted {@code root}: the side is
     *  then that collection, not the relation. */
    public static boolean collectionDeclared(
            com.legend.compiler.element.type.@Nullable ExprType declaredInfo, TypedSpec root) {
        return declaredInfo != null
                && declaredInfo.type() instanceof Type.Primitive
                && declaredInfo.multiplicity().requireBounded("result shape").isMany()
                && Type.isRelation(root.info().type());
    }

    /** A side's plan as its judge reads it: the plan (wrapped with the canon columns where a rider asks), its shape
     *  and its shape's type. */
    public record SidePlan(SqlQuery plan, ExprType shapeInfo,
            ResultShape shape) {
    }

    /** {@code plan} as a side: its shape and shape type, and, where a canon rider asks (the database judge's verdict
     *  planning), the plan wrapped with the canon columns the verdict statement composes. */
    public static SidePlan wrap(SqlQuery plan, TypedSpec root,
            com.legend.compiler.element.type.@Nullable ExprType declaredInfo,
            com.legend.lowering.@Nullable CanonRider rider, PlanEnv env) {
        ExprType shapeInfo =
                declaredInfo != null ? declaredInfo
                        : ResultShape.valueInfo(root.info());
        ResultShape shape = declaredInfo != null
                ? ResultShape.COLLECTION
                : ResultShape.of(root);
        if (rider != null && shape == ResultShape.TABULAR) {
            var schema = Type.schemaView(shapeInfo.type());
            var gw = CanonicalRenderSql.wrapTdsCanon(plan, schema);
            if (gw.declineReason() != null) {
                rider.decline(gw.declineReason());
            } else {
                rider.tdsWrap(plan.outputs().size());
                plan = gw.plan();
            }
        } else if (rider != null) {
            var w = CanonicalRenderSql.wrapWithCanon(
                    plan, shapeInfo, rider.canonicalOrder(),
                    EqualityKeys
                            .resolve(env.ctx(), shapeInfo.type()),
                    true,
                    CanonicalRenderSql.nameValued(
                            shapeInfo.type(), env.ctx()::tracksClassifier),
                    rider.enumFrame());
            if (w.declineReason() != null) {
                rider.decline(w.declineReason());
            } else {
                rider.wrap(w.kinds(), w.many(), w.literalIndex());
                plan = w.plan();
            }
        }
        return new SidePlan(plan, shapeInfo, shape);
    }
}
