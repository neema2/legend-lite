// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.ExecuteChainAssembly;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.UserCallInliner;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.error.LegendCompileException;
import com.legend.error.NotImplementedException;
import com.legend.lowering.SqlPostProcessors;
import com.legend.lowering.VerdictSql;
import com.legend.sql.SqlSelect;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A FRAME's chain, planned (the planner-driver seam program's S2g, 2026-10-31; the body driver's {@code buildFrame}
 * until then): an {@code execute(f, mapping, runtime, ext)} call, or {@code executeLegendQuery}, or a plan-execute
 * peeled to its {@code executionPlan(...)} build, to the chain its readers splice and the statement that runs it --
 * the query lambda's lets folded, the EXPLICIT mapping argument attached as the chain's execution context, the
 * runtime's table renames on the planning half, the string entry's envelope and its activity's engine-style SQL.
 * Planning, no session: whether the frame RUNS at its let (the host judge's order, the engine's), its activity rows'
 * trace comment, and the planned CTE a database judge's readers reference stay the driver's -- the last because it
 * is planned through the shared preparation whose system-database route and answered arms are the driver's until S2h
 * and S2d.
 */
public final class FramePlanner {

    private FramePlanner() {
    }

    /** A frame's chain: {@code source} the execute call it was planned from (a plan-execute's build), {@code chain}
     *  what its readers splice (the string entry's envelope; the chain itself otherwise), {@code runChain} the
     *  statement that runs it (the string entry's raw chain), {@code relationRooted} whether its value is a relation
     *  (false for the string entry: one string), {@code env} the planning half with the runtime's renames. */
    public record FrameChain(TypedNativeCall source, TypedSpec chain,
            TypedSpec runChain, boolean relationRooted, boolean legendQuery, PlanEnv env) {
    }

    /** Whether the frame {@code ec} binds is RELATION-rooted (its value a relation or a TDS: one TDS in its values
     *  envelope, the fact {@link FrameChain#relationRooted} carries), decided from the query lambda's typed root
     *  without building the chain (the test-is-the-product plan's M9b audit, F5: the body-class rule asks it of every
     *  frame before any plan); null where the shape is not one {@link #chain} builds (no zero-argument lambda at the
     *  query position, a plan-execute not tracing to its build), which a rule leaves to the run's own refusal at the
     *  let's turn. The string entry is never relation-rooted (its value one string). */
    public static @Nullable Boolean relationRooted(TypedNativeCall ec, List<TypedSpec> letPrefix,
            SpecCompiler specs) {
        if (PlatformTypes.EXECUTION_PLAN_EXECUTE.equals(ec.callee().qualifiedName())) {
            TypedSpec plan = Lets.bound(ec.args().get(0), letPrefix);
            if (!(plan instanceof TypedNativeCall pb
                    && PlatformTypes.EXECUTION_PLAN.equals(pb.callee().qualifiedName()))) {
                return null;
            }
            ec = pb;
        }
        if (NativeFn.Handle.of(ec.callee().id()).orElse(null) == NativeFn.Handle.EXECUTE_LEGEND_QUERY) {
            return false;
        }
        com.legend.compiler.spec.typed.TypedLambda lam = ExecuteChainAssembly.queryLambda(ec, letPrefix, specs);
        if (lam == null || lam.body().isEmpty()) {
            return null;
        }
        Type root = lam.body().get(lam.body().size() - 1).info().type();
        return Type.isRelation(root) || PlatformTypes.isTdsType(root);
    }

    /** {@code ec}'s chain over {@code env}, the statement's preceding lets {@code letPrefix}. */
    public static FrameChain chain(TypedNativeCall ec,
            List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env) {
        // PLAN-EXECUTE normalization (burn map: unlocks the TDG wall
        // cohort + the §5 program-replayer class): executionPlan::
        // execute(plan, values, ext) peels the plan argument to its
        // executionPlan(...) BUILD — the same positional shape (query,
        // mapping, runtime, extensions), so the ordinary frame
        // machinery serves it whole. Plan TEXT is engine-text and
        // never executes here (single-compiler tenet). Non-traceable
        // plans and non-empty parametersValues WALL counted
        // (values-binding is the §5 referee-binding cut, later).
        if (PlatformTypes
                .EXECUTION_PLAN_EXECUTE.equals(
                        ec.callee().qualifiedName())) {
            TypedSpec plan = Lets.bound(ec.args().get(0), letPrefix);
            if (!(plan instanceof TypedNativeCall pb
                    && PlatformTypes
                            .EXECUTION_PLAN.equals(
                                    pb.callee().qualifiedName()))) {
                throw new NotImplementedException(
                        "plan-execute: plan argument does not trace to"
                                + " an executionPlan(...) build");
            }
            // the values through the caller's lets (a helper's
            // parametersValues parameter bound to [] at the call)
            if (ec.args().size() > 1
                    && !(Lets.bound(ec.args().get(1), letPrefix)
                            instanceof TypedCollection pv
                            && pv.elements().isEmpty())) {
                throw new NotImplementedException(
                        "plan-execute: parametersValues binding pending"
                                + " (the referee-binding cut)");
            }
            ec = pb;
        }
        // the STRING ENTRY (meta::legend::executeLegendQuery): the query
        // lambda's parameters bind from the vars pairs, the chain rides
        // the same frame, the READ is the engine's result JSON string
        // (envelope emitted over the chain — relationRooted is false:
        // the frame's value is one string). The eager run executes the
        // RAW chain (pipeline validation at the let, engine parity).
        if ((NativeFn.Handle.of(ec.callee().id()).orElse(null) == NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
            var lq = ExecuteChainAssembly
                    .prepareLegendQuery(ec, letPrefix, specs);
            var lqChain = ExecuteChainAssembly
                    .chain(lq, ec, letPrefix, specs, env.runtimeFqn(),
                            env.queryLets());
            // the activity's SQL text (the engine records the SQL it
            // generated — goldens are engine-H2-spelled): the engine-style
            // render of the same chain, absent when no render exists
            String activitySql = null;
            if (lqChain.relationRooted()
                    || lqChain.chain().info().type() instanceof Type.ClassType) {
                String mappingFqn = firstMappingFqn(lqChain.chain());
                // a RELATION-rooted chain (the relation accessor) carries no
                // mapping and renders without one (the resolver and the root
                // form both take none); a class-rooted chain needs its mapping
                if (mappingFqn != null || lqChain.relationRooted()) {
                    try {
                        // C4 (handed off): the runtime's type is not read here yet
                        activitySql = EnginePlans.engineTestSql(List.of(lqChain.chain()),
                                mappingFqn, specs, env);
                    } catch (LegendCompileException
                            | NotImplementedException
                            | IllegalStateException e) {
                        // no engine-style render for this chain shape: the
                        // activities array stays empty (a read of it is
                        // then a loud missing member, never a fabrication)
                        activitySql = null;
                    }
                }
            }
            TypedSpec envelope = ExecuteChainAssembly
                    .legendQueryEnvelope(lqChain.chain(), env.ctx(), activitySql);
            return new FrameChain(ec, envelope, lqChain.chain(), false, true, env);
        }
        var prepared = ExecuteChainAssembly
                .prepare(ec, letPrefix, specs);
        // connection POST-PROCESSOR hooks ride the runtime argument
        // (sqlQueryPostProcessorsConnectionAware): inline the runtime
        // helper, recognize the replaceTables shape, thread the rename
        // map to the lowering seam (applied over OUR SQL IR)
        if (ec.args().size() >= 3) {
            TypedSpec rtArg = Lets.bound(
                            ec.args().get(2), letPrefix);
            if (rtArg instanceof TypedUserCall) {
                rtArg = new UserCallInliner(specs)
                        .inlineBody(List.of(rtArg)).get(0);
            }
            // the frame's table renames ride the statement's union channel
            // (the chain's planning half, which the driver's frame carries); every other frame fact — CTE
            // extraction, the nonExecutable pass, the connection's time zone
            // (batch 86) — is read off the executed chain's own from
            // (executeTyped): the from is the one carrier, no static slot
            Map<String, String> tr = ExecutionContext
                    .reader()
                    .bind(v -> Lets.bound(v, letPrefix))
                    .read(Optional.empty(), rtArg).postProcessors().tableReplace();
            if (!tr.isEmpty()) {
                env = env.withTableReplace(tr);
            }
        }
        var assembled = ExecuteChainAssembly
                .chain(prepared, ec, letPrefix, specs, env.runtimeFqn(),
                        env.queryLets());
        return new FrameChain(ec, assembled.chain(), assembled.chain(), assembled.relationRooted(), false, env);
    }

    /** The first from() mapping reference in the chain (pre-order), or null. */
    private static @Nullable String firstMappingFqn(TypedSpec n) {
        if (n instanceof TypedFrom fr
                && fr.mapping().isPresent()) {
            return fr.mapping().get().fullPath();
        }
        for (TypedSpec c : n.children()) {
            String m = firstMappingFqn(c);
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    /** A frame's planned CTE under the database judge (leg 3.4 step 2, rung 12): the plan its readers reference instead
     *  of pasting the chain, the type a reader sees it at, and, for a plain class frame, its extent rows. NONE: pasted. */
    public record Cte(com.legend.sql.@Nullable SqlQuery plan,
            com.legend.compiler.element.type.@Nullable ExprType plannedInfo,
            com.legend.sql.@Nullable SqlSelect extentRows) {
        public static final Cte NONE = new Cte(null, null, null);
    }

    /** {@code fc}'s planned CTE, or {@link Cte#NONE} where it stays pasted (planned with no session: the shared
     *  preparation's system route and answered kinds are the planner's since 2026-10-31; the body driver planned it
     *  until then). */
    public static Cte cte(FrameChain fc, List<TypedSpec> letPrefix, SpecCompiler specs) {
        return cte(fc, specs, chain -> BodyPlanner.bare(chain, letPrefix, specs, fc.env()));
    }

    /** {@link #cte} with the frame's chain planned by {@code lower} (null: answered with no plan, so pasted): the
     *  walk's identity lane ({@code BodyPlanner.bare}), or the product's configuration on its program road (the
     *  test-is-the-product plan's M3g, B12 item 9: the CTE lowered once at the let). */
    public static Cte cte(FrameChain fc, SpecCompiler specs,
            Function<TypedSpec, BodyPlanner.@Nullable Bare> lower) {
        PlanEnv env = fc.env();
        // leg 3.4 step 2: a relation-rooted frame of STATIC schema that ran
        // (eager) is also PLANNED once — every assert side that reads it
        // references the plan as a CTE instead of pasting the chain
        if (fc.relationRooted()) {
            var schema = Type.schemaView(
                    fc.chain().info().type());
            if (schema != null && !schema.isLateBound() && schema.dynamicColumns().isEmpty()
                    && !schema.columns().isEmpty()) {
                BodyPlanner.Bare b = lower.apply(fc.chain());
                // the PLANNED root's type is the frame's schema as a side sees
                // it (the resolver's view: a validation's key columns are
                // typed here, not in the raw chain); a plan whose width is
                // not that schema's stays pasted
                var planned = b == null ? null
                        : Type.schemaView(b.root().info().type());
                if (b != null && planned != null && !planned.isLateBound()
                        && planned.columns().size() == b.plan().outputs().size()) {
                    // the reference keeps the CHAIN's relation carrier (a
                    // Relation<…> / TDS wrapper — the cast-to-TDS erasure and
                    // the render recognizers read the wrapper), over the
                    // PLANNED columns
                    var chainType = fc.chain().info().type();
                    return new Cte(b.plan(), chainType instanceof Type.GenericType g
                            && PlatformTypes.RELATION_CARRIERS
                                    .contains(g.rawFqn())
                            ? new ExprType(
                                    new Type.GenericType(g.rawFqn(),
                                            List.of(planned), g.multArguments()),
                                    fc.chain().info().multiplicity())
                            : b.root().info(), null);
                }
            }
            return Cte.NONE;
        }
        if (fc.chain() instanceof TypedFrom fr
                && ResultEnvelopeSplice.plainExtentRoot(fr.source())
                        instanceof TypedGetAll root
                && BodyPlanner.resolver(specs, env).plainClassPipeline(fr, root)) {
            // rung 12: a PLAIN class-rooted let (Class.all() under filters, the
            // class bound by filters over ONE table) planned ONCE — the fold's
            // root rows (its from / where over the root table's physical
            // columns) as a CTE standing for the class's mapped extent; every
            // reader ranges over that extent. The fold stays the frame's plan
            // of record (activity, the SQL-text referee). Any other chain or
            // pipeline shape stays pasted.
            BodyPlanner.Bare b = lower.apply(fc.chain());
            if (b != null) {
                SqlSelect rows = VerdictSql.classExtentRows(b.plan());
                if (rows != null) {
                    return new Cte(b.plan(), fr.info(), rows);
                }
            }
        }
        return Cte.NONE;
    }

    /**
     * The renames a statement runs under: {@code base}, widened with the table renames of every {@code execute(...)}
     * the statement REACHES inline or through ordinary lets ({@code let result = execute(...).values} is no frame,
     * batch 80) and of every frame it READS by name ({@code frames}: a frame's name to its renames); null where it
     * reads none. Conflicting renames are refused, never a silent pick (ledger cluster 59). Moved from the body
     * driver's {@code frameReplaceEnv} (the test-is-the-product plan's M3f, B12 item 8): the product's program road
     * widens a statement's environment by it too.
     */
    public static java.util.@Nullable Map<String, String> renames(TypedSpec stmt,
            Map<String, Map<String, String>> frames, Map<String, String> base,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        Map<String, String> union = null;
        Map<String, String> reached = SqlPostProcessors
                .reachableRenames(stmt, v -> Lets.bound(v, letPrefix), v -> v instanceof
                        TypedUserCall
                        ? new UserCallInliner(specs)
                                .inlineBody(List.of(v)).get(0) : v);
        if (!reached.isEmpty()) {
            union = new LinkedHashMap<>(base);
            union.putAll(reached);
        }
        for (var e : frames.entrySet()) {
            if (e.getValue().isEmpty()
                    || !UserCallInliner.referencesVar(stmt, e.getKey())) {
                continue;
            }
            if (union == null) {
                union = new LinkedHashMap<>(base);
            }
            for (var r : e.getValue().entrySet()) {
                String prev = union.putIfAbsent(r.getKey(), r.getValue());
                if (prev != null && !prev.equals(r.getValue())) {
                    throw new IllegalStateException("conflicting table"
                            + " renames for '" + r.getKey() + "': '" + prev
                            + "' vs '" + r.getValue() + "'");
                }
            }
        }
        return union;
    }
}
