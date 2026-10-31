// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

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
    public record FrameChain(com.legend.compiler.spec.typed.TypedNativeCall source, TypedSpec chain,
            TypedSpec runChain, boolean relationRooted, boolean legendQuery, PlanEnv env) {
    }

    /** {@code ec}'s chain over {@code env}, the statement's preceding lets {@code letPrefix}. */
    public static FrameChain chain(com.legend.compiler.spec.typed.TypedNativeCall ec,
            java.util.List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env) {
        // PLAN-EXECUTE normalization (burn map: unlocks the TDG wall
        // cohort + the §5 program-replayer class): executionPlan::
        // execute(plan, values, ext) peels the plan argument to its
        // executionPlan(...) BUILD — the same positional shape (query,
        // mapping, runtime, extensions), so the ordinary frame
        // machinery serves it whole. Plan TEXT is engine-text and
        // never executes here (single-compiler tenet). Non-traceable
        // plans and non-empty parametersValues WALL counted
        // (values-binding is the §5 referee-binding cut, later).
        if (com.legend.compiler.element.type.PlatformTypes
                .EXECUTION_PLAN_EXECUTE.equals(
                        ec.callee().qualifiedName())) {
            TypedSpec plan = com.legend.compiler.spec.typed.Lets.bound(ec.args().get(0), letPrefix);
            if (!(plan instanceof com.legend.compiler.spec.typed
                            .TypedNativeCall pb
                    && com.legend.compiler.element.type.PlatformTypes
                            .EXECUTION_PLAN.equals(
                                    pb.callee().qualifiedName()))) {
                throw new com.legend.error.NotImplementedException(
                        "plan-execute: plan argument does not trace to"
                                + " an executionPlan(...) build");
            }
            // the values through the caller's lets (a helper's
            // parametersValues parameter bound to [] at the call)
            if (ec.args().size() > 1
                    && !(com.legend.compiler.spec.typed.Lets.bound(ec.args().get(1), letPrefix)
                            instanceof com.legend.compiler.spec.typed.TypedCollection pv
                            && pv.elements().isEmpty())) {
                throw new com.legend.error.NotImplementedException(
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
        if ((com.legend.builtin.NativeFn.Handle.of(ec.callee().id()).orElse(null) == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
            var lq = com.legend.compiler.spec.ExecuteChainAssembly
                    .prepareLegendQuery(ec, letPrefix, specs);
            var lqChain = com.legend.compiler.spec.ExecuteChainAssembly
                    .chain(lq, ec, letPrefix, specs, env.runtimeFqn(),
                            env.queryLets());
            // the activity's SQL text (the engine records the SQL it
            // generated — goldens are engine-H2-spelled): the engine-style
            // render of the same chain, absent when no render exists
            String activitySql = null;
            if (lqChain.relationRooted()
                    || lqChain.chain().info().type() instanceof com.legend.compiler
                            .element.type.Type.ClassType) {
                String mappingFqn = firstMappingFqn(lqChain.chain());
                // a RELATION-rooted chain (the relation accessor) carries no
                // mapping and renders without one (the resolver and the root
                // form both take none); a class-rooted chain needs its mapping
                if (mappingFqn != null || lqChain.relationRooted()) {
                    try {
                        // C4 (handed off): the runtime's type is not read here yet
                        activitySql = EnginePlans.engineTestSql(java.util.List.of(lqChain.chain()),
                                mappingFqn, specs, env);
                    } catch (com.legend.error.LegendCompileException
                            | com.legend.error.NotImplementedException
                            | IllegalStateException e) {
                        // no engine-style render for this chain shape: the
                        // activities array stays empty (a read of it is
                        // then a loud missing member, never a fabrication)
                        activitySql = null;
                    }
                }
            }
            TypedSpec envelope = com.legend.compiler.spec.ExecuteChainAssembly
                    .legendQueryEnvelope(lqChain.chain(), env.ctx(), activitySql);
            return new FrameChain(ec, envelope, lqChain.chain(), false, true, env);
        }
        var prepared = com.legend.compiler.spec.ExecuteChainAssembly
                .prepare(ec, letPrefix, specs);
        // connection POST-PROCESSOR hooks ride the runtime argument
        // (sqlQueryPostProcessorsConnectionAware): inline the runtime
        // helper, recognize the replaceTables shape, thread the rename
        // map to the lowering seam (applied over OUR SQL IR)
        if (ec.args().size() >= 3) {
            TypedSpec rtArg = com.legend.compiler.spec.typed.Lets.bound(
                            ec.args().get(2), letPrefix);
            if (rtArg instanceof com.legend.compiler.spec.typed.TypedUserCall) {
                rtArg = new com.legend.compiler.spec.UserCallInliner(specs)
                        .inlineBody(java.util.List.of(rtArg)).get(0);
            }
            // the frame's table renames ride the statement's union channel
            // (the chain's planning half, which the driver's frame carries); every other frame fact — CTE
            // extraction, the nonExecutable pass, the connection's time zone
            // (batch 86) — is read off the executed chain's own from
            // (executeTyped): the from is the one carrier, no static slot
            java.util.Map<String, String> tr = com.legend.compiler.spec.typed.ExecutionContext
                    .reader()
                    .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                    .read(java.util.Optional.empty(), rtArg).postProcessors().tableReplace();
            if (!tr.isEmpty()) {
                env = env.withTableReplace(tr);
            }
        }
        var assembled = com.legend.compiler.spec.ExecuteChainAssembly
                .chain(prepared, ec, letPrefix, specs, env.runtimeFqn(),
                        env.queryLets());
        return new FrameChain(ec, assembled.chain(), assembled.chain(), assembled.relationRooted(), false, env);
    }

    /** The first from() mapping reference in the chain (pre-order), or null. */
    private static @com.legend.base.Nullable String firstMappingFqn(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedFrom fr
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
}
