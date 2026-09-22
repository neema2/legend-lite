// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.exec.ExecutionResult;
import com.legend.exec.VerdictBatch;
import java.util.List;
import java.util.Map;

/**
 * THE BLOCK COMPILER, stages 1–4 (2026-09-21/22; docs/BLOCK_COMPILER_HOMEWORK_2026_09_21.md
 * §13, §18–§19). EVERY test body — lets, assert-family roots (a verdict call, a
 * quantified map / forAll, an if over asserts), helper calls, value statements, and
 * effect statements as scripts — is compiled to its ARTIFACT before anything of it runs: the frames (CTE
 * definitions) and the verdict rows on the batch, one fused statement per
 * connection, the appeals attached; the value statements PREPARED (helper inlining,
 * native staging, store resolution — the executor's own compile phases, shared:
 * {@link StatementExecutor#prepareValue}); the FRAGMENT MAP naming the let / assert
 * every frame and verdict branch came from. {@link #run} only sends it. The walk is
 * the executor's own arms in the executor's own order, so the artifact is
 * byte-identical to what the statement-by-statement loop produced before stage 4
 * deleted it (the ladder pins it); nothing is planned once running has begun.
 *
 * <p>Provisioning precedes planning: a statement's execution contexts are
 * established (the seeding boundary: a runtime's declared setups, a from()'s inline
 * data) before its frame is planned, because a frame's reported wire types are read
 * from the seeded tables. That is seeding, not evaluation.
 *
 * <p>Measured before this class existed: the arms claim EVERY assert-family root in
 * the corpus (a verdict, a deferred row, or a raise — never a fall-through to host
 * evaluation; 0 unclaimed on both lanes), so {@link #compile} has no host arm and
 * walls loudly if one were ever needed.
 */
public final class BodyCompiler {
    private BodyCompiler() {
    }

    /** THE SEGMENT WALK (stages 1–4): one pass over the body. Lets, asserts and value
     * statements accumulate into the open VERDICTS segment (frames and rows on its
     * batch, values prepared); an effect statement closes it — its values run in order,
     * its batch flushes as one fused statement — and is COLLECTED into the open EFFECT
     * segment through the arms' own send (an EffectSink on the environment); the next
     * non-effect statement sends that segment as ONE script first. A verdicts segment is
     * planned only after the effects before it have run: two facts are read from the
     * live session at plan time (a frame's wire types after seeding, a raw read's
     * schema — homework §19). A test-data generator's fold reads the session too, so the
     * pending script is sent before it folds. The body's value is its last statement's. */
    static @com.legend.base.Nullable ExecutionResult execute(List<TypedSpec> stmts, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env0) {
        Segments seg = new Segments(env0, specs);
        Map<String, StatementExecutor.ExecFrame> execFrames = new java.util.LinkedHashMap<>();
        Map<String, Boolean> effectMemo = new java.util.HashMap<>();
        for (int i = 0; i < stmts.size(); i++) {
            boolean effect = StatementExecutor.containsEffect(stmts.get(i), specs, effectMemo);
            boolean generator = Compiler.containsTdgGenerator(stmts.get(i));
            if (generator) {
                seg.closeEffects();     // the fold reads the state the pending script creates
                seg.closeVerdicts();    // the loop's own order: verdicts flush before a generator
            }
            // TDG lane S1: the checker's census CARRIER folds to instance literals
            // before the statement is planned (orchestration owns testdatagen)
            TypedSpec stmt = com.legend.testdatagen.TestDataGenerationNatives.foldCensus(
                    stmts.get(i), seg.env().ctx(), seg.env().connection(), letPrefix, StatementExecutor.ENGINE_TEXT);
            StatementExecutor.establishContexts(stmt, seg.env());
            boolean last = i == stmts.size() - 1;
            if (effect || StatementExecutor.containsEffect(stmt, specs, effectMemo)) {
                seg.closeVerdicts();
                seg.collectEffect(stmt, stmts, i, letPrefix, execFrames);
                continue;
            }
            seg.closeEffects();
            if (stmt instanceof TypedLet let && !last) {
                StatementExecutor.ExecFrame alias = StatementExecutor.aliasFrame(let.value(), execFrames);
                if (alias != null) {
                    execFrames.put(let.name(), alias);
                    continue;
                }
                TypedSpec rhs = let.value();
                while (rhs instanceof com.legend.compiler.spec.typed.TypedFrom rf) {
                    rhs = rf.source();
                }
                if (rhs instanceof TypedNativeCall ec
                        && (com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().qualifiedName())
                            || com.legend.builtin.NativeFn.Handle.of(ec.callee().qualifiedName()).orElse(null)
                                    == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
                    // a FRAME: planned here, its CTE defined on the batch when a reader
                    // splices it (rung 12: a plain class frame as its root rows)
                    execFrames.put(let.name(),
                            StatementExecutor.buildFrame(ec, letPrefix, true, specs, seg.env()));
                    seg.fragments.put("frame_" + let.name(), "let " + let.name() + " (statement " + (i + 1) + ")");
                    continue;
                }
                // a RAW READ bound by a let (let r = executeInDb('select …', $c)): the engine
                // runs it AT the let; here its schema is pinned at the let when a later
                // statement demands it (columnNames / values) — before any later effect
                // changes the state it read (the temp-table port, 2026-09-22: the probe
                // used to run at the verdict flush, after the drop). The data read stays
                // late-bound; the single-query rule of RawGridSchema.stamp decides.
                if (containsRawGrid(let.value())) {
                    TypedSpec stampedLet = com.legend.resolver.RawGridSchema.stamp(
                            stmts.subList(i, stmts.size()),
                            StatementExecutor.gridOracle(seg.env().connection(), seg.env())).get(0);
                    if (stampedLet instanceof TypedLet sl) {
                        let = sl;
                        rhs = let.value();
                    }
                }
                // a HANDLE or a value binding: rows under its scope, the let rides the prefix
                PlanAllocations.registerHandlesIn(let.name(), rhs, letPrefix, specs, seg.env());
                letPrefix.add(let);
                continue;
            }
            // a statement root (a trailing let IS its value): the arms claim an
            // assert-family root — a verdict call, a quantified map / forAll, an if
            // over asserts — into deferred rows …
            TypedSpec bare = com.legend.compiler.spec.typed.Lets.bare(stmt);
            int rowsBefore = seg.rows();
            ExecutionResult v = AssertVerdicts.tryAdjudicate(bare, letPrefix, specs,
                    StatementExecutor.frameReplaceEnv(stmt, execFrames, seg.env(), letPrefix, specs),
                    StatementExecutor.spliceHook(execFrames, letPrefix, specs, seg.env()));
            if (v != null) {
                if (seg.batch != null) {
                    nameRows(seg.fragments, seg.batch, rowsBefore, com.legend.compiler.spec.typed.Calls.calleeOf(bare), i + 1);
                }
                seg.verdict(v);
                continue;
            }
            // … everything else is a VALUE statement, prepared now (a helper call
            // inlines here; an inlined assert root is adjudicated by the preparation)
            StatementExecutor.PreparedValue pv = StatementExecutor.prepareValue(
                    stmt, bare, letPrefix, execFrames, specs, seg.env());
            // a CONTEXT OWNER (assertError: f's body runs under the arm's catch) and a
            // frame FORCED at value position (execute as a statement: its eager run IS
            // the value) are values like any other — prepared here, run at the segment's
            // close in walk order, through the arms the loop ran them through (stage 4)
            if (pv.verdict() != null && seg.batch != null) {
                nameRows(seg.fragments, seg.batch, rowsBefore, com.legend.compiler.spec.typed.Calls.calleeOf(bare), i + 1);
            }
            seg.value(pv);
        }
        seg.closeEffects();
        seg.closeVerdicts();
        return seg.last;
    }

    /** Whether the tree reads a late-bound raw grid anywhere. */
    private static boolean containsRawGrid(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedRawSqlRelation) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsRawGrid(c)) {
                return true;
            }
        }
        return false;
    }

    /** The segments of one body: the open verdicts segment (its batch, its prepared
     * values), the open effect segment (its sink), the fragment map, the last result. */
    private static final class Segments {
        private final StatementExecutor.ExecEnv env0;
        private final SpecCompiler specs;
        final Map<String, String> fragments = new java.util.LinkedHashMap<>();
        /** The database judge's batch; null under the host judge, which has no deferral:
         * its arm executes and compares at the assert, its value statements run in walk
         * order — the loop's own order, kept exactly (cleanup move 2c). */
        @com.legend.base.Nullable VerdictBatch batch;
        private final List<StatementExecutor.PreparedValue> values = new java.util.ArrayList<>();
        private com.legend.exec.EffectSink sink = new com.legend.exec.EffectSink();
        private int effectFrom = -1;
        private int effectTo = -1;
        private @com.legend.base.Nullable ExecutionResult last;
        private @com.legend.base.Nullable ExecutionResult pendingVerdict;
        private boolean lastIsValue;

        Segments(StatementExecutor.ExecEnv env0, SpecCompiler specs) {
            this.env0 = env0;
            this.specs = specs;
            this.batch = AssertVerdicts.databaseMode(env0) ? StatementExecutor.newVerdictBatch() : null;
        }

        StatementExecutor.ExecEnv env() {
            return batch == null ? env0 : env0.withVerdictBatch(batch);
        }

        /** The verdict rows deferred so far (none under the host judge). */
        int rows() {
            return batch == null ? 0 : batch.pendingCount();
        }

        void verdict(ExecutionResult v) {
            pendingVerdict = v;
            lastIsValue = false;
        }

        void value(StatementExecutor.PreparedValue pv) {
            if (batch == null) {
                // the host judge: no deferral anywhere — the value runs now, in walk order
                last = StatementExecutor.runValue(pv, specs, new java.util.ArrayDeque<>());
            } else {
                values.add(pv);
            }
            lastIsValue = true;
        }

        /** An effect statement, handled exactly as the loop handles it, with the sink on
         * the environment: the arms collect their statements instead of sending. */
        void collectEffect(TypedSpec stmt, List<TypedSpec> stmts, int i, List<TypedSpec> letPrefix,
                Map<String, StatementExecutor.ExecFrame> execFrames) {
            StatementExecutor.markWriting(env0.connection());
            StatementExecutor.ExecEnv sinkEnv = env().withEffectSink(sink);
            if (effectFrom < 0) {
                effectFrom = i + 1;
            }
            effectTo = i + 1;
            if (stmt instanceof TypedLet let && i < stmts.size() - 1) {
                // an executeInDb binding: the corpus binds an opaque ResultSet handle as a
                // smoke check and never reads it; any other read is walled up front
                if (!ConnectionLets.onlyConnectionReads(stmts, i + 1, let.name())) {
                    throw new IllegalStateException("reading an executeInDb result binding ('"
                            + let.name() + "') is not supported");
                }
                List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
                single.add(let.value());
                List<TypedSpec> inlined = new com.legend.compiler.spec.UserCallInliner(
                        specs, StatementExecutor.spliceHook(execFrames, letPrefix, specs, sinkEnv))
                        .inlineBody(single);
                inlined = StatementExecutor.resolver(specs, sinkEnv).resolve(inlined, sinkEnv.runtimeFqn());
                last = StatementExecutor.executeTyped(inlined, sinkEnv);
                lastIsValue = true;
                return;
            }
            TypedSpec bare = com.legend.compiler.spec.typed.Lets.bare(stmt);
            StatementExecutor.PreparedValue pv = StatementExecutor.prepareValue(
                    stmt, bare, letPrefix, execFrames, specs, sinkEnv);
            last = StatementExecutor.runValue(pv, specs, new java.util.ArrayDeque<>());
            lastIsValue = true;
        }

        /** Send the open effect segment as one script. */
        void closeEffects() {
            if (sink.isEmpty()) {
                return;
            }
            String where = effectFrom == effectTo ? "(statement " + effectFrom + ")"
                    : "(statements " + effectFrom + "–" + effectTo + ")";
            fragments.put("effects:" + effectFrom + "-" + effectTo, "effects " + where);
            StatementExecutor.sendScript(env0, sink, where);
            sink = new com.legend.exec.EffectSink();
            effectFrom = -1;
            effectTo = -1;
        }

        /** Run the open verdicts segment: its value statements in order, then its fused
         * statement (one per connection; appeals on failed rows; the first failure
         * raises); a fresh batch opens for the next segment. */
        void closeVerdicts() {
            for (StatementExecutor.PreparedValue pv : values) {
                last = StatementExecutor.runValue(pv, specs, new java.util.ArrayDeque<>());
            }
            values.clear();
            if (batch != null) {
                batch.fragments(fragments);
                AssertVerdicts.flush(batch, env());
            }
            if (!lastIsValue && pendingVerdict != null) {
                last = pendingVerdict;
            }
            pendingVerdict = null;
            if (batch != null) {
                batch = StatementExecutor.newVerdictBatch();
            }
        }
    }

    /** The verdict rows an assert root deferred, named in the fragment map. */
    private static void nameRows(Map<String, String> fragments, VerdictBatch batch, int from,
            @com.legend.base.Nullable String callee, int ordinal) {
        String name = callee == null ? "assert" : callee.substring(callee.lastIndexOf(':') + 1);
        for (int ix = from; ix < batch.pendingCount(); ix++) {
            fragments.put(com.legend.lowering.VerdictSql.INDEX + "=" + ix,
                    name + " (statement " + ordinal + ")");
        }
    }
}
