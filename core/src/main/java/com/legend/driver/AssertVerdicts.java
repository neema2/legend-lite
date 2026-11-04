// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.ExecuteOptions;

import com.legend.builtin.NativeFn;

import com.legend.compiler.element.ModelContext;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.judge.TypedSide;
import com.legend.judge.PureAsserts;

import java.util.ArrayList;
import java.util.List;

/**
 * THE ASSERT-FAMILY VERDICT ARM (Charter Clause 2c): a statement-root
 * call to the assert family is a VERDICT — its result terminates in the
 * runner. Each ARGUMENT executes through the full pipeline in the
 * database; the judgment over the two sides is {@link PureAsserts}
 * (host mode) or one VerdictSql statement (database mode). Assert
 * bodies are never inlined into SQL to produce a verdict. Map-wrapped
 * asserts ({@code values->map(f|assert(...))}) are QUANTIFIED verdicts
 * served by the quantified arm; family members without a verdict arm
 * decline loudly with their shape — never a silent skip.
 */
final class AssertVerdicts {
    private AssertVerdicts() {
    }

    private static final String PKG = com.legend.compiler.element.type.PlatformTypes.ASSERTS_PACKAGE;

    /** V7 batch 2: the statement loop's result-envelope splice hook,
     * threaded into every side evaluation so an assert argument reading
     * an execute() frame compiles the SPLICED chain — identical to the
     * ordinary-statement path (audit 19d B2; splice pin:
     * AssertVerdictSpliceTest). Null = no frames in scope. */
    interface SpliceHook extends java.util.function.BiFunction<TypedSpec,
            java.util.Set<String>, TypedSpec> {
    }

    /** Null = not a statement-root assert this arm owns (generic path
     * continues); otherwise the verdict (TRUE, or the spec's failure
     * raised as the runner's failure). */
    static @com.legend.base.Nullable TypedSide tryAdjudicate(TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        com.legend.judge.AssertListener l = env.assertListener();
        com.legend.judge.VerdictBatch batch = env.verdictBatch();
        if (batch != null) {
            return batched(batch, bare, letPrefix, specs, env, rawHook);
        }
        if (l == null) {
            return adjudicate(bare, letPrefix, specs, env, rawHook);
        }
        // the listener observes the arm's OWN outcomes only: a non-null
        // verdict = pass; a raise out of an owned adjudication = fail
        // (side-evaluation errors fail the same test either way — the
        // detail says which). Judgment itself is untouched.
        boolean settled = false;
        com.legend.judge.CanonicalDivergence.sqlEnter();   // census: a new assert, no family yet
        try {
            TypedSide v = adjudicate(bare, letPrefix, specs, env,
                    rawHook);
            settled = true;
            if (v != null) {
                l.verdict(listenerName(bare), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed
                | com.legend.error.DataError e) {
            // the seam: a FALSE verdict or a side-evaluation data error
            // fails the same test either way — the detail says which
            settled = true;
            com.legend.judge.CanonicalDivergence.sqlRaised();
            if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                l.unjudged(listenerName(bare), af.unjudgedReason());
            }
            l.verdict(listenerName(bare), false, e.getMessage());
            throw e;
        } finally {
            if (!settled) {
                // any other exit (a wall, a not-implemented shape) leaves
                // without a verdict: counted for the census, never caught
                com.legend.judge.CanonicalDivergence.sqlRaised();
            }
        }
    }

    /**
     * The database judge's classification of {@code bare} as a PLAN (the test-is-the-product plan's M7): the arm run
     * under {@code env}'s batch -- a collector, nothing sent -- so its verdict statements DEFER into the open root, a
     * decided outcome or a raise is a resolved step, and a WALL is a step raised at the root's turn (the walk discards
     * the root and surfaces the wall after the flush: {@link #batched}; here nothing may run, so the raise is planned
     * at its place). False where the arm answers null: not an assert the database judge plans. The census as the
     * walk's: the assert entered here, its family at the arm, its verdict at the judgment.
     */
    /** {@link #planDatabase}'s answer: the root planned (deferred, or resolved as a decision or a raise), WALLED
     *  (the arm left through a wall: the root a step raised at its turn, nothing after it planned), or UNPLANNED (not
     *  an assert the database arms plan). */
    enum DatabasePlan { PLANNED, WALLED, UNPLANNED }

    static DatabasePlan planDatabase(com.legend.judge.VerdictBatch batch, TypedSpec bare, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> rawHook) {
        if (isHelperCall(bare)) {
            // a helper call at the root reduced once to the assert it is (M10c item 4): gated and adjudicated as that
            // assert, under its own name -- as the walk's prepareValue opens the batch's root for the reduced assert
            TypedSpec helper = BodyDriver.helperAssertRoot(bare, letPrefix, specs, env, rawHook);
            if (helper == null) {
                return DatabasePlan.UNPLANNED;
            }
            bare = helper;
        }
        DatabaseGate gate = databaseGate(bare, letPrefix, specs, env, rawHook);
        if (gate == DatabaseGate.UNPLANNED) {
            return DatabasePlan.UNPLANNED;
        }
        // our text a side the plan reads: the arm runs under the database judge's text collector, which plans it as a
        // side of the verdict row (SqlTextVerdicts.BatchTexts) -- nothing read at planning
        SqlTextVerdicts.BatchTexts texts = gate == DatabaseGate.ENGINE_TEXT_READ ? new SqlTextVerdicts.BatchTexts() : null;
        BodyDriver.ExecEnv at = texts != null ? texts.under(env) : env;
        com.legend.judge.CanonicalDivergence.sqlEnter();
        batch.open(listenerName(bare));
        TypedSide v;
        try {
            v = adjudicate(bare, letPrefix, specs, at, rawHook);
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            batch.resolve(e);
            batch.close();
            return DatabasePlan.PLANNED;
        } catch (RuntimeException e) {
            // counted as batched counts the exit, at the arm, under the wall's own family
            com.legend.judge.CanonicalDivergence.sqlRaised();
            batch.wall(e);
            batch.close();
            return DatabasePlan.WALLED;
        } finally {
            if (texts != null) {
                // planned: the appeal the arm built runs at the flush in the same environment and reads its sides then
                texts.planning = false;
            }
        }
        if (v == null) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", listenerName(bare), "the database arm: "
                    + shapeOf(bare));
            batch.discard();
            return DatabasePlan.UNPLANNED;
        }
        batch.close();
        return DatabasePlan.PLANNED;
    }

    /**
     * THE STATIC GATE of the database judge's planner (M7): whether {@code bare} is a shape whose database arm plans
     * its verdict statement without READING a side on the host at its turn. The host checks' own classification
     * ({@link #planHost}, null for the shapes neither judge plans statically: a lineage print, the quantified forms,
     * the declining judgments) gates first; then the shapes the database
     * dispatch hands to the host at the assert -- the grid pair of two relation-typed sides ({@code HostJudge
     * .planGridPair}, D3), the rendered-text sides ({@link #renderedArm}), {@code assertEq} over a class-kind side
     * ({@code HostJudge.eq}, D43) -- which would run their sides where the planner only plans. The walk runs such a
     * body. Planning only: the host checks plan the sides; nothing is sent.
     */
    static boolean databasePlansWithoutReading(TypedSpec bare, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> rawHook) {
        return databaseGate(bare, letPrefix, specs, env, rawHook) != DatabaseGate.UNPLANNED;
    }

    /** The static gate's answers: the arm PLANS its verdict statement without reading; an ENGINE_TEXT assert plans its
     *  text row with the appeal deferred (M10b); the arm RAISES at planning (its classification raised inside the arm:
     *  {@link HostAssert#raising}) -- the planning pass walls the root at its place, which the program carries only
     *  where a row before it in the segment holds the wall ({@code DatabaseAsserts.close}); or the shape is UNPLANNED
     *  (the walk's). */
    enum DatabaseGate { PLANS, ENGINE_TEXT, ENGINE_TEXT_READ, RAISES, UNPLANNED }

    /** Whether {@code g} is an engine-text assert's: its texts constants of the plan, or OUR text a side the verdict
     *  row computes ({@link DatabaseGate#ENGINE_TEXT_READ}, 2026-11-04, the core program). */
    static boolean engineText(DatabaseGate g) {
        return g == DatabaseGate.ENGINE_TEXT || g == DatabaseGate.ENGINE_TEXT_READ;
    }

    /** {@link #databasePlansWithoutReading}'s answer in three (M10b: a raise at planning told apart, so the static pass
     *  refuses a body whose first root of a segment would wall a batch with no row to carry it -- the planning pass
     *  would then count it and give the body back, and the walk would count it again). */
    static DatabaseGate databaseGate(TypedSpec bare, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> rawHook) {
        if (isHelperCall(bare)) {
            // a helper call at the root reduced once to the assert it is (M10c item 4): the gate and the planning pass
            // read the reduced root, as planHost plans it on the host road
            TypedSpec helper = BodyDriver.helperAssertRoot(bare, letPrefix, specs, env, rawHook);
            if (helper == null) {
                return DatabaseGate.UNPLANNED;
            }
            bare = helper;
        }
        if (quantifiedForm(bare) != null) {
            // a quantified assert (M10c item 5): the vector forms plan without reading (the database arm's predicate
            // vector); the unroll form is gated element by element -- any element the walk keeps, the walk keeps the
            // body; a raise is the first element's, a row before it carries a later one (the planning pass's own
            // root holds every element's row)
            QuantifiedPlan qp = planQuantified(bare, letPrefix, specs, env, rawHook);
            if (qp == null) {
                return DatabaseGate.UNPLANNED;
            }
            if (qp instanceof VectorPlan) {
                return DatabaseGate.PLANS;
            }
            DatabaseGate worst = DatabaseGate.PLANS;
            boolean first = true;
            for (Unrolled u : ((UnrollPlan) qp).elements()) {
                DatabaseGate g = gateOf(u.assertion(), u.root(), u.before(), specs, env, rawHook);
                if (g == DatabaseGate.UNPLANNED) {
                    return g;
                }
                if (g == DatabaseGate.RAISES && first) {
                    return g;
                }
                if (engineText(g) && worst != DatabaseGate.ENGINE_TEXT_READ) {
                    worst = g;
                }
                first = false;
            }
            return worst;
        }
        HostAssert shape = planHost(bare, letPrefix, specs, env, rawHook);
        if (shape == null) {
            return DatabaseGate.UNPLANNED;
        }
        return gateOf(shape, bare, letPrefix, specs, env, rawHook);
    }

    /** {@link #databaseGate}'s answer for {@code bare} once the host checks planned it as {@code shape}: the raise and
     *  the engine text from the judgment, the rest from the arm's shape over the root's arguments. */
    private static DatabaseGate gateOf(HostAssert shape, TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> rawHook) {
        if (HostJudgment.core(shape.check().judgment()) instanceof HostJudgment.Raise) {
            return DatabaseGate.RAISES;
        }
        if (HostJudgment.core(shape.check().judgment()) instanceof HostJudgment.EngineText e) {
            if (e.collected().golden() == null) {
                // a golden the plan reads: the arm processes its golden at planning, and the verdict row composes the
                // database judge's text equality with the golden a constant -- the walk's, by name
                com.legend.exec.Census.unplannedAssert("database: a golden read at the step: " + shapeOf(bare));
                return DatabaseGate.UNPLANNED;
            }
            if (e.collected().ours() == null) {
                // our text a side the plan reads (M10c item 8; the database judge since 2026-11-04, the core
                // program): the verdict row computes it and the equality, its ACTUAL column the appeal's text
                return DatabaseGate.ENGINE_TEXT_READ;
            }
            // an engine-text assert (M10b): its texts constants of the plan, its appeal the batch row's -- the walk's
            // own deferral, planDatabase's adjudicate under the collecting batch; nothing read at planning
            return DatabaseGate.ENGINE_TEXT;
        }
        NativeFn.Verdict fn = NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (fn == null) {
            return DatabaseGate.UNPLANNED;
        }
        List<TypedSpec> raw = (bare instanceof TypedUserCall u) ? u.args() : ((TypedNativeCall) bare).args();
        SpliceHook hook = rawHook == null ? null : rawHook::apply;
        boolean plans = switch (fn) {
            case ASSERT_EQUALS, ASSERT_NOT_EQUALS -> {
                if (raw.size() < 2) {
                    yield false;
                }
                boolean wantEqual = fn == NativeFn.Verdict.ASSERT_EQUALS;
                List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(raw, letPrefix,
                        specs);
                if (com.legend.compiler.element.type.Type.isRelation(args.get(0).info().type())
                        && com.legend.compiler.element.type.Type.isRelation(args.get(1).info().type())) {
                    // the SQL grid pair (M10c item 2): the route's own predicate
                    yield sqlGridPair(args.get(0), args.get(1));
                }
                yield renderedPlan(fn.bareName(), wantEqual, args, letPrefix, hook, true) == null
                        || renderedStatic(args, letPrefix);
            }
            case ASSERT_SAME_ELEMENTS -> {
                if (raw.size() < 2) {
                    yield false;
                }
                List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(raw, letPrefix,
                        specs);
                yield renderedPlan(fn.bareName(), true, args, letPrefix, hook, false) == null
                        || renderedStatic(args, letPrefix);
            }
            // eq over a PAIR of class-kind sides is identity, the host's (D43); with one side a primitive it is the
            // kind and the value, which the database's kind-faithful canon decides (2026-11-03: assertEq('echo', an
            // Any-typed 'echo') had kept testLetWithParam from the database judge)
            case ASSERT_EQ -> raw.size() >= 2 && (!(classKind(raw.get(0)) && classKind(raw.get(1)))
                    || keylessInstances(raw.get(0), raw.get(1), env.ctx()));
            default -> true;
        };
        if (!plans) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", fn.bareName(), "the database gate: "
                    + shapeOf(bare));
        }
        return plans ? DatabaseGate.PLANS : DatabaseGate.UNPLANNED;
    }

    /** A relation-typed side whose schema the planning declares: columns named at typing, none dynamic or late-bound
     *  -- the SQL grid pair's width and kinds are known before any side runs (the test-is-the-product plan's M10c
     *  item 2). A late-bound grid (a raw grid, a dynamic pivot) is the walk's: its columns exist when the plan runs. */
    static boolean staticGrid(TypedSpec s) {
        return com.legend.compiler.element.type.Type.schemaView(s.info().type())
                instanceof com.legend.compiler.element.type.Type.RelationType rt
                && !rt.columns().isEmpty() && rt.dynamicColumns().isEmpty();
    }

    /** Whether a pair of relation-typed sides is the SQL GRID PAIR's (the database arm's `gridPair`; the test-is-the-
     *  product plan's M10c item 2): both grids {@link #staticGrid}, of one width, their columns named alike and
     *  declared of one kind, position by position -- the three facts the host grid owner decides from the grids
     *  ({@code TdsCompare.grids}: the names, the width, the expected grid's kinds typing both) that the SQL form,
     *  which compares the row canons alone, does not see. The same predicate gates the database road and routes the
     *  walk's pair, so a pair it refuses keeps the host grid owner on every road (the item's audit, A). */
    static boolean sqlGridPair(TypedSpec e, TypedSpec a) {
        // two static grids, whatever their schemas (2026-11-04, the core program): where the names or the widths
        // differ the grid owner's verdict is false by the schemas alone (gridSchemasDiffer), decided at planning, the
        // statement read for the message; where only the kinds differ the cells are spelled in the expected's kinds
        // (the grid owner's rule, TdsCompare.grids)
        return staticGrid(e) && staticGrid(a);
    }

    /** Two static grids whose column names or widths differ: the grid owner's verdict is false (TdsCompare.grids). */
    static boolean gridSchemasDiffer(TypedSpec e, TypedSpec a) {
        var se = com.legend.compiler.element.type.Type.schemaView(e.info().type());
        var sa = com.legend.compiler.element.type.Type.schemaView(a.info().type());
        if (!(se instanceof com.legend.compiler.element.type.Type.RelationType re)
                || !(sa instanceof com.legend.compiler.element.type.Type.RelationType ra)) {
            return false;
        }
        if (re.columns().size() != ra.columns().size()) {
            return true;
        }
        for (int i = 0; i < re.columns().size(); i++) {
            if (!re.columns().get(i).name().equals(ra.columns().get(i).name())) {
                return true;
            }
        }
        return false;
    }

    /** Whether a RENDERED-TEXT pair's database arm plans without a read: a rendered grid is a {@link #staticGrid}
     *  (a late-bound grid's text is a keyed statement the program road does not place: {@code renderedValueVerdict}'s
     *  raw-grid branch); a rendered value is any. */
    static boolean renderedStatic(List<TypedSpec> args, List<TypedSpec> letPrefix) {
        java.util.function.UnaryOperator<TypedSpec> chase = s -> chaseLets(s, letPrefix);
        for (TypedSpec a : args.subList(0, 2)) {
            var r = com.legend.compiler.spec.VerdictQueries.renderedSide(a, chase);
            if (r != null && r.grid() && !staticGrid(r.value())) {
                return false;
            }
        }
        return true;
    }

    /** Leg 3.4: the assert's verdict statements DEFER into the body's batch
     * (VerdictBatch); a decided outcome or a raise is a step in order,
     * reported at the flush. Any other exit (a wall) flushes what came
     * before — those verdicts were already the body's — then surfaces. */
    private static @com.legend.base.Nullable TypedSide batched(com.legend.judge.VerdictBatch batch,
            TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        com.legend.judge.CanonicalDivergence.sqlEnter();
        batch.open(listenerName(bare));
        TypedSide v = null;
        RuntimeException deferred = null;
        boolean settled = false;
        try {
            v = adjudicate(bare, letPrefix, specs, env, rawHook);
            settled = true;
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            settled = true;
            deferred = e;
        } finally {
            if (!settled) {
                com.legend.judge.CanonicalDivergence.sqlRaised();
                batch.discard();
                flush(batch, env);
            }
        }
        if (deferred != null) {
            batch.resolve(deferred);
            batch.close();
            return ok();
        }
        if (v == null) {
            batch.discard();
            return null;
        }
        batch.close();
        return v;
    }

    static void flush(com.legend.judge.VerdictBatch batch, BodyDriver.ExecEnv env) {
        batch.flush(env.assertListener());
    }

    /** A statement-root HELPER call: a user function that is neither a root engine-text arm nor a verdict native
     *  (the test-is-the-product plan's M10c item 4; {@code BodyDriver.helperAssertRoot} says what it reduces to). */
    static boolean isHelperCall(TypedSpec bare) {
        String fqn = calleeFqn(bare);
        return fqn != null && bare instanceof TypedUserCall && !SqlTextVerdicts.isRoot(fqn)
                && NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).isEmpty();
    }

    /** The name an assert is reported under: its callee's full name ({@code assertError}'s too, which
     *  {@link #calleeFqn} keeps off the verdict routes), else its shape. */
    static String listenerName(TypedSpec bare) {
        String fqn = com.legend.compiler.spec.typed.Calls.calleeOf(bare);
        return fqn != null ? fqn
                : bare instanceof com.legend.compiler.spec.typed.TypedMap
                        ? "quantified-assert" : bare.getClass().getSimpleName();
    }

    /** Two instances of KEYLESS classes: their equality is identity, which the database's identity canon (the
     *  construction's {@code __id}) decides -- {@code eq}'s own rule (2026-11-04, the core program: InstanceIdentityTest's
     *  eqIdentity under the database judge alone). A keyed class's canon is its key tree, equal's rule, not eq's: those
     *  stay the host's identity (D43). */
    static boolean keylessInstances(TypedSpec e, TypedSpec a, com.legend.compiler.element.ModelContext ctx) {
        return classKind(e) && classKind(a)
                && com.legend.compiler.element.EqualityKeys.fqnOf(e.info().type()) != null
                && com.legend.compiler.element.EqualityKeys.fqnOf(a.info().type()) != null
                && com.legend.compiler.element.EqualityKeys.resolve(ctx, e.info().type()) == null
                && com.legend.compiler.element.EqualityKeys.resolve(ctx, a.info().type()) == null;
    }

    static boolean classKind(TypedSpec s) {
        return s.info().type() instanceof com.legend.compiler.element.type.Type.ClassType;
    }

    private static @com.legend.base.Nullable TypedSide adjudicate(TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        if (env.options().judgeMode() == ExecuteOptions.JudgeMode.HOST) {
            // the host judge's planned shapes (the test-is-the-product plan's M6): the two halves the program road
            // places as a verdict step, run back to back here
            HostAssert planned = planHost(bare, letPrefix, specs, env, rawHook);
            if (planned != null) {
                return planned.run(letPrefix, specs, env);
            }
        }
        SpliceHook hook = rawHook == null ? null : rawHook::apply;
        // task #14 leg 1 (2026-09-21): a lineage tree print is judged as LINES —
        // the golden brought to its canon rows here, ours the prelude's own
        // rows; the ordinary collection verdict decides
        TypedSpec lines = com.legend.compiler.spec.LineageTreeLines.asLines(bare, letPrefix, env.ctx());
        if (lines != null) {
            bare = lines;
        }
        TypedSpec unwrapped = com.legend.compiler.spec.VerdictQueries.distinctTrueWrapper(bare);
        if (unwrapped instanceof com.legend.compiler.spec.typed.TypedMap qm2) {
            TypedSide u = unrolled(qm2, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
        }
        if (bare instanceof com.legend.compiler.spec.typed.TypedMap qm) {
            TypedSide u = unrolled(qm, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
            return quantified(qm, letPrefix, specs, env, hook);
        }
        // forAll(coll, x | <assert>) — the engine's per-element assert
        // idiom (stringToFloat testProject: `[123.456, 100.001]->zip(
        // $tds.rows.values)->forAll(pair | assertEqWithinTolerance(...))`)
        // IS the quantified assert: every element's assert holds (an
        // assert never yields false — it raises), so the forAll unrolls
        // exactly as the map form does
        com.legend.compiler.spec.typed.TypedMap forAllForm =
                com.legend.compiler.spec.VerdictQueries.forAllAsQuantified(bare);
        if (forAllForm != null) {
            TypedSide u = unrolled(forAllForm, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
            // the simple predicate under forAll is the vector form, as under map (M10c item 5's audit: the forAll
            // spelling fell through to the generic path, where the Lowerer has no arm for an assert -- a wall where
            // the map spelling judged)
            return quantified(forAllForm, letPrefix, specs, env, hook);
        }
        String fqn = calleeFqn(bare);
        if (fqn == null) {
            return null;
        }
        // cleanup move 2b (2026-09-21): the run's judge mode names the arm ONCE; every
        // family below classifies, then hands the sides to it
        VerdictArm arm = arm(env);
        // SQLTEXT charter §8.3b — the assertSameSQL ROOT arm: the
        // statement root arrives PRE-inline, so SqlTextVerdicts owns
        // the whole golden-vs-executed-frame shape (rows judge, text
        // is the emission census). Null = not the simple shape — the
        // generic path (inline + fold) keeps it exactly as today.
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT_SAME_SQL.equals(fqn)
                && bare instanceof TypedUserCall sroot) {
            TypedSide sv = SqlTextVerdicts.tryArmSameSql(sroot,
                    letPrefix, specs, env, hook);
            if (sv != null) {
                return sv;
            }
        }
        // TDG scoring flip — the assertSqlEquals root (same discipline)
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT_SQL_EQUALS_TDG.equals(fqn)
                && bare instanceof TypedUserCall troot) {
            TypedSide tv = SqlTextVerdicts.tryArmTdgRoot(troot,
                    letPrefix, specs, env, hook);
            if (tv != null) {
                return tv;
            }
        }
        // §8.3d — the dual-golden sibling (same root-arm discipline)
        if (com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS_DUAL_GOLDEN.equals(fqn)
                && bare instanceof TypedUserCall hroot) {
            TypedSide hv = SqlTextVerdicts.tryArmDualGolden(hroot,
                    letPrefix, specs, env, hook);
            if (hv != null) {
                return hv;
            }
            // the arm is TOTAL: where no text arm applies, the function's own
            // meaning on our H2 (h2Extension.pure:29 picks the upgraded
            // golden on every version but 1.4.200) IS assertEquals(upgraded,
            // actual) — adjudicated as that verdict, never as a standing
            // call for some other route to catch
            if (hroot.args().size() == 3) {
                TypedSpec asEquals = com.legend.compiler.spec.VerdictQueries
                        .assertEqualsOf(hroot.args().get(1), hroot.args().get(2), specs);
                if (asEquals != null) {
                    return adjudicate(asEquals, letPrefix, specs, env, rawHook);
                }
            }
        }
        // THE GRID VERDICT (Clause 2c — TdsCompare's chartered route;
        // witness: the relation suite's 79 assertTdsEquivalent rows):
        // both relations execute IN THE DATABASE, the cell-zip
        // adjudicates host-side (tdsEquivalent.pure's numeric delta +
        // temporal seconds policies, already the one owner).
        // The family is the closed type NativeFn.Verdict: a member not in it
        // (assertError with its own arm, fail) is a normal fall-through —
        // and the switch below is an EXPRESSION with no default, so a new
        // member does not compile until it is placed (batch 3).
        NativeFn.Verdict fn = NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (fn == null) {
            return null;
        }
        // a caller's failure message (the asserts-as-plan-nodes program, step 2, 2026-11-04): the family's arm judges the
        // base overload; under the database judge its verdict row fails with the message's text (DatabaseJudge.runVerdict)
        com.legend.compiler.spec.AssertMessages.Split split = env.options().judgeMode() == ExecuteOptions.JudgeMode.HOST
                ? null : com.legend.compiler.spec.AssertMessages.split(bare, specs);
        if (split != null) {
            env = env.withFailureMessage(DatabaseJudge.failureMessage(split.message(), letPrefix, specs, env, hook));
            bare = split.base();
        }
        String name = fn.bareName();
        com.legend.judge.CanonicalDivergence.sqlFamily(name);   // leg 3.0 census
        List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(
                (bare instanceof TypedUserCall u) ? u.args() : ((TypedNativeCall) bare).args(),
                letPrefix, specs);
        TypedSide adjudicated = switch (fn) {
            case ASSERT_TDS_EQUIVALENT -> {
            List<TypedSpec> targs = ((bare instanceof TypedUserCall u2)
                    ? u2.args() : ((TypedNativeCall) bare).args());
            if (targs.size() < 3 || targs.size() > 4) {
                yield null;
            }
            yield arm.tdsEquivalent(name, targs, letPrefix, specs, env, hook);
            }
            // toCSV is an OPERAND form (a rendered grid text the verdict
            // compares), never an assert of its own
            case TO_CSV -> {
                yield null;
            }
            case ASSERT_EQUALS, ASSERT_NOT_EQUALS -> {
                if (args.size() < 2) {
                    yield null;
                }
                boolean wantEqual = fn == NativeFn.Verdict.ASSERT_EQUALS;
                // SQLTEXT charter slice 3a — the SQL-TEXT arm: a
                // toSQLString producer in an argument tree judges on
                // ROWS (SqlTextVerdicts; text is a census number).
                // Shapes outside the exact cohort yield null here.
                TypedSide sv = SqlTextVerdicts.tryArm(name,
                        wantEqual, args, letPrefix, specs, env, hook);
                if (sv != null) {
                    com.legend.judge.CanonicalDivergence.sqlRoute(name, "sql-text");
                    yield sv;
                }
                // D3 — the RENDERED-TEXT arm: exactly one side is a
                // DB-rendered grid text (toCSV/toString/replace/join
                // spellings), the peer a string value. The DATABASE
                // computed the render; TdsCompare.renderedText (the
                // one policy owner, R1b-probed) judges the texts.
                TypedSide ra = renderedArm(name, wantEqual, args,
                        letPrefix, specs, env, hook, true);
                if (ra != null) {
                    com.legend.judge.CanonicalDivergence.sqlRoute(name, "rendered-text");
                    yield ra;
                }
                // D3 — the GRID-PAIR arm: both sides statically
                // relation-stamped execute as grids; the grid owner
                // (TdsCompare.grids: columns ordered, rows under the
                // order policy) judges — never a byte decline. The judge's
                // arm: the host's the grid owner over both grids, the
                // database's the SQL grid pair (M10c item 2); the rows in
                // order where the actual side is SORTED, else a multiset
                if (com.legend.compiler.element.type.Type.isRelation(
                        args.get(0).info().type())
                        && com.legend.compiler.element.type.Type.isRelation(
                                args.get(1).info().type())) {
                    if (sqlGridPair(args.get(0), args.get(1))) {
                        boolean sorted = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                                == com.legend.compiler.spec.OrderView.SORTED;
                        yield arm.gridPair(name, wantEqual, args, sorted, letPrefix, specs, env, hook);
                    }
                    // two grids whose declared schemas differ (a width, a name, a kind) or whose columns the
                    // planning does not know: the grid owner on the host decides, as before (the SQL form compares
                    // the cells alone; the M10c item 2 audit's A)
                    yield HostSides.judged(name, HostJudge.planGridPair(name, wantEqual, args, letPrefix, hook),
                            letPrefix, specs, env);
                }
                // D3 — the ORDER VIEW: an INCIDENTAL-order actual side
                // (unsorted store read / frame read) has SQL arrival
                // order — engine goldens encode H2's, ours is DuckDB's
                // — so both sides fetch with the CANONICAL-order riders
                // and the judgment is order-insensitive (exactly the
                // assertSameElements shape). SORTED/DEFINED sides stay
                // strictly ordered.
                boolean incidental = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                        == com.legend.compiler.spec.OrderView.INCIDENTAL;
                // §8 leg 1 — grid-ness is STATIC (the declared result
                // shape, decided before execution — the ratified
                // no-runtime-sniffing rule): a grid pair fetches in
                // DEFINITION order (the peer's row chunking depends on
                // it; the canonical-order rider would destroy it) and
                // any multiset view sorts DB-computed canon texts
                // host-side instead — semantics-free string sorting.
                boolean gridPair = tabularShaped(args.get(0))
                        || tabularShaped(args.get(1));
                yield arm.equals(name, wantEqual, args, incidental, gridPair, letPrefix, specs, env, hook);
            }
            case ASSERT_SAME_ELEMENTS -> {
                if (args.size() < 2) {
                    yield null;
                }
                // D3 — rendered-text sides (a sep-joined grid string
                // vs its golden): the token/line multiset judges
                TypedSide rse = renderedArm(name, true, args,
                        letPrefix, specs, env, hook, false);
                if (rse != null) {
                    com.legend.judge.CanonicalDivergence.sqlRoute(name, "rendered-text");
                    yield rse;
                }
                boolean seGridPair = tabularShaped(args.get(0))
                        || tabularShaped(args.get(1));
                yield arm.sameElements(name, args, seGridPair, letPrefix, specs, env, hook);
            }
            case ASSERT_SIZE -> {
                if (args.size() < 2) {
                    yield null;
                }
                yield arm.size(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_JSON_STRINGS_EQUAL -> {
                // D4 — the JSON verdict: engine semantics (object keys
                // order-INSENSITIVE, arrays order-SENSITIVE) over
                // PARSED structures; JsonCompare is the one tree owner
                // (V3 register). Sides are DB-computed strings.
                if (args.size() != 2) {
                    yield null;
                }
                yield arm.jsonStringsEqual(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_CONTAINS -> {
                // real pure membership (assertContains.pure): both
                // sides DB-computed, the lattice judges element
                // equality; message args are failure-position only
                if (args.size() < 2) {
                    yield null;
                }
                yield arm.contains(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_EQ -> {
                if (args.size() < 2) {
                    yield null;
                }
                yield arm.eq(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_EQ_WITHIN_TOLERANCE -> {
                if (args.size() < 3) {
                    yield null;
                }
                yield arm.tolerance(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT, ASSERT_FALSE -> {
                if (args.isEmpty()) {
                    yield null;
                }
                yield arm.condition(name, args.get(0), fn == NativeFn.Verdict.ASSERT,
                        letPrefix, specs, env, hook);
            }
            case ASSERT_INSTANCE_OF -> {
                if (args.size() < 2) {
                    yield null;
                }
                // the /3 message overload has no witness — fall through
                if (args.size() != 2) {
                    yield null;
                }
                yield arm.instanceOf(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_IS -> {
                // is() = IDENTITY (real pure is.pure:23, PCT.platformOnly).
                // World-1 adjudication for statically-identified operands
                // only; message overloads have no witness — fall through.
                if (args.size() != 2) {
                    yield null;
                }
                TypedSide isv = isVerdict(args.get(0), args.get(1));
                if (isv != null) {
                    arm.staticallyDecided(name);
                    yield isv;
                }
                yield arm.is(name, args, letPrefix, specs, env, hook);
            }
            case ASSERT_EMPTY, ASSERT_NOT_EMPTY -> {
                if (args.isEmpty()) {
                    yield null;
                }
                yield arm.empty(name, args.get(0), fn == NativeFn.Verdict.ASSERT_EMPTY,
                        letPrefix, specs, env, hook);
            }
        };
        if (adjudicated == null) {
            com.legend.judge.CanonicalDivergence.sqlFellThrough();
        }
        return adjudicated;
    }

    /**
     * A statement-root assert the HOST judge plans in two halves (the test-is-the-product plan's M6, brief B3): its
     * listener name, its family (the census key {@link #adjudicate} counts), whether it holds on equality, and the
     * planned arm ({@link HostSides.Check}: the sides it asks for and its judgment over them). The walk runs it in
     * place ({@link #run}); the body router places its sides as a verdict step's base and judges at that step.
     */
    record HostAssert(String listenerName, @com.legend.base.Nullable String family, boolean wantEqual,
            HostSides.Check check, @com.legend.base.Nullable RuntimeException beforeTheArm) {

        HostAssert(String listenerName, @com.legend.base.Nullable String family, boolean wantEqual,
                HostSides.Check check) {
            this(listenerName, family, wantEqual, check, null);
        }

        /** An assert whose planning raised {@code failure} INSIDE its arm: it raises it at its turn, as the walk does
         *  where it plans the assert (after the asserts before it ran), counted as the walk counts it -- entered,
         *  raised; {@code family} null where the classification raised before any family was named (the walk counts
         *  that raise before any family). */
        static HostAssert raising(String listenerName, @com.legend.base.Nullable String family,
                RuntimeException failure) {
            return new HostAssert(listenerName, family, true,
                    new HostSides.Check(List.of(), new HostJudgment.Raise(failure)));
        }

        /** An assert whose statement raised {@code failure} BEFORE its arm was entered -- the walk raises a let's
         *  handle registration at the let's turn and the assert's frame environment before {@link #tryAdjudicate}
         *  is called -- so it is raised at its turn with no census and no listener (M6's audit, S1). */
        static HostAssert raisingBeforeTheArm(String listenerName, RuntimeException failure) {
            return new HostAssert(listenerName, null, true,
                    new HostSides.Check(List.of(), new HostJudgment.Raise(failure)), failure);
        }

        /** The name the assert's sides go by: its family, or its listener name where none was classified. */
        String name() {
            return family != null ? family : listenerName;
        }

        /** The census at the assert's turn: its family, counted before any side runs (as {@link #adjudicate}
         *  counts it). */
        void enter() {
            if (family != null) {
                familyEntered(family);
            }
        }

        /** The verdict over {@code sides}, shaped: a declined judgment counted as the generic path's (null); an
         *  engine-text equality's census route counted once the arm answered, as {@link #adjudicate} counts it
         *  (M10b: never on a failed one, which raises before). */
        @com.legend.base.Nullable TypedSide judge(List<TypedSide> sides, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
            return judge(() -> sides, rules);
        }

        /** The verdict over its sides as the runner read them: their rows, or the error the reading raised, which
         *  the judgment decides (an assertError's answer, any other's failure: {@code HostJudge.judge}). */
        @com.legend.base.Nullable TypedSide judge(java.util.function.Supplier<List<TypedSide>> sides,
                com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
            TypedSide v = judgedOrFellThrough(HostJudge.judge(check.judgment(), sides, rules));
            if (v != null && HostJudgment.core(check.judgment()) instanceof HostJudgment.EngineText e && e.routed()) {
                sqlTextRouted(name());
            }
            return v;
        }

        /** Run in place (the walk): counted, its sides fetched, judged. */
        @com.legend.base.Nullable TypedSide run(List<TypedSpec> letPrefix, SpecCompiler specs,
                BodyDriver.ExecEnv env) {
            if (beforeTheArm != null) {
                throw beforeTheArm;
            }
            if (HostJudgment.core(check.judgment()) instanceof HostJudgment.Unrolled u) {
                // the walk's unroll: each element entered, fetched and judged in turn, the first failure stopping it
                // (the program road reads every element's sides in one step and judges them in order: the one
                // difference, on a failing body -- its later elements' sides are read; M10c item 5)
                TypedSide last = null;
                for (HostAssert part : u.parts()) {
                    last = part.run(letPrefix, specs, env);
                    if (last == null) {
                        throw new com.legend.error.NotImplementedException(
                                "unrolled quantified assert: element verdict not adjudicable for " + part.listenerName());
                    }
                }
                return last == null ? ok() : last;
            }
            enter();
            // an engine-text check reading our text as a side: fetched under the origin the arm marked for it, as
            // the arm sent it before the side was deferred and as the program road's step is sent (M10c item 8)
            com.legend.exec.StatementOrigin marked = deferredTextOrigin(check.judgment());
            try (var __o = marked == null ? null : com.legend.exec.StatementOrigin.enter(marked)) {
                return judge(HostSides.fetch(name(), check.asks(), letPrefix, specs, env),
                        env.options().judgingRules());
            }
        }
    }

    private static void familyEntered(String family) {
        com.legend.judge.CanonicalDivergence.sqlFamily(family);
    }

    /** The equality arm's census route, as {@link #adjudicate} counts it once the engine-text arm answered. */
    private static void sqlTextRouted(String family) {
        com.legend.judge.CanonicalDivergence.sqlRoute(family, "sql-text");
    }

    private static @com.legend.base.Nullable TypedSide judgedOrFellThrough(@com.legend.base.Nullable TypedSide v) {
        if (v == null) {
            com.legend.judge.CanonicalDivergence.sqlFellThrough();
        }
        return v;
    }

    /**
     * THE PROGRAM ROAD'S VERDICT SEAM (the test-is-the-product plan's M6): {@code a}, planned before the program ran,
     * judged at its verdict step over the sides the runner read ({@code sides} shapes them, or raises the error
     * reading them raised: the judgment decides it, {@code HostJudge.judge}) -- counted and reported to
     * {@code listener} exactly as {@link #tryAdjudicate} counts and reports an assert the walk adjudicates in place:
     * entered, its verdict reported, a failure (an assert that does not hold, a side's data error) counted as raised
     * and reported, any other exit counted as raised. A side the database refused is thus counted and reported as an
     * assert whose side raised inside the arm (M6's audit, S2; one path since 2026-11-03). Null where the judgment
     * declined the sides it read.
     */
    static @com.legend.base.Nullable TypedSide judgedAtItsStep(HostAssert a,
            com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            java.util.function.Supplier<List<TypedSide>> sides, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        if (a.beforeTheArm() != null) {
            // raised where the walk raises it, before the arm is entered: no census, no listener (S1)
            throw a.beforeTheArm();
        }
        if (listener == null) {
            a.enter();
            return a.judge(sides, rules);
        }
        com.legend.judge.CanonicalDivergence.sqlEnter();
        boolean settled = false;
        try {
            a.enter();
            TypedSide v = a.judge(sides, rules);
            settled = true;
            if (v != null) {
                listener.verdict(a.listenerName(), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            settled = true;
            com.legend.judge.CanonicalDivergence.sqlRaised();
            if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                listener.unjudged(a.listenerName(), af.unjudgedReason());
            }
            listener.verdict(a.listenerName(), false, e.getMessage());
            throw e;
        } finally {
            if (!settled) {
                com.legend.judge.CanonicalDivergence.sqlRaised();
            }
        }
    }

    /** The host half's verdict over its sides HELD for the agreement judge: its verdict, or the failure it raised
     *  (an assert that does not hold), never null together. */
    record HostHeld(@com.legend.base.Nullable TypedSide verdict, com.legend.error.@com.legend.base.Nullable AssertFailed failed) {
    }

    /**
     * THE AGREEMENT JUDGE'S HOST HALF AT ITS STEP (the walker-deletion program, 2026-11-02): {@code a} judged over the
     * sides the runner read, its verdict HELD -- the verdict of record is decided once the database half's rows are in
     * ({@link #agreedAtItsStep}) -- counted as {@link #tryAdjudicate} counts an assert it adjudicates in place over the
     * agreement arm (entered; a side's data error counted raised, reported and thrown at once, as the agreement arm
     * lets it through; any other exit counted raised). A judgment that declined the sides it read is refused by name.
     */
    static HostHeld heldAtItsStep(HostAssert a, com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            java.util.function.Supplier<List<TypedSide>> sides, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        if (a.beforeTheArm() != null) {
            throw a.beforeTheArm();
        }
        if (listener != null) {
            com.legend.judge.CanonicalDivergence.sqlEnter();
        }
        boolean settled = false;
        try {
            a.enter();
            TypedSide v = null;
            com.legend.error.AssertFailed failed = null;
            try {
                v = a.judge(sides, rules);
            } catch (com.legend.error.AssertFailed e) {
                failed = e;
            }
            settled = true;
            if (failed != null) {
                return new HostHeld(null, failed);
            }
            if (v == null) {
                throw new com.legend.error.NotImplementedException(a.name() + " on the program road: the host"
                        + " judge declined the sides it read (the walk's generic path has no program form)");
            }
            return new HostHeld(v, null);
        } catch (com.legend.error.DataError e) {
            settled = true;
            if (listener != null) {
                com.legend.judge.CanonicalDivergence.sqlRaised();
                listener.verdict(a.listenerName(), false, e.getMessage());
            }
            throw e;
        } finally {
            if (!settled && listener != null) {
                com.legend.judge.CanonicalDivergence.sqlRaised();
            }
        }
    }

    /** The agreement judge's verdict of record for {@code a} ({@code agreed}: the agreement rule over both halves,
     *  {@link AgreementArm#agree}), reported to {@code listener} as {@link #tryAdjudicate} reports it: a pass, or a
     *  failure (an assert that does not hold, a data error) counted raised, its UNJUDGED reason and its verdict told,
     *  and thrown. */
    static TypedSide agreedAtItsStep(HostAssert a, com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            java.util.function.Supplier<@com.legend.base.Nullable TypedSide> agreed) {
        try {
            TypedSide v = java.util.Objects.requireNonNull(agreed.get(), "a host verdict the agreement rule dropped");
            if (listener != null) {
                listener.verdict(a.listenerName(), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            if (listener != null) {
                com.legend.judge.CanonicalDivergence.sqlRaised();
                if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                    listener.unjudged(a.listenerName(), af.unjudgedReason());
                }
                listener.verdict(a.listenerName(), false, e.getMessage());
            }
            throw e;
        }
    }

    /**
     * {@code bare} planned for the HOST judge (the test-is-the-product plan's M6), classified as {@link #adjudicate}
     * classifies it, in its order: the grid pair, the rendered text, the equality, the multiset, the size, the JSON
     * text, the membership, {@code assertEq}, the tolerance, the condition, the instance test, the identity, the
     * emptiness, the TDS equivalence. Null where the shape is one the host checks do not plan yet -- a lineage print,
     * the quantified forms, an engine-text assert whose text side is not a constant of the plan
     * ({@link SqlTextVerdicts#plan}, M10b: the arms themselves plan since then),
     * the assertion with a message overload no arm owns, a callee outside the verdict family, and the shapes whose
     * judgment would decline the sides it read (M6's audit, S3: a membership of a value not exactly one, a JSON text
     * that is not one String a side, an instance test of a computed type, a TDS equivalence of sides not relations):
     * the walk's own dispatch adjudicates it, and the body router leaves its body on the walk. Planning sends nothing
     * and counts nothing; a classification that raises once the family is named is planned as that raise under the
     * family (the walk counts the family before the arm: M6's audit, N2).
     */
    /** An assert's SHAPE for the unplanned-assert census ({@code Census.UNPLANNED_ASSERTS}): its callee's simple name
     *  and each argument's declared type with the argument's node kind -- a label the lanes histogram, deciding
     *  nothing. */
    static String shapeOf(TypedSpec bare) {
        String fqn = calleeFqn(bare);
        String name = fqn == null ? bare.getClass().getSimpleName() : fqn;
        List<TypedSpec> args = bare instanceof TypedNativeCall nc ? nc.args()
                : bare instanceof TypedUserCall uc ? uc.args() : List.of();
        StringBuilder out = new StringBuilder(name).append('(');
        for (int i = 0; i < args.size(); i++) {
            TypedSpec a = args.get(i);
            out.append(i > 0 ? ", " : "").append(a.info().type().typeName()).append('[')
                    .append(a.info().multiplicity()).append("] ")
                    .append(a.getClass().getSimpleName().replace("Typed", ""));
        }
        return out.append(')').toString();
    }

    /** The unplanned-assert census row {@code label} for {@code bare}, written only where the statement reaches a
     *  verdict ({@code StatementEffects.callsVerdict}): the walk asks the host planner about EVERY statement root
     *  (BodyCompiler's tryAdjudicate), so a value statement -- a {@code println}, a map of them, a computed value -- is
     *  no refused assert. A measurement, deciding nothing. */
    private static void unplanned(TypedSpec bare, SpecCompiler specs, String label) {
        if (com.legend.compiler.spec.StatementEffects.callsVerdict(bare, specs, new java.util.HashMap<>())) {
            com.legend.exec.Census.unplannedAssert(label);
        }
    }

    static @com.legend.base.Nullable HostAssert planHost(TypedSpec bare, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        SpliceHook hook = rawHook == null ? null : rawHook::apply;
        TypedSpec lines = com.legend.compiler.spec.LineageTreeLines.asLines(bare, letPrefix, env.ctx());
        if (lines != null) {
            // a lineage tree print against its golden is judged as LINES, as the walk's adjudicate rewrites it (the
            // golden's lines a String collection, ours the prelude's rows over the scan's handle): the rewritten
            // assert planned as any collection equality, reported under the call the body spells (the
            // test-is-the-product plan's M10c item 3)
            HostAssert rewritten = planHost(lines, letPrefix, specs, env, rawHook);
            return rewritten == null ? null : new HostAssert(listenerName(bare), rewritten.family(),
                    rewritten.wantEqual(), rewritten.check(), rewritten.beforeTheArm());
        }
        if (quantifiedForm(bare) != null) {
            // a quantified assert (M10c item 5): the vector forms one host check over all rows; the unroll form the
            // elements' planned asserts as ONE check (their asks in order, the composite judgment), so the statement
            // reports one verdict as the walk's tryAdjudicate does
            QuantifiedPlan qp = planQuantified(bare, letPrefix, specs, env, rawHook);
            if (qp instanceof VectorPlan v) {
                return v.assertion();
            }
            if (qp == null) {
                unplanned(bare, specs, "quantified: " + shapeOf(bare));
            }
            return qp instanceof UnrollPlan u ? composite(bare, u) : null;
        }
        String fqn = calleeFqn(bare);
        if (fqn == null) {
            // a call with no callee, or an `if` whose branches assert (the walk's two-branch evaluation, S4's ruling) --
            // each named for the census; any other root is a value statement the walk asks about too, not an assert
            if (bare instanceof TypedNativeCall || bare instanceof TypedUserCall) {
                unplanned(bare, specs, "no callee: " + bare.getClass().getSimpleName());
            } else if (bare instanceof com.legend.compiler.spec.typed.TypedIf) {
                unplanned(bare, specs, "an if root");
            }
            return null;
        }
        // the three engine-text ROOT arms (the test-is-the-product plan's M10b) are planned by the arm the walk runs
        // for them, under no family (the walk counts none before a root arm); every other callee is a verdict
        // function, planned under its family
        boolean root = SqlTextVerdicts.isRoot(fqn);
        NativeFn.Verdict fn = root ? null
                : NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (!root && fn == null) {
            // a HELPER call at the root (the test-is-the-product plan's M10c item 4): the walk inlines it and
            // adjudicates its reduced root where that root asserts a class value or reads a string entry
            // (BodyDriver.helperAssertRoot); planned the same. Reported under the call the body spells: the walk's
            // tryAdjudicate names a statement's verdict by its root, and since this plan exists the walk under the
            // HOST judge takes it first -- before this item the walk's prepareValue reported the helper under its
            // reduced root's name (a deliberate change of the report's label, as the lineage-lines and dual-golden
            // rewraps; the DATABASE judge names the reduced root on both roads, planDatabase)
            TypedSpec helper = isHelperCall(bare) ? BodyDriver.helperAssertRoot(bare, letPrefix, specs, env, rawHook)
                    : null;
            if (helper == null) {
                unplanned(bare, specs, "helper root not reduced: " + shapeOf(bare));
            }
            HostAssert rewritten = helper == null ? null : planHost(helper, letPrefix, specs, env, rawHook);
            return rewritten == null ? null : new HostAssert(listenerName(bare), rewritten.family(),
                    rewritten.wantEqual(), rewritten.check(), rewritten.beforeTheArm());
        }
        String name = fn == null ? null : fn.bareName();
        // a caller's failure message (the asserts-as-plan-nodes program, step 2, 2026-11-04): the family's arm plans the
        // base overload, the message one more side its failure raises (HostJudge.messaged)
        com.legend.compiler.spec.AssertMessages.Split split = fn == null ? null
                : com.legend.compiler.spec.AssertMessages.split(bare, specs);
        TypedSpec armRoot = split == null ? bare : split.base();
        HostSides.Check check;
        try {
            check = fn == null ? SqlTextVerdicts.plan(bare, letPrefix, specs, env, hook)
                    : planArm(fn, fn.bareName(), armRoot, letPrefix, specs, env, hook);
        } catch (RuntimeException e) {
            // the arm's classification raised under its family (none for a root arm): raised at the assert's turn,
            // the family counted first, as adjudicate counts it before the arm (N2)
            return HostAssert.raising(listenerName(bare), name, e);
        }
        if (SqlTextVerdicts.notAConstant(check)) {
            // a text side the arm would evaluate by a statement at the assert's turn: the walk's (the census names
            // the side's kind and planned root, then the assert's shape)
            unplanned(bare, specs, "text not a constant: " + SqlTextVerdicts.why(check) + ": "
                    + shapeOf(bare));
            return null;
        }
        if (check == null) {
            if (com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS_DUAL_GOLDEN.equals(fqn)
                    && bare instanceof TypedUserCall hroot && hroot.args().size() == 3) {
                // the arm is TOTAL (adjudicate): where no text arm applies, the function's own meaning on our H2 is
                // assertEquals(upgraded, actual), planned as that verdict
                TypedSpec asEquals = com.legend.compiler.spec.VerdictQueries.assertEqualsOf(hroot.args().get(1),
                        hroot.args().get(2), specs);
                if (asEquals == null) {
                    unplanned(bare, specs, "dual-golden rewrite declined: " + shapeOf(bare));
                }
                HostAssert rewritten = asEquals == null ? null : planHost(asEquals, letPrefix, specs, env, rawHook);
                // reported under the call the body spells, as the walk reports it (the M10b audit's S3)
                return rewritten == null ? null : new HostAssert(listenerName(bare), rewritten.family(),
                        rewritten.wantEqual(), rewritten.check(), rewritten.beforeTheArm());
            }
            if (fn != NativeFn.Verdict.ASSERT_EQUALS && fn != NativeFn.Verdict.ASSERT_NOT_EQUALS) {
                unplanned(bare, specs, "arm declined: " + shapeOf(bare));
            }
            return null;
        }
        boolean wantEqual = fn == null || fn != NativeFn.Verdict.ASSERT_NOT_EQUALS
                && fn != NativeFn.Verdict.ASSERT_FALSE && fn != NativeFn.Verdict.ASSERT_NOT_EMPTY;
        HostSides.Check planned = split == null ? check : HostJudge.messaged(check, split.message(), hook);
        return new HostAssert(listenerName(bare), name, wantEqual, planned);
    }

    /** {@code fn}'s arm planned over {@code bare}'s arguments, or null where the host checks do not plan the shape. */
    private static HostSides.@com.legend.base.Nullable Check planArm(NativeFn.Verdict fn, String name, TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook) {
        List<TypedSpec> raw = (bare instanceof TypedUserCall u) ? u.args() : ((TypedNativeCall) bare).args();
        List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(raw, letPrefix, specs);
        return switch (fn) {
            // the grids relations by their types, so the judgment never meets a side that is not a grid (S3)
            case ASSERT_TDS_EQUIVALENT -> raw.size() < 3 || raw.size() > 4
                    || !com.legend.compiler.element.type.Type.isRelation(raw.get(0).info().type())
                    || !com.legend.compiler.element.type.Type.isRelation(raw.get(1).info().type()) ? null
                    : HostJudge.planTdsEquivalent(raw, hook);
            case TO_CSV -> null;
            case ASSERT_EQUALS, ASSERT_NOT_EQUALS -> {
                boolean wantEqual = fn == NativeFn.Verdict.ASSERT_EQUALS;
                if (args.size() < 2) {
                    yield null;
                }
                // the engine-text arm declining is no decline of the assert: the generic path below plans most such
                // shapes (a plan handle's Boolean, M10c item 8's gate); the census names it on a final decline alone
                boolean armDeclined = false;
                if (SqlTextVerdicts.mayArm(name, wantEqual, args, letPrefix, specs)) {
                    // the engine-text arm (M10b): a producer shape planned under a collector, its two texts the step's
                    // sides (the golden a constant; ours a constant or a side the step reads, M10c item 8); null where
                    // the arm declines the shape, and the generic path below is the walk's
                    HostSides.Check engineText = SqlTextVerdicts.plan(bare, letPrefix, specs, env, hook);
                    if (SqlTextVerdicts.notAConstant(engineText)) {
                        // a text side the arm would evaluate by a statement at the assert's turn: the walk's, not
                        // the generic equality's (which would compare the texts the arm appeals)
                        unplanned(bare, specs, "equals: text not a constant: "
                                + SqlTextVerdicts.why(engineText) + ": " + shapeOf(bare));
                        yield null;
                    }
                    if (engineText != null) {
                        yield engineText;
                    }
                    armDeclined = true;
                }
                String after = armDeclined ? " after the engine-text arm declined" : "";
                HostSides.Check rendered = renderedPlan(name, wantEqual, args, letPrefix, hook, true);
                if (rendered != null) {
                    yield rendered;
                }
                if (com.legend.compiler.element.type.Type.isRelation(args.get(0).info().type())
                        && com.legend.compiler.element.type.Type.isRelation(args.get(1).info().type())) {
                    yield HostJudge.planGridPair(name, wantEqual, args, letPrefix, hook);
                }
                boolean incidental = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                        == com.legend.compiler.spec.OrderView.INCIDENTAL;
                boolean gridPair = tabularShaped(args.get(0)) || tabularShaped(args.get(1));
                HostSides.Check equals = HostJudge.planEquals(name, wantEqual, args, incidental, gridPair, letPrefix,
                        env, hook);
                if (equals == null) {
                    unplanned(bare, specs, "equals: planEquals declined" + after + ": " + shapeOf(bare));
                }
                yield equals;
            }
            case ASSERT_SAME_ELEMENTS -> {
                if (args.size() < 2) {
                    yield null;
                }
                HostSides.Check rendered = renderedPlan(name, true, args, letPrefix, hook, false);
                yield rendered != null ? rendered : HostJudge.planSameElements(args,
                        tabularShaped(args.get(0)) || tabularShaped(args.get(1)), letPrefix, env, hook);
            }
            case ASSERT_SIZE -> args.size() < 2 ? null : HostJudge.planSize(args, letPrefix, hook);
            // both texts one String each by their types, so the judgment never meets another shape (S3)
            case ASSERT_JSON_STRINGS_EQUAL -> args.size() != 2 || !oneString(args.get(0)) || !oneString(args.get(1))
                    ? null : HostJudge.planJsonStringsEqual(args, letPrefix, hook);
            // the value exactly one by its multiplicity, so the judgment never meets a collection (S3)
            case ASSERT_CONTAINS -> args.size() < 2 || !exactlyOneValue(args.get(1)) ? null
                    : HostJudge.planContains(args, hook);
            case ASSERT_EQ -> args.size() < 2 ? null : HostJudge.planEq(args, letPrefix, env, hook);
            case ASSERT_EQ_WITHIN_TOLERANCE -> args.size() < 3 ? null : HostJudge.planTolerance(args, hook);
            case ASSERT, ASSERT_FALSE -> args.isEmpty() ? null
                    : HostJudge.planCondition(name, args.get(0), fn == NativeFn.Verdict.ASSERT, hook);
            // the type named by a reference, so the judgment never meets a computed type (S3)
            case ASSERT_INSTANCE_OF -> args.size() != 2 || typeRefName(args.get(1)) == null ? null
                    : HostJudge.planInstanceOf(args, env, hook);
            case ASSERT_IS -> {
                if (args.size() != 2) {
                    yield null;
                }
                TypedSpec left = args.get(0);
                TypedSpec right = args.get(1);
                // statically identified operands: decided with no side (the verdict, or its failure, at the
                // assert's turn)
                yield staticallyIdentified(left, right) ? new HostSides.Check(List.of(),
                        java.util.Objects.requireNonNull(isDecided(left, right), "statically identified operands"))
                        : HostJudge.planIs(name, args, specs, hook);
            }
            case ASSERT_EMPTY, ASSERT_NOT_EMPTY -> args.isEmpty() ? null
                    : HostJudge.planEmpty(args.get(0), fn == NativeFn.Verdict.ASSERT_EMPTY, hook);
        };
    }

    /** Whether {@code s} is typed exactly one value that is not a relation: its fetched side decodes to one value. */
    private static boolean exactlyOneValue(TypedSpec s) {
        return s.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() == 1 && b.isToOne()
                && !com.legend.compiler.element.type.Type.isRelation(s.info().type());
    }

    /** Whether {@code s} is typed one String. */
    private static boolean oneString(TypedSpec s) {
        return s.info().type() == com.legend.compiler.element.type.Type.Primitive.STRING && exactlyOneValue(s);
    }

    /** Whether {@link #isVerdict} decides {@code left} against {@code right} with no side: both a statically known
     *  type, or the same let-bound instance. */
    private static boolean staticallyIdentified(TypedSpec left, TypedSpec right) {
        if (typeIdentityOf(left) != null && typeIdentityOf(right) != null) {
            return true;
        }
        return instanceOrigin(left) instanceof com.legend.compiler.spec.typed.TypedVariable lv
                && instanceOrigin(right) instanceof com.legend.compiler.spec.typed.TypedVariable rv
                && lv.name().equals(rv.name());
    }

    /** {@link #renderedArm} planned for the host judge: null when the pair is not the rendered-text shape; the
     *  route counted where the judgment holds (as {@link #adjudicate} counts it once the arm answered). */
    private static HostSides.@com.legend.base.Nullable Check renderedPlan(String name, boolean wantEqual,
            List<TypedSpec> args, List<TypedSpec> letPrefix, @com.legend.base.Nullable SpliceHook hook,
            boolean orderedForm) {
        String eForm = renderForm(args.get(0), letPrefix);
        String aForm = renderForm(args.get(1), letPrefix);
        if ((eForm == null && aForm == null) || (eForm != null && aForm != null && !eForm.equals(aForm))) {
            return null;
        }
        String form = aForm != null ? aForm : java.util.Objects.requireNonNull(eForm);
        TypedSpec rendered = aForm != null ? args.get(1) : args.get(0);
        return HostJudge.planRendered(name, wantEqual, args, form, rendered, eForm, aForm, orderedForm, letPrefix,
                hook, true);
    }

    /** A side typed as a SEEDED metaclass (its extent is in the system
     *  database — a Database, a Mapping, a Class …): its identity is a row key. */
    static boolean elementTyped(TypedSpec s, SpecCompiler specs) {
        if (!(peel(s).info().type() instanceof com.legend.compiler.element.type.Type.ClassType ct)) {
            return false;
        }
        // the seeded metaclass itself, or a SUPERTYPE a system function
        // answers with (resolveStore returns Store; the rows are Databases):
        // some system-mapped, seeded metaclass conforms to the side's type
        var ctx = specs.ctx();
        if (ctx.tracksClassifier(ct.fqn())) {
            return true;
        }
        var sys = ctx.findMapping(com.legend.builtin.SystemMetamodel.MAPPING_FQN).orElse(null);
        return sys != null && sys.classBindings().stream().anyMatch(cb ->
                ctx.tracksClassifier(cb.classFqn())
                        && ctx.isSubtype(cb.classFqn(), ct.fqn()));
    }

    /** The IDENTITY verdict ({@code assertIs} → {@code is()}, real pure
     * is.pure:23 "pointer equality"): adjudicable in World 1 ONLY when
     * both operands are STATICALLY identified — a type reference (bare
     * element, {@code type(x)->toOne()}, {@code genericType(x).rawType})
     * or the same let-bound instance by construction provenance. Any
     * other shape returns null: the legacy path then walls loudly on
     * {@code is}'s missing SQL rule — a wire carries values, never
     * reference identity (the eq/equalNonPrimitive irreducible ruling). */
    static @com.legend.base.Nullable TypedSide isVerdict(
            TypedSpec left, TypedSpec right) {
        HostJudgment.Decided d = isDecided(left, right);
        return d == null ? null : HostJudge.decided(d);
    }

    /** {@link #isVerdict} as data (M8a): the verdict decided at planning with no side -- held, or its failure's
     *  message -- or null where the operands are not statically identified. */
    static HostJudgment.@com.legend.base.Nullable Decided isDecided(TypedSpec left, TypedSpec right) {
        String lt = typeIdentityOf(left);
        String rt = typeIdentityOf(right);
        if (lt != null && rt != null) {
            return lt.equals(rt) ? new HostJudgment.Decided(true, null)
                    : new HostJudgment.Decided(false, "\nexpected: " + lt + "\nactual:   " + rt);
        }
        TypedSpec l = instanceOrigin(left);
        TypedSpec r = instanceOrigin(right);
        if (l instanceof com.legend.compiler.spec.typed.TypedVariable lv
                && r instanceof com.legend.compiler.spec.typed.TypedVariable rv
                && lv.name().equals(rv.name())) {
            // the same let-bound variable in one frame IS the same object
            return new HostJudgment.Decided(true, null);
        }
        return null;
    }

    /** The statically-known TYPE a value expression identifies, or null.
     * {@code type()}/{@code genericType().rawType} resolve to the STATIC
     * type of their argument — sound exactly when that type is concrete
     * (a literal or constructed instance), which is what the witnesses
     * pass ({@code type(+1)}, {@code genericType(^LA_Person(...))}). */
    private static @com.legend.base.Nullable String typeIdentityOf(TypedSpec t) {
        TypedSpec s = peel(t);
        if (s instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr) {
            return canonicalTypeFqn(pr.fullPath());
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedTypeRef tr) {
            return canonicalTypeFqn(tr.target().typeName());
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::meta::type")
                && !c.args().isEmpty()) {
            return staticTypeName(c.args().get(0));
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("rawType")
                && peel(pa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNativeCall gt
                && com.legend.builtin.NativeFn.SubtypeForm.of(gt.callee().id()).orElse(null) == com.legend.builtin.NativeFn.SubtypeForm.GENERIC_TYPE
                && !gt.args().isEmpty()) {
            return staticTypeName(gt.args().get(0));
        }
        return null;
    }

    private static @com.legend.base.Nullable String staticTypeName(TypedSpec arg) {
        // concrete static identification only: a literal's primitive or a
        // constructed/class-typed value — never an Any/generic stamp
        var ty = peel(arg).info().type();
        if (ty instanceof com.legend.compiler.element.type.Type.ClassType ct) {
            // the static class decides (assertIs(Employee, [^Employee(..), ^Person(..)]->at(0)->type()) is decided
            // false by the list's static class, Person: AssertBarTest's assertIs-dynamic-type gap). Narrowing it to a
            // construction alone (aaa8badb, 2026-11-04) sent every other class-typed value to a run-time path that
            // cannot read a type -- two PCT tests lost (testConcatenateTypeInference, testAssignNewInstance) -- and
            // closed no gap: reverted the same day; the dynamic type waits on a run-time type read
            return ct.fqn();
        }
        String n = ty.typeName();
        return switch (n) {
            case "Integer", "Float", "Decimal", "String", "Boolean", "Date",
                    "StrictDate", "DateTime", "StrictTime" ->
                    canonicalTypeFqn(n);
            default -> null;
        };
    }

    /** ONE spelling for a type identity: PRIMITIVES canonicalize to their
     * M3 FQN so all three resolution arms agree (bare {@code Integer},
     * {@code @Integer}, and {@code type(1)} name the same element).
     * Anything else — including packageless user test classes — keeps
     * its resolved spelling untouched. */
    private static String canonicalTypeFqn(String name) {
        return switch (name) {
            case "Integer", "Float", "Decimal", "String", "Boolean", "Date",
                    "StrictDate", "DateTime", "StrictTime", "Number" ->
                    "meta::pure::metamodel::type::" + name;
            default -> name;
        };
    }

    /** Peel value-preserving wrappers ({@code toOne}) and fold a property
     * read over a constructed instance to its constructor argument — the
     * provenance chain the OneToOne witness rides. */
    private static TypedSpec peel(TypedSpec t) {
        TypedSpec s = t;
        while (true) {
            if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                    && !c.args().isEmpty()) {
                s = c.args().get(0);
                continue;
            }
            return s;
        }
    }

    private static TypedSpec instanceOrigin(TypedSpec t) {
        TypedSpec s = peel(t);
        if (s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && peel(pa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNewInstance ni
                && ni.properties().get(pa.property()) != null) {
            return instanceOrigin(ni.properties().get(pa.property()));
        }
        return s;
    }

    /** The QUANTIFIED verdict: {@code xs->map(f|assert(pred[, 'msg']))}
     * at a statement root. The predicate VECTORIZES in the database
     * ({@code xs->map(f|pred)} — pure data computation); the boolean
     * vector is judged here, first failure raising the assert's message
     * — the interpreter's per-element semantics without an interpreter.
     * Null = not a quantified assert (generic path continues); shapes
     * beyond assert/assertFalse with a literal-or-absent message decline
     * LOUDLY. */
    /** A quantified assert over a LITERAL collection whose lambda carries
     * lets or a non-boolean assert ({@code [pair(H2, sql), ...]->map(p|
     * let driver = $p.first; ...; assertEquals($expectedSql, $result,
     * fmt, args))}): UNROLLED — each element binds the parameter as a let
     * ahead of the lambda's own lets, the inliner reduces the lets (the
     * one substitution engine), and the final assert statement
     * adjudicates as a statement-root verdict. All elements must hold.
     * Null when not this shape (a runtime collection, a one-statement
     * predicate lambda — the vector form). */
    /** The quantified shapes the walk's {@link #adjudicate} meets, in its order: {@code xs->map(x|<assert>)} under the
     *  distinct/true wrappers, bare, or {@code xs->forAll(x|<assert>)}; null for any other root. */
    static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedMap quantifiedForm(TypedSpec bare) {
        if (com.legend.compiler.spec.VerdictQueries.distinctTrueWrapper(bare)
                instanceof com.legend.compiler.spec.typed.TypedMap qm) {
            return qm;
        }
        if (bare instanceof com.legend.compiler.spec.typed.TypedMap qm) {
            return qm;
        }
        return com.legend.compiler.spec.VerdictQueries.forAllAsQuantified(bare);
    }

    /** A quantified assert's shape as the walk's unroll reads it: the source through the lets and the one substitution
     *  engine, the lambda's root, whether the VECTOR CONTRACT holds (the predicate vector then), whether the root is
     *  the SIMPLE predicate ({@code assert}/{@code assertFalse} with a literal message: the vector form judged over all
     *  rows). Null where the shape is no quantified assert the walk adjudicates. */
    record QuantifiedShape(com.legend.compiler.spec.typed.TypedMap qm, TypedSpec source,
            com.legend.compiler.spec.typed.TypedLambda lam, TypedSpec root, @com.legend.base.Nullable String fqn,
            boolean nestedQuantified, @com.legend.base.Nullable TypedSpec vectorPredMap, boolean simplePredicate) {
    }

    static @com.legend.base.Nullable QuantifiedShape quantifiedShape(com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        var lam = qm.mapper();
        // the collection through the caller's lets (let expected = [...])
        TypedSpec source = com.legend.compiler.spec.typed.Lets.bound(qm.source(), letPrefix);
        // a property read over a LET-BOUND instance literal is that field
        // ($_s2_hoisted.columnValuePairs — the hoisted constructor's zip):
        // the one rule Pipelines.instanceLiteralProp spells, through the lets
        if (source instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && com.legend.compiler.spec.typed.Lets.bound(pa.source(), letPrefix)
                        instanceof com.legend.compiler.spec.typed.TypedNewInstance inst
                && inst.properties().get(pa.property()) != null) {
            source = java.util.Objects.requireNonNull(inst.properties().get(pa.property()));
        }
        // a COMPUTED-but-spelled collection (DatabaseType->enumValues()
        // ->filter(e | $e->in([...]))) reduces through the one substitution
        // engine to its literal elements before the shape is judged
        if (!(source instanceof com.legend.compiler.spec.typed.TypedCollection
                || source instanceof com.legend.compiler.spec.typed.TypedNewInstance
                || source instanceof TypedNativeCall)) {
            TypedSpec last = com.legend.compiler.spec.UserCallInliner
                    .forVerdictSource(specs, rawHook).reduceVerdictSource(source, letPrefix);
            if (last instanceof com.legend.compiler.spec.typed.TypedCollection) {
                source = last;
            }
        }
        if (lam.parameters().size() != 1 || lam.body().isEmpty()) {
            return null;
        }
        TypedSpec root = lam.body().get(lam.body().size() - 1);
        String fqn = calleeFqn(root);
        // a NESTED quantification (ids->map(i | pairs->map(cv |
        // assert(...)))) — the engine's guard idiom in
        // createTableRowIdentifiers: each outer element's inner map is
        // itself a quantified assert, adjudicated recursively
        boolean nestedQuantified = root instanceof com.legend.compiler.spec.typed.TypedMap
                || com.legend.compiler.spec.VerdictQueries.forAllAsQuantified(root) != null;
        if (!nestedQuantified && (fqn == null || !fqn.startsWith(PKG))) {
            return null;
        }
        // the VECTOR form (quantified) raises the message host-side, so it
        // needs a literal one; a COMPUTED message ('Table : ' + $table->
        // getQualifiedTableName() + ...) rides each element through the
        // unroll instead, where the statement-root assert arm judges the
        // condition (the message is diagnostic, never the verdict)
        boolean literalMessage = true;
        if (root instanceof TypedUserCall ru && ru.args().size() >= 2) {
            literalMessage = ru.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCString;
        } else if (root instanceof TypedNativeCall rn && rn.args().size() >= 2) {
            literalMessage = rn.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCString;
        }
        // THE VECTOR CONTRACT (VerdictQueries.vectorContract, 2026-09-22): a per-element assert
        // is the predicate it means; over a row source, row-local, with a literal message, the
        // vector is planned and judged in the fused statement (zip = a join on the row number,
        // forAll = no failing row). The unroll — which FETCHES a zip arm's cells into
        // Java to spell them as literals — stays the road for everything else.
        TypedSpec vectorPredMap = null;
        if (!nestedQuantified && lam.body().size() == 1 && fqn != null) {
            TypedSpec pred = com.legend.compiler.spec.VerdictQueries.assertAsPredicate(root, specs);
            if (pred != null && com.legend.compiler.spec.VerdictQueries.vectorContract(source, lam, root, pred)) {
                vectorPredMap = com.legend.compiler.spec.VerdictQueries.predicateVectorOver(source, qm, lam, pred);
            }
        }
        boolean simplePredicate = !nestedQuantified && fqn != null && lam.body().size() == 1
                && (fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT)
                        || fqn.equals(com.legend.compiler.element.type.PlatformTypes.ASSERT_FALSE))
                && literalMessage;
        return new QuantifiedShape(qm, source, lam, root, fqn, nestedQuantified, vectorPredMap, simplePredicate);
    }

    /** The PER-ELEMENT form's operand pairs for {@code sh} (the asserts-as-plan-nodes program, step 5, 2026-11-04):
     *  an {@code assertEquals} with no message of its own, one statement in its lambda, each operand one value per
     *  element -- {@code source->map(x | pair(expected, actual))}, the database computing both; null where the form
     *  does not apply. */
    private static @com.legend.base.Nullable TypedSpec perElementPairs(QuantifiedShape sh, BodyDriver.ExecEnv env) {
        if (sh.nestedQuantified() || NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(sh.root()))
                .orElse(null) != NativeFn.Verdict.ASSERT_EQUALS) {
            return null;
        }
        List<TypedSpec> args = com.legend.compiler.spec.typed.Calls.argsOf(sh.root());
        return args.size() != 2 ? null : com.legend.compiler.spec.VerdictQueries.pairsVector(sh.source(), sh.lam(),
                args.get(0), args.get(1), env.ctx());
    }

    /** The family name the vector arms report under: the root's verdict native's bare name, else its FQN. */
    private static String vectorFamily(QuantifiedShape sh) {
        return NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(sh.root()))
                .map(NativeFn.Verdict::bareName).orElse(java.util.Objects.requireNonNull(sh.fqn()));
    }

    private static @com.legend.base.Nullable TypedSide unrolled(
            com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        QuantifiedShape sh = quantifiedShape(qm, letPrefix, specs, env, rawHook);
        if (sh == null) {
            return null;
        }
        if (sh.vectorPredMap() != null) {
            TypedSide planned = arm(env).quantifiedVector(vectorFamily(sh), sh.vectorPredMap(), sh.source(), sh.lam(),
                    sh.root(), letPrefix, specs, env, rawHook == null ? null : rawHook::apply);
            if (planned != null) {
                return planned;
            }
        }
        if (sh.simplePredicate()) {
            return null;
        }
        // the elements are compiler-owned SYNTHESIS (VerdictQueries, Invariant 7) over LITERAL tokens alone: a zip
        // arm the database would answer is the vector form's above, never fetched here (the seam program's step
        // after S2f, 2026-10-30; the unroll rearranges spelled tokens and reads nothing)
        List<TypedSpec> elements = com.legend.compiler.spec.VerdictQueries
                .unrollElements(sh.source(), letPrefix, env.ctx());
        if (elements == null) {
            // any other source: the per-element form, the operands computed by the database (step 5)
            TypedSpec pairs = perElementPairs(sh, env);
            if (pairs == null) {
                return null;
            }
            List<TypedSpec> args = com.legend.compiler.spec.typed.Calls.argsOf(sh.root());
            return arm(env).perElement(vectorFamily(sh), pairs, args.get(0).info(), args.get(1).info(), letPrefix,
                    specs, env, rawHook == null ? null : rawHook::apply);
        }
        TypedSide last = null;
        for (TypedSpec element : elements) {
            List<TypedSpec> reduced = com.legend.compiler.spec.VerdictQueries
                    .unrolledElement(specs, letPrefix, sh.lam(), element, rawHook);
            List<TypedSpec> lets = new java.util.ArrayList<>(
                    reduced.subList(0, reduced.size() - 1));
            TypedSpec bareStmt = reduced.get(reduced.size() - 1);
            TypedSide v = adjudicate(bareStmt, lets, specs, env, rawHook);
            if (v == null) {
                throw new com.legend.error.NotImplementedException(
                        "unrolled quantified assert: element verdict not"
                        + " adjudicable for " + calleeFqn(bareStmt));
            }
            last = v;
        }
        return last == null ? ok() : last;
    }

    /** A quantified assert PLANNED for the program road (the test-is-the-product plan's M10c item 5), as the walk
     *  adjudicates it: the VECTOR forms -- the vector contract's {@code assert}/{@code assertFalse} over a row source,
     *  and the simple predicate over any source -- one host check whose one side is the predicate vector the
     *  database computes and whose judgment holds when every row is the wanted truth
     *  ({@link HostJudgment.AllRows}); the UNROLL form -- a literal collection whose lambda carries lets or another
     *  assert family -- one host assert per element, each the walk's reduced statement planned as any root
     *  ({@link #planHost}), to be placed at the statement's place in order. Null where the shape is no quantified
     *  assert, the vector contract's other families ({@code assertEquals}, the tolerance: the walk's row-by-row
     *  judgment), a nested quantification, a runtime collection, or an element's assert the host checks do not plan. */
    sealed interface QuantifiedPlan permits VectorPlan, UnrollPlan {
    }

    record VectorPlan(HostAssert assertion) implements QuantifiedPlan {
    }

    /** One element of an unrolled quantified assert: its reduced root (the walk's {@code unrolledElement}: the lets
     *  before it and the element dissolved into one statement, so {@code before} is empty) and the host assert planned
     *  for it. */
    record Unrolled(TypedSpec root, List<TypedSpec> before, HostAssert assertion) {
    }

    record UnrollPlan(List<Unrolled> elements) implements QuantifiedPlan {
    }

    /** Whether {@code j} is, or holds (an unrolled quantified assert's part), an engine-text judgment: the one whose
     *  appeal reads the user store, which a system-routed body's program cannot carry (M10b's refusal). */
    static boolean holdsEngineText(HostJudgment judgment) {
        HostJudgment j = HostJudgment.core(judgment);
        return j instanceof HostJudgment.EngineText
                || j instanceof HostJudgment.Unrolled u
                        && u.parts().stream().anyMatch(p -> holdsEngineText(p.check().judgment()));
    }

    /** The statement origin the walk's arm marked for OUR text where {@code j} holds an engine-text comparison whose
     *  text is a side the verdict step runs (M10c item 8: {@code SqlTextVerdicts.Collected.oursOrigin}), looked for
     *  through an unroll's parts; null where none (the step runs under its own). An unroll whose parts carry
     *  different marks is refused before it is placed ({@link #oneTextOrigin}). */
    static com.legend.exec.@com.legend.base.Nullable StatementOrigin deferredTextOrigin(HostJudgment judgment) {
        HostJudgment j = HostJudgment.core(judgment);
        if (j instanceof HostJudgment.EngineText e) {
            return e.collected().oursOrigin();
        }
        if (j instanceof HostJudgment.Unrolled u) {
            for (var p : u.parts()) {
                com.legend.exec.StatementOrigin o = deferredTextOrigin(p.check().judgment());
                if (o != null) {
                    return o;
                }
            }
        }
        return null;
    }

    /** Whether one statement origin serves every part of {@code j}: the program road sends a verdict step's sides
     *  under one origin where the walk enters each unrolled part's own mark, so an unroll whose parts' marks differ
     *  (one part's text deferred under the arm's mark, another's not) cannot agree with the walk's census (the M10c
     *  item 8 audit's finding 2). Any other judgment is one part. */
    static boolean oneTextOrigin(HostJudgment judgment) {
        HostJudgment j = HostJudgment.core(judgment);
        if (!(j instanceof HostJudgment.Unrolled u)) {
            return true;
        }
        java.util.Set<java.util.Optional<com.legend.exec.StatementOrigin>> marks = new java.util.HashSet<>();
        for (var p : u.parts()) {
            if (!oneTextOrigin(p.check().judgment())) {
                return false;
            }
            marks.add(java.util.Optional.ofNullable(deferredTextOrigin(p.check().judgment())));
        }
        return marks.size() <= 1;
    }

    /** The unroll's elements as ONE host assert under the statement's name and no family: every element's asks in
     *  order, judged by {@link HostJudgment.Unrolled} element by element (each entered and counted there). */
    static HostAssert composite(TypedSpec bare, UnrollPlan unroll) {
        List<HostSides.Ask> asks = new ArrayList<>();
        List<HostAssert> parts = new ArrayList<>(unroll.elements().size());
        List<Integer> counts = new ArrayList<>(unroll.elements().size());
        for (Unrolled u : unroll.elements()) {
            asks.addAll(u.assertion().check().asks());
            parts.add(u.assertion());
            counts.add(u.assertion().check().asks().size());
        }
        return new HostAssert(listenerName(bare), null, true,
                new HostSides.Check(asks, new HostJudgment.Unrolled(parts, counts)));
    }

    static @com.legend.base.Nullable QuantifiedPlan planQuantified(TypedSpec bare, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        com.legend.compiler.spec.typed.TypedMap qm = quantifiedForm(bare);
        if (qm == null) {
            return null;
        }
        QuantifiedShape sh = quantifiedShape(qm, letPrefix, specs, env, rawHook);
        // a NESTED quantification (the engine's guard idiom) unrolls as the walk unrolls it, recursively: each
        // element's root is itself a quantified assert, planned by this same rule (2026-11-04, the core program)
        if (sh == null) {
            return null;
        }
        SpliceHook hook = rawHook == null ? null : rawHook::apply;
        NativeFn.Verdict family = NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(sh.root()))
                .orElse(null);
        if (sh.vectorPredMap() != null) {
            if (family == NativeFn.Verdict.ASSERT || family == NativeFn.Verdict.ASSERT_FALSE) {
                // the vector contract's assert/assertFalse: the vector already carries the assert's polarity
                // (assertAsPredicate negates an assertFalse's condition), so every row true holds -- the database arm's
                // own reading (allOf(vector, true)); the host's quantifiedRows tested the negated vector against the
                // family's truth and inverted assertFalse until this item's audit
                return new VectorPlan(new HostAssert(listenerName(bare), vectorFamily(sh), true,
                        HostJudge.planAllRows(vectorFamily(sh), sh.vectorPredMap(), true,
                                com.legend.compiler.spec.VerdictQueries.assertMessage(sh.root()), hook)));
            }
            if (family == NativeFn.Verdict.ASSERT_EQUALS || family == NativeFn.Verdict.ASSERT_EQ_WITHIN_TOLERANCE) {
                // row by row on the host, planned (2026-11-04, the core program): the zip's arms or the relation
                // source the sides, the arguments read at planning (HostJudge.planRows); until then the walk's alone
                HostSides.Check rows = HostJudge.planRows(vectorFamily(sh), sh.source(), sh.lam(), sh.root(),
                        letPrefix, hook);
                if (rows != null) {
                    return new VectorPlan(new HostAssert(listenerName(bare), vectorFamily(sh), true, rows));
                }
                // an argument computed per element: the unroll (a literal source) or the per-element form below
            }
        }
        if (sh.simplePredicate()) {
            List<TypedSpec> aargs = com.legend.compiler.spec.typed.Calls.argsOf(sh.root());
            String msg = aargs.size() >= 2 && aargs.get(1) instanceof com.legend.compiler.spec.typed.TypedCString cs
                    ? cs.value() : "Assert failed";
            TypedSpec predMap = com.legend.compiler.spec.VerdictQueries.predicateVector(qm, sh.lam(), aargs.get(0));
            boolean truth = family == NativeFn.Verdict.ASSERT;
            // the family the walk's arm fetched under was the root's FQN, and it counted no family at all (its census
            // row is new with this item, one spelling for both vector forms: the bare name)
            return new VectorPlan(new HostAssert(listenerName(bare), vectorFamily(sh), truth,
                    HostJudge.planAllRows(vectorFamily(sh), predMap, truth, msg, hook)));
        }
        List<TypedSpec> elements = com.legend.compiler.spec.VerdictQueries.unrollElements(sh.source(), letPrefix,
                env.ctx());
        if (elements == null) {
            // any other source: the per-element form, the operands computed by the database (step 5)
            TypedSpec pairs = perElementPairs(sh, env);
            if (pairs == null) {
                return null;
            }
            List<TypedSpec> args = com.legend.compiler.spec.typed.Calls.argsOf(sh.root());
            return new VectorPlan(new HostAssert(listenerName(bare), vectorFamily(sh), true,
                    HostJudge.planPerElement(vectorFamily(sh), pairs, args.get(0).info(), args.get(1).info(), hook)));
        }
        List<Unrolled> out = new ArrayList<>(elements.size());
        for (TypedSpec element : elements) {
            List<TypedSpec> reduced = com.legend.compiler.spec.VerdictQueries.unrolledElement(specs, letPrefix,
                    sh.lam(), element, rawHook);
            List<TypedSpec> lets = new ArrayList<>(reduced.subList(0, reduced.size() - 1));
            TypedSpec root = reduced.get(reduced.size() - 1);
            HostAssert planned = planHost(root, lets, specs, env, rawHook);
            if (planned == null) {
                return null;
            }
            out.add(new Unrolled(root, lets, planned));
        }
        return new UnrollPlan(out);
    }

    private static @com.legend.base.Nullable TypedSide quantified(
            com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook) {
        var lam = qm.mapper();
        if (lam.body().size() != 1) {
            return null;
        }
        TypedSpec root = lam.body().get(0);
        String fqn = calleeFqn(root);
        if (fqn == null || !fqn.startsWith(PKG)) {
            return null;
        }
        NativeFn.Verdict qfn = NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).orElse(null);
        List<TypedSpec> aargs = root instanceof TypedUserCall u ? u.args()
                : ((TypedNativeCall) root).args();
        if (!(qfn == NativeFn.Verdict.ASSERT || qfn == NativeFn.Verdict.ASSERT_FALSE)
                || aargs.isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "quantified assert verdict: only map(f|assert/"
                    + "assertFalse(pred[, message])) is modeled — got '"
                    + fqn + "'/" + aargs.size());
        }
        String msg = aargs.size() >= 2
                && aargs.get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString cs
                ? cs.value() : "Assert failed";
        if (aargs.size() >= 2 && !(aargs.get(1) instanceof
                com.legend.compiler.spec.typed.TypedCString)) {
            throw new com.legend.error.NotImplementedException(
                    "quantified assert verdict: non-literal message"
                    + " expressions are not modeled");
        }
        // the predicate vector, computed in the database — SYNTHESIS is
        // compiler-owned (VerdictQueries, Invariant 7); the judgment
        // below stays host-side (Clause 2c)
        TypedSpec predMap = com.legend.compiler.spec.VerdictQueries
                .predicateVector(qm, lam, aargs.get(0));
        return arm(env).quantified(fqn, predMap, qfn == NativeFn.Verdict.ASSERT, msg, letPrefix, specs, env, hook);
    }

    /** D1 (V7_ARCH_AUDIT 2026-08-28) — THE ONE dual-verdict finisher:
     * the census probe, the verdict of record, and the failure
     * narrative all derive from a single judgment. {@code byteHeld}
     * null = the byte channel declined (already counted); the host
     * lattice judges. {@code hostMessage} is consulted ONLY when the
     * host lattice failed, and must speak then — a silent host failure
     * is a verdict/message divergence and THROWS (the reverted
     * flat-cells attempt's 28-row phantom, made structurally
     * impossible: no arm can print the byte-divergence text for a
     * judgment the byte channel never made, because the probe and the
     * message read the same two booleans). */
    /** The run's judge mode rides its options (ExecuteOptions.JudgeMode):
     * one mode per run, read from the execution environment — never a JVM
     * global. HOST is the verdict of record; DATABASE plans both sides and
     * lets the database return the verdict row. */
    static boolean databaseMode(BodyDriver.ExecEnv env) {
        return env.options().judgeMode() == ExecuteOptions.JudgeMode.DATABASE;
    }

    /** THE ONE DISPATCH (cleanup move 2b, 2026-09-21): the run's judge mode names the
     * arm; the router classifies and hands the sides over, nowhere else forks. */
    static VerdictArm arm(BodyDriver.ExecEnv env) {
        return switch (env.options().judgeMode()) {
            case HOST -> HostJudge.ARM;
            case DATABASE -> DatabaseJudge.ARM;
            case BOTH -> AgreementArm.ARM;
        };
    }

    /** DATABASE mode (leg 3.1, docs/DATABASE_MODE_HOMEWORK §4b): both
     * sides are PLANNED (never executed on their own), composed by
     * {@link com.legend.lowering.VerdictSql} into ONE statement, and the
     * database returns the verdict row: {@code __verdict} (never NULL),
     * the two framed canons as the evidence, and {@code __unjudged} when
     * the statement could not decide. A side the wrap declines, a
     * non-SQL side, a grid side (3.1b), a multi-candidate Number side or
     * an enum on the literal channel is UNJUDGED — the assert FAILS with
     * the reason and the census counts it; no host judgment is consulted. */
    /** A grid side's statement facts from its schema: the declared-Float
     * columns (the 2-ULP leniency) and, under toCSV's grammar, the String
     * columns whose empty cell and NULL print alike. */
    /** A grid side's schema by EFFECTIVE kind (a Number over a DOUBLE wire is a Float). */
    static com.legend.compiler.element.type.Type.@com.legend.base.Nullable RelationType effectiveSchema(
            BodyDriver.WrappedSide side) {
        var d = com.legend.compiler.element.type.Type.schemaView(side.shapeInfo().type());
        return d == null || d.columns().size() > side.plan().outputs().size() ? d : com.legend.compiler
                .spec.VerdictQueries.wireDecidedKinds(d, side.plan().outputs().subList(0, d.columns().size()));
    }

    /** A verdict row's evidence column for a message: at most 600
     * characters (a grid text drowns the diagnosis past that). */
    static String excerpt(@com.legend.base.Nullable Object evidence) {
        String s = String.valueOf(evidence);
        return s.length() <= 600 ? s : s.substring(0, 600) + "…(" + s.length() + " chars)";
    }

    /** A side's per-ROW canon texts via the grid policy owner: a
     * grid side reads its harvested row canons; a value peer frames
     * rows from its literal-channel element canons ({@link
     * com.legend.judge.TdsCompare} owns every rule and decline). */
    static @com.legend.base.Nullable List<String> sideRowCanons(
            HostJudge.SideFetch side, int width, boolean isExpected) {
        return side.grid() != null
                ? com.legend.judge.TdsCompare.tdsRowCanons(side.rider())
                : com.legend.judge.TdsCompare.peerRowCanons(side.rider(),
                        side.values().size(), width, isExpected);
    }

    /** A side's per-CELL canon texts (the sameElements view), via the
     * grid policy owner. */
    static @com.legend.base.Nullable List<String> sideCellCanons(
            HostJudge.SideFetch side, boolean isExpected) {
        return side.grid() != null
                ? com.legend.judge.TdsCompare.tdsCellCanons(side.rider())
                : com.legend.judge.TdsCompare.peerElementCanons(
                        side.rider(), side.values().size(), isExpected);
    }

    /** Whether a side is STATICALLY table-shaped (its declared result
     * shape — the same fact the executor's canon routing reads). */
    static boolean tabularShaped(TypedSpec s) {
        return com.legend.plan.ResultShape.of(s)
                == com.legend.plan.ResultShape.TABULAR;
    }

    /** A bare {@code .rows} view stamp (row collection — bare struct,
     * many multiplicity), through let bindings. */
    static boolean bareRowStamp(TypedSpec s0,
            List<TypedSpec> lets) {
        TypedSpec s = chaseLets(s0, lets);
        return s.info().type()
                instanceof com.legend.compiler.element.type.Type.RelationType
                && s.info().multiplicity().isMany();
    }

    /** A wrapped table stamp ({@code Relation<schema>}), through let
     * bindings. */
    static boolean wrappedRelationStamp(TypedSpec s0,
            List<TypedSpec> lets) {
        return com.legend.compiler.element.type.Type.isRelation(
                chaseLets(s0, lets).info().type());
    }

    // ── D3 (batch-2 slice 2): the GOLDEN GRID/ORDER conventions move
    // into verdict construction — the ORDER VIEW of a side, the
    // rendered-text forms, and the grid-pair route. The comparison
    // POLICIES stay with their one production owner (TdsCompare).



    /** The referee's gates for a verified chain, derived ONCE from the
     * typed chain by the arm that owns the verdict (Phase 0.5): the
     * extent-subset fact, the ORDER VIEW (ends in a sort = order is
     * contract), and the tail-most sort's keys. Typed-tree navigation
     * only — nothing evaluated. */
    static com.legend.judge.SqlReplayReferee.ReplayFacts replayFacts(
            TypedSpec chain, List<TypedSpec> letPrefix) {
        boolean ordered = com.legend.compiler.spec.OrderView.of(chain, letPrefix) == com.legend.compiler.spec.OrderView.SORTED;
        return new com.legend.judge.SqlReplayReferee.ReplayFacts(
                com.legend.compiler.spec.VerdictQueries.extentSubset(chain),
                ordered,
                ordered ? sortKeys(chain, letPrefix, new java.util.HashSet<>()) : null);
    }

    /** The rows read WITHOUT its tail page (batch 0.5b): the same read with
     * the first page node reached through order-preserving tails
     * ({@code TypedLimit} / {@code TypedDrop} / {@code TypedSlice}, the
     * typed forms of limit / take / drop / slice) replaced by its source —
     * OUR unpaged population for the referee's page-membership verdict.
     * Null when the chain carries no page at its tail. Typed-tree
     * navigation and rebuild ({@code withChildren}), nothing evaluated. */
    static @com.legend.base.Nullable TypedSpec unpagedRead(TypedSpec read) {
        if (read instanceof com.legend.compiler.spec.typed.TypedLimit l) {
            return l.source();
        }
        if (read instanceof com.legend.compiler.spec.typed.TypedDrop d) {
            return d.source();
        }
        if (read instanceof com.legend.compiler.spec.typed.TypedSlice sl) {
            return sl.source();
        }
        boolean wrapper = read instanceof com.legend.compiler.spec.typed.TypedFrom
                || read instanceof com.legend.compiler.spec.typed.TypedFilter
                || read instanceof com.legend.compiler.spec.typed.TypedProject
                || read instanceof com.legend.compiler.spec.typed.TypedSelect
                || read instanceof com.legend.compiler.spec.typed.TypedRename
                || read instanceof com.legend.compiler.spec.typed.TypedDistinct
                || read instanceof com.legend.compiler.spec.typed.TypedSort
                || read instanceof com.legend.compiler.spec.typed.TypedSortBy
                || read instanceof com.legend.compiler.spec.typed.TypedMap
                || read instanceof com.legend.compiler.spec.typed.TypedPropertyAccess
                || read instanceof com.legend.compiler.spec.typed.TypedCast
                || read instanceof com.legend.compiler.spec.typed.TypedNavigate;
        if (read instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            wrapper = com.legend.compiler.spec.OrderView.ORDER_PRESERVING.contains(fqn) && !c.args().isEmpty();
        }
        if (!wrapper || read.children().isEmpty()) {
            return null;
        }
        TypedSpec inner = unpagedRead(read.children().get(0));
        if (inner == null) {
            return null;
        }
        List<TypedSpec> kids = new java.util.ArrayList<>(read.children());
        kids.set(0, inner);
        return read.withChildren(kids);
    }

    /** The key names of the sort NEAREST THE TAIL (the engine's own
     * last-sort-wins semantics), through the same order-preserving tails
     * {@link #orderView} descends; null = underivable (a computed key,
     * a native sort spelling, a value the compared output cannot carry). */
    private static @com.legend.base.Nullable List<String> sortKeys(TypedSpec s,
            List<TypedSpec> lets, java.util.Set<String> seen) {
        if (s instanceof com.legend.compiler.spec.typed.TypedSort so) {
            List<String> keys = new java.util.ArrayList<>();
            for (var k : so.keys()) {
                keys.add(k.column());
            }
            return keys.isEmpty() ? null : keys;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedSortBy sb) {
            if (sb.keyAlias() != null) {
                return List.of(sb.keyAlias());
            }
            List<TypedSpec> body = sb.key().body();
            return body.size() == 1 && body.get(0)
                    instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    ? List.of(pa.property()) : null;
        }
        if (s instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            if (com.legend.compiler.spec.OrderView.EXECUTE_FRAMES.contains(fqn) && !c.args().isEmpty()
                    && c.args().get(0) instanceof com.legend.compiler.spec.typed.TypedLambda lam
                    && !lam.body().isEmpty()) {
                return sortKeys(lam.body().get(lam.body().size() - 1), lets, seen);
            }
            return com.legend.compiler.spec.OrderView.ORDER_PRESERVING.contains(fqn)
                    && !c.args().isEmpty() ? sortKeys(c.args().get(0), lets, seen) : null;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            if (!seen.add(v.name())) {
                return null;
            }
            com.legend.compiler.spec.typed.TypedLet l =
                    com.legend.compiler.spec.typed.Lets.binding(lets, v.name());
            return l == null ? null : sortKeys(l.value(), lets, seen);
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedFilter
                || s instanceof com.legend.compiler.spec.typed.TypedProject
                || s instanceof com.legend.compiler.spec.typed.TypedSelect
                || s instanceof com.legend.compiler.spec.typed.TypedRename
                || s instanceof com.legend.compiler.spec.typed.TypedDistinct
                || s instanceof com.legend.compiler.spec.typed.TypedLimit
                || s instanceof com.legend.compiler.spec.typed.TypedDrop
                || s instanceof com.legend.compiler.spec.typed.TypedSlice
                || s instanceof com.legend.compiler.spec.typed.TypedMap
                || s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess
                || s instanceof com.legend.compiler.spec.typed.TypedCast
                || s instanceof com.legend.compiler.spec.typed.TypedFrom
                || s instanceof com.legend.compiler.spec.typed.TypedNavigate
                || s instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess) {
            List<TypedSpec> ch = s.children();
            return ch.isEmpty() ? null : sortKeys(ch.get(0), lets, seen);
        }
        return null;
    }

    private static final String FQ_TO_STRING =
            com.legend.compiler.element.type.PlatformTypes.TO_STRING;

    private static final String FQ_REPLACE =
            com.legend.compiler.element.type.PlatformTypes.STRING_REPLACE;

    private static final String FQ_MAKE_STRING =
            com.legend.compiler.element.type.PlatformTypes.STRING_MAKE_STRING;

    private static final String FQ_JOIN_STRINGS =
            com.legend.compiler.element.type.PlatformTypes.STRING_JOIN_STRINGS;

    static TypedSpec chaseLets(TypedSpec s0, List<TypedSpec> lets) {
        TypedSpec s = s0;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (s instanceof com.legend.compiler.spec.typed.TypedVariable v
                && seen.add(v.name())) {
            TypedSpec bound = null;
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof
                        com.legend.compiler.spec.typed.TypedLet l
                        && l.name().equals(v.name())) {
                    bound = l.value();
                    break;
                }
            }
            if (bound == null) {
                return s;
            }
            s = bound;
        }
        return s;
    }

    /** The D3 RENDERED-TEXT verdict, or null when the pair is not the
     * shape (exactly one side a render form, both sides one string).
     * {@code orderedForm} false = the sameElements view (token/line
     * multiset regardless of the chain's sort). */
    static @com.legend.base.Nullable TypedSide renderedArm(
            String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook, boolean orderedForm) {
        String eForm = renderForm(args.get(0), letPrefix);
        String aForm = renderForm(args.get(1), letPrefix);
        // BOTH-RENDERED same-form pairs qualify too (two renders of one
        // unsorted query — pure guarantees the row MULTISET; each
        // render freezes its own execution's incident order, so a byte
        // compare was a coin flip: union testProjectThroughAsso's
        // toCSV-vs-toCSV flicker). Mismatched forms fall through.
        if ((eForm == null && aForm == null)
                || (eForm != null && aForm != null
                        && !eForm.equals(aForm))) {
            return null;
        }
        String form = aForm != null ? aForm
                : java.util.Objects.requireNonNull(eForm);
        TypedSpec rendered = aForm != null ? args.get(1) : args.get(0);
        return arm(env).rendered(name, wantEqual, args, form, rendered, eForm, aForm, orderedForm,
                        letPrefix, specs, env, hook);
    }

    /** One-line first-difference sketch of two rendered texts (failure
     * message position — the full texts drown the diagnosis). */
    static String firstTextDiff(String e, String a) {
        String[] el = e.split("\n", -1);
        String[] al = a.split("\n", -1);
        if (el.length != al.length) {
            return "line-count " + el.length + " != " + al.length
                    + " (expected first line: " + el[0] + ")";
        }
        for (int i = 0; i < el.length; i++) {
            if (!el[i].equals(al[i])) {
                return "line " + i + ": expected <" + el[i] + "> got <"
                        + al[i] + ">";
            }
        }
        return "texts differ only in leniency-adjudicated cells";
    }

    /** Database mode's rendered-text road (bucket 8): both sides rendered →
     * the two values as a multiset (two executions of one query); one side
     * rendered → its value against the golden brought to rows, ordered only
     * when the chain ends in a sort and the assert is ordered. */
    static TypedSide renderedValueVerdict(String name, boolean wantEqual,
            List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable SpliceHook hook,
            boolean orderedForm) {
        java.util.function.UnaryOperator<TypedSpec> chase = s -> chaseLets(s, letPrefix);
        var re = com.legend.compiler.spec.VerdictQueries.renderedSide(args.get(0), chase);
        var ra = com.legend.compiler.spec.VerdictQueries.renderedSide(args.get(1), chase);
        if (re != null && ra != null) {
            com.legend.sql.SqlQuery et = DatabaseJudge.textRow(args.get(0), letPrefix, specs, env, hook);
            com.legend.sql.SqlQuery at = DatabaseJudge.textRow(args.get(1), letPrefix, specs, env, hook);
            return textFirst(name, wantEqual, et != null && at != null
                    ? com.legend.lowering.VerdictSql.textEquals(et, at) : null, env,
                    () -> DatabaseJudge.databaseVerdict(name, wantEqual, re.value(), ra.value(), letPrefix, specs,
                            env, hook, true, false, false, true));
        }
        boolean goldenIsExpected = ra != null;
        var r = goldenIsExpected ? ra : re;
        if (r == null) {
            return DatabaseJudge.unjudged(name, "rendered-text: no side is a render the grammar names");
        }
        if (r.grid() && com.legend.compiler.element.type.Type.schemaView(r.value().info().type())
                instanceof com.legend.compiler.element.type.Type.RelationType late && late.isLateBound()) {
            // a RAW GRID's text: its columns exist only when the plan runs (the planner-driver seam program's S3c,
            // 2026-10-30), so the golden is not brought to rows; the two texts are compared as the strings they are,
            // as legend-pure's assertEquals compares them (the render a keyed statement, Render.gridCsv)
            return DatabaseJudge.databaseVerdict(name, wantEqual, args.get(0), args.get(1), letPrefix, specs, env,
                    hook, false, false);
        }
        String text = com.legend.compiler.spec.VerdictQueries.goldenText(
                goldenIsExpected ? args.get(0) : args.get(1), chase);
        if (text == null) {
            return DatabaseJudge.unjudged(name, "rendered-text: the golden is not a string constant");
        }
        com.legend.sql.SqlQuery rendered = DatabaseJudge.textRow(goldenIsExpected ? args.get(1) : args.get(0),
                letPrefix, specs, env, hook);
        var rendering = r;
        return textFirst(name, wantEqual, rendered != null
                ? com.legend.lowering.VerdictSql.textEquals(text, rendered) : null, env,
                () -> renderedRows(name, wantEqual, args, letPrefix, specs, env, hook, orderedForm, goldenIsExpected,
                        rendering, text));
    }

    /** {@link #renderedValueVerdict}'s FALLBACK: the golden brought to rows by the render's own grammar and judged as
     *  rows -- an unsorted query's as a multiset (S131), a Float within 2 ULP (S132) -- run where the two texts
     *  differ. */
    private static TypedSide renderedRows(String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable SpliceHook hook, boolean orderedForm, boolean goldenIsExpected,
            com.legend.compiler.spec.VerdictQueries.RenderedSide r, String text) {
        com.legend.compiler.element.type.Type.RelationType schema = null;
        if (r.grid() && r.restrictTo() != null) {
            // columnValues: the relation restricted to the one column, minted
            // from the PLANNED schema (the static type may not carry it)
            DatabaseJudge.SideRows whole = DatabaseJudge.planSide(r.value(), false, false, letPrefix, specs, env, hook);
            if (whole.why() != null) {
                return DatabaseJudge.unjudged(name, "rendered-text side: " + whole.why());
            }
            TypedSpec one = com.legend.compiler.spec.VerdictQueries.restrictedTo(r.value(),
                    com.legend.compiler.element.type.Type.schemaView(
                            java.util.Objects.requireNonNull(whole.side()).shapeInfo().type()),
                    r.restrictTo());
            if (one == null) {
                return DatabaseJudge.unjudged(name, "rendered-text: column '" + r.restrictTo() + "' is not declared");
            }
            r = new com.legend.compiler.spec.VerdictQueries.RenderedSide(one, r.grammar(), true);
        }
        if (r.grid()) {
            // the PLANNED side's schema (its shape info) is the grid verdict's
            // own width and kinds — the static type can lag it (validate's
            // late-bound ID column)
            DatabaseJudge.SideRows planned = DatabaseJudge.planSide(r.value(), false, false, letPrefix, specs, env, hook);
            if (planned.why() != null) {
                return DatabaseJudge.unjudged(name, "rendered-text side: " + planned.why());
            }
            var side = java.util.Objects.requireNonNull(planned.side());
            var pr = java.util.Objects.requireNonNull(planned.rider());   // width recorded at wrap
            List<com.legend.sql.OutputCol> data = pr.dataPrefix(side.plan().outputs());
            schema = com.legend.compiler.element.type.Type.schemaView(side.shapeInfo().type());
            if (schema == null || schema.columns().isEmpty() || !schema.dynamicColumns().isEmpty()) {
                // late-bound (a raw grid framed by the database's columns): the slots type it
                schema = com.legend.compiler.spec.VerdictQueries.wireSchema(data);
            } else {
                // a wire-decided declaration's cell is what the wire printed
                schema = com.legend.compiler.spec.VerdictQueries.wireDecidedKinds(schema, data);
            }
        }
        var parsed = com.legend.compiler.spec.VerdictQueries.parseRendered(text, r.grammar(),
                schema, r.grid() ? null : r.value().info().type());
        if (parsed.headerMismatch()) {
            com.legend.judge.CanonicalDivergence.sqlJudgedInDatabase(name);
            return fail(name + ": " + parsed.reason());   // a static verdict
        }
        if (parsed.literal() == null) {
            return DatabaseJudge.unjudged(name, java.util.Objects.requireNonNull(parsed.reason()));
        }
        boolean multiset = !orderedForm
                || com.legend.compiler.spec.OrderView.of(r.value(), letPrefix) != com.legend.compiler.spec.OrderView.SORTED;
        boolean csv = r.grammar() instanceof com.legend.compiler.spec.VerdictQueries.RenderGrammar.Csv;
        return goldenIsExpected
                ? DatabaseJudge.databaseVerdict(name, wantEqual, parsed.literal(), r.value(), letPrefix, specs,
                        env, hook, multiset, false, csv, true)
                : DatabaseJudge.databaseVerdict(name, wantEqual, r.value(), parsed.literal(), letPrefix, specs,
                        env, hook, multiset, false, csv, true);
    }

    /** GENERIC FIRST (2026-11-04, the core program: every assert on its own merit generically, a ruled fallback only
     *  after it fails): a rendered text's equality is its two strings' ({@code textRow}, the database computing the
     *  render), and {@code rows} -- the ruled line and row comparison -- decides only where they differ: under the
     *  batch as the row's fallback ({@code VerdictBatch.orElse}), else run now on a failed text. {@code textRow} null
     *  (a side the database judge cannot read as a String) or the negated family: {@code rows} alone. */
    private static TypedSide textFirst(String name, boolean wantEqual, com.legend.sql.@com.legend.base.Nullable SqlQuery textRow,
            BodyDriver.ExecEnv env, java.util.function.Supplier<TypedSide> rows) {
        if (!wantEqual || textRow == null) {
            return rows.get();
        }
        com.legend.judge.VerdictBatch batch = env.verdictBatch();
        if (batch != null && batch.active()) {
            batch.defer(name, true, textRow, env.session());
            batch.orElse(rows::get);
            return ok();
        }
        return DatabaseJudge.holdsNow(name, textRow, env) ? ok() : rows.get();
    }

    /** The RENDERED-TEXT form of a side, or null: toCSV → CSVTEXT,
     * toString over a relation → TDSTEXT, toCSV->replace('\n', sep) →
     * CSVJOIN:sep (the calendar family's one-line spelling), and a
     * makeString/joinStrings join over an INCIDENTAL-order chain →
     * CSVJOIN:sep (token multiset — sep-joined DB arrival order).
     * The comparison policy is {@link com.legend.judge.TdsCompare
     * #renderedText} — the one owner, probed by its own R1b census. */
    static @com.legend.base.Nullable String renderForm(TypedSpec s0,
            List<TypedSpec> lets) {
        TypedSpec s = chaseLets(s0, lets);
        if (s instanceof TypedNativeCall rep
                && FQ_REPLACE.equals(rep.callee().qualifiedName())
                && rep.args().size() == 3
                && chaseLets(rep.args().get(0), lets)
                        instanceof TypedNativeCall csv
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(csv.callee().qualifiedName())
                && rep.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString from
                && "\n".equals(from.value())
                && rep.args().get(2) instanceof
                        com.legend.compiler.spec.typed.TypedCString to) {
            return "CSVJOIN:" + to.value();
        }
        if (s instanceof TypedNativeCall csv2
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(csv2.callee().qualifiedName())
                && csv2.args().size() == 1) {
            return "CSVTEXT";
        }
        if (s instanceof TypedNativeCall ts
                && FQ_TO_STRING.equals(ts.callee().qualifiedName())
                && ts.args().size() == 1
                && com.legend.compiler.element.type.Type.isRelation(
                        ts.args().get(0).info().type())) {
            return "TDSTEXT";
        }
        if (s instanceof TypedNativeCall j
                && (FQ_MAKE_STRING.equals(j.callee().qualifiedName())
                        || FQ_JOIN_STRINGS.equals(j.callee().qualifiedName()))
                && j.args().size() == 2
                && j.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString sep
                && com.legend.compiler.spec.OrderView.of(j.args().get(0), lets) == com.legend.compiler.spec.OrderView.INCIDENTAL) {
            return "CSVJOIN:" + sep.value();
        }
        return null;
    }

    /** The cluster-34 envelope READ SHAPE: {@code $r.values} (through
     * optional toOne/first/at(0) peels) over a binding OUTSIDE the let
     * prefix — an execution frame; the TDS envelope is ONE carrier. */
    static boolean envelopeValuesRead(TypedSpec arg,
            List<TypedSpec> letPrefix) {
        TypedSpec s = arg;
        while (s instanceof TypedNativeCall c && !c.args().isEmpty()) {
            String fqn = c.callee().qualifiedName();
            if (fqn.equals(com.legend.builtin.Pure.TO_ONE__T_MANY.qualifiedName())
                    || fqn.equals(com.legend.compiler.element.type.PlatformTypes.FIRST)
                    || (fqn.equals(com.legend.builtin.Pure.AT__T_MANY__INTEGER_1.qualifiedName())
                            && c.args().size() == 2
                            && c.args().get(1) instanceof
                                    com.legend.compiler.spec.typed
                                            .TypedCInteger ci
                            && ci.value().longValue() == 0)) {
                s = c.args().get(0);
                continue;
            }
            break;
        }
        if (!(s instanceof com.legend.compiler.spec.typed
                .TypedPropertyAccess pa
                && pa.property().equals("values")
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v)) {
            return false;
        }
        // an ordinary let, not a frame
        return !com.legend.compiler.spec.typed.Lets.binds(letPrefix, v.name());
    }

    /** The {@code $exp->forAll(e|$act->contains($e))} SUBSET shape:
     * {expected, actual} sources, or null when not this idiom (the
     * predicate must be a contains of the forAll binder itself). */
    static TypedSpec @com.legend.base.Nullable [] forAllContains(
            TypedSpec a0) {
        if (a0 instanceof TypedNativeCall fa
                && "meta::pure::functions::collection::forAll"
                        .equals(fa.callee().qualifiedName())
                && fa.args().size() == 2
                && fa.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedLambda lam
                && lam.parameters().size() == 1
                && lam.body().size() == 1
                && lam.body().get(0) instanceof TypedNativeCall cont
                && "meta::pure::functions::collection::contains"
                        .equals(cont.callee().qualifiedName())
                && cont.args().size() == 2
                && cont.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedVariable ev
                && ev.name().equals(lam.parameters().get(0))) {
            return new TypedSpec[] {fa.args().get(0), cont.args().get(0)};
        }
        return null;
    }

    /** The root's callee — except {@code assertError}, whose arm is the host's own
     * (a non-callee here keeps it off the verdict routes). */
    static @com.legend.base.Nullable String calleeFqn(TypedSpec bare) {
        String fqn = com.legend.compiler.spec.typed.Calls.calleeOf(bare);
        return com.legend.compiler.element.type.PlatformTypes.ASSERT_ERROR.equals(fqn) ? null : fqn;
    }

    /** The engine golden's null spelling anywhere in the EXPECTED wire
     * values (scalar cells, instance properties, nested lists). */
    static boolean containsTdsNullSentinel(
            @com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> false;
            case String s -> "TDSNull".equals(s);
            case List<?> l -> l.stream()
                    .anyMatch(AssertVerdicts::containsTdsNullSentinel);
            case java.util.Map<?, ?> m -> m.values().stream()
                    .anyMatch(AssertVerdicts::containsTdsNullSentinel);
            default -> false;
        };
    }

    static boolean isAnyStamped(TypedSpec s) {
        return s.info().type() instanceof
                com.legend.compiler.element.type.Type.ClassType ct
                && com.legend.compiler.element.type.PlatformTypes.isAny(ct);
    }

    static boolean containsTreeMarker(
            @com.legend.base.Nullable String text) {
        return text != null && text.contains(
                com.legend.lowering.CanonicalRenderSql.TREE_MARKER);
    }

    static List<Object> concat(List<Object> a, List<Object> b) {
        List<Object> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a);
        out.addAll(b);
        return out;
    }

    /** F13 v1 exclusion scan: any KEYLESS model-class constructor (or
     * copy) under a lambda anywhere in the verdict's scope — the site
     * id cannot distinguish per-element evaluations. A plain
     * containment walk over {@code children()} (no shadow concerns —
     * this detects presence, it never resolves variables). */
    static boolean keylessCtorUnderLambda(List<TypedSpec> roots,
            BodyDriver.ExecEnv env) {
        for (TypedSpec r : roots) {
            if (scanKeylessCtor(r, false, env)) {
                return true;
            }
        }
        return false;
    }

    private static boolean scanKeylessCtor(TypedSpec n, boolean inLambda,
            BodyDriver.ExecEnv env) {
        if (inLambda) {
            String fqn = n instanceof
                    com.legend.compiler.spec.typed.TypedNewInstance ni
                    ? ni.classFqn()
                    : n instanceof
                            com.legend.compiler.spec.typed.TypedCopyInstance cp
                            ? cp.classFqn() : null;
            if (fqn != null && env.ctx().findClass(fqn).isPresent()
                    && com.legend.compiler.element.EqualityKeys
                            .resolve(env.ctx(), fqn) == null) {
                return true;
            }
        }
        boolean in = inLambda
                || n instanceof com.legend.compiler.spec.typed.TypedLambda;
        for (TypedSpec k : n.children()) {
            if (scanKeylessCtor(k, in, env)) {
                return true;
            }
        }
        return false;
    }

    /** True iff both sides are same-length all-finite-Double vectors
     * whose pairs each hold under the lattice's declared 2-ULP arm —
     * PureAsserts OWNS the tolerance, this only vectorizes it. */
    static boolean withinDeclaredUlp(List<Object> eVals,
            List<Object> aVals, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        return com.legend.judge.Equality.differByLeniencyOnly(
                com.legend.judge.Equality.Typed.all(eVals, null),
                com.legend.judge.Equality.Typed.all(aVals, null), rules);
    }

    static String typeName(TypedSpec spec) {
        var t = spec.info().type();
        return t.getClass().getSimpleName() + ":" + t;
    }

    /** The kind class of a SIDE, node-aware: a type reference written as a
     * value ({@code String}, {@code [Car, Bicycle]}) is stamped as its
     * prototype by the typer (Typer.typeRef) but IS a type value; a
     * metamodel type classifier or a tracked element class is the
     * name-valued kind. Everything else by its type. */
    static @com.legend.base.Nullable KindClass kindKey(TypedSpec spec,
            List<TypedSpec> letPrefix, BodyDriver.ExecEnv env) {
        TypedSpec s = chaseLets(spec, letPrefix);
        if (isTypeValueNode(s)) {
            return KindClass.Primitive.TYPE;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedCollection c
                && !c.elements().isEmpty()
                && c.elements().stream().allMatch(AssertVerdicts::isTypeValueNode)) {
            return KindClass.Primitive.TYPE;
        }
        String fqn = com.legend.compiler.element.EqualityKeys.fqnOf(spec.info().type());
        if (fqn != null && com.legend.compiler.element.type.PlatformTypes.isTypeClassifier(fqn)) {
            return KindClass.Primitive.TYPE;
        }
        if (fqn != null && env.ctx().tracksClassifier(fqn)) {
            return new KindClass.Element(fqn);
        }
        return KindClass.of(spec.info().type());
    }

    /** A type written as a VALUE: a primitive type reference ({@code
     * String} — TypedTypeRef, stamped as its prototype) or a class
     * reference ({@code Car} — a packageable ref typed {@code Class<Car>}). */
    private static boolean isTypeValueNode(TypedSpec s) {
        return s instanceof com.legend.compiler.spec.typed.TypedTypeRef
                || (s instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr
                        && pr.info().type() instanceof com.legend.compiler.element.type.Type.GenericType g
                        && g.rawFqn().equals(
                                com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS));
    }

    // X5's instance-key rule (the pair's shared key tree: both stamps the SAME keyed class, a side declared wider
    // counting as the other's class when every value on its wire is of it) is HostJudgment.Keys since M8a
    // (2026-11-01): the model read at planning, the wire read at judging.

    /** A class VALUE riding a JSON slot inside a struct (a polymorphic field: {@code parameters :
     *  RelationalOperationElement[*]}) arrives as object or array text -- the field's declared kind says it is a
     *  structure, so it decodes to the map or list the verdict's key restriction reads (WORLD_MAP §4: the value carries
     *  its class as {@code __type}); any other value as it is. */
    static @com.legend.base.Nullable Object structured(@com.legend.base.Nullable Object v) {
        if (v instanceof String s) {
            String t = s.trim();
            if (t.startsWith("{") || t.startsWith("[")) {
                return com.legend.sql.Json.parse(t);
            }
        }
        return v;
    }



    /** A JSON assert side's ONE document text: a GRAPH result's JSON
     * IS the {@code String[1]} (a serialize execute's {@code .values}
     * — leg 2 made its stamp String[1], and the DB-built envelope is
     * the value); any other result must decode to one string. Null =
     * not this shape (generic path, loud downstream). The side read already ({@code HostSides}, S2f). */
    static @com.legend.base.Nullable String jsonSideTextOf(TypedSide r) {
        if (r instanceof TypedSide.Graph g) {
            return g.json();
        }
        List<Object> v = decodeSide(r);
        return v.size() == 1 && v.get(0) instanceof String s ? s : null;
    }

    static List<Object> decodeSide(
            @com.legend.base.Nullable TypedSide r) {
        List<Object> side = decodeSideValues(r);
        // V7 §5-1 instrument: the side-size histogram (VALUES-literal
        // cost bracket for V12's fused-verdict design) — measurement
        // only, fed from the one place every side already flows through
        com.legend.judge.CanonicalDivergence.v7SideRows(side.size());
        return side;
    }

    static List<Object> decodeSideValues(
            @com.legend.base.Nullable TypedSide r) {
        return switch (r) {
            case null -> new ArrayList<>();
            case TypedSide.Scalar s -> {
                // (a list wire arriving as one JDBC array cell is decoded to a
                // Collection at the exec seam — no JDBC carrier reaches the router)
                List<Object> out = new ArrayList<>(1);
                if (s.value() != null) {
                    out.add(s.value());
                }
                yield out;
            }
            case TypedSide.Collection c -> c.values();
            // D4: a GRAPH side's values are the DATABASE-built JSON
            // array's elements (the harness Eval convention moved to
            // the owner) — parsed structures; the lattice/JsonCompare
            // judge them, never raw json text
            case TypedSide.Graph g -> {
                Object p = com.legend.sql.Json.parse(g.json());
                yield p instanceof List<?> l ? new ArrayList<Object>(l)
                        : new ArrayList<>(List.of(p));
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "assert verdict over a " + r.getClass().getSimpleName()
                    + " side — grid asserts stay with their own"
                    + " compare owners");
        };
    }

    static Object one(List<Object> side, String what) {
        if (side.size() != 1) {
            throw new IllegalStateException(what + " must be one value,"
                    + " got " + side.size());
        }
        return side.get(0);
    }

    /** The literal type argument's name: @Type annotation
     * ({@code TypedTypeRef}) or a bare reference in value position
     * ({@code TypedPackageableRef}); null = not literal (fall through,
     * the body inlines and walls on its own terms). */
    static @com.legend.base.Nullable String typeRefName(TypedSpec t) {
        return switch (t) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr ->
                    tr.target().typeName();
            case com.legend.compiler.spec.typed.TypedPackageableRef pr ->
                    pr.fullPath();
            default -> null;
        };
    }

    /** Row-major cell stream of a tabular HostJudge.frame (the cell-zip input). */
    /** Raw cell flatten — cells arrive ALREADY DECODED (the engine's
     * value-read conventions ride the FETCH: wrapTdsCanon conforms the
     * plan in SQL and the executor's label-driven unwrap delivers; the
     * former host-twin decode, valueRead, is deleted — Java-eviction
     * close of the disagree-9 burn). */
    static List<Object> cells(TypedSide.Tabular t) {
        List<Object> out = new java.util.ArrayList<>();
        for (com.legend.judge.Row r : t.rows()) {
            out.addAll(r.values());
        }
        return out;
    }

    static TypedSide ok() {
        return new TypedSide.Scalar(Boolean.TRUE,
                com.legend.compiler.element.type.Type.Primitive.BOOLEAN);
    }

    static TypedSide fail(String message) {
        // the seam: verdicts speak the platform vocabulary
        throw new com.legend.error.AssertFailed(message);
    }
}
