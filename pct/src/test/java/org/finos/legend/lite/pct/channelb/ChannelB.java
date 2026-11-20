// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import com.legend.Compiler;
import com.legend.TestExecution;
import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.model.FunctionDefinition;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * PCT CHANNEL B (One-Platform Plan Phase 4): OUR parser compiles the
 * {@code PCT.test} functions, OUR platform executes them — no
 * reference-interpreter object ever appears. The adapter is the
 * IDENTITY: {@code $f->eval(|expr)} β-reduces to {@code expr}, because
 * this platform IS the executor; the whole test body then compiles and
 * runs as ordinary pure — asserts included, which lower to
 * {@code CASE WHEN cond THEN TRUE ELSE error(msg)} and run IN THE
 * DATABASE (tenet #1).
 *
 * <p>Outcomes are three-bucket honest (the plan's addendum #6):
 * {@code PASS}/{@code FAIL} feed the AGREE/WIRE-BUG diff against
 * channel A; {@code DECLINED} names tests whose shape channel B does
 * not run (reference-only reflection, non-identity adapter spellings) —
 * a reason, never a silent skip.
 */
public final class ChannelB {
    /** A platform-independent path string: '/' separators always. A Path in a
     *  concatenation converts with the PLATFORM separator, so an id built that
     *  way differs on Windows (census 2026-09-09). */
    private static String slash(java.nio.file.Path p) {
        return p.toString().replace(java.io.File.separatorChar, '/');
    }


    private ChannelB() {
    }

    /** A test's outcome: its {@code detail} for display (a long middle clipped, {@link #first}) and its {@code full}
     *  text, the database's whole error, which the ledger matches (2026-11-15, Databricks round 53: a clipped middle
     *  removed the ledger's text; shortening is presentation and never decides a match). */
    public record Outcome(String testFqn, Status status, String detail, String full) {
        public Outcome(String testFqn, Status status, String detail) {
            this(testFqn, status, detail, detail);
        }
    }

    public enum Status { PASS, FAIL, DECLINED, ERROR }

    /** Run every {@code <<PCT.test>>} function found under the scope
     * directories. The MODEL is the whole {@code modelRoot} tree (the
     * platform sources — shared test models, the assert family, the
     * function definitions the tests exercise; files that do not parse
     * are collected as walls, never silently dropped); DISCOVERY is
     * scoped. One session per test on the lane's database (PctBackend: a
     * fresh in-memory DuckDB by default — isolation, PCT tests assume a
     * fresh world). */
    public static List<Outcome> run(Path modelRoot, List<Path> scopeDirs)
            throws IOException {
        return run(List.of(modelRoot), scopeDirs, new ArrayList<>());
    }

    public static List<Outcome> run(Path modelRoot, List<Path> scopeDirs,
            List<String> wallsOut) throws IOException {
        return run(List.of(modelRoot), scopeDirs, wallsOut);
    }

    /** MULTI-ROOT form (the engine-scope suites): the MODEL is the union
     * of every root's tree — the legend-pure platform plus an engine
     * functions tree (core_functions_standard etc., which import the
     * platform). Source names are prefixed {@code <rootIndex>:} so two
     * roots can never collide; each scope dir must live under one of the
     * roots and matches by the same prefixed name. */
    public static List<Outcome> run(List<Path> modelRoots,
            List<Path> scopeDirs, List<String> wallsOut) throws IOException {
        JUDGE_SQL[0] = 0;
        JUDGE_SQL[1] = 0;
        JUDGE_SQL[2] = 0;
        // LOUD, NOT SILENT (upstream boundary batch 2): every model root and
        // every scope directory is a hardcoded upstream path — ten across the
        // five suites — and a moved one must fail by name, never walk an empty
        // tree into a discovery count of zero (the exact pins would catch the
        // zero, but not a scope that lost a subdirectory)
        List<String> missing = new ArrayList<>();
        for (Path p : modelRoots) {
            if (!Files.isDirectory(p)) {
                missing.add("model root " + p);
            }
        }
        for (Path p : scopeDirs) {
            if (!Files.isDirectory(p)) {
                missing.add("scope dir " + p);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("ChannelB upstream paths do not resolve (moved"
                    + " upstream, or the checkout is not on the pin): " + missing);
        }
        List<Compiler.ModelSource> sources = new ArrayList<>();
        for (int r = 0; r < modelRoots.size(); r++) {
            Path root = modelRoots.get(r);
            // in a fixed order, by the '/'-separated relative path: a walk's order is the filesystem's, and the wall loop
            // below drops files in the order it meets them (Bazel workplan P3-08). The order is the INPUT's, not an
            // answer's: no verdict is compared in Java here (PctDisciplineTest)
            for (Path f : com.legend.testing.SourceWalk.inOrder(root, ".pure")) {
                sources.add(new Compiler.ModelSource(
                        r + ":" + slash(root.relativize(f)),
                        Files.readString(f)));
            }
        }
        // a storeless test executes on a declared runtime, as channel A's does (StorelessRuntime):
        // channel B runs every test on its lane's session, channel A's database (PctBackend; DuckDB unless
        // LEGENDLITE_PCT_BACKEND names another, 2026-10-11)
        sources.add(new Compiler.ModelSource("channelb/storeless-runtime.pure",
                org.finos.legend.lite.pct.extension.PctBackend.storelessRuntime()));
        List<String> parseWalls = wallsOut;
        // ITERATIVE WALL COLLECTION (census-first): the platform tree
        // includes m3-metamodel internals our model does not carry; an
        // integrity failure names its element — drop that element's
        // SOURCE FILE (never the element alone: half-files lie) and
        // recompile. Every drop is recorded; the loop is bounded.
        Compiler.ParsedModule module = null;
        ModelContext ctx = null;
        List<String> modelWalls = new ArrayList<>();
        for (int round = 0; round < 200; round++) {
            parseWalls.clear();
            parseWalls.addAll(modelWalls);
            module = Compiler.parseSources(sources,
                    (name, msg) -> parseWalls.add(name + ": " + msg),
                    com.legend.parser.GrammarLevel.LEGEND_PLATFORM);
            try {
                // TENET #2 / platform-owned doctrine: a parsed NATIVE
                // declaration is the SPEC of a function the platform
                // registry already defines — the registry is the
                // definition; the parsed twin drops (keeping it made
                // every native an ambiguous 2-candidate tie).
                var kept = module.model().elements().stream()
                        .filter(e -> !(e instanceof
                                com.legend.model.NativeFunctionDefinition))
                        .toList();
                var pruned = new com.legend.model.ParsedModel(kept,
                        module.model().imports(),
                        module.model().source(),
                        module.model().elementOffsets(),
                        module.model().elementImports(),
                        module.model().elementSources(),
                        module.model().unclaimedSections());
                ctx = Compiler.buildModel(pruned);
                break;
            } catch (com.legend.error.ModelException e) {
                String el = e.element();
                String src = el == null ? null
                        : module.model().elementSources().get(el);
                if (src == null) {
                    throw e;   // unattributable — loud
                }
                final String drop = src;
                sources.removeIf(s -> s.name().equals(drop));
                modelWalls.add(drop + ": MODEL-WALL " + first(words(e)));
            }
        }
        if (ctx == null) {
            throw new IllegalStateException(
                    "model did not converge after 200 wall rounds");
        }
        List<String> scopePrefixes = new ArrayList<>();
        for (Path d : scopeDirs) {
            String prefix = null;
            for (int r = 0; r < modelRoots.size(); r++) {
                if (d.toAbsolutePath().normalize().startsWith(
                        modelRoots.get(r).toAbsolutePath().normalize())) {
                    prefix = r + ":" + slash(modelRoots.get(r).toAbsolutePath()
                            .normalize().relativize(
                                    d.toAbsolutePath().normalize()));
                    break;
                }
            }
            if (prefix == null) {
                throw new IllegalArgumentException("scope dir " + d
                        + " is under none of the model roots " + modelRoots);
            }
            scopePrefixes.add(prefix);
        }
        // SCOPED DEBUG RUNS (the rcorpus.only idiom): -Dlegend.diagnostics=chb-only=<substr>
        // filters by test FQN. A scoped run trivially satisfies the
        // cumulative pins (fewer declines than the ceiling) and never
        // writes a scoreboard — the gate configuration is always the
        // unscoped suite.
        String only = com.legend.diagnostics.Diagnostics.value("chb-only");
        List<Outcome> out = new ArrayList<>();
        for (var el : module.model().elements()) {
            if (!(el instanceof FunctionDefinition fd) || !isPctTest(fd)) {
                continue;
            }
            if (only != null && !fd.qualifiedName().contains(only)) {
                continue;
            }
            // the side maps are keyed per element, a function's id (build rebuild Phase 3b, item 5b)
            String src = module.model().elementSources()
                    .get(com.legend.model.FunctionId.of(fd).qualified());
            if (src == null) {
                // a PCT test with no source record: its id is shared with another declaration
                // (the maps hold one record per id) -- said, never silently skipped
                System.out.println("[chB-discovery] no source record for " + com.legend.model.FunctionId.of(fd));
                continue;
            }
            if (scopePrefixes.stream().noneMatch(src::startsWith)) {
                continue;
            }
            out.add(runOne(fd, module, ctx));
        }
        return out;
    }

    private static boolean isPctTest(FunctionDefinition fd) {
        return fd.stereotypes() != null && fd.stereotypes().stream()
                .anyMatch(s -> "test".equals(s.stereotypeName())
                        && (s.profileName().equals("PCT")
                                || s.profileName().endsWith("::PCT")));
    }

    private static Outcome runOne(FunctionDefinition fd,
            Compiler.ParsedModule module, ModelContext ctx) {
        String fqn = fd.qualifiedName();
        // the measurement's one attribution label (2026-11-09: five per-census hooks until then)
        com.legend.base.Attribution.to(fqn);
        // (the dual-verdict and decline attribution prints went with the host judge's byte channel, 2026-11-07)
        return runOneInner(fd, module, ctx, fqn);
    }

    private static Outcome runOneInner(FunctionDefinition fd,
            Compiler.ParsedModule module, ModelContext ctx, String fqn) {
        if (fd.parameters().size() != 1) {
            return new Outcome(fqn, Status.DECLINED,
                    "non-adapter signature (" + fd.parameters().size()
                            + " params)");
        }
        String fParam = fd.parameters().get(0).name();
        List<ValueSpecification> body;
        try {
            body = eliminateAdapter(fd.body(), fParam);
        } catch (Declined d) {
            return new Outcome(fqn, Status.DECLINED, d.getMessage());
        }
        // the running test, by its function's full name: the diagnostics name the test they report from
        System.setProperty("legend.test.current", fqn);
        try (Connection conn = org.finos.legend.lite.pct.extension.PctBackend.connect()) {
            var lambda = new LambdaFunction(List.of(), body);
            var imports = module.model().elementImports().get(com.legend.model.FunctionId.of(fd).qualified());
            ValueSpecification resolved = imports == null
                    ? NameResolver.resolveQuery(lambda)
                    : NameResolver.resolveQueryIn(lambda, imports,
                            ctx.resolutionUniverse());
            // ONE universe with channel A (PctExecuteNative): the PCT runs
            // with no feature flag (A3, 2026-11-19; S130) — the flag is the lowering's, so channel
            // B's identity adapter carries it the same way
            // the agreement gate (the plan path program's step 6, 2026-10-18): both judges on plans, a disagreement
            // a failure naming both -- the lane's PASS floor carries it
            if (Boolean.getBoolean("pct.channelb.twice")) {
                String differs = twice(ctx, resolved);
                if (differs != null) {
                    return new Outcome(fqn, Status.FAIL, "G10: " + differs);
                }
            }
            if (measuresJudgeSql()) {
                judgeSql(ctx, resolved);
            }
            TestExecution.runProgram(com.legend.Compiler.queryResolved(ctx, resolved),
                    com.legend.testbackend.StorelessRuntime.RUNTIME, com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(conn)), null, referee(),
                    OPTIONS);
            return new Outcome(fqn, Status.PASS, "");
        } catch (java.sql.SQLException e) {
            // an assert failure arrives as the database's error() — a
            // REAL test failure; other SQL errors are failures too (the
            // diff against channel A adjudicates which are wire bugs)
            return new Outcome(fqn, Status.FAIL, first(words(e)), flat(words(e)));
        } catch (RuntimeException e) {
            // compile/lowering walls — channel B cannot run the shape
            return new Outcome(fqn, Status.ERROR,
                    e.getClass().getSimpleName() + ": " + first(words(e)),
                    e.getClass().getSimpleName() + ": " + flat(words(e)));
        }
    }

    /** E-5d, the PCT half (the design doc's §5.9; ruled with E-5, 2026-11-13): the suite's JUDGE statements -- each
     *  body's verdict groups' fused statements and the sides that keep statements of their own -- and their characters,
     *  summed from each body's plan (planning only) under the lane's judge. Pinned per suite in {@code ratchets.tsv}
     *  ({@link PctRatchets}) and held by the product's rule on the reference lane under BOTH ({@link ChannelBLane}). */
    private static final long[] JUDGE_SQL = new long[3];

    /** The judge statements and characters the last {@link #run} summed (both zero where it measured none). */
    static long[] judgeSql() {
        return JUDGE_SQL.clone();
    }

    /** Whether this run sums its judge SQL: asked ({@code -Dpct.channelb.judgeSql}, the ratchet's generator), or the
     *  reference lane under lite's own rules and BOTH, the pin's own mode, outside the G10 twice lanes. The pins moved
     *  to the LITE-PCT lane with A4 of the compatibility program, stage A ratified by the user 2026-11-19: the host
     *  judge is the only judge in compatibility, so the compatibility lanes plan no database-judge statement and the
     *  measure would read none -- the database judge's SQL is the product's alone, and lite's rules under both judges
     *  are where it is planned (the user's ruling of 2026-11-19 ~20:05, "Keep judge yes and rebase": kept, re-based at
     *  the merged batch's numbers, shrink-only from here). */
    static boolean measuresJudgeSql() {
        return Boolean.getBoolean("pct.channelb.judgeSql") || ChannelBLane.isReference() && lite()
                && OPTIONS.planning().judgeMode() == com.legend.testplan.PlanningOptions.JudgeMode.BOTH
                && !Boolean.getBoolean("pct.channelb.twice");
    }

    /** {@code resolved}'s judge statements added to {@link #JUDGE_SQL}; a body that does not plan adds none (its run
     *  fails as it does without the measure). */
    private static void judgeSql(ModelContext ctx, ValueSpecification resolved) {
        com.legend.testexecutionplan.TestPlan plan;
        try {
            plan = TestExecution.planBody(com.legend.Compiler.queryResolved(ctx, resolved),
                    com.legend.testbackend.StorelessRuntime.RUNTIME, OPTIONS);
        } catch (com.legend.error.LegendCompileException | com.legend.error.NotImplementedException
                | com.legend.sql.DialectCapability refused) {
            // a planning refusal: the body adds no judge SQL (its run fails as it does without the measure); any other
            // exception is a defect, and fails the body with its stack (G7, round 55f, 2026-11-15)
            return;
        } catch (UnsupportedOperationException e) {
            // a render arm's refusal only (thrown from a dialect); the same class from elsewhere is a defect (B7)
            StackTraceElement[] at = e.getStackTrace();
            if (e.getClass() != UnsupportedOperationException.class || at.length == 0
                    || !at[0].getClassName().startsWith("com.legend.sql.dialect.")) {
                throw e;
            }
            return;
        }
        judgeSql(plan.steps());
    }

    /** The judge's statements as the runner sends them ({@code TestPlan.sent}, 2026-11-15, PR1's review: the fused
     *  statement and the base sides alone had missed every wrapper, raising verdict and branch condition), and each
     *  verdict step's plan JSON. */
    private static void judgeSql(List<com.legend.testexecutionplan.TestPlan.TestStep> steps) {
        for (com.legend.testexecutionplan.TestPlan.TestStep step : steps) {
            if (step instanceof com.legend.testexecutionplan.TestPlan.Isolated pair) {
                // BOTH isolated (2026-11-17, the orchestrator's (a)): the judge's SQL is the database judge's plan, as
                // it was the fused program's (E-5c: BOTH planned the database judge's program)
                judgeSql(pair.database().steps());
                continue;
            }
            if (step instanceof com.legend.testexecutionplan.TestPlan.Verdicts
                    || step instanceof com.legend.testexecutionplan.TestPlan.Branch) {
                for (com.legend.executionplan.ExecutionPlan.Node n : com.legend.testexecutionplan.TestPlan.sent(step)) {
                    nodeStatements(n);
                }
                JUDGE_SQL[2] += com.legend.testexecutionplan.TestPlanJson.writeStep(step).length();
            }
        }
    }

    private static void nodeStatements(com.legend.executionplan.ExecutionPlan.Node node) {
        switch (node) {
            case com.legend.executionplan.ExecutionPlan.Sequence s -> s.steps().forEach(ChannelB::nodeStatements);
            case com.legend.executionplan.ExecutionPlan.TdsResult t -> addJudge(t.sql().statement());
            case com.legend.executionplan.ExecutionPlan.Keyed k -> {
                addJudge(k.keys().statement());
                addJudge(k.body().statement());
            }
            case com.legend.executionplan.ExecutionPlan.Update u -> addJudge(u.sql().statement());
            case com.legend.executionplan.ExecutionPlan.Effect e -> addJudge(e.sql().statement());
            case com.legend.executionplan.ExecutionPlan.Check c -> addJudge(c.sql().statement());
            case com.legend.executionplan.ExecutionPlan.Load l -> {
            }
            case com.legend.executionplan.ExecutionPlan.Constant c -> {
            }
        }
    }

    private static void addJudge(String statement) {
        JUDGE_SQL[0]++;
        JUDGE_SQL[1] += statement.length();
    }

    /** The rules the bodies run under ({@code -Dpct.channelb.rules}): COMPAT, the compatibility edge's (the lanes'
     *  default), or LITE, lite's own (the LITE-PCT lane, the compatibility program's A4 gate: "lite's two judges agree
     *  on the PCT", 2026-11-19). Any other value is refused by name. */
    private static final String RULES = rules(System.getProperty("pct.channelb.rules", "COMPAT"));

    private static String rules(String named) {
        if (!named.equals("COMPAT") && !named.equals("LITE")) {
            throw new IllegalArgumentException("-Dpct.channelb.rules=" + named + ": COMPAT or LITE");
        }
        return named;
    }

    /** Whether this run judges the bodies under lite's own rules ({@code -Dpct.channelb.rules=LITE}). */
    static boolean lite() {
        return RULES.equals("LITE");
    }

    /** The run hook: the compatibility edge's appeals (COMPAT), or none -- lite's own runs hear no appeal (LITE). */
    private static com.legend.testexec.EdgeRun referee() {
        return lite() ? com.legend.testexec.NoEdgeRun.NONE : com.legend.edge.AppealRun.with(null);
    }

    /** Channel B's options under the compatibility rules: the corpus's runtime features, the judge
     *  {@code -Dpct.channelb.judge} names, compatibility mode and upstream's spellings. */
    private static final com.legend.ExecuteOptions COMPAT_OPTIONS = com.legend.ExecuteOptions.recording(
                    // the compatibility rules' runs carry the compatibility edge, which names its rules (census C1,
                    // the orchestrator's ruling of 2026-11-17), never the product's NoEdge with a style
                    new com.legend.edge.CompatEdge(new com.legend.edge.RawLedger.Recorder(),
                            com.legend.executionplan.DataStores.none()), com.legend.executionplan.DataStores.none())
            // no feature flag (A3 of the compatibility program, 2026-11-19: substring is Pure's by default, so the
            // 2026-09-12 deviation of setting CORRECT_SQL_SUBSTRING_INDEXING is reversed; register S130)
            // the tests' planning options: as-of joins the same row on every database (PlanOption)
            .withOptions(java.util.Set.of(com.legend.platform.PlanOption.DETERMINISTIC_AS_OF))
            // the host judge alone (rule 4 of the compatibility program's rules of record: "Host judge is only judge
            // in compatibility"; A4 of the compatibility program, stage A ratified by the user 2026-11-19): until A4
            // these runs took -Dpct.channelb.judge, BOTH by default; the test planner now refuses a database-judge
            // verdict for a compatibility audience by name (TestPlanner.refuseTheDatabaseJudgeInCompatibility)
            .withJudgeMode(compatJudge())
            // legend-pure's texts, spelled by the edge (phase D's D1.0)
            .withStyle(com.legend.edge.PureText.UPSTREAM);

    /** The judge a run under the compatibility rules is planned for: the host's alone (rule 4; A4 of the compatibility program, stage A ratified by the user 2026-11-19).
     *  {@code -Dpct.channelb.judge} was this runner's judge switch until A4. Set to anything but HOST under these
     *  rules, it is refused by name rather than ignored, so a run asked for the database judge never reports a host
     *  run in its place (invariant 4; the reviewer's S2, 2026-11-19; the corpus's MinimalCorpus refuses its 'both' the
     *  same way). Lite's own rules ({@code -Dpct.channelb.rules=LITE}) still read it ({@link #LITE_OPTIONS}). */
    private static com.legend.testplan.PlanningOptions.JudgeMode compatJudge() {
        String asked = System.getProperty("pct.channelb.judge");
        if (!lite() && asked != null && !asked.equals("HOST")) {
            throw new IllegalArgumentException("-Dpct.channelb.judge=" + asked + ": under the compatibility rules the"
                    + " host judge is the only judge (rule 4 of the compatibility program's rules of record); the"
                    + " database judge runs under lite's own rules, -Dpct.channelb.rules=LITE");
        }
        return com.legend.testplan.PlanningOptions.JudgeMode.HOST;
    }

    /** Channel B's options under lite's own rules (the compatibility judges homework's prototype, H6-judges.md 5.1):
     *  the product's (no edge, the product's style and default features), as-of joins the same row on every database,
     *  and the judge {@code -Dpct.channelb.judge} names, BOTH by default. */
    private static final com.legend.ExecuteOptions LITE_OPTIONS = com.legend.ExecuteOptions.NONE
            .withOptions(java.util.Set.of(com.legend.platform.PlanOption.DETERMINISTIC_AS_OF))
            .withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.valueOf(
                    System.getProperty("pct.channelb.judge", "BOTH")));

    private static final com.legend.ExecuteOptions OPTIONS = lite() ? LITE_OPTIONS : COMPAT_OPTIONS;

    /**
     * G10'S GUARD FOR A TEST BODY (O5's term, the user's ruling of 2026-11-13: "a body planned and run twice answers the
     * same"): under {@code -Dpct.channelb.twice=true}, each body is typed and planned twice, and the two test plans
     * must be byte for byte the same JSON. It is then run twice, each run on a fresh connection, and the two runs must
     * end alike (a pass, or the same failure's class and text) and tell the same verdicts in the same order. Null
     * where they agree, else what differs; the body's own run follows as usual.
     */
    private static @com.legend.base.Nullable String twice(ModelContext ctx, ValueSpecification resolved) {
        return twice(() -> com.legend.testexecutionplan.TestPlanJson.write(TestExecution.planBody(
                        com.legend.Compiler.queryResolved(ctx, resolved), com.legend.testbackend.StorelessRuntime.RUNTIME,
                        OPTIONS)),
                () -> recorded(ctx, resolved));
    }

    /** {@link #twice(ModelContext, ValueSpecification)}'s comparison over its two halves: {@code plan} plans the body
     *  as JSON, {@code run} runs it and answers its verdicts and end. Each is called twice; null where both pairs
     *  agree, else what differs (the guard's own test, ChannelBTwiceTest, plants each difference). */
    static @com.legend.base.Nullable String twice(java.util.function.Supplier<String> plan,
            java.util.function.Supplier<List<String>> run) {
        String first = plan.get();
        String second = plan.get();
        if (!first.equals(second)) {
            int at = 0;
            while (at < Math.min(first.length(), second.length()) && first.charAt(at) == second.charAt(at)) {
                at++;
            }
            return "planned twice, two plans: they part at character " + at + ": "
                    + first.substring(at, Math.min(first.length(), at + 80)) + " | "
                    + second.substring(at, Math.min(second.length(), at + 80));
        }
        List<String> one = run.get();
        List<String> two = run.get();
        return one.equals(two) ? null : "run twice, two answers: " + one + " | " + two;
    }

    /** One run of the body on a fresh connection: each verdict as told, then how the run ended. */
    private static List<String> recorded(ModelContext ctx, ValueSpecification resolved) {
        List<String> told = new java.util.ArrayList<>();
        com.legend.testexecutionplan.AssertListener listener = (name, pass, detail) ->
                told.add(name + (pass ? " pass" : " FAIL " + detail));
        try (Connection conn = org.finos.legend.lite.pct.extension.PctBackend.connect()) {
            TestExecution.runProgram(com.legend.Compiler.queryResolved(ctx, resolved),
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(conn)), listener, referee(),
                    OPTIONS);
            told.add("ended: pass");
        } catch (java.sql.SQLException | RuntimeException e) {
            told.add(ended(e));
        }
        return told;
    }

    /** How a run ended, for the twice guard's record: the failure's class and its WHOLE message -- two runs' records are
     *  compared, and a clipped middle could hide a difference (2026-11-15, PR1's review; pinned by ChannelBTwiceTest's
     *  planted difference past the old clip, the PR1 re-review's A7). */
    static String ended(Exception e) {
        return "ended: " + e.getClass().getSimpleName() + ": " + flat(words(e));
    }

    /** The census detail line: assert messages are MULTI-LINE
     * ('\nexpected: …\nactual: …') — newlines flatten so the row keeps
     * its whole story; a long middle clips, its head and its TAIL kept (2026-11-03, round 17: a Databricks error is the
     * whole statement and then Spark's own words, which a head alone cut off -- the statement each judge writes
     * differs, the database's error does not). */
    private static String first(@com.legend.base.Nullable String msg) {
        String m = flat(msg);
        return m.length() <= 800 ? m : m.substring(0, 400) + "…" + m.substring(m.length() - 400);
    }

    /** The whole message, its newlines flattened as {@link #first}'s are: what a ledger row's text is matched in. */
    private static String flat(@com.legend.base.Nullable String msg) {
        return String.valueOf(msg).replace("\n", " ⏎ ").trim();
    }

    private static final class Declined extends RuntimeException {
        Declined(String why) {
            super(why);
        }
    }

    /** THE IDENTITY ADAPTER: {@code $f->eval(|expr)} → {@code expr},
     * and the relation suite's ONE-LET INDIRECTION —
     * {@code let e = {|expr}; $f->eval($e)} → {@code expr} (the same
     * identity application, the lambda named instead of inline). Any
     * OTHER use of the adapter parameter declines with its shape —
     * never silently dropped. */
    static List<ValueSpecification> eliminateAdapter(
            List<ValueSpecification> body, String fParam) {
        java.util.Map<String, LambdaFunction> lets = new java.util.HashMap<>();
        for (ValueSpecification stmt : body) {
            if (stmt instanceof AppliedFunction lf
                    && lf.function().equals("letFunction")
                    && lf.parameters().size() == 2
                    && lf.parameters().get(0)
                            instanceof com.legend.protocol.spec.CString ln
                    && lf.parameters().get(1) instanceof LambdaFunction lam
                    && lam.parameters().isEmpty()
                    && !lam.body().isEmpty()) {
                lets.put(ln.value(), lam);
            }
        }
        // MULTI-STATEMENT named lambdas ({| let t1=...; expr}): the
        // identity application HOISTS the leading statements into the
        // enclosing body before the first statement that evals the
        // lambda, and the eval site splices the LAST statement (the
        // relation suite's dominant shape — 51 rows). Hoist once per
        // lambda; a name collision with an enclosing let would be a
        // source bug the compile then reports.
        java.util.Set<String> hoisted = new java.util.HashSet<>();
        List<ValueSpecification> out = new ArrayList<>(body.size());
        for (ValueSpecification stmt : body) {
            for (String var : evalledVars(stmt, fParam, lets)) {
                LambdaFunction lam = lets.get(var);
                if (lam.body().size() > 1 && hoisted.add(var)) {
                    for (int i = 0; i < lam.body().size() - 1; i++) {
                        out.add(rewrite(lam.body().get(i), fParam, lets));
                    }
                }
            }
            // INLINE multi-statement eval bodies get the same hoist as the
            // named form ({@code let res = $f->eval(|let v='x'; $v;)} —
            // the letFn suite's shape): leading statements join the
            // enclosing body; the eval site splices the LAST statement.
            for (LambdaFunction lam : inlineEvalLambdas(stmt, fParam)) {
                for (int i = 0; i < lam.body().size() - 1; i++) {
                    out.add(rewrite(lam.body().get(i), fParam, lets));
                }
            }
            out.add(rewrite(stmt, fParam, lets));
        }
        return out;
    }

    /** Inline {@code $f->eval(|...)} lambdas in this statement with MORE
     * than one body statement, in occurrence order (their leading
     * statements hoist into the enclosing body). */
    private static List<LambdaFunction> inlineEvalLambdas(
            ValueSpecification n, String fParam) {
        List<LambdaFunction> found = new ArrayList<>();
        collectInlineEvalLambdas(n, fParam, found);
        return found;
    }

    private static void collectInlineEvalLambdas(ValueSpecification n,
            String fParam, List<LambdaFunction> found) {
        if (shadowsAdapterParam(n, fParam)) {
            return;
        }
        if (n instanceof AppliedFunction af
                && simpleName(af.function()).equals("eval")
                && af.parameters().size() == 2
                && af.parameters().get(0) instanceof Variable v
                && v.name().equals(fParam)
                && af.parameters().get(1) instanceof LambdaFunction lam
                && lam.parameters().isEmpty()
                && lam.body().size() > 1) {
            found.add(lam);
        }
        for (ValueSpecification k : n.children()) {
            collectInlineEvalLambdas(k, fParam, found);
        }
    }

    /** SHADOW-STOP (the lambda-walkers trap): a lambda whose OWN parameter
     * is named like the adapter param rebinds {@code $f} inside its body
     * ({@code exists(f | $f.legalName == ...)} — the exists suite); every
     * inner reference is the iteration variable, never the adapter. */
    private static boolean shadowsAdapterParam(ValueSpecification n,
            String fParam) {
        return n instanceof LambdaFunction l
                && l.parameters().stream()
                        .anyMatch(p -> p.name().equals(fParam));
    }

    /** The let-bound zero-param lambda names this statement applies the
     * adapter to ({@code $f->eval($var)}), in first-use order. */
    private static List<String> evalledVars(ValueSpecification n,
            String fParam, java.util.Map<String, LambdaFunction> lets) {
        List<String> found = new ArrayList<>();
        collectEvalledVars(n, fParam, lets, found);
        return found;
    }

    private static void collectEvalledVars(ValueSpecification n,
            String fParam, java.util.Map<String, LambdaFunction> lets,
            List<String> found) {
        if (shadowsAdapterParam(n, fParam)) {
            return;
        }
        if (n instanceof AppliedFunction af
                && simpleName(af.function()).equals("eval")
                && af.parameters().size() == 2
                && af.parameters().get(0) instanceof Variable v
                && v.name().equals(fParam)
                && af.parameters().get(1) instanceof Variable ev
                && lets.containsKey(ev.name())
                && !found.contains(ev.name())) {
            found.add(ev.name());
        }
        for (ValueSpecification k : n.children()) {
            collectEvalledVars(k, fParam, lets, found);
        }
    }

    private static ValueSpecification rewrite(ValueSpecification n,
            String fParam, java.util.Map<String, LambdaFunction> lets) {
        // SHADOW-STOP: inside a lambda that rebinds the adapter param,
        // $f is the lambda's own variable — nothing to rewrite or decline
        if (shadowsAdapterParam(n, fParam)) {
            return n;
        }
        if (n instanceof AppliedFunction af
                && simpleName(af.function()).equals("eval")
                && af.parameters().size() == 2
                && af.parameters().get(0) instanceof Variable v
                && v.name().equals(fParam)
                && af.parameters().get(1) instanceof LambdaFunction lam
                && lam.parameters().isEmpty()
                && !lam.body().isEmpty()) {
            // single-statement: the body IS the value; multi-statement:
            // the leading statements were HOISTED (inlineEvalLambdas) —
            // splice the LAST statement, same as the named form
            return rewrite(lam.body().get(lam.body().size() - 1),
                    fParam, lets);
        }
        if (n instanceof AppliedFunction af2
                && simpleName(af2.function()).equals("eval")
                && af2.parameters().size() == 2
                && af2.parameters().get(0) instanceof Variable v
                && v.name().equals(fParam)
                && af2.parameters().get(1) instanceof Variable ev
                && lets.containsKey(ev.name())) {
            // single-statement: the body IS the value; multi-statement:
            // the leading lets were HOISTED (eliminateAdapter) — splice
            // the last statement
            List<ValueSpecification> lb = lets.get(ev.name()).body();
            return rewrite(lb.get(lb.size() - 1), fParam, lets);
        }
        // any surviving reference to the adapter param is a shape we
        // do not run — decline WITH the offending call's spelling, so
        // the decline census distinguishes shapes (audit 2026-08-19:
        // 51 relation declines shared one blind string)
        if (n instanceof AppliedFunction afx
                && afx.parameters().stream().anyMatch(px ->
                        px instanceof Variable pv
                        && pv.name().equals(fParam))) {
            throw new Declined("adapter parameter used outside"
                    + " $f->eval(|...) — non-identity shape: "
                    + simpleName(afx.function()) + "/"
                    + afx.parameters().size());
        }
        if (n instanceof Variable v2 && v2.name().equals(fParam)) {
            throw new Declined("adapter parameter used outside"
                    + " $f->eval(|...) — non-identity shape: bare"
                    + " $" + fParam + " reference");
        }
        List<ValueSpecification> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<ValueSpecification> rewritten = new ArrayList<>(kids.size());
        boolean changed = false;
        for (ValueSpecification k : kids) {
            ValueSpecification r = rewrite(k, fParam, lets);
            changed |= r != k;
            rewritten.add(r);
        }
        return changed ? n.withChildren(rewritten) : n;
    }

    private static String simpleName(String fn) {
        int i = fn.lastIndexOf("::");
        return i < 0 ? fn : fn.substring(i + 2);
    }

    /** {@code e}'s message in legend-pure's words where it is a compile error of a kind (the edge's,
     *  {@code PureMessages.compile}; D1.6, W7 of docs/records/d-plan-2026-11-16.md): PCT is legend-pure's suite. */
    private static String words(Throwable e) {
        return com.legend.edge.PureMessages.compile(e);
    }
}
