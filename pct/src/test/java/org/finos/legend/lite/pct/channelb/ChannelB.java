// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import com.legend.Compiler;
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
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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

    public record Outcome(String testFqn, Status status, String detail) {
    }

    public enum Status { PASS, FAIL, DECLINED, ERROR }

    /** Run every {@code <<PCT.test>>} function found under the scope
     * directories. The MODEL is the whole {@code modelRoot} tree (the
     * platform sources — shared test models, the assert family, the
     * function definitions the tests exercise; files that do not parse
     * are collected as walls, never silently dropped); DISCOVERY is
     * scoped. One in-memory DuckDB session per test (isolation — PCT
     * tests assume a fresh world). */
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
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path f : walk
                        .filter(p -> p.toString().endsWith(".pure"))
                        .toList()) {
                    sources.add(new Compiler.ModelSource(
                            r + ":" + slash(root.relativize(f)),
                            Files.readString(f)));
                }
            }
        }
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
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
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
                modelWalls.add(drop + ": MODEL-WALL " + first(e.getMessage()));
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
        // SCOPED DEBUG RUNS (the rcorpus.only idiom): -Dchb.only=<substr>
        // filters by test FQN. A scoped run trivially satisfies the
        // cumulative pins (fewer declines than the ceiling) and never
        // writes a scoreboard — the gate configuration is always the
        // unscoped suite.
        String only = System.getProperty("chb.only");
        List<Outcome> out = new ArrayList<>();
        for (var el : module.model().elements()) {
            if (!(el instanceof FunctionDefinition fd) || !isPctTest(fd)) {
                continue;
            }
            if (only != null && !fd.qualifiedName().contains(only)) {
                continue;
            }
            String src = module.model().elementSources()
                    .get(fd.qualifiedName());
            if (src == null || scopePrefixes.stream()
                    .noneMatch(src::startsWith)) {
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
        com.legend.lowering.StampCensus.CONTEXT.set(fqn);
        com.legend.exec.SqlTypeCensus.CONTEXT.set(fqn);
        // dual-verdict attribution: name the test that grows the
        // disagreement census (the alarm's diagnosis needs the WHO) —
        // and the DECLINE census likewise (the burn targets tests, not
        // a bare histogram; the F10 census rides this)
        long disagreeBefore =
                com.legend.exec.CanonicalDivergence.sqlDisagreeCount();
        long declineBefore =
                com.legend.exec.CanonicalDivergence.sqlDeclinedCount();
        try {
            return runOneInner(fd, module, ctx, fqn);
        } finally {
            if (com.legend.exec.CanonicalDivergence.sqlDisagreeCount()
                    > disagreeBefore) {
                System.out.println("[chB] DUAL-VERDICT-DISAGREE in " + fqn);
            }
            long dd = com.legend.exec.CanonicalDivergence.sqlDeclinedCount()
                    - declineBefore;
            if (dd > 0) {
                System.out.println("[chB] DECLINES+" + dd + " in " + fqn);
            }
        }
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
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            var lambda = new LambdaFunction(List.of(), body);
            var imports = module.model().elementImports().get(fqn);
            ValueSpecification resolved = imports == null
                    ? NameResolver.resolveQuery(lambda)
                    : NameResolver.resolveQuery(lambda, imports,
                            ctx.elementFqns());
            // ONE universe with channel A (PctExecuteNative): the PCT runs
            // with CORRECT_SQL_SUBSTRING_INDEXING on, as the engine's own
            // testable runner does — the flag is the lowering's, so channel
            // B's identity adapter carries it the same way
            Compiler.executeResolved(resolved, ctx, null, conn, null, null,
                    com.legend.ExecuteOptions.NONE.withFeatures(java.util.Set.of(
                            com.legend.compiler.spec.typed.Feature.CORRECT_SQL_SUBSTRING_INDEXING)));
            return new Outcome(fqn, Status.PASS, "");
        } catch (java.sql.SQLException e) {
            // an assert failure arrives as the database's error() — a
            // REAL test failure; other SQL errors are failures too (the
            // diff against channel A adjudicates which are wire bugs)
            return new Outcome(fqn, Status.FAIL, first(e.getMessage()));
        } catch (RuntimeException e) {
            // compile/lowering walls — channel B cannot run the shape
            return new Outcome(fqn, Status.ERROR,
                    e.getClass().getSimpleName() + ": " + first(e.getMessage()));
        }
    }

    /** The census detail line: assert messages are MULTI-LINE
     * ('\nexpected: …\nactual: …') — newlines flatten so the row keeps
     * its whole story; long tails clip. */
    private static String first(@com.legend.Nullable String msg) {
        String m = String.valueOf(msg).replace("\n", " ⏎ ").trim();
        return m.length() <= 400 ? m : m.substring(0, 400) + "…";
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
}
