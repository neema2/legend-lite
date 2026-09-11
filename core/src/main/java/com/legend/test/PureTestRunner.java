// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.ProgramFacts;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.AssertListener;
import com.legend.exec.SqlReplayOracle;
import com.legend.model.ImportScope;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.ValueSpecification;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE PURE TEST RUNNER — runs a discovered {@link PureTests.TestCase} through
 * the platform and returns one {@link Result}: PASS, FAIL or SKIPPED, the
 * number of verdicts reached, and the reason. Product surface (upstream
 * boundary batch 7a, 2026-09-11): the rules below are the engine's own test
 * framework's, implemented here from the spec; none of them mentions a
 * checkout. The corpus harness is one caller; a user with
 * {@code <<test.Test>>} functions is another.
 *
 * <p>The rules:
 * <ul>
 *   <li>one database SESSION per package ({@link Sessions#open}); the package's
 *       setups — the caller's shared setups, then every {@code BeforePackage}
 *       of a package that prefixes it, outermost first — run once per session,
 *       through the platform; a setup the platform derives as having no
 *       statement effects is INERT and never runs ({@link #inertSetups});</li>
 *   <li>a setup that fails FAILS every test depending on it, named
 *       (the engine's suite scores each BeforePackage as a test case of its own);</li>
 *   <li>a body that seeds its own inline data runs on a PRIVATE workspace, not
 *       the session (the platform's {@link ProgramFacts#seedsInlineCsv});</li>
 *   <li>the body is resolved, typed and executed through the ONE production
 *       entry ({@link Compiler#executeResolved}); every assert the platform
 *       adjudicates is a verdict; the first failed assert is the reason;</li>
 *   <li>a body that reaches NO verdict function is SKIPPED, with that said —
 *       it proves nothing and is never a pass; a body that calls a verdict
 *       function the platform did not adjudicate is a FAIL.</li>
 * </ul>
 */
public final class PureTestRunner implements AutoCloseable {

    /** How a session's connection is opened (DuckDB in memory, H2, …). */
    @FunctionalInterface
    public interface Sessions {
        Connection open() throws SQLException;
    }

    public enum Status { PASS, FAIL, SKIPPED }

    /** One adjudicated assert, with what the platform reported BEFORE it
     *  decided: {@code declinedReason} when the rows leg was declined to the
     *  text channel (the arm's own vocabulary), {@code refereeOutcome} when a
     *  referee judged the rows leg (MATCH / DIVERGED / …) — both may be set;
     *  null means the event did not happen for this assert. */
    public record Verdict(String assertName, boolean pass,
            @com.legend.Nullable String declinedReason, @com.legend.Nullable String refereeOutcome) {
    }

    /** One test's outcome with its reason and its verdict log.
     *  {@code refereeMatched}: a referee reported MATCH at any point of the run
     *  (the differential witness — counted even when the assert it preceded
     *  was then declined, or never followed by a verdict). */
    public record Result(String fqn, Status status, List<Verdict> verdicts, String reason,
            boolean refereeMatched) {
        public Result(String fqn, Status status, List<Verdict> verdicts, String reason) {
            this(fqn, status, verdicts, reason, false);
        }

        public Result {
            verdicts = List.copyOf(verdicts);
        }

        public boolean pass() {
            return status == Status.PASS;
        }

        public int verdictCount() {
            return verdicts.size();
        }
    }

    private static final ValueSpecification INERT_SETUP = new CBoolean(true);

    private final ModelContext ctx;
    private final @com.legend.Nullable String runtimeFqn;
    private final Sessions sessions;
    private final List<String> sharedSetups;
    private final Map<String, List<String>> setupsByPackage;
    private final TestObserver observer;

    private @com.legend.Nullable Connection sessionConn;
    private @com.legend.Nullable String sessionPkg;
    private final Set<String> setupsDone = new LinkedHashSet<>();
    /** Each setup's resolved program (or {@link #INERT_SETUP}) — derived once. */
    private final Map<String, ValueSpecification> setupPrograms = new HashMap<>();
    private final Set<String> inertSetups = new LinkedHashSet<>();

    /**
     * @param ctx            the compiled model the tests live in (with its execution overlay)
     * @param runtimeFqn     the runtime the bodies execute against, or null for none
     * @param sessions       how a package session's connection opens
     * @param sharedSetups   zero-parameter functions every package's session runs first
     *                       (a corpus-wide fixture); may be empty
     * @param setupsByPackage {@code BeforePackage} functions by package ({@link PureTests.Discovery})
     * @param observer       the caller's instruments, or {@link TestObserver#NONE}
     */
    public PureTestRunner(ModelContext ctx, @com.legend.Nullable String runtimeFqn, Sessions sessions,
            List<String> sharedSetups, Map<String, List<String>> setupsByPackage, TestObserver observer) {
        this.ctx = ctx;
        this.runtimeFqn = runtimeFqn;
        this.sessions = sessions;
        this.sharedSetups = List.copyOf(sharedSetups);
        this.setupsByPackage = Map.copyOf(setupsByPackage);
        this.observer = observer;
    }

    /** The setups the platform derived as INERT (no statement effects) and
     *  so never ran — a caller pins the count: an effect analysis that wrongly
     *  reads a seeding setup as inert would silently unseed its package. */
    public Set<String> inertSetups() {
        return Collections.unmodifiableSet(inertSetups);
    }

    /** A setup program the caller derived itself (the corpus's shared fixture
     *  units are typed apart, before discovery); the runner takes it as-is. */
    public void registerSetup(String fqn, ValueSpecification resolved) {
        setupPrograms.put(fqn, resolved);
    }

    // ---- SESSION + SEED ---------------------------------------------------

    private void beginSession(String pkg) throws SQLException {
        endSession();
        sessionConn = sessions.open();
        observer.sessionBegan(pkg, sessionConn);
        sessionPkg = pkg;
        setupsDone.clear();
        deriveSetups(pkg);
    }

    /** Close the current package session, if any. */
    public void endSession() {
        observer.sessionEnding();
        if (sessionConn != null) {
            try {
                sessionConn.close();
            } catch (SQLException ignored) {
                // a session that fails to close cannot poison the next
            }
        }
        sessionConn = null;
        sessionPkg = null;
    }

    @Override
    public void close() {
        endSession();
    }

    /** The setups a package inherits: the shared fixture units and every
     * BeforePackage of a package that prefixes it, outermost first. */
    private List<String> setupCandidates(String pkg) {
        List<String> candidates = new ArrayList<>(sharedSetups);
        List<String> pkgs = new ArrayList<>(setupsByPackage.keySet());
        pkgs.sort(java.util.Comparator.comparingInt(String::length));
        for (String p : pkgs) {
            if (pkg.equals(p) || pkg.startsWith(p + "::")) {
                candidates.addAll(setupsByPackage.get(p));
            }
        }
        return new ArrayList<>(new LinkedHashSet<>(candidates));
    }

    /** A setup's resolved program and its effect verdict are facts about the
     * MODEL: derived once per setup, at session start — so nothing resolves
     * between a test's own resolution and its execution. */
    private void deriveSetups(String pkg) {
        for (String fqn : setupCandidates(pkg)) {
            setupPrograms.computeIfAbsent(fqn, f -> {
                ValueSpecification resolved = Compiler.resolveQuery(
                        List.of(new AppliedFunction(f, List.of())), new ImportScope(List.of()), ctx);
                if (Compiler.hasStatementEffects(resolved, ctx)) {
                    return resolved;
                }
                inertSetups.add(f);
                return INERT_SETUP;
            });
        }
    }

    private List<String> runSetups(PureTests.TestCase t, Connection conn, boolean shared,
            ExecuteOptions options) {
        List<String> failures = new ArrayList<>();
        for (String fqn : setupCandidates(t.pkg())) {
            if (shared && setupsDone.contains(fqn)) {
                continue;
            }
            ValueSpecification call = java.util.Objects.requireNonNull(
                    setupPrograms.get(fqn), "setup derived at session start");
            if (call == INERT_SETUP) {
                continue;
            }
            try {
                Compiler.executeResolved(call, ctx, runtimeFqn, conn, null, null, options);
                if (shared) {
                    setupsDone.add(fqn);
                }
            } catch (RuntimeException e) {
                failures.add("setup " + fqn + "() => " + whole(e.getMessage()));
            }
        }
        return failures;
    }

    // ---- RUN + JUDGE --------------------------------------------------------

    /** Run one test: its package session (opened on demand), its setups once,
     *  its body through the platform, one result. */
    public Result run(PureTests.TestCase t) throws SQLException {
        observer.testStarted(t);
        if (!t.pkg().equals(sessionPkg)) {
            beginSession(t.pkg());
        }
        List<ValueSpecification> body = t.fn().body();
        // the platform's facts about the program decide the session: a test
        // that seeds inline CSV data gets a private workspace
        ValueSpecification resolved;
        ProgramFacts facts;
        try {
            resolved = Compiler.resolveQuery(List.copyOf(body), t.imports(), ctx);
        } catch (RuntimeException e) {
            return new Result(t.fqn(), Status.FAIL, List.of(), "resolve: " + whole(e.getMessage()));
        }
        try {
            facts = Compiler.programFacts(resolved, ctx);
        } catch (RuntimeException e) {
            return new Result(t.fqn(), Status.FAIL, List.of(), "type: " + whole(e.getMessage()));
        }
        boolean shared = !facts.seedsInlineCsv();
        observer.privateWorkspace(!shared);
        Connection conn = shared ? java.util.Objects.requireNonNull(sessionConn, "session") : sessions.open();
        ExecuteOptions options = observer.options(t, shared);
        SqlReplayOracle oracle = observer.oracle(t);
        try {
            // a setup that fails FAILS every test depending on it: the engine's
            // suite scores each BeforePackage function as a test case of its own,
            // so a failure there is scored, never tolerated; a body judged on a
            // half-seeded session is no verdict
            List<String> setupFailures = runSetups(t, conn, shared, options);
            if (!setupFailures.isEmpty()) {
                return new Result(t.fqn(), Status.FAIL, List.of(),
                        "setup failed: " + String.join("; ", setupFailures));
            }
            return judge(t, resolved, facts, conn, options, oracle);
        } finally {
            observer.privateWorkspace(false);
            observer.bodyFinished(t, shared, options);
            if (!shared) {
                try {
                    conn.close();
                } catch (SQLException ignored) {
                    // private workspace; nothing depends on it after this
                }
            }
        }
    }

    private Result judge(PureTests.TestCase t, ValueSpecification resolved, ProgramFacts facts,
            Connection conn, ExecuteOptions options, @com.legend.Nullable SqlReplayOracle oracle)
            throws SQLException {
        boolean effectful = facts.effects();
        List<Verdict> verdicts = new ArrayList<>();
        List<String> failedAsserts = new ArrayList<>();
        // the arm reports a decline/referee BEFORE it decides: the mark applies
        // to the upcoming verdict (index verdicts.size())
        Map<Integer, String> declinedAhead = new HashMap<>();
        Map<Integer, String> refereedAhead = new HashMap<>();
        boolean[] refereeMatched = {false};
        observer.bodyStarting(conn, effectful);
        boolean passed = false;
        try {
            String failure = null;
            try {
                Compiler.executeResolved(resolved, ctx, runtimeFqn, conn,
                        new AssertListener() {
                            @Override
                            public void verdict(String name, boolean pass, @com.legend.Nullable String detail) {
                                int i = verdicts.size();
                                verdicts.add(new Verdict(name, pass,
                                        declinedAhead.get(i), refereedAhead.get(i)));
                                if (!pass) {
                                    failedAsserts.add("#" + verdicts.size() + " " + name
                                            + (detail == null ? "" : ": " + whole(detail)));
                                }
                                observer.verdict(name, pass, detail);
                            }

                            @Override
                            public void declined(String name, String reason) {
                                declinedAhead.put(verdicts.size(), reason);
                                observer.declined(name, reason);
                            }

                            @Override
                            public void refereed(String name, String outcome) {
                                refereedAhead.put(verdicts.size(), outcome);
                                if ("MATCH".equals(outcome)) {
                                    refereeMatched[0] = true;
                                }
                                observer.refereed(name, outcome);
                            }
                        },
                        oracle, options);
            } catch (RuntimeException e) {
                if (System.getenv("LEGEND_LITE_STACKS") != null) {
                    e.printStackTrace();
                }
                failure = e.getClass().getSimpleName() + ": " + whole(e.getMessage());
            }
            if (failure == null && !failedAsserts.isEmpty()) {
                failure = "assert " + failedAsserts.get(0);
            }
            if (failure == null && verdicts.isEmpty() && facts.verdicts()) {
                failure = "no verdict: the body calls an assert the platform did not adjudicate";
            }
            if (failure != null) {
                return new Result(t.fqn(), Status.FAIL, verdicts, failure, refereeMatched[0]);
            }
            passed = true;
            observer.bodyPassed(conn, effectful);
            if (verdicts.isEmpty()) {
                // facts.verdicts() is false here: the program reaches no
                // verdict function — nothing was proved
                return new Result(t.fqn(), Status.SKIPPED, verdicts,
                        "no assertion reachable (the program calls no verdict function)", refereeMatched[0]);
            }
            return new Result(t.fqn(), Status.PASS, verdicts, verdicts.size() + " verdict(s)", refereeMatched[0]);
        } finally {
            if (!passed) {
                observer.bodyFailed(conn, effectful);
            }
        }
    }

    /** The platform's message, WHOLE, on one line: a runner never truncates
     * what the platform said; lines join with {@code " | "} so every failure
     * stays one greppable line. */
    public static String whole(@com.legend.Nullable String s) {
        if (s == null) {
            return "null";
        }
        return s.strip().replaceAll("\\s*\\R\\s*", " | ");
    }
}
