// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * THE LANE'S FLOOR (the result boundary program's track T, T-d, 2026-10-29): core's integration tests run on each
 * database beside PCT's lanes. A run names its lane by the system property {@link #LANE_PROPERTY} (a backend's name,
 * {@link TestBackends#named}: {@code sqlite}, {@code h2}, {@code mongodb}, ...; unset, the platform's own engine, and
 * nothing here applies). On a lane, every test is held to the lane's RECORDED floor, the classpath resource
 * {@code lanes/<lane>.txt}: one line per test that fails on that lane today, {@code Class#method} (the class without
 * its package, a nested class with {@code $}; a parameterized invocation {@code Class#method[#N]}, its ordinal), a
 * tab, the reason (a dialect gap, a database's own rule, a defect by its register row). A listed test that fails
 * passes the run, printed; a listed test that PASSES fails the run, asking for its line to go (the floor only
 * shrinks); a failing test the floor does not list fails the run, as on the platform; a listed test whose set-up fails
 * is not run (reported successful, said in the log); a line whose reason opens {@code not run <date>:} PINS its test
 * as not run on the lane, for a failure that would end the run (one JUnit rethrows as unrecoverable, say the
 * database's planner exhausting the heap) -- dated, with its cause, moved only with one. Every integration test is on the lane: the subclasses of
 * {@code AbstractDatabaseTest} through its {@code backend()}, every other through {@link #backend()} where it opened
 * the platform's database itself (T-d2). The floor is a measurement, never a judgment: a line moves only with the
 * reason in the commit (AGENTS.md: a ceiling, pin or allowlist entry moves only with a dated justification).
 */
public final class LaneFloor implements InvocationInterceptor {

    /** The system property that names the lane; unset, the platform's own engine. */
    public static final String LANE_PROPERTY = "legend.test.lane";

    private static final Map<String, Map<String, String>> FLOORS = new java.util.concurrent.ConcurrentHashMap<>();

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(LaneFloor.class);
    private static final String SET_UP_FAILED = "set-up failed";

    /** The lane's name, or null on the platform's lane. */
    public static @com.legend.base.Nullable String lane() {
        String lane = System.getProperty(LANE_PROPERTY);
        return lane == null || lane.isBlank() ? null : lane;
    }

    /** The lane's backend: the one {@link #LANE_PROPERTY} names, else the platform's. */
    public static TestBackend backend() {
        String lane = lane();
        return lane == null ? TestBackends.platform() : TestBackends.named(lane);
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
            ExtensionContext extensionContext) throws Throwable {
        judge(invocation, key(extensionContext), extensionContext);
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
        judge(invocation, key(extensionContext), extensionContext);
    }

    /** A listed test's set-up may be what fails on the lane (a table the dialect cannot create): on the floor too. */
    @Override
    public void interceptBeforeEachMethod(Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
        around(invocation, extensionContext, "set-up");
    }

    @Override
    public void interceptAfterEachMethod(Invocation<Void> invocation,
            ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
        around(invocation, extensionContext, "tear-down");
    }

    private static void around(Invocation<Void> invocation, ExtensionContext ctx, String what) throws Throwable {
        String lane = lane();
        String key = ctx.getTestMethod().isPresent() ? key(ctx) : null;
        String reason = lane == null || key == null ? null : floor(lane).get(key);
        if (reason == null) {
            invocation.proceed();
            return;
        }
        try {
            invocation.proceed();
        } catch (Throwable t) {
            if (what.equals("set-up")) {
                ctx.getStore(NAMESPACE).put(SET_UP_FAILED, true);
            }
            System.out.println("[lane " + lane + "] on the floor, its " + what + ": " + key + " (" + reason + "): "
                    + t.getClass().getSimpleName() + ": " + excerpt(t));
        }
    }

    private static String excerpt(Throwable t) {
        String message = String.valueOf(t.getMessage());
        return message.length() > 200 ? message.substring(0, 200) + "..." : message;
    }

    /** A measured floor line's reason opens {@code measured <date>: }; the message after it, else null (a pin, or a
     *  line written by hand without a measurement). */
    static @com.legend.base.Nullable String measuredMessage(String reason) {
        if (!reason.startsWith(MEASURED)) {
            return null;
        }
        int colon = reason.indexOf(": ");
        return colon < 0 ? null : reason.substring(colon + 2);
    }

    /** A failure's message as the floor's generator records it: an execution trace id struck, whitespace collapsed. */
    static String normalized(String message) {
        return TRACE_ID.matcher(message).replaceAll("").replaceAll("\\s+", " ").trim();
    }

    static String prefix(String s) {
        return s.length() > REASON_PREFIX ? s.substring(0, REASON_PREFIX) : s;
    }

    /** {@code Class#method}, the class without its package; an invocation of a template (a parameterized or repeated
     *  test) with its ordinal, {@code Class#method[#N]}, as JUnit names it in the context's unique id
     *  ({@code [test-template-invocation:#N]}) and the run's report names it ({@code method(...)[N]}). */
    static String key(ExtensionContext ctx) {
        String cls = ctx.getRequiredTestClass().getName();
        int dot = cls.lastIndexOf('.');
        String name = cls.substring(dot + 1) + "#" + ctx.getRequiredTestMethod().getName();
        java.util.regex.Matcher m = INVOCATION.matcher(ctx.getUniqueId());
        return m.find() ? name + "[#" + m.group(1) + "]" : name;
    }

    /** A floor line's reason that pins its test as not run on the lane: {@code not run <date>: <cause>}. */
    static final String NOT_RUN = "not run ";

    /** A measured floor line's reason: {@code measured <date>: <the failure's message>}. */
    static final String MEASURED = "measured ";

    /** The characters of a failure's message a measured line is held to (the floor's generator keeps 120; a message's
     *  variable tail -- a sandbox path, a statement -- lies past the reason's words). */
    static final int REASON_PREFIX = 60;

    private static final java.util.regex.Pattern TRACE_ID =
            java.util.regex.Pattern.compile("-- \"executionTraceID\" : \"[0-9a-f-]+\"");

    private static final java.util.regex.Pattern INVOCATION = java.util.regex.Pattern.compile(
            "\\[test-template-invocation:#(\\d+)\\]");

    /** THE PLATFORM'S CONNECTION CLAUSE as every test spells "the database this test runs on" in its model text:
     *  DuckDB's own, whatever its whitespace. */
    static final java.util.regex.Pattern PLATFORM_CONNECTION = java.util.regex.Pattern.compile(
            "type:\\s*DuckDB;\\s*specification:\\s*DuckDB\\s*\\{\\s*\\};\\s*auth:\\s*Test;");

    /** {@code model} on the SESSION {@code c} is on: its platform connection clause ({@link #PLATFORM_CONNECTION},
     *  the spelling of "the database this test runs on") re-spelled as that session's backend's
     *  ({@code TestBackend.connectionBody}), so the same model runs on the lane's database; the text itself on the
     *  platform's lane, or for a session on the platform's database (a test named for it keeps its own session
     *  whatever the lane). A model that names a database by any other spelling is a test of that database and is
     *  not touched. The twin's reader and the program helper apply it to every model they run (T-d2, 2026-10-29:
     *  681 tests of the laned classes carried the platform's clause in their own text and could only report "the
     *  query executes on DuckDB"). */
    public static String model(String model, java.sql.Connection c) {
        if (lane() == null) {
            return model;
        }
        TestBackend on;
        try {
            on = TestBackends.of(c);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("the lane cannot tell the session's database", e);
        }
        return respell(model, on);
    }

    /** {@link #model(String, java.sql.Connection)} for a run with no caller's session: the lane's backend. */
    public static String model(String model) {
        return lane() == null ? model : respell(model, backend());
    }

    private static String respell(String model, TestBackend on) {
        if (on.type() == com.legend.database.Databases.PLATFORM) {
            return model;
        }
        return PLATFORM_CONNECTION.matcher(model).replaceAll(
                java.util.regex.Matcher.quoteReplacement(on.connectionBody()));
    }

    private static void judge(Invocation<Void> invocation, String key, ExtensionContext ctx) throws Throwable {
        String lane = lane();
        if (lane == null) {
            invocation.proceed();
            return;
        }
        String reason = floor(lane).get(key);
        if (reason == null) {
            invocation.proceed();
            return;
        }
        if (reason.startsWith(NOT_RUN)) {
            // a PIN, not a measurement: the test cannot be run on this lane without ending the run (a failure JUnit
            // rethrows as unrecoverable -- say the database's planner exhausting the heap), so it is NOT RUN, said in
            // the log; its line carries the date and the cause, and moves only with one (AGENTS.md's guard-file rule)
            invocation.skip();
            System.out.println("[lane " + lane + "] on the floor, not run: " + key + " (" + reason + ")");
            return;
        }
        if (ctx.getStore(NAMESPACE).get(SET_UP_FAILED) != null) {
            // its set-up failed on the floor: the test is NOT RUN against a broken set-up (which would pass or fail
            // for the wrong reason) -- JUnit reports the invocation successful, the run's log says it was not run --
            // and it stays on the floor until its set-up passes here
            invocation.skip();
            System.out.println("[lane " + lane + "] on the floor, not run (its set-up failed): " + key);
            return;
        }
        try {
            invocation.proceed();
        } catch (Throwable t) {
            // on the floor: the run passes; said, so a run's log shows the floor's weight -- but a failure for ANOTHER
            // reason than the line's measured one fails the run (the T-d2 audit's finding 9, 2026-10-29: a line
            // held only pass/fail, so a new failure behind a listed test stayed green): the message's first
            // REASON_PREFIX characters, normalized as the floor's generator normalized them, must be the line's
            String recorded = measuredMessage(reason);
            if (recorded != null) {
                String actual = prefix(normalized(String.valueOf(t.getMessage())));
                String expected = prefix(normalized(recorded));
                if (!actual.equals(expected)) {
                    throw new AssertionError("the lane's floor line changed its reason: " + key + " fails on " + lane
                            + " for '" + actual + "...', not the measured '" + expected + "...'; re-measure its line"
                            + " in lanes/" + lane + ".txt, with the reason in the commit", t);
                }
            }
            System.out.println("[lane " + lane + "] on the floor: " + key + " (" + reason + "): "
                    + t.getClass().getSimpleName() + ": " + excerpt(t));
            return;
        }
        throw new AssertionError("the lane's floor shrinks: " + key + " passes on " + lane + " now (the floor said: "
                + reason + "); remove its line from lanes/" + lane + ".txt, with the reason in the commit");
    }

    /** The lane's floor, read once: a lane with no file has an empty floor (every failure fails the run). */
    static Map<String, String> floor(String lane) {
        return FLOORS.computeIfAbsent(lane, LaneFloor::read);
    }

    private static Map<String, String> read(String lane) {
        Map<String, String> floor = new LinkedHashMap<>();
        try (InputStream in = LaneFloor.class.getResourceAsStream("/lanes/" + lane + ".txt")) {
            if (in == null) {
                return floor;
            }
            BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            int n = 0;
            while ((line = r.readLine()) != null) {
                n++;
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                int tab = line.indexOf('\t');
                if (tab < 0) {
                    throw new IllegalStateException("lanes/" + lane + ".txt line " + n + ": '<Class#method><TAB><reason>'"
                            + " expected, got '" + line + "'");
                }
                String key = line.substring(0, tab).trim();
                String reason = line.substring(tab + 1).trim();
                if (reason.isEmpty()) {
                    throw new IllegalStateException("lanes/" + lane + ".txt line " + n + ": " + key + " has no reason");
                }
                if (floor.put(key, reason) != null) {
                    throw new IllegalStateException("lanes/" + lane + ".txt line " + n + ": " + key + " listed twice");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return floor;
    }
}
