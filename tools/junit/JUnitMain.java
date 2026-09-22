package com.legend.tools.junit;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.platform.console.ConsoleLauncher;
import org.junit.platform.console.options.CommandResult;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

/**
 * The entry point every Bazel test target runs: JUnit's console launcher, told
 * where Bazel wants its reports.
 *
 * <p>Why a class and not arguments in a BUILD file. The launcher must write its
 * XML reports into {@code $TEST_UNDECLARED_OUTPUTS_DIR}, a directory Bazel names
 * only at run time, in an environment variable. BUILD {@code args} are not
 * shell-expanded, so {@code --reports-dir=${TEST_UNDECLARED_OUTPUTS_DIR}/junit}
 * arrives as that literal text and the launcher creates a directory of that
 * name in the runfiles tree — a test writing where tests must never write. A
 * shell wrapper would expand it, but nothing in this build's required path may
 * need a shell (docs/STANDARD_BUILD_PROGRAM.md rule 5: Windows has none).
 *
 * <p>Arguments are passed through unchanged; the reports directory is added
 * only under Bazel. Outside Bazel this behaves exactly like ConsoleLauncher.
 *
 * <p>THE RATCHET. A lane with a KNOWN, ledgered set of failures (gate 7, PCT on
 * H2) cannot be a plain pass/fail test. With
 * {@code -Dlegend.ratchet=minRun=N,maxFailures=F,maxErrors=E} the target passes
 * when at least N tests ran and at most F failed and E errored — the pins
 * tools/allgates.sh held, counted the way surefire counts them (a test whose
 * throwable is an {@link AssertionError} FAILED; any other throwable, or a
 * container that failed, ERRORED). Fewer failures than pinned passes and says
 * so: ratchet the pin down in the same change that earned it.
 */
public final class JUnitMain {

    private JUnitMain() {}

    public static void main(String[] args) {
        List<String> all = new ArrayList<>(List.of(args));
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            all.add("--reports-dir=" + outputs + "/junit");
        }
        String ratchet = System.getProperty("legend.ratchet");
        if (ratchet == null) {
            ConsoleLauncher.main(all.toArray(String[]::new));
            return;
        }
        System.exit(ratchet(ratchet, all.toArray(String[]::new)));
    }

    private static int ratchet(String spec, String[] args) {
        Map<String, Long> pins = new HashMap<>();
        for (String pin : spec.split(",")) {
            String[] kv = pin.split("=", 2);
            pins.put(kv[0].trim(), Long.parseLong(kv[1].trim()));
        }
        long minRun = required(pins, "minRun");
        long maxFailures = required(pins, "maxFailures");
        long maxErrors = required(pins, "maxErrors");

        PrintWriter out = new PrintWriter(System.out, true);
        PrintWriter err = new PrintWriter(System.err, true);
        CommandResult<?> result = ConsoleLauncher.run(out, err, args);
        TestExecutionSummary summary = result.getValue()
                .filter(TestExecutionSummary.class::isInstance)
                .map(TestExecutionSummary.class::cast)
                .orElseThrow(() -> new IllegalStateException(
                        "the launcher returned no execution summary — nothing ran"));

        long run = summary.getTestsStartedCount() + summary.getTestsSkippedCount();
        long failures = 0;
        long errors = summary.getContainersFailedCount();
        for (TestExecutionSummary.Failure f : summary.getFailures()) {
            if (f.getTestIdentifier().isContainer()) {
                continue; // counted above
            }
            if (f.getException() instanceof AssertionError) {
                failures++;
            } else {
                errors++;
            }
        }
        out.printf("[ratchet] Tests run: %d, Failures: %d, Errors: %d"
                + " (pins: run >= %d, failures <= %d, errors <= %d)%n",
                run, failures, errors, minRun, maxFailures, maxErrors);
        boolean ok = run >= minRun && failures <= maxFailures && errors <= maxErrors;
        if (ok && (failures < maxFailures || errors < maxErrors)) {
            out.println("[ratchet] IMPROVED — lower the pins in the same change that earned it");
        }
        if (!ok) {
            out.println("[ratchet] BROKEN — the lane regressed past its pins");
        }
        return ok ? 0 : 1;
    }

    private static long required(Map<String, Long> pins, String name) {
        Long v = pins.get(name);
        if (v == null) {
            throw new IllegalArgumentException("-Dlegend.ratchet is missing " + name);
        }
        return v;
    }
}
