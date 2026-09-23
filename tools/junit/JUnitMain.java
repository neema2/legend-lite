package com.legend.tools.junit;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
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
 *
 * <p>OUTPUT PATHS. A system property may name {@code ${TEST_UNDECLARED_OUTPUTS_DIR}};
 * it is replaced here, portably, by the directory Bazel collects into
 * {@code bazel-testlogs/.../test.outputs} (no shell expands jvm_flags on Windows).
 *
 * <p>THE PRERUN. With {@code -Dlegend.prerun=k=v,k=v} the same selection first
 * runs in a CHILD JVM — same classpath, same JVM flags, plus those properties —
 * and must pass before this JVM runs it. For a lane whose second pass reads what
 * a first, differently configured pass wrote (gate 11: the host judge's ledger,
 * then the database judge joined to it per assert). A child JVM, not a second
 * launch in this one: each pass must start from clean static state.
 */
public final class JUnitMain {

    private JUnitMain() {}

    private static final String OUTPUTS_TOKEN = "${TEST_UNDECLARED_OUTPUTS_DIR}";

    public static void main(String[] args) throws IOException, InterruptedException {
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        expandOutputs(outputs);
        String prerun = System.getProperty("legend.prerun");
        if (prerun != null) {
            int code = prerun(prerun, args, outputs);
            if (code != 0) {
                System.err.println("[prerun] the first pass failed (exit " + code + ") — the second is not run");
                System.exit(code);
            }
        }
        List<String> all = new ArrayList<>(List.of(args));
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

    /** Replaces the outputs token in every system property. Outside Bazel there is
     *  no outputs directory, and a property that names one is an error. */
    private static void expandOutputs(String outputs) {
        for (String key : System.getProperties().stringPropertyNames()) {
            String value = System.getProperty(key);
            if (value != null && value.contains(OUTPUTS_TOKEN)) {
                if (outputs == null) {
                    throw new IllegalStateException("-D" + key + " names " + OUTPUTS_TOKEN
                            + ", which only a Bazel test has");
                }
                System.setProperty(key, value.replace(OUTPUTS_TOKEN, outputs));
            }
        }
    }

    private static int prerun(String spec, String[] args, String outputs)
            throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        for (String flag : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (!flag.startsWith("-Dlegend.prerun=") && !flag.startsWith("-Dlegend.ratchet=")) {
                command.add(flag.contains(OUTPUTS_TOKEN) && outputs != null
                        ? flag.replace(OUTPUTS_TOKEN, outputs) : flag);
            }
        }
        for (String pair : spec.split(",")) {
            String p = outputs != null ? pair.replace(OUTPUTS_TOKEN, outputs) : pair;
            command.add("-D" + p.trim());
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(JUnitMain.class.getName());
        command.addAll(List.of(args));
        System.out.println("[prerun] " + spec);
        ProcessBuilder builder = new ProcessBuilder(command).inheritIO();
        if (outputs != null) {
            // the first pass's own reports and outputs land beside, not over, the
            // second's: test.outputs/prerun/...
            Path own = Path.of(outputs, "prerun");
            java.nio.file.Files.createDirectories(own);
            builder.environment().put("TEST_UNDECLARED_OUTPUTS_DIR", own.toString());
        }
        Process child = builder.start();
        return child.waitFor();
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
