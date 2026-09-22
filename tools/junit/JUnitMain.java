package com.legend.tools.junit;

import java.util.ArrayList;
import java.util.List;

import org.junit.platform.console.ConsoleLauncher;

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
 */
public final class JUnitMain {

    private JUnitMain() {}

    public static void main(String[] args) {
        List<String> all = new ArrayList<>(List.of(args));
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            all.add("--reports-dir=" + outputs + "/junit");
        }
        ConsoleLauncher.main(all.toArray(String[]::new));
    }
}
