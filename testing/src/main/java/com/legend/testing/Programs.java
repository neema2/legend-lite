// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * What a measurement or probe PROGRAM needs (Bazel workplan P3-17): such a program is a report action
 * ({@code java_run}: its inputs declared, its report a cached output) or a {@code bazel run} binary, never a test
 * that asserts nothing.
 */
public final class Programs {

    private Programs() {}

    /** Sends this JVM's stdout and stderr to {@code outDir/run.log}, a declared output: what the corpus machinery and
     *  the engine's parsers print (progress, ANTLR's syntax errors, SLF4J) never reaches the build's console. A
     *  failure still does: Bazel discards a failed action's outputs, run.log with them, so an uncaught exception is
     *  printed on the console the action started with. */
    public static void captureConsole(Path outDir) throws IOException {
        PrintStream console = System.err;
        Thread.setDefaultUncaughtExceptionHandler((thread, e) -> e.printStackTrace(console));
        PrintStream log = new PrintStream(Files.newOutputStream(outDir.resolve("run.log")), true, StandardCharsets.UTF_8);
        System.setOut(log);
        System.setErr(log);
    }

    /** A path argument of a {@code bazel run} binary, from the directory it was started in (it runs in its runfiles). */
    public static Path argument(String path) {
        String cwd = System.getenv("BUILD_WORKING_DIRECTORY");
        return cwd == null ? Path.of(path) : Path.of(cwd).resolve(path);
    }

    /** The value after {@code --name} in {@code args}, or null. */
    public static String option(String[] args, String name) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return null;
    }

    /** {@code args} without {@code --out FILE}: the program's own arguments. */
    public static String[] withoutOut(String[] args) {
        java.util.List<String> rest = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--out") && i + 1 < args.length) {
                i++;
            } else {
                rest.add(args[i]);
            }
        }
        return rest.toArray(String[]::new);
    }

    /** {@code --out FILE}, when given: stdout goes to that file from here on. */
    public static void stdoutToOut(String[] args) throws IOException {
        String out = option(args, "--out");
        if (out != null) {
            System.setOut(new PrintStream(Files.newOutputStream(argument(out)), true, StandardCharsets.UTF_8));
        }
    }
}
