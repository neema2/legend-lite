package perf;

import java.io.File;
import java.nio.file.Paths;
import java.util.StringJoiner;

/**
 * THE JVM'S CLASS PATH, MADE ABSOLUTE before legend-engine reads it (2026-11-19, the S383 probe: no storeless
 * function test ran).
 *
 * <p>legend-engine compiles the Java its plan holds for a test that reads from no store. {@code EngineJavaCompiler}
 * gives javac {@code -classpath new ClassGraph().getClasspath()}. ClassGraph 4.8.25, the version the engine's BOM gives
 * this pool, silently drops every class-path entry that starts with {@code ../}. Measured, with one jar on
 * {@code -cp}: {@code ../lib/cg.jar} is dropped, an absolute path to the same jar is kept, and so is one with
 * {@code /../} inside it.
 *
 * <p>Bazel's launcher writes the class path relative to the working directory. Under {@code bazel run} that is the
 * runfiles' {@code _main}, so every jar of an external repository, all of the pool's jars, is written
 * {@code ../rules_jvm_external++maven+maven_runner/...}. ClassGraph then kept 47 of the 666 entries, the main
 * repository's alone. javac never saw {@code legend-engine-executionPlan-dependencies}, and every such test failed
 * with "package org.finos.legend.engine.plan.dependencies.store.shared does not exist". The jar was on the JVM's class
 * path the whole time.
 *
 * <p>So the launch is corrected here, in the runner, before any engine class reads the property: each entry is
 * resolved against the working directory the JVM started in, which is what the JVM itself did with it. The JVM's own
 * class loader read the class path at startup and is not changed. Only readers of the property see the absolute form.
 * An absolute entry, and every entry when the launcher already wrote absolute paths, is unchanged.
 *
 * <p>The corpus met the same cause first (F94, 2026-11-01: a graph fetch's plan is Java too). scripts/corpus/run.py
 * launches the runner with {@code --classpath_limit=0}, so the stub writes a manifest jar of absolute URLs, and the
 * class path this sees is that one absolute jar. This covers every other launch: {@code bazel run}, the README's
 * command, and a caller that does not pass the flag.
 */
final class AbsoluteClassPath
{
    private AbsoluteClassPath()
    {
    }

    static void install()
    {
        String classPath = System.getProperty("java.class.path");
        if (classPath == null || classPath.isEmpty())
        {
            return;
        }
        StringJoiner absolute = new StringJoiner(File.pathSeparator);
        for (String entry : classPath.split(File.pathSeparator, -1))
        {
            absolute.add(entry.isEmpty() ? entry : Paths.get(entry).toAbsolutePath().toString());
        }
        System.setProperty("java.class.path", absolute.toString());
    }
}
