package com.legend.equivalence.harvest;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * GENERATES the engine fixture snapshot (parser-equivalence's
 * {@code engine-grammar-fixtures.jsonl}, corpus tier C6): tier 1 from the
 * engine's published grammar/compiler tests-jars on this program's classpath,
 * tier 2 compiled from the pinned checkout's extension test sources, both run
 * under the recording shims, deduped into one snapshot — {@link FixtureHarvest}.
 *
 * <pre>
 *   FixtureHarvestGenerator &lt;output&gt;
 *     -Dlegend.engine.root  the pinned engine tree (tier 2's sources)
 *     -Dlegend.repo.root / -Dlegend.repo.module  the tree the recorder reads the pin from
 * </pre>
 *
 * The classpath is the harvest's own (parser-equivalence's :harvest_lib): the
 * shims FIRST, so every test(...) records instead of asserting.
 */
public final class FixtureHarvestGenerator {

    private FixtureHarvestGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: FixtureHarvestGenerator <output>");
        }
        Path work = Files.createTempDirectory("harvest-");
        Path dump = work.resolve("engine-fixtures.jsonl");
        // before FixtureRecorder loads: it reads the dump location once
        System.setProperty("fixture.dump", dump.toString());
        requireShims();

        // the grammar and compiler tests-jars, in that order: declared by the BUILD
        // file (java_jars), not found on the class path
        List<String> testJars = new ArrayList<>();
        for (Path jar : com.legend.testing.Repo.listed("legend.harvest.jars")) {
            testJars.add(jar.toString());
        }
        if (testJars.size() != 2) {
            throw new IllegalStateException("expected the grammar and compiler tests-jars, declared, found "
                    + testJars);
        }
        // tier 2 compiles against this program's own class path, handed to javac
        // whole: the JDK reads a launcher's manifest jar itself
        String classpath = System.getProperty("java.class.path");
        ClassLoader loader = FixtureHarvestGenerator.class.getClassLoader();
        System.out.println("@@ tier 1 " + FixtureHarvest.tier1(testJars, loader));

        Path t2 = work.resolve("tier2-classes");
        FixtureHarvest.compileTier2(com.legend.testing.Upstream.engine(), t2, classpath, System.out);
        System.out.println("@@ tier 2 " + FixtureHarvest.tier2(t2, loader));

        String snapshot = FixtureHarvest.snapshot(dump);
        Files.writeString(Path.of(args[0]), snapshot, StandardCharsets.UTF_8);
        System.out.println("@@ snapshot: " + (snapshot.lines().count() - 1) + " fixtures");
    }

    /** The recording shims are the classes that load (Bazel workplan P3-31): each from the jar FixtureRecorder comes
     *  from (:harvest_shims), never the tests-jars' own class of the same name, which records nothing. Loaded, not
     *  initialized. */
    private static void requireShims() throws ClassNotFoundException {
        ClassLoader loader = FixtureHarvestGenerator.class.getClassLoader();
        Object shims = Class.forName("com.legend.equivalence.harvest.FixtureRecorder", false, loader)
                .getProtectionDomain().getCodeSource().getLocation();
        for (String shim : List.of(
                "org.finos.legend.engine.language.pure.compiler.test.TestCompilationFromGrammar",
                "org.finos.legend.engine.language.pure.grammar.test.TestGrammarParser",
                "org.finos.legend.engine.language.pure.grammar.test.TestGrammarRoundtrip")) {
            Object from = Class.forName(shim, false, loader).getProtectionDomain().getCodeSource().getLocation();
            if (!shims.equals(from)) {
                throw new IllegalStateException(shim + " loads from " + from + ", not the shims' jar " + shims
                        + ": the harvest would record nothing (put :harvest_shims first on the class path)");
            }
        }
    }
}
