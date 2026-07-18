package com.legend.pctcorpus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * PCT suite access — reads the REAL legend-pure / legend-engine checkouts
 * IN PLACE (the rcorpus {@code Corpus} pattern): no copies, no resource
 * embedding, zero munging of the .pure text. Roots are overridable via
 * {@code -Dlegend.pure.root} / {@code -Dlegend.engine.root}; the default
 * assumes sibling checkouts of the legend-lite repo (true on every dev
 * machine so far: {@code ~/legend/legend-{lite,pure,engine}}).
 *
 * <p>A suite is a {@code (name, root directory, package)} scope — the same
 * shape as upstream's {@code ReportScope(module, _package, filePath)}. The
 * denominator is reality: every {@code <<PCT.test>>} under the root counts,
 * discovered or not.
 */
final class PctCorpus {

    private PctCorpus() { }

    /** legend-lite repo root: surefire's working dir is the module dir. */
    static final Path REPO_ROOT = Path.of("").toAbsolutePath().getParent();

    static final Path PURE_ROOT = Path.of(System.getProperty(
            "legend.pure.root",
            REPO_ROOT.resolveSibling("legend-pure").toString()));

    static final Path ENGINE_ROOT = Path.of(System.getProperty(
            "legend.engine.root",
            REPO_ROOT.resolveSibling("legend-engine").toString()));

    record Suite(String name, Path root, String pkg) { }

    static boolean available() {
        return suites().stream().allMatch(s -> Files.isDirectory(s.root()));
    }

    static List<Suite> suites() {
        Path platform = PURE_ROOT.resolve(
                "legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure");
        Path enginePure = ENGINE_ROOT.resolve(
                "legend-engine-core/legend-engine-core-pure");
        return List.of(
                new Suite("essential",
                        platform.resolve("essential"),
                        "meta::pure::functions"),
                new Suite("grammar",
                        platform.resolve("grammar/functions"),
                        "meta::pure::functions"),
                new Suite("relation",
                        enginePure.resolve("legend-engine-pure-code-functions-relation/"
                                + "legend-engine-pure-functions-relation-pure/"
                                + "src/main/resources/core_functions_relation"),
                        "meta::pure::functions::relation"),
                new Suite("standard",
                        enginePure.resolve("legend-engine-pure-code-functions-standard/"
                                + "legend-engine-pure-functions-standard-pure/"
                                + "src/main/resources/core_functions_standard"),
                        "meta::pure::functions"),
                new Suite("unclassified",
                        enginePure.resolve("legend-engine-pure-code-functions-unclassified/"
                                + "legend-engine-pure-functions-unclassified-pure/"
                                + "src/main/resources/core_functions_unclassified"),
                        "meta::pure::functions"),
                new Suite("variant",
                        enginePure.resolve("legend-engine-pure-code-functions-variant/"
                                + "legend-engine-pure-functions-variant-pure/"
                                + "src/main/resources/core_functions_variant"),
                        "meta::pure::functions::variant"));
    }
}
