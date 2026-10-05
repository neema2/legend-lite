package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * CORE STANDS ALONE. {@code //core:core} reaches NO external jar: it speaks
 * {@code java.sql} and the JDK, nothing else. The JDBC drivers are
 * {@code //core:drivers}, a runtime choice for whatever runs core, and they are
 * product jars (tools/deps/jars.bzl, Bazel's http_jar) and nothing else. Nothing pct,
 * parser-equivalence or spec needs — legend-engine, legend-pure, their BOM, test
 * tooling — can reach the product, because a native image or a WASM build of
 * core carries every jar core reaches.
 *
 * <p>Both closures are Bazel's own answer (genqueries in this package), so this
 * checks the build graph itself, not a description of it. Adding a jar means
 * editing this test in the same change, with the reason.
 */
class CoreClosureTest {

    /** The drivers a running core is given — the product's whole external
     *  surface, and none of it needed to compile or to plan. */
    private static final List<String> DRIVER_JARS = List.of(
            "duckdb_jdbc",
            "h2",
            // 2026-10-03 (Bazel workplan P0-08): the server's Postgres arm (ConnectionResolver opens
            // jdbc:postgresql://) had no driver, so it failed with "No suitable driver" in the server and
            // its deploy jar. 2026-10-05 (docs/BUILD_REBUILD_DESIGN_2026_10_05.md, 5b): the driver alone, without
            // the checker-qual its POM declares: annotations only, ignored at run time when absent.
            "postgresql",
            "sqlite_jdbc");

    /** The product jars' repositories: @@+product_jars+<name>//jar:jar (tools/deps/jars.bzl). */
    private static final String PRODUCT_JAR = "@@+product_jars+";

    @Test
    void coreReachesNoJarAtAll() throws IOException {
        assertEquals(List.of(), jars("core_closure"),
                "core reaches an external jar — core compiles against the JDK alone;"
                        + " a runtime jar belongs on //core:drivers or the target that runs core");
    }

    @Test
    void theDriversAreCoresPoolAndOnlyTheNamedOnes() throws IOException {
        List<String> jars = jars("drivers_closure");
        for (String jar : jars) {
            assertTrue(jar.startsWith(PRODUCT_JAR) && jar.endsWith("//jar:jar"),
                    () -> "a driver is not a product jar (tools/deps/jars.bzl): " + jar);
        }
        assertEquals(DRIVER_JARS,
                jars.stream().map(j -> j.substring(PRODUCT_JAR.length(), j.indexOf("//"))).toList(),
                "the drivers changed — edit DRIVER_JARS in the same change, with the reason");
    }

    @Test
    void specReachesNoUpstreamJar() throws IOException {
        List<String> jars = jars("spec_closure");
        assertTrue(!jars.isEmpty(), "the spec closure query returned nothing — the guard is not looking");
        for (String jar : jars) {
            assertTrue(!jar.contains("maven_upstream//:") && !jar.contains("maven_runner//:"),
                    () -> "spec reaches an upstream or engine-runner jar: " + jar
                            + " — spec reads the pinned checkouts as files, never their Java");
        }
    }

    private static List<String> jars(String closure) throws IOException {
        return Files.readAllLines(Runfile.property("closure." + closure)).stream()
                .filter(l -> !l.isBlank())
                .sorted()
                .toList();
    }
}
