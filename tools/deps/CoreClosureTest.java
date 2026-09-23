package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Repo;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * CORE STANDS ALONE. {@code //core:core} reaches NO external jar: it speaks
 * {@code java.sql} and the JDK, nothing else. The JDBC drivers are
 * {@code //core:drivers}, a runtime choice for whatever runs core, and they come
 * from the {@code @maven_core} pool (MODULE.bazel) and nowhere else. Nothing pct,
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
            "com_h2database_h2",
            "org_duckdb_duckdb_jdbc",
            "org_xerial_sqlite_jdbc");

    @Test
    void coreReachesNoJarAtAll() throws IOException {
        assertEquals(List.of(), jars("core_closure"),
                "core reaches an external jar — core compiles against the JDK alone;"
                        + " a runtime jar belongs on //core:drivers or the target that runs core");
    }

    @Test
    void theDriversAreCoresPoolAndOnlyTheNamedThree() throws IOException {
        List<String> jars = jars("drivers_closure");
        for (String jar : jars) {
            assertTrue(jar.contains("maven_core//:"),
                    () -> "a driver comes from outside @maven_core: " + jar);
        }
        assertEquals(DRIVER_JARS, jars.stream().map(j -> j.substring(j.indexOf("//:") + 3)).toList(),
                "the drivers changed — edit DRIVER_JARS in the same change, with the reason");
    }

    @Test
    void specReachesNoUpstreamJar() throws IOException {
        List<String> jars = jars("spec_closure");
        assertTrue(!jars.isEmpty(), "the spec closure query returned nothing — the guard is not looking");
        for (String jar : jars) {
            assertTrue(!jar.contains("maven_upstream//:"),
                    () -> "spec reaches an upstream jar: " + jar
                            + " — spec reads the pinned checkouts as files, never their Java");
        }
    }

    private static List<String> jars(String closure) throws IOException {
        return Files.readAllLines(Repo.module(closure)).stream()
                .filter(l -> !l.isBlank())
                .sorted()
                .toList();
    }
}
