package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Repo;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * CORE STANDS ALONE: every external jar core can reach at runtime comes from the
 * {@code @maven_core} pool (MODULE.bazel), and the pool is exactly the jars named
 * here. Nothing pct, parser-equivalence or spec needs — legend-engine, legend-pure,
 * their BOM, test tooling — can reach the product, because a native image or a
 * WASM build of core has to carry every jar core reaches.
 *
 * <p>The closure is Bazel's own answer ({@code :core_closure}, a genquery over
 * {@code deps(//core:core)} minus the NullAway compiler plugin), so this checks
 * the build graph itself, not a description of it. Adding a jar to core means
 * editing {@link #CORE_JARS} in the same change, with its reason.
 */
class CoreClosureTest {

    /** The whole external surface of the product. The drivers belong to the
     *  execution side; the planner needs none of them. */
    private static final List<String> CORE_JARS = List.of(
            "com_h2database_h2",
            "org_duckdb_duckdb_jdbc",
            "org_xerial_sqlite_jdbc");

    @Test
    void coreReachesOnlyItsOwnPool() throws IOException {
        List<String> jars = Files.readAllLines(Repo.module("core_closure")).stream()
                .filter(l -> !l.isBlank())
                .sorted()
                .toList();
        assertTrue(!jars.isEmpty(), "the closure query returned nothing — the guard is not looking");
        for (String jar : jars) {
            assertTrue(jar.contains("maven_core//:"),
                    () -> "core reaches a jar outside @maven_core: " + jar
                            + " — core's external surface is @maven_core alone");
        }
        assertEquals(CORE_JARS, jars.stream().map(j -> j.substring(j.indexOf("//:") + 3)).toList(),
                "core's jars changed — edit CORE_JARS in the same change, with the reason");
    }
}
