package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testing.Repo;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Every artifact belongs to ONE classpath pool (MODULE.bazel): a test that uses two
 * pools must get one copy of each jar, not one per pool. The tools pool is left
 * out — its jars are compiler plugins and never on a classpath.
 */
class PoolsAreDisjointTest {

    private static final String[] POOLS = {"core", "h2_modern", "test", "upstream"};

    /** Overlaps that are known and safe, each with why. */
    private static final Map<String, String> ALLOWED = Map.of(
            // two H2s by design: core's 2.1.214 and the 2.4.240 gate 7 runs on.
            // Alternatives, never together — //pct:pct_h2 is given the modern
            // one and no other (2026-09-22).
            "com.h2database:h2", "core+h2_modern",
            // ArchUnit (test) logs through slf4j 2.0.12; the engine (upstream)
            // through 1.7.36. Only core and spec use ArchUnit, and neither uses
            // an upstream jar, so no classpath holds both (2026-09-22).
            "org.slf4j:slf4j-api", "test+upstream");

    private static final Pattern ARTIFACT = Pattern.compile(
            "^    \"([^\"]+:[^\"]+)\": \\{", Pattern.MULTILINE);

    @Test
    void noArtifactIsInTwoPools() throws IOException {
        Map<String, Set<String>> owners = new TreeMap<>();
        for (String pool : POOLS) {
            String lock = Files.readString(Repo.path("maven_" + pool + "_install.json"));
            String artifacts = lock.substring(lock.indexOf("\"artifacts\": {"),
                    lock.indexOf("\n  },", lock.indexOf("\"artifacts\": {")));
            Matcher m = ARTIFACT.matcher(artifacts);
            while (m.find()) {
                owners.computeIfAbsent(m.group(1), k -> new TreeSet<>()).add(pool);
            }
        }
        Map<String, String> shared = new TreeMap<>();
        owners.forEach((artifact, pools) -> {
            if (pools.size() > 1) {
                shared.put(artifact, String.join("+", pools));
            }
        });
        assertEquals(new TreeMap<>(ALLOWED), shared,
                "an artifact is in two jar pools — exclude it from the pool that does not"
                        + " own it (MODULE.bazel excluded_artifacts)");
    }
}
