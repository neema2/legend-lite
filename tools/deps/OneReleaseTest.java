package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testing.Repo;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * ONE RELEASE: MODULE.bazel's LEGEND_ENGINE_RELEASE / LEGEND_PURE_RELEASE (the jars
 * and the source archives) and tools/oracle-pins.env (what the tests read the
 * release from) name the same release, and the pinned tags are that release's.
 * //tools/bump writes both; this fails the build if they ever part — the checks
 * tools/version-report.sh made against the poms.
 */
class OneReleaseTest {

    @Test
    void theModuleAndThePinsNameOneRelease() throws IOException {
        String module = Files.readString(Repo.path("MODULE.bazel"));
        Map<String, String> pins = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Repo.path("tools", "oracle-pins.env"))) {
            int eq = line.indexOf('=');
            if (!line.startsWith("#") && eq > 0) {
                pins.put(line.substring(0, eq).trim(), line.substring(eq + 1).trim());
            }
        }
        String engine = constant(module, "LEGEND_ENGINE_RELEASE");
        String pure = constant(module, "LEGEND_PURE_RELEASE");
        assertEquals(engine, pins.get("LEGEND_ENGINE_RELEASE"), "MODULE.bazel and tools/oracle-pins.env name different engine releases");
        assertEquals(pure, pins.get("LEGEND_PURE_RELEASE"), "MODULE.bazel and tools/oracle-pins.env name different pure releases");
        assertEquals("legend-engine-" + engine, pins.get("LEGEND_ENGINE_DESCRIBE"), "the pinned engine tag is not the release's");
        assertEquals("legend-pure-" + pure, pins.get("LEGEND_PURE_DESCRIBE"), "the pinned pure tag is not the release's");
    }

    private static String constant(String module, String name) {
        Matcher m = Pattern.compile("(?m)^" + name + " = \"([^\"]+)\"").matcher(module);
        if (!m.find()) {
            throw new IllegalStateException("MODULE.bazel declares no " + name);
        }
        return m.group(1);
    }
}
