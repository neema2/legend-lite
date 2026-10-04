package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * G10 (Bazel workplan P6-10): every jar pool is locked and strict. Each maven.install in MODULE.bazel names its lock
 * (lock_file = "//:<pool>_install.json"), sets fail_if_repin_required = True (a pool edit without its repin fails
 * the build instead of resolving silently) and
 * strict_visibility = True (a BUILD file names only the jars its pool lists; P1-25).
 */
class LocksTest {

    private static final Pattern INSTALL = Pattern.compile("(?ms)^maven\\.install\\((.*?)^\\)");

    @Test
    void everyPoolIsLockedAndStrict() throws IOException {
        String module = Files.readString(Runfile.property("module.bazel"));
        Matcher m = INSTALL.matcher(module);
        List<String> missing = new ArrayList<>();
        int pools = 0;
        while (m.find()) {
            pools++;
            String body = m.group(1);
            Matcher name = Pattern.compile("name = \"([^\"]+)\"").matcher(body);
            String pool = name.find() ? name.group(1) : "?";
            for (String setting : List.of("fail_if_repin_required = True", "strict_visibility = True")) {
                if (!body.contains("\n    " + setting + ",")) {
                    missing.add(pool + ": " + setting);
                }
            }
            // fail_if_repin_required means nothing without a lock to hold the pool to
            if (!body.contains("\n    lock_file = \"//:" + pool + "_install.json\",")) {
                missing.add(pool + ": lock_file = \"//:" + pool + "_install.json\"");
            }
        }
        assertTrue(pools >= 8, "found " + pools + " maven.install pools in MODULE.bazel: the guard is not looking");
        assertEquals(List.of(), missing, "jar pools without their lock or strict-visibility setting");
    }
}
