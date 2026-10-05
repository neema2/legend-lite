package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** tools/deps/pools.bzl's POOLS is exactly MODULE.bazel's maven.install pools (Bazel workplan P1-25): a pool added to
 *  one and not the other fails here, so the pool-use check and the lock exports never miss one. */
class PoolsListTest {

    private static final Pattern INSTALL = Pattern.compile("(?m)^maven\\.install\\(\\n    name = \"([^\"]+)\",");

    @Test
    void poolsBzlNamesEveryPoolInModuleBazel() throws IOException {
        Matcher m = INSTALL.matcher(module());
        TreeSet<String> module = new TreeSet<>();
        while (m.find()) {
            module.add(m.group(1));
        }
        List<String> listed = Arrays.asList(System.getProperty("pools").split(","));
        assertEquals(List.copyOf(module), listed.stream().sorted().toList(),
                "MODULE.bazel's maven.install pools and tools/deps/pools.bzl's POOL_USERS differ");
    }

    /** A testonly pool stays testonly as a whole: its maven.install lists exactly one variable, every entry of which
     *  is amended testonly, and no other tag adds a root to it (a root that is not testonly would make whatever only
     *  it reaches usable outside tests). */
    @Test
    void testonlyPoolsAreTestonlyAsAWhole() throws IOException {
        String module = module();
        for (String pool : System.getProperty("testonly.pools").split(",")) {
            Matcher install = Pattern.compile("(?m)^maven\\.install\\(\\n    name = \"" + Pattern.quote(pool)
                    + "\",\\n    artifacts = (_[A-Z]+_ARTIFACTS),\\n").matcher(module);
            assertTrue(install.find(), pool + ": its maven.install must list one _..._ARTIFACTS variable");
            String variable = install.group(1);
            String amend = "[maven.amend_artifact(\n    name = \"" + pool + "\",\n    coordinates = coordinates,\n"
                    + "    testonly = \"true\",\n) for coordinates in " + variable + "]";
            assertTrue(module.contains(amend), pool + ": every entry of " + variable + " must be amended testonly");
            assertFalse(Pattern.compile("maven\\.artifact\\(\\s*name = \"" + Pattern.quote(pool) + "\"").matcher(module).find(),
                    pool + ": a maven.artifact tag adds a root that is not testonly");
        }
    }

    /** Every segment MODULE.bazel include()s is read here (and by //tools/guards:locks_test): a pool in a segment
     *  the list missed would escape both. */
    @Test
    void everyIncludedSegmentIsRead() throws IOException {
        List<Path> files = Runfile.envList("MODULE_FILES");
        Path main = files.stream().filter(f -> f.getFileName().toString().equals("MODULE.bazel")).findFirst()
                .orElseThrow(() -> new AssertionError("//:module_files holds no MODULE.bazel: " + files));
        TreeSet<String> included = new TreeSet<>();
        Matcher m = Pattern.compile("(?m)^include\\(\"//:([^\"]+)\"\\)").matcher(Files.readString(main));
        while (m.find()) {
            included.add(m.group(1));
        }
        TreeSet<String> read = new TreeSet<>();
        files.forEach(f -> read.add(f.getFileName().toString()));
        read.remove("MODULE.bazel");
        assertEquals(included, read, "MODULE.bazel's include()s and //:module_files differ");
        assertTrue(included.contains("release.MODULE.bazel"), "the release segment is included");
    }

    /** MODULE.bazel and every segment it includes, as one text (//:module_files). */
    static String module() throws IOException {
        StringBuilder out = new StringBuilder();
        for (Path f : Runfile.envList("MODULE_FILES")) {
            out.append(Files.readString(f)).append('\n');
        }
        return out.toString();
    }
}
