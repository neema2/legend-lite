package com.legend.tools.deps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
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
        Matcher m = INSTALL.matcher(Files.readString(Runfile.property("module.bazel")));
        TreeSet<String> module = new TreeSet<>();
        while (m.find()) {
            module.add(m.group(1));
        }
        List<String> listed = Arrays.asList(System.getProperty("pools").split(","));
        assertEquals(List.copyOf(module), listed.stream().sorted().toList(),
                "MODULE.bazel's maven.install pools and tools/deps/pools.bzl's POOL_USERS differ");
    }
}
