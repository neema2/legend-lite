package com.legend.equivalence.harvest;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/** HARVEST BY EXECUTION, tier 2: the checkout's EXTENSION grammar test
 *  sources, compiled per-file tolerantly against the shim classpath
 *  (unpublished tests-jars — relationalStore/service/persistence), then
 *  run reflectively like tier 1. {@code -Dtier2.classes=<dir>} points at
 *  the compiled classes; the shims (parent classloader) win, so every
 *  {@code test(...)} call records instead of asserting. Diagnostic. */
class ZTier2FixtureHarvest {

    @Test
    void harvest() throws Exception {
        String dir = System.getProperty("tier2.classes");
        if (dir == null) {
            System.out.println("@@ no -Dtier2.classes — skipping");
            return;
        }
        System.out.println("@@ tier2 " + FixtureHarvest.tier2(Path.of(dir), getClass().getClassLoader()));
        Path dump = Path.of(System.getProperty("fixture.dump",
                "target/engine-fixtures.jsonl"));
        System.out.println("@@ fixtures dumped: "
                + (Files.exists(dump) ? Files.lines(dump).count() : 0));
    }
}
