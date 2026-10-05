package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * G11 (Bazel workplan P6-11): no runtime classpath holds two versions of one Maven coordinate. Every package's
 * guard_classpaths report (guards_package, classpath.bzl) lists each JVM target's runtime jars as group:artifact:version;
 * a target whose classpath has one group:artifact at two versions fails here, unless ALLOWED names it with why.
 * Pools resolve on their own (MODULE.bazel), so two pools on one classpath could otherwise disagree silently.
 */
class ClasspathTest {

    /** group:artifact -> why two versions on one classpath are known and safe. None today. */
    private static final Map<String, String> ALLOWED = Map.of();

    @Test
    void noClasspathHoldsTwoVersionsOfOneCoordinate() throws IOException {
        String reports = System.getenv("CLASSPATH_REPORTS");
        List<Path> files = new ArrayList<>();
        for (String r : reports.split(" ")) {
            if (!r.isBlank()) files.add(Runfile.of(r));
        }
        Map<String, Set<String>> versions = new TreeMap<>();  // "target  group:artifact" -> versions
        int lines = 0;
        for (Path f : files) {
            for (String line : Files.readAllLines(f)) {
                if (line.isBlank()) continue;
                lines++;
                String[] fields = line.split("\t");
                String coordinate = fields[1];
                int last = coordinate.lastIndexOf(':');
                versions.computeIfAbsent(fields[0] + "  " + coordinate.substring(0, last), k -> new TreeSet<>())
                        .add(coordinate.substring(last + 1) + " (" + fields[2] + ")");
            }
        }
        assertTrue(lines > 1000, "the reports list " + lines + " classpath jars: the guard is not looking");
        List<String> conflicts = new ArrayList<>();
        versions.forEach((key, vs) -> {
            String coordinate = key.substring(key.indexOf("  ") + 2);
            long distinct = vs.stream().map(v -> v.substring(0, v.indexOf(' '))).distinct().count();
            if (distinct > 1 && !ALLOWED.containsKey(coordinate)) conflicts.add(key + ": " + vs);
        });
        assertEquals(List.of(), conflicts, "classpaths holding two versions of one coordinate");
    }
}
