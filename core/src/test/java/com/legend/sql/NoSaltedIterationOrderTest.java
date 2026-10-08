package com.legend.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.SourceFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * NO JVM-SALTED ITERATION ORDER where it can reach SQL text or an id (the compiler plan's W1.5, determinism). Java's
 * immutable collections ({@code Map.copyOf}, {@code Set.copyOf}, {@code Collectors.toUnmodifiableMap/Set}, {@code Map.of},
 * {@code Set.of} with several entries) iterate in an order salted per JVM. A typed node, an SQL node, the lowering and the resolver hold collections whose
 * order is printed (a record's {@code toString} feeds {@code FunctionBodyRows.scopeId}) or rendered (an EXCEPT list), so
 * they keep insertion order: {@code Collections.unmodifiableMap(new LinkedHashMap<>(…))} and the set alike; so does
 * every other product record, since a model or protocol record's print can reach parity and ids too. Found
 * 2026-10-08: the resolver's lambda scope ids differed between two runs on unchanged code (the render census), from
 * {@code TypedTableReference.storedTypes}.
 */
@Tag("guardrail")
class NoSaltedIterationOrderTest {

    private static final Pattern SALTED = Pattern.compile("\\b(Map|Set)\\.copyOf\\(|toUnmodifiable(Set|Map)\\(");
    /** The whole product tree: a typed node, a model record or a protocol record may print or hash any of its
     *  collections, and a lookup table loses nothing by keeping insertion order. */
    private static final String[] PACKAGES = {""};

    @Test
    void noSaltedCopiesWhereOrderReachesTextOrIds() throws IOException {
        String root = "core/src/main/java/com/legend";
        assertTrue(!SourceFiles.under(root).isEmpty(), "core's sources are not declared: " + root);
        List<String> sites = new ArrayList<>();
        for (String pkg : PACKAGES) {
            List<Path> under = SourceFiles.under(pkg.isEmpty() ? root : root + "/" + pkg);
            assertTrue(!under.isEmpty(), "no sources under " + root + "/" + pkg + ": the tree moved; move this rule with it");
            try (Stream<Path> files = under.stream()) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    int line = 0;
                    for (String text : Files.readAllLines(f)) {
                        line++;
                        String code = text.strip();
                        if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                            continue;
                        }
                        if (SALTED.matcher(text).find()) {
                            sites.add(f + ":" + line);
                        }
                    }
                }
            }
        }
        assertEquals(List.of(), sites, "Map.copyOf / Set.copyOf / toUnmodifiableMap / toUnmodifiableSet iterate in a"
                + " JVM-salted order; where the order can reach SQL text or an id, build a LinkedHashMap / LinkedHashSet"
                + " (unmodifiable) in declaration order instead; Map.of / Set.of with several entries are a lookup table only");
    }
}
