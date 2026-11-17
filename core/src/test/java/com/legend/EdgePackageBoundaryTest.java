// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE EDGE'S PACKAGE IS A BOUNDARY, NOT A PREFIX (the GUARD stream, step 2's C.14; the edge audit of 2026-11-17, section
 * C.14: "the product jars hold no com.legend.edge class; the package prefix check has its dot"). Every guard sorts a
 * class into the edge by its package; a check written {@code startsWith("com.legend.edge")} also admits a product
 * package named {@code com.legend.edgecase} -- the edge's exemptions leaking to it. And no guard held the jars
 * themselves: an edge class compiled into a product library (or a product class into the edge's) passed every
 * dependency rule.
 * <ul>
 *   <li>no source under core/src (main, test, the drivers' and the scale trees) checks a {@code com.legend} package,
 *   the root included, by a prefix without its separator
 *   ({@code startsWith("com.legend.x")}, {@code "com/legend/x"}): a package and the ones under it are
 *   {@code equals(p) || startsWith(p + ".")};</li>
 *   <li>the product's and the hosts' jars hold no class under {@code com/legend/edge/};</li>
 *   <li>the edge's jars -- the edge library's and the ones the hosts' list carries -- hold no class outside it (every
 *   edge target's jar: C.11's one source).</li>
 * </ul>
 */
@Tag("guardrail")
class EdgePackageBoundaryTest {

    /** A package-prefix check with no separator: {@code startsWith("com.legend")}, {@code startsWith("com.legend.x")}
     *  or {@code "com/legend/x"}, every segment a package's (lower case; a class-name prefix such as
     *  {@code "com.legend.edge.PureText"} is another check). Its limits, none in core today (grepped 2026-11-17): a
     *  constant argument ({@code startsWith(EDGE_PKG)}), the two-argument {@code startsWith(s, 0)}, an ArchUnit pattern
     *  ({@code "com.legend.edge*.."}), {@code matches}, {@code regionMatches} and {@code indexOf}. */
    private static final Pattern DOTLESS = Pattern.compile(
            "startsWith\\(\\s*\"(com[./]legend(?:[./][a-z_][a-z0-9_]*)*)\"\\s*\\)");

    /** The dotless prefix checks in {@code source} (comments aside, strings kept), "path: the literal". */
    static List<String> dotless(String path, String source) {
        List<String> out = new ArrayList<>();
        Matcher m = DOTLESS.matcher(OpenLineMarkedTest.code(source)[0]);
        while (m.find()) {
            out.add(path + ": startsWith(\"" + m.group(1) + "\") -- a prefix with no separator");
        }
        return out;
    }

    private static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void noPackagePrefixCheckLacksItsSeparator() {
        List<String> found = new ArrayList<>();
        // every tree of core/src (main, test, the drivers' and the scale trees: the review of C.14, 2026-11-17)
        for (Path p : SourceFiles.under("core/src")) {
            String path = SourceFiles.path(p);
            if (path.endsWith(".java") && !path.endsWith("/EdgePackageBoundaryTest.java")) {
                found.addAll(dotless(path, read(p)));
            }
        }
        assertEquals(List.of(), found, "a package checked by a prefix with no separator admits its look-alikes: write"
                + " equals(p) || startsWith(p + \".\")");
    }

    /** The class entries of {@code jars} ("a/b/C.class" paths). */
    private static Set<String> classEntries(List<Path> jars) {
        Set<String> out = new TreeSet<>();
        for (Path jar : jars) {
            try (java.util.jar.JarFile f = new java.util.jar.JarFile(jar.toFile())) {
                f.stream().map(java.util.jar.JarEntry::getName).filter(n -> n.endsWith(".class")
                        && !n.endsWith("module-info.class")).forEach(n -> out.add(jar.getFileName() + "!/" + n));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
    }

    private static boolean edgeEntry(String entry) {
        String n = entry.substring(entry.indexOf("!/") + 2).replaceFirst("^META-INF/versions/\\d+/", "");
        return n.startsWith("com/legend/edge/");
    }

    /** The edge's jars the hosts' list carries, set aside from the product's scan: the edge library and the edge's
     *  //wasm entries (its upstream boundary routes and the tab's module). */
    static final Set<String> EDGE_JARS_IN_THE_HOSTS_LIST = Set.of("libedge.jar", "libedge_boundary.jar",
            "libedge_tab.jar");

    @Test
    void theProductsJarsHoldNoEdgeClass() {
        // the hosts' list carries the edge's jars on purpose (the planner-entry rule reads their classes): each edge
        // target is named edge*, its jar libedge*.jar, and the set set aside is pinned, so no jar leaves this scan
        // unseen (C.11's one source for the edge's targets replaces the name)
        List<Path> jars = new ArrayList<>();
        Set<String> setAside = new TreeSet<>();
        for (Path jar : ConstantPools.jars("legend.product.jars", "legend.authored.jars")) {
            if (jar.getFileName().toString().startsWith("libedge")) {
                setAside.add(jar.getFileName().toString());
            } else {
                jars.add(jar);
            }
        }
        assertEquals(EDGE_JARS_IN_THE_HOSTS_LIST, setAside, "the edge's jars the hosts' list carries");
        Set<String> product = classEntries(jars);
        assertTrue(product.size() > 1000, "the product's classes are read: " + product.size());
        List<String> edge = product.stream().filter(EdgePackageBoundaryTest::edgeEntry).toList();
        assertEquals(List.of(), edge, "an edge class compiled into a product or host jar");
    }

    @Test
    void theEdgesJarHoldsOnlyEdgeClasses() {
        // the edge library's jar and the edge's jars the hosts' list carries (set aside from the product's scan above,
        // so read here: the review of C.14, 2026-11-17); every edge target's jar is C.11's one source
        List<Path> jars = new ArrayList<>(ConstantPools.jars("legend.edge.jars"));
        for (Path jar : ConstantPools.jars("legend.product.jars", "legend.authored.jars")) {
            if (EDGE_JARS_IN_THE_HOSTS_LIST.contains(jar.getFileName().toString())) {
                jars.add(jar);
            }
        }
        Set<String> edge = classEntries(jars);
        assertTrue(edge.size() > 10, "the edge's classes are read: " + edge.size());
        List<String> stray = edge.stream().filter(e -> !edgeEntry(e)).toList();
        assertEquals(List.of(), stray, "a product class compiled into the edge's jar");
    }

    /** Planted: a dotless check is found, a dotted one and the equals-or-prefix form are not; an edge entry is told from a
     *  look-alike. */
    @Test
    void theGuardBites() {
        String q = "\"";
        String src = "class X {\n"
                + "  // p.startsWith(" + q + "com.legend.edge" + q + ") in a comment is no check\n"
                + "  boolean a(String p) { return p.startsWith(" + q + "com.legend.edge" + q + "); }\n"
                + "  boolean b(String p) { return p.startsWith(" + q + "com.legend.edge." + q + "); }\n"
                + "  boolean c(String p) { return p.equals(" + q + "com.legend.edge" + q + ") || p.startsWith("
                + q + "com.legend.edge." + q + "); }\n"
                + "  boolean d(String p) { return p.startsWith(" + q + "com/legend/edge" + q + "); }\n}\n";
        assertEquals(List.of("X.java: startsWith(\"com.legend.edge\") -- a prefix with no separator",
                "X.java: startsWith(\"com/legend/edge\") -- a prefix with no separator"), dotless("X.java", src));
        assertEquals(List.of("Z.java: startsWith(\"com.legend\") -- a prefix with no separator"), dotless("Z.java",
                "class Z { boolean k(String n) { return n.startsWith(\"com.legend\"); } }"), "the root package too");
        assertTrue(edgeEntry("libedge.jar!/com/legend/edge/PureText.class"));
        assertTrue(!edgeEntry("libx.jar!/com/legend/edgecase/Y.class"));
        assertEquals(List.of(), dotless("Y.java", "class Y { boolean k(String n) { return n.startsWith(\"com.legend"
                + ".edge.PureText\"); } }"), "a class-name prefix is another check");
    }
}
