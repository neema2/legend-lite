// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * AN OPEN LINE IS MARKED ON ITS CLASS, NOT ONLY NAMED IN A LIST (the GUARD stream, step 2's C.7; the edge audit of
 * 2026-11-17, section C.7: "open-line classes marked by an annotation, not by name, so a rename cannot close them").
 * {@link EdgeSeparationTest#COMPAT_CLASSES} names each compatibility class standing in a product package; the census
 * counts it and every product class depending on it BY THAT NAME. Renamed, the class fell out of the vocabulary and its
 * dependents' entries read "gone" -- closed by a rename, nothing moved. Now each such class carries
 * {@code @com.legend.base.OpenLine("<line>")} in its source, and the marked classes (every main source of core, read
 * here) and the list are held EQUAL, line for line: a renamed class stays marked, so the list must follow it to its new
 * name; a mark removed is a visible act with its ruling; a mark in the edge, or on a nested type, is refused.
 */
@Tag("guardrail")
class OpenLineMarkedTest {

    private static final String JAVA_ROOT = "core/src/main/java/";

    private static final Pattern PACKAGE = Pattern.compile("(?m)^package\\s+([\\w.]+)\\s*;");

    /** A mark and the type it annotates: the line, the mark's indentation, and the type's simple name. */
    private static final Pattern MARK = Pattern.compile("(?m)^([ \\t]*)@(?:com\\.legend\\.base\\.)?OpenLine\\(\"([^\"]*)\"\\)"
            + "\\s*(?:@[\\w.]+(?:\\([^)]*\\))?\\s*)*(?:(?:public|protected|private|static|final|abstract|sealed|non-sealed"
            + "|strictfp)\\s+)*(?:class|interface|enum|record|@interface)\\s+(\\w+)");

    /** The marks of one source: its type's fully qualified name -> its line; a mark that is not a top-level type's, or
     *  that names no census line, is reported in {@code wrong}. */
    static Map<String, String> marks(String path, String source, List<String> wrong) {
        Map<String, String> out = new TreeMap<>();
        Matcher pkg = PACKAGE.matcher(source);
        String prefix = pkg.find() ? pkg.group(1) + "." : "";
        Matcher m = MARK.matcher(source);
        while (m.find()) {
            String fqn = prefix + m.group(3);
            if (!m.group(1).isEmpty()) {
                wrong.add(path + ": the mark on " + m.group(3) + " is not on a top-level type");
            } else if (!m.group(2).matches("[A-Z][0-9]+")) {
                wrong.add(path + ": the mark on " + m.group(3) + " names no census line: '" + m.group(2) + "'");
            } else if (fqn.startsWith("com.legend.edge.")) {
                wrong.add(path + ": the edge's class " + fqn + " is marked open; the edge is where an open line closes");
            } else {
                out.put(fqn, m.group(2));
            }
        }
        long declared = Pattern.compile("@(?:com\\.legend\\.base\\.)?OpenLine\\(").matcher(source).results().count();
        if (declared != out.size() + wrong.stream().filter(w -> w.startsWith(path + ":")).count()) {
            wrong.add(path + ": a mark this test cannot read (it annotates no type declaration it recognizes)");
        }
        return out;
    }

    private static String read(Path p) {
        try {
            return Files.readString(p, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void theMarkedClassesAreTheCensusOpenClasses() {
        Map<String, String> marked = new TreeMap<>();
        List<String> wrong = new ArrayList<>();
        for (Path p : SourceFiles.under(JAVA_ROOT)) {
            String path = SourceFiles.path(p);
            if (path.endsWith(".java")) {
                String source = read(p);
                if (source.contains("OpenLine(")) {
                    marked.putAll(marks(path, source, wrong));
                }
            }
        }
        assertEquals(List.of(), wrong, "an @OpenLine mark out of place");
        Map<String, String> listed = new TreeMap<>();
        EdgeSeparationTest.COMPAT_CLASSES.forEach((c, line) -> listed.put(c, line.split(" ")[0]));
        assertEquals(listed, marked, "the census's open classes (EdgeSeparationTest.COMPAT_CLASSES) and the classes"
                + " marked @OpenLine differ: a class renamed keeps its mark, so its entry follows it; a class whose line"
                + " closed loses both, with its ruling");
    }

    /** A renamed class keeps its mark under its new name; a nested mark, a lineless mark and an edge mark are refused;
     *  an unreadable mark is reported. */
    @Test
    void theMarkBites() {
        List<String> wrong = new ArrayList<>();
        Map<String, String> renamed = marks("lowering/Representation.java", """
                package com.legend.lowering;

                /** renamed from Repr */
                @com.legend.base.OpenLine("C5")
                final class Representation {
                }
                """, wrong);
        assertEquals(Map.of("com.legend.lowering.Representation", "C5"), renamed, "the mark travels with the class");
        assertEquals(List.of(), wrong);
        marks("lowering/Nested.java", """
                package com.legend.lowering;

                final class Outer {
                    @OpenLine("C5")
                    static final class Inner {
                    }
                }
                """, wrong);
        marks("lowering/Lineless.java", """
                package com.legend.lowering;

                @OpenLine("someday")
                public interface Lineless {
                }
                """, wrong);
        marks("edge/Moved.java", """
                package com.legend.edge;

                @com.legend.base.OpenLine("C5")
                public final class Moved {
                }
                """, wrong);
        marks("lowering/Odd.java", """
                package com.legend.lowering;

                @com.legend.base.OpenLine("C5")
                // a comment between the mark and its type
                final class Odd {
                }
                """, wrong);
        assertEquals(List.of("lowering/Nested.java: the mark on Inner is not on a top-level type",
                "lowering/Lineless.java: the mark on Lineless names no census line: 'someday'",
                "edge/Moved.java: the edge's class com.legend.edge.Moved is marked open; the edge is where an open line"
                        + " closes",
                "lowering/Odd.java: a mark this test cannot read (it annotates no type declaration it recognizes)"),
                wrong);
    }
}
