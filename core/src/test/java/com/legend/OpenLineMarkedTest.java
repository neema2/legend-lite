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
import java.util.LinkedHashMap;
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
 * here, comments aside) and the list are held EQUAL, line for line: a renamed class stays marked, so the list must
 * follow it to its new name; a mark removed is a visible act with its ruling; a listed class with no source in core is
 * refused by name (this scan reads core's sources alone -- a class moved to a host's tree must take the scan with it); a
 * mark in the edge, on a nested type, naming no line, written bare without its import, or not readable, is refused.
 */
@Tag("guardrail")
class OpenLineMarkedTest {

    private static final String JAVA_ROOT = "core/src/main/java/";

    private static final Pattern PACKAGE = Pattern.compile("(?m)^\\s*package\\s+([\\w.]+)\\s*;");

    /** Any mark, however spaced: {@code @}, the optional qualifier, the name -- in code, comments and strings aside. */
    private static final Pattern ANY_MARK = Pattern.compile(
            "@\\s*(com\\s*\\.\\s*legend\\s*\\.\\s*base\\s*\\.\\s*)?OpenLine\\b");

    /** A readable mark: its line (as the value or {@code value = }), then the type it annotates. */
    private static final Pattern MARK = Pattern.compile(
            "@\\s*(com\\s*\\.\\s*legend\\s*\\.\\s*base\\s*\\.\\s*)?OpenLine\\s*\\(\\s*(?:value\\s*=\\s*)?\"([^\"]*)\"\\s*\\)"
            + "\\s*(?:@[\\w.]+(?:\\s*\\([^)]*\\))?\\s*)*(?:(?:public|protected|private|static|final|abstract|sealed"
            + "|non-sealed|strictfp)\\s+)*(?:class|interface|enum|record|@\\s*interface)\\s+(\\w+)");

    /**
     * The source with comments removed, every string, text block and char literal kept as it stands, and -- in
     * {@code blanked} -- each literal's content replaced by spaces of the same length (so a mark inside a string is no
     * mark, and positions agree between the two).
     */
    static String[] code(String s) {
        StringBuilder kept = new StringBuilder(s.length());
        StringBuilder blanked = new StringBuilder(s.length());
        int i = 0;
        int n = s.length();
        while (i < n) {
            if (s.startsWith("/*", i)) {
                int end = s.indexOf("*/", i + 2);
                int stop = end < 0 ? n : end + 2;
                pad(kept, blanked, s.substring(i, stop), true);
                i = stop;
            } else if (s.startsWith("//", i)) {
                int end = s.indexOf('\n', i);
                int stop = end < 0 ? n : end;
                pad(kept, blanked, s.substring(i, stop), true);
                i = stop;
            } else if (s.startsWith("\"\"\"", i)) {
                int end = s.indexOf("\"\"\"", i + 3);
                while (end > 0 && s.charAt(end - 1) == '\\') {
                    end = s.indexOf("\"\"\"", end + 1);
                }
                int stop = end < 0 ? n : end + 3;
                kept.append(s, i, stop);
                pad(null, blanked, s.substring(i, stop), false);
                i = stop;
            } else if (s.charAt(i) == '"' || s.charAt(i) == '\'') {
                char q = s.charAt(i);
                int j = i + 1;
                while (j < n && s.charAt(j) != q && s.charAt(j) != '\n') {
                    j += s.charAt(j) == '\\' ? 2 : 1;
                }
                int stop = Math.min(j + 1, n);
                kept.append(s, i, stop);
                pad(null, blanked, s.substring(i, stop), false);
                i = stop;
            } else {
                kept.append(s.charAt(i));
                blanked.append(s.charAt(i));
                i++;
            }
        }
        return new String[] {kept.toString(), blanked.toString()};
    }

    /** Spaces for {@code text} (its line breaks kept) into {@code blanked}, and into {@code kept} when {@code both}. */
    private static void pad(StringBuilder kept, StringBuilder blanked, String text, boolean both) {
        for (char c : text.toCharArray()) {
            char p = c == '\n' || c == '\r' ? c : ' ';
            blanked.append(p);
            if (both && kept != null) {
                kept.append(p);
            }
        }
    }

    /** The brace depth at {@code at} in the blanked code. */
    private static int depth(String blanked, int at) {
        int d = 0;
        for (int i = 0; i < at; i++) {
            char c = blanked.charAt(i);
            d += c == '{' ? 1 : c == '}' ? -1 : 0;
        }
        return d;
    }

    /** The marks of one source: its type's fully qualified name -> its line; each mark out of place, or unreadable, is
     *  reported in {@code wrong}. */
    static Map<String, String> marks(String path, String source, List<String> wrong) {
        Map<String, String> out = new TreeMap<>();
        if (!source.contains("OpenLine")) {
            return out;
        }
        String[] c = code(source);
        String kept = c[0];
        String blanked = c[1];
        Matcher pkg = PACKAGE.matcher(blanked);
        String prefix = pkg.find() ? pkg.group(1) + "." : "";
        boolean imported = Pattern.compile("(?m)^\\s*import\\s+com\\s*\\.\\s*legend\\s*\\.\\s*base\\s*\\.\\s*OpenLine\\s*;")
                .matcher(blanked).find();
        Matcher any = ANY_MARK.matcher(blanked);
        while (any.find()) {
            Matcher m = MARK.matcher(kept);
            if (!m.find(any.start()) || m.start() != any.start()) {
                wrong.add(path + ": a mark this test cannot read (it annotates no type declaration it recognizes)");
                continue;
            }
            String fqn = prefix + m.group(3);
            if (m.group(1) == null && !imported) {
                wrong.add(path + ": the mark on " + m.group(3) + " is written bare without importing"
                        + " com.legend.base.OpenLine");
            } else if (depth(blanked, any.start()) != 0) {
                wrong.add(path + ": the mark on " + m.group(3) + " is not on a top-level type");
            } else if (!m.group(2).matches("[A-Z][0-9]+")) {
                wrong.add(path + ": the mark on " + m.group(3) + " names no census line: '" + m.group(2) + "'");
            } else if (fqn.startsWith("com.legend.edge.")) {
                wrong.add(path + ": the edge's class " + fqn + " is marked open; the edge is where an open line closes");
            } else {
                out.put(fqn, m.group(2));
            }
        }
        return out;
    }

    /** What differs between the {@code marked} classes and the {@code listed} ones, each class's source known by
     *  {@code sourced} (a listed class whose source this scan does not read is named as such, never "closed"). */
    static List<String> differences(Map<String, String> marked, Map<String, String> listed,
            java.util.function.Predicate<String> sourced) {
        List<String> out = new ArrayList<>();
        java.util.Set<String> all = new java.util.TreeSet<>(marked.keySet());
        all.addAll(listed.keySet());
        for (String cls : all) {
            String m = marked.get(cls);
            String l = listed.get(cls);
            if (l == null) {
                out.add(cls + ": marked " + m + " but not in COMPAT_CLASSES (a renamed class: its entry follows it)");
            } else if (m == null && !sourced.test(cls)) {
                out.add(cls + ": listed " + l + " but has no source in core/src/main -- moved? this scan reads core's"
                        + " sources alone; take the scan to its new tree, never delete the entry for it");
            } else if (m == null) {
                out.add(cls + ": listed " + l + " but not marked (its line closes only by a ruling: the class moved to"
                        + " the edge or deleted, and its entry deleted with it)");
            } else if (!m.equals(l)) {
                out.add(cls + ": marked " + m + " but listed " + l);
            }
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
        java.util.Set<String> sourced = new java.util.HashSet<>();
        List<String> wrong = new ArrayList<>();
        for (Path p : SourceFiles.under("core/src/main")) {
            String path = SourceFiles.path(p);
            if (path.endsWith(".java")) {
                if (path.startsWith(JAVA_ROOT)) {
                    sourced.add(path.substring(JAVA_ROOT.length(), path.length() - ".java".length()).replace('/', '.'));
                }
                marked.putAll(marks(path, read(p), wrong));
            }
        }
        assertEquals(List.of(), wrong, "an @OpenLine mark out of place");
        Map<String, String> listed = new TreeMap<>();
        EdgeSeparationTest.COMPAT_CLASSES.forEach((c, line) -> listed.put(c, line.split(" ")[0]));
        assertEquals(List.of(), differences(marked, listed, sourced::contains), "the census's open classes"
                + " (EdgeSeparationTest.COMPAT_CLASSES) and the classes marked @OpenLine differ");
    }

    /** The parser: the declaration-line form the product writes, a spaced and a broken mark, {@code value =}, a mark in a
     *  comment or a string (no mark), and each refusal. */
    @Test
    void theMarkIsReadAsJavacReadsIt() {
        List<String> wrong = new ArrayList<>();
        Map<String, String> read = new LinkedHashMap<>();
        read.putAll(marks("lowering/Representation.java", """
                package com.legend.lowering;

                /** renamed from Repr; {@code @com.legend.base.OpenLine("C9")} in a comment is no mark */
                @com.legend.base.OpenLine("C5") final class Representation {
                    static final String NOT = "@OpenLine(\\"C9\\") class Fake {}";
                }
                """, wrong));
        read.putAll(marks("lowering/Spaced.java", """
                package com.legend.lowering;

                @Deprecated @ com.legend.base. OpenLine
                    ( value = "B9" )
                public interface Spaced {
                }
                """, wrong));
        read.putAll(marks("lowering/Imported.java", """
                package com.legend.lowering;

                import com.legend.base.OpenLine;

                @OpenLine ("B2") public final class Imported {
                }
                """, wrong));
        assertEquals(Map.of("com.legend.lowering.Representation", "C5", "com.legend.lowering.Spaced", "B9",
                "com.legend.lowering.Imported", "B2"), read, "each written form is read; a comment's and a string's are not");
        assertEquals(List.of(), wrong);
        marks("lowering/Nested.java", """
                package com.legend.lowering;

                final class Outer {
                    @com.legend.base.OpenLine("C5")
                    static final class Inner {
                    }
                }
                """, wrong);
        marks("lowering/Lineless.java", """
                package com.legend.lowering;

                @com.legend.base.OpenLine("someday")
                public interface Lineless {
                }
                """, wrong);
        marks("edge/Moved.java", """
                package com.legend.edge;

                @com.legend.base.OpenLine("C5")
                public final class Moved {
                }
                """, wrong);
        marks("lowering/Bare.java", """
                package com.legend.lowering;

                @OpenLine("C5") final class Bare {
                }
                """, wrong);
        marks("lowering/Odd.java", """
                package com.legend.lowering;

                @com.legend.base.OpenLine(LINE)
                final class Odd {
                }
                """, wrong);
        assertEquals(List.of("lowering/Nested.java: the mark on Inner is not on a top-level type",
                "lowering/Lineless.java: the mark on Lineless names no census line: 'someday'",
                "edge/Moved.java: the edge's class com.legend.edge.Moved is marked open; the edge is where an open line"
                        + " closes",
                "lowering/Bare.java: the mark on Bare is written bare without importing com.legend.base.OpenLine",
                "lowering/Odd.java: a mark this test cannot read (it annotates no type declaration it recognizes)"),
                wrong);
    }

    /** The guard: a class renamed with its mark (listed under its old name) fails on both names; a listed class with no
     *  source in core is named as such, not closed; a mark with another line is found. */
    @Test
    void theEqualityBites() {
        List<String> found = differences(Map.of("com.legend.lowering.Representation", "C5", "com.legend.planner.X", "B2"),
                Map.of("com.legend.lowering.Repr", "C5", "com.legend.planner.X", "B3", "com.legend.planner.Moved", "B2"),
                cls -> !cls.endsWith(".Moved") && !cls.endsWith(".Repr"));
        assertEquals(List.of(
                "com.legend.lowering.Repr: listed C5 but has no source in core/src/main -- moved? this scan reads core's"
                        + " sources alone; take the scan to its new tree, never delete the entry for it",
                "com.legend.lowering.Representation: marked C5 but not in COMPAT_CLASSES (a renamed class: its entry"
                        + " follows it)",
                "com.legend.planner.Moved: listed B2 but has no source in core/src/main -- moved? this scan reads core's"
                        + " sources alone; take the scan to its new tree, never delete the entry for it",
                "com.legend.planner.X: marked B2 but listed B3"), found);
    }
}
