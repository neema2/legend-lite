// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DANGLING-STATE GUARD (Phase 0.4, docs/END_TO_END_PLAN_2026_09_08.md;
 * audit docs/HARNESS_AUDIT_2026_09_07.md §2). Batch 115 deleted the
 * writers of four static slots and left their readers: the ordered
 * row compare silently became a multiset compare, in the direction that
 * manufactures passes, and no guard saw it. The rule this test makes
 * mechanical: for every static mutable slot — {@code ThreadLocal},
 * {@code Atomic*}, {@code LongAdder}, {@code volatile} — across both
 * source roots (core, pct) and both trees (main, test), readers exist
 * IF AND ONLY IF writers exist. A slot with readers and no writer is a
 * dead gate (its readers see the initial value forever); a slot with
 * writers and no reader is a sink nobody watches; a slot with neither is
 * a dead declaration. All three are DANGLING; the register below is the
 * exact set, shrink-only (audit §10 item 13: pinned at zero once Phase
 * 0.5 rewires the ordered compare).
 *
 * <p>Second rule (same item): a guard may not pin a FILE absent from the
 * tree (a pin on a deleted file is a bearer bond — audit §6 found the
 * broad-catch count of the old runner's executor still pinned at 5 after
 * the file was deleted) nor justify a pin by a {@code Class.member} of a
 * class absent from the tree, unless the line says so (a history note:
 * deleted / died / retired), or the pin is a negative one
 * ({@code !Files.exists(...)}: the file must stay gone).
 */
class DanglingStateGuardTest {

    /** Source roots, relative to the core module (the test's cwd): every
     * module of the reactor (core, nlq, pct, parser-equivalence), main and
     * test trees where they exist — a slot's readers may live in another
     * module (batch 123's lesson: pct reads core's censuses). */
    private static final List<Path> ROOTS = Stream.of("core", "spec", "nlq", "pct", "parser-equivalence")
            .flatMap(m -> Stream.of(Path.of("..", m, "src/main/java"), Path.of("..", m, "src/test/java")))
            .filter(Files::isDirectory)
            .toList();

    /** A static field whose type is a mutable slot. Group 1 = the field name. */
    private static final Pattern SLOT_DECL = Pattern.compile(
            "^\\s*(?:public |protected |private )?static\\s+(?:final\\s+)?(?:volatile\\s+)?"
            + "(?:[\\w.]*(?:ThreadLocal|AtomicInteger|AtomicLong|AtomicBoolean|AtomicReference"
            + "|AtomicLongArray|LongAdder)\\b[^=;]*?|volatile\\s+[^=;]*?)\\b([A-Za-z_]\\w*)\\s*[=;]",
            Pattern.MULTILINE);

    /** Methods that WRITE the slot (no value read back). */
    private static final Set<String> WRITERS = Set.of(
            "set", "remove", "increment", "decrement", "add", "reset", "lazySet",
            "compareAndSet", "weakCompareAndSet");
    /** Methods that both write and hand a value back (an id counter). */
    private static final Set<String> READ_WRITERS = Set.of(
            "getAndIncrement", "incrementAndGet", "getAndDecrement", "decrementAndGet",
            "getAndAdd", "addAndGet", "getAndSet", "getAndUpdate", "updateAndGet",
            "accumulateAndGet", "getAndAccumulate", "sumThenReset");

    /** The known-dangling register: EXACT, shrink-only. Readers without a
     * writer since batch 115 (the old runner's per-test order derivation
     * was deleted with it); Phase 0.5 rewires both from
     * {@code AssertVerdicts.orderView}, and this register goes to zero. */
    private static final Set<String> KNOWN_DANGLING = Set.of();
    // (batch 129 registered H2Verify.ORDERED_QUERY / SORT_KEYS — the audit's
    // readers without a writer; batch 130 replaced all three referee
    // thread-locals with the ReplayFacts value on the SPI: ZERO.)

    record Slot(String cls, String name, Path file) {
        String key() {
            return cls + "." + name;
        }
    }

    @Test
    void everyStaticSlotHasReadersIffWriters() throws IOException {
        Map<Path, String> sources = new LinkedHashMap<>();
        for (Path root : ROOTS) {
            try (Stream<Path> files = Files.walk(root)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    sources.put(f, stripCommentsAndStrings(Files.readString(f)));
                }
            }
        }
        assertTrue(ROOTS.size() >= 6, "module roots collapsed: " + ROOTS);
        GuardCoverage.assertFloor("DanglingStateGuardTest", sources.size(), 900);
        List<Slot> slots = new ArrayList<>();
        for (Map.Entry<Path, String> e : sources.entrySet()) {
            Matcher m = SLOT_DECL.matcher(e.getValue());
            while (m.find()) {
                String cls = e.getKey().getFileName().toString().replace(".java", "");
                slots.add(new Slot(cls, m.group(1), e.getKey()));
            }
        }
        assertTrue(slots.size() >= 40, "slot census collapsed: " + slots.size()
                + " static slots found — the declaration regex rotted");
        Map<String, String> dangling = new TreeMap<>();
        for (Slot s : slots) {
            int reads = 0;
            int writes = 0;
            for (Map.Entry<Path, String> e : sources.entrySet()) {
                boolean own = e.getKey().equals(s.file());
                // in the declaring file the bare name; elsewhere Class.NAME
                Pattern use = Pattern.compile(own
                        ? "(?<![\\w.])" + Pattern.quote(s.name()) + "\\b(\\s*\\.\\s*(\\w+)\\s*\\()?(\\s*=(?!=))?"
                        : "\\b" + Pattern.quote(s.cls()) + "\\s*\\.\\s*" + Pattern.quote(s.name())
                                + "\\b(\\s*\\.\\s*(\\w+)\\s*\\()?(\\s*=(?!=))?");
                Matcher m = use.matcher(e.getValue());
                while (m.find()) {
                    if (own && isDeclaration(e.getValue(), m.start())) {
                        continue;
                    }
                    String method = m.group(2);
                    boolean assign = m.group(3) != null;
                    if (assign) {
                        writes++;
                    } else if (method == null) {
                        // passed as a value, a bare field read, or an operand of a
                        // conditional ((fact ? A : B).increment()): used through an
                        // alias — counts on both sides; the batch-115 class (readers
                        // calling get() with no writer anywhere) stays caught
                        reads++;
                        writes++;
                    } else if (WRITERS.contains(method)) {
                        writes++;
                    } else if (READ_WRITERS.contains(method)) {
                        reads++;
                        writes++;
                    } else {
                        reads++;          // get, sum, forEach, ...
                    }
                }
            }
            if (reads == 0 || writes == 0) {
                dangling.put(s.key(), "reads=" + reads + " writes=" + writes);
            }
        }
        assertEquals(new TreeSet<>(KNOWN_DANGLING), dangling.keySet(),
                "static slots with readers but no writer (a dead gate), writers but"
                + " no reader (an unwatched sink) or neither (a dead declaration):\n  "
                + dangling + "\n  a NEW entry is the batch-115 disease — wire the writer"
                + " or delete the slot; a removed entry shrinks KNOWN_DANGLING");
    }

    /** The declaration line itself: {@code static ... NAME =} or {@code static ... NAME;}. */
    private static boolean isDeclaration(String src, int at) {
        int lineStart = src.lastIndexOf('\n', at) + 1;
        return src.substring(lineStart, at).contains("static ");
    }

    // ---- rule 2: guards pin only what exists -------------------------------

    private static final Pattern PINNED_FILE = Pattern.compile("\"([A-Z]\\w*)\\.java\"");
    private static final Pattern CLASS_MEMBER = Pattern.compile(
            "\\b([A-Z][A-Za-z0-9]+)\\.([a-z]\\w*|[A-Z][A-Z0-9_]+)\\b");
    private static final Pattern HISTORY = Pattern.compile(
            "(?i)\\b(deleted|died|retired|removed|gone|old runner)\\b");

    @Test
    void guardsPinOnlyFilesAndSymbolsInTheTree() throws IOException {
        Set<String> classes = new TreeSet<>();
        for (Path root : ROOTS) {
            try (Stream<Path> files = Files.walk(root)) {
                files.filter(p -> p.toString().endsWith(".java"))
                        .forEach(p -> classes.add(p.getFileName().toString().replace(".java", "")));
            }
        }
        // java.*/javax.* and the JDK classes a guard comment may name are
        // not tree classes; only classes spelled like ours count
        List<String> bad = new ArrayList<>();
        List<Path> guards;
        try (Stream<Path> files = Files.list(Path.of("src/test/java/com/legend"))) {
            guards = files.filter(p -> p.getFileName().toString().matches(".*(Test|Coverage)\\.java")).sorted().toList();
        }
        GuardCoverage.assertFloor("DanglingStateGuardTest(guards)", guards.size(), 20);
        for (Path g : guards) {
            String src = Files.readString(g);
            Matcher m = PINNED_FILE.matcher(src);
            while (m.find()) {
                String line = lineText(src, m.start());
                if (line.contains("!Files.exists") || HISTORY.matcher(line).find()) {
                    continue;      // a negative pin, or a history note
                }
                if (!classes.contains(m.group(1))) {
                    bad.add(g.getFileName() + ":" + lineOf(src, m.start()) + " pins \""
                            + m.group(1) + ".java\" — no such file in the tree (a bearer bond)");
                }
            }
            for (String line : src.split("\n")) {
                if (!line.strip().startsWith("//") && !line.strip().startsWith("*")) {
                    continue;      // comments only: code is checked by the compiler
                }
                if (HISTORY.matcher(line).find()) {
                    continue;      // a history note may name what died
                }
                Matcher cm = CLASS_MEMBER.matcher(line);
                while (cm.find()) {
                    String cls = cm.group(1);
                    if (RETIRED_TREE_CLASSES.contains(cls)) {
                        bad.add(g.getFileName() + ": comment cites " + cm.group()
                                + " — " + cls + " is not in the tree; say it died, or cite"
                                + " the live mechanism");
                    }
                }
            }
        }
        assertTrue(bad.isEmpty(), "guards citing what does not exist:\n  " + String.join("\n  ", bad));
    }

    /** Classes the harness rebuild deleted (batch 115) whose names still
     * read like live mechanisms — the exact list the audit found cited. */
    private static final Set<String> RETIRED_TREE_CLASSES = Set.of(
            "EngineTestExecutor", "RelationalCorpusRunner", "Runner", "WholeTestFlip",
            "FlipProbe", "WholeTestCensus", "AssertLedger", "ExecCallFinder",
            "TestDataGenForm", "LineageForm", "ElqSplice", "AssertLoopForm",
            "RuntimeIfForm", "JsonAssertCanon", "PlanAsserts", "SqlTextShapes");

    private static String lineText(String src, int at) {
        int a = src.lastIndexOf('\n', at) + 1;
        int b = src.indexOf('\n', at);
        return src.substring(a, b < 0 ? src.length() : b);
    }

    private static int lineOf(String src, int at) {
        int n = 1;
        for (int i = 0; i < at; i++) {
            if (src.charAt(i) == '\n') {
                n++;
            }
        }
        return n;
    }

    /** Comments and string literals removed (newlines kept so line numbers
     * survive); the slot census must not count a name in prose. */
    static String stripCommentsAndStrings(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                for (int k = i; k < end; k++) {
                    if (src.charAt(k) == '\n') {
                        out.append('\n');
                    }
                }
                i = end;
            } else if (c == '"') {
                out.append('"');
                i++;
                while (i < n && src.charAt(i) != '"') {
                    if (src.charAt(i) == '\\') {
                        i++;
                    }
                    if (i < n && src.charAt(i) == '\n') {
                        out.append('\n');
                    }
                    i++;
                }
                out.append('"');
                i++;
            } else if (c == '\'') {
                out.append('\'');
                i++;
                while (i < n && src.charAt(i) != '\'') {
                    if (src.charAt(i) == '\\') {
                        i++;
                    }
                    i++;
                }
                out.append('\'');
                i++;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }
}
