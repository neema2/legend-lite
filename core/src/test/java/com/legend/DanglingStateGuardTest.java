// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

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
 * <p>Its second rule, a guard over the other guards' comments (a pinned file or
 * {@code Class.member} must exist in the tree), was deleted on 2026-09-29 with
 * execution plan W0.5: it checked the guards' prose, not the product.
 */
@Tag("census")
class DanglingStateGuardTest {

    /** Source roots, by repository path: every module of the reactor (core, spec, pct, parser-equivalence), main
     * and test trees where the test target declares files in them — a slot's readers may live in another module
     * (batch 123's lesson: pct reads core's censuses). */
    private static final List<String> ROOTS = Stream.of("core", "spec", "pct", "parser-equivalence")
            .flatMap(m -> Stream.of(m + "/src/main/java", m + "/src/test/java"))
            .filter(root -> !SourceFiles.under(root).isEmpty())
            .toList();

    private static final Pattern WORD = Pattern.compile("[A-Za-z_]\\w*");

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
        for (String root : ROOTS) {
            try (Stream<Path> files = SourceFiles.under(root).stream()) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    sources.put(f, stripCommentsAndStrings(Files.readString(f)));
                }
            }
        }
        // 6 -> 5 (2026-09-22): the nlq module was DELETED (owner decision; its
        // natural-language layer calls an external LLM and had no place in the
        // clean-room compiler), taking its main and test roots. The five left:
        // core main + test, and the test trees of spec, pct and
        // parser-equivalence, none of which has a src/main/java.
        assertTrue(ROOTS.size() >= 5, "module roots collapsed: " + ROOTS);
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
        // every file a word occurs in, from ONE pass over the sources: a
        // slot's uses are searched only where its name occurs at all
        Map<String, Set<Path>> filesByWord = new java.util.HashMap<>();
        for (Map.Entry<Path, String> e : sources.entrySet()) {
            Matcher w = WORD.matcher(e.getValue());
            while (w.find()) {
                filesByWord.computeIfAbsent(w.group(), k -> new java.util.LinkedHashSet<>())
                        .add(e.getKey());
            }
        }
        Map<String, String> dangling = new TreeMap<>();
        for (Slot s : slots) {
            int reads = 0;
            int writes = 0;
            // in the declaring file the bare name; elsewhere Class.NAME
            Pattern ownUse = Pattern.compile("(?<![\\w.])" + Pattern.quote(s.name())
                    + "\\b(\\s*\\.\\s*(\\w+)\\s*\\()?(\\s*=(?!=))?");
            Pattern foreignUse = Pattern.compile("\\b" + Pattern.quote(s.cls()) + "\\s*\\.\\s*"
                    + Pattern.quote(s.name()) + "\\b(\\s*\\.\\s*(\\w+)\\s*\\()?(\\s*=(?!=))?");
            for (Path file : filesByWord.getOrDefault(s.name(), Set.of())) {
                boolean own = file.equals(s.file());
                String src = sources.get(file);
                Matcher m = (own ? ownUse : foreignUse).matcher(src);
                while (m.find()) {
                    if (own && isDeclaration(src, m.start())) {
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
