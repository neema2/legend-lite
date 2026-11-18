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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GUARD L-A, ITS SECOND HALF (the orchestrator's order of 2026-11-18): the product's lowering, resolver and run
 * branching on upstream's shape. Two kinds of site, each a branch the product takes because legend-engine or
 * legend-pure would: a read of the judging rules' upstream tolerances ({@code JudgingRules}' {@code floatUlps},
 * {@code decimalScale}, {@code floatTolerance}), and a literal naming one of upstream's frame or text shapes
 * ({@link #UPSTREAM_SHAPES}: legend-engine's union frame, {@code unionAlias}). The scan reads every main source under
 * {@code lowering/}, {@code resolver/} and {@code testexec/}, comments stripped, and counts each kind per file. Every
 * count is pinned ({@link #SITES}), SHRINK-ONLY: a new site fails (the branch is the edge's: hand it in by a hook), a
 * count that falls fails until re-pinned.
 */
@Tag("guardrail")
class UpstreamBranchesTest {

    private static final String JAVA_ROOT = "core/src/main/java/com/legend/";

    /** The packages scanned: where a branch on upstream's shape would make the product's SQL or verdict upstream's. */
    static final List<String> PACKAGES = List.of("lowering", "resolver", "testexec");

    /** Upstream's frame and text shapes, matched as a whole string literal. */
    static final List<String> UPSTREAM_SHAPES = List.of("unionAlias");

    /** The judging rules' upstream tolerances, matched as an accessor call in code. */
    static final Pattern RULE_READ = Pattern.compile("\\.(floatUlps|decimalScale|floatTolerance)\\(\\)");

    /** A branch on a rule's value, the value handed on under its own name ({@code floatUlps > 0},
     *  {@code if (!decimalScale)}, {@code rules.floatTolerance() ? ...}): the name, then a comparison, a ternary or a
     *  Boolean operator, or the name alone as an if's or a while's condition. */
    static final Pattern RULE_BRANCH = Pattern.compile("\\b(floatUlps|decimalScale|floatTolerance)\\b(?:\\(\\))?"
            + "\\s*(?:[<>]=?|[=!]=|\\?|&&|\\|\\|)|(?:if|while)\\s*\\(\\s*!?\\s*(?:[\\w.]+\\.)?"
            + "(floatUlps|decimalScale|floatTolerance)\\b(?:\\(\\))?\\s*\\)");

    /** "file | kind" -> count, measured 2026-11-18, SHRINK-ONLY (a count lowered as a site goes, dated). Every kind is a
     *  FINDING, the orchestrator's ruling of 2026-11-18, 06:40: legend-engine's union frame name branched on in
     *  lowering (E-SQL's L10), and the judges' branches on the rules' tolerances -- C1 ruled the RULES data; L11 (the
     *  lens audit, E-Java's item 4) moves compiled legend-pure's comparison LOGIC behind two-form kinds, so the
     *  product's judges carry lite's exact rule and no compatibility branch. */
    static final Map<String, Integer> SITES = Map.ofEntries(
            Map.entry("lowering/Fold.java | shape unionAlias", 1),
            Map.entry("lowering/Lowerer.java | shape unionAlias", 7),
            Map.entry("lowering/SubselectPrune.java | shape unionAlias", 1));
    // (the judging rules' sites -- VerdictSql's 8 branches on floatUlps, Equality's 5 and HostRules' 3 reads -- gone
    // 2026-11-18 with E-Java's train-17 item 4, dacc4bf313: the database judge asks the VerdictNumbers two-form kind, the
    // host judge the run hook HostForms, compiled legend-pure's comparison the edge's forms; L-A.judgingRules closes)

    /** A site's census line: upstream's frame name, or a branch on (a read of) the judging rules' tolerances. */
    static String lineOf(String site) {
        return site.contains("| shape ") ? "L-A.unionAlias" : "L-A.judgingRules";
    }

    /** The census lines this guard holds. */
    static Set<String> lines() {
        Set<String> out = new java.util.TreeSet<>();
        SITES.keySet().forEach(k -> out.add(lineOf(k)));
        return out;
    }

    /** The two-form kinds' simple names ({@code PlanStyleFacets.TWO_FORMS}). A call on a receiver a file declares as one
     *  of them is that kind's answer -- the hook this guard asks a branch to come through, its edge form the edge's --
     *  not a read of the judging rules, whatever the method's name: VerdictNumbers#floatTolerance shares
     *  JudgingRules#floatTolerance's (E-Java's train-17 item 4, 2026-11-18; the GUARD stream's re-measure on d-int17). */
    static final Set<String> TWO_FORM_KINDS = twoFormKinds();

    private static Set<String> twoFormKinds() {
        Set<String> out = new java.util.TreeSet<>();
        com.legend.testplan.PlanStyleFacets.TWO_FORMS.forEach(c -> out.add(c.getSimpleName()));
        return out;
    }

    /** The identifiers {@code code} declares with a two-form kind's type (a field, a parameter, a local). */
    static Set<String> hookReceivers(String code) {
        Set<String> out = new java.util.TreeSet<>();
        for (String kind : TWO_FORM_KINDS) {
            Matcher d = Pattern.compile("\\b" + Pattern.quote(kind) + "\\s+(\\w+)\\s*[,;=)]").matcher(code);
            while (d.find()) {
                out.add(d.group(1));
            }
        }
        return out;
    }

    /** Whether the name starting at {@code nameStart} is called on one of {@code receivers} ({@code numbers.x()}). */
    static boolean onHook(String code, int nameStart, Set<String> receivers) {
        int i = nameStart - 1;
        while (i >= 0 && Character.isWhitespace(code.charAt(i))) {
            i--;
        }
        if (i < 0 || code.charAt(i) != '.') {
            return false;
        }
        i--;
        while (i >= 0 && Character.isWhitespace(code.charAt(i))) {
            i--;
        }
        int end = i + 1;
        while (i >= 0 && Character.isJavaIdentifierPart(code.charAt(i))) {
            i--;
        }
        return receivers.contains(code.substring(i + 1, end));
    }

    /** The rule reads and branches of {@code blanked} (comments stripped, literals blanked), "rule read X" or "rule
     *  branch X", a call on a two-form kind's receiver left out. */
    static List<String> ruleSites(String blanked) {
        Set<String> hooks = hookReceivers(blanked);
        List<String> out = new java.util.ArrayList<>();
        Matcher m = RULE_READ.matcher(blanked);
        while (m.find()) {
            if (!onHook(blanked, m.start(1), hooks)) {
                out.add("rule read " + m.group(1));
            }
        }
        Matcher b = RULE_BRANCH.matcher(blanked);
        while (b.find()) {
            int g = b.group(1) != null ? 1 : 2;
            if (!onHook(blanked, b.start(g), hooks)) {
                out.add("rule branch " + b.group(g));
            }
        }
        return out;
    }

    static Map<String, Integer> found(List<String> packages) {
        Map<String, Integer> out = new TreeMap<>();
        for (String pkg : packages) {
            for (Path f : SourceFiles.under(JAVA_ROOT + pkg)) {
                String path = SourceFiles.path(f);
                if (!path.endsWith(".java")) {
                    continue;
                }
                String code;
                try {
                    code = stripComments(Files.readString(f));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                String rel = path.substring(JAVA_ROOT.length());
                for (String site : ruleSites(blankLiterals(code))) {
                    out.merge(rel + " | " + site, 1, Integer::sum);
                }
                for (String shape : UPSTREAM_SHAPES) {
                    Matcher s = Pattern.compile("\"" + Pattern.quote(shape) + "\"").matcher(code);
                    while (s.find()) {
                        out.merge(rel + " | shape " + shape, 1, Integer::sum);
                    }
                }
            }
        }
        return out;
    }

    /** {@code code} with its comments blanked (string literals kept). */
    static String stripComments(String code) {
        StringBuilder out = new StringBuilder(code.length());
        int i = 0;
        while (i < code.length()) {
            char c = code.charAt(i);
            if (c == '"' ) {
                int j = endOfLiteral(code, i);
                out.append(code, i, j);
                i = j;
            } else if (c == '\'') {
                int j = i + 1;
                while (j < code.length() && code.charAt(j) != '\'') {
                    j += code.charAt(j) == '\\' ? 2 : 1;
                }
                out.append(code, i, Math.min(j + 1, code.length()));
                i = j + 1;
            } else if (code.startsWith("//", i)) {
                while (i < code.length() && code.charAt(i) != '\n') {
                    i++;
                }
            } else if (code.startsWith("/*", i)) {
                int j = code.indexOf("*/", i + 2);
                i = j < 0 ? code.length() : j + 2;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** The index past the literal starting at {@code i} (a text block too). */
    static int endOfLiteral(String code, int i) {
        if (code.startsWith("\"\"\"", i)) {
            int j = code.indexOf("\"\"\"", i + 3);
            return j < 0 ? code.length() : j + 3;
        }
        int j = i + 1;
        while (j < code.length() && code.charAt(j) != '"') {
            j += code.charAt(j) == '\\' ? 2 : 1;
        }
        return Math.min(j + 1, code.length());
    }

    /** {@code code} with each string literal's content blanked. */
    static String blankLiterals(String code) {
        StringBuilder out = new StringBuilder(code.length());
        int i = 0;
        while (i < code.length()) {
            if (code.charAt(i) == '"') {
                int j = endOfLiteral(code, i);
                out.append("\"\"");
                i = j;
            } else {
                out.append(code.charAt(i++));
            }
        }
        return out.toString();
    }

    @Test
    void theProductBranchesOnUpstreamOnlyWherePinned() {
        assertEquals(new TreeMap<>(SITES), found(PACKAGES), "a product branch on upstream's shape -- a judging rule's"
                + " upstream tolerance or upstream's frame name -- beyond its pin: hand the branch in from the edge by"
                + " a hook (a site gone: lower its pin, dated)");
    }

    @Test
    void theScanBites() {
        String planted = "class P { int f(Rules r, Sub s) { // .floatUlps() in a comment is no site\n"
                + " if (r.floatUlps() > 0 && \"unionAlias\".equals(s.frameName())) { return 1; } return 0; } }";
        String code = stripComments(planted);
        Matcher m = RULE_READ.matcher(blankLiterals(code));
        int rules = 0;
        while (m.find()) {
            rules++;
        }
        assertEquals(1, rules, "one rule read outside the comment");
        int branches = 0;
        Matcher b = RULE_BRANCH.matcher(blankLiterals(stripComments(planted
                + "\nclass Q { int g(int floatUlps, boolean decimalScale) { if (!decimalScale) { return 0; }"
                + " return floatUlps > 0 ? 1 : 2; } }")));
        while (b.find()) {
            branches++;
        }
        assertEquals(3, branches, "the planted branches: r.floatUlps() > 0, if (!decimalScale), floatUlps > 0");
        assertEquals(true, code.contains("\"unionAlias\""), "the shape literal survives the comment strip");
        // a two-form kind's answer of the same name is the hook's, not the rules': left out; the rules' read beside it,
        // and the same name on another receiver, still found (the GUARD stream, 2026-11-18, train 17)
        String hooked = blankLiterals(stripComments("class H { boolean f(VerdictNumbers numbers, JudgingRules rules,"
                + " Other other) { if (numbers.floatTolerance()) { return true; }"
                + " return rules.floatTolerance() && other.floatUlps() > 0 && numbers.floatTolerance() ? true : false; } }"));
        assertEquals(Set.of("numbers"), hookReceivers(hooked), "a parameter typed by a two-form kind is a hook receiver");
        assertEquals(List.of("rule read floatTolerance", "rule read floatUlps", "rule branch floatTolerance",
                "rule branch floatUlps"), ruleSites(hooked), "the hook's two answers left out; the rules' read and the"
                + " other receiver's read and branch found");
    }
}
