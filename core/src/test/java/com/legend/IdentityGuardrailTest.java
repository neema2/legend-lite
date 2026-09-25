// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * STOP THE BLEEDING (platform architecture untangle, step 0, 2026-09-24).
 *
 * <p>A compiler identifies a function by the DECLARATION its resolver chose and
 * dispatches on that; it never re-identifies it from a name string, and it never
 * asks what CATEGORY of function it is (a PCT function, an assert, a
 * platform-owned one) — such a question is always compensation for a gap in the
 * generic path. This census counts every place core's main code still does
 * either, per shape, and pins each count SHRINK-ONLY: nothing new may be added,
 * and the untangle drives each to zero by replacing the shape, never by
 * rewording it. The sites are written to {@code identity-sites.tsv} (category,
 * file:line, code) — the work list.
 *
 * <p>The scan reads each file with its comments removed (string literals kept),
 * so a shape split across lines is still one site and a comment is never one.
 * {@link PlatformNamesGuardrailTest} already pins {@code equals("meta::…")} and
 * the function-FQN literals outside the catalogs; this census counts the
 * OPERATIONS on names beside them.
 */
@Tag("guardrail")
class IdentityGuardrailTest {

    private static final Path MAIN = Repo.module("src/main/java/com/legend");

    /** Shape → pattern. Each is a way of identifying a function or type by its
     * NAME TEXT, or of asking a function's category. */
    private static final Map<String, Pattern> SHAPES = new LinkedHashMap<>();

    static {
        // a name accessor compared as text: x.qualifiedName().equals(…),
        // af.function().startsWith(…), t.fqn().endsWith(…)
        SHAPES.put("NAME_COMPARE", Pattern.compile(
                "(qualifiedName|function|fqn|fullPath|rawFqn)\\(\\)\\s*\\.\\s*"
                        + "(equals|startsWith|endsWith|contains|equalsIgnoreCase)\\("));
        // the same comparison written the other way round: K.equals(x.qualifiedName())
        SHAPES.put("NAME_COMPARE_REVERSED", Pattern.compile(
                "\\.equals\\(\\s*[A-Za-z_][A-Za-z0-9_.]*\\.(qualifiedName|function|fqn|fullPath|rawFqn)\\(\\)\\s*\\)"));
        // a literal name as the receiver: "meta::…".equals(x)
        SHAPES.put("LITERAL_NAME_COMPARE", Pattern.compile(
                "\"meta::[^\"]*\"\\s*\\.\\s*(equals|startsWith|endsWith|contains)\\("));
        // a package prefix or name suffix tested as text: .startsWith("meta::…"), .endsWith("::x")
        SHAPES.put("NAME_AFFIX_TEST", Pattern.compile(
                "\\.(startsWith|endsWith|contains)\\(\\s*\"(meta::|::)"));
        // a name cut apart to recover a package or simple name
        SHAPES.put("NAME_CUTTING", Pattern.compile(
                "(lastIndexOf|indexOf)\\(\\s*(\"::\"|':')\\s*\\)|split\\(\\s*\"::\"\\s*\\)"));
        // a signature id cut at '_' to guess its base name
        SHAPES.put("SIGNATURE_ID_CUTTING", Pattern.compile("SignatureMangle\\s*\\.\\s*resolve\\("));
        // the native catalog asked by (possibly bare) NAME rather than by declaration
        SHAPES.put("CATALOG_LOOKUP_BY_NAME", Pattern.compile("\\bnative(Keys|Functions)At\\("));
        // the compiler minting a call by a bare (or any literal) name — step 5
        // spells these by the declaration (untangle 4b.0, 2026-09-25)
        SHAPES.put("MINT_BY_NAME", Pattern.compile("new AppliedFunction\\(\\s*\""));
        // a language form dispatched on a spelled name — 4d dispatches by the
        // form's owned declarations
        SHAPES.put("FORM_DISPATCH_BY_NAME", Pattern.compile("\\bCoreFn\\.of\\("));
        // an implementer family, a language form or a legacy vocabulary looked up by name text
        SHAPES.put("FAMILY_LOOKUP_BY_NAME", Pattern.compile(
                "NativeFn\\s*\\.\\s*[A-Z][A-Za-z]*\\s*\\.\\s*of(Lifted|Derived)?\\(|\\bCoreFn\\s*\\.\\s*of\\("
                        + "|\\.matches\\(\\s*[a-z]+\\s*\\.\\s*function\\(\\)\\s*\\)|RowGetter\\s*\\.\\s*of\\("));
        // a function's CATEGORY asked: compensation for a gap in the generic path
        SHAPES.put("FUNCTION_CATEGORY_CHECK", Pattern.compile(
                "isPlatformOwnedFunction\\(|isVerdictFunction\\(|isStatementOnly\\(|PCT_PROFILE"
                        + "|CORE_FUNCTION_PACKAGES|\"_this\"|\"NormalizeRequiredFunction\""
                        + "|isPlatformImplementedDerived\\(|ASSERT_FAMILY_OWNED"));
    }

    /** The measured counts, 2026-09-24 — SHRINK-ONLY. A site leaves when the
     * code dispatches on the resolved declaration (or a registered lang item)
     * instead; lower the pin in the same commit.
     * 4a (2026-09-24, the pick by table): CATALOG_LOOKUP_BY_NAME 180 -> 179
     * (Scalars' no-rule branch reads the table), FAMILY_LOOKUP_BY_NAME 89 -> 87
     * (the inliner's and StoreEscapees' derived-member checks), FUNCTION_CATEGORY_CHECK
     * 19 -> 16 (isPlatformImplementedDerived and its two readers).
     * 4b.0 (2026-09-25): two shapes ADDED so 4b's deferrals cannot quietly stay —
     * MINT_BY_NAME 145 (the compiler minting a call by a literal name; step 5
     * spells these by the declaration), FORM_DISPATCH_BY_NAME 21 (CoreFn.of on
     * a spelled name; 4d dispatches by the form's owned declarations).
     * 4b.1 (2026-09-25): 214 -> 209, 94 -> 84, 65 -> 64 (readers ask what a call
     * REFERS TO: StaticFold's fold ops, the if-prune, ValidateDesugar, ScanRelations,
     * MappingNormalizer, ContextReading; AppliedFunction.isIf deleted); MINT_BY_NAME
     * 145 -> 144 (booleanizeCaseLiterals keeps the resolved node). */
    private static final Map<String, Integer> PINS = Map.ofEntries(
            Map.entry("NAME_COMPARE", 209),
            Map.entry("NAME_COMPARE_REVERSED", 84),
            Map.entry("LITERAL_NAME_COMPARE", 64),
            Map.entry("NAME_AFFIX_TEST", 53),
            Map.entry("NAME_CUTTING", 106),
            Map.entry("SIGNATURE_ID_CUTTING", 1),
            Map.entry("CATALOG_LOOKUP_BY_NAME", 179),
            Map.entry("FAMILY_LOOKUP_BY_NAME", 87),
            Map.entry("FUNCTION_CATEGORY_CHECK", 16),
            Map.entry("MINT_BY_NAME", 144),
            Map.entry("FORM_DISPATCH_BY_NAME", 21));

    @Test
    void stringIdentityAndCategoryChecksOnlyShrink() throws IOException {
        Map<String, Integer> counts = new LinkedHashMap<>();
        SHAPES.keySet().forEach(k -> counts.put(k, 0));
        List<String> sites = new ArrayList<>();
        sites.add("shape\tsite\tcode");
        try (Stream<Path> files = Files.walk(MAIN)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                String code = withoutComments(Files.readString(f));
                String rel = Repo.rel(MAIN, f);
                for (var shape : SHAPES.entrySet()) {
                    Matcher m = shape.getValue().matcher(code);
                    while (m.find()) {
                        counts.merge(shape.getKey(), 1, Integer::sum);
                        sites.add(shape.getKey() + "\t" + rel + ":" + lineOf(code, m.start())
                                + "\t" + lineText(code, m.start()));
                    }
                }
            }
        }
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("identity-sites.tsv"), sites);
        System.out.println("[identity-guardrail] " + counts);
        List<String> grew = new ArrayList<>();
        for (var e : counts.entrySet()) {
            int pin = PINS.getOrDefault(e.getKey(), 0);
            if (e.getValue() > pin) {
                grew.add(e.getKey() + " " + e.getValue() + " > " + pin);
            }
        }
        assertTrue(grew.isEmpty(), "string identity / function-category checks GREW (shrink-only;"
                + " dispatch on the resolved declaration instead): " + grew
                + " — the sites are in identity-sites.tsv");
    }

    /** {@code src} with every comment replaced by spaces (newlines kept, so
     * offsets keep their line numbers); string and char literals untouched. */
    static String withoutComments(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '"' && src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                end = end < 0 ? n : end + 3;
                out.append(src, i, end);
                i = end;
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < n && src.charAt(j) != c && src.charAt(j) != '\n') {
                    j += src.charAt(j) == '\\' ? 2 : 1;
                }
                j = Math.min(j + 1, n);
                out.append(src, i, j);
                i = j;
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                for (int k = i; k < end; k++) {
                    out.append(src.charAt(k) == '\n' ? '\n' : ' ');
                }
                i = end;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static int lineOf(String code, int offset) {
        int line = 1;
        for (int i = 0; i < offset; i++) {
            if (code.charAt(i) == '\n') {
                line++;
            }
        }
        return line;
    }

    private static String lineText(String code, int offset) {
        int s = code.lastIndexOf('\n', offset) + 1;
        int e = code.indexOf('\n', offset);
        return code.substring(s, e < 0 ? code.length() : e).strip();
    }
}
