// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE #TS TRANSLATOR STATES NO RULE (the user, 2026-11-01: "get all the logic out of java"; docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md):
 * the Java of {@code com.legend.protocol.ts} writes calls of the timeseries library ({@code timeseries::...}), the
 * user's own SQL translated (a source's WITH body and WHERE, a subscript's conditions, a per-point expression,
 * FILTER: {@code extend} and {@code filter} over its lambdas; a class's paths {@code project}ed from {@code getAll}),
 * and bookkeeping -- a column narrowed ({@code select}), named ({@code rename}), N lines' periods put together
 * ({@code concatenate}). A relation operation that states a rule -- a GROUP BY, a join, a window, a sort, a test of a
 * value -- is the library's: a word, a line's rule ({@code timeseries::lines::}), never the translator's.
 */
@Tag("guardrail")
class TsTranslatorGuardrailTest {

    /** The relation functions the translator may write itself, each why. */
    private static final Set<String> BOOKKEEPING = Set.of(
            "select", "rename", "concatenate",      // columns narrowed and named, N lines' periods together
            "extend", "filter",                     // the user's SQL over its lambdas; a line's constant marker
            "project", "getAll");                   // a class's paths read

    /** Every relation call the translator writes: {@code call(<name>, ...)}, its first argument as written. */
    private static final Pattern CALL = Pattern.compile("\\bcall\\(([^,()]*(?:\\([^()]*\\))?)[,)]");

    /** A call's name that is the library's: a line's rule, a word, a word by its name as written ({@code fqn}: its
     *  path, resolved by {@code TsWords.fqn(model, word)}). */
    private static final Pattern LIBRARY = Pattern.compile("(TsLines\\.)?LINES \\+ \"[A-Za-z_]+\"|PACKAGE \\+ \"[A-Za-z_:]+\""
            + "|fqn(\\([a-z().]+, [a-z]+\\))?");

    /** The forms of {@code call(} that write no relation call: the helpers' declarations, a word's dispatch. */
    private static final Pattern NOT_A_CALL = Pattern.compile("String fn|(TsFunctions\\.)?Ctx c|c|this");

    @Test
    void theTranslatorWritesNoRuleOfItsOwn() throws IOException {
        Map<String, String> rules = new TreeMap<>();
        int files = 0;
        int library = 0;
        for (Path p : SourceFiles.under("core/src/main/java/com/legend/protocol/ts")) {
            if (!p.toString().endsWith(".java")) {
                continue;
            }
            files++;
            String text = Files.readString(p);
            Matcher m = CALL.matcher(text);
            while (m.find()) {
                String fn = m.group(1).strip();
                if (NOT_A_CALL.matcher(fn).matches()) {
                    continue;
                }
                if (LIBRARY.matcher(fn).matches()) {
                    library++;
                } else if (!(fn.startsWith("\"") && BOOKKEEPING.contains(fn.substring(1, fn.length() - 1)))) {
                    rules.merge(fn, SourceFiles.path(p), (a, b) -> a);
                }
            }
            // the one way a relation call is written: the call helpers, so CALL above sees every one
            Matcher made = Pattern.compile("new AppliedFunction\\((?!fn, )").matcher(text);
            if (made.find()) {
                rules.merge("new AppliedFunction(...)", SourceFiles.path(p), (a, b) -> a);
            }
        }
        assertTrue(files >= 6, "the translator's sources read: " + files);
        assertTrue(library >= 10, "the library calls the translator writes, read: " + library);
        assertEquals(Map.of(), rules, "relation operations the #TS translator writes that state a rule -- the"
                + " library's to write (a timeseries::lines:: function or a word), never the translator's");
    }

    /** A parameter compared with a name written here: {@code "unit".equals(p.name())}, {@code p.name().equals("n")}. */
    private static final Pattern BY_NAME = Pattern.compile("\"[A-Za-z_]+\"\\.equals\\([^;\"]*\\bname\\(\\)\\)"
            + "|\\.name\\(\\)\\.equals\\(\"");

    @Test
    void theTranslatorBindsNoParameterByItsName() throws IOException {
        // what a word's parameter is given is read from its type and its place (a span's unit: the DurationUnit after
        // its count; SAMPLE BY's: a timeseries::Sample), never from its name: a word's names are the library's own
        // (2026-11-01: annualization_factor, timeUnit, n and unit were bound by name, and moved into the library)
        java.util.List<String> sites = new java.util.ArrayList<>();
        for (Path p : SourceFiles.under("core/src/main/java/com/legend/protocol/ts")) {
            if (!p.toString().endsWith(".java")) {
                continue;
            }
            String[] lines = Files.readString(p).split("\n", -1);
            for (int i = 0; i < lines.length; i++) {
                if (BY_NAME.matcher(lines[i]).find()) {
                    sites.add(SourceFiles.path(p) + ":" + (i + 1) + ": " + lines[i].strip());
                }
            }
        }
        assertEquals(java.util.List.of(), sites, "a parameter bound by its name");
    }
}
