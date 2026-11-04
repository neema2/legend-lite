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
 * ({@code concatenate}), a relation read again named once ({@code letFunction}). A relation operation that states a rule -- a GROUP BY, a join, a window, a sort, a test of a
 * value -- is the library's: a word, a line's rule ({@code timeseries::lines::}), never the translator's.
 */
@Tag("guardrail")
class TsTranslatorGuardrailTest {

    /** The relation functions the translator may write itself, each why. */
    private static final Set<String> BOOKKEEPING = Set.of(
            "select", "rename", "concatenate",      // columns narrowed and named, N lines' periods together
            "extend", "filter",                     // the user's SQL over its lambdas; a line's constant marker
            "project", "getAll",                    // a class's paths read
            // a relation the translation reads at more than one place named once (TsToPure.bindShared, 2026-11-04,
            // the user: "can we do this for real"): a let states no rule, it names a value the translation already built
            "letFunction");

    /** Every relation call the translator writes: {@code call(<name>, ...)}, its first argument as written. */
    private static final Pattern CALL = Pattern.compile("\\bcall\\(([^,()]*(?:\\([^()]*\\))?)[,)]");

    /** A call's name that is the library's: a line's rule, a word, a word by its name as written ({@code fqn}: its
     *  path, resolved by {@code TsWords.fqn(model, word)}), a value's points or a word's rows form (2026-11-03, R7
     *  part 2: the library's {@code timeseries::rows::} package, {@code ROWS + "..."}, and a word's rows form by its
     *  word's name, {@code rowsFqn}, which is only ever the rows package's: {@link #aRowsFormIsTheRowsPackages}; the
     *  A2 review, 2026-11-03, narrowed a bare {@code rows}, which any local of that name matched). */
    private static final Pattern LIBRARY = Pattern.compile("(TsLines\\.)?LINES \\+ \"[A-Za-z_]+\"|PACKAGE \\+ \"[A-Za-z_:]+\""
            + "|(TsWords\\.)?ROWS \\+ \"[A-Za-z_]+\"|rowsFqn|fqn(\\([a-z().]+, [a-z]+\\))?");

    /** The one name of a word's rows form the translator calls by a variable: always the rows package's. */
    @Test
    void aRowsFormIsTheRowsPackages() throws IOException {
        Pattern assigned = Pattern.compile("\\browsFqn\\s*=\\s*([^;]+);");
        int seen = 0;
        for (Path p : SourceFiles.under("core/src/main/java/com/legend/protocol/ts")) {
            Matcher m = assigned.matcher(Files.readString(p));
            while (m.find()) {
                seen++;
                assertTrue(m.group(1).strip().startsWith("ROWS + "), SourceFiles.path(p) + ": rowsFqn = " + m.group(1));
            }
        }
        assertEquals(1, seen, "the rows form's name, assigned once");
    }

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

    @Test
    void linesMeetByOneLibraryFunctionEachPickedByTheKeysType() throws IOException {
        // R2, R6 (2026-11-01, docs/TS_V1_PROGRAM_2026_11_01.md A1, the census's move 4; 2a, 2026-11-03): a line's kind
        // -- per instrument, or every instrument's -- is its key's type (a line of no instrument's timeseries::All), and
        // the library's overloads say what each means; the translator never picks between two meeting functions by a
        // line's kind (it chose meet or meetOne, periodsOf or periodsOfOne, onto or ontoOne before, then passed
        // timeseries::Keyed)
        String lines = Files.readString(SourceFiles.file("core/src/main/java/com/legend/protocol/ts/TsLines.java"));
        for (String fn : java.util.List.of("meet", "periodsOf", "onto")) {
            Matcher m = Pattern.compile("LINES \\+ \"" + fn + "\"").matcher(lines);
            int n = 0;
            while (m.find()) {
                n++;
            }
            assertEquals(1, n, "TsLines calls timeseries::lines::" + fn + " from one place, picked by the key's type");
        }
        java.util.List<String> variants = new java.util.ArrayList<>();
        for (Path p : SourceFiles.under("core/src/main/java/com/legend/protocol/ts")) {
            Matcher v = Pattern.compile("LINES \\+ \"(meet|periodsOf|onto)[A-Z][A-Za-z]*\"").matcher(Files.readString(p));
            while (v.find()) {
                variants.add(SourceFiles.path(p) + ": " + v.group());
            }
        }
        assertEquals(java.util.List.of(), variants, "a meeting function's variant by a line's kind, chosen by the"
                + " translator");
    }

    /** Every branch of the translator on a line's kind, each named with what it does (2026-11-02,
     *  docs/TS_V1_PROGRAM_2026_11_01.md A1 move 7; docs/TS_ONE_TREE_PLAN_2026_10_31.md §6a, what stays in Java). None
     *  picks a library function: a word's broadcast (helpers::each) is picked by the keys' types since 2a (2026-11-03:
     *  a line of no instrument's key is timeseries::All). A new branch on a line's kind fails here: its kind goes to the
     *  library as a type or as data, or it joins this list with its reason. */
    private static final java.util.Map<String, String> KIND_BRANCHES = java.util.Map.of(
            "TsLines.java: if (l.byInstrument()) {", "the instruments the lines per instrument have, gathered (bookkeeping)",
            "TsLines.java: if (l.byInstrument() && !l.part().equals(key.part())) {", "the refusal: lines by two instrument columns",
            "TsWords.java: if (key.byInstrument() && !v.line().byInstrument()) {", "the refusal of a line of no instrument"
                    + " accrued beside lines per instrument (timed)");

    @Test
    void theTranslatorBranchesOnALinesKindOnlyWhereNamed() throws IOException {
        java.util.Map<String, Integer> found = new java.util.TreeMap<>();
        for (Path p : SourceFiles.under("core/src/main/java/com/legend/protocol/ts")) {
            if (!p.toString().endsWith(".java")) {
                continue;
            }
            for (String line : Files.readString(p).split("\n", -1)) {
                if (line.contains("if (") && line.contains("byInstrument()")) {
                    found.merge(p.getFileName() + ": " + line.strip(), 1, Integer::sum);
                }
            }
        }
        assertEquals(new java.util.TreeSet<>(KIND_BRANCHES.keySet()), found.keySet(), "the translator's branches on a"
                + " line's kind");
        assertEquals(1, found.get("TsWords.java: if (key.byInstrument() && !v.line().byInstrument()) {"),
                "the accrued refusal alone (TsWords.timed): the broadcast is the library's each, by the keys' types");
    }

    /** R7's branches in Java (the A2 review's finding 5, 2026-11-03): where an expression's parts and a word's line
     *  are a table's columns, Java picks the form -- the tenet's exceptions that remain, each with its reason, the list
     *  only shrinking (docs/TENETS.md, the second tenet). They move with a rows form of FILL (docs/TS_V1_PROGRAM_2026_11_01.md
     *  A2: an expression's parts of one table under FILL). */
    private static final java.util.Map<String, String> R7_BRANCHES = java.util.Map.of(
            "TsToPure.java: if (read.stream().allMatch(p -> p.line() == read.get(0).line()) && (fill == null || points.isEmpty())) {",
            "an expression's parts of one table a column of it, else its lines met: the pairing by a row or by a date is"
                    + " Java's, and under FILL with a part's points its lines meet, a table of several rows a date"
                    + " meeting itself (refused, D1)",
            "TsToPure.java: if (points.isEmpty()) {", "every part a point of every row (ORD): no rows::both, rows::masked",
            "TsToPure.java: if (!TsLines.ORD.equals(lines.presence(v))) {", "an item's points its value's (bookkeeping)",
            "TsWords.java: if (kept != null) {", "a word the library declares a rows form of (timeseries::rows::) on its"
                    + " line's table, else its line form");

    private static final Pattern R7_BRANCH = Pattern.compile("if \\(.*(\\.line\\(\\) ==|points\\.isEmpty\\(\\)|kept != null"
            + "|presence\\()");

    @Test
    void theTranslatorPicksATablesFormOnlyWhereNamed() throws IOException {
        java.util.Set<String> found = new java.util.TreeSet<>();
        for (Path p : SourceFiles.under("core/src/main/java/com/legend/protocol/ts")) {
            if (!p.toString().endsWith(".java")) {
                continue;
            }
            for (String line : Files.readString(p).split("\n", -1)) {
                if (R7_BRANCH.matcher(line).find()) {
                    found.add(p.getFileName() + ": " + line.strip());
                }
            }
        }
        assertEquals(new java.util.TreeSet<>(R7_BRANCHES.keySet()), found, "the translator's branches on a table's"
                + " form");
    }

    /** A parameter compared with a name written here: {@code "unit".equals(p.name())}, {@code p.name().equals("n")};
     *  or a word's arguments keyed by a name written here: {@code given.put("w", ...)} (2026-11-01, the census of
     *  docs/TS_V1_PROGRAM_2026_11_01.md A1: a dead {@code TsWords.window} bound {@code w} so, unseen by the first two). */
    private static final Pattern BY_NAME = Pattern.compile("\"[A-Za-z_]+\"\\.equals\\([^;\"]*\\bname\\(\\)\\)"
            + "|\\.name\\(\\)\\.equals\\(\"|\\.(get|put|containsKey|remove)\\(\"[A-Za-z_]+\"");

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
