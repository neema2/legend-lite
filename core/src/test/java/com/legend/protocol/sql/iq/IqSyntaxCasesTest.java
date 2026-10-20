// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.json.Json;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE #IQ{...}# ISLAND'S SYNTAX-CASE RATCHET (docs/IQ_SQL_STUDY_2026_10_13.md §6.4): the research's syntax cases,
 * derived from its own probes only ({@code iq-parity/syntax-cases.jsonl}: an id, the probe, the options it ran under,
 * the grammar's policy for it, SQL Anywhere 17's measured outcome; no manual text), each read by the island's reader
 * as a translation would read it ({@link IqDialect#parse}: {@link IqParser} and the dialect's refusals before
 * translating) under a profile with the case's options, and given a verdict: PARSED (an AST the translator takes),
 * REFUSED (refused by name: the form may be IQ's, it is not read yet), REJECTED (the reader's error: IQ's syntax error,
 * or one IQ raises before running), CRASHED (anything else: a defect, never allowed).
 *
 * <p>The counts per policy and verdict are pinned in {@code iq-parity/syntax-pins.tsv}, and so is every case whose
 * verdict is a known wrong one: a {@code reject} case that parses (IQ rejects it; the translator would take it) and an
 * {@code accept} case the reader rejects (IQ reads it; the reader says it cannot be IQ). Both lists only shrink, the
 * PARSED count of the accepting policies only grows: a change either way fails, as a regression or as a fix to record
 * ({@code -Diq.syntax.update=<the pins' path in the workspace>} rewrites the file, refusing a regression). So a later
 * slice can only make the reader read more of what IQ reads and less of what it rejects.
 */
class IqSyntaxCasesTest {

    /** A case's verdict. */
    enum Verdict { PARSED, REFUSED, REJECTED, CRASHED }

    /** The options whose values the reader reads; any other option (gating a later slice's form, a run-time rule) does
     *  not change how a query parses here. */
    private static final Set<String> TDS_OPTIONS = Set.of("quoted_identifier", "escape_character");

    @Test
    void theSyntaxCasesOnlyImprove() throws IOException {
        Map<String, Map<Verdict, Integer>> counts = new TreeMap<>();
        Set<String> rejectParsed = new TreeSet<>();
        Set<String> acceptRejected = new TreeSet<>();
        List<String> crashed = new ArrayList<>();
        for (String line : Files.readAllLines(Runfile.property("iq.syntax.cases"), StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            Json.Obj o = (Json.Obj) Json.parse(line);
            String id = ((Json.Str) o.get("id")).value();
            String probe = ((Json.Str) o.get("probe")).value();
            String policy = ((Json.Str) o.get("policy")).value();
            String family = policy.contains(":") ? policy.substring(0, policy.indexOf(':')) : policy;
            Json.Obj options = (Json.Obj) o.get("options");
            Verdict v = refusedOption(options) ? Verdict.REFUSED : verdict(probe, profile(options), crashed, id);
            counts.computeIfAbsent(family, k -> new TreeMap<>()).merge(v, 1, Integer::sum);
            if (family.equals("reject") && v == Verdict.PARSED) {
                rejectParsed.add(id);
            }
            if (!family.equals("reject") && v == Verdict.REJECTED) {
                acceptRejected.add(id);
            }
        }
        List<String> problems = new ArrayList<>();
        for (String c : crashed) {
            problems.add("CRASHED: " + c);
        }
        Pins now = new Pins(counts, rejectParsed, acceptRejected);
        Path pinsFile = Runfile.property("iq.syntax.pins");
        String pinsText = Files.readString(pinsFile, StandardCharsets.UTF_8);
        Pins pinned = Pins.read(pinsText);
        String update = System.getProperty("iq.syntax.update");
        // an empty pins file is seeded by the first update: there is nothing yet to regress from
        List<String> regressions = update != null && pinned.counts().isEmpty() ? List.of()
                : now.regressionsAgainst(pinned);
        problems.addAll(regressions);
        System.out.println("[iq-syntax] " + now.summary());
        if (update != null && regressions.isEmpty() && crashed.isEmpty()) {
            // the pins' dated notes (why they moved) are kept
            Files.writeString(Path.of(update), now.text(Pins.notes(pinsText)), StandardCharsets.UTF_8);
        } else if (!now.equals(pinned) && regressions.isEmpty()) {
            problems.add("FIX DETECTED: the syntax cases improved (" + now.summary() + "); record it with"
                    + " -Diq.syntax.update=<core/src/test/resources/iq-parity/syntax-pins.tsv in the workspace>");
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    /** The profile a case's options select: a TDS login's when the probe turns quoted identifiers or escapes off
     *  (the two options the reader reads), else the default. */
    private static IqProfile profile(Json.Obj options) {
        for (Map.Entry<String, Json.Node> e : options.fields().entrySet()) {
            if (TDS_OPTIONS.contains(e.getKey().toLowerCase(java.util.Locale.ROOT)) && e.getValue() instanceof Json.Str s
                    && s.value().equalsIgnoreCase("off")) {
                return IqProfile.IQ16_TDS;
            }
        }
        return IqProfile.IQ16_DEFAULT;
    }

    /** Whether a case's options set one that changes what parses and that the island refuses a database for (IQ
     *  island, 2026-10-17: reserved_keywords, which makes LIMIT a clause -- under its default IQ 17.1 raises its syntax
     *  error for a row limit spelled LIMIT, and a database setting it is refused by name, {@code IqSettingsTest}; and
     *  non_keywords, which makes a reserved word a name -- under its default merge.a is IQ's -131). */
    private static boolean refusedOption(Json.Obj options) {
        for (Map.Entry<String, Json.Node> e : options.fields().entrySet()) {
            String name = e.getKey().toLowerCase(java.util.Locale.ROOT);
            if ((name.equals("reserved_keywords") || name.equals("non_keywords")) && e.getValue() instanceof Json.Str s
                    && !s.value().isEmpty()) {
                try {
                    IqSettings.resolve(IqProfile.IQ16_DEFAULT, List.of(new IqSettings.Setting(name.equals(
                            "non_keywords") ? IqSettings.Level.QUERY : IqSettings.Level.DATABASE, e.getKey(),
                            s.value()))).check();
                } catch (SqlTranslationException refused) {
                    return refused.kind() == SqlTranslationException.Kind.UNSUPPORTED;
                }
            }
        }
        return false;
    }

    private static Verdict verdict(String probe, IqProfile profile, List<String> crashed, String id) {
        try {
            IqDialect.of(profile.settings()).parse(probe);
            return Verdict.PARSED;
        } catch (SqlSyntaxException e) {
            return e.unsupported() ? Verdict.REFUSED : Verdict.REJECTED;
        } catch (SqlTranslationException e) {
            return e.kind() == SqlTranslationException.Kind.UNSUPPORTED ? Verdict.REFUSED : Verdict.REJECTED;
        } catch (RuntimeException | StackOverflowError e) {
            crashed.add(id + ": " + e);
            return Verdict.CRASHED;
        }
    }

    /** The pinned measure: counts per policy and verdict, and the two lists of wrong verdicts. */
    record Pins(Map<String, Map<Verdict, Integer>> counts, Set<String> rejectParsed, Set<String> acceptRejected) {

        private static final String HEADER = "# the #IQ{...}# island's syntax-case ratchet (IqSyntaxCasesTest): per policy and"
                + " verdict, how many of iq-parity/syntax-cases.jsonl the\n# reader gives that verdict; then each reject"
                + " case it parses and each accepting case it rejects (lists that only shrink).\n# Rewrite with"
                + " -Diq.syntax.update=<this file's path in the workspace>; a regression is refused.\n";

        /** The comment lines of a pins file other than its header: the dated notes of why the pins moved. */
        static List<String> notes(String text) {
            List<String> notes = new ArrayList<>();
            for (String line : text.split("\n")) {
                if (line.startsWith("#") && !HEADER.contains(line + "\n")) {
                    notes.add(line);
                }
            }
            return notes;
        }

        String text(List<String> notes) {
            StringBuilder b = new StringBuilder(HEADER);
            notes.forEach(n -> b.append(n).append('\n'));
            counts.forEach((policy, byVerdict) -> byVerdict.forEach((v, n) -> b.append(policy).append('\t')
                    .append(v.name().toLowerCase(java.util.Locale.ROOT)).append('\t').append(n).append('\n')));
            rejectParsed.forEach(id -> b.append("reject-parsed\t").append(id).append('\n'));
            acceptRejected.forEach(id -> b.append("accept-rejected\t").append(id).append('\n'));
            return b.toString();
        }

        static Pins read(String text) {
            Map<String, Map<Verdict, Integer>> counts = new TreeMap<>();
            Set<String> rejectParsed = new TreeSet<>();
            Set<String> acceptRejected = new TreeSet<>();
            for (String line : text.split("\n")) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] f = line.split("\t", -1);
                switch (f[0]) {
                    case "reject-parsed" -> rejectParsed.add(f[1]);
                    case "accept-rejected" -> acceptRejected.add(f[1]);
                    default -> counts.computeIfAbsent(f[0], k -> new TreeMap<>()).put(
                            Verdict.valueOf(f[1].toUpperCase(java.util.Locale.ROOT)), Integer.parseInt(f[2]));
                }
            }
            return new Pins(counts, rejectParsed, acceptRejected);
        }

        private int count(String policy, Verdict v) {
            return counts.getOrDefault(policy, Map.of()).getOrDefault(v, 0);
        }

        /** What got worse than {@code pinned}: a newly listed wrong verdict, fewer parsed in an accepting policy,
         *  more parsed among the rejects. */
        List<String> regressionsAgainst(Pins pinned) {
            List<String> out = new ArrayList<>();
            for (String id : rejectParsed) {
                if (!pinned.rejectParsed.contains(id)) {
                    out.add("REGRESSION: the reject case " + id + " now parses (IQ rejects it)");
                }
            }
            for (String id : acceptRejected) {
                if (!pinned.acceptRejected.contains(id)) {
                    out.add("REGRESSION: the accepted case " + id + " is now rejected by the reader (IQ reads it)");
                }
            }
            Set<String> policies = new TreeSet<>(counts.keySet());
            policies.addAll(pinned.counts.keySet());
            for (String policy : policies) {
                int was = pinned.count(policy, Verdict.PARSED);
                int is = count(policy, Verdict.PARSED);
                if (!policy.equals("reject") && is < was) {
                    out.add("REGRESSION: " + policy + " cases parsed fell from " + was + " to " + is);
                }
                if (policy.equals("reject") && is > was) {
                    out.add("REGRESSION: reject cases parsed rose from " + was + " to " + is);
                }
            }
            return out;
        }

        String summary() {
            StringBuilder b = new StringBuilder();
            counts.forEach((policy, byVerdict) -> b.append(b.isEmpty() ? "" : "; ").append(policy).append(' ')
                    .append(byVerdict));
            return b + "; reject-parsed " + rejectParsed.size() + ", accept-rejected " + acceptRejected.size();
        }
    }
}
