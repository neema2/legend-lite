// Copyright 2026 Legend Lite Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.equalitymeasure;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE POSITION MATRIX'S REPORT (tools/equality-measure/README.md; step 6B's slice 2, 2026-11-28: it was
 * {@code positions.py}, moved here so a test can pin it). Reads a run's {@code positions.tsv} and
 * {@code positions-store.tsv} and says, for each comparison and database, whether every place the comparison was
 * asked gave the answer the equality rule gives it. It writes {@code positions.md} (the comparisons that are not
 * right everywhere, place by place), {@code positions-counts.tsv} (a count per kind) and {@code
 * positions-kinds.tsv}: one line per comparison that is not right everywhere, the form the pin holds
 * ({@code tools/equality-measure/positions-baseline.tsv}, {@link EqualityPositionsPinTest}).
 *
 * <p>A comparison's expected answer: {@code T} or {@code F} (values); the ids of the rows it is true for (a store's
 * rows); {@code N}, never true: a compile error ({@code ModelException}) where the two types are written, and false
 * where they are known only when the query runs (a function whose parameters are {@code Any}); {@code a@b}: {@code
 * a} where the comparison is written, {@code b} inside such a function; {@code ?}, not plain from the rule: any one
 * answer, for the user to rule.
 */
final class PositionReport {
    static final List<String> KINDS = List.of("right everywhere", "refused by name everywhere",
            "ANSWERED ONLY IN SOME PLACES", "FAILS SOMEWHERE", "A WRONG ANSWER SOMEWHERE", "CONTRADICTS");
    private static final String LEFT_OUT = "places left out";
    /** A refusal that names what is not supported, or a compile error; anything else that is no answer is a
     *  failure. */
    private static final Set<String> BY_NAME = Set.of("refused ModelException", "refused DialectCapability",
            "refused NotImplementedException", "refused TypeInferenceException", "unjudged");
    private static final Set<String> RUN_TIME_PLACES = Set.of("function-any", "filter-function-any");
    private static final Set<Integer> ALL_ROWS = Set.of(1, 2, 3, 4, 5);
    private static final Pattern KEY = Pattern.compile("\"[^\"]*\"\\s*:");
    private static final Pattern A_ONE = Pattern.compile("\\b1\\b");
    private static final Pattern ID_AND_SAME =
            Pattern.compile("\"id\"\\s*:\\s*(\\d+)\\s*,\\s*\"same\"\\s*:\\s*(true|false|null)");
    private static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*(\\d+)");
    private static final Pattern IDS = Pattern.compile("[\\d,]*");

    private PositionReport() {
    }

    /** One run of the matrix: a line of {@code positions.tsv} or {@code positions-store.tsv}. */
    private record Run(String comparison, String expected, String left, String right, String position,
            boolean negated, String database, String judge, String outcome, String answer) {
    }

    /** One comparison's sides and expected answer: the matrix's row. */
    private record Asked(String name, String expected, String left, String right) {
    }

    /** One database's part of a matrix: the places it cannot answer even for the simplest comparison (each with
     *  why), its count per kind, and its pin lines. */
    private record PerDatabase(Map<String, String> gaps, Map<String, Integer> counts, List<String> pinned) {
    }

    /** One matrix classified: each database's part, the report's lines, the comparisons put to the user, and how
     *  many comparisons in how many places. */
    private record Classified(Map<String, PerDatabase> databases, List<String> lines, List<String> asked,
            int comparisons, int places) {
    }

    /** The report of the run in {@code out}; the pin's lines ({@code positions-kinds.tsv}'s, header included). */
    static List<String> write(Path out) throws IOException {
        StringBuilder md = new StringBuilder("# The position matrix: one comparison asked in every place\n\n");
        Map<String, Map<String, Integer>> totals = new TreeMap<>();
        List<String> kinds = new ArrayList<>();
        kinds.add("database\tmatrix\tcomparison\tkind");
        section(md, totals, kinds, "values", "Values: literals and computed values", read(out.resolve("positions.tsv")),
                false);
        if (Files.exists(out.resolve("positions-store.tsv"))) {
            section(md, totals, kinds, "store", "A store's rows: a comparison that reads the row it stands in",
                    read(out.resolve("positions-store.tsv")), true);
        }
        Files.writeString(out.resolve("positions.md"), md, StandardCharsets.UTF_8);
        // the counts, for a status line: 'right everywhere' should only rise, every other column only fall
        List<String> columns = new ArrayList<>(KINDS);
        columns.add(LEFT_OUT);
        StringBuilder counts = new StringBuilder("database\t" + String.join("\t", columns) + "\n");
        totals.forEach((d, c) -> {
            counts.append(d);
            columns.forEach(k -> counts.append('\t').append(c.getOrDefault(k, 0)));
            counts.append('\n');
        });
        Files.writeString(out.resolve("positions-counts.tsv"), counts, StandardCharsets.UTF_8);
        Files.writeString(out.resolve("positions-kinds.tsv"), String.join("\n", kinds) + "\n", StandardCharsets.UTF_8);
        System.out.println(counts);
        return kinds;
    }

    private static void section(StringBuilder md, Map<String, Map<String, Integer>> totals, List<String> kinds,
            String matrix, String title, List<Run> runs, boolean store) {
        Classified c = classify(runs, matrix, store);
        md.append("## ").append(title).append("\n\n").append(c.comparisons()).append(" comparisons, ")
                .append(runs.size()).append(" runs.\n\n");
        md.append("| database | ").append(String.join(" | ", KINDS)).append(" | places left out |\n|---|")
                .append("---|".repeat(KINDS.size() + 1)).append('\n');
        c.databases().forEach((d, of) -> {
            md.append("| ").append(d);
            KINDS.forEach(k -> md.append(" | ").append(of.counts().getOrDefault(k, 0)));
            md.append(" | ").append(of.gaps().size()).append(" |\n");
            Map<String, Integer> total = totals.computeIfAbsent(d, x -> new LinkedHashMap<>());
            KINDS.forEach(k -> total.merge(k, of.counts().getOrDefault(k, 0), Integer::sum));
            total.merge(LEFT_OUT, of.gaps().size(), Integer::sum);
        });
        md.append("\nPlaces a database cannot answer even for the simplest comparison (left out of the counts):\n");
        c.databases().forEach((d, of) -> {
            List<String> each = new ArrayList<>();
            of.gaps().forEach((p, v) -> each.add(p + " (" + v + ")"));
            md.append("- ").append(d).append(": ").append(each.isEmpty() ? "none" : String.join(", ", each)).append('\n');
        });
        if (!c.asked().isEmpty()) {
            md.append("\nNot plain from the equality rule, for the user (any one answer counts as right):\n")
                    .append(String.join("\n", c.asked())).append('\n');
        }
        md.append("\n### Comparisons that are not right everywhere\n\n")
                .append(c.lines().isEmpty() ? "none" : String.join("\n", c.lines())).append("\n\n");
        kinds.add("*\t" + matrix + "\t(asked)\t" + c.comparisons() + " comparisons in " + c.places() + " places");
        c.databases().values().forEach(of -> kinds.addAll(of.pinned()));
    }

    private static Classified classify(List<Run> runs, String matrix, boolean store) {
        Map<Asked, Map<String, Map<String, String>>> by = new LinkedHashMap<>();
        Set<String> names = new TreeSet<>();
        Set<String> places = new LinkedHashSet<>();
        for (Run r : runs) {
            String place = r.position() + (r.judge().equals("-") ? "" : " (" + r.judge().toLowerCase(Locale.ROOT) + ")");
            by.computeIfAbsent(new Asked(r.comparison(), r.expected(), r.left(), r.right()), x -> new LinkedHashMap<>())
                    .computeIfAbsent(r.database(), x -> new LinkedHashMap<>())
                    .put(place, store ? kept(r) : value(r));
            names.add(r.database());
            places.add(place);
        }
        // a place the database cannot answer even for the first, simplest comparison is that database's own gap; such
        // places are pinned with the rest, so they cannot grow quietly
        Map<String, Map<String, String>> simplest = by.isEmpty() ? Map.of() : by.values().iterator().next();
        Map<String, PerDatabase> databases = new LinkedHashMap<>();
        for (String d : names) {
            Map<String, String> gaps = new LinkedHashMap<>();
            List<String> pinned = new ArrayList<>();
            simplest.getOrDefault(d, Map.of()).forEach((p, v) -> {
                if (!isAnswer(v, store)) {
                    gaps.put(p, v);
                    pinned.add(d + "\t" + matrix + "\t(a place left out)\t" + p + ": " + v);
                }
            });
            databases.put(d, new PerDatabase(gaps, new LinkedHashMap<>(), pinned));
        }
        List<String> lines = new ArrayList<>();
        List<String> asked = new ArrayList<>();
        by.forEach((a, perDb) -> {
            List<String> entry = new ArrayList<>();
            databases.forEach((d, of) -> {
                Map<String, String> all = perDb.getOrDefault(d, Map.of());
                // each place's outcome and what it is against the expected answer, the left-out places apart
                Map<String, List<String>> groups = new LinkedHashMap<>();
                Set<String> verdicts = new LinkedHashSet<>();
                Set<String> answers = new LinkedHashSet<>();
                all.forEach((p, v) -> {
                    if (of.gaps().containsKey(p)) {
                        return;
                    }
                    String verdict = judge(v, a.expected(), p, store);
                    verdicts.add(verdict);
                    if (isAnswer(v, store)) {
                        answers.add(v);
                    }
                    groups.computeIfAbsent((v.isEmpty() ? "no row" : v) + " [" + verdict + "]", x -> new ArrayList<>())
                            .add(p);
                });
                List<String> absent = places.stream().filter(p -> !all.containsKey(p)).toList();
                String kind;
                if (answers.size() > 1 && !a.expected().equals("N")) {
                    kind = "CONTRADICTS";
                } else if (verdicts.contains("wrong")) {
                    kind = "A WRONG ANSWER SOMEWHERE";
                } else if (verdicts.contains("failed") || !absent.isEmpty()) {
                    kind = "FAILS SOMEWHERE";
                } else if (verdicts.contains("refused")) {
                    kind = verdicts.contains("right") ? "ANSWERED ONLY IN SOME PLACES" : "refused by name everywhere";
                } else {
                    kind = "right everywhere";
                }
                of.counts().merge(kind, 1, Integer::sum);
                if (kind.equals("right everywhere")) {
                    return;
                }
                of.pinned().add(d + "\t" + matrix + "\t" + a.name() + "\t" + kind);
                entry.add("  - **" + d + ": " + kind + "**");
                groups.forEach((v, ps) -> entry.add("    - `" + v + "`: " + (ps.size() <= 6 ? String.join(", ", ps)
                        : ps.size() + " places (" + String.join(", ", ps.subList(0, 2)) + ", ...)")));
                if (!absent.isEmpty()) {
                    entry.add("    - never run: " + String.join(", ", absent));
                }
            });
            String sides = "`" + a.left().replace("\\n", " ") + "` == `" + a.right().replace("\\n", " ") + "`";
            if (a.expected().equals("?")) {
                asked.add("- **" + a.name() + "**: " + sides);
            }
            if (!entry.isEmpty()) {
                lines.add("- **" + a.name() + "** (expected `" + a.expected() + "`): " + sides);
                lines.addAll(entry);
            }
        });
        return new Classified(databases, lines, asked, by.size(), places.size());
    }

    /** What one place's outcome is against the expected answer: right, wrong, refused (by name) or failed. */
    private static String judge(String v, String expectedOf, String place, boolean store) {
        boolean atRunTime = RUN_TIME_PLACES.contains(place.split(" ")[0]);
        String expected = expectedOf;
        int at = expected.indexOf('@');
        if (at >= 0) {
            expected = atRunTime ? expected.substring(at + 1) : expected.substring(0, at);
        }
        if (expected.equals("N")) {
            if (atRunTime) {
                return v.equals(store ? "" : "F") ? "right" : isAnswer(v, store) ? "wrong" : "failed";
            }
            return v.equals("refused ModelException") ? "right" : isAnswer(v, store) ? "wrong" : "failed";
        }
        if (isAnswer(v, store)) {
            return expected.equals("?") || v.equals(expected) ? "right" : "wrong";
        }
        return BY_NAME.contains(v) ? "refused" : "failed";
    }

    private static boolean isAnswer(String v, boolean store) {
        return store ? IDS.matcher(v).matches() : v.equals("T") || v.equals("F");
    }

    /** A short name of why a run gave no answer. */
    private static String noAnswer(Run run) {
        if (run.outcome().equals("UNJUDGED")) {
            return "unjudged";
        }
        String name = run.answer().split(":", 2)[0].strip();
        return run.outcome().toLowerCase(Locale.ROOT) + " " + (name.isEmpty() ? run.outcome() : name);
    }

    /** One outcome of the value matrix as {@code T}, {@code F}, or a short name of why there is no answer. */
    private static String value(Run run) {
        boolean v;
        switch (run.outcome()) {
            case "PASS" -> v = true;
            case "FAIL" -> v = false;
            case "ANSWER" -> {
                String text = run.answer().toLowerCase(Locale.ROOT);
                boolean yes = text.contains("true");
                boolean no = text.contains("false");
                if (run.position().equals("table-filter")) {
                    // the filtered one-row table: a row kept, or none
                    v = A_ONE.matcher(KEY.matcher(text).replaceAll("")).find();
                } else if (yes == no || text.contains("null")) {
                    return "other answer";
                } else {
                    v = yes;
                }
            }
            default -> {
                return noAnswer(run);
            }
        }
        return v != run.negated() ? "T" : "F";
    }

    /** One outcome of the store matrix as the ids of the rows the comparison is true for ({@code 1,5}; empty for
     *  none), or a short name of why there is no answer. A kept value that is neither true nor false (a NULL) is no
     *  answer: the rule's comparison is two-valued. */
    private static String kept(Run run) {
        if (!run.outcome().equals("ANSWER")) {
            return noAnswer(run);
        }
        Set<Integer> ids = new TreeSet<>();
        if (run.position().startsWith("value")) {
            Set<Integer> seen = new TreeSet<>();
            Matcher m = ID_AND_SAME.matcher(run.answer());
            while (m.find()) {
                if (m.group(2).equals("null")) {
                    return "other answer";
                }
                seen.add(Integer.valueOf(m.group(1)));
                if (m.group(2).equals("true")) {
                    ids.add(Integer.valueOf(m.group(1)));
                }
            }
            if (!seen.equals(ALL_ROWS)) {
                return "other answer";
            }
        } else {
            Matcher m = ID.matcher(run.answer());
            while (m.find()) {
                ids.add(Integer.valueOf(m.group(1)));
            }
        }
        if (run.negated()) {
            Set<Integer> rest = new TreeSet<>(ALL_ROWS);
            rest.removeAll(ids);
            ids = rest;
        }
        return String.join(",", ids.stream().map(String::valueOf).toList());
    }

    private static List<Run> read(Path path) throws IOException {
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        List<String> header = List.of(lines.get(0).split("\t", -1));
        List<Run> runs = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            String[] cells = line.split("\t", -1);
            runs.add(new Run(cell(header, cells, "comparison"), cell(header, cells, "expected"),
                    cell(header, cells, "left"), cell(header, cells, "right"), cell(header, cells, "position"),
                    cell(header, cells, "negated").equals("true"), cell(header, cells, "database"),
                    header.contains("judge") ? cell(header, cells, "judge") : "-", cell(header, cells, "outcome"),
                    cell(header, cells, "answer")));
        }
        return runs;
    }

    private static String cell(List<String> header, String[] cells, String column) {
        int at = header.indexOf(column);
        if (at < 0 || at >= cells.length) {
            throw new IllegalStateException("no column " + column + " in a line of " + cells.length + " cells");
        }
        return cells[at];
    }
}
