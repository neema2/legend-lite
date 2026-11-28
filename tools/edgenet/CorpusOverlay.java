// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.edgenet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE CORPUS's PLATFORM OVERLAY (2026-11-26; the user approved an overlay for the macOS corpus lanes that day, "yes
 * Macos", over a tolerance or dropping the lane). PCT's overlay ({@link PlatformOverlay}) is the model. The corpus
 * lanes' goldens are Linux's. DuckDB takes some maths functions ({@code acos}, {@code asin}, {@code atan2}, {@code tan})
 * from the platform's C maths library, and on macOS their last place differs (measured on PR #141's gate run
 * 45788970603, job "macos / corpus_duckdb"). PCT records the value itself, so its overlay rewrites one value. A corpus
 * test instead compares the value with a literal written in the test, which is Linux's value, so on macOS the test's
 * OUTCOME flips. Its first assert fails, and three goldens move: the host judge's ledger (the test's PASS lines become one
 * FAIL line naming both values), the fail roster (it gains the test) and the unordered register (it loses the test's
 * {@code unordered-chain} line).
 *
 * <p>An overlay file is {@code overlay_<os>.tsv}, named as PCT's are: a header line, then one row per fact, its fields
 * {@code test, databases, function, linux, <os>}. {@code databases} is a comma list of corpus lanes. Each row keeps every
 * digit pinned on every OS:
 * <ul>
 *   <li>on its own OS, the recording must show exactly the flip: in the ledger, ONE line, the test's first assert
 *       {@code FAIL}ing with a message that names the row's Linux value once and its OS value once; in the fail roster,
 *       the test's name; in the unordered register, nothing. Only then are the test's lines replaced by the Linux
 *       golden's lines for that test, so the diff test compares the rest. A row that does not find exactly that
 *       (a stale row, or another value, or another failure) fails the run, and any difference no row names still fails
 *       the diff test;</li>
 *   <li>on any other OS, the recording must show the test passing (in the ledger, every line {@code PASS}; in the fail
 *       roster, absent; in the unordered register, present). On Linux, whose recording the diff test holds to the
 *       golden, that is the golden's, so a row cannot outlive the fact it names. The Linux value itself is pinned by
 *       the test's own literal: Linux passes only when it computes it, and the own-OS check requires the failure
 *       message to name it.</li>
 * </ul>
 * An OS's own rows apply before the other OSes' rows are checked, as in {@link PlatformOverlay}.
 */
public final class CorpusOverlay {

    private CorpusOverlay() {
    }

    /** The corpus lanes a row may name: a row naming no lane would be applied by no normalizer, so never found stale. */
    static final Set<String> LANES = Set.of("duckdb", "h2");

    /** One fact: on {@code os}, the corpus test {@code test} on each of {@code databases} computes {@code other} where
     *  Linux computes {@code linux} (the test's literal), so the test fails there. */
    public record Row(String os, String test, Set<String> databases, String function, String linux, String other) {

        @Override
        public String toString() {
            return os + " corpus overlay row " + test + " " + databases + " (" + function + ")";
        }
    }

    /** The three measured files a flip moves. */
    public enum Kind {
        LEDGER, FAIL_ROSTER, UNORDERED_REGISTER;

        /** The kind of the golden named {@code golden} (its file name). */
        public static Kind of(String golden) {
            if (golden.endsWith("_judge-host.tsv") || golden.equals("judge-host.tsv")) {
                return LEDGER;
            }
            if (golden.endsWith("-fail-roster.txt")) {
                return FAIL_ROSTER;
            }
            if (golden.endsWith("-unordered-register.txt")) {
                return UNORDERED_REGISTER;
            }
            throw new IllegalArgumentException("the corpus overlay applies to a host ledger, a fail roster or an "
                    + "unordered register, not " + golden);
        }
    }

    private static final Pattern FILE = Pattern.compile("overlay_([a-z]+)\\.tsv");

    /** The rows of an overlay file, its OS from its name. */
    public static List<Row> read(Path file) throws IOException {
        Matcher name = FILE.matcher(file.getFileName().toString());
        if (!name.matches()) {
            throw new IllegalArgumentException("an overlay is named overlay_<os>.tsv: " + file);
        }
        return parse(name.group(1), Files.readAllLines(file, StandardCharsets.UTF_8));
    }

    /** The rows of an overlay's lines (a header first), for {@code os}. */
    public static List<Row> parse(String os, List<String> lines) {
        String header = "test\tdatabases\tfunction\tlinux\t" + os;
        if (lines.isEmpty() || !lines.get(0).equals(header)) {
            throw new IllegalArgumentException("a corpus overlay_" + os + ".tsv starts with the header " + header);
        }
        List<Row> rows = new ArrayList<>();
        Set<String> tests = new HashSet<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 5) {
                throw new IllegalArgumentException("overlay_" + os + ".tsv line " + (i + 1) + " has " + f.length
                        + " fields, not 5 (test, databases, function, linux, " + os + ")");
            }
            Set<String> databases = new LinkedHashSet<>(List.of(f[1].split(",")));
            if (!LANES.containsAll(databases)) {
                throw new IllegalArgumentException("overlay_" + os + ".tsv line " + (i + 1) + " names " + databases
                        + ", and the corpus lanes are " + LANES);
            }
            if (!tests.add(f[0])) {
                throw new IllegalArgumentException("overlay_" + os + ".tsv names " + f[0] + " twice");
            }
            if (f[3].equals(f[4])) {
                throw new IllegalArgumentException("overlay_" + os + ".tsv line " + (i + 1) + " names one value twice");
            }
            rows.add(new Row(os, f[0], databases, f[2], f[3], f[4]));
        }
        return rows;
    }

    /**
     * {@code recording}, the measured file whose golden is {@code golden} ({@code kind}, on lane {@code database}), with
     * each of {@code rows} applied on {@code currentOs} (the class's rules). Fails, naming the row, on a stale row or a
     * recording that does not show the row's fact.
     */
    public static List<String> applied(List<String> recording, List<String> golden, Kind kind, String database,
            List<Row> rows, String currentOs) {
        List<Row> ordered = new ArrayList<>();
        for (Row row : rows) {
            if (row.os().equals(currentOs)) {
                ordered.add(row);
            }
        }
        for (Row row : rows) {
            if (!row.os().equals(currentOs)) {
                ordered.add(row);
            }
        }
        List<String> out = new ArrayList<>(recording);
        for (Row row : ordered) {
            if (!row.databases().contains(database)) {
                continue;
            }
            List<Integer> at = block(out, row.test());
            List<String> lines = at.stream().map(out::get).toList();
            if (row.os().equals(currentOs)) {
                checkFlipped(row, kind, lines);
                List<String> linux = block(golden, row.test()).stream().map(golden::get).toList();
                checkPassing(row, kind, linux, "the Linux golden");
                int where = at.isEmpty() ? sortedPosition(out, linux.get(0)) : at.get(0);
                for (int i = at.size() - 1; i >= 0; i--) {
                    out.remove(at.get(i).intValue());
                }
                out.addAll(where, linux);
            } else {
                checkPassing(row, kind, lines, "this OS's recording (" + currentOs + ")");
            }
        }
        return out;
    }

    /** The indices of the lines naming {@code test}: a ledger's first field, a roster's line, a register's last word. */
    private static List<Integer> block(List<String> lines, String test) {
        List<Integer> at = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.equals(test) || line.startsWith(test + "\t") || line.endsWith(" " + test)) {
                at.add(i);
            }
        }
        return at;
    }

    private static int sortedPosition(List<String> lines, String line) {
        int i = 0;
        while (i < lines.size() && lines.get(i).compareTo(line) < 0) {
            i++;
        }
        return i;
    }

    /** On the row's own OS: the recording's lines for the test show exactly the row's flip. */
    private static void checkFlipped(Row row, Kind kind, List<String> lines) {
        String why = switch (kind) {
            case LEDGER -> {
                if (lines.size() != 1) {
                    yield lines.size() + " ledger lines, not the one failing assert";
                }
                String[] f = lines.get(0).split("\t", -1);
                if (f.length != 5 || !f[1].equals("0") || !f[3].equals("FAIL")) {
                    yield "a ledger line that is not the first assert failing: " + lines.get(0);
                }
                int linux = occurrences(f[4], row.linux());
                int other = occurrences(f[4], row.other());
                yield linux == 1 && other == 1 ? null
                        : "a failure naming the Linux value " + linux + " times and this OS's " + other
                                + " times, not once each: " + f[4];
            }
            case FAIL_ROSTER -> lines.equals(List.of(row.test())) ? null : "fail roster lines " + lines
                    + ", not the test's name alone";
            case UNORDERED_REGISTER -> lines.isEmpty() ? null : "unordered register lines " + lines
                    + ", where a failing test has none";
        };
        if (why != null) {
            throw new IllegalStateException(row + " expects the test to fail on its OS, and finds " + why
                    + " (a stale row, or another difference)");
        }
    }

    /** The test passing in {@code lines} (the golden's, or another OS's recording). */
    private static void checkPassing(Row row, Kind kind, List<String> lines, String where) {
        boolean passing = switch (kind) {
            case LEDGER -> !lines.isEmpty() && lines.stream().allMatch(l -> l.endsWith("\tPASS"));
            case FAIL_ROSTER -> lines.isEmpty();
            case UNORDERED_REGISTER -> !lines.isEmpty();
        };
        if (!passing) {
            throw new IllegalStateException(row + " expects the test to pass in " + where + ", and its "
                    + kind.name().toLowerCase(java.util.Locale.ROOT) + " lines are " + lines
                    + ": a stale row (remove it, or the test moved)");
        }
    }

    /** How often {@code value} occurs in {@code text} as a whole number (no digit, point or sign joined to it). */
    static int occurrences(String text, String value) {
        Matcher m = Pattern.compile("(?<![0-9.\\-])" + Pattern.quote(value) + "(?![0-9])").matcher(text);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    /** Arguments: {@code <database> <recording> <golden> <out> [overlay_<os>.tsv ...]}; the kind from {@code <golden>}'s
     *  file name. */
    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            throw new IllegalArgumentException("CorpusOverlay <database> <recording> <golden> <out> [overlay_<os>.tsv ...]");
        }
        List<Row> rows = new ArrayList<>();
        for (int i = 4; i < args.length; i++) {
            rows.addAll(read(Path.of(args[i])));
        }
        Path golden = Path.of(args[2]);
        List<String> recording = Files.readAllLines(Path.of(args[1]), StandardCharsets.UTF_8);
        List<String> out = applied(recording, Files.readAllLines(golden, StandardCharsets.UTF_8),
                Kind.of(golden.getFileName().toString()), args[0], rows, PlatformOverlay.currentOs());
        if (out.equals(recording)) {
            // nothing rewritten (every OS but a row's own): the recording byte for byte, its line endings included
            Files.copy(Path.of(args[1]), Path.of(args[3]));
            return;
        }
        StringBuilder text = new StringBuilder();
        for (String line : out) {
            text.append(line).append('\n');
        }
        Files.writeString(Path.of(args[3]), text, StandardCharsets.UTF_8);
    }
}
