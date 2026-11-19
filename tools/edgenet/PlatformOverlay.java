// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.edgenet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE EDGE GOLDEN NET's PLATFORM OVERLAY (the compatibility program's slice 0.2, 2026-11-19, the program owner's call on
 * PR #123's first Windows and macOS runs). The net's goldens are Linux's, and every digit of a Float is pinned. DuckDB
 * and SQLite take a few functions ({@code acos}, {@code asin}, {@code cot}, {@code sinh}, {@code cosh}, {@code tanh},
 * {@code cbrt}, {@code pow}) from the platform's C maths library, whose last place differs between glibc, the MSVC
 * runtime and macOS's libm. An overlay lists exactly those values
 * for one OS, keyed by suite, test and case, with the Linux value and that OS's value. It keeps every digit pinned on
 * every OS:
 * <ul>
 *   <li>on its own OS, each of its rows must find its line with that OS's value, which it rewrites to the Linux value
 *       before the diff test compares the line with the Linux golden. A row that finds no such line fails the run (no
 *       stale rows), and any difference no row names still fails the diff test;</li>
 *   <li>on any other OS, each row's line must hold the row's Linux value, so an overlay cannot drift from the golden:
 *       on Linux, whose recording the diff test holds to the golden, a row's Linux value is the golden's.</li>
 * </ul>
 * An overlay file is {@code overlay_<os>.tsv} (os: {@code windows}, {@code macos}): a header line, then one row per
 * fact, its fields {@code suite, test, case, databases, function, linux, <os>}. {@code databases} is a comma list (the
 * same fact on several databases is one row); each value is a VALUE line's {@code Double}. A golden it applies to is a
 * PCT answers file, {@code <database>_<suite>_answers.tsv}.
 */
public final class PlatformOverlay {

    private PlatformOverlay() {
    }

    /** One fact: on {@code os}, {@code test}'s case {@code caseNo} on each of {@code databases} answers {@code other}
     *  where Linux answers {@code linux}. */
    public record Row(String os, String suite, String test, String caseNo, Set<String> databases, String function,
            String linux, String other) {

        String key() {
            return test + "\t" + caseNo;
        }

        @Override
        public String toString() {
            return os + " overlay row " + suite + " " + test + " " + caseNo + " " + databases + " (" + function + ")";
        }
    }

    private static final Pattern FILE = Pattern.compile("overlay_([a-z]+)\\.tsv");

    private static final Pattern GOLDEN = Pattern.compile("([a-z0-9]+)_([a-z]+)_answers\\.tsv");

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
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", -1);
            if (f.length != 7) {
                throw new IllegalArgumentException("overlay_" + os + ".tsv line " + (i + 1) + " has " + f.length
                        + " fields, not 7 (suite, test, case, databases, function, linux, " + os + ")");
            }
            rows.add(new Row(os, f[0], f[1], f[2], new LinkedHashSet<>(List.of(f[3].split(","))), f[4], f[5], f[6]));
        }
        return rows;
    }

    /** The OS this JVM runs on, as an overlay names it. */
    public static String currentOs() {
        String name = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return name.startsWith("windows") ? "windows" : name.startsWith("mac") ? "macos" : "linux";
    }

    /**
     * {@code lines}, the recording of the golden named {@code golden}, with each of {@code rows} applied on
     * {@code currentOs}: on the row's own OS its line's value rewritten from that OS's to Linux's; on another OS its
     * line checked to hold Linux's value. Fails, naming the row, if a row finds no line of its key, or its line holds
     * another value. Rows for other goldens are ignored.
     *
     * <p>The current OS's rows apply first, and only then are the other OSes' rows checked: two OSes can move the same
     * fact (macOS and Windows both move {@code testSinH_Eval} 003, 2026-11-19), and on one of them the other's row finds
     * Linux's value only once that OS's own row has rewritten its line.
     */
    public static List<String> applied(List<String> lines, String golden, List<Row> rows, String currentOs) {
        Matcher g = GOLDEN.matcher(golden);
        if (!g.matches()) {
            return lines;
        }
        String database = g.group(1);
        String suite = g.group(2);
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
        List<String> out = new ArrayList<>(lines);
        for (Row row : ordered) {
            if (!row.suite().equals(suite) || !row.databases().contains(database)) {
                continue;
            }
            boolean mine = row.os().equals(currentOs);
            String expected = valueText(mine ? row.other() : row.linux());
            int found = -1;
            for (int i = 0; i < out.size(); i++) {
                if (out.get(i).startsWith(row.key() + "\t")) {
                    found = i;
                    break;
                }
            }
            if (found < 0) {
                throw new IllegalStateException(row + " finds no line " + row.key() + " in " + golden
                        + ": a stale row (remove it, or the test moved)");
            }
            String line = out.get(found);
            if (!line.endsWith(expected)) {
                throw new IllegalStateException(row + " expects its line in " + golden + " to end " + expected
                        + (mine ? " (this OS's value)" : " (Linux's value, the golden's)") + ", and it is: " + line);
            }
            if (mine) {
                out.set(found, line.substring(0, line.length() - expected.length()) + valueText(row.linux()));
            }
        }
        return out;
    }

    private static String valueText(String value) {
        return "values=Double:" + value + ";";
    }
}
