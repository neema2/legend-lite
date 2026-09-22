// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE DIALECT BOUNDARY (user ruling 2026-09-16, after three patches that
 * chose a target OUTSIDE its dialect: a {@code Ddl.Flavor} enum, a
 * {@code constraints} boolean, {@code rawH2IsNative() ? A : B} ternaries at
 * call sites). A target is decided INSIDE {@code com.legend.sql.dialect};
 * everything else passes the session's dialect and asks it to render.
 *
 * <p>Two seams legitimately name a target outside the package, and they
 * are pinned BY FILE AND COUNT, shrink-only:
 * <ul>
 *   <li><b>dialect resolution</b> — {@code Compiler.dialectOf} maps a
 *       connection's declared {@code DatabaseType} and the JDBC product to
 *       a dialect instance: the ONE place a name becomes a dialect;</li>
 *   <li><b>the raw-SQL boundary</b> — {@code StatementExecutor.adaptRaw}
 *       translates HAND-WRITTEN H2 corpus text for a non-H2 session
 *       ({@code RawSqlBoundary}); text whose origin really is another
 *       dialect, never model-derived SQL.</li>
 * </ul>
 * A new ternary, a new enum of targets, or a new {@code DatabaseType}
 * comparison anywhere else fails here: the fix is a dialect method, never
 * a pin bump.
 */
class DialectBoundaryTest {

    private static final Path MAIN = Repo.module("src/main/java/com/legend");

    /** {@code rawH2IsNative()} CALLS outside the dialect package, by file. */
    private static final Map<String, Integer> RAW_H2_CALLERS = Map.of(
            "StatementExecutor.java", 1);   // adaptRaw — the raw-SQL boundary

    /** Lines naming a target ({@code DatabaseType.H2} / {@code .DuckDB})
     *  outside the dialect package, by file. Carrying a type as DATA needs
     *  no literal; naming one is a decision. */
    private static final Map<String, Integer> DATABASE_TYPE_DECISIONS = Map.of(
            "Compiler.java", 1);            // dialectOf — dialect resolution

    @Test
    void targetsAreDecidedInsideTheDialect() throws IOException {
        assertEquals(new TreeMap<>(RAW_H2_CALLERS),
                census(Pattern.compile("\\.rawH2IsNative\\(\\)")),
                "rawH2IsNative() callers outside com.legend.sql.dialect drifted —"
                        + " a target is decided inside its dialect (render it), never by"
                        + " a ternary at the call site");
        assertEquals(new TreeMap<>(DATABASE_TYPE_DECISIONS),
                census(Pattern.compile("\\bDatabaseType\\s*\\.\\s*(H2|DuckDB)\\b")),
                "DatabaseType comparisons outside com.legend.sql.dialect drifted —"
                        + " only dialect resolution maps a declared type to a dialect");
        assertEquals(new TreeMap<>(), census(Pattern.compile("\\bFlavor\\.(H2_EXEC|DUCK_EXEC|ENGINE_TEXT)\\b")),
                "a target-flavor enum reappeared: DDL and every other target-dependent"
                        + " text is rendered by the dialect from an IR node");
    }

    /** file name → number of non-comment lines matching, outside the dialect package. */
    private static TreeMap<String, Integer> census(Pattern p) throws IOException {
        TreeMap<String, Integer> out = new TreeMap<>();
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.walk(MAIN)) {
            // relative to MAIN and '/'-separated: the absolute path would let the
            // checkout's directory names reach the contains(), and on Windows
            // toString() has backslashes, so "/sql/dialect/" never matched there
            s.filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> !Repo.rel(MAIN, f).contains("/sql/dialect/"))
                    .forEach(files::add);
        }
        for (Path f : files) {
            int n = 0;
            for (String line : Files.readAllLines(f)) {
                String code = line.strip();
                if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                    continue;
                }
                if (p.matcher(code).find()) {
                    n++;
                }
            }
            if (n > 0) {
                out.put(f.getFileName().toString(), n);
            }
        }
        return out;
    }
}
