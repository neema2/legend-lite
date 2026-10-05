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
 *   <li><b>the database owner</b> — {@code com.legend.database.Databases} maps a
 *       connection's declared {@code DatabaseType} to its dialect, engine text and
 *       plan facts: the ONE place a type becomes a decision;</li>
 *   <li><b>the raw-SQL boundary</b> — {@code StatementExecutor.adaptRaw}
 *       translates HAND-WRITTEN H2 corpus text for a non-H2 session
 *       ({@code RawSqlBoundary}); text whose origin really is another
 *       dialect, never model-derived SQL.</li>
 * </ul>
 * A new ternary, a new enum of targets, or a new {@code DatabaseType}
 * comparison anywhere else fails here: the fix is a dialect method, never
 * a pin bump.
 *
 * <p>Since C3 (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md, the guard at zero 2026-10-04) every
 * per-database decision has one owner, and the censuses below pin each shape to it: a declared type becomes a dialect
 * in {@code com.legend.database.Databases} (legend-engine's golden text in {@code com.legend.EngineText}, root layer
 * only); a session is checked and opened in {@code com.legend.exec.Sessions}; a driver's own types live beside the
 * driver ({@code exec.DriverCells}). What remains outside them is named with its reason: the connection grammar
 * parses a specification keyword per database, as upstream's grammar does.
 */
@Tag("guardrail")
class DialectBoundaryTest {

    private static final String MAIN = "core/src/main/java/com/legend";

    /** {@code rawH2IsNative()} CALLS outside the dialect package, by file. */
    private static final Map<String, Integer> RAW_H2_CALLERS = Map.of(
            "CsvSeed.java", 1);   // adaptRaw — the raw-SQL boundary (moved from StatementExecutor 2026-09-27 with the setup loop, CsvSeed.run; one site still)

    /** Lines naming a target ({@code DatabaseType.H2} / {@code .DuckDB} / {@code .Postgres})
     *  outside the dialect package, by file. Carrying a type as DATA needs
     *  no literal; naming one is a decision. */
    // 2026-10-01 W5.5/P1 Postgres dialect: the census now counts Postgres too, and
    // Compiler.java went 1 -> 2 — the JDBC dialectOf's PostgreSQL session requires its
    // runtime to declare Postgres, as the H2 session requires H2 (one shared check,
    // requireDeclared; still dialect resolution, the one seam)
    // 2 -> 1 (2026-10-03, the one dialect decision): the database a query executes on is its runtime's
    // DECLARED type (Compiler.executesOn, upstream's createDbConfig(connection.type)); a session is only
    // checked, against the dialect's own jdbcProduct(). The one name left is the platform rule: a runtime
    // with no database runs its model data on DuckDB (SEMANTICS_REGISTER S27)
    // Compiler.java 1 -> 0, Databases.java 0 -> 2, EngineText.java 0 -> 1 (2026-10-03, C3a of
    // docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): every per-database decision on the plan side
    // has ONE owner, com.legend.database.Databases — except legend-engine's golden text, whose renderers are
    // root-layer only (invariant 4d), so its one owner is com.legend.EngineText; the names left are platform facts
    // Sessions.java 0 -> 1 (2026-10-04, C3b): the execution side's one session owner names the in-process
    // databases it keeps a connection for (an in-memory DuckDB; SQLite, which this census does not count)
    private static final Map<String, Integer> DATABASE_TYPE_DECISIONS = Map.of(
            "Databases.java", 2,            // PLATFORM (S27), REPLAY_ORACLE
            "EngineText.java", 1,           // ENGINE_TEST_DATABASE
            "Sessions.java", 1);            // Opening.Held for an in-memory DuckDB

    /** Lines deciding by a database's NAME as a string ({@code "DB2".equals(t)}, {@code case "H2" ->}) outside the
     *  dialect package, by file. Added 2026-10-03 (C3a), pinned at what is left; shrink-only. */
    // SystemDatabase.java 3 -> 0 (2026-10-04, C3b): it opens by the declared type through Sessions
    private static final Map<String, Integer> DATABASE_NAME_DECISIONS = Map.of(
            "ConnectionSectionGrammar.java", 3);  // the grammar: a specification keyword parses per database, as upstream's

    /** Lines CONSTRUCTING a dialect outside the dialect package, by file: the one owner per side picks it
     *  (C3, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md; the guard at zero, 2026-10-04). */
    private static final Map<String, Integer> DIALECT_CONSTRUCTIONS = Map.of(
            "Databases.java", 4,            // the plan side's owner: DuckDB, H2, Postgres, SQLite
            "EngineText.java", 5);          // legend-engine's text printers, root layer only (invariant 4d)

    /** Lines spelling a JDBC URL, by file: only the execution side's session owner opens a database (C3b). */
    private static final Map<String, Integer> JDBC_URLS = Map.of(
            "Sessions.java", 9);            // the declared specifications' URLs and the in-memory instances

    @Test
    void eachDatabaseDecisionHasOneOwner() throws IOException {
        assertEquals(new TreeMap<>(DIALECT_CONSTRUCTIONS),
                census(Pattern.compile("\\bnew\\s+(com\\.legend\\.sql\\.dialect\\.)?(H2|H2Modern|DuckDb|Postgres"
                        + "|AnsiSqlRenderer|EngineStyleH2|EngineStyleDB2|EngineStyleComposite)\\s*\\(")),
                "a dialect constructed outside its owner — ask com.legend.database.Databases (the SQL a"
                        + " query runs) or com.legend.EngineText (legend-engine's golden text)");
        assertEquals(new TreeMap<>(JDBC_URLS), census(Pattern.compile("\"jdbc:")),
                "a JDBC URL outside com.legend.exec.Sessions — it opens every session");
        assertEquals(new TreeMap<>(), census(Pattern.compile("\"org\\.(duckdb|h2|postgresql|sqlite)\\.")),
                "a driver class named in core — a driver's own types live beside it (exec.DriverCells, :duckdb_load)");
    }

    @Test
    void targetsAreDecidedInsideTheDialect() throws IOException {
        assertEquals(new TreeMap<>(RAW_H2_CALLERS),
                census(Pattern.compile("\\.rawH2IsNative\\(\\)")),
                "rawH2IsNative() callers outside com.legend.sql.dialect drifted —"
                        + " a target is decided inside its dialect (render it), never by"
                        + " a ternary at the call site");
        assertEquals(new TreeMap<>(DATABASE_TYPE_DECISIONS),
                census(Pattern.compile("\\bDatabaseType\\s*\\.\\s*(H2|DuckDB|Postgres)\\b")),
                "DatabaseType comparisons outside com.legend.sql.dialect drifted —"
                        + " only dialect resolution maps a declared type to a dialect");
        assertEquals(new TreeMap<>(DATABASE_NAME_DECISIONS),
                census(Pattern.compile("\"(H2|DB2|Composite|DuckDB|Postgres|PostgreSQL|SQLite)\"\\s*\\.\\s*equals"
                        + "|\\.equals\\(\\s*\"(H2|DB2|Composite|DuckDB|Postgres|PostgreSQL|SQLite)\"\\s*\\)"
                        + "|\\bcase\\s+\"(H2|DB2|Composite|DuckDB|Postgres|PostgreSQL|SQLite)\"")),
                "a decision by a database's name drifted — com.legend.database.Databases owns it,"
                        + " by the declared DatabaseType");
        assertEquals(new TreeMap<>(), census(Pattern.compile("\\bFlavor\\.(H2_EXEC|DUCK_EXEC|ENGINE_TEXT)\\b")),
                "a target-flavor enum reappeared: DDL and every other target-dependent"
                        + " text is rendered by the dialect from an IR node");
    }

    /** file name → number of non-comment lines matching, outside the dialect package. */
    private static TreeMap<String, Integer> census(Pattern p) throws IOException {
        TreeMap<String, Integer> out = new TreeMap<>();
        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = SourceFiles.under(MAIN).stream()) {
            // relative to MAIN and '/'-separated: the absolute path would let the
            // checkout's directory names reach the contains(), and on Windows
            // toString() has backslashes, so "/sql/dialect/" never matched there
            s.filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> !SourceFiles.rel(MAIN, f).contains("/sql/dialect/"))
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
