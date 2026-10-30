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
 *   <li><b>the raw-SQL boundary</b> — {@code BodyDriver.adaptRaw}
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
 * in {@code com.legend.database.Databases} (legend-engine's golden text in {@code com.legend.driver.EngineText}, root layer
 * only); a session is opened by its database's connector ({@code exec/<Db>Connector.java}, found by
 * {@code exec.Connectors}; the dialect program's Phase 5, 2026-10-16), which this census does not walk, as it does not
 * walk the dialects; a driver's own types live beside the driver ({@code exec.DriverCells}). What remains outside them
 * is named with its reason: the connection grammar parses a specification keyword per database, as upstream's grammar
 * does.
 */
@Tag("guardrail")
class DialectBoundaryTest {

    private static final String MAIN = "core/src/main/java/com/legend";

    /** The DuckDB-only sources (the bulk loader, the driver's cells): walked too since 2026-10-13. */
    private static final String DUCKDB_MAIN = "core/src/main/duckdb/com/legend";

    /** Every declared database type, from the enum itself: a type added tomorrow is counted the day it is added
     *  (2026-10-13, the dialect program's Phase 0: the patterns named H2, DuckDB and Postgres only, so every decision
     *  about the other 21 types was invisible here; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md). */
    private static final String TYPES = java.util.Arrays.stream(
            com.legend.model.ConnectionDefinition.DatabaseType.values()).map(Enum::name)
            .collect(java.util.stream.Collectors.joining("|"));

    /** Every name a database goes by: its declared type, and the product name its driver reports where that differs
     *  ({@code DatabaseConnector.jdbcProduct}). */
    private static final String NAMES = TYPES + "|PostgreSQL|SparkSQL|Google BigQuery";

    /** {@code rawH2IsNative()} CALLS outside the dialect package, by file. None: the hook is gone (2026-10-13, the
     *  dialect program's Phase 1c, decision D2) -- raw text runs by the database its connection declares, which the
     *  dialect decides ({@code SqlDialect.raw}). The census stays, so the hook cannot come back unseen. */
    private static final Map<String, Integer> RAW_H2_CALLERS = Map.of();

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
    // root-layer only (invariant 4d), so its one owner is com.legend.driver.EngineText; the names left are platform facts
    // Sessions.java 0 -> 1 (2026-10-04, C3b): the execution side's one session owner names the in-process
    // databases it keeps a connection for (an in-memory DuckDB; SQLite, which this census does not count)
    private static final Map<String, Integer> DATABASE_TYPE_DECISIONS = Map.of(
            "Databases.java", 2,            // PLATFORM (S27), REPLAY_REFEREE
            // Sessions.java: 1 -> 3 (2026-10-13, the dialect program's Phase 0): not new lines -- the census counts
            // every declared type now, and Sessions also named the in-memory SQLite and, on one line, MariaDB and
            // MySQL (raisedMessage's product check); 3 -> 0, row deleted (2026-10-16, the program's Phase 5): each is
            // its connector's (an in-process instance, a held raise), and Sessions asks Connectors
            "EngineText.java", 1);          // ENGINE_TEST_DATABASE

    /** Lines with a switch arm on a declared database type ({@code case Snowflake ->}) outside the dialect package, by
     *  file (added 2026-10-13, the dialect program's Phase 0: the type census saw {@code DatabaseType.X} only, so a
     *  switch over the type was invisible). Pinned at what is there; shrink-only. */
    // 2026-10-17 (docs/MONGODB_PLAN_2026_10_16.md D4, D12): MongoDB is a declared type -- Databases' dialect arm
    // (20 -> 21); Sessions' two refusals by name, no JDBC product and no JDBC opening (24 -> 26); the storeless
    // runtime's refusal of the relational form, its runtime being the engine's MongoDBConnection (5 -> 6)
    private static final Map<String, Integer> DATABASE_TYPE_CASES = Map.of(
            // 20 -> 12 (2026-10-16, the dialect program's Phase 5): a database's server versions and how its rows load
            // are its dialect's (SqlDialect.serverVersions, loadsRowsInBulk); the IN-list facts, legend-engine's, are
            // EngineText's (7 -> 9)
            // 12 -> 14 (2026-10-16, the dialect program's Phase 6): which types the platform executes on
            // (Databases.implemented, which tests and tools iterate instead of listing databases), one arm each way
            // 14 -> 15 (2026-10-22, main's merge): MongoDB executes (the branch's MongoDB line, 2026-10-17), one arm
            "Databases.java", 15,           // the owner: a declared type's dialect
            // Sessions.java 24 -> 0, row deleted, and StorelessRuntime.java 5 -> 0, row deleted (2026-10-16, the
            // dialect program's Phase 5): each type's product name, URL, opening and storeless declaration are its
            // connector's; Connectors is the registry, one arm per type, as Databases is the dialects'
            // 12 -> 13 (2026-10-22, main's merge): MongoDB's arm, its connector found beside the drivers
            "Connectors.java", 13,
            "EngineText.java", 9,           // legend-engine's text and IN-list facts per database, root layer only
            // a connection specification's tag (same names as the types) when a plan's JSON is read: each
            // specification's own fields. PlanJson.java -> ConnectionWire.java (2026-10-16, the dialect program's
            // Phase 5): the codec is the model's, the vocabulary that carries every database's name (D9), and the
            // plan format names none; the arms are the specification tags, not the declared types
            // 4 -> 5 (2026-10-22, main's merge): the Trino specification's tag (the branch's step 9, 2026-10-19)
            "ConnectionWire.java", 5);

    /** Lines deciding by a database's NAME as a string ({@code "DB2".equals(t)}, {@code case "H2" ->}) outside the
     *  dialect package, by file. Added 2026-10-03 (C3a), pinned at what is left; shrink-only. */
    // SystemDatabase.java 3 -> 0 (2026-10-04, C3b): it opens by the declared type through Sessions
    // (SqlTypeCensus.java 10 -> 0, 2026-10-15, the dialect program's Phase 4: each database's wire names and
    // carriers are its dialect's, SqlDialect.wireTypeName and deliversOwn; the census names none. Its history:)
    // 0 -> 2 (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §12): the wire census (a diagnostic, deciding no
    // SQL) reads SQLite's own type names by the label the executor gives it, the dialect class's simple name, as
    // it reads Snowflake's and BigQuery's (names this pattern does not list); exec may not reference the
    // dialect classes themselves (the planning-references ledger). 2 -> 3 (2026-10-13, PR #34's independent
    // review): a map delivered as its entries (an ARRAY) is accepted on BigQuery and Postgres only, the two with
    // no map type; it had been widened to every database, so DuckDB's census could no longer catch a map
    // shipped as an ARRAY. Moves into the dialect with the plan's P4 (the dialect reads its own wire names)
    // 3 -> 10 (2026-10-13, the dialect program's Phase 0): not new lines -- the census names every database now,
    // and the wire census reads Trino's, Snowflake's, ClickHouse's and BigQuery's names too; all of it moves
    // into each dialect with Phase 4
    // (it was "SqlTypeCensus.java", 10)
    private static final Map<String, Integer> DATABASE_NAME_DECISIONS = Map.of(
            // the grammar: a specification keyword parses per database, as upstream's (3 -> 2, 2026-10-11,
            // docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13: the lite database types are a set, no longer SQLite by name)
            // 2 -> 12 (2026-10-13, the dialect program's Phase 0): not new lines -- the census names every database now,
            // and the grammar parses ten more specification keywords by name (DuckDB, SQLite, Snowflake, Redshift, Trino,
            // Athena, Aurora, Oracle, Spanner, Databricks, BigQuery), as upstream's grammar does
            "ConnectionSectionGrammar.java", 12,
            // 0 -> 1 (2026-10-13, Phase 0, the same widening): the PMCD reader's connection kinds by their protocol names
            "PmcdParser.java", 1,
            // 0 -> 1 (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D4): not a new line -- the class mapping's wire
            // discriminator "MongoDB" (legend-engine's own, read since the mapping grammar landed) counts since MongoDB
            // is a declared type; the mapping is carried, decided by nothing (D3: the Relation surface first)
            "MappingProtocolParser.java", 1);

    /** Lines CONSTRUCTING a dialect outside the dialect package, by file: the one owner per side picks it
     *  (C3, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md; the guard at zero, 2026-10-04). */
    private static final Map<String, Integer> DIALECT_CONSTRUCTIONS = Map.of(
            // the plan side's owner: DuckDB, H2, Postgres, SQLite, Databricks, Snowflake, BigQuery (4 -> 7, 2026-10-11,
            // docs/CLOUD_BACKENDS_PCT_2026_10_11.md §12: SQLite is a dialect class of its own, no longer an
            // AnsiSqlRenderer with its lexicon, and the guard names it and the three cloud dialects)
            // 7 -> 11 (2026-10-12, the independent review of neema2/legend-lite#30): MariaDb, MySql, Trino and
            // ClickHouse were constructed there since 2026-10-11/12 but the census below did not name them, so it
            // counted 7; it names all eleven now
            // 11 -> 12 (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D2 and D4): the MongoDb target, a dialect class
            // like the others, constructed by the owner for the MongoDB type
            "Databases.java", 12,
            "EngineText.java", 5);          // legend-engine's text printers, root layer only (invariant 4d)

    /** Lines spelling a JDBC URL, by file: only a database's connector opens it (C3b; connectors since 2026-10-16). */
    // Sessions.java 9 -> 10 (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13): a MariaDB connection's
    // declared static datasource is jdbc:mariadb://
    // Sessions.java 10 -> 0, row deleted (2026-10-16, the dialect program's Phase 5): each database's URLs, the
    // declared specifications' and its private in-memory instance's, are its connector's (exec/<Db>Connector.java),
    // which this census does not walk
    private static final Map<String, Integer> JDBC_URLS = Map.of();

    @Test
    void eachDatabaseDecisionHasOneOwner() throws IOException {
        assertEquals(new TreeMap<>(DIALECT_CONSTRUCTIONS),
                census(Pattern.compile("\\bnew\\s+(com\\.legend\\.sql\\.dialect\\.)?(H2|H2Modern|DuckDb|Postgres"
                        + "|SQLite|Databricks|Snowflake|BigQuery|MariaDb|MySql|Trino|ClickHouse|MongoDb"
                        + "|AnsiSqlRenderer|EngineStyleH2|EngineStyleDB2|EngineStyleComposite)\\s*\\(")),
                "a dialect constructed outside its owner — ask com.legend.database.Databases (the SQL a"
                        + " query runs) or com.legend.driver.EngineText (legend-engine's golden text)");
        assertEquals(new TreeMap<>(JDBC_URLS), census(Pattern.compile("\"jdbc:")),
                "a JDBC URL outside a database's connector (exec/<Db>Connector.java) — it opens that database's"
                        + " sessions");
        // the connectors too (2026-10-16, the dialect program's Phase 5, the audit of the branch): a connector opens
        // its database by URL through DriverManager, and names no driver class
        assertEquals(new TreeMap<>(), census(Pattern.compile("\"(org\\.(duckdb|h2|postgresql|sqlite|mariadb|bson)|com\\.(clickhouse|databricks|mysql"
                        + "|google\\.cloud\\.bigquery|mongodb)|io\\.trino|net\\.snowflake)\\."), true),
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
                census(Pattern.compile("\\bDatabaseType\\s*\\.\\s*(" + TYPES + ")\\b")),
                "DatabaseType comparisons outside com.legend.sql.dialect drifted —"
                        + " only dialect resolution maps a declared type to a dialect");
        assertEquals(new TreeMap<>(DATABASE_TYPE_CASES),
                census(Pattern.compile("\\bcase\\s+(" + TYPES + ")\\b\\s*(,|->|:)")),
                "a switch arm on a declared database type outside its owners drifted —"
                        + " com.legend.database.Databases decides by the declared type");
        assertEquals(new TreeMap<>(DATABASE_NAME_DECISIONS),
                census(Pattern.compile("\"(" + NAMES + ")\"\\s*\\.\\s*equals"
                        + "|\\.equals(IgnoreCase)?\\(\\s*\"(" + NAMES + ")\"\\s*\\)"
                        + "|\\bcase\\s+\"(" + NAMES + ")\"")),
                "a decision by a database's name drifted — com.legend.database.Databases owns it,"
                        + " by the declared DatabaseType");
        assertEquals(new TreeMap<>(), census(Pattern.compile("\\bFlavor\\.(H2_EXEC|DUCK_EXEC|ENGINE_TEXT)\\b")),
                "a target-flavor enum reappeared: DDL and every other target-dependent"
                        + " text is rendered by the dialect from an IR node");
    }

    /** file name → number of non-comment lines matching, outside the dialect package: core's main sources and the
     *  DuckDB-only ones. */
    private static TreeMap<String, Integer> census(Pattern p) throws IOException {
        return census(p, false);
    }

    /** {@link #census(Pattern)}, the connectors walked too when {@code withConnectors}. */
    private static TreeMap<String, Integer> census(Pattern p, boolean withConnectors) throws IOException {
        TreeMap<String, Integer> out = new TreeMap<>();
        List<Path> files = new ArrayList<>();
        SourceFiles.under(DUCKDB_MAIN).stream().filter(f -> f.toString().endsWith(".java")).forEach(files::add);
        try (Stream<Path> s = SourceFiles.under(MAIN).stream()) {
            // relative to MAIN and '/'-separated: the absolute path would let the
            // checkout's directory names reach the contains(), and on Windows
            // toString() has backslashes, so "/sql/dialect/" never matched there
            s.filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> !SourceFiles.rel(MAIN, f).contains("/sql/dialect/"))
                    // a database's connector is its owner on the execution side, as its dialect is on the plan side
                    // (2026-10-16, the dialect program's Phase 5; DatabaseOwners holds each to its own database, in
                    // its code and its strings); the driver census walks them too (withConnectors)
                    .filter(f -> withConnectors || !DatabaseOwners.isConnector(SourceFiles.path(f)))
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
