// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE OWNERS: which sources may name a database, and which databases each may name (the dialect program,
 * docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md §4 and decisions D9 to D11, the user's, 2026-10-15). The leak ratchet
 * counts a database named anywhere else ({@link DialectLeakRatchetTest}); the owner guard holds each owner to its own
 * ({@link DialectOwnerExclusivityTest}). Five kinds:
 * <ul>
 *   <li>a <b>dialect</b> (the SQL a database runs, {@code sql/dialect/}) and the DuckDB-only execution sources
 *       ({@code core/src/main/duckdb}): the database its class is named for, and its family's (the class it
 *       extends: {@code MySql} names MariaDB); a file named for none is shared, and names none;</li>
 *   <li>a <b>connector</b> (a database's sessions, {@code exec/*Connector.java}, the dialect program's Phase 5,
 *       2026-10-16): the same, by its class's name and its parent's ({@code MySqlConnector} names MariaDB), in its
 *       code and in its strings too (its URLs, its product name); {@code DatabaseConnector}, the interface, names
 *       none;</li>
 *   <li>a <b>test backend</b> (a database as the tests run on it, {@code testbackend/*Backend.java} in core's tests,
 *       the program's Phase 6, 2026-10-16): the same as a connector; {@code TestBackend} and the shared bases
 *       ({@code InProcessBackend}, {@code ServerBackend}, {@code CloudBackend}) name none;</li>
 *   <li>a <b>test named for a database</b> (any test source whose class is named for one: a test, {@code
 *       DuckDbRenderTest}, {@code PostgresArmTest}, a PCT lane's {@code Test_LegendLite_Postgres_*}, or a harness,
 *       {@code H2ExtensionFunctions}; Phase 6, 2026-10-16): that database and its parent class's, in its code and its strings, as a
 *       connector is held to its own. A test that is about no one database names none: it runs on the platform's
 *       engine or on every database ({@code testbackend.TestBackends});</li>
 *   <li>an <b>input language</b> (the SQL a user writes, {@code protocol/sql/}, D11): {@code PostgresDialect} and
 *       {@code PostgresDateTime} Postgres's, the {@code iq} package IQ's (Sybase's); the translator they share is no
 *       owner;</li>
 *   <li>a <b>host</b> (a module whose engine is one database by design, the census's SINGLE-BACKEND-APP; the program's
 *       Phase 7, 2026-10-18): the warehouse is one DuckDB, the host of every catalog, which attaches other databases
 *       through DuckDB's extensions (D4). Its sources name DuckDB, in their code and their strings; another database
 *       only in a file named for it ({@code Postgres}, {@code PostgresUrl}, {@code PostgresCatalogTest}) and in
 *       {@code Attachment}, the registry of the databases it attaches;</li>
 *   <li>a <b>vocabulary</b> (the model language: the lexer, the parser and its grammars, the protocol, the model, D9):
 *       it may parse and carry any database's name, and decides nothing by one;</li>
 *   <li>the <b>registries</b> ({@code database.Databases}, a declared type to its dialect; {@code exec.Connectors}, to
 *       its connector; {@code testbackend.TestBackends}, to its test backend; the warehouse's {@code Attachment}, to
 *       how it attaches one) and the guards, which name every database by design.</li>
 * </ul>
 * The dialect package's tests name several databases by design (one test compares them) and are owners of every one.
 */
final class DatabaseOwners {

    private DatabaseOwners() {
    }

    enum Kind { DIALECT, CONNECTOR, BACKEND, NAMED_TEST, INPUT_LANGUAGE, HOST, VOCABULARY, EVERY }

    /** An owner: its kind, and the databases it may name (for a vocabulary or the registry, every one). */
    record Owner(Kind kind, Set<String> databases) {
    }

    /** Every database the census sees, each by the pattern of its name in an identifier: the ratchet's product-name
     *  kind, one database at a time. H2 and DB2 as the ratchet reads them (an identifier's word, never a word's
     *  letters: {@code width2} is no H2). */
    static final Map<String, Pattern> DATABASES = databases();

    private static Map<String, Pattern> databases() {
        Map<String, String> names = new LinkedHashMap<>();
        names.put("DuckDB", "duck\\s?db");
        names.put("H2", "(?-i:(?<=[a-z0-9])H2|(?<![A-Za-z0-9])[Hh]2)(?![0-9])");
        names.put("DB2", "(?-i:(?<=[a-z0-9])D[Bb]2|(?<![A-Za-z0-9])[Dd][Bb]2)(?![0-9])");
        names.put("Postgres", "postgres(?:ql)?");
        names.put("Snowflake", "snowflake");
        names.put("Databricks", "databricks");
        names.put("BigQuery", "big\\s?query");
        names.put("SQLite", "sqlite");
        names.put("MariaDB", "maria\\s?db");
        names.put("MySQL", "mysql");
        names.put("Trino", "trino|presto");
        names.put("ClickHouse", "click\\s?house");
        names.put("Sybase", "sybase");
        names.put("Oracle", "oracle");
        names.put("SqlServer", "sql\\s?server");
        names.put("Redshift", "redshift");
        names.put("Athena", "athena");
        names.put("Spanner", "spanner");
        names.put("Hive", "hive\\b");
        names.put("MemSQL", "memsql|singlestore");
        names.put("MongoDB", "mongo");
        names.put("Elasticsearch", "elastic");
        Map<String, Pattern> out = new LinkedHashMap<>();
        names.forEach((db, p) -> out.put(db, Pattern.compile("(?i)\\b\\w*(" + p + ")\\w*")));
        return out;
    }

    /** The databases {@code code} (a line, or a class name) names. */
    static Set<String> named(String code) {
        Set<String> out = new LinkedHashSet<>();
        DATABASES.forEach((db, p) -> {
            if (p.matcher(code).find()) {
                out.add(db);
            }
        });
        return out;
    }

    private static final String MAIN = "core/src/main/java/com/legend/";

    private static final String TEST = "core/src/test/java/com/legend/";

    /** The dialect package's shared files that own a database their name does not say: the engine text's reader owns
     *  the language it reads, legend-engine's H2 (D2). */
    private static final Map<String, Set<String>> NAMED_BY_ROLE = Map.of(
            MAIN + "sql/dialect/RawSqlBoundary.java", Set.of("H2"));

    /** The input languages (D11): each file and the database whose SQL it reads. 2026-10-18, the dialect program's
     *  D11: Postgres's rules that were the translator's (SqlToPure's) are its own files' (operators, casts, functions,
     *  dates, jsonb, aggregates). */
    private static final Map<String, Set<String>> INPUT_LANGUAGES = Map.of(
            MAIN + "protocol/sql/PostgresDialect.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresDateTime.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresOperators.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresCasts.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresFunctions.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresDates.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresJsonb.java", Set.of("Postgres"),
            MAIN + "protocol/sql/PostgresAggregates.java", Set.of("Postgres"));

    private static final String IQ = MAIN + "protocol/sql/iq/";

    /** The vocabulary (D9): the model language's lexer, parser, protocol and model. {@code protocol/sql/}, the SQL a user
     *  writes, is not vocabulary: its owners are the input languages. */
    private static final List<String> VOCABULARY = List.of(MAIN + "lexer/", MAIN + "parser/", MAIN + "protocol/",
            MAIN + "model/",
            // 2026-10-16, the dialect program's Phase 6: the vocabulary's own tests, which read and write every
            // database's name and connection as the vocabulary does ({@code protocol/sql/}'s tests excepted, as above)
            TEST + "lexer/", TEST + "parser/", TEST + "protocol/", TEST + "model/");

    /** The warehouse's sources (D4): a host, whose engine is DuckDB. */
    private static final String WAREHOUSE = "warehouse/src/";

    /** The warehouse's engine: the database every one of its catalogs runs in. */
    private static final String WAREHOUSE_HOST = "DuckDB";

    /** Sources that name every database by design: the registry and the guards that count names. */
    private static final List<String> EVERY = List.of(
            MAIN + "database/Databases.java",
            // 2026-10-16, the dialect program's Phase 3: the engine-text registry, Databases' counterpart for
            // legend-engine's golden text (which engine-style dialect a declared type gets, and the engine's test
            // database), the census's SEAM; it stays in the root package, where the engine-style dialects are
            // quarantined (ArchitectureTest.engineStyleRendererIsQuarantinedToTheRootLayer)
            MAIN + "EngineText.java",
            // 2026-10-16, the dialect program's Phase 5: the connector registry, Databases' counterpart on the
            // execution side (a declared type to the connector that opens its sessions)
            MAIN + "exec/Connectors.java",
            // 2026-10-18, the dialect program's D11: the input-language registry, Databases' counterpart for the SQL a
            // user writes (an island's language to the dialect it is read in)
            MAIN + "protocol/sql/InputLanguages.java",
            // 2026-10-16, the dialect program's Phase 6: the tests' registry of databases, and its test (which compares
            // every database's backend by design, as ConnectorsTest compares their connectors)
            TEST + "testbackend/TestBackends.java",
            TEST + "TestBackendsTest.java",
            "core/src/test/java/com/legend/sql/dialect/",
            // 2026-10-16, the dialect program's Phase 5 (the audit of the branch): the connectors' table test, which
            // compares every database's sessions by design, as the dialect package's tests compare their SQL
            "core/src/test/java/com/legend/exec/ConnectorsTest.java",
            // 2026-10-29 (D65, the three routings): the raise recovery's test, which compares every connector's
            // declared recovery by design (MariaDB's and MySQL's the two statements, every other none) and measures
            // the seam's read on H2's own session variables
            "core/src/test/java/com/legend/exec/RaiseRecoveryTest.java",
            "core/src/test/java/com/legend/DialectBoundaryTest.java",
            // 2026-10-16, the dialect program's Phase 6: the layer guard, whose rules name the dialects they hold
            // apart (the engine-style dialects' quarantine), as the census's guards name the databases they count
            "core/src/test/java/com/legend/ArchitectureTest.java",
            "core/src/test/java/com/legend/DialectLeakRatchetTest.java",
            "core/src/test/java/com/legend/DialectOwnerExclusivityTest.java",
            "core/src/test/java/com/legend/DatabaseOwners.java",
            // 2026-10-18, the dialect program's Phase 7: the warehouse's registry of the databases a catalog attaches
            // (D4), Databases' counterpart there: one row and one exhaustive switch arm per database
            WAREHOUSE + "main/java/com/legend/warehouse/server/Attachment.java");


    /** The owner {@code rel} (a source's path from the repository root, {@code source} its text) is, or null when it
     *  is no owner and every database it names is a leak. */
    static Owner of(String rel, String source) {
        if (EVERY.stream().anyMatch(rel::startsWith)) {
            return new Owner(Kind.EVERY, DATABASES.keySet());
        }
        // 2026-10-17 (docs/MONGODB_PLAN_2026_10_16.md D12): the MongoDB connector's source set, beside the drivers as
        // the DuckDB-only sources are, owns its database as its dialect does; so does Postgres's (PostgresCells, its
        // jsonb cells behind the DriverCells seam, the plan path program's step 9, 2026-10-19; registered at main's
        // merge, 2026-10-22, when the absolute ban first read it)
        if (rel.startsWith(MAIN + "sql/dialect/") || rel.startsWith("core/src/main/duckdb/")
                || rel.startsWith("core/src/main/mongodb/") || rel.startsWith("core/src/main/postgres/")) {
            Set<String> own = ownAndParent(rel, source);
            own.addAll(NAMED_BY_ROLE.getOrDefault(rel, Set.of()));
            return new Owner(Kind.DIALECT, own);
        }
        if (rel.startsWith(WAREHOUSE)) {
            Set<String> own = new LinkedHashSet<>(Set.of(WAREHOUSE_HOST));
            own.addAll(named(className(rel)));
            return new Owner(Kind.HOST, own);
        }
        if (isConnector(rel)) {
            return new Owner(Kind.CONNECTOR, ownAndParent(rel, source));
        }
        if (BACKEND.matcher(rel).matches()) {
            return new Owner(Kind.BACKEND, ownAndParent(rel, source));
        }
        // a test, or (2026-10-18, the dialect program's Phase 7) a test database's launcher in the shared test support,
        // testing/ (EmbeddedPostgres and the rest)
        if ((rel.contains("/src/test/") || rel.startsWith("testing/src/main/java/"))
                && !named(className(rel)).isEmpty()) {
            return new Owner(Kind.NAMED_TEST, ownAndParent(rel, source));
        }
        if (INPUT_LANGUAGES.containsKey(rel)) {
            return new Owner(Kind.INPUT_LANGUAGE, INPUT_LANGUAGES.get(rel));
        }
        if (rel.startsWith(IQ)) {
            return new Owner(Kind.INPUT_LANGUAGE, Set.of("Sybase"));
        }
        if (rel.startsWith(MAIN + "protocol/sql/") || rel.startsWith(TEST + "protocol/sql/")) {
            return null;
        }
        if (VOCABULARY.stream().anyMatch(rel::startsWith)) {
            return new Owner(Kind.VOCABULARY, DATABASES.keySet());
        }
        return null;
    }

    private static final Pattern CONNECTOR = Pattern.compile(Pattern.quote(MAIN) + "exec/\\w+Connector\\.java");

    /** A test backend, {@code testbackend/<Db>Backend.java}, or the interface and bases they extend. */
    private static final Pattern BACKEND = Pattern.compile(Pattern.quote(TEST) + "testbackend/\\w+Backend\\.java");

    /** Whether {@code rel} (a source's path from the repository root) is a connector, {@code exec/<Db>Connector.java}
     *  or the interface they implement. */
    static boolean isConnector(String rel) {
        return CONNECTOR.matcher(rel).matches();
    }

    /** The databases {@code rel}'s class is named for, and its parent class's. */
    private static Set<String> ownAndParent(String rel, String source) {
        Set<String> own = new LinkedHashSet<>(named(className(rel)));
        // the file's own class's parent, read in code (2026-10-15, the audit of the guard: a comment or a nested
        // class that extends another dialect named that dialect's database for the whole file)
        Matcher parent = Pattern.compile("\\bclass\\s+" + Pattern.quote(className(rel)) + "\\s+extends\\s+(\\w+)")
                .matcher(String.join("\n", DialectLeakRatchetTest.lex(source).code()));
        if (parent.find()) {
            own.addAll(named(parent.group(1)));
        }
        return own;
    }

    private static String className(String rel) {
        String file = rel.substring(rel.lastIndexOf('/') + 1);
        return file.endsWith(".java") ? file.substring(0, file.length() - ".java".length()) : file;
    }
}
