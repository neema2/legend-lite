// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE OWNER GUARD (the dialect program, 2026-10-15; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md D9 to D11): the leak
 * ratchet keeps a database's name inside its owners, and this keeps each owner to its own. Without it,
 * {@code Postgres.java} could decide something for DuckDB and every count would still pass.
 * <ul>
 *   <li>A dialect, the DuckDB-only sources and an input language name only their own database and their family's
 *       ({@link DatabaseOwners}); a shared file in the dialect package names none. A crossing that is deliberate is
 *       registered below with its reason, shrink-only.</li>
 *   <li>A vocabulary owner (the lexer, the parser, the protocol, the model) carries a database's name and decides
 *       nothing by it: it names a database only as its own identifiers ({@code PSnowflakeSpec}, {@code parseTrinoSpec},
 *       the {@code DatabaseType} constants it declares), never as a specific {@code DatabaseType} constant, a dialect
 *       class, a driver, a JDBC URL or the driver's product name. Its dependency allowlists already keep it from
 *       reaching a dialect, a connection or {@code Databases} ({@code ArchitectureTest}: 6j, 7a, 7b, 7c,
 *       {@code compileSideLayersAreDialectBlind}).</li>
 * </ul>
 * Code only, as the ratchet reads it: a comment or a string never counts (a refusal's message names its database);
 * a connector's strings count too (its URLs and product name are its own database's).
 */
@Tag("census")
class DialectOwnerExclusivityTest {

    /** A deliberate crossing: an owner that names another database, on {@code lines} lines, and why. Shrink-only. */
    private record Crossing(String file, String database, int lines, String reason) {
    }

    private static final List<Crossing> CROSSINGS = List.of(
            // 2026-10-15 (registered as measured): the engine-H2 text spells ten functions legend-engine's H2 text
            // has no spelling for (MAP_EMPTY, TIME_BUCKET, ...) with DuckDB's own (DuckDbSpellings), because the corpus
            // reads this text on DuckDB too; refusing them broke 14 passing corpus tests (c7cd917; the plan's Phase 1b
            // step 2, the engine-H2 row). Whether the engine text should spell them at all is the engine-text work's
            // question, not this guard's.
            new Crossing("core/src/main/java/com/legend/sql/dialect/EngineStyleH2.java", "DuckDB", 9,
                    "the corpus reads the engine-H2 text on DuckDB: ten DuckDB-only functions spelled DuckDB's way"),
            // (2026-10-18, the dialect program's Phase 7: the warehouse's two crossings, its Postgres catalog tests
            // naming DuckDB, left this list: the warehouse is a host owner, its engine DuckDB (DatabaseOwners).)
            // 2026-10-18 (registered as measured, the dialect program's Phase 7, when the guards first read testing/):
            // MySQL's test server is reached through MariaDB's driver, the one driver both lanes load, as MySQL's
            // connector extends MariaDB's in the product
            new Crossing("testing/src/main/java/com/legend/testing/EmbeddedMySql.java", "MariaDB", 1,
                    "MySQL's test server is opened by MariaDB Connector/J, the one driver both lanes load"),
            // 2026-10-16 (registered as measured, the dialect program's Phase 6, the audit of the branch): two DuckDB
            // session tests whose subject is another database's text on that session: legend-engine's H2 SQL in an
            // executeInDb, translated for DuckDB (D2), and a connection helper whose overloads declare H2 and DuckDB
            new Crossing("core/src/test/java/com/legend/exec/ExecuteInDbDuckDbTest.java", "H2", 6,
                    "legend-engine's H2 text run on a DuckDB session, translated (D2)"));
            // (RawGridSchemaDuckDbTest's crossing gone with the test, 2026-10-30: the stamp it tested deleted, S3c)

    /** {@code DatabaseType.valueOf} and {@code .values()}: the vocabulary reading a name, deciding nothing. */
    private static final Pattern ANY_TYPE = Pattern.compile("\\bDatabaseType\\s*\\.\\s*(valueOf|values)\\b");

    /** {@code DatabaseType.valueOf("H2")}: one database by its name in a string, which {@link #ANY_TYPE} would pass
     *  (2026-10-15, the audit of the owner guard's hole c). Read on the line as written, comments blank. */
    private static final Pattern TYPE_BY_LITERAL =
            Pattern.compile("\\bDatabaseType\\s*\\.\\s*valueOf\\s*\\(\\s*\"");

    private static final List<String> MAIN_ROOTS = List.of("core/src/main/java", "core/src/main/duckdb");

    @Test
    void eachOwnerNamesOnlyItsOwnDatabase() throws IOException {
        Map<String, Integer> crossings = new TreeMap<>();
        // the tests too (2026-10-16, the dialect program's Phase 6): the test backends, and every test named for a
        // database, are owners there
        for (Path f : SourceFiles.under(MAIN_ROOTS.get(0), MAIN_ROOTS.get(1), "core/src/test/java", "pct/src/test/java",
                "spec/src/test/java", "parser-equivalence/src", "warehouse/src/main/java", "warehouse/src/test/java",
                "wasm/src/main/java/planner", "native/src/main/java", "testing/src/main/java")) {
            String rel = SourceFiles.path(f);
            if (!rel.endsWith(".java")) {
                continue;
            }
            String src = Files.readString(f);
            DatabaseOwners.Owner owner = DatabaseOwners.of(rel, src);
            if (owner == null || owner.kind() == DatabaseOwners.Kind.EVERY
                    || owner.kind() == DatabaseOwners.Kind.VOCABULARY) {
                continue;
            }
            DialectLeakRatchetTest.Lines lines = DialectLeakRatchetTest.lex(src);
            List<String> read = new ArrayList<>(lines.code());
            // a connector's strings too (2026-10-16, the dialect program's Phase 5, the audit of the branch): its URLs,
            // its product name and its messages are its own database's, and no census counts them (the leak ratchet
            // skips an owner, the boundary census skips a connector); a test backend's alike, and a test named for a
            // database (Phase 6, the audit of the branch: its JDBC URLs and its models are its own database's, and the
            // leak ratchet's jdbc-url count no longer reaches them)
            // the warehouse's too (a host, 2026-10-18, the dialect program's Phase 7): its messages and its fixtures
            // name its own engine, and another database only where that database owns the file
            if (owner.kind() == DatabaseOwners.Kind.CONNECTOR || owner.kind() == DatabaseOwners.Kind.BACKEND
                    || owner.kind() == DatabaseOwners.Kind.NAMED_TEST || owner.kind() == DatabaseOwners.Kind.HOST) {
                read.addAll(lines.strings());
            }
            for (String line : read) {
                for (String db : DatabaseOwners.named(line)) {
                    if (!owner.databases().contains(db)) {
                        crossings.merge(rel + "\t" + db, 1, Integer::sum);
                    }
                }
            }
        }
        List<String> failures = new ArrayList<>();
        Map<String, Crossing> pins = new TreeMap<>();
        CROSSINGS.forEach(c -> pins.put(c.file() + "\t" + c.database(), c));
        crossings.forEach((k, n) -> {
            Crossing pin = pins.get(k);
            if (pin == null || n > pin.lines()) {
                failures.add(k.replace("\t", " names ") + " on " + n + " lines"
                        + (pin == null ? "" : " (registered: " + pin.lines() + ")"));
            }
        });
        pins.forEach((k, pin) -> {
            if (crossings.getOrDefault(k, 0) < pin.lines()) {
                System.out.println("[owner-guard] lower this crossing in DialectOwnerExclusivityTest: "
                        + k.replace('\t', ' ') + " " + pin.lines() + " -> " + crossings.getOrDefault(k, 0));
            }
        });
        assertTrue(failures.isEmpty(), "an owner names a database not its own: move the decision into that database's"
                + " owner, or pass what differs in from it (a crossing is registered here only with a dated reason): "
                + failures);
    }

    /** The warehouse's registry of the databases a catalog attaches ({@code Attachment}, an EVERY owner, which neither
     *  census reads) names an attached database only in its rows and in its switches' one-line arms (DuckDB, the
     *  warehouse's host, anywhere, as in every warehouse source), as DialectBoundaryTest pins
     *  {@code Databases}' cases: a decision for one database written anywhere else in it belongs in that database's own
     *  class (2026-10-18, the dialect program's Phase 7, the review of its branch). */
    @Test
    void theWarehouseRegistryNamesADatabaseOnlyInItsRowsAndArms() throws IOException {
        String rel = "warehouse/src/main/java/com/legend/warehouse/server/Attachment.java";
        DialectLeakRatchetTest.Lines lines = DialectLeakRatchetTest.lex(Files.readString(SourceFiles.file(rel)));
        Pattern row = Pattern.compile("^\\s*[A-Z][A-Z0-9_]*\\s*\\(");
        Pattern arm = Pattern.compile("^\\s*case\\s+[A-Z][A-Z0-9_]*\\s*->[^;{]*;\\s*$");
        List<String> failures = new ArrayList<>();
        // the warehouse's host: what a warehouse source not named for a database may name
        Set<String> host = DatabaseOwners.of("warehouse/src/main/java/com/legend/warehouse/server/Catalogs.java", "")
                .databases();
        int rows = 0;
        for (int i = 0; i < lines.code().size(); i++) {
            String code = lines.code().get(i);
            Set<String> named = new java.util.HashSet<>(DatabaseOwners.named(code));
            named.removeAll(host);
            if (named.isEmpty()) {
                continue;
            }
            if (row.matcher(code).find()) {
                rows++;
            } else if (!arm.matcher(code).find()) {
                failures.add(rel + ":" + (i + 1) + " " + code.strip());
            }
        }
        assertTrue(rows > 0, "the registry's rows were not found: " + rel);
        assertTrue(failures.isEmpty(), "the warehouse's registry names a database outside a row or a one-line arm: move"
                + " the decision into that database's class (Postgres, PostgresUrl) and call it from the arm: " + failures);
    }

    /** A question about which dialect a value is, by its class: {@code instanceof} or {@code .class} of a dialect
     *  class, or its simple name compared. A dialect answers for itself; asking which one it is decides by a name
     *  this guard cannot read when the class's name holds no database's (2026-10-15, the audit of the owner guard's
     *  hole b: {@code instanceof EngineStyleComposite}, {@code getSimpleName().equals("Postgres")}). */
    private static final Pattern BY_SIMPLE_NAME = Pattern.compile(
            "getSimpleName\\s*\\(\\s*\\)\\s*\\.\\s*(equals|equalsIgnoreCase|startsWith|endsWith|contains)\\b"
                    + "|\\b(equals|equalsIgnoreCase)\\s*\\([^;]*getSimpleName|\\bswitch\\s*\\([^)]*getSimpleName");

    @Test
    void noDialectIsDecidedByItsClass() throws IOException {
        Map<String, String> sources = new TreeMap<>();
        for (Path f : SourceFiles.under(MAIN_ROOTS.get(0), MAIN_ROOTS.subList(1, MAIN_ROOTS.size())
                .toArray(String[]::new))) {
            String rel = SourceFiles.path(f);
            if (rel.endsWith(".java")) {
                sources.put(rel, String.join("\n", DialectLeakRatchetTest.lex(Files.readString(f)).code()));
            }
        }
        // the dialect classes: SqlDialect's implementations in sql/dialect, by their extends chains
        Set<String> dialects = new java.util.HashSet<>(Set.of("SqlDialect", "AnsiSqlRenderer"));
        Pattern declared = Pattern.compile("\\bclass\\s+(\\w+)\\s*(?:extends\\s+(\\w+))?([^{]*)\\{");
        Pattern implementsDialect = Pattern.compile("\\bimplements\\b.*\\bSqlDialect\\b", Pattern.DOTALL);
        for (boolean grew = true; grew; ) {
            grew = false;
            for (var e : sources.entrySet()) {
                if (!e.getKey().contains("/sql/dialect/")) {
                    continue;
                }
                var m = declared.matcher(e.getValue());
                while (m.find()) {
                    if ((m.group(2) != null && dialects.contains(m.group(2))
                            || implementsDialect.matcher(m.group(3)).find()) && dialects.add(m.group(1))) {
                        grew = true;
                    }
                }
            }
        }
        dialects.removeAll(Set.of("SqlDialect", "AnsiSqlRenderer"));
        Pattern byClass = Pattern.compile("\\binstanceof\\s+(?:[\\w.]+\\.)?(" + String.join("|", dialects)
                + ")\\b|\\b(" + String.join("|", dialects) + ")\\s*\\.\\s*class\\b");
        List<String> failures = new ArrayList<>();
        sources.forEach((rel, code) -> {
            String[] lines = code.split("\n", -1);
            for (int i = 0; i < lines.length; i++) {
                if (byClass.matcher(lines[i]).find() || BY_SIMPLE_NAME.matcher(lines[i]).find()) {
                    failures.add(rel + ":" + (i + 1));
                }
            }
        });
        assertTrue(dialects.containsAll(Set.of("DuckDb", "MySql", "H2Modern", "EngineStyleComposite")),
                "the dialect classes were not all found by their extends chains: " + dialects);
        assertTrue(failures.isEmpty(), "a dialect is decided by asking which class it is: let the dialect answer"
                + " (a method on SqlDialect it overrides), " + failures);
    }

    /** Line {@code i} as written, its comments blank: the lexer keeps the code and the strings column for column. */
    private static String written(DialectLeakRatchetTest.Lines lines, int i) {
        String code = lines.code().get(i);
        String strings = lines.strings().get(i);
        StringBuilder out = new StringBuilder(code.length());
        for (int k = 0; k < code.length(); k++) {
            char s = k < strings.length() ? strings.charAt(k) : ' ';
            out.append(s != ' ' ? s : code.charAt(k));
        }
        return out.toString();
    }

    /** A literal name read as a database type is a decision by that name; a name read from the model is not. */
    @Test
    void aDatabaseTypeByALiteralIsSeen() {
        var picked = DialectLeakRatchetTest.lex("t = DatabaseType.valueOf(\"H2\"); // DatabaseType.valueOf(\"X\")");
        assertTrue(TYPE_BY_LITERAL.matcher(written(picked, 0)).find());
        var read = DialectLeakRatchetTest.lex("t = DatabaseType.valueOf(r.databaseType()); /* valueOf(\"H2\") */");
        assertTrue(!TYPE_BY_LITERAL.matcher(written(read, 0)).find());
        var commented = DialectLeakRatchetTest.lex("// DatabaseType.valueOf(\"H2\")");
        assertTrue(!TYPE_BY_LITERAL.matcher(written(commented, 0)).find());
    }

    @Test
    void aVocabularyOwnerCarriesADatabasesNameAndDecidesNothingByIt() throws IOException {
        List<String> failures = new ArrayList<>();
        for (Path f : SourceFiles.under(MAIN_ROOTS.get(0))) {
            String rel = SourceFiles.path(f);
            if (!rel.endsWith(".java")) {
                continue;
            }
            String src = Files.readString(f);
            DatabaseOwners.Owner owner = DatabaseOwners.of(rel, src);
            if (owner == null || owner.kind() != DatabaseOwners.Kind.VOCABULARY) {
                continue;
            }
            DialectLeakRatchetTest.Lines lines = DialectLeakRatchetTest.lex(src);
            for (int i = 0; i < lines.code().size(); i++) {
                String code = ANY_TYPE.matcher(lines.code().get(i)).replaceAll("");
                for (var kind : DialectLeakRatchetTest.IN_CODE.entrySet()) {
                    if (!kind.getKey().equals("product-name") && kind.getValue().matcher(code).find()) {
                        failures.add(rel + ":" + (i + 1) + " " + kind.getKey());
                    }
                }
                if (DialectLeakRatchetTest.JDBC_URL.matcher(lines.strings().get(i)).find()) {
                    failures.add(rel + ":" + (i + 1) + " jdbc-url");
                }
                if (TYPE_BY_LITERAL.matcher(written(lines, i)).find()) {
                    failures.add(rel + ":" + (i + 1) + " database-type by a literal");
                }
            }
        }
        assertTrue(failures.isEmpty(), "the model language carries a database's name and decides nothing by it (D9):"
                + " a specific DatabaseType, a dialect, a driver, a JDBC URL or a product-name call belongs to the"
                + " database's owner: " + failures);
    }

    /** The register reads the owners the plan names, each with its own database. */
    @Test
    void theRegisterNamesEachOwnersDatabase() {
        String dialect = "core/src/main/java/com/legend/sql/dialect/";
        assertEquals(Set.of("DuckDB"), DatabaseOwners.of(dialect + "DuckDb.java", "class DuckDb extends AnsiSqlRenderer")
                .databases());
        assertEquals(Set.of("MySQL", "MariaDB"),
                DatabaseOwners.of(dialect + "MySql.java", "class MySql extends MariaDb").databases());
        assertEquals(Set.of("DB2", "H2"),
                DatabaseOwners.of(dialect + "EngineStyleDB2.java", "class EngineStyleDB2 extends EngineStyleH2")
                        .databases());
        assertEquals(Set.of(), DatabaseOwners.of(dialect + "Stars.java", "final class Stars {").databases());
        // only the file's own class's parent, in code: not a comment's, not a nested class's
        assertEquals(Set.of(), DatabaseOwners.of(dialect + "Stars.java",
                "/** like class Stars extends Postgres */ final class Stars { static class In extends DuckDb {} }")
                .databases());
        assertEquals(Set.of("H2"), DatabaseOwners.of(dialect + "RawSqlBoundary.java", "").databases());
        assertEquals(DatabaseOwners.Kind.VOCABULARY,
                DatabaseOwners.of("core/src/main/java/com/legend/parser/section/ConnectionSectionGrammar.java", "")
                        .kind());
        assertEquals(Set.of("Postgres"),
                DatabaseOwners.of("core/src/main/java/com/legend/protocol/sql/PostgresDialect.java", "").databases());
        assertEquals(null, DatabaseOwners.of("core/src/main/java/com/legend/protocol/sql/SqlToPure.java", ""));
        assertEquals(null, DatabaseOwners.of("core/src/main/java/com/legend/exec/Sessions.java", ""));
        // a connector (2026-10-16, the dialect program's Phase 5): its own database and its parent's; the interface
        // none; the registry every one
        String exec = "core/src/main/java/com/legend/exec/";
        assertEquals(Set.of("H2"), DatabaseOwners.of(exec + "H2Connector.java",
                "final class H2Connector implements DatabaseConnector").databases());
        assertEquals(Set.of("MySQL", "MariaDB"), DatabaseOwners.of(exec + "MySqlConnector.java",
                "final class MySqlConnector extends MariaDbConnector").databases());
        assertEquals(Set.of(), DatabaseOwners.of(exec + "DatabaseConnector.java", "interface DatabaseConnector")
                .databases());
        assertEquals(DatabaseOwners.Kind.EVERY, DatabaseOwners.of(exec + "Connectors.java", "").kind());
        // the warehouse (2026-10-18, the dialect program's Phase 7): a host, its engine DuckDB; a file named for an
        // attached database names that one too; its registry of attached databases every one
        String warehouse = "warehouse/src/main/java/com/legend/warehouse/server/";
        assertEquals(Set.of("DuckDB"), DatabaseOwners.of(warehouse + "WarehouseServer.java", "").databases());
        assertEquals(Set.of("DuckDB", "Postgres"), DatabaseOwners.of(warehouse + "PostgresUrl.java", "").databases());
        assertEquals(Set.of("DuckDB", "Postgres"), DatabaseOwners.of(
                "warehouse/src/test/java/com/legend/warehouse/WarehousePostgresLiveTest.java", "").databases());
        assertEquals(DatabaseOwners.Kind.HOST, DatabaseOwners.of(
                "warehouse/src/test/java/com/legend/warehouse/TestServer.java", "").kind());
        assertEquals(DatabaseOwners.Kind.EVERY, DatabaseOwners.of(warehouse + "Attachment.java", "").kind());
    }

    /** Every name the ratchet counts as a database is one the register knows: the two read the same names. */
    @Test
    void theRegisterSeesEveryNameTheRatchetCounts() {
        Pattern products = DialectLeakRatchetTest.IN_CODE.get("product-name");
        for (String named : List.of("isH2", "EngineStyleDB2", "DuckDbCells", "postgresName", "PSnowflakeSpec",
                "parseTrinoSpec", "PBigQuerySpec", "SybaseIQ", "PMongoDbConnection", "isSqlServer", "MariaDb",
                "MySql", "ClickHouse", "presto", "oracleSpec", "redshiftSpec", "athenaEndpoint", "PSpannerSpec",
                "MemSql", "Elasticsearch7", "Hive")) {
            assertTrue(products.matcher(named).find(), named);
            assertEquals(1, DatabaseOwners.named(named).size(), named + ": " + DatabaseOwners.named(named));
        }
    }
}
