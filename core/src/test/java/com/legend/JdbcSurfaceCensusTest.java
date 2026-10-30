// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE JDBC SURFACE CENSUS (ADVERSARIAL_TENET_AUDIT_2026_08_18 fix plan
 * items 6+7): a WALK over every module and BOTH source roots, with an
 * explicit exempt register — the eval ledger's hard-coded paths could
 * not see code that moved or was born outside them (10 of the audit's
 * 15 probes landed green that way; the {@code src/main -> src/test}
 * harness move silently walked 7,564 lines out of seven guards).
 *
 * <p>Three assertions:
 * <ul>
 *   <li><b>Coverage floor</b> — the walk itself reports how many files
 *   it scanned, and that number must not drop: scope rot (a renamed
 *   root, a new module the walk misses) fails loudly instead of
 *   silently shrinking the guarded surface.</li>
 *   <li><b>Production register</b> — the EXACT set of {@code src/main}
 *   files that touch JDBC or a driver API ({@code java.sql},
 *   {@code javax.sql}, every driver's packages, statement-method
 *   spellings, in comment-stripped source). A new file fails in BOTH
 *   directions: growth is a conscious registration, shrink deletes the
 *   row.</li>
 *   <li><b>The acts</b> (2026-10-30, the java.sql revamp's step 3, D69:
 *   the user's "pin the rest by class with different functionality /
 *   what they are allowed to do") — each registered file declares WHAT
 *   it may do with JDBC ({@link Act}: opens, checks, sends, loads,
 *   decodes, transacts, closes), and the census measures its source
 *   against the row: an act performed and not declared is drift, an act
 *   declared and no longer performed is a stale row. The register is
 *   ONE list for two guards: this census and the funnel
 *   ({@code ArchitectureTest.javaSqlIsFunnelledToTheCharteredSeam}),
 *   which lets no other product class reach JDBC or a driver in
 *   bytecode.</li>
 *   <li>The TEST register (every test file reaching JDBC, by name) was
 *   deleted on 2026-09-29 with execution plan W0.5: tests execute queries
 *   by nature, production JDBC is already funnelled to the chartered
 *   seams in bytecode (ArchitectureTest), and the register only made each
 *   new test file ask permission.</li>
 * </ul>
 *
 * <p>KNOWN LIMIT (the audit's own §3 lesson, recorded honestly): this
 * census is FILE-grained, and an act is a spelling. New evaluation code
 * added to an already-registered file within its declared acts is
 * invisible here — that residue is what the eval ledger's size/name
 * pins cover, and residue DELETION (the relation-typed fetchDb leg) is
 * the durable fix, not finer guards.
 */
@Tag("census")
class JdbcSurfaceCensusTest {

    /** PRECISION (2026-08-21): the bare word {@code ResultSet} is gone —
     * it over-matched the PURE-LANGUAGE class
     * {@code meta::relational::metamodel::execute::ResultSet} spelled in
     * native-signature STRINGS (builtin/Pure.java sat on the register
     * with zero JDBC). Genuine JDBC cannot hide from the tightened
     * pattern: the java.sql TYPE spelling appears in the import or FQN,
     * and connection-passed usage spells a statement-method call —
     * {@code createStatement} added for that same completeness. */
    private static final Pattern JDBC = Pattern.compile(
            // every driver's packages (2026-10-13, the dialect program's Phase 0: DuckDB, H2 and SQLite only before)
            "java\\.sql|javax\\.sql|org\\.duckdb|org\\.h2\\.|org\\.sqlite|org\\.postgresql|org\\.mariadb|com\\.mysql"
            + "|com\\.clickhouse|io\\.trino|net\\.snowflake|com\\.databricks|com\\.simba|com\\.google\\.cloud\\.bigquery"
            // MongoDB's driver (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12): a client library, counted as a driver
            + "|com\\.mongodb|org\\.bson"
            + "|prepareStatement|createStatement|prepareCall|executeQuery");

    /** Every module source root the census walks (repo-relative). A
     * NEW MODULE must be added here — the coverage floor cannot see a
     * root it was never told about, so module creation reviews this
     * file. */
    private static final List<String> ROOTS = List.of(
            "core/src", "spec/src", "pct/src", "parser-equivalence/src");

    /** Coverage floor: files scanned on 2026-08-18. Shrink needs a
     * written justification (files deleted); growth is free. */
    // 779 -> 778: HostEval DELETED (Phase 1 batch 2; GridReads was
    // a rename, net zero)
    private static final int FILE_FLOOR = 778;

    /** WHAT A REGISTERED FILE MAY DO with JDBC or a driver (2026-10-30, the java.sql revamp's step 3, D69). Each act is
     *  the spellings that perform it, matched in comment-stripped source. A row declares its file's acts, and
     *  {@link #eachRegisteredFileDoesItsActsAndNoOther} measures them. A row with no act NAMES a JDBC type and
     *  performs nothing: a key, an exception, a type number, a contract. The spellings are the measure, so a file that
     *  closes a non-JDBC resource or loads a non-JDBC thing declares the act too -- the reviewer reads the row. The
     *  spellings are the named JDBC and driver calls (widened by the D69 audit: a call, a getter, a metadata read a
     *  drifting file could use); a JDBC act in a spelling not here would read as not performed -- a limit, as the
     *  class javadoc says. */
    enum Act {
        /** Opens a connection (a driver manager's, a data source's, a driver's) or a driver's client, or reaches
         *  the driver's own connection beneath a JDBC one (the Wrapper API). */
        OPENS("\\.getConnection\\(|\\.connect\\(|MongoClients\\.create\\(|\\.isWrapperFor\\(|\\.unwrap\\(\\w+\\.class\\)"),
        /** Reads the driver's metadata: which database a session is on, its version, its driver, its tables,
         *  columns, schemas, catalogs or URL. */
        CHECKS("\\.getDatabaseProduct\\w*\\(|\\.getDatabaseM\\w+Version\\(|\\.getDriver\\w*\\("
                + "|\\.get(Tables|Columns|Schemas|Catalogs|URL)\\("),
        /** Sends a statement (created, prepared or a call; executed in any form), a pipeline or a command. */
        SENDS("\\.createStatement\\(|\\.prepareStatement\\(|\\.prepareCall\\("
                + "|\\.execute(Query|Update|Batch|LargeUpdate|LargeBatch)?\\(|\\.aggregate\\(|\\.runCommand\\("),
        /** Takes part in a bulk load: declares it, finds the engine's loader, performs it (an appender), hands rows
         *  to it -- or refuses it by name (the seam's method spelled: a refusal is this act's spelling too, so the
         *  row says which). */
        LOADS("Appender|(?<!ServiceLoader\\.)\\bload\\("),
        /** Reads a result: its columns as the driver reports them, its cells by any getter, a null's mark, a
         *  column found by name, a driver's own cell type. */
        DECODES("\\bResultSet\\b|\\.getObject\\(|\\.getString\\(|\\.getArray\\("
                + "|\\.get(Int|Long|Short|Byte|Float|Double|Boolean|BigDecimal|Timestamp|Date|Time|Bytes|NString)\\("
                + "|\\.wasNull\\(|\\.findColumn\\(|\\.getColumnCount\\(|\\.getColumnLabel\\(|\\.getColumnType\\("
                + "|\\.getColumnTypeName\\(|\\.getPrecision\\(|\\.getScale\\(|PGobject|JsonNode|BsonValue|\\bDocument\\b"),
        /** Brackets a transaction: autocommit, commit, rollback, the isolation level, savepoints. */
        TRANSACTS("\\.commit\\(|\\.rollback\\(|\\.setAutoCommit\\(|\\.getAutoCommit\\("
                + "|\\.(set|get)TransactionIsolation\\(|\\.(set|release)Savepoint\\("),
        /** Closes a connection or a client, or asks whether one is closed or still valid. A try-with-resources
         *  close spells nothing (every registered owner closes explicitly today; a new one that does not would read
         *  as not closing -- the reviewer reads the row). */
        CLOSES("\\.close\\(\\)|\\.isClosed\\(\\)|\\.isValid\\(");

        final Pattern spelled;

        Act(String spellings) {
            this.spelled = Pattern.compile(spellings);
        }
    }

    /** A registered file: what it may do with JDBC ({@link #acts}, none for a file that only names a type) and why. */
    record Row(String file, Set<Act> acts, String why) {
        /** The top-level class the file declares, as the product's jars name it. */
        String className() {
            // core/src/main/<root>/com/legend/exec/X.java -> com.legend.exec.X
            String s = file.substring(file.indexOf("/com/") + 1, file.length() - ".java".length());
            return s.replace('/', '.');
        }
    }

    private static Row row(String file, String why, Act first, Act... rest) {
        return new Row(file, EnumSet.of(first, rest), why);
    }

    private static Row names(String file, String why) {
        return new Row(file, EnumSet.noneOf(Act.class), why);
    }

    /** THE REGISTER: the EXACT set of {@code src/main} files that touch JDBC or a driver API, each with the acts it
     *  may perform and the reason it may (by role, the D68 audit's classification: the OPENERS, the SEAM, the
     *  DECODERS, one CARRIER). ONE list for two guards: this census (the files, the acts) and
     *  {@code ArchitectureTest.javaSqlIsFunnelledToTheCharteredSeam} (every other product class is kept from JDBC
     *  and the drivers in bytecode; until 2026-10-30, D69, the funnel exempted four packages whole). A row moves only
     *  with a dated justification (AGENTS.md: guard files are load-bearing). The tenet's sentence "in core's main
     *  sources the seam and its bulk loader are the only JDBC senders" is this table's SENDS column: the seam
     *  ({@code JdbcStatements}, {@code MongoDbStatements}) and DuckDB's appender load.
     *
     *  <p>Departures, newest first (the files that left this register, each the day its JDBC went): 2026-10-30 (D70,
     *  the seam's step A) {@code PlanSessions} (its handle stores keep sessions, its anonymous sessions and its
     *  liveness check the seam's), {@code SystemDatabase} (sessions opened and closed through the seam),
     *  {@code SessionState} (no longer keyed by the driver's connection: the owning session's own) and
     *  {@code server/ConnectionResolver} (its lease a session: the server package holds no java.sql); 2026-10-30 (D69)
     *  {@code SqlReplayReferee} (the referee's SPI declared {@code SQLException} on its rows contract; the harness
     *  tells a failure of its own); 2026-10-30 (D68) {@code PureTestRunner}, {@code ServiceTestRunner},
     *  {@code TestObserver} (sessions from the opener; core's {@code test} package holds no java.sql); 2026-10-30
     *  (D67) {@code Execution} (a session asked: its version, its check, its plan sessions), {@code PlanRunner} (a
     *  null's JDBC type number is the seam's, {@code JdbcStatements.nullType}), the six server connectors (an unused
     *  import each, D65's leftover), {@code QueryService} (deleted); 2026-10-29 (D65) {@code MariaDbConnector} (its
     *  raise recovery is two statements it declares as data, read by the seam); 2026-10-29 (the doors, D64)
     *  {@code PrepTrace} (its timed prepare/execute arms deleted), {@code SqlTypeCensus} (its wire arm deleted),
     *  {@code TestDataGenerator} and {@code TestDataGenerationNatives} (every statement of a run the plan runner's);
     *  2026-10-29 (track B) {@code SetupRunner} (then deleted with the doors), {@code DynamicPivot}, {@code GridProbe},
     *  {@code ColumnProbe} (the session seam: {@code Statements.columns}), {@code VerdictBatch} and {@code BodyDriver}
     *  (sessions); 2026-10-27 {@code Executor} (deleted: its sends plan steps); 2026-10-08 {@code CsvSeed},
     *  {@code Ddl}, {@code RowLoad} (to {@code setup/}, no JDBC); 2026-10-04 {@code Compiler} (C2a: the planner
     *  touches no database); 2026-08-21 {@code PureAsserts}, {@code TdsCompare} (the wire temporal carrier is
     *  PureDateLiteral, so the comparison layer's java.sql value arms went), {@code DbMetaData} (to
     *  {@code CatalogGrids}, SQL text). */
    static final List<Row> REGISTER = List.of(
            // THE OPENERS: they open sessions and read a session's state; none sends a query of the user's or computes
            // a value
            row("core/src/main/java/com/legend/exec/Sessions.java",
                    "the execution side's ONE session owner (C3b, 2026-10-04; it absorbed JdbcMetadata, the driver's"
                    + " one metadata read kept out of Compiler so the plan surface needs no java.sql): a declared"
                    + " connection's URL opened (DriverManager) or an in-memory database through its connector; the"
                    + " version a handed session is checked by; the transaction bracket (autocommit off, commit,"
                    + " rollback, the session's memo cleared first: D66); the ONE WAY IN since D70 (2026-10-30): a caller's"
                    + " connection enters as an owned session (closed with it) or a borrowed one, a session is shared"
                    + " as a view, and the handle stores ask it whether a kept session is still open. It executes"
                    + " nothing",
                    Act.OPENS, Act.CHECKS, Act.TRANSACTS, Act.CLOSES),
            row("core/src/main/java/com/legend/exec/Connectors.java",
                    "which database a session is on, the driver's product name (the dialect program's Phase 5,"
                    + " 2026-10-16); the connectors found by ServiceLoader",
                    Act.CHECKS),
            names("core/src/main/java/com/legend/exec/DatabaseConnector.java",
                    "the connector contract: the openers' shapes (a private instance, a named memory) return the"
                    + " driver's Connection, the raise recovery is two statements declared as data (D65); each"
                    + " connector opens"),
            row("core/src/main/java/com/legend/exec/DuckDbConnector.java",
                    "the in-process database's private instance (DriverManager)", Act.OPENS),
            row("core/src/main/java/com/legend/exec/H2Connector.java",
                    "the private instance and the named in-memory databases (DriverManager)", Act.OPENS),
            row("core/src/main/java/com/legend/exec/SqliteConnector.java",
                    "the private instance (DriverManager)", Act.OPENS),
            row("core/src/main/mongodb/com/legend/exec/MongoDbConnector.java",
                    "a client from the declared settings, closed with the session (2026-10-17, D12); its session's"
                    + " connection() refuses by name: a MongoDB session has no JDBC connection",
                    Act.OPENS, Act.CLOSES),
            // THE SEAM: the statements a session runs are sent here and nowhere else in core's main sources
            // (ArchitectureTest.theRunnerIsTheOneCallerOfTheSessionsStatements pins the one caller)
            row("core/src/main/java/com/legend/exec/JdbcStatements.java",
                    "the one JDBC sender (2026-10-17, D12, the Statements seam): prepare, bind, execute; the one"
                    + " getString of the plan path (carriage, C1.2) and a result's columns as the driver reports"
                    + " them; the bulk load handed to the engine's loader; the transaction bracket; a MariaDB raise"
                    + " read back from the two statements its connector declares (D65)",
                    Act.SENDS, Act.LOADS, Act.DECODES, Act.TRANSACTS),
            row("core/src/main/mongodb/com/legend/exec/MongoDbStatements.java",
                    "the pipeline through the driver's cursor and a command run, each row's cells by the plan's"
                    + " slots (D12); its load REFUSES by name (NotImplementedException: the seam's method spelled,"
                    + " no document inserted -- the D69 audit's correction of this row)",
                    Act.SENDS, Act.LOADS, Act.DECODES),
            row("core/src/main/duckdb/com/legend/exec/DuckDbAppenderLoad.java",
                    "DuckDB's Appender behind core's BulkLoad seam (2026-09-23; its own target beside the drivers,"
                    + " core compiles against none): the driver's own connection reached beneath the JDBC one"
                    + " (unwrap), every cell staged as TEXT and one INSERT ... SELECT casts, so the DATABASE types"
                    + " each value -- the load's two statements are its own",
                    Act.OPENS, Act.SENDS, Act.LOADS),
            row("core/src/main/java/com/legend/exec/BulkLoad.java",
                    "the bulk load contract: accepts a connection, loads a setup step's rows; no SQL of its own",
                    Act.LOADS),
            row("core/src/main/java/com/legend/exec/BulkLoads.java",
                    "finds the engine's loader (accepts) and hands it the rows", Act.LOADS),
            names("core/src/main/java/com/legend/exec/RaisedErrors.java",
                    "rethrows a SQLException with the raised-message envelope removed (B7, the provenance seam):"
                    + " constructs the exception, opens no connection, sends nothing"),
            // THE DECODERS: carriage only, a cell's text or a column's report; nothing computed
            row("core/src/main/java/com/legend/exec/JdbcDecode.java",
                    "a row's cells read from the driver's ResultSet by the plan's columns, one carrier conversion"
                    + " each (the plan path program's step 7, 2026-10-18)",
                    Act.DECODES),
            row("core/src/main/duckdb/com/legend/exec/DuckDbCells.java",
                    "DuckDB's own JSON cell type recognised beside its driver (C3c, 2026-10-04; DriverCells): the"
                    + " node's text",
                    Act.DECODES),
            row("core/src/main/postgres/com/legend/exec/PostgresCells.java",
                    "Postgres's json and jsonb cells, a PGobject's text (the plan path program's step 9, 2026-10-19;"
                    + " DriverCells)",
                    Act.DECODES),
            row("core/src/main/mongodb/com/legend/exec/MongoDbDecode.java",
                    "a document's cells decoded by the plan's slots, beside its driver (D12)", Act.DECODES),
            names("core/src/main/java/com/legend/exec/WireTypes.java",
                    "the wire-decided kinds (leg 3.3): a table keyed by java.sql.Types numbers"));

    /** Every registered class, as the product's jars name it: the funnel's exemption. */
    static Set<String> registeredClasses() {
        Set<String> classes = new TreeSet<>();
        for (Row r : REGISTER) {
            classes.add(r.className());
        }
        return classes;
    }

    @Test
    void jdbcSurfaceIsRegistered() throws IOException {
        Map<String, Path> files = walk();
        Set<String> mainHits = new TreeSet<>();
        for (Map.Entry<String, Path> e : files.entrySet()) {
            if (e.getKey().contains("/main/") && JDBC.matcher(stripped(e.getValue())).find()) {
                mainHits.add(e.getKey());
            }
        }
        Set<String> register = new TreeSet<>();
        for (Row r : REGISTER) {
            assertTrue(register.add(r.file()), "JDBC census register: " + r.file() + " has two rows");
        }
        StringBuilder drift = new StringBuilder();
        diff(drift, "src/main", mainHits, register);
        assertTrue(drift.length() == 0,
                "JDBC surface census drift (tenet #1 — Java orchestrates,"
                + " the DATABASE executes; audit 2026-08-18 items 6+7):"
                + drift);
    }

    /** Each registered file spells exactly the acts its row declares (D69). */
    @Test
    void eachRegisteredFileDoesItsActsAndNoOther() throws IOException {
        Map<String, Path> files = walk();
        StringBuilder drift = new StringBuilder();
        for (Row r : REGISTER) {
            Path p = files.get(r.file());
            if (p == null) {
                drift.append("\n  ").append(r.file()).append(" is not among the census's sources");
                continue;
            }
            String src = stripped(p);
            Set<Act> spelled = EnumSet.noneOf(Act.class);
            for (Act a : Act.values()) {
                if (a.spelled.matcher(src).find()) {
                    spelled.add(a);
                }
            }
            if (!spelled.equals(r.acts())) {
                drift.append("\n  ").append(r.file()).append(": its row declares ").append(r.acts())
                        .append(", its source spells ").append(spelled)
                        .append(" — an act it performs and does not declare is drift (move it to a seam that may,")
                        .append(" or declare it with a dated justification); one it declares and no longer performs")
                        .append(" is a stale row. The row: ").append(r.why());
            }
        }
        assertTrue(drift.length() == 0,
                "JDBC acts drift (the java.sql revamp's step 3, D69: each registered class does what its row says):"
                + drift);
    }

    /** Every walked source file by its repo-relative path, the coverage floor checked. */
    private static Map<String, Path> walk() {
        Map<String, Path> files = new TreeMap<>();
        for (String root : ROOTS) {
            if (SourceFiles.under(root).isEmpty()) {
                throw new IllegalStateException("JdbcSurfaceCensusTest root " + root + " is not among its inputs: declare it (Bazel workplan P3-14: a guard's declared source list)");
            }
            try (Stream<Path> s = SourceFiles.under(root).stream()) {
                s.filter(p -> p.toString().endsWith(".java"))
                        .forEach(p -> files.put(SourceFiles.path(p), p));
            }
        }
        assertTrue(files.size() >= FILE_FLOOR,
                "JDBC census coverage DROPPED: scanned " + files.size()
                + " files, floor " + FILE_FLOOR + " — a source root moved"
                + " or the walk rotted; re-point the census before"
                + " trusting any guard that scopes by path");
        return files;
    }

    /** The file's source with its comments removed: what the census and the acts read. */
    private static String stripped(Path p) throws IOException {
        return Files.readString(p)
                .replaceAll("//.*", "")
                .replaceAll("(?s)/\\*.*?\\*/", "");
    }

    private static void diff(StringBuilder drift, String where,
            Set<String> actual, Set<String> register) {
        for (String f : actual) {
            if (!register.contains(f)) {
                drift.append("\n  NEW JDBC surface in ").append(where)
                        .append(": ").append(f)
                        .append(" — register it CONSCIOUSLY (a row with its acts and")
                        .append(" the tenet argument) or route through an")
                        .append(" existing seam");
            }
        }
        for (String f : register) {
            if (!actual.contains(f)) {
                drift.append("\n  ").append(f)
                        .append(" no longer touches JDBC — shrink the ")
                        .append(where).append(" register");
            }
        }
    }
}
