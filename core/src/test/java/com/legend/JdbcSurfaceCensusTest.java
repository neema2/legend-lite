// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
 * <p>Two assertions:
 * <ul>
 *   <li><b>Coverage floor</b> — the walk itself reports how many files
 *   it scanned, and that number must not drop: scope rot (a renamed
 *   root, a new module the walk misses) fails loudly instead of
 *   silently shrinking the guarded surface.</li>
 *   <li><b>Production register</b> — the EXACT set of {@code src/main}
 *   files that touch JDBC or a driver API ({@code java.sql},
 *   {@code javax.sql}, {@code org.duckdb}, {@code org.h2},
 *   {@code org.sqlite}, statement-method spellings, in
 *   comment-stripped source). A new file fails in BOTH directions:
 *   growth is a conscious registration, shrink deletes the row.</li>
 *   <li>The TEST register (every test file reaching JDBC, by name) was
 *   deleted on 2026-09-29 with execution plan W0.5: tests execute queries
 *   by nature, production JDBC is already funnelled to the chartered
 *   seams in bytecode (ArchitectureTest), and the register only made each
 *   new test file ask permission.</li>
 * </ul>
 *
 * <p>KNOWN LIMIT (the audit's own §3 lesson, recorded honestly): this
 * census is FILE-grained. New evaluation code added to an
 * already-registered file is invisible here — that residue is what the
 * eval ledger's size/name pins cover, and residue DELETION (the
 * relation-typed fetchDb leg) is the durable fix, not finer guards.
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
            "java\\.sql|javax\\.sql|org\\.duckdb|org\\.h2\\.|org\\.sqlite"
            + "|prepareStatement|createStatement|executeQuery");

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

    private static final Set<String> MAIN_REGISTER = new TreeSet<>(List.of(
            // 2026-10-04 C2a: Compiler (the planner) left the register; Execution, the execution front door, holds the
            // execute entry points it took (a session in, checked; nothing evaluated)
            "core/src/main/java/com/legend/Execution.java",
            // leg 3.4: the deferred verdict statements are keyed by the
            // session they run on (a store side's routed connection, or the
            // body's); sent through the one Executor choke point
            // 2026-09-23: DuckDB's Appender behind core's BulkLoad seam (its own
            // target beside the drivers; core compiles against none). It stages
            // every cell as TEXT and one INSERT ... SELECT casts: the DATABASE
            // types each value, exactly as the text path's quoted literals
            "core/src/main/duckdb/com/legend/exec/DuckDbAppenderLoad.java",
            // C3c (2026-10-04): DuckDB's own JSON cell type, recognised beside its driver (DriverCells) — it
            // moved out of Executor, which matched the class by name; carriage only (the node's text)
            "core/src/main/duckdb/com/legend/exec/DuckDbCells.java",
            "core/src/main/java/com/legend/exec/BulkLoad.java",   // the seam it joins: a Connection in, no SQL of its own
            // 2026-09-27: CsvSeed.run establishes a connection -- its declared setup
            // statements and rows, through Executor.executeRaw / Executor.load under the SEED
            // origin; a Connection in, the database executes (moved from StatementExecutor)
            "core/src/main/java/com/legend/exec/CsvSeed.java",
            // the execution side's ONE session owner (C3b, 2026-10-04; it absorbed JdbcMetadata, the
            // driver's one metadata read kept out of Compiler so the plan surface needs no java.sql):
            // the product/version read a handed session is checked by, and opening a declared
            // connection or a private in-memory database. It opens sessions; it executes nothing
            "core/src/main/java/com/legend/exec/Sessions.java",
            "core/src/main/java/com/legend/exec/PrepTrace.java",   // perf diagnostics: the timed prepare/execute seam, env-switched
            "core/src/main/java/com/legend/exec/VerdictBatch.java",
            "core/src/main/java/com/legend/StatementExecutor.java",
            "core/src/main/java/com/legend/exec/DynamicPivot.java",
            // Phase 1c: the LIMIT-0 schema probe (schema, never values;
            // moved from deleted ResultNav; split out of RawGridSchema at
            // the Invariant-7 staged-compilation move). (DbMetaData row
            // RETIRED: moved to compiler/spec/CatalogGrids — pure
            // SQL-text composition.)
            "core/src/main/java/com/legend/exec/GridProbe.java",
            // (PureAsserts + TdsCompare rows RETIRED 2026-08-21, the
            // D-arc dividend: PureDateLiteral is THE wire temporal
            // carrier, so the comparison layer's java.sql value arms
            // are GONE — sql types never escape the fetch seam.)
            "core/src/main/java/com/legend/exec/Executor.java",
            // THE SYSTEM DATABASE (user ruling 2026-09-02): the graph's
            // metamodel rows in a database of their own, separate from
            // every user connection — opened once per graph per engine,
            // written once; the executor routes store-reading bodies to
            // it. It opens the in-memory session and runs the seed DDL
            // through Executor.executeRaw; no value is read here.
            "core/src/main/java/com/legend/exec/SystemDatabase.java",
            // B7 (RaisedErrors): touches java.sql ONLY to rethrow a
            // SQLException whose raised-message envelope it removed —
            // the provenance seam at Executor's own funnel; it opens no
            // connection and executes nothing
            "core/src/main/java/com/legend/exec/RaisedErrors.java",
            // contract program: the wire census READS ResultSetMetaData
            // of results the Executor already fetched — measurement of
            // the wire's self-description, zero queries, zero decode;
            // the CanonicalDivergence pattern with a java.sql import
            "core/src/main/java/com/legend/exec/SqlTypeCensus.java",
            "core/src/main/java/com/legend/exec/PctProbe.java",
            // leg 3.3: the wire-decided kinds read from the prepared statement's metadata
            "core/src/main/java/com/legend/exec/WireTypes.java",
            // SQLTEXT charter §2: the replay-oracle SPI — a pure
            // interface (AssertListener precedent) whose SIGNATURE
            // names java.sql types (SQLException, the OracleRows cell
            // shape); it opens no connection and executes nothing —
            // the harness implementation does, on the testing side
            "core/src/main/java/com/legend/exec/SqlReplayOracle.java",
            // the product's test runner (batch 7a, 2026-09-11): opens one
            // session per package through the caller's connection factory and
            // HANDS the connection to the executor; the observer seam passes
            // it to the caller's referee; neither executes a statement of its
            // own (tenet #1 — the database executes what the platform compiles)
            "core/src/main/java/com/legend/test/PureTestRunner.java",
            // the service test-suite runner (2026-09-16): the same shape —
            // opens sessions and HANDS them to the platform, whose own
            // establish step seeds the test runtime's declared data; the
            // runner executes no SQL of its own (tenet #1)
            "core/src/main/java/com/legend/test/ServiceTestRunner.java",
            "core/src/main/java/com/legend/test/TestObserver.java",
            "core/src/main/java/com/legend/server/ConnectionResolver.java",
            "core/src/main/java/com/legend/server/QueryService.java",
            "core/src/main/java/com/legend/testdatagen/TestDataGenerator.java",
            // TestDataGenerationNatives (TDG lane S2): pure ORCHESTRATION — threads the
            // ambient connection through to TestDataGenerator's fetches
            // (the database executes); no statements of its own
            "core/src/main/java/com/legend/testdatagen/TestDataGenerationNatives.java"
    ));


    @Test
    void jdbcSurfaceIsRegistered() throws IOException {
        List<Path> files = new ArrayList<>();
        for (String r : ROOTS) {
            Path root = Repo.path(r);
            if (!Files.isDirectory(root)) {
                throw new IllegalStateException("JdbcSurfaceCensusTest root " + root + " is not among its inputs: declare it (Bazel workplan P3-14: a missing root failed silently)");
            }
            try (Stream<Path> s = Files.walk(root)) {
                s.filter(p -> p.toString().endsWith(".java"))
                        .forEach(files::add);
            }
        }
        assertTrue(files.size() >= FILE_FLOOR,
                "JDBC census coverage DROPPED: scanned " + files.size()
                + " files, floor " + FILE_FLOOR + " — a source root moved"
                + " or the walk rotted; re-point the census before"
                + " trusting any guard that scopes by path");

        Set<String> mainHits = new TreeSet<>();
        for (Path p : files) {
            String src = Files.readString(p)
                    .replaceAll("//.*", "")
                    .replaceAll("(?s)/\\*.*?\\*/", "");
            if (JDBC.matcher(src).find()) {
                String rel = Repo.root().toAbsolutePath().normalize()
                        .relativize(p.toAbsolutePath().normalize())
                        .toString().replace(java.io.File.separatorChar, '/');
                if (rel.contains("/main/")) {
                    mainHits.add(rel);
                }
            }
        }
        StringBuilder drift = new StringBuilder();
        diff(drift, "src/main", mainHits, MAIN_REGISTER);
        assertTrue(drift.length() == 0,
                "JDBC surface census drift (tenet #1 — Java orchestrates,"
                + " the DATABASE executes; audit 2026-08-18 items 6+7):"
                + drift);
    }

    private static void diff(StringBuilder drift, String where,
            Set<String> actual, Set<String> register) {
        for (String f : actual) {
            if (!register.contains(f)) {
                drift.append("\n  NEW JDBC surface in ").append(where)
                        .append(": ").append(f)
                        .append(" — register it CONSCIOUSLY (with the")
                        .append(" tenet argument) or route through an")
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
