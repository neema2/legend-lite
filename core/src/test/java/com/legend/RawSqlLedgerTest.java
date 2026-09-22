// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * F1.6 — the R0 rule as a shrink-only ledger (Charter C5.2).
 * {@code RawSqlBoundary}'s contract is "text-level translation of
 * corpus-AUTHORED statements only … never against platform-GENERATED
 * SQL" — and at the F0.1 baseline the contract is not yet true: the
 * translation entry ({@code h2ToDuckDb}) has TWO callers, one clean and
 * one mixed. This ledger pins the caller set so it can only shrink;
 * F7.4 (spell model-derived DDL correctly the first time) drives it to
 * the single legitimate entry and THEN the contract becomes true.
 *
 * <p>Ledger, with text-origin classification:
 * <ul>
 *   <li>{@code StatementExecutor.adaptRaw} — the executeInDb SQL
 *       argument: corpus-AUTHORED test text. LEGITIMATE (origin is
 *       another dialect; adaptation is the boundary's charter).</li>
 *   <li>{@code rcorpus/Runner} seed replay — MIXED: corpus-authored
 *       setup statements AND Java-generated DDL
 *       ({@code Ddl.setUpDataSqlsText} output, which S4's loop spells
 *       H2-style ON PURPOSE so the boundary can rewrite it). The
 *       Java-generated share is the F7.4 work item; when it lands this
 *       entry becomes corpus-only and the ledger does NOT shrink —
 *       the FEED narrows instead (verified by F7.4's acceptance).</li>
 * </ul>
 */
class RawSqlLedgerTest {

    // F6.6: HostEval joined — the executeInDb READ path adapts the SAME
    // corpus-authored raw H2 the write path does, before running it on
    // the ambient session (the R0 contract holds: corpus-authored text
    // only, now symmetrically on both directions of the boundary).
    // F7.4: Runner LEFT — module DDL is model-derived and is now spelled
    // for its target directly (Ddl.createTable duck flavor); only
    // corpus-AUTHORED text crosses the translator, which is the stated
    // contract, true at last.
    private static final Map<String, Integer> LEDGER = Map.of(
            "StatementExecutor.java", 1,
            // Phase 1c: the ONE render-time adapter — the DuckDb pass
            // (slice 3: ResultNav's pre-adaptation DIED; it had begun
            // double-translating once the pass existed)
            "RawSqlAdapt.java", 1);

    private static final Pattern SITE =
            Pattern.compile("RawSqlBoundary(\\.|::)h2ToDuckDb");

    @Test
    void rawSqlTranslationCallersAreLedgered() throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        for (Path root : new Path[] {Repo.module("src/main/java"),
                Repo.module("src/test/java")}) {
            try (Stream<Path> files = Files.walk(root)) {
                for (Path f : files
                        .filter(p -> p.toString().endsWith(".java"))
                        .filter(p -> !p.getFileName().toString()
                                .equals("RawSqlLedgerTest.java"))
                        .toList()) {
                    Matcher m = SITE.matcher(Files.readString(f)
                            .replaceAll("//.*", "")
                            .replaceAll("(?s)/\\*.*?\\*/", ""));
                    int n = 0;
                    while (m.find()) {
                        n++;
                    }
                    if (n > 0) {
                        found.put(f.getFileName().toString(), n);
                    }
                }
            }
        }
        assertEquals(new TreeMap<>(LEDGER), found,
                "RawSqlBoundary.h2ToDuckDb caller set moved — the R0"
                + " contract admits corpus-AUTHORED text only; a new"
                + " caller is a violation, a removed caller shrinks the"
                + " ledger (F7.4 narrows Runner's feed, not this list)");
    }

    /** THE RawSql QUARANTINE (One-Platform Plan Phase 1, user-ratified
     * 2026-08-18): {@code SqlSource.RawSql} carries corpus/user-AUTHORED
     * SQL text as a relation source — it must NEVER become a smuggling
     * channel for platform-composed SQL past the compiler. Exact
     * construction-site register, both directions; the companion
     * ArchitectureTest bytecode rule enforces the same boundary below
     * source level, and the SQL-text ratchet patrols the composition
     * side. */
    private static final Map<String, Integer> RAW_SOURCE_CTORS = Map.of(
            // the LIMIT-0 schema probe (itself MIR-rendered; moved from
            // ResultNav at its Phase 1c-endgame deletion, then to
            // GridProbe at the Invariant-7 staged-compilation split —
            // the resolver pass takes the roster through its oracle)
            "GridProbe.java", 1,
            // Phase 1c: the compiler's TypedRawSqlRelation lowering,
            // carrying the AUTHORED text from the typed node verbatim
            "Lowerer.java", 1,
            // Phase 1c: RawSqlAdapt REWRAPS an existing RawSql with the
            // adapted authored text — same contract, dialect layer
            "RawSqlAdapt.java", 1,
            // leg 3.4 step 2 (2026-09-20): AliasPrefix REWRAPS an existing RawSql
            // under the frame body's alias prefix — the authored text verbatim
            "AliasPrefix.java", 1);

    @Test
    void rawSqlSourceConstructionIsQuarantined() throws IOException {
        Pattern ctor = Pattern.compile("new SqlSource\\.RawSql\\(");
        Map<String, Integer> found = new TreeMap<>();
        for (Path root : new Path[] {Repo.module("src/main/java"),
                Repo.module("src/test/java")}) {
            try (Stream<Path> files = Files.walk(root)) {
                for (Path f : files
                        .filter(p -> p.toString().endsWith(".java"))
                        .filter(p -> !p.getFileName().toString()
                                .equals("RawSqlLedgerTest.java"))
                        .toList()) {
                    Matcher m = ctor.matcher(Files.readString(f)
                            .replaceAll("//.*", "")
                            .replaceAll("(?s)/\\*.*?\\*/", ""));
                    int n = 0;
                    while (m.find()) {
                        n++;
                    }
                    if (n > 0) {
                        found.put(f.getFileName().toString(), n);
                    }
                }
            }
        }
        assertEquals(new TreeMap<>(RAW_SOURCE_CTORS), found,
                "SqlSource.RawSql construction site set moved — RawSql"
                + " carries AUTHORED text through the chartered seam"
                + " (ResultNav) only; wrapping platform-composed SQL in"
                + " it is smuggling past the compiler");
    }
}
