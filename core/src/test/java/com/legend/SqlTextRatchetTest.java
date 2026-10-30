// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SECOND-SQL-PRODUCER RATCHET (ADVERSARIAL_TENET_AUDIT_2026_08_18
 * §8, fix-plan item 10): AGENTS.md invariants 2/3/3a say there is ONE
 * path from HIR to SQL — Lowerer &rarr; typed MIR &rarr;
 * {@code dialect.render()}. Production code outside {@code sql/dialect/}
 * that builds SQL as STRING TEXT is a shadow producer the dialect cannot
 * retarget, {@code SqlPostProcessors} cannot rewrite, and no backend
 * swap reaches.
 *
 * <p>The target is NOT zero — DDL, {@code information_schema} catalog
 * queries, and plan-envelope engine text have no MIR representation and
 * are the decision rule's permitted classes. The target is ENUMERATED
 * AND ARGUED: every site below is registered per file, shrink-only.
 * Growth is a new shadow SQL producer and needs either a route through
 * the compiler or a conscious pin bump with a written justification.
 * The registered residue's durable fix is DELETION — the relation-typed
 * {@code fetchDb} leg retires the GridReads/DbMetaData rows wholesale.
 *
 * <p>Counting rule (comment-stripped source, string literals only):
 * a literal is a SQL-construction site when it matches a
 * select-from / leading-DDL-keyword / catalog / union-all / limit-n /
 * lateral shape and carries no Pure {@code ::} path. The rule is
 * deliberately high-precision over high-recall — a missed spelling is
 * drift the next audit catches; a noisy pin rots trust in the register.
 */
@Tag("guardrail")
class SqlTextRatchetTest {

    // UNROLLED-LOOP form (no per-character alternation): the naive
    // "(?:[^"\\]|\\.)*" recursed once per character and overflowed the
    // stack on a multi-KB text block (the system metamodel source, step 3)
    private static final Pattern LITERAL =
            Pattern.compile("\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\"");

    private static final Pattern SQL_SHAPE = Pattern.compile(
            "(?i)(\\bselect\\s+\\S.*\\s+from\\b"
            // create schema (2026-10-13, the dialect program's Phase 0: its blind spot let StatementExecutor's (BodyDriver since 2026-10-29)
            // dropAndCreateSchemaInDb text through)
            + "|^\"\\s*(select|create table|create schema|insert into|drop table"
            + "|delete from|alter table)\\s+\\S"
            + "|information_schema|\\bunion all\\s|\\blimit \\d"
            + "|\\bleft lateral\\s)");

    /** The register: file &rarr; exact SQL-text site count, measured
     * 2026-08-18 (audit §8 seeded 33 with a narrower rule; this rule
     * also counts Ddl — the one registered DDL owner — and
     * StatementExecutor's engine-text sites; BodyDriver since 2026-10-29). */
    private static final Map<String, Integer> REGISTER =
            new LinkedHashMap<>();

    static {
        // (StatementExecutor.java, BodyDriver since 2026-10-29: its two sites were the record-only PRIMARY KEY
        // ALTER strings of a write-only meta ledger — deleted in batch 137)
        // (LineageTreeVerdicts.java: its one site — the tree-print → rows
        // query, DuckDB-only — DELETED in task #14 leg 1, 2026-09-21: the
        // golden print is brought to its lines at compile time and the
        // ordinary collection verdict judges them against the prelude's
        // own rows)
        // batch 67 (2026-09-05): the exec-read arm RECOGNIZES the engine's
        // population statement (`select distinct <col> from <table>`) in
        // a GOLDEN — a referee's read of the spec text's shape (the let
        // it populates supplies our rows), never an emission of ours.
        REGISTER.put("driver/SqlTextVerdicts.java", 1);   // the root's until step C (D73, 2026-10-30): com.legend.driver
        // 0 -> 2 (2026-10-13, the dialect program's Phase 0, docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md): not new
        // sites -- the rule now sees `create schema`, and two were there: dropAndCreateSchemaInDb's H2 text and a
        // schema's create in the module DDL. They go to the dialect (a SqlDdl node) with the program's Phase 3
        // 2 -> 0, row deleted (2026-10-16, the dialect program's Phase 3): both are SqlDdl.CreateSchema/DropSchema
        // nodes now, rendered by the session's dialect, H2 for the replay ledger, or the engine text for toDDL
        // exec/CsvSeed.java: 1 -> 0 and row deleted (2026-09-23): its INSERT is a RowLoad and its DELETE a SqlDml node, both dialect-rendered
        // Phase 1c: DbMetaData's 9 catalog-SQL sites moved verbatim to
        // compiler/spec/CatalogGrids (the Typer's fetchDb retype; pure
        // text composition, no JDBC)
        // 9 -> 10 (batch 71, 2026-09-05): the primary-keys grid is a
        // LIVE-catalog query like its three siblings (information_schema
        // key_column_usage + table_constraints) — the model-fact VALUES
        // splice it replaces died with the Java fact-walk; catalog text
        // is the registered class this file exists for
        // 10 -> 0, row deleted (2026-10-16, the dialect program's Phase 3): a catalog grid is a typed node and an IR
        // source (TypedCatalogGrid, SqlSource.Catalog); each dialect whose database is proven reads its own catalog
        // (DuckDb and H2, through sql/dialect/InformationSchemaCatalog), every other refuses it
        // 3 -> 4 (metamodel-store leg 2026-08-28): metamodelSeed's
        // registry-extent INSERT joins the one DDL owner — system
        // setup text beside the create/drop it already renders
        // 2026-10-08: CsvSeed/Ddl/RowLoad text half moved exec -> setup, execution plan boundary step 2's opening
        // piece; docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9 (path only, count unchanged)
        // setup/Ddl.java: 1 -> 0 and row deleted (2026-10-16, the dialect program's Phase 3): the seed rows are
        // SqlDml.InsertValues, typed cells the engine-text dialect renders as legend-engine's loadValuesToDbTableH2
        // writes them. (The history: 2 -> 1, 2026-09-23: the metamodel seed's INSERT is a dialect-rendered SqlDml
        // node, RowLoad.values; 4 -> 2, 2026-09-16: CREATE/DROP are dialect-rendered SqlDdl nodes.)
        // plan/InProtocol.java 1 and plan/PlanText.java 2 -> 0, rows deleted (2026-10-16, the dialect program's
        // Phase 3): the in-collection temp table's read-back is the plan text's dialect's
        // (EngineTextDialect.inTempTableSelect), and a scalar projection's resultColumns is its select list, written
        // from the IR (EngineTextDialect.selectList), not cut out of the rendered text
        // 17 -> 16 (documented-debts 2026-08-18): the LL_TMP_DEBUG
        // COUNT(*) round-trip died
        // (chained-fetch live-session refereeing, §S5-L: the transcript
        // capture's whole-relation read consolidated with csvEnvelope's
        // union arm onto ONE selectAll() spelling — the count STAYS 16;
        // internal plumbing, not recorded artifact)
        // 16 -> 15 (views stage 3, 2026-09-22): the hand-built view fetch
        // renderer is gone — the view's SQL is the compiler's, handed in by
        // the driver (TestDataGenerator.ViewSql)
        // testdatagen/TestDataGenerator.java: 15 -> 0, row deleted (2026-10-16, the dialect program's Phase 3): the
        // generator's fetches, their temp tables, the CSV envelope and the seed read are IR its session's dialect
        // writes, its plan's fetches IR the engine-text dialect writes (renderAsAliased); the uncalled CSV compare
        // (EXCEPT, INSERT) is deleted
        // 0 -> 1 (2026-10-11, the MariaDB dialect, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13): after a
        // MariaDB statement fails, Sessions.raisedMessage reads the session variable its raise left
        // (SELECT @legend_raise) -- no query of the user's, one fixed read of a session's state: a
        // MariaDB SELECT can raise no error of its own text, so its dialect keeps the text there.
        // exec/Sessions.java -> exec/MariaDbConnector.java (2026-10-16, the dialect program's Phase 5): the
        // read is MariaDB's connector's (DatabaseConnector.raisedMessage), the owner of its sessions' state
        // (the plan's §4: the connector owns the held raise). exec may not reach the dialect for it
        // (ArchitectureTest.execReachesNoPlanning), so the statement stays on the execution side.
        // 2026-10-29 (D65, the three routings): the connector DECLARES the read and the reset as data
        // (DatabaseConnector.raiseRecovery) and the seam runs them (JdbcStatements.held); the one SQL-shaped
        // literal is the same SELECT, the count unmoved
        REGISTER.put("exec/MariaDbConnector.java", 1);
        // 0 -> 1 (2026-10-16, the Cypher island's zoned date-time leg, docs/CYPHER_STUDY_2026_10_15.md §12.8): no
        // SQL -- Neo4j's own error text "Cannot select datetime from: <value>", which the translator raises as
        // Neo4j does when a datetime is asked of a date or a time; the rule reads its "select ... from"
        REGISTER.put("protocol/cypher/CypherToPure.java", 1);
    }

    @Test
    void sqlTextOutsideTheDialectLayerOnlyShrinks() throws IOException {
        String root = "core/src/main/java/com/legend";
        Map<String, Integer> actual = new TreeMap<>();
        int scanned = 0;
        try (Stream<Path> s = SourceFiles.under(root).stream()) {
            for (Path p : s.filter(f -> f.toString().endsWith(".java"))
                    .toList()) {
                String rel = SourceFiles.rel(root, p).substring(1);
                if (rel.startsWith("sql/dialect/")) {
                    continue;
                }
                scanned++;
                String src = stripComments(Files.readString(p));
                int n = 0;
                Matcher m = LITERAL.matcher(src);
                while (m.find()) {
                    String lit = m.group();
                    if (!lit.contains("::") && SQL_SHAPE.matcher(lit)
                            .find()) {
                        n++;
                    }
                }
                if (n > 0) {
                    actual.put(rel, n);
                }
            }
        }
        // coverage self-assertion (audit item 7): the walk must keep
        // seeing the production tree it guards
        assertTrue(scanned >= 250, "SQL-text ratchet coverage DROPPED:"
                + " scanned " + scanned + " production files (floor 250)"
                + " — the walk root moved; re-point before trusting it");

        StringBuilder drift = new StringBuilder();
        for (var e : actual.entrySet()) {
            Integer pinned = REGISTER.get(e.getKey());
            if (pinned == null) {
                drift.append("\n  NEW shadow SQL producer: ")
                        .append(e.getKey()).append(" (").append(
                                e.getValue())
                        .append(" sites) — route through the compiler"
                                + " (Lowerer -> MIR -> dialect.render)"
                                + " or register with a justification");
            } else if (!pinned.equals(e.getValue())) {
                drift.append("\n  ").append(e.getKey()).append(": ")
                        .append(e.getValue())
                        .append(e.getValue() > pinned ? " > " : " < ")
                        .append(pinned)
                        .append(e.getValue() > pinned
                                ? " — SQL text GREW outside the dialect"
                                        + " layer"
                                : " — sites died: shrink this pin");
            }
        }
        for (String f : REGISTER.keySet()) {
            if (!actual.containsKey(f)) {
                drift.append("\n  ").append(f)
                        .append(" builds no SQL text anymore — delete"
                                + " its register row");
            }
        }
        assertTrue(drift.length() == 0,
                "shadow-SQL-producer drift (AGENTS.md invariants 2/3/3a;"
                + " audit 2026-08-18 §8):" + drift);
    }

    /** {@code src} with its comments removed and its literals kept as written: a {@code //} or a {@code /*} inside a
     *  literal (a JDBC URL's {@code jdbc:mariadb://}) is no comment (2026-10-16, the dialect program's Phase 5: the
     *  regex stripping before cut such a line at its {@code //}, and the literal scan, out of step with the quotes,
     *  read the code up to the next literal as one and that literal's SQL as none). */
    static String stripComments(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int n = src.length();
        int i = 0;
        while (i < n) {
            char c = src.charAt(i);
            int end;
            if (src.startsWith("\"\"\"", i)) {
                end = src.indexOf("\"\"\"", i + 3);
                while (end > 0 && escaped(src, end)) {
                    end = src.indexOf("\"\"\"", end + 1);
                }
                end = end < 0 ? n : end + 3;
                out.append(src, i, end);
            } else if (c == '"' || c == '\'') {
                end = i + 1;
                while (end < n && src.charAt(end) != c && src.charAt(end) != '\n') {
                    end += src.charAt(end) == '\\' ? 2 : 1;
                }
                end = Math.min(end + 1, n);
                out.append(src, i, end);
            } else if (src.startsWith("//", i)) {
                end = src.indexOf('\n', i);
                end = end < 0 ? n : end;
            } else if (src.startsWith("/*", i)) {
                end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
            } else {
                end = i + 1;
                out.append(c);
            }
            i = end;
        }
        return out.toString();
    }

    /** Whether the character at {@code at} is escaped: an odd run of backslashes before it ({@code \\"""} ends a text
     *  block; {@code \"""} does not). */
    private static boolean escaped(String src, int at) {
        int run = 0;
        while (at - run - 1 >= 0 && src.charAt(at - run - 1) == '\\') {
            run++;
        }
        return run % 2 == 1;
    }
}
