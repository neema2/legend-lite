// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.sql.dialect.DeephavenLoad;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DEPLOYMENT, CHECKED AND RENDERED (docs/DEPLOYMENT_DESIGN_2026_11_14.md §8 and §9, step 1; its grammar and JSON:
 * {@code parser.LakehouseDeploymentTest}): its checks, each refused by name; and its live Deephaven target's plan -- data for
 * lite's fixed loader, never code: each landing table its topic, the brokers its source binds, its columns with the
 * Java types a Deephaven column holds them as, and what it keeps (an appended table its latest rows or all of them; a
 * milestoned table its current version a key, its milestoning columns named, its delete indicator's values).
 */
class DeploymentPlannerTest {

    private static final String STORES = """
            ###Relational
            Database t::Topics
            (
                Table "tp.trades.v1" (ID VARCHAR(20) PRIMARY KEY, AT TIMESTAMP, PX DOUBLE, DAY DATE)
                Table "tp.quotes.v1" (SYM VARCHAR(8) PRIMARY KEY, BID DOUBLE)
                Table "tp.books.v1" (BOOK VARCHAR(8) PRIMARY KEY, OWNER VARCHAR(20), OP VARCHAR(1))
                Table "tp.news.v1" (STORY VARCHAR(20) PRIMARY KEY, TEXT VARCHAR(200))
            )
            Database t::Ref
            (
                Table SECURITY (SYM VARCHAR(8) PRIMARY KEY, NAME VARCHAR(80))
            )

            ###Lakehouse
            Ingest t::Feed Delta<JSON>
            [
              TRADES( |#>{t::Topics."tp.trades.v1"}# ) pk=[ ID ] writeMode=AppendOnly;
              QUOTES( |#>{t::Topics."tp.quotes.v1"}# ) pk=[ SYM ] writeMode=BatchMilestoned;
            ]
            Ingest t::Books Delta<JSON> deleteIndicator=DeleteIndicator(deleteField='OP', deleteValues=['D', 'X'])
            [
              BOOKS( |#>{t::Topics."tp.books.v1"}# ) pk=[ BOOK ] writeMode=BatchMilestoned;
            ]
            Ingest t::News Delta<JSON>
            [
              NEWS( |#>{t::Topics."tp.news.v1"}# ) pk=[ STORY ] writeMode=AppendOnly;
            ]
            Ingest t::RefLoad Snapshot writeMode=BatchMilestoned schedule=Daily('06:00', 'America/New_York')
            [
              SECURITY( |#>{t::Ref.SECURITY}# ) pk=[ SYM ];
            ]

            ###Relational
            Database t::DB
            (
              include Ingest t::Feed
              include Ingest t::Books
              include Ingest t::News
            )

            ###Connection
            RelationalDatabaseConnection t::Server
            {
              store: t::DB;
              type: Deephaven;
              specification: Deephaven { host: 'localhost'; port: 10000; useTls: false; };
              auth: PreSharedKey { keyVaultReference: 'dh.psk'; };
            }
            RelationalDatabaseConnection t::InTab
            {
              store: t::DB;
              type: DuckDB;
              specification: DuckDB { };
              auth: Test;
            }
            RelationalDatabaseConnection t::Brokers
            {
              store: t::Topics;
              type: Kafka;
              specification: Kafka { bootstrapServers: '127.0.0.1:19092'; };
              auth: Test;
            }
            """;

    private static final String DEPLOYMENT = """
            ###Lakehouse
            Deployment t::OnDeephaven
            {
              deploys: [ t::Feed, t::Books, t::News ];
              sources: [ t::Brokers: [ t::Topics ] ];
              live: t::Server keep [ appended: LastRows(500), milestoned: current, NEWS: all ];
            }
            """;

    private static DeephavenLoad.Plan plan(String deployment) {
        return DeploymentPlanner.deephaven(Compiler.compileModel(STORES + deployment), "t::OnDeephaven");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> table(Map<String, Object> plan, String name) {
        for (Object t : (List<Object>) plan.get("tables")) {
            if (((Map<String, Object>) t).get("name").equals(name)) {
                return (Map<String, Object>) t;
            }
        }
        throw new AssertionError("no table " + name + " in " + plan);
    }

    @Test
    void eachLandingTableIsDataForTheLoader() {
        Map<String, Object> data = plan(DEPLOYMENT).data();
        assertEquals("legend.deephaven.load", data.get("kind"));
        assertEquals(DeephavenLoad.VERSION, data.get("version"));
        assertEquals("t::OnDeephaven", data.get("deployment"));
        assertEquals(Map.of("name", "TRADES", "source", Map.of("kind", "topic", "topic", "tp.trades.v1",
                "brokers", "127.0.0.1:19092"), "format", "JSON",
                "columns", List.of(Map.of("name", "ID", "type", "java.lang.String"),
                        Map.of("name", "AT", "type", "java.time.Instant"), Map.of("name", "PX", "type", "double"),
                        Map.of("name", "DAY", "type", "java.time.LocalDate")),
                "keep", Map.of("kind", "rows", "rows", 500L)), table(data, "TRADES"));
        // a milestoned dataset: its current version a key, its milestoning columns named, the delete indicator's values
        Map<String, Object> milestones = Map.of("digest", "LAKE_DIGEST", "batchIn", "LAKE_IN_ID",
                "batchOut", "LAKE_OUT_ID", "openBatch", 999_999_999L, "partitionPrefix", 1_000_000_000_000L);
        Map<String, Object> quotes = table(data, "QUOTES");
        assertEquals(List.of(Map.of("name", "SYM", "type", "java.lang.String"), Map.of("name", "BID", "type", "double")),
                quotes.get("columns"), "the record's columns alone");
        assertEquals(Map.of("kind", "current", "key", List.of("SYM"), "milestones", milestones), quotes.get("keep"));
        assertEquals(Map.of("kind", "current", "key", List.of("BOOK"), "milestones", milestones,
                "delete", Map.of("field", "OP", "values", List.of("D", "X"))), table(data, "BOOKS").get("keep"));
        // a dataset's own rule over its write mode's
        assertEquals(Map.of("kind", "all"), table(data, "NEWS").get("keep"));
        assertEquals(List.of("TRADES", "QUOTES", "BOOKS", "NEWS"), ((List<?>) data.get("tables")).stream()
                .map(t -> ((Map<?, ?>) t).get("name")).toList(), "the deploys' order, each ingest's datasets in its");
        assertEquals(List.of(), DeploymentPlanner.validate(Compiler.compileModel(STORES + DEPLOYMENT), "t::OnDeephaven"));
    }

    /** A data product over the stores: its model group's mapping maps classes onto QUOTES and NEWS; its access point
     *  reads TRADES. Deployed, it lands exactly those three datasets -- upstream's dependencyDatasets. */
    private static final String PRODUCT = """
            ###Pure
            Class t::Quote { sym: String[1]; }
            Class t::Story { id: String[1]; }

            ###Mapping
            Mapping t::M
            (
              t::Quote: Relational { ~mainTable [t::DB] QUOTES sym: [t::DB] QUOTES.SYM }
              t::Story: Relational { ~mainTable [t::DB] NEWS id: [t::DB] NEWS.STORY }
            )

            ###DataProduct
            DataProduct t::Product
            {
              accessPoints: [
                model ('The classes') Model { mapping: t::M; } [
                  tape: LH(|#>{t::DB.TRADES}#)
                ]
              ]
            }
            """;

    @Test
    void aDeployedDataProductLandsTheDatasetsItReads() {
        Map<String, Object> data = DeploymentPlanner.deephaven(Compiler.compileModel(STORES + PRODUCT + """
                ###Lakehouse
                Deployment t::OnDeephaven
                {
                  deploys: [ t::Product ];
                  sources: [ t::Brokers: [ t::Topics ] ];
                  live: t::Server keep [ appended: LastRows(500), milestoned: current ];
                }
                """), "t::OnDeephaven").data();
        assertEquals(List.of("TRADES", "QUOTES", "NEWS"), ((List<?>) data.get("tables")).stream()
                .map(t -> ((Map<?, ?>) t).get("name")).toList(), "BOOKS is not read: not landed");
        // a table the product reads that no ingest makes is named: nothing would land it
        List<String> found = DeploymentPlanner.validate(Compiler.compileModel(STORES + PRODUCT.replace(
                "LH(|#>{t::DB.TRADES}#)", "LH(|#>{t::Ref.SECURITY}#)") + """
                ###Lakehouse
                Deployment t::OnDeephaven
                {
                  deploys: [ t::Product ];
                  sources: [ t::Brokers: [ t::Topics ] ];
                  live: t::Server keep [ appended: LastRows(500), milestoned: current ];
                }
                """), "t::OnDeephaven");
        assertTrue(found.stream().anyMatch(p -> p.contains("data product t::Product reads the table SECURITY of t::Ref,"
                + " which no ingest makes")), found.toString());
    }

    /** A delivered ingest's files: read at its source's drop location, one a dataset, its rows the first delivery. */
    @Test
    void aDeliveredIngestsFilesAreReadAtItsDropLocation() {
        String drops = """
                ###Lakehouse
                Ingest t::Drops Snapshot<CSV> writeMode=BatchMilestoned
                [
                  POS( ID: Varchar(20)[1], QTY: Double ) pk=[ ID ];
                ]

                ###Connection
                RelationalDatabaseConnection t::Drop
                {
                  type: Files;
                  specification: Files { location: 'file:///tmp/t/drop/'; };
                  auth: Test;
                }
                """;
        String deployment = """
                ###Lakehouse
                Deployment t::OnDeephaven
                {
                  deploys: [ t::Drops ];
                  sources: [ t::Drop: [ t::Drops ] ];
                  live: t::Server keep [ milestoned: current ];
                }
                """;
        Map<String, Object> pos = table(DeploymentPlanner.deephaven(Compiler.compileModel(STORES + drops + deployment),
                "t::OnDeephaven").data(), "POS");
        assertEquals(Map.of("kind", "file", "url", "file:///tmp/t/drop/POS.csv"), pos.get("source"));
        assertEquals("CSV", pos.get("format"));
        assertEquals(Map.of("kind", "current", "key", List.of("ID"), "milestones", Map.of("digest", "LAKE_DIGEST",
                "batchIn", "LAKE_IN_ID", "batchOut", "LAKE_OUT_ID", "openBatch", 999_999_999L, "delivery", 1)),
                pos.get("keep"));
        List<String> found = DeploymentPlanner.validate(Compiler.compileModel(STORES + drops
                + deployment.replace("sources: [ t::Drop: [ t::Drops ] ];", "")), "t::OnDeephaven");
        assertTrue(found.stream().anyMatch(p -> p.contains("POS is delivered to t::Drops as files, which the"
                + " deployment's sources bind to no Files connection")), found.toString());
        RuntimeException parquet = assertThrows(RuntimeException.class, () -> DeploymentPlanner
                .deephaven(Compiler.compileModel(STORES + drops.replace("Snapshot<CSV>", "Snapshot<Parquet>")
                        + deployment), "t::OnDeephaven"));
        assertTrue(parquet.getMessage().contains("only a CSV file is landed on Deephaven yet"), parquet.getMessage());
    }

    private static List<String> problems(String deployment) {
        return DeploymentPlanner.validate(Compiler.compileModel(STORES + deployment), "t::OnDeephaven");
    }

    private static void refused(String body, String message) {
        String deployment = "###Lakehouse\nDeployment t::OnDeephaven\n{\n" + body + "\n}\n";
        List<String> found = problems(deployment);
        assertTrue(found.stream().anyMatch(p -> p.contains(message)), body + " -> " + found);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> plan(deployment));
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    @Test
    void eachCheckIsRefusedByName() {
        String sources = "sources: [ t::Brokers: [ t::Topics ] ];";
        String keep = "live: t::Server keep [ appended: LastRows(500), milestoned: current ];";
        refused("deploys: [ t::Feed, t::DB ]; " + sources + keep, "deploys t::DB, which is no Ingest or DataProduct");
        refused("deploys: [ t::Feed ]; " + keep, "which the deployment's sources bind to no Kafka connection");
        refused("deploys: [ t::Feed ]; sources: [ t::Brokers: [ t::Topics ], t::Brokers: [ t::Topics ] ]; " + keep,
                "2 Kafka connections");
        refused("deploys: [ t::Feed ]; sources: [ t::Nope: [ t::Topics ] ]; " + keep,
                "source t::Nope is no connection");
        refused("deploys: [ t::Feed ]; sources: [ t::Brokers: [ t::Nowhere ] ]; " + keep,
                "feeds t::Nowhere, which is no Database or Ingest");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ milestoned: current ];",
                "TRADES keeps nothing on the live target");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: all ];",
                "QUOTES keeps nothing on the live target");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: all, QUOTES: LastRows(9) ];",
                "QUOTES is milestoned and keeps LastRows(9)");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: all, TRADES: current,"
                + " milestoned: current ];", "TRADES is appended and keeps current");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: all, milestoned: versions ];",
                "every version is the history tier's");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: Last('PT8H'),"
                + " milestoned: current ];", "a span of time is not kept on Deephaven yet");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: current, milestoned: current ];",
                "keeps appended datasets by a rule of the other write mode");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::Server keep [ appended: all, milestoned: current,"
                + " ELSEWHERE: all ];", "keeps ELSEWHERE, which is no dataset of an ingest it deploys");
        refused("deploys: [ t::Feed ]; " + sources + " " + keep + " history: t::Server keep [ appended: all ];",
                "lands a history tier");
        refused("deploys: [ t::Feed ]; " + sources + " live: t::InTab keep [ appended: all, milestoned: current ];",
                "is a DuckDB connection: only a Deephaven server is rendered");
        refused("deploys: [ t::RefLoad ]; sources: [ t::Brokers: [ t::Ref ] ]; " + keep,
                "lands by Snapshot on a schedule, which a Deephaven server does not land yet");
        assertThrows(IllegalArgumentException.class, () -> DeploymentPlanner.validate(Compiler.compileModel(STORES),
                "t::Missing"));
    }
}
