package com.gs.legend.test;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test: 100K classes, tables, mappings, joins.
 * 10x the 10K baseline — designed to surface integer-key vs string-key performance.
 */
@DisplayName("Stress Tests 100K")
class StressTest100K {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    private String withRuntime(String model, String dbName, String mappingName) {
        return model + """
                
                import test::*;

                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    @Test
    @DisplayName("100K hub-spoke model")
    void test100KModel() throws SQLException {
        int HUBS = 10000;
        int SATS_PER_HUB = 9;
        int SATS = HUBS * SATS_PER_HUB;
        int N = HUBS + SATS; // 100000

        // ---- Phase 0: Generate Pure source ----
        long t0 = System.nanoTime();
        var sb = new StringBuilder(N * 800);

        // ---- Hub classes H0..H9999 ----
        for (int h = 0; h < HUBS; h++) {
            sb.append("Class test::H").append(h).append(" {\n");
            sb.append("    id: Integer[1];\n");
            sb.append("    name: String[1];\n");
            sb.append("    code: String[1];\n");
            sb.append("    score: Integer[1];\n");
            sb.append("    fullLabel: String[1];\n");
            sb.append("}\n");
        }

        // ---- Satellite classes S0..S89999 ----
        for (int s = 0; s < SATS; s++) {
            sb.append("Class test::S").append(s).append(" {\n");
            sb.append("    id: Integer[1];\n");
            sb.append("    label: String[1];\n");
            sb.append("    value: Integer[1];\n");
            sb.append("}\n");
        }

        // ---- Associations ----
        int assocCount = 0;

        // Hub ring: H0→H1, ..., H9999→H0
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            sb.append("Association test::HubRing").append(h).append(" {\n");
            sb.append("    nextHub").append(h).append(": test::H").append(next).append("[0..1];\n");
            sb.append("    prevHub").append(h).append(": test::H").append(h).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // Cross-links: every 100th hub links to hub+50
        for (int h = 0; h < HUBS; h += 100) {
            int target = (h + 50) % HUBS;
            sb.append("Association test::HubCross").append(h).append(" {\n");
            sb.append("    crossTo").append(h).append(": test::H").append(target).append("[0..1];\n");
            sb.append("    crossFrom").append(h).append(": test::H").append(h).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // Satellite→Hub: S_i belongs to H_(i/9)
        for (int s = 0; s < SATS; s++) {
            int hub = s / SATS_PER_HUB;
            sb.append("Association test::SatHub").append(s).append(" {\n");
            sb.append("    hub").append(s).append(": test::H").append(hub).append("[0..1];\n");
            sb.append("    sat").append(s).append(": test::S").append(s).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // ---- Database ----
        sb.append("Database store::DB (\n");

        // Hub tables
        for (int h = 0; h < HUBS; h++) {
            sb.append("    Table TH").append(h)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), SCORE INT, STATUS VARCHAR(10)");
            sb.append(", NEXT_HUB_ID INT");
            if (h % 100 == 0) sb.append(", CROSS_HUB_ID INT");
            sb.append(")\n");
        }

        // Satellite tables
        for (int s = 0; s < SATS; s++) {
            sb.append("    Table TS").append(s)
              .append(" (ID INT, LABEL VARCHAR(100), VALUE INT, HUB_ID INT)\n");
        }

        // Hub ring joins
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            sb.append("    Join JHubRing").append(h)
              .append("(TH").append(h).append(".NEXT_HUB_ID = TH").append(next).append(".ID)\n");
        }

        // Cross-link joins
        for (int h = 0; h < HUBS; h += 100) {
            int target = (h + 50) % HUBS;
            sb.append("    Join JCross").append(h)
              .append("(TH").append(h).append(".CROSS_HUB_ID = TH").append(target).append(".ID)\n");
        }

        // Satellite→Hub joins
        for (int s = 0; s < SATS; s++) {
            int hub = s / SATS_PER_HUB;
            sb.append("    Join JSat").append(s)
              .append("(TS").append(s).append(".HUB_ID = TH").append(hub).append(".ID)\n");
        }

        sb.append(")\n");

        // ---- Mappings ----
        sb.append("Mapping test::M (\n");

        // Hub mappings
        for (int h = 0; h < HUBS; h++) {
            sb.append("    test::H").append(h).append(": Relational {\n");
            sb.append("        ~mainTable [store::DB] TH").append(h).append("\n");
            sb.append("        id: [store::DB] TH").append(h).append(".ID,\n");
            sb.append("        name: [store::DB] TH").append(h).append(".NAME,\n");
            sb.append("        code: [store::DB] TH").append(h).append(".CODE,\n");
            sb.append("        score: [store::DB] TH").append(h).append(".SCORE,\n");
            sb.append("        fullLabel: concat([store::DB] TH").append(h)
              .append(".NAME, '-', [store::DB] TH").append(h).append(".CODE)\n");
            sb.append("    }\n");
        }

        // Satellite mappings
        for (int s = 0; s < SATS; s++) {
            sb.append("    test::S").append(s).append(": Relational {\n");
            sb.append("        ~mainTable [store::DB] TS").append(s).append("\n");
            sb.append("        id: [store::DB] TS").append(s).append(".ID,\n");
            sb.append("        label: [store::DB] TS").append(s).append(".LABEL,\n");
            sb.append("        value: [store::DB] TS").append(s).append(".VALUE\n");
            sb.append("    }\n");
        }

        // Hub ring association mappings
        for (int h = 0; h < HUBS; h++) {
            sb.append("    test::HubRing").append(h).append(": AssociationMapping (\n");
            sb.append("        nextHub").append(h).append(": [store::DB]@JHubRing").append(h).append(",\n");
            sb.append("        prevHub").append(h).append(": [store::DB]@JHubRing").append(h).append("\n");
            sb.append("    )\n");
        }

        // Cross-link association mappings
        for (int h = 0; h < HUBS; h += 100) {
            sb.append("    test::HubCross").append(h).append(": AssociationMapping (\n");
            sb.append("        crossTo").append(h).append(": [store::DB]@JCross").append(h).append(",\n");
            sb.append("        crossFrom").append(h).append(": [store::DB]@JCross").append(h).append("\n");
            sb.append("    )\n");
        }

        // Satellite→Hub association mappings
        for (int s = 0; s < SATS; s++) {
            sb.append("    test::SatHub").append(s).append(": AssociationMapping (\n");
            sb.append("        hub").append(s).append(": [store::DB]@JSat").append(s).append(",\n");
            sb.append("        sat").append(s).append(": [store::DB]@JSat").append(s).append("\n");
            sb.append("    )\n");
        }

        sb.append(")\n");

        String model = withRuntime(sb.toString(), "store::DB", "test::M");
        long genMs = (System.nanoTime() - t0) / 1_000_000;

        int joinCount = HUBS + (HUBS / 100) + SATS;
        System.out.println("=== STRESS TEST: Hub-Spoke 100K ===");
        System.out.println("Model: " + HUBS + " hubs, " + SATS + " satellites, "
                + assocCount + " associations, " + joinCount + " joins");
        System.out.println("Pure source size: " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (generate source): " + genMs + " ms");

        // ---- Phase 1: Parse + build model ----
        long t1 = System.nanoTime();
        var builder = new com.gs.legend.model.PureModelBuilder().addSource(model);
        long buildMs = (System.nanoTime() - t1) / 1_000_000;
        System.out.println("Phase 1 (parse + build model): " + buildMs + " ms");

        // ---- Phase 2: Normalize ----
        long t2 = System.nanoTime();
        var mappingNames = builder.resolveMappingNames("test::RT");
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(builder, mappingNames);
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        System.out.println("Phase 2 (normalize " + N + " mappings): " + normMs + " ms");

        var dialect = builder.resolveDialect("test::RT");
        var normalizedMapping = normalizer.normalizedMapping();
        var modelCtx = normalizer.modelContext();

        // ---- 100 diverse queries spread across the 100K model ----
        var queries = new java.util.ArrayList<String>();
        for (int q = 0; q < 100; q++) {
            int h = q * (HUBS / 100); // spread across the 10000 hubs
            int s = h * SATS_PER_HUB;
            int nextH = (h + 1) % HUBS;

            switch (q % 10) {
                case 0 -> // Simple project on hub
                    queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 1 -> // Simple project on satellite
                    queries.add("test::S" + s + ".all()->project(~[id, label, value])");
                case 2 -> // Hub project + filter
                    queries.add("test::H" + h + ".all()->project(~[id, name, score])"
                            + "->filter(r|$r.score > 50)");
                case 3 -> // Hub project + sort
                    queries.add("test::H" + h + ".all()->project(~[id, name, fullLabel])"
                            + "->sort(~name->ascending())");
                case 4 -> // Hub project with DynaFunc column only
                    queries.add("test::H" + h + ".all()->project(~[id, fullLabel])");
                case 5 -> // Hub → nextHub association navigation
                    queries.add("test::H" + h + ".all()->project(~[id, name, nh:x|$x.nextHub"
                            + h + ".name])");
                case 6 -> // Satellite → hub association navigation
                    queries.add("test::S" + s + ".all()->project(~[id, label, hubName:x|$x.hub"
                            + s + ".name])");
                case 7 -> // Hub project + filter + sort + limit
                    queries.add("test::H" + h + ".all()->project(~[id, name, score])"
                            + "->filter(r|$r.id > 0)->sort(~score->descending())->limit(10)");
                case 8 -> // Hub → nextHub → nextNextHub (2-hop ring)
                    queries.add("test::H" + h + ".all()->project(~[id, name, nn:x|$x.nextHub"
                            + h + ".nextHub" + nextH + ".name])");
                case 9 -> // Simple hub project (different hub)
                    queries.add("test::H" + h + ".all()->project(~[id, name])");
            }
        }

        // ---- Run all 100 queries ----
        int passed = 0, failed = 0;
        long queryStartAll = System.nanoTime();
        for (int q = 0; q < queries.size(); q++) {
            String query = queries.get(q);
            try {
                var vs = com.gs.legend.parser.PureParser.parseQuery(query);
                var unit = new com.gs.legend.compiler.TypeChecker(modelCtx).check(vs);
                var storeRes = new com.gs.legend.compiler.MappingResolver(
                        unit, normalizedMapping, builder).resolve();
                var plan = new com.gs.legend.plan.PlanGenerator(
                        unit, dialect, storeRes).generate();
                assertNotNull(plan.sql(), "Query " + q + " produced null SQL");
                assertFalse(plan.sql().isBlank(), "Query " + q + " produced blank SQL");
                passed++;
            } catch (Exception e) {
                System.out.println("  FAIL q" + q + " [" + query.substring(0, Math.min(80, query.length()))
                        + "...]: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                failed++;
            }
        }
        long queryMs = (System.nanoTime() - queryStartAll) / 1_000_000;
        long totalMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("100 queries: " + passed + " passed, " + failed + " failed in " + queryMs + " ms");
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " queries failed out of " + queries.size());
    }
}
