package com.gs.legend.test;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test: 10K classes with DENSE connectivity (~10 hub-to-hub links each).
 * Same query patterns as StressTest — isolates normalizer/compiler impact of density.
 */
@DisplayName("Stress Tests Dense")
class StressTestDense {

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

    /**
     * Dense hub-spoke topology with 10K classes:
     *
     * 1000 Hub classes (H0..H999):
     *   - Each hub connects to 10 other hubs via distinct associations
     *     (ring + 9 skip-links at offsets 1,2,3,5,7,11,13,17,19)
     *   - ~10 hub-to-hub associations per hub
     *
     * 9000 Satellite classes (S0..S8999): 9 per hub
     *   - Each satellite connects to its hub via to-one association
     *
     * Same 10 query patterns as StressTest — measures density impact on normalizer.
     */
    @Test
    @DisplayName("10K dense hub-spoke model")
    void testDenseModel() throws SQLException {
        int HUBS = 1000;
        int SATS_PER_HUB = 9;
        int SATS = HUBS * SATS_PER_HUB;
        int N = HUBS + SATS;
        int[] SKIP_OFFSETS = {1, 2, 3, 5, 7, 11, 13, 17, 19};

        // ---- Phase 0: Generate Pure source ----
        long t0 = System.nanoTime();
        var sb = new StringBuilder(N * 1200);

        // ---- Hub classes ----
        for (int h = 0; h < HUBS; h++) {
            sb.append("Class test::H").append(h).append(" {\n");
            sb.append("    id: Integer[1];\n");
            sb.append("    name: String[1];\n");
            sb.append("    code: String[1];\n");
            sb.append("    score: Integer[1];\n");
            sb.append("    fullLabel: String[1];\n");
            sb.append("}\n");
        }

        // ---- Satellite classes ----
        for (int s = 0; s < SATS; s++) {
            sb.append("Class test::S").append(s).append(" {\n");
            sb.append("    id: Integer[1];\n");
            sb.append("    label: String[1];\n");
            sb.append("    value: Integer[1];\n");
            sb.append("}\n");
        }

        // ---- Associations ----
        int assocCount = 0;

        // Hub skip-links: each hub connects to 9 others at various offsets
        for (int h = 0; h < HUBS; h++) {
            for (int si = 0; si < SKIP_OFFSETS.length; si++) {
                int target = (h + SKIP_OFFSETS[si]) % HUBS;
                sb.append("Association test::HubLink").append(h).append("_").append(si).append(" {\n");
                sb.append("    link").append(h).append("_").append(si).append(": test::H").append(target).append("[0..1];\n");
                sb.append("    back").append(h).append("_").append(si).append(": test::H").append(h).append("[0..1];\n");
                sb.append("}\n");
                assocCount++;
            }
        }

        // Hub ring: H0→H1→...→H999→H0 (the canonical nextHub for queries)
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            sb.append("Association test::HubRing").append(h).append(" {\n");
            sb.append("    nextHub").append(h).append(": test::H").append(next).append("[0..1];\n");
            sb.append("    prevHub").append(h).append(": test::H").append(h).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // Satellite→Hub
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

        // Hub tables — each has FK columns for all skip-links + ring
        for (int h = 0; h < HUBS; h++) {
            sb.append("    Table TH").append(h)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), SCORE INT, STATUS VARCHAR(10)");
            sb.append(", NEXT_HUB_ID INT");
            for (int si = 0; si < SKIP_OFFSETS.length; si++) {
                sb.append(", LINK").append(si).append("_ID INT");
            }
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

        // Skip-link joins
        for (int h = 0; h < HUBS; h++) {
            for (int si = 0; si < SKIP_OFFSETS.length; si++) {
                int target = (h + SKIP_OFFSETS[si]) % HUBS;
                sb.append("    Join JLink").append(h).append("_").append(si)
                  .append("(TH").append(h).append(".LINK").append(si).append("_ID = TH").append(target).append(".ID)\n");
            }
        }

        // Satellite→Hub joins
        for (int s = 0; s < SATS; s++) {
            int hub = s / SATS_PER_HUB;
            sb.append("    Join JSat").append(s)
              .append("(TS").append(s).append(".HUB_ID = TH").append(hub).append(".ID)\n");
        }

        // Filters on even hubs
        for (int h = 0; h < HUBS; h += 2) {
            sb.append("    Filter ActiveHub").append(h)
              .append("(TH").append(h).append(".STATUS = 'ACTIVE')\n");
        }

        sb.append(")\n");

        // ---- Mappings ----
        sb.append("Mapping test::M (\n");

        // Hub mappings
        for (int h = 0; h < HUBS; h++) {
            sb.append("    test::H").append(h).append(": Relational {\n");
            if (h % 2 == 0) {
                sb.append("        ~filter [store::DB] ActiveHub").append(h).append("\n");
            }
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
            sb.append("    test::HubRing").append(h).append(": Relational { AssociationMapping (\n");
            sb.append("        nextHub").append(h).append(": [store::DB]@JHubRing").append(h).append(",\n");
            sb.append("        prevHub").append(h).append(": [store::DB]@JHubRing").append(h).append("\n");
            sb.append("    ) }\n");
        }

        // Skip-link association mappings
        for (int h = 0; h < HUBS; h++) {
            for (int si = 0; si < SKIP_OFFSETS.length; si++) {
                sb.append("    test::HubLink").append(h).append("_").append(si).append(": Relational { AssociationMapping (\n");
                sb.append("        link").append(h).append("_").append(si)
                  .append(": [store::DB]@JLink").append(h).append("_").append(si).append(",\n");
                sb.append("        back").append(h).append("_").append(si)
                  .append(": [store::DB]@JLink").append(h).append("_").append(si).append("\n");
                sb.append("    ) }\n");
            }
        }

        // Satellite→Hub association mappings
        for (int s = 0; s < SATS; s++) {
            sb.append("    test::SatHub").append(s).append(": Relational { AssociationMapping (\n");
            sb.append("        hub").append(s).append(": [store::DB]@JSat").append(s).append(",\n");
            sb.append("        sat").append(s).append(": [store::DB]@JSat").append(s).append("\n");
            sb.append("    ) }\n");
        }

        sb.append(")\n");

        String model = withRuntime(sb.toString(), "store::DB", "test::M");
        long genMs = (System.nanoTime() - t0) / 1_000_000;

        int totalAssocs = assocCount;
        int totalJoins = HUBS + (HUBS * SKIP_OFFSETS.length) + SATS;
        System.out.println("=== STRESS TEST: Dense Hub-Spoke 10K ===");
        System.out.println("Model: " + HUBS + " hubs, " + SATS + " satellites, "
                + totalAssocs + " associations (~" + (SKIP_OFFSETS.length + 1) + " per hub), "
                + totalJoins + " joins");
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

        // ---- Same 100 query patterns as sparse test ----
        var queries = new java.util.ArrayList<String>();
        for (int q = 0; q < 100; q++) {
            int h = q * (HUBS / 100);
            int s = h * SATS_PER_HUB;
            int nextH = (h + 1) % HUBS;

            switch (q % 10) {
                case 0 -> queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 1 -> queries.add("test::S" + s + ".all()->project(~[id, label, value])");
                case 2 -> queries.add("test::H" + h + ".all()->project(~[id, name, score])"
                        + "->filter(r|$r.score > 50)");
                case 3 -> queries.add("test::H" + h + ".all()->project(~[id, name, fullLabel])"
                        + "->sort(~name->ascending())");
                case 4 -> queries.add("test::H" + h + ".all()->project(~[id, fullLabel])");
                case 5 -> queries.add("test::H" + h + ".all()->project(~[id, name, nh:x|$x.nextHub"
                        + h + ".name])");
                case 6 -> queries.add("test::S" + s + ".all()->project(~[id, label, hubName:x|$x.hub"
                        + s + ".name])");
                case 7 -> queries.add("test::H" + h + ".all()->project(~[id, name, score])"
                        + "->filter(r|$r.id > 0)->sort(~score->descending())->limit(10)");
                case 8 -> queries.add("test::H" + h + ".all()->project(~[id, name, nn:x|$x.nextHub"
                        + h + ".nextHub" + nextH + ".name])");
                case 9 -> queries.add("test::H" + h + ".all()->project(~[id, name])");
            }
        }

        // ---- Diagnostics ----
        System.out.println("\n=== PER-QUERY TIMING DIAGNOSTICS ===");
        String[] diagQueries = {
            "test::H0.all()->project(~[id, name, code])",
            "test::H5.all()->project(~[id, name, nh:x|$x.nextHub5.name])",
            "test::H8.all()->project(~[id, name, nn:x|$x.nextHub8.nextHub9.name])",
        };
        for (String dq : diagQueries) {
            System.out.println("  Q: " + dq);
            long parseUs = -1, typeUs = -1, resolveUs = -1, planUs = -1;
            String phase = "parse";
            try {
                long t = System.nanoTime();
                var vs = com.gs.legend.parser.PureParser.parseQuery(dq);
                parseUs = (System.nanoTime() - t) / 1000;
                phase = "typeCheck";
                t = System.nanoTime();
                var unit = new com.gs.legend.compiler.TypeChecker(modelCtx).check(vs);
                typeUs = (System.nanoTime() - t) / 1000;
                phase = "resolve";
                t = System.nanoTime();
                var storeRes = new com.gs.legend.compiler.MappingResolver(
                        unit, normalizedMapping, builder).resolve();
                resolveUs = (System.nanoTime() - t) / 1000;
                phase = "planGen";
                t = System.nanoTime();
                new com.gs.legend.plan.PlanGenerator(
                        unit, dialect, storeRes).generate();
                planUs = (System.nanoTime() - t) / 1000;
                System.out.println("    parse=" + parseUs + "us  typeCheck=" + typeUs + "us  resolve=" + resolveUs + "us  planGen=" + planUs + "us");
            } catch (Exception e) {
                System.out.println("    FAILED in '" + phase + "': " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        System.out.println("=== END DIAGNOSTICS ===\n");

        // ---- Run all queries ----
        int passed = 0, failed = 0;
        long parseNs = 0, typeNs = 0, resolveNs = 0, planNs = 0;
        long queryStartAll = System.nanoTime();
        for (int q = 0; q < queries.size(); q++) {
            String query = queries.get(q);
            try {
                long t = System.nanoTime();
                var vs = com.gs.legend.parser.PureParser.parseQuery(query);
                parseNs += System.nanoTime() - t;
                t = System.nanoTime();
                var unit = new com.gs.legend.compiler.TypeChecker(modelCtx).check(vs);
                typeNs += System.nanoTime() - t;
                t = System.nanoTime();
                var storeRes = new com.gs.legend.compiler.MappingResolver(
                        unit, normalizedMapping, builder).resolve();
                resolveNs += System.nanoTime() - t;
                t = System.nanoTime();
                var plan = new com.gs.legend.plan.PlanGenerator(
                        unit, dialect, storeRes).generate();
                planNs += System.nanoTime() - t;
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
        System.out.printf("  Pipeline: parse=%dms  typeCheck=%dms  resolve=%dms  planGen=%dms%n",
                parseNs / 1_000_000, typeNs / 1_000_000, resolveNs / 1_000_000, planNs / 1_000_000);
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " queries failed out of " + queries.size());
    }
}
