package com.gs.legend.test;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test: 1K classes with dense connectivity + complex queries.
 * Isolates compiler/resolver/planGen per-query cost with:
 *   - Multi-property association projections (5-10 assoc columns)
 *   - 3-4 hop navigation chains
 *   - Filters on association properties
 *   - Sorts on association properties
 *   - Mixed: filter + sort + multi-hop + limit
 */
@DisplayName("Stress Tests Complex Queries")
class StressTestComplexQueries {

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

                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    /**
     * Dense 1K model: 100 hubs, each connected to 10 others via skip-links.
     * 900 satellites (9 per hub).
     * Ring: H0→H1→...→H99→H0 (nextHub).
     * Skip-links at offsets 2,3,5,7,11,13,17,19,23 (link0..link8).
     *
     * Queries exercise deep navigation, fan-out projections, and filter/sort on associations.
     */
    @Test
    @DisplayName("Complex queries on dense 1K model")
    void testComplexQueries() throws SQLException {
        int HUBS = 100;
        int SATS_PER_HUB = 9;
        int SATS = HUBS * SATS_PER_HUB;
        int N = HUBS + SATS;
        int[] SKIP_OFFSETS = {2, 3, 5, 7, 11, 13, 17, 19, 23};

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

        // Hub ring
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            sb.append("Association test::HubRing").append(h).append(" {\n");
            sb.append("    nextHub").append(h).append(": test::H").append(next).append("[0..1];\n");
            sb.append("    prevHub").append(h).append(": test::H").append(h).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // Skip-links
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

        for (int h = 0; h < HUBS; h++) {
            sb.append("    Table TH").append(h)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), SCORE INT, STATUS VARCHAR(10)");
            sb.append(", NEXT_HUB_ID INT");
            for (int si = 0; si < SKIP_OFFSETS.length; si++) {
                sb.append(", LINK").append(si).append("_ID INT");
            }
            sb.append(")\n");
        }

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

        for (int h = 0; h < HUBS; h += 2) {
            sb.append("    Filter ActiveHub").append(h)
              .append("(TH").append(h).append(".STATUS = 'ACTIVE')\n");
        }

        sb.append(")\n");

        // ---- Mappings ----
        sb.append("Mapping test::M (\n");

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

        // Skip-link association mappings
        for (int h = 0; h < HUBS; h++) {
            for (int si = 0; si < SKIP_OFFSETS.length; si++) {
                sb.append("    test::HubLink").append(h).append("_").append(si).append(": AssociationMapping (\n");
                sb.append("        link").append(h).append("_").append(si)
                  .append(": [store::DB]@JLink").append(h).append("_").append(si).append(",\n");
                sb.append("        back").append(h).append("_").append(si)
                  .append(": [store::DB]@JLink").append(h).append("_").append(si).append("\n");
                sb.append("    )\n");
            }
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

        System.out.println("=== STRESS TEST: Complex Queries on Dense 1K ===");
        System.out.println("Model: " + HUBS + " hubs, " + SATS + " satellites, "
                + assocCount + " associations (~" + (SKIP_OFFSETS.length + 1) + " per hub)");
        System.out.println("Pure source size: " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (generate source): " + genMs + " ms");

        long t1 = System.nanoTime();
        var builder = new com.gs.legend.model.PureModelBuilder().addSource(model);
        long buildMs = (System.nanoTime() - t1) / 1_000_000;
        System.out.println("Phase 1 (parse + build model): " + buildMs + " ms");

        long t2 = System.nanoTime();
        var mappingNames = builder.resolveMappingNames("test::RT");
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(builder, mappingNames);
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        System.out.println("Phase 2 (normalize " + N + " mappings): " + normMs + " ms");

        var dialect = builder.resolveDialect("test::RT");
        var normalizedMapping = normalizer.normalizedMapping();
        var modelCtx = normalizer.modelContext();

        // ---- Complex Queries ----
        // Using hub 0 as base (H0). Its ring neighbor is H1, skip-links go to H2,H3,H5,...
        // H1's ring neighbor is H2, skip-links go to H3,H4,H6,...
        var queries = new java.util.ArrayList<String>();

        // Q0: Fan-out — project 5 association properties from one class
        queries.add("test::H0.all()->project(~[id, name, "
                + "n1:x|$x.nextHub0.name, "
                + "l0:x|$x.link0_0.name, "
                + "l1:x|$x.link0_1.name, "
                + "l2:x|$x.link0_2.name, "
                + "l3:x|$x.link0_3.name])");

        // Q1: Fan-out — project 8 association properties
        queries.add("test::H0.all()->project(~[id, "
                + "n1:x|$x.nextHub0.name, "
                + "l0:x|$x.link0_0.name, "
                + "l1:x|$x.link0_1.name, "
                + "l2:x|$x.link0_2.name, "
                + "l3:x|$x.link0_3.name, "
                + "l4:x|$x.link0_4.name, "
                + "l5:x|$x.link0_5.name, "
                + "l6:x|$x.link0_6.name])");

        // Q2: 2-hop via ring: H0→H1→H2
        queries.add("test::H0.all()->project(~[id, name, hop2:x|$x.nextHub0.nextHub1.name])");

        // Q3: 3-hop via ring: H0→H1→H2→H3
        queries.add("test::H0.all()->project(~[id, hop3:x|$x.nextHub0.nextHub1.nextHub2.name])");

        // Q4: 2-hop via skip-link: H0→link0_0(=H2)→nextHub2(=H3)
        queries.add("test::H0.all()->project(~[id, mixed:x|$x.link0_0.nextHub2.name])");

        // Q5: Project association + filter on projected property
        queries.add("test::H0.all()->project(~[id, name, score, nh:x|$x.nextHub0.name])"
                + "->filter(r|$r.score > 50)");

        // Q6: Project association + sort on direct property
        queries.add("test::H0.all()->project(~[id, name, nh:x|$x.nextHub0.name])"
                + "->sort(~name->ascending())");

        // Q7: Association + filter + sort + limit
        queries.add("test::H0.all()->project(~[id, name, score, nh:x|$x.nextHub0.name, l0:x|$x.link0_0.name])"
                + "->filter(r|$r.score > 0)->sort(~name->descending())->limit(25)");

        // Q8: Satellite → hub → hub's neighbor (2-hop from satellite)
        queries.add("test::S0.all()->project(~[id, label, hubNeighbor:x|$x.hub0.nextHub0.name])");

        // Q9: Simple baseline for comparison
        queries.add("test::H0.all()->project(~[id, name, code])");

        // ---- Per-query diagnostics ----
        System.out.println("\n=== PER-QUERY TIMING DIAGNOSTICS ===");
        String[] labels = {
            "Fan-out 5 assocs", "Fan-out 8 assocs", "2-hop ring", "3-hop ring",
            "2-hop mixed", "Assoc + filter", "Assoc + sort",
            "Assoc + filter + sort + limit", "Sat→Hub→Hub (2-hop)", "Simple baseline"
        };
        for (int q = 0; q < queries.size(); q++) {
            String dq = queries.get(q);
            System.out.println("  Q" + q + " [" + labels[q] + "]: " + dq.substring(0, Math.min(80, dq.length())) + "...");
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
                var plan = new com.gs.legend.plan.PlanGenerator(
                        unit, dialect, storeRes).generate();
                planUs = (System.nanoTime() - t) / 1000;
                System.out.println("    parse=" + parseUs + "us  typeCheck=" + typeUs + "us  resolve=" + resolveUs + "us  planGen=" + planUs + "us");
                System.out.println("    SQL(" + plan.sql().length() + "chars): " + plan.sql().substring(0, Math.min(120, plan.sql().length())) + "...");
            } catch (Exception e) {
                System.out.println("    FAILED in '" + phase + "': " + e.getClass().getSimpleName() + ": " + e.getMessage());
                var st = e.getStackTrace();
                for (int si = 0; si < Math.min(8, st.length); si++) {
                    System.out.println("      " + st[si]);
                }
            }
        }
        System.out.println("=== END DIAGNOSTICS ===\n");

        // ---- Run all queries ----
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
                System.out.println("  FAIL q" + q + " [" + labels[Math.min(q, labels.length - 1)] + "]: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
                failed++;
            }
        }
        long queryMs = (System.nanoTime() - queryStartAll) / 1_000_000;
        long totalMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.println(queries.size() + " queries: " + passed + " passed, " + failed + " failed in " + queryMs + " ms");
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " queries failed out of " + queries.size());
    }
}
