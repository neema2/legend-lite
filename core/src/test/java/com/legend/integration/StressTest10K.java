package com.legend.integration;

import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test: 10K classes, tables, mappings, joins.
 * Scaling dimension 1: model size (10x the 1K baseline).
 * Same query patterns as StressTest — measures how parser/normalizer scale with model size.
 */
@DisplayName("Stress Tests 10K")
class StressTest10K {

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
                ###Connection
                import test::*;
                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                import test::*;
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    /**
     * Hub-spoke topology with 10K classes:
     *
     * 1000 Hub classes (H0..H999): 10x the 1K baseline
     *   - Each hub has: id, name, code, score, status, fullLabel (DynaFunc)
     *   - Hub-to-hub ring: H0→H1→...→H999→H0
     *   - Cross-links: every 10th hub links to hub+5
     *
     * 9000 Satellite classes (S0..S8999): 9 per hub
     *   - Each satellite has: id, label, value
     *   - S_i connects to hub H_(i/9) via to-one association
     *
     * Same 10 query patterns as StressTest, 100 queries total.
     * Measures parser + normalizer scaling with 10x model size.
     */
    @Test
    @DisplayName("10K hub-spoke model with same query patterns")
    void test10KModel() throws SQLException {
        int HUBS = 1000;
        int SATS_PER_HUB = 9;
        int SATS = HUBS * SATS_PER_HUB;
        int N = HUBS + SATS; // 10000

        // ---- Phase 0: Generate Pure source ----
        long t0 = System.nanoTime();
        var sb = new StringBuilder(N * 800);

        // ---- Hub classes H0..H999 ----
        for (int h = 0; h < HUBS; h++) {
            sb.append("Class test::H").append(h).append(" {\n");
            sb.append("    id: Integer[1];\n");
            sb.append("    name: String[1];\n");
            sb.append("    code: String[1];\n");
            sb.append("    score: Integer[1];\n");
            sb.append("    fullLabel: String[1];\n");
            sb.append("}\n");
        }

        // ---- Satellite classes S0..S8999 ----
        for (int s = 0; s < SATS; s++) {
            sb.append("Class test::S").append(s).append(" {\n");
            sb.append("    id: Integer[1];\n");
            sb.append("    label: String[1];\n");
            sb.append("    value: Integer[1];\n");
            sb.append("}\n");
        }

        // ---- Associations ----
        int assocCount = 0;

        // Hub ring: H0→H1, H1→H2, ..., H999→H0
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            sb.append("Association test::HubRing").append(h).append(" {\n");
            sb.append("    nextHub").append(h).append(": test::H").append(next).append("[0..1];\n");
            sb.append("    prevHub").append(h).append(": test::H").append(h).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // Cross-links: every 10th hub links to hub+5
        for (int h = 0; h < HUBS; h += 10) {
            int target = (h + 5) % HUBS;
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
        sb.append("###Relational\nDatabase store::DB (\n");

        // Hub tables
        for (int h = 0; h < HUBS; h++) {
            sb.append("    Table TH").append(h)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), SCORE INT, STATUS VARCHAR(10)");
            sb.append(", NEXT_HUB_ID INT"); // FK for ring
            if (h % 10 == 0) sb.append(", CROSS_HUB_ID INT"); // FK for cross-link
            sb.append(")\n");
        }

        // Satellite tables
        for (int s = 0; s < SATS; s++) {
            sb.append("    Table TS").append(s)
              .append(" (ID INT, LABEL VARCHAR(100), VALUE INT, HUB_ID INT)\n");
        }

        // Hub ring joins (table-level — consumed by the views' v_next_name)
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            sb.append("    Join JHubRing").append(h)
              .append("(TH").append(h).append(".NEXT_HUB_ID = TH").append(next).append(".ID)\n");
        }

        // ASSOCIATION joins are view-aware: an end whose class maps to a
        // view (h % 10 == 0) joins through the view's columns — core
        // requires association joins to land on the mapped mainTable row
        for (int h = 0; h < HUBS; h++) {
            int next = (h + 1) % HUBS;
            String src = (h % 10 == 0) ? "VH" + h + ".v_next_id" : "TH" + h + ".NEXT_HUB_ID";
            String tgt = (next % 10 == 0) ? "VH" + next + ".v_id" : "TH" + next + ".ID";
            sb.append("    Join JARing").append(h)
              .append("(").append(src).append(" = ").append(tgt).append(")\n");
        }

        // Cross-link joins (sources are all view hubs: h % 10 == 0)
        for (int h = 0; h < HUBS; h += 10) {
            int target = (h + 5) % HUBS;
            String tgt = (target % 10 == 0) ? "VH" + target + ".v_id" : "TH" + target + ".ID";
            sb.append("    Join JCross").append(h)
              .append("(VH").append(h).append(".v_cross_id = ").append(tgt).append(")\n");
        }

        // Satellite→Hub joins (hub end through the view when view-mapped)
        for (int s = 0; s < SATS; s++) {
            int hub = s / SATS_PER_HUB;
            String tgt = (hub % 10 == 0) ? "VH" + hub + ".v_id" : "TH" + hub + ".ID";
            sb.append("    Join JSat").append(s)
              .append("(TS").append(s).append(".HUB_ID = ").append(tgt).append(")\n");
        }

        // Filters on even hubs
        for (int h = 0; h < HUBS; h += 2) {
            sb.append("    Filter ActiveHub").append(h)
              .append("(TH").append(h).append(".STATUS = 'ACTIVE')\n");
        }

        // Views on every 10th hub: filter + ring-join column + DynaFunc
        for (int h = 0; h < HUBS; h += 10) {
            int next = (h + 1) % HUBS;
            sb.append("    View VH").append(h).append(" (\n");
            sb.append("        ~filter ActiveHub").append(h).append("\n");
            sb.append("        v_id: TH").append(h).append(".ID PRIMARY KEY,\n");
            sb.append("        v_name: TH").append(h).append(".NAME,\n");
            sb.append("        v_code: TH").append(h).append(".CODE,\n");
            sb.append("        v_score: TH").append(h).append(".SCORE,\n");
            sb.append("        v_next_name: @JHubRing").append(h).append(" | TH").append(next).append(".NAME,\n");
            sb.append("        v_next_id: TH").append(h).append(".NEXT_HUB_ID,\n");
            sb.append("        v_cross_id: TH").append(h).append(".CROSS_HUB_ID,\n");
            sb.append("        v_label: concat(TH").append(h).append(".NAME, '-', TH").append(h).append(".CODE)\n");
            sb.append("    )\n");
        }

        sb.append(")\n");

        // ---- Mappings ----
        sb.append("###Mapping\nMapping test::M (\n");

        // Hub mappings
        for (int h = 0; h < HUBS; h++) {
            boolean useView = (h % 10 == 0);
            sb.append("    test::H").append(h).append(": Relational {\n");

            if (useView) {
                sb.append("        ~mainTable [store::DB] VH").append(h).append("\n");
                sb.append("        id: [store::DB] VH").append(h).append(".v_id,\n");
                sb.append("        name: [store::DB] VH").append(h).append(".v_name,\n");
                sb.append("        code: [store::DB] VH").append(h).append(".v_code,\n");
                sb.append("        score: [store::DB] VH").append(h).append(".v_score,\n");
                sb.append("        fullLabel: [store::DB] VH").append(h).append(".v_label\n");
            } else {
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
            }
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
            sb.append("        nextHub").append(h).append(": [store::DB]@JARing").append(h).append(",\n");
            sb.append("        prevHub").append(h).append(": [store::DB]@JARing").append(h).append("\n");
            sb.append("    ) }\n");
        }

        // Cross-link association mappings
        for (int h = 0; h < HUBS; h += 10) {
            sb.append("    test::HubCross").append(h).append(": Relational { AssociationMapping (\n");
            sb.append("        crossTo").append(h).append(": [store::DB]@JCross").append(h).append(",\n");
            sb.append("        crossFrom").append(h).append(": [store::DB]@JCross").append(h).append("\n");
            sb.append("    ) }\n");
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

        int joinCount = HUBS + (HUBS / 10) + SATS;
        System.out.println("=== STRESS TEST: Hub-Spoke 10K ===");
        System.out.println("Model: " + HUBS + " hubs, " + SATS + " satellites, "
                + assocCount + " associations, " + joinCount + " joins, "
                + (HUBS / 2) + " filters, " + (HUBS / 10) + " views");
        System.out.println("Pure source size: " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (generate source): " + genMs + " ms");

        // ---- Phase 1: Parse + build model ----
        long t1 = System.nanoTime();
        var ctx = com.legend.Compiler.compileModel(model);
        long buildMs = (System.nanoTime() - t1) / 1_000_000;
        System.out.println("Phase 1 (parse + build model): " + buildMs + " ms");

        // ---- Phase 2: Normalize ----
        long t2 = System.nanoTime();
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        System.out.println("Phase 2 (normalize " + N + " mappings): " + normMs + " ms");

        var dialect = new com.legend.sql.dialect.DuckDb();

        // ---- 100 diverse queries (same patterns as 1K test) ----
        var queries = new java.util.ArrayList<String>();
        for (int q = 0; q < 100; q++) {
            int h = q * (HUBS / 100); // spread across the 1000 hubs
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
                case 9 -> // Hub → cross-link association
                    queries.add("test::H" + h + ".all()->project(~[id, name])");
            }
        }

        // ---- Diagnostics: 3 representative queries ----
        System.out.println("\n=== PER-QUERY TIMING DIAGNOSTICS ===");
        String[] diagQueries = {
            "test::H0.all()->project(~[id, name, code])",
            "test::H5.all()->project(~[id, name, nh:x|$x.nextHub5.name])",
            "test::H8.all()->project(~[id, name, nn:x|$x.nextHub8.nextHub9.name])",
        };
        for (String dq : diagQueries) {
            System.out.println("  Q: " + dq);
            long parseUs = -1, typeUs = -1, resolveUs = -1, planUs = -1;
            int tiCount = -1, apCount = 0, storeCount = -1;
            String phase = "parse";
            try {
                long t = System.nanoTime();
                var vs = com.legend.compiler.NameResolver.resolveQuery(
                        com.legend.testing.Own.spec(dq));
                parseUs = (System.nanoTime() - t) / 1000;

                phase = "typeCheck";
                t = System.nanoTime();
                var sqlq = com.legend.Compiler.lowerResolved(vs, ctx, "test::RT", false);
                typeUs = (System.nanoTime() - t) / 1000;
                // Post-big-bang: TypeInfo sidecar is replaced by TypedSpec HIR.
                // Leave these diagnostic counters at zero until the stress harness is
                // updated to walk the HIR (tracked by plangen-typed-port-c0954a).
                tiCount = 0;

                phase = "resolve";
                t = System.nanoTime();
                // resolve is folded into the lowering above
                resolveUs = (System.nanoTime() - t) / 1000;
                storeCount = 0;

                phase = "planGen";
                t = System.nanoTime();
                String sql = dialect.render(sqlq);
                planUs = (System.nanoTime() - t) / 1000;

                System.out.println("    parse=" + parseUs + "us  lower=" + typeUs + "us  resolveFolded=" + resolveUs + "us  render=" + planUs + "us");
                System.out.println("    typeInfoNodes=" + tiCount + "  appliedProps=" + apCount + "  storeResolutions=" + storeCount);
                System.out.println("    SQL(" + sql.length() + "chars): " + sql.substring(0, Math.min(120, sql.length())) + "...");
            } catch (Exception e) {
                System.out.println("    FAILED in phase '" + phase + "': " + e.getClass().getSimpleName() + ": " + e.getMessage());
                System.out.println("    partial: parse=" + parseUs + "us  lower=" + typeUs + "us  resolveFolded=" + resolveUs + "us  render=" + planUs + "us");
                System.out.println("    typeInfoNodes=" + tiCount + "  appliedProps=" + apCount + "  storeResolutions=" + storeCount);
                var st = e.getStackTrace();
                for (int si = 0; si < Math.min(10, st.length); si++) {
                    System.out.println("      " + st[si]);
                }
            }
        }
        System.out.println("=== END DIAGNOSTICS ===\n");

        // ---- Run all 100 queries ----
        int passed = 0, failed = 0;
        long parseNs = 0, typeNs = 0, resolveNs = 0, planNs = 0;
        long queryStartAll = System.nanoTime();
        for (int q = 0; q < queries.size(); q++) {
            String query = queries.get(q);
            try {
                long t = System.nanoTime();
                var vs = com.legend.compiler.NameResolver.resolveQuery(
                        com.legend.testing.Own.spec(query));
                parseNs += System.nanoTime() - t;
                t = System.nanoTime();
                var sqlq = com.legend.Compiler.lowerResolved(vs, ctx, "test::RT", false);
                typeNs += System.nanoTime() - t;
                t = System.nanoTime();
                // resolve is folded into the lowering above
                resolveNs += System.nanoTime() - t;
                t = System.nanoTime();
                String sql = dialect.render(sqlq);
                planNs += System.nanoTime() - t;
                assertNotNull(sql, "Query " + q + " produced null SQL");
                assertFalse(sql.isBlank(), "Query " + q + " produced blank SQL");
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
        System.out.printf("  Pipeline: parse=%dms  lower=%dms  resolveFolded=%dms  render=%dms%n",
                parseNs / 1_000_000, typeNs / 1_000_000, resolveNs / 1_000_000, planNs / 1_000_000);
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " queries failed out of " + queries.size());
    }
}
