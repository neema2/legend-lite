package com.gs.legend.test;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Domain-based stress test: 20 domains × 10 classes × ~12 properties = ~200 classes, ~2400 properties.
 * 20 databases, 20 mappings, ~200 tables, ~200 joins, cross-domain associations.
 *
 * Tests parsing, typechecking, normalization, and plan generation across
 * a realistic investment banking domain model loaded from .pure resource files.
 */
@DisplayName("Domain Stress Tests")
class StressDomainTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    /** Load all .pure files from the stress/ resource directory, sorted by name. */
    private String loadStressModel() throws IOException {
        var stressUrl = getClass().getClassLoader().getResource("stress");
        assertNotNull(stressUrl, "stress/ resource directory not found on classpath");

        Path stressDir = Path.of(stressUrl.getPath());
        List<Path> pureFiles;
        try (var stream = Files.list(stressDir)) {
            pureFiles = stream
                    .filter(p -> p.toString().endsWith(".pure"))
                    .sorted()
                    .collect(Collectors.toList());
        }
        assertFalse(pureFiles.isEmpty(), "No .pure files found in stress/");

        var sb = new StringBuilder();
        for (Path f : pureFiles) {
            sb.append(Files.readString(f, StandardCharsets.UTF_8));
            sb.append("\n");
        }
        return sb.toString();
    }

    @Test
    @DisplayName("20-domain model: parse + build + normalize + plan generation via Services")
    void testDomainModel() throws Exception {
        // ---- Phase 0: Load all Pure source from resource files (model + runtime + services) ----
        long t0 = System.nanoTime();
        String model = loadStressModel();
        long loadMs = (System.nanoTime() - t0) / 1_000_000;

        System.out.println("=== DOMAIN STRESS TEST: 20 Domains (Service-based) ===");
        System.out.println("Pure source size: " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (load files): " + loadMs + " ms");

        // ---- Phase 1: Parse + build model ----
        long t1 = System.nanoTime();
        var builder = new com.gs.legend.model.PureModelBuilder().addSource(model);
        long buildMs = (System.nanoTime() - t1) / 1_000_000;
        System.out.println("Phase 1 (parse + build model): " + buildMs + " ms");

        // ---- Phase 2: Normalize all 20 mappings ----
        long t2 = System.nanoTime();
        var mappingNames = builder.resolveMappingNames("stress::RT");
        System.out.println("Resolved " + mappingNames.size() + " mappings from runtime");
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(builder, mappingNames);
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        System.out.println("Phase 2 (normalize " + mappingNames.size() + " mappings): " + normMs + " ms");

        var dialect = builder.resolveDialect("stress::RT");
        var normalizedMapping = normalizer.normalizedMapping();
        var modelCtx = normalizer.modelContext();

        // ---- Phase 3: Discover Services and execute each one ----
        var allServices = builder.getAllServices();
        var stressServices = allServices.entrySet().stream()
                .filter(e -> e.getKey().startsWith("stress::"))
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());

        assertFalse(stressServices.isEmpty(), "No stress:: Services found in model");
        System.out.println("\nDiscovered " + stressServices.size() + " Services in stress:: package");

        System.out.println("\n=== SERVICE EXECUTION ===");
        int passed = 0, failed = 0;
        long queryStartAll = System.nanoTime();
        for (var entry : stressServices) {
            var svc = entry.getValue();
            String svcName = svc.simpleName();
            String query = svc.functionBody();
            String phase = "parse";
            try {
                long qStart = System.nanoTime();
                var vs = com.gs.legend.parser.PureParser.parseQuery(query);
                long parseUs = (System.nanoTime() - qStart) / 1_000;
                phase = "typeCheck";
                long t = System.nanoTime();
                var unit = new com.gs.legend.compiler.TypeChecker(modelCtx).check(vs);
                long typeUs = (System.nanoTime() - t) / 1_000;
                phase = "resolve";
                t = System.nanoTime();
                var storeRes = new com.gs.legend.compiler.MappingResolver(
                        unit, normalizedMapping, builder).resolve();
                long resolveUs = (System.nanoTime() - t) / 1_000;
                phase = "planGen";
                t = System.nanoTime();
                var plan = new com.gs.legend.plan.PlanGenerator(
                        unit, dialect, storeRes).generate();
                long planUs = (System.nanoTime() - t) / 1_000;
                long totalUs = (System.nanoTime() - qStart) / 1_000;
                assertNotNull(plan.sql(), svcName + " produced null SQL");
                assertFalse(plan.sql().isBlank(), svcName + " produced blank SQL");
                System.out.printf("  PASS %s: SQL(%d chars) %,dμs [parse=%,d type=%,d resolve=%,d plan=%,d]%n",
                        svcName, plan.sql().length(), totalUs, parseUs, typeUs, resolveUs, planUs);
                passed++;
            } catch (Exception e) {
                System.out.println("  FAIL " + svcName + " [" + phase + "]: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
                var st = e.getStackTrace();
                for (int si = 0; si < Math.min(5, st.length); si++) {
                    System.out.println("      " + st[si]);
                }
                failed++;
            }
        }
        long queryMs = (System.nanoTime() - queryStartAll) / 1_000_000;
        long totalMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("\n" + stressServices.size() + " services: " + passed + " passed, " + failed + " failed in " + queryMs + " ms");
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " services failed out of " + stressServices.size());
    }
}
