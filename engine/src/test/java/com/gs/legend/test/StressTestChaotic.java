package com.gs.legend.test;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test: 50K non-uniform classes with varied property counts (1-50),
 * all supported Pure types, diverse mapping strategies, and chaotic queries.
 *
 * Property types: String, Integer, Boolean, Date, DateTime, Float, Decimal
 * SQL column types: VARCHAR, INT, BIGINT, BOOLEAN, DATE, TIMESTAMP, DOUBLE, DECIMAL
 *
 * Class shapes:
 *   - Tiny (1-3 props): id + 0-2 fields
 *   - Small (4-8 props): mixed types
 *   - Medium (9-20 props): all types represented, some with DynaFunc concat
 *   - Large (21-35 props): heavy mixed types
 *   - Huge (36-50 props): widest classes, multiple computed columns
 *
 * Mapping variety:
 *   - Direct column mappings (all types)
 *   - DynaFunc concat on string columns
 *   - EnumerationMapping on enum-bearing classes
 *   - ~filter on filtered classes
 *
 * Association graph: 1-3 random links per class + ~10% self-referential
 *
 * 200 chaotic queries: simple projects, filters on every type, sorts, limits,
 * 1-hop and 2-hop association navigation, computed columns, slices.
 */
@DisplayName("Stress Tests Chaotic")
@Tag("heavy")
class StressTestChaotic {

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

    // Deterministic hash — reproducible across runs
    static int hash(int seed) {
        seed = ((seed >>> 16) ^ seed) * 0x45d9f3b;
        seed = ((seed >>> 16) ^ seed) * 0x45d9f3b;
        seed = (seed >>> 16) ^ seed;
        return seed & 0x7FFFFFFF;
    }

    // All supported Pure types and their SQL column equivalents
    static final String[][] TYPE_MAP = {
        // { pureType, sqlColumnType }
        {"String",   "VARCHAR(200)"},
        {"Integer",  "INT"},
        {"Integer",  "BIGINT"},
        {"Boolean",  "BOOLEAN"},
        {"Date",     "DATE"},
        {"DateTime", "TIMESTAMP"},
        {"Float",    "DOUBLE"},
        {"Decimal",  "DECIMAL"},
    };

    static final String[] PROP_STEMS = {
        "nm", "lb", "cd", "ds", "tg", "nt", "tt", "rf",
        "ct", "st", "rg", "kd", "sr", "mm", "pf",
        "am", "qt", "sc", "rk", "lv", "cn", "wt",
        "pr", "vr", "rt", "sz", "dp", "ht", "wd",
        "ag", "fl", "ac", "cr", "up", "ln", "ra",
        "px", "to", "bl", "mg", "of", "sp", "du",
        "al", "be", "ga", "de", "ep", "ze", "th"
    };

    record ClassInfo(int index, String fqn, int propCount, List<String> props,
                     List<String> pureTypes, List<String> sqlCols, boolean hasEnum,
                     boolean hasFilter, boolean hasConcat) {}

    @Test
    @DisplayName("100K chaotic non-uniform classes")
    void testChaoticModel100K() throws SQLException {
        int N = 100_000;
        int NUM_QUERIES = 200;

        long t0 = System.nanoTime();
        var sb = new StringBuilder(N * 600);
        var classInfos = new ArrayList<ClassInfo>(N);

        // ---- Generate class metadata ----
        for (int i = 0; i < N; i++) {
            int h = hash(i * 7 + 13);

            // Property count: 1-50, distributed: 40% tiny(1-3), 25% small(4-8),
            // 20% medium(9-20), 10% large(21-35), 5% huge(36-50)
            int propCount;
            int bucket = h % 100;
            if (bucket < 40) propCount = 1 + (hash(i * 3) % 3);        // 1-3
            else if (bucket < 65) propCount = 4 + (hash(i * 5) % 5);   // 4-8
            else if (bucket < 85) propCount = 9 + (hash(i * 11) % 12); // 9-20
            else if (bucket < 95) propCount = 21 + (hash(i * 17) % 15);// 21-35
            else propCount = 36 + (hash(i * 23) % 15);                 // 36-50

            var props = new ArrayList<String>(propCount + 2);
            var pureTypes = new ArrayList<String>(propCount + 2);
            var sqlCols = new ArrayList<String>(propCount + 2);

            // Everyone gets id
            props.add("id");
            pureTypes.add("Integer");
            sqlCols.add("ID INT");

            for (int p = 0; p < propCount; p++) {
                int typeIdx = hash(i * 100 + p * 13) % TYPE_MAP.length;
                String pureType = TYPE_MAP[typeIdx][0];
                String sqlType = TYPE_MAP[typeIdx][1];
                String stem = PROP_STEMS[hash(i * 50 + p * 7) % PROP_STEMS.length];
                String propName = stem + p;
                String colName = stem.toUpperCase() + p;
                props.add(propName);
                pureTypes.add(pureType);
                sqlCols.add(colName + " " + sqlType);
            }

            // ~5% get an enum property
            boolean hasEnum = (hash(i * 41) % 20 == 0) && propCount >= 2;
            if (hasEnum) {
                props.add("prio");
                pureTypes.add("test::Priority");
                sqlCols.add("PRIO_VAL VARCHAR(20)");
            }

            // ~10% of medium+ get a concat computed property (only if 2+ VARCHAR cols)
            long varcharCount = sqlCols.stream().filter(c -> c.contains("VARCHAR")).count();
            boolean hasConcat = propCount >= 9 && varcharCount >= 2 && (hash(i * 59) % 10 == 0);
            if (hasConcat) {
                props.add("computed");
                pureTypes.add("String");
                // no sql col — DynaFunc
            }

            // ~8% get a mapping filter
            boolean hasFilter = (hash(i * 37) % 12 == 0);

            classInfos.add(new ClassInfo(i, "test::C" + i, propCount, props,
                    pureTypes, sqlCols, hasEnum, hasFilter, hasConcat));
        }

        // ---- Emit enum ----
        sb.append("Enum test::Priority { HIGH, MEDIUM, LOW, CRITICAL, NONE }\n\n");

        // ---- Emit classes ----
        for (var ci : classInfos) {
            sb.append("Class ").append(ci.fqn).append(" {\n");
            for (int p = 0; p < ci.props.size(); p++) {
                String type = ci.pureTypes.get(p);
                sb.append("    ").append(ci.props.get(p)).append(": ").append(type).append("[1];\n");
            }
            sb.append("}\n");
        }

        // ---- Associations: random graph ----
        // Only wire 1 link per class to keep source size manageable at 100K
        int assocCount = 0;
        var assocTarget = new int[N]; // source → single target
        Arrays.fill(assocTarget, -1);

        for (int i = 0; i < N; i++) {
            int target = hash(i * 97 + 53) % N;
            if (target == i) target = (target + 1) % N;
            assocTarget[i] = target;

            sb.append("Association test::L").append(i).append(" {\n");
            sb.append("    to").append(i).append(": test::C").append(target).append("[0..1];\n");
            sb.append("    from").append(i).append(": test::C").append(i).append("[0..1];\n");
            sb.append("}\n");
            assocCount++;
        }

        // ---- Database ----
        sb.append("Database store::DB (\n");

        for (var ci : classInfos) {
            sb.append("    Table TC").append(ci.index).append(" (");
            sb.append(String.join(", ", ci.sqlCols));
            sb.append(", FK0 INT"); // FK for association
            sb.append(")\n");
        }

        // Joins
        for (int i = 0; i < N; i++) {
            int target = assocTarget[i];
            sb.append("    Join JL").append(i)
              .append("(TC").append(i).append(".FK0 = TC").append(target).append(".ID)\n");
        }

        // Filters
        for (var ci : classInfos) {
            if (ci.hasFilter) {
                sb.append("    Filter F").append(ci.index)
                  .append("(TC").append(ci.index).append(".ID > 0)\n");
            }
        }

        sb.append(")\n");

        // ---- Mapping ----
        sb.append("Mapping test::M (\n");

        for (var ci : classInfos) {
            sb.append("    ").append(ci.fqn).append(": Relational {\n");

            if (ci.hasFilter) {
                sb.append("        ~filter [store::DB] F").append(ci.index).append("\n");
            }

            sb.append("        ~mainTable [store::DB] TC").append(ci.index).append("\n");

            for (int p = 0; p < ci.props.size(); p++) {
                String prop = ci.props.get(p);
                String type = ci.pureTypes.get(p);

                if (prop.equals("computed")) {
                    // concat two VARCHAR columns
                    String col1 = null, col2 = null;
                    for (String sc : ci.sqlCols) {
                        if (sc.contains("VARCHAR")) {
                            if (col1 == null) col1 = sc.split(" ")[0];
                            else if (col2 == null) { col2 = sc.split(" ")[0]; break; }
                        }
                    }
                    if (col1 == null) { col1 = "ID"; col2 = "ID"; }
                    if (col2 == null) col2 = col1;
                    sb.append("        ").append(prop).append(": concat([store::DB] TC").append(ci.index)
                      .append(".").append(col1).append(", '-', [store::DB] TC").append(ci.index)
                      .append(".").append(col2).append(")");
                } else if (type.equals("test::Priority")) {
                    sb.append("        ").append(prop)
                      .append(": EnumerationMapping PM: [store::DB] TC").append(ci.index)
                      .append(".PRIO_VAL");
                } else {
                    String col = (p < ci.sqlCols.size())
                            ? ci.sqlCols.get(p).split(" ")[0] : "ID";
                    sb.append("        ").append(prop).append(": [store::DB] TC").append(ci.index)
                      .append(".").append(col);
                }

                if (p < ci.props.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("    }\n");
        }

        // Association mappings
        for (int i = 0; i < N; i++) {
            sb.append("    test::L").append(i).append(": Relational { AssociationMapping (\n");
            sb.append("        to").append(i).append(": [store::DB]@JL").append(i).append(",\n");
            sb.append("        from").append(i).append(": [store::DB]@JL").append(i).append("\n");
            sb.append("    ) }\n");
        }

        // Enum mapping
        sb.append("    test::Priority: EnumerationMapping PM {\n");
        sb.append("        HIGH: ['HIGH'],\n");
        sb.append("        MEDIUM: ['MEDIUM'],\n");
        sb.append("        LOW: ['LOW'],\n");
        sb.append("        CRITICAL: ['CRITICAL'],\n");
        sb.append("        NONE: ['NONE']\n");
        sb.append("    }\n");

        sb.append(")\n");

        String model = withRuntime(sb.toString(), "store::DB", "test::M");
        long genMs = (System.nanoTime() - t0) / 1_000_000;

        // Count shape distribution
        int tiny = 0, small = 0, med = 0, large = 0, huge = 0, enumC = 0, filterC = 0, concatC = 0;
        for (var ci : classInfos) {
            if (ci.propCount <= 3) tiny++;
            else if (ci.propCount <= 8) small++;
            else if (ci.propCount <= 20) med++;
            else if (ci.propCount <= 35) large++;
            else huge++;
            if (ci.hasEnum) enumC++;
            if (ci.hasFilter) filterC++;
            if (ci.hasConcat) concatC++;
        }

        System.out.println("=== STRESS TEST: Chaotic 100K ===");
        System.out.println("Classes: " + N + " (tiny=" + tiny + ", small=" + small
                + ", med=" + med + ", large=" + large + ", huge=" + huge + ")");
        System.out.println("Enum classes: " + enumC + ", Filtered: " + filterC + ", Concat: " + concatC);
        System.out.println("Associations: " + assocCount);
        System.out.println("Pure source size: " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (generate source): " + genMs + " ms");

        // ---- Profile: isolate parse vs build ----
        com.gs.legend.model.ParseCache.global().clear();

        // Cold parse only (no model building)
        long pCold0 = System.nanoTime();
        var coldParsed = com.gs.legend.model.ParseCache.global().getOrParse(model);
        long coldParseMs = (System.nanoTime() - pCold0) / 1_000_000;

        // Warm parse only (cache hit, no model building)
        long pWarm0 = System.nanoTime();
        var warmParsed = com.gs.legend.model.ParseCache.global().getOrParse(model);
        long warmParseMs = (System.nanoTime() - pWarm0) / 1_000_000;

        System.out.println("\n--- PARSE PROFILING ---");
        System.out.println("Parse only (cold):  " + coldParseMs + " ms  (" + coldParsed.definitions().size() + " definitions)");
        System.out.println("Parse only (warm):  " + warmParseMs + " ms  (cache hit: " + (coldParsed == warmParsed) + ")");

        // Full addSource: cold (cache already warm, so parse is free — measures pure build cost)
        com.gs.legend.model.ParseCache.global().clear();
        long t1cold = System.nanoTime();
        var builder = new com.gs.legend.model.PureModelBuilder().addSource(model);
        long coldTotalMs = (System.nanoTime() - t1cold) / 1_000_000;

        // Full addSource: warm (cache hit from above cold run)
        long t1warm = System.nanoTime();
        new com.gs.legend.model.PureModelBuilder().addSource(model);
        long warmTotalMs = (System.nanoTime() - t1warm) / 1_000_000;

        System.out.println("\n--- FULL addSource PROFILING ---");
        System.out.println("addSource (cold = parse + build): " + coldTotalMs + " ms");
        System.out.println("addSource (warm = cache + build):  " + warmTotalMs + " ms");
        System.out.println("Implied parse cost:  " + (coldTotalMs - warmTotalMs) + " ms");
        System.out.println("Implied build cost:  " + warmTotalMs + " ms");
        System.out.println("Parse fraction:      " + (coldTotalMs > 0 ? (100 * (coldTotalMs - warmTotalMs) / coldTotalMs) + "%" : "N/A"));

        // ---- Phase 2: Normalize ----
        long t2 = System.nanoTime();
        var mappingNames = builder.resolveMappingNames("test::RT");
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(builder, mappingNames);
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        System.out.println("Phase 2 (normalize " + N + " mappings): " + normMs + " ms");

        var dialect = builder.resolveDialect("test::RT");
        var normalizedMapping = normalizer.normalizedMapping();
        var modelCtx = normalizer.modelContext();

        // ---- 200 chaotic queries ----
        var queries = new ArrayList<String>(NUM_QUERIES);
        for (int q = 0; q < NUM_QUERIES; q++) {
            int classIdx = hash(q * 137 + 7) % N;
            var ci = classInfos.get(classIdx);
            int target = assocTarget[classIdx];
            var targetCi = classInfos.get(target);
            int pattern = hash(q * 71 + 3) % 16;

            // Pick first prop of each type for type-specific queries
            String firstStr = null, firstInt = null, firstBool = null;
            String firstDate = null, firstFloat = null;
            for (int p = 1; p < ci.props.size(); p++) {
                String t = ci.pureTypes.get(p);
                if (firstStr == null && t.equals("String")) firstStr = ci.props.get(p);
                if (firstInt == null && t.equals("Integer")) firstInt = ci.props.get(p);
                if (firstBool == null && t.equals("Boolean")) firstBool = ci.props.get(p);
                if (firstDate == null && (t.equals("Date") || t.equals("DateTime"))) firstDate = ci.props.get(p);
                if (firstFloat == null && (t.equals("Float") || t.equals("Decimal"))) firstFloat = ci.props.get(p);
            }

            // safe 3-prop project list
            String proj3 = String.join(", ", ci.props.subList(0, Math.min(3, ci.props.size())));
            String proj2 = "id" + (ci.props.size() > 1 ? ", " + ci.props.get(1) : "");

            switch (pattern) {
                case 0 -> // Simple project: first 3 props
                    queries.add(ci.fqn + ".all()->project(~[" + proj3 + "])");

                case 1 -> // Project all props (wide)
                {
                    int cap = Math.min(ci.props.size(), 20); // cap to avoid mega-wide
                    String allProps = String.join(", ", ci.props.subList(0, cap));
                    queries.add(ci.fqn + ".all()->project(~[" + allProps + "])");
                }

                case 2 -> // Filter on id (Integer)
                    queries.add(ci.fqn + ".all()->project(~[" + proj3 + "])->filter(r|$r.id > 100)");

                case 3 -> // Sort ascending
                    queries.add(ci.fqn + ".all()->project(~[" + proj3 + "])->sort(~id->ascending())");

                case 4 -> // Sort descending + limit
                    queries.add(ci.fqn + ".all()->project(~[" + proj2 + "])->sort(~id->descending())->limit(25)");

                case 5 -> // Filter + sort + limit
                    queries.add(ci.fqn + ".all()->project(~[" + proj2
                            + "])->filter(r|$r.id > 0)->sort(~id->ascending())->limit(50)");

                case 6 -> // 1-hop association
                {
                    String tProp = targetCi.props.size() > 1 ? targetCi.props.get(1) : "id";
                    queries.add(ci.fqn + ".all()->project(~[id, linked:x|$x.to"
                            + classIdx + "." + tProp + "])");
                }

                case 7 -> // 2-hop association chain
                {
                    int mid = target;
                    int end = assocTarget[mid];
                    var endCi = classInfos.get(end);
                    String eProp = endCi.props.size() > 1 ? endCi.props.get(1) : "id";
                    queries.add(ci.fqn + ".all()->project(~[id, deep:x|$x.to"
                            + classIdx + ".to" + mid + "." + eProp + "])");
                }

                case 8 -> // Filter on String property (contains not supported, use id)
                {
                    if (firstInt != null) {
                        queries.add(ci.fqn + ".all()->project(~[id, " + firstInt
                                + "])->filter(r|$r." + firstInt + " > 42)");
                    } else {
                        queries.add(ci.fqn + ".all()->project(~[id])->filter(r|$r.id > 0)");
                    }
                }

                case 9 -> // Filter on Boolean
                {
                    if (firstBool != null) {
                        queries.add(ci.fqn + ".all()->project(~[id, " + firstBool
                                + "])->filter(r|$r." + firstBool + " == true)");
                    } else {
                        queries.add(ci.fqn + ".all()->project(~[id])->filter(r|$r.id > 0)");
                    }
                }

                case 10 -> // Computed column (DynaFunc)
                {
                    if (ci.hasConcat) {
                        queries.add(ci.fqn + ".all()->project(~[id, computed])");
                    } else {
                        queries.add(ci.fqn + ".all()->project(~[" + proj2 + "])");
                    }
                }

                case 11 -> // Float/Decimal column
                {
                    if (firstFloat != null) {
                        queries.add(ci.fqn + ".all()->project(~[id, " + firstFloat
                                + "])->filter(r|$r." + firstFloat + " > 0.5)");
                    } else {
                        queries.add(ci.fqn + ".all()->project(~[id])->filter(r|$r.id > 0)");
                    }
                }

                case 12 -> // Slice (skip + limit)
                    queries.add(ci.fqn + ".all()->project(~[" + proj2
                            + "])->sort(~id->ascending())->slice(10, 20)");

                case 13 -> // Assoc + filter
                {
                    String tProp = targetCi.props.size() > 1 ? targetCi.props.get(1) : "id";
                    queries.add(ci.fqn + ".all()->project(~[id, linked:x|$x.to"
                            + classIdx + "." + tProp + "])->filter(r|$r.id > 0)");
                }

                case 14 -> // Assoc + sort + limit
                {
                    String tProp = targetCi.props.size() > 1 ? targetCi.props.get(1) : "id";
                    queries.add(ci.fqn + ".all()->project(~[id, linked:x|$x.to"
                            + classIdx + "." + tProp + "])->sort(~id->descending())->limit(10)");
                }

                case 15 -> // Date column project
                {
                    if (firstDate != null) {
                        queries.add(ci.fqn + ".all()->project(~[id, " + firstDate + "])");
                    } else {
                        queries.add(ci.fqn + ".all()->project(~[id])");
                    }
                }

                default -> queries.add(ci.fqn + ".all()->project(~[id])");
            }
        }

        // ---- Run all queries ----
        int passed = 0, failed = 0;
        long parseNs = 0, typeNs = 0, resolveNs = 0, planNs = 0;
        long queryStart = System.nanoTime();
        for (int q = 0; q < queries.size(); q++) {
            String query = queries.get(q);
            try {
                long t = System.nanoTime();
                var vs = builder.resolveQuery(query);
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
                System.out.println("  FAIL q" + q + " [" + query.substring(0, Math.min(90, query.length()))
                        + "...]: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                failed++;
            }
        }
        long queryMs = (System.nanoTime() - queryStart) / 1_000_000;
        long totalMs = (System.nanoTime() - t0) / 1_000_000;

        System.out.println(NUM_QUERIES + " queries: " + passed + " passed, " + failed + " failed in " + queryMs + " ms");
        System.out.printf("  Pipeline: parse=%dms  typeCheck=%dms  resolve=%dms  planGen=%dms%n",
                parseNs / 1_000_000, typeNs / 1_000_000, resolveNs / 1_000_000, planNs / 1_000_000);
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " queries failed out of " + queries.size());
    }
}
