package com.gs.legend.test;

import com.gs.legend.model.ParseCache;
import com.gs.legend.model.PureModelBuilder;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Profiling test for addSource() build cost.
 * Pre-warms the parse cache, then measures rebuild from cache to isolate
 * parse vs model-building cost at 100K classes.
 */
@DisplayName("Profile Build Cost")
@Tag("heavy")
class ProfileBuildCost {

    @Test
    @DisplayName("100K model: parse vs build cost breakdown")
    void profileParsVsBuild() {
        int N = 100_000;
        String model = generateModel(N);

        // Cold parse (no cache)
        ParseCache.global().clear();
        long t0 = System.nanoTime();
        ParseCache.global().getOrParse(model);
        long coldParseMs = (System.nanoTime() - t0) / 1_000_000;

        // Warm parse (cache hit)
        long t1 = System.nanoTime();
        var cached = ParseCache.global().getOrParse(model);
        long warmParseMs = (System.nanoTime() - t1) / 1_000_000;

        // Cold addSource (parse + build)
        ParseCache.global().clear();
        long t2 = System.nanoTime();
        var builder = new PureModelBuilder().addSource(model);
        long coldTotalMs = (System.nanoTime() - t2) / 1_000_000;

        // Warm addSource (cache hit + build only)
        long t3 = System.nanoTime();
        new PureModelBuilder().addSource(model);
        long warmTotalMs = (System.nanoTime() - t3) / 1_000_000;

        long impliedParse = coldTotalMs - warmTotalMs;
        long impliedBuild = warmTotalMs;

        System.out.println("=== PROFILE: 100K Model Parse vs Build ===");
        System.out.println("Source size:         " + (model.length() / 1024) + " KB");
        System.out.println("Definitions:         " + cached.definitions().size());
        System.out.println("Parse only (cold):   " + coldParseMs + " ms");
        System.out.println("Parse only (warm):   " + warmParseMs + " ms");
        System.out.println("addSource (cold):    " + coldTotalMs + " ms");
        System.out.println("addSource (warm):    " + warmTotalMs + " ms");
        System.out.println("Implied parse cost:  " + impliedParse + " ms");
        System.out.println("Implied build cost:  " + impliedBuild + " ms");
        System.out.println("Parse fraction:      " + (coldTotalMs > 0 ? (100 * impliedParse / coldTotalMs) + "%" : "N/A"));

        assertTrue(warmParseMs <= 1, "Warm parse should be <1ms (cache hit), was " + warmParseMs);
        assertTrue(warmTotalMs < coldTotalMs, "Warm addSource should be faster than cold");
        assertNotNull(builder.findClass("test::C0").orElse(null), "Model should contain test::C0");
    }

    static String generateModel(int n) {
        var sb = new StringBuilder(n * 600);
        sb.append("Enum test::Priority { HIGH, MEDIUM, LOW, CRITICAL, NONE }\n\n");

        for (int i = 0; i < n; i++) {
            int h = StressTestChaotic.hash(i * 7 + 13);
            int bucket = h % 100;
            int propCount;
            if (bucket < 40) propCount = 1 + (StressTestChaotic.hash(i * 3) % 3);
            else if (bucket < 65) propCount = 4 + (StressTestChaotic.hash(i * 5) % 5);
            else if (bucket < 85) propCount = 9 + (StressTestChaotic.hash(i * 11) % 12);
            else if (bucket < 95) propCount = 21 + (StressTestChaotic.hash(i * 17) % 15);
            else propCount = 36 + (StressTestChaotic.hash(i * 23) % 15);

            sb.append("Class test::C").append(i).append(" {\n");
            sb.append("    id: Integer[1];\n");
            for (int p = 0; p < propCount; p++) {
                String stem = StressTestChaotic.PROP_STEMS[StressTestChaotic.hash(i * 50 + p * 7) % StressTestChaotic.PROP_STEMS.length];
                int typeIdx = StressTestChaotic.hash(i * 100 + p * 13) % StressTestChaotic.TYPE_MAP.length;
                sb.append("    ").append(stem).append(p).append(": ")
                  .append(StressTestChaotic.TYPE_MAP[typeIdx][0]).append("[1];\n");
            }
            sb.append("}\n");
        }

        // Associations
        for (int i = 0; i < n; i++) {
            int target = StressTestChaotic.hash(i * 97 + 53) % n;
            if (target == i) target = (target + 1) % n;
            sb.append("Association test::L").append(i).append(" {\n");
            sb.append("    to").append(i).append(": test::C").append(target).append("[0..1];\n");
            sb.append("    from").append(i).append(": test::C").append(i).append("[0..1];\n");
            sb.append("}\n");
        }

        // DB + Mapping + Runtime
        sb.append("Database store::DB (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    Table TC").append(i).append(" (ID INT, FK0 INT)\n");
        }
        for (int i = 0; i < n; i++) {
            int target = StressTestChaotic.hash(i * 97 + 53) % n;
            if (target == i) target = (target + 1) % n;
            sb.append("    Join JL").append(i).append("(TC").append(i).append(".FK0 = TC").append(target).append(".ID)\n");
        }
        sb.append(")\n");

        sb.append("Mapping test::M (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    test::C").append(i).append(": Relational {\n");
            sb.append("        ~mainTable [store::DB] TC").append(i).append("\n");
            sb.append("        id: [store::DB] TC").append(i).append(".ID\n");
            sb.append("    }\n");
        }
        for (int i = 0; i < n; i++) {
            int target = StressTestChaotic.hash(i * 97 + 53) % n;
            if (target == i) target = (target + 1) % n;
            sb.append("    test::L").append(i).append(": AssociationMapping (\n");
            sb.append("        to").append(i).append(": [store::DB]@JL").append(i).append(",\n");
            sb.append("        from").append(i).append(": [store::DB]@JL").append(i).append("\n");
            sb.append("    )\n");
        }
        sb.append(")\n");

        sb.append("import test::*;\n");
        sb.append("RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }\n");
        sb.append("Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }\n");

        return sb.toString();
    }
}
