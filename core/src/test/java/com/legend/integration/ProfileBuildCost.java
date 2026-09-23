package com.legend.integration;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Profiling test for model compile cost: parse-only vs full compileModel
 * at 100K classes (core has no global parse cache on this path — the
 * split is parse-only vs compile).
 */
@DisplayName("Profile Build Cost")
@Tag("heavy")
class ProfileBuildCost {

    @Test
    @DisplayName("100K model: parse vs build cost breakdown")
    void profileParsVsBuild() {
        int N = 100_000;
        String model = generateModel(N);

        // Parse only
        long t0 = System.nanoTime();
        var parsed = com.legend.testing.Own.model(model);
        long parseOnlyMs = (System.nanoTime() - t0) / 1_000_000;

        // Full compile (parse + build)
        long t2 = System.nanoTime();
        var ctx = com.legend.Compiler.compileModel(model);
        long compileMs = (System.nanoTime() - t2) / 1_000_000;

        long impliedBuild = Math.max(0, compileMs - parseOnlyMs);

        System.out.println("=== PROFILE: 100K Model Parse vs Build ===");
        System.out.println("Source size:         " + (model.length() / 1024) + " KB");
        System.out.println("Elements:            " + parsed.elements().size());
        System.out.println("Parse only:          " + parseOnlyMs + " ms");
        System.out.println("compileModel:        " + compileMs + " ms");
        System.out.println("Implied build cost:  " + impliedBuild + " ms");
        System.out.println("Parse fraction:      " + (compileMs > 0 ? (100 * parseOnlyMs / compileMs) + "%" : "N/A"));

        assertNotNull(ctx.findClass("test::C0").orElse(null), "Model should contain test::C0");
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
        sb.append("###Relational\nDatabase store::DB (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    Table TC").append(i).append(" (ID INT, FK0 INT)\n");
        }
        for (int i = 0; i < n; i++) {
            int target = StressTestChaotic.hash(i * 97 + 53) % n;
            if (target == i) target = (target + 1) % n;
            sb.append("    Join JL").append(i).append("(TC").append(i).append(".FK0 = TC").append(target).append(".ID)\n");
        }
        sb.append(")\n");

        sb.append("###Mapping\nMapping test::M (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    test::C").append(i).append(": Relational {\n");
            sb.append("        ~mainTable [store::DB] TC").append(i).append("\n");
            sb.append("        id: [store::DB] TC").append(i).append(".ID\n");
            sb.append("    }\n");
        }
        for (int i = 0; i < n; i++) {
            int target = StressTestChaotic.hash(i * 97 + 53) % n;
            if (target == i) target = (target + 1) % n;
            sb.append("    test::L").append(i).append(": Relational { AssociationMapping (\n");
            sb.append("        to").append(i).append(": [store::DB]@JL").append(i).append(",\n");
            sb.append("        from").append(i).append(": [store::DB]@JL").append(i).append("\n");
            sb.append("    ) }\n");
        }
        sb.append(")\n");

        sb.append("\n###Connection\nRelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }\n");
        sb.append("\n###Runtime\nRuntime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }\n");

        return sb.toString();
    }
}
