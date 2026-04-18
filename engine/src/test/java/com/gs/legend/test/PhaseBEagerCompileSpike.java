package com.gs.legend.test;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.MappingNormalizer;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.def.ClassDefinition;
import com.gs.legend.model.def.FunctionDefinition;
import com.gs.legend.model.def.PackageableElement;
import com.gs.legend.parser.PureParser;
import com.gs.legend.plan.GenericType;

import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Phase B measurement spike — throw-away test that answers
 * "how expensive is pre-type-checking every element at build time, including
 * all the tricky things (join chains, DynaFuncs, views, M2M, embedded, derived
 * properties, constraints, user functions)?"
 *
 * <p>Four topologies at 10K, all @Tag("spike"):
 * <ol>
 *   <li>{@code spike_hubSpoke_10K} — minimal baseline (simple column mappings)</li>
 *   <li>{@code spike_linearChain_10K} — pathological linear chain topology</li>
 *   <li>{@code spike_chaoticLite_10K} — heterogeneous property shapes</li>
 *   <li>{@code spike_kitchenSink_10K} — <b>THE REAL TEST</b>: every feature mixed in</li>
 * </ol>
 *
 * <p>Measurement phases (each scenario):
 * <ol>
 *   <li>generate source</li>
 *   <li>parse + build</li>
 *   <li>normalize</li>
 *   <li><b>3a: eager TC — mapping sourceSpecs</b> (relational + M2M)</li>
 *   <li><b>3b: eager TC — derived property bodies</b> (re-parse required — bodies not retained)</li>
 *   <li><b>3c: eager TC — constraint bodies</b> (re-parse required — bodies not retained)</li>
 *   <li><b>3d: eager TC — user function bodies</b> (resolvedBody pre-parsed by builder)</li>
 *   <li>cold queries (fresh TypeChecker) vs warm queries (pre-compiled TypeChecker)</li>
 * </ol>
 *
 * <p>Run with:
 * <pre>mvn test -pl engine -Dtest=PhaseBEagerCompileSpike</pre>
 *
 * <p>See {@code /Users/neema/.windsurf/plans/phase-b-measurement-spike-10k-c0954a.md}.
 */
@DisplayName("Phase B Eager Compile Spike 10K")
@Tag("spike")
class PhaseBEagerCompileSpike {

    private static final int N = 10_000;

    // =============================================================================================
    // Scenarios
    // =============================================================================================

    @Test
    @DisplayName("spike_hubSpoke_10K (minimal baseline)")
    void spike_hubSpoke_10K() {
        long t0 = System.nanoTime();
        String model = generateHubSpoke(N);
        long genMs = (System.nanoTime() - t0) / 1_000_000;
        runSpike("hub-spoke 10K (minimal)", model, hubSpokeQueries(N, 100), genMs);
    }

    @Test
    @DisplayName("spike_linearChain_10K")
    void spike_linearChain_10K() {
        long t0 = System.nanoTime();
        String model = generateLinearChain(N);
        long genMs = (System.nanoTime() - t0) / 1_000_000;
        runSpike("linear chain 10K", model, linearChainQueries(N, 100), genMs);
    }

    @Test
    @DisplayName("spike_chaoticLite_10K")
    void spike_chaoticLite_10K() {
        long t0 = System.nanoTime();
        String model = generateChaoticLite(N);
        long genMs = (System.nanoTime() - t0) / 1_000_000;
        runSpike("chaotic-lite 10K", model, chaoticLiteQueries(N, 100), genMs);
    }

    @Test
    @DisplayName("spike_kitchenSink_10K (ALL features)")
    void spike_kitchenSink_10K() {
        long t0 = System.nanoTime();
        String model = generateKitchenSink(N);
        long genMs = (System.nanoTime() - t0) / 1_000_000;
        runSpike("kitchen-sink 10K (ALL features)", model, kitchenSinkQueries(N, 100), genMs);
    }

    // =============================================================================================
    // Shared measurement pipeline
    // =============================================================================================

    private void runSpike(String label, String model, List<String> queries, long genMs) {
        System.out.println();
        System.out.println("================================================================");
        System.out.println("=== Phase B Spike: " + label);
        System.out.println("================================================================");
        System.out.println("Source size:                            " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (generate source):              " + genMs + " ms");

        // ---- Phase 1: Parse + build ----
        long t1 = System.nanoTime();
        var builder = new PureModelBuilder().addSource(model);
        long buildMs = (System.nanoTime() - t1) / 1_000_000;
        int classCount = builder.getAllClasses().size();
        int assocCount = builder.getAllAssociations().size();
        int enumCount = builder.getAllEnums().size();
        System.out.println("Phase 1 (parse + build):                " + buildMs + " ms  "
                + "[" + classCount + " classes, " + assocCount + " assocs, " + enumCount + " enums]");

        // ---- Phase 2: Normalize ----
        long t2 = System.nanoTime();
        var mappingNames = builder.resolveMappingNames("test::RT");
        var normalizer = new MappingNormalizer(builder, mappingNames);
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        var modelCtx = normalizer.modelContext();
        System.out.println("Phase 2 (normalize):                    " + normMs + " ms");

        // ---- Phase 3a: EAGER TC — MAPPING sourceSpecs (relational + M2M) ----
        long memBefore = usedMemoryMb();
        long t3a = System.nanoTime();
        var warmTc = new TypeChecker(modelCtx);
        int relCompiled = 0, m2mCompiled = 0;
        long relTotalNs = 0, m2mTotalNs = 0;
        long slowestNs = 0;
        String slowestClass = null;
        String slowestKind = null;
        for (var pureClass : builder.getAllClasses().values()) {
            String fqn = pureClass.qualifiedName();
            var specOpt = modelCtx.findSourceSpec(fqn);
            if (specOpt.isEmpty()) continue;
            ValueSpecification sourceSpec = specOpt.get();
            String kind = builder.getMappingRegistry().findPureClassMapping(fqn).isPresent()
                    ? "M2M" : "relational";
            long s = System.nanoTime();
            warmTc.compileExpr(sourceSpec, new TypeChecker.CompilationContext());
            long d = System.nanoTime() - s;
            if (kind.equals("relational")) { relCompiled++; relTotalNs += d; }
            else { m2mCompiled++; m2mTotalNs += d; }
            if (d > slowestNs) {
                slowestNs = d;
                slowestClass = pureClass.qualifiedName();
                slowestKind = kind;
            }
        }
        long phase3aMs = (System.nanoTime() - t3a) / 1_000_000;
        long memAfter = usedMemoryMb();
        long relAvgUs = relCompiled > 0 ? (relTotalNs / relCompiled) / 1_000 : 0;
        long m2mAvgUs = m2mCompiled > 0 ? (m2mTotalNs / m2mCompiled) / 1_000 : 0;
        System.out.println("Phase 3a (eager TC mappings):           " + phase3aMs + " ms  "
                + "[" + relCompiled + " relational @ " + relAvgUs + " μs avg, "
                + m2mCompiled + " M2M @ " + m2mAvgUs + " μs avg, "
                + "slowest " + (slowestNs / 1_000) + " μs @ " + slowestKind + " " + slowestClass + "]");
        System.out.println("  heap delta:                           ~" + (memAfter - memBefore) + " MB");

        // ---- Phase 3b/3c: Re-parse source to access derived properties + constraints ----
        // PureClass doesn't retain derived/constraint bodies — they live on ClassDefinition
        // which PureModelBuilder clears after build. Re-parsing is the cleanest workaround
        // for the spike. In a real Phase B these would be type-checked INSIDE model-build,
        // so the re-parse cost here represents the measurement overhead, not production cost.
        long t3reparse = System.nanoTime();
        List<PackageableElement> parsedDefs = PureParser.parseModel(model);
        var classDefs = new ArrayList<ClassDefinition>();
        for (var def : parsedDefs) {
            if (def instanceof ClassDefinition cd) classDefs.add(cd);
        }
        long reparseMs = (System.nanoTime() - t3reparse) / 1_000_000;

        // ---- Phase 3b: EAGER TC — DERIVED PROPERTY bodies ----
        long t3b = System.nanoTime();
        int derivedCount = 0;
        long derivedTotalNs = 0;
        int derivedFail = 0;
        long slowestDerivedNs = 0;
        String slowestDerivedWhere = null;
        for (var cd : classDefs) {
            for (var dp : cd.derivedProperties()) {
                try {
                    var body = PureParser.parseQuery(dp.expression());
                    var thisType = new GenericType.ClassType(cd.qualifiedName());
                    var ctx = new TypeChecker.CompilationContext().withLambdaParam("this", thisType);
                    long s = System.nanoTime();
                    warmTc.compileExpr(body, ctx);
                    long d = System.nanoTime() - s;
                    derivedTotalNs += d;
                    if (d > slowestDerivedNs) {
                        slowestDerivedNs = d;
                        slowestDerivedWhere = cd.qualifiedName() + "." + dp.name();
                    }
                    derivedCount++;
                } catch (Exception e) {
                    derivedFail++;
                }
            }
        }
        long phase3bMs = (System.nanoTime() - t3b) / 1_000_000;
        long derivedAvgUs = derivedCount > 0 ? (derivedTotalNs / derivedCount) / 1_000 : 0;
        System.out.println("Phase 3b (eager TC derived props):      " + phase3bMs + " ms  "
                + "[" + derivedCount + " derived @ " + derivedAvgUs + " μs avg, "
                + derivedFail + " fail, "
                + (slowestDerivedWhere != null
                        ? "slowest " + (slowestDerivedNs / 1_000) + " μs @ " + slowestDerivedWhere
                        : "no derived props") + "]");

        // ---- Phase 3c: EAGER TC — CONSTRAINT bodies ----
        long t3c = System.nanoTime();
        int constraintCount = 0;
        long constraintTotalNs = 0;
        int constraintFail = 0;
        for (var cd : classDefs) {
            for (var cons : cd.constraints()) {
                try {
                    var body = PureParser.parseQuery(cons.expression());
                    var thisType = new GenericType.ClassType(cd.qualifiedName());
                    var ctx = new TypeChecker.CompilationContext().withLambdaParam("this", thisType);
                    long s = System.nanoTime();
                    warmTc.compileExpr(body, ctx);
                    constraintTotalNs += System.nanoTime() - s;
                    constraintCount++;
                } catch (Exception e) {
                    constraintFail++;
                }
            }
        }
        long phase3cMs = (System.nanoTime() - t3c) / 1_000_000;
        long consAvgUs = constraintCount > 0 ? (constraintTotalNs / constraintCount) / 1_000 : 0;
        System.out.println("Phase 3c (eager TC constraints):        " + phase3cMs + " ms  "
                + "[" + constraintCount + " constraints @ " + consAvgUs + " μs avg, "
                + constraintFail + " fail]");

        // ---- Phase 3d: EAGER TC — USER FUNCTION bodies ----
        // User functions are pre-parsed by PureModelBuilder (resolvedBody populated).
        // We just need to type-check each statement.
        long t3d = System.nanoTime();
        int funcCount = 0;
        long funcTotalNs = 0;
        int funcFail = 0;
        var funcDefs = new ArrayList<FunctionDefinition>();
        for (var def : parsedDefs) {
            if (def instanceof FunctionDefinition fd) funcDefs.add(fd);
        }
        for (var fd : funcDefs) {
            try {
                // Look up the resolved function from the builder (body already parsed + name-resolved)
                var resolved = builder.findFunction(fd.qualifiedName());
                if (resolved.isEmpty()) continue;
                var fn = resolved.get(0);
                if (fn.resolvedBody() == null) continue;
                // Build context with parameters bound
                var ctx = new TypeChecker.CompilationContext();
                for (var param : fn.parameters()) {
                    GenericType paramType = mapTypeName(param.type());
                    ctx = ctx.withLambdaParam(param.name(), paramType);
                }
                long s = System.nanoTime();
                for (var stmt : fn.resolvedBody()) {
                    warmTc.compileExpr(stmt, ctx);
                }
                funcTotalNs += System.nanoTime() - s;
                funcCount++;
            } catch (Exception e) {
                funcFail++;
            }
        }
        long phase3dMs = (System.nanoTime() - t3d) / 1_000_000;
        long fnAvgUs = funcCount > 0 ? (funcTotalNs / funcCount) / 1_000 : 0;
        System.out.println("Phase 3d (eager TC user functions):     " + phase3dMs + " ms  "
                + "[" + funcCount + " functions @ " + fnAvgUs + " μs avg, "
                + funcFail + " fail]");

        // ---- Phase 4a: COLD queries (fresh TypeChecker per query) ----
        long coldNs = 0;
        int coldOk = 0, coldFail = 0;
        for (String q : queries) {
            try {
                var vs = PureParser.parseQuery(q);
                long s = System.nanoTime();
                new TypeChecker(modelCtx).check(vs);
                coldNs += System.nanoTime() - s;
                coldOk++;
            } catch (Exception e) {
                coldFail++;
            }
        }
        long coldUsPerQuery = coldOk > 0 ? (coldNs / coldOk) / 1_000 : 0;
        System.out.println("Phase 4a (cold queries, fresh TC):      " + (coldNs / 1_000_000) + " ms  "
                + "[" + queries.size() + " queries, avg " + coldUsPerQuery + " μs/q, "
                + coldOk + " ok, " + coldFail + " fail]");

        // ---- Phase 4b: WARM queries (shared TypeChecker) ----
        long warmNs = 0;
        int warmOk = 0, warmFail = 0;
        for (String q : queries) {
            try {
                var vs = PureParser.parseQuery(q);
                long s = System.nanoTime();
                warmTc.check(vs);
                warmNs += System.nanoTime() - s;
                warmOk++;
            } catch (Exception e) {
                warmFail++;
            }
        }
        long warmUsPerQuery = warmOk > 0 ? (warmNs / warmOk) / 1_000 : 0;
        long amortUs = Math.max(coldUsPerQuery - warmUsPerQuery, 0);
        double amortFactor = warmUsPerQuery > 0 ? (double) coldUsPerQuery / warmUsPerQuery : 0;
        System.out.println("Phase 4b (warm queries, shared TC):     " + (warmNs / 1_000_000) + " ms  "
                + "[" + queries.size() + " queries, avg " + warmUsPerQuery + " μs/q, "
                + warmOk + " ok, " + warmFail + " fail]");
        System.out.println("  amortization:                         "
                + amortUs + " μs/q saved, "
                + String.format("%.2f", amortFactor) + "x faster");

        // Total eager TC (everything we'd need at build time for Phase B eager mode)
        long eagerTotalMs = phase3aMs + phase3bMs + phase3cMs + phase3dMs;
        System.out.println("──");
        System.out.println("Phase 3-total (ALL eager TC):           " + eagerTotalMs + " ms  "
                + "[+ " + reparseMs + " ms re-parse for derived/constraints — measurement overhead only]");
        long buildEndToEndMs = buildMs + normMs + eagerTotalMs;
        System.out.println("Phase 1+2+3 (model fully compiled):     " + buildEndToEndMs + " ms");
        System.out.println();

        Assertions.assertTrue(relCompiled + m2mCompiled > 0,
                "expected at least one sourceSpec to be compiled");
        Assertions.assertEquals(0, coldFail, coldFail + " cold queries failed");
    }

    /** Map a Pure primitive/class type name to a GenericType for CompilationContext binding. */
    private static GenericType mapTypeName(String pureType) {
        return switch (pureType) {
            case "String" -> GenericType.Primitive.STRING;
            case "Integer" -> GenericType.Primitive.INTEGER;
            case "Boolean" -> GenericType.Primitive.BOOLEAN;
            case "Float" -> GenericType.Primitive.FLOAT;
            case "Decimal" -> GenericType.DEFAULT_DECIMAL;
            case "Date" -> GenericType.Primitive.DATE;
            case "DateTime" -> GenericType.Primitive.DATE_TIME;
            case "StrictDate" -> GenericType.Primitive.STRICT_DATE;
            case "Number" -> GenericType.Primitive.NUMBER;
            case "Any" -> GenericType.Primitive.ANY;
            default -> new GenericType.ClassType(pureType); // user class
        };
    }

    private static long usedMemoryMb() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    private static String withRuntime(String model, String dbName, String mappingName) {
        return model + """

                import test::*;

                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::RT { mappings: [ %s ]; connections: [ %s: [ environment: store::Conn ] ]; }
                """.formatted(mappingName, dbName);
    }

    // =============================================================================================
    // Generator: hub-spoke minimal (reference floor)
    // =============================================================================================

    private static String generateHubSpoke(int total) {
        int hubs = total / 10;
        int satsPerHub = 9;
        int sats = hubs * satsPerHub;
        var sb = new StringBuilder(total * 600);

        for (int h = 0; h < hubs; h++) {
            sb.append("Class test::H").append(h).append(" {\n")
              .append("    id: Integer[1];\n    name: String[1];\n    code: String[1];\n}\n");
        }
        for (int s = 0; s < sats; s++) {
            sb.append("Class test::S").append(s).append(" {\n")
              .append("    id: Integer[1];\n    label: String[1];\n}\n");
        }
        for (int h = 0; h < hubs; h++) {
            int next = (h + 1) % hubs;
            sb.append("Association test::HubRing").append(h).append(" {\n")
              .append("    nextHub").append(h).append(": test::H").append(next).append("[0..1];\n")
              .append("    prevHub").append(h).append(": test::H").append(h).append("[0..1];\n}\n");
        }
        for (int s = 0; s < sats; s++) {
            int hub = s / satsPerHub;
            sb.append("Association test::SatHub").append(s).append(" {\n")
              .append("    hub").append(s).append(": test::H").append(hub).append("[0..1];\n")
              .append("    sat").append(s).append(": test::S").append(s).append("[0..1];\n}\n");
        }
        sb.append("Database store::DB (\n");
        for (int h = 0; h < hubs; h++) {
            sb.append("    Table TH").append(h)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), NEXT_HUB_ID INT)\n");
        }
        for (int s = 0; s < sats; s++) {
            sb.append("    Table TS").append(s)
              .append(" (ID INT, LABEL VARCHAR(100), HUB_ID INT)\n");
        }
        for (int h = 0; h < hubs; h++) {
            int next = (h + 1) % hubs;
            sb.append("    Join JHubRing").append(h)
              .append("(TH").append(h).append(".NEXT_HUB_ID = TH").append(next).append(".ID)\n");
        }
        for (int s = 0; s < sats; s++) {
            int hub = s / satsPerHub;
            sb.append("    Join JSat").append(s)
              .append("(TS").append(s).append(".HUB_ID = TH").append(hub).append(".ID)\n");
        }
        sb.append(")\nMapping test::M (\n");
        for (int h = 0; h < hubs; h++) {
            sb.append("    test::H").append(h).append(": Relational {\n")
              .append("        ~mainTable [store::DB] TH").append(h).append("\n")
              .append("        id: [store::DB] TH").append(h).append(".ID,\n")
              .append("        name: [store::DB] TH").append(h).append(".NAME,\n")
              .append("        code: [store::DB] TH").append(h).append(".CODE\n    }\n");
        }
        for (int s = 0; s < sats; s++) {
            sb.append("    test::S").append(s).append(": Relational {\n")
              .append("        ~mainTable [store::DB] TS").append(s).append("\n")
              .append("        id: [store::DB] TS").append(s).append(".ID,\n")
              .append("        label: [store::DB] TS").append(s).append(".LABEL\n    }\n");
        }
        for (int h = 0; h < hubs; h++) {
            sb.append("    test::HubRing").append(h).append(": Relational { AssociationMapping (\n")
              .append("        nextHub").append(h).append(": [store::DB]@JHubRing").append(h).append(",\n")
              .append("        prevHub").append(h).append(": [store::DB]@JHubRing").append(h).append("\n    ) }\n");
        }
        for (int s = 0; s < sats; s++) {
            sb.append("    test::SatHub").append(s).append(": Relational { AssociationMapping (\n")
              .append("        hub").append(s).append(": [store::DB]@JSat").append(s).append(",\n")
              .append("        sat").append(s).append(": [store::DB]@JSat").append(s).append("\n    ) }\n");
        }
        sb.append(")\n");
        return withRuntime(sb.toString(), "store::DB", "test::M");
    }

    private static List<String> hubSpokeQueries(int total, int count) {
        int hubs = total / 10;
        int satsPerHub = 9;
        var queries = new ArrayList<String>(count);
        for (int q = 0; q < count; q++) {
            int h = q * (hubs / count);
            int s = h * satsPerHub;
            switch (q % 5) {
                case 0 -> queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 1 -> queries.add("test::S" + s + ".all()->project(~[id, label])");
                case 2 -> queries.add("test::H" + h + ".all()->project(~[id, name])->filter(r|$r.id > 0)");
                case 3 -> queries.add("test::H" + h + ".all()->project(~[id, name, nh:x|$x.nextHub"
                        + h + ".name])");
                case 4 -> queries.add("test::S" + s + ".all()->project(~[id, label, hn:x|$x.hub"
                        + s + ".name])");
            }
        }
        return queries;
    }

    // =============================================================================================
    // Generator: linear chain
    // =============================================================================================

    private static String generateLinearChain(int n) {
        var sb = new StringBuilder(n * 500);
        for (int i = 0; i < n; i++) {
            sb.append("Class test::C").append(i).append(" {\n")
              .append("    id: Integer[1];\n    name: String[1];\n}\n");
        }
        int chainLen = n - 1;
        for (int i = 0; i < chainLen; i++) {
            sb.append("Association test::L").append(i).append(" {\n")
              .append("    next").append(i).append(": test::C").append(i + 1).append("[0..1];\n")
              .append("    prev").append(i).append(": test::C").append(i).append("[0..1];\n}\n");
        }
        sb.append("Database store::DB (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    Table TC").append(i).append(" (ID INT, NAME VARCHAR(100), NEXT_ID INT)\n");
        }
        for (int i = 0; i < chainLen; i++) {
            sb.append("    Join JL").append(i)
              .append("(TC").append(i).append(".NEXT_ID = TC").append(i + 1).append(".ID)\n");
        }
        sb.append(")\nMapping test::M (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    test::C").append(i).append(": Relational {\n")
              .append("        ~mainTable [store::DB] TC").append(i).append("\n")
              .append("        id: [store::DB] TC").append(i).append(".ID,\n")
              .append("        name: [store::DB] TC").append(i).append(".NAME\n    }\n");
        }
        for (int i = 0; i < chainLen; i++) {
            sb.append("    test::L").append(i).append(": Relational { AssociationMapping (\n")
              .append("        next").append(i).append(": [store::DB]@JL").append(i).append(",\n")
              .append("        prev").append(i).append(": [store::DB]@JL").append(i).append("\n    ) }\n");
        }
        sb.append(")\n");
        return withRuntime(sb.toString(), "store::DB", "test::M");
    }

    private static List<String> linearChainQueries(int n, int count) {
        var queries = new ArrayList<String>(count);
        for (int q = 0; q < count; q++) {
            int i = q * (n / count);
            switch (q % 4) {
                case 0 -> queries.add("test::C" + i + ".all()->project(~[id, name])");
                case 1 -> {
                    int safe = Math.min(i, n - 2);
                    queries.add("test::C" + safe + ".all()->project(~[id, name, "
                            + "nx:x|$x.next" + safe + ".name])");
                }
                case 2 -> queries.add("test::C" + i + ".all()->project(~[id, name])->filter(r|$r.id > 0)");
                case 3 -> {
                    int safe = Math.min(i, n - 3);
                    queries.add("test::C" + safe + ".all()->project(~[id, "
                            + "nn:x|$x.next" + safe + ".next" + (safe + 1) + ".name])");
                }
            }
        }
        return queries;
    }

    // =============================================================================================
    // Generator: chaotic-lite
    // =============================================================================================

    private static int hash(int seed) {
        seed = ((seed >>> 16) ^ seed) * 0x45d9f3b;
        seed = ((seed >>> 16) ^ seed) * 0x45d9f3b;
        return (seed >>> 16) ^ seed & 0x7FFFFFFF;
    }

    private static final String[][] TYPE_MAP = {
            {"String",   "VARCHAR(200)"},
            {"Integer",  "INT"},
            {"Boolean",  "BOOLEAN"},
            {"Float",    "DOUBLE"},
    };

    private static final String[] PROP_STEMS = {
            "nm", "lb", "cd", "ds", "tg", "nt", "tt", "rf", "ct", "st",
            "rg", "kd", "sr", "mm", "pf", "am", "qt", "sc", "rk", "lv"
    };

    private static String generateChaoticLite(int n) {
        var sb = new StringBuilder(n * 400);
        int[][] classProps = new int[n][];
        String[][] propNames = new String[n][];
        int[] assocTarget = new int[n];

        for (int i = 0; i < n; i++) {
            int h = hash(i * 7 + 13);
            int propCount = 1 + (h % 10);
            classProps[i] = new int[propCount];
            propNames[i] = new String[propCount];
            for (int p = 0; p < propCount; p++) {
                int th = hash(i * 31 + p * 17);
                classProps[i][p] = th % TYPE_MAP.length;
                propNames[i][p] = PROP_STEMS[(th / 7) % PROP_STEMS.length] + p;
            }
            int at = hash(i * 11 + 1) % n;
            if (at == i) at = (at + 1) % n;
            assocTarget[i] = at;
        }

        for (int i = 0; i < n; i++) {
            sb.append("Class test::C").append(i).append(" {\n    id: Integer[1];\n");
            for (int p = 0; p < propNames[i].length; p++) {
                sb.append("    ").append(propNames[i][p]).append(": ")
                  .append(TYPE_MAP[classProps[i][p]][0]).append("[1];\n");
            }
            sb.append("}\n");
        }
        for (int i = 0; i < n; i++) {
            int t = assocTarget[i];
            sb.append("Association test::A").append(i).append(" {\n")
              .append("    rel").append(i).append(": test::C").append(t).append("[0..1];\n")
              .append("    back").append(i).append(": test::C").append(i).append("[0..1];\n}\n");
        }
        sb.append("Database store::DB (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    Table TC").append(i).append(" (ID INT");
            for (int p = 0; p < propNames[i].length; p++) {
                sb.append(", ").append(propNames[i][p].toUpperCase()).append(' ')
                  .append(TYPE_MAP[classProps[i][p]][1]);
            }
            sb.append(", REL_ID INT)\n");
        }
        for (int i = 0; i < n; i++) {
            int t = assocTarget[i];
            sb.append("    Join JA").append(i)
              .append("(TC").append(i).append(".REL_ID = TC").append(t).append(".ID)\n");
        }
        sb.append(")\nMapping test::M (\n");
        for (int i = 0; i < n; i++) {
            sb.append("    test::C").append(i).append(": Relational {\n")
              .append("        ~mainTable [store::DB] TC").append(i).append("\n")
              .append("        id: [store::DB] TC").append(i).append(".ID");
            for (int p = 0; p < propNames[i].length; p++) {
                sb.append(",\n        ").append(propNames[i][p]).append(": [store::DB] TC")
                  .append(i).append('.').append(propNames[i][p].toUpperCase());
            }
            sb.append("\n    }\n");
        }
        for (int i = 0; i < n; i++) {
            sb.append("    test::A").append(i).append(": Relational { AssociationMapping (\n")
              .append("        rel").append(i).append(": [store::DB]@JA").append(i).append(",\n")
              .append("        back").append(i).append(": [store::DB]@JA").append(i).append("\n    ) }\n");
        }
        sb.append(")\n");
        return withRuntime(sb.toString(), "store::DB", "test::M");
    }

    private static List<String> chaoticLiteQueries(int n, int count) {
        var queries = new ArrayList<String>(count);
        for (int q = 0; q < count; q++) {
            int i = q * (n / count);
            switch (q % 3) {
                case 0 -> queries.add("test::C" + i + ".all()->project(~[id])");
                case 1 -> queries.add("test::C" + i + ".all()->project(~[id])->filter(r|$r.id > 0)");
                case 2 -> queries.add("test::C" + i + ".all()->project(~[id, rn:x|$x.rel" + i + ".id])");
            }
        }
        return queries;
    }

    // =============================================================================================
    // Generator: kitchen-sink — EVERY feature mixed in
    //
    // Feature distribution by hub bucket (hub % 10):
    //   0: ~filter in mapping (references Database Filter)
    //   1: DynaFunc concat + toUpper + plus in property mappings
    //   2: Multi-hop join chain in property mapping (@J1 > @J2)
    //   3: Plain mapping + derived property on class
    //   4: Plain mapping + constraint on class
    //   5: View-backed (view has ~filter + ~distinct + join chain + DynaFunc)
    //   6: Plain mapping (reserved / baseline within kitchen-sink)
    //   7: M2M from Upstream raw class (Pure mapping with ~src + property expressions)
    //   8: Enum-typed property + EnumerationMapping reference
    //   9: Plain mapping (baseline)
    //
    // 1000 hubs total (100 per bucket) + 9000 plain satellites.
    // 10 enums with EnumerationMappings, 100 upstream raw classes for M2M.
    // 100 user functions with typed parameters and DynaFunc bodies.
    // =============================================================================================

    private static final int KS_ENUMS = 10;
    private static final int KS_USER_FNS = 100;

    private static String generateKitchenSink(int total) {
        int hubs = total / 10;                     // 1000
        int satsPerHub = 9;
        int sats = hubs * satsPerHub;              // 9000
        int upstreamClasses = hubs / 10;           // 100 (for M2M bucket)

        var sb = new StringBuilder(total * 800);

        // ---- 1. Enum definitions ----
        for (int e = 0; e < KS_ENUMS; e++) {
            sb.append("Enum test::E").append(e).append(" { V0, V1, V2, V3 }\n");
        }

        // ---- 2. Upstream raw classes (for M2M bucket 7) ----
        for (int u = 0; u < upstreamClasses; u++) {
            sb.append("Class test::U").append(u).append(" {\n")
              .append("    rawId: Integer[1];\n")
              .append("    rawName: String[1];\n")
              .append("    rawCode: String[1];\n")
              .append("    rawScore: Integer[1];\n")
              .append("}\n");
        }

        // ---- 3. Hub classes (with bucket-specific extras: derived props + constraints) ----
        for (int h = 0; h < hubs; h++) {
            int bucket = h % 10;

            sb.append("Class test::H").append(h);
            // Bucket 4: constraint block BEFORE class body
            if (bucket == 4) {
                sb.append("\n[\n")
                  .append("    validScore: $this.score > 0,\n")
                  .append("    nameSet: $this.name->length() > 0\n")
                  .append("]");
            }
            sb.append("\n{\n")
              .append("    id: Integer[1];\n")
              .append("    name: String[1];\n")
              .append("    code: String[1];\n")
              .append("    score: Integer[1];\n");
            // Bucket 8: enum-typed property
            if (bucket == 8) {
                sb.append("    status: test::E").append(h % KS_ENUMS).append("[1];\n");
            }
            // Bucket 3: derived properties
            if (bucket == 3) {
                sb.append("    fullLabel() { $this.name + ' - ' + $this.code }: String[1];\n")
                  .append("    scoreX2() { $this.score * 2 }: Integer[1];\n")
                  .append("    isHighScore() { $this.score > 50 }: Boolean[1];\n");
            }
            sb.append("}\n");
        }

        // ---- 4. Satellite classes (plain) ----
        for (int s = 0; s < sats; s++) {
            sb.append("Class test::S").append(s).append(" {\n")
              .append("    id: Integer[1];\n")
              .append("    label: String[1];\n")
              .append("    value: Integer[1];\n")
              .append("}\n");
        }

        // ---- 5. Associations ----
        for (int h = 0; h < hubs; h++) {
            int next = (h + 1) % hubs;
            sb.append("Association test::HubRing").append(h).append(" {\n")
              .append("    nextHub").append(h).append(": test::H").append(next).append("[0..1];\n")
              .append("    prevHub").append(h).append(": test::H").append(h).append("[0..1];\n}\n");
        }
        for (int s = 0; s < sats; s++) {
            int hub = s / satsPerHub;
            sb.append("Association test::SatHub").append(s).append(" {\n")
              .append("    hub").append(s).append(": test::H").append(hub).append("[0..1];\n")
              .append("    sat").append(s).append(": test::S").append(s).append("[0..1];\n}\n");
        }

        // ---- 6. User functions (100 with typed params, calling builtins) ----
        for (int f = 0; f < KS_USER_FNS; f++) {
            sb.append("function test::fn").append(f)
              .append("(x: Integer[1], y: String[1]): String[1]\n{\n")
              .append("    $y + '=' + $x->toString() + ':' + $y->toUpper()\n}\n");
        }

        // ---- 7. Database ----
        sb.append("Database store::DB (\n");
        // Raw tables (for M2M upstream)
        for (int u = 0; u < upstreamClasses; u++) {
            sb.append("    Table UT").append(u)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), SCORE INT)\n");
        }
        // Hub tables
        for (int h = 0; h < hubs; h++) {
            sb.append("    Table TH").append(h)
              .append(" (ID INT, NAME VARCHAR(100), CODE VARCHAR(20), SCORE INT, STATUS VARCHAR(20), NEXT_HUB_ID INT)\n");
        }
        // Satellite tables
        for (int s = 0; s < sats; s++) {
            sb.append("    Table TS").append(s)
              .append(" (ID INT, LABEL VARCHAR(100), VAL INT, HUB_ID INT)\n");
        }
        // Joins
        for (int h = 0; h < hubs; h++) {
            int next = (h + 1) % hubs;
            sb.append("    Join JHubRing").append(h)
              .append("(TH").append(h).append(".NEXT_HUB_ID = TH").append(next).append(".ID)\n");
        }
        for (int s = 0; s < sats; s++) {
            int hub = s / satsPerHub;
            sb.append("    Join JSat").append(s)
              .append("(TS").append(s).append(".HUB_ID = TH").append(hub).append(".ID)\n");
        }
        // Filters for bucket 0
        for (int h = 0; h < hubs; h += 10) {
            sb.append("    Filter ActiveHub").append(h)
              .append("(TH").append(h).append(".SCORE > 0)\n");
        }
        // Views for bucket 5 (h % 10 == 5)
        for (int h = 5; h < hubs; h += 10) {
            int next = (h + 1) % hubs;
            int filterHub = (h / 10) * 10; // reuse nearby filter (h-5 rounded down)
            sb.append("    View VH").append(h).append(" (\n")
              .append("        ~filter ActiveHub").append(filterHub).append("\n")
              .append("        ~distinct\n")
              .append("        v_id: TH").append(h).append(".ID PRIMARY KEY,\n")
              .append("        v_name: TH").append(h).append(".NAME,\n")
              .append("        v_code: TH").append(h).append(".CODE,\n")
              .append("        v_score: TH").append(h).append(".SCORE,\n")
              .append("        v_next: @JHubRing").append(h).append(" | TH").append(next).append(".NAME,\n")
              .append("        v_label: concat(TH").append(h).append(".NAME, '-', TH").append(h).append(".CODE)\n")
              .append("    )\n");
        }
        sb.append(")\n");

        // ---- 8. Mapping ----
        sb.append("Mapping test::M (\n");

        // EnumerationMappings
        for (int e = 0; e < KS_ENUMS; e++) {
            sb.append("    test::E").append(e).append(": EnumerationMapping EM").append(e).append(" {\n")
              .append("        V0: ['A', 'AA'],\n")
              .append("        V1: ['B', 'BB'],\n")
              .append("        V2: 'C',\n")
              .append("        V3: 'D'\n")
              .append("    }\n");
        }

        // Upstream raw mappings (for M2M sources)
        for (int u = 0; u < upstreamClasses; u++) {
            sb.append("    test::U").append(u).append(": Relational {\n")
              .append("        ~mainTable [store::DB] UT").append(u).append("\n")
              .append("        rawId: [store::DB] UT").append(u).append(".ID,\n")
              .append("        rawName: [store::DB] UT").append(u).append(".NAME,\n")
              .append("        rawCode: [store::DB] UT").append(u).append(".CODE,\n")
              .append("        rawScore: [store::DB] UT").append(u).append(".SCORE\n")
              .append("    }\n");
        }

        // Hub mappings (by bucket)
        for (int h = 0; h < hubs; h++) {
            int bucket = h % 10;
            switch (bucket) {
                case 0 -> {
                    // Filter bucket
                    int filterHub = (h / 10) * 10;
                    sb.append("    test::H").append(h).append(": Relational {\n")
                      .append("        ~filter [store::DB] ActiveHub").append(filterHub).append("\n")
                      .append("        ~mainTable [store::DB] TH").append(h).append("\n")
                      .append("        id: [store::DB] TH").append(h).append(".ID,\n")
                      .append("        name: [store::DB] TH").append(h).append(".NAME,\n")
                      .append("        code: [store::DB] TH").append(h).append(".CODE,\n")
                      .append("        score: [store::DB] TH").append(h).append(".SCORE\n    }\n");
                }
                case 1 -> {
                    // DynaFunc bucket
                    sb.append("    test::H").append(h).append(": Relational {\n")
                      .append("        ~mainTable [store::DB] TH").append(h).append("\n")
                      .append("        id: [store::DB] TH").append(h).append(".ID,\n")
                      .append("        name: concat([store::DB] TH").append(h).append(".NAME, '-', [store::DB] TH").append(h).append(".CODE),\n")
                      .append("        code: toUpper([store::DB] TH").append(h).append(".CODE),\n")
                      .append("        score: plus([store::DB] TH").append(h).append(".SCORE, 1)\n    }\n");
                }
                case 2 -> {
                    // Multi-hop join chain bucket: code comes through @JHubRing[h] > @JHubRing[next]
                    int next = (h + 1) % hubs;
                    int next2 = (h + 2) % hubs;
                    sb.append("    test::H").append(h).append(": Relational {\n")
                      .append("        ~mainTable [store::DB] TH").append(h).append("\n")
                      .append("        id: [store::DB] TH").append(h).append(".ID,\n")
                      .append("        name: [store::DB] @JHubRing").append(h).append(" | TH").append(next).append(".NAME,\n")
                      .append("        code: [store::DB] @JHubRing").append(h).append(" > @JHubRing").append(next).append(" | TH").append(next2).append(".CODE,\n")
                      .append("        score: [store::DB] TH").append(h).append(".SCORE\n    }\n");
                }
                case 5 -> {
                    // View-backed bucket
                    sb.append("    test::H").append(h).append(": Relational {\n")
                      .append("        ~mainTable [store::DB] VH").append(h).append("\n")
                      .append("        id: [store::DB] VH").append(h).append(".v_id,\n")
                      .append("        name: [store::DB] VH").append(h).append(".v_name,\n")
                      .append("        code: [store::DB] VH").append(h).append(".v_code,\n")
                      .append("        score: [store::DB] VH").append(h).append(".v_score\n    }\n");
                }
                case 7 -> {
                    // M2M bucket: hub is Pure-mapped from upstream raw class
                    int upstreamIdx = (h / 10) % upstreamClasses;
                    sb.append("    test::H").append(h).append(": Pure {\n")
                      .append("        ~src test::U").append(upstreamIdx).append("\n")
                      .append("        id: $src.rawId,\n")
                      .append("        name: $src.rawName + ' (copied)',\n")
                      .append("        code: $src.rawCode->toUpper(),\n")
                      .append("        score: $src.rawScore * 2\n    }\n");
                }
                case 8 -> {
                    // Enum property bucket
                    int enumIdx = h % KS_ENUMS;
                    sb.append("    test::H").append(h).append(": Relational {\n")
                      .append("        ~mainTable [store::DB] TH").append(h).append("\n")
                      .append("        id: [store::DB] TH").append(h).append(".ID,\n")
                      .append("        name: [store::DB] TH").append(h).append(".NAME,\n")
                      .append("        code: [store::DB] TH").append(h).append(".CODE,\n")
                      .append("        score: [store::DB] TH").append(h).append(".SCORE,\n")
                      .append("        status: EnumerationMapping EM").append(enumIdx).append(": [store::DB] TH").append(h).append(".STATUS\n    }\n");
                }
                default -> {
                    // Plain relational (buckets 3, 4, 6, 9)
                    sb.append("    test::H").append(h).append(": Relational {\n")
                      .append("        ~mainTable [store::DB] TH").append(h).append("\n")
                      .append("        id: [store::DB] TH").append(h).append(".ID,\n")
                      .append("        name: [store::DB] TH").append(h).append(".NAME,\n")
                      .append("        code: [store::DB] TH").append(h).append(".CODE,\n")
                      .append("        score: [store::DB] TH").append(h).append(".SCORE\n    }\n");
                }
            }
        }

        // Satellite mappings (plain)
        for (int s = 0; s < sats; s++) {
            sb.append("    test::S").append(s).append(": Relational {\n")
              .append("        ~mainTable [store::DB] TS").append(s).append("\n")
              .append("        id: [store::DB] TS").append(s).append(".ID,\n")
              .append("        label: [store::DB] TS").append(s).append(".LABEL,\n")
              .append("        value: [store::DB] TS").append(s).append(".VAL\n    }\n");
        }

        // Association mappings
        for (int h = 0; h < hubs; h++) {
            sb.append("    test::HubRing").append(h).append(": Relational { AssociationMapping (\n")
              .append("        nextHub").append(h).append(": [store::DB]@JHubRing").append(h).append(",\n")
              .append("        prevHub").append(h).append(": [store::DB]@JHubRing").append(h).append("\n    ) }\n");
        }
        for (int s = 0; s < sats; s++) {
            sb.append("    test::SatHub").append(s).append(": Relational { AssociationMapping (\n")
              .append("        hub").append(s).append(": [store::DB]@JSat").append(s).append(",\n")
              .append("        sat").append(s).append(": [store::DB]@JSat").append(s).append("\n    ) }\n");
        }
        sb.append(")\n");

        return withRuntime(sb.toString(), "store::DB", "test::M");
    }

    private static List<String> kitchenSinkQueries(int total, int count) {
        int hubs = total / 10;
        int satsPerHub = 9;
        var queries = new ArrayList<String>(count);
        for (int q = 0; q < count; q++) {
            int h = q * (hubs / count);
            int s = h * satsPerHub;
            // Target distinct feature buckets as we iterate
            int bucket = h % 10;
            switch (bucket) {
                case 0, 6, 9 -> // filter / plain — simple project
                    queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 1 -> // DynaFunc — project shows the computed columns
                    queries.add("test::H" + h + ".all()->project(~[id, name, code, score])");
                case 2 -> // join-chain — project hits the chained name/code columns
                    queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 3 -> // derived prop on class — query can project direct cols
                    queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 4 -> // constraint class
                    queries.add("test::H" + h + ".all()->project(~[id, name, score])->filter(r|$r.score > 0)");
                case 5 -> // view-backed
                    queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 7 -> // M2M
                    queries.add("test::H" + h + ".all()->project(~[id, name, code])");
                case 8 -> // enum prop
                    queries.add("test::H" + h + ".all()->project(~[id, name])");
            }
            // Every 5th query: use satellite + association navigation to exercise pass-2
            if (q % 5 == 0) {
                queries.add("test::S" + s + ".all()->project(~[id, label, hn:x|$x.hub" + s + ".name])");
            }
        }
        return queries;
    }
}
