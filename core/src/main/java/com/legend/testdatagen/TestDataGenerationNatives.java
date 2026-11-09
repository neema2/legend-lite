// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedCsvCensus;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * The orchestration-time FOLD for {@link TypedCsvCensus} (TDG lane S1):
 * the carrier arrives from the checker holding the protocol query and
 * mapping FQN; this fold runs the production census (Java ORCHESTRATES
 * — the census is model-space, no execution) and replaces the node with
 * instance literals, so every downstream navigation lowers through the
 * ordinary pipeline. Lives in testdatagen because the census
 * implementation does (compiler must not depend upward — the carrier
 * pattern exists exactly for this layering).
 */
public final class TestDataGenerationNatives {

    private TestDataGenerationNatives() {
    }

    /** The carrier's captured protocol, classified by SHAPE — every
     * engine overload's extra args (booleans, milestoning-dates ctors,
     * runtime/extension refs) classify structurally, mirroring the
     * engine's own argument vocabulary. (A near-twin parser lives in
     * the harness's TestDataGenForm for the S3-deferred sqls-text
     * advisory; S4 deletes that copy.) */

    /** Package-visible for the harness's DEFERRED plan-text arm (the
     * alloy lane) — ONE argument classifier, no test-side twin. */
    /** planToString over a TDG carrier: the {@code plan} flavor prints
     * the engine's MultiResultSequence text; any other flavor is not a
     * plan (loud). */
    public static String planTextResult(
            com.legend.compiler.spec.typed.TypedTestDataGen g,
            ModelContext ctx, com.legend.sql.dialect.EngineTextDialect text, String connection) {
        if (!"plan".equals(g.flavor())) {
            throw new com.legend.error.NotImplementedException(
                    "planToString over a non-plan test-data carrier ("
                            + g.flavor() + ")");
        }
        return planText(g, ctx, text, connection);
    }

    /** The TDG plan text for a {@code plan}-flavored carrier: query,
     * mapping, then the row identifiers / hash flag / milestoning dates
     * from the remaining arguments (runtime and exeCtx skipped). */
    public static String planText(
            com.legend.compiler.spec.typed.TypedTestDataGen g,
            ModelContext ctx, com.legend.sql.dialect.EngineTextDialect text, String connection) {
        List<ValueSpecification> ps = g.params();
        LambdaFunction query = (LambdaFunction) ps.get(0);
        String mappingFqn = ((com.legend.protocol.spec
                .PackageableElementPtr) ps.get(1)).fullPath();
        List<TestDataGenerator.TableRowIds> rowIds = new ArrayList<>();
        TestDataGenerator.MilestoningDates[] dates =
                new TestDataGenerator.MilestoningDates[1];
        boolean[] hash = new boolean[1];
        for (int i = 4; i < ps.size(); i++) {
            classifyArg(ps.get(i), rowIds, dates, hash);
        }
        return TestDataGenerator.planText(ctx, query, mappingFqn, rowIds,
                dates[0], text, connection);
    }

    public static void classifyArg(ValueSpecification arg,
            List<TestDataGenerator.TableRowIds> rowIds,
            TestDataGenerator.MilestoningDates[] dates, boolean[] hash) {
        if (arg instanceof com.legend.protocol.spec.CBoolean b) {
            hash[0] = b.value();
            return;
        }
        if (arg instanceof com.legend.protocol.spec.PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                classifyArg(e, rowIds, dates, hash);
            }
            return;
        }
        // the engine's constructors are PROGRAMS the statement inliner
        // expands: their VALUES reach this carrier as instance literals
        // (let-bound, adopted by SourceSubst.resolveStructuralArgs)
        com.legend.protocol.spec.NewInstance ni =
                com.legend.compiler.spec.SourceSubst.instanceOf(arg);
        if (ni != null) {
            switch (ni.className()) {
                case TABLE_ROW_IDENTIFIERS_FQN -> {
                    rowIds.add(tableRowIds(ni));
                    return;
                }
                case TEMPORAL_MILESTONING_DATES_FQN -> {
                    dates[0] = new TestDataGenerator.MilestoningDates(
                            dateField(ni, "businessDate"),
                            dateField(ni, "processingDate"),
                            dateField(ni, "snapshotDate"));
                    return;
                }
                default -> {
                    // an execution-context / extension instance rides the
                    // call (the generator uses the ambient session); one
                    // WITH properties would need reading — loud
                    if (!ni.properties().isEmpty()) {
                        throw new com.legend.error.NotImplementedException(
                                "generateTestData: instance argument '"
                                        + ni.className() + "' with properties pending");
                    }
                    return;
                }
            }
        }
        if (arg instanceof AppliedFunction af) {
            String simple = simple(af.function());
            switch (simple) {
                case "createTemporalMilestoningDates" -> {
                    String[] d = new String[3];
                    for (int i = 0; i < af.parameters().size() && i < 3;
                            i++) {
                        ValueSpecification pv = af.parameters().get(i);
                        if (pv instanceof com.legend.protocol.spec.CDate cd) {
                            d[i] = cd.value().toEngineString();
                        } else if (!(pv instanceof com.legend.protocol.spec
                                        .PureCollection pc2)
                                || !pc2.values().isEmpty()) {
                            throw new com.legend.error.NotImplementedException(
                                    "generateTestData: non-literal"
                                            + " milestoning date");
                        }
                    }
                    dates[0] = new TestDataGenerator.MilestoningDates(
                            d[0], d[1], d[2]);
                    return;
                }
                case "new" -> {
                    for (ValueSpecification pv : af.parameters()) {
                        if (pv instanceof com.legend.protocol.spec
                                        .PureCollection pc2
                                && !pc2.values().isEmpty()) {
                            throw new com.legend.error.NotImplementedException(
                                    "generateTestData: instance argument"
                                            + " with properties pending");
                        }
                    }
                    return;
                }
                case "relationalExtensions", "testRuntime",
                        "executionContext", "extension" -> {
                    return;
                }
                default -> throw new com.legend.error.NotImplementedException(
                        "generateTestData: unclassified argument call '"
                                + simple + "'");
            }
        }
        if (arg instanceof com.legend.protocol.spec.PackageableElementPtr) {
            // a runtime/db/extension reference — the generator uses the
            // ambient connection (engine parity: the test runtime IS the
            // ambient database)
            return;
        }
        throw new com.legend.error.NotImplementedException(
                "generateTestData: unclassified argument "
                        + arg.getClass().getSimpleName()
                        + (arg instanceof com.legend.protocol.spec.Variable v
                                ? " '" + v.name() + "' (a let the carrier did not adopt)" : ""));
    }

    private static final String TABLE_ROW_IDENTIFIERS_FQN =
            "meta::relational::testDataGeneration::TableRowIdentifiers";
    private static final String ROW_IDENTIFIER_FQN =
            "meta::relational::testDataGeneration::RowIdentifier";
    private static final String TEMPORAL_MILESTONING_DATES_FQN =
            "meta::relational::testDataGeneration::TemporalMilestoningDates";
    private static final String GET_TABLE_FQN =
            "meta::relational::testDataGeneration::getTable";

    private static @com.legend.base.Nullable ValueSpecification field(
            com.legend.protocol.spec.NewInstance ni, String key) {
        for (var kb : ni.properties()) {
            if (kb.key().equals(key)) {
                return kb.expression().value();
            }
        }
        return null;
    }

    /** {@code ^TableRowIdentifiers(table = getTable(db, schema, table),
     * rowIdentifiers = [...])} — the table is the engine's own element
     * accessor spelled with its literal names (D2: an element's identity). */
    private static TestDataGenerator.TableRowIds tableRowIds(
            com.legend.protocol.spec.NewInstance ni) {
        ValueSpecification table = field(ni, "table");
        if (!(table instanceof AppliedFunction gt
                && GET_TABLE_FQN.equals(gt.function())
                && gt.parameters().size() == 3
                && gt.parameters().get(1) instanceof com.legend.protocol.spec.CString sc
                && gt.parameters().get(2) instanceof com.legend.protocol.spec.CString tc)) {
            throw new com.legend.error.NotImplementedException(
                    "generateTestData: TableRowIdentifiers.table must be"
                            + " getTable(db, 'schema', 'table') — got "
                            + (table == null ? "no table" : table.getClass().getSimpleName()));
        }
        List<TestDataGenerator.RowId> out = new ArrayList<>();
        ValueSpecification ids = field(ni, "rowIdentifiers");
        if (ids != null) {
            collectRowIds(ids, out);
        }
        return new TestDataGenerator.TableRowIds(sc.value(), tc.value(), out);
    }

    private static @com.legend.base.Nullable String dateField(
            com.legend.protocol.spec.NewInstance ni, String key) {
        ValueSpecification v = field(ni, key);
        if (v == null || v instanceof com.legend.protocol.spec.PureCollection pc
                && pc.values().isEmpty()) {
            return null;
        }
        if (v instanceof com.legend.protocol.spec.CDate cd) {
            return cd.value().toEngineString();
        }
        throw new com.legend.error.NotImplementedException(
                "generateTestData: non-literal milestoning date");
    }

    /** {@code ^RowIdentifier(columnValuePairs = [cols]->zip([values]))}
     * (createRowIdentifier's value) or a collection of them. */
    private static void collectRowIds(ValueSpecification v,
            List<TestDataGenerator.RowId> out) {
        if (v instanceof com.legend.protocol.spec.PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                collectRowIds(e, out);
            }
            return;
        }
        com.legend.protocol.spec.NewInstance ni =
                com.legend.compiler.spec.SourceSubst.instanceOf(v);
        if (ni != null && ROW_IDENTIFIER_FQN.equals(ni.className())) {
            ValueSpecification pairs = field(ni, "columnValuePairs");
            if (pairs instanceof AppliedFunction z
                    && com.legend.compiler.ResolvedNames.names(z, com.legend.compiler.element.type.PlatformTypes.ZIP)
                    && z.parameters().size() == 2) {
                out.add(new TestDataGenerator.RowId(
                        literalStrings(z.parameters().get(0)),
                        literalValues(z.parameters().get(1))));
                return;
            }
            throw new com.legend.error.NotImplementedException(
                    "generateTestData: RowIdentifier.columnValuePairs must be"
                            + " zip([names], [values]) — got "
                            + (pairs == null ? "nothing" : pairs.getClass().getSimpleName()));
        }
        throw new com.legend.error.NotImplementedException(
                "generateTestData: row-identifier value shape pending: "
                        + v.getClass().getSimpleName());
    }

    private static List<String> literalStrings(ValueSpecification v) {
        List<String> out = new ArrayList<>();
        for (Object o : literalValues(v)) {
            out.add((String) o);
        }
        return out;
    }

    private static List<Object> literalValues(ValueSpecification v) {
        List<Object> out = new ArrayList<>();
        collectLiterals(v, out);
        return out;
    }

    private static void collectLiterals(ValueSpecification v, List<Object> out) {
        switch (v) {
            case com.legend.protocol.spec.PureCollection pc -> {
                for (ValueSpecification e : pc.values()) {
                    collectLiterals(e, out);
                }
            }
            case com.legend.protocol.spec.CString c -> out.add(c.value());
            case com.legend.protocol.spec.CInteger c -> out.add(c.value());
            case com.legend.protocol.spec.CBoolean c -> out.add(c.value());
            case com.legend.protocol.spec.CDate c ->
                    out.add(c.value().toEngineString());
            default -> throw new com.legend.error.NotImplementedException(
                    "generateTestData: non-literal row-identifier value "
                            + v.getClass().getSimpleName());
        }
    }

    private static String simple(String fn) {
        int i = fn.lastIndexOf("::");
        return i < 0 ? fn : fn.substring(i + 2);
    }

}
