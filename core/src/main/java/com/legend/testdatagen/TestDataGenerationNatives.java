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

    private static final String DATA_FQN =
            "meta::relational::metamodel::data::RelationalCSVData";
    private static final String TABLE_FQN =
            "meta::relational::metamodel::data::RelationalCSVTable";

    private TestDataGenerationNatives() {
    }

    /** Replace every TDG carrier under {@code stmt} with its folded
     * instance-literal result: {@link TypedCsvCensus} folds from the
     * MODEL (no execution); {@link com.legend.compiler.spec.typed
     * .TypedTestDataGen} EXECUTES the extraction through the database
     * (S2 — deterministic reads over static test seeds, so per-statement
     * re-evaluation is sound). */
    public static TypedSpec foldCensus(TypedSpec stmt, ModelContext ctx,
            java.sql.Connection conn,
            List<TypedSpec> letPrefix,
            com.legend.sql.dialect.SqlDialect engineText,
            TestDataGenerator.ViewSql viewSql) {
        if (stmt instanceof TypedCsvCensus cc) {
            return literal(cc, ctx);
        }
        if (stmt instanceof com.legend.compiler.spec.typed
                .TypedTestDataGen g) {
            if ("plan".equals(g.flavor())) {
                // a PLAN is a value (its planToString reads it) — nothing
                // executes here
                return stmt;
            }
            if ("seedString".equals(g.flavor())) {
                return com.legend.compiler.spec.CsvCensusChecker
                        .literalStrings(List.of(
                                seedStringOrDataError(ctx, g, conn)),
                                g.info())
                        // a [1] string, not a collection — unwrap
                        .children().get(0);
            }
            TestDataGenerator.Result r = transcript(g, ctx, conn, viewSql);
            return com.legend.compiler.spec.CsvCensusChecker.literalTestData(
                    java.util.Objects.requireNonNull(r.dataCsvString(),
                            "generateTestData produced no csv"),
                    r.sqls(), g.info(), g);
        }
        List<TypedSpec> kids = stmt.children();
        if (kids.isEmpty()) {
            return stmt;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = foldCensus(k, ctx, conn, letPrefix, engineText, viewSql);
            changed |= r != k;
            out.add(r);
        }
        return postFold(changed ? stmt.withChildren(out) : stmt, ctx,
                letPrefix, engineText);
    }

    /** Bottom-up constant folds the spliced-literal world enables
     * (S2): a property read over an instance literal is its value (THE
     * one rule — Pipelines.instanceLiteralProp); setUpDataSQLs over a
     * LITERAL csv + a db reference is model-space text generation
     * (Ddl.setUpDataSqlsText — no execution) and folds to string
     * literals so assertTestData's body lowers wholesale. Non-literal
     * spellings are untouched (the statement-level K-arm owns them). */
    private static TypedSpec postFold(TypedSpec n, ModelContext ctx,
            List<TypedSpec> letPrefix,
            com.legend.sql.dialect.SqlDialect engineText) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa) {
            TypedSpec lit = com.legend.resolver.Pipelines.instanceLiteralProp(pa);
            if (lit != null) {
                return lit;
            }
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.builtin.NativeFn.Effect.isSeedSqlForm(nc.callee().id())
                && nc.args().size() == 2
                && deref(nc.args().get(0), letPrefix)
                        instanceof com.legend.compiler.spec.typed
                        .TypedCString csv
                && deref(nc.args().get(1), letPrefix)
                        instanceof com.legend.compiler.spec.typed
                        .TypedPackageableRef dbRef) {
            var db = ctx.findDatabase(dbRef.fullPath()).orElseThrow(
                    () -> new com.legend.error.NotImplementedException(
                            "setUpDataSQLs: unknown database '"
                                    + dbRef.fullPath() + "'"));
            // COMPILER-minted literals (invariant 7): the factory owns
            // node construction; this layer computes the strings only
            return com.legend.compiler.spec.CsvCensusChecker.literalStrings(
                    com.legend.exec.Ddl.setUpDataSqlsText(
                            csv.value(), db, f -> ctx.findDatabase(f), engineText),
                    nc.info());
        }
        return n;
    }

    /** Whether a statement-root USER call's body contains the
     * K-orchestrated setUpDataSQLs native (S2: tests::assertTestData) —
     * such calls route to BODY INLINING (the effectful-assert
     * precedent), where the fold walk sees the native with its frame
     * literals and folds it. */
    public static boolean needsBodyRoute(
            com.legend.compiler.spec.typed.TypedUserCall call,
            com.legend.compiler.spec.SpecCompiler specs) {
        try {
            for (TypedSpec b : specs.compile(call.callee()).body()) {
                if (containsSetUpDataSqls(b)) {
                    return true;
                }
            }
        } catch (com.legend.error.NotImplementedException
                | com.legend.compiler.spec.TypeInferenceException e) {
            // an uncompilable callee routes NOTHING here — the ordinary
            // path owns its wall
            return false;
        }
        return false;
    }

    private static boolean containsSetUpDataSqls(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.builtin.NativeFn.Effect.isSeedSqlForm(nc.callee().id())) {
            return true;
        }
        for (TypedSpec k : n.children()) {
            if (containsSetUpDataSqls(k)) {
                return true;
            }
        }
        return false;
    }

    /** A frame/let-bound LITERAL behind a variable read (the inlined
     * user-call body references its arguments through the frame's
     * lets) — one level, literals only; anything else stays put. */
    private static TypedSpec deref(TypedSpec v, List<TypedSpec> letPrefix) {
        if (v instanceof com.legend.compiler.spec.typed.TypedVariable tv) {
            com.legend.compiler.spec.typed.TypedLet tl =
                    com.legend.compiler.spec.typed.Lets.binding(letPrefix, tv.name());
            if (tl != null) {
                return tl.value();
            }
        }
        return v;
    }

    /** The carrier's captured protocol, classified by SHAPE — every
     * engine overload's extra args (booleans, milestoning-dates ctors,
     * runtime/extension refs) classify structurally, mirroring the
     * engine's own argument vocabulary. (A near-twin parser lives in
     * the harness's TestDataGenForm for the S3-deferred sqls-text
     * advisory; S4 deletes that copy.) */
    /** The generator's run for a typed {@code generateTestData} call —
     * the fold's own run, and (batch 64) the sql-text verdict's
     * chained-fetch TRANSCRIPT: deterministic reads over static seeds,
     * so a re-run yields the same fetch texts and rows. */
    public static TestDataGenerator.Result transcript(
            com.legend.compiler.spec.typed.TypedTestDataGen g,
            ModelContext ctx, java.sql.Connection conn,
            TestDataGenerator.ViewSql viewSql) {
        List<ValueSpecification> ps = g.params();
        LambdaFunction query = (LambdaFunction) ps.get(0);
        String mappingFqn = ((com.legend.protocol.spec
                .PackageableElementPtr) ps.get(1)).fullPath();
        List<TestDataGenerator.TableRowIds> rowIds = new ArrayList<>();
        TestDataGenerator.MilestoningDates[] dates =
                new TestDataGenerator.MilestoningDates[1];
        boolean[] hash = new boolean[1];
        for (int i = 3; i < ps.size(); i++) {
            classifyArg(ps.get(i), rowIds, dates, hash);
        }
        try {
            return TestDataGenerator.generate(ctx, query, mappingFqn,
                    rowIds, dates[0], hash[0], conn, viewSql);
        } catch (java.sql.SQLException e) {
            // the seam: the TDG funnel's java.sql stops at this door
            throw new com.legend.error.DataError(
                    String.valueOf(e.getMessage()), e);
        }
    }

    /** Package-visible for the harness's DEFERRED plan-text arm (the
     * alloy lane) — ONE argument classifier, no test-side twin. */
    /** planToString over a TDG carrier: the {@code plan} flavor prints
     * the engine's MultiResultSequence text; any other flavor is not a
     * plan (loud). */
    public static com.legend.exec.ExecutionResult planTextResult(
            com.legend.compiler.spec.typed.TypedTestDataGen g,
            ModelContext ctx) {
        if (!"plan".equals(g.flavor())) {
            throw new com.legend.error.NotImplementedException(
                    "planToString over a non-plan test-data carrier ("
                            + g.flavor() + ")");
        }
        return new com.legend.exec.ExecutionResult.Scalar(planText(g, ctx),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }

    /** The TDG plan text for a {@code plan}-flavored carrier: query,
     * mapping, then the row identifiers / hash flag / milestoning dates
     * from the remaining arguments (runtime and exeCtx skipped). */
    public static String planText(
            com.legend.compiler.spec.typed.TypedTestDataGen g,
            ModelContext ctx) {
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
                dates[0]);
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

    private static TypedSpec literal(TypedCsvCensus cc, ModelContext ctx) {
        // the census computes HERE (its implementation lives in this
        // layer); the typed literals are COMPILER-minted (invariant 7)
        // by the checker's own factory — a downward call
        return com.legend.compiler.spec.CsvCensusChecker.literal(
                TestDataGenerator.necessaryColumns(
                        ctx, cc.query(), cc.mappingFqn()),
                cc.info());
    }
    private static String seedStringOrDataError(
            com.legend.compiler.element.ModelContext ctx,
            com.legend.compiler.spec.typed.TypedTestDataGen g,
            java.sql.Connection conn) {
        try {
            return TestDataGenerator.seedDataString(ctx,
                    (LambdaFunction) g.params().get(0),
                    ((com.legend.protocol.spec.PackageableElementPtr)
                            g.params().get(1)).fullPath(), conn);
        } catch (java.sql.SQLException e) {
            // the seam: the TDG funnel's java.sql stops at this door
            throw new com.legend.error.DataError(
                    String.valueOf(e.getMessage()), e);
        }
    }
}
