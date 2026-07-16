// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.ImportScope;
import com.legend.parser.SpecParser;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * NATIVE test-body execution &mdash; runs a real pure {@code <<test.Test>>}
 * function body (a STATEMENT SEQUENCE of lets, {@code execute(...)} calls
 * and {@code assert*} calls) through the ordinary compile-to-SQL pipeline.
 *
 * <p><strong>No interpreter.</strong> Tenet #1 applies to tests too:
 * <ul>
 *   <li>{@code let r = execute(|Q, mapping, runtime, ext)} binds {@code r}
 *       to <em>the query expression itself</em> (a lazy handle) plus its
 *       execution context. Nothing runs yet.</li>
 *   <li>Every downstream read ({@code $r.values.rows->map(...)->sort()})
 *       SPLICES the query into the surrounding chain; the whole chain
 *       compiles and executes as ONE SQL statement.</li>
 *   <li>{@code assert*} natives are the orchestration boundary: BOTH sides
 *       compile and execute through the pipeline; Java compares the two
 *       wire values &mdash; strictly, since both sides share one wire
 *       convention.</li>
 * </ul>
 *
 * <p><strong>The one driver-level form.</strong> {@code execute(...)}'s
 * runtime/extensions arguments are the engine harness's plumbing
 * ({@code testRuntime()}, {@code relationalExtensions()} &mdash; functions
 * whose bodies construct engine-runtime objects legend-lite deliberately
 * does not model). The driver consumes the QUERY (arg 0, fully compiled)
 * and the MAPPING (arg 1, an element ref resolved under the caller's
 * imports); the trailing config arguments are accepted un-typed and the
 * CALLER supplies the physical connection + runtime. This is the same
 * boundary the engine's own {@code execute} crosses into Java.
 *
 * <p><strong>Failure polarity.</strong> Anything this driver does not
 * recognize is {@link Outcome.Unsupported} (named, loud) &mdash; never a
 * silent skip; a compile error in any chain propagates as an exception.
 * Assertion evaluation STOPS at the first failing assert (real pure
 * {@code assert} raises).
 */
public final class TestBody {

    private TestBody() {
    }

    /** The result of driving one test body. */
    public sealed interface Outcome {

        /**
         * The body ran to completion or first assert failure.
         *
         * @param verified  row/value-comparing asserts that ran
         * @param advisory  golden-SQL asserts recognized but not compared
         *                  (legend-lite's SQL is its dialect's, by design)
         * @param failures  first assert failure (empty = all held)
         */
        record Ran(int verified, int advisory, List<String> failures) implements Outcome {
        }

        /** A statement/assert shape the driver does not support yet — NAMED. */
        record Unsupported(String reason) implements Outcome {
        }
    }

    /** A bound {@code execute(...)}: the lazy query handle + its context. */
    private record ExecHandle(LambdaFunction query, ValueSpecification mappingRef,
            boolean relationRooted) {

        ExecHandle(LambdaFunction query, ValueSpecification mappingRef) {
            this(query, mappingRef, true);
        }
    }

    /** at(k>0) on a relation-rooted Result.values: the envelope has ONE
     * element — collapsing would silently ignore the index. */
    private static String envelopeIndexError(java.util.List<AppliedFunction> wrappers) {
        for (AppliedFunction w : wrappers) {
            if (w.function().equals("at") && w.parameters().size() == 2
                    && !(w.parameters().get(1) instanceof CInteger k
                            && k.value().longValue() == 0)) {
                return "Result.values->at(k>0) on a relation-rooted query"
                        + " — the values envelope holds one TDS";
            }
        }
        return null;
    }

    /**
     * The engine's {@code Result.values} shape depends on the QUERY ROOT:
     * a relation/TDS query's values holds ONE element (the TDS — the
     * {@code ->at(0)}/{@code ->toOne()} wrappers are envelope peels), while
     * a class- or scalar-rooted query's values IS the row collection and
     * {@code at(k)} means the k-th VALUE (audit 9: the blind collapse
     * silently read all objects). Decided from the USER-LEVEL type — the
     * query typed BEFORE store resolution.
     */
    private static boolean relationRooted(ExecHandle h, ModelContext ctx,
            ImportScope imports, String runtimeFqn) {
        try {
            ValueSpecification spliced = splice(h, runtimeFqn);
            LambdaFunction wrapped = new LambdaFunction(List.of(), List.of(spliced));
            ValueSpecification resolved = com.legend.compiler.NameResolver
                    .resolveQuery(wrapped, imports, ctx.elementFqns());
            List<TypedSpec> body = new SpecCompiler(ctx).typeQueryBody(resolved);
            TypedSpec root = body.get(body.size() - 1);
            while (root instanceof com.legend.compiler.spec.typed.TypedFrom fr) {
                root = fr.source();
            }
            return root.info().type()
                    instanceof com.legend.compiler.element.type.Type.RelationType;
        } catch (RuntimeException e) {
            return true;   // typing fails loudly at evaluation either way
        }
    }

    /**
     * Drive one test body.
     *
     * @param ctx        the compiled model (compile once per model text,
     *                   reuse across the file's tests)
     * @param body       the test function's body source (statements between
     *                   the braces)
     * @param imports    the enclosing section's import scope (plus the
     *                   test's own package)
     * @param runtimeFqn the driver-supplied runtime (connections; also the
     *                   dialect)
     */
    public static Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn) throws java.sql.SQLException {
        return run(ctx, body, imports, runtimeFqn, conn, false);
    }

    /**
     * {@code emptinessUnverifiable}: the caller knows the database may be
     * missing rows for environmental reasons (failed seed replay) — an
     * emptiness-shaped assertion (assertEmpty, assertSize 0, an empty
     * expected grid) proves nothing then and the body reports Unsupported
     * instead of a hollow pass.
     */
    public static Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable)
            throws java.sql.SQLException {
        return run(ctx, SpecParser.parseCodeBlock(body), imports, runtimeFqn,
                conn, emptinessUnverifiable);
    }

    /**
     * AST entry (Phase C): the test body arrives ALREADY PARSED — the
     * harness discovers test functions from the parsed model, so their
     * statement lists come straight off the FunctionDefinition, no
     * re-parse of extracted text.
     */
    public static Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable)
            throws java.sql.SQLException {
        return run(ctx, statements, imports, runtimeFqn, conn,
                emptinessUnverifiable, null);
    }

    /**
     * {@code seedFailures}: the caller's failed-seed LEDGER — setup calls
     * the body makes report per-statement raw-SQL failures here instead of
     * aborting (engine-harness tolerance), and a non-empty ledger makes
     * emptiness-shaped assertions unverifiable from that point on.
     */
    public static Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            java.util.List<String> seedFailures)
            throws java.sql.SQLException {
        java.util.ArrayDeque<ValueSpecification> work =
                new java.util.ArrayDeque<>(statements);
        Map<String, ValueSpecification> lets = new LinkedHashMap<>();
        Map<String, ExecHandle> handles = new LinkedHashMap<>();
        int verified = 0;
        int advisory = 0;
        while (!work.isEmpty()) {
            ValueSpecification stmt = work.poll();
            // side-effect-free harness noise
            if (stmt instanceof AppliedFunction pln
                    && harnessVocabName(pln.function())
                    && ("println".equals(simpleName(pln.function()))
                            || "print".equals(simpleName(pln.function())))) {
                continue;
            }
            // engine test-harness WRAPPERS: the lambda argument's body IS
            // the test — inline its statements at the front of the worklist
            if (stmt instanceof AppliedFunction wrap
                    && harnessVocabName(wrap.function())
                    && java.util.Set.of("runLegendTest", "runTest",
                            "runGraphFetchTest", "mayExecuteAlloyTest",
                            "mayExecuteLegendTest")
                            .contains(simpleName(wrap.function()))) {
                LambdaFunction inner = null;
                for (ValueSpecification arg : wrap.parameters()) {
                    ValueSpecification a2 = arg instanceof Variable av
                            && lets.get(av.name()) != null
                            ? lets.get(av.name()) : arg;
                    if (a2 instanceof LambdaFunction lf0
                            && lf0.parameters().isEmpty()) {
                        inner = lf0;
                        break;
                    }
                }
                if (inner != null) {
                    List<ValueSpecification> bodyStmts =
                            new ArrayList<>(inner.body());
                    for (int i = bodyStmts.size() - 1; i >= 0; i--) {
                        work.addFirst(bodyStmts.get(i));
                    }
                    continue;
                }
                return new Outcome.Unsupported("harness wrapper '"
                        + simpleName(wrap.function())
                        + "' carries no zero-arg lambda body");
            }
            // let name = rhs
            if (stmt instanceof AppliedFunction af && af.function().equals("letFunction")
                    && af.parameters().size() == 2
                    && af.parameters().get(0) instanceof CString name) {
                ValueSpecification raw = af.parameters().get(1);
                // let r = execute(...).values / .values->at(0) / ->toOne():
                // for a RELATION-rooted query these wrappers are the Result
                // envelope and peel to the handle; for a class/scalar root
                // values IS the collection and at/toOne are REAL selections
                // (audit 9) — those bind as ordinary lazy lets instead.
                java.util.List<AppliedFunction> wrappers = new ArrayList<>();
                while (true) {
                    if (raw instanceof AppliedProperty pw
                            && pw.property().equals("values")) {
                        raw = pw.receiver();
                        continue;
                    }
                    if (raw instanceof AppliedFunction fw
                            && (fw.function().equals("at") || fw.function().equals("toOne"))
                            && !fw.parameters().isEmpty()
                            && (fw.parameters().get(0) instanceof AppliedProperty
                                    || fw.parameters().get(0) instanceof AppliedFunction ifn
                                            && isExecuteCall(ifn))) {
                        wrappers.add(fw);
                        raw = fw.parameters().get(0);
                        continue;
                    }
                    break;
                }
                // let tds = $r.values->at(0) over a RELATION-rooted handle:
                // the peel bottoms at the handle VAR — the alias is the
                // same envelope handle (so $tds->size() keeps ONE-TDS
                // semantics, and $tds.rows reads splice as before)
                if (raw instanceof Variable hv && handles.containsKey(hv.name())
                        && handles.get(hv.name()).relationRooted()) {
                    String bad = envelopeIndexError(wrappers);
                    if (bad != null) {
                        return new Outcome.Unsupported(bad);
                    }
                    handles.put(name.value(), handles.get(hv.name()));
                    continue;
                }
                ValueSpecification rhs = raw instanceof AppliedFunction rex
                        && isExecuteCall(rex)
                        ? new AppliedFunction(rex.function(), substituteAll(
                                rex.parameters(), lets, handles, runtimeFqn))
                        : substitute(af.parameters().get(1), lets, handles, runtimeFqn);
                if (rhs instanceof AppliedFunction ex && isExecuteCall(ex)) {
                    ExecHandle h = toHandle(ex, lets);
                    if (h == null) {
                        return new Outcome.Unsupported(
                                "execute() whose query argument is not a lambda");
                    }
                    h = new ExecHandle(h.query(), h.mappingRef(),
                            relationRooted(h, ctx, imports, runtimeFqn));
                    if (!wrappers.isEmpty() && !h.relationRooted()) {
                        // class/scalar root: at/toOne are REAL selections —
                        // bind the whole wrapped chain as a lazy let (the
                        // inline-execute substitution splices the query and
                        // keeps the wrappers in place)
                        lets.put(name.value(), substitute(af.parameters().get(1),
                                lets, handles, runtimeFqn));
                        continue;
                    }
                    String bad = envelopeIndexError(wrappers);
                    if (bad != null) {
                        return new Outcome.Unsupported(bad);
                    }
                    handles.put(name.value(), h);
                    // EAGER execution (audit 16 F1, engine parity): the
                    // engine's execute() runs AT the let — a broken
                    // compile/lowering must surface here even when no
                    // assert ever reads the handle. Lazy handles let
                    // envelope-shape-only asserts (size-1 idiom) PASS
                    // vacuously against a broken pipeline.
                    eval(splice(h, runtimeFqn), lets, handles, ctx, imports,
                            runtimeFqn, conn);
                } else {
                    lets.put(name.value(), rhs);
                }
                continue;
            }
            if (stmt instanceof AppliedFunction af
                    && harnessVocabName(af.function())
                    && simpleName(af.function()).startsWith("assert")) {
                String failure = checkAssert(af, lets, handles, ctx, imports,
                        runtimeFqn, conn, emptinessUnverifiable
                                || seedFailures != null && !seedFailures.isEmpty());
                if (failure == UNSUPPORTED_MARKER) {
                    return new Outcome.Unsupported("assert form '" + af.function()
                            + "/" + af.parameters().size() + "' is not supported yet");
                }
                if (failure == ADVISORY_MARKER) {
                    advisory++;
                    continue;
                }
                verified++;
                if (failure != null) {
                    return new Outcome.Ran(verified, advisory, List.of(failure));
                }
                continue;
            }
            // the conventional trailing `true`
            if (stmt instanceof CBoolean) {
                continue;
            }
            if (stmt instanceof AppliedFunction af && isExecuteCall(af)) {
                return new Outcome.Unsupported("execute() not bound to a let");
            }
            // CSV-seeding setup (modelJoin's setupTestData([csv...], db, rt)):
            // each CSV block is schema\ntable\nheader\nrows — the engine's
            // setUpDataSQLsV2 convention, loaded natively here
            if (stmt instanceof AppliedFunction af
                    && harnessVocabName(af.function())
                    && "setupTestData".equals(simpleName(af.function()))
                    && !af.parameters().isEmpty()) {
                List<String> csvs = constantStrings(
                        substitute(af.parameters().get(0), lets, handles,
                                runtimeFqn));
                String dbFqn = af.parameters().size() > 1
                        && substitute(af.parameters().get(1), lets, handles,
                                runtimeFqn)
                                instanceof PackageableElementPtr dbp
                        ? dbp.fullPath() : null;
                if (csvs != null) {
                    for (String csv : csvs) {
                        loadCsvSeed(csv, dbFqn, ctx, conn);
                    }
                    continue;
                }
            }
            // K-natives arc (S4): any other EXPRESSION STATEMENT executes
            // through the platform — the engine's setup calls
            // (createTablesAndFillDb(), setUp($m), executeInDb(...)) are
            // ordinary pure code, and the pipeline is the only executor.
            // SQLExceptions propagate (an honest ERROR); compile/type
            // failures report Unsupported — the body's data cannot be
            // trusted after a failed setup statement.
            if (stmt instanceof AppliedFunction af3) {
                try {
                    Compiler.executeResolved(
                            NameResolver.resolveQuery(
                                    substitute(stmt, lets, handles, runtimeFqn),
                                    imports, ctx.elementFqns()),
                            ctx, runtimeFqn, conn,
                            seedFailures == null ? null : seedFailures::add);
                    continue;
                } catch (java.sql.SQLException sql) {
                    throw sql;
                } catch (com.legend.error.NotImplementedException e) {
                    // a VOCABULARY gap — honestly SHAPE; any OTHER
                    // RuntimeException is a real pipeline defect and must
                    // surface as ERROR, not hide in the SHAPE bucket
                    // (audit 17) — it propagates to the runner's scorer
                    return new Outcome.Unsupported("statement '" + af3.function()
                            + "' failed through the pipeline: "
                            + String.valueOf(e.getMessage()).split("\\n")[0]);
                }
            }
            return new Outcome.Unsupported("unsupported statement: "
                    + stmt.getClass().getSimpleName());
        }
        return new Outcome.Ran(verified, advisory, List.of());
    }

    /** One side of a JSON assert as a PARSED structure: a GRAPH result's
     * envelope, or a String value holding JSON text. Null = not JSON-shaped
     * (the caller reports Unsupported, never a false verdict). */
    private static Object jsonValueOf(Eval e) {
        if (e.result instanceof com.legend.exec.ExecutionResult.Graph g) {
            return ExecJson.parse(g.json());
        }
        List<Object> vals = e.values();
        if (vals.size() == 1 && vals.get(0) instanceof String str) {
            try {
                return ExecJson.parse(str);
            } catch (RuntimeException notJson) {
                return null;
            }
        }
        return null;
    }

    private static String abbreviate(String s) {
        return s.length() <= 160 ? s : s.substring(0, 157) + "...";
    }

    /** The elements of a CONSTANT string collection ({@code ['a'+'b', $x]}
     * with let-resolved, concat-folded elements), or null if any element
     * is not a compile-time string. */
    private static List<String> constantStrings(ValueSpecification v) {
        List<ValueSpecification> elems =
                v instanceof PureCollection pc ? pc.values() : List.of(v);
        List<String> out = new ArrayList<>(elems.size());
        for (ValueSpecification e : elems) {
            String sv = constantString(e);
            if (sv == null) {
                return null;
            }
            out.add(sv);
        }
        return out;
    }

    private static String constantString(ValueSpecification v) {
        if (v instanceof CString cs) {
            return cs.value();
        }
        if (v instanceof AppliedFunction af && af.parameters().size() == 2
                && ("plus".equals(af.function()) || "+".equals(af.function()))) {
            String l = constantString(af.parameters().get(0));
            String r = constantString(af.parameters().get(1));
            return l != null && r != null ? l + r : null;
        }
        if (v instanceof AppliedFunction af && "plus".equals(af.function())
                && af.parameters().size() == 1
                && af.parameters().get(0) instanceof PureCollection pc) {
            StringBuilder sb = new StringBuilder();
            for (ValueSpecification e : pc.values()) {
                String sv = constantString(e);
                if (sv == null) {
                    return null;
                }
                sb.append(sv);
            }
            return sb.toString();
        }
        return null;
    }

    /** One CSV seed block: {@code schema\ntable\nHEADER\nrows...} —
     * DROP + CREATE from the model's OWN table definition (engine
     * setUpDataSQLsV2 semantics: the test connection holds exactly the
     * CSV tables, so a bulk-seeded base table sharing the name cannot
     * shadow the family's — audit: 37 modelJoin binder errors), then
     * typed INSERTs ('default' schema is bare; empty tokens are NULL;
     * numerics ride bare, everything else quotes). */
    private static void loadCsvSeed(String csv, String dbFqn,
            ModelContext ctx, Connection conn)
            throws java.sql.SQLException {
        String[] lines = csv.split("\n");
        if (lines.length < 3) {
            return;
        }
        String schema = lines[0].strip();
        String table = lines[1].strip();
        String qualified = "default".equals(schema) ? table
                : schema + "." + table;
        String[] cols = lines[2].split(",");
        var tableType = dbFqn == null
                ? java.util.Optional.<com.legend.compiler.element.type
                        .Type.RelationType>empty()
                : ctx.findTable(dbFqn, table);
        if (tableType.isPresent()) {
            StringBuilder ddl = new StringBuilder("CREATE OR REPLACE TABLE ")
                    .append(qualified).append(" (");
            var tcols = tableType.get().columns();
            for (int c = 0; c < tcols.size(); c++) {
                if (c > 0) {
                    ddl.append(", ");
                }
                ddl.append(tcols.get(c).name()).append(' ')
                        .append(sqlDdlType(tcols.get(c).type()));
            }
            try (var st = conn.prepareStatement(ddl.append(")").toString())) {
                st.execute();
            }
        } else {
            try (var st = conn.prepareStatement(
                    "DELETE FROM " + qualified)) {
                st.execute();
            }
        }
        {
            for (int i = 3; i < lines.length; i++) {
                if (lines[i].isBlank()) {
                    continue;
                }
                String[] vals = lines[i].split(",", -1);
                StringBuilder sql = new StringBuilder("INSERT INTO ")
                        .append(qualified).append(" (")
                        .append(String.join(", ", cols)).append(") VALUES (");
                for (int c = 0; c < cols.length; c++) {
                    String tok = c < vals.length ? vals[c].strip() : "";
                    if (c > 0) {
                        sql.append(", ");
                    }
                    if (tok.isEmpty()) {
                        sql.append("NULL");
                    } else if (tok.matches("[+-]?\\d+(\\.\\d+)?")) {
                        sql.append(tok);
                    } else {
                        sql.append("'").append(tok.replace("'", "''"))
                                .append("'");
                    }
                }
                try (var ins = conn.prepareStatement(
                        sql.append(")").toString())) {
                    ins.execute();
                }
            }
        }
    }

    private static String sqlDdlType(com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER) {
            return "BIGINT";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT
                || t == com.legend.compiler.element.type.Type.Primitive.NUMBER) {
            return "DOUBLE";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            return "BOOLEAN";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE) {
            return "DATE";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME
                || t == com.legend.compiler.element.type.Type.Primitive.DATE) {
            return "TIMESTAMP";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                || t instanceof com.legend.compiler.element.type
                        .Type.PrecisionDecimal) {
            return "DECIMAL(38, 9)";
        }
        return "VARCHAR";
    }

    // ===== assert dispatch =====

    private static final String UNSUPPORTED_MARKER = new String("unsupported");
    private static final String ADVISORY_MARKER = new String("advisory");

    /** null = held; ADVISORY_MARKER = golden-SQL; UNSUPPORTED_MARKER; else the failure text. */
    private static String checkAssert(AppliedFunction af,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn,
            boolean emptinessUnverifiable) throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        switch (simpleName(af.function())) {
            case "assert", "assertFalse" -> {
                if (args.isEmpty()) {
                    return UNSUPPORTED_MARKER;
                }
                if (containsSqlText(args.get(0))) {
                    // predicate PURELY over golden SQL text is advisory; a
                    // MIXED assert (sql text AND value reads) must not have
                    // its value conjuncts silently skipped (audit 9)
                    return containsValuesRead(args.get(0))
                            ? UNSUPPORTED_MARKER : ADVISORY_MARKER;
                }
                if (emptinessUnverifiable) {
                    // seeds failed: a predicate like isEmpty(...) would
                    // hollow-PASS over the tables the failed seeds left
                    // empty — same guard as the equals/size-0 spellings
                    // (audit 16 F4); assert over verifiable state is rare
                    // enough that blanket-unsupported stays honest
                    return UNSUPPORTED_MARKER;
                }
                Object v = evalScalar(args.get(0), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                boolean expect = af.function().equals("assert");
                return Boolean.valueOf(expect).equals(v) ? null
                        : "assert" + (expect ? "" : "False") + " did not hold ("
                                + v + ")";
            }
            case "assertEquals", "assertEq", "assertEqualsH2Compatible", "assertNotEquals" -> {
                if (args.size() < 2) {
                    return UNSUPPORTED_MARKER;
                }
                // golden-SQL spellings are advisory: our SQL is DuckDB's.
                // A MIXED side (sql text AND value reads) is loud instead —
                // skipping its value conjuncts would be silent (audit 9).
                if (containsSqlText(args.get(args.size() - 1))
                        || containsSqlText(args.get(0))) {
                    return containsValuesRead(args.get(0))
                            || containsValuesRead(args.get(args.size() - 1))
                            ? UNSUPPORTED_MARKER : ADVISORY_MARKER;
                }
                // legacy 3-arg H2-compat: (legacySql, h2Sql, actualSql) —
                // all SQL text, advisory
                if (args.size() == 3 && af.function().equals("assertEqualsH2Compatible")) {
                    return ADVISORY_MARKER;
                }
                Eval e = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                if (emptinessUnverifiable && e.size() == 0) {
                    // seeds failed: an EMPTY expectation would hollow-PASS
                    // against the empty tables (audit 9 — the assertSize-0/
                    // assertEmpty guard alone missed the equals spellings)
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(1), lets, handles, ctx, imports, runtimeFqn, conn);
                boolean equal = compare(e, a, /* ordered */ true);
                if (af.function().equals("assertNotEquals")) {
                    return equal ? "assertNotEquals: both sides are " + e.render() : null;
                }
                return equal ? null : "assertEquals: expected " + e.render()
                        + ", got " + a.render();
            }
            case "assertSameElements" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Eval e = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                if (emptinessUnverifiable && e.size() == 0) {
                    return UNSUPPORTED_MARKER;   // see the assertEquals guard
                }
                Eval a = eval(args.get(1), lets, handles, ctx, imports, runtimeFqn, conn);
                return compare(e, a, /* ordered */ false) ? null
                        : "assertSameElements: expected " + e.render() + ", got " + a.render();
            }
            case "assertSize" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Object n = evalScalar(args.get(1), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                if (emptinessUnverifiable && n instanceof Number zn && zn.longValue() == 0) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                long actual = a.size();
                return (n instanceof Number num && num.longValue() == actual) ? null
                        : "assertSize: expected " + n + ", got " + actual;
            }
            case "assertEmpty" -> {
                if (args.size() != 1) {
                    return UNSUPPORTED_MARKER;
                }
                if (emptinessUnverifiable) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                return a.size() == 0 ? null : "assertEmpty: got " + a.size() + " values";
            }
            case "assertSameSQL" -> {
                return ADVISORY_MARKER;
            }
            case "assertJsonStringsEqual" -> {
                // graph-fetch JSON equality (engine semantics): object keys
                // order-INSENSITIVE, arrays order-SENSITIVE — exactly deep
                // equality over the PARSED structures (both sides through
                // the same parser, so number spelling is symmetric)
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Eval e = eval(args.get(0), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                if (emptinessUnverifiable) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(1), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                Object expected = jsonValueOf(e);
                Object actual = jsonValueOf(a);
                if (expected == null || actual == null) {
                    return UNSUPPORTED_MARKER;
                }
                // pure's [x] ≡ x value semantics at the ROOT: the engine
                // serializes a one-element result as the bare object; our
                // envelope always arrays. Bridge exactly that case — an
                // object-shaped expectation against a singleton array.
                if (!(expected instanceof List) && actual instanceof List<?> al
                        && al.size() == 1) {
                    actual = al.get(0);
                }
                return java.util.Objects.equals(expected, actual) ? null
                        : "assertJsonStringsEqual: expected "
                                + abbreviate(String.valueOf(expected))
                                + ", got " + abbreviate(String.valueOf(actual));
            }
            default -> {
                return UNSUPPORTED_MARKER;
            }
        }
    }

    /** Harness vocabulary matches by SIMPLE name only for BARE or
     * meta::-qualified spellings — a user function my::pkg::assertFoo
     * must route to the platform, never be hijacked (audit 17). */
    private static boolean harnessVocabName(String fn) {
        return !fn.contains("::") || fn.startsWith("meta::");
    }

    private static String simpleName(String fn) {
        int cut = fn.lastIndexOf("::");
        return cut < 0 ? fn : fn.substring(cut + 2);
    }

    /** A golden-SQL spelling: any chain ending in sqlRemoveFormatting()/sql(). */
    private static boolean isSqlText(ValueSpecification v) {
        return v instanceof AppliedFunction af
                && (af.function().equals("sqlRemoveFormatting") || af.function().equals("sql"));
    }

    /** A Result VALUES read anywhere in the expression — the assert also
     * verifies row data, so it must not be swallowed as advisory. */
    private static boolean containsValuesRead(ValueSpecification v) {
        if (v instanceof AppliedProperty ap && ap.property().equals("values")) {
            return true;
        }
        if (v instanceof AppliedFunction af) {
            for (ValueSpecification p2 : af.parameters()) {
                if (containsValuesRead(p2)) {
                    return true;
                }
            }
        }
        if (v instanceof AppliedProperty ap2) {
            return containsValuesRead(ap2.receiver());
        }
        return false;
    }

    /** A golden-SQL read ANYWHERE in the expression (nested spellings:
     * {@code $r->sqlRemoveFormatting()->toLower()->contains(...)}) — the
     * whole assertion is about SQL text, advisory by policy. */
    private static boolean containsSqlText(ValueSpecification v) {
        if (isSqlText(v)) {
            return true;
        }
        if (v instanceof AppliedFunction af) {
            for (ValueSpecification p : af.parameters()) {
                if (containsSqlText(p)) {
                    return true;
                }
            }
        }
        if (v instanceof AppliedProperty ap) {
            return containsSqlText(ap.receiver());
        }
        return false;
    }

    // ===== evaluation: compile one side through the pipeline =====

    /** One evaluated side: the execution result + how it compares. */
    private record Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
            boolean csvTail, String joinSep) {

        Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
                boolean csvTail) {
            this(result, sortedChain, csvTail, null);
        }

        long size() {
            return switch (result) {
                case com.legend.exec.ExecutionResult.Scalar sc ->
                        sc.value() == null ? 0 : flatten(sc.value()).size();
                case com.legend.exec.ExecutionResult.Collection c -> c.values().size();
                case com.legend.exec.ExecutionResult.Tabular t -> t.rows().size();
                case com.legend.exec.ExecutionResult.Graph g -> {
                    Object p = ExecJson.parse(g.json());
                    yield p instanceof List<?> l ? l.size() : 1;
                }
            };
        }

        List<Object> values() {
            return switch (result) {
                case com.legend.exec.ExecutionResult.Scalar sc ->
                        sc.value() == null ? List.of() : flatten(sc.value());
                case com.legend.exec.ExecutionResult.Collection c -> c.values();
                case com.legend.exec.ExecutionResult.Tabular t -> {
                    List<Object> out = new ArrayList<>();
                    t.rows().forEach(r -> out.addAll(r.values()));
                    yield out;
                }
                case com.legend.exec.ExecutionResult.Graph g -> {
                    Object p = ExecJson.parse(g.json());
                    yield p instanceof List<?> l ? new ArrayList<>(l) : List.of(p);
                }
            };
        }

        String render() {
            List<Object> v = values();
            return v.size() == 1 ? String.valueOf(v.get(0)) : String.valueOf(v);
        }

        /** A collection-literal root arrives as an ARRAY-valued scalar. */
        private static List<Object> flatten(Object v) {
            if (v instanceof List<?> l) {
                return new ArrayList<>(l);
            }
            if (v instanceof java.sql.Array arr) {
                try {
                    return new ArrayList<>(List.of((Object[]) arr.getArray()));
                } catch (java.sql.SQLException e) {
                    throw new IllegalStateException(e);
                }
            }
            return List.of(v);
        }
    }

    private static Eval eval(ValueSpecification expr,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        ValueSpecification spliced = substitute(expr, lets, handles, runtimeFqn);
        // SERIALIZATION TAILS (toCSV/toString over a TDS) strip: the grid
        // compares STRUCTURALLY (or renders for a string-literal peer) —
        // rendering is a wire concern, not a query. A tail whose receiver
        // turns out non-relational falls back to the original expression.
        boolean csv = false;
        // toCSV(tds)->replace(a, b): render the grid to CSV text, apply the
        // replace LITERALLY, compare as a string (the calendar family's
        // one-line assert spelling)
        if (spliced instanceof AppliedFunction rep
                && simpleName(rep.function()).equals("replace")
                && rep.parameters().size() == 3
                && rep.parameters().get(0) instanceof AppliedFunction innerCsv
                && simpleName(innerCsv.function()).equals("toCSV")
                && innerCsv.parameters().size() == 1
                && rep.parameters().get(1) instanceof CString from
                && "\n".equals(from.value())
                && rep.parameters().get(2) instanceof CString to) {
            com.legend.exec.ExecutionResult stripped2 = evalSpliced(
                    innerCsv.parameters().get(0), ctx, imports, runtimeFqn, conn);
            if (stripped2 instanceof com.legend.exec.ExecutionResult.Tabular tab2) {
                // structured compare: keep the TABULAR and the joined-line
                // separator — string-exact comparison broke on ROW ORDER
                // (unordered groupBy) and float ULPs (5.72 vs 5.7199...);
                // csvJoinedEquals below applies the header/multiset/
                // tolerant-cell policy instead
                return new Eval(stripped2,
                        endsInSort(innerCsv.parameters().get(0)), false,
                        "CSVJOIN:" + to.value());
            }
        }
        if (spliced instanceof AppliedFunction tail
                && (simpleName(tail.function()).equals("toCSV")
                        || simpleName(tail.function()).equals("toString"))
                && tail.parameters().size() == 1) {
            com.legend.exec.ExecutionResult stripped = evalSpliced(
                    tail.parameters().get(0), ctx, imports, runtimeFqn, conn);
            if (stripped instanceof com.legend.exec.ExecutionResult.Tabular) {
                return new Eval(stripped, endsInSort(tail.parameters().get(0)),
                        simpleName(tail.function()).equals("toCSV"));
            }
        }
        com.legend.exec.ExecutionResult r = evalSpliced(spliced, ctx, imports,
                runtimeFqn, conn);
        // A makeString/joinStrings tail over an UNSORTED chain: the joined
        // string's element order is the DB's incidental row order — record
        // the separator so the compare can fall back to split-multiset
        // (the ORDER POLICY at string granularity).
        String joinSep = null;
        if (spliced instanceof AppliedFunction jf
                && (simpleName(jf.function()).equals("makeString")
                        || simpleName(jf.function()).equals("joinStrings"))
                && jf.parameters().size() == 2
                && jf.parameters().get(1) instanceof CString sep
                && !endsInSort(jf.parameters().get(0))) {
            joinSep = sep.value();
        }
        return new Eval(r, endsInSort(spliced), csv, joinSep);
    }

    private static Object evalScalar(ValueSpecification expr,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        Eval e = eval(expr, lets, handles, ctx, imports, runtimeFqn, conn);
        List<Object> v = e.values();
        return v.size() == 1 ? v.get(0) : v;
    }

    /** Compile + execute ONE expression (handles already spliced in) —
     * delegates to THE one back-half sequence ({@link Compiler#executeResolved}). */
    private static com.legend.exec.ExecutionResult evalSpliced(ValueSpecification expr,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        LambdaFunction wrapped = new LambdaFunction(List.of(), List.of(expr));
        ValueSpecification resolved = NameResolver.resolveQuery(wrapped, imports,
                ctx.elementFqns());
        return Compiler.executeResolved(resolved, ctx, runtimeFqn, conn);
    }

    // ===== comparison (both sides share ONE wire convention — strict) =====

    private static boolean compare(Eval expected, Eval actual, boolean ordered) {
        // toCSV(..)->replace('\n', SEP) actual vs a string-literal expected:
        // header EXACT, rows as an (un)ordered multiset, CELLS via the
        // tolerant wire comparison (numeric ULP policy included)
        if (actual.joinSep() != null && actual.joinSep().startsWith("CSVJOIN:")
                && actual.result()
                        instanceof com.legend.exec.ExecutionResult.Tabular tj
                && expected.values().size() == 1
                && expected.values().get(0) instanceof String es) {
            return csvJoinedEquals(es,
                    actual.joinSep().substring("CSVJOIN:".length()), tj,
                    ordered && actual.sortedChain());
        }
        // TDS grids compare STRUCTURALLY: column names ordered, rows under
        // the order policy — both sides evaluated by the same pipeline
        if (expected.result() instanceof com.legend.exec.ExecutionResult.Tabular te
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular ta) {
            return gridEquals(te, ta, ordered && actual.sortedChain());
        }
        // toCSV() against a STRING literal: render the grid (wire concern);
        // header pinned, data lines under the order policy
        if (actual.csvTail()
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular tt
                && expected.values().size() == 1
                && expected.values().get(0) instanceof String es) {
            return csvEquals(es, tt, actual.sortedChain());
        }
        if (expected.csvTail()
                && expected.result() instanceof com.legend.exec.ExecutionResult.Tabular tt2
                && actual.values().size() == 1
                && actual.values().get(0) instanceof String as) {
            return csvEquals(as, tt2, true);
        }
        List<Object> e = expected.values();
        List<Object> a = actual.values();
        if (e.size() != a.size()) {
            return false;
        }
        // ORDER POLICY (the single deliberate leniency, documented): pure
        // assertEquals is ordered, but an actual side with NO sort in its
        // chain has no defined SQL row order — the engine's expectation
        // encodes H2's incidental order, ours is DuckDB's. Multiset-compare
        // exactly then; a sorted chain compares exactly ordered.
        if (ordered && actual.sortedChain()) {
            for (int i = 0; i < e.size(); i++) {
                if (!wireEquals(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (ordered) {
            // try ordered first — identical orders stay strongest evidence
            boolean ok = true;
            for (int i = 0; i < e.size() && ok; i++) {
                ok = wireEquals(e.get(i), a.get(i));
            }
            if (ok) {
                return true;
            }
        }
        // ORDER POLICY at STRING granularity: a makeString over an
        // unsorted chain joined the DB's incidental row order — compare
        // the split parts as a multiset.
        if (actual.joinSep() != null && !actual.joinSep().isEmpty()
                && e.size() == 1 && a.size() == 1
                && e.get(0) instanceof String es2 && a.get(0) instanceof String as2
                && !es2.equals(as2)) {
            List<String> ep = new ArrayList<>(List.of(
                    es2.split(java.util.regex.Pattern.quote(actual.joinSep()), -1)));
            List<String> ap = new ArrayList<>(List.of(
                    as2.split(java.util.regex.Pattern.quote(actual.joinSep()), -1)));
            if (ep.size() == ap.size()) {
                java.util.Collections.sort(ep);
                java.util.Collections.sort(ap);
                return ep.equals(ap);
            }
            return false;
        }
        // ROW COHESION (audit 9): an ORDERED compare's multiset fallback
        // (the order policy) must match ROW TUPLES, not loose cells —
        // cross-row cell shuffles must not compare equal. assertSameElements
        // stays a loose pool: the corpus itself writes its flat expected
        // sets column-grouped (testGreaterThanWithOptionalProperty), so
        // loose multiset IS that assert's reference semantics.
        if (ordered
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular tab
                && tab.columns().size() > 1
                && !(expected.result()
                        instanceof com.legend.exec.ExecutionResult.Tabular)
                && e.size() == a.size() && a.size() % tab.columns().size() == 0) {
            int w = tab.columns().size();
            List<List<Object>> ep = chunk(e, w);
            List<List<Object>> ap = chunk(a, w);
            for (List<Object> row : ep) {
                int hit = -1;
                for (int i = 0; i < ap.size(); i++) {
                    boolean all = true;
                    for (int j = 0; j < w && all; j++) {
                        all = wireEquals(row.get(j), ap.get(i).get(j));
                    }
                    if (all) {
                        hit = i;
                        break;
                    }
                }
                if (hit < 0) {
                    return false;
                }
                ap.remove(hit);
            }
            return true;
        }
        List<Object> pool = new ArrayList<>(a);
        for (Object x : e) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (wireEquals(x, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static List<List<Object>> chunk(List<Object> flat, int w) {
        List<List<Object>> out = new ArrayList<>(flat.size() / w);
        for (int i = 0; i + w <= flat.size(); i += w) {
            out.add(new ArrayList<>(flat.subList(i, i + w)));
        }
        return out;
    }

    /** Column-name + row-grid equality (rows ordered iff the chain sorts). */
    private static boolean gridEquals(com.legend.exec.ExecutionResult.Tabular expected,
            com.legend.exec.ExecutionResult.Tabular actual, boolean ordered) {
        if (expected.columns().size() != actual.columns().size()) {
            return false;
        }
        for (int i = 0; i < expected.columns().size(); i++) {
            if (!expected.columns().get(i).name().equals(actual.columns().get(i).name())) {
                return false;
            }
        }
        List<List<Object>> e = new ArrayList<>();
        expected.rows().forEach(r -> e.add(r.values()));
        List<List<Object>> a = new ArrayList<>();
        actual.rows().forEach(r -> a.add(r.values()));
        if (e.size() != a.size()) {
            return false;
        }
        if (ordered) {
            for (int i = 0; i < e.size(); i++) {
                if (!rowEquals(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        List<List<Object>> pool = new ArrayList<>(a);
        for (List<Object> row : e) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (rowEquals(row, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static boolean rowEquals(List<Object> e, List<Object> a) {
        if (e.size() != a.size()) {
            return false;
        }
        for (int i = 0; i < e.size(); i++) {
            if (!wireEquals(e.get(i), a.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** toCSV wire rendering vs an expected CSV string (header pinned). */
    /** {@code toCSV->replace('\n', sep)} against a string literal: tokens
     * split on {@code sep}; the first nCols are the HEADER (exact); the
     * rest group into rows of nCols compared as a multiset (ordered when
     * the chain sorts) with wireEquals cells (numeric tolerance). */
    private static boolean csvJoinedEquals(String expected, String sep,
            com.legend.exec.ExecutionResult.Tabular t, boolean ordered) {
        int n = t.columns().size();
        if (n == 0 || sep.isEmpty()) {
            return false;
        }
        List<String> tokens = new ArrayList<>(
                List.of(expected.split(java.util.regex.Pattern.quote(sep), -1)));
        // a trailing separator leaves one empty tail token
        if (!tokens.isEmpty() && tokens.get(tokens.size() - 1).isEmpty()) {
            tokens.remove(tokens.size() - 1);
        }
        if (tokens.size() < n || tokens.size() % n != 0) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (!t.columns().get(i).name().equals(tokens.get(i))) {
                return false;
            }
        }
        List<List<String>> expRows = new ArrayList<>();
        for (int i = n; i < tokens.size(); i += n) {
            expRows.add(tokens.subList(i, i + n));
        }
        List<List<Object>> actRows = new ArrayList<>();
        t.rows().forEach(r -> actRows.add(r.values()));
        if (expRows.size() != actRows.size()) {
            return false;
        }
        if (ordered) {
            for (int i = 0; i < expRows.size(); i++) {
                if (!csvRowEquals(expRows.get(i), actRows.get(i))) {
                    return false;
                }
            }
            return true;
        }
        List<List<Object>> pool = new ArrayList<>(actRows);
        for (List<String> er : expRows) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (csvRowEquals(er, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static boolean csvRowEquals(List<String> expected, List<Object> actual) {
        for (int i = 0; i < expected.size(); i++) {
            String e = expected.get(i);
            Object a = actual.get(i);
            String aCell = csvCell(a);
            if (e.equals(aCell)) {
                continue;
            }
            // numeric cells: the expected CSV prints ROUNDED (the engine
            // truncates to ~12 significant digits — 0.383333333333 vs our
            // 0.38333333333333336) — equal iff the actual rounds to the
            // expected at the EXPECTED's own printed precision
            try {
                double ev = Double.parseDouble(e);
                double av = a instanceof Number num ? num.doubleValue()
                        : Double.parseDouble(aCell);
                int dp = e.contains(".")
                        ? e.length() - e.indexOf('.') - 1 : 0;
                // Two leniencies, BOTH bounded by the same rationale (the
                // engine prints ~12 significant digits; H2 and DuckDB sum
                // doubles in different orders and addition is not
                // associative — the engine's own 12th digit moves,
                // testPwaValue):
                //   1. relative 1e-11 accumulation epsilon — always;
                //   2. half-ulp at the expected's PRINTED precision — ONLY
                //      when the printed token actually carries >= 10
                //      significant digits (i.e. it IS a ~12-sig-digit
                //      truncation artifact; a TRIMMED TRAILING ZERO can
                //      shave the visible count to 10: 0.05657370518 in
                //      testPwaValueOnStartYear). A coarse golden like
                //      '100' (audit 16: dp=0 granted +-0.5) is exact
                //      decimal output, not a truncation — it gets the
                //      relative floor only.
                int sig = 0;
                boolean seenNonZero = false;
                for (int ci = 0; ci < e.length(); ci++) {
                    char ch = e.charAt(ci);
                    if (ch >= '1' && ch <= '9') {
                        seenNonZero = true;
                    }
                    if (Character.isDigit(ch) && (seenNonZero || ch != '0')) {
                        sig++;
                    }
                }
                double tol = sig >= 10
                        ? Math.max(0.5 * Math.pow(10, -dp), Math.abs(ev) * 1e-11)
                        : Math.abs(ev) * 1e-11;
                if (Math.abs(av - ev) > tol) {
                    return false;
                }
            } catch (NumberFormatException nfe) {
                return false;
            }
        }
        return true;
    }

    /** The toCSV wire text: header line + one line per row, every line
     * newline-terminated (the engine's Result->toCSV convention). */
    private static String csvText(com.legend.exec.ExecutionResult.Tabular t) {
        StringBuilder header = new StringBuilder();
        for (var c : t.columns()) {
            if (header.length() > 0) {
                header.append(',');
            }
            header.append(c.name());
        }
        StringBuilder out = new StringBuilder(header).append('\n');
        for (var r : t.rows()) {
            StringBuilder line = new StringBuilder();
            for (Object v : r.values()) {
                if (line.length() > 0) {
                    line.append(',');
                }
                line.append(csvCell(v));
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }

    private static boolean csvEquals(String expected,
            com.legend.exec.ExecutionResult.Tabular actual, boolean sorted) {
        StringBuilder header = new StringBuilder();
        for (var c : actual.columns()) {
            if (header.length() > 0) {
                header.append(',');
            }
            header.append(c.name());
        }
        List<String> lines = new ArrayList<>();
        for (var r : actual.rows()) {
            StringBuilder line = new StringBuilder();
            for (Object v : r.values()) {
                if (line.length() > 0) {
                    line.append(',');
                }
                line.append(csvCell(v));
            }
            lines.add(line.toString());
        }
        String rendered = header + "\n"
                + lines.stream().map(l -> l + "\n").reduce("", String::concat);
        if (expected.equals(rendered)) {
            return true;
        }
        if (sorted) {
            return false;
        }
        // order policy: header line pinned, data lines as a multiset
        String[] el = expected.split("\n", -1);
        if (el.length == 0 || !el[0].equals(header.toString())) {
            return false;
        }
        List<String> pool = new ArrayList<>(lines);
        int dataLines = 0;
        for (int i = 1; i < el.length; i++) {
            if (el[i].isEmpty() && i == el.length - 1) {
                continue;   // trailing newline
            }
            dataLines++;
            if (!pool.remove(el[i])) {
                return false;
            }
        }
        return pool.isEmpty() && dataLines == lines.size();
    }

    /** STRICT wire equality: integral kinds normalize; decimal by compareTo; no cross-kind. */
    private static boolean isTemporal(Object v) {
        return v instanceof java.sql.Timestamp || v instanceof java.sql.Date
                || v instanceof java.time.LocalDate
                || v instanceof java.time.LocalDateTime
                || v instanceof java.time.OffsetDateTime;
    }

    private static boolean temporalEquals(String s, Object t) {
        // wire temporals are NAIVE (UTC-normalized); strip a UTC-zero
        // offset/zone suffix from the string form
        String v = s.trim().replaceFirst("(Z|\\+00(:?00)?|\\+0000)$", "")
                .replace('T', ' ').trim();
        try {
            if (t instanceof java.sql.Date d) {
                return java.time.LocalDate.parse(v).equals(d.toLocalDate());
            }
            if (t instanceof java.time.LocalDate ld) {
                return java.time.LocalDate.parse(v).equals(ld);
            }
            java.time.LocalDateTime other = t instanceof java.sql.Timestamp ts
                    ? ts.toLocalDateTime()
                    : t instanceof java.time.LocalDateTime ldt ? ldt
                    : t instanceof java.time.OffsetDateTime odt
                            ? odt.toLocalDateTime() : null;
            if (other == null) {
                return false;
            }
            String norm = v.contains(" ") ? v.replace(' ', 'T') : v + "T00:00";
            return java.time.LocalDateTime.parse(norm).equals(other);
        } catch (java.time.format.DateTimeParseException ex) {
            return false;
        }
    }

    /** RFC4180 cell rendering (the engine's toCSV): a cell containing a
     * comma, quote or newline wraps in quotes, inner quotes double. */
    private static String csvCell(Object v) {
        if (v == null) {
            return "";
        }
        String s = String.valueOf(v);
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0) {
            return s;
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    private static boolean wireEquals(Object e, Object a) {
        // the null-cell wire sentinel: an expected ^TDSNull() (or a TDS-grid
        // 'TDSNull' cell) equals an actual NULL cell — 'TDSNull' is never a
        // genuine string payload (established: real pure parses it as the
        // instance, our literals decode it to SQL NULL)
        if ("TDSNull".equals(e) && a == null) {
            return true;
        }
        // NO actual-side bridge (audit 16 F5): if a bug ever put the
        // literal string 'TDSNull' on OUR wire where a NULL belongs, the
        // symmetric grant would mask it — same refusal as the temporal
        // bridge below (audit 9)
        if (e == null || a == null) {
            return e == a;
        }
        boolean eInt = e instanceof Long || e instanceof Integer || e instanceof java.math.BigInteger;
        boolean aInt = a instanceof Long || a instanceof Integer || a instanceof java.math.BigInteger;
        if (eInt || aInt) {
            if (!(eInt && aInt)) {
                return false;
            }
            return ((Number) e).longValue() == ((Number) a).longValue();
        }
        if (e instanceof java.math.BigDecimal be && a instanceof java.math.BigDecimal ba) {
            return be.compareTo(ba) == 0;
        }
        boolean eFp = e instanceof Double || e instanceof Float || e instanceof java.math.BigDecimal;
        boolean aFp = a instanceof Double || a instanceof Float || a instanceof java.math.BigDecimal;
        if (eFp || aFp) {
            if (!(eFp && aFp)) {
                return false;
            }
            if (new java.math.BigDecimal(String.valueOf(e))
                    .compareTo(new java.math.BigDecimal(String.valueOf(a))) == 0) {
                return true;
            }
            // DIALECT-ARITHMETIC leniency (documented, the only float one):
            // corpus expectations encode H2's libm (ln/asin/acos/atan...);
            // DuckDB's differs in the LAST ULP on the same real number.
            // Two DOUBLE wire values within 2 ULP compare equal. Exact-zero,
            // kind and BigDecimal (pure Decimal) compares stay strict.
            if (e instanceof Double de && a instanceof Double da
                    && !de.isNaN() && !da.isNaN()) {
                double ulp = Math.ulp(Math.max(Math.abs(de), Math.abs(da)));
                return Math.abs(de - da) <= 2 * ulp;
            }
            return false;
        }
        // TEMPORAL through the Any-carrier: a mixed-collection LITERAL's
        // date decodes as its JSON STRING (the variant carrier is untyped
        // for temporals) — bridge by PARSING, value-exact, and ONLY in the
        // expected-string vs actual-temporal direction: an ACTUAL that
        // comes back as a string where the engine returns a Date is a
        // TYPING BUG this compare must catch (audit 9), never bridge.
        if (e instanceof String es && isTemporal(a)) {
            return temporalEquals(es, a);
        }
        if (e instanceof Map<?, ?> em && a instanceof Map<?, ?> am) {
            if (!em.keySet().equals(am.keySet())) {
                return false;
            }
            for (Object k : em.keySet()) {
                if (!wireEquals(em.get(k), am.get(k))) {
                    return false;
                }
            }
            return true;
        }
        return e.equals(a);
    }

    // ===== substitution: lets inline, handles splice =====

    private static boolean isExecuteCall(AppliedFunction af) {
        return af.function().equals("execute") && af.parameters().size() >= 2;
    }

    /**
     * A multi-statement query lambda ({@code {| let date = %d; Product.all(
     * $date)->...}}) folds its OWN lets into the final expression — pure
     * lets are single-assignment, so textual inlining is exact (the outer
     * statement driver does the same for test-body lets).
     */
    private static LambdaFunction inlineQueryLets(LambdaFunction lf) {
        if (lf.body().size() <= 1) {
            return lf;
        }
        Map<String, ValueSpecification> lets = new LinkedHashMap<>();
        ValueSpecification last = null;
        for (ValueSpecification stmt : lf.body()) {
            if (stmt instanceof AppliedFunction af
                    && af.function().equals("letFunction")
                    && af.parameters().size() == 2
                    && af.parameters().get(0) instanceof CString name) {
                lets.put(name.value(),
                        substitute(af.parameters().get(1), lets, Map.of(), null));
                continue;
            }
            last = substitute(stmt, lets, Map.of(), null);
        }
        return new LambdaFunction(lf.parameters(),
                List.of(last != null ? last
                        : lf.body().get(lf.body().size() - 1)));
    }

    private static ExecHandle toHandle(AppliedFunction ex) {
        return toHandle(ex, Map.of());
    }

    /** {@code lets}: a let-bound zero-arg lambda passed as the query
     * argument resolves through it ({@code let q = |...; execute($q, ...)}). */
    private static ExecHandle toHandle(AppliedFunction ex,
            Map<String, ValueSpecification> lets) {
        ValueSpecification q = ex.parameters().get(0);
        if (q instanceof Variable qv && lets.get(qv.name()) != null) {
            q = lets.get(qv.name());
        }
        if (!(q instanceof LambdaFunction lf) || !lf.parameters().isEmpty()) {
            return null;
        }
        return new ExecHandle(inlineQueryLets(lf), ex.parameters().get(1));
    }

    /**
     * Replace let-bound variables with their expressions and handle reads
     * with the SPLICED query: {@code $r.values} &rarr; the query chain
     * wrapped in {@code ->from(mapping, runtime? no — context rides the
     * driver runtime)}; head spellings {@code $r.values->at(0)} /
     * {@code ->toOne()} collapse onto the chain (the engine's Result.values
     * for a TDS query IS the single TDS). Shadowing lambda params stop
     * let-substitution.
     */
    private static ValueSpecification substitute(ValueSpecification v,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            String runtimeFqn) {
        // ^TDSNull() in a TEST literal is the engine's null-cell INSTANCE
        // (a real value, not a pure empty — an empty would VANISH from
        // [^TDSNull(), 5.0] and break the comparison): it travels as the
        // wire sentinel, which wireEquals equates with an actual null cell
        if (v instanceof NewInstance tn
                && (tn.className().equals("TDSNull")
                        || tn.className().equals("meta::pure::tds::TDSNull"))) {
            return new CString("TDSNull");
        }
        if (v instanceof AppliedFunction nf && nf.function().equals("new")
                && nf.parameters().size() == 2
                && nf.parameters().get(1) instanceof NewInstance tn2
                && (tn2.className().equals("TDSNull")
                        || tn2.className().equals("meta::pure::tds::TDSNull"))) {
            return new CString("TDSNull");
        }
        // $r.values → spliced query (with mapping context attached)
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable var
                && handles.containsKey(var.name())
                && ap.property().equals("values")) {
            return splice(handles.get(var.name()), runtimeFqn);
        }
        // $r.values->at(0) / ->toOne(): for a RELATION-rooted query the
        // values envelope holds ONE TDS — the wrapper collapses. For a
        // class/scalar root, values IS the collection: keep the wrapper as
        // a REAL selection over the spliced chain (audit 9). at(k>0) on a
        // relation root is loud — the envelope has one element.
        if (v instanceof AppliedFunction af
                && (af.function().equals("at") || af.function().equals("toOne"))
                && !af.parameters().isEmpty()
                && af.parameters().get(0) instanceof AppliedProperty ap2
                && ap2.receiver() instanceof Variable var2
                && handles.containsKey(var2.name())
                && ap2.property().equals("values")) {
            ExecHandle h2 = handles.get(var2.name());
            if (!h2.relationRooted()) {
                List<ValueSpecification> ps = new ArrayList<>(af.parameters());
                ps.set(0, splice(h2, runtimeFqn));
                return new AppliedFunction(af.function(), ps);
            }
            if (af.function().equals("at") && af.parameters().size() == 2
                    && !(af.parameters().get(1) instanceof CInteger k2
                            && k2.value().longValue() == 0)) {
                throw new IllegalStateException("Result.values->at(k>0) on a"
                        + " relation-rooted query — the values envelope holds"
                        + " one TDS");
            }
            return splice(h2, runtimeFqn);
        }
        // $tds->size() where $tds IS the peeled envelope (a relation-rooted
        // handle): ONE TDS value — pure size of a single instance, never
        // the row count (rows count as $tds.rows->size())
        if (v instanceof AppliedFunction sf && sf.function().equals("size")
                && sf.parameters().size() == 1
                && sf.parameters().get(0) instanceof Variable sv
                && handles.containsKey(sv.name())
                && handles.get(sv.name()).relationRooted()) {
            return new CInteger(1L);
        }
        return switch (v) {
            case Variable var when lets.containsKey(var.name()) -> lets.get(var.name());
            case Variable var when handles.containsKey(var.name()) ->
                    splice(handles.get(var.name()), runtimeFqn);
            // an INLINE execute (chained without a let: execute(...).values
            // or nested in an assert arg) splices in place
            case AppliedFunction af when isExecuteCall(af) && toHandle(af) != null ->
                    splice(toHandle(new AppliedFunction(af.function(),
                            substituteAll(af.parameters(), lets, handles, runtimeFqn))),
                            runtimeFqn);
            // instanceOf(cell, TDSNull): the TDSNull cell IS the SQL NULL
            // (LEFT-join miss) — the wire-exact test is isEmpty
            case AppliedFunction af when af.function().equals("instanceOf")
                    && af.parameters().size() == 2
                    && af.parameters().get(1) instanceof PackageableElementPtr pep
                    && (pep.fullPath().equals("meta::pure::tds::TDSNull")
                            || pep.fullPath().equals("TDSNull")) ->
                    new AppliedFunction("isEmpty", List.of(
                            substitute(af.parameters().get(0), lets, handles,
                                    runtimeFqn)));
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    substituteAll(af.parameters(), lets, handles, runtimeFqn));
            case AppliedProperty ap3 -> new AppliedProperty(
                    substitute(ap3.receiver(), lets, handles, runtimeFqn), ap3.property());
            case PureCollection pc -> new PureCollection(
                    substituteAll(pc.values(), lets, handles, runtimeFqn));
            case LambdaFunction lf -> {
                // shadowing params stop LET substitution (handles keep —
                // a lambda param can't shadow an execute binding usefully)
                Map<String, ValueSpecification> visible = new LinkedHashMap<>(lets);
                lf.parameters().forEach(p2 -> visible.remove(p2.name()));
                yield new LambdaFunction(lf.parameters(),
                        substituteAll(lf.body(), visible, handles, runtimeFqn));
            }
            default -> v;
        };
    }

    private static List<ValueSpecification> substituteAll(List<ValueSpecification> vs,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            String runtimeFqn) {
        List<ValueSpecification> out = new ArrayList<>(vs.size());
        for (ValueSpecification v : vs) {
            out.add(substitute(v, lets, handles, runtimeFqn));
        }
        return out;
    }

    /** The handle's query chain with its execution context attached. */
    private static ValueSpecification splice(ExecHandle h, String runtimeFqn) {
        ValueSpecification chain = h.query().body().get(h.query().body().size() - 1);
        // ->from(mapping, runtime): the 3-ARG form — the 2-arg overload is
        // (source, RUNTIME) and would read the mapping as a runtime. An
        // in-chain from() the corpus wrote itself already carries its own.
        if (containsFrom(chain)) {
            return chain;
        }
        return new AppliedFunction("from", List.of(chain, h.mappingRef(),
                new PackageableElementPtr(runtimeFqn)));
    }

    private static boolean containsFrom(ValueSpecification v) {
        if (v instanceof AppliedFunction af) {
            if (af.function().equals("from")) {
                return true;
            }
            for (ValueSpecification p : af.parameters()) {
                if (containsFrom(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The chain's OUTER tail carries a sort — its order is a contract. */
    private static boolean endsInSort(ValueSpecification v) {
        // names compare by SIMPLE name uniformly — an FQN-spelled sort must
        // still count as sorted (audit 9: raw-name matching left FQN
        // spellings silently lenient)
        if (!(v instanceof AppliedFunction af)) {
            return false;
        }
        String fn = simpleName(af.function());
        if (fn.equals("sort") || fn.equals("sortBy")) {
            return true;
        }
        // order survives through order-preserving tails only
        return switch (fn) {
            case "map", "limit", "take", "drop", "slice", "rows", "toOne", "at",
                    "makeString", "toCSV", "toString", "from" ->
                    !af.parameters().isEmpty() && endsInSort(af.parameters().get(0));
            default -> false;
        };
    }

    /** Minimal JSON reader for the GRAPH result envelope (core's own wire). */
    static final class ExecJson {
        private final String s;
        private int i;

        private ExecJson(String s) {
            this.s = s;
        }

        static Object parse(String json) {
            ExecJson p = new ExecJson(json);
            p.ws();
            Object v = p.value();
            p.ws();
            if (p.i < p.s.length()) {
                throw new IllegalStateException("trailing JSON at " + p.i);
            }
            return v;
        }

        private Object value() {
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> obj();
                case '[' -> arr();
                case '"' -> str();
                case 't' -> { i += 4; yield Boolean.TRUE; }
                case 'f' -> { i += 5; yield Boolean.FALSE; }
                case 'n' -> { i += 4; yield null; }
                default -> num();
            };
        }

        private Map<String, Object> obj() {
            Map<String, Object> out = new LinkedHashMap<>();
            i++;
            ws();
            if (s.charAt(i) == '}') {
                i++;
                return out;
            }
            while (true) {
                ws();
                String k = str();
                ws();
                i++;    // ':'
                ws();
                out.put(k, value());
                ws();
                if (s.charAt(i) == ',') {
                    i++;
                    continue;
                }
                i++;    // '}'
                return out;
            }
        }

        private List<Object> arr() {
            List<Object> out = new ArrayList<>();
            i++;
            ws();
            if (s.charAt(i) == ']') {
                i++;
                return out;
            }
            while (true) {
                ws();
                out.add(value());
                ws();
                if (s.charAt(i) == ',') {
                    i++;
                    continue;
                }
                i++;    // ']'
                return out;
            }
        }

        private String str() {
            StringBuilder b = new StringBuilder();
            i++;
            while (s.charAt(i) != '"') {
                char c = s.charAt(i);
                if (c == '\\') {
                    i++;
                    char e = s.charAt(i);
                    b.append(switch (e) {
                        case 'n' -> '\n';
                        case 't' -> '\t';
                        case 'r' -> '\r';
                        case 'b' -> '\b';
                        case 'f' -> '\f';
                        case 'u' -> {
                            char u = (char) Integer.parseInt(
                                    s.substring(i + 1, i + 5), 16);
                            i += 4;
                            yield u;
                        }
                        default -> e;
                    });
                } else {
                    b.append(c);
                }
                i++;
            }
            i++;
            return b.toString();
        }

        private Object num() {
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
                i++;
            }
            String t = s.substring(start, i);
            return t.contains(".") || t.contains("e") || t.contains("E")
                    ? (Object) Double.parseDouble(t) : (Object) Long.parseLong(t);
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
