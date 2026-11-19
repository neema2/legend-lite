// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.InferenceKernel;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.FunctionId;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DATABASE'S OWN SUBSTRING OVER THE ARGUMENTS AS GIVEN (A3 of the compatibility program, 2026-11-19; the native
 * {@code meta::legend::lite::substringAsGiven}, {@code SqlFn.SUBSTRING_AS_GIVEN}, and the {@code engine} rewrite's
 * substitute, {@link EngineSubstitutes}). Four claims, each with its partner:
 * <ul>
 *   <li>the substitute is the registry's function for the native's id, typed by the typer; each planted wrong callee
 *       or type is refused (D-S2 of the design's review);</li>
 *   <li>every dialect writes it as its plain call: no {@code GREATEST(..., 1)} and no raised literal start, where the
 *       same arguments under piece 1's {@code sqlSubstring} are clamped;</li>
 *   <li>on the two databases the {@code engine} setting is measured on (H2, DuckDB), its answer to each of the
 *       batteries' {@code substring} rows ({@code tools/engine-runner/src/test/resources/batteries/
 *       substring_indexof.tsv}, {@code col.s01}-{@code s18} and {@code lit.s01}-{@code s18}) is the database's own
 *       SUBSTRING's, run directly as legend-engine writes it;</li>
 *   <li>the native is the census's: internal, not user-resolvable, its one lowering rule the SCALAR_RULE.</li>
 * </ul>
 */
class EngineSubstitutesTest {

    /** The battery's rows' subject: the trades seed's distinct desks, in a one-column table. */
    private static final List<String> DESKS = List.of("Credit", "Equity", "FX", "Rates");

    /** The store, its connection's body {@code %s}, and the desks as the model's own data (a {@code ###Data} element:
     *  the plan's load, no fixture write). */
    private static final String STORE = String.join("\n",
            "###Relational",
            "Database t::DB ( Table DESKS ( DESK VARCHAR(32) PRIMARY KEY ) )",
            "###Connection",
            "RelationalDatabaseConnection t::Conn { store: t::DB; %s }",
            "###Runtime",
            "Runtime t::RT { mappings: []; connections: [ t::DB: [ c1: t::Conn ] ]; }",
            "###Data",
            "Data t::D",
            "{",
            "  Relational",
            "  #{",
            "    default.DESKS:",
            "      'DESK\\n" + String.join("\\n", DESKS) + "\\n';",
            "  }#",
            "}",
            "");

    private static final String TABLE = "#>{t::DB.DESKS}#->sort(~DESK->ascending())";

    /** The two databases whose answers the {@code engine} setting is measured on (the design's section 2). */
    private static final Set<String> MEASURED = Set.of("H2", "DuckDB");

    /** The batteries' substring rows: Pure's arguments after the subject ({@code $} the subject), and the same
     *  arguments as legend-engine writes them into SQL ({@code %s} the subject). */
    private record Row(String id, String pure, String sql) {
    }

    private static final List<Row> ROWS = List.of(
            new Row("s01", "0, 2", "%s, 0, 2"),
            new Row("s02", "1, 3", "%s, 1, 3"),
            new Row("s03", "2, 10", "%s, 2, 10"),
            new Row("s04", "0, 0", "%s, 0, 0"),
            new Row("s05", "3, 1", "%s, 3, 1"),
            new Row("s06", "-1, 2", "%s, -1, 2"),
            new Row("s07", "$->length() - 2, $->length()", "%s, length(%s) - 2, length(%s)"),
            new Row("s08", "1", "%s, 1"),
            new Row("s09", "0", "%s, 0"),
            new Row("s10", "4", "%s, 4"),
            new Row("s11", "-2", "%s, -2"),
            new Row("s12", "-7, 2", "%s, -7, 2"),
            new Row("s13", "-8, 2", "%s, -8, 2"),
            new Row("s14", "0, 1", "%s, 0, 1"),
            new Row("s15", "-3", "%s, -3"),
            new Row("s16", "-8", "%s, -8"),
            new Row("s17", "2, -1", "%s, 2, -1"),
            new Row("s18", "$->length() - 7, 3", "%s, length(%s) - 7, 3"));

    private static String model(String connectionBody) {
        return String.format(STORE, connectionBody);
    }

    private static boolean isPureSubstring(TypedSpec n) {
        return n instanceof TypedNativeCall c && Pure.AT_STRING_SUBSTRING.contains(c.callee().id());
    }

    /** {@code body} with each node {@code pick} accepts replaced by {@code by}'s answer, by identity, bottom-up. */
    private static List<TypedSpec> replace(List<TypedSpec> body, java.util.function.Predicate<TypedSpec> pick,
            UnaryOperator<TypedSpec> by) {
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec s : body) {
            out.add(replace(s, pick, by));
        }
        return out;
    }

    private static TypedSpec replace(TypedSpec n, java.util.function.Predicate<TypedSpec> pick,
            UnaryOperator<TypedSpec> by) {
        List<TypedSpec> kids = n.children();
        List<TypedSpec> next = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = replace(k, pick, by);
            changed |= r != k;
            next.add(r);
        }
        TypedSpec self = changed ? n.withChildren(next) : n;
        return pick.test(self) ? by.apply(self) : self;
    }

    private static int count(List<TypedSpec> body, java.util.function.Predicate<TypedSpec> pick) {
        int[] n = {0};
        replace(body, pick, s -> {
            n[0]++;
            return s;
        });
        return n[0];
    }

    /** {@code query} typed over {@code model}, each Pure {@code substring} in it the engine rewrite's substitute. */
    private static TypedQuery asGiven(String model, String query) {
        ModelContext ctx = Compiler.compileModel(model);
        TypedQuery q = Compiler.query(ctx, query);
        List<TypedSpec> body = replace(q.body(), EngineSubstitutesTest::isPureSubstring,
                s -> EngineSubstitutes.substring(ctx, (TypedNativeCall) s));
        assertTrue(count(q.body(), EngineSubstitutesTest::isPureSubstring) > 0, "the query holds a substring");
        assertEquals(0, count(body, EngineSubstitutesTest::isPureSubstring), "every substring is substituted");
        return q.withBody(body);
    }

    private static String query(String subject, String args) {
        return "|" + TABLE + "->extend(~x: r|" + subject + "->substring(" + args.replace("$", subject) + "))"
                + "->from(t::RT)";
    }

    private static final String COLUMN = "$r.DESK->toOne()";

    // ---- the substitute, typed as the typer types it (D-S2) --------------------------------------------------------

    @Test
    void theSubstituteIsTheRegistrysFunctionForEachArity_itsArgumentsAndTypeTheOriginals() {
        ModelContext ctx = Compiler.compileModel(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"));
        TypedQuery q = Compiler.query(ctx, "|" + TABLE + "->extend(~[a: r|" + COLUMN + "->substring(1, 3), b: r|"
                + COLUMN + "->substring(2)])->from(t::RT)");
        List<TypedNativeCall> seen = new ArrayList<>();
        replace(q.body(), EngineSubstitutesTest::isPureSubstring, s -> {
            seen.add((TypedNativeCall) s);
            return s;
        });
        assertEquals(2, seen.size(), seen.toString());
        for (TypedNativeCall original : seen) {
            TypedNativeCall sub = EngineSubstitutes.substring(ctx, original);
            FunctionId expected = FunctionId.of(original.args().size() == 3
                    ? Pure.SUBSTRING_AS_GIVEN__STRING_1__INTEGER_1__INTEGER_1
                    : Pure.SUBSTRING_AS_GIVEN__STRING_1__INTEGER_1);
            assertEquals(expected, sub.callee().id());
            assertEquals(ctx.findFunctionById(expected.qualified()), List.of(sub.callee()),
                    "the callee is the registry's function for the id, not a copy");
            assertEquals(original.args(), sub.args());
            assertEquals(original.info(), sub.info());
            assertSame(original.pos(), sub.pos());
        }
    }

    @Test
    void aPlantedWrongCalleeOrTypeIsRefused_eachWay() {
        ModelContext ctx = Compiler.compileModel(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"));
        TypedQuery q = Compiler.query(ctx, "|" + TABLE + "->extend(~a: r|" + COLUMN + "->substring(1, 3))"
                + "->from(t::RT)");
        TypedNativeCall[] original = new TypedNativeCall[1];
        replace(q.body(), EngineSubstitutesTest::isPureSubstring, s -> {
            original[0] = (TypedNativeCall) s;
            return s;
        });
        TypedNativeCall good = EngineSubstitutes.substring(ctx, original[0]);
        assertSame(good, EngineSubstitutes.checked(ctx, good, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN), "the registry's own passes");
        TypedFunction right = good.callee();

        // the other arity's overload
        TypedFunction twoArgument = ctx.findFunctionById(
                FunctionId.of(Pure.SUBSTRING_AS_GIVEN__STRING_1__INTEGER_1).qualified()).get(0);
        // piece 1's clamped native, over the same arguments
        TypedFunction sqlSubstring = new InferenceKernel(ctx).resolveOverload(ctx.findFunction(Pure.Lite.SQL_SUBSTRING),
                good.args().stream().map(TypedSpec::info).toList()).chosen();
        // a hand-assembled signature carrying the right id and definition, its return type wrong
        TypedFunction handMade = new TypedFunction(right.qualifiedName(), right.typeParameters(),
                right.multiplicityParameters(), right.parameters(), Type.Primitive.INTEGER, right.returnMultiplicity(),
                right.body(), right.isNative(), right.definition(), right.id());
        for (TypedFunction wrong : List.of(twoArgument, sqlSubstring, handMade, original[0].callee())) {
            TypedNativeCall planted = new TypedNativeCall(wrong, good.args(), good.info(), good.pos());
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> EngineSubstitutes.checked(ctx, planted, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN), wrong.qualifiedName());
            assertTrue(e.getMessage().contains("not the registry's"), e.getMessage());
        }
        // the right callee, a hand-made type
        TypedNativeCall wrongType = new TypedNativeCall(right, good.args(),
                new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_ONE), good.pos());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> EngineSubstitutes.checked(ctx, wrongType, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN));
        assertTrue(e.getMessage().contains("is typed"), e.getMessage());
    }

    @Test
    void theIndexOfSubstituteIsSqlPosition_andACallNoSubstituteStandsForIsRefused() {
        ModelContext ctx = Compiler.compileModel(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"));
        TypedQuery q = Compiler.query(ctx, "|" + TABLE + "->extend(~[a: r|" + COLUMN + "->indexOf('e'), b: r|"
                + COLUMN + "->indexOf('e', 2), c: r|" + COLUMN + "->substring(1)])->from(t::RT)");
        List<TypedNativeCall> indexOfs = new ArrayList<>();
        List<TypedNativeCall> substrings = new ArrayList<>();
        replace(q.body(), n -> n instanceof TypedNativeCall, s -> {
            TypedNativeCall c = (TypedNativeCall) s;
            if (Pure.AT_STRING_INDEX_OF.contains(c.callee().id())) {
                indexOfs.add(c);
            } else if (Pure.AT_STRING_SUBSTRING.contains(c.callee().id())) {
                substrings.add(c);
            }
            return s;
        });
        assertEquals(2, indexOfs.size(), indexOfs.toString());
        assertEquals(1, substrings.size(), substrings.toString());
        TypedNativeCall two = indexOfs.stream().filter(c -> c.args().size() == 2).findFirst().orElseThrow();
        TypedNativeCall three = indexOfs.stream().filter(c -> c.args().size() == 3).findFirst().orElseThrow();
        TypedNativeCall sub = EngineSubstitutes.indexOf(ctx, two);
        FunctionId position = FunctionId.of(Pure.SQL_POSITION__STRING_1__STRING_1);
        assertEquals(position, sub.callee().id());
        assertEquals(ctx.findFunctionById(position.qualified()), List.of(sub.callee()));
        assertEquals(two.args(), sub.args());
        assertEquals(two.info(), sub.info());
        assertSame(two.pos(), sub.pos());
        // the substitute holds to its own native's ids, not the other's
        assertSame(sub, EngineSubstitutes.checked(ctx, sub, Pure.AT_LEGEND_LITE_SQL_POSITION));
        assertThrows(RuntimeException.class,
                () -> EngineSubstitutes.checked(ctx, sub, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN));
        // the 3-argument indexOf has no substitute; neither entry takes the other's call
        for (Runnable wrong : List.<Runnable>of(() -> EngineSubstitutes.indexOf(ctx, three),
                () -> EngineSubstitutes.indexOf(ctx, substrings.get(0)),
                () -> EngineSubstitutes.substring(ctx, two))) {
            IllegalStateException e = assertThrows(IllegalStateException.class, wrong::run);
            assertTrue(e.getMessage().contains("substitutes"), e.getMessage());
        }
    }

    // ---- each dialect's SQL: the plain call ------------------------------------------------------------------------

    /** A clamp {@code SubstringClamp} writes round a computed start: {@code GREATEST(..., 1)}, Snowflake's
     *  {@code greatest_ignore_nulls}, SQLite's two-argument {@code max}. */
    private static final Pattern CLAMP = Pattern.compile("(?i)(greatest(_ignore_nulls)?|\\bmax)\\(");

    private static final OutputCol DESK = new OutputCol("DESK", SqlType.Scalar.VARCHAR, false,
            OutputCol.Origin.PHYSICAL);

    /** {@code SELECT fn(t.DESK, start, 3) AS v FROM DESKS AS t}, rendered by {@code type}'s dialect, its rewrites
     *  included. */
    private static String render(DatabaseType type, SqlFn fn, SqlExpr start) {
        SqlSource.Table desks = new SqlSource.Table("DESKS", "t", List.of(DESK), false, Map.of(),
                com.legend.sql.NameQuoting.BARE);
        SqlExpr call = new SqlExpr.Call(fn, List.of(SqlExpr.Column.of("t", DESK), start, new SqlExpr.IntLit(3)));
        SqlSelect q = new SqlSelect(List.of(new SqlSelect.Projection(call, "v", null)), false, desks, null, List.of(),
                null, null, List.of(), null, null,
                List.of(new OutputCol("v", SqlType.Scalar.VARCHAR, false, OutputCol.Origin.DERIVED)));
        return Databases.dialect(type).render(q);
    }

    @Test
    void everyDialectWritesThePlainCall_whereTheSameArgumentsAsSubstringAreClamped() {
        SqlExpr computed = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, SqlExpr.Column.of("t", DESK)),
                new SqlExpr.IntLit(7));
        SqlExpr zero = new SqlExpr.IntLit(0);
        SqlExpr two = new SqlExpr.IntLit(2);
        List<String> clamped = new ArrayList<>();
        List<String> relational = new ArrayList<>();
        for (DatabaseType type : Databases.implemented()) {
            String asGiven = render(type, SqlFn.SUBSTRING_AS_GIVEN, computed);
            String partner = render(type, SqlFn.SUBSTRING, computed);
            if (!Databases.dialect(type).relational()) {
                // MongoDB has no SUBSTRING of its own: its arm writes lite's $substrCP translation, whose start is
                // clamped in the pipeline ($max), for both functions alike -- so the label is not the database's own
                // reading there. The engine setting is not enabled on it (refused by name, the design's section 2)
                assertEquals(partner, asGiven, type.name());
                continue;
            }
            relational.add(type.name());
            assertFalse(CLAMP.matcher(asGiven).find(), type + ": " + asGiven);
            // the plain call: where nothing rewrites SUBSTRING (a start of 2), the two write the same text -- but on
            // Postgres, whose StringCountsAsInt casts SUBSTRING's counts alone
            if (!type.name().equals("Postgres")) {
                assertEquals(render(type, SqlFn.SUBSTRING, two), render(type, SqlFn.SUBSTRING_AS_GIVEN, two),
                        type.name());
            }
            if (CLAMP.matcher(partner).find()) {
                clamped.add(type.name());
                // a literal start below 1: SUBSTRING's raised to 1, as given its own
                assertEquals(render(type, SqlFn.SUBSTRING, new SqlExpr.IntLit(1)), render(type, SqlFn.SUBSTRING, zero),
                        type.name());
                assertFalse(render(type, SqlFn.SUBSTRING_AS_GIVEN, zero)
                        .equals(render(type, SqlFn.SUBSTRING_AS_GIVEN, new SqlExpr.IntLit(1))), type.name());
            }
        }
        assertEquals(11, relational.size(), relational.toString());
        // SubstringClamp's nine (BigQuery, ClickHouse, Databricks, DuckDB, MariaDB, Postgres, SQLite, Snowflake,
        // Trino) and MySQL, MariaDB's own chain; H2 reads a start below 1 itself
        assertEquals(10, clamped.size(), clamped.toString());
        assertFalse(clamped.contains("H2"), clamped.toString());
    }

    // ---- the answers: the database's own SUBSTRING, run directly ---------------------------------------------------

    @Test
    void eachBatteryRowAnswersAsTheDatabasesOwnSubstringRunDirectly_onTheMeasuredDatabases() throws Exception {
        int compared = 0;
        for (TestBackend b : TestBackends.all()) {
            if (!MEASURED.contains(b.type().name())) {
                continue;
            }
            String m = model(b.connectionBody());
            try (Connection c = b.open()) {
                for (Row row : ROWS) {
                    for (boolean literal : new boolean[] {false, true}) {
                        String subject = literal ? "'Credit'" : COLUMN;
                        String sqlSubject = literal ? "'Credit'" : "DESK";
                        Lean.Result lite = Lean.run(asGiven(m, query(subject, row.pure())), "t::RT", c);
                        List<String> liteAnswers = new ArrayList<>();
                        for (List<Object> r : lite.rows()) {
                            liteAnswers.add(r.get(1) == null ? null : r.get(1).toString());
                        }
                        // read after lite's run, whose plan loaded the model's data on this session
                        List<String> direct = new ArrayList<>();
                        String sql = "SELECT substring(" + row.sql().replace("%s", sqlSubject)
                                + ") FROM DESKS ORDER BY DESK";
                        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                            while (rs.next()) {
                                direct.add(rs.getString(1));
                            }
                        }
                        assertEquals(direct, liteAnswers, b.type() + " " + (literal ? "lit." : "col.") + row.id()
                                + ": " + sql);
                        compared++;
                    }
                }
            }
        }
        assertEquals(2 * 2 * ROWS.size(), compared);
    }

    // ---- the census ------------------------------------------------------------------------------------------------

    @Test
    void theNativeIsInternal_andAUserCannotNameItByAnyName() {
        assertTrue(Pure.INTERNAL_DESUGAR.contains("substringAsGiven"), Pure.INTERNAL_DESUGAR.toString());
        assertFalse(Pure.userResolvableFunctionFqns().contains(Pure.Lite.SUBSTRING_AS_GIVEN));
        assertEquals(2, Pure.AT_LEGEND_LITE_SUBSTRING_AS_GIVEN.size());
        assertTrue(Pure.COMPILER_OWN.contains(Pure.Lite.SUBSTRING_AS_GIVEN), Pure.COMPILER_OWN.toString());
        // a user's program cannot name it: by its full name, by an overload's id, or under eval (the compiler's own
        // natives' refusal, compiler.NameResolver); a bare name is none of the user's
        ModelContext ctx = Compiler.compileModel(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"));
        for (String query : List.of("|" + Pure.Lite.SUBSTRING_AS_GIVEN + "('abc', 1, 2)",
                "|" + Pure.Lite.SUBSTRING_AS_GIVEN + "('abc', 1)",
                "|" + com.legend.model.SignatureMangle.mangle(Pure.SUBSTRING_AS_GIVEN__STRING_1__INTEGER_1) + "('abc', 1)",
                "|" + Pure.Lite.SUBSTRING_AS_GIVEN + "->eval('abc', 1)")) {
            com.legend.error.LegendCompileException e = assertThrows(com.legend.error.LegendCompileException.class,
                    () -> Compiler.query(ctx, query), query);
            assertTrue(e.getMessage().contains("'" + Pure.Lite.SUBSTRING_AS_GIVEN + "' is the compiler's own"),
                    e.getMessage());
        }
        // a bare name is none of the user's: an unknown function, found when the query is typed (its body read)
        com.legend.error.TypeInferenceException bare = assertThrows(com.legend.error.TypeInferenceException.class,
                () -> Compiler.query(ctx, "|'abc'->substringAsGiven(1, 2)").body());
        assertTrue(bare.getMessage().contains("unknown function 'substringAsGiven'"), bare.getMessage());
    }
}
