// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
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
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DATABASE'S OWN SUBSTRING OVER THE ARGUMENTS AS GIVEN (A3 of the compatibility program, 2026-11-19; the native
 * {@code meta::legend::lite::substringAsGiven}, {@code SqlFn.SUBSTRING_AS_GIVEN}, which legend-engine's door's edition
 * table names for a store-bound {@code substring}: {@link EngineTable}; where the table lands is
 * {@code EngineTablePlacementTest}'s). Three claims, each with its partner:
 * <ul>
 *   <li>every relational dialect writes it as its plain call: no {@code GREATEST(..., 1)} and no raised literal start,
 *       where the same arguments under {@code SUBSTRING} are clamped; MongoDB, which has no SUBSTRING of its own,
 *       refuses it by name;</li>
 *   <li>on the two databases legend-engine's answer is checked on (H2, DuckDB), the door's answer to each of the
 *       batteries' {@code substring} rows ({@code tools/engine-runner/src/test/resources/batteries/
 *       substring_indexof.tsv}, {@code col.s01}-{@code s18} and {@code lit.s01}-{@code s18}) is the database's own
 *       SUBSTRING's, run directly as legend-engine writes it;</li>
 *   <li>the native is internal and the compiler's own: a user's call by any name is refused.</li>
 * </ul>
 */
class EngineSubstitutesTest {

    /** The battery's rows' subject: the trades seed's distinct desks, in a one-column table. */
    private static final List<String> DESKS = List.of("Credit", "Equity", "FX", "Rates");

    /** The store and its connection, the connection's body {@code %s}. */
    // (each element's literal carries its own ###Section header, so parser-equivalence reads the snippet as it stands:
    // its own-corpus check found the Database and the Runtime without theirs, 2026-11-20, the stage-A batch; the joined
    // text is unchanged)
    private static final String STORE = String.join("\n",
            "###Relational\nDatabase t::DB ( Table DESKS ( DESK VARCHAR(32) PRIMARY KEY ) )",
            "###Connection",
            "RelationalDatabaseConnection t::Conn { store: t::DB; %s }",
            "###Runtime\nRuntime t::RT { mappings: []; connections: [ t::DB: [ c1: t::Conn ] ]; }",
            "");

    /** A connection on {@code type} whose declared test data is the desks: the plan's load on the session it is given
     *  (a LocalH2 specification's {@code testDataSetupCSV}, upstream's carrier of it), no fixture write. */
    private static String withDesks(String type) {
        return "type: " + type + "; specification: LocalH2 { testDataSetupCSV: 'default\\nDESKS\\nDESK\\n"
                + String.join("\\n", DESKS) + "\\n'; }; auth: DefaultH2;";
    }

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

    /** The product's style handed legend-engine's table: the product's own dialects and session settings (the
     *  backends' sessions are the product's), every store-bound {@code substring} lowered by the table's row. */
    private static final com.legend.database.PlanStyle PRODUCT_WITH_THE_TABLE = new com.legend.database.PlanStyle() {
        @Override
        public com.legend.sql.dialect.SqlDialect dialect(DatabaseType type) {
            return com.legend.database.PlanStyle.PRODUCT.dialect(type);
        }

        @Override
        public TraceCell traceCell() {
            return com.legend.database.PlanStyle.PRODUCT.traceCell();
        }

        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            return kind == com.legend.lowering.EditionTable.class ? kind.cast(EngineTable.INSTANCE) : productForm;
        }

        @Override
        public String graphKeyDate(String written) {
            return com.legend.database.PlanStyle.PRODUCT.graphKeyDate(written);
        }

        @Override
        public String graphKeyString(String value) {
            return com.legend.database.PlanStyle.PRODUCT.graphKeyString(value);
        }

        @Override
        public <F> F facet(Class<F> kind) {
            return com.legend.database.PlanStyle.PRODUCT.facet(kind);
        }

        @Override
        public boolean judgesEngineText() {
            return com.legend.database.PlanStyle.PRODUCT.judgesEngineText();
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            return com.legend.database.PlanStyle.PRODUCT.facet(kind, refusal);
        }
    };

    private static String query(String subject, String args) {
        return "|" + TABLE + "->extend(~x: r|" + subject + "->substring(" + args.replace("$", subject) + "))"
                + "->from(t::RT)";
    }

    private static final String COLUMN = "$r.DESK->toOne()";

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
        int refused = 0;
        for (DatabaseType type : Databases.implemented()) {
            String partner = render(type, SqlFn.SUBSTRING, computed);
            if (!Databases.dialect(type).relational()) {
                // MongoDB has no SUBSTRING of its own (its SUBSTRING is lite's $substrCP translation, its start clamped
                // in the pipeline), so it refuses the function by name rather than answer lite's reading under the
                // "as given" name (the compat owner's ruling, 2026-11-19 ~21:12)
                assertTrue(partner.contains("$substrCP"), type + ": " + partner);
                com.legend.sql.DialectCapability e = assertThrows(com.legend.sql.DialectCapability.class,
                        () -> render(type, SqlFn.SUBSTRING_AS_GIVEN, computed), type.name());
                assertTrue(e.getMessage().contains("no SUBSTRING of its own"), e.getMessage());
                refused++;
                continue;
            }
            String asGiven = render(type, SqlFn.SUBSTRING_AS_GIVEN, computed);
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
        assertEquals(1, refused);
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
            String m = model(withDesks(b.type().name()));
            try (Connection c = b.open()) {
                for (Row row : ROWS) {
                    for (boolean literal : new boolean[] {false, true}) {
                        String subject = literal ? "'Credit'" : COLUMN;
                        String sqlSubject = literal ? "'Credit'" : "DESK";
                        Lean.Result lite = Lean.runInStyle(m, query(subject, row.pure()), "t::RT", c,
                                PRODUCT_WITH_THE_TABLE);
                        List<String> liteAnswers = new ArrayList<>();
                        for (List<Object> r : lite.rows()) {
                            liteAnswers.add(r.get(1) == null ? null : r.get(1).toString());
                        }
                        // read after lite's run, whose plan loaded the declared test data on this session
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

    // ---- substr and the corrected substring: substrAsGiven (A3, 2026-11-20; the user's word ~07:45, "Okay 1") -------

    /** substrAsGiven's one lowering rule: SUBSTRING_AS_GIVEN over start + 1 and end - start as SQL arithmetic the
     *  database computes, a literal index included (no fold in Java), and no clamp or range check. */
    @Test
    void substrAsGivenLowersToTheDatabasesSubstringOverSqlArithmetic_noFoldNoClampNoCheck() {
        ModelContext ctx = Compiler.compileModel(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"));
        TypedQuery q = Compiler.query(ctx, "|" + TABLE + "->extend(~a: r|" + COLUMN + "->substr(1, 3))->from(t::RT)");
        String sql = q.plan("t::RT", com.legend.edge.PureText.PURE_V1).sql().toLowerCase(java.util.Locale.ROOT);
        assertTrue(sql.matches("(?s).*substr(ing)?\\(\"?[a-z0-9_]*\"?\\.?desk, \\(?1 \\+ 1\\)?, \\(?3 - 1\\)?\\).*"), sql);
        assertFalse(sql.contains("case"), sql);
    }

    @Test
    void substrAsGivenIsInternal_andAUserCannotNameItByAnyName() {
        assertTrue(Pure.INTERNAL_DESUGAR.contains("substrAsGiven"), Pure.INTERNAL_DESUGAR.toString());
        assertFalse(Pure.userResolvableFunctionFqns().contains(Pure.Lite.SUBSTR_AS_GIVEN));
        assertEquals(2, Pure.AT_LEGEND_LITE_SUBSTR_AS_GIVEN.size());
        assertTrue(Pure.COMPILER_OWN.contains(Pure.Lite.SUBSTR_AS_GIVEN), Pure.COMPILER_OWN.toString());
        ModelContext ctx = Compiler.compileModel(model("type: H2; specification: LocalH2 { }; auth: DefaultH2;"));
        for (String query : List.of("|" + Pure.Lite.SUBSTR_AS_GIVEN + "('abc', 1, 2)",
                "|" + Pure.Lite.SUBSTR_AS_GIVEN + "('abc', 1)",
                "|" + com.legend.model.SignatureMangle.mangle(Pure.SUBSTR_AS_GIVEN__STRING_1__INTEGER_1) + "('abc', 1)",
                "|" + Pure.Lite.SUBSTR_AS_GIVEN + "->eval('abc', 1)")) {
            com.legend.error.LegendCompileException e = assertThrows(com.legend.error.LegendCompileException.class,
                    () -> Compiler.query(ctx, query), query);
            assertTrue(e.getMessage().contains("'" + Pure.Lite.SUBSTR_AS_GIVEN + "' is the compiler's own"),
                    e.getMessage());
        }
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
