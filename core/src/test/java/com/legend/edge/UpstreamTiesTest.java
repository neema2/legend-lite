// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.database.PlanStyle;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WHERE EQUAL NUMBERS OF DIFFERENT TYPES LAND, AT EACH DOOR (the edition program's step 6, slice 6.2; the user's
 * rulings of 2026-11-26 ~23:10: lite's rule in lite's mode, upstream's in the other two). Each expression is planned
 * and run at lite's own door, at legend-pure's and at legend-engine's two, and its answer is read as typed values: an
 * Integer 3 and a Float 3.0 are different answers here, though both print as a three.
 *
 * <p>Upstream's answers are not recalled. Each is taken from
 * {@code docs/records/edition-homework/upstream-step6/answers.md}, groups B and E, where interpreted legend-pure,
 * compiled legend-pure and legend-engine's own Java were each RUN on 2026-11-26 and agree on every row; the
 * two-argument {@code min} alone is READ, in the pinned source, and says so where it is held. Lite's own answers are
 * its rule: equal numbers are placed by their type (Integer, Float, Decimal), and two written values of two types
 * are never compared.
 */
class UpstreamTiesTest {

    /** A table with an Integer and a Float column, on the platform's own engine. */
    private static final String MODEL = """
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, I INTEGER, F FLOAT ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: %s;
                specification: LocalH2 { };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """.formatted(Databases.PLATFORM.name());

    private static final ModelContext CONTEXT = Compiler.compileModel(MODEL);

    /** A door: the style its plans are made in, and how it admits a typed query (each compatibility door answers a
     *  comparison across types its own way before anything is planned). */
    private enum Door {
        LITE(PlanStyle.PRODUCT) {
            @Override
            TypedQuery admitted(TypedQuery typed) {
                return typed;
            }
        },
        /** PCT's legend-pure run. */
        LEGEND_PURE(PureText.UPSTREAM) {
            @Override
            TypedQuery admitted(TypedQuery typed) {
                return PureComparisons.asPures(typed);
            }
        },
        /** legend-engine's API, where a statement that reads no store is stamped as its own Java's. */
        LEGEND_ENGINE(PureText.PURE_V1) {
            @Override
            TypedQuery admitted(TypedQuery typed) {
                return PureV1Routes.asLegendEngine(typed);
            }
        },
        /** PCT's legend-engine run, where nothing is stamped. */
        LEGEND_ENGINE_PCT(PureText.ENGINE_PCT) {
            @Override
            TypedQuery admitted(TypedQuery typed) {
                return EngineComparisons.asTheDatabases(typed);
            }
        };

        final PlanStyle style;

        Door(PlanStyle style) {
            this.style = style;
        }

        abstract TypedQuery admitted(TypedQuery typed);
    }

    private static final List<Door> UPSTREAMS = List.of(Door.LEGEND_PURE, Door.LEGEND_ENGINE, Door.LEGEND_ENGINE_PCT);

    private static ExecutionPlan plan(Door door, String query) {
        return door.admitted(Compiler.query(CONTEXT, query)).executionPlan("s::RT", java.util.Set.of(), door.style);
    }

    /** {@code query}'s answer at {@code door}, each value with its type. */
    private static List<String> answer(Door door, String query) throws Exception {
        ExecutionPlan plan = plan(door, query);
        try (Connection c = TestBackends.platform().open(door.style)) {
            List<String> out = new ArrayList<>();
            for (List<Object> row : PlanRunner.rows(plan, Map.of(), plan.sendsTheAnswer()
                    ? PlanSessions.acquired(TestSessions.over(c), plan.answerSql().target())
                    : PlanSessions.of(TestSessions.over(c)), java.util.UUID.randomUUID()).rows()) {
                assertEquals(1, row.size(), query);
                out.add(typed(row.get(0)));
            }
            return out;
        }
    }

    /** A value with its type's name: the carrier the typed seam hands says which number type it is. */
    private static String typed(Object cell) {
        Object value = cell instanceof com.legend.result.TaggedCell t ? t.value() : cell;
        if (value instanceof Long whole) {
            return "Integer " + whole;
        }
        if (value instanceof Double d) {
            return "Float " + d;
        }
        if (value instanceof BigDecimal exact) {
            return "Decimal " + exact.stripTrailingZeros().toPlainString();
        }
        if (value instanceof String text) {
            return "String " + text;
        }
        throw new AssertionError("a value of a kind this test reads none of: " + value.getClass().getName());
    }

    /** One expression: upstream's answer (answers.md's row {@code id}) and lite's own. */
    private record Case(String id, String query, List<String> upstream, List<String> lite) {
    }

    private static final String I3 = "Integer 3";
    private static final String F3 = "Float 3.0";
    private static final String D3 = "Decimal 3";
    private static final String I1 = "Integer 1";

    /** Group B's list forms: every row, upstream's answer as run on all three runtimes. */
    private static final List<Case> LISTS = List.of(
            new Case("B1", "|[3, 3.0]->max()", List.of(F3), List.of(F3)),
            new Case("B2", "|[3.0, 3]->max()", List.of(I3), List.of(F3)),
            new Case("B3", "|[3, 3.0]->min()", List.of(F3), List.of(I3)),
            new Case("B4", "|[3.0, 3]->min()", List.of(I3), List.of(I3)),
            new Case("B5", "|[3.0, 3, 1]->sort()", List.of(I1, F3, I3), List.of(I1, I3, F3)),
            new Case("B6", "|[3, 3.0, 1]->sort()", List.of(I1, I3, F3), List.of(I1, I3, F3)),
            new Case("B7", "|[3D, 3.0, 3]->sort()", List.of(D3, F3, I3), List.of(I3, F3, D3)),
            new Case("B9", "|[3.0, 'a', 3]->sort()", List.of(F3, I3, "String a"), List.of(I3, F3, "String a")),
            new Case("B9x", "|['a', 3, 3.0]->sort()", List.of(I3, F3, "String a"), List.of(I3, F3, "String a")));

    /** The forms of several arguments: upstream keeps the left of a tie (B8a and B8b run; {@code min} read in
     *  {@code min.pure:27-30}). */
    private static final Map<String, String> ARGUMENTS = arguments();

    private static Map<String, String> arguments() {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("|max(3, 3.0)", I3);
        out.put("|max(3.0, 3)", F3);
        out.put("|min(3, 3.0)", I3);
        out.put("|min(3.0, 3)", F3);
        return out;
    }

    @Test
    @DisplayName("lite's own door places equal numbers of different types by their type, wherever they are written")
    void litesDoorAnswersLitesRule() throws Exception {
        for (Case c : LISTS) {
            assertEquals(c.lite(), answer(Door.LITE, c.query()), c.id() + " " + c.query());
        }
    }

    @Test
    @DisplayName("legend-pure's door and legend-engine's two answer what all three upstream runtimes answered: a sort"
            + " keeps equal numbers as written, a list's max and min keep the last written of a tie")
    void theCompatibilityDoorsAnswerUpstreams() throws Exception {
        for (Door door : UPSTREAMS) {
            for (Case c : LISTS) {
                assertEquals(c.upstream(), answer(door, c.query()), door + " " + c.id() + " " + c.query());
            }
        }
    }

    @Test
    @DisplayName("a sort that is read further keeps its door's order: the sorted list printed and joined")
    void aSortReadFurtherKeepsItsDoorsOrder() throws Exception {
        String query = "|[3.0, 3, 1]->sort()->map(x | $x->toString())->joinStrings(',')";
        assertEquals(List.of("String 1,3,3.0"), answer(Door.LITE, query), "lite");
        for (Door door : UPSTREAMS) {
            assertEquals(List.of("String 1,3.0,3"), answer(door, query), door.toString());
        }
    }

    @Test
    @DisplayName("of several arguments the compatibility doors keep the first of a tie: max(3, 3.0) is the Integer")
    void ofSeveralArgumentsTheCompatibilityDoorsKeepTheFirst() throws Exception {
        for (Door door : UPSTREAMS) {
            for (Map.Entry<String, String> e : ARGUMENTS.entrySet()) {
                assertEquals(List.of(e.getValue()), answer(door, e.getKey()), door + " " + e.getKey());
            }
        }
    }

    /** Group E (run on all three runtimes the same day, after this session read the functions' bodies in the pinned
     *  source): {@code greatest} and {@code least} are the generic {@code max} and {@code min} over a list, a sort's
     *  last and a sort's first ({@code core_functions_standard/collection/max.pure:22-35}, {@code min.pure:22-35}).
     *  The sort keeps equal numbers as written, so {@code greatest} keeps the LAST written of a tie and
     *  {@code least}, with the generic {@code min} of a list that is not all numbers, the FIRST. Only the number
     *  {@code min} is the fold that keeps the last (B3, B4). */
    private static final List<Case> BY_THE_SORT = List.of(
            new Case("E1", "|[3, 3.0]->least()", List.of(I3), List.of(I3)),
            new Case("E2", "|[3.0, 3]->least()", List.of(F3), List.of(I3)),
            new Case("E3", "|[3, 3.0]->greatest()", List.of(F3), List.of(F3)),
            new Case("E4", "|[3.0, 3]->greatest()", List.of(I3), List.of(F3)),
            new Case("E5", "|[3, 3.0, 'a']->min()", List.of(I3), List.of(I3)),
            new Case("E6", "|[3.0, 3, 'a']->min()", List.of(F3), List.of(I3)),
            new Case("E7", "|[3, 3.0, 'a']->max()", List.of("String a"), List.of("String a")),
            new Case("E8", "|[3.0, 3, 'a']->max()", List.of("String a"), List.of("String a")));

    @Test
    @DisplayName("greatest keeps the last written of a tie and least the first at the compatibility doors, as the"
            + " sort they are written with does; lite's door keeps the one its type orders so")
    void greatestAndLeastFollowTheSort() throws Exception {
        for (Case c : BY_THE_SORT) {
            for (Door door : UPSTREAMS) {
                assertEquals(c.upstream(), answer(door, c.query()), door + " " + c.id() + " " + c.query());
            }
            assertEquals(c.lite(), answer(Door.LITE, c.query()), c.id() + " " + c.query());
        }
    }

    @Test
    @DisplayName("lite's own door refuses max and min of two numbers of different types where it compiles them: two"
            + " types never compare")
    void litesDoorRefusesSeveralArgumentsOfTwoTypes() {
        for (String query : ARGUMENTS.keySet()) {
            com.legend.error.ModelException refused = assertThrows(com.legend.error.ModelException.class,
                    () -> plan(Door.LITE, query), query);
            assertTrue(refused.getMessage().contains(" compares Integer with Float")
                    || refused.getMessage().contains(" compares Float with Integer"), refused.getMessage());
        }
    }

    // ---- legend-engine's own tests' door (the relational corpus) --------------------------------------------------

    /** {@code body}'s plan under {@code style}, as text: the body admitted as legend-engine's runners read it. */
    private static String planned(String body, PlanStyle style) {
        TypedQuery typed = UpstreamTestBodies.admitted(Compiler.query(CONTEXT, body));
        return com.legend.testexecutionplan.TestPlanJson.write(com.legend.TestExecution.planBody(typed, "s::RT",
                com.legend.ExecuteOptions.NONE.withStyle(style)
                        .withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.HOST)));
    }

    @Test
    @DisplayName("legend-engine's own tests' door: a test's own code, which that door stamps as outside any store"
            + " query, keeps upstream's ties, as legend-pure's door does")
    void theCorpussOwnCodeKeepsUpstreamsTies() {
        for (String body : List.of("{|[3.0, 3, 1]->sort();}", "{|[3.0, 3]->max();}", "{|max(3, 3.0);}")) {
            assertEquals(planned(body, PureText.UPSTREAM), planned(body, PureText.ENGINE_TESTS), body);
            assertNotEquals(planned(body, withNoRow(PureText.ENGINE_TESTS)), planned(body, PureText.ENGINE_TESTS),
                    body + ": the row applies to the test's own code");
        }
    }

    // ---- the rows add nothing where no two number types meet ------------------------------------------------------

    /** {@code base} with the product's edition in its own's place: the same door's plan with no row at all. What a
     *  row's effect is read against (as {@code EngineTablePlacementTest} reads legend-engine's rows). */
    private static PlanStyle withNoRow(PlanStyle base) {
        return new PlanStyle() {
            @Override
            public SqlDialect dialect(DatabaseType type) {
                return base.dialect(type);
            }

            @Override
            public TraceCell traceCell() {
                return base.traceCell();
            }

            @Override
            public <F> F twoForm(Class<F> kind, F productForm) {
                return kind == com.legend.planner.Edition.class ? productForm : base.twoForm(kind, productForm);
            }

            @Override
            public String graphKeyDate(String written) {
                return base.graphKeyDate(written);
            }

            @Override
            public String graphKeyString(String value) {
                return base.graphKeyString(value);
            }

            @Override
            public <F> F facet(Class<F> kind) {
                return base.facet(kind);
            }

            @Override
            public boolean judgesEngineText() {
                return base.judgesEngineText();
            }

            @Override
            public <F> F facet(Class<F> kind, Supplier<? extends RuntimeException> refusal) {
                return base.facet(kind, refusal);
            }
        };
    }

    private static String sql(Door door, PlanStyle style, String query) {
        return door.admitted(Compiler.query(CONTEXT, query)).plan("s::RT", style).sql();
    }

    /** Calls of the rows' functions where no two number types meet: a list of one number type, a sort of texts, a
     *  sort by a key and by a comparator, an extreme of whole numbers or of texts. */
    private static final List<String> ONE_TYPE = List.of(
            "|[3, 1, 2]->sort()",
            "|[2.5, 1.5]->sort()",
            "|['b', 'a', 'c']->sort()",
            "|['b', 'a', 'c']->sort({x, y | $y->compare($x)})",
            "|['bb', 'a', 'ccc']->sort(x | $x->length(), {x, y | $x->compare($y)})",
            "|[3, 1, 2]->max()",
            "|[3, 1, 2]->min()",
            "|[2.5, 1.5]->max()",
            "|max(1, 2)",
            "|min(1.5, 2.5)",
            "|[3, 1, 2]->greatest()",
            "|[3, 1, 2]->least()",
            "|['b', 'a']->greatest()");

    @Test
    @DisplayName("a list of one number type and a sort of non-numbers are the same SQL, byte for byte, with the rows"
            + " and without them")
    void theRowsAddNothingWhereNoTwoNumberTypesMeet() {
        for (Door door : UPSTREAMS) {
            for (String query : ONE_TYPE) {
                assertEquals(sql(door, withNoRow(door.style), query), sql(door, door.style, query),
                        door + " " + query);
            }
        }
    }

    @Test
    @DisplayName("the same calls are one SQL text at lite's door, legend-pure's and legend-engine's")
    void oneTextAtTheThreeDoors() {
        for (String query : ONE_TYPE) {
            String lites = sql(Door.LITE, PlanStyle.PRODUCT, query);
            for (Door door : UPSTREAMS) {
                assertEquals(lites, sql(door, door.style, query), door + " " + query);
            }
        }
    }

    // ---- inside a store query, and on any database ----------------------------------------------------------------

    /** A mixed extreme over a row's two columns, printed (a mixed extreme is returned, compared or printed: a
     *  product limit at every door, so a column of one is not planned). */
    private static final String IN_A_STORE = "|#>{s::DB.T}#->extend(~m: r | [$r.F->toOne(), $r.I->toOne()]->max()"
            + "->toString())->select(~[m])->from(s::RT)";

    @Test
    @DisplayName("the rows answer inside a store query too: legend-engine's door reads them before the stamp, and"
            + " the three compatibility doors write one statement")
    void theRowsAnswerInsideAStoreQuery() {
        for (Door door : UPSTREAMS) {
            assertNotEquals(sql(door, withNoRow(door.style), IN_A_STORE), sql(door, door.style, IN_A_STORE),
                    door + ": the row applies to a store query's call");
        }
        assertEquals(sql(Door.LEGEND_PURE, PureText.UPSTREAM, IN_A_STORE),
                sql(Door.LEGEND_ENGINE, PureText.PURE_V1, IN_A_STORE));
        assertEquals(sql(Door.LEGEND_PURE, PureText.UPSTREAM, IN_A_STORE),
                sql(Door.LEGEND_ENGINE_PCT, PureText.ENGINE_PCT, IN_A_STORE));
    }

    @Test
    @DisplayName("on a database where legend-engine's own functions are not yet checked, these rows are answered,"
            + " never refused: the answer is lite's own SQL, the same on every database")
    void theRowsAreNeverRefusedByDatabase() {
        ModelContext unchecked = Compiler.compileModel(EngineTablePlacementTest.UNCHECKED);
        for (String query : List.of("|[3.0, 3, 1]->sort()", "|[3.0, 3]->max()", "|max(3, 3.0)",
                "|" + EngineTablePlacementTest.TABLE + "->extend(~m: r | [2.5, $r.ID->toOne()]->max()->toString())"
                        + "->from(t::RT)")) {
            TypedQuery admitted = PureV1Routes.asLegendEngine(Compiler.query(unchecked, query));
            // planned in the door's style: a refusal would be raised here, while the statement is planned; and the
            // row applied, the statement is not the one the door writes with no row
            assertNotEquals(admitted.plan("t::RT", withNoRow(PureText.PURE_V1)).sql(),
                    admitted.plan("t::RT", PureText.PURE_V1).sql(), query);
        }
    }
}
