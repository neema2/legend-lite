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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            ###Pure
            function u::same(a: Number[1], b: Number[1]): Boolean[1] { $a == $b }
            function u::less(a: Number[1], b: Number[1]): Boolean[1] { $a < $b }
            function u::amongWholes(a: Number[1]): Boolean[1] { $a->in([0, 1]) }
            function u::amongFloats(a: Number[1]): Boolean[1] { $a->in([0.0, 1.0]) }
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
        if (value instanceof Boolean truth) {
            return "Boolean " + truth;
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
            // (lite: an exact number before a Float, and two equal exact numbers in the order they were written --
            // the user's ruling of 2026-11-27; until then Integer, Float, Decimal)
            new Case("B7", "|[3D, 3.0, 3]->sort()", List.of(D3, F3, I3), List.of(D3, I3, F3)),
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
    @DisplayName("numbers of different types in a mixed list: lite's two families; the compatibility doors keep three"
            + " types never equal")
    void mixedNumbersKeepThreeTypesAtTheCompatibilityDoors() throws Exception {
        // the user's ruling of 2026-11-27: lite's numbers are of two families (an Integer and a Decimal equal by
        // value), and "legend-pure's and legend-engine's modes keep their own behaviour" -- each door answers what it
        // answered before the ruling, interpreted legend-pure's answer on every row (answers.md, group A: A1, A4,
        // A5, A7; compiled legend-pure says true on the first three and legend-engine on the third, as recorded)
        Map<String, String[]> rows = new java.util.LinkedHashMap<>();
        rows.put("|[3, 'a']->contains(3D)", new String[] {"Boolean true", "Boolean false"});
        rows.put("|3->in([3D, 'a'])", new String[] {"Boolean true", "Boolean false"});
        rows.put("|[3, 'a'] == [3D, 'a']", new String[] {"Boolean true", "Boolean false"});
        rows.put("|[3, 3D]->removeDuplicates()->size()", new String[] {"Integer 1", "Integer 2"});
        // and what no rule moves: an exact number never equals a Float, at any door
        rows.put("|[1, 'a']->contains(1.0)", new String[] {"Boolean false", "Boolean false"});
        rows.put("|[1, 1.0]->removeDuplicates()->size()", new String[] {"Integer 2", "Integer 2"});
        for (Map.Entry<String, String[]> e : rows.entrySet()) {
            assertEquals(List.of(e.getValue()[0]), answer(Door.LITE, e.getKey()), "lite " + e.getKey());
            for (Door door : UPSTREAMS) {
                assertEquals(List.of(e.getValue()[1]), answer(door, e.getKey()), door + " " + e.getKey());
            }
        }
    }

    @Test
    @DisplayName("a function written for Number: legend-pure's door answers by the types its arguments have, as"
            + " legend-pure does")
    void aFunctionWrittenForNumberAtLegendPuresDoor() throws Exception {
        // run 2026-11-26 on interpreted and compiled legend-pure (answers.md, group D: D1 to D6): two types are
        // never equal, a literal written in the function is NOT read as the argument's type, an ordering is by
        // value. Until the edition program's step 6 legend-pure's door sent a function's comparison to the database,
        // which answered same(1, 1.0) true
        Map<String, String> pure = new java.util.LinkedHashMap<>();
        pure.put("|u::same(1, 1.0)", "Boolean false");
        pure.put("|u::same(1, 1)", "Boolean true");
        pure.put("|u::amongWholes(1.0)", "Boolean false");
        pure.put("|u::amongWholes(1)", "Boolean true");
        pure.put("|u::amongFloats(1)", "Boolean false");
        pure.put("|u::amongFloats(1.0)", "Boolean true");
        pure.put("|u::less(1, 1.0)", "Boolean false");
        pure.put("|u::less(1, 2.0)", "Boolean true");
        for (Map.Entry<String, String> e : pure.entrySet()) {
            assertEquals(List.of(e.getValue()), answer(Door.LEGEND_PURE, e.getKey()), e.getKey());
        }
        // lite's own door: by the types too, but a literal the function's author wrote reads as the argument's type
        assertEquals(List.of("Boolean false"), answer(Door.LITE, "|u::same(1, 1.0)"));
        assertEquals(List.of("Boolean true"), answer(Door.LITE, "|u::amongWholes(1.0)"));
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

    @Test
    @DisplayName("a date built to the hour against a date-time: upstream reads the written precision as part of the"
            + " value, lite compares the two by their instant")
    void aDateBuiltToTheHourIsComparedByItsWrittenPrecisionAtTheDoors() {
        // run 2026-11-27 on interpreted and compiled legend-pure and legend-engine's own Java
        // (docs/records/edition-homework/upstream-step6/answers-request-3-2026-11-27.md, group D): against a full
        // date-time false, against a date written to the same hour true
        String full = "|date(2015, 4, 16, 14) == %2015-04-16T14:00:00";
        String sameHour = "|date(2015, 4, 16, 14) == %2015-04-16T14";
        java.util.function.BiFunction<Door, String, Boolean> never = (door, query) -> {
            String text = sql(door, door.style, query).toLowerCase(java.util.Locale.ROOT);
            return text.contains("concat('l'") && text.contains("concat('r'");
        };
        for (Door door : List.of(Door.LEGEND_PURE, Door.LEGEND_ENGINE)) {
            assertTrue(never.apply(door, full), door + ": never equal, both sides read");
            assertTrue(sql(door, door.style, sameHour).contains("= '2015-04-16T14'"),
                    door + ": the same where the two are written alike: " + sql(door, door.style, sameHour));
            // a date built to the hour against one written to the MINUTE: two written precisions, never equal
            assertTrue(never.apply(door, "|date(2015, 4, 16, 14) == %2015-04-16T14:00"), door + ": two precisions");
            // a date built with a fraction of a second is a FULL date-time, carried as text only for its digits: by
            // the instant, as before (the code audit of the closing batch, B2: read by its carrier it was "never
            // equal" to a literal of its own precision)
            assertFalse(never.apply(door, "|date(2015, 4, 16, 14, 30, 15.5) == %2015-04-16T14:30:15.5"),
                    door + ": a full date-time is compared by its instant");
        }
        // lite's own rule: by the instant (register S399), no mark of never-equal
        assertFalse(never.apply(Door.LITE, full));
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

    /** {@code max} and {@code min} of an Integer column and a Float column inside a store query (the user,
     *  2026-11-27: "great to match engine but only if not too complex and costly"): legend-engine writes the
     *  database's own CASE over the two there, so the answer is a Float whichever wins -- (3, 3.0) answers 3.0 and
     *  {@code min} of (2, 2.5) answers 2.0, RUN on legend-engine 2026-11-26 (answers.md rows S9b and K5). Lite's
     *  door for legend-engine's API writes the database's GREATEST or LEAST over the Integer converted and the Float;
     *  outside a store query, and at the two doors that do not tell inside from outside, the winner keeps its type. */
    @Test
    @DisplayName("max and min of an Integer and a Float column at legend-engine's API door: a Float, the database's"
            + " type for the two")
    void anIntegerAndAFloatColumnsExtremeIsAFloatAtLegendEnginesApi() {
        String max = "|#>{s::DB.T}#->extend(~m: r | max($r.I->toOne(), $r.F->toOne()))->select(~[m])->from(s::RT)";
        String min = "|#>{s::DB.T}#->extend(~m: r | min($r.F->toOne(), $r.I->toOne()))->select(~[m])->from(s::RT)";
        // legend-engine's own CASE (max: when l < r then r else l; min: when r < l then r else l), the Integer converted
        String engines = sql(Door.LEGEND_ENGINE, PureText.PURE_V1, max);
        assertTrue(engines.contains("CASE WHEN CAST(t0.I AS DOUBLE) < t0.F THEN t0.F ELSE CAST(t0.I AS DOUBLE) END"),
                engines);
        String least = sql(Door.LEGEND_ENGINE, PureText.PURE_V1, min);
        assertTrue(least.contains("CASE WHEN CAST(t0.I AS DOUBLE) < t0.F THEN CAST(t0.I AS DOUBLE) ELSE t0.F END"),
                least);
        // legend-engine's PCT door tells no inside from outside, and legend-pure's has no such rule: the winner keeps
        // its type there, as before, and a column of such a value is the product's limit it was (measured 2026-11-27)
        for (Door untouched : List.of(Door.LEGEND_ENGINE_PCT, Door.LEGEND_PURE)) {
            RuntimeException limit = assertThrows(RuntimeException.class, () -> sql(untouched, untouched.style, max));
            assertTrue(String.valueOf(limit.getMessage()).contains("a tagged value reached the renderer"),
                    untouched + ": " + limit.getMessage());
        }
        // two written values keep the winner's type, outside any store query (ARGUMENTS holds the answer) and inside
        // one (run on legend-engine's H2 2026-11-27: max(3, 3.0) there is 3): never this rule's statement
        for (String written : List.of("|max(3, 3.0)",
                "|#>{s::DB.T}#->extend(~m: r | max(3, 3.0)->toString())->select(~[m])->from(s::RT)")) {
            assertFalse(sql(Door.LEGEND_ENGINE, PureText.PURE_V1, written).contains("CASE WHEN CAST(3 AS DOUBLE) < 3.0"),
                    written);
        }
        // two columns of one type: the call's own rule, no conversion
        assertFalse(sql(Door.LEGEND_ENGINE, PureText.PURE_V1,
                "|#>{s::DB.T}#->extend(~m: r | max($r.I->toOne(), $r.ID->toOne()))->select(~[m])->from(s::RT)")
                .contains("AS DOUBLE)"));
    }

    @Test
    @DisplayName("an equality one side of which may be empty: lite and legend-pure answer false there; legend-engine"
            + " as it does, by where the comparison runs")
    void anEmptySideIsAnsweredAsEachAudienceAnswersIt() {
        // run 2026-11-27 (docs/records/edition-homework/upstream-step6/answers-request-3-2026-11-27.md): inside its
        // store query legend-engine writes a plain '=' for a column against a value (no value where the column is
        // empty), and outside any store query its own Java answers false, as Pure does
        // (the guard is one COALESCE, spelled in the database's case)
        java.util.function.BiFunction<Door, String, Boolean> guarded =
                (door, query) -> sql(door, door.style, query).toUpperCase(java.util.Locale.ROOT).contains("COALESCE(");
        String inside = "|#>{s::DB.T}#->extend(~e: r | $r.I == 1)->select(~[e])->from(s::RT)";
        String outside = "|[1, 2]->filter(x | $x > 5)->first() == 1";
        assertTrue(guarded.apply(Door.LITE, inside), "lite, a column's value");
        assertTrue(guarded.apply(Door.LEGEND_PURE, inside), "legend-pure, the same");
        assertFalse(guarded.apply(Door.LEGEND_ENGINE, inside),
                "legend-engine inside a store query: the database's own answer");
        assertTrue(guarded.apply(Door.LEGEND_ENGINE, outside),
                "legend-engine outside any store query: false, as its own Java answers");
        // a filter is a positive condition at every door: as written
        String filtered = "|#>{s::DB.T}#->filter(r | $r.I == 1)->select(~[ID])->from(s::RT)";
        for (Door door : List.of(Door.LITE, Door.LEGEND_PURE, Door.LEGEND_ENGINE)) {
            assertFalse(guarded.apply(door, filtered), door + ": a filter's condition");
        }
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
