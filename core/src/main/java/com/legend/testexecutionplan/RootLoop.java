// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * THE DATABASE JUDGE'S ROOT LOOP AS DATA (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 5, 2026-11-08): one
 * segment's statement-root asserts in body order, each with its steps -- a verdict row the plan reads, by its verdict's
 * id, or an outcome decided at planning -- and its caller's message. The run judges them in this order once the
 * segment's last verdict group has run, the first failure raising (the walk's first-failure sequencing). Carried on the
 * segment's last group ({@link TestPlan.Verdicts#roots}). Until this step the planner's batch held the same facts in
 * maps keyed by object identity, and judged them itself.
 *
 * @param roots     the segment's asserts, in body order
 * @param fragments each frame name of the body and the let or assert it came from: a database error on the segment's
 *                  fused statement names the statement that owns it (the census's reason)
 */
public record RootLoop(List<Root> roots, Map<String, String> fragments) {


    /** No appeal: a host engine-text comparison's failure is the assert's ({@code HostJudgment.EdgeJudged}; a database
     *  row names none since A4 of the compatibility program, stage A ratified by the user 2026-11-19). */
    public static final int NO_APPEAL = 0;

    /** A typed verdict row's verdict, by position: its first column, as every typed member selects it (pinned by
     *  {@code VerdictRowOrderTest}); the run reads a row by position, never by the compiler's schema (slice 5,
     *  2026-11-09; item 3's S4: the seven-column row and its sentence gone). */
    public static final int VERDICT_COLUMN = 0;

    public RootLoop {
        roots = List.copyOf(roots);
        fragments = Collections.unmodifiableMap(new LinkedHashMap<>(fragments));
    }

    /**
     * One statement-root assert.
     *
     * @param listenerName the name the test listener hears its verdict under
     * @param steps        its rows and decided outcomes, in the order they decide (an unrolled assert is several rows)
     * @param message      its caller's message where it READS (a side read only where the assert fails), or null
     * @param unplanned    computing its caller's message raises an error upstream raises at run, found while planning:
     *                     that error, raised in place of the assert's failure where the root fails and only there (item
     *                     3, 2026-11-21, the user's ruling: the message lazy, its error the outcome; the host judge's
     *                     {@code HostJudgment.Messaged#unplanned}); null where the message planned, or there is none
     */
    public record Root(String listenerName, List<Step> steps, @Nullable Message message, @Nullable Failure unplanned) {
        public Root {
            Objects.requireNonNull(listenerName, "listenerName");
            steps = List.copyOf(steps);
            if (message != null && unplanned != null) {
                throw new IllegalArgumentException(listenerName + ": a message that planned and one that did not");
            }
        }

        /** A root whose message, if any, planned. */
        public Root(String listenerName, List<Step> steps, @Nullable Message message) {
            this(listenerName, steps, message, null);
        }
    }

    /**
     * A root's caller's message that reads: the side the plan reads for it (a message the dialect cannot spell refuses
     * the body at planning, D: its refusal slot went 2026-11-21, stage B's BE, always empty since D).
     *
     * @param side    the message's statement, a side of the plan
     * @param carried whether a verdict of the plan carries the side (a root every member of which was decided at
     *                planning has no statement to carry it: its message is refused by name)
     */
    public record Message(TestPlan.Side side, boolean carried) {
        public Message {
            Objects.requireNonNull(side, "a caller's message's side");
        }
    }

    /** A root's step. */
    public sealed interface Step permits Row, Decided, Judged {
    }

    /**
     * A verdict the database judge decided at planning (a static decision: two primitive kinds that differ, a grid's
     * columns, a rendered header, a statically identified {@code is} pair): the verdict census counts it at its turn,
     * so a plan run twice counts it twice and a root not run counts nothing (J39, 2026-11-18: until then planning
     * counted it as it planned). It carries no name: the census counts it under its root's listener name.
     */
    public record Judged() implements Step {
    }

    /**
     * A verdict row the database computes.
     *
     * @param name    the row's assert name (the row's own message names it)
     * @param verdict the id of the plan's verdict whose row this is; {@code message} / {@code unplanned}: an unrolled
     *                element's own caller's message that reads, or its error, used where THIS row fails (item 3's S4
     *                step 3, the user's decision 4) (a row the dialect cannot spell refuses the body at
     *                planning, D: its refusal slot and NO_VERDICT went 2026-11-21, stage B's BE, always empty since D)
     *
     * <p>(Three fields went with A4 of the compatibility program, stage A ratified by the user 2026-11-19, the test
     * plan's version 33: {@code orElse}, the row's ruled comparison -- the rendered-text rule's rows fallback,
     * compatibility mode's S131/S132 --; {@code appeal}, the appeal a failed row was heard by; and {@code decline}, the
     * engine-text arm's reason decided before its texts were compared. The host judge is the only judge in
     * compatibility, so no database row carries any of them.)
     */
    public record Row(String name, int verdict, Report report, @Nullable Message message,
            @Nullable Failure unplanned) implements Step {
        public Row {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(report, "report");
            if (verdict < 0) {
                throw new IllegalArgumentException(name + ": a row's verdict is an id of the plan's: " + verdict);
            }
            if (message != null && unplanned != null) {
                throw new IllegalArgumentException(name + ": a row's message that planned and one that did not");
            }
        }

        /** A typed row whose failure is its report's, or the root's message. */
        public Row(String name, int verdict, Report report) {
            this(name, verdict, report, null, null);
        }
    }

    /**
     * How a TYPED verdict row's failure is recorded (item 3's S4, 2026-11-21, the user's Q-A: the database decides over
     * typed values): the row is its verdict and the typed values its family reads -- {@code [verdict, expected values,
     * actual values]}, each side a list in its own order, the values written only where the verdict fails -- and the
     * run builds the failure's record from them with the host judge's own builders, so both judges word one failure
     * one way. Every row carries its report (S4 step 4: the seven-column row deleted).
     *
     * @param family the assert's family: which builder records its failure
     * @param want   the assert's polarity (equal, not equal)
     * @param e      the expected side's static facts (its class, its declared kind), or null
     * @param a      the actual side's
     * @param fixed  the failure's record where planning fixes it (a caller's literal message), else null
     * @param index  an unrolled quantifier's element the failure is framed at ({@code FrameKind.AT_INDEX}), else -1
     */
    public record Report(Family family, boolean want, HostJudgment.@Nullable SideFacts e,
            HostJudgment.@Nullable SideFacts a, com.legend.error.@Nullable ErrorRecord fixed, int index) {
        public Report {
            Objects.requireNonNull(family, "family");
            if (fixed != null && !(fixed.code() instanceof com.legend.error.AssertKind)) {
                throw new IllegalArgumentException("a typed row's fixed failure is an assert's record: " + fixed.code());
            }
            if (index < -1) {
                throw new IllegalArgumentException("an element index is -1 or more: " + index);
            }
        }

        /** This report framed at an unrolled quantifier's element {@code at}. */
        public Report atElement(int at) {
            return new Report(family, want, e, a, fixed, at);
        }

        /** This report with the caller's literal message as its failure. */
        public Report withFixed(com.legend.error.ErrorRecord failure) {
            return new Report(family, want, e, a, failure, index);
        }
    }

    /** A typed row's family: which of the host judge's builders records its failure. */
    public enum Family {
        /** {@code assertEquals} / {@code assertNotEquals} in order: {@code [verdict, expected, actual]}. */
        EQUALS,
        /** {@code assertSameElements} and an incidental equals: {@code [verdict, expected, actual]}, each in its order. */
        SAME_ELEMENTS,
        /** {@code assertIsNot} over values its equality decides: {@code [verdict, expected, actual]}. */
        IS_NOT,
        /** {@code assertEq} / {@code assertIs} over instances: {@code [verdict, expected, actual]}, each a one-value list,
         *  recorded as the host's identity assert names its values -- an instance its class and its identity (item 3's
         *  S4, instances, 2026-11-22). */
        EQ,
        /** {@code assert} / {@code assertFalse} (and a condition of the planner's making): {@code [verdict]}, its record
         *  fixed at planning, or the polarity's. */
        CONDITION,
        /** {@code assertSize} / {@code assertNotSize}: {@code [verdict, size, count]}. */
        SIZE,
        /** {@code assertEmpty} / {@code assertNotEmpty}: {@code [verdict, values]}. */
        EMPTY,
        /** {@code assertContains} / {@code assertNotContains}: {@code [verdict, collection, value]}. */
        CONTAINS,
        /** {@code assertEqWithinTolerance}: {@code [verdict, expected, actual, delta]}. */
        TOLERANCE,
        /** A quantified equality element by element: {@code [verdict, expected, actual, index]}, the first failing
         *  element's operands and its zero-based index. */
        PER_ELEMENT,
        /** {@code assertTdsEquivalent}: {@code [verdict, expected rows, actual rows, delta]}, each grid's rows a list of
         *  row structs in its order (item 3's S4, tables). */
        TDS_EQUIVALENT,
        /** {@code assertEquals} or {@code assertNotEquals} over two grids: {@code [verdict, expected rows, actual
         *  rows]}, each grid's rows a list of row structs in its order (item 3's S4, tables). */
        GRIDS,
        /** {@code assertInstanceOf} where the value's static type decided it: {@code [verdict, value, type]}, the value a
         *  list (item 3's S4). */
        INSTANCE_OF,
        /** {@link #EQUALS} over tagged values: each side a list of {@code {k, v0, ...}} structs, its value the column
         *  {@code k} names (item 3's S4). */
        EQUALS_TAGGED,
        /** {@link #SAME_ELEMENTS} over tagged values, as {@link #EQUALS_TAGGED}. */
        SAME_ELEMENTS_TAGGED,
        /** {@link #IS_NOT} over tagged values, as {@link #EQUALS_TAGGED}. */
        IS_NOT_TAGGED,
        /** {@link #INSTANCE_OF} over a tagged value (a Number the database decides among candidates): its value one
         *  {@code {k, v0, ...}} struct, as {@link #EQUALS_TAGGED}'s (item 3's S4, the last families). */
        INSTANCE_OF_TAGGED,
        /** {@code assertJsonStringsEqual}: {@code [verdict, golden text, document text]}, the document's text a list of
         *  its one value (item 3's S4, the last families). */
        JSON,
        /** {@link #JSON} where the document's root has no defined order: its root's elements compared as a multiset. */
        JSON_UNORDERED_ROOT,
        /** {@link #EMPTY} over a graph side: {@code [verdict, documents]}, each document's root objects the side's values,
         *  as the host reads a graph side (item 3's S4, the last families). */
        EMPTY_GRAPH,
        /** {@link #EQUALS} over documents (instances carried as JSON text, a graph's array): each side a list of
         *  documents, each document's root objects the side's values, as the host reads a graph side. */
        EQUALS_DOCUMENTS,
        /** {@link #IS_NOT} over documents, as {@link #EQUALS_DOCUMENTS}. */
        IS_NOT_DOCUMENTS,
        /** {@link #CONTAINS} over tagged values (a mixed list, an Any), each side a list of {@code {k, v0, ...}} structs,
         *  as {@link #EQUALS_TAGGED}'s. */
        CONTAINS_TAGGED,
        /** {@link #EMPTY} over tagged values, as {@link #EQUALS_TAGGED}'s. */
        EMPTY_TAGGED;

        /** The equality family: in any order or in order, over tagged values or typed ones. */
        public static Family equality(boolean multiset, boolean tagged) {
            return tagged ? multiset ? SAME_ELEMENTS_TAGGED : EQUALS_TAGGED : multiset ? SAME_ELEMENTS : EQUALS;
        }
    }

    /**
     * An outcome decided at planning: a static decision, an unjudged decline, a planned raise ({@code reported}: the
     * listener hears its verdict), or a WALL the arm left through ({@code reported} false: raised at its turn after the
     * roots before it, told to no listener).
     */
    public record Decided(Failure failure, boolean reported) implements Step {
        public Decided {
            Objects.requireNonNull(failure, "failure");
        }
    }
}
