// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testexecutionplan;

import com.legend.base.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE HOST JUDGMENT AS DATA (the test-is-the-product plan's M8a; the user's ruling of 2026-11-01, the plan's §6 "B3 as
 * settled", design 2): a host arm's verdict step carries its sides and ONE NAME for the comparison -- one record here
 * per comparison, a closed list -- with the few facts the comparison needs that the sides' rows do not carry, every
 * one computed when the assert is PLANNED from the typed arguments and the model: the order view of a side, whether a
 * {@code $r.values} read is the envelope's, the subtype closure an {@code assertInstanceOf} accepts, the key trees a
 * keyed pair restricts to, the byte verdict's static gates. {@code com.legend.testexec.HostJudgments.judge} switches on the record and runs
 * that one Java function over the rows and the facts. Part of a test body's plan (the plan-as-data design's step 4,
 * 2026-11-08: docs/PLAN_AS_DATA_2026_11_07.md 2.2): data alone -- no compiler object, no typed tree, no model, no
 * closure; the engine-text asserts' APPEAL is named by its number ({@link EngineText}), the plan's own record by verdict
 * ({@code TestPlan.extensionsByVerdict}, step 8b).
 */
public sealed interface HostJudgment permits HostJudgment.AllRows, HostJudgment.Rows, HostJudgment.Unrolled, HostJudgment.TdsEquivalent, HostJudgment.GridPair, HostJudgment.Size,
        HostJudgment.Contains, HostJudgment.Tolerance, HostJudgment.Condition, HostJudgment.Subset,
        HostJudgment.Empty, HostJudgment.Eq, HostJudgment.InstanceOf, HostJudgment.Is, HostJudgment.Decided,
        HostJudgment.SameElements, HostJudgment.Equals, HostJudgment.Json, HostJudgment.Rendered,
        HostJudgment.Raise, HostJudgment.EngineText, HostJudgment.AssertError, HostJudgment.Messaged {

    /** The static facts of one side's typed argument the equality arms read: the keyed class it declares (null for a
     *  non-class side), its kind class, whether it is Any-stamped, Nil-typed, a map carrier; its type's name for the
     *  census, and the declared type's name its values are judged by. */
    record SideFacts(@Nullable String classFqn, @Nullable KindClass kind, boolean anyStamped, boolean nil,
            boolean mapCarrier, String typeName, String declared) {
    }

    /**
     * The keyed-class facts a pair's equality restricts by (X5: the engine's own relation for keyed classes, non-key
     * fields outside it): each side's declared class's keys, resolved exactly where the walk resolved them once the
     * wire said which class the pair is (the expected side's where the classes agree or it is the narrower, the
     * actual side's where it is the narrower); the subtype relation between the two declared classes (a side declared
     * a supertype takes the other's keys when every value on its wire is of that class); and, where a key slot
     * declares no keyed class, the keys of every keyed class a value in that slot may carry as its own
     * ({@code __type}): the slot's declared class and its subtree -- every class in the model for a slot declared Any
     * -- and, transitively, the same for the slots of those classes' keys; read once at planning. Null keys where a
     * side declares no class.
     */
    record Keys(@Nullable KeyTree e, @Nullable KeyTree a, boolean eSubA, boolean aSubE,
            Map<String, KeyTree> byClass) {
        public Keys {
            byClass = Collections.unmodifiableMap(new LinkedHashMap<>(byClass));
        }
    }

    /** The flat-cells verdict's typed-shape facts: each side a bare {@code TDSRow.values} read, each a wrapped
     *  relation stamp. */
    record FlatFacts(boolean bareRowStampE, boolean bareRowStampA, boolean wrappedRelationStampE,
            boolean wrappedRelationStampA) {
    }

    /** {@code assertTdsEquivalent(one, two, delta[, timeDelta])}. */
    record TdsEquivalent(boolean hasTimeDelta) implements HostJudgment {
    }

    /** The grid pair ({@code assertEquals}/{@code assertNotEquals} over two relation-stamped sides, D3);
     *  {@code sortedOrder} whether the actual side's order view is SORTED (rows compared in order). */
    record GridPair(String name, boolean wantEqual, boolean sortedOrder) implements HostJudgment {
    }

    /** {@code assertSize(coll, n)}: whether the collection is the Result envelope's {@code values}. */
    record Size(boolean envelopeValuesRead) implements HostJudgment {
    }

    /** {@code collection} and {@code value}: the two sides' static facts, an instance's {@code instanceOf} half in the
     *  failure message (2026-11-05, the assert bar: a list of pairs printed {@code instanceOf ?}). */
    record Contains(SideFacts collection, SideFacts value) implements HostJudgment {
    }

    record Tolerance() implements HostJudgment {
    }

    /** A quantified assert's VECTOR judgment (the test-is-the-product plan's M10c item 5): the one side is the
     *  predicate vector the database computes over the source's elements; the assert holds when every row is
     *  {@code wantTrue}, else it fails with {@code message} (the element arm's own words). */
    record AllRows(String name, boolean wantTrue, String message) implements HostJudgment {
    }

    /** A quantified {@code assertEquals} / {@code assertEqWithinTolerance} over a source's ROWS, judged row by row as
     *  the element arm judges a pair ({@code judge.PureAsserts}: the same decision, the same message, the
     *  first failing row's): the sides are the zip's arms ({@code pairing}: their pairing by position, stopped at the
     *  shorter, zip's own rule) or one relation source ({@code pairing} null: its rows as records by column name); each
     *  argument a row's field, the row itself, or a literal, decided at planning (2026-11-04, the core program: until
     *  then the walk's alone, HostJudge.quantifiedRows). */
    record Rows(String name, boolean tolerance, List<RowArg> args, @Nullable String expectedClass,
            @Nullable String actualClass, @Nullable Pairing pairing) implements HostJudgment {
        public Rows {
            args = List.copyOf(args);
        }
    }

    /** A row assert's argument: the row, one of its fields, or a literal. */
    sealed interface RowArg permits RowArg.Whole, RowArg.Field, RowArg.Literal {
        record Whole() implements RowArg {
        }

        record Field(String name) implements RowArg {
        }

        /** A literal argument: the Java carrier the compiler typed it as ({@code String}, {@code Boolean},
         *  {@code Double}, {@code Long}, {@code Integer}, {@code BigInteger}) and its text, read back to the same value
         *  at the step (a double's {@code Double.toString} reads back exactly). */
        record Literal(String carrier, String text) implements RowArg {
        }
    }

    /** The zip's shape over its arms, left to right: an arm by its index among the sides, or a zip of two. */
    sealed interface Pairing permits Pairing.Arm, Pairing.Zip {
        record Arm(int index) implements Pairing {
        }

        record Zip(Pairing left, Pairing right) implements Pairing {
        }
    }

    /** An UNROLLED quantified assert's judgment (M10c item 5): the elements' planned asserts in order, each judged over
     *  its own slice of the sides (each part's {@code asks}), each counted as the walk counts it (entered, its
     *  family); the statement's one verdict is the last element's, the first failure raising -- exactly the walk's
     *  unroll, which adjudicates each element and reports the statement once. */
    record Unrolled(List<Part> parts) implements HostJudgment {
        public Unrolled {
            parts = List.copyOf(parts);
        }
    }

    /** One element's planned assert inside an unrolled quantifier, or the assert inside an {@code assertError}: its
     *  facts, its judgment, and how many of the step's sides it reads. */
    record Part(Assertion assertion, HostJudgment judgment, int asks) {
    }

    /** {@code assert(cond)} / {@code assertFalse(cond)} on the identity lane. */
    record Condition(String name, boolean wantTrue) implements HostJudgment {
    }

    /** The forAll-contains subset fold of a condition. */
    record Subset(String name, boolean wantTrue) implements HostJudgment {
    }

    record Empty(boolean wantEmpty) implements HostJudgment {
    }

    /** {@code assertEq}: the identity rule over primitives (a class pair is the loud wall, at judging). */
    record Eq(SideFacts e, SideFacts a, Keys keys) implements HostJudgment {
    }

    /** {@code assertInstanceOf(v, type)}: the type's name and the classes it accepts -- itself and its subtree,
     *  the model's subtype relation read at planning; a null name where the type argument is no reference (the
     *  judgment declines the side it read: the walk's generic path). */
    record InstanceOf(@Nullable String typeName, java.util.Set<String> acceptedTypes) implements HostJudgment {
        public InstanceOf {
            acceptedTypes = Collections.unmodifiableSet(new java.util.LinkedHashSet<>(acceptedTypes));
        }
    }

    /** {@code assertIs} over two tracked elements: the identity condition's one boolean side. */
    record Is(String name) implements HostJudgment {
    }

    /** A verdict decided at planning with no side (two type identities, the same let-bound instance). */
    record Decided(boolean held, @Nullable String message) implements HostJudgment {
    }

    record SameElements(SideFacts e, SideFacts a, boolean gridPair, Keys keys) implements HostJudgment {
    }

    record Equals(String name, boolean wantEqual, boolean incidental, boolean gridPair, SideFacts e, SideFacts a,
            Keys keys, FlatFacts flat) implements HostJudgment {
    }

    /** {@code assertJsonStringsEqual(golden, actual)}: whether the actual root's order is incidental. */
    record Json(boolean incidentalRoot) implements HostJudgment {
    }

    /** The rendered-text pair under its grammar; {@code counted} whether the route is counted where the judgment
     *  holds (the dispatch's own rendered arm, not the equality's). */
    record Rendered(String name, boolean wantEqual, String form, @Nullable String eForm, @Nullable String aForm,
            boolean sortedOrder, boolean counted) implements HostJudgment {
    }

    /** {@code j} with its wrapper taken off ({@link Messaged}: a caller's message, which does not change what the
     *  judgment IS): the judgment the planners ask about -- a planned raise, an engine-text arm, an unrolled
     *  quantifier. (2026-11-04: the wrappers hid those from the gate: 61 DuckDB corpus bodies. The run's mode is no
     *  wrapper since the explicit rules, the same day: the verdict step names them, TestPlan.JudgingRules, and the
     *  judge is handed them.) */
    static HostJudgment core(HostJudgment j) {
        HostJudgment c = j;
        while (c instanceof Messaged m) {
            c = m.inner();
        }
        return c;
    }

    /** A caller's failure message (the asserts-as-plan-nodes program, step 2, 2026-11-04;
     *  {@code compiler.spec.AssertMessages}): {@code inner} judges the first {@code innerSides} sides, the family's
     *  check over the base overload; the side after them is the message, a String the plan read -- a failure of
     *  {@code inner} raises its text, as upstream's {@code assert(condition, | message)} does. {@code unplanned}: the
     *  message is no side -- its planning failed (a message reading what the planner cannot plan), and this is the
     *  assert's whole failure text where {@code inner} fails, naming that failure ({@code AssertMessages.unevaluated},
     *  spelled at planning since 2026-11-09), as upstream evaluates the message only there (2026-11-04, the assert
     *  bar). */
    record Messaged(HostJudgment inner, int innerSides, @Nullable String unplanned) implements HostJudgment {
        /** The message a side the plan reads. */
        public Messaged(HostJudgment inner, int innerSides) {
            this(inner, innerSides, null);
        }
    }

    /** A planned RAISE at the assert's turn (R14): the planning raised {@code failure} inside the arm or before it --
     *  data (the plan-as-data design's 2.4), raised as its class with its message at the turn. */
    record Raise(Failure failure) implements HostJudgment {
    }

    /** {@code assertError(f, expected[, line, column])} (the walker-deletion program, 2026-11-03): an ordinary verdict
     *  whose answer is an error -- its function's whole body is the step's sub-plan (ruling N3): each let that does
     *  work and the last statement are the step's sides, read in the body's order, and the first error reading them
     *  raises is judged at the step by the arm's own rule ({@code com.legend.testexec.AssertErrorRuns.judge}), counted and reported as
     *  any assert is; {@code caught} the error {@code f} raised while it was PLANNED (a deferred guard's, as the walk's arm
     *  catches it where its lowering is the evaluation), else null. A side that raises nothing fails the assert: "No
     *  error was thrown". */
    record AssertError(String expected, @Nullable Long line, @Nullable Long column, @Nullable Failure caught,
            @Nullable Part inner) implements HostJudgment {
        /** f's body a value: the error a side raises is the answer. */
        public AssertError(String expected, @Nullable Long line, @Nullable Long column, @Nullable Failure caught) {
            this(expected, line, column, caught, null);
        }
    }

    /** An ENGINE-TEXT assert (the plan's M10b): the comparison the arm planned -- its name, the golden and our text where
     *  the plan holds them as constants (null: the text is a side the step reads), their equality, the decline decided
     *  before them, the statement origin our text is read under (an exec origin's name, or null) -- and its APPEAL,
     *  named by its number in the body ({@code appeal}): the plan's {@link Appeal} record by verdict, heard at the step
     *  and decided after the run (step 8b, 2.7); whether the equality arm's census route is counted at the step. */
    record EngineText(String name, @Nullable String golden, @Nullable String ours, boolean textEqual,
            @Nullable String decline, @Nullable String oursOrigin, int appeal, boolean routed) implements HostJudgment {
    }
}
