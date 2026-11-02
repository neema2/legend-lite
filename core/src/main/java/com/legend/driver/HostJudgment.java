// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.base.Nullable;
import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;

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
 * keyed pair restricts to, the byte verdict's static gates. {@link HostJudge#judge} switches on the record and runs
 * that one Java function over the rows and the facts. The sink holds no compiler object: no typed tree, no let prefix,
 * no model, no closure -- but for the engine-text asserts' APPEAL ({@link EngineText}, the plan's M10b): harness Java
 * outside the plan by the user's ruling (R3, T13, R6), run at the step on a failed text alone. The walk runs the same
 * function in place ({@link HostSides#run}).
 */
sealed interface HostJudgment permits HostJudgment.AllRows, HostJudgment.Unrolled, HostJudgment.TdsEquivalent, HostJudgment.GridPair, HostJudgment.Size,
        HostJudgment.Contains, HostJudgment.Tolerance, HostJudgment.Condition, HostJudgment.Subset,
        HostJudgment.Empty, HostJudgment.Eq, HostJudgment.InstanceOf, HostJudgment.Is, HostJudgment.Decided,
        HostJudgment.SameElements, HostJudgment.Equals, HostJudgment.Json, HostJudgment.Rendered,
        HostJudgment.Raise, HostJudgment.EngineText {

    /** The static facts of one side's typed argument the equality arms read: its type, the keyed class it declares
     *  (null for a non-class side), its kind class, whether it is Any-stamped, Nil-typed, a map carrier; its type's
     *  name for the census. */
    record SideFacts(ExprType info, @Nullable String classFqn, @Nullable KindClass kind, boolean anyStamped,
            boolean nil, boolean mapCarrier, String typeName) {

        static SideFacts of(TypedSpec s) {
            Type t = s.info().type();
            return new SideFacts(s.info(), EqualityKeys.fqnOf(t), KindClass.of(t), AssertVerdicts.isAnyStamped(s),
                    PlatformTypes.isNil(t), PlatformTypes.isMapCarrier(t), AssertVerdicts.typeName(s));
        }

        Type type() {
            return info.type();
        }
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
    record Keys(@Nullable EqualityKeys e, @Nullable EqualityKeys a, boolean eSubA, boolean aSubE,
            Map<String, EqualityKeys> byClass) {
        public Keys {
            byClass = Collections.unmodifiableMap(new LinkedHashMap<>(byClass));
        }

        static Keys of(TypedSpec eSpec, TypedSpec aSpec, ModelContext ctx) {
            String ef = EqualityKeys.fqnOf(eSpec.info().type());
            String af = EqualityKeys.fqnOf(aSpec.info().type());
            boolean eSubA = false;
            boolean aSubE = false;
            if (ef != null && af != null && !ef.equals(af)) {
                eSubA = ctx.isSubtype(ef, af);
                aSubE = ctx.isSubtype(af, ef);
            }
            EqualityKeys e = ef != null && (ef.equals(af) || eSubA) ? EqualityKeys.resolve(ctx, eSpec.info().type())
                    : null;
            EqualityKeys a = aSubE ? EqualityKeys.resolve(ctx, aSpec.info().type()) : null;
            Map<String, EqualityKeys> byClass = new LinkedHashMap<>();
            Set<String> seen = new java.util.HashSet<>();
            ownClassesOfTheSlots(e, ctx, byClass, seen);
            ownClassesOfTheSlots(a, ctx, byClass, seen);
            return new Keys(e, a, eSubA, aSubE, byClass);
        }

        /**
         * The keyed classes a value may carry as its own in a key slot of {@code k} that declares no keyed class
         * (the slot's declared class is keyless or Any: {@link HostJudge#restrictNested} judges the value by its own
         * classifier's keys): the declared class and its subtree -- the model's whole universe for Any, whose subtree
         * the knowledge layer indexes by explicit generalization alone -- each class that DECLARES a key resolved once
         * ({@link EqualityKeys#declaresKeys}: read off the definitions, so no class the body never touched is compiled
         * here; a class declaring none has no keys); a keyed class found contributes its own slots the same way (a
         * nested slot of a declared keyed class is walked through its keys).
         */
        private static void ownClassesOfTheSlots(@Nullable EqualityKeys k, ModelContext ctx,
                Map<String, EqualityKeys> byClass, Set<String> seen) {
            if (k == null) {
                return;
            }
            for (EqualityKeys.Key key : k.keys()) {
                if (key.nested() != null) {
                    ownClassesOfTheSlots(key.nested(), ctx, byClass, seen);
                    continue;
                }
                String declared = key.declaredClass();
                if (declared == null || !seen.add(declared + "[*]")) {
                    continue;
                }
                Iterable<String> candidates = declared.equals(PlatformTypes.ANY) ? ctx.resolutionUniverse()
                        : withSubtree(declared, ctx);
                for (String fqn : candidates) {
                    if (!seen.add(fqn) || !EqualityKeys.declaresKeys(ctx, fqn)) {
                        continue;
                    }
                    EqualityKeys own = EqualityKeys.resolve(ctx, fqn);
                    if (own != null) {
                        byClass.put(fqn, own);
                        ownClassesOfTheSlots(own, ctx, byClass, seen);
                    }
                }
            }
        }

        private static Set<String> withSubtree(String declared, ModelContext ctx) {
            Set<String> out = new java.util.LinkedHashSet<>();
            out.add(declared);
            out.addAll(ctx.subtree(declared));
            return out;
        }

        /** The keys the pair restricts to, or null: both sides declare the SAME keyed class (a side declared a
         *  supertype counts as the other's class when every value on its wire is of it), as
         *  {@code AssertVerdicts.instanceKeys} decided it with the typed arguments and the model. */
        @Nullable EqualityKeys resolve(SideFacts ef, SideFacts af, List<Object> eVals, List<Object> aVals) {
            String ec = ef.classFqn();
            String ac = af.classFqn();
            EqualityKeys ek = e;
            EqualityKeys ak = a;
            if (ec != null && ac != null && !ec.equals(ac)) {
                if (eSubA && HostJudge.allWireType(aVals, ec)) {
                    ac = ec;
                    ak = ek;
                } else if (aSubE && HostJudge.allWireType(eVals, ac)) {
                    ec = ac;
                    ek = ak;
                }
            }
            return ec != null && ec.equals(ac) ? ek : null;
        }

        /** The keys of the class {@code fqn} (a polymorphic key slot's value's own), or null where it has none. */
        @Nullable EqualityKeys of(String fqn) {
            return byClass.get(fqn);
        }
    }

    /** The byte verdict's static gates (F13): whether the pair's declared classes share keys, and whether a keyless
     *  constructor stands under a lambda in the sides' scope (one site id for many instances: declined). */
    record ByteGate(boolean hasInstanceKeys, boolean keylessCtorUnderLambda) {
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

    record Contains() implements HostJudgment {
    }

    record Tolerance() implements HostJudgment {
    }

    /** A quantified assert's VECTOR judgment (the test-is-the-product plan's M10c item 5): the one side is the
     *  predicate vector the database computes over the source's elements; the assert holds when every row is
     *  {@code wantTrue}, else it fails with {@code message} (the element arm's own words). */
    record AllRows(String name, boolean wantTrue, String message) implements HostJudgment {
    }

    /** An UNROLLED quantified assert's judgment (M10c item 5): the elements' planned asserts in order, each judged over
     *  its own slice of the sides ({@code askCounts} asks each), each counted as the walk counts it (entered, its
     *  family); the statement's one verdict is the last element's, the first failure raising -- exactly the walk's
     *  unroll, which adjudicates each element and reports the statement once. */
    record Unrolled(List<AssertVerdicts.HostAssert> parts, List<Integer> askCounts) implements HostJudgment {
        public Unrolled {
            parts = List.copyOf(parts);
            askCounts = List.copyOf(askCounts);
        }
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
    record Eq(SideFacts e, SideFacts a, Keys keys, ByteGate gate) implements HostJudgment {
    }

    /** {@code assertInstanceOf(v, type)}: the type's name and the classes it accepts -- itself and its subtree,
     *  the model's subtype relation read at planning; a null name where the type argument is no reference (the
     *  judgment declines the side it read: the walk's generic path). */
    record InstanceOf(@Nullable String typeName, Set<String> acceptedTypes) implements HostJudgment {
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

    record SameElements(SideFacts e, SideFacts a, boolean gridPair, Keys keys, ByteGate gate)
            implements HostJudgment {
    }

    record Equals(String name, boolean wantEqual, boolean incidental, boolean gridPair, SideFacts e, SideFacts a,
            Keys keys, ByteGate gate, FlatFacts flat) implements HostJudgment {
    }

    /** {@code assertJsonStringsEqual(golden, actual)}: whether the actual root's order is incidental. */
    record Json(boolean incidentalRoot) implements HostJudgment {
    }

    /** The rendered-text pair under its grammar; {@code counted} whether the route is counted where the judgment
     *  holds (the dispatch's own rendered arm, not the equality's). */
    record Rendered(String name, boolean wantEqual, String form, @Nullable String eForm, @Nullable String aForm,
            boolean sortedOrder, boolean counted) implements HostJudgment {
    }

    /** A planned RAISE at the assert's turn (R14): the planning raised {@code failure} inside the arm or before it. */
    record Raise(RuntimeException failure) implements HostJudgment {
    }

    /** An ENGINE-TEXT assert (the plan's M10b): the comparison the arm planned ({@link SqlTextVerdicts.Collected}: the
     *  two texts, their equality, the decline decided before them, the APPEAL and its environment) and whether the
     *  equality arm's census route is counted at the step. The one judgment holding a closure and the harness's
     *  objects: the rows leg is harness Java outside the plan by the user's ruling (R3, T13, R6), run at the step on a
     *  failed text alone; the texts themselves are the step's two constant sides, checked against the planned ones. */
    record EngineText(SqlTextVerdicts.Collected collected, boolean routed) implements HostJudgment {
    }
}
