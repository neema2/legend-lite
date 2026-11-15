// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.element.type.ExprType;
import com.legend.platform.Feature;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Classes PLANTED in the planner's package, each naming a guarded METHOD by reference ({@code Type::method}) and never
 * calling it, for one test alone (the next PR's item 1, "separation made foolproof", 2026-11-13; C4's finding:
 * ArchUnit's {@code callMethod}, {@code callMethodWhere} and {@code callCodeUnitWhere} read calls, not references, so
 * {@code UUID::randomUUID} as a Supplier passed every method rule): {@code ArchitectureTest
 * .theMethodRulesBiteAReference} shows each rule fails on its own. Test classes: the rules' real checks read the
 * product's classes, never these.
 */
public final class PlantedMethodReference {

    private PlantedMethodReference() {
    }

    /** The program road's test entry, named outside the test-planning library. */
    public static final class TestEntry {
        private TestEntry() {
        }

        /** {@code PlanMaker.testProgram}, as a function of its five parameters. */
        public interface Entry {
            PlanMaker.TestProgram plan(TypedQuery query, String runtime, Set<Feature> features, ProgramKinds kinds,
                    boolean systemRouted);
        }

        public static Entry entry() {
            return PlanMaker::testProgram;
        }
    }

    /** A class looked up by name: reflection. */
    public static final class Reflects {
        private Reflects() {
        }

        /** {@code Class.forName}, its checked exception declared. */
        public interface Lookup {
            Class<?> of(String name) throws ClassNotFoundException;
        }

        public static Lookup lookup() {
            return Class::forName;
        }
    }

    /** TeaVM's own blank. */
    public static final class Blank {
        private Blank() {
        }

        public static Predicate<String> blank() {
            return String::isBlank;
        }
    }

    /** Planning's values of its own, one per clause of the planning no-value rule. */
    public static final class PlanningRandomUuid {
        private PlanningRandomUuid() {
        }

        public static Supplier<java.util.UUID> minted() {
            return java.util.UUID::randomUUID;
        }
    }

    public static final class PlanningMathRandom {
        private PlanningMathRandom() {
        }

        public static DoubleSupplier drawn() {
            return Math::random;
        }
    }

    public static final class PlanningStrictMathRandom {
        private PlanningStrictMathRandom() {
        }

        public static DoubleSupplier drawn() {
            return StrictMath::random;
        }
    }

    public static final class PlanningNanoTime {
        private PlanningNanoTime() {
        }

        public static LongSupplier clock() {
            return System::nanoTime;
        }
    }

    public static final class PlanningCurrentTimeMillis {
        private PlanningCurrentTimeMillis() {
        }

        public static LongSupplier clock() {
            return System::currentTimeMillis;
        }
    }

    public static final class PlanningNow {
        private PlanningNow() {
        }

        public static Supplier<java.time.Instant> clock() {
            return java.time.Instant::now;
        }
    }

    /** A typed node minted outside the compiler's layers. */
    public static final class TypedNode {
        private TypedNode() {
        }

        public static BiFunction<String, ExprType, TypedCString> minted() {
            return TypedCString::new;
        }
    }

    /** A call built by a parser-side factory outside the builder. */
    public static final class CallFactory {
        private CallFactory() {
        }

        public static BiFunction<String, List<ValueSpecification>, AppliedFunction> built() {
            return AppliedFunction::infixRun;
        }
    }
}
