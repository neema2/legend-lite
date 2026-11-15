// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.PlannedSql;

import java.util.function.BiPredicate;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Classes PLANTED in exec's package, each naming a guarded METHOD by reference ({@code Type::method}) and never calling
 * it, for one test alone (the next PR's item 1, "separation made foolproof", 2026-11-13; C4's finding: the method rules
 * read calls, not references): {@code ArchitectureTest.theMethodRulesBiteAReference} shows each rule fails on its own.
 * Test classes: the rules' real checks read the product's classes, never these.
 */
public final class PlantedMethodReference {

    private PlantedMethodReference() {
    }

    /** A product's session over a borrowed connection: the test side's door. */
    public static final class Borrows {
        private Borrows() {
        }

        public static Function<java.sql.Connection, Sessions.Session> borrowing() {
            return Sessions::borrowed;
        }
    }

    /** A session's statements sent past the runner. */
    public static final class Sends {
        private Sends() {
        }

        public static BiPredicate<Statements, PlannedSql> sending() {
            return Statements::execute;
        }
    }

    /** A session's statements taken past the runner. */
    public static final class Takes {
        private Takes() {
        }

        public static Function<Sessions.Session, Statements> taking() {
            return Sessions.Session::statements;
        }
    }

    /** Exec's values of its own, one per clause of exec's no-value rule. */
    public static final class RandomUuid {
        private RandomUuid() {
        }

        public static Supplier<java.util.UUID> minted() {
            return java.util.UUID::randomUUID;
        }
    }

    public static final class ParsedUuid {
        private ParsedUuid() {
        }

        public static Function<String, java.util.UUID> parsed() {
            return java.util.UUID::fromString;
        }
    }

    public static final class NamedUuid {
        private NamedUuid() {
        }

        public static Function<byte[], java.util.UUID> named() {
            return java.util.UUID::nameUUIDFromBytes;
        }
    }

    public static final class MathRandom {
        private MathRandom() {
        }

        public static DoubleSupplier drawn() {
            return Math::random;
        }
    }

    public static final class StrictMathRandom {
        private StrictMathRandom() {
        }

        public static DoubleSupplier drawn() {
            return StrictMath::random;
        }
    }

    /** The SQL typing owner reached by the plan runner (its rule built over this class's name). */
    public static final class RunnerTyping {
        private RunnerTyping() {
        }

        public static Function<com.legend.sql.SqlType, com.legend.sql.TypeFact> typing() {
            return com.legend.sql.SqlTyping::typed;
        }
    }
}
