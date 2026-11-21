// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;
import com.legend.error.LegendCompileException;

/**
 * AN ERROR KNOWN AT PLANNING, AS DATA (docs/PLAN_AS_DATA_2026_11_07.md 2.4, the design's step 2, 2026-11-08): where
 * planning an assert, a let or a side raised, the test plan carries the error as this record and the run raises it at
 * the verdict's turn -- an exception of the {@link Kind} named, with this message, exactly as the planned exception
 * was raised before (a failure's text names the simple class, the agreement judge's message names it, and the
 * catches decide counting by type). Never a Java exception object in the plan: the plan read back from its JSON raises
 * the same error. Lost, as the design rules: the planning-time stack trace and cause (no reader of a planned error
 * reads either: a database's position rides a cause the database raised at run, and planning sends no statement).
 *
 * @param kind           the exception's class, from the closed list
 * @param message        its message, as {@code getMessage()} answered (null where it had none, and where its record
 *                       says it: {@link #message()} is then the record's sentence -- stage B's BE, 2026-11-21: no
 *                       prose in the test plan for an error that has its record)
 * @param phase          a {@link Kind#MODEL} error's phase; null for every other kind (each has its one phase)
 * @param element        the model element a compile error names ({@link Kind#MODEL}, {@link Kind#MAPPING_RESOLUTION})
 * @param unjudgedReason an {@link Kind#ASSERT_FAILED} that is an UNJUDGED decline: why
 * @param record         the error as data ({@link com.legend.error.ErrorRecord}: its code, values, position and frames;
 *                       stage B's BE, 2026-11-21, the one error model): what every audience words it from; null where
 *                       its site states none yet
 */
public record Failure(Kind kind, @Nullable String message, LegendCompileException.@Nullable Phase phase,
        @Nullable String element, @Nullable String unjudgedReason, com.legend.error.@Nullable ErrorRecord record) {
    // (where: the planning site that recorded it, a Java method's name in the plan -- gone 2026-11-21, stage B's
    // BE-P: no Java in the test plan; a planning failure names its site in the exception that refuses it instead)

    /** The kinds of failure a record rides: the classes that are lite's errors with a record (stage B's BE,
     *  2026-11-21; one list for the record's check and {@link Failures#of}'s reading of it). */
    static final java.util.Set<Kind> RECORDED = java.util.Set.of(Kind.MODEL, Kind.TYPE_INFERENCE, Kind.RESOLUTION,
            Kind.MAPPING_RESOLUTION, Kind.SCHEMA_INVARIANT, Kind.ISLAND_SYNTAX, Kind.DATA, Kind.ASSERT_FAILED);

    /** A failure with no record, its message its own. */
    public Failure(Kind kind, @Nullable String message, LegendCompileException.@Nullable Phase phase,
            @Nullable String element, @Nullable String unjudgedReason) {
        this(kind, message, phase, element, unjudgedReason, null);
    }

    public Failure {
        if ((phase != null) != (kind == Kind.MODEL)) {
            throw new IllegalArgumentException("a failure's phase is a model error's alone: " + kind + " " + phase);
        }
        if (element != null && kind != Kind.MODEL && kind != Kind.MAPPING_RESOLUTION) {
            throw new IllegalArgumentException("a failure's element is a compile error's alone: " + kind);
        }
        if (unjudgedReason != null && kind != Kind.ASSERT_FAILED) {
            throw new IllegalArgumentException("an unjudged reason is a failed assert's alone: " + kind);
        }
        if (record != null) {
            if (!RECORDED.contains(kind)) {
                throw new IllegalArgumentException("a failure's record is a lite error's alone: " + kind);
            }
            // a data failure's record a run's code, a compile failure's a compile's: Failures.raised rebuilds the error
            // its kind names over the record (the BE audit 5's L6, 2026-11-21)
            // an assert's failure its record an assert's code (item 3, 2026-11-21)
            boolean codeIsKinds = switch (kind) {
                case DATA -> record.code() instanceof com.legend.error.ErrorKind;
                case ASSERT_FAILED -> record.code() instanceof com.legend.error.AssertKind;
                case MODEL, TYPE_INFERENCE, RESOLUTION, MAPPING_RESOLUTION, SCHEMA_INVARIANT, ISLAND_SYNTAX ->
                        record.code() instanceof com.legend.error.CompileKind;
                case NOT_IMPLEMENTED, RAW_SQL_REFUSAL, WALLED_BODY, DIALECT_CAPABILITY, NOT_A_CONSTANT -> false;
            };
            if (!codeIsKinds) {
                throw new IllegalArgumentException("a " + kind + " failure's record is not its kind's code: "
                        + record.code().qualified());
            }
            if (message != null) {
                throw new IllegalArgumentException("a failure with its record carries no message of its own (the"
                        + " record's sentence is its message): " + kind);
            }
            if (unjudgedReason != null) {
                throw new IllegalArgumentException("an unjudged decline is not a record (the database judge's refusal"
                        + " of an assert's shape): " + kind);
            }
        }
    }

    /** Its message: its record's sentence in lite's words where it has a record, else its own. */
    @Override
    public @Nullable String message() {
        return record != null ? record.sentence() : message;
    }

    /** The message it carries of its own (null where its record says it). */
    public @Nullable String ownMessage() {
        return message;
    }

    /** The exception classes a planning failure is raised as, each by its class's name: a closed list (a class not
     *  listed fails the planning that recorded it, by name -- never a stand-in class). */
    /** An exception that names its own kind (a planning library's class this records layer cannot name). */
    public interface Kinded {
        /** The kind this exception is recorded as. */
        Kind failureKind();
    }

    public enum Kind {
        NOT_IMPLEMENTED("com.legend.error.NotImplementedException"),
        /** a statement's own SQL text refused (a NotImplementedException of its own class; the C4 final fix round's
         *  nit, 2026-11-14) */
        RAW_SQL_REFUSAL("com.legend.error.RawSqlRefusal"),
        WALLED_BODY("com.legend.error.WalledBodyException"),
        MODEL("com.legend.error.ModelException"),
        RESOLUTION("com.legend.error.ResolutionException"),
        MAPPING_RESOLUTION("com.legend.error.MappingResolutionException"),
        ISLAND_SYNTAX("com.legend.error.IslandSyntaxException"),
        TYPE_INFERENCE("com.legend.error.TypeInferenceException"),
        /** a schema invariant a typed side broke (a TypeInferenceException of its own class; the slice 1 audit) */
        SCHEMA_INVARIANT("com.legend.error.SchemaInvariantException"),
        DATA("com.legend.error.DataError"),
        ASSERT_FAILED("com.legend.error.AssertFailed"),
        DIALECT_CAPABILITY("com.legend.sql.DialectCapability"),
        /** the engine-text arm's refusal of a text side that is not a constant: answered at planning (the body is
         *  refused by name), never raised at run. Named by the exception itself ({@link Kinded}), never by its class's
         *  name (the class moved twice; census C1, 2026-11-17: the edge's call failed on a stale name) */
        NOT_A_CONSTANT(null);
        // (Java's own exceptions -- IllegalStateException, IllegalArgumentException, UnsupportedOperationException,
        // ArithmeticException, NumberFormatException, NullPointerException, ClassCastException,
        // IndexOutOfBoundsException, NoSuchElementException, DateTimeException, UncheckedIOException -- gone 2026-11-21,
        // stage B's BE-P: a defect of lite's fails loud, never a planned failure (the user's "defects def fail loud"
        // and "D"), so no Java class rides the test plan)

        /** The exception class's binary name; null where the exception names its kind itself ({@link Kinded}). */
        private final @Nullable String className;

        Kind(@Nullable String className) {
            this.className = className;
        }

        /** The exception class's binary name, or null where the exception names its kind itself ({@link Kinded}). */
        public @Nullable String className() {
            return className;
        }

        /** The kind whose class is named {@code className}, or null where none is. */
        public static @Nullable Kind named(String className) {
            for (Kind k : values()) {
                if (className.equals(k.className)) {
                    return k;
                }
            }
            return null;
        }
    }
}
