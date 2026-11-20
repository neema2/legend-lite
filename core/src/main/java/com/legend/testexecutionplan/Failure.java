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
 * @param message        its message, as {@code getMessage()} answered (null where it had none)
 * @param phase          a {@link Kind#MODEL} error's phase; null for every other kind (each has its one phase)
 * @param element        the model element a compile error names ({@link Kind#MODEL}, {@link Kind#MAPPING_RESOLUTION})
 * @param unjudgedReason an {@link Kind#ASSERT_FAILED} that is an UNJUDGED decline: why
 * @param where          the planning site that recorded it, for debugging (ruled 2026-11-08): stable across runs
 * @param compile        the kinded compile error in it and lite's wrapper around that, as data ({@link Compile}); null
 *                       where it holds none
 */
public record Failure(Kind kind, @Nullable String message, LegendCompileException.@Nullable Phase phase,
        @Nullable String element, @Nullable String unjudgedReason, String where, @Nullable Compile compile) {

    /**
     * A COMPILE ERROR'S KINDED PART, AS DATA (stage B's B1, 2026-11-20, errors as records; until then the plan's style worded the error
     * at planning, {@code PlanStyle.errorWords}): the {@link com.legend.error.CompileKind}, its operands and its
     * {@code [line:col]}, and LITE'S WRAPPER around the kinded sentence as its two stated texts ({@code prefix}, the
     * body compiler's {@code "in function 'f': "}; {@code suffix}, the inliner's {@code " [inlined via a -> b]"}).
     * {@code message} stays lite's own, whole: {@code prefix + [position + " "] + kind's sentence + suffix}. Another
     * audience's formatter words the kinded part its way and keeps the wrapper, after the run.
     */
    public record Compile(String prefix, com.legend.error.CompileKind kind, java.util.List<String> operands,
            @Nullable String position, String suffix) {
        public Compile {
            java.util.Objects.requireNonNull(prefix, "prefix");
            java.util.Objects.requireNonNull(kind, "kind");
            operands = java.util.List.copyOf(operands);
            java.util.Objects.requireNonNull(suffix, "suffix");
        }

        /** Lite's own sentence of the kinded part, after its position. */
        public String own() {
            return (position == null ? "" : position + " ") + kind.sentence(operands);
        }
    }

    /** A failure with no kinded compile error in it. */
    public Failure(Kind kind, @Nullable String message, LegendCompileException.@Nullable Phase phase,
            @Nullable String element, @Nullable String unjudgedReason, String where) {
        this(kind, message, phase, element, unjudgedReason, where, null);
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
        if (compile != null && !(compile.prefix() + compile.own() + compile.suffix()).equals(message)) {
            throw new IllegalArgumentException("a failure's kinded part and lite's wrapper spell its message: '"
                    + compile.prefix() + compile.own() + compile.suffix() + "' is not '" + message + "'");
        }
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
        NOT_A_CONSTANT(null),
        ILLEGAL_STATE("java.lang.IllegalStateException"),
        ILLEGAL_ARGUMENT("java.lang.IllegalArgumentException"),
        UNSUPPORTED_OPERATION("java.lang.UnsupportedOperationException"),
        ARITHMETIC("java.lang.ArithmeticException"),
        NUMBER_FORMAT("java.lang.NumberFormatException"),
        NULL_POINTER("java.lang.NullPointerException"),
        CLASS_CAST("java.lang.ClassCastException"),
        INDEX_OUT_OF_BOUNDS("java.lang.IndexOutOfBoundsException"),
        // the slice 1 audit (2026-11-08): raisable by planning's folds and reads, raised as they were
        NO_SUCH_ELEMENT("java.util.NoSuchElementException"),
        DATE_TIME("java.time.DateTimeException"),
        UNCHECKED_IO("java.io.UncheckedIOException");

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
