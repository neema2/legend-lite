// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;
import com.legend.error.AssertFailed;
import com.legend.error.LegendCompileException;

/**
 * A PLANNING FAILURE TO DATA, AND BACK AT ITS TURN (docs/PLAN_AS_DATA_2026_11_07.md 2.4, the design's step 2,
 * 2026-11-08): {@link #of} is the planning half -- the exception an assert's, a let's or a side's planning raised, as
 * the test plan's {@link Failure}; {@link #raised} the run's -- the exception the failure names, with its message,
 * raised at the verdict's turn as the planned exception was. Two halves of one rule, in the test plan's library: the
 * planning half is called by testplan, the run's by testexec (slice 5, 2026-11-09).
 */
public final class Failures {

    private Failures() {
    }

    /** {@code e}, raised while {@code where} planned, as data: its class one of the closed list (any other fails the
     *  planning here, by name -- never recorded as a stand-in class). */
    public static Failure of(RuntimeException e, String where) {
        // its own message, and its kinded part as data (stage B's B1, 2026-11-20, errors as records: until then a plan style's words of
        // the error, PlanStyle.errorWords, 2026-11-16)
        String message = e.getMessage();
        Failure.Kind kind = e instanceof Failure.Kinded k ? k.failureKind() : Failure.Kind.named(e.getClass().getName());
        if (kind == null) {
            throw new IllegalStateException("a planning failure of a class the test plan does not name ("
                    + e.getClass().getName() + ", planned at " + where + "): " + e.getMessage(), e);
        }
        if (message == null && needsMessage(kind)) {
            // these classes are built with a message (their constructors take one): a null is a defect of the site
            throw new IllegalStateException("a planning failure with no message (" + e.getClass().getName()
                    + ", planned at " + where + ")", e);
        }
        LegendCompileException.@Nullable Phase phase = kind == Failure.Kind.MODEL
                ? ((LegendCompileException) e).phase() : null;
        String element = e instanceof LegendCompileException c
                && (kind == Failure.Kind.MODEL || kind == Failure.Kind.MAPPING_RESOLUTION) ? c.element() : null;
        String unjudged = e instanceof AssertFailed f ? f.unjudgedReason() : null;
        return new Failure(kind, message, phase, element, unjudged, where, compileOf(e));
    }

    /**
     * {@code e}'s kinded compile error and lite's wrapper around it, as data (stage B's B1, 2026-11-20, errors as records): the first
     * error of {@code e}'s chain that has a {@link com.legend.error.CompileKind}, reached through wrappers that state
     * their texts ({@link LegendCompileException#wraps}) -- each wrapper's prefix before and suffix after. Null where the
     * chain holds no kinded error, or reaches it through any other error (nothing to state: no text is searched).
     */
    public static Failure.@Nullable Compile compileOf(Throwable e) {
        StringBuilder prefix = new StringBuilder();
        String suffix = "";
        for (Throwable t = e; t instanceof LegendCompileException c; t = t.getCause()) {
            if (c.kind() != null) {
                return new Failure.Compile(prefix.toString(), c.kind(), c.operands(), c.position(), suffix);
            }
            if (!c.wraps()) {
                return null;
            }
            prefix.append(c.prefix());
            suffix = c.suffix() + suffix;
        }
        return null;
    }

    /** The exception {@code f} names, its message as planned: raised at the verdict's turn, its kinded part
     *  ({@link Failure#compile}) riding it as its cause ({@link Planned}), so an audience's words of a caught error word
     *  it from the data (stage B's B1, 2026-11-20, errors as records). */
    public static RuntimeException raised(Failure f) {
        RuntimeException raised = rebuilt(f);
        Failure.Compile compile = f.compile();
        if (compile != null) {
            try {
                raised.initCause(new Planned(compile));
            } catch (IllegalStateException held) {
                throw new IllegalStateException("a planned failure of kind " + f.kind() + " is rebuilt with a cause, so"
                        + " its kinded part cannot ride it", held);
            }
        }
        return raised;
    }

    /** A planned failure's kinded part and lite's wrapper ({@link Failure.Compile}), riding the exception
     *  {@link #raised} rebuilds: its message lite's own sentence of the kinded part. */
    public static final class Planned extends RuntimeException {
        private final transient Failure.Compile compile;

        Planned(Failure.Compile compile) {
            super(compile.own());
            this.compile = compile;
        }

        /** The kinded part and the wrapper, as planned. */
        public Failure.Compile compile() {
            return compile;
        }
    }

    private static RuntimeException rebuilt(Failure f) {
        String m = f.message();
        return switch (f.kind()) {
            case NOT_IMPLEMENTED -> new com.legend.error.NotImplementedException(text(f));
            case RAW_SQL_REFUSAL -> new com.legend.error.RawSqlRefusal(text(f));
            case WALLED_BODY -> new com.legend.error.WalledBodyException(text(f));
            case MODEL -> new com.legend.error.ModelException(java.util.Objects.requireNonNull(f.phase()), m,
                    f.element());
            case RESOLUTION -> new com.legend.error.ResolutionException(text(f));
            case MAPPING_RESOLUTION -> new com.legend.error.MappingResolutionException(m, f.element());
            case ISLAND_SYNTAX -> new com.legend.error.IslandSyntaxException(text(f));
            case TYPE_INFERENCE -> new com.legend.error.TypeInferenceException(text(f));
            case SCHEMA_INVARIANT -> new com.legend.error.SchemaInvariantException(text(f));
            case DATA -> new com.legend.error.DataError(text(f));
            case ASSERT_FAILED -> {
                String unjudged = f.unjudgedReason();
                yield unjudged != null ? AssertFailed.unjudged(unjudged, text(f)) : new AssertFailed(text(f));
            }
            case DIALECT_CAPABILITY -> new com.legend.sql.DialectCapability(text(f));
            case NOT_A_CONSTANT -> new IllegalStateException("a text side that is not a constant of the plan is"
                    + " refused at planning, never raised at run (" + f.where() + "): " + m);
            case ILLEGAL_STATE -> new IllegalStateException(m);
            case ILLEGAL_ARGUMENT -> new IllegalArgumentException(m);
            case UNSUPPORTED_OPERATION -> new UnsupportedOperationException(m);
            case ARITHMETIC -> new ArithmeticException(m);
            case NUMBER_FORMAT -> new NumberFormatException(m);
            case NULL_POINTER -> new NullPointerException(m);
            case CLASS_CAST -> new ClassCastException(m);
            case INDEX_OUT_OF_BOUNDS -> new IndexOutOfBoundsException(m);
            case NO_SUCH_ELEMENT -> new java.util.NoSuchElementException(m);
            case DATE_TIME -> new java.time.DateTimeException(text(f));
            // the message as planned; the I/O cause it wrapped is lost with the planning's stack (2.4)
            case UNCHECKED_IO -> new java.io.UncheckedIOException(m, new java.io.IOException(m));
        };
    }

    /** The kinds whose constructor takes a message that is not null. */
    private static boolean needsMessage(Failure.Kind kind) {
        return switch (kind) {
            case NOT_IMPLEMENTED, RAW_SQL_REFUSAL, WALLED_BODY, RESOLUTION, ISLAND_SYNTAX, TYPE_INFERENCE, SCHEMA_INVARIANT, DATA,
                    ASSERT_FAILED, DIALECT_CAPABILITY, NOT_A_CONSTANT, DATE_TIME -> true;
            case MODEL, MAPPING_RESOLUTION, ILLEGAL_STATE, ILLEGAL_ARGUMENT, UNSUPPORTED_OPERATION, ARITHMETIC,
                    NUMBER_FORMAT, NULL_POINTER, CLASS_CAST, INDEX_OUT_OF_BOUNDS, NO_SUCH_ELEMENT, UNCHECKED_IO -> false;
        };
    }

    /** The message of a kind that {@link #needsMessage}: always present ({@link #of} records none without one). */
    private static String text(Failure f) {
        return java.util.Objects.requireNonNull(f.message(), "a lite error's planned failure carries its message");
    }
}
