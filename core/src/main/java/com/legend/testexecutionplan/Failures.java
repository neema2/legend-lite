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
        Failure.Kind kind = Failure.Kind.named(e.getClass().getName());
        if (kind == null) {
            throw new IllegalStateException("a planning failure of a class the test plan does not name ("
                    + e.getClass().getName() + ", planned at " + where + "): " + e.getMessage(), e);
        }
        String message = e.getMessage();
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
        return new Failure(kind, message, phase, element, unjudged, where);
    }

    /** The exception {@code f} names, its message as planned: raised at the verdict's turn. */
    public static RuntimeException raised(Failure f) {
        String m = f.message();
        return switch (f.kind()) {
            case NOT_IMPLEMENTED -> new com.legend.error.NotImplementedException(text(f));
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
            case NOT_IMPLEMENTED, WALLED_BODY, RESOLUTION, ISLAND_SYNTAX, TYPE_INFERENCE, SCHEMA_INVARIANT, DATA,
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
