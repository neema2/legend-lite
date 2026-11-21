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

    /** A TEST BODY'S PLANNING FAILURE, by where upstream raises it (the user's ruling of 2026-11-21, "D", the one
     *  error model's §4): an error legend-pure raises when the expression RUNS is planned and raised at its turn --
     *  a database's error found while planning ({@code DataError}), an assert lite decided at planning
     *  ({@code AssertFailed}; not the database judge's decline of an assert's shape, {@code unjudgedReason}, which is
     *  lite's own refusal: the BE audit's finding 3, 2026-11-21), and a code upstream raises at run
     *  ({@code CompileKind#atRun}: the window frames, a type the relation runtime does not hold, {@code timeBucket}'s
     *  StrictDate unit, a row's cell offset).
     *  Any other error -- a compile error upstream raises compiling, a refusal (lite cannot do it; legend-pure would
     *  run it), a defect -- refuses the body at planning, as a query is refused: it is thrown, whether or not the run
     *  would reach it. So a test plan holds run-time errors alone. A mapping error ({@code MappingResolutionException})
     *  is among the refused, under no code yet: the user's option 1 of 2026-11-21 (register S393; the ~14 that
     *  legend-engine's router raises at run become codes in a follow-up,
     *  docs/records/compat-mapping-errors-followup-2026-11-21.md). */
    public static Failure planned(RuntimeException e, String where) {
        if (!atItsTurn(e)) {
            throw e;
        }
        return of(e, where);
    }

    /** Whether {@code e} is an error upstream raises when the expression runs ({@link #planned}), decided per code,
     *  never by the exception's class (the BE audit's finding 3, 2026-11-21: a mapping error was kept by its class
     *  until then; the user's option 1 refuses it at planning, register S393). */
    public static boolean atItsTurn(RuntimeException e) {
        if (e instanceof AssertFailed f) {
            return f.unjudgedReason() == null;
        }
        if (e instanceof com.legend.error.DataError) {
            return true;
        }
        com.legend.error.@Nullable ErrorRecord r = e instanceof com.legend.error.LegendError le ? le.record() : null;
        return r != null && r.code() instanceof com.legend.error.CompileKind k && k.atRun();
    }

    /** {@code e}, raised while {@code where} planned, as data: its class one of the closed list (any other fails the
     *  planning here, by name -- never recorded as a stand-in class); its record where it is a lite error with one
     *  (stage B's BE, 2026-11-21, the one error model: the record, and no message, rides the plan), else its message. */
    public static Failure of(RuntimeException e, String where) {
        Failure.Kind kind = e instanceof Failure.Kinded k ? k.failureKind() : Failure.Kind.named(e.getClass().getName());
        if (kind == null) {
            throw new IllegalStateException("a planning failure of a class the test plan does not name ("
                    + e.getClass().getName() + ", planned at " + where + "): " + e.getMessage(), e);
        }
        com.legend.error.@Nullable ErrorRecord record = e instanceof com.legend.error.LegendError le
                && Failure.RECORDED.contains(kind) ? le.record() : null;
        if (record != null && !record.sentence().equals(e.getMessage())) {
            // a lite error's message IS its record's sentence: a difference is a defect of the site
            throw new IllegalStateException("a planning failure whose message is not its record's sentence ("
                    + e.getClass().getName() + ", planned at " + where + "): '" + e.getMessage() + "' against '"
                    + record.sentence() + "'", e);
        }
        String message = record != null ? null : e.getMessage();
        if (message == null && record == null && needsMessage(kind)) {
            // these classes are built with a message (their constructors take one): a null is a defect of the site
            throw new IllegalStateException("a planning failure with no message (" + e.getClass().getName()
                    + ", planned at " + where + ")", e);
        }
        LegendCompileException.@Nullable Phase phase = kind == Failure.Kind.MODEL
                ? ((LegendCompileException) e).phase() : null;
        String element = e instanceof LegendCompileException c
                && (kind == Failure.Kind.MODEL || kind == Failure.Kind.MAPPING_RESOLUTION) ? c.element() : null;
        String unjudged = e instanceof AssertFailed f ? f.unjudgedReason() : null;
        return new Failure(kind, message, phase, element, unjudged, record);
    }

    /** The exception {@code f} names, raised at the verdict's turn: rebuilt from its record where it has one, so its
     *  message is lite's sentence and every audience words it from the same record (stage B's BE, 2026-11-21), else
     *  with its message as planned. */
    public static RuntimeException raised(Failure f) {
        com.legend.error.ErrorRecord r = f.record();
        if (r == null) {
            return rebuilt(f);
        }
        return switch (f.kind()) {
            case MODEL -> new com.legend.error.ModelException(java.util.Objects.requireNonNull(f.phase()), r,
                    f.element());
            case RESOLUTION -> new com.legend.error.ResolutionException(r);
            case MAPPING_RESOLUTION -> new com.legend.error.MappingResolutionException(r, f.element());
            case ISLAND_SYNTAX -> new com.legend.error.IslandSyntaxException(r);
            case TYPE_INFERENCE -> new com.legend.error.TypeInferenceException(r);
            case SCHEMA_INVARIANT -> new com.legend.error.SchemaInvariantException(r);
            case DATA -> new com.legend.error.DataError(r);
            case ASSERT_FAILED -> new com.legend.error.AssertFailed(r);
            case NOT_IMPLEMENTED, RAW_SQL_REFUSAL, WALLED_BODY, DIALECT_CAPABILITY, NOT_A_CONSTANT ->
                    throw new IllegalStateException("a failure of kind " + f.kind() + " carries no record (Failure"
                            + " refuses it)");
        };
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
                    + " refused at planning, never raised at run: " + m);
        };
    }

    /** The kinds whose constructor takes a message that is not null. */
    private static boolean needsMessage(Failure.Kind kind) {
        return switch (kind) {
            case NOT_IMPLEMENTED, RAW_SQL_REFUSAL, WALLED_BODY, RESOLUTION, ISLAND_SYNTAX, TYPE_INFERENCE,
                    SCHEMA_INVARIANT, DATA, ASSERT_FAILED, DIALECT_CAPABILITY, NOT_A_CONSTANT -> true;
            case MODEL, MAPPING_RESOLUTION -> false;
        };
    }

    /** The message of a kind that {@link #needsMessage}: always present ({@link #of} records none without one). */
    private static String text(Failure f) {
        return java.util.Objects.requireNonNull(f.message(), "a lite error's planned failure carries its message");
    }
}
