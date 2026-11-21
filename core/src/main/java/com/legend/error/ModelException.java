package com.legend.error;

/**
 * A semantic error in the USER's model — bad mapping shapes, unknown
 * tables/filters/classes referenced by elements, malformed hierarchies.
 * Raised by the normalizer ({@link Phase#NORMALIZE}) and element
 * compilation ({@link Phase#MODEL}).
 */
public final class ModelException extends LegendCompileException {

    public ModelException(Phase phase, @com.legend.base.Nullable String message) {
        super(phase, message);
    }

    /** An error of {@code kind} over {@code operands} (D1.6): its message lite's sentence. */
    public ModelException(Phase phase, CompileKind kind, java.util.List<String> operands) {
        super(phase, kind, operands, null, null);
    }

    /** An error of {@code kind} over {@code operands} about the element {@code elementFqn} (position decoration). */
    public ModelException(Phase phase, CompileKind kind, java.util.List<String> operands,
            @com.legend.base.Nullable String elementFqn) {
        super(phase, kind, operands, elementFqn, null);
    }

    /** {@code e} about the element {@code elementFqn}, its record kept where it has one. */
    public static ModelException attributed(LegendCompileException e, String elementFqn) {
        ErrorRecord r = e.record();
        return r == null ? new ModelException(e.phase(), e.getMessage(), elementFqn)
                : new ModelException(e.phase(), r, elementFqn);
    }

    /** The error {@code record} states, about the element {@code elementFqn} (a planned failure rebuilt from its
     *  record, stage B's BE, 2026-11-21). */
    public ModelException(Phase phase, ErrorRecord record, @com.legend.base.Nullable String elementFqn) {
        super(phase, record, elementFqn);
    }

    /** {@code e} placed at {@code where} (the element's {@code [line:col]}): its record kept, placed, where it has one,
     *  so a reader that words the record (the edge) still can. */
    public static ModelException placed(LegendCompileException e, String where) {
        ErrorRecord r = e.record();
        return r == null ? new ModelException(e.phase(), e.getMessage(), e.element(), where)
                : new ModelException(e.phase(), r.placed(where), e.element());
    }

    /** With the element's position, placed before the message and kept as data ({@link #position()}). */
    private ModelException(Phase phase, @com.legend.base.Nullable String message,
            @com.legend.base.Nullable String elementFqn, String position) {
        super(phase, message, elementFqn, position);
    }

    /** With the FQN of the element the failure is about (position decoration). */
    public ModelException(Phase phase, @com.legend.base.Nullable String message,
            @com.legend.base.Nullable String elementFqn) {
        super(phase, message, elementFqn);
    }
}
