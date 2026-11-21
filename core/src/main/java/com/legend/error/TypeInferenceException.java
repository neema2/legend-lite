package com.legend.error;

/**
 * Phase-G type-inference failure &mdash; a body (or a native-call argument) does
 * not type-check: unification conflict, an unbound type/multiplicity variable
 * at resolution, or an unsatisfied subtype constraint.
 */
public class TypeInferenceException extends LegendCompileException {
    public TypeInferenceException(String message) {
        super(Phase.TYPE, message);
    }

    /** An error of {@code kind} over {@code operands} (D1.6): its message lite's sentence. */
    public TypeInferenceException(CompileKind kind, java.util.List<String> operands) {
        super(Phase.TYPE, kind, operands, null, null);
    }

    public TypeInferenceException(String message, Throwable cause) {
        super(Phase.TYPE, message, cause);
    }

    /** {@code wrapped} inside one more {@code frame} (stage B's BE, 2026-11-21, the one error model). */
    public TypeInferenceException(ErrorRecord.Frame frame, TypeInferenceException wrapped) {
        super(Phase.TYPE, wrapped, frame);
    }

    /** The error {@code record} states (a planned failure rebuilt from its record, stage B's BE). */
    public TypeInferenceException(ErrorRecord record) {
        super(Phase.TYPE, record, null);
    }
}
