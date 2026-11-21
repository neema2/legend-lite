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

    /** A wrapper of {@code wrapped}, its texts before and after it stated (stage B's B1, 2026-11-20, errors as records). */
    public TypeInferenceException(String prefix, TypeInferenceException wrapped, String suffix) {
        super(Phase.TYPE, wrapped, prefix, suffix);
    }
}
