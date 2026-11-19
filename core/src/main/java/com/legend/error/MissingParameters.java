package com.legend.error;

/**
 * A RUN'S REQUIRED PARAMETERS GIVEN NO VALUE (phase D's D1.6, W7 of docs/records/d-plan-2026-11-16.md, 2026-11-16):
 * each named with its type and multiplicity ({@code p:Integer[1]}), in lite's sentence; legend-engine's
 * ({@code Missing external parameter(s): ...}) is the compatibility edge's ({@code edge.PureMessages.compile}), which
 * words it where an upstream client reads the error. {@code rest} is any other problem found with the same values,
 * after it.
 */
public final class MissingParameters extends IllegalArgumentException {

    private final java.util.List<String> missing;
    private final @com.legend.base.Nullable String rest;

    public MissingParameters(java.util.List<String> missing, @com.legend.base.Nullable String rest) {
        super(sentence(missing, rest));
        this.missing = java.util.List.copyOf(missing);
        this.rest = rest;
    }

    /** Lite's sentence: the parameters with no value, then the rest. */
    private static String sentence(java.util.List<String> missing, @com.legend.base.Nullable String rest) {
        return "no value for the parameter(s) " + String.join(", ", missing) + (rest == null ? "" : "; " + rest);
    }

    /** Each parameter given no value, as {@code name:Type[multiplicity]}. */
    public java.util.List<String> missing() {
        return missing;
    }

    /** The other problems found with the same values, or null. */
    public @com.legend.base.Nullable String rest() {
        return rest;
    }
}
