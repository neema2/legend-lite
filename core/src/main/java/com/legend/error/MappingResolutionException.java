package com.legend.error;

/**
 * A Phase-H (store resolution) failure in the USER's input &mdash; a class
 * query that cannot be resolved against the active mapping: an unmapped
 * class or property, a missing execution context ({@code ->from(...)} /
 * driver runtime), an ambiguous runtime or include set. (Unsupported-but-
 * legal constructs are {@link NotImplementedException}, not this.) Named
 * per the taxonomy so "your mapping doesn't cover this"
 * is distinguishable by type from resolver bugs ({@code IllegalStateException})
 * and unbuilt features ({@link NotImplementedException}).
 */
public final class MappingResolutionException extends LegendCompileException {

    public MappingResolutionException(@com.legend.base.Nullable String message) {
        super(Phase.MAPPING, message);
    }

    public MappingResolutionException(@com.legend.base.Nullable String message,
            @com.legend.base.Nullable String elementFqn) {
        super(Phase.MAPPING, traced(message), elementFqn);
    }

    /** {@code -Dlegend.mapping.trace=<fragment>} prints the throw site of a
     * matching resolution wall (diagnostics only; the message is returned). */
    private static @com.legend.base.Nullable String traced(@com.legend.base.Nullable String message) {
        String want = System.getProperty("legend.mapping.trace");
        if (message != null && want != null && message.contains(want)) {
            new Exception("[mapping] " + message).printStackTrace(System.err);
        }
        return message;
    }
}
