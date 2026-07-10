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

    public MappingResolutionException(String message) {
        super(Phase.MAPPING, message);
    }

    public MappingResolutionException(String message, String elementFqn) {
        super(Phase.MAPPING, message, elementFqn);
    }
}
