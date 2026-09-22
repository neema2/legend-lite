package com.legend.protocol.spec;

/**
 * Boolean literal. Example Pure source: {@code true} or {@code false}.
 *
 * <p><b>Position is excluded from equality on purpose</b> — same contract as
 * {@link CInteger}/{@link CString}, guarded by {@code ValueSpecEqualityTest}.
 */
public record CBoolean(boolean value, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    /** Position-free convenience constructor. */
    public CBoolean(boolean value) {
        this(value, null);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CBoolean other && value == other.value();
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(value);
    }
}
