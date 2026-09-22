package com.legend.protocol.spec;

/**
 * Float literal. Example Pure source: {@code 3.14} or {@code 1.5e-3}.
 *
 * <p>{@code value} is the {@code double} mirroring the engine's wire
 * record ({@code DomainParseTreeWalker} builds float literals as
 * doubles — the byte-parity surface). {@code exact} is legend-pure's
 * EXECUTION semantics (B8 receipt, read from the reference source):
 * the interpreted runtime's Float primitive is BigDecimal-backed by
 * declaration ({@code FloatCoreInstance extends
 * PrimitiveCoreInstance&lt;BigDecimal&gt;};
 * {@code ModelRepository.newFloatCoreInstance(String)} parses the
 * SOURCE TEXT into a BigDecimal; the interpreted natives compute on
 * it — Abs's {@code BigDecimal.abs()} arm re-wraps as Float). So a
 * literal whose digits do not survive the double round-trip carries
 * them here, STILL LABELED FLOAT — set by the execution-surface
 * parser dialects only, always null on LEGEND_ENGINE. The old
 * parse-time species change to {@link CDecimal} (which made the TYPE
 * lie to keep the VALUE) is deleted.
 */
public record CFloat(double value,
        java.math.@com.legend.base.Nullable BigDecimal exact,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    /** Position-free convenience constructor. */
    public CFloat(double value) {
        this(value, null, null);
    }

    /** Exact-free convenience constructor (the wire-parity shape). */
    public CFloat(double value, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(value, null, pos);
    }

    /** Position is excluded from equality — see {@code ValueSpecEqualityTest}.
     * {@code exact} is INCLUDED: two literals with different exact digits are
     * different values even when they round to one double. */
    @Override
    public boolean equals(Object o) {
        return o instanceof CFloat other
                && Double.compare(value, other.value()) == 0
                && java.util.Objects.equals(exact, other.exact());
    }

    /** Hash on the double only — consistent (equal objects share it). */
    @Override
    public int hashCode() {
        return Double.hashCode(value);
    }
}
