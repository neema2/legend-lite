package com.legend.protocol.spec;

/**
 * Byte-array literal — {@code toBytes('...')} in service test-parameter
 * position. Engine wire (harvest testBindingServices, probe-verified):
 * {@code {"_type":"byteArray","value":"<base64 of the string>"}} with the
 * span covering the whole {@code toBytes(...)} call. The {@code value}
 * here is the BASE64 text exactly as the wire carries it.
 */
public record CByteArray(String value,
                         @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    /** Position is excluded from equality, as on every value specification ({@code ValueSpecEqualityTest}). */
    @Override
    public boolean equals(Object o) {
        return o instanceof CByteArray other && java.util.Objects.equals(value, other.value());
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hashCode(value);
    }
}
