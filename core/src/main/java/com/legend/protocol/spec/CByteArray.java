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
}
