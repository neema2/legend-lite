package com.gs.legend.ast;

/** Byte array literal. */
public record CByteArray(byte[] value) implements ValueSpecification {
}
