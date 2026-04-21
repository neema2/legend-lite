package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Byte array literal. */
public record TypedCByteArray(byte[] value, ExpressionType info) implements TypedSpec {}
