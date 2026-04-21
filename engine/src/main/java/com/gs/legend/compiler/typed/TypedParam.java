package com.gs.legend.compiler.typed;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

/**
 * Typed parameter descriptor for a {@link TypedLambda}'s parameter list.
 * Holds name, resolved type, and multiplicity — every parameter has all three.
 */
public record TypedParam(String name, Type type, Multiplicity multiplicity) {}
