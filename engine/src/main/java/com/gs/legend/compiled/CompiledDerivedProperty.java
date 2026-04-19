package com.gs.legend.compiled;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Compiled-state derived (a.k.a. qualified) property: a name, its parameter
 * list, declared return type and multiplicity, and the compiled body.
 *
 * <p>Parameters are non-empty for <em>qualified</em> properties (e.g.
 * {@code synonymsByType(type: ProductSynonymType[1])}) and empty for
 * classic derived properties ({@code fullName}).
 */
public record CompiledDerivedProperty(
        String name,
        List<CompiledParameter> parameters,
        Type returnType,
        Multiplicity returnMultiplicity,
        CompiledExpression body) {
}
