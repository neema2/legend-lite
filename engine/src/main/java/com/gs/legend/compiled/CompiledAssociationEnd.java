package com.gs.legend.compiled;

import com.gs.legend.model.m3.Type;

/**
 * One end of a {@link CompiledAssociation}. An association has exactly two
 * ends; each end names the injected property and the target class it points
 * to, along with the multiplicity at this end.
 */
public record CompiledAssociationEnd(
        String name,
        Type targetType,
        Multiplicity multiplicity) {
}
