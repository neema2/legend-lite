package com.gs.legend.compiled;

/**
 * One end of a {@link CompiledAssociation}. An association has exactly two
 * ends; each end names the injected property and the target class it points
 * to, along with the multiplicity at this end.
 */
public record CompiledAssociationEnd(
        String name,
        TypeRef targetTypeRef,
        Multiplicity multiplicity) {
}
