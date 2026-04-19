package com.gs.legend.compiled;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

/**
 * Compiled-state property: a name plus its declared type and multiplicity.
 *
 * <p>Covers <em>own</em> properties of a {@link CompiledClass}. Properties
 * injected via associations are not stored here — they live in
 * {@link CompiledBackRefFragment}s and are merged at lookup time.
 */
public record CompiledProperty(
        String name,
        Type type,
        Multiplicity multiplicity) {
}
