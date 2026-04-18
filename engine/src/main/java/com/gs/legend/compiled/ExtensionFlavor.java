package com.gs.legend.compiled;

/**
 * Flavor of a cross-project extension declared in a {@link CompiledProjectManifest}.
 *
 * <p>{@code DERIVED_ONLY} extensions contribute only derived-property bodies
 * to the target class. {@code SHADOW_MAPPED} extensions additionally
 * contribute a shadow mapping of the target class.
 */
public enum ExtensionFlavor {
    DERIVED_ONLY,
    SHADOW_MAPPED
}
