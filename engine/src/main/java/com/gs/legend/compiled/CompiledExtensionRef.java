package com.gs.legend.compiled;

/**
 * One entry in a {@link CompiledProjectManifest}: describes a Tier 2
 * extension that this project contributes to {@code targetFqn}.
 *
 * @param extensionFqn FQN of the Extension element in the contributor project.
 * @param targetFqn    FQN of the target class the extension contributes to.
 * @param exported     {@code true} if this extension is visible to downstream
 *                     callers that depend on this project.
 * @param flavor       {@link ExtensionFlavor#DERIVED_ONLY} or
 *                     {@link ExtensionFlavor#SHADOW_MAPPED}.
 */
public record CompiledExtensionRef(
        String extensionFqn,
        String targetFqn,
        boolean exported,
        ExtensionFlavor flavor) {
}
