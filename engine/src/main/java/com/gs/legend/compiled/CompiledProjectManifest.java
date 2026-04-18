package com.gs.legend.compiled;

import java.util.List;

/**
 * Per-project Tier 2 manifest — lists every cross-project extension this
 * project declares ({@link CompiledExtensionRef}s). Scaffolded with an
 * empty list in v1; populated once the Extension element lands.
 *
 * <p>On disk: {@code project_manifest.legend} at the project's output root.
 * Always emitted by {@code PureModelBuilder.compileAll()} so the manifest
 * file is a stable artifact shape across every build.
 */
public record CompiledProjectManifest(
        int legendVersion,
        String projectId,
        List<CompiledExtensionRef> extensions) {

    /** Empty manifest scaffold for a project with no Tier 2 extensions. */
    public static CompiledProjectManifest empty(String projectId) {
        return new CompiledProjectManifest(1, projectId, List.of());
    }
}
