package com.gs.legend.compiled;

import java.util.List;

/**
 * File-level artifact holding every back-reference contributed from a
 * single contributor project to a single target class. One fragment per
 * (contributor, target-class) pair; multiple contributors produce multiple
 * same-named files in different output directories.
 *
 * <p>On disk: {@code <targetPackage>/<TargetSimpleName>.backrefs.legend}
 * under the contributor's output directory. The loader walks every
 * registered dependency directory and merges fragments at
 * lookup time — {@link CompiledClass} is never mutated.
 */
public record CompiledBackRefFragment(
        int legendVersion,
        String targetFqn,
        String contributorProject,
        List<CompiledBackReference> backReferences) {
}
