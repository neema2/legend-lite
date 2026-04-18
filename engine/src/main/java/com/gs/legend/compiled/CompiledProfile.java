package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state representation of a Pure profile — a named collection of
 * stereotype names and tag names that other elements may reference via
 * {@link StereotypeRef} and tagged-value entries.
 */
public record CompiledProfile(
        String qualifiedName,
        List<String> stereotypes,
        List<String> tags,
        SourceLocation sourceLocation) implements CompiledElement {
}
