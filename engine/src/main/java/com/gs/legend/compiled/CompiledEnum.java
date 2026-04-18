package com.gs.legend.compiled;

import java.util.List;
import java.util.Map;

/**
 * Compiled-state representation of a Pure enumeration — its values plus
 * enumeration-level stereotypes and tagged values. Per-value annotations
 * live on {@link CompiledEnumValue}.
 */
public record CompiledEnum(
        String qualifiedName,
        List<CompiledEnumValue> values,
        List<StereotypeRef> stereotypes,
        Map<String, String> taggedValues,
        SourceLocation sourceLocation) implements CompiledElement {
}
