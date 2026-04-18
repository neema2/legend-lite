package com.gs.legend.compiled;

import java.util.List;
import java.util.Map;

/**
 * One value of a {@link CompiledEnum}, with its own stereotypes and tagged
 * values. Mirrors Pure's {@code EnumValue} as a pure-data record.
 */
public record CompiledEnumValue(
        String name,
        List<StereotypeRef> stereotypes,
        Map<String, String> taggedValues) {
}
