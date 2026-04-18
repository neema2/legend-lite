package com.gs.legend.compiled;

import com.gs.legend.model.m3.Type;

/**
 * Compiled-state function / service / derived-property parameter:
 * name, declared type, and multiplicity.
 */
public record CompiledParameter(
        String name,
        Type type,
        Multiplicity multiplicity) {
}
