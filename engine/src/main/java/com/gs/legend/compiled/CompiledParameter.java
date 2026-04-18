package com.gs.legend.compiled;

/**
 * Compiled-state function / service / derived-property parameter:
 * name, declared type, and multiplicity.
 */
public record CompiledParameter(
        String name,
        TypeRef typeRef,
        Multiplicity multiplicity) {
}
