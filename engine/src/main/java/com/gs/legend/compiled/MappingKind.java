package com.gs.legend.compiled;

/**
 * The kind of a {@link CompiledMappedClass} — distinguishes a relational
 * mapping (class backed by a relational store) from a model-to-model mapping
 * (class projected from another class).
 */
public enum MappingKind {
    /** Class is mapped to a relational source: sourceSpec is a relation expression. */
    RELATIONAL,
    /** Class is mapped from another class: sourceSpec projects properties of the source class. */
    M2M
}
