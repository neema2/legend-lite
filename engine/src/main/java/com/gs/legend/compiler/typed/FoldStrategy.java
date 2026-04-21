package com.gs.legend.compiler.typed;

/**
 * Fold reduction strategy. Sealed hierarchy replaces the current string-tagged
 * {@code foldStrategy} / {@code elementTransform} / {@code accParam} fields on
 * {@code TypeInfo}.
 */
public sealed interface FoldStrategy permits
        Concatenation, SameType, MapReduce, CollectionBuild {}
