package com.gs.legend.compiled;

/**
 * Sealed root of the {@code Compiled*} record hierarchy — one permitted
 * subtype per {@code PackageableElement} kind.
 *
 * <p>Each subtype is a pure-data record: no cycles, no live refs, no
 * identity-keyed maps at the top level. That property makes compiled state
 * trivially serializable to {@code .legend} files and safe to share across
 * threads.
 *
 * <p>The exhaustive sealed switch lives on this interface — adding a new
 * element kind forces a compile error at every {@code switch (e)} site that
 * pattern-matches on {@code CompiledElement}, which is how
 * {@code TypeChecker.check(PackageableElement)} and artifact codecs stay
 * complete as the language grows.
 *
 * <p>Back-references (association-injected properties, qualified properties,
 * future subclass enumeration and Extensions) are <strong>not</strong>
 * embedded here — they live in separate {@link CompiledBackRefFragment}s and
 * merge at lookup time. See Decision 10 in
 * {@code docs/PHASE_B_COMPILED_ELEMENTS.md}.
 */
public sealed interface CompiledElement
        permits CompiledClass, CompiledAssociation, CompiledMapping,
                CompiledFunction, CompiledService, CompiledEnum,
                CompiledProfile, CompiledDatabase, CompiledConnection,
                CompiledRuntime {

    /** Fully-qualified name of the compiled element (e.g. {@code model::Person}). */
    String qualifiedName();

    /** Source location where the element was declared. {@link SourceLocation#UNKNOWN} if not available. */
    SourceLocation sourceLocation();
}
