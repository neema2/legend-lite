package com.legend.parser;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The shallow index of a Pure model source &mdash; a map from
 * fully-qualified element name to a {@link Entry} that records the
 * element's kind and its token range in the parent
 * {@link com.legend.lexer.TokenStream}.
 *
 * <p>The index is produced by {@link ModelIndexer#scan} in a single
 * linear pass over the token stream, without parsing element bodies. It
 * is the foundation of {@link ModelOrchestrator}'s demand-driven
 * resolve(): given an FQN the orchestrator looks up its range here and
 * runs the full element parser on just that slice.
 *
 * <p>The map preserves source order via {@link LinkedHashMap} so that
 * iterating the index reproduces the original file's element sequence
 * &mdash; helpful for {@code resolveAll()} reconstructing a
 * {@link ParsedModel} that round-trips against the eager path.
 */
public final class ModelIndex {

    /**
     * One entry in the {@link ModelIndex}: an element's kind plus the
     * inclusive-start / exclusive-end token indices of its full range
     * (header + body) in the parent token stream.
     *
     * <p>{@code [startTokenInclusive, endTokenExclusive)} is suitable to
     * pass directly to {@link com.legend.lexer.TokenStream#slice(int, int)}.
     */
    public record Entry(String fqn,
                        ElementKind kind,
                        int startTokenInclusive,
                        int endTokenExclusive) {
        public Entry {
            Objects.requireNonNull(fqn, "fqn");
            Objects.requireNonNull(kind, "kind");
            if (startTokenInclusive < 0 || endTokenExclusive < startTokenInclusive) {
                throw new IllegalArgumentException(
                        "invalid range [" + startTokenInclusive + ", " + endTokenExclusive + ") for " + fqn);
            }
        }

        /** Number of tokens in this element's range. */
        public int tokenCount() {
            return endTokenExclusive - startTokenInclusive;
        }
    }

    private final Map<String, Entry> byFqn;
    private final List<ImportEntry> imports;

    /**
     * Records a single top-level {@code import} statement encountered
     * during scanning. Imports are file-level (not associated with any
     * particular element) and are needed for name resolution; the
     * indexer records their token ranges so the orchestrator can parse
     * them eagerly (or together) without re-scanning.
     */
    public record ImportEntry(int startTokenInclusive, int endTokenExclusive) {
        public ImportEntry {
            if (startTokenInclusive < 0 || endTokenExclusive < startTokenInclusive) {
                throw new IllegalArgumentException(
                        "invalid import range [" + startTokenInclusive + ", " + endTokenExclusive + ")");
            }
        }
    }

    ModelIndex(Map<String, Entry> byFqn, List<ImportEntry> imports) {
        // Defensive copies; preserve insertion order for byFqn.
        this.byFqn = new LinkedHashMap<>(byFqn);
        this.imports = List.copyOf(imports);
    }

    /** Look up an entry by FQN, or {@code null} if the FQN is not declared. */
    public Entry get(String fqn) {
        return byFqn.get(fqn);
    }

    /** Whether the index declares {@code fqn}. */
    public boolean contains(String fqn) {
        return byFqn.containsKey(fqn);
    }

    /** All declared FQNs, in source order. */
    public Collection<String> fqns() {
        return byFqn.keySet();
    }

    /** All entries, in source order. */
    public Collection<Entry> entries() {
        return byFqn.values();
    }

    /** Number of declared elements (excludes imports). */
    public int size() {
        return byFqn.size();
    }

    /** All recorded import ranges, in source order. */
    public List<ImportEntry> imports() {
        return imports;
    }
}
