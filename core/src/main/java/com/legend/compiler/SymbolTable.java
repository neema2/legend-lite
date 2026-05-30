package com.legend.compiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Interning table mapping fully-qualified names (FQNs) to dense integer ids.
 *
 * <p>The primary storage backing {@link ModelBuilder}. Each interned FQN
 * gets a monotonic id starting at {@code 0}; ids are stable for the
 * lifetime of the table. Reverse lookup ({@code nameOf}) is supported.
 *
 * <p>Mirrors engine's {@code com.gs.legend.compiled.SymbolTable} as a
 * tiny, focused interning primitive. Deliberately minimal &mdash; this
 * class owns FQN&hArr;id mapping and nothing else; kind-specific
 * storage (per-class, per-database, &hellip;) lives on
 * {@link ModelBuilder}.
 *
 * <p>Thread safety: not thread-safe. {@link ModelBuilder} performs all
 * interning during a single-threaded ingest pass; after the builder is
 * published the table is read-only and lookups become safe to share.
 *
 * <p>Why integer ids and not pure {@code Map<String, T>}: matches
 * engine's representation so kind-specific storage uses {@code
 * ArrayList<T>} indexed by id (cache-friendly, compact, equality is
 * a single int compare). When a real compiled-graph layer lands above
 * the AST, it can opt into id-based APIs through {@link #resolveId};
 * AST-level callers stay on FQN-string lookups via {@link ModelBuilder}.
 */
public final class SymbolTable {

    /** Sentinel returned by {@link #resolveId(String)} for unknown FQNs. */
    public static final int UNRESOLVED = -1;

    /** FQN &rarr; id. Populated by {@link #intern(String)}. */
    private final Map<String, Integer> idByFqn = new HashMap<>();

    /** id &rarr; FQN. Reverse index; same size and order as {@link #idByFqn} values. */
    private final List<String> fqnById = new ArrayList<>();

    /**
     * Returns the existing id for {@code fqn} or allocates a fresh one.
     * Subsequent calls with the same FQN return the same id.
     *
     * @param fqn fully-qualified name (non-null)
     * @return interned id, &ge; 0
     */
    public int intern(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        Integer existing = idByFqn.get(fqn);
        if (existing != null) {
            return existing;
        }
        int id = fqnById.size();
        idByFqn.put(fqn, id);
        fqnById.add(fqn);
        return id;
    }

    /**
     * Returns the id for {@code fqn} if previously interned, or
     * {@link #UNRESOLVED} otherwise. Does NOT allocate.
     *
     * @param fqn fully-qualified name (non-null)
     * @return id &ge; 0, or {@link #UNRESOLVED}
     */
    public int resolveId(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        Integer id = idByFqn.get(fqn);
        return id == null ? UNRESOLVED : id;
    }

    /**
     * Returns the FQN for {@code id}. Throws if {@code id} is out of
     * bounds; never returns {@code null}.
     *
     * @param id interned id (must be in {@code [0, size())})
     * @return the FQN at that id
     * @throws IndexOutOfBoundsException if {@code id < 0} or {@code id >= size()}
     */
    public String nameOf(int id) {
        if (id < 0 || id >= fqnById.size()) {
            throw new IndexOutOfBoundsException(
                    "SymbolTable id " + id + " out of bounds [0, " + fqnById.size() + ")");
        }
        return fqnById.get(id);
    }

    /** Number of distinct FQNs interned so far. */
    public int size() {
        return fqnById.size();
    }

    /** Read-only view of all interned FQNs. */
    public Set<String> allFqns() {
        return Collections.unmodifiableSet(idByFqn.keySet());
    }
}
