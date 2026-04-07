package com.gs.legend.model;

import java.util.*;

/**
 * Centralized name registry for all packageable elements (classes, associations,
 * enums, mappings, databases, etc.).
 *
 * <p>Eliminates the dual-key pattern (registering under both FQN and simple name)
 * by maintaining a single canonical entry per element. All lookups are FQN-only.
 *
 * <p>Simple-name-to-FQN resolution is an explicit build-time operation via
 * {@link #resolveSimpleToFqn(String)}, called at PureModelBuilder boundaries
 * where parser output may contain unqualified names. After build time, all
 * names are canonical FQN — no fallback, no ambiguity.
 *
 * <p>Each registered name gets a stable integer ID for efficient downstream
 * comparison (future: replace string-keyed maps in compiler pipeline).
 */
public final class SymbolTable {

    // ID → qualified name
    private final List<String> idToFqn = new ArrayList<>();

    // Qualified name → ID (exact FQN match)
    private final Map<String, Integer> fqnToId = new HashMap<>();

    // Simple name → ID (first registration wins; ambiguity detected at resolve time)
    private final Map<String, Integer> simpleToId = new HashMap<>();

    // Simple name → list of FQNs (for ambiguity diagnostics)
    private final Map<String, List<String>> simpleToAllFqns = new HashMap<>();

    /**
     * Registers a qualified name and returns its integer ID.
     * If already registered, returns the existing ID.
     *
     * @param qualifiedName Fully qualified name (e.g., "test::Person")
     * @return The integer ID for this name
     */
    public int intern(String qualifiedName) {
        Integer existing = fqnToId.get(qualifiedName);
        if (existing != null) return existing;

        int id = idToFqn.size();
        idToFqn.add(qualifiedName);
        fqnToId.put(qualifiedName, id);

        String simple = extractSimpleName(qualifiedName);
        simpleToId.putIfAbsent(simple, id);
        simpleToAllFqns.computeIfAbsent(simple, k -> new ArrayList<>()).add(qualifiedName);

        return id;
    }

    /**
     * Resolves a FQN to its canonical FQN. FQN-only — no simple name fallback.
     * Returns null if not found.
     */
    public String resolve(String fqn) {
        return fqnToId.containsKey(fqn) ? fqn : null;
    }

    /**
     * Resolves a FQN to its integer ID. FQN-only — no simple name fallback.
     * Returns -1 if not found.
     */
    public int resolveId(String fqn) {
        Integer id = fqnToId.get(fqn);
        return id != null ? id : -1;
    }

    /**
     * Build-time only: resolves a simple name to its canonical FQN.
     * Called at PureModelBuilder boundaries where parser output may be unqualified.
     * Throws on ambiguity (two classes with same simple name).
     * Returns null if not found.
     */
    public String resolveSimpleToFqn(String simpleName) {
        // Already FQN?
        if (fqnToId.containsKey(simpleName)) return simpleName;
        // Simple name lookup
        List<String> allFqns = simpleToAllFqns.get(simpleName);
        if (allFqns == null || allFqns.isEmpty()) return null;
        if (allFqns.size() > 1) {
            throw new IllegalStateException(
                    "Ambiguous reference '" + simpleName + "' — matches: " + allFqns
                    + ". Use fully qualified name or add import.");
        }
        return allFqns.get(0);
    }

    /**
     * Returns the canonical FQN for an integer ID.
     */
    public String nameOf(int id) {
        return idToFqn.get(id);
    }

    /**
     * Returns the total number of registered names.
     */
    public int size() {
        return idToFqn.size();
    }

    /**
     * Returns true if the given FQN is registered.
     */
    public boolean contains(String fqn) {
        return fqnToId.containsKey(fqn);
    }

    /**
     * Returns the set of all registered FQNs.
     */
    public Set<String> allFqns() {
        return Collections.unmodifiableSet(fqnToId.keySet());
    }

    /**
     * Extracts the simple name from a qualified name.
     * "test::model::Person" → "Person". "Person" → "Person".
     */
    public static String extractSimpleName(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * Extracts the package path from a qualified name.
     * "test::model::Person" → "test::model". "Person" → "".
     */
    public static String extractPackagePath(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }
}
