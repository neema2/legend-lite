package com.gs.legend.model.def;

import java.util.*;

/**
 * Tracks active imports for name resolution in Pure source files.
 * 
 * Pure import syntax:
 * import package::name::*; - wildcard import (all from package)
 * import package::name::Type; - specific type import
 * 
 * Example:
 * import simple::model::*;
 * 
 * Mapping simple::mapping::Map (
 * Firm: Relational { ... } // "Firm" resolves to "simple::model::Firm"
 * )
 */
public class ImportScope {

    private final List<String> wildcardImports; // e.g., ["simple::model", "simple::store"]
    private final Map<String, String> typeImports; // e.g., "Firm" -> "simple::model::Firm"

    public ImportScope() {
        this.wildcardImports = new ArrayList<>();
        this.typeImports = new HashMap<>();
    }

    /**
     * Adds an import statement to the scope.
     * 
     * @param importStatement The import path, e.g., "simple::model::*" or
     *                        "simple::model::Firm"
     */
    public void addImport(String importStatement) {
        String path = importStatement.trim();

        if (path.endsWith("::*")) {
            // Wildcard import: simple::model::* -> add "simple::model"
            String packagePath = path.substring(0, path.length() - 3);
            if (!wildcardImports.contains(packagePath)) {
                wildcardImports.add(packagePath);
            }
        } else if (path.contains("::")) {
            // Specific import: simple::model::Firm -> "Firm" -> "simple::model::Firm"
            int lastSep = path.lastIndexOf("::");
            String simpleName = path.substring(lastSep + 2);
            typeImports.put(simpleName, path);
        }
    }

    /**
     * Resolves a type name to its fully-qualified form.
     * Matches legend-engine's CompileContext.resolve() semantics:
     * 
     * 1. If already qualified (contains ::), return as-is
     * 2. Check specific imports
     * 3. Try ALL wildcard imports against known types:
     *    - 0 matches → return original (may be a primitive)
     *    - 1 match → return it
     *    - >1 matches → throw ambiguity error
     * 
     * @param name       The type name (may be simple or already qualified)
     * @param knownTypes Set of all known fully-qualified type names
     * @return The fully-qualified name, or the original if not resolvable
     */
    public String resolve(String name, Set<String> knownTypes) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        // Already qualified?
        if (name.contains("::")) {
            return name;
        }

        // Check specific imports first (e.g., "import pkg::Firm" makes "Firm" ->
        // "pkg::Firm")
        if (typeImports.containsKey(name)) {
            return typeImports.get(name);
        }

        // Try ALL wildcard imports — collect all matches for ambiguity detection
        List<String> matches = new ArrayList<>();
        for (String pkg : wildcardImports) {
            String candidate = pkg + "::" + name;
            if (knownTypes.contains(candidate)) {
                matches.add(candidate);
            }
        }

        if (matches.size() == 1) {
            return matches.get(0);
        }
        if (matches.size() > 1) {
            throw new IllegalStateException(
                    "Ambiguous reference '" + name + "' — multiple matches found via imports: " + matches
                    + ". Use a fully qualified name.");
        }

        // 0 matches — return as-is (may be a primitive like String, Integer, etc.)
        return name;
    }

    /**
     * Resolves a type name without checking against known types.
     * Uses first matching wildcard import.
     * 
     * @param name The type name
     * @return The resolved name using first wildcard match, or original if no match
     */
    public String resolveSimple(String name) {
        if (name == null || name.isEmpty() || name.contains("::")) {
            return name;
        }

        if (typeImports.containsKey(name)) {
            return typeImports.get(name);
        }

        // Use first wildcard import
        if (!wildcardImports.isEmpty()) {
            return wildcardImports.get(0) + "::" + name;
        }

        return name;
    }

    /**
     * @return List of wildcard import packages
     */
    public List<String> getWildcardImports() {
        return List.copyOf(wildcardImports);
    }

    /**
     * @return Map of specific type imports (simple name -> qualified name)
     */
    public Map<String, String> getTypeImports() {
        return Map.copyOf(typeImports);
    }

    /**
     * @return true if no imports have been added
     */
    public boolean isEmpty() {
        return wildcardImports.isEmpty() && typeImports.isEmpty();
    }

    @Override
    public String toString() {
        return "ImportScope{wildcards=" + wildcardImports + ", types=" + typeImports + "}";
    }
}
