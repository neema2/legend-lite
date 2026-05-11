package com.legend.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable set of {@code import} statements collected from one Pure source.
 *
 * <p>Pure import syntax:
 * <ul>
 *   <li>{@code import package::name::*;} &mdash; wildcard import</li>
 *   <li>{@code import package::name::Type;} &mdash; specific import</li>
 * </ul>
 *
 * <p>Records:
 * <ul>
 *   <li>{@link #wildcards()} &mdash; package paths whose contents are in scope
 *       (e.g. {@code "simple::model"}).</li>
 *   <li>{@link #typeImports()} &mdash; simple-name &rarr; FQN entries
 *       (e.g. {@code "Firm" -> "simple::model::Firm"}).</li>
 * </ul>
 *
 * <p>This is <strong>pure data</strong>. Name resolution &mdash; turning a
 * simple name into a fully qualified one given an {@code ImportScope}
 * and the universe of known elements &mdash; lives in {@code NameResolver}
 * (Phase D). Putting it here would invite premature, ambiguity-prone
 * lookups before the model is fully populated.
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.ImportScope}, but
 * immutable: built once by the parser via {@link Builder}, never mutated.
 */
public record ImportScope(List<String> wildcards, Map<String, String> typeImports) {

    public ImportScope {
        wildcards = wildcards == null ? List.of() : List.copyOf(wildcards);
        typeImports = typeImports == null ? Map.of() : Map.copyOf(typeImports);
    }

    /** Empty scope &mdash; no imports. */
    public static ImportScope empty() {
        return new ImportScope(List.of(), Map.of());
    }

    /** {@code true} if this scope holds no imports. */
    public boolean isEmpty() {
        return wildcards.isEmpty() && typeImports.isEmpty();
    }

    /**
     * Mutable builder used by {@link ElementParser} while walking imports.
     * Constructs an immutable {@link ImportScope} via {@link #build()}.
     */
    public static final class Builder {
        private final List<String> wildcards = new ArrayList<>();
        private final Map<String, String> typeImports = new HashMap<>();

        /**
         * Add a full import path. Matches engine's {@code ImportScope.addImport}
         * semantics: paths ending in {@code ::*} become wildcards; others
         * become simple-name &rarr; FQN entries.
         */
        public Builder add(String importStatement) {
            String path = importStatement.trim();
            if (path.endsWith("::*")) {
                String pkg = path.substring(0, path.length() - 3);
                if (!wildcards.contains(pkg)) {
                    wildcards.add(pkg);
                }
            } else if (path.contains("::")) {
                int lastSep = path.lastIndexOf("::");
                String simple = path.substring(lastSep + 2);
                typeImports.put(simple, path);
            }
            return this;
        }

        public ImportScope build() {
            return new ImportScope(wildcards, typeImports);
        }
    }
}
