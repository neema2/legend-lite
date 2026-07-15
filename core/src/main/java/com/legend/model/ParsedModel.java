package com.legend.model;

import com.legend.model.PackageableElement;

import java.util.List;

/**
 * Result of step B (parse model): the list of
 * {@link PackageableElement} declarations the parser saw, plus the
 * {@link ImportScope} accumulated from {@code import} statements.
 *
 * <p>Returned by {@link ElementParser#parse(String)} /
 * {@link ElementParser#parse(com.legend.lexer.TokenStream)}.
 *
 * <p>Renamed from engine's {@code ParseResult}. {@code ParsedModel} is more
 * descriptive &mdash; "the model the parser produced" &mdash; and avoids the
 * generic {@code Result} suffix.
 *
 * @param elements parsed packageable elements, in source order
 * @param imports  accumulated import scope
 */
public record ParsedModel(List<PackageableElement> elements, ImportScope imports,
                          String source, java.util.Map<String, Integer> elementOffsets,
                          java.util.Map<String, ImportScope> elementImports) {

    public ParsedModel {
        elements = elements == null ? List.of() : List.copyOf(elements);
        if (imports == null) {
            imports = ImportScope.empty();
        }
        elementOffsets = elementOffsets == null ? java.util.Map.of()
                : java.util.Map.copyOf(elementOffsets);
        elementImports = elementImports == null ? java.util.Map.of()
                : java.util.Map.copyOf(elementImports);
    }

    /**
     * Real pure imports are SECTION-scoped, not file-global: each element
     * resolves against the imports of ITS OWN section ({@code elementImports},
     * keyed by FQN). {@code imports()} stays the union — the query-side scope
     * and older callers — but element resolution prefers the per-element view
     * (concatenated multi-file models would otherwise cross-contaminate:
     * two files wildcard-importing different packages made every shared
     * simple name ambiguous).
     */
    public ParsedModel(List<PackageableElement> elements, ImportScope imports,
                       String source, java.util.Map<String, Integer> elementOffsets) {
        this(elements, imports, source, elementOffsets, java.util.Map.of());
    }

    /**
     * Positions live in a SIDE INDEX keyed by element FQN — not on the
     * element records (they are protocol-faithful shapes, and the normalizer
     * rebuilds them; an FQN key survives both). Empty for synthesized models.
     *
     * @param source         original source text ({@code null} when unknown)
     * @param elementOffsets element FQN &rarr; char offset of its declaration
     */
    public ParsedModel(List<PackageableElement> elements, ImportScope imports) {
        this(elements, imports, null, java.util.Map.of());
    }

    /** {@code true} if no elements and no imports were parsed. */
    public boolean isEmpty() {
        return elements.isEmpty() && imports.isEmpty();
    }
}
