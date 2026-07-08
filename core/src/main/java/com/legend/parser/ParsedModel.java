package com.legend.parser;

import com.legend.parser.element.PackageableElement;

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
                          String source, java.util.Map<String, Integer> elementOffsets) {

    public ParsedModel {
        elements = elements == null ? List.of() : List.copyOf(elements);
        if (imports == null) {
            imports = ImportScope.empty();
        }
        elementOffsets = elementOffsets == null ? java.util.Map.of()
                : java.util.Map.copyOf(elementOffsets);
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
