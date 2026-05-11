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
public record ParsedModel(List<PackageableElement> elements, ImportScope imports) {

    public ParsedModel {
        elements = elements == null ? List.of() : List.copyOf(elements);
        if (imports == null) {
            imports = ImportScope.empty();
        }
    }

    /** {@code true} if no elements and no imports were parsed. */
    public boolean isEmpty() {
        return elements.isEmpty() && imports.isEmpty();
    }
}
