package com.legend.model;

import com.legend.model.PackageableElement;

import java.util.List;

/**
 * Result of step B (parse model): the list of
 * {@link PackageableElement} declarations the parser saw, plus the
 * {@link ImportScope} accumulated from {@code import} statements.
 *
 * <p>The side maps ({@code elementOffsets}, {@code elementImports},
 * {@code elementSources}, {@code elementSpans}) are keyed by {@link #keyOf the element's key}:
 * a function's id, every other element's qualified name. They were keyed
 * by qualified name alone until 2026-10-09 (build rebuild Phase 3b, item
 * 5b), so a function's overloads shared one record and the last file read
 * set the import scope for all of them (three of the manifest census's
 * load walls: {@code Runtime} and {@code Mapping} not found in the
 * service's {@code from} and the router's {@code routeFunction}).
 *
 * <p>Returned by {@link ElementParser#parseLegendLite(String)} and the other
 * named-level entries.
 *
 * <p>Renamed from engine's {@code ParseResult}. {@code ParsedModel} is more
 * descriptive &mdash; "the model the parser produced" &mdash; and avoids the
 * generic {@code Result} suffix.
 *
 * @param elements parsed packageable elements, in source order
 * @param imports  accumulated import scope
 */
public record ParsedModel(List<PackageableElement> elements, ImportScope imports,
                          @com.legend.base.Nullable String source,
                          java.util.Map<String, Integer> elementOffsets,
                          java.util.Map<String, ImportScope> elementImports,
                          java.util.Map<String, String> elementSources,
                          List<UnclaimedSection> unclaimedSections,
                          java.util.Map<String, Position> elementSpans) {

    /** A {@code ###} section no registered grammar claims — explicit and
     *  reportable, never lexer silence (SectionGrammarRegistry step 1). */
    public record UnclaimedSection(String name, int startOffset, int endOffset) {
    }

    /** Where an element starts, as a model given as protocol records says it ({@code ModelFromProtocol}): its
     *  source information's start. A model parsed from text keeps offsets into its text instead. */
    public record Position(int line, int column) {
    }

    public ParsedModel {
        elements = elements == null ? List.of() : List.copyOf(elements);
        if (imports == null) {
            imports = ImportScope.empty();
        }
        elementOffsets = elementOffsets == null ? java.util.Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(elementOffsets));
        elementImports = elementImports == null ? java.util.Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(elementImports));
        elementSources = elementSources == null ? java.util.Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(elementSources));
        unclaimedSections = unclaimedSections == null ? List.of()
                : List.copyOf(unclaimedSections);
        elementSpans = elementSpans == null ? java.util.Map.of()
                : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(elementSpans));
        if (!elementSpans.isEmpty() && !elementOffsets.isEmpty()) {
            // one model, one kind of position: offsets into its text, or the spans its records carried
            throw new IllegalArgumentException("a model has offsets into a text or spans from its records, not both");
        }
    }

    /** A model parsed from text: its elements' offsets into that text, and no spans. */
    public ParsedModel(List<PackageableElement> elements, ImportScope imports,
                       @com.legend.base.Nullable String source,
                       java.util.Map<String, Integer> elementOffsets,
                       java.util.Map<String, ImportScope> elementImports,
                       java.util.Map<String, String> elementSources,
                       List<UnclaimedSection> unclaimedSections) {
        this(elements, imports, source, elementOffsets, elementImports, elementSources, unclaimedSections,
                java.util.Map.of());
    }

    /**
     * Where the element with key {@code key} ({@link #keyOf}) starts, as {@code "[line:col]"}: from its span for a
     * model given as records, from its offset into the source for a model parsed from one text; empty when the model
     * does not say (records without source information, a model with no source text). The one rule an error's
     * position is decorated by.
     */
    public java.util.Optional<String> position(String key) {
        Position span = elementSpans.get(key);
        if (span != null) {
            return java.util.Optional.of("[" + span.line() + ":" + span.column() + "]");
        }
        Integer offset = elementOffsets.get(key);
        return offset == null || source == null ? java.util.Optional.empty()
                : java.util.Optional.of(positionIn(source, offset));
    }

    /** {@code "[line:col]"} of char {@code offset} in {@code text}, both from 1 -- how an error names a place in the
     *  text it was read from (a multi-source module's per-file texts as well as one model's). */
    public static String positionIn(String text, int offset) {
        int line = 1;
        int col = 1;
        int end = Math.min(offset, text.length());
        for (int i = 0; i < end; i++) {
            if (text.charAt(i) == '\n') {
                line++;
                col = 1;
            } else {
                col++;
            }
        }
        return "[" + line + ":" + col + "]";
    }

    /**
     * Single-source form ({@code elementSources} empty): every element
     * came from {@code source}. The multi-source module compile
     * ({@code Compiler.parseSources}) fills {@code elementSources} (element
     * key &rarr; source unit name) so errors attribute to the right FILE.
     */
    /** Multi-source form without section data. */
    public ParsedModel(List<PackageableElement> elements, ImportScope imports,
                       @com.legend.base.Nullable String source,
                       java.util.Map<String, Integer> elementOffsets,
                       java.util.Map<String, ImportScope> elementImports,
                       java.util.Map<String, String> elementSources) {
        this(elements, imports, source, elementOffsets, elementImports,
                elementSources, List.of());
    }

    public ParsedModel(List<PackageableElement> elements, ImportScope imports,
                       @com.legend.base.Nullable String source,
                       java.util.Map<String, Integer> elementOffsets,
                       java.util.Map<String, ImportScope> elementImports) {
        this(elements, imports, source, elementOffsets, elementImports,
                java.util.Map.of(), List.of());
    }

    /**
     * Real pure imports are SECTION-scoped, not file-global: each element
     * resolves against the imports of ITS OWN section ({@code elementImports},
     * keyed by {@link #keyOf}). {@code imports()} stays the union — the query-side scope
     * and older callers — but element resolution prefers the per-element view
     * (concatenated multi-file models would otherwise cross-contaminate:
     * two files wildcard-importing different packages made every shared
     * simple name ambiguous).
     */
    public ParsedModel(List<PackageableElement> elements, ImportScope imports,
                       @com.legend.base.Nullable String source,
                       java.util.Map<String, Integer> elementOffsets) {
        this(elements, imports, source, elementOffsets, java.util.Map.of());
    }

    /**
     * Positions live in a SIDE INDEX keyed by {@link #keyOf element key} — not
     * on the element records (they are protocol-faithful shapes, and the
     * normalizer rebuilds them; a string key survives both). Empty for
     * synthesized models.
     *
     * @param source         original source text ({@code null} when unknown)
     * @param elementOffsets element key &rarr; char offset of its declaration
     */
    public ParsedModel(List<PackageableElement> elements, ImportScope imports) {
        this(elements, imports, null, java.util.Map.of());
    }

    /** {@code true} if no elements and no imports were parsed. */
    public boolean isEmpty() {
        return elements.isEmpty() && imports.isEmpty();
    }

    /**
     * The key the side maps record {@code el} under: a function's
     * {@link FunctionId id} (its overloads are distinct elements and each
     * has its own section, file and position), any other element's
     * qualified name. The id spells types by simple name, so the key is
     * the same before and after name resolution.
     */
    public static String keyOf(PackageableElement el) {
        return el instanceof Function f ? FunctionId.of(f).qualified() : el.qualifiedName();
    }
}
