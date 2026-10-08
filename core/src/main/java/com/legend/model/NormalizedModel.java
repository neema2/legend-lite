package com.legend.model;

import java.util.List;
import java.util.Map;

/**
 * Result of Phase E ({@link ModelNormalizer#normalize}) &mdash; the canonical
 * post-normalization model ({@code docs/CLEAN_SHEET_INVERSION.md} &sect;4).
 *
 * <p>{@code elements} holds the structural elements (parser records, untouched
 * by Phase E) <em>plus</em> every behavior function Phase E lifted from a body
 * site (mapping transform, derived property, constraint, service query) as an
 * ordinary top-level {@link FunctionDefinition} &mdash; distinguishable from a
 * user-written function only by its reserved {@code $}-sigil FQN and its
 * {@link FunctionDefinition#synthesizedFrom()} provenance tag. Phase F ingests
 * lifted functions through the same {@code case FunctionDefinition} arm as
 * user functions; there is no separate flatten step.
 *
 * <p>What Phase E LEARNED about a mapping (poisons, mixed unions, union key
 * threads, the nullable census) rides the compiled {@link MappingDefinition}
 * itself ({@link MappingDefinition#facts()}, T4.1 step 2) &mdash; this record
 * carries no side channels for it.
 *
 * <p>The type is the phase gate: {@code normalize} accepts a
 * {@link com.legend.model.ParsedModel} and returns a {@code NormalizedModel},
 * so a model cannot be re-normalized (the duplicate-synth footgun dies at the
 * signature) and Phase F entry points can demand normalization at the type
 * level.
 *
 * @param elements       structural elements + all functions (user-written and lifted)
 * @param imports        import scope carried through from the parsed model
 * @param legacySurfaces READ-ONLY archive of the pre-Door-1 mapping DSL (the
 *                       authored mapping plus its JSON identity sets) for
 *                       ANALYSIS consumers (static lineage #44) &mdash; the
 *                       compilation pipeline never reads it
 */
public record NormalizedModel(List<PackageableElement> elements, ImportScope imports,
        java.util.Map<String, LegacyMappingDefinition> legacySurfaces) {

    /** Without legacy surfaces (tests, mapping-free paths). */
    public NormalizedModel(List<PackageableElement> elements, ImportScope imports) {
        this(elements, imports, java.util.Map.of());
    }

    public NormalizedModel {
        elements = elements == null ? List.of() : List.copyOf(elements);
        if (imports == null) {
            imports = ImportScope.empty();
        }
        legacySurfaces = legacySurfaces == null
                ? java.util.Map.of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(legacySurfaces));
    }

}
