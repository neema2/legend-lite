// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import com.legend.model.ClassDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * THE PRELUDE AS A MODULE (docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md §10,
 * docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md): the generated shapes
 * ({@code prelude.pure}, written by {@code PreludeGenerator},
 * {@code bazel run //:update_generated} — the spec's declarations VERBATIM under their
 * files' imports) read as ONE Pure source and parsed once. They are not
 * catalog entries: the compiler's BOOT LAYER ({@code Compiler.bootLayer})
 * resolves and normalizes them beside the system metamodel and joins them
 * into every graph, so a derived property lifts like a user class's and a
 * body's names resolve through its section's imports. The catalog
 * ({@link Pure}) keeps the native signatures and the hand shapes Java
 * constructs directly (phase 2 migrates those).
 */
public final class Prelude {

    private Prelude() {
    }

    private static final String SOURCE = read();

    private static String read() {
        try (InputStream in = Prelude.class.getResourceAsStream("/com/legend/builtin/prelude.pure")) {
            if (in == null) {
                throw new IllegalStateException("prelude.pure missing from the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Parsed once at class load — fails loudly if the source rots (the
     * {@link Pure} native-catalog discipline; the same bootstrap-loader
     * regime as {@link SystemMetamodel}). */
    private static final ParsedModel PARSED = com.legend.parser.ElementParser.parse(
            SOURCE, com.legend.parser.Dialect.LEGEND_PLATFORM);

    // ORDERED, in module order (PRELUDE_MODULE_HOMEWORK §9.12): the
    // generator writes legend-pure's sections before legend-engine's, each
    // by spec path, declarations in source order; the resolver's bare-name
    // fallback reads these sets in that order (first claimant wins after
    // the catalog) — a rule, where the catalog's HashMap order was luck
    private static final Set<String> CLASS_FQNS = Collections.unmodifiableSet(
            PARSED.elements().stream()
                    .filter(e -> e instanceof ClassDefinition).map(PackageableElement::qualifiedName)
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<String>())));
    private static final Set<String> ENUM_FQNS = Collections.unmodifiableSet(
            PARSED.elements().stream()
                    .filter(e -> e instanceof EnumDefinition).map(PackageableElement::qualifiedName)
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<String>())));
    /** The platform library's FUNCTIONS (legend-pure's platform packages, bodied,
     * non-test — batch 169): graph copies yield to them (Compiler.withoutPreludeShadows),
     * the system metamodel's own row-reading twins win over them (Compiler.bootLayer). */
    private static final Set<String> FUNCTION_FQNS = Collections.unmodifiableSet(
            PARSED.elements().stream()
                    .filter(e -> e instanceof com.legend.model.FunctionDefinition)
                    .map(PackageableElement::qualifiedName)
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<String>())));
    private static final Set<String> ELEMENT_FQNS = Collections.unmodifiableSet(
            PARSED.elements().stream()
                    .map(PackageableElement::qualifiedName)
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<String>())));

    /** The module's source text — the boot layer's cache key rides on it. */
    public static String source() {
        return SOURCE;
    }

    /** The module as parsed: elements with their per-section imports. */
    public static ParsedModel parsedModel() {
        return PARSED;
    }

    public static List<PackageableElement> elements() {
        return PARSED.elements();
    }

    /** Every prelude element's FQN — the boot layer's contribution to a graph's name universe. */
    public static Set<String> elementFqns() {
        return ELEMENT_FQNS;
    }

    public static Set<String> classFqns() {
        return CLASS_FQNS;
    }

    public static Set<String> functionFqns() {
        return FUNCTION_FQNS;
    }

    public static Set<String> enumFqns() {
        return ENUM_FQNS;
    }

    /** A generated class by FQN, AS PARSED (unresolved) — for tests and the
     * few Java sites that need the declaration rather than the name. */
    public static ClassDefinition cls(String fqn) {
        return PARSED.elements().stream()
                .filter(e -> e instanceof ClassDefinition && e.qualifiedName().equals(fqn))
                .map(e -> (ClassDefinition) e).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("not a generated class: " + fqn));
    }

    /** A generated enum by FQN. */
    public static EnumDefinition enumOf(String fqn) {
        return PARSED.elements().stream()
                .filter(e -> e instanceof EnumDefinition && e.qualifiedName().equals(fqn))
                .map(e -> (EnumDefinition) e).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("not a generated enum: " + fqn));
    }
}
