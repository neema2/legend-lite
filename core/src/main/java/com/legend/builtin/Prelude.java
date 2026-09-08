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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * THE PRELUDE AS A MODULE (docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md §10):
 * the generated shapes ({@code prelude.pure}, written by
 * {@code PreludeGeneratorTest -Dprelude.generate=1}) read as ONE Pure
 * source and parsed once. They are not catalog entries: the compiler's
 * BOOT LAYER resolves and normalizes them beside the system metamodel and
 * joins them into every graph, so a derived property lifts like a user
 * class's and a body's names resolve through its section's imports. The
 * catalog ({@link Pure}) keeps the native signatures and the hand shapes
 * Java constructs directly.
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

    /** Parsed once at class load — fails loudly if the source rots. */
    private static final ParsedModel PARSED = com.legend.parser.ElementParser.parse(
            SOURCE, com.legend.parser.Dialect.LEGEND_PLATFORM);

    private static final Set<String> CLASS_FQNS = PARSED.elements().stream()
            .filter(e -> e instanceof ClassDefinition).map(PackageableElement::qualifiedName)
            .collect(Collectors.toUnmodifiableSet());
    private static final Set<String> ENUM_FQNS = PARSED.elements().stream()
            .filter(e -> e instanceof EnumDefinition).map(PackageableElement::qualifiedName)
            .collect(Collectors.toUnmodifiableSet());

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
        return PARSED.elements().stream().map(PackageableElement::qualifiedName)
                .collect(Collectors.toUnmodifiableSet());
    }

    public static Set<String> classFqns() {
        return CLASS_FQNS;
    }

    public static Set<String> enumFqns() {
        return ENUM_FQNS;
    }

    /** A generated class by FQN — for tests and the few Java sites that need the DEFINITION. */
    public static ClassDefinition cls(String fqn) {
        return PARSED.elements().stream()
                .filter(e -> e instanceof ClassDefinition && e.qualifiedName().equals(fqn))
                .map(e -> (ClassDefinition) e).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("not a generated class: " + fqn));
    }

    public static EnumDefinition enumOf(String fqn) {
        return PARSED.elements().stream()
                .filter(e -> e instanceof EnumDefinition && e.qualifiedName().equals(fqn))
                .map(e -> (EnumDefinition) e).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("not a generated enum: " + fqn));
    }
}
