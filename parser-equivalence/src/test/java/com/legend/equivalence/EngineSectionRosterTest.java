// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.extension.PureGrammarParserExtension;
import org.finos.legend.engine.language.pure.grammar.from.extension.SectionParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE DENOMINATOR: every {@code ###Section} legend-engine can parse, asked of
 * engine's own extension registry rather than grepped for.
 *
 * <p>Section names live behind constants in each
 * {@code PureGrammarParserExtension}, so grepping the sources finds a handful
 * of string literals and misses the rest — which is exactly how a coverage
 * table ends up listing only the sections we happened to trip over. The
 * registry is the authority, and it is one call away.
 *
 * <p>Prints the roster for {@code docs/PROTOCOL_MIGRATION_CENSUS.md} and
 * fails if it SHRINKS, so an upstream pull that adds a section shows up as a
 * number rather than as silence.
 */
class EngineSectionRosterTest {

    /** Sections engine can parse at the pinned oracle release (release.MODULE.bazel's pins):
     *  21 extension + 4 core. A pull that ADDS one should widen the census,
     *  not pass in silence. */
    private static final int MIN_SECTIONS = 25;

    @Test
    void listEverySectionEngineCanParse() {
        // TWO registries, and missing the second is how a denominator ends
        // up wrong. EXTENSION sections implement `SectionParser` and are
        // ServiceLoader-discovered. The four CORE sections implement
        // `DEPRECATED_SectionGrammarParser` instead — DomainParser "Pure",
        // MappingParser "Mapping", ConnectionParser "Connection",
        // RuntimeParser "Runtime" — and are wired in by hand, so they appear
        // in NEITHER a ServiceLoader walk nor a grep for section literals.
        // PureGrammarParser dispatches every `###X` through one lookup
        // (PureGrammarParser:153), so the roster is the union.
        List<String> names = new ArrayList<>();
        for (PureGrammarParserExtension ext
                : java.util.ServiceLoader.load(PureGrammarParserExtension.class)) {
            for (SectionParser p : ext.getExtraSectionParsers()) {
                names.add(p.getSectionTypeName());
            }
        }
        for (Class<?> core : List.of(
                org.finos.legend.engine.language.pure.grammar.from.domain.DomainParser.class,
                org.finos.legend.engine.language.pure.grammar.from.mapping.MappingParser.class,
                org.finos.legend.engine.language.pure.grammar.from.connection.ConnectionParser.class,
                org.finos.legend.engine.language.pure.grammar.from.runtime.RuntimeParser.class)) {
            try {
                names.add((String) core.getField("name").get(null));
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("core section parser "
                        + core.getSimpleName() + " no longer exposes `name`"
                        + " — the roster would silently shrink", e);
            }
        }
        TreeSet<String> sorted = new TreeSet<>(names);

        System.out.println("[engine-sections] " + sorted.size()
                + " sections engine can parse:");
        sorted.forEach(n -> System.out.println("[engine-section] " + n));

        assertTrue(sorted.size() >= MIN_SECTIONS,
                "engine section roster shrank to " + sorted.size()
                        + " — did the oracle jars change? " + sorted);
    }
}
