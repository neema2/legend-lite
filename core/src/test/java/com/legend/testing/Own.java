// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.model.ParsedModel;
import com.legend.parser.GrammarLevel;
import com.legend.door.ModelFromText;
import com.legend.parser.SpecParser;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * THE own-suite provenance funnel: own unit tests parse through here, so
 * the suite's dialect is named ONCE — not at hundreds of call sites.
 *
 * <p>The exceptions that name a level themselves, and why:
 * <ul>
 *   <li>{@code com.legend.parser.**} tests — they PIN per-level behavior
 *       (that is what they test);</li>
 *   <li>{@code rcorpus.Runner} — m2 corpus provenance
 *       (PLATFORM outer, ENGINE for compileLegendGrammar payloads);</li>
 *   <li>{@code harness.**} tests — corpus machinery;</li>
 *   <li>parser-equivalence — the parity harness ENUMERATES levels.</li>
 * </ul>
 */
public final class Own {

    /**
     * The own suite's level: THE PRODUCT SURFACE. Flipped from PLATFORM
     * 2026-08-12 at a cost of exactly TWO pins (UserCallInlinerTest's
     * .allVersionsInRange sweep, ModelIndexerTest's native-declaration
     * sources) — both now name LEGEND_PLATFORM explicitly at their own
     * sites, which is the design: a platform-behavior test declares
     * itself.
     */
    public static final GrammarLevel SUITE = GrammarLevel.LEGEND_LITE;

    private Own() {
    }

    public static ParsedModel model(String source) {
        return ModelFromText.parse(source, SUITE);
    }

    public static ValueSpecification spec(String source) {
        return SpecParser.parse(source, SUITE);
    }

    public static ValueSpecification spec(com.legend.lexer.TokenStream tokens) {
        return SpecParser.parse(tokens, SUITE);
    }

    public static List<ValueSpecification> block(String source) {
        return SpecParser.parseCodeBlock(source, SUITE);
    }

    public static List<ValueSpecification> block(
            com.legend.lexer.TokenStream tokens) {
        return SpecParser.parseCodeBlock(tokens, SUITE);
    }
}
