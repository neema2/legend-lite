// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

/**
 * THE THREE GRAMMAR LEVELS (user directive 2026-08-12; docs/GRAMMAR_LEVELS.md): the language surface a parser accepts,
 * never a database ("dialect" alone means a database's SQL dialect: renamed from {@code Dialect} 2026-11-12, the user's
 * "there are two different Dialects").
 * A parser always knows which level it is serving — there is no neutral
 * default a caller can grab by accident.
 *
 * <ol>
 *   <li>{@link #LEGEND_PLATFORM} — legend-lite's OWN legend-pure grammar (m3/m2:
 *       {@code #TDS}, {@code ^$x(...)}, {@code native}, generics,
 *       function-type literals, m2 mapping forms). Reachable ONLY from
 *       built-in/bootstrap/platform-source loading and the test harness —
 *       the analogue of legend-engine depending on precompiled legend-pure
 *       jars. Caller-whitelisted ({@code PlatformSurfaceGuardrailTest}).</li>
 *   <li>{@link #LEGEND_ENGINE} — user-facing EXACT legend-engine. The drop-in
 *       surface: PmcdParser, the SPI bridge, byte parity. Refuses BOTH the
 *       platform grammar and the lite extensions, with the engine's own
 *       messages.</li>
 *   <li>{@link #LEGEND_LITE} — {@link #LEGEND_ENGINE} plus the DECLARED lite
 *       extensions (docs/OWN_CORPUS_DECISIONS.md LITE-DESIGN families:
 *       mapping-as-function, inline-association, sqlite-backend,
 *       function-types-generics). The product surface for lite's users. Its whole
 *       delta vs LEGEND must classify to a declared family — nothing
 *       undeclared rides along.</li>
 * </ol>
 */
public enum GrammarLevel {
    LEGEND_PLATFORM,
    LEGEND_LITE,
    LEGEND_ENGINE;

    /** The platform dialect (m3/m2 constructs) refuses here. */
    public boolean refusesPlatformDialect() {
        return this != LEGEND_PLATFORM;
    }

    /** The declared LITE-DESIGN extensions refuse here. */
    public boolean refusesLiteExtensions() {
        return this == LEGEND_ENGINE;
    }
}
