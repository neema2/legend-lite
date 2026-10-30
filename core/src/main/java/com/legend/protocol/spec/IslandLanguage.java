// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.spec;

/**
 * THE REGISTRY of the languages a query-text island ({@link SqlIsland}) is written in (the user, 2026-10-30: "instead
 * of hard coding if statements everywhere", docs/TS_LANGUAGE_BUILD_2026_10_30.md P0): each language's opener, the key
 * of its text on the wire, whether it takes a profile, and whether the LEGEND_ENGINE surface refuses it. The parser,
 * the wire reader and the emitter read these facts and name no language; what an island means is the compiler's
 * island seam's ({@code compiler/spec/SqlIslands}), one switch over this enum, so a new language is one constant here
 * and one arm there.
 */
public enum IslandLanguage {

    /** {@code #SQL{ select ... }#}: Postgres SQL, upstream's sql-expression extension (ZTailProbe "sql-island"), its
     *  text the UNTRIMMED inner text (engine keeps trailing whitespace; corpus byte diff, 2026-08-09). */
    SQL("SQL", "sql", false, false),

    /** {@code #IQ{ select ... }#} and {@code #IQ:<profile>{ select ... }#}: legend-lite's SAP IQ island
     *  (docs/IQ_SQL_STUDY_2026_10_13.md §4, I1-I2), its profile one of a closed set ({@code IqProfile}). Upstream has
     *  none: the LEGEND_ENGINE surface refuses it as any unknown island type. On the LEGEND_PLATFORM surface it takes
     *  the place of legend-pure's legacy graph-fetch reading of the same spelling (a tree rooted at a class named
     *  {@code IQ}, which nothing here writes). */
    IQ("IQ", "sql", true, true),

    /** {@code #Cypher{ MATCH ... }#}: legend-lite's Cypher island (docs/CYPHER_STUDY_2026_10_15.md §4, C1), a query as
     *  Neo4j accepts it, its {@code CYPHER 5} or {@code CYPHER 25} prefix included; its wire value is
     *  {@code {"cypher": <text>}}. A lite extension, as the IQ island is. */
    CYPHER("Cypher", "cypher", false, true),

    /** {@code #TS{ SELECT avg(close, 20) AS ma20 FROM prices ON date BY symbol }#}: legend-lite's timeseries language
     *  (docs/TIMESERIES_LANGUAGE_2026_10_29.md; the user, 2026-10-30, docs/TS_LANGUAGE_BUILD_2026_10_30.md), a query
     *  of lines along time over a class, a table or a relation, each figure a call of the {@code timeseries} library;
     *  its wire value is {@code {"ts": <text>}}. A lite extension, as the IQ and Cypher islands are. */
    TS("TS", "ts", false, true);

    private final String opener;
    private final String textKey;
    private final boolean profiled;
    private final boolean liteExtension;

    IslandLanguage(String opener, String textKey, boolean profiled, boolean liteExtension) {
        this.opener = opener;
        this.textKey = textKey;
        this.profiled = profiled;
        this.liteExtension = liteExtension;
    }

    /** The name between {@code #} and the profile or the brace, as written ({@code IQ} of {@code #IQ:iq16-odbc{}); the
     *  wire's {@code classInstance} type. */
    public String opener() {
        return opener;
    }

    /** The key of the island's text in its wire value ({@code sql}, {@code cypher}). */
    public String textKey() {
        return textKey;
    }

    /** Whether the island may name a profile, {@code #IQ:<profile>{}. */
    public boolean profiled() {
        return profiled;
    }

    /** Whether the language is legend-lite's own, which the LEGEND_ENGINE surface refuses as an unknown island type. */
    public boolean liteExtension() {
        return liteExtension;
    }

    /** The language an island's discriminator ({@code SQL}, {@code IQ}, {@code IQ:iq16-odbc}) names, or null when it
     *  names none of these (a graph-fetch tree, a table reference, another island). */
    public static @com.legend.base.Nullable IslandLanguage named(String discriminator) {
        for (IslandLanguage l : values()) {
            if (discriminator.equals(l.opener) || l.profiled && discriminator.startsWith(l.opener + ":")) {
                return l;
            }
        }
        return null;
    }

    /** The profile {@code discriminator} names after its language's opener and a colon; null when it names none. */
    public @com.legend.base.Nullable String profileOf(String discriminator) {
        return profiled && discriminator.startsWith(opener + ":") ? discriminator.substring(opener.length() + 1) : null;
    }

    /** Why {@code profile} is not one of this language's profiles, or null when it is (the IQ island's closed set). */
    public @com.legend.base.Nullable String unknownProfile(String profile) {
        if (this != IQ) {
            return "the " + opener + " island takes no profile";
        }
        return com.legend.protocol.sql.iq.IqProfile.named(profile) == null
                ? com.legend.protocol.sql.iq.IqProfile.unknown(profile) : null;
    }
}
