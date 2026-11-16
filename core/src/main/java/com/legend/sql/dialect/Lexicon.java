package com.legend.sql.dialect;

import java.util.Set;

/**
 * Dialect LEXICON &mdash; pure DATA (remediation T3.2): what a backend quotes
 * and how, with no behavior attached. Each dialect class holds its own row
 * ({@link DuckDb#LEXICON}, {@link H2#LEXICON}, ...: the dialect program's
 * Phase 1a, 2026-10-13).
 *
 * @param quoteChar     the identifier quote character
 * @param reservedWords lower-cased words that force quoting of a
 *                      plain-looking identifier
 */
public record Lexicon(char quoteChar, Set<String> reservedWords) {

    public Lexicon {
        reservedWords = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(reservedWords));
    }
}
