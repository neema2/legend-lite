// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.Lexer;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE lexer/registry agreement guardrail: whether a section's content may
 * reach the shared token rules is decided in TWO places — the lexer's
 * {@code LEXABLE_SECTIONS} set and the registered grammar's
 * {@code lexable()} flag — and they must never drift. Drift is SILENT:
 * a registered-but-unlisted section raw-skips its content and every
 * element in it simply vanishes (Elasticsearch, 2026-08-10 — caught only
 * because the parity harness drains one-directional misses as MISSED).
 */
class LexerRegistryAgreementTest {

    @Test
    void lexableSetMatchesRegisteredLexableGrammars() {
        Set<String> fromRegistry = new TreeSet<>();
        for (var e : SectionGrammarRegistry.all().entrySet()) {
            if (e.getValue().lexable()) {
                fromRegistry.add(e.getKey());
            }
        }
        // overlay grammars registered via ServiceLoader on the TEST
        // classpath are not core built-ins — they re-lex through the SPI
        // and never ride the shared stream, so they are exempt
        fromRegistry.removeIf(name ->
                !SectionGrammarRegistry.lookup(name).orElseThrow()
                        .getClass().getPackageName()
                        .startsWith("com.legend.parser"));
        assertEquals(fromRegistry, new TreeSet<>(Lexer.lexableSections()),
                "Lexer.LEXABLE_SECTIONS and the registry's lexable built-in"
                        + " grammars have drifted — a claimed section whose"
                        + " name is missing from the lexer set raw-skips"
                        + " SILENTLY");
    }
}
