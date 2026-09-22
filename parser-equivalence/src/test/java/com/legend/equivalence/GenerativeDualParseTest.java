// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.testing.Repo;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.RuleStartState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A4 — THE GENERATIVE DUAL-PARSE GATE (adversarial-audit charter):
 * grammar-driven sentences from the ENGINE'S OWN ATN (random walk,
 * FIXED SEED — deterministic), dual-parsed by oracle and lite, verdict
 * parity pinned. Attacks corpus bias: the corpus exercises the corpus's
 * habits; the generator exercises the GRAMMAR's paths, including ones
 * no checked-in source spells. TRIGGERED cadence (like the census).
 *
 * <p>Terminals render as their grammar literals; abstract tokens
 * (identifiers, strings, numbers) render from an exemplar table. A
 * generated sentence is frequently oracle-INVALID beyond the walked
 * grammar's view (cross-rule predicates, walker checks) — those land
 * in both-reject and cost nothing. The signal is DISAGREEMENT.
 */
class GenerativeDualParseTest {

    private static final long SEED = 42;
    private static final int SENTENCES = 1500;
    private static final int MAX_STEPS = 400;
    /** First pin 2026-08-15 — divergences adjudicate down, never up. */
    private static final int MAX_DIVERGENCES = 0;   // 2026-08-15 first run: 1,043 sentences, ZERO divergences — any future one adjudicates before landing

    @Test
    void dualParse() throws Exception {
        Class<?> pc = Class.forName("org.finos.legend.engine.language.pure"
                + ".grammar.from.antlr4.domain.DomainParserGrammar");
        Class<?> lc = Class.forName("org.finos.legend.engine.language.pure"
                + ".grammar.from.antlr4.domain.DomainLexerGrammar");
        Parser probe = (Parser) pc.getConstructor(
                org.antlr.v4.runtime.TokenStream.class)
                .newInstance(new CommonTokenStream((Lexer) lc
                        .getConstructor(org.antlr.v4.runtime.CharStream.class)
                        .newInstance(CharStreams.fromString(""))));
        ATN atn = probe.getATN();
        Vocabulary vocab = probe.getVocabulary();
        int defIdx = List.of(probe.getRuleNames()).indexOf("definition");

        PureGrammarParser oracle = PureGrammarParser.newInstance();
        Random rnd = new Random(SEED);
        int bothAccept = 0;
        int bothReject = 0;
        List<String> diverge = new ArrayList<>();
        for (int i = 0; i < SENTENCES; i++) {
            String sentence = generate(atn, vocab, defIdx, rnd);
            if (sentence == null || sentence.isBlank()) {
                continue;
            }
            boolean o = accepts(() -> oracle.parseModel(sentence));
            boolean l = accepts(() -> Surfaces.engine(sentence));
            if (o == l) {
                if (o) {
                    bothAccept++;
                } else {
                    bothReject++;
                }
            } else if (diverge.size() < 300) {
                diverge.add((o ? "ORACLE-ONLY" : "LITE-ONLY") + " :: "
                        + sentence.substring(0,
                                Math.min(160, sentence.length()))
                                .replace('\n', ' '));
            }
        }
        Files.writeString(Repo.out("generative-dual-parse.txt"),
                "seed " + SEED + " sentences " + SENTENCES
                        + " bothAccept " + bothAccept
                        + " bothReject " + bothReject
                        + " diverge " + diverge.size() + "\n\n"
                        + String.join("\n", diverge));
        assertTrue(diverge.size() <= MAX_DIVERGENCES,
                "generative divergences grew: " + diverge.size()
                        + " (target/generative-dual-parse.txt)");
        assertTrue(bothAccept + bothReject + diverge.size() > 500,
                "generator produced too few parseable sentences — "
                        + "renderer or walk broke");
    }

    /** Bounded random walk of the ATN from {@code ruleIdx}'s start
     *  state; null when the walk exceeds its budget. */
    private static @com.legend.Nullable String generate(ATN atn,
            Vocabulary vocab, int ruleIdx, Random rnd) {
        StringBuilder out = new StringBuilder();
        Deque<ATNState> stack = new ArrayDeque<>();
        RuleStartState start = atn.ruleToStartState[ruleIdx];
        ATNState st = start;
        int steps = 0;
        while (steps++ < MAX_STEPS) {
            if (st instanceof org.antlr.v4.runtime.atn.RuleStopState) {
                if (stack.isEmpty()) {
                    return out.toString();
                }
                st = stack.pop();
                continue;
            }
            int n = st.getNumberOfTransitions();
            if (n == 0) {
                return null;
            }
            Transition t = st.transition(rnd.nextInt(n));
            if (t instanceof RuleTransition rt) {
                stack.push(rt.followState);
                st = rt.target;
                continue;
            }
            IntervalSet label = t.label();
            if (label != null && label.size() > 0) {
                int tok = label.toList().get(
                        rnd.nextInt(label.size()));
                out.append(render(tok, vocab)).append(' ');
            }
            st = t.target;
        }
        return null;
    }

    private static String render(int tokenType, Vocabulary vocab) {
        String lit = vocab.getLiteralName(tokenType);
        if (lit != null) {
            return lit.substring(1, lit.length() - 1);
        }
        String sym = String.valueOf(vocab.getSymbolicName(tokenType));
        return switch (sym) {
            case "VALID_STRING", "IDENTIFIER" -> "a";
            case "STRING" -> "'s'";
            case "INTEGER" -> "1";
            case "FLOAT", "DECIMAL" -> "1.0";
            case "DATE" -> "%2020-01-01";
            case "BOOLEAN" -> "true";
            case "PATH_SEPARATOR" -> "::";
            default -> "a";
        };
    }

    private static boolean accepts(Runnable r) {
        try {
            r.run();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
