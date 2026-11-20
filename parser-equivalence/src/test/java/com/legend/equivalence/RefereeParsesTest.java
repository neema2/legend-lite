package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What {@link RefereeParses} keeps of a referee that throws (the review of 37ef44ff0, 2026-11-19): a refusal and a
 * StackOverflowError are the text's, kept and thrown again; an OutOfMemoryError is one moment's pressure, thrown on and
 * never kept, so the next class to ask parses the text again. Each referee here is planted.
 */
class RefereeParsesTest {

    /** A model the engine parses: one class, {@code a::A}. */
    private static final String MODEL = "Class a::A\n{\n}\n";

    @Test
    void anOutOfMemoryErrorIsThrownOnAndNeverKept() throws Exception {
        AtomicInteger asked = new AtomicInteger();
        RefereeParses memo = new RefereeParses(key -> {
            if (asked.incrementAndGet() == 1) {
                throw new OutOfMemoryError("planted");
            }
            return PureGrammarParser.newInstance().parseModel(key.text());
        });
        RefereeParses.Key key = new RefereeParses.Key(MODEL, true);

        assertEquals("planted", assertThrows(OutOfMemoryError.class, () -> memo.answer(key)).getMessage());
        // the next ask parses the text again and gets the referee's answer
        assertTrue(memo.answer(key).json().contains("\"name\":\"A\""), "the class's own JSON");
        assertEquals(2, asked.get(), "the pressure was not kept: the text was parsed again");
        // and that answer is the one kept
        memo.answer(key);
        assertEquals(2, asked.get(), "the parse is kept once made");
    }

    @Test
    void aRefusalAndAStackOverflowAreKeptAndThrownAgain() {
        IllegalStateException refusal = new IllegalStateException("planted refusal");
        StackOverflowError deep = new StackOverflowError("planted");
        AtomicInteger asked = new AtomicInteger();
        RefereeParses memo = new RefereeParses(key -> {
            asked.incrementAndGet();
            if (key.text().equals("refused")) {
                throw refusal;
            }
            throw deep;
        });
        RefereeParses.Key refused = new RefereeParses.Key("refused", true);
        RefereeParses.Key tooDeep = new RefereeParses.Key("too deep", true);

        for (int ask = 0; ask < 2; ask++) {
            assertSame(refusal, assertThrows(IllegalStateException.class, () -> memo.answer(refused)));
            assertSame(deep, assertThrows(StackOverflowError.class, () -> memo.answer(tooDeep)));
        }
        assertEquals(2, asked.get(), "each text parsed once: its refusal and its overflow are kept");
    }
}
