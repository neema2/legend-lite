package com.legend.sdlc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sdlc.grammar.CoreGrammar;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * THE SDLC'S COMPILE CHECK IS LITE'S OWN ({@link CoreGrammar#compile}: "as legend-lite's compilation/compile finds
 * them"): the check a review must pass to land answers as lite's own compile route does, so since the edition
 * program's step 4 (2026-11-26) a comparison of two different types inside a function's body is a compile error
 * here too, and a tree holding one does not land. Held here because nothing held it: the independent audit of that
 * step found the SDLC's answer had moved with no test and no record. Which meaning the SDLC gives a project that
 * declares none is the user's to rule (lite's, as built; or legend-engine's, whose compile route accepts such a
 * body); this test is where that ruling shows.
 */
class CoreGrammarTest {

    @Test
    void aComparisonAcrossTypesInABodyDoesNotCompile_asAtLitesOwnCompileRoute() {
        List<String> errors = new CoreGrammar().compile(
                "function demo::loose(s: String[1]): Boolean[1] { $s == $s->length() }");
        assertEquals(1, errors.size(), errors.toString());
        assertTrue(errors.get(0).contains("== compares String with Integer"), errors.get(0));
    }

    @Test
    void aNumberLiteralAgainstAValueOfAnotherNumberTypeCompiles() {
        assertEquals(List.of(), new CoreGrammar().compile(
                "function demo::fine(x: Float[1]): Boolean[1] { $x > 0 }"));
    }
}
