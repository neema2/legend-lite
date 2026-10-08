package com.legend.lexer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** W0.8: an island's content is lexed in place -- the range's tokens carry the source's own offsets and the
 *  outer stream's line index, so a span inside an island is the document's span with no padded copy. */
class LexRangeTest {

    private static final String SOURCE = "###Mapping\nMapping a::M\n(\n  *a::C: Pure { ~src a::S }\n)\n"
            + "###Data\nData a::D #{ ExternalFormat #{ contentType: 'x'; data: 'y'; }# }#\n";

    @Test
    void theRangeCarriesTheSourcesOwnOffsetsAndLines() {
        TokenStream outer = Lexer.tokenize(SOURCE);
        int from = SOURCE.indexOf("ExternalFormat");
        int to = SOURCE.indexOf("}#", from);
        TokenStream inner = outer.lexRange(from, to);
        assertEquals("ExternalFormat", inner.text(0));
        assertEquals(from, inner.start(0), "the first token starts where the range starts, in the source's offsets");
        assertEquals(outer.lineOf(from), inner.startLine(0), "the line is the document's");
        assertEquals(outer.columnOf(from), inner.startColumn(0), "the column is the document's");
        assertSame(SOURCE, inner.source(), "the same source, not a copy");
        assertTrue(inner.end(inner.count() - 1) <= to, "no token reaches past the range");
    }

    @Test
    void aRangeOutsideTheSourceIsRefused() {
        TokenStream outer = Lexer.tokenize(SOURCE);
        assertThrows(IndexOutOfBoundsException.class, () -> outer.lexRange(5, SOURCE.length() + 1));
        assertThrows(IndexOutOfBoundsException.class, () -> outer.lexRange(9, 3));
    }
}
