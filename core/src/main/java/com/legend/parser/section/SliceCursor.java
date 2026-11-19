// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.parser.TokenStreamCursor;

/** THE minimal cursor over a re-lexed slice — the SPI feed's walker for
 *  every {@link ElementwiseSectionGrammar}. */
final class SliceCursor implements TokenStreamCursor {

    private final com.legend.lexer.TokenStream tokens;
    private final com.legend.parser.GrammarLevel dialect;
    private int pos;

    SliceCursor(com.legend.lexer.TokenStream tokens,
            com.legend.parser.GrammarLevel dialect) {
        this.tokens = tokens;
        this.dialect = dialect;
    }

    @Override
    public com.legend.parser.GrammarLevel dialect() {
        return dialect;
    }

    @Override
    public com.legend.lexer.TokenStream tokens() {
        return tokens;
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public void setPos(int pos) {
        this.pos = pos;
    }
}
