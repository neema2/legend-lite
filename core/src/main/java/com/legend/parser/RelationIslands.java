// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.List;

/**
 * {@code Relation #{ ... }#} islands read off a HOST cursor by grammars
 * outside the mapping parser — the mapping parser owns the island reader
 * and the relation-element rows; this is the seam other sections reach
 * them through.
 */
public final class RelationIslands {

    private RelationIslands() {
    }

    /** {@code Relation #{ ... }#} as ONE standalone relation element, no
     *  path line — a DataSpace executable's {@code sampleValues} (4.145.0;
     *  the engine reads it with its test-assertion relation reader and
     *  stamps the whole {@code Relation #{...}#} as its span). Any other
     *  kind refuses with the engine's message. */
    public static Protocol.PRelationElement parseStandaloneRelationAt(TokenStreamCursor host) {
        MappingProtocolParser p = new MappingProtocolParser(host.tokens(), host.pos(), host.dialect());
        int kindTok = p.pos();
        if (!(p.peek() == TokenType.VALID_STRING && "Relation".equals(p.text()))) {
            throw p.error("Data space executable sampleValues must be a standalone Relation element"
                    + " (e.g. sampleValues: Relation #{ ... }#), got type '" + p.safeText() + "'");
        }
        p.advance();
        int braceTok = p.pos();                     // the ISLAND_OPEN '#{'
        MappingProtocolParser.IslandBlock ri = p.readIsland();
        List<Protocol.PRelationElement> rels = p.parseRelationElements(ri, false);
        if (rels.size() != 1) {
            throw TokenStreamCursor.throwAt(host.tokens(), kindTok,
                    "sampleValues must be ONE relation element");
        }
        host.setPos(p.pos());
        Protocol.PRelationElement re = rels.get(0);
        // the engine's inner relation-data walker stamps the element from
        // just past '#{' to (the rows' terminating ';' one line EARLIER than
        // it stands) — the same coordinates as a test assertion's expected
        // relation (probed 2026-09-11 against 4.145.0: 10:11-12:15 for rows
        // ending at 13:15)
        SourceInfo es = re.sourceInformation();
        return new Protocol.PRelationElement(re.columns(), re.paths(), re.rows(),
                new SourceInfo("", host.tokens().startLine(braceTok),
                        host.tokens().startColumn(braceTok) + 2, es.endLine() - 1, es.endColumn()));
    }
}
