// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/** The LEGACY ServiceStore service-mapping tail — extracted from
 *  {@link MappingProtocolParser} (file-size guardrail). */
final class ServiceLegacyMappingParser {

    private ServiceLegacyMappingParser() {
    }

    /** The LEGACY service-mapping tail ({@code ~paramMapping(p: expr,
     *  ...)} and/or a bare {@code prop: $service.parameters.p} list —
     *  ServiceStoreParserGrammar "TO BE REMOVED" blocks). The walker
     *  builds a FRESH requestParametersBuildInfo (no spans on the
     *  envelope objects) and REPLACES any ~request parameters with it
     *  (probe t2-servicestore 2026-08-14). Consumes through the outer
     *  PAREN_CLOSE. */
    static Protocol.PServiceMapping parseTail(
            MappingProtocolParser p, String store, List<Protocol.PServiceSegment> segments,
            Protocol.@com.legend.base.Nullable PPathOffset pathOffset,
            Protocol.@com.legend.base.Nullable PRequestBuildInfo request,
            int svcStart) {
        List<Protocol.PParameterBuildInfo> legacy = new ArrayList<>();
        if (p.peek() == TokenType.TILDE && "paramMapping".equals(p.peekText(1))) {
            p.advance();
            p.parseIdentifier();                  // 'paramMapping'
            p.expect(TokenType.PAREN_OPEN);
            while (!p.atEnd() && p.peek() != TokenType.PAREN_CLOSE) {
                int eStart = p.pos();
                String pname;
                if (p.peek() == TokenType.QUOTED_STRING) {
                    pname = TokenStreamCursor.stripDoubleQuotes(p.text());
                    p.advance();
                } else {
                    pname = p.parseIdentifier();
                }
                p.expect(TokenType.COLON);
                int exprStart = p.pos();
                var expr = p.parseTransformExpr();
                legacy.add(new Protocol.PParameterBuildInfo(pname, expr,
                        p.spanOf(exprStart, p.pos() - 1), p.spanOf(eStart, p.pos() - 1)));
                p.match(TokenType.COMMA);
            }
            p.expect(TokenType.PAREN_CLOSE);
        }
        // bare mappingBlock: prop : $service.parameters.<name>
        while (!p.atEnd() && p.peek() != TokenType.PAREN_CLOSE) {
            int eStart = p.pos();
            String prop = p.parseIdentifier();
            p.expect(TokenType.COLON);
            p.expect(TokenType.DOLLAR);
            if (!"service".equals(p.parseIdentifier())) {
                throw p.error("elementMapping must read"
                        + " $service.parameters.<name>");
            }
            p.expect(TokenType.DOT);
            if (!"parameters".equals(p.parseIdentifier())) {
                throw p.error("elementMapping must read"
                        + " $service.parameters.<name>");
            }
            p.expect(TokenType.DOT);
            String pname = p.parseIdentifier();
            var entrySpan = p.spanOf(eStart, p.pos() - 1);
            // the walker re-parses the synthesized "$this.<prop>" snippet
            // anchored at the elementMapping start — $this spans 5 cols,
            // the property follows after the dot
            int line = entrySpan.startLine();
            int col = entrySpan.startColumn();
            var varSpan = com.legend.protocol.SpanOrigin
                    .syntheticAt(line, col, 5);           // "$this"
            var propSpan = com.legend.protocol.SpanOrigin
                    .syntheticAt(line, col + 6, prop.length());
            var expr = new com.legend.protocol.spec.AppliedProperty(
                    new com.legend.protocol.spec.Variable("this", null,
                            null, varSpan), prop, propSpan);
            legacy.add(new Protocol.PParameterBuildInfo(pname, expr,
                    entrySpan, entrySpan));
            p.match(TokenType.COMMA);
        }
        int outerEnd = p.pos();
        p.expect(TokenType.PAREN_CLOSE);
        var params = new Protocol.PParametersBuildInfo(legacy, null);
        Protocol.PRequestBuildInfo merged = request == null
                ? new Protocol.PRequestBuildInfo(params, null, null)
                : new Protocol.PRequestBuildInfo(params, request.body(),
                        request.sourceInformation());
        return new Protocol.PServiceMapping(
                new Protocol.PServicePtr(store, segments), pathOffset,
                merged, p.spanOf(svcStart, outerEnd));
    }

}
