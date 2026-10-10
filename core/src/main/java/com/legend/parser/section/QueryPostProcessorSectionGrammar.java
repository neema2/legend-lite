// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * THE {@code ###QueryPostProcessor} grammar — owns the
 * {@code RelationalMapper} element (harvest
 * TestRelationalMapperCompilationFromGrammar; wire probe rm.out):
 * DatabaseMappers ({@code [db.Schema, ...] -> 'name'}), SchemaMappers
 * ({@code db.Schema -> 'name'}) and TableMappers
 * ({@code db.Schema.Table -> 'name'}), each list optional, any order.
 */
public final class QueryPostProcessorSectionGrammar
        implements ElementwiseSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final QueryPostProcessorSectionGrammar INSTANCE =
            new QueryPostProcessorSectionGrammar();

    private QueryPostProcessorSectionGrammar() {
    }

    @Override
    public String name() {
        return "QueryPostProcessor";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PRelationalMapper) e).qualifiedName();
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        if (!(c.isIdentifierToken(c.peek())
                && "RelationalMapper".equals(c.safeText()))) {
            throw c.error("unsupported ###QueryPostProcessor element: "
                    + c.safeText());
        }
        int declStart = c.pos();
        c.advance();
        String qn = Protocol.unquotePath(c.parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        c.expect(TokenType.PAREN_OPEN);
        List<Protocol.PDatabaseMapper> dbMappers = new ArrayList<>();
        List<Protocol.PSchemaMapper2> schemaMappers = new ArrayList<>();
        List<Protocol.PTableMapper2> tableMappers = new ArrayList<>();
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c);
            c.expect(TokenType.COLON);
            c.expect(TokenType.BRACKET_OPEN);
            switch (key) {
                case "DatabaseMappers" -> {
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        c.expect(TokenType.BRACKET_OPEN);
                        List<Protocol.PSchemaPointer> schemas =
                                new ArrayList<>();
                        while (c.peek() != TokenType.BRACKET_CLOSE
                                && !c.atEnd()) {
                            schemas.add(schemaPointer(c));
                            c.match(TokenType.COMMA);
                        }
                        c.expect(TokenType.BRACKET_CLOSE);
                        c.expect(TokenType.ARROW);
                        String to = SectionParse.stringValue(c);
                        dbMappers.add(new Protocol.PDatabaseMapper(to,
                                schemas));
                        c.match(TokenType.COMMA);
                    }
                }
                case "SchemaMappers" -> {
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        Protocol.PSchemaPointer from = schemaPointer(c);
                        c.expect(TokenType.ARROW);
                        schemaMappers.add(new Protocol.PSchemaMapper2(from,
                                SectionParse.stringValue(c)));
                        c.match(TokenType.COMMA);
                    }
                }
                case "TableMappers" -> {
                    while (c.peek() != TokenType.BRACKET_CLOSE
                            && !c.atEnd()) {
                        Protocol.PTablePointer2 from = tablePointer(c);
                        c.expect(TokenType.ARROW);
                        tableMappers.add(new Protocol.PTableMapper2(from,
                                SectionParse.stringValue(c)));
                        c.match(TokenType.COMMA);
                    }
                }
                default -> throw c.error("unknown RelationalMapper key: "
                        + key);
            }
            c.expect(TokenType.BRACKET_CLOSE);
            c.expect(TokenType.SEMI_COLON);
        }
        int close = c.pos();
        c.expect(TokenType.PAREN_CLOSE);
        return new Protocol.PRelationalMapper(pkg, name, dbMappers,
                schemaMappers, tableMappers, c.spanOf(declStart, close));
    }

    /** {@code db::path.Schema} — span covers the whole dotted path. */
    private static Protocol.PSchemaPointer schemaPointer(TokenStreamCursor c) {
        int s = c.pos();
        String db = Protocol.unquotePath(c.parseQualifiedName());
        c.expect(TokenType.DOT);
        String schema = c.parseIdentifier();
        return new Protocol.PSchemaPointer(db, schema,
                c.spanOf(s, c.pos() - 1));
    }

    /** {@code db::path.Schema.Table}. */
    private static Protocol.PTablePointer2 tablePointer(TokenStreamCursor c) {
        int s = c.pos();
        String db = Protocol.unquotePath(c.parseQualifiedName());
        c.expect(TokenType.DOT);
        String schema = c.parseIdentifier();
        c.expect(TokenType.DOT);
        String table = c.parseIdentifier();
        return new Protocol.PTablePointer2(db, schema, table,
                c.spanOf(s, c.pos() - 1));
    }


    @Override
    public com.legend.model.PackageableElement toModel(
            Protocol.Element element) {
        // parse-only element: the model carries the named carrier; no compiler phase opens it
        return com.legend.model.ModelFromProtocol.element(element);
    }
}
