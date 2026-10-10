// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ###ExternalFormat} hosts TWO element kinds (ZTailProbe
 * "externalFormatShapes"): {@code SchemaSet} (format + schema list, each
 * schema's content span being the string TOKEN itself) and {@code Binding}
 * (schema pointer + contentType + model include/exclude paths).
 */
public final class ExternalFormatSectionGrammar
        implements ElementwiseSectionGrammar {

    public static final ExternalFormatSectionGrammar INSTANCE =
            new ExternalFormatSectionGrammar();

    private ExternalFormatSectionGrammar() {
    }

    @Override
    public String name() {
        return "ExternalFormat";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return e instanceof Protocol.PSchemaSet s ? s.qualifiedName()
                : ((Protocol.PBinding) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        String kind = c.safeText();
        return switch (kind) {
            case "SchemaSet" -> parseSchemaSet(c);
            case "Binding" -> parseBinding(c);
            default -> throw c.error(
                    "unsupported ###ExternalFormat element: " + kind);
        };
    }

    private static Protocol.PSchemaSet parseSchemaSet(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "SchemaSet");
        c.expect(TokenType.BRACE_OPEN);
        String format = null;
        int formatTok = -1;
        List<Protocol.PSchema> schemas = new ArrayList<>();
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            int keyTok = c.pos();
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c);
            c.expect(TokenType.COLON);
            switch (key) {
                case "format" -> {
                    formatTok = keyTok;
                    format = c.parseIdentifier();
                }
                case "schemas" -> parseSchemas(c, schemas);
                default -> throw c.error(
                        "unknown SchemaSet key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (format == null) {
            throw c.error("SchemaSet needs a format");
        }
        // engine-parity validation: the PRODUCTION engine's format
        // extensions (corpus-evidenced set). 'Example' exists only in the
        // engine's OWN test sources — production refuses it, so do we.
        if (!"FlatData".equals(format) && !"JSON".equals(format)
                && !"XSD".equals(format)) {
            throw com.legend.parser.TokenStreamCursor.throwAt(
                    c.tokens(), formatTok,
                    "Unknown schema format: " + format);
        }
        return new Protocol.PSchemaSet(h.pkg(), h.name(), format, schemas,
                c.spanOf(h.declStart(), c.pos() - 1));
    }

    private static void parseSchemas(TokenStreamCursor c,
            List<Protocol.PSchema> out) {
        c.expect(TokenType.BRACKET_OPEN);
        if (c.peek() == TokenType.BRACKET_CLOSE) {
            // the engine grammar requires >= 1 schema — it refuses the
            // empty list at the ']' BEFORE any format validation
            // (reprobe TestExternalFormatGrammarParser#6)
            throw c.error("Unexpected token ']'");
        }
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            int nodeStart = c.pos();
            c.expect(TokenType.BRACE_OPEN);
            String id = null;
            String location = null;
            String content = null;
            SourceInfo contentSpan = null;
            java.util.Set<String> seenSchemaKeys = new java.util.HashSet<>();
            while (c.peek() != TokenType.BRACE_CLOSE) {
                int keyStart = c.pos();
                String k = c.parseIdentifier();
                // once-only per schema entry (mutant duplicate-field probe)
                TokenStreamCursor.once(seenSchemaKeys, k, c);
                c.expect(TokenType.COLON);
                switch (k) {
                    case "id" -> id = c.parseIdentifier();
                    case "location" -> location = SectionParse.stringValue(c);
                    case "content" -> content = SectionParse.stringValue(c);
                    default -> throw c.error(
                            "unknown schema key '" + k + "'");
                }
                c.expect(TokenType.SEMI_COLON);
                if ("content".equals(k)) {
                    // the wire span covers the WHOLE `content: '...';`
                    // statement, semicolon included (ZTailProbe schema-esc)
                    contentSpan = c.spanOf(keyStart, c.pos() - 1);
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (content == null || contentSpan == null) {
                throw c.error("schema needs content");
            }
            out.add(new Protocol.PSchema(id, location, content, contentSpan,
                    c.spanOf(nodeStart, c.pos() - 1)));
            if (!c.match(TokenType.COMMA)) {
                break;
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }

    private static Protocol.PBinding parseBinding(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Binding");
        c.expect(TokenType.BRACE_OPEN);
        String schemaSet = null;
        String schemaId = null;
        String contentType = null;
        List<String> includes = new ArrayList<>();
        List<String> excludes = new ArrayList<>();
        java.util.Set<String> seenKeys2 = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys2, key, c);
            c.expect(TokenType.COLON);
            switch (key) {
                case "schemaSet" -> schemaSet = Protocol.unquotePath(
                        c.parseQualifiedName());
                case "schemaId" -> schemaId = c.parseIdentifier();
                case "contentType" -> contentType =
                        SectionParse.stringValue(c);
                case "modelIncludes" -> parsePaths(c, includes);
                case "modelExcludes" -> parsePaths(c, excludes);
                default -> throw c.error(
                        "unknown Binding key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (contentType == null) {
            throw c.error("Binding needs a contentType");
        }
        if (includes.isEmpty()) {
            // walker-required (sibling negative
            // neg-extformat-binding-missing-modelincludes)
            throw TokenStreamCursor.throwAt(c.tokens(), h.declStart(),
                    "Field 'modelIncludes' is required");
        }
        return new Protocol.PBinding(h.pkg(), h.name(), schemaSet, schemaId,
                contentType, includes, excludes,
                c.spanOf(h.declStart(), c.pos() - 1));
    }

    private static void parsePaths(TokenStreamCursor c, List<String> out) {
        c.expect(TokenType.BRACKET_OPEN);
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            out.add(Protocol.unquotePath(c.parseQualifiedName()));
            if (!c.match(TokenType.COMMA)) {
                break;
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
    }
}
