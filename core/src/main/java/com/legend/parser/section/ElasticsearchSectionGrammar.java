// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/** {@code ###Elasticsearch} — {@code Elasticsearch7Cluster qn { indices:
 *  [ name: { properties: [ p: Keyword ]; } ]; }} (ZTailProbe
 *  "elastic-cluster"; validation re-derived from the engine's
 *  ElasticsearchParserGrammar.g4 + ElasticsearchStoreParseTreeWalker,
 *  deep-audit #2 "ES validation gaps"). */
public final class ElasticsearchSectionGrammar
        implements ElementwiseSectionGrammar {

    public static final ElasticsearchSectionGrammar INSTANCE =
            new ElasticsearchSectionGrammar();

    private ElasticsearchSectionGrammar() {
    }

    @Override
    public String name() {
        return "Elasticsearch";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PElasticsearch7Cluster) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Elasticsearch7Cluster");
        c.expect(TokenType.BRACE_OPEN);
        List<Protocol.PElasticsearch7Cluster.PEsIndex> indices = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c);
            c.expect(TokenType.COLON);
            if (!"indices".equals(key)) {
                throw c.error("unknown Elasticsearch7Cluster key '"
                        + key + "'");
            }
            indices = new ArrayList<>();
            c.expect(TokenType.BRACKET_OPEN);
            // indexDefinition (COMMA indexDefinition)* — non-empty,
            // no trailing comma
            do {
                indices.add(parseIndex(c));
            } while (c.match(TokenType.COMMA));
            c.expect(TokenType.BRACKET_CLOSE);
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (indices == null) {
            // walker: validateAndExtractRequiredField(ctx.indices(), ...)
            throw TokenStreamCursor.throwAt(c.tokens(), h.declStart(),
                    "Field 'indices' is required");
        }
        return new Protocol.PElasticsearch7Cluster(h.pkg(), h.name(), indices,
                c.spanOf(h.declStart(), c.pos() - 1));
    }

    /** {@code name: Type} (scalar, optional {@code { fields: [...]; }})
     *  or {@code name: Type { properties: [...]; }} (complex — the brace
     *  body is MANDATORY in the .g4 and {@code properties} is
     *  walker-required). The wire triple per type
     *  (key/_pure_protocol_type/type) was probed live incl. the DOUBLED
     *  _pure_protocol_type quirk. */
    private static Protocol.PElasticsearch7Cluster.PEsProperty parseProperty(
            TokenStreamCursor c) {
        String propName = nameOrString(c);
        c.expect(TokenType.COLON);
        int kindTok = c.pos();
        String kind = c.parseIdentifier();
        boolean complex = "Object".equals(kind) || "Nested".equals(kind);
        String[] t = switch (kind) {
            case "Keyword" -> new String[]{"keyword", "keywordProperty", "keyword"};
            case "Text" -> new String[]{"text", "textProperty", "text"};
            case "Date" -> new String[]{"date", "dateProperty", "date"};
            case "Short" -> new String[]{"_short", "shortNumberProperty", "short"};
            case "Byte" -> new String[]{"_byte", "byteNumberProperty", "byte"};
            case "Integer" -> new String[]{"integer", "integerNumberProperty", "integer"};
            case "Long" -> new String[]{"_long", "longNumberProperty", "long"};
            case "Float" -> new String[]{"_float", "floatNumberProperty", "float"};
            case "HalfFloat" -> new String[]{"half_float", "halfFloatNumberProperty", "half_float"};
            case "Double" -> new String[]{"_double", "doubleNumberProperty", "double"};
            case "Boolean" -> new String[]{"_boolean", "booleanProperty", "boolean"};
            case "Object" -> new String[]{"object", "objectProperty", "object"};
            case "Nested" -> new String[]{"nested", "nestedProperty", "nested"};
            default -> throw c.error("unmapped Elasticsearch property type '"
                    + kind + "'");
        };
        List<Protocol.PElasticsearch7Cluster.PEsProperty> fields = null;
        List<Protocol.PElasticsearch7Cluster.PEsProperty> children = null;
        if (complex && c.peek() != TokenType.BRACE_OPEN) {
            // complexPropertyDefinition: complexPropertyTypes
            // complexPropertyContent — the body is not optional
            throw TokenStreamCursor.throwAt(c.tokens(), kindTok,
                    kind + " property requires a { properties: [...]; } body");
        }
        if (c.match(TokenType.BRACE_OPEN)) {
            // scalar bodies take ONLY fields (0..1); complex bodies take
            // ONLY properties (exactly 1) — the keys are NOT interchangeable
            String want = complex ? "properties" : "fields";
            boolean seenBody = false;
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                String fk = c.parseIdentifier();
                if (!want.equals(fk)) {
                    throw c.error("unknown " + kind + " property key '"
                            + fk + "'");
                }
                if (seenBody) {
                    throw c.error("Field '" + fk
                            + "' should be specified only once");
                }
                seenBody = true;
                c.expect(TokenType.COLON);
                List<Protocol.PElasticsearch7Cluster.PEsProperty> into =
                        new ArrayList<>();
                c.expect(TokenType.BRACKET_OPEN);
                do {
                    into.add(parseProperty(c));
                } while (c.match(TokenType.COMMA));
                c.expect(TokenType.BRACKET_CLOSE);
                c.expect(TokenType.SEMI_COLON);
                // the walker collects nested arrays with
                // Collectors.toMap — a duplicate name throws
                java.util.Set<String> names = new java.util.HashSet<>();
                for (var p : into) {
                    if (!names.add(p.propertyName())) {
                        throw c.error("duplicate property name '"
                                + p.propertyName() + "'");
                    }
                }
                if (complex) {
                    children = into;
                } else {
                    fields = into;
                }
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (complex && children == null) {
                throw TokenStreamCursor.throwAt(c.tokens(), kindTok,
                        "Field 'properties' is required");
            }
            // `Keyword { }` emits NO fields key (probed): the walker's
            // empty map is dropped by the serializer, same as bare
            // `Keyword` — fields stays null either way
        }
        return new Protocol.PElasticsearch7Cluster.PEsProperty(propName,
                t[0], t[1], t[2], fields, children);
    }

    private static String nameOrString(TokenStreamCursor c) {
        if (c.peek() == TokenType.STRING) {
            return SectionParse.stringValue(c);
        }
        return c.parseIdentifier();
    }

    private static Protocol.PElasticsearch7Cluster.PEsIndex parseIndex(
            TokenStreamCursor c) {
        int nameTok = c.pos();
        String indexName = nameOrString(c);
        c.expect(TokenType.COLON);
        c.expect(TokenType.BRACE_OPEN);
        List<Protocol.PElasticsearch7Cluster.PEsProperty> props = null;
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String pk = c.parseIdentifier();
            c.expect(TokenType.COLON);
            if (!"properties".equals(pk)) {
                throw c.error("unknown index key '" + pk + "'");
            }
            if (props != null) {
                throw c.error("Field 'properties' should be"
                        + " specified only once");
            }
            props = new ArrayList<>();
            c.expect(TokenType.BRACKET_OPEN);
            do {
                props.add(parseProperty(c));
            } while (c.match(TokenType.COMMA));
            c.expect(TokenType.BRACKET_CLOSE);
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (props == null) {
            // walker: validateAndExtractRequiredField on properties
            throw TokenStreamCursor.throwAt(c.tokens(), nameTok,
                    "Field 'properties' is required");
        }
        return new Protocol.PElasticsearch7Cluster.PEsIndex(indexName, props);
    }
}
