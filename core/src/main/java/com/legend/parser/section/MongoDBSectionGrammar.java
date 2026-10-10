// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ###MongoDB} — {@code Database qn ( Collection name ( ... ) )}
 * (ZTailProbe "mongo-db"/"mongo-rich"/"mongo-empty"): collections carry
 * validation levels and a {@code jsonSchema:} island, captured balanced,
 * read by the platform JSON reader and mapped to typed BSON schema nodes.
 */
public final class MongoDBSectionGrammar implements ElementwiseSectionGrammar {

    public static final MongoDBSectionGrammar INSTANCE =
            new MongoDBSectionGrammar();

    private MongoDBSectionGrammar() {
    }

    @Override
    public String name() {
        return "MongoDB";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return ((Protocol.PMongoDatabase) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Database");
        c.expect(TokenType.PAREN_OPEN);
        List<Protocol.PMongoDatabase.PMongoCollection> collections =
                new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            String ck = c.parseIdentifier();
            if ("include".equals(ck)) {
                // the walker parses `include qn` and DROPS it —
                // includedStores stays [] on the wire (probe t2-mongodb
                // 2026-08-14)
                c.parseQualifiedName();
                continue;
            }
            if ("Join".equals(ck)) {
                // Join name ( ... ) — parsed and DROPPED the same way
                // (probe t2-mongodb: no join on the wire)
                c.parseIdentifier();
                c.skipBalancedBlock();
                continue;
            }
            if (!"Collection".equals(ck)) {
                throw c.error("expected Collection, got " + ck);
            }
            collections.add(parseCollection(c));
        }
        c.expect(TokenType.PAREN_CLOSE);
        return new Protocol.PMongoDatabase(h.pkg(), h.name(), collections,
                c.spanOf(h.declStart(), c.pos() - 1));
    }

    private static Protocol.PMongoDatabase.PMongoCollection parseCollection(
            TokenStreamCursor c) {
        String collName;
        if (c.peek() == TokenType.QUOTED_STRING) {
            String raw = c.text();
            c.expect(TokenType.QUOTED_STRING);
            collName = raw.length() >= 2 && raw.charAt(0) == '"'
                    && raw.charAt(raw.length() - 1) == '"'
                    ? raw.substring(1, raw.length() - 1) : raw;
        } else {
            collName = c.parseIdentifier();
        }
        c.expect(TokenType.PAREN_OPEN);
        String level = null;
        String action = null;
        Protocol.PBsonSchema schema = null;
        java.util.Set<String> seenKeys = new java.util.HashSet<>();
        int lastRank = -1;
        while (c.peek() != TokenType.PAREN_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seenKeys, key, c);
            c.expect(TokenType.COLON);
            // the .g4 fixes the field ORDER (mutant swap-siblings probe:
            // engine refuses validationAction before validationLevel)
            int rank = switch (key) {
                case "validationLevel" -> 0;
                case "validationAction" -> 1;
                case "jsonSchema" -> 2;
                default -> throw c.error(
                        "unknown Collection key '" + key + "'");
            };
            if (rank < lastRank) {
                throw c.error("Unexpected token '" + key + "'");
            }
            lastRank = rank;
            switch (key) {
                case "validationLevel" -> level = c.parseIdentifier();
                case "validationAction" -> action = c.parseIdentifier();
                case "jsonSchema" -> schema = parseSchemaIsland(c);
                default -> throw c.error(
                        "unknown Collection key '" + key + "'");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.PAREN_CLOSE);
        // validationLevel/validationAction are OPTIONAL with wire
        // DEFAULTS strict/error (probed both-absent 2026-08-14)
        if (level == null) {
            level = "strict";
        }
        if (action == null) {
            action = "error";
        }
        if (schema == null) {
            throw c.error("Collection '" + collName + "' needs jsonSchema");
        }
        return new Protocol.PMongoDatabase.PMongoCollection(collName, level,
                action, schema);
    }

    /** The island is already lexed by the shared lexer (double-quoted keys
     *  are QUOTED_STRING tokens), so the JSON walks as TOKENS — the
     *  parser's native medium; no string re-parse, no layer crossing. */
    private static Protocol.PBsonSchema parseSchemaIsland(TokenStreamCursor c) {
        Object parsed = parseJsonValue(c);
        if (!(parsed instanceof java.util.Map<?, ?> m)) {
            throw c.error("jsonSchema is not a JSON object");
        }
        Object rootType = m.get("bsonType");
        if (rootType != null && !"object".equals(rootType)) {
            // the ROOT schema must be an object (sibling negative
            // neg-mongodb-jsonschema-not-object; engine's Jackson
            // deserialization refuses a non-object root)
            throw c.error("jsonSchema root must be an object");
        }
        return schemaOf(m, true, c);
    }

    /** F3.1e EXEMPTION (recorded): this stays a separate JSON walk on
     *  purpose — it is TOKEN-level (it rides the section lexer's tokens,
     *  never raw chars), decodes no escapes of its own, and is LOUD on
     *  floats; collapsing it into the char-level platform reader would
     *  mean re-lexing text the cursor already tokenized. */
    private static Object parseJsonValue(TokenStreamCursor c) {
        switch (c.peek()) {
            case BRACE_OPEN -> {
                c.advance();
                var m = new java.util.LinkedHashMap<String, Object>();
                while (c.peek() != TokenType.BRACE_CLOSE) {
                    String raw = c.text();
                    c.expect(TokenType.QUOTED_STRING);
                    c.expect(TokenType.COLON);
                    m.put(raw.substring(1, raw.length() - 1),
                            parseJsonValue(c));
                    if (!c.match(TokenType.COMMA)) {
                        break;
                    }
                }
                c.expect(TokenType.BRACE_CLOSE);
                return m;
            }
            case BRACKET_OPEN -> {
                c.advance();
                var l = new ArrayList<Object>();
                while (c.peek() != TokenType.BRACKET_CLOSE) {
                    l.add(parseJsonValue(c));
                    if (!c.match(TokenType.COMMA)) {
                        break;
                    }
                }
                c.expect(TokenType.BRACKET_CLOSE);
                return l;
            }
            case QUOTED_STRING -> {
                String raw = c.text();
                c.advance();
                return raw.substring(1, raw.length() - 1);
            }
            case INTEGER -> {
                String t = c.text();
                c.advance();
                return Long.valueOf(t);
            }
            case MINUS -> {
                c.advance();
                String t = c.text();
                c.expect(TokenType.INTEGER);
                return Long.valueOf("-" + t);
            }
            case TRUE -> {
                c.advance();
                return Boolean.TRUE;
            }
            case FALSE -> {
                c.advance();
                return Boolean.FALSE;
            }
            default -> {
                if ("null".equals(c.safeText())) {
                    // JSON null — Jackson keeps the key with a null value;
                    // unknown schema keys drop anyway (probe t2-mongodb)
                    c.advance();
                    return JSON_NULL;
                }
                throw c.error("unsupported jsonSchema value: "
                        + c.safeText());
            }
        }
    }

    /** Sentinel for a JSON {@code null} literal inside a jsonSchema —
     *  kept out of the map-consumer casts (the null-policy sentinel tier;
     *  unknown keys drop on the wire regardless). */
    private static final Object JSON_NULL = new Object();

    private static Protocol.PBsonSchema schemaOf(java.util.Map<?, ?> m,
            boolean root, TokenStreamCursor at) {
        String bsonType = (String) m.get("bsonType");
        String wireType = switch (bsonType == null ? "object" : bsonType) {
            case "object" -> root ? "schema" : "objectType";
            case "string" -> "stringType";
            case "long" -> "longType";
            case "int" -> "intType";
            case "bool" -> "boolType";
            case "double" -> "doubleType";
            case "decimal" -> "decimalType";
            case "array" -> "arrayType";
            case "objectId" -> "objectIdType";
            default -> throw at.error("unmapped bsonType '" + bsonType + "'");
        };
        List<java.util.Map.Entry<String, Protocol.PBsonSchema>> props =
                new ArrayList<>();
        if (m.get("properties") instanceof java.util.Map<?, ?> pm) {
            for (java.util.Map.Entry<?, ?> e : pm.entrySet()) {
                props.add(java.util.Map.entry((String) e.getKey(),
                        schemaOf((java.util.Map<?, ?>) e.getValue(), false,
                                at)));
            }
        }
        List<String> required = new ArrayList<>();
        if (m.get("required") instanceof List<?> rl) {
            for (Object r : rl) {
                required.add((String) r);
            }
        }
        Object addl = m.get("additionalProperties");
        List<Protocol.PBsonSchema> items = null;
        Object it = m.get("items");
        if (it instanceof java.util.Map<?, ?> one) {
            items = List.of(schemaOf(one, false, at));
        } else if (it instanceof List<?> many) {
            items = new ArrayList<>();
            for (Object o : many) {
                items.add(schemaOf((java.util.Map<?, ?>) o, false, at));
            }
        }
        return new Protocol.PBsonSchema(wireType,
                addl == null ? null : (Boolean) addl, props, required,
                (String) m.get("title"), (String) m.get("description"),
                (Long) m.get("minLength"), (Long) m.get("maxLength"), items);
    }
}
