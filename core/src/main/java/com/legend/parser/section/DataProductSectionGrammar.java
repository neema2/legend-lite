// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * THE {@code ###DataProduct} grammar, the part lite carries (docs/DEPLOYMENT_DESIGN_2026_11_14.md §7, step 2; register
 * S408). Legend's own grammar is closed -- legend-engine's public history never held it -- so the forms legend-studio
 * shows written (its round-trip fixtures' comments, {@code TEST_DATA__DataProductRoundtrip.ts}) are kept exactly:
 *
 * <pre>
 * DataProduct &lt;&lt;p::P.s&gt;&gt; {p::P.t = 'v'} x::A
 * {
 *   title: 'Test title'
 *   description: 'Test description'
 *   accessPoints: [
 *     group &lt;&lt;p::P.s&gt;&gt; ('Group description') [
 *       myId ('Id description'): LH(Snowflake, |1)classification1,
 *       latest: LH(Snowflake, |1)classification1
 *     ]
 *   ]
 * }
 * </pre>
 *
 * Access points written outside a group are the default group the engine makes of them ({@code default}, "The default
 * access group", as the fixtures hold). Lite's own spellings: a model access group, {@code id ('...') Model { mapping:
 * m::M; } [ ... ]} (Studio shows none written), and an access point with no target, {@code LH(|...)} (upstream's target
 * is optional). Every other field of upstream's data product is refused by name until lite carries it.
 *
 * <p>The engine's dialect never reaches here: it refuses the section as unknown, in its own words.
 */
public final class DataProductSectionGrammar implements ElementwiseSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final DataProductSectionGrammar INSTANCE = new DataProductSectionGrammar();

    /** The group the engine makes of the access points written outside one (legend-studio's fixtures). */
    public static final String DEFAULT_GROUP = "default";
    public static final String DEFAULT_GROUP_DESCRIPTION = "The default access group";

    /** Upstream's other fields, refused by name: lite does not carry them yet. */
    private static final Set<String> NOT_CARRIED = Set.of("icon", "deliveryFrequency", "coverageRegions",
            "supportInfo", "type", "owner", "nativeModelAccess", "sampleValues", "operationalMetadata", "testSuites",
            "prodDataProductOverride", "expertise");

    private DataProductSectionGrammar() {
    }

    @Override
    public String name() {
        return "DataProduct";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return Protocol.pathOf(e);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        if (c.dialect().refusesLiteExtensions()) {
            throw c.error("'DataProduct' is not a known section parser");
        }
        SectionParse.Head h = SectionParse.head(c, "DataProduct");
        c.expect(TokenType.BRACE_OPEN);
        String title = null;
        String description = null;
        List<Protocol.PAccessPointGroup> groups = null;
        Set<String> seen = new HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            if (!seen.add(key)) {
                throw c.error("the data product's '" + key + "' is written twice");
            }
            if (NOT_CARRIED.contains(key)) {
                throw c.error("the data product's '" + key + "' is not carried by lite yet");
            }
            c.expect(TokenType.COLON);
            switch (key) {
                case "title" -> title = SectionParse.stringValue(c);
                case "description" -> description = SectionParse.stringValue(c);
                case "accessPoints" -> groups = accessPoints(c);
                default -> throw c.error("unknown data product field: " + key + " (title, description, accessPoints)");
            }
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (groups == null) {
            throw c.error("the data product " + h.name() + " writes no accessPoints");
        }
        return new Protocol.PDataProduct(h.pkg(), h.name(), h.dec().stereotypes(), h.dec().taggedValues(), title,
                description, groups, c.spanOf(h.declStart(), c.pos() - 1));
    }

    /** {@code [ item, ... ]}: access points outside a group (the default group) or groups, never both. */
    private static List<Protocol.PAccessPointGroup> accessPoints(TokenStreamCursor c) {
        c.expect(TokenType.BRACKET_OPEN);
        List<Protocol.PAccessPointGroup> groups = new ArrayList<>();
        List<Protocol.PLakehouseAccessPoint> loose = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
            String id = c.parseIdentifier();
            TokenStreamCursor.Decorations dec = c.parseDecorations();
            String description = described(c);
            if (c.peek() == TokenType.COLON) {
                if (!dec.stereotypes().isEmpty() || !dec.taggedValues().isEmpty()) {
                    throw c.error("the access point " + id + "'s decorations are not carried by lite yet");
                }
                loose.add(accessPoint(c, id, description));
            } else {
                if (!dec.taggedValues().isEmpty()) {
                    throw c.error("the access point group " + id + "'s tagged values are not carried by lite yet");
                }
                if (!ids.add(id)) {
                    throw c.error("the access point group " + id + " is written twice");
                }
                groups.add(group(c, id, description, dec.stereotypes()));
            }
            if (c.peek() != TokenType.BRACKET_CLOSE) {
                c.expect(TokenType.COMMA);
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
        if (!loose.isEmpty() && !groups.isEmpty()) {
            throw c.error("access points written outside a group beside groups: write each in a group");
        }
        if (!loose.isEmpty()) {
            groups.add(new Protocol.PAccessPointGroup(DEFAULT_GROUP, DEFAULT_GROUP_DESCRIPTION, List.of(), null, loose));
        }
        return groups;
    }

    /** {@code ('...')}, or nothing. */
    private static @com.legend.base.Nullable String described(TokenStreamCursor c) {
        if (c.peek() != TokenType.PAREN_OPEN) {
            return null;
        }
        c.advance();
        String d = SectionParse.stringValue(c);
        c.expect(TokenType.PAREN_CLOSE);
        return d;
    }

    /** {@code [ ap, ... ]} for a default group, {@code Model { mapping: m::M; } [ ap, ... ]} for a model group. */
    private static Protocol.PAccessPointGroup group(TokenStreamCursor c, String id,
            @com.legend.base.Nullable String description, List<Protocol.PStereotype> stereotypes) {
        String mapping = null;
        if (c.peek() != TokenType.BRACKET_OPEN) {
            String kind = c.parseIdentifier();
            if (!kind.equals("Model")) {
                throw c.error("the access point group " + id + " is '" + kind + "': a group is [ ... ] or"
                        + " Model { mapping: ...; } [ ... ]");
            }
            c.expect(TokenType.BRACE_OPEN);
            while (!c.atEnd() && c.peek() != TokenType.BRACE_CLOSE) {
                String key = c.parseIdentifier();
                if (!key.equals("mapping") || mapping != null) {
                    throw c.error("the model access group " + id + "'s '" + key + "' is not carried by lite yet"
                            + " (mapping, once)");
                }
                c.expect(TokenType.COLON);
                mapping = Protocol.unquotePath(c.parseQualifiedName());
                c.expect(TokenType.SEMI_COLON);
            }
            c.expect(TokenType.BRACE_CLOSE);
            if (mapping == null) {
                throw c.error("the model access group " + id + " names no mapping");
            }
        }
        c.expect(TokenType.BRACKET_OPEN);
        List<Protocol.PLakehouseAccessPoint> points = new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.BRACKET_CLOSE) {
            String apId = c.parseIdentifier();
            String apDescription = described(c);
            points.add(accessPoint(c, apId, apDescription));
            if (c.peek() != TokenType.BRACKET_CLOSE) {
                c.expect(TokenType.COMMA);
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return new Protocol.PAccessPointGroup(id, description, stereotypes, mapping, points);
    }

    /** {@code : LH(Target, |lambda)classification}, the target and the classification each optional. */
    private static Protocol.PLakehouseAccessPoint accessPoint(TokenStreamCursor c, String id,
            @com.legend.base.Nullable String description) {
        c.expect(TokenType.COLON);
        String kind = c.parseIdentifier();
        if (!kind.equals("LH")) {
            throw c.error("the access point " + id + " is '" + kind + "': lite carries lakehouse access points,"
                    + " LH(...)");
        }
        c.expect(TokenType.PAREN_OPEN);
        String target = null;
        if (c.peek() != TokenType.PIPE) {
            target = c.parseIdentifier();
            c.expect(TokenType.COMMA);
        }
        int from = c.pos();
        int depth = 0;
        while (!(c.peek() == TokenType.PAREN_CLOSE && depth == 0)) {
            if (c.atEnd()) {
                throw c.error("the access point " + id + "'s function is not closed");
            }
            switch (c.peek()) {
                case PAREN_OPEN, BRACE_OPEN, BRACKET_OPEN -> depth++;
                case PAREN_CLOSE, BRACE_CLOSE, BRACKET_CLOSE -> depth--;
                default -> { }
            }
            c.advance();
        }
        var spec = com.legend.parser.SpecParser.parse(c.tokens().slice(from, c.pos()), c.dialect());
        if (!(spec instanceof com.legend.protocol.spec.LambdaFunction lambda)) {
            throw c.error("the access point " + id + "'s function is a lambda, |<expression>");
        }
        c.expect(TokenType.PAREN_CLOSE);
        String classification = c.isIdentifierToken(c.peek()) ? c.parseIdentifier() : null;
        return new Protocol.PLakehouseAccessPoint(id, description, target, classification, lambda, false);
    }
}
