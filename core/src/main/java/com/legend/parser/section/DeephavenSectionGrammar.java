// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ###Deephaven} hosts TWO element kinds: the {@code Deephaven}
 * store (paren body of typed tables — ZTailProbe "deephaven-store" /
 * "deephaven-cols") and the {@code DeephavenApp} activator, whose body is
 * the FUNCTION-ACTIVATOR grammar exactly (ZTailProbe "deephaven-app").
 */
public final class DeephavenSectionGrammar
        implements ElementwiseSectionGrammar {

    public static final DeephavenSectionGrammar INSTANCE =
            new DeephavenSectionGrammar();

    private static final FunctionActivatorSectionGrammar APP =
            new FunctionActivatorSectionGrammar("Deephaven",
                    java.util.Set.of("DeephavenApp"));

    private static final java.util.Map<String, String> COLUMN_TYPES =
            java.util.Map.of("STRING", "stringType", "INT", "intType",
                    "BOOLEAN", "booleanType", "DATETIME", "dateTimeType",
                    "FLOAT", "floatType", "DOUBLE", "doubleType",
                    "TIMESTAMP", "timestampType", "DECIMAL", "decimalType");

    private DeephavenSectionGrammar() {
    }

    @Override
    public String name() {
        return "Deephaven";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return e instanceof Protocol.PFunctionActivator a ? a.qualifiedName()
                : ((Protocol.PDeephavenDatabase) e).qualifiedName();
    }

    @Override
    public com.legend.model.PackageableElement toModel(Protocol.Element element) {
        return com.legend.model.ModelFromProtocol.element(element);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        String kind = c.safeText();
        return switch (kind) {
            case "Deephaven" -> parseStore(c);
            case "DeephavenApp" -> APP.parseElement(c);
            default -> throw c.error(
                    "unsupported ###Deephaven element: " + kind);
        };
    }

    private static Protocol.PDeephavenDatabase parseStore(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Deephaven");
        c.expect(TokenType.PAREN_OPEN);
        List<Protocol.PDeephavenDatabase.PDeephavenTable> tables =
                new ArrayList<>();
        while (!c.atEnd() && c.peek() != TokenType.PAREN_CLOSE) {
            String tk = c.parseIdentifier();
            if (!"Table".equals(tk)) {
                throw c.error("expected Table, got " + tk);
            }
            tables.add(parseTable(c));
        }
        c.expect(TokenType.PAREN_CLOSE);
        return new Protocol.PDeephavenDatabase(h.pkg(), h.name(), tables,
                c.spanOf(h.declStart(), c.pos() - 1));
    }

    private static Protocol.PDeephavenDatabase.PDeephavenTable parseTable(
            TokenStreamCursor c) {
        String tableName = c.parseIdentifier();
        c.expect(TokenType.PAREN_OPEN);
        List<Protocol.PDeephavenColumn> cols = new ArrayList<>();
        while (c.peek() != TokenType.PAREN_CLOSE) {
            String colName = c.parseIdentifier();
            c.expect(TokenType.COLON);
            String colType = c.parseIdentifier();
            String wire = COLUMN_TYPES.get(colType);
            if (wire == null) {
                throw c.error("unknown Deephaven column type " + colType);
            }
            Integer precision = null;
            Integer scale = null;
            if ("DECIMAL".equals(colType)) {
                c.expect(TokenType.PAREN_OPEN);
                precision = (int) c.consumeLong();
                c.expect(TokenType.COMMA);
                scale = (int) c.consumeLong();
                c.expect(TokenType.PAREN_CLOSE);
            }
            cols.add(new Protocol.PDeephavenColumn(colName, wire, precision,
                    scale));
            if (!c.match(TokenType.COMMA)) {
                break;
            }
        }
        c.expect(TokenType.PAREN_CLOSE);
        return new Protocol.PDeephavenDatabase.PDeephavenTable(tableName, cols);
    }
}
