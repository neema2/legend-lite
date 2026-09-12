// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ClassDefinition;
import com.legend.model.ParsedModel;
import com.legend.model.TaggedValue;
import com.legend.protocol.Protocol;
import com.legend.protocol.ProtocolEmitter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * DOCUMENTATION (legend-pure 5.99.0 / legend-engine 4.145.0, the batch-8
 * bump): a {@code '''...'''} literal before a declaration is sugar for the
 * {@code meta::pure::profiles::doc} {@code doc} tagged value — prepended,
 * its content literal (never unescaped) with the text-block layout and the
 * surrounding blank lines dropped, flagged {@code multiLine} on the wire.
 * The rules here are the engine's own {@code TestDocumentationParsing}
 * (gate 8 adjudicates the same fixtures byte for byte; this is the
 * platform-local statement of the same spec, for the day the corpus is
 * not at hand).
 */
class DocumentationTest {

    private static final String DOC = "meta::pure::profiles::doc";

    private static ParsedModel model(String src) {
        return ElementParser.parse(src, Dialect.LEGEND_ENGINE);
    }

    private static String wire(String src) {
        var ts = com.legend.lexer.Lexer.tokenize(src);
        ElementParser p = ElementParser.at(ts, 0, Dialect.LEGEND_ENGINE);
        Protocol.Element el = switch (ts.type(ts.type(0) == com.legend.lexer.TokenType.DOC_STRING ? 1 : 0)) {
            case CLASS -> p.parseClassDefinition(false);
            case ENUM -> p.parseEnumDefinition();
            case ASSOCIATION -> p.parseAssociationDefinition();
            case FUNCTION -> p.parseFunctionProtocol();
            default -> throw new AssertionError("no declaration at the start of: " + src);
        };
        return ProtocolEmitter.emitElement(el);
    }

    @Test
    @DisplayName("a block before a class is its doc tagged value, first, flagged multiLine on the wire")
    void onClass() {
        String src = "'''\nA person in the system.\n'''\nClass model::A\n{\n}\n";
        ClassDefinition c = (ClassDefinition) model(src).elements().get(0);
        assertEquals(java.util.List.of(new TaggedValue(DOC, "doc", "A person in the system.")),
                c.taggedValues());
        String json = wire(src);
        assertTrue(json.contains("\"value\":{\"_type\":\"string\",\"multiLine\":true,"
                + "\"value\":\"A person in the system.\"}"), json);
        // the tagged value spans the WHOLE literal, closing delimiter included
        assertTrue(json.contains("\"taggedValues\":[{\"sourceInformation\":{\"endColumn\":3,"
                + "\"endLine\":3,"), json);
    }

    @Test
    @DisplayName("sugar: the block and the explicit doc.doc tagged value hold the same string")
    void sugarForTheDocTag() {
        ClassDefinition sugared = (ClassDefinition) model(
                "'''\nDocumented.\n'''\nClass model::A\n{\n}\n").elements().get(0);
        ClassDefinition explicit = (ClassDefinition) model(
                "Class {meta::pure::profiles::doc.doc = 'Documented.'} model::A\n{\n}\n").elements().get(0);
        assertEquals(explicit.taggedValues(), sugared.taggedValues());
        // ...but only the block is flagged: an ordinary value stays a bare string
        assertFalse(wire("Class {meta::pure::profiles::doc.doc = 'Documented.'} model::A\n{\n}\n")
                .contains("multiLine"));
    }

    @Test
    @DisplayName("documentation precedes stereotypes and other tagged values; it comes first")
    void precedesAnnotations() {
        String json = wire("'''\nAttached to the class.\n'''\n"
                + "Class <<access.private>> {meta::pure::profiles::doc.todo = 'x'} model::A\n{\n}\n");
        int doc = json.indexOf("\"value\":\"doc\"");
        int todo = json.indexOf("\"value\":\"todo\"");
        assertTrue(doc > 0 && todo > doc, json);
        assertTrue(json.contains("\"stereotypes\":[{"), json);
    }

    @Test
    @DisplayName("properties, derived properties, enum values, functions and associations take it too")
    void onMembersAndOtherDeclarations() {
        String cls = wire("Class model::A\n{\n  '''\n  Given name.\n  '''\n  firstName: String[1];\n"
                + "  '''\n  Full legal name.\n  '''\n  fullName() {$this.firstName}: String[1];\n}\n");
        assertTrue(cls.contains("\"value\":\"Given name.\""), cls);
        assertTrue(cls.contains("\"value\":\"Full legal name.\""), cls);
        String en = wire("'''\nA currency.\n'''\nEnum model::A\n{\n  '''\n  US dollar.\n  '''\n  USD,\n  EUR\n}\n");
        assertTrue(en.contains("\"value\":\"A currency.\"") && en.contains("\"value\":\"US dollar.\""), en);
        String fn = wire("'''\nAnswers everything.\n'''\nfunction model::f(): Integer[1]\n{\n  42\n}\n");
        assertTrue(fn.contains("\"value\":\"Answers everything.\""), fn);
        String as = wire("'''\nLinks a B to a B.\n'''\nAssociation model::A\n{\n  left: model::B[1];\n  right: model::B[1];\n}\n");
        assertTrue(as.contains("\"value\":\"Links a B to a B.\""), as);
    }

    @Test
    @DisplayName("canonicalization: indentation floor from the closing delimiter, edge blank lines dropped, content literal")
    void canonicalization() {
        assertEquals("A\n\nB", docOf("'''\n  A\n\n  B\n  '''\nClass model::A\n{\n}\n"));
        assertEquals("Text\n\n    code", docOf("'''\n  Text\n\n      code\n  '''\nClass model::A\n{\n}\n"));
        assertEquals("Text", docOf("'''\n\nText\n\n'''\nClass model::A\n{\n}\n"));
        assertEquals("", docOf("'''\n'''\nClass model::A\n{\n}\n"));
        assertEquals("\\d+ and \\* and C:\\temp and \\n",
                docOf("'''\n\\d+ and \\* and C:\\temp and \\n\n'''\nClass model::A\n{\n}\n"));
        assertEquals("Options:\n* first\n* second",
                docOf("'''\nOptions:\n* first\n* second\n'''\nClass model::A\n{\n}\n"));
    }

    private static String docOf(String src) {
        ClassDefinition c = (ClassDefinition) model(src).elements().get(0);
        return c.taggedValues().get(0).value();
    }

    @Test
    @DisplayName("errors: a conflicting explicit doc.doc, and a plain string where a block is required")
    void errors() {
        String conflict = "'''\nFrom the documentation.\n'''\n"
                + "Class {meta::pure::profiles::doc.doc = 'From the tagged value.'} model::A\n{\n}\n";
        ParseException e = assertThrows(ParseException.class, () -> model(conflict));
        // located at the conflicting tagged value (the engine: [4:8-63])
        assertEquals("[4:8] Element has both documentation and an explicit doc.doc tagged value. Use one.",
                e.getMessage());
        String bareDoc = "'''\nx\n'''\nClass {doc.doc = 'y'} model::A\n{\n}\n";
        assertThrows(ParseException.class, () -> model(bareDoc));
        // a different profile that merely ends in `doc` is not the doc profile
        ClassDefinition ok = (ClassDefinition) model(
                "'''\nDocumented.\n'''\nClass {my::pkg::doc.doc = 'Not the doc profile.'} model::A\n{\n}\n")
                .elements().get(0);
        assertEquals(2, ok.taggedValues().size());
        ParseException s = assertThrows(ParseException.class,
                () -> model("'not a block'\nClass model::A\n{\n}\n"));
        assertEquals("[1:1] Documentation must be written as a multi-line ('''...''') literal", s.getMessage());
    }

    @Test
    @DisplayName("a block in expression position is a value, not documentation")
    void expressionPositionIsAValue() {
        String json = wire("Class model::A\n{\n  fullName() {'''\n  Formatted name.\n  '''}: String[1];\n}\n");
        assertFalse(json.contains("\"value\":\"doc\""), json);
    }
}
