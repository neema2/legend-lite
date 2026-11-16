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
        return com.legend.door.ModelFromText.parse(src, Dialect.LEGEND_ENGINE);
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

    /** The one element of a document, its section index set aside. */
    private static Protocol.Element sectionElement(String src) {
        java.util.List<Protocol.Element> elements = PmcdParser.parseModel(src).elements().stream()
                .filter(e -> !(e instanceof Protocol.PSectionIndex)).toList();
        assertEquals(1, elements.size(), src);
        return elements.get(0);
    }

    private static void assertDocumented(String text, java.util.List<Protocol.PTaggedValue> tvs,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo span) {
        Protocol.PTaggedValue doc = tvs.get(0);
        assertEquals(DOC + ".doc", doc.tag().profile() + "." + doc.tag().value());
        assertEquals(text, doc.value());
        assertTrue(doc.multiLine());
        // the element starts at its documentation (line 2: the section header is line 1)
        assertEquals(2, java.util.Objects.requireNonNull(span).startLine());
        assertEquals(1, span.startColumn());
    }

    @Test
    @DisplayName("a block before a Service or a function activator is its doc tagged value, first; the element starts at"
            + " the block (Studio's grammar roundtrip, 2026-11-14)")
    void onServicesAndFunctionActivators() {
        Protocol.PService s = (Protocol.PService) sectionElement("###Service\n'''\nA documented service.\n'''\n"
                + "Service {a::P.t = 'x'} a::S\n{\n   pattern: '/s';\n   documentation: 'its own field';\n"
                + "   execution: Single\n   {\n      query: |1;\n      mapping: a::M;\n      runtime: a::R;\n   }\n}\n");
        assertDocumented("A documented service.", s.taggedValues(), s.sourceInformation());
        assertEquals("t", s.taggedValues().get(1).tag().value());
        assertEquals("its own field", s.documentation());
        String common = "   function : a::f():Integer[1];\n   ownership : Deployment { identifier: '1' };\n}\n";
        // section, kind, the kind's own required keys
        String[][] activators = {
                {"Snowflake", "SnowflakeApp", "   applicationName : 'x';\n"},
                {"Snowflake", "SnowflakeM2MUdf",
                        "   udfName : 'x';\n   deploymentSchema : 'x';\n   deploymentStage : 'x';\n"},
                {"MemSql", "MemSqlFunction", "   functionName : 'x';\n"},
                {"BigQuery", "BigQueryFunction", "   functionName : 'x';\n"},
                {"HostedService", "HostedService", "   pattern : '/x';\n"},
                {"FunctionJar", "FunctionJar", ""},
        };
        for (String[] sectionKind : activators) {
            String kind = sectionKind[1];
            Protocol.PFunctionActivator a = (Protocol.PFunctionActivator) sectionElement("###" + sectionKind[0]
                    + "\n'''\n  A documented " + kind + ".\n'''\n" + kind + " a::A\n{\n" + sectionKind[2] + common);
            assertEquals(kind, a.kind());
            assertDocumented("  A documented " + kind + ".", a.taggedValues(), a.sourceInformation());
        }
    }

    @Test
    @DisplayName("a block before a deployment configuration, an ExecutionEnvironment or a DeephavenApp is refused:"
            + " the engine's rules there have no documentation")
    void refusedWhereTheGrammarHasNoSlot() {
        String doc = "'''\nA documented element.\n'''\n";
        String env = " a::Env\n{\n   executions:\n   [\n      UAT:\n      {\n         mapping: a::M;\n"
                + "         runtime: a::R;\n      }\n   ];\n}\n";
        String[][] cases = {
                {"###Service\n", "ExecutionEnvironment", env},
                {"###HostedService\n", "ExecutionEnvironment", env},
                {"###Snowflake\n", "SnowflakeAppDeploymentConfiguration", " a::C\n{\n   activationConnection : a::Conn;\n}\n"},
                {"###MemSql\n", "MemSqlFunctionDeploymentConfiguration", " a::C\n{\n   activationConnection : a::Conn;\n}\n"},
                {"###BigQuery\n", "BigQueryFunctionDeploymentConfiguration",
                        " a::C\n{\n   activationConnection : a::Conn;\n}\n"},
                {"###HostedService\n", "HostedServiceDeploymentConfiguration", " a::C\n{\n}\n"},
        };
        for (String[] c : cases) {
            // without the block the element parses: the refusal is the block's
            sectionElementOrNone(c[0] + c[1] + c[2]);
            ParseException e = assertThrows(ParseException.class,
                    () -> PmcdParser.parseModel(c[0] + doc + c[1] + c[2]), c[1]);
            assertEquals("[5:1] Unexpected token '" + c[1] + "'", e.getMessage());
        }
        String app = "DeephavenApp a::App\n{\n   applicationName: 'x';\n   function: a::f():Integer[1];\n"
                + "   ownership: Deployment { identifier: '1' };\n}\n";
        sectionElement("###Deephaven\n" + app);
        assertThrows(ParseException.class, () -> PmcdParser.parseModel("###Deephaven\n" + doc + app));
    }

    /** The document parses; a deployment configuration may leave no element (the engine's walkers drop some). */
    private static void sectionElementOrNone(String src) {
        PmcdParser.parseModel(src);
    }
}
