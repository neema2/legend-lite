// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.protocol.Protocol;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Section values legend-engine 4.145.0 keeps AS WRITTEN (Studio's grammar roundtrip, the parser tier, 2026-11-14):
 * an {@code EqualToJson} content pattern's escapes, a FileGeneration config property name, a diagram view id. Gate
 * 8's sibling fixtures adjudicate the same inputs against the engine byte for byte
 * ({@code servicestore-content-pattern-escapes}, {@code filegeneration-dotted-config-keys},
 * {@code diagram-quoted-view-ids}); this is the local statement of the rules.
 */
class SectionValuesAsWrittenTest {

    private static <T> T only(String src, Class<T> type) {
        List<Protocol.Element> elements = PmcdParser.parseModel(src).elements().stream()
                .filter(e -> !(e instanceof Protocol.PSectionIndex)).toList();
        assertEquals(1, elements.size(), src);
        return type.cast(elements.get(0));
    }

    private static Protocol.PServiceStub stub(String request) {
        Protocol.PDataElement d = only("###Data\nData a::D\n{\n  ServiceStore\n  #{\n    [\n      {\n"
                + "        request:\n        {\n" + request + "        };\n"
                + "        response:\n        {\n          body:\n            ExternalFormat\n            #{\n"
                + "              contentType: 'application/json';\n              data: '[]';\n            }#;\n"
                + "        };\n      }\n    ]\n  }#\n}\n", Protocol.PDataElement.class);
        Protocol.PServiceStoreData data = (Protocol.PServiceStoreData) java.util.Objects.requireNonNull(
                d.body().value());
        return data.stubs().get(0);
    }

    private static String pattern(String kind, String expected) {
        return "            " + kind + "\n            #{\n              expected:" + expected + ";\n            }#\n";
    }

    @Test
    @DisplayName("EqualToJson keeps its escapes as written; EqualTo unescapes; a text block is a STRING to both")
    void contentPatternExpected() {
        Protocol.PServiceStub s = stub("          method: POST;\n          url: '/e';\n          bodyPatterns:\n"
                + "          [\n"
                + pattern("EqualToJson", "'{\\\"a\\\": \\\"b \\\\ c\\\", \\\"d\\\": \\\"it\\'s\\\"}'")
                + pattern("EqualTo", "'{\\\"a\\\": \\\"it\\'s\\\"}'")
                + pattern("EqualToJson", "'''\n                {\"a\": \"kept \\\"as\\\" written\\n\"}\n                '''")
                + pattern("EqualTo", "'''\n                a\\tb\n                '''")
                + "          ];\n");
        List<Protocol.PStringValuePattern> ps = java.util.Objects.requireNonNull(s.bodyPatterns());
        assertEquals(new Protocol.PStringValuePattern("equalToJson", "{\\\"a\\\": \\\"b \\\\ c\\\", \\\"d\\\": \\\"it\\'s\\\"}"),
                ps.get(0));
        assertEquals(new Protocol.PStringValuePattern("equalTo", "{\"a\": \"it's\"}"), ps.get(1));
        assertEquals(new Protocol.PStringValuePattern("equalToJson", "{\"a\": \"kept \\\"as\\\" written\\n\"}\n"),
                ps.get(2));
        assertEquals(new Protocol.PStringValuePattern("equalTo", "a\tb\n"), ps.get(3));
        // the parameters' patterns are read by the same reader
        Protocol.PServiceStub q = stub("          method: GET;\n          urlPath: '/e';\n          headerParameters:\n"
                + "          {\n            region:\n" + pattern("EqualToJson", "'{\\\"r\\\": 1}'") + "          };\n");
        assertEquals("{\\\"r\\\": 1}",
                java.util.Objects.requireNonNull(q.headerParams()).get(0).pattern().expectedValue());
        // a one-line ''' literal is no text block: the engine's lexer splits it, and the pattern refuses
        String body = "          method: POST;\n          url: '/e';\n          bodyPatterns:\n          [\n";
        assertThrows(ParseException.class, () -> stub(body + pattern("EqualToJson", "'''{}'''") + "          ];\n"));
        // `expected: STRING;` and nothing after it: the semicolon is required, and a second definition refuses
        assertThrows(ParseException.class, () -> stub(body
                + "            EqualToJson\n            #{\n              expected:'{}'\n            }#\n          ];\n"));
        assertThrows(ParseException.class, () -> stub(body
                + pattern("EqualTo", "'a';\n              expected:'b'") + "          ];\n"));
    }

    @Test
    @DisplayName("a FileGeneration config property name is its segments joined by '.', a quoted one keeping its quotes")
    void fileGenerationConfigPropertyName() {
        Protocol.PFileGeneration g = only("###FileGeneration\nJsonSchema a::G\n{\n  generator.version: 'v1';\n"
                + "  'schema Specification': 'DRAFT_07';\n  a . b.c: 3;\n  outer.'quoted part'.inner: true;\n}\n",
                Protocol.PFileGeneration.class);
        assertEquals(List.of("generator.version", "'schema Specification'", "a.b.c", "outer.'quoted part'.inner"),
                g.configurationProperties().stream().map(Protocol.PConfigProperty::name).toList());
        // a digit-led segment is two tokens to the engine's lexer, so no identifier
        assertThrows(ParseException.class,
                () -> PmcdParser.parseModel("###FileGeneration\nJsonSchema a::G\n{\n  generator.2x: 'v1';\n}\n"));
    }

    @Test
    @DisplayName("a quoted diagram view id keeps its quotes and escapes on the wire; its span covers them")
    void quotedDiagramViewId() {
        Protocol.PDiagram d = only("###Diagram\nDiagram a::D\n{\n  classView 'person view'\n  {\n"
                + "    class: a::P;\n    position: (0.0,0.0);\n    rectangle: (1.0,1.0);\n  }\n"
                + "  classView 'firm\\'s view'\n  {\n    class: a::F;\n    position: (0.0,0.0);\n"
                + "    rectangle: (1.0,1.0);\n  }\n  propertyView\n  {\n    property: a::P.firm;\n"
                + "    source: 'person view';\n    target: 'firm\\'s view';\n    points: [];\n  }\n"
                + "  generalizationView\n  {\n    source: 'firm\\'s view';\n    target: plainView;\n"
                + "    points: [];\n  }\n}\n", Protocol.PDiagram.class);
        assertEquals(List.of("'person view'", "'firm\\'s view'"),
                d.classViews().stream().map(Protocol.PClassView::id).toList());
        Protocol.PPropertyView pv = d.propertyViews().get(0);
        assertEquals("'person view'", pv.sourceView());
        assertEquals("'firm\\'s view'", pv.targetView());
        com.legend.protocol.SourceInfo span = java.util.Objects.requireNonNull(pv.sourceViewSpan());
        // `    source: 'person view';` on line 19: the id is columns 13 to 25, its quotes included
        assertEquals(List.of(19, 13, 19, 25),
                List.of(span.startLine(), span.startColumn(), span.endLine(), span.endColumn()));
        assertEquals("'firm\\'s view'", d.generalizationViews().get(0).sourceView());
        assertEquals("plainView", d.generalizationViews().get(0).targetView());
    }
}
