// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.parser.PmcdParser;
import com.legend.protocol.SourceInformation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link EngineSpans#withoutSpans} (moved from protocol.SourceInformation, J35's residue, 2026-11-18): every span cut from the text, nothing else touched. */
class EngineSpansTest {

    private static final String SPAN = "{\"endColumn\":2,\"endLine\":1,\"sourceId\":\"\",\"startColumn\":1,\"startLine\":1}";

    @Test
    void cutsASpanWhereverItSits_firstMiddleLastOrAlone() {
        assertEquals("{\"a\":1}", EngineSpans.withoutSpans("{\"sourceInformation\":" + SPAN + ",\"a\":1}"));
        assertEquals("{\"a\":1,\"b\":2}", EngineSpans.withoutSpans("{\"a\":1,\"classSourceInformation\":" + SPAN + ",\"b\":2}"));
        assertEquals("{\"a\":1}", EngineSpans.withoutSpans("{\"a\":1,\"profileSourceInformation\":" + SPAN + "}"));
        assertEquals("{}", EngineSpans.withoutSpans("{\"sourceInformation\":" + SPAN + "}"));
        assertEquals("[{\"a\":[{}]}]", EngineSpans.withoutSpans(
                "[{\"a\":[{\"sourceInformation\":" + SPAN + "}],\"sourceInformation\":" + SPAN + "}]"));
    }

    @Test
    void keepsEverythingElseByteForByte_aRepeatedTypeIncluded() {
        // the engine writes some _type keys twice (a graph fetch tree's); a parsed object would keep one
        String json = "{\"_type\":\"rootGraphFetchTree\",\"_type\":\"rootGraphFetchTree\",\"sourceInformation\":" + SPAN
                + ",\"class\":\"a::B\",\"s\":\"a \\\"sourceInformation\\\": [x, {y}]\"}";
        assertEquals("{\"_type\":\"rootGraphFetchTree\",\"_type\":\"rootGraphFetchTree\",\"class\":\"a::B\","
                + "\"s\":\"a \\\"sourceInformation\\\": [x, {y}]\"}", EngineSpans.withoutSpans(json));
    }

    @Test
    void keepsTheSpansTheEngineKeeps_insideATestsParameterValuesAndExpectedValue() {
        // the engine parses these through a span context of its own, which records spans whatever it was asked
        String value = "{\"_type\":\"string\",\"sourceInformation\":" + SPAN + ",\"value\":\"x\"}";
        String functionTest = "{\"_type\":\"functionTest\",\"assertions\":[{\"_type\":\"equalTo\",\"expected\":" + value
                + ",\"id\":\"a\",\"sourceInformation\":" + SPAN + "}],\"id\":\"t\",\"parameters\":[{\"name\":\"p\",\"value\":"
                + value + "}],\"sourceInformation\":" + SPAN + "}";
        assertEquals("{\"_type\":\"functionTest\",\"assertions\":[{\"_type\":\"equalTo\",\"expected\":" + value
                + ",\"id\":\"a\"}],\"id\":\"t\",\"parameters\":[{\"name\":\"p\",\"value\":" + value + "}]}",
                EngineSpans.withoutSpans(functionTest));
        String serviceTest = "{\"_type\":\"serviceTest\",\"parameters\":[{\"name\":\"p\",\"value\":" + value + "}]}";
        assertEquals(serviceTest, EngineSpans.withoutSpans(serviceTest));
        String context = "{\"_type\":\"persistenceContext\",\"serviceParameters\":[{\"name\":\"p\",\"sourceInformation\":"
                + SPAN + ",\"value\":{\"_type\":\"primitiveTypeValue\",\"primitiveType\":" + value + "}}]}";
        assertEquals("{\"_type\":\"persistenceContext\",\"serviceParameters\":[{\"name\":\"p\",\"value\":"
                + "{\"_type\":\"primitiveTypeValue\",\"primitiveType\":" + value + "}}]}", EngineSpans.withoutSpans(context));
        // every EqualTo's expected value: a mapping test suite's goes through the same EqualTo parser
        String mappingTest = "{\"_type\":\"mappingTest\",\"assertions\":[{\"_type\":\"equalTo\",\"expected\":" + value + "}]}";
        assertEquals(mappingTest, EngineSpans.withoutSpans(mappingTest));
        // a legacy service test's parameter values; of a list, the values and not the list around them
        String legacy = "{\"asserts\":[{\"parametersValues\":[" + value + ",{\"_type\":\"classInstance\",\"sourceInformation\":"
                + SPAN + ",\"type\":\"listInstance\",\"value\":{\"sourceInformation\":" + SPAN + ",\"values\":[" + value + "]}}]}]}";
        assertEquals("{\"asserts\":[{\"parametersValues\":[" + value + ",{\"_type\":\"classInstance\",\"type\":\"listInstance\","
                + "\"value\":{\"values\":[" + value + "]}}]}]}", EngineSpans.withoutSpans(legacy));
    }

    @Test
    void cutsTheSameShapesElsewhere() {
        String value = "{\"_type\":\"string\",\"sourceInformation\":" + SPAN + ",\"value\":\"x\"}";
        String lambda = "{\"_type\":\"lambda\",\"body\":[" + value + "],\"parameters\":[{\"_type\":\"var\",\"name\":\"p\","
                + "\"sourceInformation\":" + SPAN + ",\"value\":" + value + "}]}";
        assertEquals("{\"_type\":\"lambda\",\"body\":[{\"_type\":\"string\",\"value\":\"x\"}],\"parameters\":[{\"_type\":\"var\","
                + "\"name\":\"p\",\"value\":{\"_type\":\"string\",\"value\":\"x\"}}]}", EngineSpans.withoutSpans(lambda));
        String classInstance = "{\"_type\":\"classInstance\",\"sourceInformation\":" + SPAN + ",\"type\":\"listInstance\","
                + "\"value\":{\"values\":[" + value + "]}}";
        assertEquals("{\"_type\":\"classInstance\",\"type\":\"listInstance\",\"value\":{\"values\":"
                + "[{\"_type\":\"string\",\"value\":\"x\"}]}}", EngineSpans.withoutSpans(classInstance));
    }

    @Test
    void agreesWithTheParsedStripper_whereNoKeyRepeats() {
        String json = PmcdParser.parseDocument("""
                Class <<p::P.s>> a::B extends a::C
                [
                  positive: $this.x > 0
                ]
                {
                  {p::P.t = 'doc'} x: Integer[1];
                  y() {$this.x + 1}: Integer[1];
                }
                """);
        assertEquals(SourceInformation.stripAll(json), EngineSpans.withoutSpans(json));
    }
}
