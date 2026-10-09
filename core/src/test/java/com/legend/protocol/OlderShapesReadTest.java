// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.parser.SpecParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The OLDER expression shapes legend-engine 4.145.0 still reads (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 2 step 2):
 * each, read and written back, is exactly what the text it means parses and writes to (source positions aside); a
 * shape no Pure text means is refused, naming it. The whole of it against the engine itself, over every engine test
 * file that holds older JSON, is parser-equivalence's OlderJsonParityTest.
 */
class OlderShapesReadTest {

    private static final String VAR_X = "{\"_type\":\"var\",\"name\":\"x\"}";
    private static final String VAR_Y = "{\"_type\":\"var\",\"name\":\"y\"}";
    /** {@code x|$x.a} */
    private static final String X_DOT_A = "{\"_type\":\"lambda\",\"body\":[{\"_type\":\"property\",\"property\":\"a\","
            + "\"parameters\":[" + VAR_X + "]}],\"parameters\":[" + VAR_X + "]}";
    /** {@code y|$y->sum()} */
    private static final String Y_SUM = "{\"_type\":\"lambda\",\"body\":[{\"_type\":\"func\",\"function\":\"sum\","
            + "\"parameters\":[" + VAR_Y + "]}],\"parameters\":[" + VAR_Y + "]}";

    private static String lambda(String body) {
        return "{\"_type\":\"lambda\",\"body\":[" + body + "],\"parameters\":[]}";
    }

    /** What lite writes for {@code json}, read, without source positions. */
    private static String readAndWritten(String json) {
        return compact(SourceInformation.stripAll(ProtocolEmitter.emitLambda(ProtocolReader.lambda(json))));
    }

    /** What lite writes for the text, parsed, without source positions. */
    private static String parsedAndWritten(String text) {
        return compact(SourceInformation.stripAll(ProtocolEmitter.emitLambda(SpecParser.parseLambda(text))));
    }

    private static String compact(String json) {
        return Json.toCompact(Json.parse(json));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', quoteCharacter = '`', value = {
        // the pointers the engine's reader turns into an element pointer
        "class | {\"_type\":\"class\",\"fullPath\":\"my::P\"} | `|my::P`",
        "enum | {\"_type\":\"enum\",\"fullPath\":\"my::E\"} | `|my::E`",
        "mappingInstance | {\"_type\":\"mappingInstance\",\"fullPath\":\"my::M\"} | `|my::M`",
        "primitiveType by name | {\"_type\":\"primitiveType\",\"name\":\"String\"} | `|String`",
        "primitiveType by path | {\"_type\":\"primitiveType\",\"fullPath\":\"String\"} | `|String`",
        "unitType by unitType | {\"_type\":\"unitType\",\"unitType\":\"my::Mass~Kg\"} | `|my::Mass~Kg`",
        // a single value's multiplicity, [1]: what the value already is
        "pointer with [1] | {\"_type\":\"class\",\"fullPath\":\"my::P\",\"multiplicity\":{\"lowerBound\":1,\"upperBound\":1}}"
                + " | `|my::P`",
        // the type annotations
        "hackedClass | {\"_type\":\"func\",\"function\":\"cast\",\"parameters\":[{\"_type\":\"string\",\"value\":\"a\"},"
                + "{\"_type\":\"hackedClass\",\"fullPath\":\"String\"}]} | `|'a'->cast(@String)`",
        "genericTypeInstance by path | {\"_type\":\"func\",\"function\":\"cast\",\"parameters\":[{\"_type\":\"string\","
                + "\"value\":\"a\"},{\"_type\":\"genericTypeInstance\",\"fullPath\":\"String\"}]} | `|'a'->cast(@String)`",
        "hackedUnit | {\"_type\":\"func\",\"function\":\"cast\",\"parameters\":[{\"_type\":\"string\",\"value\":\"a\"},"
                + "{\"_type\":\"hackedUnit\",\"unitType\":\"my::Mass~Kg\"}]} | `|'a'->cast(@my::Mass~Kg)`",
        // literals written as a values list
        "one value | {\"_type\":\"string\",\"values\":[\"a\"],\"multiplicity\":{\"lowerBound\":1,\"upperBound\":1}} | `|'a'`",
        "two values | {\"_type\":\"integer\",\"values\":[1,2],\"multiplicity\":{\"lowerBound\":2,\"upperBound\":2}} | `|[1, 2]`",
        "no value | {\"_type\":\"boolean\",\"values\":[]} | `|[]`",
        "the empty-set fix | {\"_type\":\"string\",\"values\":[],\"multiplicity\":{\"lowerBound\":0,\"upperBound\":1}} | `|''`",
        // a qualified property is a property access with arguments
        "qualifiedProperty | {\"_type\":\"qualifiedProperty\",\"qualifiedProperty\":\"orgs\","
                + "\"parameters\":[{\"_type\":\"packageableElementPtr\",\"fullPath\":\"my::P\"},{\"_type\":\"string\","
                + "\"value\":\"a\"}]} | `|my::P.orgs('a')`",
        // classInstance kinds under their own _type, and as classInstance
        "path | {\"_type\":\"path\",\"name\":\"\",\"startType\":\"my::P\",\"path\":[{\"_type\":\"propertyPath\","
                + "\"property\":\"lastName\"}]} | `|#/my::P/lastName#`",
        "rootGraphFetchTree | {\"_type\":\"rootGraphFetchTree\",\"class\":\"my::P\",\"subTrees\":[{\"_type\":"
                + "\"propertyGraphFetchTree\",\"property\":\"name\"}]} | `|#{my::P {name}}#`",
        "listInstance | {\"_type\":\"listInstance\",\"values\":[{\"_type\":\"string\",\"value\":\"a\"},"
                + "{\"_type\":\"string\",\"value\":\"b\"}]} | `|list(['a', 'b'])`",
        "pair | {\"_type\":\"classInstance\",\"type\":\"pair\",\"value\":{\"first\":{\"_type\":\"integer\",\"value\":1},"
                + "\"second\":{\"_type\":\"string\",\"value\":\"a\"}}} | `|meta::pure::functions::collection::pair(1, 'a')`",
        "aggregateValue | {\"_type\":\"aggregateValue\",\"mapFn\":" + X_DOT_A + ",\"aggregateFn\":" + Y_SUM + "}"
                + " | `|meta::pure::functions::collection::agg(x|$x.a, y|$y->sum())`",
        "tdsAggregateValue | {\"_type\":\"classInstance\",\"type\":\"tdsAggregateValue\",\"value\":{\"name\":\"n\","
                + "\"mapFn\":" + X_DOT_A + ",\"aggregateFn\":" + Y_SUM + "}}"
                + " | `|meta::pure::tds::agg('n', x|$x.a, y|$y->sum())`",
        "tdsColumnInformation | {\"_type\":\"tdsColumnInformation\",\"name\":\"n\",\"columnFn\":" + X_DOT_A + "}"
                + " | `|meta::pure::tds::col(x|$x.a, 'n')`",
        "tdsSortInformation | {\"_type\":\"tdsSortInformation\",\"column\":\"c\",\"direction\":\"DESC\"}"
                + " | `|meta::pure::tds::desc('c')`",
        "tdsOlapRank | {\"_type\":\"tdsOlapRank\",\"function\":" + Y_SUM + "} | `|meta::pure::tds::func(y|$y->sum())`",
        "tdsOlapAggregation | {\"_type\":\"tdsOlapAggregation\",\"columnName\":\"c\",\"function\":" + Y_SUM + "}"
                + " | `|meta::pure::tds::func('c', y|$y->sum())`",
        "unitInstance | {\"_type\":\"unitInstance\",\"unitType\":\"my::Mass~Kg\",\"unitValue\":5}"
                + " | `|newUnit(my::Mass~Kg, 5)`",
    })
    void readsAsTheTextItMeans(String shape, String body, String text) {
        assertEquals(parsedAndWritten(text), readAndWritten(lambda(body)), shape);
    }

    /**
     * Fields the engine keeps and never acts on -- a call's {@code fControl}, a property access's {@code class} (a
     * qualified property's too) -- are kept and written back where they came from (the user, 2026-10-08).
     */
    @Test
    void keepsTheWrittenDetailsTheEngineKeeps() {
        String call = "{\"_type\":\"func\",\"fControl\":\"toUpper_String_1__String_1_\",\"function\":\"toUpper\","
                + "\"parameters\":[{\"_type\":\"string\",\"value\":\"a\"}]}";
        assertEquals(compact(lambda(call)), readAndWritten(lambda(call)));
        String access = "{\"_type\":\"property\",\"class\":\"my::P\",\"parameters\":[" + VAR_X + "],\"property\":\"name\"}";
        assertEquals(compact(lambda(access)), readAndWritten(lambda(access)));
        String qualified = "{\"_type\":\"qualifiedProperty\",\"class\":\"my::P\",\"qualifiedProperty\":\"orgs\","
                + "\"parameters\":[" + VAR_X + ",{\"_type\":\"string\",\"value\":\"a\"}]}";
        assertEquals(compact(lambda("{\"_type\":\"property\",\"class\":\"my::P\",\"parameters\":[" + VAR_X
                + ",{\"_type\":\"string\",\"value\":\"a\"}],\"property\":\"orgs\"}")), readAndWritten(lambda(qualified)));
        String newWith = "{\"_type\":\"func\",\"fControl\":\"new_Class_1__String_1__KeyExpression_MANY__T_1_\","
                + "\"function\":\"new\",\"parameters\":[{\"_type\":\"genericTypeInstance\",\"genericType\":{"
                + "\"multiplicityArguments\":[],\"rawType\":{\"_type\":\"packageableType\",\"fullPath\":"
                + "\"meta::pure::metamodel::type::Class\"},\"typeArguments\":[{\"multiplicityArguments\":[],\"rawType\":"
                + "{\"_type\":\"packageableType\",\"fullPath\":\"my::P\"},\"typeArguments\":[],\"typeVariableValues\":[]}],"
                + "\"typeVariableValues\":[]}},{\"_type\":\"string\",\"value\":\"\"},{\"_type\":\"collection\","
                + "\"multiplicity\":{\"lowerBound\":0,\"upperBound\":0},\"values\":[]}]}";
        assertEquals(compact(lambda(newWith)), readAndWritten(lambda(newWith)));
    }

    /** An older variable names its type in {@code class}; an upper bound of 2147483647 is "many". */
    @Test
    void readsAnOlderVariable() {
        String older = "{\"_type\":\"lambda\",\"body\":[" + VAR_X + "],\"parameters\":[{\"_type\":\"var\",\"name\":\"x\","
                + "\"class\":\"String\",\"multiplicity\":{\"lowerBound\":0,\"upperBound\":2147483647}}]}";
        assertEquals(parsedAndWritten("{x: String[*]|$x}"), readAndWritten(older));
    }

    /**
     * A variable REFERENCE with a type and a multiplicity, which only older JSON writes: the engine declares the
     * variable with that type there, so the record keeps it and writes it back as the engine does.
     */
    @Test
    void keepsATypedVariableReference() {
        String ref = "{\"_type\":\"var\",\"name\":\"x\",\"class\":\"String\",\"multiplicity\":{\"lowerBound\":1,"
                + "\"upperBound\":1}}";
        String once = readAndWritten("{\"_type\":\"lambda\",\"body\":[" + ref + "],\"parameters\":[" + VAR_X + "]}");
        assertTrue(once.contains("{\"_type\":\"var\",\"genericType\":{\"multiplicityArguments\":[],\"rawType\":{\"_type\":"
                + "\"packageableType\",\"fullPath\":\"String\"},\"typeArguments\":[],\"typeVariableValues\":[]},"
                + "\"multiplicity\":{\"lowerBound\":1,\"upperBound\":1},\"name\":\"x\"}"), once);
        assertEquals(once, readAndWritten(once), "written back, it reads to itself");
    }

    /** What the engine reads and no record can carry, or no Pure text means: refused, naming it. */
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', quoteCharacter = '`', value = {
        "whatever | {\"_type\":\"whatever\",\"class\":\"my::P\"} | whatever",
        "unknownFunc | {\"_type\":\"unknownFunc\",\"function\":\"f\"} | unknownFunc",
        "runtimeInstance | {\"_type\":\"runtimeInstance\",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"my::R\"}}"
                + " | runtimeInstance",
        "executionContextInstance | {\"_type\":\"classInstance\",\"type\":\"executionContextInstance\","
                + "\"value\":{\"executionContext\":{\"_type\":\"BaseExecutionContext\"}}} | executionContextInstance",
        "a single value that says many | {\"_type\":\"string\",\"value\":\"a\",\"multiplicity\":{\"lowerBound\":0}}"
                + " | multiplicity",
        "both names | {\"_type\":\"primitiveType\",\"name\":\"String\",\"fullPath\":\"Integer\"} | both",
        "a values list the engine drops | {\"_type\":\"strictTime\",\"values\":[\"10:00\"]} | values",
        "value and values | {\"_type\":\"string\",\"value\":\"a\",\"values\":[\"b\"]} | both 'value' and 'values'",
        "a list of the wrong size | {\"_type\":\"integer\",\"values\":[1,2],\"multiplicity\":{\"lowerBound\":1,"
                + "\"upperBound\":1}} | multiplicity",
        "a sort direction | {\"_type\":\"tdsSortInformation\",\"column\":\"c\",\"direction\":\"UP\"} | direction 'UP'",
        "a reference's type alone | {\"_type\":\"var\",\"name\":\"x\",\"class\":\"String\"} | no multiplicity",
        "no kind | {\"_type\":\"pair\",\"somethingElse\":1} | NOT SUPPORTED",
    })
    void refusesNamingIt(String shape, String body, String named) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ProtocolReader.lambda(lambda(body)), shape);
        assertTrue(e.getMessage().contains(named), shape + ": " + e.getMessage());
    }
}
