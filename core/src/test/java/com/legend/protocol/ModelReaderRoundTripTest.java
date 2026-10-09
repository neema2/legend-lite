// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol.Element;
import com.legend.protocol.Protocol.PureModelContextData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The MODEL READER is the emitter's inverse on representative models (docs/PROTOCOL_PROGRAM_2026_10_05.md, the
 * read leg): text, parsed to the typed records, emitted to JSON and read back, gives the same RECORDS (source
 * information aside: compared with every span taken out) and the same JSON, byte for byte -- with the spans and
 * without them. The whole corpus is pinned by parser-equivalence's ModelReaderParityTest.
 */
class ModelReaderRoundTripTest {

    static Stream<String> models() {
        return Stream.concat(ModelComposerRoundTripTest.models(), Stream.of(
                // a function with a test suite, a persistence, a data-quality validation and activators
                """
                ###Pure
                Class my::P
                {
                  name: String[1];
                  age: Integer[0..1];
                }

                function my::double(x: Integer[1]): Integer[1]
                {
                  $x * 2
                }
                {
                  myTest
                  (
                    t1 | double(2) => 4;
                    t2 | double(3) => 6;
                  )
                }

                ###DataQualityValidation
                DataQualityValidation my::Check
                {
                  context: fromMappingAndRuntime(my::M, my::R);
                  validationTree: $[
                    my::P<mustBeNamed>{
                      name
                    }
                  ]$;
                  filter: p: my::P[1]|$p.name == 'John';
                }

                ###Snowflake
                SnowflakeApp my::App
                {
                  applicationName: 'app';
                  function: my::double(Integer[1]): Integer[1];
                  ownership: Deployment { identifier: 'owner' };
                  description: 'an app';
                }
                """));
    }

    /** text -> records -> JSON -> records: the same records, spans aside; the same JSON, with and without spans. */
    @ParameterizedTest
    @MethodSource("models")
    void readsBackWhatTheParserProduced(String text) {
        PureModelContextData parsed = PmcdParser.parseModel(text);
        String json = ProtocolEmitter.emit(parsed);
        assertEquals(PmcdParser.parseDocument(text), json, "parseDocument is the parsed records, emitted");

        PureModelContextData read = ModelReader.read(json);
        assertEquals(json, ProtocolEmitter.emit(read), "emit(read(J)) is J");
        assertEquals(parsed.elements().size(), read.elements().size());
        for (int i = 0; i < parsed.elements().size(); i++) {
            Element expected = parsed.elements().get(i);
            assertEquals(withoutSpans(expected), withoutSpans(read.elements().get(i)),
                    "read back as parsed: " + expected.getClass().getSimpleName());
        }

        String stripped = SourceInformation.stripAll(json);
        PureModelContextData spanless = ModelReader.read(stripped);
        assertEquals(stripped, SourceInformation.stripAll(ProtocolEmitter.emit(spanless)),
                "without source information too");
        for (int i = 0; i < parsed.elements().size(); i++) {
            assertEquals(withoutSpans(parsed.elements().get(i)), spanless.elements().get(i),
                    "spanless JSON reads as the parsed records, spans aside");
        }
    }

    /** A field no rule takes, and a _type no rule knows, are refused by name -- never dropped. */
    @Test
    void refusesWhatItCannotCarry_namingIt() {
        String cls = "{\"_type\":\"class\",\"constraints\":[],\"name\":\"C\",\"originalMilestonedProperties\":[],"
                + "\"package\":\"p\",\"properties\":[],\"qualifiedProperties\":[],\"stereotypes\":[],"
                + "\"superTypes\":[],\"taggedValues\":[]";
        assertEquals(cls + "}", ProtocolEmitter.emitElement(ModelReader.readElement(cls + "}")));
        IllegalArgumentException extra = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.readElement(cls + ",\"somethingNew\":1}"));
        assertTrue(extra.getMessage().contains("somethingNew"), extra.getMessage());
        IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.read("{\"_type\":\"data\",\"elements\":[{\"_type\":\"aNewElement\"}]}"));
        assertTrue(unknown.getMessage().contains("aNewElement"), unknown.getMessage());
        // a list the emitter writes empty and no record carries: refused, naming it
        String fn = "{\"_type\":\"function\",\"body\":[],\"name\":\"f__String_1_\",\"package\":\"p\",\"parameters\":[],"
                + "\"postConstraints\":[],\"preConstraints\":[{}],\"returnGenericType\":{\"multiplicityArguments\":[],"
                + "\"rawType\":{\"_type\":\"packageableType\",\"fullPath\":\"String\"},\"typeArguments\":[],"
                + "\"typeVariableValues\":[]},\"returnMultiplicity\":{\"lowerBound\":1,\"upperBound\":1},"
                + "\"stereotypes\":[],\"taggedValues\":[],\"tests\":[]}";
        IllegalArgumentException constant = assertThrows(IllegalArgumentException.class,
                () -> ModelReader.readElement(fn));
        assertTrue(constant.getMessage().contains("preConstraints"), constant.getMessage());
        // older JSON's milestoned properties written out: kept, and written back (leg 2 step 2)
        String milestoned = cls.replace("\"originalMilestonedProperties\":[]", "\"originalMilestonedProperties\":[{"
                + "\"genericType\":{\"multiplicityArguments\":[],\"rawType\":{\"_type\":\"packageableType\","
                + "\"fullPath\":\"String\"},\"typeArguments\":[],\"typeVariableValues\":[]},\"multiplicity\":{"
                + "\"lowerBound\":1,\"upperBound\":1},\"name\":\"n\",\"stereotypes\":[],\"taggedValues\":[]}]") + "}";
        assertEquals(milestoned, ProtocolEmitter.emitElement(ModelReader.readElement(milestoned)));
    }

    /**
     * A path literal's parts carry spans exactly when the literal does (the record keeps a part's position as an
     * offset into the literal): a part's span under a literal without one could not be written back, so it is
     * refused -- never dropped.
     */
    @Test
    void refusesAPathSegmentSpanUnderALiteralWithoutOne() {
        String json = SourceInformation.stripAll(PmcdParser.parseDocument("""
                Class my::P
                {
                  name: String[1];
                }

                function my::f(): Any[*]
                {
                  #/my::P/name#
                }
                """));
        assertEquals(json, ProtocolEmitter.emit(ModelReader.read(json)));
        String mixed = json.replace("{\"_type\":\"propertyPath\",", "{\"_type\":\"propertyPath\",\"sourceInformation\":"
                + "{\"endColumn\":13,\"endLine\":8,\"sourceId\":\"\",\"startColumn\":10,\"startLine\":8},");
        assertTrue(!mixed.equals(json), "the spanless JSON holds a path segment: " + json);
        IllegalArgumentException mix = assertThrows(IllegalArgumentException.class, () -> ModelReader.read(mixed));
        assertTrue(mix.getMessage().contains("path segment with a span in a path literal without one"), mix.getMessage());
    }

    /**
     * A table reference keeps how it was written, with source positions and without (leg 2's decision, 2026-10-08): the
     * {@code #>{...}#} island and the ordinary {@code tableReference(...)} call are one record, told apart by its written
     * form, not by whether a span is there -- JSON without positions (Depot entities, a browser's save) used to bring
     * the ordinary call back as an island.
     */
    @Test
    void keepsHowATableReferenceWasWritten() {
        String text = """
                function my::islands(): Any[*]
                {
                  [#>{my::DB.S.T}#, #>{my::DB}#]
                }

                function my::calls(): Any[*]
                {
                  [tableReference(my::DB, 'S.T'), tableReference(my::DB)]
                }
                """;
        String json = PmcdParser.parseDocument(text);
        assertEquals(json, ProtocolEmitter.emit(ModelReader.read(json)));
        String spanless = SourceInformation.stripAll(json);
        assertEquals(spanless, ProtocolEmitter.emit(ModelReader.read(spanless)));
        assertEquals(2, spanless.split("\"type\":\">\"", -1).length - 1, "the two islands stay islands: " + spanless);
        assertTrue(spanless.contains("\"function\":\"tableReference\""), "the two calls stay calls: " + spanless);
    }

    /** Numbers stay exact: a decimal keeps its digits as written. */
    @Test
    void keepsExactDecimals() {
        String text = """
                function my::f(): Decimal[1]
                {
                  10.10D->divide(2.1D, 1)
                }
                """;
        String json = PmcdParser.parseDocument(text);
        assertTrue(json.contains("\"value\":10.10"), json);
        assertEquals(json, ProtocolEmitter.emit(ModelReader.read(json)));
    }

    // ---------------------------------------------------------------------
    // Records with every span taken out (test-side reflection over the record components)
    // ---------------------------------------------------------------------

    /** {@code value} with every {@link SourceInfo} replaced by {@code null}, rebuilt through each canonical constructor. */
    static Object withoutSpans(Object value) {
        if (value == null || value instanceof SourceInfo) {
            return null;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object o : list) {
                out.add(withoutSpans(o));
            }
            return out;
        }
        if (value instanceof Map.Entry<?, ?> e) {
            return Map.entry(e.getKey(), withoutSpans(e.getValue()));
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put(k, withoutSpans(v)));
            return map instanceof LinkedHashMap ? out : Map.copyOf(out);
        }
        if (!value.getClass().isRecord()) {
            return value;
        }
        RecordComponent[] components = value.getClass().getRecordComponents();
        Object[] args = new Object[components.length];
        Class<?>[] types = new Class<?>[components.length];
        try {
            for (int i = 0; i < components.length; i++) {
                types[i] = components[i].getType();
                args[i] = withoutSpans(components[i].getAccessor().invoke(value));
            }
            Constructor<?> canonical = value.getClass().getDeclaredConstructor(types);
            canonical.setAccessible(true);
            return canonical.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot rebuild " + value.getClass(), e);
        }
    }
}
