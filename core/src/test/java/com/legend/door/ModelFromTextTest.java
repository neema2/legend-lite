// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ParseException;
import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The text's door (leg 8's last step): a text's records -- the one parser's -- made the compiler's model, the one
 * place the parser and the model meet. A shape the model cannot represent is the text's ParseException at the element,
 * from either entry.
 */
class ModelFromTextTest {

    private static final String MODEL = """
            ###Pure
            Class a::P
            {
              name: String[1];
            }
            Enum a::E
            {
              A, B
            }
            """;

    /** An Operation union naming no member set: a record the model cannot represent (MappingFromProtocol refuses
     *  it), its mapping starting at line 3, column 1. */
    private static final String UNREPRESENTABLE = """
            ###Mapping

            Mapping a::M
            (
              *a::P : Operation
              {
                meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_()
              }
            )
            """;

    @Test
    void aTextIsItsRecordsThroughTheDoor() {
        ParsedModel model = ModelFromText.parse(MODEL, Dialect.LEGEND_LITE);
        assertEquals(List.of("a::P", "a::E"), model.elements().stream().map(PackageableElement::qualifiedName).toList());
        // each element alone (the IDE's entry) is the same model element the whole text's door made
        List<Protocol.Element> records = PmcdParser.parse(MODEL, Dialect.LEGEND_LITE).model().elements().stream()
                .filter(e -> !(e instanceof Protocol.PSectionIndex)).toList();
        assertEquals(model.elements(), records.stream().map(ModelFromText::element).toList());
    }

    @Test
    void aShapeTheModelCannotRepresentIsTheTextsErrorAtTheElement() {
        ParseException whole = assertThrows(ParseException.class,
                () -> ModelFromText.parse(UNREPRESENTABLE, Dialect.LEGEND_LITE));
        assertEquals(3, whole.line(), whole.getMessage());
        assertEquals(1, whole.column(), whole.getMessage());
        Protocol.Element mapping = PmcdParser.parse(UNREPRESENTABLE, Dialect.LEGEND_LITE).model().elements().stream()
                .filter(e -> e instanceof Protocol.PMapping).findFirst().orElseThrow();
        ParseException alone = assertThrows(ParseException.class, () -> ModelFromText.element(mapping));
        assertEquals(whole.getMessage(), alone.getMessage());
        assertEquals(3, alone.line());
        // its JSON, which carries the element's position, is refused the same way at the same place (2026-10-21, the
        // independent audit: the JSON door dropped the position and changed the kind)
        com.legend.protocol.Protocol.PureModelContextData read = com.legend.protocol.ModelReader.read(
                com.legend.protocol.ProtocolEmitter.emit(PmcdParser.parse(UNREPRESENTABLE, Dialect.LEGEND_LITE).model()));
        ParseException json = assertThrows(ParseException.class, () -> com.legend.Compiler.compileModel(read));
        assertEquals(whole.getMessage(), json.getMessage());
        assertEquals(3, json.line());
        assertEquals(1, json.column());
    }
}
