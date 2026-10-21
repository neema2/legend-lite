// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.Compiler;
import com.legend.error.NotImplementedException;
import com.legend.parser.ParseException;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Relation class mapping's {@code ~func} descriptor spells its types by simple name, as the engine reads it
 * (legend-pure {@code FunctionDescriptor.validateType}: "Package path not allowed in types", "Unsupported type";
 * a type's arguments are not read). Lite resolves the name and drops the signature, so until 2026-10-11 a
 * descriptor the engine refuses compiled here (the DataCube line's find against legend-engine 4.145.0).
 */
class RelationFunctionDescriptorTest {

    private static String model(String signature) {
        return """
                ###Pure
                Class w::Thing { id: Integer[1]; name: String[1]; }
                function w::thingsFn(): meta::pure::metamodel::relation::Relation<Any>[1]
                {
                  #>{w::DB.T}#
                }
                ###Relational
                Database w::DB ( Table T (ID INTEGER, NAME VARCHAR(100)) )
                ###Mapping
                Mapping w::M ( *w::Thing : Relation { ~func w::thingsFn%s id: ID, name: NAME } )
                """.formatted(signature);
    }

    @Test
    @DisplayName("a package path in the descriptor's return type is refused, naming the descriptor and the type")
    void packagePathInTheReturnType() {
        ParseException e = assertThrows(ParseException.class,
                () -> Compiler.compileModel(model("():meta::pure::metamodel::relation::Relation<Any>[1]")));
        assertTrue(e.getMessage().contains(
                "Package path not allowed in types: meta::pure::metamodel::relation::Relation<Any>"), e.getMessage());
        // the text door reports it at the element, naming the descriptor and its own position
        assertTrue(e.getMessage().contains("~func 'w::thingsFn():meta::pure::metamodel::relation::Relation<Any>[1]' at 10:"),
                e.getMessage());
    }

    @Test
    @DisplayName("a package path in a parameter type is refused; the type is named without its multiplicity, as the engine names it")
    void packagePathInAParameterType() {
        ParseException e = assertThrows(ParseException.class,
                () -> Compiler.compileModel(model("(w::Thing[1]):Relation<Any>[1]")));
        assertTrue(e.getMessage().contains("Package path not allowed in types: w::Thing"), e.getMessage());
        assertTrue(e.getMessage().contains("~func 'w::thingsFn(w::Thing[1]):Relation<Any>[1]'"), e.getMessage());
    }

    @Test
    @DisplayName("a function type as a parameter type is the engine's \"Unsupported type\"")
    void functionTypeAsAParameterType() {
        ParseException e = assertThrows(ParseException.class,
                () -> Compiler.compileModel(model("({String[1]->Boolean[1]}[1]):Relation<Any>[1]")));
        assertTrue(e.getMessage().contains("Unsupported type: {String[1]->Boolean[1]}"), e.getMessage());
    }

    @Test
    @DisplayName("the JSON door refuses the same descriptor, as the text door does, at the element its JSON places")
    void jsonDoor() {
        // with its record's position, the text door's error at the same place (2026-10-21, the independent audit:
        // a NotImplementedException without one)
        String text = model("():meta::pure::metamodel::relation::Relation<Any>[1]");
        ParseException fromText = assertThrows(ParseException.class, () -> Compiler.compileModel(text));
        ParseException e = assertThrows(ParseException.class,
                () -> Compiler.compileModel(ModelReader.read(PmcdParser.parseDocument(text))));
        assertTrue(e.getMessage().contains(
                "Package path not allowed in types: meta::pure::metamodel::relation::Relation<Any>"), e.getMessage());
        org.junit.jupiter.api.Assertions.assertEquals(fromText.getMessage(), e.getMessage());
    }

    @Test
    @DisplayName("a JSON-carried descriptor with spaces is read as the engine's lexer reads it, the type named without them")
    void spacesInAJsonDescriptor() {
        RuntimeException e = assertThrows(MappingFromProtocol.UnsupportedMappingShape.class,
                () -> MappingFromProtocol.refuseDescriptorTypes("w::f( ) : meta::x::R< Any >[1]", "( ) : meta::x::R< Any >[1]", null));
        assertTrue(e.getMessage().contains("Package path not allowed in types: meta::x::R<Any>"), e.getMessage());
        assertTrue(e.getMessage().contains("~func 'w::f( ) : meta::x::R< Any >[1]'"), e.getMessage());
    }

    @Test
    @DisplayName("the simple-name form compiles, with or without the signature, and a type ARGUMENT may carry a package")
    void simpleNamesCompile() {
        assertDoesNotThrow(() -> Compiler.compileModel(model("():Relation<Any>[1]")));
        assertDoesNotThrow(() -> Compiler.compileModel(model("")));
        // the engine's id keeps the head name alone (descriptorTypeToId), so a type argument is never validated
        assertDoesNotThrow(() -> Compiler.compileModel(model("():Relation<meta::pure::metamodel::type::Any>[1]")));
        // a unit type and a type variable are simple names too (the engine's validateType reads the unit's name)
        assertDoesNotThrow(() -> Compiler.compileModel(model("(Mass~Kilogram[1]):Relation<Any>[1]")));
        assertDoesNotThrow(() -> Compiler.compileModel(model("(T[1]):Relation<Any>[1]")));
    }
}
