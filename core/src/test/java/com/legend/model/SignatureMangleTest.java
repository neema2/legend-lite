// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.Compiler;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A user function's id spells its relation parameters' columns (register S146, the user's 2a, 2026-11-03), so two
 * overloads that differ only in a column's type are two functions; a reference resolves by lite's id, and by
 * legend-pure's own spelling ({@code FunctionDescriptor}: a relation its raw type alone) to exactly the declarations
 * that spelling names.
 */
class SignatureMangleTest {

    private static final List<FunctionDefinition> FUNCTIONS = Compiler.parseModel("""
            ###Pure
            Enum test::All { all }
            function test::pick<K>(x: Relation<(key:K, v:Integer)>[1]): Integer[1] { 1 }
            function test::pick(x: Relation<(key:test::All, v:Integer)>[1]): Integer[1] { 2 }
            function test::one(x: Relation<(key:Integer, v:Integer)>[1]): Integer[1] { 3 }
            """).elements().stream().filter(e -> e instanceof FunctionDefinition).map(e -> (FunctionDefinition) e)
            .toList();

    private static List<FunctionDefinition> resolve(String ref) {
        return SignatureMangle.resolve(ref, base -> FUNCTIONS.stream().filter(f -> f.qualifiedName().equals(base))
                .toList(), f -> f).exact();
    }

    @Test
    void aUserFunctionsIdSpellsItsRelationsColumns() {
        assertEquals(List.of("test::pick_Relation$key~K$v~Integer$_1__Integer_1_",
                "test::pick_Relation$key~All$v~Integer$_1__Integer_1_"),
                FUNCTIONS.stream().filter(f -> f.qualifiedName().equals("test::pick")).map(SignatureMangle::mangle)
                        .toList(), "two overloads, two ids");
    }

    @Test
    void litesIdResolvesExactlyOne() {
        assertEquals(1, resolve("test::pick_Relation$key~All$v~Integer$_1__Integer_1_").size());
    }

    @Test
    void upstreamsSpellingResolvesTheDeclarationsItNames() {
        // legend-pure's id of test::one: its relation's raw type alone -- one declaration, found
        List<FunctionDefinition> one = resolve("test::one_Relation_1__Integer_1_");
        assertEquals(1, one.size(), "upstream's spelling of a function of one overload");
        // the two overloads of test::pick share upstream's spelling: both named, for the caller to refuse
        assertEquals(2, resolve("test::pick_Relation_1__Integer_1_").size(), "upstream's spelling of both overloads");
        assertTrue(resolve("test::one_Relation_1__String_1_").isEmpty(), "a return upstream does not spell: none");
    }
}
