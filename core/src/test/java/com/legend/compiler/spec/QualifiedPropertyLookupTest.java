// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Build rebuild Phase 3b, item 4: a dot call with arguments is a qualified-property call, looked up by name and
 * arity through the class's generalizations — never hidden by a plain property of the same name
 * ({@code Extension.serializerExtension} and {@code serializerExtension(version)} upstream), and never a function
 * of that name (legend-pure refuses that; PARK-14 decided 2026-10-09). The read without parentheses takes the plain
 * property.
 */
class QualifiedPropertyLookupTest {

    private static final ModelContext CTX = Compiler.compileModel("""
            ###Pure
            Class m::E
            {
              ext: Integer[0..1];
              ext(version: String[1]) { $version + '!' }: String[1];
            }
            Class m::Sub extends m::E {}
            function m::other(e: m::E[1], s: String[1]): String[1] { $s }
            """);

    private static ExprType typeOf(String lambda) {
        return Compiler.query(CTX, lambda).resultType();
    }

    @Test
    @DisplayName("the call with arguments is the qualified property, not the same-named plain property")
    void theCallIsTheQualifiedProperty() {
        ExprType t = typeOf("|m::E.all()->map(e|$e.ext('v'))");
        assertEquals("String", t.type().typeName(), t.toString());
    }

    @Test
    @DisplayName("the read without parentheses is the plain property")
    void theReadIsThePlainProperty() {
        ExprType t = typeOf("|m::E.all()->map(e|$e.ext)");
        assertEquals("Integer", t.type().typeName(), t.toString());
    }

    @Test
    @DisplayName("a qualified property declared on a generalization is found")
    void inheritedQualifiedPropertyIsFound() {
        ExprType t = typeOf("|m::Sub.all()->map(s|$s.ext('v'))");
        assertEquals("String", t.type().typeName(), t.toString());
    }

    @Test
    @DisplayName("a dot call with arguments and no qualified property is refused, as legend-pure refuses it")
    void aDotCallWithNoQualifiedPropertyIsRefused() {
        RuntimeException e = assertThrows(RuntimeException.class, () -> typeOf("|m::E.all()->map(e|$e.other('v'))"));
        assertTrue(e.getMessage().contains("no qualified property 'other' with 1 argument(s) on 'm::E'"), e.getMessage());
        // the function is called with -> (by its full name: the query's scope has no import of m::)
        assertEquals("String", typeOf("|m::E.all()->map(e|$e->m::other('v'))").type().typeName());
    }
}
