// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.Pure;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.testing.Engine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The declaration table's identity and merge rules. */
class DeclarationTableTest {

    private static List<Function> declare(String source) {
        List<Function> out = new ArrayList<>();
        for (PackageableElement e : Engine.model(source).elements()) {
            if (e instanceof Function f) {
                out.add(f);
            }
        }
        return out;
    }

    /** A catalog native and upstream's bodied declaration of the same id are one
     * declaration; the bodied one (upstream's reference) is kept. */
    @Test
    void oneIdIsOneDeclarationAndTheBodiedOneIsKept() {
        NativeFunctionDefinition upper = Pure.nativeFunctionById(
                "meta::pure::functions::string::toUpper_String_1__String_1_");
        List<Function> fs = new ArrayList<>(List.of(java.util.Objects.requireNonNull(upper)));
        fs.addAll(declare("function meta::pure::functions::string::toUpper(source:String[1]):String[1] { $source }"));
        DeclarationTable t = DeclarationTable.of(fs);
        assertEquals(1, t.size());
        assertInstanceOf(FunctionDefinition.class,
                t.get(new FunctionId("meta::pure::functions::string::toUpper_String_1__String_1_")));
    }

    @Test
    void overloadsAreDistinctDeclarationsAtOneFqn() {
        DeclarationTable t = DeclarationTable.of(declare(
                "function my::pkg::twice(x:Integer[1]):Integer[1] { $x }\n"
                        + "function my::pkg::twice(x:String[1]):String[1] { $x }"));
        assertEquals(2, t.size());
        assertEquals(2, t.at("my::pkg::twice").size());
        assertEquals(0, t.at("my::pkg::thrice").size());
    }

    @Test
    void theSameBodyTwiceIsOneDeclaration() {
        List<Function> fs = new ArrayList<>(declare("function my::pkg::one(x:Integer[1]):Integer[1] { $x }"));
        fs.addAll(declare("function my::pkg::one(x:Integer[1]):Integer[1] { $x }"));
        assertEquals(1, DeclarationTable.of(fs).size());
    }

    @Test
    void twoDifferentBodiesUnderOneIdAreRefused() {
        List<Function> fs = new ArrayList<>(declare("function my::pkg::one(x:Integer[1]):Integer[1] { $x }"));
        fs.addAll(declare("function my::pkg::one(x:Integer[1]):Integer[1] { 1 }"));
        assertThrows(IllegalStateException.class, () -> DeclarationTable.of(fs));
    }
}
