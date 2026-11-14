// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.Compiler;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RULING 29'S SECOND POLICY in step E (docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 29; the normalizer slice's
 * step 1, 2026-11-14): a raw Java exception a user's model text reached in the normalizer -- an
 * IllegalStateException, a NoSuchElementException, a JDK collection's IllegalArgumentException or
 * NullPointerException -- is a refusal now, its words modelled on the nearest refusal of its kind beside it. Each
 * case is a model text that reached the raw exception before the fix, asked of both public doors: the strict
 * compile ({@link Compiler#compileErrors}, which lists a model's errors and throws a raw exception) and the tolerant
 * module build ({@link Compiler#buildModule}: its walls, and the poison a fetch of the class or association raises).
 * A {@code NotImplementedException} inside a class's synthesis is that class's poison in both modes (a roadmap gap
 * defers, as the unsupported dynafunction beside it does); a {@code ModelException} there is the strict build's
 * error and the module's poison.
 */
class RefusalsNotCrashesTest {

    private static List<String> strict(String text) {
        return Compiler.compileErrors(text);
    }

    private static Compiler.BuiltModule module(String text) {
        return Compiler.buildModule(Compiler.parseSources(List.of(new Compiler.ModelSource("m.pure", text))).model());
    }

    private static String poison(Compiler.BuiltModule module, String mapping, String classFqn) {
        return module.context().mappingPoison(mapping, classFqn).orElseThrow();
    }

    // ---- RelOpTranslator: a lambda argument, and its parameter, in a relational expression ----

    private static final String LAMBDA_ARGUMENT = """
            Class t::Person { name: String[1]; }
            ###Relational
            Database t::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(100)) )
            ###Mapping
            Mapping t::M ( *t::Person: Relational { ~mainTable [t::DB] T name: upper([t::DB] T.NAME, x | $x) } )
            """;

    private static final String LAMBDA_PARAMETER = """
            Class t::Person { name: String[1]; }
            ###Relational
            Database t::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(100)) )
            ###Mapping
            Mapping t::M ( *t::Person: Relational { ~mainTable [t::DB] T name: upper($x) } )
            """;

    private static final String LAMBDA_IN_A_VIEW = """
            Class t::Person { name: String[1]; }
            ###Relational
            Database t::DB (
              Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
              View V (ID: T.ID PRIMARY KEY, NAME: upper(T.NAME, (a, b | $a)))
            )
            ###Mapping
            Mapping t::M ( *t::Person: Relational { ~mainTable [t::DB] V name: [t::DB] V.NAME } )
            """;

    @Test
    @DisplayName("a lambda in a relational expression is refused by name, not an IllegalStateException naming a Java class")
    void aRelationalLambdaIsRefusedByName() {
        String argument = "a lambda argument (x | ...) in a relational expression is not supported by this platform yet";
        assertEquals(List.of(), strict(LAMBDA_ARGUMENT), "a roadmap gap defers: the class is poisoned");
        Compiler.BuiltModule argumentModule = module(LAMBDA_ARGUMENT);
        assertEquals(Map.of(), argumentModule.walls());
        assertEquals(argument, poison(argumentModule, "t::M", "t::Person"));

        String parameter = "a lambda parameter ($x) in a relational expression is not supported by this platform yet";
        assertEquals(List.of(), strict(LAMBDA_PARAMETER));
        assertEquals(parameter, poison(module(LAMBDA_PARAMETER), "t::M", "t::Person"));

        String inView = "a lambda argument (a, b | ...) in a relational expression is not supported by this platform"
                + " yet";
        assertEquals(List.of(inView), strict(LAMBDA_IN_A_VIEW), "the view's lift refuses the model");
        Compiler.BuiltModule viewModule = module(LAMBDA_IN_A_VIEW);
        assertEquals(Map.of("t::DB$view$V", inView), viewModule.walls());
        assertEquals(inView, poison(viewModule, "t::M", "t::Person"));
    }
}
