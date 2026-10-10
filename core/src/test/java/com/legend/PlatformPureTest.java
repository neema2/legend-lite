// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.error.ModelException;
import com.legend.model.FunctionId;
import com.legend.platform.Implementation;
import com.legend.platform.PlatformPure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The platform's own Pure (build rebuild Phase 3b, item 1b; {@link PlatformPure}): a graph function with a boot
 * version's id keeps its declaration and takes the platform's body; another version under the name is its own
 * function, refused until it has a row; the platform's version and the declaration must name their parameters
 * alike. {@code extractDBs(Mapping)} is a boot version no prelude module declares, so a graph may declare it.
 */
class PlatformPureTest {

    private static final String NAME = "meta::relational::runtime::extractDBs";
    private static final FunctionId ID = new FunctionId(NAME + "_Mapping_1__Database_MANY_");
    private static final String MODEL = "Class m::X { k: String[1]; }\n";

    @Test
    @DisplayName("a graph twin by id keeps its declaration (its stereotype) and takes the platform's body")
    void aTwinKeepsItsDeclarationAndTakesThePlatformBody() {
        ModelContext plain = Compiler.compileModel(MODEL);
        ModelContext withTwin = Compiler.compileModel(MODEL
                + "function <<test.Test>> " + NAME + "(m: meta::pure::mapping::Mapping[1]):"
                + " meta::relational::metamodel::Database[*] { [] }\n");
        List<TypedFunction> own = withTwin.findFunction(NAME);
        assertEquals(1, own.size(), "one function at the id, never a duplicate");
        TypedFunction f = own.get(0);
        assertEquals(ID, f.id());
        assertTrue(f.definition() instanceof com.legend.model.FunctionDefinition fd && !fd.stereotypes().isEmpty(),
                "the graph's declaration, with its stereotype");
        assertEquals(plain.findFunction(NAME).get(0).body(), f.body(), "the platform's body, not the graph's []");
        assertInstanceOf(Implementation.PlatformPure.class, withTwin.implementations().of(ID));
        assertTrue(PlatformPure.ids().contains(ID));
    }

    @Test
    @DisplayName("another version under a platform name is its own function and has no row: refused, naming it")
    void anotherVersionWithoutARowIsRefused() {
        ModelContext ctx = Compiler.compileModel(MODEL
                + "function " + NAME + "(m: meta::pure::mapping::Mapping[1], flag: Boolean[1]):"
                + " meta::relational::metamodel::Database[*] { [] }\n");
        assertEquals(2, ctx.findFunction(NAME).size(), "the platform's version and the graph's own");
        Implementation row = ctx.implementations().of(new FunctionId(NAME + "_Mapping_1__Boolean_1__Database_MANY_"));
        assertInstanceOf(Implementation.Refused.class, row);
        assertEquals(Implementation.Reason.NO_ROW, ((Implementation.Refused) row).reason());
    }

    @Test
    @DisplayName("the declaration must name its parameters as the platform's version does")
    void parameterNamesMustAgree() {
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(MODEL
                + "function " + NAME + "(mapping: meta::pure::mapping::Mapping[1]):"
                + " meta::relational::metamodel::Database[*] { [] }\n"));
        assertEquals(ID.qualified(), e.element());
        assertTrue(e.getMessage().contains("'m'") && e.getMessage().contains("'mapping'"), e.getMessage());
        // tolerant: walled under the id, the graph still builds
        Compiler.BuiltModule module = Compiler.buildModule(Compiler.parseSources(List.of(new Compiler.ModelSource(
                "m.pure", MODEL + "function " + NAME + "(mapping: meta::pure::mapping::Mapping[1]):"
                        + " meta::relational::metamodel::Database[*] { [] }\n"))).model());
        assertTrue(module.walls().containsKey(ID.qualified()), module.walls().toString());
    }

    @Test
    @DisplayName("every decided version names an id the platform's own names cover, and none is also the platform's")
    void theDecisionsAreAtThePlatformsOwnNames() {
        java.util.Set<String> names = new java.util.HashSet<>();
        for (FunctionId id : PlatformPure.ids()) {
            names.add(id.qualified().substring(0, id.qualified().indexOf('_', id.qualified().lastIndexOf("::"))));
        }
        for (FunctionId id : PlatformPure.upstreamBodies().keySet()) {
            assertTrue(!PlatformPure.ids().contains(id), id + " is the platform's own");
            assertTrue(names.contains(nameOf(id)), id + " is not at a platform name");
        }
        for (FunctionId id : PlatformPure.refusedVersions().keySet()) {
            assertTrue(!PlatformPure.ids().contains(id), id + " is the platform's own");
            assertTrue(!PlatformPure.upstreamBodies().containsKey(id), id + " decided twice");
            assertTrue(names.contains(nameOf(id)), id + " is not at a platform name");
        }
    }

    private static String nameOf(FunctionId id) {
        String q = id.qualified();
        return q.substring(0, q.indexOf('_', q.lastIndexOf("::")));
    }
}
