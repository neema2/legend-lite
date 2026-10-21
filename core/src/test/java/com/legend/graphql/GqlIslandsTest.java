// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.protocol.graphql.GqlSchema;
import com.legend.compiler.graphql.GqlSchemaException;
import com.legend.compiler.graphql.GqlValidator;
import com.legend.compiler.graphql.SchemaGenerator;
import com.legend.compiler.graphql.Scope;
import com.legend.compiler.graphql.SdlPrinter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The island inside Pure (docs/GRAPHQL_STUDY_2026_10_18.md §8.3): a {@code #GQL{...}#} body is validated against
 * the model's schema by the Typer's arm and typed as the Pure query it means (PR2); an invalid one is the compile
 * error its problems spell, every problem listed; a construct a later landing builds is refused by its name.
 */
class GqlIslandsTest {

    static Map<String, String> walls(String island) {
        ModelContext ctx = Compiler.compileModel(SchemaGeneratorTest.resource("models/demo.pure") + """
                ###Pure
                function demo::q(): String[1]
                {
                  #GQL{ %s }#
                }
                """.formatted(island));
        return Compiler.compileAllBodies(ctx);
    }

    @Test
    void aValidIslandCompilesAsTheQueryItMeans() {
        Map<String, String> walls = walls("{ Person(orderBy: [{ lastName: ASC }], limit: 2) { fullName age } }");
        assertTrue(walls.isEmpty(), "the island is the Pure query it means, typed once: " + walls);
    }

    /** Groups under a parent object (the relation node, register S118) type as the tree they translate to. */
    @Test
    void anIslandWithGroupsUnderAParentCompiles() {
        Map<String, String> walls = walls("{ Firm { employeesGroups(where: { age: { gt: 30 } }, having: { count: { gt: 1"
                + " } }, orderBy: [{ count: DESC }], limit: 2) { key { address { country } } count avg { age } } } }");
        assertTrue(walls.isEmpty(), "the relation node types: " + walls);
    }

    @Test
    void anIslandSelectsOneRoot() {
        Map<String, String> walls = walls("{ Person { fullName } Firm { legalName } }");
        String wall = walls.values().stream().findFirst().orElse(null);
        assertNotNull(wall);
        assertTrue(wall.contains("selects 2 root fields"), wall);
    }

    @Test
    void anInvalidIslandListsEveryProblem() {
        Map<String, String> walls = walls("{ Person(where: { agee: { gt: 1 } }) { fullNam ... on Employee { salary } } }");
        String wall = walls.values().stream().findFirst().orElse(null);
        assertNotNull(wall);
        assertTrue(wall.contains("did you mean age"), wall);
        assertTrue(wall.contains("did you mean fullName"), wall);
        assertTrue(wall.contains("spread on EmployeeInterface"), wall);
    }
}
