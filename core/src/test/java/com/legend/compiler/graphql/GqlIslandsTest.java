// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The island inside Pure (docs/GRAPHQL_STUDY_2026_10_18.md §8.3): a {@code #GQL{...}#} body is validated against
 * the model's schema by the Typer's arm. In this landing a valid island is refused by name with its shape (its
 * execution is PR2's); an invalid one is the compile error its problems spell, every problem listed.
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
    void aValidIslandIsRefusedByNameWithItsShape() {
        Map<String, String> walls = walls("{ Person(orderBy: [{ lastName: ASC }]) { fullName ... on EmployeeInterface { salary } } }");
        String wall = walls.values().stream().findFirst().orElse(null);
        assertNotNull(wall, "the island is walled, not executed: " + walls);
        assertTrue(wall.contains("validates against the model's schema"), wall);
        assertTrue(wall.contains("{ Person: [{ fullName: String!, salary: Float! @EmployeeInterface }] }"), wall);
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
