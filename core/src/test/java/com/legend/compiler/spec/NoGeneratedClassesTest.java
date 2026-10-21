// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.GraphEdges;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.graphql.Scope;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

/**
 * THE MODEL IS THE USER'S (docs/GRAPHQL_STUDY_2026_10_18.md §13.8; register S118): the GraphQL line generates no
 * class. Until 2026-10-21 its companion generated an aggregate class and fold classes per user class, under a reserved
 * package, which every listing reading "every class of the model" had to hide (a label-less Cypher node, the graph's
 * edge classes, the GraphQL schema's scope, the lineage scan's short names): 198 Cypher cases regressed once when a
 * listing forgot. The relation node serves the aggregates, so the generated classes, and the predicate that hid
 * them, are gone. This pins the absence on the compiled model and on each of those listings. The companion's MEMBERS
 * on a hierarchy's root ({@code __typename()}, {@code as<Sub>()}) remain the class's own.
 */
class NoGeneratedClassesTest {

    static final String MODEL = """
            ###Pure
            Class demo::Person { firstName: String[1]; age: Integer[1]; born: Date[0..1]; }
            Class demo::Employee extends demo::Person { salary: Float[1]; }
            Class demo::Firm { legalName: String[1]; }
            Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
            """;

    static final ModelContext CTX = Compiler.compileModel(MODEL);

    /** The package the companion's classes lived under. */
    static final String OLD_PACKAGE = "meta::legend::lite::graphql::companion";

    @Test
    void theCompiledModelHoldsTheUsersClassesOnly() {
        for (String fqn : new TreeSet<>(CTX.elementFqns())) {
            assertFalse(fqn.startsWith(OLD_PACKAGE), "a generated element: " + fqn);
            assertFalse(fqn.endsWith("Aggregate") || fqn.endsWith("Sum"), "a generated element: " + fqn);
        }
        assertFalse(CTX.findProperty("demo::Firm", "employeesAggregate").isPresent(),
                "no aggregate member: the schema's field translates to a relation node");
        assertTrue(CTX.findProperty("demo::Person", "__typename").isPresent(), "the hierarchy root's member");
        assertTrue(CTX.findProperty("demo::Person", "asEmployee").isPresent(), "the hierarchy root's member");
    }

    @Test
    void aLabellessCypherNodeReadsTheUsersClasses() {
        CypherIslands.Labels labels = CypherIslands.Labels.of(CTX);
        assertTrue(labels.bySimpleName().keySet().containsAll(List.of("Employee", "Firm", "Person")),
                labels.bySimpleName().keySet().toString());
        assertFalse(labels.bySimpleName().containsKey("PersonAggregate"));
        assertFalse(labels.bySimpleName().containsKey("PersonSum"));
        for (var e : labels.bySimpleName().entrySet()) {
            for (String fqn : e.getValue()) {
                assertFalse(fqn.startsWith(OLD_PACKAGE), "a label names " + fqn);
            }
        }
    }

    @Test
    void theGraphsEdgeClassesAreTheUsers() {
        List<String> names = GraphEdges.of(CTX).toString().lines().filter(l -> l.contains(OLD_PACKAGE)).toList();
        assertTrue(names.isEmpty(), names.toString());
    }

    @Test
    void theGraphqlScopeIsTheUsersClasses() {
        List<String> all = Scope.all(CTX).asList();
        assertEquals(List.of("demo::Employee", "demo::Firm", "demo::Person"), all);
    }
}
