// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.GraphEdges;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.graphql.Scope;
import com.legend.model.ClassDefinition;
import com.legend.protocol.graphql.Companion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * THE SYNTHETIC RULE (docs/GRAPHQL_STUDY_2026_10_18.md §5.8; register row S111; the fifth landing's full gate):
 * the GraphQL companion's classes are callable from Pure by name and hidden from every listing that reads
 * "every class of the model". This pins the rule on each listing that reads one: {@code ModelContext.isSynthetic}
 * itself, a label-less Cypher node's labels, the untyped relationship's ends, the graph's edge classes, the GraphQL
 * schema's scope. A listing added later reads the same predicate, or a case lands here.
 */
class SyntheticClassesTest {

    static final String MODEL = """
            ###Pure
            Class demo::Person { firstName: String[1]; age: Integer[1]; born: Date[0..1]; }
            Class demo::Employee extends demo::Person { salary: Float[1]; }
            Class demo::Firm { legalName: String[1]; }
            Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
            """;

    static final ModelContext CTX = Compiler.compileModel(MODEL);
    static final String AGGREGATE = Companion.aggregateClass("demo::Person");
    static final String SUM = Companion.foldClass("demo::Person", Companion.Fold.SUM);

    @Test
    void theCompanionsClassesAreSyntheticAndTheUsersAreNot() {
        assertTrue(CTX.findClass(AGGREGATE).isPresent(), "the companion's class exists, callable by name");
        assertTrue(CTX.isSynthetic(AGGREGATE));
        assertTrue(CTX.isSynthetic(SUM));
        assertFalse(CTX.isSynthetic("demo::Person"));
        assertFalse(CTX.isSynthetic("demo::Employment"), "an association is not a class");
        assertFalse(CTX.isSynthetic("demo::Nobody"), "an unknown name is nobody's");
        // the companion's MEMBERS on the user's class are the class's own
        assertTrue(CTX.findProperty("demo::Firm", "employeesAggregate").isPresent());
        assertTrue(CTX.findProperty("demo::Person", "__typename").isPresent());
    }

    @Test
    void aLabellessCypherNodeReadsNoSyntheticClass() {
        CypherIslands.Labels labels = CypherIslands.Labels.of(CTX);
        assertTrue(labels.bySimpleName().containsKey("Person"), labels.bySimpleName().keySet().toString());
        assertTrue(labels.bySimpleName().containsKey("Firm"));
        for (var e : labels.bySimpleName().entrySet()) {
            for (String fqn : e.getValue()) {
                assertFalse(fqn.startsWith(Companion.PACKAGE), "a label names " + fqn);
            }
        }
        assertFalse(labels.bySimpleName().containsKey("PersonAggregate"));
        assertFalse(labels.bySimpleName().containsKey("PersonSum"));
    }

    @Test
    void theGraphsEdgeClassesAreTheUsers() {
        GraphEdges edges = GraphEdges.of(CTX);
        // the companion's classes carry no edge; the reader walks the user's classes only
        List<String> names = edges.toString().lines().filter(l -> l.contains(Companion.PACKAGE)).toList();
        assertTrue(names.isEmpty(), names.toString());
    }

    @Test
    void theGraphqlScopeIsTheUsersClasses() {
        List<String> all = Scope.all(CTX).asList();
        assertTrue(all.contains("demo::Person") && all.contains("demo::Firm") && all.contains("demo::Employee"), all.toString());
        for (String fqn : all) {
            assertFalse(fqn.startsWith(Companion.PACKAGE), fqn);
        }
    }

    @Test
    void theParsedDefinitionCarriesTheMark() {
        ClassDefinition cd = CTX.findClassDefinition(AGGREGATE).orElseThrow();
        assertTrue(cd.stereotypes().stream().anyMatch(st -> st.stereotypeName().equals(
                com.legend.compiler.element.type.PlatformTypes.GRAPHQL_COMPANION)), cd.stereotypes().toString());
    }
}
