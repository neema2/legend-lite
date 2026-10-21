// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.Compiler;
import com.legend.error.ModelException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The property-graph profile's reader (docs/CYPHER_STUDY_2026_10_15.md §5.6): the edge classes of a model, in
 *  the association and the compact form, inherited ends, the typed relationship type, and the rule's errors in
 *  phase F. */
class GraphEdgesTest {

    private static final String PROFILE = """
            Profile graph::graph
            {
               stereotypes: [Anonymous, Edge, Open, from, to, type, nanoseconds, timeOfDay];
            }
            """;

    private static final String NODES = """
            Class demo::Actor { name: String[1]; }
            Class demo::Movie { title: String[1]; }
            Enum demo::RelationshipType { ACTED_IN, DIRECTED }
            """;

    @Test
    void theAssociationFormReadsItsEndsAndTheirInverses() {
        ModelContext ctx = Compiler.compileModel(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Acting
                {
                  <<graph::graph.type>> type() { demo::RelationshipType.ACTED_IN }: demo::RelationshipType[1];
                  role: String[1];
                }
                Association demo::ActorActing { <<graph::graph.from>> actor: demo::Actor[1]; actings: demo::Acting[*]; }
                Association demo::ActingMovie { <<graph::graph.to>> movie: demo::Movie[1]; cast: demo::Acting[*]; }
                """);
        GraphEdges edges = GraphEdges.of(ctx);
        assertEquals(List.of(new GraphEdges.Edge("demo::Acting", "ACTED_IN",
                new GraphEdges.End("actor", "demo::Actor", "actings", true),
                new GraphEdges.End("movie", "demo::Movie", "cast", true))), edges.edges());
        assertNull(edges.edgeAt("demo::Actor"));
    }

    @Test
    void theCompactFormHasNoInverseAndTheTypeIsTheSimpleName() {
        ModelContext ctx = Compiler.compileModel(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Directing
                {
                  <<graph::graph.from>> director: demo::Actor[1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                }
                """);
        assertEquals(List.of(new GraphEdges.Edge("demo::Directing", "Directing",
                new GraphEdges.End("director", "demo::Actor", null, false),
                new GraphEdges.End("movie", "demo::Movie", null, false))), GraphEdges.of(ctx).edges());
    }

    @Test
    void theIslandsViewAgreesWithPhaseFOnAToManyPrimitiveProperty() {
        // (the review's first finding: the context's class lookup reaches the native catalog, where String is a
        // class; the island's view must read the model's classes as phase F does)
        ModelContext ctx = Compiler.compileModel(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Acting
                {
                  <<graph::graph.from>> actor: demo::Actor[1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                  roles: String[*];
                  kinds: demo::RelationshipType[*];
                }
                """);
        assertEquals(1, GraphEdges.of(ctx).edges().size());
    }

    @Test
    void anEndTypedByAPrimitiveOrAnEnumerationIsRefused() {
        String primitive = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::E { <<graph::graph.from>> actor: String[1]; <<graph::graph.to>> movie: demo::Movie[1]; }
                """);
        assertTrue(primitive.contains("actor, which is not typed by a class of the model"), primitive);
        String enumeration = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::E { <<graph::graph.from>> actor: demo::Actor[1]; <<graph::graph.to>> movie: demo::RelationshipType[1]; }
                """);
        assertTrue(enumeration.contains("movie, which is not typed by a class of the model"), enumeration);
        String edge = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::E { <<graph::graph.from>> actor: demo::Actor[1]; <<graph::graph.to>> movie: demo::Movie[1]; }
                Class <<graph::graph.Edge>> demo::F { <<graph::graph.from>> e: demo::E[1]; <<graph::graph.to>> movie: demo::Movie[1]; }
                """);
        assertTrue(edge.contains("e, which holds the edge class demo::E"), edge);
    }

    @Test
    void anInheritedAssociationEndHasNoInverseForTheSubclass() {
        ModelContext ctx = Compiler.compileModel(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::L { role: String[1]; }
                Class demo::Sub extends demo::L { credit: String[0..1]; }
                Association demo::ActorL { <<graph::graph.from>> actor: demo::Actor[1]; links: demo::L[*]; }
                Association demo::LMovie { <<graph::graph.to>> movie: demo::Movie[1]; linked: demo::L[*]; }
                """);
        GraphEdges edges = GraphEdges.of(ctx);
        GraphEdges.Edge l = java.util.Objects.requireNonNull(edges.edgeAt("demo::L"));
        GraphEdges.Edge sub = java.util.Objects.requireNonNull(edges.edgeAt("demo::Sub"));
        assertEquals("links", l.from().inverse());
        // (the inverse `links` reaches every L, not only the Subs: none of Sub's)
        assertNull(sub.from().inverse());
        assertNull(sub.to().inverse());
        assertEquals("actor", sub.from().property());
    }

    @Test
    void anEndWithoutTheMarkerIsRefusedWhateverTheClassOrder() {
        // (the review's fourth finding: the association scan must not depend on a marked class sorting first)
        for (String other : List.of("demo::AAA", "demo::ZZZ")) {
            String message = refused(PROFILE + NODES + """
                    Class demo::Acting { role: String[1]; }
                    Association demo::ActorActing { <<graph::graph.from>> actor: demo::Actor[1]; actings: demo::Acting[*]; }
                    Association demo::ActingMovie { <<graph::graph.to>> movie: demo::Movie[1]; cast: demo::Acting[*]; }
                    """ + "Class <<graph::graph.Edge>> " + other + " { <<graph::graph.from>> a: demo::Actor[1];"
                    + " <<graph::graph.to>> m: demo::Movie[1]; }\n");
            assertTrue(message.contains("demo::Acting carries <<graph::graph.from>> on actor"), message);
        }
    }

    @Test
    void aThirdLinkOnAnEdgeIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Acting
                {
                  <<graph::graph.from>> actor: demo::Actor[1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                }
                Association demo::Third { producer: demo::Actor[1]; produced: demo::Acting[*]; }
                """);
        assertTrue(message.contains("demo::Acting is linked by demo::Third's end producer, carrying neither"), message);
    }

    @Test
    void aTypeMarkerWithoutTheEdgeMarkerIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class demo::Plain { <<graph::graph.type>> type() { demo::RelationshipType.ACTED_IN }: demo::RelationshipType[1]; }
                """);
        assertTrue(message.contains("demo::Plain carries <<graph::graph.type>> on type but not"), message);
    }

    @Test
    void twoInheritedTypesThatDisagreeAreRefused() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::L
                {
                  <<graph::graph.from>> a: demo::Actor[1];
                  <<graph::graph.to>> m: demo::Movie[1];
                  <<graph::graph.type>> type() { demo::RelationshipType.ACTED_IN }: demo::RelationshipType[1];
                }
                Class demo::R { <<graph::graph.type>> kind() { demo::RelationshipType.DIRECTED }: demo::RelationshipType[1]; }
                Class demo::Both extends demo::L, demo::R {}
                """);
        assertTrue(message.contains("inherits two relationship types") || message.contains("demo::R carries"),
                message);
    }

    @Test
    void aDerivedEndIsRefusedByName() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Acting
                {
                  <<graph::graph.from>> from() { $this.actor }: demo::Actor[1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                  actor: demo::Actor[1];
                }
                """);
        assertTrue(message.contains("a derived end"), message);
    }

    @Test
    void anInheritedFaultNamesTheDeclaringClass() {
        String message = refused(PROFILE + NODES + """
                Class demo::Z { <<graph::graph.from>> x: demo::Actor[1]; }
                Class <<graph::graph.Edge>> demo::A extends demo::Z { <<graph::graph.to>> m: demo::Movie[1]; x: demo::Actor[0..1]; }
                """);
        // (A redeclares x without the marker, so A has no from end: A's fault; Z's own end without the marker is
        // Z's fault, reported first under Z's name)
        assertTrue(message.contains("demo::Z carries <<graph::graph.from>> on x but not"), message);
    }

    @Test
    void aSubclassInheritsTheMarkerTheEndsAndTheType() {
        ModelContext ctx = Compiler.compileModel(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Link
                {
                  <<graph::graph.from>> a: demo::Actor[1];
                  <<graph::graph.to>> m: demo::Movie[1];
                  <<graph::graph.type>> type() { demo::RelationshipType.DIRECTED }: demo::RelationshipType[1];
                }
                Class demo::Credited extends demo::Link { credit: String[0..1]; }
                """);
        GraphEdges edges = GraphEdges.of(ctx);
        assertEquals(2, edges.edges().size());
        GraphEdges.Edge sub = edges.edgeAt("demo::Credited");
        assertEquals("DIRECTED", java.util.Objects.requireNonNull(sub).type());
        assertEquals("a", sub.from().property());
    }

    @Test
    void anEndWithoutTheMarkerIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class demo::Plain { <<graph::graph.from>> actor: demo::Actor[1]; }
                """);
        assertTrue(message.contains("demo::Plain carries <<graph::graph.from>> on actor but not"), message);
    }

    @Test
    void aMarkerWithoutBothEndsIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Half { <<graph::graph.from>> actor: demo::Actor[1]; }
                """);
        assertTrue(message.contains("demo::Half has no property carrying <<graph::graph.to>>"), message);
    }

    @Test
    void anOptionalEndIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Loose
                {
                  <<graph::graph.from>> actor: demo::Actor[0..1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                }
                """);
        assertTrue(message.contains("actor, which is not [1]"), message);
    }

    @Test
    void aTypeThatIsNotOneEnumerationValueIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Acting
                {
                  <<graph::graph.from>> actor: demo::Actor[1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                  <<graph::graph.type>> type() { 'ACTED_IN' }: String[1];
                }
                """);
        assertTrue(message.contains("whose body is not one spelled enumeration value"), message);
    }

    @Test
    void aToManyClassPropertyOnAnEdgeIsRefused() {
        String message = refused(PROFILE + NODES + """
                Class <<graph::graph.Edge>> demo::Acting
                {
                  <<graph::graph.from>> actor: demo::Actor[1];
                  <<graph::graph.to>> movie: demo::Movie[1];
                  others: demo::Actor[*];
                }
                """);
        assertTrue(message.contains("holds many demo::Actor in others"), message);
    }

    @Test
    void theProfileMustBeDeclared() {
        String message = refused(NODES + """
                Class <<graph.Edge>> demo::Acting
                {
                  <<graph.from>> actor: demo::Actor[1];
                  <<graph.to>> movie: demo::Movie[1];
                }
                """);
        // the integrity pass refuses a stereotype of an undeclared profile before any edge is read (2026-10-21),
        // in legend-engine's words; the bare name resolves to no profile, so it stays as written
        assertTrue(message.contains("Can't find the profile 'graph'"), message);
    }

    @Test
    void aModelWithoutEdgesReadsNone() {
        assertEquals(List.of(), GraphEdges.of(Compiler.compileModel(NODES)).edges());
    }

    private static String refused(String model) {
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(model));
        return String.valueOf(e.getMessage());
    }
}
