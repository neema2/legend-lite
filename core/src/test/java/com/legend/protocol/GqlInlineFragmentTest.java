// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.parser.GqlParser;
import com.legend.parser.ParseException;
import com.legend.parser.SpecParser;
import com.legend.protocol.spec.Gql;
import com.legend.protocol.spec.GqlIsland;
import com.legend.protocol.spec.LambdaFunction;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The inline fragment (docs/GRAPHQL_STUDY_2026_10_18.md §6.1), legend-lite's own node in the GraphQL AST: parsed
 * with and without a type condition, carried on the wire as {@code _type: inlineFragment}, read back as itself.
 * legend-engine's grammar refuses the construct; this is the one place its reader goes past the engine's.
 */
class GqlInlineFragmentTest {

    @Test
    void parsesWithAndWithoutATypeCondition() {
        Gql.Document d = GqlParser.parseDocument(
                "{ Person { fullName ... on Employee @include(if: true) { salary } ... { age } } }", 1, 1);
        Gql.Operation op = (Gql.Operation) d.definitions().get(0);
        Gql.Field person = (Gql.Field) op.selectionSet().get(0);
        assertEquals(3, person.selectionSet().size());
        Gql.InlineFragment typed = (Gql.InlineFragment) person.selectionSet().get(1);
        assertEquals("Employee", typed.typeCondition());
        assertEquals(1, typed.directives().size());
        assertEquals(List.of(new Gql.Field(null, "salary", List.of(), List.of(), List.of())), typed.selectionSet());
        Gql.InlineFragment bare = (Gql.InlineFragment) person.selectionSet().get(2);
        assertEquals(null, bare.typeCondition());
        assertEquals("age", ((Gql.Field) bare.selectionSet().get(0)).name());
    }

    @Test
    void aSpreadIsStillASpread() {
        Gql.Document d = GqlParser.parseDocument("{ Person { ...p } } fragment p on PersonInterface { fullName }", 1, 1);
        Gql.Operation op = (Gql.Operation) d.definitions().get(0);
        Gql.Field person = (Gql.Field) op.selectionSet().get(0);
        assertTrue(person.selectionSet().get(0) instanceof Gql.FragmentSpread);
    }

    @Test
    void anInlineFragmentNeedsItsSelection() {
        assertThrows(ParseException.class, () -> GqlParser.parseDocument("{ Person { ... on Employee } }", 1, 1));
    }

    @Test
    void theIslandRoundTripsThroughTheWire() {
        String text = "|#GQL{ { Person { fullName ... on Employee { salary } ... @skip(if: false) { age } } } }#";
        // the product surface (Compiler.parseQuery, LEGEND_LITE) reads lite's node; the drop-in surface refuses it,
        // below
        LambdaFunction parsed = (LambdaFunction) com.legend.Compiler.parseQuery(text);
        String once = ProtocolEmitter.emitLambda(parsed);
        assertTrue(once.contains("\"_type\":\"inlineFragment\""), once);
        assertTrue(once.contains("\"typeCondition\":\"Employee\""), once);
        LambdaFunction read = ProtocolReader.lambda(once);
        assertEquals(once, ProtocolEmitter.emitLambda(read), "emit, read, emit is a fixpoint");
        GqlIsland before = (GqlIsland) parsed.body().get(0);
        GqlIsland after = (GqlIsland) read.body().get(0);
        assertEquals(before.document(), after.document(), "the document read back is the document parsed");
    }

    /** Register S90: the LEGEND_ENGINE surface (the drop-in parser, {@code SpecParser.parseLambda}, PMCD) keeps
     *  legend-engine's refusal of an inline fragment in the island, with or without a type condition, where the
     *  fragment stands; a named fragment spread still parses there. */
    @Test
    void theEngineSurfaceRefusesAnInlineFragment() {
        for (String fragment : List.of("... on Employee { salary }", "...on Employee { salary }",
                "... @skip(if: false) { age }", "... { age }")) {
            ParseException e = assertThrows(ParseException.class,
                    () -> SpecParser.parseLambda("|#GQL{ { Person { fullName " + fragment + " } } }#"), fragment);
            assertTrue(e.getMessage().contains("inline fragments are not supported"), e.getMessage());
        }
        assertTrue(SpecParser.parseLambda("|#GQL{ { Person { ...onboarded } } fragment onboarded on Person"
                + " { fullName } }#").body().get(0) instanceof GqlIsland, "a spread whose name starts with 'on'");
        assertTrue(com.legend.Compiler.parseQuery("|#GQL{ { Person { ... on Employee { salary } } } }#")
                instanceof LambdaFunction, "the product surface reads it");
    }
}
