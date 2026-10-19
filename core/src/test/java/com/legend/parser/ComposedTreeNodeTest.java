// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.ValueSpecification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The composed body on a tree node (lite, GraphQL PR2c, docs/GRAPHQL_STUDY_2026_10_18.md §6.2): {@code 'top3':
 * $this.employees->filter(...)->sortByReversed(...)->limit(3) { fullName }} is ordinary Pure expression syntax in a
 * tree position; the parser reads it, the composer prints it, and the two agree.
 */
class ComposedTreeNodeTest {

    private static GraphFetchLiteral tree(String text) {
        ValueSpecification vs = SpecParser.parse(text, Dialect.LEGEND_LITE);
        assertTrue(vs instanceof AppliedFunction, vs.toString());
        ValueSpecification arg = ((AppliedFunction) vs).parameters().get(1);
        assertTrue(arg instanceof GraphFetchLiteral, arg.toString());
        return (GraphFetchLiteral) arg;
    }

    @Test
    void theNodeKeepsItsBodyAndItsHead() {
        GraphFetchLiteral t = tree("demo::Firm.all()->graphFetch(#{ demo::Firm { legalName,"
                + " 'top3': $this.employees->filter(e|$e.age > 30)->sortByReversed(e|$e.salary)->limit(3) { fullName } } }#)");
        assertEquals(2, t.subTrees().size());
        GraphFetchLiteral.Node top3 = t.subTrees().get(1);
        assertEquals("top3", top3.alias());
        assertEquals("employees", top3.property(), "the body's navigation head");
        assertNotNull(top3.body());
        assertTrue(top3.body() instanceof AppliedFunction, top3.body().toString());
        assertEquals(1, top3.subTrees().size());
        assertEquals("fullName", top3.subTrees().get(0).property());
        // the body is among the tree's arguments, so every generic walk reaches it
        assertEquals(1, t.arguments().size());
    }

    @Test
    void theComposerPrintsWhatTheParserReads() {
        String text = "demo::Firm.all()->graphFetch(#{demo::Firm{legalName,'top3':$this.employees->filter(e|$e.age > 30)->limit(3){fullName}}}#)";
        ValueSpecification once = SpecParser.parse(text, Dialect.LEGEND_LITE);
        String printed = PureComposer.valueSpecification(once, PureComposer.Style.STANDARD);
        assertTrue(printed.contains("'top3':$this.employees->filter("), printed);
        ValueSpecification twice = SpecParser.parse(printed, Dialect.LEGEND_LITE);
        assertEquals(once, twice, printed);
    }

    @Test
    void aBodyNavigatesFromThis() {
        ParseException e = assertThrows(ParseException.class, () -> tree("demo::Firm.all()->graphFetch(#{ demo::Firm {"
                + " 'x': $other.employees { fullName } } }#)"));
        assertTrue(e.getMessage().contains("navigates from $this"), e.getMessage());
    }
}
