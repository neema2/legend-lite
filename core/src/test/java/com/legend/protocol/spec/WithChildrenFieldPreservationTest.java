// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.spec;

import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;
import com.legend.testing.KnownDefect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A rebuild with the node's own children is the node: every field that is not a child
 * survives {@link ValueSpecification#withChildren}. Checked field by field, because
 * equality excludes positions and these annotation fields ({@code ValueSpecEqualityTest}).
 */
class WithChildrenFieldPreservationTest {

    private static final SourceInfo A = new SourceInfo("q.pure", 1, 3, 1, 9);

    private static ValueSpecification rebuild(ValueSpecification n) {
        return n.withChildren(n.children());
    }

    @Test
    @KnownDefect(owner = "W1.2", reason = "withChildren rebuilds AppliedProperty without pos")
    void appliedPropertyKeepsPosition() {
        var ap = new AppliedProperty(new Variable("p"), "name", A);
        assertEquals(A, ((AppliedProperty) rebuild(ap)).pos());
    }

    @Test
    @KnownDefect(owner = "W1.2", reason = "withChildren rebuilds LambdaFunction without pos;"
            + " the emitter omits sourceInformation for a positionless lambda")
    void lambdaKeepsPosition() {
        var lam = new LambdaFunction(List.of(new Variable("x")), List.of(new CInteger(1L)), A);
        assertEquals(A, ((LambdaFunction) rebuild(lam)).pos());
    }

    @Test
    @KnownDefect(owner = "W1.2", reason = "withChildren rebuilds ColSpecArray without pos")
    void colSpecArrayKeepsPosition() {
        var arr = new ColSpecArray(List.of(new ColSpec("a")), A);
        assertEquals(A, ((ColSpecArray) rebuild(arr)).pos());
    }

    @Test
    void colSpecKeepsAnnotations() {
        // fixed 2026-11-16 (was a W1.2 known defect; the Studio roundtrip's post-pin constructs, S421): withChildren
        // rebuilt a ColSpec through the annotation-free constructor, dropping its stereotypes and tagged values
        var st = List.of(new Protocol.PStereotype("doc", "deprecated", A, A));
        var tv = List.of(new Protocol.PTaggedValue(new Protocol.PTag("doc", "doc", A, A),
                "text", false, A));
        var cs = new ColSpec("a",
                new LambdaFunction(List.of(new Variable("x")), List.of(new CInteger(1L))),
                null, null, List.of(), false, A, null, null, st, tv);
        ColSpec r = (ColSpec) rebuild(cs);
        assertEquals(A, r.pos());
        assertEquals(st, r.stereotypes());
        assertEquals(tv, r.taggedValues());
    }

    @Test
    void graphFetchKeepsSubTypeTrees() {
        // fixed 2026-09-30 (was a W1.2 known defect): the tree's children are its call
        // arguments, and withChildren puts them back into the same tree
        var sub = List.of(new GraphFetchLiteral.SubTypeNode("m::Emp", A, List.of(
                new GraphFetchLiteral.Node("id", A, List.of(new CInteger(1L)), null, null, List.of()))));
        var gf = new GraphFetchLiteral("m::Person", List.of(), sub, A);
        GraphFetchLiteral r = (GraphFetchLiteral) rebuild(gf);
        assertEquals(A, r.pos());
        assertEquals(sub, r.subTypeTrees());
        assertEquals(A, r.subTypeTrees().get(0).subTrees().get(0).pos());
    }
}
