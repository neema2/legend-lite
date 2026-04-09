package com.gs.legend.parser;

import com.gs.legend.ast.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for graphFetch/serialize parsing via g4 sub-parse in ValueSpecificationBuilder.
 *
 * Verifies that #{...}# island content is desugared into ColSpec-based AST
 * with fn1 for property access and fn2 for nested properties.
 */
class GraphFetchParserTest {

    @Test
    void testParseSimpleGraphFetchTree() {
        String query = "Person.all()->graphFetch(#{ Person { firstName, lastName } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        // graphFetch(...) is an AppliedFunction
        assertInstanceOf(AppliedFunction.class, vs);
        AppliedFunction af = (AppliedFunction) vs;
        assertEquals("graphFetch", af.function());

        // Second parameter is ClassInstance wrapping ColSpecArray (desugared)
        ClassInstance ci = (ClassInstance) af.parameters().get(1);
        assertEquals("colSpecArray", ci.type());
        assertInstanceOf(ColSpecArray.class, ci.value());

        ColSpecArray csa = (ColSpecArray) ci.value();
        assertEquals(2, csa.colSpecs().size());
        assertEquals("firstName", csa.colSpecs().get(0).name());
        assertEquals("lastName", csa.colSpecs().get(1).name());
        // Each has fn1 lambda: {x|$x.prop}
        assertNotNull(csa.colSpecs().get(0).function1());
        assertNotNull(csa.colSpecs().get(1).function1());
    }

    @Test
    void testParseGraphFetchWithSerialize() {
        String query = "Person.all()->graphFetch(#{ Person { fullName, upperLastName } }#)->serialize(#{ Person { fullName, upperLastName } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        // serialize is the outermost function
        assertInstanceOf(AppliedFunction.class, vs);
        AppliedFunction serialize = (AppliedFunction) vs;
        assertEquals("serialize", serialize.function());

        // First param of serialize is graphFetch(...)
        AppliedFunction graphFetch = (AppliedFunction) serialize.parameters().get(0);
        assertEquals("graphFetch", graphFetch.function());

        // serialize tree — desugared ColSpecArray
        ClassInstance serializeCi = (ClassInstance) serialize.parameters().get(1);
        ColSpecArray serializeCsa = (ColSpecArray) serializeCi.value();
        assertEquals(2, serializeCsa.colSpecs().size());

        // graphFetch tree — desugared ColSpecArray
        ClassInstance fetchCi = (ClassInstance) graphFetch.parameters().get(1);
        assertEquals("colSpecArray", fetchCi.type());
        ColSpecArray fetchCsa = (ColSpecArray) fetchCi.value();
        assertEquals(2, fetchCsa.colSpecs().size());
    }

    @Test
    void testParseWithFilter() {
        String query = "Person.all()->filter({p | $p.age > 18})->graphFetch(#{ Person { name } }#)->serialize(#{ Person { name } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        assertInstanceOf(AppliedFunction.class, vs);
        AppliedFunction serialize = (AppliedFunction) vs;
        assertEquals("serialize", serialize.function());

        // Verify the desugared ColSpecArray inside serialize
        ClassInstance ci = (ClassInstance) serialize.parameters().get(1);
        ColSpecArray csa = (ColSpecArray) ci.value();
        assertEquals(1, csa.colSpecs().size());
        assertEquals("name", csa.colSpecs().get(0).name());
    }

    @Test
    void testNestedGraphFetchTree() {
        String query = "Person.all()->graphFetch(#{ Person { firstName, addresses { street, city } } }#)";
        ValueSpecification vs = PureParser.parseQuery(query);

        AppliedFunction af = (AppliedFunction) vs;
        ClassInstance ci = (ClassInstance) af.parameters().get(1);
        ColSpecArray csa = (ColSpecArray) ci.value();

        assertEquals(2, csa.colSpecs().size());

        // Simple property: ColSpec("firstName", {x|$x.firstName})
        ColSpec firstNameSpec = csa.colSpecs().get(0);
        assertEquals("firstName", firstNameSpec.name());
        assertNotNull(firstNameSpec.function1());
        // Lambda body is AppliedProperty
        assertInstanceOf(AppliedProperty.class, firstNameSpec.function1().body().get(0));

        // Nested property: ColSpec("addresses", fn1={x|$x.addresses}, fn2={-> ~[street, city]})
        ColSpec addressesSpec = csa.colSpecs().get(1);
        assertEquals("addresses", addressesSpec.name());
        assertNotNull(addressesSpec.function1());
        // fn1 body is AppliedProperty (just $x.addresses)
        assertInstanceOf(AppliedProperty.class, addressesSpec.function1().body().get(0));
        assertEquals("addresses", ((AppliedProperty) addressesSpec.function1().body().get(0)).property());
        // fn2 is non-null — carries nested ColSpecArray
        assertNotNull(addressesSpec.function2());
        assertTrue(addressesSpec.function2().parameters().isEmpty()); // 0-param lambda
        ClassInstance nestedCi = (ClassInstance) addressesSpec.function2().body().get(0);
        ColSpecArray nestedCsa = (ColSpecArray) nestedCi.value();
        assertEquals(2, nestedCsa.colSpecs().size());
        assertEquals("street", nestedCsa.colSpecs().get(0).name());
        assertEquals("city", nestedCsa.colSpecs().get(1).name());
        // Nested ColSpecs are scalar — fn2 is null
        assertNull(nestedCsa.colSpecs().get(0).function2());
        assertNull(nestedCsa.colSpecs().get(1).function2());
    }

    @Test
    void testGraphFetchRequiresClassExpression() {
        // graphFetch on a relation expression should parse (validation is compiler's job)
        String invalidQuery = "#>{store::DB.TABLE}#->graphFetch(#{ Person { name } }#)";
        // This should parse without error — the clean pipeline doesn't do semantic validation
        assertDoesNotThrow(() -> PureParser.parseQuery(invalidQuery));
    }
}
