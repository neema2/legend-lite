// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.graphql.GqlSchema.Schema;
import com.legend.compiler.graphql.GqlValidator.Limits;
import com.legend.compiler.graphql.GqlValidator.Problem;
import com.legend.compiler.graphql.GqlValidator.Validation;
import com.legend.parser.GqlParser;
import com.legend.protocol.spec.Gql;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The validator over the demo schema (docs/GRAPHQL_STUDY_2026_10_18.md §6.3, §7): the specification's rules,
 * the exact-type fragment trap (G10), the group rules (G7), the limits (G17), the warnings, and the typed result
 * shape a valid document gets back. Every error is reported, with the selection path and a hint.
 */
class GqlValidatorTest {

    static final Schema SCHEMA = SchemaGeneratorTest.demoSchema();

    static Validation validate(String document) {
        return validate(document, Limits.DEFAULT);
    }

    static Validation validate(String document, Limits limits) {
        Gql.Document doc = GqlParser.parseDocument(document, 1, 1);
        return GqlValidator.validate(doc, SCHEMA, limits, null);
    }

    static String errors(Validation v) {
        return String.join("\n", v.errors().stream().map(Problem::toString).toList());
    }

    static void assertValid(Validation v) {
        assertTrue(v.valid(), () -> "expected valid, got:\n" + errors(v));
    }

    static void assertError(Validation v, String fragment) {
        assertFalse(v.valid(), "expected an error mentioning " + fragment);
        assertTrue(errors(v).contains(fragment), () -> "expected an error mentioning '" + fragment + "', got:\n"
                + errors(v));
    }

    @Test
    void theWorkedExampleIsValidAndShaped() {
        Validation v = validate("""
                {
                  Person(where: { age: { gt: 30 }, address: { country: { equals: UK } } },
                         orderBy: [{ lastName: ASC }, { age: DESC }], limit: 10) {
                    fullName age address { city } firm { legalName }
                  }
                }
                """);
        assertValid(v);
        assertEquals("{ Person: [{ fullName: String!, age: BigInt!, address: { city: String! }?,"
                + " firm: { legalName: String! }? }] }", v.shape());
        assertTrue(v.warnings().isEmpty(), v.warnings().toString());
    }

    @Test
    void nestedClausesAggregatesAndAVariableReused() {
        Validation v = validate("""
                query ($w: PersonWhere!) {
                  Firm(orderBy: [{ legalName: ASC }]) {
                    legalName
                    headcount: employeesAggregate { count }
                    seniors: employeesAggregate(where: { age: { gte: 50 } }) { count }
                    oldest: employeesAggregate { max { age born } }
                    top3: employees(orderBy: [{ asEmployee: { salary: DESC } }], limit: 3) {
                      fullName ... on EmployeeInterface { salary }
                    }
                  }
                  people: Person(where: $w, limit: 20) { __typename fullName ... on Manager { budget } }
                  total: PersonAggregate(where: $w) { count }
                }
                """);
        assertValid(v);
        assertEquals("{ Firm: [{ legalName: String!, headcount: { count: BigInt! }, seniors: { count: BigInt! },"
                + " oldest: { max: { age: BigInt, born: Date }? }, top3: [{ fullName: String!,"
                + " salary: Float! @EmployeeInterface }] }], people: [{ __typename: String!, fullName: String!,"
                + " budget: Float! @Manager }], total: { count: BigInt! } }", v.shape());
        assertEquals(1, v.warnings().size(), "limit without orderBy, once (people): " + v.warnings());
    }

    @Test
    void theExactTypeFragmentTrapIsRefusedWithTheFix() {
        Validation v = validate("{ Person { fullName ... on Employee { salary } } }");
        assertError(v, "spread on EmployeeInterface");
        assertValid(validate("{ Person { fullName ... on Employee { salary } ... on Manager { salary budget } } }"));
        assertValid(validate("{ Employee { fullName ... on Manager { budget } } }"));
        assertError(validate("{ Employee { fullName ... on Employee { salary } } }"), "spread on EmployeeInterface");
        assertError(validate("{ Firm { ... on Person { fullName } } }"), "can never match a Firm");
    }

    @Test
    void unknownNamesCarryTheNearestName() {
        assertError(validate("{ Person { fullNam } }"), "did you mean fullName");
        assertError(validate("{ Person(wher: {}) { fullName } }"), "did you mean where");
        assertError(validate("{ Person(where: { agee: { gt: 1 } }) { fullName } }"), "did you mean age");
        assertError(validate("{ Nobody { x } }"), "Query has no field Nobody");
    }

    @Test
    void valuesAreTypedByTheSchema() {
        assertError(validate("{ Person(where: { age: 30 }) { fullName } }"), "write { equals: 30 }");
        assertError(validate("{ Person(where: { age: { gt: \"30\" } }) { fullName } }"), "where BigInt is expected");
        assertError(validate("{ Person(where: { address: { country: { equals: \"UK\" } } }) { fullName } }"),
                "write UK without quotes");
        assertError(validate("{ Person(where: { born: { gte: \"yesterday\" } }) { fullName } }"), "ISO date");
        assertValid(validate("{ Person(where: { born: { gte: \"2000-01-01\" }, age: { in: [30, 40] } }) { fullName } }"));
        assertError(validate("{ Person(orderBy: [{ lastName: ASC, age: DESC }]) { fullName } }"), "one key");
        assertError(validate("{ Person(limit: 3000000000) { fullName } }"), "Int is 32-bit");
        assertValid(validate("{ Employee { salaryOn(d: \"2024-06-30\") } }"));
        assertError(validate("{ Employee { salaryOn } }"), "requires the argument d");
    }

    @Test
    void variablesAreDeclaredUsedAndCompatible() {
        assertError(validate("{ Person(where: $w) { fullName } }"), "$w is not declared");
        assertError(validate("query ($w: PersonWhere) { Person { fullName } }"), "$w is declared and never used");
        assertError(validate("query ($lo: Int!) { Person(where: { age: { gte: $lo } }) { fullName } }"),
                "$lo of type Int! used where BigInt is expected");
        assertValid(validate("query ($lo: BigInt!, $n: Int) { Person(where: { age: { gte: $lo } }, limit: $n,"
                + " orderBy: [{ age: ASC }]) { fullName } }"));
        assertError(validate("query ($w: Person) { Person(where: $w) { fullName } }"), "not an input type");
    }

    @Test
    void fragmentsAreDefinedUsedAndAcyclic() {
        assertValid(validate("{ Person { ...p } } fragment p on PersonInterface { fullName }"));
        assertError(validate("{ Person { fullName } } fragment p on PersonInterface { fullName }"), "never spread");
        assertError(validate("{ Person { ...q } }"), "no fragment named q");
        assertError(validate("{ Person { ...p } } fragment p on PersonInterface { fullName ...p }"), "spreads itself");
        assertError(validate("{ Person { ...p } } fragment p on Nothing { fullName }"), "no type of the schema");
    }

    @Test
    void leavesCompositesAndMergingAreChecked() {
        assertError(validate("{ Person { fullName { x } } }"), "a leaf");
        assertError(validate("{ Person { firm } }"), "select its fields");
        assertError(validate("{ Person { firm { legalName } firm: address { city } } }"), "selected twice");
        assertValid(validate("{ Person { firm { legalName } firm { founded } } }"));
    }

    @Test
    void theLimitsAreEnforced() {
        String deep = "{ Firm { employees { firm { employees { firm { legalName } } } } } }";
        assertValid(validate(deep));
        assertError(validate(deep, new Limits(3, 25)), "over the limit of 3");
        String wide = "{ Firm { a: employees { fullName } b: employees { fullName } c: employees { fullName } } }";
        assertError(validate(wide, new Limits(8, 2)), "3 list fields in one selection");
    }

    @Test
    void theGroupRulesHold() {
        assertValid(validate("{ PersonGroups(having: { count: { gt: 10 } }, orderBy: [{ count: DESC },"
                + " { key: { age: ASC } }]) { key { age born(truncate: YEAR) } count avg { age } } }"));
        assertError(validate("{ PersonGroups { count } }"), "ask PersonAggregate");
        assertError(validate("{ PersonGroups(orderBy: [{ key: { lastName: ASC } }]) { key { age } count } }"),
                "key.lastName, which is not selected");
        assertValid(validate("{ Firm { employeesGroups { key { address { country } } count } } }"));
    }

    @Test
    void operationsAreQueriesAndIntrospectionIsNotYet() {
        assertError(validate("mutation { Person { fullName } }"), "this endpoint reads");
        assertError(validate("{ __schema { types { name } } }"), "not served by this landing");
        assertError(validate("query A { Person { fullName } } query B { Firm { legalName } }"), "name the one to run");
        assertError(validate("{ Person { fullName @foo } }"), "not a directive of this schema");
        assertValid(validate("query ($x: Boolean!) { Person { fullName @include(if: $x) } }"));
    }

    @Test
    void everyErrorIsReportedNotTheFirst() {
        Validation v = validate("{ Person(where: { agee: { gt: 1 } }) { fullNam firm { nope } } }");
        assertEquals(3, v.errors().size(), errors(v));
        List<String> paths = v.errors().stream().map(Problem::path).toList();
        assertEquals(List.of("Person(where).agee", "Person.fullNam", "Person.firm.nope"), paths);
    }
}
