// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * The GraphQL routes (docs/GRAPHQL_STUDY_2026_10_18.md §8.4): {@code /api/lite/v1/graphql/schema} answers the SDL
 * of a model's data space or class list, {@code /api/lite/v1/graphql/validate} a document's verdict with every
 * error, every warning and the shape; a model the generator refuses is 400 COMPILATION like any other.
 */
class GraphqlApiTest {

    static final PureV1Api.Runner NO_RUN = (m, l, r, w) -> {
        throw new AssertionError("no execution in these routes");
    };

    static String demo() {
        try (InputStream in = GraphqlApiTest.class.getResourceAsStream("/graphql-parity/models/demo.pure")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static String body(String model, String extra) {
        Json.Obj m = Json.obj();
        m.fields().put("_type", Json.str("text"));
        m.fields().put("code", Json.str(model));
        return "{\"model\":" + Json.toCompact(m) + (extra.isEmpty() ? "" : "," + extra) + "}";
    }

    static Json.Obj answer(String path, String body) {
        PureV1Api.Answer a = PureV1Api.route(path, null, body, NO_RUN);
        return (Json.Obj) Json.parse(a.json(), PureV1Api.REQUEST);
    }

    @Test
    void theSchemaRouteAnswersTheSdlOfTheWholeModel() {
        Json.Obj a = answer("/api/lite/v1/graphql/schema", body(demo(), ""));
        assertTrue(a.getString("sdl").startsWith("type Query {"), a.getString("sdl").substring(0, 40));
        assertEquals(java.util.List.of("demo::Address", "demo::Employee", "demo::Firm", "demo::Manager",
                "demo::Person"), a.getStringArray("classes"));
    }

    @Test
    void theSchemaRouteScopesByClassList() {
        Json.Obj a = answer("/api/lite/v1/graphql/schema",
                body(demo(), "\"classes\":[\"demo::Address\"]"));
        assertEquals(java.util.List.of("demo::Address"), a.getStringArray("classes"));
        assertTrue(a.getString("sdl").contains("type Address {"));
        assertFalse(a.getString("sdl").contains("type Firm"), "Firm is out of scope");
    }

    @Test
    void theSchemaRouteRefusesAModelTheGeneratorRefuses() {
        PureV1Api.Answer a = PureV1Api.route("/api/lite/v1/graphql/schema", null, body("""
                ###Pure
                Class demo::Firm { legalName: String[1]; employeesAggregate: Integer[1]; }
                Class demo::Person { name: String[1]; }
                Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
                """, ""), NO_RUN);
        assertEquals(400, a.status());
        assertTrue(a.json().contains("demo::Firm.employeesAggregate"), a.json());
    }

    @Test
    void theValidateRouteAnswersTheShapeOrEveryError() {
        Json.Obj ok = answer("/api/lite/v1/graphql/validate",
                body(demo(), "\"query\":\"{ Person(limit: 2) { fullName firm { legalName } } }\""));
        assertTrue(ok.getBool("valid"));
        assertEquals("{ Person: [{ fullName: String!, firm: { legalName: String! }? }] }", ok.getString("shape"));
        assertEquals(1, ok.getArr("warnings").items().size(), "limit without orderBy");
        Json.Obj bad = answer("/api/lite/v1/graphql/validate",
                body(demo(), "\"query\":\"{ Person(where: { agee: { gt: 1 } }) { fullNam } }\""));
        assertFalse(bad.getBool("valid"));
        assertEquals(2, bad.getArr("errors").items().size());
        Json.Obj first = (Json.Obj) bad.getArr("errors").items().get(0);
        assertEquals("Person(where).agee", first.getString("path"));
        assertTrue(first.getString("message").contains("did you mean age"), first.getString("message"));
    }

    @Test
    void theValidateRouteAnswersAParseErrorAsACompilationFailure() {
        PureV1Api.Answer a = PureV1Api.route("/api/lite/v1/graphql/validate", null,
                body(demo(), "\"query\":\"{ Person { \""), NO_RUN);
        assertEquals(400, a.status(), a.json());
        assertTrue(a.json().contains("COMPILATION"), a.json());
    }

    @Test
    void theLimitsAreTheRequests() {
        Json.Obj a = answer("/api/lite/v1/graphql/validate", body(demo(),
                "\"query\":\"{ Firm { employees { firm { legalName } } } }\",\"limits\":{\"maxDepth\":2}"));
        assertFalse(a.getBool("valid"));
        assertTrue(a.toString().contains("over the limit of 2"), a.toString());
    }

    // ---- the query route (PR2d) ----

    /** A store-backed model with its rows as Data, which the server fills a kept memory database from. */
    static final String STORE = """
            ###Pure
            Class demo::Person { firstName: String[1]; lastName: String[1]; age: Integer[1]; salary: Float[0..1];
              fullName() { $this.firstName + ' ' + $this.lastName }: String[1]; }
            Class demo::Firm { legalName: String[1]; }
            Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
            ###Relational
            Database demo::Db
            (
              Schema DEMO
              (
                Table T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100), AGE INTEGER,
                                SALARY DOUBLE, FIRM_ID INTEGER)
                Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
              )
              Join PersonFirm (DEMO.T_PERSON.FIRM_ID = DEMO.T_FIRM.ID)
            )
            ###Mapping
            Mapping demo::Map
            (
              demo::Person: Relational
              {
                ~primaryKey ([demo::Db] DEMO.T_PERSON.ID)
                ~mainTable [demo::Db] DEMO.T_PERSON
                firstName: [demo::Db] DEMO.T_PERSON.FIRST_NAME,
                lastName: [demo::Db] DEMO.T_PERSON.LAST_NAME,
                age: [demo::Db] DEMO.T_PERSON.AGE,
                salary: [demo::Db] DEMO.T_PERSON.SALARY
              }
              demo::Firm: Relational
              {
                ~primaryKey ([demo::Db] DEMO.T_FIRM.ID)
                ~mainTable [demo::Db] DEMO.T_FIRM
                legalName: [demo::Db] DEMO.T_FIRM.LEGAL_NAME
              }
              demo::Employment: Relational
              {
                AssociationMapping
                (
                  firm: [demo::Db] @PersonFirm,
                  employees: [demo::Db] @PersonFirm
                )
              }
            )
            ###Connection
            RelationalDatabaseConnection demo::Conn { store: demo::Db; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime demo::Runtime { mappings: [ demo::Map ]; connections: [ demo::Db: [ db: demo::Conn ] ]; }
            ###Data
            Data demo::Rows
            {
              Relational
              #{
                DEMO.T_FIRM:
                  'ID,LEGAL_NAME\\n1,Acme\\n2,Globex\\n';
                DEMO.T_PERSON:
                  'ID,FIRST_NAME,LAST_NAME,AGE,SALARY,FIRM_ID\\n1,Alice,Smith,45,120000.0,1\\n2,Bob,Jones,32,90000.0,1\\n3,Carol,White,51,150000.0,1\\n4,Eve,Black,39,110000.0,2\\n';
              }#
            }
            """;

    static final PureV1Api.Runner RUN = new QueryService()::executeUpstream;

    static Json.Obj query(String document, String extra) {
        PureV1Api.Answer a = PureV1Api.route("/api/lite/v1/graphql/query", null,
                body(STORE, "\"runtime\":{\"runtime\":\"demo::Runtime\"},\"query\":" + Json.toCompact(document)
                        + (extra.isEmpty() ? "" : "," + extra)), RUN);
        Json.Obj o = (Json.Obj) Json.parse(a.json(), PureV1Api.REQUEST);
        o.fields().put("_status", Json.num(a.status()));
        return o;
    }

    @Test
    void theQueryRouteAnswersEachRootUnderItsKey() {
        Json.Obj a = query("{ acme: Firm(where: { legalName: { equals: \"Acme\" } }) { legalName"
                + " employees(orderBy: [{ salary: DESC }], limit: 2) { firstName } } seniors: Person(where: { age: { gt: 40 } },"
                + " orderBy: [{ age: ASC }]) { fullName } }", "");
        assertEquals(200, a.getInt("_status"), a.toString());
        Json.Obj data = a.getObj("data");
        String acme = Json.toCompact(data.get("acme"));
        assertTrue(acme.contains("Carol") && acme.contains("Alice") && !acme.contains("Bob"), acme);
        String seniors = Json.toCompact(data.get("seniors"));
        assertTrue(seniors.indexOf("Alice Smith") < seniors.indexOf("Carol White"), seniors);
        assertTrue(Json.toCompact(a.getObj("extensions").getObj("pure").get("acme")).contains("demo::Firm.all()"),
                a.toString());
        assertTrue(seniors.contains("\"fullName\":"), "the response key is the field's: " + seniors);
        assertFalse(a.has("errors"), a.toString());
    }

    @Test
    void theQueryRouteTakesVariables() {
        Json.Obj a = query("query Q($min: BigInt, $n: Int) { Person(where: { age: { gte: $min } }, orderBy: [{ age: DESC }], limit: $n) { firstName age } }",
                "\"variables\":{\"min\":40,\"n\":1}");
        assertEquals(200, a.getInt("_status"), a.toString());
        String people = Json.toCompact(a.getObj("data").get("Person"));
        assertTrue(people.contains("Carol") && !people.contains("Alice"), people);
    }

    @Test
    void theQueryRouteRefusesWithEveryValidationErrorAndNoData() {
        Json.Obj a = query("{ Person(where: { agee: { gt: 1 } }) { fullNam } }", "");
        assertEquals(400, a.getInt("_status"), a.toString());
        assertFalse(a.has("data"), a.toString());
        String errors = Json.toCompact(a.get("errors"));
        assertTrue(errors.contains("did you mean age") && errors.contains("did you mean fullName"), errors);
        assertTrue(errors.contains("VALIDATION"), errors);
    }

    @Test
    void theQueryRouteNamesWhatALaterLandingBuilds() {
        Json.Obj a = query("{ PersonGroups { key { age } count } }", "");
        assertEquals(400, a.getInt("_status"), a.toString());
        String errors = Json.toCompact(a.get("errors"));
        assertTrue(errors.contains("PR5") && errors.contains("UNSUPPORTED"), errors);
    }

    @Test
    void theQueryRouteArrangesAnAggregateRootsRow() {
        Json.Obj a = query("{ PersonAggregate(where: { age: { gte: 40 } }) { count max { age } sum { salary } } }", "");
        assertEquals(200, a.getInt("_status"), a.toString());
        Json.Obj agg = a.getObj("data").getObj("PersonAggregate");
        assertEquals(2, agg.getInt("count"), a.toString());
        assertEquals(51, agg.getObj("max").getInt("age"), a.toString());
        assertEquals(270000.0, agg.getObj("sum").getDouble("salary"), 0.01, a.toString());
    }

    @Test
    void theQueryRouteAnswersIntrospection() {
        Json.Obj a = query("{ __schema { queryType { name } types { kind name } } __type(name: \"PersonWhere\") {"
                + " kind inputFields { name type { kind name ofType { name } } } } }", "");
        assertEquals(200, a.getInt("_status"), a.toString());
        Json.Obj data = a.getObj("data");
        assertEquals("Query", data.getObj("__schema").getObj("queryType").getString("name"));
        String types = Json.toCompact(data.getObj("__schema").get("types"));
        assertTrue(types.contains("\"name\":\"Person\"") && types.contains("\"name\":\"PersonWhere\""), types);
        String where = Json.toCompact(data.get("__type"));
        assertTrue(where.contains("INPUT_OBJECT") && where.contains("\"name\":\"age\"") && where.contains("BigIntFilter"), where);
    }

    @Test
    void theQueryRouteFollowsGraphiqlsIntrospectionQuery() {
        String q = "query IntrospectionQuery { __schema { queryType { name } mutationType { name } subscriptionType { name }"
                + " types { ...FullType } directives { name description locations args { ...InputValue } } } }"
                + " fragment FullType on __Type { kind name description specifiedByURL fields(includeDeprecated: true) { name description"
                + " args { ...InputValue } type { ...TypeRef } isDeprecated deprecationReason } inputFields { ...InputValue }"
                + " interfaces { ...TypeRef } enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }"
                + " possibleTypes { ...TypeRef } }"
                + " fragment InputValue on __InputValue { name description type { ...TypeRef } defaultValue }"
                + " fragment TypeRef on __Type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name"
                + " ofType { kind name ofType { kind name ofType { kind name } } } } } } } }";
        Json.Obj a = query(q, "");
        assertEquals(200, a.getInt("_status"), a.toString());
        String types = Json.toCompact(a.getObj("data").getObj("__schema").get("types"));
        assertTrue(types.contains("\"name\":\"employees\"") && types.contains("NON_NULL") && types.contains("LIST"), types);
    }
}
