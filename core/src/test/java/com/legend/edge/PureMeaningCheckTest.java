// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.json.Json;
import com.legend.server.LiteV1Api;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PURE-MEANING DETECTOR at legend-engine's door (A3 piece 3; {@link PureMeaningCheck}; the final plan's section
 * 4a; D1, ruled by the user 2026-11-19 ~13:25, and the default, ruled ~19:20). Each test is the check's answer to a
 * planted request.
 * Where a test proves a reading (store-bound or host-side, quiet or flagged), its partner plants the other side, so a
 * check that flagged everything, or nothing, fails one of the two.
 */
class PureMeaningCheckTest {

    /** A relational model over H2: a table, a class mapped to it, a qualified property, a user function, a constraint
     *  and a runtime. */
    static final String MODEL = String.join("\n",
            "###Relational\nDatabase t::DB ( Table PERSON ( ID INTEGER PRIMARY KEY, NAME VARCHAR(32), CODE VARCHAR(8) ) )",
            "###Pure",
            "Class t::Person",
            "[ shortName: $this.name->substring(1, 2)->length() == 1 ]",
            "{",
            "    name: String[1];",
            "    code: String[1];",
            "    initials() { $this.name->substring(1, 2) }: String[1];",
            "}",
            "Class t::Out { short: String[1]; }",
            "Class t::Auth extends meta::pure::alloy::connections::alloy::authentication::AuthenticationStrategy {}",
            "function t::firstThree(s: String[1]): String[1] { $s->substring(1, 3) }",
            "function t::position(s: String[1]): Integer[1] { $s->indexOf('a') }",
            "###Mapping\nMapping t::M ( t::Person: Relational { ~mainTable [t::DB]PERSON name: [t::DB]PERSON.NAME,"
                    + " code: [t::DB]PERSON.CODE } )",
            "###Mapping\nMapping t::M2 ( t::Out: Pure { ~src t::Person short: $src.name->substring(1, 2) } )",
            "###Connection\nRelationalDatabaseConnection t::Conn { store: t::DB; type: H2; specification: LocalH2 { };"
                    + " auth: DefaultH2; }",
            "###Runtime\nRuntime t::RT { mappings: [t::M]; connections: [ t::DB: [ c1: t::Conn ] ]; }",
            "###Runtime\nRuntime t::JsonRT { mappings: [t::M2]; connections: [ ModelStore: [ json: #{ JsonModelConnection {"
                    + " class: t::Person; url: 'data:application/json,[]'; } }# ] ]; }",
            "");

    static final String TABLE = "#>{t::DB.PERSON}#";

    private static ModelContext ctx(String model) {
        return Compiler.compileModel(model);
    }

    private static PureMeaningCheck.Result check(String model, String query) {
        ModelContext c = ctx(model);
        TypedQuery q = Compiler.query(c, query);
        return PureMeaningCheck.of(q, c, "t::RT", null);
    }

    private static PureMeaningCheck.Result check(String query) {
        return check(MODEL, query);
    }

    // ---- what it flags -------------------------------------------------------------------------------------------

    @Test
    void aStoreBoundSubstringAndIndexOfAreFlagged() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3),"
                + " b: r|$r.NAME->toOne()->substring(2), c: r|$r.NAME->toOne()->indexOf('o')])->from(t::RT)");
        assertEquals("substring/2=1, substring/3=1, indexOf/2=1", r.header());
    }

    @Test
    void aLiteralSubstringInsideTheRoutedQueryIsFlagged_asLegendEngineSendsItToSql() {
        // legend-engine 4.145.0 planned `select substring('abcdef', 1, 3)` for this (H16's host-side probe)
        PureMeaningCheck.Result r = check("|" + TABLE + "->limit(1)->extend(~s: r|'abcdef'->substring(1, 3))"
                + "->from(t::RT)");
        assertEquals("substring/3=1", r.header());
    }

    @Test
    void aHostSideLetIsNotFlagged_butTheSameCallInTheQueryIs() {
        // legend-engine computes a store-free let before the query, Pure's meaning (`where region = 'bc'`, H16)
        PureMeaningCheck.Result host = check("{|let x = 'abcdef'->substring(1, 3); " + TABLE
                + "->filter(r|$r.NAME == $x)->from(t::RT);}");
        assertTrue(host.clean(), host.header());
        PureMeaningCheck.Result routed = check("|" + TABLE
                + "->filter(r|$r.NAME == 'abcdef'->substring(1, 3))->from(t::RT)");
        assertEquals("substring/3=1", routed.header());
    }

    @Test
    void aLetBoundLambdaTheQueryNamesIsStoreBound() {
        PureMeaningCheck.Result r = check("{|let f = {s: String[1]|$s->indexOf('x')}; " + TABLE
                + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        assertEquals("indexOf/2=1", r.header());
    }

    @Test
    void theListIndexOfAndTheThreeArgumentIndexOfAreNotFlagged() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|[1, 2, 3]->indexOf(2),"
                + " b: r|$r.NAME->toOne()->indexOf('o', 1)])->from(t::RT)");
        assertTrue(r.clean(), r.header());
    }

    // ---- the reach -------------------------------------------------------------------------------------------------

    @Test
    void aUserFunctionTheQueryCallsIsFollowed() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|t::firstThree($r.NAME->toOne()),"
                + " b: r|t::position($r.NAME->toOne())])->from(t::RT)");
        assertEquals("substring/3=1, indexOf/2=1", r.header());
        assertTrue(r.sites().stream().anyMatch(s -> s.where().equals("t::firstThree")), r.sites().toString());
    }

    @Test
    void aQualifiedPropertyTheQueryReadsIsFollowed() {
        PureMeaningCheck.Result r = check("|t::Person.all()->project(~[i: p|$p.initials()])->from(t::M, t::RT)");
        assertEquals("substring/3=1", r.header());
    }

    @Test
    void aConstraintIsNotFollowed_legendEngineChecksItInJava() {
        // the class's constraint calls substring; a query reading the class does not reach it
        PureMeaningCheck.Result r = check("|t::Person.all()->project(~[n: p|$p.name])->from(t::M, t::RT)");
        assertTrue(r.clean(), r.header());
    }

    /** validate's constraints become the query's filter before typing (ValidateDesugar, through the query front
     *  door {@code Compiler.resolveQuery}): store-bound code, as legend-engine's validate pushes them into the SQL. The
     *  pure/v1 door types a request through {@code Compiler.query}, which does not desugar validate, so there a validate
     *  request is refused at typing and never answered (the partner below). */
    @Test
    void aConstraintValidateTurnsIntoAFilterIsFlagged_andTheDoorRefusesAValidateRequest() {
        // validate's runtime is a Runtime value (it calls execute), as legend-engine's validation tests pass one
        String runtime = "^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore("
                + "element = t::DB, connection = ^meta::external::store::relational::runtime::RelationalDatabaseConnection("
                + "type = meta::relational::runtime::DatabaseType.H2, datasourceSpecification ="
                + " ^meta::pure::alloy::connections::alloy::specification::LocalH2DatasourceSpecification(),"
                + " authenticationStrategy = ^t::Auth())))";
        String validate = "|meta::relational::validation::validate({|t::Person.all()}, t::M, " + runtime + ", [])";
        ModelContext c = ctx(MODEL);
        com.legend.protocol.spec.LambdaFunction lambda =
                (com.legend.protocol.spec.LambdaFunction) Compiler.parseQuery(validate);
        TypedQuery q = Compiler.queryResolved(c, Compiler.resolveQuery(lambda.body(),
                new com.legend.model.ImportScope(List.of()), c));
        PureMeaningCheck.Result r = PureMeaningCheck.of(q, c, "t::RT", null);
        assertEquals("substring/3=1", r.header());
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(validate), NO_RUN,
                LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.WARN);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("validate"), a.json());
    }

    /** An M2M binding's transforms, over a chain whose source is a store, in a TDS query: legend-engine pushes them
     *  into the source query ({@code chain.pure:111-116}), so they are store-bound code. */
    @Test
    void anM2MTransformInATdsQueryOverAChainIsFlagged() {
        PureMeaningCheck.Result r = check("|t::Out.all()->project(~[s: o|$o.short])"
                + "->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
        assertEquals("substring/3=1", r.header());
        assertTrue(r.sites().stream().anyMatch(s -> s.where().contains("t::M2")
                || s.where().contains("t::Out")), r.sites().toString());
    }

    /** The same transforms fetched as a graph: legend-engine runs a graphFetch's M2M transforms in Java
     *  ({@code storeContract.pure:89-91}), Pure's meaning. */
    @Test
    void anM2MTransformUnderAGraphFetchIsNotFlagged() {
        PureMeaningCheck.Result r = check("|t::Out.all()->graphFetch(#{t::Out{short}}#)->serialize(#{t::Out{short}}#)"
                + "->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
        assertTrue(r.clean(), r.header());
    }

    /** An M2M mapping over a model connection, with no chain to a store: legend-engine runs it in Java. */
    @Test
    void anUnchainedM2MTransformIsNotFlagged() {
        ModelContext c = ctx(MODEL);
        TypedQuery q = Compiler.query(c, "|t::Out.all()->project(~[s: o|$o.short])->from(t::M2, t::JsonRT)");
        PureMeaningCheck.Result r = PureMeaningCheck.of(q, c, "t::JsonRT", null);
        assertTrue(r.clean(), r.header());
    }

    // ---- unverified ----------------------------------------------------------------------------------------------

    /** The model with a function whose body does not type, which the check cannot follow. Kept out of
     *  {@link #MODEL}: compile reports it. */
    static final String UNFOLLOWABLE = MODEL + String.join("\n",
            "###Pure",
            "function t::untyped(s: String[1]): Integer[1] { $s->substring(1, 2) }",
            "");

    /** A function value the check does not hold (a lambda read out of a collection, which legend-engine's grammar
     *  writes; a class property of a function type is lite's own dialect, which no legend-engine client sends):
     *  reported unverified, never clean, and refused under refuse. */
    @Test
    void aFunctionValueTheCheckDoesNotHoldIsUnverified_andRefused() {
        String query = "|" + TABLE + "->extend(~i: r|[{s: String[1]|$s->length()}]->at(0)->eval($r.NAME->toOne()))"
                + "->from(t::RT)";
        PureMeaningCheck.Result r = check(query);
        assertEquals("unverified=1", r.header());
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(query), NO_RUN);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("unverified"), a.json());
        assertEquals("unverified=1", a.headers().get(PureMeaningCheck.HEADER));
        // the partner: the same lambda let-bound and named is followed, so its body is read, and it is clean
        assertTrue(check("{|let f = {s: String[1]|$s->length()}; " + TABLE
                + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}").clean());
    }

    /** A function whose body does not type cannot be read: unverified, never clean. */
    @Test
    void aFunctionWhoseBodyDoesNotTypeIsUnverified() {
        PureMeaningCheck.Result r = check(UNFOLLOWABLE, "|" + TABLE
                + "->extend(~i: r|t::untyped($r.NAME->toOne()))->from(t::RT)");
        assertEquals("unverified=1", r.header());
        assertTrue(r.sites().get(0).where().startsWith("t::untyped"), r.sites().toString());
    }

    /** A mapping the check cannot find (here, a test-data request's mapping the model does not declare) is
     *  unverified, never clean (the reviewer's S2). */
    @Test
    void aMappingTheModelDoesNotDeclareIsUnverified() {
        ModelContext c = ctx(MODEL);
        TypedQuery q = Compiler.query(c, "|t::Person.all()->project(~[n: p|$p.name])->from(t::M, t::RT)");
        PureMeaningCheck.Result r = PureMeaningCheck.of(q, c, "t::RT", "t::NotInTheModel");
        assertEquals("unverified=1", r.header());
        assertTrue(PureMeaningCheck.of(q, c, "t::RT", "t::M").clean());
    }

    // ---- quiet --------------------------------------------------------------------------------------------------

    @Test
    void theCorrectedOptionQuietsSubstring_andNotIndexOf() {
        String calls = "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3), b: r|$r.NAME->toOne()->indexOf('o')])";
        PureMeaningCheck.Result r = check("|" + TABLE + calls + "->from(t::RT)"
                + "->meta::pure::executionPlan::featureFlag::withFeatureFlags("
                + "meta::pure::executionPlan::features::Feature.CORRECT_SQL_SUBSTRING_INDEXING)");
        assertEquals("indexOf/2=1", r.header());
    }

    @Test
    void aLiteralZeroStartIsExemptInBothForms_whereMeasuredEqualSo() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(0, 3),"
                + " b: r|$r.NAME->toOne()->substring(0)])->from(t::RT)");
        assertTrue(r.clean(), r.header());
        PureMeaningCheck.Result one = check("|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->substring(1, 3))"
                + "->from(t::RT)");
        assertEquals("substring/3=1", one.header());
    }

    @Test
    void aLiteralZeroStartIsExemptInTheTwoArgumentFormAlone_whereMeasuredSo() {
        String twoArgumentOnly = MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
                "type: DuckDB; specification: DuckDB { }; auth: Test;");
        PureMeaningCheck.Result r = check(twoArgumentOnly, "|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(0, 3),"
                + " b: r|$r.NAME->toOne()->substring(0)])->from(t::RT)");
        assertEquals("substring/3=1", r.header());
    }

    @Test
    void aLiteralZeroStartIsFlaggedOnATargetNotMeasured() {
        String unmeasured = MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
                "type: Postgres; specification: Static { name: 'x'; host: 'h'; port: 5432; };"
                        + " auth: UserNamePassword { baseVaultReference: 'v'; userNameVaultReference: 'u';"
                        + " passwordVaultReference: 'p'; };");
        PureMeaningCheck.Result r = check(unmeasured, "|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(0, 3),"
                + " b: r|$r.NAME->toOne()->substring(0)])->from(t::RT)");
        assertEquals("substring/2=1, substring/3=1", r.header());
    }

    // ---- the header and the sentence -------------------------------------------------------------------------------

    @Test
    void theHeaderIsBounded_codesAndCountsOnly() {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            cols.append(i == 0 ? "" : ", ").append("c").append(i).append(": r|$r.NAME->toOne()->substring(")
                    .append(i + 1).append(", ").append(i + 3).append(")");
        }
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[" + cols + "])->from(t::RT)");
        assertEquals("substring/3=50", r.header());
        String sentence = r.sentence("execute", PureMeaningCheck.Mode.REFUSE);
        assertTrue(sentence.contains("and 30 more"), sentence);
        assertFalse(sentence.contains("NAME"), "the sentence names calls, never the query's text: " + sentence);
    }

    // ---- the setting ----------------------------------------------------------------------------------------------

    @Test
    void theSettingIsRefuseByDefault_warnByChoice_andNothingElse() {
        assertEquals(PureMeaningCheck.Mode.REFUSE, PureMeaningCheck.mode(null));
        assertEquals(PureMeaningCheck.Mode.REFUSE, PureMeaningCheck.mode(""));
        assertEquals(PureMeaningCheck.Mode.REFUSE, PureMeaningCheck.mode("refuse"));
        assertEquals(PureMeaningCheck.Mode.WARN, PureMeaningCheck.mode("warn"));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> PureMeaningCheck.mode("engine"));
        assertTrue(e.getMessage().contains("LEGEND_LITE_PURE_MEANING=engine"), e.getMessage());
    }

    // ---- the routes ------------------------------------------------------------------------------------------------

    /** A runner that must not run: a refusal happens before the query is planned or run. */
    private static final LiteV1Api.Runner NO_RUN = (query, runtime, output) -> {
        throw new AssertionError("a refused query is never run");
    };

    /** A runner that runs nothing and answers no rows: the route's header is what these tests read. */
    private static final LiteV1Api.Runner NOTHING = (query, runtime, output) -> {
    };

    private static String body(String lambdaText) {
        return body(MODEL, lambdaText);
    }

    private static String body(String modelText, String lambdaText) {
        String function = PureV1Routes.grammarToJsonLambda(lambdaText, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", modelText);
        return "{\"clientVersion\":\"vX_X_X\",\"function\":" + function + ",\"model\":" + Json.toCompact(model)
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
    }

    static final String PLANTED = "|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3),"
            + " b: r|$r.NAME->toOne()->indexOf('o')])->from(t::RT)";

    @Test
    void executeRefusesAFlaggedQueryByDefault_inLegendEnginesEnvelope_withTheHeader() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(PLANTED), NO_RUN);
        assertEquals(500, a.status(), a.json());
        Json.Obj o = Json.parseObject(a.json());
        assertEquals("COMPILATION", o.getString("errorType"));
        String message = o.getString("message");
        assertTrue(message.contains("substring/3 at 1:") && message.contains("indexOf/2 at 1:"), message);
        assertTrue(message.contains("LEGEND_LITE_PURE_MEANING=warn"), message);
        assertEquals("substring/3=1, indexOf/2=1", a.headers().get(PureMeaningCheck.HEADER));
    }

    /** A refused request sends nothing to the database: the check runs right after the request is typed, before it
     *  is planned or run, on every route that runs a query (the compat owner's order of ~19:52). The runner is the
     *  routes' one way to a session, and a refused request never reaches it. */
    @Test
    void aRefusedRequestRunsNoStatement_onEveryRouteThatRunsAQuery() {
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        LiteV1Api.Runner counting = (query, runtime, output) -> runs.incrementAndGet();
        String testData = testDataBody("|t::Person.all()->project(~[i: p|$p.name->substring(1, 2)])");
        Map<String, String> routes = new java.util.LinkedHashMap<>();
        routes.put("/api/pure/v1/execution/execute", body(PLANTED));
        routes.put("/api/pure/v1/execution/generatePlan", body(PLANTED));
        routes.put("/api/pure/v1/execution/testDataGeneration/generateSeedData", testData);
        routes.put("/api/pure/v1/execution/testDataGeneration/generateTestData_WithSeed", testData);
        routes.put("/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed", testData);
        for (Map.Entry<String, String> route : routes.entrySet()) {
            for (String format : java.util.Arrays.asList(null, "serializationFormat=ARROW_IPC")) {
                LiteV1Api.Answer a = PureV1Routes.route(route.getKey(), format, route.getValue(), counting);
                assertEquals(500, a.status(), route.getKey() + " " + a.json());
                assertTrue(a.json().contains("LEGEND_LITE_PURE_MEANING=warn"), route.getKey() + " " + a.json());
                assertTrue(a.headers().get(PureMeaningCheck.HEADER) != null, route.getKey());
            }
        }
        assertEquals(0, runs.get(), "a refused request reached the runner");
        // the partner: the same counting runner IS reached by a clean request, so the zero above is the refusal's
        PureV1Routes.route("/api/pure/v1/execution/execute", null,
                body("|" + TABLE + "->select(~[NAME])->from(t::RT)"), counting);
        assertEquals(1, runs.get(), "a clean request runs");
    }

    private static String testDataBody(String lambdaText) {
        String function = PureV1Routes.grammarToJsonLambda(lambdaText, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", MODEL);
        return "{\"function\":" + function + ",\"mapping\":\"t::M\",\"runtime\":{\"_type\":\"runtimePointer\","
                + "\"runtime\":\"t::RT\"},\"model\":" + Json.toCompact(model) + ",\"tableRowIdentifiers\":[]}";
    }

    @Test
    void executeUnderWarnAnswers_withTheHeaderAndTheBrowsersLeaveToReadIt() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(PLANTED), NOTHING,
                LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.WARN);
        assertEquals(200, a.status(), a.json());
        assertEquals("substring/3=1, indexOf/2=1", a.headers().get(PureMeaningCheck.HEADER));
        assertEquals(PureMeaningCheck.HEADER, a.headers().get("Access-Control-Expose-Headers"));
    }

    @Test
    void aCleanQueryAnswersAsItDidWithNoHeader() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null,
                body("|" + TABLE + "->select(~[NAME])->from(t::RT)"), NOTHING);
        assertEquals(200, a.status(), a.json());
        assertNull(a.headers().get(PureMeaningCheck.HEADER));
    }

    @Test
    void generatePlanUnderWarnPlansTheQueryUnchanged_withTheHeader_andRefusesItByDefault() {
        String b = body(PLANTED);
        LiteV1Api.Answer warned = PureV1Routes.route("/api/pure/v1/execution/generatePlan", null, b, NO_RUN,
                LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.WARN);
        assertEquals(200, warned.status(), warned.json());
        assertEquals("substring/3=1, indexOf/2=1", warned.headers().get(PureMeaningCheck.HEADER));
        // the query is never changed: the plan's SQL is the core's own plan of the same typed query
        String sql = Json.parseObject(warned.json()).getObj("rootExecutionNode").getArr("executionNodes").items()
                .stream().map(n -> ((Json.Obj) n).getString("sqlQuery")).findFirst().orElseThrow();
        assertEquals(Compiler.query(ctx(MODEL), PLANTED).plan("t::RT", PureText.PURE_V1).sql(), sql);
        LiteV1Api.Answer refused = PureV1Routes.route("/api/pure/v1/execution/generatePlan", null, b, NO_RUN);
        assertEquals(500, refused.status(), refused.json());
        assertEquals("substring/3=1, indexOf/2=1", refused.headers().get(PureMeaningCheck.HEADER));
    }

    @Test
    void theArrowAnswerCarriesTheHeaderBesideItsFormat() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", "serializationFormat=ARROW_IPC",
                body(PLANTED), NOTHING, LiteV1Api.Models.COMPILED, PureMeaningCheck.Mode.WARN);
        assertEquals(200, a.status(), a.json());
        assertEquals("FormatNotSet", a.headers().get("x-legend-response-format"));
        assertEquals("substring/3=1, indexOf/2=1", a.headers().get(PureMeaningCheck.HEADER));
    }

    @Test
    void theRoutesThatRunNoQueryAreNotChecked() {
        // compile types every body (the constraint and the functions call substring) and is answered as before
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", MODEL);
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/compilation/compile", null, Json.toCompact(model),
                NO_RUN);
        assertEquals(200, a.status(), a.json());
        assertNull(a.headers().get(PureMeaningCheck.HEADER));
    }

    /** A check that misses what it must flag fails the first; one that flags what it must not fails the second. */
    @Test
    void theCheckBites_bothWays() {
        List<String> flagged = List.of(
                "|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT)",
                "|" + TABLE + "->extend(~a: r|t::firstThree($r.NAME->toOne()))->from(t::RT)");
        List<String> quiet = List.of(
                "{|let x = 'abc'->indexOf('b'); " + TABLE + "->filter(r|$r.ID == $x)->from(t::RT);}",
                "|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->substring(0))->from(t::RT)");
        for (String q : flagged) {
            assertFalse(check(q).clean(), q);
        }
        for (String q : quiet) {
            assertTrue(check(q).clean(), q + " -> " + check(q).header());
        }
    }
}
