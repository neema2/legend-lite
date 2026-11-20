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
            "    firstC() { $this.name->indexOf('c') }: Integer[1];",
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
        assertEquals("substring/2=1, substring/3=1, indexOf/2=1", r.counts());
    }

    /** substr means legend-pure's in both, apart from out of range: legend-engine's processSubstr sends the database's
     *  SUBSTRING over start + 1 and end - start with no range check, so out of range it answers the database's cut-short
     *  value ('FX'->substr(1, 3) is 'X' on H2 and DuckDB, asked of legend-engine 4.145.0, 2026-11-20), where lite
     *  raises as legend-pure does (S383). Both forms are flagged. */
    @Test
    void aStoreBoundSubstrIsFlaggedInBothForms() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substr(1, 3),"
                + " b: r|$r.NAME->toOne()->substr(2)])->from(t::RT)");
        assertEquals("substr/2=1, substr/3=1", r.counts());
    }

    /** toOne of an operand that is always empty (register S384): legend-engine's processNoOp writes the empty literal's
     *  SQL null, so it answers null where lite raises legend-pure's size error; flagged. A toOne of a row's value that
     *  may be empty (lite already lets it through unchecked, as processNoOp does), of a value that is exactly one, of a
     *  literal list of two (legend-engine refuses it at planning with legend-pure's sentence), and the message overload
     *  (which legend-engine has no SQL for) are not. */
    @Test
    void aStoreBoundToOneOfAnEmptyOperandIsFlagged_andNoOtherToOne() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|[]->cast(@Integer)->toOne(),"
                + " b: r|$r.NAME->toOne(), c: r|$r.ID->toOne(), d: r|[1, 2]->toOne(),"
                + " e: r|[]->cast(@Integer)->toOne('none')])->from(t::RT)");
        assertEquals("toOne(empty)=1", r.counts());
    }

    /** A store-free let's toOne of empty is computed on the host, with legend-pure's raise: not flagged. */
    @Test
    void aHostSideToOneOfEmptyIsNotFlagged() {
        PureMeaningCheck.Result host = check("{|let x = []->cast(@Integer)->toOne(); " + TABLE
                + "->filter(r|$r.ID == $x)->from(t::RT);}");
        assertTrue(host.clean(), host.counts());
    }

    /** A store-free let's substr is computed on the host, as legend-engine computes any store-free let: not flagged. */
    @Test
    void aHostSideSubstrIsNotFlagged() {
        PureMeaningCheck.Result host = check("{|let x = 'abcdef'->substr(1, 3); " + TABLE
                + "->filter(r|$r.NAME == $x)->from(t::RT);}");
        assertTrue(host.clean(), host.counts());
    }

    @Test
    void aLiteralSubstringInsideTheRoutedQueryIsFlagged_asLegendEngineSendsItToSql() {
        // legend-engine 4.145.0 planned `select substring('abcdef', 1, 3)` for this (H16's host-side probe)
        PureMeaningCheck.Result r = check("|" + TABLE + "->limit(1)->extend(~s: r|'abcdef'->substring(1, 3))"
                + "->from(t::RT)");
        assertEquals("substring/3=1", r.counts());
    }

    @Test
    void aHostSideLetIsNotFlagged_butTheSameCallInTheQueryIs() {
        // legend-engine computes a store-free let before the query, Pure's meaning (`where region = 'bc'`, H16)
        PureMeaningCheck.Result host = check("{|let x = 'abcdef'->substring(1, 3); " + TABLE
                + "->filter(r|$r.NAME == $x)->from(t::RT);}");
        assertTrue(host.clean(), host.counts());
        PureMeaningCheck.Result routed = check("|" + TABLE
                + "->filter(r|$r.NAME == 'abcdef'->substring(1, 3))->from(t::RT)");
        assertEquals("substring/3=1", routed.counts());
    }

    /** A let whose value is computed with a lambda (not a lambda itself) is a store-free value legend-engine computes on
     *  the host before the query (H16's probe: a store-free let is computed first, Pure's meaning), so its call is
     *  not store-bound code. Its partner below: a let whose value IS a lambda the query evaluates is. */
    /** A value a store-bound let reads, then a call on it in a statement of its own: legend-engine computes that call on
     *  the host, Pure's meaning (asked of legend-engine 4.145.0: `{|let d = ...->toOne().desk; $d->substring(1, 3);}`
     *  answers 're'), so it is not flagged. The same call in the one statement is flagged, as legend-engine sends it to
     *  the database (`->toOne().desk->substring(1, 3)` answers SQL's 'Cre'). */
    @Test
    void aCallOnAStoreReadLetInAStatementOfItsOwnIsHostSide_andInTheOneStatementIsFlagged() {
        String one = "t::Person.all()->filter(p|$p.name == 'abcdef')->from(t::M, t::RT)->toOne().name";
        assertTrue(check("{|let d = " + one + "; $d->substring(1, 3);}").clean());
        assertEquals("substring/3=1", check("|" + one + "->substring(1, 3)").counts());
    }

    /** A let whose value is a function computed some other way (here read out of a collection), evaluated by
     *  store-bound code: the typer keeps the variable (it substitutes only a literal lambda let), so the walk cannot
     *  read the function's body: unverified, never clean (the reviewer's R2-S1). Its partner: the literal lambda let is
     *  read and flagged (aLetBoundLambdaTheQueryNamesIsStoreBound). */
    @Test
    void anEvaluatedLetWhoseValueIsNotALiteralLambdaIsUnverified() {
        PureMeaningCheck.Result r = check("{|let f = [{s: String[1]|$s->indexOf('x')}]->toOne(); " + TABLE
                + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        assertEquals("unverified=1", r.counts());
    }

    /** A class collection a let reads, then a call over it in a statement of its own: legend-engine runs the let's query
     *  and computes the call on the host, Pure's meaning (asked of legend-engine 4.145.0: `$ps->map(p|$p.desk->
     *  substring(1, 3))` answers ["re"]), so it is not flagged; the same map in the one statement is SQL's ('Cre') and
     *  flagged. A relation let read the same way legend-engine refuses ("No SQL translation exists for the PURE function
     *  'letFunction...'"), so the detector follows no variable to a let. */
    @Test
    void aCallOverAStoreReadCollectionLetIsHostSide_andInTheOneStatementIsFlagged() {
        String people = "t::Person.all()->filter(p|$p.name == 'abcdef')->from(t::M, t::RT)";
        assertTrue(check("{|let ps = " + people + "; $ps->map(p|$p.name->substring(1, 3));}").clean());
        assertEquals("substring/3=1", check("|" + people + "->map(p|$p.name->substring(1, 3))").counts());
    }

    /** A let whose own value holds the call, then read by a statement of its own: legend-engine computes the let's call
     *  in its SQL (asked: `let d = ...->toOne().desk->substring(1, 3); $d->toUpper()` plans
     *  `select substring(desk, 1, 3)` and answers "CRE"), so the let is store-bound code and flagged. */
    @Test
    void aStoreBoundValueLetsOwnCallIsFlagged() {
        assertEquals("substring/3=1", check("{|let d = t::Person.all()->filter(p|$p.name == 'abcdef')"
                + "->from(t::M, t::RT)->toOne().name->substring(1, 3); $d->toUpper();}").counts());
    }

    @Test
    void aLetComputedWithALambdaIsHostSide_notStoreBound() {
        PureMeaningCheck.Result r = check("{|let n = ['ax', 'bx']->map(s|$s->indexOf('x')); " + TABLE
                + "->filter(r|$r.ID->in($n))->from(t::RT);}");
        assertTrue(r.clean(), r.counts());
    }

    @Test
    void aLetBoundLambdaTheQueryNamesIsStoreBound() {
        PureMeaningCheck.Result r = check("{|let f = {s: String[1]|$s->indexOf('x')}; " + TABLE
                + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        assertEquals("indexOf/2=1", r.counts());
    }

    @Test
    void theListIndexOfAndTheThreeArgumentIndexOfAreNotFlagged() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|[1, 2, 3]->indexOf(2),"
                + " b: r|$r.NAME->toOne()->indexOf('o', 1)])->from(t::RT)");
        assertTrue(r.clean(), r.counts());
    }

    // ---- the reach -------------------------------------------------------------------------------------------------

    @Test
    void aUserFunctionTheQueryCallsIsFollowed() {
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[a: r|t::firstThree($r.NAME->toOne()),"
                + " b: r|t::position($r.NAME->toOne())])->from(t::RT)");
        assertEquals("substring/3=1, indexOf/2=1", r.counts());
        assertTrue(r.sites().stream().anyMatch(s -> s.where().equals("t::firstThree")), r.sites().toString());
    }

    @Test
    void aQualifiedPropertyTheQueryReadsIsFollowed() {
        PureMeaningCheck.Result r = check("|t::Person.all()->project(~[i: p|$p.initials()])->from(t::M, t::RT)");
        assertEquals("substring/3=1", r.counts());
    }

    /** A graph fetch's tree reads properties that are not the node's children: a qualified property in it is followed,
     *  as legend-engine computes it in the database (asked of legend-engine 4.145.0: a graphFetch's {@code deskPart()},
     *  a {@code substring}, answers SQL's 'Cre' on H2 and DuckDB). Its partner: the same fetch without it is clean. */
    @Test
    void aQualifiedPropertyAGraphFetchReadsIsFollowed() {
        PureMeaningCheck.Result r = check("|t::Person.all()->graphFetch(#{t::Person{name,initials,firstC}}#)"
                + "->serialize(#{t::Person{name,initials,firstC}}#)->from(t::M, t::RT)");
        assertEquals("substring/3=1, indexOf/2=1", r.counts());
        assertTrue(check("|t::Person.all()->graphFetch(#{t::Person{name}}#)->serialize(#{t::Person{name}}#)"
                + "->from(t::M, t::RT)").clean());
    }

    @Test
    void aConstraintIsNotFollowed_legendEngineChecksItInJava() {
        // the class's constraint calls substring; a query reading the class does not reach it
        PureMeaningCheck.Result r = check("|t::Person.all()->project(~[n: p|$p.name])->from(t::M, t::RT)");
        assertTrue(r.clean(), r.counts());
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
        assertEquals("substring/3=1", r.counts());
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(validate), NO_RUN,
                LiteV1Api.Models.COMPILED);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("validate"), a.json());
    }

    /** An M2M binding's transforms, over a chain whose source is a store, in a TDS query: legend-engine pushes them
     *  into the source query ({@code chain.pure:111-116}), so they are store-bound code. */
    @Test
    void anM2MTransformInATdsQueryOverAChainIsFlagged() {
        PureMeaningCheck.Result r = check("|t::Out.all()->project(~[s: o|$o.short])"
                + "->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
        assertEquals("substring/3=1", r.counts());
        assertTrue(r.sites().stream().anyMatch(s -> s.where().contains("t::M2")
                || s.where().contains("t::Out")), r.sites().toString());
    }

    /** The same transforms fetched as a graph: legend-engine runs a graphFetch's M2M transforms in Java
     *  ({@code storeContract.pure:89-91}), Pure's meaning. */
    @Test
    void anM2MTransformUnderAGraphFetchIsNotFlagged() {
        PureMeaningCheck.Result r = check("|t::Out.all()->graphFetch(#{t::Out{short}}#)->serialize(#{t::Out{short}}#)"
                + "->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
        assertTrue(r.clean(), r.counts());
    }

    /** An M2M mapping over a model connection, with no chain to a store: legend-engine runs it in Java. */
    @Test
    void anUnchainedM2MTransformIsNotFlagged() {
        ModelContext c = ctx(MODEL);
        TypedQuery q = Compiler.query(c, "|t::Out.all()->project(~[s: o|$o.short])->from(t::M2, t::JsonRT)");
        PureMeaningCheck.Result r = PureMeaningCheck.of(q, c, "t::JsonRT", null);
        assertTrue(r.clean(), r.counts());
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
     *  reported unverified, never clean, and refused by name at the door (it cannot be given legend-engine's meaning). */
    @Test
    void aFunctionValueTheCheckDoesNotHoldIsUnverified_andRefused() {
        String query = "|" + TABLE + "->extend(~i: r|[{s: String[1]|$s->length()}]->at(0)->eval($r.NAME->toOne()))"
                + "->from(t::RT)";
        PureMeaningCheck.Result r = check(query);
        assertEquals("unverified=1", r.counts());
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(query), NO_RUN);
        assertEquals(500, a.status(), a.json());
        assertTrue(a.json().contains("unverified") && a.json().contains("could not follow"), a.json());
        // the partner: the same lambda let-bound and named is followed, so its body is read, and it is clean
        assertTrue(check("{|let f = {s: String[1]|$s->length()}; " + TABLE
                + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}").clean());
    }

    /** A function whose body does not type cannot be read: unverified, never clean. */
    @Test
    void aFunctionWhoseBodyDoesNotTypeIsUnverified() {
        PureMeaningCheck.Result r = check(UNFOLLOWABLE, "|" + TABLE
                + "->extend(~i: r|t::untyped($r.NAME->toOne()))->from(t::RT)");
        assertEquals("unverified=1", r.counts());
        assertTrue(r.sites().get(0).where().startsWith("t::untyped"), r.sites().toString());
    }

    /** A mapping the check cannot find (here, a test-data request's mapping the model does not declare) is
     *  unverified, never clean (the reviewer's S2). */
    @Test
    void aMappingTheModelDoesNotDeclareIsUnverified() {
        ModelContext c = ctx(MODEL);
        TypedQuery q = Compiler.query(c, "|t::Person.all()->project(~[n: p|$p.name])->from(t::M, t::RT)");
        PureMeaningCheck.Result r = PureMeaningCheck.of(q, c, "t::RT", "t::NotInTheModel");
        assertEquals("unverified=1", r.counts());
        assertTrue(PureMeaningCheck.of(q, c, "t::RT", "t::M").clean());
    }

    // ---- no exemption ---------------------------------------------------------------------------------------------

    /** Under CORRECT_SQL_SUBSTRING_INDEXING legend-engine's processSubstring is its processSubstr (pureToSQLQuery.pure:4662),
     *  so a corrected substring agrees with lite in range and differs out of range alone, as substr does: flagged as
     *  corrected, its literal-0 start included. indexOf has no such option (:10644), so it is flagged as before. */
    @Test
    void theCorrectedOptionFlagsSubstringAsCorrected_andIndexOfAsBefore() {
        String calls = "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3), b: r|$r.NAME->toOne()->indexOf('o'),"
                + " c: r|$r.NAME->toOne()->substring(0)])";
        PureMeaningCheck.Result r = check("|" + TABLE + calls + "->from(t::RT)"
                + "->meta::pure::executionPlan::featureFlag::withFeatureFlags("
                + "meta::pure::executionPlan::features::Feature.CORRECT_SQL_SUBSTRING_INDEXING)");
        assertEquals("substring(corrected)/2=1, substring(corrected)/3=1, indexOf/2=1", r.counts());
        PureMeaningCheck.Result plain = check("|" + TABLE + calls + "->from(t::RT)");
        assertEquals("substring/2=1, substring/3=1, indexOf/2=1", plain.counts());
    }

    /** A literal start of 0 is flagged like any other start, in both forms, on every database (the reviewer's M-a1):
     *  in range it gives Pure's answer, but out of range lite raises (S383) where legend-engine answers the database's
     *  SUBSTRING ('abcdef'->substring(0, 9) is 'abcdef'), so the door rewrites it too. */
    @Test
    void aLiteralZeroStartIsFlagged_noCallIsExempt() {
        String zeroStarts = "|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(0, 3),"
                + " b: r|$r.NAME->toOne()->substring(0)])->from(t::RT)";
        assertEquals("substring/2=1, substring/3=1", check(zeroStarts).counts());
        String zeroStart = MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
                "type: DuckDB; specification: DuckDB { }; auth: Test;");
        assertEquals("substring/2=1, substring/3=1", check(zeroStart, zeroStarts).counts());
    }

    // ---- what a refusal names --------------------------------------------------------------------------------------

    @Test
    void theRefusalsCallListIsBounded_codesAndCountsOnly() {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            cols.append(i == 0 ? "" : ", ").append("c").append(i).append(": r|$r.NAME->toOne()->substring(")
                    .append(i + 1).append(", ").append(i + 3).append(")");
        }
        PureMeaningCheck.Result r = check("|" + TABLE + "->extend(~[" + cols + "])->from(t::RT)");
        assertEquals("substring/3=50", r.counts());
        String calls = r.calls();
        assertTrue(calls.contains("and 30 more"), calls);
        assertFalse(calls.contains("NAME"), "a refusal names calls, never the query's text: " + calls);
    }

    // ---- the routes ------------------------------------------------------------------------------------------------

    /** The header the detector once answered with (2026-11-19), gone with the setting (the user's ruling of 2026-11-20
     *  ~07:55): legend-engine sends no such header. */
    private static final String OLD_HEADER = "x-legend-lite-pure-meaning";

    /** A runner that must not run: a refusal happens before the query is planned or run. */
    private static final LiteV1Api.Runner NO_RUN = (query, runtime, output) -> {
        throw new AssertionError("a refused query is never run");
    };

    /** A runner that runs nothing and answers no rows: what these tests read is the route's answer around it. */
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

    /** The model on a database where legend-engine's answer is not yet checked. */
    static final String UNCHECKED = MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
            "type: Postgres; specification: Static { name: 'x'; host: 'h'; port: 5432; };"
                    + " auth: UserNamePassword { baseVaultReference: 'v'; userNameVaultReference: 'u';"
                    + " passwordVaultReference: 'p'; };");

    /** On a database where legend-engine's answer is not yet checked, a flagged query is refused by name, in
     *  legend-engine's envelope, never answered with a guess (the user's ruling of 2026-11-20 ~07:55). */
    @Test
    void aFlaggedQueryOnADatabaseNotYetCheckedIsRefusedByName_inLegendEnginesEnvelope() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(UNCHECKED, PLANTED),
                NO_RUN, LiteV1Api.Models.COMPILED);
        assertEquals(500, a.status(), a.json());
        Json.Obj o = Json.parseObject(a.json());
        assertEquals("COMPILATION", o.getString("errorType"));
        String message = o.getString("message");
        assertTrue(message.contains("legend-engine's answer for substring/3 at 1:") && message.contains("indexOf/2 at 1:")
                && message.contains("is not yet checked on Postgres"), message);
        assertNull(a.headers().get(OLD_HEADER));
    }

    /** A refused request sends nothing to the database: the door's check runs right after the request is typed, before
     *  it is planned or run, on every route whose answer a flagged call chooses (the compat owner's order of ~19:52,
     *  2026-11-19). The runner is the routes' one way to a session, and a refused request never reaches it. */
    @Test
    void aRefusedRequestRunsNoStatement_onEveryRouteWhoseAnswerTheCallChooses() {
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        LiteV1Api.Runner counting = (query, runtime, output) -> runs.incrementAndGet();
        String testData = testDataBody(UNCHECKED, "|t::Person.all()->filter(p|$p.name == 'abcdef'->substring(1, 3))"
                + "->project([p|$p.name], ['n'])");
        Map<String, String> routes = new java.util.LinkedHashMap<>();
        routes.put("/api/pure/v1/execution/execute", body(UNCHECKED, PLANTED));
        routes.put("/api/pure/v1/execution/generatePlan", body(UNCHECKED, PLANTED));
        routes.put("/api/pure/v1/execution/testDataGeneration/generateSeedData", testData);
        routes.put("/api/pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed", testData);
        for (Map.Entry<String, String> route : routes.entrySet()) {
            for (String format : java.util.Arrays.asList(null, "serializationFormat=ARROW_IPC")) {
                LiteV1Api.Answer a = PureV1Routes.route(route.getKey(), format, route.getValue(), counting);
                assertEquals(500, a.status(), route.getKey() + " " + a.json());
                assertTrue(a.json().contains("is not yet checked on Postgres"), route.getKey() + " " + a.json());
            }
        }
        assertEquals(0, runs.get(), "a refused request reached the runner");
        // the partner: the same counting runner IS reached by a clean request, so the zero above is the refusal's
        PureV1Routes.route("/api/pure/v1/execution/execute", null,
                body(UNCHECKED, "|" + TABLE + "->select(~[NAME])->from(t::RT)"), counting);
        assertEquals(1, runs.get(), "a clean request runs");
    }

    private static String testDataBody(String modelText, String lambdaText) {
        String function = PureV1Routes.grammarToJsonLambda(lambdaText, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", modelText);
        return "{\"function\":" + function + ",\"mapping\":\"t::M\",\"runtime\":{\"_type\":\"runtimePointer\","
                + "\"runtime\":\"t::RT\"},\"model\":" + Json.toCompact(model) + ",\"tableRowIdentifiers\":[]}";
    }

    /** On a checked database a flagged query is answered (rewritten: PureMeaningEngineTest reads the answers), and no
     *  answer carries a header of the detector's. */
    @Test
    void aFlaggedQueryOnACheckedDatabaseIsAnswered_withNoHeader() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null, body(PLANTED), NOTHING);
        assertEquals(200, a.status(), a.json());
        assertNull(a.headers().get(OLD_HEADER));
        assertNull(a.headers().get("Access-Control-Expose-Headers"));
    }

    @Test
    void aCleanQueryAnswersAsItDid() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/execute", null,
                body("|" + TABLE + "->select(~[NAME])->from(t::RT)"), NOTHING);
        assertEquals(200, a.status(), a.json());
        assertNull(a.headers().get(OLD_HEADER));
    }

    /** generatePlan plans the rewritten query: its SQL is not the core's plan of the query as written. */
    @Test
    void generatePlanPlansTheRewrittenQuery() {
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/execution/generatePlan", null, body(PLANTED), NO_RUN);
        assertEquals(200, a.status(), a.json());
        String sql = Json.parseObject(a.json()).getObj("rootExecutionNode").getArr("executionNodes").items()
                .stream().map(n -> ((Json.Obj) n).getString("sqlQuery")).findFirst().orElseThrow();
        assertFalse(Compiler.query(ctx(MODEL), PLANTED).plan("t::RT", PureText.PURE_V1).sql().equals(sql), sql);
    }

    @Test
    void theRoutesThatRunNoQueryAreNotChecked() {
        // compile types every body (the constraint and the functions call substring) and is answered as before
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", UNCHECKED);
        LiteV1Api.Answer a = PureV1Routes.route("/api/pure/v1/compilation/compile", null, Json.toCompact(model),
                NO_RUN);
        assertEquals(200, a.status(), a.json());
    }

    /** A check that misses what it must flag fails the first; one that flags what it must not fails the second. */
    @Test
    void theCheckBites_bothWays() {
        List<String> flagged = List.of(
                "|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT)",
                "|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->substr(1, 3))->from(t::RT)",
                "|" + TABLE + "->extend(~a: r|t::firstThree($r.NAME->toOne()))->from(t::RT)",
                "|" + TABLE + "->extend(~a: r|[]->cast(@String)->toOne())->from(t::RT)");
        List<String> quiet = List.of(
                "{|let x = 'abc'->indexOf('b'); " + TABLE + "->filter(r|$r.ID == $x)->from(t::RT);}",
                "|" + TABLE + "->extend(~a: r|['a', 'b']->indexOf('b'))->from(t::RT)",
                "|" + TABLE + "->extend(~a: r|$r.NAME->toOne())->from(t::RT)");
        for (String q : flagged) {
            assertFalse(check(q).clean(), q);
        }
        for (String q : quiet) {
            assertTrue(check(q).clean(), q + " -> " + check(q).counts());
        }
    }
}
