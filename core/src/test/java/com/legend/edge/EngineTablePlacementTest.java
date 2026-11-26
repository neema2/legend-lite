// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.PlanStyle;
import com.legend.error.NotImplementedException;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WHERE LEGEND-ENGINE'S TABLE LANDS (the edition program's step 2, 2026-11-25;
 * docs/records/edition-plan-to-finish-stage-b-2026-11-25.md sections 2.2 and 2.3). At legend-engine's door a call means
 * the database's own function where legend-engine sends it to the database, and legend-pure's where legend-engine's
 * own Java runs it. The lowering's table ({@link EngineTable}) holds the rows; the planner's stamp
 * ({@code TypedNativeCall.outsideStore}) says which code is outside any store query. Each case here plans a query in the
 * door's style and reads its SQL against the same query planned in the door's style with the product's table in
 * legend-engine's place: the same statement means no row applied (Pure's meaning), another means one did.
 *
 * <p>The cases are the ones the door's rewrite of the typed query was held to until this step (A3's tests,
 * PureMeaningCheckTest and PureMeaningRewriteTest, deleted with it): while both stood, the table and the rewrite were
 * planned side by side over them and wrote the same statement for each. Three kinds of code the rewrite refused by name
 * are answered now, because everything that becomes SQL passes the lowering: a graph fetch's qualified property, a
 * model-to-model mapping's transforms in a TDS query over a chain, and a function value read out of a collection.
 */
class EngineTablePlacementTest {

    /** A relational model over H2: a table, a class mapped to it, two qualified properties, two user functions, a
     *  constraint, a model-to-model mapping over the class (whose own class has a qualified property), a runtime on the
     *  store and one on a model connection. */
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
            "Class t::Out { short: String[1]; where() { $this.short->indexOf('b') }: Integer[1]; }",
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

    /** A store-bound {@code substring} and {@code indexOf}: two rows of the table. */
    static final String PLANTED = "|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3),"
            + " b: r|$r.NAME->toOne()->indexOf('o')])->from(t::RT)";

    /** The model on a database where legend-engine's answer is not yet checked. */
    static final String UNCHECKED = MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
            "type: Postgres; specification: Static { name: 'x'; host: 'h'; port: 5432; };"
                    + " auth: UserNamePassword { baseVaultReference: 'v'; userNameVaultReference: 'u';"
                    + " passwordVaultReference: 'p'; };");

    static final String CORRECTED = "->meta::pure::executionPlan::featureFlag::withFeatureFlags("
            + "meta::pure::executionPlan::features::Feature.CORRECT_SQL_SUBSTRING_INDEXING)";

    /** The door's style with the product's edition in legend-engine's place: the door's own plan of a query as written,
     *  every call by its own rule. What a row's effect is read against. */
    private static final PlanStyle AS_WRITTEN = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return PureText.PURE_V1.dialect(type);
        }

        @Override
        public TraceCell traceCell() {
            return PureText.PURE_V1.traceCell();
        }

        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            return kind == com.legend.planner.Edition.class ? productForm
                    : PureText.PURE_V1.twoForm(kind, productForm);
        }

        @Override
        public String graphKeyDate(String written) {
            return PureText.PURE_V1.graphKeyDate(written);
        }

        @Override
        public String graphKeyString(String value) {
            return PureText.PURE_V1.graphKeyString(value);
        }

        @Override
        public <F> F facet(Class<F> kind) {
            return PureText.PURE_V1.facet(kind);
        }

        @Override
        public boolean judgesEngineText() {
            return PureText.PURE_V1.judgesEngineText();
        }

        @Override
        public <F> F facet(Class<F> kind, Supplier<? extends RuntimeException> refusal) {
            return PureText.PURE_V1.facet(kind, refusal);
        }
    };

    private static final ModelContext MODELLED = Compiler.compileModel(MODEL);

    private static String cut(String call) {
        return "|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->" + call + ")->select(~[a])->from(t::RT)";
    }

    /** {@code query} planned as the door plans it. */
    private static String engines(ModelContext c, String query, String runtime) {
        return PureV1Routes.asLegendEngine(Compiler.query(c, query)).plan(runtime, PureText.PURE_V1).sql();
    }

    private static String engines(String query) {
        return engines(MODELLED, query, "t::RT");
    }

    /** {@code query} planned in the door's style with no row: every call by its own rule. */
    private static String asWritten(ModelContext c, String query, String runtime) {
        return PureV1Routes.asLegendEngine(Compiler.query(c, query)).plan(runtime, AS_WRITTEN).sql();
    }

    private static String asWritten(String query) {
        return asWritten(MODELLED, query, "t::RT");
    }

    // ---- inside a store query: the database's own function ---------------------------------------------------------

    /** Code legend-engine sends to the database, each case a query the door's rewrite answered the same way. */
    private static final Map<String, String> STORE_BOUND = new LinkedHashMap<>();

    static {
        STORE_BOUND.put("substring and indexOf", PLANTED);
        STORE_BOUND.put("substring, two arguments", cut("substring(2)"));
        STORE_BOUND.put("substr, three arguments", cut("substr(1, 3)"));
        STORE_BOUND.put("substr, two arguments", cut("substr(2)"));
        STORE_BOUND.put("the corrected substring", cut("substring(1, 3)") + CORRECTED);
        STORE_BOUND.put("the corrected substring, two arguments", cut("substring(2)") + CORRECTED);
        STORE_BOUND.put("a literal start of 0", cut("substring(0, 3)"));
        STORE_BOUND.put("a literal start of 0, two arguments", cut("substring(0)"));
        STORE_BOUND.put("a computed index", cut("substr(1, $r.NAME->toOne()->length() + 1)"));
        STORE_BOUND.put("toOne of an operand that is always empty",
                "|" + TABLE + "->limit(1)->extend(~o: r|[]->cast(@Integer)->toOne())->select(~[o])->from(t::RT)");
        STORE_BOUND.put("a literal inside the routed query",
                "|" + TABLE + "->limit(1)->extend(~s: r|'abcdef'->substring(1, 3))->from(t::RT)");
        STORE_BOUND.put("a user function the query calls", "|" + TABLE
                + "->extend(~[a: r|t::firstThree($r.NAME->toOne()), b: r|t::position($r.NAME->toOne())])->from(t::RT)");
        STORE_BOUND.put("a qualified property in a TDS query",
                "|t::Person.all()->project(~[i: p|$p.initials()])->from(t::M, t::RT)");
        STORE_BOUND.put("a class's projected substring", "|t::Person.all()->filter(p|$p.name == 'abcdef')"
                + "->project([p|$p.name->substring(1, 3)], ['d'])->from(t::M, t::RT)");
        STORE_BOUND.put("a class's filtering substring", "|t::Person.all()->filter(p|$p.name->substring(1, 3) == 'abc')"
                + "->project([p|$p.name], ['n'])->from(t::M, t::RT)");
        STORE_BOUND.put("a class's projected indexOf",
                "|t::Person.all()->project([p|$p.name->indexOf('c')], ['i'])->from(t::M, t::RT)");
        STORE_BOUND.put("a let-bound lambda evaluated in the store query", "{|let f = {s: String[1]|$s->indexOf('x')}; "
                + TABLE + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        STORE_BOUND.put("a let that reads a store", "{|let t = " + TABLE
                + "->extend(~a: r|$r.NAME->toOne()->substring(1, 3)); $t->select(~[a])->from(t::RT);}");
        STORE_BOUND.put("a value let that reads a store, read by a statement of its own",
                "{|let d = t::Person.all()->filter(p|$p.name == 'abcdef')->from(t::M, t::RT)"
                        + "->toOne().name->substring(1, 3); $d->toUpper();}");
        // answered since the table: the rewrite refused these three by name
        STORE_BOUND.put("a graph fetch's qualified property",
                "|t::Person.all()->graphFetch(#{t::Person{name,initials,firstC}}#)"
                        + "->serialize(#{t::Person{name,initials,firstC}}#)->from(t::M, t::RT)");
        STORE_BOUND.put("a model-to-model transform in a TDS query over a chain", "|t::Out.all()->project(~[s: o|$o.short])"
                + "->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
        STORE_BOUND.put("a function value read out of a collection", "|" + TABLE
                + "->extend(~i: r|[{s: String[1]|$s->indexOf('x')}]->at(0)->eval($r.NAME->toOne()))->from(t::RT)");
        // the independent audit's cases (docs/records/edition-homework/r3/audit-step2-3.md)
        STORE_BOUND.put("a let bound to a function value that is no written lambda, applied in the store query (F1)",
                "{|let f = [{s: String[1]|$s->indexOf('x')}]->toOne(); " + TABLE
                        + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        STORE_BOUND.put("a let that builds on a relation not yet executed (F3)", "{|let t = " + TABLE
                + "; let u = $t->extend(~a: r|$r.NAME->toOne()->substring(1, 3)); $u->select(~[a])->from(t::RT);}");
    }

    @Test
    void codeLegendEngineSendsToTheDatabaseIsTheDatabasesOwn() {
        for (Map.Entry<String, String> e : STORE_BOUND.entrySet()) {
            assertNotEquals(asWritten(e.getValue()), engines(e.getValue()), e.getKey() + ": no row applied");
        }
    }

    /** The rows by what they write on H2: the database's SUBSTRING over the arguments as given, its position function,
     *  its SUBSTRING over legend-engine's corrected positions with no range check, and no check on toOne of empty. */
    @Test
    void eachRowWritesTheDatabasesOwnCall() {
        String planted = engines(PLANTED);
        assertTrue(planted.contains("substr(\"t0\".NAME, 1, 3)") && planted.contains("LOCATE('o', \"t0\".NAME)"), planted);
        String substr = engines(cut("substr(1, 3)")).toLowerCase(java.util.Locale.ROOT);
        assertTrue(substr.matches("(?s).*substr\\(\"t0\"\\.name, \\(?1 \\+ 1\\)?, \\(?3 - 1\\)?\\).*")
                && !substr.contains("case"), substr);
        String corrected = engines(cut("substring(1, 3)") + CORRECTED).toLowerCase(java.util.Locale.ROOT);
        assertTrue(corrected.matches("(?s).*substr\\(\"t0\"\\.name, \\(?1 \\+ 1\\)?, \\(?3 - 1\\)?\\).*")
                && !corrected.contains("case"), corrected);
        String toOne = engines(STORE_BOUND.get("toOne of an operand that is always empty"));
        assertFalse(toOne.contains("LG#"), toOne);
        assertTrue(asWritten(STORE_BOUND.get("toOne of an operand that is always empty")).contains("LG#"));
        String graph = engines(STORE_BOUND.get("a graph fetch's qualified property"));
        assertTrue(graph.contains("substr(\"t0\".NAME, 1, 2)") && graph.contains("LOCATE('c', \"t0\".NAME)"), graph);
    }

    // ---- outside any store query: legend-pure's meaning -------------------------------------------------------------

    /** Code legend-engine's own Java runs, and calls the table has no row for: the statement the door plans with no
     *  table at all. */
    private static final Map<String, String> AS_PURE = new LinkedHashMap<>();

    static {
        AS_PURE.put("a clean query", "|" + TABLE + "->select(~[NAME])->from(t::RT)");
        AS_PURE.put("a host-side let's substring",
                "{|let x = 'abcdef'->substring(1, 3); " + TABLE + "->filter(r|$r.NAME == $x)->from(t::RT);}");
        AS_PURE.put("a host-side let's substr",
                "{|let x = 'abcdef'->substr(1, 3); " + TABLE + "->filter(r|$r.NAME == $x)->from(t::RT);}");
        AS_PURE.put("a host-side let's indexOf",
                "{|let x = 'abc'->indexOf('b'); " + TABLE + "->filter(r|$r.ID == $x)->from(t::RT);}");
        AS_PURE.put("a host-side let's toOne of empty",
                "{|let x = []->cast(@Integer)->toOne(); " + TABLE + "->filter(r|$r.ID == $x)->from(t::RT);}");
        AS_PURE.put("a host-side let's call of a user function",
                "{|let x = t::firstThree('abcdef'); " + TABLE + "->filter(r|$r.NAME == $x)->from(t::RT);}");
        AS_PURE.put("the three-argument indexOf",
                "|" + TABLE + "->extend(~[b: r|$r.NAME->toOne()->indexOf('o', 1)])->from(t::RT)");
        AS_PURE.put("a toOne of a value that may be empty, and of one that is exactly one",
                "|" + TABLE + "->extend(~[b: r|$r.NAME->toOne(), c: r|$r.ID->toOne()])->from(t::RT)");
        AS_PURE.put("a class read whose constraint calls substring",
                "|t::Person.all()->project(~[n: p|$p.name])->from(t::M, t::RT)");
        AS_PURE.put("a model-to-model transform under a graph fetch", "|t::Out.all()->graphFetch(#{t::Out{short}}#)"
                + "->serialize(#{t::Out{short}}#)->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
        // the independent audit's cases
        AS_PURE.put("a qualified property a graph fetch reads off a class a model-to-model mapping makes (F4)",
                "|t::Out.all()->graphFetch(#{t::Out{short,where}}#)->serialize(#{t::Out{short,where}}#)"
                        + "->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)");
    }

    @Test
    void codeLegendEnginesOwnJavaRunsKeepsLegendPuresMeaning() {
        for (Map.Entry<String, String> e : AS_PURE.entrySet()) {
            assertEquals(asWritten(e.getValue()), engines(e.getValue()), e.getKey());
        }
    }

    /** A model-to-model mapping over a model connection, with no chain to a store: legend-engine runs it in Java. */
    @Test
    void anUnchainedModelToModelTransformKeepsLegendPuresMeaning() {
        String query = "|t::Out.all()->project(~[s: o|$o.short])->from(t::M2, t::JsonRT)";
        assertEquals(asWritten(MODELLED, query, "t::JsonRT"), engines(MODELLED, query, "t::JsonRT"));
    }

    // ---- both in one query ------------------------------------------------------------------------------------------

    /** A host-side let beside a store-bound call, and one function called on both sides: each call by where it is. */
    @Test
    void oneQuerysCallsAreEachPlacedByWhereTheyAre() {
        String mixed = engines("{|let x = 'abcdef'->substring(1, 3); " + TABLE
                + "->filter(r|$r.NAME == $x)->extend(~a: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT);}");
        assertTrue(mixed.contains("LOCATE('o', \"t0\".NAME)"), mixed);
        assertFalse(mixed.contains("substr('abcdef', 1, 3)"), mixed);
        String both = engines("{|let x = t::firstThree('abcdef'); " + TABLE
                + "->filter(r|$r.NAME == $x)->extend(~a: r|t::firstThree($r.NAME->toOne()))->from(t::RT);}");
        assertTrue(both.contains("substr(\"t0\".NAME, 1, 3)"), both);
        assertFalse(both.contains("substr('abcdef', 1, 3)"), both);
        // a let-bound lambda the store query evaluates and a host-side let evaluates too
        String shared = engines("{|let f = {s: String[1]|$s->indexOf('x')}; let n = $f->eval('abcx'); " + TABLE
                + "->filter(r|$r.ID == $n)->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        // the store's use is the database's position, 1-based; the host's is legend-pure's, 0-based
        assertTrue(shared.contains("LOCATE('x', \"t0\".NAME) AS \"i\""), shared);
        assertTrue(shared.contains("LOCATE('x', 'abcx') - 1"), shared);
    }

    /** A function value a let holds is placed where it is applied (the audit's F1): in the store query the database's
     *  position, in a host-side let legend-pure's. */
    @Test
    void aFunctionValueIsPlacedWhereItIsApplied() {
        String shared = engines("{|let f = [{s: String[1]|$s->indexOf('x')}]->toOne(); let n = $f->eval('abcx'); "
                + TABLE + "->filter(r|$r.ID == $n)->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}");
        assertTrue(shared.contains("LOCATE('x', \"t0\".NAME) AS \"i\""), shared);
        assertTrue(shared.contains("LOCATE('x', 'abcx') - 1"), shared);
    }

    /** A checked graph fetch reads its constraints in legend-engine's own Java and its tree's qualified property in
     *  the database: one statement, each by where it runs. Planned on the model's second database, whose dialect
     *  writes a checked fetch's defects. */
    @Test
    void aCheckedGraphFetchsConstraintAndItsQualifiedPropertyAreEachPlaced() {
        ModelContext c = Compiler.compileModel(MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
                "type: DuckDB; specification: DuckDB { }; auth: Test;"));
        // the constraint alone: its substring(1, 2) keeps legend-pure's form, the statement the door plans with no row
        String constraintOnly = "|t::Person.all()->graphFetchChecked(#{t::Person{name}}#)"
                + "->serialize(#{t::Person{name}}#)->from(t::M, t::RT)";
        assertEquals(asWritten(c, constraintOnly, "t::RT"), engines(c, constraintOnly, "t::RT"));
        // with the tree's qualified property beside it: a row applies to the property, and to it alone
        String checked = "|t::Person.all()->graphFetchChecked(#{t::Person{name,initials}}#)"
                + "->serialize(#{t::Person{name,initials}}#)->from(t::M, t::RT)";
        assertNotEquals(asWritten(c, checked, "t::RT"), engines(c, checked, "t::RT"));
        // (were the constraint's call given the row too, the two statements would differ by two calls: the fetch
        // without the property shows the constraint's alone is not)
    }

    /** A runtime that binds model data alone names no database of legend-engine's (legend-engine runs the whole query
     *  in its own Java): a call a row would answer in the query's own code is refused by name, as before the table (the
     *  audit's F2); the mapping's transforms, stamped, are not. */
    @Test
    void onARuntimeOfModelDataAloneAQuerysOwnCallIsRefused() {
        NotImplementedException refused = assertThrows(NotImplementedException.class, () -> engines(MODELLED,
                "|t::Out.all()->project(~[s: o|$o.short->substring(0, 1)])->from(t::M2, t::JsonRT)", "t::JsonRT"));
        assertTrue(refused.getMessage().contains("legend-engine's answer for substring/3")
                && refused.getMessage().contains("a runtime with no single database"), refused.getMessage());
    }

    /** Each row's rule lowers each overload its row maps: a rule reads its call's operands by position, so a rule
     *  handed an overload it was not written for would fail here, where the statement is planned (the independent
     *  audit of 2026-11-26, finding 7; until that day a row named a native of the call's own signature, and a test
     *  compared the two signatures). */
    @Test
    void eachRowsRuleLowersEachOverloadItMaps() {
        for (String call : List.of("$r.NAME->toOne()->substring(1)", "$r.NAME->toOne()->substring(1, 3)",
                "$r.NAME->toOne()->substr(1)", "$r.NAME->toOne()->substr(1, 3)", "$r.NAME->toOne()->indexOf('o')",
                "($r.ID->toOne() / 3)->round()", "($r.ID->toOne() / 3)->round(1)",
                "$r.ID->toOne()->toDecimal()->round(1)", "($r.ID->toOne() / 3)->toDecimal()",
                "$r.NAME->toOne()->parseInteger()", "$r.NAME->toOne()->parseFloat()",
                "[]->cast(@Integer)->toOne()")) {
            String store = "|" + TABLE + "->extend(~a: r|" + call + ")->from(t::RT)";
            assertNotEquals(asWritten(store), engines(store), store);
        }
    }

    /** {@code round} is a row (register S422): inside a store query the database's own ROUND, by the row's rule; in
     *  a host-side let, which legend-engine's own Java computes, the call's own rule, Pure's. */
    @Test
    void roundIsTheDatabasesOwnInsideAStoreQuery_andItsOwnRuleOutside() {
        for (String rounded : List.of("round()", "round(1)")) {
            String store = "|" + TABLE + "->extend(~a: r|($r.ID->toOne() / 3)->" + rounded + ")->from(t::RT)";
            assertNotEquals(asWritten(store), engines(store), store);
            String host = "{|let x = (7 / 2)->" + rounded + "; " + TABLE + "->filter(r|$r.ID > $x)->from(t::RT);}";
            assertEquals(asWritten(host), engines(host), host);
        }
        // where legend-engine's answer is not checked, the row refuses by name
        NotImplementedException refused = assertThrows(NotImplementedException.class, () -> engines(
                Compiler.compileModel(UNCHECKED),
                "|" + TABLE + "->extend(~a: r|($r.ID->toOne() / 3)->round())->from(t::RT)", "t::RT"));
        assertTrue(refused.getMessage().contains("legend-engine's answer for round/1"), refused.getMessage());
    }

    /** The plain parses are rows that answer on every database (register S424; 2026-11-26): inside a store query the
     *  database's own cast, with no check of the text; in a host-side let, which legend-engine's own Java computes,
     *  the checked parse; and on a database whose other rows are not yet checked, the cast all the same -- the cast
     *  is legend-engine's on each by what it writes. */
    @Test
    void thePlainParsesAreTheDatabasesOwnCastInsideAStoreQuery_onEveryDatabase() {
        ModelContext unchecked = Compiler.compileModel(UNCHECKED);
        for (String parse : List.of("parseInteger()", "parseFloat()")) {
            String store = "|" + TABLE + "->extend(~a: r|$r.NAME->toOne()->" + parse + ")->from(t::RT)";
            assertNotEquals(asWritten(store), engines(store), store);
            assertFalse(engines(store).contains("LG#"), engines(store));
            assertTrue(asWritten(store).contains("LG#"), asWritten(store));
            String host = "{|let x = '12'->" + parse + "; " + TABLE + "->filter(r|$r.ID > $x)->from(t::RT);}";
            assertEquals(asWritten(host), engines(host), host);
            assertFalse(engines(unchecked, store, "t::RT").contains("LG#"), store);
        }
    }

    /** A check legend-engine's SQL does not write (a zero divisor's) is written where legend-engine's own Java runs the
     *  call: a host-side let raises as Pure does, a store-bound call is the plain operation (the stamp reaches the
     *  raise choice; the edition program's step 3). */
    @Test
    void aCheckLegendEnginesSqlDoesNotWriteIsWrittenOutsideAStoreQuery() {
        String host = "{|let x = 5->rem(0); " + TABLE + "->filter(r|$r.ID == $x)->from(t::RT);}";
        assertTrue(engines(host).contains("LG#"), engines(host));
        String store = "|" + TABLE + "->extend(~m: r|$r.ID->toOne()->rem(0))->from(t::RT)";
        assertFalse(engines(store).contains("LG#"), engines(store));
        // the product's edition writes the check in both places
        assertTrue(asWritten(host).contains("LG#") && asWritten(store).contains("LG#"), asWritten(store));
    }

    /** The same for each kind of check the lowering asks the table about with the call (the independent audit of
     *  2026-11-26, finding 5): a square root of a negative, a zero divisor -- written in a host-side let, where
     *  legend-engine's own Java raises. (A shift, a range's zero step and a {@code toOne} over several are not run
     *  here: this model's database has no bit operation and no list encoding.) */
    @Test
    void eachCheckLegendEnginesSqlDoesNotWriteIsWrittenInAHostSideLet() {
        for (String value : List.of("(0.0 - 4.0)->sqrt()->toString()->length()", "(7 / 0)->toString()->length()")) {
            String host = "{|let x = " + value + "; " + TABLE + "->extend(~v: r|$x)->from(t::RT);}";
            assertTrue(engines(host).contains("LG#"), value + ": " + engines(host));
        }
        // inside the store query the plain operation, as before: the division over a row's value
        for (String cell : List.of("7 / $r.ID->toOne()")) {
            String store = "|" + TABLE + "->extend(~v: r|" + cell + ")->from(t::RT)";
            assertFalse(engines(store).contains("LG#"), cell + ": " + engines(store));
            assertTrue(asWritten(store).contains("LG#"), cell + ": " + asWritten(store));
        }
    }

    // ---- a database where legend-engine's answer is not yet checked -------------------------------------------------

    /** A call a row would answer is refused by name while the statement is planned, never answered with a guess; a
     *  query no row answers plans as it did. */
    @Test
    void onADatabaseNotYetCheckedACallARowAnswersIsRefusedByName() {
        ModelContext unchecked = Compiler.compileModel(UNCHECKED);
        Map<String, String> named = new LinkedHashMap<>();
        named.put(PLANTED, "substring/3 at 1:");
        named.put(cut("substring(2)"), "substring/2 at 1:");
        named.put(cut("substr(1, 3)"), "substr/3 at 1:");
        named.put(cut("substring(1, 3)") + CORRECTED, "substring(corrected)/3 at 1:");
        named.put("|" + TABLE + "->extend(~b: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT)", "indexOf/2 at 1:");
        named.put("|" + TABLE + "->limit(1)->extend(~o: r|[]->cast(@Integer)->toOne())->from(t::RT)", "toOne(empty)");
        for (Map.Entry<String, String> e : named.entrySet()) {
            NotImplementedException refused = assertThrows(NotImplementedException.class,
                    () -> engines(unchecked, e.getKey(), "t::RT"), e.getKey());
            assertTrue(refused.getMessage().contains("legend-engine's answer for " + e.getValue())
                    && refused.getMessage().contains("is not yet checked on Postgres"), refused.getMessage());
            assertFalse(refused.getMessage().contains("NAME"), "a refusal names calls, never the query's text");
        }
        for (String quiet : List.of("|" + TABLE + "->select(~[NAME])->from(t::RT)",
                "{|let x = 'abcdef'->substring(1, 3); " + TABLE + "->filter(r|$r.NAME == $x)->from(t::RT);}",
                "|" + TABLE + "->extend(~[b: r|$r.NAME->toOne()->indexOf('o', 1)])->from(t::RT)")) {
            assertEquals(asWritten(unchecked, quiet, "t::RT"), engines(unchecked, quiet, "t::RT"), quiet);
        }
    }

    /** The door's edition answers no row itself: the planner makes the table of a statement's database from it. */
    @Test
    void theTableIsMadeForItsStatementsDatabase() {
        com.legend.compiler.spec.typed.TypedNativeCall[] call = new com.legend.compiler.spec.typed.TypedNativeCall[1];
        find(Compiler.query(MODELLED, PLANTED).body(), call);
        // the database the model's connection declares (read, never named here: the dialect owners name databases)
        DatabaseType declared = MODELLED.findConnection("t::Conn").orElseThrow().databaseType();
        assertThrows(IllegalStateException.class, () -> EngineTable.DOOR.lowersAs(call[0], java.util.Set.of()));
        assertEquals(com.legend.lowering.NamedRule.SUBSTRING_AS_GIVEN,
                EngineTable.DOOR.table(declared).lowersAs(call[0], java.util.Set.of()));
        assertEquals(null, EngineTable.DOOR.table(declared).lowersAs(call[0].stampedOutsideStore(true),
                java.util.Set.of()));
        // no database named: refused, as on one not yet checked
        assertThrows(NotImplementedException.class,
                () -> EngineTable.DOOR.table(null).lowersAs(call[0], java.util.Set.of()));
    }

    /** A function whose body does not type is no concern of a query that does not call it (the door's rewrite read
     *  every function a query could reach, and reported one it could not type; the lowering meets only what the query
     *  becomes). */
    @Test
    void aFunctionWhoseBodyDoesNotTypeIsNoConcernOfAQueryThatDoesNotCallIt() {
        ModelContext c = Compiler.compileModel(MODEL + String.join("\n",
                "###Pure",
                "function t::untyped(s: String[1]): Integer[1] { $s->substring(1, 2) }",
                ""));
        assertEquals(engines(PLANTED), engines(c, PLANTED, "t::RT"));
    }

    private static void find(List<com.legend.compiler.spec.typed.TypedSpec> nodes,
            com.legend.compiler.spec.typed.TypedNativeCall[] out) {
        for (com.legend.compiler.spec.typed.TypedSpec n : nodes) {
            if (out[0] == null && n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && com.legend.builtin.Pure.AT_STRING_SUBSTRING.contains(c.callee().id())) {
                out[0] = c;
            }
            find(n.children(), out);
        }
    }
}
