// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * THE STAMP AND THE TABLE, IN SHADOW BESIDE THE A3 REWRITE (the edition program's step 2a;
 * docs/records/edition-plan-to-finish-stage-b-2026-11-25.md section 5): for each query the A3 rewrite's own tests
 * plant, the SQL planned through the rewrite ({@link PureMeaningRewrite}, legend-engine's meaning by substitution at
 * the door) and the SQL planned through legend-engine's table ({@link EngineTable}) with the planner's stamp must be
 * the same statement. Where they are, the stamp marks the same code the rewrite's walk does: a {@code let} that reads
 * no store keeps Pure's meaning, store-bound code gets the database's, a function called on both sides gets each.
 */
class EngineTableShadowTest {

    private static final String TABLE = PureMeaningCheckTest.TABLE;

    /** pure/v1's style, handing the lowering legend-engine's table. */
    private static final PlanStyle TABLED = new PlanStyle() {
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
            return kind == com.legend.lowering.EditionTable.class ? kind.cast(EngineTable.INSTANCE)
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

    /** The queries of {@code PureMeaningRewriteTest} and {@code PureMeaningCheckTest} that the rewrite answers (none
     *  it refuses): a store-bound call, a host-side let, both in one query, a function called on both sides, a lambda
     *  let shared by the store and the host, a qualified property, a let that reads a store read again by a statement
     *  of its own. */
    private static final List<String> QUERIES = List.of(
            PureMeaningCheckTest.PLANTED,
            "|" + TABLE + "->select(~[NAME])->from(t::RT)",
            "{|let x = 'abcdef'->substring(1, 3); " + TABLE + "->filter(r|$r.NAME == $x)->from(t::RT);}",
            "{|let x = 'abcdef'->substring(1, 3); " + TABLE
                    + "->filter(r|$r.NAME == $x)->extend(~a: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT);}",
            "{|let x = t::firstThree('abcdef'); " + TABLE
                    + "->filter(r|$r.NAME == $x)->extend(~a: r|t::firstThree($r.NAME->toOne()))->from(t::RT);}",
            "|" + TABLE + "->extend(~[a: r|t::firstThree($r.NAME->toOne()), b: r|t::position($r.NAME->toOne())])"
                    + "->from(t::RT)",
            "{|let f = {s: String[1]|$s->indexOf('x')}; let n = $f->eval('abcx'); "
                    + TABLE + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}",
            "{|let x = 'abc'->indexOf('b'); " + TABLE + "->filter(r|$r.ID == $x)->from(t::RT);}",
            "{|let d = t::Person.all()->filter(p|$p.name == 'abcdef')->from(t::M, t::RT)"
                    + "->toOne().name->substring(1, 3); $d->toUpper();}");

    private static String throughTheRewrite(ModelContext c, String query) {
        TypedQuery q = EngineComparisons.asTheDatabases(Compiler.query(c, query));
        return PureMeaningRewrite.rewrite(q, c, "t::RT", null, PureMeaningRewrite::engine).query()
                .plan("t::RT", PureText.PURE_V1).sql();
    }

    private static String throughTheTable(ModelContext c, String query) {
        return EngineComparisons.asTheDatabases(Compiler.query(c, query)).plan("t::RT", TABLED).sql();
    }

    @Test
    void theTableAndTheStampWriteTheStatementTheRewriteWrites() {
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        List<String> differing = new ArrayList<>();
        for (String query : QUERIES) {
            String rewrite = throughTheRewrite(c, query);
            String table = throughTheTable(c, query);
            if (!rewrite.equals(table)) {
                differing.add(query + "\n  the rewrite: " + rewrite + "\n  the table:   " + table);
            }
        }
        assertEquals(List.of(), differing, "the stamp and the rewrite place a call differently");
    }

    /** The table does something: a store-bound substring is the database's own, not Pure's. */
    @Test
    void aStoreBoundCallIsTheDatabases() {
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        String tabled = throughTheTable(c, PureMeaningCheckTest.PLANTED);
        String untouched = Compiler.query(c, PureMeaningCheckTest.PLANTED).plan("t::RT", PureText.PURE_V1).sql();
        assertNotEquals(untouched, tabled);
    }

    /** The stamp does something: a host-side let's substring is Pure's, the statement the door plans with no table. */
    @Test
    void aHostSideLetKeepsPuresMeaning() {
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        String query = "{|let x = 'abcdef'->substring(1, 3); " + TABLE + "->filter(r|$r.NAME == $x)->from(t::RT);}";
        assertEquals(Compiler.query(c, query).plan("t::RT", PureText.PURE_V1).sql(), throughTheTable(c, query));
    }
}
