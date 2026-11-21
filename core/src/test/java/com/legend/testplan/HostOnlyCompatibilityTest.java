// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.TypedQuery;
import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE HOST JUDGE IS THE ONLY JUDGE IN COMPATIBILITY, A GUARD SHOWN TO CATCH A PLANT (rule 4 of the compatibility
 * program's rules of record, 2026-11-19; A4 of the compatibility program, stage A ratified by the user 2026-11-19).
 * {@code TestPlanner.refuseTheDatabaseJudgeInCompatibility} refuses a database-judge verdict for any style but the
 * product's. The plant is a style that answers exactly as the product's does but is not it -- an audience other than
 * lite's, standing in for legend-pure's or legend-engine's: under the host judge the body plans as the product's
 * would; under the database judge, or both, it is refused by name. The product's style plans every judge: both run in
 * the product.
 */
class HostOnlyCompatibilityTest {

    private static final String MODEL = """
            Class x::Firm { name: String[1]; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::Conn
            {
                store: x::DB;
                type: H2;
                specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME\\n1,Acme\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
            """;

    private static final String BODY = "|meta::pure::functions::asserts::assertEquals(1, x::Firm.all()->size())";

    /** THE PLANT: a style that is not the product's, every answer the product's. */
    private static final PlanStyle PLANTED = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return PlanStyle.PRODUCT.dialect(type);
        }

        @Override
        public @com.legend.base.Nullable TraceCell traceCell() {
            return PlanStyle.PRODUCT.traceCell();
        }

        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            return PlanStyle.PRODUCT.twoForm(kind, productForm);
        }

        @Override
        public String graphKeyDate(String written) {
            return PlanStyle.PRODUCT.graphKeyDate(written);
        }

        @Override
        public String graphKeyString(String value) {
            return PlanStyle.PRODUCT.graphKeyString(value);
        }

        @Override
        public <F> F facet(Class<F> kind) {
            return PlanStyle.PRODUCT.facet(kind);
        }

        @Override
        public boolean judgesEngineText() {
            return PlanStyle.PRODUCT.judgesEngineText();
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            return PlanStyle.PRODUCT.facet(kind, refusal);
        }
    };

    private static TypedQuery body() {
        return Compiler.query(Compiler.compileModel(MODEL), BODY);
    }

    private static com.legend.testexecutionplan.TestPlan plan(PlanStyle style, PlanningOptions.JudgeMode judge) {
        return TestExecution.planBody(body(), "x::RT", ExecuteOptions.NONE.withStyle(style).withJudgeMode(judge));
    }

    @Test
    void aCompatibilityAudienceIsJudgedByTheHostAlone() {
        assertNotNull(plan(PLANTED, PlanningOptions.JudgeMode.HOST), "the host judge plans for any audience");
        for (PlanningOptions.JudgeMode judge : new PlanningOptions.JudgeMode[] {PlanningOptions.JudgeMode.DATABASE,
                PlanningOptions.JudgeMode.BOTH}) {
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> plan(PLANTED, judge),
                    "a compatibility audience planned a database-judge verdict under " + judge);
            assertTrue(refused.getMessage().startsWith("the host judge is the only judge in compatibility (rule 4"),
                    refused.getMessage());
        }
    }

    @Test
    void theProductPlansBothJudges() {
        for (PlanningOptions.JudgeMode judge : PlanningOptions.JudgeMode.values()) {
            assertNotNull(plan(PlanStyle.PRODUCT, judge), "the product plans the " + judge + " judge");
        }
    }
}
