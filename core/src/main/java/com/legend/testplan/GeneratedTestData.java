// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.GeneratorSplice;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testdatagen.TestDataGenerationNatives;
import com.legend.testdatagen.TestDataGenerator;
import com.legend.testdatagen.TestDataPlanner;

import java.util.ArrayList;
import java.util.List;

/**
 * THE TEST-DATA GENERATOR ON THE PROGRAM ROAD (B2d.2 of the total-wall program, 2026-11-11; the design the user approved
 * on 2026-11-10, docs/data-delivery-homework/TEST_DATA_GENERATOR_DESIGN_2026_11_10.md): a test body's
 * {@code generateTestData(...)} PLANNED before the body is classed, by {@link TestDataPlanner} -- one read-only statement
 * whose one cell is the test-data text, and one text per fetch, each written in the body's dialect -- and its reads
 * spliced by the compiler ({@link GeneratorSplice}), so the body is classed and planned as any other. Planning only:
 * nothing is sent, no value evaluated.
 */
final class GeneratedTestData {

    private GeneratedTestData() {
    }

    /** {@code typed} with each test-data generator it binds planned, or {@code typed} itself where it binds none;
     *  statements written in {@code dialect}, the body's. */
    static TypedQuery planned(TypedQuery typed, SqlDialect dialect) {
        if (!GeneratorSplice.holdsAGenerator(typed.body())) {
            return typed;
        }
        ModelContext ctx = typed.context();
        return typed.withBody(GeneratorSplice.spliced(typed.body(), g -> generated(g, ctx, dialect)));
    }

    /** One generator planned: its fetches' texts and its text's statement. */
    private record Generated(List<String> sqls, SqlQuery csv, TestDataPlanner.Planned planned)
            implements GeneratorSplice.Generated {
    }

    /** {@code g} planned by {@link TestDataPlanner}: its arguments after the query, the mapping and the runtime classified
     *  (the row identifiers, the milestoning dates, the hash flag; each a literal, a non-literal refused by name), its
     *  fetches' texts written in {@code dialect}. The runtime argument is the body's: the generator reads the database
     *  the body's target names. */
    private static Generated generated(TypedTestDataGen g, ModelContext ctx, SqlDialect dialect) {
        List<ValueSpecification> ps = g.params();
        LambdaFunction query = (LambdaFunction) ps.get(0);
        String mapping = ((PackageableElementPtr) ps.get(1)).fullPath();
        List<TestDataGenerator.TableRowIds> rowIds = new ArrayList<>();
        TestDataGenerator.MilestoningDates[] dates = new TestDataGenerator.MilestoningDates[1];
        boolean[] hash = new boolean[1];
        for (int i = 3; i < ps.size(); i++) {
            TestDataGenerationNatives.classifyArg(ps.get(i), rowIds, dates, hash);
        }
        TestDataPlanner.Planned p = TestDataPlanner.plan(ctx, query, mapping, rowIds, dates[0], hash[0]);
        List<String> texts = new ArrayList<>(p.fetches().size());
        for (SqlQuery f : p.fetches()) {
            texts.add(dialect.render(f));
        }
        return new Generated(List.copyOf(texts), p.csv(), p);
    }
}
