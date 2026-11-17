// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.compiler.element.ModelContext;
import com.legend.lineage.ScanRelations;
import com.legend.model.DatabaseDefinition;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/** Lite's own: a timestamp in full, as lite's writers spell a relation's time-bearing cell
 *  ({@code result.CellText}): the product's text of a DateTime, {@code yyyy-MM-ddTHH:mm:ss.ffffffZ} on every
 *  database (register S312; W4, 2026-11-16); a NULL stays NULL. Upstream's (the compatibility rules' and the
 *  server's legend-engine routes', its {@code toCSVString}) is the compatibility edge's, through the plan
 *  style's {@code UpstreamPlans} facet (Phase 6's B13, 2026-11-17). The rest lite's own too (J7/K12 of the
 *  edge audit, 2026-11-17): a starting table with no identifiers refused as {@link TestDataPlanner.NoRowIdentifiers}, the
 *  tables in the walk's order, the columns by their display names, every relation tree admitted.
 *
 * <p>The PRODUCT's form of the two-form kind {@link TestDataPlanner.Printing}, a class of its own beside the kind (the
 * kind declares types only, hook criterion (a): GUARD's C.9 finding, 2026-11-17, moved out of the kind's CLEAN field).
 */
public final class LitePrinting implements TestDataPlanner.Printing {

    /** The product's form. */
    public static final LitePrinting INSTANCE = new LitePrinting();

    private LitePrinting() {
    }

    @Override
    public SqlExpr timestampText(SqlExpr x) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, x),
                new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR, SqlExpr.Reason.DECLARED))),
                SqlExpr.Call.of(SqlFn.DATETIME_TO_TEXT, x), SqlExpr.Reason.DECLARED);
    }

    @Override
    public RuntimeException unidentified(TestDataPlanner.NoRowIdentifiers facts) {
        return facts;
    }

    @Override
    public List<TestDataPlanner.Table> tableOrder(List<TestDataPlanner.Table> tables) {
        return tables;
    }

    @Override
    public List<String> headerOrder(DatabaseDefinition.TableDefinition def, List<String> cols) {
        return TestDataPlanner.headerOrder(cols);
    }

    @Override
    public String headerName(DatabaseDefinition.TableDefinition def, String col) {
        return TestDataPlanner.headerCase(col);
    }

    @Override
    public void checkTree(ModelContext ctx, List<ScanRelations.Rel> roots) {
        // every relation tree admitted: a view's own fetch is planned (Views)
    }
}
