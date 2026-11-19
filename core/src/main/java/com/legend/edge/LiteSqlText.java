// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.SqlDialect;

/**
 * LITE'S OWN SQL TEXT where the compatibility routes print SQL: {@code toSQLString}, plan text ({@code planToString},
 * an execution plan's SQL nodes and allocations), the DDL natives, an execute's activity SQL, a plan connection's
 * test-data statements and the test-data generator's plan. Each prints what the edge's dialect of the database writes
 * ({@link PureText#dialect}), the SQL lite runs on these routes; a database lite has no dialect for is refused by name
 * there ({@code Databases.dialect}).
 *
 * <p>A2 of the compatibility program (stage A, ratified by the user 2026-11-19; plan slice 2.3a): legend-engine's own
 * SQL spellings, its printers {@code EngineStyleH2}, {@code EngineStyleDB2} and {@code EngineStyleComposite} and their
 * interface {@code EngineTextDialect}, were deleted under CP-4: upstream's SQL text is never promised, so a test that
 * checks SQL text is judged by its answer (the appeal), and a request that asks for SQL text gets lite's.
 *
 * <p>The plan-text family still asks the questions only legend-engine's text answered. Each has its plainest answer
 * here, and none of them writes SQL outside the dialect or edits SQL the dialect wrote:
 * <ul>
 *   <li>{@link #params}: lite's text has no template parameters, so it reads none;</li>
 *   <li>{@link #collectionSplice}: lite has no FreeMarker collection splice, refused by name;</li>
 *   <li>{@link #renderedAlias}: an alias as given;</li>
 *   <li>{@link #selectList}: none (the plan text then prints the whole statement);</li>
 *   <li>{@link #renderAsAliased}: the statement as the dialect writes any other;</li>
 *   <li>{@link #inTempTableSelect}: the dialect's select of the temp table's rows.</li>
 * </ul>
 * The family's code behind those questions is the second half of A2 (its second PR), which deletes them.
 */
final class LiteSqlText {

    private final SqlDialect dialect;

    private LiteSqlText(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /** Lite's text for {@code type}: a fresh dialect per use (a dialect keeps state while it writes a statement). */
    static LiteSqlText of(DatabaseType type) {
        return new LiteSqlText(PureText.dialect(type));
    }

    /** The dialect itself, for a caller that takes one (the test-data statements' printer). */
    SqlDialect dialect() {
        return dialect;
    }

    String render(SqlQuery query) {
        return dialect.render(query);
    }

    String render(SqlDdl ddl) {
        return dialect.render(ddl);
    }

    /** Lite's text has no template parameters ({@link EngineParams}): it reads none. */
    void params(EngineParams params) {
        java.util.Objects.requireNonNull(params, "the plan's template parameters");
    }

    /** Lite has no FreeMarker collection splice: refused by name. */
    String collectionSplice(com.legend.sql.SqlExpr.PlanParam cp) {
        throw new com.legend.error.NotImplementedException("a collection parameter spliced into plan text ("
                + cp.name() + "): lite's SQL has no FreeMarker collection splice (legend-engine's SQL spelling is"
                + " not lite's, CP-4)");
    }

    /** An alias as given: lite's dialect names its own relations. */
    String renderedAlias(String alias) {
        return alias;
    }

    /** None: lite's dialect writes no select list apart from its statement. */
    @com.legend.base.Nullable String selectList(SqlQuery query) {
        java.util.Objects.requireNonNull(query, "the query");
        return null;
    }

    /** The statement as the dialect writes any other. */
    String renderAsAliased(SqlQuery query) {
        return dialect.render(query);
    }

    /** The dialect's select of every row of the temp table {@code tempTable}. */
    String inTempTableSelect(String tempTable) {
        return dialect.render(com.legend.sql.SqlSelect.starOf(
                new com.legend.sql.SqlSource.Table(tempTable, tempTable, java.util.List.of(), false,
                        java.util.Map.of(), com.legend.sql.NameQuoting.BARE)));
    }
}
