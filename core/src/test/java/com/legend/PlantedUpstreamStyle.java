// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * A PLANTED VIOLATOR, a test class for {@code ArchitectureTest.theHookRuleBitesAPlantedStyle} alone: a plan style
 * outside {@code com.legend.edge}, which the hook rule ({@code ArchitectureTest.onlyTheEdgeImplementsTheHooksUpstreamSide})
 * names. Never used.
 */
final class PlantedUpstreamStyle implements PlanStyle {

    @Override
    public SqlDialect dialect(DatabaseType type) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public @com.legend.base.Nullable TraceCell traceCell() {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public java.util.Set<com.legend.sql.RaiseKind> plainRaises() {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public String activitySql(Activity activity) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public String graphKeyDate(String written) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public String graphKeyString(String value) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public String errorWords(RuntimeException e) {
        return e.getMessage();
    }

    @Override
    public @com.legend.base.Nullable String siteWords(com.legend.sql.dialect.RaiseSite site) {
        return null;
    }

    @Override
    public <F> F facet(Class<F> kind) {
        throw new UnsupportedOperationException("planted");
    }

    @Override
    public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
        return facet(kind);
    }
}
