// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (HookRulingTest.theRaisePinBitesWordsBesideTheToken; the review of dd5deb781f, 2026-11-18, MUST-FIX 4): a
 *  product dialect whose raise statement reads exactly as the bare token's, its words in a bound value instead -- the
 *  same {@code || ?} written either way, words bound beside a raise and nothing beside a string. It names a dialect, so
 *  it lives here (DatabaseOwners). */
public final class PlantedBoundRaise extends DuckDb {
    @Override
    protected SqlWriter expr(SqlWriter writer, com.legend.sql.SqlExpr e, int parentPrec) {
        if (e instanceof com.legend.sql.SqlExpr.Call c && c.fn() == com.legend.sql.SqlFn.ERROR) {
            boolean raise = c.args().get(0) instanceof com.legend.sql.SqlExpr.RaiseText;
            super.expr(writer, e, parentPrec);
            return writer.append(" || ").bind(new RenderedStatement.Bind("", new RenderedStatement.Binding.Payload(
                    raise ? " Division by zero" : "")));
        }
        return super.expr(writer, e, parentPrec);
    }
}
