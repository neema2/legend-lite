// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * The engine's COMPOSITE dialect text: the DEFAULT spellings — native
 * trim/pad/cbrt like DB2's 'common' goldens, but WITHOUT DB2's own
 * quirks ({@code CHARACTER_LENGTH(x,CODEUNITS32)} stays plain
 * {@code char_length}). Divergent goldens fail honestly.
 */
public final class EngineStyleComposite extends EngineStyleDB2 {

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        if (c.fn() == SqlFn.LENGTH) {
            return writer.append("char_length(").expr(c.args().get(0), 0).append(")");
        }
        if (c.fn() == SqlFn.SUBSTRING) {
            // Composite keeps the FULL substring keyword (DB2 shortens)
            return writer.append("substring(").list(c.args()).append(")");
        }
        return super.call(writer, c, parentPrec);
    }
}
