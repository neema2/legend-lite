// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.sql.PostgresDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import java.math.BigInteger;
import java.util.List;

/**
 * THE SQL A #TS STATEMENT HOLDS: Postgres's, the {@code #SQL{...}#} island's (the language's build ruling 2, by
 * upstream fidelity) -- its items' per-point expressions, its WHERE, its FILTER, its WITH bodies -- but for {@code /}:
 * #TS's gives nothing where the divisor is 0, on every database, where Postgres raises (A3's decision 4(a),
 * docs/TS_A3_ONE_READER_PLAN_2026_11_03.md §3; register S151). {@code x / y} is read as {@code x / nullif(y, 0)}, as
 * the tile has written its formulas' division since it had one: a chart of {@code close / volume} draws, where an
 * instrument has no volume, nothing there, and the rest. One meaning in every clause of a statement (D2's reason).
 */
public final class TsSql extends PostgresDialect {

    /** The one #TS dialect: it has no options. */
    public static final TsSql INSTANCE = new TsSql();

    private TsSql() {
    }

    @Override
    public SqlToPure.Tx binary(SqlAst.Binary b, SqlToPure.At at) {
        if (b.op() != SqlAst.BinaryOp.DIVIDE) {
            return super.binary(b, at);
        }
        SqlAst.Expr divisor = new SqlAst.FunctionCall(List.of(new SqlAst.Ident("nullif", false, b.pos())),
                List.of(b.right(), new SqlAst.IntegerLit(BigInteger.ZERO, b.pos())), false, false, b.pos());
        return super.binary(new SqlAst.Binary(b.op(), b.left(), divisor, b.pos()), at);
    }
}
