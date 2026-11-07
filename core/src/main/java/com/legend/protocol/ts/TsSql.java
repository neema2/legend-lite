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

    /** The series queries a statement's plain SQL reads by their WITH names (the spec's §8: {@code SELECT
     *  regr_slope(qqq, spx) FROM s}): a relation for each name, the translator's sources (TsToPure). */
    interface Named extends SqlToPure.Sources {
        com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification relation(String name);
    }

    /** A one-part table name a series query's WITH gives: its relation, read as a table is (its columns typed by the
     *  sources); else none, as Postgres's. */
    @Override
    public com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification table(SqlAst.TableName t,
            SqlToPure.Sources sources) {
        return sources instanceof Named named && t.name().size() == 1 ? named.relation(t.name().get(0).text()) : null;
    }

    @Override
    public SqlToPure.Tx binary(SqlAst.Binary b, SqlToPure.At at) {
        if (b.op() != SqlAst.BinaryOp.DIVIDE || nonzero(b.right())) {
            // a divisor written as a number other than 0 is never 0: Postgres's own, its constant's width checked
            return super.binary(b, at);
        }
        SqlAst.Expr divisor = new SqlAst.FunctionCall(List.of(new SqlAst.Ident("nullif", false, b.pos())),
                List.of(b.right(), new SqlAst.IntegerLit(BigInteger.ZERO, b.pos())), false, false, b.pos());
        return super.binary(new SqlAst.Binary(b.op(), b.left(), divisor, b.pos()), at);
    }

    /**
     * A VARIABLE OF THE ENCLOSING PURE read as a value, {@code $from} (decision 7 of docs/TS_DECISIONS_2026_11_07.md,
     * 2026-11-07): the variable in scope, typed by it -- one value, of a type the language reads; anything else
     * refused by name. Postgres's positional {@code $1} stays refused.
     */
    @Override
    public SqlToPure.Tx parameter(SqlAst.Param p, SqlToPure.At at, SqlToPure.Sources sources) {
        String name = p.name();
        if (name == null) {
            return super.parameter(p, at, sources);
        }
        SqlToPure.VariableType type = SqlToPure.variable(sources, name);
        if (type == null) {
            throw com.legend.protocol.sql.SqlTranslationException.invalid("$" + name + " is no variable of the"
                    + " enclosing Pure in scope", p.pos());
        }
        if (type.many() || type.nullable()) {
            throw com.legend.protocol.sql.SqlTranslationException.unsupported("$" + name + " may hold "
                    + (type.many() ? "more than one value" : "no value") + ": #TS reads a variable of one value, "
                    + name + ": " + type.type() + "[1]", p.pos());
        }
        SqlToPure.Kind kind = switch (type.type()) {
            case "Integer" -> SqlToPure.Kind.INTEGER;
            case "Float" -> SqlToPure.Kind.FLOAT;
            case "Number" -> SqlToPure.Kind.NUMBER;
            case "String" -> SqlToPure.Kind.STRING;
            case "Boolean" -> SqlToPure.Kind.BOOLEAN;
            case "StrictDate" -> SqlToPure.Kind.DATE;
            case "DateTime" -> SqlToPure.Kind.DATETIME;
            default -> throw com.legend.protocol.sql.SqlTranslationException.unsupported("$" + name + " is a "
                    + type.type() + ": #TS reads a variable of a number, a string, a boolean or a date", p.pos());
        };
        return SqlToPure.Tx.value(new com.legend.protocol.spec.Variable(name), kind, false);
    }

    private static boolean nonzero(SqlAst.Expr e) {
        return e instanceof SqlAst.IntegerLit i && i.value().signum() != 0
                || e instanceof SqlAst.NumericLit n && n.value().signum() != 0;
    }
}
