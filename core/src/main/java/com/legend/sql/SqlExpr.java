package com.legend.sql;

import java.math.BigDecimal;
import java.util.List;

/**
 * Scalar SQL expressions &mdash; sealed, immutable, data-only. Function calls
 * ({@link Call}) carry SEMANTIC names; the dialect owns the SQL spelling
 * (an unknown semantic name is a loud rendering error, never a fallback).
 */
public sealed interface SqlExpr
        permits SqlExpr.Column, SqlExpr.Star, SqlExpr.StringLit, SqlExpr.IntLit,
                SqlExpr.FloatLit, SqlExpr.DecimalLit, SqlExpr.BoolLit, SqlExpr.NullLit,
                SqlExpr.DateLit, SqlExpr.TimestampLit, SqlExpr.ArrayLit, SqlExpr.Call,
                SqlExpr.Case, SqlExpr.Exists, SqlExpr.ScalarSubquery, SqlExpr.WindowCall,
                SqlExpr.Lambda, SqlExpr.Cast, SqlExpr.FoldCall, SqlAgg.Reducer {

    /** A column reference, optionally qualified by a source alias. */
    record Column(String table, String name) implements SqlExpr {
        public static Column of(String table, String name) {
            return new Column(table, name);
        }
    }

    /** {@code *} or {@code alias.*}. */
    record Star(String table) implements SqlExpr {
    }

    record StringLit(String value) implements SqlExpr {
    }

    record IntLit(long value) implements SqlExpr {
    }

    record FloatLit(double value) implements SqlExpr {
    }

    record DecimalLit(BigDecimal value) implements SqlExpr {
    }

    record BoolLit(boolean value) implements SqlExpr {
    }

    record NullLit() implements SqlExpr {
    }

    /** ISO {@code yyyy-MM-dd}; renders as a typed DATE literal. */
    record DateLit(String iso) implements SqlExpr {
    }

    /** ISO timestamp; renders as a typed TIMESTAMP literal. */
    record TimestampLit(String iso) implements SqlExpr {
    }

    /** A list literal, {@code [a, b, c]} in DuckDB. */
    record ArrayLit(List<SqlExpr> elements) implements SqlExpr {
    }

    /** A function application by SEMANTIC vocabulary entry (see {@link SqlFn}). */
    record Call(SqlFn fn, List<SqlExpr> args) implements SqlExpr {
        public static Call of(SqlFn fn, SqlExpr... args) {
            return new Call(fn, List.of(args));
        }
    }

    /** {@code CASE WHEN ... THEN ... [WHEN ...] ELSE ... END}. */
    record Case(List<When> whens, SqlExpr otherwise) implements SqlExpr {
        public record When(SqlExpr condition, SqlExpr then) {
        }
    }

    /** {@code EXISTS (subquery)} &mdash; Boolean-composable association predicate. */
    record Exists(SqlQuery subquery) implements SqlExpr {
    }

    /** A single-value subquery in scalar position. */
    record ScalarSubquery(SqlQuery subquery) implements SqlExpr {
    }

    /**
     * {@code fn(...) OVER (PARTITION BY ... ORDER BY ... frame)}. Any
     * {@link SqlAgg} kind is legal here &mdash; this is the ONLY position that
     * admits the window-only kinds.
     */
    record WindowCall(SqlAgg fn, List<SqlExpr> partitionBy, List<SqlSelect.SortKey> orderBy,
                      Frame frame) implements SqlExpr {

        /** {@code ROWS|RANGE BETWEEN <from> AND <to>}. */
        public record Frame(Kind kind, Bound from, Bound to) {
            public enum Kind { ROWS, RANGE }

            public sealed interface Bound {
                record UnboundedPreceding() implements Bound {
                }

                record Preceding(long n) implements Bound {
                }

                record CurrentRow() implements Bound {
                }

                record Following(long n) implements Bound {
                }

                record UnboundedFollowing() implements Bound {
                }
            }
        }
    }

    /** A lambda for DuckDB list functions: {@code x -> body} / {@code (a, x) -> body}. */
    record Lambda(List<String> params, SqlExpr body) implements SqlExpr {
    }

    /**
     * {@code CAST(value AS <type>[])} — the target rides as a PURE type; the
     * SQL type name is the dialect's business. {@code array} casts to a list
     * of the target ({@code toMany}). A dialect may render a variant-access
     * value through its text-extraction idiom (DuckDB {@code ->>}) — that
     * swap is RENDERING knowledge, not IR content.
     */
    record Cast(SqlExpr value, SqlType target, boolean array) implements SqlExpr {
    }

    /**
     * A FOLD over a collection value, in PURE conventions: the lambda's
     * parameters are {@code (element, accumulator)} — Pure's order — and the
     * dialect owns the encoding (DuckDB: {@code list_reduce} with swapped
     * params, single-item-list wrap/unwrap when {@code accIsList}; a
     * lambda-less backend: recursive CTE or a loud error).
     */
    record FoldCall(SqlExpr source, Lambda lambda, SqlExpr init, boolean accIsList,
                    boolean homogeneous) implements SqlExpr {
    }
}
