package com.legend.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * FROM-clause sources. A {@link Join} tree renders FLAT
 * ({@code a JOIN b ON ... LEFT OUTER JOIN c ON ...}) &mdash; joins are inlined,
 * never wrapped, per the lean-SQL tenet; {@link Subselect} is the ONLY
 * nesting construct and exists solely where the fold policy demands isolation.
 */
public sealed interface SqlSource {

    /**
     * The source's binding alias — satisfied by each record's {@code alias}
     * component. A nested {@link Join} has no single alias (its sides
     * resolve individually); asking is a caller bug.
     */
    String alias();

    List<OutputCol> outputs();

    /**
     * DuckDB {@code PIVOT <source> ON <col> [IN (v…)] USING <agg> AS <alias>}
     * — a structural source; output columns are DYNAMIC (one per pivot
     * value), so {@code outputs} carries only what Phase G could type
     * statically. A non-empty {@code in} pins the pivoted columns to exactly
     * those values (the static {@code pivot(~col, [v…], ~agg)} form).
     */
    record Pivot(SqlSource source, List<SqlExpr> on, List<SqlExpr> in, List<Using> usings,
                 String alias, List<OutputCol> outputs) implements SqlSource {
        /** {@code type}: the aggregate's LOWERING-typed result slot —
         * the typed fact pivot-generated columns inherit (E1: recovered
         * from backend metadata before, which typed SUM columns Decimal
         * on H2 and Integer on DuckDB). Null only through the legacy
         * ctor at rewrite sites that predate the field. */
        public record Using(SqlAgg.Reducer agg, String alias,
                @com.legend.base.Nullable SqlType type) {
            public Using(SqlAgg.Reducer agg, String alias) {
                this(agg, alias, null);
            }
        }
    }

    /**
     * An external semi-structured source ({@code sourceUrl('data:...')}) —
     * ONE {@code data} column of JSON rows; the DIALECT renders the URL
     * into a complete subquery (scheme-dispatched: {@code data:} inlines,
     * {@code file:} reads).
     */
    record SourceUrl(String url, String alias, List<OutputCol> outputs) implements SqlSource {
    }

    record Table(String name, String alias, List<OutputCol> outputs) implements SqlSource {
    }

    /** A reference to a CTE the statement defines (leg 3.4 step 2: a planned
     * execute() frame every assert side reads) — NOT a base table: it has no
     * physical row order of its own (no rowid; the scan-order pass re-exports
     * the ordinals its definition threads), and no store to replace. */
    record Cte(String name, String alias, List<OutputCol> outputs) implements SqlSource {
    }

    /** The engine's cross-store VarSetPlaceHolder: a PLAN VARIABLE
     * standing in for another execution node's result set — spelled
     * {@code (${varName}) as "alias"} in plan SQL (freemarker splice at
     * execution); the reAlias grouping keys by the lowercased varName
     * (tdsvar_0/tdsvar_1 goldens). PLAN-TEXT ONLY today: reaching a
     * DuckDB execution renderer is a caller bug. */
    record VarSetPlaceholder(String varName, String alias,
            List<OutputCol> outputs) implements SqlSource {
    }

    /** The FROM-less scalar select's source ({@code SELECT <expr>} — the
     * executeInDb value channel). A REAL variant rather than a null
     * {@code from}: an absent source is a different KIND of source, and
     * dialects that need a dummy table (DB2 {@code SYSIBM.SYSDUMMY1},
     * Oracle {@code DUAL}) get a render hook instead of an omission
     * convention. Current dialects all render it as clause omission. */
    record Dual() implements SqlSource {
        @Override
        public String alias() {
            throw new IllegalStateException(
                    "a Dual (FROM-less) source has no alias — caller bug");
        }

        @Override
        public List<OutputCol> outputs() {
            return List.of();
        }
    }

    /** {@code frameName}: the derived table's MODEL identity (a view's
     * own name) — null for anonymous isolation subselects. Dialects that
     * re-alias by table group name view frames by it. */
    record Subselect(SqlQuery inner, String alias,
            @com.legend.base.Nullable String frameName)
            implements SqlSource {

        /** SYNTHETIC frame marker (not a model identity): the engine's
         * join-distinct exists key subselect (ExistsJoinForm) — dialects
         * group-number its interior and spell its DISTINCT keys bare. */
        public static final String EXISTS_KEYS_FRAME = "existsKeys";

        // NO short overload: a defaulted frameName silently anonymized a
        // view frame at rebuild sites (remediation T2.2); every
        // construction names every field.

        @Override
        public List<OutputCol> outputs() {
            return inner.outputs();
        }
    }

    /** {@code (VALUES (...), (...)) AS alias(col, ...)} &mdash; TDS / instance literals. */
    record Values(List<List<SqlExpr>> rows, List<String> columns, String alias,
                  List<OutputCol> outputs) implements SqlSource {
    }

    /** A CORPUS-AUTHORED raw SQL text as a relation source —
     * {@code (rawSql) AS alias} (One-Platform Plan Phase 1: the typed
     * {@code executeInDb} result grid; columns from the LIMIT-0 schema
     * probe). The text is CARRIED data (a user/test-authored query),
     * never platform-composed SQL — the SQL-text ratchet's distinction. */
    record RawSql(String sql, String alias, List<OutputCol> outputs)
            implements SqlSource {
    }

    /** {@code on} is KIND-COUPLED, enforced at construction: the CROSS
     * family takes no ON clause; every other kind REQUIRES one — a null
     * {@code on} on an INNER/LEFT join would render {@code JOIN t}
     * (invalid SQL, or an accidental natural join). LEFT_LATERAL spells
     * its always-true condition explicitly ({@code ON true}). */
    record Join(SqlSource left, SqlSource right, Kind kind,
            @com.legend.base.Nullable SqlExpr on) implements SqlSource {

        public Join {
            boolean onless = kind == Kind.CROSS || kind == Kind.CROSS_LATERAL;
            if (onless && on != null) {
                throw new IllegalArgumentException(
                        kind + " takes no ON clause");
            }
            if (!onless && on == null) {
                throw new IllegalArgumentException(
                        kind + " requires an ON condition");
            }
        }
        @Override
        public String alias() {
            throw new IllegalStateException(
                    "a nested join has no single alias — resolve per side");
        }


        public enum Kind {
            INNER("JOIN"),
            LEFT("LEFT OUTER JOIN"),
            RIGHT("RIGHT OUTER JOIN"),
            FULL("FULL OUTER JOIN"),
            CROSS("CROSS JOIN"),
            // Per-row correlated right side (real relation lateral.pure);
            // the right subselect references the left alias.
            CROSS_LATERAL("CROSS JOIN LATERAL"),
            ASOF_LEFT("ASOF LEFT JOIN"),
            /** Correlated derived table preserving left rows ({@code ... ON TRUE}) — array explosion. */
            LEFT_LATERAL("LEFT JOIN LATERAL");

            public final String sql;

            Kind(String sql) {
                this.sql = sql;
            }

            /** §E3 M-N2 — PAD PROVENANCE, the kind's own semantics:
             * does this join NULL-pad its LEFT side's columns on
             * unmatched rows? (RIGHT and FULL outer joins do.) A read
             * resolved from a padded side may be NULL regardless of
             * DDL — the join, not the column, is the authority. */
            public boolean padsLeft() {
                return this == RIGHT || this == FULL;
            }

            /** Does this join NULL-pad its RIGHT side's columns?
             * (LEFT/FULL outer, ASOF LEFT, and LEFT LATERAL — the
             * ON-TRUE row-preserving explosion pads on empty.) */
            public boolean padsRight() {
                return this == LEFT || this == FULL || this == ASOF_LEFT
                        || this == LEFT_LATERAL;
            }
        }

        /** The join's delivered columns: both sides, with the PAD
         * TRUTH stamped at the source (§E3 M-N2 moved home — SQL-IR
         * slice 2 finish): a padded side's non-null columns weaken to
         * nullable HERE, because this node is the one owner that holds
         * both the kind and the sides. Every consumer (starOf frames,
         * star expansion, wraps) spends the fact instead of running a
         * repair pass — the old Fold.padJoinOutputs is deleted. */
        @Override
        public List<OutputCol> outputs() {
            List<OutputCol> all = new ArrayList<>();
            side(left.outputs(), kind.padsLeft(), all);
            side(right.outputs(), kind.padsRight(), all);
            return List.copyOf(all);
        }

        private static void side(List<OutputCol> outs, boolean padded,
                List<OutputCol> into) {
            for (OutputCol c : outs) {
                into.add(padded && !c.nullable()
                        ? new OutputCol(c.name(), c.type(), true, c.origin())
                        : c);
            }
        }
    }
}
