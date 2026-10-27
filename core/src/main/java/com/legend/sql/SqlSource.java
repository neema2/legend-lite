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
     *
     * <p>{@code named} (the plan path program's step 9, 2026-10-19): the values a cast over a DYNAMIC pivot names
     * (each generated column's key, {@code '2011__|__newCol'} naming 2011), each the ON key's typed literal, in the
     * cast's order; empty when {@code in} is given or no cast names them. They pin no row: a dialect without a native
     * dynamic PIVOT pivots on them as it does a static {@code in} (a group whose key none of them is keeps its row,
     * every named column null), and one with it runs the dynamic form and reads the columns by name (the engine's
     * DuckDB: a named key no row has is the database's error there, a null column elsewhere -- register S109).
     */
    record Pivot(SqlSource source, List<SqlExpr> on, List<SqlExpr> in, List<SqlExpr> named, List<Using> usings,
                 String alias, List<OutputCol> outputs) implements SqlSource {
        /** The values whose columns a dialect without a dynamic PIVOT writes: {@code in}'s, else the named ones;
         *  empty for a dynamic pivot nothing names (its columns exist only once the database is asked). */
        public List<SqlExpr> values() {
            return in.isEmpty() ? named : in;
        }

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

    /** A base table &mdash; or, {@code call}, a TABULAR FUNCTION: a named
     *  relation the database computes, read as {@code FROM name()}. It is a
     *  table to every pass (its outputs are declared, like a table's) except
     *  the renderer; it has no physical row order (no rowid). No defaulting
     *  constructor: a rebuild site says what it carries.
     *
     *  {@code storedTypes}: the columns' DECLARED store types, by bare name -- what the database
     *  holds, which a column's {@code outputs} type (the value as read) does not say. A dialect reads
     *  a stored type the platform cannot use as held (docs/STORE_TYPES_HOMEWORK_2026_10_02.md).
     *  Empty for a table nothing declared (a synthetic CTE, the calendar table).
     *
     *  {@code name} is the table's text, {@code schema.table} for a schema's, bare (no quote characters): the
     *  identity every reader looks it up by. {@code quoting}: which parts the store declared quoted, which the
     *  dialect delimits, their case kept ({@link NameQuoting}; {@link NameQuoting#BARE} for every name the
     *  platform makes). */
    record Table(String name, String alias, List<OutputCol> outputs, boolean call,
            java.util.Map<String, SqlDdl.ColumnType> storedTypes, NameQuoting quoting) implements SqlSource {
        public Table {
            storedTypes = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(storedTypes));
        }
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

    /** An AUTHORED raw SQL text as a relation source —
     * {@code (rawSql) AS alias} (One-Platform Plan Phase 1: the typed
     * {@code executeInDb} result grid; columns from the LIMIT-0 schema
     * probe). The text is CARRIED data (a user/test-authored query),
     * never platform-composed SQL — the SQL-text ratchet's distinction.
     * {@code writtenFor} is the database its connection declares (that
     * type's name): the dialect translates the engine's H2 text and runs
     * any other as written (decision D2). The platform writes no text
     * here (the dialect program's Phase 3, 2026-10-16: a catalog grid,
     * the one platform text it carried, is a {@link Catalog}). */
    record RawSql(String sql, String alias, List<OutputCol> outputs, String writtenFor) implements SqlSource {
        public RawSql {
            java.util.Objects.requireNonNull(writtenFor, "raw SQL carries the database its text is written for");
        }
    }

    /** A catalog grid of the session's own database (the {@code fetchDb*MetaData} natives): its schemas, tables,
     * columns or primary keys, each kind filtered by a JDBC {@code LIKE} pattern (null matches every name), in the
     * columns JDBC's {@code DatabaseMetaData} fixes for the grid, named by {@code outputs} in order. Where a database
     * keeps its catalog and how it is read is the dialect's (the dialect program's Phase 3, 2026-10-16: until then
     * the compiler wrote {@code information_schema} text into a {@link RawSql}). A schema grid takes no table
     * pattern, and only the column grid takes a column pattern. */
    record Catalog(Grid grid, @com.legend.base.Nullable String schemaPattern,
            @com.legend.base.Nullable String tablePattern, @com.legend.base.Nullable String columnPattern,
            String alias, List<OutputCol> outputs) implements SqlSource {

        public Catalog {
            if (grid == Grid.SCHEMAS && tablePattern != null || grid != Grid.COLUMNS && columnPattern != null) {
                throw new IllegalArgumentException("the " + grid + " catalog grid takes no such pattern");
            }
            outputs = List.copyOf(outputs);
        }

        /** The four grids. */
        public enum Grid { SCHEMAS, TABLES, COLUMNS, PRIMARY_KEYS }
    }

    /** {@code on} is KIND-COUPLED, enforced at construction: the CROSS
     * family takes no ON clause; every other kind REQUIRES one — a null
     * {@code on} on an INNER/LEFT join would render {@code JOIN t}
     * (invalid SQL, or an accidental natural join). LEFT_LATERAL spells
     * its always-true condition explicitly ({@code ON true}). */
    record Join(SqlSource left, SqlSource right, Kind kind,
            @com.legend.base.Nullable SqlExpr on, List<OutputCol> outputs) implements SqlSource {

        /** A join over {@code left} and {@code right}; its delivered columns are computed here, once
         *  ({@link #delivered}). The five-argument form is the record's own, which a record cannot hide: its
         *  {@code outputs} must be {@code delivered(left, right, kind)}, and no caller passes a list of its own. */
        public Join(SqlSource left, SqlSource right, Kind kind, @com.legend.base.Nullable SqlExpr on) {
            this(left, right, kind, on, delivered(left, right, kind));
        }

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


        /** What a join does; each dialect writes its keywords ({@code joinKeyword}: until 2026-10-15 each kind
         *  carried its ANSI SQL here, the dialect program's Phase 2). */
        public enum Kind {
            INNER,
            LEFT,
            RIGHT,
            FULL,
            CROSS,
            // Per-row correlated right side (real relation lateral.pure);
            // the right subselect references the left alias.
            CROSS_LATERAL,
            ASOF_LEFT,
            /** Correlated derived table preserving left rows ({@code ... ON TRUE}) — array explosion. */
            LEFT_LATERAL;

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
        private static List<OutputCol> delivered(SqlSource left, SqlSource right, Kind kind) {
            // ONCE, when the join is made, and {@link #outputs} answers the list (2026-10-15): until then every
            // reader had the list built again, both sides down to the leaves, so a chain of N joins built N levels
            // at each column lookup -- the tenth line's 250-join report planned in 27-40 s
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
