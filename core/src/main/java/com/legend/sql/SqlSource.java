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
     * DuckDB {@code PIVOT <source> ON <col> USING <agg> AS <alias>} — a
     * structural source; output columns are DYNAMIC (one per pivot value),
     * so {@code outputs} carries only what Phase G could type statically.
     */
    record Pivot(SqlSource source, List<SqlExpr> on, List<Using> usings, String alias,
                 List<OutputCol> outputs) implements SqlSource {
        public record Using(SqlAgg.Reducer agg, String alias) {
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

    record Subselect(SqlQuery inner, String alias) implements SqlSource {
        @Override
        public List<OutputCol> outputs() {
            return inner.outputs();
        }
    }

    /** {@code (VALUES (...), (...)) AS alias(col, ...)} &mdash; TDS / instance literals. */
    record Values(List<List<SqlExpr>> rows, List<String> columns, String alias,
                  List<OutputCol> outputs) implements SqlSource {
    }

    record Join(SqlSource left, SqlSource right, Kind kind, SqlExpr on) implements SqlSource {
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
            ASOF_LEFT("ASOF LEFT JOIN");

            public final String sql;

            Kind(String sql) {
                this.sql = sql;
            }
        }

        @Override
        public List<OutputCol> outputs() {
            List<OutputCol> all = new ArrayList<>(left.outputs());
            all.addAll(right.outputs());
            return List.copyOf(all);
        }
    }
}
