package com.legend.sql;

/**
 * The SQL layer's OWN type vocabulary (LEGEND_SQL_VISION.md prerequisite #1):
 * logical SQL types, independent of any frontend's type system. Frontends map
 * their types to these at the lowering boundary (Pure Integer → BIGINT is the
 * FRONTEND's 64-bit decision); dialects map these to spellings.
 */
public sealed interface SqlType {

    enum Scalar implements SqlType {
        BOOLEAN, INTEGER, BIGINT, HUGEINT, DOUBLE, VARCHAR, DATE, TIMESTAMP,
        TIMESTAMPTZ,
        /** A time of day with no date and no zone (Pure's StrictTime): the database's TIME, to the microsecond
         *  (DuckDB, Postgres, H2, MariaDB, Trino, Snowflake, BigQuery name one; SQLite holds its text; ClickHouse
         *  and Databricks have none, and refuse). Lite's own: legend-engine's relational lowering refuses
         *  StrictTime ({@code pureToSQLQuery.pure}, "not supported yet"), docs/SEMANTICS_REGISTER.md S63
         *  (the Cypher island's time-of-day leg, 2026-10-16). */
        TIME, JSON,
        /** The KIND-FAITHFUL CARRIER (F10 proper, docs/F10_CARRIER_
         * DESIGN.md): the cell holds a value's PURE-LITERAL SPELLING as
         * text — the six mutually disjoint source forms (bare int,
         * pointed float, D-suffix decimal, quoted string, bare bool,
         * %-temporal), so the value carries its own kind. Physical form
         * is VARCHAR on every backend; the LABEL records the contract
         * (the spelling grammar), which plain VARCHAR cannot promise.
         * Encoder: {@code lowering/LiteralSpelling}. Decoder:
         * {@code values/LiteralText}. Canon: the cell IS canonical. */
        LITERAL,
        /** The PRECISION-FAITHFUL TEMPORAL-TEXT CARRIER (§4bZ-V B3,
         * the LITERAL pattern): a temporal value whose pure precision
         * a SQL temporal cannot hold — partials (%2015, %2015-04),
         * padded HOUR/MINUTE forms, written subsecond digit counts —
         * carried as its engine print-form text. Physical form is
         * VARCHAR on every backend; the LABEL records the contract (a
         * temporal value in text carriage), which plain VARCHAR cannot
         * promise. Constructed ONLY by the temporal emitters (a
         * marker {@code Cast} that never renders — a label device);
         * decode is pure-type-driven date parsing, unchanged. */
        TEMPORAL_TEXT,
        /** The WIRE-DECODED DECIMAL CARRIER (disagree-9 burn, the B3
         * pattern's decimal twin): a store DECIMAL cell egressing into
         * the pure VALUE lane, carried as its canonical-scale
         * D-suffixed spelling ('1.234D') — the engine's own decode for
         * that lane erases wire scale before pure equality ever runs
         * (VERDICT_DISAGREEMENT_BURN R3/R8), so the value-lane
         * observable is the scale-minimal decimal. Physical form is
         * VARCHAR; the LABEL is the decode instruction (String →
         * BigDecimal at the executor's one-carrier seam). Constructed
         * ONLY by {@code LiteralSpelling#wireValueEgress} (a marker
         * {@code Cast} that never renders). TDS cells never carry this
         * — the raw lane keeps column scale (R6's twin convention). */
        DECIMAL_TEXT
    }

    record Decimal(int precision, int scale) implements SqlType {
    }

    record Array(SqlType element) implements SqlType {
    }

    /** {@code MAP(K, V)} — the Map<U,V> collection carrier. */
    record Map(SqlType key, SqlType value) implements SqlType {
    }

    /**
     * A named-field composite (DuckDB/BigQuery {@code STRUCT}, Postgres
     * {@code ROW}). Field order is load-bearing — it is the layout the
     * emitting frontend declared, never inferred from data.
     */
    record Struct(java.util.List<Field> fields) implements SqlType {
        public Struct {
            fields = java.util.List.copyOf(fields);
        }

        public record Field(String name, SqlType type) {
        }
    }

    /**
     * A value whose Pure kind the database decides among statically-kinded candidates -- a mixed Number's or Date's
     * {@code max}, {@code min}, {@code greatest}, {@code least} or {@code mode}
     * (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a, 2026-10-23). The column of this type holds the TAG: the index
     * into {@code slots} of the winner's slot. Beside it, one column per slot, named by {@link #slotColumn}, holds
     * the winner in that slot's own type and NULL in the others. A slot is a distinct pair of a Pure kind
     * ({@code Integer}, {@code Float}, {@code Decimal}, {@code StrictDate}, {@code DateTime}, {@code Date}) and an
     * SQL type, so the kind survives as a type, never as a spelling. Never cast, never stored: the carrier of a
     * query's root alone.
     */
    record Tagged(java.util.List<Slot> slots) implements SqlType {
        public Tagged {
            slots = java.util.List.copyOf(slots);
            if (slots.isEmpty()) {
                throw new IllegalArgumentException("a tagged type has at least one slot");
            }
        }

        /** One slot: the Pure kind it holds and the SQL type that holds it exactly. */
        public record Slot(String kind, SqlType type) {
        }

        /** The physical column holding slot {@code i} of the tagged column {@code column}: {@code column__i}. The one
         *  owner of the name, read by the planner's column facts and the runner's decode alike. */
        public static String slotColumn(String column, int i) {
            return column + "__" + i;
        }
    }
}
