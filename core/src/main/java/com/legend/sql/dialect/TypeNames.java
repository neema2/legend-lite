package com.legend.sql.dialect;

import com.legend.sql.SqlType;

import java.util.EnumMap;
import java.util.Map;

/**
 * Dialect TYPE SPELLINGS &mdash; pure DATA (remediation T3.2, extraction 2):
 * the scalar CAST names a backend uses, plus its composite-type
 * capabilities. Absence is LOUD at the read site &mdash; a scalar with no
 * spelling here is "this backend cannot cast to that type", never a
 * silent fallback. Composite RULES (DECIMAL(p,s), array suffix, MAP,
 * STRUCT field lists) are structural and live with the renderer; this
 * record only says which are supported and what the leaves are called.
 *
 * @param scalarNames   scalar &rarr; spelling; absent = unsupported (loud)
 * @param structSupport whether STRUCT(...) composite casts exist
 */
public record TypeNames(Map<SqlType.Scalar, String> scalarNames,
        boolean structSupport) {

    public TypeNames {
        scalarNames = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(scalarNames));
    }

    /** ANSI baseline: no JSON, no STRUCT. */
    public static final TypeNames ANSI = new TypeNames(base(), false);

    /** DuckDB: bare DOUBLE, native JSON, STRUCT(...). */
    public static final TypeNames DUCKDB = new TypeNames(duck(), true);

    /** H2 2.1.214: standard names; JSON exists as a type but maps to
     * byte[] on read-back (codec row pending, H2_BACKEND.md step 11);
     * no STRUCT. HUGEINT has no H2 name — NUMERIC(38) (probed: cast
     * rounding and VARCHAR print parity with DuckDB HUGEINT on
     * 2.7/2.0/-3.9); JSON casts spell JSON (the R5a variant carrier). */
    public static final TypeNames H2 = new TypeNames(h2(), false);

    /** PostgreSQL 16+ (2026-10-01 W5.5/P1 Postgres dialect): DOUBLE PRECISION (a bare
     * {@code DOUBLE} is no Postgres type); HUGEINT as NUMERIC(38); JSON as JSONB, the
     * collection and variant carrier (POSTGRES_BACKEND.md §5); no STRUCT. */
    public static final TypeNames POSTGRES = new TypeNames(postgres(), false);

    private static Map<SqlType.Scalar, String> postgres() {
        Map<SqlType.Scalar, String> m = base();
        m.put(SqlType.Scalar.HUGEINT, "NUMERIC(38)");
        m.put(SqlType.Scalar.JSON, "JSONB");
        return m;
    }

    private static Map<SqlType.Scalar, String> h2() {
        Map<SqlType.Scalar, String> m = base();
        m.put(SqlType.Scalar.HUGEINT, "NUMERIC(38)");
        m.put(SqlType.Scalar.JSON, "JSON");
        return m;
    }

    private static Map<SqlType.Scalar, String> base() {
        Map<SqlType.Scalar, String> m = new EnumMap<>(SqlType.Scalar.class);
        m.put(SqlType.Scalar.BOOLEAN, "BOOLEAN");
        m.put(SqlType.Scalar.INTEGER, "INTEGER");
        m.put(SqlType.Scalar.BIGINT, "BIGINT");
        m.put(SqlType.Scalar.HUGEINT, "HUGEINT");
        m.put(SqlType.Scalar.TIMESTAMPTZ, "TIMESTAMPTZ");
        m.put(SqlType.Scalar.DOUBLE, "DOUBLE PRECISION");
        m.put(SqlType.Scalar.VARCHAR, "VARCHAR");
        // the kind-faithful carriers are spelling TEXT on every backend
        m.put(SqlType.Scalar.LITERAL, "VARCHAR");
        m.put(SqlType.Scalar.TEMPORAL_TEXT, "VARCHAR");
        m.put(SqlType.Scalar.DATE, "DATE");
        m.put(SqlType.Scalar.TIMESTAMP, "TIMESTAMP");
        return m;
    }

    private static Map<SqlType.Scalar, String> duck() {
        Map<SqlType.Scalar, String> m = base();
        m.put(SqlType.Scalar.DOUBLE, "DOUBLE");
        m.put(SqlType.Scalar.JSON, "JSON");
        return m;
    }
}
