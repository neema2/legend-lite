package com.legend.sql;

/**
 * One output column of a query node, in the SQL layer's own type vocabulary
 * (LEGEND_SQL_VISION.md). Frontends stamp these at the lowering boundary;
 * result layers that need frontend types (Pure) read them from the FRONTEND's
 * typed root, never from the plan.
 */
public record OutputCol(String name, SqlType type, boolean nullable,
        Origin origin) {

    /** WHERE the column NAME was born — the fact a case-sensitive
     * renderer needs (convergence batch C blocker; SQL-IR
     * backend-agnosticism slice 1): a PHYSICAL name exists in DDL and
     * spells bare-unless-special (folds with the DDL's casing); a
     * DERIVED name is invented by the query (projection label, VALUES
     * column) and quotes unconditionally at definition AND reference —
     * the engine's own convention (as "root", as "legalName").
     * Stamped at construction, never re-derived at consumption.
     * PHYSICAL_QUOTED: a physical name the DDL declared QUOTED — the name is
     * bare (quotes are a spelling, not identity) and it spells delimited
     * wherever it is referenced, keeping its case on a case-folding database
     * ({@code "firstName"} is not {@code firstName} on H2).
     * PER_KEY: a name born once per key of the plan's keys probe (a dynamic pivot's generated columns, the plan path
     * program's probe plan, 2026-10-20): the name is the stem the key's columns share (the aggregate's place, {@code _u0}),
     * and the dialect spells it delimited with the key's position in the probe written into it by the runner, inside
     * the per-key region the runner repeats ({@code "_pk3__u0"}); outside one it is refused by name. */
    public enum Origin { PHYSICAL, PHYSICAL_QUOTED, DERIVED, PER_KEY }

    /** A store table's {@code outputs}, with the columns its DDL declared
     *  QUOTED ({@code quoted}, by bare name) as {@link Origin#PHYSICAL_QUOTED}
     *  — stamped once, where the table's scan is born. */
    public static java.util.List<OutputCol> declaredQuoted(java.util.List<OutputCol> outputs,
            java.util.Set<String> quoted) {
        return quoted.isEmpty() ? outputs : outputs.stream()
                .map(c -> quoted.contains(c.name())
                        ? new OutputCol(c.name(), c.type(), c.nullable(), Origin.PHYSICAL_QUOTED) : c)
                .toList();
    }

    /** Derived-frame convenience — PHYSICAL outputs are born ONLY at
     * the store boundary ({@code Lowerer.outputsOf}), which uses the
     * canonical constructor explicitly. */
    public OutputCol(String name, SqlType type, boolean nullable) {
        this(name, type, nullable, Origin.DERIVED);
    }

    /** The TYPE is the WIRE — what the slot's expression computes (the
     * compiler's belief of what the database returns); the declared Pure
     * kind lives in the plan's schema, never here (the wire-slot leg,
     * docs/WIRE_SLOT_HOMEWORK_2026_09_19.md). */
}
