package com.legend.sql;

import com.legend.base.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * THE query node (PHASE_HIJ_LOWERING.md): one record with every clause slot,
 * mirroring real legend's {@code SelectSQLQuery}. The fold policy extends a
 * single {@code SqlSelect} through a run of compatible relational ops via the
 * {@code with*} copiers; a fresh nesting level exists only as an explicit
 * {@link SqlSource.Subselect}. Empty {@link #projections} means {@code SELECT *}.
 *
 * <p>OUTPUTS-FROM-PROJECTIONS (SQL-IR backend-agnosticism slice 2,
 * ORIGIN_ARCHITECTURE_AUDIT_2026_08.md): a projection frame's
 * {@link #outputs} are BUILT here from the projection list — each
 * {@link Projection} carries its declared {@link OutputCol} (reconciled
 * against the expression's stored type per slot), a star projection
 * carries the starred source's whole list verbatim (origins included,
 * join pads applied). The list recomputes at every construction, so a
 * rebuild can never hold a stale claim. Only STAR FRAMES (empty
 * projections — {@code SELECT *}) take their outputs from the caller:
 * the source passthrough, schema-born at the physical doors.
 */
public record SqlSelect(List<Projection> projections, boolean distinct,
                        SqlSource from,
                        @Nullable SqlExpr where, List<SqlExpr> groupBy,
                        @Nullable SqlExpr having, @Nullable SqlExpr qualify,
                        List<SortKey> orderBy, @Nullable Long limit,
                        @Nullable Long offset, List<OutputCol> outputs)
        implements SqlQuery {

    public SqlSelect {
        java.util.Objects.requireNonNull(from,
                "a FROM-less select spells SqlSource.Dual, never null");
        if (!projections.isEmpty()) {
            // THE LABEL RULE, per slot at construction (subsumes the old
            // positional reconcileLabels and its star-tail shift): the
            // declared label keeps the pure-contract erasure when equal
            // or ADMITTED; a label lie ADOPTS the wire; nullability
            // adopts the slot truth (§E3 M-N3). Stars expand the
            // starred source's own outputs — origin and tolerance ride
            // along, pad sides weaken (§E3 M-N2).
            outputs = outputsFrom(projections, from, groupBy);
        } else if (from instanceof SqlSource.Join j
                && where != null && outputs != null && !outputs.isEmpty()) {
            // §E3-S WHERE≡INNER: a star-framed join whose WHERE
            // null-rejects a pad side restores that side's DDL truth (no
            // padded row survives the filter). Star frames only —
            // projection frames adopt from facts above.
            outputs = SqlTyping.wherePadNeutralized(j, where, outputs);
        }
    }

    /** The projection-frame output list: declared slots reconciled
     * against their expressions' stored types; stars expand their
     * source verbatim; output-less projections (scalar envelopes)
     * contribute no slot. */
    private static List<OutputCol> outputsFrom(List<Projection> ps,
            SqlSource from, List<SqlExpr> groupBy) {
        boolean grouped = !groupBy.isEmpty();
        List<OutputCol> out = new ArrayList<>(ps.size());
        for (Projection p : ps) {
            if (p.expr() instanceof SqlExpr.Star s) {
                expandStar(from, s.table(), false, null, out);
            } else if (p.expr() instanceof SqlExpr.StarExcept se) {
                expandStar(from, se.table(), false,
                        java.util.Set.copyOf(se.except()), out);
            } else if (p.out() != null) {
                out.add(SqlTyping.reconcileSlot(p.expr(), p.out(), grouped));
            }
        }
        return List.copyOf(out);
    }

    /** A star projection's expansion: the starred source's outputs,
     * VERBATIM — the star passes labels through, so origin, tolerance
     * and spelling are the source's own facts. Descending through a
     * padding join weakens that side's columns to nullable (the
     * padJoinOutputs truth, structural — no name lookup). */
    private static void expandStar(SqlSource src, @Nullable String table,
            boolean padded, java.util.@Nullable Set<String> except,
            List<OutputCol> into) {
        if (src instanceof SqlSource.Join j) {
            expandStar(j.left(), table, padded || j.kind().padsLeft(),
                    except, into);
            expandStar(j.right(), table, padded || j.kind().padsRight(),
                    except, into);
            return;
        }
        if (table != null && !table.equals(src.alias())) {
            return;
        }
        for (OutputCol c : src.outputs()) {
            if (except != null && except.contains(c.name())) {
                continue;
            }
            into.add(padded && !c.nullable()
                    ? new OutputCol(c.name(), c.type(), true, c.origin())
                    : c);
        }
    }

    /** {@code SELECT * FROM source} with every other clause empty. */
    public static SqlSelect starOf(SqlSource from) {
        return new SqlSelect(List.of(), false, from, null, List.of(), null, null,
                List.of(), null, null, from.outputs());
    }

    /** SYNTHETIC scalar-map column-name marker (resolver
     * scalarMapAsProject): the engine spells a bare map scalar select
     * UNALIASED — the engine-TEXT channel drops such aliases entirely;
     * execution keeps them (downstream references use the row type). */
    public static final String SYNTH_MAP_COL = "u_map__";

    /** One projection with its DECLARED output column. {@code out} is
     * the slot this projection delivers — the contract label the frame
     * above reads through. Null in exactly two shapes: a STAR (the
     * projection delivers the starred source's whole list — carrying a
     * single slot would be a lie), and a deliberately OUTPUT-LESS
     * projection (scalar-position value envelopes, {@code SELECT 1}
     * exists probes — nothing reads the frame by name).
     *
     * <p>An explicit projection's output name is the QUERY's own
     * declaration — the renderer that spends origins labels every such
     * projection explicitly (the engine's convention) — so an attached
     * output is normalized to DERIVED at construction; PHYSICAL
     * spellings survive only through star passthrough and the frame
     * doors. */
    public record Projection(SqlExpr expr, @Nullable String alias,
            @Nullable OutputCol out) {

        public Projection {
            if (expr instanceof SqlExpr.Star
                    || expr instanceof SqlExpr.StarExcept) {
                if (out != null) {
                    throw new IllegalArgumentException("a star projection"
                            + " carries the source's whole output list —"
                            + " attaching a single OutputCol is a caller"
                            + " bug");
                }
            } else if (out != null
                    && out.origin() != OutputCol.Origin.DERIVED) {
                out = new OutputCol(out.name(), out.type(), out.nullable());
            }
        }

        /**
         * The projected OUTPUT name: the alias, else the bare column's own
         * name, else null (a computed expression with no alias has no
         * addressable name). THE one implementation of the rule (an audit
         * found it duplicated across Fold and the Lowerer).
         */
        public @Nullable String outputName() {
            return alias != null ? alias
                    : expr instanceof SqlExpr.Column c ? c.name() : null;
        }

    }

    /** One ORDER BY key; {@code nullOrder} null = dialect default.
     * {@code outputName} — the projected TDS column a COLUMN-NAME-keyed
     * sort addresses; engine text spells it ({@code order by "name"
     * asc}), execution dialects render {@code expr}. Null otherwise. */
    public record SortKey(SqlExpr expr, boolean ascending,
            @Nullable NullOrder nullOrder, @Nullable String outputName) {
        public enum NullOrder { NULLS_FIRST, NULLS_LAST }

        // NO short overload: a defaulted outputName silently de-addressed a
        // column-name-keyed sort at rebuild sites (remediation T2.2); every
        // construction names every field.

        public static SortKey asc(SqlExpr e) {
            return new SortKey(e, true, null, null);
        }

        /** Test-DSL convenience (no production callers; hand-built IR only). */
        public static SortKey desc(SqlExpr e) {
            return new SortKey(e, false, null, null);
        }
    }

    // ----- clause copiers: the fold policy's fingers -----

    public SqlSelect withFrom(SqlSource f) {
        return new SqlSelect(projections, distinct, f, where, groupBy, having,
                qualify, orderBy, limit, offset, outputs);
    }

    /** Replace the projection list; outputs REBUILD from it (each
     * projection carries its slot). Non-empty only — a star frame's
     * outputs are the caller's fact: {@link #withOutputs}. */
    public SqlSelect withProjections(List<Projection> p) {
        if (p.isEmpty()) {
            throw new IllegalArgumentException("withProjections(empty):"
                    + " a star frame's outputs are caller knowledge —"
                    + " use withOutputs");
        }
        return new SqlSelect(p, distinct, from, where, groupBy, having,
                qualify, orderBy, limit, offset, outputs);
    }

    /** A projection frame RE-TYPED: the projections and their outputs
     * together (a wire-kind reconciliation relabels a column reference
     * and its output as one fact — {@code exec.WireTypes}). */
    public SqlSelect withProjections(List<Projection> p, List<OutputCol> out) {
        if (p.isEmpty() || p.size() != out.size()) {
            throw new IllegalArgumentException("withProjections(p, out): "
                    + p.size() + " projections for " + out.size() + " outputs");
        }
        return new SqlSelect(p, distinct, from, where, groupBy, having,
                qualify, orderBy, limit, offset, out);
    }

    /** STAR-FRAME outputs door ({@code SELECT *} — empty projections):
     * the one shape whose outputs are the caller's own fact (source
     * passthrough / schema-born at the physical doors). */
    public SqlSelect withOutputs(List<OutputCol> out) {
        if (!projections.isEmpty()) {
            throw new IllegalStateException("withOutputs on a projection"
                    + " frame: its outputs derive from the projections");
        }
        return new SqlSelect(projections, distinct, from, where, groupBy,
                having, qualify, orderBy, limit, offset, out);
    }

    public SqlSelect withDistinct() {
        return new SqlSelect(projections, true, from, where, groupBy, having, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withWhere(@Nullable SqlExpr w) {
        return new SqlSelect(projections, distinct, from, w, groupBy, having, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withGroupBy(List<SqlExpr> keys) {
        return new SqlSelect(projections, distinct, from, where, keys, having, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withHaving(@Nullable SqlExpr h) {
        return new SqlSelect(projections, distinct, from, where, groupBy, h, qualify, orderBy, limit, offset, outputs);
    }

    public SqlSelect withQualify(@Nullable SqlExpr q) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, q, orderBy, limit, offset, outputs);
    }

    public SqlSelect withOrderBy(List<SortKey> keys) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, qualify, keys, limit, offset, outputs);
    }

    public SqlSelect withLimit(@Nullable Long n) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, qualify, orderBy, n, offset, outputs);
    }

    public SqlSelect withOffset(@Nullable Long n) {
        return new SqlSelect(projections, distinct, from, where, groupBy, having, qualify, orderBy, limit, n, outputs);
    }
}
