// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** THE WIRE KIND OF A WIRE-DECIDED COLUMN IS THE DATABASE'S (leg 3.3,
 * docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4t): the engine types a
 * result cell by the result set's metadata, never by the store's
 * declaration — {@code Interaction.id : String[1]} over a store column
 * declared {@code ID INT} that the fixture created {@code VARCHAR(200)}
 * delivers the String {@code '7'}. A declaration that leaves the kind to
 * the wire (String, unrefined Number, Any — {@code dataTypeTransformer}'s
 * identity arm) therefore takes the type the database REPORTS for the
 * planned statement: one prepare (parse and bind, no row fetched — the
 * driver's {@code getMetaData} on a prepared statement), memoized per
 * connection and statement text. A projection whose stamped kind differs
 * from the reported one is cast to the reported type — a cast to a
 * column's own type, so the value is untouched and the canon spells the
 * wire's kind. Numeric, Boolean and temporal declarations convert the
 * wire cell (the transformer's other arms) and are never touched; nor is
 * a label carrier (LITERAL / TEMPORAL_TEXT / DECIMAL_TEXT / JSON), whose
 * VARCHAR wire is the label's contract. */
public final class WireTypes {

    private WireTypes() {
    }

    /** CENSUS (audit §4y, printed by the corpus lanes — the Executor's
     * round-trip counter's pattern): a wire-decided BARE store column whose
     * stamped kind differed from the kind the database reported and was
     * RE-TYPED to the wire; and a column the reconciliation LEAVES (a
     * computed expression, a label carrier) whose reported kind differs
     * from its slot's — the slot stands, the disagreement counted. */

    /** A column as the database reports it for a prepared statement: its
     * label, its type in the SQL vocabulary (null outside it), nullability. */
    public record ReportedColumn(String name, @com.legend.base.Nullable SqlType type, boolean nullable) {
    }

    /** A plan the compiler could not type (a raw {@code executeInDb} grid: no
     * outputs) framed as {@code SELECT * FROM (plan)} with the database's
     * reported columns as its outputs — the LIMIT-0 probe's own pattern
     * (PctProbe) for a staticized pivot; a column outside the vocabulary
     * carries VARCHAR (its kind is the golden's to infer). {@code plan}
     * itself when the database reports nothing. */
    public static SqlQuery staticized(SqlQuery plan, com.legend.sql.dialect.SqlDialect dialect,
            Connection conn, Map<String, List<ReportedColumn>> memo) {
        List<ReportedColumn> reported = memo.computeIfAbsent(dialect.render(plan), s -> reported(s, conn));
        if (reported.isEmpty()) {
            return plan;
        }
        List<OutputCol> outs = new ArrayList<>(reported.size());
        for (ReportedColumn r : reported) {
            outs.add(new OutputCol(r.name(), r.type() == null ? SqlType.Scalar.VARCHAR : r.type(),
                    r.nullable()));
        }
        return SqlSelect.starOf(new com.legend.sql.SqlSource.Subselect(plan, "side", null))
                .withOutputs(outs);
    }

    /** {@code plan} with its wire-decided outputs reconciled to the
     * database's reported types, or {@code plan} itself when nothing is
     * wire-decided or the plan is not a projection frame. */
    public static SqlQuery reconcile(SqlQuery plan, ExprType shapeInfo,
            com.legend.sql.dialect.SqlDialect dialect, Connection conn,
            Map<String, List<ReportedColumn>> memo) {
        return reconcile(plan, plan, shapeInfo, dialect, conn, memo);
    }

    /** With the frame definitions the plan may reference (leg 3.4 step 2):
     * the plan stays bare; the prepare sees them at the statement's head. */
    public static SqlQuery reconcile(SqlQuery plan, ExprType shapeInfo,
            com.legend.sql.dialect.SqlDialect dialect, Connection conn,
            Map<String, List<ReportedColumn>> memo, Map<String, SqlQuery> frames) {
        return reconcile(plan, com.legend.sql.FrameCtes.attach(plan, frames), shapeInfo,
                dialect, conn, memo);
    }

    private static SqlQuery reconcile(SqlQuery plan, SqlQuery whole, ExprType shapeInfo,
            com.legend.sql.dialect.SqlDialect dialect, Connection conn,
            Map<String, List<ReportedColumn>> memo) {
        if (!(plan instanceof SqlSelect ps) || ps.projections().isEmpty()
                || ps.projections().size() != plan.outputs().size()) {
            return plan;
        }
        List<Type> declared = declaredKinds(shapeInfo, plan.outputs().size());
        if (declared == null || declared.stream().noneMatch(Type::wireDecided)) {
            return plan;
        }
        String sql = dialect.render(whole);
        List<ReportedColumn> reported = memo.computeIfAbsent(sql, s -> reported(s, conn));
        if (reported.size() != plan.outputs().size()) {
            return plan;
        }
        List<SqlSelect.Projection> projections = new ArrayList<>(ps.projections());
        List<OutputCol> outputs = new ArrayList<>(plan.outputs());
        boolean changed = false;
        for (int i = 0; i < outputs.size(); i++) {
            SqlType wire = reported.get(i).type();
            if (wire == null || !Type.wireDecided(declared.get(i))) {
                continue;
            }
            SqlSelect.Projection p = projections.get(i);
            OutputCol col = outputs.get(i);
            Type wireKind = Type.kindOfSqlType(wire);
            if (wireKind == null || wireKind == Type.kindOfSqlType(col.type())) {
                continue;
            }
            // a BARE store-column reference only: its stamp is the store's
            // declaration, the one fact the fixture can contradict. A
            // computed expression keeps the compiler's own type — and is
            // never cast: H2 reports a computed DECIMAL at scale 0 while
            // the cell carries the value's real scale (the calendar rows)
            if (!(p.expr() instanceof SqlExpr.Column ref) || labelCarrier(col.type())) {
                Census.inc(Census.Key.WIRE_SLOT_SKEW);   // the slot stands; counted
                continue;
            }
            Census.inc(Census.Key.WIRE_RETYPED);
            if (System.getenv("LEGEND_LITE_DUMP_SQL") != null) {
                System.err.println("[wire] " + col.name() + ": stamped " + col.type()
                        + ", the database reports " + wire);
            }
            // the reference re-typed, the value untouched (no cast)
            SqlExpr.Column retyped = SqlExpr.Column.of(ref.table(), ref.name(), wire,
                    col.nullable(), java.util.Objects.requireNonNullElse(ref.origin(),
                            OutputCol.Origin.DERIVED));
            projections.set(i, new SqlSelect.Projection(retyped, p.alias(),
                    p.out() == null ? null : withType(p.out(), wire)));
            outputs.set(i, withType(col, wire));
            changed = true;
        }
        return changed ? ps.withProjections(projections, outputs) : plan;
    }

    private static OutputCol withType(OutputCol c, SqlType t) {
        return new OutputCol(c.name(), t, c.nullable(), c.origin());
    }

    /** The declared kind per output: the relation's columns for a grid,
     * the root type for a one-column value; null when the shape does not
     * name one kind per output. */
    private static @com.legend.base.Nullable List<Type> declaredKinds(ExprType shapeInfo, int width) {
        Type.RelationType schema = Type.schemaView(shapeInfo.type());
        if (schema != null) {
            if (schema.isLateBound() || schema.columns().size() != width) {
                return null;
            }
            List<Type> kinds = new ArrayList<>(width);
            for (Type.Column c : schema.columns()) {
                kinds.add(c.type());
            }
            return kinds;
        }
        return width == 1 ? List.of(shapeInfo.type()) : null;
    }

    private static boolean labelCarrier(SqlType t) {
        return t == SqlType.Scalar.LITERAL || t == SqlType.Scalar.TEMPORAL_TEXT
                || t == SqlType.Scalar.DECIMAL_TEXT || t == SqlType.Scalar.JSON;
    }

    /** The database's reported type per column — a prepare, no execution;
     * a column outside the vocabulary reports null; a statement the
     * database cannot prepare raises the data error the verdict would have. */
    private static List<ReportedColumn> reported(String sql, Connection conn) {
        try (var st = conn.prepareStatement(sql)) {
            ResultSetMetaData md = st.getMetaData();
            if (md == null) {
                return Collections.emptyList();
            }
            List<ReportedColumn> out = new ArrayList<>(md.getColumnCount());
            for (int i = 1; i <= md.getColumnCount(); i++) {
                out.add(new ReportedColumn(md.getColumnLabel(i),
                        sqlTypeOf(md.getColumnType(i), md.getPrecision(i), md.getScale(i)),
                        md.isNullable(i) != ResultSetMetaData.columnNoNulls));
            }
            return out;
        } catch (SQLException e) {
            // the seam: java.sql stops at this probe's boundary — a
            // statement the database cannot prepare cannot judge either
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /** JDBC's type code to the SQL vocabulary (the kinds
     * {@link Type#kindOfSqlType} names). */
    static @com.legend.base.Nullable SqlType sqlTypeOf(int jdbcType, int precision, int scale) {
        return switch (jdbcType) {
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR,
                    Types.LONGNVARCHAR -> SqlType.Scalar.VARCHAR;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> SqlType.Scalar.INTEGER;
            case Types.BIGINT -> SqlType.Scalar.BIGINT;
            case Types.FLOAT, Types.REAL, Types.DOUBLE -> SqlType.Scalar.DOUBLE;
            case Types.DECIMAL, Types.NUMERIC -> precision > 0
                    ? new SqlType.Decimal(precision, Math.max(scale, 0)) : null;
            case Types.BIT, Types.BOOLEAN -> SqlType.Scalar.BOOLEAN;
            case Types.DATE -> SqlType.Scalar.DATE;
            case Types.TIMESTAMP -> SqlType.Scalar.TIMESTAMP;
            case Types.TIMESTAMP_WITH_TIMEZONE -> SqlType.Scalar.TIMESTAMPTZ;
            default -> null;
        };
    }
}
