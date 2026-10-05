// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import com.legend.testing.Rows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * V4 mini-probe (Leg 3, task #82): an ASSOCIATION with PER-PAIR routes
 * into a union target where one route is a CHAINED join (@X0_A > @A_Y1)
 * — the engine push-into-arm shape. Each case runs the emitted SQL on DuckDB
 * over one row per route and asserts every route's row arrives, once (Bazel
 * workplan P3-17: it printed the SQL and asserted nothing).
 */
class ResolveUnionV4ProbeTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL2 = ("""
            Class <<temporal.businesstemporal>> v::X { pk: Integer[1]; }
            Class <<temporal.businesstemporal>> v::Y { pk: Integer[1]; }
            Class <<temporal.businesstemporal>> v::Z { pk: Integer[1]; }
            Association v::XY { x: v::X[1]; y: v::Y[*]; }
            Association v::YZ { yy: v::Y[1]; z: v::Z[*]; }
            ###Relational
            Database v::DB (
              Table xT0 (pk INTEGER PRIMARY KEY, fk INTEGER, fk1 INTEGER,
                from_z DATE, thru_z DATE)
              Table yT0 (pk INTEGER PRIMARY KEY, fk INTEGER, zfk INTEGER,
                from_z DATE, thru_z DATE)
              Table yT1 (pk INTEGER PRIMARY KEY, yfk1 INTEGER, gfk INTEGER,
                from_z DATE, thru_z DATE)
              Table zT0 (pk INTEGER PRIMARY KEY, fk INTEGER,
                from_z DATE, thru_z DATE)
              Table zT1 (pk INTEGER PRIMARY KEY, zfk1 INTEGER,
                from_z DATE, thru_z DATE)
              Table yT2 (pk INTEGER PRIMARY KEY, cfk INTEGER,
                from_z DATE, thru_z DATE)
              Table zT2 (pk INTEGER PRIMARY KEY, ifk INTEGER,
                from_z DATE, thru_z DATE)
              Table aT (fk1 INTEGER, afk INTEGER)
              Table gT (fk0 INTEGER, fk1 INTEGER)
              Table bT (fk0 INTEGER, fk1 INTEGER)
              Table cT (fk0 INTEGER, fk1 INTEGER)
              Table hT (fk0 INTEGER, fk1 INTEGER)
              Table iT (fk0 INTEGER, fk1 INTEGER)
              Join X0_Y0 (xT0.fk = yT0.fk)
              Join X0_A (xT0.fk1 = aT.fk1)
              Join A_Y1 (aT.afk = yT1.yfk1)
              Join Y0_Z0 (yT0.zfk = zT0.fk)
              Join Y1_G (yT1.gfk = gT.fk0)
              Join G_Z1 (gT.fk1 = zT1.zfk1)
              Join X0_B (xT0.fk1 = bT.fk0)
              Join B_C (bT.fk1 = cT.fk0)
              Join C_Y2 (cT.fk1 = yT2.cfk)
              Join Y2_H (yT2.cfk = hT.fk0)
              Join H_I (hT.fk1 = iT.fk0)
              Join I_Z2 (iT.fk1 = zT2.ifk)
            )
            ###Mapping
            Mapping v::CM (
              *v::X[x0] : Relational { ~mainTable [v::DB] xT0 pk: xT0.pk }
              *v::Y[y0] : Relational { ~mainTable [v::DB] yT0 pk: yT0.pk }
              v::Y[y1] : Relational { ~mainTable [v::DB] yT1 pk: yT1.pk }
              v::Y[y2] : Relational { ~mainTable [v::DB] yT2 pk: yT2.pk }
              *v::Z[z0] : Relational { ~mainTable [v::DB] zT0 pk: zT0.pk }
              v::Z[z1] : Relational { ~mainTable [v::DB] zT1 pk: zT1.pk }
              v::Z[z2] : Relational { ~mainTable [v::DB] zT2 pk: zT2.pk }
            )
            Mapping v::AM (
              include v::CM
              v::XY : Relational { AssociationMapping (
                x[y0, x0] : [v::DB] @X0_Y0,
                y[x0, y0] : [v::DB] @X0_Y0,
                x[y1, x0] : [v::DB] @A_Y1 > @X0_A,
                y[x0, y1] : [v::DB] @X0_A > @A_Y1,
                x[y2, x0] : [v::DB] @C_Y2 > @B_C > @X0_B,
                y[x0, y2] : [v::DB] @X0_B > @B_C > @C_Y2 ) }
              v::YZ : Relational { AssociationMapping (
                yy[z0, y0] : [v::DB] @Y0_Z0,
                z[y0, z0] : [v::DB] @Y0_Z0,
                yy[z1, y1] : [v::DB] @G_Z1 > @Y1_G,
                z[y1, z1] : [v::DB] @Y1_G > @G_Z1,
                yy[z2, y2] : [v::DB] @I_Z2 > @H_I > @Y2_H,
                z[y2, z2] : [v::DB] @Y2_H > @H_I > @I_Z2 ) }
            )
            Mapping v::M2 (
              include v::AM
              *v::Y : Operation { %s(y0, y1) }
              *v::Z : Operation { %s(z0, z1) }
            )
            ###Runtime
            Runtime v::RT2 { mappings: [v::M2]; }
            """).formatted(UNION_FQN, UNION_FQN);

    @Test
    @DisplayName("V4 mini 2-hop: chained routes across TWO unions")
    void v4TwoUnionHops() throws SQLException {
        String sql = sqlOf(MODEL2, "v::X.all(%2018-1-1)"
                + "->project([x|$x.pk, x|$x.y.pk, x|$x.y.z.pk], ['xpk','ypk','zpk'])"
                + "->from(v::M2, v::RT2)");
        // x1 reaches y100 (y0, X0_Y0) and y200 (y1, X0_A > A_Y1); y101 and y201 match no route: a join that lost
        // its condition would bring them in; y100 reaches z500 (z0, Y0_Z0), y200 z600
        // (z1, Y1_G > G_Z1); every milestoned row is live on 2018-01-01. y101, y201, z501 and z601 match no route:
        // a join that lost its condition would bring them in
        Rows.assertSameRows(List.of(List.of(1, 100, 500), List.of(1, 200, 600)), rows(sql,
                "CREATE TABLE xT0 (pk INTEGER PRIMARY KEY, fk INTEGER, fk1 INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE yT0 (pk INTEGER PRIMARY KEY, fk INTEGER, zfk INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE yT1 (pk INTEGER PRIMARY KEY, yfk1 INTEGER, gfk INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE zT0 (pk INTEGER PRIMARY KEY, fk INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE zT1 (pk INTEGER PRIMARY KEY, zfk1 INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE \"aT\" (fk1 INTEGER, afk INTEGER)",
                "CREATE TABLE gT (fk0 INTEGER, fk1 INTEGER)",
                "INSERT INTO xT0 VALUES (1, 10, 20, DATE '2017-01-01', DATE '2019-01-01')",
                "INSERT INTO yT0 VALUES (100, 10, 50, DATE '2017-01-01', DATE '2019-01-01'),"
                        + " (101, 11, 51, DATE '2017-01-01', DATE '2019-01-01')",
                "INSERT INTO yT1 VALUES (200, 30, 60, DATE '2017-01-01', DATE '2019-01-01'),"
                        + " (201, 31, 61, DATE '2017-01-01', DATE '2019-01-01')",
                "INSERT INTO zT0 VALUES (500, 50, DATE '2017-01-01', DATE '2019-01-01'),"
                        + " (501, 52, DATE '2017-01-01', DATE '2019-01-01')",
                "INSERT INTO zT1 VALUES (600, 70, DATE '2017-01-01', DATE '2019-01-01'),"
                        + " (601, 71, DATE '2017-01-01', DATE '2019-01-01')",
                "INSERT INTO \"aT\" VALUES (20, 30)",
                "INSERT INTO gT VALUES (60, 70)"));
    }

    /** {@code sql}'s rows on a fresh DuckDB after {@code setup}. */
    private static List<List<Object>> rows(String sql, String... setup) throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            for (String s : setup) {
                st.execute(s);
            }
            List<List<Object>> out = new ArrayList<>();
            try (ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= n; i++) {
                        row.add(rs.getObject(i));
                    }
                    out.add(row);
                }
            }
            return out;
        }
    }

    private static final String MODEL = ("""
            Class v::X { pk: Integer[1]; }
            Class v::Y { pk: Integer[1]; }
            Association v::XY { x: v::X[1]; y: v::Y[*]; }
            ###Relational
            Database v::DB (
              Table xT0 (pk INTEGER PRIMARY KEY, fk INTEGER, fk1 INTEGER)
              Table yT0 (pk INTEGER PRIMARY KEY, fk INTEGER)
              Table yT1 (pk INTEGER PRIMARY KEY, yfk1 INTEGER)
              Table aT (fk1 INTEGER, afk INTEGER)
              Join X0_Y0 (xT0.fk = yT0.fk)
              Join X0_A (xT0.fk1 = aT.fk1)
              Join A_Y1 (aT.afk = yT1.yfk1)
            )
            ###Mapping
            Mapping v::M (
              *v::Y : Operation { %s(y0, y1) }
              v::X[x0] : Relational { ~mainTable [v::DB] xT0 pk: xT0.pk }
              v::Y[y0] : Relational { ~mainTable [v::DB] yT0 pk: yT0.pk }
              v::Y[y1] : Relational { ~mainTable [v::DB] yT1 pk: yT1.pk }
              v::XY : Relational { AssociationMapping (
                y[x0, y0] : [v::DB] @X0_Y0,
                y[x0, y1] : [v::DB] @X0_A > @A_Y1,
                x[y0, x0] : [v::DB] @X0_Y0,
                x[y1, x0] : [v::DB] @A_Y1 > @X0_A ) }
            )
            ###Runtime
            Runtime v::RT { mappings: [v::M]; }
            """).formatted(UNION_FQN);

    private static String sqlOf(String query) {
        return sqlOf(MODEL, query);
    }

    private static String sqlOf(String model, String query) {
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved);
        return new DuckDb().render(plan);
    }

    @Test
    @DisplayName("V4 mini: per-pair chained routes into a union target")
    void v4ChainedPairRoutes() throws SQLException {
        String sql = sqlOf("v::X.all()"
                + "->project([x|$x.pk, x|$x.y.pk], ['xpk','ypk'])"
                + "->from(v::M, v::RT)");
        // x1 reaches y100 (y0, X0_Y0) and y200 (y1, X0_A > A_Y1)
        Rows.assertSameRows(List.of(List.of(1, 100), List.of(1, 200)), rows(sql,
                "CREATE TABLE xT0 (pk INTEGER PRIMARY KEY, fk INTEGER, fk1 INTEGER)",
                "CREATE TABLE yT0 (pk INTEGER PRIMARY KEY, fk INTEGER)",
                "CREATE TABLE yT1 (pk INTEGER PRIMARY KEY, yfk1 INTEGER)",
                "CREATE TABLE \"aT\" (fk1 INTEGER, afk INTEGER)",
                "INSERT INTO xT0 VALUES (1, 10, 20)",
                "INSERT INTO yT0 VALUES (100, 10), (101, 11)",
                "INSERT INTO yT1 VALUES (200, 30), (201, 31)",
                "INSERT INTO \"aT\" VALUES (20, 30)"));
    }
}
