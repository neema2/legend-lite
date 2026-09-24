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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * OUTER-ROW date over a UNION parent (corpus
 * testBusinessDateInjectionFromVarReferenceWithUnion): the union'd
 * CarDetails navigates {@code car($x.time)} into a milestoned target —
 * the window must compose against the PARENT row's own {@code time}
 * column (engine golden: from_z &lt;= "root".time inside the join ON),
 * never stamp the target pipe (the target lacks 'time').
 */
class ResolveUnionOuterDateProbeTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class u::CarDetails { id: Integer[1]; time: Date[1]; car: u::Car[0..1]; }
            Class <<temporal.businesstemporal>> u::Car { id: Integer[1]; description: String[1]; bicycle: u::Bicycle[0..1]; }
            Class u::Bicycle { id: Integer[1]; bdescription: String[1]; }
            ###Relational
            Database u::DB (
              Table CarDetails (id INTEGER PRIMARY KEY, time DATE)
              Table Cars (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                id INTEGER PRIMARY KEY, description VARCHAR(200),
                from_z DATE, thru_z DATE)
              Table Bicycles (id INTEGER PRIMARY KEY, bdescription VARCHAR(200))
              Join Car_CarDetails (CarDetails.id = Cars.id)
              Join CarBicycle (Cars.id = Bicycles.id)
            )
            ###Mapping
            Mapping u::M (
              *u::CarDetails : Operation { %s(c1, c2) }
              u::CarDetails[c1] : Relational { ~mainTable [u::DB] CarDetails
                id: CarDetails.id,
                time: CarDetails.time,
                car: [u::DB] @Car_CarDetails }
              u::CarDetails[c2] : Relational { ~mainTable [u::DB] CarDetails
                id: CarDetails.id,
                time: CarDetails.time,
                car: [u::DB] @Car_CarDetails }
              *u::Car : Relational { ~mainTable [u::DB] Cars
                id: Cars.id,
                description: Cars.description,
                bicycle: [u::DB] @CarBicycle }
              *u::Bicycle : Relational { ~mainTable [u::DB] Bicycles
                id: Bicycles.id,
                bdescription: Bicycles.bdescription }
            )
            ###Runtime
            Runtime u::RT { mappings: [u::M]; }
            """).formatted(UNION_FQN);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE CarDetails (id INTEGER, time DATE)");
            st.execute("CREATE TABLE Cars (id INTEGER, description VARCHAR,"
                    + " from_z DATE, thru_z DATE)");
            st.execute("INSERT INTO CarDetails VALUES"
                    + " (1, DATE '2020-06-01'), (2, DATE '2020-06-01')");
            // car 1's window covers 2020-06-01; car 2's does not
            st.execute("INSERT INTO Cars VALUES"
                    + " (1, 'Sedan', DATE '2020-01-01', DATE '9999-12-31'),"
                    + " (2, 'Truck', DATE '2021-01-01', DATE '9999-12-31')");
            st.execute("CREATE TABLE Bicycles (id INTEGER, bdescription VARCHAR)");
            st.execute("INSERT INTO Bicycles VALUES (1, 'Roadster'), (2, 'BMX')");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static String sqlOf(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved);
        return new DuckDb().render(plan);
    }

    private List<String> exec(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append("|");
                    }
                    b.append(rs.getObject(i));
                }
                rows.add(b.toString());
            }
        }
        return rows;
    }

    @Test
    @DisplayName("union parent, milestoned target, outer-row date composes into the ON")
    void outerDateOverUnionParent() throws SQLException {
        String sql = sqlOf("u::CarDetails.all()"
                + "->project([x|$x.id, x|$x.car($x.time).description,"
                + " x|$x.car($x.time).bicycle.bdescription],"
                + " ['id', 'car', 'bike'])->from(u::M, u::RT)");
        System.out.println("[union-outer-date]\n" + sql);
        // engine rows: each CarDetails row appears once per member; car 1
        // resolves (window covers time), car 2 reads NULL
        assertEquals(List.of(
                "1|Sedan|Roadster", "1|Sedan|Roadster",
                "2|null|null", "2|null|null"),
                exec(sql + " ORDER BY 1, 2"), sql);
    }
}
