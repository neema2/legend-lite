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
 * Union JTC probe (testProjectAndFilterSamePropertySameJoinInUnion): both
 * union members read the SAME main table with join-terminal-column PMs
 * ({@code @J | T.col}) — each member thread must carry its OWN joined
 * read; plus per-member routed firm navigation used in filter AND project.
 */
class ResolveUnionJtcProbeTest {

    private static final String UNION_FQN =
            "meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = ("""
            Class j::Person { firstName: String[1]; lastName: String[1]; otherNames: String[0..1];
              extraInformation: String[0..1]; }
            Class j::Firm { legalName: String[1]; }
            Association j::PF { firm: j::Firm[0..1]; employees: j::Person[*]; }
            ###Relational
            Database j::DB (
              Table PersonMaster (ID INTEGER PRIMARY KEY, firstName VARCHAR(200), lastName VARCHAR(200), FirmID INTEGER)
              Table PersonAdditional (ID INTEGER PRIMARY KEY, otherName VARCHAR(200), extrainfo VARCHAR(200))
              Table FirmSet1 (ID INTEGER PRIMARY KEY, name VARCHAR(200))
              Table FirmSet2 (ID INTEGER PRIMARY KEY, name VARCHAR(200))
              Join PersonMasterPersonAdditional (PersonMaster.ID = PersonAdditional.ID)
              Join PersonMasterFirmSet1 (PersonMaster.FirmID = FirmSet1.ID)
              Join PersonMasterFirmSet2 (PersonMaster.FirmID = FirmSet2.ID)
            )
            ###Mapping
            Mapping j::M (
              *j::Firm : Operation { %s(FirmSet1, FirmSet2) }
              *j::Person : Operation { %s(set1, set2) }
              j::Firm[FirmSet1] : Relational { ~mainTable [j::DB] FirmSet1
                legalName: FirmSet1.name,
                employees[FirmSet1, set1]: [j::DB] @PersonMasterFirmSet1 }
              j::Firm[FirmSet2] : Relational { ~mainTable [j::DB] FirmSet2
                legalName: FirmSet2.name,
                employees[FirmSet2, set2]: [j::DB] @PersonMasterFirmSet2 }
              j::Person[set1] : Relational { ~mainTable [j::DB] PersonMaster
                firstName: PersonMaster.firstName,
                lastName: PersonMaster.lastName,
                extraInformation: [j::DB] @PersonMasterPersonAdditional | PersonAdditional.extrainfo,
                otherNames: [j::DB] @PersonMasterPersonAdditional | PersonAdditional.otherName,
                firm[set1, FirmSet1]: [j::DB] @PersonMasterFirmSet1 }
              j::Person[set2] : Relational { ~mainTable [j::DB] PersonMaster
                firstName: PersonMaster.firstName,
                lastName: PersonMaster.lastName,
                extraInformation: [j::DB] @PersonMasterPersonAdditional | PersonAdditional.extrainfo,
                otherNames: [j::DB] @PersonMasterPersonAdditional | PersonAdditional.otherName,
                firm[set2, FirmSet2]: [j::DB] @PersonMasterFirmSet2 }
            )
            ###Runtime
            Runtime j::RT { mappings: [j::M]; }
            """).formatted(UNION_FQN, UNION_FQN);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE PersonMaster (ID INTEGER, firstName VARCHAR, lastName VARCHAR, FirmID INTEGER)");
            st.execute("CREATE TABLE PersonAdditional (ID INTEGER, otherName VARCHAR, extrainfo VARCHAR)");
            st.execute("CREATE TABLE FirmSet1 (ID INTEGER, name VARCHAR)");
            st.execute("CREATE TABLE FirmSet2 (ID INTEGER, name VARCHAR)");
            st.execute("INSERT INTO PersonMaster VALUES (1,'P','Scott',10),(2,'D','Wright',20),(3,'N','NoFirm',99)");
            st.execute("INSERT INTO PersonAdditional VALUES"
                    + " (1,'GDPR Redacted','Not Available'),"
                    + " (2,'GDPR Redacted','Not Available'),(3,'X','Other')");
            st.execute("INSERT INTO FirmSet1 VALUES (10,'Firm X')");
            st.execute("INSERT INTO FirmSet2 VALUES (20,'Firm A')");
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
    @DisplayName("union JTC props + routed firm in filter and project")
    void jtcAndRoutedFilterProject() throws SQLException {
        String sql = sqlOf("j::Person.all()"
                + "->filter(p|($p.extraInformation == 'Not Available')"
                + " && !$p.firm.legalName->isEmpty())"
                + "->project([p|$p.lastName, p|$p.otherNames,"
                + " p|$p.firm.legalName, p|$p.extraInformation],"
                + " ['lastName','otherNames','firmName','extraInformation'])"
                + "->from(j::M, j::RT)");
        System.out.println("[jtc-sql]\n" + sql);
        // Scott matches via member 1 (Firm X), Wright via member 2 (Firm A);
        // each row appears once per matching member thread — engine rows:
        // exactly these two.
        assertEquals(List.of(
                "Scott|GDPR Redacted|Firm X|Not Available",
                "Wright|GDPR Redacted|Firm A|Not Available"),
                exec(sql + " ORDER BY 1"), sql);
    }
}
