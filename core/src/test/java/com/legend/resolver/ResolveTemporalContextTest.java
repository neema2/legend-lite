package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.TypeInferenceException;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Temporal-context pins (audit 13): the hop context carries its
 * DIMENSION and PROVENANCE. Each pin guards a wrong-rows class fixed
 * at e123e505 — all were corpus-invisible (engineered shapes).
 */
class ResolveTemporalContextTest {

    /** All-business chain with a milestoned scalar slot on the MID hop. */
    private static final String DRILL_MODEL = """
            Class <<temporal.businesstemporal>> t::Order { id: Integer[1]; mid: t::Mid[1]; }
            Class <<temporal.businesstemporal>> t::Mid { mname: String[1]; descr: String[1]; subq: t::Sub[1]; }
            Class <<temporal.businesstemporal>> t::Sub { sname: String[1]; }
            Database t::DB (
              Table OrderT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, MID INTEGER, from_z DATE, thru_z DATE )
              Table MidT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, mname VARCHAR(64), SID INTEGER, from_z DATE, thru_z DATE )
              Table DescT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                MID INTEGER PRIMARY KEY, descr VARCHAR(64), from_z DATE, thru_z DATE )
              Table SubT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, sname VARCHAR(64), from_z DATE, thru_z DATE )
              Join OM (OrderT.MID = MidT.ID)
              Join MD (MidT.ID = DescT.MID)
              Join MS (MidT.SID = SubT.ID)
            )
            Mapping t::M (
              *t::Order : Relational { ~mainTable [t::DB] OrderT
                id: OrderT.ID, mid: [t::DB]@OM }
              *t::Mid : Relational { ~mainTable [t::DB] MidT
                mname: MidT.mname, descr: @MD | DescT.descr, subq: [t::DB]@MS }
              *t::Sub : Relational { ~mainTable [t::DB] SubT
                sname: SubT.sname }
            )
            Runtime t::RT { mappings: [t::M]; }
            """;

    /** Business chain whose scalar slot table is PROCESSING-milestoned. */
    private static final String XDIM_MODEL = """
            Class <<temporal.businesstemporal>> x::Order { id: Integer[1]; product: x::Product[1]; }
            Class <<temporal.businesstemporal>> x::Product { name: String[1]; descr: String[1]; }
            Database x::DB (
              Table OrderT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, PID INTEGER, from_z DATE, thru_z DATE )
              Table ProdT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, name VARCHAR(64), from_z DATE, thru_z DATE )
              Table DescT (
                milestoning( processing(PROCESSING_IN=in_z, PROCESSING_OUT=out_z) )
                PID INTEGER PRIMARY KEY, descr VARCHAR(64), in_z TIMESTAMP, out_z TIMESTAMP )
              Join OP (OrderT.PID = ProdT.ID)
              Join PD (ProdT.ID = DescT.PID)
            )
            Mapping x::M (
              *x::Order : Relational { ~mainTable [x::DB] OrderT
                id: OrderT.ID, product: [x::DB]@OP }
              *x::Product : Relational { ~mainTable [x::DB] ProdT
                name: ProdT.name, descr: @PD | DescT.descr }
            )
            Runtime x::RT { mappings: [x::M]; }
            """;

    private static String sqlOf(String model, String query) {
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer().lower(resolved);
        return new DuckDb().render(plan);
    }

    private static List<String> exec(String sql, String... ddl) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = conn.createStatement()) {
                for (String d : ddl) {
                    st.execute(d);
                }
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) {
                            sb.append('|');
                        }
                        sb.append(rs.getString(i));
                    }
                    rows.add(sb.toString());
                }
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    @Test
    @DisplayName("an explicitly-dated sub-hop keeps ITS date — no root re-stamp")
    void drilledDatedChainUsesHopDate() throws SQLException {
        String sql = sqlOf(DRILL_MODEL, "|t::Order.all(%2015-06-06)"
                + "->project([o|$o.mid.subq(%2013-06-01).sname], ['s'])"
                + "->from(t::M, t::RT)");
        assertTrue(sql.contains("2013-06-01"), sql);
        // the sub-hop scan must NOT also carry the root date (audit 13
        // F1: the double stamp voided rows valid only at the hop date)
        int subScan = sql.indexOf("SubT");
        assertTrue(subScan > 0, sql);
        assertEquals(List.of("S-2013only"), exec(sql,
                "CREATE TABLE OrderT (ID INTEGER, MID INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE MidT (ID INTEGER, mname VARCHAR, SID INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE DescT (MID INTEGER, descr VARCHAR, from_z DATE, thru_z DATE)",
                "CREATE TABLE SubT (ID INTEGER, sname VARCHAR, from_z DATE, thru_z DATE)",
                "INSERT INTO OrderT VALUES (1, 10, DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO MidT VALUES (10, 'M1', 100, DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO DescT VALUES (10, 'D1', DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO SubT VALUES (100, 'S-2013only', DATE '2012-01-01', DATE '2014-01-01')"),
                "a row valid only at the HOP date must survive:\n" + sql);
    }

    @Test
    @DisplayName("a business context never filters a processing-milestoned slot table")
    void crossDimensionSlotStaysUnfiltered() throws SQLException {
        String sql = sqlOf(XDIM_MODEL, "|x::Order.all(%2015-06-06)"
                + "->project([o|$o.product.descr], ['d'])->from(x::M, x::RT)");
        assertFalse(sql.contains("in_z <=") || sql.contains("out_z >"),
                "processing columns must stay unfiltered under a business"
                        + " context (engine capability rule):\n" + sql);
        assertEquals(List.of("DESC-LOADED-2020"), exec(sql,
                "CREATE TABLE OrderT (ID INTEGER, PID INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE ProdT (ID INTEGER, name VARCHAR, from_z DATE, thru_z DATE)",
                "CREATE TABLE DescT (PID INTEGER, descr VARCHAR, in_z TIMESTAMP, out_z TIMESTAMP)",
                "INSERT INTO OrderT VALUES (1, 100, DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO ProdT VALUES (100, 'ProdA', DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO DescT VALUES (100, 'DESC-LOADED-2020',"
                        + " TIMESTAMP '2020-01-01', TIMESTAMP '9999-12-31')"));
    }

    /** Business-temporal root, NON-temporal target, milestoned MID table
     * in a chained PM — audit 14 F1: the mid table's OWN milestoning
     * filters by the ambient context regardless of the target class. */
    private static final String MID_MODEL = """
            Class <<temporal.businesstemporal>> m::Acct { id: Integer[1]; ref: m::Ref[0..1]; }
            Class m::Ref { rname: String[1]; }
            Database m::DB (
              Table AcctT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, MID INTEGER, from_z DATE, thru_z DATE )
              Table MidT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, RID INTEGER, from_z DATE, thru_z DATE )
              Table RefT ( ID INTEGER PRIMARY KEY, rname VARCHAR(64) )
              Join AM (AcctT.MID = MidT.ID)
              Join MR (MidT.RID = RefT.ID)
            )
            Mapping m::M (
              *m::Acct : Relational { ~mainTable [m::DB] AcctT
                id: AcctT.ID, ref: @AM > @MR }
              *m::Ref : Relational { ~mainTable [m::DB] RefT
                rname: RefT.rname }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    @Test
    @DisplayName("a milestoned MID table stamps by the ambient context even when the target class is non-temporal")
    void midTableStampsUnderNonTemporalTarget() throws SQLException {
        String sql = sqlOf(MID_MODEL, "|m::Acct.all(%2015-06-06)"
                + "->project([a|$a.ref.rname], ['r'])->from(m::M, m::RT)");
        // the mid table has TWO versions of the same link — an unstamped
        // mid join returns one output row per version (audit 14 F1)
        assertEquals(List.of("RefName"), exec(sql,
                "CREATE TABLE AcctT (ID INTEGER, MID INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE MidT (ID INTEGER, RID INTEGER, from_z DATE, thru_z DATE)",
                "CREATE TABLE RefT (ID INTEGER, rname VARCHAR)",
                "INSERT INTO AcctT VALUES (1, 10, DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO MidT VALUES (10, 100, DATE '2015-01-01', DATE '9999-12-31')",
                "INSERT INTO MidT VALUES (10, 100, DATE '2014-01-01', DATE '2015-01-01')",
                "INSERT INTO RefT VALUES (100, 'RefName')"),
                "one row per account, never per mid version:\n" + sql);
    }

    /** Non-temporal association chain for the filter-position lift pin. */
    private static final String FIRM_MODEL = """
            Class f::Firm { legal: String[1]; }
            Class f::Person { firstName: String[1]; lastName: String[1]; }
            Association f::Employment { employer: f::Firm[0..1]; employees: f::Person[*]; }
            Database f::DB (
              Table FirmT ( ID INTEGER PRIMARY KEY, LEGAL VARCHAR(64) )
              Table PersonT ( ID INTEGER PRIMARY KEY, FIRST VARCHAR(64), LAST VARCHAR(64), FIRMID INTEGER )
              Join FP (FirmT.ID = PersonT.FIRMID)
            )
            Mapping f::M (
              *f::Firm : Relational { ~mainTable [f::DB] FirmT
                legal: FirmT.LEGAL }
              *f::Person : Relational { ~mainTable [f::DB] PersonT
                firstName: PersonT.FIRST, lastName: PersonT.LAST }
              f::Employment : Relational { AssociationMapping (
                employer: [f::DB]@FP, employees: [f::DB]@FP ) }
            )
            Runtime f::RT { mappings: [f::M]; }
            """;

    @Test
    @DisplayName("a lifted chain filter under != excludes parents with no matching child")
    void liftedFilterUnderNotEqualExcludesUnmatchedParents() throws SQLException {
        // audit 14 F2: engine parks the outermost chain filter in the outer
        // WHERE for filter position; our in-join placement is row-identical
        // ONLY while not(equal) lowers strict-3VL (no IS NULL disjunct).
        // This pin freezes the OBSERVABLE contract so either half changing
        // alone fails loudly: a firm with employees but NO Smith must be
        // EXCLUDED (its filtered read is NULL, and NULL != 'John' passes
        // nothing), and the matched-but-equal firm is excluded too.
        String sql = sqlOf(FIRM_MODEL, "|f::Firm.all()"
                + "->filter(f|$f.employees->filter(e|$e.lastName == 'Smith')"
                + ".firstName != 'John')"
                + "->project([f|$f.legal], ['l'])->from(f::M, f::RT)");
        assertEquals(List.of("Z Corp"), exec(sql,
                "CREATE TABLE FirmT (ID INTEGER, LEGAL VARCHAR)",
                "CREATE TABLE PersonT (ID INTEGER, FIRST VARCHAR, LAST VARCHAR, FIRMID INTEGER)",
                "INSERT INTO FirmT VALUES (1, 'X Corp'), (2, 'Y Corp'), (3, 'Z Corp')",
                "INSERT INTO PersonT VALUES (1, 'John', 'Smith', 1)",
                "INSERT INTO PersonT VALUES (2, 'Bob', 'Jones', 2)",
                "INSERT INTO PersonT VALUES (3, 'Anna', 'Smith', 3)"),
                "only the firm with a non-John Smith survives:\n" + sql);
    }

    @Test
    @DisplayName("a name-less non-property function column stays LOUD")
    void namelessFunctionColumnStaysLoud() {
        assertThrows(TypeInferenceException.class, () -> sqlOf(XDIM_MODEL,
                "|x::Order.all(%2015-06-06)"
                        + "->project([o|$o.product.name->toUpper()])"
                        + "->from(x::M, x::RT)"));
    }
}
