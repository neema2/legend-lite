package com.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A store column's DECLARED type, from the model to the SQL (docs/STORE_TYPES_HOMEWORK_2026_10_02.md).
 *
 * <p>Step 1, the typing (ruled 2026-10-02): a type Pure cannot name -- OTHER, DISTINCT -- is a String;
 * a nested value -- ARRAY, OBJECT, SEMISTRUCTURED -- is a Variant. Until then a table holding one
 * OTHER column did not compile at all, even for a query that never read it (step 0's probe, now
 * these). A milestoning date declared OTHER stays refused: compared as text it would be silently wrong.
 *
 * <p>Step 3, the stamp: every scan of a table carries its columns' declared types, by whichever road
 * the query reached it -- the relation accessor, a class mapping, a join hop, a view -- because the
 * dialect reads a stored value at every reference (step 4).
 *
 * <p>Step 4, the read: a column stored as a type Pure cannot name is read as text at every
 * reference -- projected, filtered, grouped, spelled out of a star -- on every dialect; Postgres
 * also reads a nested value as {@code jsonb}. Run on DuckDB, a UUID column declared OTHER filters
 * and groups as the text it shows.
 */
class StoreTypesTest {

    private static final String MODEL = """
            ###Pure
            Class s::Host { id: Integer[1]; name: String[0..1]; addr: String[0..1]; }
            ###Relational
            Database s::DB ( Table HOSTS (ID INTEGER PRIMARY KEY, NAME VARCHAR(32), ADDR OTHER,
                TAGS ARRAY, META SEMISTRUCTURED) )
            ###Mapping
            Mapping s::M (
              *s::Host: Relational { ~mainTable [s::DB] HOSTS
                id: HOSTS.ID, name: HOSTS.NAME, addr: HOSTS.ADDR }
            )
            ###Connection
            RelationalDatabaseConnection s::Conn { store: s::DB; type: DuckDB;
              specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    @Test
    void aTableHoldingAnOtherColumnCompilesThroughTheAccessor() {
        // step 0 pinned "has no scalar Pure type" here, for a query that never read ADDR
        assertTrue(Compiler.query(Compiler.compileModel(MODEL), "#>{s::DB.HOSTS}#->select(~[ID])").plan("s::RT").sql().contains("ID"));
    }

    @Test
    void aTableHoldingAnOtherColumnCompilesThroughAClassMapping() {
        assertTrue(Compiler.query(Compiler.compileModel(MODEL), "s::Host.all()->project(~[id: h|$h.id])").plan("s::RT").sql().contains("ID"));
        // and a property mapped TO it is a String property, as declared
        assertTrue(Compiler.query(Compiler.compileModel(MODEL), "s::Host.all()->project(~[addr: h|$h.addr])").plan("s::RT").sql().contains("ADDR"));
    }

    @Test
    void aTypePureCannotNameIsAStringAndANestedValueIsAVariant() {
        Map<String, String> types = columnTypes("#>{s::DB.HOSTS}#");
        assertEquals("String", types.get("ADDR"), types.toString());
        // the grammar's ARRAY keyword parses to OTHER, as upstream's does (DatabaseProtocolParser,
        // RelationalParseTreeWalker): written in a model it is a type Pure cannot name. A typed
        // Array -- what a database's own catalog yields -- is a Variant (StoreCompilerTypesTest).
        assertEquals("String", types.get("TAGS"), types.toString());
        assertEquals("Variant", types.get("META"), types.toString());
        assertEquals("Integer", types.get("ID"), types.toString());
    }

    @Test
    void aViewOverAnOtherColumnTypesItAsAString() {
        String model = MODEL.replace("META SEMISTRUCTURED) )",
                "META SEMISTRUCTURED)\n    View HOST_ADDRS (id: HOSTS.ID, addr: HOSTS.ADDR) )");
        assertEquals("String", columnTypes(model, "#>{s::DB.HOST_ADDRS}#").get("addr"));
    }

    @Test
    void aMilestoningDateDeclaredOtherIsRefusedByName() {
        String model = """
                ###Relational
                Database s::Hist ( Table PRICES ( milestoning( business(BUS_FROM = FROM_Z, BUS_THRU = THRU_Z) )
                    ID INTEGER PRIMARY KEY, FROM_Z OTHER, THRU_Z DATE ) )
                """;
        RuntimeException e = assertThrows(RuntimeException.class, () -> Compiler.compileModel(model));
        assertTrue(String.valueOf(e.getMessage()).contains("milestoning column 'FROM_Z'"), e.getMessage());
    }

    private static final String JOIN_MODEL = """
            ###Pure
            Class s::Host { id: Integer[1]; loc: String[0..1]; }
            ###Relational
            Database s::DB ( Table HOSTS (ID INTEGER PRIMARY KEY)
                Table SITES (ID INTEGER PRIMARY KEY, HOST_ID INTEGER, LOC OTHER)
                Join HostSite(HOSTS.ID = SITES.HOST_ID) )
            ###Mapping
            Mapping s::M (
              *s::Host: Relational { ~mainTable [s::DB] HOSTS
                id: HOSTS.ID, loc: @HostSite | SITES.LOC }
            )
            ###Connection
            RelationalDatabaseConnection s::Conn { store: s::DB; type: DuckDB;
              specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    private static final SqlDdl.ColumnType OTHER = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.OTHER);
    private static final SqlDdl.ColumnType JSON = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.JSON);

    @Test
    void theAccessorsScanCarriesEveryColumnsDeclaredType() {
        Map<String, SqlDdl.ColumnType> stored = scanOf(MODEL, "#>{s::DB.HOSTS}#->select(~[ID])", "HOSTS");
        assertEquals(OTHER, stored.get("ADDR"), stored.toString());
        assertEquals(JSON, stored.get("META"), stored.toString());
        assertEquals(new SqlDdl.ColumnType.Sized("VARCHAR", 32), stored.get("NAME"), stored.toString());
        assertEquals(new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER), stored.get("ID"), stored.toString());
        assertEquals(5, stored.size(), stored.toString());
    }

    @Test
    void aClassMappingsScanCarriesThem() {
        assertEquals(OTHER, scanOf(MODEL, "s::Host.all()->project(~[addr: h|$h.addr])", "HOSTS").get("ADDR"));
    }

    @Test
    void aJoinHopsScanCarriesThem() {
        assertEquals(OTHER, scanOf(JOIN_MODEL, "s::Host.all()->project(~[loc: h|$h.loc])", "SITES").get("LOC"));
    }

    @Test
    void aViewsScanOfItsTableCarriesThem() {
        String model = MODEL.replace("META SEMISTRUCTURED) )",
                "META SEMISTRUCTURED)\n    View HOST_ADDRS (id: HOSTS.ID, addr: HOSTS.ADDR) )");
        assertEquals(OTHER, scanOf(model, "#>{s::DB.HOST_ADDRS}#", "HOSTS").get("ADDR"));
    }

    // ---- step 4: the read ----

    /** {@code CAST(<qualified ref to col> AS VARCHAR)}, however the dialect quotes the reference. */
    private static String textRead(String col) {
        return "CAST\\(\"?\\w+\"?\\.\"?" + col + "\"?\\s+AS VARCHAR\\)";
    }

    private static String sql(String model, String query) {
        return Compiler.query(Compiler.compileModel(model), query).plan("s::RT").sql();
    }

    private static String on(String type) {
        return type.equals("H2") ? MODEL.replace("type: DuckDB;\n  specification: DuckDB { }; auth: Test;",
                "type: H2;\n  specification: LocalH2 { }; auth: DefaultH2;") : MODEL.replace("type: DuckDB;", "type: " + type + ";");
    }

    @Test
    void anOtherColumnIsReadAsTextOnEveryDialect() {
        for (String type : List.of("DuckDB", "H2", "Postgres")) {
            String q = sql(on(type), "#>{s::DB.HOSTS}#->select(~[ID, ADDR])");
            assertTrue(java.util.regex.Pattern.compile(textRead("ADDR")).matcher(q).find(), type + ": " + q);
        }
    }

    @Test
    void theReadAppliesAtEveryReference() {
        String q = sql(MODEL, "#>{s::DB.HOSTS}#->filter(x|$x.ADDR->toOne()->contains('10.'))"
                + "->groupBy(~[ADDR], ~[n: x|$x.ID : y|$y->count()])->sort([~ADDR->ascending()])");
        var m = java.util.regex.Pattern.compile(textRead("ADDR")).matcher(q);
        int reads = 0;
        while (m.find()) {
            reads++;
        }
        // the filter, the group key, and its projection, at least
        assertTrue(reads >= 3, q);
        // and no reference reads the raw value
        assertFalse(java.util.regex.Pattern.compile("\\w\\.ADDR\\b").matcher(q.replaceAll(textRead("ADDR"), "")).find(), q);
    }

    @Test
    void aWholeTableSpellsItsColumnsOutSoTheReadReachesThem() {
        String q = sql(MODEL, "#>{s::DB.HOSTS}#");
        assertTrue(java.util.regex.Pattern.compile(textRead("ADDR") + " AS ADDR").matcher(q).find(), q);
        assertFalse(q.contains("*"), q);
    }

    @Test
    void aClassMappingAJoinHopAndAViewReadIt() {
        assertTrue(java.util.regex.Pattern.compile(textRead("ADDR")).matcher(
                sql(MODEL, "s::Host.all()->project(~[addr: h|$h.addr])")).find());
        assertTrue(java.util.regex.Pattern.compile(textRead("LOC")).matcher(
                sql(JOIN_MODEL, "s::Host.all()->project(~[loc: h|$h.loc])")).find());
        String view = MODEL.replace("META SEMISTRUCTURED) )",
                "META SEMISTRUCTURED)\n    View HOST_ADDRS (id: HOSTS.ID, addr: HOSTS.ADDR) )");
        assertTrue(java.util.regex.Pattern.compile(textRead("ADDR")).matcher(
                sql(view, "#>{s::DB.HOST_ADDRS}#")).find());
    }

    @Test
    void aNestedValueIsReadAsStoredOnDuckDbAndAsJsonbOnPostgres() {
        String duck = sql(MODEL, "#>{s::DB.HOSTS}#->select(~[META])");
        assertFalse(duck.contains("JSONB"), duck);
        String pg = sql(on("Postgres"), "#>{s::DB.HOSTS}#->select(~[META])");
        assertTrue(java.util.regex.Pattern.compile("CAST\\(\"\\w+\"\\.\"META\" AS JSONB\\)").matcher(pg).find(), pg);
    }

    @Test
    void aTableWithoutSuchAColumnRendersAsItDid() {
        String plain = """
                ###Relational
                Database s::DB ( Table HOSTS (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; type: DuckDB;
                  specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        String q = sql(plain, "#>{s::DB.HOSTS}#");
        assertFalse(q.contains("CAST"), q);
    }

    @Test
    void aUuidDeclaredOtherFiltersAndGroupsAsTheTextItShowsOnDuckDb() throws Exception {
        String model = """
                ###Relational
                Database s::DB ( Table HOSTS (ID INTEGER PRIMARY KEY, REF OTHER) )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; type: DuckDB;
                  specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE HOSTS (ID INTEGER, REF UUID)");
                st.execute("INSERT INTO HOSTS VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),"
                        + " (2, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), (3, 'b1ffcd00-0000-4000-8000-000000000001')");
            }
            var r = Execution.execute(model, "|#>{s::DB.HOSTS}#->filter(x|$x.REF->toOne()->startsWith('a0ee'))"
                    + "->groupBy(~[REF], ~[n: x|$x.ID : y|$y->count()])", "s::RT", c);
            assertEquals(List.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|2"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1)).toList());
        }
    }

    /** The stored types the ONE scan of {@code table} carries, in the query's lowered MIR. */
    private static Map<String, SqlDdl.ColumnType> scanOf(String model, String query, String table) {
        SqlQuery lowered = Compiler.lowerResolved(
                com.legend.compiler.NameResolver.resolveQuery(com.legend.testing.Own.spec(query)),
                Compiler.compileModel(model), "s::RT", false);
        List<SqlSource.Table> scans = new ArrayList<>();
        new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Table t && t.name().equals(table)) {
                    scans.add(t);
                }
                return s;
            }
        }.rewrite(lowered);
        assertEquals(1, scans.size(), "scans of " + table + ": " + scans);
        return scans.get(0).storedTypes();
    }

    private static Map<String, String> columnTypes(String query) {
        return columnTypes(MODEL, query);
    }

    private static Map<String, String> columnTypes(String model, String query) {
        Type.RelationType rt = Type.schemaView(Compiler.query(Compiler.compileModel(model), query).expression().info().type());
        Map<String, String> out = new LinkedHashMap<>();
        for (Type.Column c : rt.columns()) {
            out.put(c.name(), c.type() instanceof Type.ClassType ct ? ct.fqn().replaceAll(".*::", "")
                    : c.type() instanceof Type.Primitive p ? p.typeName() : c.type().toString());
        }
        return out;
    }
}
