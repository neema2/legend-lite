package com.legend.sql.dialect;

import com.legend.compiler.element.type.ExprType;
import com.legend.plan.UpstreamRelationType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Database built from DuckDB's own catalog (T2) is real legend-lite: it compiles, and the
 * COMPILER types each column -- every DuckDB type DESCRIBE reports, read by the DuckDB dialect.
 */
class CatalogModelTest {

    /** The compiler reports Variant by its path. */
    private static final String VARIANT = com.legend.compiler.element.type.PlatformTypes.VARIANT;

    private static final Map<String, String> EXPECTED = new LinkedHashMap<>();

    static {
        EXPECTED.put("VARCHAR", "String");
        EXPECTED.put("BOOLEAN", "Boolean");
        EXPECTED.put("TINYINT", "Integer");
        EXPECTED.put("SMALLINT", "Integer");
        EXPECTED.put("INTEGER", "Integer");
        EXPECTED.put("BIGINT", "Integer");
        EXPECTED.put("UTINYINT", "Integer");
        EXPECTED.put("USMALLINT", "Integer");
        EXPECTED.put("UINTEGER", "Integer");
        EXPECTED.put("UBIGINT", "Decimal");
        EXPECTED.put("HUGEINT", "Decimal");
        EXPECTED.put("FLOAT", "Float");
        EXPECTED.put("DOUBLE", "Float");
        EXPECTED.put("DECIMAL(9,2)", "Decimal");
        EXPECTED.put("DATE", "StrictDate");
        EXPECTED.put("TIMESTAMP", "DateTime");
        EXPECTED.put("TIMESTAMP_NS", "DateTime");
        EXPECTED.put("TIMESTAMP WITH TIME ZONE", "DateTime");
        EXPECTED.put("JSON", VARIANT);
        EXPECTED.put("STRUCT(a INTEGER, b VARCHAR)", VARIANT);
        EXPECTED.put("INTEGER[]", VARIANT);
        EXPECTED.put("MAP(VARCHAR, INTEGER)", VARIANT);
        EXPECTED.put("TIME", "String");
        EXPECTED.put("UUID", "String");
        EXPECTED.put("INTERVAL", "String");
        EXPECTED.put("ENUM('a', 'b')", "String");
    }

    private static final String WRAPPER = """
            ###Connection
            RelationalDatabaseConnection t::C { store: t::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::RT { mappings: []; connections: [ t::DB: [ c: t::C ] ]; }
            """;

    /**
     * A table of every type, created in a real DuckDB and read back through its STRUCTURED catalog
     * ({@link DuckDb#CATALOG_COLUMNS_SQL}): no type string parsed.
     */
    static List<CatalogModel.Column> readCatalog(java.util.Collection<String> types) throws java.sql.SQLException {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
             java.sql.Statement st = conn.createStatement()) {
            List<String> cols = new java.util.ArrayList<>();
            int i = 0;
            for (String type : types) {
                cols.add("c" + i++ + " " + type);
            }
            st.execute("CREATE TABLE T (" + String.join(", ", cols) + ")");
            return columnsOf(st, "main", "T");
        }
    }

    /** A table's columns, read by THE catalog question. */
    static List<CatalogModel.Column> columnsOf(java.sql.Statement st, String schema, String table) throws java.sql.SQLException {
        List<CatalogModel.Column> out = new java.util.ArrayList<>();
        String sql = DuckDb.CATALOG_COLUMNS_SQL.replace("{schema}", "'" + schema + "'").replace("{table}", "'" + table + "'");
        try (java.sql.ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new CatalogModel.Column(rs.getString(1), rs.getString(2), rs.getString(3),
                        (Integer) rs.getObject(4), (Integer) rs.getObject(5), rs.getBoolean(6)));
            }
        }
        return out;
    }

    /** The columns of a real DuckDB table declared as {@code ddl}, read by THE catalog question. */
    static List<CatalogModel.Column> catalog(String ddl) {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
             java.sql.Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE T (" + ddl + ")");
            return columnsOf(st, "main", "T");
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void everyDuckDbTypeCompiles_andTheCompilerTypesIt() throws java.sql.SQLException {
        List<CatalogModel.Column> columns = readCatalog(EXPECTED.keySet());
        CatalogModel.Database db = CatalogModel.database("t::DB", null, "T", columns, new DuckDb(), true);
        ExprType root = com.legend.Compiler.query(com.legend.Compiler.compileModel(db.text() + WRAPPER), db.accessor()).resultType();
        List<String> types = UpstreamRelationType.columns(root).stream()
                .map(c -> UpstreamRelationType.typePath(c.type())).toList();
        assertEquals(List.copyOf(EXPECTED.values()), types, db.text());
    }

    /**
     * EVERY canonical type the DuckDB this builds with knows has a DECISION: declared
     * ({@link DuckDb#CATALOG_TYPES}, or DECIMAL from its precision and scale) or refused with a
     * reason ({@link DuckDb#CATALOG_REFUSED}). A DuckDB upgrade that adds a type fails here until
     * someone decides -- it is never refused by accident.
     */
    @Test
    void everyCanonicalDuckDbTypeHasADecision() throws java.sql.SQLException {
        List<String> undecided = new java.util.ArrayList<>();
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
             java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery(
                     "SELECT DISTINCT logical_type FROM duckdb_types() WHERE internal AND type_oid IS NOT NULL ORDER BY 1")) {
            while (rs.next()) {
                String t = rs.getString(1);
                if (!t.equals("DECIMAL") && !DuckDb.CATALOG_TYPES.containsKey(t) && !DuckDb.CATALOG_REFUSED.containsKey(t)) {
                    undecided.add(t);
                }
            }
        }
        assertEquals(List.of(), undecided, "canonical DuckDB types with no decision");
    }

    @Test
    void aTypeTheDdlCannotSayIsConvertedAtTheSource_named() {
        CatalogModel.Database db = CatalogModel.database("t::DB", "s", "orders", catalog("id BIGINT, \"at\" TIMESTAMP WITH TIME ZONE, big UBIGINT"), new DuckDb(), true);
        assertEquals(List.of(new CatalogModel.Conversion("at", "CAST(timezone('UTC', \"at\") AS TIMESTAMP)"),
                new CatalogModel.Conversion("big", "CAST(\"big\" AS DECIMAL(20,0))")), db.conversions());
        assertTrue(db.text().contains("Schema s"), db.text());
        assertEquals("#>{t::DB.s.orders}#", db.accessor());
    }

    /** A copy's select list names each converted column once, quoted as the writer quotes it, and is a plain star
     *  when nothing converts. */
    @Test
    void aCopyAppliesEveryConversionUnderItsColumnsName() {
        CatalogModel.Database db = CatalogModel.database("t::DB", null, "orders",
                catalog("id BIGINT, \"at\" TIMESTAMP WITH TIME ZONE, big UBIGINT"), new DuckDb(), true);
        assertEquals("* REPLACE (CAST(timezone('UTC', \"at\") AS TIMESTAMP) AS \"at\", CAST(\"big\" AS DECIMAL(20,0)) AS \"big\")",
                db.copySelectList());
        assertEquals("*", CatalogModel.database("t::DB", null, "t", catalog("id BIGINT"), new DuckDb(), true).copySelectList());
    }

    @Test
    void aNestedColumnIsAVariantAsStored_evenOnAReadOnlySource() {
        CatalogModel.Database db = CatalogModel.database("t::DB", null, "orders", catalog("items STRUCT(sku VARCHAR)[], attrs MAP(VARCHAR, INTEGER)"), new DuckDb(), false);
        assertEquals(List.of(), db.conversions());
        assertEquals(List.of(), db.excluded());
        assertTrue(db.text().contains("items SEMISTRUCTURED") && db.text().contains("attrs SEMISTRUCTURED"), db.text());
    }

    @Test
    void aReadOnlySourceLeavesOutWhatItCannotConvert_namingIt() {
        CatalogModel.Database db = CatalogModel.database("t::DB", null, "orders", catalog("id BIGINT, ref UUID, big UBIGINT"), new DuckDb(), false);
        assertEquals(List.of("big"), db.excluded());
        assertEquals(List.of(), db.conversions());
        assertTrue(!db.text().contains("big ") && db.text().contains("ref OTHER"), db.text());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CatalogModel.database("t::DB", null, "T",
                catalog("big UBIGINT"), new DuckDb(), false));
        assertTrue(e.getMessage().contains("big"), e.getMessage());
    }

    /** A type Pure cannot name is declared OTHER and read as its text in place (StoredReads), so even a
     *  read-only source keeps the column: a uuid, an interval, a time of day, an enum, a bit string. */
    @Test
    void aTypePureCannotNameIsOther_readInPlaceOnAnySource() {
        CatalogModel.Database db = CatalogModel.database("t::DB", null, "t",
                catalog("u UUID, i INTERVAL, tm TIME, e ENUM('a', 'b'), b BIT"), new DuckDb(), false);
        assertEquals(List.of(), db.excluded());
        assertEquals(List.of(), db.conversions());
        for (String c : List.of("u", "i", "tm", "e", "b")) {
            assertTrue(db.text().contains(c + " OTHER"), db.text());
        }
    }

    /**
     * A zoned timestamp is its UTC instant (docs/DATACUBE_APP_PLAN_2026_10_02.md, leg B): a read-only
     * source declares it TIMESTAMP and reads it as stored, under the UTC session every reader runs
     * ({@link SqlDialect#sessionSetup}); a copy is converted to its UTC wall time.
     */
    @Test
    void aZonedTimestampIsReadInPlaceOnAReadOnlySource_andConvertedInACopy() {
        List<CatalogModel.Column> columns = catalog("id BIGINT, \"at\" TIMESTAMPTZ NOT NULL");
        CatalogModel.Database readOnly = CatalogModel.database("t::DB", "sales", "orders", columns, new DuckDb(), false);
        assertEquals(List.of(), readOnly.excluded());
        // read in place as stored; listed for a copy (a Snap) to apply
        assertEquals(List.of(new CatalogModel.Conversion("at", "CAST(timezone('UTC', \"at\") AS TIMESTAMP)")), readOnly.conversions());
        assertTrue(readOnly.text().contains(" at TIMESTAMP NOT NULL"), readOnly.text());
        CatalogModel.Database copy = CatalogModel.database("t::DB", "sales", "orders", columns, new DuckDb(), true);
        assertEquals(List.of(new CatalogModel.Conversion("at", "CAST(timezone('UTC', \"at\") AS TIMESTAMP)")), copy.conversions());
        assertEquals(readOnly.text(), copy.text());
    }

    /**
     * The same model on a Postgres runtime (a warehouse's Postgres catalog): the zoned column is read as
     * stored, compared with a timestamp literal and grouped by its year and month in Postgres's own SQL --
     * which the session's UTC zone makes its UTC instant's.
     */
    @Test
    void aZonedTimestampOnPostgres_filtersAndGroupsByDatePartsAsStored() {
        CatalogModel.Database db = CatalogModel.database("t::DB", "sales", "orders",
                catalog("id BIGINT, ordered_at TIMESTAMPTZ NOT NULL"), new DuckDb(), false);
        String model = db.text() + WRAPPER.replace("type: DuckDB", "type: Postgres");
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), db.accessor()
                + "->filter(r|$r.ordered_at >= %2025-01-01T00:00:00)"
                + "->extend(~[y: r|$r.ordered_at->year(), m: r|$r.ordered_at->monthNumber()])"
                + "->groupBy(~[y, m], ~n: r|$r.id: c|$c->count())").plan("t::RT").sql();
        assertTrue(sql.contains("FROM \"sales\".\"orders\""), sql);
        assertTrue(sql.contains("\"ordered_at\" >= "), sql);
        assertTrue(!sql.contains("timezone("), sql);
    }

    @Test
    void twoColumnsOneNameApartByCaseAreRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CatalogModel.database("t::DB", null, "T", List.of(new CatalogModel.Column("a", "INTEGER", "INTEGER", 32, 0, false),
                        new CatalogModel.Column("A", "INTEGER", "INTEGER", 32, 0, false)), new DuckDb(), true));
        assertTrue(e.getMessage().contains("'A'"), e.getMessage());
    }

    @Test
    void awkwardNamesReadThroughTheAccessor_andRenderAsTheDatabaseSpellsThem() {
        CatalogModel.Database db = CatalogModel.database("t::DB", "my schema", "total \"pnl\" 2024",
                catalog("\"a b\" INTEGER"), new DuckDb(), true);
        assertEquals("#>{t::DB.\"my schema\".\"total \\\"pnl\\\" 2024\"}#", db.accessor());
        ExprType root = com.legend.Compiler.query(com.legend.Compiler.compileModel(db.text() + WRAPPER), db.accessor()).resultType();
        assertEquals(List.of("a b"), UpstreamRelationType.columns(root).stream().map(c -> c.name()).toList());
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(db.text() + WRAPPER), db.accessor() + "->select(~['a b'])").plan("t::RT").sql();
        assertTrue(sql.contains("FROM \"my schema\".\"total \"\"pnl\"\" 2024\""), sql);
        for (String name : List.of("select", "a-b", "2024")) {
            CatalogModel.Database k = CatalogModel.database("t::DB", null, name,
                    catalog("a INTEGER"), new DuckDb(), true);
            assertEquals(List.of("a"), UpstreamRelationType.columns(
                    com.legend.Compiler.query(com.legend.Compiler.compileModel(k.text() + WRAPPER), k.accessor()).resultType()).stream().map(c -> c.name()).toList(),
                    name);
        }
        // upstream splits the accessor on '.': a dotted name cannot be carried
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CatalogModel.database(
                "t::DB", null, "a.b", catalog("a INTEGER"), new DuckDb(), true));
        assertTrue(e.getMessage().contains("a.b"), e.getMessage());
    }

    /** Bytes (a Postgres bytea) are left out, by name, on every source; the rest of the table opens. */
    @Test
    void aBlobIsLeftOut_namingTheColumn() {
        for (boolean convertible : List.of(true, false)) {
            CatalogModel.Database db = CatalogModel.database("t::DB", null, "T", catalog("id INTEGER, payload BLOB"), new DuckDb(), convertible);
            assertEquals(List.of("payload"), db.excluded());
            assertEquals(List.of(), db.conversions());
            assertTrue(db.text().contains("id INTEGER") && !db.text().contains("payload"), db.text());
        }
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CatalogModel.database("t::DB", null, "T",
                catalog("payload BLOB"), new DuckDb(), true));
        assertTrue(e.getMessage().contains("payload"), e.getMessage());
    }

    @Test
    void aCatalogTypeSaysExactlyWhatItsReadNeeds() {
        assertThrows(IllegalArgumentException.class, () -> new CatalogType(CatalogType.Read.AS_STORED, "INTEGER", "CAST(%s AS INTEGER)", null));
        assertThrows(IllegalArgumentException.class, () -> new CatalogType(CatalogType.Read.COPY_CONVERTED, "TIMESTAMP", null, null));
        assertThrows(IllegalArgumentException.class, () -> new CatalogType(CatalogType.Read.LEFT_OUT, "VARCHAR", null, "why"));
        assertThrows(IllegalArgumentException.class, () -> new CatalogType(CatalogType.Read.LEFT_OUT, null, null, null));
    }

    @Test
    void aNameThatIsNotAnIdentifierIsQuoted_withTheLexersEscapes() {
        assertEquals("\"total pnl\"", CatalogModel.ident("total pnl"));
        assertEquals("\"a\\\"b\"", CatalogModel.ident("a\"b"));
        assertEquals("region", CatalogModel.ident("region"));
    }
}
