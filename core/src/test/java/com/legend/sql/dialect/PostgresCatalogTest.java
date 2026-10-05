// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.type.ExprType;
import com.legend.plan.UpstreamRelationType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A Postgres table's model, from Postgres's OWN catalog (docs/STORE_TYPES_HOMEWORK_2026_10_02.md
 * step 7). The rows are what the warehouse's Postgres catalog question answered for a probe table
 * of every kind of column on Postgres 17.11 (2026-10-02) -- the same rows the live lane reads.
 */
class PostgresCatalogTest {

    /** name, data_type, logical_type, precision, scale, not_null -- as the catalog question answered. */
    private static final List<CatalogModel.Column> PROBE = List.of(
            col("id", "integer", "int4", true), col("i2", "smallint", "int2"), col("i8", "bigint", "int8"),
            col("n", "numeric", "numeric"), new CatalogModel.Column("n12", "numeric(12,3)", "numeric", 12, 3, false),
            col("r", "real", "float4"), col("d", "double precision", "float8"), col("b", "boolean", "bool"),
            col("t", "text", "text"), col("vc", "character varying(20)", "varchar"), col("ch", "character(3)", "bpchar"),
            col("dt", "date", "date"), col("ts", "timestamp without time zone", "timestamp"),
            col("tstz", "timestamp with time zone", "timestamptz"), col("tm", "time without time zone", "time"),
            col("tmtz", "time with time zone", "timetz"), col("iv", "interval", "interval"), col("u", "uuid", "uuid"),
            col("j", "json", "json"), col("jb", "jsonb", "jsonb"), col("ia", "integer[]", "ARRAY"),
            col("ta", "text[]", "ARRAY"), col("ip", "inet", "inet"), col("cidr_", "cidr", "cidr"),
            col("mac", "macaddr", "macaddr"), col("m", "money", "money"), col("x", "xml", "xml"),
            col("by", "bytea", "bytea"), col("pt", "point", "point"), col("mo", "probe.mood", "ENUM"),
            col("bits", "bit(3)", "bit"), col("vb", "bit varying", "varbit"), col("rng", "int4range", "int4range"),
            col("ts_v", "tsvector", "tsvector"), col("oid_", "oid", "oid"),
            new CatalogModel.Column("dom", "probe.pos", "numeric", 9, 2, false), col("comp", "probe.pair", "COMPOSITE"),
            col("mood_arr", "probe.mood[]", "ARRAY"), col("nn", "integer", "int4", true));

    private static CatalogModel.Column col(String name, String dataType, String logical) {
        return col(name, dataType, logical, false);
    }

    private static CatalogModel.Column col(String name, String dataType, String logical, boolean notNull) {
        return new CatalogModel.Column(name, dataType, logical, null, null, notNull);
    }

    /** Every non-array base type in Postgres 17.11's pg_catalog (typtype b, r, m), as it listed them, 2026-10-02. */
    private static final List<String> PG17_BASE_TYPES = List.of("bool", "date", "time", "timestamp", "timestamptz",
            "timetz", "box", "circle", "line", "lseg", "path", "point", "polygon", "cidr", "inet", "float4", "float8",
            "int2", "int4", "int8", "money", "numeric", "oid", "regclass", "regcollation", "regconfig", "regdictionary",
            "regnamespace", "regoper", "regoperator", "regproc", "regprocedure", "regrole", "regtype", "datemultirange",
            "int4multirange", "int8multirange", "nummultirange", "tsmultirange", "tstzmultirange", "daterange",
            "int4range", "int8range", "numrange", "tsrange", "tstzrange", "bpchar", "name", "text", "varchar",
            "interval", "aclitem", "bytea", "cid", "gtsvector", "json", "jsonb", "jsonpath", "macaddr", "macaddr8",
            "pg_lsn", "pg_snapshot", "refcursor", "tid", "tsquery", "tsvector", "txid_snapshot", "uuid", "xid", "xid8",
            "xml", "bit", "varbit", "char", "pg_brin_bloom_summary", "pg_brin_minmax_multi_summary", "pg_dependencies",
            "pg_mcv_list", "pg_ndistinct", "pg_node_tree");

    @Test
    void everyBuiltInPostgresTypeAndEveryKindHasADecision() {
        List<String> undecided = new ArrayList<>();
        for (String t : PG17_BASE_TYPES) {
            try {
                new Postgres().catalogType(col("c", t, t));
            } catch (DialectCapability e) {
                undecided.add(t);
            }
        }
        for (String kind : List.of("ARRAY", "ENUM", "COMPOSITE", "RANGE", "USER-DEFINED")) {
            assertEquals("OTHER", new Postgres().catalogType(col("c", kind, kind)).declared(), kind);
        }
        assertEquals(List.of(), undecided, "built-in Postgres types with no decision");
    }

    @Test
    void eachColumnIsDeclaredByPostgressRules() {
        Map<String, String> declared = new LinkedHashMap<>();
        for (CatalogModel.Column c : PROBE) {
            CatalogType t = new Postgres().catalogType(c);
            declared.put(c.name(), t.read() == CatalogType.Read.LEFT_OUT ? "left out" : t.declared());
        }
        Map<String, String> expected = new LinkedHashMap<>();
        for (String[] e : new String[][] {
                {"id", "INTEGER"}, {"i2", "SMALLINT"}, {"i8", "BIGINT"}, {"n", "DOUBLE"}, {"n12", "DECIMAL(12,3)"},
                {"r", "REAL"}, {"d", "DOUBLE"}, {"b", "BIT"}, {"t", "VARCHAR(4096)"}, {"vc", "VARCHAR(4096)"},
                {"ch", "VARCHAR(4096)"}, {"dt", "DATE"}, {"ts", "TIMESTAMP"}, {"tstz", "TIMESTAMP"}, {"tm", "OTHER"},
                {"tmtz", "OTHER"}, {"iv", "OTHER"}, {"u", "OTHER"}, {"j", "SEMISTRUCTURED"}, {"jb", "SEMISTRUCTURED"},
                {"ia", "OTHER"}, {"ta", "OTHER"}, {"ip", "OTHER"}, {"cidr_", "OTHER"}, {"mac", "OTHER"}, {"m", "OTHER"},
                {"x", "OTHER"}, {"by", "left out"}, {"pt", "OTHER"}, {"mo", "OTHER"}, {"bits", "OTHER"}, {"vb", "OTHER"},
                {"rng", "OTHER"}, {"ts_v", "OTHER"}, {"oid_", "OTHER"}, {"dom", "DECIMAL(9,2)"}, {"comp", "OTHER"},
                {"mood_arr", "OTHER"}, {"nn", "INTEGER"}}) {
            expected.put(e[0], e[1]);
        }
        assertEquals(expected, declared);
    }

    private static final String WRAPPER = """
            ###Connection
            RelationalDatabaseConnection t::C { store: t::DB; type: Postgres; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::RT { mappings: []; connections: [ t::DB: [ c: t::C ] ]; }
            """;

    @Test
    void theProbeTablesModelCompiles_bytesLeftOut_andItsColumnsReadByPostgres() {
        CatalogModel.Database db = CatalogModel.database("t::DB", "probe", "kinds", PROBE, new Postgres(), false);
        assertEquals(List.of("by"), db.excluded());
        assertEquals(List.of(new CatalogModel.Conversion("tstz", "CAST(timezone('UTC', \"tstz\") AS TIMESTAMP)")),
                db.conversions());
        ExprType root = com.legend.Compiler.query(com.legend.Compiler.compileModel(db.text() + WRAPPER), db.accessor()).resultType();
        Map<String, String> types = new LinkedHashMap<>();
        UpstreamRelationType.columns(root).forEach(c -> types.put(c.name(), UpstreamRelationType.typePath(c.type())));
        assertEquals("String", types.get("ip"));
        assertEquals("String", types.get("ia"));
        assertEquals(com.legend.compiler.element.type.PlatformTypes.VARIANT, types.get("jb"));
        assertEquals("Decimal", types.get("dom"));
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(db.text() + WRAPPER), db.accessor()
                + "->filter(x|$x.ip->toOne()->contains('10.'))->select(~[id, ip, ia, j])").plan("t::RT").sql();
        // every Postgres type has a text form: an inet is searched as text, an array is shown as text,
        // and json is read as jsonb -- never cast an array to jsonb
        assertTrue(sql.contains("strpos(CAST(\"t0\".\"ip\" AS VARCHAR), '10.')"), sql);
        assertTrue(sql.contains("CAST(\"t0\".\"ia\" AS VARCHAR)"), sql);
        assertTrue(sql.contains("CAST(\"t0\".\"j\" AS JSONB)"), sql);
    }

    @Test
    void aDatabaseTypeReadsItsCatalogWithItsOwnDialect() {
        assertTrue(com.legend.database.Databases.dialect(com.legend.model.ConnectionDefinition.DatabaseType.Postgres) instanceof Postgres);
        assertTrue(com.legend.database.Databases.dialect(com.legend.model.ConnectionDefinition.DatabaseType.DuckDB) instanceof DuckDb);
    }
}
