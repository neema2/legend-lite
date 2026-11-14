// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The stored-reads pass on its own (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, 4.3): identity
 * where nothing is read, each dialect's spelling of a read, and the refusals.
 */
class StoredReadsTest {

    private static final SqlDdl.ColumnType OTHER = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.OTHER);
    private static final SqlDdl.ColumnType ARRAY = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.ARRAY);
    private static final SqlDdl.ColumnType INT = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER);

    private static final OutputCol ID = new OutputCol("ID", SqlType.Scalar.INTEGER, false, OutputCol.Origin.PHYSICAL);
    private static final OutputCol V = new OutputCol("V", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.PHYSICAL);

    private static SqlSource.Table table(Map<String, SqlDdl.ColumnType> stored) {
        return new SqlSource.Table("T", "t0", List.of(ID, V), false, stored, com.legend.sql.NameQuoting.BARE);
    }

    private static SqlSelect selectV(SqlSource.Table t) {
        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", V), null, null)), false,
                t, null, List.of(), null, null, List.of(), null, null, List.of(V));
    }

    @Test
    void aStatementOverNoReadColumnIsReturnedAsItCame() {
        SqlQuery q = selectV(table(Map.of("ID", INT, "V", INT)));
        assertSame(q, new StoredReads(new DuckDb()::readsStored).rewriteRoot(q));
        SqlQuery unstamped = selectV(table(Map.of()));
        assertSame(unstamped, new StoredReads(new Postgres()::readsStored).rewriteRoot(unstamped));
    }

    @Test
    void eachDialectSpellsItsRead() {
        SqlQuery other = selectV(table(Map.of("V", OTHER)));
        assertEquals("SELECT CAST(t0.V AS VARCHAR) AS V\nFROM T AS t0", new DuckDb().render(other));
        // Postgres folds the store's bare V (D8). Inside a query it labels the read as it is referenced, so an outer
        // query's reference to it names the same column; at the root (2026-11-12, the total wall's B) the label is the
        // column's own name, quoted where Postgres would fold it (AnsiSqlRenderer#labeled)
        assertTrue(new Postgres().render(other).startsWith("SELECT CAST(\"t0\".v AS VARCHAR) AS \"V\""),
                new Postgres().render(other));
        // the engine-text printer spells what legend-engine would: every column bare (S26)
        assertTrue(!new EngineStyleH2().render(other).contains("CAST"), new EngineStyleH2().render(other));
        // an array cannot be cast to jsonb on Postgres: it is converted; DuckDB reads it as stored
        SqlQuery array = selectV(table(Map.of("V", ARRAY)));
        assertTrue(new Postgres().render(array).startsWith("SELECT to_jsonb(\"t0\".v) AS \"V\""),
                new Postgres().render(array));
        assertEquals("SELECT t0.V AS V\nFROM T AS t0", new DuckDb().render(array));
    }

    @Test
    void anUnqualifiedReferenceToAReadColumnIsRefusedByName() {
        SqlQuery q = new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlExpr.Column(null, "V", com.legend.sql.SqlTyping.UNKNOWN, OutputCol.Origin.PHYSICAL), null, null)),
                false, table(Map.of("V", OTHER)), null, List.of(), null, null, List.of(), null, null, List.of(V));
        var e = assertThrows(DialectCapability.class, () -> new DuckDb().render(q));
        assertTrue(e.getMessage().contains("'V' of table 'T'"), e.getMessage());
    }
}
